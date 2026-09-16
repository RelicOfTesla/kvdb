package rpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/rpc/codec"
)

// Config 是 RPC 客户端配置。
type Config struct {
	// Addr 是服务端地址 host:port。
	Addr string

	// Auth / Password 必须与服务端一致。
	Auth     AuthMode
	Password string

	// TLSConfig 非空则用 TLS 连接。留空且 URI 带 tls=1 时，按后续字段自动构造。
	TLSConfig *tls.Config

	// PoolSize 是连接池上限（<=0 取 DefaultPoolSize）。
	// RPC 单连接一次只能跑一条命令（请求-应答必须一一对应），
	// 并发要靠多条连接，因此这个值直接决定客户端并发度。
	PoolSize int

	// DialTimeout 是建立连接（含 TLS 握手与认证）的超时。
	DialTimeout time.Duration

	// Codec 指定编解码实现，nil 取默认的 RESP。必须与服务端一致。
	Codec codecFactory
}

// DefaultPoolSize 是未显式配置时的连接数。
const DefaultPoolSize = 8

// DefaultDialTimeout 是未显式配置时的拨号超时。
const DefaultDialTimeout = 10 * time.Second

// Provider 是 RPC 客户端：把远端基座当作本地基座使用。
//
// 它实现 core.FullProvider（KV + Queue + ZSet + Batch + Close），因此可以直接
// 交给 kvdb.Wrap 得到 DB，或注册成 scheme 用 kvdb.Open("rpc://…") 打开。
//
// **client 不知道也不关心对端是哪种底座**：能力（含 BatchComposed）在首次连接时
// 向服务端查询一次后缓存，除此之外所有命令都是透传。
type Provider struct {
	addr     string
	auth     AuthMode
	password string
	tls      *tls.Config
	codec    codecFactory
	dialTO   time.Duration

	// 连接池：idle 是空闲连接栈；created 是"已建立且未回收"的连接数
	// （空闲 + 在借）。取用时若无空闲且已达 size 则等待归还，
	// 用 wait 这个一次性 channel 做通知（关闭即广播）。
	mu      sync.Mutex
	idle    []*transport
	created int
	size    int
	wait    chan struct{}

	// caps 由首次连接时查询服务端得到，之后只读；
	// 因此不需要锁，Capabilities 可以任意并发调用。
	caps core.Caps

	closed atomic.Bool
}

// 编译期校验：client 必须是一个完好的 FullProvider。
var (
	_ core.FullProvider          = (*Provider)(nil)
	_ core.BatchComposedProvider = (*Provider)(nil)
	_ core.BatchProvider         = (*Provider)(nil)
	_ core.QueueProvider         = (*Provider)(nil)
	_ core.ZSetProvider          = (*Provider)(nil)
)

// Open 连接 addr（host:port），不做认证。
func Open(ctx context.Context, addr string) (*Provider, error) {
	return OpenWithConfig(ctx, Config{Addr: addr})
}

// OpenWithConfig 按配置建立客户端。会先拨一条连接完成连通性/认证/能力探测，
// 失败即返回错误（"打开成功"意味着确实能用）。
func OpenWithConfig(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("rpc: client needs an address")
	}
	if cfg.Auth != AuthNone && cfg.Password == "" {
		return nil, fmt.Errorf("rpc: auth mode %s requires a password", cfg.Auth)
	}
	size := cfg.PoolSize
	if size <= 0 {
		size = DefaultPoolSize
	}
	dialTO := cfg.DialTimeout
	if dialTO <= 0 {
		dialTO = DefaultDialTimeout
	}
	p := &Provider{
		addr:     cfg.Addr,
		auth:     cfg.Auth,
		password: cfg.Password,
		tls:      cfg.TLSConfig,
		codec:    cfg.Codec,
		dialTO:   dialTO,
		size:     size,
	}
	// 预建一条连接：验证地址可达、认证可用、对端 codec 与本端一致。
	t, err := p.dial(ctx)
	if err != nil {
		return nil, err
	}
	if err := p.loadCaps(ctx, t); err != nil {
		t.Close()
		return nil, err
	}
	p.put(t)
	return p, nil
}

// OpenURI 解析 rpc://host:port 形式的 URI，使 RPC 客户端能像其他基座一样
// 经 kvdb.Open 使用：
//
//	rpc://127.0.0.1:7788?auth=challenge&password=secret
//	rpc://127.0.0.1:7788?auth=plain&password=secret
//	rpc://127.0.0.1:7788?auth=none
//	rpc://host:7788?tls=1&ca=./ca.pem
//	rpc://host:7788?tls=1&ca=./ca.pem&cert=./c.pem&key=./c.key&server_name=kvdb.internal
//	rpc://host:7788?tls=1&insecure=1        # 跳过证书校验，仅测试用
//	rpc://host:7788?codec=binary&pool=16
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	cfg, err := ConfigFromURL(u)
	if err != nil {
		return nil, err
	}
	return OpenWithConfig(ctx, cfg)
}

// ConfigFromURL 把 URI 解析成 Config，供 OpenURI 与测试共用。
func ConfigFromURL(u *url.URL) (Config, error) {
	cfg := Config{Addr: u.Host}
	q := u.Query()

	mode, err := ParseAuthMode(q.Get("auth"))
	if err != nil {
		return cfg, err
	}
	cfg.Auth = mode

	// 口令来源优先级：userinfo 密码 > userinfo 用户名（rpc://secret@host 形态）> ?password=
	if u.User != nil {
		if pw, ok := u.User.Password(); ok {
			cfg.Password = pw
		} else if name := u.User.Username(); name != "" {
			cfg.Password = name
		}
	}
	if cfg.Password == "" {
		cfg.Password = q.Get("password")
	}
	if cfg.Auth != AuthNone && cfg.Password == "" {
		return cfg, fmt.Errorf("rpc: auth=%s requires a password in the URI", cfg.Auth)
	}
	if cfg.Auth == AuthNone && cfg.Password != "" {
		// 给出口令却没选模式，几乎肯定是漏写 auth=；静默忽略会让人误以为已加密。
		return cfg, fmt.Errorf("rpc: password given but auth mode is none (add auth=plain or auth=challenge)")
	}

	if v := q.Get("pool"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return cfg, fmt.Errorf("rpc: bad pool size %q", v)
		}
		cfg.PoolSize = n
	}
	switch q.Get("codec") {
	case "", "resp":
		cfg.Codec = CodecRESP
	case "binary":
		cfg.Codec = CodecBinary
	default:
		return cfg, fmt.Errorf("rpc: unknown codec %q (want resp|binary)", q.Get("codec"))
	}
	tlsCfg, err := tlsConfigFromQuery(q)
	if err != nil {
		return cfg, err
	}
	cfg.TLSConfig = tlsCfg
	return cfg, nil
}

// tlsConfigFromQuery 按参数构造 TLS 配置。tls=1 是总开关：
// 不给证书文件时用系统根证书 + 系统主机名校验（最常见的部署）。
func tlsConfigFromQuery(q url.Values) (*tls.Config, error) {
	if !isTruthy(q.Get("tls")) {
		if q.Get("ca") != "" || q.Get("cert") != "" || q.Get("key") != "" || isTruthy(q.Get("insecure")) {
			return nil, fmt.Errorf("rpc: TLS options given without tls=1")
		}
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if sn := q.Get("server_name"); sn != "" {
		cfg.ServerName = sn
	}
	if isTruthy(q.Get("insecure")) {
		// 明文跳过校验只应出现在测试里，因此单独一个开关且命名直白。
		cfg.InsecureSkipVerify = true
	}
	if ca := q.Get("ca"); ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("rpc: read ca %s: %w", ca, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("rpc: ca %s contains no valid certificate", ca)
		}
		cfg.RootCAs = pool
	}
	cert, key := q.Get("cert"), q.Get("key")
	if (cert == "") != (key == "") {
		return nil, fmt.Errorf("rpc: client TLS needs both cert and key")
	}
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("rpc: load client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

func isTruthy(v string) bool {
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// ---- 连接管理 ----

// dial 建立并认证一条连接。
func (p *Provider) dial(ctx context.Context) (*transport, error) {
	if p.closed.Load() {
		return nil, core.ErrClosed
	}
	d := net.Dialer{Timeout: p.dialTO}
	dialCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, p.dialTO)
		defer cancel()
	}
	nc, err := d.DialContext(dialCtx, "tcp", p.addr)
	if err != nil {
		return nil, fmt.Errorf("rpc: dial %s: %w", p.addr, err)
	}
	if tcp, ok := nc.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	if p.tls != nil {
		tc := tls.Client(nc, p.tls)
		// 握手也要受超时约束，否则黑洞地址会挂住整个 Open。
		if dl, ok := dialCtx.Deadline(); ok {
			_ = tc.SetDeadline(dl)
		}
		if err := tc.HandshakeContext(dialCtx); err != nil {
			nc.Close()
			return nil, fmt.Errorf("rpc: tls handshake with %s: %w", p.addr, err)
		}
		_ = tc.SetDeadline(time.Time{})
		nc = tc
	}
	factory := p.codec
	if factory == nil {
		factory = defaultCodecFactory
	}
	cdc := factory(nc, nc)
	tr := newTransport(nc, cdc)
	// 握手与认证的应答都是即时的，因此给一个**短**截止时间：
	// 若两端 codec 不同，对端永远不会回话（它解析不了我们的字节），
	// 这时必须快速失败，而不是让 Open 卡到 readGuard 才报错。
	// 超时被明确解释成"可能是 codec 不一致"，避免把人引向网络排查。
	// 走 tr.setDeadline（而不是直接 nc.SetDeadline）：transport 自己记录
	// 当前截止时间，armReadGuard 才不会再把它覆盖成 readGuard。
	tr.setDeadline(time.Now().Add(handshakeTimeout))
	if err := clientHello(tr, cdc.Name()); err != nil {
		tr.Close()
		return nil, handshakeErr(err, cdc.Name())
	}
	if err := clientAuth(tr, p.auth, p.password); err != nil {
		tr.Close()
		return nil, handshakeErr(err, cdc.Name())
	}
	tr.setDeadline(time.Time{})
	return tr, nil
}

// handshakeTimeout 是握手/认证阶段的截止时间。
const handshakeTimeout = 5 * time.Second

// handshakeErr 给握手失败补上"codec 可能不一致"的提示。
// 这个错误现场（两端各自在等对方）不提示的话极难定位。
func handshakeErr(err error, codecName string) error {
	if errors.Is(err, ErrAuthFailed) || errors.Is(err, ErrProtocol) {
		return err
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return fmt.Errorf("rpc: handshake timed out (this codec %q may not match the server's; "+
			"the server also uses whatever was configured in ServerConfig.Codec): %w", codecName, err)
	}
	return err
}

// get 取一条可用连接：优先复用空闲连接，否则新建。
func (p *Provider) get(ctx context.Context) (*transport, error) {
	if p.closed.Load() {
		return nil, core.ErrClosed
	}
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		tr := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return tr, nil
	}
	if p.created >= p.size {
		// 池已满且都借出：等归还，而不是无上限地新建连接。
		wait := p.waitChLocked()
		p.mu.Unlock()
		select {
		case <-wait:
			return p.get(ctx)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	p.created++
	p.mu.Unlock()

	tr, err := p.dial(ctx)
	if err != nil {
		p.mu.Lock()
		p.created--
		p.mu.Unlock()
		p.notify()
		return nil, err
	}
	return tr, nil
}

// put 归还一条连接。连接数超过池大小时直接关闭（避免配置调小后长期超配）。
func (p *Provider) put(tr *transport) {
	if tr == nil {
		return
	}
	if p.closed.Load() {
		p.release(tr)
		return
	}
	p.mu.Lock()
	if len(p.idle) >= p.size {
		p.mu.Unlock()
		p.release(tr)
		return
	}
	p.idle = append(p.idle, tr)
	p.mu.Unlock()
	p.notify()
}

// release 关闭连接并腾出配额。
func (p *Provider) release(tr *transport) {
	tr.Close()
	p.mu.Lock()
	p.created--
	p.mu.Unlock()
	p.notify()
}

// waitChLocked 返回"有空位"的广播 channel（须持锁调用）。
// 用一次性 channel 而不是 sync.Cond：等待者数量是并发调用数级别的，
// 且 Cond 无法直接参与 ctx 选择，channel 可以和 ctx.Done() 一起 select。
func (p *Provider) waitChLocked() chan struct{} {
	if p.wait == nil {
		p.wait = make(chan struct{})
	}
	return p.wait
}

// notify 唤醒所有等待者（关闭并重置 channel，使后续等待者拿到新的）。
func (p *Provider) notify() {
	p.mu.Lock()
	ch := p.wait
	p.wait = nil
	p.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// call 是客户端所有命令的统一入口：借连接 → 调用 → 归还。
//
// 失败**不做任何重试**，错误原样抛给业务层：非幂等命令（QPush/ZIncr/批内的
// 同类操作）在"服务端已执行、应答未收到"的场景下重试会重复生效（重复入队、
// 重复加分），静默重试把 at-least-once 语义藏进 SDK 是不能接受的。需要重试
// 的业务方自行加工幂等键或改用可幂等的命令组合。
//
// 出错的连接只会被丢弃（见 callOnce 里的 release）：流里可能残留半个报文，
// 复用会串味；下一次调用自然走 get() 拉新连接继续。
func (p *Provider) call(ctx context.Context, args ...[]byte) (codec.Status, [][]byte, error) {
	return p.callOnce(ctx, args)
}

func (p *Provider) callOnce(ctx context.Context, args [][]byte) (codec.Status, [][]byte, error) {
	tr, err := p.get(ctx)
	if err != nil {
		return 0, nil, err
	}
	tr.setDeadline(deadlineOf(ctx))
	st, payload, err := tr.call(args...)
	if err != nil {
		// 出错的连接一律丢弃：流里可能残留半个报文，复用会串味。
		p.release(tr)
		return 0, nil, err
	}
	tr.setDeadline(time.Time{})
	p.put(tr)
	return st, payload, nil
}

func deadlineOf(ctx context.Context) time.Time {
	dl, _ := ctx.Deadline()
	return dl
}

// loadCaps 向服务端查询底层基座能力并缓存。
//
// 与握手同理，这是一个即时应答的探测：给它短截止时间，
// 使"两端不通"表现为快速失败而不是长时间挂住 Open。
func (p *Provider) loadCaps(ctx context.Context, tr *transport) error {
	tr.setDeadline(time.Now().Add(handshakeTimeout))
	st, payload, err := tr.call([]byte(mCaps))
	tr.setDeadline(time.Time{})
	if err != nil {
		return handshakeErr(err, "caps probe")
	}
	if st != codec.StatusOK || len(payload) == 0 {
		return errorForStatus(st, payload)
	}
	caps, err := decCaps(payload[0])
	if err != nil {
		return err
	}
	p.caps = caps
	return nil
}

// Capabilities 返回服务端**底层基座**的真实能力。
// 这是 RPC 透明性的核心：远程与本地必须得到同一份 Capabilities，
// 包括 BatchComposed —— 若底层基座是 leveldb，这里就是 false。
func (p *Provider) Capabilities() core.Caps { return p.caps }

// Close 关闭全部空闲连接。**不会**关闭服务端基座——server 拥有基座的生命周期。
func (p *Provider) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.created -= len(idle)
	p.mu.Unlock()
	for _, tr := range idle {
		tr.Close()
	}
	p.notify()
	return nil
}

// BatchComposed 透传底层基座的批内可见性声明。
func (p *Provider) BatchComposed() bool { return p.caps.BatchComposed }

// IncrWraps 透传底层基座的 Incr 溢出语义（Caps.IncrWraps，CAPS bit4）。
// 两者与 BatchComposed 一样按标记接口透传：根适配器/业务经类型断言拿到
// 的一定是"服务端底座的真实语义"，不会因 RPC 这一层被抹平。
func (p *Provider) IncrWraps() bool { return p.caps.IncrWraps }

// Ping 探活：验证连接与认证仍然有效（不做任何数据操作）。
func (p *Provider) Ping(ctx context.Context) error {
	st, payload, err := p.call(ctx, []byte(cmdPing))
	if err != nil {
		return err
	}
	if st != codec.StatusOK {
		return errorForStatus(st, payload)
	}
	return nil
}

// Addr 返回服务端地址。
func (p *Provider) Addr() string { return p.addr }

// Addr 返回服务端地址（已在上面定义）。
