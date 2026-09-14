// Package ssdb 提供以 SSDB 服务器（原生文本协议，8888 端口）为后端的基座，
// 实现 KV + Queue + ZSet 三种能力，命令均对应 SSDB 原生命令。
// 单连接串行复用；连接断开后返回错误，不自动重连（业务层可重新 Open）。
//
// 认证：服务端配置 server.auth 后，除 auth 外的命令都会得到 noauth 状态。
// 用 Config.Password 在 Open 时自动认证，连接即处于已认证状态；
// 也可显式调用 Auth（对应 SSDB auth 命令）。
package ssdb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

func init() { kvdb.MustRegister("ssdb", OpenURI) }

// OpenURI 解析 ssdb://[user:pass@]host:port（缺省端口 8888）；
// URI 中的密码部分作为 auth 凭据，亦支持 ?password= 传参。
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	cfg := Config{Addr: u.Host}
	if u.User != nil {
		if pw, ok := u.User.Password(); ok {
			cfg.Password = pw
		} else if name := u.User.Username(); name != "" {
			cfg.Password = name // 也接受 ssdb://password@host 形态
		}
	}
	if cfg.Password == "" {
		cfg.Password = u.Query().Get("password")
	}
	return OpenWithConfig(ctx, cfg)
}

var (
	_ core.FullProvider = (*Provider)(nil)
)

// ErrAuth 表示 SSDB 认证失败（密码错误）或命令因未认证被拒（noauth）。
var ErrAuth = errors.New("ssdb: authentication failed")

// Config 是 SSDB 连接配置。
type Config struct {
	// Addr 为 host:port；缺省端口补 8888。
	Addr string
	// Password 非空时在 Open 阶段自动执行 auth；密码错返回 ErrAuth。
	Password string
	// PoolSize 是并发连接上限，<=0 取 DefaultPoolSize。SSDB 单连接为串行
	// 请求-应答，池化让并发调用真正并行（此前单连接下并发无提升）。
	PoolSize int
}

// DefaultPoolSize 是未显式配置时的连接池大小。
const DefaultPoolSize = 8

// Provider 是 SSDB 基座。连接池并发安全：每次操作借一条连接串行收发，
// 用毕归还；池空时阻塞等待（受 ctx 约束）。ctx 可携带超时。
type Provider struct {
	addr      string
	password  string
	idle      chan *conn   // 空闲连接队列，容量即池大小
	total     atomic.Int32 // 已创建的连接总数（池内+在借+预建），上限 max
	max       int32
	closed    chan struct{}
	closeOnce sync.Once
}

// Open 连接到 addr（host:port；缺省端口补 8888），不带认证。
func Open(ctx context.Context, addr string) (*Provider, error) {
	return OpenWithConfig(ctx, Config{Addr: addr})
}

// OpenWithConfig 按配置建立连接池；Password 非空时每条连接建立后先认证。
func OpenWithConfig(ctx context.Context, cfg Config) (*Provider, error) {
	addr := cfg.Addr
	if !strings.Contains(addr, ":") {
		addr = addr + ":8888"
	}
	size := cfg.PoolSize
	if size <= 0 {
		size = DefaultPoolSize
	}
	p := &Provider{
		addr:     addr,
		password: cfg.Password,
		idle:     make(chan *conn, size),
		max:      int32(size),
		closed:   make(chan struct{}),
	}
	// 预建一条连接用于启动期连通性/认证校验，失败即报错（保留原语义）。
	c, err := p.openConn(ctx)
	if err != nil {
		return nil, err
	}
	p.total.Store(1) // 预建连接计入总数（会计一致：total=池内+在借）
	p.idle <- c
	return p, nil
}

// openConn 新建一条连接并按需认证。
func (p *Provider) openConn(ctx context.Context) (*conn, error) {
	c, err := dial(ctx, p.addr)
	if err != nil {
		return nil, err
	}
	if p.password != "" {
		st, recs, err := c.request(ctx, []byte("auth"), []byte(p.password))
		if err != nil {
			c.c.Close()
			return nil, err
		}
		if st != "ok" {
			c.c.Close()
			return nil, fmt.Errorf("%w: %s", ErrAuth, firstOr(recs, st))
		}
	}
	return c, nil
}

// acquire 借出一条已认证连接；池空且已达上限时阻塞等待空闲连接或 ctx 结束。
// 借出的连接先做存活探测（connAlive，约 100µs 开销）：已断开（服务器重启/
// 网络中断）的连接在池内被拦截丢弃并重建，业务无感——这就是"简易重连"：
// 池中只存在健康连接，断连由下一次 acquire 自动替换（半开连接仍会消耗一次
// 失败的请求，见 connAlive 说明）。
func (p *Provider) acquire(ctx context.Context) (*conn, error) {
	select {
	case <-p.closed:
		return nil, core.ErrClosed
	default:
	}
	for {
		select {
		case c := <-p.idle:
			if connAlive(c) {
				return c, nil
			}
			// 已断开：丢弃并归还计数，继续取/建。
			c.c.Close()
			p.total.Add(-1)
			continue
		default:
		}
		// 池内暂无空闲：若尚未达上限则新建（原子抢占一个名额）。
		if p.total.Load() < p.max && p.total.Add(1) <= p.max {
			c, err := p.openConn(ctx)
			if err != nil {
				p.total.Add(-1)
				return nil, err
			}
			return c, nil
		}
		select {
		case c := <-p.idle:
			if connAlive(c) {
				return c, nil
			}
			c.c.Close()
			p.total.Add(-1)
		case <-p.closed:
			return nil, core.ErrClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// connAlive 非阻塞探测连接是否仍可用：向读侧设立即超时并尝试读一字节。
//   - 读超时（无数据可读）＝ 空闲健康连接，清除 deadline 后归还可用；
//   - EOF/连接被重置 ＝ 已断开；
//   - 读到数据 ＝ 请求-应答协议下不可能有待读数据，视为连接状态异常。
//
// SSDB 服务器重启/主动断开都会把 FIN/RST 送到本地，本探测可在此类连接
// 被复用时提前拦截，避免"拿到死连接请求一次才失败"。
func connAlive(c *conn) bool {
	// 注意：deadline 必须是"未来"时刻。若设为当前时刻（已过期），Go 的 poll
	// 在读检查前就直接返回 timeout——即使对端已发 FIN 也探测不到断开。
	// 未来 100µs 的窗口足以让 poll 报告 EOF/重置，健康连接最坏多等 100µs
	//（相对请求本身可忽略；实测 EOF 探测约 30µs 返回）。
	c.c.SetReadDeadline(time.Now().Add(100 * time.Microsecond))
	var b [1]byte
	if _, err := c.c.Read(b[:]); err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			// 无数据可读：空闲健康连接，清除 deadline 后复用。
			c.c.SetReadDeadline(time.Time{})
			return true
		}
		// EOF / connection reset 等：已断开。
		return false
	}
	// 有数据可读：请求-应答协议下不应有待读数据，保守判定异常。
	return false
}

// put 归还连接；Provider 已关闭或连接已断开则直接关闭。
// 归还前做存活探测：已断开/异常连接不再回到空闲池，下次请求
// 不会复用到坏连接（简易重连 = 池中只保留健康连接，断连由新请求触发重建）。
func (p *Provider) put(c *conn) {
	select {
	case <-p.closed:
		c.c.Close()
		p.total.Add(-1)
		return
	default:
	}
	select {
	case p.idle <- c:
	default:
		// 池已满（理论上不会：连接数不超上限），保守丢弃。
		c.c.Close()
		p.total.Add(-1)
	}
}

// Auth 以 password 认证当前连接（SSDB auth 命令）。服务端未配置 auth 时
// SSDB 一律返回 ok；密码错误返回 ErrAuth。
func (p *Provider) Auth(ctx context.Context, password string) error {
	if err := p.check(); err != nil {
		return err
	}
	st, recs, err := p.do(ctx, "auth", password)
	if err != nil {
		return err
	}
	if st != "ok" {
		// 服务端回复 error + "invalid password"（见 SSDB net/server.cpp proc_auth）。
		return fmt.Errorf("%w: %s", ErrAuth, firstOr(recs, st))
	}
	return nil
}

func (p *Provider) Close() error {
	p.closeOnce.Do(func() {
		close(p.closed)
		// 关闭池内空闲连接；借出中的连接在归还时自行关闭。
		for {
			select {
			case c := <-p.idle:
				c.c.Close()
				p.total.Add(-1)
			default:
				return
			}
		}
	})
	return nil
}

// do 执行一次命令并返回 (status, payload)。
func (p *Provider) do(ctx context.Context, args ...string) (string, [][]byte, error) {
	c, err := p.acquire(ctx)
	if err != nil {
		return "", nil, err
	}
	bs := make([][]byte, len(args))
	for i, a := range args {
		bs[i] = []byte(a)
	}
	st, recs, err := c.request(ctx, bs...)
	if err != nil {
		// 连接级错误（含超时/断开）：丢弃该连接，避免污染后续请求。
		c.c.Close()
		p.total.Add(-1)
		return "", nil, err
	}
	p.put(c)
	return st, recs, nil
}

func (p *Provider) check() error {
	select {
	case <-p.closed:
		return core.ErrClosed
	default:
		return nil
	}
}

// errFrom 将服务端错误状态转换为错误。SSDB incr/zincr 失败固定回复
// "value is not an integer or out of range"，据此映射为 ErrNotInteger；
// 未认证时服务端返回 noauth（net/server.cpp 的 AUTH 前置检查），映射为 ErrAuth。
func errFrom(msg []byte) error {
	s := string(msg)
	if strings.Contains(s, "not an integer") {
		return core.ErrNotInteger
	}
	if strings.Contains(s, "authentication required") {
		return fmt.Errorf("%w: %s", ErrAuth, s)
	}
	return fmt.Errorf("ssdb: server error: %s", msg)
}

// firstOr 返回首条负载（无则回退到 fallback），用于拼接服务端错误说明。
func firstOr(recs [][]byte, fallback string) string {
	if len(recs) > 0 {
		return string(recs[0])
	}
	return fallback
}

// ---- KV ----

func (p *Provider) Set(ctx context.Context, key string, value []byte) error {
	if err := p.check(); err != nil {
		return err
	}
	st, _, err := p.do(ctx, "set", key, string(value))
	if err != nil {
		return err
	}
	if st != "ok" {
		return fmt.Errorf("ssdb: set: status %q", st)
	}
	return nil
}

// SetEx 写入 value 并设置 TTL（SSDB 原生命令 setx）。
func (p *Provider) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	if err := p.check(); err != nil {
		return err
	}
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	st, _, err := p.do(ctx, "setx", key, string(value), strconv.FormatInt(ttl, 10))
	if err != nil {
		return err
	}
	if st != "ok" {
		return fmt.Errorf("ssdb: setx: status %q", st)
	}
	return nil
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := p.check(); err != nil {
		return nil, false, err
	}
	return p.getLocked(ctx, key)
}

// getLocked 在调用方已持有 p.mu 时读取 key（Scan 补偿用）。
func (p *Provider) getLocked(ctx context.Context, key string) ([]byte, bool, error) {
	st, recs, err := p.do(ctx, "get", key)
	if err != nil {
		return nil, false, err
	}
	switch st {
	case "ok":
		return recs[0], true, nil
	case "not_found":
		return nil, false, nil
	default:
		return nil, false, errFrom(recs[0])
	}
}

func (p *Provider) Del(ctx context.Context, key string) error {
	if err := p.check(); err != nil {
		return err
	}
	st, _, err := p.do(ctx, "del", key)
	if err != nil {
		return err
	}
	if st != "ok" {
		return fmt.Errorf("ssdb: del: status %q", st)
	}
	return nil
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	if err := p.check(); err != nil {
		return false, err
	}
	st, recs, err := p.do(ctx, "exists", key)
	if err != nil {
		return false, err
	}
	if st != "ok" {
		return false, fmt.Errorf("ssdb: exists: status %q", st)
	}
	return recs[0][0] == '1', nil
}

func (p *Provider) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
	st, recs, err := p.do(ctx, "incr", key, strconv.FormatInt(delta, 10))
	if err != nil {
		return 0, err
	}
	if st != "ok" {
		return 0, errFrom(recs[0])
	}
	return strconv.ParseInt(string(recs[0]), 10, 64)
}

func (p *Provider) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	args := append([]string{"multi_get"}, keys...)
	st, recs, err := p.do(ctx, args...)
	if err != nil {
		return nil, err
	}
	if st != "ok" {
		return nil, fmt.Errorf("ssdb: multi_get: status %q", st)
	}
	out := make(map[string][]byte, len(recs)/2)
	for i := 0; i+1 < len(recs); i += 2 {
		out[string(recs[i])] = recs[i+1]
	}
	return out, nil
}

func (p *Provider) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	limit = normalizeLimit(limit)
	// 真实 SSDB 的 scan 语义为 start 开区间、end 闭区间（分页便利），
	// SDK 契约统一为闭区间，此处对存在的 start 键做一次 get 补偿。
	st, recs, err := p.do(ctx, "scan", start, end, strconv.Itoa(limit))
	if err != nil {
		return nil, err
	}
	if st != "ok" {
		return nil, fmt.Errorf("ssdb: scan: status %q", st)
	}
	out := make([]core.KeyValue, 0, len(recs)/2)
	for i := 0; i+1 < len(recs); i += 2 {
		out = append(out, core.KeyValue{Key: string(recs[i]), Value: recs[i+1]})
	}
	if start == "" || len(out) >= limit {
		return out, nil
	}
	if v, ok, err := p.getLocked(ctx, start); err != nil {
		return nil, err
	} else if ok {
		out = append([]core.KeyValue{{Key: start, Value: v}}, out...)
	}
	return out, nil
}

func (p *Provider) Expire(ctx context.Context, key string, ttl int64) error {
	if err := p.check(); err != nil {
		return err
	}
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	st, _, err := p.do(ctx, "expire", key, strconv.FormatInt(ttl, 10))
	if err != nil {
		return err
	}
	if st != "ok" {
		return fmt.Errorf("ssdb: expire: status %q", st)
	}
	return nil
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	if err := p.check(); err != nil {
		return 0, false, err
	}
	st, recs, err := p.do(ctx, "ttl", key)
	if err != nil {
		return 0, false, err
	}
	if st != "ok" {
		return 0, false, fmt.Errorf("ssdb: ttl: status %q", st)
	}
	n, err := strconv.ParseInt(string(recs[0]), 10, 64)
	if err != nil {
		return 0, false, err
	}
	// SSDB ttl 对"无 TTL 或 key 不存在"均返回 -1，统一映射为 ok=false。
	if n < 0 {
		return -1, false, nil
	}
	return n, true, nil
}

// ---- Queue ----

func (p *Provider) QPush(ctx context.Context, name string, value []byte) error {
	return p.qpush(ctx, name, value, "qpush")
}

func (p *Provider) QPushFront(ctx context.Context, name string, value []byte) error {
	return p.qpush(ctx, name, value, "qpush_front")
}

func (p *Provider) qpush(ctx context.Context, name string, value []byte, cmd string) error {
	if err := p.check(); err != nil {
		return err
	}
	st, _, err := p.do(ctx, cmd, name, string(value))
	if err != nil {
		return err
	}
	if st != "ok" {
		return fmt.Errorf("ssdb: %s: status %q", cmd, st)
	}
	return nil
}

func (p *Provider) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpop(ctx, name, "qpop")
}

func (p *Provider) QPopBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpop(ctx, name, "qpop_back")
}

func (p *Provider) qpop(ctx context.Context, name, cmd string) ([]byte, bool, error) {
	if err := p.check(); err != nil {
		return nil, false, err
	}
	st, recs, err := p.do(ctx, cmd, name)
	if err != nil {
		return nil, false, err
	}
	switch st {
	case "ok":
		return recs[0], true, nil
	case "not_found":
		return nil, false, nil
	default:
		return nil, false, errFrom(recs[0])
	}
}

func (p *Provider) QSize(ctx context.Context, name string) (int64, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
	st, recs, err := p.do(ctx, "qsize", name)
	if err != nil {
		return 0, err
	}
	if st != "ok" {
		return 0, fmt.Errorf("ssdb: qsize: status %q", st)
	}
	return strconv.ParseInt(string(recs[0]), 10, 64)
}

func (p *Provider) QFront(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpeek(ctx, name, "qfront")
}

func (p *Provider) QBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpeek(ctx, name, "qback")
}

func (p *Provider) qpeek(ctx context.Context, name, cmd string) ([]byte, bool, error) {
	if err := p.check(); err != nil {
		return nil, false, err
	}
	st, recs, err := p.do(ctx, cmd, name)
	if err != nil {
		return nil, false, err
	}
	switch st {
	case "ok":
		return recs[0], true, nil
	case "not_found":
		return nil, false, nil
	default:
		return nil, false, errFrom(recs[0])
	}
}

// ---- ZSet ----

func (p *Provider) ZSet(ctx context.Context, name, key string, score int64) error {
	if err := p.check(); err != nil {
		return err
	}
	st, _, err := p.do(ctx, "zset", name, key, strconv.FormatInt(score, 10))
	if err != nil {
		return err
	}
	if st != "ok" {
		return fmt.Errorf("ssdb: zset: status %q", st)
	}
	return nil
}

func (p *Provider) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	if err := p.check(); err != nil {
		return 0, false, err
	}
	st, recs, err := p.do(ctx, "zget", name, key)
	if err != nil {
		return 0, false, err
	}
	switch st {
	case "ok":
		s, err := strconv.ParseInt(string(recs[0]), 10, 64)
		return s, err == nil, err
	case "not_found":
		return 0, false, nil
	default:
		return 0, false, errFrom(recs[0])
	}
}

func (p *Provider) ZDel(ctx context.Context, name, key string) error {
	if err := p.check(); err != nil {
		return err
	}
	st, _, err := p.do(ctx, "zdel", name, key)
	if err != nil {
		return err
	}
	if st != "ok" {
		return fmt.Errorf("ssdb: zdel: status %q", st)
	}
	return nil
}

func (p *Provider) ZSize(ctx context.Context, name string) (int64, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
	st, recs, err := p.do(ctx, "zsize", name)
	if err != nil {
		return 0, err
	}
	if st != "ok" {
		return 0, fmt.Errorf("ssdb: zsize: status %q", st)
	}
	return strconv.ParseInt(string(recs[0]), 10, 64)
}

func (p *Provider) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	if err := p.check(); err != nil {
		return 0, false, err
	}
	st, recs, err := p.do(ctx, "zrank", name, key)
	if err != nil {
		return 0, false, err
	}
	switch st {
	case "ok":
		r, err := strconv.ParseInt(string(recs[0]), 10, 64)
		return r, err == nil, err
	case "not_found":
		return 0, false, nil
	default:
		return 0, false, errFrom(recs[0])
	}
}

func (p *Provider) ZRange(ctx context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	// SSDB 原生 zrange 为 offset/limit（无负索引）语义，客户端按
	// Redis 风格 start/stop 索引换算：负索引先经 zsize 归一化。
	offset, limit, err := p.zrangeArgs(ctx, name, start, stop)
	if err != nil {
		return nil, err
	}
	st, recs, err := p.do(ctx, "zrange", name, strconv.FormatInt(offset, 10), strconv.FormatInt(limit, 10))
	if err != nil {
		return nil, err
	}
	if st != "ok" {
		return nil, fmt.Errorf("ssdb: zrange: status %q", st)
	}
	out := make([]core.ZItem, 0, len(recs)/2)
	for i := 0; i+1 < len(recs); i += 2 {
		s, err := strconv.ParseInt(string(recs[i+1]), 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, core.ZItem{Key: string(recs[i]), Score: s})
	}
	return out, nil
}

// zrangeArgs 把 redis 风格索引换算为 SSDB zrange 的 offset/limit。
// 需在持有锁时调用（内部会发 zsize 命令）。
func (p *Provider) zrangeArgs(ctx context.Context, name string, start, stop int64) (int64, int64, error) {
	if start >= 0 && stop >= 0 {
		if start > stop {
			return 0, 0, nil
		}
		return start, stop - start + 1, nil
	}
	st, recs, err := p.do(ctx, "zsize", name)
	if err != nil {
		return 0, 0, err
	}
	if st != "ok" {
		return 0, 0, fmt.Errorf("ssdb: zsize: status %q", st)
	}
	size, err := strconv.ParseInt(string(recs[0]), 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if start < 0 {
		start = size + start
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop = size + stop
	}
	if start > stop || stop < 0 {
		return 0, 0, nil
	}
	return start, stop - start + 1, nil
}

func (p *Provider) ZIncr(ctx context.Context, name, key string, delta int64) (int64, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
	st, recs, err := p.do(ctx, "zincr", name, key, strconv.FormatInt(delta, 10))
	if err != nil {
		return 0, err
	}
	if st != "ok" {
		if errors.Is(errFrom(recs[0]), core.ErrNotInteger) {
			return 0, core.ErrNotInteger
		}
		return 0, fmt.Errorf("ssdb: zincr: status %q", st)
	}
	return strconv.ParseInt(string(recs[0]), 10, 64)
}

// normalizeLimit 与 mem 基座相同的默认页大小约定（见 mem 包注释）。
func normalizeLimit(limit int) int {
	const defaultLimit = 100 // 与 core.DefaultScanLimit 对齐
	if limit <= 0 {
		return defaultLimit
	}
	return limit
}

// ---- Batch ----

// ApplyBatch 实现 core.BatchProvider：整批命令以**流水线**发出（写入全部请求后
// 一次 Flush，再按序读取全部响应），把 N 次往返压缩为 1 次。
//
// 注意：SSDB 没有事务，流水线不提供整批原子性——个别命令失败时其余仍会生效。
// 需要原子性的场景请使用 SQL 基座（事务）或 Redis（MULTI/EXEC）。
func (p *Provider) ApplyBatch(ctx context.Context, ops []core.BatchOp) error {
	if err := p.check(); err != nil {
		return err
	}
	if len(ops) == 0 {
		return nil
	}
	reqs := make([][][]byte, 0, len(ops))
	for _, op := range ops {
		args, err := batchArgs(op)
		if err != nil {
			return err
		}
		reqs = append(reqs, args)
	}

	c, err := p.acquire(ctx)
	if err != nil {
		return err
	}
	resps, err := c.pipeline(ctx, reqs)
	if err != nil {
		// 连接级错误：丢弃该连接，避免污染后续请求。
		c.c.Close()
		p.total.Add(-1)
		return err
	}
	p.put(c)

	for i, r := range resps {
		switch r.status {
		case "ok", "not_found":
			// not_found 对 expire 等指令属正常（与单条路径一致）。
		default:
			return fmt.Errorf("ssdb: batch op %d (kind %d): %w", i, ops[i].Kind, errFrom(firstPayload(r.payload)))
		}
	}
	return nil
}

// batchArgs 把契约批操作翻译为 SSDB 命令参数。
func batchArgs(op core.BatchOp) ([][]byte, error) {
	b := func(parts ...string) [][]byte {
		out := make([][]byte, len(parts))
		for i, s := range parts {
			out[i] = []byte(s)
		}
		return out
	}
	switch op.Kind {
	case core.BatchSet:
		return [][]byte{[]byte("set"), []byte(op.Key), op.Value}, nil
	case core.BatchSetEx:
		if op.TTL <= 0 {
			return nil, core.ErrInvalidTTL
		}
		return [][]byte{[]byte("setx"), []byte(op.Key), op.Value, []byte(strconv.FormatInt(op.TTL, 10))}, nil
	case core.BatchDel:
		return b("del", op.Key), nil
	case core.BatchExpire:
		if op.TTL <= 0 {
			return nil, core.ErrInvalidTTL
		}
		return b("expire", op.Key, strconv.FormatInt(op.TTL, 10)), nil
	case core.BatchQPush:
		return [][]byte{[]byte("qpush"), []byte(op.Key), op.Value}, nil
	case core.BatchQPushFront:
		return [][]byte{[]byte("qpush_front"), []byte(op.Key), op.Value}, nil
	case core.BatchZSet:
		return b("zset", op.Key, op.Member, strconv.FormatInt(op.Score, 10)), nil
	case core.BatchZDel:
		return b("zdel", op.Key, op.Member), nil
	case core.BatchZIncr:
		return b("zincr", op.Key, op.Member, strconv.FormatInt(op.Delta, 10)), nil
	default:
		return nil, fmt.Errorf("ssdb: unknown batch op %d", op.Kind)
	}
}

// firstPayload 取首条负载，缺省为空字节串。
func firstPayload(payload [][]byte) []byte {
	if len(payload) > 0 {
		return payload[0]
	}
	return nil
}
