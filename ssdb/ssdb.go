// Package ssdb 提供以 SSDB 服务器（原生文本协议，8888 端口）为后端的基座，
// 实现 KV + Queue + ZSet 三种能力，命令均对应 SSDB 原生命令。
// 单连接串行复用；连接断开后返回错误，不自动重连（业务层可重新 Open）。
//
// 认证：服务端配置 server.auth 后，除 auth 外的命令都会得到 noauth 状态。
// 用 Config.Password 在 Open 时自动认证，连接即处于已认证状态；
// 也可调用 Auth 在线设置**池级**凭据（验证成功后池内旧连接被丢弃重建，
// 后续新连接自动用新密码认证）。
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
	if v := u.Query().Get("key_prefix"); v != "" {
		cfg.KeyPrefix = v
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
	// 请求-应答，池化让并发调用真正并行。
	PoolSize int
	// KeyPrefix 加在用户可见的 key / 队列名 / zset 名之前（SSDB 无 namespace
	// 概念，靠它与其他应用在同一实例内互相隔离）。空串表示不加前缀。
	KeyPrefix string
}

// DefaultPoolSize 是未显式配置时的连接池大小。
const DefaultPoolSize = 8

// DialTimeout 是建立 TCP 连接的超时；超时会传导到 Open 与运行期补连。
var DialTimeout = 10 * time.Second

// connProbeWindow 是 acquire 前存活探测的非阻塞读窗口：足够让内核把已到达的
// FIN/RST 报出来，又不至于拖慢每次取连接（实测 EOF 约 30µs 返回）。
var connProbeWindow = 100 * time.Microsecond

// Provider 是 SSDB 基座。连接池并发安全：每次操作借一条连接串行收发，
// 用毕归还；池空时阻塞等待（受 ctx 约束）。ctx 可携带超时。
type Provider struct {
	addr      string
	keyPrefix string                 // 见 Config.KeyPrefix：作用于三类数据的键名（SSDB 无 namespace）
	password  atomic.Pointer[string] // 池级凭据：新连接一律按此认证（Auth 可在线更新）
	idle      chan *conn             // 空闲连接队列，容量即池大小
	total     atomic.Int32           // 已创建的连接总数（池内+在借+预建），上限 max
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
		addr:      addr,
		keyPrefix: cfg.KeyPrefix,
		idle:      make(chan *conn, size),
		max:       int32(size),
		closed:    make(chan struct{}),
	}
	p.password.Store(&cfg.Password)
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
	if pw := p.password.Load(); pw != nil && *pw != "" {
		st, recs, err := c.request(ctx, []byte("auth"), []byte(*pw))
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
		// 池内暂无空闲：若尚未达上限则新建（CAS 原子预留一个名额，
		// 避免 Load 与 Add 之间的竞争把 total 永久虚增、池容量悄悄缩水）。
		if p.reserve() {
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

// reserve 以 CAS 循环原子地预留一个连接名额；已达上限返回 false。
// 不能用 Load()+Add(1) 组合：并发下失败分支已经自增过，没减回去就会永久
// 虚增 total，让连接池再也补不满容量。
func (p *Provider) reserve() bool {
	for {
		cur := p.total.Load()
		if cur >= p.max {
			return false
		}
		if p.total.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// dropIdle 关闭并计数归零当前所有空闲连接（用于凭据变更后强制重建）。
// 借出中的连接不在其中：它们仍用旧凭据，归还时会因存活探测/服务端拒绝而
// 被淘汰，最迟在下一次 acquire 时重建。
func (p *Provider) dropIdle() {
	for {
		select {
		case c := <-p.idle:
			c.c.Close()
			p.total.Add(-1)
		default:
			return
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
	c.c.SetReadDeadline(time.Now().Add(connProbeWindow))
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

// Auth 设置**池级**认证凭据（SSDB auth 命令）并立即作用于所有后续连接：
//   - 先用一条连接验证密码，成功才写入池级状态并把池中旧凭据下建立的连接
//     全部丢弃（后续按需重建时自动用新密码认证）；
//   - 失败（密码错）返回 ErrAuth，池级状态与连接池保持不变。
//
// 注意：调用瞬间**已借出**的连接仍持有旧凭据，其上的命令可能收到 noauth；
// 切换凭据建议在无明显并发请求时进行。
//
// 服务端未配置 auth 时 SSDB 一律返回 ok（此时设置任意密码都会"成功"）。
func (p *Provider) Auth(ctx context.Context, password string) error {
	if err := p.check(); err != nil {
		return err
	}
	c, err := p.acquire(ctx)
	if err != nil {
		return err
	}
	st, recs, err := c.request(ctx, []byte("auth"), []byte(password))
	if err != nil {
		c.c.Close()
		p.total.Add(-1) // 连接状态已不可信，丢弃并归还计数
		return err
	}
	if st != "ok" {
		// 服务端回复 error + "invalid password"（见 SSDB net/server.cpp proc_auth）。
		p.put(c)
		return fmt.Errorf("%w: %s", ErrAuth, firstOr(recs, st))
	}
	p.put(c)
	p.password.Store(&password)
	p.dropIdle()
	return nil
}

func (p *Provider) Close() error {
	p.closeOnce.Do(func() {
		close(p.closed)
		// 关闭池内空闲连接；借出中的连接在归还时自行关闭。
		p.dropIdle()
	})
	return nil
}

// ---- 键命名空间（见 Config.KeyPrefix）----

// k 给用户可见的 key / 队列名 / zset 名加前缀。
func (p *Provider) k(key string) string {
	if p.keyPrefix == "" {
		return key
	}
	return p.keyPrefix + key
}

// un 剥掉服务端返回键名的前缀；不带前缀（非本应用写入）的键原样返回，
// 由调用方按需要过滤。
func (p *Provider) un(key string) string {
	if p.keyPrefix == "" {
		return key
	}
	return strings.TrimPrefix(key, p.keyPrefix)
}

// has 判断服务端键名是否属于本前缀命名空间。
func (p *Provider) has(key string) bool {
	return p.keyPrefix == "" || strings.HasPrefix(key, p.keyPrefix)
}

// scanEnd 计算带前缀扫描的上界：显式 end 加前缀；空 end 用前缀的"下一个键"
// （末字节 +1）作为开区间上界，从而只覆盖本命名空间的键。
func (p *Provider) scanEnd(end string) string {
	if end != "" {
		return p.k(end)
	}
	if p.keyPrefix == "" {
		return ""
	}
	b := []byte(p.keyPrefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return p.keyPrefix // 全 0xff 前缀（极端情况）：退化为不设上界
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
	if st == "noauth" {
		// 未认证：集中映射 ErrAuth，调用方 errors.Is 即可触发重新认证，
		// 无需每个命令各自判断（服务端对未认证命令一律回复 noauth）。
		err := fmt.Errorf("%w: %s", ErrAuth, firstOr(recs, st))
		p.put(c)
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
	key = p.k(key)
	if key == "" {
		// SSDB 对空 key 返回 ok 却不写数据（SSDBImpl::set 直接返回 0），
		// 写入侧拒绝，避免"报成功但丢数据"。
		return fmt.Errorf("ssdb: set: key must not be empty")
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
	key = p.k(key)
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if key == "" {
		return fmt.Errorf("ssdb: setx: key must not be empty")
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
	key = p.k(key)
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
		return firstPayload(recs), true, nil
	case "not_found":
		return nil, false, nil
	default:
		return nil, false, errFrom([]byte(firstOr(recs, st)))
	}
}

func (p *Provider) Del(ctx context.Context, key string) error {
	if err := p.check(); err != nil {
		return err
	}
	key = p.k(key)
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
	key = p.k(key)
	st, recs, err := p.do(ctx, "exists", key)
	if err != nil {
		return false, err
	}
	if st != "ok" {
		return false, fmt.Errorf("ssdb: exists: status %q", st)
	}
	return string(firstPayload(recs)) == "1", nil
}

func (p *Provider) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
	key = p.k(key)
	if key == "" {
		return 0, fmt.Errorf("ssdb: incr: key must not be empty")
	}
	st, recs, err := p.do(ctx, "incr", key, strconv.FormatInt(delta, 10))
	if err != nil {
		return 0, err
	}
	if st != "ok" {
		return 0, errFrom([]byte(firstOr(recs, st)))
	}
	return strconv.ParseInt(string(firstPayload(recs)), 10, 64)
}

func (p *Provider) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		// 与 mem/SQL 基座对齐：空 key 列表返回空 map（SSDB multi_get 要求
		// 至少 1 个 key，会回 client_error）。
		return map[string][]byte{}, nil
	}
	args := make([]string, 0, len(keys)+1)
	args = append(args, "multi_get")
	for _, key := range keys {
		args = append(args, p.k(key))
	}
	st, recs, err := p.do(ctx, args...)
	if err != nil {
		return nil, err
	}
	if st != "ok" {
		return nil, fmt.Errorf("ssdb: multi_get: status %q", st)
	}
	out := make(map[string][]byte, len(recs)/2)
	for i := 0; i+1 < len(recs); i += 2 {
		k := string(recs[i])
		if !p.has(k) {
			continue // 不属于本命名空间的键不外泄
		}
		out[p.un(k)] = recs[i+1]
	}
	return out, nil
}

func (p *Provider) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	limit = normalizeLimit(limit)
	// 闭区间为空（start>end）直接返回，避免补偿逻辑把 start 键塞进空区间。
	if start != "" && end != "" && start > end {
		return []core.KeyValue{}, nil
	}
	// 真实 SSDB 的 scan 语义为 start 开区间、end 闭区间（分页便利），
	// SDK 契约统一为闭区间：对存在的 start 键做一次 get 补偿；补偿导致
	// 超页时截掉扫出的最后一个键，保证返回"闭区间的前 limit 个"。
	// 带 KeyPrefix 时把扫描区间夹到本命名空间内：SSDB 的 scan 按全库字典序推进，
	// 不夹住会把其他应用的键扫进来（end 为空时上界取前缀的下一个键）。
	lo, hi := p.k(start), p.scanEnd(end)
	st, recs, err := p.do(ctx, "scan", lo, hi, strconv.Itoa(limit))
	if err != nil {
		return nil, err
	}
	if st != "ok" {
		return nil, fmt.Errorf("ssdb: scan: status %q", st)
	}
	out := make([]core.KeyValue, 0, len(recs)/2)
	for i := 0; i+1 < len(recs); i += 2 {
		k := string(recs[i])
		if !p.has(k) {
			continue
		}
		out = append(out, core.KeyValue{Key: p.un(k), Value: recs[i+1]})
	}
	if start == "" {
		return out, nil
	}
	if v, ok, err := p.getLocked(ctx, lo); err != nil { // 用带前缀的键读，与上面的 scan 一致
		return nil, err
	} else if ok {
		if len(out) >= limit {
			out = out[:limit-1]
		}
		out = append([]core.KeyValue{{Key: start, Value: v}}, out...)
	}
	return out, nil
}

func (p *Provider) Expire(ctx context.Context, key string, ttl int64) error {
	if err := p.check(); err != nil {
		return err
	}
	key = p.k(key)
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
	key = p.k(key)
	st, recs, err := p.do(ctx, "ttl", key)
	if err != nil {
		return 0, false, err
	}
	if st != "ok" {
		return 0, false, fmt.Errorf("ssdb: ttl: status %q", st)
	}
	n, err := strconv.ParseInt(string(firstPayload(recs)), 10, 64)
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
	name = p.k(name)
	if name == "" {
		return fmt.Errorf("ssdb: %s: queue name must not be empty", cmd)
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
		return firstPayload(recs), true, nil
	case "not_found":
		return nil, false, nil
	default:
		return nil, false, errFrom([]byte(firstOr(recs, st)))
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
	return strconv.ParseInt(string(firstPayload(recs)), 10, 64)
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
		return firstPayload(recs), true, nil
	case "not_found":
		return nil, false, nil
	default:
		return nil, false, errFrom([]byte(firstOr(recs, st)))
	}
}

// ---- ZSet ----

func (p *Provider) ZSet(ctx context.Context, name, key string, score int64) error {
	if err := p.check(); err != nil {
		return err
	}
	name = p.k(name)
	if name == "" || key == "" {
		return fmt.Errorf("ssdb: zset: zset name and member must not be empty")
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
	name = p.k(name)
	st, recs, err := p.do(ctx, "zget", name, key)
	if err != nil {
		return 0, false, err
	}
	switch st {
	case "ok":
		s, err := strconv.ParseInt(string(firstPayload(recs)), 10, 64)
		return s, err == nil, err
	case "not_found":
		return 0, false, nil
	default:
		return 0, false, errFrom([]byte(firstOr(recs, st)))
	}
}

func (p *Provider) ZDel(ctx context.Context, name, key string) error {
	if err := p.check(); err != nil {
		return err
	}
	name = p.k(name)
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
	name = p.k(name)
	st, recs, err := p.do(ctx, "zsize", name)
	if err != nil {
		return 0, err
	}
	if st != "ok" {
		return 0, fmt.Errorf("ssdb: zsize: status %q", st)
	}
	return strconv.ParseInt(string(firstPayload(recs)), 10, 64)
}

func (p *Provider) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	if err := p.check(); err != nil {
		return 0, false, err
	}
	name = p.k(name)
	st, recs, err := p.do(ctx, "zrank", name, key)
	if err != nil {
		return 0, false, err
	}
	switch st {
	case "ok":
		r, err := strconv.ParseInt(string(firstPayload(recs)), 10, 64)
		return r, err == nil, err
	case "not_found":
		return 0, false, nil
	default:
		return 0, false, errFrom([]byte(firstOr(recs, st)))
	}
}

func (p *Provider) ZRange(ctx context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	name = p.k(name)
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
	size, err := strconv.ParseInt(string(firstPayload(recs)), 10, 64)
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
	name = p.k(name)
	if name == "" || key == "" {
		return 0, fmt.Errorf("ssdb: zincr: zset name and member must not be empty")
	}
	st, recs, err := p.do(ctx, "zincr", name, key, strconv.FormatInt(delta, 10))
	if err != nil {
		return 0, err
	}
	if st != "ok" {
		if errors.Is(errFrom([]byte(firstOr(recs, st))), core.ErrNotInteger) {
			return 0, core.ErrNotInteger
		}
		return 0, fmt.Errorf("ssdb: zincr: status %q", st)
	}
	return strconv.ParseInt(string(firstPayload(recs)), 10, 64)
}

// normalizeLimit 保证 limit<=0 时使用 core.DefaultScanLimit（统一常量）。
func normalizeLimit(limit int) int {
	if limit <= 0 {
		return core.DefaultScanLimit
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
		args, err := p.batchArgs(op)
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
func (p *Provider) batchArgs(op core.BatchOp) ([][]byte, error) {
	// SSDB 对空 key/成员静默不写（set 对空 key 返回 ok 却不落数据），拒绝之。
	switch op.Kind {
	case core.BatchSet, core.BatchSetEx, core.BatchDel, core.BatchExpire,
		core.BatchQPush, core.BatchQPushFront:
		if op.Key == "" {
			return nil, fmt.Errorf("ssdb: batch op %d: key must not be empty", op.Kind)
		}
	case core.BatchZSet, core.BatchZDel, core.BatchZIncr:
		if op.Key == "" || op.Member == "" {
			return nil, fmt.Errorf("ssdb: batch op %d: zset name and member must not be empty", op.Kind)
		}
	}
	b := func(parts ...string) [][]byte {
		out := make([][]byte, len(parts))
		for i, s := range parts {
			out[i] = []byte(s)
		}
		return out
	}
	switch op.Kind {
	case core.BatchSet:
		return [][]byte{[]byte("set"), []byte(p.k(op.Key)), op.Value}, nil
	case core.BatchSetEx:
		if op.TTL <= 0 {
			return nil, core.ErrInvalidTTL
		}
		return [][]byte{[]byte("setx"), []byte(p.k(op.Key)), op.Value, []byte(strconv.FormatInt(op.TTL, 10))}, nil
	case core.BatchDel:
		return b("del", p.k(op.Key)), nil
	case core.BatchExpire:
		if op.TTL <= 0 {
			return nil, core.ErrInvalidTTL
		}
		return b("expire", p.k(op.Key), strconv.FormatInt(op.TTL, 10)), nil
	case core.BatchQPush:
		return [][]byte{[]byte("qpush"), []byte(p.k(op.Key)), op.Value}, nil
	case core.BatchQPushFront:
		return [][]byte{[]byte("qpush_front"), []byte(p.k(op.Key)), op.Value}, nil
	case core.BatchZSet:
		return b("zset", p.k(op.Key), op.Member, strconv.FormatInt(op.Score, 10)), nil
	case core.BatchZDel:
		return b("zdel", p.k(op.Key), op.Member), nil
	case core.BatchZIncr:
		return b("zincr", p.k(op.Key), op.Member, strconv.FormatInt(op.Delta, 10)), nil
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
