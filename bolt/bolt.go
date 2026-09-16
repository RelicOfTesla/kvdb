// Package bolt 提供以 bbolt（纯 Go 嵌入式 B+tree KV 库）为基座的实现，
// 覆盖 KV + Queue + ZSet + Batch 全部能力。bbolt 单写者、多读者（MVCC），
// 每次 Update 即一次事务提交：单条写一次提交，批写整批一次提交。
package bolt

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/RelicOfTesla/kvdb/core"
)

var _ core.FullProvider = (*Provider)(nil)

// DefaultSyncInterval 是缺省档（Sync=false）下周期落盘的间隔：距上次落盘超过
// 这么久且期间有写入，就把脏页交给 OS（不再等下一次写、也不等 Close）。
const DefaultSyncInterval = time.Second

// Config 控制打开行为。
type Config struct {
	// Timeout 是获取文件锁的最长等待时间；0 表示无限等待。
	Timeout time.Duration
	// Sync 为 true 时每次提交都 fsync：进程/机器崩溃不丢已确认写入，吞吐显著下降。
	// 默认 false（不 fsync）：高速模式，进程崩溃或断电可能丢最近已提交事务
	// —— 与 jsonl/leveldb/badger 基座的 Sync 选项同极性。
	//
	// 注意缺省档**不是"永不落盘"**：由 SyncInterval 兜底周期落盘（见下），
	// 且 Close 前会强制落盘一次。它放弃的只是"每次提交都保证落盘"。
	Sync bool
	// SyncInterval 是缺省档（Sync=false）的周期落盘间隔；<=0 用 DefaultSyncInterval。
	// 仅在 Sync=false 时生效（Sync=true 时每次提交本来就 fsync）。
	SyncInterval time.Duration
}

// 桶布局：KV 与 TTL 分开存放（Set 不改变已有 TTL，对齐 SSDB 语义）；
// 队列与 zset 用「长度前缀名 + 大端序号/分数」的复合键，以便按名前缀扫描。
var (
	bKV  = []byte("kv")  // key -> value
	bTTL = []byte("kvt") // key -> uint64BE(过期时间)
	bQ   = []byte("q")   // lp(name)+be64(ordered(seq)) -> value
	bQS  = []byte("qs")  // lp(name) -> next|front|count
	bZ   = []byte("z")   // lp(name)+be64(ordered(score))+member -> nil
	bZM  = []byte("zm")  // lp(name)+member -> be64(ordered(score))
	bZC  = []byte("zc")  // lp(name) -> uint64BE(count)
)

// Provider 是 bbolt 基座。
type Provider struct {
	db        *bolt.DB
	path      string
	closed    atomic.Bool
	lastSweep atomic.Int64 // 上次过期回收时刻（unix 秒），节流写事务内的清理
	// 缺省档（Sync=false）的周期落盘状态。syncOn 为 false 时下面这些不参与。
	syncOn   bool
	interval time.Duration // 周期落盘间隔
	lastSync atomic.Int64  // 上次落盘时刻（unix 纳秒；间隔可配到亚秒级）
	idle     *time.Timer   // 空闲兜底：长时间无写入也把脏页交出去
	// syncMu 串行化"落盘/关闭"：db.Sync() 与 db.Close() 都操作底层 fd，
	// 并发调用是数据竞争（bbolt 只保证事务级并发安全，不覆盖这两个）。
	// 空闲回调必须与 Close 互斥，否则会出现"一边 fdatasync、一边关 fd"。
	syncMu sync.Mutex
	// pending 记录落盘失败的粘性错误：后台回调里失败不能在后台吞掉，
	// 下一次写或 Close 必须把它报给调用方。
	pending atomic.Pointer[error]
}

// Open 打开（不存在则创建）数据库文件。
func Open(ctx context.Context, path string, cfg Config) (*Provider, error) {
	_ = ctx
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: cfg.Timeout, NoSync: !cfg.Sync})
	if err != nil {
		return nil, fmt.Errorf("bolt: open %s: %w", path, err)
	}
	p := &Provider{db: db, path: path}
	p.syncOn = !cfg.Sync
	if p.syncOn {
		p.interval = cfg.SyncInterval
		if p.interval <= 0 {
			p.interval = DefaultSyncInterval
		}
	}
	if err := p.init(); err != nil {
		db.Close()
		return nil, err
	}
	now := core.Now()
	// init 已做过一次全量清理，节流起点从现在起算。
	p.lastSweep.Store(now.Unix())
	if p.syncOn {
		// 落盘起点在 init 之后：init 自己的写入已由这次初始 Sync 覆盖。
		if err := db.Sync(); err != nil {
			db.Close()
			return nil, fmt.Errorf("bolt: initial sync: %w", err)
		}
		p.lastSync.Store(now.UnixNano())
		// 空闲兜底计时器：初值设很大，等第一次写入时 Reset 到 interval，
		// 这样"没有写入"就不会产生任何唤醒（也就没有空转开销）。
		p.idle = time.AfterFunc(time.Hour, p.syncIdle)
	}
	return p, nil
}

// syncIfDue 在缺省档下按 SyncInterval 节流落盘；由写路径调用，空转时零开销。
// 顺带把上一次后台落盘的失败报出来，避免它被静默吞掉。
// 只在写事务**结束之后**取 syncMu，因此不会与 Close 等待事务形成死锁。
func (p *Provider) syncIfDue() error {
	if !p.syncOn {
		return nil
	}
	if err := p.takePending(); err != nil {
		return err
	}
	// 先把空闲兜底推到"从现在起 interval 之后"：它的语义是"距最后一次写入
	// interval 仍无新写入就落盘"。必须在节流判断之前做——否则在周期未到时
	// 提前返回，计时器会一直停在 Open 时设的初值（等于永不空闲落盘）。
	if p.idle != nil {
		p.idle.Reset(p.interval)
	}
	now := core.Now()
	if now.UnixNano()-p.lastSync.Load() < int64(p.interval) {
		return nil
	}
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	// 等锁期间可能已被 Close：此时跳过落盘（Close 自己会做最后一次 Sync）。
	if p.closed.Load() {
		return nil
	}
	// 先记时间戳再落盘：即便本次落盘失败也不必立刻重试（避免出错时每次写都
	// 触发一次失败的 fsync 拖慢写路径），失败会由上面/Close 上报。
	p.lastSync.Store(now.UnixNano())
	if err := p.db.Sync(); err != nil {
		return fmt.Errorf("bolt: periodic sync: %w", err)
	}
	return nil
}

// syncIdle 是空闲兜底回调：连续 interval 没有写入也把脏页交给 OS。
// 失败必须留痕（后台不能吞），由后续写或 Close 上报。
// 全程持 syncMu：与 Close 的"关 fd"互斥，避免对同一 fd 并发 Sync/Close。
func (p *Provider) syncIdle() {
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	if p.closed.Load() {
		return
	}
	if err := p.db.Sync(); err != nil {
		p.setPending(fmt.Errorf("bolt: idle sync: %w", err))
		return
	}
	p.lastSync.Store(core.Now().UnixNano())
	p.idle.Reset(p.interval)
}

// setPending 记录落盘失败的粘性错误（只保留第一个）。
func (p *Provider) setPending(err error) {
	if err == nil {
		return
	}
	p.pending.CompareAndSwap(nil, &err)
}

// takePending 取出并清空粘性错误。
func (p *Provider) takePending() error {
	if pe := p.pending.Swap(nil); pe != nil {
		return *pe
	}
	return nil
}

// OpenURI 解析 bolt://<path>?sync=1&timeout=5s&sync_interval=1s。
// 缺省档（不写 sync=1）不逐提交 fsync，但按 sync_interval（缺省 1s）周期落盘，
// 且 Close 前强制落盘一次。
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	p := u.Path
	if u.Host != "" {
		p = strings.TrimPrefix(u.Host+u.Path, "/")
	}
	var cfg Config
	if v := u.Query().Get("sync"); v == "1" || v == "true" {
		cfg.Sync = true
	}
	if v, ok := u.Query()["nosync"]; ok {
		// 旧参数已废弃：本基座默认即不 fsync，nosync=1 已无意义；而它的字面含义
		// 会让使用者误以为"只有写了 nosync 才高速"，这里直接报错点明。
		return nil, fmt.Errorf("bolt: nosync 参数已废弃（默认即不 fsync，去掉了 %q）；如需逐提交 fsync 请用 sync=1", v)
	}
	if t := u.Query().Get("timeout"); t != "" {
		// 必须报错而不是静默忽略：Timeout 留 0 意味着 bbolt 获取文件锁
		// 无限等待（"timeout=5" 这类无单位写法会被 ParseDuration 拒绝）。
		d, err := time.ParseDuration(t)
		if err != nil {
			return nil, fmt.Errorf("bolt: bad timeout %q: %w", t, err)
		}
		cfg.Timeout = d
	}
	if s := u.Query().Get("sync_interval"); s != "" {
		// 同样必须报错而不是静默忽略：写错单位会让周期落盘退化成默认值，
		// 而使用者以为自己已经调过（这是"以为安全其实没有"的一类误解）。
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, fmt.Errorf("bolt: bad sync_interval %q: %w", s, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("bolt: sync_interval must be positive, got %q", s)
		}
		cfg.SyncInterval = d
	}
	return Open(ctx, p, cfg)
}

// Bolt 返回底层句柄（备份、统计等高级用途）。
func (p *Provider) Bolt() *bolt.DB { return p.db }

// Path 返回数据库文件路径。
func (p *Provider) Path() string { return p.path }

func (p *Provider) init() error {
	return p.db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bKV, bTTL, bQ, bQS, bZ, bZM, bZC} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("bolt: create bucket %s: %w", name, err)
			}
		}
		// 打开时清理已过期项（运行期读取路径同样会过滤）。
		return cleanupExpired(tx, core.NowUnix())
	})
}

// Close 关闭数据库。缺省档（Sync=false）下 bbolt 的 Close 只 munmap + 关 fd、
// **不做 fdatasync**，因此这里必须补一次 Sync，否则正常关闭也可能留下未落盘的
// 已提交事务（进程安全退出却丢数据，是最不该出现的一类丢失）。
func (p *Provider) Close() error {
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	if p.closed.Swap(true) {
		return nil
	}
	if p.idle != nil {
		// Stop 返回 false 说明回调已在跑：它会阻塞在 syncMu 上，等我们放开锁后
		// 看到 closed=true 直接返回，不会碰已关闭的 fd。
		p.idle.Stop()
	}
	var err error
	if p.syncOn {
		// 先报后台/周期落盘留下的失败（真实丢数据信号），再补最后一次落盘。
		err = p.takePending()
		if serr := p.db.Sync(); serr != nil && err == nil {
			err = fmt.Errorf("bolt: close sync: %w", serr)
		}
	}
	if cerr := p.db.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

func (p *Provider) check() error {
	if p.closed.Load() {
		return core.ErrClosed
	}
	return nil
}

// IncrWraps 能力声明：bolt 的 Incr 按 int64 回绕（kvIncrTx 不做溢出检查），
// 见 core.IncrWrapsProvider 与 Caps.IncrWraps。
func (p *Provider) IncrWraps() bool { return true }

var _ core.IncrWrapsProvider = (*Provider)(nil)

func (p *Provider) view(fn func(*bolt.Tx) error) error {
	if err := p.check(); err != nil {
		return err
	}
	return p.db.View(fn)
}

func (p *Provider) update(fn func(*bolt.Tx) error) error {
	if err := p.check(); err != nil {
		return err
	}
	err := p.db.Update(func(tx *bolt.Tx) error {
		// 写事务开头按节流回收过期条目：TTL 磨损负载下过期键可能不再被
		// 任何写触碰，挂在写事务上保证长期运行进程的磁盘占用有界
		//（Open 时另有一次全量清理）。清理失败回滚整个事务，可重试。
		now := core.NowUnix()
		if now-p.lastSweep.Load() >= core.SweepInterval {
			p.lastSweep.Store(now)
			if err := cleanupExpired(tx, now); err != nil {
				return err
			}
		}
		return fn(tx)
	})
	if err != nil {
		return err
	}
	// 缺省档按 SyncInterval 周期落盘：bbolt 在 NoSync 下提交只写页缓存，
	// 这里保证"有写入就会在有界时间内落盘"，而不是攒到 Close。
	return p.syncIfDue()
}

// ---- 键编码 ----

// 二进制布局常量。复合键/记录里所有整数一律 8 字节大端（见 be64/ordered），
// 队列计数器是三个连续的 be64。解析处一律引用这些名字而不是字面量：
// 改布局时只需改这里，避免"某一处漏改"导致静默错读。
const (
	be64Len = 8 // 大端 int64/uint64 的字节数

	// qCounters 记录布局：next | front | count，各占一个 be64。
	qCountersLen = 3 * be64Len
	qNextOff     = 0
	qFrontOff    = be64Len
	qCountOff    = 2 * be64Len
)

// lp 为变长名加 uvarint 长度前缀，使复合键可按名字做前缀扫描。
func lp(name string) []byte {
	out := make([]byte, binary.MaxVarintLen64+len(name))
	n := binary.PutUvarint(out, uint64(len(name)))
	copy(out[n:], name)
	return out[:n+len(name)]
}

// signBit 是 int64 的符号位：ordered/unorder 通过翻转它把有符号整数映射为
// 可按字节序比较的无符号整数（两个方向都必须翻转同一位，故提取为常量）。
const signBit uint64 = 1 << 63

// ordered 把 int64 映射为可按字节序比较的 uint64（翻转符号位）。
func ordered(v int64) uint64 { return uint64(v) ^ signBit }

func unorder(u uint64) int64 { return int64(u ^ signBit) }

func be64(u uint64) []byte {
	var b [be64Len]byte
	binary.BigEndian.PutUint64(b[:], u)
	return b[:]
}

func cat(parts ...[]byte) []byte {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// prefixEnd 返回前缀的字典序上界（nil 表示无上界）。
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func ttlExpired(tx *bolt.Tx, key []byte, now int64) bool {
	v := tx.Bucket(bTTL).Get(key)
	if len(v) != be64Len {
		// 无 TTL（nil）或记录长度异常：按未过期处理（fail-open，保持可读）。
		return false
	}
	return int64(binary.BigEndian.Uint64(v)) <= now
}

func cleanupExpired(tx *bolt.Tx, now int64) error {
	kvt, kk := tx.Bucket(bTTL), tx.Bucket(bKV)
	var expired [][]byte
	c := kvt.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if len(v) != be64Len {
			continue // 记录异常的 TTL 不参与清理
		}
		if int64(binary.BigEndian.Uint64(v)) <= now {
			expired = append(expired, append([]byte(nil), k...))
		}
	}
	for _, k := range expired {
		if err := kvt.Delete(k); err != nil {
			return err
		}
		if err := kk.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// ---- KV（事务内实现，单条与批写共用）----

// kvSetTx 写入值：保留既有**未过期** TTL；已过期的 TTL 必须先删除，
// 否则 Set 成功了键却仍然读不到（过期判定在 Get/Exists 侧）。同一事务内完成，
// 与并发读写互斥。传入 now 便于批量操作共享同一时刻。
func kvSetTx(tx *bolt.Tx, key string, value []byte, now int64) error {
	k := []byte(key)
	if ttlExpired(tx, k, now) {
		if err := tx.Bucket(bTTL).Delete(k); err != nil {
			return err
		}
	}
	return tx.Bucket(bKV).Put(k, value)
}

func kvSetExTx(tx *bolt.Tx, key string, value []byte, ttl int64) error {
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if err := tx.Bucket(bKV).Put([]byte(key), value); err != nil {
		return err
	}
	// AddTTL 饱和：now+ttl 溢出为负再经 uint64 编码会被所有读者判"已过期"。
	return tx.Bucket(bTTL).Put([]byte(key), be64(uint64(core.AddTTL(core.NowUnix(), ttl))))
}

func kvDelTx(tx *bolt.Tx, key string) error {
	if err := tx.Bucket(bKV).Delete([]byte(key)); err != nil {
		return err
	}
	return tx.Bucket(bTTL).Delete([]byte(key))
}

// kvExpireAtTx 把 key 的到期时刻设为 at（unix 秒）；at 已过去则删除该 key
// （与 Redis EXPIREAT 一致）。key 不存在/已过期时不处理（不视为错误）。
func kvExpireAtTx(tx *bolt.Tx, key string, at int64, now int64) error {
	k := []byte(key)
	if tx.Bucket(bKV).Get(k) == nil || ttlExpired(tx, k, now) {
		return nil
	}
	if at <= now {
		return kvDelTx(tx, key)
	}
	return tx.Bucket(bTTL).Put(k, be64(uint64(at)))
}

// kvExpireTx 设置 TTL；key 不存在**或已过期**时按不存在处理（不"复活"过期键）。
func kvExpireTx(tx *bolt.Tx, key string, ttl int64, now int64) error {
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	k := []byte(key)
	if tx.Bucket(bKV).Get(k) == nil || ttlExpired(tx, k, now) {
		return nil
	}
	return tx.Bucket(bTTL).Put(k, be64(uint64(core.AddTTL(now, ttl))))
}

// kvIncrTx 原子累加：已过期的键按不存在处理（从 0 起算并清除旧 TTL），
// 未过期的键保留原 TTL（SSDB incr 不改 ttl 表）。
func kvIncrTx(tx *bolt.Tx, key string, delta int64, now int64) (int64, error) {
	k := []byte(key)
	b := tx.Bucket(bKV)
	expired := ttlExpired(tx, k, now)
	var cur int64
	if v := b.Get(k); v != nil && !expired {
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, core.ErrNotInteger
		}
		cur = n
	}
	if expired {
		if err := tx.Bucket(bTTL).Delete(k); err != nil {
			return 0, err
		}
	}
	cur += delta
	if err := b.Put(k, []byte(strconv.FormatInt(cur, 10))); err != nil {
		return 0, err
	}
	return cur, nil
}

// ---- Queue ----
//
// 序号模型与 sqlstore 一致：next 递增（队尾追加），front 递减（队头插入），
// 复合键用「符号翻转的大端整数」编码，使字节序即序号顺序；count 让 QSize 为 O(1)。

type qCounters struct {
	next  int64  // 队尾下一个序号
	front int64  // 队头下一个序号（先 -- 再使用）
	count uint64 // 元素数
}

func qCountersGet(tx *bolt.Tx, name string) qCounters {
	v := tx.Bucket(bQS).Get(lp(name))
	if len(v) != qCountersLen {
		return qCounters{}
	}
	return qCounters{
		next:  int64(binary.BigEndian.Uint64(v[qNextOff:qFrontOff])),
		front: int64(binary.BigEndian.Uint64(v[qFrontOff:qCountOff])),
		count: binary.BigEndian.Uint64(v[qCountOff:qCountersLen]),
	}
}

func qCountersPut(tx *bolt.Tx, name string, c qCounters) error {
	buf := make([]byte, qCountersLen)
	binary.BigEndian.PutUint64(buf[qNextOff:qFrontOff], uint64(c.next))
	binary.BigEndian.PutUint64(buf[qFrontOff:qCountOff], uint64(c.front))
	binary.BigEndian.PutUint64(buf[qCountOff:qCountersLen], c.count)
	return tx.Bucket(bQS).Put(lp(name), buf)
}

func qKey(name string, seq int64) []byte { return cat(lp(name), be64(ordered(seq))) }

func qPushTx(tx *bolt.Tx, name string, value []byte, front bool) error {
	c := qCountersGet(tx, name)
	var seq int64
	if front {
		c.front--
		seq = c.front
	} else {
		seq = c.next
		c.next++
	}
	c.count++
	if err := tx.Bucket(bQ).Put(qKey(name, seq), value); err != nil {
		return err
	}
	return qCountersPut(tx, name, c)
}

// qSeekTx 定位队头/队尾元素并返回其上的 cursor（便于就地删除）。
func qSeekTx(tx *bolt.Tx, name string, back bool) (*bolt.Cursor, []byte, []byte) {
	prefix := lp(name)
	c := tx.Bucket(bQ).Cursor()
	var k, v []byte
	if back {
		if end := prefixEnd(prefix); end != nil {
			k, v = c.Seek(end)
			if k == nil {
				k, v = c.Last()
			} else {
				k, v = c.Prev()
			}
		} else {
			k, v = c.Last()
		}
	} else {
		k, v = c.Seek(prefix)
	}
	if k == nil || !bytes.HasPrefix(k, prefix) {
		return c, nil, nil
	}
	return c, k, v
}

func qPeekTx(tx *bolt.Tx, name string, back bool) ([]byte, bool) {
	_, _, v := qSeekTx(tx, name, back)
	if v == nil {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

func qPopTx(tx *bolt.Tx, name string, back bool) ([]byte, bool, error) {
	c, k, v := qSeekTx(tx, name, back)
	if k == nil {
		return nil, false, nil
	}
	out := append([]byte(nil), v...)
	if err := c.Delete(); err != nil { // 就地删除当前元素
		return nil, false, err
	}
	cs := qCountersGet(tx, name)
	if cs.count > 0 {
		cs.count--
	}
	if cs.count == 0 {
		// 队列排空：删除计数记录，防止队列命名 churn 永久泄漏
		//（与 zset 的 zc 记录在清空时删除保持一致；计数从 0 重启无碰撞，
		// 因为旧元素已全部删除）。
		if err := tx.Bucket(bQS).Delete(lp(name)); err != nil {
			return nil, false, err
		}
		return out, true, nil
	}
	if err := qCountersPut(tx, name, cs); err != nil {
		return nil, false, err
	}
	return out, true, nil
}

// ---- ZSet ----
//
// 两个索引：z 按 (score, member) 有序（支持范围/排名），zm 按 member 查分数。
// score 同样做符号翻转，保证 int64 分数按字节序正确排序。

func zOrderedKey(name string, score int64, member string) []byte {
	return cat(lp(name), be64(ordered(score)), []byte(member))
}

func zMemberKey(name, member string) []byte { return cat(lp(name), []byte(member)) }

func zScoreGet(tx *bolt.Tx, name, member string) (int64, bool) {
	v := tx.Bucket(bZM).Get(zMemberKey(name, member))
	if v == nil {
		return 0, false
	}
	return unorder(binary.BigEndian.Uint64(v)), true
}

func zCountGet(tx *bolt.Tx, name string) int64 {
	v := tx.Bucket(bZC).Get(lp(name))
	if len(v) != be64Len {
		return 0 // 缺失或记录异常按 0 处理
	}
	return int64(binary.BigEndian.Uint64(v))
}

func zCountAdd(tx *bolt.Tx, name string, d int64) error {
	n := zCountGet(tx, name) + d
	if n <= 0 {
		return tx.Bucket(bZC).Delete(lp(name))
	}
	return tx.Bucket(bZC).Put(lp(name), be64(uint64(n)))
}

func zPutScoreTx(tx *bolt.Tx, name, member string, score int64) error {
	if err := tx.Bucket(bZM).Put(zMemberKey(name, member), be64(ordered(score))); err != nil {
		return err
	}
	return tx.Bucket(bZ).Put(zOrderedKey(name, score, member), nil)
}

func zSetTx(tx *bolt.Tx, name, member string, score int64) error {
	old, exists := zScoreGet(tx, name, member)
	if exists {
		if old == score {
			return nil
		}
		if err := tx.Bucket(bZ).Delete(zOrderedKey(name, old, member)); err != nil {
			return err
		}
	}
	if err := zPutScoreTx(tx, name, member, score); err != nil {
		return err
	}
	if !exists {
		return zCountAdd(tx, name, 1)
	}
	return nil
}

func zDelTx(tx *bolt.Tx, name, member string) error {
	score, ok := zScoreGet(tx, name, member)
	if !ok {
		return nil
	}
	if err := tx.Bucket(bZM).Delete(zMemberKey(name, member)); err != nil {
		return err
	}
	if err := tx.Bucket(bZ).Delete(zOrderedKey(name, score, member)); err != nil {
		return err
	}
	return zCountAdd(tx, name, -1)
}

func zIncrTx(tx *bolt.Tx, name, member string, delta int64) (int64, error) {
	old, ok := zScoreGet(tx, name, member)
	score := old + delta
	if ok {
		if err := tx.Bucket(bZ).Delete(zOrderedKey(name, old, member)); err != nil {
			return 0, err
		}
	}
	if err := zPutScoreTx(tx, name, member, score); err != nil {
		return 0, err
	}
	if !ok {
		if err := zCountAdd(tx, name, 1); err != nil {
			return 0, err
		}
	}
	return score, nil
}

func zRangeTx(tx *bolt.Tx, name string, start, stop int64) ([]core.ZItem, error) {
	size := zCountGet(tx, name)
	if start < 0 {
		start = size + start
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop = size + stop
	}
	if size == 0 || start > stop || start >= size {
		return []core.ZItem{}, nil
	}
	if stop >= size {
		stop = size - 1
	}
	prefix := lp(name)
	c := tx.Bucket(bZ).Cursor()
	out := make([]core.ZItem, 0, stop-start+1)
	idx := int64(0)
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		if idx > stop {
			break
		}
		if idx >= start {
			rest := k[len(prefix):]
			if len(rest) < be64Len {
				return nil, fmt.Errorf("bolt: corrupt zset key")
			}
			out = append(out, core.ZItem{
				Key:   string(rest[be64Len:]),
				Score: unorder(binary.BigEndian.Uint64(rest[:be64Len])),
			})
		}
		idx++
	}
	return out, nil
}

// zRankTx 通过有序索引计数得到名次（O(rank)）。
func zRankTx(tx *bolt.Tx, name, member string) (int64, bool) {
	score, ok := zScoreGet(tx, name, member)
	if !ok {
		return 0, false
	}
	target := zOrderedKey(name, score, member)
	prefix := lp(name)
	c := tx.Bucket(bZ).Cursor()
	var rank int64
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		if bytes.Equal(k, target) {
			return rank, true
		}
		rank++
	}
	return 0, false
}

// ---- KV 公开方法 ----

func (p *Provider) Set(ctx context.Context, key string, value []byte) error {
	_ = ctx
	// now 在事务内采样：等待 bbolt 单写者锁期间若跨过键的过期点，
	// 事务外的陈旧 now 会让 kvSetTx 保留已失效的 TTL（写成功却不可见）。
	return p.update(func(tx *bolt.Tx) error {
		return kvSetTx(tx, key, value, core.NowUnix())
	})
}

func (p *Provider) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	_ = ctx
	return p.update(func(tx *bolt.Tx) error { return kvSetExTx(tx, key, value, ttl) })
}

// SetExAt 写入 value 并让 key 在 at（unix 秒）过期；at 已过去则删除该 key。
func (p *Provider) SetExAt(ctx context.Context, key string, value []byte, at int64) error {
	_ = ctx
	return p.update(func(tx *bolt.Tx) error {
		now := core.NowUnix()
		if at <= now {
			return kvDelTx(tx, key)
		}
		if err := tx.Bucket(bKV).Put([]byte(key), value); err != nil {
			return err
		}
		return tx.Bucket(bTTL).Put([]byte(key), be64(uint64(at)))
	})
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	_ = ctx
	var out []byte
	var ok bool
	err := p.view(func(tx *bolt.Tx) error {
		now := core.NowUnix()
		k := []byte(key)
		v := tx.Bucket(bKV).Get(k)
		if v == nil || ttlExpired(tx, k, now) {
			return nil
		}
		out, ok = append([]byte(nil), v...), true
		return nil
	})
	return out, ok, err
}

func (p *Provider) Del(ctx context.Context, key string) error {
	_ = ctx
	return p.update(func(tx *bolt.Tx) error { return kvDelTx(tx, key) })
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	_ = ctx
	var ok bool
	err := p.view(func(tx *bolt.Tx) error {
		now := core.NowUnix()
		k := []byte(key)
		v := tx.Bucket(bKV).Get(k)
		ok = v != nil && !ttlExpired(tx, k, now)
		return nil
	})
	return ok, err
}

func (p *Provider) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	_ = ctx
	var out int64
	err := p.update(func(tx *bolt.Tx) error {
		n, err := kvIncrTx(tx, key, delta, core.NowUnix())
		if err != nil {
			return err
		}
		out = n
		return nil
	})
	return out, err
}

func (p *Provider) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	_ = ctx
	out := make(map[string][]byte, len(keys))
	err := p.view(func(tx *bolt.Tx) error {
		now := core.NowUnix()
		b := tx.Bucket(bKV)
		for _, key := range keys {
			k := []byte(key)
			v := b.Get(k)
			if v == nil || ttlExpired(tx, k, now) {
				continue
			}
			out[key] = append([]byte(nil), v...)
		}
		return nil
	})
	return out, err
}

func (p *Provider) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	_ = ctx
	limit = normalizeLimit(limit)
	var out []core.KeyValue
	err := p.view(func(tx *bolt.Tx) error {
		now := core.NowUnix()
		c := tx.Bucket(bKV).Cursor()
		var k, v []byte
		if start != "" {
			k, v = c.Seek([]byte(start))
		} else {
			k, v = c.First()
		}
		for ; k != nil; k, v = c.Next() {
			if end != "" && string(k) > end {
				break
			}
			if ttlExpired(tx, k, now) {
				continue
			}
			out = append(out, core.KeyValue{Key: string(k), Value: append([]byte(nil), v...)})
			if len(out) >= limit {
				break
			}
		}
		return nil
	})
	return out, err
}

func (p *Provider) Expire(ctx context.Context, key string, ttl int64) error {
	_ = ctx
	return p.update(func(tx *bolt.Tx) error { return kvExpireTx(tx, key, ttl, core.NowUnix()) })
}

// ExpireAt 让 key 在 at（unix 秒）过期；at 已过去则立即删除。
func (p *Provider) ExpireAt(ctx context.Context, key string, at int64) error {
	_ = ctx
	return p.update(func(tx *bolt.Tx) error { return kvExpireAtTx(tx, key, at, core.NowUnix()) })
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	_ = ctx
	var secs int64
	var ok bool
	err := p.view(func(tx *bolt.Tx) error {
		now := core.NowUnix()
		v := tx.Bucket(bTTL).Get([]byte(key))
		if v == nil || len(v) != be64Len {
			return nil
		}
		exp := int64(binary.BigEndian.Uint64(v))
		if exp <= now || tx.Bucket(bKV).Get([]byte(key)) == nil {
			return nil
		}
		secs, ok = exp-now, true
		return nil
	})
	if !ok {
		return -1, false, err
	}
	return secs, true, err
}

// ---- Queue 公开方法 ----

func (p *Provider) QPush(ctx context.Context, name string, value []byte) error {
	_ = ctx
	return p.update(func(tx *bolt.Tx) error { return qPushTx(tx, name, value, false) })
}

func (p *Provider) QPushFront(ctx context.Context, name string, value []byte) error {
	_ = ctx
	return p.update(func(tx *bolt.Tx) error { return qPushTx(tx, name, value, true) })
}

func (p *Provider) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpop(ctx, name, false)
}

func (p *Provider) QPopBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpop(ctx, name, true)
}

func (p *Provider) qpop(ctx context.Context, name string, back bool) ([]byte, bool, error) {
	_ = ctx
	var out []byte
	var ok bool
	err := p.update(func(tx *bolt.Tx) error {
		v, got, err := qPopTx(tx, name, back)
		if err != nil {
			return err
		}
		out, ok = v, got
		return nil
	})
	return out, ok, err
}

func (p *Provider) QSize(ctx context.Context, name string) (int64, error) {
	_ = ctx
	var n int64
	err := p.view(func(tx *bolt.Tx) error {
		n = int64(qCountersGet(tx, name).count)
		return nil
	})
	return n, err
}

func (p *Provider) QFront(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpeek(ctx, name, false)
}

func (p *Provider) QBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpeek(ctx, name, true)
}

func (p *Provider) qpeek(ctx context.Context, name string, back bool) ([]byte, bool, error) {
	_ = ctx
	var out []byte
	var ok bool
	err := p.view(func(tx *bolt.Tx) error {
		v, got := qPeekTx(tx, name, back)
		out, ok = v, got
		return nil
	})
	return out, ok, err
}

// ---- ZSet 公开方法 ----

func (p *Provider) ZSet(ctx context.Context, name, key string, score int64) error {
	_ = ctx
	return p.update(func(tx *bolt.Tx) error { return zSetTx(tx, name, key, score) })
}

func (p *Provider) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	_ = ctx
	var score int64
	var ok bool
	err := p.view(func(tx *bolt.Tx) error {
		score, ok = zScoreGet(tx, name, key)
		return nil
	})
	return score, ok, err
}

func (p *Provider) ZDel(ctx context.Context, name, key string) error {
	_ = ctx
	return p.update(func(tx *bolt.Tx) error { return zDelTx(tx, name, key) })
}

func (p *Provider) ZSize(ctx context.Context, name string) (int64, error) {
	_ = ctx
	var n int64
	err := p.view(func(tx *bolt.Tx) error {
		n = zCountGet(tx, name)
		return nil
	})
	return n, err
}

func (p *Provider) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	_ = ctx
	var rank int64
	var ok bool
	err := p.view(func(tx *bolt.Tx) error {
		rank, ok = zRankTx(tx, name, key)
		return nil
	})
	return rank, ok, err
}

func (p *Provider) ZRange(ctx context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	_ = ctx
	var out []core.ZItem
	err := p.view(func(tx *bolt.Tx) error {
		items, err := zRangeTx(tx, name, start, stop)
		if err != nil {
			return err
		}
		out = items
		return nil
	})
	return out, err
}

func (p *Provider) ZIncr(ctx context.Context, name, key string, delta int64) (int64, error) {
	_ = ctx
	var score int64
	err := p.update(func(tx *bolt.Tx) error {
		s, err := zIncrTx(tx, name, key, delta)
		if err != nil {
			return err
		}
		score = s
		return nil
	})
	return score, err
}

// ---- Batch ----

// ApplyBatch 在**一个 bbolt 事务**内按序执行整批操作：一次提交、一次 fsync；
// 任一步失败即整体回滚（原子）。批内均为无条件写。
// BatchComposed 恒为 true：本基座在同一把锁/同一事务内逐条应用批操作，
// 批内后续操作能看到前序效果（见 core.BatchComposedProvider）。
func (p *Provider) BatchComposed() bool { return true }

var _ core.BatchComposedProvider = (*Provider)(nil)

func (p *Provider) ApplyBatch(ctx context.Context, ops []core.BatchOp) error {
	_ = ctx
	if len(ops) == 0 {
		return nil
	}
	for _, op := range ops {
		switch op.Kind {
		case core.BatchSetEx, core.BatchExpire:
			if op.TTL <= 0 {
				return core.ErrInvalidTTL
			}
		}
	}
	return p.update(func(tx *bolt.Tx) error {
		now := core.NowUnix() // 整批共享同一"当前时刻"
		for i, op := range ops {
			var err error
			switch op.Kind {
			case core.BatchSet:
				err = kvSetTx(tx, op.Key, op.Value, now)
			case core.BatchSetEx:
				err = kvSetExTx(tx, op.Key, op.Value, op.TTL)
			case core.BatchDel:
				err = kvDelTx(tx, op.Key)
			case core.BatchExpire:
				err = kvExpireTx(tx, op.Key, op.TTL, now)
			case core.BatchQPush:
				err = qPushTx(tx, op.Key, op.Value, false)
			case core.BatchQPushFront:
				err = qPushTx(tx, op.Key, op.Value, true)
			case core.BatchZSet:
				err = zSetTx(tx, op.Key, op.Member, op.Score)
			case core.BatchZDel:
				err = zDelTx(tx, op.Key, op.Member)
			case core.BatchZIncr:
				_, err = zIncrTx(tx, op.Key, op.Member, op.Delta)
			default:
				err = fmt.Errorf("bolt: unknown batch op %d", op.Kind)
			}
			if err != nil {
				return fmt.Errorf("bolt: batch op %d (kind %d): %w", i, op.Kind, err)
			}
		}
		return nil
	})
}

// normalizeLimit 保证 limit<=0 时使用 core.DefaultScanLimit（统一常量，避免散弹式修改）。
func normalizeLimit(limit int) int {
	if limit <= 0 {
		return core.DefaultScanLimit
	}
	return limit
}
