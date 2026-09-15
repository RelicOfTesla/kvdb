// Package badger 提供以 Badger（dgraph-io/badger，纯 Go 的 LSM-tree KV）为后端的基座，
// 实现 KV + Queue + ZSet + Batch 全部能力。
//
// # 与 bolt / leveldb 基座的关系
//
// 三者都是进程内嵌入式 KV，键空间布局（命名空间标签 + uvarint 长度前缀的名字 +
// 大端有序整数）与队列/zset 的"计数器 + 双侧索引"模型完全一致，便于横向对比。
// 差别在底座机制：
//
//	bolt    单写事务 + B+tree，读走 mmap
//	leveldb 无事务，靠单次原子 Write(batch)
//	badger  有 MVCC 事务（SSI）：本基座的多键写直接用一个 db.Update 事务提交
//
// # 键空间布局
//
//	'k' + lp(key)                        -> value            （KV 值）
//	't' + lp(key)                        -> be64(过期秒)      （KV TTL，独立于值）
//	'q' + lp(name) + be64(ordered(seq)) -> value            （队列元素）
//	's' + lp(name)                       -> qCounters        （队列计数器）
//	'z' + lp(name) + be64(ordered(score)) + member           -> nil（zset 排序侧）
//	'm' + lp(name) + member              -> be64(ordered(score))（zset 分数索引）
//	'c' + lp(name)                       -> be64(成员数)       （zset 计数）
//
// 名字带 uvarint 长度前缀，避免 "a"+后缀 与 "ab"+后缀 之类的跨名误匹配；整数一律
// 8 字节大端且翻转符号位（ordered/unorder），使字节序等于数值序，从而可以直接用
// 迭代器做范围扫描与按分数排序。
//
// # 为什么 TTL 不用 Badger 原生过期
//
// 契约里的"当前时刻"来自可注入的 core.Now（测试用 VirtualClock 快进），而 Badger 的
// `Entry.WithTTL` 走真实时间。若用原生 TTL，"快进时钟后键应已过期"的用例无法成立。
// 因此与 leveldb 一致：TTL 单独存一条绝对过期秒记录，读写两侧都以 core.NowUnix() 判定，
// 已过期一律视作不存在。物理回收交给 Badger 自身的 compaction。
//
// # 并发
//
// 只读操作走 db.View（BADGER 的 MVCC 快照，天然一致，不需要 SDK 这一层的锁）；
// "读当前状态 → 计算 → 写回"这类复合动作（Incr、队列 push/pop、zset 增删改）跨多条
// 记录，用按 key 分片的互斥锁串行化，避免同键并发各自基于同一旧值计算而丢更新，
// 也避免 Badger 的 SSI 冲突重试风暴（见 keyMutex 注释）。
package badger

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/RelicOfTesla/kvdb/core"

	badgerdb "github.com/dgraph-io/badger/v4"
)

var _ core.FullProvider = (*Provider)(nil)

// Config 控制 Badger 基座行为。
type Config struct {
	// NoSync 关闭每次提交的 fsync（吞吐更高；崩溃可能丢最近已确认的写入）。
	NoSync bool
	// BlockCacheSize 是块缓存大小（MiB）；<=0 使用 Badger 默认值。
	BlockCacheSize int
	// MemTableSize 是 memtable 大小（MiB）；<=0 使用 Badger 默认值。
	MemTableSize int
}

// 命名空间标签：单键空间里区分三类数据。与 leveldb 基座保持一致。
const (
	nsKV      byte = 'k'
	nsTTL     byte = 't'
	nsQueue   byte = 'q'
	nsQSeq    byte = 's'
	nsZScore  byte = 'z'
	nsZMember byte = 'm'
	nsZCount  byte = 'c'
)

// 二进制布局常量。所有整数一律 8 字节大端；队列计数器是三个连续 be64。
const (
	be64Len = 8

	qCountersLen = 3 * be64Len
	qNextOff     = 0
	qFrontOff    = be64Len
	qCountOff    = 2 * be64Len
)

// signBit 是 int64 的符号位：ordered/unorder 通过翻转它把有符号整数映射为可按
// 字节序比较的无符号整数。
const signBit uint64 = 1 << 63

// conflictRetries 是 db.Update 遇到 Badger SSI 冲突（ErrConflict）时的重试次数。
// 本基座的复合写已按 key 分片串行化，正常路径不会冲突；重试是兜底，覆盖
// "Set 读旧 TTL → 决定是否删除 TTL 记录"这类未加锁的读-写组合。
const conflictRetries = 16

// Provider 是 Badger 基座。并发安全；ctx 仅用于接口一致，不参与调度。
type Provider struct {
	db        *badgerdb.DB
	path      string
	closeOnce sync.Once
	closed    atomic.Bool
	keyMu     [keyShards]sync.Mutex // 见 keyMutex
}

// Open 打开（不存在则创建）path 指向的 Badger 目录。
func Open(ctx context.Context, path string, cfg Config) (*Provider, error) {
	_ = ctx
	if path == "" {
		return nil, errors.New("badger: path must not be empty")
	}
	opts := badgerdb.DefaultOptions(path).
		WithSyncWrites(!cfg.NoSync).
		WithLogger(nil) // 默认 logger 会往 stderr 打 INFO 级日志
	if cfg.BlockCacheSize > 0 {
		opts = opts.WithBlockCacheSize(int64(cfg.BlockCacheSize) << 20)
	}
	if cfg.MemTableSize > 0 {
		sz := int64(cfg.MemTableSize) << 20
		opts = opts.WithMemTableSize(sz)
		// Badger 由 BaseTableSize 推出"单次事务批上限"，而 ValueThreshold 默认 1MB；
		// 把 memtable 调到 8MB 以下后 1MB 会超过该上限，Open 直接报错。
		if vt := sz / 16; vt < opts.ValueThreshold {
			opts = opts.WithValueThreshold(vt)
		}
	}
	db, err := badgerdb.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("badger: open %s: %w", path, err)
	}
	return &Provider{db: db, path: path}, nil
}

// OpenURI 解析 badger://<path>?nosync=1&cache=64&memtable=64。
// 路径支持 badger://./data、badger:///abs/data（Host 为空表示绝对路径）。
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	q := u.Query()
	cfg := Config{NoSync: q.Get("nosync") == "1"}
	if v := q.Get("cache"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("badger: invalid cache %q: %w", v, err)
		}
		cfg.BlockCacheSize = n
	}
	if v := q.Get("memtable"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("badger: invalid memtable %q: %w", v, err)
		}
		cfg.MemTableSize = n
	}
	return Open(ctx, pathFromURL(u), cfg)
}

// pathFromURL 归一化文件型路径：Host 为空表示绝对路径 badger:///abs/x。
func pathFromURL(u *url.URL) string {
	if u.Host == "" {
		return u.Path
	}
	return strings.TrimPrefix(u.Host+u.Path, "/")
}

// DB 暴露底层句柄，供需要直接操作 Badger 的场景（自定义 GC、备份、统计等）。
func (p *Provider) DB() *badgerdb.DB { return p.db }

// Path 返回打开时使用的目录路径。
func (p *Provider) Path() string { return p.path }

func (p *Provider) check() error {
	if p.closed.Load() {
		return core.ErrClosed
	}
	return nil
}

// Close 幂等：closeOnce 保证底层句柄只关一次。
func (p *Provider) Close() error {
	var err error
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		if e := p.db.Close(); e != nil && !errors.Is(e, badgerdb.ErrDBClosed) {
			err = fmt.Errorf("badger: close: %w", e)
		}
	})
	return err
}

// keyMu 把同一 key 的"读-改-写"串行化，不同 key 之间仍可并行。分片锁避免为每个
// key 常驻一个 mutex。
//
// 为什么在"有事务的 Badger"上还要这把锁：Badger 的事务用 SSI——两个事务若读了
// 同一个键、都去写，后提交者拿到 ErrConflict 而整体回滚。仅靠重试也能保证正确性，
// 但同键热点（Incr 同 key、同队列 push/pop）会退化成"冲突-重试"风暴，且每次重试
// 都要重做整个事务。按 key 分片串行化把冲突从"运行时概率事件"变成"结构上不可能"，
// 冲突重试只作为兜底。
//
// 为什么是 Mutex 而不是 RWMutex——由操作所需的一致性语义决定，与"当前有几个调用点"
// 无关：需要互斥的是"读当前状态 → 计算 → 写回"这类**复合序列**（Incr、队列 push/pop、
// zset 增删改），临界区必须排他。只读操作不取这把锁：Badger 的 View 事务给出 MVCC
// 快照，单次读与整次扫描都是一致的；多键更新收在一个 Update 事务里原子生效。分片
// 粒度本就给不出跨分片一致性，给这把锁加 RLock 只会让读多付一层开销。
const keyShards = 64

func (p *Provider) keyMutex(key string) *sync.Mutex {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h = (h ^ uint32(key[i])) * 16777619
	}
	return &p.keyMu[h%keyShards]
}

// ---- 事务封装 ----

// view 在只读事务里执行 fn（MVCC 快照，读多键天然一致）。
func (p *Provider) view(op string, fn func(txn *badgerdb.Txn) error) error {
	if err := p.check(); err != nil {
		return err
	}
	if err := p.db.View(fn); err != nil {
		return p.wrap(op, err)
	}
	return nil
}

// update 在读写事务里执行 fn；遇到 SSI 冲突则重试（fn 会被重复调用，因此必须
// 只依赖事务内的读取，不得有跨次累积的副作用）。
func (p *Provider) update(op string, fn func(txn *badgerdb.Txn) error) error {
	if err := p.check(); err != nil {
		return err
	}
	var err error
	for attempt := 0; attempt <= conflictRetries; attempt++ {
		if err = p.db.Update(fn); !errors.Is(err, badgerdb.ErrConflict) {
			break
		}
		runtime.Gosched() // 让冲突对方先提交，避免纯自旋
	}
	if err != nil {
		return p.wrap(op, err)
	}
	return nil
}

// wrap 把底层错误映射为契约哨兵。
func (p *Provider) wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, badgerdb.ErrDBClosed) {
		return core.ErrClosed
	}
	return fmt.Errorf("badger: %s: %w", op, err)
}

// txnGet 读取键的值副本；不存在返回 ok=false。
func txnGet(txn *badgerdb.Txn, key []byte) ([]byte, bool, error) {
	item, err := txn.Get(key)
	if errors.Is(err, badgerdb.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

// ---- 编码工具 ----

// lp 为变长名加 uvarint 长度前缀，使复合键可按名前缀扫描且不会跨名误匹配。
func lp(name string) []byte {
	out := make([]byte, binary.MaxVarintLen64+len(name))
	n := binary.PutUvarint(out, uint64(len(name)))
	copy(out[n:], name)
	return out[:n+len(name)]
}

func ordered(v int64) uint64 { return uint64(v) ^ signBit }

func unorder(u uint64) int64 { return int64(u ^ signBit) }

func be64(u uint64) []byte {
	var b [be64Len]byte
	binary.BigEndian.PutUint64(b[:], u)
	return b[:]
}

// keyOf 拼出带命名空间标签的完整键。
func keyOf(ns byte, parts ...[]byte) []byte {
	out := make([]byte, 1, 1+len(parts[0]))
	out[0] = ns
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func kvKey(key string) []byte  { return keyOf(nsKV, lp(key)) }
func ttlKey(key string) []byte { return keyOf(nsTTL, lp(key)) }
func qItemKey(name string, seq int64) []byte {
	return keyOf(nsQueue, lp(name), be64(ordered(seq)))
}
func qSeqKey(name string) []byte { return keyOf(nsQSeq, lp(name)) }
func zScoreKey(name string, score int64, member string) []byte {
	return keyOf(nsZScore, lp(name), be64(ordered(score)), []byte(member))
}
func zMemberKey(name, member string) []byte {
	return keyOf(nsZMember, lp(name), []byte(member))
}
func zCountKey(name string) []byte { return keyOf(nsZCount, lp(name)) }

// nsPrefix 返回某命名空间的键前缀（Badger 迭代器用它限制扫描范围）。
func nsPrefix(ns byte) []byte { return []byte{ns} }

// prefixEnd 返回前缀 p 的"直接后继"：按字节序最小的、大于所有以 p 开头的键的键。
// 反向遍历前缀时必须 seek 到它，因为 Badger 的反向 Seek 按**用户键**比较，
// seek 到 p 本身会落在所有 p+… 之前而定位不到任何元素（实测 Rewind/Seek(p) 均失效）；
// seek 到 p+0xff 也不够——后缀首字节恰为 0xff 时仍会漏。返回 nil 表示 p 全为 0xff，
// 此时无上界，调用方改用 Rewind。
func prefixEnd(p []byte) []byte {
	end := append([]byte(nil), p...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// parseName 解出「uvarint 长度前缀 + 名字」。
func parseName(b []byte) (string, bool) {
	n, adv := binary.Uvarint(b)
	if adv <= 0 || uint64(len(b)-adv) < n {
		return "", false
	}
	return string(b[adv : adv+int(n)]), true
}

// parseKVEntry 从 'k' 前缀键还原用户键。
func parseKVEntry(k []byte) (string, bool) {
	if len(k) < 2 || k[0] != nsKV {
		return "", false
	}
	return parseName(k[1:])
}

// ---- TTL ----
//
// 过期时间存成绝对秒。三个 helper 都以事务为参数：读路径在 View 里、复合写在 Update 里，
// 都必须在同一个事务快照上读，否则并发下会读到撕裂的 (值, TTL) 组合。

// ttlValueOf 读出 TTL 记录的绝对过期秒。has=false 表示无 TTL 记录或记录异常。
func ttlValueOf(txn *badgerdb.Txn, key string) (exp int64, has bool, err error) {
	v, ok, err := txnGet(txn, ttlKey(key))
	if err != nil || !ok {
		return 0, false, err
	}
	if len(v) != be64Len {
		return 0, false, nil // 记录异常：按无 TTL 处理
	}
	return int64(binary.BigEndian.Uint64(v)), true, nil
}

// ttlExpired 只判定不修改。
func ttlExpired(txn *badgerdb.Txn, key string, now int64) (bool, error) {
	v, ok, err := txnGet(txn, ttlKey(key))
	if err != nil || !ok {
		return false, err
	}
	if len(v) != be64Len {
		return false, nil
	}
	return int64(binary.BigEndian.Uint64(v)) <= now, nil
}

// kvLive 返回键当前是否有效（存在且未过期）。
func kvLive(txn *badgerdb.Txn, key string, now int64) (bool, error) {
	if expired, err := ttlExpired(txn, key, now); err != nil || expired {
		return false, err
	}
	_, ok, err := txnGet(txn, kvKey(key))
	return ok, err
}

// ---- KV ----

// Set 写入 value，保留既有**未过期** TTL；已过期的键按不存在处理（清除旧 TTL），
// 否则会出现"写成功却读不到"。
func (p *Provider) Set(ctx context.Context, key string, value []byte) error {
	_ = ctx
	now := core.NowUnix()
	return p.update("set", func(txn *badgerdb.Txn) error {
		if err := txn.Set(kvKey(key), value); err != nil {
			return err
		}
		exp, has, err := ttlValueOf(txn, key)
		if err != nil {
			return err
		}
		if has && exp <= now {
			return txn.Delete(ttlKey(key))
		}
		return nil
	})
}

// SetEx 写入 value 并覆盖 TTL（对应 Redis SETEX / SSDB setx）。
func (p *Provider) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	_ = ctx
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	exp := core.AddTTL(core.NowUnix(), ttl)
	return p.update("setex", func(txn *badgerdb.Txn) error {
		if err := txn.Set(kvKey(key), value); err != nil {
			return err
		}
		return txn.Set(ttlKey(key), be64(uint64(exp)))
	})
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	_ = ctx
	var (
		out []byte
		ok  bool
	)
	now := core.NowUnix()
	err := p.view("get", func(txn *badgerdb.Txn) error {
		expired, err := ttlExpired(txn, key, now)
		if err != nil || expired {
			return err
		}
		v, found, err := txnGet(txn, kvKey(key))
		if err != nil {
			return err
		}
		if found {
			// ValueCopy 已是副本，但再拷一次以明确"返回值所有权归调用方"。
			out, ok = append([]byte(nil), v...), true
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return out, ok, nil
}

func (p *Provider) Del(ctx context.Context, key string) error {
	_ = ctx
	return p.update("del", func(txn *badgerdb.Txn) error {
		if err := txn.Delete(kvKey(key)); err != nil {
			return err
		}
		return txn.Delete(ttlKey(key))
	})
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	_ = ctx
	var ok bool
	now := core.NowUnix()
	err := p.view("exists", func(txn *badgerdb.Txn) error {
		var err error
		ok, err = kvLive(txn, key, now)
		return err
	})
	return ok, err
}

// Incr 原子累加。读-改-写在同一个 Update 事务内完成，外层再按 key 分片串行化
// （见 keyMutex）：已过期或值非整数的处理遵循契约。
func (p *Provider) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	_ = ctx
	mu := p.keyMutex(key)
	mu.Lock()
	defer mu.Unlock()

	now := core.NowUnix()
	var next int64
	err := p.update("incr", func(txn *badgerdb.Txn) error {
		expired, err := ttlExpired(txn, key, now)
		if err != nil {
			return err
		}
		var cur int64
		if !expired {
			v, found, err := txnGet(txn, kvKey(key))
			if err != nil {
				return err
			}
			if found {
				n, perr := strconv.ParseInt(string(v), 10, 64)
				if perr != nil {
					return core.ErrNotInteger
				}
				cur = n
			}
		}
		next = cur + delta
		if err := txn.Set(kvKey(key), []byte(strconv.FormatInt(next, 10))); err != nil {
			return err
		}
		if expired {
			return txn.Delete(ttlKey(key)) // 过期键按不存在：不得把旧 TTL 带过来
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return next, nil
}

func (p *Provider) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	_ = ctx
	out := make(map[string][]byte, len(keys))
	now := core.NowUnix()
	err := p.view("mget", func(txn *badgerdb.Txn) error {
		for _, k := range keys {
			expired, err := ttlExpired(txn, k, now)
			if err != nil {
				return err
			}
			if expired {
				continue
			}
			v, ok, err := txnGet(txn, kvKey(k))
			if err != nil {
				return err
			}
			if ok {
				out[k] = append([]byte(nil), v...)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Scan 按字节序闭区间 [start, end] 升序返回至多 limit 个键值对。
// 只扫 'k' 命名空间，天然不会把队列/zset 的排序键当 KV 条目。
func (p *Provider) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	_ = ctx
	limit = normalizeLimit(limit)
	now := core.NowUnix()
	out := make([]core.KeyValue, 0, minCap(limit))
	err := p.view("scan", func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.Prefix = nsPrefix(nsKV)
		it := txn.NewIterator(opts)
		defer it.Close()
		seek := nsPrefix(nsKV)
		if start != "" {
			seek = kvKey(start)
		}
		for it.Seek(seek); it.Valid(); it.Next() {
			item := it.Item()
			user, ok := parseKVEntry(item.Key())
			if !ok {
				continue
			}
			if end != "" && user > end {
				break
			}
			expired, err := ttlExpired(txn, user, now)
			if err != nil {
				return err
			}
			if expired {
				continue
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			out = append(out, core.KeyValue{Key: user, Value: v})
			if len(out) >= limit {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (p *Provider) Expire(ctx context.Context, key string, ttl int64) error {
	_ = ctx
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	now := core.NowUnix()
	exp := core.AddTTL(now, ttl)
	return p.update("expire", func(txn *badgerdb.Txn) error {
		live, err := kvLive(txn, key, now)
		if err != nil || !live {
			return err // 不存在或已过期都按不存在处理：不复活过期键
		}
		return txn.Set(ttlKey(key), be64(uint64(exp)))
	})
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	_ = ctx
	var (
		secs int64
		ok   bool
	)
	now := core.NowUnix()
	err := p.view("ttl", func(txn *badgerdb.Txn) error {
		exp, has, err := ttlValueOf(txn, key)
		if err != nil || !has || exp <= now {
			return err
		}
		live, err := kvLive(txn, key, now)
		if err != nil || !live {
			return err
		}
		secs, ok = exp-now, true
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return -1, false, nil
	}
	return secs, true, nil
}

// normalizeLimit 保证 limit<=0 时使用 core.DefaultScanLimit（统一常量）。
func normalizeLimit(limit int) int {
	if limit <= 0 {
		return core.DefaultScanLimit
	}
	return limit
}

// minCap 为预分配给出安全上界，避免调用方传入超大 limit 时一次性申请巨量内存。
func minCap(n int) int {
	const maxPrealloc = 1024
	if n > maxPrealloc {
		return maxPrealloc
	}
	return n
}
