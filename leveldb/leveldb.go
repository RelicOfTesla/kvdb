// Package leveldb 提供以 LevelDB（syndtr/goleveldb，纯 Go LSM-tree）为后端的基座，
// 实现 KV + Queue + ZSet + Batch 全部能力。
//
// # 键空间布局
//
// LevelDB 是**单一有序键空间**（不像 bbolt 有 bucket），因此三类数据用首字节
// 命名空间标签隔离，其后是「uvarint 长度前缀的名字」+ 该结构自己的排序键：
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
// 8 字节大端、且经符号位翻转（ordered/unorder），使字节序等于数值序，从而可以直接
// 用迭代器做范围扫描与按分数排序。
//
// # 原子性
//
// LevelDB 没有事务，但 `Write(batch)` 是原子的（一次 WAL 追加 + memtable 应用）。
// 本基座把**所有多键写**（含 Set/SetEx 的"值+TTL"两步、批写、zset 的双侧索引维护）
// 都收进一个 Batch，因此对外语义与 bolt 的事务实现一致：要么全生效，要么全不生效。
//
// # 过期语义
//
// 契约要求"已过期 = 不存在"，故读写两侧都以当前时刻判定：过期的 TTL 记录在写入前
// 清除（Set 不会继承旧过期时间）、Incr 从 0 起算、Expire 不复活过期键。
package leveldb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/RelicOfTesla/kvdb/core"

	gldb "github.com/syndtr/goleveldb/leveldb"
	gerr "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var _ core.FullProvider = (*Provider)(nil)

// Config 控制 LevelDB 基座行为。
type Config struct {
	// NoSync 关闭每次提交的 fsync（吞吐更高；崩溃可能丢最近已确认的写入）。
	NoSync bool
	// BlockCacheSize 是块缓存大小（MiB）；<=0 使用 goleveldb 默认值。
	BlockCacheSize int
	// WriteBuffer 是 memtable 大小（MiB）；<=0 使用 goleveldb 默认值。
	WriteBuffer int
}

// 命名空间标签：单键空间里区分三类数据。取值互不相同即可，这里用可读字母。
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
// 解析处一律引用这些名字而不是字面量，改布局时只需改这里。
const (
	be64Len = 8 // 大端 int64/uint64 的字节数

	qCountersLen = 3 * be64Len
	qNextOff     = 0
	qFrontOff    = be64Len
	qCountOff    = 2 * be64Len
)

// signBit 是 int64 的符号位：ordered/unorder 通过翻转它把有符号整数映射为可按
// 字节序比较的无符号整数（两个方向必须翻转同一位，故提取为常量）。
const signBit uint64 = 1 << 63

// Provider 是 LevelDB 基座。并发安全（goleveldb 自身可并发使用）；
// ctx 仅用于接口一致，不参与调度。
type Provider struct {
	db        *gldb.DB
	path      string
	wo        *opt.WriteOptions // 预置 NoSync 等写选项，避免每次构造
	closeOnce sync.Once
	closed    atomic.Bool
	keyMu     [keyShards]sync.Mutex // 见 keyMutex
}

// Open 打开（不存在则创建）path 指向的 LevelDB 目录。
func Open(ctx context.Context, path string, cfg Config) (*Provider, error) {
	_ = ctx
	if path == "" {
		return nil, errors.New("leveldb: path must not be empty")
	}
	o := &opt.Options{}
	if cfg.BlockCacheSize > 0 {
		o.BlockCacheCapacity = cfg.BlockCacheSize * opt.MiB // 字段单位为字节
	}
	if cfg.WriteBuffer > 0 {
		o.WriteBuffer = cfg.WriteBuffer * opt.MiB
	}
	db, err := gldb.OpenFile(path, o)
	if err != nil {
		return nil, fmt.Errorf("leveldb: open %s: %w", path, err)
	}
	p := &Provider{db: db, path: path}
	// 注意极性：goleveldb 的 WriteOptions 只有 Sync（默认 false＝不 fsync）。
	// Config.NoSync=true 时这里保持 Sync=false；默认（NoSync=false）才要求 Sync。
	p.wo = &opt.WriteOptions{Sync: !cfg.NoSync}
	return p, nil
}

// OpenURI 解析 leveldb://<path>?nosync=1&cache=8&wb=4。
// 路径支持 leveldb://./data、leveldb:///abs/data（Host 为空表示绝对路径）。
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	path := pathFromURL(u)
	q := u.Query()
	cfg := Config{NoSync: q.Get("nosync") == "1"}
	if v := q.Get("cache"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("leveldb: invalid cache %q: %w", v, err)
		}
		cfg.BlockCacheSize = n
	}
	if v := q.Get("wb"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("leveldb: invalid wb %q: %w", v, err)
		}
		cfg.WriteBuffer = n
	}
	return Open(ctx, path, cfg)
}

// pathFromURL 归一化文件型路径：Host 为空表示绝对路径 leveldb:///abs/x。
func pathFromURL(u *url.URL) string {
	if u.Host == "" {
		return u.Path
	}
	return strings.TrimPrefix(u.Host+u.Path, "/")
}

// DB 暴露底层句柄，供需要直接操作 LevelDB 的场景（自定义压缩、统计等）。
func (p *Provider) DB() *gldb.DB { return p.db }

// Path 返回打开时使用的目录路径。
func (p *Provider) Path() string { return p.path }

func (p *Provider) check() error {
	if p.closed.Load() {
		return core.ErrClosed
	}
	return nil
}

// Close 幂等：closeOnce 保证底层句柄只关一次；closed 为原子标志，
// 与并发请求的 check() 之间无数据竞争。
func (p *Provider) Close() error {
	var err error
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		if e := p.db.Close(); e != nil && !errors.Is(e, gldb.ErrClosed) {
			err = fmt.Errorf("leveldb: close: %w", e)
		}
	})
	return err
}

// keyMu 把同一 key 的"读-改-写"串行化（LevelDB 没有 CAS/事务原语），
// 不同 key 之间仍可并行。分片锁避免为每个 key 常驻一个 mutex。
const keyShards = 64

func (p *Provider) keyMutex(key string) *sync.Mutex {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h = (h ^ uint32(key[i])) * 16777619
	}
	return &p.keyMu[h%keyShards]
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

// nsRange 返回某命名空间（可选再加名字前缀）的迭代范围。
func nsRange(ns byte, namePrefix ...byte) *util.Range {
	start := []byte{ns}
	start = append(start, namePrefix...)
	limit := make([]byte, len(start))
	copy(limit, start)
	for i := len(limit) - 1; i >= 0; i-- {
		if limit[i] < 0xff {
			limit[i]++
			return &util.Range{Start: start, Limit: limit[:i+1]}
		}
	}
	return &util.Range{Start: start} // 全 0xff（极端情况）：不设上界
}

// parseTTLEntry 从 't' 前缀键还原用户键。
func parseTTLEntry(k []byte) (string, bool) {
	if len(k) < 2 || k[0] != nsTTL {
		return "", false
	}
	return parseName(k[1:])
}

// parseKVEntry 从 'k' 前缀键还原用户键。
func parseKVEntry(k []byte) (string, bool) {
	if len(k) < 2 || k[0] != nsKV {
		return "", false
	}
	return parseName(k[1:])
}

// parseName 解出「uvarint 长度前缀 + 名字」。
func parseName(b []byte) (string, bool) {
	n, adv := binary.Uvarint(b)
	if adv <= 0 || uint64(len(b)-adv) < n {
		return "", false
	}
	return string(b[adv : adv+int(n)]), true
}

// ---- TTL ----

// ttlValueOf 读出 TTL 记录的绝对过期秒。has=false 表示无 TTL 记录或记录异常。
func (p *Provider) ttlValueOf(key string) (exp int64, has bool, err error) {
	v, err := p.db.Get(ttlKey(key), nil)
	if errors.Is(err, gerr.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("leveldb: ttl: %w", err)
	}
	if len(v) != be64Len {
		return 0, false, nil // 记录异常：按无 TTL 处理
	}
	return int64(binary.BigEndian.Uint64(v)), true, nil
}

// ttlExpiredAt 只判定不修改（读路径用）。
func (p *Provider) ttlExpiredAt(key string, now int64) (bool, error) {
	v, err := p.db.Get(ttlKey(key), nil)
	if errors.Is(err, gerr.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("leveldb: ttl: %w", err)
	}
	if len(v) != be64Len {
		return false, nil
	}
	return int64(binary.BigEndian.Uint64(v)) <= now, nil
}

// ---- KV ----

// Set 写入 value，保留既有**未过期** TTL；已过期的键按不存在处理（清除旧 TTL），
// 否则会出现"写成功却读不到"。两步收进一个 Batch，原子生效。
func (p *Provider) Set(ctx context.Context, key string, value []byte) error {
	_ = ctx
	if err := p.check(); err != nil {
		return err
	}
	now := core.NowUnix()
	exp, has, err := p.ttlValueOf(key)
	if err != nil {
		return err
	}
	var b gldb.Batch
	b.Put(kvKey(key), value)
	if has && exp <= now {
		b.Delete(ttlKey(key))
	}
	return p.write(&b, "set")
}

// SetEx 写入 value 并覆盖 TTL（对应 Redis SETEX / SSDB setx）。
func (p *Provider) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	_ = ctx
	if err := p.check(); err != nil {
		return err
	}
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	var b gldb.Batch
	b.Put(kvKey(key), value)
	b.Put(ttlKey(key), be64(uint64(core.AddTTL(core.NowUnix(), ttl))))
	return p.write(&b, "setex")
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return nil, false, err
	}
	now := core.NowUnix()
	expired, err := p.ttlExpiredAt(key, now)
	if err != nil {
		return nil, false, err
	}
	if expired {
		return nil, false, nil
	}
	v, err := p.db.Get(kvKey(key), nil)
	if errors.Is(err, gerr.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("leveldb: get: %w", err)
	}
	// 返回副本：调用方不得通过返回值改写库内状态（Get 的返回值所有权契约）。
	return append([]byte(nil), v...), true, nil
}

func (p *Provider) Del(ctx context.Context, key string) error {
	_ = ctx
	if err := p.check(); err != nil {
		return err
	}
	var b gldb.Batch
	b.Delete(kvKey(key))
	b.Delete(ttlKey(key))
	return p.write(&b, "del")
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return false, err
	}
	now := core.NowUnix()
	if ok, err := p.ttlExpiredAt(key, now); err != nil || ok {
		return false, err
	}
	ok, err := p.db.Has(kvKey(key), nil)
	if err != nil {
		return false, fmt.Errorf("leveldb: exists: %w", err)
	}
	return ok, nil
}

// Incr 原子累加。LevelDB 无 CAS 原语，用 per-key 互斥把"读-改-写"串行化
// （不同 key 之间仍并行）；已过期或值非整数的处理遵循契约。
func (p *Provider) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return 0, err
	}
	mu := p.keyMutex(key)
	mu.Lock()
	defer mu.Unlock()

	now := core.NowUnix()
	expired, err := p.ttlExpiredAt(key, now)
	if err != nil {
		return 0, err
	}
	var cur int64
	var keepExp int64
	var keepTTL bool
	if !expired {
		if v, err := p.db.Get(kvKey(key), nil); err == nil {
			n, perr := strconv.ParseInt(string(v), 10, 64)
			if perr != nil {
				return 0, core.ErrNotInteger
			}
			cur = n
			if exp, has, err := p.ttlValueOf(key); err != nil {
				return 0, err
			} else if has {
				keepExp, keepTTL = exp, true
			}
		} else if !errors.Is(err, gerr.ErrNotFound) {
			return 0, fmt.Errorf("leveldb: incr: %w", err)
		}
	}
	next := cur + delta
	var b gldb.Batch
	b.Put(kvKey(key), []byte(strconv.FormatInt(next, 10)))
	if expired {
		b.Delete(ttlKey(key)) // 过期键按不存在：不得把旧 TTL 带过来
	} else if keepTTL {
		b.Put(ttlKey(key), be64(uint64(keepExp)))
	}
	if err := p.write(&b, "incr"); err != nil {
		return 0, err
	}
	return next, nil
}

func (p *Provider) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(keys))
	now := core.NowUnix()
	for _, k := range keys {
		expired, err := p.ttlExpiredAt(k, now)
		if err != nil {
			return nil, err
		}
		if expired {
			continue
		}
		v, err := p.db.Get(kvKey(k), nil)
		if errors.Is(err, gerr.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("leveldb: mget: %w", err)
		}
		out[k] = append([]byte(nil), v...)
	}
	return out, nil
}

// Scan 按字节序闭区间 [start, end] 升序返回至多 limit 个键值对。
// 只扫 'k' 命名空间，天然不会把队列/zset 的排序键当 KV 条目。
func (p *Provider) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return nil, err
	}
	limit = normalizeLimit(limit)
	now := core.NowUnix()
	r := nsRange(nsKV)
	if start != "" {
		r.Start = kvKey(start)
	}
	if end != "" {
		r.Limit = append(kvKey(end), 0xff) // 闭区间：包含 end 的全部字节
	}
	it := p.db.NewIterator(r, nil)
	defer it.Release()
	out := make([]core.KeyValue, 0, minCap(limit))
	for it.Seek(r.Start); it.Valid(); it.Next() {
		k := it.Key()
		user, ok := parseKVEntry(k)
		if !ok {
			continue
		}
		if end != "" && user > end {
			break
		}
		if expired, err := p.ttlExpiredAt(user, now); err != nil {
			return nil, err
		} else if expired {
			continue
		}
		out = append(out, core.KeyValue{Key: user, Value: append([]byte(nil), it.Value()...)})
		if len(out) >= limit {
			break
		}
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("leveldb: scan: %w", err)
	}
	return out, nil
}

func (p *Provider) Expire(ctx context.Context, key string, ttl int64) error {
	_ = ctx
	if err := p.check(); err != nil {
		return err
	}
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	now := core.NowUnix()
	// 不存在或已过期都按不存在处理：不复活过期键。
	ok, err := p.db.Has(kvKey(key), nil)
	if err != nil {
		return fmt.Errorf("leveldb: expire: %w", err)
	}
	if !ok {
		return nil
	}
	if expired, err := p.ttlExpiredAt(key, now); err != nil {
		return err
	} else if expired {
		return nil
	}
	var b gldb.Batch
	b.Put(ttlKey(key), be64(uint64(core.AddTTL(now, ttl))))
	return p.write(&b, "expire")
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return 0, false, err
	}
	now := core.NowUnix()
	exp, has, err := p.ttlValueOf(key)
	if err != nil {
		return 0, false, err
	}
	if !has || exp <= now {
		return -1, false, nil
	}
	ok, err := p.db.Has(kvKey(key), nil)
	if err != nil {
		return 0, false, fmt.Errorf("leveldb: ttl: %w", err)
	}
	if !ok {
		return -1, false, nil
	}
	return exp - now, true, nil
}

// write 提交一个 Batch（LevelDB 的 Write 是原子的：一次 WAL 追加 + memtable 应用）。
func (p *Provider) write(b *gldb.Batch, op string) error {
	if b.Len() == 0 {
		return nil
	}
	if err := p.db.Write(b, p.wo); err != nil {
		if errors.Is(err, gldb.ErrClosed) {
			return core.ErrClosed
		}
		return fmt.Errorf("leveldb: %s: %w", op, err)
	}
	return nil
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
