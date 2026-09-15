// Package mem 提供纯内存基座：数据不落盘，进程退出即失。实现 KV + Queue + ZSet
// 三种能力。适合原型、测试与纯计算缓存；如需进程内持久化使用 jsonl 或 sqlite 基座。
package mem

import (
	"container/list"
	"context"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"sync"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

func init() { kvdb.MustRegister("mem", OpenURI) }

// OpenURI 按 mem://（忽略路径与参数）创建内存基座，供 kvdb.Open 使用。
func OpenURI(_ context.Context, _ *url.URL) (core.KvProvider, error) {
	return New(), nil
}

var (
	_ core.FullProvider = (*Provider)(nil)
)

type entry struct {
	val []byte
	exp int64 // unix 秒；0 表示无 TTL
}

// Provider 是内存基座。并发安全；ctx 仅用于接口一致，不参与调度。
type Provider struct {
	mu     sync.RWMutex
	kv     map[string]*entry
	queue  map[string]*list.List
	zset   map[string]map[string]int64
	closed bool
	// lastSweep 是上次过期回收的时刻（unix 秒）。仅写路径在写锁内读写，
	// 用于节流 sweepLocked（无需后台 goroutine）。
	lastSweep int64
}

// New 创建一个空的内存基座。
func New() *Provider {
	return &Provider{
		kv:    make(map[string]*entry),
		queue: make(map[string]*list.List),
		zset:  make(map[string]map[string]int64),
	}
}

func (p *Provider) checkOpen() error {
	if p.closed {
		return core.ErrClosed
	}
	return nil
}

// lookup 返回 key 当前是否有效（存在且未过期）。purge 为 true 时顺带删除已过期
// 条目——**只有持有写锁的调用方才能传 true**：读路径在 RLock 下 delete(map) 会与
// 其他读者并发写同一张 map（数据竞争，甚至 "concurrent map writes" 崩溃）。
// 只读路径传 false：过期条目由后续写操作、Scan 过滤或 Snapshot 处理。
func (p *Provider) lookup(key string, now int64, purge bool) (*entry, bool) {
	e, ok := p.kv[key]
	if !ok {
		return nil, false
	}
	if e.exp > 0 && e.exp <= now {
		if purge {
			delete(p.kv, key)
		}
		return nil, false
	}
	return e, true
}

func (p *Provider) Set(ctx context.Context, key string, value []byte) error {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return err
	}
	p.setLocked(key, value, core.NowUnix())
	return nil
}

// setLocked 写入值：保留既有**未过期**条目的 TTL；已过期的条目按"不存在"处理，
// 不得继承其过期时间（否则写入成功却仍读不到）。调用方需持有 p.mu。
func (p *Provider) setLocked(key string, value []byte, now int64) {
	if e, ok := p.lookup(key, now, true); ok {
		e.val = append([]byte(nil), value...)
		return
	}
	p.kv[key] = &entry{val: append([]byte(nil), value...)}
}

// SetEx 写入 value 并覆盖 TTL（对应 Redis SETEX / SSDB setx）。
func (p *Provider) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	_ = ctx
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return err
	}
	p.setExLocked(key, value, ttl, core.NowUnix())
	return nil
}

// setExLocked 写入值并覆盖 TTL，调用方需持有 p.mu。
func (p *Provider) setExLocked(key string, value []byte, ttl int64, now int64) {
	p.sweepLocked(now) // TTL 条目只会经此路径与 expireLocked 产生，回收在此节流触发
	p.kv[key] = &entry{val: append([]byte(nil), value...), exp: core.AddTTL(now, ttl)}
}

// sweepInterval 是过期条目回收的最小间隔（秒）。
const sweepInterval = 60

// sweepLocked 物理删除已过期的 kv 条目并清理空容器，调用方需持有 p.mu。
// 读路径在 RLock 下不得改写 map（见 lookup 注释），因此回收挂在产生 TTL
// 的写路径上按间隔节流执行；纯读负载不产生新的过期条目，无需回收。
func (p *Provider) sweepLocked(now int64) {
	if now-p.lastSweep < sweepInterval {
		return
	}
	p.lastSweep = now
	for k, e := range p.kv {
		if e.exp > 0 && e.exp <= now {
			delete(p.kv, k)
		}
	}
	for name, l := range p.queue {
		if l.Len() == 0 {
			delete(p.queue, name)
		}
	}
	for name, m := range p.zset {
		if len(m) == 0 {
			delete(p.zset, name)
		}
	}
}

// ---- 读路径 ----
//
// 返回值所有权：Get/MGet/Scan/QFront/QBack 一律返回内部数据的**深拷贝**。
// 若直接返回内部切片，调用方改一个字节就能篡改库内状态（也会让 jsonl 的
// 内存态与日志回放结果不一致）。

// clone 返回 b 的拷贝；b 为 nil 时返回 nil。
func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	_ = ctx
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return nil, false, err
	}
	e, ok := p.lookup(key, core.NowUnix(), false)
	if !ok {
		return nil, false, nil
	}
	return clone(e.val), true, nil
}

func (p *Provider) Del(ctx context.Context, key string) error {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return err
	}
	p.delLocked(key)
	return nil
}

// delLocked 删除键，调用方需持有 p.mu。
func (p *Provider) delLocked(key string) { delete(p.kv, key) }

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	_ = ctx
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return false, err
	}
	_, ok := p.lookup(key, core.NowUnix(), false)
	return ok, nil
}

func (p *Provider) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return 0, err
	}
	var cur int64
	var exp int64 // 保留原 TTL（SSDB incr 不改 ttl 表）
	if e, ok := p.lookup(key, core.NowUnix(), true); ok {
		v, err := parseInt(e.val)
		if err != nil {
			return 0, err
		}
		cur = v
		exp = e.exp
	}
	cur += delta
	p.kv[key] = &entry{val: []byte(strconv.FormatInt(cur, 10)), exp: exp}
	return cur, nil
}

func (p *Provider) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	_ = ctx
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return nil, err
	}
	now := core.NowUnix()
	out := make(map[string][]byte, len(keys))
	for _, k := range keys {
		if e, ok := p.lookup(k, now, false); ok {
			out[k] = clone(e.val)
		}
	}
	return out, nil
}

func (p *Provider) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	_ = ctx
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return nil, err
	}
	limit = normalizeLimit(limit)
	now := core.NowUnix()
	keys := make([]string, 0, len(p.kv))
	for k, e := range p.kv {
		if e.exp > 0 && e.exp <= now {
			// 只读路径不得改写 map（见 lookup 注释）：跳过即可，
			// 过期条目由写操作或 Snapshot 清理。
			continue
		}
		if start != "" && k < start {
			continue
		}
		if end != "" && k > end {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]core.KeyValue, 0, len(keys))
	for _, k := range keys {
		out = append(out, core.KeyValue{Key: k, Value: clone(p.kv[k].val)})
	}
	return out, nil
}

func (p *Provider) Expire(ctx context.Context, key string, ttl int64) error {
	_ = ctx
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return err
	}
	p.expireLocked(key, ttl, core.NowUnix())
	return nil
}

// expireLocked 设置 TTL：已过期/不存在的 key 按不存在处理，不做"复活"。
// 调用方需持有 p.mu。
func (p *Provider) expireLocked(key string, ttl int64, now int64) {
	p.sweepLocked(now)
	if e, ok := p.lookup(key, now, true); ok {
		e.exp = core.AddTTL(now, ttl)
	}
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	_ = ctx
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return 0, false, err
	}
	now := core.NowUnix()
	e, ok := p.lookup(key, now, false)
	if !ok || e.exp == 0 {
		return -1, false, nil
	}
	rem := e.exp - now
	if rem <= 0 {
		// 秒级边界上恰好到期：与 Get/Exists 的"已过期即不存在"保持一致。
		return -1, false, nil
	}
	return rem, true, nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.kv = nil
	p.queue = nil
	p.zset = nil
	return nil
}

// Snapshot 返回当前状态的完整拷贝（值均为深拷贝）。供备份与
// jsonl 基座日志压缩使用；不对接口族产生依赖。
type Snapshot struct {
	KV    map[string][]byte   // key -> value（不含已过期项）
	Exp   map[string]int64    // key -> 过期绝对时间戳（unix 秒），无 TTL 的 key 不在其中
	Queue map[string][][]byte // 队列名 -> 队头到队尾的值
	ZSet  map[string]map[string]int64
}

func (p *Provider) Snapshot() Snapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := core.NowUnix()
	s := Snapshot{
		KV:    make(map[string][]byte, len(p.kv)),
		Exp:   make(map[string]int64),
		Queue: make(map[string][][]byte, len(p.queue)),
		ZSet:  make(map[string]map[string]int64, len(p.zset)),
	}
	for k, e := range p.kv {
		if e.exp > 0 && e.exp <= now {
			continue
		}
		s.KV[k] = append([]byte(nil), e.val...)
		if e.exp > 0 {
			s.Exp[k] = e.exp
		}
	}
	for name, l := range p.queue {
		vals := make([][]byte, 0, l.Len())
		for el := l.Front(); el != nil; el = el.Next() {
			vals = append(vals, append([]byte(nil), el.Value.([]byte)...))
		}
		s.Queue[name] = vals
	}
	for name, m := range p.zset {
		cp := make(map[string]int64, len(m))
		for k, v := range m {
			cp[k] = v
		}
		s.ZSet[name] = cp
	}
	return s
}

// ---- Queue ----

func (p *Provider) QPush(ctx context.Context, name string, value []byte) error {
	return p.qpush(ctx, name, value, false)
}

func (p *Provider) QPushFront(ctx context.Context, name string, value []byte) error {
	return p.qpush(ctx, name, value, true)
}

func (p *Provider) qpush(_ context.Context, name string, value []byte, front bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return err
	}
	p.qpushLocked(name, value, front)
	return nil
}

// qpushLocked 追加/插入队列元素，调用方需持有 p.mu。
func (p *Provider) qpushLocked(name string, value []byte, front bool) {
	l := p.queue[name]
	if l == nil {
		l = list.New()
		p.queue[name] = l
	}
	v := append([]byte(nil), value...)
	if front {
		l.PushFront(v)
	} else {
		l.PushBack(v)
	}
}

func (p *Provider) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpop(ctx, name, false)
}

func (p *Provider) QPopBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpop(ctx, name, true)
}

func (p *Provider) qpop(_ context.Context, name string, back bool) ([]byte, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return nil, false, err
	}
	l := p.queue[name]
	if l == nil || l.Len() == 0 {
		return nil, false, nil
	}
	var el *list.Element
	if back {
		el = l.Back()
	} else {
		el = l.Front()
	}
	l.Remove(el)
	if l.Len() == 0 {
		// 弹空后删除外层条目，防止队列命名 churn 的外层 map 泄漏。
		delete(p.queue, name)
	}
	return el.Value.([]byte), true, nil
}

func (p *Provider) QSize(_ context.Context, name string) (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return 0, err
	}
	if l := p.queue[name]; l != nil {
		return int64(l.Len()), nil
	}
	return 0, nil
}

func (p *Provider) QFront(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qfront(ctx, name, false)
}

func (p *Provider) QBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qfront(ctx, name, true)
}

func (p *Provider) qfront(_ context.Context, name string, back bool) ([]byte, bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return nil, false, err
	}
	l := p.queue[name]
	if l == nil || l.Len() == 0 {
		return nil, false, nil
	}
	if back {
		return clone(l.Back().Value.([]byte)), true, nil
	}
	return clone(l.Front().Value.([]byte)), true, nil
}

// ---- ZSet ----

func (p *Provider) ZSet(_ context.Context, name, key string, score int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return err
	}
	p.zsetLocked(name, key, score)
	return nil
}

// zsetLocked 写入 zset 成员分数，调用方需持有 p.mu。
func (p *Provider) zsetLocked(name, key string, score int64) {
	m := p.zset[name]
	if m == nil {
		m = make(map[string]int64)
		p.zset[name] = m
	}
	m[key] = score
}

func (p *Provider) ZGet(_ context.Context, name, key string) (int64, bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return 0, false, err
	}
	score, ok := p.zset[name][key]
	return score, ok, nil
}

func (p *Provider) ZDel(_ context.Context, name, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return err
	}
	p.zdelLocked(name, key)
	return nil
}

// zdelLocked 删除 zset 成员，调用方需持有 p.mu。
func (p *Provider) zdelLocked(name, key string) {
	if m := p.zset[name]; m != nil {
		delete(m, key)
		if len(m) == 0 {
			// 清空后删除内层 map 与外层条目，防止成员命名 churn 泄漏。
			delete(p.zset, name)
		}
	}
}

func (p *Provider) ZSize(_ context.Context, name string) (int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return 0, err
	}
	return int64(len(p.zset[name])), nil
}

func (p *Provider) ZRank(_ context.Context, name, key string) (int64, bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return 0, false, err
	}
	score, ok := p.zset[name][key]
	if !ok {
		return 0, false, nil
	}
	rank := int64(0)
	for k, s := range p.zset[name] {
		if s < score || (s == score && k < key) {
			rank++
		}
	}
	return rank, true, nil
}

func (p *Provider) ZRange(_ context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return nil, err
	}
	m := p.zset[name]
	items := make([]core.ZItem, 0, len(m))
	for k, s := range m {
		items = append(items, core.ZItem{Key: k, Score: s})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Score != items[j].Score {
			return items[i].Score < items[j].Score
		}
		return items[i].Key < items[j].Key
	})
	return sliceRange(items, start, stop), nil
}

func (p *Provider) ZIncr(_ context.Context, name, key string, delta int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return 0, err
	}
	return p.zincrLocked(name, key, delta), nil
}

// zincrLocked 累加 zset 成员分数，调用方需持有 p.mu。
func (p *Provider) zincrLocked(name, key string, delta int64) int64 {
	m := p.zset[name]
	if m == nil {
		m = make(map[string]int64)
		p.zset[name] = m
	}
	m[key] += delta
	return m[key]
}

// ---- 工具 ----

// normalizeLimit 保证 limit<=0 时使用 core.DefaultScanLimit 的页大小，
// 改默认值只需改一处。
func normalizeLimit(limit int) int {
	if limit <= 0 {
		return core.DefaultScanLimit
	}
	return limit
}

func parseInt(b []byte) (int64, error) {
	v, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, core.ErrNotInteger
	}
	return v, nil
}

// sliceRange 按 Redis ZRANGE 索引语义裁剪切片：0 起闭区间，负索引从末尾数
// （-1 为最后一个；如 -start 超出长度则回到 0）。
func sliceRange(items []core.ZItem, start, stop int64) []core.ZItem {
	n := int64(len(items))
	if start < 0 {
		start = n + start
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop = n + stop
	}
	if n == 0 || start >= n || stop < start {
		return nil
	}
	if stop >= n {
		stop = n - 1
	}
	return items[start : stop+1]
}

// ---- Batch ----

// ApplyBatch 在单次持锁内按序应用整批操作：要么全部生效，要么（参数非法时）
// 一条都不生效。批内均为无条件写，故正常情况下不会失败。
func (p *Provider) ApplyBatch(ctx context.Context, ops []core.BatchOp) error {
	_ = ctx
	if len(ops) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return err
	}
	// 先校验整批（Kind 与 TTL 合法性），避免"应用一半才发现参数非法"。
	for _, op := range ops {
		switch op.Kind {
		case core.BatchSet, core.BatchDel, core.BatchQPush, core.BatchQPushFront,
			core.BatchZSet, core.BatchZDel, core.BatchZIncr:
			// 无条件写，无参数校验需求
		case core.BatchSetEx, core.BatchExpire:
			if op.TTL <= 0 {
				return core.ErrInvalidTTL
			}
		default:
			return fmt.Errorf("mem: unknown batch op %d", op.Kind)
		}
	}
	now := core.NowUnix() // 整批共享同一"当前时刻"，避免批内语义漂移
	for _, op := range ops {
		switch op.Kind {
		case core.BatchSet:
			p.setLocked(op.Key, op.Value, now)
		case core.BatchSetEx:
			p.setExLocked(op.Key, op.Value, op.TTL, now)
		case core.BatchDel:
			p.delLocked(op.Key)
		case core.BatchExpire:
			p.expireLocked(op.Key, op.TTL, now)
		case core.BatchQPush:
			p.qpushLocked(op.Key, op.Value, false)
		case core.BatchQPushFront:
			p.qpushLocked(op.Key, op.Value, true)
		case core.BatchZSet:
			p.zsetLocked(op.Key, op.Member, op.Score)
		case core.BatchZDel:
			p.zdelLocked(op.Key, op.Member)
		case core.BatchZIncr:
			p.zincrLocked(op.Key, op.Member, op.Delta)
		default:
			return fmt.Errorf("mem: unknown batch op %d", op.Kind)
		}
	}
	return nil
}
