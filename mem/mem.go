// Package mem 提供纯内存基座：数据不落盘，进程退出即失。实现 KV + Queue + ZSet
// 三种能力。适合原型、测试与纯计算缓存；如需进程内持久化使用 jsonl 或 sqlite 基座。
package mem

import (
	"container/list"
	"context"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"kvdb"
	"kvdb/core"
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

// alive 返回 key 当前是否有效（存在且未过期），并顺带清除已过期条目。
func (p *Provider) alive(key string, now int64) (*entry, bool) {
	e, ok := p.kv[key]
	if !ok {
		return nil, false
	}
	if e.exp > 0 && e.exp <= now {
		delete(p.kv, key)
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
	if e, ok := p.kv[key]; ok {
		e.val = append([]byte(nil), value...)
		return nil
	}
	p.kv[key] = &entry{val: append([]byte(nil), value...)}
	return nil
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
	p.kv[key] = &entry{val: append([]byte(nil), value...), exp: time.Now().Unix() + ttl}
	return nil
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	_ = ctx
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return nil, false, err
	}
	e, ok := p.alive(key, time.Now().Unix())
	if !ok {
		return nil, false, nil
	}
	return e.val, true, nil
}

func (p *Provider) Del(ctx context.Context, key string) error {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return err
	}
	delete(p.kv, key)
	return nil
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	_ = ctx
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return false, err
	}
	_, ok := p.alive(key, time.Now().Unix())
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
	if e, ok := p.alive(key, time.Now().Unix()); ok {
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
	now := time.Now().Unix()
	out := make(map[string][]byte, len(keys))
	for _, k := range keys {
		if e, ok := p.alive(k, now); ok {
			out[k] = e.val
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
	now := time.Now().Unix()
	keys := make([]string, 0, len(p.kv))
	for k, e := range p.kv {
		if e.exp > 0 && e.exp <= now {
			delete(p.kv, k)
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
		out = append(out, core.KeyValue{Key: k, Value: p.kv[k].val})
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
	if e, ok := p.alive(key, time.Now().Unix()); ok {
		e.exp = time.Now().Unix() + ttl
	}
	return nil
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	_ = ctx
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkOpen(); err != nil {
		return 0, false, err
	}
	e, ok := p.alive(key, time.Now().Unix())
	if !ok || e.exp == 0 {
		return -1, false, nil
	}
	return e.exp - time.Now().Unix(), true, nil
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
	now := time.Now().Unix()
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
	return nil
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
		return l.Back().Value.([]byte), true, nil
	}
	return l.Front().Value.([]byte), true, nil
}

// ---- ZSet ----

func (p *Provider) ZSet(_ context.Context, name, key string, score int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkOpen(); err != nil {
		return err
	}
	m := p.zset[name]
	if m == nil {
		m = make(map[string]int64)
		p.zset[name] = m
	}
	m[key] = score
	return nil
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
	if m := p.zset[name]; m != nil {
		delete(m, key)
	}
	return nil
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
	m := p.zset[name]
	if m == nil {
		m = make(map[string]int64)
		p.zset[name] = m
	}
	m[key] += delta
	return m[key], nil
}

// ---- 工具 ----

// normalizeLimit 保证 limit<=0 时使用与 kvdb.DefaultScanLimit 一致的分页大小
// （基座包避免反向依赖根包，此处内联同一常量的语义并注释对齐）。
func normalizeLimit(limit int) int {
	const defaultLimit = 100 // 与 core.DefaultScanLimit 对齐
	if limit <= 0 {
		return defaultLimit
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
