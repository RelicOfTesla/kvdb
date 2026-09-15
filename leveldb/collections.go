package leveldb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"sync"

	"github.com/RelicOfTesla/kvdb/core"

	gldb "github.com/syndtr/goleveldb/leveldb"
	gerr "github.com/syndtr/goleveldb/leveldb/errors"
)

// ---- Queue ----
//
// 队列元素键为 nsQueue + lp(name) + be64(ordered(seq))：同一队列内按 seq 有序，
// 队尾追加用递增 seq、队头插入用递减 seq（与 bolt / sqlstore 的序号模型一致）。
// 计数器单独一条记录，让 QSize 是 O(1) 而不是全量扫描。

type qCounters struct {
	next  int64  // 队尾下一个序号
	front int64  // 队头下一个序号（先 -- 再使用）
	count uint64 // 元素数
}

func (p *Provider) qCountersGet(name string) (qCounters, error) {
	v, err := p.db.Get(qSeqKey(name), nil)
	if errors.Is(err, gerr.ErrNotFound) {
		return qCounters{}, nil
	}
	if err != nil {
		return qCounters{}, fmt.Errorf("leveldb: qsize: %w", err)
	}
	if len(v) != qCountersLen {
		return qCounters{}, nil // 记录异常按空队列处理
	}
	return qCounters{
		next:  int64(binary.BigEndian.Uint64(v[qNextOff:qFrontOff])),
		front: int64(binary.BigEndian.Uint64(v[qFrontOff:qCountOff])),
		count: binary.BigEndian.Uint64(v[qCountOff:qCountersLen]),
	}, nil
}

func putQCounters(b *gldb.Batch, name string, c qCounters) {
	buf := make([]byte, qCountersLen)
	binary.BigEndian.PutUint64(buf[qNextOff:qFrontOff], uint64(c.next))
	binary.BigEndian.PutUint64(buf[qFrontOff:qCountOff], uint64(c.front))
	binary.BigEndian.PutUint64(buf[qCountOff:qCountersLen], c.count)
	b.Put(qSeqKey(name), buf)
}

func (p *Provider) QPush(ctx context.Context, name string, value []byte) error {
	return p.qpush(ctx, name, value, false)
}

func (p *Provider) QPushFront(ctx context.Context, name string, value []byte) error {
	return p.qpush(ctx, name, value, true)
}

func (p *Provider) qpush(ctx context.Context, name string, value []byte, front bool) error {
	_ = ctx
	if err := p.check(); err != nil {
		return err
	}
	mu := p.keyMutex("q:" + name)
	mu.Lock()
	defer mu.Unlock()

	c, err := p.qCountersGet(name)
	if err != nil {
		return err
	}
	var seq int64
	if front {
		c.front--
		seq = c.front
	} else {
		seq = c.next
		c.next++
	}
	c.count++
	var b gldb.Batch
	b.Put(qItemKey(name, seq), value)
	putQCounters(&b, name, c)
	return p.write(&b, "qpush")
}

// qSeek 定位队头（back=false，最小 seq）或队尾（back=true，最大 seq），返回
// 当前项的键与值副本（键要用于后续 Delete，故必须拷贝）。
// NewIterator(r) 已把游标限制在 [r.Start, r.Limit) 内，无需再自行判界。
func (p *Provider) qSeek(name string, back bool) (k, v []byte, ok bool, err error) {
	r := nsRange(nsQueue, lp(name)...)
	it := p.db.NewIterator(r, nil)
	defer it.Release()
	if back {
		for x := it.Last(); x; x = it.Prev() {
			return append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...), true, nil
		}
	} else {
		for x := it.First(); x; x = it.Next() {
			return append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...), true, nil
		}
	}
	if ierr := it.Error(); ierr != nil {
		return nil, nil, false, fmt.Errorf("leveldb: queue seek: %w", ierr)
	}
	return nil, nil, false, nil
}

// inRange 判定键是否落在 [r.Start, r.Limit) 内（Limit 为 nil 表示无上界）。
func inRange(k []byte, r interface {
	Contains([]byte) bool
}) bool {
	return r.Contains(k)
}

func (p *Provider) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpop(ctx, name, false)
}

func (p *Provider) QPopBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpop(ctx, name, true)
}

func (p *Provider) qpop(ctx context.Context, name string, back bool) ([]byte, bool, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return nil, false, err
	}
	mu := p.keyMutex("q:" + name)
	mu.Lock()
	defer mu.Unlock()

	k, v, ok, err := p.qSeek(name, back)
	if err != nil || !ok {
		return nil, false, err
	}
	c, err := p.qCountersGet(name)
	if err != nil {
		return nil, false, err
	}
	if c.count > 0 {
		c.count--
	}
	var b gldb.Batch
	b.Delete(k)
	putQCounters(&b, name, c)
	if err := p.write(&b, "qpop"); err != nil {
		return nil, false, err
	}
	return v, true, nil // qSeek 已返回副本，且该条目已从库里删除
}

func (p *Provider) QSize(ctx context.Context, name string) (int64, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return 0, err
	}
	c, err := p.qCountersGet(name)
	if err != nil {
		return 0, err
	}
	return int64(c.count), nil
}

func (p *Provider) QFront(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpeek(ctx, name, false)
}

func (p *Provider) QBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpeek(ctx, name, true)
}

func (p *Provider) qpeek(ctx context.Context, name string, back bool) ([]byte, bool, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return nil, false, err
	}
	_, v, ok, err := p.qSeek(name, back)
	if err != nil || !ok {
		return nil, false, err
	}
	return v, true, nil
}

// parseQItemKey 从队列元素键还原 (name, seq)。
func parseQItemKey(k []byte) (string, int64, bool) {
	if len(k) < 2 || k[0] != nsQueue {
		return "", 0, false
	}
	name, adv := readName(k[1:])
	if adv <= 0 || len(k)-1-adv < be64Len {
		return "", 0, false
	}
	u := binary.BigEndian.Uint64(k[1+adv : 1+adv+be64Len])
	return name, unorder(u), true
}

// readName 解出「uvarint 长度前缀 + 名字」并返回消耗字节数（含前缀本身）。
func readName(b []byte) (string, int) {
	n, adv := binary.Uvarint(b)
	if adv <= 0 || uint64(len(b)-adv) < n {
		return "", 0
	}
	return string(b[adv : adv+int(n)]), adv + int(n)
}

// ---- ZSet ----
//
// 排序侧键 nsZScore + lp(name) + be64(ordered(score)) + member，成员分数索引键
// nsZMember + lp(name) + member；两者在同一 Batch 内维护，保证不会出现单边残留。
// 成员数单独记录，使 ZSize 为 O(1)。

func (p *Provider) zScoreGet(name, member string) (int64, bool, error) {
	v, err := p.db.Get(zMemberKey(name, member), nil)
	if errors.Is(err, gerr.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("leveldb: zget: %w", err)
	}
	if len(v) != be64Len {
		return 0, false, nil
	}
	return unorder(binary.BigEndian.Uint64(v)), true, nil
}

func (p *Provider) zCountGet(name string) (int64, error) {
	v, err := p.db.Get(zCountKey(name), nil)
	if errors.Is(err, gerr.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("leveldb: zsize: %w", err)
	}
	if len(v) != be64Len {
		return 0, nil
	}
	return int64(binary.BigEndian.Uint64(v)), nil
}

func (p *Provider) ZSet(ctx context.Context, name, key string, score int64) error {
	_ = ctx
	if err := p.check(); err != nil {
		return err
	}
	mu := p.keyMutex("z:" + name)
	mu.Lock()
	defer mu.Unlock()

	old, existed, err := p.zScoreGet(name, key)
	if err != nil {
		return err
	}
	var b gldb.Batch
	b.Put(zScoreKey(name, score, key), nil)
	b.Put(zMemberKey(name, key), be64(ordered(score)))
	if !existed {
		n, err := p.zCountGet(name)
		if err != nil {
			return err
		}
		b.Put(zCountKey(name), be64(uint64(n+1)))
	} else if old != score {
		b.Delete(zScoreKey(name, old, key)) // 分数变化：清掉旧排序键，避免残留
	}
	return p.write(&b, "zset")
}

func (p *Provider) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return 0, false, err
	}
	return p.zScoreGet(name, key)
}

func (p *Provider) ZDel(ctx context.Context, name, key string) error {
	_ = ctx
	if err := p.check(); err != nil {
		return err
	}
	mu := p.keyMutex("z:" + name)
	mu.Lock()
	defer mu.Unlock()

	score, existed, err := p.zScoreGet(name, key)
	if err != nil || !existed {
		return err
	}
	n, err := p.zCountGet(name)
	if err != nil {
		return err
	}
	var b gldb.Batch
	b.Delete(zScoreKey(name, score, key))
	b.Delete(zMemberKey(name, key))
	if n > 0 {
		b.Put(zCountKey(name), be64(uint64(n-1)))
	}
	return p.write(&b, "zdel")
}

func (p *Provider) ZSize(ctx context.Context, name string) (int64, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return 0, err
	}
	return p.zCountGet(name)
}

func (p *Provider) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return 0, false, err
	}
	score, existed, err := p.zScoreGet(name, key)
	if err != nil || !existed {
		return 0, false, err
	}
	var rank int64
	err = p.zEach(name, func(s int64, m string) bool {
		if s < score || (s == score && m < key) {
			rank++
		}
		return true
	})
	if err != nil {
		return 0, false, err
	}
	return rank, true, nil
}

// zEach 按 (score 升序, member 升序) 遍历某 zset 的成员。
func (p *Provider) zEach(name string, fn func(score int64, member string) bool) error {
	r := nsRange(nsZScore, lp(name)...)
	it := p.db.NewIterator(r, nil)
	defer it.Release()
	prefix := lp(name)
	for it.Seek(r.Start); it.Valid(); it.Next() {
		k := it.Key()
		if len(k) < 1 || k[0] != nsZScore {
			break
		}
		rest := k[1:]
		if len(rest) < len(prefix)+be64Len || !hasPrefixBytes(rest, prefix) {
			break
		}
		body := rest[len(prefix):]
		score := unorder(binary.BigEndian.Uint64(body[:be64Len]))
		member := string(body[be64Len:])
		if !fn(score, member) {
			return nil
		}
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("leveldb: zrange: %w", err)
	}
	return nil
}

func hasPrefixBytes(b, prefix []byte) bool { return bytes.HasPrefix(b, prefix) }

func (p *Provider) ZRange(ctx context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return nil, err
	}
	total, err := p.zCountGet(name)
	if err != nil {
		return nil, err
	}
	lo, hi, ok := normalizeRange(start, stop, total)
	if !ok {
		return nil, nil
	}
	out := make([]core.ZItem, 0, minCap(int(hi-lo+1)))
	idx := int64(0)
	err = p.zEach(name, func(s int64, m string) bool {
		if idx > hi {
			return false
		}
		if idx >= lo {
			out = append(out, core.ZItem{Key: m, Score: s})
		}
		idx++
		return true
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// normalizeRange 把 Redis ZRANGE 式索引（0 起闭区间、负数从末尾数）折算成
// 正向下标区间；返回 ok=false 表示区间为空。
func normalizeRange(start, stop, n int64) (lo, hi int64, ok bool) {
	if start < 0 {
		start += n
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += n
	}
	if n == 0 || start >= n || stop < start {
		return 0, 0, false
	}
	if stop >= n {
		stop = n - 1
	}
	return start, stop, true
}

func (p *Provider) ZIncr(ctx context.Context, name, key string, delta int64) (int64, error) {
	_ = ctx
	if err := p.check(); err != nil {
		return 0, err
	}
	mu := p.keyMutex("z:" + name)
	mu.Lock()
	defer mu.Unlock()

	old, existed, err := p.zScoreGet(name, key)
	if err != nil {
		return 0, err
	}
	next := old + delta
	var b gldb.Batch
	b.Put(zScoreKey(name, next, key), nil)
	b.Put(zMemberKey(name, key), be64(ordered(next)))
	if !existed {
		n, err := p.zCountGet(name)
		if err != nil {
			return 0, err
		}
		b.Put(zCountKey(name), be64(uint64(n+1)))
	} else if old != next {
		b.Delete(zScoreKey(name, old, key))
	}
	if err := p.write(&b, "zincr"); err != nil {
		return 0, err
	}
	return next, nil
}

// ---- Batch ----

// ApplyBatch 把整批操作收进**一个** LevelDB Batch 提交：Write 原子，因此
// 要么全部生效、要么全部不生效。now 在批内采样一次，避免批内 TTL 语义漂移。
func (p *Provider) ApplyBatch(ctx context.Context, ops []core.BatchOp) error {
	_ = ctx
	if err := p.check(); err != nil {
		return err
	}
	if len(ops) == 0 {
		return nil
	}
	for _, op := range ops {
		switch op.Kind {
		case core.BatchSetEx, core.BatchExpire:
			if op.TTL <= 0 {
				return core.ErrInvalidTTL
			}
		case core.BatchSet, core.BatchDel, core.BatchQPush, core.BatchQPushFront,
			core.BatchZSet, core.BatchZDel, core.BatchZIncr:
		default:
			return fmt.Errorf("leveldb: unknown batch op %d", op.Kind)
		}
	}
	now := core.NowUnix()
	var b gldb.Batch
	// 队列/zset 的计数与双侧索引需要读当前状态：按涉及的名称加锁，
	// 与单条写路径共用同一把分片锁，避免并发批互相覆盖计数。
	names := map[string]bool{}
	for _, op := range ops {
		switch op.Kind {
		case core.BatchQPush, core.BatchQPushFront:
			names["q:"+op.Key] = true
		case core.BatchZSet, core.BatchZDel, core.BatchZIncr:
			names["z:"+op.Key] = true
		}
	}
	unlock := p.lockKeys(sortedKeys(names))
	defer unlock()

	for _, op := range ops {
		var err error
		switch op.Kind {
		case core.BatchSet:
			b.Put(kvKey(op.Key), op.Value)
			if exp, has, herr := p.ttlValueOf(op.Key); herr != nil {
				return herr
			} else if has && exp <= now {
				b.Delete(ttlKey(op.Key))
			}
		case core.BatchSetEx:
			b.Put(kvKey(op.Key), op.Value)
			b.Put(ttlKey(op.Key), be64(uint64(core.AddTTL(now, op.TTL))))
		case core.BatchDel:
			b.Delete(kvKey(op.Key))
			b.Delete(ttlKey(op.Key))
		case core.BatchExpire:
			b.Put(ttlKey(op.Key), be64(uint64(core.AddTTL(now, op.TTL))))
		case core.BatchQPush:
			if err = p.batchQPush(&b, op.Key, op.Value, false); err != nil {
				return err
			}
		case core.BatchQPushFront:
			if err = p.batchQPush(&b, op.Key, op.Value, true); err != nil {
				return err
			}
		case core.BatchZSet:
			if err = p.batchZSet(&b, op.Key, op.Member, op.Score); err != nil {
				return err
			}
		case core.BatchZDel:
			if err = p.batchZDel(&b, op.Key, op.Member); err != nil {
				return err
			}
		case core.BatchZIncr:
			if err = p.batchZIncr(&b, op.Key, op.Member, op.Delta); err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
	}
	return p.write(&b, "batch")
}

func (p *Provider) batchQPush(b *gldb.Batch, name string, value []byte, front bool) error {
	c, err := p.qCountersGet(name)
	if err != nil {
		return err
	}
	var seq int64
	if front {
		c.front--
		seq = c.front
	} else {
		seq = c.next
		c.next++
	}
	c.count++
	b.Put(qItemKey(name, seq), value)
	putQCounters(b, name, c)
	return nil
}

func (p *Provider) batchZSet(b *gldb.Batch, name, member string, score int64) error {
	old, existed, err := p.zScoreGet(name, member)
	if err != nil {
		return err
	}
	b.Put(zScoreKey(name, score, member), nil)
	b.Put(zMemberKey(name, member), be64(ordered(score)))
	if !existed {
		n, err := p.zCountGet(name)
		if err != nil {
			return err
		}
		b.Put(zCountKey(name), be64(uint64(n+1)))
	} else if old != score {
		b.Delete(zScoreKey(name, old, member))
	}
	return nil
}

func (p *Provider) batchZDel(b *gldb.Batch, name, member string) error {
	score, existed, err := p.zScoreGet(name, member)
	if err != nil || !existed {
		return err
	}
	n, err := p.zCountGet(name)
	if err != nil {
		return err
	}
	b.Delete(zScoreKey(name, score, member))
	b.Delete(zMemberKey(name, member))
	if n > 0 {
		b.Put(zCountKey(name), be64(uint64(n-1)))
	}
	return nil
}

func (p *Provider) batchZIncr(b *gldb.Batch, name, member string, delta int64) error {
	old, existed, err := p.zScoreGet(name, member)
	if err != nil {
		return err
	}
	next := old + delta
	b.Put(zScoreKey(name, next, member), nil)
	b.Put(zMemberKey(name, member), be64(ordered(next)))
	if !existed {
		n, err := p.zCountGet(name)
		if err != nil {
			return err
		}
		b.Put(zCountKey(name), be64(uint64(n+1)))
	} else if old != next {
		b.Delete(zScoreKey(name, old, member))
	}
	return nil
}

// lockKeys 按稳定顺序加锁（避免不同批之间的死锁），返回一次性解锁函数。
func (p *Provider) lockKeys(keys []string) func() {
	mu := make([]*sync.Mutex, 0, len(keys))
	for _, k := range keys {
		m := p.keyMutex(k)
		m.Lock()
		mu = append(mu, m)
	}
	return func() {
		for i := len(mu) - 1; i >= 0; i-- {
			mu[i].Unlock()
		}
	}
}

// sortedKeys 返回稳定顺序，供 lockKeys 按同一次序加锁。
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
