package badger

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/RelicOfTesla/kvdb/core"

	badgerdb "github.com/dgraph-io/badger/v4"
)

// ---- Queue ----
//
// 队列元素键为 nsQueue + lp(name) + be64(ordered(seq))：同一队列内按 seq 有序，
// 队尾追加用递增 seq、队头插入用递减 seq（与 bolt / leveldb / sqlstore 的序号模型
// 一致）。计数器单独一条记录，让 QSize 是 O(1) 而不是全量扫描。

type qCounters struct {
	next  int64  // 队尾下一个序号
	front int64  // 队头下一个序号（先 -- 再使用）
	count uint64 // 元素数
}

func qCountersGet(txn *badgerdb.Txn, name string) (qCounters, error) {
	v, ok, err := txnGet(txn, qSeqKey(name))
	if err != nil || !ok {
		return qCounters{}, err
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

func putQCounters(txn *badgerdb.Txn, name string, c qCounters) error {
	buf := make([]byte, qCountersLen)
	binary.BigEndian.PutUint64(buf[qNextOff:qFrontOff], uint64(c.next))
	binary.BigEndian.PutUint64(buf[qFrontOff:qCountOff], uint64(c.front))
	binary.BigEndian.PutUint64(buf[qCountOff:qCountersLen], c.count)
	return txn.Set(qSeqKey(name), buf)
}

func (p *Provider) QPush(ctx context.Context, name string, value []byte) error {
	return p.qpush(ctx, name, value, false)
}

func (p *Provider) QPushFront(ctx context.Context, name string, value []byte) error {
	return p.qpush(ctx, name, value, true)
}

func (p *Provider) qpush(ctx context.Context, name string, value []byte, front bool) error {
	_ = ctx
	mu := p.keyMutex("q:" + name)
	mu.Lock()
	defer mu.Unlock()

	return p.update("qpush", func(txn *badgerdb.Txn) error {
		c, err := qCountersGet(txn, name)
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
		if err := txn.Set(qItemKey(name, seq), value); err != nil {
			return err
		}
		return putQCounters(txn, name, c)
	})
}

// qSeek 定位队头（back=false，最小 seq）或队尾（back=true，最大 seq），返回
// 当前项的键与值副本（键要用于后续删除，故必须拷贝）。
func qSeek(txn *badgerdb.Txn, name string, back bool) (k, v []byte, ok bool, err error) {
	prefix := keyOf(nsQueue, lp(name))
	opts := badgerdb.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.Reverse = back
	it := txn.NewIterator(opts)
	defer it.Close()
	if !back {
		it.Seek(prefix) // 正向：第一个 >= prefix 的键即队头
	} else if end := prefixEnd(prefix); end != nil {
		it.Seek(end) // 反向：seek 到前缀的后继，落点即队尾（见 prefixEnd）
	} else {
		it.Rewind()
	}
	if !it.Valid() {
		return nil, nil, false, nil
	}
	item := it.Item()
	k = item.KeyCopy(nil)
	v, err = item.ValueCopy(nil)
	if err != nil {
		return nil, nil, false, err
	}
	return k, v, true, nil
}

func (p *Provider) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpop(ctx, name, false)
}

func (p *Provider) QPopBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpop(ctx, name, true)
}

func (p *Provider) qpop(ctx context.Context, name string, back bool) ([]byte, bool, error) {
	_ = ctx
	mu := p.keyMutex("q:" + name)
	mu.Lock()
	defer mu.Unlock()

	var v []byte
	found := false
	err := p.update("qpop", func(txn *badgerdb.Txn) error {
		k, val, ok, err := qSeek(txn, name, back)
		if err != nil || !ok {
			return err
		}
		c, err := qCountersGet(txn, name)
		if err != nil {
			return err
		}
		if c.count > 0 {
			c.count--
		}
		if err := txn.Delete(k); err != nil {
			return err
		}
		if err := putQCounters(txn, name, c); err != nil {
			return err
		}
		v, found = val, true // qSeek 已返回值副本，且该条目已在本事务内删除
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return v, found, nil
}

func (p *Provider) QSize(ctx context.Context, name string) (int64, error) {
	_ = ctx
	var n int64
	err := p.view("qsize", func(txn *badgerdb.Txn) error {
		c, err := qCountersGet(txn, name)
		n = int64(c.count)
		return err
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
	var (
		v  []byte
		ok bool
	)
	err := p.view("qpeek", func(txn *badgerdb.Txn) error {
		_, val, found, err := qSeek(txn, name, back)
		if err != nil || !found {
			return err
		}
		v, ok = val, true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return v, ok, nil
}

// ---- ZSet ----
//
// 排序侧键 nsZScore + lp(name) + be64(ordered(score)) + member，成员分数索引键
// nsZMember + lp(name) + member；两者在同一事务内维护，保证不会出现单边残留。
// 成员数单独记录，使 ZSize 为 O(1)。

func zScoreGet(txn *badgerdb.Txn, name, member string) (int64, bool, error) {
	v, ok, err := txnGet(txn, zMemberKey(name, member))
	if err != nil || !ok {
		return 0, false, err
	}
	if len(v) != be64Len {
		return 0, false, nil
	}
	return unorder(binary.BigEndian.Uint64(v)), true, nil
}

func zCountGet(txn *badgerdb.Txn, name string) (int64, error) {
	v, ok, err := txnGet(txn, zCountKey(name))
	if err != nil || !ok {
		return 0, err
	}
	if len(v) != be64Len {
		return 0, nil
	}
	return int64(binary.BigEndian.Uint64(v)), nil
}

// zPut 维护一对索引：新排序键 + 成员分数索引；分数变化时清掉旧排序键并更新计数。
func zPut(txn *badgerdb.Txn, name, member string, score int64) error {
	old, existed, err := zScoreGet(txn, name, member)
	if err != nil {
		return err
	}
	if err := txn.Set(zScoreKey(name, score, member), nil); err != nil {
		return err
	}
	if err := txn.Set(zMemberKey(name, member), be64(ordered(score))); err != nil {
		return err
	}
	if !existed {
		n, err := zCountGet(txn, name)
		if err != nil {
			return err
		}
		return txn.Set(zCountKey(name), be64(uint64(n+1)))
	}
	if old != score {
		return txn.Delete(zScoreKey(name, old, member)) // 分数变化：清掉旧排序键，避免残留
	}
	return nil
}

func (p *Provider) ZSet(ctx context.Context, name, key string, score int64) error {
	_ = ctx
	mu := p.keyMutex("z:" + name)
	mu.Lock()
	defer mu.Unlock()

	return p.update("zset", func(txn *badgerdb.Txn) error {
		return zPut(txn, name, key, score)
	})
}

func (p *Provider) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	_ = ctx
	var (
		score int64
		ok    bool
	)
	err := p.view("zget", func(txn *badgerdb.Txn) error {
		var err error
		score, ok, err = zScoreGet(txn, name, key)
		return err
	})
	return score, ok, err
}

func (p *Provider) ZDel(ctx context.Context, name, key string) error {
	_ = ctx
	mu := p.keyMutex("z:" + name)
	mu.Lock()
	defer mu.Unlock()

	return p.update("zdel", func(txn *badgerdb.Txn) error {
		score, existed, err := zScoreGet(txn, name, key)
		if err != nil || !existed {
			return err
		}
		n, err := zCountGet(txn, name)
		if err != nil {
			return err
		}
		if err := txn.Delete(zScoreKey(name, score, key)); err != nil {
			return err
		}
		if err := txn.Delete(zMemberKey(name, key)); err != nil {
			return err
		}
		if n > 0 {
			return txn.Set(zCountKey(name), be64(uint64(n-1)))
		}
		return nil
	})
}

func (p *Provider) ZSize(ctx context.Context, name string) (int64, error) {
	_ = ctx
	var n int64
	err := p.view("zsize", func(txn *badgerdb.Txn) error {
		var err error
		n, err = zCountGet(txn, name)
		return err
	})
	return n, err
}

func (p *Provider) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	_ = ctx
	var (
		rank int64
		ok   bool
	)
	err := p.view("zrank", func(txn *badgerdb.Txn) error {
		score, existed, err := zScoreGet(txn, name, key)
		if err != nil || !existed {
			return err
		}
		ok = true
		return zEach(txn, name, func(s int64, m string) bool {
			if s < score || (s == score && m < key) {
				rank++
			}
			return true
		})
	})
	if err != nil {
		return 0, false, err
	}
	return rank, ok, nil
}

// zEach 按 (score 升序, member 升序) 遍历某 zset 的成员。
func zEach(txn *badgerdb.Txn, name string, fn func(score int64, member string) bool) error {
	prefix := keyOf(nsZScore, lp(name))
	opts := badgerdb.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false // 排序侧的值恒为 nil，取值纯属浪费
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		k := it.Item().Key()
		if len(k) < len(prefix)+be64Len {
			break
		}
		body := k[len(prefix):]
		score := unorder(binary.BigEndian.Uint64(body[:be64Len]))
		if !fn(score, string(body[be64Len:])) {
			return nil
		}
	}
	return nil
}

func (p *Provider) ZRange(ctx context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	_ = ctx
	out := []core.ZItem{}
	err := p.view("zrange", func(txn *badgerdb.Txn) error {
		total, err := zCountGet(txn, name)
		if err != nil {
			return err
		}
		lo, hi, ok := normalizeRange(start, stop, total)
		if !ok {
			return nil
		}
		out = make([]core.ZItem, 0, minCap(int(hi-lo+1)))
		idx := int64(0)
		return zEach(txn, name, func(s int64, m string) bool {
			if idx > hi {
				return false
			}
			if idx >= lo {
				out = append(out, core.ZItem{Key: m, Score: s})
			}
			idx++
			return true
		})
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
	mu := p.keyMutex("z:" + name)
	mu.Lock()
	defer mu.Unlock()

	var next int64
	err := p.update("zincr", func(txn *badgerdb.Txn) error {
		old, _, err := zScoreGet(txn, name, key)
		if err != nil {
			return err
		}
		next = old + delta
		return zPut(txn, name, key, next)
	})
	if err != nil {
		return 0, err
	}
	return next, nil
}

// ---- Batch ----

// BatchComposed 声明批内可见性：整批在**一个** Badger 事务里按声明顺序应用，
// 后续操作读的是同一个事务（read-your-writes），因此同批覆盖、同队列按序入队、
// zset 的 Set+Incr 累加都成立。
func (p *Provider) BatchComposed() bool { return true }

// ApplyBatch 把整批操作放进一个 db.Update 事务：要么全部生效、要么全部不生效。
// now 在批内采样一次，避免批内 TTL 语义漂移。
func (p *Provider) ApplyBatch(ctx context.Context, ops []core.BatchOp) error {
	_ = ctx
	if len(ops) == 0 {
		return nil
	}
	if err := validateOps(ops); err != nil {
		return err
	}
	now := core.NowUnix()
	// 队列/zset 的计数与双侧索引需要读当前状态：按涉及的名称加锁，与单条写路径
	// 共用同一把分片锁，避免并发批互相覆盖计数。
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

	return p.update("batch", func(txn *badgerdb.Txn) error {
		for _, op := range ops {
			if err := applyOne(txn, op, now); err != nil {
				return err
			}
		}
		return nil
	})
}

// validateOps 先整批校验参数，避免"改到一半才发现非法"（虽然事务会回滚，但提前
// 失败能给出更准确的错误来源）。
func validateOps(ops []core.BatchOp) error {
	for _, op := range ops {
		switch op.Kind {
		case core.BatchSetEx, core.BatchExpire:
			if op.TTL <= 0 {
				return core.ErrInvalidTTL
			}
		case core.BatchSet, core.BatchDel, core.BatchQPush, core.BatchQPushFront,
			core.BatchZSet, core.BatchZDel, core.BatchZIncr:
		default:
			return fmt.Errorf("badger: unknown batch op %d", op.Kind)
		}
	}
	return nil
}

func applyOne(txn *badgerdb.Txn, op core.BatchOp, now int64) error {
	switch op.Kind {
	case core.BatchSet:
		if err := txn.Set(kvKey(op.Key), op.Value); err != nil {
			return err
		}
		exp, has, err := ttlValueOf(txn, op.Key)
		if err != nil {
			return err
		}
		if has && exp <= now {
			return txn.Delete(ttlKey(op.Key))
		}
		return nil
	case core.BatchSetEx:
		if err := txn.Set(kvKey(op.Key), op.Value); err != nil {
			return err
		}
		return txn.Set(ttlKey(op.Key), be64(uint64(core.AddTTL(now, op.TTL))))
	case core.BatchDel:
		if err := txn.Delete(kvKey(op.Key)); err != nil {
			return err
		}
		return txn.Delete(ttlKey(op.Key))
	case core.BatchExpire:
		return txn.Set(ttlKey(op.Key), be64(uint64(core.AddTTL(now, op.TTL))))
	case core.BatchQPush:
		return batchQPush(txn, op.Key, op.Value, false)
	case core.BatchQPushFront:
		return batchQPush(txn, op.Key, op.Value, true)
	case core.BatchZSet:
		return zPut(txn, op.Key, op.Member, op.Score)
	case core.BatchZDel:
		return batchZDel(txn, op.Key, op.Member)
	case core.BatchZIncr:
		old, _, err := zScoreGet(txn, op.Key, op.Member)
		if err != nil {
			return err
		}
		return zPut(txn, op.Key, op.Member, old+op.Delta)
	default:
		return fmt.Errorf("badger: unknown batch op %d", op.Kind)
	}
}

func batchQPush(txn *badgerdb.Txn, name string, value []byte, front bool) error {
	c, err := qCountersGet(txn, name)
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
	if err := txn.Set(qItemKey(name, seq), value); err != nil {
		return err
	}
	return putQCounters(txn, name, c)
}

func batchZDel(txn *badgerdb.Txn, name, member string) error {
	score, existed, err := zScoreGet(txn, name, member)
	if err != nil || !existed {
		return err
	}
	n, err := zCountGet(txn, name)
	if err != nil {
		return err
	}
	if err := txn.Delete(zScoreKey(name, score, member)); err != nil {
		return err
	}
	if err := txn.Delete(zMemberKey(name, member)); err != nil {
		return err
	}
	if n > 0 {
		return txn.Set(zCountKey(name), be64(uint64(n-1)))
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
