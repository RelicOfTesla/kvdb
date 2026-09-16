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
	if err := core.CheckKey(name); err != nil {
		return err
	}
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
	if err := core.CheckKey(name); err != nil {
		return nil, false, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return 0, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return nil, false, err
	}
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

// qEach 按 seq 升序（即队头 → 队尾）遍历某队列的元素，fn 返回 false 即提前停止。
// 键形如 nsQueue + lp(name) + be64(ordered(seq))，前缀扫描天然按 seq 升序。
func qEach(txn *badgerdb.Txn, name string, fn func(v []byte) bool) error {
	prefix := keyOf(nsQueue, lp(name))
	opts := badgerdb.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		v, err := it.Item().ValueCopy(nil) // 必须拷贝：Badger 复用值缓冲
		if err != nil {
			return err
		}
		if !fn(v) {
			return nil
		}
	}
	return nil
}

// QRange 只读返回 [start, stop] 区间内的元素，方向为队头 → 队尾。
// 索引语义与 ZRange 一致（0 起闭区间、负索引从末尾数、越界裁剪，空区间返回 nil）。
func (p *Provider) QRange(ctx context.Context, name string, start, stop int64) ([][]byte, error) {
	_ = ctx
	if err := core.CheckKey(name); err != nil {
		return nil, err
	}
	var out [][]byte
	err := p.view("qrange", func(txn *badgerdb.Txn) error {
		c, err := qCountersGet(txn, name) // 与 QSize 同源：计数器记录
		if err != nil {
			return err
		}
		lo, hi, ok := normalizeRange(start, stop, int64(c.count))
		if !ok {
			return nil
		}
		out = make([][]byte, 0, minCap(int(hi-lo+1)))
		idx := int64(0)
		return qEach(txn, name, func(v []byte) bool {
			if idx > hi {
				return false
			}
			if idx >= lo {
				out = append(out, v) // qEach 已给副本
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
	if err := core.CheckKeys(name, key); err != nil {
		return err
	}
	mu := p.keyMutex("z:" + name)
	mu.Lock()
	defer mu.Unlock()

	return p.update("zset", func(txn *badgerdb.Txn) error {
		return zPut(txn, name, key, score)
	})
}

func (p *Provider) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	_ = ctx
	if err := core.CheckKeys(name, key); err != nil {
		return 0, false, err
	}
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
	if err := core.CheckKeys(name, key); err != nil {
		return err
	}
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
	if err := core.CheckKey(name); err != nil {
		return 0, err
	}
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
	if err := core.CheckKeys(name, key); err != nil {
		return 0, false, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return nil, err
	}
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

// zEachReverse 按 (score 降序, member 降序) 反向遍历某 zset 的成员。
// badger 的反向迭代器（opts.Reverse）仍按 key 的逆序输出，故同分数内的成员
// 也是逆序——调用方若需要同分升序，需自行翻正（见 zScoreWindow）。
func zEachReverse(txn *badgerdb.Txn, name string, fn func(score int64, member string) bool) error {
	prefix := keyOf(nsZScore, lp(name))
	opts := badgerdb.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.Reverse = true
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	// 反向迭代必须 seek 到前缀的上界才能落到最后一个元素；同前缀末元素 = 前缀+0xff。
	start := append(append([]byte(nil), prefix...), 0xff)
	for it.Seek(start); it.Valid(); it.Next() {
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

// zScoreWindow 在排序侧索引 nsZScore 上按**分数闭区间** [min, max] 取窗口。
//
// 注意 normalizeRange 是 ZRANGE 的**索引**区间，这里是**分数**区间，不可互换。
// desc 只改遍历方向，不改变参数含义：始终 min<=max，min>max 直接判空。
// limit>0 时"按遍历方向取前 limit 个"——降序即**最高分**那一端，因此降序
// 边走边截断（而非先取完再截），与 mem.zScoreWindow 的截断方向一致。
//
// 键为 lp(name)+be64(ordered(score))+member，键序即 (分数升序, 成员升序)；
// 降序时反向迭代会把同分数段也倒转，故把**每个同分数段**内部翻回升序。
func (p *Provider) zScoreWindow(txn *badgerdb.Txn, name string, min, max int64, limit int, desc bool) ([]core.ZItem, error) {
	if min > max {
		return nil, nil
	}
	out := []core.ZItem{}
	if !desc {
		out = make([]core.ZItem, 0, minCap(limit))
		err := zEach(txn, name, func(s int64, m string) bool {
			if s < min {
				return true // 还没进入区间
			}
			if s > max {
				return false // 越过上界：升序遍历可安全提前结束
			}
			out = append(out, core.ZItem{Key: m, Score: s})
			return limit <= 0 || len(out) < limit
		})
		if err != nil {
			return nil, err
		}
		if limit > 0 && len(out) > limit {
			out = out[:limit]
		}
		return out, nil
	}

	out = make([]core.ZItem, 0, minCap(limit))
	var run []core.ZItem // 刚收集到、尚未输出的同分数成员（当前为降序）
	flush := func() {
		for i, j := 0, len(run)-1; i < j; i, j = i+1, j-1 {
			run[i], run[j] = run[j], run[i]
		}
		out = append(out, run...)
	}
	err := zEachReverse(txn, name, func(s int64, m string) bool {
		if s > max {
			return true // 还没进入区间
		}
		if s < min {
			return false // 越过下界：降序遍历可安全提前结束
		}
		if len(run) > 0 && run[0].Score != s {
			flush()
			run = run[:0]
			if limit > 0 && len(out) >= limit {
				return false
			}
		}
		run = append(run, core.ZItem{Key: m, Score: s})
		if limit > 0 && len(out)+len(run) >= limit {
			flush() // 本分数段已凑满：翻正后截断即可，无需再往前扫
			run = run[:0]
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if len(run) > 0 {
		flush()
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ZRangeByScore 返回分数落在闭区间 [min, max] 内的成员；desc 只改遍历方向。
func (p *Provider) ZRangeByScore(ctx context.Context, name string, min, max int64, limit int, desc bool) ([]core.ZItem, error) {
	_ = ctx
	if err := core.CheckKey(name); err != nil {
		return nil, err
	}
	var out []core.ZItem
	err := p.view("zrangebyscore", func(txn *badgerdb.Txn) error {
		items, err := p.zScoreWindow(txn, name, min, max, limit, desc)
		if err != nil {
			return err
		}
		out = items
		return nil
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
	if err := core.CheckKeys(name, key); err != nil {
		return 0, err
	}
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
		// 空 key/队列名/zset 名（以及 zset 成员）整批拒绝：与单条路径同口径，
		// 且**不得静默跳过**（跳过会让调用方以为整批已写入，见 core.ErrInvalidKey）。
		switch op.Kind {
		case core.BatchZSet, core.BatchZDel, core.BatchZIncr:
			if err := core.CheckKeys(op.Key, op.Member); err != nil {
				return err
			}
		default:
			if err := core.CheckKey(op.Key); err != nil {
				return err
			}
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
