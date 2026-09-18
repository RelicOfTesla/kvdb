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
	if err := core.CheckKey(name); err != nil {
		return err
	}
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

// QRange 只读返回 [start, stop] 区间内的元素，方向为队头 → 队尾。
// 索引语义与 ZRange 一致（0 起闭区间、负索引从末尾数、越界裁剪）。
func (p *Provider) QRange(ctx context.Context, name string, start, stop int64) ([][]byte, error) {
	_ = ctx
	if err := core.CheckKey(name); err != nil {
		return nil, err
	}
	if err := p.check(); err != nil {
		return nil, err
	}
	cs, err := p.qCountersGet(name)
	if err != nil {
		return nil, err
	}
	lo, hi, ok := normalizeRange(start, stop, int64(cs.count))
	if !ok {
		return nil, nil
	}
	r := nsRange(nsQueue, lp(name)...)
	it := p.db.NewIterator(r, nil)
	defer it.Release()
	out := make([][]byte, 0, minCap(int(hi-lo+1)))
	idx := int64(0)
	// 注意写法：First() 已定位到首个元素，必须**先读后进**。
	// 写成 `for it.First(); it.Next();` 会在读之前跳过首元素（off-by-one，
	// 表现为 QRange(0,2) 返回 b,c,d）。
	for it.First(); it.Valid(); it.Next() {
		if idx > hi {
			break
		}
		if idx >= lo {
			out = append(out, append([]byte(nil), it.Value()...))
		}
		idx++
	}
	if ierr := it.Error(); ierr != nil {
		return nil, fmt.Errorf("leveldb: qrange: %w", ierr)
	}
	return out, nil
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
	if err := core.CheckKey(name); err != nil {
		return nil, false, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return 0, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return nil, false, err
	}
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
	if err := core.CheckKeys(name, key); err != nil {
		return err
	}
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
	if err := core.CheckKeys(name, key); err != nil {
		return 0, false, err
	}
	if err := p.check(); err != nil {
		return 0, false, err
	}
	return p.zScoreGet(name, key)
}

func (p *Provider) ZDel(ctx context.Context, name, key string) error {
	_ = ctx
	if err := core.CheckKeys(name, key); err != nil {
		return err
	}
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
	if err := core.CheckKey(name); err != nil {
		return 0, err
	}
	if err := p.check(); err != nil {
		return 0, err
	}
	return p.zCountGet(name)
}

func (p *Provider) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	_ = ctx
	if err := core.CheckKeys(name, key); err != nil {
		return 0, false, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return nil, err
	}
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

// zScoreWindow 在排序侧索引 nsZScore 上按**分数闭区间** [min, max] 取窗口。
//
// 注意与 normalizeRange 的区别：那个是 ZRANGE 的**索引**区间，这里是**分数**
// 区间，两者不可互换（分数范围无法折算成下标，除非先全量扫描计数）。
//
// desc 只改遍历方向，不改变参数含义：始终 min<=max，min>max 直接判空。
// limit>0 时"按遍历方向取前 limit 个"——降序即**最高分**那一端，因此降序
// 是边走边截断（而非先取完再截），与 mem.zScoreWindow 的截断方向一致。
//
// 键为 lp(name)+be64(ordered(score))+member，键序即 (分数升序, 成员升序)。
// 升序时直接用 zEach 正序遍历；降序时 goleveldb 没有反向迭代器，
// 故由 zScoreWindowReverse 用"正向 Seek + Prev"自建反向遍历，
// 再把**每个同分数段**内部翻回升序以满足"同分不随方向翻转"。
func (p *Provider) zScoreWindow(name string, min, max int64, limit int, desc bool) ([]core.ZItem, error) {
	if min > max {
		return nil, nil
	}
	if !desc {
		out := make([]core.ZItem, 0, minCap(limit))
		err := p.zEach(name, func(s int64, m string) bool {
			if s < min {
				return true // 还没进入区间
			}
			if s > max {
				return false // 已越过区间上界（升序遍历可安全提前结束）
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
	return p.zScoreWindowReverse(name, min, max, limit)
}

// zScoreWindowReverse 是 desc=true 的实现：按分数降序输出区间内成员，
// 同一分数内的成员仍按成员升序（与 Redis 同分行为一致）。
//
// goleveldb 的迭代器**只有正向 Seek**（没有 Reverse 选项，也没有反向 Seek），
// 因此降序分两步：先正向 Seek 到区间上界定位到区间内**最大**的键，再连续
// Prev() 向前遍历；每当跨入新的（更低的）分数段，就从该分数的段尾继续
// Prev() 走完整段，把整段收集后翻转为成员升序再输出。
func (p *Provider) zScoreWindowReverse(name string, min, max int64, limit int) ([]core.ZItem, error) {
	prefix := lp(name)
	r := nsRange(nsZScore, prefix...)
	it := p.db.NewIterator(r, nil)
	defer it.Release()

	out := make([]core.ZItem, 0, minCap(limit))

	// 区间上界（开）：分数 max 的所有成员键都小于它；向前越过它即是区间内最大键。
	hiOpen := keyOf(nsZScore, prefix, be64(ordered(max)+1))

	// 定位到区间内最大的键：Seek(hiOpen) 落到第一个 >= hiOpen 的键，Prev 一步
	// 即区间内（或低于区间的）最大键。Seek 越界返回 false 时迭代器仍停在末尾，
	// 此时 Prev 依然有效，故不依赖 Seek 的返回值。
	it.Seek(hiOpen)
	ok := it.Prev()
	for ok {
		k := it.Key()
		if len(k) == 0 || k[0] != nsZScore {
			break
		}
		if !hasPrefixBytes(k[1:], prefix) {
			// 其他 zset 的键：继续向前找（不 break，因为键序在 prefix 之外仍有序）
			ok = it.Prev()
			continue
		}
		body := k[1+len(prefix):]
		if len(body) < be64Len {
			ok = it.Prev()
			continue
		}
		score := unorder(binary.BigEndian.Uint64(body[:be64Len]))
		if score < min {
			break // 已低于区间下界，且分数单调递减
		}
		// 收集本分数段的全部成员：从段尾（当前）向前走完同分键。
		run := append(out[:0:0], core.ZItem{Key: string(body[be64Len:]), Score: score})
		for {
			ok = it.Prev()
			if !ok {
				break
			}
			nk := it.Key()
			if len(nk) == 0 || nk[0] != nsZScore {
				ok = false
				break
			}
			nbody := nk[1+len(prefix):]
			if len(nbody) < be64Len {
				continue
			}
			nscore := unorder(binary.BigEndian.Uint64(nbody[:be64Len]))
			if nscore != score {
				break // 下一段（分数更低）：游标已停在该段最大键上
			}
			run = append(run, core.ZItem{Key: string(nbody[be64Len:]), Score: nscore})
		}
		// run 当前是成员降序，翻正为成员升序（同分不随方向翻转）
		for i, j := 0, len(run)-1; i < j; i, j = i+1, j-1 {
			run[i], run[j] = run[j], run[i]
		}
		out = append(out, run...)
		if !ok {
			break
		}
		if limit > 0 && len(out) >= limit {
			break // 已凑满 limit：同分段的完整成员已全部输出，直接截断
		}
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("leveldb: zrangebyscore: %w", err)
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
	if err := p.check(); err != nil {
		return nil, err
	}
	return p.zScoreWindow(name, min, max, limit, desc)
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

// BatchComposed 声明批内可见性：整批事件收进一个 LevelDB Batch，队列/zset 的
// 计数与成员分数在批内由 batchState 本地合成——后续操作读到的是本批前序效果
// （read-your-writes），同批覆盖、同队列按序入队、ZSet+ZIncr 累加都成立。
// 见 core.BatchComposedProvider 与 batchState 注释。
// 批内 op 之间经 batchState 合成可见；批与并发写之间由名称分片锁串行。
func (p *Provider) BatchComposed() bool { return true }

var _ core.BatchComposedProvider = (*Provider)(nil)

// batchState 是一次批写内的"当前状态"缓存：LevelDB 的 Batch 只是写缓冲，
// 提交前读不到未提交内容，而批内计数器（队列 seq/count、zset 成员数）依赖
// "读当前状态 → 计算 → 写回"。若每条 op 都直接读库，同批对同一队列/zset 的
// 第二条 op 会与第一条拿到相同基数，写入同一排序键互相覆盖、计数少加——
// 静默丢元素。把整个批涉及的队列/zset 状态拉进 pending，逐条 op 在缓存上
// 合成（Incr/Pop自家的下一跳都基于上一条的结果），批写一次提交时把最终
// 状态一次性写进同一个 batch，语义与"逐条应用到同一事务"等价。
//
// 缓存只覆盖三类复合结构，外加 KV 侧的"存在性 + TTL"：
//   - 队列计数器（per 队列名）：qCounters
//   - zset 成员当前分数（per (zset, member)）与存在性
//   - zset 成员数（per zset 名）
//   - KV 键的"存在且未过期"与 TTL 记录（per 用户 key）：kvState
//
// KV 一侧同样需要这层覆盖：BatchExpire 的条件语义要求"知道键是否存在"，而
// LevelDB 的 Batch 只写不读——若逐条 Expire 直接读库，同批内前序 Set 出来的键
// 会被误判为不存在（批内可见性被破坏），而无条件写 TTL 又会复活过期键。
type batchState struct {
	p   *Provider
	qc  map[string]qCounters // 队列名 -> 最新计数器（出现过才存）
	qt  map[string]bool      // 批内被修改过的队列名（末尾统一写回）
	zm  map[string]int64     // "名\x00成员" -> 最新分数
	zex map[string]bool      // "名\x00成员" -> 是否存在（批内视角）
	zc  map[string]int64     // zset 名 -> 最新成员数
	zct map[string]bool      // 批内被修改过的 zset 名
	kvs map[string]kvState   // 用户 key -> 批内最新 KV 状态
}

func (p *Provider) newBatchState() *batchState {
	return &batchState{
		p:   p,
		qc:  map[string]qCounters{},
		qt:  map[string]bool{},
		zm:  map[string]int64{},
		zex: map[string]bool{},
		zc:  map[string]int64{},
		zct: map[string]bool{},
		kvs: map[string]kvState{},
	}
}

// qGet 返回 name 的最新队列计数器：批内已修改过则用批内值，否则读库（并缓存）。
func (st *batchState) q(name string) (qCounters, error) {
	if c, ok := st.qc[name]; ok {
		return c, nil
	}
	c, err := st.p.qCountersGet(name)
	if err != nil {
		return qCounters{}, err
	}
	st.qc[name] = c
	return c, nil
}

// qFinalize 把批内修改过的队列计数器写进整批（每个涉及名只写一次，取最终值）。
func (st *batchState) qFinalize(b *gldb.Batch) {
	for name := range st.qt {
		putQCounters(b, name, st.qc[name])
	}
}

// zCacheKey 以 "名\x00成员" 作为成员级缓存键；\x00 不会出现在正常名字里，即便
// 出现也只是把两个不同字符串并到同一键上——多付一次读库，末值仍正确。
func zCacheKey(name, member string) string { return name + "\x00" + member }

// zMemberScore 返回成员的最新分数与存在性（批内值优先，未命中读库）。
func (st *batchState) zMemberScore(name, member string) (int64, bool, error) {
	ck := zCacheKey(name, member)
	if v, ok := st.zex[ck]; ok {
		// 存在性已缓存：分数用批内最新值（不存在时返回值无意义，直接置零）。
		if v {
			return st.zm[ck], true, nil
		}
		return 0, false, nil
	}
	old, existed, err := st.p.zScoreGet(name, member)
	if err != nil {
		return 0, false, err
	}
	st.zex[ck] = existed
	if existed {
		st.zm[ck] = old
	}
	return old, existed, nil
}

// zSetCount 把 name 的成员数按批内合成逻辑增减：首次触碰先读库初始化，
// 之后在该批内值上累加；批末经 zCountFinalize 统一写回终值。
func (st *batchState) zSetCount(name string, d int64) error {
	if !st.zct[name] {
		n, err := st.p.zCountGet(name)
		if err != nil {
			return err
		}
		st.zc[name] = n
		st.zct[name] = true
	}
	st.zc[name] += d
	return nil
}

// zCountFinalize 把批内修改过的 zset 成员数写进整批。
func (st *batchState) zCountFinalize(b *gldb.Batch) {
	for name := range st.zct {
		b.Put(zCountKey(name), be64(uint64(st.zc[name])))
	}
}

// kvState 是批内某个用户 key 的 KV 侧状态：是否"存在且未过期"、TTL 记录的存在性
// 与到期秒。LevelDB 的 Batch 只是写缓冲、提交前读不到，没有这层本地状态就无法在
// 批内做条件判定——"键是否存在"决定 Expire 是否生效，"是否存在未过期 TTL"决定
// Set 是否保留 TTL。这与队列/zset 的 batchState 合成是同一个理由（见 batchState
// 注释），只是 KV 侧此前缺了这一层，导致批内 Expire 只能无条件写 TTL。
type kvState struct {
	live   bool  // 按批内视角：存在且未过期
	hasTTL bool  // 是否存在 TTL 记录（无论是否已过期）
	exp    int64 // TTL 记录的绝对过期秒（hasTTL 时有效）
}

// kvStateOf 返回 key 的批内最新 KV 状态：批内已触碰过则用批内值，否则读库并缓存。
// 整批期间库不会被本批改写（写都在 Batch 缓冲里），故首次读库的缓存是安全的；
// 之后同批 op 只改缓存，从而实现批内 read-your-writes。
func (st *batchState) kvStateOf(key string, now int64) (kvState, error) {
	if s, ok := st.kvs[key]; ok {
		return s, nil
	}
	exp, has, err := st.p.ttlValueOf(key)
	if err != nil {
		return kvState{}, err
	}
	exists, err := st.p.db.Has(kvKey(key), nil)
	if err != nil {
		return kvState{}, fmt.Errorf("leveldb: batch: %w", err)
	}
	s := kvState{hasTTL: has, exp: exp, live: exists && !(has && exp <= now)}
	st.kvs[key] = s
	return s, nil
}

// kvSet 合成一次批内 Set：值覆盖；既有**未过期** TTL 保留，已过期的 TTL 记录
// 按不存在处理并从批内删除（与单条 Set 同口径：写成功必须读得到）。
func (st *batchState) kvSet(b *gldb.Batch, key string, value []byte, now int64) error {
	s, err := st.kvStateOf(key, now)
	if err != nil {
		return err
	}
	b.Put(kvKey(key), value)
	if s.hasTTL && s.exp <= now {
		b.Delete(ttlKey(key)) // 过期 TTL 记录不继承
		s.hasTTL, s.exp = false, 0
	}
	s.live = true
	st.kvs[key] = s
	return nil
}

// kvSetEx 合成一次批内 SetEx：值覆盖且 TTL 覆盖（含清掉旧记录的语义）。
func (st *batchState) kvSetEx(b *gldb.Batch, key string, value []byte, ttl, now int64) {
	exp := core.AddTTL(now, ttl)
	b.Put(kvKey(key), value)
	b.Put(ttlKey(key), be64(uint64(exp)))
	st.kvs[key] = kvState{live: true, hasTTL: true, exp: exp}
}

// kvDel 合成一次批内 Del：值与 TTL 记录一并删除（批内此后按"不存在"处理）。
func (st *batchState) kvDel(b *gldb.Batch, key string) {
	b.Delete(kvKey(key))
	b.Delete(ttlKey(key))
	st.kvs[key] = kvState{}
}

// kvExpire 合成一次批内 Expire：仅当 key（按批内视角）存在且未过期时才写 TTL。
// 不存在的键保持 no-op——不得创建 TTL 记录，更不得复活已过期的键（契约：
// core.KvProvider.Expire 对不存在的 key 不视为错误，且"已过期 = 不存在"）。
func (st *batchState) kvExpire(b *gldb.Batch, key string, ttl, now int64) error {
	s, err := st.kvStateOf(key, now)
	if err != nil {
		return err
	}
	if !s.live {
		return nil
	}
	exp := core.AddTTL(now, ttl)
	b.Put(ttlKey(key), be64(uint64(exp)))
	s.hasTTL, s.exp = true, exp
	st.kvs[key] = s
	return nil
}

// ApplyBatch 把整批操作收进**一个** LevelDB Batch 提交：Write 原子，因此
// 要么全部生效、要么全部不生效。now 在批内采样一次，避免批内 TTL 语义漂移。
//
// 批内可见性（BatchComposed=true 的一半）：队列/zset 计数与成员分数经
// batchState 在批内本地合成，后续 op 读到本批前序效果；批末把最终计数
// 一次性写进同一个 batch。队列/zset 涉及名仍按分片锁串行（与单条写路径
// 共用同一把锁，防并发批/单条互踩计数）；批内 op 之间不额外加锁。
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
		// 空 key/队列名/zset 名（以及 zset 成员）整批拒绝：与单条路径同口径，
		// 且**不得静默跳过**——跳过会让调用方以为整批已写入（见 core.ErrInvalidKey）。
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
	now := core.NowUnix()
	var b gldb.Batch
	// 队列/zset 计数与单条写路径共用同一把名称分片锁：
	// 批内合成解决的是"批内 op 之间读不到前序"，这把锁解决的是
	// "批与批 / 批与单条之间的并发覆盖"，二者互补、缺一不可。
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

	st := p.newBatchState()
	for _, op := range ops {
		var err error
		switch op.Kind {
		case core.BatchSet:
			// 经 batchState 合成：批内前序 SetEx/Expire/Del 的效果对本条可见
			// （否则"SetEx 后再 Set"会因读到旧的已过期 TTL 而误删新 TTL）。
			err = st.kvSet(&b, op.Key, op.Value, now)
		case core.BatchSetEx:
			st.kvSetEx(&b, op.Key, op.Value, op.TTL, now)
		case core.BatchDel:
			st.kvDel(&b, op.Key)
		case core.BatchExpire:
			// 条件式：键不存在/已过期则 no-op，不写 TTL、不复活过期键。
			err = st.kvExpire(&b, op.Key, op.TTL, now)
		case core.BatchQPush:
			err = st.batchQPush(&b, op.Key, op.Value, false)
		case core.BatchQPushFront:
			err = st.batchQPush(&b, op.Key, op.Value, true)
		case core.BatchZSet:
			err = st.batchZSet(&b, op.Key, op.Member, op.Score)
		case core.BatchZDel:
			err = st.batchZDel(&b, op.Key, op.Member)
		case core.BatchZIncr:
			err = st.batchZIncr(&b, op.Key, op.Member, op.Delta)
		}
		if err != nil {
			return err
		}
	}
	// 批内涉及到的计数器在批末统一写终值：同一队列被批内反复入队时，
	// 元素记录（各自不同 seq）在循环内逐条 Put，计数器只取最终值一份。
	st.qFinalize(&b)
	st.zCountFinalize(&b)
	return p.write(&b, "batch")
}

// batchQPush 在批内合成一次入队：seq/count 全部经 batchState 取最新值，
// 同一批对同一队列的第二条 op 绝不会撞上第一条的序号。
func (st *batchState) batchQPush(b *gldb.Batch, name string, value []byte, front bool) error {
	c, err := st.q(name)
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
	st.qc[name] = c
	st.qt[name] = true
	return nil
}

// batchZSet 在批内合成一次分数写入：存在性与旧分数经 batchState 取最新值，
// ZSet+ZIncr 同批可正确累加、Set 后 Del 再 Set 不漏计数。
func (st *batchState) batchZSet(b *gldb.Batch, name, member string, score int64) error {
	old, existed, err := st.zMemberScore(name, member)
	if err != nil {
		return err
	}
	if !existed {
		if err := st.zSetCount(name, 1); err != nil {
			return err
		}
	} else if old != score {
		b.Delete(zScoreKey(name, old, member)) // 分数变化：清掉旧排序键，避免残留
	}
	ck := zCacheKey(name, member)
	st.zex[ck] = true
	st.zm[ck] = score
	b.Put(zScoreKey(name, score, member), nil)
	b.Put(zMemberKey(name, member), be64(ordered(score)))
	return nil
}

func (st *batchState) batchZDel(b *gldb.Batch, name, member string) error {
	score, existed, err := st.zMemberScore(name, member)
	if err != nil || !existed {
		return err
	}
	if err := st.zSetCount(name, -1); err != nil {
		return err
	}
	ck := zCacheKey(name, member)
	st.zex[ck] = false
	st.zm[ck] = 0
	b.Delete(zScoreKey(name, score, member))
	b.Delete(zMemberKey(name, member))
	return nil
}

func (st *batchState) batchZIncr(b *gldb.Batch, name, member string, delta int64) error {
	old, existed, err := st.zMemberScore(name, member)
	if err != nil {
		return err
	}
	next := old + delta
	if !existed {
		if err := st.zSetCount(name, 1); err != nil {
			return err
		}
	} else if old != next {
		b.Delete(zScoreKey(name, old, member))
	}
	ck := zCacheKey(name, member)
	st.zex[ck] = true
	st.zm[ck] = next
	b.Put(zScoreKey(name, next, member), nil)
	b.Put(zMemberKey(name, member), be64(ordered(next)))
	return nil
}

// lockKeys 为批内涉及的名称取分片锁，返回一次性解锁函数。
//
// 两个要点：
//   - 名称不同 ≠ 分片不同：两个名字可能哈希到同一分片（例如 "q:q19" 与 "q:q20"
//     都落在分片 19，"q:q4" 与 "z:z0" 都落在分片 23）。sync.Mutex 不可重入，
//     对同一把锁二次 Lock 会**自死锁**，故按 mutex 指针去重、同一把锁只加一次；
//   - 加锁次序必须全局一致：仅按**名字**字典序加锁并不构成锁对象的全序（哈希把
//     名字序打乱），两个并发批各持一把、再互等对方那把就形成 ABBA 次序环。
//     因此这里按**分片下标升序**加锁——下标即锁对象的全序，环在结构上不可能
//     出现（名字序只用于收集，不决定加锁次序）。
//
// 参照 poc/kvstore 的 lockBatchNames（按名字序 + 指针去重，解决自死锁）；
// 这里在同样去重的前提下把次序提升为分片全序，把次序环也一并消除。
func (p *Provider) lockKeys(keys []string) func() {
	shards := make([]int, 0, len(keys))
	for _, k := range keys {
		shards = append(shards, int(keyShard(k)))
	}
	sort.Ints(shards)
	held := make([]*sync.Mutex, 0, len(shards))
	seen := make(map[*sync.Mutex]bool, len(shards))
	for _, s := range shards {
		m := &p.keyMu[s]
		if seen[m] {
			continue // 两个不同名字落到同一分片：同一把锁只加一次
		}
		seen[m] = true
		m.Lock()
		held = append(held, m)
	}
	return func() {
		for i := len(held) - 1; i >= 0; i-- {
			held[i].Unlock()
		}
	}
}

// sortedKeys 把批内涉及的名称按键序收集成切片（去重由调用方的 map 完成）。
// 加锁次序由 lockKeys 按**分片下标**决定，这里只保证输入确定性、便于调试。
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
