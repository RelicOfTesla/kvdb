package rpc

import (
	"context"
	"fmt"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/rpc/codec"
)

// batchKind 是批操作类型在**线格式**里的编号。它与 core.BatchOpKind 一一对应，
// 但刻意分开定义：线格式一旦发布就不该跟着内部枚举漂移，映射集中在
// wireKind / coreKind 两个函数里。
type batchKind uint8

const (
	kindSet        batchKind = 1
	kindSetEx      batchKind = 2
	kindDel        batchKind = 3
	kindExpire     batchKind = 4
	kindQPush      batchKind = 5
	kindQPushFront batchKind = 6
	kindZSet       batchKind = 7
	kindZDel       batchKind = 8
	kindZIncr      batchKind = 9
)

// batchOpWire 是 BatchOp 的传输形态。
type batchOpWire struct {
	Kind   batchKind
	Key    string
	Member string
	Value  []byte
	TTL    int64
	Score  int64
	Delta  int64
}

func wireKind(k core.BatchOpKind) (batchKind, error) {
	switch k {
	case core.BatchSet:
		return kindSet, nil
	case core.BatchSetEx:
		return kindSetEx, nil
	case core.BatchDel:
		return kindDel, nil
	case core.BatchExpire:
		return kindExpire, nil
	case core.BatchQPush:
		return kindQPush, nil
	case core.BatchQPushFront:
		return kindQPushFront, nil
	case core.BatchZSet:
		return kindZSet, nil
	case core.BatchZDel:
		return kindZDel, nil
	case core.BatchZIncr:
		return kindZIncr, nil
	}
	return 0, fmt.Errorf("rpc: unsupported batch op kind %d", k)
}

func coreKind(k batchKind) (core.BatchOpKind, error) {
	switch k {
	case kindSet:
		return core.BatchSet, nil
	case kindSetEx:
		return core.BatchSetEx, nil
	case kindDel:
		return core.BatchDel, nil
	case kindExpire:
		return core.BatchExpire, nil
	case kindQPush:
		return core.BatchQPush, nil
	case kindQPushFront:
		return core.BatchQPushFront, nil
	case kindZSet:
		return core.BatchZSet, nil
	case kindZDel:
		return core.BatchZDel, nil
	case kindZIncr:
		return core.BatchZIncr, nil
	}
	return 0, fmt.Errorf("rpc: unknown batch op kind %d", k)
}

// reply 是一条命令的执行结果。用一个结构体而不是 (status, payload, handled)
// 三返回值，是因为"未处理"与"处理了但出错"必须区分，而 Go 的多返回值
// 在这种嵌套分发里会迅速退化成难以阅读的位置参数。
type reply struct {
	status  codec.Status
	payload [][]byte
	handled bool
}

// ok / fail 构造"已处理"的结果；argErrReply 是参数个数错的统一回复。
func ok(payload ...[]byte) reply {
	return reply{status: codec.StatusOK, payload: payload, handled: true}
}
func empty() reply { return reply{status: codec.StatusEmpty, handled: true} }
func fail(err error) reply {
	st, pl := statusFor(err)
	return reply{status: st, payload: pl, handled: true}
}
func argErrReply(cmd string, want ...string) reply {
	return reply{
		status:  codec.StatusError,
		payload: [][]byte{[]byte(fmt.Sprintf("rpc: %s expects %d args (%v)", cmd, len(want), want))},
		handled: true,
	}
}
func unhandled() reply { return reply{} }

// dispatch 执行一条数据命令。返回协议状态与负载块。
//
// 这里**不做**任何语义加工：参数原样拆开、调用基座、按约定摆放结果。
// 任何"替基座擦屁股"的补偿逻辑都不允许出现在这一层，否则 RPC 就不再是
// 透明通道，远程与本地会分叉。
func (s *Server) dispatch(ctx context.Context, cmd string, args [][]byte) (codec.Status, [][]byte) {
	r := s.route(ctx, cmd, args)
	return r.status, r.payload
}

// route 按命令名分发。分组函数返回 unhandled() 时落到下一组，
// 最后一组是队列与 zset（它们需要按能力声明分别判断）。
func (s *Server) route(ctx context.Context, cmd string, args [][]byte) reply {
	if r := s.routeKV(ctx, cmd, args); r.handled {
		return r
	}
	if r := s.dispatchQueue(ctx, cmd, args); r.handled {
		return r
	}
	if r := s.dispatchZSet(ctx, cmd, args); r.handled {
		return r
	}
	return fail(unsupportedCmd(cmd))
}

// routeKV 处理 KV 命令与 BATCH/CAPS。
func (s *Server) routeKV(ctx context.Context, cmd string, args [][]byte) reply {
	switch cmd {
	case mSet:
		if len(args) != 2 {
			return argErrReply(mSet, "key", "value")
		}
		return fail2(s.prov.Set(ctx, string(args[0]), args[1]))

	case mSetEx:
		if len(args) != 3 {
			return argErrReply(mSetEx, "key", "value", "ttl")
		}
		ttl, err := decInt(args[2])
		if err != nil {
			return fail(err)
		}
		return fail2(s.prov.SetEx(ctx, string(args[0]), args[1], ttl))

	case mGet:
		if len(args) != 1 {
			return argErrReply(mGet, "key")
		}
		v, has, err := s.prov.Get(ctx, string(args[0]))
		if err != nil {
			return fail(err)
		}
		if !has {
			return empty()
		}
		return ok(v)

	case mDel:
		if len(args) != 1 {
			return argErrReply(mDel, "key")
		}
		return fail2(s.prov.Del(ctx, string(args[0])))

	case mExists:
		if len(args) != 1 {
			return argErrReply(mExists, "key")
		}
		has, err := s.prov.Exists(ctx, string(args[0]))
		if err != nil {
			return fail(err)
		}
		return ok(boolBlk(has))

	case mIncr:
		if len(args) != 2 {
			return argErrReply(mIncr, "key", "delta")
		}
		delta, err := decInt(args[1])
		if err != nil {
			return fail(err)
		}
		n, err := s.prov.Incr(ctx, string(args[0]), delta)
		if err != nil {
			return fail(err)
		}
		return ok(encInt(n))

	case mMGet:
		keys := make([]string, len(args))
		for i, a := range args {
			keys[i] = string(a)
		}
		m, err := s.prov.MGet(ctx, keys...)
		if err != nil {
			return fail(err)
		}
		// map 无序，这里也不承诺顺序（与本地 MGet 的契约一致）。
		out := make([][]byte, 0, len(m)*2)
		for k, v := range m {
			out = append(out, []byte(k), v)
		}
		return ok(out...)

	case mScan:
		if len(args) != 3 {
			return argErrReply(mScan, "start", "end", "limit")
		}
		limit, err := decInt(args[2])
		if err != nil {
			return fail(err)
		}
		kvs, err := s.prov.Scan(ctx, string(args[0]), string(args[1]), int(limit))
		if err != nil {
			return fail(err)
		}
		out := make([][]byte, 0, len(kvs)*2)
		for _, kv := range kvs { // Scan 保序，不能打乱
			out = append(out, []byte(kv.Key), kv.Value)
		}
		return ok(out...)

	case mExpire:
		if len(args) != 2 {
			return argErrReply(mExpire, "key", "ttl")
		}
		ttl, err := decInt(args[1])
		if err != nil {
			return fail(err)
		}
		return fail2(s.prov.Expire(ctx, string(args[0]), ttl))

	case mTTL:
		if len(args) != 1 {
			return argErrReply(mTTL, "key")
		}
		secs, has, err := s.prov.TTL(ctx, string(args[0]))
		if err != nil {
			return fail(err)
		}
		if !has {
			return empty()
		}
		return ok(encInt(secs))

	case mBatch:
		return s.dispatchBatch(ctx, args)

	case mCaps:
		return ok(encCaps(s.caps))
	}
	return unhandled()
}

// dispatchQueue 处理队列命令；handled=false 表示该命令不属于队列。
func (s *Server) dispatchQueue(ctx context.Context, cmd string, args [][]byte) reply {
	switch cmd {
	case mQPush, mQPushFr, mQPop, mQPopBk, mQSize, mQFront, mQBack:
	default:
		return unhandled()
	}
	q, has := s.prov.(core.QueueProvider)
	if !has {
		return fail(core.ErrUnsupported)
	}
	switch cmd {
	case mQPush:
		if len(args) != 2 {
			return argErrReply(mQPush, "name", "value")
		}
		return call(func() error { return q.QPush(ctx, string(args[0]), args[1]) })
	case mQPushFr:
		if len(args) != 2 {
			return argErrReply(mQPushFr, "name", "value")
		}
		return call(func() error { return q.QPushFront(ctx, string(args[0]), args[1]) })
	case mQPop:
		if len(args) != 1 {
			return argErrReply(mQPop, "name")
		}
		return bytesOrEmpty(func() ([]byte, bool, error) { return q.QPop(ctx, string(args[0])) })
	case mQPopBk:
		if len(args) != 1 {
			return argErrReply(mQPopBk, "name")
		}
		return bytesOrEmpty(func() ([]byte, bool, error) { return q.QPopBack(ctx, string(args[0])) })
	case mQFront:
		if len(args) != 1 {
			return argErrReply(mQFront, "name")
		}
		return bytesOrEmpty(func() ([]byte, bool, error) { return q.QFront(ctx, string(args[0])) })
	case mQBack:
		if len(args) != 1 {
			return argErrReply(mQBack, "name")
		}
		return bytesOrEmpty(func() ([]byte, bool, error) { return q.QBack(ctx, string(args[0])) })
	case mQSize:
		if len(args) != 1 {
			return argErrReply(mQSize, "name")
		}
		return intReply(func() (int64, error) { return q.QSize(ctx, string(args[0])) })
	}
	return unhandled()
}

// dispatchZSet 处理 sorted-set 命令。
func (s *Server) dispatchZSet(ctx context.Context, cmd string, args [][]byte) reply {
	switch cmd {
	case mZSet, mZGet, mZDel, mZSize, mZRank, mZRange, mZIncr:
	default:
		return unhandled()
	}
	z, has := s.prov.(core.ZSetProvider)
	if !has {
		return fail(core.ErrUnsupported)
	}
	switch cmd {
	case mZSet:
		if len(args) != 3 {
			return argErrReply(mZSet, "name", "member", "score")
		}
		score, err := decInt(args[2])
		if err != nil {
			return fail(err)
		}
		return call(func() error { return z.ZSet(ctx, string(args[0]), string(args[1]), score) })
	case mZGet:
		if len(args) != 2 {
			return argErrReply(mZGet, "name", "member")
		}
		return intOrEmpty(func() (int64, bool, error) { return z.ZGet(ctx, string(args[0]), string(args[1])) })
	case mZDel:
		if len(args) != 2 {
			return argErrReply(mZDel, "name", "member")
		}
		return call(func() error { return z.ZDel(ctx, string(args[0]), string(args[1])) })
	case mZSize:
		if len(args) != 1 {
			return argErrReply(mZSize, "name")
		}
		return intReply(func() (int64, error) { return z.ZSize(ctx, string(args[0])) })
	case mZRank:
		if len(args) != 2 {
			return argErrReply(mZRank, "name", "member")
		}
		return intOrEmpty(func() (int64, bool, error) { return z.ZRank(ctx, string(args[0]), string(args[1])) })
	case mZRange:
		if len(args) != 3 {
			return argErrReply(mZRange, "name", "start", "stop")
		}
		start, err := decInt(args[1])
		if err != nil {
			return fail(err)
		}
		stop, err := decInt(args[2])
		if err != nil {
			return fail(err)
		}
		items, err := z.ZRange(ctx, string(args[0]), start, stop)
		if err != nil {
			return fail(err)
		}
		out := make([][]byte, 0, len(items)*2)
		for _, it := range items { // ZRange 保序
			out = append(out, []byte(it.Key), encInt(it.Score))
		}
		return ok(out...)
	case mZIncr:
		if len(args) != 3 {
			return argErrReply(mZIncr, "name", "member", "delta")
		}
		delta, err := decInt(args[2])
		if err != nil {
			return fail(err)
		}
		n, err := z.ZIncr(ctx, string(args[0]), string(args[1]), delta)
		if err != nil {
			return fail(err)
		}
		return ok(encInt(n))
	}
	return unhandled()
}

// call / bytesOrEmpty / intReply / intOrEmpty 把"可能返回哨兵错误的基座调用"
// 收敛成统一的 reply，避免每个 case 重复四行样板。
func call(fn func() error) reply { return fail2(fn()) }

func fail2(err error) reply {
	if err != nil {
		return fail(err)
	}
	return ok()
}

func bytesOrEmpty(fn func() ([]byte, bool, error)) reply {
	v, has, err := fn()
	if err != nil {
		return fail(err)
	}
	if !has {
		return empty()
	}
	return ok(v)
}

func intReply(fn func() (int64, error)) reply {
	n, err := fn()
	if err != nil {
		return fail(err)
	}
	return ok(encInt(n))
}

func intOrEmpty(fn func() (int64, bool, error)) reply {
	n, has, err := fn()
	if err != nil {
		return fail(err)
	}
	if !has {
		return empty()
	}
	return ok(encInt(n))
}

// dispatchBatch 处理批写：请求负载每块一条自描述 op。
func (s *Server) dispatchBatch(ctx context.Context, payload [][]byte) reply {
	bp, has := s.prov.(core.BatchProvider)
	if !has {
		return fail(core.ErrUnsupported)
	}
	limit := s.cfg.MaxBatchOps
	if limit <= 0 {
		limit = DefaultMaxBatchOps
	}
	if len(payload) > limit {
		return reply{
			status:  codec.StatusError,
			payload: [][]byte{[]byte(fmt.Sprintf("rpc: batch too large: %d ops (limit %d)", len(payload), limit))},
			handled: true,
		}
	}
	ops := make([]core.BatchOp, 0, len(payload))
	for _, blk := range payload {
		w, err := decBatchOp(blk)
		if err != nil {
			return fail(err)
		}
		k, err := coreKind(w.Kind)
		if err != nil {
			return fail(err)
		}
		ops = append(ops, core.BatchOp{
			Kind: k, Key: w.Key, Member: w.Member,
			Value: w.Value, TTL: w.TTL, Score: w.Score, Delta: w.Delta,
		})
	}
	return fail2(bp.ApplyBatch(ctx, ops))
}

// unsupportedCmd 让"命令名打错"与"能力未实现"给出不同错误，
// 便于联调时一眼看出是拼错了还是底座不支持。
func unsupportedCmd(cmd string) error {
	return fmt.Errorf("rpc: %w: unknown method %q", core.ErrUnsupported, cmd)
}

func boolBlk(v bool) []byte {
	if v {
		return []byte{'1'}
	}
	return []byte{'0'}
}

func decBool(b []byte) (bool, error) {
	if len(b) == 1 && b[0] == '1' {
		return true, nil
	}
	if len(b) == 1 && b[0] == '0' {
		return false, nil
	}
	return false, fmt.Errorf("rpc: bad bool %q", truncateBytes(b))
}

// encCaps 把能力位编码成一个十进制数：跨进程传结构体不值当，
// 位图足够且不随字段增删破坏兼容（新位只在新增能力时追加）。
func encCaps(c core.Caps) []byte {
	var v int64
	if c.Queue {
		v |= 1 << 0
	}
	if c.ZSet {
		v |= 1 << 1
	}
	if c.Batch {
		v |= 1 << 2
	}
	if c.BatchComposed {
		v |= 1 << 3
	}
	return encInt(v)
}

func decCaps(b []byte) (core.Caps, error) {
	v, err := decInt(b)
	if err != nil {
		return core.Caps{}, err
	}
	return core.Caps{
		Queue:         v&(1<<0) != 0,
		ZSet:          v&(1<<1) != 0,
		Batch:         v&(1<<2) != 0,
		BatchComposed: v&(1<<3) != 0,
	}, nil
}
