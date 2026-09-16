package kvdb

import (
	"context"

	"github.com/RelicOfTesla/kvdb/core"
)

// Batcher 是调用方视角的批量写接口（回调式收集 + 一次提交）。
// 与基座侧的 core.BatchProvider（ApplyBatch 原始操作）互补：前者给业务用，
// 后者给基座实现用，二者职责不同、不重复。
type Batcher interface {
	Batch(ctx context.Context, fn func(b *Batch) error) error
}

// DB 是 kv + queue + zset + batch 的统一适配器接口，由 kvdb.Open / kvdb.Wrap 返回。
// 返回接口而非具体类型，便于业务代码依赖并在测试中替换为 mock。
//
// 它直接组合 core 已有的能力接口（不重复定义方法集）：
//   - 需要多窄的能力就依赖多窄的接口——只读场景可自定义窄接口或用
//     KvProvider 的读方法子集；只需 KV 的函数就声明 kvdb.KvProvider 参数；
//   - DB 自身满足全部能力，可直接传给任何依赖其子接口的函数。
type DB interface {
	KvProvider
	QueueProvider
	ZSetProvider
	Batcher
	Closer
	// Capabilities 报告底层基座实际具备的能力（见 core.Caps）。
	Capabilities() core.Caps
}

// adapter 是 DB 接口的默认实现：内部持有一个 KvProvider，并在构造时通过类型
// 断言捕获可选的 Queue/ZSet/Batch 能力，未实现的能力返回 ErrUnsupported。
type adapter struct {
	p     core.KvProvider
	q     core.QueueProvider
	z     core.ZSetProvider
	batch core.BatchProvider
}

// Wrap 将任意 KvProvider 包装为 DB 接口；可选能力在构造时一次性探测。
func Wrap(p core.KvProvider) DB {
	a := &adapter{p: p}
	a.q, _ = p.(core.QueueProvider)
	a.z, _ = p.(core.ZSetProvider)
	a.batch, _ = p.(core.BatchProvider)
	return a
}

// Unwrap 返回适配器背后的基座（取不到时返回 nil），供需要直接使用能力接口
// （core.QueueProvider / core.ZSetProvider / core.BatchProvider）的场景使用。
func Unwrap(d DB) core.KvProvider {
	if a, ok := d.(*adapter); ok {
		return a.p
	}
	return nil
}

// Capabilities 报告底层基座实际具备的能力。
func (a *adapter) Capabilities() core.Caps {
	c := core.Caps{Queue: a.q != nil, ZSet: a.z != nil, Batch: a.batch != nil}
	if _, ok := a.p.(core.BatchComposedProvider); ok {
		c.BatchComposed = true
	}
	if w, ok := a.p.(core.IncrWrapsProvider); ok {
		c.IncrWraps = w.IncrWraps()
	}
	return c
}

// KvProvider 返回底层基座，便于使用能力接口做类型断言。
func (a *adapter) KvProvider() core.KvProvider { return a.p }

// ---- KV ----

func (a *adapter) Set(ctx context.Context, key string, value []byte) error {
	return a.p.Set(ctx, key, value)
}

func (a *adapter) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	return a.p.SetEx(ctx, key, value, ttl)
}

func (a *adapter) SetExAt(ctx context.Context, key string, value []byte, at int64) error {
	return a.p.SetExAt(ctx, key, value, at)
}

func (a *adapter) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return a.p.Get(ctx, key)
}

func (a *adapter) Del(ctx context.Context, key string) error {
	return a.p.Del(ctx, key)
}

func (a *adapter) Exists(ctx context.Context, key string) (bool, error) {
	return a.p.Exists(ctx, key)
}

func (a *adapter) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	return a.p.Incr(ctx, key, delta)
}

func (a *adapter) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	return a.p.MGet(ctx, keys...)
}

func (a *adapter) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	return a.p.Scan(ctx, start, end, limit)
}

func (a *adapter) Expire(ctx context.Context, key string, ttl int64) error {
	return a.p.Expire(ctx, key, ttl)
}

func (a *adapter) ExpireAt(ctx context.Context, key string, at int64) error {
	return a.p.ExpireAt(ctx, key, at)
}

func (a *adapter) TTL(ctx context.Context, key string) (int64, bool, error) {
	return a.p.TTL(ctx, key)
}

// ---- Queue（可选能力） ----

func (a *adapter) QPush(ctx context.Context, name string, value []byte) error {
	if a.q == nil {
		return ErrUnsupported
	}
	return a.q.QPush(ctx, name, value)
}

func (a *adapter) QPushFront(ctx context.Context, name string, value []byte) error {
	if a.q == nil {
		return ErrUnsupported
	}
	return a.q.QPushFront(ctx, name, value)
}

func (a *adapter) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	if a.q == nil {
		return nil, false, ErrUnsupported
	}
	return a.q.QPop(ctx, name)
}

func (a *adapter) QPopBack(ctx context.Context, name string) ([]byte, bool, error) {
	if a.q == nil {
		return nil, false, ErrUnsupported
	}
	return a.q.QPopBack(ctx, name)
}

func (a *adapter) QSize(ctx context.Context, name string) (int64, error) {
	if a.q == nil {
		return 0, ErrUnsupported
	}
	return a.q.QSize(ctx, name)
}

func (a *adapter) QFront(ctx context.Context, name string) ([]byte, bool, error) {
	if a.q == nil {
		return nil, false, ErrUnsupported
	}
	return a.q.QFront(ctx, name)
}

func (a *adapter) QBack(ctx context.Context, name string) ([]byte, bool, error) {
	if a.q == nil {
		return nil, false, ErrUnsupported
	}
	return a.q.QBack(ctx, name)
}

// ---- ZSet（可选能力） ----

func (a *adapter) ZSet(ctx context.Context, name, key string, score int64) error {
	if a.z == nil {
		return ErrUnsupported
	}
	return a.z.ZSet(ctx, name, key, score)
}

func (a *adapter) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	if a.z == nil {
		return 0, false, ErrUnsupported
	}
	return a.z.ZGet(ctx, name, key)
}

func (a *adapter) ZDel(ctx context.Context, name, key string) error {
	if a.z == nil {
		return ErrUnsupported
	}
	return a.z.ZDel(ctx, name, key)
}

func (a *adapter) ZSize(ctx context.Context, name string) (int64, error) {
	if a.z == nil {
		return 0, ErrUnsupported
	}
	return a.z.ZSize(ctx, name)
}

func (a *adapter) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	if a.z == nil {
		return 0, false, ErrUnsupported
	}
	return a.z.ZRank(ctx, name, key)
}

func (a *adapter) ZRange(ctx context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	if a.z == nil {
		return nil, ErrUnsupported
	}
	return a.z.ZRange(ctx, name, start, stop)
}

func (a *adapter) ZIncr(ctx context.Context, name, key string, delta int64) (int64, error) {
	if a.z == nil {
		return 0, ErrUnsupported
	}
	return a.z.ZIncr(ctx, name, key, delta)
}

func (a *adapter) Close() error {
	// Close 不在 KvProvider 契约内：基座实现 Closer 才由适配器关闭，
	// 未实现时为空操作（资源由业务方管理）。
	if c, ok := a.p.(core.Closer); ok {
		return c.Close()
	}
	return nil
}
