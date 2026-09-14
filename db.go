package kvdb

import (
	"context"

	"kvdb/core"
)

// DB 是 kv + queue + zset 的统一适配器：内部持有一个 KvProvider，并在构造时
// 通过类型断言捕获可选的 Queue/ZSet 能力。对未实现的能力返回 ErrUnsupported，
// 调用方可用 Capabilities 提前探测，或直接类型断言底层 KvProvider 使用能力接口。
type DB struct {
	p core.KvProvider
	q core.QueueProvider
	z core.ZSetProvider
}

// Wrap 将任意 KvProvider 包装为适配器 DB；可选能力在构造时一次性探测。
func Wrap(p core.KvProvider) *DB {
	db := &DB{p: p}
	db.q, _ = p.(core.QueueProvider)
	db.z, _ = p.(core.ZSetProvider)
	return db
}

// Capabilities 报告底层基座是否实现了 queue / zset 可选能力。
func (db *DB) Capabilities() (hasQueue, hasZSet bool) {
	return db.q != nil, db.z != nil
}

// KvProvider 返回底层基座，便于使用能力接口做类型断言。
func (db *DB) KvProvider() core.KvProvider { return db.p }

// ---- KV ----

func (db *DB) Set(ctx context.Context, key string, value []byte) error {
	return db.p.Set(ctx, key, value)
}

func (db *DB) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	return db.p.SetEx(ctx, key, value, ttl)
}

func (db *DB) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return db.p.Get(ctx, key)
}

func (db *DB) Del(ctx context.Context, key string) error {
	return db.p.Del(ctx, key)
}

func (db *DB) Exists(ctx context.Context, key string) (bool, error) {
	return db.p.Exists(ctx, key)
}

func (db *DB) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	return db.p.Incr(ctx, key, delta)
}

func (db *DB) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	return db.p.MGet(ctx, keys...)
}

func (db *DB) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	return db.p.Scan(ctx, start, end, limit)
}

func (db *DB) Expire(ctx context.Context, key string, ttl int64) error {
	return db.p.Expire(ctx, key, ttl)
}

func (db *DB) TTL(ctx context.Context, key string) (int64, bool, error) {
	return db.p.TTL(ctx, key)
}

// ---- Queue（可选能力） ----

func (db *DB) QPush(ctx context.Context, name string, value []byte) error {
	if db.q == nil {
		return ErrUnsupported
	}
	return db.q.QPush(ctx, name, value)
}

func (db *DB) QPushFront(ctx context.Context, name string, value []byte) error {
	if db.q == nil {
		return ErrUnsupported
	}
	return db.q.QPushFront(ctx, name, value)
}

func (db *DB) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	if db.q == nil {
		return nil, false, ErrUnsupported
	}
	return db.q.QPop(ctx, name)
}

func (db *DB) QPopBack(ctx context.Context, name string) ([]byte, bool, error) {
	if db.q == nil {
		return nil, false, ErrUnsupported
	}
	return db.q.QPopBack(ctx, name)
}

func (db *DB) QSize(ctx context.Context, name string) (int64, error) {
	if db.q == nil {
		return 0, ErrUnsupported
	}
	return db.q.QSize(ctx, name)
}

func (db *DB) QFront(ctx context.Context, name string) ([]byte, bool, error) {
	if db.q == nil {
		return nil, false, ErrUnsupported
	}
	return db.q.QFront(ctx, name)
}

func (db *DB) QBack(ctx context.Context, name string) ([]byte, bool, error) {
	if db.q == nil {
		return nil, false, ErrUnsupported
	}
	return db.q.QBack(ctx, name)
}

// ---- ZSet（可选能力） ----

func (db *DB) ZSet(ctx context.Context, name, key string, score int64) error {
	if db.z == nil {
		return ErrUnsupported
	}
	return db.z.ZSet(ctx, name, key, score)
}

func (db *DB) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	if db.z == nil {
		return 0, false, ErrUnsupported
	}
	return db.z.ZGet(ctx, name, key)
}

func (db *DB) ZDel(ctx context.Context, name, key string) error {
	if db.z == nil {
		return ErrUnsupported
	}
	return db.z.ZDel(ctx, name, key)
}

func (db *DB) ZSize(ctx context.Context, name string) (int64, error) {
	if db.z == nil {
		return 0, ErrUnsupported
	}
	return db.z.ZSize(ctx, name)
}

func (db *DB) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	if db.z == nil {
		return 0, false, ErrUnsupported
	}
	return db.z.ZRank(ctx, name, key)
}

func (db *DB) ZRange(ctx context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	if db.z == nil {
		return nil, ErrUnsupported
	}
	return db.z.ZRange(ctx, name, start, stop)
}

func (db *DB) ZIncr(ctx context.Context, name, key string, delta int64) (int64, error) {
	if db.z == nil {
		return 0, ErrUnsupported
	}
	return db.z.ZIncr(ctx, name, key, delta)
}

func (db *DB) Close() error {
	// Close 不在 KvProvider 契约内：基座实现 Closer 才由适配器关闭，
	// 未实现时为空操作（资源由业务方管理）。
	if c, ok := db.p.(core.Closer); ok {
		return c.Close()
	}
	return nil
}
