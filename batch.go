package kvdb

import (
	"context"

	"kvdb/core"
)

// Batch 是跨基座共享的批量写收集器：在 DB.Batch 的回调内收集操作，
// 回调返回 nil 时整批提交、返回错误则丢弃（不提交）。
//
// 收集期的参数校验错误（如非正 TTL）会被记录，并在提交前返回；
// 批内不含 Incr/QPop 这类依赖当前状态的操作，需单独调用。
type Batch struct {
	ops []core.BatchOp
	err error
}

// NewBatch 创建一个空批。
func NewBatch() *Batch { return &Batch{} }

// Len 返回已收集的操作数。
func (b *Batch) Len() int { return len(b.ops) }

// Ops 返回已收集的操作（只读用途；直接调用基座 ApplyBatch 时可传入）。
func (b *Batch) Ops() []core.BatchOp { return b.ops }

// Err 返回收集期记录的首个错误。
func (b *Batch) Err() error { return b.err }

// Set 收集一次写入（不改变既有 TTL）。
func (b *Batch) Set(key string, value []byte) {
	b.ops = append(b.ops, core.BatchOp{Kind: core.BatchSet, Key: key, Value: value})
}

// SetEx 收集一次带 TTL 的写入；ttl<=0 记为错误，提交前返回。
func (b *Batch) SetEx(key string, value []byte, ttl int64) {
	if ttl <= 0 {
		b.fail(core.ErrInvalidTTL)
		return
	}
	b.ops = append(b.ops, core.BatchOp{Kind: core.BatchSetEx, Key: key, Value: value, TTL: ttl})
}

// Del 收集一次删除。
func (b *Batch) Del(key string) {
	b.ops = append(b.ops, core.BatchOp{Kind: core.BatchDel, Key: key})
}

// Expire 收集一次 TTL 设置；ttl<=0 记为错误，提交前返回。
func (b *Batch) Expire(key string, ttl int64) {
	if ttl <= 0 {
		b.fail(core.ErrInvalidTTL)
		return
	}
	b.ops = append(b.ops, core.BatchOp{Kind: core.BatchExpire, Key: key, TTL: ttl})
}

// QPush 收集一次队尾追加。
func (b *Batch) QPush(name string, value []byte) {
	b.ops = append(b.ops, core.BatchOp{Kind: core.BatchQPush, Key: name, Value: value})
}

// QPushFront 收集一次队头插入。
func (b *Batch) QPushFront(name string, value []byte) {
	b.ops = append(b.ops, core.BatchOp{Kind: core.BatchQPushFront, Key: name, Value: value})
}

// ZSet 收集一次分数写入。
func (b *Batch) ZSet(name, member string, score int64) {
	b.ops = append(b.ops, core.BatchOp{Kind: core.BatchZSet, Key: name, Member: member, Score: score})
}

// ZDel 收集一次成员删除。
func (b *Batch) ZDel(name, member string) {
	b.ops = append(b.ops, core.BatchOp{Kind: core.BatchZDel, Key: name, Member: member})
}

// ZIncr 收集一次分数累加（分数为数值运算，无校验需求）。
func (b *Batch) ZIncr(name, member string, delta int64) {
	b.ops = append(b.ops, core.BatchOp{Kind: core.BatchZIncr, Key: name, Member: member, Delta: delta})
}

func (b *Batch) fail(err error) {
	if b.err == nil {
		b.err = err
	}
}

// Batch 在回调内收集写操作，并在回调返回 nil 时整批提交。
// 未实现 BatchProvider 的基座返回 ErrUnsupported；回调返回错误或收集期校验
// 失败时整批不提交（基座状态不变）。
//
//	err := db.Batch(ctx, func(b *kvdb.Batch) error {
//	    b.Set("k", value)
//	    b.QPush("jobs", payload)
//	    b.ZIncr("rank", "alice", 1)
//	    return nil
//	})
func (db *DB) Batch(ctx context.Context, fn func(b *Batch) error) error {
	if db.batch == nil {
		return ErrUnsupported
	}
	b := NewBatch()
	if err := fn(b); err != nil {
		return err
	}
	if b.err != nil {
		return b.err
	}
	if len(b.ops) == 0 {
		return nil
	}
	return db.batch.ApplyBatch(ctx, b.ops)
}

// BatchProvider 返回底层基座的批量能力（无则返回 nil）。
func (db *DB) BatchProvider() BatchProvider {
	if db.batch == nil {
		return nil
	}
	return db.batch
}
