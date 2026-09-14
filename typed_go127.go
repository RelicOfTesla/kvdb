//go:build go1.27

package kvdb

import (
	"context"
	"fmt"
)

// TypedDB 是 DB 的薄壳，仅在用 Go 1.27+ 构建时提供：本文件带 go1.27 构建约束，
// 旧工具链不会编译它（因此 kvdb.TypedDB / kvdb.Typed 在 Go < 1.27 下不存在），
// 而模块本身仍保持较低的 go 指令，其余代码在旧版本照常可用。
//
// 薄壳把「取字节 + 解码」合并为泛型方法（Go 1.27 起支持方法级类型参数）：
//
//	tdb := kvdb.Typed(db)
//	u, err := tdb.Get[User](ctx, "user:1")    // = kvdb.D[User](db.Get(ctx, "user:1"))
//	n, err := tdb.Get[int64](ctx, "visits")   // 标量走文本编码，与 Incr 互操作
//	job, err := tdb.QPop[string](ctx, "jobs")
//
// 编码/解码沿用 B/P/D 的规则与可替换的 Marshal/Unmarshal（见 bytes.go）。
//
// 注意：泛型方法 Get/GetOK/MGet/QPop/QPopBack/QFront/QBack 会遮蔽内嵌 DB 的同名
// 方法，因此 TypedDB **不再满足 DB 接口**。其余方法（Set/QPush/ZSet/Batch/Close…）
// 仍经内嵌字段直接透传；需要 DB 语义时用 tdb.DB 或保留原始 db。
type TypedDB struct {
	DB
}

// Typed 把 DB 包成带泛型方法的薄壳（仅在 Go 1.27+ 构建中可用）。
func Typed(d DB) TypedDB { return TypedDB{DB: d} }

// Get 读取 key 并解码为 T；key 不存在或已过期返回 ErrNotFound。
func (t TypedDB) Get[T any](ctx context.Context, key string) (T, error) {
	return D[T](t.DB.Get(ctx, key))
}

// GetOK 与 Get 相同，但保留 ok 语义：key 不存在时 ok=false 且 err=nil。
func (t TypedDB) GetOK[T any](ctx context.Context, key string) (T, bool, error) {
	val, ok, err := t.DB.Get(ctx, key)
	if err != nil || !ok {
		var zero T
		return zero, false, err
	}
	v, err := P[T](val)
	if err != nil {
		var zero T
		return zero, false, err
	}
	return v, true, nil
}

// MGet 批量读取并解码；不存在的 key 不出现在结果中。
func (t TypedDB) MGet[T any](ctx context.Context, keys ...string) (map[string]T, error) {
	raw, err := t.DB.MGet(ctx, keys...)
	if err != nil {
		return nil, err
	}
	out := make(map[string]T, len(raw))
	for k, b := range raw {
		v, err := P[T](b)
		if err != nil {
			return nil, fmt.Errorf("kvdb: MGet %q: %w", k, err)
		}
		out[k] = v
	}
	return out, nil
}

// QPop 取出并解码队头；队列为空返回 ErrNotFound。
func (t TypedDB) QPop[T any](ctx context.Context, name string) (T, error) {
	return D[T](t.DB.QPop(ctx, name))
}

// QPopBack 取出并解码队尾；队列为空返回 ErrNotFound。
func (t TypedDB) QPopBack[T any](ctx context.Context, name string) (T, error) {
	return D[T](t.DB.QPopBack(ctx, name))
}

// QFront 只读查看并解码队头；队列为空返回 ErrNotFound。
func (t TypedDB) QFront[T any](ctx context.Context, name string) (T, error) {
	return D[T](t.DB.QFront(ctx, name))
}

// QBack 只读查看并解码队尾；队列为空返回 ErrNotFound。
func (t TypedDB) QBack[T any](ctx context.Context, name string) (T, error) {
	return D[T](t.DB.QBack(ctx, name))
}
