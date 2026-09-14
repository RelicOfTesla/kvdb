//go:build go1.27

// 文件名中的 go1.27 只是可读性约定（Go 仅对 *_GOOS / *_GOARCH / *_GOOS_GOARCH
// 文件名赋予隐式约束，不含版本号后缀），真正的门禁是上面的 //go:build 行。
package kvdb

import (
	"context"
	"fmt"
)

// TypedDB 是 DB 的薄壳，建议 Go 1.27.1+：本文件带 go1.27 构建约束，未满足时
// 工具链不会编译它（因此 kvdb.TypedDB / kvdb.Typed 在更低版本下不存在），而模块
// 本身仍保持较低的 go 指令，其余代码照常可用。
//
// 版本口径与门禁（实测矩阵见 README 的「TypedDB 薄壳」小节）：
//   - 方法级类型参数自 Go 1.27（含 1.27.0）起支持；1.27.0 存在泛型方法相关的编译器
//     缺陷（指针别名接收者的 malformed linker symbol，golang/go#81195，1.27.1 修复），
//     故建议 1.27.1+；
//   - 构建标签只有系列级：ReleaseTags 里没有 go1.27.0 / go1.27.1（也没有 go1.26.5）
//     这类补丁级标签，所以 //go:build go1.27.1 永远不成立，而且不报错，只会静默
//     排除文件——无法用它表达"1.27.1+"；
//   - 上面这行 //go:build go1.27 同时干两件事：① 1.27 之前的工具链不编译该文件
//     （1.25.1 / 1.26.6 下 kvdb.TypedDB / kvdb.Typed 不存在，其余 API 照常）；
//     ② 按 Go 规则把该文件的 language version 抬到 go1.27（cmd/go 依文件内的
//     go1.N 约束传 -lang），因此模块与消费方的 go 指令都无需抬高。实测去掉本行、
//     go.mod 写 go 1.25 时，1.27.1 报 "generic method requires go1.27 or later
//     (-lang was set to go1.25; check go.mod)"；写成 //go:build go1.26 则报
//     "... (file declares //go:build go1.26)"；只有 go1.27 能通过。
//
// 薄壳把「取字节 + 解码」合并为泛型方法：
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
