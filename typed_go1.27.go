//go:build go1.27

// 文件名中的 go1.27 只是可读性约定（Go 仅对 *_GOOS / *_GOARCH / *_GOOS_GOARCH
// 文件名赋予隐式约束，不含版本号后缀），真正的门禁是上面的 //go:build 行。
package kvdb

import (
	"context"
	"fmt"
)

// 本文件是"类型化薄壳"层：把「取字节 + 解码」与「编码 + 写字节」合并为泛型方法。
//
// 需要 Go 1.27.1+：方法级类型参数自 1.27 起支持，1.27.0 存在泛型方法相关的
// 编译器缺陷（golang/go#81195，1.27.1 修复）。
//
// 门禁说明：
//   - 本文件带 //go:build go1.27：更低版本的工具链不编译它，kvdb.TypedStore /
//     TypedBatch / Typed 不存在，其余 API 不受影响；
//   - 该行同时把本文件的 language version 抬到 go1.27（cmd/go 依文件内的 go1.N
//     约束传 -lang），因此模块与消费方的 go 指令都无需抬高；
//   - 构建标签只有系列级（没有 go1.27.0 / go1.27.1 这类补丁级标签），无法表达
//     "1.27.1+"，只能按 1.27 系列放行。
//
// TypedStore 与 StoreProvider 一一对照（泛型壳只依赖这一个接口，不要求 Batch/Close）：
//
//	tdb := kvdb.Typed(store)                  // store 需满足 kvdb.StoreProvider
//	u, err := tdb.Get[User](ctx, "user:1")    // = kvdb.D[User](store.Get(ctx, "user:1"))
//	err = tdb.Set(ctx, "user:1", u)           // = store.Set(ctx, "user:1", kvdb.Enc(u))
//	n, ok, err := tdb.Get[int64](ctx, "visits")   // 标量走文本编码，与 Incr 互操作
//	job, ok, err := tdb.QPop[string](ctx, "jobs")
//
// 编码/解码沿用 Enc/Dec/D 的规则与可替换的 Marshal/Unmarshal（见 bytes.go）：
// 写方向一律 Enc[T]，读方向一律 Dec[T]/D[T]，因此 `T = []byte` 时行为与直接调用
// 基座方法完全一致（Enc 对 []byte 原样透传，见 bytes.go），不引入额外拷贝语义。
//
// 读方法的签名与 D 一致（返回 (T, bool, error)），不把「缺失」折成错误。
//
// 注意：带类型参数的方法（Get/MGet/Set/SetEx/QPop/...）会遮蔽内嵌接口的同名方法，
// 因此 TypedStore 不满足 StoreProvider（签名不同）。这是刻意取舍：泛型壳给业务用，
// 原始接口由其内嵌字段 StoreProvider 直取。
type TypedStore struct {
	StoreProvider
}

// Typed 把具备三种能力的存储包成类型化薄壳（仅在 Go 1.27+ 构建中可用）。
//
// 参数是 StoreProvider 而非 FullProvider：后者要求 BatchProvider（原始 ApplyBatch），
// 而 kvdb.Open 返回的 DB 并不实现它（DB 对外暴露的是回调式的 Batcher），
// 用 FullProvider 会让最常见的 kvdb.Typed(db) 无法编译。
func Typed(p StoreProvider) TypedStore { return TypedStore{StoreProvider: p} }

// ---- KV：写 ----

// Set 编码 value 并写入 key（不改变既有 TTL）。
//
// 类型参数在调用点由实参推出（tdb.Set(ctx, "k", u) 即 T=User），也可显式指定。
func (t TypedStore) Set[T any](ctx context.Context, key string, value T) error {
	return t.StoreProvider.Set(ctx, key, Enc(value))
}

// SetEx 编码 value 并写入，同时设置 ttl 秒存活（覆盖既有 TTL）。
func (t TypedStore) SetEx[T any](ctx context.Context, key string, value T, ttl int64) error {
	return t.StoreProvider.SetEx(ctx, key, Enc(value), ttl)
}

// ---- KV：读 ----

// Get 读取 key 并解码为 T，**签名与 D 一致**：ok=false 表示 key 不存在（含已过期），
// 此时 err=nil；只有 IO/解析失败才有 err。
//
// 这样做是为了不与底层契约打架：取出「缺失」与「出错」的取舍交给调用方，
// 而不是让这一层把缺失折成 ErrNotFound（想那样做只需自己判 !ok）。
func (t TypedStore) Get[T any](ctx context.Context, key string) (T, bool, error) {
	return D[T](t.StoreProvider.Get(ctx, key))
}

// MGet 批量读取并解码；不存在的 key 不出现在结果中。
func (t TypedStore) MGet[T any](ctx context.Context, keys ...string) (map[string]T, error) {
	raw, err := t.StoreProvider.MGet(ctx, keys...)
	if err != nil {
		return nil, err
	}
	out := make(map[string]T, len(raw))
	for k, b := range raw {
		v, err := Dec[T](b)
		if err != nil {
			return nil, fmt.Errorf("kvdb: MGet %q: %w", k, err)
		}
		out[k] = v
	}
	return out, nil
}

// ---- Queue：写 ----

// QPush 编码 value 并追加到队尾。
func (t TypedStore) QPush[T any](ctx context.Context, name string, value T) error {
	return t.StoreProvider.QPush(ctx, name, Enc(value))
}

// QPushFront 编码 value 并插入到队头。
func (t TypedStore) QPushFront[T any](ctx context.Context, name string, value T) error {
	return t.StoreProvider.QPushFront(ctx, name, Enc(value))
}

// ---- Queue：读 ----

// QPop 取出并解码队头；ok=false 表示队列为空（此时 err=nil）。签名与 D 一致。
func (t TypedStore) QPop[T any](ctx context.Context, name string) (T, bool, error) {
	return D[T](t.StoreProvider.QPop(ctx, name))
}

// QPopBack 取出并解码队尾；ok=false 表示队列为空。签名与 D 一致。
func (t TypedStore) QPopBack[T any](ctx context.Context, name string) (T, bool, error) {
	return D[T](t.StoreProvider.QPopBack(ctx, name))
}

// QFront 只读查看并解码队头；ok=false 表示队列为空。签名与 D 一致。
func (t TypedStore) QFront[T any](ctx context.Context, name string) (T, bool, error) {
	return D[T](t.StoreProvider.QFront(ctx, name))
}

// QBack 只读查看并解码队尾；ok=false 表示队列为空。签名与 D 一致。
func (t TypedStore) QBack[T any](ctx context.Context, name string) (T, bool, error) {
	return D[T](t.StoreProvider.QBack(ctx, name))
}

// ---- Batch ----

// TypedBatch 是 Batch 的类型化收集壳：收集时即完成编码（Enc[T]），因此批内容与
// 直接调用基座写入的字节完全一致。**同一批可混装多种类型**——类型参数在方法上，
// 每次调用各自推导：
//
//	b.Set("cnt", 42)          // T = int
//	b.Set("user:1", u)        // T = User
//	b.QPush("jobs", "job-1")  // T = string
//
// Del/Expire/ZSet/ZDel/ZIncr/Len/Ops/Err 经内嵌的 *Batch 直接可用（它们不涉及 value）。
//
// 为什么不像 TypedStore 那样嵌入 StoreProvider：那个接口要求持有 provider，
// 而 Batch 是**零依赖收集器**（只往 ops 追加 core.BatchOp）。实测两个后果：
//
//  1. 涉及 value 的四个方法签名与语义都不同（这里 Set(key, v) 是"收集"，
//     TypedStore 的 Set(ctx, key, v) 是"立即写 provider"），因此必然被本类型的方法
//     遮蔽，嵌入进来的那四个方法一个都用不上，纯属形式；
//  2. 未被遮蔽的方法（如 Del）会解析到嵌入里那个 nil 的 provider 字段，
//     调用即 panic（实测 nil pointer dereference），而不是收集到批里。
//
// 所以二者只在**编码规则**上统一（都走 Enc[T]/Dec[T]/D[T]），不共享嵌入结构。
type TypedBatch struct {
	*Batch
}

// TypedBatchOf 把已存在的 Batch 包成类型化壳（复用同一个收集器）。
func TypedBatchOf(b *Batch) TypedBatch { return TypedBatch{Batch: b} }

// Set 收集一次写入（value 按 T 编码）。
func (b TypedBatch) Set[T any](key string, value T) {
	b.Batch.Set(key, Enc(value))
}

// SetEx 收集一次带 TTL 的写入（value 按 T 编码）。
func (b TypedBatch) SetEx[T any](key string, value T, ttl int64) {
	b.Batch.SetEx(key, Enc(value), ttl)
}

// QPush 收集一次队尾入队（value 按 T 编码）。
func (b TypedBatch) QPush[T any](name string, value T) {
	b.Batch.QPush(name, Enc(value))
}

// QPushFront 收集一次队头入队（value 按 T 编码）。
func (b TypedBatch) QPushFront[T any](name string, value T) {
	b.Batch.QPushFront(name, Enc(value))
}

// BatchT 与 DB.Batch 相同，但回调里拿到的是类型化收集壳：回调返回 nil 时整批
// 提交，返回错误或收集期校验失败时整批不提交。批内可混装多种类型。
//
//	err := tdb.BatchT(ctx, func(b kvdb.TypedBatch) error {
//		b.Set("cnt", 42)          // T = int
//		b.Set("user:1", u)        // T = User
//		b.QPush("jobs", "job-1")  // T = string
//		return nil
//	})
//
// 批写不是 StoreProvider 的能力（它是独立可选的 Batcher），因此这里运行时探测：
// 基座/适配器不支持时返回 ErrUnsupported，与 DB.Batch 的契约一致。
func (t TypedStore) BatchT(ctx context.Context, fn func(b TypedBatch) error) error {
	b, ok := t.StoreProvider.(Batcher)
	if !ok {
		return fmt.Errorf("kvdb: typed batch: %w", ErrUnsupported)
	}
	return b.Batch(ctx, func(raw *Batch) error {
		return fn(TypedBatch{Batch: raw})
	})
}
