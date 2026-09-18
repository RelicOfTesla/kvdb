// Package kvdbtest 提供跨基座共享的行为用例（KV/Queue/ZSet/Batch 契约），
// 各基座包在测试中通过同一套用例验证其行为一致性，避免逐基座复制断言。
//
// 文件按**主题**划分，一文件一主题，便于定位与新增用例：
//
//	harness.go     运行入口与测试夹具（Run/RunWithOptions/Options/newDB/waitExpired）
//	kv.go          KV 基本语义
//	queue.go       队列语义
//	zset.go        有序集语义（含按分数区间读取）
//	batch.go       批量写：原子性与批内可见性
//	ttl.go         过期与 TTL：写路径、并发读、边界
//	scan.go        范围查询：闭区间、越界、字节序、limit
//	ownership.go   读写两侧的所有权（别名）语义
//	namespace.go   三类数据的命名空间隔离
//	lifecycle.go   生命周期（Close）与参数合法性（空名字、零长值）
//	incr.go        计数器：解析边界与溢出
//
// 判定原则：除 core.Caps **显式声明**的差异（Queue/ZSet/Batch/BatchComposed/
// IncrWraps）外，一律断言各基座行为一致。注释不是声明载体——凡注释里提过的
// 分歧，都必须能在 Caps 里查到，否则按 bug 处理；反之，凡 Caps 声明了的差异，
// 用例应断言"声明是否被兑现"（见 incr.go 的溢出分支）。
//
// 注意：本包的文件**不能**命名为 *_test.go。Go 工具链会校验 _test.go 内的
// func TestXxx 必须恰为 (t *testing.T)，而本套件统一用 (t, db) 形态的共享用例；
// 现有各 *_test.go（如 kvdbtest.go 原先的命名）之所以能编译，正是因为不带该后缀。
// 新增文件请沿用普通 .go 命名。
package kvdbtest

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

// 运行入口与测试夹具。

// Options 控制共享用例的可选行为。
type Options struct {
	// FastForward 推进"虚拟时钟"（如 miniredis 的 FastForward）：这类进程内替身
	// 不随真实时间过期，等待过期的用例会调用它来推进时间。真实基座保持 nil。
	FastForward func()
}

// Run 对 factory 产出的新实例依次跑 KV/Queue/ZSet 三套用例；

// Run 对 factory 产出的新实例依次跑 KV/Queue/ZSet 三套用例；
// factory 每次调用必须返回独立的新基座（测试内部会负责 Close）。
func Run(t *testing.T, factory func(t *testing.T) core.KvProvider) {
	RunWithOptions(t, Options{}, factory)
}

// VirtualClock 返回一个用注入时钟推进过期的 Options：把 core.Now 换成可控假时钟，
// FastForward 只移动假时钟，因此本地基座（mem/jsonl/bolt/sqlite）的过期用例
// 不再依赖真实秒与轮询。
//
// 注意：必须在被测基座创建前后都使用同一份时钟；测试结束由 t.Cleanup 还原，
// 避免污染同包其他用例。服务端基座（redis/ssdb/mysql/pg）的时钟在服务端，

// VirtualClock 返回一个用注入时钟推进过期的 Options：把 core.Now 换成可控假时钟，
// FastForward 只移动假时钟，因此本地基座（mem/jsonl/bolt/sqlite）的过期用例
// 不再依赖真实秒与轮询。
//
// 注意：必须在被测基座创建前后都使用同一份时钟；测试结束由 t.Cleanup 还原，
// 避免污染同包其他用例。服务端基座（redis/ssdb/mysql/pg）的时钟在服务端，
// 不要用这个选项（改用 Options{} 走真实等待）。
func VirtualClock(t *testing.T) Options {
	t.Helper()
	base := time.Now()
	// offset 用原子存：并发读用例会在 FastForward 推进时钟的同时从多个 goroutine
	// 经 core.Now 读取它，普通变量会被 -race 判为数据竞争。
	var offset atomic.Int64 // 纳秒
	real := core.Now
	core.Now = func() time.Time { return base.Add(time.Duration(offset.Load())) }
	t.Cleanup(func() { core.Now = real })
	return Options{FastForward: func() { offset.Add(int64(2 * time.Second)) }}
}

// RunWithOptions 是 Run 的带选项版本（虚拟时钟等）。
func RunWithOptions(t *testing.T, opt Options, factory func(t *testing.T) core.KvProvider) {
	t.Run("KV", func(t *testing.T) { TestKV(t, newDB(t, factory)) })
	t.Run("Queue", func(t *testing.T) { TestQueue(t, newDB(t, factory)) })
	t.Run("ZSet", func(t *testing.T) { TestZSet(t, newDB(t, factory)) })
	t.Run("Batch", func(t *testing.T) { TestBatch(t, newDB(t, factory)) })
	t.Run("BatchComposed", func(t *testing.T) { TestBatchComposed(t, newDB(t, factory)) })
	t.Run("ExpiredWrites", func(t *testing.T) { TestExpiredWrites(t, newDB(t, factory), opt) })
	t.Run("ReadOwnership", func(t *testing.T) { TestReadOwnership(t, newDB(t, factory)) })
	t.Run("NamespaceIndependence", func(t *testing.T) { TestNamespaceIndependence(t, newDB(t, factory)) })
	t.Run("ExpiredConcurrentRead", func(t *testing.T) { TestExpiredConcurrentRead(t, newDB(t, factory), opt) })
	// 以下用例按主题分布在各文件（见包文档的文件清单）。
	// 每个用例都用独立实例：CloseSemantics 会真的关闭 db，不得与其他用例共用。
	t.Run("CloseSemantics", func(t *testing.T) { TestCloseSemantics(t, newDB(t, factory)) })
	t.Run("WriteOwnership", func(t *testing.T) { TestWriteOwnership(t, newDB(t, factory)) })
	t.Run("EmptyValue", func(t *testing.T) { TestEmptyValue(t, newDB(t, factory)) })
	t.Run("MGetScanQRangeOwnership", func(t *testing.T) { TestMGetScanQRangeOwnership(t, newDB(t, factory)) })
	t.Run("NamespacePrefixKeys", func(t *testing.T) { TestNamespacePrefixKeys(t, newDB(t, factory)) })
	t.Run("EmptyName", func(t *testing.T) { TestEmptyName(t, newDB(t, factory)) })
	t.Run("ScanBoundaries", func(t *testing.T) { TestScanBoundaries(t, newDB(t, factory)) })
	t.Run("TTLBoundaries", func(t *testing.T) { TestTTLBoundaries(t, newDB(t, factory)) })
	t.Run("IncrBoundaries", func(t *testing.T) { TestIncrBoundaries(t, newDB(t, factory)) })
	t.Run("ZSetRangeBoundaries", func(t *testing.T) { TestZSetRangeBoundaries(t, newDB(t, factory)) })
	// 批写锁死锁用例放在最后：它用超时兜底把死锁表现为失败，但真死锁时会泄漏一个
	// 永久阻塞的 goroutine 并持有该实例的分片锁，放末尾可避免连累前面的子用例。
	t.Run("BatchManyNamesNoDeadlock", func(t *testing.T) { TestBatchManyNamesNoDeadlock(t, newDB(t, factory)) })
}

// waitExpired 轮询等待 key 过期（TTL 粒度为秒，不能只 sleep 固定时长：
// 恰好卡在秒边界上会偶发失败）。opt.FastForward 非 nil 时先推进虚拟时钟。
func waitExpired(t *testing.T, db kvdb.DB, opt Options, key string) {
	t.Helper()
	ctx := context.Background()
	advanced := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok, err := db.Exists(ctx, key)
		if err != nil {
			t.Fatalf("Exists(%s): %v", key, err)
		}
		if !ok {
			return
		}
		if opt.FastForward != nil && !advanced {
			opt.FastForward()
			advanced = true
			continue
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("key %s 在 5s 内未过期", key)
}

// TestExpiredWrites 覆盖"过期键 = 不存在"在**写路径**上的语义：
// 过期后 Set 必须可见（不得继承旧的过期时间）、Incr 必须从 0 起算

func newDB(t *testing.T, factory func(t *testing.T) core.KvProvider) kvdb.DB {
	t.Helper()
	p := factory(t)
	t.Cleanup(func() {
		// Close 已从 KvProvider 拆出：基座实现 Closer 才需要释放。
		if c, ok := p.(core.Closer); ok {
			c.Close()
		}
	})
	return kvdb.Wrap(p)
}
