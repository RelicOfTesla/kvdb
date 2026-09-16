package bench

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	_ "github.com/RelicOfTesla/kvdb/all" // 基准可跑任意内置基座
)

// 性能基准：通过 KVDB_BENCH_URI 指定基座，未设置时全部跳过（不影响 go test ./...）。
//
//	KVDB_BENCH_URI=mem://                    go test -bench . -benchtime 2000x .
//	KVDB_BENCH_URI=sqlite://./tmp/bench.db   go test -bench . -benchtime 2000x .
//	KVDB_BENCH_URI=redis://127.0.0.1:6379/0  go test -bench . -benchtime 2000x .
//	KVDB_BENCH_URI='ssdb://127.0.0.1:8888'   go test -bench . -benchtime 2000x .
//
// 说明：
//   - Set/Get 在 16 个 key 间轮转，值约 20 字节；
//   - IncrSequential / IncrParallelSameKey 是单键计数器（受行锁或单点串行限制）；
//     IncrParallelMultiKey 每个 goroutine 用独立 key（无重叠，可吃满组提交）；
//   - BatchSet100 每轮一次提交写入 100 条：单操作吞吐 = 1/(ns/op) × 100；
//   - 数值受硬件、容器网络与存储介质影响，请以同机相对比较为准。
func benchDB(b *testing.B) kvdb.DB {
	b.Helper()
	uri := os.Getenv("KVDB_BENCH_URI")
	if uri == "" {
		b.Skip("未设置 KVDB_BENCH_URI，跳过基准测试")
	}
	ctx := context.Background()
	db, err := kvdb.Open(ctx, uri)
	if err != nil {
		b.Fatalf("打开基座 %s: %v", uri, err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

var benchValue = []byte("value-xxxxxxxxxxxxxx") // ~20B

func BenchmarkSet(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Set(ctx, fmt.Sprintf("bench:set:%d", i%16), benchValue); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	for i := 0; i < 16; i++ {
		if err := db.Set(ctx, fmt.Sprintf("bench:get:%d", i), benchValue); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, err := db.Get(ctx, fmt.Sprintf("bench:get:%d", i%16)); err != nil || !ok {
			b.Fatalf("Get: ok=%v err=%v", ok, err)
		}
	}
}

func BenchmarkIncrSequential(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Incr(ctx, "bench:cnt", 1); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIncrParallelSameKey 全部 goroutine 打同一个 key（写热点）。
func BenchmarkIncrParallelSameKey(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := db.Incr(ctx, "bench:hot", 1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkIncrParallelMultiKey 每个 goroutine 使用独立 key（无重叠，可并行提交）。
func BenchmarkIncrParallelMultiKey(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	var seq atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := fmt.Sprintf("bench:mk:%d", seq.Add(1))
		for pb.Next() {
			if _, err := db.Incr(ctx, key, 1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

const benchBatchSize = 100

// BenchmarkBatchSet100 每轮一次提交写入 100 条（单操作吞吐 = 1/(ns/op) × 100）。
func BenchmarkBatchSet100(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	if !db.Capabilities().Batch {
		b.Skip("基座未实现 Batch 能力")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base := i * benchBatchSize
		err := db.Batch(ctx, func(w *kvdb.Batch) error {
			for j := 0; j < benchBatchSize; j++ {
				w.Set(fmt.Sprintf("bench:batch:%d", base+j), benchValue)
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQPush(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	// 未实现队列的基座在首次调用返回 ErrUnsupported，据此跳过。
	if err := db.QPush(ctx, "bench:q", benchValue); err != nil {
		if errors.Is(err, kvdb.ErrUnsupported) {
			b.Skip("基座未实现 Queue 能力")
		}
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		if err := db.QPush(ctx, "bench:q", benchValue); err != nil {
			b.Fatal(err)
		}
	}
}
