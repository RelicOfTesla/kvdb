package bench

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb"
)

// 本文件测量**真实处理量**（吞吐），而不是用单条延迟的倒数推算。
//
// 区别很重要：各基座对写路径有不同的合并优化——SQL 的组提交、SQLite WAL 的检查点
// 搭车、LSM 的 memtable 批量落盘、Batch 把 N 次提交摊成 1 次。这些只在"固定时间窗
// 内跑满负载"时才显现；串行地测一条再取倒数，会把它们全部抹平（并得出错误的
// "某基座快是因为不持久"之类结论）。
//
// 每个基准以 ops/s 作为上报指标（ReportMetric），并用 -benchtime 控制时间窗：
//
//	KVDB_BENCH_URI=<uri> go test -bench Throughput -benchtime 5s ./bench/
//
// 注意 b.N 由 testing 框架按时长自动标定，因此这里不做任何手工换算。

// benchThroughput 在计时区间内持续执行 op，并把实际完成的**操作数/秒**作为指标上报。
// opID 为全局唯一递增序号；每个 goroutine 再叠加自己的独立基址，保证不同写者
// 一定不落在同一个 key 上（否则测到的是同键锁竞争而不是写路径吞吐）。
func benchThroughput(b *testing.B, name string, op func(ctx context.Context, db kvdb.DB, id int64) error) {
	b.Helper()
	ctx := context.Background()
	db := benchDB(b)

	var done atomic.Int64
	var wid atomic.Int64
	b.ResetTimer()
	start := time.Now()
	base := wid.Add(1) << 48 // 每 goroutine 一段互不重叠的 id 空间
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := op(ctx, db, base+done.Add(1)); err != nil {
				b.Error(err)
				return
			}
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()

	n := done.Load()
	if n == 0 {
		b.Fatalf("%s: 时间窗内没有完成任何操作", name)
	}
	reportThroughput(b, name, n, elapsed)
}

// reportThroughput 统一上报：真实吞吐、实际平均耗时（由完成量与墙钟算出，
// 不是单条延迟的倒数）。
func reportThroughput(b *testing.B, name string, n int64, elapsed time.Duration) {
	b.ReportMetric(float64(n)/elapsed.Seconds(), "ops/s")
	b.ReportMetric(float64(elapsed.Microseconds())/float64(n), "\u00b5s/op-actual")
	b.Logf("%s: 完成 %d 次 / %v => %.0f ops/s", name, n, elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds())
}

// BenchmarkThroughputSet 多写者并发 Set：反映真实的"每秒能落多少条"。
func BenchmarkThroughputSet(b *testing.B) {
	benchThroughput(b, "set", func(ctx context.Context, db kvdb.DB, id int64) error {
		return db.Set(ctx, fmt.Sprintf("thr:set:%d", id), benchValue)
	})
}

// BenchmarkThroughputIncr 多写者并发 Incr（不同 key）：与同键热点分开测，
// 因为前者吃合并提交、后者受单点串行限制，混在一起会互相掩盖。
func BenchmarkThroughputIncrMultiKey(b *testing.B) {
	benchThroughput(b, "incr-multikey", func(ctx context.Context, db kvdb.DB, id int64) error {
		_, err := db.Incr(ctx, fmt.Sprintf("thr:inrmk:%d", id), 1)
		return err
	})
}

// BenchmarkThroughputIncrSameKey 全部写者打同一个计数器：最坏情况（行锁/单点串行）。
func BenchmarkThroughputIncrSameKey(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	var done atomic.Int64
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := db.Incr(ctx, "thr:cnt:same", 1); err != nil {
				b.Error(err)
				return
			}
			done.Add(1)
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()
	n := done.Load()
	if n == 0 {
		b.Fatal("时间窗内没有完成任何操作")
	}
	reportThroughput(b, "incr-samekey", n, elapsed)
}

// BenchmarkThroughputGet 读吞吐：预先写入固定小集合（全部 goroutine 共享、只读），
// 衡量"读路径本身"的吞吐；与 Set 不同 key 空间的写吞吐分开看。
func BenchmarkThroughputGet(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	const keys = 64 // 热集大小：足够避开单点缓存效应，又能全部驻留页缓存
	for i := 0; i < keys; i++ {
		if err := db.Set(ctx, fmt.Sprintf("thrget:%d", i), benchValue); err != nil {
			b.Fatal(err)
		}
	}
	var wid atomic.Int64
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		base := (wid.Add(1) % keys) // 各 goroutine 起点错开，避免同 key 热行
		i := base
		for pb.Next() {
			if _, ok, err := db.Get(ctx, fmt.Sprintf("thrget:%d", i%keys)); err != nil || !ok {
				b.Fatalf("Get: ok=%v err=%v", ok, err)
			}
			i++
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()
	n := int64(b.N)
	reportThroughput(b, "get", n, elapsed)
}

// BenchmarkThroughputQPush 队列追加吞吐：多 goroutine 各自推自己的队列，
// 避免同队列串行化掩盖写路径能力（同队列热点另由 QSize/QPop 语义用例覆盖）。
func BenchmarkThroughputQPush(b *testing.B) {
	ctx := context.Background()
	probe := benchDB(b)
	if err := probe.QPush(ctx, "thr:probe", benchValue); err != nil {
		b.Skip("基座未实现 Queue 能力")
	}
	probe.Close()

	db := benchDB(b)
	var done atomic.Int64
	var wid atomic.Int64
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		q := fmt.Sprintf("thrpush:%d", wid.Add(1)) // 每 goroutine 一条独立队列
		for pb.Next() {
			if err := db.QPush(ctx, q, benchValue); err != nil {
				b.Error(err)
				return
			}
			done.Add(1)
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()
	n := done.Load()
	if n == 0 {
		b.Fatal("时间窗内没有完成任何 QPush")
	}
	reportThroughput(b, "qpush", n, elapsed)
}

// BenchmarkThroughputMGet 批量读吞吐：每次 MGet 一批 key，上报**条目级**吞吐。
func BenchmarkThroughputMGet(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	const chunk = 20
	ks := make([]string, chunk)
	for i := range ks {
		ks[i] = fmt.Sprintf("thrmget:%d", i)
		if err := db.Set(ctx, ks[i], benchValue); err != nil {
			b.Fatal(err)
		}
	}
	var got atomic.Int64
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := db.MGet(ctx, ks...); err != nil {
				b.Error(err)
				return
			}
			got.Add(chunk)
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()
	n := got.Load()
	if n == 0 {
		b.Fatal("时间窗内没有完成任何 MGet")
	}
	b.ReportMetric(float64(n)/elapsed.Seconds(), "items/s")
	b.Logf("mget: 读到 %d 条 / %v => %.0f items/s", n, elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds())
}

// BenchmarkThroughputMixedReadWrite 混合负载：一半 goroutine 持续读热集、
// 一半持续写各自独立的 key，同时上报 reads/s 与 writes/s。纯读/纯写基准各自
// 测出的高吞吐在混合时能不能保住，取决于该基座读路径与写路径对同步原语的争用：
// 若读者也要排队等写者（共用锁/事务/内部同步），读吞吐会随写频率显著下滑。
func BenchmarkThroughputMixedReadWrite(b *testing.B) {
	ctx := context.Background()
	db := benchDB(b)
	const hotKeys = 64
	for i := 0; i < hotKeys; i++ {
		if err := db.Set(ctx, fmt.Sprintf("mix:r:%d", i), benchValue); err != nil {
			b.Fatal(err)
		}
	}
	var reads, writes atomic.Int64
	var wid atomic.Int64
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		id := wid.Add(1)
		if id%2 == 0 { // 读角色
			i := int(id)
			for pb.Next() {
				if _, _, err := db.Get(ctx, fmt.Sprintf("mix:r:%d", i%hotKeys)); err != nil {
					b.Error(err)
					return
				}
				reads.Add(1)
				i++
			}
			return
		}
		base := id << 48 // 写角色：独占自己的 key 段
		for pb.Next() {
			if err := db.Set(ctx, fmt.Sprintf("mix:w:%d", base+writes.Add(1)), benchValue); err != nil {
				b.Error(err)
				return
			}
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()
	r, w := reads.Load(), writes.Load()
	// 标定阶段 b.N 可能小到只够一个 goroutine 执行一次，此时不可能同时出现读与写；
	// 这不代表失败，等 testing 框架放大 b.N 后重测即可（只有 GOMAXPROCS==1 才会一直如此）。
	if r == 0 || w == 0 {
		return
	}
	// 混合负载下 b.N 不代表任一角色的完成量，ns/op 无意义，置零隐藏。
	b.ReportMetric(0, "ns/op")
	b.ReportMetric(float64(r)/elapsed.Seconds(), "reads/s")
	b.ReportMetric(float64(w)/elapsed.Seconds(), "writes/s")
	b.Logf("mixed: 读 %d 次 / 写 %d 次 / %v", r, w, elapsed.Round(time.Millisecond))
}

// BenchmarkThroughputBatchedSet 以批为单位持续写入，上报**条目级**吞吐
// （每批 benchBatchSize 条），用于衡量"允许改写成批"时的真实上限。
func BenchmarkThroughputBatchedSet(b *testing.B) {
	db0 := benchDB(b)
	if !db0.Capabilities().Batch {
		b.Skip("基座未实现 Batch 能力")
	}
	db0.Close()

	ctx := context.Background()
	db := benchDB(b)
	var items, batches atomic.Int64
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		base := int64(0)
		for pb.Next() {
			first := base
			if err := db.Batch(ctx, func(w *kvdb.Batch) error {
				for j := 0; j < benchBatchSize; j++ {
					w.Set(fmt.Sprintf("thr:bat:%d", first+int64(j)), benchValue)
				}
				return nil
			}); err != nil {
				b.Error(err)
				return
			}
			batches.Add(1)
			items.Add(int64(benchBatchSize))
			base += benchBatchSize * 1000 // 批次间留足间隔，避免 key 重叠
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()

	it := items.Load()
	if it == 0 {
		b.Fatal("时间窗内没有完成任何批")
	}
	b.ReportMetric(float64(it)/elapsed.Seconds(), "items/s")
	b.ReportMetric(float64(batches.Load())/elapsed.Seconds(), "batches/s")
	b.ReportMetric(float64(elapsed.Microseconds())/float64(it), "µs/item-actual")
}
