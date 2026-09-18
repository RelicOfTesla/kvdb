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
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

// batchDeadlockTimeout 是批写锁死锁用例的超时兜底：批在独立 goroutine 里执行，
// 超时即判定为死锁（而不是让 go test -timeout 静默杀掉整个套件）。30s 远大于
// 任一基座提交几百条 op 的正常耗时，也大到足以区分"死锁"与"慢"。
const batchDeadlockTimeout = 30 * time.Second

// 批量写：一次提交的原子性与批内可见性（后者由 Caps.BatchComposed 声明）。

// TestBatch 覆盖批量写契约：一次提交内混合 KV/Queue/ZSet 操作，
// 验证顺序、覆盖语义、TTL、以及与逐条读的一致性。
func TestBatch(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	if !db.Capabilities().Batch {
		t.Skip("基座未实现 Batch 能力")
	}

	// 一批混合操作
	err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("bk1", []byte("v1"))
		b.Set("bk2", []byte("old"))
		b.Set("bk2", []byte("new")) // 同批内覆盖：后者生效
		b.SetEx("bk3", []byte("v3"), 100)
		b.QPush("bq", []byte("a"))
		b.QPush("bq", []byte("b"))
		b.QPushFront("bq", []byte("z"))
		b.ZSet("bz", "m1", 5)
		b.ZIncr("bz", "m1", 3)
		b.Set("bk4", []byte("v4"))
		b.Del("bk4")
		b.Expire("bk1", 50)
		if b.Len() != 12 {
			t.Fatalf("收集操作数 = %d, want 12", b.Len())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}

	if v, ok, _ := db.Get(ctx, "bk1"); !ok || string(v) != "v1" {
		t.Fatalf("bk1 = %q,%v", v, ok)
	}
	// 以下三项属于"批内可见性"（同批后续操作看到前序效果）。契约不要求它，
	// 各基座取决于自身机制，因此只在 Capabilities().BatchComposed 为真时断言。
	composed := db.Capabilities().BatchComposed
	if composed {
		if v, ok, _ := db.Get(ctx, "bk2"); !ok || string(v) != "new" {
			t.Fatalf("同批覆盖应取后者, bk2 = %q,%v", v, ok)
		}
	}
	if secs, has, _ := db.TTL(ctx, "bk3"); !has || secs <= 0 || secs > 100 {
		t.Fatalf("bk3 TTL = %d,%v", secs, has)
	}
	if secs, has, _ := db.TTL(ctx, "bk1"); !has || secs <= 0 || secs > 50 {
		t.Fatalf("bk1 批内 Expire = %d,%v", secs, has)
	}

	// 批内 Set 不清掉既有 TTL（契约：Set 不改变已存在键的 TTL）——这条对
	// 批内的 Set 同样成立，不只是单条 Set。
	if err := db.SetEx(ctx, "bkttl", []byte("v"), 30); err != nil {
		t.Fatal(err)
	}
	if err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("bkttl", []byte("v2"))
		return nil
	}); err != nil {
		t.Fatalf("Batch bkttl: %v", err)
	}
	if v, ok, _ := db.Get(ctx, "bkttl"); !ok || string(v) != "v2" {
		t.Fatalf("bkttl = %q,%v", v, ok)
	}
	if secs, has, _ := db.TTL(ctx, "bkttl"); !has || secs <= 0 || secs > 30 {
		t.Fatalf("批内 Set 后 TTL = %d,%v, want 0<secs<=30", secs, has)
	}
	if err := db.Del(ctx, "bkttl"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := db.Exists(ctx, "bk4"); ok {
		t.Fatal("批内 Del 应生效")
	}
	// 队列顺序与 zset 累加同样只在具备批内可见性时断言。
	if composed {
		for _, want := range []string{"z", "a", "b"} {
			if v, ok, _ := db.QPop(ctx, "bq"); !ok || string(v) != want {
				t.Fatalf("批内队列顺序 QPop = %q,%v; want %q", v, ok, want)
			}
		}
		if s, ok, _ := db.ZGet(ctx, "bz", "m1"); !ok || s != 8 {
			t.Fatalf("批内 ZSet+ZIncr = %d,%v (5+3=8)", s, ok)
		}
	} else {
		// 不具备批内可见性的基座：至少要求整批已落库（元素数 > 0 且成员存在），
		// 具体终值依机制而定，不作断言。
		if n, err := db.QSize(ctx, "bq"); err != nil || n == 0 {
			t.Fatalf("批写后队列不应为空: n=%d err=%v", n, err)
		}
		if _, ok, _ := db.ZGet(ctx, "bz", "m1"); !ok {
			t.Fatal("批写后 zset 成员应存在")
		}
		db.Del(ctx, "bq")
	}

	// 空批：无操作、无错误
	if err := db.Batch(ctx, func(b *kvdb.Batch) error { return nil }); err != nil {
		t.Fatalf("空批应无错: %v", err)
	}

	// 回调返回错误 -> 整批不提交
	sentinel := errors.New("abort")
	if err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("bk9", []byte("x"))
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("回调错误应原样返回, got %v", err)
	}
	if ok, _ := db.Exists(ctx, "bk9"); ok {
		t.Fatal("回调出错时不应提交")
	}

	// 收集期校验失败 -> 整批不提交
	if err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("bk10", []byte("x"))
		b.SetEx("bk11", []byte("y"), 0)
		return nil
	}); !errors.Is(err, core.ErrInvalidTTL) {
		t.Fatalf("非法 TTL 应 ErrInvalidTTL, got %v", err)
	}
	if ok, _ := db.Exists(ctx, "bk10"); ok {
		t.Fatal("校验失败时整批不应生效")
	}
}

// TestBatchComposed 校验"批内组合结果"的跨基座行为：
//
//   - 声明 BatchComposed=true 的基座，组合语义必须是**确定**的：
//     同批覆盖同一 key、同批同队列按声明顺序入队且序号不撞、
//     ZSet+ZIncr 同批累加、成员 Set->Del->Set 计数正确；
//   - 未声明（BatchComposed=false）的基座，契约允许组合终值有差异，
//     这里只校验不依赖批内可见性的底线：每条 op 至少各生效一次。
//
// 该用例与 Capabilities() 联动，把"契约照着写、测试逼着对"落到跨基座层面：
// 批内计数互相覆盖、元素丢失、序号碰撞这类问题，只能靠逐个基座跑同一套断言发现。
func TestBatchComposed(t *testing.T, db kvdb.DB) {
	caps := db.Capabilities()
	if !caps.Batch {
		t.Skip("基座不支持 Batch")
	}
	ctx := context.Background()
	composed := caps.BatchComposed

	// 1. 同批覆盖同一 key
	if err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("bc:over", []byte("first"))
		b.Set("bc:over", []byte("second"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	v, ok, err := db.Get(ctx, "bc:over")
	if err != nil || !ok {
		t.Fatalf("Get after batch: %v ok=%v err=%v", v, ok, err)
	}
	if composed && string(v) != "second" {
		t.Fatalf("BatchComposed 基座同批覆盖应为\"后者覆盖前者\"，got %q", v)
	}

	// 2. 同批同队列按声明顺序入队（序号不碰撞是正确性的硬指标，
	// 任何基座出现同批元素丢失都必须在这里被发现——composed 与否）
	if err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.QPush("bc:q", []byte("a"))
		b.QPush("bc:q2", []byte("x"))
		b.QPush("bc:q2", []byte("y"))
		b.QPush("bc:q2", []byte("z"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	n, err := db.QSize(ctx, "bc:q2")
	if err != nil || n != 3 {
		t.Fatalf("同批同队列 3 条入队后 QSize=%d err=%v（不得丢元素）", n, err)
	}
	if composed {
		var got []byte
		for i, want := range []string{"x", "y", "z"} {
			got, ok, err = db.QPop(ctx, "bc:q2")
			if err != nil || !ok || string(got) != want {
				t.Fatalf("队列序 #%d: got=%q ok=%v err=%v (want %q)", i, got, ok, err, want)
			}
		}
	}

	// 3. ZSet + ZIncr 同批累加
	if err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.ZSet("bc:z", "m", 10)
		b.ZIncr("bc:z", "m", 5)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	score, ok, err := db.ZGet(ctx, "bc:z", "m")
	if err != nil || !ok {
		t.Fatalf("ZGet: %d ok=%v err=%v", score, ok, err)
	}
	if composed && score != 15 {
		t.Fatalf("BatchComposed 基座 ZSet+ZIncr 应累加 (10+5)，got %d", score)
	}
	if !composed && score < 10 {
		t.Fatalf("非批量可见性基座至少保证成员存在且分数不被前序覆盖变负: %d", score)
	}

	// 4. 成员 Set -> Del -> Set 在批内不漏计数
	if err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.ZSet("bc:z2", "m", 1)
		b.ZDel("bc:z2", "m")
		b.ZSet("bc:z2", "m", 2)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	size, err := db.ZSize(ctx, "bc:z2")
	if err != nil {
		t.Fatal(err)
	}
	// composed 基座最终恰 1 个成员；无论 composed 与否，成员都必须可达，且
	// ZSize 与 ZGet/ZDel 的观察结果一致（计数一旦失真，三者就会互相矛盾）。
	if size != 1 {
		t.Fatalf("Set->Del->Set 后成员数应为 1（计数不得失真）: %d", size)
	}
	if s, ok, err := db.ZGet(ctx, "bc:z2", "m"); err != nil || !ok || s != 2 {
		t.Fatalf("成员分数应可达且为批内末值 2: %d ok=%v err=%v", s, ok, err)
	}

	// 5. 能力自洽断言：Capabilities().IncrWraps 声明的是真承诺——声明"回绕"
	//    就必须真的回绕，未声明就不得静默回绕。与 composed 的确定性断言
	//    同属"能力即承诺"。
	if caps.IncrWraps {
		// 回绕型基座：设置 MaxInt64 再加正数必须回绕，不报错。
		if err := db.Set(ctx, "bc:wrap", []byte("9223372036854775807")); err != nil {
			t.Fatal(err)
		}
		if n, err := db.Incr(ctx, "bc:wrap", 1); err != nil || n != math.MinInt64 {
			t.Fatalf("IncrWraps 基座应回绕至 MinInt64，got %d err=%v", n, err)
		}
	} else {
		if err := db.Set(ctx, "bc:wrap", []byte("9223372036854775807")); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Incr(ctx, "bc:wrap", 1); !errors.Is(err, core.ErrNotInteger) && err == nil {
			// 报错型基座并不要求必然用该哨兵（远端可能给别的形态），
			// 只要求**不能**"静默回绕成负数"。
			t.Fatalf("非回绕基座应报错而非回绕: %v", err)
		}
	}
}

// TestBatchManyNamesNoDeadlock 是"批写锁自死锁 / 次序环"的**引擎无关**回归用例。
//
// 缺陷回顾（逐引擎的白盒版本见 badger/batchlock_test.go、leveldb/batchlock_test.go）：
// ApplyBatch 曾按**名字序**逐个对"每个名字的分片锁"加锁，于是有两类永久阻塞：
//
//  1. 自死锁：两个不同名字落到同一分片时，同一批会对同一把不可重入的
//     sync.Mutex 二次 Lock()；
//  2. ABBA 次序环：两个并发批以不同名字序加锁时互相等待彼此已持有的分片锁。
//
// 两者都只在特定名字集合/交错下触发，触发后表现是**永久阻塞而非报错**，
// 因此必须在所有基座上持续回归，不能只靠 badger/leveldb 的白盒用例。
//
// 与白盒用例的分工：白盒用例直接调用引擎内部哈希精确构造"同分片"；本用例
// **不依赖任何引擎的内部哈希或分片数**，只靠生日悖论——一个批里放入大量不同的
// 队列名与 zset 名（各 128 个），任何固定分片数的锁实现都会在这些名字间产生
// 同分片碰撞；若实现未做"按互斥锁去重 + 按分片下标全序加锁"，就会死锁。
//
// 超时兜底：批在独立 goroutine 里执行，select + time.After 把"永久阻塞"变成
// 一条定位明确的测试失败，而不是让整个套件挂死到外层 go test -timeout。
// （若真死锁，只泄漏一个永久阻塞的 goroutine——它持有的是本用例实例的分片锁，
// 因此本用例放在 RunWithOptions 末尾，避免阻塞同实例上的其它子用例。）
//
// 门禁：Batch 能力缺失即 skip；Queue/ZSet 能力缺失时也无从触发（锁键来自这两类
// 名字），一并 skip。不与既有用例的键前缀混淆：本用例统一用 "bml:" 前缀，避免在
// 共用同一实例的基座（如 ssdb 的假服务端）上被其它子用例的键干扰或反向污染。
//
// 除"不死锁"外还断言"结果对"：每个名字对应的数据结构必须存在且内容正确，
// 否则锁修复本身没有意义。
func TestBatchManyNamesNoDeadlock(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	caps := db.Capabilities()
	if !caps.Batch {
		t.Skip("基座未实现 Batch 能力")
	}
	if !caps.Queue || !caps.ZSet {
		t.Skipf("基座未实现 Queue(%v)/ZSet(%v) 能力，批写锁的分片碰撞无从触发", caps.Queue, caps.ZSet)
	}

	const (
		n      = 128 // 每类名字数：远大于常见分片数，碰撞由生日悖论保证
		prefix = "bml:"
	)

	// ---- 阶段 1：单批大量不同名字，回归"同分片二次加锁"的自死锁 ----
	// 128 个名字对 64 分片的期望碰撞对数约 128*127/2/64 ≈ 127，必然发生；
	// 对分片数更多（如 256/1024）的实现，128 个名字仍有相当概率碰撞，
	// 故这里不假设分片数，只依赖"名字足够多"。
	queues := make([]string, n)
	zsets := make([]string, n)
	for i := 0; i < n; i++ {
		queues[i] = fmt.Sprintf("%sq%03d", prefix, i)
		zsets[i] = fmt.Sprintf("%sz%03d", prefix, i)
	}
	kvKeys := make([]string, 32)
	for i := range kvKeys {
		kvKeys[i] = fmt.Sprintf("%sk%02d", prefix, i)
	}

	batchDone := make(chan error, 1)
	go func() {
		batchDone <- db.Batch(ctx, func(b *kvdb.Batch) error {
			// 顺序刻意交错三类名字，逼近真实混合批。
			for i := 0; i < n; i++ {
				b.QPush(queues[i], []byte(fmt.Sprintf("qv%03d", i)))
				b.ZSet(zsets[i], "m", int64(i+1))
			}
			for i, k := range kvKeys {
				b.Set(k, []byte(fmt.Sprintf("kv%02d", i)))
			}
			return nil
		})
	}()
	select {
	case err := <-batchDone:
		if err != nil {
			t.Fatalf("Batch: %v", err)
		}
	case <-time.After(batchDeadlockTimeout):
		t.Fatalf("Batch 在 %s 内未返回（疑似批写锁自死锁）：单批 %d 个队列名 + %d 个 zset 名 + %d 个 KV 名；"+
			"若 lockKeys 按名字序逐个加不可重入的分片锁，同分片的两个名字会对同一把锁二次 Lock()；"+
			"请确认已按互斥锁去重且按分片下标全序加锁",
			batchDeadlockTimeout, len(queues), len(zsets), len(kvKeys))
	}

	// 不死锁之外还要结果对：所有名字的数据结构存在且内容正确。
	for i, qn := range queues {
		want := fmt.Sprintf("qv%03d", i)
		if got, err := db.QSize(ctx, qn); err != nil || got != 1 {
			t.Fatalf("批后 QSize(%q) = %d,%v; want 1（元素丢失或落库不完整）", qn, got, err)
		}
		if v, ok, err := db.QFront(ctx, qn); err != nil || !ok || string(v) != want {
			t.Fatalf("批后 QFront(%q) = %q,%v,%v; want %q", qn, v, ok, err, want)
		}
	}
	for i, zn := range zsets {
		if s, ok, err := db.ZGet(ctx, zn, "m"); err != nil || !ok || s != int64(i+1) {
			t.Fatalf("批后 ZGet(%q, m) = %d,%v,%v; want %d", zn, s, ok, err, i+1)
		}
		if got, err := db.ZSize(ctx, zn); err != nil || got != 1 {
			t.Fatalf("批后 ZSize(%q) = %d,%v; want 1", zn, got, err)
		}
	}
	for i, k := range kvKeys {
		want := fmt.Sprintf("kv%02d", i)
		if v, ok, err := db.Get(ctx, k); err != nil || !ok || string(v) != want {
			t.Fatalf("批后 Get(%q) = %q,%v,%v; want %q", k, v, ok, err, want)
		}
	}

	// ---- 阶段 2：并发批以**正序/逆序**操作同一组名字，回归 ABBA 次序环 ----
	// 若实现仍按"名字序"加锁，正序批与逆序批会以相反顺序争抢同一组分片锁而成环；
	// 按分片下标全序加锁后，所有批的加锁顺序一致，只会排队不会成环。
	type namedOp struct {
		zset bool
		name string
	}
	const (
		phase2Names   = 48
		phase2Batches = 4
	)
	ops := make([]namedOp, 0, phase2Names*2)
	for i := 0; i < phase2Names; i++ {
		ops = append(ops, namedOp{false, fmt.Sprintf("%sabq%03d", prefix, i)})
		ops = append(ops, namedOp{true, fmt.Sprintf("%sabz%03d", prefix, i)})
	}
	errs := make(chan error, phase2Batches)
	for g := 0; g < phase2Batches; g++ {
		g := g
		go func() {
			errs <- db.Batch(ctx, func(b *kvdb.Batch) error {
				apply := func(op namedOp) {
					if op.zset {
						b.ZSet(op.name, fmt.Sprintf("m%d", g), int64(g+1))
					} else {
						b.QPush(op.name, []byte(fmt.Sprintf("v%d", g)))
					}
				}
				if g%2 == 0 {
					for _, op := range ops {
						apply(op)
					}
					return nil
				}
				for i := len(ops) - 1; i >= 0; i-- {
					apply(ops[i])
				}
				return nil
			})
		}()
	}
	deadline := time.After(batchDeadlockTimeout)
	for g := 0; g < phase2Batches; g++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("并发批 #%d: %v", g, err)
			}
		case <-deadline:
			t.Fatalf("并发批在 %s 内未全部返回（疑似批写锁 ABBA 次序环）：%d 个批以正序/逆序操作同一组 %d 个名字；"+
				"请确认所有批都按分片下标全序加锁，而不是按名字序",
				batchDeadlockTimeout, phase2Batches, len(ops))
		}
	}
	// 并发批的结果同样要对：每个名字必须收到全部批的写入。
	for i := 0; i < phase2Names; i++ {
		qn := fmt.Sprintf("%sabq%03d", prefix, i)
		if got, err := db.QSize(ctx, qn); err != nil || got != phase2Batches {
			t.Fatalf("并发批后 QSize(%q) = %d,%v; want %d（并发批丢元素）", qn, got, err, phase2Batches)
		}
		zn := fmt.Sprintf("%sabz%03d", prefix, i)
		if got, err := db.ZSize(ctx, zn); err != nil || got != phase2Batches {
			t.Fatalf("并发批后 ZSize(%q) = %d,%v; want %d（并发批丢成员）", zn, got, err, phase2Batches)
		}
		for g := 0; g < phase2Batches; g++ {
			if s, ok, err := db.ZGet(ctx, zn, fmt.Sprintf("m%d", g)); err != nil || !ok || s != int64(g+1) {
				t.Fatalf("并发批后 ZGet(%q, m%d) = %d,%v,%v; want %d", zn, g, s, ok, err, g+1)
			}
		}
	}
}
