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
	"math"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

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

	// 批内 Set 不得清掉既有 TTL（契约：Set 不改变已存在键的 TTL）。
	// 回归 redis 基座此前批内用裸 SET 清 TTL 的分歧。
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
// 该用例与 Capabilities() 联动，正是把"契约照着写、测试逼着对"落到
// 跨基座层面：此前 leveldb 的批内计数互相覆盖 / 元素丢失就是被这类

// TestBatchComposed 校验"批内组合结果"的跨基座行为：
//
//   - 声明 BatchComposed=true 的基座，组合语义必须是**确定**的：
//     同批覆盖同一 key、同批同队列按声明顺序入队且序号不撞、
//     ZSet+ZIncr 同批累加、成员 Set->Del->Set 计数正确；
//   - 未声明（BatchComposed=false）的基座，契约允许组合终值有差异，
//     这里只校验不依赖批内可见性的底线：每条 op 至少各生效一次。
//
// 该用例与 Capabilities() 联动，正是把"契约照着写、测试逼着对"落到
// 跨基座层面：此前 leveldb 的批内计数互相覆盖 / 元素丢失就是被这类
// 序号碰撞漏掉，契约测试补上后自动被所有基座运行。
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
	// composed 基座最终恰 1 个成员；即便 composed 与否，成员也必须可达、
	// ZSize 与 ZGet/ZDel 观察一致（计数失真是此前 leveldb 的实际病灶）。
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
