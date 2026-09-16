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
	"sync"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

// 过期与 TTL：过期键在各写路径上的语义、并发读、及边界值。

// TestExpiredWrites 覆盖"过期键 = 不存在"在**写路径**上的语义：
// 过期后 Set 必须可见（不得继承旧的过期时间）、Incr 必须从 0 起算
// （不得在陈旧值上累加）、Expire 不得复活过期键。
func TestExpiredWrites(t *testing.T, db kvdb.DB, opt Options) {
	ctx := context.Background()

	// Set 后必须可读，且不再带 TTL。
	if err := db.SetEx(ctx, "ew:set", []byte("old"), 1); err != nil {
		t.Fatal(err)
	}
	waitExpired(t, db, opt, "ew:set")
	if err := db.Set(ctx, "ew:set", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := db.Get(ctx, "ew:set"); err != nil || !ok || string(v) != "new" {
		t.Fatalf("过期后 Set 应可见: v=%q ok=%v err=%v", v, ok, err)
	}
	if _, has, err := db.TTL(ctx, "ew:set"); err != nil || has {
		t.Fatalf("过期后 Set 不应保留 TTL: has=%v err=%v", has, err)
	}

	// Incr 从 0 起算，不得复用陈旧值 5。
	if err := db.SetEx(ctx, "ew:incr", []byte("5"), 1); err != nil {
		t.Fatal(err)
	}
	waitExpired(t, db, opt, "ew:incr")
	if n, err := db.Incr(ctx, "ew:incr", 1); err != nil || n != 1 {
		t.Fatalf("过期后 Incr = %d,%v (want 1，不应在陈旧值上累加)", n, err)
	}
	if v, ok, err := db.Get(ctx, "ew:incr"); err != nil || !ok || string(v) != "1" {
		t.Fatalf("过期后 Incr 结果应可见: v=%q ok=%v err=%v", v, ok, err)
	}

	// Incr 遇到"过期 + 非整数"：过期优先，按不存在处理。
	if err := db.SetEx(ctx, "ew:bad", []byte("abc"), 1); err != nil {
		t.Fatal(err)
	}
	waitExpired(t, db, opt, "ew:bad")
	if n, err := db.Incr(ctx, "ew:bad", 2); err != nil || n != 2 {
		t.Fatalf("过期后 Incr(非整数旧值) = %d,%v (want 2)", n, err)
	}

	// Expire 不得复活过期键。
	if err := db.SetEx(ctx, "ew:exp", []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	waitExpired(t, db, opt, "ew:exp")
	if err := db.Expire(ctx, "ew:exp", 100); err != nil {
		t.Fatal(err)
	}
	if ok, err := db.Exists(ctx, "ew:exp"); err != nil || ok {
		t.Fatalf("Expire 不应复活过期键: ok=%v err=%v", ok, err)
	}

	// SetEx 覆盖过期键：正常（显式写 expire_at）。
	if err := db.SetEx(ctx, "ew:setx", []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	waitExpired(t, db, opt, "ew:setx")
	if err := db.SetEx(ctx, "ew:setx", []byte("v2"), 100); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := db.Get(ctx, "ew:setx"); err != nil || !ok || string(v) != "v2" {
		t.Fatalf("过期后 SetEx 应可见: v=%q ok=%v err=%v", v, ok, err)
	}
}

// TestNamespaceIndependence 验证三类数据的命名空间彼此独立：同名 KV 键、

// TestExpiredConcurrentRead 在 key 过期瞬间用多个读者并发触发惰性清理，
// 配合 -race 捕捉"读锁下改写 map"的数据竞争（mem/jsonl）。
func TestExpiredConcurrentRead(t *testing.T, db kvdb.DB, opt Options) {
	ctx := context.Background()
	keys := []string{"cr:1", "cr:2", "cr:3", "cr:4"}
	for _, k := range keys {
		if err := db.SetEx(ctx, k, []byte("v"), 1); err != nil {
			t.Fatal(err)
		}
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, k := range keys {
					db.Get(ctx, k)
					db.Exists(ctx, k)
					db.MGet(ctx, keys...)
					db.TTL(ctx, k)
				}
				db.Scan(ctx, "", "", 100)
			}
		}()
	}
	// 跨越过期时刻：真实基座靠真实时间，虚拟时钟替身（miniredis）靠 FastForward。
	if opt.FastForward != nil {
		time.Sleep(100 * time.Millisecond)
		opt.FastForward()
		time.Sleep(200 * time.Millisecond)
	} else {
		time.Sleep(1500 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
}

// TestTTLBoundaries 覆盖 TTL 的边界：无 TTL/不存在的区分、非正 TTL 拒绝、
// 绝对时刻的过去/未来、以及超大 TTL 不得溢出成"已过期"。
func TestTTLBoundaries(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	now := core.NowUnix()

	// 无 TTL 的 key：ok=false（不是"有 TTL 且为 0"）
	if err := db.Set(ctx, "tb:noTtl", []byte("v")); err != nil {
		t.Fatal(err)
	}
	// 注意：ok=false 时 seconds **无契约含义**（各基座可能给 0 或 -1），
	// 因此只断言 ok=false 与无错，不断言具体数值。
	if secs, ok, err := db.TTL(ctx, "tb:noTtl"); err != nil || ok {
		t.Fatalf("无 TTL 的 key 应 ok=false 且无错, got %d,%v,%v", secs, ok, err)
	}
	// 不存在的 key：同样 ok=false
	if _, ok, err := db.TTL(ctx, "tb:absent"); err != nil || ok {
		t.Fatalf("不存在的 key TTL 应 ok=false, got ok=%v err=%v", ok, err)
	}

	// 非正 TTL 一律拒绝（契约 ErrInvalidTTL）
	for _, ttl := range []int64{0, -1, math.MinInt64} {
		if err := db.SetEx(ctx, "tb:x", []byte("v"), ttl); !errors.Is(err, core.ErrInvalidTTL) {
			t.Fatalf("SetEx ttl=%d 应 ErrInvalidTTL, got %v", ttl, err)
		}
		if err := db.Expire(ctx, "tb:noTtl", ttl); !errors.Is(err, core.ErrInvalidTTL) {
			t.Fatalf("Expire ttl=%d 应 ErrInvalidTTL, got %v", ttl, err)
		}
	}

	// 正 TTL 生效，且剩余量在合理范围（1..ttl）
	if err := db.SetEx(ctx, "tb:pos", []byte("v"), 100); err != nil {
		t.Fatal(err)
	}
	if secs, ok, err := db.TTL(ctx, "tb:pos"); err != nil || !ok || secs <= 0 || secs > 100 {
		t.Fatalf("SetEx(100) 后 TTL 应在 (0,100], got %d,%v,%v", secs, ok, err)
	}

	// ExpireAt：未来时刻 = 有 TTL；过去时刻 = **删除**该键（契约）
	if err := db.Set(ctx, "tb:at", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.ExpireAt(ctx, "tb:at", now+100); err != nil {
		t.Fatal(err)
	}
	if secs, ok, err := db.TTL(ctx, "tb:at"); err != nil || !ok || secs <= 0 || secs > 100 {
		t.Fatalf("ExpireAt(未来) 后 TTL 应在 (0,100], got %d,%v,%v", secs, ok, err)
	}
	if err := db.ExpireAt(ctx, "tb:at", now-1); err != nil {
		t.Fatalf("ExpireAt(过去) 不应报错: %v", err)
	}
	if _, ok, _ := db.Get(ctx, "tb:at"); ok {
		t.Fatal("ExpireAt(过去) 应删除该键")
	}
	// ExpireAt 对不存在的键不报错
	if err := db.ExpireAt(ctx, "tb:absent", now+100); err != nil {
		t.Fatalf("ExpireAt 对不存在的键不应报错: %v", err)
	}

	// SetExAt：未来写入且可见；过去 = 删除
	if err := db.SetExAt(ctx, "tb:seAt", []byte("v"), now+100); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := db.Get(ctx, "tb:seAt"); err != nil || !ok || string(v) != "v" {
		t.Fatalf("SetExAt(未来) 后应可读, got %q,%v,%v", v, ok, err)
	}
	if err := db.SetExAt(ctx, "tb:seAt", []byte("x"), now-1); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := db.Get(ctx, "tb:seAt"); ok {
		t.Fatal("SetExAt(过去) 应删除该键")
	}

	// 超大 TTL 必须饱和而非溢出成负（core.AddTTL 的语义）：
	// 溢出实现会让该键立刻"已过期"，读不到。
	if err := db.SetEx(ctx, "tb:huge", []byte("v"), math.MaxInt64); err != nil {
		t.Fatalf("SetEx(MaxInt64) 不应报错: %v", err)
	}
	if v, ok, err := db.Get(ctx, "tb:huge"); err != nil || !ok || string(v) != "v" {
		t.Fatalf("SetEx(MaxInt64) 后应可读（溢出会变成已过期）, got %q,%v,%v", v, ok, err)
	}
	if _, ok, _ := db.TTL(ctx, "tb:huge"); !ok {
		t.Fatal("SetEx(MaxInt64) 后 TTL 应 ok=true")
	}
}

// TestIncrBoundaries 覆盖 Incr 的解析与溢出边界。
//
// 溢出是本套件里**唯一**被 Caps 显式声明的差异点（IncrWraps），因此这里分支断言
