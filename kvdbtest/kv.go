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
	"sync"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

// KV 基本语义：增删查改、计数器、批量读。

func TestKV(t *testing.T, db kvdb.DB) {
	ctx := context.Background()

	// 基本读写与缺失
	if _, ok, err := db.Get(ctx, "a"); err != nil || ok {
		t.Fatalf("Get missing = %v, %v", ok, err)
	}
	bin := []byte{0xff, 0xfe, 0x00, '\n', 0x80}
	if err := db.Set(ctx, "a", bin); err != nil {
		t.Fatal(err)
	}
	got, ok, err := db.Get(ctx, "a")
	if err != nil || !ok || string(got) != string(bin) {
		t.Fatalf("Get = %q,%v,%v", got, ok, err)
	}
	if ex, err := db.Exists(ctx, "a"); err != nil || !ex {
		t.Fatalf("Exists = %v,%v", ex, err)
	}

	// Set 覆盖 + 保留 TTL 语义
	if err := db.Set(ctx, "a", []byte(string(bin)+"!")); err != nil {
		t.Fatal(err)
	}
	if err := db.Expire(ctx, "a", 100); err != nil {
		t.Fatal(err)
	}
	secs, hasTTL, err := db.TTL(ctx, "a")
	if err != nil || !hasTTL || secs <= 0 || secs > 100 {
		t.Fatalf("TTL = %d,%v,%v", secs, hasTTL, err)
	}
	if err := db.Set(ctx, "a", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if secs2, hasTTL2, _ := db.TTL(ctx, "a"); !hasTTL2 || secs2 <= 0 {
		t.Fatalf("Set 应保留 TTL，got hasTTL=%v secs=%d", hasTTL2, secs2)
	}
	if v, ok, _ := db.Get(ctx, "a"); !ok || string(v) != "v2" {
		t.Fatalf("Set 覆盖后 Get = %q,%v", v, ok)
	}

	// SetEx：一次写入值与 TTL（对应 Redis SETEX / SSDB setx）
	if err := db.SetEx(ctx, "se", []byte("sv"), 50); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := db.Get(ctx, "se"); !ok || string(v) != "sv" {
		t.Fatalf("SetEx 后 Get = %q,%v", v, ok)
	}
	if s, has, err := db.TTL(ctx, "se"); err != nil || !has || s <= 0 || s > 50 {
		t.Fatalf("SetEx 后 TTL = %d,%v,%v", s, has, err)
	}
	// SetEx 覆盖既有 TTL（而非叠加）：先设 100，再设 20，剩余应 <= 20
	if err := db.SetEx(ctx, "se", []byte("sv2"), 20); err != nil {
		t.Fatal(err)
	}
	if s, has, _ := db.TTL(ctx, "se"); !has || s <= 0 || s > 20 {
		t.Fatalf("SetEx 应覆盖旧 TTL, got %d,%v", s, has)
	}
	// 非正 TTL 拒绝
	if err := db.SetEx(ctx, "se", []byte("x"), 0); !errors.Is(err, core.ErrInvalidTTL) {
		t.Fatalf("SetEx ttl=0 应 ErrInvalidTTL, got %v", err)
	}
	if err := db.Del(ctx, "se"); err != nil {
		t.Fatal(err)
	}

	// SetExAt / ExpireAt：绝对到期时刻（对应 Redis SETEXAT / EXPIREAT）
	now := core.NowUnix()
	if err := db.SetExAt(ctx, "at1", []byte("av"), now+40); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := db.Get(ctx, "at1"); !ok || string(v) != "av" {
		t.Fatalf("SetExAt 后 Get = %q,%v", v, ok)
	}
	if secs, has, err := db.TTL(ctx, "at1"); err != nil || !has || secs <= 0 || secs > 40 {
		t.Fatalf("SetExAt 后 TTL = %d,%v,%v", secs, has, err)
	}
	// 绝对时刻不受"写入时点"影响：把它改成更晚的绝对时刻，剩余秒数应随之变大
	if err := db.SetExAt(ctx, "at1", []byte("av"), now+80); err != nil {
		t.Fatal(err)
	}
	if secs, has, _ := db.TTL(ctx, "at1"); !has || secs <= 40 {
		t.Fatalf("SetExAt 改晚后 TTL 应变大, got %d,%v", secs, has)
	}
	// at 已是过去时间 -> 删除该 key（与 Redis SETEXAT 一致）
	if err := db.SetExAt(ctx, "at1", []byte("gone"), now-1); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := db.Get(ctx, "at1"); ok {
		t.Fatal("SetExAt 过去时间点应删除 key")
	}

	// ExpireAt：key 存在 -> 设置绝对到期；不存在 -> 不报错
	if err := db.Set(ctx, "at2", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.ExpireAt(ctx, "at2", now+30); err != nil {
		t.Fatal(err)
	}
	if secs, has, _ := db.TTL(ctx, "at2"); !has || secs <= 0 || secs > 30 {
		t.Fatalf("ExpireAt 后 TTL = %d,%v", secs, has)
	}
	// 过去时间点 -> 立即删除
	if err := db.ExpireAt(ctx, "at2", now-1); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := db.Get(ctx, "at2"); ok {
		t.Fatal("ExpireAt 过去时间点应删除 key")
	}
	// 不存在的 key：不视为错误（与 Expire 一致）
	if err := db.ExpireAt(ctx, "at-absent", now+30); err != nil {
		t.Fatalf("ExpireAt 对不存在的 key 不应报错: %v", err)
	}
	if err := db.Del(ctx, "at1"); err != nil {
		t.Fatal(err)
	}

	// 无 TTL 的 key：TTL ok=false
	if _, has, err := db.TTL(ctx, "noexpire"); err != nil || has {
		t.Fatalf("TTL 无过期 key = %v,%v", has, err)
	}

	// Del
	if err := db.Del(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if ok, err := db.Exists(ctx, "a"); err != nil || ok {
		t.Fatalf("Del 后仍存在 (err=%v)", err)
	}

	// Incr：缺失按 0 起算；非整数报错；并发原子性
	if n, err := db.Incr(ctx, "cnt", 5); err != nil || n != 5 {
		t.Fatalf("Incr missing = %d,%v", n, err)
	}
	if n, err := db.Incr(ctx, "cnt", -2); err != nil || n != 3 {
		t.Fatalf("Incr = %d,%v", n, err)
	}
	if err := db.Set(ctx, "bad", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Incr(ctx, "bad", 1); !errors.Is(err, core.ErrNotInteger) {
		t.Fatalf("Incr 非整数应 ErrNotInteger，got %v", err)
	}

	const goroutines, per = 8, 25
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if _, err := db.Incr(ctx, "race", 1); err != nil {
					t.Errorf("Incr race: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if n, err := db.Incr(ctx, "race", 0); err != nil || n != goroutines*per {
		t.Fatalf("并发 Incr 结果 = %d (err=%v), want %d", n, err, goroutines*per)
	}

	// 多 key 并发：每个 goroutine 只写自己的 key（互不重叠），
	// 验证互不干扰且无丢更（组提交/行锁语义下不可串寄存器）。
	const mkG, mkPer = 8, 25
	var mkWg sync.WaitGroup
	for g := 0; g < mkG; g++ {
		mkWg.Add(1)
		go func(id int) {
			defer mkWg.Done()
			key := fmt.Sprintf("mk%d", id)
			for i := 0; i < mkPer; i++ {
				if _, err := db.Incr(ctx, key, 1); err != nil {
					t.Errorf("多key Incr(%s): %v", key, err)
					return
				}
			}
		}(g)
	}
	mkWg.Wait()
	for g := 0; g < mkG; g++ {
		n, err := db.Incr(ctx, fmt.Sprintf("mk%d", g), 0)
		if err != nil || n != mkPer {
			t.Fatalf("多key 并发后 mk%d = %d (err=%v), want %d", g, n, err, mkPer)
		}
	}

	// MGet：只返回存在的 key
	for i, k := range []string{"m1", "m2", "m3"} {
		if err := db.Set(ctx, k, []byte("v"+string(rune('0'+i)))); err != nil {
			t.Fatal(err)
		}
	}
	gotm, err := db.MGet(ctx, "m1", "missing", "m3")
	if err != nil {
		t.Fatal(err)
	}
	if len(gotm) != 2 || string(gotm["m1"]) != "v0" || string(gotm["m3"]) != "v2" {
		t.Fatalf("MGet = %v", gotm)
	}

	// Scan：闭区间、升序、limit
	if err := db.Del(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Del(ctx, "m2"); err != nil {
		t.Fatal(err)
	}
	if err := db.Del(ctx, "m3"); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"k1", "k2", "k3", "k4", "k5"} {
		if err := db.Set(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	gotScan, err := db.Scan(ctx, "k2", "k4", 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"k2", "k3", "k4"}
	if len(gotScan) != len(want) {
		t.Fatalf("Scan len = %d, want %d: %v", len(gotScan), len(want), gotScan)
	}
	for i, kv := range gotScan {
		if kv.Key != want[i] {
			t.Fatalf("Scan[%d] = %s, want %s", i, kv.Key, want[i])
		}
	}
	// 无上界：先清掉前面并发用例遗留的键（race/cnt/bad 及多 key 并发的 mk*，
	// 它们落在 k4 之后会影响开区间断言），保证断言只覆盖本用例写入的数据。
	clean := []string{"race", "cnt", "bad"}
	for g := 0; g < 8; g++ {
		clean = append(clean, fmt.Sprintf("mk%d", g))
	}
	for _, k := range clean {
		if err := db.Del(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	open, err := db.Scan(ctx, "k4", "", 0)
	if err != nil || len(open) != 2 || open[0].Key != "k4" || open[1].Key != "k5" {
		t.Fatalf("Scan open-end = %v, %v", open, err)
	}

	// Scan × limit：start 键存在且区间内键数 > limit 时，必须返回闭区间的
	// **前 limit 个**（含 start 键本身）。
	for _, k := range []string{"sl2", "sl3", "sl4", "sl5", "sl6"} {
		if err := db.Set(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	gotLimit, err := db.Scan(ctx, "sl2", "slz", 3)
	if err != nil {
		t.Fatal(err)
	}
	wantLimit := []string{"sl2", "sl3", "sl4"}
	if len(gotLimit) != len(wantLimit) {
		t.Fatalf("Scan limit len = %d, want %d: %v", len(gotLimit), len(wantLimit), gotLimit)
	}
	for i, kv := range gotLimit {
		if kv.Key != wantLimit[i] {
			t.Fatalf("Scan limit[%d] = %s, want %s", i, kv.Key, wantLimit[i])
		}
	}
	for _, k := range []string{"sl2", "sl3", "sl4", "sl5", "sl6"} {
		if err := db.Del(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
}
