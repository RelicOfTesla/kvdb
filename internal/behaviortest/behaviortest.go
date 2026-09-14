// Package behaviortest 提供跨基座共享的行为用例（KV/Queue/ZSet 合同），
// 各基座包在测试中通过同一套用例验证其适配一致性，避免逐基座复制断言。
package behaviortest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"kvdb"
	"kvdb/core"
)

// Run 对 factory 产出的新实例依次跑 KV/Queue/ZSet 三套用例；
// factory 每次调用必须返回独立的新基座（测试内部会负责 Close）。
func Run(t *testing.T, factory func(t *testing.T) core.KvProvider) {
	t.Run("KV", func(t *testing.T) { TestKV(t, newDB(t, factory)) })
	t.Run("Queue", func(t *testing.T) { TestQueue(t, newDB(t, factory)) })
	t.Run("ZSet", func(t *testing.T) { TestZSet(t, newDB(t, factory)) })
	t.Run("Batch", func(t *testing.T) { TestBatch(t, newDB(t, factory)) })
}

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

	// 无 TTL 的 key：TTL ok=false
	if _, has, err := db.TTL(ctx, "noexpire"); err != nil || has {
		t.Fatalf("TTL 无过期 key = %v,%v", has, err)
	}

	// Del
	if err := db.Del(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := db.Exists(ctx, "a"); ok {
		t.Fatal("Del 后仍存在")
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
	if n, _ := db.Incr(ctx, "race", 0); n != goroutines*per {
		t.Fatalf("并发 Incr 结果 = %d, want %d", n, goroutines*per)
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
		if n, _ := db.Incr(ctx, fmt.Sprintf("mk%d", g), 0); n != mkPer {
			t.Fatalf("多key 并发后 mk%d = %d, want %d", g, n, mkPer)
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
}

func TestQueue(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	hasQueue, _, _ := db.Capabilities()
	if !hasQueue {
		t.Skip("基座未实现 Queue 能力")
	}

	// 先进先出
	for _, v := range []string{"a", "b", "c"} {
		if err := db.QPush(ctx, "q1", []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := db.QSize(ctx, "q1"); err != nil || n != 3 {
		t.Fatalf("QSize = %d,%v", n, err)
	}
	if v, ok, _ := db.QFront(ctx, "q1"); !ok || string(v) != "a" {
		t.Fatalf("QFront = %q,%v", v, ok)
	}
	if v, ok, _ := db.QBack(ctx, "q1"); !ok || string(v) != "c" {
		t.Fatalf("QBack = %q,%v", v, ok)
	}
	for _, want := range []string{"a", "b", "c"} {
		if v, ok, err := db.QPop(ctx, "q1"); err != nil || !ok || string(v) != want {
			t.Fatalf("QPop = %q,%v,%v; want %q", v, ok, err, want)
		}
	}
	if _, ok, _ := db.QPop(ctx, "q1"); ok {
		t.Fatal("空队列 QPop 应 ok=false")
	}

	// 队头插入 / 队尾弹出
	db.QPushFront(ctx, "q2", []byte("f"))
	db.QPush(ctx, "q2", []byte("b"))
	if v, _, _ := db.QPopBack(ctx, "q2"); string(v) != "b" {
		t.Fatalf("QPopBack = %q", v)
	}
	if v, _, _ := db.QPop(ctx, "q2"); string(v) != "f" {
		t.Fatalf("QPop after front push = %q", v)
	}

	// 二进制值
	bin := []byte{0x00, 0xff, '\n', 0x80}
	if err := db.QPush(ctx, "q3", bin); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := db.QPop(ctx, "q3"); !ok || string(v) != string(bin) {
		t.Fatalf("QPop binary = %q,%v", v, ok)
	}
}

func TestZSet(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	_, hasZSet, _ := db.Capabilities()
	if !hasZSet {
		t.Skip("基座未实现 ZSet 能力")
	}

	// 写入与读取
	if err := db.ZSet(ctx, "r", "a", 3); err != nil {
		t.Fatal(err)
	}
	if err := db.ZSet(ctx, "r", "b", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.ZSet(ctx, "r", "c", 3); err != nil {
		t.Fatal(err)
	}
	if err := db.ZSet(ctx, "r", "d", 2); err != nil {
		t.Fatal(err)
	}
	if n, err := db.ZSize(ctx, "r"); err != nil || n != 4 {
		t.Fatalf("ZSize = %d,%v", n, err)
	}
	if s, ok, _ := db.ZGet(ctx, "r", "c"); !ok || s != 3 {
		t.Fatalf("ZGet = %d,%v", s, ok)
	}
	if _, ok, _ := db.ZGet(ctx, "r", "nope"); ok {
		t.Fatal("ZGet 缺失成员应 ok=false")
	}

	// 排名：按 (score, key) 升序，0 起
	// b(1)=0, d(2)=1, a(3)=2, c(3)=3（同分按 key）
	ranks := map[string]int64{"b": 0, "d": 1, "a": 2, "c": 3}
	for k, want := range ranks {
		if r, ok, err := db.ZRank(ctx, "r", k); err != nil || !ok || r != want {
			t.Fatalf("ZRank(%s) = %d,%v,%v; want %d", k, r, ok, err, want)
		}
	}

	// 范围：redis 风格索引，负索引从末尾
	got, err := db.ZRange(ctx, "r", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"b", "d", "a", "c"}
	if len(got) != len(want) {
		t.Fatalf("ZRange len = %d", len(got))
	}
	for i, it := range got {
		if it.Key != want[i] {
			t.Fatalf("ZRange[%d] = %s(%d), want %s", i, it.Key, it.Score, want[i])
		}
	}
	last2, _ := db.ZRange(ctx, "r", -2, -1)
	if len(last2) != 2 || last2[0].Key != "a" || last2[1].Key != "c" {
		t.Fatalf("ZRange(-2,-1) = %v", last2)
	}
	empty, _ := db.ZRange(ctx, "r", 2, 1)
	if len(empty) != 0 {
		t.Fatalf("ZRange(2,1) 应为空, got %v", empty)
	}

	// zincr：不存在按 0 起算
	if s, err := db.ZIncr(ctx, "r", "e", 5); err != nil || s != 5 {
		t.Fatalf("ZIncr missing = %d,%v", s, err)
	}
	if s, err := db.ZIncr(ctx, "r", "a", -1); err != nil || s != 2 {
		t.Fatalf("ZIncr = %d,%v", s, err)
	}

	// 删除
	if err := db.ZDel(ctx, "r", "e"); err != nil {
		t.Fatal(err)
	}
	if n, _ := db.ZSize(ctx, "r"); n != 4 {
		t.Fatalf("ZDel 后 ZSize = %d", n)
	}
}

// TestBatch 覆盖批量写契约：一次提交内混合 KV/Queue/ZSet 操作，
// 验证顺序、覆盖语义、TTL、以及与逐条读的一致性。
func TestBatch(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	if _, _, hasBatch := db.Capabilities(); !hasBatch {
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
	if v, ok, _ := db.Get(ctx, "bk2"); !ok || string(v) != "new" {
		t.Fatalf("同批覆盖应取后者, bk2 = %q,%v", v, ok)
	}
	if secs, has, _ := db.TTL(ctx, "bk3"); !has || secs <= 0 || secs > 100 {
		t.Fatalf("bk3 TTL = %d,%v", secs, has)
	}
	if secs, has, _ := db.TTL(ctx, "bk1"); !has || secs <= 0 || secs > 50 {
		t.Fatalf("bk1 批内 Expire = %d,%v", secs, has)
	}
	if ok, _ := db.Exists(ctx, "bk4"); ok {
		t.Fatal("批内 Del 应生效")
	}
	// 队列顺序：z, a, b
	for _, want := range []string{"z", "a", "b"} {
		if v, ok, _ := db.QPop(ctx, "bq"); !ok || string(v) != want {
			t.Fatalf("批内队列顺序 QPop = %q,%v; want %q", v, ok, want)
		}
	}
	if s, ok, _ := db.ZGet(ctx, "bz", "m1"); !ok || s != 8 {
		t.Fatalf("批内 ZSet+ZIncr = %d,%v (5+3=8)", s, ok)
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
