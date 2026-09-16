// Package kvdbtest 提供跨基座共享的行为用例（KV/Queue/ZSet/Batch 合同），
// 各基座包在测试中通过同一套用例验证其适配一致性，避免逐基座复制断言。
package kvdbtest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

// Options 控制共享用例的可选行为。
type Options struct {
	// FastForward 推进"虚拟时钟"（如 miniredis 的 FastForward）：这类进程内替身
	// 不随真实时间过期，等待过期的用例会调用它来推进时间。真实基座保持 nil。
	FastForward func()
}

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
// 队列、zset 必须能共存且互不影响（Redis 只有单一 keyspace，必须自行隔离）。
func TestNamespaceIndependence(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	const name = "ns:same"

	if err := db.Set(ctx, name, []byte("kv")); err != nil {
		t.Fatal(err)
	}
	if err := db.QPush(ctx, name, []byte("q")); err != nil {
		t.Fatal(err)
	}
	if err := db.ZSet(ctx, name, "m", 3); err != nil {
		t.Fatal(err)
	}

	if v, ok, err := db.Get(ctx, name); err != nil || !ok || string(v) != "kv" {
		t.Fatalf("同名下 KV 键被干扰: %q,%v,%v", v, ok, err)
	}
	if v, ok, err := db.QFront(ctx, name); err != nil || !ok || string(v) != "q" {
		t.Fatalf("同名下队列被干扰: %q,%v,%v", v, ok, err)
	}
	if s, ok, err := db.ZGet(ctx, name, "m"); err != nil || !ok || s != 3 {
		t.Fatalf("同名下 zset 被干扰: %d,%v,%v", s, ok, err)
	}
	// Scan 只应看到 KV 条目（不得把队列/zset 键当成 KV，也不得报 WRONGTYPE）。
	kvs, err := db.Scan(ctx, name, name, 10)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(kvs) != 1 || kvs[0].Key != name || string(kvs[0].Value) != "kv" {
		t.Fatalf("Scan 混入了非 KV 条目: %v", kvs)
	}

	// 删除 KV 键不影响另两个命名空间。
	if err := db.Del(ctx, name); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := db.Get(ctx, name); ok {
		t.Fatal("Del 后 KV 键仍存在")
	}
	if v, ok, _ := db.QFront(ctx, name); !ok || string(v) != "q" {
		t.Fatalf("Del KV 影响了队列: %q,%v", v, ok)
	}
	if _, ok, _ := db.ZGet(ctx, name, "m"); !ok {
		t.Fatal("Del KV 影响了 zset")
	}
}

// TestReadOwnership 覆盖返回值所有权：Get/MGet/Scan/QFront/QBack 返回的切片
// 必须是副本，调用方改写不得影响库内状态（否则 mem/jsonl 的内存态可被外部
// 篡改，且与 jsonl 重启回放结果不一致）。
func TestReadOwnership(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	const orig = "abc"

	if err := db.Set(ctx, "own", []byte(orig)); err != nil {
		t.Fatal(err)
	}
	v, ok, err := db.Get(ctx, "own")
	if err != nil || !ok {
		t.Fatalf("Get: %v %v", ok, err)
	}
	v[0] = 'x'
	if got, _, _ := db.Get(ctx, "own"); string(got) != orig {
		t.Fatalf("改写 Get 返回值污染了库内状态: %q", got)
	}

	m, err := db.MGet(ctx, "own")
	if err != nil {
		t.Fatal(err)
	}
	m["own"][0] = 'y'
	if got, _, _ := db.Get(ctx, "own"); string(got) != orig {
		t.Fatalf("改写 MGet 返回值污染了库内状态: %q", got)
	}

	kvs, err := db.Scan(ctx, "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range kvs {
		if kv.Key == "own" {
			kv.Value[0] = 'z'
		}
	}
	if got, _, _ := db.Get(ctx, "own"); string(got) != orig {
		t.Fatalf("改写 Scan 返回值污染了库内状态: %q", got)
	}

	if err := db.QPush(ctx, "ownq", []byte(orig)); err != nil {
		t.Fatal(err)
	}
	if f, ok, err := db.QFront(ctx, "ownq"); err != nil || !ok {
		t.Fatalf("QFront: %v %v", ok, err)
	} else {
		f[0] = 'x'
	}
	if b, ok, err := db.QBack(ctx, "ownq"); err != nil || !ok {
		t.Fatalf("QBack: %v %v", ok, err)
	} else {
		b[0] = 'x'
	}
	if f, _, _ := db.QFront(ctx, "ownq"); string(f) != orig {
		t.Fatalf("改写 QFront/QBack 返回值污染了库内状态: %q", f)
	}
}

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
	// **前 limit 个**（含 start 键本身）——回归 ssdb 基座此前"满页丢 start"的分歧。
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

func TestQueue(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	caps := db.Capabilities()
	hasQueue := caps.Queue
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
	if _, ok, err := db.QPop(ctx, "q1"); err != nil || ok {
		t.Fatalf("空队列 QPop 应 ok=false (err=%v)", err)
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

	// QRange：按位置只读读取（0 起闭区间，负索引从末尾数，越界裁剪）。
	// 语义与 ZRange 对称，且**不得改动队列**。
	rq := "qr"
	for _, v := range []string{"a", "b", "c", "d", "e"} {
		if err := db.QPush(ctx, rq, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	// 逐项断言，顺带确认方向是队头 → 队尾
	for _, tc := range []struct {
		name        string
		start, stop int64
		want        []string
	}{
		{"首三个", 0, 2, []string{"a", "b", "c"}},
		{"末尾两个(负索引)", -2, -1, []string{"d", "e"}},
		{"全部(0,-1)", 0, -1, []string{"a", "b", "c", "d", "e"}},
		{"单个首元素", 0, 0, []string{"a"}},
		{"单个末元素", -1, -1, []string{"e"}},
		{"stop 越界被裁剪", 0, 100, []string{"a", "b", "c", "d", "e"}},
		{"start 越界(负到超头)", -100, 1, []string{"a", "b"}},
		{"覆盖全长的负区间", -100, -1, []string{"a", "b", "c", "d", "e"}},
		{"空区间(start>stop)", 3, 1, nil},
		{"start 超出长度", 10, 20, nil},
	} {
		got, err := db.QRange(ctx, rq, tc.start, tc.stop)
		if err != nil {
			t.Fatalf("QRange(%s, %d, %d): %v", tc.name, tc.start, tc.stop, err)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("QRange(%s, %d, %d) = %q, want %q", tc.name, tc.start, tc.stop, got, tc.want)
		}
		for i := range got {
			if string(got[i]) != tc.want[i] {
				t.Fatalf("QRange(%s)[%d] = %q, want %q", tc.name, i, got[i], tc.want[i])
			}
		}
	}
	// QRange 是只读的：长度与两端都不应变化
	if n, _ := db.QSize(ctx, rq); n != 5 {
		t.Fatalf("QRange 后 QSize 变了: %d", n)
	}
	if v, ok, _ := db.QFront(ctx, rq); !ok || string(v) != "a" {
		t.Fatalf("QRange 后 QFront = %q,%v", v, ok)
	}
	if v, ok, _ := db.QBack(ctx, rq); !ok || string(v) != "e" {
		t.Fatalf("QRange 后 QBack = %q,%v", v, ok)
	}
	// 二进制值经 QRange 必须原样往返
	bin2 := []byte{0x00, 0xff, '\n', 0x80, '\r'}
	if err := db.QPush(ctx, "qrbin", bin2); err != nil {
		t.Fatal(err)
	}
	if got, err := db.QRange(ctx, "qrbin", 0, -1); err != nil || len(got) != 1 || string(got[0]) != string(bin2) {
		t.Fatalf("QRange 二进制往返 = %q,%v", got, err)
	}
	// 空队列：返回空且无错（而不是 ErrNotFound 之类）
	if got, err := db.QRange(ctx, "qr-empty", 0, -1); err != nil || len(got) != 0 {
		t.Fatalf("空队列 QRange 应为空且无错, got %q,%v", got, err)
	}
}

func TestZSet(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	hasZSet := db.Capabilities().ZSet
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
