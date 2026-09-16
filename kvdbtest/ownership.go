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
	"testing"

	"github.com/RelicOfTesla/kvdb"
)

// 所有权（别名）语义：读返回值必须是副本；写入必须拷贝入参。

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

// TestMGetScanQRangeOwnership 补足 TestReadOwnership 未覆盖的读取侧所有权：
// MGet 结果的值、Scan 结果的 Value、QRange 结果的每个元素都必须是对副本的
// 引用，改写它们不得污染库内状态。
//
// TestReadOwnership 已覆盖 Get / MGet 的 map 项 / Scan 的 Value / QFront/QBack，
// 但**没有**覆盖 QRange 的元素；且它改写 MGet/Scan 值后只复核了同一个 key，
// 这里额外验证"改写后经另一个读接口观察仍是原值"，避免某基座在读路径上有
// 缓存（缓存被改写后，另一接口可能读到脏值而同一接口读回原值）。
func TestMGetScanQRangeOwnership(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	caps := db.Capabilities()
	const orig = "abc"
	const mutated = "ZZZ"

	if err := db.Set(ctx, "om:k", []byte(orig)); err != nil {
		t.Fatal(err)
	}

	// 1. MGet：改写返回的字节切片，再从 Get / MGet 两个方向复核。
	m, err := db.MGet(ctx, "om:k")
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 1 {
		t.Fatalf("MGet 结果 = %v, want 1 项", m)
	}
	copy(m["om:k"], mutated)
	if v, _, _ := db.Get(ctx, "om:k"); string(v) != orig {
		t.Fatalf("改写 MGet 返回值后 Get = %q, want %q", v, orig)
	}
	m2, err := db.MGet(ctx, "om:k")
	if err != nil {
		t.Fatal(err)
	}
	if string(m2["om:k"]) != orig {
		t.Fatalf("改写 MGet 返回值后再 MGet = %q, want %q", m2["om:k"], orig)
	}

	// 2. Scan：改写返回的 Value，再从 Scan / Get 复核。
	kvs, err := db.Scan(ctx, "om:", "om:~", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 || kvs[0].Key != "om:k" {
		t.Fatalf("Scan 结果 = %+v, want 仅 om:k", kvs)
	}
	copy(kvs[0].Value, mutated)
	if v, _, _ := db.Get(ctx, "om:k"); string(v) != orig {
		t.Fatalf("改写 Scan 返回的 Value 后 Get = %q, want %q", v, orig)
	}
	kvs2, err := db.Scan(ctx, "om:", "om:~", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs2) != 1 || string(kvs2[0].Value) != orig {
		t.Fatalf("改写 Scan 返回的 Value 后再 Scan = %+v, want value=%q", kvs2, orig)
	}

	if caps.Queue {
		// 3. QRange：改写每个元素的字节切片，再从 QRange / QFront 复核。
		for _, v := range []string{orig, "def"} {
			if err := db.QPush(ctx, "om:q", []byte(v)); err != nil {
				t.Fatal(err)
			}
		}
		items, err := db.QRange(ctx, "om:q", 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) != 2 {
			t.Fatalf("QRange = %q, want 2 项", items)
		}
		for _, it := range items {
			copy(it, mutated)
		}
		if v, _, _ := db.QFront(ctx, "om:q"); string(v) != orig {
			t.Fatalf("改写 QRange 返回值后 QFront = %q, want %q", v, orig)
		}
		items2, err := db.QRange(ctx, "om:q", 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		if len(items2) != 2 || string(items2[0]) != orig || string(items2[1]) != "def" {
			t.Fatalf("改写 QRange 返回值后再 QRange = %q, want [%q def]", items2, orig)
		}
		// QRange 的多次调用之间也不得共享底层数组：改写第一次的结果
		// 不应影响第二次取到的切片。
		if &items2[0][0] == &items[0][0] {
			t.Fatal("QRange 两次调用返回了同一底层数组（调用方两次读取会互相污染）")
		}
	}
}

// TestWriteOwnership 覆盖**写入侧**的所有权：调用方把自己持有的切片交给
// 基座后可以立即复用它（改写内容），库内值不得随之变化。读取侧的副本语义
// 已由 TestReadOwnership 覆盖，这里补的是反向（set 时必须拷贝）。
//
// 这是经典分歧源：内存型基座若不拷贝就会与"日志回放/落盘后再读"的结果不一致；
// Batch 的收集器（kvdb.Batch）尤其危险——它把调用方的切片直接存进 BatchOp，
// 由基座在提交时决定是否拷贝。
func TestWriteOwnership(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	caps := db.Capabilities()
	const orig = "abc"
	const mutated = "XYZ"

	// 1. Set：写入后改写调用方切片。
	buf := []byte(orig)
	if err := db.Set(ctx, "wo:k", buf); err != nil {
		t.Fatal(err)
	}
	copy(buf, mutated)
	if v, ok, err := db.Get(ctx, "wo:k"); err != nil || !ok || string(v) != orig {
		t.Fatalf("Set 后改写调用方切片污染了库内值: got %q, want %q (ok=%v err=%v)", v, orig, ok, err)
	}

	// 2. SetEx：同上（带 TTL 的写路径可能走另一条分支）。
	bufEx := []byte(orig)
	if err := db.SetEx(ctx, "wo:ex", bufEx, 100); err != nil {
		t.Fatal(err)
	}
	copy(bufEx, mutated)
	if v, ok, err := db.Get(ctx, "wo:ex"); err != nil || !ok || string(v) != orig {
		t.Fatalf("SetEx 后改写调用方切片污染了库内值: got %q, want %q (ok=%v err=%v)", v, orig, ok, err)
	}

	if caps.Queue {
		// 3. QPush：写入后改写调用方切片。
		bufQ := []byte(orig)
		if err := db.QPush(ctx, "wo:q", bufQ); err != nil {
			t.Fatal(err)
		}
		copy(bufQ, mutated)
		if v, ok, err := db.QFront(ctx, "wo:q"); err != nil || !ok || string(v) != orig {
			t.Fatalf("QPush 后改写调用方切片污染了库内值: got %q, want %q (ok=%v err=%v)", v, orig, ok, err)
		}

		// 4. QPushFront：队头插入同样必须拷贝。
		bufQF := []byte(orig)
		if err := db.QPushFront(ctx, "wo:qf", bufQF); err != nil {
			t.Fatal(err)
		}
		copy(bufQF, mutated)
		if v, ok, err := db.QFront(ctx, "wo:qf"); err != nil || !ok || string(v) != orig {
			t.Fatalf("QPushFront 后改写调用方切片污染了库内值: got %q, want %q (ok=%v err=%v)", v, orig, ok, err)
		}
	}

	if caps.Batch {
		// 5. Batch 的 b.Set：收集器保存的是切片引用。提交发生在回调返回之后，
		// 因此这里在**回调内写入、回调返回前的最后一次改写**才是真正的考验——
		// 但更贴近真实用法、且契约必须成立的是：Batch 返回后改写调用方切片
		// 不得影响已提交的值（提交已完成，值必须已固化）。
		bufB := []byte(orig)
		if err := db.Batch(ctx, func(b *kvdb.Batch) error {
			b.Set("wo:batch", bufB)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		copy(bufB, mutated)
		if v, ok, err := db.Get(ctx, "wo:batch"); err != nil || !ok || string(v) != orig {
			t.Fatalf("Batch(b.Set) 返回后改写调用方切片污染了库内值: got %q, want %q (ok=%v err=%v)", v, orig, ok, err)
		}

		// 6. Batch 的 b.SetEx / b.QPush 同理。
		bufBE := []byte(orig)
		if err := db.Batch(ctx, func(b *kvdb.Batch) error {
			b.SetEx("wo:batchex", bufBE, 100)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		copy(bufBE, mutated)
		if v, ok, err := db.Get(ctx, "wo:batchex"); err != nil || !ok || string(v) != orig {
			t.Fatalf("Batch(b.SetEx) 返回后改写调用方切片污染了库内值: got %q, want %q (ok=%v err=%v)", v, orig, ok, err)
		}

		if caps.Queue {
			bufBQ := []byte(orig)
			if err := db.Batch(ctx, func(b *kvdb.Batch) error {
				b.QPush("wo:batchq", bufBQ)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			copy(bufBQ, mutated)
			if v, ok, err := db.QFront(ctx, "wo:batchq"); err != nil || !ok || string(v) != orig {
				t.Fatalf("Batch(b.QPush) 返回后改写调用方切片污染了库内值: got %q, want %q (ok=%v err=%v)", v, orig, ok, err)
			}
		}
	}

	// 7. 同一份调用方缓冲区被连续写入两个 key：两者必须各自独立留存，
	//    不得共享同一底层数组（否则后写覆盖前写）。
	// 缓冲区按较长值分配，第一次写入只交出前 5 字节（"first"），
	// 复用同一底层数组改写为等长 6 字节（"second"）后再次写入。
	shared := []byte("second")
	copy(shared, "first")
	if err := db.Set(ctx, "wo:s1", shared[:5]); err != nil {
		t.Fatal(err)
	}
	copy(shared, "second")
	if err := db.Set(ctx, "wo:s2", shared); err != nil {
		t.Fatal(err)
	}
	if v1, _, _ := db.Get(ctx, "wo:s1"); string(v1) != "first" {
		t.Fatalf("复用同一缓冲区写入两个 key 后 s1 被覆盖: %q, want %q", v1, "first")
	}
	if v2, _, _ := db.Get(ctx, "wo:s2"); string(v2) != "second" {
		t.Fatalf("复用同一缓冲区写入两个 key 后 s2 = %q, want %q", v2, "second")
	}
}

// TestEmptyValue 覆盖零长值的往返：`[]byte{}`（非 nil 零长）与 `[]byte(nil)`
// 都必须写入成功、读回 ok=true，且 len 为 0。契约不区分二者（Get 只承诺
