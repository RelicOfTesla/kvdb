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

// 命名空间隔离：KV/Queue/ZSet 互不干扰，含形如内部分隔符的 key。

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

// TestNamespacePrefixKeys 覆盖"键字面量看起来像内部命名空间前缀"的情形：
// 一个 KV 键就叫 "kv:foo"（或含 NUL、含队列/zset 的内部标记）时，不得与
// 真正的队列/zset 命名空间互相串扰。Redis/SSDB 基座在内部拼接前缀把三类
// 数据装进同一个 keyspace，若拼接方式不够隔离（例如把用户键直接当物理键），
// 同名或"像前缀"的键就会撞车。
func TestNamespacePrefixKeys(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	caps := db.Capabilities()

	// 这些键故意长得像各基座可能使用的内部前缀（kv:/q:/z: 与 redis 默认
	// 前缀 kvdb:），以及含 NUL 的二进制键——NUL 是 C 系与部分协议的截断点。
	keys := []string{
		"kv:foo",
		"q:foo",
		"z:foo",
		"kvdb:kv:foo",
		"kvdb:q:foo",
		"k\x00v",
		"kv:\x00foo",
	}

	for i, k := range keys {
		want := []byte{byte('0' + i)}
		if err := db.Set(ctx, k, want); err != nil {
			t.Fatalf("Set(%q) 应成功: %v", k, err)
		}
		if v, ok, err := db.Get(ctx, k); err != nil || !ok || string(v) != string(want) {
			t.Fatalf("Get(%q) = %q ok=%v err=%v; want %q", k, v, ok, err, want)
		}
	}

	if caps.Queue {
		// 同一个字面量同时作为 KV 键与队列名：两者必须各自独立
		//（TestNamespaceIndependence 已覆盖普通名，这里用"像前缀"的名）。
		const shared = "kv:q:foo"
		if err := db.Set(ctx, shared, []byte("kv")); err != nil {
			t.Fatal(err)
		}
		if err := db.QPush(ctx, shared, []byte("q")); err != nil {
			t.Fatal(err)
		}
		if v, ok, err := db.Get(ctx, shared); err != nil || !ok || string(v) != "kv" {
			t.Fatalf("像前缀的 KV 键被队列干扰: %q ok=%v err=%v", v, ok, err)
		}
		if v, ok, err := db.QFront(ctx, shared); err != nil || !ok || string(v) != "q" {
			t.Fatalf("像前缀的队列被 KV 干扰: %q ok=%v err=%v", v, ok, err)
		}
		// 队列名不得被 Scan 当作 KV 键扫出来。
		kvs, err := db.Scan(ctx, shared, shared, 10)
		if err != nil {
			t.Fatalf("Scan(%q): %v", shared, err)
		}
		if len(kvs) != 1 || kvs[0].Key != shared || string(kvs[0].Value) != "kv" {
			t.Fatalf("Scan 把队列名当成了 KV 条目: %+v", kvs)
		}
	}

	if caps.ZSet {
		const shared = "z:shared"
		if err := db.Set(ctx, shared, []byte("kv")); err != nil {
			t.Fatal(err)
		}
		if err := db.ZSet(ctx, shared, "m", 9); err != nil {
			t.Fatal(err)
		}
		if v, ok, err := db.Get(ctx, shared); err != nil || !ok || string(v) != "kv" {
			t.Fatalf("像前缀的 KV 键被 zset 干扰: %q ok=%v err=%v", v, ok, err)
		}
		if s, ok, err := db.ZGet(ctx, shared, "m"); err != nil || !ok || s != 9 {
			t.Fatalf("像前缀的 zset 被 KV 干扰: %d ok=%v err=%v", s, ok, err)
		}
		// zset 的**成员名**与 KV 键同处一个命名空间边界时也不得串扰：
		// 成员名等于某个 KV 键名，两者互不影响。
		if err := db.Set(ctx, "z:member", []byte("kvval")); err != nil {
			t.Fatal(err)
		}
		if err := db.ZSet(ctx, shared, "z:member", 4); err != nil {
			t.Fatal(err)
		}
		if v, ok, err := db.Get(ctx, "z:member"); err != nil || !ok || string(v) != "kvval" {
			t.Fatalf("zset 成员写入影响了同名 KV 键: %q ok=%v err=%v", v, ok, err)
		}
	}

	// Del 一个"像前缀"的键不得连带删除其它命名空间。
	if caps.Queue {
		const shared = "kv:q:foo"
		if err := db.Del(ctx, shared); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := db.Get(ctx, shared); ok {
			t.Fatal("Del 后 KV 键仍存在")
		}
		if v, ok, _ := db.QFront(ctx, shared); !ok || string(v) != "q" {
			t.Fatalf("Del 像前缀的 KV 键影响了同名队列: %q,%v", v, ok)
		}
	}
}

// TestEmptyName 覆盖空串作为 key / 队列名 / zset 名的行为。
//
// 契约对空串**未作规定**（core.KvProvider 的注释只约定空串在 Scan 中表示
// "该侧不限"）。既未规定，本用例的判据是**跨基座一致**：要么所有基座都接受
// 空串，要么所有基座都以同一种方式拒绝。空串是各协议的高危边界——
// SSDB 在协议层（link.cpp）不接受空键，会显式拒绝——因此这里把每个基座的
// 观察结果原样报出来，任何不一致都是真实分歧（而不是"某基座更严格所以更好"）。
//
// 实现方式：先探测本基座对空键的取舍，再把"接受"分支与"拒绝"分支各自的
// 完整行为钉住。这样本用例在两种约定下都能通过，但**同一基座内部必须自洽**，
