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
	"fmt"
	"reflect"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

// 范围查询：闭区间两端、越界、空串=该侧不限、**字节序**排序、limit 边界。

// TestScanBoundaries 覆盖 Scan 的区间语义与排序：闭区间两端、越界、空串=不限、
// **字节序**（而非大小写不敏感/本地化排序）、二进制 key、前缀 key。
func TestScanBoundaries(t *testing.T, db kvdb.DB) {
	ctx := context.Background()

	// 空区间：start>end 必须为空且无错。
	// 注意**不能**假设库是空的：部分基座（ssdb/rpc）的测试框架在多个子用例间
	// 共用同一实例，别处写入的键会留在这里。因此只断言"区间为空"这一与库内容
	// 无关的性质，并用一个专用前缀做后续断言，避免被既有键干扰。
	if got, err := db.Scan(ctx, "zz-inv", "zz-inv0", 0); err != nil || len(got) != 0 {
		t.Fatalf("start>end 应为空且无错, got %v,%v", keysOf(got), err)
	}

	// 字节序基准集：覆盖大小写混排、数字、符号，能抓出"大小写不敏感"或
	// "本地化排序"的实现（SQL 基座若用默认 collation 就会在此暴露）。
	seq := []string{"0", "9", "A", "Z", "_", "a", "b", "~"}
	for _, k := range seq {
		if err := db.Set(ctx, "sb:"+k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	all, err := db.Scan(ctx, "sb:", "sb:~", 1000)
	if err != nil {
		t.Fatal(err)
	}
	got := keysOf(all)
	wantSeq := make([]string, len(seq))
	for i, k := range seq {
		wantSeq[i] = "sb:" + k
	}
	if !reflect.DeepEqual(got, wantSeq) {
		t.Fatalf("Scan 应为字节序 %v, got %v（大小写不敏感或本地化排序会在此失败）", wantSeq, got)
	}

	// 闭区间：两端都必须包含。用 b..d 取 b,c,d——半开实现会漏掉一端。
	for _, k := range []string{"b", "c", "d", "e"} {
		if err := db.Set(ctx, "ci:"+k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	ci, err := db.Scan(ctx, "ci:b", "ci:d", 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(ci); !equalStrs(got, []string{"ci:b", "ci:c", "ci:d"}) {
		t.Fatalf("闭区间 Scan(ci:b,ci:d) = %v, want [ci:b ci:c ci:d]（两端都必须含）", got)
	}

	// start == end：命中恰好一个（存在时）
	if got, _ := db.Scan(ctx, "ci:b", "ci:b", 100); !equalStrs(keysOf(got), []string{"ci:b"}) {
		t.Fatalf("start==end 应只返回该键, got %v", keysOf(got))
	}
	// start > end：空
	if got, err := db.Scan(ctx, "ci:d", "ci:b", 100); err != nil || len(got) != 0 {
		t.Fatalf("start>end 应为空且无错, got %v,%v", keysOf(got), err)
	}
	// 空串表示该侧**不限**——注意是"不限"而非"到前缀末尾"，因此 end="" 会
	// 一直扫到库中最后一个 key（含其它前缀）。这里显式钉住这一语义。
	got3, err := db.Scan(ctx, "ci:c", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got3) < 3 || got3[0].Key != "ci:c" {
		t.Fatalf("start 给定、end 为空串应从 ci:c 起且不限上界, got %v", keysOf(got3))
	}
	for i := 1; i < len(got3); i++ {
		if got3[i-1].Key > got3[i].Key {
			t.Fatalf("end 不限时仍须升序, got %v", keysOf(got3))
		}
	}
	// start 为空串 = 从最前开始；用带前缀的 end 限定范围
	got4, err := db.Scan(ctx, "", "ci:c", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got4) == 0 || got4[len(got4)-1].Key != "ci:c" {
		t.Fatalf("end 给定、start 为空串应以 ci:c 结尾, got %v", keysOf(got4))
	}

	// 前缀 key 的字节序："pk" < "pk1" < "pk10" < "pk2"（不是数值序）。
	// 注意上界：字节序下 '9' > '1'，所以 "pk9" 反而**大于** "pk10"，
	// 用 "pk9" 作 end 会漏掉 pk10。这里用足够的字节序上界（"pk:"→"pl"）。
	for _, k := range []string{"pk", "pk1", "pk10", "pk2"} {
		if err := db.Set(ctx, k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	pk, err := db.Scan(ctx, "pk", "pl", 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(pk); !equalStrs(got, []string{"pk", "pk1", "pk10", "pk2"}) {
		t.Fatalf("前缀 key 应字节序 [pk pk1 pk10 pk2], got %v（数值序实现会在此失败）", got)
	}
	// 上界按**字节序**而非数值序生效：'1' < '9'，故 "pk10" < "pk9"，
	// 于是 [pk, pk9] 仍包含 pk10（若实现按数值序比较，"pk10"=10 > 9 就会被排除）。
	// 这一条正是用区分二者的关键断言。
	upTo9, err := db.Scan(ctx, "pk", "pk9", 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(upTo9); !equalStrs(got, []string{"pk", "pk1", "pk10", "pk2"}) {
		t.Fatalf("字节序下 'pk10' < 'pk9'，[pk,pk9] 应含 pk10；got %v（数值序实现会漏掉 pk10）", got)
	}

	// 二进制/特殊 key：NUL、换行、0xff 都必须能存能取且参与排序
	binKeys := []string{"bk:\x00", "bk:\n", "bk:\xff"}
	for _, k := range binKeys {
		if err := db.Set(ctx, k, []byte("v")); err != nil {
			t.Fatalf("写入二进制 key %q: %v", k, err)
		}
	}
	bk, err := db.Scan(ctx, "bk:", "bk:\xff", 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(bk); !equalStrs(got, binKeys) {
		t.Fatalf("二进制 key 应字节序 %q, got %q", binKeys, got)
	}
	for _, k := range binKeys {
		if _, ok, err := db.Get(ctx, k); err != nil || !ok {
			t.Fatalf("二进制 key %q 应可读, ok=%v err=%v", k, ok, err)
		}
	}

	// limit 边界：恰好 N 条时 limit=N 取满、limit=N-1 少一条且仍是**升序前 N-1 条**
	lim := []string{}
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("lm:%d", i)
		lim = append(lim, k)
		if err := db.Set(ctx, k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	full, err := db.Scan(ctx, "lm:", "lm:~", 5)
	if err != nil || len(full) != 5 {
		t.Fatalf("limit==N 应取满 5 条, got %d,%v", len(full), err)
	}
	less, err := db.Scan(ctx, "lm:", "lm:~", 4)
	if err != nil {
		t.Fatal(err)
	}
	if got := keysOf(less); !equalStrs(got, lim[:4]) {
		t.Fatalf("limit=N-1 应返回**升序前 4 条** %v, got %v", lim[:4], got)
	}
	// limit<=0 按 DefaultScanLimit：与显式传该值等价（不返回全部）
	def, err1 := db.Scan(ctx, "lm:", "lm:~", 0)
	explicit, err2 := db.Scan(ctx, "lm:", "lm:~", core.DefaultScanLimit)
	if err1 != nil || err2 != nil {
		t.Fatalf("Scan: %v %v", err1, err2)
	}
	if !equalStrs(keysOf(def), keysOf(explicit)) {
		t.Fatalf("limit<=0 应等价于 DefaultScanLimit(%d): %v vs %v",
			core.DefaultScanLimit, keysOf(def), keysOf(explicit))
	}
}

// TestTTLBoundaries 覆盖 TTL 的边界：无 TTL/不存在的区分、非正 TTL 拒绝、

// keysOf 把 Scan 结果取成 key 序列，便于整体比较（保序）。
func keysOf(kvs []core.KeyValue) []string {
	out := make([]string, len(kvs))
	for i, kv := range kvs {
		out[i] = kv.Key
	}
	return out
}

// equalStrs 只比较长度与逐项内容：nil 与空切片在契约上不可区分，不使用

// equalStrs 只比较长度与逐项内容：nil 与空切片在契约上不可区分，不使用
// reflect.DeepEqual 以免把"返回空切片而非 nil"误判为不一致。
func equalStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
