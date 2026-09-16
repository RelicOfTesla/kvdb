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
	"github.com/RelicOfTesla/kvdb/core"
)

// 有序集语义：分数排序、排名、按索引与按分数区间读取。

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

	// ---- ZRangeByScore：按分数闭区间取成员，desc 只改方向 ----
	// 单独用一个集合，并**故意造同分**（b 与 f 同为 2），才能钉住"同分按成员升序、
	// 且 desc 时该次序不翻转"这条容易写错的规则。
	for _, m := range []struct {
		key   string
		score int64
	}{{"a", 1}, {"b", 2}, {"f", 2}, {"c", 3}, {"d", 4}, {"e", 5}} {
		if err := db.ZSet(ctx, "rbs", m.key, m.score); err != nil {
			t.Fatal(err)
		}
	}
	keys := func(items []core.ZItem) []string {
		out := make([]string, len(items))
		for i, it := range items {
			out[i] = it.Key
		}
		return out
	}
	eq := func(got []string, want []string) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}
	for _, tc := range []struct {
		name     string
		min, max int64
		limit    int
		desc     bool
		want     []string
	}{
		{"全区间升序", 0, 10, 0, false, []string{"a", "b", "f", "c", "d", "e"}},
		{"全区间降序", 0, 10, 0, true, []string{"e", "d", "c", "b", "f", "a"}},
		{"区间[2,4]升序", 2, 4, 0, false, []string{"b", "f", "c", "d"}},
		{"区间[2,4]降序", 2, 4, 0, true, []string{"d", "c", "b", "f"}},
		{"limit=2 升序取最低端", 0, 10, 2, false, []string{"a", "b"}},
		{"limit=2 降序取最高端", 0, 10, 2, true, []string{"e", "d"}},
		{"区间内 limit=2 降序", 2, 4, 2, true, []string{"d", "c"}},
		{"单点 min==max", 3, 3, 0, false, []string{"c"}},
		{"空区间 min>max", 5, 1, 0, false, nil},
		{"无匹配", 100, 200, 0, false, nil},
		{"空区间降序", 5, 1, 0, true, nil},
	} {
		items, err := db.ZRangeByScore(ctx, "rbs", tc.min, tc.max, tc.limit, tc.desc)
		if err != nil {
			t.Fatalf("ZRangeByScore(%s): %v", tc.name, err)
		}
		if got := keys(items); !eq(got, tc.want) {
			t.Fatalf("ZRangeByScore(%s, min=%d max=%d limit=%d desc=%v) = %v, want %v",
				tc.name, tc.min, tc.max, tc.limit, tc.desc, got, tc.want)
		}
		// 分数必须落在闭区间内（顺带验证边界是闭的）
		for _, it := range items {
			if it.Score < tc.min || it.Score > tc.max {
				t.Fatalf("ZRangeByScore(%s) 返回越界分数 %s=%d", tc.name, it.Key, it.Score)
			}
		}
	}
	// 不存在的集合：空且无错
	if items, err := db.ZRangeByScore(ctx, "rbs-absent", 0, 10, 0, false); err != nil || len(items) != 0 {
		t.Fatalf("不存在的集合应为空且无错, got %v,%v", items, err)
	}
	// 只读：不应改动集合
	if n, _ := db.ZSize(ctx, "rbs"); n != 6 {
		t.Fatalf("ZRangeByScore 后 ZSize = %d, want 6", n)
	}
}

// TestBatch 覆盖批量写契约：一次提交内混合 KV/Queue/ZSet 操作，

// TestZSetRangeBoundaries 覆盖 ZSet 区间边界与 ZRangeByScore 的闭区间/同分/limit 规则。
func TestZSetRangeBoundaries(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	if !db.Capabilities().ZSet {
		t.Skip("基座未实现 ZSet 能力")
	}

	// a=1 b=2 f=2 c=3 d=4 e=5 —— 故意让 b/f 同分，以钉住同分规则
	for _, m := range []struct {
		k string
		s int64
	}{{"a", 1}, {"b", 2}, {"f", 2}, {"c", 3}, {"d", 4}, {"e", 5}} {
		if err := db.ZSet(ctx, "zrb", m.k, m.s); err != nil {
			t.Fatal(err)
		}
	}

	// ZRange 索引边界：负索引、越界、空区间
	for _, tc := range []struct {
		name        string
		start, stop int64
		want        []string
	}{
		{"全部", 0, -1, []string{"a", "b", "f", "c", "d", "e"}},
		{"前二", 0, 1, []string{"a", "b"}},
		{"后二", -2, -1, []string{"d", "e"}},
		{"负索引超头被裁剪", -100, 1, []string{"a", "b"}},
		{"stop 超尾被裁剪", 4, 100, []string{"d", "e"}},
		{"start>stop 为空", 3, 1, nil},
		{"start 超长度为空", 10, 20, nil},
		{"单元素", 0, 0, []string{"a"}},
	} {
		got, err := db.ZRange(ctx, "zrb", tc.start, tc.stop)
		if err != nil {
			t.Fatalf("ZRange(%s): %v", tc.name, err)
		}
		// 用长度 + 逐项比较：接口返回 nil 还是空切片不在契约内（不可区分），
		// 因此不能用 DeepEqual 区分二者。
		if gk := zkeysOf(got); len(gk) != len(tc.want) {
			t.Fatalf("ZRange(%s, %d, %d) = %v, want %v", tc.name, tc.start, tc.stop, gk, tc.want)
		} else {
			for i := range gk {
				if gk[i] != tc.want[i] {
					t.Fatalf("ZRange(%s, %d, %d) = %v, want %v", tc.name, tc.start, tc.stop, gk, tc.want)
				}
			}
		}
	}

	// ZRangeByScore：闭区间（恰好等于 min/max 必须包含）、同分、limit 方向
	for _, tc := range []struct {
		name     string
		min, max int64
		limit    int
		desc     bool
		want     []string
	}{
		{"全区间升序", 0, 10, 0, false, []string{"a", "b", "f", "c", "d", "e"}},
		{"全区间降序", 0, 10, 0, true, []string{"e", "d", "c", "b", "f", "a"}},
		{"闭下界恰好命中", 2, 10, 0, false, []string{"b", "f", "c", "d", "e"}},
		{"闭上界恰好命中", 0, 2, 0, false, []string{"a", "b", "f"}},
		{"min==max 命中同分对", 2, 2, 0, false, []string{"b", "f"}},
		{"min==max 降序同分仍升序", 2, 2, 0, true, []string{"b", "f"}},
		{"空区间 min>max", 5, 1, 0, false, nil},
		{"无匹配", 100, 200, 0, false, nil},
		{"limit=全部条数", 0, 10, 6, false, []string{"a", "b", "f", "c", "d", "e"}},
		{"limit=条数-1", 0, 10, 5, false, []string{"a", "b", "f", "c", "d"}},
		{"降序 limit 取最高端", 0, 10, 2, true, []string{"e", "d"}},
		{"升序 limit 取最低端", 0, 10, 2, false, []string{"a", "b"}},
		{"区间内降序 limit", 2, 4, 2, true, []string{"d", "c"}},
	} {
		got, err := db.ZRangeByScore(ctx, "zrb", tc.min, tc.max, tc.limit, tc.desc)
		if err != nil {
			t.Fatalf("ZRangeByScore(%s): %v", tc.name, err)
		}
		if gk := zkeysOf(got); len(gk) != len(tc.want) {
			t.Fatalf("ZRangeByScore(%s, min=%d max=%d limit=%d desc=%v) = %v, want %v",
				tc.name, tc.min, tc.max, tc.limit, tc.desc, gk, tc.want)
		} else {
			for i := range gk {
				if gk[i] != tc.want[i] {
					t.Fatalf("ZRangeByScore(%s, min=%d max=%d limit=%d desc=%v) = %v, want %v",
						tc.name, tc.min, tc.max, tc.limit, tc.desc, gk, tc.want)
				}
			}
		}
		// 分数必须落在闭区间内
		for _, it := range got {
			if it.Score < tc.min || it.Score > tc.max {
				t.Fatalf("ZRangeByScore(%s) 返回越界分数 %s=%d", tc.name, it.Key, it.Score)
			}
		}
	}

	// ZRank 边界：首个、末个、不存在
	if r, ok, err := db.ZRank(ctx, "zrb", "a"); err != nil || !ok || r != 0 {
		t.Fatalf("ZRank(首个) = %d,%v,%v, want 0", r, ok, err)
	}
	if r, ok, err := db.ZRank(ctx, "zrb", "e"); err != nil || !ok || r != 5 {
		t.Fatalf("ZRank(末个) = %d,%v,%v, want 5", r, ok, err)
	}
	if r, ok, err := db.ZRank(ctx, "zrb", "nope"); err != nil || ok || r != 0 {
		t.Fatalf("ZRank(不存在) 应 ok=false, got %d,%v,%v", r, ok, err)
	}

	// ZIncr 边界：负增量、缺失成员按 0 起算（不是从 delta 起算）
	if s, err := db.ZIncr(ctx, "zrb", "a", -1); err != nil || s != 0 {
		t.Fatalf("ZIncr(-1 于 1) 应得 0, got %d,%v", s, err)
	}
	if s, err := db.ZIncr(ctx, "zrb", "new", -7); err != nil || s != -7 {
		t.Fatalf("ZIncr(缺失, -7) 应得 -7（从 0 起算）, got %d,%v", s, err)
	}
	// 不存在的 zset 上做区间查询：空且无错
	if got, err := db.ZRangeByScore(ctx, "zrb-absent", 0, 10, 0, false); err != nil || len(got) != 0 {
		t.Fatalf("不存在的 zset 应空且无错, got %v,%v", got, err)
	}
	if got, err := db.ZRange(ctx, "zrb-absent", 0, -1); err != nil || len(got) != 0 {
		t.Fatalf("不存在的 zset ZRange 应空且无错, got %v,%v", got, err)
	}
}

// zkeysOf 取 ZSet 结果的成员名序列，便于整体比较（保序）。
// 与 keysOf 一样只比较长度与逐项内容：接口返回 nil 还是空切片不在契约内。
func zkeysOf(items []core.ZItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.Key
	}
	return out
}
