package kvdbtest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

// 本文件覆盖**区间与边界**语义：闭/开区间、越界裁剪、字节序排序、limit 边界、
// TTL 边界、Incr 解析与溢出边界、ZSet 区间边界。
//
// 判定原则：除 `core.Caps` **显式声明**的差异外，一律断言各基座行为一致。
// 注释不是声明载体——凡注释里提过的分歧，都必须能在 Caps 里查到，否则视为 bug。

// keysOf 把 Scan 结果取成 key 序列，便于整体比较（保序）。
func keysOf(kvs []core.KeyValue) []string {
	out := make([]string, len(kvs))
	for i, kv := range kvs {
		out[i] = kv.Key
	}
	return out
}

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

// zkeysOf 同 keysOf，用于 ZSet 结果。
func zkeysOf(items []core.ZItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.Key
	}
	return out
}

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
// **声明本身是否被兑现**——若某基座 Caps 说谎，用例必须失败。
func TestIncrBoundaries(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	caps := db.Capabilities()

	// 缺失键按 0 起算（delta=0 也创建）
	if n, err := db.Incr(ctx, "ib:zero", 0); err != nil || n != 0 {
		t.Fatalf("Incr(missing, 0) 应返回 0, got %d,%v", n, err)
	}
	if v, ok, _ := db.Get(ctx, "ib:zero"); !ok || string(v) != "0" {
		t.Fatalf("Incr 应把缺失键写成 \"0\", got %q,%v", v, ok)
	}
	// 常规累加与负增量
	if n, err := db.Incr(ctx, "ib:n", 5); err != nil || n != 5 {
		t.Fatalf("Incr 5 = %d,%v", n, err)
	}
	if n, err := db.Incr(ctx, "ib:n", -3); err != nil || n != 2 {
		t.Fatalf("Incr -3 = %d,%v", n, err)
	}

	// 非整数形态必须统一拒绝。这些是各基座解析器最易分歧的输入。
	// 注意：Go 的 ParseInt 接受前导 "+"，因此 "+1" 是**合法**整数，不在此列
	//（下面单独断言它可解析）。"" 也不在此列：空值由各基座按"非整数"处理，
	// 但空字节串只可能来自显式 Set(k, []byte{})，语义上属"非整数值"，保留在列内。
	bad := []string{"abc", "", " 1", "1 ", "0x10", "1.5", "1e3", "١٢٣", "1\n", "--1", "1-"}
	for _, val := range bad {
		key := "ib:bad:" + strings.ReplaceAll(val, "\n", `\n`)
		if err := db.Set(ctx, key, []byte(val)); err != nil {
			t.Fatalf("预热 Set(%q): %v", val, err)
		}
		if _, err := db.Incr(ctx, key, 1); !errors.Is(err, core.ErrNotInteger) {
			t.Fatalf("Incr 对非整数值 %q 应 ErrNotInteger, got %v", val, err)
		}
	}
	// 明确合法的整数形态（含前导零与负号）应可解析
	for _, tc := range []struct {
		val  string
		want int64
	}{{"0", 1}, {"007", 8}, {"-3", -2}, {"+1", 2}} {
		key := "ib:ok:" + tc.val
		if err := db.Set(ctx, key, []byte(tc.val)); err != nil {
			t.Fatal(err)
		}
		if n, err := db.Incr(ctx, key, 1); err != nil || n != tc.want {
			t.Fatalf("Incr(%q,+1) = %d,%v, want %d", tc.val, n, err, tc.want)
		}
	}

	// 溢出：唯一由 Caps 显式声明的差异
	if err := db.Set(ctx, "ib:ovf", []byte(fmt.Sprint(int64(math.MaxInt64)))); err != nil {
		t.Fatal(err)
	}
	n, err := db.Incr(ctx, "ib:ovf", 1)
	if caps.IncrWraps {
		if err != nil {
			t.Fatalf("Caps.IncrWraps=true 声称回绕，但 Incr 溢出报错: %v（声明与行为不符）", err)
		}
		if n != math.MinInt64 {
			t.Fatalf("Caps.IncrWraps=true 声称回绕到 MinInt64, got %d", n)
		}
	} else {
		// 契约（core/provider.go 的 Incr 说明）只承诺"溢出**返回错误**"，
		// **不承诺**具体哨兵：SQL 基座映射为 ErrNotInteger，Redis 则把服务端的
		// "increment or decrement would overflow" 原样包装后返回。两者都符合
		// Caps.IncrWraps=false 的声明，因此这里只断言"确实报错、且没有回绕"。
		if err == nil {
			t.Fatalf("Caps.IncrWraps=false 声称溢出报错，但返回了 n=%d, err=nil（声明与行为不符）", n)
		}
		if n == math.MinInt64 {
			t.Fatalf("Caps.IncrWraps=false 却回绕成了 MinInt64（声明与行为不符）")
		}
	}
}

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
