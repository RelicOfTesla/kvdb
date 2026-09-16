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
	"math"
	"strings"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

// 计数器：非整数形态的统一拒绝与溢出（溢出方向由 Caps.IncrWraps 声明，用例断言该声明被兑现）。

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
