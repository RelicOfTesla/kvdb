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
	"testing"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

// 生命周期与参数合法性：Close 语义、空名字、零长值。

// TestCloseSemantics 覆盖 Close 后的可观察行为：
//
//   - 关闭后每一个操作都必须以 core.ErrClosed 失败（errors.Is 判定），
//     不得静默成功、不得 panic、也不得返回别的错误形态；
//   - 重复 Close 必须幂等（不 panic，且与首次 Close 同样返回 nil）。
//
// 关闭检查在各基座实现内（无共享包装层），因此这里逐方法钉住，
// 任何"漏检某一族方法"的基座都会在此暴露。
func TestCloseSemantics(t *testing.T, db kvdb.DB) {
	caps := db.Capabilities()

	// 先写入数据，确保关闭后各读方法走的不是"键不存在"的短路分支：
	// 若某基座的读路径漏检 closed，就会读到真实值而掩盖分歧。
	ctx := context.Background()
	if err := db.Set(ctx, "cs:k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.SetEx(ctx, "cs:ttl", []byte("v"), 100); err != nil {
		t.Fatal(err)
	}
	if caps.Queue {
		if err := db.QPush(ctx, "cs:q", []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if caps.ZSet {
		if err := db.ZSet(ctx, "cs:z", "m", 1); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("首次 Close = %v, want nil", err)
	}
	// 重复 Close：契约要求幂等（各基座用 sync.Once / atomic Swap 实现）。
	if err := db.Close(); err != nil {
		t.Fatalf("重复 Close = %v, want nil（Close 必须幂等）", err)
	}

	// 关闭后每个操作都必须 ErrClosed。用表驱动把"方法名 → 调用"列全，
	// 遗漏的方法会以失败消息里的方法名直接指出来。
	type op struct {
		name string
		call func() error
	}
	ops := []op{
		{"Set", func() error { return db.Set(ctx, "cs:k2", []byte("v")) }},
		{"Get", func() error { _, _, err := db.Get(ctx, "cs:k"); return err }},
		{"Del", func() error { return db.Del(ctx, "cs:k") }},
		{"Exists", func() error { _, err := db.Exists(ctx, "cs:k"); return err }},
		{"Incr", func() error { _, err := db.Incr(ctx, "cs:k", 1); return err }},
		{"MGet", func() error { _, err := db.MGet(ctx, "cs:k"); return err }},
		{"Scan", func() error { _, err := db.Scan(ctx, "", "", 10); return err }},
		{"Expire", func() error { return db.Expire(ctx, "cs:ttl", 50) }},
		{"TTL", func() error { _, _, err := db.TTL(ctx, "cs:ttl"); return err }},
	}
	if caps.Queue {
		ops = append(ops,
			op{"QPush", func() error { return db.QPush(ctx, "cs:q", []byte("v")) }},
			op{"QPop", func() error { _, _, err := db.QPop(ctx, "cs:q"); return err }},
			op{"QSize", func() error { _, err := db.QSize(ctx, "cs:q"); return err }},
			op{"QRange", func() error { _, err := db.QRange(ctx, "cs:q", 0, -1); return err }},
		)
	}
	if caps.ZSet {
		ops = append(ops,
			op{"ZSet", func() error { return db.ZSet(ctx, "cs:z", "m", 2) }},
			op{"ZGet", func() error { _, _, err := db.ZGet(ctx, "cs:z", "m"); return err }},
			op{"ZRange", func() error { _, err := db.ZRange(ctx, "cs:z", 0, -1); return err }},
			op{"ZSize", func() error { _, err := db.ZSize(ctx, "cs:z"); return err }},
		)
	}
	if caps.Batch {
		ops = append(ops, op{"Batch", func() error {
			return db.Batch(ctx, func(b *kvdb.Batch) error {
				b.Set("cs:k3", []byte("v"))
				return nil
			})
		}})
	}

	// 循环若干轮：Close 后的错误必须是**稳定**的 ErrClosed，不是"第一次才报"。
	for round := 0; round < 3; round++ {
		for _, o := range ops {
			err := o.call()
			if !errors.Is(err, core.ErrClosed) {
				t.Fatalf("第 %d 轮 Close 后 %s: err = %v, want core.ErrClosed", round+1, o.name, err)
			}
		}
	}

	// 关闭后的读还必须是"无副作用"的：若某基座在读路径上误删/误写，
	// 这里通过重启后的可见性无法观察，故直接确认读返回的 ok 为 false。
	if v, ok, err := db.Get(ctx, "cs:k"); ok || v != nil || !errors.Is(err, core.ErrClosed) {
		t.Fatalf("Close 后 Get = %q,%v,%v; want nil,false,ErrClosed", v, ok, err)
	}
}

// TestWriteOwnership 覆盖**写入侧**的所有权：调用方把自己持有的切片交给
// 基座后可以立即复用它（改写内容），库内值不得随之变化。读取侧的副本语义
// 已由 TestReadOwnership 覆盖，这里补的是反向（set 时必须拷贝）。
//
// 这是经典分歧源：内存型基座若不拷贝就会与"日志回放/落盘后再读"的结果不一致；
// Batch 的收集器（kvdb.Batch）尤其危险——它把调用方的切片直接存进 BatchOp，

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
// 且报告里会明确列出每个基座落在哪一侧。
func TestEmptyName(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	caps := db.Capabilities()

	// 契约（core.ErrInvalidKey）把空 key/空名字定义为**非法**，且要求
	// **读写一致拒绝**。此前契约沉默：ssdb 只在写路径拒绝（真实 SSDB 对空 key
	// 返回 ok 却静默丢弃写入），bolt 由 bbolt 的 ErrKeyRequired 顺带拒绝，
	// 其余基座两种都接受——于是出现"写不进去却读得到"这类自相矛盾。
	// 现在统一在适配层校验，这里逐个断言，任何基座漏掉都会立刻暴露。
	wantErr := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, core.ErrInvalidKey) {
			t.Fatalf("%s 对空 key 应返回 core.ErrInvalidKey, got %v", what, err)
		}
	}

	// --- KV：读写路径都必须拒绝 ---
	wantErr("Set", db.Set(ctx, "", []byte("v")))
	if _, _, err := db.Get(ctx, ""); true {
		wantErr("Get", err)
	}
	wantErr("Del", db.Del(ctx, ""))
	if _, err := db.Exists(ctx, ""); true {
		wantErr("Exists", err)
	}
	if _, err := db.Incr(ctx, "", 1); true {
		wantErr("Incr", err)
	}
	wantErr("SetEx", db.SetEx(ctx, "", []byte("v"), 10))
	wantErr("SetExAt", db.SetExAt(ctx, "", []byte("v"), core.NowUnix()+10))
	wantErr("Expire", db.Expire(ctx, "", 10))
	wantErr("ExpireAt", db.ExpireAt(ctx, "", core.NowUnix()+10))
	if _, _, err := db.TTL(ctx, ""); true {
		wantErr("TTL", err)
	}

	// 空 key 被拒绝**不得**影响正常键：确认基座仍然可用。
	if err := db.Set(ctx, "en:ok", []byte("v")); err != nil {
		t.Fatalf("空 key 校验不应影响正常键: %v", err)
	}
	if v, ok, err := db.Get(ctx, "en:ok"); err != nil || !ok || string(v) != "v" {
		t.Fatalf("正常键往返失败: %q,%v,%v", v, ok, err)
	}

	// --- 队列：空队列名 ---
	if caps.Queue {
		wantErr("QPush", db.QPush(ctx, "", []byte("v")))
		wantErr("QPushFront", db.QPushFront(ctx, "", []byte("v")))
		if _, _, err := db.QPop(ctx, ""); true {
			wantErr("QPop", err)
		}
		if _, _, err := db.QPopBack(ctx, ""); true {
			wantErr("QPopBack", err)
		}
		if _, err := db.QSize(ctx, ""); true {
			wantErr("QSize", err)
		}
		if _, _, err := db.QFront(ctx, ""); true {
			wantErr("QFront", err)
		}
		if _, _, err := db.QBack(ctx, ""); true {
			wantErr("QBack", err)
		}
		if _, err := db.QRange(ctx, "", 0, -1); true {
			wantErr("QRange", err)
		}
	}

	// --- ZSet：空 zset 名 与 空成员 ---
	if caps.ZSet {
		wantErr("ZSet(空名)", db.ZSet(ctx, "", "m", 1))
		wantErr("ZSet(空成员)", db.ZSet(ctx, "en:z", "", 1))
		if _, _, err := db.ZGet(ctx, "", "m"); true {
			wantErr("ZGet(空名)", err)
		}
		if _, _, err := db.ZGet(ctx, "en:z", ""); true {
			wantErr("ZGet(空成员)", err)
		}
		wantErr("ZDel(空名)", db.ZDel(ctx, "", "m"))
		wantErr("ZDel(空成员)", db.ZDel(ctx, "en:z", ""))
		if _, err := db.ZSize(ctx, ""); true {
			wantErr("ZSize", err)
		}
		if _, _, err := db.ZRank(ctx, "", "m"); true {
			wantErr("ZRank(空名)", err)
		}
		if _, _, err := db.ZRank(ctx, "en:z", ""); true {
			wantErr("ZRank(空成员)", err)
		}
		if _, err := db.ZRange(ctx, "", 0, -1); true {
			wantErr("ZRange", err)
		}
		if _, err := db.ZRangeByScore(ctx, "", 0, 10, 0, false); true {
			wantErr("ZRangeByScore", err)
		}
		if _, err := db.ZIncr(ctx, "", "m", 1); true {
			wantErr("ZIncr(空名)", err)
		}
		if _, err := db.ZIncr(ctx, "en:z", "", 1); true {
			wantErr("ZIncr(空成员)", err)
		}
	}

	// Scan 的空串是**合法**的：表示该侧不限（见 core/provider.go 的 Scan 契约），
	// 因此绝不能被上面的空 key 校验误伤。
	if _, err := db.Scan(ctx, "", "", 10); err != nil {
		t.Fatalf("Scan 的空串表示「不限」，不应报 ErrInvalidKey: %v", err)
	}
}

// TestMGetScanQRangeOwnership 补足 TestReadOwnership 未覆盖的读取侧所有权：
// MGet 结果的值、Scan 结果的 Value、QRange 结果的每个元素都必须是对副本的
// 引用，改写它们不得污染库内状态。
//
// TestReadOwnership 已覆盖 Get / MGet 的 map 项 / Scan 的 Value / QFront/QBack，
// 但**没有**覆盖 QRange 的元素；且它改写 MGet/Scan 值后只复核了同一个 key，
// 这里额外验证"改写后经另一个读接口观察仍是原值"，避免某基座在读路径上有

// TestEmptyValue 覆盖零长值的往返：`[]byte{}`（非 nil 零长）与 `[]byte(nil)`
// 都必须写入成功、读回 ok=true，且 len 为 0。契约不区分二者（Get 只承诺
// value+ok，不承诺 nil 与非 nil 零长的差别），因此这里只钉"都能往返且长度一致"。
func TestEmptyValue(t *testing.T, db kvdb.DB) {
	ctx := context.Background()
	caps := db.Capabilities()

	for _, tc := range []struct {
		name string
		val  []byte
	}{
		{"非nil零长", []byte{}},
		{"nil", []byte(nil)},
	} {
		key := "ev:" + tc.name
		if err := db.Set(ctx, key, tc.val); err != nil {
			t.Fatalf("Set(%s) 零长值应成功: %v", tc.name, err)
		}
		v, ok, err := db.Get(ctx, key)
		if err != nil || !ok {
			t.Fatalf("Get(%s) 零长值应 ok=true: ok=%v err=%v", tc.name, ok, err)
		}
		if len(v) != 0 {
			t.Fatalf("Get(%s) 零长值读回 len=%d, want 0", tc.name, len(v))
		}
		// 零长值不等于"键不存在"：Exists 必须为真。
		if ex, err := db.Exists(ctx, key); err != nil || !ex {
			t.Fatalf("Exists(%s) 零长值应存在: ex=%v err=%v", tc.name, ex, err)
		}
		// MGet 也必须把它当作存在的键返回。
		m, err := db.MGet(ctx, key)
		if err != nil {
			t.Fatalf("MGet(%s): %v", tc.name, err)
		}
		if _, ok := m[key]; !ok {
			t.Fatalf("MGet(%s) 零长值应出现在结果里: %v", tc.name, m)
		}
		if n := len(m[key]); n != 0 {
			t.Fatalf("MGet(%s) 零长值 len=%d, want 0", tc.name, n)
		}
	}

	// 零长值覆盖非零长值：Set 必须真的把值改成零长（而非忽略空值不写）。
	if err := db.Set(ctx, "ev:over", []byte("full")); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(ctx, "ev:over", []byte{}); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := db.Get(ctx, "ev:over"); err != nil || !ok || len(v) != 0 {
		t.Fatalf("零长值覆盖非零长值后 Get = %q ok=%v err=%v, want len=0 ok=true", v, ok, err)
	}

	if caps.Queue {
		// 队列里的零长元素必须与"队列为空"区分开：QFront ok=true 且 len=0，
		// 而 QPop 取出的也是零长（不是 ok=false）。
		if err := db.QPush(ctx, "ev:q", []byte{}); err != nil {
			t.Fatalf("QPush 零长值应成功: %v", err)
		}
		if n, err := db.QSize(ctx, "ev:q"); err != nil || n != 1 {
			t.Fatalf("零长元素入队后 QSize = %d,%v, want 1", n, err)
		}
		v, ok, err := db.QFront(ctx, "ev:q")
		if err != nil || !ok || len(v) != 0 {
			t.Fatalf("零长元素 QFront = %q ok=%v err=%v, want len=0 ok=true", v, ok, err)
		}
		got, err := db.QRange(ctx, "ev:q", 0, -1)
		if err != nil || len(got) != 1 || len(got[0]) != 0 {
			t.Fatalf("零长元素 QRange = %v (len=%d), err=%v; want 1 个零长元素", got, len(got), err)
		}
		if v, ok, err := db.QPop(ctx, "ev:q"); err != nil || !ok || len(v) != 0 {
			t.Fatalf("零长元素 QPop = %q ok=%v err=%v, want len=0 ok=true", v, ok, err)
		}
		// 取出后队列应为空：空队列的 QPop 才是 ok=false。
		if v, ok, err := db.QPop(ctx, "ev:q"); err != nil || ok || v != nil {
			t.Fatalf("空队列 QPop = %q,%v,%v; want nil,false,nil", v, ok, err)
		}
	}
}

// TestNamespacePrefixKeys 覆盖"键字面量看起来像内部命名空间前缀"的情形：
// 一个 KV 键就叫 "kv:foo"（或含 NUL、含队列/zset 的内部标记）时，不得与
// 真正的队列/zset 命名空间互相串扰。Redis/SSDB 基座在内部拼接前缀把三类
// 数据装进同一个 keyspace，若拼接方式不够隔离（例如把用户键直接当物理键），
