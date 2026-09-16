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

// 队列语义：先进先出、两端插入/弹出、按位置读、二进制值。

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
