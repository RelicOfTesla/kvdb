package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/RelicOfTesla/kvdb"
)

// newTestCLI 建一个连着 mem 基座的 cli，输出收集到 buffer 便于断言。
// testCLI 把 cli 与它的输出收集器绑在一起，并保证"断言前输出已读完"。
//
// cli.out 是 *os.File（写的是真实路径，而不是另开一个 io.Writer 分支），
// 所以用 os.Pipe 收集：读取端在 goroutine 里排空，finish() 负责关写入端并
// 等读取端结束——**不等就会读到半截**（这正是先前 PING 断言偶发失败的成因：
// 测试在读取 goroutine 把 PONG 搬进 buffer 之前就断言了）。
type testCLI struct {
	*cli
	buf  *bytes.Buffer
	pw   *os.File
	pr   *os.File
	done chan struct{}
}

func newTestCLI(t *testing.T) *testCLI {
	t.Helper()
	db, err := kvdb.Open(context.Background(), "mem://")
	if err != nil {
		t.Fatalf("Open mem://: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	tc := &testCLI{
		cli:  &cli{db: db, timeout: 5e9, out: pw},
		buf:  &bytes.Buffer{},
		pw:   pw,
		pr:   pr,
		done: make(chan struct{}),
	}
	go func() {
		_, _ = tc.buf.ReadFrom(pr)
		close(tc.done)
	}()
	t.Cleanup(func() { tc.finish() })
	return tc
}

// finish 关闭写入端并等待读取端排空，之后 buf 内容才完整。
// 可重复调用（用 select 判断是否已关闭）。
func (tc *testCLI) finish() {
	select {
	case <-tc.done:
		return // 已结束
	default:
	}
	tc.pw.Close() // 触发读取端 EOF
	<-tc.done
	tc.pr.Close()
}

// out 返回已排空的输出内容（内部先 finish）。
func (tc *testCLI) out() string {
	tc.finish()
	return tc.buf.String()
}

// run 执行一条命令行并返回退出码。
func run(t *testing.T, c *cli, line string) int {
	t.Helper()
	args, err := splitArgs(line)
	if err != nil {
		t.Fatalf("splitArgs(%q): %v", line, err)
	}
	return c.exec(context.Background(), args)
}

// TestSplitArgs 覆盖引号、转义、空参数与错误形态。
// 这是 CLI 唯一"自造"的逻辑，必须钉牢：切错了会把值写坏。
func TestSplitArgs(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{`SET k v`, []string{"SET", "k", "v"}},
		{`SET   k    v`, []string{"SET", "k", "v"}},          // 连续空白
		{"SET k ''", []string{"SET", "k", ""}},               // 空参数
		{`SET k ""`, []string{"SET", "k", ""}},               // 双引号空参数
		{`SET "a b" v`, []string{"SET", "a b", "v"}},         // 引号内含空格
		{`SET 'a b' v`, []string{"SET", "a b", "v"}},         // 单引号
		{`SET k "a\nb"`, []string{"SET", "k", "a\nb"}},       // 双引号内转义
		{`SET k a\nb`, []string{"SET", "k", "a\nb"}},         // 引号外转义
		{`SET k 'a\nb'`, []string{"SET", "k", `a\nb`}},       // 单引号内**不**转义
		{`SET k \x00\xff`, []string{"SET", "k", "\x00\xff"}}, // 二进制值
		{`SET k "a\"b"`, []string{"SET", "k", `a"b`}},        // 转义引号
		{`SET k \\`, []string{"SET", "k", `\`}},              // 转义反斜杠
	} {
		got, err := splitArgs(tc.in)
		if err != nil {
			t.Fatalf("splitArgs(%q) 报错: %v", tc.in, err)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("splitArgs(%q) = %q, want %q", tc.in, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("splitArgs(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
	// 错误形态必须报错，而不是静默吞掉剩余内容
	for _, bad := range []string{`SET "unclosed`, `SET k \`, `SET k \xZZ`, `SET k "a\x1"`} {
		if _, err := splitArgs(bad); err == nil {
			t.Fatalf("splitArgs(%q) 应报错", bad)
		}
	}
}

// TestCLIKeyValue 覆盖 KV 命令的往返与输出形态。
func TestCLIKeyValue(t *testing.T) {
	tc := newTestCLI(t)

	if code := run(t, tc.cli, `SET k v`); code != 0 {
		t.Fatalf("SET 退出码 = %d", code)
	}
	if code := run(t, tc.cli, `GET k`); code != 0 {
		t.Fatalf("GET 退出码 = %d", code)
	}
	// 缺失 key 应是 (nil) 而非错误
	if code := run(t, tc.cli, `GET missing`); code != 0 {
		t.Fatalf("GET 缺失退出码 = %d（不应视为错误）", code)
	}
	if code := run(t, tc.cli, `EXISTS k`); code != 0 {
		t.Fatal("EXISTS 失败")
	}
	if code := run(t, tc.cli, `INCR n`); code != 0 {
		t.Fatal("INCR 失败")
	}
	if code := run(t, tc.cli, `INCR n 4`); code != 0 {
		t.Fatal("INCR 带增量失败")
	}
	if code := run(t, tc.cli, `DEL k`); code != 0 {
		t.Fatal("DEL 失败")
	}
	if code := run(t, tc.cli, `GET k`); code != 0 {
		t.Fatal("DEL 后 GET 失败")
	}
	// 参数不足必须报错（退出码 1），而不是 panic 或静默成功
	if code := run(t, tc.cli, `SET onlykey`); code != 1 {
		t.Fatalf("参数不足应退出码 1, got %d", code)
	}
	if code := run(t, tc.cli, `INCR n notanumber`); code != 1 {
		t.Fatalf("非法整数应退出码 1, got %d", code)
	}
	if code := run(t, tc.cli, `NOSUCHCMD`); code != 1 {
		t.Fatalf("未知命令应退出码 1, got %d", code)
	}

	// 等 pipe 读完再断言内容
	got := tc.out()
	for _, want := range []string{"OK", `"v"`, "(nil)", "1", "5"} {
		if !strings.Contains(got, want) {
			t.Fatalf("输出缺少 %q:\n%s", want, got)
		}
	}
}

// TestCLITTL 覆盖相对与绝对 TTL 两条路径（SETEX / SETEXAT / EXPIRE / TTL）。
func TestCLITTL(t *testing.T) {
	tc := newTestCLI(t)

	if code := run(t, tc.cli, `SETEX k 100 v`); code != 0 {
		t.Fatal("SETEX 失败")
	}
	if code := run(t, tc.cli, `TTL k`); code != 0 {
		t.Fatal("TTL 失败")
	}
	// 无 TTL 的 key：输出 (no ttl) 而不是报错
	if code := run(t, tc.cli, `SET plain v`); code != 0 {
		t.Fatal("SET 失败")
	}
	if code := run(t, tc.cli, `TTL plain`); code != 0 {
		t.Fatal("无 TTL 的 TTL 不应报错")
	}
	// SETEXAT 传过去时刻 -> 等价于删除
	if code := run(t, tc.cli, `SETEXAT gone 1 x`); code != 0 {
		t.Fatal("SETEXAT 失败")
	}
	got := tc.out()
	if !strings.Contains(got, "(no ttl)") {
		t.Fatalf("应输出 (no ttl):\n%s", got)
	}
	// gone 应已被删除（等同 SETEXAT 过去时间点的删除语义）
	if _, ok, _ := tc.cli.db.Get(context.Background(), "gone"); ok {
		t.Fatal("SETEXAT 过去时刻应删除 key")
	}
}

// TestCLIQueueAndZSet 覆盖队列与有序集的命令分派（含空队列的 (nil) 输出）。
func TestCLIQueueAndZSet(t *testing.T) {
	tc := newTestCLI(t)

	for _, line := range []string{
		`QPUSH q a`, `QPUSHFRONT q z`, `QSIZE q`, `QFRONT q`, `QBACK q`,
		`QPOP q`, `QPOPBACK q`, `QPOP q`, // 最后一次应得 (nil)
		`ZSET rank alice 90`, `ZINCR rank alice 5`, `ZGET rank alice`,
		`ZSIZE rank`, `ZRANK rank alice`, `ZRANGE rank 0 -1`, `ZDEL rank alice`,
	} {
		if code := run(t, tc.cli, line); code != 0 {
			t.Fatalf("%q 退出码 = %d", line, code)
		}
	}
	got := tc.out()
	// ZINCR 90+5=95；队列顺序 z,a（QFRONT 先看 z，QBACK 看 a）
	for _, want := range []string{"95", "(nil)", "z", "alice"} {
		if !strings.Contains(got, want) {
			t.Fatalf("输出缺少 %q:\n%s", want, got)
		}
	}
}

// TestCLIQRange: 队列按位置读取（只读），覆盖默认全量、区间与空队列。
func TestCLIQRange(t *testing.T) {
	tc := newTestCLI(t)
	for _, line := range []string{
		`QPUSH q a`, `QPUSH q b`, `QPUSH q c`, `QPUSH q d`, `QPUSH q e`,
		`QRANGE q`,          // 默认 0..-1 = 全部
		`QRANGE q 0 2`,      // 前三个
		`QRANGE q -2 -1`,    // 后两个
		`QRANGE q 3 1`,      // 空区间
		`QRANGE empty 0 -1`, // 不存在的队列 -> (empty)
	} {
		if code := run(t, tc.cli, line); code != 0 {
			t.Fatalf("%q 退出码 = %d", line, code)
		}
	}
	// 只读校验：QRange 不应改动队列。注意 out() 会关闭管道并排空输出，
	// 所以这条命令必须在取输出**之前**发。
	if code := run(t, tc.cli, `QSIZE q`); code != 0 {
		t.Fatal("QSIZE 失败")
	}
	got := tc.out()
	for _, want := range []string{`"a"`, `"b"`, `"c"`, `"d"`, `"e"`, "(empty)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("输出缺少 %q:\n%s", want, got)
		}
	}
	// QSIZE 的输出里应含 5：队列条数未变，证明 QRange 是只读的
	if !strings.Contains(got, "5") {
		t.Fatalf("QRANGE 后队列应仍为 5 条; 实际输出:\n%s", got)
	}
}

// TestCLINonPrintableIsHex 验证非可打印值以十六进制输出（不把二进制塞进终端）。
func TestCLINonPrintableIsHex(t *testing.T) {
	tc := newTestCLI(t)
	if code := run(t, tc.cli, `SET bin \x00\xff`); code != 0 {
		t.Fatal("SET 二进制值失败")
	}
	if code := run(t, tc.cli, `GET bin`); code != 0 {
		t.Fatal("GET 二进制值失败")
	}
	if got := tc.out(); !strings.Contains(got, "00ff") {
		t.Fatalf("非可打印值应以十六进制输出:\n%s", got)
	}
}

// TestCLIHelpAndCaps 覆盖不碰数据的两条命令与能力查询。
func TestCLIHelpAndCaps(t *testing.T) {
	tc := newTestCLI(t)
	for _, line := range []string{`HELP`, `CAPS`, `PING`} {
		if code := run(t, tc.cli, line); code != 0 {
			t.Fatalf("%q 退出码 = %d", line, code)
		}
	}
	got := tc.out()
	if !strings.Contains(got, "PONG") {
		t.Fatalf("PING 应输出 PONG:\n%s", got)
	}
	if !strings.Contains(got, "queue=") {
		t.Fatalf("CAPS 应输出能力字段:\n%s", got)
	}
}
