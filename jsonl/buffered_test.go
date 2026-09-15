package jsonl

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestBufferedIdleFlush 验证 Buffered 模式的核心承诺：**没有后续写入也会落盘**。
// 这是"接受延迟、不接受永远不落盘"的兜底：写入后不 Close、不再写，仅靠空闲
// 计时器把数据交给 OS，此时用另一个句柄应能读到记录。
func TestBufferedIdleFlush(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "idle.jsonl")
	p, err := Open(ctx, path, Config{Buffered: true, FlushInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	// 不 Close、不写第二次：等空闲落盘。给足余量（20ms 阈值 → 最多等 2s）。
	deadline := time.Now().Add(2 * time.Second)
	for {
		if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
			return // 已落盘
		}
		if time.Now().After(deadline) {
			t.Fatal("空闲落盘未发生：无后续写入时数据始终未交给 OS")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestBufferedCloseFlushes 验证 Buffered 模式 Close 后数据完整（不依赖计时器）。
func TestBufferedCloseFlushes(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "close.jsonl")
	p, err := Open(ctx, path, Config{Buffered: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := p.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2, err := Open(ctx, path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if v, ok, _ := p2.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("Close 后重开 Get = %q,%v", v, ok)
	}
}

// TestBufferedWriteFailureSurfaces 验证 Buffered 模式不吞写失败：
// 破坏日志后，失败要么在下一次写报出，要么在 Close 报出——不能静默成功到最后。
func TestBufferedWriteFailureSurfaces(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "fail.jsonl")
	p, err := Open(ctx, path, Config{Buffered: true, FlushInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Set(ctx, "k", []byte("v0")); err != nil {
		t.Fatal(err)
	}
	p.file.Close() // 破坏日志

	// 反复写直到报错（缓冲写满或空闲落盘都会暴露），最多 200 次。
	var got error
	for i := 0; i < 200 && got == nil; i++ {
		got = p.Set(ctx, "k", make([]byte, 1024))
		time.Sleep(5 * time.Millisecond) // 让空闲落盘也有机会触发
	}
	if got == nil {
		t.Fatal("日志不可写时 Buffered 模式始终未报错（写失败被吞）")
	}
}

// TestOpenURIBuffered 验证 buffered 参数解析，以及 sync=1 与 buffered=1 互斥。
func TestOpenURIBuffered(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	u, err := url.Parse("jsonl://" + filepath.Join(dir, "a.jsonl") + "?buffered=1")
	if err != nil {
		t.Fatal(err)
	}
	p, err := OpenURI(ctx, u)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.(*Provider).Close(); err != nil {
		t.Fatal(err)
	}

	u2, err := url.Parse("jsonl://" + filepath.Join(dir, "b.jsonl") + "?sync=1&buffered=1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenURI(ctx, u2); err == nil {
		t.Fatal("sync=1 与 buffered=1 同时设置应报错")
	}
}
