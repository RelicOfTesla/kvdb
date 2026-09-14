package jsonl_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/internal/behaviortest"
	"github.com/RelicOfTesla/kvdb/jsonl"
)

// factory 每个用例使用独立临时文件。
func factory(t *testing.T) core.KvProvider {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db.jsonl")
	p, err := jsonl.Open(context.Background(), path, jsonl.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestBehavior(t *testing.T) {
	behaviortest.Run(t, factory)
}

// TestReopen 验证 WAL 回放恢复 KV/Queue/ZSet/Incr/TTL 状态。
func TestReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.jsonl")

	ctx := context.Background()
	p, err := jsonl.Open(ctx, path, jsonl.Config{})
	if err != nil {
		t.Fatal(err)
	}
	bin := []byte{0xff, 0x00, '\n', 0x80}
	if err := p.Set(ctx, "k", bin); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Incr(ctx, "cnt", 7); err != nil {
		t.Fatal(err)
	}
	if err := p.Expire(ctx, "k", 100); err != nil {
		t.Fatal(err)
	}
	if err := p.QPush(ctx, "q", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := p.QPush(ctx, "q", []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := p.ZSet(ctx, "z", "m1", 5); err != nil {
		t.Fatal(err)
	}
	p.Close()

	p2, err := jsonl.Open(ctx, path, jsonl.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if v, ok, _ := p2.Get(ctx, "k"); !ok || string(v) != string(bin) {
		t.Fatalf("reopen Get = %q,%v", v, ok)
	}
	// TTL 为绝对时间戳回放：剩余 < 100 且 > 0
	if secs, has, _ := p2.TTL(ctx, "k"); !has || secs <= 0 || secs > 100 {
		t.Fatalf("reopen TTL = %d,%v", secs, has)
	}
	if n, _ := p2.Incr(ctx, "cnt", 0); n != 7 {
		t.Fatalf("reopen Incr = %d", n)
	}
	if v, ok, _ := p2.QPop(ctx, "q"); !ok || string(v) != "a" {
		t.Fatalf("reopen QPop = %q,%v", v, ok)
	}
	if s, ok, _ := p2.ZGet(ctx, "z", "m1"); !ok || s != 5 {
		t.Fatalf("reopen ZGet = %d,%v", s, ok)
	}
}

// TestSetExReopen 验证 setx 记录以绝对时间戳回放：重开前后 TTL 有效性一致
// （不因停机时长缩短有效期）。
func TestSetExReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.jsonl")
	ctx := context.Background()

	p, err := jsonl.Open(ctx, path, jsonl.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SetEx(ctx, "s", []byte("v1"), 100); err != nil {
		t.Fatal(err)
	}
	p.Close()

	p2, err := jsonl.Open(ctx, path, jsonl.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if v, ok, _ := p2.Get(ctx, "s"); !ok || string(v) != "v1" {
		t.Fatalf("reopen Get = %q,%v", v, ok)
	}
	if secs, has, _ := p2.TTL(ctx, "s"); !has || secs <= 0 || secs > 100 {
		t.Fatalf("reopen TTL = %d,%v", secs, has)
	}
}

// TestCrashTailTolerance 验证崩溃残留的半行不阻断启动，且之前的状态完整。
func TestCrashTailTolerance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.jsonl")
	ctx := context.Background()

	p, err := jsonl.Open(ctx, path, jsonl.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Set(ctx, "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	p.Close()

	// 追加一行损坏的半行（模拟写一半断电）
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"op":"set","k":"broken","v":"unterminated`)
	f.Close()

	p2, err := jsonl.Open(ctx, path, jsonl.Config{})
	if err != nil {
		t.Fatalf("崩溃尾部应可容忍: %v", err)
	}
	defer p2.Close()
	if v, ok, _ := p2.Get(ctx, "k"); !ok || string(v) != "v1" {
		t.Fatalf("Get = %q,%v", v, ok)
	}
}

// TestCompact 验证压缩后状态一致且日志变小；TTL 以绝对戳保留。
func TestCompact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.jsonl")
	ctx := context.Background()

	p, err := jsonl.Open(ctx, path, jsonl.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := p.Set(ctx, "k"+string(rune('0'+i%10)), []byte("v")); err != nil {
			t.Fatal(err)
		}
		if err := p.Del(ctx, "k"+string(rune('0'+i%10))); err != nil {
			t.Fatal(err)
		}
	}
	p.Set(ctx, "keep", []byte("x"))
	p.Expire(ctx, "keep", 100)
	p.QPush(ctx, "qq", []byte("a"))
	p.QPush(ctx, "qq", []byte("b"))
	p.ZSet(ctx, "zz", "m", 9)
	p.Close()
	before, _ := os.Stat(path)

	p2, err := jsonl.Open(ctx, path, jsonl.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p2.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	p2.Close()
	after, _ := os.Stat(path)
	if after.Size() >= before.Size() {
		t.Fatalf("Compact 后日志应缩小: before=%d after=%d", before.Size(), after.Size())
	}

	p3, err := jsonl.Open(ctx, path, jsonl.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p3.Close()
	if v, ok, _ := p3.Get(ctx, "keep"); !ok || string(v) != "x" {
		t.Fatalf("compact Get = %q,%v", v, ok)
	}
	if secs, has, _ := p3.TTL(ctx, "keep"); !has || secs <= 0 || secs > 100 {
		t.Fatalf("compact TTL = %d,%v", secs, has)
	}
	if n, _ := p3.QSize(ctx, "qq"); n != 2 {
		t.Fatalf("compact QSize = %d", n)
	}
	if v, ok, _ := p3.QPop(ctx, "qq"); !ok || string(v) != "a" {
		t.Fatalf("compact QPop = %q,%v", v, ok)
	}
	if v, ok, _ := p3.QPop(ctx, "qq"); !ok || string(v) != "b" {
		t.Fatalf("compact QPop2 = %q,%v", v, ok)
	}
	if s, ok, _ := p3.ZGet(ctx, "zz", "m"); !ok || s != 9 {
		t.Fatalf("compact ZGet = %d,%v", s, ok)
	}
}
