// 白盒测试：直接访问 Provider 的日志文件以构造写入失败场景，
// 验证 WAL 顺序（日志先行）与批写原子性。
package jsonl

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
)

// TestWriteFailureKeepsMemoryConsistent 验证写日志失败时内存不变：
// 反序实现（先改内存后写日志）会在这里暴露不一致。
func TestWriteFailureKeepsMemoryConsistent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db.jsonl")
	p, err := Open(ctx, path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if err := p.Set(ctx, "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := p.QPush(ctx, "q", []byte("a")); err != nil {
		t.Fatal(err)
	}

	// 破坏日志：关闭底层文件，后续 append 必然失败。
	p.file.Close()

	if err := p.Set(ctx, "k", []byte("v2")); err == nil {
		t.Fatal("日志不可写时 Set 应返回错误")
	}
	if v, ok, _ := p.Get(ctx, "k"); !ok || string(v) != "v1" {
		t.Fatalf("写日志失败后内存应与日志一致（仍为 v1）, got %q,%v", v, ok)
	}
	if err := p.Del(ctx, "k"); err == nil {
		t.Fatal("日志不可写时 Del 应返回错误")
	}
	if ok, _ := p.Exists(ctx, "k"); !ok {
		t.Fatal("写日志失败后 Del 不应生效")
	}
	if err := p.QPush(ctx, "q", []byte("b")); err == nil {
		t.Fatal("日志不可写时 QPush 应返回错误")
	}
	if n, _ := p.QSize(ctx, "q"); n != 1 {
		t.Fatalf("写日志失败后队列长度应保持 1, got %d", n)
	}
}

// TestBatchCommitAndReplay 验证批写生效且可确定性回放。
func TestBatchCommitAndReplay(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db.jsonl")
	p, err := Open(ctx, path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	db := kvdb.Wrap(p)

	n := 0
	if err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("k1", []byte("v1"))
		b.SetEx("k2", []byte("v2"), 100)
		b.QPush("q", []byte("a"))
		b.QPushFront("q", []byte("z"))
		b.ZSet("z", "m", 7)
		b.ZIncr("z", "m", 3)
		b.Set("k3", []byte("v3"))
		b.Del("k3")
		n = b.Len()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 8 {
		t.Fatalf("收集操作数 = %d, want 8", n)
	}
	// 空批提交为空操作
	if err := db.Batch(ctx, func(b *kvdb.Batch) error { return nil }); err != nil {
		t.Fatalf("空批应无错: %v", err)
	}
	if v, ok, _ := db.Get(ctx, "k1"); !ok || string(v) != "v1" {
		t.Fatalf("k1 = %q,%v", v, ok)
	}
	if secs, has, _ := db.TTL(ctx, "k2"); !has || secs <= 0 || secs > 100 {
		t.Fatalf("k2 TTL = %d,%v", secs, has)
	}
	if ok, _ := db.Exists(ctx, "k3"); ok {
		t.Fatal("批内 Del 应生效")
	}
	if v, _, _ := db.QPop(ctx, "q"); string(v) != "z" {
		t.Fatalf("队头应为先插入的 z, got %q", v)
	}
	if v, _, _ := db.QPop(ctx, "q"); string(v) != "a" {
		t.Fatalf("队尾追加的 a 应在 z 之后, got %q", v)
	}
	if s, ok, _ := db.ZGet(ctx, "z", "m"); !ok || s != 10 {
		t.Fatalf("z m = %d,%v (7+3=10)", s, ok)
	}
	p.Close()

	// 回放后状态一致
	p2, err := Open(ctx, path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	db2 := kvdb.Wrap(p2)
	if v, ok, _ := db2.Get(ctx, "k1"); !ok || string(v) != "v1" {
		t.Fatalf("replay k1 = %q,%v", v, ok)
	}
	if secs, has, _ := db2.TTL(ctx, "k2"); !has || secs <= 0 || secs > 100 {
		t.Fatalf("replay k2 TTL = %d,%v", secs, has)
	}
	if s, ok, _ := db2.ZGet(ctx, "z", "m"); !ok || s != 10 {
		t.Fatalf("replay z m = %d,%v", s, ok)
	}
}

// TestBatchAtomicOnWriteFailure 验证提交失败时整批不生效（原子性）。
func TestBatchAtomicOnWriteFailure(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db.jsonl")
	p, err := Open(ctx, path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	db := kvdb.Wrap(p)
	if err := db.Set(ctx, "k", []byte("v0")); err != nil {
		t.Fatal(err)
	}

	p.file.Close() // 破坏日志
	if err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("k", []byte("v1"))
		b.Set("n", []byte("1"))
		b.QPush("q", []byte("a"))
		return nil
	}); err == nil {
		t.Fatal("日志不可写时 Batch 应返回错误")
	}
	// 整批不生效
	if v, ok, _ := db.Get(ctx, "k"); !ok || string(v) != "v0" {
		t.Fatalf("提交失败后 k 应保持 v0, got %q,%v", v, ok)
	}
	if ok, _ := db.Exists(ctx, "n"); ok {
		t.Fatal("提交失败后 n 不应存在")
	}
	if n, _ := db.QSize(ctx, "q"); n != 0 {
		t.Fatalf("提交失败后队列应为空, got %d", n)
	}
}

// TestBatchValidation 验证批内非法 TTL 在提交时报错且整批不生效。
func TestBatchValidation(t *testing.T) {
	ctx := context.Background()
	p, err := Open(ctx, filepath.Join(t.TempDir(), "db.jsonl"), Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	db := kvdb.Wrap(p)

	err = db.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("k", []byte("v"))
		b.SetEx("bad", []byte("v"), 0)
		return nil
	})
	if !errors.Is(err, core.ErrInvalidTTL) {
		t.Fatalf("非法 TTL 应 ErrInvalidTTL, got %v", err)
	}
	if ok, _ := db.Exists(ctx, "k"); ok {
		t.Fatal("批校验失败时整批不应生效")
	}
}

// TestBatchCallbackError 验证回调返回错误时不提交。
func TestBatchCallbackError(t *testing.T) {
	ctx := context.Background()
	p, err := Open(ctx, filepath.Join(t.TempDir(), "db.jsonl"), Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	db := kvdb.Wrap(p)

	if err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("a", []byte("1"))
		b.QPush("q", []byte("x"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := db.Get(ctx, "a"); !ok || string(v) != "1" {
		t.Fatalf("a = %q,%v", v, ok)
	}

	sentinel := errors.New("业务校验失败")
	if err := db.Batch(ctx, func(b *kvdb.Batch) error {
		b.Set("b", []byte("2"))
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("回调错误应原样返回, got %v", err)
	}
	if ok, _ := db.Exists(ctx, "b"); ok {
		t.Fatal("回调出错时不应提交")
	}
}

// TestIncrNonIntegerDoesNotPoisonLog 验证非法自增不会写入日志：
// 若写入，回放时会应用失败导致打开失败（毒化日志）。
func TestIncrNonIntegerDoesNotPoisonLog(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db.jsonl")
	p, err := Open(ctx, path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Set(ctx, "s", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Incr(ctx, "s", 1); !errors.Is(err, core.ErrNotInteger) {
		t.Fatalf("非整数自增应 ErrNotInteger, got %v", err)
	}
	p.Close()

	// 重新打开必须成功（日志未被非法记录毒化）
	p2, err := Open(ctx, path, Config{})
	if err != nil {
		t.Fatalf("回放失败（日志被毒化）: %v", err)
	}
	defer p2.Close()
	if _, err := p2.Incr(ctx, "s", 1); !errors.Is(err, core.ErrNotInteger) {
		t.Fatalf("回放后非整数自增仍应 ErrNotInteger, got %v", err)
	}
	if v, ok, _ := p2.Get(ctx, "s"); !ok || string(v) != "abc" {
		t.Fatalf("值不应被修改, got %q,%v", v, ok)
	}
}
