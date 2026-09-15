package bolt_test

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/RelicOfTesla/kvdb/bolt"
	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/kvdbtest"
)

// TestBehavior 跑跨基座共享合同用例（KV / Queue / ZSet / Batch，
// 含并发 Incr 原子性与多 key 并发）。
func TestBehavior(t *testing.T) {
	kvdbtest.RunWithOptions(t, kvdbtest.VirtualClock(t), func(t *testing.T) core.KvProvider {
		p, err := bolt.Open(context.Background(), filepath.Join(t.TempDir(), "db.bolt"), bolt.Config{})
		if err != nil {
			t.Fatal(err)
		}
		return p
	})
}

// TestFilePersistence 验证关闭重开后 KV / 队列 / zset 状态保留。
func TestFilePersistence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db.bolt")
	p, err := bolt.Open(ctx, path, bolt.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Set(ctx, "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := p.SetEx(ctx, "ttl", []byte("v2"), 100); err != nil {
		t.Fatal(err)
	}
	if err := p.QPush(ctx, "q", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := p.ZSet(ctx, "z", "m", 7); err != nil {
		t.Fatal(err)
	}
	p.Close()

	p2, err := bolt.Open(ctx, path, bolt.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if v, ok, _ := p2.Get(ctx, "k"); !ok || string(v) != "v1" {
		t.Fatalf("reopen Get = %q,%v", v, ok)
	}
	if secs, ok, _ := p2.TTL(ctx, "ttl"); !ok || secs <= 0 || secs > 100 {
		t.Fatalf("reopen TTL = %d,%v", secs, ok)
	}
	if v, ok, _ := p2.QPop(ctx, "q"); !ok || string(v) != "a" {
		t.Fatalf("reopen QPop = %q,%v", v, ok)
	}
	if s, ok, _ := p2.ZGet(ctx, "z", "m"); !ok || s != 7 {
		t.Fatalf("reopen ZGet = %d,%v", s, ok)
	}
}

// TestOpenURI 验证 URI 形态与参数解析。
func TestOpenURI(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "uri.bolt")
	u, err := url.Parse("bolt://" + path + "?nosync=1&timeout=3s")
	if err != nil {
		t.Fatal(err)
	}
	kp, err := bolt.OpenURI(ctx, u)
	if err != nil {
		t.Fatal(err)
	}
	defer kp.(core.Closer).Close()
	if err := kp.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := kp.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("Get = %q,%v", v, ok)
	}
}

// TestClosed 验证 Close 后返回 ErrClosed，且重复 Close 幂等。
func TestClosed(t *testing.T) {
	ctx := context.Background()
	p, err := bolt.Open(ctx, filepath.Join(t.TempDir(), "db.bolt"), bolt.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Set(ctx, "k", []byte("v")); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("Close 后 Set 应 ErrClosed, got %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("重复 Close 应无错: %v", err)
	}
}

// TestExpireHidden 验证过期键在读路径被过滤（不依赖后台清理）。
func TestExpireHidden(t *testing.T) {
	ctx := context.Background()
	p, err := bolt.Open(ctx, filepath.Join(t.TempDir(), "db.bolt"), bolt.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.SetEx(ctx, "k", []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	if ok, _ := p.Exists(ctx, "k"); !ok {
		t.Fatal("刚写入应存在")
	}
	time.Sleep(1100 * time.Millisecond)
	if ok, _ := p.Exists(ctx, "k"); ok {
		t.Fatal("过期后应视为不存在")
	}
	if _, ok, _ := p.Get(ctx, "k"); ok {
		t.Fatal("过期后 Get 应 ok=false")
	}
	if pairs, _ := p.Scan(ctx, "", "", 0); len(pairs) != 0 {
		t.Fatalf("过期键不应出现在 Scan 结果: %v", pairs)
	}
}

// TestBatchAtomic 验证批内非法 TTL 导致整批不生效。
func TestBatchAtomic(t *testing.T) {
	ctx := context.Background()
	p, err := bolt.Open(ctx, filepath.Join(t.TempDir(), "db.bolt"), bolt.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.ApplyBatch(ctx, []core.BatchOp{
		{Kind: core.BatchSet, Key: "a", Value: []byte("1")},
		{Kind: core.BatchSetEx, Key: "b", Value: []byte("2"), TTL: 0},
	}); !errors.Is(err, core.ErrInvalidTTL) {
		t.Fatalf("非法 TTL 应 ErrInvalidTTL, got %v", err)
	}
	if ok, _ := p.Exists(ctx, "a"); ok {
		t.Fatal("批校验失败时整批不应生效")
	}
}
