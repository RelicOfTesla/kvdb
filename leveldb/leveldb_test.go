package leveldb_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/kvdbtest"
	"github.com/RelicOfTesla/kvdb/leveldb"
)

// TestBehavior 用共享合同用例验证 LevelDB 基座（KV/Queue/ZSet/Batch/过期写语义/
// 返回值所有权/命名空间/并发）。
func TestBehavior(t *testing.T) {
	kvdbtest.RunWithOptions(t, kvdbtest.VirtualClock(t), func(t *testing.T) core.KvProvider {
		p, err := leveldb.Open(t.Context(), filepath.Join(t.TempDir(), "db"), leveldb.Config{})
		if err != nil {
			t.Fatal(err)
		}
		return p
	})
}

// TestFilePersistence 验证重开目录后数据保留（LevelDB 是持久化基座）。
func TestFilePersistence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "kv")
	p, err := leveldb.Open(ctx, path, leveldb.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := p.QPush(ctx, "q", []byte("e1")); err != nil {
		t.Fatal(err)
	}
	if err := p.ZSet(ctx, "z", "m", 7); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.Get(ctx, "k"); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("Close 后应 ErrClosed, got %v", err)
	}

	p2, err := leveldb.Open(ctx, path, leveldb.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if v, ok, _ := p2.Get(ctx, "k"); !ok || string(v) != "v" {
		t.Fatalf("重开后 Get = %q,%v", v, ok)
	}
	if v, ok, _ := p2.QPop(ctx, "q"); !ok || string(v) != "e1" {
		t.Fatalf("重开后 QPop = %q,%v", v, ok)
	}
	if s, ok, _ := p2.ZGet(ctx, "z", "m"); !ok || s != 7 {
		t.Fatalf("重开后 ZGet = %d,%v", s, ok)
	}
	if n, _ := p2.QSize(ctx, "gone"); n != 0 {
		t.Fatalf("空队列 QSize = %d", n)
	}
}

// TestOpenURI 验证 URI 形态与参数解析。
func TestOpenURI(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	for _, tc := range []struct{ uri string }{
		{"leveldb://" + filepath.Join(dir, "a")},
		{"leveldb://" + filepath.Join(dir, "b") + "?sync=1"}, // 逐提交 fsync（显式要耐久）
		{"leveldb://" + filepath.Join(dir, "c") + "?cache=4&wb=2"},
	} {
		db, err := kvdb.Open(ctx, tc.uri)
		if err != nil {
			t.Fatalf("%s: %v", tc.uri, err)
		}
		if err := db.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		if v, ok, _ := db.Get(ctx, "k"); !ok || string(v) != "v" {
			t.Fatalf("%s Get = %q,%v", tc.uri, v, ok)
		}
		db.Close()
	}
	// 非法参数必须报错，而不是静默忽略
	if _, err := kvdb.Open(ctx, "leveldb://"+filepath.Join(dir, "d")+"?cache=abc"); err == nil {
		t.Fatal("非法 cache 应报错")
	}
	// 已废弃的 nosync 必须报错：默认即不 fsync，静默接受会让人误以为"写了才高速"
	if _, err := kvdb.Open(ctx, "leveldb://"+filepath.Join(dir, "e")+"?nosync=1"); err == nil {
		t.Fatal("nosync 参数已废弃，应报错")
	}
}

// TestCloseIdempotent 验证重复 Close 不报错、不 panic。
func TestCloseIdempotent(t *testing.T) {
	ctx := context.Background()
	p, err := leveldb.Open(ctx, filepath.Join(t.TempDir(), "x"), leveldb.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("二次 Close 应幂等, got %v", err)
	}
}
