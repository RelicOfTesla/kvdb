package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/internal/behaviortest"
	"github.com/RelicOfTesla/kvdb/sqlite"
)

// TestBehavior 用内存 SQLite 验证 sqlstore 共享实现的完整行为。
func TestBehavior(t *testing.T) {
	behaviortest.Run(t, func(t *testing.T) core.KvProvider {
		p, err := sqlite.Open(t.Context(), ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		return p
	})
}

// TestFilePersistence 验证文件库重开保留数据。
func TestFilePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kv.db")
	ctx := context.Background()
	p, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Set(ctx, "k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := p.QPush(ctx, "q", []byte("a")); err != nil {
		t.Fatal(err)
	}
	p.Close()

	p2, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	if v, ok, _ := p2.Get(ctx, "k"); !ok || string(v) != "v1" {
		t.Fatalf("reopen Get = %q,%v", v, ok)
	}
	if v, ok, _ := p2.QPop(ctx, "q"); !ok || string(v) != "a" {
		t.Fatalf("reopen QPop = %q,%v", v, ok)
	}
}

// TestScanIndexed 用 SQLite 的 EXPLAIN QUERY PLAN 验证 Scan 的过滤/排序
// 命中 kv_items 主键索引（SEARCH ... USING COVERING INDEX，而非 SCAN + 临时排序）。
func TestScanIndexed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "kv.db")
	p, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		key := string(rune('a'+i%26)) + string(rune('0'+i/26))
		if err := p.Set(ctx, key, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()

	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx,
		"EXPLAIN QUERY PLAN SELECT k, v FROM kv_items WHERE k >= ? AND k <= ? AND (expire_at = 0 OR expire_at > ?) ORDER BY k LIMIT ?",
		[]byte("a0"), []byte("z9"), 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if len(plan) == 0 {
		t.Fatal("EXPLAIN 无输出")
	}
	t.Logf("EXPLAIN QUERY PLAN: %v", plan)
	joined := strings.Join(plan, " | ")
	if strings.Contains(joined, "SCAN kv_items") && !strings.Contains(joined, "USING") {
		t.Fatalf("Scan 未走主键索引: %s", joined)
	}
	if strings.Contains(joined, "USE TEMP B-TREE") {
		t.Fatalf("Scan 出现临时排序: %s", joined)
	}
}

// TestLegacySchemaMigration 验证旧库（kv_items 无数值投影列 n）升级后
// 所有写路径可用：缺少该列时 Set/Incr 会报 "no column named n"。
func TestLegacySchemaMigration(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")

	// 构造 de79c5b 之前的 schema：只有 k/v/expire_at，没有 n。
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx,
		"CREATE TABLE kv_items (k BLOB NOT NULL, v BLOB NOT NULL, expire_at INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (k))"); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, "INSERT INTO kv_items (k, v) VALUES (?, ?)", []byte("num"), []byte("41")); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, "INSERT INTO kv_items (k, v) VALUES (?, ?)", []byte("txt"), []byte("abc")); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	p, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatalf("旧库应能自动迁移打开: %v", err)
	}
	// 1) 旧库此前会在这里报 "table kv_items has no column named n"。
	if err := p.Set(ctx, "new", []byte("v")); err != nil {
		t.Fatalf("迁移后 Set: %v", err)
	}
	// 2) 回填：旧行 41 的数值投影应已补齐，Incr 得到 42（而不是 ErrNotInteger）。
	if n, err := p.Incr(ctx, "num", 1); err != nil || n != 42 {
		t.Fatalf("迁移回填后 Incr(num) = %d,%v (want 42)", n, err)
	}
	// 3) 非整数旧行仍按非整数处理（回填必须保持 NULL）。
	if _, err := p.Incr(ctx, "txt", 1); !errors.Is(err, core.ErrNotInteger) {
		t.Fatalf("非整数旧行应 ErrNotInteger, got %v", err)
	}
	// 4) Set 覆盖后 n 同步更新。
	if err := p.Set(ctx, "num", []byte("7")); err != nil {
		t.Fatal(err)
	}
	if n, err := p.Incr(ctx, "num", 1); err != nil || n != 8 {
		t.Fatalf("Set 后 Incr = %d,%v (want 8)", n, err)
	}
	p.Close()

	// 5) 迁移幂等：再开一次不再改 schema，数据仍在。
	p2, err := sqlite.Open(ctx, path)
	if err != nil {
		t.Fatalf("二次打开（幂等迁移）: %v", err)
	}
	defer p2.Close()
	if v, ok, _ := p2.Get(ctx, "new"); !ok || string(v) != "v" {
		t.Fatalf("二次打开数据丢失: %q,%v", v, ok)
	}
}
