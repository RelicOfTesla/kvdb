package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"kvdb/core"
	"kvdb/internal/behaviortest"
	"kvdb/sqlite"
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
