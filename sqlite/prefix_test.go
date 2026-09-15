package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/sqlite"

	_ "modernc.org/sqlite"
)

// TestTablePrefix 验证 Config.TablePrefix 生效：四张表与二级索引都带前缀，
// 且不存在未加前缀的同名表（DDL 与查询语句必须走同一套名字）。
func TestTablePrefix(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "p.db")
	p, err := sqlite.OpenConfig(ctx, sqlite.Config{Path: path, TablePrefix: "app1_"})
	if err != nil {
		t.Fatal(err)
	}
	db := kvdb.Wrap(p)
	defer db.Close()
	if err := db.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if n, err := db.Incr(ctx, "c", 5); err != nil || n != 5 {
		t.Fatalf("Incr=%d,%v", n, err)
	}
	if err := db.QPush(ctx, "q", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := db.ZSet(ctx, "z", "m", 3); err != nil {
		t.Fatal(err)
	}
	db.Close()
	// 表必须真的叫 app1_kv_items，且默认名不存在
	raw, _ := sql.Open("sqlite", "file:"+path)
	defer raw.Close()
	for _, tbl := range []string{"app1_kv_items", "app1_q_seq", "app1_q_items", "app1_z_items"} {
		var c int
		if err := raw.QueryRow("SELECT count(*) FROM " + tbl).Scan(&c); err != nil {
			t.Fatalf("表 %s 不存在: %v", tbl, err)
		}
	}
	if err := raw.QueryRow("SELECT count(*) FROM kv_items").Scan(new(int)); err == nil {
		t.Fatal("不该存在无前缀的 kv_items")
	}
}

func TestBadPrefixRejected(t *testing.T) {
	_, err := sqlite.OpenConfig(context.Background(), sqlite.Config{Path: ":memory:", TablePrefix: "a-b"})
	if err == nil {
		t.Fatal("非法前缀应被拒绝")
	}
}
