package pg_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/kvdbtest"
	"github.com/RelicOfTesla/kvdb/pg"
)

// TestBehavior 仅在显式设置 KVDB_TEST_PG_DSN 时运行真实 PostgreSQL 用例：
//
//	docker run -d --name kvdb-test-pg -p 127.0.0.1:45432:5432 \
//	  -e POSTGRES_PASSWORD=kvdbpass -e POSTGRES_DB=kvdb_test postgres:16-alpine
//	KVDB_TEST_PG_DSN='postgres://postgres:kvdbpass@127.0.0.1:45432/kvdb_test?sslmode=disable' go test ./pg/
//
// DSN 必须指向专用测试库：用例开始前会 DROP 本基座使用的三张表。
func TestBehavior(t *testing.T) {
	dsn := os.Getenv("KVDB_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("未设置 KVDB_TEST_PG_DSN，跳过真实 PostgreSQL 测试")
	}
	kvdbtest.Run(t, func(t *testing.T) core.KvProvider {
		ctx := context.Background()
		dropTables(t, dsn)
		p, err := pg.Open(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		return p
	})
}

func dropTables(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tbl := range []string{"kv_items", "q_items", "q_seq", "z_items"} {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
}
