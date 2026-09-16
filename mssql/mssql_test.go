package mssql_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/kvdbtest"
	"github.com/RelicOfTesla/kvdb/mssql"
)

// TestBehavior 仅在显式设置 KVDB_TEST_MSSQL_DSN 时运行真实 SQL Server 用例：
//
//	docker run -d --name kvdb-test-mssql -p 127.0.0.1:41433:1433 \
//	  -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD='KvdbTest#9887' \
//	  mcr.microsoft.com/mssql/server:2022-latest
//	KVDB_TEST_MSSQL_DSN='sqlserver://sa:KvdbTest#9887@127.0.0.1:41433?database=kvdb_test&encrypt=disable' \
//	  go test ./mssql/
//
// DSN 必须指向专用测试库：用例开始前会 DROP 本基座使用的四张表。
func TestBehavior(t *testing.T) {
	dsn := os.Getenv("KVDB_TEST_MSSQL_DSN")
	if dsn == "" {
		t.Skip("未设置 KVDB_TEST_MSSQL_DSN，跳过真实 SQL Server 测试")
	}
	kvdbtest.Run(t, func(t *testing.T) core.KvProvider {
		ctx := context.Background()
		dropTables(t, dsn)
		p, err := mssql.Open(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		return p
	})
}

func dropTables(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tbl := range []string{"kv_items", "q_items", "q_seq", "z_items"} {
		// DROP TABLE IF EXISTS 自 SQL Server 2016+ 可用（测试容器为 2022）。
		if _, err := db.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
}
