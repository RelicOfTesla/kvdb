package mysql_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/internal/behaviortest"
	"github.com/RelicOfTesla/kvdb/mysql"
)

// TestBehavior 仅在显式设置 KVDB_TEST_MYSQL_DSN 时运行真实 MySQL 用例：
//
//	docker run -d --name kvdb-test-mysql -p 127.0.0.1:43306:3306 \
//	  -e MYSQL_ROOT_PASSWORD=kvdbroot -e MYSQL_DATABASE=kvdb_test mysql:8.0
//	KVDB_TEST_MYSQL_DSN='root:kvdbroot@tcp(127.0.0.1:43306)/kvdb_test?parseTime=true' go test ./mysql/
//
// DSN 必须指向专用测试库：用例开始前会 DROP 本基座使用的三张表。
func TestBehavior(t *testing.T) {
	dsn := os.Getenv("KVDB_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("未设置 KVDB_TEST_MYSQL_DSN，跳过真实 MySQL 测试")
	}
	behaviortest.Run(t, func(t *testing.T) core.KvProvider {
		ctx := context.Background()
		dropTables(t, dsn)
		p, err := mysql.Open(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		return p
	})
}

func dropTables(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
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
