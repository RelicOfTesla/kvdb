package sqlite

import (
	"context"
	"database/sql"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
)

// TestSyncModeDSN 验证 sync 三态的解析与**实际生效值**。
//
// 只断言 DSN 字符串不够：synchronous 是连接级 pragma，真正要确认的是连接上
// 生效的档位。这里按各自的 DSN 开一条原生连接读 PRAGMA 校验。
func TestSyncModeDSN(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	for _, tc := range []struct {
		query  string
		mode   SyncMode
		pragma int
	}{
		{"", SyncFull, 2},       // 缺省：与 SQLite 自身默认一致，不静默降级
		{"?sync=1", SyncFull, 2}, // 显式要耐久
		{"?sync=0", SyncNormal, 1}, // 高速档：NORMAL
	} {
		path := filepath.Join(dir, strings.TrimPrefix(tc.query, "?")+"a.db")
		dsn := sqliteDSN(path, tc.mode)
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		var got int
		if err := db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&got); err != nil {
			db.Close()
			t.Fatalf("%s: %v", tc.query, err)
		}
		db.Close()
		if got != tc.pragma {
			t.Fatalf("%s: PRAGMA synchronous = %d, want %d", tc.query, got, tc.pragma)
		}
	}
}

// TestOpenURISyncParsing 验证三种合法写法都能打开、非法取值报错（不回落默认档）。
func TestOpenURISyncParsing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	for _, q := range []string{"", "?sync=1", "?sync=0"} {
		u, err := url.Parse("sqlite://" + filepath.Join(dir, "u.db") + q)
		if err != nil {
			t.Fatal(err)
		}
		p, err := OpenURI(ctx, u)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if err := p.(*Provider).Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, bad := range []string{"?sync=2", "?sync=true", "?sync=off"} {
		u, err := url.Parse("sqlite://" + filepath.Join(dir, "bad.db") + bad)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := OpenURI(ctx, u); err == nil {
			t.Fatalf("%s 应报错", bad)
		}
	}
}

// TestMemoryDSN 确认内存库无落盘语义，不带 WAL/synchronous 相关参数。
func TestMemoryDSN(t *testing.T) {
	if dsn := sqliteDSN(":memory:", SyncFull); dsn != ":memory:?_pragma=busy_timeout(5000)" {
		t.Fatalf("内存库 DSN = %q", dsn)
	}
}
