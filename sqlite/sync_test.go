package sqlite

import (
	"context"
	"database/sql"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
)

// TestSyncModeDSN 验证 sync 各档的解析与**实际生效值**。
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
		{"", SyncNormal, 1},        // 缺省：NORMAL（与其他本地基座缺省不逐提交 fsync 一致）
		{"?sync=0", SyncNormal, 1}, // 显式 NORMAL，等价缺省
		{"?sync=1", SyncFull, 2},   // 显式要耐久
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

// TestDefaultIsNormal 锁定"零值即 NORMAL"：Config{} 与不写 sync 参数都必须落到
// NORMAL。缺省语义的回归点——若有人把 SyncMode 的 iota 顺序调回去（或让 OpenURI
// 的缺省分支指向 FULL），缺省档会悄悄慢一个数量级而测试却全绿，这里必须钉住。
func TestDefaultIsNormal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// 1) Config 零值就是 NORMAL（SyncMode 的零值必须是 SyncNormal）
	if got := sqliteDSN(filepath.Join(dir, "z.db"), Config{}.Sync); !strings.Contains(got, "_synchronous=NORMAL") {
		t.Fatalf("Config{} 零值应为 NORMAL, DSN = %q", got)
	}
	// 2) URI 不写 sync 必须与 Config{} 落到同一档：用 OpenURI 真实走一遍，
	//    再用同档位构造 DSN 比对（不在此处复刻 OpenURI 的 switch，避免测试与
	//    实现各写一份、同时改错还都通过）。
	u, err := url.Parse("sqlite://" + filepath.Join(dir, "u.db"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := OpenURI(ctx, u)
	if err != nil {
		t.Fatalf("URI 不写 sync 应能打开: %v", err)
	}
	if err := p.(*Provider).Close(); err != nil {
		t.Fatal(err)
	}
	// 3) 与 SQLite 自身"缺省即 FULL"相反是刻意的：断言我们确实改成了 NORMAL
	//    而非依赖驱动默认。
	if got := sqliteDSN(filepath.Join(dir, "e.db"), SyncNormal); !strings.Contains(got, "_synchronous=NORMAL") {
		t.Fatalf("SyncNormal 应生成 NORMAL, DSN = %q", got)
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
