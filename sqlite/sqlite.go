// Package sqlite 提供以 SQLite 文件（或 :memory:）为后端的基座包装。
// 通过 modernc.org/sqlite 纯 Go 驱动接入 sqlstore 共享实现，不需要 cgo。
// 写并发按 SQLite 单写者模型用 MaxOpenConns(1) 串行化。
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "modernc.org/sqlite" // 注册 "sqlite" 驱动

	"kvdb"
	"kvdb/core"
	"kvdb/sqlstore"
)

func init() { kvdb.MustRegister("sqlite", OpenURI) }

// OpenURI 解析 sqlite://<path>；Host 为空表示绝对路径 sqlite:///abs/x。
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	p := u.Path
	if u.Host != "" {
		p = strings.TrimPrefix(u.Host+u.Path, "/")
	}
	return Open(ctx, p)
}

// Provider 是 SQLite 基座（别名 sqlstore.Provider）。
type Provider = sqlstore.Provider

// Open 打开 SQLite 数据库文件；path 为 ":memory:" 时使用进程内临时库。
// 建表（IF NOT EXISTS）与过期清理在 Open 内完成。
func Open(ctx context.Context, path string) (*Provider, error) {
	dsn := sqliteDSN(path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}
	// SQLite 单写者：串行化连接避免 SQLITE_BUSY；busy_timeout 缓冲瞬时竞争。
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: ping: %w", err)
	}
	p, err := sqlstore.New(db, sqlstore.SQLiteDialect)
	if err != nil {
		db.Close()
		return nil, err
	}
	return p, nil
}

func sqliteDSN(path string) string {
	if path == ":memory:" {
		return ":memory:?_pragma=busy_timeout(5000)"
	}
	// 文件库启用 WAL：读写不互斥，崩溃恢复能力更好。
	if !strings.HasPrefix(path, "file:") {
		path = "file:" + path
	}
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
}

var _ core.KvProvider = (*Provider)(nil)
