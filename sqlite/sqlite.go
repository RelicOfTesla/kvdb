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
	// 连接数策略：
	//   - :memory: 每个连接是独立库，必须单连接，否则并发请求落到空库；
	//   - 文件库在 WAL 下写单写者、读可并行，放宽连接让并发读不排队，
	//     写竞争由 busy_timeout 排队（见 sqliteDSN）。
	if isMemory(path) {
		db.SetMaxOpenConns(1)
	} else {
		db.SetMaxOpenConns(4)
	}
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

// isMemory 判断是否为进程内内存库（:memory: 及其 URI 形态）。
func isMemory(path string) bool {
	return path == ":memory:" || strings.Contains(path, "mode=memory")
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
	// _pragma 参数由 modernc 驱动在**每条新连接**建立时执行，因此多连接下
	// busy_timeout 依然生效；_txlock=immediate 让写事务一开始就取写锁，
	// 避免"读事务升级写锁"在并发下直接返回 SQLITE_BUSY（不等待 busy_timeout）。
	return path + sep + "_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_txlock=immediate"
}

var _ core.KvProvider = (*Provider)(nil)
