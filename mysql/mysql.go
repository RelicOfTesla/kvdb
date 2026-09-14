// Package mysql 提供以 MySQL 为后端的基座包装（go-sql-driver/mysql）。
// 连接串为驱动 DSN（含 user:pass@tcp(host:port)/db?params 形式），
// 正式使用请在 DSN 或环境中维护好凭据。
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/go-sql-driver/mysql" // 注册 "mysql" 驱动

	"kvdb"
	"kvdb/core"
	"kvdb/sqlstore"
)

func init() { kvdb.MustRegister("mysql", OpenURI) }

// OpenURI 解析 mysql://user:pass@host:port/db?opts 并还原为驱动 DSN。
// unix socket 等 DSN 形态请直接用 Open。
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	dsn := ""
	if u.User != nil {
		if pw, ok := u.User.Password(); ok {
			dsn = u.User.Username() + ":" + pw + "@"
		} else {
			dsn = u.User.Username() + "@"
		}
	}
	dsn += "tcp(" + u.Host + ")" + u.Path
	if q := u.Query().Encode(); q != "" {
		dsn += "?" + q
	}
	return Open(ctx, dsn)
}

// Provider 是 MySQL 基座（别名 sqlstore.Provider）。
type Provider = sqlstore.Provider

// Open 连接 MySQL 并执行建表（IF NOT EXISTS）与过期清理。
func Open(ctx context.Context, dsn string) (*Provider, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql: open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("mysql: ping: %w", err)
	}
	p, err := sqlstore.New(db, sqlstore.MySQLDialect)
	if err != nil {
		db.Close()
		return nil, err
	}
	return p, nil
}

var _ core.KvProvider = (*Provider)(nil)
