// Package pg 提供以 PostgreSQL 为后端的基座包装（jackc/pgx/v5 纯 Go 驱动）。
// dsn 接受 pgx 标准连接串与 postgres:// URL（与 kvdb.Open 的 pg:// URI 同源）。
package pg

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/jackc/pgx/v5/stdlib" // 注册 "pgx" 驱动

	"kvdb"
	"kvdb/core"
	"kvdb/sqlstore"
)

func init() { kvdb.MustRegister("pg", OpenURI) }

// OpenURI 解析 pg://user:pass@host:port/db?opts 为 pgx 标准连接串。
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	dsn := "postgres://" + u.Host + u.Path
	if u.User != nil {
		dsn = "postgres://" + u.User.String() + "@" + u.Host + u.Path
	}
	if q := u.RawQuery; q != "" {
		dsn += "?" + q
	}
	return Open(ctx, dsn)
}

// Provider 是 PostgreSQL 基座（别名 sqlstore.Provider）。
type Provider = sqlstore.Provider

// Open 连接 PostgreSQL 并执行建表（IF NOT EXISTS）与过期清理。
func Open(ctx context.Context, dsn string) (*Provider, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	p, err := sqlstore.New(db, sqlstore.PostgresDialect)
	if err != nil {
		db.Close()
		return nil, err
	}
	return p, nil
}

var _ core.KvProvider = (*Provider)(nil)
