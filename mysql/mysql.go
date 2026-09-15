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

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/sqlstore"
)

// Config 控制 MySQL 基座行为。
type Config struct {
	// DSN 是 go-sql-driver 的连接串。
	DSN string
	// TablePrefix 加在四张表名前（如 "kvdb_" -> kvdb_kv_items），用于与其他应用
	// 共用一个 schema。空串使用默认表名。
	TablePrefix string
}

// OpenURI 解析 mysql://user:pass@host:port/db?opts 并还原为驱动 DSN。
// 其中 table_prefix=<pfx> 由本基座消费，不会传给驱动。
// unix socket 等 DSN 形态请直接用 Open / OpenConfig。
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
	q := u.Query()
	prefix := q.Get("table_prefix")
	q.Del("table_prefix") // 驱动不认识该参数，必须从 DSN 里剔除
	if enc := q.Encode(); enc != "" {
		dsn += "?" + enc
	}
	return OpenConfig(ctx, Config{DSN: dsn, TablePrefix: prefix})
}

// Provider 是 MySQL 基座（别名 sqlstore.Provider）。
type Provider = sqlstore.Provider

// DefaultMaxOpenConns 是连接池上限：并发事务由 InnoDB 组提交共享 fsync，
// 池越大同 key 串行等待越少（实测 pool 1→32 提升约一个数量级，见 README 性能一节）。
const DefaultMaxOpenConns = 32

// Open 连接 MySQL 并执行建表（IF NOT EXISTS）与过期清理。
func Open(ctx context.Context, dsn string) (*Provider, error) {
	return OpenConfig(ctx, Config{DSN: dsn})
}

// OpenConfig 按配置打开（可指定表名前缀）。
func OpenConfig(ctx context.Context, cfg Config) (*Provider, error) {
	dsn := cfg.DSN
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql: open: %w", err)
	}
	// 显式放开连接池：驱动默认 MaxIdleConns=2 会让并发写退化为串行，
	// 从而失去组提交带来的吞吐收益。
	db.SetMaxOpenConns(DefaultMaxOpenConns)
	db.SetMaxIdleConns(DefaultMaxOpenConns)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("mysql: ping: %w", err)
	}
	d := sqlstore.MySQLDialect
	d.TablePrefix = cfg.TablePrefix
	p, err := sqlstore.New(db, d)
	if err != nil {
		db.Close()
		return nil, err
	}
	return p, nil
}

var _ core.KvProvider = (*Provider)(nil)
