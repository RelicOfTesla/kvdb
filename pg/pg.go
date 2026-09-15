// Package pg 提供以 PostgreSQL 为后端的基座包装（jackc/pgx/v5 纯 Go 驱动）。
// dsn 接受 pgx 标准连接串与 postgres:// URL（与 kvdb.Open 的 pg:// URI 同源）。
package pg

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/jackc/pgx/v5/stdlib" // 注册 "pgx" 驱动

	"github.com/RelicOfTesla/kvdb"
	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/sqlstore"
)

func init() { kvdb.MustRegister("pg", OpenURI) }

// Config 控制 PostgreSQL 基座行为。
type Config struct {
	// DSN 是 pgx 连接串。
	DSN string
	// TablePrefix 加在四张表名前（如 "kvdb_" -> kvdb_kv_items），用于与其他应用
	// 共用一个 schema。空串使用默认表名。
	TablePrefix string
}

// OpenURI 解析 pg://user:pass@host:port/db?opts 为 pgx 标准连接串。
// 其中 table_prefix=<pfx> 由本基座消费，不会传给驱动。
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	dsn := "postgres://" + u.Host + u.Path
	if u.User != nil {
		dsn = "postgres://" + u.User.String() + "@" + u.Host + u.Path
	}
	q := u.Query()
	prefix := q.Get("table_prefix")
	q.Del("table_prefix") // 驱动不认识该参数，必须从 DSN 里剔除
	if enc := q.Encode(); enc != "" {
		dsn += "?" + enc
	}
	return OpenConfig(ctx, Config{DSN: dsn, TablePrefix: prefix})
}

// Provider 是 PostgreSQL 基座（别名 sqlstore.Provider）。
type Provider = sqlstore.Provider

// DefaultMaxOpenConns 是连接池上限：并发事务由 PostgreSQL 组提交共享 WAL fsync，
// 池越大吞吐越高（实测 8→32 并发仍有明显提升，见 README 性能一节）。
const DefaultMaxOpenConns = 32

// Open 连接 PostgreSQL 并执行建表（IF NOT EXISTS）与过期清理。
func Open(ctx context.Context, dsn string) (*Provider, error) {
	return OpenConfig(ctx, Config{DSN: dsn})
}

// OpenConfig 按配置打开（可指定表名前缀）。
func OpenConfig(ctx context.Context, cfg Config) (*Provider, error) {
	dsn := cfg.DSN
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: open: %w", err)
	}
	// 显式放开连接池：避免驱动默认的空闲连接数过低导致并发写排队。
	db.SetMaxOpenConns(DefaultMaxOpenConns)
	db.SetMaxIdleConns(DefaultMaxOpenConns)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	d := sqlstore.PostgresDialect
	d.TablePrefix = cfg.TablePrefix
	p, err := sqlstore.New(db, d)
	if err != nil {
		db.Close()
		return nil, err
	}
	return p, nil
}

var _ core.KvProvider = (*Provider)(nil)
