// Package mssql 提供以 Microsoft SQL Server 为后端的基座包装（go-mssqldb 驱动）。
// 通过 sqlstore 共享实现接入；MSSQL 无 RETURNING，Incr/ZIncr 走事务路径，
// upsert 走 MERGE、行锁用 WITH (UPDLOCK, HOLDLOCK)、分页用 OFFSET/FETCH NEXT。
package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/microsoft/go-mssqldb" // 注册 "sqlserver" 驱动

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/sqlstore"
)

// Config 控制 SQL Server 基座行为。
type Config struct {
	// DSN 是 go-mssqldb 连接串（sqlserver://user:pass@host:port?database=... 形态，
	// 或 k=v 分号形态）。加密按 DSN 的 encrypt 参数控制。
	DSN string
	// TablePrefix 加在四张表名前（如 "kvdb_" -> kvdb_kv_items），与其他应用共用库。空串用默认表名。
	TablePrefix string
	// MaxOpenConns 是连接池上限；<=0 用 DefaultMaxOpenConns。
	MaxOpenConns int
}

// Provider 是 SQL Server 基座（别名 sqlstore.Provider）。
type Provider = sqlstore.Provider

// DefaultMaxOpenConns 是连接池上限：与其他 JDBC/ADO.net 常规部署对齐；
// InnoDB/PG 组提交的经验同样适用（池越大同 key 串行等待越少）。
const DefaultMaxOpenConns = 32

// Open 连接 SQL Server 并执行建表（IF OBJECT_ID 幂等）与过期清理。
func Open(ctx context.Context, dsn string) (*Provider, error) {
	return OpenConfig(ctx, Config{DSN: dsn})
}

// OpenConfig 按配置打开（可指定表名前缀）。
func OpenConfig(ctx context.Context, cfg Config) (*Provider, error) {
	db, err := sql.Open("sqlserver", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("mssql: open: %w", err)
	}
	n := cfg.MaxOpenConns
	if n <= 0 {
		n = DefaultMaxOpenConns
	}
	db.SetMaxOpenConns(n)
	db.SetMaxIdleConns(n)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("mssql: ping: %w", err)
	}
	d := sqlstore.MSSQLDialect
	d.TablePrefix = cfg.TablePrefix
	p, err := sqlstore.New(db, d)
	if err != nil {
		db.Close()
		return nil, err
	}
	return p, nil
}

// OpenURI 解析 mssql://user:pass@host:port/db?table_prefix=kvdb_[&<驱动参数>]。
// 其余查询参数原样透传给驱动（sqlserver:// DSN 的 query 部分与 go-mssqldb 一致，
// 如 encrypt=disable）。table_prefix 由本基座消费，不会传给驱动。
// unix socket / ADO 关键字形态请直接用 Open / OpenConfig。
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	dsn := "sqlserver://" + u.Host + u.Path
	if u.User != nil {
		if pw, ok := u.User.Password(); ok {
			dsn = "sqlserver://" + u.User.Username() + ":" + pw + "@" + u.Host + u.Path
		} else if name := u.User.Username(); name != "" {
			dsn = "sqlserver://" + name + "@" + u.Host + u.Path
		} else {
			dsn = "sqlserver://" + u.Host + u.Path
		}
	}
	q := u.Query()
	prefix := q.Get("table_prefix")
	q.Del("table_prefix")
	if enc := q.Encode(); enc != "" {
		dsn += "?" + enc
	}
	return OpenConfig(ctx, Config{DSN: dsn, TablePrefix: prefix})
}

var _ core.KvProvider = (*Provider)(nil)
