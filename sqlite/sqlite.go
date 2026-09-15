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

	"github.com/RelicOfTesla/kvdb/core"
	"github.com/RelicOfTesla/kvdb/sqlstore"
)

// Config 控制 SQLite 基座行为。
type Config struct {
	// Path 是数据库文件路径；":memory:" 表示进程内临时库。
	Path string
	// TablePrefix 加在四张表名前（如 "kvdb_" -> kvdb_kv_items），用于与其他应用
	// 共用一个库。空串使用默认表名。
	TablePrefix string
	// Sync 控制提交时的落盘强度（映射 PRAGMA synchronous）：
	//
	//	SyncFull（缺省）FULL：每个提交都 fsync WAL，断电不丢已确认写入（最慢）。
	//	SyncNormal      NORMAL：只在 checkpoint 时 fsync；WAL 模式下崩溃可能丢
	//	                 最近的若干已提交事务，但库文件不会被写坏（更快）。
	//
	// 这里刻意不提供 synchronous=OFF：OFF 在崩溃时可能损坏数据库文件，
	// 而本选项的契约是"最多丢尾部已确认写入"，不是"可能丢整个库"。
	Sync SyncMode
}

// SyncMode 是 sqlite 的落盘强度档位。
type SyncMode int

const (
	// SyncFull 是缺省档：PRAGMA synchronous=FULL，逐提交 fsync。
	SyncFull SyncMode = iota
	// SyncNormal 是高速档：PRAGMA synchronous=NORMAL，崩溃可能丢最近提交但不损坏库。
	SyncNormal
)

// OpenURI 解析 sqlite://<path>[?table_prefix=pfx_][&sync=0|1]；
// Host 为空表示绝对路径 sqlite:///abs/x。sync 缺省＝FULL（与 SQLite 自身默认
// 一致），sync=1 显式 FULL，sync=0 用 NORMAL（更快）。
func OpenURI(ctx context.Context, u *url.URL) (core.KvProvider, error) {
	p := u.Path
	if u.Host != "" {
		p = strings.TrimPrefix(u.Host+u.Path, "/")
	}
	cfg := Config{Path: p, TablePrefix: u.Query().Get("table_prefix")}
	switch v := u.Query().Get("sync"); v {
	case "", "1":
		cfg.Sync = SyncFull
	case "0":
		cfg.Sync = SyncNormal
	default:
		return nil, fmt.Errorf("sqlite: invalid sync %q (want 0 or 1)", v)
	}
	return OpenConfig(ctx, cfg)
}

// Provider 是 SQLite 基座（别名 sqlstore.Provider）。
type Provider = sqlstore.Provider

// Open 打开 SQLite 数据库文件；path 为 ":memory:" 时使用进程内临时库。
// 建表（IF NOT EXISTS）与过期清理在 Open 内完成。
func Open(ctx context.Context, path string) (*Provider, error) {
	return OpenConfig(ctx, Config{Path: path})
}

// OpenConfig 按配置打开（可指定表名前缀）。
func OpenConfig(ctx context.Context, cfg Config) (*Provider, error) {
	path := cfg.Path
	dsn := sqliteDSN(path, cfg.Sync)
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
	d := sqlstore.SQLiteDialect
	d.TablePrefix = cfg.TablePrefix
	p, err := sqlstore.New(db, d)
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

func sqliteDSN(path string, sync SyncMode) string {
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
	// 用驱动的 _synchronous 简写而非 _pragma：它会校验取值，写错直接报错，
	// 不会静默降级耐久性；且每条连接都生效（synchronous 是连接级 pragma）。
	syncName := "FULL"
	if sync == SyncNormal {
		syncName = "NORMAL"
	}
	return path + sep + "_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_txlock=immediate&_synchronous=" + syncName
}

var _ core.KvProvider = (*Provider)(nil)
