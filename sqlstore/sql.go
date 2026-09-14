// Package sqlstore 提供基于 database/sql 的关系型基座共享实现，由
// kvdb/mysql、kvdb/sqlite、kvdb/pg 三个包装包以不同方言实例化。
// 物理模型：kv_items / q_seq+q_items / z_items 三张表；键与值为二进制安全
// （BLOB/BYTEA/VARBINARY），按字节序比较，与 SSDB 的 LevelDB 序一致。
// 过期的 kv 行在读取路径过滤、Open 时批量清理一次（长期运行累积时
// 可自行按 expire_at 清理）。
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"kvdb/core"
)

var (
	_ core.FullProvider = (*Provider)(nil)
)

// Dialect 描述不同数据库的 SQL 差异点。
type Dialect struct {
	// Name 仅为错误信息与调试使用。
	Name string
	// Ph 返回第 n（1 起）个参数占位符。
	Ph func(n int) string
	// KvUpsertTail 是 kv_items 与 q_seq INSERT 的"已存在则覆盖/忽略"冲突子句。
	KvUpsertTail string // INSERT INTO kv_items (k, v) VALUES (...)+此尾；空尾表示不支持（不适用）
	// KvSetExTail 是 SetEx 的冲突子句：同时覆盖值与 expire_at。
	KvSetExTail string
	// IncrSeedTail 是 Incr 的占位插入冲突子句：行不存在则插入 '0'，
	// 存在则不做任何改动——保证随后 SELECT FOR UPDATE 锁到的是已存在行，
	// 避免缺失键上的 gap 锁互相等待形成死锁环。
	IncrSeedTail    string
	ZSetUpsertTail  string // INSERT INTO z_items ... 冲突时覆盖分数
	ZIncrUpsertTail string // INSERT INTO z_items ... 冲突时 s = s + 新值
	QSeqIgnoreTail  string // INSERT INTO q_seq ... 冲突时忽略（确保行存在）
	ForUpdate       string // SELECT ... FOR UPDATE 后缀（SQLite 无）
	// Returning 为 true 时用 UPDATE ... RETURNING 把"锁行 + 自增 + 写回"
	// 合并为单条语句（PostgreSQL/SQLite 支持；MySQL 不支持走多条路径）。
	Returning bool
	// IncrSQL 是单语句原子自增（缺失按 0 起算、非整数报错）的完整语句模板；
	// 空串表示该方言不具备单语句形态，Incr 走事务多语句路径。
	// 占位符：{1}=key, {2}=delta。
	IncrSQL string
	// HasNumCol 表示 kv_items 有数值投影列 n（供单语句 Incr 使用）；
	// 为 true 时 Set/SetEx 必须同步维护 n，否则 Incr 会误判为非整数。
	HasNumCol bool
	// SerializeWrites 为 true 时基座在进程内串行化全部写操作（单写者模型）。
	// SQLite 需要：多连接并发写即使有 busy_timeout 也会在持续竞争下报
	// SQLITE_BUSY；进程级写锁把竞争变成排队，读仍由 WAL 并行。
	SerializeWrites bool
	// IncrDeltaParams 是 IncrSQL 模板中 {2}（增量）占位符的出现次数：
	// PG 的 $n 重复引用同一参数，只需传一次；MySQL/SQLite 的 ? 为位置参数，
	// 重复出现必须重复传值（SQLite 的 n/v 两列都需要增量）。
	IncrDeltaParams int
	// DDL（占位符无需参数）。
	KvDDL, QSeqDDL, QItemsDDL, ZDDL, ZIdxDDL string
}

// MySQLDialect / SQLiteDialect / PostgresDialect 是三个内置方言，
// 供 kvdb/mysql、kvdb/sqlite、kvdb/pg 包装包实例化本基座。
var (
	MySQLDialect = Dialect{
		Name:            "mysql",
		Ph:              func(n int) string { return "?" },
		KvUpsertTail:    "ON DUPLICATE KEY UPDATE v = VALUES(v)",
		KvSetExTail:     "ON DUPLICATE KEY UPDATE v = VALUES(v), expire_at = VALUES(expire_at)",
		IncrSeedTail:    "ON DUPLICATE KEY UPDATE v = v",
		ZSetUpsertTail:  "ON DUPLICATE KEY UPDATE s = VALUES(s)",
		ZIncrUpsertTail: "ON DUPLICATE KEY UPDATE s = s + VALUES(s)",
		QSeqIgnoreTail:  "ON DUPLICATE KEY UPDATE next = next",
		ForUpdate:       " FOR UPDATE",
		Returning:       false, // MySQL 无 UPDATE ... RETURNING（走 qSeq/事务路径）
		KvDDL:           "CREATE TABLE IF NOT EXISTS kv_items (k VARBINARY(768) NOT NULL, v LONGBLOB NOT NULL, expire_at BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (k))",
		QSeqDDL:         "CREATE TABLE IF NOT EXISTS q_seq (q VARBINARY(768) NOT NULL, next BIGINT NOT NULL, prev BIGINT NOT NULL, PRIMARY KEY (q))",
		QItemsDDL:       "CREATE TABLE IF NOT EXISTS q_items (q VARBINARY(768) NOT NULL, seq BIGINT NOT NULL, v LONGBLOB NOT NULL, PRIMARY KEY (q, seq))",
		// MySQL 无 CREATE INDEX IF NOT EXISTS（不幂等），索引内联在 CREATE TABLE
		// 的 KEY 子句；ZIdxDDL 留空让 New 跳过独立索引 DDL（见表 DDL 注释）。
		ZDDL:    "CREATE TABLE IF NOT EXISTS z_items (z VARBINARY(768) NOT NULL, k VARBINARY(768) NOT NULL, s BIGINT NOT NULL, PRIMARY KEY (z, k), KEY idx_z_items_s (z, s, k))",
		ZIdxDDL: "",
	}
	SQLiteDialect = Dialect{
		Name:            "sqlite",
		Ph:              func(n int) string { return "?" },
		KvUpsertTail:    "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
		KvSetExTail:     "ON CONFLICT(k) DO UPDATE SET v = excluded.v, expire_at = excluded.expire_at",
		IncrSeedTail:    "ON CONFLICT(k) DO NOTHING",
		ZSetUpsertTail:  "ON CONFLICT(z, k) DO UPDATE SET s = excluded.s",
		ZIncrUpsertTail: "ON CONFLICT(z, k) DO UPDATE SET s = s + excluded.s",
		QSeqIgnoreTail:  "ON CONFLICT(q) DO NOTHING",
		ForUpdate:       "",
		SerializeWrites: true, // SQLite 单写者：进程内串行化写，WAL 保读并行
		Returning:       true, // SQLite >= 3.35 支持 RETURNING
		// 单语句 upsert：n 列缓存数值投影，v 列同步物化为十进制文本。
		// WHERE 过滤非法值 -> 无匹配行 -> RETURNING 无结果 -> 映射 ErrNotInteger。
		HasNumCol:       true,
		IncrDeltaParams: 2, // {2} 出现两处（CAST 文本与数值），? 位置参数需重复传值
		// 增量经 excluded.n 在两个分支间复用。
		// {1}=key、{2}=增值；INSERT 分支把 n 直接置为增值（缺失按 0 起算），
		// v 列由返回后的调用方按需回填——本语句只保证 n 与 v 的数值一致性。
		IncrSQL: "INSERT INTO kv_items (k, v, n) VALUES ({1}, CAST({2} AS TEXT), {2}) " +
			"ON CONFLICT(k) DO UPDATE SET v = CAST(kv_items.n + excluded.n AS TEXT), n = kv_items.n + excluded.n " +
			"WHERE kv_items.n IS NOT NULL RETURNING kv_items.n",
		KvDDL: "CREATE TABLE IF NOT EXISTS kv_items (k BLOB NOT NULL, v BLOB NOT NULL, n INTEGER NULL, " +
			"expire_at INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (k))",
		QSeqDDL:   "CREATE TABLE IF NOT EXISTS q_seq (q BLOB NOT NULL, next INTEGER NOT NULL, prev INTEGER NOT NULL, PRIMARY KEY (q))",
		QItemsDDL: "CREATE TABLE IF NOT EXISTS q_items (q BLOB NOT NULL, seq INTEGER NOT NULL, v BLOB NOT NULL, PRIMARY KEY (q, seq))",
		ZDDL:      "CREATE TABLE IF NOT EXISTS z_items (z BLOB NOT NULL, k BLOB NOT NULL, s INTEGER NOT NULL, PRIMARY KEY (z, k))",
		ZIdxDDL:   "CREATE INDEX IF NOT EXISTS idx_z_items_s ON z_items (z, s, k)",
	}
	PostgresDialect = Dialect{
		Name:            "postgres",
		Ph:              func(n int) string { return "$" + strconv.Itoa(n) },
		KvUpsertTail:    "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
		KvSetExTail:     "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, expire_at = EXCLUDED.expire_at",
		IncrSeedTail:    "ON CONFLICT (k) DO NOTHING",
		ZSetUpsertTail:  "ON CONFLICT (z, k) DO UPDATE SET s = EXCLUDED.s",
		ZIncrUpsertTail: "ON CONFLICT (z, k) DO UPDATE SET s = z_items.s + EXCLUDED.s",
		QSeqIgnoreTail:  "ON CONFLICT (q) DO NOTHING",
		ForUpdate:       " FOR UPDATE",
		Returning:       true, // PostgreSQL 支持 RETURNING
		// 单语句 upsert：bytea 经 convert_from/convert_to 与文本互转；
		// 正则保证既有值是十进制整数，否则无匹配行 -> ErrNotInteger。
		// {2} 只出现一次：增量经 excluded.v 在两个分支间复用（文本形态参与运算）。
		IncrSQL: "INSERT INTO kv_items (k, v) VALUES ({1}, convert_to({2},'UTF8')) " +
			"ON CONFLICT (k) DO UPDATE SET v = convert_to((convert_from(kv_items.v,'UTF8')::bigint + convert_from(excluded.v,'UTF8')::bigint)::text,'UTF8') " +
			"WHERE convert_from(kv_items.v,'UTF8') ~ '^-?[0-9]+$' " +
			"RETURNING convert_from(kv_items.v,'UTF8')::bigint",
		KvDDL:     "CREATE TABLE IF NOT EXISTS kv_items (k BYTEA NOT NULL, v BYTEA NOT NULL, expire_at BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (k))",
		QSeqDDL:   "CREATE TABLE IF NOT EXISTS q_seq (q BYTEA NOT NULL, next BIGINT NOT NULL, prev BIGINT NOT NULL, PRIMARY KEY (q))",
		QItemsDDL: "CREATE TABLE IF NOT EXISTS q_items (q BYTEA NOT NULL, seq BIGINT NOT NULL, v BYTEA NOT NULL, PRIMARY KEY (q, seq))",
		ZDDL:      "CREATE TABLE IF NOT EXISTS z_items (z BYTEA NOT NULL, k BYTEA NOT NULL, s BIGINT NOT NULL, PRIMARY KEY (z, k))",
		ZIdxDDL:   "CREATE INDEX IF NOT EXISTS idx_z_items_s ON z_items (z, s, k)",
	}
)

// stmts 保存按方言参数化后的全部语句。
type stmts struct {
	kvUpsert     string
	kvSetEx      string
	kvGet        string
	kvExists     string
	kvDel        string
	kvIncrSelect string
	kvIncrSeed   string
	kvIncrUpdate string
	// kvIncrOne 是单语句自增（方言提供 IncrSQL 时使用）。
	kvIncrOne string
	kvExpire  string
	kvTTL     string
	kvCleanup string
	kvScan    func(hasStart, hasEnd bool) string
	kvMGet    func(n int) string

	qSeqEnsure string
	qSeqSelect string
	qSeqUpdate string
	// qSeqBumpBack / qSeqBumpFront 是 RETURNING 方言下的单语句合并形式：
	// 一条 UPDATE 同时推进 next/prev 并返回本次分配的 seq
	//（省去 SELECT ... FOR UPDATE 与独立 UPDATE 两次往返）。
	qSeqBumpBack  string
	qSeqBumpFront string
	qItemInsert   string
	qItemPop      func(desc bool) string
	qItemDelete   string
	qItemCount    string
	qItemPeek     func(desc bool) string

	zUpsert     func(incr bool) string
	zGet        string
	zDel        string
	zCount      string
	zRankMember string
	zRankCount  string
	zRange      string
	zIncrSelect string
}

// kvSetTemplate 生成 Set/SetEx 的 upsert 语句。当方言使用数值投影列（HasNumCol）时
// 语句多带一个 n 参数（由调用方在 Go 侧解析值得到，无法解析则传 NULL），
// 保证 Set 之后 Incr 的"非整数报错 / 整数累加"语义与 SSDB 一致。
// 注意：模板里每个 {n} 只出现一次，因为 build 会把每处引用展开成独立占位符。
func kvSetTemplate(d Dialect, withExpire bool) string {
	cols, vals := "k, v", "{1}, {2}"
	if withExpire {
		cols += ", expire_at"
		vals += ", {3}"
		conflict := d.KvSetExTail
		if d.HasNumCol {
			cols += ", n"
			vals += ", {4}"
			conflict += ", n = excluded.n"
		}
		return "INSERT INTO kv_items (" + cols + ") VALUES (" + vals + ") " + conflict
	}
	conflict := d.KvUpsertTail
	if d.HasNumCol {
		cols += ", n"
		vals += ", {3}"
		conflict += ", n = excluded.n"
	}
	return "INSERT INTO kv_items (" + cols + ") VALUES (" + vals + ") " + conflict
}

// qSeqBumpTemplate 生成"一条语句分配 seq"的 SQL：先确保 q_seq 行存在，
// 再用 UPDATE ... RETURNING 原子自增并返回新 seq。非 RETURNING 方言返回空串，
// qpush 走 qSeqEnsure + qSeqSelect(FOR UPDATE) + qSeqUpdate 的多语句路径。
//   - 队尾追加（back）：seq = next，随后 next = next + 1
//   - 队头插入（front）：prev = prev - 1，seq = prev
func qSeqBumpTemplate(d Dialect, front bool) string {
	if !d.Returning {
		return ""
	}
	// 一条 UPDATE 同时推进 next/prev，RETURNING 取本次分配的序号：
	//   - back：自增前的 next，即 RETURNING next - 1
	//   - front：自减后的 prev
	// RETURNING 引用的是更新后的行值（PostgreSQL/SQLite 语义一致），
	// 因此两条语句分别表达两个方向，避免在 SQL 里做布尔分支。
	if front {
		return "UPDATE q_seq SET next = next + 1, prev = prev - 1 WHERE q = {1} RETURNING prev"
	}
	return "UPDATE q_seq SET next = next + 1, prev = prev - 1 WHERE q = {1} RETURNING next - 1"
}

// build 用"花括号内序号"占位符构建参数化语句：{n} 表示第 n 个参数。
// 同一序号可重复出现（如 zRank 的 s < {2} OR s = {2}），在 MySQL/SQLite
// 下展开为重复的 ?，在 PG 下展开为重复的 $n，语义一致。
func build(d Dialect, tpl string) string {
	var sb strings.Builder
	sb.Grow(len(tpl) + 8)
	for i := 0; i < len(tpl); {
		if tpl[i] == '{' {
			j := i + 1
			for j < len(tpl) && tpl[j] != '}' {
				j++
			}
			if j < len(tpl) {
				if n, err := strconv.Atoi(tpl[i+1 : j]); err == nil && n >= 1 {
					sb.WriteString(d.Ph(n))
					i = j + 1
					continue
				}
			}
		}
		sb.WriteByte(tpl[i])
		i++
	}
	return sb.String()
}

func makeStmts(d Dialect) stmts {
	inList := func(n int) string {
		ps := make([]string, n)
		for i := range ps {
			ps[i] = d.Ph(i + 1)
		}
		return strings.Join(ps, ", ")
	}
	s := stmts{
		kvUpsert:     build(d, kvSetTemplate(d, false)),
		kvSetEx:      build(d, kvSetTemplate(d, true)),
		kvGet:        build(d, "SELECT v FROM kv_items WHERE k = {1} AND (expire_at = 0 OR expire_at > {2})"),
		kvExists:     build(d, "SELECT 1 FROM kv_items WHERE k = {1} AND (expire_at = 0 OR expire_at > {2})"),
		kvDel:        build(d, "DELETE FROM kv_items WHERE k = {1}"),
		kvIncrSelect: build(d, "SELECT v FROM kv_items WHERE k = {1}"+d.ForUpdate),
		kvIncrSeed:   build(d, "INSERT INTO kv_items (k, v) VALUES ({1}, '0') "+d.IncrSeedTail),
		kvIncrUpdate: build(d, "UPDATE kv_items SET v = {1} WHERE k = {2}"),
		kvIncrOne:    build(d, d.IncrSQL),
		kvExpire:     build(d, "UPDATE kv_items SET expire_at = {1} WHERE k = {2}"),
		kvTTL:        build(d, "SELECT expire_at FROM kv_items WHERE k = {1}"),
		kvCleanup:    build(d, "DELETE FROM kv_items WHERE expire_at > 0 AND expire_at <= {1}"),
		kvScan: func(hasStart, hasEnd bool) string {
			var conds []string
			i := 1
			if hasStart {
				conds = append(conds, "k >= "+d.Ph(i))
				i++
			}
			if hasEnd {
				conds = append(conds, "k <= "+d.Ph(i))
				i++
			}
			conds = append(conds, "(expire_at = 0 OR expire_at > "+d.Ph(i)+")")
			i++
			return "SELECT k, v FROM kv_items WHERE " + strings.Join(conds, " AND ") +
				" ORDER BY k LIMIT " + d.Ph(i)
		},
		kvMGet: func(n int) string {
			return "SELECT k, v FROM kv_items WHERE k IN (" + inList(n) +
				") AND (expire_at = 0 OR expire_at > " + d.Ph(n+1) + ")"
		},

		qSeqEnsure:    build(d, "INSERT INTO q_seq (q, next, prev) VALUES ({1}, 0, 0) "+d.QSeqIgnoreTail),
		qSeqSelect:    build(d, "SELECT next, prev FROM q_seq WHERE q = {1}"+d.ForUpdate),
		qSeqUpdate:    build(d, "UPDATE q_seq SET next = {1}, prev = {2} WHERE q = {3}"),
		qSeqBumpBack:  build(d, qSeqBumpTemplate(d, false)),
		qSeqBumpFront: build(d, qSeqBumpTemplate(d, true)),
		qItemInsert:   build(d, "INSERT INTO q_items (q, seq, v) VALUES ({1}, {2}, {3})"),
		qItemPop: func(desc bool) string {
			ord := "ASC"
			if desc {
				ord = "DESC"
			}
			return build(d, "SELECT seq, v FROM q_items WHERE q = {1} ORDER BY seq "+ord+" LIMIT 1"+d.ForUpdate)
		},
		qItemDelete: build(d, "DELETE FROM q_items WHERE q = {1} AND seq = {2}"),
		qItemCount:  build(d, "SELECT COUNT(*) FROM q_items WHERE q = {1}"),
		qItemPeek: func(desc bool) string {
			ord := "ASC"
			if desc {
				ord = "DESC"
			}
			return build(d, "SELECT v FROM q_items WHERE q = {1} ORDER BY seq "+ord+" LIMIT 1")
		},

		zUpsert: func(incr bool) string {
			tail := d.ZSetUpsertTail
			if incr {
				tail = d.ZIncrUpsertTail
			}
			return build(d, "INSERT INTO z_items (z, k, s) VALUES ({1}, {2}, {3}) "+tail)
		},
		zGet:        build(d, "SELECT s FROM z_items WHERE z = {1} AND k = {2}"),
		zDel:        build(d, "DELETE FROM z_items WHERE z = {1} AND k = {2}"),
		zCount:      build(d, "SELECT COUNT(*) FROM z_items WHERE z = {1}"),
		zRankMember: build(d, "SELECT s FROM z_items WHERE z = {1} AND k = {2}"),
		zRankCount:  build(d, "SELECT COUNT(*) FROM z_items WHERE z = {1} AND (s < {2} OR (s = {3} AND k < {4}))"),
		zRange:      build(d, "SELECT k, s FROM z_items WHERE z = {1} ORDER BY s ASC, k ASC LIMIT {2} OFFSET {3}"),
		zIncrSelect: build(d, "SELECT s FROM z_items WHERE z = {1} AND k = {2}"),
	}
	return s
}

// Provider 是 SQL 基座。并发安全由 database/sql 连接池与事务保障。
type Provider struct {
	mu              sync.Mutex
	writeMu         *sync.Mutex // 方言要求写串行化时非 nil（SQLite 单写者）
	db              *sql.DB
	st              stmts
	hasNumCol       bool // 方言是否使用数值投影列 n
	incrDeltaParams int  // 口语化命名；实际来自方言 IncrDeltaParams
	closed          bool
}

// New 在既有 *sql.DB 上构造基座并执行建表与过期清理。
func New(db *sql.DB, d Dialect) (*Provider, error) {
	// 空 DDL 跳过（如 MySQL 索引已内联建表）。
	for _, ddl := range []string{d.KvDDL, d.QSeqDDL, d.QItemsDDL, d.ZDDL, d.ZIdxDDL} {
		if ddl == "" {
			continue
		}
		if _, err := db.Exec(ddl); err != nil {
			return nil, fmt.Errorf("sqlstore: migrate (%s): %w", d.Name, err)
		}
	}
	ndp := d.IncrDeltaParams
	if ndp <= 0 {
		ndp = 1
	}
	p := &Provider{db: db, st: makeStmts(d), hasNumCol: d.HasNumCol, incrDeltaParams: ndp}
	if d.SerializeWrites {
		p.writeMu = &sync.Mutex{}
	}
	// 打开时兜底清理已过期行（运行期读取路径已过滤）。
	if _, err := db.Exec(p.st.kvCleanup, time.Now().Unix()); err != nil {
		return nil, fmt.Errorf("sqlstore: cleanup (%s): %w", d.Name, err)
	}
	return p, nil
}

func (p *Provider) check() error {
	if p.closed {
		return core.ErrClosed
	}
	return nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return p.db.Close()
}

// bs 把字符串键转 []byte，保证二进制安全的参数绑定。
func bs(s string) []byte { return []byte(s) }

// writeLock 返回一个释放函数：方言要求写串行化时加进程级写锁，
// 否则为空操作。所有写方法在进入时调用（SQLite 单写者排队）。
func (p *Provider) writeLock() func() {
	if p.writeMu == nil {
		return func() {}
	}
	p.writeMu.Lock()
	return p.writeMu.Unlock
}

// ---- KV ----

func (p *Provider) Set(ctx context.Context, key string, value []byte) error {
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	args := []any{bs(key), value}
	if p.hasNumCol {
		args = append(args, numProjection(value))
	}
	if _, err := p.db.ExecContext(ctx, p.st.kvUpsert, args...); err != nil {
		return fmt.Errorf("sqlstore: set: %w", err)
	}
	return nil
}

// numProjection 把值解析为数值投影列 n 的内容：十进制整数存数值，否则 NULL
// （NULL 让单语句 Incr 判定为"非整数"并报 ErrNotInteger）。
func numProjection(value []byte) any {
	if n, err := strconv.ParseInt(string(value), 10, 64); err == nil {
		return n
	}
	return nil
}

// SetEx 写入 value 并设置 TTL（单条 upsert 同时写值与 expire_at，
// 对应 Redis SETEX / SSDB setx）。
func (p *Provider) SetEx(ctx context.Context, key string, value []byte, ttl int64) error {
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	args := []any{bs(key), value, time.Now().Unix() + ttl}
	if p.hasNumCol {
		args = append(args, numProjection(value))
	}
	if _, err := p.db.ExecContext(ctx, p.st.kvSetEx, args...); err != nil {
		return fmt.Errorf("sqlstore: setex: %w", err)
	}
	return nil
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := p.check(); err != nil {
		return nil, false, err
	}
	var v []byte
	err := p.db.QueryRowContext(ctx, p.st.kvGet, bs(key), time.Now().Unix()).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("sqlstore: get: %w", err)
	}
	return v, true, nil
}

func (p *Provider) Del(ctx context.Context, key string) error {
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	if _, err := p.db.ExecContext(ctx, p.st.kvDel, bs(key)); err != nil {
		return fmt.Errorf("sqlstore: del: %w", err)
	}
	return nil
}

func (p *Provider) Exists(ctx context.Context, key string) (bool, error) {
	if err := p.check(); err != nil {
		return false, err
	}
	var one int
	err := p.db.QueryRowContext(ctx, p.st.kvExists, bs(key), time.Now().Unix()).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sqlstore: exists: %w", err)
	}
	return true, nil
}

func (p *Provider) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return 0, err
	}
	// 快路径：PostgreSQL/SQLite 用单语句 upsert + RETURNING 完成
	// 「缺失按 0 起算 + 原子累加 + 校验非整数 + 取回新值」，一条语句一次 fsync，
	// 实测约为事务多语句路径的 2 倍吞吐（见 README 性能一节）。
	if p.st.kvIncrOne != "" {
		return p.incrOne(ctx, key, delta)
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("sqlstore: incr begin: %w", err)
	}
	defer tx.Rollback()

	// 先占位插入（行不存在则建 '0'，存在则不动）：保证随后的 SELECT FOR UPDATE
	// 锁到的是已存在行——并发 Incr 对缺失键不会再互相持有 gap 锁而死锁。
	if _, err := tx.ExecContext(ctx, p.st.kvIncrSeed, bs(key)); err != nil {
		return 0, fmt.Errorf("sqlstore: incr seed: %w", err)
	}

	var raw []byte
	if err := tx.QueryRowContext(ctx, p.st.kvIncrSelect, bs(key)).Scan(&raw); err != nil {
		return 0, fmt.Errorf("sqlstore: incr select: %w", err)
	}
	cur, e := strconv.ParseInt(string(raw), 10, 64)
	if e != nil {
		return 0, core.ErrNotInteger
	}
	newVal := cur + delta
	if _, err := tx.ExecContext(ctx, p.st.kvIncrUpdate, strconv.FormatInt(newVal, 10), bs(key)); err != nil {
		return 0, fmt.Errorf("sqlstore: incr update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sqlstore: incr commit: %w", err)
	}
	return newVal, nil
}

// incrOne 是单语句自增快路径。方言的 IncrSQL 在既有值非十进制整数时
// 不匹配 WHERE（SQLite/PG），语句因此不返回任何行——据此映射 ErrNotInteger。
// 行被原子地加锁并更新，无需显式事务。
func (p *Provider) incrOne(ctx context.Context, key string, delta int64) (int64, error) {
	var newVal int64
	// 增量以文本形态传参：SQLite 的 CAST(? AS INTEGER/TEXT) 与
	// PG 的 ?::bigint 都能由文本隐式/显式转换，避免 pgx 对 int64->text 编码失败。
	ds := strconv.FormatInt(delta, 10)
	args := []any{bs(key), ds}
	for i := 1; i < p.incrDeltaParams; i++ {
		args = append(args, ds)
	}
	err := p.db.QueryRowContext(ctx, p.st.kvIncrOne, args...).Scan(&newVal)
	if errors.Is(err, sql.ErrNoRows) {
		// upsert 未命中：key 已存在且值不是十进制整数。
		return 0, core.ErrNotInteger
	}
	if err != nil {
		// PG 在转换失败等场景返回带类型的错误；统一归类为不可自增。
		if isNotIntegerErr(err) {
			return 0, core.ErrNotInteger
		}
		return 0, fmt.Errorf("sqlstore: incr: %w", err)
	}
	return newVal, nil
}

// isNotIntegerErr 识别驱动层暴露的数值转换失败（如 PG 的 22P02/invalid input syntax）。
func isNotIntegerErr(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "22p02") ||
		strings.Contains(msg, "invalid input syntax") ||
		strings.Contains(msg, "cannot cast") ||
		strings.Contains(msg, "out of range")
}

// mgetChunk 分批上限：IN 子句占位符控制在 SQLite/MySQL 变量上限内。
const mgetChunk = 100

func (p *Provider) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(keys))
	for i := 0; i < len(keys); i += mgetChunk {
		chunk := keys[i:min(i+mgetChunk, len(keys))]
		args := make([]any, 0, len(chunk)+1)
		for _, k := range chunk {
			args = append(args, bs(k))
		}
		args = append(args, time.Now().Unix())
		rows, err := p.db.QueryContext(ctx, p.st.kvMGet(len(chunk)), args...)
		if err != nil {
			return nil, fmt.Errorf("sqlstore: mget: %w", err)
		}
		for rows.Next() {
			var k []byte
			var v []byte
			if err := rows.Scan(&k, &v); err != nil {
				rows.Close()
				return nil, fmt.Errorf("sqlstore: mget scan: %w", err)
			}
			out[string(k)] = v
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (p *Provider) Scan(ctx context.Context, start, end string, limit int) ([]core.KeyValue, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	limit = normalizeLimit(limit)
	hasStart, hasEnd := start != "", end != ""
	q := p.st.kvScan(hasStart, hasEnd)
	args := make([]any, 0, 4)
	if hasStart {
		args = append(args, bs(start))
	}
	if hasEnd {
		args = append(args, bs(end))
	}
	args = append(args, time.Now().Unix(), limit)
	rows, err := p.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: scan: %w", err)
	}
	defer rows.Close()
	out := make([]core.KeyValue, 0, limit)
	for rows.Next() {
		var k, v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("sqlstore: scan row: %w", err)
		}
		out = append(out, core.KeyValue{Key: string(k), Value: v})
	}
	return out, rows.Err()
}

func (p *Provider) Expire(ctx context.Context, key string, ttl int64) error {
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if _, err := p.db.ExecContext(ctx, p.st.kvExpire, time.Now().Unix()+ttl, bs(key)); err != nil {
		return fmt.Errorf("sqlstore: expire: %w", err)
	}
	return nil
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	if err := p.check(); err != nil {
		return 0, false, err
	}
	var exp int64
	err := p.db.QueryRowContext(ctx, p.st.kvTTL, bs(key)).Scan(&exp)
	if errors.Is(err, sql.ErrNoRows) {
		return -1, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("sqlstore: ttl: %w", err)
	}
	if exp == 0 || exp <= time.Now().Unix() {
		return -1, false, nil
	}
	return exp - time.Now().Unix(), true, nil
}

// ---- Queue ----
//
// 序号模型：q_seq 每队列两个计数器——next（队尾追加，0,1,2...）与
// prev（队头插入，-1,-2...）。q_items 按 (q, seq) 主键排序即天然保持
// 队头->队尾顺序，前后插入共用同一顺序轴。

func (p *Provider) QPush(ctx context.Context, name string, value []byte) error {
	defer p.writeLock()()
	return p.qpush(ctx, name, value, false)
}

func (p *Provider) QPushFront(ctx context.Context, name string, value []byte) error {
	defer p.writeLock()()
	return p.qpush(ctx, name, value, true)
}

func (p *Provider) qpush(ctx context.Context, name string, value []byte, front bool) error {
	if err := p.check(); err != nil {
		return err
	}
	if p.st.qSeqBumpBack != "" {
		return p.qpushReturning(ctx, name, value, front)
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlstore: qpush begin: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, p.st.qSeqEnsure, bs(name)); err != nil {
		return fmt.Errorf("sqlstore: qpush ensure: %w", err)
	}
	var next, prev int64
	if err := tx.QueryRowContext(ctx, p.st.qSeqSelect, bs(name)).Scan(&next, &prev); err != nil {
		return fmt.Errorf("sqlstore: qpush seq: %w", err)
	}
	var seq int64
	if front {
		seq = prev - 1
		prev = seq
	} else {
		seq = next
		next = seq + 1
	}
	if _, err := tx.ExecContext(ctx, p.st.qSeqUpdate, next, prev, bs(name)); err != nil {
		return fmt.Errorf("sqlstore: qpush update seq: %w", err)
	}
	if _, err := tx.ExecContext(ctx, p.st.qItemInsert, bs(name), seq, value); err != nil {
		return fmt.Errorf("sqlstore: qpush insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlstore: qpush commit: %w", err)
	}
	return nil
}

// qpushReturning 是 PostgreSQL/SQLite 的快路径：事务内两条语句——
// UPDATE ... RETURNING 分配 seq（原子自增，无显式行锁往返），再插入数据行。
// 单条 UPDATE ... RETURNING 同时推进 next/prev 并按方向返回分配到的序号，
// 相比多语句路径省去 SELECT ... FOR UPDATE 与独立 UPDATE 两次往返。
func (p *Provider) qpushReturning(ctx context.Context, name string, value []byte, front bool) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlstore: qpush begin: %w", err)
	}
	defer tx.Rollback()

	// 保证 q_seq 行存在（该语句幂等，且与后续 UPDATE 同事务）。
	if _, err := tx.ExecContext(ctx, p.st.qSeqEnsure, bs(name)); err != nil {
		return fmt.Errorf("sqlstore: qpush ensure: %w", err)
	}
	bump := p.st.qSeqBumpBack
	if front {
		bump = p.st.qSeqBumpFront
	}
	var seq int64
	if err := tx.QueryRowContext(ctx, bump, bs(name)).Scan(&seq); err != nil {
		return fmt.Errorf("sqlstore: qpush bump: %w", err)
	}
	if _, err := tx.ExecContext(ctx, p.st.qItemInsert, bs(name), seq, value); err != nil {
		return fmt.Errorf("sqlstore: qpush insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlstore: qpush commit: %w", err)
	}
	return nil
}

func (p *Provider) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	defer p.writeLock()()
	return p.qpop(ctx, name, false)
}

func (p *Provider) QPopBack(ctx context.Context, name string) ([]byte, bool, error) {
	defer p.writeLock()()
	return p.qpop(ctx, name, true)
}

func (p *Provider) qpop(ctx context.Context, name string, back bool) ([]byte, bool, error) {
	if err := p.check(); err != nil {
		return nil, false, err
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("sqlstore: qpop begin: %w", err)
	}
	defer tx.Rollback()

	var seq int64
	var v []byte
	err = tx.QueryRowContext(ctx, p.st.qItemPop(back), bs(name)).Scan(&seq, &v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("sqlstore: qpop select: %w", err)
	}
	if _, err := tx.ExecContext(ctx, p.st.qItemDelete, bs(name), seq); err != nil {
		return nil, false, fmt.Errorf("sqlstore: qpop delete: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("sqlstore: qpop commit: %w", err)
	}
	return v, true, nil
}

func (p *Provider) QSize(ctx context.Context, name string) (int64, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
	var n int64
	if err := p.db.QueryRowContext(ctx, p.st.qItemCount, bs(name)).Scan(&n); err != nil {
		return 0, fmt.Errorf("sqlstore: qsize: %w", err)
	}
	return n, nil
}

func (p *Provider) QFront(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpeek(ctx, name, false)
}

func (p *Provider) QBack(ctx context.Context, name string) ([]byte, bool, error) {
	return p.qpeek(ctx, name, true)
}

func (p *Provider) qpeek(ctx context.Context, name string, back bool) ([]byte, bool, error) {
	if err := p.check(); err != nil {
		return nil, false, err
	}
	var v []byte
	err := p.db.QueryRowContext(ctx, p.st.qItemPeek(back), bs(name)).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("sqlstore: qpeek: %w", err)
	}
	return v, true, nil
}

// ---- ZSet ----

func (p *Provider) ZSet(ctx context.Context, name, key string, score int64) error {
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	if _, err := p.db.ExecContext(ctx, p.st.zUpsert(false), bs(name), bs(key), score); err != nil {
		return fmt.Errorf("sqlstore: zset: %w", err)
	}
	return nil
}

func (p *Provider) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	if err := p.check(); err != nil {
		return 0, false, err
	}
	var s int64
	err := p.db.QueryRowContext(ctx, p.st.zGet, bs(name), bs(key)).Scan(&s)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("sqlstore: zget: %w", err)
	}
	return s, true, nil
}

func (p *Provider) ZDel(ctx context.Context, name, key string) error {
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	if _, err := p.db.ExecContext(ctx, p.st.zDel, bs(name), bs(key)); err != nil {
		return fmt.Errorf("sqlstore: zdel: %w", err)
	}
	return nil
}

func (p *Provider) ZSize(ctx context.Context, name string) (int64, error) {
	if err := p.check(); err != nil {
		return 0, err
	}
	var n int64
	if err := p.db.QueryRowContext(ctx, p.st.zCount, bs(name)).Scan(&n); err != nil {
		return 0, fmt.Errorf("sqlstore: zsize: %w", err)
	}
	return n, nil
}

func (p *Provider) ZRank(ctx context.Context, name, key string) (int64, bool, error) {
	if err := p.check(); err != nil {
		return 0, false, err
	}
	var s int64
	err := p.db.QueryRowContext(ctx, p.st.zRankMember, bs(name), bs(key)).Scan(&s)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("sqlstore: zrank member: %w", err)
	}
	var rank int64
	// 参数按展开后的占位符顺序排列：z={1}, s={2}, s={3}（{2} 重复，需再传一次）, k={4}。
	if err := p.db.QueryRowContext(ctx, p.st.zRankCount, bs(name), s, s, bs(key)).Scan(&rank); err != nil {
		return 0, false, fmt.Errorf("sqlstore: zrank count: %w", err)
	}
	return rank, true, nil
}

func (p *Provider) ZRange(ctx context.Context, name string, start, stop int64) ([]core.ZItem, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	// 负索引需先取集合大小归一化。
	if start < 0 || stop < 0 {
		var n int64
		if err := p.db.QueryRowContext(ctx, p.st.zCount, bs(name)).Scan(&n); err != nil {
			return nil, fmt.Errorf("sqlstore: zrange size: %w", err)
		}
		if start < 0 {
			start = n + start
			if start < 0 {
				start = 0
			}
		}
		if stop < 0 {
			stop = n + stop
		}
	}
	if stop < start || start < 0 {
		return []core.ZItem{}, nil
	}
	offset := start
	limit := stop - start + 1
	rows, err := p.db.QueryContext(ctx, p.st.zRange, bs(name), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: zrange: %w", err)
	}
	defer rows.Close()
	out := make([]core.ZItem, 0, limit)
	for rows.Next() {
		var k []byte
		var s int64
		if err := rows.Scan(&k, &s); err != nil {
			return nil, fmt.Errorf("sqlstore: zrange row: %w", err)
		}
		out = append(out, core.ZItem{Key: string(k), Score: s})
	}
	return out, rows.Err()
}

func (p *Provider) ZIncr(ctx context.Context, name, key string, delta int64) (int64, error) {
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return 0, err
	}
	if _, err := p.db.ExecContext(ctx, p.st.zUpsert(true), bs(name), bs(key), delta); err != nil {
		return 0, fmt.Errorf("sqlstore: zincr: %w", err)
	}
	var s int64
	if err := p.db.QueryRowContext(ctx, p.st.zIncrSelect, bs(name), bs(key)).Scan(&s); err != nil {
		return 0, fmt.Errorf("sqlstore: zincr select: %w", err)
	}
	return s, nil
}

// normalizeLimit 与 mem/ssdb/redis 基座相同的默认页大小约定。
func normalizeLimit(limit int) int {
	const defaultLimit = 100 // 与 core.DefaultScanLimit 对齐
	if limit <= 0 {
		return defaultLimit
	}
	return limit
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
