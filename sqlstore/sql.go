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
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/RelicOfTesla/kvdb/core"
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
	// KvUpsertTail 是 kv_items 的 Set（不带 TTL）冲突子句：覆盖 v，并在既有行
	// **已过期**时把 expire_at 归零——否则 Set 成功了键仍会被读路径当过期过滤掉。
	// `{now}` 由 makeStmts 替换为该方言"当前 unix 秒"参数的占位符。
	KvUpsertTail string
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
	// IncrSQL 是单语句原子自增（缺失按 0 起算、非整数报错、已过期按不存在）的
	// 完整语句模板；空串表示该方言不具备单语句形态，Incr 走事务多语句路径。
	// 占位符：{1}=key, {2}=delta, {3}=当前 unix 秒。
	IncrSQL string
	// HasNumCol 表示 kv_items 有数值投影列 n（供单语句 Incr 使用）；
	// 为 true 时 Set/SetEx 必须同步维护 n，否则 Incr 会误判为非整数。
	HasNumCol bool
	// MaxKeyLen 是方言对 key/队列名/zset 名/成员的字节长度上限（0 = 不限）。
	// MySQL 的 VARBINARY(255)：strict 模式超限报 1406，非 strict 模式静默截断
	// 会导致键碰撞——由写入侧显式校验并报错。
	MaxKeyLen int
	// SerializeWrites 为 true 时基座在进程内串行化全部写操作（单写者模型）。
	// SQLite 需要：多连接并发写即使有 busy_timeout 也会在持续竞争下报
	// SQLITE_BUSY；进程级写锁把竞争变成排队，读仍由 WAL 并行。
	SerializeWrites bool
	// TablePrefix 加在四张表名之前（如 "kvdb_" -> kvdb_kv_items），用于与其他应用
	// 共用一个库/schema。空串表示用默认表名。索引名同样带前缀，不会与别的表的索引冲突。
	TablePrefix string
	// LimitStyle 控制"取前 n 行"的渲染方式：
	//   ""（默认）     -> SELECT ... ORDER BY ... LIMIT {n}
	//   "top"（MSSQL） -> SELECT TOP ({n}) ... ORDER BY ...
	LimitStyle string
	// DDL（占位符无需参数）。
	KvDDL, QSeqDDL, QItemsDDL, ZDDL, ZIdxDDL string
	// StmtOverrides 是"整条语句覆盖"：MSSQL 的 upsert 是 MERGE、行锁是
	// WITH (UPDLOCK, HOLDLOCK)、分页是 OFFSET ... FETCH NEXT，都无法用
	// "单一冲突子句 / 单一 FOR UPDATE 后缀"表达，这里按语句名整体替换。
	// 模板仍是 {1}..{n} 占位符风格；{now} 照常经 withNowParam 替换。
	// 键名与被替换对象一一对应：
	//   kvUpsert / kvSetEx          —— Set / SetEx / SetExAt 的单条 upsert
	//   kvIncrSeed / kvIncrSelect   —— Incr 事务路径的占位与锁行
	//   qSeqEnsure / qSeqSelect     —— 队列计数器的占位与锁
	//   qItemPop / qItemPopBack     —— 出队（队头 / 队尾）
	//   qItemPeek / qItemPeekBack   —— 窥视（队头 / 队尾）
	//   zUpsert / zIncrUpsert       —— ZSet 覆盖写 / 累加写
	//   zRange                      —— ZRange（分页占位符序与 LIMIT 相反）
	StmtOverrides map[string]string
}

// mysqlKeyLen 是 MySQL 键列（k / q / z）的字节上限，mysqlKeyCol 是由它生成的列类型。
// 运行期长度校验（Dialect.MaxKeyLen）与建表语句的列宽必须严格一致：列比校验窄时，
// 校验放行的键会在 strict 模式报 1406、非 strict 模式被静默截断。故两者共用此常量。
const mysqlKeyLen = 255

var mysqlKeyCol = fmt.Sprintf("VARBINARY(%d)", mysqlKeyLen)

// StmtOverrides 的键名常量（与 stmts 字段一一对应，见 Dialect.StmtOverrides 注释）。
const (
	kvUpsertKey      = "kvUpsert"
	kvSetExStmt      = "kvSetEx"
	kvIncrSeedKey    = "kvIncrSeed"
	kvIncrSelectKey  = "kvIncrSelect"
	kvIncrUpdateKey  = "kvIncrUpdate"
	qSeqEnsureKey    = "qSeqEnsure"
	qSeqSelectKey    = "qSeqSelect"
	qItemPopKey      = "qItemPop"
	qItemPopBackKey  = "qItemPopBack"
	qItemPeekKey     = "qItemPeek"
	qItemPeekBackKey = "qItemPeekBack"
	zUpsertKey       = "zUpsert"
	zIncrUpsertKey   = "zIncrUpsert"
	zRangeKey        = "zRange"
	// zRangeByScoreKey / zRangeByScoreDescKey 与 zRangeKey 同理：两个方向的 ORDER BY
	// 无法用"同一模板 + 后缀"表达（LIMIT 与 ORDER BY 的相对位置在方言间不同），
	// 故各占一个键、分别整句覆盖。占位符序固定为
	// {1}=name、{2}=min、{3}=max、{4}=limit；limit<=0 时用无限版语句、无 {4}。
	// LIMIT 后置（MySQL/SQLite/PG）与 OFFSET/FETCH（MSSQL）分别由默认模板与
	// StmtOverrides 提供，见 makeStmts 与 MSSQLDialect。
	zRangeByScoreKey          = "zRangeByScore"
	zRangeByScoreDescKey      = "zRangeByScoreDesc"
	zRangeByScoreLimitKey     = "zRangeByScoreLimit"
	zRangeByScoreDescLimitKey = "zRangeByScoreDescLimit"

	// qRangeKey 与 zRangeKey 同理：LIMIT 后置方言无法用"同一模板 + 后缀"表达，
	// 故也允许整句覆盖（占位符序同为 {1}=name、{2}=limit、{3}=offset）。
	qRangeKey = "qRange"
)

// MySQLDialect / SQLiteDialect / PostgresDialect 是三个内置方言，
// 供 kvdb/mysql、kvdb/sqlite、kvdb/pg 包装包实例化本基座。
var (
	MySQLDialect = Dialect{
		Name: "mysql",
		Ph:   func(n int) string { return "?" },
		KvUpsertTail: "ON DUPLICATE KEY UPDATE v = VALUES(v), " +
			"expire_at = IF(kv_items.expire_at > 0 AND kv_items.expire_at <= {now}, 0, kv_items.expire_at)",
		KvSetExTail:     "ON DUPLICATE KEY UPDATE v = VALUES(v), expire_at = VALUES(expire_at)",
		IncrSeedTail:    "ON DUPLICATE KEY UPDATE v = v",
		ZSetUpsertTail:  "ON DUPLICATE KEY UPDATE s = VALUES(s)",
		ZIncrUpsertTail: "ON DUPLICATE KEY UPDATE s = s + VALUES(s)",
		QSeqIgnoreTail:  "ON DUPLICATE KEY UPDATE next = next",
		ForUpdate:       " FOR UPDATE",
		Returning:       false, // MySQL 无 UPDATE ... RETURNING（走 qSeq/事务路径）
		MaxKeyLen:       mysqlKeyLen,
		KvDDL:           "CREATE TABLE IF NOT EXISTS kv_items (k " + mysqlKeyCol + " NOT NULL, v LONGBLOB NOT NULL, expire_at BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (k))",
		QSeqDDL:         "CREATE TABLE IF NOT EXISTS q_seq (q " + mysqlKeyCol + " NOT NULL, next BIGINT NOT NULL, prev BIGINT NOT NULL, PRIMARY KEY (q))",
		QItemsDDL:       "CREATE TABLE IF NOT EXISTS q_items (q " + mysqlKeyCol + " NOT NULL, seq BIGINT NOT NULL, v LONGBLOB NOT NULL, PRIMARY KEY (q, seq))",
		// MySQL 无 CREATE INDEX IF NOT EXISTS（不幂等），索引内联在 CREATE TABLE
		// 的 KEY 子句；ZIdxDDL 留空让 New 跳过独立索引 DDL（见表 DDL 注释）。
		ZDDL:    "CREATE TABLE IF NOT EXISTS z_items (z " + mysqlKeyCol + " NOT NULL, k " + mysqlKeyCol + " NOT NULL, s BIGINT NOT NULL, PRIMARY KEY (z, k), KEY idx_z_items_s (z, s, k))",
		ZIdxDDL: "",
	}
	SQLiteDialect = Dialect{
		Name: "sqlite",
		Ph:   func(n int) string { return "?" },
		KvUpsertTail: "ON CONFLICT(k) DO UPDATE SET v = excluded.v, " +
			"expire_at = CASE WHEN kv_items.expire_at > 0 AND kv_items.expire_at <= {now} " +
			"THEN 0 ELSE kv_items.expire_at END",
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
		HasNumCol: true,
		// 单语句 upsert：n 列缓存数值投影，v 列同步物化为十进制文本。
		// expired 分支（expire_at 非 0 且 <= {3}）把键按"不存在"处理：以增量本身为
		// 基数并把 expire_at 归零；非 expired 分支要求 n NOT NULL 且 n+增量不溢出
		// int64——SQLite 整数加法溢出会静默转 REAL 并永久腐蚀该键，这里在 WHERE
		// 中拒绝（无匹配行 -> RETURNING 无结果 -> 映射 ErrNotInteger，行保持原值）。
		// {1}=key、{2}=增量、{3}=当前 unix 秒。
		IncrSQL: "INSERT INTO kv_items (k, v, n, expire_at) VALUES ({1}, CAST({2} AS TEXT), {2}, 0) " +
			"ON CONFLICT(k) DO UPDATE SET " +
			"v = CASE WHEN kv_items.expire_at > 0 AND kv_items.expire_at <= {3} " +
			"THEN CAST(excluded.n AS TEXT) ELSE CAST(kv_items.n + excluded.n AS TEXT) END, " +
			"n = CASE WHEN kv_items.expire_at > 0 AND kv_items.expire_at <= {3} " +
			"THEN excluded.n ELSE kv_items.n + excluded.n END, " +
			"expire_at = CASE WHEN kv_items.expire_at > 0 AND kv_items.expire_at <= {3} " +
			"THEN 0 ELSE kv_items.expire_at END " +
			"WHERE (kv_items.expire_at > 0 AND kv_items.expire_at <= {3}) " +
			"OR (kv_items.n IS NOT NULL AND kv_items.n + excluded.n BETWEEN -9223372036854775807 - 1 AND 9223372036854775807) " +
			"RETURNING kv_items.n",
		KvDDL: "CREATE TABLE IF NOT EXISTS kv_items (k BLOB NOT NULL, v BLOB NOT NULL, n INTEGER NULL, " +
			"expire_at INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (k))",
		QSeqDDL:   "CREATE TABLE IF NOT EXISTS q_seq (q BLOB NOT NULL, next INTEGER NOT NULL, prev INTEGER NOT NULL, PRIMARY KEY (q))",
		QItemsDDL: "CREATE TABLE IF NOT EXISTS q_items (q BLOB NOT NULL, seq INTEGER NOT NULL, v BLOB NOT NULL, PRIMARY KEY (q, seq))",
		ZDDL:      "CREATE TABLE IF NOT EXISTS z_items (z BLOB NOT NULL, k BLOB NOT NULL, s INTEGER NOT NULL, PRIMARY KEY (z, k))",
		ZIdxDDL:   "CREATE INDEX IF NOT EXISTS idx_z_items_s ON z_items (z, s, k)",
	}
	PostgresDialect = Dialect{
		Name: "postgres",
		Ph:   func(n int) string { return "$" + strconv.Itoa(n) },
		KvUpsertTail: "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, " +
			"expire_at = CASE WHEN kv_items.expire_at > 0 AND kv_items.expire_at <= {now} " +
			"THEN 0 ELSE kv_items.expire_at END",
		KvSetExTail:     "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, expire_at = EXCLUDED.expire_at",
		IncrSeedTail:    "ON CONFLICT (k) DO NOTHING",
		ZSetUpsertTail:  "ON CONFLICT (z, k) DO UPDATE SET s = EXCLUDED.s",
		ZIncrUpsertTail: "ON CONFLICT (z, k) DO UPDATE SET s = z_items.s + EXCLUDED.s",
		QSeqIgnoreTail:  "ON CONFLICT (q) DO NOTHING",
		ForUpdate:       " FOR UPDATE",
		Returning:       true, // PostgreSQL 支持 RETURNING
		// 单语句 upsert：bytea 经 convert_from/convert_to 与文本互转。
		// expired 分支（expire_at 非 0 且 <= {3}）把键按"不存在"处理：以增量本身
		// 为基数并把 expire_at 归零；非 expired 分支用正则保证既有值是十进制整数，
		// 否则无匹配行 -> ErrNotInteger。
		// {2} 只出现一次：增量经 excluded.v 在两个分支间复用（文本形态参与运算）；
		// {3}=当前 unix 秒。
		IncrSQL: "INSERT INTO kv_items (k, v, expire_at) VALUES ({1}, convert_to({2},'UTF8'), 0) " +
			"ON CONFLICT (k) DO UPDATE SET " +
			"v = convert_to((CASE WHEN kv_items.expire_at > 0 AND kv_items.expire_at <= {3} " +
			"THEN convert_from(excluded.v,'UTF8')::bigint " +
			"ELSE convert_from(kv_items.v,'UTF8')::bigint + convert_from(excluded.v,'UTF8')::bigint END)::text,'UTF8'), " +
			"expire_at = CASE WHEN kv_items.expire_at > 0 AND kv_items.expire_at <= {3} THEN 0 ELSE kv_items.expire_at END " +
			"WHERE (kv_items.expire_at > 0 AND kv_items.expire_at <= {3}) " +
			"OR convert_from(kv_items.v,'UTF8') ~ '^-?[0-9]+$' " +
			"RETURNING convert_from(kv_items.v,'UTF8')::bigint",
		KvDDL:     "CREATE TABLE IF NOT EXISTS kv_items (k BYTEA NOT NULL, v BYTEA NOT NULL, expire_at BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (k))",
		QSeqDDL:   "CREATE TABLE IF NOT EXISTS q_seq (q BYTEA NOT NULL, next BIGINT NOT NULL, prev BIGINT NOT NULL, PRIMARY KEY (q))",
		QItemsDDL: "CREATE TABLE IF NOT EXISTS q_items (q BYTEA NOT NULL, seq BIGINT NOT NULL, v BYTEA NOT NULL, PRIMARY KEY (q, seq))",
		ZDDL:      "CREATE TABLE IF NOT EXISTS z_items (z BYTEA NOT NULL, k BYTEA NOT NULL, s BIGINT NOT NULL, PRIMARY KEY (z, k))",
		ZIdxDDL:   "CREATE INDEX IF NOT EXISTS idx_z_items_s ON z_items (z, s, k)",
	}
	// mssqlKeyLen 取 255：MSSQL 索引键上限 900 字节，键列参与主键（聚集索引）与
	// 二级索引，255 留有充足余量，并给出与 MySQL 相同的写入期校验口径。
	MSSQLDialect = Dialect{
		Name: "mssql",
		// go-mssqldb 原生占位符是 @pN："sqlserver" 注册名下 processQueryText=false，
		// 驱动不做 "?" 重写（只有 "mssql" 注册名才重写），因此必须直接命名。
		// 命名参数可重复引用（build 对重复 {n} 输出同一 @pN），按序号绑定。
		Ph: func(n int) string { return "@p" + strconv.Itoa(n) },
		// upsert/行锁/分页无法收敛进"单一冲突子句"，全部走整条语句覆盖（见 StmtOverrides）。
		Returning: false, // MSSQL 无 UPDATE ... RETURNING，Incr 走事务路径（溢出在 Go 侧显式检查）
		StmtOverrides: map[string]string{
			// MERGE 语法要求以分号结束，所有 MERGE 覆盖模板的末行都带 ";"。
			kvUpsertKey: "MERGE kv_items WITH (HOLDLOCK) AS t " +
				"USING (VALUES ({1}, {2}, {3})) AS src(k, v, now) ON t.k = src.k " +
				// 与 MySQL/PG 的 KvUpsertTail 同义：既有行已过期时把 expire_at 归零，
				// 否则 Set 成功了键仍会被读路径当过期过滤掉。
				"WHEN MATCHED THEN UPDATE SET v = src.v, " +
				"expire_at = CASE WHEN t.expire_at > 0 AND t.expire_at <= src.now THEN 0 ELSE t.expire_at END " +
				"WHEN NOT MATCHED THEN INSERT (k, v, expire_at) VALUES (src.k, src.v, 0);", // 新增行 expire_at 恒为 0（无 TTL），与 INSERT 省略 expire_at 列的方言一致
			kvSetExStmt: "MERGE kv_items WITH (HOLDLOCK) AS t " +
				"USING (VALUES ({1}, {2}, {3})) AS src(k, v, expire_at) ON t.k = src.k " +
				"WHEN MATCHED THEN UPDATE SET v = src.v, expire_at = src.expire_at " +
				"WHEN NOT MATCHED THEN INSERT (k, v, expire_at) VALUES (src.k, src.v, src.expire_at);",
			kvIncrSeedKey: "MERGE kv_items WITH (HOLDLOCK) AS t " +
				// 占位值 '0' 用二进制字面量避免隐式 varchar->varbinary 转换歧义。
				"USING (VALUES ({1}, CAST(0x30 AS VARBINARY(MAX)))) AS src(k, v) ON t.k = src.k " +
				// 已存在则原值写回（自赋值是合法 no-op），保证后续 UPDLOCK SELECT 锁到已存在行。
				"WHEN MATCHED THEN UPDATE SET v = t.v " +
				"WHEN NOT MATCHED THEN INSERT (k, v) VALUES (src.k, src.v);",
			kvIncrSelectKey: "SELECT v, expire_at FROM kv_items WITH (UPDLOCK, HOLDLOCK) WHERE k = {1}",
			// v 列是 varbinary，绑定字符串需显式 CONVERT（MSSQL 不允许 nvarchar -> varbinary 隐式转换）。
			kvIncrUpdateKey: "UPDATE kv_items SET v = CONVERT(VARBINARY(MAX), {1}), expire_at = {2} WHERE k = {3}",
			qSeqEnsureKey: "MERGE q_seq WITH (HOLDLOCK) AS t " +
				"USING (VALUES ({1}, 0, 0)) AS src(q, next, prev) ON t.q = src.q " +
				"WHEN MATCHED THEN UPDATE SET next = t.next, prev = t.prev " +
				"WHEN NOT MATCHED THEN INSERT (q, next, prev) VALUES (src.q, 0, 0);",
			qSeqSelectKey: "SELECT next, prev FROM q_seq WITH (UPDLOCK, HOLDLOCK) WHERE q = {1}",
			qItemPopKey:   "SELECT seq, v FROM q_items WITH (UPDLOCK, HOLDLOCK) WHERE q = {1} ORDER BY seq ASC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY",
			qItemPopBackKey: "SELECT seq, v FROM q_items WITH (UPDLOCK, HOLDLOCK) WHERE q = {1} " +
				"ORDER BY seq DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY",
			qItemPeekKey:     "SELECT v FROM q_items WHERE q = {1} ORDER BY seq ASC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY",
			qItemPeekBackKey: "SELECT v FROM q_items WHERE q = {1} ORDER BY seq DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY",
			// 与 zRangeKey 同形：MSSQL 2012+ 无 LIMIT，占位符序同样是
			// {1}=name、{2}=limit、{3}=offset（模板里 OFFSET 先出现），
			// 参数则由调用方按 (name, limit, offset) 传，build 已固化其序号。
			qRangeKey: "SELECT v FROM q_items WHERE q = {1} ORDER BY seq ASC " +
				"OFFSET {3} ROWS FETCH NEXT {2} ROWS ONLY",
			zUpsertKey: "MERGE z_items WITH (HOLDLOCK) AS t " +
				"USING (VALUES ({1}, {2}, {3})) AS src(z, k, sc) ON t.z = src.z AND t.k = src.k " +
				"WHEN MATCHED THEN UPDATE SET s = src.sc " +
				"WHEN NOT MATCHED THEN INSERT (z, k, s) VALUES (src.z, src.k, src.sc);",
			zIncrUpsertKey: "MERGE z_items WITH (HOLDLOCK) AS t " +
				"USING (VALUES ({1}, {2}, {3})) AS src(z, k, delta) ON t.z = src.z AND t.k = src.k " +
				"WHEN MATCHED THEN UPDATE SET s = t.s + src.delta " +
				"WHEN NOT MATCHED THEN INSERT (z, k, s) VALUES (src.z, src.k, src.delta);",
			zRangeKey: "SELECT k, s FROM z_items WHERE z = {1} ORDER BY s ASC, k ASC " +
				"OFFSET {3} ROWS FETCH NEXT {2} ROWS ONLY",
			// ZRangeByScore 两个方向各一条：MSSQL 2012+ 无 LIMIT，取前 n 行只能
			// 用 OFFSET ... FETCH NEXT。limit<=0（不限）时为 NULL 语义，这里把
			// OFFSET 写 0、FETCH NEXT 2147483647 —— SQL Server 的 FETCH NEXT
			// 不接受 NULL，用 INT 上限兜底即"实际不限"（z_items 行数远低于此）。
			// 注意方向只翻转 s，k 恒为 ASC：同分成员按成员字节序升序，desc 时不翻转。
			zRangeByScoreKey: "SELECT k, s FROM z_items WHERE z = {1} AND s >= {2} AND s <= {3} " +
				"ORDER BY s ASC, k ASC OFFSET 0 ROWS FETCH NEXT 2147483647 ROWS ONLY",
			zRangeByScoreDescKey: "SELECT k, s FROM z_items WHERE z = {1} AND s >= {2} AND s <= {3} " +
				"ORDER BY s DESC, k ASC OFFSET 0 ROWS FETCH NEXT 2147483647 ROWS ONLY",
			// 含 limit 的两条（{4} = limit 行数；OFFSET/FETCH 是 MSSQL 唯一的取前 n 行写法）：
			zRangeByScoreLimitKey: "SELECT k, s FROM z_items WHERE z = {1} AND s >= {2} AND s <= {3} " +
				"ORDER BY s ASC, k ASC OFFSET 0 ROWS FETCH NEXT {4} ROWS ONLY",
			zRangeByScoreDescLimitKey: "SELECT k, s FROM z_items WHERE z = {1} AND s >= {2} AND s <= {3} " +
				"ORDER BY s DESC, k ASC OFFSET 0 ROWS FETCH NEXT {4} ROWS ONLY",
		},
		// 键列 VARBINARY(255)：与 MySQL 同一口径（255 字节内索引安全，写入期校验超长）。
		MaxKeyLen:  255,
		LimitStyle: "top",
		// MSSQL 无 CREATE TABLE IF NOT EXISTS：用 IF OBJECT_ID 守护；表名在
		// 单引号内仍是独立标识符（前后是引号），applyTablePrefix 的 replaceIdent
		// 会正常加上前缀。KEY 列 VARBINARY(255)，值列 VARBINARY(MAX)。
		KvDDL:     "IF OBJECT_ID(N'kv_items') IS NULL CREATE TABLE kv_items (k VARBINARY(255) NOT NULL, v VARBINARY(MAX) NOT NULL, expire_at BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (k))",
		QSeqDDL:   "IF OBJECT_ID(N'q_seq') IS NULL CREATE TABLE q_seq (q VARBINARY(255) NOT NULL, next BIGINT NOT NULL, prev BIGINT NOT NULL, PRIMARY KEY (q))",
		QItemsDDL: "IF OBJECT_ID(N'q_items') IS NULL CREATE TABLE q_items (q VARBINARY(255) NOT NULL, seq BIGINT NOT NULL, v VARBINARY(MAX) NOT NULL, PRIMARY KEY (q, seq))",
		ZDDL:      "IF OBJECT_ID(N'z_items') IS NULL CREATE TABLE z_items (z VARBINARY(255) NOT NULL, k VARBINARY(255) NOT NULL, s BIGINT NOT NULL, PRIMARY KEY (z, k))",
		ZIdxDDL:   "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'idx_z_items_s') CREATE INDEX idx_z_items_s ON z_items (z, s, k)",
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
	qRange        string

	zUpsert     func(incr bool) string
	zGet        string
	zDel        string
	zCount      string
	zRankMember string
	zRankCount  string
	zRange      string
	// zRangeByScore 取"按分数闭区间"的结果。limit <= 0 表示不限，此时走
	// 无 LIMIT 的语句（绝不把 0/负数当 LIMIT 传给 SQL）；desc 只决定用哪条
	// 整句覆盖（方向只翻转 s，k 恒为 ASC），见 StmtOverrides 的键注释。
	zRangeByScore func(desc, hasLimit bool) string
	zIncrSelect   string
	// zIncrOne 是单语句原子自增 + RETURNING 新分数（方言支持 Returning 时），
	// 消除"upsert 后另起语句回读"在并发下返回值不对应该次调用的竞态。
	zIncrOne string
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
	return applyTablePrefix(d, sb.String())
}

// tableNames 是 sqlstore 使用的四张表；applyTablePrefix 在 SQL 文本里把它们替换成
// 带前缀的形式。只匹配左右都不是标识符字符的位置，因此 "idx_z_items_s" 这类派生名
// 不会被误改（它左侧是 "_"，属于标识符字符）。
var tableNames = []string{"kv_items", "q_seq", "q_items", "z_items"}

func applyTablePrefix(d Dialect, sql string) string {
	if d.TablePrefix == "" {
		return sql
	}
	// 索引名先单独处理：它含表名子串但不是独立标识符（左邻 "z_"），
	// 不先替换会被下面的表名替换漏掉，导致同库多前缀互相撞索引名。
	sql = strings.ReplaceAll(sql, indexName, d.TablePrefix+indexName)
	for _, t := range tableNames {
		sql = replaceIdent(sql, t, d.TablePrefix+t)
	}
	return sql
}

// indexName 是 z_items(z,s,k) 的二级索引名（MySQL 内联在 DDL，其余用 CREATE INDEX）。
const indexName = "idx_z_items_s"

// replaceIdent 把 s 中作为独立标识符出现的 from 换成 to。
func replaceIdent(s, from, to string) string {
	isIdent := func(b byte) bool {
		return b == '_' || b == '$' || (b >= '0' && b <= '9') ||
			(b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
	}
	var sb strings.Builder
	for i := 0; i < len(s); {
		j := strings.Index(s[i:], from)
		if j < 0 {
			sb.WriteString(s[i:])
			break
		}
		start, end := i+j, i+j+len(from)
		leftOK := start == 0 || !isIdent(s[start-1])
		rightOK := end == len(s) || !isIdent(s[end])
		sb.WriteString(s[i:start])
		if leftOK && rightOK {
			sb.WriteString(to)
		} else {
			sb.WriteString(from)
		}
		i = end
	}
	return sb.String()
}

// withNowParam 把模板中的 {now} 替换为该方言"当前 unix 秒"参数的占位符。
// Set 语句的参数顺序固定为 k, v[, n]，因此 now 的序号是 3（无 n 列）或 4（有 n 列）。
// {now} 不是纯数字序号，build 会原样保留，正好留到这里替换。
func withNowParam(d Dialect, sql string) string {
	idx := 3
	if d.HasNumCol {
		idx = 4
	}
	return strings.ReplaceAll(sql, "{now}", d.Ph(idx))
}

// expandArgs 按模板中 {n} 的出现顺序装配实参：位置占位符方言（?）重复引用同一
// 参数序号时必须重复传值；命名占位符方言（PG 的 $n）每个参数只传一次。
// 这样 IncrSQL 可以自由地多次引用 key/delta/now 而不必再维护"重复次数"常量。
func expandArgs(d Dialect, tpl string, vals []any) []any {
	if !strings.Contains(d.Ph(1), "?") {
		return vals // $n：参数与序号一一对应
	}
	out := make([]any, 0, len(vals)*2)
	for i := 0; i < len(tpl); {
		if tpl[i] == '{' {
			j := i + 1
			for j < len(tpl) && tpl[j] != '}' {
				j++
			}
			if j < len(tpl) {
				if n, err := strconv.Atoi(tpl[i+1 : j]); err == nil && n >= 1 && n <= len(vals) {
					out = append(out, vals[n-1])
					i = j + 1
					continue
				}
			}
		}
		i++
	}
	return out
}

func makeStmts(d Dialect) stmts {
	inList := func(n int) string {
		ps := make([]string, n)
		for i := range ps {
			ps[i] = d.Ph(i + 1)
		}
		return strings.Join(ps, ", ")
	}
	// ov 取整条语句覆盖模板：overrides 是方言级"整句替换"（MSSQL 的 MERGE /
	// WITH(UPDLOCK,HOLDLOCK) / OFFSET-FETCH 无法表达为冲突子句或后缀），统一先 build
	// 再 withNowParam，与普通模板同一装配管线。
	ov := d.StmtOverrides
	ovr := func(key, def string) string {
		if o, ok := ov[key]; ok {
			return withNowParam(d, build(d, o))
		}
		return def
	}
	s := stmts{
		kvUpsert:     ovr(kvUpsertKey, withNowParam(d, build(d, kvSetTemplate(d, false)))),
		kvSetEx:      ovr(kvSetExStmt, build(d, kvSetTemplate(d, true))),
		kvGet:        build(d, "SELECT v FROM kv_items WHERE k = {1} AND (expire_at = 0 OR expire_at > {2})"),
		kvExists:     build(d, "SELECT 1 FROM kv_items WHERE k = {1} AND (expire_at = 0 OR expire_at > {2})"),
		kvDel:        build(d, "DELETE FROM kv_items WHERE k = {1}"),
		kvIncrSelect: ovr(kvIncrSelectKey, build(d, "SELECT v, expire_at FROM kv_items WHERE k = {1}"+d.ForUpdate)),
		kvIncrSeed:   ovr(kvIncrSeedKey, build(d, "INSERT INTO kv_items (k, v) VALUES ({1}, '0') "+d.IncrSeedTail)),
		// expire_at 一并写回：过期键按不存在处理时归零（{2}），未过期时保持原值。
		kvIncrUpdate: ovr(kvIncrUpdateKey, build(d, "UPDATE kv_items SET v = {1}, expire_at = {2} WHERE k = {3}")),
		kvIncrOne:    build(d, d.IncrSQL),
		// 已过期（非 0 且 <= now）的键按不存在处理，不"复活"。
		kvExpire:  build(d, "UPDATE kv_items SET expire_at = {1} WHERE k = {2} AND (expire_at = 0 OR expire_at > {3})"),
		kvTTL:     build(d, "SELECT expire_at FROM kv_items WHERE k = {1}"),
		kvCleanup: build(d, "DELETE FROM kv_items WHERE expire_at > 0 AND expire_at <= {1}"),
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
			if d.LimitStyle == "top" {
				// MSSQL：TOP 内联参数（TOP ({n})），无 LIMIT 语法。
				return build(d, "SELECT TOP ({"+strconv.Itoa(i)+"}) k, v FROM kv_items WHERE "+
					strings.Join(conds, " AND ")+" ORDER BY k")
			}
			return "SELECT k, v FROM kv_items WHERE " + strings.Join(conds, " AND ") +
				" ORDER BY k LIMIT " + d.Ph(i)
		},
		kvMGet: func(n int) string {
			return "SELECT k, v FROM kv_items WHERE k IN (" + inList(n) +
				") AND (expire_at = 0 OR expire_at > " + d.Ph(n+1) + ")"
		},

		qSeqEnsure:    ovr(qSeqEnsureKey, build(d, "INSERT INTO q_seq (q, next, prev) VALUES ({1}, 0, 0) "+d.QSeqIgnoreTail)),
		qSeqSelect:    ovr(qSeqSelectKey, build(d, "SELECT next, prev FROM q_seq WHERE q = {1}"+d.ForUpdate)),
		qSeqUpdate:    build(d, "UPDATE q_seq SET next = {1}, prev = {2} WHERE q = {3}"),
		qSeqBumpBack:  build(d, qSeqBumpTemplate(d, false)),
		qSeqBumpFront: build(d, qSeqBumpTemplate(d, true)),
		qItemInsert:   build(d, "INSERT INTO q_items (q, seq, v) VALUES ({1}, {2}, {3})"),
		qItemPop: func(desc bool) string {
			ord := "ASC"
			key := qItemPopKey
			if desc {
				ord, key = "DESC", qItemPopBackKey
			}
			return ovr(key, build(d, "SELECT seq, v FROM q_items WHERE q = {1} ORDER BY seq "+ord+" LIMIT 1"+d.ForUpdate))
		},
		qItemDelete: build(d, "DELETE FROM q_items WHERE q = {1} AND seq = {2}"),
		qItemCount:  build(d, "SELECT COUNT(*) FROM q_items WHERE q = {1}"),
		qItemPeek: func(desc bool) string {
			ord := "ASC"
			key := qItemPeekKey
			if desc {
				ord, key = "DESC", qItemPeekBackKey
			}
			return ovr(key, build(d, "SELECT v FROM q_items WHERE q = {1} ORDER BY seq "+ord+" LIMIT 1"))
		},
		// 与 zRange 同一形状：seq ASC 即队头 → 队尾，LIMIT/OFFSET 表达闭区间
		// [start, stop]（两条语句的占位符序都固定为 {1}=name、{2}=limit、{3}=offset）。
		qRange: ovr(qRangeKey, build(d, "SELECT v FROM q_items WHERE q = {1} ORDER BY seq ASC LIMIT {2} OFFSET {3}")),

		zUpsert: func(incr bool) string {
			tail := d.ZSetUpsertTail
			key := zUpsertKey
			if incr {
				tail, key = d.ZIncrUpsertTail, zIncrUpsertKey
			}
			return ovr(key, build(d, "INSERT INTO z_items (z, k, s) VALUES ({1}, {2}, {3}) "+tail))
		},
		zGet:        build(d, "SELECT s FROM z_items WHERE z = {1} AND k = {2}"),
		zDel:        build(d, "DELETE FROM z_items WHERE z = {1} AND k = {2}"),
		zCount:      build(d, "SELECT COUNT(*) FROM z_items WHERE z = {1}"),
		zRankMember: build(d, "SELECT s FROM z_items WHERE z = {1} AND k = {2}"),
		zRankCount:  build(d, "SELECT COUNT(*) FROM z_items WHERE z = {1} AND (s < {2} OR (s = {3} AND k < {4}))"),
		zRange:      ovr(zRangeKey, build(d, "SELECT k, s FROM z_items WHERE z = {1} ORDER BY s ASC, k ASC LIMIT {2} OFFSET {3}")),
		zRangeByScore: func(desc, hasLimit bool) string {
			// 方向二选一（两个键各自整句覆盖），limit 有无再二选一，共四条语句：
			// 有 LIMIT 的模板 {4}=limit；不限的模板压根不写 LIMIT 子句，也就
			// 不会把 0/负数当行数交给 SQL（负 LIMIT 在 SQLite 里是"无上限"、
			// 在 MySQL/PostgreSQL 里直接报错，语义按基座分叉，必须避免）。
			// 排序键只在 s 上翻转，k 恒为 ASC：同分成员按成员字节序升序，
			// desc 时该次序不翻转（Redis 同分行为一致）。
			ord := "ASC"
			noLimitKey, limitKey := zRangeByScoreKey, zRangeByScoreLimitKey
			if desc {
				ord, noLimitKey, limitKey = "DESC", zRangeByScoreDescKey, zRangeByScoreDescLimitKey
			}
			where := "SELECT k, s FROM z_items WHERE z = {1} AND s >= {2} AND s <= {3} "
			if hasLimit {
				return ovr(limitKey, build(d, where+"ORDER BY s "+ord+", k ASC LIMIT {4}"))
			}
			return ovr(noLimitKey, build(d, where+"ORDER BY s "+ord+", k ASC"))
		},
		zIncrSelect: build(d, "SELECT s FROM z_items WHERE z = {1} AND k = {2}"),
	}
	if d.Returning {
		// 与 zUpsert(true) 共用冲突子句，RETURNING 取更新后的分数：
		// 自增与读取在一条语句内完成（见 Provider.ZIncr）。
		s.zIncrOne = build(d, "INSERT INTO z_items (z, k, s) VALUES ({1}, {2}, {3}) "+d.ZIncrUpsertTail+" RETURNING s")
	}
	return s
}

// Provider 是 SQL 基座。并发安全由 database/sql 连接池与事务保障。
type Provider struct {
	writeMu   *sync.Mutex // 方言要求写串行化时非 nil（SQLite 单写者）
	db        *sql.DB
	st        stmts
	dialect   Dialect // 保留方言：IncrSQL 的参数装配需按占位符风格判定
	hasNumCol bool    // 方言是否使用数值投影列 n
	maxKeyLen int     // 方言键长上限（0 = 不限），写入侧校验
	closed    atomic.Bool
}

// New 在既有 *sql.DB 上构造基座：建表、清理过期行。
//
// Dialect.TablePrefix 若非空，必须是合法标识符片段（字母/数字/下划线，且不以数字
// 开头），因为它会被直接拼进表名而不是作为参数绑定。
func New(db *sql.DB, d Dialect) (*Provider, error) {
	if err := validTablePrefix(d.TablePrefix); err != nil {
		return nil, fmt.Errorf("sqlstore: dialect %s: %w", d.Name, err)
	}
	// 空 DDL 跳过（如 MySQL 索引已内联建表）。DDL 不经 build()，故在此单独加前缀。
	for _, raw := range []string{d.KvDDL, d.QSeqDDL, d.QItemsDDL, d.ZDDL, d.ZIdxDDL} {
		if raw == "" {
			continue
		}
		ddl := applyTablePrefix(d, raw)
		if _, err := db.Exec(ddl); err != nil {
			return nil, fmt.Errorf("sqlstore: migrate (%s): %w", d.Name, err)
		}
	}
	p := &Provider{db: db, st: makeStmts(d), dialect: d, hasNumCol: d.HasNumCol, maxKeyLen: d.MaxKeyLen}
	if d.SerializeWrites {
		p.writeMu = &sync.Mutex{}
	}
	// 打开时兜底清理已过期行（运行期读取路径已过滤）。
	if _, err := db.Exec(p.st.kvCleanup, core.NowUnix()); err != nil {
		return nil, fmt.Errorf("sqlstore: cleanup (%s): %w", d.Name, err)
	}
	return p, nil
}

// validTablePrefix 校验表名前缀只含标识符安全字符，防止把任意内容拼进 SQL。
func validTablePrefix(p string) error {
	if p == "" {
		return nil
	}
	for i := 0; i < len(p); i++ {
		c := p[i]
		ok := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (i > 0 && c >= '0' && c <= '9')
		if !ok {
			return fmt.Errorf("invalid table prefix %q (want identifier-safe: letters, digits, underscore)", p)
		}
	}
	return nil
}

// check 判定基座是否已关闭。
//
// 注意它与 Close 之间存在固有竞态：判定通过后、真正执行语句前，db.Close() 可能
// 已经落地，此时驱动会返回"database is closed"这类**非哨兵**错误，破坏
// "关闭后一律 ErrClosed"的契约（sqlite 的 close_race_test 正是钉这一点）。
// 该窗口无法用 closed 标志消除，故 DB 层调用统一经 closedErr 归一。
func (p *Provider) check() error {
	if p.closed.Load() {
		return core.ErrClosed
	}
	return nil
}

// closedErr 把"底层连接池已关闭"导致的驱动错误归一成 core.ErrClosed。
//
// 判据是**标志位 + 错误**同时成立：只有 Close 已经把 closed 置位、并且这条语句
// 确实失败了，才把错误改写成哨兵。正常业务错误（约束冲突、类型不匹配等）发生
// 在 closed=false 时不受影响；而 closed=true 之后本就不该有任何成功调用。
// 这样既消除上面 check/Close 的竞态泄漏，又不会掩盖真实错误。
func (p *Provider) closedErr(err error) error {
	if err != nil && p.closed.Load() {
		return core.ErrClosed
	}
	return err
}

// IncrWraps 能力声明：sqlstore 的 Incr 有显式溢出检查（事务与单语句路径都返回
// ErrNotInteger，不回绕），见 core.IncrWrapsProvider 与 Caps.IncrWraps。
func (p *Provider) IncrWraps() bool { return false }

var _ core.IncrWrapsProvider = (*Provider)(nil)

// Close 幂等：closed 为原子标志，Swap 保证只有一个调用者真正关闭底层连接池。
func (p *Provider) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	return p.db.Close()
}

// checkLen 校验方言的键长上限（仅 MySQL VARBINARY(255) 非零）：超长 key 在
// strict 模式下报 1406、非 strict 模式静默截断导致键碰撞，统一显式报错。
func (p *Provider) checkLen(what string, parts ...string) error {
	if p.maxKeyLen <= 0 {
		return nil
	}
	for _, s := range parts {
		if len(s) > p.maxKeyLen {
			return fmt.Errorf("sqlstore: %s: argument length %d exceeds dialect limit %d", what, len(s), p.maxKeyLen)
		}
	}
	return nil
}

// bs 把字符串键转 []byte，保证二进制安全的参数绑定。
func bs(s string) []byte { return []byte(s) }

// bv 归一化待写入的值：nil 切片按**空值**绑定，而不是 SQL NULL。
//
// 四张表的 v 列都声明为 NOT NULL，而 nil []byte 会被驱动绑定成 NULL，
// 于是 `Set(k, nil)` 报 constraint failed —— 与其它基座"零长值可正常往返"
// 的契约不一致（nil 与 []byte{} 在契约中无区别，Get 只承诺 ok=true 且 len=0）。
// 这里统一把 nil 转成非 nil 零长，使零长值在所有方言上都能写入。
func bv(v []byte) []byte {
	if v == nil {
		return []byte{}
	}
	return v
}

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
	if err := core.CheckKey(key); err != nil {
		return err
	}
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	if err := p.checkLen("set", key); err != nil {
		return err
	}
	// 参数顺序须与 kvSetTemplate 一致：k, v[, n], now（now 供冲突子句清过期）。
	args := []any{bs(key), bv(value)}
	if p.hasNumCol {
		args = append(args, numProjection(value))
	}
	args = append(args, core.NowUnix())
	if _, err := p.db.ExecContext(ctx, p.st.kvUpsert, args...); err != nil {
		return fmt.Errorf("sqlstore: set: %w", p.closedErr(err))
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
	if err := core.CheckKey(key); err != nil {
		return err
	}
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if err := p.checkLen("setex", key); err != nil {
		return err
	}
	args := []any{bs(key), bv(value), core.AddTTL(core.NowUnix(), ttl)}
	if p.hasNumCol {
		args = append(args, numProjection(value))
	}
	if _, err := p.db.ExecContext(ctx, p.st.kvSetEx, args...); err != nil {
		return fmt.Errorf("sqlstore: setex: %w", err)
	}
	return nil
}

// SetExAt 写入 value 并让 key 在 at（unix 秒）过期；at 已过去则删除该 key。
// expire_at 列本就是绝对秒，因此直接把 at 写进去。
func (p *Provider) SetExAt(ctx context.Context, key string, value []byte, at int64) error {
	if err := core.CheckKey(key); err != nil {
		return err
	}
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	if err := p.checkLen("setexat", key); err != nil {
		return err
	}
	if at <= core.NowUnix() {
		if _, err := p.db.ExecContext(ctx, p.st.kvDel, bs(key)); err != nil {
			return fmt.Errorf("sqlstore: setexat: %w", err)
		}
		return nil
	}
	args := []any{bs(key), bv(value), at}
	if p.hasNumCol {
		args = append(args, numProjection(value))
	}
	if _, err := p.db.ExecContext(ctx, p.st.kvSetEx, args...); err != nil {
		return fmt.Errorf("sqlstore: setexat: %w", err)
	}
	return nil
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := core.CheckKey(key); err != nil {
		return nil, false, err
	}
	if err := p.check(); err != nil {
		return nil, false, err
	}
	var v []byte
	err := p.db.QueryRowContext(ctx, p.st.kvGet, bs(key), core.NowUnix()).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("sqlstore: get: %w", p.closedErr(err))
	}
	return v, true, nil
}

func (p *Provider) Del(ctx context.Context, key string) error {
	if err := core.CheckKey(key); err != nil {
		return err
	}
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
	if err := core.CheckKey(key); err != nil {
		return false, err
	}
	if err := p.check(); err != nil {
		return false, err
	}
	var one int
	err := p.db.QueryRowContext(ctx, p.st.kvExists, bs(key), core.NowUnix()).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sqlstore: exists: %w", err)
	}
	return true, nil
}

func (p *Provider) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	if err := core.CheckKey(key); err != nil {
		return 0, err
	}
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return 0, err
	}
	if err := p.checkLen("incr", key); err != nil {
		return 0, err
	}
	// 快路径：PostgreSQL/SQLite 用单语句 upsert + RETURNING 完成
	// 「缺失按 0 起算 + 原子累加 + 校验非整数 + 已过期按不存在 + 取回新值」，
	// 一条语句一次 fsync，实测约为事务多语句路径的 2 倍吞吐（见 README 性能一节）。
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

	now := core.NowUnix()
	var raw []byte
	var expireAt int64
	if err := tx.QueryRowContext(ctx, p.st.kvIncrSelect, bs(key)).Scan(&raw, &expireAt); err != nil {
		return 0, fmt.Errorf("sqlstore: incr select: %w", err)
	}
	// 已过期的键契约上视为不存在：忽略陈旧值，从 0 起算并清除过期时间。
	expired := expireAt > 0 && expireAt <= now
	var cur int64
	if !expired {
		v, e := strconv.ParseInt(string(raw), 10, 64)
		if e != nil {
			return 0, core.ErrNotInteger
		}
		cur = v
	}
	// 溢出检查：与 Redis/SSDB 对齐返回错误，而不是 Go 侧静默回绕。
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, core.ErrNotInteger
	}
	newVal := cur + delta
	newExpire := expireAt
	if expired {
		newExpire = 0
	}
	// 新值以字节形态绑定：MySQL/PG/SQLite 的 TEXT/BLOB 列都能收 varbinary 形态的
	// 十进制文本；MSSQL 的 varbinary 列则必须由 Go 侧按二进制绑定（显式
	// CONVERT(nvarchar->varbinary) 得到的是 UTF-16 编码，会静默写坏计数值）。
	if _, err := tx.ExecContext(ctx, p.st.kvIncrUpdate, []byte(strconv.FormatInt(newVal, 10)), newExpire, bs(key)); err != nil {
		return 0, fmt.Errorf("sqlstore: incr update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sqlstore: incr commit: %w", err)
	}
	return newVal, nil
}

// incrOne 是单语句自增快路径。方言的 IncrSQL 在既有值非十进制整数时
// 不匹配 WHERE（SQLite/PG），语句因此不返回任何行——据此映射 ErrNotInteger；
// 已过期的行会被语句按"不存在"处理（从 0 起算并清除 expire_at）。
// 行被原子地加锁并更新，无需显式事务。
func (p *Provider) incrOne(ctx context.Context, key string, delta int64) (int64, error) {
	var newVal int64
	// 增量以文本形态传参：SQLite 的 CAST(? AS INTEGER/TEXT) 与
	// PG 的 ?::bigint 都能由文本隐式/显式转换，避免 pgx 对 int64->text 编码失败。
	ds := strconv.FormatInt(delta, 10)
	args := expandArgs(p.dialect, p.dialect.IncrSQL, []any{bs(key), ds, core.NowUnix()})
	err := p.db.QueryRowContext(ctx, p.st.kvIncrOne, args...).Scan(&newVal)
	if errors.Is(err, sql.ErrNoRows) {
		// upsert 未命中：key 已存在、未过期且值不是十进制整数。
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

// maxZRangePrealloc 限制按请求条数预分配切片的规模上界：调用方传入超大 limit
// 时先按此值分配、按需增长，避免一次性 OOM。
const maxZRangePrealloc = 1024

// maxZRangeLimit 是 ZRange 在不查集合大小时接受的最大请求行数：
// 超过则先取 zCount 收敛 stop（防溢出/巨型预分配，见 ZRange）。
const maxZRangeLimit = 1 << 32

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
		args = append(args, core.NowUnix())
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
		// rows.Close 只返回释放连接的错误，迭代期错误（ctx 取消、驱动错误）
		// 必须用 rows.Err() 捕获，否则会把部分结果当完整结果返回。
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("sqlstore: mget: %w", err)
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
	args = append(args, core.NowUnix(), limit)
	rows, err := p.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: scan: %w", err)
	}
	defer rows.Close()
	// 预分配按 1024 封顶：limit 是调用方入参，直接按 limit 分配会被
	// "合法但巨大"的 limit 打爆内存（OOM），行数由 SQL LIMIT 保证。
	out := make([]core.KeyValue, 0, min(limit, 1024))
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
	if err := core.CheckKey(key); err != nil {
		return err
	}
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	if ttl <= 0 {
		return core.ErrInvalidTTL
	}
	if err := p.checkLen("expire", key); err != nil {
		return err
	}
	now := core.NowUnix()
	// 参数：新过期时间、key、当前秒（用于把"已过期 = 不存在"写进 WHERE，不复活过期键）。
	if _, err := p.db.ExecContext(ctx, p.st.kvExpire, core.AddTTL(now, ttl), bs(key), now); err != nil {
		return fmt.Errorf("sqlstore: expire: %w", err)
	}
	return nil
}

// ExpireAt 让 key 在 at（unix 秒）过期；at 已过去则立即删除该 key。
// key 不存在/已过期时不处理（无匹配行即不生效，不复活过期键）。
func (p *Provider) ExpireAt(ctx context.Context, key string, at int64) error {
	if err := core.CheckKey(key); err != nil {
		return err
	}
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	if err := p.checkLen("expireat", key); err != nil {
		return err
	}
	now := core.NowUnix()
	if at <= now {
		if _, err := p.db.ExecContext(ctx, p.st.kvDel, bs(key)); err != nil {
			return fmt.Errorf("sqlstore: expireat: %w", err)
		}
		return nil
	}
	if _, err := p.db.ExecContext(ctx, p.st.kvExpire, at, bs(key), now); err != nil {
		return fmt.Errorf("sqlstore: expireat: %w", err)
	}
	return nil
}

func (p *Provider) TTL(ctx context.Context, key string) (int64, bool, error) {
	if err := core.CheckKey(key); err != nil {
		return 0, false, err
	}
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
	now := core.NowUnix()
	if exp == 0 || exp <= now {
		return -1, false, nil
	}
	rem := exp - now
	if rem <= 0 {
		// 单次采样的 now 保证"ok=true ⇒ 剩余秒数为正"，避免两次取时
		// 在秒边界上返回 (0, true) 与 Get 的"仍然存在"自相矛盾。
		return -1, false, nil
	}
	return rem, true, nil
}

// ---- Queue ----
//
// 序号模型：q_seq 每队列两个计数器——next（队尾追加，0,1,2...）与
// prev（队头插入，-1,-2...）。q_items 按 (q, seq) 主键排序即天然保持
// 队头->队尾顺序，前后插入共用同一顺序轴。

func (p *Provider) QPush(ctx context.Context, name string, value []byte) error {
	if err := core.CheckKey(name); err != nil {
		return err
	}
	defer p.writeLock()()
	return p.qpush(ctx, name, value, false)
}

func (p *Provider) QPushFront(ctx context.Context, name string, value []byte) error {
	if err := core.CheckKey(name); err != nil {
		return err
	}
	defer p.writeLock()()
	return p.qpush(ctx, name, value, true)
}

func (p *Provider) qpush(ctx context.Context, name string, value []byte, front bool) error {
	if err := p.check(); err != nil {
		return err
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlstore: qpush begin: %w", err)
	}
	defer tx.Rollback()
	if err := p.qpushTx(ctx, tx, name, value, front); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlstore: qpush commit: %w", err)
	}
	return nil
}

// qpushTx 在给定事务内完成一次入队：分配序号并写入数据行。
// RETURNING 方言用一条 UPDATE ... RETURNING 原子分配（同时推进 next/prev）；
// 其他方言（MySQL）走"确保行存在 + 行锁读 + 更新 + 插入"的多语句路径。
// 供单条 QPush 与批量 ApplyBatch 共用（批量时整批共享一个事务与一次提交）。
func (p *Provider) qpushTx(ctx context.Context, tx *sql.Tx, name string, value []byte, front bool) error {
	if err := p.checkLen("qpush", name); err != nil {
		return err
	}
	if p.st.qSeqBumpBack != "" {
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
		if _, err := tx.ExecContext(ctx, p.st.qItemInsert, bs(name), seq, bv(value)); err != nil {
			return fmt.Errorf("sqlstore: qpush insert: %w", err)
		}
		return nil
	}

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
	if _, err := tx.ExecContext(ctx, p.st.qItemInsert, bs(name), seq, bv(value)); err != nil {
		return fmt.Errorf("sqlstore: qpush insert: %w", err)
	}
	return nil
}

func (p *Provider) QPop(ctx context.Context, name string) ([]byte, bool, error) {
	if err := core.CheckKey(name); err != nil {
		return nil, false, err
	}
	defer p.writeLock()()
	return p.qpop(ctx, name, false)
}

func (p *Provider) QPopBack(ctx context.Context, name string) ([]byte, bool, error) {
	if err := core.CheckKey(name); err != nil {
		return nil, false, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return 0, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return nil, false, err
	}
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

// QRange 只读返回 [start, stop] 索引区间内的元素，方向为队头 → 队尾。
//
// 索引归一化与 ZRange 同规矩：0 起闭区间、负索引从末尾数、越界按可用范围裁剪
// 且不报错，空队列返回空。区别只在排序键——z_items 按 (s, k) 排，q_items 按
// seq ASC 排，而 seq 在 push 时单调分配（back 取 next 自增，front 取 prev 自减，
// 见 qpush），故 seq ASC 恰好是队头 → 队尾。出队是直接 DELETE q_items 行，
// 没有"待弹出"列，无需额外过滤。
func (p *Provider) QRange(ctx context.Context, name string, start, stop int64) ([][]byte, error) {
	if err := core.CheckKey(name); err != nil {
		return nil, err
	}
	if err := p.check(); err != nil {
		return nil, err
	}
	// 越界/负索引裁剪都要知道队列长度，而 QRange 是读路径：只有能确定
	// "区间落在 [0, n)" 内时才省掉这次 COUNT，否则与 ZRange 一样先取大小。
	// 不做这一步的话，start 越界时 SQL 只会返回更少的行（结果仍正确），
	// 但 stop 越界会让 LIMIT 按 stop 预分配（见下）。
	var prealloc int64
	if start >= 0 && stop >= start {
		prealloc = stop - start + 1
	}
	if start < 0 || stop < 0 || prealloc <= 0 || prealloc > maxZRangeLimit {
		// 负索引需先归一化；prealloc 溢出为负（如 stop=MaxInt64）或大到不可信
		// 时也必须先真实取 n——maxZRangeLimit 与 ZRange 同一口径，防巨型预分配。
		var n int64
		if err := p.db.QueryRowContext(ctx, p.st.qItemCount, bs(name)).Scan(&n); err != nil {
			return nil, fmt.Errorf("sqlstore: qrange size: %w", err)
		}
		// 裁剪规矩与 mem 基座的 indexRange 一致：start 负则加 n 并夹到 0，
		// stop 负则加 n（再加一次也不会回正，故同时覆盖"负数过小"的情形）。
		if start < 0 {
			start = n + start
			if start < 0 {
				start = 0
			}
		}
		if stop < 0 {
			stop = n + stop
		}
		// n == 0（空队列）、start 越过队尾、stop 落在 start 之前都返回空：
		// 既与 indexRange 的 ok=false 对应，也避免给 SQL 传负 OFFSET。
		if n == 0 || stop < start || start >= n {
			return nil, nil
		}
		if stop >= n {
			stop = n - 1
		}
		prealloc = stop - start + 1
	}
	// 预分配按 1024 封顶，理由与 ZRange 相同：prealloc 源自调用方入参，
	// 按"合法但巨大"的值直接分配会被打爆内存（行数由 SQL LIMIT 保证）。
	if prealloc > maxZRangePrealloc {
		prealloc = maxZRangePrealloc
	}
	rows, err := p.db.QueryContext(ctx, p.st.qRange, bs(name), prealloc, start)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: qrange: %w", err)
	}
	defer rows.Close()
	// 不预分配 out（只给个 0 容量起手）：元素是 []byte，append 会自动扩容，
	// 比照 prealloc 预留 limit 个切片头更省内存（ZRange 的元素是值类型才值得预分配）。
	var out [][]byte
	for rows.Next() {
		var v []byte
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("sqlstore: qrange row: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ---- ZSet ----

func (p *Provider) ZSet(ctx context.Context, name, key string, score int64) error {
	if err := core.CheckKeys(name, key); err != nil {
		return err
	}
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	if err := p.checkLen("zset", name, key); err != nil {
		return err
	}
	if _, err := p.db.ExecContext(ctx, p.st.zUpsert(false), bs(name), bs(key), score); err != nil {
		return fmt.Errorf("sqlstore: zset: %w", err)
	}
	return nil
}

func (p *Provider) ZGet(ctx context.Context, name, key string) (int64, bool, error) {
	if err := core.CheckKeys(name, key); err != nil {
		return 0, false, err
	}
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
	if err := core.CheckKeys(name, key); err != nil {
		return err
	}
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return err
	}
	if err := p.checkLen("zdel", name, key); err != nil {
		return err
	}
	if _, err := p.db.ExecContext(ctx, p.st.zDel, bs(name), bs(key)); err != nil {
		return fmt.Errorf("sqlstore: zdel: %w", err)
	}
	return nil
}

func (p *Provider) ZSize(ctx context.Context, name string) (int64, error) {
	if err := core.CheckKey(name); err != nil {
		return 0, err
	}
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
	if err := core.CheckKeys(name, key); err != nil {
		return 0, false, err
	}
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
	if err := core.CheckKey(name); err != nil {
		return nil, err
	}
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
	limit := stop - start + 1
	if limit <= 0 || limit > maxZRangeLimit {
		// limit 溢出为负（如 stop=MaxInt64）或大到不可信：按集合大小收敛
		// stop——与 Redis ZRANGE 对越界正索引的 clamp 语义一致。否则轻则
		// 负 cap 的 make 直接 panic，重则按 cap 预清零巨型切片 OOM。
		var n int64
		if err := p.db.QueryRowContext(ctx, p.st.zCount, bs(name)).Scan(&n); err != nil {
			return nil, fmt.Errorf("sqlstore: zrange size: %w", err)
		}
		if stop >= n {
			stop = n - 1
		}
		if stop < start || start < 0 {
			return []core.ZItem{}, nil
		}
		limit = stop - start + 1
	}
	if stop < start || start < 0 {
		return []core.ZItem{}, nil
	}
	offset := start
	rows, err := p.db.QueryContext(ctx, p.st.zRange, bs(name), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: zrange: %w", err)
	}
	defer rows.Close()
	// 预分配按 1024 封顶：limit 是调用方入参，直接按 limit 分配会被
	// "合法但巨大"的 limit 打爆内存（OOM），行数由 SQL LIMIT 保证。
	prealloc := limit
	if prealloc > 1024 {
		prealloc = maxZRangePrealloc
	}
	out := make([]core.ZItem, 0, prealloc)
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

// ZRangeByScore 返回分数落在闭区间 [min, max] 内的成员，按 (s, k) 序。
//
// desc 只翻转分数方向，不改参数含义：始终 min <= max，min > max 是空区间。
// 同分成员恒按成员字节序升序（desc 时也不翻转），与 ZRange 的排序一致。
// limit <= 0 表示不限，此时语句里根本没有 LIMIT 子句——绝不把 0/负数交给
// SQL（SQLite 视负 LIMIT 为不限、MySQL/PostgreSQL 直接报错，语义会按方言分叉）。
func (p *Provider) ZRangeByScore(ctx context.Context, name string, min, max int64, limit int, desc bool) ([]core.ZItem, error) {
	if err := core.CheckKey(name); err != nil {
		return nil, err
	}
	if err := p.check(); err != nil {
		return nil, err
	}
	// 空区间/倒置区间在读之前就短路：与 QRange 的"绝对不可能有结果"分支一致，
	// 省一次往返，也避免方言对矛盾 WHERE 的退化执行计划。
	if min > max {
		return []core.ZItem{}, nil
	}
	hasLimit := limit > 0
	stmt := p.st.zRangeByScore(desc, hasLimit)
	args := []any{bs(name), min, max}
	// 预分配按 1024 封顶，理由与 ZRange/QRange 相同：limit 是调用方入参，
	// 按"合法但巨大"的值直接分配会被打爆内存（行数由 SQL LIMIT 保证）。
	prealloc := limit
	if prealloc > maxZRangePrealloc {
		prealloc = maxZRangePrealloc
	}
	if hasLimit {
		args = append(args, limit)
	} else {
		prealloc = maxZRangePrealloc
	}
	rows, err := p.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: zrangebyscore: %w", err)
	}
	defer rows.Close()
	out := make([]core.ZItem, 0, prealloc)
	for rows.Next() {
		var k []byte
		var s int64
		if err := rows.Scan(&k, &s); err != nil {
			return nil, fmt.Errorf("sqlstore: zrangebyscore row: %w", err)
		}
		out = append(out, core.ZItem{Key: string(k), Score: s})
	}
	return out, rows.Err()
}

func (p *Provider) ZIncr(ctx context.Context, name, key string, delta int64) (int64, error) {
	if err := core.CheckKeys(name, key); err != nil {
		return 0, err
	}
	defer p.writeLock()()
	if err := p.check(); err != nil {
		return 0, err
	}
	if err := p.checkLen("zincr", name, key); err != nil {
		return 0, err
	}
	// 快路径：单语句原子自增 + RETURNING 新分数（PostgreSQL/SQLite），
	// 避免两条语句间并发写入导致返回值不是本次调用的结果。
	if p.st.zIncrOne != "" {
		var s int64
		if err := p.db.QueryRowContext(ctx, p.st.zIncrOne, bs(name), bs(key), delta).Scan(&s); err != nil {
			return 0, fmt.Errorf("sqlstore: zincr: %w", err)
		}
		return s, nil
	}
	// MySQL 无 RETURNING：upsert 本身原子，但并发下回读值可能包含其他
	// 调用的增量（契约只承诺分数正确落库，不承诺返回值线性对应本次调用）。
	if _, err := p.db.ExecContext(ctx, p.st.zUpsert(true), bs(name), bs(key), delta); err != nil {
		return 0, fmt.Errorf("sqlstore: zincr: %w", err)
	}
	var s int64
	if err := p.db.QueryRowContext(ctx, p.st.zIncrSelect, bs(name), bs(key)).Scan(&s); err != nil {
		return 0, fmt.Errorf("sqlstore: zincr select: %w", err)
	}
	return s, nil
}

// normalizeLimit 与 mem/ssdb/redis 基座相同的默认页大小约定（统一取 core 常量）。
func normalizeLimit(limit int) int {
	if limit <= 0 {
		return core.DefaultScanLimit
	}
	return limit
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---- Batch ----

// ApplyBatch 实现 core.BatchProvider：整批操作在**一个事务**内执行，提交时
// 只产生一次持久化（InnoDB redo / WAL fsync），把 N 次提交开销摊成 1 次。
// 任一步失败即回滚，整批不生效。SQLite 仍受进程内写锁串行化。
// BatchComposed 恒为 true：本基座在同一把锁/同一事务内逐条应用批操作，
// 批内后续操作能看到前序效果（见 core.BatchComposedProvider）。
func (p *Provider) BatchComposed() bool { return true }

var _ core.BatchComposedProvider = (*Provider)(nil)

func (p *Provider) ApplyBatch(ctx context.Context, ops []core.BatchOp) error {
	if err := checkBatchOps(ops); err != nil {
		return err
	}
	if err := p.check(); err != nil {
		return err
	}
	if len(ops) == 0 {
		return nil
	}
	defer p.writeLock()()

	// 先校验 TTL，避免事务做到一半才发现参数非法（虽然会回滚，但省一次往返）。
	for _, op := range ops {
		switch op.Kind {
		case core.BatchSetEx, core.BatchExpire:
			if op.TTL <= 0 {
				return core.ErrInvalidTTL
			}
		}
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlstore: batch begin: %w", err)
	}
	defer tx.Rollback()

	now := core.NowUnix()
	for i, op := range ops {
		if err := p.batchOp(ctx, tx, op, now); err != nil {
			return fmt.Errorf("sqlstore: batch op %d (kind %d): %w", i, op.Kind, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlstore: batch commit: %w", err)
	}
	return nil
}

// checkBatchOps 在开事务之前校验整批的 key/队列名/zset 名/成员。
//
// 与空 key 一样，这里也在**取写锁之前**完成：非法参数不需要排队等锁。
// zset 类操作（Key 是集合名、Member 是成员）要同时校验两者，与
// ZSet/ZGet/ZDel/ZRank/ZIncr 单键路径同一口径（core.CheckKeys）。
// 非 zset 类操作的 Member 字段按契约被忽略（见 core.BatchOp 注释），
// 因此不参与校验——否则会给一条本来合法的 BatchSet 平添无谓的失败。
func checkBatchOps(ops []core.BatchOp) error {
	for _, op := range ops {
		switch op.Kind {
		case core.BatchZSet, core.BatchZDel, core.BatchZIncr:
			if err := core.CheckKeys(op.Key, op.Member); err != nil {
				return err
			}
		default:
			if err := core.CheckKey(op.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

// batchOp 在事务内执行单条批操作。
func (p *Provider) batchOp(ctx context.Context, tx *sql.Tx, op core.BatchOp, now int64) error {
	switch op.Kind {
	case core.BatchSet:
		if err := p.checkLen("batch set", op.Key); err != nil {
			return err
		}
		args := []any{bs(op.Key), bv(op.Value)}
		if p.hasNumCol {
			args = append(args, numProjection(op.Value))
		}
		args = append(args, now) // kvUpsert 的冲突子句需要"当前秒"以清除已过期 TTL
		_, err := tx.ExecContext(ctx, p.st.kvUpsert, args...)
		return err
	case core.BatchSetEx:
		if err := p.checkLen("batch setex", op.Key); err != nil {
			return err
		}
		args := []any{bs(op.Key), bv(op.Value), core.AddTTL(now, op.TTL)}
		if p.hasNumCol {
			args = append(args, numProjection(op.Value))
		}
		_, err := tx.ExecContext(ctx, p.st.kvSetEx, args...)
		return err
	case core.BatchDel:
		_, err := tx.ExecContext(ctx, p.st.kvDel, bs(op.Key))
		return err
	case core.BatchExpire:
		_, err := tx.ExecContext(ctx, p.st.kvExpire, core.AddTTL(now, op.TTL), bs(op.Key), now)
		return err
	case core.BatchQPush:
		return p.qpushTx(ctx, tx, op.Key, op.Value, false)
	case core.BatchQPushFront:
		return p.qpushTx(ctx, tx, op.Key, op.Value, true)
	case core.BatchZSet:
		if err := p.checkLen("batch zset", op.Key, op.Member); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, p.st.zUpsert(false), bs(op.Key), bs(op.Member), op.Score)
		return err
	case core.BatchZDel:
		if err := p.checkLen("batch zdel", op.Key, op.Member); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, p.st.zDel, bs(op.Key), bs(op.Member))
		return err
	case core.BatchZIncr:
		if err := p.checkLen("batch zincr", op.Key, op.Member); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, p.st.zUpsert(true), bs(op.Key), bs(op.Member), op.Delta)
		return err
	default:
		return fmt.Errorf("unknown batch op kind %d", op.Kind)
	}
}
