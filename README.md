# kvdb — redis/ssdb 语义的持久化 Provider SDK

以 **redis/ssdb 命令语义为原型**的 Go 持久化适配器 SDK：对外提供统一的
**KV + Queue + ZSet** 三套接口，底层可插拔切换不同持久化基座。KV 为所有基座
必选能力；Queue / ZSet 为可选能力，基座尽量全部实现，未实现时调用返回
`ErrUnsupported`（可用 `DB.Capabilities()` 探测）。

内置基座（均可通过 `kvdb.Open(uri)` 或各包直接构造函数使用）：

| 基座 | package | KV | Queue | ZSet | 说明 |
|---|---|---|---|---|---|
| 纯内存 | `kvdb/mem` | ✅ | ✅ | ✅ | 不落盘，原型/测试/纯计算缓存 |
| JSONL 日志 | `kvdb/jsonl` | ✅ | ✅ | ✅ | append-only WAL，回放恢复，可 Compact |
| SSDB | `kvdb/ssdb` | ✅ | ✅ | ✅ | 原生文本协议客户端 |
| Redis | `kvdb/redis` | ✅ | ✅ | ✅ | String/List/Sorted Set 原生映射 |
| MySQL | `kvdb/mysql` | ✅ | ✅ | ✅ | 共享 `kvdb/sqlstore` 实现 |
| SQLite | `kvdb/sqlite` | ✅ | ✅ | ✅ | 共享 `kvdb/sqlstore` 实现 |
| PostgreSQL | `kvdb/pg` | ✅ | ✅ | ✅ | 共享 `kvdb/sqlstore` 实现 |

想要更多数据结构的组合在接口层就绪：`KvProvider`（KV）之上增加
`QueueProvider` / `ZSetProvider` 可选能力接口，消费者用类型断言或适配器接入。

### 基座接入：注册表默认空，按需 import

根包**不绑定任何基座实现**（因而不引入任何驱动依赖），注册表初始为空。
用哪个基座就 import 哪个包，各包在 `init` 中自注册：

```go
import (
	"kvdb"
	_ "kvdb/sqlite"   // 只接入 sqlite：不引入 mysql/pg/redis 等驱动
)

db, err := kvdb.Open(ctx, "sqlite://./data.db")
```

需要"全部可用"的宿主（示例、测试、运维工具）用聚合包一行接入：

```go
import _ "kvdb/all"   // 等价于 import 全部 7 个基座包
```

未注册的 scheme 会得到明确的装配错误（含已注册列表与修复提示），
也可用 `kvdb.Schemes()` 查询当前注册表：

```
kvdb: unknown provider scheme "jsonl" (registered: [mem]); import the backend
package (e.g. _ "kvdb/jsonl") or _ "kvdb/all"
```

自定义基座同样通过 `kvdb.Register` / `kvdb.MustRegister` 接入（见下）。

## 快速开始

```go
import (
	"kvdb"
	_ "kvdb/mem"      // 按需接入基座（或用 _ "kvdb/all" 全部接入）
)

ctx := context.Background()
db, err := kvdb.Open(ctx, "mem://")          // 换成 jsonl://./data.jsonl、sqlite://./db 等
if err != nil { /* ... */ }
defer db.Close()

db.Set(ctx, "user:1", []byte("alice"))       // KV
v, ok, _ := db.Get(ctx, "user:1")

db.QPush(ctx, "jobs", []byte("job-1"))       // Queue（可选能力）
job, ok, _ := db.QPop(ctx, "jobs")

db.ZSet(ctx, "rank", "alice", 90)            // ZSet（可选能力）
items, _ := db.ZRange(ctx, "rank", 0, -1)
```

URI 一览（等价于各包直接构造函数）：

```
kvdb.Open(ctx, "mem://")
kvdb.Open(ctx, "jsonl://./data.jsonl?sync=1")       // sync=1 每写 fsync
kvdb.Open(ctx, "sqlite://./data.db")
kvdb.Open(ctx, "mysql://user:pass@host:3306/dbname?parseTime=true")
kvdb.Open(ctx, "pg://user:pass@host:5432/dbname?sslmode=disable")
kvdb.Open(ctx, "redis://:pass@host:6379/0")
kvdb.Open(ctx, "ssdb://host:8888")
kvdb.Open(ctx, "ssdb://:password@host:8888")   // 服务端配置 server.auth 时携带密码
```

SSDB 认证（对应 `ssdb.conf` 的 `server.auth`，密码须 >=32 字符）：URI 携带
`ssdb://[:pass@]host:port`，或编程式 `ssdb.OpenWithConfig(ctx, ssdb.Config{Addr, Password})`，
连接建立时自动执行 `auth`；也可显式 `Provider.Auth(ctx, password)`。密码错误或
未认证的命令返回 `ssdb.ErrAuth`（映射服务端 `noauth`/`invalid password`）。
```

完整演示见 `example/main.go`；运行 `scripts/go.sh run ./example -backend sqlite://./tmp/demo.db`。

## 字节 <-> T 辅助（B / P / D / DMust）

`bytes <=> T` 的泛型辅助，覆盖三种常用调用形态（数值按 T 位宽严格解析；
整数编码为十进制，与 `Incr` 互操作——`B(int64)` 写入、`Incr` 累加、
`D[int64]` 读回全链路自洽）：

```go
// 1) 写路径内联编码：Set(x, B(v), ...)
db.Set(ctx, "n", kvdb.B(int64(42)))
db.SetEx(ctx, "s", kvdb.B("token"), 3600)
db.QPush(ctx, "q", kvdb.B(1.5))

// 2) 读路径合并三返回值（Go 多返回值直接展开为实参）：
n, err := kvdb.D[int64](db.Get(ctx, "n"))     // 缺失 -> ErrNotFound
v, err := kvdb.D[string](db.QPop(ctx, "q"))   // 空队列同理

// 3) must 变体：缺失/解析失败直接 panic(err)
n := kvdb.DMust[int64](db.Get(ctx, "n"))

// 底层类型别名同样支持：type OrderID int64；kvdb.B(o)、kvdb.P[OrderID](b)
```

支持的标量：整数家族（int/int8..int64/uint/..uint64/uintptr）、float32/64、
string、bool、`[]byte`（恒等）。`P[T]` 是单值解码；`D` 额外合并
`(val, ok, err)` 三值；`DMust` 为 panic 版。

## 接口契约（以 redis/SSDB 为原型）

- **KV**（必选）：`Set / SetEx / Get / Del / Exists / Incr / MGet / Scan / Expire / TTL`，
  与 SSDB `set/setx/get/del/exists/incr/multi_get/scan/expire/ttl` 一一对应。
  - 值与键二进制安全；`Scan(start, end, limit)` 返回 **字节序** 升序闭区间。
    `start`/`end` 为空表示该侧不限；`limit<=0` 取 `DefaultScanLimit=100`。
  - `Set` 保留既有 TTL（SSDB 不改 ttl 表；Redis 用 `SET ... KEEPTTL`，需 Redis ≥ 6.0）。
  - `SetEx(key, value, ttl)` 一次写入值与存活秒数，覆盖既有 TTL（Redis `SETEX` / SSDB `setx`；
    SQL 基座单条 upsert 同时写值与 `expire_at`）；`ttl<=0` 返回 `ErrInvalidTTL`。
  - `Incr` 缺失按 0 起算；既有值非十进制整数返回 `ErrNotInteger`。
  - `Expire(ttl>0)`；`TTL` 返回 `(剩余秒数, ok)`：`ok=false` 表示 key 不存在 /
    无 TTL / 已过期（SSDB 对后两者均返回 -1，统一映射；Redis -2/-1 同样归并）。
- **Queue**（可选）：`QPush / QPushFront / QPop / QPopBack / QSize / QFront / QBack`，
  先进先出；队尾 = SSDB `qpush` / Redis `RPUSH`，队头 = Redis List 左端。
- **ZSet**（可选）：`ZSet / ZGet / ZDel / ZSize / ZRank / ZRange / ZIncr`，
  排序 `(score 升序, key 升序)`，排名 0 起；`ZRange(start, stop)` 用 Redis 风格
  0 起闭区间索引，负索引从末尾数。
  - **分数类型为 int64**（对齐 SSDB：其 zset score 即 int64）。Redis 基座经
    float64 换算，`|score| ≤ 2^53` 内无损；超出会有尾数误差（契约按 int64 定义）。

三套数据结构的命名空间相互独立。

### 基座语义差异（已统一，仅作说明）

| 差异点 | 处理 |
|---|---|
| SSDB `scan` 为 start 开区间 | ssdb 基座对存在的 start 键做一次 get 补偿，对外仍闭区间 |
| Redis 无字节序范围扫描 | redis 基座全量 SCAN 游标 + 客户端过滤排序，**代价与 keyspace 大小相关** |
| SQL 过期行 | 读取路径过滤；Open 时批量清理一次。长期运行可自行按 `expire_at` 清理 |
| SQL 并发 | MySQL/PG 用行锁+幂等占位（Incr 事务）；PG/SQLite 的 QPush 用 `UPDATE ... RETURNING` 单语句分配序号（MySQL 走多语句兼容路径）；SQLite 内存库单连接、文件库多连接 + `_txlock=immediate` 写串行 |
| JSONL | 单进程内嵌；日志增长需定期 `Compact()`；崩溃尾部半行容错回放；打开时流式回放（内存峰值限单行） |
| SSDB 连接 | 连接池（默认 8，`Config.PoolSize` 可调）：每操作借还连接，池满阻塞等待；单连接时代并发无提升 |

## 扩展：自定义持久化基座

实现 `core.KvProvider`（KV）+ 可选能力接口，注册后即可经 `kvdb.Open` 使用：

```go
type myStore struct{ /* ... */ }
func (m *myStore) Set(...) error { /* ... */ }
// ... 其余 KV 方法；可选再实现 core.QueueProvider / core.ZSetProvider

func init() {
	// 基座包推荐 MustRegister（重复注册是装配错误，启动期 panic 优于运行期静默）
	kvdb.MustRegister("mybase", func(ctx context.Context, u *url.URL)(core.KvProvider, error) {
		return &myStore{}, nil
	})
}
// 之后 kvdb.Open(ctx, "mybase://...") 直接可用（业务侧 import _ "your/module/mybase"）
```

## 构建与测试

默认沙箱/开发机的 `GOPATH` 模块缓存可能只读，本仓库统一用包装脚本隔离缓存：

```bash
scripts/go.sh build ./...
scripts/go.sh test ./...        # 全部本地基座（mem/jsonl/sqlite + 进程内替身）
```

真实基座用例默认跳过，显式设置连接后全量跑（见各 `*_test.go` 顶部注释）：

```bash
# 临时测试容器（专用端口，不碰环境现有服务）
docker run -d --name kvdb-test-mysql -p 127.0.0.1:43306:3306 \
  -e MYSQL_ROOT_PASSWORD=kvdbroot -e MYSQL_DATABASE=kvdb_test mysql:8.0
docker run -d --name kvdb-test-pg -p 127.0.0.1:45432:5432 \
  -e POSTGRES_PASSWORD=kvdbpass -e POSTGRES_DB=kvdb_test postgres:16-alpine
docker run -d --name kvdb-test-redis -p 127.0.0.1:46379:6379 redis:7-alpine
docker run -d --name kvdb-test-ssdb -p 127.0.0.1:48888:8888 leobuskin/ssdb-docker

KVDB_TEST_MYSQL_DSN='root:kvdbroot@tcp(127.0.0.1:43306)/kvdb_test' \
KVDB_TEST_PG_DSN='postgres://postgres:kvdbpass@127.0.0.1:45432/kvdb_test?sslmode=disable' \
KVDB_TEST_REDIS_ADDR=127.0.0.1:46379 \
KVDB_TEST_SSDB_ADDR=127.0.0.1:48888 \
KVDB_TEST_SSDB_AUTH_ADDR=127.0.0.1:48889 KVDB_TEST_SSDB_AUTH_PASS=<强密码> \
  scripts/go.sh test ./...
```
`KVDB_TEST_SSDB_AUTH_ADDR/KVDB_TEST_SSDB_AUTH_PASS` 指向开启 `server.auth` 的
专用 SSDB 实例（覆盖未认证拒绝、错误密码、正确密码读写三条路径）。

测试结构：所有基座共用 `internal/behaviortest` 行为用例（KV/Queue/ZSet 合同，
含并发 Incr 原子性）；SSDB 另有进程内假服务器（按协议文档独立实现）交叉验证
线编码；redis 用 miniredis；sqlstore 逻辑经 SQLite 内存库实测、MySQL/PG 经
真实容器验证。

## 性能要点（实测量级，WSL+容器环境）

SQL 基座的写入瓶颈是**每事务一次的持久化 fsync**（InnoDB redo / PG WAL），
不是语句数。实测（单机容器）：

| 场景 | 量级 | 说明 |
|---|---|---|
| InnoDB / PG 并发写·多 key | 700-2,200 op/s | **组提交**自动合并 fsync，连接池越大越快（默认 32） |
| InnoDB / PG 单 key 计数器 | 90-160 / 240-512 op/s | 行锁串行 + 每次提交 fsync |
| MyISAM（无事务） | ~2× InnoDB | 破坏原子性，不采用 |
| SSDB / Redis 基座 | 3,500-11,000 op/s | 高频写请用这两个基座 |

jsonl 的写吞吐取决于文件系统介质：WSL 挂载盘(drvfs)实测约 1.5-2k op/s，
WSL 原生盘/ext4 约 20 万 op/s（内部封装 mem，读路径与 mem 同级；写差距是
每 op 一次文件追加的 WAL 固有成本）。JSONL 适合低频/中等写入的进程内持久化。

已落地优化：
- **连接池**：mysql/pg 默认 `SetMaxOpenConns(32)`，多 key 并发写吃满组提交；
- **Incr 单语句化**：PG/SQLite 用 upsert + `RETURNING` 一条语句完成
  「缺失按 0、原子累加、非整数报错、取回新值」，PG 实测 237→512 op/s（2.2×）；
  MySQL 无 `RETURNING` 保持事务路径；
- **QPush 单语句分配序号**：PG/SQLite 的 `UPDATE ... RETURNING`（4 条→2 条 SQL/事务）。

部署侧可选项（**会缩短崩溃恢复窗口，需业务确认持久性等级**）：
- MySQL `innodb_flush_log_at_trx_commit=2`、PG `synchronous_commit=off`：写入再提升 2-2.6×，
  崩溃时可能丢最近约 1 秒已提交事务（原子性不受影响，持久性降级）；
- 同 key 高频计数/队列的最终解法是 `Batch`（一个事务 N 条 = 1 次 fsync）或改用 ssdb/redis 基座。

## 兼容性与驱动选择（实测）

| 依赖 | 实测通过版本 | 说明 |
|---|---|---|
| MySQL | **5.6.51** / 8.0.46 | 5.6 默认 innodb_large_prefix=OFF、索引前缀上限 767 字节，键列因此取 |
| PostgreSQL | **9.6** / 10 / 12 / 16 | `ON CONFLICT ... RETURNING` 需 9.5+；bytea 排序在 9.6+ 稳定 |
| SQLite | modernc.org/sqlite（纯 Go） | 需 3.35+（`RETURNING`），WAL 模式 |
| Redis | 7.x | `SET ... KEEPTTL` 需 6.0+ |
| SSDB | leobuskin/ssdb-docker | 原生协议；支持 `server.auth` |

MySQL 键列上限 **255 字节**（`VARBINARY(255)`）：这是兼容 5.6 默认索引前缀
（767 字节；组合索引 `(z,s,k)` = 255+8+255 = 518 亦在限内）的取舍。
PostgreSQL/SQLite 用 BYTEA/BLOB，无此限制。

**SQLite 驱动为何选纯 Go（modernc）而非 CGO（mattn/go-sqlite3）**——实测对比
（同一 `sqlstore` 实现，仅换底层驱动）：

| 介质 | 驱动 | Set | Get | Incr | QPush |
|---|---|---|---|---|---|
| tmpfs（fsync 免费） | pure-Go | 23,875 | 25,361 | 10,573 | 6,141 |
| tmpfs | CGO | 37,158 | 45,177 | 18,951 | 6,674 |
| **真实盘** | pure-Go | 543 | 1,917 | 130 | 91 |
| **真实盘** | CGO | 568 | 2,178 | **128** | **94** |

结论：CGO 仅在 I/O 免费（tmpfs）时快 1.6-1.8×；在真实存储上二者几乎相同
（Incr 130 vs 128），因为瓶颈是每事务 fsync 而非驱动实现。纯 Go 因此免去
CGO/gcc/交叉编译成本而无性能损失——SDK 不引入 CGO 依赖。

## 已知边界（非缺陷，按契约行事）

- sqlite/mysql/pg 的键是二进制列（BLOB/BYTEA/VARBINARY），超长键受列类型限制
  （可调 DDL，见 `sqlstore` 方言）；SSDB 原生也限制键长。
- ssdb 基座用连接池提升并发（默认 8 连接），单条连接上的请求仍为串行请求-应答；
  池内置断连自愈：连接借出前做 ~100µs 非阻塞存活探测，服务器重启/断开的连接
  在池内被自动丢弃并重建（业务无感；个别半开连接仍会消耗一次失败请求）。
  请求级错误（非连接类）不自动重试，业务按需自行重试。
- jsonl 基座不跨进程共享同一文件（无文件锁）。

## 目录结构

```
kvdb/                   根包：DB 适配器 + Open/Register 注册表（默认空）+ 契约再导出
all/                    聚合注册包：import _ 即接入全部内置基座
bytes.go                字节 <-> T 泛型辅助（B/P/D/DMust）
core/                   契约：KvProvider/FullProvider/Queue/ZSet/Closer/哨兵错误/常量
mem|jsonl|ssdb|redis|mysql|sqlite|pg/   各基座实现与测试
sqlstore/               MySQL/SQLite/PG 共享的 database/sql 实现（方言参数化）
internal/behaviortest/  三套共享行为用例
example/                跨基座演示
scripts/go.sh           Go 命令包装（隔离模块/构建缓存到工作区）
.ref/                   引用的 SSDB 源码（协议/语义核对用，勿提交到业务仓库）
```