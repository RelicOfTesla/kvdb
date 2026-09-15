# kvdb

以 **Redis / SSDB 命令语义为原型**的 Go 持久化适配器 SDK：对外提供统一的
**KV + Queue + ZSet** 接口，底层可插拔切换不同持久化基座（内存 / 文件日志 /
SQL / Redis / SSDB / BoltDB），并内置批量写与字节编解码辅助。

```go
db, _ := kvdb.Open(ctx, "sqlite://./data.db")
defer db.Close()

db.Set(ctx, "user:1", []byte("alice"))
n, err := db.Incr(ctx, "visits", 1)
```

## 特性

- **8 个内置基座**，同一套 API：`mem` / `jsonl` / `bolt` / `sqlite` / `mysql` / `pg` / `redis` / `ssdb`
- **能力可选、按需探测**：KV 必选；Queue / ZSet / Batch / 生命周期为可选能力，
  未实现时返回 `ErrUnsupported`，可用 `Capabilities()` 探测
- **注册表默认空**：用哪个基座就 `import _` 哪个包，根包不引入任何驱动依赖
- **接口化返回**：`kvdb.Open` 返回接口 `DB`，业务可窄依赖 `KvProvider` 等子接口，便于 mock
- **批量写**：一批操作映射到各基座原生机制（事务 / MULTI/EXEC / 流水线 / 单次 flush）
- **字节 ↔ 泛型辅助**：`B` / `P` / `D` / `DMust` 支持标量与结构体（默认 JSON，编解码可替换），标量编码与 `Incr` 互操作
- **Go 1.27.1+ 可选薄壳**：`kvdb.Typed(db)` 提供 `db.Get[T](...)` 泛型方法（构建约束隔离，不影响旧版本）
- 纯 Go 依赖，无 CGO

需要 Go 1.25+。依赖按基座引入：`sqlite`（modernc.org/sqlite）、`mysql`
（go-sql-driver/mysql）、`pg`（jackc/pgx）、`redis`（redis/go-redis）；
`bolt`（go.etcd.io/bbolt）、`mem` / `jsonl` / `ssdb` 仅用标准库或纯 Go 库。

## 引入方式

```bash
go get github.com/RelicOfTesla/kvdb
```

```go
import (
    "github.com/RelicOfTesla/kvdb"
    _ "github.com/RelicOfTesla/kvdb/sqlite"   // 按需接入基座
)
```

## 快速开始

```go
import (
    "context"

    "github.com/RelicOfTesla/kvdb"
    _ "github.com/RelicOfTesla/kvdb/sqlite"  // 只接入 sqlite；或 _ .../all 一次接入全部
)

ctx := context.Background()
db, err := kvdb.Open(ctx, "sqlite://./data.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// KV
db.Set(ctx, "user:1", []byte("alice"))
v, ok, _ := db.Get(ctx, "user:1")
db.SetEx(ctx, "session", []byte("token"), 1800) // 写入并设置 TTL
n, _ := db.Incr(ctx, "visits", 1)
pairs, _ := db.Scan(ctx, "user:", "user;", 100) // 闭区间：":" 的下一字节是 ";"

// Queue（可选能力）
db.QPush(ctx, "jobs", []byte("job-1"))
job, ok, _ := db.QPop(ctx, "jobs")

// ZSet（可选能力）
db.ZSet(ctx, "rank", "alice", 90)
top, _ := db.ZRange(ctx, "rank", 0, -1)
```

URI 一览（各包也提供等价的直接构造函数，如 `sqlite.Open`）：

```
mem://
jsonl://./data.jsonl?sync=1        # sync=1 每次写 fsync
sqlite://./data.db
bolt://./data.bolt?nosync=1        # nosync=1 关闭 fsync（更快，崩溃可能丢最近提交）
mysql://user:pass@host:3306/dbname?parseTime=true
pg://user:pass@host:5432/dbname?sslmode=disable
redis://:password@host:6379/0
ssdb://host:8888
ssdb://:password@host:8888         # 服务端启用 server.auth 时
```

完整演示见 [`example/main.go`](example/main.go)。

## 内置基座

| 基座 | 子包 | KV | Queue | ZSet | 说明 |
|---|---|---|---|---|---|
| 纯内存 | `mem` | ✅ | ✅ | ✅ | 不落盘，测试/缓存 |
| JSONL 日志 | `jsonl` | ✅ | ✅ | ✅ | append-only WAL，打开时回放，支持 `Compact()`；单进程内嵌 |
| BoltDB | `bolt` | ✅ | ✅ | ✅ | bbolt 单文件 B+tree（纯 Go）；每写一次事务提交，批写整批一次提交 |
| SQLite | `sqlite` | ✅ | ✅ | ✅ | 纯 Go 驱动（modernc），无 CGO |
| MySQL | `mysql` | ✅ | ✅ | ✅ | 共享 `sqlstore` |
| PostgreSQL | `pg` | ✅ | ✅ | ✅ | 共享 `sqlstore` |
| Redis | `redis` | ✅ | ✅ | ✅ | String / List / Sorted Set 原生映射 |
| SSDB | `ssdb` | ✅ | ✅ | ✅ | 原生文本协议客户端，连接池 + 认证 |

导入路径为 `github.com/RelicOfTesla/kvdb/<子包>`，另有聚合包 `.../all`。

## 能力模型

`kvdb.Open` / `kvdb.Wrap` 返回接口 `DB`，它由若干能力接口组合而成，具体适配器
为非导出实现：

```go
type DB interface {
    KvProvider      // KV（必选能力）
    QueueProvider   // 队列
    ZSetProvider    // sorted set
    Batcher         // db.Batch(ctx, fn)
    Closer          // Close
    Capabilities() (hasQueue, hasZSet, hasBatch bool)
}
```

- 基座未实现的能力：调用返回 `ErrUnsupported`，先用 `Capabilities()` 探测可避免。
- **窄依赖**：业务函数只需声明用到的能力接口，测试里实现对应方法即可，无需实现整个 `DB`：

```go
func touch(ctx context.Context, store kvdb.KvProvider, key string) (int64, error) {
    return store.Incr(ctx, key, 1)   // 真实基座与 mock 均可传入
}
```

- `kvdb.Unwrap(db)` 可取回适配器背后的基座（非适配器实现返回 nil）。

## 批量写

一批操作一次提交，显著降低往返与持久化开销：

```go
err := db.Batch(ctx, func(b *kvdb.Batch) error {
    b.Set("k", value)
    b.QPush("jobs", payload)
    b.ZIncr("rank", "alice", 1)
    return nil        // 返回 nil 才提交；返回错误或收集期校验失败则整批不生效
})
```

- 设计为「共享收集器 + 基座只实现提交」：基座实现 `ApplyBatch(ctx, ops)`，
  各基座映射到原生机制（SQL = 一个事务、Redis = 一次 MULTI/EXEC、SSDB = 一次
  流水线、jsonl = 一次 flush、mem = 单次持锁）。
- 批内只允许**无条件写**（Set/SetEx/Del/Expire/QPush/QPushFront/ZSet/ZDel/ZIncr）；
  `Incr`/`QPop` 依赖键的当前状态、需先校验再提交，混入会破坏整批原子性，需单独调用。
- 原子性：MySQL/SQLite/PG（事务）、Redis（MULTI/EXEC）、mem/jsonl（进程内）
  在提交失败时整批不生效；**SSDB 无事务**，流水线失败可能部分生效（其价值在减少往返）。

## 字节 ↔ T 辅助

```go
type User struct {
    ID   int64    `json:"id"`
    Name string   `json:"name"`
    Tags []string `json:"tags"`
}

// 写：内联编码
db.Set(ctx, "n", kvdb.B(int64(42)))     // 标量 -> 十进制文本
db.Set(ctx, "u", kvdb.B(User{ID: 7}))   // 结构体 -> JSON

// 读：D 合并 Get/QPop 的 (val, ok, err) 三返回值（缺失 -> ErrNotFound）
n, err := kvdb.D[int64](db.Get(ctx, "n"))
u, err := kvdb.D[User](db.Get(ctx, "u"))
v, err := kvdb.D[string](db.QPop(ctx, "jobs"))

// panic 变体 / 单值解码
n := kvdb.DMust[int64](db.Get(ctx, "n"))
raw, err := kvdb.P[User](b)
```

编码规则：

| 类型 | 编码 | 说明 |
|---|---|---|
| 整数（含 `~` 别名）、float、string、bool | 文本 | 与 `Incr` 互操作（`B(int64)` → 十进制） |
| `[]byte` | 恒等 | 不经过 JSON/base64 |
| 结构体、切片、映射、指针、接口等 | `Marshal`（默认 JSON） | 支持嵌套结构体与 `json` tag；未导出字段忽略 |

**编解码可替换**：`kvdb.Marshal` / `kvdb.Unmarshal` 是包级变量，默认 JSON，
可在 init 中换成 msgpack / protobuf / gob 等；标量路径不受影响。

```go
func init() {
    kvdb.Marshal = msgpack.Marshal
    kvdb.Unmarshal = msgpack.Unmarshal
}
```

注意 `B` 对编码失败会 panic（不返回 error）；需要错误处理时直接调用 `Marshal`。

### Go 1.27.1+：`TypedDB` 薄壳（`db.Get[T](...)`）

用 Go 1.27.1+ 构建时会额外提供 `TypedDB` 薄壳，把「取字节 + 解码」合并为泛型方法
（方法级类型参数需 1.27.1+）：

```go
tdb := kvdb.Typed(db)
u, err := tdb.Get[User](ctx, "user:1")    // = kvdb.D[User](db.Get(ctx, "user:1"))
n, err := tdb.Get[int64](ctx, "visits")
job, err := tdb.QPop[string](ctx, "jobs")
ms, err := tdb.MGet[User](ctx, "user:1", "user:2")
```

- 提供 `Get` / `GetOK` / `MGet` / `QPop` / `QPopBack` / `QFront` / `QBack`；
  缺失的 key 返回 `ErrNotFound`（`GetOK` 保留 `ok` 语义）；
- 编码解码复用 `B`/`P` 与可替换的 `Marshal`/`Unmarshal`，标量仍与 `Incr` 互操作；
- 其余方法（`Set` / `QPush` / `ZSet` / `Batch` / `Close` …）经内嵌 `DB` 直接透传；
  但泛型方法会遮蔽同名方法，**`TypedDB` 不再满足 `DB` 接口**，需要 DB 语义时用
  `tdb.DB` 或保留原始 `db`；
- 需要 **Go 1.27.1+**（方法级类型参数自 1.27 起支持，但 1.27.0 有泛型方法相关的
  编译器缺陷，如 [golang/go#81195](https://github.com/golang/go/issues/81195) 的
  `malformed linker symbol`，已在 1.27.1 修复）。实现放在带 `//go:build go1.27`
  约束的文件中，**本模块和消费方的 `go` 指令都无需抬高**：1.27 之前的工具链不编译
  此文件，`TypedDB` / `Typed` 不存在，其余功能不受影响。

版本/门禁实测矩阵（同一份探针代码，`go.mod` 写 `go 1.25.0`；✅ 编译、❌ 报错）：

| 探针 | go1.25.1 | go1.26.5 | go1.26.6 | go1.27.1 |
| --- | --- | --- | --- | --- |
| 泛型方法，无构建标签 | ❌ `method must have no type parameters` | ❌ 同左 | ❌ 同左 | ❌ `generic method requires go1.27 or later (-lang was set to go1.25; check go.mod)` |
| 泛型方法，`//go:build go1.27` | 文件被排除 | 文件被排除 | 文件被排除 | ✅ |
| 泛型方法，`//go:build go1.26` | 文件被排除 | 文件被排除 | 文件被排除 | ❌ `generic method requires go1.27 or later (file declares //go:build go1.26)` |
| 文件名 `xxx_go1.26.5.go` / `xxx_go1265.go` / `xxx_go1.99.go` | ✅ 全被编译 | ✅ | ✅ | ✅ |
| 仅 `//go:build go1.26.5`（补丁级标签） | 文件被排除 | 文件被排除 | 文件被排除 | 文件被排除 |
| `go.mod` 写 `go 1.26.5` / `go 1.26.6` | 合法，但要求 1.26.5+ 工具链 | ✅ | ✅ | ✅ |

由此可确认三点：

1. **补丁级构建标签不存在**：`ReleaseTags` 只有 `… go1.25` / `… go1.26` / `… go1.27`，
   写 `//go:build go1.26.5` 永远不成立，而且**不报错**（静默排除文件），所以无法表达
   "1.27.1+"，只能按 1.27 系列放行。`go.mod` 里写补丁级 `go` 指令倒合法，但那是工具链
   下限，不是文件门禁。
2. **文件名后缀不产生任何约束**：Go 只认 `*_GOOS` / `*_GOARCH` / `*_GOOS_GOARCH`；
   `xxx_go1.99.go` 在 go1.25.1 上照样编译。`typed_go1.27.go` 里的 `go1.27` 纯属可读性
   命名，真正的门禁是文件内的 `//go:build` 行。
3. **`//go:build go1.27` 一举两得**：既是文件选择门禁，又会按 Go 规则把该文件的
   language version 抬到 `go1.27`（cmd/go 依文件内的 `go1.N` 约束传 `-lang`），这正是
   `go.mod` 只写 `go 1.25.0` 也能用 `db.Get[T](...)` 的原因；去掉这行就会在 1.27.1 上
   编译失败（见上表），所以**不要**为了"顺便兼容 1.25"而删掉它。

## 语义要点

- **KV**：`Set / SetEx / Get / Del / Exists / Incr / MGet / Scan / Expire / TTL`，
  对应 SSDB `set/setx/get/del/exists/incr/multi_get/scan/expire/ttl`。
  - 键与值二进制安全；`Scan(start, end, limit)` 为**字节序闭区间**升序，
    空串表示该侧不限，`limit<=0` 取 `DefaultScanLimit`（100）。
  - `Set` 保留既有 TTL；`SetEx` 覆盖 TTL，`ttl<=0` 返回 `ErrInvalidTTL`。
  - `Incr` 缺失按 0 起算；值非十进制整数返回 `ErrNotInteger`。
  - `TTL` 返回 `(剩余秒数, ok)`，`ok=false` 表示 key 不存在 / 无 TTL / 已过期。
- **Queue**：`QPush / QPushFront / QPop / QPopBack / QSize / QFront / QBack`，先进先出。
- **ZSet**：`ZSet / ZGet / ZDel / ZSize / ZRank / ZRange / ZIncr`，排序
  `(score 升序, key 升序)`，排名 0 起；`ZRange(start, stop)` 为 0 起闭区间索引，
  负索引从末尾数。**分数类型为 int64**（对齐 SSDB）；Redis 基座经 float64 换算，
  `|score| ≤ 2^53` 内无损。
- 三类数据的命名空间相互独立。哨兵错误：`ErrUnsupported` / `ErrClosed` /
  `ErrNotInteger` / `ErrInvalidTTL` / `ErrNotFound`。

### 各基座差异（对外已统一）

| 差异点 | 处理 |
|---|---|
| SSDB `scan` 是 start 开区间 | ssdb 基座对存在的 start 键做一次 get 补偿，对外仍为闭区间 |
| Redis 无字节序范围扫描 | redis 基座全量 SCAN + 客户端过滤排序，**只扫描 KV 前缀**；代价与 KV 键数量相关 |
| SQL 过期行 | 读取路径过滤，开库时清理一次 |
| SQLite 并发写 | 进程内写串行化（单写者），WAL 保留读并行 |
| BoltDB 并发写 | 单写者、多读者（MVCC）；写操作按 bbolt 事务串行提交 |
| Redis 的 `Set` | 用 `SET ... KEEPTTL` 保持既有 TTL（需 Redis ≥ 6.0） |
| Redis 键前缀 | 三类数据共用一个 keyspace，基座自动加 `kvdb:kv:` / `kvdb:q:` / `kvdb:z:` 前缀，保证命名空间独立；**前缀属于数据布局**，旧版本写入的裸 key 数据不再可见（`SCAN kvdb:kv:*` 可导出旧数据） |
| SQL 键长 | MySQL 键列上限 255 字节（兼容 5.6 默认索引前缀）；PostgreSQL/SQLite 用 BYTEA/BLOB 无此限制 |
| SQLite 旧库升级 | `kv_items.n` 列由开库迁移自动补列并回填整数投影（幂等），旧库无需手工处理 |
| 过期键的写语义 | 所有基座统一"已过期 = 不存在"：过期后 `Set` 不继承旧 TTL、`Incr` 从 0 起算、`Expire` 不复活 |
| 读返回值所有权 | `Get`/`MGet`/`Scan`/`QFront`/`QBack` 一律返回副本，调用方改写不影响库内状态（mem/jsonl 曾是内部切片别名） |

## 扩展：自定义基座

实现 `core.KvProvider`（KV 必选）+ 可选能力接口，注册后即可经 `kvdb.Open` 使用：

```go
type myStore struct{ /* ... */ }

func (m *myStore) Set(ctx context.Context, key string, value []byte) error { /* ... */ }
// ... 其余 KV 方法；可选再实现 core.QueueProvider / core.ZSetProvider / core.BatchProvider

func init() {
    kvdb.MustRegister("mybase", func(ctx context.Context, u *url.URL) (core.KvProvider, error) {
        return &myStore{}, nil
    })
}
// 业务侧 import _ "your/module/mybase"，随后 kvdb.Open(ctx, "mybase://...") 即可用
```

`kvdb.Register` 返回重复/空 scheme 错误；`kvdb.Schemes()` 列出已注册 scheme。
`FullProvider` 用于整体声明"KV + Queue + ZSet + Batch + Close"全部能力。

## 兼容性

| 依赖 | 已验证版本 |
|---|---|
| MySQL | 5.6.51、8.0.46 |
| PostgreSQL | 9.6、10、12、16 |
| SQLite | modernc.org/sqlite（纯 Go，需 3.35+ 支持 `RETURNING`） |
| Redis | 7.x |
| SSDB | 原生协议，支持 `server.auth` |

SQLite 采用纯 Go 驱动：实测在真实存储上，纯 Go 与 CGO（mattn/go-sqlite3）
吞吐基本一致（瓶颈是每事务的持久化，而非驱动实现），因此不引入 CGO 依赖。

## 性能

> 完整实测数据、各基座成本模型与选型建议见 **[PERFORMANCE.md](PERFORMANCE.md)**。

写入瓶颈通常是**每次提交的持久化（fsync）**，而不是语句数量。要点：

- **批量写收益最大**：1000 次 Set、每 100 条提交一次，实测提升
  MySQL 6.1×、PostgreSQL 3.3×、SQLite 49.7×、JSONL 66.4×、Redis 37.8×、SSDB 6.5×。
- **多 key 并发写**吃满组提交（MySQL/PG 连接池默认 32）；**单 key 计数器**受
  行锁与每次提交的 fsync 限制，是 SQL 基座的固有成本。
- 高频写入优先选 `redis` / `ssdb` 基座；JSONL 的写吞吐取决于存储介质，
  适合低频到中等写入的进程内持久化。
- 部署侧可调（会缩短崩溃恢复窗口，需自行确认持久性等级）：
  MySQL `innodb_flush_log_at_trx_commit=2`、PostgreSQL `synchronous_commit=off`。
- 基准可自行复测：`KVDB_BENCH_URI=<uri> go test -bench . -benchtime 2000x ./bench/`

## 测试

```bash
go test ./...        # 本地基座（mem / jsonl / sqlite）+ 进程内替身（miniredis、假 SSDB）
```

真实基座用例默认跳过，设置对应环境变量后启用（端口按需调整，避免与本地服务冲突）：

```bash
docker run -d --name kvdb-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=pw -e MYSQL_DATABASE=kvdb_test mysql:8.0
docker run -d --name kvdb-pg    -p 5432:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=kvdb_test postgres:16-alpine
docker run -d --name kvdb-redis -p 6379:6379 redis:7-alpine

KVDB_TEST_MYSQL_DSN='root:pw@tcp(127.0.0.1:3306)/kvdb_test' \
KVDB_TEST_PG_DSN='postgres://postgres:pw@127.0.0.1:5432/kvdb_test?sslmode=disable' \
KVDB_TEST_REDIS_ADDR=127.0.0.1:6379 \
  go test ./...
```

| 环境变量 | 用途 |
|---|---|
| `KVDB_TEST_MYSQL_DSN` | MySQL DSN（专用测试库，用例前会 DROP 本 SDK 的表） |
| `KVDB_TEST_PG_DSN` | PostgreSQL DSN（同上） |
| `KVDB_TEST_REDIS_ADDR` | Redis 地址（用例使用独立 DB 并清空） |
| `KVDB_TEST_SSDB_ADDR` | SSDB 地址（用例前 flushdb） |
| `KVDB_TEST_SSDB_AUTH_ADDR` / `KVDB_TEST_SSDB_AUTH_PASS` | 启用 `server.auth` 的 SSDB 实例 |

所有基座共用 `internal/behaviortest` 的合同用例（KV / Queue / ZSet / Batch，
含并发原子性）；SSDB 另用进程内假服务器交叉验证线协议编码。

## 目录结构

模块根 `github.com/RelicOfTesla/kvdb`：

```
core/                  契约：KvProvider / Queue- / ZSet- / BatchProvider / Closer / FullProvider
provider.go            Open 与契约再导出
db.go                  DB 接口与默认适配器（adapter）
batch.go               Batch 收集器与 DB.Batch 分发
bytes.go               B / P / D / DMust 字节编解码
registry.go            Register / MustRegister / Schemes
all/                   聚合注册包：import _ 即接入全部内置基座
mem/ jsonl/ sqlite/ mysql/ pg/ redis/ ssdb/   各基座实现
sqlstore/              MySQL / SQLite / PG 共享的 database/sql 实现（方言参数化）
internal/behaviortest/ 跨基座合同用例
example/               可运行演示
```

## License

尚未添加开源许可证文件；使用前请与作者确认授权。
