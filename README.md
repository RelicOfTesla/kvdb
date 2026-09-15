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

- **10 个内置基座**，同一套 API：`mem` / `jsonl` / `bolt` / `leveldb` / `badger` / `sqlite` / `mysql` / `pg` / `redis` / `ssdb`
- **能力可选、按需探测**：KV 必选；Queue / ZSet / Batch / 生命周期为可选能力，
  未实现时返回 `ErrUnsupported`，可用 `Capabilities()` 探测
- **注册表默认空**：用哪个基座就 `import _` 哪个包，根包与mod不引入任何驱动依赖
- **接口化返回**：`kvdb.Open` 返回接口 `DB`，业务可窄依赖 `KvProvider` 等子接口，便于 mock
- **批量写**：一批操作映射到各基座原生机制（事务 / MULTI/EXEC / 流水线 / 单次 flush）
- **字节 ↔ 泛型辅助**：`B` / `P` / `D` / `DMust` 支持标量与结构体（默认 JSON，编解码可替换），标量编码与 `Incr` 互操作
- **本地变远程（c/s）**：`rpc` 把任一基座暴露成服务端，客户端用同一组接口访问；
  客户端**不感知服务端底座**，自带 c/s 认证（明文 / 挑战-响应）与可选 TLS，协议编解码可替换
- **Go 1.27.1+ 可选薄壳**：`kvdb.Typed(db)` 提供 `db.Get[T](...)` 泛型方法（构建约束隔离）
- 纯 Go 依赖，无 CGO

**每个基座是独立模块**，各自声明所需的最低 Go 版本——只用 `mem` / `jsonl` / `ssdb`
的项目不会被 SQL / Redis 驱动的版本要求抬高：

| 模块 | 最低 Go | 说明 |
|---|---|---|
| `kvdb`（根，含 `core` / `kvdbtest`） | 1.18 | 零第三方依赖 |
| `kvdb/mem`、`kvdb/jsonl` | 1.18 | 仅标准库 |
| `kvdb/sqlstore` | 1.18 | 仅标准库（`database/sql` 抽象） |
| `kvdb/ssdb`、`kvdb/leveldb` | 1.19 | 用到 `atomic.Bool` / `atomic.Pointer[T]` |
| `kvdb/badger` | 1.24 | Badger v4 自身声明 `go 1.24.0` |
| `kvdb/bolt`、`kvdb/sqlite`、`kvdb/mysql`、`kvdb/pg`、`kvdb/redis` | 1.25 | 由驱动及其传递依赖决定（如 `golang.org/x/sys` 要求 1.25） |
| `kvdb/rpc` | 1.19 | 仅标准库 + 根包：**零第三方依赖**（客户端尤其重要） |
| `kvdb/all`、`kvdb/bench`、`kvdb/example` | 1.25 | 聚合了上述模块 |
| `kvdb/rpcserver` | 1.25 | 可直接运行的 RPC 服务端命令（import `all` 接入全部底座） |

版本按各模块**依赖图里最大的 `go` 指令**取（`go list -m -f '{{.GoVersion}}' all`），
不是照抄直接依赖的声明值。

实测：Go 1.20 的消费方只 import `kvdb/mem` 可正常构建运行，依赖闭包为空（不产生
go.sum）。

## 引入方式

```bash
go get github.com/RelicOfTesla/kvdb        # 根包
go get github.com/RelicOfTesla/kvdb/mem    # 按需引入各基座模块
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
jsonl://./data.jsonl?sync=1        # 默认逐操作 flush 不 fsync；sync=1 每次写 flush+fsync
jsonl://./data.jsonl?buffered=1    # 攒 32KiB 缓冲，空闲 100ms 自动落盘（约 2× 写吞吐）
sqlite://./data.db?sync=0          # 缺省 FULL（逐提交 fsync）；sync=0 用 NORMAL（更快，崩溃可能丢最近提交）
sqlite://./data.db?table_prefix=app_       # 表名前缀（mysql/pg 同名参数）
bolt://./data.bolt?nosync=1        # nosync=1 关闭 fsync（更快，崩溃可能丢最近提交）
leveldb://./data.dir?sync=1&cache=8&wb=4     # 目录型存储；默认不 fsync，sync=1 逐提交 fsync；cache/wb 单位 MiB
badger://./data.dir?sync=1&cache=64&memtable=64   # 目录型存储；默认不 fsync，sync=1 逐提交 fsync；cache/memtable 单位 MiB
mysql://user:pass@host:3306/dbname?parseTime=true&table_prefix=app_
pg://user:pass@host:5432/dbname?sslmode=disable&table_prefix=app_
redis://:password@host:6379/0?key_prefix=app:   # 键命名空间前缀
ssdb://host:8888?key_prefix=app:              # SSDB 无 namespace，用逻辑前缀隔离
ssdb://:password@host:8888         # 服务端启用 server.auth 时
rpc://host:7788?auth=challenge&password=s3cret   # 连 RPC 服务端（详见「本地变远程」）
```

**与其他应用共用一套存储时用前缀隔离**：`sqlite/mysql/pg` 的 `Config.TablePrefix`
给四张表和二级索引加前缀；`redis` 的 `Config.KeyPrefix` 派生 `<pfx>kv:` /
`<pfx>q:` / `<pfx>z:` 三段；`ssdb` 的 `Config.KeyPrefix` 给三类数据的键名加逻辑
前缀（读回时自动剥除，`Scan` 也夹在该前缀内）。默认值即当前布局，不配则不变。

完整演示见 [`example/main.go`](example/main.go)。

## 内置基座

| 基座 | 子包 | KV | Queue | ZSet | 说明 |
|---|---|---|---|---|---|
| 纯内存 | `mem` | ✅ | ✅ | ✅ | 不落盘，测试/缓存 |
| JSONL 日志 | `jsonl` | ✅ | ✅ | ✅ | append-only WAL，打开时回放，支持 `Compact()`；单进程内嵌 |
| BoltDB | `bolt` | ✅ | ✅ | ✅ | bbolt 单文件 B+tree（纯 Go）；每写一次事务提交，批写整批一次提交 |
| LevelDB | `leveldb` | ✅ | ✅ | ✅ | syndtr/goleveldb LSM-tree（纯 Go）；批写收进单个 Batch 原子提交 |
| Badger | `badger` | ✅ | ✅ | ✅ | dgraph-io/badger LSM-tree（纯 Go）；有 MVCC 事务，批写整批一个 `Update` 提交，**批内可见** |
| SQLite | `sqlite` | ✅ | ✅ | ✅ | 纯 Go 驱动（modernc），无 CGO |
| MySQL | `mysql` | ✅ | ✅ | ✅ | 共享 `sqlstore` |
| PostgreSQL | `pg` | ✅ | ✅ | ✅ | 共享 `sqlstore` |
| Redis | `redis` | ✅ | ✅ | ✅ | String / List / Sorted Set 原生映射 |
| SSDB | `ssdb` | ✅ | ✅ | ✅ | 原生文本协议客户端，连接池 + 认证 |
| RPC | `rpc` | ✅ | ✅ | ✅ | 连远端 kvdb 服务端；能力随服务端底座，客户端不感知底座 |

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
    Capabilities() core.Caps   // 实际具备的能力
}
```

- 基座未实现的能力：调用返回 `ErrUnsupported`，先用 `Capabilities()` 探测可避免。
  `core.Caps` 是结构体（新增能力不改签名）：

```go
c := db.Capabilities()
c.Queue, c.ZSet, c.Batch           // 是否实现对应接口
c.BatchComposed                    // 批内后续操作能否看到本批前序效果
```

- **批内可见性是可感知、非强制的能力**：同一批内多条操作涉及同一 key / 队列 /
  zset 成员时，终值取决于基座机制。在同一事务或同一把锁内逐条应用的基座
  （`mem` / `jsonl` / `bolt` / `badger` / `sqlite` / `mysql` / `pg`）为 `true`；
  LevelDB 的 Batch、Redis 的 MULTI/EXEC、SSDB 的流水线在提交前读不到未提交内容，
  为 `false`。需要确定性组合时，先探测再决定，或直接把相互依赖的操作拆批。
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

```go
tdb := kvdb.Typed(db)
u, err := tdb.Get[User](ctx, "user:1")    // = kvdb.D[User](db.Get(ctx, "user:1"))
n, err := tdb.Get[int64](ctx, "visits")   // 标量走文本编码，与 Incr 互操作
job, err := tdb.QPop[string](ctx, "jobs")
ms, err := tdb.MGet[User](ctx, "user:1", "user:2")
```

- 提供 `Get` / `GetOK` / `MGet` / `QPop` / `QPopBack` / `QFront` / `QBack`，
  缺失的 key 返回 `ErrNotFound`（`GetOK` 保留 `ok` 语义）；编码复用 `B`/`P`
  与可替换的 `Marshal`/`Unmarshal`；
- 其余方法经内嵌 `DB` 透传；泛型方法会遮蔽同名方法，**`TypedDB` 不满足 `DB`
  接口**，需要 DB 语义时用 `tdb.DB` 或保留原始 `db`；
- 版本与门禁：方法级类型参数自 Go 1.27 起支持，实现在带 `//go:build go1.27` 的文件中

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
| Redis 无字节序范围扫描 | redis 基座 SCAN KV 前缀 + 客户端过滤排序，代价与 KV 键数量相关 |
| Redis keyspace | 三类数据自动加 `kvdb:kv:` / `kvdb:q:` / `kvdb:z:` 前缀，保证命名空间独立 |
| Redis 的 `Set` | 用 `SET ... KEEPTTL` 保持既有 TTL（需 Redis ≥ 6.0） |
| SQL 过期行 | 读取路径过滤，开库时清理一次 |
| SQLite 并发写 | 进程内写串行化（单写者），WAL 保留读并行 |
| BoltDB 并发写 | 单写者、多读者（MVCC）；写操作按 bbolt 事务串行提交 |
| LevelDB 无 bucket / 无事务 | 单一有序键空间，三类数据用首字节命名空间标签隔离；`Write(batch)` 本身原子，所有多键写（含值+TTL、zset 双侧索引）都收进一个 Batch。**Batch 是写缓冲、读不到未提交内容**，故 `Capabilities().BatchComposed=false`：同批内针对同一队列/zset 成员的多条操作可能互相覆盖，需确定性组合请拆批 |
| LevelDB 并发 Incr | LevelDB 无 CAS 原语，同 key 的读-改-写由分片锁串行化（不同 key 仍并行） |
| Badger 有事务 | 多键写用 `db.Update` 事务提交，批内 read-your-writes（`BatchComposed=true`）。同 key 的读-改-写仍先用分片锁串行化：仅靠事务的 SSI 冲突重试也能保证正确，但同键热点会退化成重试风暴 |
| Badger 的 TTL | 不用原生 `WithTTL`（走真实时间），改为独立 TTL 记录 + 可注入时钟判定，与 leveldb 一致 |
| SQL 键长 | MySQL 键列上限 255 字节（兼容 5.6 默认索引前缀）；PG / SQLite 用 BYTEA/BLOB 无此限制 |
| 过期键的写语义 | 所有基座统一"已过期 = 不存在"：`Set` 不继承旧 TTL、`Incr` 从 0 起算、`Expire` 不复活 |
| 读返回值所有权 | `Get`/`MGet`/`Scan`/`QFront`/`QBack` 返回副本，调用方改写不影响库内状态 |

### 测试与调优用的可注入项

| 入口 | 作用 |
|---|---|
| `kvdb.Now` / `core.Now` | 基座取当前时刻的唯一入口（默认 `time.Now`）。测试里替换即可确定性触发 TTL 边界，不必 sleep 真实秒数 |
| `core.SweepInterval` | 写路径顺带回收过期条目的最小间隔（秒，默认 60）；置 0 表示每次写都回收 |
| `ssdb.DialTimeout` / `redis.ScanCount` | 连接超时、每轮 SCAN 的工作量提示 |

替换全局变量不是并发安全的做法：请在测试初始化阶段设置并用 `t.Cleanup` 还原。

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

## 本地变远程（RPC / c-s）

把任一基座放到服务端，客户端经网络用**同一组接口**访问它——用于跨进程/跨机隔离、
把嵌入式基座（jsonl/bolt/sqlite…）变成可供多个消费者共享的服务。

```go
// 服务端：选一个底座即可（server 侧 import 对应基座包或 .../all）
srv, err := rpc.NewServer(ctx, rpc.ServerConfig{
    Addr:     ":7788",
    Backend:  "jsonl://./data.jsonl",   // 换成 bolt/sqlite/mysql/ssdb… 客户端都不用改
    Auth:     rpc.AuthChallenge,
    Password: "s3cret",
})
go srv.Serve(ctx)

// 客户端：不 import 任何基座，也无需知道对端是什么
db, err := kvdb.Open(ctx, "rpc://127.0.0.1:7788?auth=challenge&password=s3cret")
defer db.Close()
db.Set(ctx, "k", []byte("v"))           // KV / Queue / ZSet / Batch 全部可用
```

也可直接跑现成的服务端命令：

```bash
go run ./rpcserver -addr :7788 -backend jsonl://./data.jsonl -auth challenge -password s3cret
```

要点：

- **客户端不感知底座**：`rpc` 实现的是 `core.FullProvider`，与本地基座同一组接口。
  连的是 jsonl 还是 mysql，客户端代码完全一致；换底座只改服务端一个参数。
- **`rpc` 模块零第三方依赖**（仅标准库 + 根包），客户端侧只引入 `kvdb` + `kvdb/rpc`。
- **能力如实透传**：`db.Capabilities()` 报告的正是**服务端底座**的能力，含
  `BatchComposed`——底层是 leveldb 就报 `false`，不会因为套了一层 RPC 而"变强"。
- **哨兵错误原样过线**：`ErrUnsupported` / `ErrClosed` / `ErrNotInteger` /
  `ErrInvalidTTL` / `ErrNotFound` 在客户端可用 `errors.Is` 正常判等。
- **批写一次往返**：`db.Batch(...)` 整批发给服务端，由底座一次提交；批内可见性
  取决于底座本身（与本地直连一致）。
- **生命周期边界**：客户端 `Close()` 只关自己的连接，不会关掉服务端基座。

### 认证（c/s 协议自己的认证，与底座 auth 无关）

| 模式 | URI 参数 | 说明 |
|---|---|---|
| 无认证 | `auth=none`（默认） | 本机/内网裸奔 |
| 明文 | `auth=plain&password=…` | 口令直接发送；**默认不打开**，仅在已有 TLS/unix socket 时用 |
| 挑战-响应 | `auth=challenge&password=…` | 服务端下发一次性 nonce，客户端回 `HMAC-SHA256(password, nonce)`；**口令不上线**，且重放无效。需要认证时选它 |

口令也可写在 userinfo 里：`rpc://:s3cret@host:7788?auth=challenge`。
给了口令却没写 `auth=` 会直接报错，避免"以为加密了其实没开"。

> 这里是**传输层**的认证。底座自身的认证（如 mysql 用户口令、ssdb `server.auth`）
> 由服务端在连接底座时处理，客户端不承担也不应感知。

### TLS

标准 `crypto/tls`，**同一端口按服务端配置切换**（给证书即 TLS，不给即明文）：

```bash
rpc://host:7788?tls=1&ca=./ca.pem                                  # 校验服务端
rpc://host:7788?tls=1&ca=./ca.pem&cert=./c.pem&key=./c.key         # 双向 TLS
rpc://host:7788?tls=1&server_name=kvdb.internal
rpc://host:7788?tls=1&insecure=1                                   # 跳过校验，仅测试
```

给了 TLS 参数却没写 `tls=1` 会报错；用明文连 TLS 端口会失败，**不会静默降级**。

### 协议与 codec

默认使用一套**仿 Redis（RESP2）**的报文格式，因此抓包可读、也能用 `nc` 手测：

```
$ nc 127.0.0.1 7788
$3
SET
*2
$1
k
$1
v

:1
+ok

```

编解码抽象成 `rpc/codec.Codec`，可整体替换；内置 `resp`（默认）与 `binary`
（uvarint 长度前缀，省掉文本转义）。两端必须装配同一个：

```go
rpc.ServerConfig{Codec: rpc.CodecBinary}
rpc.Config{Codec: rpc.CodecBinary}          // 或 URI 加 ?codec=binary
```

连接建立时双方会交换 codec 名称，不一致立即报错（而不是互等到超时）。

### 其他可用参数

| 参数 | 说明 |
|---|---|
| `pool=N` | 客户端连接池大小（默认 8）。单连接一次只跑一条命令，并发靠多连接 |
| `codec=resp\|binary` | 报文编解码 |
| `max-conns`（服务端） | 最大并发连接数 |

## 兼容性

| 依赖 | 已验证版本 |
|---|---|
| MySQL | 5.6.51、8.0.46 |
| PostgreSQL | 9.6、10、12、16 |
| SQLite | modernc.org/sqlite（纯 Go，需 3.35+ 支持 `RETURNING`） |
| Redis | 7.x |
| SSDB | 原生协议，支持 `server.auth` |

SQLite 采用纯 Go 驱动（modernc），吞吐与 CGO 驱动相当，不引入 CGO 依赖。

## 性能

> 完整实测数据、各基座成本模型与选型建议见 **[PERFORMANCE.md](PERFORMANCE.md)**。

口径：**固定时间窗内跑满并发负载、统计实际完成量**（ops/s）。要点：

- **批写收益随介质与持久化等级变化**：ext4(WSL vhdx) 上 sqlite 48.6×、bolt 86.9×、
  leveldb 73.1×、badger 38.9×；tmpfs 上多数为 2.5–7×（没有 fsync 可摊薄）。
- **写瓶颈通常是每个事务一次 fsync**，而非语句条数：本环境裸 fsync 为
  tmpfs 2–4 µs、ext4(vhdx) ~2.5 ms、drvfs(9p) 3.8–5.5 ms。
- **同 key 热点在 SQL 上明显掉档**：mysql 多 key 447 vs 同 key 116 ops/s（3.9×）、
  pg 2.25k vs 627（3.6×）。计数器请分散 key 或改批写。
- **服务端基座**：redis 批写收益最大（42×），ssdb 因无事务只有 2.6×。
- **读写混合下读会被写压掉**：`mem`/`jsonl` 共用全局锁，高写频率时读只剩纯读的 5–9%；
  服务端基座则读写互不阻塞（各保留 ~44–63%）。见下方「读写混合下的相互影响」。
- 两个独立杠杆：**改用批写**、以及放宽部署侧持久化（MySQL
  `innodb_flush_log_at_trx_commit=2`、PostgreSQL `synchronous_commit=off`，
  会缩短崩溃恢复窗口，需自行确认可接受）。
- 复测：`cd bench && KVDB_BENCH_URI=<uri> go test -run '^$' -bench Throughput -benchtime 3s`

### 实测吞吐一览（ops/s）

8 goroutine、时间窗 `-benchtime 2s`、统计实际完成量。`MGet条目` / `批写条目` 为折算到
条目级的 items/s；`Incr多key` 各写者独立 key，`Incr同key` 全部打同一个计数器。
文件基座的 `· tmpfs / · ext4 / · 9p` 是存储介质档位（ext4 = WSL vhdx；服务端基座容器盘
同为 ext4，可与 `· ext4` 档类比）。

| 基座 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 |
|---|---|---|---|---|---|---|---
| mem | ~775k | ~8.4M | ~825k | ~3.0M | ~2.85M | ~14.8M | ~1.48M |
| jsonl · tmpfs | ~201.8k | ~10.1M | ~221.9k | ~298.2k | ~286.3k | ~13.4M | ~510.5k |
| bolt · tmpfs | ~22.1k | ~569.8k | ~22.9k | ~26.9k | ~20.9k | ~2.2M | ~479.1k |
| leveldb · tmpfs | ~137.0k | ~1.0M | ~111.8k | ~122.4k | ~108.5k | ~1.0M | ~381.8k |
| badger · tmpfs | ~67.0k | ~274.4k | ~63.5k | ~34.0k | ~53.1k | ~638.4k | ~461.5k |
| sqlite · tmpfs | ~12.0k | ~60.6k | ~5.7k | ~5.8k | ~7.3k | ~447.9k | ~35.6k |
| jsonl · ext4 | ~177.0k | ~10.0M | ~190.1k | ~242.2k | ~315.3k | ~14.1M | ~548.7k |
| bolt · ext4 | ~426 | ~591.1k | ~461 | ~481 | ~518 | ~2.6M | ~38.6k |
| leveldb · ext4 | ~1.3k | ~1.0M | ~1.4k | ~448 | ~1.4k | ~1.0M | ~92.8k |
| badger · ext4 | ~757 | ~272.0k | ~741 | ~620 | ~724 | ~608.2k | ~29.4k |
| sqlite · ext4 | ~559 | ~61.3k | ~312 | ~316 | ~358 | ~453.0k | ~30.4k |
| jsonl · 9p | ~1.4k | ~9.6M | ~1.5k | ~1.5k | ~1.6k | ~13.3M | ~110.3k |
| bolt · 9p | ~89 | ~589.5k | ~91 | ~88 | ~94 | ~2.6M | ~7.1k |
| leveldb · 9p | ~1.0k | ~1.1M | ~1.0k | ~288 | ~1.1k | ~1.0M | ~75.2k |
| badger · 9p | ~321 | ~297.6k | ~312 | ~286 | ~338 | ~622.8k | ~26.8k |
| sqlite · 9p | ~234 | ~9.6k | ~129 | ~151 | ~105 | ~102.0k | ~16.1k |
| redis | ~12.7k | ~12.4k | ~14.9k | ~13.4k | ~13.2k | ~256.4k | ~497.2k |
| ssdb | ~8.0k | ~7.6k | ~7.7k | ~7.9k | ~8.2k | ~137.4k | ~20.6k |
| mysql 8.0 | ~718 | ~5.3k | ~428 | ~119 | ~400 | ~95.6k | ~4.6k |
| pg 16 | ~2.4k | ~9.9k | ~2.2k | ~647 | ~1.4k | ~185.7k | ~8.8k |

① jsonl 默认只 flush 到 OS、不逐条 fsync（`?sync=1` 才是每写一次 fsync）：比较时
须先对齐持久化等级。

读路径几乎不受介质影响（leveldb Get 三档均 ~1.0M、badger ~272–298k），而写路径跨介质差 2–3 个数量级
（bolt Set：22.1k → 426 → 89）。服务端基座的**读**吞吐低于嵌入式（redis Get ~12.4k vs
bolt ~590k），瓶颈是网络往返。完整分析见 [PERFORMANCE.md](PERFORMANCE.md)。

### 读写混合下的相互影响

上表是纯读 / 纯写各自的吞吐。混合负载（8 goroutine 中一半持续读 64-key 热集、一半写
独立 key）下两者会互相影响，"读保留率"= 混合 `reads/s` ÷ 同介质纯读 Get：

| 基座 | reads/s | writes/s | 读保留率 |
|---|---|---|---|
| mem | ~783k | ~251k | 9% |
| jsonl · tmpfs | ~485k | ~101k | 5% |
| jsonl · ext4 | ~486k | ~106k | 5% |
| jsonl · 9p | ~4.45M | ~1.5k | 46% |
| bolt · tmpfs | ~166k | ~12.9k | 29% |
| bolt · ext4 | ~500k | ~491 | 85% |
| bolt · 9p | ~319k | ~119 | 54% |
| leveldb · tmpfs | ~109k | ~58.8k | 11% |
| badger · tmpfs | ~74.6k | ~40.3k | 27% |
| leveldb · ext4 | ~736k | ~827 | 74% |
| badger · ext4 | ~1.0k | ~689 | 0.4% |
| leveldb · 9p | ~658k | ~658 | 60% |
| badger · 9p | ~497 | ~314 | 0.2% |
| sqlite · tmpfs | ~37k | ~6.7k | 61% |
| sqlite · ext4 | ~74k | ~409 | 121% |
| sqlite · 9p | ~19k | ~85 | 193% |
| redis | ~7.1k | ~7.0k | 57% |
| ssdb | ~3.7k | ~3.5k | 48% |
| mysql 8.0 | ~2.9k | ~453 | 55% |
| pg 16 | ~5.3k | ~1.4k | 54% |

- **服务端基座读写互不阻塞**（各保留 ~44–63%，只是把并发度对半分）；`mem`/`jsonl`
  因共用全局锁，高写频率下读只剩纯读的 5–9%。
- **`badger` 的读保留率由"写提交窗口"决定**：默认逐条 fsync 时，一次提交在 9p/ext4
  上要 1.3–3 ms，读者排在提交锁之后，读保留率只剩 0.2–0.4%；换成 `?nosync=1` 后同一
  组负载在 9p 上读回到 92.7k ops/s（保留率 ~33%）。这是它与其他嵌入式基座最不一样的地方。
- **掉幅取决于写者进入共享同步原语的频率，而非介质带宽**：同一基座的写频率越低
  （如 9p 上），读保留率越高——盘慢反而让读者更容易穿插。所以"Get 比 Set 快几十倍"
  只在低写负载下成立。
- 读保留率 >100% 是该档纯读基准的调度与页缓存波动，属噪声量级。


## 测试

**每个模块独立**，`go test ./...` 只覆盖当前模块；跑全部模块用：

```bash
# 跑全部模块（. 开头的目录都是本地脚手架，不属于仓库）
for m in $(find . -name go.mod -not -path './.*'); do
    (cd "$(dirname "$m")" && go test ./...) || exit 1
done
```

单模块跑法（跨模块依赖由各 go.mod 的 replace 解析，无需工作区文件）：

```bash
cd mem    && go test ./...     # 本地基座 + 进程内替身（miniredis、假 SSDB）
cd sqlite && go test ./...
cd rpc    && go test ./...     # RPC：合同用例跨 RPC、三种认证、TLS、codec、多种真实底座
```

真实基座用例默认跳过，设置对应环境变量后启用（端口按需调整，避免与本地服务冲突）：

```bash
# 测试容器只绑回环地址，避免弱口令测试服务暴露到局域网。
docker run -d --name kvdb-mysql -p 127.0.0.1:3306:3306 -e MYSQL_ROOT_PASSWORD=pw -e MYSQL_DATABASE=kvdb_test mysql:8.0
docker run -d --name kvdb-pg    -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=kvdb_test postgres:16-alpine
docker run -d --name kvdb-redis -p 127.0.0.1:6379:6379 redis:7-alpine

KVDB_TEST_MYSQL_DSN='root:pw@tcp(127.0.0.1:3306)/kvdb_test' \
KVDB_TEST_PG_DSN='postgres://postgres:pw@127.0.0.1:5432/kvdb_test?sslmode=disable' \
KVDB_TEST_REDIS_ADDR=127.0.0.1:6379 \
  go test ./...          # 在对应基座模块目录内执行
```

| 环境变量 | 用途 |
|---|---|
| `KVDB_TEST_MYSQL_DSN` | MySQL DSN（专用测试库，用例前会 DROP 本 SDK 的表） |
| `KVDB_TEST_PG_DSN` | PostgreSQL DSN（同上） |
| `KVDB_TEST_REDIS_ADDR` | Redis 地址（用例使用独立 DB 并清空） |
| `KVDB_TEST_SSDB_ADDR` | SSDB 地址（用例前 flushdb） |
| `KVDB_TEST_SSDB_AUTH_ADDR` / `KVDB_TEST_SSDB_AUTH_PASS` | 启用 `server.auth` 的 SSDB 实例 |

所有基座共用根模块内 `kvdbtest` 的合同用例（KV / Queue / ZSet / Batch /
过期写语义 / 返回值所有权 / 命名空间，含并发原子性）；SSDB 另用进程内假服务器
交叉验证线协议编码。

根模块的注册表/编解码用例用一个**测试桩**（`stub_test.go` 注册的 `stub://`）验证
"注册表默认空 + 显式接入"语义，因此根模块自身零第三方依赖；真实基座的"import 即
自注册"由各基座模块自己的用例覆盖（如 `mem/registry_test.go`）。

## 目录结构

**每个子目录是一个独立 Go 模块**（各有 go.mod；跨模块依赖用 require + replace
指向同级目录，保证每个模块单独可 build / test / tidy）：

```
core/                  契约：KvProvider / Queue- / ZSet- / BatchProvider / Closer / FullProvider
provider.go            Open 与契约再导出
db.go                  DB 接口与默认适配器（adapter）
batch.go               Batch 收集器与 DB.Batch 分发
bytes.go               B / P / D / DMust 字节编解码
registry.go            Register / MustRegister / Schemes
kvdbtest/              跨基座共享合同用例（公开包，供各基座模块测试引用）
all/                   聚合注册包：import _ 即接入全部内置基座
mem/ jsonl/ bolt/ leveldb/ badger/ sqlite/ mysql/ pg/ redis/ ssdb/   各基座实现（各自独立模块）
rpc/                   RPC 客户端与服务端（独立模块，零第三方依赖）
rpc/codec/             报文编解码抽象 + RESP（默认）/ binary 两套实现
rpcserver/             可运行的 RPC 服务端命令（独立模块，import .../all）
sqlstore/              MySQL / SQLite / PG 共享的 database/sql 实现（方言参数化）
bench/                 基准测试（独立模块，import .../all）
example/               可运行演示
```

## License

尚未添加开源许可证文件；使用前请与作者确认授权。
