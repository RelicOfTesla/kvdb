# kvdb

[English](README.md) | **中文**

Go 持久化适配器 SDK：对外提供统一的 **KV + Queue + ZSet** 接口，底层可插拔切换
不同持久化基座（内存 / jsonl / SQL / Redis / SSDB / BoltDB），并内置批量写与字节
编解码辅助。

语义以 **SSDB/Redis 家族**为准：命令**命名**取自 SSDB（`qpush`/`qpop`/`zset`…），
KV 行为在两者一致处对齐 Redis。有若干点**刻意与 Redis 不同**——完整清单见
[语义要点](#语义要点)（重点：`Set` 保留 TTL，以及 `Scan` 是确定性范围查询而非
Redis 的游标式 `SCAN`）。

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
- **字节 ↔ 泛型辅助**：`Enc` / `Dec` / `D` / `DMust` 支持标量与结构体（默认 JSON，编解码可替换），标量编码与 `Incr` 互操作。`D` 原值透传 `ok` 与 `err`；`DMust` 只看 `err`
- **本地变远程（c/s）**：`rpc` 把任一基座暴露成服务端，客户端用同一组接口访问；
  客户端**不感知服务端底座**，自带 c/s 认证（明文 / 挑战-响应）与可选 TLS，协议编解码可替换
- **Go 1.27.1+ 可选薄壳**：`kvdb.Typed(store)` 提供 `db.Get[T](...)`/`db.Set(ctx, k, v)` 读写泛型方法（构建约束隔离）
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
jsonl://./data.jsonl?sync=1        # 缺省 flush 每 500ms、fsync 每 1s；sync=1 逐操作 flush+fsync（掉电不丢）
jsonl://./data.jsonl?each_flush=1  # 逐操作 flush（fsync 仍按周期）；flush_interval/sync_interval 可调周期
sqlite://./data.db?sync=1          # 缺省 NORMAL（不逐提交 fsync，与其他本地基座一致）；sync=1 用 FULL（掉电不丢）
sqlite://./data.db?table_prefix=app_       # 表名前缀（mysql/pg 同名参数）
bolt://./data.bolt?sync=1&sync_interval=1s   # 缺省不逐提交 fsync，但按 sync_interval 周期落盘（缺省 1s）；sync=1 逐提交 fsync
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
db.Set(ctx, "n", kvdb.Enc(int64(42)))     // 标量 -> 十进制文本
db.Set(ctx, "u", kvdb.Enc(User{ID: 7}))   // 结构体 -> JSON

// 读：D 合并 Get/QPop 的 (val, ok, err) 三返回值，ok 与 err 都原值透传
//（缺失时 ok=false 且 err=nil）
n, ok, err := kvdb.D[int64](db.Get(ctx, "n"))
u, ok, err := kvdb.D[User](db.Get(ctx, "u"))
v, ok, err := kvdb.D[string](db.QPop(ctx, "jobs"))

// 需要"缺失即错误"时自行判断：
//   if err != nil { return err }   // IO / 解析错误
//   if !ok { return kvdb.ErrNotFound }

// panic 变体：只看 err（忽略 ok，缺失返回零值）
n := kvdb.DMust[int64](db.Get(ctx, "n"))
raw, err := kvdb.Dec[User](b)
```

编码规则：

| 类型 | 编码 | 说明 |
|---|---|---|
| 整数（含 `~` 别名）、float、string、bool | 文本 | 与 `Incr` 互操作（`Enc(int64)` → 十进制） |
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

注意 `Enc` 对编码失败会 panic（不返回 error）；需要错误处理时直接调用 `Marshal`。

### Go 1.27.1+：`TypedStore` 薄壳（读写都走类型参数）

`TypedStore` 与 `StoreProvider`（`KvProvider + QueueProvider + ZSetProvider`）
一一对照——泛型壳只依赖这一个接口，不要求 `Batch`/`Close`：

```go
tdb := kvdb.Typed(db)                      // db 满足 kvdb.StoreProvider 即可
u, err := tdb.Get[User](ctx, "user:1")     // = kvdb.D[User](db.Get(ctx, "user:1"))
err = tdb.Set(ctx, "user:1", u)            // = db.Set(ctx, "user:1", kvdb.Enc(u))
err = tdb.SetEx(ctx, "sess", s, 3600)
n, err := tdb.Get[int64](ctx, "visits")    // 标量走文本编码，与 Incr 互操作
job, err := tdb.QPush(ctx, "jobs", j)      // 队列写也是泛型
ms, err := tdb.MGet[User](ctx, "user:1", "user:2")
```

- **写**：`Set[T]` / `SetEx[T]` / `QPush[T]` / `QPushFront[T]`，编码 `Enc[T]`；
- **读**：`Get[T]` / `MGet[T]` / `QPop[T]` / `QPopBack[T]` / `QFront[T]` / `QBack[T]`，
  解码 `Dec[T]`/`D[T]`，空/缺失折算为 `ErrNotFound`。它们各有一个 `…OK` 变体
  （`GetOK` / `QPopOK` / `QPopBackOK` / `QFrontOK` / `QBackOK`），**保留 `ok` 原值**：
  `ok=false` 且 `err=nil`；
- `T = []byte` 时与直接调用基座方法**完全等价**（`Enc` 对 `[]byte` 恒等透传）；
- **批写**：`tdb.BatchT(ctx, func(b kvdb.TypedBatch) error {...})`——收集时即编码，
  **同一批可混装多种类型**（`b.Set("cnt", 42)` 与 `b.Set("u:1", u)` 并存）；
  `Del`/`Expire`/`ZSet` 等经内嵌 `*Batch` 直接可用；
- 泛型方法会遮蔽同名方法，**`TypedStore` 不满足 `StoreProvider`**（签名不同），
  需要原始接口时用 `tdb.StoreProvider`；参数是 `StoreProvider`（不含 Batch/Close），
  因此 `kvdb.Typed(db)` 对 `kvdb.Open` 的返回值同样可用；
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

### 与 Redis 的差异

命令**命名**取自 SSDB 而非 Redis。以下行为点也与 Redis 不同——从 Redis 迁移代码前请先了解：

| 主题 | Redis | kvdb |
|---|---|---|
| `Set` 与 TTL | **清除** TTL（要保留需 `KEEPTTL`） | **保留** TTL（SSDB `set` 语义）；Redis 基座内部用 `SET ... KEEPTTL` 对齐 |
| `Scan` | 游标式遍历，无序，只保证有限次遍历内覆盖全部 | 确定性**字节序闭区间**范围查询，升序，带 limit |
| `TTL` | `-2` 表示 key 不存在、`-1` 表示存在但无 TTL | 两者都收敛为 `ok=false`，无法区分 |
| `Del` / `Expire` | 返回受影响 key 数 | 只返回 `error` |
| `Exists` | 可传多 key，返回计数 | 单 key，返回 `bool` |
| `Incr` 溢出 | 始终报错 | SQL/Redis/SSDB 报错；mem/bolt/jsonl **静默回绕**（契约不统一承诺） |
| ZSet 分数 | IEEE-754 double（可有小数） | `int64`；Redis 基座在 `\|score\| ≤ 2^53` 内无损 |
| 列表命令 | `LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`LLEN`/`LINDEX` | `QPush`/`QPushFront`/`QPop`/`QPopBack`/`QSize`/`QFront`/`QBack` |
| 有序集命令 | `ZADD`/`ZSCORE`/`ZREM`/`ZCARD`/`ZINCRBY` | `ZSet`/`ZGet`/`ZDel`/`ZSize`/`ZIncr`（仅 `ZRank`/`ZRange` 沿用 Redis 名） |

`ZRange` 的 0 起闭区间索引、负索引从末尾数、以及 `(score, key)` 排序均与 Redis 完全一致。

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

> 完整实测数据、各基座成本模型与选型建议见 **[PERFORMANCE_CN.md](PERFORMANCE_CN.md)**。

口径：**固定时间窗内跑满并发负载、统计实际完成量**（ops/s）。要点：

- **缺省档不逐提交 fsync，`?sync=1` 才要断电安全**：同一真实 ext4 上，`sync=1` 比缺省档
  慢 29–940×。**"要不要 sync=1"比"选哪个基座"影响更大**，选型时应先定档位。
- **写瓶颈通常是每个事务一次 fsync**，而非语句条数：本环境裸 fsync 为
  tmpfs 2–4 µs、ext4(vhdx) ~2.5 ms、drvfs(9p) 3.8–5.5 ms。
- **批写收益随提交成本缩放**：缺省档 2.2–6.0×（bolt 达 18.8×，因它每写必开事务）；
  `sync=1` 档高达 39–86×。
- **同 key 热点在 SQL 上明显掉档**：mysql 多 key 428 vs 同 key 119 ops/s（3.6×）、
  pg 2.2k vs 647（3.4×）。计数器请分散 key 或改批写。
- **服务端基座**：redis 批写收益最大（39×），ssdb 因无事务只有 2.6×。
- **读写混合下读会被写压掉**：`mem`/`jsonl` 共用全局锁，高写频率时读只剩纯读的 5–9%；
  服务端基座则读写互不阻塞（各保留 ~44–63%）。见下方主表的混合读写列。
- 两个独立杠杆：**改用批写**、以及放宽部署侧持久化（MySQL
  `innodb_flush_log_at_trx_commit=2`、PostgreSQL `synchronous_commit=off`，
  会缩短崩溃恢复窗口，需自行确认可接受）。
- 复测：`cd bench && KVDB_BENCH_URI=<uri> go test -run '^$' -bench Throughput -benchtime 3s`

### 主表：ext4 + 缺省档（选型看这一张）

真实块设备（ext4/WSL vhdx）、各基座缺省档（不逐提交 fsync）。8 goroutine、时间窗
`-benchtime 2s`、统计实际完成量。`MGet条目` / `批写条目` 为折算到条目级的 items/s；
`Incr多key` 各写者独立 key，`Incr同key` 全部打同一个计数器。

`混合读` / `混合写` 来自混合负载基准（8 goroutine 中一半持续读 64-key 热集、一半写
独立 key）；`读保留率` = 混合读 ÷ 该基座纯读 Get。

| 基座 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 | 混合读 | 混合写 | 读保留率 |
|---|---|---|---|---|---|---|---|---|---|---
| jsonl | ~353.6k | ~10.0M | ~190.1k | ~242.2k | ~639.3k | ~14.1M | ~454.7k | ~474k | ~105k | 5% |
| bolt | ~25.2k | ~591.1k | ~22.9k | ~26.9k | ~20.9k | ~2.6M | ~473.8k | ~170k | ~13.5k | 27% |
| leveldb | ~164.6k | ~1.0M | ~111.8k | ~122.4k | ~111.8k | ~1.0M | ~354.2k | ~98.7k | ~52.4k | 11% |
| badger | ~81.8k | ~274.4k | ~63.5k | ~34.0k | ~64.7k | ~608.2k | ~493.8k | ~76.4k | ~49.7k | 28% |
| sqlite | ~12.6k | ~62.9k | ~5.0k | ~5.8k | ~5.5k | ~450.9k | ~37.6k | ~40.1k | ~5.8k | 64% |

**参照系（不同介质/部署，不与主表直接比较）**

| 基座 | 介质 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 | 混合读 | 混合写 | 读保留率 |
|---|---|---|---|---|---|---|---|---|---|---|---
| mem | 无 IO | ~775k | ~8.4M | ~825k | ~3.0M | ~2.85M | ~14.8M | ~1.48M | ~783k | ~251k | 9% |
| redis | 容器 ext4 | ~12.7k | ~12.4k | ~14.9k | ~13.4k | ~13.2k | ~256.4k | ~497.2k | ~7.1k | ~7.0k | 57% |
| ssdb | 容器 ext4 | ~8.0k | ~7.6k | ~7.7k | ~7.9k | ~8.2k | ~137.4k | ~20.6k | ~3.7k | ~3.5k | 48% |
| mysql 8.0 | 容器 ext4 | ~718 | ~5.3k | ~428 | ~119 | ~400 | ~95.6k | ~4.6k | ~2.9k | ~453 | 55% |
| pg 16 | 容器 ext4 | ~2.4k | ~9.9k | ~2.2k | ~647 | ~1.4k | ~185.7k | ~8.8k | ~5.3k | ~1.4k | 54% |

### 其余介质与档位矩阵

主表只覆盖"ext4 + 缺省档"，其余组合如下（换介质或换档位即换了比较基准）：

**`?sync=1`（逐提交 fsync）@ ext4**：只有写路径受影响，读与主表相同。

| 基座 | Set | QPush | 相对缺省档慢 |
|---|---|---|---|
| jsonl | ~377 | ~410 | ~940× |
| bolt | ~449 | ~464 | ~56× |
| leveldb | ~1.3k | ~1.3k | ~128× |
| badger | ~748 | ~736 | ~109× |
| sqlite | ~440 | — | ~29× |

**结论**

- **"要不要 `sync=1`"比"选哪个基座"影响更大**：缺省档把 fsync 从写热路径摘掉后，
  嵌入式写吞吐提升 1–2 个数量级；需要掉电不丢时 `sync=1` 的吞吐只有 ~377–1.3k ops/s，
  此时应优先用 `Batch` 摊薄提交。
- **读路径几乎不受介质影响**（leveldb Get 各档均 ~1.0M、badger ~272–298k），
  写路径跨介质差 2–3 个数量级。
- **服务端基座读吞吐低于嵌入式**（redis Get ~12.4k vs bolt ~590k），瓶颈是网络往返；
  但它天然"缺省即耐久"，无需在速度与安全间二选一。
- **同 key 热点在 SQL 上掉档最明显**：mysql 多 key 428 → 同 key 119（3.6×）、
  pg 2.2k → 647（3.4×）。计数器请分散 key 或改批写。
- **读写混合**：服务端基座读写互不阻塞（各保留 ~44–63%）；`mem`/`jsonl` 因共用全局锁，
  高写频率下读只剩纯读的 5–9%。`badger` 的读保留率由"写提交窗口"决定——`?sync=1` 时
  一次提交要 1.3–3 ms，读保留率只剩 0.2–0.4%，缺省档已消除该现象。
- 两个独立杠杆：**改用批写**、以及放宽部署侧持久化（MySQL
  `innodb_flush_log_at_trx_commit=2`、PostgreSQL `synchronous_commit=off`，
  会缩短崩溃恢复窗口，需自行确认可接受）。
- 复测：`cd bench && KVDB_BENCH_URI=<uri> go test -run '^$' -bench Throughput -benchtime 3s`

> 各基座成本模型（每操作做了什么）与更长的分析见 **[PERFORMANCE_CN.md](PERFORMANCE_CN.md)**。



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
bytes.go               Enc / Dec / D / DMust 字节编解码
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
