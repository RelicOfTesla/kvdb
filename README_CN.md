# kvdb

[English](README.md) | **中文**

Go 持久化适配器 SDK：对外提供统一的 **KV + Queue + ZSet** 接口，底层可插拔切换
不同持久化基座（内存 / jsonl / SQL / Redis / SSDB / BoltDB），并内置批量写与字节
编解码辅助。

语义以 **SSDB/Redis 家族**为准：命令**命名**取自 SSDB（`qpush`/`qpop`/`zset`…），
KV 行为在两者一致处对齐 Redis。有若干点**刻意与 Redis 不同**（重点：`Set` 保留 TTL、
`Scan` 是确定性范围查询而非 Redis 的游标式 `SCAN`）——逐条规则见 `core` 包文档，
[`core/provider.go`](core/provider.go) 是契约的唯一出处。

> 本工具由 AI 生成，不保证严谨与安全，使用需自酌。

```go
db, _ := kvdb.Open(ctx, "sqlite://./data.db")
defer db.Close()

db.Set(ctx, "user:1", []byte("alice"))
n, err := db.Incr(ctx, "visits", 1)
```

## 特性

- **11 个内置基座、同一套 API**：`mem` / `jsonl` / `bolt` / `leveldb` / `badger` / `sqlite` /
  `mysql` / `pg` / `mssql` / `redis` / `ssdb`
- **可选能力、按需探测**：KV 必选；Queue / ZSet / Batch / Close 为可选，未实现时返回
  `ErrUnsupported`，可经 `Capabilities()` 探测
- **默认空注册表**：用到哪个基座就 `import _` 哪个；根包不引入任何驱动依赖
- **接口化返回**：`kvdb.Open` 返回接口 `DB`，业务可只依赖子接口（如 `KvProvider`），便于 mock
- **批量写**：映射到各基座的原生机制（事务 / MULTI-EXEC / 流水线 / 一次 flush）
- **字节 ↔ 泛型辅助**：`Enc` / `Dec` / `D` / `DMust`；标量编码与 `Incr` 互操作
- **本地变远程**：任一基座可作 RPC 服务端；客户端不感知底座，含认证（明文 / 挑战）与可选 TLS，codec 可替换
- **可选 Go 1.27.1+ 类型化薄壳**：`kvdb.Typed(store)` 提供泛型读写方法
- **纯 Go、无 CGO**

| 模块 | 最低 Go |
|---|---|
| `kvdb`（根，含 `core`/`kvdbtest`）、`mem`、`jsonl`、`sqlstore`、`mssql` | 1.18 |
| `ssdb`、`leveldb`、`rpc` | 1.19 |
| `badger` | 1.24（Badger v4 自身声明 `go 1.24.0`） |
| `bolt`、`sqlite`、`mysql`、`pg`、`redis`、`all`、`example` | 1.25（驱动/传递依赖决定） |

版本取各模块依赖图中**最大的 `go` 指令**（`go list -m -f '{{.GoVersion}}' all`）。根包、
`mem`、`jsonl`、`rpc` 仅需标准库——只引入 `kvdb/mem` 的项目依赖闭包为空。

## 引入方式

```bash
go get github.com/RelicOfTesla/kvdb        # 根包
go get github.com/RelicOfTesla/kvdb/mem    # 各基座按需拉取
```

根包不依赖任何驱动——用到哪个基座就引入哪个（见「快速开始」）。

## 快速开始

```go
import (
    "github.com/RelicOfTesla/kvdb"
    _ "github.com/RelicOfTesla/kvdb/sqlite"   // 接入单个基座；_ .../all 则一次接入全部
)

db, _ := kvdb.Open(ctx, "sqlite://./data.db")
defer db.Close()

db.Set(ctx, "user:1", []byte("alice"))
v, ok, _ := db.Get(ctx, "user:1")
db.SetEx(ctx, "session", []byte("token"), 1800)
n, _ := db.Incr(ctx, "visits", 1)
pairs, _ := db.Scan(ctx, "user:", "user;", 100)   // 闭区间

db.QPush(ctx, "jobs", []byte("job-1"))            // Queue / ZSet 为可选能力
job, ok, _ := db.QPop(ctx, "jobs")
db.ZSet(ctx, "rank", "alice", 90)
top, _ := db.ZRange(ctx, "rank", 0, -1)
```

完整示例见 [`example/main.go`](example/main.go)。

## 内置基座

同一套 API 覆盖 11 个基座；URI 列即 scheme 参考。SQL 系用 `?table_prefix=` 加前缀，
`redis`/`ssdb` 用 `?key_prefix=`（逻辑前缀，读回时剥除，`Scan` 也限定其中）；不配置即当前布局。

| 基座 | 包 | URI 与要点 |
|---|---|---|
| 内存 | `mem` | `mem://` — 不持久化，测试/缓存用 |
| JSONL 日志 | `jsonl` | `jsonl://./d.jsonl` — 追加写 WAL，打开时回放，可 `Compact()`；缺省 flush 500ms + fsync 1s，`?sync=1` 逐操作，`?each_flush=1` 逐操作 flush |
| BoltDB | `bolt` | `bolt://./d.bolt` — 单文件 B+tree；缺省不逐提交 fsync、按 `?sync_interval=1s` 周期落盘，`?sync=1` 逐提交 |
| LevelDB | `leveldb` | `leveldb://./d.dir` — LSM；`?sync=1` 逐提交，`?cache`/`?wb` 单位 MiB |
| Badger | `badger` | `badger://./d.dir` — 带 MVCC 的 LSM；`?sync=1` 逐提交，`?cache`/`?memtable` 单位 MiB |
| SQLite | `sqlite` | `sqlite://./d.db` — 纯 Go 驱动（无 CGO）；缺省 NORMAL，`?sync=1` 即 FULL |
| MySQL | `mysql` | `mysql://user:pass@h:3306/db?parseTime=true` |
| PostgreSQL | `pg` | `pg://user:pass@h:5432/db?sslmode=disable` |
| SQL Server | `mssql` | `mssql://sa:pass@h:1433?database=db&encrypt=disable` |
| Redis | `redis` | `redis://:pass@h:6379/0` — 原生 String/List/Sorted Set |
| SSDB | `ssdb` | `ssdb://[user:pass@]h:8888` — 原生文本协议客户端，带连接池 |
| RPC | `rpc` | `rpc://h:7788?auth=challenge&password=…` — 客户端不感知底座 |

四个 SQL 基座共用 `sqlstore`；导入路径为 `github.com/RelicOfTesla/kvdb/<pkg>`
（另有聚合包 `.../all`）。

## 能力模型

`kvdb.Open` 返回接口 `DB`，内嵌各能力接口（`KvProvider` 必选；`QueueProvider` /
`ZSetProvider` / `Batcher` / `Closer` 可选）与 `Capabilities() core.Caps`。未实现的能力调用
返回 `ErrUnsupported`，故先探测：

```go
c := db.Capabilities()   // c.Queue / c.ZSet / c.Batch / c.BatchComposed
```

只依赖用到的接口即可（测试桩只需实现对应方法）。`BatchComposed` 表示同一批内后续操作能否
看到前序效果（LevelDB Batch / Redis MULTI-EXEC / SSDB 流水线为 `false`）；据此分支，或把
互相依赖的操作拆到不同批次。

## 批量写

整批一次提交，降低往返与持久化开销：

```go
err := db.Batch(ctx, func(b *kvdb.Batch) error {
    b.Set("k", value)
    b.QPush("jobs", payload)
    b.ZIncr("rank", "alice", 1)
    return nil   // 仅 nil 提交；收集期出错则整批不生效
})
```

基座只需实现 `ApplyBatch(ctx, ops)` 并映射到各自原生机制（一次 SQL 事务、一次 Redis
MULTI/EXEC、一次 SSDB 流水线、一次 jsonl flush、mem 一次持锁）。批内**只允许无条件写**
（`Set/SetEx/Del/Expire/QPush/QPushFront/ZSet/ZDel/ZIncr`）；`Incr`/`QPop` 依赖当前状态，
必须单独调用。原子性：事务型与进程内基座失败即整批不生效，而 **SSDB 无事务**，
流水线失败可能部分生效。

## 字节 ↔ T 辅助

```go
db.Set(ctx, "n", kvdb.Enc(int64(42)))     // 标量 -> 十进制文本（与 Incr 互操作）
db.Set(ctx, "u", kvdb.Enc(User{ID: 7}))   // 结构体 -> JSON

// D 合并 Get/QPop 的 (val, ok, err)，两者都原值透传（缺失 => ok=false, err=nil）
u, ok, err := kvdb.D[User](db.Get(ctx, "u"))
n := kvdb.DMust[int64](db.Get(ctx, "n"))  // panic 变体：只看 err（缺失返回零值）
raw, err := kvdb.Dec[User](b)
```

| 类型 | 编码 |
|---|---|
| 整数、float、string、bool | 文本（与 `Incr` 互操作） |
| `[]byte` | 恒等（不经 JSON/base64） |
| 结构体、切片、映射、指针等 | `Marshal`（默认 JSON） |

`kvdb.Marshal` / `kvdb.Unmarshal` 是包级变量，可在 init 中整体替换（msgpack / protobuf /
gob）；标量路径不受影响。`Enc` 编码失败会 panic——需要处理错误请直接调 `Marshal`。

### Go 1.27.1+：`TypedStore` 薄壳

`TypedStore` 只包 `StoreProvider`（`KvProvider + QueueProvider + ZSetProvider`），
不要求 `Batch`/`Close`：

```go
tdb := kvdb.Typed(db)                      // db 只需满足 kvdb.StoreProvider
u, ok, err := tdb.Get[User](ctx, "user:1")  // = kvdb.D[User](db.Get(ctx, "user:1"))
err = tdb.Set(ctx, "user:1", u)             // = db.Set(ctx, "user:1", kvdb.Enc(u))
n, ok, err := tdb.Get[int64](ctx, "visits") // 标量走文本编码，与 Incr 互操作
ms, err := tdb.MGet[User](ctx, "user:1", "user:2")
```

写为 `Set[T]` / `SetEx[T]` / `SetExAt[T]` / `QPush[T]` / `QPushFront[T]`（`Enc[T]` 编码）；
读为 `Get[T]` / `MGet[T]` / `QPop[T]` / `QPopBack[T]` / `QFront[T]` / `QBack[T]` / `QRange[T]`，
解码用 `Dec[T]`/`D[T]`。**凡值语义为 `[]byte` 的契约方法都有对应的 `T` 版本**；不携带值的
方法（`Del`/`Exists`/`Incr`/`Scan`/`TTL`/`QSize`/ZSet 各方法…）仍经内嵌 `StoreProvider` 直取，
不带类型参数。读方法采用 `D` 的签名——`(T, bool, error)`——缺失时
`ok=false` 且 `err=nil`。`T = []byte` 时与直接调用基座完全等价。`tdb.BatchT(ctx, func(b kvdb.TypedBatch) error {...})`
在收集时编码，**同一批可混装多种类型**；`Del`/`Expire`/`ZSet` 经内嵌 `*Batch` 仍可用。
泛型方法会遮蔽同名方法，故 `TypedStore` **不满足** `StoreProvider`（用 `tdb.StoreProvider`）。
由 `//go:build go1.27` 门禁。

## 扩展：自定义基座

实现 `core.KvProvider`（及所需可选能力接口），用 `kvdb.MustRegister(scheme, factory)` 注册，
即可经 `kvdb.Open` 使用。接口细节见 `core` 包文档与 [`example/`](example)。

## 本地变远程（RPC / c-s）

任一基座放在服务端，客户端经**同一套接口**通过网络访问——用于跨进程/跨机隔离，或把
嵌入式基座变成共享服务。

```go
srv, err := rpc.NewServer(ctx, rpc.ServerConfig{
    Addr: ":7788", Backend: "jsonl://./data.jsonl",   // bolt/sqlite/mysql/ssdb…
    Auth: rpc.AuthChallenge, Password: "s3cret",
})
go srv.Serve(ctx)

db, err := kvdb.Open(ctx, "rpc://127.0.0.1:7788?auth=challenge&password=s3cret")
db.Set(ctx, "k", []byte("v"))   // KV / Queue / ZSet / Batch 都可用
```

现成服务端：`go run ./example/rpcdemo -addr :7788 -backend jsonl://./data.jsonl -auth challenge -password s3cret`

客户端**不感知底座**（`rpc` 实现 `core.FullProvider`，且零第三方依赖），`Capabilities()`
报告的是**服务端底座**的能力。哨兵错误过线后仍可用 `errors.Is` 判等；批写一次往返；
客户端 `Close()` 只关自己的连接。

认证是 **c/s 协议自己的**（底座凭据由服务端负责）：`auth=none`（缺省）/ `auth=plain`
（口令上线）/ `auth=challenge`（nonce + `HMAC-SHA256`，口令不上线）。TLS 为标准
`crypto/tls`，由服务端配置在同一端口启用（`?tls=1&ca=./ca.pem`，双向再加 `cert=`/`key=`）。
codec 为 `resp`（缺省）/ `binary` / `textproto`，经 `?codec=` 选择，两端必须一致。
配置错误一律报错，不会静默降级。

## 兼容性

| 依赖 | 已验证版本 |
|---|---|
| MySQL | 5.6.51, 8.0.46 |
| PostgreSQL | 9.6, 10, 12, 16 |
| SQL Server | 2022（mcr.microsoft.com/mssql/server） |
| SQLite | modernc.org/sqlite（纯 Go，`RETURNING` 需 3.35+） |
| Redis | 7.x |
| SSDB | 原生协议，支持 `server.auth` |

## 性能

真实块设备（ext4）、各基座缺省档、8 goroutine、2s 窗口统计实际完成量（ops/s；`MGet条目`/
`批写条目` 为条目级 items/s）。完整数据、其余介质与 `?sync=1` 矩阵见
**[PERFORMANCE_CN.md](PERFORMANCE_CN.md)**。

| 基座 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 | 混合读 | 混合写 | 读保留率 |
|---|---|---|---|---|---|---|---|---|---|---|
| jsonl | ~353.6k | ~10.0M | ~190.1k | ~242.2k | ~639.3k | ~14.1M | ~454.7k | ~474k | ~105k | 5% |
| bolt | ~25.2k | ~591.1k | ~22.9k | ~26.9k | ~20.9k | ~2.6M | ~473.8k | ~170k | ~13.5k | 27% |
| leveldb | ~164.6k | ~1.0M | ~111.8k | ~122.4k | ~111.8k | ~1.0M | ~354.2k | ~98.7k | ~52.4k | 11% |
| badger | ~81.8k | ~274.4k | ~63.5k | ~34.0k | ~64.7k | ~608.2k | ~493.8k | ~76.4k | ~49.7k | 28% |
| sqlite | ~12.6k | ~62.9k | ~5.0k | ~5.8k | ~5.5k | ~450.9k | ~37.6k | ~40.1k | ~5.8k | 64% |

由此表可得两点：**先定持久化档位再挑基座**（缺省档不逐提交 fsync；同一介质上 `?sync=1`
代价 29–940×）；**批写是写侧主要杠杆**（缺省档 2.2–6.0×，`?sync=1` 档 39–86×）。

## 测试

各模块独立，故 `go test ./...` 只覆盖当前模块：

```bash
for m in $(find . -name go.mod -not -path './.*'); do (cd "$(dirname "$m")" && go test ./...) || exit 1; done
```

容器型基座在未设置 DSN 时跳过（容器均绑定回环地址）：`KVDB_TEST_MYSQL_DSN`、
`KVDB_TEST_PG_DSN`、`KVDB_TEST_MSSQL_DSN`、`KVDB_TEST_REDIS_ADDR`、`KVDB_TEST_SSDB_ADDR`、
`KVDB_TEST_SSDB_AUTH_ADDR`/`_PASS`。

各基座共用 `kvdbtest` 的合同用例（一文件一主题）。该套件断言**各基座行为一致**，仅当
`Capabilities()` 显式声明差异时才分支——因此任何**未声明**的分歧都会表现为失败；SSDB 另用
进程内假服务器交叉验证线协议编码。

## 目录结构

**每个子目录是一个独立 Go 模块**（各有 `go.mod`；跨模块依赖用 `require` + `replace`
指向同级目录，保证每个模块单独可 build / test / tidy）：

```
core/                  契约：接口、哨兵错误、能力声明、共享辅助
kvdbtest/              跨基座共享合同用例，一文件一主题
mem/ jsonl/ bolt/ leveldb/ badger/ sqlite/ mysql/ pg/ mssql/ redis/ ssdb/   各基座模块
rpc/                   RPC 客户端与服务端（+ rpc/codec：RESP / binary / textproto）
sqlstore/              sqlite/mysql/pg/mssql 共享的 database/sql 实现
all/                   聚合注册包（import _ 即接入全部基座）
example/               rpcdemo（RPC 服务端）、cmd/cli、cmd/migration、bench
```

根包文件：`provider.go`（Open 与再导出）、`db.go`（DB 接口与适配器）、`batch.go`、
`bytes.go`、`registry.go`。

## License

尚未添加开源许可证文件；使用前请与作者确认授权。
