# 性能报告

`kvdb` 各基座的实测性能、成本模型与选型建议。

> 数据为单机容器环境的相对量级，**不是承诺值**：绝对值受 CPU、存储介质、网络与
> 容器配置影响很大。请以同机复测（见文末「复现」）为准。

## 1. 测试环境与方法

| 项 | 说明 |
|---|---|
| 主机 | WSL2（Windows 11），8 核可用，Docker 与本机同宿 |
| Go | go1.27.1 linux/amd64 |
| 服务端 | `mysql:8.0.46`、`postgres:16`（另测 5.6 / 9.6 / 10 / 12）、`redis:7`、SSDB 容器 |
| 文件基座 | 工作区为 drvfs(9p)（Windows 挂载）或 `/tmp`（tmpfs），报告标注介质 |
| 数据规模 | key 短（`bench:*`），value ~20 字节，页缓存已热 |
| 测量方式 | 单 goroutine 顺序调用（吞吐 = 1/平均延迟）；并发项为 8 goroutine |

两组并发写必须区分，结论完全不同：

- **同 key（写热点）**：单个计数器，受行锁/单点串行限制；
- **多 key（无重叠）**：各 goroutine 写独立 key，SQL 可吃满组提交。

## 2. 单操作吞吐（顺序调用，op/s）

| 基座 | Set | Get | Incr | QPush |
|---|---|---|---|---|
| mem | ~1.5M | ~3.5M | ~2.7M | ~3.2M |
| jsonl（drvfs(9p)） | ~1.4k | ~2.6M | ~1.7k | ~1.8k |
| jsonl（tmpfs） | ~40k | ~2.6M | ~**200k** | — |
| sqlite（drvfs(9p)） | ~600 | ~2.4k | ~120–136 | ~91–95 |
| sqlite（tmpfs） | ~24k | ~25k | ~10.6k | ~6.1k |
| ssdb | ~1.7k–3.7k | ~1.6k–4.0k | ~1.8k–3.8k | ~1.6k–3.4k |
| redis | ~2.1k–4.2k | ~2.3k–4.1k | ~2.4k–4.6k | ~2.0k–3.6k |
| mysql（8.0） | ~900 | ~1.0k | ~90–160 | ~83–105 |
| pg（16） | ~500 | ~1.7k | ~240–512① | ~209–245 |

① PG 的 `Incr` 采用单语句 `upsert + RETURNING`（缺失按 0、原子累加、非整数报错），
实测 237 → 512 op/s（约 2.2×）；MySQL 无 `RETURNING` 保留事务多语句路径。

**关键观察**

- `mem` 是内存基线；`jsonl`/`sqlite` 的**读**与内存同级（读不走文件），**写**受
  文件追加/事务提交支配；
- 文件基座的写吞吐几乎完全由**存储介质**决定（同一份代码：jsonl 在 tmpfs 上
  Incr 约 20 万 op/s，在 drvfs(9p) 上约 1.7k op/s，差 ~100 倍）；
- SSDB/Redis 单操作受**网络往返**支配（本机容器 ~0.4–0.6 ms/次），并发靠连接池放大。

## 3. 写入成本模型（每操作）

| 操作 | mem | jsonl | sqlite / pg | mysql | redis | ssdb |
|---|---|---|---|---|---|---|
| Set | map 写 | 1 次 append+flush | 1 条 upsert | 1 条 upsert | 1 命令 | 1 往返 |
| Incr | map 写 | 1 次 append+flush | 1 条 upsert+`RETURNING` | 事务：3 条语句 | 1 命令 | 1 往返 |
| QPush | map 写 | 1 次 append+flush | 事务：2 条语句 | 事务：4 条语句 | 1 命令 | 1 往返 |
| Batch(N) | N 次写（1 次持锁） | 1 次 write+flush | **1 个事务** | **1 个事务** | 1 次 MULTI/EXEC | 1 次流水线 |

SQL 写入的瓶颈是**每个事务一次持久化（fsync）**，而不是语句条数。同机隔离实验
（MySQL，300 次单语句自增）：

| 配置 | 单语句自增 | 事务 3 语句 |
|---|---|---|
| `innodb_flush_log_at_trx_commit=1`（默认） | 131 op/s | 127 op/s |
| `innodb_flush_log_at_trx_commit=2` | **266 op/s** | 174 op/s |

即：把 3 条语句压成 1 条只带来约 3% 提升，而放宽持久化等级带来约 2×；PostgreSQL
同理（`synchronous_commit=off` 时 upsert 自增 490 → 1280 op/s）。

## 4. 并发写：同 key vs 多 key（8 goroutine，Incr）

| 基座 | 同 key | 多 key | 说明 |
|---|---|---|---|
| mem | ~250k | ~2.4M | 锁竞争 vs 无竞争 |
| ssdb | ~10.9k | ~12.3k | 连接池后可并行（见 §7） |
| redis | ~10.5k | ~11.8k | go-redis 连接池 |
| jsonl | ~1.8k | ~1.8k | 写路径必须串行（单日志文件） |
| sqlite | ~120 | ~120 | 进程内写串行化（单写者） |
| mysql | ~160 | **~694** | 多 key 吃满 InnoDB 组提交（4.3×） |
| pg | ~619 | **~2,196** | 多 key 吃满 WAL 组提交（3.5×） |

**结论**：SQL 基座的并发写要**分散到不同 key** 才能受益于组提交（多个事务共享
一次 fsync）；单 key 热点（计数器、单队列）在 SQL 上是行锁串行 + 每次提交 fsync，
这是固有成本，不应期待并发扩容。

## 5. 批量写（`db.Batch`，每 100 条提交一次）

1000 次 Set，对比逐条写与批写：

| 基座 | 逐条 | 批写 100 | 提升 |
|---|---|---|---|
| MySQL | 130 op/s | 793 op/s | **6.1×** |
| PostgreSQL | 521 op/s | 1,715 op/s | **3.3×** |
| SQLite | 124 op/s | 6,167 op/s | **49.7×** |
| JSONL | 1,764 op/s | 117,052 op/s | **66.4×** |
| Redis | 2,329 op/s | 88,027 op/s | **37.8×** |
| SSDB | 612 op/s | 3,979 op/s | **6.5×** |

批写把 N 次提交/往返摊成 1 次（SQL=事务、Redis=MULTI/EXEC、SSDB=流水线、
jsonl=一次 flush），是**唯一能跨数量级提升写入的手段**，且不牺牲原子性
（提交失败整批不生效；SSDB 无事务，流水线失败可能部分生效）。

> 表中 SQLite / JSONL 的绝对值是在 drvfs(9p) 挂载上测得的；同一份代码在 tmpfs 上更高
> （例如 SQLite 批写约 48k op/s），差异来自存储介质而非实现（见 §2、§6）。

SQL 的批内仍是逐条语句、往返次数未减少，所以提升小于 Redis/jsonl；后续可把
连续 `Set` 合并为多行 `INSERT` 进一步压缩往返。

## 6. SQLite 驱动选择：纯 Go vs CGO

同一 `sqlstore` 实现，仅替换底层驱动（pure-Go = `modernc.org/sqlite`，
CGO = `mattn/go-sqlite3`）：

| 介质 | 驱动 | Set | Get | Incr | QPush |
|---|---|---|---|---|---|
| tmpfs（fsync 近乎免费） | pure-Go | 23,875 | 25,361 | 10,573 | 6,141 |
| tmpfs | CGO | 37,158 | 45,177 | 18,951 | 6,674 |
| 真实盘 | pure-Go | 543 | 1,917 | 130 | 91 |
| 真实盘 | CGO | 568 | 2,178 | **128** | **94** |

CGO 只在 I/O 免费时快 1.6–1.8×；真实存储上两者基本一致（瓶颈是每事务持久化，
而非驱动实现）。因此本项目选用纯 Go 驱动，免去 CGO/gcc/交叉编译成本。

## 7. 其他已落地的优化与实测效果

| 优化 | 效果 |
|---|---|
| SSDB 连接池（默认 8，`Config.PoolSize`） | 并发 Incr 1,810 → **9,226–11,060 op/s（5–6×）**，追平 Redis |
| PG/SQLite `Incr` 单语句化（upsert + `RETURNING`） | PG 237 → 512 op/s（2.2×） |
| PG/SQLite `QPush` 单语句分配序号（`UPDATE … RETURNING`） | 事务内 4 → 2 条语句 |
| MySQL/PG 连接池显式放开（默认 32） | 多 key 并发吃满组提交（§4） |
| mem/jsonl 读路径 `RWMutex` | 并发读不再互斥（`Get` 与内存同级） |
| jsonl 流式回放 | 打开时内存峰值从 ~2× 文件降到单行 |
| SQLite 进程内写串行化 | 消除多连接并发写的 `SQLITE_BUSY` |

## 8. 选型建议

1. **高频写 / 队列 / 计数器**：`redis` 或 `ssdb`（单操作 2–4k op/s，并发 10k+）；
   需要持久化到文件且单进程内嵌时用 `jsonl`（写受介质限制）。
2. **SQL 基座**：适合数据量中等、写频率不高、需要 SQL/事务语义的场景；
   写多时务必用 `db.Batch` 并按 key 打散并发。
3. **单 key 热点计数**：SQL 上限约 100–600 op/s；要更高请用 `redis`/`ssdb`，
   或把计数聚合到应用层再批量落库。
4. **部署侧可调项**（会缩短崩溃恢复窗口，需自行确认持久性等级）：
   MySQL `innodb_flush_log_at_trx_commit=2`、PostgreSQL `synchronous_commit=off`
   —— 写入约 2–2.6×，代价是崩溃可能丢最近约 1 秒已提交事务（原子性不受影响）。
5. **不要为性能换存储引擎**：同机实验里 MyISAM 约为 InnoDB 的 2×，但无事务、
   崩溃易损毁，本项目不采用；MySQL 官方发行版亦无 RocksDB 引擎。

## 9. 复现

基准位于 `./bench/`，通过 `KVDB_BENCH_URI` 指定基座；未设置时自动跳过（不影响 `go test ./...`）：

```bash
# 本地基座
KVDB_BENCH_URI=mem://                  go test -bench . -benchtime 2000x ./bench/
KVDB_BENCH_URI=jsonl://./bench.jsonl   go test -bench . -benchtime 2000x ./bench/
KVDB_BENCH_URI=sqlite://./bench.db     go test -bench . -benchtime 2000x ./bench/

# 服务型基座（先起容器，见 README「测试」）
KVDB_BENCH_URI=redis://127.0.0.1:6379/0            go test -bench . -benchtime 2000x ./bench/
KVDB_BENCH_URI=ssdb://127.0.0.1:8888               go test -bench . -benchtime 2000x ./bench/
KVDB_BENCH_URI='mysql://root:pw@127.0.0.1:3306/db' go test -bench . -benchtime 2000x ./bench/
KVDB_BENCH_URI='pg://postgres:pw@127.0.0.1:5432/db?sslmode=disable' go test -bench . -benchtime 2000x ./bench/
```

包含的基准：`BenchmarkSet`、`BenchmarkGet`、`BenchmarkIncrSequential`、
`BenchmarkIncrParallelSameKey`、`BenchmarkIncrParallelMultiKey`、
`BenchmarkBatchSet100`（单操作吞吐 = 1/(ns/op) × 100）、`BenchmarkQPush`。

用 `-cpu 1,8` 可对比单核与多核；`-benchmem` 可看分配。报告中的表格可用同一
容器组合复测，量级应当一致（绝对值随机器波动）。
