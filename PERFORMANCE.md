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
| bolt（sync） | ~58k | ~900k | ~57k | ~27k |
| bolt（nosync） | ~50k | ~500k | ~54k | ~35k |
| leveldb（tmpfs） | ~186k | ~750k | ~152k | ~165k |
| leveldb（drvfs(9p)） | ~306 | ~750k | ~300 | ~300 |
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

注：`Get` 类数值已包含**返回值的深拷贝**（mem/jsonl 为保证调用方无法绕过 API
篡改库内状态，见 README「各基座差异」）；拷贝成本与 value 大小成正比，~20B 的
value 下可忽略（实测 mem 的 `Get` 与 `Set` 同为 ~210ns/op）。文件基座的 `Set`
在 drvfs(9p) 上受 fsync 影响极大：同一 bolt 代码在 9p 上约 8.0ms/op，在 /dev/shm
上约 16µs/op。

## 3. 写入成本模型（每操作）

| 操作 | mem | jsonl | bolt | leveldb | sqlite / pg | mysql | redis | ssdb |
|---|---|---|---|---|---|---|---|---|
| Set | map 写 | 1 次 append+flush | 1 个事务（1 fsync） | 1 次 `Write(batch)`（1 fsync） | 1 条 upsert | 1 条 upsert | 1 命令 | 1 往返 |
| Incr | map 写 | 1 次 append+flush | 1 个事务 | 分片锁内读-改-写 + 1 次 `Write` | 1 条 upsert+`RETURNING` | 事务：3 条语句 | 1 命令 | 1 往返 |
| QPush | map 写 | 1 次 append+flush | 1 个事务 | 1 次 `Write`（元素+计数器同批） | 事务：2 条语句 | 事务：4 条语句 | 1 命令 | 1 往返 |
| Batch(N) | N 次写（1 次持锁） | 1 次 write+flush | **1 个事务** | **1 个 Batch** | **1 个事务** | **1 个事务** | 1 次 MULTI/EXEC | 1 次流水线 |

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
| bolt | ~47k | ~45k | 单写者按事务串行（多 key 无额外收益） |
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
| BoltDB | 57,800 op/s | **446,000 op/s** | **7.7×** |
| SQLite | 124 op/s | 6,167 op/s | **49.7×** |
| JSONL | 1,764 op/s | 117,052 op/s | **66.4×** |
| Redis | 2,329 op/s | 88,027 op/s | **37.8×** |
| SSDB | 612 op/s | 3,979 op/s | **6.5×** |

批写把 N 次提交/往返摊成 1 次（SQL=事务、Redis=MULTI/EXEC、SSDB=流水线、
jsonl=一次 flush），是**唯一能跨数量级提升写入的手段**，且不牺牲原子性
（提交失败整批不生效；SSDB 无事务，流水线失败可能部分生效）。

> 口径说明：BoltDB/SQLite 行来自 `./bench`（进程内基座，同机同盘）；MySQL/PG/Redis/SSDB
> 行来自另一组对比探针（容器 + 9p 挂载），两者绝对值不可直接横比，只用于各自的前后对比。
> 表中 SQLite / JSONL 的绝对值是在 drvfs(9p) 挂载上测得的；同一份代码在 tmpfs 上更高
> （例如 SQLite 批写约 48k op/s），差异来自存储介质而非实现（见 §2、§7）。

SQL 的批内仍是逐条语句、往返次数未减少，所以提升小于 Redis/jsonl；后续可把
连续 `Set` 合并为多行 `INSERT` 进一步压缩往返。

## 6. BoltDB 基座（bbolt）

`bolt` 基座基于 `go.etcd.io/bbolt`（纯 Go 单文件 B+tree），同机同盘（9p）对比 SQLite：

| 基准 | bolt（默认） | bolt（nosync=1） | sqlite | 相对 sqlite |
|---|---|---|---|---|
| Set | 17.3 µs（58k/s） | 20.0 µs | 30.1 µs | **1.7×** |
| Get | **1.1 µs（900k/s）** | 2.0 µs | 31.1 µs | **28×** |
| Incr（单键） | 17.4 µs（57k/s） | 18.4 µs | 72.2 µs | **4.1×** |
| Incr 并发热点 | 21.3 µs | 23.5 µs | 96.4 µs | **4.5×** |
| Incr 并发多 key | 22.0 µs | 22.5 µs | 98.3 µs | **4.5×** |
| BatchSet100（单 iter=100 写） | 224 µs（2.2 µs/写） | 174 µs | 2,161 µs（21.6 µs/写） | **9.6×** |
| QPush | 37.6 µs | 28.3 µs | 117.7 µs | **3.1×** |

要点：

- **读极快**：bbolt 走 mmap/B+tree 查找，Get 在 µs 级；SQLite 每条查询都有语句准备与解析开销。
- **写由事务提交支配**：与 SQL 同因（一次提交一次 fsync），故单键 Incr 与 Set 同量级。
- **批写收益明显**：`db.Batch` 落到单个 bbolt 事务，100 条只提交一次（2.2 µs/写）。
- `nosync=1` 关闭 fsync：本轮多数项在噪声内（Batch/QPush 略快，Set/Get 略慢），
  收益不显著而牺牲崩溃持久性，**不建议默认开启**。
- 选型：需要"嵌入式 + 单文件 + 强于 SQLite 的点查/批量写"时选 `bolt`；
  需要 SQL、复杂查询或多进程共享同一文件时选 `sqlite`/`mysql`/`pg`
  （bbolt 文件同时只能被一个进程以写模式打开）。

## 6b. LevelDB 基座（syndtr/goleveldb）

`leveldb` 基座基于 `github.com/syndtr/goleveldb`（纯 Go LSM-tree）。实测在
`/dev/shm`（tmpfs）上，benchtime=2000x：

| 基准 | leveldb（默认 sync） | leveldb（nosync=1） |
|---|---|---|
| Set | 5.4 µs（186k/s） | 7.5 µs |
| Get | 1.3 µs（750k/s） | 2.0 µs |
| Incr（单键） | 6.6 µs（152k/s） | 5.3 µs |
| Incr 并发热点（同 key） | 7.7 µs | 5.4 µs |
| Incr 并发多 key | 10.1 µs | 11.3 µs |
| BatchSet100 | 207 µs（2.1 µs/写） | 221 µs（2.2 µs/写） |
| QPush | 6.1 µs（165k/s） | 5.0 µs |

> 未与 bbolt 并列比较：§6 的 bolt 数据取自 drvfs(9p)，介质不同不可直接对照；
> 需要二者横评时请在同一目录重跑（见 §10）。

要点：

- **介质主导**：同一份代码在 drvfs(9p) 上 Set 约 3.27 ms/op、在 tmpfs 上 5.4 µs/op，
  差近 600×；跨基座比较必须在同一介质上做。
- **写成本＝一次提交一次 fsync**：与 bbolt 同因，故 Set / Incr / QPush 同量级；
  批写把 100 次提交压成 1 次（2.1 µs/写，相对单写约 **84×**）。
- `nosync=1` 无稳定收益（本轮各项都在噪声内），却牺牲崩溃持久性，**不建议默认开启**。
- **无事务但有原子 Batch**：`Write(batch)` 一次 WAL 追加＋memtable 应用即原子；
  本基座把所有多键写（值+TTL、zset 双侧索引、队列元素+计数器）收进一个 Batch。
- **Batch 是写缓冲、读不到未提交内容**：同批内针对同一队列/zset 成员的多条操作
  各自按已提交状态计算，可能互相覆盖（如两次 `QPush` 只保留一条）。这属于契约允许
  的机制差异，本基座据实上报 `Capabilities().BatchComposed=false`，不做补偿；
  需要确定性组合时拆批或用单键操作。
- 选型：需要**写吞吐优先、可接受后台压缩抖动**（LSM 特性）时选 `leveldb`；
  需要强一致点查与 mmap 读性能时选 `bolt`；需要 SQL 能力时选 SQL 系基座。
  LevelDB 目录同样只能被一个进程以写模式打开。

## 7. SQLite 驱动选择：纯 Go vs CGO

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

## 8. 其他已落地的优化与实测效果

| 优化 | 效果 |
|---|---|
| SSDB 连接池（默认 8，`Config.PoolSize`） | 并发 Incr 1,810 → **9,226–11,060 op/s（5–6×）**，追平 Redis |
| PG/SQLite `Incr` 单语句化（upsert + `RETURNING`） | PG 237 → 512 op/s（2.2×） |
| PG/SQLite `QPush` 单语句分配序号（`UPDATE … RETURNING`） | 事务内 4 → 2 条语句 |
| MySQL/PG 连接池显式放开（默认 32） | 多 key 并发吃满组提交（§4） |
| mem/jsonl 读路径 `RWMutex` | 并发读不再互斥（`Get` 与内存同级） |
| jsonl 流式回放 | 打开时内存峰值从 ~2× 文件降到单行 |
| SQLite 进程内写串行化 | 消除多连接并发写的 `SQLITE_BUSY` |

## 9. 选型建议

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

## 10. 复现

基准位于 `bench/`，是一个独立模块（`import .../all`）；通过 `KVDB_BENCH_URI` 指定基座，
未设置时自动跳过。**在 `bench/` 目录内执行**：

```bash
cd bench

# 本地基座
KVDB_BENCH_URI=mem://                  go test -bench . -benchtime 2000x
KVDB_BENCH_URI=jsonl://./bench.jsonl   go test -bench . -benchtime 2000x
KVDB_BENCH_URI=sqlite://./bench.db     go test -bench . -benchtime 2000x
KVDB_BENCH_URI=bolt://./bench.bolt     go test -bench . -benchtime 2000x
KVDB_BENCH_URI='bolt://./bench.bolt?nosync=1' go test -bench . -benchtime 2000x
KVDB_BENCH_URI=/dev/shm/bench.ldb                go test -bench . -benchtime 2000x   # LevelDB 用目录
KVDB_BENCH_URI='/dev/shm/bench.ldb?nosync=1'    go test -bench . -benchtime 2000x

# 服务型基座（先起容器，见 README「测试」）
KVDB_BENCH_URI=redis://127.0.0.1:6379/0            go test -bench . -benchtime 2000x
KVDB_BENCH_URI=ssdb://127.0.0.1:8888               go test -bench . -benchtime 2000x
KVDB_BENCH_URI='mysql://root:pw@127.0.0.1:3306/db' go test -bench . -benchtime 2000x
KVDB_BENCH_URI='pg://postgres:pw@127.0.0.1:5432/db?sslmode=disable' go test -bench . -benchtime 2000x
```

包含的基准：`BenchmarkSet`、`BenchmarkGet`、`BenchmarkIncrSequential`、
`BenchmarkIncrParallelSameKey`、`BenchmarkIncrParallelMultiKey`、
`BenchmarkBatchSet100`（单操作吞吐 = 1/(ns/op) × 100）、`BenchmarkQPush`。

用 `-cpu 1,8` 可对比单核与多核；`-benchmem` 可看分配。报告中的表格可用同一
容器组合复测，量级应当一致（绝对值随机器波动）。
