# 性能报告

[English](PERFORMANCE.md) | **中文**

`kvdb` 各基座的实测吞吐、成本模型与选型建议。

> 数字为单机容器环境的相对量级，**不是承诺值**；绝对值受 CPU、存储介质、网络与
> 容器配置影响很大。复现命令见 §6。

## 1. 口径与方法

只用一种口径：**固定时间窗内压满并发负载，统计实际完成量**（ops/s；批写折算到条目级
items/s）。基准为 `example/bench/throughput_test.go` 的 `BenchmarkThroughput*`，8 goroutine，
`-benchtime` 控制窗口长度。

写路径普遍带合并与异步成分（SQL 组提交、SQLite WAL checkpoint 搭车、LSM memtable 批量落盘、
jsonl 缓冲落盘、Batch 摊薄提交、后台 compaction），单条串行 `ns/op` 会把这些全部抹平，并把
"被缓冲吸收"读成"落盘很快"——jsonl 缓冲档 `ns/op` 约 4.5 µs，那只是"写进内存缓冲"的耗时。
故本报告的吞吐一律指时间窗口内的实际完成量，`ns/op` 仅作辅助。

比较时必须固定三个变量：

| 维度 | 影响 |
|---|---|
| **介质** | tmpfs / ext4(WSL vhdx) / drvfs(9p) 的 fsync 差 3 个数量级 |
| **持久化等级** | 嵌入式基座缺省不逐条 fsync；跨级比较无意义 |
| **key 分布** | 同 key 热点受单点串行限制，多 key 才能吃到组提交 |

**介质必须真实**（涉及 `sync=1` 时尤其如此）：tmpfs 上 fsync 是空操作，会把耐久档的代价整体
抹平——同一基座在 tmpfs 上 `sync=1` 只慢 13%，真实 ext4 上慢 570×。因此 **tmpfs 数据不得用于
"fsync 代价"这类结论**，只用于基座间相对比较。裸 `write(16B)+fsync` 参考值：tmpfs 2–4 µs、
ext4(vhdx) ~2.5 ms、drvfs(9p) 3.8–5.5 ms——本环境的"常规磁盘"是 WSL vhdx，一次 fsync 约
2.5 ms，只比 9p 快约 1.5×，不属于快的那一档。另注意本仓库自身就在 `G:\` 的 drvfs(9p) 挂载上；
测 ext4 请用 WSL 根盘（如 `~/test/tmp`）。

**介质越慢，测试越灵敏。** 9p 上每次 I/O 都要过协议往返，故"多一次 I/O / 多写一页"这类改动会被
放大而不是被掩盖——同一个持久档 `Set` 在 9p 上约 11 ms，ext4 上约 2.3 ms。比较存储层结构
（键布局、页大小、bucket/命名空间组织）时，靠 9p 与写放大计数才能看出差异；tmpfs 甚至 ext4 都可能
把差异淹没在固定的每次提交代价里。

### 1.1 持久化档位

| 基座 | 缺省（高速） | `?sync=1`（耐久） | 备注 |
|---|---|---|---|
| jsonl | flush 500ms + fsync 1s | 逐操作 flush + fsync | 两个旋钮独立：`?each_flush=1` 逐操作 flush，`?flush_interval`/`?sync_interval` 调周期 |
| bolt | 不逐提交 fsync，**周期落盘（缺省 1s）** | 逐提交 fsync | bbolt `NoSync=true` + SDK 自建周期 Sync；`?sync_interval=` |
| leveldb | 不 fsync | 逐提交 fsync | goleveldb `WriteOptions.Sync=false` |
| badger | 不 fsync | 逐提交 fsync | `WithSyncWrites(false)` |
| sqlite | `NORMAL` | `FULL` | `?sync=0` 等价缺省；不提供 `OFF`（会损坏库） |

**五个本地基座取向一致**：都不逐提交 fsync，`?sync=1` 才要断电安全。但**缺省档不等于"永不
落盘"**，各基座的兜底层次不同：

- **jsonl / bolt** 数据先落在**进程内存缓冲**里 → 进程崩溃就会丢。因此都有有界兜底：jsonl 每
  500ms flush、bolt 每 1s 周期落盘，且两者 `Close` 都强制落盘一次（bbolt 自身的 `Close` 不做
  fdatasync，这一层由 SDK 补）。strace 实证：`sync_interval=100ms` 静置 600ms 触发 9 次
  fdatasync，调到 5s 降到 4 次。
- **sqlite（NORMAL）** 每次提交都已 `write()` 进 **OS 页缓存**，WAL 里有全部记录 → **进程崩溃
  不丢**（实测写 200 条后不 Close 直接退出，FULL/NORMAL 均 200/200 恢复）。只影响**机器掉电**。
  其 checkpoint **按帧数**触发（`DEFAULT_WAL_AUTOCHECKPOINT=1000`），**没有时间驱动的
  checkpoint**（实测写 3000 条越过阈值后静置 5s，主库与 WAL 字节数完全不变）——所以它**不需要**
  周期落盘兜底。

`nosync` 不是可用参数：写它直接报错（缺省即不 fsync，接受它反而会让人误判持久化等级）。

## 2. 各基座实测吞吐（主表：ext4 + 缺省档）

**这是选型时唯一需要看的一张表**：真实块设备（ext4/WSL vhdx）、各基座缺省档
（不逐提交 fsync）。其余介质与 `?sync=1` 档位见 §2.3 的矩阵对照。

单位 ops/s；`MGet条目` 与 `批写条目` 为折算到**条目级**的 items/s。均为 8 goroutine、
时间窗 `-benchtime 2s`、统计实际完成量。`Incr多key` 是各写者用独立 key，
`Incr同key` 是所有写者打同一个计数器。

`混合读` / `混合写` 来自 `BenchmarkThroughputMixedReadWrite`（8 goroutine 中一半持续读
同一 64-key 热集、一半写各自互不重叠的 key）；`读保留率` = 混合读 ÷ 该基座纯读 Get。

| 基座 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 | 混合读 | 混合写 | 读保留率 |
|---|---|---|---|---|---|---|---|---|---|---|
| jsonl | ~353.6k | ~10.0M | ~190.1k | ~242.2k | ~639.3k | ~14.1M | ~454.7k | ~474k | ~105k | 5% |
| bolt | ~25.2k | ~591.1k | ~22.9k | ~26.9k | ~20.9k | ~2.6M | ~473.8k | ~170k | ~13.5k | 27% |
| leveldb | ~164.6k | ~1.0M | ~111.8k | ~122.4k | ~111.8k | ~1.0M | ~354.2k | ~98.7k | ~52.4k | 11% |
| badger | ~81.8k | ~274.4k | ~63.5k | ~34.0k | ~64.7k | ~608.2k | ~493.8k | ~76.4k | ~49.7k | 28% |
| sqlite | ~12.6k | ~62.9k | ~5.0k | ~5.8k | ~5.5k | ~450.9k | ~37.6k | ~40.1k | ~5.8k | 64% |

**参照系（不同介质/部署，不与上表直接比较）**

| 基座 | 介质 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 | 混合读 | 混合写 | 读保留率 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| mem | 无 IO | ~775k | ~8.4M | ~825k | ~3.0M | ~2.85M | ~14.8M | ~1.48M | ~783k | ~251k | 9% |
| redis | 容器 ext4 | ~12.7k | ~12.4k | ~14.9k | ~13.4k | ~13.2k | ~256.4k | ~497.2k | ~7.1k | ~7.0k | 57% |
| ssdb | 容器 ext4 | ~8.0k | ~7.6k | ~7.7k | ~7.9k | ~8.2k | ~137.4k | ~20.6k | ~3.7k | ~3.5k | 48% |
| mysql 8.0 | 容器 ext4 | ~718 | ~5.3k | ~428 | ~119 | ~400 | ~95.6k | ~4.6k | ~2.9k | ~453 | 55% |
| pg 16 | 容器 ext4 | ~2.4k | ~9.9k | ~2.2k | ~647 | ~1.4k | ~185.7k | ~8.8k | ~5.3k | ~1.4k | 54% |

### 2.1 从主表读出的结论

- **读路径几乎不受介质支配**——同一基座的 Get/MGet 在 tmpfs/ext4/9p 上基本不变（都命中页缓存）；
  只有写路径受介质支配。
- **"用不用 `sync=1`"比"选哪个基座"更重要**：缺省档把 fsync 移出写热路径后，嵌入式写吞吐比
  `sync=1` 高 1–2 个数量级（§2.3）。选型应先定档位，再挑基座。
- **服务端基座的读吞吐低于嵌入式**（redis Get ~12.4k vs bolt ~590k）——瓶颈是网络往返；但它天然
  "缺省即耐久"，不存在速度/安全的取舍。
- **同 key 热点对 SQL 系伤害最大**（mysql 428→119，pg 2.2k→647）；单写模型的嵌入式基座几乎无差。
- **SSDB 各项均衡但天花板低**（约 8k ops/s，批写仅 2.6×）：无事务，流水线只省往返。
- **jsonl 的嵌入式写吞吐最高**（Set ~353.6k），但它对 leveldb（~164.6k）的领先主要来自 500ms
  flush 缓冲而非引擎差异——需连同读延迟与持久化语义一并权衡。

### 2.2 批写收益（批写条目 ÷ Set，缺省档）

| 基座 | jsonl | bolt | leveldb | badger | sqlite |
|---|---|---|---|---|---|
| 收益 | 2.3× | **18.8×** | 2.2× | 6.0× | 3.0× |

收益取决于**单次写里含多少提交成本**。`sync=1` 时一次提交含一次 fsync，批写能把 N 次 fsync 摊
成 1 次（39–86×，§2.3）；缺省档已无 fsync，只剩事务/Batch 的固定开销（2.2–6.0×）。**bolt 仍达
18.8×**，因为它每次写都开事务，而该开销与介质无关。SQL 系收益较低（mysql 6.4×、pg 3.7×），因为
批内仍是逐条执行、只共享提交；redis 的 39× 来自一次 MULTI/EXEC 往返加服务端合并。

### 2.3 介质与持久化档位矩阵对照

主表是"ext4 + 缺省档"。其余组合**不得与主表直接比较**（换介质或换档位即换了基准）。

**`?sync=1`（逐提交 fsync）@ ext4**——只影响写路径，读与主表一致：

| 基座 | Set | QPush | 批写条目 | 相对缺省档慢 |
|---|---|---|---|---|
| jsonl | ~377 | ~410 | — | ~940× |
| bolt | ~449 | ~464 | ~86× | ~56× |
| leveldb | ~1.3k | ~1.3k | ~71× | ~128× |
| badger | ~748 | ~736 | ~39× | ~109× |
| sqlite | ~440 | — | ~70× | ~29× |

**其余介质（缺省档）**：性能随 fsync 成本变化，跨三个数量级。为保持可读，只列 Set / QPush
（Get/MGet 在各介质上都在百万级，故省略）。

| 基座 | tmpfs Set / QPush | 9p Set / QPush |
|---|---|---|
| jsonl | ~330.7k / ~580.9k | ~1.4k / ~1.6k |
| bolt | ~22.1k / ~20.9k | ~89 / ~94 |
| leveldb | ~137.0k / ~108.5k | ~1.0k / ~1.1k |
| badger | ~67.0k / ~53.1k | ~321 / ~338 |
| sqlite | ~12.9k / ~8.8k | ~234 / ~105 |

**其余档位下的读保留率**（缺省档在主表；保留极端值，它们支撑 §2.4 的结论）：

| 基座 | tmpfs | ext4 `sync=1` | 9p |
|---|---|---|---|
| jsonl | 5% | — | 46% |
| bolt | 29% | 85% | 54% |
| leveldb | 11% | 74% | 60% |
| badger | 27% | **0.4%** | **0.2%** |
| sqlite | 61% | 121% | 193% |

两个要点：**9p 档的批写收益最大**（68–84×，连 flush 都要过协议往返）；**badger 在 `sync=1` 下读
保留率塌到 ~0.4%**（读排在 1.3–3 ms 的提交后面，见 §2.4）。（tmpfs 数据为何不能用于 fsync 结论：
§1。）

### 2.4 读写混合：读者会不会被写者阻塞

来源：主表的 `混合读` / `混合写` / `读保留率` 三列；跨档位对照见 §2.3。

- **服务端基座的读写几乎互不阻塞**（各自保留 ~44–63%）：连接池隔离了请求，读不排在写后面。
- **`mem`/`jsonl` 的读在高写频率下掉到纯读的 5–9%**。jsonl 自己不加锁也只有 5%，说明原因是两者
  共用的 `mem` 全局锁：每次写进入该锁都会让后续读者排队。
- **决定跌幅的是"写者进入共享同步原语的频繁程度"，而非介质带宽**——写频率越低（如 9p 上几百
  ops/s），读者留出的空隙越多，慢盘反而更容易交错。
- **"Get 比 Set 快几十倍"只在低写负载下成立**。混合负载下应看读保留率；改善读阻塞的杠杆与降低
  写延迟相同：批写与分片。
- **badger 的混合读由"提交窗口"支配，而非写频率**：`sync=1` 下一次提交 1.3–3 ms，读者排在它后面，
  保留率跌到 0.2–0.4%（而其写保留率高达 91–98%，即写本身没变慢）。该耦合只属于 `sync=1`
  档：缺省档下读为 ~76.4k、保留率 ~28%，与其他嵌入式基座持平。
- 保留率 >100%（部分 sqlite 档位）是该档自身纯读基线的调度与页缓存抖动。

## 3. 写入成本模型（每操作做了什么）

| 操作 | mem | jsonl | bolt | leveldb | badger | sqlite / pg | mysql | redis | ssdb |
|---|---|---|---|---|---|---|---|---|---|
| Set | map 写 | 1 次 append+flush | 1 个事务（缺省无 fsync） | 1 次 `Write(batch)`（缺省无 fsync） | 1 个 `Update` 事务（缺省无 fsync） | 1 条 upsert（sqlite 缺省 NORMAL；`sync=1` 为 FULL） | 1 条 upsert | 1 命令 | 1 往返 |
| Incr | map 写 | 1 次 append+flush | 1 个事务 | 分片锁内读-改-写 + 1 次 `Write` | 分片锁内读-改-写 + 1 个事务 | 1 条 upsert+`RETURNING` | 事务：3 条语句 | 1 命令 | 1 往返 |
| QPush | map 写 | 1 次 append+flush | 1 个事务 | 1 次 `Write`（元素+计数器同批） | 1 个事务（元素+计数器同批） | 事务：2 条语句 | 事务：4 条语句 | 1 命令 | 1 往返 |
| Batch(N) | N 次写（1 次持锁） | 1 次 write+flush | **1 个事务** | **1 个 Batch** | **1 个 `Update` 事务** | **1 个事务** | **1 个事务** | 1 次 MULTI/EXEC | 1 次流水线 |

瓶颈通常是**每个事务一次持久化（fsync）**，而不是语句条数。加 `?sync=1` 后嵌入式基座
的每次提交都要付这条瓶颈（§2 的 `sync=1` 表：377–1.3k ops/s）；缺省档不逐提交 fsync。
同机隔离实验（MySQL，300 次单语句自增）：

| 配置 | 单语句自增 | 事务 3 语句 |
|---|---|---|
| `innodb_flush_log_at_trx_commit=1`（默认） | 131 op/s | 127 op/s |
| `innodb_flush_log_at_trx_commit=2` | **266 op/s** | 174 op/s |

把 3 条语句压成 1 条约 +3%，放宽持久化等级约 2×；PostgreSQL 同理
（`synchronous_commit=off` 时 490 → 1280 op/s）。**部署侧参数与批写是两个独立杠杆。**

## 4. 选型建议

- **嵌入式 + 点查为主** → `bolt`（mmap/B+tree 读极快，且每次写一个事务，批写收益在任何介质上都成立）。
- **嵌入式 + 写吞吐优先** → `leveldb`（LSM 批量落盘；各介质上单条写都不差）。
- **需要事务 / 批内依赖组合** → `badger`（唯一带 MVCC 事务与批内可见性的嵌入式基座）；代价是同步写
  下读写被提交窗口耦合（§2.4），写吞吐低于 leveldb。
- **需要 SQL 能力 / 多进程共享** → `sqlite` / `mysql` / `pg`（另有 `mssql`）；注意同 key 热点。
- **服务端 KV** → `redis`（批写收益最大）或 `ssdb`（原生协议；无事务、批写收益有限）。
- **轻量进程内持久化** → `mem`（不落盘）或 `jsonl`（追加写 WAL；需理解其持久化档位）。
- **通用原则**：在虚拟化磁盘上，**先把单条写变成批写**，再谈选基座。"每条都持久化且高吞吐"需要
  真实本地 NVMe 或服务端组提交。
- **混合读写场景**（§2.4）：`mem`/`jsonl` 共用全局锁（高写下读只剩 5–9%）；`bolt`/`leveldb`/
  `sqlite` 的 SDK 读路径不加锁，但保留率仍随写频率下降；服务端基座读写互不阻塞。

## 5. 已知取舍

| 事项 | 说明 |
|---|---|
| 单 key 计数器 | SQL 系为行锁串行 + 每提交 fsync，固有成本 |
| SSDB 批写 | 无事务，流水线失败可能部分生效（价值在减少往返） |
| Redis 批内可见性 | 不保证（见 README「批量写」与 `Capabilities().BatchComposed`） |
| Badger 读被提交窗口耦合 | 仅 `?sync=1` 档存在：一次提交 1.3–3 ms，混合读保留率 0.2–0.4%。缺省档（不 fsync）下保留率 ~28%，与其他嵌入式基座持平 |
| 缺省档的持久化代价 | 嵌入式基座缺省不逐条 fsync：进程崩溃/断电可能丢最近的已确认写入。要断电安全必须显式 `?sync=1`（代价见 §2 的 `sync=1` 表：慢 56–570×） |

## 6. 复现

```bash
cd example/bench          # 独立模块；换目录即换介质
D=/dev/shm/bt             # tmpfs；或 ~/test/tmp（ext4/WSL vhdx）、./tmp（drvfs 9p）；mkdir -p "$D"
BE='ThroughputSet$|ThroughputGet$|ThroughputIncrMultiKey$|ThroughputIncrSameKey$|ThroughputQPush$|ThroughputMGet$|ThroughputBatchedSet$|ThroughputMixedReadWrite$'

for u in jsonl bolt leveldb badger sqlite; do
  KVDB_BENCH_URI="$u://$D/t.$u" go test -run '^$' -bench "$BE" -benchtime 2s
  KVDB_BENCH_URI="$u://$D/s.$u?sync=1" go test -run '^$' -bench "$BE" -benchtime 2s   # 仅真实块设备
done

# 服务端基座（需先起容器，见 README「测试」）
KVDB_BENCH_URI='redis://127.0.0.1:6379/0' go test -run '^$' -bench "$BE" -benchtime 2s
KVDB_BENCH_URI='ssdb://127.0.0.1:8888'    go test -run '^$' -bench "$BE" -benchtime 2s
KVDB_BENCH_URI='mysql://root:pw@127.0.0.1:3306/db?parseTime=true' go test -run '^$' -bench "$BE" -benchtime 2s
KVDB_BENCH_URI='pg://postgres:pw@127.0.0.1:5432/db?sslmode=disable' go test -run '^$' -bench "$BE" -benchtime 2s
```

上报指标：`ops/s`、`items/s`（MGet / 批写折算到条目级）、`reads/s` 与 `writes/s`（混合负载下
两个角色各自的完成量）、`µs/op-actual`（由完成量与墙钟算出的平均耗时，不是单条串行延迟）。
顺序调用基准（`BenchmarkSet`/`Get`/`IncrSequential`）仅用于排查单次调用开销与回归对比；吞吐结论
一律以上述窗口数据为准。

Docker 容器数据盘在 ext4(vhdx) 上，因此服务端基座的数字可与"文件基座 @ ext4"一档类比看。
（本环境的介质注意事项见 §1。）
