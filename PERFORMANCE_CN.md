# 性能报告

[English](PERFORMANCE.md) | **中文**

`kvdb` 各基座的实测吞吐、成本模型与选型建议。

> 数字为单机容器环境的相对量级，**不是承诺值**；绝对值受 CPU、存储介质、网络与
> 容器配置影响很大。复现命令见 §7。

## 1. 口径与方法

只使用一种口径：**固定时间窗内跑满并发负载，统计实际完成量**（ops/s；批写折算到
条目级 items/s）。基准为 `bench/throughput_test.go` 的 `BenchmarkThroughput*`，
默认 8 个 goroutine、`-benchtime` 控制窗口长度。

**为什么不以单步长（`ns/op`）为基准**：写入路径普遍带合并与异步成分——SQL 组提交、
SQLite WAL 检查点搭车、LSM memtable 批量落盘、jsonl 缓冲落盘、Batch 摊薄提交，以及
各引擎的后台 compaction。串行测一条再取倒数会把这些全部抹平，并把"被缓冲吸收"误读为
"落盘很快"：例如 jsonl 缓冲档单看 `ns/op` 约 4.5 µs，但那只是"写进内存缓冲"的耗时，
真实持续写盘能力要看窗口内完成了多少。因此本报告只认时间窗口指标，`ns/op` 仅作辅助。

比较时必须固定三个变量：

| 维度 | 影响 |
|---|---|
| **介质** | tmpfs / ext4(WSL vhdx) / drvfs(9p) 的 fsync 差 3 个数量级 |
| **持久化等级** | 嵌入式基座默认不逐条 fsync（`?sync=1` 才开启）；跨级比较无意义 |
| **key 分布** | 同 key 热点受单点串行限制，多 key 才能吃到组提交 |

**介质必须真实（涉及 `sync=1` 时尤其如此）**：tmpfs 上 fsync 是空操作，会把耐久档的
代价整体抹平——实测同一基座在 tmpfs 上 `sync=1` 只慢 13%，在真实 ext4 上慢 570×。
因此 **tmpfs 数据不得用于"fsync 代价/耐久档退避"这类结论**，只用于基座间相对比较。

裸 `write(16B)+fsync` 参考值（解释成因用，非吞吐）：tmpfs 2–4 µs、ext4(vhdx) ~2.5 ms、
drvfs(9p) 3.8–5.5 ms。**本环境的"常规磁盘"是 WSL vhdx，一次 fsync 约 2.5 ms，
只比 9p 快约 1.5×，不属于快的那一档。**

### 1.1 持久化档位（嵌入式基座已统一为 `sync` 开关）

| 基座 | 缺省（高速） | `?sync=1`（耐久） | 备注 |
|---|---|---|---|
| jsonl | flush 每 500ms + fsync 每 1s | 逐操作 flush + fsync | 两个旋钮独立：`?each_flush=1` 逐操作 flush、`?flush_interval`/`?sync_interval` 调周期 |
| bolt | 不逐提交 fsync，**周期落盘（缺省 1s）** | 逐提交 fsync | bbolt `NoSync=true` + SDK 自建周期 Sync；`?sync_interval=1s` 可调 |
| leveldb | 不 fsync | 逐提交 fsync | goleveldb `WriteOptions.Sync=false` |
| badger | 不 fsync | 逐提交 fsync | `WithSyncWrites(false)` |
| sqlite | `NORMAL` | `FULL`（逐提交 fsync） | `?sync=0` 等价缺省（显式写法）；不提供 `OFF`（会损坏库） |

**五个本地基座的缺省档取向一致**：都不逐提交 fsync，`?sync=1` 才要断电安全
（sqlite 同理：`NORMAL` 为缺省，`FULL` 对应 `?sync=1`）。

**缺省档不等于"永不落盘"**，但各基座的兜底层次不同，不要混为一谈：

- **jsonl / bolt**：数据先落在**进程内存缓冲**里 → 进程崩溃就会丢。因此都有有界兜底：
  jsonl 每 500ms flush、bolt 每 1s 周期落盘，且两者 `Close` 都强制落盘一次
  （bbolt 自身的 `Close` 不做 fdatasync，这一层由 SDK 补）。strace 实证：静置 600ms、
  写入 1 次时，`sync_interval=100ms` 触发 9 次 fdatasync，调到 5s 降到 4 次——
  落盘频率确由该参数控制。
- **sqlite（NORMAL）**：每次提交都已 `write()` 进 **OS 页缓存**，WAL 里有全部记录 →
  **进程崩溃不丢**（实测写 200 条后不 Close 直接退出，FULL/NORMAL 均 200/200 恢复）。
  它省掉的只是"提交时 fsync WAL"，故只影响**机器掉电**。SQLite 的 checkpoint
  **按帧数**触发（`DEFAULT_WAL_AUTOCHECKPOINT=1000` 页），**没有时间驱动的 checkpoint**：
  实测写 3000 条（越过阈值，主库 4KB→11.9MB）后静置 5s，主库与 WAL 字节数完全不变。
  所以它**不需要**周期落盘兜底——缺省档的丢失边界就是"掉电丢最近若干提交"。

`nosync` 不是可用参数：写它直接报错（缺省即不 fsync，接受它反而会让人误判持久化等级）。

## 2. 各基座实测吞吐（主表：ext4 + 缺省档）

**这是选型时唯一需要看的一张表**：真实块设备（ext4/WSL vhdx）、各基座缺省档
（不逐提交 fsync）。其余介质与 `?sync=1` 档位见 §2.5 的矩阵对照。

单位 ops/s；`MGet条目` 与 `批写条目` 为折算到**条目级**的 items/s。均为 8 goroutine、
时间窗 `-benchtime 2s`、统计实际完成量。`Incr多key` 是各写者用独立 key，
`Incr同key` 是所有写者打同一个计数器。

`混合读` / `混合写` 来自 `BenchmarkThroughputMixedReadWrite`（8 goroutine 中一半持续读
同一 64-key 热集、一半写各自互不重叠的 key）；`读保留率` = 混合读 ÷ 该基座纯读 Get。

| 基座 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 | 混合读 | 混合写 | 读保留率 |
|---|---|---|---|---|---|---|---|---|---|---
| jsonl | ~353.6k | ~10.0M | ~190.1k | ~242.2k | ~639.3k | ~14.1M | ~454.7k | ~474k | ~105k | 5% |
| bolt | ~25.2k | ~591.1k | ~22.9k | ~26.9k | ~20.9k | ~2.6M | ~473.8k | ~170k | ~13.5k | 27% |
| leveldb | ~164.6k | ~1.0M | ~111.8k | ~122.4k | ~111.8k | ~1.0M | ~354.2k | ~98.7k | ~52.4k | 11% |
| badger | ~81.8k | ~274.4k | ~63.5k | ~34.0k | ~64.7k | ~608.2k | ~493.8k | ~76.4k | ~49.7k | 28% |
| sqlite | ~12.6k | ~62.9k | ~5.0k | ~5.8k | ~5.5k | ~450.9k | ~37.6k | ~40.1k | ~5.8k | 64% |

**参照系（不同介质/部署，不与上表直接比较）**

| 基座 | 介质 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 | 混合读 | 混合写 | 读保留率 |
|---|---|---|---|---|---|---|---|---|---|---|---
| mem | 无 IO | ~775k | ~8.4M | ~825k | ~3.0M | ~2.85M | ~14.8M | ~1.48M | ~783k | ~251k | 9% |
| redis | 容器 ext4 | ~12.7k | ~12.4k | ~14.9k | ~13.4k | ~13.2k | ~256.4k | ~497.2k | ~7.1k | ~7.0k | 57% |
| ssdb | 容器 ext4 | ~8.0k | ~7.6k | ~7.7k | ~7.9k | ~8.2k | ~137.4k | ~20.6k | ~3.7k | ~3.5k | 48% |
| mysql 8.0 | 容器 ext4 | ~718 | ~5.3k | ~428 | ~119 | ~400 | ~95.6k | ~4.6k | ~2.9k | ~453 | 55% |
| pg 16 | 容器 ext4 | ~2.4k | ~9.9k | ~2.2k | ~647 | ~1.4k | ~185.7k | ~8.8k | ~5.3k | ~1.4k | 54% |

### 2.1 从主表读出的结论

- **读路径几乎不受介质支配**：同一基座的 Get/MGet 在 tmpfs/ext4/9p 各档上基本不变
  （如 leveldb Get ~1.0M、jsonl Get ~9.4–10.1M），因为读命中页缓存；写路径才受介质支配。
- **"要不要 `sync=1`"比"选哪个基座"影响更大**：缺省档把 fsync 从写热路径摘掉后，
  嵌入式写吞吐比 `sync=1` 高 1–2 个数量级（§2.5）。选型时应先定持久化档位，再挑基座。
- **服务端基座的读吞吐低于嵌入式**：redis Get ~12.4k vs bolt Get ~590k，瓶颈是网络
  往返而非磁盘；但服务端基座天然"缺省即耐久"，无需在速度与安全间二选一。
- **SQL 的同 key 热点掉档最严重**：mysql 多 key 428 → 同 key 119（3.6×）、
  pg 2.2k → 647（3.4×）。嵌入式基座因单写者模型，同 key 与多 key 差距很小。
- **SSDB 各项吞吐均衡但上限低**（约 8k ops/s 且批写仅 2.6×）：无事务，流水线只省往返。
- **mem 是唯一"读写都不受任何 IO 约束"的基座**（Set ~775k、Get ~8.4M、批写 ~1.5M）。
- **写吞吐最高的嵌入式基座是 jsonl**（Set ~353.6k），但它与 leveldb（~164.6k）的差距
  主要来自"flush 有 500ms 周期缓冲"而非引擎差异，读延迟与持久化语义须一并权衡（§2.4）。

### 2.2 批写收益（批写条目吞吐 ÷ Set 吞吐，主表档位）

| 基座 | ext4 缺省 |
|---|---|
| jsonl | 2.3× |
| bolt | 18.8× |
| leveldb | 2.2× |
| badger | 6.0× |
| sqlite | 3.0× |

收益取决于单条写中可被摊薄的提交成本：

- **批写收益与"单条写里有多少提交成本"成正比**：`sync=1` 下一次提交含一次 fsync，
  批写能把 N 次 fsync 摊成 1 次，故倍数高达 39–86×（§2.5）；缺省档已无 fsync，倍数
  回落到 2.2–6.0×，剩下的可摊薄成本是事务/Batch 本身的固定开销。
- **bolt 在缺省档仍有 18.8×**：它每写必开一个 bbolt 事务，该开销与介质无关，因此在
  tmpfs 上照样能被批掉。
- SQL 系收益偏低（mysql 6.4×、pg 3.7×）是因为批内仍逐条语句执行，只是共享一次提交；
  Redis 39× 来自 MULTI/EXEC 一次往返 + 服务端合并。

### 2.3 读写混合：读者会不会被写者阻塞

主表的 `混合读/混合写/读保留率` 三列即此结论的来源，各档位对照见 §2.5。

- **服务端基座几乎不互相阻塞**：读写各保留 ~44–63%，相当于把并发度对半分。连接池
  隔离了请求，读不会因为写而排队。
- **mem / jsonl 在高写频率下读掉到纯读的 ~5–9%**。jsonl 的读路径不取自身任何锁，
  仍只有 5%，说明瓶颈是两者共用的 mem 全局锁粒度：每次写进入该锁都会让后续读者排队。
- **掉幅由写者进入共享同步原语的频率决定，而不是介质带宽**：写频率越低（如 9p 上
  几百 ops/s），读保留率越高——盘慢反而让读者更容易穿插。
- **"Get 比 Set 快几十倍"只在无写或低写负载下成立**。混合负载下应看读保留率，而改善
  读阻塞的抓手与降低写延迟同源：批写（把 N 次对锁/事务的占用合并成 1 次）与分片。
- **badger 的混合读由"写提交窗口"支配，而不是写频率**：`sync=1` 时一次提交在 ext4/9p
  上要 1.3–3 ms，读者排在提交锁之后，读保留率只剩 0.2–0.4%（它对写的保留率却高达
  91–98%，说明写本身没被拖慢）。缺省档（不 fsync）已消除这一现象：读回到 ~76.4k、
  保留率 ~28%，与其他嵌入式同级。
- **写频率推高后读保留率并不会更差**：bolt/leveldb/badger 缺省档的写频率比 `sync=1`
  高 1–2 个数量级，读保留率仍在 11–28%。说明这些基座的读阻塞主要来自"写者占据同步
  原语的**时长**"，而非次数。
- 读保留率 >100%（sqlite 部分档位）是该档纯读基准自身的调度与页缓存波动，属噪声量级。

### 2.4 jsonl 的两个落盘旋钮

jsonl 的落盘分两个**独立**的旋钮，各自有周期，互不干涉：

| 旋钮 | 缺省 | 干的事 | 决定什么 |
|---|---|---|---|
| **flush** | 每 500ms | 把进程缓冲（bufio）交给 OS | 进程崩溃丢多少（已 flush 的在 OS 页缓存，进程崩溃不丢） |
| **fsync** | 每 1s | 让 OS 把数据写到介质 | 机器掉电丢多少 |

| 档位 | 行为 | 丢失边界 | 写失败可见性 |
|---|---|---|---|
| 缺省 | flush 每 500ms、fsync 每 1s | 进程崩溃丢 ≤500ms；掉电丢 ≤1s | 推迟到周期 tick 或 Close |
| `?each_flush=1` | 逐操作 flush，fsync 仍按周期 | 进程崩溃不丢；掉电丢 ≤1s | 当次操作立即返回 |
| `?sync=1` | 逐操作 flush + fsync | 都不丢 | 当次操作立即返回 |

**两个周期都是"真周期"**（Open 起跑、回调自我重排），不是"空闲才触发"：否则持续
写入会不断推后到期时间，等于永不落盘。`?flush_interval` / `?sync_interval` 可调。

**fsync 隐含 flush**：`Sync` 只作用于 fd 上"已写出去"的字节，数据还在 bufio 缓冲里
时它刷不到——所以周期 fsync 到期会先 Flush 再 Sync，否则那段数据既没交给 OS 也没落盘
（这次 fsync 等于白做，且症状隐蔽：fsync 照常发生，只是同步了个空）。

**失败即停写**：`bufio.Writer` 的错误是粘性的（Go 没有清除 `b.err` 的公开 API），
一旦 Flush/Write 出错，缓冲里残留的字节再也不会被写出去。因此任何落盘失败都会把
Provider 置为不可写，后续写操作一律报错、内存不被改动，`Close` 也必定把该错误报出
——而不是"每次 Set 都报成功、却永远写不出去"。要"写失败当次即报"用 `each_flush=1`。

### 2.5 介质与持久化档位矩阵对照

主表只覆盖"ext4 + 缺省档"。其余组合如下，**不与主表直接比较**（换介质或换档位即换了
比较基准）。

**`?sync=1`（逐提交 fsync）@ ext4**：只有写路径受影响，读与主表相同。

| 基座 | Set | QPush | 批写条目 | 相对缺省档慢 |
|---|---|---|---|---|
| jsonl | ~377 | ~410 | — | ~940× |
| bolt | ~449 | ~464 | ~86× | ~56× |
| leveldb | ~1.3k | ~1.3k | ~71× | ~128× |
| badger | ~748 | ~736 | ~39× | ~109× |
| sqlite | ~440 | — | ~70× | ~29× |

**文件基座 @ tmpfs（内存盘）**

| 基座 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 |
|---|---|---|---|---|---|---|---
| jsonl | ~330.7k | ~10.1M | ~221.9k | ~298.2k | ~580.9k | ~13.4M | ~510.5k |
| bolt | ~22.1k | ~569.8k | ~22.9k | ~26.9k | ~20.9k | ~2.2M | ~479.1k |
| leveldb | ~137.0k | ~1.0M | ~111.8k | ~122.4k | ~108.5k | ~1.0M | ~381.8k |
| badger | ~67.0k | ~274.4k | ~63.5k | ~34.0k | ~53.1k | ~638.4k | ~461.5k |
| sqlite | ~12.9k | ~64.9k | ~5.8k | ~7.4k | ~8.8k | ~460.1k | ~42.6k |

**文件基座 @ drvfs(9p)**

| 基座 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 |
|---|---|---|---|---|---|---|---
| jsonl | ~1.4k | ~9.6M | ~1.5k | ~1.5k | ~1.6k | ~13.3M | ~110.3k |
| bolt | ~89 | ~589.5k | ~91 | ~88 | ~94 | ~2.6M | ~7.1k |
| leveldb | ~1.0k | ~1.1M | ~1.0k | ~288 | ~1.1k | ~1.0M | ~75.2k |
| badger | ~321 | ~297.6k | ~312 | ~286 | ~338 | ~622.8k | ~26.8k |
| sqlite | ~234 | ~9.6k | ~129 | ~151 | ~105 | ~102.0k | ~16.1k |

**混合读写 @ 各档位**（主表已列缺省档，此处补齐其余组合）

| 基座 | 介质/档位 | 混合读 | 混合写 | 读保留率 |
|---|---|---|---|---|
| jsonl | tmpfs | ~485k | ~101k | 5% |
| jsonl | 9p | ~4.45M | ~1.5k | 46% |
| bolt | tmpfs | ~166k | ~12.9k | 29% |
| bolt | ext4 `sync=1` | ~500k | ~491 | 85% |
| bolt | 9p | ~319k | ~119 | 54% |
| leveldb | tmpfs | ~109k | ~58.8k | 11% |
| leveldb | ext4 `sync=1` | ~736k | ~827 | 74% |
| leveldb | 9p | ~658k | ~658 | 60% |
| badger | tmpfs | ~74.6k | ~40.3k | 27% |
| badger | ext4 `sync=1` | ~1.0k | ~689 | 0.4% |
| badger | 9p | ~497 | ~314 | 0.2% |
| sqlite | tmpfs | ~37k | ~6.7k | 61% |
| sqlite | ext4 `sync=1` | ~74k | ~409 | 121% |
| sqlite | 9p | ~19k | ~85 | 193% |

**读矩阵的两条结论**：

- **tmpfs 不适合测 fsync 代价**：tmpfs 上 fsync 是空操作，会把耐久档的代价抹平
  （同一场景 tmpfs 只差 13%，真实 ext4 差 570×）。tmpfs 数据只用于基座间相对比较。
- **9p 档收益普遍更高**（批写 68–84×）：连 flush 都要过协议往返，单条写被往返成本支配。

## 3. 写入成本模型（每操作做了什么）

| 操作 | mem | jsonl | bolt | leveldb | badger | sqlite / pg | mysql | redis | ssdb |
|---|---|---|---|---|---|---|---|---|---|
| Set | map 写 | 1 次 append+flush | 1 个事务（缺省无 fsync） | 1 次 `Write(batch)`（缺省无 fsync） | 1 个 `Update` 事务（缺省无 fsync） | 1 条 upsert（sqlite 缺省 NORMAL；`sync=1` 为 FULL） | 1 条 upsert | 1 命令 | 1 往返 |
| Incr | map 写 | 1 次 append+flush | 1 个事务 | 分片锁内读-改-写 + 1 次 `Write` | 分片锁内读-改-写 + 1 个事务 | 1 条 upsert+`RETURNING` | 事务：3 条语句 | 1 命令 | 1 往返 |
| QPush | map 写 | 1 次 append+flush | 1 个事务 | 1 次 `Write`（元素+计数器同批） | 1 个事务（元素+计数器同批） | 事务：2 条语句 | 事务：4 条语句 | 1 命令 | 1 往返 |
| Batch(N) | N 次写（1 次持锁） | 1 次 write+flush | **1 个事务** | **1 个 Batch** | **1 个 `Update` 事务** | **1 个事务** | **1 个事务** | 1 次 MULTI/EXEC | 1 次流水线 |

瓶颈通常是**每个事务一次持久化（fsync）**，而不是语句条数。加 `?sync=1` 后嵌入式基座
的每次提交都回到这条瓶颈上（§2 的 `sync=1` 表：377–1.3k ops/s）；缺省档把它摘掉了。
同机隔离实验（MySQL，300 次单语句自增）：

| 配置 | 单语句自增 | 事务 3 语句 |
|---|---|---|
| `innodb_flush_log_at_trx_commit=1`（默认） | 131 op/s | 127 op/s |
| `innodb_flush_log_at_trx_commit=2` | **266 op/s** | 174 op/s |

把 3 条语句压成 1 条约 +3%，放宽持久化等级约 2×；PostgreSQL 同理
（`synchronous_commit=off` 时 490 → 1280 op/s）。**部署侧参数与批写是两个独立杠杆。**

## 4. 已落地的实现优化

| 项 | 效果 |
|---|---|
| SQLite / PG 的 `Incr` 用单语句 `upsert + RETURNING`（缺失按 0、原子累加、非整数报错） | 替代事务多语句路径；MySQL 无 `RETURNING` 保留事务路径 |
| MySQL 键列用 `VARBINARY(255)` | 兼容 5.6 默认索引前缀限制 |
| SQLite 进程内写串行化 + WAL | 写排队而非 `SQLITE_BUSY`，读仍可并行 |
| SSDB 连接池 | 单连接是串行请求-应答，池化后并发才真正并行 |
| LevelDB 所有多键写收进单个 `Write(batch)` | 值+TTL、zset 双侧索引、队列元素+计数器各自原子 |
| Badger 多键写收进单个 `db.Update` 事务 | 同上，且批内 read-your-writes（`BatchComposed=true`） |
| Badger 同键读-改-写用分片锁先行串行化 | 避免 SSI 冲突重试风暴；冲突重试仅作兜底 |

未采用的方案：CGO 版 SQLite 驱动——实测真实存储上纯 Go 与 CGO 吞吐基本一致
（瓶颈是每事务持久化，不是驱动实现），故不引入 CGO 依赖。

## 5. 选型建议

- **嵌入式 + 点查为主**：`bolt`（mmap/B+tree 读极快，且每写一事务，批写收益在
  任何介质都成立）。
- **嵌入式 + 写吞吐优先**：`leveldb`（LSM 批量落盘，tmpfs/9p 上单条写都不错）。
- **需要事务 / 批内依赖组合**：`badger`（唯一提供 MVCC 事务与批内可见性的嵌入式基座）；
  代价是默认同步写下读写被提交窗口耦合（§2.3），且写吞吐低于 leveldb。
- **需要 SQL 能力 / 多进程共享**：`sqlite` / `mysql` / `pg`；注意同 key 热点掉档。
- **服务端 KV**：`redis`（批写收益最大）、`ssdb`（原生协议，但无事务、批写收益有限）。
- **进程内轻量持久化**：`mem`（不落盘）、`jsonl`（append-only WAL，需理解其持久化等级）。
- **通用原则**：在 WSL/虚拟化盘上，**先把写改成批写**，再谈挑基座；要"每条落盘且
  高吞吐"需真实本地 NVMe 或依赖服务端的组提交策略。
- **读写混合场景**（§2.3）：`mem`/`jsonl` 因共用全局锁，高写频率下读只剩纯读的 5–9%；
  `bolt`/`leveldb`/`sqlite` 的 SDK 层读路径不取锁，但读保留率仍随写频率下滑
  （tmpfs 为 11–61%，9p 为 54–193%）；服务端基座读写互不阻塞（各保留 44–63%）。

## 6. 已知取舍

| 事项 | 说明 |
|---|---|
| 单 key 计数器 | SQL 系为行锁串行 + 每提交 fsync，固有成本 |
| SSDB 批写 | 无事务，流水线失败可能部分生效（价值在减少往返） |
| Redis 批内可见性 | 不保证（见 README「批量写」与 `Capabilities().BatchComposed`） |
| Badger 读被提交窗口耦合 | `?sync=1` 时一次提交 1.3–3 ms，混合读保留率仅 0.2–0.4%；**缺省档（不 fsync）已消除该现象**（读保留率回到 ~28%） |
| 缺省档的持久化代价 | 嵌入式基座缺省不逐条 fsync：进程崩溃/断电可能丢最近的已确认写入。要断电安全必须显式 `?sync=1`（代价见 §2 的 `sync=1` 表：慢 56–570×） |
| 介质标注 | 仓库内 `kvdb/tmp` 属 `G:\` 的 drvfs(9p)，**不是**常规磁盘；测 ext4 须放 WSL 根盘（如 `~/test/tmp`） |
| tmpfs 数据不可用于 fsync 结论 | tmpfs 上 fsync 是空操作，`sync=1` 的代价会被完全抹平（实测仅慢 13%，真实 ext4 上慢 570×） |

## 7. 复现

```bash
cd bench   # 独立模块；换目录即换介质

D=/dev/shm/bt            # tmpfs；或 ~/test/tmp（ext4/WSL vhdx）、./tmp（drvfs 9p）
mkdir -p "$D"
BE='ThroughputSet$|ThroughputGet$|ThroughputIncrMultiKey$|ThroughputIncrSameKey$|\
ThroughputQPush$|ThroughputMGet$|ThroughputBatchedSet$|ThroughputMixedReadWrite$'

for u in jsonl bolt leveldb badger sqlite; do
  case $u in jsonl) p="$D/t.jsonl";; bolt) p="$D/t.bolt";;
                 leveldb) p="$D/t.ldb";; badger) p="$D/t.badger";;
                 sqlite) p="$D/t.db";; esac
  KVDB_BENCH_URI="$u://$p" go test -run '^$' -bench "$BE" -benchtime 2s
done

# 耐久档对照：只在真实块设备上做（tmpfs 的 fsync 是空操作，测不出代价）
for u in jsonl bolt leveldb badger; do
  case $u in jsonl) p="$D/s.jsonl";; bolt) p="$D/s.bolt";;
                 leveldb) p="$D/s.ldb";; badger) p="$D/s.badger";; esac
  KVDB_BENCH_URI="$u://$p?sync=1" go test -run '^$' -bench "$BE" -benchtime 2s
done

# 服务端基座（需先起容器，见 README「测试」）
KVDB_BENCH_URI='redis://127.0.0.1:6379/0' go test -run '^$' -bench "$BE" -benchtime 2s
KVDB_BENCH_URI='ssdb://127.0.0.1:8888'    go test -run '^$' -bench "$BE" -benchtime 2s
KVDB_BENCH_URI='mysql://root:pw@127.0.0.1:3306/db?parseTime=true' \
  go test -run '^$' -bench "$BE" -benchtime 2s
KVDB_BENCH_URI='pg://postgres:pw@127.0.0.1:5432/db?sslmode=disable' \
  go test -run '^$' -bench "$BE" -benchtime 2s
```

上报指标：`ops/s`（真实吞吐）、`items/s`（MGet / 批写折算到条目级）、
`reads/s` 与 `writes/s`（混合负载下两个角色各自的完成量）、
`µs/op-actual`（由完成量与墙钟算出的平均耗时，不是单条串行延迟）。

另有 `BenchmarkSet`/`Get`/`IncrSequential` 等**顺序调用**基准，仅用于排查单次调用的
固有开销与回归对比；吞吐结论一律以本节实测数据为准。

注意：仓库内 `kvdb/tmp` 属 `G:\` 的 drvfs(9p) 挂载，不是常规磁盘；测 ext4 须放 WSL
根盘（如 `~/test/tmp`，即 `/dev/sdd`）。Docker 容器数据盘在 ext4(vhdx) 上，因此
服务端基座的数字可与"文件基座 @ ext4"一档类比看。
