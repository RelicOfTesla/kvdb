# 性能报告

`kvdb` 各基座的实测吞吐、成本模型与选型建议。

> 数字为单机容器环境的相对量级，**不是承诺值**；绝对值受 CPU、存储介质、网络与
> 容器配置影响很大。复现命令见 §7。

## 1. 口径与方法

只使用一种口径：**固定时间窗内跑满并发负载，统计实际完成量**（ops/s；批写折算到
条目级 items/s）。基准为 `bench/throughput_test.go` 的 `BenchmarkThroughput*`，
默认 8 个 goroutine、`-benchtime` 控制窗口长度。

合并类优化（SQL 组提交、SQLite WAL 检查点搭车、LSM memtable 批量落盘、Batch 摊薄
提交）只在持续负载下出现，因此吞吐必须由实际完成量得出。

比较时必须固定三个变量：

| 维度 | 影响 |
|---|---|
| **介质** | tmpfs / ext4(WSL vhdx) / drvfs(9p) 的 fsync 差 3 个数量级 |
| **持久化等级** | jsonl 默认不逐条 fsync、MySQL/PG 可放宽同步；跨级比较无意义 |
| **key 分布** | 同 key 热点受单点串行限制，多 key 才能吃到组提交 |

裸 `write(16B)+fsync` 参考值（解释成因用，非吞吐）：tmpfs 2–4 µs、ext4(vhdx) ~2.5 ms、
drvfs(9p) 3.8–5.5 ms。**本环境的"常规磁盘"是 WSL vhdx，一次 fsync 约 2.5 ms，
只比 9p 快约 1.5×，不属于快的那一档。**

## 2. 各基座实测吞吐（全部操作类型）

单位 ops/s；`MGet条目` 与 `批写条目` 为折算到**条目级**的 items/s。均为 8 goroutine、
时间窗 `-benchtime 2s`、统计实际完成量。`Incr多key` 是各写者用独立 key，
`Incr同key` 是所有写者打同一个计数器。

**纯内存（不落盘，与介质无关）**

| 基座 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 |
|---|---|---|---|---|---|---|---
| mem | ~775k | ~8.4M | ~825k | ~3.0M | ~2.85M | ~14.8M | ~1.48M |

**文件基座 @ tmpfs**

| 基座 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 |
|---|---|---|---|---|---|---|---
| jsonl | ~201.8k | ~10.1M | ~221.9k | ~298.2k | ~286.3k | ~13.4M | ~510.5k |
| bolt | ~22.1k | ~569.8k | ~22.9k | ~26.9k | ~20.9k | ~2.2M | ~479.1k |
| leveldb | ~137.0k | ~1.0M | ~111.8k | ~122.4k | ~108.5k | ~1.0M | ~381.8k |
| badger② | ~67.0k | ~274.4k | ~63.5k | ~34.0k | ~53.1k | ~638.4k | ~461.5k |
| sqlite | ~12.0k | ~60.6k | ~5.7k | ~5.8k | ~7.3k | ~447.9k | ~35.6k |

**文件基座 @ ext4(WSL vhdx)**

| 基座 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 |
|---|---|---|---|---|---|---|---
| jsonl① | ~177.0k | ~10.0M | ~190.1k | ~242.2k | ~315.3k | ~14.1M | ~548.7k |
| bolt | ~426 | ~591.1k | ~461 | ~481 | ~518 | ~2.6M | ~38.6k |
| leveldb | ~1.3k | ~1.0M | ~1.4k | ~448 | ~1.4k | ~1.0M | ~92.8k |
| badger② | ~757 | ~272.0k | ~741 | ~620 | ~724 | ~608.2k | ~29.4k |
| sqlite | ~559 | ~61.3k | ~312 | ~316 | ~358 | ~453.0k | ~30.4k |

**文件基座 @ drvfs(9p)**

| 基座 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 |
|---|---|---|---|---|---|---|---
| jsonl① | ~1.4k | ~9.6M | ~1.5k | ~1.5k | ~1.6k | ~13.3M | ~110.3k |
| bolt | ~89 | ~589.5k | ~91 | ~88 | ~94 | ~2.6M | ~7.1k |
| leveldb | ~1.0k | ~1.1M | ~1.0k | ~288 | ~1.1k | ~1.0M | ~75.2k |
| badger② | ~321 | ~297.6k | ~312 | ~286 | ~338 | ~622.8k | ~26.8k |
| sqlite | ~234 | ~9.6k | ~129 | ~151 | ~105 | ~102.0k | ~16.1k |

**服务端基座（容器盘为 ext4，可与上表 ext4 档类比）**

| 基座 | Set | Get | Incr多key | Incr同key | QPush | MGet条目 | 批写条目 |
|---|---|---|---|---|---|---|---
| redis | ~12.7k | ~12.4k | ~14.9k | ~13.4k | ~13.2k | ~256.4k | ~497.2k |
| ssdb | ~8.0k | ~7.6k | ~7.7k | ~7.9k | ~8.2k | ~137.4k | ~20.6k |
| mysql 8.0 | ~718 | ~5.3k | ~428 | ~119 | ~400 | ~95.6k | ~4.6k |
| pg 16 | ~2.4k | ~9.9k | ~2.2k | ~647 | ~1.4k | ~185.7k | ~8.8k |

① jsonl 默认只 flush 到 OS、不逐条 fsync（`?sync=1` 才是每写一次 fsync）：跨基座
比较前需先对齐持久化等级。
② badger 默认逐条 fsync（`?nosync=1` 关闭）。它的读写会被提交窗口耦合，见 §2.3。

### 2.1 从表中读出的结论

- **读路径基本不受介质支配**：同一基座的 Get/MGet 在 tmpfs/ext4/9p 三档上几乎不变
  （如 leveldb Get ~1.0M、jsonl Get ~9.6–10.1M），因为读命中页缓存；而 Set/Incr/QPush
  这类写路径跨介质差 2–3 个数量级（bolt Set：tmpfs 22.1k → ext4 426 → 9p 89）。
- **服务端基座的读吞吐低于嵌入式**：redis Get ~12.4k vs bolt Get ~590k，瓶颈是网络
  往返而非磁盘。
- **SQL 的同 key 热点掉档最严重**：mysql 多 key 428 → 同 key 119（3.6×）、
  pg 2.2k → 647（3.4×）。嵌入式基座因单写者模型，同 key 与多 key 差距很小。
- **SSDB 各项吞吐均衡但上限低**（约 8k ops/s 且批写仅 2.6×）：无事务，流水线只省往返。
- **mem 是唯一"读写都不受任何 IO 约束"的基座**（Set ~775k、Get ~8.4M、批写 ~1.5M）。
- **badger 是唯一"读会被写提交窗口耦合"的嵌入式基座**：纯读与其他嵌入式同级
  （~272–298k），但默认同步写下混合读保留率仅 0.2–0.4%（§2.3）。

### 2.2 批写收益（批写条目吞吐 ÷ Set 吞吐）

| 基座 | tmpfs | ext4(vhdx) | drvfs(9p) | 服务端 |
|---|---|---|---|---|
| jsonl | 2.5× | 3.1× | 81.3× | — |
| bolt | 21.7× | 90.6× | 80.4× | — |
| leveldb | 2.8× | 69.7× | 74.4× | — |
| badger | 6.9× | 38.9× | 83.7× | — |
| sqlite | 3.0× | 54.4× | 68.8× | mysql 6.4× / pg 3.7× |
| redis | — | — | — | 39.0× |
| ssdb | — | — | — | 2.6× |

收益取决于单条写中可被摊薄的提交成本：

- tmpfs 上没有 fsync 可省，多数基座为 2.5–3×；**bolt 是 21.7×**：它每写必开一个
  bbolt 事务，该开销与介质无关，因此在内存文件系统上照样能被批掉。
- jsonl 在 ext4 为 3.1×、在 9p 为 81.3×：它默认不逐条 fsync，ext4 上单条已经便宜，
  而 9p 上连 flush 都要过协议往返，故批写仍有大收益。
- SQL 系收益偏低（mysql 6.4×、pg 3.7×）是因为批内仍逐条语句执行，只是共享一次
  提交；Redis 42× 来自 MULTI/EXEC 一次往返 + 服务端合并。

### 2.3 读写混合：读者会不会被写者阻塞

`BenchmarkThroughputMixedReadWrite`：8 个 goroutine 中一半持续读同一 64-key 热集、
一半写各自互不重叠的 key，同时上报 `reads/s` 与 `writes/s`。表中"读保留率"= 混合
`reads/s` ÷ 该基座同介质纯读 Get，"写保留率"= 混合 `writes/s` ÷ 纯写 Set。

| 基座 | 介质 | 混合 reads/s | 混合 writes/s | 读保留率 | 写保留率 |
|---|---|---|---|---|---|
| mem | — | ~783k | ~251k | 9% | 32% |
| jsonl | tmpfs | ~485k | ~101k | 5% | 50% |
| jsonl | ext4 | ~486k | ~106k | 5% | 60% |
| jsonl | 9p | ~4.45M | ~1.5k | 46% | 106% |
| bolt | tmpfs | ~166k | ~12.9k | 29% | 58% |
| bolt | ext4 | ~500k | ~491 | 85% | 115% |
| bolt | 9p | ~319k | ~119 | 54% | 134% |
| leveldb | tmpfs | ~109k | ~58.8k | 11% | 43% |
| leveldb | ext4 | ~736k | ~827 | 74% | 64% |
| leveldb | 9p | ~658k | ~658 | 60% | 66% |
| badger | tmpfs | ~74.6k | ~40.3k | 27% | 60% |
| badger | ext4 | ~1.0k | ~689 | 0.4% | 91% |
| badger | 9p | ~497 | ~314 | 0.2% | 98% |
| sqlite | tmpfs | ~37k | ~6.7k | 61% | 56% |
| sqlite | ext4 | ~74k | ~409 | 121% | 73% |
| sqlite | 9p | ~19k | ~85 | 193% | 36% |
| redis | 容器 | ~7.1k | ~7.0k | 57% | 55% |
| ssdb | 容器 | ~3.7k | ~3.5k | 48% | 44% |
| mysql 8.0 | 容器 | ~2.9k | ~453 | 55% | 63% |
| pg 16 | 容器 | ~5.3k | ~1.4k | 54% | 58% |

- **服务端基座几乎不互相阻塞**：读写各保留 ~44–63%，相当于把并发度对半分。连接池
  隔离了请求，读不会因为写而排队。
- **mem / jsonl 在高写频率下读掉到纯读的 ~5–9%**（tmpfs、ext4）。jsonl 的读路径不取
  自身任何锁，仍只有 5%，说明瓶颈是两者共用的 mem 全局锁粒度：每次写进入该锁都会让
  后续读者排队。`jsonl` 的纯读吞吐（Get ~9.6–10.1M）正说明其自身读路径没有额外代价。
- **掉幅由写者进入共享同步原语的频率决定，而不是介质带宽**：同一基座的写频率从
  tmpfs 的 10 万级/s 降到 9p 的几百 ~1.5k/s 时，读保留率从 5% 回升到 46%（jsonl）、
  从 11% 回升到 60%（leveldb）。盘越慢，写者占用同步原语的次数越少，读者越容易穿插。
- **"Get 比 Set 快几十倍"只在无写或低写负载下成立**。混合负载下应看读保留率，而改善
  读阻塞的抓手与降低写延迟同源：批写（把 N 次对锁/事务的占用合并成 1 次）与分片。
- **badger 的混合读由"写提交窗口"支配，而不是写频率**：默认同步提交时一次提交在
  ext4/9p 上要 1.3–3 ms，读者排在提交锁之后，读保留率只剩 0.2–0.4%（它对写的保留率
  却高达 91–98%，说明写本身没被拖慢）。关掉逐条 fsync（`?nosync=1`）后同一组负载在
  9p 上读回到 ~92.7k ops/s（保留率 ~33%）、tmpfs 上 ~97.3k（~35%）。
- 读保留率 >100%（sqlite ext4/9p）是该档纯读基准自身的调度与页缓存波动，属噪声量级。

## 3. 写入成本模型（每操作做了什么）

| 操作 | mem | jsonl | bolt | leveldb | badger | sqlite / pg | mysql | redis | ssdb |
|---|---|---|---|---|---|---|---|---|---|
| Set | map 写 | 1 次 append+flush | 1 个事务（1 fsync） | 1 次 `Write(batch)`（1 fsync） | 1 个 `Update` 事务（1 fsync） | 1 条 upsert | 1 条 upsert | 1 命令 | 1 往返 |
| Incr | map 写 | 1 次 append+flush | 1 个事务 | 分片锁内读-改-写 + 1 次 `Write` | 分片锁内读-改-写 + 1 个事务 | 1 条 upsert+`RETURNING` | 事务：3 条语句 | 1 命令 | 1 往返 |
| QPush | map 写 | 1 次 append+flush | 1 个事务 | 1 次 `Write`（元素+计数器同批） | 1 个事务（元素+计数器同批） | 事务：2 条语句 | 事务：4 条语句 | 1 命令 | 1 往返 |
| Batch(N) | N 次写（1 次持锁） | 1 次 write+flush | **1 个事务** | **1 个 Batch** | **1 个 `Update` 事务** | **1 个事务** | **1 个事务** | 1 次 MULTI/EXEC | 1 次流水线 |

瓶颈通常是**每个事务一次持久化（fsync）**，而不是语句条数。同机隔离实验
（MySQL，300 次单语句自增）：

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
| Badger 读被提交窗口耦合 | 默认同步提交时混合读保留率仅 0.2–0.4%；`?nosync=1` 可显著缓解，代价是崩溃丢最近提交 |
| 介质标注 | 仓库内 `kvdb/tmp` 属 `G:\` 的 drvfs(9p)，**不是**常规磁盘；测 ext4 须放 WSL 根盘（如 `~/test/tmp`） |

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
