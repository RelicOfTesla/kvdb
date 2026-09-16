# Performance report

**English** | [中文](PERFORMANCE_CN.md)

Measured throughput, cost model, and selection guidance for each `kvdb` backend.

> The numbers are relative magnitudes from a single-machine container environment and are
> **not promises**; absolute values depend heavily on CPU, storage medium, network, and
> container configuration. Reproduction commands are in §6.

## 1. Scope and method

One methodology: **saturate the backend with concurrent load over a fixed time window and count
what actually completes** (ops/s; batch writes converted to per-item items/s). Basis:
`BenchmarkThroughput*` in `example/bench/throughput_test.go`, 8 goroutines, `-benchtime` setting
the window.

**Why not `ns/op`**: the write path coalesces and defers work (SQL group commit, SQLite WAL
checkpoint piggybacking, LSM memtable flushing, jsonl buffer flushing, Batch-amortized commits,
background compaction). Measuring one call serially and taking its reciprocal flattens all of
that and misreads "absorbed by a buffer" as "fast to disk" — jsonl's buffered mode shows ~4.5 µs
per `ns/op`, but that is only the cost of writing into a memory buffer. Only time-window metrics
are used here; `ns/op` is auxiliary.

Three variables must be held fixed:

| Dimension | Impact |
|---|---|
| **Medium** | fsync on tmpfs / ext4 (WSL vhdx) / drvfs (9p) differs by 3 orders of magnitude |
| **Durability level** | embedded backends do not fsync per op by default; cross-level comparisons are meaningless |
| **key distribution** | same-key hot spots serialize; only multiple keys benefit from group commit |

**The medium must be real** (especially for `sync=1`): fsync on tmpfs is a no-op and flattens the
durable mode's cost entirely — the same backend measures 13% slower on tmpfs but 570× slower on
real ext4. So **tmpfs data must not be used for fsync-cost conclusions**, only for relative
comparisons between backends. Bare `write(16B)+fsync` for reference: tmpfs 2–4 µs, ext4 (vhdx)
~2.5 ms, drvfs (9p) 3.8–5.5 ms — this environment's "ordinary disk" is a WSL vhdx at ~2.5 ms per
fsync, only ~1.5× faster than 9p, so it is not in the fast tier. Note also that this repository
itself lives on a drvfs (9p) mount of `G:\`; measure ext4 on a WSL root disk such as `~/test/tmp`.

### 1.1 Durability modes (embedded backends share one `sync` switch)

| Backend | Default (fast) | `?sync=1` (durable) | Notes |
|---|---|---|---|
| jsonl | flush 500ms + fsync 1s | flush + fsync per op | Independent knobs: `?each_flush=1` flushes per op, `?flush_interval`/`?sync_interval` tune the periods |
| bolt | no fsync per commit, **periodic flush (1s)** | fsync per commit | bbolt `NoSync=true` plus an SDK periodic Sync; `?sync_interval=` |
| leveldb | no fsync | fsync per commit | goleveldb `WriteOptions.Sync=false` |
| badger | no fsync | fsync per commit | `WithSyncWrites(false)` |
| sqlite | `NORMAL` | `FULL` | `?sync=0` is the explicit default; `OFF` is not offered (it corrupts the DB) |

**All five local backends take the same stance**: no fsync per commit by default, `?sync=1` for
power-loss safety. But the **default mode is not "never persisted"**, and the fallback layers
differ:

- **jsonl / bolt** buffer in **process memory**, so a process crash loses data. Both add a bounded
  fallback — jsonl flushes every 500ms, bolt flushes on a 1s period — and `Close` forces one flush
  on both (bbolt's own `Close` does not fdatasync, so the SDK supplies that). Verified by strace:
  with `sync_interval=100ms` a 600ms idle period shows 9 fdatasyncs, versus 4 at 5s.
- **sqlite (NORMAL)** writes into the **OS page cache** on every commit and the WAL holds every
  record, so **a process crash loses nothing** (measured: 200/200 recovered for both FULL and
  NORMAL after exiting without Close). Only machine **power loss** can lose recent commits.
  Its checkpoint triggers on **frame count** (`DEFAULT_WAL_AUTOCHECKPOINT=1000`), with **no
  time-driven checkpoint** (3000 records crossing the threshold, then 5s idle: main DB and WAL
  byte counts unchanged) — so it needs **no** periodic-flush fallback.

`nosync` is not accepted: it errors immediately, since the default already means no fsync and
accepting it would misrepresent the durability level.

## 2. Measured throughput (main table: ext4 + default mode)

**This is the only table you need to look at when choosing a backend**: a real block device
(ext4/WSL vhdx) with each backend in its default mode (no fsync per commit). Other media and
the `?sync=1` modes are covered by the matrix in §2.3.

Units are ops/s; the `MGet items` and `Batch items` columns are items/s converted to the
**per-item** level. All use 8 goroutines, a `-benchtime 2s` time window, and count what
actually completes. `Incr multi-key` has each writer using an independent key, while
`Incr same-key` has all writers hitting the same counter.

`Mixed read` / `Mixed write` come from `BenchmarkThroughputMixedReadWrite` (of 8 goroutines,
half continuously read the same 64-key hot set and half write their own non-overlapping
keys); `Read retention` = mixed read ÷ that backend's pure-read Get.

| Backend | Set | Get | Incr multi-key | Incr same-key | QPush | MGet items | Batch items | Mixed read | Mixed write | Read retention |
|---|---|---|---|---|---|---|---|---|---|---|
| jsonl | ~353.6k | ~10.0M | ~190.1k | ~242.2k | ~639.3k | ~14.1M | ~454.7k | ~474k | ~105k | 5% |
| bolt | ~25.2k | ~591.1k | ~22.9k | ~26.9k | ~20.9k | ~2.6M | ~473.8k | ~170k | ~13.5k | 27% |
| leveldb | ~164.6k | ~1.0M | ~111.8k | ~122.4k | ~111.8k | ~1.0M | ~354.2k | ~98.7k | ~52.4k | 11% |
| badger | ~81.8k | ~274.4k | ~63.5k | ~34.0k | ~64.7k | ~608.2k | ~493.8k | ~76.4k | ~49.7k | 28% |
| sqlite | ~12.6k | ~62.9k | ~5.0k | ~5.8k | ~5.5k | ~450.9k | ~37.6k | ~40.1k | ~5.8k | 64% |

**Reference set (different medium/deployment; not directly comparable to the table above)**

| Backend | Medium | Set | Get | Incr multi-key | Incr same-key | QPush | MGet items | Batch items | Mixed read | Mixed write | Read retention |
|---|---|---|---|---|---|---|---|---|---|---|---|
| mem | no IO | ~775k | ~8.4M | ~825k | ~3.0M | ~2.85M | ~14.8M | ~1.48M | ~783k | ~251k | 9% |
| redis | container ext4 | ~12.7k | ~12.4k | ~14.9k | ~13.4k | ~13.2k | ~256.4k | ~497.2k | ~7.1k | ~7.0k | 57% |
| ssdb | container ext4 | ~8.0k | ~7.6k | ~7.7k | ~7.9k | ~8.2k | ~137.4k | ~20.6k | ~3.7k | ~3.5k | 48% |
| mysql 8.0 | container ext4 | ~718 | ~5.3k | ~428 | ~119 | ~400 | ~95.6k | ~4.6k | ~2.9k | ~453 | 55% |
| pg 16 | container ext4 | ~2.4k | ~9.9k | ~2.2k | ~647 | ~1.4k | ~185.7k | ~8.8k | ~5.3k | ~1.4k | 54% |

### 2.1 Conclusions from the main table

- **Reads barely depend on the medium** — for a given backend, Get/MGet is essentially unchanged
  across tmpfs/ext4/9p (they hit the page cache); only the write path is medium-bound.
- **The durability mode matters more than the backend choice**: with fsync off the hot path,
  embedded write throughput is 1–2 orders of magnitude above `sync=1` (§2.3). Decide the mode
  first, then the backend.
- **Server backends read slower than embedded ones** (redis Get ~12.4k vs bolt ~590k) — the
  bottleneck is the network round trip; but they are durable by default, so there is no
  speed/safety trade to make.
- **Same-key hot spots hurt SQL most** (mysql 428→119, pg 2.2k→647); the single-writer embedded
  backends show almost no gap.
- **SSDB is balanced but capped** (~8k ops/s, batch writes only 2.6×): no transactions, and
  pipelining only saves round trips.
- **jsonl has the highest embedded write throughput** (Set ~353.6k), but its lead over leveldb
  (~164.6k) comes mostly from its 500ms flush buffer, not the engine — weigh read latency and
  durability semantics alongside.

### 2.2 Batch-write benefit (batch items ÷ Set, default mode)

| Backend | jsonl | bolt | leveldb | badger | sqlite |
|---|---|---|---|---|---|
| Benefit | 2.3× | **18.8×** | 2.2× | 6.0× | 3.0× |

The benefit tracks **how much commit cost a single write carries**. With `sync=1` one commit is
one fsync, so batching collapses N fsyncs into 1 (39–86×, §2.3); in the default mode no fsync
remains, leaving only the transaction/Batch fixed overhead (2.2–6.0×). **bolt still reaches
18.8×** because it opens a transaction per write, and that cost is medium-independent. The SQL
family gains less (mysql 6.4×, pg 3.7×) as statements are still executed one by one and only the
commit is shared; redis's 39× comes from one MULTI/EXEC round trip plus server-side merging.

### 2.3 Medium × durability-mode matrix

The main table is "ext4 + default mode". The other combinations **must not be compared directly
to it** (changing the medium or the mode changes the baseline).

**`?sync=1` (fsync per commit) @ ext4** — only the write path changes; reads match the main table:

| Backend | Set | QPush | Batch items | Slower than default |
|---|---|---|---|---|
| jsonl | ~377 | ~410 | — | ~940× |
| bolt | ~449 | ~464 | ~86× | ~56× |
| leveldb | ~1.3k | ~1.3k | ~71× | ~128× |
| badger | ~748 | ~736 | ~39× | ~109× |
| sqlite | ~440 | — | ~70× | ~29× |

**Other media (default mode)**: performance tracks fsync cost, which spans three orders of
magnitude. Summarised as Set / QPush (ops/s) to keep the shape legible; `Get`/`MGet` stay in the
millions everywhere and are therefore omitted.

| Backend | tmpfs Set / QPush | 9p Set / QPush |
|---|---|---|
| jsonl | ~330.7k / ~580.9k | ~1.4k / ~1.6k |
| bolt | ~22.1k / ~20.9k | ~89 / ~94 |
| leveldb | ~137.0k / ~108.5k | ~1.0k / ~1.1k |
| badger | ~67.0k / ~53.1k | ~321 / ~338 |
| sqlite | ~12.9k / ~8.8k | ~234 / ~105 |

**Read retention under other modes** (default mode is in the main table; equal-score/extreme cases
kept because they drive the §2.4 conclusion):

| Backend | tmpfs | ext4 `sync=1` | 9p |
|---|---|---|---|
| jsonl | 5% | — | 46% |
| bolt | 29% | 85% | 54% |
| leveldb | 11% | 74% | 60% |
| badger | 27% | **0.4%** | **0.2%** |
| sqlite | 61% | 121% | 193% |

Two things this matrix shows: **the 9p mode has the largest batch-write benefit** (68–84×, since
even a flush pays a protocol round trip), and **`sync=1` on badger collapses read retention to
~0.4%** because reads queue behind a 1.3–3 ms commit (§2.4). (Why tmpfs numbers must not be used
for fsync conclusions: §1.)

### 2.4 Mixed read/write: are readers blocked by writers?

Source: the `Mixed read` / `Mixed write` / `Read retention` columns of the main table; the
cross-mode view is in §2.3.

- **Server backends barely block each other** (each direction retains ~44–63%): the connection
  pool isolates requests, so reads do not queue behind writes.
- **`mem`/`jsonl` reads fall to 5–9% of pure-read** under high write frequency. jsonl takes no
  lock of its own yet still hits 5%, so the cause is the shared global lock in `mem`: every write
  entering it queues subsequent readers.
- **What determines the drop is how often writers take the shared primitive, not medium
  bandwidth** — lower write frequency (e.g. a few hundred ops/s on 9p) leaves readers more room,
  so a slower disk actually interleaves better.
- **"Get is dozens of times faster than Set" holds only at low write load.** Under mixed load look
  at read retention, and the levers are the same ones that cut write latency: batching and sharding.
- **badger's mixed read is governed by the commit window, not write frequency**: with `sync=1` a
  commit costs 1.3–3 ms and readers queue behind it, dropping retention to 0.2–0.4% (while write
  retention stays 91–98%, i.e. writes themselves are not slowed). The default mode removes this
  entirely (reads ~76.4k, retention ~28%, on par with the other embedded backends).
- Retention >100% (some sqlite modes) is scheduling and page-cache jitter in that mode's own
  pure-read baseline.

## 3. Write cost model (what each operation does)

| Operation | mem | jsonl | bolt | leveldb | badger | sqlite / pg | mysql | redis | ssdb |
|---|---|---|---|---|---|---|---|---|---|
| Set | map write | 1 append+flush | 1 transaction (no fsync by default) | 1 `Write(batch)` (no fsync by default) | 1 `Update` transaction (no fsync by default) | 1 upsert (sqlite default NORMAL; `sync=1` is FULL) | 1 upsert | 1 command | 1 round trip |
| Incr | map write | 1 append+flush | 1 transaction | read-modify-write under a shard lock + 1 `Write` | read-modify-write under a shard lock + 1 transaction | 1 upsert+`RETURNING` | transaction: 3 statements | 1 command | 1 round trip |
| QPush | map write | 1 append+flush | 1 transaction | 1 `Write` (element and counter in the same batch) | 1 transaction (element and counter in the same batch) | transaction: 2 statements | transaction: 4 statements | 1 command | 1 round trip |
| Batch(N) | N writes (1 lock acquisition) | 1 write+flush | **1 transaction** | **1 Batch** | **1 `Update` transaction** | **1 transaction** | **1 transaction** | 1 MULTI/EXEC | 1 pipeline |

The bottleneck is usually **one durability point (fsync) per transaction**, not the number of
statements. Adding `?sync=1` puts every commit of the embedded backends back on that
bottleneck (the `sync=1` table in §2: 377–1.3k ops/s); the default mode removes it. Isolated
same-machine experiment (MySQL, 300 single-statement increments):

| Setting | Single-statement increment | 3-statement transaction |
|---|---|---|
| `innodb_flush_log_at_trx_commit=1` (default) | 131 op/s | 127 op/s |
| `innodb_flush_log_at_trx_commit=2` | **266 op/s** | 174 op/s |

Collapsing 3 statements into 1 gains about +3%; relaxing the durability level gains about 2×;
PostgreSQL behaves the same way (490 → 1280 op/s with `synchronous_commit=off`).
**Deployment-side parameters and batch writes are two independent levers.**

## 4. Choosing a backend

- **Embedded, point lookups dominant** → `bolt` (mmap/B+tree reads are very fast, and one
  transaction per write keeps the batch benefit on any medium).
- **Embedded, write throughput first** → `leveldb` (LSM batch flushing; decent single writes on
  every medium).
- **Transactions / in-batch dependent composition** → `badger` (the only embedded backend with
  MVCC transactions and in-batch visibility); the cost is that with synchronous writes reads and
  writes couple to the commit window (§2.4), and write throughput is below leveldb.
- **SQL capability / multi-process sharing** → `sqlite` / `mysql` / `pg` (`mssql` too); watch same-key
  hot spots.
- **Server KV** → `redis` (largest batch benefit) or `ssdb` (native protocol; no transactions,
  limited batch benefit).
- **Lightweight in-process persistence** → `mem` (no disk) or `jsonl` (append-only WAL; understand
  its durability level).
- **General principle**: on virtualized disks, **turn single writes into batch writes first**,
  then talk about choosing a backend. "Every record persisted *and* high throughput" needs real
  local NVMe or server-side group commit.
- **Mixed read/write** (§2.4): `mem`/`jsonl` share a global lock (reads drop to 5–9% under heavy
  writes); `bolt`/`leveldb`/`sqlite` take no lock on the SDK read path yet retention still falls
  with write frequency; server backends do not block the two directions.

## 5. Known trade-offs

| Item | Notes |
|---|---|
| Single-key counters | The SQL family serializes on row locks plus an fsync per commit — an inherent cost |
| SSDB batch writes | No transactions; a pipeline failure may take partial effect (the value is in reducing round trips) |
| Redis in-batch visibility | Not guaranteed (see README "Batch writes" and `Capabilities().BatchComposed`) |
| Badger reads coupled to the commit window | With `?sync=1` a commit takes 1.3–3 ms and mixed-read retention is only 0.2–0.4%; **the default mode (no fsync) has eliminated this phenomenon** (read retention back to ~28%) |
| Durability cost of the default mode | Embedded backends do not fsync per operation by default: a process crash or power loss may lose the most recent acknowledged writes. Power-loss safety requires an explicit `?sync=1` (cost in the `sync=1` table in §2: 56–570× slower) |

## 6. Reproducing

```bash
cd example/bench          # standalone module; changing the directory changes the medium
D=/dev/shm/bt             # tmpfs; or ~/test/tmp (ext4/WSL vhdx), ./tmp (drvfs 9p); mkdir -p "$D"
BE='ThroughputSet$|ThroughputGet$|ThroughputIncrMultiKey$|ThroughputIncrSameKey$|ThroughputQPush$|ThroughputMGet$|ThroughputBatchedSet$|ThroughputMixedReadWrite$'

for u in jsonl bolt leveldb badger sqlite; do
  KVDB_BENCH_URI="$u://$D/t.$u" go test -run '^$' -bench "$BE" -benchtime 2s
  KVDB_BENCH_URI="$u://$D/s.$u?sync=1" go test -run '^$' -bench "$BE" -benchtime 2s   # real block device only
done

# Server backends (start the containers first, see README "Testing")
KVDB_BENCH_URI='redis://127.0.0.1:6379/0' go test -run '^$' -bench "$BE" -benchtime 2s
KVDB_BENCH_URI='ssdb://127.0.0.1:8888'    go test -run '^$' -bench "$BE" -benchtime 2s
KVDB_BENCH_URI='mysql://root:pw@127.0.0.1:3306/db?parseTime=true' go test -run '^$' -bench "$BE" -benchtime 2s
KVDB_BENCH_URI='pg://postgres:pw@127.0.0.1:5432/db?sslmode=disable' go test -run '^$' -bench "$BE" -benchtime 2s
```

Reported metrics: `ops/s`, `items/s` (MGet / batch writes at entry level), `reads/s` and
`writes/s` (per-role completion under mixed load), and `µs/op-actual` (average latency from
completed volume and wall clock, not single-call serial latency). Sequential-call benchmarks
(`BenchmarkSet`/`Get`/`IncrSequential`) exist only for per-call cost and regression comparison;
all throughput conclusions rest on the windowed data above.

Docker container data disks sit on ext4 (vhdx), so the server-backend numbers read as analogous
to the "file backends @ ext4" tier. (Medium caveats for this environment: §1.)
