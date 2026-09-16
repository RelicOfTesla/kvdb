# Performance report

**English** | [中文](PERFORMANCE_CN.md)

Measured throughput, cost model, and selection guidance for each `kvdb` backend.

> The numbers are relative magnitudes from a single-machine container environment and are
> **not promises**; absolute values depend heavily on CPU, storage medium, network, and
> container configuration. Reproduction commands are in §6.

## 1. Scope and method

Only one methodology is used: **saturate the backend with concurrent load over a fixed time
window and count what actually completes** (ops/s; batch writes are converted to per-item
items/s). The basis is `BenchmarkThroughput*` in `example/bench/throughput_test.go`, with 8
goroutines by default and `-benchtime` controlling the window length.

**Why not use per-step latency (`ns/op`) as the basis**: the write path generally involves
coalescing and asynchrony — SQL group commit, SQLite WAL checkpoint piggybacking, LSM
memtable batch flushing, jsonl buffer flushing, Batch-amortized commits, and each engine's
background compaction. Measuring one call serially and taking its reciprocal flattens all of
that away and misreads "absorbed by a buffer" as "fast to disk": for example, the jsonl
buffered mode shows roughly 4.5 µs per `ns/op`, but that is only the cost of "writing into
the memory buffer"; real sustained write capacity depends on how much completes within the
window. This report therefore recognizes only time-window metrics; `ns/op` is auxiliary.

Three variables must be held fixed when comparing:

| Dimension | Impact |
|---|---|
| **Medium** | fsync on tmpfs / ext4 (WSL vhdx) / drvfs (9p) differs by 3 orders of magnitude |
| **Durability level** | Embedded backends do not fsync per operation by default (only `?sync=1` enables it); comparisons across levels are meaningless |
| **key distribution** | Same-key hotspots are limited by single-point serialization; only multiple keys benefit from group commit |

**The medium must be real (especially when `sync=1` is involved)**: on tmpfs fsync is a no-op
and flattens the entire cost of the durable mode — measured on the same backend, `sync=1` on
tmpfs is only 13% slower, while on real ext4 it is 570× slower. Therefore **tmpfs data must
not be used for conclusions such as "fsync cost / durable-mode backoff"**; it is only for
relative comparisons between backends.

Bare `write(16B)+fsync` reference values (to explain the causes, not throughput): tmpfs
2–4 µs, ext4 (vhdx) ~2.5 ms, drvfs (9p) 3.8–5.5 ms. **The "ordinary disk" in this
environment is a WSL vhdx; one fsync takes about 2.5 ms, only about 1.5× faster than 9p, so
it does not belong to the fast tier.** Note also that this repository itself lives on a drvfs
(9p) mount of `G:\`; measure ext4 on a WSL root disk such as `~/test/tmp`.

### 1.1 Durability modes (embedded backends now share one `sync` switch)

| Backend | Default (fast) | `?sync=1` (durable) | Notes |
|---|---|---|---|
| jsonl | flush every 500ms + fsync every 1s | flush + fsync per operation | The two knobs are independent: `?each_flush=1` flushes per operation, `?flush_interval`/`?sync_interval` adjust the periods |
| bolt | no fsync per commit, **periodic flush to disk (1s by default)** | fsync per commit | bbolt `NoSync=true` plus an SDK-built periodic Sync; tunable via `?sync_interval=1s` |
| leveldb | no fsync | fsync per commit | goleveldb `WriteOptions.Sync=false` |
| badger | no fsync | fsync per commit | `WithSyncWrites(false)` |
| sqlite | `NORMAL` | `FULL` (fsync per commit) | `?sync=0` is equivalent to the default (the explicit form); `OFF` is not offered (it corrupts the database) |

**The five local backends take the same default-mode stance**: none of them fsyncs per
commit, and only `?sync=1` provides power-loss safety (the same holds for sqlite: `NORMAL` is
the default and `FULL` corresponds to `?sync=1`).

**The default mode does not mean "never persisted"**, but the backends differ in their
fallback layers, and these should not be conflated:

- **jsonl / bolt**: data first lands in a **process memory buffer** → a process crash loses
  it. Both therefore have a bounded fallback: jsonl flushes every 500ms, bolt flushes to disk
  on a 1s period, and `Close` on both forces one flush (bbolt's own `Close` does not
  fdatasync, so the SDK supplies this layer). Confirmed by strace: with 600ms idle and one
  write, `sync_interval=100ms` triggers 9 fdatasyncs, while at 5s it drops to 4 — the flush
  frequency is indeed controlled by that parameter.
- **sqlite (NORMAL)**: every commit has already been `write()`-ten into the **OS page
  cache**, and the WAL holds every record → **a process crash loses nothing** (measured:
  after writing 200 records, exiting directly without Close recovers 200/200 for both FULL
  and NORMAL). All it skips is "fsync the WAL at commit", so it only affects **machine power
  loss**. SQLite's checkpoint **triggers on frame count** (`DEFAULT_WAL_AUTOCHECKPOINT=1000`
  pages), and there is **no time-driven checkpoint**: measured after writing 3000 records
  (crossing the threshold, with the main database growing 4KB→11.9MB) and then idling for 5s,
  the main database and WAL byte counts are completely unchanged. So it **does not need** a
  periodic flush fallback — the loss boundary of the default mode is simply "power loss loses
  the most recent few commits".

`nosync` is not a usable parameter: writing it produces an immediate error (the default
already means no fsync, and accepting it would mislead people about the durability level).

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

### 2.1 Conclusions read from the main table

- **The read path is barely dominated by the medium**: for the same backend, Get/MGet is
  essentially unchanged across tmpfs/ext4/9p (e.g. leveldb Get ~1.0M, jsonl Get ~9.4–10.1M),
  because reads hit the page cache; only the write path is dominated by the medium.
- **"Whether to use `sync=1`" matters more than "which backend to pick"**: once the default
  mode removes fsync from the write hot path, embedded write throughput is 1–2 orders of
  magnitude higher than with `sync=1` (§2.3). When choosing, decide the durability mode
  first, then pick the backend.
- **Server backends have lower read throughput than embedded ones**: redis Get ~12.4k vs bolt
  Get ~590k — the bottleneck is the network round trip, not the disk; but server backends are
  naturally "durable by default", so there is no need to choose between speed and safety.
- **SQL suffers the worst same-key hotspot degradation**: mysql goes from 428 multi-key to 119
  same-key (3.6×), pg from 2.2k to 647 (3.4×). Embedded backends, with their single-writer
  model, show very little gap between same-key and multi-key.
- **SSDB is balanced across metrics but has a low ceiling** (about 8k ops/s, and batch writes
  only 2.6×): no transactions, and pipelining only saves round trips.
- **mem is the only backend "constrained by no IO at all for either reads or writes"**
  (Set ~775k, Get ~8.4M, batch writes ~1.5M).
- **The embedded backend with the highest write throughput is jsonl** (Set ~353.6k), but its
  lead over leveldb (~164.6k) comes mainly from its 500ms periodic flush buffer rather than
  from an engine difference, so read latency and durability semantics must be weighed
  alongside it.

### 2.2 Batch-write benefit (batch items throughput ÷ Set throughput, main-table mode)

| Backend | ext4 default |
|---|---|
| jsonl | 2.3× |
| bolt | 18.8× |
| leveldb | 2.2× |
| badger | 6.0× |
| sqlite | 3.0× |

The benefit depends on how much commit cost in a single write can be amortized:

- **The batch-write benefit is proportional to "how much commit cost a single write
  contains"**: with `sync=1`, one commit includes one fsync, so batch writes can collapse N
  fsyncs into 1, giving multipliers as high as 39–86× (§2.3); the default mode has no fsync
  left, so the multiplier falls back to 2.2–6.0×, and the remaining amortizable cost is the
  fixed overhead of the transaction/Batch itself.
- **bolt still shows 18.8× in the default mode**: it opens a bbolt transaction on every
  write, and that overhead is independent of the medium, so it can still be batched away on
  tmpfs.
- The SQL family has lower benefit (mysql 6.4×, pg 3.7×) because statements are still
  executed one by one inside the batch and only the commit is shared; Redis's 39× comes from
  a single MULTI/EXEC round trip plus server-side merging.

### 2.3 Medium × durability-mode matrix

The main table covers only "ext4 + default mode". The remaining combinations follow and
**must not be compared directly to the main table** (changing the medium or the mode changes
the comparison baseline).

**`?sync=1` (fsync per commit) @ ext4**: only the write path is affected; reads are the same
as in the main table.

| Backend | Set | QPush | Batch items | Slower than default |
|---|---|---|---|---|
| jsonl | ~377 | ~410 | — | ~940× |
| bolt | ~449 | ~464 | ~86× | ~56× |
| leveldb | ~1.3k | ~1.3k | ~71× | ~128× |
| badger | ~748 | ~736 | ~39× | ~109× |
| sqlite | ~440 | — | ~70× | ~29× |

**File backends @ tmpfs (RAM disk)**

| Backend | Set | Get | Incr multi-key | Incr same-key | QPush | MGet items | Batch items |
|---|---|---|---|---|---|---|---|
| jsonl | ~330.7k | ~10.1M | ~221.9k | ~298.2k | ~580.9k | ~13.4M | ~510.5k |
| bolt | ~22.1k | ~569.8k | ~22.9k | ~26.9k | ~20.9k | ~2.2M | ~479.1k |
| leveldb | ~137.0k | ~1.0M | ~111.8k | ~122.4k | ~108.5k | ~1.0M | ~381.8k |
| badger | ~67.0k | ~274.4k | ~63.5k | ~34.0k | ~53.1k | ~638.4k | ~461.5k |
| sqlite | ~12.9k | ~64.9k | ~5.8k | ~7.4k | ~8.8k | ~460.1k | ~42.6k |

**File backends @ drvfs (9p)**

| Backend | Set | Get | Incr multi-key | Incr same-key | QPush | MGet items | Batch items |
|---|---|---|---|---|---|---|---|
| jsonl | ~1.4k | ~9.6M | ~1.5k | ~1.5k | ~1.6k | ~13.3M | ~110.3k |
| bolt | ~89 | ~589.5k | ~91 | ~88 | ~94 | ~2.6M | ~7.1k |
| leveldb | ~1.0k | ~1.1M | ~1.0k | ~288 | ~1.1k | ~1.0M | ~75.2k |
| badger | ~321 | ~297.6k | ~312 | ~286 | ~338 | ~622.8k | ~26.8k |
| sqlite | ~234 | ~9.6k | ~129 | ~151 | ~105 | ~102.0k | ~16.1k |

**Mixed read/write @ each mode** (the main table already lists the default mode; the remaining
combinations are filled in here)

| Backend | Medium/Mode | Mixed read | Mixed write | Read retention |
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

**One conclusion from the read matrix**: **the 9p mode generally has a larger batch-write
benefit** (68–84×) — even flush has to go through a protocol round trip, so a single write is
dominated by round-trip cost. (Why tmpfs numbers must not be used for fsync conclusions is in
§1; it is not repeated here.)

### 2.4 Mixed read/write: are readers blocked by writers?

The `Mixed read/Mixed write/Read retention` columns of the main table are the source of this
conclusion; the cross-mode comparison is in §2.3.

- **Server backends barely block each other**: reads and writes each retain ~44–63%,
  equivalent to splitting concurrency in half. The connection pool isolates requests, so
  reads do not queue behind writes.
- **mem / jsonl reads drop to ~5–9% of pure-read under high write frequency**. jsonl's read
  path takes no lock of its own yet still only reaches 5%, showing that the bottleneck is the
  granularity of the global lock in mem that both share: every write entering that lock makes
  subsequent readers queue.
- **The size of the drop is determined by how often writers enter the shared synchronization
  primitive, not by medium bandwidth**: the lower the write frequency (e.g. a few hundred
  ops/s on 9p), the higher the read retention — a slower disk actually lets readers interleave
  more easily.
- **"Get is dozens of times faster than Set" holds only under no or low write load**. Under
  mixed load you should look at read retention, and the levers that improve read blocking are
  the same ones that cut write latency: batch writes (merging N acquisitions of a lock or
  transaction into 1) and sharding.
- **badger's mixed read is governed by the "write commit window", not by write frequency**:
  with `sync=1`, one commit on ext4/9p takes 1.3–3 ms, readers queue behind the commit lock,
  and read retention falls to only 0.2–0.4% (yet its retention for writes is as high as
  91–98%, showing that writes themselves are not slowed). The default mode (no fsync) has
  eliminated this phenomenon: reads return to ~76.4k and retention to ~28%, on par with the
  other embedded backends.
- **Raising write frequency does not make read retention worse**: bolt/leveldb/badger in the
  default mode have write frequencies 1–2 orders of magnitude higher than with `sync=1`, yet
  read retention remains at 11–28%. This shows that read blocking in these backends comes
  mainly from the **duration** for which a writer occupies the synchronization primitive, not
  from the number of times.
- Read retention >100% (some sqlite modes) is scheduling and page-cache jitter in that mode's
  own pure-read baseline, at the noise level.

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

- **Embedded + point lookups dominant**: `bolt` (mmap/B+tree reads are extremely fast, and one
  transaction per write means the batch-write benefit holds on any medium).
- **Embedded + write throughput first**: `leveldb` (LSM batch flushing; single writes are
  decent on both tmpfs and 9p).
- **Need transactions / in-batch dependent composition**: `badger` (the only embedded backend
  offering MVCC transactions and in-batch visibility); the cost is that with synchronous
  writes by default, reads and writes are coupled to the commit window (§2.4), and write
  throughput is lower than leveldb.
- **Need SQL capability / multi-process sharing**: `sqlite` / `mysql` / `pg`; watch out for
  same-key hotspot degradation.
- **Server KV**: `redis` (largest batch-write benefit), `ssdb` (native protocol, but no
  transactions and limited batch-write benefit).
- **Lightweight in-process persistence**: `mem` (does not hit disk), `jsonl` (append-only WAL;
  its durability level must be understood).
- **General principle**: on WSL/virtualized disks, **first turn writes into batch writes**,
  then talk about picking a backend; wanting "every record persisted and high throughput"
  requires real local NVMe or a server-side group-commit strategy.
- **Mixed read/write scenarios** (§2.4): `mem`/`jsonl` share a global lock, so under high write
  frequency reads drop to only 5–9% of pure reads; `bolt`/`leveldb`/`sqlite` take no lock in
  the SDK-level read path, yet read retention still declines with write frequency (11–61% on
  tmpfs, 54–193% on 9p); server backends do not block reads and writes against each other
  (each retains 44–63%).

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
cd example/bench   # standalone module; changing the directory changes the medium

D=/dev/shm/bt            # tmpfs; or ~/test/tmp (ext4/WSL vhdx), ./tmp (drvfs 9p)
mkdir -p "$D"
BE='ThroughputSet$|ThroughputGet$|ThroughputIncrMultiKey$|ThroughputIncrSameKey$|\
ThroughputQPush$|ThroughputMGet$|ThroughputBatchedSet$|ThroughputMixedReadWrite$'

for u in jsonl bolt leveldb badger sqlite; do
  case $u in jsonl) p="$D/t.jsonl";; bolt) p="$D/t.bolt";;
                 leveldb) p="$D/t.ldb";; badger) p="$D/t.badger";;
                 sqlite) p="$D/t.db";; esac
  KVDB_BENCH_URI="$u://$p" go test -run '^$' -bench "$BE" -benchtime 2s
done

# Durable-mode comparison: only on a real block device (fsync on tmpfs is a no-op and shows no cost)
for u in jsonl bolt leveldb badger; do
  case $u in jsonl) p="$D/s.jsonl";; bolt) p="$D/s.bolt";;
                 leveldb) p="$D/s.ldb";; badger) p="$D/s.badger";; esac
  KVDB_BENCH_URI="$u://$p?sync=1" go test -run '^$' -bench "$BE" -benchtime 2s
done

# Server backends (containers must be started first, see README "Testing")
KVDB_BENCH_URI='redis://127.0.0.1:6379/0' go test -run '^$' -bench "$BE" -benchtime 2s
KVDB_BENCH_URI='ssdb://127.0.0.1:8888'    go test -run '^$' -bench "$BE" -benchtime 2s
KVDB_BENCH_URI='mysql://root:pw@127.0.0.1:3306/db?parseTime=true' \
  go test -run '^$' -bench "$BE" -benchtime 2s
KVDB_BENCH_URI='pg://postgres:pw@127.0.0.1:5432/db?sslmode=disable' \
  go test -run '^$' -bench "$BE" -benchtime 2s
```

Reported metrics: `ops/s` (real throughput), `items/s` (MGet / batch writes converted to the
per-item level), `reads/s` and `writes/s` (how much each of the two roles completes under
mixed load), `µs/op-actual` (the average latency computed from completed volume and wall
clock, not the serial latency of a single call).

There are also **sequential-call** benchmarks such as `BenchmarkSet`/`Get`/`IncrSequential`,
used only to investigate the inherent cost of a single call and for regression comparison;
all throughput conclusions are based on the measured data in this section.

Docker container data disks sit on ext4 (vhdx), so the server-backend numbers can be read as
analogous to the "file backends @ ext4" tier. (Medium caveats for this environment are in §1.)
