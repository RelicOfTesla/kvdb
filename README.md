# kvdb

**English** | [中文](README_CN.md)

A Go persistence adapter SDK. It exposes a unified **KV + Queue + ZSet** interface over a
pluggable persistence backend (in-memory / jsonl / SQL / Redis / SSDB / BoltDB), with built-in
batched writes and byte codec helpers.

Semantics follow the **SSDB/Redis family**, with the command *names* taken from SSDB
(`qpush`/`qpop`/`zset`…) while KV behavior follows Redis where the two agree. A few points
differ deliberately from Redis — see [Semantics](#semantics) for the exact list (notably
`Set` preserving TTL, and `Scan` being a deterministic range query rather than Redis's
cursor-based `SCAN`).

> This tool is AI-generated. Use at your own discretion.

```go
db, _ := kvdb.Open(ctx, "sqlite://./data.db")
defer db.Close()

db.Set(ctx, "user:1", []byte("alice"))
n, err := db.Incr(ctx, "visits", 1)
```

## Features

- **11 built-in backends**, one single API: `mem` / `jsonl` / `bolt` / `leveldb` / `badger` / `sqlite` / `mysql` / `pg` / `mssql` / `redis` / `ssdb`
- **Optional, probed-on-demand capabilities**: KV is mandatory; Queue / ZSet / Batch / lifecycle are optional capabilities,
  returning `ErrUnsupported` when unimplemented, and probeable via `Capabilities()`
- **Empty registry by default**: use a backend and `import _` its package; the root package and mod pull in no driver dependencies
- **Interface-based returns**: `kvdb.Open` returns the interface `DB`, so business code can depend narrowly on sub-interfaces such as `KvProvider`, which eases mocking
- **Batched writes**: a batch of operations maps onto each backend's native mechanism (transaction / MULTI/EXEC / pipeline / a single flush)
- **Bytes ↔ generic helpers**: `Enc` / `Dec` / `D` / `DMust` support scalars and structs (JSON by default, codec replaceable); scalar encoding interoperates with `Incr`. `D` passes `ok`/`err` through untouched; `DMust` inspects only `err`
- **Local becomes remote (c/s)**: `rpc` exposes any backend as a server, and the client accesses it through the same set of interfaces;
  the client is **unaware of the server's underlying backend**, and comes with c/s authentication (plaintext / challenge-response) plus optional TLS, with a replaceable protocol codec
- **Optional Go 1.27.1+ thin shell**: `kvdb.Typed(store)` provides the generic read/write methods `db.Get[T](...)` / `db.Set(ctx, k, v)` (isolated by build constraints)
- Pure Go dependencies, no CGO

**Each backend is an independent module**, declaring its own minimum required Go version — a
project that only uses `mem` / `jsonl` / `ssdb` will not have its version requirement raised
by the SQL / Redis drivers:

| Module | Min Go | Notes |
|---|---|---|
| `kvdb` (root, including `core` / `kvdbtest`) | 1.18 | Zero third-party dependencies |
| `kvdb/mem`, `kvdb/jsonl` | 1.18 | Standard library only |
| `kvdb/sqlstore` | 1.18 | Standard library only (the `database/sql` abstraction) |
| `kvdb/ssdb`, `kvdb/leveldb` | 1.19 | Uses `atomic.Bool` / `atomic.Pointer[T]` |
| `kvdb/badger` | 1.24 | Badger v4 itself declares `go 1.24.0` |
| `kvdb/bolt`, `kvdb/sqlite`, `kvdb/mysql`, `kvdb/pg`, `kvdb/redis` | 1.25 | Determined by the driver and its transitive dependencies (e.g. `golang.org/x/sys` requires 1.25) |
| `kvdb/mssql` | 1.18 | `go-mssqldb` v1.8.2 (newer driver lines v1.9+ require `go 1.25`; this module keeps the 1.18 baseline to align with the root package) |
| `kvdb/rpc` | 1.19 | Standard library + root package only: **zero third-party dependencies** (especially important for the client) |
| `kvdb/all`, `kvdb/bench`, `kvdb/example` | 1.25 | Aggregates the modules above |
| `kvdb/rpcserver` | 1.25 | A directly runnable RPC server command (imports `all` to wire in every backend) |

Versions are taken as the **largest `go` directive in each module's dependency graph**
(`go list -m -f '{{.GoVersion}}' all`), not copied from the declared value of a direct
dependency.

Measured: a Go 1.20 consumer importing only `kvdb/mem` builds and runs normally, with an
empty dependency closure (no go.sum is produced).

## Installation

```bash
go get github.com/RelicOfTesla/kvdb        # root package
go get github.com/RelicOfTesla/kvdb/mem    # pull in each backend module as needed
```

```go
import (
    "github.com/RelicOfTesla/kvdb"
    _ "github.com/RelicOfTesla/kvdb/sqlite"   // wire in the backend as needed
)
```

## Quick start

```go
import (
    "context"

    "github.com/RelicOfTesla/kvdb"
    _ "github.com/RelicOfTesla/kvdb/sqlite"  // wire in sqlite only; or _ .../all to wire in everything at once
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
db.SetEx(ctx, "session", []byte("token"), 1800) // write and set a TTL
n, _ := db.Incr(ctx, "visits", 1)
pairs, _ := db.Scan(ctx, "user:", "user;", 100) // closed interval: the byte after ":" is ";"

// Queue (optional capability)
db.QPush(ctx, "jobs", []byte("job-1"))
job, ok, _ := db.QPop(ctx, "jobs")

// ZSet (optional capability)
db.ZSet(ctx, "rank", "alice", 90)
top, _ := db.ZRange(ctx, "rank", 0, -1)
```

URI overview (same backend = same scheme or the same `sqlstore` flavor, shown as one merged
group; each package also offers an equivalent direct constructor, such as `sqlite.Open`):

```
mem://
jsonl://./data.jsonl?sync=1        # default flush every 500ms, fsync every 1s; sync=1 does flush+fsync per operation (nothing lost on power failure)
jsonl://./data.jsonl?each_flush=1  # flush per operation (fsync still periodic); flush_interval/sync_interval tune the periods

# ---- SQL 基座（同 sqlstore 基座，仅 scheme 与 DSN 形态不同；table_prefix 通用） ----
sqlite://./data.db?sync=1          # default NORMAL (no fsync per commit, consistent with other local backends); sync=1 uses FULL (nothing lost on power failure)
mysql://user:pass@host:3306/dbname?parseTime=true&table_prefix=app_
pg://user:pass@host:5432/dbname?sslmode=disable&table_prefix=app_
mssql://sa:pass@host:1433?database=dbname&encrypt=disable&table_prefix=app_   # 其余 query 参数照传 go-mssqldb

bolt://./data.bolt?sync=1&sync_interval=1s   # by default no fsync per commit, but data is written out periodically per sync_interval (1s by default); sync=1 fsyncs per commit
leveldb://./data.dir?sync=1&cache=8&wb=4     # directory-based storage; no fsync by default, sync=1 fsyncs per commit; cache/wb are in MiB
badger://./data.dir?sync=1&cache=64&memtable=64   # directory-based storage; no fsync by default, sync=1 fsyncs per commit; cache/memtable are in MiB
redis://:password@host:6379/0?key_prefix=app:   # key namespace prefix
ssdb://[user:pass@]host:8888?key_prefix=app:   # SSDB has no namespace; isolate with a logical prefix; with user:pass = the auth style (when server.auth is on; OpenWithConfig also supports online Auth)
rpc://host:7788?auth=challenge&password=s3cret   # connect to an RPC server (see "Local becomes remote" for details)
```

**Use prefix isolation when sharing one storage with other applications**: `TablePrefix` in
`sqlite/mysql/pg/mssql`'s `Config` prefixes the four tables and secondary indexes; `KeyPrefix` in
`redis`'s `Config` derives the three segments `<pfx>kv:` / `<pfx>q:` / `<pfx>z:`; `KeyPrefix`
in `ssdb`'s `Config` prefixes the key names of the three data kinds logically (stripped
automatically on read-back, and `Scan` is confined to that prefix too). The defaults are the
current layout, and stay unchanged when unconfigured.

See [`example/main.go`](example/main.go) for a complete demo.

## Built-in backends

| Backend | Package | KV | Queue | ZSet | Notes |
|---|---|---|---|---|---|
| In-memory | `mem` | ✅ | ✅ | ✅ | No persistence, for tests/caching |
| JSONL log | `jsonl` | ✅ | ✅ | ✅ | append-only WAL, replayed on open, supports `Compact()`; single-process embedded |
| BoltDB | `bolt` | ✅ | ✅ | ✅ | bbolt single-file B+tree (pure Go); commits one transaction per write, and one commit for a whole batch |
| LevelDB | `leveldb` | ✅ | ✅ | ✅ | syndtr/goleveldb LSM-tree (pure Go); batched writes go into a single Batch committed atomically |
| Badger | `badger` | ✅ | ✅ | ✅ | dgraph-io/badger LSM-tree (pure Go); has MVCC transactions, a batch is one `Update` commit, **visible within the batch** |
| SQLite | `sqlite` | ✅ | ✅ | ✅ | Pure Go driver (modernc), no CGO |
| MySQL | `mysql` | ✅ | ✅ | ✅ | Shares `sqlstore` |
| PostgreSQL | `pg` | ✅ | ✅ | ✅ | Shares `sqlstore` |
| SQL Server | `mssql` | ✅ | ✅ | ✅ | Shares `sqlstore`; upsert via `MERGE`, row lock `WITH (UPDLOCK, HOLDLOCK)`, pagination `OFFSET/FETCH NEXT`; Incr takes the transactional path (no `RETURNING`) |
| Redis | `redis` | ✅ | ✅ | ✅ | Native String / List / Sorted Set mapping |
| SSDB | `ssdb` | ✅ | ✅ | ✅ | Native text protocol client, connection pool + authentication |
| RPC | `rpc` | ✅ | ✅ | ✅ | Connects to a remote kvdb server; capabilities follow the server's backend, and the client is unaware of it |

The import path is `github.com/RelicOfTesla/kvdb/<package>`, plus the aggregate package `.../all`.

## Capability model

`kvdb.Open` / `kvdb.Wrap` return the interface `DB`, which is composed of several capability
interfaces; the concrete adapter is an unexported implementation:

```go
type DB interface {
    KvProvider      // KV (mandatory capability)
    QueueProvider   // queue
    ZSetProvider    // sorted set
    Batcher         // db.Batch(ctx, fn)
    Closer          // Close
    Capabilities() core.Caps   // capabilities actually present
}
```

- Capabilities a backend does not implement: calls return `ErrUnsupported`; probing with
  `Capabilities()` first avoids that.
  `core.Caps` is a struct (adding a capability does not change signatures):

```go
c := db.Capabilities()
c.Queue, c.ZSet, c.Batch           // whether the corresponding interfaces are implemented
c.BatchComposed                    // whether later operations in a batch can see earlier effects of the same batch
```

- **In-batch visibility is a perceptible, non-mandatory capability**: when several operations
  in the same batch touch the same key / queue / zset member, the final value depends on the
  backend's mechanism. Backends that apply operations one by one within the same transaction
  or the same lock (`mem` / `jsonl` / `bolt` / `badger` / `sqlite` / `mysql` / `pg`) are `true`;
  LevelDB's Batch, Redis's MULTI/EXEC, and SSDB's pipeline cannot read uncommitted content
  before commit, so they are `false`. When deterministic composition is needed, probe first
  and then decide, or simply split interdependent operations into separate batches.
- **Narrow dependencies**: a business function only needs to declare the capability interfaces it uses, and tests only need to implement the corresponding methods, not the whole `DB`:

```go
func touch(ctx context.Context, store kvdb.KvProvider, key string) (int64, error) {
    return store.Incr(ctx, key, 1)   // both a real backend and a mock can be passed in
}
```

- `kvdb.Unwrap(db)` retrieves the backend behind an adapter (returns nil for non-adapter implementations).

## Batched writes

A batch of operations commits in one go, significantly reducing round trips and persistence overhead:

```go
err := db.Batch(ctx, func(b *kvdb.Batch) error {
    b.Set("k", value)
    b.QPush("jobs", payload)
    b.ZIncr("rank", "alice", 1)
    return nil        // only nil commits; an error or a validation failure during collection leaves the whole batch without effect
})
```

- Designed as "a shared collector + backends implementing only the commit": a backend implements `ApplyBatch(ctx, ops)`,
  and each backend maps it onto its native mechanism (SQL = one transaction, Redis = one MULTI/EXEC, SSDB = one
  pipeline, jsonl = one flush, mem = holding the lock once).
- Only **unconditional writes** are allowed inside a batch (Set/SetEx/Del/Expire/QPush/QPushFront/ZSet/ZDel/ZIncr);
  `Incr`/`QPop` depend on the key's current state and require validation before commit, so mixing them in would break the
  batch's atomicity and they must be called separately.
- Atomicity: MySQL/SQLite/PG (transactions), Redis (MULTI/EXEC), mem/jsonl (in-process)
  leave the whole batch without effect when the commit fails; **SSDB has no transactions**, so a
  pipeline failure may take partial effect (its value lies in reducing round trips).

## Bytes ↔ T helpers

```go
type User struct {
    ID   int64    `json:"id"`
    Name string   `json:"name"`
    Tags []string `json:"tags"`
}

// Write: inline encoding
db.Set(ctx, "n", kvdb.Enc(int64(42)))     // scalar -> decimal text
db.Set(ctx, "u", kvdb.Enc(User{ID: 7}))   // struct -> JSON

// Read: D merges the (val, ok, err) three return values of Get/QPop.
// Both ok and err pass through as-is: a missing key gives ok=false, err=nil.
n, ok, err := kvdb.D[int64](db.Get(ctx, "n"))
u, ok, err := kvdb.D[User](db.Get(ctx, "u"))
v, ok, err := kvdb.D[string](db.QPop(ctx, "jobs"))

// Treat "missing" as an error yourself when that is what you want:
//   if err != nil { return err }   // IO / parse error
//   if !ok { return kvdb.ErrNotFound }

// panic variant: only inspects err (ok is ignored, so a missing key yields the zero value)
n := kvdb.DMust[int64](db.Get(ctx, "n"))
raw, err := kvdb.Dec[User](b)
```

Encoding rules:

| Type | Encoding | Notes |
|---|---|---|
| integers (including `~` aliases), float, string, bool | text | interoperates with `Incr` (`Enc(int64)` → decimal) |
| `[]byte` | identity | does not go through JSON/base64 |
| structs, slices, maps, pointers, interfaces, etc. | `Marshal` (JSON by default) | supports nested structs and `json` tags; unexported fields are ignored |

**The codec is replaceable**: `kvdb.Marshal` / `kvdb.Unmarshal` are package-level variables,
JSON by default, and can be swapped in init for msgpack / protobuf / gob and the like; the
scalar path is unaffected.

```go
func init() {
    kvdb.Marshal = msgpack.Marshal
    kvdb.Unmarshal = msgpack.Unmarshal
}
```

Note that `Enc` panics on an encoding failure (it returns no error); call `Marshal` directly when
error handling is needed.

### Go 1.27.1+: the `TypedStore` shell (read and write both via type parameters)

`TypedStore` corresponds one-to-one with `StoreProvider` (`KvProvider + QueueProvider + ZSetProvider`)
— the generic shell depends on this one interface only, and does not require `Batch`/`Close`:

```go
tdb := kvdb.Typed(db)                      // db only needs to satisfy kvdb.StoreProvider
u, err := tdb.Get[User](ctx, "user:1")     // = kvdb.D[User](db.Get(ctx, "user:1"))
err = tdb.Set(ctx, "user:1", u)            // = db.Set(ctx, "user:1", kvdb.Enc(u))
err = tdb.SetEx(ctx, "sess", s, 3600)
n, err := tdb.Get[int64](ctx, "visits")    // scalars use text encoding, interoperating with Incr
job, err := tdb.QPush(ctx, "jobs", j)      // queue writes are generic too
ms, err := tdb.MGet[User](ctx, "user:1", "user:2")
```

- **Writes**: `Set[T]` / `SetEx[T]` / `QPush[T]` / `QPushFront[T]`, encoded with `Enc[T]`;
- **Reads**: `Get[T]` / `MGet[T]` / `QPop[T]` / `QPopBack[T]` / `QFront[T]` / `QBack[T]`,
  decoded with `Dec[T]`/`D[T]`; empty/missing is folded into `ErrNotFound`. Each of them has an
  `…OK` variant (`GetOK` / `QPopOK` / `QPopBackOK` / `QFrontOK` / `QBackOK`) that **preserves the
  `ok` value** instead: `ok=false` with `err=nil`;
- when `T = []byte` it is **exactly equivalent** to calling the backend method directly (`Enc` passes `[]byte` through by identity);
- **Batched writes**: `tdb.BatchT(ctx, func(b kvdb.TypedBatch) error {...})` — encoding happens at collection time,
  and **one batch may mix several types** (`b.Set("cnt", 42)` coexisting with `b.Set("u:1", u)`);
  `Del`/`Expire`/`ZSet` and the like remain directly available through the embedded `*Batch`;
- generic methods shadow same-named methods, so **`TypedStore` does not satisfy `StoreProvider`** (different signatures);
  use `tdb.StoreProvider` when the original interface is needed; the parameter is `StoreProvider` (excluding Batch/Close),
  so `kvdb.Typed(db)` works just as well on the value returned by `kvdb.Open`;
- version and gating: method-level type parameters have been supported since Go 1.27, and the implementation lives in a file carrying `//go:build go1.27`
## Semantics

- **KV**: `Set / SetEx / SetExAt / Get / Del / Exists / Incr / MGet / Scan / Expire / ExpireAt / TTL`,
  corresponding to SSDB `set/setx/get/del/exists/incr/multi_get/scan/expire/ttl`.
  - Keys and values are binary-safe; `Scan(start, end, limit)` is **byte-ordered closed-interval**
    ascending, an empty string means unbounded on that side, and `limit<=0` uses
    `DefaultScanLimit` (100).
  - `Set` preserves the existing TTL; `SetEx` overwrites the TTL, and `ttl<=0` returns `ErrInvalidTTL`.
  - `SetExAt` / `ExpireAt` take an **absolute** deadline in unix seconds (Redis `SETEXAT` / `EXPIREAT`);
    a deadline already in the past **deletes the key** rather than erroring. Use these when the deadline
    must survive a restart or be shared across processes — `SetEx(ttl)` computes it from "now".
  - A missing `Incr` key counts from 0; a value that is not a decimal integer returns `ErrNotInteger`.
  - `TTL` returns `(remaining seconds, ok)`, where `ok=false` means the key does not exist /
    has no TTL / has expired.
- **Queue**: `QPush / QPushFront / QPop / QPopBack / QSize / QFront / QBack`, first in first out.
- **ZSet**: `ZSet / ZGet / ZDel / ZSize / ZRank / ZRange / ZIncr`, ordered by
  `(score ascending, key ascending)`, ranks starting at 0; `ZRange(start, stop)` uses
  0-based inclusive indices, and negative indices count from the end. **The score type is int64**
  (aligned with SSDB); the Redis backend converts through float64, which is
  lossless within `|score| ≤ 2^53`.
- The namespaces of the three data types are independent of each other. Sentinel errors:
  `ErrUnsupported` / `ErrClosed` / `ErrNotInteger` / `ErrInvalidTTL` / `ErrNotFound`.

### Differences from Redis

Command **names** are SSDB's, not Redis's. The following points also differ in behavior —
know them before porting Redis code:

| Topic | Redis | kvdb |
|---|---|---|
| `Set` and TTL | **clears** the TTL (`KEEPTTL` needed to keep it) | **preserves** the TTL (SSDB `set` semantics); the Redis backend uses `SET ... KEEPTTL` internally |
| Absolute deadlines | `SETEXAT` requires Redis ≥ 6.2; `EXPIREAT` in seconds | `SetExAt` / `ExpireAt` always available; SSDB has no absolute command, so its backend converts to a relative TTL client-side |
| `Scan` | cursor-based iteration, unordered, only guarantees full coverage over a finite number of calls | deterministic **byte-ordered closed-interval** range query, ascending, with a limit |
| `TTL` | `-2` = key missing, `-1` = exists without TTL | both collapse into `ok=false`; the two cases cannot be told apart |
| `Del` / `Expire` | return how many keys were affected | return only `error` |
| `Exists` | accepts multiple keys, returns a count | single key, returns `bool` |
| `Incr` overflow | always errors | errors on SQL/Redis; **wraps around silently** on mem/bolt/jsonl/leveldb/badger/SSDB (probe via `Caps.IncrWraps`) |
| ZSet score | IEEE-754 double (fractional values allowed) | `int64`; the Redis backend is lossless only within `\|score\| ≤ 2^53` |
| List commands | `LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`LLEN`/`LINDEX` | `QPush`/`QPushFront`/`QPop`/`QPopBack`/`QSize`/`QFront`/`QBack` |
| Sorted-set commands | `ZADD`/`ZSCORE`/`ZREM`/`ZCARD`/`ZINCRBY` | `ZSet`/`ZGet`/`ZDel`/`ZSize`/`ZIncr` (only `ZRank`/`ZRange` keep the Redis names) |

`ZRange`'s 0-based inclusive indices, negative indices counting from the end, and the
`(score, key)` ordering all match Redis exactly.

### Backend differences (unified externally)

| Difference | Handling |
|---|---|
| SSDB `scan` uses an open start interval | The ssdb backend compensates with one get on an existing start key, so externally it is still a closed interval |
| Redis has no byte-ordered range scan | The redis backend SCANs a KV prefix, then filters and sorts on the client; the cost scales with the number of KV keys |
| Redis keyspace | The three data types automatically get the `kvdb:kv:` / `kvdb:q:` / `kvdb:z:` prefixes, guaranteeing namespace independence |
| Redis `Set` | Uses `SET ... KEEPTTL` to preserve the existing TTL (requires Redis ≥ 6.0) |
| SQL expired rows | Filtered on the read path, and cleaned up once when the database is opened |
| SQLite concurrent writes | In-process write serialization (single writer), WAL keeps reads parallel |
| BoltDB concurrent writes | Single writer, multiple readers (MVCC); writes are committed serially as bbolt transactions |
| LevelDB has no buckets / no transactions | A single ordered keyspace, with the three data types isolated by first-byte namespace tags; `Write(batch)` is itself atomic, and all multi-key writes (including value+TTL and the zset two-sided index) are collected into one Batch. Queue/zset counters and member scores are composed in-batch via a per-batch pending state, so a batch is read-your-writes (`BatchComposed=true`) |
| LevelDB concurrent Incr | LevelDB has no CAS primitive, so read-modify-write on the same key is serialized by a sharded lock (different keys still run in parallel) |
| Badger has transactions | Multi-key writes are committed with a `db.Update` transaction, giving read-your-writes within the batch (`BatchComposed=true`). Read-modify-write on the same key is still first serialized by a sharded lock: the SSI conflict retry of transactions alone also guarantees correctness, but hot spots on the same key degenerate into a retry storm |
| Badger TTL | Native `WithTTL` is not used (it follows real time); instead a separate TTL record plus an injectable clock is used for the decision, consistent with leveldb |
| SQL key length | MySQL key columns are capped at 255 bytes (compatible with the 5.6 default index prefix); PG / SQLite use BYTEA/BLOB with no such limit |
| Write semantics of expired keys | All backends uniformly treat "expired = nonexistent": `Set` does not inherit the old TTL, `Incr` counts from 0, and `Expire` does not revive |
| Ownership of read return values | `Get`/`MGet`/`Scan`/`QFront`/`QBack` return copies, so the caller mutating them does not affect the store's state |

### Injectables for testing and tuning

| Entry point | Effect |
|---|---|
| `kvdb.Now` / `core.Now` | The sole entry point through which backends obtain the current instant (defaults to `time.Now`). Replacing it in tests deterministically triggers TTL boundaries without sleeping through real seconds |
| `core.SweepInterval` | The minimum interval at which the write path also reclaims expired entries (seconds, default 60); 0 means reclaim on every write |
| `ssdb.DialTimeout` / `redis.ScanCount` | Connection timeout, and the workload hint for each round of SCAN |

Replacing global variables is not a concurrency-safe practice: set them during test
initialization and restore them with `t.Cleanup`.

## Extending: custom backends

Implement `core.KvProvider` (mandatory for KV) plus optional capability interfaces, and once
registered it can be used through `kvdb.Open`:

```go
type                   myStore struct{ /* ... */ }

func                   (m *myStore) Set(ctx context.Context, key string, value []byte) error { /* ... */ }
//                     ... the remaining KV methods; optionally also implement core.QueueProvider / core.ZSetProvider / core.BatchProvider

func                   init() {
    kvdb.MustRegister("mybase", func(ctx context.Context, u *url.URL) (core.KvProvider, error) {
        return &myStore{}, nil
    })
}
//                     On the business side, import _ "your/module/mybase", then kvdb.Open(ctx, "mybase://...") works
```

`kvdb.Register` returns an error on a duplicate or empty scheme; `kvdb.Schemes()` lists the
registered schemes. `FullProvider` is used to declare the full "KV + Queue + ZSet + Batch + Close"
capability set all at once.

## Local to remote (RPC client/server)

Put any backend on the server side, and the client accesses it over the network through **the
same set of interfaces** — useful for cross-process/cross-machine isolation, and for turning
embedded backends (jsonl/bolt/sqlite…) into a service shared by multiple consumers.

```go
//                     Server: just pick a backend (the server side imports the corresponding backend package or .../all)
srv,                   err := rpc.NewServer(ctx, rpc.ServerConfig{
    Addr:     ":7788",
    Backend:  "jsonl://./data.jsonl",   // switch to bolt/sqlite/mysql/ssdb… and the client needs no change
    Auth:     rpc.AuthChallenge,
    Password: "s3cret",
})
go                     srv.Serve(ctx)

//                     Client: imports no backend at all, and needs no knowledge of what the peer is
db,                    err := kvdb.Open(ctx, "rpc://127.0.0.1:7788?auth=challenge&password=s3cret")
defer                  db.Close()
db.Set(ctx,            "k", []byte("v"))           // KV / Queue / ZSet / Batch are all available
```

You can also just run the ready-made server command:

```bash
go                     run ./rpcserver -addr :7788 -backend jsonl://./data.jsonl -auth challenge -password s3cret
```

Key points:

- **The client is unaware of the backend**: `rpc` implements `core.FullProvider`, the same set of
  interfaces as the local backends. Whether it is connected to jsonl or mysql, the client code is
  identical; switching backends changes only one server-side parameter.
- **The `rpc` module has zero third-party dependencies** (only the standard library + the root
  package), and the client side pulls in only `kvdb` + `kvdb/rpc`.
- **Capabilities pass through faithfully**: what `db.Capabilities()` reports is exactly the
  capability of the **server-side backend**, including `BatchComposed` — and it does not
  "become stronger" just because an RPC layer was wrapped around it.
- **Sentinel errors cross the wire as-is**: on the client, `ErrUnsupported` / `ErrClosed` /
  `ErrNotInteger` / `ErrInvalidTTL` / `ErrNotFound` can be compared normally with `errors.Is`.
- **A batch write is one round trip**: `db.Batch(...)` sends the whole batch to the server, and the
  backend commits it once; visibility within the batch depends on the backend itself (the same as
  a local direct connection).
- **Lifecycle boundary**: the client's `Close()` only closes its own connection and does not close
  the server-side backend.

### Authentication (the c/s protocol's own auth, unrelated to backend auth)

| Mode | URI parameter | Notes |
|---|---|---|
| No authentication | `auth=none` (default) | Bare on localhost/intranet |
| Plaintext | `auth=plain&password=…` | The password is sent directly; **off by default**, use it only when TLS/unix socket is already in place |
| Challenge-response | `auth=challenge&password=…` | The server issues a one-time nonce, and the client replies with `HMAC-SHA256(password, nonce)`; **the password never goes on the wire**, and replays are ineffective. Choose this when authentication is needed |

The password can also be written in the userinfo: `rpc://:s3cret@host:7788?auth=challenge`.
Supplying a password without writing `auth=` is an immediate error, avoiding the case of
"thinking it is encrypted when it is not".

> This is **transport-layer** authentication. The backend's own authentication (such as a mysql
> user password or ssdb `server.auth`) is handled by the server when connecting to the backend,
> and the client neither bears it nor should be aware of it.

### TLS

Standard `crypto/tls`, **switched on the same port according to the server configuration**
(give a certificate and it is TLS, give none and it is plaintext):

```bash
rpc://host:7788?tls=1&ca=./ca.pem                                  # verify the server
rpc://host:7788?tls=1&ca=./ca.pem&cert=./c.pem&key=./c.key         # mutual TLS
rpc://host:7788?tls=1&server_name=kvdb.internal
rpc://host:7788?tls=1&insecure=1                                   # skip verification, testing only
```

Supplying TLS parameters without writing `tls=1` is an error; connecting to a TLS port in
plaintext fails, and **there is no silent downgrade**.

### Protocol and codec

By default a **Redis-like (RESP2)** wire format is used, so packet captures are readable and it
can also be hand-tested with `nc`:

```
$                      nc 127.0.0.1 7788
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

Encoding and decoding are abstracted into `rpc/codec.Codec` and can be replaced wholesale; built
in are `resp` (default) and `binary` (uvarint length prefix, skipping text escaping). Both ends
must be equipped with the same one:

```go
rpc.ServerConfig{Codec: rpc.CodecBinary}
rpc.Config{Codec:      rpc.CodecBinary}          // or add ?codec=binary to the URI
```

When a connection is established the two sides exchange codec names, and a mismatch is an
immediate error (rather than the two sides waiting for each other until timeout).

### Other parameters

| Parameter | Notes |
|---|---|
| `pool=N` | Client connection pool size (default 8). A single connection runs only one command at a time, so concurrency relies on multiple connections |
| `codec=resp\|binary` | Message encoding/decoding |
| `max-conns` (server side) | Maximum number of concurrent connections |

## Compatibility

| Dependency | Verified versions |
|---|---|
| MySQL | 5.6.51, 8.0.46 |
| PostgreSQL | 9.6, 10, 12, 16 |
| SQL Server | 2022 (mcr.microsoft.com/mssql/server) |
| SQLite | modernc.org/sqlite (pure Go, requires 3.35+ for `RETURNING` support) |
| Redis | 7.x |
| SSDB | Native protocol, supports `server.auth` |

SQLite uses a pure Go driver (modernc); throughput is on par with the CGO driver, with no CGO
dependency introduced.

## Performance

> For the complete measured data, the cost model of each backend, and selection advice, see **[PERFORMANCE.md](PERFORMANCE.md)**.

Basis: **run a saturated concurrent load within a fixed time window and count the actual
completed volume** (ops/s). Key points:

- **The default mode does not fsync per commit; only `?sync=1` asks for power-loss safety**: on the
  same real ext4, `sync=1` is 29–940× slower than the default mode. **"Whether to use sync=1"
  matters more than "which backend to choose"**, so the mode should be decided first when selecting.
- **The write bottleneck is usually one fsync per transaction**, not the number of statements: in
  this environment a bare fsync is 2–4 µs on tmpfs, ~2.5 ms on ext4(vhdx), and 3.8–5.5 ms on
  drvfs(9p).
- **Batch-write gains scale with commit cost**: 2.2–6.0× in the default mode (bolt reaches 18.8×,
  because it opens a transaction on every write); as high as 39–86× in the `sync=1` mode.
- **Same-key hot spots drop noticeably on SQL**: mysql multi-key 428 vs same-key 119 ops/s (3.6×),
  pg 2.2k vs 647 (3.4×). Spread counters across keys or switch to batch writes.
- **Server backends**: redis has the largest batch-write gain (39×), while ssdb has only 2.6×
  because it has no transactions.
- **Under mixed read/write, reads are squeezed out by writes**: `mem`/`jsonl` share a global lock,
  so at high write frequency reads are left with only 5–9% of pure reads; server backends, by
  contrast, do not block reads and writes against each other (each retaining ~44–63%). See the
  mixed read/write columns of the main table below.
- Two independent levers: **switching to batch writes**, and relaxing deployment-side durability
  (MySQL `innodb_flush_log_at_trx_commit=2`, PostgreSQL `synchronous_commit=off`, which shortens
  the crash-recovery window and needs your own confirmation that it is acceptable).
- Re-measure: `cd bench && KVDB_BENCH_URI=<uri> go test -run '^$' -bench Throughput -benchtime 3s`

### Main table: ext4 + default mode (the one to read when choosing)

Real block device (ext4/WSL vhdx), default mode of each backend (no fsync per commit). 8
goroutines, time window `-benchtime 2s`, counting the actual completed volume. `MGet items` /
`Batch items` are items/s converted to the entry level; in `Incr multi-key` each writer uses an
independent key, while in `Incr same-key` all of them hit the same counter.

`Mixed read` / `Mixed write` come from the mixed-load benchmark (of 8 goroutines, half
continuously read a 64-key hot set and half write independent keys); `Read retention` = mixed
read ÷ that backend's pure-read Get.

| Backend | Set | Get | Incr multi-key | Incr same-key | QPush | MGet items | Batch items | Mixed read | Mixed write | Read retention |
|---|---|---|---|---|---|---|---|---|---|---
| jsonl | ~353.6k | ~10.0M | ~190.1k | ~242.2k | ~639.3k | ~14.1M | ~454.7k | ~474k | ~105k | 5% |
| bolt | ~25.2k | ~591.1k | ~22.9k | ~26.9k | ~20.9k | ~2.6M | ~473.8k | ~170k | ~13.5k | 27% |
| leveldb | ~164.6k | ~1.0M | ~111.8k | ~122.4k | ~111.8k | ~1.0M | ~354.2k | ~98.7k | ~52.4k | 11% |
| badger | ~81.8k | ~274.4k | ~63.5k | ~34.0k | ~64.7k | ~608.2k | ~493.8k | ~76.4k | ~49.7k | 28% |
| sqlite | ~12.6k | ~62.9k | ~5.0k | ~5.8k | ~5.5k | ~450.9k | ~37.6k | ~40.1k | ~5.8k | 64% |

**Reference frame (different media/deployments, not directly comparable to the main table)**

| Backend | Medium | Set | Get | Incr multi-key | Incr same-key | QPush | MGet items | Batch items | Mixed read | Mixed write | Read retention |
|---|---|---|---|---|---|---|---|---|---|---|---
| mem | No IO | ~775k | ~8.4M | ~825k | ~3.0M | ~2.85M | ~14.8M | ~1.48M | ~783k | ~251k | 9% |
| redis | Container ext4 | ~12.7k | ~12.4k | ~14.9k | ~13.4k | ~13.2k | ~256.4k | ~497.2k | ~7.1k | ~7.0k | 57% |
| ssdb | Container ext4 | ~8.0k | ~7.6k | ~7.7k | ~7.9k | ~8.2k | ~137.4k | ~20.6k | ~3.7k | ~3.5k | 48% |
| mysql 8.0 | Container ext4 | ~718 | ~5.3k | ~428 | ~119 | ~400 | ~95.6k | ~4.6k | ~2.9k | ~453 | 55% |
| pg 16 | Container ext4 | ~2.4k | ~9.9k | ~2.2k | ~647 | ~1.4k | ~185.7k | ~8.8k | ~5.3k | ~1.4k | 54% |

### Other media and mode matrix

The main table covers only "ext4 + default mode"; the remaining combinations are as follows
(changing the medium or the mode changes the comparison baseline):

**`?sync=1` (fsync per commit) @ ext4**: only the write path is affected; reads are the same as in
the main table.

| Backend | Set | QPush | Slower than default mode |
|---|---|---|---|
| jsonl | ~377 | ~410 | ~940× |
| bolt | ~449 | ~464 | ~56× |
| leveldb | ~1.3k | ~1.3k | ~128× |
| badger | ~748 | ~736 | ~109× |
| sqlite | ~440 | — | ~29× |

**Conclusions**

- **"Whether to use `sync=1`" matters more than "which backend to choose"**: once the default mode
  takes fsync off the write hot path, embedded write throughput rises by 1–2 orders of magnitude;
  when power-loss durability is required, `sync=1` throughput is only ~377–1.3k ops/s, and at that
  point `Batch` should be preferred to amortize commits.
- **The read path is barely affected by the medium** (leveldb Get is ~1.0M in every mode, badger
  ~272–298k), while the write path differs by 2–3 orders of magnitude across media.
- **Server backends have lower read throughput than embedded ones** (redis Get ~12.4k vs bolt
  ~590k); the bottleneck is the network round trip. But they are naturally "durable by default",
  with no need to choose between speed and safety.
- **Same-key hot spots degrade most visibly on SQL**: mysql multi-key 428 → same-key 119 (3.6×),
  pg 2.2k → 647 (3.4×). Spread counters across keys or switch to batch writes.
- **Mixed read/write**: server backends do not block reads and writes against each other (each
  retaining ~44–63%); `mem`/`jsonl` share a global lock, so at high write frequency reads are left
  with only 5–9% of pure reads. `badger`'s read retention is determined by its "write commit
  window" — with `?sync=1` one commit takes 1.3–3 ms and read retention drops to only 0.2–0.4%,
  a phenomenon the default mode has already eliminated.
- Two independent levers: **switching to batch writes**, and relaxing deployment-side durability
  (MySQL `innodb_flush_log_at_trx_commit=2`, PostgreSQL `synchronous_commit=off`, which shortens
  the crash-recovery window and needs your own confirmation that it is acceptable).
- Re-measure: `cd bench && KVDB_BENCH_URI=<uri> go test -run '^$' -bench Throughput -benchtime 3s`

> For the cost model of each backend (what each operation actually does) and a longer analysis,
> see **[PERFORMANCE.md](PERFORMANCE.md)**.



## Testing

**Each module is independent**, and `go test ./...` covers only the current module; to run all
modules:

```bash
#                      Run all modules (directories starting with . are local scaffolding, not part of the repository)
for                    m in $(find . -name go.mod -not -path './.*'); do
    (cd "$(dirname "$m")" && go test ./...) || exit 1
done
```

Running a single module (cross-module dependencies are resolved by each go.mod's replace, with no
workspace file needed):

```bash
cd                     mem    && go test ./...     # local backends + in-process stand-ins (miniredis, fake SSDB)
cd                     sqlite && go test ./...
cd                     rpc    && go test ./...     # RPC: contract cases across RPC, three auth modes, TLS, codec, several real backends
```

Real-backend cases are skipped by default and enabled after setting the corresponding environment
variables (adjust ports as needed to avoid clashing with local services):

```bash
#                      Test containers bind only to the loopback address, so weak-password test services are not exposed to the LAN.
docker                 run -d --name kvdb-mysql -p 127.0.0.1:3306:3306 -e MYSQL_ROOT_PASSWORD=pw -e MYSQL_DATABASE=kvdb_test mysql:8.0
docker                 run -d --name kvdb-pg    -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=kvdb_test postgres:16-alpine
docker                 run -d --name kvdb-redis -p 127.0.0.1:6379:6379 redis:7-alpine

KVDB_TEST_MYSQL_DSN='root:pw@tcp(127.0.0.1:3306)/kvdb_test' \
KVDB_TEST_PG_DSN='postgres://postgres:pw@127.0.0.1:5432/kvdb_test?sslmode=disable' \
KVDB_TEST_REDIS_ADDR=127.0.0.1:6379 \
  go test ./...          # run inside the directory of the corresponding backend module
```

| Environment variable | Purpose |
|---|---|
| `KVDB_TEST_MYSQL_DSN` | MySQL DSN (a dedicated test database; the SDK's tables are DROPped before the cases) |
| `KVDB_TEST_PG_DSN` | PostgreSQL DSN (same as above) |
| `KVDB_TEST_REDIS_ADDR` | Redis address (the cases use a separate DB and clear it) |
| `KVDB_TEST_SSDB_ADDR` | SSDB address (flushdb before the cases) |
| `KVDB_TEST_SSDB_AUTH_ADDR` / `KVDB_TEST_SSDB_AUTH_PASS` | An SSDB instance with `server.auth` enabled |

All backends share the contract cases in `kvdbtest` in the root module (KV / Queue / ZSet / Batch /
expired-write semantics / return-value ownership / namespaces, including concurrent atomicity);
SSDB additionally uses an in-process fake server to cross-validate the wire-protocol encoding.

The root module's registry/codec cases use a **test stub** (the `stub://` registered in
`stub_test.go`) to verify the "empty registry by default + explicit opt-in" semantics, so the root
module itself has zero third-party dependencies; the "import means self-registration" of real
backends is covered by each backend module's own cases (such as `mem/registry_test.go`).

## Repository layout

**Each subdirectory is an independent Go module** (each has its own go.mod; cross-module
dependencies use require + replace pointing at the sibling directory, so that every module can be
built / tested / tidied on its own):

```
core/                  Contract: KvProvider / Queue- / ZSet- / BatchProvider / Closer / FullProvider
provider.go            Open and contract re-exports
db.go                  DB interface and the default adapter
batch.go               Batch collector and DB.Batch dispatch
bytes.go               Enc / Dec / D / DMust byte encoding and decoding
registry.go            Register / MustRegister / Schemes
kvdbtest/              Contract cases shared across backends (public package, referenced by backend module tests)
all/                   Aggregating registration package: import _ to pull in all built-in backends
mem/                   jsonl/ bolt/ leveldb/ badger/ sqlite/ mysql/ pg/ redis/ ssdb/   Backend implementations (each an independent module)
rpc/                   RPC client and server (independent module, zero third-party dependencies)
rpc/codec/             Message codec abstraction + RESP (default) / binary implementations
rpcserver/             Runnable RPC server command (independent module, imports .../all)
sqlstore/              The database/sql implementation shared by MySQL / SQLite / PG (dialect-parameterized)
bench/                 Benchmarks (independent module, imports .../all)
example/               Runnable demos
```

## License

No open-source license file has been added yet; please confirm authorization with the author
before use.
