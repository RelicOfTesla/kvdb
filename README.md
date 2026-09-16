# kvdb

**English** | [中文](README_CN.md)

A Go persistence adapter SDK. It exposes a unified **KV + Queue + ZSet** interface over a
pluggable persistence backend (in-memory / jsonl / SQL / Redis / SSDB / BoltDB), with built-in
batched writes and byte codec helpers.

Semantics follow the **SSDB/Redis family**: command *names* come from SSDB
(`qpush`/`qpop`/`zset`…), while KV behavior follows Redis wherever the two agree. A few
points differ deliberately from Redis (notably `Set` preserving TTL, and `Scan` being a
deterministic range query rather than Redis's cursor-based `SCAN`); the exact per-command
rules live in the `core` package docs, and [`core/provider.go`](core/provider.go) is the
single source of truth for the contract.

> This tool is AI-generated. Use at your own discretion.

```go
db, _ := kvdb.Open(ctx, "sqlite://./data.db")
defer db.Close()

db.Set(ctx, "user:1", []byte("alice"))
n, err := db.Incr(ctx, "visits", 1)
```

## Features

- **11 built-in backends, one API**: `mem` / `jsonl` / `bolt` / `leveldb` / `badger` / `sqlite` /
  `mysql` / `pg` / `mssql` / `redis` / `ssdb`
- **Optional, probed capabilities**: KV is mandatory; Queue / ZSet / Batch / Close are optional and
  report `ErrUnsupported` when absent, probeable via `Capabilities()`
- **Empty registry by default**: `import _` the backend you use; the root package pulls in no driver
- **Interface-based returns**: `kvdb.Open` returns `DB`, so business code can depend narrowly on
  sub-interfaces such as `KvProvider` (eases mocking)
- **Batched writes** mapped onto each backend's native mechanism (transaction / MULTI-EXEC / pipeline / one flush)
- **Bytes ↔ generic helpers**: `Enc` / `Dec` / `D` / `DMust`; scalar encoding interoperates with `Incr`
- **Local becomes remote**: any backend as an RPC server; the client is backend-unaware, with
  authentication (plaintext / challenge) and optional TLS, and a replaceable codec
- **Optional Go 1.27.1+ typed shell**: `kvdb.Typed(store)` adds generic read/write methods
- **Pure Go, no CGO**

| Module | Min Go |
|---|---|
| `kvdb` (root, incl. `core`/`kvdbtest`), `mem`, `jsonl`, `sqlstore`, `mssql` | 1.18 |
| `ssdb`, `leveldb`, `rpc` | 1.19 |
| `badger` | 1.24 (Badger v4 declares `go 1.24.0`) |
| `bolt`, `sqlite`, `mysql`, `pg`, `redis`, `all`, `example` | 1.25 (driver / transitive deps) |

Taken as the largest `go` directive in each module's dependency graph
(`go list -m -f '{{.GoVersion}}' all`). The root package, `mem`, `jsonl` and `rpc` need only the
standard library: a project importing just `kvdb/mem` builds with an empty dependency closure.

## Installation

```bash
go get github.com/RelicOfTesla/kvdb        # root package
go get github.com/RelicOfTesla/kvdb/mem    # each backend is fetched as needed
```

The root package has no driver dependencies — import the backend you use (see Quick start).

## Quick start

```go
import (
    "github.com/RelicOfTesla/kvdb"
    _ "github.com/RelicOfTesla/kvdb/sqlite"   // wire in one backend, or _ .../all for everything
)

db, _ := kvdb.Open(ctx, "sqlite://./data.db")
defer db.Close()

db.Set(ctx, "user:1", []byte("alice"))
v, ok, _ := db.Get(ctx, "user:1")
db.SetEx(ctx, "session", []byte("token"), 1800)
n, _ := db.Incr(ctx, "visits", 1)
pairs, _ := db.Scan(ctx, "user:", "user;", 100)   // closed interval

db.QPush(ctx, "jobs", []byte("job-1"))            // Queue / ZSet are optional capabilities
job, ok, _ := db.QPop(ctx, "jobs")
db.ZSet(ctx, "rank", "alice", 90)
top, _ := db.ZRange(ctx, "rank", 0, -1)
```

A complete demo is in [`example/main.go`](example/main.go).

## Built-in backends

One API over 11 backends; the URI column doubles as the scheme reference. For the SQL family,
`?table_prefix=` prefixes the tables and indexes; `redis`/`ssdb` take `?key_prefix=` (a logical
prefix, stripped on read-back, with `Scan` confined to it). Unconfigured means the current layout.

| Backend | Package | URI and notes |
|---|---|---|
| In-memory | `mem` | `mem://` — no persistence, for tests/caching |
| JSONL log | `jsonl` | `jsonl://./d.jsonl` — append-only WAL replayed on open, `Compact()`; flush 500ms + fsync 1s by default, `?sync=1` per-op, `?each_flush=1` per-op flush |
| BoltDB | `bolt` | `bolt://./d.bolt` — single-file B+tree; no per-commit fsync, periodic flush (`?sync_interval=1s`), `?sync=1` per-commit |
| LevelDB | `leveldb` | `leveldb://./d.dir` — LSM; `?sync=1` per-commit, `?cache`/`?wb` in MiB |
| Badger | `badger` | `badger://./d.dir` — LSM with MVCC; `?sync=1` per-commit, `?cache`/`?memtable` in MiB |
| SQLite | `sqlite` | `sqlite://./d.db` — pure Go driver (no CGO); NORMAL by default, `?sync=1` = FULL |
| MySQL | `mysql` | `mysql://user:pass@h:3306/db?parseTime=true` |
| PostgreSQL | `pg` | `pg://user:pass@h:5432/db?sslmode=disable` |
| SQL Server | `mssql` | `mssql://sa:pass@h:1433?database=db&encrypt=disable` |
| Redis | `redis` | `redis://:pass@h:6379/0` — native String/List/Sorted Set |
| SSDB | `ssdb` | `ssdb://[user:pass@]h:8888` — native text protocol client with pooling |
| RPC | `rpc` | `rpc://h:7788?auth=challenge&password=…` — client is backend-unaware |

All four SQL backends share `sqlstore`; import path is `github.com/RelicOfTesla/kvdb/<package>`
(plus the aggregate `.../all`).

## Capability model

`kvdb.Open` returns the interface `DB`, embedding the capability interfaces (`KvProvider` is
mandatory; `QueueProvider` / `ZSetProvider` / `Batcher` / `Closer` are optional) plus
`Capabilities() core.Caps`. An absent capability returns `ErrUnsupported`, so probe first:

```go
c := db.Capabilities()   // c.Queue / c.ZSet / c.Batch / c.BatchComposed
```

Depend only on the interfaces you use — a mock then implements just those methods. `BatchComposed`
says whether later ops in one batch see earlier ones (`false` for LevelDB Batch / Redis MULTI-EXEC
/ SSDB pipeline); probe it, or split interdependent ops into separate batches.

## Batched writes

One commit for the whole batch, cutting round trips and persistence overhead:

```go
err := db.Batch(ctx, func(b *kvdb.Batch) error {
    b.Set("k", value)
    b.QPush("jobs", payload)
    b.ZIncr("rank", "alice", 1)
    return nil   // only nil commits; an error during collection voids the whole batch
})
```

Backends implement just `ApplyBatch(ctx, ops)` and map it onto their native mechanism (one SQL
transaction, one Redis MULTI/EXEC, one SSDB pipeline, one jsonl flush, one lock acquisition in
`mem`). Only **unconditional writes** are allowed — `Set/SetEx/Del/Expire/QPush/QPushFront/ZSet/
ZDel/ZIncr`; `Incr`/`QPop` depend on current state and must be called separately. Atomicity:
transactional and in-process backends void the batch on failure, while **SSDB has no
transactions**, so a pipeline failure may take partial effect.

## Bytes ↔ T helpers

```go
db.Set(ctx, "n", kvdb.Enc(int64(42)))     // scalar -> decimal text (Incr-compatible)
db.Set(ctx, "u", kvdb.Enc(User{ID: 7}))   // struct -> JSON

// D merges the (val, ok, err) of Get/QPop; both pass through as-is (missing => ok=false, err=nil)
u, ok, err := kvdb.D[User](db.Get(ctx, "u"))
n := kvdb.DMust[int64](db.Get(ctx, "n"))  // panic variant: inspects err only (missing => zero value)
raw, err := kvdb.Dec[User](b)
```

| Type | Encoding |
|---|---|
| integers, float, string, bool | text (interoperates with `Incr`) |
| `[]byte` | identity (no JSON/base64) |
| structs, slices, maps, pointers… | `Marshal` (JSON by default) |

`kvdb.Marshal` / `kvdb.Unmarshal` are swappable package variables (msgpack / protobuf / gob);
the scalar path is unaffected. `Enc` panics on an encoding failure — call `Marshal` to handle it.

### Go 1.27.1+: the `TypedStore` shell

`TypedStore` wraps `StoreProvider` (`KvProvider + QueueProvider + ZSetProvider`) only — it needs
neither `Batch` nor `Close`:

```go
tdb := kvdb.Typed(db)                      // db only needs to satisfy kvdb.StoreProvider
u, ok, err := tdb.Get[User](ctx, "user:1")  // = kvdb.D[User](db.Get(ctx, "user:1"))
err = tdb.Set(ctx, "user:1", u)             // = db.Set(ctx, "user:1", kvdb.Enc(u))
n, ok, err := tdb.Get[int64](ctx, "visits") // scalars use text encoding, interoperating with Incr
ms, err := tdb.MGet[User](ctx, "user:1", "user:2")
```

Writes are `Set[T]` / `SetEx[T]` / `SetExAt[T]` / `QPush[T]` / `QPushFront[T]` (encoded with
`Enc[T]`); reads are `Get[T]` / `MGet[T]` / `QPop[T]` / `QPopBack[T]` / `QFront[T]` / `QBack[T]` /
`QRange[T]`, decoded with `Dec[T]`/`D[T]`. Every `[]byte`-valued contract method has a `T`
counterpart; methods that carry no value (`Del`/`Exists`/`Incr`/`Scan`/`TTL`/`QSize`/ZSet…) stay on
the embedded `StoreProvider` and take no type parameter. Read methods take `D`'s
signature — `(T, bool, error)` — so a missing entry is `ok=false` with `err=nil`. With
`T = []byte` it is exactly equivalent to calling the backend directly. `tdb.BatchT(ctx, func(b kvdb.TypedBatch) error {...})` encodes at collection
time and **one batch may mix types**; `Del`/`Expire`/`ZSet` remain available via the embedded
`*Batch`. The generic methods shadow same-named ones, so `TypedStore` does **not** satisfy
`StoreProvider` (use `tdb.StoreProvider`). Gated by `//go:build go1.27`.

## Extending: custom backends

Implement `core.KvProvider` (plus any optional capability interfaces) and register it with
`kvdb.MustRegister(scheme, factory)`; it is then reachable through `kvdb.Open`. See
[`example/`](example) and the `core` package docs for the interfaces.

## Local to remote (RPC client/server)

Put any backend on the server side; the client reaches it over the network through **the same
interfaces**. Useful for cross-process/machine isolation, or to share an embedded backend.

```go
srv, err := rpc.NewServer(ctx, rpc.ServerConfig{
    Addr: ":7788", Backend: "jsonl://./data.jsonl",   // bolt/sqlite/mysql/ssdb…
    Auth: rpc.AuthChallenge, Password: "s3cret",
})
go srv.Serve(ctx)

db, err := kvdb.Open(ctx, "rpc://127.0.0.1:7788?auth=challenge&password=s3cret")
db.Set(ctx, "k", []byte("v"))   // KV / Queue / ZSet / Batch all available
```

Ready-made server: `go run ./example/rpcdemo -addr :7788 -backend jsonl://./data.jsonl -auth challenge -password s3cret`

The client is **backend-unaware** (`rpc` implements `core.FullProvider`, and has zero third-party
dependencies), and `Capabilities()` reports the *server* backend's set. Errors keep their identity
over the wire, so `errors.Is` works; one batch is one round trip, and the client's `Close()` closes
only its own connection.

Authentication is the **c/s protocol's own** (backend credentials are the server's business):
`auth=none` (default) / `auth=plain` (password on the wire) / `auth=challenge` (nonce +
`HMAC-SHA256`; the password never goes on the wire). TLS is standard `crypto/tls`, enabled by the
server config on the same port (`?tls=1&ca=./ca.pem`, plus `cert=`/`key=` for mutual TLS). Codecs
are `resp` (default) / `binary` / `textproto`, selected via `?codec=`; both ends must match.
Misconfiguration never degrades silently — it errors.

## Compatibility

| Dependency | Verified versions |
|---|---|
| MySQL | 5.6.51, 8.0.46 |
| PostgreSQL | 9.6, 10, 12, 16 |
| SQL Server | 2022 (mcr.microsoft.com/mssql/server) |
| SQLite | modernc.org/sqlite (pure Go, requires 3.35+ for `RETURNING` support) |
| Redis | 7.x |
| SSDB | Native protocol, supports `server.auth` |

## Performance

Real block device (ext4), default mode, 8 goroutines, 2s window counting completed volume
(ops/s; `MGet items`/`Batch items` are items/s at entry level). Full data, other media and the
`?sync=1` matrix: **[PERFORMANCE.md](PERFORMANCE.md)**.

| Backend | Set | Get | Incr multi-key | Incr same-key | QPush | MGet items | Batch items | Mixed read | Mixed write | Read retention |
|---|---|---|---|---|---|---|---|---|---|---|
| jsonl | ~353.6k | ~10.0M | ~190.1k | ~242.2k | ~639.3k | ~14.1M | ~454.7k | ~474k | ~105k | 5% |
| bolt | ~25.2k | ~591.1k | ~22.9k | ~26.9k | ~20.9k | ~2.6M | ~473.8k | ~170k | ~13.5k | 27% |
| leveldb | ~164.6k | ~1.0M | ~111.8k | ~122.4k | ~111.8k | ~1.0M | ~354.2k | ~98.7k | ~52.4k | 11% |
| badger | ~81.8k | ~274.4k | ~63.5k | ~34.0k | ~64.7k | ~608.2k | ~493.8k | ~76.4k | ~49.7k | 28% |
| sqlite | ~12.6k | ~62.9k | ~5.0k | ~5.8k | ~5.5k | ~450.9k | ~37.6k | ~40.1k | ~5.8k | 64% |

Two things follow from this table: **decide the durability mode before the backend** (the default
mode does not fsync per commit; `?sync=1` costs 29–940× on the same medium), and **batch writes**
are the main write-side lever (2.2–6.0× by default, 39–86× with `?sync=1`).

## Testing

Each module is independent, so `go test ./...` covers only the current one:

```bash
for m in $(find . -name go.mod -not -path './.*'); do (cd "$(dirname "$m")" && go test ./...) || exit 1; done
```

Container-backed backends skip unless their DSN is set (bound to loopback for safety):
`KVDB_TEST_MYSQL_DSN`, `KVDB_TEST_PG_DSN`, `KVDB_TEST_MSSQL_DSN`, `KVDB_TEST_REDIS_ADDR`,
`KVDB_TEST_SSDB_ADDR`, `KVDB_TEST_SSDB_AUTH_ADDR`/`_PASS`.

All backends share the contract cases in `kvdbtest` (one file per topic). The suite asserts
**identical behaviour** except where `Capabilities()` declares a difference, so an undeclared
divergence fails; SSDB additionally cross-validates the wire encoding against an in-process fake
server.

## Repository layout

**Each subdirectory is an independent Go module** (own `go.mod`; cross-module deps use
`require` + `replace` to the sibling path, so each builds/tests/tidies alone):

```
core/         Contract: interfaces, sentinel errors, capability declaration, shared helpers
kvdbtest/     Contract cases shared across backends, one file per topic
mem/ jsonl/ bolt/ leveldb/ badger/ sqlite/ mysql/ pg/ mssql/ redis/ ssdb/   backend modules
rpc/          RPC client and server (+ rpc/codec: RESP / binary / textproto)
sqlstore/     shared database/sql implementation behind sqlite/mysql/pg/mssql
all/          aggregating registration package (import _ to wire in every backend)
example/      rpcdemo (RPC server), cmd/cli, cmd/migration, bench
```

Root-package files: `provider.go` (Open + re-exports), `db.go` (DB interface and adapter),
`batch.go`, `bytes.go`, `registry.go`.

## License

No open-source license file has been added yet; please confirm authorization with the author
before use.
