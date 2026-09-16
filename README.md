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

| Backend | Package | KV/Queue/ZSet | URI and notes |
|---|---|---|---|
| In-memory | `mem` | ✅✅✅ | `mem://` — no persistence, for tests/caching |
| JSONL log | `jsonl` | ✅✅✅ | `jsonl://./d.jsonl` — append-only WAL replayed on open, `Compact()`; flush 500ms + fsync 1s by default, `?sync=1` per-op, `?each_flush=1` per-op flush |
| BoltDB | `bolt` | ✅✅✅ | `bolt://./d.bolt` — single-file B+tree; no per-commit fsync, periodic flush (`?sync_interval=1s`), `?sync=1` per-commit |
| LevelDB | `leveldb` | ✅✅✅ | `leveldb://./d.dir` — LSM; `?sync=1` per-commit, `?cache`/`?wb` in MiB |
| Badger | `badger` | ✅✅✅ | `badger://./d.dir` — LSM with MVCC; `?sync=1` per-commit, `?cache`/`?memtable` in MiB |
| SQLite | `sqlite` | ✅✅✅ | `sqlite://./d.db` — pure Go driver (no CGO); NORMAL by default, `?sync=1` = FULL |
| MySQL | `mysql` | ✅✅✅ | `mysql://user:pass@h:3306/db?parseTime=true` |
| PostgreSQL | `pg` | ✅✅✅ | `pg://user:pass@h:5432/db?sslmode=disable` |
| SQL Server | `mssql` | ✅✅✅ | `mssql://sa:pass@h:1433?database=db&encrypt=disable` |
| Redis | `redis` | ✅✅✅ | `redis://:pass@h:6379/0` — native String/List/Sorted Set |
| SSDB | `ssdb` | ✅✅✅ | `ssdb://[user:pass@]h:8888` — native text protocol client with pooling |
| RPC | `rpc` | ✅✅✅ | `rpc://h:7788?auth=challenge&password=…` — client is backend-unaware |

All four SQL backends share `sqlstore`; import path is `github.com/RelicOfTesla/kvdb/<package>`
(plus the aggregate `.../all`).

## Capability model

`kvdb.Open` / `kvdb.Wrap` return the interface `DB`, which embeds the capability interfaces
(`KvProvider` mandatory, plus `QueueProvider` / `ZSetProvider` / `Batcher` / `Closer`) and
`Capabilities() core.Caps`. The concrete adapter is unexported.

An unimplemented capability returns `ErrUnsupported`, so probe first:

```go
c := db.Capabilities()     // c.Queue / c.ZSet / c.Batch / c.BatchComposed
```

`Caps` is a struct, so adding a capability never changes signatures. `BatchComposed` is a real,
non-mandatory difference: backends applying ops one by one inside one transaction or lock
(`mem`/`jsonl`/`bolt`/`badger`/`sqlite`/`mysql`/`pg`) are `true`, while LevelDB's Batch, Redis's
MULTI/EXEC and SSDB's pipeline cannot read uncommitted content, so they are `false` — probe it,
or split interdependent operations into separate batches.

Depend only on the interfaces you use (a mock then implements just those methods):

```go
func touch(ctx context.Context, store kvdb.KvProvider, key string) (int64, error) {
    return store.Incr(ctx, key, 1)
}
```

`kvdb.Unwrap(db)` returns the backend behind an adapter (nil if it is not an adapter).

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

Writes are `Set[T]` / `SetEx[T]` / `QPush[T]` / `QPushFront[T]` (encoded with `Enc[T]`); reads are
`Get[T]` / `MGet[T]` / `QPop[T]` / `QPopBack[T]` / `QFront[T]` / `QBack[T]` plus the non-generic
`QRange` (queue elements are raw bytes), decoded with `Dec[T]`/`D[T]`. Read methods **keep `D`'s
signature** — `(T, bool, error)` — so a missing entry is `ok=false` with `err=nil` rather than a
folded `ErrNotFound`; judge the trade-off where you call it. With `T = []byte` it is exactly equivalent to calling the
backend directly. `tdb.BatchT(ctx, func(b kvdb.TypedBatch) error {...})` encodes at collection
time and **one batch may mix types**; `Del`/`Expire`/`ZSet` remain available via the embedded
`*Batch`. The generic methods shadow same-named ones, so `TypedStore` does **not** satisfy
`StoreProvider` (use `tdb.StoreProvider`). Gated by `//go:build go1.27`.

## Extending: custom backends

Implement `core.KvProvider` (mandatory) plus any optional capability interfaces, register it, and
`kvdb.Open` can use it:

```go
type myStore struct{ /* ... */ }

func (m *myStore) Set(ctx context.Context, key string, value []byte) error { /* ... */ }
// ... remaining KV methods; optionally core.QueueProvider / core.ZSetProvider / core.BatchProvider

func init() {
    kvdb.MustRegister("mybase", func(ctx context.Context, u *url.URL) (core.KvProvider, error) {
        return &myStore{}, nil
    })
}
// consumer: import _ "your/module/mybase", then kvdb.Open(ctx, "mybase://...")
```

`kvdb.Register` errors on a duplicate or empty scheme; `kvdb.Schemes()` lists what is registered;
`FullProvider` declares the whole "KV + Queue + ZSet + Batch + Close" set at once.

## Local to remote (RPC client/server)

Put any backend on the server side and the client reaches it over the network through **the same
interfaces** — for cross-process/machine isolation, or to turn an embedded backend into a shared
service.

```go
// Server: pick a backend (imports that backend package, or .../all)
srv, err := rpc.NewServer(ctx, rpc.ServerConfig{
    Addr: ":7788", Backend: "jsonl://./data.jsonl",   // change to bolt/sqlite/mysql/ssdb…
    Auth: rpc.AuthChallenge, Password: "s3cret",
})
go srv.Serve(ctx)

// Client: imports no backend and needs no knowledge of the peer
db, err := kvdb.Open(ctx, "rpc://127.0.0.1:7788?auth=challenge&password=s3cret")
defer db.Close()
db.Set(ctx, "k", []byte("v"))           // KV / Queue / ZSet / Batch all available
```

Or run the ready-made server: `go run ./example/rpcdemo -addr :7788 -backend jsonl://./data.jsonl -auth challenge -password s3cret`

- **Client is backend-unaware**: `rpc` implements `core.FullProvider`, so client code is identical
  whatever the peer runs; switching backends changes one server-side parameter. The `rpc` module
  (and the client in particular) has **zero third-party dependencies**.
- **Capabilities and errors pass through faithfully**: `Capabilities()` reports the *server*
  backend's set (never "stronger" for having an RPC layer), and sentinel errors keep their identity
  so `errors.Is` still works (see the `core` package docs for the list).
- **One batch = one round trip**; in-batch visibility is the backend's own semantics. The client's
  `Close()` closes only its connection, not the server's backend.

### Authentication, TLS, codec

Authentication is the **c/s protocol's own** (backend credentials such as a mysql password are the
server's business, invisible to the client):

| Mode | URI | Notes |
|---|---|---|
| none (default) | `auth=none` | Bare on localhost/intranet |
| plaintext | `auth=plain&password=…` | Password on the wire; off by default, use only behind TLS |
| challenge | `auth=challenge&password=…` | Server nonce + `HMAC-SHA256(password, nonce)`; the password never goes on the wire |

TLS is standard `crypto/tls`, switched on the same port by server config (certificate ⇒ TLS):

```bash
rpc://h:7788?tls=1&ca=./ca.pem                            # verify the server
rpc://h:7788?tls=1&ca=./ca.pem&cert=./c.pem&key=./c.key   # mutual TLS
rpc://h:7788?tls=1&insecure=1                             # testing only
```

Wrong configuration fails loudly: a password without `auth=`, TLS params without `tls=1`, or
plaintext against a TLS port are all immediate errors — **there is no silent downgrade**.

Codecs live behind `rpc/codec.Codec`: `resp` (default, RESP2-like and hand-testable with `nc`),
`binary` (uvarint length prefix), `textproto` (SSDB-style records, not SSDB-interoperable). Both
ends must match; a mismatch fails at handshake. Other parameters: `pool=N` (client connection
pool, default 8), `max-conns` (server side).

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

Full measured data, cost model and selection advice: **[PERFORMANCE.md](PERFORMANCE.md)**.
Method: saturate a fixed time window with concurrent load and count what actually completes
(ops/s). The five points that drive most decisions:

- **Decide the durability mode before the backend**: the default mode does not fsync per commit;
  on real ext4, `?sync=1` is **29–940× slower**. Same medium, same backend — this lever dominates
  backend choice. (How to pick: `?sync=1` only when a power loss must not lose acknowledged writes.)
- **The write bottleneck is normally one fsync per commit**, not the statement count, so
  **batch writes scale with commit cost**: 2.2–6.0× in the default mode (bolt 18.8×, as it opens a
  transaction per write) and 39–86× with `?sync=1`.
- **Same-key hot spots hurt the SQL family** (mysql 428 multi-key vs 119 same-key; pg 2.2k vs 647):
  spread counters across keys, or batch.
- **Under mixed read/write, `mem`/`jsonl` reads are squeezed to 5–9% of pure reads** (shared global
  lock); server backends do not block the two directions against each other (~44–63% each).
- Re-measure: `cd example/bench && KVDB_BENCH_URI=<uri> go test -run '^$' -bench Throughput -benchtime 3s`

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
