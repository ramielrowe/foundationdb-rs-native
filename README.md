# foundationdb-rs-native

A pure-Rust, native [FoundationDB](https://www.foundationdb.org/) client. It speaks
FoundationDB's wire protocol directly and does not link `libfdb_c`.

**Status: pre-release (0.x, unpublished).** The client does what a FoundationDB binding does
for key-value work, against 7.4 and 8.0 clusters, plain and TLS: reads (keys, key selectors,
ranges and range streams), writes, clears, atomic operations, versionstamps, watches, options
and timeouts, with read-your-writes, FDB's conflict detection and its retry semantics, ported
from the C++ client. The tuple and subspace layers are in `fdb-native-tuple`. Not there yet:
see [Not supported yet](#not-supported-yet). The API may still change before 1.0; see
[CHANGELOG.md](CHANGELOG.md).

## Supported servers

FoundationDB **7.4** and **8.0** (tested with the `foundationdb/foundationdb:7.4.10` and
`8.0.1` images). FDB has no protocol negotiation: the client announces the protocol version
of the cluster it connects to and speaks that release's message schemas, so other releases
(7.3 and earlier) are not supported. How the client behaves toward a cluster (coordinators,
`status json`, throttling, backoff, connections, upgrades), compared with `libfdb_c`, is in
[docs/well-behaved-client.md](docs/well-behaved-client.md). Throughput, latency and CPU against `libfdb_c` 7.4.10, with the method
and the findings, are in [docs/benchmarks.md](docs/benchmarks.md).

## Quick start

```toml
[dependencies]
fdb-native-client = { git = "https://github.com/ramielrowe/foundationdb-rs-native" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use fdb_native_client::{Database, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // FDB_CLUSTER_FILE, ./fdb.cluster or /etc/foundationdb/fdb.cluster.
    let db = Database::open(None)?;

    // A retry loop, as fdb_transaction_on_error drives it: conflicts and other retryable
    // errors run the closure again.
    db.run(|tx, _maybe_committed| async move {
        tx.set(b"hello", b"world");
        Ok(())
    })
    .await?;

    let value = db
        .run(|tx, _| async move { tx.get(b"hello").await })
        .await?;
    assert_eq!(value.as_deref(), Some(&b"world"[..]));

    db.shutdown().await;
    Ok(())
}
```

## Usage guide

The API is in `fdb_native_client`'s crate root; its rustdoc has the details and the C++
behaviour each part ports.

**Opening a database.** `Database::open(cluster_file)` reads a cluster file (or finds the
default one) and keeps it up to date when the coordinators change;
`Database::from_connection_string("desc:id@host:port,...")` keeps the connection string in
memory. `Database::open_with(runtime, cluster_file, DatabaseOptions)` takes a runtime and the
client's knobs. Opening does not wait for the cluster: a cluster that cannot be reached shows
as operations that do not complete, so set a timeout (below).

**Transactions.** `db.run(|tx, maybe_committed| async move { ... })` runs the closure in a
transaction, commits it, and retries it on the errors FDB retries (with FDB's backoff, retry
limit and timeout). `db.create_transaction()` gives a transaction to drive by hand:
`commit()`, then `on_error(e)` on failure, which waits the backoff and resets it for another
attempt or returns the error. Transactions are cheap handles; every method takes `&self`.

* Reads: `get`, `get_key(&KeySelector)`, `get_range(&begin, &end, &RangeOptions)` (one
  batch, as `fdb_transaction_get_range`), and the streams `get_ranges` (batches) and
  `get_ranges_keyvalues` (rows), which implement `futures_core::Stream`.
  `tx.snapshot().get(...)` reads without read conflict ranges. Reads see the transaction's
  own writes. A read starts when it is called: its future does not borrow the transaction,
  and may be awaited later or dropped.
* Writes: `set`, `clear`, `clear_range`, `atomic_op(key, operand, MutationType::Add)` (and
  the other atomic operations), `set_versionstamped_key` / `set_versionstamped_value` with
  `get_versionstamp()` for the stamp. Writes return nothing; an invalid one fails the next
  read or the commit, as in `fdb_c`.
* Conflict ranges: `add_read_conflict_range` / `_key`, `add_write_conflict_range` / `_key`.
* Prefixes: `strinc(prefix)` is the end of the range of the keys that start with `prefix`
  (`tx.clear_range(prefix, &strinc(prefix).unwrap())`); `fdb-native-tuple`'s `Subspace` has
  `range_all`.
* Watches: `tx.watch(key)` resolves to a `Watch`, a future that completes when the key's value
  changes after the transaction commits.
* Options: `tx.set_option(TransactionOption::Timeout(..))`, `RetryLimit`, `SizeLimit`,
  `ReadYourWritesDisable`, `AccessSystemKeys`, ...; `db.set_option(DatabaseOption::...)` for
  the defaults of new transactions and the database's own options.
* Versions: `get_read_version`, `set_read_version`, `committed_version`, `approximate_size`.

**Errors.** One type, `fdb_native_client::Error`: an FDB error code (`e.code()`, the
constants in `fdb_native_client::error::codes`) with a description, and the predicates of
`fdb_error_predicate`: `is_retryable`, `is_maybe_committed`, `is_retryable_not_committed`.

**Examples.** [`crates/fdb-native-client/examples/`](crates/fdb-native-client/examples/):

| Example | What it shows |
| --- | --- |
| `basic` | get and set in `Database::run`, read-modify-write, concurrent reads |
| `range_scan` | range reads: one batch, a stream of rows, a stream of batches, key selectors |
| `watch` | watching a key and waiting for another client to change it |
| `versionstamp` | an append-only log keyed by versionstamps with `fdb-native-tuple`'s `Subspace` |
| `counter` | an atomic counter (`MutationType::Add`) incremented concurrently without conflicts |
| `options` | database and transaction options, timeouts, retry limits, the errors they give |
| `tls` | connecting to a TLS cluster (`--features tls`) |

```bash
cargo run -p fdb-native-client --example basic -- /path/to/fdb.cluster
cargo run -p fdb-native-client --features tls --example tls -- fdb.cluster ca.pem cert.pem key.pem
```

## Retries and at-most-once semantics

FoundationDB transactions are serializable, and a commit is all or nothing. But a client cannot
always tell whether a commit happened:

* **Not committed.** An error for which `is_retryable_not_committed()` is true (a
  `not_committed` (1020) conflict, `transaction_too_old`, `future_version`, throttling, ...)
  means that attempt did not commit. Retrying it cannot apply that attempt twice, but an
  earlier attempt of the same retry loop may still have committed (below).
* **Maybe committed.** `commit_unknown_result` (1021) means the commit may or may not have
  happened (the connection to the proxy failed mid-request, a recovery, ...).
  `is_maybe_committed()` is true for it, and this client keeps it true for an error that
  hides such a commit: a commit cancelled or timed out after its request was sent, an
  `on_error` that gives up after a maybe-committed attempt, a `used_during_commit` after a
  commit that may have been sent.

`Database::run` (like every binding's retry loop) **retries maybe-committed transactions**,
so a transaction may take effect more than once unless it is idempotent. Writing fixed values
is idempotent; an atomic `Add`, a versionstamped key or an append is not. The closure's
`MaybeCommitted` argument is true when an earlier attempt may have committed: a closure that
is not idempotent should then check (for example, write a unique marker key in the same
transaction and read it first). If `run` gives up after an attempt that may have committed,
its error is `is_maybe_committed()`, whatever its code.

**Read the guarantee from the predicates, not from the error code.** `run` can return
`not_committed` (1020) marked maybe committed: an attempt ends in `commit_unknown_result`, the
next in a conflict, and the retry limit stops the loop. A hand-written `commit` / `on_error`
loop sees one attempt at a time: there, a 1020 after an earlier 1021 is a plain 1020, and the
caller must remember that an earlier attempt may have committed.

## Runtime model

The client is async and runtime-agnostic: it is written against the
`fdb_native_transport::runtime::Runtime` trait (TCP connect, timers, task spawning, DNS),
re-exported as `fdb_native_client::runtime`. The tokio adapter, `TokioRuntime`, is the default
(`tokio` feature): `Database::open` uses the current tokio runtime (multi-threaded or
current-thread). The second adapter is smol's, `SmolRuntime` (`smol` feature: async-io sockets and
timers, async-executor tasks, no tokio): `Database::open_with(SmolRuntime::new(), ..)` runs the
client's tasks on a process-wide executor with its own threads, so it works from any thread (see
`examples/smol.rs`); `SmolRuntime::with_executor` runs them on an executor you drive. Any other
executor works through `Database::open_with` and an adapter of the trait; futures are `Send`, so
thread-local executors are not supported. The transport and client test suites run on both
adapters in CI.

A database spawns its background tasks when it is opened (the coordinators' long poll, a
connection task per server), so open it inside the runtime. Reads run as tasks of their own.
`db.shutdown().await` stops everything; dropping the last handle does too.

## Feature flags

`fdb-native-client`:

| Feature | Default | What it does |
| --- | --- | --- |
| `tokio` | yes | `Database::open` / `from_connection_string` on tokio, `TokioRuntime` as the default runtime type |
| `smol` | no | `SmolRuntime`, the smol adapter, to open a `Database` over with `Database::open_with`; independent of `tokio` (`default-features = false, features = ["smol"]` builds without tokio) |
| `tls` | no | TLS clusters (`:tls` coordinators) with rustls; certificates from `DatabaseOptions::tls` (`TlsConfig`) or FDB's `FDB_TLS_*` variables. Without it, a TLS cluster fails to open with `tls_error` (2107) |
| `bench-internals` | no | a hidden facade for the RYW benchmarks; not part of the API |

## Not supported yet

`get_estimated_range_size_bytes` and `get_range_split_points` (storage metrics),
`get_addresses_for_key`, mapped range reads, the special key space (`\xff\xff/...`),
tenants, transaction tags (throttling), idempotency ids, read priorities and the server-side
cache hint sent to storage servers, the directory layer, and a multi-version client.

## Crates

| Crate | Purpose |
| --- | --- |
| `fdb-native-wire` | Sans-IO codec for FDB's flatbuffers dialect and the legacy binary format |
| `fdb-native-wire-derive` | `#[derive(Wire)]`, re-exported by `wire` (its default `derive` feature). It dev-depends on `wire` while `wire` depends on it, so publish it first (see its crate docs) |
| `fdb-native-schema` | Per-protocol-version message schemas (7.4, 8.0); depends on `wire` |
| `fdb-native-transport` | Runtime-agnostic connection layer; depends on `wire` |
| `fdb-native-client` | Public client API; depends on `schema` and `transport` |
| `fdb-native-tuple` | Tuple and subspace layers |
| `fdb-native-testkit` | Docker test harness (not published) |
| `fdb-native-stacktester` | Binding tester stack machine binary (not published) |
| `fdb-native-sim` | Deterministic simulation: a seeded `Runtime`, a faulty in-memory network and a model cluster, with a seed sweep (not published) |

## Golden vectors

[`tools/golden-vectors/`](tools/golden-vectors/) is a dev-only C++ tool that builds against
FoundationDB's own serializer headers and writes golden flatbuffers-dialect vectors (bytes plus
decoded field dumps, and malformed-input cases) to `tools/golden-vectors/fixtures/`. Message-level
vectors for real FDB request and reply types (`fixtures/release-7.4/`, `fixtures/release-8.0/`)
are generated by each release branch's own code, built inside the official FDB build image
(`regen_messages.sh`, needs docker). The fixtures are checked in; the Rust crates never build the
tool, and only an opt-in test runs it. See its [README](tools/golden-vectors/README.md) for the
fixture format and how to regenerate.

## Testing

Requires [cargo-nextest](https://nexte.st/).

```bash
# Unit tests
cargo nextest run --workspace

# The deterministic simulation's seed sweep alone (part of the unit tests; see CLAUDE.md to
# replay a seed with its trace)
cargo nextest run -p fdb-native-sim --no-capture -E 'test(seed_sweep)'

# Integration tests that need a live FoundationDB (requires Docker)
cargo nextest run --workspace --run-ignored only --no-tests=pass -E 'not (binary(cpp_crosscheck) | test(cpp_crosscheck))'

# fdb-native-wire's encoder against FDB's C++ readers (requires tools/golden-vectors to be built)
FDB_GOLDEN_VECTORS_BIN=$PWD/tools/golden-vectors/build/golden-vectors \
  cargo nextest run -p fdb-native-wire --run-ignored only -E 'binary(cpp_crosscheck)'
```

The smol adapter's tests (the adapter's own, the peer manager and client on smol against a fake
cluster and TLS loopback, and docker tests of both) need the `smol` features:

```bash
cargo nextest run --workspace --features fdb-native-transport/smol,fdb-native-client/smol
cargo nextest run --workspace --features fdb-native-transport/smol,fdb-native-client/smol \
  --run-ignored only -E 'binary(docker_smol) | binary(smol_docker)'
```

Tests that need a live FoundationDB container are marked `#[ignore = "requires docker"]`.
The `FDB_IMAGE` environment variable selects the server image
(default `foundationdb/foundationdb:7.4.10`):

```bash
FDB_IMAGE=foundationdb/foundationdb:8.0.1 cargo nextest run --workspace --run-ignored only \
  -E 'not (binary(cpp_crosscheck) | test(cpp_crosscheck))'
```

The client's TLS docker tests (in `docker_database` and `docker_examples`) need the client's
`tls` feature:

```bash
cargo nextest run -p fdb-native-client --features tls --run-ignored only \
  -E 'binary(docker_database) | binary(docker_examples)'
```

`docker_examples` runs the examples against a testkit container.

Tests must only target disposable containers started by `fdb-native-testkit`, never a
shared or long-lived cluster: a malformed packet can make an FDB 7.4/8.0 server process exit.

Lint and format checks, as run in CI:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --features fdb-native-transport/tls,fdb-native-client/tls -- -D warnings
cargo clippy --workspace --all-targets --features fdb-native-transport/smol,fdb-native-client/smol -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo build --workspace --examples
```

## License

Apache-2.0. See [LICENSE](LICENSE).
