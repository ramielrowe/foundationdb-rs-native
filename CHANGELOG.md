# Changelog

The crates are versioned together (`[workspace.package]`) and are not published yet. Until
1.0, a minor version may break the API.

## Unreleased: v0

The first version: a pure-Rust FoundationDB client for 7.4 and 8.0 clusters that does not
link `libfdb_c`.

### fdb-native-client

* **Databases**: `Database::open` (cluster files, with FDB's default lookup and updates when
  the coordinators change), `Database::from_connection_string`, `Database::open_with` over any
  runtime with `DatabaseOptions`; `DatabaseOption` (`MaxWatches`, `SnapshotRywEnable` /
  `Disable`, the `Transaction*` defaults); `Database::run`, the retry loop, with
  `MaybeCommitted`; `shutdown`.
* **Transactions**, with C++'s read-your-writes semantics (`ReadYourWritesTransaction`: the
  write map, the snapshot cache, read and write conflict ranges as C++ forms them, unreadable
  ranges of versionstamped keys, a commit that waits for the reads in progress):
  * reads: `get`, `get_key` (`KeySelector`), `get_range` (`RangeOptions`, `StreamingMode`,
    `fdb_c`'s limits and batch sizes), the streams `get_ranges` and `get_ranges_keyvalues`,
    snapshot reads (`snapshot()`), `\xff/metadataVersion`;
  * writes: `set`, `clear`, `clear_range`, `atomic_op` (`MutationType`: every atomic operation
    of `fdb.options`), `set_versionstamped_key` / `set_versionstamped_value` and
    `get_versionstamp`; deferred write errors as `fdb_c`'s;
  * conflict ranges (`add_read_conflict_range`, `add_write_conflict_range` and their `_key`
    forms), `get_read_version` / `set_read_version`, `commit` (`CommitResult`),
    `committed_version`, `versionstamp`, `approximate_size`;
  * `on_error` with C++'s backoff, retry limit and retry predicates, `reset`, `cancel`,
    timeouts, used-during-commit protection;
  * `TransactionOption`: the options of `fdb.options` that apply to this client (timeouts,
    retry and size limits, priorities, system keys, lock awareness, read-your-writes and
    snapshot read-your-writes, causal options, conflict reporting, ...).
* **Watches**: `Transaction::watch` and `Watch`, with C++'s semantics (the value as the
  transaction sees it, started by the commit, the cap on outstanding watches).
* **Errors**: `Error`, an FDB error code with `is_retryable`, `is_maybe_committed`,
  `is_retryable_not_committed` and `transaction_retry`; maybe-committed is kept through the
  errors that hide a commit (timeouts, cancellations, `on_error`, `Database::run`'s final
  error), so the guarantee is the predicates', not the code's: `run` can return a
  `not_committed` (1020) that is maybe committed.
* **TLS** (`tls` feature): `:tls` coordinators, `TlsConfig` and FDB's `FDB_TLS_*` variables,
  FDB's peer verification rules.
* **Runtime**: runtime-agnostic (`runtime::Runtime`), tokio by default (`tokio` feature); a
  second adapter for smol (`smol` feature: `SmolRuntime` over async-io, async-executor and
  `blocking`, no tokio), whose test suites run in CI.
* The read path (GRV batching, the location cache, load balancing as `LoadBalance.actor.h`
  does) and the commit path (proxies, `commit_unknown_result` handling) are ports of the C++
  client; they are public as the lower-layer modules `read`, `commit` and `cluster`.
* Examples (`examples/`): basic reads and writes, range scans, watches, versionstamped keys
  with the tuple layer, atomic counters, options and timeouts, TLS.

### fdb-native-tuple

The tuple layer (every type code, byte-compatible with the other bindings, versionstamps
with API 520+ offsets) and `Subspace`.

### fdb-native-transport, fdb-native-schema, fdb-native-wire

FDB's connection protocol (ConnectPacket, framing with checksums, the connection monitor's
pings, TLS with rustls, `Runtime` adapters for tokio and smol), the per-release message schemas of 7.4 and 8.0, and the flatbuffers dialect
and binary codecs, checked against the C++ readers with golden vectors.

### API decisions made before the first release

These changed the API that earlier development builds had:

* `MutationType` is an enum of the atomic operations (`MutationType::Add`, `BitAnd`, ...,
  with `code`, `from_code`, `name`, `from_name` and the slice `ALL`), no longer the wire type
  (`MutationType::ADD_VALUE`, ...). `BitAnd` and `Min` are sent as `AndV2` and `MinV2`.
  `atomic::apply` takes either.
* `Transaction::range_stream` (and `Snapshot::range_stream`) is now `get_ranges_keyvalues`,
  next to `get_ranges` (the batches).
* `add_read_conflict_range` and `add_write_conflict_range` return `Result`: `inverted_range`
  at once, as `fdb_transaction_add_conflict_range` does (it was a deferred error, checked
  after `key_outside_legal_range`); new `add_read_conflict_key` / `add_write_conflict_key`.
* `Transaction::watch` starts when it is called and returns a future that does not borrow
  the transaction (it was an `async fn`, started when polled), as `get` does.
  `get_versionstamp`'s future no longer borrows the transaction either.
* `Transaction::watch` allows `\xff/metadataVersion` without the system key options, as C++
  does.
* The crate root re-exports the application API only. The lower layers are reached through
  their modules: `cluster::{ClusterMonitor, ConnectionRecord, ...}`,
  `cluster_file::{ClusterConnectionString, ...}`, `commit::{Committer, CommitBuffer, ...}`,
  `read::{Reader, ReadConfig, ...}`, `watch::WatchConfig`, `database::ClientTls`. New:
  `runtime` (the transport's), `TlsConfig` (with `tls`), `database::PeerManagerConfig`. The
  knob fields of `DatabaseOptions`, `Database::from_record` and the lower-layer accessors
  (`reader`, `committer`, `peers`, `cluster_monitor`) follow the lower tier's stability.
* `Error::on_error` is private (use `Error::transaction_retry`, or `Transaction::on_error`).
* `CommitResult` is `Copy` and `Hash`; `RangeOptions::limit` joins the other builder
  methods; builders, `create_transaction`, `snapshot` and the range streams are
  `#[must_use]`.
* New: `strinc` (the end of a prefix's range, as the bindings' `strinc`) and
  `DatabaseOption::TransactionLoggingMaxFieldLength` (405, the default of the transaction
  option).
