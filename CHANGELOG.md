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
  `Disable`, `LocationCacheSize`, `DatacenterId` (replica preference) and `MachineId`
  (stored), the `Transaction*` defaults); `Database::run`, the retry loop, with
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
  * storage metrics: `get_estimated_range_size_bytes` and `get_range_split_points`
    (`fdb_c`'s, through the storage servers' `waitMetrics` and `getRangeSplitPoints`);
  * `on_error` with C++'s backoff, retry limit and retry predicates, `reset`, `cancel`,
    timeouts (a deadline that passed kills the transaction even if nothing ran then, as
    C++'s timer does), used-during-commit protection;
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
and binary codecs, checked against the C++ readers with golden vectors. As in C++, a peer made
anew starts with its address available, and a failed peer that is referenced again is probed again
(found by the deterministic simulation, `fdb-native-sim`, which is not published).
Requests are encoded with a writer each thread keeps (new: `Writer::buffer_capacity`, to drop
one that grew large), and the transport's reply and request maps use FxHash (bead `e82`).

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
* **The rows of a range read share the reply they came in** (bead `s1t`): `KeyValue`'s
  `key` and `value` are `Bytes` (`bytes::Bytes`, re-exported as `fdb_native_client::Bytes`
  and `fdb_native_wire::Bytes`), not `Vec<u8>`, and are slices of the storage server's reply,
  as C++'s rows point into the reply's arena: a row costs two reference counts instead of two
  allocations and copies, on the way from the reply through the read-your-writes cache to
  `RangeResult::kvs` and `KeyValueStream`. They deref to `[u8]` and compare with `[u8]`,
  `&[u8]` and `Vec<u8>` (not with a byte array: `kv.key == b"a"[..]`); `to_vec()` copies.
  A row keeps its whole reply alive: about 80 kB (`REPLY_BYTE_LIMIT` is soft, so a reply can
  be that plus the row that crosses it: up to about 190 kB with a 10 kB key and a 100 kB
  value). Copy the few rows you keep for long.
  `KeyValue::new` takes `impl Into<Bytes>` (`Vec<u8>`, `&'static [u8]`, `String`, `Bytes`).
  New in `fdb-native-wire`: `SharedKeyValueVector` (a `GetKeyValuesReply` blob in a shared
  buffer). `read::RangeRead::into_key_values` no longer copies.
* `Error::on_error` is private (use `Error::transaction_retry`, or `Transaction::on_error`).
* `CommitResult` is `Copy` and `Hash`; `RangeOptions::limit` joins the other builder
  methods; builders, `create_transaction`, `snapshot` and the range streams are
  `#[must_use]`.
* New: `strinc` (the end of a prefix's range, as the bindings' `strinc`) and
  `DatabaseOption::TransactionLoggingMaxFieldLength` (405, the default of the transaction
  option).

These aligned behaviour with C++'s read-your-writes layer:

* **`used_during_commit` before the cancellation or timeout.** A read, `watch`, `commit`,
  the storage metrics and `get_versionstamp` of a transaction that is committing give
  `used_during_commit` (2017) even if the transaction was cancelled or timed out meanwhile
  (they gave `transaction_cancelled` / `transaction_timed_out`), as C++ checks
  `checkUsedDuringCommit()` before `resetPromise` (`ReadYourWrites.actor.cpp` 7.4:1675-1680,
  2441-2446, 2502-2513, 2515-2521). The deferred error of a write still comes first.
* **A `watch` after the commit started fails the commit too**, as every other operation
  already did: C++'s `watch` calls `checkUsedDuringCommit()` (7.4:2441-2446, 8.0:2357-2362),
  which fails the commit with `used_during_commit` (2017, maybe committed once the commit
  may have been sent). Before, only the watch failed. `UsedDuringCommitProtectionDisable`
  (701) restores that: only the offending operation fails.
* **`get_read_version` during a commit** gives the read version and leaves the commit alone
  (it was `used_during_commit`, which also failed the commit): C++'s `getReadVersion`
  (7.4:1581-1587) has no `checkUsedDuringCommit`.
* **Dropping the last handle of a transaction cancels it**, as `~ReadYourWritesTransaction`
  does (7.4:2774-2776): its reads, watches' reads, range batches and read version request in
  progress end at once with `transaction_cancelled` (1025), also for a future the user still
  holds; nothing of the transaction outlives it (its tasks held it, and the database, until
  their reads ended).
* **`get_versionstamp` after a sent commit was cut short is maybe committed.** When the
  transaction is killed (`cancel`, the timeout, the drop of the last handle) after its commit
  request may have reached a proxy, the future fails with that error marked
  `is_maybe_committed()` (C++: a bare `transaction_cancelled`), as the commit's own result
  already is; the stamp of a commit that may have succeeded is lost. Before the request was
  sent the error is as before.
* **Dropping a `watch` future before its read ends cancels the watch**: no read conflict
  range, no registration (the read went on and added its range), as C++'s `watch` actor is
  cancelled with its future.
* **The bound of the readable keys** (`ReadSystemKeys`, `AccessSystemKeys`) is looked at
  again after each wait of a read, as C++ calls `getMaxReadKey()` there (it was taken when
  the read was called).
* **`set(\xff/metadataVersion)`** is refused before anything else (C++ 7.4:2299-2302): during
  a commit it is a deferred `client_invalid_operation` that does not poison the commit, and it
  does not use up `NextWriteNoWriteConflictRange`.
* **`Database::shutdown` stops the read version batchers** even while handles of the
  database or of its transactions are held (they ran until the last one was dropped): the
  waiting requests and every later one fail with `operation_cancelled` (1101). New:
  `read::Reader::shutdown`.
* **Databases share their connections**, as the databases of a C++ process share its
  `FlowTransport`: databases opened on the same runtime with the same
  `DatabaseOptions::peers` and TLS material use one connection per server and one failure
  monitor (each keeps its own long poll, location cache, batchers and options). The sharing is
  counted: `Database::shutdown` (or dropping a database) ends that database's requests in
  progress and closes the connections only when no other database uses them. New:
  `DatabaseOptions::share_connections` (default `true`), `Runtime::same_runtime` (default
  `false`: a runtime that does not say never shares; tokio compares runtime ids, smol the
  executor), `PeerManager::share` / `downgrade` / `users` / `shares_connections_with` /
  `connections_shut_down` / `runtime` / `stream_wrapper` and `WeakPeerManager` (`share`,
  `share_on`, `share_on_if`); `PeerManager::shutdown` and `is_shut_down` are now per user (the
  peers' tasks end a user's commands; a request's path is unchanged). `PeerManagerConfig` is
  `PartialEq`. The databases that share connections share their per-connection limits
  (command queue, requests in flight). `read::Reader::shutdown` lets go of the reader's server
  references (the location cache is emptied, and a location fetched after is not kept).
