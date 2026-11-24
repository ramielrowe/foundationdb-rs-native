//! A pure-Rust native FoundationDB client: it speaks FoundationDB's wire protocol to 7.4 and
//! 8.0 clusters directly, without `libfdb_c`.
//!
//! # Quick start
//!
//! ```no_run
//! use fdb_native_client::{Database, Error, TransactionOption};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     // The default cluster file (`FDB_CLUSTER_FILE`, `./fdb.cluster`,
//!     // `/etc/foundationdb/fdb.cluster`), on the current tokio runtime.
//!     let db = Database::open(None)?;
//!
//!     // A transaction in a retry loop: conflicts and other retryable errors run the
//!     // closure again (it must be idempotent: see `Database::run`).
//!     db.run(|tx, _maybe_committed| async move {
//!         tx.set_option(TransactionOption::Timeout(Duration::from_secs(5)))?;
//!         tx.set(b"hello", b"world");
//!         tx.clear_range(b"tmp/", b"tmp0");
//!         Ok(())
//!     })
//!     .await?;
//!
//!     // Read-modify-write: the read adds a conflict range, so concurrent increments are
//!     // serialized by FDB's conflict detection and retried by `run`. (An atomic `Add` would
//!     // not conflict at all: see `Transaction::atomic_op`.)
//!     let count = db
//!         .run(|tx, _| async move {
//!             let n = match tx.get(b"counter").await? {
//!                 Some(v) => u64::from_le_bytes(v.try_into().unwrap_or([0; 8])),
//!                 None => 0,
//!             };
//!             tx.set(b"counter", &(n + 1).to_le_bytes());
//!             Ok(n + 1)
//!         })
//!         .await?;
//!     println!("counter = {count}");
//!
//!     // The same, by hand: `on_error` is FDB's retry policy (backoff, retry limit, ...).
//!     let tx = db.create_transaction();
//!     let value = loop {
//!         let attempt = async {
//!             let value = tx.get(b"hello").await?;
//!             tx.commit().await?;
//!             Ok::<_, Error>(value)
//!         };
//!         match attempt.await {
//!             Ok(value) => break value,
//!             Err(e) => tx.on_error(e).await?,
//!         }
//!     };
//!     assert_eq!(value.as_deref(), Some(&b"world"[..]));
//!
//!     db.shutdown().await;
//!     Ok(())
//! }
//! ```
//!
//! The crate's `examples/` directory has more: range scans, watches, versionstamped keys
//! with the tuple layer, atomic counters, options and timeouts, TLS.
//!
//! # The API
//!
//! * [`Database`]: a cluster, opened from a cluster file ([`Database::open`]) or a
//!   connection string ([`Database::from_connection_string`]), with [`DatabaseOptions`]
//!   ([`Database::open_with`]) and [`DatabaseOption`]s ([`Database::set_option`]).
//!   [`Database::run`] is the retry loop.
//! * [`Transaction`]: reads ([`get`](Transaction::get), [`get_key`](Transaction::get_key)
//!   with a [`KeySelector`], [`get_range`](Transaction::get_range) with [`RangeOptions`] and a
//!   [`StreamingMode`], and the range streams [`get_ranges`](Transaction::get_ranges)
//!   ([`RangeStream`]) and [`get_ranges_keyvalues`](Transaction::get_ranges_keyvalues)
//!   ([`KeyValueStream`])), snapshot reads ([`Transaction::snapshot`], [`Snapshot`]), writes
//!   ([`set`](Transaction::set), [`clear`](Transaction::clear),
//!   [`clear_range`](Transaction::clear_range), [`atomic_op`](Transaction::atomic_op) with a
//!   [`MutationType`], the versionstamped operations), conflict ranges, read versions,
//!   [`commit`](Transaction::commit) ([`CommitResult`]),
//!   [`get_versionstamp`](Transaction::get_versionstamp), [`watch`](Transaction::watch)
//!   ([`Watch`]), [`on_error`](Transaction::on_error), [`reset`](Transaction::reset),
//!   [`cancel`](Transaction::cancel), [`approximate_size`](Transaction::approximate_size),
//!   the storage metrics
//!   ([`get_estimated_range_size_bytes`](Transaction::get_estimated_range_size_bytes),
//!   [`get_range_split_points`](Transaction::get_range_split_points)) and
//!   [`TransactionOption`]s. Reads see the transaction's own writes (read-your-writes, as
//!   every FoundationDB binding); see [`transaction`] for the semantics, ported from C++.
//! * [`Error`]: the one error type, an FDB error code with C++'s retry predicates
//!   ([`is_retryable`](Error::is_retryable), [`is_maybe_committed`](Error::is_maybe_committed),
//!   [`is_retryable_not_committed`](Error::is_retryable_not_committed)); the codes are in
//!   [`error::codes`].
//! * [`strinc`]: the end of the range of the keys with a prefix.
//!
//! # Retries and at-most-once
//!
//! A commit either happens or does not, but the client cannot always tell which: a commit
//! that fails with `commit_unknown_result` (1021), or that is cancelled or times out after
//! its request was sent, **may have committed**. [`Error::is_maybe_committed`] says so (and
//! this client keeps it true through `on_error`, a timeout or a cancellation that follows).
//! [`Database::run`] retries such a transaction as C++'s retry loops do, so its closure must
//! be idempotent, or check whether an earlier attempt took effect: the [`MaybeCommitted`]
//! argument tells it when one may have.
//!
//! **Read the guarantee from the predicates, not from the code**: an error for which
//! [`is_retryable_not_committed`](Error::is_retryable_not_committed) is true did not commit;
//! one for which [`is_maybe_committed`](Error::is_maybe_committed) is true may have. The
//! code alone does not say: `run` can return `not_committed` (1020) marked maybe committed,
//! when an earlier attempt ended in `commit_unknown_result` and the retry limit stopped the
//! loop after a conflict. A hand-written `commit` / `on_error` loop sees one attempt at a
//! time: there, a 1020 after an earlier 1021 is a plain 1020, and the caller must remember
//! that an earlier attempt may have committed.
//!
//! # Runtime
//!
//! The client is async and runtime-agnostic: it runs over a
//! [`Runtime`](runtime::Runtime) (TCP, timers, task spawning, DNS). With the default `tokio`
//! feature, [`Database::open`] and [`Database::from_connection_string`] use the current
//! tokio runtime (multi-threaded or current-thread); [`Database::open_with`] takes any
//! runtime. A database spawns its tasks (the coordinator long poll, one connection task per
//! server) when it is opened, so open it inside the runtime. Every future and handle is
//! `Send`; [`Database`], [`Transaction`] and [`Error`] are `Send + Sync` and cheap to clone.
//!
//! # Features
//!
//! * `tokio` (default): `Database::open` and friends on tokio, and `TokioRuntime` as the
//!   default runtime. Without it, use [`Database::open_with`] over any
//!   [`Runtime`](runtime::Runtime).
//! * `tls`: TLS clusters (`:tls` coordinators), configured as FoundationDB is (the
//!   `FDB_TLS_*` variables, or `DatabaseOptions::tls` with a `TlsConfig`, re-exported here).
//!   Without it, opening a cluster with TLS coordinators fails with `tls_error` (2107).
//!
//! # API design notes
//!
//! These are the choices made for the 0.x API; the semantics follow C++'s
//! `ReadYourWritesTransaction` and `fdb_c` throughout.
//!
//! * **Handles, `&self` everywhere.** `Database` and `Transaction` are cheap shared handles;
//!   every method takes `&self`, so reads can run concurrently on one transaction (as with
//!   `fdb_c`'s futures). [`Database::run`] hands its closure a handle of the attempt, which
//!   goes stale when the attempt is over (see the [`transaction`] module).
//! * **Reads start when they are called.** `get`, `get_key`, `get_range` and `watch` return
//!   `Send` futures that do not borrow the transaction or the arguments, and the read runs
//!   as a task of its own, as a C++ read actor does: dropping the future does not cancel the
//!   read, a read sees the writes made before it was called, and the commit waits for the
//!   reads in progress. This is what C++'s read-your-writes semantics need; it differs from
//!   Rust's usual lazy futures. `get_versionstamp` likewise registers its future when it is
//!   called. `commit`, `on_error` and `get_read_version` are ordinary `async fn`s (they
//!   start when polled, and borrow the transaction: clone the handle to move one into a
//!   task).
//! * **Writes return nothing; their errors are deferred**, as `fdb_c`'s: an invalid `set`
//!   (`key_too_large`, ...) fails the next read or the commit. The exception is the conflict
//!   range methods, which return `inverted_range` at once, as
//!   `fdb_transaction_add_conflict_range` does (and defer the rest).
//! * **Typed options and operations.** [`TransactionOption`], [`DatabaseOption`],
//!   [`MutationType`] and [`StreamingMode`] are `#[non_exhaustive]` enums with their
//!   `fdb.options` codes ([`TransactionOption::code`], [`MutationType::from_name`], ...).
//!   Options that only matter to features this client does not have are either left out
//!   (no variant) or accepted as validated no-ops where that is what C++ does in effect
//!   (`IncludePortInAddress`, `TransactionLoggingMaxFieldLength`, the debug and tracing
//!   options); an option whose effect would be silently missing fails instead (`Tag` and
//!   `AutoThrottleTag` are `invalid_option`). The tables in [`transaction::options`] and
//!   [`database::options`] say which is which.
//! * **Synchronous getters where C++ is synchronous.**
//!   [`approximate_size`](Transaction::approximate_size) is C++'s `getApproximateSize()`, a
//!   counter of the read-your-writes layer: `fdb_c` returns it as a future only because
//!   every call goes through its network thread, which this client does not have. Likewise
//!   [`committed_version`](Transaction::committed_version) and
//!   [`versionstamp`](Transaction::versionstamp) read what the last commit left;
//!   [`get_versionstamp`](Transaction::get_versionstamp) is the future the bindings have, to
//!   be called before the commit.
//! * **Range reads**: [`get_range`](Transaction::get_range) is one batch, as
//!   `fdb_transaction_get_range` (the limits, `more`, the streaming mode's batch sizes);
//!   [`get_ranges`](Transaction::get_ranges) streams the batches and
//!   [`get_ranges_keyvalues`](Transaction::get_ranges_keyvalues) the rows, as the bindings'
//!   iterators read them. The streams implement `futures_core::Stream`; use any `StreamExt`
//!   (`futures`, `futures-lite`, `tokio-stream`) to iterate them.
//! * **One error type**, an FDB error code with the predicates `fdb_error_predicate` has
//!   ([`Error`]); it is not `PartialEq` (compare [`code`](Error::code)s), and it may know
//!   more than its code (whether a commit may have happened, the conflicting key ranges).
//! * **Two tiers of modules.** The crate root re-exports the application API above. The
//!   modules [`read`], [`commit`], [`cluster`], [`cluster_file`], [`watch`] and [`atomic`]
//!   are the client's lower layers (the read and commit paths, the coordinator connection,
//!   cluster files, the watch machinery, the atomic operations' arithmetic); they are public
//!   for tools and tests, follow C++'s structure, and may change more freely than the
//!   application API. A few application items reach into them on purpose, and those parts
//!   follow the lower tier's stability: the knob fields of [`DatabaseOptions`] (all but
//!   `tls`; [`database::PeerManagerConfig`] is re-exported for the first),
//!   [`Database::from_record`] and the accessors [`Database::cluster_monitor`],
//!   [`Database::reader`], [`Database::committer`] and [`Database::peers`].
//!
//! # Not supported yet
//!
//! `get_estimated_range_size_bytes` and `get_range_split_points` (the storage servers'
//! metrics requests), `get_addresses_for_key`, mapped range reads, the special key space
//! (`\xff\xff`), tenants, transaction tags, idempotency ids, and the read options sent to
//! storage servers (read priority, server-side cache). The directory layer is not part of
//! this crate; the tuple and subspace layers are in `fdb-native-tuple`.

pub mod atomic;
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod bench_support;
pub mod cluster;
pub mod cluster_file;
pub mod commit;
pub mod database;
pub mod error;
mod keys;
pub mod read;
mod ryw;
pub mod transaction;
pub mod watch;

/// The runtime abstraction the client runs over (`fdb_native_transport::runtime`):
/// [`Runtime`](runtime::Runtime), and with the `tokio` feature
/// [`TokioRuntime`](runtime::TokioRuntime).
pub use fdb_native_transport::runtime;

pub use database::{Database, DatabaseOption, DatabaseOptions, MaybeCommitted};
pub use error::{Error, TransactionRetry};
pub use fdb_native_schema::domain::KeySelector;
#[cfg(feature = "tls")]
pub use fdb_native_transport::tls::TlsConfig;
pub use fdb_native_wire::KeyValue;
pub use keys::strinc;
pub use read::{RangeOptions, StreamingMode};
pub use transaction::{
    CommitResult, KeyValueStream, MutationType, RangeResult, RangeStream, Snapshot, Transaction,
    TransactionOption,
};
pub use watch::Watch;

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;

    fn send_sync<T: Send + Sync>() {}
    fn send<T: Send>() {}

    /// C-SEND-SYNC: the handles and values are `Send + Sync`, the streams and futures
    /// `Send`.
    #[test]
    fn public_types_are_send_and_sync() {
        send_sync::<Database>();
        send_sync::<Transaction>();
        send_sync::<Error>();
        send_sync::<CommitResult>();
        send_sync::<RangeResult>();
        send_sync::<RangeOptions>();
        send_sync::<KeySelector>();
        send_sync::<TransactionOption>();
        send_sync::<DatabaseOption>();
        send_sync::<DatabaseOptions>();
        send_sync::<MutationType>();
        send_sync::<Watch>();
        send::<RangeStream<runtime::TokioRuntime>>();
        send::<KeyValueStream<runtime::TokioRuntime>>();
        send::<Snapshot<'static, runtime::TokioRuntime>>();
    }

    #[allow(dead_code, clippy::let_underscore_future)]
    fn futures_are_send(tx: Transaction, db: Database) {
        fn is_send<T: Send>(_: &T) {}
        is_send(&tx.get(b"k"));
        is_send(&tx.get_key(&KeySelector::first_greater_or_equal(b"k".to_vec())));
        is_send(&tx.get_range(
            &KeySelector::first_greater_or_equal(b"a".to_vec()),
            &KeySelector::first_greater_or_equal(b"b".to_vec()),
            &RangeOptions::default(),
        ));
        is_send(&tx.commit());
        is_send(&tx.watch(b"k"));
        is_send(&tx.on_error(Error::new(1020)));
        is_send(&tx.get_versionstamp());
        is_send(&tx.get_read_version());
        is_send(&db.run(|tx, _| async move { tx.get(b"k").await }));
    }

    /// The reads' futures borrow neither the transaction nor their arguments.
    #[allow(dead_code)]
    fn read_futures_are_static(tx: Transaction) -> Vec<Box<dyn std::any::Any + Send>> {
        fn boxed<T: Send + 'static>(t: T) -> Box<dyn std::any::Any + Send> {
            Box::new(t)
        }
        let key = b"k".to_vec();
        let selector = KeySelector::first_greater_or_equal(key.clone());
        let futures = vec![
            boxed(tx.get(&key)),
            boxed(tx.snapshot().get(&key)),
            boxed(tx.get_key(&selector)),
            boxed(tx.get_range(&selector, &selector, &RangeOptions::default())),
            boxed(tx.watch(&key)),
            boxed(tx.get_versionstamp()),
            boxed(tx.get_ranges_keyvalues(
                selector.clone(),
                selector.clone(),
                RangeOptions::default(),
            )),
        ];
        drop((tx, key, selector));
        futures
    }
}
