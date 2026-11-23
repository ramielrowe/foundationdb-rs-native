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
//!     // serialized by FDB's conflict detection and retried by `run`.
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
//! The vertical slice is **reads of single keys, writes, clears, atomic operations and
//! commits**, with FDB's conflict detection and retry semantics; reads do not yet see the
//! transaction's own writes (see [`transaction`]).
//!
//! # Layers
//!
//! * [`Database`] and [`Transaction`] ([`database`], [`transaction`]): the API.
//! * [`error`]: [`Error`], the one error type, an FDB error with C++'s retry predicates.
//! * [`cluster_file`]: cluster files and connection strings.
//! * [`cluster`]: the coordinator connection, [`ClusterMonitor`], which keeps the client's
//!   `ClientDBInfo` (its GRV and commit proxies) current.
//! * [`read`]: the read path, [`Reader`]: read versions, the key location cache, and
//!   values.
//! * [`commit`]: the commit path, [`Committer`] and [`CommitBuffer`].
//!
//! # Features
//!
//! * `tokio` (default): `Database::open` and friends on tokio, and `TokioRuntime` as the
//!   default runtime. Without it, use [`Database::open_with`] over any
//!   [`Runtime`](fdb_native_transport::runtime::Runtime).
//! * `tls`: TLS clusters (`:tls` coordinators), configured as FoundationDB is (the
//!   `FDB_TLS_*` variables, or [`DatabaseOptions`]`::tls`).

pub mod cluster;
pub mod cluster_file;
pub mod commit;
pub mod database;
pub mod error;
pub mod read;
pub mod transaction;

pub use cluster::{
    ClusterInfo, ClusterInfoWatch, ClusterMonitor, ClusterMonitorConfig, CommitProxyEndpoints,
    ConnectionRecord, GrvProxyEndpoints, MonitorClosed, MonitorError, MonitorStatus,
};

pub use commit::{CommitBuffer, CommitOptions, CommitOutcome, Committer, CommitterConfig};

pub use cluster_file::{
    ClusterConnectionString, ClusterFile, ClusterFileError, ClusterFileLocation, ClusterFileSource,
    ConnectionStringError, Coordinator, ResolvedCoordinator,
};

pub use database::{ClientTls, Database, DatabaseOptions, MaybeCommitted};
pub use error::{Error, TransactionRetry};
pub use fdb_native_schema::domain::{KeySelector, MutationType};
pub use read::{KeyLocation, ReadConfig, ReadSnapshot, ReadVersionOptions, Reader};
pub use transaction::{CommitResult, Snapshot, Transaction, TransactionOption};
