//! [`Database`]: a connection to a cluster, the transactions on it, and the retry loop.
//!
//! A database owns everything a client needs to talk to one cluster, built from a cluster
//! file or a connection string:
//!
//! * the [`PeerManager`] (one connection per server address, with TLS when the cluster uses
//!   it, see [TLS](#tls));
//! * the [`ClusterMonitor`] (the coordinators' long poll for the proxies);
//! * the [`Reader`] (read versions, locations, values) and the [`Committer`].
//!
//! [`Database::create_transaction`] makes a [`Transaction`]; [`Database::run`] runs a closure
//! in a transaction and retries it as C++'s retry loops do.
//!
//! # TLS
//!
//! With the `tls` feature, a database whose connection string has a `:tls` coordinator (or
//! whose `DatabaseOptions::tls` is set) connects to TLS addresses with the transport's TLS
//! layer, configured as FoundationDB configures TLS: the explicit `TlsConfig` settings,
//! else the `FDB_TLS_*` environment variables, else `cert.pem` / `key.pem` in the default
//! config directory (see `fdb_native_transport::tls::TlsConfig::load`). Without the feature,
//! opening a cluster with `:tls` coordinators fails with `tls_error` (2107).
//!
//! # Shutdown
//!
//! [`Database::shutdown`] stops the cluster monitor and closes every connection, waiting for
//! both; transactions then fail with `operation_cancelled` (1101). Dropping the last handle
//! of a database (and of its transactions, which hold one) stops them too, without waiting.

#[cfg(feature = "tls")]
use fdb_native_transport::tls::TlsConfig;
use std::fmt;
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use fdb_native_transport::peer::{DiscardEvents, PeerManager, StreamWrapper};
use fdb_native_transport::runtime::Runtime;
#[cfg(feature = "tokio")]
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_wire::NetworkAddress;

use crate::cluster::{ClusterMonitor, ClusterMonitorConfig, ConnectionRecord};
use crate::cluster_file::{ClusterConnectionString, ClusterFile};
use crate::commit::{Committer, CommitterConfig};
use crate::error::Error;
use crate::error::codes::TLS_ERROR;
use crate::read::util::Rng;
use crate::read::{ReadConfig, Reader};
use crate::transaction::{Transaction, TransactionOption};
use crate::watch::{ABSOLUTE_MAX_WATCHES, WatchConfig, WatchContext};

pub mod options;

pub use fdb_native_transport::peer::PeerManagerConfig;
pub use options::DatabaseOption;

/// How a [`Database`] is built. [`DatabaseOptions::default`] has C++'s knobs everywhere.
///
/// The struct is `#[non_exhaustive]`: start from the default and change fields.
///
/// **Stability.** `tls` is part of the application API. The other fields are the knobs of
/// the client's lower layers ([`PeerManagerConfig`], re-exported here,
/// [`ClusterMonitorConfig`], [`ReadConfig`], [`CommitterConfig`], [`WatchConfig`]): they
/// follow those layers' stability (see the crate's [API design
/// notes](crate#api-design-notes)), and most programs leave them at their defaults.
///
/// ```no_run
/// # #[cfg(feature = "tls")]
/// # fn example() -> Result<(), fdb_native_client::Error> {
/// use fdb_native_client::runtime::TokioRuntime;
/// use fdb_native_client::{Database, DatabaseOptions, TlsConfig};
///
/// let mut options = DatabaseOptions::default();
/// options.tls = Some(
///     TlsConfig::new()
///         .ca_path("/etc/foundationdb/ca.pem")
///         .cert_path("/etc/foundationdb/cert.pem")
///         .key_path("/etc/foundationdb/key.pem"),
/// );
/// let db = Database::open_with(TokioRuntime::new(), None, options)?;
/// # let _ = db;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct DatabaseOptions {
    /// The connections.
    pub peers: PeerManagerConfig,
    /// The coordinator long poll.
    pub monitor: ClusterMonitorConfig,
    /// The read path.
    pub read: ReadConfig,
    /// The commit path. Its backoff knobs (`initial_backoff`, `max_backoff`,
    /// `resource_constrained_max_backoff`, `backoff_growth`, `future_version_retry_delay`)
    /// are also the transactions' (`DEFAULT_BACKOFF`, `DEFAULT_MAX_BACKOFF`, ...).
    pub commit: CommitterConfig,
    /// Watches: the cap and the polling time.
    pub watch: WatchConfig,
    /// TLS settings (FoundationDB's `TLS_*` network options). `None` (the default) uses the
    /// environment and the default files, and only if the cluster has `:tls` coordinators;
    /// `Some` enables TLS whatever the coordinators are. See the [module
    /// documentation](self#tls).
    #[cfg(feature = "tls")]
    pub tls: Option<TlsConfig>,
}

/// Whether the previous attempts of a [`Database::run`] closure may have committed: the
/// closure runs again after `commit_unknown_result`, and must then be idempotent (or check
/// whether an earlier attempt took effect). `MaybeCommitted(false)` on the first attempt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct MaybeCommitted(pub bool);

impl From<MaybeCommitted> for bool {
    fn from(m: MaybeCommitted) -> bool {
        m.0
    }
}

/// The [`StreamWrapper`] of a database's connections: the transport's TLS layer when the
/// database uses TLS (`tls` feature), else none (a TLS address then fails to connect with
/// [`PeerError::TlsNotConfigured`](fdb_native_transport::peer::PeerError::TlsNotConfigured)).
#[derive(Clone, Debug, Default)]
pub struct ClientTls {
    #[cfg(feature = "tls")]
    wrapper: Option<fdb_native_transport::tls::peer::TlsStreamWrapper>,
}

impl ClientTls {
    /// No TLS.
    pub fn none() -> ClientTls {
        ClientTls::default()
    }

    /// TLS with `wrapper`.
    #[cfg(feature = "tls")]
    pub fn new(wrapper: fdb_native_transport::tls::peer::TlsStreamWrapper) -> ClientTls {
        ClientTls {
            wrapper: Some(wrapper),
        }
    }

    /// Whether TLS addresses can be connected to.
    pub fn is_enabled(&self) -> bool {
        #[cfg(feature = "tls")]
        {
            self.wrapper.is_some()
        }
        #[cfg(not(feature = "tls"))]
        {
            false
        }
    }

    /// What a database of `connection_string` with `options` connects with (see the [module
    /// documentation](self#tls)).
    fn for_cluster(
        connection_string: &ClusterConnectionString,
        options: &DatabaseOptions,
    ) -> Result<ClientTls, Error> {
        let wants_tls = connection_string.coordinators().iter().any(|c| c.tls());
        #[cfg(feature = "tls")]
        {
            use fdb_native_transport::tls::TlsConnector;
            use fdb_native_transport::tls::peer::TlsStreamWrapper;
            if !wants_tls && options.tls.is_none() {
                return Ok(ClientTls::none());
            }
            let config = options.tls.clone().unwrap_or_default();
            let loaded = config
                .load()
                .map_err(|e| Error::with_source(TLS_ERROR, e))?;
            let connector =
                TlsConnector::new(&loaded).map_err(|e| Error::with_source(TLS_ERROR, e))?;
            Ok(ClientTls::new(TlsStreamWrapper::new(connector)))
        }
        #[cfg(not(feature = "tls"))]
        {
            let _ = options;
            if wants_tls {
                return Err(Error::with_message(
                    TLS_ERROR,
                    "the cluster's coordinators use TLS: enable the `tls` feature of \
                     fdb-native-client",
                ));
            }
            Ok(ClientTls::none())
        }
    }
}

#[cfg(feature = "tls")]
type TlsStream<S> = fdb_native_transport::tls::peer::PeerTlsStream<S>;
#[cfg(not(feature = "tls"))]
type TlsStream<S> = S;

impl<S> StreamWrapper<S> for ClientTls
where
    S: futures_io::AsyncRead + futures_io::AsyncWrite + Send + Unpin + 'static,
{
    type Stream = TlsStream<S>;

    fn tls_enabled(&self) -> bool {
        self.is_enabled()
    }

    async fn wrap(&self, stream: S, peer: NetworkAddress) -> std::io::Result<TlsStream<S>> {
        #[cfg(feature = "tls")]
        if let Some(wrapper) = &self.wrapper {
            return wrapper.wrap(stream, peer).await;
        }
        let _ = (stream, peer);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "TLS is not configured",
        ))
    }
}

/// The transaction knobs, from [`CommitterConfig`].
#[derive(Clone, Debug)]
pub(crate) struct TransactionKnobs {
    pub(crate) initial_backoff: std::time::Duration,
    pub(crate) max_backoff: std::time::Duration,
    pub(crate) resource_constrained_max_backoff: std::time::Duration,
    pub(crate) backoff_growth: f64,
    pub(crate) future_version_retry_delay: std::time::Duration,
}

pub(crate) struct DbInner<R: Runtime> {
    pub(crate) rt: R,
    pub(crate) peers: PeerManager<R, ClientTls>,
    pub(crate) monitor: Arc<ClusterMonitor<R>>,
    pub(crate) reader: Reader<R, ClientTls>,
    pub(crate) committer: Committer<R, ClientTls>,
    pub(crate) watches: Arc<WatchContext<R>>,
    pub(crate) knobs: TransactionKnobs,
    /// `DatabaseContext::transactionDefaults`: the transaction options new transactions start
    /// with ([`Database::set_option`]).
    transaction_defaults: Mutex<Arc<Vec<TransactionOption>>>,
    /// `DatabaseContext::snapshotRywEnabled`: 1 (`apiVersionAtLeast(300)`), counted up and
    /// down by the `SNAPSHOT_RYW_ENABLE` / `DISABLE` database options; a transaction's
    /// snapshot reads see its writes while its own count (which starts from this one) is
    /// above 0.
    pub(crate) snapshot_ryw: AtomicI32,
    tls: bool,
    rng: Mutex<Rng>,
}

impl<R: Runtime> DbInner<R> {
    /// The defaults a transaction created now starts with.
    pub(crate) fn transaction_defaults(&self) -> Arc<Vec<TransactionOption>> {
        self.transaction_defaults
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// `random01()`, for the backoff jitter.
    pub(crate) fn random01(&self) -> f64 {
        self.rng
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .unit()
    }
}

macro_rules! database_struct {
    ($($default:ty)?) => {
        /// A FoundationDB database: the client's connection to one cluster. See the [module
        /// documentation](self).
        ///
        /// Cheap to clone (a shared handle). Open it inside the executor (it spawns the
        /// cluster monitor and connection tasks): with tokio (the `tokio` feature),
        /// `Database::open` or `Database::from_connection_string`; over any [`Runtime`],
        /// [`Database::open_with`] or [`Database::from_record`].
        ///
        /// ```no_run
        /// use fdb_native_client::{Database, DatabaseOption};
        /// use std::path::Path;
        /// use std::time::Duration;
        ///
        /// # async fn example() -> Result<(), fdb_native_client::Error> {
        /// let db = Database::open(Some(Path::new("/etc/foundationdb/fdb.cluster")))?;
        /// // Or, without a file: Database::from_connection_string("test:abcd@127.0.0.1:4500")
        /// db.set_option(DatabaseOption::TransactionTimeout(Duration::from_secs(10)))?;
        /// db.run(|tx, _| async move {
        ///     tx.set(b"k", b"v");
        ///     Ok(())
        /// })
        /// .await?;
        /// db.shutdown().await;
        /// # Ok(())
        /// # }
        /// ```
        pub struct Database<R: Runtime $(= $default)?> {
            pub(crate) inner: Arc<DbInner<R>>,
        }
    };
}

#[cfg(feature = "tokio")]
database_struct!(TokioRuntime);
#[cfg(not(feature = "tokio"))]
database_struct!();

impl<R: Runtime> Clone for Database<R> {
    fn clone(&self) -> Self {
        Database {
            inner: self.inner.clone(),
        }
    }
}

impl<R: Runtime> fmt::Debug for Database<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database")
            .field("connection_string", &self.inner.monitor.connection_string())
            .field("tls", &self.inner.tls)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "tokio")]
impl Database<TokioRuntime> {
    /// Opens the database of a cluster file, on the current tokio runtime: the file at
    /// `cluster_file`, or the one `fdb_create_database(NULL)` would use (`FDB_CLUSTER_FILE`,
    /// then `./fdb.cluster`, then the platform default, as
    /// [`lookup_cluster_file`](crate::cluster_file::lookup_cluster_file) finds it). The file
    /// is kept up to date when the cluster's coordinators change.
    ///
    /// This only reads the file (and, for TLS, the certificates) and starts the tasks; it
    /// does not wait for the cluster. A cluster that cannot be reached shows as requests that
    /// do not complete (use the `timeout` transaction option to bound them).
    ///
    /// # Errors
    ///
    /// `no_cluster_file_found` (1515), `connection_string_invalid` (2104), `io_error`
    /// (1510), `file_too_large` (1516), `tls_error` (2107).
    ///
    /// # Panics
    ///
    /// Outside a tokio runtime ([`TokioRuntime::new`]). Use [`Database::open_with`] there.
    pub fn open(cluster_file: Option<&Path>) -> Result<Database<TokioRuntime>, Error> {
        Database::open_with(
            TokioRuntime::new(),
            cluster_file,
            DatabaseOptions::default(),
        )
    }

    /// Opens the database of a connection string (`description:id@host:port,...`), on the
    /// current tokio runtime. It is kept in memory only: no cluster file is read or written.
    ///
    /// # Errors
    ///
    /// `connection_string_invalid` (2104), `tls_error` (2107).
    ///
    /// # Panics
    ///
    /// Outside a tokio runtime.
    pub fn from_connection_string(
        connection_string: &str,
    ) -> Result<Database<TokioRuntime>, Error> {
        Database::from_connection_string_with(
            TokioRuntime::new(),
            connection_string,
            DatabaseOptions::default(),
        )
    }
}

impl<R: Runtime> Database<R> {
    /// Opens the database of a cluster file over `rt`, with `options`: the file at
    /// `cluster_file`, or the one the default lookup finds (`FDB_CLUSTER_FILE`,
    /// `./fdb.cluster`, the platform default). The file is kept up to date when the
    /// coordinators change. Call it inside the executor. Errors: `no_cluster_file_found`
    /// (1515), `connection_string_invalid` (2104), `io_error` (1510), `file_too_large`
    /// (1516), `tls_error` (2107).
    pub fn open_with(
        rt: R,
        cluster_file: Option<&Path>,
        options: DatabaseOptions,
    ) -> Result<Database<R>, Error> {
        let file = ClusterFile::open_or_default(cluster_file)?;
        Database::from_record(rt, ConnectionRecord::from(file), options)
    }

    /// Opens the database of a connection string (kept in memory only) over `rt`, with
    /// `options`. Call it inside the executor. Errors: `connection_string_invalid` (2104),
    /// `tls_error` (2107).
    pub fn from_connection_string_with(
        rt: R,
        connection_string: &str,
        options: DatabaseOptions,
    ) -> Result<Database<R>, Error> {
        let connection_string: ClusterConnectionString = connection_string.parse()?;
        Database::from_record(rt, ConnectionRecord::memory(connection_string), options)
    }

    /// The database of `record` (a cluster file, or a connection string in memory), over
    /// `rt`, with `options`. Call it inside the executor.
    ///
    /// Lower tier: [`ConnectionRecord`] is the cluster layer's, and follows its stability
    /// (see the crate's [API design notes](crate#api-design-notes)); [`Database::open_with`]
    /// and [`Database::from_connection_string_with`] are the application API's.
    pub fn from_record(
        rt: R,
        record: ConnectionRecord,
        options: DatabaseOptions,
    ) -> Result<Database<R>, Error> {
        let tls = ClientTls::for_cluster(record.connection_string(), &options)?;
        let uses_tls = tls.is_enabled();
        let peers = PeerManager::with_stream_wrapper(rt.clone(), options.peers, tls, DiscardEvents);
        let monitor = Arc::new(ClusterMonitor::start(
            rt.clone(),
            peers.clone(),
            record,
            options.monitor,
        ));
        let reader = Reader::new(rt.clone(), peers.clone(), monitor.clone(), options.read);
        let knobs = TransactionKnobs {
            initial_backoff: options.commit.initial_backoff,
            max_backoff: options.commit.max_backoff,
            resource_constrained_max_backoff: options.commit.resource_constrained_max_backoff,
            backoff_growth: options.commit.backoff_growth,
            future_version_retry_delay: options.commit.future_version_retry_delay,
        };
        let watches = WatchContext::new(
            rt.clone(),
            reader.clone(),
            &options.watch,
            options.commit.future_version_retry_delay,
        );
        let rng = Rng::new(options.commit.seed.map(|s| s ^ 0x5eed));
        let committer = Committer::new(rt.clone(), peers.clone(), monitor.clone(), options.commit);
        Ok(Database {
            inner: Arc::new(DbInner {
                rt,
                peers,
                monitor,
                reader,
                committer,
                watches,
                knobs,
                transaction_defaults: Mutex::new(Arc::new(Vec::new())),
                snapshot_ryw: AtomicI32::new(1),
                tls: uses_tls,
                rng: Mutex::new(rng),
            }),
        })
    }

    /// A new transaction (`createTransaction`), with the database's [default
    /// options](Database::set_option).
    #[must_use]
    pub fn create_transaction(&self) -> Transaction<R> {
        Transaction::new(self.clone())
    }

    /// Runs `f` in a transaction, commits it, and retries both as C++'s retry loops do
    /// (`fdb_transaction_on_error` in a loop): on an error that
    /// [`Transaction::on_error`] retries (a conflict, `commit_unknown_result`,
    /// `transaction_too_old`, `future_version`, throttling, ...) the transaction is reset,
    /// waits its backoff, and `f` runs again; any other error (from `f`, the commit, or
    /// `on_error`, such as a timeout or the retry limit) is returned.
    ///
    /// `f` gets a handle to the transaction (cheap: a clone of the one the loop uses), and
    /// [`MaybeCommitted`]: whether an earlier attempt may have committed. The handle belongs
    /// to the attempt: once it is over, what is done with it fails with
    /// `transaction_cancelled` (its writes fail the attempt in progress instead of slipping
    /// into it; see [Handles of a retry loop](crate::transaction#handles-of-a-retry-loop)).
    /// `f` should not commit itself: `run` commits, and a second commit is
    /// `used_during_commit` (reported as [maybe committed](Error::is_maybe_committed), since
    /// the first one may have committed).
    ///
    /// # Idempotency
    ///
    /// **A transaction that ends in `commit_unknown_result` may have committed, and `run`
    /// retries it.** Like the C++ retry loops, `run` cannot know whether the attempt took
    /// effect, so `f` should be idempotent: writing the same values again is harmless, but
    /// an increment is not. A non-idempotent `f` must check whether its earlier attempt
    /// committed (for example by also writing a unique marker key and reading it first),
    /// which [`MaybeCommitted`] tells it to do. If `run` gives up after an attempt that may
    /// have committed, the error it returns
    /// [`is_maybe_committed`](Error::is_maybe_committed), whatever its code.
    ///
    /// Options set in `f` apply to that attempt only, except those that C++ keeps across
    /// retries (`timeout`, `retry_limit`, `max_retry_delay`); setting them on every attempt
    /// is fine. The transaction starts with the [database's defaults](Database::set_option),
    /// which also stay across the retries.
    ///
    /// ```no_run
    /// # async fn example(db: fdb_native_client::Database) -> Result<(), fdb_native_client::Error> {
    /// let previous = db
    ///     .run(|tx, _maybe_committed| async move {
    ///         let previous = tx.get(b"hello").await?;
    ///         tx.set(b"hello", b"world");
    ///         Ok(previous)
    ///     })
    ///     .await?;
    /// # let _ = previous;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run<F, Fut, T>(&self, mut f: F) -> Result<T, Error>
    where
        F: FnMut(Transaction<R>, MaybeCommitted) -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        let tx = self.create_transaction();
        let mut maybe_committed = false;
        loop {
            let attempt = match f(tx.for_attempt(), MaybeCommitted(maybe_committed)).await {
                Ok(value) => tx.commit().await.map(|_| value),
                Err(e) => Err(e),
            };
            let error = match attempt {
                Ok(value) => return Ok(value),
                Err(e) => e,
            };
            // Before `on_error`, which may return another error (a timeout, the retry
            // limit's): whatever `run` returns from now on may hide a commit.
            maybe_committed |= error.is_maybe_committed();
            if let Err(e) = tx.on_error(error).await {
                return Err(if maybe_committed {
                    e.into_maybe_committed()
                } else {
                    e
                });
            }
        }
    }

    /// The cluster monitor (the proxies, the connection string, the cluster's version).
    ///
    /// This and [`reader`](Self::reader), [`committer`](Self::committer) and
    /// [`peers`](Self::peers) give access to the client's lower layers, for tools and tests:
    /// their types follow those layers' stability, not the application API's (see the
    /// crate's [API design notes](crate#api-design-notes)).
    pub fn cluster_monitor(&self) -> &ClusterMonitor<R> {
        &self.inner.monitor
    }

    /// The read path the transactions use.
    pub fn reader(&self) -> &Reader<R, ClientTls> {
        &self.inner.reader
    }

    /// The commit path the transactions use.
    pub fn committer(&self) -> &Committer<R, ClientTls> {
        &self.inner.committer
    }

    /// The connections (the transport's `PeerManager`; lower tier, see
    /// [`cluster_monitor`](Self::cluster_monitor)).
    pub fn peers(&self) -> &PeerManager<R, ClientTls> {
        &self.inner.peers
    }

    /// Stops the cluster monitor and closes every connection, and waits for both. Every
    /// handle of the database (and its transactions) then fails with `operation_cancelled`
    /// (1101). Idempotent.
    ///
    /// None of the database's tasks outlives the call, also while handles of it (or of its
    /// transactions) are still held: the read version batchers and the watches stop at once
    /// ([`Reader::shutdown`](crate::read::Reader::shutdown)), and a read version request after
    /// it fails at once with `operation_cancelled` instead of starting one.
    pub async fn shutdown(&self) {
        // The watches fail with `operation_cancelled` (the cluster monitor and the
        // connections would only end them eventually), and so do the read version requests
        // (the batchers would otherwise live as long as a handle of the database).
        self.inner.watches.shutdown();
        self.inner.reader.shutdown();
        self.inner.monitor.shutdown().await;
        self.inner.peers.shutdown().await;
    }

    /// `setOption(option)` of the database (`fdb_database_set_option`): see
    /// [`DatabaseOption`] for the options and the
    /// transaction defaults. `invalid_option_value` (2006) for a value out of range (the
    /// options are unchanged then). Every option [`DatabaseOption`] offers is supported, so
    /// this never fails with `invalid_option` (2007): the options this client does not
    /// support have no variant.
    ///
    /// ```no_run
    /// # fn example(db: &fdb_native_client::Database) -> Result<(), fdb_native_client::Error> {
    /// use std::time::Duration;
    /// use fdb_native_client::DatabaseOption;
    ///
    /// // Every transaction from now on times out after 5 s, and retries 10 times at most.
    /// db.set_option(DatabaseOption::TransactionTimeout(Duration::from_secs(5)))?;
    /// db.set_option(DatabaseOption::TransactionRetryLimit(10))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_option(&self, option: DatabaseOption) -> Result<(), Error> {
        match option {
            DatabaseOption::LocationCacheSize(size) => {
                // `extractIntOption(value, 0, std::numeric_limits<int>::max())`.
                if size > i32::MAX as usize {
                    return Err(Error::with_message(
                        crate::error::codes::INVALID_OPTION_VALUE,
                        "the location cache size is at most i32::MAX",
                    ));
                }
                self.inner.reader.set_location_cache_size(size);
                return Ok(());
            }
            DatabaseOption::MaxWatches(max) => return self.set_max_watches(max),
            DatabaseOption::MachineId(id) => {
                self.inner.reader.set_client_zone_id(id);
                return Ok(());
            }
            DatabaseOption::DatacenterId(id) => {
                self.inner.reader.set_client_dc_id(id);
                return Ok(());
            }
            DatabaseOption::SnapshotRywEnable => {
                self.inner.snapshot_ryw.fetch_add(1, Ordering::SeqCst);
                return Ok(());
            }
            DatabaseOption::SnapshotRywDisable => {
                self.inner.snapshot_ryw.fetch_sub(1, Ordering::SeqCst);
                return Ok(());
            }
            _ => {}
        }
        let Some(default) = option.default_for() else {
            return Ok(());
        };
        default.validate(self.inner.committer.limits().transaction_size_limit)?;
        let mut defaults = self
            .inner
            .transaction_defaults
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // The same option again replaces the earlier default.
        let kind = std::mem::discriminant(&default);
        let mut next: Vec<TransactionOption> = defaults
            .iter()
            .filter(|o| std::mem::discriminant(*o) != kind)
            .cloned()
            .collect();
        next.push(default);
        *defaults = Arc::new(next);
        Ok(())
    }

    /// `MAX_WATCHES` (the database option): the most watches that may be outstanding at once
    /// (default 10 000; [`WatchConfig::max_outstanding_watches`]). Watches already
    /// outstanding stay, whatever the new cap. `invalid_option_value` (2006) above
    /// [`ABSOLUTE_MAX_WATCHES`] (1 000 000). Also [`DatabaseOption::MaxWatches`].
    pub fn set_max_watches(&self, max: usize) -> Result<(), Error> {
        if max > ABSOLUTE_MAX_WATCHES {
            return Err(Error::with_message(
                crate::error::codes::INVALID_OPTION_VALUE,
                "the cap on watches is at most 1 000 000",
            ));
        }
        self.inner.watches.set_max(max);
        Ok(())
    }

    /// How many watches are outstanding now: those returned by
    /// [`Transaction::watch`] that have not resolved, failed or been dropped.
    pub fn outstanding_watches(&self) -> usize {
        self.inner.watches.outstanding()
    }

    /// How many keys the database is watching on the storage servers: one request serves all
    /// the watches of a key and value (C++'s `WatchMapSize`).
    pub fn watched_keys(&self) -> usize {
        self.inner.watches.map_size()
    }

    /// Whether [`shutdown`](Self::shutdown) took effect.
    pub fn is_shut_down(&self) -> bool {
        self.inner.monitor.is_closed() || self.inner.peers.is_shut_down()
    }
}
