//! [`Transaction`]: reads at one read version, buffered writes, and a commit; with C++'s
//! `onError` retry semantics, its options, timeouts and cancellation.
//!
//! This is C++'s `ReadYourWritesTransaction` over its `Transaction`:
//!
//! * **Reads see the transaction's own writes** (read-your-writes, the default of every FDB
//!   binding). [`get`](Transaction::get), [`get_key`](Transaction::get_key) and
//!   [`get_range`](Transaction::get_range) (and its streams,
//!   [`get_ranges`](Transaction::get_ranges) and
//!   [`get_ranges_keyvalues`](Transaction::get_ranges_keyvalues)) read the database at the transaction's read version with the transaction's
//!   [`set`](Transaction::set)s, [`clear`](Transaction::clear)s and atomic operations
//!   applied, and remember what they read (a snapshot cache), so that reading the same keys
//!   again does not ask the cluster again. Key selectors resolve through the writes too.
//!   Keys that a versionstamped operation of the transaction may write are unreadable until
//!   the commit: reading them fails with `accessed_unreadable` (1036). The code is a port of
//!   `ReadYourWrites.actor.cpp` (`RYWImpl`, `RYWIterator`, `WriteMap`, `SnapshotCache`). A
//!   read sees the writes made **before
//!   it was called** (the future it returns has taken its view already, as C++'s read
//!   actors do).
//! * **Read conflict ranges are C++'s** (`RYWImpl::addConflictRange`): a serializable read
//!   conflicts on the keys whose database value it depended on, not on the keys the
//!   transaction had set or cleared before it (which it read from its own writes); a
//!   snapshot read ([`snapshot`](Transaction::snapshot)) adds none. The commit sends their
//!   union, coalesced, as C++'s `readConflicts` map does. Each write adds its key or range
//!   as a write conflict range; so a transaction whose reads were changed by another
//!   transaction that committed after its read version fails with `not_committed` (1020),
//!   and [`Database::run`] retries it.
//! * **The commit sends the write map** (`writeRangeToNativeTransaction`,
//!   `ReadYourWrites.actor.cpp` 7.4:1997-2072): the writes are kept in the write map only,
//!   coalesced there (a set replaces the key's earlier writes, a clear drops the writes in
//!   its range, atomic operations fold into a set or into each other), and the commit sends
//!   what is left, as C++ does: the cleared ranges first, then each key's operations in key
//!   order, then the write conflict ranges as the maximal ranges of keys written with one,
//!   then the read conflict ranges. A `SetVersionstampedKey` sends the writes already made
//!   in the range of keys it may take ahead of it. The size the `SizeLimit` option and
//!   `transaction_too_large` (2101) apply to is the size of what is sent, so a transaction
//!   that writes a key many times counts it once.
//!   [`approximate_size`](Transaction::approximate_size) is C++'s running estimate
//!   (`getApproximateSize`).
//! * **Options.** [`ReadYourWritesDisable`](TransactionOption::ReadYourWritesDisable) turns
//!   all this off (before the first read or write): reads go to the database as C++'s
//!   `readThrough` does, with the read conflict ranges of C++'s `Transaction`; the writes
//!   go to C++'s native transaction (here a [`CommitBuffer`]) and are sent as they were
//!   made; [`watch`](Transaction::watch) is `watches_disabled` (1034).
//!   [`SnapshotRywDisable`](TransactionOption::SnapshotRywDisable) makes snapshot reads
//!   ignore the transaction's writes (they still use and fill the cache).
//!   [`BypassUnreadable`](TransactionOption::BypassUnreadable) lets `get` read unreadable
//!   keys.
//! * **The read version** is requested lazily, by the first read that needs the cluster (or
//!   [`get_read_version`](Transaction::get_read_version)), once for the transaction: the
//!   request runs as its own task on the runtime and every read waiting for it gets its
//!   answer, whichever of them is still being polled. [`set_read_version`](Transaction::set_read_version)
//!   supplies one instead. A transaction that only writes gets one at commit, with
//!   `CAUSAL_READ_RISKY`, as C++'s `commitMutations` does.
//! * **Errors of writes are deferred.** `set`, `clear`, ... return nothing; an error they
//!   meet (`key_too_large`, `key_outside_legal_range`, ...) is kept and returned by the next
//!   read or the commit, as `fdb_c`'s deferred error.
//! * **A commit waits for the reads in progress.** As C++'s read-your-writes layer counts
//!   every read in `reading` and makes the commit wait for them (`ReadYourWrites.actor.cpp`
//!   7.4:1359-1360, 8.0:1351-1352), the commit waits for the reads (`get`, `get_key`,
//!   `get_range`, range stream batches, snapshot or not) and `watch` reads called before it,
//!   whether they succeed or fail, before it takes the conflict ranges: a read adds its
//!   range when it ends (a read that fails adds none). **Each read runs as a task of its
//!   own, started when it is called**, as a C++ read actor runs to its end: a read future
//!   that is dropped, or kept but never polled (the loser of a `select`), does not hold a
//!   commit up; its read still ends and adds its range. A read called after the commit
//!   started fails with `used_during_commit`. With read-your-writes disabled, a `get` adds
//!   its range when it is called and the ranges of `get_key` and `get_range` are C++'s
//!   `extraConflictRanges`, which do not count toward the transaction's size (the
//!   `SizeLimit` option and `transaction_too_large`); with read-your-writes, every read
//!   conflict range counts, as C++'s commit adds them with `addReadConflictRange`.
//!
//! * **Storage metrics** ([`get_estimated_range_size_bytes`](Transaction::get_estimated_range_size_bytes),
//!   [`get_range_split_points`](Transaction::get_range_split_points)) pass through to the
//!   storage servers' estimates, as in C++: no read version, no conflict range, not waited
//!   for by the commit, and the transaction's writes are not in them.
//!
//! * **Watches** ([`watch`](Transaction::watch)) have the read-your-writes layer's semantics:
//!   the watch works out the key's value with the transaction's own writes
//!   (atomic operations included, with a read conflict range when the database's value
//!   counts), starts when the transaction commits (a read-only one too) and fails if it does
//!   not; the transaction's later writes trigger it; see [`crate::watch`].
//!
//! # Retries: [`on_error`](Transaction::on_error)
//!
//! `Transaction::onError` and the read-your-writes layer's retry bookkeeping (NativeAPI
//! 7.4:8112-8167, `ReadYourWrites.actor.cpp` 7.4:1499-1534; the same on 8.0):
//!
//! * The retry limit (`RETRY_LIMIT`) is checked first: once the transaction has been retried
//!   that many times, the error is returned.
//! * An error `onError` retries ([`Error::transaction_retry`]) resets the transaction and
//!   waits: for most, the **backoff**: a random fraction of the current backoff (`backoff *
//!   random01()`), after which the backoff grows by `BACKOFF_GROWTH_RATE` (2) up to the max
//!   retry delay (`MAX_RETRY_DELAY`, default `DEFAULT_MAX_BACKOFF` 1 s), or up to
//!   `RESOURCE_CONSTRAINED_MAX_BACKOFF` (30 s) for the resource-constrained errors; it
//!   starts at `DEFAULT_BACKOFF` (10 ms) and only a full [`reset`](Transaction::reset)
//!   brings it back. For `transaction_too_old` and `future_version`, `FUTURE_VERSION_RETRY_DELAY`
//!   (10 ms, capped by the max retry delay).
//! * Any other error is returned, and the transaction is cancelled: it fails with
//!   `transaction_cancelled` (1025) until it is [`reset`](Transaction::reset).
//! * The reset keeps the options C++ keeps (the "persistent" ones of `fdb.options`:
//!   `timeout`, `retry_limit`, `max_retry_delay`), the retry count, the backoff and the time
//!   the timeout counts from; it forgets the read version, the writes and conflict ranges,
//!   the deferred error and the other options (`size_limit` among them), except that the
//!   [database's defaults](crate::DatabaseOption) are applied again. See
//!   [`TransactionOption`] for every option.
//! * **An error that may hide a commit stays so.** If the error given to `on_error`
//!   [`is_maybe_committed`](Error::is_maybe_committed), whatever `on_error` returns instead
//!   (a timeout during the backoff, the transaction's cancellation) is too.
//!
//! # Timeouts and cancellation
//!
//! The `timeout` option bounds the transaction's life from its creation (or its last
//! [`reset`](Transaction::reset); not from [`on_error`](Transaction::on_error)): once it
//! has passed, every operation, including one in progress and `on_error`, fails with
//! `transaction_timed_out` (1031) until the transaction is reset. Setting the option applies
//! to the operations in progress too (at once if the deadline has already passed). As with
//! C++'s timer, a deadline that has passed has killed the transaction even if nothing ran
//! then: changing or removing the timeout afterwards does not bring it back, and a later
//! [`cancel`](Transaction::cancel) leaves the error `transaction_timed_out`.
//! [`cancel`](Transaction::cancel) does the same with `transaction_cancelled` (1025), and a
//! [`reset`](Transaction::reset) (or `on_error`) makes the operations in progress fail with
//! `transaction_cancelled`. The future of [`get_versionstamp`](Transaction::get_versionstamp)
//! is ended by all of these too, when they happen (the timeout does not wait for an
//! operation to notice it).
//!
//! **A commit that times out or is cancelled while its request may be on its way may still
//! commit.** C++ reports `transaction_timed_out` / `transaction_cancelled`; here that error
//! is also [`is_maybe_committed`](Error::is_maybe_committed), so that it is not mistaken for
//! a commit that did not happen. So is the `used_during_commit` of an operation after a
//! commit that may have been sent.
//!
//! # Handles of a retry loop
//!
//! The handle [`Database::run`] gives its closure belongs to that attempt. Once the attempt
//! is over (the loop called `on_error`), the handle is stale: its reads, `commit` and
//! `on_error` fail with `transaction_cancelled` (1025), its `set_option` too, and its writes
//! (`set`, `clear`, ..., `set_read_version`) become a deferred `transaction_cancelled` of
//! the attempt in progress, which then fails, rather than slip into it; its `cancel` and
//! `reset` do nothing. A handle from [`Database::create_transaction`] (or a clone of it) is
//! never stale.
//!
//! # Options
//!
//! [`Transaction::set_option`] takes a [`TransactionOption`]: the timeout, retry and size
//! limits, the read version priorities and `max_grv_queue_delay`, `lock_aware` and
//! `read_lock_aware`, the causal options, the system key access options and the rest of
//! `fdb.options` that make sense for this client (a table says which are supported and
//! which are accepted as no-ops). [`Database::set_option`] sets the defaults new
//! transactions start with.
//!
//! # `\xff/metadataVersion`
//!
//! A [`get`](Transaction::get) of `\xff/metadataVersion` needs no system key option, and is
//! answered without a storage server when it can be (NativeAPI 7.4:5860-5897): from the GRV
//! proxy's reply to this transaction's read version request, else from what the database
//! remembers of recent read versions and commits ([`Reader::metadata_version`](crate::read::Reader::metadata_version)). A commit
//! tells the database the metadata version at its commit version (`tryCommit`,
//! 7.4:6990-6996), so that a transaction that reads at that version knows it.
//!
//! The key is written only as C++'s read-your-writes layer allows (7.4:2226-2230, 2300):
//! [`atomic_op`](Transaction::atomic_op) with `SetVersionstampedValue` and the 14-byte
//! value `metadataVersionRequiredValue` (all zeros), with or without the system key
//! options; any other atomic operation on it, and a [`set`](Transaction::set) of it, is a
//! (deferred) `client_invalid_operation` (2000). Once written, the key is unreadable in the
//! transaction (`accessed_unreadable`), as any versionstamped value.
//!
//! # Not ported yet
//!
//! Mapped range reads (`getMappedRange`), `getRangeStream` (the storage servers' streaming
//! reads), the special key space (`\xff\xff`), transaction tags, idempotency ids, tenants
//! and the read options sent to storage servers (read priority, server side cache).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use fdb_native_schema::domain::{INVALID_VERSION, KeyRange, KeySelector};
#[cfg(feature = "tokio")]
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_transport::runtime::{Instant, Runtime};
use futures_channel::oneshot;

use crate::commit::{CommitBuffer, CommitLimits, CommitOptions, CommitOutcome};
use crate::database::Database;
use crate::error::codes::*;
use crate::error::{Error, TransactionRetry};
use crate::read::util::{Either, never, race};
use crate::read::{Limits, METADATA_VERSION_KEY, RangeRead, ReadVersionOptions, key_after};
use crate::ryw::read::{BoxFuture, RywRead, RywSource};
use crate::ryw::{self, ReadConflicts, SnapshotCache, WriteMap, WriteMapIter};
use crate::watch::PendingWatches;

mod metrics;
mod mutation;
pub mod options;
mod range;
mod watch;
mod write;

pub use mutation::MutationType;
pub use options::TransactionOption;
use options::{Flags, Persistent};
pub use range::{KeyValueStream, RangeResult, RangeStream};

/// `metadataVersionRequiredValue`: the only operand of a write of `\xff/metadataVersion`
/// (a `SetVersionstampedValue` whose stamp goes at offset 0).
const METADATA_VERSION_REQUIRED_VALUE: &[u8] = &[0; 14];

/// The first key a transaction may not read or write without the system-key options.
const SYSTEM_KEYS_BEGIN: &[u8] = b"\xff";
/// The end of the system keys (and the first key of the special key space).
const SYSTEM_KEYS_END: &[u8] = b"\xff\xff";

/// What a successful [`Transaction::commit`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CommitResult {
    version: i64,
    versionstamp: Option<[u8; 10]>,
}

impl CommitResult {
    /// `getCommittedVersion()`: the version the transaction committed at, or `-1` if it had
    /// nothing to write (a read-only transaction commits without contacting the cluster).
    pub fn committed_version(&self) -> i64 {
        self.version
    }

    /// `getVersionstamp()`: the 10-byte versionstamp of the commit (the commit version and
    /// the transaction's order in its batch); `None` for a transaction with nothing to
    /// write.
    pub fn versionstamp(&self) -> Option<[u8; 10]> {
        self.versionstamp
    }
}

impl From<CommitOutcome> for CommitResult {
    fn from(outcome: CommitOutcome) -> CommitResult {
        CommitResult {
            version: outcome.version(),
            versionstamp: outcome.versionstamp(),
        }
    }
}

macro_rules! transaction_struct {
    ($($default:ty)?) => {
        /// A transaction on a [`Database`]. See the [module documentation](self) for what
        /// it does (and does not do yet).
        ///
        /// A handle: clones share the transaction (as the closure of [`Database::run`]
        /// gets one). Every method takes `&self`; reads may run concurrently.
        ///
        /// ```no_run
        /// # async fn example(db: fdb_native_client::Database) -> Result<(), fdb_native_client::Error> {
        /// use fdb_native_client::{KeySelector, RangeOptions};
        ///
        /// let tx = db.create_transaction();
        /// // Two reads at once; each adds a read conflict range.
        /// let (a, b) = (tx.get(b"a"), tx.get(b"b"));
        /// let (a, b) = (a.await?, b.await?);
        /// // Reads see the transaction's writes.
        /// tx.set(b"c", b"3");
        /// let range = tx
        ///     .get_range(
        ///         &KeySelector::first_greater_or_equal(b"a".to_vec()),
        ///         &KeySelector::first_greater_or_equal(b"d".to_vec()),
        ///         &RangeOptions::default(),
        ///     )
        ///     .await?;
        /// assert!(range.kvs.iter().any(|kv| kv.key == b"c"));
        /// tx.commit().await?;
        /// # let _ = (a, b);
        /// # Ok(())
        /// # }
        /// ```
        pub struct Transaction<R: Runtime $(= $default)?> {
            inner: Arc<Inner<R>>,
            /// For a handle of a [`Database::run`] attempt: the epoch of that attempt.
            attempt: Option<u64>,
        }
    };
}

#[cfg(feature = "tokio")]
transaction_struct!(TokioRuntime);
#[cfg(not(feature = "tokio"))]
transaction_struct!();

struct Inner<R: Runtime> {
    db: Database<R>,
    state: Mutex<State>,
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The read version, shared by the transaction's reads.
#[derive(Debug)]
enum ReadVersion {
    Unset,
    /// Being requested (by a task of its own); those waiting for it.
    Fetching(Vec<oneshot::Sender<Result<i64, Error>>>),
    Set(i64),
    Failed(Error),
}

#[derive(Debug)]
enum CommitStage {
    Idle,
    /// Started; whether the request may have reached a proxy.
    Started(Arc<AtomicBool>),
    Committed(CommitResult),
}

/// What wakes an operation in progress.
#[derive(Debug)]
enum Interrupt {
    /// It fails with this error.
    Abort(Error),
    /// The timeout changed: look at the deadline again.
    Deadline,
}

struct State {
    /// Bumped by every reset (`on_error` or a full one): what an operation of an earlier
    /// epoch finds out is dropped, and the handles of earlier `run` attempts are stale.
    epoch: u64,
    read_version: ReadVersion,
    /// `\xff/metadataVersion` at the read version, as the GRV reply that gave it reported it
    /// (`TransactionState::metadataVersion`): set once a read version was requested; not for
    /// one from [`Transaction::set_read_version`].
    metadata: Option<Option<Vec<u8>>>,
    buffer: CommitBuffer,
    deferred: Option<Error>,
    flags: Flags,
    persistent: Persistent,
    /// The database's default options (`DatabaseContext::transactionDefaults`) when the
    /// transaction was created or fully reset: applied then, and again (the non-persistent
    /// ones) after every [`on_error`](Transaction::on_error).
    defaults: Arc<Vec<TransactionOption>>,
    /// The read-your-writes layer: the writes, the cache of what was read, the read
    /// conflict ranges.
    ryw: RywState,
    retries: u32,
    backoff: Duration,
    /// What the timeout counts from.
    created: Instant,
    /// Cancelled or timed out (or poisoned by a use during a commit): every operation fails
    /// with this until a full reset.
    dead: Option<Error>,
    commit: CommitStage,
    /// The operations in progress, to interrupt.
    interrupts: Vec<oneshot::Sender<Interrupt>>,
    /// The futures of [`Transaction::get_versionstamp`] that wait for the commit.
    versionstamp_waiters: Vec<oneshot::Sender<Result<[u8; 10], Error>>>,
    /// The watches to start when the transaction commits ([`Transaction::watch`]).
    watches: PendingWatches,
    /// The reads in progress (the read-your-writes layer's `reading`).
    reading: Reading,
}

/// The read-your-writes layer's state (`ReadYourWritesTransaction`'s `writes`, `cache`,
/// `readConflicts` and `approximateSize`), which `resetRyow` starts over.
#[derive(Debug, Default)]
struct RywState {
    /// The writes, with read-your-writes: what the reads see and what the commit sends
    /// ([`ryw::commit`]). With read-your-writes disabled the writes go to the commit buffer
    /// (the native transaction) instead, and this stays empty.
    writes: WriteMap,
    /// What reads found out of the database at the read version.
    cache: SnapshotCache,
    /// The read conflict ranges of the reads (and of `add_read_conflict_range`), with what
    /// they add to the approximate size.
    read_conflicts: ReadConflicts,
    /// `approximateSize`, but for what the read conflict ranges add (kept with them).
    approximate_size: u64,
}

/// The read-your-writes layer's `reading` (`ReadYourWrites.actor.cpp` 7.4:1707, 8.0:1694):
/// the reads in progress, which the commit waits for before it takes the
/// conflict ranges (7.4:1359-1360, 8.0:1351-1352), so that a read that started before the
/// commit is in it whenever it ends.
#[derive(Debug, Default)]
struct Reading {
    /// The reads in progress ([`ReadGuard`]s).
    count: usize,
    /// The commits waiting for `count` to be 0.
    waiters: Vec<oneshot::Sender<()>>,
    /// The commit took the buffer: a read that ends now cannot be in it.
    closed: bool,
}

impl Reading {
    fn end_one(&mut self) {
        self.count = self.count.saturating_sub(1);
        if self.count == 0 {
            for waiter in self.waiters.drain(..) {
                let _ = waiter.send(());
            }
        }
    }
}

/// A read in progress, counted in [`Reading`] until it is dropped: when it
/// ended, after adding its conflict range ([`Transaction::add_read_conflict`]), or failed or
/// was abandoned (it adds nothing then).
struct ReadGuard<R: Runtime> {
    inner: Weak<Inner<R>>,
    epoch: u64,
}

impl<R: Runtime> Drop for ReadGuard<R> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let mut state = lock(&inner.state);
            if state.epoch == self.epoch {
                state.reading.end_one();
            }
        }
    }
}

impl State {
    fn new<R: Runtime>(db: &Database<R>, epoch: u64) -> State {
        let knobs = &db.inner.knobs;
        let limits = db.inner.committer.limits();
        let mut state = State {
            epoch,
            read_version: ReadVersion::Unset,
            metadata: None,
            buffer: CommitBuffer::new(limits),
            deferred: None,
            flags: Flags::new(
                CommitOptions::default().size_limit,
                db.inner.snapshot_ryw.load(Ordering::SeqCst),
            ),
            persistent: Persistent {
                timeout: None,
                retry_limit: None,
                max_retry_delay: knobs.max_backoff,
            },
            defaults: db.inner.transaction_defaults(),
            ryw: RywState::default(),
            retries: 0,
            backoff: knobs.initial_backoff,
            created: db.inner.rt.now(),
            dead: None,
            commit: CommitStage::Idle,
            interrupts: Vec::new(),
            versionstamp_waiters: Vec::new(),
            watches: PendingWatches::default(),
            reading: Reading::default(),
        };
        // `ReadYourWritesTransaction`'s constructor: the database's defaults, validated when
        // they were set.
        for option in state.defaults.clone().iter() {
            let _ = state.apply_option(option);
        }
        state
    }

    /// Answers the [`Transaction::get_versionstamp`] futures that wait (the promise of
    /// `trState->versionstampPromise` and the `resetPromise` of the read-your-writes layer).
    fn resolve_versionstamp(&mut self, result: Result<[u8; 10], Error>) {
        for waiter in self.versionstamp_waiters.drain(..) {
            let _ = waiter.send(result.clone());
        }
    }

    fn deadline(&self) -> Option<Instant> {
        self.created.checked_add(self.persistent.timeout?)
    }

    /// Fails if the transaction is dead, or its timeout has passed (which kills it).
    fn check_alive(&mut self, now: Instant) -> Result<(), Error> {
        self.fire_timebomb(now);
        match &self.dead {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// `timebomb` (`ReadYourWrites.actor.cpp`): C++ arms a timer that sets `resetPromise`
    /// to `transaction_timed_out` at the deadline, whatever the transaction is doing. Here
    /// the deadline is looked at whenever the transaction is ([`Transaction::state`], and
    /// every wait of an operation in progress): one that has passed kills the transaction
    /// then, before anything else happens to it, as if the timer had fired at the deadline.
    /// So a later change of the timeout (`set_option(Timeout(0))` included), a `cancel` or
    /// a `used_during_commit` does not replace the timeout.
    fn fire_timebomb(&mut self, now: Instant) {
        if self.dead.is_none() && self.deadline().is_some_and(|deadline| deadline <= now) {
            self.kill(Error::new(TRANSACTION_TIMED_OUT));
        }
    }

    /// Makes every operation fail with `error` (the first such error stays) until a full
    /// reset, and interrupts those in progress.
    fn kill(&mut self, error: Error) {
        let error = self.dead.get_or_insert(error).clone();
        self.resolve_versionstamp(Err(error.clone()));
        self.interrupt(|| Interrupt::Abort(error.clone()));
    }

    fn interrupt(&mut self, mut what: impl FnMut() -> Interrupt) {
        for interrupt in self.interrupts.drain(..) {
            let _ = interrupt.send(what());
        }
    }

    /// `resetRyow` and `Transaction::reset` as `onError` does them.
    fn reset_for_retry(&mut self, limits: CommitLimits, snapshot_ryw: i32) {
        self.epoch += 1;
        self.interrupt(|| Interrupt::Abort(Error::new(TRANSACTION_CANCELLED)));
        self.watches.cancel_all(&Error::new(TRANSACTION_CANCELLED));
        self.resolve_versionstamp(Err(Error::new(TRANSACTION_CANCELLED)));
        self.read_version = ReadVersion::Unset;
        self.metadata = None;
        self.buffer = CommitBuffer::new(limits);
        self.deferred = None;
        self.flags = Flags::new(CommitOptions::default().size_limit, snapshot_ryw);
        self.ryw = RywState::default();
        self.commit = CommitStage::Idle;
        self.reading = Reading::default();
        // `applyPersistentOptions`: the persistent options are kept as they are; the
        // database's defaults come back (they are in C++'s `persistentOptions` too).
        for option in self.defaults.clone().iter() {
            if !option.is_persistent() {
                let _ = self.apply_option(option);
            }
        }
    }

    /// The first deferred error stays.
    fn defer(&mut self, error: Error) {
        self.deferred.get_or_insert(error);
    }

    /// `checkUsedDuringCommit`: an operation after the commit started poisons the
    /// transaction with `used_during_commit`, which may hide a commit if the request may
    /// have been sent (or the commit succeeded).
    fn check_not_committing(&mut self) -> Result<(), Error> {
        let Some(e) = self.used_during_commit() else {
            return Ok(());
        };
        // With `USED_DURING_COMMIT_PROTECTION_DISABLE` only the offending operation fails.
        if !self.flags.used_during_commit_protection_disable {
            self.kill(e.clone());
        }
        Err(e)
    }

    /// The `used_during_commit` an operation gets once the commit started (maybe committed
    /// once the request may have been sent, or the commit succeeded), without killing the
    /// transaction; `None` before.
    fn used_during_commit(&self) -> Option<Error> {
        let maybe_committed = match &self.commit {
            CommitStage::Idle => return None,
            CommitStage::Started(sent) => sent.load(Ordering::SeqCst),
            CommitStage::Committed(_) => true,
        };
        let e = Error::new(USED_DURING_COMMIT);
        Some(if maybe_committed {
            e.into_maybe_committed()
        } else {
            e
        })
    }

    /// The read version request's options (`TransactionState::getReadVersion`).
    fn read_version_options(&self, causal_read_risky: bool) -> ReadVersionOptions {
        ReadVersionOptions {
            priority: self.flags.priority,
            causal_read_risky: causal_read_risky || self.flags.causal_read_risky,
            use_provisional_proxies: self.flags.use_provisional_proxies,
            lock_aware: self.flags.lock_aware,
            max_grv_queue_delay_ms: self.flags.max_grv_queue_delay_ms,
        }
    }

    fn commit_options(&self) -> CommitOptions {
        CommitOptions {
            causal_write_risky: self.flags.causal_write_risky,
            lock_aware: self.flags.lock_aware,
            first_in_batch: self.flags.first_in_batch,
            bypass_storage_quota: self.flags.bypass_storage_quota,
            report_conflicting_keys: self.flags.report_conflicting_keys,
            read_only: self.flags.read_only,
            commit_on_first_proxy: self.flags.commit_on_first_proxy,
            use_provisional_proxies: self.flags.use_provisional_proxies,
            size_limit: self.flags.size_limit,
        }
    }
}

impl<R: Runtime> Clone for Transaction<R> {
    fn clone(&self) -> Self {
        Transaction {
            inner: self.inner.clone(),
            attempt: self.attempt,
        }
    }
}

impl<R: Runtime> fmt::Debug for Transaction<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock(&self.inner.state);
        f.debug_struct("Transaction")
            .field("read_version", &state.read_version)
            .field("retries", &state.retries)
            .field("dead", &state.dead)
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

/// Puts the read version back to unset if its task ends without an answer (it panicked,
/// or the runtime dropped it), so that the waiters ask again.
struct FetchGuard<R: Runtime> {
    inner: Weak<Inner<R>>,
    epoch: u64,
    done: bool,
}

impl<R: Runtime> Drop for FetchGuard<R> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        if let Some(inner) = self.inner.upgrade() {
            let mut state = lock(&inner.state);
            if state.epoch == self.epoch && matches!(state.read_version, ReadVersion::Fetching(_)) {
                state.read_version = ReadVersion::Unset;
            }
        }
    }
}

/// What a read takes from the transaction when it is called.
#[derive(Clone, Copy, Debug)]
struct ReadStart {
    epoch: u64,
    use_provisional_proxies: bool,
    max_read_key: &'static [u8],
}

/// What a read-your-writes read sees of the transaction's writes ([`Transaction::ryw_view`]).
struct RywView {
    writes: WriteMapIter,
    bypass_unreadable: bool,
}

impl RywView {
    fn read<'s, S>(&self, source: &'s S, max_read_key: &'static [u8]) -> RywRead<'s, S> {
        RywRead {
            source,
            writes: self.writes.clone(),
            max_read_key,
            bypass_unreadable: self.bypass_unreadable,
        }
    }
}

/// The cache and the snapshot reads of a transaction, for a read-your-writes read
/// ([`RywSource`]): each storage read is a NativeAPI read at the read version, interrupted by
/// the transaction's reset, cancellation and timeout ([`Transaction::guarded`]).
struct TxSource<'t, R: Runtime> {
    tx: &'t Transaction<R>,
    epoch: u64,
    use_provisional_proxies: bool,
}

impl<'t, R: Runtime> TxSource<'t, R> {
    fn new(tx: &'t Transaction<R>, start: &ReadStart) -> TxSource<'t, R> {
        TxSource {
            tx,
            epoch: start.epoch,
            use_provisional_proxies: start.use_provisional_proxies,
        }
    }
}

impl<R: Runtime> RywSource for TxSource<'_, R> {
    fn with_cache<T>(&self, f: impl FnOnce(&mut SnapshotCache) -> T) -> Result<T, Error> {
        let mut state = self.tx.state();
        if state.epoch != self.epoch {
            return Err(Error::new(TRANSACTION_CANCELLED));
        }
        Ok(f(&mut state.ryw.cache))
    }

    fn read_value(&self, key: Vec<u8>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        Box::pin(async move {
            self.tx
                .native_get_value(&key, self.epoch, self.use_provisional_proxies)
                .await
        })
    }

    fn read_range(
        &self,
        begin: KeySelector,
        end: KeySelector,
        limits: Limits,
        reverse: bool,
    ) -> BoxFuture<'_, Result<RangeRead, Error>> {
        Box::pin(async move {
            let tx = self.tx;
            tx.guarded(self.epoch, async {
                let version = tx.read_version(false).await?;
                tx.inner
                    .db
                    .inner
                    .reader
                    .get_range_with_raw_limits(
                        &begin,
                        &end,
                        limits,
                        reverse,
                        version,
                        self.use_provisional_proxies,
                    )
                    .await
            })
            .await?
        })
    }
}

/// The answer of a read task ([`Transaction::spawn_read`]): `transaction_cancelled` if the
/// task ended without one.
struct ReadTask<T>(oneshot::Receiver<Result<T, Error>>);

impl<T> Future for ReadTask<T> {
    type Output = Result<T, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|r| r.unwrap_or_else(|oneshot::Canceled| Err(Error::new(TRANSACTION_CANCELLED))))
    }
}

/// The future of a read: its answer, known when it was called (an error of its checks), or
/// its task's.
enum ReadFuture<T> {
    Ready(Option<Result<T, Error>>),
    Task(ReadTask<T>),
}

impl<T> ReadFuture<T> {
    fn ready(result: Result<T, Error>) -> ReadFuture<T> {
        ReadFuture::Ready(Some(result))
    }
}

// Nothing is pinned through a `ReadFuture`: a ready answer is moved out, a task's receiver
// is `Unpin`.
impl<T> Unpin for ReadFuture<T> {}

impl<T> Future for ReadFuture<T> {
    type Output = Result<T, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut *self {
            ReadFuture::Ready(result) => Poll::Ready(
                result
                    .take()
                    .expect("a read future polled after it completed"),
            ),
            ReadFuture::Task(task) => Pin::new(task).poll(cx),
        }
    }
}

impl<R: Runtime> Transaction<R> {
    pub(crate) fn new(db: Database<R>) -> Transaction<R> {
        let state = State::new(&db, 0);
        Transaction {
            inner: Arc::new(Inner {
                db,
                state: Mutex::new(state),
            }),
            attempt: None,
        }
    }

    /// A handle for one attempt of a retry loop: stale once the transaction is reset.
    pub(crate) fn for_attempt(&self) -> Transaction<R> {
        Transaction {
            inner: self.inner.clone(),
            attempt: Some(self.state().epoch),
        }
    }

    /// The transaction's state, killed first if its deadline has passed
    /// ([`State::fire_timebomb`]).
    fn state(&self) -> MutexGuard<'_, State> {
        let mut state = lock(&self.inner.state);
        state.fire_timebomb(self.rt().now());
        state
    }

    fn rt(&self) -> &R {
        &self.inner.db.inner.rt
    }

    /// Whether this is the handle of an attempt that is over.
    fn is_stale(&self, state: &State) -> bool {
        self.attempt.is_some_and(|epoch| epoch != state.epoch)
    }

    fn stale() -> Error {
        Error::with_message(
            TRANSACTION_CANCELLED,
            "the handle belongs to an earlier attempt of the retry loop",
        )
    }

    /// Fails for a stale handle.
    fn check_handle(&self, state: &State) -> Result<(), Error> {
        if self.is_stale(state) {
            return Err(Self::stale());
        }
        Ok(())
    }

    /// The database.
    pub fn database(&self) -> &Database<R> {
        &self.inner.db
    }

    /// Runs `op` unless the transaction is dead or was reset since `epoch` (the epoch the
    /// operation started in: `transaction_cancelled`), and interrupts it if the transaction
    /// is cancelled, reset or times out meanwhile (`Err` with that error). The deadline is
    /// looked at again whenever the timeout option changes.
    async fn guarded<T>(
        &self,
        epoch: u64,
        op: impl Future<Output = Result<T, Error>>,
    ) -> Result<Result<T, Error>, Error> {
        {
            let mut state = self.state();
            self.check_handle(&state)?;
            state.check_alive(self.rt().now())?;
            if state.epoch != epoch {
                return Err(Error::new(TRANSACTION_CANCELLED));
            }
        }
        let interrupt = async {
            loop {
                let (interrupted, deadline) = {
                    let mut state = self.state();
                    if let Err(e) = state.check_alive(self.rt().now()) {
                        return e;
                    }
                    if state.epoch != epoch {
                        return Error::new(TRANSACTION_CANCELLED);
                    }
                    let (tx, rx) = oneshot::channel();
                    state.interrupts.retain(|s| !s.is_canceled());
                    state.interrupts.push(tx);
                    (rx, state.deadline())
                };
                let timeout = async {
                    match deadline {
                        Some(deadline) => self.rt().sleep_until(deadline).await,
                        None => never().await,
                    }
                };
                match race(interrupted, timeout).await {
                    Either::Left(Ok(Interrupt::Abort(e))) => return e,
                    Either::Left(Err(oneshot::Canceled)) => {
                        return Error::new(TRANSACTION_CANCELLED);
                    }
                    // Look at the deadline (and the rest) again.
                    Either::Left(Ok(Interrupt::Deadline)) | Either::Right(()) => {}
                }
            }
        };
        match race(op, interrupt).await {
            Either::Left(result) => Ok(result),
            Either::Right(e) => Err(e),
        }
    }

    /// The transaction's read version: the one it has, or the one being requested, or a new
    /// one (with `CAUSAL_READ_RISKY` if `causal_read_risky`), shared with every other read.
    /// The request runs as a task of its own, so that no reader depends on another being
    /// polled.
    async fn read_version(&self, causal_read_risky: bool) -> Result<i64, Error> {
        loop {
            let (waiter, epoch) = {
                let mut state = self.state();
                let epoch = state.epoch;
                let (tx, rx) = oneshot::channel();
                match &mut state.read_version {
                    ReadVersion::Set(v) => return Ok(*v),
                    ReadVersion::Failed(e) => return Err(e.clone()),
                    ReadVersion::Fetching(waiters) => waiters.push(tx),
                    ReadVersion::Unset => {
                        let options = state.read_version_options(causal_read_risky);
                        state.read_version = ReadVersion::Fetching(vec![tx]);
                        self.spawn_fetch(epoch, options);
                    }
                }
                (rx, epoch)
            };
            match waiter.await {
                Ok(result) => return result,
                // A reset dropped the request: what waited for it is over.
                Err(oneshot::Canceled) if self.state().epoch != epoch => {
                    return Err(Error::new(TRANSACTION_CANCELLED));
                }
                // Its task ended without an answer: ask again.
                Err(oneshot::Canceled) => {}
            }
        }
    }

    fn spawn_fetch(&self, epoch: u64, options: ReadVersionOptions) {
        let reader = self.inner.db.inner.reader.clone();
        let mut guard = FetchGuard {
            inner: Arc::downgrade(&self.inner),
            epoch,
            done: false,
        };
        // Detached: it ends with the request.
        drop(self.rt().spawn(async move {
            let fetched = reader.get_read_version_and_metadata(options).await;
            guard.done = true;
            let Some(inner) = guard.inner.upgrade() else {
                return;
            };
            let mut state = lock(&inner.state);
            if state.epoch != epoch {
                return;
            }
            let result = match fetched {
                Ok((version, metadata)) => {
                    // `metadataVersion.send(rep.metadataVersion)`.
                    state.metadata = Some(metadata);
                    Ok(version)
                }
                Err(e) => Err(e),
            };
            let next = match &result {
                Ok(v) => ReadVersion::Set(*v),
                Err(e) => ReadVersion::Failed(e.clone()),
            };
            if let ReadVersion::Fetching(waiters) = std::mem::replace(&mut state.read_version, next)
            {
                for waiter in waiters {
                    let _ = waiter.send(result.clone());
                }
            }
        }));
    }

    /// `getReadVersion()`: the transaction's read version, requested from a GRV proxy if it
    /// has none yet.
    pub async fn get_read_version(&self) -> Result<i64, Error> {
        let epoch = {
            let mut state = self.state();
            self.check_handle(&state)?;
            if let Some(e) = state.deferred.clone() {
                return Err(e);
            }
            state.check_not_committing()?;
            state.epoch
        };
        self.guarded(epoch, self.read_version(false)).await?
    }

    /// `setReadVersion(version)`: read at `version` instead of asking for one. A transaction
    /// that already has a read version gets `read_version_already_set` (2010), a version of
    /// 0 or less `version_invalid` (2011); both are deferred (see the [module
    /// documentation](self)). A version the storage servers no longer have makes reads fail
    /// with `transaction_too_old`, one they do not have yet with `future_version`.
    pub fn set_read_version(&self, version: i64) {
        let mut state = self.state();
        if self.is_stale(&state) {
            state.defer(Self::stale());
        } else if !matches!(state.read_version, ReadVersion::Unset) {
            state.defer(Error::new(READ_VERSION_ALREADY_SET));
        } else if version <= 0 {
            state.defer(Error::new(VERSION_INVALID));
        } else {
            state.read_version = ReadVersion::Set(version);
        }
    }

    /// `get(key)`: the value of `key` as the transaction sees it, `None` if it is not set:
    /// the database's value at the read version with the transaction's writes before this
    /// call applied (see the [module documentation](self)).
    ///
    /// The read starts when `get` is called (the future may be dropped or polled later: the
    /// read and its conflict range happen anyway), and sees the writes made before the call,
    /// not those made while it waits.
    ///
    /// * **Read conflict range** (`RYWImpl::addConflictRange`): `[key, key\x00)` unless the
    ///   transaction had set or cleared the key (an atomic operation on a key it had not
    ///   written depends on the database's value: the range is added). With
    ///   [`ReadYourWritesDisable`](TransactionOption::ReadYourWritesDisable), always, as soon
    ///   as `get` is called.
    /// * **Errors.** `key_outside_legal_range` (2004) for a key from `\xff` on (unless the
    ///   `ReadSystemKeys` or `AccessSystemKeys` option is set; `\xff/metadataVersion` is
    ///   always allowed) or from `\xff\xff` on; `accessed_unreadable` (1036) for a key a
    ///   versionstamped operation of the transaction may write or whose value it stamps
    ///   (unless [`BypassUnreadable`](TransactionOption::BypassUnreadable)). A key longer than
    ///   any key can be is `None`, with no conflict range.
    pub fn get(
        &self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Error>> + Send + use<R> {
        self.get_with(key, false)
    }

    /// A view of the transaction whose reads are snapshot reads: they add no read conflict
    /// range.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot<'_, R> {
        Snapshot { tx: self }
    }

    /// The checks every read makes when it is called, in `fdb_c`'s order (the deferred
    /// error first), then what the read needs.
    fn begin_read(&self, state: &mut State) -> Result<ReadStart, Error> {
        self.check_handle(state)?;
        if let Some(e) = state.deferred.clone() {
            return Err(e);
        }
        state.check_alive(self.rt().now())?;
        state.check_not_committing()?;
        Ok(ReadStart {
            epoch: state.epoch,
            use_provisional_proxies: state.flags.use_provisional_proxies,
            max_read_key: state.flags.max_read_key(),
        })
    }

    /// The view of a read-your-writes read called now (`readWithConflictRange`): `None` with
    /// read-your-writes disabled (`readThrough`); the cache alone for a snapshot read when
    /// snapshot reads do not see the writes (`readWithConflictRangeSnapshot`); else the
    /// writes as they are now (`readWithConflictRangeRYW`).
    fn ryw_view(state: &State, snapshot: bool) -> Option<RywView> {
        if state.flags.read_your_writes_disabled {
            return None;
        }
        let writes = if snapshot && state.flags.snapshot_ryw <= 0 {
            WriteMap::new().iter()
        } else {
            state.ryw.writes.iter()
        };
        Some(RywView {
            writes,
            bypass_unreadable: state.flags.bypass_unreadable,
        })
    }

    fn get_with(&self, key: &[u8], snapshot: bool) -> ReadFuture<Option<Vec<u8>>> {
        let (start, view, guard) = {
            let mut state = self.state();
            let start = match self.begin_read(&mut state) {
                Ok(start) => start,
                Err(e) => return ReadFuture::ready(Err(e)),
            };
            // `\xff/metadataVersion` may be read without any system key option.
            if key >= start.max_read_key && key != METADATA_VERSION_KEY {
                return ReadFuture::ready(Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE)));
            }
            if key.len() > state.buffer.limits().max_range_key_size(key) {
                // No key in the database is longer than the limit (C++ does not ask either).
                return ReadFuture::ready(Ok(None));
            }
            let view = Self::ryw_view(&state, snapshot);
            if view.is_none() && !snapshot {
                // NativeAPI's `get` adds the range when it is called. The key is below
                // `\xff\xff`, so the range is not inverted.
                if let Err(e) = state.buffer.add_read_conflict_range(key, &key_after(key)) {
                    return ReadFuture::ready(Err(e));
                }
            }
            (start, view, self.start_read(&mut state))
        };
        let tx = self.clone();
        let key = key.to_vec();
        ReadFuture::Task(self.spawn_read(async move {
            let result = match view {
                None => {
                    tx.native_get_value(&key, start.epoch, start.use_provisional_proxies)
                        .await
                }
                Some(view) => {
                    let source = TxSource::new(&tx, &start);
                    let read = view.read(&source, start.max_read_key);
                    match ryw::read::get(&read, &key).await {
                        Ok(value) if !snapshot => tx
                            .add_ryw_conflict(&guard, |c| {
                                ryw::read::add_get_conflict(c, &key, &read.writes);
                                Ok(())
                            })
                            .map(|()| value),
                        other => other,
                    }
                }
            };
            drop(guard);
            result
        }))
    }

    /// NativeAPI's `Transaction::get(key, Snapshot::True)`: the database's value at the
    /// read version.
    async fn native_get_value(
        &self,
        key: &[u8],
        epoch: u64,
        use_provisional_proxies: bool,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.guarded(epoch, async {
            let version = self.read_version(false).await?;
            if key == METADATA_VERSION_KEY {
                return self
                    .metadata_version(version, epoch, use_provisional_proxies)
                    .await;
            }
            self.inner
                .db
                .inner
                .reader
                .get_with(key, version, use_provisional_proxies)
                .await
        })
        .await?
    }

    /// `Transaction::get` of `\xff/metadataVersion` (NativeAPI 7.4:5860-5897, 8.0:3647-3677):
    /// the value the GRV proxy reported with this transaction's read version, if it asked for
    /// one; else what the database remembers of the read version
    /// ([`Reader::metadata_version`](crate::read::Reader::metadata_version)), which reads the
    /// key from a storage server if it does not know.
    async fn metadata_version(
        &self,
        version: i64,
        epoch: u64,
        use_provisional_proxies: bool,
    ) -> Result<Option<Vec<u8>>, Error> {
        {
            let state = self.state();
            if state.epoch != epoch {
                return Err(Error::new(TRANSACTION_CANCELLED));
            }
            if let Some(metadata) = &state.metadata {
                return Ok(metadata.clone());
            }
        }
        self.inner
            .db
            .inner
            .reader
            .metadata_version_with(version, use_provisional_proxies)
            .await
    }

    /// `getKey(selector)`: the key `selector` resolves to as the transaction sees it (its
    /// writes before this call included: a key it set counts, a key it cleared does not).
    /// The read starts when `get_key` is called, as [`get`](Self::get)'s.
    ///
    /// * **Bounds.** A selector that resolves before the first key gives the empty key; one
    ///   that resolves past the last key the transaction may read gives that bound: `\xff`, or
    ///   `\xff\xff` with the `ReadSystemKeys` or `AccessSystemKeys` option.
    /// * **Errors.** `key_outside_legal_range` (2004) for a selector key after that bound
    ///   (the bound itself is allowed); `accessed_unreadable` (1036) if the resolution meets
    ///   keys a versionstamped operation of the transaction made unreadable; then as
    ///   [`get`](Self::get). A selector key longer than any key can be is shortened first, as
    ///   `KeySelectorRef::setKey` does (to one byte over the limit of `getMaxKeySize`).
    /// * **Read conflict range** (`RYWImpl::addConflictRange(GetKeyReq)`), from the
    ///   selector's key `k` and the result `r`: for an offset of 0 or less, `[r, k)`
    ///   (`[r, keyAfter(k))` if `or_equal`); otherwise `[k, keyAfter(r))`
    ///   (`[keyAfter(k), keyAfter(r))` if `or_equal`), where `keyAfter(k)` is `k\x00` and
    ///   `keyAfter(\xff\xff)` is `\xff\xff`; less the keys the transaction had set or
    ///   cleared; added when the read ends. With
    ///   [`ReadYourWritesDisable`](TransactionOption::ReadYourWritesDisable), NativeAPI's
    ///   (`getKeyAndConflictRange`, 7.4:6077-6092, 8.0:3817-3835): the same from the
    ///   unclamped result, all of it, not counted in the transaction's size
    ///   (`extraConflictRanges`).
    pub fn get_key(
        &self,
        selector: &KeySelector,
    ) -> impl Future<Output = Result<Vec<u8>, Error>> + Send + use<R> {
        self.get_key_with(selector, false)
    }

    fn get_key_with(&self, selector: &KeySelector, snapshot: bool) -> ReadFuture<Vec<u8>> {
        let (start, selector, view, guard) = {
            let mut state = self.state();
            let start = match self.begin_read(&mut state) {
                Ok(start) => start,
                Err(e) => return ReadFuture::ready(Err(e)),
            };
            let selector = self.set_key(&state, selector);
            if selector.key.as_slice() > start.max_read_key {
                return ReadFuture::ready(Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE)));
            }
            let view = Self::ryw_view(&state, snapshot);
            (start, selector, view, self.start_read(&mut state))
        };
        let tx = self.clone();
        ReadFuture::Task(self.spawn_read(async move {
            let result = match view {
                None => {
                    let conflict = (!snapshot).then_some(&guard);
                    tx.native_get_key(
                        &selector,
                        conflict,
                        start.epoch,
                        start.use_provisional_proxies,
                    )
                    .await
                    // `readThrough`: results in keys the transaction may not read are
                    // clamped.
                    .map(|key| {
                        if key.as_slice() > start.max_read_key {
                            start.max_read_key.to_vec()
                        } else {
                            key
                        }
                    })
                }
                Some(view) => {
                    let source = TxSource::new(&tx, &start);
                    let read = view.read(&source, start.max_read_key);
                    match ryw::read::get_key(&read, selector.clone()).await {
                        Ok(key) if !snapshot => tx
                            .add_ryw_conflict(&guard, |c| {
                                ryw::read::add_get_key_conflict(c, &selector, &key, &read.writes)
                            })
                            .map(|()| key),
                        other => other,
                    }
                }
            };
            drop(guard);
            result
        }))
    }

    /// `KeySelectorRef::setKey`: a copy of `selector` whose key is at most one byte longer
    /// than the longest key can be.
    fn set_key(&self, state: &State, selector: &KeySelector) -> KeySelector {
        let mut selector = selector.clone();
        let limit = state.buffer.limits().max_range_key_size(&selector.key);
        selector.key.truncate(limit.saturating_add(1));
        selector
    }

    /// NativeAPI's `Transaction::getKey` (with `getKeyAndConflictRange` for a non-snapshot
    /// read, `guard` set): the unclamped key, and its read conflict range added.
    async fn native_get_key(
        &self,
        selector: &KeySelector,
        guard: Option<&ReadGuard<R>>,
        epoch: u64,
        use_provisional_proxies: bool,
    ) -> Result<Vec<u8>, Error> {
        let key = self
            .guarded(epoch, async {
                let version = self.read_version(false).await?;
                self.inner
                    .db
                    .inner
                    .reader
                    .get_key_with(selector, version, use_provisional_proxies)
                    .await
            })
            .await??;
        if let Some(guard) = guard {
            let k = &selector.key;
            let (begin, end) = if selector.offset <= 0 {
                let end = if selector.or_equal {
                    key_after(k)
                } else {
                    k.clone()
                };
                (key.clone(), end)
            } else {
                let begin = if selector.or_equal {
                    key_after(k)
                } else {
                    k.clone()
                };
                (begin, key_after(&key))
            };
            self.add_read_conflict(guard, KeyRange { begin, end })?;
        }
        Ok(key)
    }

    /// Runs `read`, a read that waits for the cluster and adds its read conflict range,
    /// holding its [`ReadGuard`], as a task of its own, started now: as a C++ read actor, it
    /// ends (and lets a waiting commit go on) whether or not the future that started it is
    /// still polled or kept. The result goes to the [`ReadTask`] this returns.
    fn spawn_read<T: Send + 'static>(
        &self,
        read: impl Future<Output = Result<T, Error>> + Send + 'static,
    ) -> ReadTask<T> {
        let (tx, rx) = oneshot::channel();
        // Detached: it ends with the read (which the transaction's reset, cancellation or
        // timeout interrupts).
        drop(self.rt().spawn(async move {
            let _ = tx.send(read.await);
        }));
        ReadTask(rx)
    }

    /// Adds the read conflict ranges of a read-your-writes read that `guard` counts (`f`
    /// adds them to the transaction's `readConflicts`). Fails as
    /// [`add_read_conflict`](Self::add_read_conflict) does.
    fn add_ryw_conflict(
        &self,
        guard: &ReadGuard<R>,
        f: impl FnOnce(&mut ReadConflicts) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let mut state = self.state();
        if state.epoch != guard.epoch {
            return Err(Error::new(TRANSACTION_CANCELLED));
        }
        if state.reading.closed {
            state.check_not_committing()?;
            return Err(Error::new(USED_DURING_COMMIT));
        }
        f(&mut state.ryw.read_conflicts)
    }

    /// Counts a read in progress (`reading.add`) until the guard is dropped.
    fn start_read(&self, state: &mut State) -> ReadGuard<R> {
        state.reading.count += 1;
        ReadGuard {
            inner: Arc::downgrade(&self.inner),
            epoch: state.epoch,
        }
    }

    /// Adds the read conflict range of a `getKey` or `getRange` that `guard` counts (C++'s
    /// `extraConflictRanges`; an empty range is left out). The commit waits for counted
    /// reads before it takes the ranges, so this fails only if the transaction was reset
    /// (`transaction_cancelled`) or, defensively, if the ranges were taken anyway
    /// (`used_during_commit`): a read never succeeds without its range.
    fn add_read_conflict(&self, guard: &ReadGuard<R>, range: KeyRange) -> Result<(), Error> {
        let mut state = self.state();
        if state.epoch != guard.epoch {
            return Err(Error::new(TRANSACTION_CANCELLED));
        }
        if state.reading.closed {
            state.check_not_committing()?;
            return Err(Error::new(USED_DURING_COMMIT));
        }
        state.buffer.add_extra_read_conflict_range(range);
        Ok(())
    }

    /// A mutation (`set`, `clear`, `clear_range` or `atomic_op`): `f` on the state (its
    /// buffer), unless the transaction is committing (`used_during_commit`) or the handle is
    /// stale; its error is deferred. On success, the state (still locked) and what `f`
    /// returned: the write's watches are triggered then (`triggerWatches` comes after the
    /// write's checks), unless the transaction is dead (cancelled or timed out: the write
    /// is made, as in C++, but its watches have failed already).
    ///
    /// The `NEXT_WRITE_NO_WRITE_CONFLICT_RANGE` option is used up first thing, as the
    /// read-your-writes layer's `getAndResetWriteConflictDisabled` does (whatever the write
    /// does next): `f` adds no write conflict range then.
    ///
    /// `f` gets whether the write adds its write conflict range (`addWriteConflict`), for
    /// the write map.
    fn write_then<T>(
        &self,
        f: impl FnOnce(&mut State, bool) -> Result<T, Error>,
    ) -> Option<(MutexGuard<'_, State>, T)> {
        self.write_inner(true, f)
    }

    fn write_inner<T>(
        &self,
        mutation: bool,
        f: impl FnOnce(&mut State, bool) -> Result<T, Error>,
    ) -> Option<(MutexGuard<'_, State>, T)> {
        let mut state = self.state();
        let skip_conflict =
            mutation && std::mem::take(&mut state.flags.next_write_no_write_conflict_range);
        if self.is_stale(&state) {
            state.defer(Self::stale());
            return None;
        }
        if let Err(e) = state.check_not_committing() {
            state.defer(e);
            return None;
        }
        state.buffer.set_skip_write_conflict(skip_conflict);
        let result = f(&mut state, !skip_conflict);
        state.buffer.set_skip_write_conflict(false);
        match result {
            // C++'s writes do not look at `resetPromise`: on a cancelled or timed-out
            // transaction a write is still checked, its error deferred (which the next
            // operation returns before the cancellation), and applied (only
            // `approximate_size` sees it). It triggers no watch: the transaction's watches
            // have failed with its error already.
            Ok(_) if state.dead.is_some() => None,
            Ok(t) => Some((state, t)),
            Err(e) => {
                state.defer(e);
                None
            }
        }
    }

    /// [`write_inner`](Self::write_inner) for a conflict range: no watch, and the option
    /// that skips the next write's conflict range is not used up.
    fn write(&self, f: impl FnOnce(&mut State) -> Result<(), Error>) {
        drop(self.write_inner(false, |state, _| f(state)));
    }

    /// `commit()`: sends the writes and conflict ranges to a commit proxy, after getting a
    /// read version if the transaction has none (see the [module documentation](self)).
    ///
    /// * `Ok`: committed ([`CommitResult`]); a transaction with nothing to write returns
    ///   at once with version `-1`.
    /// * `not_committed` (1020): a conflict; `commit_unknown_result` (1021): **the
    ///   transaction may have committed**; both are retried by
    ///   [`on_error`](Self::on_error). See [`Database::run`] about idempotency.
    /// * A deferred error of a write, `transaction_too_large` (2101), ...
    ///
    /// Using the transaction after the commit started (another commit, a read or a write)
    /// fails with `used_during_commit` (2017) until it is reset; once the commit may have
    /// been sent, that error [`is_maybe_committed`](Error::is_maybe_committed).
    pub async fn commit(&self) -> Result<CommitResult, Error> {
        let sent = Arc::new(AtomicBool::new(false));
        let (epoch, deferred) = {
            let mut state = self.state();
            // `ThreadSafeTransaction::commit` checks the deferred error first
            // (`checkDeferredError`), before the transaction's cancellation or timeout,
            // as every other operation does here.
            let deferred = match self.is_stale(&state) {
                true => None,
                false => state.deferred.clone(),
            };
            if let Some(e) = &deferred {
                // C++ leaves the `getVersionstamp` futures pending until the reset; they
                // get the deferred error here, as the commit never will.
                state.resolve_versionstamp(Err(e.clone()));
            }
            (state.epoch, deferred)
        };
        let result = match deferred {
            Some(e) => Err(e),
            None => match self.guarded(epoch, self.commit_inner(&sent, epoch)).await {
                Ok(result) => result,
                // Interrupted (cancelled, timed out, reset) once the request may have left.
                Err(e) if sent.load(Ordering::SeqCst) => Err(e.into_maybe_committed()),
                Err(e) => Err(e),
            },
        };
        if let Err(e) = &result {
            // `commitAndWatch`: the watches fail with the commit's error.
            self.fail_watches(&sent, e);
        }
        result
    }

    async fn commit_inner(
        &self,
        sent: &Arc<AtomicBool>,
        epoch: u64,
    ) -> Result<CommitResult, Error> {
        {
            let mut state = self.state();
            if let Some(e) = state.deferred.clone() {
                // C++ leaves the `getVersionstamp` futures pending until the reset; they
                // get the deferred error here, as the commit never will.
                state.resolve_versionstamp(Err(e.clone()));
                return Err(e);
            }
            state.check_not_committing()?;
            // `commitStarted`: from now on a new read or write is `used_during_commit`.
            state.commit = CommitStage::Started(sent.clone());
        }
        // `wait(ryw->resetPromise.getFuture() || ryw->reading)` (`ReadYourWrites.actor.cpp`
        // 7.4:1359-1360, 8.0:1351-1352): the reads in progress end (and add their conflict
        // ranges) before the commit takes them. (A reset or cancellation interrupts the wait:
        // the commit runs `guarded`.)
        let (buffer, options) = loop {
            let ended = {
                let mut state = self.state();
                if state.epoch != epoch {
                    return Err(Error::new(TRANSACTION_CANCELLED));
                }
                // `resetPromise.getFuture() || ready`: a transaction cancelled, timed out or
                // poisoned (a use during the commit) while the reads ended does not commit,
                // whichever of the two this sees first.
                state.check_alive(self.rt().now())?;
                if state.reading.count == 0 {
                    state.reading.closed = true;
                    if !state.flags.read_your_writes_disabled {
                        // `RYWImpl::commit` (7.4:1392-1399, 8.0:1386-1393): the writes, from
                        // the write map (`writeRangeToNativeTransaction` over all the keys),
                        // then the reads' conflict ranges, coalesced, as
                        // `addReadConflictRange`s of the native transaction (counted in its
                        // size).
                        let state = &mut *state;
                        ryw::commit::write_range_to_native(
                            &state.ryw.writes,
                            b"",
                            SYSTEM_KEYS_END,
                            &mut state.buffer,
                        )?;
                        let ranges: Vec<KeyRange> = state.ryw.read_conflicts.ranges().collect();
                        for range in ranges {
                            state
                                .buffer
                                .add_read_conflict_range(&range.begin, &range.end)?;
                        }
                    }
                    let limits = *state.buffer.limits();
                    let buffer = std::mem::replace(&mut state.buffer, CommitBuffer::new(limits));
                    break (buffer, state.commit_options());
                }
                let (tx, rx) = oneshot::channel();
                state.reading.waiters.push(tx);
                rx
            };
            // `Canceled`: a reset dropped the waiters; the loop sees the new epoch.
            let _ = ended.await;
        };
        let outcome = self.send_commit(buffer, options, sent).await;
        // `tryCommit` (NativeAPI 7.4:6990-6996, 8.0:4672): the database remembers the
        // metadata version at the commit version, whatever became of the transaction.
        if let Some(id) = outcome.as_ref().ok().and_then(CommitOutcome::commit_id) {
            self.inner
                .db
                .inner
                .reader
                .note_committed_version(id.version, id.metadata_version.clone());
        }
        let (result, current) = {
            let mut state = self.state();
            let current = state.epoch == epoch;
            match outcome {
                Ok(outcome) => {
                    let result = CommitResult::from(outcome);
                    if current {
                        state.commit = CommitStage::Committed(result);
                        // C++: the stamp, or `no_commit_version` for a transaction with
                        // nothing to write.
                        state.resolve_versionstamp(
                            result.versionstamp().ok_or(Error::new(NO_COMMIT_VERSION)),
                        );
                    }
                    (result, current)
                }
                Err(e) => {
                    if current {
                        // `commit`'s `catch`: `transaction_invalid_version`.
                        state.resolve_versionstamp(Err(Error::new(TRANSACTION_INVALID_VERSION)));
                    }
                    return Err(e);
                }
            }
        };
        if current {
            // `commitAndWatch`: the watches start once the commit succeeded.
            self.start_watches(result.committed_version()).await;
        }
        Ok(result)
    }

    async fn send_commit(
        &self,
        buffer: CommitBuffer,
        options: CommitOptions,
        sent: &Arc<AtomicBool>,
    ) -> Result<CommitOutcome, Error> {
        let committer = &self.inner.db.inner.committer;
        // `commitMutations`' checks (`transaction_read_only`, the size, ...) come before the
        // read version request (NativeAPI 7.4:7129-7180, 8.0:4820-4875).
        let Some(commit) = committer.prepare_buffer(buffer, &options)? else {
            return Ok(CommitOutcome::NothingToCommit);
        };
        let read_version = self.read_version(true).await?;
        // The committer sets `sent` once the request may reach a proxy.
        committer
            .commit_prepared_tracked(commit, read_version, &options, sent)
            .await
    }

    /// `getCommittedVersion()`: the version the last [`commit`](Self::commit) committed at;
    /// `-1` if the transaction has not committed (since it was reset), or committed with
    /// nothing to write.
    pub fn committed_version(&self) -> i64 {
        match &self.state().commit {
            CommitStage::Committed(result) => result.committed_version(),
            _ => INVALID_VERSION,
        }
    }

    /// The versionstamp of the last [`commit`](Self::commit) (the 10 bytes the
    /// versionstamped operations wrote); `None` if the transaction has not committed, or
    /// committed with nothing to write. See [`get_versionstamp`](Self::get_versionstamp) for
    /// the future C++ gives.
    pub fn versionstamp(&self) -> Option<[u8; 10]> {
        match &self.state().commit {
            CommitStage::Committed(result) => result.versionstamp(),
            _ => None,
        }
    }

    /// `getVersionstamp()`: a future for the versionstamp of this transaction's commit, the
    /// 10 bytes that [`set_versionstamped_key`](Self::set_versionstamped_key) and
    /// [`set_versionstamped_value`](Self::set_versionstamped_value) write. **Call it before
    /// [`commit`](Self::commit)** (as with every FoundationDB binding): the future is
    /// registered by this call and resolves when the commit does, so it may be awaited
    /// during, after or instead of the commit's own result.
    ///
    /// The future's result:
    ///
    /// * The stamp when the commit succeeds (the same as [`versionstamp`](Self::versionstamp)).
    /// * `no_commit_version` (2021) if the commit has nothing to write (a read-only
    ///   transaction).
    /// * `transaction_invalid_version` (2020) if the request was sent and the commit
    ///   **failed**: a conflict (`not_committed`), `commit_unknown_result`, or any other error
    ///   of the commit proxy or of reaching it (C++'s `commit` catches them all).
    /// * The **deferred error** of a write (`key_too_large`, ...) if the commit refuses to
    ///   start because of it (C++ leaves the future pending until the reset, as the commit
    ///   fails before it reaches the versionstamp's promise; here it is told). A call made
    ///   when a deferred error is already there returns it at once.
    /// * `used_during_commit` (2017) for a second commit (or any use of the transaction)
    ///   once a commit started: it kills the transaction, so every future that waits
    ///   gets that error, not 2020; a call made while the commit is in progress is
    ///   `used_during_commit` too.
    /// * `transaction_cancelled` (1025) if the transaction is reset (or `on_error` retries
    ///   it) first, or [`cancel`](Self::cancel)led; `transaction_timed_out` (1031) **when the
    ///   timeout passes** (the future does not wait for an operation to notice it, as C++'s
    ///   `resetPromise` does); the error that killed the transaction otherwise. A commit that
    ///   was sent may still have committed in all these cases (the stamp is then lost).
    ///
    /// Unlike C++ (where a call after the commit started is `used_during_commit`), a call
    /// after the commit **succeeded** returns the stamp at once, and does not disturb the
    /// transaction.
    pub fn get_versionstamp(
        &self,
    ) -> impl Future<Output = Result<[u8; 10], Error>> + Send + use<R> {
        enum Pending<R: Runtime> {
            Done(Result<[u8; 10], Error>),
            Wait {
                rx: oneshot::Receiver<Result<[u8; 10], Error>>,
                inner: Weak<Inner<R>>,
                rt: R,
                epoch: u64,
            },
        }
        let pending = {
            let mut state = self.state();
            let ready = if let Err(e) = self.check_handle(&state) {
                Some(Err(e))
            } else if let CommitStage::Committed(result) = &state.commit {
                Some(result.versionstamp().ok_or(Error::new(NO_COMMIT_VERSION)))
            } else if let Some(e) = state.deferred.clone() {
                Some(Err(e))
            } else if let Err(e) = state
                .check_alive(self.rt().now())
                .and_then(|()| state.check_not_committing())
            {
                Some(Err(e))
            } else {
                None
            };
            match ready {
                Some(result) => Pending::Done(result),
                None => {
                    let (tx, rx) = oneshot::channel();
                    // Waiters whose future was dropped are not kept.
                    state.versionstamp_waiters.retain(|w| !w.is_canceled());
                    state.versionstamp_waiters.push(tx);
                    Pending::Wait {
                        rx,
                        inner: Arc::downgrade(&self.inner),
                        rt: self.rt().clone(),
                        epoch: state.epoch,
                    }
                }
            }
        };
        async move {
            match pending {
                Pending::Done(result) => result,
                Pending::Wait {
                    rx,
                    inner,
                    rt,
                    epoch,
                } => wait_versionstamp(rx, inner, rt, epoch).await,
            }
        }
    }

    /// `onError(error)`: if `error` is one C++'s `onError` retries ([`Error::transaction_retry`])
    /// and the retry limit is not reached, resets the transaction for another attempt and
    /// waits the backoff, then `Ok`; otherwise returns the error (`error` itself, or the
    /// transaction's timeout or cancellation), and the transaction is cancelled until
    /// [`reset`](Self::reset). See the [module documentation](self#retries-on_error).
    ///
    /// An `error` that [`is_maybe_committed`](Error::is_maybe_committed) is retried like a
    /// conflict: the transaction may have committed (see [`Database::run`]). If such an
    /// error is not retried, the error returned is maybe committed too, whatever it is.
    pub async fn on_error(&self, error: Error) -> Result<(), Error> {
        let maybe_committed = error.is_maybe_committed();
        self.on_error_inner(error).await.map_err(|e| {
            // `RYWImpl::onError` (7.4:1531, 8.0:1532): `if (e.code() ==
            // error_code_broken_promise) throw transaction_cancelled();`.
            let e = if e.code() == BROKEN_PROMISE {
                Error::new(TRANSACTION_CANCELLED)
            } else {
                e
            };
            if maybe_committed {
                e.into_maybe_committed()
            } else {
                e
            }
        })
    }

    async fn on_error_inner(&self, error: Error) -> Result<(), Error> {
        let delay = {
            let mut state = self.state();
            self.check_handle(&state)?;
            state.check_alive(self.rt().now())?;
            let limit_hit = state
                .persistent
                .retry_limit
                .is_some_and(|limit| state.retries >= limit);
            state.retries = state.retries.saturating_add(1);
            let retry = if limit_hit {
                Err(error.clone())
            } else {
                error.on_error_for(self.inner.db.inner.monitor.schema_version())
            };
            let knobs = &self.inner.db.inner.knobs;
            let delay = match retry {
                Err(e) => {
                    state.kill(Error::new(TRANSACTION_CANCELLED));
                    return Err(e);
                }
                // `getBackoff`: a random fraction of the backoff, which then grows.
                Ok(TransactionRetry::Backoff) => {
                    let delay = state.backoff.mul_f64(self.inner.db.inner.random01());
                    let cap = if error.is_resource_constrained() {
                        knobs.resource_constrained_max_backoff
                    } else {
                        state.persistent.max_retry_delay
                    };
                    state.backoff = state.backoff.mul_f64(knobs.backoff_growth).min(cap);
                    delay
                }
                Ok(TransactionRetry::FutureVersion) => knobs
                    .future_version_retry_delay
                    .min(state.persistent.max_retry_delay),
            };
            state.reset_for_retry(
                self.inner.db.inner.committer.limits(),
                self.inner.db.inner.snapshot_ryw.load(Ordering::SeqCst),
            );
            (delay, state.epoch)
        };
        let (delay, epoch) = delay;
        self.guarded(epoch, async {
            self.rt().sleep(delay).await;
            Ok(())
        })
        .await?
    }

    /// `setOption(option)`: validates the option (`invalid_option_value`, 2006, for a value out
    /// of range, `client_invalid_operation` (2000) for one that is not valid now, `invalid_option`
    /// (2007) for one this client does not support; the transaction is unchanged then) and
    /// applies it. See [`TransactionOption`] for what each option does and which survive
    /// [`on_error`](Self::on_error).
    pub fn set_option(&self, option: TransactionOption) -> Result<(), Error> {
        let mut state = self.state();
        self.check_handle(&state)?;
        state.apply_option(&option)?;
        if let TransactionOption::Timeout(_) = option {
            // `resetTimeout`: the operations in progress see the new deadline (and fail at
            // once if it has passed). A deadline that passed before this call has killed
            // the transaction already (`state()`), and no new timeout brings it back.
            if state.check_alive(self.rt().now()).is_ok() {
                state.interrupt(|| Interrupt::Deadline);
            }
        }
        Ok(())
    }

    /// `reset()`: back to a new transaction: no read version, writes, options or
    /// deferred error; the retry count and the backoff start again, and the timeout counts
    /// from now. Operations in progress fail with `transaction_cancelled` (1025). Does
    /// nothing on a stale handle of a [`Database::run`] attempt.
    pub fn reset(&self) {
        let mut state = self.state();
        if self.is_stale(&state) {
            return;
        }
        state.interrupt(|| Interrupt::Abort(Error::new(TRANSACTION_CANCELLED)));
        state.watches.cancel_all(&Error::new(TRANSACTION_CANCELLED));
        state.resolve_versionstamp(Err(Error::new(TRANSACTION_CANCELLED)));
        let epoch = state.epoch + 1;
        *state = State::new(&self.inner.db, epoch);
    }

    /// `cancel()`: every operation, in progress or to come, fails with
    /// `transaction_cancelled` (1025) until [`reset`](Self::reset). A commit in progress may
    /// still happen: its error is then [maybe committed](Error::is_maybe_committed). Does
    /// nothing on a stale handle of a [`Database::run`] attempt.
    pub fn cancel(&self) {
        let mut state = self.state();
        if !self.is_stale(&state) {
            state.kill(Error::new(TRANSACTION_CANCELLED));
        }
    }

    /// How many times [`on_error`](Self::on_error) was called since the transaction was
    /// created or [reset](Self::reset).
    pub fn retries(&self) -> u32 {
        self.state().retries
    }
}

/// What [`Transaction::get_versionstamp`] awaits: the answer the commit (or whatever kills
/// the transaction) sends, raced against the transaction's timeout, which kills it when it
/// passes. (`waitOrError(tr.getVersionstamp(), resetPromise.getFuture())` with the
/// `timebomb` that sets the promise.) The future holds the transaction weakly: if the
/// transaction is dropped, the answer is `transaction_cancelled`.
async fn wait_versionstamp<R: Runtime>(
    mut rx: oneshot::Receiver<Result<[u8; 10], Error>>,
    inner: Weak<Inner<R>>,
    rt: R,
    epoch: u64,
) -> Result<[u8; 10], Error> {
    let cancelled = || Err(Error::new(TRANSACTION_CANCELLED));
    loop {
        // `None`: the transaction is gone or was reset; the answer is in `rx`.
        let registered = inner.upgrade().and_then(|inner| {
            let mut state = lock(&inner.state);
            if state.epoch != epoch {
                return None;
            }
            let (tx, interrupted) = oneshot::channel();
            state.interrupts.retain(|s| !s.is_canceled());
            state.interrupts.push(tx);
            Some((interrupted, state.deadline()))
        });
        let Some((interrupted, deadline)) = registered else {
            return rx.await.unwrap_or_else(|oneshot::Canceled| cancelled());
        };
        let timeout = async {
            match deadline {
                Some(deadline) => rt.sleep_until(deadline).await,
                None => never().await,
            }
        };
        match race(&mut rx, race(interrupted, timeout)).await {
            Either::Left(result) => return result.unwrap_or_else(|oneshot::Canceled| cancelled()),
            // A change of the timeout, or its end: the transaction dies (and answers us) if
            // the deadline has passed.
            Either::Right(_) => {
                if let Some(inner) = inner.upgrade() {
                    let mut state = lock(&inner.state);
                    if state.epoch == epoch {
                        let _ = state.check_alive(rt.now());
                    }
                }
            }
        }
    }
}

/// Snapshot reads of a [`Transaction`] ([`Transaction::snapshot`]): no read conflict ranges.
pub struct Snapshot<'a, R: Runtime> {
    tx: &'a Transaction<R>,
}

impl<R: Runtime> fmt::Debug for Snapshot<'_, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Snapshot").field(self.tx).finish()
    }
}

impl<R: Runtime> Snapshot<'_, R> {
    /// [`Transaction::get`] without a read conflict range (and without the transaction's
    /// writes if snapshot reads do not see them:
    /// [`SnapshotRywDisable`](TransactionOption::SnapshotRywDisable)).
    pub fn get(
        &self,
        key: &[u8],
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Error>> + Send + use<R> {
        self.tx.get_with(key, true)
    }

    /// [`Transaction::get_key`] without a read conflict range (see [`get`](Self::get)).
    pub fn get_key(
        &self,
        selector: &KeySelector,
    ) -> impl Future<Output = Result<Vec<u8>, Error>> + Send + use<R> {
        self.tx.get_key_with(selector, true)
    }
}
