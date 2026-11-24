//! [`Transaction`]: reads at one read version, buffered writes, and a commit; with C++'s
//! `onError` retry semantics, its options, timeouts and cancellation.
//!
//! This is the minimal transaction of the vertical slice, the C++ `Transaction` behind a
//! `ReadYourWritesTransaction` **with read-your-writes disabled**:
//!
//! * **Reads do not see the transaction's own writes (yet).** [`get`](Transaction::get),
//!   [`get_key`](Transaction::get_key) and [`get_range`](Transaction::get_range) (and its
//!   streams, [`get_ranges`](Transaction::get_ranges) and
//!   [`range_stream`](Transaction::range_stream)) read the database at the transaction's read
//!   version, whatever the transaction has
//!   [`set`](Transaction::set) or [`clear`](Transaction::clear)ed. The read-your-writes layer
//!   (M5) will make reads see the transaction's own writes **by default**, as every FDB
//!   binding does, with an option to disable it; code should not rely on today's behaviour.
//! * **Conflict ranges are tracked as C++ tracks them.** Each (non-snapshot) read adds the
//!   key it read as a read conflict range; each write adds its key or range as a write
//!   conflict range; the commit sends them, so a transaction whose reads were changed by
//!   another transaction that committed after its read version fails with `not_committed`
//!   (1020), and [`Database::run`] retries it.
//! * **The read version** is requested lazily, by the first read (or
//!   [`get_read_version`](Transaction::get_read_version)), once for the transaction: the
//!   request runs as its own task on the runtime and every read waiting for it gets its
//!   answer, whichever of them is still being polled. [`set_read_version`](Transaction::set_read_version)
//!   supplies one instead. A transaction that only writes gets one at commit, with
//!   `CAUSAL_READ_RISKY`, as C++'s `commitMutations` does.
//! * **Errors of writes are deferred.** `set`, `clear`, ... return nothing; an error they
//!   meet (`key_too_large`, `key_outside_legal_range`, ...) is kept and returned by the next
//!   read or the commit, as `fdb_c`'s deferred error.
//! * **A commit waits for the reads in progress.** As C++'s read-your-writes layer (even with
//!   read-your-writes disabled) counts every read in `reading` and makes the commit wait for
//!   them (`ReadYourWrites.actor.cpp` 7.4:1359-1360, 8.0:1340-1341), the commit waits for
//!   the `get_key`, `get_range` (and range stream batches) and `watch` reads that started
//!   before it, whether they succeed or fail, before it takes the conflict ranges: their
//!   ranges depend on what they read and are added when they end (a read that fails adds
//!   none). A `get` adds its range when it starts, so the commit does not need to wait for
//!   it, and does not. **Each of these reads runs as a task of its own**, as a C++ read
//!   actor runs to its end: a read future that is dropped, or kept but no longer polled
//!   (the loser of a `select`), does not hold a commit up; its read still ends and adds its
//!   range. A read started after the commit fails with `used_during_commit`. The conflict
//!   ranges of `get_key` and `get_range` are C++'s `extraConflictRanges`: they do not count
//!   toward the transaction's size (the `SizeLimit` option and `transaction_too_large`).
//!
//! * **Watches** ([`watch`](Transaction::watch)) have the read-your-writes layer's semantics
//!   already: the watch works out the key's value with the transaction's own writes
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
//!   the deferred error and the other options (`size_limit` among them).
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
//! to the operations in progress too (at once if the deadline has already passed).
//! [`cancel`](Transaction::cancel) does the same with `transaction_cancelled` (1025), and a
//! [`reset`](Transaction::reset) (or `on_error`) makes the operations in progress fail with
//! `transaction_cancelled`.
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
//! # Not ported yet
//!
//! Read-your-writes, mapped range reads (`getMappedRange`), `getRangeStream` (the
//! storage servers' streaming reads), snapshot read-your-writes options, the special key space (`\xff\xff`), transaction
//! tags, priorities, idempotency ids, tenants, the `metadataVersion` key exception, and
//! database-wide default transaction options.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use fdb_native_schema::domain::{INVALID_VERSION, KeyRange, KeySelector, MutationType};
#[cfg(feature = "tokio")]
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_transport::runtime::{Instant, Runtime};
use futures_channel::oneshot;

use crate::atomic;
use crate::commit::{CommitBuffer, CommitLimits, CommitOptions, CommitOutcome};
use crate::database::Database;
use crate::error::codes::*;
use crate::error::{Error, TransactionRetry};
use crate::read::util::{Either, never, race};
use crate::read::{ReadVersionOptions, key_after};
use crate::watch::{PendingWatches, StampedKey, Written};

mod range;
mod watch;

pub use range::{KeyValueStream, RangeResult, RangeStream};

/// The first key a transaction may not read or write without the system-key options.
const SYSTEM_KEYS_BEGIN: &[u8] = b"\xff";
/// The end of the system keys (and the first key of the special key space).
const SYSTEM_KEYS_END: &[u8] = b"\xff\xff";
/// `extractIntOption`'s upper bound for the millisecond options.
const MAX_MILLIS: u128 = i32::MAX as u128;

/// A transaction option (`FDBTransactionOption`), for [`Transaction::set_option`].
///
/// `Timeout`, `RetryLimit` and `MaxRetryDelay` persist across
/// [`on_error`](Transaction::on_error) (`fdb.options`' "persistent" options); the others
/// apply until the transaction is reset, by `on_error` or [`reset`](Transaction::reset). A
/// full reset clears them all.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TransactionOption {
    /// `TIMEOUT`: the transaction fails with `transaction_timed_out` once this long has
    /// passed since it was created or last [reset](Transaction::reset). Zero disables it.
    /// At most `i32::MAX` milliseconds. Persistent.
    Timeout(Duration),
    /// `RETRY_LIMIT`: how many times [`on_error`](Transaction::on_error) may retry; `-1`
    /// (the default) is unlimited. Persistent.
    RetryLimit(i32),
    /// `MAX_RETRY_DELAY`: the largest backoff [`on_error`](Transaction::on_error) waits
    /// (default 1 s). At most `i32::MAX` milliseconds. Persistent.
    MaxRetryDelay(Duration),
    /// `SIZE_LIMIT`: the largest size (`getSize()`) the transaction may commit, 32 bytes to
    /// `TRANSACTION_SIZE_LIMIT` (10 000 000, the default). Not persistent: `on_error`
    /// forgets it.
    SizeLimit(u32),
    /// `CAUSAL_WRITE_RISKY`: the commit adds no self-conflicting key to a blind write, and
    /// does not settle an unknown result with a dummy transaction.
    CausalWriteRisky,
    /// `CAUSAL_READ_RISKY`: the read version may not reflect every earlier commit.
    CausalReadRisky,
    /// `LOCK_AWARE`: the transaction may read and commit while the database is locked.
    LockAware,
    /// `USE_PROVISIONAL_PROXIES`: use the proxies of a recovery that has not finished.
    UseProvisionalProxies,
    /// `REPORT_CONFLICTING_KEYS`: a conflict says which read conflict ranges conflicted
    /// ([`Error::conflicting_ranges`]).
    ReportConflictingKeys,
    /// `ACCESS_SYSTEM_KEYS`: the transaction may read and write keys from `\xff` to
    /// `\xff\xff`.
    AccessSystemKeys,
    /// `READ_SYSTEM_KEYS`: the transaction may read keys from `\xff` to `\xff\xff`.
    ReadSystemKeys,
}

/// What a successful [`Transaction::commit`] did.
#[derive(Clone, Debug, PartialEq, Eq)]
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

/// The options that persist across `on_error` (`persistent="true"` in `fdb.options`).
#[derive(Clone, Copy, Debug)]
struct Persistent {
    timeout: Option<Duration>,
    retry_limit: Option<u32>,
    max_retry_delay: Duration,
}

/// The options a reset clears.
#[derive(Clone, Copy, Debug)]
struct Flags {
    causal_write_risky: bool,
    causal_read_risky: bool,
    lock_aware: bool,
    use_provisional_proxies: bool,
    report_conflicting_keys: bool,
    access_system_keys: bool,
    read_system_keys: bool,
    size_limit: u64,
}

impl Default for Flags {
    fn default() -> Flags {
        Flags {
            causal_write_risky: false,
            causal_read_risky: false,
            lock_aware: false,
            use_provisional_proxies: false,
            report_conflicting_keys: false,
            access_system_keys: false,
            read_system_keys: false,
            size_limit: CommitOptions::default().size_limit,
        }
    }
}

impl Flags {
    /// `getMaxReadKey()`.
    fn max_read_key(&self) -> &'static [u8] {
        if self.access_system_keys || self.read_system_keys {
            SYSTEM_KEYS_END
        } else {
            SYSTEM_KEYS_BEGIN
        }
    }

    /// `getMaxWriteKey()`.
    fn max_write_key(&self) -> &'static [u8] {
        if self.access_system_keys {
            SYSTEM_KEYS_END
        } else {
            SYSTEM_KEYS_BEGIN
        }
    }
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
    buffer: CommitBuffer,
    deferred: Option<Error>,
    flags: Flags,
    persistent: Persistent,
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
    /// The non-snapshot reads in progress (the read-your-writes layer's `reading`).
    reading: Reading,
    /// The `SetVersionstampedKey`s of the buffer, for what [`Transaction::watch`] may read.
    stamped_keys: Vec<StampedKey>,
}

/// The read-your-writes layer's `reading` (`ReadYourWrites.actor.cpp` 7.4:1707, 8.0:1694):
/// the non-snapshot reads in progress, which the commit waits for before it takes the
/// conflict ranges (7.4:1359-1360, 8.0:1340-1341), so that a read that started before the
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

/// A non-snapshot read in progress, counted in [`Reading`] until it is dropped: when it
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
        State {
            epoch,
            read_version: ReadVersion::Unset,
            buffer: CommitBuffer::new(db.inner.committer.limits()),
            deferred: None,
            flags: Flags::default(),
            persistent: Persistent {
                timeout: None,
                retry_limit: None,
                max_retry_delay: knobs.max_backoff,
            },
            retries: 0,
            backoff: knobs.initial_backoff,
            created: db.inner.rt.now(),
            dead: None,
            commit: CommitStage::Idle,
            interrupts: Vec::new(),
            versionstamp_waiters: Vec::new(),
            watches: PendingWatches::default(),
            reading: Reading::default(),
            stamped_keys: Vec::new(),
        }
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
        if let Some(e) = &self.dead {
            return Err(e.clone());
        }
        if self.deadline().is_some_and(|deadline| deadline <= now) {
            let e = Error::new(TRANSACTION_TIMED_OUT);
            self.kill(e.clone());
            return Err(e);
        }
        Ok(())
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
    fn reset_for_retry(&mut self, limits: CommitLimits) {
        self.epoch += 1;
        self.interrupt(|| Interrupt::Abort(Error::new(TRANSACTION_CANCELLED)));
        self.watches.cancel_all(&Error::new(TRANSACTION_CANCELLED));
        self.resolve_versionstamp(Err(Error::new(TRANSACTION_CANCELLED)));
        self.read_version = ReadVersion::Unset;
        self.buffer = CommitBuffer::new(limits);
        self.deferred = None;
        self.flags = Flags::default();
        self.commit = CommitStage::Idle;
        self.reading = Reading::default();
        self.stamped_keys.clear();
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
        self.kill(e.clone());
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

    fn read_version_options(&self, causal_read_risky: bool) -> ReadVersionOptions {
        ReadVersionOptions {
            causal_read_risky: causal_read_risky || self.flags.causal_read_risky,
            use_provisional_proxies: self.flags.use_provisional_proxies,
            lock_aware: self.flags.lock_aware,
            ..ReadVersionOptions::default()
        }
    }

    fn commit_options(&self) -> CommitOptions {
        CommitOptions {
            causal_write_risky: self.flags.causal_write_risky,
            lock_aware: self.flags.lock_aware,
            use_provisional_proxies: self.flags.use_provisional_proxies,
            report_conflicting_keys: self.flags.report_conflicting_keys,
            size_limit: self.flags.size_limit,
            ..CommitOptions::default()
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
        let state = self.state();
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

    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.inner.state)
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
            let result = reader.get_read_version_with(options).await;
            guard.done = true;
            let Some(inner) = guard.inner.upgrade() else {
                return;
            };
            let mut state = lock(&inner.state);
            if state.epoch != epoch {
                return;
            }
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

    /// `get(key)`: the value of `key` at the transaction's read version, `None` if it is not
    /// set. Adds `key` as a read conflict range. **Does not see the transaction's own writes
    /// yet** (see the [module documentation](self)).
    ///
    /// `key_outside_legal_range` (2004) for a key from `\xff` on (unless the
    /// `ReadSystemKeys` or `AccessSystemKeys` option is set) or from `\xff\xff` on.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.get_with(key, false).await
    }

    /// A view of the transaction whose reads are snapshot reads: they add no read conflict
    /// range.
    pub fn snapshot(&self) -> Snapshot<'_, R> {
        Snapshot { tx: self }
    }

    async fn get_with(&self, key: &[u8], snapshot: bool) -> Result<Option<Vec<u8>>, Error> {
        let (too_long, use_provisional_proxies, epoch) = {
            let mut state = self.state();
            self.check_handle(&state)?;
            // `fdb_c` reports the deferred error first.
            if let Some(e) = state.deferred.clone() {
                return Err(e);
            }
            state.check_alive(self.rt().now())?;
            state.check_not_committing()?;
            if key >= state.flags.max_read_key() {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            if !snapshot {
                let end = key_after(key);
                // The key is below `\xff\xff`, so the range is not inverted.
                state.buffer.add_read_conflict_range(key, &end)?;
            }
            let limits = *state.buffer.limits();
            // Its range is in already: the commit need not wait for it (see the module
            // documentation), so it is not counted in `reading`.
            (
                key.len() > limits.max_range_key_size(key),
                state.flags.use_provisional_proxies,
                state.epoch,
            )
        };
        if too_long {
            // No key in the database is longer than the limit (C++ does not ask either).
            return Ok(None);
        }
        self.guarded(epoch, async {
            let version = self.read_version(false).await?;
            self.inner
                .db
                .inner
                .reader
                .get_with(key, version, use_provisional_proxies)
                .await
        })
        .await?
    }

    /// `getKey(selector)`: the key `selector` resolves to at the transaction's read version
    /// ([`Reader::get_key`](crate::read::Reader::get_key)), as `fdb_c` returns it with
    /// read-your-writes disabled. **Does not see the transaction's own writes yet** (see the
    /// [module documentation](self)).
    ///
    /// * **Bounds.** A selector that resolves before the first key gives the empty key; one
    ///   that resolves past the last key the transaction may read gives that bound: `\xff`, or
    ///   `\xff\xff` with the `ReadSystemKeys` or `AccessSystemKeys` option
    ///   (`ReadYourWritesTransaction`'s `readThrough` clamps to `getMaxReadKey()`).
    /// * **Errors.** `key_outside_legal_range` (2004) for a selector key after that bound
    ///   (the bound itself is allowed); then as [`get`](Self::get). A selector key longer
    ///   than any key can be is shortened first, as `KeySelectorRef::setKey` does (to one
    ///   byte over the limit of `getMaxKeySize`).
    /// * **Read conflict range** (`getKeyAndConflictRange`, NativeAPI 7.4:6077-6092,
    ///   8.0:3817-3835), from the selector's key `k` and the (unclamped) result `r`: for an
    ///   offset of 0 or less, `[r, k)` (`[r, keyAfter(k))` if `or_equal`); otherwise
    ///   `[k, keyAfter(r))` (`[keyAfter(k), keyAfter(r))` if `or_equal`), where `keyAfter(k)`
    ///   is `k\x00` and `keyAfter(\xff\xff)` is `\xff\xff`; none if that range is empty or
    ///   the read fails. It is added when the result arrives; a commit started meanwhile
    ///   waits for the read (see the [module documentation](self)). As C++'s
    ///   `extraConflictRanges` (7.4:7166-7172, 8.0:4863-4867), it does not count toward the
    ///   transaction's size.
    pub async fn get_key(&self, selector: &KeySelector) -> Result<Vec<u8>, Error> {
        self.get_key_with(selector, false).await
    }

    async fn get_key_with(&self, selector: &KeySelector, snapshot: bool) -> Result<Vec<u8>, Error> {
        let (selector, max_read_key, use_provisional_proxies, epoch, guard) = {
            let mut state = self.state();
            self.check_handle(&state)?;
            if let Some(e) = state.deferred.clone() {
                return Err(e);
            }
            state.check_alive(self.rt().now())?;
            state.check_not_committing()?;
            let max_read_key = state.flags.max_read_key();
            let selector = self.set_key(&state, selector);
            if selector.key.as_slice() > max_read_key {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            (
                selector,
                max_read_key,
                state.flags.use_provisional_proxies,
                state.epoch,
                (!snapshot).then(|| self.start_read(&mut state)),
            )
        };
        let key = match guard {
            None => {
                self.native_get_key(&selector, None, epoch, use_provisional_proxies)
                    .await?
            }
            Some(guard) => {
                let tx = self.clone();
                self.spawn_read(async move {
                    let key = tx
                        .native_get_key(&selector, Some(&guard), epoch, use_provisional_proxies)
                        .await;
                    drop(guard);
                    key
                })
                .await?
            }
        };
        // `readThrough`: results in keys the transaction may not read are clamped.
        if key.as_slice() > max_read_key {
            return Ok(max_read_key.to_vec());
        }
        Ok(key)
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

    /// Runs `read`, the part of a non-snapshot read that waits for the cluster and adds the
    /// read conflict range, holding its [`ReadGuard`], as a task of its own: as a C++ read
    /// actor, it ends (and lets a waiting commit go on) whether or not the future that
    /// started it is still polled or kept. The result goes to the future this returns.
    fn spawn_read<T: Send + 'static>(
        &self,
        read: impl Future<Output = Result<T, Error>> + Send + 'static,
    ) -> impl Future<Output = Result<T, Error>> + Send + 'static {
        let (tx, rx) = oneshot::channel();
        // Detached: it ends with the read (which the transaction's reset, cancellation or
        // timeout interrupts).
        drop(self.rt().spawn(async move {
            let _ = tx.send(read.await);
        }));
        async move {
            rx.await
                .unwrap_or_else(|oneshot::Canceled| Err(Error::new(TRANSACTION_CANCELLED)))
        }
    }

    /// Counts a non-snapshot read in progress (`reading.add`) until the guard is dropped.
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

    /// A write: `f` on the state (its buffer), unless the transaction is committing
    /// (`used_during_commit`) or dead, or the handle is stale; its error is deferred. On
    /// success, the state (still locked) and what `f` returned: the write's watches are
    /// triggered then (`triggerWatches` comes after the write's checks).
    fn write_then<T>(
        &self,
        f: impl FnOnce(&mut State) -> Result<T, Error>,
    ) -> Option<(MutexGuard<'_, State>, T)> {
        let mut state = self.state();
        if self.is_stale(&state) {
            state.defer(Self::stale());
            return None;
        }
        if state.dead.is_some() {
            return None;
        }
        if let Err(e) = state.check_not_committing() {
            state.defer(e);
            return None;
        }
        match f(&mut state) {
            Ok(t) => Some((state, t)),
            Err(e) => {
                state.defer(e);
                None
            }
        }
    }

    /// [`write_then`](Self::write_then) for a write that triggers no watch.
    fn write(&self, f: impl FnOnce(&mut State) -> Result<(), Error>) {
        drop(self.write_then(f));
    }

    /// `set(key, value)`: writes `value` to `key` when the transaction commits. Errors are
    /// deferred: `key_too_large` (2102, a key over 10 000 bytes), `value_too_large` (2103,
    /// a value over 100 000 bytes), `key_outside_legal_range` (2004, a key from `\xff` on
    /// without the `AccessSystemKeys` option).
    pub fn set(&self, key: &[u8], value: &[u8]) {
        let written = self.write_then(|state| {
            if key >= state.flags.max_write_key() {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            state.buffer.set(key, value)
        });
        if let Some((mut state, ())) = written {
            state
                .watches
                .trigger(key, None, Written::Value(Some(value)));
        }
    }

    /// `clear(key)`: removes `key` when the transaction commits.
    pub fn clear(&self, key: &[u8]) {
        let written = self.write_then(|state| {
            if key >= state.flags.max_write_key() {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            state.buffer.clear(key);
            Ok(())
        });
        if let Some((mut state, ())) = written {
            state.watches.trigger(key, None, Written::Value(None));
        }
    }

    /// `clear(begin, end)`: removes every key in `[begin, end)` when the transaction
    /// commits. Deferred errors: `inverted_range` (2005) if `begin > end`,
    /// `key_outside_legal_range` if either is past `\xff` (without `AccessSystemKeys`).
    pub fn clear_range(&self, begin: &[u8], end: &[u8]) {
        let written = self.write_then(|state| {
            let max = state.flags.max_write_key();
            if begin > max || end > max {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            state.buffer.clear_range(begin, end)
        });
        if let Some((mut state, ())) = written {
            state
                .watches
                .trigger(begin, Some(end), Written::Value(None));
        }
    }

    /// `atomicOp(key, operand, op)`: an atomic operation on `key` when the transaction
    /// commits (`ADD_VALUE`, `BIT_AND`, ...; see [`MutationType`], and [`crate::atomic`] for
    /// what each does to the value), including the two versionstamp operations (see
    /// [`set_versionstamped_key`](Self::set_versionstamped_key)). Deferred errors:
    /// `invalid_mutation_type` (2018) if `op` is not an atomic operation, `key_too_large`,
    /// `value_too_large`, `key_outside_legal_range`, and for the versionstamp operations
    /// `client_invalid_operation` (2000) if the 4-byte offset is invalid.
    pub fn atomic_op(&self, key: &[u8], operand: &[u8], op: MutationType) {
        let written = self.write_then(|state| {
            if key >= state.flags.max_write_key() {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            state.buffer.atomic_op(key, operand, op)?;
            if op != MutationType::SET_VERSIONSTAMPED_KEY {
                return Ok(None);
            }
            // `getVersionstampKeyRange` and `transformVersionstampKey` with the read version
            // the transaction has now (`getCachedReadVersion`), for its watches.
            let read_version = match state.read_version {
                ReadVersion::Set(v) => Some(v),
                _ => None,
            };
            let max_read_key = state.flags.max_read_key();
            let range = atomic::versionstamp_key_range(key, read_version, max_read_key);
            let stamped =
                atomic::transform_versionstamp_key(key, read_version.map_or(0, |v| v + 1), 0)
                    .unwrap_or_else(|_| key.to_vec());
            state.stamped_keys.push(StampedKey {
                index: state.buffer.mutations().len() - 1,
                range: range.as_ref().ok().cloned(),
                key: stamped.clone(),
            });
            // C++ throws `inverted_range` here (for a range past the readable keys).
            range?;
            Ok(Some(stamped))
        });
        if let Some((mut state, stamped)) = written {
            let key = stamped.as_deref().unwrap_or(key);
            state.watches.trigger(key, None, Written::Unknown);
        }
    }

    /// `atomicOp(key, value, SetVersionstampedKey)`: when the transaction commits, sets
    /// `value` at `key` with 10 bytes of `key` replaced by the transaction's versionstamp
    /// ([`versionstamp`](Self::versionstamp)): the commit version (8 bytes big-endian) and
    /// the transaction's index in the commit batch (2 bytes big-endian). Keys written by
    /// transactions that commit later sort after, whatever the batch.
    ///
    /// `key` is the key **with a placeholder and the offset**: ten bytes (their value is
    /// ignored; the tuple layer uses `0xff`) at position `p`, and as the last 4 bytes `p`
    /// as a little-endian `i32` (the encoding of API version 520 and later;
    /// [`with_versionstamp_offset`](crate::commit::with_versionstamp_offset), or
    /// `fdb_native_tuple::pack_with_versionstamp`, which builds it). The 4 bytes are removed
    /// by the commit. `client_invalid_operation` (2000, deferred) if `key` has fewer than 4
    /// bytes, or `p < 0`, or `p + 10 > len - 4`; the key and value size limits (`key_too_large`,
    /// `value_too_large`) include the 4 bytes.
    ///
    /// **No write conflict range is added** for the key: the key is not known before the
    /// commit (C++ adds none either: `Transaction::atomicOp` skips it for
    /// `SetVersionstampedKey`). A transaction that writes nothing else is still sent, with
    /// the self-conflicting key, as any blind write. Reads of the transaction do not see the
    /// key (nothing is read-your-writes yet); a [`watch`](Self::watch) of a key in the range
    /// the key may take (see [`atomic::versionstamp_key_range`]) fails with
    /// `accessed_unreadable` (1036) until a clear covers it, and a watch of the key with the
    /// placeholder stamped (`transformVersionstampKey`) is triggered, as C++'s
    /// read-your-writes layer does.
    ///
    /// ```no_run
    /// # async fn example(db: fdb_native_client::Database) -> Result<(), fdb_native_client::Error> {
    /// use fdb_native_tuple::{Versionstamp, pack_with_versionstamp, tuple};
    ///
    /// let tx = db.create_transaction();
    /// // The key `("log", <stamp>, "event")`, completed by the commit.
    /// let key = pack_with_versionstamp(&tuple!["log", Versionstamp::incomplete(0), "event"])
    ///     .expect("one incomplete versionstamp");
    /// tx.set_versionstamped_key(&key, b"payload");
    /// let stamp = tx.get_versionstamp(); // before the commit
    /// tx.commit().await?;
    /// let stamp = stamp.await?; // the 10 bytes now in the key
    /// # let _ = stamp;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_versionstamped_key(&self, key: &[u8], value: &[u8]) {
        self.atomic_op(key, value, MutationType::SET_VERSIONSTAMPED_KEY);
    }

    /// `atomicOp(key, value, SetVersionstampedValue)`: when the transaction commits, sets
    /// `key` to `value` with 10 bytes of `value` replaced by the transaction's versionstamp
    /// (see [`set_versionstamped_key`](Self::set_versionstamped_key)). `value` ends with the
    /// 4-byte little-endian position of the placeholder: `client_invalid_operation` (2000,
    /// deferred) if it has fewer than 4 bytes, or the position is negative or
    /// `position + 10 > len - 4`. The key gets a write conflict range as for `set`. A
    /// [`watch`](Self::watch) of the key fails with `accessed_unreadable` (1036) until a
    /// clear of it (its value is not known before the commit).
    pub fn set_versionstamped_value(&self, key: &[u8], value: &[u8]) {
        self.atomic_op(key, value, MutationType::SET_VERSIONSTAMPED_VALUE);
    }

    /// `addReadConflictRange(begin, end)`: the commit conflicts if another transaction
    /// wrote in `[begin, end)` after the read version, as if the range had been read.
    pub fn add_read_conflict_range(&self, begin: &[u8], end: &[u8]) {
        self.write(|state| {
            let max = state.flags.max_read_key();
            if begin > max || end > max {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            state.buffer.add_read_conflict_range(begin, end)
        });
    }

    /// `addWriteConflictRange(begin, end)`: other transactions that read in
    /// `[begin, end)` conflict with this one, as if it had written there.
    pub fn add_write_conflict_range(&self, begin: &[u8], end: &[u8]) {
        self.write(|state| {
            let max = state.flags.max_write_key();
            if begin > max || end > max {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            state.buffer.add_write_conflict_range(begin, end)
        });
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
        let epoch = self.state().epoch;
        let result = match self.guarded(epoch, self.commit_inner(&sent, epoch)).await {
            Ok(result) => result,
            // Interrupted (cancelled, timed out, reset) once the request may have left.
            Err(e) if sent.load(Ordering::SeqCst) => Err(e.into_maybe_committed()),
            Err(e) => Err(e),
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
                return Err(e);
            }
            state.check_not_committing()?;
            // `commitStarted`: from now on a new read or write is `used_during_commit`.
            state.commit = CommitStage::Started(sent.clone());
        }
        // `wait(ryw->resetPromise.getFuture() || ryw->reading)` (`ReadYourWrites.actor.cpp`
        // 7.4:1359-1360, 8.0:1340-1341): the reads in progress end (and add their conflict
        // ranges) before the commit takes them. (A reset or cancellation interrupts the wait:
        // the commit runs `guarded`.)
        let (buffer, options) = loop {
            let ended = {
                let mut state = self.state();
                if state.epoch != epoch {
                    return Err(Error::new(TRANSACTION_CANCELLED));
                }
                if state.reading.count == 0 {
                    state.reading.closed = true;
                    let limits = *state.buffer.limits();
                    let buffer = std::mem::replace(&mut state.buffer, CommitBuffer::new(limits));
                    state.stamped_keys.clear();
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
        let (result, current) = {
            let mut state = self.state();
            let current = state.epoch == epoch;
            match outcome {
                Ok(outcome) => {
                    let result = CommitResult::from(outcome);
                    if current {
                        state.commit = CommitStage::Committed(result.clone());
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
        let read_version = if buffer.is_empty() {
            // Nothing to send: the committer returns without using it.
            INVALID_VERSION
        } else {
            self.read_version(true).await?
        };
        // The committer sets `sent` once the request may reach a proxy.
        self.inner
            .db
            .inner
            .committer
            .commit_buffer_tracked(buffer, read_version, &options, sent)
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
    /// * The stamp when the commit succeeds (the same as [`versionstamp`](Self::versionstamp)).
    /// * `no_commit_version` (2021) if the commit has nothing to write (a read-only
    ///   transaction).
    /// * `transaction_invalid_version` (2020) if the commit fails.
    /// * `transaction_cancelled` (1025) if the transaction is reset (or `on_error` retries
    ///   it) first, and the error of [`cancel`](Self::cancel), of a timeout or of
    ///   `used_during_commit` if one of them kills it first.
    ///
    /// Unlike C++ (where a call after the commit started is `used_during_commit`), a call
    /// after the commit **succeeded** returns the stamp at once, and does not disturb the
    /// transaction; a call while the commit is in progress is `used_during_commit`.
    pub fn get_versionstamp(
        &self,
    ) -> impl Future<Output = Result<[u8; 10], Error>> + Send + 'static {
        enum Pending {
            Done(Result<[u8; 10], Error>),
            Wait(oneshot::Receiver<Result<[u8; 10], Error>>),
        }
        let pending = {
            let mut state = self.state();
            if let Err(e) = self.check_handle(&state) {
                Pending::Done(Err(e))
            } else if let CommitStage::Committed(result) = &state.commit {
                Pending::Done(result.versionstamp().ok_or(Error::new(NO_COMMIT_VERSION)))
            } else if let Some(e) = state.dead.clone() {
                Pending::Done(Err(e))
            } else if let Err(e) = state.check_not_committing() {
                Pending::Done(Err(e))
            } else {
                let (tx, rx) = oneshot::channel();
                state.versionstamp_waiters.push(tx);
                Pending::Wait(rx)
            }
        };
        async move {
            match pending {
                Pending::Done(result) => result,
                // The transaction was dropped or reset without an answer.
                Pending::Wait(rx) => rx
                    .await
                    .unwrap_or_else(|oneshot::Canceled| Err(Error::new(TRANSACTION_CANCELLED))),
            }
        }
    }

    /// `onError(error)`: if `error` is one C++'s `onError` retries ([`Error::on_error`])
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
                error.on_error()
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
            state.reset_for_retry(self.inner.db.inner.committer.limits());
            (delay, state.epoch)
        };
        let (delay, epoch) = delay;
        self.guarded(epoch, async {
            self.rt().sleep(delay).await;
            Ok(())
        })
        .await?
    }

    /// `setOption(option)`. `invalid_option_value` (2006) for a value out of range (the
    /// option is then unchanged). See [`TransactionOption`] for which options survive
    /// [`on_error`](Self::on_error).
    pub fn set_option(&self, option: TransactionOption) -> Result<(), Error> {
        let invalid = |what: &'static str| Error::with_message(INVALID_OPTION_VALUE, what);
        let millis = |d: Duration, what| {
            let ms = d.as_millis();
            if ms > MAX_MILLIS {
                Err(invalid(what))
            } else {
                Ok(Duration::from_millis(ms as u64))
            }
        };
        let mut state = self.state();
        self.check_handle(&state)?;
        match option {
            TransactionOption::Timeout(d) => {
                let d = millis(d, "the timeout is at most i32::MAX milliseconds")?;
                state.persistent.timeout = (!d.is_zero()).then_some(d);
                // `resetTimeout`: the operations in progress see the new deadline (and fail
                // at once if it has passed).
                if state.check_alive(self.rt().now()).is_ok() {
                    state.interrupt(|| Interrupt::Deadline);
                }
            }
            TransactionOption::RetryLimit(n) => {
                if n < -1 {
                    return Err(invalid("the retry limit is -1 or more"));
                }
                state.persistent.retry_limit = u32::try_from(n).ok();
            }
            TransactionOption::MaxRetryDelay(d) => {
                state.persistent.max_retry_delay =
                    millis(d, "the max retry delay is at most i32::MAX milliseconds")?;
            }
            TransactionOption::SizeLimit(n) => {
                let limit = state.buffer.limits().transaction_size_limit;
                if u64::from(n) < 32 || u64::from(n) > limit {
                    return Err(invalid(
                        "the size limit must be from 32 to TRANSACTION_SIZE_LIMIT",
                    ));
                }
                state.flags.size_limit = u64::from(n);
            }
            TransactionOption::CausalWriteRisky => state.flags.causal_write_risky = true,
            TransactionOption::CausalReadRisky => state.flags.causal_read_risky = true,
            TransactionOption::LockAware => state.flags.lock_aware = true,
            TransactionOption::UseProvisionalProxies => {
                state.flags.use_provisional_proxies = true;
            }
            TransactionOption::ReportConflictingKeys => {
                state.flags.report_conflicting_keys = true;
            }
            TransactionOption::AccessSystemKeys => state.flags.access_system_keys = true,
            TransactionOption::ReadSystemKeys => state.flags.read_system_keys = true,
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
    /// [`Transaction::get`] without a read conflict range.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.tx.get_with(key, true).await
    }

    /// [`Transaction::get_key`] without a read conflict range.
    pub async fn get_key(&self, selector: &KeySelector) -> Result<Vec<u8>, Error> {
        self.tx.get_key_with(selector, true).await
    }
}
