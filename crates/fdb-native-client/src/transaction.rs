//! [`Transaction`]: reads at one read version, buffered writes, and a commit; with C++'s
//! `onError` retry semantics, its options, timeouts and cancellation.
//!
//! This is the minimal transaction of the vertical slice, the C++ `Transaction` behind a
//! `ReadYourWritesTransaction` **with read-your-writes disabled**:
//!
//! * **Reads do not see the transaction's own writes.** [`get`](Transaction::get) reads the
//!   database at the transaction's read version, whatever the transaction has
//!   [`set`](Transaction::set) or [`clear`](Transaction::clear)ed. (The read-your-writes
//!   layer comes later.)
//! * **Conflict ranges are tracked as C++ tracks them.** Each (non-snapshot) read adds the
//!   key it read as a read conflict range; each write adds its key or range as a write
//!   conflict range; the commit sends them, so a transaction whose reads were changed by
//!   another transaction that committed after its read version fails with `not_committed`
//!   (1020), and [`Database::run`] retries it.
//! * **The read version** is requested lazily, by the first read (or
//!   [`get_read_version`](Transaction::get_read_version)), once for the transaction;
//!   [`set_read_version`](Transaction::set_read_version) supplies one instead. A
//!   transaction that only writes gets one at commit, with `CAUSAL_READ_RISKY`, as C++'s
//!   `commitMutations` does.
//! * **Errors of writes are deferred.** `set`, `clear`, ... return nothing; an error they
//!   meet (`key_too_large`, `key_outside_legal_range`, ...) is kept and returned by the next
//!   read or the commit, as `fdb_c`'s deferred error.
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
//! * The reset keeps the options C++ keeps (`timeout`, `retry_limit`, `max_retry_delay`,
//!   `size_limit`, the "persistent" options), the retry count, the backoff and the time the
//!   timeout counts from; it forgets the read version, the writes and conflict ranges, the
//!   deferred error and the other options.
//!
//! # Timeouts and cancellation
//!
//! The `timeout` option bounds the transaction's life from its creation (or its last
//! [`reset`](Transaction::reset); not from [`on_error`](Transaction::on_error)): once it
//! has passed, every operation, including one in progress and `on_error`, fails with
//! `transaction_timed_out` (1031) until the transaction is reset.
//! [`cancel`](Transaction::cancel) does the same with `transaction_cancelled` (1025), and a
//! [`reset`](Transaction::reset) (or `on_error`) makes the operations in progress fail with
//! `transaction_cancelled`.
//!
//! **A commit that times out or is cancelled while its request may be on its way may still
//! commit.** C++ reports `transaction_timed_out` / `transaction_cancelled`; here that error
//! is also [`is_maybe_committed`](Error::is_maybe_committed), so that it is not mistaken for
//! a commit that did not happen.
//!
//! # Not ported yet
//!
//! Range reads, key selectors, watches, versionstamped operations, read-your-writes,
//! snapshot read-your-writes options, the special key space (`\xff\xff`), transaction
//! tags, priorities, idempotency ids, tenants, the `metadataVersion` key exception, and
//! database-wide default transaction options.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use fdb_native_schema::domain::{INVALID_VERSION, MutationType};
#[cfg(feature = "tokio")]
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_transport::runtime::{Instant, Runtime};
use futures_channel::oneshot;

use crate::commit::{CommitBuffer, CommitLimits, CommitOptions, CommitOutcome};
use crate::database::Database;
use crate::error::codes::*;
use crate::error::{Error, TransactionRetry};
use crate::read::ReadVersionOptions;
use crate::read::util::{Either, never, race};

/// The first key a transaction may not read or write without the system-key options.
const SYSTEM_KEYS_BEGIN: &[u8] = b"\xff";
/// The end of the system keys (and the first key of the special key space).
const SYSTEM_KEYS_END: &[u8] = b"\xff\xff";
/// `extractIntOption`'s upper bound for the millisecond options.
const MAX_MILLIS: u128 = i32::MAX as u128;

/// A transaction option (`FDBTransactionOption`), for [`Transaction::set_option`].
///
/// `Timeout`, `RetryLimit`, `MaxRetryDelay` and `SizeLimit` persist across
/// [`on_error`](Transaction::on_error) (C++'s "persistent" options); the others apply until
/// the transaction is reset, by `on_error` or [`reset`](Transaction::reset). A full reset
/// clears them all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TransactionOption {
    /// `TIMEOUT`: the transaction fails with `transaction_timed_out` once this long has
    /// passed since it was created or last [reset](Transaction::reset). Zero disables it.
    /// At most `i32::MAX` milliseconds.
    Timeout(Duration),
    /// `RETRY_LIMIT`: how many times [`on_error`](Transaction::on_error) may retry; `-1`
    /// (the default) is unlimited.
    RetryLimit(i32),
    /// `MAX_RETRY_DELAY`: the largest backoff [`on_error`](Transaction::on_error) waits
    /// (default 1 s). At most `i32::MAX` milliseconds.
    MaxRetryDelay(Duration),
    /// `SIZE_LIMIT`: the largest size (`getSize()`) the transaction may commit, 32 bytes to
    /// `TRANSACTION_SIZE_LIMIT` (10 000 000, the default).
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

/// The read version, shared by the transaction's reads.
#[derive(Debug)]
enum ReadVersion {
    Unset,
    /// Being requested; those waiting for it.
    Fetching(Vec<oneshot::Sender<Result<i64, Error>>>),
    Set(i64),
    Failed(Error),
}

/// The options that persist across `on_error`.
#[derive(Clone, Copy, Debug)]
struct Persistent {
    timeout: Option<Duration>,
    retry_limit: Option<u32>,
    max_retry_delay: Duration,
    size_limit: u64,
}

/// The options a reset clears.
#[derive(Clone, Copy, Debug, Default)]
struct Flags {
    causal_write_risky: bool,
    causal_read_risky: bool,
    lock_aware: bool,
    use_provisional_proxies: bool,
    report_conflicting_keys: bool,
    access_system_keys: bool,
    read_system_keys: bool,
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
    Started,
}

struct State {
    /// Bumped by every reset: what an operation of an earlier epoch finds out is dropped.
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
    interrupts: Vec<oneshot::Sender<Error>>,
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
                size_limit: CommitOptions::default().size_limit,
            },
            retries: 0,
            backoff: knobs.initial_backoff,
            created: db.inner.rt.now(),
            dead: None,
            commit: CommitStage::Idle,
            interrupts: Vec::new(),
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
        self.interrupt(error);
    }

    fn interrupt(&mut self, error: Error) {
        for interrupt in self.interrupts.drain(..) {
            let _ = interrupt.send(error.clone());
        }
    }

    /// `resetRyow` and `Transaction::reset` as `onError` does them.
    fn reset_for_retry(&mut self, limits: CommitLimits) {
        self.epoch += 1;
        self.interrupt(Error::new(TRANSACTION_CANCELLED));
        self.read_version = ReadVersion::Unset;
        self.buffer = CommitBuffer::new(limits);
        self.deferred = None;
        self.flags = Flags::default();
        self.commit = CommitStage::Idle;
    }

    /// The first deferred error stays.
    fn defer(&mut self, error: Error) {
        self.deferred.get_or_insert(error);
    }

    /// `checkUsedDuringCommit`: an operation after the commit started poisons the
    /// transaction with `used_during_commit`.
    fn check_not_committing(&mut self) -> Result<(), Error> {
        if matches!(self.commit, CommitStage::Started) {
            let e = Error::new(USED_DURING_COMMIT);
            self.kill(e.clone());
            return Err(e);
        }
        Ok(())
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
            size_limit: self.persistent.size_limit,
            ..CommitOptions::default()
        }
    }
}

impl<R: Runtime> Clone for Transaction<R> {
    fn clone(&self) -> Self {
        Transaction {
            inner: self.inner.clone(),
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
            .finish_non_exhaustive()
    }
}

/// Removes a read version request's claim if it is dropped before it finished, so that
/// another read can make the request.
struct FetchGuard<'a, R: Runtime> {
    tx: &'a Transaction<R>,
    epoch: u64,
    done: bool,
}

impl<R: Runtime> Drop for FetchGuard<'_, R> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let mut state = self.tx.state();
        if state.epoch == self.epoch && matches!(state.read_version, ReadVersion::Fetching(_)) {
            // The waiters see their sender dropped and ask again.
            state.read_version = ReadVersion::Unset;
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
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn rt(&self) -> &R {
        &self.inner.db.inner.rt
    }

    /// The database.
    pub fn database(&self) -> &Database<R> {
        &self.inner.db
    }

    /// Runs `op` unless the transaction is dead, and interrupts it if the transaction is
    /// cancelled, reset or times out meanwhile: `Err` with that error.
    async fn guarded<T>(
        &self,
        op: impl Future<Output = Result<T, Error>>,
    ) -> Result<Result<T, Error>, Error> {
        let (interrupted, deadline) = {
            let mut state = self.state();
            state.check_alive(self.rt().now())?;
            let (tx, rx) = oneshot::channel();
            state.interrupts.retain(|s| !s.is_canceled());
            state.interrupts.push(tx);
            (rx, state.deadline())
        };
        let interrupt = async {
            let timeout = async {
                match deadline {
                    Some(deadline) => self.rt().sleep_until(deadline).await,
                    None => never().await,
                }
            };
            match race(interrupted, timeout).await {
                Either::Left(Ok(e)) => e,
                Either::Left(Err(oneshot::Canceled)) => Error::new(TRANSACTION_CANCELLED),
                Either::Right(()) => {
                    let mut state = self.state();
                    match state.check_alive(self.rt().now()) {
                        Err(e) => e,
                        // A full reset moved the deadline: what was in progress is gone.
                        Ok(()) => Error::new(TRANSACTION_CANCELLED),
                    }
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
    async fn read_version(&self, causal_read_risky: bool) -> Result<i64, Error> {
        loop {
            let (waiter, epoch, options) = {
                let mut state = self.state();
                let options = state.read_version_options(causal_read_risky);
                let epoch = state.epoch;
                match &mut state.read_version {
                    ReadVersion::Set(v) => return Ok(*v),
                    ReadVersion::Failed(e) => return Err(e.clone()),
                    ReadVersion::Fetching(waiters) => {
                        let (tx, rx) = oneshot::channel();
                        waiters.push(tx);
                        (Some(rx), epoch, options)
                    }
                    ReadVersion::Unset => {
                        state.read_version = ReadVersion::Fetching(Vec::new());
                        (None, epoch, options)
                    }
                }
            };
            if let Some(waiter) = waiter {
                match waiter.await {
                    Ok(result) => return result,
                    // The request was abandoned: ask again.
                    Err(oneshot::Canceled) => continue,
                }
            }
            let mut guard = FetchGuard {
                tx: self,
                epoch,
                done: false,
            };
            let result = self
                .inner
                .db
                .inner
                .reader
                .get_read_version_with(options)
                .await;
            guard.done = true;
            let mut state = self.state();
            if state.epoch == epoch {
                let next = match &result {
                    Ok(v) => ReadVersion::Set(*v),
                    Err(e) => ReadVersion::Failed(e.clone()),
                };
                if let ReadVersion::Fetching(waiters) =
                    std::mem::replace(&mut state.read_version, next)
                {
                    for waiter in waiters {
                        let _ = waiter.send(result.clone());
                    }
                }
            }
            return result;
        }
    }

    /// `getReadVersion()`: the transaction's read version, requested from a GRV proxy if it
    /// has none yet.
    pub async fn get_read_version(&self) -> Result<i64, Error> {
        if let Some(e) = self.state().deferred.clone() {
            return Err(e);
        }
        self.guarded(self.read_version(false)).await?
    }

    /// `setReadVersion(version)`: read at `version` instead of asking for one. A transaction
    /// that already has a read version gets `read_version_already_set` (2010), a version of
    /// 0 or less `version_invalid` (2011); both are deferred (see the [module
    /// documentation](self)). A version the storage servers no longer have makes reads fail
    /// with `transaction_too_old`, one they do not have yet with `future_version`.
    pub fn set_read_version(&self, version: i64) {
        let mut state = self.state();
        if !matches!(state.read_version, ReadVersion::Unset) {
            state.defer(Error::new(READ_VERSION_ALREADY_SET));
        } else if version <= 0 {
            state.defer(Error::new(VERSION_INVALID));
        } else {
            state.read_version = ReadVersion::Set(version);
        }
    }

    /// `get(key)`: the value of `key` at the transaction's read version, `None` if it is not
    /// set. Adds `key` as a read conflict range. **Does not see the transaction's own
    /// writes.**
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
        let (too_long, use_provisional_proxies) = {
            let mut state = self.state();
            state.check_alive(self.rt().now())?;
            if let Some(e) = state.deferred.clone() {
                return Err(e);
            }
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
            (
                key.len() > limits.max_range_key_size(key),
                state.flags.use_provisional_proxies,
            )
        };
        if too_long {
            // No key in the database is longer than the limit (C++ does not ask either).
            return Ok(None);
        }
        self.guarded(async {
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

    /// A write: `f` on the buffer, unless the transaction is committing (`used_during_commit`)
    /// or dead; its error is deferred.
    fn write(&self, f: impl FnOnce(&mut CommitBuffer, &Flags) -> Result<(), Error>) {
        let mut state = self.state();
        if state.dead.is_some() {
            return;
        }
        if let Err(e) = state.check_not_committing() {
            state.defer(e);
            return;
        }
        let flags = state.flags;
        if let Err(e) = f(&mut state.buffer, &flags) {
            state.defer(e);
        }
    }

    /// `set(key, value)`: writes `value` to `key` when the transaction commits. Errors are
    /// deferred: `key_too_large` (2102, a key over 10 000 bytes), `value_too_large` (2103,
    /// a value over 100 000 bytes), `key_outside_legal_range` (2004, a key from `\xff` on
    /// without the `AccessSystemKeys` option).
    pub fn set(&self, key: &[u8], value: &[u8]) {
        self.write(|buffer, flags| {
            if key >= flags.max_write_key() {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            buffer.set(key, value)
        });
    }

    /// `clear(key)`: removes `key` when the transaction commits.
    pub fn clear(&self, key: &[u8]) {
        self.write(|buffer, flags| {
            if key >= flags.max_write_key() {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            buffer.clear(key);
            Ok(())
        });
    }

    /// `clear(begin, end)`: removes every key in `[begin, end)` when the transaction
    /// commits. Deferred errors: `inverted_range` (2005) if `begin > end`,
    /// `key_outside_legal_range` if either is past `\xff` (without `AccessSystemKeys`).
    pub fn clear_range(&self, begin: &[u8], end: &[u8]) {
        self.write(|buffer, flags| {
            let max = flags.max_write_key();
            if begin > max || end > max {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            buffer.clear_range(begin, end)
        });
    }

    /// `atomicOp(key, operand, op)`: an atomic operation on `key` when the transaction
    /// commits (`ADD_VALUE`, `BIT_AND`, ...; see [`MutationType`]). The versionstamp
    /// operations are not supported yet (`client_invalid_operation`, deferred).
    pub fn atomic_op(&self, key: &[u8], operand: &[u8], op: MutationType) {
        self.write(|buffer, flags| {
            if key >= flags.max_write_key() {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            buffer.atomic_op(key, operand, op)
        });
    }

    /// `addReadConflictRange(begin, end)`: the commit conflicts if another transaction
    /// wrote in `[begin, end)` after the read version, as if the range had been read.
    pub fn add_read_conflict_range(&self, begin: &[u8], end: &[u8]) {
        self.write(|buffer, flags| {
            let max = flags.max_read_key();
            if begin > max || end > max {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            buffer.add_read_conflict_range(begin, end)
        });
    }

    /// `addWriteConflictRange(begin, end)`: other transactions that read in
    /// `[begin, end)` conflict with this one, as if it had written there.
    pub fn add_write_conflict_range(&self, begin: &[u8], end: &[u8]) {
        self.write(|buffer, flags| {
            let max = flags.max_write_key();
            if begin > max || end > max {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            buffer.add_write_conflict_range(begin, end)
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
    /// fails with `used_during_commit` (2017) until it is reset.
    pub async fn commit(&self) -> Result<CommitResult, Error> {
        let sent = AtomicBool::new(false);
        match self.guarded(self.commit_inner(&sent)).await {
            Ok(result) => result,
            // Interrupted (cancelled, timed out, reset) once the request may have left.
            Err(e) if sent.load(Ordering::SeqCst) => Err(e.into_maybe_committed()),
            Err(e) => Err(e),
        }
    }

    async fn commit_inner(&self, sent: &AtomicBool) -> Result<CommitResult, Error> {
        let (buffer, options) = {
            let mut state = self.state();
            if let Some(e) = state.deferred.clone() {
                return Err(e);
            }
            state.check_not_committing()?;
            state.commit = CommitStage::Started;
            (state.buffer.clone(), state.commit_options())
        };
        let read_version = if buffer.is_empty() {
            // Nothing to send: the committer returns without using it.
            INVALID_VERSION
        } else {
            self.read_version(true).await?
        };
        sent.store(true, Ordering::SeqCst);
        let outcome = self
            .inner
            .db
            .inner
            .committer
            .commit_buffer(buffer, read_version, &options)
            .await?;
        Ok(CommitResult::from(outcome))
    }

    /// `onError(error)`: if `error` is one C++'s `onError` retries ([`Error::on_error`])
    /// and the retry limit is not reached, resets the transaction for another attempt and
    /// waits the backoff, then `Ok`; otherwise returns the error (`error` itself, or the
    /// transaction's timeout or cancellation), and the transaction is cancelled until
    /// [`reset`](Self::reset). See the [module documentation](self#retries-on_error).
    ///
    /// An `error` that [`is_maybe_committed`](Error::is_maybe_committed) is retried like a
    /// conflict: the transaction may have committed (see [`Database::run`]).
    pub async fn on_error(&self, error: Error) -> Result<(), Error> {
        let delay = {
            let mut state = self.state();
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
            delay
        };
        self.guarded(async {
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
        match option {
            TransactionOption::Timeout(d) => {
                let d = millis(d, "the timeout is at most i32::MAX milliseconds")?;
                state.persistent.timeout = (!d.is_zero()).then_some(d);
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
                state.persistent.size_limit = u64::from(n);
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
    /// from now. Operations in progress fail with `transaction_cancelled` (1025).
    pub fn reset(&self) {
        let mut state = self.state();
        state.interrupt(Error::new(TRANSACTION_CANCELLED));
        let epoch = state.epoch + 1;
        *state = State::new(&self.inner.db, epoch);
    }

    /// `cancel()`: every operation, in progress or to come, fails with
    /// `transaction_cancelled` (1025) until [`reset`](Self::reset). A commit in progress may
    /// still happen: its error is then [maybe committed](Error::is_maybe_committed).
    pub fn cancel(&self) {
        self.state().kill(Error::new(TRANSACTION_CANCELLED));
    }

    /// How many times [`on_error`](Self::on_error) was called since the transaction was
    /// created or [reset](Self::reset).
    pub fn retries(&self) -> u32 {
        self.state().retries
    }
}

/// The key right after `key`: `key\x00`.
fn key_after(key: &[u8]) -> Vec<u8> {
    let mut end = Vec::with_capacity(key.len() + 1);
    end.extend_from_slice(key);
    end.push(0);
    end
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
}
