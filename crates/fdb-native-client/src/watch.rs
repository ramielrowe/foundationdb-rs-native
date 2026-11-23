//! Watches: [`Transaction::watch`](crate::Transaction::watch) returns a [`Watch`], a future
//! that resolves once the value of a key changes.
//!
//! ```no_run
//! # async fn example(db: fdb_native_client::Database) -> Result<(), fdb_native_client::Error> {
//! // Register the watch in a transaction, commit it, then wait for the change.
//! let watch = db
//!     .run(|tx, _| async move { tx.watch(b"config").await })
//!     .await?;
//! watch.await?; // some other transaction changed "config"
//! # Ok(())
//! # }
//! ```
//!
//! # What C++ does, and what is ported
//!
//! Citations are `file:line` on `origin/release-7.4` / `origin/release-8.0` of the
//! FoundationDB repository: `NativeAPI` is `fdbclient/NativeAPI.actor.cpp` (7.4) /
//! `NativeAPI.cpp` (8.0); `RYW` is `ReadYourWrites.actor.cpp` / `ReadYourWrites.cpp`;
//! `ClientKnobs` is `fdbclient/ClientKnobs.cpp`. The logic is the same on both releases
//! (8.0 converted the actors to coroutines and removed tenants).
//!
//! ## In the transaction (the read-your-writes layer)
//!
//! * **`watch(key)`** (`ReadYourWritesTransaction::watch`, RYW 7.4:2440-2459, 8.0:2356;
//!   `RYWImpl::watch`, 7.4:1284-1340, 8.0:1278): checks the transaction (`used_during_commit`,
//!   the key's range `key_outside_legal_range`, its size `key_too_large`), reads the key
//!   **with a read conflict range** (the transaction's own write if it has one), and
//!   registers a `Watch` object with the transaction (`Transaction::watch`, NativeAPI
//!   7.4:6008-6017, 8.0:3752), which **counts it against the cap**: `increaseWatchCounter`
//!   (NativeAPI 7.4:2407-2413, 8.0:339) throws `too_many_watches` (1032) when
//!   `outstandingWatches >= maxOutstandingWatches` (`DEFAULT_MAX_OUTSTANDING_WATCHES`
//!   10 000, ClientKnobs 7.4:127, 8.0:235; the `MAX_WATCHES` database option sets it, up to
//!   `ABSOLUTE_MAX_WATCHES` 1 000 000). The count goes down again when the watch finishes
//!   (fires, fails, or its future is dropped; `watch`, NativeAPI 7.4:5951-6003).
//! * **Later writes to the key** (`triggerWatches`, RYW 7.4:1243-1282, called by `set`
//!   2354, `clear` 2403 and 2437, `atomicOp` 2296): a watch whose value was read, or whose
//!   key was written since, fires **at once** (before the commit) if the new value differs
//!   from either; an atomic operation (`valueKnown = false`) always fires it. A write of the
//!   same value changes nothing.
//! * **At commit** (`commitAndWatch`, NativeAPI 7.4:7248-7280, 8.0:4938; `setupWatches`,
//!   7.4:6743-6762, 8.0:4478): after a successful commit each watch of the transaction is
//!   started with `watchValueMap`, at version `getCommittedVersion() > 0 ? committed :
//!   getReadVersion()`. **A read-only transaction commits without contacting the cluster
//!   and still starts its watches, at its read version.** If the commit fails, every watch
//!   fails with the commit's error (`cancelWatches(e)`, 7.4:6735-6741, 8.0:4470); when the
//!   transaction is reset (`reset`, `onError`) or destroyed before committing, they fail with
//!   `transaction_cancelled` (1025).
//!
//! ## In the database (`DatabaseContext`)
//!
//! * **The watch map** (`getWatchMetadata`, `setWatchMetadata`, `increaseWatchRefCount`,
//!   `decreaseWatchRefCount`, `fdbclient/DatabaseContext.cpp` 7.4:23-80;
//!   `DatabaseContext.h` 7.4:126-160, 839-860): one `WatchMetadata` per key with the
//!   parameters (value, version) and a promise every watcher of the key waits on; one
//!   storage-server watch (`watchFutureSS`) serves them all. A multiset of the versions
//!   of the watchers of a key counts them: when it empties, the storage watch is cancelled
//!   and the metadata deleted.
//! * **`getWatchFuture`** (NativeAPI 7.4:4407-4455, 8.0:2221-2265) joins a watch to the map:
//!   1. key not in the map: new metadata, start the storage watch;
//!   2. same value: join it (the metadata takes the newer version);
//!   3. different value, newer version: the watchers of the old metadata fire (the value
//!      changed between the two versions), the old storage watch is cancelled, new metadata;
//!   4. different value, older version: fires at once (the value changed since);
//!   5. different value, same version: `sameVersionDiffValue` (7.4:4362-4405) reads the key's
//!      current value to settle it.
//! * **`watchStorageServerResp`** (7.4:4312-4360, 8.0:2135-2184): runs `watchValue` for the
//!   metadata's parameters; when it returns a version at or past the metadata's, the
//!   watchers fire; if it is older (the metadata was updated to a newer version meanwhile)
//!   it goes around again (the "ABA" case); on `future_version` it goes around again, on
//!   another error the watchers fail with it.
//! * **`watchValue`** is [`Reader::watch_value`](crate::Reader::watch_value).
//!
//! ## Differences
//!
//! * **No read-your-writes layer yet.** `Transaction::get` does not see the transaction's own
//!   writes, but `watch` does what `ReadYourWritesTransaction::watch` does: the watched value
//!   is the transaction's own write to the key if there is one (found in the commit buffer:
//!   a `set` gives the value, a `clear` or a covering `clear_range` none), else the
//!   value in the database at the read version. A pending atomic operation on the key (whose
//!   result only the cluster can compute) makes the watched value **the key's value at the
//!   commit version**, read once the commit has succeeded. The later writes of the
//!   transaction act as `triggerWatches` has them (above).
//! * **`getFutureReferenceCount() == 1`.** `watchStorageServerResp` treats a metadata with
//!   one future left as one without any: it deletes it silently on an error, which leaves
//!   its single watcher with `broken_promise`. Here the storage watch is abandoned only when
//!   no watcher is left; a watcher that is left gets the error.
//! * **Shutdown.** [`Database::shutdown`](crate::Database::shutdown) fails every watch of the
//!   map with `operation_cancelled` (1101). Watches in transactions that are not committed
//!   yet fail when the transaction does (its commit fails with `operation_cancelled`).
//! * **Not ported:** switching the cluster file (`connectionFileChanged`, `restartWatch`),
//!   tenants, watching `\xff/metadataVersion` (needs the metadata version key exception of
//!   `get`), tags and debug ids.

use std::collections::{BTreeMap, HashMap};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use fdb_native_schema::domain::{Mutation, MutationType};
use fdb_native_transport::runtime::{Runtime, TaskHandle};
use futures_channel::oneshot;

use crate::database::ClientTls;
use crate::error::Error;
use crate::error::codes::*;
use crate::read::util::{Either, race};
use crate::read::{Reader, WatchTimings};

/// `DEFAULT_MAX_OUTSTANDING_WATCHES` (ClientKnobs 7.4:127, 8.0:235).
pub const DEFAULT_MAX_WATCHES: usize = 10_000;
/// `ABSOLUTE_MAX_WATCHES` (ClientKnobs 7.4:128, 8.0:236): the most the cap can be set to.
pub const ABSOLUTE_MAX_WATCHES: usize = 1_000_000;

/// The watch knobs of a database. [`WatchConfig::default`] has C++'s values.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct WatchConfig {
    /// `MAX_WATCHES` (database option; default `DEFAULT_MAX_OUTSTANDING_WATCHES`, 10 000):
    /// the most watches outstanding at once; one more is refused with `too_many_watches`.
    /// At most [`ABSOLUTE_MAX_WATCHES`] (checked by [`Database::set_max_watches`], and
    /// clamped when opening).
    ///
    /// [`Database::set_max_watches`]: crate::Database::set_max_watches
    pub max_outstanding_watches: usize,
    /// `WATCH_POLLING_TIME` (1 s): how long a watch waits before it is sent again after the
    /// storage server cancelled it (`watch_cancelled`, too many watches there) or was
    /// behind (`process_behind`).
    pub polling_time: Duration,
}

impl Default for WatchConfig {
    fn default() -> Self {
        WatchConfig {
            max_outstanding_watches: DEFAULT_MAX_WATCHES,
            polling_time: Duration::from_secs(1),
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---- the cap ------------------------------------------------------------------------------

/// `outstandingWatches` and `maxOutstandingWatches`.
#[derive(Debug)]
pub(crate) struct Counter {
    outstanding: AtomicUsize,
    max: AtomicUsize,
}

impl Counter {
    fn new(max: usize) -> Counter {
        Counter {
            outstanding: AtomicUsize::new(0),
            max: AtomicUsize::new(max.min(ABSOLUTE_MAX_WATCHES)),
        }
    }

    /// `increaseWatchCounter`.
    pub(crate) fn acquire(self: &Arc<Self>) -> Result<Slot, Error> {
        let max = self.max.load(Ordering::SeqCst);
        self.outstanding
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < max).then_some(n + 1)
            })
            .map_err(|_| Error::new(TOO_MANY_WATCHES))?;
        Ok(Slot(self.clone()))
    }
}

/// One outstanding watch of the count; `decreaseWatchCounter` when dropped.
#[derive(Debug)]
pub(crate) struct Slot(Arc<Counter>);

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.outstanding.fetch_sub(1, Ordering::SeqCst);
    }
}

// ---- the watch ----------------------------------------------------------------------------

/// A watch: a future that resolves with `Ok(())` once the value of the key changes, or fails.
/// Returned by [`Transaction::watch`](crate::Transaction::watch).
///
/// * **It starts when the transaction commits.** Until then it is pending; after a successful
///   commit it watches the key's value **as of the commit**, and resolves when that value
///   changes (or has already changed by the time the watch reaches a storage server, so a
///   change right after the commit is not missed). The commit's own writes are the value
///   watched: it does not fire for them.
/// * **It fails** with the commit's error if the transaction does not commit, with
///   `transaction_cancelled` (1025) if the transaction is reset or dropped first (so a
///   [`Database::run`](crate::Database::run) retry cancels the watches of the failed
///   attempt), with `too_many_watches` (1032) from [`Transaction::watch`](crate::Transaction::watch) itself if the
///   database has too many outstanding watches, and with `operation_cancelled` (1101) if the
///   database is shut down.
/// * **Dropping it cancels it:** the database stops watching the key if no other watch needs
///   it, and the watch no longer counts against the cap.
/// * **Several watches** of one key and value share one request to the storage server.
///
/// `Ok(())` does not say that the value differs from the one watched when you read it: a
/// value that changed and changed back may fire the watch (and a change may be missed if it is
/// undone before the storage server looks); read the key again to see its value.
#[must_use = "a watch does nothing once dropped"]
pub struct Watch {
    state: WatchState,
}

enum WatchState {
    Waiting {
        rx: oneshot::Receiver<Result<(), Error>>,
        _slot: Slot,
    },
    Ready(Option<Result<(), Error>>),
}

impl Watch {
    /// A watch that resolved before it started (the value was written differently by the
    /// transaction itself).
    pub(crate) fn ready(result: Result<(), Error>) -> Watch {
        Watch {
            state: WatchState::Ready(Some(result)),
        }
    }

    fn waiting(rx: oneshot::Receiver<Result<(), Error>>, slot: Slot) -> Watch {
        Watch {
            state: WatchState::Waiting { rx, _slot: slot },
        }
    }
}

impl std::fmt::Debug for Watch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match &self.state {
            WatchState::Waiting { .. } => "waiting",
            WatchState::Ready(_) => "ready",
        };
        f.debug_struct("Watch").field("state", &state).finish()
    }
}

impl Future for Watch {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            WatchState::Ready(result) => Poll::Ready(
                result
                    .take()
                    .unwrap_or_else(|| Err(Error::new(OPERATION_CANCELLED))),
            ),
            WatchState::Waiting { rx, .. } => match Pin::new(rx).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(result)) => {
                    // Finished: no longer outstanding (the slot goes with the state).
                    self.state = WatchState::Ready(None);
                    Poll::Ready(result)
                }
                // The transaction ended without starting or failing it.
                Poll::Ready(Err(oneshot::Canceled)) => {
                    self.state = WatchState::Ready(None);
                    Poll::Ready(Err(Error::new(TRANSACTION_CANCELLED)))
                }
            },
        }
    }
}

// ---- a transaction's watches ----------------------------------------------------------------

/// What a watch watches.
#[derive(Debug)]
pub(crate) enum Baseline {
    /// This value (`None`: the key not being set).
    Value(Option<Vec<u8>>),
    /// The key's value at the commit version, not known before.
    AfterCommit,
}

#[derive(Debug)]
enum Phase {
    /// `watch` is reading the value.
    Reading,
    /// Waiting for the commit; resolves through this.
    Registered(oneshot::Sender<Result<(), Error>>),
    /// Fired by a write of the transaction while it was `Reading`.
    Fired,
}

#[derive(Debug)]
struct Pending {
    id: u64,
    key: Vec<u8>,
    /// `valuePresent`/`value`: the value read (`Some(None)`: the key is not set).
    value: Option<Option<Vec<u8>>>,
    /// `setPresent`/`setValue`: the last value a write gave the key since the watch started.
    set: Option<Option<Vec<u8>>>,
    after_commit: bool,
    use_provisional: bool,
    phase: Phase,
}

/// What a write did to a key, for [`PendingWatches::trigger`].
pub(crate) enum Written<'a> {
    /// The key is now this value (`None`: cleared).
    Value(Option<&'a [u8]>),
    /// An atomic operation: its result is not known.
    Unknown,
}

/// A watch to start when the transaction commits.
pub(crate) struct Armed {
    pub(crate) key: Vec<u8>,
    pub(crate) baseline: Baseline,
    pub(crate) use_provisional: bool,
    tx: oneshot::Sender<Result<(), Error>>,
}

impl Armed {
    /// The watch fails with `error` without having started.
    pub(crate) fn fail(self, error: &Error) {
        let _ = self.tx.send(Err(error.clone()));
    }
}

/// The watches of a transaction that has not committed (`Transaction::watches` and
/// `ReadYourWritesTransaction::watchMap`).
#[derive(Debug, Default)]
pub(crate) struct PendingWatches {
    next_id: u64,
    entries: Vec<Pending>,
}

impl PendingWatches {
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Adds a watch of `key` whose value is being read.
    pub(crate) fn register(&mut self, key: &[u8], use_provisional: bool) -> u64 {
        self.next_id += 1;
        self.entries.push(Pending {
            id: self.next_id,
            key: key.to_vec(),
            value: None,
            set: None,
            after_commit: false,
            use_provisional,
            phase: Phase::Reading,
        });
        self.next_id
    }

    /// Forgets watch `id` (its read failed).
    pub(crate) fn remove(&mut self, id: u64) {
        self.entries.retain(|p| p.id != id);
    }

    /// `RYWImpl::watch` after the read: `Watch` already resolved if a write of the
    /// transaction fired it, else registered with the transaction (`Transaction::watch`),
    /// which counts it against the cap.
    pub(crate) fn finish_read(
        &mut self,
        id: u64,
        baseline: Baseline,
        counter: &Arc<Counter>,
    ) -> Result<Watch, Error> {
        let Some(pos) = self.entries.iter().position(|p| p.id == id) else {
            // The transaction was reset meanwhile.
            return Err(Error::new(TRANSACTION_CANCELLED));
        };
        let p = &mut self.entries[pos];
        if matches!(p.phase, Phase::Fired) {
            self.entries.swap_remove(pos);
            return Ok(Watch::ready(Ok(())));
        }
        match baseline {
            Baseline::Value(v) => p.value = Some(v),
            Baseline::AfterCommit => p.after_commit = true,
        }
        if matches!((&p.set, &p.value), (Some(set), Some(value)) if set != value) {
            self.entries.swap_remove(pos);
            return Ok(Watch::ready(Ok(())));
        }
        let slot = match counter.acquire() {
            Ok(slot) => slot,
            Err(e) => {
                self.entries.swap_remove(pos);
                return Err(e);
            }
        };
        let (tx, rx) = oneshot::channel();
        p.phase = Phase::Registered(tx);
        Ok(Watch::waiting(rx, slot))
    }

    /// `triggerWatches` for a write to `begin` (when `end` is `None`) or to `[begin, end)`.
    pub(crate) fn trigger(&mut self, begin: &[u8], end: Option<&[u8]>, written: Written<'_>) {
        self.entries.retain_mut(|p| {
            let hit = match end {
                None => p.key == begin,
                Some(end) => begin <= &p.key[..] && &p.key[..] < end,
            };
            if !hit {
                return true;
            }
            if matches!(&p.phase, Phase::Registered(tx) if tx.is_canceled()) {
                return false;
            }
            let fire = match written {
                Written::Unknown => true,
                Written::Value(v) => {
                    p.set.as_ref().is_some_and(|s| s.as_deref() != v)
                        || p.value.as_ref().is_some_and(|x| x.as_deref() != v)
                }
            };
            if !fire {
                if let Written::Value(v) = written {
                    p.set = Some(v.map(<[u8]>::to_vec));
                }
                return true;
            }
            match std::mem::replace(&mut p.phase, Phase::Fired) {
                Phase::Registered(tx) => {
                    let _ = tx.send(Ok(()));
                    false
                }
                // `watch` finds out when its read is over.
                _ => true,
            }
        });
    }

    /// `setupWatches`: the registered watches, to start.
    pub(crate) fn take_registered(&mut self) -> Vec<Armed> {
        std::mem::take(&mut self.entries)
            .into_iter()
            .filter_map(|p| match p.phase {
                Phase::Registered(tx) => Some(Armed {
                    key: p.key,
                    baseline: if p.after_commit {
                        Baseline::AfterCommit
                    } else {
                        Baseline::Value(p.value.flatten())
                    },
                    use_provisional: p.use_provisional,
                    tx,
                }),
                _ => None,
            })
            .collect()
    }

    /// `cancelWatches(e)`: every registered watch fails with `error`.
    pub(crate) fn cancel_all(&mut self, error: &Error) {
        for p in std::mem::take(&mut self.entries) {
            if let Phase::Registered(tx) = p.phase {
                let _ = tx.send(Err(error.clone()));
            }
        }
    }
}

/// What a transaction's own writes say about a key.
pub(crate) enum Own {
    /// Nothing: read the database.
    None,
    /// The key's value after the writes (`None`: cleared).
    Known(Option<Vec<u8>>),
    /// An atomic operation is pending: only the cluster can say.
    Unknown,
}

/// What the buffered `mutations` make of `key`, in order.
pub(crate) fn own_value(mutations: &[Mutation], key: &[u8]) -> Own {
    let mut own = Own::None;
    for m in mutations {
        match m.kind {
            MutationType::SET_VALUE if m.param1 == key => {
                own = Own::Known(Some(m.param2.clone()));
            }
            MutationType::CLEAR_RANGE => {
                if &m.param1[..] <= key && key < &m.param2[..] {
                    own = Own::Known(None);
                }
            }
            // The key of a versionstamped key is not the one written.
            MutationType::SET_VERSIONSTAMPED_KEY => {}
            MutationType::SET_VALUE => {}
            _ if m.param1 == key => own = Own::Unknown,
            _ => {}
        }
    }
    own
}

// ---- the database's watches -------------------------------------------------------------------

type Waiter = oneshot::Sender<Result<i64, Error>>;

/// `WatchMetadata` and its parameters.
struct Entry<R: Runtime> {
    id: u64,
    /// `parameters->value`.
    value: Option<Vec<u8>>,
    /// `parameters->version`.
    version: i64,
    use_provisional: bool,
    /// `watchPromise`'s futures.
    waiters: Vec<Waiter>,
    /// `watchFutureSS`.
    task: Option<R::Task<()>>,
}

struct MapState<R: Runtime> {
    closed: bool,
    next_id: u64,
    /// `watchMap`.
    map: HashMap<Vec<u8>, Entry<R>>,
    /// `watchCounterMap`: the versions of the watchers of each key.
    refs: HashMap<Vec<u8>, BTreeMap<i64, usize>>,
}

/// What `getWatchFuture` decided.
enum Join {
    Wait(oneshot::Receiver<Result<i64, Error>>),
    /// The value changed already.
    Fired,
    /// `sameVersionDiffValue`.
    SameVersion,
}

/// The watch state of a database (`DatabaseContext`'s `watchMap`, `watchCounterMap`,
/// `outstandingWatches` and `maxOutstandingWatches`).
pub(crate) struct WatchContext<R: Runtime> {
    rt: R,
    reader: Reader<R, ClientTls>,
    timings: WatchTimings,
    pub(crate) counter: Arc<Counter>,
    state: Mutex<MapState<R>>,
}

/// One watcher's count in `watchCounterMap` (`WatchRefCountUpdater`).
struct RefGuard<R: Runtime> {
    ctx: Arc<WatchContext<R>>,
    key: Vec<u8>,
    version: i64,
}

impl<R: Runtime> RefGuard<R> {
    /// `increaseWatchRefCount`.
    fn new(ctx: &Arc<WatchContext<R>>, key: &[u8], version: i64) -> RefGuard<R> {
        *lock(&ctx.state)
            .refs
            .entry(key.to_vec())
            .or_default()
            .entry(version)
            .or_default() += 1;
        RefGuard {
            ctx: ctx.clone(),
            key: key.to_vec(),
            version,
        }
    }
}

impl<R: Runtime> Drop for RefGuard<R> {
    /// `decreaseWatchRefCount`: the last watcher of a key takes its storage watch and its
    /// metadata with it.
    fn drop(&mut self) {
        let mut abort = None;
        {
            let mut st = lock(&self.ctx.state);
            let Some(versions) = st.refs.get_mut(&self.key) else {
                return;
            };
            let Some(count) = versions.get_mut(&self.version) else {
                return;
            };
            *count -= 1;
            if *count == 0 {
                versions.remove(&self.version);
            }
            if versions.is_empty() {
                st.refs.remove(&self.key);
                if let Some(entry) = st.map.remove(&self.key) {
                    abort = entry.task;
                }
            }
        }
        if let Some(task) = abort {
            task.abort();
        }
    }
}

fn live(waiters: &mut Vec<Waiter>) {
    waiters.retain(|w| !w.is_canceled());
}

impl<R: Runtime> WatchContext<R> {
    pub(crate) fn new(
        rt: R,
        reader: Reader<R, ClientTls>,
        config: &WatchConfig,
        future_version_retry_delay: Duration,
    ) -> Arc<WatchContext<R>> {
        Arc::new(WatchContext {
            rt,
            reader,
            timings: WatchTimings {
                polling_time: config.polling_time,
                future_version_retry_delay,
            },
            counter: Arc::new(Counter::new(config.max_outstanding_watches)),
            state: Mutex::new(MapState {
                closed: false,
                next_id: 0,
                map: HashMap::new(),
                refs: HashMap::new(),
            }),
        })
    }

    /// `maxOutstandingWatches`.
    pub(crate) fn set_max(&self, max: usize) {
        self.counter.max.store(max, Ordering::SeqCst);
    }

    /// `outstandingWatches`.
    pub(crate) fn outstanding(&self) -> usize {
        self.counter.outstanding.load(Ordering::SeqCst)
    }

    /// How many keys the watch map has an entry for (`WatchMapSize`).
    pub(crate) fn map_size(&self) -> usize {
        lock(&self.state).map.len()
    }

    /// Fails every watch of the map with `operation_cancelled` and stops their storage
    /// watches; watches started later fail at once.
    pub(crate) fn shutdown(&self) {
        let entries: Vec<Entry<R>> = {
            let mut st = lock(&self.state);
            st.closed = true;
            st.map.drain().map(|(_, e)| e).collect()
        };
        for mut entry in entries {
            for waiter in entry.waiters.drain(..) {
                let _ = waiter.send(Err(Error::new(OPERATION_CANCELLED)));
            }
            if let Some(task) = entry.task.take() {
                task.abort();
            }
        }
    }

    /// `setupWatches`: starts the watches of a committed transaction at `version`
    /// (`watchValueMap`).
    pub(crate) fn start(self: &Arc<Self>, armed: Vec<Armed>, version: i64) {
        for Armed {
            key,
            baseline,
            use_provisional,
            tx,
        } in armed
        {
            if tx.is_canceled() {
                continue;
            }
            // Detached: it ends with the watch.
            drop(
                self.rt.spawn(self.clone().watch_value_map(
                    key,
                    baseline,
                    version,
                    use_provisional,
                    tx,
                )),
            );
        }
    }

    /// `watchValueMap`: the count of the watcher, then `getWatchFuture`; ends early when
    /// the watch is dropped.
    async fn watch_value_map(
        self: Arc<Self>,
        key: Vec<u8>,
        baseline: Baseline,
        version: i64,
        use_provisional: bool,
        mut tx: oneshot::Sender<Result<(), Error>>,
    ) {
        let _count = RefGuard::new(&self, &key, version);
        let work = async {
            let value = match baseline {
                Baseline::Value(v) => v,
                Baseline::AfterCommit => self.read_at(&key, version, use_provisional).await?,
            };
            self.watch_future(&key, value, version, use_provisional)
                .await
        };
        let outcome = {
            let dropped = poll_fn(|cx| tx.poll_canceled(cx));
            race(work, dropped).await
        };
        if let Either::Left(result) = outcome {
            let _ = tx.send(result);
        }
    }

    /// The value of `key` at `version`, once the storage servers have it.
    async fn read_at(
        &self,
        key: &[u8],
        version: i64,
        use_provisional: bool,
    ) -> Result<Option<Vec<u8>>, Error> {
        loop {
            match self.reader.get_with(key, version, use_provisional).await {
                Err(e) if matches!(e.code(), FUTURE_VERSION | PROCESS_BEHIND) => {
                    self.rt.sleep(self.timings.future_version_retry_delay).await;
                }
                other => return other,
            }
        }
    }

    /// `getWatchFuture` (and `sameVersionDiffValue`): resolves when the key's value changes.
    async fn watch_future(
        self: &Arc<Self>,
        key: &[u8],
        value: Option<Vec<u8>>,
        version: i64,
        use_provisional: bool,
    ) -> Result<(), Error> {
        loop {
            match self.join(key, &value, version, use_provisional) {
                Join::Fired => return Ok(()),
                Join::Wait(rx) => {
                    return match rx.await {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) => Err(e),
                        Err(oneshot::Canceled) => Err(Error::new(BROKEN_PROMISE)),
                    };
                }
                Join::SameVersion => {
                    if self
                        .same_version_diff_value(key, &value, version, use_provisional)
                        .await?
                    {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// The cases of `getWatchFuture`.
    fn join(
        self: &Arc<Self>,
        key: &[u8],
        value: &Option<Vec<u8>>,
        version: i64,
        use_provisional: bool,
    ) -> Join {
        let (tx, rx) = oneshot::channel();
        let mut abort = None;
        let join = {
            let mut st = lock(&self.state);
            if st.closed {
                let _ = tx.send(Err(Error::new(OPERATION_CANCELLED)));
                return Join::Wait(rx);
            }
            match st.map.get_mut(key) {
                // Same value: join it, and let it have the newer version.
                Some(entry) if entry.value == *value => {
                    if version > entry.version {
                        entry.version = version;
                        entry.use_provisional = use_provisional;
                    }
                    live(&mut entry.waiters);
                    entry.waiters.push(tx);
                    Join::Wait(rx)
                }
                // Another value, a newer version: the old watchers fire, a new watch.
                Some(entry) if version > entry.version => {
                    let mut old = st.map.remove(key).expect("the entry");
                    for waiter in old.waiters.drain(..) {
                        let _ = waiter.send(Ok(version));
                    }
                    abort = old.task.take();
                    self.insert(&mut st, key, value, version, use_provisional, tx);
                    Join::Wait(rx)
                }
                Some(entry) if entry.version == version => Join::SameVersion,
                // Another value, an older version: it has changed since.
                Some(_) => Join::Fired,
                None => {
                    self.insert(&mut st, key, value, version, use_provisional, tx);
                    Join::Wait(rx)
                }
            }
        };
        if let Some(task) = abort {
            task.abort();
        }
        join
    }

    /// New metadata for `key` with one watcher, and its storage watch.
    fn insert(
        self: &Arc<Self>,
        st: &mut MapState<R>,
        key: &[u8],
        value: &Option<Vec<u8>>,
        version: i64,
        use_provisional: bool,
        waiter: Waiter,
    ) {
        st.next_id += 1;
        let id = st.next_id;
        let task = self
            .rt
            .spawn(self.clone().watch_storage_server_resp(key.to_vec(), id));
        st.map.insert(
            key.to_vec(),
            Entry {
                id,
                value: value.clone(),
                version,
                use_provisional,
                waiters: vec![waiter],
                task: Some(task),
            },
        );
    }

    /// `sameVersionDiffValue`: a watch for another value than the map's, at the same
    /// version: the key's current value settles which is stale. Whether the watch fires.
    async fn same_version_diff_value(
        &self,
        key: &[u8],
        value: &Option<Vec<u8>>,
        version: i64,
        use_provisional: bool,
    ) -> Result<bool, Error> {
        let current = self.read_latest(key, use_provisional).await?;
        let mut abort = None;
        {
            let mut st = lock(&self.state);
            // `val_3 != val_1`: the map's watch is stale.
            if st.map.get(key).is_some_and(|e| e.value != current) {
                let mut old = st.map.remove(key).expect("the entry");
                for waiter in old.waiters.drain(..) {
                    let _ = waiter.send(Ok(version));
                }
                abort = old.task.take();
            }
        }
        if let Some(task) = abort {
            task.abort();
        }
        // `val_3 != val_2`: this one is stale too. Else it joins the map (again).
        Ok(current != *value)
    }

    /// The key's value at a new read version, retrying what a transaction retries.
    async fn read_latest(
        &self,
        key: &[u8],
        use_provisional: bool,
    ) -> Result<Option<Vec<u8>>, Error> {
        let mut backoff = Duration::from_millis(10);
        loop {
            let attempt = async {
                let version = self.reader.get_read_version().await?;
                self.reader.get_with(key, version, use_provisional).await
            };
            match attempt.await {
                Err(e) if e.on_error().is_ok() => {
                    self.rt.sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                }
                other => return other,
            }
        }
    }

    /// `watchStorageServerResp`: the storage watch of `key`'s metadata `id`.
    async fn watch_storage_server_resp(self: Arc<Self>, key: Vec<u8>, id: u64) {
        loop {
            let (value, version, use_provisional) = {
                let st = lock(&self.state);
                match st.map.get(&key) {
                    Some(e) if e.id == id => (e.value.clone(), e.version, e.use_provisional),
                    _ => return,
                }
            };
            let result = self
                .reader
                .watch_value(
                    &key,
                    value.as_deref(),
                    version,
                    use_provisional,
                    self.timings,
                )
                .await;
            let mut st = lock(&self.state);
            let Some(entry) = st.map.get_mut(&key).filter(|e| e.id == id) else {
                return;
            };
            live(&mut entry.waiters);
            let finished = match result {
                // Case 1: the server saw the change at or after the metadata's version.
                Ok(seen) if seen >= entry.version => Some(Ok(seen)),
                // ABA: the metadata moved on to a newer version while the server answered
                // for the old one. Without watchers it is over; else watch again.
                Ok(_) if entry.waiters.is_empty() => None,
                Ok(_) => continue,
                Err(_) if entry.waiters.is_empty() => None,
                Err(e) if e.code() == FUTURE_VERSION => continue,
                Err(e) => Some(Err(e)),
            };
            let mut entry = st.map.remove(&key).expect("the entry");
            drop(st);
            if let Some(outcome) = finished {
                for waiter in entry.waiters.drain(..) {
                    let _ = waiter.send(outcome.clone());
                }
            }
            return;
        }
    }
}
