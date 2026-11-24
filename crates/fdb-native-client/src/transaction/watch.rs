//! [`Transaction::watch`] and the hooks that keep a transaction's watches right: the commit
//! that starts them and the failures that cancel them (the writes that trigger them are in
//! `write.rs`, after each write's checks). The watch machinery is [`crate::watch`]; see its
//! documentation for what C++ does.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use fdb_native_transport::runtime::Runtime;
use futures_channel::oneshot;

use super::{
    CommitStage, ReadFuture, ReadGuard, ReadStart, ReadTask, RywView, SYSTEM_KEYS_END, Transaction,
    TxSource,
};
use crate::error::Error;
use crate::error::codes::*;
use crate::read::METADATA_VERSION_KEY;
use crate::read::util::{Either, race};
use crate::ryw;
use crate::watch::{PendingId, Watch};

/// What the read of a watch needs.
struct WatchRead {
    key: Vec<u8>,
    id: PendingId,
    /// Resolves if a write fires the watch during the read.
    fired: oneshot::Receiver<()>,
    start: ReadStart,
    /// The transaction's writes when `watch` was called.
    view: RywView,
}

impl<R: Runtime> Transaction<R> {
    /// `watch(key)`: a [`Watch`] that resolves when the value of `key` changes, once this
    /// transaction has committed.
    ///
    /// Like `ReadYourWritesTransaction::watch`, this reads the key's value as the
    /// transaction sees it (a [`get`](Self::get) of it) and counts the watch against the
    /// database's cap. The watch is **registered when the transaction commits** (also a
    /// read-only one, which commits without contacting the cluster), watching that value as
    /// of the commit. If the transaction fails to commit, or is reset or dropped first, the
    /// watch fails (see [`Watch`]).
    ///
    /// * **The value watched** is what `get(key)` returns, with the transaction's writes
    ///   before the call: a set or clear of the key (with any atomic operations since folded
    ///   in, [`crate::atomic`]) gives it without a read; otherwise the key is read at the read
    ///   version (or found in what the transaction read already), **with a read conflict
    ///   range**, and the transaction's atomic operations on it, if any, are applied: a
    ///   commit then fails with `not_committed` if another transaction changed the key after
    ///   the read version, so the value is the key's value at the commit. The read counts as
    ///   a read in progress: a commit started meanwhile waits for it, and registers the
    ///   watch.
    /// * **Later writes** of the transaction to the key that give it another value than the
    ///   one watched resolve the watch at once; an atomic operation on the key always does.
    ///
    /// This is the behaviour of C++'s read-your-writes layer (see the [module
    /// documentation](super)).
    ///
    /// As a read, the watch **starts when `watch` is called**: its checks, its registration
    /// with the transaction and its read happen whether or not the future is awaited before
    /// the commit (C++'s `watch` future). The future resolves to the [`Watch`] once the value
    /// is known; it may be awaited after the commit:
    ///
    /// ```no_run
    /// # async fn example(db: fdb_native_client::Database) -> Result<(), fdb_native_client::Error> {
    /// let tx = db.create_transaction();
    /// let watch = tx.watch(b"config"); // registered now
    /// tx.commit().await?; // starts the watch
    /// let watch = watch.await?;
    /// watch.await?; // "config" changed
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// **Dropping the future before its read ends cancels the watch**, as C++'s `watch` actor
    /// is cancelled with its future (`RYWImpl::watch`, 7.4:1284-1335, 8.0:1278-1327): the
    /// read stops, adds no read conflict range, and the watch is neither registered nor
    /// counted; a commit waiting for the read goes on. (A future kept but not polled is not
    /// dropped: its read goes on, and the commit waits for it.) Once the read ended, the
    /// watch is the future's answer: dropping the future then drops the [`Watch`].
    ///
    /// # Errors
    ///
    /// `watches_disabled` (1034) with
    /// [`ReadYourWritesDisable`](super::TransactionOption::ReadYourWritesDisable).
    /// `too_many_watches` (1032): the database has [`max_outstanding_watches`] outstanding
    /// watches. `key_outside_legal_range` (2004) for a key from `\xff` on (unless the
    /// `ReadSystemKeys` or `AccessSystemKeys` option is set; `\xff/metadataVersion` may
    /// always be watched, as C++ allows) or from `\xff\xff` on,
    /// `key_too_large` (2102), `accessed_unreadable` (1036) for a key whose value a pending
    /// versionstamped operation of the transaction makes unknown (unless
    /// [`BypassUnreadable`](super::TransactionOption::BypassUnreadable)), the errors of
    /// [`get`](Self::get), and `used_during_commit` (2017) after the commit started
    /// ([maybe committed](Error::is_maybe_committed) once the commit may have been sent; the
    /// commit itself goes on).
    ///
    /// [`max_outstanding_watches`]: crate::watch::WatchConfig::max_outstanding_watches
    pub fn watch(&self, key: &[u8]) -> impl Future<Output = Result<Watch, Error>> + Send + use<R> {
        match self.begin_watch(key) {
            Ok((read, guard)) => {
                let tx = self.task_handle();
                let (mut answer, rx) = oneshot::channel();
                // Detached, as every read's task; but the future's drop cancels the read
                // (C++'s `watch` actor is cancelled with its future, and its read with it:
                // `readWithConflictRange` adds the range only when the read ends).
                drop(self.rt().spawn(async move {
                    let id = read.id.clone();
                    let read = race(tx.watch_read(read, &guard), answer.cancellation()).await;
                    match read {
                        Either::Left(result) => {
                            drop(guard);
                            let _ = answer.send(result);
                        }
                        // Dropped before its read ended: no range, no registration.
                        Either::Right(()) => {
                            let mut state = tx.state();
                            if state.epoch == guard.epoch {
                                state.watches.remove(&id);
                            }
                            drop(state);
                            drop(guard);
                        }
                    }
                }));
                ReadFuture::Task(ReadTask(rx))
            }
            Err(e) => ReadFuture::ready(Err(e)),
        }
    }

    /// The checks of `ReadYourWritesTransaction::watch` and the registration of the watch
    /// with the transaction, made when [`watch`](Self::watch) is called.
    fn begin_watch(&self, key: &[u8]) -> Result<(WatchRead, ReadGuard<R>), Error> {
        let mut state = self.state();
        self.check_handle(&state)?;
        if let Some(e) = state.deferred.clone() {
            return Err(e);
        }
        // `checkUsedDuringCommit()` before `resetPromise.isSet()` (7.4:2441-2446,
        // 8.0:2357-2362), but without killing the transaction (see the differences in
        // `crate::watch`).
        if let Some(e) = state.used_during_commit() {
            return Err(e);
        }
        state.check_alive(self.rt().now())?;
        let start = ReadStart {
            epoch: state.epoch,
            use_provisional_proxies: state.flags.use_provisional_proxies,
            max_read_key: state.flags.max_read_key(),
        };
        // `ReadYourWritesTransaction::watch` (7.4:2440-2459, 8.0:2356-2375).
        let Some(view) = Self::ryw_view(&state, false) else {
            return Err(Error::new(WATCHES_DISABLED));
        };
        // `\xff/metadataVersion` may be watched without the system key options, as it may
        // be read.
        if key >= SYSTEM_KEYS_END || (key >= start.max_read_key && key != METADATA_VERSION_KEY) {
            return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
        }
        if key.len() > state.buffer.limits().max_write_key_size(key) {
            return Err(Error::new(KEY_TOO_LARGE));
        }
        // `RYWImpl::watch`: `watchMap[key].push_back(watch)`, then the read, counted in
        // `ryw->reading` until the watch is registered (or failed).
        let (id, fired) = state.watches.register(key, start.use_provisional_proxies);
        let guard = self.start_read(&mut state);
        let read = WatchRead {
            key: key.to_vec(),
            id,
            fired,
            start,
            view,
        };
        Ok((read, guard))
    }

    /// `RYWImpl::watch` (7.4:1284-1335, 8.0:1278-1327) from the read on: the value read
    /// (`readWithConflictRange(GetValueReq(key), Snapshot::False)`, unless a write fires the
    /// watch first), its read conflict range, then the registration.
    async fn watch_read(&self, read: WatchRead, guard: &ReadGuard<R>) -> Result<Watch, Error> {
        let WatchRead {
            key,
            id,
            fired,
            start,
            view,
        } = read;
        let epoch = start.epoch;
        let source = TxSource::new(self, &start);
        let ryw_read = view.read(&source, start.max_read_key);
        let mut value = pin!(ryw::read::get(&ryw_read, &key));
        let value = match race(fired, value.as_mut()).await {
            Either::Right(result) => result,
            // `onChangeTrigger`: a write fired it; the read is abandoned.
            Either::Left(Ok(())) => {
                let mut state = self.state();
                if state.epoch != epoch {
                    return Err(Error::new(TRANSACTION_CANCELLED));
                }
                state.watches.remove(&id);
                return Ok(Watch::ready(Ok(())));
            }
            // The entry went (a reset): the read finds out.
            Either::Left(Err(oneshot::Canceled)) => value.await,
        };
        let mut state = self.state();
        if state.epoch != epoch || state.epoch != guard.epoch {
            return Err(Error::new(TRANSACTION_CANCELLED));
        }
        let value = match value {
            Ok(value) => value,
            Err(e) => {
                state.watches.remove(&id);
                return Err(e);
            }
        };
        if state.reading.closed {
            // Not possible while the guard counts the read; as `add_read_conflict`.
            state.watches.remove(&id);
            return Err(state
                .used_during_commit()
                .unwrap_or(Error::new(USED_DURING_COMMIT)));
        }
        // `addConflictRange(GetValueReq)`: the key, if its value depended on the database
        // (unmodified, or dependent writes) in the writes the read saw.
        ryw::read::add_get_conflict(&mut state.ryw.read_conflicts, &key, &ryw_read.writes);
        let counter = &self.inner.db.inner.watches.counter;
        state.watches.finish_read(&id, value, counter)
    }

    /// The commit succeeded (`setupWatches`): starts the watches at the commit version, or
    /// at the read version for a transaction that committed nothing.
    pub(super) async fn start_watches(&self, committed_version: i64) {
        let armed = {
            let mut state = self.state();
            if state.watches.is_empty() {
                return;
            }
            state.watches.take_registered()
        };
        if armed.is_empty() {
            return;
        }
        let version = if committed_version > 0 {
            committed_version
        } else {
            match self.read_version(false).await {
                Ok(version) => version,
                Err(e) => {
                    for armed in armed {
                        armed.fail(&e);
                    }
                    return;
                }
            }
        };
        self.inner.db.inner.watches.start(armed, version);
    }

    /// The commit this call started failed with `error` (`cancelWatches(e)`); a commit
    /// that was refused (a deferred error, `used_during_commit`) leaves the watches be.
    pub(super) fn fail_watches(&self, sent: &Arc<AtomicBool>, error: &Error) {
        let mut state = self.state();
        let ours = matches!(&state.commit, CommitStage::Started(s) if Arc::ptr_eq(s, sent));
        if ours {
            state.watches.cancel_all(error);
        }
    }
}
