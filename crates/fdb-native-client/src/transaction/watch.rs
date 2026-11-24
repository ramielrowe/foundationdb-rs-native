//! [`Transaction::watch`] and the hooks that keep a transaction's watches right: the commit
//! that starts them and the failures that cancel them (the writes that trigger them are in
//! `transaction.rs`, after each write's checks). The watch machinery is [`crate::watch`];
//! see its documentation for what C++ does.

use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use fdb_native_schema::domain::MutationType;
use fdb_native_transport::runtime::Runtime;
use futures_channel::oneshot;

use super::{CommitStage, ReadGuard, SYSTEM_KEYS_END, Transaction};
use crate::error::Error;
use crate::error::codes::*;
use crate::read::key_after;
use crate::read::util::{Either, race};
use crate::watch::{Own, PendingId, Watch, fold, own_writes};

/// What the read of a watch needs.
struct WatchRead {
    key: Vec<u8>,
    id: PendingId,
    /// Resolves if a write of the transaction fires the watch during the read.
    fired: oneshot::Receiver<()>,
    /// The transaction's own atomic operations on the key, to apply to the value read.
    ops: Vec<(MutationType, Vec<u8>)>,
    epoch: u64,
    use_provisional: bool,
    value_size_limit: usize,
}

impl<R: Runtime> Transaction<R> {
    /// `watch(key)`: a [`Watch`] that resolves when the value of `key` changes, once this
    /// transaction has committed.
    ///
    /// Like `ReadYourWritesTransaction::watch`, this works out the key's value as the
    /// transaction sees it (its own writes to the key applied to the database's value) and
    /// counts the watch against the database's cap. The watch is **registered when the
    /// transaction commits** (also a read-only one, which commits without contacting the
    /// cluster), watching that value as of the commit. If the transaction fails to commit,
    /// or is reset or dropped first, the watch fails (see [`Watch`]).
    ///
    /// * **The value watched.** A set or clear of the key (with any atomic operations since
    ///   folded in, [`crate::atomic`]) gives it without a read. Otherwise the key is read at
    ///   the read version, **with a read conflict range**, and the transaction's atomic
    ///   operations on it, if any, are applied in order: a commit then fails with
    ///   `not_committed` if another transaction changed the key after the read version, so
    ///   the value is the key's value at the commit. The read counts as a read in progress:
    ///   a commit started meanwhile waits for it, and registers the watch.
    /// * **Later writes** of the transaction to the key that give it another value than the
    ///   one watched resolve the watch at once; an atomic operation on the key always does.
    ///
    /// This is the behaviour of C++'s read-your-writes layer, whose watch semantics the
    /// transaction has even though its reads do not see its writes yet (see the [module
    /// documentation](super)).
    ///
    /// # Errors
    ///
    /// `too_many_watches` (1032): the database has [`max_outstanding_watches`] outstanding
    /// watches. `key_outside_legal_range` (2004) for a key from `\xff` on (unless the
    /// `ReadSystemKeys` or `AccessSystemKeys` option is set) or from `\xff\xff` on,
    /// `key_too_large` (2102), `accessed_unreadable` (1036) for a key whose value a pending
    /// versionstamped operation of the transaction makes unknown (see [`crate::watch`]), the
    /// errors of [`get`](Self::get), and `used_during_commit` (2017) after the commit started
    /// ([maybe committed](Error::is_maybe_committed) once the commit may have been sent; the
    /// commit itself goes on).
    ///
    /// [`max_outstanding_watches`]: crate::WatchConfig::max_outstanding_watches
    pub async fn watch(&self, key: &[u8]) -> Result<Watch, Error> {
        let (read, guard) = {
            let mut state = self.state();
            self.check_handle(&state)?;
            if let Some(e) = state.deferred.clone() {
                return Err(e);
            }
            state.check_alive(self.rt().now())?;
            if let Some(e) = state.used_during_commit() {
                return Err(e);
            }
            if key >= SYSTEM_KEYS_END || key >= state.flags.max_read_key() {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            let limits = *state.buffer.limits();
            if key.len() > limits.max_write_key_size(key) {
                return Err(Error::new(KEY_TOO_LARGE));
            }
            let use_provisional = state.flags.use_provisional_proxies;
            let own = own_writes(
                state.buffer.mutations(),
                &state.stamped_keys,
                key,
                limits.value_size_limit,
            );
            let ops = match own {
                Own::Unreadable => return Err(Error::new(ACCESSED_UNREADABLE)),
                // The write map answers: nothing to read or to wait for.
                Own::Known(value) => {
                    let counter = &self.inner.db.inner.watches.counter;
                    return state
                        .watches
                        .add_known(key, value, use_provisional, counter);
                }
                Own::Unmodified => Vec::new(),
                Own::Dependent(ops) => ops,
            };
            let (id, fired) = state.watches.register(key, use_provisional);
            // `ryw->reading.add(done)`: held until the watch is registered (or failed).
            let guard = self.start_read(&mut state);
            let read = WatchRead {
                key: key.to_vec(),
                id,
                fired,
                ops,
                epoch: state.epoch,
                use_provisional,
                value_size_limit: limits.value_size_limit,
            };
            (read, guard)
        };
        let tx = self.clone();
        self.spawn_read(async move {
            let result = tx.watch_read(read, &guard).await;
            drop(guard);
            result
        })
        .await
    }

    /// `RYWImpl::watch` from the read on: the value read (unless a write fires the watch
    /// first), the read conflict range, then the registration.
    async fn watch_read(&self, read: WatchRead, guard: &ReadGuard<R>) -> Result<Watch, Error> {
        let WatchRead {
            key,
            id,
            fired,
            ops,
            epoch,
            use_provisional,
            value_size_limit,
        } = read;
        let mut value = pin!(async {
            self.guarded(epoch, async {
                let version = self.read_version(false).await?;
                self.inner
                    .db
                    .inner
                    .reader
                    .get_with(&key, version, use_provisional)
                    .await
            })
            .await?
        });
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
        // `updateConflictMap`: the key was unmodified or had dependent writes.
        let end = key_after(&key);
        if let Err(e) = state.buffer.add_read_conflict_range(&key, &end) {
            state.watches.remove(&id);
            return Err(e);
        }
        let value = fold(&ops, value, value_size_limit);
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
