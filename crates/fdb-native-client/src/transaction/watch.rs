//! [`Transaction::watch`] and the hooks that keep a transaction's watches right: writes that
//! trigger them, the commit that starts them, and the failures that cancel them. The watch
//! machinery is [`crate::watch`]; see its documentation for what C++ does.

use fdb_native_schema::domain::MutationType;
use fdb_native_transport::runtime::Runtime;

use super::{CommitStage, SYSTEM_KEYS_END, Transaction};
use crate::error::Error;
use crate::error::codes::*;
use crate::watch::{Baseline, Own, Watch, Written, own_value};

impl<R: Runtime> Transaction<R> {
    /// `watch(key)`: a [`Watch`] that resolves when the value of `key` changes, once this
    /// transaction has committed.
    ///
    /// Like `ReadYourWritesTransaction::watch`, this **reads the key** (adding it as a read
    /// conflict range) unless the transaction has written it, and counts the watch against the
    /// database's cap. The watch is **registered when the transaction commits** (also a
    /// read-only one, which commits without contacting the cluster), watching the value the
    /// key has as of the commit: the transaction's own write to it if it made one, else the
    /// value it read. If the transaction fails to commit, or is reset or dropped first, the
    /// watch fails (see [`Watch`]). Writes to the key made after this call that give it
    /// another value than the one watched resolve the watch at once; an atomic operation on
    /// the key always does.
    ///
    /// This is the behaviour of C++'s read-your-writes layer, whose watch semantics the
    /// transaction has even though its reads do not see its writes yet (see the [module
    /// documentation](super)): the watched value is the transaction's write to the key if there
    /// is one, whatever `get` returns.
    ///
    /// # Errors
    ///
    /// `too_many_watches` (1032): the database has [`max_outstanding_watches`] outstanding
    /// watches. `key_outside_legal_range` (2004) for a key from `\xff` on (unless the
    /// `ReadSystemKeys` or `AccessSystemKeys` option is set) or from `\xff\xff` on,
    /// `key_too_large` (2102), the errors of [`get`](Self::get), and `used_during_commit`
    /// (2017) after the commit started.
    ///
    /// [`max_outstanding_watches`]: crate::WatchConfig::max_outstanding_watches
    pub async fn watch(&self, key: &[u8]) -> Result<Watch, Error> {
        let ctx = &self.inner.db.inner.watches;
        let (id, own) = {
            let mut state = self.state();
            self.check_handle(&state)?;
            if let Some(e) = state.deferred.clone() {
                return Err(e);
            }
            state.check_alive(self.rt().now())?;
            state.check_not_committing()?;
            if key >= SYSTEM_KEYS_END || key >= state.flags.max_read_key() {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            if key.len() > state.buffer.limits().max_write_key_size(key) {
                return Err(Error::new(KEY_TOO_LARGE));
            }
            let own = own_value(state.buffer.mutations(), key);
            let use_provisional = state.flags.use_provisional_proxies;
            (state.watches.register(key, use_provisional), own)
        };
        let baseline = match own {
            Own::Known(value) => Baseline::Value(value),
            Own::Unknown => Baseline::AfterCommit,
            Own::None => match self.get_with(key, false).await {
                Ok(value) => Baseline::Value(value),
                Err(e) => {
                    self.state().watches.remove(id);
                    return Err(e);
                }
            },
        };
        let mut state = self.state();
        // The commit may have started while the key was read (a commit does not wait for
        // the reads in progress yet).
        if let Err(e) = state.check_not_committing() {
            state.watches.remove(id);
            return Err(e);
        }
        state.watches.finish_read(id, baseline, &ctx.counter)
    }

    /// `triggerWatches` for a write, unless this handle's writes are ignored.
    fn trigger_watches(
        &self,
        write: impl FnOnce(&super::Flags, &mut crate::watch::PendingWatches),
    ) {
        let mut state = self.state();
        if state.watches.is_empty()
            || self.is_stale(&state)
            || state.dead.is_some()
            || !matches!(state.commit, CommitStage::Idle)
        {
            return;
        }
        let flags = state.flags;
        write(&flags, &mut state.watches);
    }

    /// `set(key, value)` was called.
    pub(super) fn watches_on_set(&self, key: &[u8], value: &[u8]) {
        self.trigger_watches(|flags, watches| {
            if key < flags.max_write_key() {
                watches.trigger(key, None, Written::Value(Some(value)));
            }
        });
    }

    /// `clear(key)` was called.
    pub(super) fn watches_on_clear(&self, key: &[u8]) {
        self.trigger_watches(|flags, watches| {
            if key < flags.max_write_key() {
                watches.trigger(key, None, Written::Value(None));
            }
        });
    }

    /// `clear_range(begin, end)` was called.
    pub(super) fn watches_on_clear_range(&self, begin: &[u8], end: &[u8]) {
        self.trigger_watches(|flags, watches| {
            let max = flags.max_write_key();
            if begin <= end && begin <= max && end <= max {
                watches.trigger(begin, Some(end), Written::Value(None));
            }
        });
    }

    /// `atomic_op(key, .., op)` was called.
    pub(super) fn watches_on_atomic_op(&self, key: &[u8], op: MutationType) {
        // A versionstamped key writes elsewhere; the other operations (and a refused
        // `SET_VALUE`) leave `key`'s value unknown (`RYWImpl::triggerWatches(.., false)`).
        if op == MutationType::SET_VERSIONSTAMPED_KEY {
            return;
        }
        self.trigger_watches(|flags, watches| {
            if key < flags.max_write_key() {
                watches.trigger(key, None, Written::Unknown);
            }
        });
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
    pub(super) fn fail_watches(
        &self,
        sent: &std::sync::Arc<std::sync::atomic::AtomicBool>,
        error: &Error,
    ) {
        let mut state = self.state();
        let ours =
            matches!(&state.commit, CommitStage::Started(s) if std::sync::Arc::ptr_eq(s, sent));
        if ours {
            state.watches.cancel_all(error);
        }
    }
}
