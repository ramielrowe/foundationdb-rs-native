//! The writes of a [`Transaction`]: `ReadYourWritesTransaction::set`, `clear`, `atomicOp`,
//! `addReadConflictRange` and `addWriteConflictRange` (`ReadYourWrites.actor.cpp`
//! 7.4:1945-1987 and 2219-2500, `ReadYourWrites.cpp` 8.0:1867-1909 and 2135-2416; the same
//! logic).
//!
//! With read-your-writes (the default), a write goes to the transaction's write map only:
//! the reads see it there, and the commit derives the mutations and the write conflict
//! ranges from the map ([`crate::ryw::commit`]). With
//! [`ReadYourWritesDisable`](super::TransactionOption::ReadYourWritesDisable), C++ writes
//! to the native transaction at once; here the [`CommitBuffer`](crate::commit::CommitBuffer),
//! which the commit sends as it is. Either way a write's checks are made when it is called
//! and its error is deferred (see the [module documentation](super)).
//!
//! Each write also adds to the transaction's approximate size
//! ([`Transaction::approximate_size`]) what C++ adds to `approximateSize`, and, with
//! read-your-writes, triggers the transaction's watches of the keys it writes
//! (`triggerWatches`).

use std::borrow::Cow;

use fdb_native_schema::domain::{KeyRange, MutationType};
use fdb_native_transport::runtime::Runtime;

use super::mutation::MutationType as AtomicOp;
use super::{METADATA_VERSION_REQUIRED_VALUE, ReadVersion, State, Transaction};
use crate::atomic;
use crate::commit::{KEY_RANGE_REF_SIZE, MUTATION_REF_SIZE, check_write, normalize_range};
use crate::error::Error;
use crate::error::codes::*;
use crate::read::{METADATA_VERSION_KEY, key_after};
use crate::ryw;
use crate::watch::Written;

/// `sizeof(MutationRef)`, as `approximateSize` counts a mutation.
const MUTATION: u64 = MUTATION_REF_SIZE;
/// `sizeof(KeyRangeRef)`.
const RANGE: u64 = KEY_RANGE_REF_SIZE;

fn len(bytes: &[u8]) -> u64 {
    bytes.len() as u64
}

/// `KeyRangeRef(begin, end)`, which `fdb_transaction_add_conflict_range` builds before it
/// calls the transaction: `inverted_range` (2005) at once, not deferred.
fn check_not_inverted(begin: &[u8], end: &[u8]) -> Result<(), Error> {
    if begin > end {
        return Err(Error::with_message(
            INVERTED_RANGE,
            "begin is larger than end",
        ));
    }
    Ok(())
}

impl State {
    /// Adds `n` to the approximate size (`approximateSize += n`).
    fn count(&mut self, n: u64) {
        self.ryw.approximate_size = self.ryw.approximate_size.saturating_add(n);
    }

    /// Whether the writes go to the write map (read-your-writes is on).
    fn ryw_writes(&self) -> bool {
        !self.flags.read_your_writes_disabled
    }
}

impl<R: Runtime> Transaction<R> {
    /// `set(key, value)`: writes `value` to `key` when the transaction commits; the
    /// transaction's reads see it from now on. Errors are deferred: `key_too_large` (2102, a
    /// key over 10 000 bytes), `value_too_large` (2103, a value over 100 000 bytes),
    /// `key_outside_legal_range` (2004, a key from `\xff` on without the `AccessSystemKeys`
    /// option), `client_invalid_operation` (2000) for `\xff/metadataVersion` (see the
    /// [module documentation](super)).
    pub fn set(&self, key: &[u8], value: &[u8]) {
        let written = self.write_then(|state, add_conflict| {
            if key == METADATA_VERSION_KEY {
                return Err(Error::with_message(
                    CLIENT_INVALID_OPERATION,
                    "\\xff/metadataVersion can only be written with SetVersionstampedValue",
                ));
            }
            if key >= state.flags.max_write_key() {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            let conflict = if add_conflict {
                RANGE + 2 * len(key) + 1
            } else {
                0
            };
            state.count(len(key) + len(value) + MUTATION + conflict);
            if !state.ryw_writes() {
                state.buffer.set(key, value)?;
                return Ok(false);
            }
            check_write(MutationType::SET_VALUE, key, value, state.buffer.limits())?;
            state
                .ryw
                .writes
                .mutate(key, MutationType::SET_VALUE, value, add_conflict)?;
            Ok(true)
        });
        if let Some((mut state, true)) = written {
            state
                .watches
                .trigger(key, None, Written::Value(Some(value)));
        }
    }

    /// `clear(key)`: removes `key` when the transaction commits.
    pub fn clear(&self, key: &[u8]) {
        let written = self.write_then(|state, add_conflict| {
            if key >= state.flags.max_write_key() {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            // No key in the database is longer than this (`getMaxClearKeySize`): nothing
            // to clear.
            if key.len() > state.buffer.limits().max_range_key_size(key) {
                return Ok(false);
            }
            if !state.ryw_writes() {
                state.buffer.clear(key);
                return Ok(false);
            }
            // `r = singleKeyRange(key)`: `approximateSize += r.expectedSize() +
            // sizeof(KeyRangeRef) + (addWriteConflict ? sizeof(KeyRangeRef) + r.expectedSize()
            // : 0)`.
            let range = 2 * len(key) + 1;
            let conflict = if add_conflict { RANGE + range } else { 0 };
            state.count(range + RANGE + conflict);
            state.ryw.writes.clear(key, &key_after(key), add_conflict);
            Ok(true)
        });
        if let Some((mut state, true)) = written {
            state.watches.trigger(key, None, Written::Value(None));
        }
    }

    /// `clear(begin, end)`: removes every key in `[begin, end)` when the transaction
    /// commits. Deferred errors: `inverted_range` (2005) if `begin > end`,
    /// `key_outside_legal_range` if either is past `\xff` (without `AccessSystemKeys`).
    pub fn clear_range(&self, begin: &[u8], end: &[u8]) {
        let written = self.write_then(|state, add_conflict| {
            // `KeyRangeRef(begin, end)`, made by the caller in C++.
            if begin > end {
                return Err(Error::new(INVERTED_RANGE));
            }
            let max = state.flags.max_write_key();
            if begin > max || end > max {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            let size = len(begin) + len(end);
            let conflict = if add_conflict { RANGE + size } else { 0 };
            state.count(size + MUTATION + conflict);
            if !state.ryw_writes() {
                state.buffer.clear_range(begin, end)?;
                return Ok(None);
            }
            // Keys over the limit shortened (`getMaxClearKeySize`); nothing if it is empty
            // then.
            let range = KeyRange {
                begin: begin.to_vec(),
                end: end.to_vec(),
            };
            let Some(range) = normalize_range(&range, state.buffer.limits())? else {
                return Ok(None);
            };
            state
                .ryw
                .writes
                .clear(&range.begin, &range.end, add_conflict);
            Ok(Some(range))
        });
        if let Some((mut state, Some(range))) = written {
            state
                .watches
                .trigger(&range.begin, Some(&range.end), Written::Value(None));
        }
    }

    /// `atomicOp(key, operand, op)`: an atomic operation on `key` when the transaction
    /// commits ([`Add`](AtomicOp::Add), [`BitAnd`](AtomicOp::BitAnd), ...; see
    /// [`MutationType`](AtomicOp), and [`crate::atomic`] for what each does to the value),
    /// including the two versionstamp operations (see
    /// [`set_versionstamped_key`](Self::set_versionstamped_key)). Deferred errors:
    /// `key_too_large`, `value_too_large`, `key_outside_legal_range`, and for the
    /// versionstamp operations `client_invalid_operation` (2000) if the 4-byte offset is
    /// invalid.
    ///
    /// ```no_run
    /// # async fn example(db: fdb_native_client::Database) -> Result<(), fdb_native_client::Error> {
    /// use fdb_native_client::MutationType;
    ///
    /// // Blind increments do not conflict with each other: no read conflict range.
    /// db.run(|tx, _| async move {
    ///     tx.atomic_op(b"hits", &1i64.to_le_bytes(), MutationType::Add);
    ///     Ok(())
    /// })
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn atomic_op(&self, key: &[u8], operand: &[u8], op: AtomicOp) {
        self.atomic_op_wire(key, operand, op.into());
    }

    /// [`atomic_op`](Self::atomic_op) with the mutation type as sent.
    fn atomic_op_wire(&self, key: &[u8], operand: &[u8], op: MutationType) {
        let written = self.write_then(|state, add_conflict| {
            if key == METADATA_VERSION_KEY {
                // `ReadYourWritesTransaction::atomicOp` (7.4:2226-2230): only the versionstamp
                // of the metadata version, with or without the system key options.
                if op != MutationType::SET_VERSIONSTAMPED_VALUE
                    || operand != METADATA_VERSION_REQUIRED_VALUE
                {
                    return Err(Error::with_message(
                        CLIENT_INVALID_OPERATION,
                        "\\xff/metadataVersion can only be written with SetVersionstampedValue \
                         of the 14-byte required value",
                    ));
                }
            } else if key >= state.flags.max_write_key() {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            // `isAtomicOp` and `apiVersionAtLeast(510)` (`Min` and `And` are `MinV2` and
            // `AndV2`) hold by construction: the type comes from `AtomicOp` (or is one of
            // the versionstamp operations), which has no `SetValue` and converts to the V2
            // types. Then the sizes and the offsets.
            debug_assert!(
                ![
                    MutationType::SET_VALUE,
                    MutationType::CLEAR_RANGE,
                    MutationType::MIN,
                    MutationType::AND,
                ]
                .contains(&op),
                "{op:?} is not an atomic operation this client sends"
            );
            check_write(op, key, operand, state.buffer.limits())?;
            let kind = op;
            let mut add_conflict = add_conflict;
            let mut k = Cow::Borrowed(key);
            if kind == MutationType::SET_VERSIONSTAMPED_KEY {
                // `getVersionstampKeyRange` with the read version the transaction has now
                // (`getCachedReadVersion`); C++ throws `inverted_range` for a range past
                // the readable keys.
                let read_version = match state.read_version {
                    ReadVersion::Set(v) => Some(v),
                    _ => None,
                };
                let range =
                    atomic::versionstamp_key_range(key, read_version, state.flags.max_read_key())?;
                // No write conflict range: the key is not known before the commit.
                add_conflict = false;
                if state.ryw_writes() {
                    // The writes the stamped key may overwrite go to the commit first
                    // (`writeRangeToNativeTransaction`), then their keys become unmodified
                    // and unreadable until the commit.
                    ryw::commit::write_range_to_native(
                        &state.ryw.writes,
                        &range.begin,
                        &range.end,
                        &mut state.buffer,
                    )?;
                    state
                        .ryw
                        .writes
                        .add_unmodified_and_unreadable_range(&range.begin, &range.end);
                }
                // `transformVersionstampKey`: the key with the smallest stamp it may get
                // (the read version + 1, or 0), which is the write map's key for it.
                k = Cow::Owned(atomic::transform_versionstamp_key(
                    key,
                    read_version.map_or(0, |v| v + 1),
                    0,
                )?);
            }
            let conflict = if add_conflict {
                RANGE + 2 * len(key) + 1
            } else {
                0
            };
            state.count(len(&k) + len(operand) + MUTATION + conflict);
            if !state.ryw_writes() {
                state.buffer.atomic_op(&k, operand, kind)?;
                return Ok(None);
            }
            state.ryw.writes.mutate(&k, kind, operand, add_conflict)?;
            Ok(Some(k.into_owned()))
        });
        if let Some((mut state, Some(key))) = written {
            state.watches.trigger(&key, None, Written::Unknown);
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
    /// As C++ does (`transformVersionstampKey`), the key sent has the placeholder filled
    /// with the smallest stamp the commit may give it (the read version + 1 if the
    /// transaction has one, else zeros); the commit proxy overwrites the 10 bytes anyway.
    ///
    /// **No write conflict range is added** for the key: the key is not known before the
    /// commit (C++ adds none either: `Transaction::atomicOp` skips it for
    /// `SetVersionstampedKey`). A transaction that writes nothing else is still sent, with
    /// the self-conflicting key, as any blind write. The keys the stamped key may be (see
    /// [`atomic::versionstamp_key_range`]) are unreadable until the commit: a read or a
    /// [`watch`](Self::watch) of them fails with `accessed_unreadable` (1036) until a clear
    /// covers them, and a watch of the key with the placeholder stamped
    /// (`transformVersionstampKey`) is triggered, as C++'s read-your-writes layer does. The
    /// transaction's earlier writes to those keys are sent before the versionstamped key
    /// (`writeRangeToNativeTransaction`), so that it overwrites them if it lands on one.
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
        self.atomic_op_wire(key, value, MutationType::SET_VERSIONSTAMPED_KEY);
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
        self.atomic_op_wire(key, value, MutationType::SET_VERSIONSTAMPED_VALUE);
    }

    /// `addReadConflictRange(begin, end)`: the commit conflicts if another transaction
    /// wrote in `[begin, end)` after the read version, as if the range had been read.
    ///
    /// With read-your-writes (`ReadYourWritesTransaction::addReadConflictRange`), the parts
    /// of the range the transaction has set or cleared are left out, as a read of them would
    /// be (`updateConflictMap`), and the range joins the reads' ranges.
    ///
    /// # Errors
    ///
    /// As `fdb_transaction_add_conflict_range`: `inverted_range` (2005, `begin > end`) is
    /// returned at once (`fdb_c` builds the `KeyRangeRef` before anything else) and the
    /// transaction is unchanged. The other errors are deferred (see the [module
    /// documentation](super)): `key_outside_legal_range` (2004) for a key after `\xff`
    /// (`\xff\xff` with the system key options; the range of `\xff/metadataVersion` is
    /// always allowed), `used_during_commit`, ...
    pub fn add_read_conflict_range(&self, begin: &[u8], end: &[u8]) -> Result<(), Error> {
        check_not_inverted(begin, end)?;
        self.write(|state| {
            let max = state.flags.max_read_key();
            let metadata_version = begin == METADATA_VERSION_KEY && end == key_after(begin);
            if (begin > max || end > max) && !metadata_version {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            let range = KeyRange {
                begin: begin.to_vec(),
                end: end.to_vec(),
            };
            let Some(range) = normalize_range(&range, state.buffer.limits())? else {
                return Ok(());
            };
            if !state.ryw_writes() {
                state.count(len(&range.begin) + len(&range.end) + RANGE);
                return state
                    .buffer
                    .add_read_conflict_range(&range.begin, &range.end);
            }
            let writes = state.ryw.writes.iter();
            ryw::read::add_range_conflict(
                &mut state.ryw.read_conflicts,
                &range.begin,
                &range.end,
                &writes,
            );
            Ok(())
        });
        Ok(())
    }

    /// `addReadConflictRange(key, keyAfter(key))`: [`add_read_conflict_range`] of the
    /// single key `key` (`[key, key\x00)`), as the bindings' `add_read_conflict_key`.
    ///
    /// [`add_read_conflict_range`]: Self::add_read_conflict_range
    pub fn add_read_conflict_key(&self, key: &[u8]) -> Result<(), Error> {
        self.add_read_conflict_range(key, &key_after(key))
    }

    /// `addWriteConflictRange(begin, end)`: other transactions that read in
    /// `[begin, end)` conflict with this one, as if it had written there.
    ///
    /// # Errors
    ///
    /// As [`add_read_conflict_range`](Self::add_read_conflict_range): `inverted_range` (2005)
    /// at once, the rest deferred (`key_outside_legal_range` (2004) for a key after `\xff`,
    /// or `\xff\xff` with the `AccessSystemKeys` option).
    pub fn add_write_conflict_range(&self, begin: &[u8], end: &[u8]) -> Result<(), Error> {
        check_not_inverted(begin, end)?;
        self.write(|state| {
            let max = state.flags.max_write_key();
            if begin > max || end > max {
                return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
            }
            let range = KeyRange {
                begin: begin.to_vec(),
                end: end.to_vec(),
            };
            let Some(range) = normalize_range(&range, state.buffer.limits())? else {
                return Ok(());
            };
            state.count(len(&range.begin) + len(&range.end) + RANGE);
            if !state.ryw_writes() {
                return state
                    .buffer
                    .add_write_conflict_range(&range.begin, &range.end);
            }
            state
                .ryw
                .writes
                .add_conflict_range(&range.begin, &range.end);
            Ok(())
        });
        Ok(())
    }

    /// `addWriteConflictRange(key, keyAfter(key))`: [`add_write_conflict_range`] of the
    /// single key `key` (`[key, key\x00)`), as the bindings' `add_write_conflict_key`.
    ///
    /// [`add_write_conflict_range`]: Self::add_write_conflict_range
    pub fn add_write_conflict_key(&self, key: &[u8]) -> Result<(), Error> {
        self.add_write_conflict_range(key, &key_after(key))
    }

    /// `getApproximateSize()`: an estimate of the transaction's size so far, in bytes, as
    /// C++'s read-your-writes layer counts it (`approximateSize`): each write's key and value
    /// and the size of a mutation (44 bytes), each write conflict range a write adds (24
    /// bytes and its keys), and the read conflict ranges of the serializable reads and of
    /// [`add_read_conflict_range`](Self::add_read_conflict_range). It grows with every call,
    /// also for writes that coalesce in the write map, so it may be more than the commit
    /// will send (which is what the `SizeLimit` option and `transaction_too_large` (2101)
    /// apply to); a [`reset`](Self::reset) or [`on_error`](Self::on_error) brings it back to
    /// 0. Fails with the deferred error of a write, if any (as `fdb_c` does).
    ///
    /// Synchronous: C++'s `getApproximateSize()` is a counter of the read-your-writes layer,
    /// which `fdb_c` (and the bindings over it) returns as a future only because every call
    /// goes through its network thread. The value is the same: the writes made before the
    /// call, and the read conflict ranges of the reads that have ended.
    pub fn approximate_size(&self) -> Result<u64, Error> {
        let state = self.state();
        self.check_handle(&state)?;
        if let Some(e) = state.deferred.clone() {
            return Err(e);
        }
        Ok(state
            .ryw
            .approximate_size
            .saturating_add(state.ryw.read_conflicts.approximate_size()))
    }
}
