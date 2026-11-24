//! What a read-your-writes transaction sends: its writes, from the write map, to the native
//! transaction (`ReadYourWritesTransaction::writeRangeToNativeTransaction`,
//! `ReadYourWrites.actor.cpp` 7.4:1997-2072, `ReadYourWrites.cpp` 8.0:1911-1986; the same
//! logic).
//!
//! The read-your-writes layer keeps a transaction's writes in its [`WriteMap`] and gives
//! them to the native transaction (here the [`CommitBuffer`]) only:
//!
//! * at the commit, all of them (`RYWImpl::commit`, 7.4:1392, 8.0:1386:
//!   `writeRangeToNativeTransaction(KeyRangeRef(StringRef(), allKeys.end))`), before the read
//!   conflict ranges (7.4:1394-1399, 8.0:1388-1393);
//! * for a `SetVersionstampedKey`, those in the range of keys the stamped key may be, before
//!   the range becomes unmodified and unreadable
//!   (`ReadYourWritesTransaction::atomicOp`, 7.4:2270-2271, 8.0:2186-2187): the writes there
//!   reach the native transaction before the versionstamped key, which may overwrite them.
//!
//! [`write_range_to_native`] does what C++ does, in its order: first every **cleared range**
//! (maximal runs of cleared segments, cut to the range written) as a `ClearRange`, then, in a
//! second walk, each maximal run of segments in the **write conflict ranges** as a write
//! conflict range, and each **operation stack** bottom to top: a `SetValue` as a set (a
//! `SetValue` without a value, what a `CompareAndClear` folded into a set leaves, as a clear of
//! the key), an atomic operation (versionstamped sets included) as itself. The mutations
//! add no write conflict range of their own (`AddConflictRange::False`): the write map's
//! conflict ranges are the transaction's. The clears come first "because of keys that are
//! both cleared and set to a new value": a key set after a clear of its range is an
//! operation inside the cleared run, and the commit proxy applies mutations in order.

use crate::commit::CommitBuffer;
use crate::error::Error;
use crate::ryw::{ExtKey, WriteMap};
use fdb_native_schema::domain::MutationType;

/// `writeRangeToNativeTransaction(KeyRangeRef(begin, end))`: the writes of `writes` in
/// `[begin, end)` to `native` (see the module documentation). `end` is at most `\xff\xff`
/// (the write map's key space).
///
/// Fails only if `native` refuses a write, which the checks the transaction made before
/// writing to the map rule out.
pub(crate) fn write_range_to_native(
    writes: &WriteMap,
    begin: &[u8],
    end: &[u8],
    native: &mut CommitBuffer,
) -> Result<(), Error> {
    // The mutations go without their write conflict ranges (`AddConflictRange::False`).
    let skip = native.skip_write_conflict();
    native.set_skip_write_conflict(true);
    let result = write_range(writes, begin, end, native);
    native.set_skip_write_conflict(skip);
    result
}

fn write_range(
    writes: &WriteMap,
    begin: &[u8],
    end: &[u8],
    native: &mut CommitBuffer,
) -> Result<(), Error> {
    let begin_key = ExtKey::from(begin);
    let end_key = ExtKey::from(end);

    // The cleared ranges first.
    let mut it = writes.iter_at(begin);
    let mut clear_begin: Option<Vec<u8>> = None;
    while it.begin_key() < end_key {
        if it.is_cleared_range() {
            if clear_begin.is_none() {
                clear_begin = Some(begin_key.max(it.begin_key()).to_vec());
            }
        } else if let Some(b) = clear_begin.take() {
            native.clear_range(&b, &it.begin_key().to_vec())?;
        }
        // From the last segment `advance` reaches the sentinel, past `end`.
        let _ = it.advance();
    }
    if let Some(b) = clear_begin {
        native.clear_range(&b, end)?;
    }

    // Then the conflict ranges and the operations.
    let mut it = writes.iter_at(begin);
    let mut conflict_begin: Option<Vec<u8>> = None;
    while it.begin_key() < end_key {
        if it.is_conflict_range() {
            if conflict_begin.is_none() {
                conflict_begin = Some(begin_key.max(it.begin_key()).to_vec());
            }
        } else if let Some(b) = conflict_begin.take() {
            native.add_write_conflict_range(&b, &it.begin_key().to_vec())?;
        }
        if let Some(stack) = it.op() {
            // An operation's segment starts at its key (`assertRef`).
            let key = it.begin_key().assert_ref();
            for m in stack {
                match m.kind {
                    MutationType::SET_VALUE => match &m.value {
                        Some(value) => native.set(key, value)?,
                        None => native.clear(key),
                    },
                    MutationType::ADD_VALUE
                    | MutationType::APPEND_IF_FITS
                    | MutationType::AND
                    | MutationType::OR
                    | MutationType::XOR
                    | MutationType::MAX
                    | MutationType::MIN
                    | MutationType::SET_VERSIONSTAMPED_KEY
                    | MutationType::SET_VERSIONSTAMPED_VALUE
                    | MutationType::BYTE_MIN
                    | MutationType::BYTE_MAX
                    | MutationType::MIN_V2
                    | MutationType::AND_V2
                    | MutationType::COMPARE_AND_CLEAR => {
                        native.atomic_op(key, m.value.as_deref().unwrap_or_default(), m.kind)?;
                    }
                    // C++'s `default: break` (the write map holds no other operation).
                    _ => {}
                }
            }
        }
        let _ = it.advance();
    }
    if let Some(b) = conflict_begin {
        native.add_write_conflict_range(&b, end)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
