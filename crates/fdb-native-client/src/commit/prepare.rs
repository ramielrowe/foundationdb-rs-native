//! `Transaction::commitMutations`: from a transaction's buffers to the request a commit proxy
//! gets.

use fdb_native_schema::domain::{Commit, KeyRange, Mutation, MutationType};
use fdb_native_schema::self_conflict_key;
use fdb_native_wire::Uid;

use super::error::CommitError;
use super::limits::{CommitLimits, expected_size, normalize_range};
use super::options::CommitOptions;

/// `SIZE_LIMIT`'s lower bound (`extractIntOption(value, 32, ...)`).
const MIN_SIZE_LIMIT: u64 = 32;

/// The mutation types a commit may carry: `SetValue`, `ClearRange` and the atomic operations
/// `NativeAPI`'s `atomicOp` takes, except the versionstamp operations (not supported yet:
/// they need the offset checks `ReadYourWritesTransaction::atomicOp` does).
fn supported(kind: MutationType) -> bool {
    matches!(
        kind,
        MutationType::SET_VALUE
            | MutationType::CLEAR_RANGE
            | MutationType::ADD_VALUE
            | MutationType::AND
            | MutationType::OR
            | MutationType::XOR
            | MutationType::APPEND_IF_FITS
            | MutationType::MAX
            | MutationType::MIN
            | MutationType::BYTE_MIN
            | MutationType::BYTE_MAX
            | MutationType::MIN_V2
            | MutationType::AND_V2
            | MutationType::COMPARE_AND_CLEAR
    )
}

/// The checks `Transaction::set`, `atomicOp` and `clear(range)` do, on one mutation: `None`
/// if it is a clear that is empty once its keys are truncated.
pub(crate) fn check_mutation(
    m: Mutation,
    limits: &CommitLimits,
) -> Result<Option<Mutation>, CommitError> {
    if !supported(m.kind) {
        return Err(CommitError::Unsupported(m.kind.0));
    }
    if m.kind == MutationType::CLEAR_RANGE {
        let range = KeyRange {
            begin: m.param1,
            end: m.param2,
        };
        return Ok(normalize_range(&range, limits)?.map(|r| Mutation {
            kind: MutationType::CLEAR_RANGE,
            param1: r.begin,
            param2: r.end,
        }));
    }
    let max = limits.max_write_key_size(&m.param1);
    if m.param1.len() > max {
        return Err(CommitError::KeyTooLarge {
            size: m.param1.len(),
            limit: max,
        });
    }
    if m.param2.len() > limits.value_size_limit {
        return Err(CommitError::ValueTooLarge {
            size: m.param2.len(),
            limit: limits.value_size_limit,
        });
    }
    Ok(Some(m))
}

fn normalize_all(
    ranges: Vec<KeyRange>,
    limits: &CommitLimits,
) -> Result<Vec<KeyRange>, CommitError> {
    let mut out = Vec::with_capacity(ranges.len());
    for range in &ranges {
        out.extend(normalize_range(range, limits)?);
    }
    Ok(out)
}

/// `intersects(lhs, rhs)` (`NativeAPI.actor.cpp` 7.4:6536-6553): sorts both lists **in
/// place** by `begin` (as C++ does, through the shared arena: the request's ranges end up
/// sorted), then sweeps for the first overlapping pair and returns their intersection.
pub(crate) fn intersects(lhs: &mut [KeyRange], rhs: &mut [KeyRange]) -> Option<KeyRange> {
    if lhs.is_empty() || rhs.is_empty() {
        return None;
    }
    lhs.sort_by(|a, b| a.begin.cmp(&b.begin));
    rhs.sort_by(|a, b| a.begin.cmp(&b.begin));
    let (mut l, mut r) = (0, 0);
    while l < lhs.len() && r < rhs.len() {
        if lhs[l].end <= rhs[r].begin {
            l += 1;
        } else if rhs[r].end <= lhs[l].begin {
            r += 1;
        } else {
            let begin = lhs[l].begin.clone().max(rhs[r].begin.clone());
            let end = lhs[l].end.clone().min(rhs[r].end.clone());
            return Some(KeyRange { begin, end });
        }
    }
    None
}

/// The checks and additions of `Transaction::commitMutations` (`NativeAPI.actor.cpp`
/// 7.4:7129-7220, 8.0:4820-4915), in its order; the result is what `tryCommit` sends once it
/// has the read version.
///
/// 1. The buffers are normalized as the `Transaction` methods do: oversized keys of clears
///    and conflict ranges truncated, empty ranges dropped, set and atomic operations checked
///    against the key and value limits.
/// 2. No write conflict range and no mutation: a read-only transaction, `Ok(None)`: nothing
///    is sent (C++ returns at once, `committedVersion = invalidVersion`).
/// 3. `readOnly` with writes: `transaction_read_only`.
/// 4. The size (`getSize()`, before anything is added) over the limit:
///    `transaction_too_large`.
/// 5. Unless `causal_write_risky`, if no write conflict range intersects a read conflict
///    range, `makeSelfConflicting`: a random `\xff/SC/<uid>` key as a read and a write
///    conflict range.
/// 6. The flags and `report_conflicting_keys`.
///
/// Not ported: `extraConflictRanges` (the read-your-writes layer's), idempotency ids (they
/// need the idempotency-record read path), `checkWrites`, transaction tags and cost
/// estimation, tenants.
pub(crate) fn prepare(
    read_version: i64,
    mutations: Vec<Mutation>,
    read_conflicts: Vec<KeyRange>,
    write_conflicts: Vec<KeyRange>,
    options: &CommitOptions,
    limits: &CommitLimits,
    self_conflict_id: impl FnOnce() -> Uid,
) -> Result<Option<Commit>, CommitError> {
    if options.size_limit < MIN_SIZE_LIMIT || options.size_limit > limits.transaction_size_limit {
        return Err(CommitError::InvalidOptionValue(
            "the size limit must be from 32 to TRANSACTION_SIZE_LIMIT",
        ));
    }
    let mut kept = Vec::with_capacity(mutations.len());
    for m in mutations {
        kept.extend(check_mutation(m, limits)?);
    }
    let mut read_conflicts = normalize_all(read_conflicts, limits)?;
    let mut write_conflicts = normalize_all(write_conflicts, limits)?;

    if write_conflicts.is_empty() && kept.is_empty() {
        return Ok(None);
    }
    if options.read_only {
        return Err(CommitError::TransactionReadOnly);
    }
    let size = expected_size(&kept, &read_conflicts, &write_conflicts);
    if size > options.size_limit {
        return Err(CommitError::TransactionTooLarge {
            size,
            limit: options.size_limit,
        });
    }

    if !options.causal_write_risky
        && intersects(&mut write_conflicts, &mut read_conflicts).is_none()
    {
        // makeSelfConflicting
        let key = self_conflict_key(self_conflict_id())?;
        let range = KeyRange::single_key(&key);
        read_conflicts.push(range.clone());
        write_conflicts.push(range);
    }

    let mut flags = 0;
    if options.lock_aware {
        flags |= Commit::FLAG_IS_LOCK_AWARE;
    }
    if options.first_in_batch {
        flags |= Commit::FLAG_FIRST_IN_BATCH;
    }
    if options.bypass_storage_quota {
        flags |= Commit::FLAG_BYPASS_STORAGE_QUOTA;
    }
    Ok(Some(Commit {
        read_conflict_ranges: read_conflicts,
        write_conflict_ranges: write_conflicts,
        mutations: kept,
        read_snapshot: read_version,
        report_conflicting_keys: options.report_conflicting_keys,
        flags,
        ..Commit::default()
    }))
}
