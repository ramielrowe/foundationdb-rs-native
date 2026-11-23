//! The mutation and conflict range buffers of a transaction: `Transaction::set`, `clear`,
//! `atomicOp`, `addReadConflictRange` and `addWriteConflictRange`.

use fdb_native_schema::domain::{KeyRange, Mutation, MutationType};

use super::error::{invalid_mutation_type, inverted_range};
use super::limits::CommitLimits;
use super::prepare::check_mutation;
use crate::error::Error;

/// What a transaction has buffered to commit, built the way the C++ `Transaction` builds it
/// (`NativeAPI.actor.cpp` 7.4:6242-6420, 8.0:3985-4160):
///
/// * [`set`](Self::set): a `SetValue` mutation on the key and a write conflict range
///   `[key, key\x00)`; `key_too_large` / `value_too_large` at once.
/// * [`clear`](Self::clear): one key, a `ClearRange` `[key, key\x00)` and the same write
///   conflict range; a key longer than the limit is skipped (nothing can be stored there).
/// * [`clear_range`](Self::clear_range): a `ClearRange` and a write conflict range on it,
///   with keys over the limit truncated and an empty result dropped.
/// * [`atomic_op`](Self::atomic_op): the mutation and, except for `SetVersionstampedKey`,
///   a write conflict range on the key. `Min` and `And` become `MinV2` and `AndV2` (what
///   `apiVersionAtLeast(510)` does). The versionstamp operations
///   ([`set_versionstamped_key`](Self::set_versionstamped_key),
///   [`set_versionstamped_value`](Self::set_versionstamped_value)) have their 4-byte offset
///   checked (`client_invalid_operation`); a versionstamped key adds **no** write conflict
///   range (the key it writes is not known), as in C++.
/// * [`add_read_conflict_range`](Self::add_read_conflict_range),
///   [`add_write_conflict_range`](Self::add_write_conflict_range): as given.
///
/// The reads a transaction does add their read conflict ranges with
/// [`add_read_conflict_range`](Self::add_read_conflict_range) (the read path's business).
/// [`Committer::commit_buffer`](super::Committer::commit_buffer) sends it.
#[derive(Clone, Debug)]
pub struct CommitBuffer {
    limits: CommitLimits,
    mutations: Vec<Mutation>,
    read_conflicts: Vec<KeyRange>,
    write_conflicts: Vec<KeyRange>,
    /// C++'s `extraConflictRanges`: the read conflict ranges of `getKey` and `getRange`.
    extra_read_conflicts: Vec<KeyRange>,
}

impl CommitBuffer {
    /// An empty buffer that enforces `limits` ([`CommitLimits::for_schema`]).
    pub fn new(limits: CommitLimits) -> CommitBuffer {
        CommitBuffer {
            limits,
            mutations: Vec::new(),
            read_conflicts: Vec::new(),
            write_conflicts: Vec::new(),
            extra_read_conflicts: Vec::new(),
        }
    }

    /// `Transaction::set`.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let m = Mutation {
            kind: MutationType::SET_VALUE,
            param1: key.to_vec(),
            param2: value.to_vec(),
        };
        self.push_mutation(m, Some(KeyRange::single_key(key)))
    }

    /// `Transaction::clear(key)`.
    pub fn clear(&mut self, key: &[u8]) {
        if key.len() > self.limits.max_range_key_size(key) {
            return;
        }
        let range = KeyRange::single_key(key);
        self.mutations.push(Mutation {
            kind: MutationType::CLEAR_RANGE,
            param1: range.begin.clone(),
            param2: range.end.clone(),
        });
        self.write_conflicts.push(range);
    }

    /// `Transaction::clear(range)`. `inverted_range` if `begin > end`.
    pub fn clear_range(&mut self, begin: &[u8], end: &[u8]) -> Result<(), Error> {
        let range = KeyRange::new(begin, end).map_err(|_| inverted_range())?;
        let m = Mutation {
            kind: MutationType::CLEAR_RANGE,
            param1: range.begin,
            param2: range.end,
        };
        // The same truncation `commit` applies; the mutation's range is the conflict range.
        if let Some(m) = check_mutation(m, &self.limits)? {
            self.write_conflicts.push(KeyRange {
                begin: m.param1.clone(),
                end: m.param2.clone(),
            });
            self.mutations.push(m);
        }
        Ok(())
    }

    /// `ReadYourWritesTransaction::atomicOp` (on `Transaction::atomicOp`): `invalid_mutation_type`
    /// (2018) unless `op` is an atomic operation (`SetValue` and `ClearRange` are not),
    /// `key_too_large`, `value_too_large`, and for the versionstamp operations
    /// `client_invalid_operation` if the offset is not valid (see
    /// [`with_versionstamp_offset`](super::with_versionstamp_offset)).
    ///
    /// The write conflict range is the key's, except for `SetVersionstampedKey`:
    /// `Transaction::atomicOp` adds none for it (`NativeAPI.actor.cpp` 7.4:6321, 8.0:4064) and
    /// the read-your-writes layer passes `AddConflictRange::False` for it (7.4:2268), so
    /// that the commit has no write conflict range for the key it writes.
    pub fn atomic_op(&mut self, key: &[u8], operand: &[u8], op: MutationType) -> Result<(), Error> {
        if op == MutationType::SET_VALUE || op == MutationType::CLEAR_RANGE {
            return Err(invalid_mutation_type(op.0));
        }
        let m = Mutation {
            kind: op,
            param1: key.to_vec(),
            param2: operand.to_vec(),
        };
        let write = (op != MutationType::SET_VERSIONSTAMPED_KEY).then(|| KeyRange::single_key(key));
        self.push_mutation(m, write)
    }

    /// `atomic_op(key, value, SetVersionstampedKey)`: sets `value` at `key` with the
    /// 10-byte placeholder at the offset in the last 4 bytes of `key` replaced by the
    /// transaction's versionstamp. No write conflict range is added.
    pub fn set_versionstamped_key(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.atomic_op(key, value, MutationType::SET_VERSIONSTAMPED_KEY)
    }

    /// `atomic_op(key, value, SetVersionstampedValue)`: sets `key` to `value` with the
    /// placeholder at the offset in the last 4 bytes of `value` replaced by the
    /// transaction's versionstamp.
    pub fn set_versionstamped_value(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.atomic_op(key, value, MutationType::SET_VERSIONSTAMPED_VALUE)
    }

    fn push_mutation(&mut self, m: Mutation, write: Option<KeyRange>) -> Result<(), Error> {
        if let Some(m) = check_mutation(m, &self.limits)? {
            self.mutations.push(m);
            self.write_conflicts.extend(write);
        }
        Ok(())
    }

    /// `Transaction::addReadConflictRange`.
    pub fn add_read_conflict_range(&mut self, begin: &[u8], end: &[u8]) -> Result<(), Error> {
        self.read_conflicts
            .push(KeyRange::new(begin, end).map_err(|_| inverted_range())?);
        Ok(())
    }

    /// A read conflict range of a `getKey` or `getRange` (C++'s `extraConflictRanges`,
    /// NativeAPI 7.4:7166-7172, 8.0:4863-4867): sent as is (keys not shortened), and not
    /// counted in the transaction's size (C++ adds these after its size check). An empty
    /// range is left out.
    pub fn add_extra_read_conflict_range(&mut self, range: KeyRange) {
        if range.begin < range.end {
            self.extra_read_conflicts.push(range);
        }
    }

    /// `Transaction::addWriteConflictRange`.
    pub fn add_write_conflict_range(&mut self, begin: &[u8], end: &[u8]) -> Result<(), Error> {
        self.write_conflicts
            .push(KeyRange::new(begin, end).map_err(|_| inverted_range())?);
        Ok(())
    }

    /// The limits this buffer enforces.
    pub fn limits(&self) -> &CommitLimits {
        &self.limits
    }

    /// Whether nothing was buffered (no mutation and no write conflict range: a commit of it
    /// sends nothing).
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty() && self.write_conflicts.is_empty()
    }

    /// The mutations buffered so far, in order (what a watch's baseline value is worked out
    /// from).
    pub fn mutations(&self) -> &[Mutation] {
        &self.mutations
    }

    /// The mutations, the read conflict ranges (the
    /// [extra ones](Self::add_extra_read_conflict_range) last) and the write conflict
    /// ranges, for [`Committer::commit`](super::Committer::commit) (which counts all of them
    /// in the size; [`Committer::commit_buffer`](super::Committer::commit_buffer) does not
    /// count the extra ones).
    pub fn into_parts(self) -> (Vec<Mutation>, Vec<KeyRange>, Vec<KeyRange>) {
        let (mutations, mut reads, writes, extra) = self.into_parts_with_extra();
        reads.extend(extra);
        (mutations, reads, writes)
    }

    /// The mutations, the read conflict ranges, the write conflict ranges and the extra read
    /// conflict ranges.
    pub(crate) fn into_parts_with_extra(
        self,
    ) -> (Vec<Mutation>, Vec<KeyRange>, Vec<KeyRange>, Vec<KeyRange>) {
        (
            self.mutations,
            self.read_conflicts,
            self.write_conflicts,
            self.extra_read_conflicts,
        )
    }
}
