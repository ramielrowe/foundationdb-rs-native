//! The mutation and conflict range buffers of a transaction: `Transaction::set`, `clear`,
//! `atomicOp`, `addReadConflictRange` and `addWriteConflictRange`.

use fdb_native_schema::domain::{KeyRange, Mutation, MutationType};

use super::error::CommitError;
use super::limits::CommitLimits;
use super::prepare::check_mutation;

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
///   `apiVersionAtLeast(510)` does). The versionstamp operations are not supported yet.
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
}

impl CommitBuffer {
    /// An empty buffer that enforces `limits` ([`CommitLimits::for_schema`]).
    pub fn new(limits: CommitLimits) -> CommitBuffer {
        CommitBuffer {
            limits,
            mutations: Vec::new(),
            read_conflicts: Vec::new(),
            write_conflicts: Vec::new(),
        }
    }

    /// `Transaction::set`.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), CommitError> {
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
    pub fn clear_range(&mut self, begin: &[u8], end: &[u8]) -> Result<(), CommitError> {
        let range = KeyRange::new(begin, end).map_err(|_| CommitError::InvertedRange)?;
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

    /// `Transaction::atomicOp`.
    pub fn atomic_op(
        &mut self,
        key: &[u8],
        operand: &[u8],
        op: MutationType,
    ) -> Result<(), CommitError> {
        if op == MutationType::SET_VALUE || op == MutationType::CLEAR_RANGE {
            return Err(CommitError::Unsupported(op.0));
        }
        let m = Mutation {
            kind: op,
            param1: key.to_vec(),
            param2: operand.to_vec(),
        };
        self.push_mutation(m, Some(KeyRange::single_key(key)))
    }

    fn push_mutation(&mut self, m: Mutation, write: Option<KeyRange>) -> Result<(), CommitError> {
        if let Some(m) = check_mutation(m, &self.limits)? {
            self.mutations.push(m);
            self.write_conflicts.extend(write);
        }
        Ok(())
    }

    /// `Transaction::addReadConflictRange`.
    pub fn add_read_conflict_range(&mut self, begin: &[u8], end: &[u8]) -> Result<(), CommitError> {
        self.read_conflicts
            .push(KeyRange::new(begin, end).map_err(|_| CommitError::InvertedRange)?);
        Ok(())
    }

    /// `Transaction::addWriteConflictRange`.
    pub fn add_write_conflict_range(
        &mut self,
        begin: &[u8],
        end: &[u8],
    ) -> Result<(), CommitError> {
        self.write_conflicts
            .push(KeyRange::new(begin, end).map_err(|_| CommitError::InvertedRange)?);
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

    /// The mutations, the read conflict ranges and the write conflict ranges, for
    /// [`Committer::commit`](super::Committer::commit).
    pub fn into_parts(self) -> (Vec<Mutation>, Vec<KeyRange>, Vec<KeyRange>) {
        (self.mutations, self.read_conflicts, self.write_conflicts)
    }
}
