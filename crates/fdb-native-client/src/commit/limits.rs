//! Size limits and sizes, as the C++ client computes them.

use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::{KeyRange, Mutation};

/// `sizeof(MutationRef)` as `VectorRef<MutationRef>::expectedSize()` counts it: `type` (1),
/// two 12-byte packed `StringRef`s, `Optional<uint32_t> checksum`,
/// `Optional<uint16_t> accumulativeChecksumIndex` and `bool corrupted`, 4-aligned. The same
/// layout on 7.4 and 8.0 (`CommitTransaction.h`, 7.4:103-107, 8.0:101-105). Measured against
/// `libfdb_c` 7.4.10: a transaction of 123456 one-key sets with 4-byte keys and empty values
/// commits to the size check, 123457 is `transaction_too_large`.
pub const MUTATION_REF_SIZE: u64 = 44;

/// `sizeof(KeyRangeRef)`: two 12-byte packed `StringRef`s.
pub const KEY_RANGE_REF_SIZE: u64 = 24;

/// The client knobs and rules that bound what a transaction may contain.
///
/// The defaults are `CLIENT_KNOBS`' (`ClientKnobs.cpp` 7.4:78-81, 8.0:183-186):
/// `TRANSACTION_SIZE_LIMIT` 1e7, `KEY_SIZE_LIMIT` 1e4, `SYSTEM_KEY_SIZE_LIMIT` 3e4,
/// `VALUE_SIZE_LIMIT` 1e5. What differs between the releases is `getMaxKeySize`, the limit
/// above which a key in a conflict range or a clear is truncated: 7.4 adds the tenant prefix
/// size (8) to a non-system key's limit (`getMaxWriteKeySize(key, true)`,
/// `NativeAPI.actor.cpp` 7.4:11987-12003), 8.0 does not (`NativeAPI.cpp` 8.0:7370-7380).
/// [`CommitLimits::for_schema`] picks the right one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitLimits {
    /// `KEY_SIZE_LIMIT`.
    pub key_size_limit: usize,
    /// `SYSTEM_KEY_SIZE_LIMIT`: the limit for keys starting with `\xff`.
    pub system_key_size_limit: usize,
    /// `VALUE_SIZE_LIMIT`.
    pub value_size_limit: usize,
    /// `TRANSACTION_SIZE_LIMIT`: the largest value of the size limit option.
    pub transaction_size_limit: u64,
    /// The extra length `getMaxKeySize` allows a non-system key (conflict ranges and clears):
    /// `TenantAPI::PREFIX_SIZE` (8) on 7.4, 0 on 8.0.
    pub range_key_slack: usize,
    /// What a set or atomic operation may add to `key_size_limit`: 0, or 8 on 7.4 with the
    /// `RAW_ACCESS` option (`getMaxWriteKeySize(key, rawAccess)`).
    pub write_key_slack: usize,
}

impl CommitLimits {
    /// The limits of the release `schema` is for.
    pub const fn for_schema(schema: SchemaVersion) -> CommitLimits {
        CommitLimits {
            key_size_limit: 10_000,
            system_key_size_limit: 30_000,
            value_size_limit: 100_000,
            transaction_size_limit: 10_000_000,
            range_key_slack: match schema {
                SchemaVersion::V7_4 => 8,
                SchemaVersion::V8_0 => 0,
            },
            write_key_slack: 0,
        }
    }

    fn is_system(key: &[u8]) -> bool {
        key.first() == Some(&0xff)
    }

    /// `getMaxWriteKeySize(key, rawAccess)`: the longest `key` a set or atomic operation
    /// may use.
    pub fn max_write_key_size(&self, key: &[u8]) -> usize {
        if Self::is_system(key) {
            self.system_key_size_limit
        } else {
            self.key_size_limit + self.write_key_slack
        }
    }

    /// `getMaxKeySize(key)` (also `getMaxReadKeySize` and `getMaxClearKeySize`): the key
    /// length above which a range's key is truncated.
    pub fn max_range_key_size(&self, key: &[u8]) -> usize {
        if Self::is_system(key) {
            self.system_key_size_limit
        } else {
            self.key_size_limit + self.range_key_slack
        }
    }
}

/// `getSize()` of the parts of a commit: `VectorRef::expectedSize()` of the mutations and of
/// the read and write conflict ranges (the element sizes plus the bytes of every key and
/// value).
pub fn expected_size(
    mutations: &[Mutation],
    read_conflicts: &[KeyRange],
    write_conflicts: &[KeyRange],
) -> u64 {
    let ranges = |ranges: &[KeyRange]| {
        ranges
            .iter()
            .map(|r| KEY_RANGE_REF_SIZE + (r.begin.len() + r.end.len()) as u64)
            .sum::<u64>()
    };
    let muts: u64 = mutations
        .iter()
        .map(|m| MUTATION_REF_SIZE + (m.param1.len() + m.param2.len()) as u64)
        .sum();
    muts + ranges(read_conflicts) + ranges(write_conflicts)
}

/// The range as `addReadConflictRange`, `addWriteConflictRange` and `clear(range)` store it:
/// a key longer than its limit is cut to one byte past the limit (no key in the database is
/// longer than the limit, so the range means the same), and a range that is empty then is
/// dropped (`None`). `Err` for `begin > end`.
pub(crate) fn normalize_range(
    range: &KeyRange,
    limits: &CommitLimits,
) -> Result<Option<KeyRange>, crate::error::Error> {
    if range.begin > range.end {
        return Err(super::error::inverted_range());
    }
    let cut = |key: &[u8]| {
        let max = limits.max_range_key_size(key);
        key[..key.len().min(max + 1)].to_vec()
    };
    let (begin, end) = (cut(&range.begin), cut(&range.end));
    Ok((begin != end).then_some(KeyRange { begin, end }))
}
