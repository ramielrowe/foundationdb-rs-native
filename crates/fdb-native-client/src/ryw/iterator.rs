//! `RYWIterator` (`fdbclient/RYWIterator.cpp` and `RYWIterator.h`: 7.4 in
//! `fdbclient/include/fdbclient/`, 8.0 in `fdbclient/`; the same code in both): the
//! transaction's view of the key space, a [`WriteMapIter`] and a [`SnapshotCacheIter`] walked
//! together. Each of its segments is a piece of one write map segment and one cache segment,
//! and its type comes from the two (`typeMap`):
//!
//! | write map \ cache | unknown | empty | key/value |
//! |---|---|---|---|
//! | unmodified | unknown | empty | key/value |
//! | cleared | empty | empty | empty |
//! | independent write | key/value | key/value | key/value |
//! | dependent write | unknown | key/value | key/value |
//!
//! The value of a key/value segment ([`kv`](RywIter::kv)) is the cache's for an unmodified
//! key, else the key's operations coalesced over the cached value
//! ([`WriteMap::coalesce_under`]). A segment of keys the transaction's versionstamped
//! operations made unreadable is `accessed_unreadable` (1036) for anything but
//! [`is_unreadable`](RywIter::is_unreadable), unless the iterator
//! [bypasses](RywIter::bypass_unreadable_protection) that.
//!
//! # Lifetimes
//!
//! C++'s iterator points into the live cache and the write map's persistent tree; the read
//! path keeps it over its waits and repositions it ([`skip`](RywIter::skip)) after each. Here
//! the cache iterator borrows the cache (behind the transaction's lock), so an [`RywIter`]
//! lives only between two waits; what the read path keeps over a wait is the write map
//! iterator, a snapshot of the writes when the read started
//! ([`into_writes`](RywIter::into_writes)), as C++'s `WriteMap::iterator` is.
//!
//! `RYWIterator.cpp` 7.4 (8.0): `typeMap` 25-42 (27-44), `type` 44-49 (46-51), `kv` 72-89
//! (74-91), `operator++` 91-99 (93-101), `operator--` 101-109 (103-111), `skip` 119-124
//! (121-126), `skipContiguous(Back)` 126-138 (128-140), `updateCmp` 157-160 (159-162).

use std::cmp::Ordering;

use fdb_native_wire::KeyValue;

use crate::error::Error;
use crate::error::codes::ACCESSED_UNREADABLE;
use crate::ryw::{
    ExtKey, SegmentType, SnapshotCache, SnapshotCacheIter, WriteMap, WriteMapIter, WriteSegmentType,
};

/// `RYWIterator::SEGMENT_TYPE`.
pub(crate) type RywSegment = SegmentType;

/// `RYWIterator::typeMap`, indexed by the write map segment's type then the cache's.
fn type_map(writes: WriteSegmentType, cache: SegmentType) -> RywSegment {
    use SegmentType::{Empty, KeyValue as Kv, Unknown};
    match (writes, cache) {
        (WriteSegmentType::UnmodifiedRange, c) => c,
        (WriteSegmentType::ClearedRange, _) => Empty,
        (WriteSegmentType::IndependentWrite, _) => Kv,
        (WriteSegmentType::DependentWrite, Unknown) => Unknown,
        (WriteSegmentType::DependentWrite, _) => Kv,
    }
}

/// `RYWIterator`: see the module documentation.
#[derive(Clone)]
pub(crate) struct RywIter<'c> {
    cache: SnapshotCacheIter<'c>,
    writes: WriteMapIter,
    /// `begin_key_cmp`: the cache segment's begin against the write segment's.
    begin_cmp: Ordering,
    /// `end_key_cmp`.
    end_cmp: Ordering,
    /// `bypassUnreadable`.
    bypass_unreadable: bool,
}

impl<'c> RywIter<'c> {
    /// `RYWIterator(cache, writes)` at the segment of `""`: the cache, and a snapshot of the
    /// writes (any position: it is skipped).
    pub(crate) fn new(cache: &'c SnapshotCache, writes: WriteMapIter) -> RywIter<'c> {
        let mut it = RywIter {
            cache: SnapshotCacheIter::new(cache),
            writes,
            begin_cmp: Ordering::Equal,
            end_cmp: Ordering::Equal,
            bypass_unreadable: false,
        };
        it.skip(b"");
        it
    }

    /// The snapshot cache iterator a read without read-your-writes uses (C++'s
    /// `SnapshotCache::iterator` as the read path's `Iter`): an `RYWIterator` over an empty
    /// write map behaves the same (all its keys are unmodified and readable).
    #[cfg(test)]
    pub(crate) fn without_writes(cache: &'c SnapshotCache) -> RywIter<'c> {
        RywIter::new(cache, WriteMap::new().iter())
    }

    /// `bypassUnreadableProtection`.
    pub(crate) fn bypass_unreadable_protection(&mut self) {
        self.bypass_unreadable = true;
    }

    fn check_readable(&self) -> Result<(), Error> {
        if self.is_unreadable() && !self.bypass_unreadable {
            return Err(Error::new(ACCESSED_UNREADABLE));
        }
        Ok(())
    }

    /// `type`: `accessed_unreadable` for an unreadable segment (unless bypassed).
    pub(crate) fn segment_type(&self) -> Result<RywSegment, Error> {
        self.check_readable()?;
        Ok(type_map(
            self.writes.segment_type(),
            self.cache.segment_type(),
        ))
    }

    /// `is_kv`.
    pub(crate) fn is_kv(&self) -> Result<bool, Error> {
        Ok(self.segment_type()? == SegmentType::KeyValue)
    }

    /// `is_unknown_range`.
    pub(crate) fn is_unknown_range(&self) -> Result<bool, Error> {
        Ok(self.segment_type()? == SegmentType::Unknown)
    }

    /// `is_empty_range`.
    pub(crate) fn is_empty_range(&self) -> Result<bool, Error> {
        Ok(self.segment_type()? == SegmentType::Empty)
    }

    /// `is_dependent`: the segment is a dependent write.
    pub(crate) fn is_dependent(&self) -> bool {
        self.writes.segment_type() == WriteSegmentType::DependentWrite
    }

    /// `is_unreadable` (never an error).
    pub(crate) fn is_unreadable(&self) -> bool {
        self.writes.is_unreadable()
    }

    /// `beginKey`: the later of the two segments' begins.
    pub(crate) fn begin_key(&self) -> ExtKey<'_> {
        if self.begin_cmp != Ordering::Greater {
            self.writes.begin_key()
        } else {
            self.cache.begin_key()
        }
    }

    /// `endKey`: the earlier of the two segments' ends.
    pub(crate) fn end_key(&self) -> ExtKey<'_> {
        if self.end_cmp != Ordering::Greater {
            self.cache.end_key()
        } else {
            self.writes.end_key()
        }
    }

    /// `kv`: the segment's key and value, `None` if the key's operations clear it (a
    /// `CompareAndClear`). Only for a key/value segment.
    pub(crate) fn kv(&self) -> Result<Option<KeyValue>, Error> {
        self.check_readable()?;
        if self.writes.is_unmodified_range() {
            return Ok(self.cache.kv().cloned());
        }
        let op = self
            .writes
            .op()
            .expect("a key/value segment that is not unmodified is an operation");
        let base = if self.writes.is_independent() == Some(true) || self.cache.is_empty_range() {
            None
        } else {
            self.cache.kv().map(|kv| &kv.value[..])
        };
        let result = WriteMap::coalesce_under(op, base)?;
        Ok(result.value.map(|value| {
            KeyValue::new(
                self.writes.begin_key().assert_ref().to_vec(),
                value.to_vec(),
            )
        }))
    }

    /// `operator++`.
    pub(crate) fn advance(&mut self) {
        if self.end_cmp != Ordering::Greater {
            let _ = self.cache.advance();
        }
        if self.end_cmp != Ordering::Less {
            let _ = self.writes.advance();
        }
        self.begin_cmp = self.end_cmp.reverse();
        self.end_cmp = self.cache.end_key().cmp(&self.writes.end_key());
    }

    /// `operator--`.
    pub(crate) fn retreat(&mut self) {
        if self.begin_cmp != Ordering::Less {
            let _ = self.cache.retreat();
        }
        if self.begin_cmp != Ordering::Greater {
            let _ = self.writes.retreat();
        }
        self.end_cmp = self.begin_cmp.reverse();
        self.begin_cmp = self.cache.begin_key().cmp(&self.writes.begin_key());
    }

    /// `skip`: to the segment that contains `key`.
    pub(crate) fn skip(&mut self, key: &[u8]) {
        self.cache.skip(key);
        self.writes.skip(key);
        self.update_cmp();
    }

    /// `skipContiguous` and the pointer arithmetic of `getRangeValue` around it: from a
    /// key/value segment, the key/values up to the last one before `key` that are contiguous
    /// in the cache within an unmodified range (this segment's first), moving there. A
    /// segment that is not such a run gives its own key/value only (`None` if its operations
    /// clear the key), without moving.
    pub(crate) fn take_contiguous(&mut self, key: &[u8]) -> Result<Vec<KeyValue>, Error> {
        if self.is_kv()? && self.writes.is_unmodified_range() {
            let bound = ExtKey::from(key).min(self.writes.end_key());
            let rows = self.cache.skip_contiguous(bound).to_vec();
            self.update_cmp();
            return Ok(rows);
        }
        Ok(self.kv()?.into_iter().collect())
    }

    /// `skipContiguousBack` and `getRangeValueBack`'s arithmetic around it: the key/values
    /// (ascending) from the first one at or after `key` contiguous with this one, moving to
    /// it; as [`take_contiguous`](Self::take_contiguous) otherwise.
    pub(crate) fn take_contiguous_back(&mut self, key: &[u8]) -> Result<Vec<KeyValue>, Error> {
        if self.is_kv()? && self.writes.is_unmodified_range() {
            let wb = self.writes.begin_key();
            let after = wb.key_after().to_vec();
            let bound = ExtKey::from(key).max(ExtKey::from(&after));
            let rows = self.cache.skip_contiguous_back(bound).to_vec();
            self.update_cmp();
            return Ok(rows);
        }
        Ok(self.kv()?.into_iter().collect())
    }

    fn update_cmp(&mut self) {
        self.begin_cmp = self.cache.begin_key().cmp(&self.writes.begin_key());
        self.end_cmp = self.cache.end_key().cmp(&self.writes.end_key());
    }
}

impl PartialEq for RywIter<'_> {
    /// `operator==`: the same segment of both.
    fn eq(&self, other: &RywIter<'_>) -> bool {
        self.cache == other.cache && self.writes == other.writes
    }
}

impl Eq for RywIter<'_> {}

#[cfg(test)]
mod tests;
