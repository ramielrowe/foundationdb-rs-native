//! The read-your-writes snapshot cache: a port of `fdbclient/SnapshotCache.h` (identical in 7.4
//! and 8.0 up to formatting; 8.0 only adds `ExtStringRef::copyTo` and a materialization unit
//! test), plus `getKnownKeyRange` / `getKnownKeyRangeBack` from `ReadYourWrites.actor.cpp`
//! (`ReadYourWrites.cpp` in 8.0), which turn a range read's outcome into the range the cache
//! may mark known.
//!
//! The cache records what the transaction has read from storage at its read version, so a
//! later read of the same keys is answered without a round trip. It holds a set of *entries*.
//! An entry is a *known* range `[begin, end)`: the keys that are not among its `values` are
//! known to be absent. Everything outside every entry is *unknown*. Entries never overlap, and
//! they are not coalesced when they touch (as in C++: `skipUncached` and friends in
//! `ReadYourWrites` rely on the boundaries staying where reads put them).
//!
//! Reading the cache is done through [`SnapshotCacheIter`], which walks the key space as a
//! sequence of *segments*, each one of three types ([`SegmentType`]): a single known key/value
//! pair, a known-empty range, or an unknown range. Every key belongs to exactly one segment;
//! the first segment begins at `""` and the last one is the always-known-empty
//! `[\xff\xff, \xff\xff\x00)`. A segment boundary is an [`ExtKey`], a key with trailing zero
//! bytes appended (`key + \x00` is the smallest key after `key`, and a single-key segment is
//! `[key, key + \x00)`), so a boundary never has to be allocated.
//!
//! # Differences from C++
//!
//! * Keys and values are owned `Vec<u8>` (`KeyValue`), not arena references, and
//!   [`SnapshotCache::insert_range`] copies what it keeps; there is no arena to tie lifetimes
//!   to, and so no bytes accounting (the C++ cache tracks none either).
//! * The `IndexedSet` is a `BTreeMap` keyed by an entry's begin key.
//! * An iterator borrows the cache, so the borrow checker enforces what C++ documents as
//!   "invalidation": after an `insert` the caller builds a new iterator (`ReadYourWrites`
//!   does the same, re-resolving its key selectors).
//! * [`SnapshotCacheIter::advance`] / [`retreat`](SnapshotCacheIter::retreat) at the last / first
//!   segment return `false` and stay put, where C++ steps off the end (undefined behaviour).
//!   `skip` clamps any key `>= \xff\xff` to the final segment, where C++ only handles
//!   `\xff\xff` exactly. [`SnapshotCache::insert_range`] clamps a range end to `\xff\xff` and
//!   always drops rows outside the range it marks known, where C++ trusts its caller.
//! * `SnapshotCache::promise` is declared but never defined in C++; it is not ported.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;

use fdb_native_wire::KeyValue;

/// `allKeys.end`: the end of the key space a transaction can read (`\xff\xff`).
const ALL_KEYS_END: &[u8] = b"\xff\xff";
/// `afterAllKeys`: the end of the last segment, `keyAfter(allKeys.end)`.
const AFTER_ALL_KEYS: &[u8] = b"\xff\xff\x00";

/// FDB's `ExtStringRef`: a borrowed key followed by `extra_zero_bytes` zero bytes.
///
/// The ordering is the byte string's: `ExtKey::new(b"a", 1) == ExtKey::from(&b"a\0"[..])`, and
/// `ExtKey::new(b"a\0", 0) == ExtKey::new(b"a", 1)`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExtKey<'a> {
    base: &'a [u8],
    extra_zero_bytes: usize,
}

impl<'a> ExtKey<'a> {
    /// The empty key.
    pub(crate) const EMPTY: ExtKey<'static> = ExtKey {
        base: b"",
        extra_zero_bytes: 0,
    };

    /// `base` followed by `extra_zero_bytes` zero bytes.
    pub(crate) const fn new(base: &'a [u8], extra_zero_bytes: usize) -> ExtKey<'a> {
        ExtKey {
            base,
            extra_zero_bytes,
        }
    }

    /// The key's length in bytes.
    pub(crate) const fn size(&self) -> usize {
        self.base.len() + self.extra_zero_bytes
    }

    /// The part that is borrowed (the key without its trailing extra zero bytes).
    pub(crate) const fn base(&self) -> &'a [u8] {
        self.base
    }

    /// The number of zero bytes after [`base`](Self::base).
    pub(crate) const fn extra_zero_bytes(&self) -> usize {
        self.extra_zero_bytes
    }

    /// `ExtStringRef::keyAfter`: this key followed by one more zero byte.
    pub(crate) const fn key_after(&self) -> ExtKey<'a> {
        ExtKey {
            base: self.base,
            extra_zero_bytes: self.extra_zero_bytes + 1,
        }
    }

    /// `ExtStringRef::isKeyAfter`: is `self == s + \x00`?
    pub(crate) fn is_key_after(&self, s: &ExtKey<'_>) -> bool {
        if self.size() != s.size() + 1 {
            return false;
        }
        if self.extra_zero_bytes == 0 && self.base.last() != Some(&0) {
            return false;
        }
        self.starts_with(s)
    }

    /// `ExtStringRef::startsWith`.
    pub(crate) fn starts_with(&self, s: &ExtKey<'_>) -> bool {
        if self.size() < s.size() {
            return false;
        }
        let cbl = self.base.len().min(s.base.len());
        // Bytes of our base that lie within `s`'s extra zeros must be zero.
        if self.base.iter().take(s.size()).skip(cbl).any(|&b| b != 0) {
            return false;
        }
        // Bytes of `s`'s base beyond our base must be matched by our extra zeros.
        if s.base[cbl..].iter().any(|&b| b != 0) {
            return false;
        }
        self.base[..cbl] == s.base[..cbl]
    }

    /// `ExtStringRef::toStandaloneStringRef`: the key as an owned byte string.
    pub(crate) fn to_vec(self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.size());
        v.extend_from_slice(self.base);
        v.resize(self.size(), 0);
        v
    }

    /// `ExtStringRef::toArenaOrRef`: borrows the base when there are no extra zero bytes.
    pub(crate) fn to_cow(self) -> Cow<'a, [u8]> {
        if self.extra_zero_bytes == 0 {
            Cow::Borrowed(self.base)
        } else {
            Cow::Owned(self.to_vec())
        }
    }

    /// `ExtStringRef::assertRef`: the key itself, which must have no extra zero bytes.
    pub(crate) fn assert_ref(&self) -> &'a [u8] {
        assert_eq!(self.extra_zero_bytes, 0, "ExtKey has extra zero bytes");
        self.base
    }
}

impl<'a> From<&'a [u8]> for ExtKey<'a> {
    fn from(base: &'a [u8]) -> ExtKey<'a> {
        ExtKey {
            base,
            extra_zero_bytes: 0,
        }
    }
}

impl<'a> From<&'a Vec<u8>> for ExtKey<'a> {
    fn from(base: &'a Vec<u8>) -> ExtKey<'a> {
        ExtKey {
            base,
            extra_zero_bytes: 0,
        }
    }
}

impl Ord for ExtKey<'_> {
    /// `ExtStringRef::compare`: bytewise, as if the zeros were materialized.
    fn cmp(&self, rhs: &Self) -> Ordering {
        let cbl = self.base.len().min(rhs.base.len());
        let c = self.base[..cbl].cmp(&rhs.base[..cbl]);
        if c != Ordering::Equal {
            return c;
        }
        // One side's base may extend past the other's: its extra bytes compare against zeros.
        if self.base[cbl..].iter().any(|&b| b != 0) {
            return Ordering::Greater;
        }
        if rhs.base[cbl..].iter().any(|&b| b != 0) {
            return Ordering::Less;
        }
        self.size().cmp(&rhs.size())
    }
}

impl PartialOrd for ExtKey<'_> {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        Some(self.cmp(rhs))
    }
}

impl PartialEq for ExtKey<'_> {
    fn eq(&self, rhs: &Self) -> bool {
        self.cmp(rhs) == Ordering::Equal
    }
}

impl Eq for ExtKey<'_> {}

/// What a [`SnapshotCacheIter`] segment is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SegmentType {
    /// Nothing is known about the keys in the segment.
    Unknown,
    /// The keys in the segment are known to be absent.
    Empty,
    /// A single key, known to have a value.
    KeyValue,
}

/// One known range of the cache (C++ `SnapshotCache::Entry`; its begin key is the map key).
#[derive(Clone, Debug)]
struct Entry {
    /// The exclusive end of the range. Always fully materialized (the C++ `ExtStringRef` end of
    /// a single-key entry, `key + \x00`, is stored as bytes).
    end: Vec<u8>,
    /// The keys in `[begin, end)` that have values, ascending; the others are known absent.
    values: Vec<KeyValue>,
}

impl Entry {
    /// The number of segments the entry is made of: the unknown range before it (offset 0),
    /// then an empty range before each value, each value, and the empty range after the last
    /// one. Some of the empty ranges can be degenerate (zero length).
    fn segments(&self) -> usize {
        2 * (self.values.len() + 1)
    }
}

/// The transaction's cache of storage reads at its read version (C++ `SnapshotCache`).
#[derive(Clone, Debug)]
pub(crate) struct SnapshotCache {
    /// Entries by begin key. There are always degenerate entries at the two ends,
    /// `["", "")` and `[\xff\xff, \xff\xff\x00)`, which "reduce edge cases".
    entries: BTreeMap<Vec<u8>, Entry>,
}

impl Default for SnapshotCache {
    fn default() -> SnapshotCache {
        SnapshotCache::new()
    }
}

impl SnapshotCache {
    /// An empty cache: all of `["", \xff\xff)` is unknown.
    pub(crate) fn new() -> SnapshotCache {
        let mut entries = BTreeMap::new();
        entries.insert(
            Vec::new(),
            Entry {
                end: Vec::new(),
                values: Vec::new(),
            },
        );
        entries.insert(
            ALL_KEYS_END.to_vec(),
            Entry {
                end: AFTER_ALL_KEYS.to_vec(),
                values: Vec::new(),
            },
        );
        SnapshotCache { entries }
    }

    /// `empty()`: is nothing known about the snapshot's contents?
    pub(crate) fn is_empty(&self) -> bool {
        !self
            .entries
            .iter()
            .any(|(begin, e)| *begin != e.end && begin.as_slice() < ALL_KEYS_END)
    }

    /// A segment iterator positioned at the segment containing `key` (see
    /// [`SnapshotCacheIter::skip`]).
    pub(crate) fn segment_at(&self, key: &[u8]) -> SnapshotCacheIter<'_> {
        let mut it = SnapshotCacheIter::new(self);
        it.skip(key);
        it
    }

    /// `insert(KeyRef, Optional<ValueRef>)`: records that, in the snapshot, `key` has `value`
    /// (or is absent, if `None`). Does nothing, returning `false`, when `key` is already known;
    /// the new entry is `[key, key + \x00)`.
    pub(crate) fn insert_key(&mut self, key: &[u8], value: Option<&[u8]>) -> bool {
        if !self.segment_at(key).is_unknown_range() {
            return false;
        }
        let mut end = Vec::with_capacity(key.len() + 1);
        end.extend_from_slice(key);
        end.push(0);
        let values = value.map(|v| KeyValue::new(key, v)).into_iter().collect();
        self.entries.insert(key.to_vec(), Entry { end, values });
        true
    }

    /// `insert(KeyRangeRef, VectorRef<KeyValueRef>)`: records that, in the snapshot, the keys
    /// in `[begin, end)` have (only) the values `rows`, which are ascending and within the
    /// range. Returns whether anything was recorded.
    ///
    /// The ranges already known are never changed at their ends: the new entry covers just
    /// the part of `[begin, end)` that is not in the (at most two) entries that straddle its
    /// ends, and replaces the entries strictly inside. That is correct because every read is at
    /// the same version, so the cache's data and `rows` agree wherever they overlap.
    pub(crate) fn insert_range(&mut self, begin: &[u8], end: &[u8], rows: &[KeyValue]) -> bool {
        let end = end.min(ALL_KEYS_END);
        if begin >= end {
            return false;
        }

        let itb = self.segment_at(begin);
        let ite = self.segment_at(end);

        // `itb`'s entry, or the entry after the unknown range it is in, is the first one erased.
        let mut new_begin = begin;
        let mut erase_from = itb.begin.clone();
        if !itb.is_unknown_range() && itb.begin.as_slice() != begin {
            // Known data starts before `begin` and runs past it: start where it ends instead,
            // and erase from the entry after it.
            new_begin = itb.entry.end.as_slice();
            let Some((next, _)) = itb.next_entry() else {
                return false;
            };
            erase_from = next.clone();
        }
        // `ite`'s entry (if the end is known) is kept, and the new range stops where it begins.
        let new_end: &[u8] = if ite.is_unknown_range() {
            end
        } else {
            ite.begin.as_slice()
        };
        let erase_to = ite.begin.clone();

        if new_begin >= new_end {
            return false;
        }
        let lo = rows.partition_point(|kv| kv.key.as_slice() < new_begin);
        let hi = rows
            .partition_point(|kv| kv.key.as_slice() < new_end)
            .max(lo);
        let new_entry = Entry {
            end: new_end.to_vec(),
            values: rows[lo..hi].to_vec(),
        };
        let new_begin = new_begin.to_vec();

        let doomed: Vec<Vec<u8>> = self
            .entries
            .range::<[u8], _>((
                Bound::Included(erase_from.as_slice()),
                Bound::Excluded(erase_to.as_slice()),
            ))
            .map(|(k, _)| k.clone())
            .collect();
        for k in &doomed {
            self.entries.remove(k);
        }
        // `begin != allKeys.begin && itb.it->beginKey == allKeys.begin`: the erased entry was
        // the one at the start of the key space. (C++ keeps this case; it cannot arise.)
        let add_begin = !new_begin.is_empty() && erase_from.is_empty();
        self.entries.insert(new_begin, new_entry);
        if add_begin {
            self.entries.insert(
                Vec::new(),
                Entry {
                    end: Vec::new(),
                    values: Vec::new(),
                },
            );
        }
        true
    }

    /// Inserts the outcome of a range read that resolved `begin` and `end`: the range
    /// [`known_key_range`] (or, `reverse`, [`known_key_range_back`]) says the read covered gets
    /// the rows in it. `rows` are in the order the read returned them (descending if
    /// `reverse`). This is what `ReadYourWrites` does after each `getRange` it issues to
    /// storage (`cache.insert(range, snapshot_read)`, reversing the rows first for a reverse
    /// read). Returns whether anything was recorded.
    pub(crate) fn insert_read(
        &mut self,
        reverse: bool,
        outcome: &RangeReadOutcome<'_>,
        begin: SelectorBound<'_>,
        end: SelectorBound<'_>,
    ) -> bool {
        if reverse {
            let Some((b, e)) = known_key_range_back(outcome, begin, end) else {
                return false;
            };
            let ascending: Vec<KeyValue> = outcome.rows.iter().rev().cloned().collect();
            self.insert_range(&b, &e, &ascending)
        } else {
            let Some((b, e)) = known_key_range(outcome, begin, end) else {
                return false;
            };
            self.insert_range(&b, &e, outcome.rows)
        }
    }

    /// The entry containing `key`, or the nearest one before it (C++ `lastLessOrEqual`).
    fn last_entry_at_or_before(&self, key: &[u8]) -> (&Vec<u8>, &Entry) {
        self.entries
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
            .next_back()
            // The entry at "" is never erased (see `insert_range`'s `add_begin`).
            .or_else(|| self.entries.iter().next())
            .expect("a snapshot cache always has entries")
    }

    /// Checks the structure's invariants, for tests.
    #[cfg(test)]
    fn assert_invariants(&self) {
        let mut prev_end: Option<&[u8]> = None;
        assert!(
            self.entries.contains_key(&b""[..]),
            "no entry at the start of the key space"
        );
        let last = self.entries.iter().next_back().expect("entries");
        assert_eq!(last.0.as_slice(), ALL_KEYS_END);
        assert_eq!(last.1.end.as_slice(), AFTER_ALL_KEYS);
        for (begin, e) in &self.entries {
            assert!(begin <= &e.end, "entry {begin:?} ends before it begins");
            if let Some(p) = prev_end {
                assert!(p <= begin.as_slice(), "entries overlap at {begin:?}");
            }
            let mut prev_key: Option<&[u8]> = None;
            for kv in &e.values {
                assert!(
                    kv.key >= *begin && kv.key < e.end,
                    "value outside its entry"
                );
                if let Some(p) = prev_key {
                    assert!(p < kv.key.as_slice(), "values not ascending");
                }
                prev_key = Some(&kv.key);
            }
            prev_end = Some(&e.end);
        }
    }
}

/// Iterates over the segments of a [`SnapshotCache`] (C++ `SnapshotCache::iterator`): single
/// key/value pairs, empty ranges, and unknown ranges, in key order, each key in exactly one.
///
/// Note that an unknown range stops at the next individual key that is known, even though a
/// reader might want to read through that key; and `RYWIterator` merges the cache's segments
/// with the write map's, so it can stop an unknown range at a dependent write as well.
///
/// The iterator borrows the cache, so it is invalidated (statically) by an insert.
#[derive(Clone, Debug)]
pub(crate) struct SnapshotCacheIter<'a> {
    cache: &'a SnapshotCache,
    /// The begin key of the current entry.
    begin: &'a Vec<u8>,
    entry: &'a Entry,
    /// 0 for the unknown range before the entry, then 1 for the empty range up to the first
    /// value, 2 for the first value, 3 for the empty range after it, ...: `0 <= offset <
    /// entry.segments()`.
    offset: usize,
}

impl PartialEq for SnapshotCacheIter<'_> {
    fn eq(&self, r: &Self) -> bool {
        self.begin == r.begin && self.offset == r.offset
    }
}

impl Eq for SnapshotCacheIter<'_> {}

impl<'a> SnapshotCacheIter<'a> {
    /// C++ `iterator(cache)`: at the (degenerate) first segment; use [`skip`](Self::skip) to
    /// position it.
    pub(crate) fn new(cache: &'a SnapshotCache) -> SnapshotCacheIter<'a> {
        let (begin, entry) = cache
            .entries
            .iter()
            .next()
            .expect("a snapshot cache always has entries");
        SnapshotCacheIter {
            cache,
            begin,
            entry,
            offset: 0,
        }
    }

    /// The segment's type.
    pub(crate) fn segment_type(&self) -> SegmentType {
        if self.offset == 0 {
            SegmentType::Unknown
        } else if self.offset & 1 == 1 {
            SegmentType::Empty
        } else {
            SegmentType::KeyValue
        }
    }

    /// Is the segment a single known key/value pair?
    pub(crate) fn is_kv(&self) -> bool {
        self.segment_type() == SegmentType::KeyValue
    }

    /// Is nothing known about the segment's keys?
    pub(crate) fn is_unknown_range(&self) -> bool {
        self.segment_type() == SegmentType::Unknown
    }

    /// Are the segment's keys known to be absent?
    pub(crate) fn is_empty_range(&self) -> bool {
        self.segment_type() == SegmentType::Empty
    }

    /// The first key of the segment.
    pub(crate) fn begin_key(&self) -> ExtKey<'a> {
        match self.offset {
            0 => self
                .prev_entry()
                .map_or(ExtKey::EMPTY, |(_, prev)| ExtKey::from(&prev.end)),
            1 => ExtKey::from(self.begin),
            o => ExtKey::new(&self.entry.values[(o - 2) >> 1].key, o & 1),
        }
    }

    /// The first key after the segment.
    pub(crate) fn end_key(&self) -> ExtKey<'a> {
        if self.offset == 0 {
            ExtKey::from(self.begin)
        } else if self.offset == self.entry.segments() - 1 {
            ExtKey::from(&self.entry.end)
        } else {
            ExtKey::new(
                &self.entry.values[(self.offset - 1) >> 1].key,
                1 - (self.offset & 1),
            )
        }
    }

    /// The key/value pair, if the segment [`is_kv`](Self::is_kv).
    pub(crate) fn kv(&self) -> Option<&'a KeyValue> {
        if self.is_kv() {
            self.entry.values.get((self.offset - 2) >> 1)
        } else {
            None
        }
    }

    /// C++ `++it`: moves to the next (non-degenerate) segment. Returns `false`, without
    /// moving, if this is the last segment.
    #[must_use]
    pub(crate) fn advance(&mut self) -> bool {
        let original_end = self.end_key();
        let saved = self.clone();
        loop {
            if !self.raw_next() {
                *self = saved;
                return false;
            }
            if self.end_key() != original_end {
                return true;
            }
        }
    }

    /// C++ `--it`: moves to the previous (non-degenerate) segment. Returns `false`, without
    /// moving, if this is the first segment.
    #[must_use]
    pub(crate) fn retreat(&mut self) -> bool {
        let original_begin = self.begin_key();
        let saved = self.clone();
        loop {
            if !self.raw_prev() {
                *self = saved;
                return false;
            }
            if self.begin_key() != original_begin {
                return true;
            }
        }
    }

    /// C++ `skip`: moves to the segment containing `key` (so `begin_key() <= key < end_key()`).
    /// A `key` at or after `\xff\xff` goes to the last segment.
    pub(crate) fn skip(&mut self, key: &[u8]) {
        if key >= ALL_KEYS_END {
            let (begin, entry) = self.cache.entries.iter().next_back().expect("entries");
            (self.begin, self.entry, self.offset) = (begin, entry, 1);
            return;
        }
        let (begin, entry) = self.cache.last_entry_at_or_before(key);
        (self.begin, self.entry) = (begin, entry);
        if key >= entry.end.as_slice() {
            // After the entry: in the unknown range before the next one.
            self.offset = 0;
            if let Some((b, e)) = self.next_entry() {
                (self.begin, self.entry) = (b, e);
            } else {
                self.offset = 1; // unreachable: the last entry begins at \xff\xff > key
            }
        } else {
            let idx = entry.values.partition_point(|kv| kv.key.as_slice() < key);
            let hit = entry.values.get(idx).is_some_and(|kv| kv.key == key);
            self.offset = idx * 2 + 1 + usize::from(hit);
        }
    }

    /// C++ `skipContiguous`: from a key/value segment, moves to the last key/value in the same
    /// entry that comes before `key`, so that every pair from here through there has a key
    /// `< key` and nothing else is known in between but empty ranges. Returns those pairs
    /// (this one first). Does nothing, returning no pairs, if the segment is not a key/value.
    ///
    /// The segment's own key must be `< key`.
    pub(crate) fn skip_contiguous(&mut self, key: ExtKey<'_>) -> &'a [KeyValue] {
        if !self.is_kv() {
            return &[];
        }
        let values = &self.entry.values;
        let first = (self.offset - 2) >> 1;
        let from = first + 1;
        let j = from + values[from..].partition_point(|kv| ExtKey::from(&kv.key) < key);
        self.offset = 2 * j;
        &values[first..j]
    }

    /// C++ `skipContiguousBack`: from a key/value segment, moves to the first key/value in the
    /// same entry such that every pair from there through here has a key `>= key`. Returns
    /// those pairs (ascending: this one is last). Does nothing, returning no pairs, if the
    /// segment is not a key/value.
    ///
    /// The segment's own key must be `>= key`.
    pub(crate) fn skip_contiguous_back(&mut self, key: ExtKey<'_>) -> &'a [KeyValue] {
        if !self.is_kv() {
            return &[];
        }
        let values = &self.entry.values;
        let last = (self.offset - 2) >> 1;
        let j = values[..last].partition_point(|kv| ExtKey::from(&kv.key) < key);
        self.offset = 2 * j + 2;
        &values[j..=last]
    }

    fn next_entry(&self) -> Option<(&'a Vec<u8>, &'a Entry)> {
        self.cache
            .entries
            .range::<[u8], _>((Bound::Excluded(self.begin.as_slice()), Bound::Unbounded))
            .next()
    }

    fn prev_entry(&self) -> Option<(&'a Vec<u8>, &'a Entry)> {
        self.cache
            .entries
            .range::<[u8], _>((Bound::Unbounded, Bound::Excluded(self.begin.as_slice())))
            .next_back()
    }

    /// One step of `++`, which may land on a degenerate segment. `false` (not moved) at the end.
    fn raw_next(&mut self) -> bool {
        if self.offset + 1 < self.entry.segments() {
            self.offset += 1;
            true
        } else if let Some((b, e)) = self.next_entry() {
            (self.begin, self.entry, self.offset) = (b, e, 0);
            true
        } else {
            false
        }
    }

    /// One step of `--`, which may land on a degenerate segment. `false` (not moved) at the start.
    fn raw_prev(&mut self) -> bool {
        if self.offset > 0 {
            self.offset -= 1;
            true
        } else if let Some((b, e)) = self.prev_entry() {
            (self.begin, self.entry, self.offset) = (b, e, e.segments() - 1);
            true
        } else {
            false
        }
    }
}

/// A resolved key selector, as far as [`known_key_range`] needs it: its key and offset.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SelectorBound<'a> {
    /// The selector's reference key (`KeySelector::getKey`).
    pub(crate) key: &'a [u8],
    /// The selector's offset (`firstGreaterOrEqual(k)` has offset 1).
    pub(crate) offset: i32,
}

/// What a storage range read returned, as far as [`known_key_range`] needs it (the fields of
/// C++ `RangeResultRef`).
#[derive(Clone, Copy, Debug)]
pub(crate) struct RangeReadOutcome<'a> {
    /// The rows, in the order the read returned them.
    pub(crate) rows: &'a [KeyValue],
    /// The read stopped at a limit with more data left.
    pub(crate) more: bool,
    /// The read reached the beginning of the key space.
    pub(crate) read_to_begin: bool,
    /// The read reached the end of the key space (`\xff\xff`).
    pub(crate) read_through_end: bool,
    /// If present, how far the read got (the key to continue from) even if no row says so.
    pub(crate) read_through: Option<&'a [u8]>,
}

/// `getKnownKeyRange`: the range `[begin, end)` of keys a forward range read from `begin` to
/// `end` found out everything about, to hand to [`SnapshotCache::insert_range`] with the rows.
/// `None` for an empty range.
///
/// The read's end is known only if the read was not cut short (`!more`) and the end selector
/// is a plain key (`offset >= 1`); a cut-short read is known up to just past its last row (or
/// as far as `read_through`).
pub(crate) fn known_key_range(
    outcome: &RangeReadOutcome<'_>,
    begin: SelectorBound<'_>,
    end: SelectorBound<'_>,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut begin_key: &[u8] = if begin.offset <= 1 {
        begin.key
    } else {
        ALL_KEYS_END
    };
    let mut end_key = if !outcome.more && end.offset >= 1 {
        ExtKey::from(end.key)
    } else {
        ExtKey::EMPTY
    };

    if outcome.read_to_begin {
        begin_key = b"";
    }
    if outcome.read_through_end {
        end_key = ExtKey::from(ALL_KEYS_END);
    }

    if let (Some(first), Some(last)) = (outcome.rows.first(), outcome.rows.last()) {
        begin_key = begin_key.min(first.key.as_slice());
        end_key = if let Some(rt) = outcome.read_through {
            end_key.max(ExtKey::from(rt))
        } else if !outcome.more && ExtKey::from(&last.key) < end_key {
            end_key
        } else {
            ExtKey::new(&last.key, 1)
        };
    }
    (ExtKey::from(begin_key) < end_key).then(|| (begin_key.to_vec(), end_key.to_vec()))
}

/// `getKnownKeyRangeBack`: [`known_key_range`] for a reverse range read. `outcome.rows` are in
/// the order returned (descending); [`SnapshotCache::insert_range`] wants them ascending.
pub(crate) fn known_key_range_back(
    outcome: &RangeReadOutcome<'_>,
    begin: SelectorBound<'_>,
    end: SelectorBound<'_>,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut begin_key: &[u8] = if !outcome.more && begin.offset <= 1 {
        begin.key
    } else {
        ALL_KEYS_END
    };
    let mut end_key = if end.offset >= 1 {
        ExtKey::from(end.key)
    } else {
        ExtKey::EMPTY
    };

    if outcome.read_to_begin {
        begin_key = b"";
    }
    if outcome.read_through_end {
        end_key = ExtKey::from(ALL_KEYS_END);
    }

    if let (Some(first), Some(last)) = (outcome.rows.first(), outcome.rows.last()) {
        begin_key = if let Some(rt) = outcome.read_through {
            rt.min(begin_key)
        } else if !outcome.more && last.key.as_slice() > begin_key {
            begin_key
        } else {
            &last.key
        };
        end_key = if ExtKey::from(&first.key) < end_key {
            end_key
        } else {
            ExtKey::new(&first.key, 1)
        };
    }
    (ExtKey::from(begin_key) < end_key).then(|| (begin_key.to_vec(), end_key.to_vec()))
}

#[cfg(test)]
mod tests;
