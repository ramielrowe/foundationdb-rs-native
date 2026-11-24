//! The read conflict ranges of a read-your-writes transaction: `readConflicts`, a
//! `CoalescedKeyRefRangeMap<bool>` (`ReadYourWrites.h` 7.4:237; `ReadYourWrites.cpp` 8.0 keeps
//! it in `RYWTransactionState`), which the reads fill (`updateConflictMap`) and the commit
//! sends, one read conflict range per maximal range of `true`
//! (`ReadYourWrites.actor.cpp` 7.4:1394-1399, 8.0:1388-1393).
//!
//! `CoalescedKeyRefRangeMap::insert` (`KeyRangeMap.h` 7.4:304-351) merges a range with the
//! ranges of the same value it overlaps or touches, and only `true` is ever inserted, so the
//! map is the union of the ranges inserted, as disjoint, non-adjacent ranges: here a
//! [`BTreeMap`] from each range's begin to its end.

use std::collections::BTreeMap;
use std::ops::Bound;

use fdb_native_schema::domain::KeyRange;

/// The union of the read conflict ranges added so far (see the module documentation).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReadConflicts {
    /// Disjoint, non-adjacent, non-empty ranges: begin -> end.
    ranges: BTreeMap<Vec<u8>, Vec<u8>>,
    /// What the reads added to the transaction's `approximateSize` (`updateConflictMap`
    /// counts each insertion, overlapping or not).
    approximate_size: u64,
}

impl ReadConflicts {
    /// No range.
    #[cfg(test)]
    pub(crate) fn new() -> ReadConflicts {
        ReadConflicts::default()
    }

    /// Whether there is no range.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// `readConflicts.insert(range, true)`: adds `[begin, end)` (nothing if it is empty).
    pub(crate) fn insert(&mut self, begin: &[u8], end: &[u8]) {
        if begin >= end {
            return;
        }
        let mut new_begin = begin.to_vec();
        let mut new_end = end.to_vec();
        // A range that starts before `begin` and reaches it merges.
        if let Some((b, e)) = self
            .ranges
            .range::<[u8], _>((Bound::Unbounded, Bound::Excluded(begin)))
            .next_back()
        {
            if e.as_slice() >= begin {
                new_begin = b.clone();
                if *e > new_end {
                    new_end = e.clone();
                }
            }
        }
        // The ranges that start in `[begin, end]` merge.
        let merged: Vec<Vec<u8>> = self
            .ranges
            .range::<[u8], _>((Bound::Included(begin), Bound::Included(end)))
            .map(|(b, _)| b.clone())
            .collect();
        for b in merged {
            if let Some(e) = self.ranges.remove(&b) {
                if e > new_end {
                    new_end = e;
                }
            }
        }
        self.ranges.insert(new_begin, new_end);
    }

    /// Counts `n` bytes in the transaction's approximate size.
    pub(crate) fn add_approximate_size(&mut self, n: usize) {
        self.approximate_size = self.approximate_size.saturating_add(n as u64);
    }

    /// What the insertions added to the transaction's approximate size.
    pub(crate) fn approximate_size(&self) -> u64 {
        self.approximate_size
    }

    /// The ranges, ascending.
    pub(crate) fn ranges(&self) -> impl Iterator<Item = KeyRange> + '_ {
        self.ranges.iter().map(|(b, e)| KeyRange {
            begin: b.clone(),
            end: e.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(b: &[u8], e: &[u8]) -> KeyRange {
        KeyRange {
            begin: b.to_vec(),
            end: e.to_vec(),
        }
    }

    fn all(c: &ReadConflicts) -> Vec<KeyRange> {
        c.ranges().collect()
    }

    #[test]
    fn merges_overlapping_and_touching_ranges() {
        let mut c = ReadConflicts::new();
        c.insert(b"b", b"b");
        assert!(c.is_empty());
        c.insert(b"c", b"d");
        c.insert(b"a", b"b");
        assert_eq!(all(&c), vec![r(b"a", b"b"), r(b"c", b"d")]);
        c.insert(b"b", b"c");
        assert_eq!(all(&c), vec![r(b"a", b"d")]);
        c.insert(b"e", b"f");
        c.insert(b"g", b"h");
        c.insert(b"d\x00", b"g\x00");
        assert_eq!(all(&c), vec![r(b"a", b"d"), r(b"d\x00", b"h")]);
        c.insert(b"", b"\xff\xff");
        assert_eq!(all(&c), vec![r(b"", b"\xff\xff")]);
    }

    /// The map against a brute-force set of keys over a small alphabet.
    #[test]
    fn random_inserts_match_a_key_set() {
        let mut seed = 0x1234_5678_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..200 {
            let mut c = ReadConflicts::new();
            let mut covered = [false; 16];
            for _ in 0..8 {
                let a = (next() % 16) as u8;
                let b = (next() % 17) as u8;
                c.insert(&[a], &[b]);
                for k in a..b.min(16) {
                    covered[k as usize] = true;
                }
                let ranges = all(&c);
                for w in ranges.windows(2) {
                    assert!(w[0].end < w[1].begin, "{ranges:?}");
                }
                for (k, cov) in covered.iter().enumerate() {
                    let key = [k as u8];
                    let inside = ranges
                        .iter()
                        .any(|r| r.begin.as_slice() <= &key[..] && &key[..] < r.end.as_slice());
                    assert_eq!(inside, *cov, "{ranges:?} {k}");
                }
            }
        }
    }
}
