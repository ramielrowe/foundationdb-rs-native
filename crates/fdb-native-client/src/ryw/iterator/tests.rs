//! `RYWIterator` tests: `testSnapshotCache`'s data (`RYWIterator.cpp` 7.4:232-283, which only
//! prints the segments) with the segments worked out from the type map, and the unreadable
//! protection.

use fdb_native_schema::domain::MutationType as M;
use fdb_native_wire::KeyValue;

use super::*;

fn kv(k: &[u8], v: &[u8]) -> KeyValue {
    KeyValue::new(k.to_vec(), v.to_vec())
}

/// `testSnapshotCache`'s cache and writes.
fn data() -> (SnapshotCache, WriteMap) {
    let mut cache = SnapshotCache::new();
    cache.insert_range(
        b"d",
        b"f\x00",
        vec![
            kv(b"d", b"doo"),
            kv(b"e", b"eoo"),
            kv(b"e\x00", b"zoo"),
            kv(b"f", b"foo"),
        ],
    );
    cache.insert_range(b"g", b"h", vec![]);
    cache.insert_range(b"j", b"m", vec![kv(b"k", b"koo"), kv(b"l", b"loo")]);

    let mut writes = WriteMap::new();
    writes.mutate(b"c", M::SET_VALUE, b"c--", true).unwrap();
    writes.clear(b"c\x00", b"e", true);
    writes
        .mutate(b"c\x00", M::SET_VALUE, b"c00--", true)
        .unwrap();
    writes.mutate(b"d", M::SET_VALUE, b"d--", true).unwrap();
    writes.mutate(b"e", M::SET_VALUE, b"e++", true).unwrap();
    writes.mutate(b"i", M::SET_VALUE, b"i--", true).unwrap();
    (cache, writes)
}

type Seg = (Vec<u8>, Vec<u8>, RywSegment, Option<Vec<u8>>);

fn seg(b: &[u8], e: &[u8], t: RywSegment, v: Option<&[u8]>) -> Seg {
    (b.to_vec(), e.to_vec(), t, v.map(<[u8]>::to_vec))
}

fn here(it: &RywIter<'_>) -> Seg {
    let t = it.segment_type().unwrap();
    let v = if t == SegmentType::KeyValue {
        it.kv().unwrap().map(|kv| kv.value)
    } else {
        None
    };
    (it.begin_key().to_vec(), it.end_key().to_vec(), t, v)
}

fn expected() -> Vec<Seg> {
    use SegmentType::{Empty, KeyValue as Kv, Unknown};
    vec![
        seg(b"", b"c", Unknown, None),
        seg(b"c", b"c\x00", Kv, Some(b"c--")),
        seg(b"c\x00", b"c\x00\x00", Kv, Some(b"c00--")),
        seg(b"c\x00\x00", b"d", Empty, None),
        seg(b"d", b"d\x00", Kv, Some(b"d--")),
        seg(b"d\x00", b"e", Empty, None),
        seg(b"e", b"e\x00", Kv, Some(b"e++")),
        seg(b"e\x00", b"e\x00\x00", Kv, Some(b"zoo")),
        seg(b"e\x00\x00", b"f", Empty, None),
        seg(b"f", b"f\x00", Kv, Some(b"foo")),
        seg(b"f\x00", b"g", Unknown, None),
        seg(b"g", b"h", Empty, None),
        seg(b"h", b"i", Unknown, None),
        seg(b"i", b"i\x00", Kv, Some(b"i--")),
        seg(b"i\x00", b"j", Unknown, None),
        seg(b"j", b"k", Empty, None),
        seg(b"k", b"k\x00", Kv, Some(b"koo")),
        seg(b"k\x00", b"l", Empty, None),
        seg(b"l", b"l\x00", Kv, Some(b"loo")),
        seg(b"l\x00", b"m", Empty, None),
        seg(b"m", b"\xff\xff", Unknown, None),
    ]
}

#[test]
fn walks_test_snapshot_cache_forward_and_back() {
    let (cache, writes) = data();
    let mut it = RywIter::new(&cache, writes.iter());
    it.skip(b"a");
    let mut forward = vec![here(&it)];
    while it.end_key() < ExtKey::from(&b"z"[..]) {
        it.advance();
        forward.push(here(&it));
    }
    assert_eq!(forward, expected());

    it.skip(b"z");
    let mut back = vec![here(&it)];
    while it.begin_key() > ExtKey::from(&b"a"[..]) {
        it.retreat();
        back.push(here(&it));
    }
    back.reverse();
    assert_eq!(back, expected());

    // Every key in a segment skips to it.
    for (b, e, t, _) in expected() {
        it.skip(&b);
        assert_eq!(it.begin_key().to_vec(), b);
        assert_eq!(it.end_key().to_vec(), e);
        assert_eq!(it.segment_type().unwrap(), t);
    }
    // The last segment, and `++` from it to `\xff\xff\x00`.
    it.skip(b"\xff\xff");
    assert_eq!(it.segment_type().unwrap(), SegmentType::Empty);
    assert_eq!(it.end_key().to_vec(), b"\xff\xff\x00");
}

#[test]
fn contiguous_runs_stop_at_writes() {
    let (cache, writes) = data();
    let mut it = RywIter::new(&cache, writes.iter());
    // From `e\x00` (unmodified, cached), the run of the cache's entry up to `f`.
    it.skip(b"e\x00");
    let rows = it.take_contiguous(b"z").unwrap();
    assert_eq!(rows, vec![kv(b"e\x00", b"zoo"), kv(b"f", b"foo")]);
    assert_eq!(it.begin_key().to_vec(), b"f");
    // An operation is a run of one.
    it.skip(b"e");
    assert_eq!(it.take_contiguous(b"z").unwrap(), vec![kv(b"e", b"e++")]);
    assert_eq!(it.begin_key().to_vec(), b"e");
    // Backward from `l`: down to `k`, not past the write map segment's begin + `\x00`.
    it.skip(b"l");
    let rows = it.take_contiguous_back(b"").unwrap();
    assert_eq!(rows, vec![kv(b"k", b"koo"), kv(b"l", b"loo")]);
    assert_eq!(it.begin_key().to_vec(), b"k");
    // From `f` back: `e\x00` is in the cache's entry, but the write map segment begins at
    // `e\x00` (after `e`'s operation), so the run stops at `e\x00\x00` (C++'s
    // `writes.beginKey().keyAfter()`).
    it.skip(b"f");
    let rows = it.take_contiguous_back(b"").unwrap();
    assert_eq!(rows, vec![kv(b"f", b"foo")]);
}

#[test]
fn dependent_writes_need_the_database_value() {
    let mut cache = SnapshotCache::new();
    let mut writes = WriteMap::new();
    writes.mutate(b"a", M::ADD_VALUE, &[1, 0], true).unwrap();
    writes
        .mutate(b"b", M::COMPARE_AND_CLEAR, b"x", true)
        .unwrap();
    {
        let mut it = RywIter::new(&cache, writes.iter());
        it.skip(b"a");
        assert!(it.is_dependent());
        assert_eq!(it.segment_type().unwrap(), SegmentType::Unknown);
    }
    cache.insert_key(b"a", Some(&[5, 0]));
    cache.insert_key(b"b", Some(b"x"));
    let mut it = RywIter::new(&cache, writes.iter());
    it.skip(b"a");
    assert_eq!(it.kv().unwrap(), Some(kv(b"a", &[6, 0])));
    // `CompareAndClear` of the value: the key is gone.
    it.skip(b"b");
    assert!(it.is_kv().unwrap());
    assert_eq!(it.kv().unwrap(), None);
    drop(it);
    // Known absent: the operation applies to no value.
    let mut cache = SnapshotCache::new();
    cache.insert_key(b"a", None);
    let mut it = RywIter::new(&cache, writes.iter());
    it.skip(b"a");
    assert_eq!(it.kv().unwrap(), Some(kv(b"a", &[1, 0])));
}

#[test]
fn unreadable_keys_fail_unless_bypassed() {
    let cache = SnapshotCache::new();
    let mut writes = WriteMap::new();
    writes
        .mutate(
            b"v",
            M::SET_VERSIONSTAMPED_VALUE,
            b"0123456789\x00\x00\x00\x00",
            true,
        )
        .unwrap();
    writes.add_unmodified_and_unreadable_range(b"k", b"l");
    let mut it = RywIter::new(&cache, writes.iter());
    it.skip(b"v");
    assert!(it.is_unreadable());
    assert_eq!(it.segment_type().unwrap_err().code(), ACCESSED_UNREADABLE);
    assert_eq!(it.kv().unwrap_err().code(), ACCESSED_UNREADABLE);
    it.skip(b"k\x01");
    assert_eq!(it.is_kv().unwrap_err().code(), ACCESSED_UNREADABLE);
    it.skip(b"a");
    assert!(!it.is_unreadable());
    assert_eq!(it.segment_type().unwrap(), SegmentType::Unknown);

    it.bypass_unreadable_protection();
    it.skip(b"v");
    assert_eq!(
        it.kv().unwrap(),
        Some(kv(b"v", b"0123456789\x00\x00\x00\x00"))
    );
    it.skip(b"k\x01");
    assert_eq!(it.segment_type().unwrap(), SegmentType::Unknown);
}

#[test]
fn without_writes_is_the_cache() {
    let (cache, _) = data();
    let mut it = RywIter::without_writes(&cache);
    it.skip(b"d");
    assert_eq!(it.kv().unwrap(), Some(kv(b"d", b"doo")));
    it.skip(b"c");
    assert_eq!(it.segment_type().unwrap(), SegmentType::Unknown);
    assert_eq!(it.end_key().to_vec(), b"d");
}
