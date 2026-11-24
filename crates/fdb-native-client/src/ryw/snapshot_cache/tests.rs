//! Tests for the snapshot cache: ports of the C++ `testESR`, `testSnapshotCache` and (8.0)
//! `/fdbclient/ExtStringRef/materialization`, then a randomized model test and a no-panic
//! property test.

use std::borrow::Cow;
use std::collections::BTreeMap;

use proptest::prelude::*;

use super::*;

type Seg = (SegmentType, Vec<u8>, Vec<u8>);

fn kv(k: &[u8], v: &[u8]) -> KeyValue {
    KeyValue::new(k.to_vec(), v.to_vec())
}

/// All segments of the cache, forward from the first.
fn forward(cache: &SnapshotCache) -> Vec<Seg> {
    let mut it = cache.segment_at(b"");
    let mut out = vec![(
        it.segment_type(),
        it.begin_key().to_vec(),
        it.end_key().to_vec(),
    )];
    while it.advance() {
        out.push((
            it.segment_type(),
            it.begin_key().to_vec(),
            it.end_key().to_vec(),
        ));
    }
    out
}

/// All segments of the cache, backward from the last, returned in key order.
fn backward(cache: &SnapshotCache) -> Vec<Seg> {
    let mut it = cache.segment_at(ALL_KEYS_END);
    let mut out = vec![(
        it.segment_type(),
        it.begin_key().to_vec(),
        it.end_key().to_vec(),
    )];
    while it.retreat() {
        out.push((
            it.segment_type(),
            it.begin_key().to_vec(),
            it.end_key().to_vec(),
        ));
    }
    out.reverse();
    out
}

/// Merges adjacent unknown segments and adjacent empty segments (the cache does not coalesce
/// touching known ranges, but the model does not know where they touch).
fn normalize(segs: Vec<Seg>) -> Vec<Seg> {
    let mut out: Vec<Seg> = Vec::new();
    for s in segs {
        match out.last_mut() {
            Some(last) if last.0 == s.0 && s.0 != SegmentType::KeyValue => last.2 = s.2,
            _ => out.push(s),
        }
    }
    out
}

fn seg(t: SegmentType, b: &[u8], e: &[u8]) -> Seg {
    (t, b.to_vec(), e.to_vec())
}

use SegmentType::{Empty as E, KeyValue as V, Unknown as U};

// ---- ExtKey (C++ `ExtStringRef`) ----

/// C++ `testESR`: `ExtStringRef` behaves as the byte strings it stands for.
#[test]
fn ext_key_matches_materialized_bytes() {
    let chars = [0u8, 1, 127, 128, 255];
    let mut bases: Vec<Vec<u8>> = vec![vec![]];
    for &i in &chars {
        bases.push(vec![i]);
        for &j in &chars {
            bases.push(vec![i, j]);
            for &k in &chars {
                bases.push(vec![i, j, k]);
            }
        }
    }
    let mut exts = Vec::new();
    for extra in 0..3 {
        for b in &bases {
            let mut full = b.clone();
            full.resize(b.len() + extra, 0);
            exts.push((ExtKey::new(b, extra), full));
        }
    }
    for (a, abytes) in &exts {
        assert_eq!(a.to_vec(), *abytes);
        assert_eq!(a.size(), abytes.len());
        for (b, bbytes) in &exts {
            assert_eq!(a.cmp(b), abytes.cmp(bbytes), "{abytes:?} cmp {bbytes:?}");
            assert_eq!(a == b, abytes == bbytes);
            assert_eq!(
                a.starts_with(b),
                abytes.starts_with(bbytes),
                "{abytes:?} starts {bbytes:?}"
            );
            let is_key_after = abytes.len() == bbytes.len() + 1
                && abytes.starts_with(bbytes)
                && abytes.last() == Some(&0);
            assert_eq!(
                a.is_key_after(b),
                is_key_after,
                "{abytes:?} keyafter {bbytes:?}"
            );
        }
        assert_eq!(a.key_after().to_vec(), [abytes.as_slice(), &[0]].concat());
    }
}

/// 8.0's `/fdbclient/ExtStringRef/materialization`.
#[test]
fn ext_key_materialization() {
    let cases: [(&[u8], usize, &[u8]); 5] = [
        (b"", 0, b""),
        (b"", 3, b"\x00\x00\x00"),
        (b"a\x00z", 0, b"a\x00z"),
        (b"a\x00z", 1, b"a\x00z\x00"),
        (b"a\x00z", 3, b"a\x00z\x00\x00\x00"),
    ];
    for (base, padding, expected) in cases {
        let ext = ExtKey::new(base, padding);
        assert_eq!(ext.to_vec(), expected);
        let cow = ext.to_cow();
        assert_eq!(&*cow, expected);
        // No padding: borrowed, not copied.
        assert_eq!(matches!(cow, Cow::Borrowed(_)), padding == 0);
        if padding == 0 {
            assert_eq!(ext.assert_ref(), expected);
        }
    }
}

#[test]
#[should_panic(expected = "extra zero bytes")]
fn ext_key_assert_ref_panics_with_padding() {
    ExtKey::new(b"a", 1).assert_ref();
}

// ---- SnapshotCache ----

/// C++ `testSnapshotCache` (which only prints the segments): the same inserts, with the
/// segments asserted.
#[test]
fn lookup_of_a_new_cache_and_after_inserts() {
    for first in [true, false] {
        let mut cache = SnapshotCache::new();
        assert_eq!(cache.lookup(b"a"), None);
        assert_eq!(cache.lookup(b"\xff\xfe"), None);
        // The final segment is always known empty.
        assert_eq!(cache.lookup(b"\xff\xff"), Some(None));
        if first {
            assert!(cache.insert_key(b"a", Some(b"1")));
            assert!(!cache.insert_key(b"a", Some(b"2")));
        } else {
            assert!(cache.insert_range(b"a", b"c", vec![kv(b"a", b"1")]));
        }
        assert_eq!(
            cache.lookup(b"a").map(|kv| kv.map(|kv| kv.value)),
            Some(Some(&b"1"[..]))
        );
        assert_eq!(cache.lookup(b"a\0"), if first { None } else { Some(None) });
        assert_eq!(cache.lookup(b"d"), None);
        assert!(!cache.insert_key(b"\xff\xff", None));
        cache.assert_invariants();
    }
}

#[test]
fn cpp_test_snapshot_cache_segments() {
    let mut cache = SnapshotCache::new();
    assert!(cache.is_empty());
    let rows = [
        kv(b"d", b"doo"),
        kv(b"e", b"eoo"),
        kv(b"e\0", b"zoo"),
        kv(b"f", b"foo"),
    ];
    assert!(cache.insert_range(b"d", b"f\0", rows.to_vec()));
    assert!(cache.insert_range(b"g", b"h", vec![]));
    assert!(cache.insert_range(b"j", b"m", vec![kv(b"k", b"koo"), kv(b"l", b"loo")]));
    assert!(!cache.is_empty());
    cache.assert_invariants();

    let expected = vec![
        seg(U, b"", b"d"),
        seg(V, b"d", b"d\0"),
        seg(E, b"d\0", b"e"),
        seg(V, b"e", b"e\0"),
        seg(V, b"e\0", b"e\0\0"),
        seg(E, b"e\0\0", b"f"),
        seg(V, b"f", b"f\0"),
        seg(U, b"f\0", b"g"),
        seg(E, b"g", b"h"),
        seg(U, b"h", b"j"),
        seg(E, b"j", b"k"),
        seg(V, b"k", b"k\0"),
        seg(E, b"k\0", b"l"),
        seg(V, b"l", b"l\0"),
        seg(E, b"l\0", b"m"),
        seg(U, b"m", b"\xff\xff"),
        seg(E, b"\xff\xff", b"\xff\xff\0"),
    ];
    assert_eq!(forward(&cache), expected);
    assert_eq!(backward(&cache), expected);

    // `skip` lands on the segment containing the key, and the values are right.
    let it = cache.segment_at(b"e\0");
    assert_eq!(it.kv(), Some(kv(b"e\0", b"zoo").borrowed()));
    let it = cache.segment_at(b"e\0\0");
    assert!(it.is_empty_range());
    assert_eq!(it.begin_key().to_vec(), b"e\0\0");
    let it = cache.segment_at(b"x");
    assert!(it.is_unknown_range());
    assert_eq!(it.begin_key().to_vec(), b"m");
    assert_eq!(it.end_key().to_vec(), b"\xff\xff");
    // Anything at or past the end of the key space is the last segment.
    for k in [&b"\xff\xff"[..], b"\xff\xff\0", b"\xff\xff\xff"] {
        let it = cache.segment_at(k);
        assert!(it.is_empty_range());
        assert_eq!(it.end_key().to_vec(), b"\xff\xff\0");
    }
}

#[test]
fn fresh_cache_is_one_unknown_range() {
    let cache = SnapshotCache::default();
    assert!(cache.is_empty());
    assert_eq!(
        forward(&cache),
        vec![seg(U, b"", b"\xff\xff"), seg(E, b"\xff\xff", b"\xff\xff\0")]
    );
    // Moving off either end stays put.
    let mut it = cache.segment_at(b"");
    assert!(!it.retreat());
    assert!(it.is_unknown_range());
    let mut it = cache.segment_at(ALL_KEYS_END);
    assert!(!it.advance());
    assert_eq!(it.begin_key().to_vec(), ALL_KEYS_END);
}

#[test]
fn insert_key_values_and_absences() {
    let mut cache = SnapshotCache::new();
    assert!(cache.insert_key(b"b", Some(b"B")));
    assert!(cache.insert_key(b"d", None));
    assert!(cache.insert_key(b"c", Some(b"C")));
    // Known keys are not touched, with the same value or not.
    assert!(!cache.insert_key(b"b", Some(b"other")));
    assert!(!cache.insert_key(b"d", Some(b"now present")));
    assert!(!cache.insert_key(b"\xff\xff", None));
    cache.assert_invariants();
    assert!(!cache.is_empty());
    assert_eq!(
        forward(&cache),
        vec![
            seg(U, b"", b"b"),
            seg(V, b"b", b"b\0"),
            seg(U, b"b\0", b"c"),
            seg(V, b"c", b"c\0"),
            seg(U, b"c\0", b"d"),
            seg(E, b"d", b"d\0"),
            seg(U, b"d\0", b"\xff\xff"),
            seg(E, b"\xff\xff", b"\xff\xff\0"),
        ]
    );
    assert_eq!(cache.segment_at(b"b").kv(), Some(kv(b"b", b"B").borrowed()));
}

#[test]
fn insert_range_does_not_change_known_ends() {
    let mut cache = SnapshotCache::new();
    assert!(cache.insert_range(b"c", b"f", vec![kv(b"d", b"D")]));
    assert!(cache.insert_range(b"j", b"m", vec![kv(b"k", b"K")]));
    // Straddles both known ranges and covers the gap: only the unknown parts are new.
    assert!(cache.insert_range(
        b"e",
        b"l",
        vec![
            kv(b"e", b"E"),
            kv(b"g", b"G"),
            kv(b"k", b"K"),
            kv(b"l\0", b"out of range")
        ]
    ));
    cache.assert_invariants();
    assert_eq!(
        normalize(forward(&cache)),
        vec![
            seg(U, b"", b"c"),
            seg(E, b"c", b"d"),
            seg(V, b"d", b"d\0"),
            seg(E, b"d\0", b"g"),
            seg(V, b"g", b"g\0"),
            seg(E, b"g\0", b"k"),
            seg(V, b"k", b"k\0"),
            seg(E, b"k\0", b"m"),
            seg(U, b"m", b"\xff\xff"),
            seg(E, b"\xff\xff", b"\xff\xff\0"),
        ]
    );
    // [e, f) was already known to hold only "d", so the new "e" is dropped: the cache is
    // never rewritten at the ends of the range it overlaps.
    assert!(cache.segment_at(b"e").is_empty_range());
    // A range that starts at a known entry's begin replaces the entries from there on (the
    // data agrees), which is also how touching entries get merged: `[c, f)`, `[f, j)` and
    // `[j, m)` become one entry.
    assert!(cache.insert_range(
        b"c",
        b"m",
        vec![kv(b"d", b"D"), kv(b"g", b"G"), kv(b"k", b"K")]
    ));
    cache.assert_invariants();
    assert_eq!(cache.entries.len(), 3);
    // Inside a single known entry, there is nothing new.
    assert!(!cache.insert_range(b"d", b"e", vec![kv(b"d", b"D")]));
    assert!(!cache.insert_range(b"d", b"m", vec![kv(b"d", b"D")]));
}

#[test]
fn insert_range_replaces_entries_inside() {
    let mut cache = SnapshotCache::new();
    cache.insert_key(b"c", Some(b"C"));
    cache.insert_key(b"e", None);
    cache.insert_key(b"g", Some(b"G"));
    assert!(cache.insert_range(b"b", b"h", vec![kv(b"c", b"C"), kv(b"g", b"G")]));
    cache.assert_invariants();
    assert_eq!(
        forward(&cache),
        vec![
            seg(U, b"", b"b"),
            seg(E, b"b", b"c"),
            seg(V, b"c", b"c\0"),
            seg(E, b"c\0", b"g"),
            seg(V, b"g", b"g\0"),
            seg(E, b"g\0", b"h"),
            seg(U, b"h", b"\xff\xff"),
            seg(E, b"\xff\xff", b"\xff\xff\0"),
        ]
    );
}

#[test]
fn insert_range_edge_cases() {
    let mut cache = SnapshotCache::new();
    // Empty and inverted ranges record nothing.
    assert!(!cache.insert_range(b"b", b"b", vec![]));
    assert!(!cache.insert_range(b"c", b"b", vec![kv(b"b", b"x")]));
    assert!(!cache.insert_range(b"\xff\xff", b"\xff\xff\xff", vec![]));
    assert!(cache.is_empty());
    // From the very start, to the very end (and past it).
    assert!(cache.insert_range(
        b"",
        b"\xff\xff\xff",
        vec![kv(b"", b"empty key"), kv(b"z", b"Z")]
    ));
    cache.assert_invariants();
    assert!(!cache.is_empty());
    assert_eq!(
        forward(&cache),
        vec![
            seg(V, b"", b"\0"),
            seg(E, b"\0", b"z"),
            seg(V, b"z", b"z\0"),
            seg(E, b"z\0", b"\xff\xff"),
            seg(E, b"\xff\xff", b"\xff\xff\0"),
        ]
    );
    assert_eq!(backward(&cache), forward(&cache));

    // A known range to the start of the key space is replaced in place.
    let mut cache = SnapshotCache::new();
    assert!(cache.insert_range(b"", b"m", vec![]));
    assert!(cache.insert_range(b"c", b"p", vec![]));
    cache.assert_invariants();
    assert_eq!(
        normalize(forward(&cache)),
        vec![
            seg(E, b"", b"p"),
            seg(U, b"p", b"\xff\xff"),
            seg(E, b"\xff\xff", b"\xff\xff\0"),
        ]
    );
    // An unknown range at the start is kept when the insert is not at the start.
    let mut cache = SnapshotCache::new();
    assert!(cache.insert_range(b"c", b"p", vec![]));
    assert_eq!(cache.segment_at(b"").end_key().to_vec(), b"c");
    assert!(cache.segment_at(b"").is_unknown_range());
}

#[test]
fn skip_contiguous_and_back() {
    let mut cache = SnapshotCache::new();
    let rows: Vec<KeyValue> = [&b"a"[..], b"b", b"c", b"d", b"e"]
        .iter()
        .map(|k| kv(k, k))
        .collect();
    cache.insert_range(b"a", b"f", rows.to_vec());
    cache.insert_key(b"x", Some(b"X"));

    // Forward: the pairs from here up to (not including) the key.
    let mut it = cache.segment_at(b"b");
    assert_eq!(it.skip_contiguous(ExtKey::from(&b"d"[..])), &rows[1..3]);
    assert_eq!(it.kv(), Some(rows[2].borrowed()));
    let mut it = cache.segment_at(b"b");
    assert_eq!(it.skip_contiguous(ExtKey::new(b"d", 1)), &rows[1..4]);
    assert_eq!(it.kv(), Some(rows[3].borrowed()));
    // Past the entry's last value: stops at the entry's end.
    let mut it = cache.segment_at(b"b");
    assert_eq!(it.skip_contiguous(ExtKey::from(&b"zz"[..])), &rows[1..5]);
    assert_eq!(it.kv(), Some(rows[4].borrowed()));
    // A single pair.
    let mut it = cache.segment_at(b"b");
    assert_eq!(it.skip_contiguous(ExtKey::from(&b"c"[..])), &rows[1..2]);
    assert_eq!(it.kv(), Some(rows[1].borrowed()));
    // Not a key/value: nothing.
    let mut it = cache.segment_at(b"m");
    assert!(it.is_unknown_range());
    assert!(it.skip_contiguous(ExtKey::from(&b"z"[..])).is_empty());
    assert!(it.is_unknown_range());

    // Backward: the pairs from the key (inclusive) up to here.
    let mut it = cache.segment_at(b"d");
    assert_eq!(
        it.skip_contiguous_back(ExtKey::from(&b"b"[..])),
        &rows[1..4]
    );
    assert_eq!(it.kv(), Some(rows[1].borrowed()));
    let mut it = cache.segment_at(b"d");
    assert_eq!(it.skip_contiguous_back(ExtKey::new(b"b", 1)), &rows[2..4]);
    assert_eq!(it.kv(), Some(rows[2].borrowed()));
    let mut it = cache.segment_at(b"d");
    assert_eq!(it.skip_contiguous_back(ExtKey::EMPTY), &rows[0..4]);
    assert_eq!(it.kv(), Some(rows[0].borrowed()));
    let mut it = cache.segment_at(b"d");
    assert_eq!(
        it.skip_contiguous_back(ExtKey::from(&b"d"[..])),
        &rows[3..4]
    );
    // The single key `x` is in its own entry.
    let mut it = cache.segment_at(b"x");
    assert_eq!(it.skip_contiguous_back(ExtKey::EMPTY).len(), 1);
    assert!(it.skip_contiguous_back(ExtKey::EMPTY).len() == 1);
}

#[test]
fn rows_of_replies_stay_in_the_replies() {
    use fdb_native_wire::KeyValueVector;
    let reply = |pairs: &[(&[u8], &[u8])]| {
        SharedKeyValueVector::from(
            KeyValueVector::from_pairs(pairs.iter().map(|&(k, v)| KeyValueRef::new(k, v))).unwrap(),
        )
    };
    let first = reply(&[(b"a", b"1"), (b"b", b"")]);
    let empty = reply(&[]);
    let second = reply(&[(b"c", b"3"), (b"d", b"4")]);
    let rows = CacheRows::from_chunks([&first, &empty, &second]);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows.get(2), KeyValueRef::new(b"c", b"3"));

    let mut cache = SnapshotCache::new();
    assert!(cache.insert_range(b"a", b"z", rows));
    cache.assert_invariants();
    // One buffer per (non-empty) reply, not one per row.
    assert_eq!(cache.buffers.len(), 2);
    let mut it = cache.segment_at(b"a");
    let run = it.skip_contiguous(ExtKey::from(&b"z"[..]));
    let mut out = Vec::new();
    run.append_shared(0..run.len(), false, &mut out);
    assert_eq!(
        out,
        [
            kv(b"a", b"1"),
            kv(b"b", b""),
            kv(b"c", b"3"),
            kv(b"d", b"4")
        ]
    );
    // The rows are slices of the replies, not copies.
    assert!(
        first
            .payload()
            .as_ptr_range()
            .contains(&out[0].value.as_ptr())
    );
    assert!(
        second
            .payload()
            .as_ptr_range()
            .contains(&out[3].key.as_ptr())
    );
    out.clear();
    run.append_shared(1..3, true, &mut out);
    assert_eq!(out, [kv(b"c", b"3"), kv(b"b", b"")]);

    // A range that keeps none of a reply's rows does not keep the reply.
    let mut cache = SnapshotCache::new();
    assert!(cache.insert_range(b"e", b"z", CacheRows::from_chunks([&first])));
    assert!(cache.buffers.is_empty());
}

#[test]
fn iterator_navigation() {
    let mut cache = SnapshotCache::new();
    cache.insert_range(b"b", b"d", vec![kv(b"b", b"1"), kv(b"b\0", b"2")]);
    let mut it = cache.segment_at(b"b\0");
    assert_eq!(it.kv(), Some(kv(b"b\0", b"2").borrowed()));
    let at = it.clone();
    assert!(it.advance());
    assert!(it != at);
    assert!(it.is_empty_range());
    assert!(it.retreat());
    assert!(it == at);
    // Retreating over the degenerate empty range between "b" and "b\0".
    assert!(it.retreat());
    assert_eq!(it.kv(), Some(kv(b"b", b"1").borrowed()));
    assert!(it.retreat());
    assert!(it.is_unknown_range());
    assert!(!it.retreat());
    assert!(it.is_unknown_range());
    assert_eq!(it.begin_key(), ExtKey::EMPTY);
}

// ---- getKnownKeyRange / getKnownKeyRangeBack ----

fn range(
    forward: bool,
    outcome: RangeReadOutcome<'_>,
    begin: (&[u8], i32),
    end: (&[u8], i32),
) -> Option<(Vec<u8>, Vec<u8>)> {
    let (b, e) = (
        SelectorBound {
            key: begin.0,
            offset: begin.1,
        },
        SelectorBound {
            key: end.0,
            offset: end.1,
        },
    );
    if forward {
        known_key_range(&outcome, b, e)
    } else {
        known_key_range_back(&outcome, b, e)
    }
}

fn outcome(rows: &[KeyValue], more: bool) -> RangeReadOutcome<'static> {
    RangeReadOutcome {
        rows: rows.into(),
        more,
        read_to_begin: false,
        read_through_end: false,
        read_through: None,
    }
}

#[test]
fn known_key_range_forward() {
    let rows = [kv(b"c", b""), kv(b"e", b"")];
    let r = |o, b, e| range(true, o, b, e);
    // Complete read: the whole requested range, even past the last row.
    assert_eq!(
        r(outcome(&rows, false), (b"a", 1), (b"z", 1)),
        Some((b"a".to_vec(), b"z".to_vec()))
    );
    assert_eq!(
        r(outcome(&[], false), (b"a", 1), (b"z", 1)),
        Some((b"a".to_vec(), b"z".to_vec()))
    );
    // Cut short: known through the last row only.
    assert_eq!(
        r(outcome(&rows, true), (b"a", 1), (b"z", 1)),
        Some((b"a".to_vec(), b"e\0".to_vec()))
    );
    // An end selector that is not a plain key says nothing about its resolved key.
    assert_eq!(
        r(outcome(&rows, false), (b"a", 1), (b"z", 0)),
        Some((b"a".to_vec(), b"e\0".to_vec()))
    );
    assert_eq!(r(outcome(&[], false), (b"a", 1), (b"z", 0)), None);
    // A begin selector past the key (offset > 1) starts at the first row.
    assert_eq!(
        r(outcome(&rows, false), (b"a", 2), (b"z", 1)),
        Some((b"c".to_vec(), b"z".to_vec()))
    );
    // A row before the begin key (the begin selector resolved earlier): known from the row.
    assert_eq!(
        r(outcome(&rows, true), (b"d", 1), (b"z", 1)),
        Some((b"c".to_vec(), b"e\0".to_vec()))
    );
    // read_through extends the end; read_to_begin / read_through_end go to the key space's ends.
    let mut o = outcome(&rows, true);
    o.read_through = Some(b"m");
    assert_eq!(
        r(o.clone(), (b"a", 1), (b"z", 1)),
        Some((b"a".to_vec(), b"m".to_vec()))
    );
    o.read_through = Some(b"d");
    assert_eq!(
        r(o.clone(), (b"a", 1), (b"z", 1)),
        Some((b"a".to_vec(), b"d".to_vec()))
    );
    let mut o = outcome(&[], true);
    o.read_to_begin = true;
    o.read_through_end = true;
    assert_eq!(
        r(o.clone(), (b"a", 1), (b"z", 1)),
        Some((b"".to_vec(), b"\xff\xff".to_vec()))
    );
    // Empty ranges.
    assert_eq!(r(outcome(&[], false), (b"z", 1), (b"a", 1)), None);
    assert_eq!(r(outcome(&[], true), (b"a", 1), (b"z", 1)), None);
}

#[test]
fn known_key_range_reverse() {
    // A reverse read returns rows in descending order.
    let rows = [kv(b"e", b""), kv(b"c", b"")];
    let r = |o, b, e| range(false, o, b, e);
    assert_eq!(
        r(outcome(&rows, false), (b"a", 1), (b"z", 1)),
        Some((b"a".to_vec(), b"z".to_vec()))
    );
    assert_eq!(
        r(outcome(&[], false), (b"a", 1), (b"z", 1)),
        Some((b"a".to_vec(), b"z".to_vec()))
    );
    // Cut short: known from the last (lowest) row, up to the end.
    assert_eq!(
        r(outcome(&rows, true), (b"a", 1), (b"z", 1)),
        Some((b"c".to_vec(), b"z".to_vec()))
    );
    // A row at or past the end selector's key (the end selector resolved earlier).
    assert_eq!(
        r(outcome(&rows, true), (b"a", 1), (b"d", 1)),
        Some((b"c".to_vec(), b"e\0".to_vec()))
    );
    // An end selector that is not a plain key says nothing about its resolved key.
    assert_eq!(
        r(outcome(&rows, false), (b"a", 1), (b"z", 0)),
        Some((b"a".to_vec(), b"e\0".to_vec()))
    );
    // A begin selector that is not a plain key is no help either.
    assert_eq!(
        r(outcome(&rows, false), (b"a", 2), (b"z", 1)),
        Some((b"c".to_vec(), b"z".to_vec()))
    );
    let mut o = outcome(&rows, true);
    o.read_through = Some(b"b");
    assert_eq!(
        r(o.clone(), (b"a", 1), (b"z", 1)),
        Some((b"b".to_vec(), b"z".to_vec()))
    );
    let mut o = outcome(&[], true);
    o.read_to_begin = true;
    o.read_through_end = true;
    assert_eq!(
        r(o.clone(), (b"a", 1), (b"z", 1)),
        Some((b"".to_vec(), b"\xff\xff".to_vec()))
    );
    assert_eq!(r(outcome(&[], true), (b"a", 1), (b"z", 1)), None);
}

#[test]
fn insert_read_reverses_rows_of_a_reverse_read() {
    let mut cache = SnapshotCache::new();
    let rows = [kv(b"e", b"E"), kv(b"c", b"C")];
    let (b, e) = (
        SelectorBound {
            key: b"a",
            offset: 1,
        },
        SelectorBound {
            key: b"z",
            offset: 1,
        },
    );
    assert!(cache.insert_read(true, outcome(&rows, true), b, e));
    cache.assert_invariants();
    assert_eq!(
        normalize(forward(&cache)),
        vec![
            seg(U, b"", b"c"),
            seg(V, b"c", b"c\0"),
            seg(E, b"c\0", b"e"),
            seg(V, b"e", b"e\0"),
            seg(E, b"e\0", b"z"),
            seg(U, b"z", b"\xff\xff"),
            seg(E, b"\xff\xff", b"\xff\xff\0"),
        ]
    );
    assert!(!cache.insert_read(true, outcome(&[], true), b, e));
}

// ---- Randomized model test ----

/// The alphabet the model's keys are built from: the smallest one that has keys that are a
/// prefix of one another and `key + \x00` boundaries.
const ALPHABET: [u8; 3] = [0, b'a', b'b'];

fn strings_up_to(len: usize) -> Vec<Vec<u8>> {
    let mut all = vec![vec![]];
    let mut layer = vec![vec![]];
    for _ in 0..len {
        layer = layer
            .iter()
            .flat_map(|s| {
                ALPHABET.iter().map(move |&c| {
                    let mut t = s.clone();
                    t.push(c);
                    t
                })
            })
            .collect();
        all.extend(layer.iter().cloned());
    }
    all.sort();
    all
}

/// Every key the cache's operations use is in `keys3`; every segment boundary is in the model's
/// universe `points` (`keys3` and their `+ \x00`, and `\xff\xff` at the end).
struct Model {
    /// The keys the operations use, ascending.
    keys3: Vec<Vec<u8>>,
    /// The boundaries of the model's elementary intervals, ascending, ending with `\xff\xff`.
    points: Vec<Vec<u8>>,
    /// The ground truth of the database at the read version.
    truth: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Whether the elementary interval `[points[i], points[i + 1])` is known.
    known: Vec<bool>,
}

impl Model {
    fn new(truth_mask: &[bool]) -> Model {
        let keys3 = strings_up_to(3);
        let mut points = strings_up_to(4);
        points.push(ALL_KEYS_END.to_vec());
        let truth = keys3
            .iter()
            .zip(truth_mask.iter().cycle())
            .filter(|&(_, &present)| present)
            .map(|(k, _)| (k.clone(), [b"v".as_slice(), k].concat()))
            .collect();
        let known = vec![false; points.len() - 1];
        Model {
            keys3,
            points,
            truth,
            known,
        }
    }

    fn point(&self, key: &[u8]) -> usize {
        self.points
            .binary_search_by(|p| p.as_slice().cmp(key))
            .expect("key in the universe")
    }

    fn mark(&mut self, begin: &[u8], end: &[u8]) {
        for i in self.point(begin)..self.point(end) {
            self.known[i] = true;
        }
    }

    fn rows(&self, begin: &[u8], end: &[u8]) -> Vec<KeyValue> {
        self.truth
            .range::<[u8], _>((Bound::Included(begin), Bound::Excluded(end)))
            .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
            .collect()
    }

    /// The segments the cache must have, with adjacent unknown / empty ones merged.
    fn expected(&self) -> Vec<Seg> {
        let mut segs: Vec<Seg> = Vec::new();
        for i in 0..self.known.len() {
            let (lo, hi) = (&self.points[i], &self.points[i + 1]);
            if !self.known[i] {
                segs.push((U, lo.clone(), hi.clone()));
            } else if self.truth.contains_key(lo) {
                let after = [lo.as_slice(), &[0]].concat();
                segs.push((V, lo.clone(), after.clone()));
                if after < *hi {
                    segs.push((E, after, hi.clone()));
                }
            } else {
                segs.push((E, lo.clone(), hi.clone()));
            }
        }
        segs.push((E, ALL_KEYS_END.to_vec(), AFTER_ALL_KEYS.to_vec()));
        normalize(segs)
    }
}

/// How a model read's selectors resolved and what the storage reply said, beyond the keys.
#[derive(Clone, Debug)]
struct ReadShape {
    /// The begin selector's offset (1 is a plain key; 0 and below resolved at or before the
    /// key, 2 and above after it).
    begin_offset: i32,
    /// The end selector's offset (1 and above resolved at or after its key, 0 and below
    /// before it).
    end_offset: i32,
    /// How many keys away from the selector's key it resolved (in the model's key order).
    begin_shift: usize,
    end_shift: usize,
    /// `read_to_begin`, if the begin selector can have hit the start of the key space.
    read_to_begin: bool,
    /// `read_through_end`, if the end selector can have hit the end.
    read_through_end: bool,
    /// `read_through`: a key (as an index into the model's keys) the read says it got through,
    /// moved to the nearest one that is true.
    read_through: Option<usize>,
}

impl ReadShape {
    /// Two plain `firstGreaterOrEqual(key)` selectors, resolved exactly, and nothing else.
    fn plain() -> ReadShape {
        ReadShape {
            begin_offset: 1,
            end_offset: 1,
            begin_shift: 0,
            end_shift: 0,
            read_to_begin: false,
            read_through_end: false,
            read_through: None,
        }
    }

    fn is_plain(&self) -> bool {
        self.begin_offset == 1
            && self.end_offset == 1
            && self.begin_shift == 0
            && self.end_shift == 0
            && !self.read_to_begin
            && !self.read_through_end
            && self.read_through.is_none()
    }
}

fn shape_strategy() -> impl Strategy<Value = ReadShape> {
    prop_oneof![
        2 => Just(ReadShape::plain()),
        3 => (
            -1..4i32,
            -1..4i32,
            0..4usize,
            0..4usize,
            any::<bool>(),
            any::<bool>(),
            proptest::option::of(0..41usize),
        )
            .prop_map(|(bo, eo, bs, es, to_begin, through_end, rt)| ReadShape {
                begin_offset: bo,
                end_offset: eo,
                begin_shift: bs,
                end_shift: es,
                read_to_begin: to_begin,
                read_through_end: through_end,
                read_through: rt,
            }),
    ]
}

#[derive(Clone, Debug)]
enum Op {
    /// `insert(key, value)` of a single key.
    Key(usize),
    /// `insert(range, rows)`: all the rows in the range.
    Range(usize, usize),
    /// What `getRange` does: a read of `[begin, end)` with a row limit, forward or reverse, and
    /// the cache is given its `getKnownKeyRange`.
    Read {
        reverse: bool,
        begin: usize,
        end: usize,
        limit: usize,
        shape: ReadShape,
    },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    // Indexes into `keys3` (40 keys); the end index 40 is `\xff\xff`.
    prop_oneof![
        (0..40usize).prop_map(Op::Key),
        (0..40usize, 0..41usize).prop_map(|(b, e)| Op::Range(b, e)),
        (
            any::<bool>(),
            0..40usize,
            0..41usize,
            1..6usize,
            shape_strategy()
        )
            .prop_map(|(reverse, begin, end, limit, shape)| Op::Read {
                reverse,
                begin,
                end,
                limit,
                shape,
            }),
    ]
}

fn check_against_model(cache: &SnapshotCache, model: &Model) {
    cache.assert_invariants();
    let fwd = forward(cache);
    // Segments tile the key space, none empty.
    assert_eq!(fwd.first().map(|s| s.1.as_slice()), Some(&b""[..]));
    assert_eq!(fwd.last().map(|s| s.2.as_slice()), Some(AFTER_ALL_KEYS));
    for w in fwd.windows(2) {
        assert_eq!(w[0].2, w[1].1, "segments not contiguous");
    }
    for s in &fwd {
        assert!(s.1 < s.2, "empty segment {s:?}");
        if s.0 == V {
            assert_eq!(s.2, [s.1.as_slice(), &[0]].concat(), "{s:?}");
        }
    }
    assert_eq!(backward(cache), fwd, "reverse iteration differs");
    assert_eq!(normalize(fwd.clone()), model.expected());

    // `skip` finds the segment of every key, and the values are the truth's.
    for p in model.points.iter() {
        let it = cache.segment_at(p);
        let (b, e) = (it.begin_key().to_vec(), it.end_key().to_vec());
        assert!(b <= *p && *p < e, "skip({p:?}) -> [{b:?}, {e:?})");
        assert!(fwd.contains(&(it.segment_type(), b, e)));
        // `lookup` is the segment's answer (also for a cache nothing was inserted into).
        let expected = match it.segment_type() {
            SegmentType::Unknown => None,
            SegmentType::Empty => Some(None),
            SegmentType::KeyValue => Some(it.kv()),
        };
        assert_eq!(cache.lookup(p), expected, "lookup({p:?})");
        match it.kv() {
            Some(row) => assert_eq!(model.truth.get(row.key).map(Vec::as_slice), Some(row.value)),
            None => assert_ne!(it.segment_type(), V),
        }
        // `skip_contiguous` over a run of pairs: ascending, all `< limit`, nothing but empty
        // known ranges between them.
        if let Some(first) = it.kv() {
            for limit in model
                .points
                .iter()
                .filter(|l| l.as_slice() > first.key)
                .take(8)
            {
                let mut fwd_it = it.clone();
                let run: Vec<_> = fwd_it.skip_contiguous(ExtKey::from(limit)).iter().collect();
                assert_eq!(run.first(), Some(&first));
                assert_eq!(fwd_it.kv().as_ref(), run.last());
                for pair in run.windows(2) {
                    assert!(pair[0].key < pair[1].key);
                    let (lo, hi) = (model.point(pair[0].key), model.point(pair[1].key));
                    assert!(
                        model.known[lo..hi].iter().all(|&k| k),
                        "run spans unknown keys"
                    );
                }
                assert!(run.iter().all(|r| r.key < limit.as_slice()));
                let mut back_it = fwd_it.clone();
                let back: Vec<_> = back_it
                    .skip_contiguous_back(ExtKey::from(first.key))
                    .iter()
                    .collect();
                assert_eq!(back, run);
                assert_eq!(back_it.kv(), Some(first));
            }
        }
    }
}

fn run_model(truth_mask: Vec<bool>, ops: Vec<Op>) {
    let mut model = Model::new(&truth_mask);
    let mut cache = SnapshotCache::new();
    assert!(cache.is_empty());
    let bound = |model: &Model, i: usize| -> Vec<u8> {
        model
            .keys3
            .get(i)
            .cloned()
            .unwrap_or_else(|| ALL_KEYS_END.to_vec())
    };
    for op in ops {
        match op {
            Op::Key(k) => {
                let key = model.keys3[k].clone();
                let was_known = model.known[model.point(&key)];
                let value = model.truth.get(&key).cloned();
                assert_eq!(cache.insert_key(&key, value.as_deref()), !was_known);
                model.mark(&key, &[key.as_slice(), &[0]].concat());
            }
            Op::Range(b, e) => {
                let (begin, end) = (bound(&model, b), bound(&model, e));
                if begin >= end {
                    assert!(!cache.insert_range(&begin, &end, vec![]));
                    continue;
                }
                let rows = model.rows(&begin, &end);
                let changed = cache.insert_range(&begin, &end, rows.clone());
                // Recorded unless everything in the range was already known (a range that is
                // partly known is recorded: the entries inside it are replaced).
                let all_known = model.known[model.point(&begin)..model.point(&end)]
                    .iter()
                    .all(|&k| k);
                if !all_known {
                    assert!(changed, "[{begin:?}, {end:?}) not recorded");
                }
                model.mark(&begin, &end);
            }
            Op::Read {
                reverse,
                begin,
                end,
                limit,
                shape,
            } => {
                let at = |i: usize| bound(&model, i);
                let (bk, ek) = (at(begin), at(end));
                let (bo, eo) = (shape.begin_offset, shape.end_offset);
                // What the storage read really covered, `[sb, se)`, given the selectors: a
                // begin selector with offset <= 1 resolved at or before its key (so the read
                // covers from the key on), one with a larger offset at or after it; an end
                // selector with offset >= 1 resolved at or after its key, one with a smaller
                // offset at or before it. `read_to_begin` / `read_through_end` say the
                // resolution went to the end of the key space, so the read covers it (the
                // C++ flags are only set when the selector's resolution hit the end; and a
                // read that stopped at its limit going away from that end cannot claim it).
                let to_begin = shape.read_to_begin && bo <= 0;
                let through_end = shape.read_through_end && eo >= 1;
                let sb = if to_begin {
                    Vec::new()
                } else if bo <= 1 {
                    at(begin.saturating_sub(shape.begin_shift))
                } else {
                    at(begin + shape.begin_shift)
                };
                let se = if through_end {
                    ALL_KEYS_END.to_vec()
                } else if eo >= 1 {
                    at(end + shape.end_shift)
                } else {
                    at(end.saturating_sub(shape.end_shift))
                };
                if sb >= se {
                    continue;
                }
                let all_rows = model.rows(&sb, &se);
                let more = all_rows.len() > limit;
                if (reverse && more && to_begin) || (!reverse && more && through_end) {
                    continue;
                }
                let rows: Vec<KeyValue> = if reverse {
                    all_rows.iter().rev().take(limit).cloned().collect()
                } else {
                    all_rows.iter().take(limit).cloned().collect()
                };
                // `read_through`: how far the read got, a key where nothing between the last
                // row and it was skipped.
                let read_through: Option<Vec<u8>> = shape.read_through.and_then(|idx| {
                    let last = &rows.last()?.key;
                    let (lo, hi) = if reverse {
                        let lo = if more {
                            let dropped = &all_rows[all_rows.len() - 1 - limit].key;
                            [&dropped[..], &[0]].concat()
                        } else {
                            sb.clone()
                        };
                        (lo, last.to_vec())
                    } else {
                        let hi = if more {
                            all_rows[limit].key.to_vec()
                        } else {
                            se.clone()
                        };
                        ([&last[..], &[0]].concat(), hi)
                    };
                    Some(at(idx).clamp(lo, hi))
                });
                let (b, e) = (
                    SelectorBound {
                        key: &bk,
                        offset: bo,
                    },
                    SelectorBound {
                        key: &ek,
                        offset: eo,
                    },
                );
                let out = RangeReadOutcome {
                    rows: rows.clone().into(),
                    more,
                    read_to_begin: to_begin,
                    read_through_end: through_end,
                    read_through: read_through.as_deref(),
                };
                let range = if reverse {
                    known_key_range_back(&out, b, e)
                } else {
                    known_key_range(&out, b, e)
                };
                let desc = format!(
                    "{shape:?} reverse={reverse} limit={limit} bk={bk:?} ek={ek:?} \
                     scan=[{sb:?}, {se:?}) rows={rows:?} more={more}"
                );
                if shape.is_plain() {
                    // The plain read: what `getKnownKeyRange` is described to give.
                    let known = if reverse {
                        // Known from the lowest row returned, if cut short.
                        let from = if more {
                            rows.last().expect("limit >= 1").key.to_vec()
                        } else {
                            bk.clone()
                        };
                        (from, ek.clone())
                    } else {
                        // Known up to just past the last row returned, if cut short.
                        let to = if more {
                            [&rows.last().expect("limit >= 1").key[..], &[0]].concat()
                        } else {
                            ek.clone()
                        };
                        (bk.clone(), to)
                    };
                    assert_eq!(range.as_ref(), Some(&known), "known range of the read");
                }
                let Some((kb, ke)) = range else {
                    assert!(!cache.insert_read(reverse, out, b, e), "{desc}");
                    continue;
                };
                // Sound: only what the read covered is claimed, and the rows it returned
                // are all the keys there are in the claim.
                assert!(sb <= kb && ke <= se, "{desc}: claims [{kb:?}, {ke:?})");
                let mut seen = rows.clone();
                seen.retain(|r| kb <= r.key && r.key < ke);
                seen.sort_by(|a, b| a.key.cmp(&b.key));
                assert_eq!(
                    seen,
                    model.rows(&kb, &ke),
                    "{desc}: claims [{kb:?}, {ke:?})"
                );
                // Not too timid: the rows are in the claim, and the ends the selectors and
                // flags describe are known.
                let first = rows.first().map(|r| r.key.to_vec());
                let last = rows.last().map(|r| r.key.to_vec());
                let after = |k: &Option<Vec<u8>>| k.as_ref().map(|k| [k.as_slice(), &[0]].concat());
                let (lowest, highest) = if reverse {
                    (&last, &first)
                } else {
                    (&first, &last)
                };
                if let (Some(lowest), Some(highest)) = (lowest, after(highest)) {
                    assert!(
                        kb <= *lowest && ke >= highest,
                        "{desc}: claims [{kb:?}, {ke:?})"
                    );
                }
                if to_begin {
                    assert_eq!(kb, b"", "{desc}");
                }
                if through_end {
                    assert_eq!(ke, ALL_KEYS_END, "{desc}");
                }
                if !rows.is_empty() {
                    if reverse {
                        if bo <= 1 && !more {
                            assert!(kb <= bk, "{desc}");
                        }
                        if eo >= 1 {
                            assert!(ke >= ek, "{desc}");
                        } else if !through_end {
                            assert_eq!(Some(&ke), after(&first).as_ref(), "{desc}");
                        }
                        if read_through.is_none() && !to_begin && (more || bo > 1) {
                            assert_eq!(Some(&kb), last.as_ref(), "{desc}");
                        }
                        if let (true, Some(rt)) = (more, &read_through) {
                            assert_eq!(&kb, rt, "{desc}");
                        }
                    } else {
                        if eo >= 1 && !more {
                            assert!(ke >= ek, "{desc}");
                        }
                        if bo <= 1 {
                            assert!(kb <= bk, "{desc}");
                        } else {
                            assert_eq!(Some(&kb), first.as_ref(), "{desc}");
                        }
                        if more && read_through.is_none() {
                            assert_eq!(Some(&ke), after(&last).as_ref(), "{desc}");
                        }
                        if let (true, Some(rt)) = (more, &read_through) {
                            assert_eq!(&ke, rt, "{desc}");
                        }
                    }
                }
                cache.insert_read(reverse, out, b, e);
                model.mark(&kb, &ke);
            }
        }
        check_against_model(&cache, &model);
        assert_eq!(cache.is_empty(), model.known.iter().all(|&k| !k));
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 400, ..ProptestConfig::default() })]

    /// After random inserts the cache's segments, forward and reverse, are what the model says.
    #[test]
    fn matches_model(
        truth_mask in proptest::collection::vec(any::<bool>(), 1..12),
        ops in proptest::collection::vec(op_strategy(), 1..24),
    ) {
        run_model(truth_mask, ops);
    }
}

#[test]
fn model_smoke_deterministic() {
    // A fixed run through every kind of op, so a failure here is easy to debug.
    let ops = vec![
        Op::Key(5),
        Op::Range(3, 9),
        Op::Read {
            reverse: false,
            begin: 1,
            end: 30,
            limit: 2,
            shape: ReadShape::plain(),
        },
        Op::Read {
            reverse: true,
            begin: 1,
            end: 30,
            limit: 3,
            shape: ReadShape::plain(),
        },
        Op::Range(0, 40),
        Op::Key(7),
    ];
    run_model(vec![true, false, true], ops);
}

#[test]
fn model_smoke_selector_offsets_and_flags() {
    // Reads whose selectors did not resolve at their keys, and that reached the ends of the
    // key space or say how far they got.
    let shape = |bo, eo, bs, es, to_begin, through_end, rt| ReadShape {
        begin_offset: bo,
        end_offset: eo,
        begin_shift: bs,
        end_shift: es,
        read_to_begin: to_begin,
        read_through_end: through_end,
        read_through: rt,
    };
    let read = |reverse, begin, end, limit, shape| Op::Read {
        reverse,
        begin,
        end,
        limit,
        shape,
    };
    let ops = vec![
        // Offsets past the key: known from the first row only.
        read(false, 4, 30, 3, shape(2, 1, 1, 0, false, false, None)),
        read(true, 8, 33, 3, shape(3, 1, 2, 1, false, false, None)),
        // Offsets before the key and an end selector that resolved earlier.
        read(false, 12, 36, 2, shape(0, 0, 1, 2, false, false, None)),
        read(true, 15, 36, 4, shape(-1, 2, 1, 1, false, false, None)),
        // Through the ends of the key space.
        read(false, 20, 30, 5, shape(0, 1, 0, 0, true, true, None)),
        read(true, 20, 30, 5, shape(0, 1, 0, 0, true, true, None)),
        // `read_through` on cut-short reads, both directions.
        read(false, 2, 38, 2, shape(1, 1, 0, 0, false, false, Some(25))),
        read(true, 2, 38, 2, shape(1, 1, 0, 0, false, false, Some(6))),
        read(false, 6, 40, 1, shape(1, 1, 0, 0, false, false, Some(40))),
        read(true, 6, 40, 1, shape(1, 1, 0, 0, false, false, Some(0))),
    ];
    for mask in [
        vec![true],
        vec![true, false, true],
        vec![false, true, true, false],
    ] {
        run_model(mask, ops.clone());
    }
}

// ---- No panics ----

#[derive(Clone, Debug)]
enum Wild {
    Key(Vec<u8>, Option<Vec<u8>>),
    Range(Vec<u8>, Vec<u8>, Vec<Vec<u8>>),
    Read {
        reverse: bool,
        keys: Vec<Vec<u8>>,
        begin: (Vec<u8>, i32),
        end: (Vec<u8>, i32),
        flags: (bool, bool, bool, Option<Vec<u8>>),
    },
    Nav(Vec<u8>, Vec<u8>, Vec<bool>),
}

fn wild_key() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(
        prop_oneof![Just(0u8), Just(1), Just(b'a'), Just(0xfe), Just(0xff)],
        0..4,
    )
}

fn wild_op() -> impl Strategy<Value = Wild> {
    let keys = || proptest::collection::vec(wild_key(), 0..6);
    prop_oneof![
        (wild_key(), proptest::option::of(wild_key())).prop_map(|(k, v)| Wild::Key(k, v)),
        (wild_key(), wild_key(), keys()).prop_map(|(b, e, r)| Wild::Range(b, e, r)),
        (
            any::<bool>(),
            keys(),
            (wild_key(), -3..4i32),
            (wild_key(), -3..4i32),
            (
                any::<bool>(),
                any::<bool>(),
                any::<bool>(),
                proptest::option::of(wild_key())
            ),
        )
            .prop_map(|(reverse, keys, begin, end, flags)| Wild::Read {
                reverse,
                keys,
                begin,
                end,
                flags
            }),
        (
            wild_key(),
            wild_key(),
            proptest::collection::vec(any::<bool>(), 0..12)
        )
            .prop_map(|(a, b, c)| Wild::Nav(a, b, c)),
    ]
}

fn sorted_rows(mut keys: Vec<Vec<u8>>, descending: bool) -> Vec<KeyValue> {
    keys.sort();
    keys.dedup();
    if descending {
        keys.reverse();
    }
    keys.into_iter()
        .map(|k| KeyValue::new(k.clone(), k))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 500, ..ProptestConfig::default() })]

    /// No sequence of inserts (with arbitrary, even inconsistent, keys, ranges and read
    /// outcomes) and moves of an iterator panics or breaks the cache's structure.
    #[test]
    fn arbitrary_use_does_not_panic(ops in proptest::collection::vec(wild_op(), 1..30)) {
        let mut cache = SnapshotCache::new();
        for op in ops {
            match op {
                Wild::Key(k, v) => {
                    cache.insert_key(&k, v.as_deref());
                }
                Wild::Range(b, e, rows) => {
                    cache.insert_range(&b, &e, sorted_rows(rows, false));
                }
                Wild::Read { reverse, keys, begin, end, flags } => {
                    let rows = sorted_rows(keys, reverse);
                    let out = RangeReadOutcome {
                        rows: rows.into(),
                        more: flags.0,
                        read_to_begin: flags.1,
                        read_through_end: flags.2,
                        read_through: flags.3.as_deref(),
                    };
                    cache.insert_read(
                        reverse,
                        out,
                        SelectorBound { key: &begin.0, offset: begin.1 },
                        SelectorBound { key: &end.0, offset: end.1 },
                    );
                }
                Wild::Nav(start, limit, moves) => {
                    let mut it = cache.segment_at(&start);
                    let (b, e) = (it.begin_key(), it.end_key());
                    if start.as_slice() < ALL_KEYS_END {
                        assert!(b <= ExtKey::from(&start) && ExtKey::from(&start) < e);
                    }
                    for forward in moves {
                        if forward {
                            let _ = it.advance();
                            it.skip_contiguous(ExtKey::from(&limit));
                        } else {
                            let _ = it.retreat();
                            it.skip_contiguous_back(ExtKey::from(&limit));
                        }
                        let _ = (it.begin_key(), it.end_key(), it.kv());
                    }
                }
            }
            cache.assert_invariants();
            let fwd = forward(&cache);
            assert_eq!(backward(&cache), fwd);
        }
    }
}
