//! Tests of the write map: the `/fdbclient/WriteMap/*` unit tests of `RYWIterator.cpp` (7.4
//! 424-822, 8.0 469-867), a randomized comparison with a naive model (every operation kept,
//! values computed by applying them one by one), and a property test.

use std::collections::BTreeMap;
use std::sync::Arc;

use fdb_native_schema::domain::MutationType as M;
use proptest::prelude::*;

use super::*;

/// One segment, as an iterator sees it.
#[derive(Clone, Debug, PartialEq)]
struct Seg {
    begin: Vec<u8>,
    end: Vec<u8>,
    kind: WriteSegmentType,
    conflict: bool,
    unreadable: bool,
    cleared: bool,
    unmodified: bool,
    independent: Option<bool>,
    op: Option<OperationStack>,
}

fn seg(it: &WriteMapIter) -> Seg {
    Seg {
        begin: it.begin_key().to_vec(),
        end: it.end_key().to_vec(),
        kind: it.segment_type(),
        conflict: it.is_conflict_range(),
        unreadable: it.is_unreadable(),
        cleared: it.is_cleared_range(),
        unmodified: it.is_unmodified_range(),
        independent: it.is_independent(),
        op: it.op().cloned(),
    }
}

/// The segments from `""` to `\xff\xff`, walking forward.
fn segments(wm: &WriteMap) -> Vec<Seg> {
    let mut out = Vec::new();
    let mut it = wm.iter();
    while it.begin_key() < ExtKey::from(ALL_KEYS_END) {
        out.push(seg(&it));
        assert!(it.advance());
    }
    out
}

/// The same, walking backward from the last segment.
fn segments_back(wm: &WriteMap) -> Vec<Seg> {
    let mut out = Vec::new();
    let mut it = wm.iter_at(ALL_KEYS_END);
    assert_eq!(it.begin_key().to_vec(), ALL_KEYS_END);
    assert!(
        !it.clone().advance(),
        "[\\xff\\xff, \\xff\\xff\\x00) is the last segment"
    );
    while it.retreat() {
        out.push(seg(&it));
    }
    assert_eq!(it.begin_key().to_vec(), b"");
    out.reverse();
    out
}

/// `getWriteMapCount`.
fn count(wm: &WriteMap) -> usize {
    segments(wm).len()
}

/// Structural checks: the segments tile `["", \xff\xff)`, every segment is non-empty, an
/// operation is one key, the walk is the same both ways, and `skip` finds each segment.
fn check_structure(wm: &WriteMap) -> Vec<Seg> {
    let segs = segments(wm);
    assert_eq!(segs.first().map(|s| &s.begin[..]), Some(&b""[..]));
    assert_eq!(segs.last().map(|s| &s.end[..]), Some(ALL_KEYS_END));
    for pair in segs.windows(2) {
        assert_eq!(pair[0].end, pair[1].begin, "{segs:#?}");
    }
    for s in &segs {
        assert!(s.begin < s.end, "{s:?}");
        match s.kind {
            WriteSegmentType::IndependentWrite | WriteSegmentType::DependentWrite => {
                let mut after = s.begin.clone();
                after.push(0);
                assert_eq!(after, s.end);
                assert!(s.op.as_ref().is_some_and(|op| !op.is_empty()));
                assert!(!s.cleared && !s.unmodified);
            }
            WriteSegmentType::ClearedRange => {
                assert!(s.cleared && s.op.is_none());
                assert!(!s.unreadable, "a cleared range is readable: {s:?}");
            }
            WriteSegmentType::UnmodifiedRange => assert!(s.unmodified && s.op.is_none()),
        }
        let at = wm.iter_at(&s.begin);
        assert_eq!(&seg(&at), s);
    }
    assert_eq!(segments_back(wm), segs);
    check_entries(wm);
    segs
}

/// Invariants of the entries themselves (which the segments hide): cleared ranges are readable
/// (`clear` makes its range readable, and making a range unreadable makes it unmodified), a
/// dependent stack has no cleared keys after it (an operation on a cleared key starts from the
/// clear, so is independent), and the sentinels bound the key space.
fn check_entries(wm: &WriteMap) {
    let keys: Vec<&[u8]> = wm.tree.keys().map(|k| &k[..]).collect();
    assert!(
        keys.windows(2).all(|w| w[0] < w[1]),
        "entries are not sorted"
    );
    assert_eq!(keys.first(), Some(&&b""[..]));
    assert_eq!(keys[keys.len() - 2..], [ALL_KEYS_END, AFTER_ALL_KEYS]);
    for (key, entry) in wm.tree.iter() {
        let at = || format!("entry at {key:?}: {entry:?}");
        assert!(
            !(entry.following_keys_cleared && entry.following_keys_unreadable),
            "cleared and unreadable after {}",
            at()
        );
        assert!(
            !(entry.stack.is_dependent() && entry.following_keys_cleared),
            "dependent stack with cleared keys after {}",
            at()
        );
        assert!(
            entry.stack.is_empty() || key[..] < *ALL_KEYS_END,
            "operations at or after \\xff\\xff: {}",
            at()
        );
    }
}

// ---- The C++ unit tests -------------------------------------------------------------------

#[test]
fn emptiness() {
    let mut wm = WriteMap::new();
    assert!(wm.is_empty());
    wm.mutate(b"apple", M::SET_VALUE, b"red", true).unwrap();
    assert!(!wm.is_empty());
}

/// `/fdbclient/WriteMap/VersionstampedvalueAfterSet`: C++ reads the key through an
/// `RYWIterator` over an empty snapshot cache with the unreadable protection bypassed; the
/// write map's part is an unreadable independent write whose value is the versionstamped
/// value's parameter.
#[test]
fn versionstamped_value_after_set() {
    let metadata_version_required_value = [0u8; 14];
    let mut wm = WriteMap::new();
    assert!(wm.is_empty());
    wm.mutate(b"apple", M::SET_VALUE, b"red", true).unwrap();
    wm.mutate(
        b"apple",
        M::SET_VERSIONSTAMPED_VALUE,
        &metadata_version_required_value,
        true,
    )
    .unwrap();

    let it = wm.iter_at(b"apple");
    assert!(it.is_unreadable());
    assert_eq!(it.segment_type(), WriteSegmentType::IndependentWrite);
    assert_eq!(it.begin_key().assert_ref(), b"apple");
    let kv = WriteMap::coalesce_under(it.op().unwrap(), None).unwrap();
    assert_eq!(
        kv.value.as_deref(),
        Some(&metadata_version_required_value[..])
    );
}

#[test]
fn clear() {
    let mut wm = WriteMap::new();
    assert!(wm.is_empty());
    assert_eq!(count(&wm), 1);

    wm.mutate(b"apple", M::SET_VALUE, b"red", true).unwrap();
    assert!(!wm.is_empty());
    assert_eq!(count(&wm), 3);

    wm.clear(b"a", b"j", true);
    assert_eq!(count(&wm), 3);
}

/// One expected segment of the C++ walk tests: begin, end, cleared, conflict, operation,
/// independent (for an operation), unmodified, unreadable.
type Expected<'a> = (&'a [u8], &'a [u8], bool, bool, bool, bool, bool, bool);

fn check_walk(wm: &WriteMap, expected: &[Expected<'_>]) {
    let mut it = wm.iter();
    for &(begin, end, cleared, conflict, operation, independent, unmodified, unreadable) in expected
    {
        assert!(it.begin_key() < ExtKey::from(ALL_KEYS_END));
        assert_eq!(it.begin_key().to_vec(), begin);
        assert_eq!(it.end_key().to_vec(), end);
        assert_eq!(it.is_cleared_range(), cleared);
        assert_eq!(it.is_conflict_range(), conflict);
        assert_eq!(it.is_operation(), operation);
        if operation {
            assert_eq!(it.is_independent(), Some(independent));
        }
        assert_eq!(it.is_unmodified_range(), unmodified);
        assert_eq!(it.is_unreadable(), unreadable);
        let _ = it.advance();
    }
    assert!(it.begin_key() >= ExtKey::from(ALL_KEYS_END));
}

#[test]
fn set_versionstamped_key() {
    let mut wm = WriteMap::new();
    assert!(wm.is_empty());
    assert_eq!(count(&wm), 1);

    let stamp: &[u8] = b"stamp:XXXXXXXX\x06\x00\x00\x00";
    wm.mutate(stamp, M::SET_VERSIONSTAMPED_KEY, b"1", true)
        .unwrap();
    assert!(!wm.is_empty());
    assert_eq!(count(&wm), 3);

    wm.mutate(b"stamp:ZZZZZZZZZZ", M::ADD_VALUE, b"2", true)
        .unwrap();
    assert_eq!(count(&wm), 5);

    check_walk(
        &wm,
        &[
            (b"", stamp, false, false, false, false, true, false),
            (
                stamp,
                b"stamp:XXXXXXXX\x06\x00\x00\x00\x00",
                false,
                true,
                true,
                true,
                false,
                true,
            ),
            (
                b"stamp:XXXXXXXX\x06\x00\x00\x00\x00",
                b"stamp:ZZZZZZZZZZ",
                false,
                false,
                false,
                false,
                true,
                false,
            ),
            (
                b"stamp:ZZZZZZZZZZ",
                b"stamp:ZZZZZZZZZZ\x00",
                false,
                true,
                true,
                false,
                false,
                false,
            ),
            (
                b"stamp:ZZZZZZZZZZ\x00",
                b"\xff\xff",
                false,
                false,
                false,
                false,
                true,
                false,
            ),
        ],
    );
    check_structure(&wm);
}

#[test]
fn set_versionstamped_value() {
    let mut wm = WriteMap::new();
    assert!(wm.is_empty());
    assert_eq!(count(&wm), 1);

    wm.mutate(
        b"stamp",
        M::SET_VERSIONSTAMPED_VALUE,
        b"XXXXXXXX\x00\x00\x00\x00\x00\x00",
        true,
    )
    .unwrap();
    assert!(!wm.is_empty());
    assert_eq!(count(&wm), 3);

    wm.mutate(b"stamp123", M::ADD_VALUE, b"1", true).unwrap();
    assert_eq!(count(&wm), 5);

    check_walk(
        &wm,
        &[
            (b"", b"stamp", false, false, false, false, true, false),
            (b"stamp", b"stamp\x00", false, true, true, true, false, true),
            (
                b"stamp\x00",
                b"stamp123",
                false,
                false,
                false,
                false,
                true,
                false,
            ),
            (
                b"stamp123",
                b"stamp123\x00",
                false,
                true,
                true,
                false,
                false,
                false,
            ),
            (
                b"stamp123\x00",
                b"\xff\xff",
                false,
                false,
                false,
                false,
                true,
                false,
            ),
        ],
    );
    check_structure(&wm);
}

#[test]
fn add_value() {
    let mut wm = WriteMap::new();
    assert!(wm.is_empty());
    assert_eq!(count(&wm), 1);

    wm.mutate(b"apple123", M::SET_VALUE, b"17", true).unwrap();
    assert_eq!(count(&wm), 3);

    wm.mutate(b"apple123", M::ADD_VALUE, b"1", true).unwrap();
    assert_eq!(count(&wm), 3);
}

/// A small seeded generator (SplitMix64), for `deterministicRandom()`.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// `randomInt(lo, hi)`: in `[lo, hi)`.
    fn int(&mut self, lo: usize, hi: usize) -> usize {
        lo + (self.next_u64() % (hi - lo) as u64) as usize
    }

    fn coin(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }
}

/// `RandomTestImpl::getKeyForIndex`: `%010d` of `idx / 3`, then `idx % 3` zero bytes.
fn key_for_index(idx: usize) -> Vec<u8> {
    let mut key = format!("{:010}", idx / 3).into_bytes();
    key.resize(key.len() + idx % 3, 0);
    key
}

/// `RandomTestImpl::getRandomRange`.
fn random_range(rng: &mut Rng) -> (Vec<u8>, Vec<u8>) {
    let start = rng.int(0, 100);
    let end = start + rng.int(1, 1 + 100 - start);
    (key_for_index(start), key_for_index(end))
}

/// `KeyRangeMap<bool>` (default `false`): boundaries, each with the value from it on.
#[derive(Clone, Debug)]
struct RangeModel(BTreeMap<Vec<u8>, bool>);

impl RangeModel {
    fn new() -> RangeModel {
        RangeModel(BTreeMap::from([(Vec::new(), false)]))
    }

    fn at(&self, key: &[u8]) -> bool {
        *self
            .0
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
            .next_back()
            .expect("\"\" is a boundary")
            .1
    }

    fn insert(&mut self, begin: &[u8], end: &[u8], value: bool) {
        let after = self.at(end);
        let doomed: Vec<Vec<u8>> = self
            .0
            .range::<[u8], _>((Bound::Included(begin), Bound::Excluded(end)))
            .map(|(k, _)| k.clone())
            .collect();
        for k in doomed {
            self.0.remove(&k);
        }
        self.0.insert(begin.to_vec(), value);
        self.0.insert(end.to_vec(), after);
    }

    fn insert_key(&mut self, key: &[u8], value: bool) {
        let mut end = key.to_vec();
        end.push(0);
        self.insert(key, &end, value);
    }

    /// Whether the model has `value` over all of `[begin, end)`.
    fn constant_over(&self, begin: &[u8], end: &[u8], value: bool) -> bool {
        self.at(begin) == value
            && self
                .0
                .range::<[u8], _>((Bound::Excluded(begin), Bound::Excluded(end)))
                .all(|(_, v)| *v == value)
    }
}

fn check_flags(segs: &[Seg], name: &str, model: &RangeModel, flag: impl Fn(&Seg) -> bool) {
    for s in segs {
        assert!(
            model.constant_over(&s.begin, &s.end, flag(s)),
            "{name} differs over {s:?}: model {model:?}"
        );
    }
}

/// `/fdbclient/WriteMap/random`, with its model (`setMap` of operation stacks built with
/// `coalesceOver`, and the conflict, cleared and unreadable range maps), over many seeds.
#[test]
fn random() {
    for seed in 0..300 {
        random_once(seed);
    }
}

fn random_value(rng: &mut Rng) -> Vec<u8> {
    vec![b'x'; rng.int(0, 1000)]
}

fn random_once(seed: u64) {
    let mut rng = Rng(seed);
    let mut wm = WriteMap::new();
    assert!(wm.is_empty());
    assert_eq!(count(&wm), 1);

    let mut set_map: BTreeMap<Vec<u8>, OperationStack> = BTreeMap::new();
    let mut conflict_map = RangeModel::new();
    let mut clear_map = RangeModel::new();
    let mut unreadable_map = RangeModel::new();
    let erase = |m: &mut BTreeMap<Vec<u8>, OperationStack>, b: &[u8], e: &[u8]| {
        m.retain(|k, _| !(&k[..] >= b && &k[..] < e));
    };

    for _ in 0..100 {
        match rng.int(0, 10) {
            0 => {
                let (b, e) = random_range(&mut rng);
                wm.add_conflict_range(&b, &e);
                conflict_map.insert(&b, &e, true);
            }
            1 => {
                let (b, e) = random_range(&mut rng);
                wm.add_unmodified_and_unreadable_range(&b, &e);
                erase(&mut set_map, &b, &e);
                conflict_map.insert(&b, &e, false);
                clear_map.insert(&b, &e, false);
                unreadable_map.insert(&b, &e, true);
            }
            2 => {
                let add_conflict = rng.coin();
                let (b, e) = random_range(&mut rng);
                wm.clear(&b, &e, add_conflict);
                erase(&mut set_map, &b, &e);
                if add_conflict {
                    conflict_map.insert(&b, &e, true);
                }
                clear_map.insert(&b, &e, true);
                unreadable_map.insert(&b, &e, false);
            }
            3 => {
                let add_conflict = rng.coin();
                let key = key_for_index(rng.int(0, 100));
                let value = random_value(&mut rng);
                wm.mutate(&key, M::SET_VERSIONSTAMPED_VALUE, &value, add_conflict)
                    .unwrap();
                let op = RywMutation::new(M::SET_VERSIONSTAMPED_VALUE, Some(&value));
                if unreadable_map.at(&key) {
                    set_map.entry(key.clone()).or_default().push(op);
                } else {
                    set_map.insert(key.clone(), OperationStack::new(op));
                }
                if add_conflict {
                    conflict_map.insert_key(&key, true);
                }
                clear_map.insert_key(&key, false);
                unreadable_map.insert_key(&key, true);
            }
            4 => {
                let add_conflict = rng.coin();
                let key = key_for_index(rng.int(0, 100));
                let value = random_value(&mut rng);
                wm.mutate(&key, M::SET_VERSIONSTAMPED_KEY, &value, add_conflict)
                    .unwrap();
                set_map
                    .entry(key.clone())
                    .or_default()
                    .push(RywMutation::new(M::SET_VERSIONSTAMPED_KEY, Some(&value)));
                if add_conflict {
                    conflict_map.insert_key(&key, true);
                }
                clear_map.insert_key(&key, false);
                unreadable_map.insert_key(&key, true);
            }
            5 => {
                let add_conflict = rng.coin();
                let key = key_for_index(rng.int(0, 100));
                let value = random_value(&mut rng);
                wm.mutate(&key, M::AND, &value, add_conflict).unwrap();
                let op = RywMutation::new(M::AND, Some(&value));
                let stack = set_map.entry(key.clone()).or_default();
                if clear_map.at(&key) {
                    // C++'s model starts from `StringRef()` (an empty value), the map from
                    // no value: `And` treats them alike.
                    *stack = OperationStack::new(RywMutation::new(M::SET_VALUE, Some(b"")));
                    WriteMap::coalesce_over(stack, op).unwrap();
                } else if !unreadable_map.at(&key) && !stack.is_empty() {
                    WriteMap::coalesce_over(stack, op).unwrap();
                } else {
                    stack.push(op);
                }
                if add_conflict {
                    conflict_map.insert_key(&key, true);
                }
                clear_map.insert_key(&key, false);
            }
            _ => {
                let add_conflict = rng.coin();
                let key = key_for_index(rng.int(0, 100));
                let value = random_value(&mut rng);
                wm.mutate(&key, M::SET_VALUE, &value, add_conflict).unwrap();
                let op = RywMutation::new(M::SET_VALUE, Some(&value));
                if unreadable_map.at(&key) {
                    set_map.entry(key.clone()).or_default().push(op);
                } else {
                    set_map.insert(key.clone(), OperationStack::new(op));
                }
                if add_conflict {
                    conflict_map.insert_key(&key, true);
                }
                clear_map.insert_key(&key, false);
            }
        }
    }

    let segs = check_structure(&wm);
    let ops: Vec<(Vec<u8>, OperationStack)> = segs
        .iter()
        .filter_map(|s| s.op.clone().map(|op| (s.begin.clone(), op)))
        .collect();
    let expected: Vec<(Vec<u8>, OperationStack)> = set_map.into_iter().collect();
    assert_eq!(ops, expected, "seed {seed}");
    check_flags(&segs, "conflict", &conflict_map, |s| s.conflict);
    check_flags(&segs, "cleared", &clear_map, |s| s.cleared);
    check_flags(&segs, "unreadable", &unreadable_map, |s| s.unreadable);
}

// ---- Coalescing and other rules -------------------------------------------------------------

fn only_op(wm: &WriteMap, key: &[u8]) -> OperationStack {
    let it = wm.iter_at(key);
    assert_eq!(it.begin_key().to_vec(), key);
    it.op().cloned().expect("an operation")
}

fn mutation(kind: MutationType, value: &[u8]) -> RywMutation {
    RywMutation::new(kind, Some(value))
}

#[test]
fn atomic_ops_coalesce_over_a_set() {
    let mut wm = WriteMap::new();
    wm.mutate(b"k", M::SET_VALUE, &[5, 0], false).unwrap();
    wm.mutate(b"k", M::ADD_VALUE, &[1, 0], false).unwrap();
    wm.mutate(b"k", M::AND_V2, &[0x0f], false).unwrap();
    wm.mutate(b"k", M::BYTE_MAX, &[9], false).unwrap();
    assert_eq!(
        only_op(&wm, b"k").as_slice(),
        [mutation(M::SET_VALUE, &[9])]
    );
    let it = wm.iter_at(b"k");
    assert_eq!(it.segment_type(), WriteSegmentType::IndependentWrite);
    assert!(!it.is_conflict_range());
}

#[test]
fn same_atomic_ops_coalesce_unless_lengths_differ() {
    let mut wm = WriteMap::new();
    wm.mutate(b"k", M::ADD_VALUE, &[1, 0], false).unwrap();
    wm.mutate(b"k", M::ADD_VALUE, &[2, 0], false).unwrap();
    assert_eq!(
        only_op(&wm, b"k").as_slice(),
        [mutation(M::ADD_VALUE, &[3, 0])]
    );
    // `AddValue` is not associative over operands of different lengths.
    wm.mutate(b"k", M::ADD_VALUE, &[1], false).unwrap();
    assert_eq!(
        only_op(&wm, b"k").as_slice(),
        [
            mutation(M::ADD_VALUE, &[3, 0]),
            mutation(M::ADD_VALUE, &[1])
        ]
    );
    // `AndV2` is, whatever the lengths.
    wm.mutate(b"j", M::AND_V2, &[0xff, 0x0f], false).unwrap();
    wm.mutate(b"j", M::AND_V2, &[0x3c], false).unwrap();
    assert_eq!(
        only_op(&wm, b"j").as_slice(),
        [mutation(M::AND_V2, &[0x3c])]
    );
    // Two different atomic operations stay apart; the stack stays dependent.
    wm.mutate(b"j", M::OR, &[1], false).unwrap();
    assert_eq!(only_op(&wm, b"j").len(), 2);
    assert_eq!(
        wm.iter_at(b"j").segment_type(),
        WriteSegmentType::DependentWrite
    );
    assert_eq!(wm.iter_at(b"j").is_independent(), Some(false));
    // A compare-and-clear never folds into another one.
    wm.mutate(b"i", M::COMPARE_AND_CLEAR, b"a", false).unwrap();
    wm.mutate(b"i", M::COMPARE_AND_CLEAR, b"a", false).unwrap();
    assert_eq!(only_op(&wm, b"i").len(), 2);
    // A set replaces the stack.
    wm.mutate(b"j", M::SET_VALUE, b"v", false).unwrap();
    assert_eq!(
        only_op(&wm, b"j").as_slice(),
        [mutation(M::SET_VALUE, b"v")]
    );
}

#[test]
fn atomic_op_on_a_cleared_key_becomes_a_set() {
    let mut wm = WriteMap::new();
    wm.clear(b"a", b"z", true);
    wm.mutate(b"m", M::ADD_VALUE, &[7], false).unwrap();
    assert_eq!(
        only_op(&wm, b"m").as_slice(),
        [mutation(M::SET_VALUE, &[7])]
    );
    let it = wm.iter_at(b"m");
    assert_eq!(it.segment_type(), WriteSegmentType::IndependentWrite);
    assert!(it.is_conflict_range(), "inside the clear's conflict range");
    // A compare-and-clear of a cleared key: still cleared.
    wm.mutate(b"n", M::COMPARE_AND_CLEAR, b"x", false).unwrap();
    assert_eq!(only_op(&wm, b"n").as_slice(), [RywMutation::set(None)]);
    // The range after an operation in a cleared range is still cleared.
    assert!(wm.iter_at(b"m\x00").is_cleared_range());
    // A clear at the operation's own key, starting a cleared range there.
    wm.clear(b"a", b"b", true);
    wm.mutate(b"a", M::MAX, &[3], false).unwrap();
    assert_eq!(
        only_op(&wm, b"a").as_slice(),
        [mutation(M::SET_VALUE, &[3])]
    );
}

#[test]
fn coalesce_under_a_value() {
    let mut stack = OperationStack::new(mutation(M::ADD_VALUE, &[1]));
    WriteMap::coalesce_over(&mut stack, mutation(M::BYTE_MIN, &[3])).unwrap();
    assert_eq!(stack.len(), 2);
    let got = WriteMap::coalesce_under(&stack, Some(&[5])).unwrap();
    assert_eq!(got, mutation(M::SET_VALUE, &[3]));
    let got = WriteMap::coalesce_under(&stack, None).unwrap();
    assert_eq!(got, mutation(M::SET_VALUE, &[1]));
    // An independent single operation is itself.
    let stack = OperationStack::new(mutation(M::SET_VERSIONSTAMPED_VALUE, b"v"));
    assert_eq!(
        WriteMap::coalesce_under(&stack, Some(b"x")).unwrap(),
        mutation(M::SET_VERSIONSTAMPED_VALUE, b"v")
    );
    // A versionstamped operation under an atomic one cannot be coalesced.
    let mut stack = stack;
    stack.push(mutation(M::ADD_VALUE, &[1]));
    assert_eq!(
        WriteMap::coalesce_under(&stack, None).unwrap_err().code(),
        OPERATION_FAILED
    );
    assert_eq!(
        WriteMap::coalesce(mutation(M::OR, &[1]), mutation(M::ADD_VALUE, &[1]))
            .unwrap_err()
            .code(),
        OPERATION_FAILED
    );
}

#[test]
fn writes_to_unreadable_keys_stack_up() {
    let mut wm = WriteMap::new();
    wm.mutate(b"k", M::SET_VERSIONSTAMPED_VALUE, &[0; 14], false)
        .unwrap();
    wm.mutate(b"k", M::ADD_VALUE, &[1], false).unwrap();
    wm.mutate(b"k", M::SET_VALUE, b"v", false).unwrap();
    let it = wm.iter_at(b"k");
    assert!(it.is_unreadable());
    assert!(!it.is_conflict_range());
    assert_eq!(it.op().unwrap().len(), 3);
    // A clear makes it readable again.
    wm.clear(b"k", b"l", false);
    wm.mutate(b"k", M::SET_VALUE, b"w", false).unwrap();
    assert!(!wm.iter_at(b"k").is_unreadable());
    assert_eq!(only_op(&wm, b"k").len(), 1);
}

#[test]
fn versionstamped_key_range_is_unmodified_and_unreadable() {
    let mut wm = WriteMap::new();
    wm.mutate(b"p5", M::SET_VALUE, b"v", true).unwrap();
    wm.clear(b"p7", b"p8", true);
    wm.add_unmodified_and_unreadable_range(b"p", b"q");
    // Not a write (C++ leaves `writeMapEmpty` as it was).
    assert!(!wm.is_empty());
    let segs = check_structure(&wm);
    let at_p = segs.iter().find(|s| s.begin == b"p").unwrap();
    assert_eq!(at_p.end, b"q");
    assert!(at_p.unmodified && at_p.unreadable && !at_p.conflict);
    assert!(WriteMap::new().is_empty());
    let mut fresh = WriteMap::new();
    fresh.add_unmodified_and_unreadable_range(b"a", b"b");
    assert!(fresh.is_empty());
}

#[test]
fn clear_without_conflict_keeps_conflict_ranges() {
    let mut wm = WriteMap::new();
    wm.mutate(b"b", M::SET_VALUE, b"1", true).unwrap();
    wm.mutate(b"d", M::SET_VALUE, b"2", false).unwrap();
    wm.add_conflict_range(b"f", b"g");
    wm.clear(b"a", b"z", false);
    let segs = check_structure(&wm);
    let conflict: Vec<(Vec<u8>, Vec<u8>)> = segs
        .iter()
        .filter(|s| s.conflict)
        .map(|s| (s.begin.clone(), s.end.clone()))
        .collect();
    assert_eq!(
        conflict,
        [
            (b"b".to_vec(), b"b\x00".to_vec()),
            (b"f".to_vec(), b"g".to_vec())
        ]
    );
    assert!(segs.iter().all(|s| s.op.is_none()));
    assert!(
        segs.iter()
            .filter(|s| s.begin >= b"a".to_vec() && s.end <= b"z".to_vec())
            .all(|s| s.cleared)
    );
}

#[test]
fn iterators_are_snapshots() {
    let mut wm = WriteMap::new();
    wm.mutate(b"a", M::SET_VALUE, b"1", false).unwrap();
    let before = wm.iter_at(b"a");
    let segs_before = segments(&wm);
    wm.mutate(b"a", M::SET_VALUE, b"2", true).unwrap();
    wm.clear(b"0", b"z", true);
    assert_eq!(
        before.op().unwrap().as_slice(),
        [mutation(M::SET_VALUE, b"1")]
    );
    assert!(!before.is_conflict_range());
    let mut it = before.clone();
    it.skip(b"");
    let mut walked = Vec::new();
    while it.begin_key() < ExtKey::from(ALL_KEYS_END) {
        walked.push(seg(&it));
        let _ = it.advance();
    }
    assert_eq!(walked, segs_before);
    assert!(wm.iter_at(b"a").is_cleared_range());
}

/// The map and its iterators are held across `await`s by transactions that move between
/// threads.
#[test]
fn map_and_iterators_are_send_and_sync() {
    fn check<T: Send + Sync>() {}
    check::<WriteMap>();
    check::<WriteMapIter>();
    check::<OperationStack>();
}

/// Iterators held across many writes (the map shares its structure with them) still see the
/// version they were made at, walking forward, backward and by `skip`, and a copy of the map
/// is independent of the original.
#[test]
fn snapshots_stay_frozen_across_writes() {
    fn walk(mut it: WriteMapIter) -> Vec<Seg> {
        it.skip(b"");
        let mut out = Vec::new();
        while it.begin_key() < ExtKey::from(ALL_KEYS_END) {
            out.push(seg(&it));
            assert!(it.advance());
        }
        out
    }
    for seed in 0..100 {
        let mut rng = Rng(seed ^ 0xf00d);
        let mut wm = WriteMap::new();
        let mut held: Vec<(WriteMapIter, Vec<Seg>)> = Vec::new();
        let mut copies: Vec<(WriteMap, Vec<Seg>)> = Vec::new();
        for step in 0..60 {
            apply_to_map(&mut wm, &naive_random_op(&mut rng));
            if step % 3 == 0 {
                held.push((wm.iter_at(&key_for_index(rng.int(0, 30))), segments(&wm)));
            }
            if step % 20 == 0 {
                copies.push((wm.clone(), segments(&wm)));
            }
        }
        for (it, segs) in held {
            assert_eq!(walk(it), segs, "seed {seed}");
        }
        for (copy, segs) in copies {
            assert_eq!(check_structure(&copy), segs, "seed {seed}");
        }
    }
}

#[test]
fn navigation_at_the_ends() {
    let mut wm = WriteMap::new();
    let mut it = wm.iter();
    assert!(!it.retreat());
    assert_eq!(it.end_key().to_vec(), ALL_KEYS_END);
    assert!(it.advance());
    assert_eq!(it.begin_key().to_vec(), ALL_KEYS_END);
    assert_eq!(it.end_key().to_vec(), AFTER_ALL_KEYS);
    // `++` from the last segment reaches the `\xff\xff\x00` sentinel, a terminal position.
    assert!(!it.advance());
    assert_eq!(it.begin_key().to_vec(), AFTER_ALL_KEYS);
    assert!(!it.is_cleared_range() && !it.is_conflict_range() && !it.is_unreadable());
    assert!(!it.advance());
    assert_eq!(it.begin_key().to_vec(), AFTER_ALL_KEYS);
    // `--` from there is the last segment again.
    assert!(it.retreat());
    assert_eq!(it.begin_key().to_vec(), ALL_KEYS_END);
    assert_eq!(it.end_key().to_vec(), AFTER_ALL_KEYS);
    // Past `\xff\xff`: the last segment.
    let it = wm.iter_at(b"\xff\xff\xff");
    assert_eq!(it.begin_key().to_vec(), ALL_KEYS_END);

    // An operation at "" is the first segment; the empty key is a key.
    wm.mutate(b"", M::SET_VALUE, b"v", false).unwrap();
    wm.mutate(b"\x00", M::SET_VALUE, b"w", false).unwrap();
    let mut it = wm.iter();
    assert!(it.is_operation());
    assert!(!it.retreat());
    assert!(it.advance());
    // `""` and `\x00` are adjacent: no empty range between them.
    assert!(it.is_operation());
    assert_eq!(it.begin_key().to_vec(), b"\x00");
    assert!(it.retreat());
    assert_eq!(it.begin_key().to_vec(), b"");
    check_structure(&wm);
}

#[test]
fn bad_arguments() {
    let mut wm = WriteMap::new();
    assert_eq!(
        wm.mutate(b"\xff\xff", M::SET_VALUE, b"", false)
            .unwrap_err()
            .code(),
        KEY_OUTSIDE_LEGAL_RANGE
    );
    assert_eq!(
        wm.mutate(b"a", M::CLEAR_RANGE, b"", false)
            .unwrap_err()
            .code(),
        INVALID_MUTATION_TYPE
    );
    assert!(wm.is_empty());
    // Empty ranges do nothing; ranges past `\xff\xff` stop there.
    wm.clear(b"b", b"b", true);
    wm.clear(b"c", b"a", true);
    wm.add_conflict_range(b"b", b"b");
    wm.add_unmodified_and_unreadable_range(b"b", b"b");
    assert!(wm.is_empty());
    assert_eq!(count(&wm), 1);
    wm.clear(b"\xfe", b"\xff\xff\xff", true);
    let segs = check_structure(&wm);
    assert_eq!(segs.last().unwrap().begin, b"\xfe");
    assert!(segs.last().unwrap().cleared);
}

// ---- The naive model ----------------------------------------------------------------------

/// Every operation on a key since it was last cleared (or made unmodified), and whether it
/// was in a cleared range when the first one came.
#[derive(Clone, Debug)]
struct History {
    base_cleared: bool,
    ops: Vec<(MutationType, Vec<u8>)>,
    /// The stack the map should hold, worked out by [`model_stack_add`] (not by
    /// `coalesce_over`): operation types and operands (`None`: a clear).
    stack: Vec<ModelOp>,
}

/// One operation of a model stack.
type ModelOp = (MutationType, Option<Vec<u8>>);

/// `MutationRef::NON_ASSOCIATIVE_MASK`, written out again for the model.
fn model_non_associative(kind: MutationType) -> bool {
    [
        M::ADD_VALUE,
        M::OR,
        M::XOR,
        M::MAX,
        M::MIN,
        M::SET_VERSIONSTAMPED_KEY,
        M::SET_VERSIONSTAMPED_VALUE,
        M::MIN_V2,
        M::COMPARE_AND_CLEAR,
    ]
    .contains(&kind)
}

/// Adds `kind` with `operand` on top of a readable key's `stack`, as the C++ rules say, with
/// its own case analysis (the map's is `coalesce_over`, over `coalesce`): the model's
/// operations are a set, a versionstamped set or an atomic operation, and only a set is not
/// atomic.
///
/// * a set replaces the top operation;
/// * a `CompareAndClear` over a set clears it if it is cleared already or equals the operand,
///   and keeps it otherwise; over an atomic operation it is stacked;
/// * another atomic operation over the same one is combined with it, unless it is not
///   associative and the operands' lengths differ (then it is stacked); over a different
///   atomic operation it is stacked; over a set it is applied to the set's value.
fn model_stack_add(stack: &mut Vec<ModelOp>, kind: MutationType, operand: &[u8]) {
    let new = (kind, Some(operand.to_vec()));
    let Some(top) = stack.last().cloned() else {
        stack.push(new);
        return;
    };
    let top_value = top.1.as_deref();
    let top_is_set = top.0 == M::SET_VALUE;
    let result = match kind {
        M::SET_VALUE | M::SET_VERSIONSTAMPED_VALUE => new,
        M::COMPARE_AND_CLEAR if top_is_set => {
            if top_value.is_none_or(|v| v == operand) {
                (M::SET_VALUE, None)
            } else {
                top.clone()
            }
        }
        _ if top.0 == kind && kind != M::COMPARE_AND_CLEAR => {
            if model_non_associative(kind) && top_value.is_some_and(|v| v.len() != operand.len()) {
                stack.push(new);
                return;
            }
            let value = atomic::apply(kind, top_value, operand, VALUE_SIZE_LIMIT).unwrap();
            (kind, value)
        }
        _ if !top_is_set => {
            stack.push(new);
            return;
        }
        _ => {
            let value = atomic::apply(kind, top_value, operand, VALUE_SIZE_LIMIT).unwrap();
            (M::SET_VALUE, value)
        }
    };
    *stack.last_mut().expect("a top") = result;
}

/// What the write map should say, kept naively.
#[derive(Clone, Debug)]
struct Naive {
    keys: BTreeMap<Vec<u8>, History>,
    cleared: RangeModel,
    conflict: RangeModel,
    unreadable: RangeModel,
}

#[derive(Clone, Debug)]
enum Op {
    Mutate(Vec<u8>, MutationType, Vec<u8>, bool),
    Clear(Vec<u8>, Vec<u8>, bool),
    Conflict(Vec<u8>, Vec<u8>),
    Unreadable(Vec<u8>, Vec<u8>),
}

impl Naive {
    fn new() -> Naive {
        Naive {
            keys: BTreeMap::new(),
            cleared: RangeModel::new(),
            conflict: RangeModel::new(),
            unreadable: RangeModel::new(),
        }
    }

    fn drop_keys(&mut self, b: &[u8], e: &[u8]) {
        self.keys.retain(|k, _| !(&k[..] >= b && &k[..] < e));
    }

    fn apply(&mut self, op: &Op) {
        // The map's key space ends at `\xff\xff`.
        let clamp = |e: &[u8]| e.min(ALL_KEYS_END).to_vec();
        let op = &match op {
            Op::Mutate(k, ..) if &k[..] >= ALL_KEYS_END => return,
            Op::Clear(b, e, c) => Op::Clear(b.clone(), clamp(e), *c),
            Op::Conflict(b, e) => Op::Conflict(b.clone(), clamp(e)),
            Op::Unreadable(b, e) => Op::Unreadable(b.clone(), clamp(e)),
            other => other.clone(),
        };
        match op {
            Op::Mutate(k, kind, v, add_conflict) => {
                let base_cleared = self.cleared.at(k);
                let was_unreadable = self.unreadable.at(k);
                let is_versionstamp = matches!(
                    *kind,
                    M::SET_VERSIONSTAMPED_KEY | M::SET_VERSIONSTAMPED_VALUE
                );
                let history = self.keys.entry(k.clone()).or_insert(History {
                    base_cleared,
                    ops: Vec::new(),
                    stack: Vec::new(),
                });
                history.ops.push((*kind, v.clone()));
                // The stack, by the rules `WriteMap::mutate` is documented to follow.
                let new = (*kind, Some(v.clone()));
                if history.stack.is_empty() {
                    if base_cleared && *kind != M::SET_VALUE && !is_versionstamp {
                        // An atomic operation on a cleared key is a set of its result.
                        history.stack = vec![(M::SET_VALUE, None)];
                        model_stack_add(&mut history.stack, *kind, v);
                    } else {
                        history.stack = vec![new];
                    }
                } else if !was_unreadable
                    && matches!(*kind, M::SET_VALUE | M::SET_VERSIONSTAMPED_VALUE)
                {
                    // A set of a readable key replaces its operations.
                    history.stack = vec![new];
                } else if was_unreadable || is_versionstamp {
                    // On an unreadable key, operations stack up.
                    history.stack.push(new);
                } else {
                    model_stack_add(&mut history.stack, *kind, v);
                }
                if *add_conflict {
                    self.conflict.insert_key(k, true);
                }
                self.cleared.insert_key(k, false);
                if matches!(
                    *kind,
                    M::SET_VERSIONSTAMPED_KEY | M::SET_VERSIONSTAMPED_VALUE
                ) {
                    self.unreadable.insert_key(k, true);
                }
            }
            Op::Clear(b, e, add_conflict) => {
                if b >= e {
                    return;
                }
                self.drop_keys(b, e);
                self.cleared.insert(b, e, true);
                self.unreadable.insert(b, e, false);
                if *add_conflict {
                    self.conflict.insert(b, e, true);
                }
            }
            Op::Conflict(b, e) => {
                if b < e {
                    self.conflict.insert(b, e, true);
                }
            }
            Op::Unreadable(b, e) => {
                if b >= e {
                    return;
                }
                self.drop_keys(b, e);
                self.cleared.insert(b, e, false);
                self.conflict.insert(b, e, false);
                self.unreadable.insert(b, e, true);
            }
        }
    }
}

fn apply_to_map(wm: &mut WriteMap, op: &Op) {
    match op {
        Op::Mutate(k, kind, v, c) if &k[..] >= ALL_KEYS_END => {
            let e = wm.mutate(k, *kind, v, *c).unwrap_err();
            assert_eq!(e.code(), KEY_OUTSIDE_LEGAL_RANGE);
        }
        Op::Mutate(k, kind, v, c) => wm.mutate(k, *kind, v, *c).unwrap(),
        Op::Clear(b, e, c) => wm.clear(b, e, *c),
        Op::Conflict(b, e) => wm.add_conflict_range(b, e),
        Op::Unreadable(b, e) => wm.add_unmodified_and_unreadable_range(b, e),
    }
}

/// The values a key may have in the database, to read dependent operations over.
const DB_VALUES: [Option<&[u8]>; 6] = [
    None,
    Some(b""),
    Some(&[1]),
    Some(&[0xff, 0x01]),
    Some(&[2, 0, 1]),
    Some(b"ab"),
];

/// Checks the write map against the model: the segments' flags, the keys with operations,
/// and for each readable one whether it is independent and the value it gives over each of
/// [`DB_VALUES`], against applying its operations one by one.
fn check_against_model(wm: &WriteMap, model: &Naive) {
    let segs = check_structure(wm);
    check_flags(&segs, "conflict", &model.conflict, |s| s.conflict);
    check_flags(&segs, "cleared", &model.cleared, |s| s.cleared);
    check_flags(&segs, "unreadable", &model.unreadable, |s| s.unreadable);

    let op_keys: Vec<&[u8]> = segs
        .iter()
        .filter(|s| s.op.is_some())
        .map(|s| &s.begin[..])
        .collect();
    let model_keys: Vec<&[u8]> = model.keys.keys().map(|k| &k[..]).collect();
    assert_eq!(op_keys, model_keys, "{segs:#?}");

    // The stacks have the model's shape, readable or not (`model_stack_add`, which does not use
    // `coalesce_over`).
    for s in segs.iter().filter(|s| s.op.is_some()) {
        let history = &model.keys[&s.begin];
        let got: Vec<ModelOp> =
            s.op.as_ref()
                .unwrap()
                .iter()
                .map(|m| (m.kind, m.value.as_deref().map(<[u8]>::to_vec)))
                .collect();
        assert_eq!(got, history.stack, "stack of {s:?} after {:?}", history.ops);
        assert_eq!(
            s.unreadable,
            model.unreadable.at(&s.begin),
            "unreadable flag of {s:?}"
        );
    }

    for s in segs.iter().filter(|s| s.op.is_some() && !s.unreadable) {
        let history = &model.keys[&s.begin];
        let independent =
            history.base_cleared || history.ops.iter().any(|(kind, _)| *kind == M::SET_VALUE);
        assert_eq!(s.independent, Some(independent), "{s:?} {history:?}");
        let stack = s.op.as_ref().unwrap();
        for db in DB_VALUES {
            let mut value = if history.base_cleared {
                None
            } else {
                db.map(<[u8]>::to_vec)
            };
            for (kind, operand) in &history.ops {
                value = atomic::apply(*kind, value.as_deref(), operand, VALUE_SIZE_LIMIT).unwrap();
            }
            let under = if independent { None } else { db };
            let got = WriteMap::coalesce_under(stack, under).unwrap();
            assert_eq!(got.kind, M::SET_VALUE);
            assert_eq!(
                got.value.as_deref(),
                value.as_deref(),
                "{s:?} {history:?} over {db:?}"
            );
        }
    }
}

const NAIVE_OPS: [MutationType; 15] = [
    M::SET_VALUE,
    M::SET_VALUE,
    M::ADD_VALUE,
    M::AND,
    M::AND_V2,
    M::OR,
    M::XOR,
    M::MAX,
    M::MIN,
    M::MIN_V2,
    M::BYTE_MIN,
    M::BYTE_MAX,
    M::APPEND_IF_FITS,
    M::COMPARE_AND_CLEAR,
    M::SET_VERSIONSTAMPED_VALUE,
];

/// Keys `%02d` of `idx / 3` followed by `idx % 3` zeros (adjacent keys, as C++'s test has),
/// and sometimes `\xff\xff` as a range end.
fn naive_random_op(rng: &mut Rng) -> Op {
    let key = |rng: &mut Rng| {
        let idx = rng.int(0, 36);
        let mut key = format!("{:02}", idx / 3).into_bytes();
        key.resize(key.len() + idx % 3, 0);
        key
    };
    let range = |rng: &mut Rng| {
        let b = key(rng);
        let e = if rng.int(0, 20) == 0 {
            ALL_KEYS_END.to_vec()
        } else {
            key(rng)
        };
        if b <= e { (b, e) } else { (e, b) }
    };
    match rng.int(0, 12) {
        0 | 1 => {
            let (b, e) = range(rng);
            Op::Clear(b, e, rng.coin())
        }
        2 => {
            let (b, e) = range(rng);
            Op::Conflict(b, e)
        }
        3 => {
            let (b, e) = range(rng);
            Op::Unreadable(b, e)
        }
        _ => {
            let kind = if rng.int(0, 30) == 0 {
                M::SET_VERSIONSTAMPED_KEY
            } else {
                NAIVE_OPS[rng.int(0, NAIVE_OPS.len())]
            };
            let len = rng.int(0, 4);
            let value: Vec<u8> = (0..len).map(|_| [0u8, 1, 2, 0xff][rng.int(0, 4)]).collect();
            Op::Mutate(key(rng), kind, value, rng.coin())
        }
    }
}

/// The randomized comparison with the naive model, checked after every operation.
#[test]
fn random_against_naive_model() {
    for seed in 0..400 {
        let mut rng = Rng(seed ^ 0x5eed);
        let mut wm = WriteMap::new();
        let mut model = Naive::new();
        let mut log = Vec::new();
        for _ in 0..60 {
            let op = naive_random_op(&mut rng);
            log.push(op.clone());
            apply_to_map(&mut wm, &op);
            model.apply(&op);
            let result = std::panic::catch_unwind(|| check_against_model(&wm, &model));
            if let Err(e) = result {
                eprintln!("seed {seed}, operations: {log:#?}");
                std::panic::resume_unwind(e);
            }
        }
    }
}

fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        8 => prop::collection::vec(prop::sample::select(vec![0u8, 1, 2, 0xff]), 0..4),
        1 => Just(b"\xff".to_vec()),
        1 => Just(b"\xff\xfe\xff".to_vec()),
    ]
}

fn arb_range() -> impl Strategy<Value = (Vec<u8>, Vec<u8>)> {
    (
        arb_key(),
        prop_oneof![4 => arb_key(), 1 => Just(ALL_KEYS_END.to_vec())],
    )
        .prop_map(|(a, b)| if a <= b { (a, b) } else { (b, a) })
}

fn arb_op() -> impl Strategy<Value = Op> {
    let kinds: Vec<MutationType> = NAIVE_OPS
        .iter()
        .copied()
        .chain([M::SET_VERSIONSTAMPED_KEY])
        .collect();
    prop_oneof![
        6 => (
            arb_key(),
            prop::sample::select(kinds),
            prop::collection::vec(any::<u8>(), 0..4),
            any::<bool>()
        )
            .prop_map(|(k, kind, v, c)| Op::Mutate(k, kind, v, c)),
        2 => (arb_range(), any::<bool>()).prop_map(|((b, e), c)| Op::Clear(b, e, c)),
        1 => arb_range().prop_map(|(b, e)| Op::Conflict(b, e)),
        1 => arb_range().prop_map(|(b, e)| Op::Unreadable(b, e)),
    ]
}

proptest! {
    /// Any sequence of operations: no panic, and the map agrees with the naive model.
    #[test]
    fn arbitrary_operations(ops in prop::collection::vec(arb_op(), 0..40)) {
        let mut wm = WriteMap::new();
        let mut model = Naive::new();
        for op in &ops {
            apply_to_map(&mut wm, op);
            model.apply(op);
        }
        check_against_model(&wm, &model);
        // Shared snapshots: writing after taking an iterator copies, and leaves it alone.
        let it = wm.iter();
        let before = segments(&wm);
        let mut copy = wm.clone();
        copy.clear(b"", ALL_KEYS_END, true);
        prop_assert_eq!(segments(&wm), before);
        prop_assert!(it.begin_key() == ExtKey::from(&b""[..]));
    }
}

/// A C++ `for (; it.beginKey() < keys.end; ++it)` loop up to `keyAfter(\xff\xff)` ends: `++`
/// from the last segment reaches `\xff\xff\x00` (review repro).
#[test]
fn update_conflict_map_loop_to_after_all_keys() {
    let wm = WriteMap::new();
    let end = ExtKey::from(AFTER_ALL_KEYS);
    let mut it = wm.iter_at(b"\xff\xfe");
    let mut steps = 0;
    while it.begin_key() < end {
        steps += 1;
        assert!(
            steps <= 1000,
            "did not terminate: stuck at {:?}",
            it.begin_key().to_vec()
        );
        let _ = it.advance();
    }
    assert_eq!(steps, 2);
}
