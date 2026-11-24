//! The read paths over an in-memory database (a [`RywSource`] that answers reads as
//! NativeAPI does on a database split into shards: `more` and `readThrough` at a shard
//! boundary, `readToBegin` / `readThroughEnd`, the limits with `minRows`), against a model:
//! the database with the transaction's writes applied (`crate::atomic` for the atomic
//! operations). The cache carries over between reads, as in a transaction.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use fdb_native_schema::domain::MutationType as M;
use proptest::prelude::*;

use super::*;
use crate::atomic;
use crate::read::RangeRead;
use crate::ryw::WriteMap;

const MAX: &[u8] = b"\xff";
const KEY_SPACE_END: &[u8] = b"\xff\xff";

/// Runs a future that never waits (the in-memory source answers at once).
fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    let mut cx = Context::from_waker(Waker::noop());
    match f.as_mut().poll(&mut cx) {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("the in-memory source never waits"),
    }
}

/// A database in memory, split into shards at `boundaries`.
struct Mem {
    cache: Mutex<SnapshotCache>,
    db: BTreeMap<Vec<u8>, Vec<u8>>,
    boundaries: Vec<Vec<u8>>,
    value_reads: AtomicUsize,
    range_reads: AtomicUsize,
}

impl Mem {
    fn new(db: BTreeMap<Vec<u8>, Vec<u8>>, boundaries: Vec<Vec<u8>>) -> Mem {
        Mem {
            cache: Mutex::new(SnapshotCache::new()),
            db,
            boundaries,
            value_reads: AtomicUsize::new(0),
            range_reads: AtomicUsize::new(0),
        }
    }

    /// What `sel` resolves to (`""` before the first key, `\xff\xff` after the last).
    fn resolve(&self, sel: &KeySelector) -> Vec<u8> {
        let keys: Vec<&Vec<u8>> = self.db.keys().collect();
        let below = keys
            .iter()
            .filter(|k| {
                if sel.or_equal {
                    k.as_slice() <= sel.key.as_slice()
                } else {
                    k.as_slice() < sel.key.as_slice()
                }
            })
            .count() as i64;
        let target = below - 1 + i64::from(sel.offset);
        if target < 0 {
            Vec::new()
        } else if target as usize >= keys.len() {
            KEY_SPACE_END.to_vec()
        } else {
            keys[target as usize].clone()
        }
    }

    fn range(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        mut limits: Limits,
        reverse: bool,
    ) -> Result<RangeRead, Error> {
        self.range_reads.fetch_add(1, Ordering::SeqCst);
        assert!(limits.is_valid(), "{limits:?}");
        assert!(!limits.is_reached(), "{limits:?}");
        // NativeAPI reads from a `firstGreaterOrEqual` selector's key as it is, and resolves
        // the others first.
        let at = |s: &KeySelector| {
            if is_first_greater_or_equal(s) {
                s.key.clone()
            } else {
                self.resolve(s)
            }
        };
        let b = at(begin);
        let e = at(end);
        let mut rows = Vec::new();
        let mut more = false;
        let mut read_through = None;
        if b < e {
            let mut all: Vec<(&Vec<u8>, &Vec<u8>)> = self.db.range(b.clone()..e.clone()).collect();
            if reverse {
                all.reverse();
            }
            // The shard boundaries inside the range, in the order of the read.
            let mut cuts: Vec<&Vec<u8>> = self
                .boundaries
                .iter()
                .filter(|k| **k > b && **k < e)
                .collect();
            cuts.sort();
            cuts.dedup();
            if reverse {
                cuts.reverse();
            }
            let mut cuts = cuts.into_iter().peekable();
            for (k, v) in &all {
                // A shard ends before this row: with a byte limit, a batch ends with the first
                // shard that returned `minRows` rows (`more`, `readThrough` the boundary),
                // as NativeAPI's `getRange` does.
                let mut crossed = None;
                while let Some(cut) = cuts.peek() {
                    let past = if reverse { *k < *cut } else { *k >= *cut };
                    if !past {
                        break;
                    }
                    crossed = cuts.next();
                }
                if let Some(cut) = crossed {
                    if limits.has_satisfied_min_rows() && !rows.is_empty() {
                        more = true;
                        read_through = Some(cut.clone());
                        break;
                    }
                }
                if limits.is_reached() {
                    more = true;
                    break;
                }
                let kv = KeyValue::new((*k).clone(), (*v).clone());
                decrement(&mut limits, &kv);
                rows.push(kv);
            }
        }
        let read_to_begin = b.is_empty() && (!reverse || !more);
        let read_through_end = e == KEY_SPACE_END && (reverse || !more);
        Ok(RangeRead::for_tests(
            rows,
            more,
            read_to_begin,
            read_through_end,
            read_through,
        ))
    }
}

impl RywSource for Mem {
    fn with_cache<T>(&self, f: impl FnOnce(&mut SnapshotCache) -> T) -> Result<T, Error> {
        Ok(f(&mut self.cache.lock().unwrap()))
    }

    fn read_value(&self, key: Vec<u8>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        self.value_reads.fetch_add(1, Ordering::SeqCst);
        let value = self.db.get(&key).cloned();
        Box::pin(async move { Ok(value) })
    }

    fn read_range(
        &self,
        begin: KeySelector,
        end: KeySelector,
        limits: Limits,
        reverse: bool,
    ) -> BoxFuture<'_, Result<RangeRead, Error>> {
        let result = self.range(&begin, &end, limits, reverse);
        Box::pin(async move { result })
    }
}

fn read<'s>(source: &'s Mem, writes: &WriteMap) -> RywRead<'s, Mem> {
    RywRead {
        source,
        writes: writes.iter(),
        max_read_key: MAX,
        bypass_unreadable: false,
    }
}

fn fge(k: &[u8]) -> KeySelector {
    KeySelector::first_greater_or_equal(k.to_vec())
}

fn keys_of(rows: &[KeyValue]) -> Vec<Vec<u8>> {
    rows.iter().map(|kv| kv.key.to_vec()).collect()
}

fn db(pairs: &[(&[u8], &[u8])]) -> BTreeMap<Vec<u8>, Vec<u8>> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect()
}

/// A row limit that never binds (`fdb_c`'s limits always have a row or a byte limit).
fn unlimited() -> Limits {
    Limits {
        rows: 1000,
        min_rows: 1,
        bytes: -1,
    }
}

#[test]
fn reads_see_own_writes_and_cache_what_they_read() {
    let mem = Mem::new(db(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]), vec![]);
    let mut writes = WriteMap::new();
    writes.mutate(b"b", M::SET_VALUE, b"own", true).unwrap();
    writes.clear(b"c", b"d", true);
    writes.mutate(b"d", M::ADD_VALUE, &[1], true).unwrap();
    let r = read(&mem, &writes);
    assert_eq!(block_on(get(&r, b"a")).unwrap(), Some(b"1".to_vec()));
    assert_eq!(block_on(get(&r, b"b")).unwrap(), Some(b"own".to_vec()));
    assert_eq!(block_on(get(&r, b"c")).unwrap(), None);
    // A dependent write on a missing key: the operation over nothing.
    assert_eq!(block_on(get(&r, b"d")).unwrap(), Some(vec![1]));
    // `b` and `c` needed no read; `a` and `d` did, once.
    assert_eq!(mem.value_reads.load(Ordering::SeqCst), 2);
    assert_eq!(block_on(get(&r, b"a")).unwrap(), Some(b"1".to_vec()));
    assert_eq!(mem.value_reads.load(Ordering::SeqCst), 2);

    let out = block_on(get_range_value(&r, fge(b""), fge(b"\xff"), unlimited())).unwrap();
    assert_eq!(
        out.rows,
        vec![
            KeyValue::new(b"a".to_vec(), b"1".to_vec()),
            KeyValue::new(b"b".to_vec(), b"own".to_vec()),
            KeyValue::new(b"d".to_vec(), vec![1]),
        ]
    );
    assert!(!out.more);
    // All known now: no more storage reads.
    let reads = mem.range_reads.load(Ordering::SeqCst);
    let out = block_on(get_range_value_back(
        &r,
        fge(b""),
        fge(b"\xff"),
        unlimited(),
    ))
    .unwrap();
    assert_eq!(
        keys_of(&out.rows),
        vec![b"d".to_vec(), b"b".to_vec(), b"a".to_vec()]
    );
    assert_eq!(mem.range_reads.load(Ordering::SeqCst), reads);
    assert_eq!(
        block_on(get_key(&r, KeySelector::last_less_than(b"d".to_vec()))).unwrap(),
        b"b"
    );
    assert_eq!(mem.range_reads.load(Ordering::SeqCst), reads);
}

#[test]
fn conflict_ranges_skip_independent_writes() {
    let mut writes = WriteMap::new();
    writes.mutate(b"b", M::SET_VALUE, b"own", true).unwrap();
    writes.clear(b"d", b"f", true);
    writes.mutate(b"g", M::ADD_VALUE, &[1], true).unwrap();
    let mut c = ReadConflicts::new();
    add_range_conflict(&mut c, b"a", b"z", &writes.iter());
    let ranges: Vec<(Vec<u8>, Vec<u8>)> = c.ranges().map(|r| (r.begin, r.end)).collect();
    assert_eq!(
        ranges,
        vec![
            (b"a".to_vec(), b"b".to_vec()),
            (b"b\x00".to_vec(), b"d".to_vec()),
            (b"f".to_vec(), b"z".to_vec()),
        ]
    );
    let mut c = ReadConflicts::new();
    add_get_conflict(&mut c, b"b", &writes.iter());
    add_get_conflict(&mut c, b"e", &writes.iter());
    assert!(c.is_empty());
    add_get_conflict(&mut c, b"g", &writes.iter());
    assert_eq!(c.ranges().count(), 1);
    // A range up to `keyAfter(\xff\xff)`'s neighbourhood ends (the write map's last segment).
    let mut c = ReadConflicts::new();
    add_get_key_conflict(
        &mut c,
        &KeySelector::first_greater_than(b"\xff\xfe".to_vec()),
        b"\xff\xff",
        &writes.iter(),
    )
    .unwrap();
    let ranges: Vec<(Vec<u8>, Vec<u8>)> = c.ranges().map(|r| (r.begin, r.end)).collect();
    assert_eq!(
        ranges,
        vec![(b"\xff\xfe\x00".to_vec(), b"\xff\xff".to_vec())]
    );
}

#[test]
fn set_request_limits_as_cpp() {
    // No byte limit: rows grow by the extra rows, `minRows` too.
    let mut l = Limits {
        rows: 5,
        min_rows: 1,
        bytes: -1,
    };
    set_request_limits(&mut l, 3, 1, 0);
    assert_eq!((l.rows, l.min_rows, l.bytes), (8, 4, -1));
    // A byte limit doubles with each request, up to `REPLY_BYTE_LIMIT`.
    let mut l = Limits {
        rows: -1,
        min_rows: 1,
        bytes: 1000,
    };
    set_request_limits(&mut l, 0, 1, 3);
    assert_eq!((l.rows, l.min_rows, l.bytes), (-1, 1, 8000));
    let mut l = Limits {
        rows: -1,
        min_rows: 1,
        bytes: 1000,
    };
    set_request_limits(&mut l, 0, 1, 30);
    assert_eq!(l.bytes, REPLY_BYTE_LIMIT);
    // A byte limit used up: no byte limit, and a row limit of what is still wanted.
    let mut l = Limits {
        rows: -1,
        min_rows: 1,
        bytes: 0,
    };
    set_request_limits(&mut l, 2, 4, 1);
    assert_eq!((l.rows, l.min_rows, l.bytes), (4, 4, -1));
}

// ---- the model ----------------------------------------------------------------------------

/// A write of the model.
#[derive(Clone, Debug)]
enum Write {
    Set(Vec<u8>, Vec<u8>),
    Clear(Vec<u8>),
    ClearRange(Vec<u8>, Vec<u8>),
    Atomic(Vec<u8>, M, Vec<u8>),
}

/// A read of the model.
#[derive(Clone, Debug)]
enum Read {
    Get(Vec<u8>),
    GetKey(KeySelector),
    Range {
        begin: KeySelector,
        end: KeySelector,
        rows: Option<u32>,
        bytes: Option<u32>,
        reverse: bool,
    },
}

#[derive(Clone, Debug)]
enum Op {
    Write(Write),
    Read(Read),
}

/// The atomic operations of the model. Not `CompareAndClear`: a key it clears still counts
/// as a key when a selector resolves through it in the cache (`RYWIterator::typeMap` says
/// key/value for a dependent write over a known value, and only `kv()` finds it cleared),
/// a C++ quirk the port keeps ([`compare_and_clear_quirk`]).
const ATOMICS: [M; 9] = [
    M::ADD_VALUE,
    M::AND_V2,
    M::OR,
    M::XOR,
    M::MAX,
    M::MIN_V2,
    M::BYTE_MIN,
    M::BYTE_MAX,
    M::APPEND_IF_FITS,
];

fn key() -> impl Strategy<Value = Vec<u8>> {
    (0u8..12, 0usize..3).prop_map(|(k, zeros)| {
        let mut key = vec![b'a' + k];
        key.extend(std::iter::repeat_n(0u8, zeros));
        key
    })
}

fn value() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(0u8..4, 0..3)
}

fn selector() -> impl Strategy<Value = KeySelector> {
    (key(), any::<bool>(), -3i32..5)
        .prop_map(|(k, or_equal, offset)| KeySelector::new(k, or_equal, offset))
}

fn write() -> impl Strategy<Value = Write> {
    prop_oneof![
        (key(), value()).prop_map(|(k, v)| Write::Set(k, v)),
        key().prop_map(Write::Clear),
        (key(), key()).prop_map(|(a, b)| if a <= b {
            Write::ClearRange(a, b)
        } else {
            Write::ClearRange(b, a)
        }),
        (key(), 0usize..ATOMICS.len(), value())
            .prop_map(|(k, i, v)| Write::Atomic(k, ATOMICS[i], v)),
    ]
}

fn range_read() -> impl Strategy<Value = Read> {
    (
        selector(),
        selector(),
        prop::option::of(1u32..5),
        prop::option::of(1u32..40),
        any::<bool>(),
    )
        .prop_map(|(begin, end, rows, bytes, reverse)| Read::Range {
            begin,
            end,
            rows,
            bytes,
            reverse,
        })
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => write().prop_map(Op::Write),
        1 => key().prop_map(|k| Op::Read(Read::Get(k))),
        1 => selector().prop_map(|s| Op::Read(Read::GetKey(s))),
        3 => range_read().prop_map(Op::Read),
    ]
}

fn database() -> impl Strategy<Value = BTreeMap<Vec<u8>, Vec<u8>>> {
    prop::collection::btree_map(key(), value(), 0..12)
}

fn boundaries() -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(key(), 0..4)
}

fn apply(model: &mut BTreeMap<Vec<u8>, Vec<u8>>, writes: &mut WriteMap, w: &Write) {
    match w {
        Write::Set(k, v) => {
            writes.mutate(k, M::SET_VALUE, v, true).unwrap();
            model.insert(k.clone(), v.clone());
        }
        Write::Clear(k) => {
            let mut end = k.clone();
            end.push(0);
            writes.clear(k, &end, true);
            model.remove(k);
        }
        Write::ClearRange(b, e) => {
            writes.clear(b, e, true);
            let doomed: Vec<Vec<u8>> = model
                .range(b.clone()..e.clone())
                .map(|(k, _)| k.clone())
                .collect();
            for k in doomed {
                model.remove(&k);
            }
        }
        Write::Atomic(k, op, v) => {
            writes.mutate(k, *op, v, true).unwrap();
            match atomic::apply(
                *op,
                model.get(k).map(Vec::as_slice),
                v,
                atomic::VALUE_SIZE_LIMIT,
            )
            .unwrap()
            {
                Some(new) => model.insert(k.clone(), new),
                None => model.remove(k),
            };
        }
    }
}

/// What a selector resolves to in the model, as the transaction returns it: `""` before
/// the first key, `MAX` after the last readable one.
fn model_key(model: &BTreeMap<Vec<u8>, Vec<u8>>, sel: &KeySelector) -> Vec<u8> {
    let keys: Vec<&Vec<u8>> = model.keys().collect();
    let below = keys
        .iter()
        .filter(|k| {
            if sel.or_equal {
                ***k <= sel.key
            } else {
                ***k < sel.key
            }
        })
        .count() as i64;
    let target = below - 1 + i64::from(sel.offset);
    if target < 0 {
        Vec::new()
    } else if target as usize >= keys.len() {
        MAX.to_vec()
    } else {
        keys[target as usize].clone()
    }
}

fn check_read(
    mem: &Mem,
    writes: &WriteMap,
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    r: &Read,
) -> Result<(), TestCaseError> {
    let ryw = read(mem, writes);
    match r {
        Read::Get(k) => {
            prop_assert_eq!(
                block_on(get(&ryw, k)).unwrap(),
                model.get(k).cloned(),
                "get {:?}",
                k
            );
        }
        Read::GetKey(sel) => {
            let got = block_on(get_key(&ryw, sel.clone())).unwrap();
            prop_assert_eq!(got, model_key(model, sel), "get_key {:?}", sel);
        }
        Read::Range {
            begin,
            end,
            rows,
            bytes,
            reverse,
        } => {
            let begin = remove_or_equal(begin);
            let end = remove_or_equal(end);
            if begin.offset >= end.offset && begin.key >= end.key {
                // `getRange`'s inverted range: never read.
                return Ok(());
            }
            // `fdb_c`'s limits always have a row or a byte limit (the streaming mode's).
            let bytes = if rows.is_none() && bytes.is_none() {
                Some(100_000)
            } else {
                *bytes
            };
            let limits = Limits::new(crate::read::RangeLimits::new(*rows, bytes));
            let out = if *reverse {
                block_on(get_range_value_back(
                    &ryw,
                    begin.clone(),
                    end.clone(),
                    limits,
                ))
            } else {
                block_on(get_range_value(&ryw, begin.clone(), end.clone(), limits))
            }
            .unwrap();
            let b = model_key(model, &begin);
            let e = model_key(model, &end);
            let mut expected: Vec<KeyValue> = if b < e {
                model
                    .range(b.clone()..e.clone())
                    .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                    .collect()
            } else {
                Vec::new()
            };
            if *reverse {
                expected.reverse();
            }
            let n = out.rows.len();
            prop_assert!(
                n <= expected.len(),
                "{:?} {:?}: {:?} vs {:?}",
                begin,
                end,
                out,
                expected
            );
            prop_assert_eq!(
                &out.rows[..],
                &expected[..n],
                "{:?} {:?} {:?}",
                begin,
                end,
                r
            );
            if let Some(rows) = rows {
                prop_assert!(n <= *rows as usize);
            }
            if bytes.is_none() {
                let want = rows.map_or(expected.len(), |r| expected.len().min(r as usize));
                prop_assert_eq!(n, want, "{:?}: {:?} vs {:?}", r, out.rows, expected);
            } else if !expected.is_empty() {
                prop_assert!(n >= 1);
            }
            if n < expected.len() {
                prop_assert!(out.more, "{:?}: cut short without more: {:?}", r, out);
            }
            let mut c = ReadConflicts::new();
            add_range_read_conflict(&mut c, &begin, &end, *reverse, &out, MAX, &writes.iter())
                .unwrap();
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(600))]

    /// Random writes and reads in one transaction: every read sees the database with the
    /// writes made before it, whatever the cache knows by then.
    #[test]
    fn random_reads_match_the_model(
        db in database(),
        boundaries in boundaries(),
        ops in prop::collection::vec(op(), 1..40),
    ) {
        let mem = Mem::new(db.clone(), boundaries);
        let mut writes = WriteMap::new();
        let mut model = db;
        for op in &ops {
            match op {
                Op::Write(w) => apply(&mut model, &mut writes, w),
                Op::Read(r) => check_read(&mem, &writes, &model, r)?,
            }
        }
        // A snapshot read that does not see the writes sees the database (and the cache).
        let base = WriteMap::new();
        for op in &ops {
            if let Op::Read(r) = op {
                check_read(&mem, &base, &mem.db, r)?;
            }
        }
    }
}

/// C++'s `resolveKeySelectorFromCache` counts a key whose `CompareAndClear` cleared it once
/// its value is known (a dependent write over a known value is a key/value segment), while
/// `getRangeValue` and `get` find it cleared: the port does the same.
#[test]
fn compare_and_clear_quirk() {
    let mem = Mem::new(db(&[(b"a", b"")]), vec![]);
    let mut writes = WriteMap::new();
    writes
        .mutate(b"a\x00", M::COMPARE_AND_CLEAR, b"", true)
        .unwrap();
    let r = read(&mem, &writes);
    assert_eq!(block_on(get(&r, b"a\x00")).unwrap(), None);
    // `lastLessThan(a\x00\x00)` is `a` in the database, but once `a\x00` is known absent
    // its operation counts as a key: the range is `[a\x00, a\x00)`.
    let out = block_on(get_range_value(
        &r,
        KeySelector::last_less_than(b"a\x00\x00".to_vec()),
        KeySelector::first_greater_than(b"a".to_vec()),
        unlimited(),
    ))
    .unwrap();
    assert!(out.rows.is_empty(), "{out:?}");
}

// ---- the conflict ranges cover every key a result depends on --------------------------------
//
// An oracle for the read conflict ranges (from the ygr.3 review): run a sequence of writes
// and reads over a database, then, for every key `k` of a small universe and a database
// that differs from it only at `k` (the key removed, or set, or set to another value), check
// that if the results the reads returned are not what a transaction could have seen over
// that database, `k` is in the read conflict ranges. A conflict range that misses a key the
// results depend on would let a transaction commit on a stale read.

/// A xorshift generator.
struct XGen(u64);

impl XGen {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn key(&mut self) -> Vec<u8> {
        let mut k = vec![b'a' + self.below(6) as u8];
        k.extend(std::iter::repeat_n(0u8, self.below(3) as usize));
        k
    }

    fn sel(&mut self) -> KeySelector {
        KeySelector::new(self.key(), self.below(2) == 0, self.below(13) as i32 - 6)
    }
}

/// The keys the oracle changes: `a`..`g` with 0 to 2 zero bytes after.
fn universe() -> Vec<Vec<u8>> {
    let mut u = Vec::new();
    for c in 0..7u8 {
        for z in 0..3 {
            let mut k = vec![b'a' + c];
            k.extend(std::iter::repeat_n(0u8, z));
            u.push(k);
        }
    }
    u
}

/// What a read returned.
#[derive(Debug, PartialEq)]
enum Out {
    V(Option<Vec<u8>>),
    K(Vec<u8>),
    R(Vec<KeyValue>, bool),
    E(u16),
}

/// Runs `ops` over `db` (split at `boundaries`) as a transaction does: the results and the
/// read conflict ranges.
fn run_ops(
    db: &BTreeMap<Vec<u8>, Vec<u8>>,
    boundaries: &[Vec<u8>],
    ops: &[Op],
) -> (Vec<Out>, ReadConflicts) {
    let mem = Mem::new(db.clone(), boundaries.to_vec());
    let mut writes = WriteMap::new();
    let mut model = db.clone();
    let mut conflicts = ReadConflicts::new();
    let mut outs = Vec::new();
    for op in ops {
        let r = match op {
            Op::Write(w) => {
                apply(&mut model, &mut writes, w);
                continue;
            }
            Op::Read(r) => r,
        };
        let ryw = read(&mem, &writes);
        match r {
            Read::Get(k) => match block_on(get(&ryw, k)) {
                Ok(v) => {
                    add_get_conflict(&mut conflicts, k, &ryw.writes);
                    outs.push(Out::V(v));
                }
                Err(e) => outs.push(Out::E(e.code())),
            },
            Read::GetKey(sel) => match block_on(get_key(&ryw, sel.clone())) {
                Ok(k) => {
                    add_get_key_conflict(&mut conflicts, sel, &k, &ryw.writes).unwrap();
                    outs.push(Out::K(k));
                }
                Err(e) => outs.push(Out::E(e.code())),
            },
            Read::Range {
                begin,
                end,
                rows,
                bytes,
                reverse,
            } => {
                let begin = remove_or_equal(begin);
                let end = remove_or_equal(end);
                if begin.offset >= end.offset && begin.key >= end.key {
                    outs.push(Out::R(Vec::new(), false));
                    continue;
                }
                let bytes = if rows.is_none() && bytes.is_none() {
                    Some(100_000)
                } else {
                    *bytes
                };
                let limits = Limits::new(crate::read::RangeLimits::new(*rows, bytes));
                let out = if *reverse {
                    block_on(get_range_value_back(
                        &ryw,
                        begin.clone(),
                        end.clone(),
                        limits,
                    ))
                } else {
                    block_on(get_range_value(&ryw, begin.clone(), end.clone(), limits))
                };
                match out {
                    Ok(out) => {
                        add_range_read_conflict(
                            &mut conflicts,
                            &begin,
                            &end,
                            *reverse,
                            &out,
                            MAX,
                            &ryw.writes,
                        )
                        .unwrap();
                        outs.push(Out::R(out.rows.clone(), out.more));
                    }
                    Err(e) => outs.push(Out::E(e.code())),
                }
            }
        }
    }
    (outs, conflicts)
}

/// Whether `k` is in the read conflict ranges.
fn covers(c: &ReadConflicts, k: &[u8]) -> bool {
    c.ranges()
        .any(|r| r.begin.as_slice() <= k && k < r.end.as_slice())
}

/// Whether `outs` are results a transaction could have seen running `ops` over `db`:
/// `Some(why)` if not. A range read's rows must be a prefix of the rows in its range, all of
/// them if it says there are no more.
fn valid_under(db: &BTreeMap<Vec<u8>, Vec<u8>>, ops: &[Op], outs: &[Out]) -> Option<String> {
    let mut model = db.clone();
    let mut wm = WriteMap::new();
    let mut outs = outs.iter();
    for op in ops {
        let r = match op {
            Op::Write(w) => {
                apply(&mut model, &mut wm, w);
                continue;
            }
            Op::Read(r) => r,
        };
        match (r, outs.next().expect("a result per read")) {
            (_, Out::E(_)) => {}
            (Read::Get(k), Out::V(v)) => {
                if model.get(k) != v.as_ref() {
                    return Some(format!("get {k:?}: {v:?} vs {:?}", model.get(k)));
                }
            }
            (Read::GetKey(sel), Out::K(k)) => {
                let want = model_key(&model, sel);
                if &want != k {
                    return Some(format!("get_key {sel:?}: {k:?} vs {want:?}"));
                }
            }
            (
                Read::Range {
                    begin,
                    end,
                    rows,
                    reverse,
                    ..
                },
                Out::R(got, more),
            ) => {
                let b = model_key(&model, &remove_or_equal(begin));
                let e = model_key(&model, &remove_or_equal(end));
                let mut exp: Vec<KeyValue> = if b < e {
                    model
                        .range(b..e)
                        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                        .collect()
                } else {
                    Vec::new()
                };
                if *reverse {
                    exp.reverse();
                }
                if let Some(r) = rows {
                    exp.truncate(*r as usize);
                }
                if got.len() > exp.len() || got[..] != exp[..got.len()] {
                    return Some(format!("range rows {got:?} vs {exp:?}"));
                }
                if !*more && got.len() != exp.len() {
                    return Some(format!("range !more {got:?} vs {exp:?}"));
                }
            }
            (r, o) => unreachable!("{r:?} {o:?}"),
        }
    }
    None
}

/// The oracle on one case: `Some(failure)` for a key the results depend on that the read
/// conflict ranges miss.
fn uncovered_dependency(
    db: &BTreeMap<Vec<u8>, Vec<u8>>,
    boundaries: &[Vec<u8>],
    ops: &[Op],
) -> Option<String> {
    let (outs, conflicts) = run_ops(db, boundaries, ops);
    for k in universe() {
        let mut variants = Vec::new();
        match db.get(&k) {
            Some(v) => {
                let mut d = db.clone();
                d.remove(&k);
                variants.push(d);
                let mut d = db.clone();
                let mut other = v.clone();
                other.push(7);
                d.insert(k.clone(), other);
                variants.push(d);
            }
            None => {
                let mut d = db.clone();
                d.insert(k.clone(), vec![5]);
                variants.push(d);
            }
        }
        for d in variants {
            if let Some(why) = valid_under(&d, ops, &outs) {
                if !covers(&conflicts, &k) {
                    return Some(format!(
                        "key {k:?}: {why}\n db {db:?}\n bounds {boundaries:?}\n ops {ops:?}\n \
                         outs {outs:?}\n conflicts {:?}",
                        conflicts.ranges().collect::<Vec<_>>()
                    ));
                }
            }
        }
    }
    None
}

fn db_of(keys: &[&[u8]]) -> BTreeMap<Vec<u8>, Vec<u8>> {
    keys.iter().map(|k| (k.to_vec(), vec![1])).collect()
}

fn range_op(begin: KeySelector, end: KeySelector, rows: u32, reverse: bool) -> Op {
    Op::Read(Read::Range {
        begin,
        end,
        rows: Some(rows),
        bytes: None,
        reverse,
    })
}

/// The cases where a range read's result depends on keys past the rows it returned
/// (`addConflictRange(GetRangeReq)`, `ReadYourWrites.actor.cpp` 7.4:250-300): a read cut by
/// its limit depends on the keys between its last row and its end only if its end selector
/// counts back (offset 0 or less), and, reversed, on the keys between its begin and its
/// lowest row only if its begin selector counts forward past the first key (offset over 1).
#[test]
fn conflicts_cover_dependencies_past_a_limit() {
    let lt = |k: &[u8], offset: i32| KeySelector::new(k.to_vec(), false, offset);
    let cases: Vec<(&str, Vec<&[u8]>, Op)> = vec![
        // `[a, lastLessThan(c))` is `[a, b)`; one row: `a`, and more. Without `b` the end
        // is `a` and the range empty.
        (
            "forward, end offset 0, cut",
            vec![b"a", b"b", b"c"],
            range_op(fge(b"a"), lt(b"c", 0), 1, false),
        ),
        // The end two keys before `d`: `b`.
        (
            "forward, end offset -1, cut",
            vec![b"a", b"b", b"c", b"d"],
            range_op(fge(b"a"), lt(b"d", -1), 1, false),
        ),
        // `[second key from a, end)` reversed, two rows `c b`, and more. Without `a` the
        // begin is `c`.
        (
            "reverse, begin offset 2, cut",
            vec![b"a", b"b", b"c"],
            range_op(lt(b"a", 2), fge(b"z"), 2, true),
        ),
        (
            "reverse, begin offset 3, cut",
            vec![b"a", b"b", b"c", b"d"],
            range_op(lt(b"a", 3), fge(b"z"), 2, true),
        ),
    ];
    for (name, keys, op) in cases {
        let db = db_of(&keys);
        for boundaries in [vec![], vec![b"b".to_vec()], vec![b"c".to_vec()]] {
            let ops = vec![op.clone()];
            if let Some(failure) = uncovered_dependency(&db, &boundaries, &ops) {
                panic!("{name}: {failure}");
            }
        }
        // The rows came from the database (not from the transaction's writes): the read
        // depended on them.
        let (outs, _) = run_ops(&db, &[], std::slice::from_ref(&op));
        assert!(
            matches!(&outs[0], Out::R(rows, true) if !rows.is_empty()),
            "{name}: {outs:?}"
        );
    }
}

/// The oracle over random sequences of writes and reads over random databases and shard
/// boundaries (`PSEED` and `PCASES` choose the seed and the number of cases).
#[test]
fn conflicts_cover_dependencies() {
    let seed: u64 = std::env::var("PSEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x5eed_1234_abcd_0001);
    let cases: usize = std::env::var("PCASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let mut g = XGen(seed);
    let ops_list = [M::ADD_VALUE, M::MAX, M::BYTE_MIN, M::APPEND_IF_FITS, M::XOR];
    let mut failures = Vec::new();
    for case in 0..cases {
        let mut db = BTreeMap::new();
        for _ in 0..g.below(10) {
            db.insert(g.key(), vec![g.below(3) as u8]);
        }
        let boundaries: Vec<Vec<u8>> = (0..g.below(3)).map(|_| g.key()).collect();
        let mut ops = Vec::new();
        for _ in 0..(1 + g.below(10)) {
            let op = match g.below(8) {
                0 => Op::Write(Write::Set(g.key(), vec![g.below(3) as u8])),
                1 => Op::Write(Write::Clear(g.key())),
                2 => {
                    let (mut a, mut b) = (g.key(), g.key());
                    if a > b {
                        std::mem::swap(&mut a, &mut b);
                    }
                    Op::Write(Write::ClearRange(a, b))
                }
                3 => Op::Write(Write::Atomic(
                    g.key(),
                    ops_list[g.below(5) as usize],
                    vec![g.below(3) as u8],
                )),
                4 => Op::Read(Read::Get(g.key())),
                5 => Op::Read(Read::GetKey(g.sel())),
                _ => Op::Read(Read::Range {
                    begin: g.sel(),
                    end: g.sel(),
                    rows: [None, Some(1), Some(2), Some(3)][g.below(4) as usize],
                    bytes: [None, Some(1), Some(12)][g.below(3) as usize],
                    reverse: g.below(2) == 0,
                }),
            };
            ops.push(op);
        }
        if let Some(failure) = uncovered_dependency(&db, &boundaries, &ops) {
            failures.push(format!("case {case}: {failure}"));
        }
    }
    if !failures.is_empty() {
        let n = failures.len();
        failures.sort_by_key(String::len);
        panic!("{n} failures; shortest:\n{}", failures[0]);
    }
}
