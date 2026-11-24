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
    rows.iter().map(|kv| kv.key.clone()).collect()
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
