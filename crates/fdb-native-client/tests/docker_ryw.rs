//! Read-your-writes against a real, disposable FDB server started by `fdb-native-testkit`
//! (image from `FDB_IMAGE`, default 7.4.10): reads see the transaction's writes over data in
//! the database (gets, ranges both ways, selectors, atomic operations), the resolver checks
//! the read conflict ranges as C++'s read-your-writes layer forms them (a key the
//! transaction wrote before reading it does not conflict, a key it read from the database
//! does, a snapshot read does not), and the read conflict ranges of a corpus of operation
//! sequences are the ones C++'s own read-your-writes transaction forms, read from `fdbcli`
//! (`\xff\xff/transaction/read_conflict_range/`, the transaction's `readConflicts` map). Run
//! with `cargo nextest run -p fdb-native-client --run-ignored only` (and
//! `FDB_IMAGE=...:8.0.1`).

#![cfg(feature = "tokio")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::read::{RangeOptions, StreamingMode};
use fdb_native_client::{
    Database, KeySelector, KeyValue, MutationType, Transaction, TransactionOption,
};
use fdb_native_testkit::FdbContainer;

const LIMIT: Duration = Duration::from_secs(120);

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {LIMIT:?}"))
}

async fn cli(fdb: &Arc<FdbContainer>, command: &str) -> String {
    let fdb = fdb.clone();
    let command = command.to_owned();
    tokio::task::spawn_blocking(move || fdb.fdbcli(&command))
        .await
        .unwrap()
        .unwrap_or_else(|e| panic!("fdbcli: {e}"))
}

async fn start() -> (Arc<FdbContainer>, Database) {
    let fdb = Arc::new(
        tokio::task::spawn_blocking(FdbContainer::start)
            .await
            .unwrap()
            .expect("container"),
    );
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
    let db = Database::open(Some(fdb.cluster_file())).expect("database");
    (fdb, db)
}

fn fge(k: &[u8]) -> KeySelector {
    KeySelector::first_greater_or_equal(k.to_vec())
}

fn exact(limit: u32) -> RangeOptions {
    RangeOptions::with_limit(if limit == 0 { 10_000 } else { limit }).mode(StreamingMode::Exact)
}

fn keys(rows: &[KeyValue]) -> Vec<Vec<u8>> {
    rows.iter().map(|kv| kv.key.clone()).collect()
}

/// Writes `pairs` in a transaction of its own.
async fn load(db: &Database, pairs: &[(&[u8], &[u8])]) {
    let tx = db.create_transaction();
    for (k, v) in pairs {
        tx.set(k, v);
    }
    within("load", tx.commit()).await.expect("load");
}

async fn rows(tx: &Transaction, b: &[u8], e: &[u8], options: RangeOptions) -> Vec<KeyValue> {
    within("range", tx.get_range(&fge(b), &fge(e), &options))
        .await
        .expect("range")
        .kvs
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_ryw_reads_see_own_writes_over_the_database() {
    let (_fdb, db) = start().await;
    let base: Vec<(&[u8], &[u8])> = vec![
        (b"r/a", b"1"),
        (b"r/c", b"3"),
        (b"r/e", b"5"),
        (b"r/g", &[1, 0]),
        (b"r/i", b"9"),
    ];
    load(&db, &base).await;

    let tx = db.create_transaction();
    tx.set(b"r/b", b"own");
    tx.clear(b"r/c");
    tx.clear_range(b"r/h", b"r/j");
    tx.atomic_op(b"r/g", &[5, 0], MutationType::Add);
    tx.atomic_op(b"r/z", b"x", MutationType::AppendIfFits);
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> =
        base.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
    model.insert(b"r/b".to_vec(), b"own".to_vec());
    model.remove(b"r/c".as_slice());
    model.remove(b"r/i".as_slice());
    model.insert(b"r/g".to_vec(), vec![6, 0]);
    model.insert(b"r/z".to_vec(), b"x".to_vec());
    let expected: Vec<KeyValue> = model
        .iter()
        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
        .collect();

    for k in [&b"r/a"[..], b"r/b", b"r/c", b"r/g", b"r/i", b"r/z", b"r/q"] {
        assert_eq!(
            within("get", tx.get(k)).await.unwrap(),
            model.get(k).cloned(),
            "{k:?}"
        );
    }
    assert_eq!(rows(&tx, b"r/", b"r0", exact(0)).await, expected);
    let mut reversed = expected.clone();
    reversed.reverse();
    assert_eq!(
        rows(&tx, b"r/", b"r0", exact(0).reverse(true)).await,
        reversed
    );
    assert_eq!(
        keys(&rows(&tx, b"r/", b"r0", exact(2).reverse(true)).await),
        vec![b"r/z".to_vec(), b"r/g".to_vec()]
    );
    // Selectors through the writes: the keys are r/a r/b r/e r/g r/z.
    let get_key = |sel: KeySelector| {
        let tx = tx.clone();
        async move { within("get_key", tx.get_key(&sel)).await.unwrap() }
    };
    assert_eq!(
        get_key(KeySelector::first_greater_than(b"r/a".to_vec())).await,
        b"r/b"
    );
    assert_eq!(
        get_key(KeySelector::last_less_than(b"r/e".to_vec())).await,
        b"r/b"
    );
    assert_eq!(
        get_key(KeySelector::new(b"r/a".to_vec(), false, 4)).await,
        b"r/g"
    );
    assert_eq!(
        get_key(KeySelector::first_greater_than(b"r/z".to_vec())).await,
        b"\xff"
    );

    // A read takes its view when it is called.
    let before = tx.get(b"r/a");
    tx.set(b"r/a", b"later");
    assert_eq!(within("get", before).await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(
        within("get", tx.get(b"r/a")).await.unwrap(),
        Some(b"later".to_vec())
    );

    within("commit", tx.commit()).await.expect("commit");
    let check = db.create_transaction();
    assert_eq!(
        within("get", check.get(b"r/g")).await.unwrap(),
        Some(vec![6, 0])
    );
    assert_eq!(within("get", check.get(b"r/i")).await.unwrap(), None);

    // Versionstamped keys are unreadable until the commit.
    let tx = db.create_transaction();
    let mut key = b"r/v".to_vec();
    key.extend([0xff; 10]);
    key.extend(3i32.to_le_bytes());
    tx.set_versionstamped_key(&key, b"stamped");
    let e = within("range", tx.get_range(&fge(b"r/"), &fge(b"r0"), &exact(0)))
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::ACCESSED_UNREADABLE);
    assert_eq!(
        within("get", tx.get(b"r/a")).await.unwrap(),
        Some(b"later".to_vec())
    );
}

/// The resolver applies the read conflict ranges the read-your-writes layer forms.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_ryw_conflicts_skip_own_writes_and_snapshot_reads() {
    let (_fdb, db) = start().await;
    load(
        &db,
        &[
            (b"c/a", b"1"),
            (b"c/b", b"2"),
            (b"c/s", b"3"),
            (b"c/r", b"4"),
        ],
    )
    .await;

    // Written, then read: no conflict on it when another transaction writes it.
    let tx = db.create_transaction();
    tx.set(b"c/a", b"own");
    assert_eq!(
        within("get", tx.get(b"c/a")).await.unwrap(),
        Some(b"own".to_vec())
    );
    // Read from the database with a snapshot read: no conflict either.
    within("get", tx.snapshot().get(b"c/s")).await.unwrap();
    load(&db, &[(b"c/a", b"other"), (b"c/s", b"other")]).await;
    within("commit", tx.commit()).await.expect("no conflict");

    // Read from the database: a conflict.
    let tx = db.create_transaction();
    tx.set(b"c/x", b"own");
    within("get", tx.get(b"c/b")).await.unwrap();
    load(&db, &[(b"c/b", b"other")]).await;
    let e = within("commit", tx.commit()).await.unwrap_err();
    assert_eq!(e.code(), codes::NOT_COMMITTED);

    // A range read: the cleared part of it does not conflict, the rest does.
    let tx = db.create_transaction();
    tx.clear_range(b"c/a", b"c/c");
    let read = rows(&tx, b"c/", b"c0", exact(0)).await;
    assert_eq!(keys(&read), vec![b"c/r".to_vec(), b"c/s".to_vec()]);
    load(&db, &[(b"c/b", b"again")]).await;
    within("commit", tx.commit())
        .await
        .expect("no conflict in the cleared range");
    let tx = db.create_transaction();
    tx.clear_range(b"c/a", b"c/c");
    rows(&tx, b"c/", b"c0", exact(0)).await;
    load(&db, &[(b"c/q", b"new")]).await;
    let e = within("commit", tx.commit()).await.unwrap_err();
    assert_eq!(e.code(), codes::NOT_COMMITTED);

    // With read-your-writes disabled, reads conflict whatever the transaction wrote.
    load(&db, &[(b"c/z", b"db")]).await;
    let tx = db.create_transaction();
    tx.set_option(fdb_native_client::TransactionOption::ReadYourWritesDisable)
        .unwrap();
    tx.set(b"c/z", b"own");
    assert_eq!(
        within("get", tx.get(b"c/z")).await.unwrap(),
        Some(b"db".to_vec())
    );
    load(&db, &[(b"c/z", b"third")]).await;
    let e = within("commit", tx.commit()).await.unwrap_err();
    assert_eq!(e.code(), codes::NOT_COMMITTED);
}

/// The boundaries of a conflict range module of the special key space in `fdbcli`'s output
/// (`which` is `read` or `write`): `(key, "1")` starts a range, `(key, "0")` ends one.
fn parse_conflicts(out: &str, which: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let prefix = format!("\\xff\\xff/transaction/{which}_conflict_range/");
    let mut bounds: Vec<(Vec<u8>, bool)> = Vec::new();
    for line in out.lines() {
        let Some(rest) = line.trim().strip_prefix('`') else {
            continue;
        };
        let Some((key, value)) = rest.split_once("' is `") else {
            continue;
        };
        let Some(key) = key.strip_prefix(prefix.as_str()) else {
            continue;
        };
        bounds.push((unprintable(key), value.starts_with('1')));
    }
    let mut ranges = Vec::new();
    let mut open: Option<Vec<u8>> = None;
    for (key, start) in bounds {
        match (start, open.take()) {
            (true, None) => open = Some(key),
            (false, Some(begin)) => ranges.push((begin, key)),
            (s, o) => panic!("unexpected boundary {key:?} {s} after {o:?}: {out}"),
        }
    }
    if let Some(begin) = open {
        ranges.push((begin, b"\xff\xff".to_vec()));
    }
    ranges
}

/// The inverse of `printable()`.
fn unprintable(text: &str) -> Vec<u8> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'\\' {
            out.push(bytes[i]);
            i += 1;
        } else if bytes[i + 1] == b'\\' {
            out.push(b'\\');
            i += 2;
        } else {
            out.push(u8::from_str_radix(&text[i + 2..i + 4], 16).unwrap());
            i += 4;
        }
    }
    out
}

/// `fdbcli`'s escaping of a key argument.
fn arg(k: &[u8]) -> String {
    k.iter()
        .map(|&b| {
            if b.is_ascii_alphanumeric() || b == b'/' {
                (b as char).to_string()
            } else {
                format!("\\x{b:02x}")
            }
        })
        .collect()
}

/// A step that both `fdbcli` (C++'s read-your-writes transaction) and this client can do.
#[derive(Clone, Debug)]
enum Step {
    Set(&'static [u8]),
    Clear(&'static [u8]),
    ClearRange(&'static [u8], &'static [u8]),
    Get(&'static [u8]),
    /// `getrange begin end limit`: firstGreaterOrEqual selectors, a row limit, no byte limit.
    Range(&'static [u8], &'static [u8], u32),
    /// The `NEXT_WRITE_NO_WRITE_CONFLICT_RANGE` option.
    NoConflict,
}

/// Corpus cases `fdbcli` can express, with the ranges the fake-cluster corpus (`ryw.rs`)
/// expects; the keys are under `p/` so the cases do not touch each other's data.
/// A corpus case: its name, its steps, and the read conflict ranges expected.
type Case = (&'static str, Vec<Step>, Vec<(&'static [u8], &'static [u8])>);

fn corpus() -> Vec<Case> {
    use Step::*;
    vec![
        ("get", vec![Get(b"p/a")], vec![(b"p/a", b"p/a\x00")]),
        ("get of an own set", vec![Set(b"p/a"), Get(b"p/a")], vec![]),
        (
            "get of an own clear",
            vec![Clear(b"p/a"), Get(b"p/a")],
            vec![],
        ),
        (
            "gets coalesce",
            vec![Get(b"p/a"), Get(b"p/a\x00"), Get(b"p/c")],
            vec![(b"p/a", b"p/a\x00\x00"), (b"p/c", b"p/c\x00")],
        ),
        (
            "range",
            vec![Range(b"p/b", b"p/m", 100)],
            vec![(b"p/b", b"p/m")],
        ),
        (
            "range cut by a limit",
            vec![Range(b"p/b", b"p/m", 1)],
            vec![(b"p/b", b"p/f\x00")],
        ),
        (
            "range over an own set",
            vec![Set(b"p/g"), Range(b"p/b", b"p/m", 100)],
            vec![(b"p/b", b"p/g"), (b"p/g\x00", b"p/m")],
        ),
        (
            "range over an own clear",
            vec![ClearRange(b"p/c", b"p/h"), Range(b"p/a", b"p/z", 100)],
            vec![(b"p/a", b"p/c"), (b"p/h", b"p/z")],
        ),
        (
            "range over an own clear, cut",
            vec![ClearRange(b"p/c", b"p/h"), Range(b"p/a", b"p/z", 2)],
            vec![(b"p/a", b"p/c"), (b"p/h", b"p/k\x00")],
        ),
        (
            "range over an own set, cut",
            vec![Set(b"p/g"), Range(b"p/b", b"p/m", 2)],
            vec![(b"p/b", b"p/g")],
        ),
        (
            "get in an own clear",
            vec![ClearRange(b"p/a", b"p/c"), Get(b"p/b")],
            vec![],
        ),
        (
            "empty range",
            vec![Range(b"p/b", b"p/c", 10)],
            vec![(b"p/b", b"p/c")],
        ),
        (
            "range around an own single clear",
            vec![Clear(b"p/k"), Range(b"p/f", b"p/n", 100)],
            vec![(b"p/f", b"p/k"), (b"p/k\x00", b"p/n")],
        ),
        // A write after a read leaves the read's range.
        (
            "read, write, read",
            vec![Get(b"p/a"), Set(b"p/a"), Get(b"p/a")],
            vec![(b"p/a", b"p/a\x00")],
        ),
        (
            "range, then a write in it",
            vec![Range(b"p/a", b"p/c", 10), Set(b"p/b")],
            vec![(b"p/a", b"p/c")],
        ),
        (
            "ranges over keys with zero bytes",
            vec![Set(b"p/f\x00"), Range(b"p/f", b"p/f\x00\x00", 10)],
            vec![(b"p/f", b"p/f\x00")],
        ),
    ]
}

/// Runs a corpus case with `fdbcli` and returns C++'s `readConflicts`.
async fn cpp_conflicts(fdb: &Arc<FdbContainer>, steps: &[Step]) -> Vec<(Vec<u8>, Vec<u8>)> {
    cpp_conflict_ranges(fdb, steps, "read").await
}

/// Runs a corpus case with `fdbcli` and returns the `which` (`read` or `write`) conflict
/// ranges of C++'s transaction (`getReadConflictRangeIntersecting`,
/// `getWriteConflictRangeIntersecting`).
async fn cpp_conflict_ranges(
    fdb: &Arc<FdbContainer>,
    steps: &[Step],
    which: &str,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut script = String::from("writemode on; begin;");
    for step in steps {
        script.push_str(&match step {
            Step::Set(k) => format!(" set {} own;", arg(k)),
            Step::Clear(k) => format!(" clear {};", arg(k)),
            Step::ClearRange(b, e) => format!(" clearrange {} {};", arg(b), arg(e)),
            Step::Get(k) => format!(" get {};", arg(k)),
            Step::Range(b, e, n) => format!(" getrange {} {} {n};", arg(b), arg(e)),
            Step::NoConflict => " option on NEXT_WRITE_NO_WRITE_CONFLICT_RANGE;".to_owned(),
        });
    }
    script.push_str(&format!(
        " getrange \\xff\\xff/transaction/{which}_conflict_range/ \
         \\xff\\xff/transaction/{which}_conflict_range0 1000; rollback;"
    ));
    let out = cli(fdb, &script).await;
    parse_conflicts(&out, which)
}

/// The corpus against C++ itself: `fdbcli`'s transactions are C++'s read-your-writes
/// transactions, and `\xff\xff/transaction/read_conflict_range/` reads their `readConflicts`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_ryw_read_conflict_ranges_match_cpp() {
    let (fdb, db) = start().await;
    let data: Vec<(&[u8], &[u8])> = [b"p/a", b"p/f", b"p/k", b"p/n", b"p/q", b"p/x"]
        .iter()
        .map(|k| (&k[..], &b"v"[..]))
        .collect();
    load(&db, &data).await;
    for (name, steps, expected) in corpus() {
        let expected: Vec<(Vec<u8>, Vec<u8>)> = expected
            .iter()
            .map(|(b, e)| (b.to_vec(), e.to_vec()))
            .collect();
        let cpp = cpp_conflicts(&fdb, &steps).await;
        assert_eq!(cpp, expected, "C++: {name}");
        // This client forms the same ranges: a transaction that writes in one of them
        // (outside the transaction's own writes) makes the commit conflict; the ranges are
        // checked by the fake-cluster corpus exactly.
        let tx = db.create_transaction();
        for step in &steps {
            match step {
                Step::Set(k) => tx.set(k, b"own"),
                Step::Clear(k) => tx.clear(k),
                Step::ClearRange(b, e) => tx.clear_range(b, e),
                Step::Get(k) => {
                    within("get", tx.get(k)).await.unwrap();
                }
                Step::Range(b, e, n) => {
                    rows(&tx, b, e, exact(*n)).await;
                }
                Step::NoConflict => tx
                    .set_option(TransactionOption::NextWriteNoWriteConflictRange)
                    .unwrap(),
            }
        }
        tx.add_write_conflict_range(b"q/", b"q0").unwrap();
        // A write just before each expected range's end, and just after it.
        let probe = db.create_transaction();
        let mut conflicts = false;
        for (b, _) in &expected {
            probe.set(b, b"probe");
            conflicts = true;
        }
        if conflicts {
            within("probe", probe.commit()).await.expect("probe");
            let e = within("commit", tx.commit()).await.unwrap_err();
            assert_eq!(e.code(), codes::NOT_COMMITTED, "{name}");
        } else {
            within("commit", tx.commit()).await.expect(name);
        }
        // Back to the corpus's data (the probe and the commit wrote in it).
        let reset = db.create_transaction();
        reset.clear_range(b"p/", b"p0");
        for (k, v) in &data {
            reset.set(k, v);
        }
        within("reset", reset.commit()).await.expect("reset");
    }
}

// ---- the commit -------------------------------------------------------------------------------

fn key_after(k: &[u8]) -> Vec<u8> {
    let mut k = k.to_vec();
    k.push(0);
    k
}

/// Write conflict range corpus cases `fdbcli` can express, with the ranges the fake-cluster
/// commit corpus (`ryw.rs`) has for the same writes: the maximal runs of the write map's
/// conflict segments, as `writeRangeToNativeTransaction` sends them.
fn write_corpus() -> Vec<Case> {
    use Step::*;
    vec![
        ("a set", vec![Set(b"w/a")], vec![(b"w/a", b"w/a\x00")]),
        (
            "sets of adjacent keys",
            vec![Set(b"w/a"), Set(b"w/a\x00"), Set(b"w/c")],
            vec![(b"w/a", b"w/a\x00\x00"), (b"w/c", b"w/c\x00")],
        ),
        (
            "a clear, then a set in it",
            vec![ClearRange(b"w/a", b"w/m"), Set(b"w/c")],
            vec![(b"w/a", b"w/m")],
        ),
        (
            "single clears",
            vec![Clear(b"w/c"), Clear(b"w/e")],
            vec![(b"w/c", b"w/c\x00"), (b"w/e", b"w/e\x00")],
        ),
        (
            "writes without a write conflict range",
            vec![NoConflict, Set(b"w/a"), Set(b"w/b")],
            vec![(b"w/b", b"w/b\x00")],
        ),
        (
            "a clear without one keeps the ranges in it",
            vec![Set(b"w/b"), NoConflict, ClearRange(b"w/a", b"w/e")],
            vec![(b"w/b", b"w/b\x00")],
        ),
        (
            "a set with one in a clear without one",
            vec![NoConflict, ClearRange(b"w/a", b"w/e"), Set(b"w/c")],
            vec![(b"w/c", b"w/c\x00")],
        ),
        (
            "a clear with one over a set without one",
            vec![NoConflict, Set(b"w/c"), ClearRange(b"w/a", b"w/e")],
            vec![(b"w/a", b"w/e")],
        ),
        (
            "reads only",
            vec![Get(b"w/a"), Range(b"w/", b"w0", 10)],
            vec![],
        ),
    ]
}

/// The write conflict ranges of the corpus are C++'s (`fdbcli`'s transaction, read from
/// `\xff\xff/transaction/write_conflict_range/`), and this client's commit makes the
/// resolver fail exactly the transactions that read a key in them: one probe transaction per
/// key of interest reads it (a read conflict range) at a read version before the commit, and
/// commits after it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_ryw_write_conflict_ranges_match_cpp() {
    let (fdb, db) = start().await;
    for (name, steps, expected) in write_corpus() {
        let expected: Vec<(Vec<u8>, Vec<u8>)> = expected
            .iter()
            .map(|(b, e)| (b.to_vec(), e.to_vec()))
            .collect();
        let cpp = cpp_conflict_ranges(&fdb, &steps, "write").await;
        assert_eq!(cpp, expected, "C++: {name}");

        // The keys of interest: those of the steps, the ranges' bounds and the keys right
        // after their begins.
        let mut probes: Vec<Vec<u8>> = Vec::new();
        for step in &steps {
            match step {
                Step::Set(k) | Step::Clear(k) | Step::Get(k) => probes.push(k.to_vec()),
                Step::ClearRange(b, e) | Step::Range(b, e, _) => {
                    probes.extend([b.to_vec(), e.to_vec(), key_after(b)]);
                }
                Step::NoConflict => {}
            }
        }
        for (b, e) in &expected {
            probes.extend([b.clone(), e.clone(), key_after(b)]);
        }
        probes.sort();
        probes.dedup();
        let mut readers = Vec::new();
        for p in probes {
            let reader = db.create_transaction();
            within("read version", reader.get_read_version())
                .await
                .unwrap();
            reader.add_read_conflict_range(&p, &key_after(&p)).unwrap();
            reader
                .add_write_conflict_range(b"wprobe", b"wprobe\x00")
                .unwrap();
            readers.push((p, reader));
        }
        let tx = db.create_transaction();
        for step in &steps {
            match step {
                Step::Set(k) => tx.set(k, b"own"),
                Step::Clear(k) => tx.clear(k),
                Step::ClearRange(b, e) => tx.clear_range(b, e),
                Step::Get(k) => {
                    within("get", tx.get(k)).await.unwrap();
                }
                Step::Range(b, e, n) => {
                    rows(&tx, b, e, exact(*n)).await;
                }
                Step::NoConflict => tx
                    .set_option(TransactionOption::NextWriteNoWriteConflictRange)
                    .unwrap(),
            }
        }
        within("commit", tx.commit()).await.expect(name);
        for (p, reader) in readers {
            let wanted = expected
                .iter()
                .any(|(b, e)| b.as_slice() <= p.as_slice() && p < *e);
            match within("probe", reader.commit()).await {
                Ok(_) => assert!(!wanted, "{name}: {p:?} should conflict"),
                Err(e) => {
                    assert_eq!(e.code(), codes::NOT_COMMITTED, "{name}: {p:?}");
                    assert!(wanted, "{name}: {p:?} should not conflict");
                }
            }
        }
    }
}

/// A xorshift generator.
struct Gen(u64);

impl Gen {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn key(&mut self, prefix: &[u8]) -> Vec<u8> {
        let mut k = prefix.to_vec();
        k.push(b'a' + self.below(8) as u8);
        k.extend(std::iter::repeat_n(0, self.below(2) as usize));
        k
    }

    fn value(&mut self) -> Vec<u8> {
        (0..self.below(9)).map(|_| self.below(256) as u8).collect()
    }
}

/// Every atomic operation (`Min` and `BitAnd` are sent as `MinV2` and `AndV2`), those two
/// twice (the list had the version 1 and 2 types of each).
const ALL_OPS: [MutationType; 12] = [
    MutationType::Add,
    MutationType::BitAnd,
    MutationType::BitAnd,
    MutationType::BitOr,
    MutationType::BitXor,
    MutationType::Max,
    MutationType::Min,
    MutationType::Min,
    MutationType::ByteMin,
    MutationType::ByteMax,
    MutationType::AppendIfFits,
    MutationType::CompareAndClear,
];

fn apply_model(model: &mut BTreeMap<Vec<u8>, Vec<u8>>, k: &[u8], op: MutationType, v: &[u8]) {
    let new = fdb_native_client::atomic::apply(
        op,
        model.get(k).map(Vec::as_slice),
        v,
        fdb_native_client::atomic::VALUE_SIZE_LIMIT,
    )
    .unwrap();
    match new {
        Some(new) => model.insert(k.to_vec(), new),
        None => model.remove(k),
    };
}

/// Random atomic operations (every kind, on random values of 0 to 8 bytes), sets and clears
/// through the read-your-writes layer: the transaction's reads see the values the model
/// (`atomic::apply`) gives, and after the commit (the write map's coalesced mutations) the
/// server holds them too: the server's own atomic operations agree with the model and with
/// what the transaction read. Range reads only in rounds without `CompareAndClear` (a key it
/// clears still counts for C++'s selectors, a quirk the unit tests pin).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_ryw_random_atomic_operations_match_the_server() {
    let (_fdb, db) = start().await;
    let mut g = Gen(0x5eed_a70c_0de5_0001);
    for round in 0..24u32 {
        let prefix = format!("ra/{round:02}/").into_bytes();
        let end = {
            let mut e = prefix.clone();
            *e.last_mut().unwrap() += 1;
            e
        };
        let mut base: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for _ in 0..g.below(8) {
            base.insert(g.key(&prefix), g.value());
        }
        let pairs: Vec<(&[u8], &[u8])> = base
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        load(&db, &pairs).await;
        let with_cac = round % 2 == 1;
        let tx = db.create_transaction();
        let mut model = base.clone();
        for step in 0..40 {
            match g.below(10) {
                0 => {
                    let (k, v) = (g.key(&prefix), g.value());
                    tx.set(&k, &v);
                    model.insert(k, v);
                }
                1 => {
                    let k = g.key(&prefix);
                    tx.clear(&k);
                    model.remove(&k);
                }
                2 => {
                    let (mut a, mut b) = (g.key(&prefix), g.key(&prefix));
                    if a > b {
                        std::mem::swap(&mut a, &mut b);
                    }
                    tx.clear_range(&a, &b);
                    let doomed: Vec<Vec<u8>> =
                        model.range(a.clone()..b).map(|(k, _)| k.clone()).collect();
                    for k in doomed {
                        model.remove(&k);
                    }
                }
                3..=6 => {
                    let (k, v) = (g.key(&prefix), g.value());
                    let n = if with_cac { 12 } else { 11 };
                    let op = ALL_OPS[g.below(n) as usize];
                    tx.atomic_op(&k, &v, op);
                    apply_model(&mut model, &k, op, &v);
                }
                7 | 8 => {
                    let k = g.key(&prefix);
                    assert_eq!(
                        within("get", tx.get(&k)).await.unwrap(),
                        model.get(&k).cloned(),
                        "round {round} step {step}: get {k:?}"
                    );
                }
                _ if !with_cac => {
                    let reverse = g.below(2) == 0;
                    let got = rows(&tx, &prefix, &end, exact(0).reverse(reverse)).await;
                    let mut expected: Vec<KeyValue> = model
                        .iter()
                        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                        .collect();
                    if reverse {
                        expected.reverse();
                    }
                    assert_eq!(got, expected, "round {round} step {step}: range");
                }
                _ => {}
            }
        }
        within("commit", tx.commit())
            .await
            .unwrap_or_else(|e| panic!("round {round}: {e}"));
        let check = db.create_transaction();
        let got = rows(&check, &prefix, &end, exact(0)).await;
        let expected: Vec<KeyValue> = model
            .iter()
            .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
            .collect();
        assert_eq!(got, expected, "round {round}: the server's values");
    }
}

/// Two `AppendIfFits` of one key coalesce into one (`WriteMap::coalesce`), so near the value
/// size limit the server keeps what C++'s `libfdb_c` leaves (checked by the ygr.4 review's
/// oracle), not what the two applied in turn would give: a key of 99 995 bytes with `xyz`
/// appended twice keeps its 99 995 bytes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_ryw_append_if_fits_near_the_value_size_limit() {
    let (_fdb, db) = start().await;
    let near = vec![b'q'; 99_995];
    let small = vec![b's'; 10];
    load(&db, &[(b"aif/a", &near), (b"aif/c", &small)]).await;
    let tx = db.create_transaction();
    tx.atomic_op(b"aif/a", b"xyz", MutationType::AppendIfFits);
    tx.atomic_op(b"aif/a", b"xyz", MutationType::AppendIfFits);
    tx.atomic_op(b"aif/c", &[b'x'; 60_000], MutationType::AppendIfFits);
    tx.atomic_op(b"aif/c", &[b'y'; 50_000], MutationType::AppendIfFits);
    within("commit", tx.commit()).await.expect("commit");
    let tx = db.create_transaction();
    let a = within("get", tx.get(b"aif/a")).await.unwrap().unwrap();
    assert_eq!(a, near);
    let c = within("get", tx.get(b"aif/c")).await.unwrap().unwrap();
    assert_eq!(c.len(), 60_010);
    assert!(c.ends_with(&[b'x'; 60_000]));
}

/// A versionstamped key: the transaction's earlier writes in the range of keys it may take
/// are sent first (`writeRangeToNativeTransaction`), its later ones after; a clear over it
/// drops it (C++'s write map does), and the transaction's other writes go as usual.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_ryw_versionstamped_keys_with_writes_around_them() {
    let (_fdb, db) = start().await;
    let mut key = b"vs/".to_vec();
    key.extend([0xff; 10]);
    key.extend(3i32.to_le_bytes());
    let tx = db.create_transaction();
    let read_version = within("read version", tx.get_read_version()).await.unwrap();
    // In the range: `vs/` + the stamp of a version after the read version.
    let mut before = b"vs/".to_vec();
    before.extend((read_version + 1).to_be_bytes());
    before.extend([0, 0, b'b']);
    let mut after = b"vs/".to_vec();
    after.extend((read_version + 1).to_be_bytes());
    after.extend([0, 0, b'a']);
    tx.set(&before, b"before");
    tx.atomic_op(&before, b"+", MutationType::AppendIfFits);
    tx.set_versionstamped_key(&key, b"stamped");
    tx.set(&after, b"after");
    tx.set(b"vs0", b"outside");
    let stamp = tx.get_versionstamp();
    within("commit", tx.commit()).await.expect("commit");
    let stamp = within("stamp", stamp).await.unwrap();
    let mut stamped_key = b"vs/".to_vec();
    stamped_key.extend(stamp);
    let check = db.create_transaction();
    let got = rows(&check, b"vs/", b"vs1", exact(0)).await;
    let mut expected = vec![
        KeyValue::new(before.clone(), b"before+".to_vec()),
        KeyValue::new(after.clone(), b"after".to_vec()),
        KeyValue::new(stamped_key, b"stamped".to_vec()),
        KeyValue::new(b"vs0".to_vec(), b"outside".to_vec()),
    ];
    expected.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(got, expected);

    // A clear over the versionstamped key drops it.
    let tx = db.create_transaction();
    tx.clear_range(b"vs/", b"vs0");
    within("commit", tx.commit()).await.unwrap();
    let tx = db.create_transaction();
    tx.set_versionstamped_key(&key, b"dropped");
    tx.clear_range(b"vs/", b"vs/\xff");
    tx.set(b"vs/z", b"kept");
    within("commit", tx.commit()).await.unwrap();
    let check = db.create_transaction();
    assert_eq!(
        rows(&check, b"vs/", b"vs0", exact(0)).await,
        vec![KeyValue::new(b"vs/z".to_vec(), b"kept".to_vec())]
    );
}

/// A watch reads through the read-your-writes layer: a key the transaction read already is
/// not read again, its read conflict range makes the commit fail if the key changed, and a
/// transaction with read-your-writes disabled cannot watch.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_ryw_watches() {
    let (_fdb, db) = start().await;
    load(&db, &[(b"wt/k", b"1"), (b"wt/n", &[1, 0])]).await;

    let tx = db.create_transaction();
    tx.set_option(TransactionOption::ReadYourWritesDisable)
        .unwrap();
    assert_eq!(
        within("watch", tx.watch(b"wt/k")).await.unwrap_err().code(),
        codes::WATCHES_DISABLED
    );

    // A dependent write: the watch watches the database's value with the addition.
    let tx = db.create_transaction();
    tx.atomic_op(b"wt/n", &[1, 0], MutationType::Add);
    let watch_n = within("watch", tx.watch(b"wt/n")).await.unwrap();
    let watch_k = within("watch", tx.watch(b"wt/k")).await.unwrap();
    within("commit", tx.commit()).await.unwrap();
    assert_eq!(
        within("get", db.create_transaction().get(b"wt/n"))
            .await
            .unwrap(),
        Some(vec![2, 0])
    );
    load(&db, &[(b"wt/n", &[3, 0]), (b"wt/k", b"2")]).await;
    within("watch n", watch_n).await.unwrap();
    within("watch k", watch_k).await.unwrap();

    // The watch's read conflict range: a change after the read version fails the commit.
    let tx = db.create_transaction();
    tx.atomic_op(b"wt/n", &[1, 0], MutationType::Add);
    let watch = within("watch", tx.watch(b"wt/n")).await.unwrap();
    load(&db, &[(b"wt/n", &[9, 0])]).await;
    let e = within("commit", tx.commit()).await.unwrap_err();
    assert_eq!(e.code(), codes::NOT_COMMITTED);
    assert_eq!(
        within("watch", watch).await.unwrap_err().code(),
        codes::NOT_COMMITTED
    );
}
