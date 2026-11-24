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
use fdb_native_client::{Database, KeySelector, KeyValue, MutationType, Transaction};
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
    tx.atomic_op(b"r/g", &[5, 0], MutationType::ADD_VALUE);
    tx.atomic_op(b"r/z", b"x", MutationType::APPEND_IF_FITS);
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

/// The boundaries of C++'s `readConflicts` map in `fdbcli`'s output: `(key, "1")` starts a
/// range, `(key, "0")` ends one.
fn parse_conflicts(out: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let prefix = "\\xff\\xff/transaction/read_conflict_range/";
    let mut bounds: Vec<(Vec<u8>, bool)> = Vec::new();
    for line in out.lines() {
        let Some(rest) = line.trim().strip_prefix('`') else {
            continue;
        };
        let Some((key, value)) = rest.split_once("' is `") else {
            continue;
        };
        let Some(key) = key.strip_prefix(prefix) else {
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
    let mut script = String::from("writemode on; begin;");
    for step in steps {
        script.push_str(&match step {
            Step::Set(k) => format!(" set {} own;", arg(k)),
            Step::Clear(k) => format!(" clear {};", arg(k)),
            Step::ClearRange(b, e) => format!(" clearrange {} {};", arg(b), arg(e)),
            Step::Get(k) => format!(" get {};", arg(k)),
            Step::Range(b, e, n) => format!(" getrange {} {} {n};", arg(b), arg(e)),
        });
    }
    script.push_str(
        " getrange \\xff\\xff/transaction/read_conflict_range/ \
         \\xff\\xff/transaction/read_conflict_range0 1000; rollback;",
    );
    let out = cli(fdb, &script).await;
    parse_conflicts(&out)
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
            }
        }
        tx.add_write_conflict_range(b"q/", b"q0");
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
