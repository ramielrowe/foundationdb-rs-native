//! `getRange` against a real, disposable FDB server started by `fdb-native-testkit` (image
//! from `FDB_IMAGE`, default 7.4.10), on data written by `fdbcli` and checked against its
//! `getrange` output: whole-range reads forward and reverse through the reader and
//! transactions, `fdbcli`'s own `getrange begin end limit` windows, the batches of every
//! streaming mode and the range streams, selectors that need `getKey` (compared with the key
//! list by definition), and the read conflict ranges checked by the resolver (a write inside
//! the range a limited read covered conflicts, one after it does not). The multi-shard case is
//! in the `cluster` test binary. Run with
//! `cargo nextest run -p fdb-native-client --run-ignored only` (and `FDB_IMAGE=...:8.0.1`).

#![cfg(feature = "tokio")]

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::read::{RangeLimits, RangeOptions, StreamingMode};
use fdb_native_client::{Database, KeySelector, KeyValue};
use fdb_native_testkit::FdbContainer;
use futures_core::Stream;

const LIMIT: Duration = Duration::from_secs(120);
const COUNT: usize = 400;

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

/// The inverse of `printable()` (how `fdbcli` prints keys and values).
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

/// The rows of `fdbcli`'s `getrange` output.
fn parse_getrange(out: &str) -> Vec<KeyValue> {
    out.lines()
        .filter_map(|line| {
            let rest = line.strip_prefix('`')?;
            let (key, value) = rest.split_once("' is `")?;
            Some(KeyValue::new(
                unprintable(key),
                unprintable(value.strip_suffix('\'')?),
            ))
        })
        .collect()
}

fn key(i: usize) -> Vec<u8> {
    format!("gr/{i:03}").into_bytes()
}

/// What a selector resolves to among `keys`, by definition (`""` before the first key, the
/// end of the range of interest after the last).
fn reference(keys: &[Vec<u8>], sel: &KeySelector, after: &[u8]) -> Vec<u8> {
    let below = keys
        .iter()
        .filter(|k| {
            if sel.or_equal {
                **k <= sel.key
            } else {
                **k < sel.key
            }
        })
        .count() as i64;
    let target = below - 1 + sel.offset as i64;
    if target < 0 {
        Vec::new()
    } else if target as usize >= keys.len() {
        after.to_vec()
    } else {
        keys[target as usize].clone()
    }
}

async fn collect<S: Stream + Unpin>(mut stream: S) -> Vec<S::Item> {
    let mut out = Vec::new();
    while let Some(item) = within("stream", poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))).await
    {
        out.push(item);
    }
    out
}

fn fge(key: &[u8]) -> KeySelector {
    KeySelector::first_greater_or_equal(key.to_vec())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_get_range_matches_fdbcli() {
    let fdb = Arc::new(
        tokio::task::spawn_blocking(FdbContainer::start)
            .await
            .unwrap()
            .expect("container"),
    );
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
    // `gr/000` .. `gr/399` with values of 0 to 299 bytes.
    for chunk in (0..COUNT).collect::<Vec<_>>().chunks(100) {
        let mut writes = String::from("writemode on;");
        for &i in chunk {
            let value = "v".repeat((i * 37) % 300);
            writes.push_str(&format!(
                " set {} x{value};",
                String::from_utf8(key(i)).unwrap()
            ));
        }
        let out = cli(&fdb, &writes).await;
        assert!(out.contains("Committed"), "{out}");
    }
    let all = parse_getrange(&cli(&fdb, "getrange gr/ gr0 10000").await);
    assert_eq!(all.len(), COUNT);
    let keys: Vec<Vec<u8>> = all.iter().map(|kv| kv.key.clone()).collect();
    let mut reversed = all.clone();
    reversed.reverse();

    let db = Database::open(Some(fdb.cluster_file())).expect("database");
    let reader = db.reader();
    let version = within("GRV", reader.get_read_version()).await.unwrap();

    // The reader, whole range, both directions.
    for (reverse, want) in [(false, &all), (true, &reversed)] {
        let r = within(
            "reader",
            reader.get_range_with_limits(
                &fge(b"gr/"),
                &fge(b"gr0"),
                RangeLimits::UNLIMITED,
                reverse,
                version,
                false,
            ),
        )
        .await
        .unwrap();
        assert!(!r.more());
        assert_eq!(&r.into_key_values(), want, "reverse {reverse}");
    }

    // `fdbcli getrange begin end limit` windows (fdbcli reads forward, EXACT-like).
    let tx = db.create_transaction();
    for (b, e, n) in [(17, 230, 10), (0, 399, 1), (250, 260, 100), (390, 400, 5)] {
        let out = cli(
            &fdb,
            &format!(
                "getrange {} {} {n}",
                String::from_utf8(key(b)).unwrap(),
                String::from_utf8(key(e)).unwrap()
            ),
        )
        .await;
        let want = parse_getrange(&out);
        let o = RangeOptions::with_limit(n as u32).mode(StreamingMode::Exact);
        let got = within("tx", tx.get_range(&fge(&key(b)), &fge(&key(e)), &o))
            .await
            .unwrap();
        assert_eq!(got.kvs, want, "{b}..{e} limit {n}");
    }

    // Every streaming mode, through a stream, both directions: the whole range; the first
    // batch's size follows the mode.
    for mode in [
        StreamingMode::Iterator,
        StreamingMode::Small,
        StreamingMode::Medium,
        StreamingMode::Large,
        StreamingMode::Serial,
        StreamingMode::WantAll,
    ] {
        for (reverse, want) in [(false, &all), (true, &reversed)] {
            let o = RangeOptions::default().mode(mode).reverse(reverse);
            let batches: Vec<_> = collect(tx.get_ranges(fge(b"gr/"), fge(b"gr0"), o))
                .await
                .into_iter()
                .map(Result::unwrap)
                .collect();
            let rows: Vec<KeyValue> = batches.iter().flat_map(|b| b.kvs.clone()).collect();
            assert_eq!(&rows, want, "{mode:?} reverse {reverse}");
            println!("{mode:?} reverse {reverse}: {} batches", batches.len());
            if mode == StreamingMode::Small {
                assert!(batches.len() > 100, "{}", batches.len());
            }
        }
    }
    let rows: Vec<KeyValue> = collect(tx.range_stream(
        fge(b"gr/"),
        fge(b"gr0"),
        RangeOptions::with_limit(123).reverse(true),
    ))
    .await
    .into_iter()
    .map(Result::unwrap)
    .collect();
    assert_eq!(rows, reversed[..123].to_vec());

    // Selectors that need `getKey`, by definition.
    let s = |k: Vec<u8>, or_equal: bool, offset: i32| KeySelector::new(k, or_equal, offset);
    let cases = [
        (s(key(10), false, 5), s(key(300), true, 3)),
        (s(key(10), true, -4), s(key(20), false, 0)),
        (s(b"gr/".to_vec(), false, -3), s(key(5), false, 0)),
        (s(key(390), false, 1), s(key(399), false, 7)),
        (s(key(200), true, -50), s(key(200), true, 50)),
    ];
    for (b, e) in cases {
        for reverse in [false, true] {
            let rb = reference(&keys, &b, b"gr0");
            let re = reference(&keys, &e, b"gr0");
            let mut want: Vec<KeyValue> = all
                .iter()
                .filter(|kv| kv.key >= rb && kv.key < re)
                .cloned()
                .collect();
            if reverse {
                want.reverse();
            }
            // Whatever lies outside `gr/` (none here but the system keys past the end) is
            // excluded by keeping the selectors inside.
            let o = RangeOptions::with_limit(1000)
                .mode(StreamingMode::Exact)
                .reverse(reverse);
            let got = within("selectors", tx.get_range(&b, &e, &o)).await.unwrap();
            let got: Vec<KeyValue> = got
                .kvs
                .into_iter()
                .filter(|kv| kv.key.starts_with(b"gr/"))
                .collect();
            assert_eq!(got, want, "{b:?}..{e:?} reverse {reverse}");
        }
    }

    // The read conflict range of a limited read: `[gr/, keyAfter(gr/004))`.
    let o = RangeOptions::with_limit(5).mode(StreamingMode::Exact);
    for (write, conflicts) in [
        (key(3), true),
        (key(4), true),
        (key(5), false),
        (key(300), false),
    ] {
        let tx = db.create_transaction();
        let r = within("read", tx.get_range(&fge(b"gr/"), &fge(b"gr0"), &o))
            .await
            .unwrap();
        assert_eq!(r.kvs.len(), 5);
        assert!(r.more);
        tx.set(b"other", b"1");
        let w = db.create_transaction();
        w.set(&write, b"changed");
        within("write", w.commit()).await.unwrap();
        let result = within("commit", tx.commit()).await;
        if conflicts {
            assert_eq!(
                result.unwrap_err().code(),
                codes::NOT_COMMITTED,
                "a write to {write:?}"
            );
        } else {
            result.unwrap_or_else(|e| panic!("a write to {write:?}: {e}"));
        }
    }
    // A reverse read with a limit: from the last row read.
    let o = RangeOptions::with_limit(3)
        .mode(StreamingMode::Exact)
        .reverse(true);
    for (write, conflicts) in [(key(397), true), (key(396), false)] {
        let tx = db.create_transaction();
        let r = within("read", tx.get_range(&fge(b"gr/"), &fge(b"gr0"), &o))
            .await
            .unwrap();
        assert_eq!(r.kvs.len(), 3);
        tx.set(b"other", b"1");
        let w = db.create_transaction();
        w.set(&write, b"changed again");
        within("write", w.commit()).await.unwrap();
        let result = within("commit", tx.commit()).await;
        assert_eq!(
            result.is_err(),
            conflicts,
            "a write to {write:?}: {result:?}"
        );
    }
    // A snapshot read conflicts with nothing.
    let tx = db.create_transaction();
    tx.snapshot()
        .get_range(&fge(b"gr/"), &fge(b"gr0"), &RangeOptions::default())
        .await
        .unwrap();
    tx.set(b"other", b"2");
    let w = db.create_transaction();
    w.set(&key(0), b"changed");
    within("write", w.commit()).await.unwrap();
    within("commit", tx.commit()).await.unwrap();

    db.shutdown().await;
    drop(db);
    tokio::task::spawn_blocking(move || drop(fdb))
        .await
        .unwrap();
}
