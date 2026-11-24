//! The client against a multi-process FDB cluster started by `fdb-native-testkit`
//! ([`FdbCluster`], image from `FDB_IMAGE`, default 7.4.10), for what needs several shards and
//! storage servers. This binary is in the `cluster` nextest group (`.config/nextest.toml`):
//! each test runs 3 `fdbserver` containers for a minute or more. Run with
//! `cargo nextest run -p fdb-native-client --test cluster --run-ignored only`.
//!
//! * `getKey` across shards: 3000 keys of 1 KB written by `fdbcli` (`write_keys`) on a
//!   cluster with small shards, split into several shards; selectors that cross shard
//!   boundaries forward and backward, with offsets in the thousands (several byte-limited
//!   replies per shard), resolve to what the known key list says.
//! * `getRange` across shards: the same data read whole, forward and reverse, by the reader
//!   (unlimited), by range streams in every streaming mode, with row limits that stop
//!   inside later shards, and with selectors that need `getKey`; checked against the key
//!   list and `fdbcli getrange`.

#![cfg(feature = "tokio")]

use std::sync::Arc;
use std::time::Duration;

use std::future::poll_fn;
use std::pin::Pin;

use fdb_native_client::read::{KEY_SPACE_END, RangeLimits, RangeOptions, StreamingMode};
use fdb_native_client::{Database, KeySelector, KeyValue};
use fdb_native_testkit::FdbCluster;
use futures_core::Stream;

const LIMIT: Duration = Duration::from_secs(120);
const WAIT: Duration = Duration::from_secs(240);
const COUNT: usize = 3000;

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {LIMIT:?}"))
}

fn key(i: usize) -> Vec<u8> {
    format!("k{i:08}").into_bytes()
}

/// What a selector resolves to among `keys`, by definition: `""` before the first, `None`
/// past the last (the system keys, or the end of the key space).
fn reference(keys: &[Vec<u8>], sel: &KeySelector) -> Option<Vec<u8>> {
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
        Some(Vec::new())
    } else if target as usize >= keys.len() {
        None
    } else {
        Some(keys[target as usize].clone())
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn cluster_get_key_resolves_selectors_across_shards() {
    let cluster = Arc::new(
        tokio::task::spawn_blocking(|| FdbCluster::builder().small_shards().start())
            .await
            .unwrap()
            .expect("cluster"),
    );
    println!("{cluster}");
    let shards = {
        let cluster = cluster.clone();
        tokio::task::spawn_blocking(move || {
            cluster.write_keys("k", COUNT, 1000)?;
            cluster.wait_for_partitions(4, WAIT)
        })
        .await
        .unwrap()
        .expect("keys written and split")
    };
    println!("{shards} shards");
    let keys: Vec<Vec<u8>> = (0..COUNT).map(key).collect();

    let db = Database::open(Some(cluster.cluster_file())).expect("database");
    let reader = db.reader();
    let version = within("GRV", reader.get_read_version()).await.unwrap();

    let s = |k: &[u8], or_equal: bool, offset: i32| KeySelector::new(k.to_vec(), or_equal, offset);
    let mut selectors = vec![
        // From the start, across every shard.
        s(b"k", false, 1),
        s(b"k", false, 1500),
        s(b"k", false, 2999),
        s(b"k", false, 3000),
        s(b"k", false, 3001),
        s(b"", false, 100_000),
        // Backward from the end, across every shard.
        s(b"l", false, 0),
        s(b"l", false, -1500),
        s(b"l", false, -2999),
        s(b"l", false, -3000),
        s(b"l", true, -100_000),
        // From the middle, both ways.
        s(&key(1234), false, 1000),
        s(&key(1234), true, -1000),
        s(&key(2000), true, 0),
        s(&key(2000), false, 0),
        s(&key(500), true, 2400),
    ];
    // Every 97th key, a step forward and back over a few hundred keys.
    for i in (0..COUNT).step_by(97) {
        selectors.push(s(&key(i), i % 2 == 0, 300));
        selectors.push(s(&key(i), i % 3 == 0, -300));
        selectors.push(s(&key(i), true, 1));
        selectors.push(s(&key(i), false, 0));
    }
    let tx = db.create_transaction();
    for sel in &selectors {
        let got = within("get_key", reader.get_key(sel, version))
            .await
            .unwrap_or_else(|e| panic!("{sel:?}: {e}"));
        let in_tx = within("get_key", tx.get_key(sel)).await.unwrap();
        match reference(&keys, sel) {
            Some(want) => {
                assert_eq!(got, want, "{sel:?}");
                assert_eq!(in_tx, want, "{sel:?}");
            }
            None => {
                assert!(got.as_slice() > b"\xff".as_slice(), "{sel:?}: {got:?}");
                assert!(got.as_slice() <= KEY_SPACE_END, "{sel:?}: {got:?}");
                assert_eq!(in_tx, b"\xff", "{sel:?}");
            }
        }
    }
    let cached = reader.cached_locations();
    println!(
        "{} selectors, {cached} shard locations cached",
        selectors.len()
    );
    assert!(
        cached >= 3,
        "the selectors crossed several shards ({cached})"
    );

    // fdbcli agrees on a few keys in the middle.
    let out = {
        let cluster = cluster.clone();
        tokio::task::spawn_blocking(move || cluster.fdbcli("getrangekeys k00001500 k00001503"))
            .await
            .unwrap()
            .expect("fdbcli")
    };
    for i in 1500..1503 {
        let want = String::from_utf8(key(i)).unwrap();
        assert!(out.contains(&format!("`{want}'")), "{want} in {out}");
        let got = reader
            .get_key(&KeySelector::first_greater_than(key(i - 1)), version)
            .await
            .unwrap();
        assert_eq!(got, key(i));
    }

    db.shutdown().await;
    drop(db);
    tokio::task::spawn_blocking(move || drop(cluster))
        .await
        .unwrap();
}

async fn collect<S: Stream + Unpin>(mut stream: S) -> Vec<S::Item> {
    let mut out = Vec::new();
    while let Some(item) = within("stream", poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))).await
    {
        out.push(item);
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn cluster_get_range_reads_across_shards() {
    let cluster = Arc::new(
        tokio::task::spawn_blocking(|| FdbCluster::builder().small_shards().start())
            .await
            .unwrap()
            .expect("cluster"),
    );
    println!("{cluster}");
    let shards = {
        let cluster = cluster.clone();
        tokio::task::spawn_blocking(move || {
            cluster.write_keys("k", COUNT, 1000)?;
            cluster.wait_for_partitions(4, WAIT)
        })
        .await
        .unwrap()
        .expect("keys written and split")
    };
    println!("{shards} shards");
    let keys: Vec<Vec<u8>> = (0..COUNT).map(key).collect();
    let mut reversed = keys.clone();
    reversed.reverse();
    let fge = |k: &[u8]| KeySelector::first_greater_or_equal(k.to_vec());
    let keys_of =
        |rows: &[KeyValue]| -> Vec<Vec<u8>> { rows.iter().map(|kv| kv.key.clone()).collect() };

    let db = Database::open(Some(cluster.cluster_file())).expect("database");
    let reader = db.reader();
    let version = within("GRV", reader.get_read_version()).await.unwrap();

    // The reader, unlimited: every shard, both directions (3 MB: several requests a shard).
    for (reverse, want) in [(false, &keys), (true, &reversed)] {
        let r = within(
            "reader",
            reader.get_range_with_limits(
                &fge(b"k"),
                &fge(b"l"),
                RangeLimits::UNLIMITED,
                reverse,
                version,
                false,
            ),
        )
        .await
        .unwrap();
        assert!(!r.more());
        let rows = r.into_key_values();
        assert_eq!(&keys_of(&rows), want, "reverse {reverse}");
        assert!(rows.iter().all(|kv| kv.value.len() == 1000));
    }
    let cached = reader.cached_locations();
    assert!(cached >= 3, "the reads crossed several shards ({cached})");

    // Streams in every mode, both directions.
    let tx = db.create_transaction();
    for mode in [
        StreamingMode::Iterator,
        StreamingMode::Serial,
        StreamingMode::Large,
    ] {
        for (reverse, want) in [(false, &keys), (true, &reversed)] {
            let o = RangeOptions::default().mode(mode).reverse(reverse);
            let rows: Vec<KeyValue> = collect(tx.get_ranges_keyvalues(fge(b"k"), fge(b"l"), o))
                .await
                .into_iter()
                .map(Result::unwrap)
                .collect();
            assert_eq!(&keys_of(&rows), want, "{mode:?} reverse {reverse}");
        }
    }

    // Row limits that end inside later shards, from the middle.
    for (b, n) in [(0, 2500), (700, 1700), (2999, 10)] {
        for reverse in [false, true] {
            let o = RangeOptions::with_limit(n)
                .mode(StreamingMode::Exact)
                .reverse(reverse);
            let (begin, end) = if reverse {
                (fge(b"k"), fge(&key(b)))
            } else {
                (fge(&key(b)), fge(b"l"))
            };
            let r = within("limit", tx.get_range(&begin, &end, &o))
                .await
                .unwrap();
            let want: Vec<Vec<u8>> = if reverse {
                reversed[COUNT - b..]
                    .iter()
                    .take(n as usize)
                    .cloned()
                    .collect()
            } else {
                keys[b..].iter().take(n as usize).cloned().collect()
            };
            assert_eq!(keys_of(&r.kvs), want, "{b} limit {n} reverse {reverse}");
        }
    }

    // Selectors that need `getKey` across shards.
    let s = |k: Vec<u8>, or_equal: bool, offset: i32| KeySelector::new(k, or_equal, offset);
    for (b, e, from, to) in [
        (s(key(100), false, 1000), s(key(2900), false, 0), 1099, 2899),
        (
            s(key(1500), true, -1200),
            s(key(1500), true, 1200),
            300,
            2700,
        ),
        (
            s(b"k".to_vec(), false, 2950),
            s(b"l".to_vec(), false, 5),
            2949,
            COUNT,
        ),
    ] {
        for reverse in [false, true] {
            let o = RangeOptions::with_limit(5000)
                .mode(StreamingMode::Exact)
                .reverse(reverse);
            let r = within("selectors", tx.get_range(&b, &e, &o)).await.unwrap();
            let got: Vec<Vec<u8>> = keys_of(&r.kvs)
                .into_iter()
                .filter(|k| k.starts_with(b"k"))
                .collect();
            let mut want = keys[from..to].to_vec();
            if reverse {
                want.reverse();
            }
            assert_eq!(got, want, "{b:?}..{e:?} reverse {reverse}");
        }
    }

    // fdbcli agrees on a window across a boundary.
    let out = {
        let cluster = cluster.clone();
        tokio::task::spawn_blocking(move || cluster.fdbcli("getrangekeys k00000990 k00002010 2000"))
            .await
            .unwrap()
            .expect("fdbcli")
    };
    let printed = out.matches("`k0000").count();
    assert_eq!(printed, 2010 - 990, "{out}");

    db.shutdown().await;
    drop(db);
    tokio::task::spawn_blocking(move || drop(cluster))
        .await
        .unwrap();
}
