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

#![cfg(feature = "tokio")]

use std::sync::Arc;
use std::time::Duration;

use fdb_native_client::read::KEY_SPACE_END;
use fdb_native_client::{Database, KeySelector};
use fdb_native_testkit::FdbCluster;

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
