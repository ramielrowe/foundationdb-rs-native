//! Versionstamped operations against a real, disposable FDB server started by
//! `fdb-native-testkit` (image from `FDB_IMAGE`, default 7.4.10), checked with `fdbcli`: the
//! keys and values the proxy completes carry the stamp `Transaction::versionstamp()` reports,
//! stamps of later commits sort after earlier ones (also within one commit batch), and a key
//! packed by the tuple layer unpacks to the committed stamp. Run with
//! `cargo nextest run -p fdb-native-client --run-ignored only` (and `FDB_IMAGE=...:8.0.1`).

#![cfg(feature = "tokio")]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use fdb_native_client::commit::with_versionstamp_offset;
use fdb_native_client::{Database, MutationType};
use fdb_native_testkit::FdbContainer;
use fdb_native_tuple::{
    Element, Subspace, Versionstamp, pack, pack_with_versionstamp, tuple, unpack,
};

const LIMIT: Duration = Duration::from_secs(120);

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {LIMIT:?}"))
}

/// `printable()` (`flow`): how `fdbcli` prints keys and values.
fn printable(bytes: &[u8]) -> String {
    let mut out = String::new();
    for &b in bytes {
        match b {
            b'\\' => out.push_str("\\\\"),
            32..=126 => out.push(b as char),
            _ => out.push_str(&format!("\\x{b:02x}")),
        }
    }
    out
}

/// The inverse of [`printable`].
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
            assert_eq!(bytes[i + 1], b'x', "{text}");
            out.push(u8::from_str_radix(&text[i + 2..i + 4], 16).unwrap());
            i += 4;
        }
    }
    out
}

/// Every byte as `\xNN`, for an `fdbcli` command.
fn escaped(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("\\x{b:02x}")).collect()
}

/// The `(key, value)` pairs of `fdbcli`'s `getrange` output.
fn parse_getrange(out: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    out.lines()
        .filter_map(|line| {
            let rest = line.strip_prefix('`')?;
            let (key, value) = rest.split_once("' is `")?;
            Some((unprintable(key), unprintable(value.strip_suffix('\'')?)))
        })
        .collect()
}

struct Cluster {
    fdb: Arc<FdbContainer>,
    db: Database,
}

impl Cluster {
    async fn start() -> Cluster {
        let fdb = tokio::task::spawn_blocking(FdbContainer::start)
            .await
            .unwrap()
            .expect("container");
        println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
        let db = Database::open(Some(fdb.cluster_file())).expect("database");
        Cluster {
            fdb: Arc::new(fdb),
            db,
        }
    }

    async fn cli(&self, command: &str) -> String {
        let fdb = self.fdb.clone();
        let command = command.to_owned();
        tokio::task::spawn_blocking(move || fdb.fdbcli(&command))
            .await
            .unwrap()
            .unwrap_or_else(|e| panic!("fdbcli: {e}"))
    }

    /// `fdbcli`'s `getrange begin end`, parsed.
    async fn getrange(&self, begin: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let out = self
            .cli(&format!(
                "getrange {} {} 1000",
                escaped(begin),
                escaped(end)
            ))
            .await;
        parse_getrange(&out)
    }

    async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let key = key.to_vec();
        within(
            "get",
            self.db.run(move |tx, _| {
                let key = key.clone();
                async move { tx.get(&key).await }
            }),
        )
        .await
        .expect("get")
    }

    async fn finish(self) {
        self.db.shutdown().await;
        let Cluster { fdb, db } = self;
        drop(db);
        tokio::task::spawn_blocking(move || drop(fdb))
            .await
            .unwrap();
    }
}

/// `prefix`, a placeholder for the stamp, `tail`, with the offset appended.
fn stamped_key(prefix: &[u8], tail: &[u8]) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(&[0xff; 10]);
    key.extend_from_slice(tail);
    with_versionstamp_offset(key, u32::try_from(prefix.len()).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_versionstamped_keys_and_values_carry_the_stamp_of_the_commit() {
    let c = Cluster::start().await;
    let tx = c.db.create_transaction();
    let stamp_future = tx.get_versionstamp();
    tx.set_versionstamped_key(&stamped_key(b"vs/key/", b"/tail"), b"key value");
    // The placeholder is inside the value: `pre`, ten bytes, `post`.
    let mut value = b"pre:".to_vec();
    value.extend_from_slice(&[0; 10]);
    value.extend_from_slice(b":post");
    tx.set_versionstamped_value(b"vs/value", &with_versionstamp_offset(value, 4));
    // A plain write and an atomic operation in the same transaction.
    tx.set(b"vs/plain", b"plain");
    tx.atomic_op(b"vs/counter", &1u64.to_le_bytes(), MutationType::Add);
    let result = within("commit", tx.commit()).await.expect("commit");
    let stamp = result.versionstamp().expect("a versionstamp");
    assert_eq!(&stamp[..8], &result.committed_version().to_be_bytes());
    assert_eq!(tx.versionstamp(), Some(stamp));
    assert_eq!(within("stamp", stamp_future).await.unwrap(), stamp);
    assert_eq!(within("stamp", tx.get_versionstamp()).await.unwrap(), stamp);

    // fdbcli sees the completed key (the offset is gone) and value.
    let expected_key = [&b"vs/key/"[..], &stamp, b"/tail"].concat();
    let expected_value = [&b"pre:"[..], &stamp, b":post"].concat();
    let rows = c.getrange(b"vs/", b"vs0").await;
    println!("{rows:?}");
    assert_eq!(
        rows,
        vec![
            (b"vs/counter".to_vec(), 1u64.to_le_bytes().to_vec()),
            (expected_key.clone(), b"key value".to_vec()),
            (b"vs/plain".to_vec(), b"plain".to_vec()),
            (b"vs/value".to_vec(), expected_value.clone()),
        ]
    );
    let out = c.cli("getrange vs/key/ vs/key0").await;
    assert!(
        out.contains(&format!("`{}' is `key value'", printable(&expected_key))),
        "{out}"
    );
    // And the client reads them back.
    assert_eq!(
        c.get(&expected_key).await.as_deref(),
        Some(&b"key value"[..])
    );
    assert_eq!(c.get(b"vs/value").await, Some(expected_value));

    // fdbcli reads its own versionstamp stamp too: a second transaction gets a later one.
    let tx = c.db.create_transaction();
    tx.set_versionstamped_key(&stamped_key(b"vs/key/", b"/tail"), b"second");
    let second = within("commit", tx.commit()).await.expect("commit");
    assert!(second.versionstamp().unwrap() > stamp);
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_stamps_order_commits_also_within_a_batch() {
    const N: usize = 24;
    let c = Cluster::start().await;

    // One after the other: later commits get greater stamps and keys sort in commit order.
    let mut sequential = Vec::new();
    for i in 0..3u8 {
        let tx = c.db.create_transaction();
        tx.set_versionstamped_key(&stamped_key(b"ord/seq/", &[b'/', i]), &[i]);
        let result = within("commit", tx.commit()).await.expect("commit");
        sequential.push(result.versionstamp().unwrap());
    }
    assert!(sequential.windows(2).all(|w| w[0] < w[1]), "{sequential:?}");
    let rows = c.getrange(b"ord/seq/", b"ord/seq0").await;
    assert_eq!(rows.len(), 3);
    for (i, ((key, value), stamp)) in rows.iter().zip(&sequential).enumerate() {
        assert_eq!(key, &[&b"ord/seq/"[..], stamp, &[b'/', i as u8]].concat());
        assert_eq!(value, &[i as u8]);
    }

    // Many at once: commits that share a batch differ in the 2-byte batch index; every stamp
    // is distinct and the keys in the database are in stamp order.
    let tasks: Vec<_> = (0..N)
        .map(|i| {
            let db = c.db.clone();
            tokio::spawn(async move {
                let tx = db.create_transaction();
                tx.set_versionstamped_key(&stamped_key(b"ord/par/", b""), &[i as u8]);
                let stamp = tx.get_versionstamp();
                let result = within("commit", tx.commit()).await.expect("commit");
                let from_future = within("stamp", stamp).await.unwrap();
                assert_eq!(Some(from_future), result.versionstamp());
                (from_future, i as u8)
            })
        })
        .collect();
    let mut committed = Vec::new();
    for t in tasks {
        committed.push(t.await.unwrap());
    }
    let distinct: HashSet<_> = committed.iter().map(|(s, _)| *s).collect();
    assert_eq!(distinct.len(), N, "stamps are unique");
    committed.sort();
    let rows = c.getrange(b"ord/par/", b"ord/par0").await;
    assert_eq!(rows.len(), N);
    for ((key, value), (stamp, i)) in rows.iter().zip(&committed) {
        assert_eq!(key, &[&b"ord/par/"[..], stamp].concat());
        assert_eq!(value, &[*i]);
    }
    println!(
        "{N} concurrent commits: {} distinct versions",
        committed
            .iter()
            .map(|(s, _)| s[..8].to_vec())
            .collect::<HashSet<_>>()
            .len()
    );
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_a_tuple_packed_versionstamped_key_unpacks_to_the_committed_stamp() {
    let c = Cluster::start().await;
    let space = Subspace::new(b"tup/".to_vec());

    // `("event", <stamp>, 7, "x")` in a subspace, and a bare tuple.
    let tx = c.db.create_transaction();
    let key = space
        .pack_with_versionstamp(&tuple!["event", Versionstamp::incomplete(7), "x"])
        .unwrap();
    tx.set_versionstamped_key(&key, b"in subspace");
    let bare = pack_with_versionstamp(&tuple![Versionstamp::incomplete(1), "bare"]).unwrap();
    tx.set_versionstamped_key(&bare, b"bare");
    let result = within("commit", tx.commit()).await.expect("commit");
    let stamp = result.versionstamp().unwrap();

    // Read back what is in the database with fdbcli and unpack it.
    let (begin, end) = space.range_all();
    let rows = c.getrange(&begin, &end).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    let (stored, value) = &rows[0];
    assert_eq!(value, b"in subspace");
    let unpacked = space.unpack(stored).unwrap();
    assert_eq!(
        unpacked.as_slice(),
        &[
            Element::from("event"),
            Element::from(Versionstamp::complete(stamp, 7)),
            Element::from("x")
        ]
    );
    // It is the key the tuple layer packs once the stamp is known.
    let known = space
        .pack(&tuple!["event", Versionstamp::complete(stamp, 7), "x"])
        .unwrap();
    assert_eq!(stored, &known);
    assert_eq!(c.get(&known).await.as_deref(), Some(&b"in subspace"[..]));

    // A key without a subspace: it starts with the stamp, whose offset is 1.
    assert_eq!(&bare[bare.len() - 4..], &[1, 0, 0, 0]);
    let bare_rows = c.getrange(&[0x33], &[0x34]).await;
    assert_eq!(bare_rows.len(), 1, "{bare_rows:?}");
    let stored = &bare_rows[0].0;
    assert_eq!(
        unpack(stored).unwrap().as_slice(),
        &[
            Element::from(Versionstamp::complete(stamp, 1)),
            Element::from("bare")
        ]
    );
    assert_eq!(
        stored,
        &pack(&tuple![Versionstamp::complete(stamp, 1), "bare"]).unwrap()
    );
    c.finish().await;
}
