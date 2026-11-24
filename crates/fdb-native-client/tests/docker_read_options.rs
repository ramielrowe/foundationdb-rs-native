//! The read options (`READ_PRIORITY_*`, `READ_SERVER_SIDE_CACHE_*`, and the `lockAware` of
//! `LOCK_AWARE` / `READ_LOCK_AWARE`) against a real, disposable FDB server started by
//! `fdb-native-testkit` (image from `FDB_IMAGE`, default 7.4.10; also run with 8.0.1): every
//! kind of storage read (`GetValue`, `GetKey`, `GetKeyValues`, with read-your-writes on and
//! off, snapshot or not) succeeds with each option and returns what it returns without, and
//! the storage server is still serving afterwards. What the requests carry is checked by
//! `tests/read_options.rs` and, against the releases' own readers, by the golden vectors.
//! Run with `cargo nextest run -p fdb-native-client --run-ignored only`.

#![cfg(feature = "tokio")]

use std::time::Duration;

use fdb_native_client::read::{RangeOptions, StreamingMode};
use fdb_native_client::{Database, KeySelector, KeyValue, Transaction, TransactionOption};
use fdb_native_testkit::FdbContainer;

const LIMIT: Duration = Duration::from_secs(120);

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {LIMIT:?}"))
}

/// What one round of reads returns.
#[derive(Debug, PartialEq, Eq)]
struct Reads {
    value: Option<Vec<u8>>,
    missing: Option<Vec<u8>>,
    key: Vec<u8>,
    key_back: Vec<u8>,
    range: Vec<KeyValue>,
    reverse: Vec<KeyValue>,
    snapshot_value: Option<Vec<u8>>,
}

/// Every row between `begin` and `end`, batch after batch (a read-your-writes range read
/// may return a batch early, with `more`).
async fn whole_range(
    tx: &Transaction,
    begin: &KeySelector,
    end: &KeySelector,
    options: &RangeOptions,
) -> Vec<KeyValue> {
    let (mut begin, mut end) = (begin.clone(), end.clone());
    let mut rows = Vec::new();
    loop {
        let batch = tx.get_range(&begin, &end, options).await.unwrap();
        let more = batch.more;
        let last = batch.kvs.last().map(|kv| kv.key.clone());
        rows.extend(batch.kvs);
        match last {
            Some(last) if more => {
                if options.reverse {
                    end = KeySelector::first_greater_or_equal(last);
                } else {
                    begin = KeySelector::first_greater_than(last);
                }
            }
            _ => return rows,
        }
    }
}

/// The reads of one transaction with `options` set (and read-your-writes disabled first if
/// `plain`).
async fn reads(db: &Database, options: &[TransactionOption], plain: bool) -> Reads {
    let tx = db.create_transaction();
    if plain {
        tx.set_option(TransactionOption::ReadYourWritesDisable)
            .unwrap();
    }
    for o in options {
        tx.set_option(o.clone()).unwrap();
    }
    let all = RangeOptions::default().mode(StreamingMode::WantAll);
    let begin = KeySelector::first_greater_or_equal(b"ro/".to_vec());
    let end = KeySelector::first_greater_or_equal(b"ro0".to_vec());
    let what = format!("{options:?} (plain {plain})");
    Reads {
        value: within(&what, tx.get(b"ro/05")).await.unwrap(),
        missing: within(&what, tx.get(b"ro/missing")).await.unwrap(),
        key: within(
            &what,
            tx.get_key(&KeySelector::new(b"ro/05".to_vec(), false, 3)),
        )
        .await
        .unwrap(),
        key_back: within(
            &what,
            tx.get_key(&KeySelector::last_less_than(b"ro/05".to_vec())),
        )
        .await
        .unwrap(),
        range: within(&what, whole_range(&tx, &begin, &end, &all)).await,
        reverse: within(&what, whole_range(&tx, &begin, &end, &all.reverse(true))).await,
        snapshot_value: within(&what, tx.snapshot().get(b"ro/07")).await.unwrap(),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_reads_with_every_read_option() {
    use TransactionOption::*;
    let fdb = tokio::task::spawn_blocking(FdbContainer::start)
        .await
        .unwrap()
        .expect("container");
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
    let db = Database::open(Some(fdb.cluster_file())).expect("database");

    within(
        "write",
        db.run(|tx, _| async move {
            for i in 0..20u8 {
                tx.set(format!("ro/{i:02}").as_bytes(), &[i; 3]);
            }
            Ok(())
        }),
    )
    .await
    .unwrap();

    let cases: Vec<Vec<TransactionOption>> = vec![
        vec![ReadPriorityLow],
        vec![ReadPriorityNormal],
        vec![ReadPriorityHigh],
        vec![ReadServerSideCacheEnable],
        vec![ReadServerSideCacheDisable],
        vec![ReadPriorityLow, ReadServerSideCacheDisable],
        vec![ReadPriorityHigh, ReadServerSideCacheDisable],
        vec![LockAware],
        vec![ReadLockAware, ReadPriorityHigh],
        vec![LockAware, ReadPriorityLow, ReadServerSideCacheDisable],
    ];
    for plain in [true, false] {
        let want = reads(&db, &[], plain).await;
        assert_eq!(want.value, Some(vec![5; 3]));
        assert_eq!(want.missing, None);
        assert_eq!(want.key, b"ro/07".to_vec());
        assert_eq!(want.key_back, b"ro/04".to_vec());
        assert_eq!(want.range.len(), 20);
        assert_eq!(want.reverse.len(), 20);
        assert_eq!(want.reverse.first(), want.range.last());
        for options in &cases {
            assert_eq!(
                reads(&db, options, plain).await,
                want,
                "{options:?} (plain {plain})"
            );
        }
    }

    // A whole scan at low priority without caching, through the range stream.
    let tx = db.create_transaction();
    tx.set_option(ReadPriorityLow).unwrap();
    tx.set_option(ReadServerSideCacheDisable).unwrap();
    let rows = within(
        "scan",
        tx.get_range(
            &KeySelector::first_greater_or_equal(b"".to_vec()),
            &KeySelector::first_greater_or_equal(b"\xff".to_vec()),
            &RangeOptions::default().mode(StreamingMode::WantAll),
        ),
    )
    .await
    .unwrap();
    assert!(rows.kvs.len() >= 20);

    // The storage server is still serving, to a transaction without options too.
    let fresh = db.create_transaction();
    assert_eq!(
        within("after", fresh.get(b"ro/19")).await.unwrap(),
        Some(vec![19; 3])
    );
    let status = tokio::task::spawn_blocking({
        let fdb = std::sync::Arc::new(fdb);
        move || {
            let out = fdb.fdbcli("status minimal");
            drop(fdb);
            out
        }
    })
    .await
    .unwrap()
    .expect("fdbcli");
    assert!(status.contains("available"), "{status}");
    db.shutdown().await;
}
