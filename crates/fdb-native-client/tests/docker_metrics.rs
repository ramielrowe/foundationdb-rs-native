//! The storage metrics reads (`get_estimated_range_size_bytes`, `get_range_split_points`)
//! against a real, disposable FDB server started by `fdb-native-testkit` (image from
//! `FDB_IMAGE`, default 7.4.10): 10 MB written by the client, then estimates of the whole
//! and of parts (the servers' estimates come from a byte sample, so the checks are bounds
//! and monotonicity, not exact values) and split points (sorted, inside the range, from
//! `begin` to `end`, about one per chunk). The multi-shard case is in the `cluster` test
//! binary. Run with `cargo nextest run -p fdb-native-client --run-ignored only` (and
//! `FDB_IMAGE=foundationdb/foundationdb:8.0.1`).

#![cfg(feature = "tokio")]

use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::{Database, TransactionOption};
use fdb_native_testkit::FdbContainer;

const LIMIT: Duration = Duration::from_secs(120);
const COUNT: usize = 1000;
const VALUE: usize = 10_000;

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {LIMIT:?}"))
}

fn key(i: usize) -> Vec<u8> {
    format!("sm/{i:05}").into_bytes()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_storage_metrics_estimate_and_split() {
    let fdb = tokio::task::spawn_blocking(FdbContainer::start)
        .await
        .unwrap()
        .expect("container");
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
    let db = Database::open(Some(fdb.cluster_file())).expect("database");

    // Nothing there yet.
    let tx = db.create_transaction();
    assert_eq!(
        within(
            "estimate",
            tx.get_estimated_range_size_bytes(b"sm/", b"sm0")
        )
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        within(
            "split",
            tx.get_range_split_points(b"sm/", b"sm0", 1_000_000)
        )
        .await
        .unwrap(),
        vec![b"sm/".to_vec(), b"sm0".to_vec()]
    );

    // 10 MB, 500 keys a transaction.
    for batch in 0..COUNT / 500 {
        within(
            "write",
            db.run(move |tx, _| async move {
                for i in batch * 500..(batch + 1) * 500 {
                    tx.set(&key(i), &[b'x'; VALUE]);
                }
                Ok(())
            }),
        )
        .await
        .expect("write");
    }
    let total = (COUNT * (VALUE + key(0).len())) as i64;

    // The byte sample follows the writes once the storage server applied them.
    let estimate = |begin: Vec<u8>, end: Vec<u8>| {
        let db = db.clone();
        async move {
            within(
                "estimate",
                db.create_transaction()
                    .get_estimated_range_size_bytes(&begin, &end),
            )
            .await
            .unwrap()
        }
    };
    let mut whole = 0;
    for _ in 0..100 {
        whole = estimate(b"sm/".to_vec(), b"sm0".to_vec()).await;
        if whole >= total / 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("estimate {whole} for {total} bytes");
    assert!(
        (total / 2..=total * 2).contains(&whole),
        "estimate {whole} for {total} bytes"
    );
    // Parts: within the whole, the halves about half, outside nothing.
    let first = estimate(key(0), key(COUNT / 2)).await;
    let second = estimate(key(COUNT / 2), b"sm0".to_vec()).await;
    let quarter = estimate(key(0), key(COUNT / 4)).await;
    println!("halves {first} + {second}, quarter {quarter}");
    assert!(first <= whole && second <= whole && quarter <= first);
    assert!((total / 4..=total).contains(&first), "{first} of {total}");
    assert!((total / 4..=total).contains(&second), "{second} of {total}");
    assert_eq!(estimate(b"sm0".to_vec(), b"sm1".to_vec()).await, 0);
    assert_eq!(estimate(b"a".to_vec(), b"sm/".to_vec()).await, 0);
    // The whole key space, system keys included, needs no option.
    let all = estimate(Vec::new(), b"\xff\xff".to_vec()).await;
    assert!(all >= whole, "{all} < {whole}");

    // Split points: from begin to end, sorted, about one a megabyte.
    let tx = db.create_transaction();
    for chunk in [1_000_000i64, 3_000_000, 100_000_000] {
        let points = within("split", tx.get_range_split_points(b"sm/", b"sm0", chunk))
            .await
            .unwrap();
        println!("chunk {chunk}: {} points", points.len());
        assert_eq!(points.first().map(Vec::as_slice), Some(&b"sm/"[..]));
        assert_eq!(points.last().map(Vec::as_slice), Some(&b"sm0"[..]));
        assert!(points.windows(2).all(|w| w[0] < w[1]), "{points:?}");
        let inner = &points[1..points.len() - 1];
        assert!(inner.iter().all(|k| k.starts_with(b"sm/")), "{points:?}");
        let expected = (whole / chunk) as usize;
        assert!(
            inner.len() <= expected + 2 && inner.len() + 2 >= expected / 2,
            "{} inner points for {whole} bytes in chunks of {chunk}",
            inner.len()
        );
        if chunk == 100_000_000 {
            assert!(inner.is_empty());
        }
    }
    // A part of the range.
    let points = within(
        "split",
        tx.get_range_split_points(&key(100), &key(300), 500_000),
    )
    .await
    .unwrap();
    assert_eq!(points.first(), Some(&key(100)));
    assert_eq!(points.last(), Some(&key(300)));
    assert!(points.windows(2).all(|w| w[0] < w[1]), "{points:?}");
    assert!(points.len() >= 3, "{points:?}");

    // The checks.
    let code = |r: Result<Vec<Vec<u8>>, fdb_native_client::Error>| r.unwrap_err().code();
    assert_eq!(
        code(tx.get_range_split_points(b"b", b"a", 1000).await),
        codes::INVERTED_RANGE
    );
    assert_eq!(
        code(tx.get_range_split_points(b"a", b"\xff\x01", 1000).await),
        codes::KEY_OUTSIDE_LEGAL_RANGE
    );
    assert_eq!(
        code(tx.get_range_split_points(b"a", b"b", 0).await),
        codes::CLIENT_INVALID_OPERATION
    );
    let sys = db.create_transaction();
    sys.set_option(TransactionOption::ReadSystemKeys).unwrap();
    let points = within(
        "split",
        sys.get_range_split_points(b"\xff", b"\xff\xff", 1_000_000_000),
    )
    .await
    .unwrap();
    assert_eq!(points.first().map(Vec::as_slice), Some(&b"\xff"[..]));
    assert_eq!(points.last().map(Vec::as_slice), Some(&b"\xff\xff"[..]));

    // In a retry loop, alongside reads and writes: no read version, no conflict.
    let (size, points) = within(
        "run",
        db.run(|tx, _| async move {
            let size = tx.get_estimated_range_size_bytes(b"sm/", b"sm0");
            let points = tx.get_range_split_points(b"sm/", b"sm0", 2_000_000);
            tx.set(b"sm-done", b"1");
            Ok((size.await?, points.await?))
        }),
    )
    .await
    .unwrap();
    assert!(size >= total / 2 && points.len() >= 3, "{size} {points:?}");

    db.shutdown().await;
    tokio::task::spawn_blocking(move || drop(fdb))
        .await
        .unwrap();
}
