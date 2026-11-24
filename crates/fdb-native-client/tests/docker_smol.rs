//! `Database` and `Transaction` on the smol runtime (`SmolRuntime`, feature `smol`) against a
//! real, disposable FDB server started by `fdb-native-testkit` (image from `FDB_IMAGE`,
//! default 7.4.10): `Database::open_with(SmolRuntime::new(), ..)`, reads and writes, a range
//! read in several batches, the range streams, a watch, atomic operations from many
//! concurrent `run` loops (conflicts retried on smol timers) and shutdown. There is no tokio
//! in this binary: the futures are driven by `futures_lite::future::block_on` and every task
//! and timer of the client is smol's. Run with `cargo nextest run -p fdb-native-client
//! --features smol --run-ignored only -E 'binary(docker_smol)'`.

#![cfg(feature = "smol")]

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use fdb_native_client::runtime::{Runtime, SmolRuntime};
use fdb_native_client::{
    Database, DatabaseOptions, KeySelector, MutationType, RangeOptions, StreamingMode,
};
use fdb_native_testkit::FdbContainer;
use futures_lite::StreamExt;
use futures_lite::future::{block_on, or};

/// The `smol` example's `run`, against the same container.
#[allow(dead_code)]
#[path = "../examples/smol.rs"]
mod smol_example;

const LIMIT: Duration = Duration::from_secs(60);

async fn within<T>(rt: &SmolRuntime, what: &str, f: impl Future<Output = T>) -> T {
    or(async { Some(f.await) }, async {
        rt.sleep(LIMIT).await;
        None
    })
    .await
    .unwrap_or_else(|| panic!("{what}: nothing within {LIMIT:?}"))
}

#[test]
#[ignore = "requires docker"]
fn docker_smol_database_reads_writes_ranges_watches_and_atomics() {
    // Start the container outside any async context (the testkit is blocking).
    let fdb = FdbContainer::start().expect("container");
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());

    block_on(async {
        let rt = SmolRuntime::new();
        let db = Database::open_with(
            rt.clone(),
            Some(fdb.cluster_file()),
            DatabaseOptions::default(),
        )
        .expect("database");

        // A write and a read, in two transactions.
        within(
            &rt,
            "set",
            db.run(|tx, _| async move {
                tx.set(b"smol/k", b"v1");
                Ok(())
            }),
        )
        .await
        .unwrap();
        let got = within(
            &rt,
            "get",
            db.run(|tx, _| async move { tx.get(b"smol/k").await }),
        )
        .await
        .unwrap();
        assert_eq!(got.as_deref(), Some(&b"v1"[..]));
        // A key that is not there, and the transaction's own write.
        let tx = db.create_transaction();
        assert_eq!(
            within(&rt, "get missing", tx.get(b"smol/missing"))
                .await
                .unwrap(),
            None
        );
        tx.set(b"smol/mine", b"x");
        assert_eq!(
            within(&rt, "get own", tx.get(b"smol/mine"))
                .await
                .unwrap()
                .as_deref(),
            Some(&b"x"[..])
        );
        drop(tx);

        // 600 keys, then a range read in several batches through the streams, and one
        // batch with a limit, in reverse.
        for chunk in 0..6 {
            within(
                &rt,
                "fill",
                db.run(move |tx, _| async move {
                    for i in chunk * 100..(chunk + 1) * 100 {
                        tx.set(
                            format!("smol/range/{i:04}").as_bytes(),
                            format!("value {i}").as_bytes(),
                        );
                    }
                    Ok(())
                }),
            )
            .await
            .unwrap();
        }
        let begin = KeySelector::first_greater_or_equal(b"smol/range/");
        let end = KeySelector::first_greater_or_equal(b"smol/range0");
        let count = within(
            &rt,
            "stream",
            db.run(|tx, _| {
                let (begin, end) = (begin.clone(), end.clone());
                async move {
                    let mut rows = tx.get_ranges_keyvalues(begin, end, RangeOptions::default());
                    let mut count = 0;
                    while let Some(row) = rows.next().await {
                        let row = row?;
                        assert_eq!(row.key, format!("smol/range/{count:04}").as_bytes());
                        count += 1;
                    }
                    Ok(count)
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(count, 600);
        let tx = db.create_transaction();
        let last = within(
            &rt,
            "reverse limit",
            tx.get_range(
                &begin,
                &end,
                &RangeOptions::with_limit(5)
                    .reverse(true)
                    .mode(StreamingMode::WantAll),
            ),
        )
        .await
        .unwrap();
        assert_eq!(last.kvs.len(), 5);
        assert!(last.more);
        assert_eq!(last.kvs[0].key, &b"smol/range/0599"[..]);
        drop(tx);

        // A watch fires when another transaction changes the key.
        let tx = db.create_transaction();
        let watch = within(&rt, "watch", tx.watch(b"smol/watched"))
            .await
            .unwrap();
        within(&rt, "commit", tx.commit()).await.unwrap();
        within(
            &rt,
            "change",
            db.run(|tx, _| async move {
                tx.set(b"smol/watched", b"changed");
                Ok(())
            }),
        )
        .await
        .unwrap();
        within(&rt, "the watch", watch).await.unwrap();

        // Concurrent atomic increments plus read-modify-write transactions: tasks on the smol
        // executor, conflicts retried after smol-timer backoffs. The counter ends exact.
        let db = Arc::new(db);
        let tasks: Vec<_> = (0..20)
            .map(|i| {
                let db = db.clone();
                rt.spawn(async move {
                    db.run(move |tx, _| async move {
                        tx.atomic_op(b"smol/atomic", &1_i64.to_le_bytes(), MutationType::Add);
                        // A read-modify-write on one key: these conflict and retry.
                        let n = tx
                            .get(b"smol/rmw")
                            .await?
                            .map_or(0, |v| String::from_utf8(v).unwrap().parse::<u64>().unwrap());
                        tx.set(b"smol/rmw", (n + 1).to_string().as_bytes());
                        let _ = i;
                        Ok(())
                    })
                    .await
                })
            })
            .collect();
        for task in tasks {
            within(&rt, "a concurrent transaction", task)
                .await
                .unwrap()
                .unwrap();
        }
        let tx = db.create_transaction();
        let atomic = within(&rt, "read atomic", tx.get(b"smol/atomic"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_le_bytes(atomic.try_into().unwrap()), 20);
        let rmw = within(&rt, "read rmw", tx.get(b"smol/rmw"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rmw, b"20");
        drop(tx);

        within(&rt, "the smol example", smol_example::run(&db))
            .await
            .expect("the smol example");

        // Clear it all and shut down.
        within(
            &rt,
            "cleanup",
            db.run(|tx, _| async move {
                tx.clear_range(b"smol/", b"smol0");
                Ok(())
            }),
        )
        .await
        .unwrap();
        within(&rt, "shutdown", db.shutdown()).await;
    });

    drop(fdb);
}
