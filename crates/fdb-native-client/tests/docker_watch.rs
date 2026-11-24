//! Watches against a real, disposable FDB server started by `fdb-native-testkit` (image from
//! `FDB_IMAGE`, default 7.4.10), changed with `fdbcli`: a watch fires when the value changes
//! (a set, a clear), does not fire for a set of the same value or for the watching
//! transaction's own commit, starts at the commit of a read-only transaction too, is
//! cancelled by dropping it, and the cap refuses one more watch with `too_many_watches`;
//! two transactions that increment a counter and watch it concurrently either conflict or
//! fire each other's watches. Run with `cargo nextest run -p fdb-native-client --run-ignored
//! only`.

#![cfg(feature = "tokio")]

use std::sync::Arc;
use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::watch::WatchConfig;
use fdb_native_client::{Database, DatabaseOptions, MutationType, Watch};
use fdb_native_testkit::FdbContainer;
use fdb_native_transport::runtime::TokioRuntime;

const LIMIT: Duration = Duration::from_secs(60);
/// How long a watch must stay pending for us to believe it will not fire.
const QUIET: Duration = Duration::from_secs(3);

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {LIMIT:?}"))
}

struct Cluster {
    fdb: Arc<FdbContainer>,
    db: Database,
}

impl Cluster {
    async fn start() -> Cluster {
        Cluster::start_with(DatabaseOptions::default()).await
    }

    async fn start_with(options: DatabaseOptions) -> Cluster {
        let fdb = tokio::task::spawn_blocking(FdbContainer::start)
            .await
            .unwrap()
            .expect("container");
        println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
        let db = Database::open_with(TokioRuntime::new(), Some(fdb.cluster_file()), options)
            .expect("database");
        Cluster {
            fdb: Arc::new(fdb),
            db,
        }
    }

    async fn cli(&self, command: &str) -> String {
        let fdb = self.fdb.clone();
        let command = format!("writemode on; {command}");
        let out = tokio::task::spawn_blocking(move || fdb.fdbcli(&command))
            .await
            .unwrap()
            .unwrap_or_else(|e| panic!("fdbcli: {e}"));
        assert!(out.contains("Committed"), "{out}");
        out
    }

    /// A watch of `key`, registered by a transaction that read it.
    async fn watch(&self, key: &'static [u8]) -> Watch {
        within(
            "watch",
            self.db.run(move |tx, _| async move { tx.watch(key).await }),
        )
        .await
        .expect("watch")
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

/// Polls `cond` until it holds.
async fn until(what: &str, mut cond: impl FnMut() -> bool) {
    within(what, async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
}

async fn stays_pending(watch: &mut Watch) {
    if let Ok(result) = tokio::time::timeout(QUIET, &mut *watch).await {
        panic!("the watch fired: {result:?}");
    }
}

async fn fires(what: &str, watch: &mut Watch) {
    within(what, watch)
        .await
        .unwrap_or_else(|e| panic!("{what}: {e}"));
}

/// `\xff/metadataVersion` may be watched without the system key options (as C++ allows),
/// and a transaction that bumps it (the versionstamped value `metadataVersionRequiredValue`)
/// fires the watch.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_watch_of_the_metadata_version_fires_when_it_is_bumped() {
    const KEY: &[u8] = b"\xff/metadataVersion";
    let c = Cluster::start().await;
    let mut watch = c.watch(KEY).await;
    stays_pending(&mut watch).await;
    within(
        "bump",
        c.db.run(|tx, _| async move {
            tx.atomic_op(KEY, &[0; 14], MutationType::SetVersionstampedValue);
            Ok(())
        }),
    )
    .await
    .expect("bump");
    fires("the metadata version bump", &mut watch).await;
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_watch_fires_when_fdbcli_changes_the_value() {
    let c = Cluster::start().await;

    // A key that is not set: a set fires the watch.
    let mut watch = c.watch(b"watch/a").await;
    stays_pending(&mut watch).await;
    c.cli("set watch/a one").await;
    fires("set", &mut watch).await;

    // A set of the value it already has does not; another value does.
    let mut watch = c.watch(b"watch/a").await;
    stays_pending(&mut watch).await;
    c.cli("set watch/a one").await;
    stays_pending(&mut watch).await;
    c.cli("set watch/a two").await;
    fires("a different value", &mut watch).await;

    // A clear does.
    let mut watch = c.watch(b"watch/a").await;
    c.cli("clear watch/a").await;
    fires("clear", &mut watch).await;

    // A clear of a key that is not set does not (the value is what it was).
    let mut watch = c.watch(b"watch/a").await;
    c.cli("clear watch/a").await;
    stays_pending(&mut watch).await;
    drop(watch);

    // A read-only transaction registers its watch at its commit.
    c.cli("set watch/ro before").await;
    let tx = c.db.create_transaction();
    let mut watch = tx.watch(b"watch/ro").await.unwrap();
    stays_pending(&mut watch).await;
    assert_eq!(
        within("commit", tx.commit())
            .await
            .unwrap()
            .committed_version(),
        -1,
        "nothing written"
    );
    stays_pending(&mut watch).await;
    c.cli("set watch/ro after").await;
    fires("read-only", &mut watch).await;

    // The value written by the watching transaction is the value watched: its own commit
    // does not fire the watch, a later change does.
    let mut watch = within(
        "write and watch",
        c.db.run(|tx, _| async move {
            tx.set(b"watch/w", b"mine");
            tx.watch(b"watch/w").await
        }),
    )
    .await
    .unwrap();
    stays_pending(&mut watch).await;
    c.cli("set watch/w mine").await;
    stays_pending(&mut watch).await;
    c.cli("set watch/w theirs").await;
    fires("own write", &mut watch).await;

    // A pending atomic operation: the value watched is the one the commit made.
    let mut watch = within(
        "atomic and watch",
        c.db.run(|tx, _| async move {
            tx.atomic_op(b"watch/n", &1u64.to_le_bytes(), MutationType::Add);
            tx.watch(b"watch/n").await
        }),
    )
    .await
    .unwrap();
    stays_pending(&mut watch).await;
    c.cli(r"set watch/n \x01\x00\x00\x00\x00\x00\x00\x00").await;
    stays_pending(&mut watch).await;
    c.cli("set watch/n changed").await;
    fires("atomic", &mut watch).await;

    // Writes of the watching transaction after the watch: a different value fires it at
    // once, without a commit.
    let tx = c.db.create_transaction();
    let mut watch = tx.watch(b"watch/x").await.unwrap();
    tx.set(b"watch/x", b"written");
    within("the write", &mut watch).await.unwrap();

    // Many watches, one request per key, all firing.
    let mut watches = Vec::new();
    let tx = c.db.create_transaction();
    let keys: Vec<Vec<u8>> = (0..50)
        .map(|i| format!("watch/many/{i:02}").into_bytes())
        .collect();
    for key in &keys {
        watches.push(tx.watch(key).await.unwrap());
    }
    // Two more watches of one of them: they share its request.
    let tx2 = c.db.create_transaction();
    let mut twin = tx2.watch(&keys[0]).await.unwrap();
    tx.commit().await.unwrap();
    tx2.commit().await.unwrap();
    until("50 keys watched", || c.db.watched_keys() == 50).await;
    let mut sets = String::new();
    for key in &keys {
        sets.push_str(&format!("set {} v; ", String::from_utf8_lossy(key)));
    }
    c.cli(&sets).await;
    for (i, watch) in watches.iter_mut().enumerate() {
        fires(&format!("watch {i}"), watch).await;
    }
    fires("twin", &mut twin).await;
    assert_eq!(c.db.outstanding_watches(), 0);

    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_watch_cancel() {
    let c = Cluster::start().await;

    // Dropped before its transaction commits: nothing is ever watched.
    let tx = c.db.create_transaction();
    let watch = tx.watch(b"cancel/a").await.unwrap();
    drop(watch);
    tx.commit().await.unwrap();
    assert_eq!(c.db.outstanding_watches(), 0);
    assert_eq!(c.db.watched_keys(), 0);

    // Dropped while watching: the watch is given up, and the key forgotten.
    let watch = c.watch(b"cancel/b").await;
    assert_eq!(c.db.outstanding_watches(), 1);
    until("the key watched", || c.db.watched_keys() == 1).await;
    drop(watch);
    assert_eq!(c.db.outstanding_watches(), 0);
    until("the map to empty", || c.db.watched_keys() == 0).await;

    // A transaction that is reset before it commits cancels its watches.
    let tx = c.db.create_transaction();
    let mut watch = tx.watch(b"cancel/c").await.unwrap();
    tx.reset();
    assert_eq!(
        within("reset", &mut watch).await.unwrap_err().code(),
        codes::TRANSACTION_CANCELLED
    );

    // Watching goes on working after the cancellations.
    let mut watch = c.watch(b"cancel/b").await;
    c.cli("set cancel/b v").await;
    fires("a new watch", &mut watch).await;

    // Shutting the database down fails the watches that are left.
    let mut watch = c.watch(b"cancel/d").await;
    c.db.shutdown().await;
    assert_eq!(
        within("shutdown", &mut watch).await.unwrap_err().code(),
        codes::OPERATION_CANCELLED
    );
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_watch_cap() {
    let mut options = DatabaseOptions::default();
    let mut watch = WatchConfig::default();
    watch.max_outstanding_watches = 3;
    options.watch = watch;
    let c = Cluster::start_with(options).await;

    let tx = c.db.create_transaction();
    let a = tx.watch(b"cap/a").await.unwrap();
    let mut b = tx.watch(b"cap/b").await.unwrap();
    let _c = tx.watch(b"cap/c").await.unwrap();
    let e = tx.watch(b"cap/d").await.unwrap_err();
    assert_eq!(e.code(), codes::TOO_MANY_WATCHES);
    assert_eq!(e.code(), 1032);
    assert_eq!(c.db.outstanding_watches(), 3);
    // The cap is the database's: another transaction is refused too, also after commit.
    tx.commit().await.unwrap();
    let other = c.db.create_transaction();
    assert_eq!(
        other.watch(b"cap/d").await.unwrap_err().code(),
        codes::TOO_MANY_WATCHES
    );
    // The watches that were allowed still work.
    c.cli("set cap/b v").await;
    fires("an allowed watch", &mut b).await;
    assert_eq!(c.db.outstanding_watches(), 2);
    // A resolved one made room, and so does a dropped one.
    let d = other.watch(b"cap/d").await.unwrap();
    assert_eq!(
        other.watch(b"cap/e").await.unwrap_err().code(),
        codes::TOO_MANY_WATCHES
    );
    drop(a);
    let e = other.watch(b"cap/e").await.unwrap();
    drop((d, e));
    assert_eq!(c.db.outstanding_watches(), 1);

    c.finish().await;
}

/// The counter's value.
async fn counter(db: &Database, key: &'static [u8]) -> u64 {
    let value = within(
        "read the counter",
        db.run(move |tx, _| async move { tx.get(key).await }),
    )
    .await
    .unwrap()
    .unwrap_or_default();
    u64::from_le_bytes(value.try_into().expect("8 bytes"))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_watch_concurrent_increments() {
    // Two transactions each add 1 to a counter and watch it, and commit concurrently (in one
    // commit batch, most likely). The watch reads the counter (a dependent write: with a
    // read conflict range), so either one conflicts, or each watches its own view and fires
    // once the other's increment is visible. Never does a committed watch miss the other's
    // increment.
    let c = Cluster::start().await;
    let key: &'static [u8] = b"count/n";
    let one = 1u64.to_le_bytes();
    let mut expected = 0u64;
    for round in 0..3 {
        let (t1, t2) = (c.db.create_transaction(), c.db.create_transaction());
        let mut watches = Vec::new();
        for tx in [&t1, &t2] {
            tx.atomic_op(key, &one, MutationType::Add);
            watches.push(within("watch", tx.watch(key)).await.unwrap());
        }
        let (r1, r2) = within("the commits", async {
            tokio::join!(t1.commit(), t2.commit())
        })
        .await;
        println!(
            "round {round}: {:?} / {:?}",
            r1.as_ref()
                .map(|r| r.committed_version())
                .map_err(|e| e.code()),
            r2.as_ref()
                .map(|r| r.committed_version())
                .map_err(|e| e.code())
        );
        let mut w2 = watches.pop().unwrap();
        let mut w1 = watches.pop().unwrap();
        match (r1, r2) {
            (Ok(_), Ok(_)) => {
                expected += 2;
                fires("the first watch", &mut w1).await;
                fires("the second watch", &mut w2).await;
            }
            (Ok(_), Err(e)) | (Err(e), Ok(_)) => {
                expected += 1;
                assert_eq!(e.code(), codes::NOT_COMMITTED, "{e}");
                let (winner, loser) = if t1.committed_version() > 0 {
                    (&mut w1, &mut w2)
                } else {
                    (&mut w2, &mut w1)
                };
                // The loser's watch fails with its commit's error.
                assert_eq!(
                    within("the loser's watch", loser).await.unwrap_err().code(),
                    codes::NOT_COMMITTED
                );
                // The winner watches its own result, which is the counter's value now.
                assert_eq!(counter(&c.db, key).await, expected);
                stays_pending(winner).await;
                // The loser, retried, increments it: that fires the winner's watch, and the
                // retry's own watch watches the new value.
                let mut again = within(
                    "the retry",
                    c.db.run(move |tx, _| async move {
                        tx.atomic_op(key, &one, MutationType::Add);
                        tx.watch(key).await
                    }),
                )
                .await
                .unwrap();
                expected += 1;
                fires("the winner's watch", winner).await;
                stays_pending(&mut again).await;
            }
            (Err(a), Err(b)) => panic!("both failed: {a} / {b}"),
        }
        assert_eq!(counter(&c.db, key).await, expected);
    }
    c.finish().await;
}
