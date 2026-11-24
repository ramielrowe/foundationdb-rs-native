//! Two `Database` handles opened on one cluster share their connections (b35.7), against a
//! real, disposable FDB server started by `fdb-native-testkit` (image from `FDB_IMAGE`,
//! default 7.4.10): one connection to the server for both, each with its own long poll; the
//! shutdown of one leaves the other working on the same connection; the connection closes with
//! the last one. Run with `cargo nextest run -p fdb-native-client --run-ignored only`.

#![cfg(feature = "tokio")]

use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::{Database, DatabaseOptions};
use fdb_native_testkit::FdbContainer;
use fdb_native_transport::peer::PeerPhase;
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_wire::NetworkAddress;

const LIMIT: Duration = Duration::from_secs(120);

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {LIMIT:?}"))
}

async fn set(db: &Database, key: &'static [u8], value: &'static [u8]) {
    within(
        "a write",
        db.run(|tx, _| async move {
            tx.set(key, value);
            Ok(())
        }),
    )
    .await
    .expect("committed");
}

async fn get(db: &Database, key: &'static [u8]) -> Option<Vec<u8>> {
    within("a read", db.run(|tx, _| async move { tx.get(key).await }))
        .await
        .expect("read")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_two_databases_share_one_connection() {
    let fdb = tokio::task::spawn_blocking(FdbContainer::start)
        .await
        .unwrap()
        .expect("container");
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
    let a = Database::open(Some(fdb.cluster_file())).expect("database");
    let b = Database::open(Some(fdb.cluster_file())).expect("database");
    assert!(a.peers().shares_connections_with(b.peers()));
    assert_eq!(a.peers().users(), 2);
    // A database with connections of its own, for comparison.
    let mut options = DatabaseOptions::default();
    options.share_connections = false;
    let own = Database::open_with(TokioRuntime::new(), Some(fdb.cluster_file()), options)
        .expect("database");
    assert!(!a.peers().shares_connections_with(own.peers()));

    set(&a, b"sharing/a", b"1").await;
    assert_eq!(get(&b, b"sharing/a").await.as_deref(), Some(&b"1"[..]));
    set(&b, b"sharing/b", b"2").await;
    assert_eq!(get(&a, b"sharing/b").await.as_deref(), Some(&b"2"[..]));
    assert_eq!(get(&own, b"sharing/b").await.as_deref(), Some(&b"2"[..]));
    // The server is one process: one peer (one connection) for both databases.
    assert_eq!(a.peers().peer_count(), 1);
    assert_eq!(own.peers().peer_count(), 1);
    let coordinator = a
        .cluster_monitor()
        .current()
        .expect("connected")
        .coordinator;
    let address = NetworkAddress::new(
        coordinator.addr.ip().into(),
        coordinator.addr.port(),
        true,
        coordinator.tls,
    );
    let peer = b.peers().peer(&address);
    let status = peer.status();
    assert_eq!(status.phase, PeerPhase::Connected);

    // `a` goes: `b` goes on, on the same connection.
    within("a's shutdown", a.shutdown()).await;
    let e = within("a read of a", a.create_transaction().get(b"sharing/a"))
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::OPERATION_CANCELLED);
    set(&b, b"sharing/b", b"3").await;
    assert_eq!(get(&b, b"sharing/b").await.as_deref(), Some(&b"3"[..]));
    assert_eq!(peer.status().phase, PeerPhase::Connected);
    assert_eq!(peer.status().generation, status.generation);
    assert_eq!(b.peers().users(), 1);

    // The last one goes: the connection closes; `own` is untouched.
    let weak = b.peers().downgrade();
    drop(a);
    drop(b);
    within("the connection's close", async {
        while !(weak.is_shut_down() && peer.status().phase == PeerPhase::Shutdown) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(get(&own, b"sharing/a").await.as_deref(), Some(&b"1"[..]));
    within("own's shutdown", own.shutdown()).await;
    drop(fdb);
}
