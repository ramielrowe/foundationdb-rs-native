//! The crate's examples (`examples/`), run against a real, disposable FDB server started by
//! `fdb-native-testkit` (image from `FDB_IMAGE`, default 7.4.10): each example's `run` is
//! included here and called with a database of the container (the examples' own `main`
//! opens the cluster file of their first argument instead). The TLS example runs against a
//! TLS container with the `tls` feature. Run with
//! `cargo nextest run -p fdb-native-client --run-ignored only -E 'binary(docker_examples)'`.

#![cfg(feature = "tokio")]

use std::time::Duration;

use fdb_native_client::Database;
use fdb_native_testkit::FdbContainer;

#[allow(dead_code)]
#[path = "../examples/basic.rs"]
mod basic;
#[allow(dead_code)]
#[path = "../examples/counter.rs"]
mod counter;
#[allow(dead_code)]
#[path = "../examples/options.rs"]
mod options;
#[allow(dead_code)]
#[path = "../examples/range_scan.rs"]
mod range_scan;
#[cfg(feature = "tls")]
#[allow(dead_code)]
#[path = "../examples/tls.rs"]
mod tls;
#[allow(dead_code)]
#[path = "../examples/versionstamp.rs"]
mod versionstamp;
#[allow(dead_code)]
#[path = "../examples/watch.rs"]
mod watch;

const LIMIT: Duration = Duration::from_secs(120);

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {LIMIT:?}"))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_examples_run_against_a_cluster() {
    let fdb = tokio::task::spawn_blocking(FdbContainer::start)
        .await
        .unwrap()
        .expect("container");
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
    let db = Database::open(Some(fdb.cluster_file())).expect("database");

    within("basic", basic::run(&db)).await.expect("basic");
    within("range_scan", range_scan::run(&db))
        .await
        .expect("range_scan");
    within("watch", watch::run(&db)).await.expect("watch");
    within("versionstamp", versionstamp::run(&db))
        .await
        .expect("versionstamp");
    within("counter", counter::run(&db)).await.expect("counter");
    // Last: it sets database-wide transaction defaults.
    within("options", options::run(&db)).await.expect("options");

    db.shutdown().await;
    tokio::task::spawn_blocking(move || drop(fdb))
        .await
        .unwrap();
}

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_examples_tls_connects_to_a_tls_cluster() {
    use fdb_native_client::runtime::TokioRuntime;

    let fdb = tokio::task::spawn_blocking(|| {
        FdbContainer::builder()
            .image(fdb_native_testkit::default_image())
            .tls()
            .start()
    })
    .await
    .unwrap()
    .expect("TLS container");
    assert!(fdb.cluster_string().ends_with(":tls"));
    let files = fdb.tls().expect("TLS files");
    let db = Database::open_with(
        TokioRuntime::new(),
        Some(fdb.cluster_file()),
        tls::options(
            &files.ca_path(),
            &files.client_cert_path(),
            &files.client_key_path(),
        ),
    )
    .expect("database");
    within("tls", tls::run(&db)).await.expect("tls");
    db.shutdown().await;
    tokio::task::spawn_blocking(move || drop(fdb))
        .await
        .unwrap();
}
