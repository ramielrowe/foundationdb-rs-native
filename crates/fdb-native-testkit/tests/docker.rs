//! Docker-backed integration tests. Run with `cargo nextest run --run-ignored only`.
//!
//! Every test only touches containers created by the test itself.

use std::fs;
use std::process::Command;
use std::time::Duration;

use fdb_native_testkit::{Error, FdbContainer};

/// Returns whether `docker inspect <id-or-name>` succeeds (i.e. the container exists).
fn container_exists(id_or_name: &str) -> bool {
    Command::new("docker")
        .args(["inspect", id_or_name])
        .output()
        .expect("docker must be installed")
        .status
        .success()
}

#[test]
#[ignore = "requires docker"]
fn starts_ready_container_and_removes_it_on_drop() {
    let fdb = FdbContainer::start().expect("container should start");
    let id = fdb.id().to_owned();
    let cluster_file = fdb.cluster_file().to_owned();

    assert_eq!(id.len(), 64);
    assert!(container_exists(&id));
    assert!(fdb.name().starts_with("fdb-native-test-"));

    // Cluster file contents match the advertised cluster string and address.
    let contents = fs::read_to_string(&cluster_file).expect("cluster file readable");
    assert_eq!(contents.trim(), fdb.cluster_string());
    assert_eq!(
        fdb.cluster_string(),
        format!("docker:docker@{}", fdb.address())
    );
    assert_eq!(fdb.address().port(), 4500);

    // The database is up.
    let status = fdb.fdbcli("status minimal").expect("fdbcli works");
    assert!(
        status.contains("The database is available"),
        "unexpected status: {status}"
    );

    // The container carries our label.
    let labels = Command::new("docker")
        .args(["inspect", "-f", "{{json .Config.Labels}}", &id])
        .output()
        .expect("docker inspect");
    assert!(String::from_utf8_lossy(&labels.stdout).contains("\"fdb-native-testkit\":\"1\""));

    drop(fdb);

    assert!(!container_exists(&id), "container must be gone after drop");
    assert!(
        !cluster_file.exists(),
        "cluster file must be gone after drop"
    );
}

#[test]
#[ignore = "requires docker"]
fn concurrent_containers_are_independent() {
    let a = FdbContainer::start().expect("first container");
    let b = FdbContainer::start().expect("second container");
    assert_ne!(a.id(), b.id());
    assert_ne!(a.name(), b.name());
    assert_ne!(a.address(), b.address());
    let (a_id, b_id) = (a.id().to_owned(), b.id().to_owned());
    drop(a);
    assert!(!container_exists(&a_id));
    assert!(
        container_exists(&b_id),
        "dropping one must not affect the other"
    );
    drop(b);
    assert!(!container_exists(&b_id));
}

#[test]
#[ignore = "requires docker"]
fn failed_startup_returns_error_and_leaks_nothing() {
    let name = format!("fdb-native-test-failure-{}", std::process::id());
    let err = FdbContainer::builder()
        .image("foundationdb/foundationdb:0.0.0-does-not-exist")
        .name(name.clone())
        .start()
        .expect_err("nonexistent image must fail");
    assert!(matches!(err, Error::Docker { .. }), "got {err:?}");
    assert!(!container_exists(&name), "no container may be left behind");
}

#[test]
#[ignore = "requires docker"]
fn readiness_timeout_returns_error_and_removes_container() {
    let name = format!("fdb-native-test-timeout-{}", std::process::id());
    let err = FdbContainer::builder()
        .name(name.clone())
        .startup_timeout(Duration::from_millis(1))
        .start()
        .expect_err("a 1ms deadline cannot be met");
    assert!(matches!(err, Error::NotReady { .. }), "got {err:?}");
    assert!(!container_exists(&name), "container must be removed");
}
