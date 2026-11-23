//! Docker-backed integration tests. Run with `cargo nextest run --run-ignored only`.
//!
//! Every test only touches containers created by the test itself. This binary is named
//! `docker`, which puts its tests in the nextest `docker` test group (see
//! `.config/nextest.toml`).

use std::fs;
use std::process::Command;
use std::time::Duration;

use fdb_native_testkit::{Error, FdbContainer};

fn docker_output(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("docker must be installed")
}

/// Returns whether `docker inspect <id-or-name>` succeeds (i.e. the container exists).
fn container_exists(id_or_name: &str) -> bool {
    docker_output(&["inspect", id_or_name]).status.success()
}

fn is_running(id: &str) -> bool {
    let out = docker_output(&["inspect", "-f", "{{.State.Running}}", id]);
    String::from_utf8_lossy(&out.stdout).trim() == "true"
}

/// Names of the volumes mounted in the container.
fn volume_names(id: &str) -> Vec<String> {
    let out = docker_output(&[
        "inspect",
        "-f",
        "{{range .Mounts}}{{.Name}}{{\"\\n\"}}{{end}}",
        id,
    ]);
    assert!(out.status.success());
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect()
}

fn volume_exists(name: &str) -> bool {
    docker_output(&["volume", "inspect", name]).status.success()
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
    let labels = docker_output(&["inspect", "-f", "{{json .Config.Labels}}", &id]);
    assert!(String::from_utf8_lossy(&labels.stdout).contains("\"fdb-native-testkit\":\"1\""));

    // The image declares VOLUME /var/fdb/data, so there is an anonymous volume to clean up.
    let volumes = volume_names(&id);
    assert!(!volumes.is_empty(), "expected an anonymous data volume");
    assert!(volumes.iter().all(|v| volume_exists(v)));

    drop(fdb);

    assert!(!container_exists(&id), "container must be gone after drop");
    for v in &volumes {
        assert!(
            !volume_exists(v),
            "volume {v} must be removed with the container"
        );
    }
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
    // The server needs seconds to come up and readiness takes several polling steps, so this
    // short but real timeout is always exceeded, after at least one polling iteration.
    let err = FdbContainer::builder()
        .name(name.clone())
        .startup_timeout(Duration::from_millis(500))
        .start()
        .expect_err("a 500ms readiness deadline cannot be met");
    assert!(matches!(err, Error::NotReady { .. }), "got {err:?}");
    assert!(!container_exists(&name), "container must be removed");
}

#[test]
#[ignore = "requires docker"]
fn name_collision_fails_without_touching_the_existing_container() {
    let name = format!("fdb-native-test-collision-{}", std::process::id());
    let first = FdbContainer::builder()
        .name(name.clone())
        .start()
        .expect("first container");
    let first_id = first.id().to_owned();

    let err = FdbContainer::builder()
        .name(name)
        .start()
        .expect_err("a duplicate name must fail");
    assert!(matches!(err, Error::Docker { .. }), "got {err:?}");

    assert!(container_exists(&first_id));
    assert!(
        is_running(&first_id),
        "first container must still be running"
    );
    let status = first.fdbcli("status minimal").expect("first still serves");
    assert!(status.contains("The database is available"));

    drop(first);
    assert!(!container_exists(&first_id));
}
