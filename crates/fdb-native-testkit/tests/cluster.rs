//! Docker-backed tests of the multi-process cluster. Run with
//! `cargo nextest run --run-ignored only`; select the image with `FDB_IMAGE`.
//!
//! This binary is named `cluster`, which puts its tests in the nextest `cluster` test group (see
//! `.config/nextest.toml`): each test runs 3 to 4 `fdbserver` containers for about a minute.
//! Every test only touches containers and networks created by the test itself.

use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fdb_native_testkit::{Error, FdbCluster, StorageEngine};

const WAIT: Duration = Duration::from_secs(120);

fn docker_stdout(args: &[&str]) -> String {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("docker must be installed");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn docker_ok(args: &[&str]) -> bool {
    Command::new("docker")
        .args(args)
        .output()
        .expect("docker must be installed")
        .status
        .success()
}

fn container_exists(id: &str) -> bool {
    docker_ok(&["inspect", id])
}

fn network_exists(id: &str) -> bool {
    docker_ok(&["network", "inspect", id])
}

fn volume_exists(name: &str) -> bool {
    docker_ok(&["volume", "inspect", name])
}

fn volume_names(id: &str) -> Vec<String> {
    docker_stdout(&[
        "inspect",
        "-f",
        "{{range .Mounts}}{{.Name}}{{\"\\n\"}}{{end}}",
        id,
    ])
    .lines()
    .map(str::trim)
    .filter(|l| !l.is_empty())
    .map(str::to_owned)
    .collect()
}

/// IDs of all containers carrying the testkit label (read-only listing).
fn labelled_containers() -> Vec<String> {
    docker_stdout(&[
        "ps",
        "-a",
        "-q",
        "--no-trunc",
        "--filter",
        "label=fdb-native-testkit=1",
    ])
    .lines()
    .map(str::to_owned)
    .collect()
}

/// IDs of all networks carrying the testkit label (read-only listing).
fn labelled_networks() -> Vec<String> {
    docker_stdout(&[
        "network",
        "ls",
        "-q",
        "--no-trunc",
        "--filter",
        "label=fdb-native-testkit=1",
    ])
    .lines()
    .map(str::to_owned)
    .collect()
}

fn assert_all_gone(containers: &[String], network: &str, volumes: &[String]) {
    for id in containers {
        assert!(!container_exists(id), "container {id} must be gone");
    }
    assert!(!network_exists(network), "network {network} must be gone");
    for v in volumes {
        assert!(!volume_exists(v), "volume {v} must be gone");
    }
    let labelled = labelled_containers();
    assert!(
        containers.iter().all(|id| !labelled.contains(id)),
        "labelled containers left: {labelled:?}"
    );
    assert!(!labelled_networks().iter().any(|n| n == network));
}

fn unique_prefix(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("fdb-native-test-{tag}-{}-{nanos:x}", std::process::id())
}

fn cluster_file_contents(cluster: &FdbCluster) -> String {
    fs::read_to_string(cluster.cluster_file())
        .expect("cluster file readable")
        .trim()
        .to_owned()
}

#[test]
fn cluster_rejects_bad_configuration_without_touching_docker() {
    for processes in [0, 2, 6] {
        let err = FdbCluster::builder()
            .processes(processes)
            .start()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidConfig(_)), "{err:?}");
    }
    let err = FdbCluster::builder().coordinators(4).start().unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)), "{err:?}");
    let err = FdbCluster::builder()
        .process_classes(["storage"])
        .start()
        .unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)), "{err:?}");
}

#[test]
#[ignore = "requires docker"]
fn cluster_starts_healthy_and_is_removed_on_drop() {
    let started = Instant::now();
    let cluster = FdbCluster::start().expect("cluster should start");
    eprintln!("cluster ({cluster}) started in {:?}", started.elapsed());

    let ids: Vec<String> = cluster
        .process_ids()
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    let network = cluster.network_id().to_owned();
    assert_eq!(cluster.process_count(), 3);
    assert_eq!(ids.len(), 3);
    assert!(ids.iter().all(|id| id.len() == 64));
    assert!(network_exists(&network));

    // Labels, network membership and static addresses.
    let labels = docker_stdout(&["network", "inspect", "-f", "{{json .Labels}}", &network]);
    assert!(labels.contains("\"fdb-native-testkit\":\"1\""), "{labels}");
    for (i, id) in ids.iter().enumerate() {
        let labels = docker_stdout(&["inspect", "-f", "{{json .Config.Labels}}", id]);
        assert!(labels.contains("\"fdb-native-testkit\":\"1\""), "{labels}");
        let net = docker_stdout(&[
            "inspect",
            "-f",
            "{{range $n, $v := .NetworkSettings.Networks}}{{$n}} {{$v.NetworkID}} {{$v.IPAddress}}{{end}}",
            id,
        ]);
        let parts: Vec<&str> = net.split_whitespace().collect();
        assert_eq!(parts.len(), 3, "{net}");
        assert_eq!(
            parts[1], network,
            "container must be on the cluster network only"
        );
        assert_eq!(parts[2], cluster.address(i).ip().to_string());
        assert_eq!(cluster.address(i).port(), 4500);
    }
    let labelled = labelled_containers();
    assert!(ids.iter().all(|id| labelled.contains(id)));
    assert!(labelled_networks().contains(&network));

    // The cluster file names the coordinators (the first 3 processes) and matches the one in
    // the containers.
    assert_eq!(cluster_file_contents(&cluster), cluster.cluster_string());
    assert_eq!(
        cluster.current_cluster_string().unwrap(),
        cluster.cluster_string()
    );
    for i in 0..3 {
        assert!(
            cluster
                .cluster_string()
                .contains(&cluster.address(i).to_string())
        );
    }
    assert_eq!(cluster.coordinator_indexes(), [0, 1, 2]);

    // Healthy double/memory cluster with one reporting process per container.
    let status = cluster.fdbcli("status minimal").unwrap();
    assert!(status.contains("The database is available"), "{status}");
    let health = cluster.wait_healthy(WAIT).unwrap();
    assert_eq!(health.processes, 3);
    let json = cluster.status_json().unwrap();
    let config = &json["cluster"]["configuration"];
    assert_eq!(config["redundancy_mode"], "double");
    assert_eq!(config["storage_engine"], "memory");
    assert_eq!(config["coordinators_count"], 3);
    let storage = cluster.processes_with_role("storage").unwrap();
    assert!(!storage.is_empty(), "some process must be a storage server");
    for i in 0..3 {
        assert!(cluster.is_up(i));
        let roles = cluster.process_roles(i).unwrap();
        assert!(roles.iter().any(|r| r == "coordinator"), "{roles:?}");
    }

    // Data written through one process is readable.
    cluster.write_keys("life/", 20, 100).unwrap();
    assert_eq!(cluster.get("life/00000007").unwrap(), Some("x".repeat(100)));
    assert_eq!(cluster.get("life/missing").unwrap(), None);

    let mut volumes = Vec::new();
    for id in &ids {
        volumes.extend(volume_names(id));
    }
    assert!(!volumes.is_empty(), "expected anonymous data volumes");
    assert!(volumes.iter().all(|v| volume_exists(v)));
    let file = cluster.cluster_file().to_owned();

    drop(cluster);

    assert_all_gone(&ids, &network, &volumes);
    assert!(!file.exists(), "cluster file must be gone after drop");
}

#[test]
#[ignore = "requires docker"]
fn cluster_survives_kill_restart_and_pause_of_a_storage_process() {
    let cluster = FdbCluster::builder()
        .storage_engine(StorageEngine::Ssd)
        .start()
        .expect("cluster should start");
    let json = cluster.status_json().unwrap();
    assert_eq!(json["cluster"]["configuration"]["storage_engine"], "ssd-2");

    cluster.write_keys("k", 500, 1000).unwrap();
    let value = "x".repeat(1000);
    assert_eq!(cluster.get("k00000007").unwrap().as_deref(), Some(&*value));

    // SIGKILL a storage process: the other two keep the database available, data intact.
    let victim = *cluster
        .processes_with_role("storage")
        .unwrap()
        .last()
        .expect("a storage process");
    let killed_at = Instant::now();
    cluster.kill(victim).unwrap();
    assert!(!cluster.is_up(victim));
    let health = cluster.wait_healthy(WAIT).unwrap();
    eprintln!("recovered from kill in {:?}: {health}", killed_at.elapsed());
    assert_eq!(health.processes, 2);
    assert_eq!(cluster.get("k00000499").unwrap().as_deref(), Some(&*value));
    cluster.write_keys("after-kill/", 10, 100).unwrap();

    // The process comes back at the same address and the cluster heals fully.
    let address = cluster.address(victim);
    cluster.start_process(victim).unwrap();
    assert!(cluster.is_up(victim));
    assert_eq!(cluster.address(victim), address);
    let health = cluster.wait_healthy(WAIT).unwrap();
    assert_eq!(health.processes, 3);
    assert_eq!(cluster.get("k00000250").unwrap().as_deref(), Some(&*value));
    assert!(cluster.get("after-kill/00000009").unwrap().is_some());

    // A graceful restart of another process (`docker restart`).
    let other = (victim + 1) % 3;
    cluster.restart(other).unwrap();
    cluster.wait_healthy(WAIT).unwrap();

    // Pause (cgroup freezer) a process: it is declared failed, then rejoins when thawed.
    let paused = (victim + 2) % 3;
    cluster.pause(paused).unwrap();
    assert!(!cluster.is_up(paused));
    // A frozen process cannot run fdbcli, but the others still serve it.
    cluster.wait_healthy(WAIT).unwrap();
    cluster.write_keys("while-paused/", 10, 100).unwrap();
    cluster.unpause(paused).unwrap();
    let health = cluster.wait_healthy(WAIT).unwrap();
    assert_eq!(health.processes, 3);
    assert!(cluster.get("while-paused/00000003").unwrap().is_some());

    // Stop (graceful) and start again.
    cluster.stop(paused).unwrap();
    cluster.wait_healthy(WAIT).unwrap();
    cluster.start_process(paused).unwrap();
    cluster.wait_healthy(WAIT).unwrap();
}

#[test]
#[ignore = "requires docker"]
fn cluster_exclude_shard_split_and_coordinator_change() {
    let cluster = FdbCluster::builder()
        .processes(4)
        .small_shards()
        .start()
        .expect("cluster should start");
    assert_eq!(cluster.process_count(), 4);
    assert_eq!(cluster.coordinator_indexes(), [0, 1, 2]);

    // 3 MB with ~800 KB maximum shards: data distribution splits the keyspace.
    cluster.write_keys("s", 3000, 1000).unwrap();
    let parts = cluster.wait_for_partitions(3, WAIT).unwrap();
    eprintln!("{parts} shards after writing 3 MB");
    assert!(parts >= 3);

    // Excluding a process moves its shards away; including it again lets them move back.
    let started = Instant::now();
    cluster.exclude(3).unwrap();
    eprintln!("exclude took {:?}", started.elapsed());
    let excluded = cluster.fdbcli("exclude").unwrap();
    assert!(
        excluded.contains(&cluster.address(3).to_string()),
        "{excluded}"
    );
    assert_eq!(
        cluster.get("s00001500").unwrap().as_deref(),
        Some(&*"x".repeat(1000))
    );
    cluster.wait_healthy(WAIT).unwrap();
    cluster.include(3).unwrap();
    let excluded = cluster.fdbcli("exclude").unwrap();
    assert!(
        excluded.contains("no servers or localities excluded"),
        "{excluded}"
    );
    cluster.wait_healthy(WAIT).unwrap();

    // Moving the coordinators from {0,1,2} to {1,2,3}. The host's cluster file goes stale on
    // purpose; the processes' own files follow.
    let stale = cluster.cluster_string().to_owned();
    let new = cluster.set_coordinators(&[1, 2, 3]).unwrap();
    assert_ne!(new, stale);
    assert!(new.contains(&cluster.address(3).to_string()), "{new}");
    assert!(!new.contains(&cluster.address(0).to_string()), "{new}");
    assert_eq!(cluster.coordinator_indexes(), [1, 2, 3]);
    assert_eq!(cluster_file_contents(&cluster), stale);
    assert_eq!(cluster.refresh_cluster_file().unwrap(), new);
    assert_eq!(cluster_file_contents(&cluster), new);
    cluster.wait_healthy(WAIT).unwrap();

    // The old coordinator restarts, started with the old cluster string in its environment, and
    // is forwarded to the new ones. So is a process that was down during the change.
    cluster.restart(0).unwrap();
    cluster.kill(2).unwrap();
    cluster.start_process(2).unwrap();
    cluster.wait_healthy(WAIT).unwrap();
    assert_eq!(
        cluster.get("s00002999").unwrap().as_deref(),
        Some(&*"x".repeat(1000))
    );
}

#[test]
#[ignore = "requires docker"]
fn cluster_start_failure_leaves_nothing_behind() {
    let prefix = unique_prefix("fail");
    // Too short to form a cluster: start fails after creating the network and containers.
    let err = FdbCluster::builder()
        .name_prefix(prefix.clone())
        .startup_timeout(Duration::from_secs(3))
        .start()
        .expect_err("a 3 second start cannot succeed");
    eprintln!("expected failure: {err}");

    let containers = docker_stdout(&["ps", "-a", "-q", "--filter", &format!("name={prefix}")]);
    assert!(
        containers.trim().is_empty(),
        "containers left: {containers}"
    );
    let networks = docker_stdout(&["network", "ls", "-q", "--filter", &format!("name={prefix}")]);
    assert!(networks.trim().is_empty(), "networks left: {networks}");
}

#[test]
#[ignore = "requires docker"]
fn cluster_is_removed_when_the_test_panics() {
    let cluster = FdbCluster::start().expect("cluster should start");
    let ids: Vec<String> = cluster
        .process_ids()
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    let network = cluster.network_id().to_owned();
    let mut volumes = Vec::new();
    for id in &ids {
        volumes.extend(volume_names(id));
    }
    assert!(ids.iter().all(|id| container_exists(id)));

    let result = catch_unwind(AssertUnwindSafe(move || {
        let _cluster = cluster;
        panic!("simulated test failure");
    }));
    assert!(result.is_err());
    assert_all_gone(&ids, &network, &volumes);
}
