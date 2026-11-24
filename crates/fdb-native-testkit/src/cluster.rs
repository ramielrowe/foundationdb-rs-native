//! Multi-process, multi-container FoundationDB clusters for fault tests.
//!
//! [`FdbContainer`](crate::FdbContainer) is one `fdbserver` with `single` redundancy: enough for
//! protocol tests, useless for anything that needs a peer to survive (process death, a paused
//! process, exclusion, coordinator changes, shard moves). [`FdbCluster`] starts N (3 to 5)
//! `fdbserver` containers, one process each, on a dedicated Docker network, configures
//! `double`/`triple` redundancy and waits until the cluster is fully recovered and healthy.
//!
//! ```no_run
//! use std::time::Duration;
//! use fdb_native_testkit::FdbCluster;
//!
//! # fn main() -> Result<(), fdb_native_testkit::Error> {
//! // 3 processes, `double memory`; image from `FDB_IMAGE` (default 7.4.10).
//! let cluster = FdbCluster::start()?;
//! println!("cluster file: {}", cluster.cluster_file().display());
//!
//! cluster.write_keys("k", 100, 1000)?;
//! cluster.kill(2)?; // SIGKILL one process
//! cluster.wait_healthy(Duration::from_secs(120))?; // two survivors re-replicate
//! cluster.start_process(2)?; // same IP and port as before
//! cluster.wait_healthy(Duration::from_secs(120))?;
//! // Containers, volumes and the network are removed (by ID) when `cluster` is dropped.
//! # Ok(())
//! # }
//! ```
//!
//! # How a cluster is built
//!
//! 1. `docker network create --label fdb-native-testkit=1 <name>`; the printed ID goes into a
//!    drop guard at once and the network is referred to by that ID from then on. Docker picks
//!    the subnet (so it cannot collide with anything on the host); the crate reads it back with
//!    `docker network inspect` and gives process `i` the **static address** `<subnet>.(10 + i)`
//!    (`docker create --ip`). Because the address is fixed, `kill`/`start_process`/`restart`
//!    bring a process back at the same `IP:4500`; there is no [`Error::AddressChanged`] problem
//!    and the cluster file never goes stale on a restart.
//! 2. One container per process: `docker create --network <network-id> --ip <ip> --label
//!    fdb-native-testkit=1 [-e ...] <image>`, again with the image's default entrypoint (tini ->
//!    `fdb.bash`) and no command arguments, each ID into a drop guard, then `docker start`.
//! 3. Wait until every container runs and accepts TCP connections from the host, run
//!    `fdbcli --exec "configure new <redundancy> <engine>"` in the first process (retried until
//!    the cluster has formed), and wait until `status json` says: database available, recovery
//!    `fully_recovered`, data distribution healthy with nothing in flight, and one process
//!    reporting per running container. Data distribution takes 20 to 30 seconds to initialise on
//!    a new cluster, which dominates the start-up time (about 40 seconds).
//!
//! # Per-image configuration (verified on 7.4.10 and 8.0.1)
//!
//! Both images run `fdb.bash` as their entrypoint. What differs between the two scripts, and
//! what this crate relies on:
//!
//! | | 7.4.10 | 8.0.1 |
//! |---|---|---|
//! | Cluster file variable | `FDB_CLUSTER_FILE_CONTENTS` (wins over `FDB_COORDINATOR`) | `FDB_CLUSTER_FILE_CONTENTS` (**loses** to `FDB_COORDINATOR`, which the image sets to the empty string) |
//! | `FDB_COORDINATOR` | one hostname, resolved with `dig` | one hostname, resolved with `getent ahostsv4` |
//! | Public address | `hostname -i` | `$FDB_PUBLIC_IP`, else the first IPv4 address of `hostname -I` |
//! | Command arguments | ignored | appended to the `fdbserver` command line |
//! | Extra variables | | `FDB_IP_VERSION=4`, `FDB_LISTEN_IP` |
//!
//! `FDB_COORDINATOR` names a **single** coordinator (the script writes
//! `docker:docker@<resolved ip>:<FDB_COORDINATOR_PORT>`), which cannot express a three-coordinator
//! cluster, and needs DNS. So this crate sets only `FDB_CLUSTER_FILE_CONTENTS` to the same
//! full string on every container:
//!
//! ```text
//! docker:docker@172.18.0.10:4500,172.18.0.11:4500,172.18.0.12:4500
//! ```
//!
//! (the first 3 processes are the coordinators by default). Both scripts write it to
//! `FDB_CLUSTER_FILE` (`/var/fdb/fdb.cluster`) on every start, then `fdbserver` follows the
//! coordinators and rewrites the file when they change, so a process restarted after a
//! `coordinators` change still joins (through the forwarding the old coordinators keep).
//!
//! Everything else keeps the image defaults: `FDB_PORT=4500`, `FDB_NETWORKING_MODE=container`
//! and `FDB_PROCESS_CLASS=unset`, i.e. every process can take any role, including storage and
//! coordinator (set per-process classes with [`FdbClusterBuilder::process_classes`]). The
//! zone and machine ids are the container hostnames, so every process is its own fault domain.
//!
//! Server knobs can be set with `FDB_KNOB_<lower case name>=value` environment variables (both
//! 7.4 and 8.0 `fdbserver` read them; the name must be lower case on 7.4, which otherwise
//! prints `Invalid knob option`): [`FdbClusterBuilder::knob`] and
//! [`FdbClusterBuilder::small_shards`].
//!
//! # Fault helpers
//!
//! All helpers address a process by its index `0..process_count()` and act by exact container
//! ID. They are blocking.
//!
//! | Helper | Effect |
//! |---|---|
//! | [`kill`](FdbCluster::kill) | `docker kill`: SIGKILL, connections reset |
//! | [`stop`](FdbCluster::stop) | `docker stop -t 1`: SIGTERM, then SIGKILL |
//! | [`start_process`](FdbCluster::start_process) | `docker start` after kill/stop, waits for the process to rejoin |
//! | [`restart`](FdbCluster::restart) | `docker restart -t 1`, waits for the process to rejoin |
//! | [`pause`](FdbCluster::pause) / [`unpause`](FdbCluster::unpause) | cgroup freezer: sockets stay open, nothing answers |
//! | [`exclude`](FdbCluster::exclude) / [`include`](FdbCluster::include) | `fdbcli exclude`/`include`; exclude waits until the data has moved off |
//! | [`set_coordinators`](FdbCluster::set_coordinators) | `fdbcli coordinators ...`; the host cluster file goes stale on purpose, see [`FdbCluster::refresh_cluster_file`] |
//! | [`write_keys`](FdbCluster::write_keys), [`set_data_distribution`](FdbCluster::set_data_distribution), [`partitions`](FdbCluster::partitions) | produce data and shard splits/moves |
//! | [`status_json`](FdbCluster::status_json), [`health`](FdbCluster::health) | inspect the cluster |
//!
//! ## Shard splits and moves
//!
//! Data distribution splits a shard that outgrows
//! `(min_shard_bytes + sqrt(db_bytes) * shard_bytes_per_sqrt_bytes) * 4` bytes, which is 40 MB
//! or more by default. [`FdbClusterBuilder::small_shards`] sets `min_shard_bytes=200000` and
//! `shard_bytes_per_sqrt_bytes=0` (the simulation's "small shards" values), so that
//! `write_keys("k", 3000, 1000)` (3 MB) splits the keyspace into about ten shards within a few
//! seconds, and [`FdbCluster::wait_for_partitions`] waits for that. Shards then move when a
//! process is excluded ([`exclude`](FdbCluster::exclude)), killed, or included again, which is
//! what invalidates a client's location cache. `fdbcli datadistribution` only switches data
//! distribution on and off ([`set_data_distribution`](FdbCluster::set_data_distribution)); it
//! cannot force a split.
//!
//! # Cleanup
//!
//! Exactly as for [`FdbContainer`](crate::FdbContainer): the containers are removed with
//! `docker rm -f -v <id>` (volumes included) and then the network with `docker network rm <id>`,
//! by ID only, when the [`FdbCluster`] is dropped (also on panic and when start-up fails
//! part-way). Removal errors are logged to stderr, never panic. Nothing is ever removed by name,
//! pattern, label or filter. If the process is killed, what leaked is findable with
//! `docker ps -a --filter label=fdb-native-testkit=1` and `docker network ls --filter
//! label=fdb-native-testkit=1`.
//!
//! # Resource use
//!
//! A 3 to 5 process cluster runs 3 to 5 `fdbserver` processes for about a minute. Tests using
//! it should run few at a time: the workspace's `.config/nextest.toml` puts the `cluster`
//! integration-test binary into its own test group.

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use crate::{
    CONTAINER_CLUSTER_FILE, ContainerGuard, DOCKER_TIMEOUT, EXEC_TIMEOUT, Error, FDB_PORT, LABEL,
    POLL_INTERVAL, REMOVE_TIMEOUT, Result, TCP_TIMEOUT, configure_succeeded, default_image, docker,
    docker_raw, docker_raw_input, ensure_image, generate_container_name, log_stderr, log_tail,
    parse_container_id,
};

/// Re-export of the JSON type returned by [`FdbCluster::status_json`].
pub use serde_json;

/// Default process count of a cluster.
pub const DEFAULT_PROCESSES: usize = 3;
/// Smallest allowed process count.
pub const MIN_PROCESSES: usize = 3;
/// Largest allowed process count.
pub const MAX_PROCESSES: usize = 5;

/// Default timeout for a cluster to start (the clock starts after the containers are started,
/// so it excludes any image pull).
pub const DEFAULT_CLUSTER_STARTUP_TIMEOUT: Duration = Duration::from_secs(180);

/// Default timeout for waits after a fault (health, rejoin, shard splits).
pub const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(120);

/// Host part of the first process's address inside the network's subnet (`.10`).
const FIRST_HOST: u32 = 10;
/// Default fdbcli timeout for ordinary commands.
const FDBCLI_DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long an `exclude` may take to finish moving data away.
const EXCLUDE_TIMEOUT: Duration = Duration::from_secs(120);
/// Extra time for `docker exec` on top of fdbcli's own timeout.
const EXEC_MARGIN: Duration = Duration::from_secs(20);
/// Roughly how many bytes of `set` commands go into one `write_keys` transaction.
const WRITE_BATCH_BYTES: usize = 2_000_000;
/// Knobs set by [`FdbClusterBuilder::small_shards`].
const SMALL_SHARD_KNOBS: [(&str, &str); 2] = [
    ("min_shard_bytes", "200000"),
    ("shard_bytes_per_sqrt_bytes", "0"),
];

// ---------------------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------------------

/// Redundancy mode of a cluster (`configure new <mode> <engine>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Redundancy {
    /// `double`: two replicas. Survives the loss of one process of three.
    #[default]
    Double,
    /// `triple`: three replicas. Needs at least three live processes to stay fully replicated.
    Triple,
}

impl Redundancy {
    /// The fdbcli name of the mode.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Double => "double",
            Self::Triple => "triple",
        }
    }
}

/// Storage engine of a cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum StorageEngine {
    /// `memory`: fast to start; recovers its data from the data volume after a restart.
    #[default]
    Memory,
    /// `ssd` (sqlite based): the on-disk engine.
    Ssd,
}

impl StorageEngine {
    /// The fdbcli name of the engine.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Ssd => "ssd",
        }
    }
}

/// What the cluster says about itself in `status json`; see [`FdbCluster::health`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterHealth {
    /// `cluster.database_available`.
    pub available: bool,
    /// `cluster.recovery_state.name`, e.g. `fully_recovered`.
    pub recovery_state: String,
    /// `cluster.data.state.name`, e.g. `healthy` or `healthy_repartitioning`; empty if absent
    /// (for example while data distribution initialises).
    pub data_state: String,
    /// `cluster.data.state.healthy`.
    pub data_healthy: bool,
    /// Number of entries in `cluster.processes`.
    pub processes: usize,
    /// `cluster.data.partitions_count`: the number of shards, once data distribution knows it.
    pub partitions: Option<u64>,
    /// Bytes queued or in flight in data movement (`cluster.data.moving_data`).
    pub moving_bytes: u64,
}

impl ClusterHealth {
    /// Reads the fields out of a `status json` document. Missing fields count as "not good".
    pub fn from_status(status: &serde_json::Value) -> Self {
        let cluster = &status["cluster"];
        let data = &cluster["data"];
        let moving = &data["moving_data"];
        Self {
            available: cluster["database_available"].as_bool().unwrap_or(false),
            recovery_state: cluster["recovery_state"]["name"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            data_state: data["state"]["name"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            data_healthy: data["state"]["healthy"].as_bool().unwrap_or(false),
            processes: cluster["processes"].as_object().map_or(0, |p| p.len()),
            partitions: data["partitions_count"].as_u64(),
            moving_bytes: moving["in_flight_bytes"].as_u64().unwrap_or(0)
                + moving["in_queue_bytes"].as_u64().unwrap_or(0),
        }
    }

    /// Whether the cluster is available, fully recovered, has healthy data with nothing moving,
    /// and `expected_processes` processes report in.
    pub fn is_healthy(&self, expected_processes: usize) -> bool {
        self.available
            && self.recovery_state == "fully_recovered"
            && self.data_healthy
            && self.moving_bytes == 0
            && self.processes == expected_processes
    }
}

impl fmt::Display for ClusterHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "available={} recovery={:?} data={:?} (healthy={}, moving={} B) processes={} partitions={:?}",
            self.available,
            self.recovery_state,
            self.data_state,
            self.data_healthy,
            self.moving_bytes,
            self.processes,
            self.partitions
        )
    }
}

// ---------------------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------------------

/// Parses `a.b.c.d/len` into the network address (as an integer) and the prefix length.
fn parse_subnet(cidr: &str) -> Option<(u32, u8)> {
    let (ip, len) = cidr.trim().split_once('/')?;
    let ip: Ipv4Addr = ip.parse().ok()?;
    let len: u8 = len.parse().ok()?;
    if len > 32 {
        return None;
    }
    let mask = if len == 0 { 0 } else { u32::MAX << (32 - len) };
    Some((u32::from(ip) & mask, len))
}

/// The static address of process `index` in a subnet, or `None` if it does not fit.
fn process_ip(subnet: (u32, u8), index: usize) -> Option<Ipv4Addr> {
    let (base, len) = subnet;
    let host = FIRST_HOST.checked_add(u32::try_from(index).ok()?)?;
    // Leave the broadcast address alone.
    let hosts = if len >= 32 { 1 } else { 1u32 << (32 - len) };
    (host < hosts.saturating_sub(1)).then(|| Ipv4Addr::from(base + host))
}

/// First IPv4 subnet in the output of the network inspect template (one per line).
fn first_ipv4_subnet(output: &str) -> Option<(u32, u8)> {
    output.lines().find_map(parse_subnet)
}

/// `docker:docker@ip:4500,ip:4500,...` for the given coordinators.
fn coordinator_string(addresses: &[SocketAddr]) -> String {
    let list: Vec<String> = addresses.iter().map(ToString::to_string).collect();
    format!("docker:docker@{}", list.join(","))
}

/// The cluster string in the contents of a cluster file: the last line that is not a comment.
fn parse_cluster_file(contents: &str) -> Option<String> {
    contents
        .lines()
        .map(str::trim)
        .rfind(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_owned)
}

/// Parses `status json` output (anything before the first `{` is dropped).
pub(crate) fn parse_status_output(output: &str) -> Result<serde_json::Value> {
    let start = output
        .find('{')
        .ok_or_else(|| Error::Status(format!("no JSON in output: {:?}", output.trim())))?;
    serde_json::from_str(&output[start..])
        .map_err(|e| Error::Status(format!("invalid status JSON: {e}")))
}

/// Maps process IP to the roles it currently has, from a `status json` document.
fn roles_by_ip(status: &serde_json::Value) -> HashMap<String, Vec<String>> {
    let mut map = HashMap::new();
    let Some(processes) = status["cluster"]["processes"].as_object() else {
        return map;
    };
    for process in processes.values() {
        let Some(address) = process["address"].as_str() else {
            continue;
        };
        let ip = address.split(':').next().unwrap_or(address).to_owned();
        let roles = process["roles"]
            .as_array()
            .map(|roles| {
                roles
                    .iter()
                    .filter_map(|r| r["role"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        map.insert(ip, roles);
    }
    map
}

/// Parses the output of fdbcli `get <key>`: `` `k' is `v' `` or `` `k': not found ``.
fn parse_get_output(output: &str) -> Option<Option<String>> {
    let line = output.lines().map(str::trim).find(|l| l.starts_with('`'))?;
    if line.ends_with("': not found") {
        return Some(None);
    }
    let (_, rest) = line.split_once("' is `")?;
    Some(Some(rest.strip_suffix('\'')?.to_owned()))
}

/// Whether a key prefix is safe to put on an fdbcli command line.
fn valid_key_prefix(prefix: &str) -> bool {
    !prefix.is_empty()
        && prefix
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':' | b'/'))
}

/// The fdbcli script writing `count` keys `<prefix><8 digit index>` of `value_size` bytes, in
/// transactions of about [`WRITE_BATCH_BYTES`]; also returns the number of transactions.
fn write_script(prefix: &str, count: usize, value_size: usize) -> (String, usize) {
    let value = "x".repeat(value_size);
    let per_batch = (WRITE_BATCH_BYTES / (value_size + prefix.len() + 16)).max(1);
    let mut script = String::from("writemode on\n");
    let mut batches = 0;
    let mut next = 0;
    while next < count {
        let end = (next + per_batch).min(count);
        script.push_str("begin\n");
        for i in next..end {
            script.push_str(&format!("set {prefix}{i:08} {value}\n"));
        }
        script.push_str("commit\n");
        batches += 1;
        next = end;
    }
    (script, batches)
}

/// The `docker create` environment variables for process `index`.
fn process_env(
    cluster_string: &str,
    class: Option<&str>,
    knobs: &[(String, String)],
) -> Vec<String> {
    let mut env = vec![format!("FDB_CLUSTER_FILE_CONTENTS={cluster_string}")];
    if let Some(class) = class {
        env.push(format!("FDB_PROCESS_CLASS={class}"));
    }
    for (name, value) in knobs {
        env.push(format!("FDB_KNOB_{}={value}", name.to_ascii_lowercase()));
    }
    env
}

// ---------------------------------------------------------------------------------------
// Guards
// ---------------------------------------------------------------------------------------

/// Owns a network ID and removes exactly that network on drop. Always dropped after the
/// containers attached to it.
#[derive(Debug)]
struct NetworkGuard {
    id: String,
}

impl Drop for NetworkGuard {
    fn drop(&mut self) {
        match docker_raw(&["network", "rm", &self.id], REMOVE_TIMEOUT) {
            Ok(out) if out.status.success() => {}
            Ok(out) => log_stderr(format_args!(
                "`docker network rm {}` failed ({}): {}",
                self.id,
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            )),
            Err(e) => log_stderr(format_args!("could not remove network {}: {e}", self.id)),
        }
    }
}

#[derive(Debug)]
struct Node {
    guard: ContainerGuard,
    name: String,
    ip: Ipv4Addr,
    /// Running and not paused, as far as this handle knows.
    up: AtomicBool,
}

impl Node {
    fn id(&self) -> &str {
        &self.guard.id
    }

    fn address(&self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(self.ip), FDB_PORT)
    }

    fn is_up(&self) -> bool {
        self.up.load(Ordering::SeqCst)
    }

    fn set_up(&self, up: bool) {
        self.up.store(up, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------------------

/// Builder for [`FdbCluster`].
#[derive(Debug, Clone)]
pub struct FdbClusterBuilder {
    image: String,
    processes: usize,
    coordinators: Option<usize>,
    redundancy: Redundancy,
    engine: StorageEngine,
    classes: Option<Vec<String>>,
    knobs: Vec<(String, String)>,
    startup_timeout: Duration,
    pull_timeout: Duration,
    name_prefix: Option<String>,
}

impl Default for FdbClusterBuilder {
    fn default() -> Self {
        Self {
            image: default_image(),
            processes: DEFAULT_PROCESSES,
            coordinators: None,
            redundancy: Redundancy::default(),
            engine: StorageEngine::default(),
            classes: None,
            knobs: Vec::new(),
            startup_timeout: DEFAULT_CLUSTER_STARTUP_TIMEOUT,
            pull_timeout: crate::DEFAULT_PULL_TIMEOUT,
            name_prefix: None,
        }
    }
}

impl FdbClusterBuilder {
    /// Sets the Docker image (default: `$FDB_IMAGE` or the crate's default image).
    #[must_use]
    pub fn image(mut self, image: impl Into<String>) -> Self {
        self.image = image.into();
        self
    }

    /// Sets the number of processes, 3 to 5 (default 3).
    #[must_use]
    pub fn processes(mut self, processes: usize) -> Self {
        self.processes = processes;
        self
    }

    /// Sets the number of initial coordinators (the first `n` processes); default
    /// `min(3, processes)`.
    #[must_use]
    pub fn coordinators(mut self, n: usize) -> Self {
        self.coordinators = Some(n);
        self
    }

    /// Sets the redundancy mode (default [`Redundancy::Double`]).
    #[must_use]
    pub fn redundancy(mut self, redundancy: Redundancy) -> Self {
        self.redundancy = redundancy;
        self
    }

    /// Sets the storage engine (default [`StorageEngine::Memory`]).
    #[must_use]
    pub fn storage_engine(mut self, engine: StorageEngine) -> Self {
        self.engine = engine;
        self
    }

    /// Sets the `FDB_PROCESS_CLASS` of every process (`storage`, `transaction`, `stateless`,
    /// `unset`, ...); the slice length must equal the process count. Default: all `unset`.
    #[must_use]
    pub fn process_classes<S: Into<String>>(
        mut self,
        classes: impl IntoIterator<Item = S>,
    ) -> Self {
        self.classes = Some(classes.into_iter().map(Into::into).collect());
        self
    }

    /// Sets a server knob on every process (`FDB_KNOB_<name>`; the name is lower-cased).
    #[must_use]
    pub fn knob(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.knobs.push((name.into(), value.into()));
        self
    }

    /// Makes data distribution split shards at about 800 KB (see the
    /// [module docs](crate::cluster#shard-splits-and-moves)).
    #[must_use]
    pub fn small_shards(mut self) -> Self {
        for (name, value) in SMALL_SHARD_KNOBS {
            self = self.knob(name, value);
        }
        self
    }

    /// Sets the timeout for forming and configuring the cluster (default 180 seconds). It
    /// starts after the containers are started, so it excludes any image pull.
    #[must_use]
    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Sets the timeout for making the image available locally (default 300 seconds).
    #[must_use]
    pub fn pull_timeout(mut self, timeout: Duration) -> Self {
        self.pull_timeout = timeout;
        self
    }

    /// Overrides the name prefix of the containers (`<prefix>-n<i>`) and the network
    /// (`<prefix>-net`). Names must be unique: a clash makes `start` fail without touching the
    /// other object.
    #[must_use]
    pub fn name_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.name_prefix = Some(prefix.into());
        self
    }

    fn validate(&self) -> Result<usize> {
        let n = self.processes;
        if !(MIN_PROCESSES..=MAX_PROCESSES).contains(&n) {
            return Err(Error::InvalidConfig(format!(
                "{n} processes; must be {MIN_PROCESSES} to {MAX_PROCESSES}"
            )));
        }
        let coordinators = self.coordinators.unwrap_or(n.min(3));
        if coordinators == 0 || coordinators > n {
            return Err(Error::InvalidConfig(format!(
                "{coordinators} coordinators with {n} processes"
            )));
        }
        if let Some(classes) = &self.classes {
            if classes.len() != n {
                return Err(Error::InvalidConfig(format!(
                    "{} process classes for {n} processes",
                    classes.len()
                )));
            }
        }
        Ok(coordinators)
    }

    /// Creates the network and containers, forms the cluster and waits until it is healthy.
    pub fn start(self) -> Result<FdbCluster> {
        let coordinator_count = self.validate()?;
        ensure_image(&self.image, self.pull_timeout)?;

        let base = self
            .name_prefix
            .clone()
            .unwrap_or_else(generate_container_name);

        // `docker network create` either fails (nothing exists) or prints the new ID.
        let created = docker(
            &[
                "network",
                "create",
                "--driver",
                "bridge",
                "--label",
                LABEL,
                &format!("{base}-net"),
            ],
            DOCKER_TIMEOUT,
        )?;
        let network_id = parse_container_id(&created).ok_or_else(|| {
            Error::Parse(format!("docker network create printed no id: {created:?}"))
        })?;
        // From here on the guard removes exactly this network, on any early return too.
        let network = NetworkGuard { id: network_id };

        let inspected = docker(
            &[
                "network",
                "inspect",
                "-f",
                "{{range .IPAM.Config}}{{.Subnet}}{{\"\\n\"}}{{end}}",
                &network.id,
            ],
            DOCKER_TIMEOUT,
        )?;
        let subnet = first_ipv4_subnet(&inspected).ok_or_else(|| {
            Error::Parse(format!("no IPv4 subnet in network inspect: {inspected:?}"))
        })?;
        let ips: Vec<Ipv4Addr> = (0..self.processes)
            .map(|i| process_ip(subnet, i))
            .collect::<Option<_>>()
            .ok_or_else(|| {
                Error::InvalidConfig(format!(
                    "network subnet {}/{} is too small for {} processes",
                    Ipv4Addr::from(subnet.0),
                    subnet.1,
                    self.processes
                ))
            })?;
        let addresses: Vec<SocketAddr> = ips
            .iter()
            .map(|ip| SocketAddr::new(IpAddr::V4(*ip), FDB_PORT))
            .collect();
        let cluster_string = coordinator_string(&addresses[..coordinator_count]);

        let dir = tempfile::Builder::new()
            .prefix("fdb-native-testkit-")
            .tempdir()
            .map_err(Error::ClusterFile)?;
        let cluster_file = dir.path().join("fdb.cluster");
        fs::write(&cluster_file, format!("{cluster_string}\n")).map_err(Error::ClusterFile)?;

        // The cluster owns the guards from the first container on (containers drop before the
        // network, which drops before the temp dir).
        let mut cluster = FdbCluster {
            nodes: Vec::with_capacity(self.processes),
            network,
            image: self.image.clone(),
            redundancy: self.redundancy,
            engine: self.engine,
            coordinators: Mutex::new((0..coordinator_count).collect()),
            cluster_string,
            cluster_file,
            _dir: dir,
        };

        for (i, ip) in ips.iter().enumerate() {
            let name = format!("{base}-n{i}");
            let class = self.classes.as_ref().map(|c| c[i].as_str());
            let mut args: Vec<String> = [
                "create",
                "--network",
                &cluster.network.id,
                "--ip",
                &ip.to_string(),
                "--label",
                LABEL,
                "--name",
                &name,
            ]
            .map(str::to_owned)
            .into();
            for env in process_env(&cluster.cluster_string, class, &self.knobs) {
                args.extend(["--env".to_owned(), env]);
            }
            args.push(self.image.clone());
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let created = docker(&args, DOCKER_TIMEOUT)?;
            let id = parse_container_id(&created).ok_or_else(|| {
                Error::Parse(format!(
                    "docker create printed no container id: {created:?}"
                ))
            })?;
            cluster.nodes.push(Node {
                guard: ContainerGuard { id },
                name,
                ip: *ip,
                up: AtomicBool::new(false),
            });
        }
        for node in &cluster.nodes {
            docker(&["start", node.id()], DOCKER_TIMEOUT)?;
            node.set_up(true);
        }

        cluster.form(self.startup_timeout)?;
        Ok(cluster)
    }
}

// ---------------------------------------------------------------------------------------
// FdbCluster
// ---------------------------------------------------------------------------------------

/// A disposable multi-process FoundationDB cluster: one `fdbserver` container per process, on
/// a dedicated Docker network. See the [module docs](crate::cluster).
///
/// Everything is removed, by ID, when the handle is dropped (which blocks for a few seconds).
#[derive(Debug)]
pub struct FdbCluster {
    // Field order is drop order: containers, then the network they are attached to, then the
    // temp dir holding the host's cluster file.
    nodes: Vec<Node>,
    network: NetworkGuard,
    image: String,
    redundancy: Redundancy,
    engine: StorageEngine,
    /// Indexes of the current coordinators.
    coordinators: Mutex<Vec<usize>>,
    /// The cluster string the cluster was created with (stale after a coordinator change).
    cluster_string: String,
    cluster_file: PathBuf,
    _dir: TempDir,
}

impl FdbCluster {
    /// Returns a builder.
    pub fn builder() -> FdbClusterBuilder {
        FdbClusterBuilder::default()
    }

    /// Starts a 3-process `double memory` cluster on [`default_image`]. Blocking (about 40
    /// seconds).
    pub fn start() -> Result<Self> {
        Self::builder().start()
    }

    /// Starts a 3-process `double memory` cluster on `image`. Blocking.
    pub fn start_with_image(image: impl Into<String>) -> Result<Self> {
        Self::builder().image(image).start()
    }

    // ---- accessors ----

    /// The image the cluster runs.
    pub fn image(&self) -> &str {
        &self.image
    }

    /// The redundancy mode the cluster was configured with.
    pub fn redundancy(&self) -> Redundancy {
        self.redundancy
    }

    /// The storage engine the cluster was configured with.
    pub fn storage_engine(&self) -> StorageEngine {
        self.engine
    }

    /// The ID of the dedicated network.
    pub fn network_id(&self) -> &str {
        &self.network.id
    }

    /// Number of processes.
    pub fn process_count(&self) -> usize {
        self.nodes.len()
    }

    /// The container ID of process `index`. Panics if out of range.
    pub fn process_id(&self, index: usize) -> &str {
        self.nodes[index].id()
    }

    /// All container IDs, in process order.
    pub fn process_ids(&self) -> Vec<&str> {
        self.nodes.iter().map(Node::id).collect()
    }

    /// The container name of process `index`. Panics if out of range.
    pub fn process_name(&self, index: usize) -> &str {
        &self.nodes[index].name
    }

    /// The (static) address of process `index`. Panics if out of range.
    pub fn address(&self, index: usize) -> SocketAddr {
        self.nodes[index].address()
    }

    /// The addresses of all processes, in process order.
    pub fn addresses(&self) -> Vec<SocketAddr> {
        self.nodes.iter().map(Node::address).collect()
    }

    /// Whether the handle believes process `index` is running and not paused.
    pub fn is_up(&self, index: usize) -> bool {
        self.nodes[index].is_up()
    }

    /// Indexes of the current coordinators (as set at start or by
    /// [`set_coordinators`](Self::set_coordinators)), in the order given.
    pub fn coordinator_indexes(&self) -> Vec<usize> {
        self.coordinators
            .lock()
            .map_or_else(|e| e.into_inner().clone(), |g| g.clone())
    }

    /// Path of the cluster file on the host. It holds the cluster string the cluster started
    /// with and is **not** updated by [`set_coordinators`](Self::set_coordinators), so a
    /// client using it afterwards must be forwarded to the new coordinators (use
    /// [`refresh_cluster_file`](Self::refresh_cluster_file) for an up-to-date one).
    pub fn cluster_file(&self) -> &Path {
        &self.cluster_file
    }

    /// The cluster string the cluster started with (`docker:docker@ip:4500,...`).
    pub fn cluster_string(&self) -> &str {
        &self.cluster_string
    }

    // ---- fdbcli and status ----

    /// Runs `fdbcli --exec <command>` in the first running process and returns stdout. Fails if
    /// fdbcli exits non-zero (including its own 10 second timeout).
    pub fn fdbcli(&self, command: &str) -> Result<String> {
        self.fdbcli_with_timeout(command, FDBCLI_DEFAULT_TIMEOUT)
    }

    /// [`fdbcli`](Self::fdbcli) with an explicit fdbcli timeout.
    pub fn fdbcli_with_timeout(&self, command: &str, timeout: Duration) -> Result<String> {
        let node = self.live_node()?;
        Self::exec_fdbcli(node, command, timeout)
    }

    /// Runs `fdbcli --exec <command>` in process `index` (which must be running).
    pub fn fdbcli_on(&self, index: usize, command: &str) -> Result<String> {
        Self::exec_fdbcli(&self.nodes[index], command, FDBCLI_DEFAULT_TIMEOUT)
    }

    fn live_node(&self) -> Result<&Node> {
        self.nodes
            .iter()
            .find(|n| n.is_up())
            .ok_or_else(|| Error::InvalidConfig("no running process to run fdbcli in".to_owned()))
    }

    /// `docker exec <id> fdbcli --timeout <t> --exec <command>`; stdout + stderr and whether it
    /// exited successfully.
    fn exec_fdbcli_lenient(
        node: &Node,
        command: &str,
        timeout: Duration,
    ) -> Result<(bool, String)> {
        let secs = timeout.as_secs().max(1).to_string();
        let out = docker_raw(
            &[
                "exec",
                node.id(),
                "fdbcli",
                "--timeout",
                &secs,
                "--exec",
                command,
            ],
            timeout.max(EXEC_TIMEOUT) + EXEC_MARGIN,
        )?;
        let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&out.stderr));
        Ok((out.status.success(), text))
    }

    fn exec_fdbcli(node: &Node, command: &str, timeout: Duration) -> Result<String> {
        let (ok, text) = Self::exec_fdbcli_lenient(node, command, timeout)?;
        if ok {
            Ok(text)
        } else {
            Err(Error::Fdbcli {
                command: command.to_owned(),
                output: text.trim().to_owned(),
            })
        }
    }

    /// `status json`, parsed. Re-exported as [`serde_json::Value`].
    pub fn status_json(&self) -> Result<serde_json::Value> {
        let out = self.fdbcli_with_timeout("status json", FDBCLI_DEFAULT_TIMEOUT)?;
        parse_status_output(&out)
    }

    /// The health summary from `status json`.
    pub fn health(&self) -> Result<ClusterHealth> {
        self.status_json().map(|s| ClusterHealth::from_status(&s))
    }

    fn up_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.is_up()).count()
    }

    /// Polls `check` (which gets the health summary) until it returns `Some`, or the timeout
    /// passes. Status errors while the cluster recovers are not fatal.
    fn wait_for<T>(
        &self,
        wanted: &'static str,
        timeout: Duration,
        mut check: impl FnMut(&ClusterHealth) -> Option<T>,
    ) -> Result<T> {
        let deadline = Instant::now() + timeout;
        loop {
            let last = match self.status_json() {
                Ok(status) => {
                    let health = ClusterHealth::from_status(&status);
                    if let Some(v) = check(&health) {
                        return Ok(v);
                    }
                    health.to_string()
                }
                Err(e) => e.to_string(),
            };
            if Instant::now() >= deadline {
                return Err(Error::ClusterNotReady {
                    wanted,
                    timeout,
                    last,
                });
            }
            thread::sleep(POLL_INTERVAL * 2);
        }
    }

    /// Waits until the cluster is available, fully recovered, its data is healthy with nothing
    /// moving, and exactly one process per running container reports in. Returns the final
    /// summary. Use [`DEFAULT_WAIT_TIMEOUT`] unless there is a reason not to.
    pub fn wait_healthy(&self, timeout: Duration) -> Result<ClusterHealth> {
        let expected = self.up_count();
        self.wait_for("healthy", timeout, |h| {
            h.is_healthy(expected).then(|| h.clone())
        })
    }

    /// Waits until exactly one process per running container reports in `status json` and the
    /// database is available (a weaker, quicker condition than
    /// [`wait_healthy`](Self::wait_healthy)).
    pub fn wait_processes(&self, timeout: Duration) -> Result<()> {
        let expected = self.up_count();
        self.wait_for("showing the expected processes", timeout, |h| {
            (h.available && h.processes == expected).then_some(())
        })
    }

    /// The roles process `index` currently has in `status json` (`storage`, `log`,
    /// `coordinator`, `master`, `commit_proxy`, ...); empty if it is not reporting.
    pub fn process_roles(&self, index: usize) -> Result<Vec<String>> {
        let status = self.status_json()?;
        let ip = self.nodes[index].ip.to_string();
        Ok(roles_by_ip(&status).remove(&ip).unwrap_or_default())
    }

    /// Indexes of the processes that currently have `role`.
    pub fn processes_with_role(&self, role: &str) -> Result<Vec<usize>> {
        let by_ip = roles_by_ip(&self.status_json()?);
        Ok(self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| {
                by_ip
                    .get(&n.ip.to_string())
                    .is_some_and(|roles| roles.iter().any(|r| r == role))
            })
            .map(|(i, _)| i)
            .collect())
    }

    // ---- start-up ----

    /// Waits for the containers, configures the database and waits for health.
    fn form(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;

        // Every container runs and accepts connections from the host.
        for node in &self.nodes {
            self.wait_node_listening(node, deadline, timeout)?;
        }

        // `configure new` in the first process, until the cluster has formed.
        let command = format!(
            "configure new {} {}",
            self.redundancy.as_str(),
            self.engine.as_str()
        );
        loop {
            let (_, out) =
                Self::exec_fdbcli_lenient(&self.nodes[0], &command, Duration::from_secs(10))?;
            if configure_succeeded(&out) {
                break;
            }
            if Instant::now() >= deadline {
                return Err(Error::ClusterNotReady {
                    wanted: "configured",
                    timeout,
                    last: format!("{}; {}", out.trim(), self.logs_tail_of(0)),
                });
            }
            self.ensure_running(0)?;
            thread::sleep(POLL_INTERVAL);
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        self.wait_healthy(remaining).map(drop).map_err(|e| match e {
            Error::ClusterNotReady { wanted, last, .. } => Error::ClusterNotReady {
                wanted,
                timeout,
                last: format!("{last}; {}", self.logs_tail_of(0)),
            },
            e => e,
        })
    }

    fn logs_tail_of(&self, index: usize) -> String {
        format!(
            "logs of process {index}: {:?}",
            log_tail(self.nodes[index].id())
        )
    }

    fn ensure_running(&self, index: usize) -> Result<()> {
        let node = &self.nodes[index];
        let running = docker(
            &["inspect", "-f", "{{.State.Running}}", node.id()],
            DOCKER_TIMEOUT,
        )?;
        if running.trim() == "true" {
            Ok(())
        } else {
            Err(Error::ContainerExited {
                container: node.id().to_owned(),
                logs: log_tail(node.id()),
            })
        }
    }

    fn wait_node_listening(&self, node: &Node, deadline: Instant, timeout: Duration) -> Result<()> {
        let index = self
            .nodes
            .iter()
            .position(|n| n.id() == node.id())
            .unwrap_or(0);
        loop {
            self.ensure_running(index)?;
            let last = match TcpStream::connect_timeout(&node.address(), TCP_TIMEOUT) {
                Ok(_) => return Ok(()),
                Err(e) => format!("host cannot connect to {}: {e}", node.address()),
            };
            if Instant::now() >= deadline {
                return Err(Error::ClusterNotReady {
                    wanted: "listening",
                    timeout,
                    last,
                });
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    // ---- process faults ----

    fn node_checked(&self, index: usize) -> Result<&Node> {
        self.nodes.get(index).ok_or_else(|| {
            Error::InvalidConfig(format!(
                "process index {index} out of range (cluster has {})",
                self.nodes.len()
            ))
        })
    }

    /// Kills process `index` with SIGKILL (`docker kill` by exact ID). Returns immediately;
    /// [`wait_healthy`](Self::wait_healthy) accounts for the process leaving. Undo with
    /// [`start_process`](Self::start_process).
    pub fn kill(&self, index: usize) -> Result<()> {
        let node = self.node_checked(index)?;
        docker(&["kill", node.id()], DOCKER_TIMEOUT)?;
        node.set_up(false);
        Ok(())
    }

    /// Stops process `index` gracefully (`docker stop -t 1`: SIGTERM, then SIGKILL).
    pub fn stop(&self, index: usize) -> Result<()> {
        let node = self.node_checked(index)?;
        docker(&["stop", "-t", "1", node.id()], DOCKER_TIMEOUT)?;
        node.set_up(false);
        Ok(())
    }

    /// Starts a process stopped with [`kill`](Self::kill) or [`stop`](Self::stop) (`docker
    /// start`) and waits until it reports in `status json` again. It comes back at the same
    /// address.
    pub fn start_process(&self, index: usize) -> Result<()> {
        let node = self.node_checked(index)?;
        docker(&["start", node.id()], DOCKER_TIMEOUT)?;
        self.rejoined(index)
    }

    /// Restarts process `index` (`docker restart -t 1`) and waits until it reports in again.
    pub fn restart(&self, index: usize) -> Result<()> {
        let node = self.node_checked(index)?;
        docker(&["restart", "-t", "1", node.id()], DOCKER_TIMEOUT)?;
        self.rejoined(index)
    }

    fn rejoined(&self, index: usize) -> Result<()> {
        let node = &self.nodes[index];
        let ip_out = docker(
            &[
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                node.id(),
            ],
            DOCKER_TIMEOUT,
        )?;
        // Static addressing makes this an invariant check, not an expected failure.
        if let Ok(after) = ip_out.trim().parse::<IpAddr>() {
            if after != IpAddr::V4(node.ip) {
                return Err(Error::AddressChanged {
                    container: node.id().to_owned(),
                    before: IpAddr::V4(node.ip),
                    after,
                });
            }
        }
        node.set_up(true);
        self.wait_processes(DEFAULT_WAIT_TIMEOUT)
    }

    /// Freezes process `index` (`docker pause`, the cgroup freezer): its sockets stay open but
    /// nothing is answered. The cluster notices after a few seconds. Undo with
    /// [`unpause`](Self::unpause).
    pub fn pause(&self, index: usize) -> Result<()> {
        let node = self.node_checked(index)?;
        docker(&["pause", node.id()], DOCKER_TIMEOUT)?;
        node.set_up(false);
        Ok(())
    }

    /// Thaws a process frozen with [`pause`](Self::pause) and waits until it reports in again.
    pub fn unpause(&self, index: usize) -> Result<()> {
        let node = self.node_checked(index)?;
        docker(&["unpause", node.id()], DOCKER_TIMEOUT)?;
        node.set_up(true);
        self.wait_processes(DEFAULT_WAIT_TIMEOUT)
    }

    // ---- exclude / include ----

    /// Excludes process `index` (`fdbcli exclude <ip>:4500`) and waits until the data has been
    /// moved off it and fdbcli reports it safe to remove. The process keeps running. Needs
    /// enough other processes to hold the data: use 4 or more for `double`.
    pub fn exclude(&self, index: usize) -> Result<()> {
        let node = self.node_checked(index)?;
        let command = format!("exclude {}", node.address());
        let out = self.fdbcli_with_timeout(&command, EXCLUDE_TIMEOUT)?;
        if out.contains("safe to remove") && !out.contains("ERROR") {
            Ok(())
        } else {
            Err(Error::Fdbcli {
                command,
                output: out.trim().to_owned(),
            })
        }
    }

    /// Reverses [`exclude`](Self::exclude) (`fdbcli include <ip>:4500`).
    pub fn include(&self, index: usize) -> Result<()> {
        let node = self.node_checked(index)?;
        let command = format!("include {}", node.address());
        let out = self.fdbcli(&command)?;
        if out.contains("ERROR") {
            return Err(Error::Fdbcli {
                command,
                output: out.trim().to_owned(),
            });
        }
        Ok(())
    }

    // ---- coordinators ----

    /// Changes the coordinators to the processes `indexes` (`fdbcli coordinators ...`) and
    /// returns the new cluster string. The processes must be running.
    ///
    /// The cluster file on the host ([`cluster_file`](Self::cluster_file)) keeps the old
    /// coordinators, which forward clients to the new ones; call
    /// [`refresh_cluster_file`](Self::refresh_cluster_file) to rewrite it. Note that the
    /// cluster ID (the part after `docker:`) changes with the coordinators.
    pub fn set_coordinators(&self, indexes: &[usize]) -> Result<String> {
        if indexes.is_empty() {
            return Err(Error::InvalidConfig("no coordinators given".to_owned()));
        }
        let mut addrs = Vec::new();
        for &i in indexes {
            addrs.push(self.node_checked(i)?.address().to_string());
        }
        let command = format!("coordinators {}", addrs.join(" "));
        let out = self.fdbcli_with_timeout(&command, Duration::from_secs(30))?;
        if !out.contains("Coordination state changed") {
            return Err(Error::Fdbcli {
                command,
                output: out.trim().to_owned(),
            });
        }
        if let Ok(mut guard) = self.coordinators.lock() {
            *guard = indexes.to_vec();
        }
        self.current_cluster_string()
    }

    /// The cluster string a running process currently has in its own cluster file (it follows
    /// coordinator changes).
    pub fn current_cluster_string(&self) -> Result<String> {
        let node = self.live_node()?;
        let out = docker(
            &["exec", node.id(), "cat", CONTAINER_CLUSTER_FILE],
            EXEC_TIMEOUT,
        )?;
        parse_cluster_file(&out)
            .ok_or_else(|| Error::Parse(format!("empty cluster file in container: {out:?}")))
    }

    /// Rewrites the host cluster file with [`current_cluster_string`](Self::current_cluster_string)
    /// and returns the string.
    pub fn refresh_cluster_file(&self) -> Result<String> {
        let current = self.current_cluster_string()?;
        fs::write(&self.cluster_file, format!("{current}\n")).map_err(Error::ClusterFile)?;
        Ok(current)
    }

    // ---- data ----

    /// Writes `count` keys named `<prefix><8 digit index>` with `value_size`-byte values, in
    /// transactions of about 2 MB, through `fdbcli` in the first running process. `prefix` may
    /// contain only ASCII letters, digits and `_-.:/`.
    pub fn write_keys(&self, prefix: &str, count: usize, value_size: usize) -> Result<()> {
        if !valid_key_prefix(prefix) || value_size == 0 {
            return Err(Error::InvalidConfig(format!(
                "bad key prefix {prefix:?} or value size {value_size}"
            )));
        }
        let node = self.live_node()?;
        let (script, batches) = write_script(prefix, count, value_size);
        // fdbcli reads commands from stdin; `timeout` stops it if the database is unavailable.
        let out = docker_raw_input(
            &[
                "exec",
                "-i",
                node.id(),
                "timeout",
                "-k",
                "5",
                "60",
                "fdbcli",
            ],
            script.into_bytes(),
            Duration::from_secs(90),
        )?;
        let text = String::from_utf8_lossy(&out.stdout).into_owned()
            + &String::from_utf8_lossy(&out.stderr);
        if text.matches("Committed (").count() == batches && !text.contains("ERROR") {
            Ok(())
        } else {
            Err(Error::Fdbcli {
                command: format!("write_keys({prefix:?}, {count}, {value_size})"),
                output: text.trim().to_owned(),
            })
        }
    }

    /// Reads one key through fdbcli (`get <key>`); `key` must not contain whitespace or
    /// special characters.
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        if !valid_key_prefix(key) {
            return Err(Error::InvalidConfig(format!("bad key {key:?}")));
        }
        let command = format!("get {key}");
        let out = self.fdbcli(&command)?;
        parse_get_output(&out).ok_or(Error::Fdbcli {
            command,
            output: out.trim().to_owned(),
        })
    }

    /// Switches data distribution on or off (`fdbcli datadistribution on|off`). Off, no shard
    /// moves and no re-replication happen.
    pub fn set_data_distribution(&self, enabled: bool) -> Result<()> {
        let command = format!("datadistribution {}", if enabled { "on" } else { "off" });
        let out = self.fdbcli(&command)?;
        if out.contains("ERROR") {
            return Err(Error::Fdbcli {
                command,
                output: out.trim().to_owned(),
            });
        }
        Ok(())
    }

    /// The number of shards (`cluster.data.partitions_count`), if data distribution reports it.
    pub fn partitions(&self) -> Result<Option<u64>> {
        self.health().map(|h| h.partitions)
    }

    /// Waits until there are at least `min` shards and nothing is moving; returns the count.
    pub fn wait_for_partitions(&self, min: u64, timeout: Duration) -> Result<u64> {
        self.wait_for("split into enough shards", timeout, |h| {
            h.partitions.filter(|&p| p >= min && h.moving_bytes == 0)
        })
    }
}

impl fmt::Display for FdbCluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} processes of {} ({} {}, {})",
            self.nodes.len(),
            self.image,
            self.redundancy.as_str(),
            self.engine.as_str(),
            self.cluster_string
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn subnet_and_process_addresses() {
        let subnet = parse_subnet("172.18.0.0/16").unwrap();
        assert_eq!(subnet, (u32::from(Ipv4Addr::new(172, 18, 0, 0)), 16));
        assert_eq!(process_ip(subnet, 0), Some(Ipv4Addr::new(172, 18, 0, 10)));
        assert_eq!(process_ip(subnet, 4), Some(Ipv4Addr::new(172, 18, 0, 14)));
        // Host bits in the CIDR are masked off.
        assert_eq!(
            parse_subnet("10.1.2.3/24").unwrap().0,
            u32::from(Ipv4Addr::new(10, 1, 2, 0))
        );
        // A /27 has 32 addresses: .10 to .14 fit, and so do more, up to the broadcast address.
        let small = parse_subnet("192.168.5.0/27").unwrap();
        assert_eq!(process_ip(small, 4), Some(Ipv4Addr::new(192, 168, 5, 14)));
        assert_eq!(process_ip(small, 20), Some(Ipv4Addr::new(192, 168, 5, 30)));
        assert_eq!(process_ip(small, 21), None);
        // Too small.
        let tiny = parse_subnet("192.168.5.0/29").unwrap();
        assert_eq!(process_ip(tiny, 4), None);
        assert_eq!(parse_subnet("garbage"), None);
        assert_eq!(parse_subnet("10.0.0.0/33"), None);
        assert_eq!(parse_subnet("fd00::/64"), None);
        assert_eq!(
            first_ipv4_subnet("fd00::/64\n172.19.0.0/16\n"),
            Some((u32::from(Ipv4Addr::new(172, 19, 0, 0)), 16))
        );
        assert_eq!(first_ipv4_subnet("\n"), None);
    }

    #[test]
    fn cluster_string_and_file_parsing() {
        let addrs: Vec<SocketAddr> = ["172.18.0.10:4500", "172.18.0.11:4500", "172.18.0.12:4500"]
            .iter()
            .map(|a| a.parse().unwrap())
            .collect();
        assert_eq!(
            coordinator_string(&addrs),
            "docker:docker@172.18.0.10:4500,172.18.0.11:4500,172.18.0.12:4500"
        );
        let file = "# DO NOT EDIT!\n# This file is auto-generated\ndocker:abc@172.18.0.11:4500,172.18.0.12:4500\n";
        assert_eq!(
            parse_cluster_file(file).as_deref(),
            Some("docker:abc@172.18.0.11:4500,172.18.0.12:4500")
        );
        assert_eq!(parse_cluster_file("# only comments\n\n"), None);
    }

    #[test]
    fn env_for_a_process() {
        let knobs = vec![("MIN_SHARD_BYTES".to_owned(), "1".to_owned())];
        assert_eq!(
            process_env("docker:docker@1.2.3.4:4500", Some("storage"), &knobs),
            [
                "FDB_CLUSTER_FILE_CONTENTS=docker:docker@1.2.3.4:4500",
                "FDB_PROCESS_CLASS=storage",
                "FDB_KNOB_min_shard_bytes=1",
            ]
        );
        assert_eq!(
            process_env("docker:docker@1.2.3.4:4500", None, &[]),
            ["FDB_CLUSTER_FILE_CONTENTS=docker:docker@1.2.3.4:4500"]
        );
    }

    #[test]
    fn builder_validation() {
        let b = FdbCluster::builder();
        assert_eq!(b.validate().unwrap(), 3);
        assert_eq!(b.clone().processes(5).validate().unwrap(), 3);
        assert_eq!(
            b.clone().processes(4).coordinators(4).validate().unwrap(),
            4
        );
        for bad in [
            b.clone().processes(2),
            b.clone().processes(6),
            b.clone().coordinators(0),
            b.clone().coordinators(4),
            b.clone().process_classes(["storage", "storage"]),
        ] {
            assert!(
                matches!(bad.validate(), Err(Error::InvalidConfig(_))),
                "{bad:?}"
            );
        }
        assert!(
            b.clone()
                .process_classes(["a", "b", "c"])
                .validate()
                .is_ok()
        );
        let small = b.small_shards();
        assert_eq!(small.knobs.len(), 2);
    }

    fn sample_status() -> serde_json::Value {
        json!({"cluster": {
            "database_available": true,
            "recovery_state": {"name": "fully_recovered"},
            "data": {
                "state": {"healthy": true, "name": "healthy", "min_replicas_remaining": 2},
                "partitions_count": 10,
                "moving_data": {"in_flight_bytes": 0, "in_queue_bytes": 0}
            },
            "processes": {
                "a": {"address": "172.18.0.10:4500", "roles": [{"role": "storage"}, {"role": "coordinator"}]},
                "b": {"address": "172.18.0.11:4500", "roles": []},
                "c": {"address": "172.18.0.12:4500:tls"}
            }
        }})
    }

    #[test]
    fn health_from_status() {
        let status = sample_status();
        let health = ClusterHealth::from_status(&status);
        assert_eq!(health.processes, 3);
        assert_eq!(health.partitions, Some(10));
        assert!(health.is_healthy(3));
        assert!(!health.is_healthy(2));

        let mut moving = status.clone();
        moving["cluster"]["data"]["moving_data"]["in_queue_bytes"] = json!(5);
        assert!(!ClusterHealth::from_status(&moving).is_healthy(3));

        let mut recovering = status.clone();
        recovering["cluster"]["recovery_state"]["name"] = json!("accepting_commits");
        assert!(!ClusterHealth::from_status(&recovering).is_healthy(3));

        // While data distribution initialises there is no `data.state.healthy`.
        let mut init = status;
        init["cluster"]["data"] = json!({"state": {"name": "initializing"}});
        let health = ClusterHealth::from_status(&init);
        assert!(!health.is_healthy(3));
        assert_eq!(health.data_state, "initializing");
        assert_eq!(health.partitions, None);

        // Garbage never reads as healthy.
        assert!(!ClusterHealth::from_status(&json!({})).is_healthy(0));
    }

    #[test]
    fn roles_from_status() {
        let roles = roles_by_ip(&sample_status());
        assert_eq!(roles["172.18.0.10"], ["storage", "coordinator"]);
        assert!(roles["172.18.0.11"].is_empty());
        assert!(roles.contains_key("172.18.0.12"));
        assert!(roles_by_ip(&json!({})).is_empty());
    }

    #[test]
    fn status_output_parsing() {
        let v =
            parse_status_output("Using cluster file `x'.\n{\"cluster\": {\"a\": 1}}\n").unwrap();
        assert_eq!(v["cluster"]["a"], 1);
        assert!(matches!(parse_status_output("nope"), Err(Error::Status(_))));
        assert!(matches!(
            parse_status_output("{broken"),
            Err(Error::Status(_))
        ));
    }

    #[test]
    fn get_output_parsing() {
        assert_eq!(parse_get_output("`k' is `v'\n"), Some(Some("v".to_owned())));
        assert_eq!(parse_get_output("`k' is `'\n"), Some(Some(String::new())));
        assert_eq!(parse_get_output("`k': not found\n"), Some(None));
        assert_eq!(parse_get_output("ERROR: something\n"), None);
    }

    #[test]
    fn key_writing_script() {
        assert!(valid_key_prefix("k"));
        assert!(valid_key_prefix("a/b-c_d.e:f1"));
        assert!(!valid_key_prefix(""));
        assert!(!valid_key_prefix("a b"));
        assert!(!valid_key_prefix("a;b"));
        assert!(!valid_key_prefix("a\"b"));

        let (script, batches) = write_script("k", 3, 4);
        assert_eq!(batches, 1);
        assert_eq!(
            script,
            "writemode on\nbegin\nset k00000000 xxxx\nset k00000001 xxxx\nset k00000002 xxxx\ncommit\n"
        );
        // Big values split into several transactions.
        let (script, batches) = write_script("k", 10, 500_000);
        assert_eq!(batches, 4);
        assert_eq!(script.matches("commit\n").count(), 4);
        assert_eq!(script.matches("set k").count(), 10);
        let (_, batches) = write_script("k", 0, 10);
        assert_eq!(batches, 0);
    }
}
