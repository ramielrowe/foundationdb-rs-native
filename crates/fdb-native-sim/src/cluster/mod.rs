//! A model cluster that the real client can run transactions against.
//!
//! It speaks the wire protocol (handshake, frames, pings, the schema's request and reply
//! messages of the cluster's [`SchemaVersion`]) from simulated processes, and is just
//! faithful enough for the client's logic:
//!
//! * **Coordinators** answer `OpenDatabaseCoordRequest` long polls with the current
//!   `ClientDBInfo` (at once if the client's is stale, else when it changes).
//! * **GRV proxies** hand out read versions from the model's version clock (below), or a
//!   throttling error (`grv_proxy_memory_limit_exceeded`) while throttled.
//! * **Commit proxies** resolve each commit against the model's conflict history
//!   (`not_committed`, `transaction_too_old`) and apply it at a new version, or answer
//!   `commit_proxy_memory_limit_exceeded` while throttled. They serve key locations from the
//!   shard map. A commit proxy can be made to crash just before or just after applying a
//!   commit ([`SimCluster::arm_commit_crash`]): the client then sees its connection fail
//!   with the commit's fate unknown.
//! * **Storage servers** serve `getValue`, `getKey` and `getKeyValues` for the shards their
//!   team holds (`wrong_shard_server` otherwise), at versions within the MVCC window
//!   (`transaction_too_old` below it), waiting up to 1 s for a version they do not have yet
//!   (`future_version` after that; a server can be made to lag), and answering
//!   `process_behind` while marked behind.
//! * **Recoveries** ([`SimCluster::recover`]) make a new generation of proxies on the
//!   stateless processes with new endpoint tokens, retire the old ones (their pending
//!   requests get `broken_promise`, later ones `ENDPOINT_NOT_FOUND`), optionally publish an
//!   info without proxies first, and may make the version clock jump. Killing a process that
//!   hosts a current proxy triggers one after a detection delay.
//! * **Restarts**: a restarted storage server comes back with new endpoint tokens, as an FDB
//!   storage role does (cached locations then hit `ENDPOINT_NOT_FOUND`).
//!
//! Every commit applied is in the [log](SimCluster::log), with its version and time: the
//! invariants compare the client's outcomes with it.

mod model;
mod process;

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use fdb_native_schema::domain::{
    ClientDbInfo, Commit, CommitId, CommitProxy, GetKey, GetKeyReply, GetKeyServerLocations,
    GetKeyValues, GetKeyValuesReply, GetReadVersion, GetValue, GetValueReply, GrvProxy,
    INVALID_VERSION, KeyRange, KeySelector, KeySelectorRef, KeyServerLocations, OpenDatabaseCoord,
    ReadVersion, StorageServer, TenantMode,
};
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::connection::well_known;
use fdb_native_transport::runtime::{Instant, TaskHandle};
use fdb_native_wire::{
    Endpoint, KeyValueRef, KeyValueVector, NetworkAddress, NetworkAddressList, Uid,
};

pub use model::{LogEntry, MVCC_WINDOW};

use crate::executor::lock;
use crate::rng::SimRng;
use crate::trace::TraceLevel;
use crate::{KillMode, NodeId, Sim, SimTask};
use model::{Model, Resolution};
use process::{Incoming, Process, Reply};

/// FDB error codes the cluster answers with.
pub mod codes {
    /// `wrong_shard_server`.
    pub const WRONG_SHARD_SERVER: u16 = 1001;
    /// `transaction_too_old`.
    pub const TRANSACTION_TOO_OLD: u16 = 1007;
    /// `future_version`.
    pub const FUTURE_VERSION: u16 = 1009;
    /// `process_behind`.
    pub const PROCESS_BEHIND: u16 = 1037;
    /// `commit_proxy_memory_limit_exceeded`.
    pub const COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED: u16 = 1042;
    /// `grv_proxy_memory_limit_exceeded`.
    pub const GRV_PROXY_MEMORY_LIMIT_EXCEEDED: u16 = 1078;
}

/// The end of the key space a shard map covers.
const KEYSPACE_END: &[u8] = b"\xff\xff";

/// How long a storage server waits for a version it does not have before `future_version`
/// (`FUTURE_VERSION_DELAY`).
const FUTURE_VERSION_DELAY: Duration = Duration::from_secs(1);

/// The cluster's shape.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClusterConfig {
    /// The protocol release the processes speak.
    pub schema: SchemaVersion,
    /// Coordinators (1 to 5).
    pub coordinators: usize,
    /// Stateless processes, which host the proxies.
    pub stateless: usize,
    /// Storage processes.
    pub storage: usize,
    /// Shards at the start.
    pub shards: usize,
    /// Storage servers per shard.
    pub replication: usize,
    /// GRV proxies per generation.
    pub grv_proxies: usize,
    /// Commit proxies per generation.
    pub commit_proxies: usize,
}

impl Default for ClusterConfig {
    fn default() -> ClusterConfig {
        ClusterConfig {
            schema: SchemaVersion::V7_4,
            coordinators: 3,
            stateless: 3,
            storage: 3,
            shards: 3,
            replication: 2,
            grv_proxies: 1,
            commit_proxies: 2,
        }
    }
}

impl ClusterConfig {
    /// A shape drawn from `rng`.
    pub fn randomized(rng: &mut SimRng) -> ClusterConfig {
        let storage = rng.range(2, 4) as usize;
        let stateless = rng.range(2, 4) as usize;
        ClusterConfig {
            schema: *rng.pick(&SchemaVersion::ALL),
            coordinators: *rng.pick(&[1, 3, 3, 5]),
            stateless,
            storage,
            shards: rng.range(1, 5) as usize,
            replication: rng.range(1, storage.min(3) as u64) as usize,
            grv_proxies: rng.range(1, stateless.min(2) as u64) as usize,
            commit_proxies: rng.range(1, stateless.min(3) as u64) as usize,
        }
    }
}

/// When an armed commit proxy crashes ([`SimCluster::arm_commit_crash`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashPoint {
    /// After it received a commit, before resolving it: the commit is not applied.
    BeforeApply,
    /// After it applied a commit, before replying: the commit is applied.
    AfterApply,
}

/// Counters of what the cluster did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClusterStats {
    /// Read versions handed out.
    pub read_versions: u64,
    /// Commits applied.
    pub commits: u64,
    /// Commits that conflicted.
    pub conflicts: u64,
    /// `transaction_too_old` answers (resolver and storage).
    pub too_old: u64,
    /// `future_version` answers.
    pub future_version: u64,
    /// `wrong_shard_server` answers.
    pub wrong_shard: u64,
    /// `process_behind` answers.
    pub process_behind: u64,
    /// Throttling answers.
    pub throttled: u64,
    /// Proxies that crashed around a commit.
    pub commit_crashes: u64,
    /// Recoveries.
    pub recoveries: u64,
    /// Storage reads served.
    pub reads: u64,
    /// `OpenDatabaseCoordRequest`s the coordinators received (the long poll).
    pub open_database_requests: u64,
    /// `GetReadVersionRequest`s the GRV proxies received.
    pub grv_requests: u64,
    /// The sum of their `transactionCount`s: the transactions the client said it started.
    pub grv_transactions: u64,
    /// `GetKeyServerLocationsRequest`s the commit proxies received.
    pub location_requests: u64,
}

/// A process of the cluster, as [`SimCluster::processes`] lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessInfo {
    /// Its index (for [`SimCluster::kill_process`] and friends).
    pub index: usize,
    /// Its node.
    pub node: NodeId,
    /// Its name (`coord-1`, `stateless-2`, `storage-1`, ...).
    pub name: String,
    /// Its listening address.
    pub addr: SocketAddr,
    /// What it is.
    pub class: ProcessClass,
    /// Whether it is up.
    pub alive: bool,
}

/// What a process is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessClass {
    /// A coordinator.
    Coordinator,
    /// A stateless process (proxies).
    Stateless,
    /// A storage server (its index among the storage servers).
    Storage(usize),
}

#[derive(Clone, Debug)]
struct Shard {
    begin: Vec<u8>,
    end: Vec<u8>,
    team: Vec<usize>,
}

struct StorageState {
    process: usize,
    iface: StorageServer,
    restarts: u64,
    lag: Duration,
    behind_until: Instant,
}

struct Role {
    process: usize,
    token: Uid,
    tasks: Vec<SimTask<()>>,
}

impl Role {
    fn track(&mut self, task: SimTask<()>) {
        self.tasks.retain(|t| !t.is_finished());
        self.tasks.push(task);
    }

    fn retire(&mut self, process: &Process, tokens: &[Uid]) {
        for t in tokens {
            process.unhandle(*t);
        }
        for t in self.tasks.drain(..) {
            t.abort();
        }
    }
}

struct State {
    model: Model,
    generation: u64,
    info: ClientDbInfo,
    waiters: Vec<(Uid, Reply)>,
    shards: Vec<Shard>,
    storage: Vec<StorageState>,
    grv: Vec<Role>,
    commit: Vec<Role>,
    grv_throttle_until: Instant,
    commit_throttle_until: Instant,
    commit_crash: Option<(CrashPoint, KillMode)>,
    stats: ClusterStats,
    rng: SimRng,
    /// A recovery is scheduled (a proxy host died).
    recovery_pending: bool,
    /// Storage servers removed for good ([`SimCluster::remove_storage`]): in no team, never
    /// restarted.
    removed: BTreeSet<usize>,
}

struct Inner {
    sim: Sim,
    config: ClusterConfig,
    controller: NodeId,
    processes: Vec<Arc<Process>>,
    classes: Vec<ProcessClass>,
    state: Mutex<State>,
    violations: Arc<Mutex<Vec<String>>>,
}

/// The simulated cluster. Cheap to clone (a shared handle).
#[derive(Clone)]
pub struct SimCluster {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for SimCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimCluster")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

fn ip(subnet: u8, host: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(10, 0, subnet, host))
}

/// A stream token (odd `first`: FlowTransport's stream flag) unique to a role's kind,
/// generation and index. `Endpoint::adjusted` adds the stream index to bits 32 and up.
fn token(kind: u64, generation: u64, index: u64) -> Uid {
    Uid::new(
        (kind << 56) | ((generation & 0xfff) << 44) | ((index & 0xf) << 40) | 1,
        0x5151_0000_0000_0000 | (kind << 40) | (generation << 32),
    )
}

fn endpoint(addr: SocketAddr, token: Uid) -> Endpoint {
    Endpoint::new(
        NetworkAddressList {
            address: NetworkAddress::new(addr.ip().into(), addr.port(), true, false),
            secondary_address: None,
        },
        token,
    )
}

fn is_backward(sel: &KeySelector) -> bool {
    !sel.or_equal && sel.offset <= 0
}

/// `selectorInRange`.
fn selector_in_range(sel: &KeySelector, begin: &[u8], end: &[u8]) -> bool {
    if is_backward(sel) {
        sel.key.as_slice() > begin && sel.key.as_slice() <= end
    } else {
        sel.key.as_slice() >= begin && sel.key.as_slice() < end
    }
}

/// Where `sel` lands among `keys` (the keys of the shard `[begin, end)`): the index of the
/// key, which may be off either end.
fn selector_target(keys: &[Vec<u8>], sel: &KeySelector) -> i64 {
    let below = keys
        .iter()
        .filter(|k| {
            if sel.or_equal {
                k.as_slice() <= sel.key.as_slice()
            } else {
                k.as_slice() < sel.key.as_slice()
            }
        })
        .count() as i64;
    below + i64::from(sel.offset) - 1
}

impl SimCluster {
    /// Builds the cluster on `sim` and starts its processes (generation 1).
    pub fn start(sim: &Sim, config: ClusterConfig, rng: SimRng) -> SimCluster {
        let version = config.schema.protocol_version();
        let violations = Arc::new(Mutex::new(Vec::new()));
        let mut processes = Vec::new();
        let mut classes = Vec::new();
        let mut add = |name: String, ip: IpAddr, class: ProcessClass| {
            let node = sim.add_node(name, ip);
            processes.push(Process::new(
                sim,
                node,
                SocketAddr::new(ip, 4500),
                version,
                violations.clone(),
            ));
            classes.push(class);
        };
        for i in 0..config.coordinators {
            add(
                format!("coord-{}", i + 1),
                ip(1, i as u8 + 1),
                ProcessClass::Coordinator,
            );
        }
        for i in 0..config.stateless {
            add(
                format!("stateless-{}", i + 1),
                ip(2, i as u8 + 1),
                ProcessClass::Stateless,
            );
        }
        for i in 0..config.storage {
            add(
                format!("storage-{}", i + 1),
                ip(3, i as u8 + 1),
                ProcessClass::Storage(i),
            );
        }
        let controller = sim.add_node("controller", ip(0, 1));
        let storage_procs: Vec<usize> = classes
            .iter()
            .enumerate()
            .filter(|(_, c)| matches!(c, ProcessClass::Storage(_)))
            .map(|(i, _)| i)
            .collect();
        let mut rng = rng;
        let storage = storage_procs
            .iter()
            .enumerate()
            .map(|(i, p)| StorageState {
                process: *p,
                iface: StorageServer {
                    id: Uid::new(0x5500 + i as u64, 0x55),
                    locality: BTreeMap::new(),
                    base: endpoint(processes[*p].addr, token(3, 0, i as u64)),
                    tss_pair_id: None,
                    accepting_requests: true,
                },
                restarts: 0,
                lag: Duration::ZERO,
                behind_until: Instant::ZERO,
            })
            .collect::<Vec<_>>();
        // Shard boundaries among the keys the workloads use.
        let mut bounds: Vec<Vec<u8>> = Vec::new();
        for _ in 1..config.shards {
            let b = format!("{}", (b'a' + rng.below(26) as u8) as char).into_bytes();
            let suffix = rng.below(100);
            let mut key = b;
            key.extend_from_slice(format!("/{suffix:02}").as_bytes());
            bounds.push(key);
        }
        bounds.sort();
        bounds.dedup();
        let mut shards = Vec::new();
        let mut begin = Vec::new();
        for end in bounds.into_iter().chain([KEYSPACE_END.to_vec()]) {
            let team = random_team(
                &mut rng,
                storage.len(),
                config.replication,
                &BTreeSet::new(),
            );
            shards.push(Shard {
                begin: std::mem::replace(&mut begin, end.clone()),
                end,
                team,
            });
        }
        let state = State {
            model: Model::new(),
            generation: 0,
            info: empty_info(0, config.schema),
            waiters: Vec::new(),
            shards,
            storage,
            grv: Vec::new(),
            commit: Vec::new(),
            grv_throttle_until: Instant::ZERO,
            commit_throttle_until: Instant::ZERO,
            commit_crash: None,
            stats: ClusterStats::default(),
            rng,
            recovery_pending: false,
            removed: BTreeSet::new(),
        };
        let cluster = SimCluster {
            inner: Arc::new(Inner {
                sim: sim.clone(),
                config,
                controller,
                processes,
                classes,
                state: Mutex::new(state),
                violations,
            }),
        };
        for i in 0..cluster.inner.processes.len() {
            cluster.start_process(i);
        }
        cluster.recover(false, None);
        cluster
    }

    /// The connection string of the cluster (its coordinators).
    pub fn connection_string(&self) -> String {
        let coords: Vec<String> = self
            .inner
            .processes
            .iter()
            .zip(&self.inner.classes)
            .filter(|(_, c)| **c == ProcessClass::Coordinator)
            .map(|(p, _)| p.addr.to_string())
            .collect();
        format!("sim:cluster@{}", coords.join(","))
    }

    /// The cluster's shape.
    pub fn config(&self) -> &ClusterConfig {
        &self.inner.config
    }

    /// The current recovery generation.
    pub fn generation(&self) -> u64 {
        lock(&self.inner.state).generation
    }

    /// The current `ClientDBInfo`.
    pub fn info(&self) -> ClientDbInfo {
        lock(&self.inner.state).info.clone()
    }

    /// Every commit applied so far, in version order.
    pub fn log(&self) -> Vec<LogEntry> {
        lock(&self.inner.state).model.log.clone()
    }

    /// The value of `key` at `version`, as the cluster knows it.
    pub fn value_at(&self, key: &[u8], version: i64) -> Option<Vec<u8>> {
        lock(&self.inner.state).model.get(key, version)
    }

    /// The rows of `[begin, end)` at `version`, as the cluster knows them.
    pub fn range_at(&self, begin: &[u8], end: &[u8], version: i64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let state = lock(&self.inner.state);
        state
            .model
            .keys(begin, end, version)
            .into_iter()
            .map(|k| {
                let v = state.model.get(&k, version).expect("present");
                (k, v)
            })
            .collect()
    }

    /// The largest version handed out (as a read or commit version) so far.
    pub fn max_version(&self) -> i64 {
        lock(&self.inner.state).model.max_version()
    }

    /// The counters.
    pub fn stats(&self) -> ClusterStats {
        lock(&self.inner.state).stats
    }

    /// Protocol breaches the servers saw (a frame they could not decode, a request of the
    /// wrong shape, a mutation the model does not support).
    pub fn violations(&self) -> Vec<String> {
        lock(&self.inner.violations).clone()
    }

    /// The processes.
    pub fn processes(&self) -> Vec<ProcessInfo> {
        self.inner
            .processes
            .iter()
            .zip(&self.inner.classes)
            .enumerate()
            .map(|(index, (p, class))| ProcessInfo {
                index,
                node: p.node,
                name: p.name(),
                addr: p.addr,
                class: *class,
                alive: p.is_alive(),
            })
            .collect()
    }

    /// Requests that reached a handler of process `index`.
    pub fn requests(&self, index: usize) -> u64 {
        self.inner.processes[index].requests.load(Ordering::SeqCst)
    }

    /// The process index of storage server `ss`.
    pub fn storage_process(&self, ss: usize) -> usize {
        lock(&self.inner.state).storage[ss].process
    }

    /// The storage servers holding the shard of `key`.
    pub fn team_of(&self, key: &[u8]) -> Vec<usize> {
        let state = lock(&self.inner.state);
        state
            .shards
            .iter()
            .find(|s| s.begin.as_slice() <= key && key < s.end.as_slice())
            .map(|s| s.team.clone())
            .unwrap_or_default()
    }

    /// The processes hosting the current proxies.
    pub fn proxy_processes(&self) -> Vec<usize> {
        let state = lock(&self.inner.state);
        let mut out: Vec<usize> = state
            .grv
            .iter()
            .chain(&state.commit)
            .map(|r| r.process)
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    fn trace(&self, kind: &str, args: &[u64], detail: impl FnOnce() -> String) {
        self.inner.sim.trace(TraceLevel::Events, kind, args, detail);
    }

    fn violation(&self, what: String) {
        self.trace("cluster-violation", &[], || what.clone());
        lock(&self.inner.violations).push(what);
    }

    // ---- processes ----------------------------------------------------------------------

    fn start_process(&self, index: usize) {
        let process = &self.inner.processes[index];
        process.start();
        match self.inner.classes[index] {
            ProcessClass::Coordinator => {
                let weak = Arc::downgrade(&self.inner);
                process.handle(
                    well_known::CLIENTLEADERREG_OPENDATABASE,
                    Arc::new(move |incoming| {
                        if let Some(inner) = weak.upgrade() {
                            SimCluster { inner }.on_open_database(incoming);
                        }
                    }),
                );
            }
            ProcessClass::Stateless => {}
            ProcessClass::Storage(ss) => self.register_storage(ss),
        }
    }

    /// Kills process `index` as `mode` says. If it hosts a current proxy, a recovery follows
    /// after a detection delay.
    pub fn kill_process(&self, index: usize, mode: KillMode) {
        let process = &self.inner.processes[index];
        if !process.is_alive() {
            return;
        }
        process.kill(mode);
        let hosts_proxy = {
            let state = lock(&self.inner.state);
            state
                .grv
                .iter()
                .chain(&state.commit)
                .any(|r| r.process == index)
        };
        if hosts_proxy {
            self.schedule_recovery();
        }
    }

    /// Starts process `index` again (a storage server gets new endpoint tokens; one
    /// [removed](Self::remove_storage) stays down).
    pub fn restart_process(&self, index: usize) {
        let process = &self.inner.processes[index];
        if process.is_alive() {
            return;
        }
        if let ProcessClass::Storage(ss) = self.inner.classes[index] {
            let mut state = lock(&self.inner.state);
            if state.removed.contains(&ss) {
                return;
            }
            let s = &mut state.storage[ss];
            s.restarts += 1;
            s.iface.base = endpoint(process.addr, token(3, s.restarts, ss as u64));
        }
        self.start_process(index);
        self.trace("restart", &[index as u64], || process.name());
    }

    fn schedule_recovery(&self) {
        let delay = {
            let mut state = lock(&self.inner.state);
            if state.recovery_pending {
                return;
            }
            state.recovery_pending = true;
            state
                .rng
                .duration(Duration::from_millis(100), Duration::from_secs(2))
        };
        let me = self.clone();
        let sim = self.inner.sim.clone();
        let scheduled_in = self.generation();
        self.inner.sim.spawn_on(self.inner.controller, async move {
            sim.sleep(delay).await;
            if me.generation() != scheduled_in {
                // Someone recovered in the meantime.
                return;
            }
            let (jump, gap) = {
                let mut state = lock(&me.inner.state);
                let jump = state.rng.chance(0.5);
                let gap = state.rng.chance(0.5).then(|| {
                    state
                        .rng
                        .duration(Duration::ZERO, Duration::from_millis(500))
                });
                (jump, gap)
            };
            me.recover(jump, gap);
        });
    }

    // ---- recoveries -----------------------------------------------------------------------

    /// A recovery: a new generation of proxies on live stateless processes (one is
    /// restarted if none is up), the old ones retired. With `jump` the version clock jumps
    /// by 100 000 000 (every read version in use becomes too old). With `gap` an info
    /// without proxies is published first, the new one `gap` later.
    pub fn recover(&self, jump: bool, gap: Option<Duration>) {
        let stateless: Vec<usize> = (0..self.inner.processes.len())
            .filter(|i| self.inner.classes[*i] == ProcessClass::Stateless)
            .collect();
        if !stateless
            .iter()
            .any(|i| self.inner.processes[*i].is_alive())
        {
            self.restart_process(stateless[0]);
        }
        let alive: Vec<usize> = stateless
            .iter()
            .copied()
            .filter(|i| self.inner.processes[*i].is_alive())
            .collect();
        let schema = self.inner.config.schema;
        let (generation, info, old_grv, old_commit) = {
            let mut state = lock(&self.inner.state);
            state.recovery_pending = false;
            state.generation += 1;
            state.stats.recoveries += 1;
            let generation = state.generation;
            if jump {
                state.model.jump(100_000_000);
            }
            let old_grv = std::mem::take(&mut state.grv);
            let old_commit = std::mem::take(&mut state.commit);
            let mut grv = Vec::new();
            for i in 0..self.inner.config.grv_proxies {
                let process = alive[(generation as usize + i) % alive.len()];
                grv.push(Role {
                    process,
                    token: token(1, generation, i as u64),
                    tasks: Vec::new(),
                });
            }
            let mut commit = Vec::new();
            for i in 0..self.inner.config.commit_proxies {
                let at = state.rng.index(alive.len());
                commit.push(Role {
                    process: alive[at],
                    token: token(2, generation, i as u64),
                    tasks: Vec::new(),
                });
            }
            let info = ClientDbInfo {
                id: Uid::new(0xdb, generation),
                grv_proxies: grv
                    .iter()
                    .enumerate()
                    .map(|(i, r)| GrvProxy {
                        process_id: Some(format!("grv-{generation}-{i}").into_bytes()),
                        provisional: false,
                        base: endpoint(self.inner.processes[r.process].addr, r.token),
                    })
                    .collect(),
                commit_proxies: commit
                    .iter()
                    .enumerate()
                    .map(|(i, r)| CommitProxy {
                        process_id: Some(format!("cp-{generation}-{i}").into_bytes()),
                        provisional: false,
                        base: endpoint(self.inner.processes[r.process].addr, r.token),
                        schema,
                    })
                    .collect(),
                ..empty_info(generation, schema)
            };
            state.grv = grv;
            state.commit = commit;
            (generation, info, old_grv, old_commit)
        };
        // Retire the old generation: pending requests break their promises.
        for mut role in old_grv {
            let process = self.inner.processes[role.process].clone();
            let tokens = [self.grv_endpoint(&process, role.token).token];
            role.retire(&process, &tokens);
        }
        for mut role in old_commit {
            let process = self.inner.processes[role.process].clone();
            let streams = self.commit_proxy(&process, role.token).endpoints();
            role.retire(
                &process,
                &[
                    streams.commit.token,
                    streams.get_key_servers_locations.token,
                ],
            );
        }
        self.register_proxies();
        self.trace("recovery", &[generation, u64::from(jump)], || {
            format!(
                "generation {generation}: {} GRV, {} commit proxies{}{}",
                info.grv_proxies.len(),
                info.commit_proxies.len(),
                if jump { ", version jump" } else { "" },
                gap.map_or(String::new(), |g| format!(", {g:?} without proxies"))
            )
        });
        match gap {
            Some(gap) => {
                self.publish(ClientDbInfo {
                    id: Uid::new(0xdb0, generation),
                    ..empty_info(generation, schema)
                });
                let me = self.clone();
                let sim = self.inner.sim.clone();
                self.inner.sim.spawn_on(self.inner.controller, async move {
                    sim.sleep(gap).await;
                    if me.generation() == generation {
                        me.publish(info);
                    }
                });
            }
            None => self.publish(info),
        }
    }

    fn grv_endpoint(&self, process: &Process, token: Uid) -> Endpoint {
        GrvProxy {
            process_id: None,
            provisional: false,
            base: endpoint(process.addr, token),
        }
        .endpoints()
        .get_consistent_read_version
    }

    fn commit_proxy(&self, process: &Process, token: Uid) -> CommitProxy {
        CommitProxy {
            process_id: None,
            provisional: false,
            base: endpoint(process.addr, token),
            schema: self.inner.config.schema,
        }
    }

    fn register_proxies(&self) {
        let (grv, commit) = {
            let state = lock(&self.inner.state);
            let grv: Vec<(usize, usize, Uid)> = state
                .grv
                .iter()
                .enumerate()
                .map(|(i, r)| (i, r.process, r.token))
                .collect();
            let commit: Vec<(usize, usize, Uid)> = state
                .commit
                .iter()
                .enumerate()
                .map(|(i, r)| (i, r.process, r.token))
                .collect();
            (grv, commit)
        };
        let generation = self.generation();
        for (i, p, token) in grv {
            let process = &self.inner.processes[p];
            if !process.is_alive() {
                continue;
            }
            let weak = Arc::downgrade(&self.inner);
            process.handle(
                self.grv_endpoint(process, token).token,
                Arc::new(move |incoming| {
                    with(&weak, |c| c.on_grv(generation, i, p, incoming));
                }),
            );
        }
        for (i, p, token) in commit {
            let process = &self.inner.processes[p];
            if !process.is_alive() {
                continue;
            }
            let streams = self.commit_proxy(process, token).endpoints();
            let weak = Arc::downgrade(&self.inner);
            process.handle(
                streams.commit.token,
                Arc::new(move |incoming| {
                    with(&weak, |c| c.on_commit(generation, i, p, incoming));
                }),
            );
            let weak = Arc::downgrade(&self.inner);
            process.handle(
                streams.get_key_servers_locations.token,
                Arc::new(move |incoming| {
                    with(&weak, |c| c.on_locations(generation, i, p, incoming));
                }),
            );
        }
    }

    fn publish(&self, info: ClientDbInfo) {
        let waiters = {
            let mut state = lock(&self.inner.state);
            state.info = info.clone();
            std::mem::take(&mut state.waiters)
        };
        for (_, reply) in waiters {
            if reply.is_closed() {
                reply.forget();
                continue;
            }
            let payload = OpenDatabaseCoord::encode_reply(reply.schema(), &Ok(info.clone()))
                .expect("an info encodes");
            reply.send(payload);
        }
    }

    // ---- faults ---------------------------------------------------------------------------

    /// Makes the next commit a commit proxy handles crash its process at `point`, as `mode`.
    pub fn arm_commit_crash(&self, point: CrashPoint, mode: KillMode) {
        lock(&self.inner.state).commit_crash = Some((point, mode));
    }

    /// GRV proxies answer `grv_proxy_memory_limit_exceeded` for `duration`.
    pub fn throttle_grv(&self, duration: Duration) {
        let until = self.inner.sim.now() + duration;
        lock(&self.inner.state).grv_throttle_until = until;
        self.trace("throttle-grv", &[duration.as_micros() as u64], || {
            format!("{duration:?}")
        });
    }

    /// Commit proxies answer `commit_proxy_memory_limit_exceeded` for `duration`.
    pub fn throttle_commits(&self, duration: Duration) {
        let until = self.inner.sim.now() + duration;
        lock(&self.inner.state).commit_throttle_until = until;
        self.trace("throttle-commit", &[duration.as_micros() as u64], || {
            format!("{duration:?}")
        });
    }

    /// Storage server `ss` serves versions `lag` old (reads at newer ones wait, then
    /// `future_version`).
    pub fn set_storage_lag(&self, ss: usize, lag: Duration) {
        lock(&self.inner.state).storage[ss].lag = lag;
        self.trace("storage-lag", &[ss as u64, lag.as_micros() as u64], || {
            format!("storage {ss}: {lag:?}")
        });
    }

    /// Storage server `ss` answers `process_behind` (half the time) for `duration`.
    pub fn set_storage_behind(&self, ss: usize, duration: Duration) {
        let until = self.inner.sim.now() + duration;
        lock(&self.inner.state).storage[ss].behind_until = until;
        self.trace("storage-behind", &[ss as u64], || {
            format!("storage {ss}: {duration:?}")
        });
    }

    /// Removes storage server `ss` for good, as an exclusion followed by the death of its
    /// machine does: every shard its team holds moves to a team of the other storage servers
    /// (of the configured size, or all of them if there are not enough), so the commit
    /// proxies stop listing it, and its process is killed as `mode` says and never restarted
    /// by [`heal`](Self::heal); later shard moves leave it out too. At least one storage
    /// server must remain.
    pub fn remove_storage(&self, ss: usize, mode: KillMode) {
        let process = {
            let mut state = lock(&self.inner.state);
            let n = state.storage.len();
            state.removed.insert(ss);
            assert!(
                state.removed.len() < n,
                "the last storage server cannot be removed"
            );
            for i in 0..state.shards.len() {
                if state.shards[i].team.contains(&ss) {
                    let removed = state.removed.clone();
                    let replication = self.inner.config.replication;
                    state.shards[i].team = random_team(&mut state.rng, n, replication, &removed);
                }
            }
            state.storage[ss].process
        };
        self.trace("remove-storage", &[ss as u64], || format!("storage {ss}"));
        self.kill_process(process, mode);
    }

    /// Moves a random shard to a new random team.
    pub fn move_shard(&self) {
        let (i, team) = {
            let mut state = lock(&self.inner.state);
            let n = state.storage.len();
            let shards = state.shards.len();
            let i = state.rng.index(shards);
            let removed = state.removed.clone();
            let team = random_team(&mut state.rng, n, self.inner.config.replication, &removed);
            state.shards[i].team = team.clone();
            (i, team)
        };
        self.trace("move-shard", &[i as u64], || {
            format!("shard {i} -> {team:?}")
        });
    }

    /// Splits a random shard at a random key inside it (each half keeps the team or gets a
    /// new one).
    pub fn split_shard(&self) {
        let done = {
            let mut state = lock(&self.inner.state);
            let shards = state.shards.len();
            let i = state.rng.index(shards);
            let shard = state.shards[i].clone();
            let c = (b'a' + state.rng.below(26) as u8) as char;
            let suffix = state.rng.below(100);
            let key = format!("{c}/{suffix:02}").into_bytes();
            if shard.begin < key && key < shard.end {
                let n = state.storage.len();
                let team = if state.rng.chance(0.5) {
                    shard.team.clone()
                } else {
                    let removed = state.removed.clone();
                    random_team(&mut state.rng, n, self.inner.config.replication, &removed)
                };
                state.shards[i].end = key.clone();
                state.shards.insert(
                    i + 1,
                    Shard {
                        begin: key.clone(),
                        end: shard.end,
                        team,
                    },
                );
                Some(key)
            } else {
                None
            }
        };
        if let Some(key) = done {
            self.trace("split-shard", &[], || {
                format!("at {:?}", String::from_utf8_lossy(&key))
            });
        }
    }

    /// Ends every fault: restarts the dead processes (recovering if a proxy host was among
    /// them), and clears throttles, lags and the armed crash. The network is healed by
    /// [`Sim::heal_network`].
    pub fn heal(&self) {
        for i in 0..self.inner.processes.len() {
            self.restart_process(i);
        }
        let needs_recovery = {
            let mut state = lock(&self.inner.state);
            state.grv_throttle_until = Instant::ZERO;
            state.commit_throttle_until = Instant::ZERO;
            state.commit_crash = None;
            for s in &mut state.storage {
                s.lag = Duration::ZERO;
                s.behind_until = Instant::ZERO;
            }
            // A proxy host that died lost its roles (they are only made by a recovery).
            state.recovery_pending
        };
        if needs_recovery {
            self.recover(false, None);
        }
        self.trace("heal-cluster", &[], String::new);
    }

    // ---- handlers -------------------------------------------------------------------------

    fn spawn_role_task(
        &self,
        process: usize,
        role: Option<(bool, u64, usize)>,
        future: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        let node = self.inner.processes[process].node;
        let task = self.inner.sim.spawn_on(node, future);
        if let Some((commit, generation, i)) = role {
            let mut state = lock(&self.inner.state);
            if state.generation == generation {
                let roles = if commit {
                    &mut state.commit
                } else {
                    &mut state.grv
                };
                if let Some(r) = roles.get_mut(i) {
                    r.track(task);
                    return;
                }
            }
            drop(state);
            task.abort();
        }
    }

    fn on_open_database(&self, incoming: Incoming) {
        let (request, reply) = match incoming.decode::<OpenDatabaseCoord>() {
            Ok(r) => r,
            Err(e) => return self.violation(e),
        };
        let mut state = lock(&self.inner.state);
        state.stats.open_database_requests += 1;
        state.waiters.retain(|(_, r)| !r.is_closed());
        if request.known_client_info_id != state.info.id {
            let payload = OpenDatabaseCoord::encode_reply(reply.schema(), &Ok(state.info.clone()))
                .expect("an info encodes");
            drop(state);
            reply.send(payload);
        } else {
            state.waiters.push((request.known_client_info_id, reply));
        }
    }

    fn latency(&self, lo_us: u64, hi_us: u64) -> Duration {
        let mut state = lock(&self.inner.state);
        Duration::from_micros(state.rng.range(lo_us, hi_us))
    }

    fn on_grv(&self, generation: u64, i: usize, process: usize, incoming: Incoming) {
        let (request, reply) = match incoming.decode::<GetReadVersion>() {
            Ok(r) => r,
            Err(e) => return self.violation(e),
        };
        {
            let mut state = lock(&self.inner.state);
            state.stats.grv_requests += 1;
            state.stats.grv_transactions += u64::from(request.transaction_count);
        }
        let delay = self.latency(50, 3000);
        let me = self.clone();
        let sim = self.inner.sim.clone();
        self.spawn_role_task(process, Some((false, generation, i)), async move {
            sim.sleep(delay).await;
            let now = sim.now();
            let answer = {
                let mut state = lock(&me.inner.state);
                if state.grv_throttle_until > now {
                    state.stats.throttled += 1;
                    Err(codes::GRV_PROXY_MEMORY_LIMIT_EXCEEDED)
                } else {
                    state.stats.read_versions += 1;
                    Ok(state.model.read_version(now))
                }
            };
            match answer {
                Err(code) => reply.error(code),
                Ok(version) => {
                    let rv = ReadVersion {
                        process_busy_time: 0,
                        version,
                        locked: false,
                        metadata_version: None,
                        tag_throttle_info: Default::default(),
                        mid_shard_size: 0,
                        rk_default_throttled: false,
                        rk_batch_throttled: false,
                        ss_version_vector_delta: Default::default(),
                        proxy_id: Uid::new(generation, i as u64),
                        proxy_tag_throttled_duration: 0.0,
                    };
                    let payload = GetReadVersion::encode_reply(reply.schema(), &Ok(rv))
                        .expect("a read version encodes");
                    reply.send(payload);
                }
            }
        });
    }

    fn on_commit(&self, generation: u64, i: usize, process: usize, incoming: Incoming) {
        let (request, reply) = match incoming.decode::<Commit>() {
            Ok(r) => r,
            Err(e) => return self.violation(e),
        };
        let delay = self.latency(100, 5000);
        let me = self.clone();
        let sim = self.inner.sim.clone();
        self.spawn_role_task(process, Some((true, generation, i)), async move {
            sim.sleep(delay).await;
            let crash = |me: &SimCluster, point: CrashPoint| {
                let mode = {
                    let mut state = lock(&me.inner.state);
                    match state.commit_crash {
                        Some((p, mode)) if p == point => {
                            state.commit_crash = None;
                            state.stats.commit_crashes += 1;
                            Some(mode)
                        }
                        _ => None,
                    }
                };
                if let Some(mode) = mode {
                    me.trace("commit-crash", &[process as u64], || {
                        format!("{} crashes {point:?}", me.inner.processes[process].name())
                    });
                    me.kill_process(process, mode);
                    true
                } else {
                    false
                }
            };
            if crash(&me, CrashPoint::BeforeApply) {
                reply.forget();
                return;
            }
            let now = sim.now();
            let answer = {
                let mut state = lock(&me.inner.state);
                if state.generation != generation {
                    // Retired: the promise breaks.
                    None
                } else if state.commit_throttle_until > now {
                    state.stats.throttled += 1;
                    Some(Err(codes::COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED))
                } else {
                    match state.model.resolve(now, &request) {
                        Resolution::TooOld => {
                            state.stats.too_old += 1;
                            Some(Err(codes::TRANSACTION_TOO_OLD))
                        }
                        Resolution::Conflict(indices) => {
                            state.stats.conflicts += 1;
                            Some(Ok(CommitId {
                                version: INVALID_VERSION,
                                txn_batch_id: 0,
                                metadata_version: None,
                                conflicting_kr_indices: request
                                    .report_conflicting_keys
                                    .then_some(indices),
                            }))
                        }
                        Resolution::Commit => match state.model.apply(now, &request, generation) {
                            Ok(version) => {
                                state.stats.commits += 1;
                                Some(Ok(CommitId {
                                    version,
                                    txn_batch_id: 0,
                                    metadata_version: None,
                                    conflicting_kr_indices: None,
                                }))
                            }
                            Err(e) => {
                                drop(state);
                                me.violation(format!("unsupported mutation {:?}", e.0));
                                Some(Err(4100)) // `internal_error`: the model cannot apply it.
                            }
                        },
                    }
                }
            };
            let Some(answer) = answer else {
                drop(reply);
                return;
            };
            if let Ok(id) = &answer {
                if id.version != INVALID_VERSION {
                    let v = id.version;
                    me.trace("applied", &[v as u64], || {
                        format!(
                            "version {v}: {} mutations via {}",
                            request.mutations.len(),
                            me.inner.processes[process].name()
                        )
                    });
                }
            }
            if crash(&me, CrashPoint::AfterApply) {
                reply.forget();
                return;
            }
            let delay = me.latency(10, 500);
            sim.sleep(delay).await;
            match answer {
                Ok(id) => {
                    let payload = Commit::encode_reply(reply.schema(), &Ok(id))
                        .expect("a commit reply encodes");
                    reply.send(payload);
                }
                Err(code) => reply.error(code),
            }
        });
    }

    fn on_locations(&self, generation: u64, i: usize, process: usize, incoming: Incoming) {
        let (request, reply) = match incoming.decode::<GetKeyServerLocations>() {
            Ok(r) => r,
            Err(e) => return self.violation(e),
        };
        lock(&self.inner.state).stats.location_requests += 1;
        let delay = self.latency(50, 2000);
        let me = self.clone();
        let sim = self.inner.sim.clone();
        self.spawn_role_task(process, Some((true, generation, i)), async move {
            sim.sleep(delay).await;
            let results = {
                let state = lock(&me.inner.state);
                let shards = &state.shards;
                let picked: Vec<&Shard> = match &request.end {
                    None => {
                        let k = request.begin.as_slice();
                        let shard = if request.reverse {
                            shards
                                .iter()
                                .find(|s| s.begin.as_slice() < k && k <= s.end.as_slice())
                                .or(shards.first())
                        } else {
                            shards
                                .iter()
                                .find(|s| s.begin.as_slice() <= k && k < s.end.as_slice())
                                .or(shards.last())
                        };
                        shard.into_iter().collect()
                    }
                    Some(end) => {
                        let mut hits: Vec<&Shard> = shards
                            .iter()
                            .filter(|s| {
                                s.begin.as_slice() < end.as_slice()
                                    && request.begin.as_slice() < s.end.as_slice()
                            })
                            .collect();
                        if request.reverse {
                            hits.reverse();
                        }
                        hits.truncate(request.limit.max(1) as usize);
                        hits
                    }
                };
                picked
                    .into_iter()
                    .map(|s| {
                        (
                            KeyRange {
                                begin: s.begin.clone(),
                                end: s.end.clone(),
                            },
                            s.team
                                .iter()
                                .map(|ss| state.storage[*ss].iface.clone())
                                .collect(),
                        )
                    })
                    .collect()
            };
            let locations = KeyServerLocations {
                results,
                tss_mapping: Vec::new(),
                tag_mapping: Vec::new(),
            };
            let payload = GetKeyServerLocations::encode_reply(reply.schema(), &Ok(locations))
                .expect("locations encode");
            reply.send(payload);
        });
    }

    fn register_storage(&self, ss: usize) {
        let (process, streams) = {
            let state = lock(&self.inner.state);
            let s = &state.storage[ss];
            (s.process, s.iface.endpoints())
        };
        let p = &self.inner.processes[process];
        let weak = Arc::downgrade(&self.inner);
        p.handle(
            streams.get_value.token,
            Arc::new(move |incoming| {
                with(&weak, |c| c.on_read(ss, process, Read::Value, incoming))
            }),
        );
        let weak = Arc::downgrade(&self.inner);
        p.handle(
            streams.get_key.token,
            Arc::new(move |incoming| with(&weak, |c| c.on_read(ss, process, Read::Key, incoming))),
        );
        let weak = Arc::downgrade(&self.inner);
        p.handle(
            streams.get_key_values.token,
            Arc::new(move |incoming| {
                with(&weak, |c| c.on_read(ss, process, Read::Range, incoming));
            }),
        );
    }

    fn on_read(&self, ss: usize, process: usize, kind: Read, incoming: Incoming) {
        let request = match kind {
            Read::Value => incoming
                .decode::<GetValue>()
                .map(|(r, reply)| (ReadRequest::Value(r), reply)),
            Read::Key => incoming
                .decode::<GetKey>()
                .map(|(r, reply)| (ReadRequest::Key(r), reply)),
            Read::Range => incoming
                .decode::<GetKeyValues>()
                .map(|(r, reply)| (ReadRequest::Range(r), reply)),
        };
        let (request, reply) = match request {
            Ok(r) => r,
            Err(e) => return self.violation(e),
        };
        let delay = self.latency(30, 2000);
        let me = self.clone();
        let sim = self.inner.sim.clone();
        self.spawn_role_task(process, None, async move {
            sim.sleep(delay).await;
            let start = sim.now();
            loop {
                let now = sim.now();
                let step = me.serve_read(ss, &request, reply.schema(), now, now - start);
                match step {
                    ReadStep::Wait => sim.sleep(Duration::from_millis(20)).await,
                    ReadStep::Error(code) => return reply.error(code),
                    ReadStep::Reply(payload) => return reply.send(payload),
                }
            }
        });
    }

    fn serve_read(
        &self,
        ss: usize,
        request: &ReadRequest,
        schema: SchemaVersion,
        now: Instant,
        waited: Duration,
    ) -> ReadStep {
        let mut state = lock(&self.inner.state);
        let version = request.version();
        // The shard the request is for, and whether this server holds it.
        let shard = match request {
            ReadRequest::Value(r) => state
                .shards
                .iter()
                .find(|s| s.begin <= r.key && r.key < s.end)
                .cloned(),
            ReadRequest::Key(r) => shard_of_selector(&state.shards, &r.sel),
            ReadRequest::Range(r) => shard_of_selector(&state.shards, &r.begin),
        };
        let Some(shard) = shard.filter(|s| s.team.contains(&ss)) else {
            state.stats.wrong_shard += 1;
            return ReadStep::Error(codes::WRONG_SHARD_SERVER);
        };
        if state.storage[ss].behind_until > now && state.rng.chance(0.5) {
            state.stats.process_behind += 1;
            return ReadStep::Error(codes::PROCESS_BEHIND);
        }
        if version < state.model.oldest(now) {
            state.stats.too_old += 1;
            return ReadStep::Error(codes::TRANSACTION_TOO_OLD);
        }
        let lag = state.storage[ss].lag;
        if version > state.model.storage_version(now, lag) {
            if waited >= FUTURE_VERSION_DELAY {
                state.stats.future_version += 1;
                return ReadStep::Error(codes::FUTURE_VERSION);
            }
            return ReadStep::Wait;
        }
        state.stats.reads += 1;
        let model = &state.model;
        let payload = match request {
            ReadRequest::Value(r) => {
                let value = model.get(&r.key, version);
                let answer = GetValueReply {
                    value: value.as_deref(),
                    ..GetValueReply::default()
                };
                GetValue::encode_reply(schema, &Ok(answer))
            }
            ReadRequest::Key(r) => {
                let keys = model.keys(&shard.begin, &shard.end, version);
                let target = selector_target(&keys, &r.sel);
                let (key, or_equal, offset) = if target < 0 {
                    (shard.begin.clone(), false, 1 + target)
                } else if (target as usize) < keys.len() {
                    (keys[target as usize].clone(), true, 0)
                } else {
                    (shard.end.clone(), false, target - keys.len() as i64 + 1)
                };
                let answer = GetKeyReply {
                    sel: KeySelectorRef {
                        key: &key,
                        or_equal,
                        offset: offset as i32,
                    },
                    ..GetKeyReply::default()
                };
                GetKey::encode_reply(schema, &Ok(answer))
            }
            ReadRequest::Range(r) => {
                let end_ok = selector_in_range(&r.end, &shard.begin, &shard.end)
                    || (!r.end.or_equal && r.end.offset == 1 && r.end.key == shard.end);
                if !end_ok {
                    state.stats.wrong_shard += 1;
                    return ReadStep::Error(codes::WRONG_SHARD_SERVER);
                }
                let keys = model.keys(&shard.begin, &shard.end, version);
                let resolve = |sel: &KeySelector| -> Option<Vec<u8>> {
                    let t = selector_target(&keys, sel);
                    if t >= 0 && (t as usize) < keys.len() {
                        Some(keys[t as usize].clone())
                    } else if t == keys.len() as i64 || (t > 0 && shard.end == KEYSPACE_END) {
                        Some(shard.end.clone())
                    } else if t < 0 && shard.begin.is_empty() {
                        Some(Vec::new())
                    } else {
                        None
                    }
                };
                let (Some(begin), Some(end)) = (resolve(&r.begin), resolve(&r.end)) else {
                    state.stats.wrong_shard += 1;
                    return ReadStep::Error(codes::WRONG_SHARD_SERVER);
                };
                let mut rows: Vec<(Vec<u8>, Vec<u8>)> = if begin < end {
                    keys.iter()
                        .filter(|k| begin <= **k && **k < end)
                        .map(|k| (k.clone(), model.get(k, version).expect("present")))
                        .collect()
                } else {
                    Vec::new()
                };
                if r.limit < 0 {
                    rows.reverse();
                }
                let mut limit = r.limit.unsigned_abs() as usize;
                let mut bytes = i64::from(r.limit_bytes);
                let mut out = Vec::new();
                for row in rows {
                    if limit == 0 || bytes <= 0 {
                        break;
                    }
                    // `sizeof(KeyValueRef)` a row, as `readRange` counts.
                    bytes -= (32 + row.0.len() + row.1.len()) as i64;
                    limit -= 1;
                    out.push(row);
                }
                let more = limit == 0 || bytes <= 0;
                let vector =
                    KeyValueVector::from_pairs(out.iter().map(|(k, v)| KeyValueRef::new(k, v)))
                        .expect("rows encode");
                let answer = GetKeyValuesReply {
                    data: vector.borrowed(),
                    more,
                    version,
                    ..GetKeyValuesReply::default()
                };
                GetKeyValues::encode_reply(schema, &Ok(answer))
            }
        };
        ReadStep::Reply(payload.expect("a read reply encodes"))
    }
}

#[derive(Clone, Copy, Debug)]
enum Read {
    Value,
    Key,
    Range,
}

enum ReadRequest {
    Value(GetValue),
    Key(GetKey),
    Range(GetKeyValues),
}

impl ReadRequest {
    fn version(&self) -> i64 {
        match self {
            ReadRequest::Value(r) => r.version,
            ReadRequest::Key(r) => r.version,
            ReadRequest::Range(r) => r.version,
        }
    }
}

enum ReadStep {
    Wait,
    Error(u16),
    Reply(Vec<u8>),
}

/// `getShardKeyRange`: the shard holding the selector's key (the one before it for a
/// backward selector).
fn shard_of_selector(shards: &[Shard], sel: &KeySelector) -> Option<Shard> {
    shards
        .iter()
        .find(|s| selector_in_range(sel, &s.begin, &s.end))
        .cloned()
}

fn with(weak: &Weak<Inner>, f: impl FnOnce(&SimCluster)) {
    if let Some(inner) = weak.upgrade() {
        f(&SimCluster { inner });
    }
}

/// `size` (at least 1) random storage servers out of `servers`, none of `removed`.
fn random_team(
    rng: &mut SimRng,
    servers: usize,
    size: usize,
    removed: &BTreeSet<usize>,
) -> Vec<usize> {
    let mut all: Vec<usize> = (0..servers).collect();
    for i in (1..all.len()).rev() {
        let j = rng.index(i + 1);
        all.swap(i, j);
    }
    all.retain(|s| !removed.contains(s));
    let available = all.len();
    all.truncate(size.clamp(1, available));
    all.sort_unstable();
    all
}

fn empty_info(generation: u64, schema: SchemaVersion) -> ClientDbInfo {
    ClientDbInfo {
        id: Uid::new(0xdb, generation),
        grv_proxies: Vec::new(),
        commit_proxies: Vec::new(),
        forward: None,
        history: Vec::new(),
        cluster_id: Uid::new(0xc1, 0xd),
        cluster_type: 0,
        tenant_mode: match schema {
            SchemaVersion::V7_4 => Some(TenantMode::DISABLED),
            SchemaVersion::V8_0 => None,
        },
    }
}
