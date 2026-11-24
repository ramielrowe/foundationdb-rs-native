//! One seed, end to end: a randomized cluster and network, the real client, the workloads
//! under a chaos schedule, the invariants, and the shutdown.
//!
//! [`run_seed`] (or [`run`] with a [`SeedConfig`]) does, all on the seed's virtual time:
//!
//! 1. **Setup.** The cluster's shape ([`ClusterConfig::randomized`]), the faulty network's
//!    settings ([`NetConfig::randomized`]), the client's seeds and the stream's write promises
//!    (from `seed % 4`: both kept, neither, or one of the two) are drawn from the seed. The
//!    bank's accounts are written on a clean network.
//! 2. **Chaos.** For 5 to 25 s the network is faulty and a chaos task strikes, at random
//!    times, faults drawn with per-seed weights: process kills (crash or power off) and
//!    restarts, recoveries (with version jumps and gaps without proxies), clogs, partitions
//!    of the client from servers, connection resets mid-frame and half-open connections,
//!    storage lag (`future_version`) and `process_behind`, shard moves and splits
//!    (`wrong_shard_server`), GRV and commit throttling, and commit proxies crashing just
//!    before or after applying a commit (`commit_unknown_result`). The workloads run
//!    meanwhile. Then everything heals.
//! 3. **Liveness.** The workloads must finish within 5 minutes of virtual time after the
//!    chaos.
//! 4. **Invariants**: at-most-once, the bank's serializability, the reads (checked as they
//!    run), the backoff bounds (checked as they run); no task panicked; the servers saw no
//!    malformed frame; the client kept the stream contract.
//! 5. **Convergence** (a third of the seeds, with a replicated cluster): after a quiet
//!    period the failure monitor marks no live server failed; a storage replica is powered
//!    off, and reads of its shard keep succeeding, fast once the failure monitor has caught
//!    up; it is restarted, and the client's load balancing sends it requests again.
//! 6. **Shutdown**: `Database::shutdown`, then no task of the client's node may be left.

use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_client::cluster::ConnectionRecord;
use fdb_native_client::{Database, DatabaseOptions};
use fdb_native_transport::runtime::{Runtime, TaskHandle};
use fdb_native_wire::NetworkAddress;

use crate::cluster::{ClusterConfig, ClusterStats, CrashPoint, ProcessClass, SimCluster};
use crate::executor::lock;
use crate::workload::{self, COUNTER, Records, WorkloadStats};
use crate::{KillMode, NetConfig, NetStats, NodeId, Sim, SimRng, SimRuntime, SimTask, TraceLevel};

/// How long the workloads have to finish once the chaos is over.
pub const LIVENESS: Duration = Duration::from_secs(300);

/// What to run for one seed. [`SeedConfig::new`] draws everything from the seed.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct SeedConfig {
    /// The seed.
    pub seed: u64,
    /// How much of the trace to print.
    pub level: TraceLevel,
    /// The stream's write promises, `(WRITE_ERRORS_TAKE_NOTHING, PENDING_TAKES_NOTHING)`;
    /// `None` takes them from `seed % 4`.
    pub trust: Option<(bool, bool)>,
    /// How long the chaos lasts; `None` draws 5 to 25 s; zero runs without faults.
    pub chaos: Option<Duration>,
    /// Whether to run the convergence phase; `None` draws it (a third of the seeds).
    pub convergence: Option<bool>,
    /// The network's behaviour during the chaos; `None` draws it
    /// ([`NetConfig::randomized`]).
    pub net: Option<NetConfig>,
    /// Whether the client runs the location cache's peer evictor
    /// (`ReadConfig::location_cache_peer_evictor`); `None` takes it from `seed / 4 % 2`
    /// (every trust setting gets both), as C++'s simulation flips
    /// `LOCATION_CACHE_PEER_EVICTOR_ENABLED`.
    pub peer_evictor: Option<bool>,
}

impl SeedConfig {
    /// Everything from the seed, printing the trace as `FDB_SIM_TRACE` says.
    pub fn new(seed: u64) -> SeedConfig {
        SeedConfig {
            seed,
            level: TraceLevel::from_env(),
            trust: None,
            chaos: None,
            convergence: None,
            net: None,
            peer_evictor: None,
        }
    }

    /// The same, with or without the location cache's peer evictor.
    pub fn with_peer_evictor(mut self, on: bool) -> Self {
        self.peer_evictor = Some(on);
        self
    }

    /// The same, with the network behaving as `net` during the chaos.
    pub fn with_net(mut self, net: NetConfig) -> Self {
        self.net = Some(net);
        self
    }

    /// The same, with the stream's write promises set.
    pub fn with_trust(mut self, errors_take_nothing: bool, pending_takes_nothing: bool) -> Self {
        self.trust = Some((errors_take_nothing, pending_takes_nothing));
        self
    }

    /// The same, with the chaos lasting `chaos` (zero: none).
    pub fn with_chaos(mut self, chaos: Duration) -> Self {
        self.chaos = Some(chaos);
        self
    }

    /// The same, with or without the convergence phase.
    pub fn with_convergence(mut self, on: bool) -> Self {
        self.convergence = Some(on);
        self
    }

    /// The same, printing the trace at `level`.
    pub fn with_level(mut self, level: TraceLevel) -> Self {
        self.level = level;
        self
    }
}

/// What a seed did.
#[derive(Clone, Debug)]
pub struct SeedReport {
    /// The seed.
    pub seed: u64,
    /// The stream's write promises it ran with.
    pub trust: (bool, bool),
    /// The cluster's shape.
    pub cluster_config: ClusterConfig,
    /// The hash of the whole trace.
    pub trace_hash: u64,
    /// How many events were traced.
    pub trace_events: u64,
    /// Virtual time at the end.
    pub virtual_time: Duration,
    /// Broken invariants (empty: the seed passed).
    pub violations: Vec<String>,
    /// The network's fault counters.
    pub net: NetStats,
    /// The cluster's counters.
    pub cluster: ClusterStats,
    /// The workloads' counters.
    pub workload: WorkloadStats,
    /// How many times each chaos fault struck.
    pub faults: BTreeMap<&'static str, u64>,
    /// Whether the convergence phase ran.
    pub convergence: bool,
    /// Whether the client ran the location cache's peer evictor.
    pub peer_evictor: bool,
}

impl SeedReport {
    /// Whether every invariant held.
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }
}

impl fmt::Display for SeedReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "seed {} ({}): trace {:016x} ({} events), {:?} virtual, trust {:?}, {:?}",
            self.seed,
            if self.passed() { "passed" } else { "FAILED" },
            self.trace_hash,
            self.trace_events,
            self.virtual_time,
            self.trust,
            self.cluster_config
        )?;
        writeln!(
            f,
            "  faults: {:?}, convergence: {}, peer evictor: {}",
            self.faults, self.convergence, self.peer_evictor
        )?;
        writeln!(f, "  network: {:?}", self.net)?;
        writeln!(f, "  cluster: {:?}", self.cluster)?;
        writeln!(f, "  workload: {}", self.workload)?;
        for v in &self.violations {
            writeln!(f, "  VIOLATION: {v}")?;
        }
        if !self.passed() {
            writeln!(
                f,
                "  replay: FDB_SIM_SEED={} FDB_SIM_TRACE=1 cargo nextest run -p fdb-native-sim \
                 --no-capture -E 'test(replay_seed)'",
                self.seed
            )?;
        }
        Ok(())
    }
}

/// Runs `seed` with everything drawn from it.
pub fn run_seed(seed: u64) -> SeedReport {
    run(&SeedConfig::new(seed))
}

/// Runs one seed as `config` says.
pub fn run(config: &SeedConfig) -> SeedReport {
    let trust = config.trust.unwrap_or(match config.seed % 4 {
        0 => (true, true),
        1 => (false, false),
        2 => (true, false),
        _ => (false, true),
    });
    match trust {
        (true, true) => run_with::<true, true>(config),
        (false, false) => run_with::<false, false>(config),
        (true, false) => run_with::<true, false>(config),
        (false, true) => run_with::<false, true>(config),
    }
}

fn client_ip() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(10, 0, 9, 1))
}

fn run_with<const E: bool, const P: bool>(config: &SeedConfig) -> SeedReport {
    let sim = Sim::new(config.seed, config.level);
    let mut rng = sim.rng();
    let cluster_config = ClusterConfig::randomized(&mut rng);
    let cluster = SimCluster::start(&sim, cluster_config.clone(), rng.fork());
    let client = sim.add_node("client", client_ip());
    let chaos_node = sim.add_node("chaos", IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
    let rt = SimRuntime::<E, P>::new(&sim, client);
    let faulty = NetConfig::randomized(&mut rng);
    let faulty = config.net.clone().unwrap_or(faulty);
    let chaos_len = config
        .chaos
        .unwrap_or_else(|| rng.duration(Duration::from_secs(5), Duration::from_secs(25)));
    let convergence =
        config.convergence.unwrap_or_else(|| rng.chance(0.33)) && cluster_config.replication >= 2;
    let accounts = rng.range(3, 8);
    let peer_evictor = config.peer_evictor.unwrap_or(config.seed / 4 % 2 == 0);
    let records = Records::new(&sim);
    let faults = Arc::new(Mutex::new(BTreeMap::new()));
    let mut violations = Vec::new();

    let mut options = DatabaseOptions::default();
    options.peers.seed = Some(rng.next_u64());
    options.monitor.seed = Some(rng.next_u64());
    options.read.seed = Some(rng.next_u64());
    options.read.location_cache_peer_evictor = peer_evictor;
    options.commit.seed = Some(rng.next_u64());
    let record = ConnectionRecord::memory(
        cluster
            .connection_string()
            .parse()
            .expect("a valid connection string"),
    );
    let db = Database::from_record(rt.clone(), record, options).expect("the database opens");

    // Every report also lists what was recorded along the way: panics, breaches of the stream
    // contract, protocol breaches the servers saw.
    let report = |mut violations: Vec<String>, sim: &Sim, cluster: &SimCluster, convergence| {
        violations.extend(sim.panics().into_iter().map(|p| format!("panic: {p}")));
        violations.extend(
            sim.net_violations()
                .into_iter()
                .map(|v| format!("stream contract: {v}")),
        );
        violations.extend(
            cluster
                .violations()
                .into_iter()
                .map(|v| format!("server: {v}")),
        );
        SeedReport {
            seed: config.seed,
            trust: (E, P),
            cluster_config: cluster_config.clone(),
            trace_hash: sim.trace_hash(),
            trace_events: sim.trace_events(),
            virtual_time: sim.elapsed(),
            violations,
            net: sim.net_stats(),
            cluster: cluster.stats(),
            workload: records.stats(),
            faults: lock(&faults).clone(),
            convergence,
            peer_evictor,
        }
    };

    // 1. Setup, on a clean network.
    match sim.block_on_within(
        Duration::from_secs(120),
        workload::bank_setup(&db, accounts),
    ) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => violations.push(format!("setup failed: {e:?}")),
        Err(e) => violations.push(format!("liveness: setup did not finish: {e}")),
    }
    if !violations.is_empty() {
        return report(violations, &sim, &cluster, false);
    }

    // 2. Chaos and the workloads.
    if !chaos_len.is_zero() {
        sim.set_net_config(faulty);
    }
    let chaos_task = sim.spawn_on(
        chaos_node,
        chaos(
            sim.clone(),
            cluster.clone(),
            client,
            chaos_node,
            chaos_len,
            rng.fork(),
            faults.clone(),
        ),
    );
    let mut tasks: Vec<SimTask<()>> = Vec::new();
    for w in 0..rng.range(1, 4) {
        let ops = rng.range(5, 15);
        tasks.push(rt.spawn(workload::at_most_once(
            db.clone(),
            records.clone(),
            w,
            ops,
            rng.fork(),
        )));
    }
    for w in 0..rng.range(1, 3) {
        let ops = rng.range(4, 12);
        tasks.push(rt.spawn(workload::bank(
            db.clone(),
            records.clone(),
            w,
            ops,
            accounts,
            rng.fork(),
        )));
    }
    for _ in 0..rng.range(1, 2) {
        let ops = rng.range(5, 15);
        tasks.push(rt.spawn(workload::reads(
            db.clone(),
            records.clone(),
            cluster.clone(),
            ops,
            accounts,
            rng.fork(),
        )));
    }
    let all = async {
        for task in &mut tasks {
            if let Err(e) = task.await {
                return Err(format!("a workload task ended with {e}"));
            }
        }
        chaos_task
            .await
            .map_err(|e| format!("the chaos task ended with {e}"))
    };
    match sim.block_on_within(chaos_len + LIVENESS, all) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => violations.push(e),
        Err(e) => {
            let live: Vec<String> = sim
                .live_tasks(client)
                .iter()
                .map(ToString::to_string)
                .collect();
            violations.push(format!(
                "liveness: the workloads did not finish within {:?} of the chaos: {e} \
                 ({} client tasks alive: {}); {}",
                LIVENESS,
                live.len(),
                live.join("; "),
                client_state(&db, &cluster)
            ));
        }
    }
    violations.extend(records.violations());
    if !violations.is_empty() {
        return report(violations, &sim, &cluster, false);
    }

    // 3. Liveness was checked above. 4. The invariants that need the whole log.
    let log = cluster.log();
    records.check_at_most_once(&log, cluster.value_at(COUNTER, cluster.max_version()));
    records.check_bank(&log, &cluster, accounts);

    // 5. Convergence.
    if convergence {
        let phase = convergence_phase(
            sim.clone(),
            cluster.clone(),
            db.clone(),
            rng.fork(),
            peer_evictor,
        );
        match sim.block_on_within(Duration::from_secs(300), phase) {
            Ok(found) => violations.extend(found),
            Err(e) => violations.push(format!(
                "liveness: the convergence phase did not finish: {e}"
            )),
        }
    }

    // 6. Shutdown: nothing of the client may be left, first with the database still held
    // (`shutdown` must end every task by itself), then once it is dropped.
    let shutdown = async {
        db.shutdown().await;
    };
    if let Err(e) = sim.block_on_within(Duration::from_secs(60), shutdown) {
        violations.push(format!("liveness: Database::shutdown did not finish: {e}"));
    }
    drop(tasks);
    let mut held = Some(db);
    for when in ["after Database::shutdown", "after the database was dropped"] {
        if let Err(e) = sim.run_for(Duration::from_secs(5)) {
            violations.push(format!("{when}: {e}"));
        }
        let leaked = sim.live_tasks(client);
        if !leaked.is_empty() {
            violations.push(format!(
                "leak: {} client tasks alive 5 s {when}: {}",
                leaked.len(),
                leaked
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
            break;
        }
        held = None;
    }
    drop(held);

    violations.extend(records.violations());
    report(violations, &sim, &cluster, convergence)
}

/// What the client knows, for a liveness report: its `ClientDBInfo` against the cluster's,
/// and the servers its failure monitor marks failed.
fn client_state<R: Runtime>(db: &Database<R>, cluster: &SimCluster) -> String {
    let known = db.cluster_monitor().current().map(|i| i.id());
    let failed: Vec<String> = cluster
        .processes()
        .iter()
        .filter(|p| {
            let address = NetworkAddress::new(p.addr.ip().into(), p.addr.port(), true, false);
            db.peers().failure_monitor().is_failed(&address)
        })
        .map(|p| format!("{}{}", p.name, if p.alive { "" } else { " (down)" }))
        .collect();
    let peers: Vec<String> = cluster
        .proxy_processes()
        .iter()
        .map(|p| {
            let p = &cluster.processes()[*p];
            let address = NetworkAddress::new(p.addr.ip().into(), p.addr.port(), true, false);
            let status = db.peers().peer(&address).status();
            format!(
                "{}: {:?}, {} references",
                p.name, status.phase, status.references
            )
        })
        .collect();
    format!(
        "client info {known:?}, cluster info {:?}, proxies on {:?} ({peers:?}), failed in the \
         client: {failed:?}",
        cluster.info().id,
        cluster
            .proxy_processes()
            .iter()
            .map(|p| cluster.processes()[*p].name.clone())
            .collect::<Vec<_>>()
    )
}

/// The faults the chaos task strikes with.
const FAULTS: [&str; 13] = [
    "kill",
    "recover",
    "clog",
    "partition",
    "reset",
    "half-open",
    "lag",
    "behind",
    "move-shard",
    "split-shard",
    "throttle-grv",
    "throttle-commit",
    "commit-crash",
];

async fn chaos(
    sim: Sim,
    cluster: SimCluster,
    client: NodeId,
    chaos_node: NodeId,
    length: Duration,
    mut rng: SimRng,
    counts: Arc<Mutex<BTreeMap<&'static str, u64>>>,
) {
    let end = sim.now() + length;
    let mut weights: Vec<u32> = FAULTS
        .iter()
        .map(|_| {
            if rng.chance(0.7) {
                rng.range(1, 10) as u32
            } else {
                0
            }
        })
        .collect();
    if weights.iter().all(|w| *w == 0) {
        weights[0] = 1;
    }
    let mean = rng.duration(Duration::from_millis(150), Duration::from_millis(1500));
    let mut healers: Vec<SimTask<()>> = Vec::new();
    let later = |healers: &mut Vec<SimTask<()>>, after: Duration, f: Box<dyn FnOnce() + Send>| {
        let s = sim.clone();
        healers.push(sim.spawn_on(chaos_node, async move {
            s.sleep(after).await;
            f();
        }));
    };
    let processes = cluster.processes();
    let storage = processes
        .iter()
        .filter(|p| matches!(p.class, ProcessClass::Storage(_)))
        .count() as u64;
    loop {
        let wait = rng.exponential(mean);
        if sim.now() + wait >= end || length.is_zero() {
            sim.sleep_until(end).await;
            break;
        }
        sim.sleep(wait).await;
        let fault = FAULTS[rng.weighted(&weights)];
        *lock(&counts).entry(fault).or_insert(0) += 1;
        match fault {
            "kill" => {
                let p = rng.pick(&processes).index;
                let mode = if rng.chance(0.5) {
                    KillMode::Reboot
                } else {
                    KillMode::PowerOff
                };
                cluster.kill_process(p, mode);
                let c = cluster.clone();
                let down = rng.duration(Duration::from_millis(100), Duration::from_secs(5));
                later(&mut healers, down, Box::new(move || c.restart_process(p)));
            }
            "recover" => {
                let jump = rng.chance(0.3);
                let gap = rng
                    .chance(0.3)
                    .then(|| rng.duration(Duration::ZERO, Duration::from_millis(500)));
                cluster.recover(jump, gap);
            }
            "clog" => {
                let ips: Vec<IpAddr> = processes
                    .iter()
                    .map(|p| p.addr.ip())
                    .chain([client_ip()])
                    .collect();
                let d = rng.duration(Duration::from_millis(20), Duration::from_secs(2));
                if rng.chance(0.5) {
                    sim.clog_ip(*rng.pick(&ips), d);
                } else {
                    let (a, b) = (*rng.pick(&ips), *rng.pick(&ips));
                    sim.clog_pair(a, b, d);
                }
            }
            "partition" => {
                let cut: Vec<IpAddr> = processes
                    .iter()
                    .filter(|_| rng.chance(0.4))
                    .map(|p| p.addr.ip())
                    .collect();
                if cut.is_empty() {
                    continue;
                }
                sim.partition(&[client_ip()], &cut);
                let s = sim.clone();
                let d = rng.duration(Duration::from_millis(200), Duration::from_secs(6));
                later(
                    &mut healers,
                    d,
                    Box::new(move || s.heal_partition(&[client_ip()], &cut)),
                );
            }
            "reset" | "half-open" => {
                let conns: Vec<u64> = sim
                    .connections()
                    .into_iter()
                    .filter(|c| c.client_node == client)
                    .map(|c| c.id)
                    .collect();
                if conns.is_empty() {
                    continue;
                }
                let id = *rng.pick(&conns);
                if fault == "reset" {
                    sim.reset_connection(id);
                } else {
                    sim.half_open_connection(id);
                }
            }
            "lag" => {
                let ss = rng.below(storage) as usize;
                cluster.set_storage_lag(
                    ss,
                    rng.duration(Duration::from_millis(50), Duration::from_secs(3)),
                );
                let c = cluster.clone();
                let d = rng.duration(Duration::from_millis(200), Duration::from_secs(5));
                later(
                    &mut healers,
                    d,
                    Box::new(move || c.set_storage_lag(ss, Duration::ZERO)),
                );
            }
            "behind" => {
                let ss = rng.below(storage) as usize;
                cluster.set_storage_behind(
                    ss,
                    rng.duration(Duration::from_millis(50), Duration::from_secs(3)),
                );
            }
            "move-shard" => cluster.move_shard(),
            "split-shard" => cluster.split_shard(),
            "throttle-grv" => {
                cluster
                    .throttle_grv(rng.duration(Duration::from_millis(20), Duration::from_secs(2)));
            }
            "throttle-commit" => cluster
                .throttle_commits(rng.duration(Duration::from_millis(20), Duration::from_secs(2))),
            "commit-crash" => {
                let point = if rng.chance(0.5) {
                    CrashPoint::BeforeApply
                } else {
                    CrashPoint::AfterApply
                };
                let mode = if rng.chance(0.5) {
                    KillMode::Reboot
                } else {
                    KillMode::PowerOff
                };
                cluster.arm_commit_crash(point, mode);
            }
            _ => unreachable!("every fault is handled"),
        }
    }
    for h in healers {
        h.abort();
    }
    sim.set_net_config(NetConfig::default());
    sim.heal_network();
    cluster.heal();
    sim.trace(TraceLevel::Events, "chaos-end", &[], String::new);
}

/// The failure monitor and the load balancing converge after a storage replica dies and
/// comes back.
async fn convergence_phase<R: Runtime>(
    sim: Sim,
    cluster: SimCluster,
    db: Database<R>,
    mut rng: SimRng,
    peer_evictor: bool,
) -> Vec<String> {
    let mut found = Vec::new();
    let address = |p: &crate::cluster::ProcessInfo| {
        NetworkAddress::new(p.addr.ip().into(), p.addr.port(), true, false)
    };
    sim.trace(TraceLevel::Events, "convergence", &[], String::new);
    sim.sleep(Duration::from_secs(10)).await;

    // 1. A quiet, healthy cluster: none of the servers the client depends on (the
    // coordinators, which the cluster monitor keeps referenced, and the current proxies,
    // which the read path references when it next asks for a read version) is marked failed
    // within 20 s of using them. A server the client no longer uses may stay marked failed:
    // nothing probes an unreferenced peer, in C++ either.
    let mut healthy = false;
    for _ in 0..20 {
        if let Err(e) = read_once(&db, b"c/count").await {
            found.push(format!(
                "convergence: a read failed on a healthy cluster: {e:?}"
            ));
            return found;
        }
        let proxies = cluster.proxy_processes();
        let failed: Vec<String> = cluster
            .processes()
            .iter()
            .filter(|p| p.class == ProcessClass::Coordinator || proxies.contains(&p.index))
            .filter(|p| p.alive && db.peers().failure_monitor().is_failed(&address(p)))
            .map(|p| p.name.clone())
            .collect();
        if failed.is_empty() {
            healthy = true;
            break;
        }
        sim.sleep(Duration::from_secs(1)).await;
    }
    if !healthy {
        found.push(format!(
            "convergence: live servers still marked failed 30 s after the faults: {}",
            client_state(&db, &cluster)
        ));
        return found;
    }

    // 2. Power off a replica of a replicated shard; reads of the shard keep working.
    let keys: Vec<Vec<u8>> = (0..8).map(workload::account).collect();
    let Some(key) = keys.iter().find(|k| cluster.team_of(k).len() >= 2).cloned() else {
        return found;
    };
    let team = cluster.team_of(&key);
    let ss = *rng.pick(&team);
    let victim = cluster.storage_process(ss);
    let info = cluster.processes()[victim].clone();
    cluster.kill_process(victim, KillMode::PowerOff);
    let killed_at = sim.now();
    let mut slow_late = 0;
    while sim.now() - killed_at < Duration::from_secs(35) {
        let t0 = sim.now();
        let read = read_once(&db, &key).await;
        let took = sim.now() - t0;
        sim.trace(
            TraceLevel::Events,
            "convergence-read",
            &[took.as_micros() as u64],
            || format!("took {took:?}: {read:?}"),
        );
        if let Err(e) = read {
            found.push(format!(
                "convergence: a read failed with a replica down: {e:?}"
            ));
            return found;
        }
        // A read may wait out `allAlternativesFailedDelay`, which grows to
        // `ALTERNATIVES_FAILURE_SLOW_MAX_DELAY` (30 s) while alternatives keep failing.
        if took > Duration::from_secs(35) {
            found.push(format!(
                "convergence: a read took {took:?} with a replica down"
            ));
        }
        if t0 - killed_at > Duration::from_secs(20) && took > Duration::from_millis(100) {
            slow_late += 1;
        }
        sim.sleep(Duration::from_millis(100)).await;
    }
    // With the peer evictor (8.0's default), a sweep that sees the dead replica's connects
    // failing forgets the cached locations that name it; the shard map still lists it (no
    // data distribution here), so the next read caches it again with a new peer, which
    // starts available (C++'s `Peer::Peer`) and is found failed again: a slow read and a
    // failure monitor that says "available" for a while, once a sweep, as in C++ 8.0. Reads
    // must still work (checked above); the late checks are for the evictor off.
    if slow_late > 0 && !peer_evictor {
        found.push(format!(
            "convergence: {slow_late} reads were still slow (> 100 ms) 20 s after a replica \
             died"
        ));
    }
    if !peer_evictor && !db.peers().failure_monitor().is_failed(&address(&info)) {
        found.push(format!(
            "convergence: the failure monitor does not mark {} failed 35 s after it died",
            info.name
        ));
    }

    // 3. Restart it: it comes back in the failure monitor and gets requests again.
    cluster.restart_process(victim);
    let before = cluster.requests(victim);
    let restarted = sim.now();
    let mut back = false;
    while sim.now() - restarted < Duration::from_secs(60) {
        if let Err(e) = read_once(&db, &key).await {
            found.push(format!(
                "convergence: a read failed after the restart: {e:?}"
            ));
            return found;
        }
        if cluster.requests(victim) > before
            && !db.peers().failure_monitor().is_failed(&address(&info))
        {
            back = true;
            break;
        }
        sim.sleep(Duration::from_millis(100)).await;
    }
    if !back {
        found.push(format!(
            "convergence: {} got no request (or is still marked failed) 60 s after its restart",
            info.name
        ));
    }
    found
}

/// Reads `key` in a fresh transaction, retrying as `on_error` says.
async fn read_once<R: Runtime>(
    db: &Database<R>,
    key: &[u8],
) -> Result<Option<Vec<u8>>, fdb_native_client::Error> {
    let tx = db.create_transaction();
    loop {
        match tx.get(key).await {
            Ok(v) => return Ok(v),
            Err(e) => tx.on_error(e).await?,
        }
    }
}
