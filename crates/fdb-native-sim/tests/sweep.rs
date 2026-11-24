//! The seed sweep: every seed runs the whole simulation (a randomized cluster and network,
//! the client under a chaos schedule, the invariants, the shutdown; see
//! `fdb_native_sim::harness`), and must pass. Seeds run in parallel threads (each simulation
//! is single-threaded and independent).
//!
//! * `FDB_SIM_SEEDS` (default 200) and `FDB_SIM_SEED_START` (default 0) choose the seeds.
//! * `FDB_SIM_SEED=n` makes `replay_seed` run seed `n` alone; with `FDB_SIM_TRACE=1` (or `2`
//!   for every poll and segment) it prints the trace.
//!
//! Every tenth seed is run twice and must give the same trace hash; the sweep prints the
//! combined hash of all seeds, which is the same on every run of the same build.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use fdb_native_sim::TraceLevel;
use fdb_native_sim::harness::{SeedConfig, SeedReport, run};

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

#[test]
fn seed_sweep() {
    let count = env_u64("FDB_SIM_SEEDS").unwrap_or(200);
    let start = env_u64("FDB_SIM_SEED_START").unwrap_or(0);
    let threads = std::thread::available_parallelism().map_or(4, usize::from);
    let next = AtomicU64::new(start);
    let reports: Mutex<Vec<SeedReport>> = Mutex::new(Vec::new());
    let replays: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let wall = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                loop {
                    let seed = next.fetch_add(1, Ordering::SeqCst);
                    if seed >= start + count {
                        break;
                    }
                    let config = SeedConfig::new(seed).with_level(TraceLevel::Quiet);
                    let report = run(&config);
                    if seed % 10 == 0 {
                        let again = run(&config);
                        if again.trace_hash != report.trace_hash {
                            replays.lock().unwrap().push(format!(
                                "seed {seed}: trace {:016x} then {:016x} ({} then {} events)",
                                report.trace_hash,
                                again.trace_hash,
                                report.trace_events,
                                again.trace_events
                            ));
                        }
                    }
                    reports.lock().unwrap().push(report);
                }
            });
        }
    });
    let mut reports = reports.into_inner().unwrap();
    reports.sort_by_key(|r| r.seed);
    let mut combined = 0xcbf2_9ce4_8422_2325u64;
    for r in &reports {
        combined = (combined ^ r.trace_hash).wrapping_mul(0x0000_0100_0000_01b3);
    }
    let failed: Vec<&SeedReport> = reports.iter().filter(|r| !r.passed()).collect();
    let virtual_secs: f64 = reports.iter().map(|r| r.virtual_time.as_secs_f64()).sum();
    let mut faults = std::collections::BTreeMap::new();
    for r in &reports {
        for (k, v) in &r.faults {
            *faults.entry(*k).or_insert(0u64) += v;
        }
    }
    eprintln!(
        "sim sweep: {} seeds from {start} in {:.1?} ({virtual_secs:.0} s virtual), {} failed, \
         combined trace hash {combined:016x}",
        reports.len(),
        wall.elapsed(),
        failed.len()
    );
    eprintln!("sim sweep: faults struck {faults:?}");
    let shared = reports.iter().filter(|r| r.databases > 1).count();
    eprintln!("sim sweep: {shared} seeds with two databases sharing the connections");
    let mut errors = std::collections::BTreeMap::new();
    let (mut committed, mut not, mut maybe, mut transfers, mut reads, mut backoffs) =
        (0, 0, 0, 0, 0, 0);
    let (mut partial, mut pending, mut resets, mut untrusted, mut refused) = (0, 0, 0, 0, 0);
    let (mut crashes, mut wrong, mut future, mut behind, mut old, mut throttled, mut conflicts) =
        (0, 0, 0, 0, 0, 0, 0);
    for r in &reports {
        for (k, v) in &r.workload.errors {
            *errors.entry(*k).or_insert(0u64) += v;
        }
        committed += r.workload.committed;
        not += r.workload.not_committed;
        maybe += r.workload.maybe;
        transfers += r.workload.transfers;
        reads += r.workload.reads_checked;
        backoffs += r.workload.backoffs;
        partial += r.net.partial_writes;
        pending += r.net.pending_writes;
        resets += r.net.resets;
        untrusted += r.net.untrusted_takes;
        refused += r.net.refused;
        crashes += r.cluster.commit_crashes;
        wrong += r.cluster.wrong_shard;
        future += r.cluster.future_version;
        behind += r.cluster.process_behind;
        old += r.cluster.too_old;
        throttled += r.cluster.throttled;
        conflicts += r.cluster.conflicts;
    }
    eprintln!(
        "sim sweep: attempts {committed} committed / {not} not / {maybe} maybe, {transfers} \
         transfers, {reads} checked reads, {backoffs} backoffs checked; client errors {errors:?}"
    );
    eprintln!(
        "sim sweep: network: {partial} partial writes, {pending} pending writes, {resets} resets, \
         {untrusted} untrusted takes, {refused} refused connects; cluster: {crashes} commit \
         crashes, {conflicts} conflicts, {wrong} wrong_shard, {future} future_version, {behind} \
         process_behind, {old} too_old, {throttled} throttled"
    );
    for r in &failed {
        eprintln!("{r}");
    }
    let replays = replays.into_inner().unwrap();
    for r in &replays {
        eprintln!("NONDETERMINISTIC: {r}");
    }
    assert!(
        replays.is_empty(),
        "{} seeds did not replay identically",
        replays.len()
    );
    assert!(
        failed.is_empty(),
        "{} of {} seeds failed: {:?}",
        failed.len(),
        reports.len(),
        failed.iter().map(|r| r.seed).collect::<Vec<_>>()
    );
}

/// Runs `FDB_SIM_SEED` alone (nothing without it), printing its report (and its trace with
/// `FDB_SIM_TRACE`).
#[test]
fn replay_seed() {
    let Some(seed) = env_u64("FDB_SIM_SEED") else {
        return;
    };
    let report = run(&SeedConfig::new(seed));
    eprintln!("{report}");
    assert!(report.passed(), "seed {seed} failed");
}

/// The same seed gives the same trace (hash) every time, whatever the process's hash map
/// seeds; different seeds give different ones.
#[test]
fn a_seed_replays_identically() {
    let hashes: Vec<Vec<u64>> = (0..3)
        .map(|_| {
            (0..4)
                .map(|seed| {
                    let report = run(&SeedConfig::new(seed).with_level(TraceLevel::Quiet));
                    assert!(report.passed(), "{report}");
                    assert!(report.trace_events > 1000);
                    report.trace_hash
                })
                .collect()
        })
        .collect();
    assert_eq!(hashes[0], hashes[1]);
    assert_eq!(hashes[0], hashes[2]);
    let mut distinct = hashes[0].clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), 4, "different seeds, different traces");
}
