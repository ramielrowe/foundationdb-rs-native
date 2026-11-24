//! The location cache's peer evictor (`locationCachePeerEvictorActor`, on by default as in
//! 8.0) against the model cluster: a storage server removed for good (its shards moved to
//! other servers, its machine dead) is probed every second while cached shards name it;
//! within one sweep of the evictor (60 s) after its connects start failing, those shards
//! are forgotten and the probes stop, and the next reads of them find the new servers with a
//! fresh location lookup, without dialing the dead one. With the evictor off (7.4's
//! default) the probes go on.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use fdb_native_client::cluster::ConnectionRecord;
use fdb_native_client::{Database, DatabaseOptions};
use fdb_native_sim::cluster::{ClusterConfig, SimCluster};
use fdb_native_sim::{KillMode, Sim, SimRuntime, TraceLevel};

struct Setup {
    sim: Sim,
    cluster: SimCluster,
    db: Database<SimRuntime>,
}

fn setup(seed: u64, evictor: bool) -> Setup {
    let sim = Sim::new(seed, TraceLevel::Quiet);
    let mut rng = sim.rng();
    let mut config = ClusterConfig::default();
    config.storage = 4;
    config.shards = 4;
    config.replication = 2;
    let cluster = SimCluster::start(&sim, config, rng.fork());
    let client = sim.add_node("client", IpAddr::V4(Ipv4Addr::new(10, 0, 9, 1)));
    let rt = SimRuntime::new(&sim, client);
    let mut options = DatabaseOptions::default();
    options.peers.seed = Some(rng.next_u64());
    options.monitor.seed = Some(rng.next_u64());
    options.read.seed = Some(rng.next_u64());
    options.read.location_cache_peer_evictor = evictor;
    options.commit.seed = Some(rng.next_u64());
    let record = ConnectionRecord::memory(cluster.connection_string().parse().unwrap());
    let db = Database::from_record(rt, record, options).expect("the database opens");
    Setup { sim, cluster, db }
}

/// A key in every part of the key space the shard boundaries are drawn from.
fn keys() -> Vec<Vec<u8>> {
    (b'a'..=b'z')
        .flat_map(|c| [10, 60].map(|n| format!("{}/{n}", c as char).into_bytes()))
        .collect()
}

fn write_all(s: &Setup) {
    let db = s.db.clone();
    s.sim
        .block_on_within(Duration::from_secs(60), async move {
            db.run(|tx, _| async move {
                for k in keys() {
                    tx.set(&k, &k);
                }
                Ok(())
            })
            .await
        })
        .expect("in time")
        .expect("committed");
}

/// Reads every key (in one transaction), checking the values.
fn read_all(s: &Setup) {
    let db = s.db.clone();
    let values = s
        .sim
        .block_on_within(Duration::from_secs(120), async move {
            db.run(|tx, _| async move {
                let mut values = Vec::new();
                for k in keys() {
                    values.push(tx.get(&k).await?);
                }
                Ok(values)
            })
            .await
        })
        .expect("in time")
        .expect("read");
    let want: Vec<Option<Vec<u8>>> = keys().into_iter().map(Some).collect();
    assert_eq!(values, want);
}

/// What one run saw of the removed storage server.
#[derive(Debug)]
struct Outcome {
    /// Dials of it in the 20 s after it died.
    early: u64,
    /// Dials of it from 130 s to 250 s after it died.
    late: u64,
    /// Dials of it while every key was read again (at 250 s).
    during_reads: u64,
    /// Location requests those reads made.
    lookups: u64,
    /// Cached ranges the evictor forgot.
    evictions: u64,
}

fn run(seed: u64, evictor: bool, mode: KillMode) -> Outcome {
    let s = setup(seed, evictor);
    write_all(&s);
    // Every shard's location is cached, so every storage server of a team is referenced;
    // the one removed is one the client has connected to (a referenced peer that never
    // connected is not dialed by itself).
    read_all(&s);
    let address =
        |ss: usize| -> SocketAddr { s.cluster.processes()[s.cluster.storage_process(ss)].addr };
    let ss = (0..s.cluster.config().storage)
        .find(|&ss| s.sim.dials(address(ss)) > 0)
        .expect("a storage server the client read from");
    let addr = address(ss);
    s.cluster.remove_storage(ss, mode);
    let dials = || s.sim.dials(addr);

    let at_kill = dials();
    s.sim.run_for(Duration::from_secs(20)).unwrap();
    let early = dials() - at_kill;
    s.sim.run_for(Duration::from_secs(110)).unwrap();
    let before_late = dials();
    s.sim.run_for(Duration::from_secs(120)).unwrap();
    let late = dials() - before_late;

    let (dials_before, lookups_before) = (dials(), s.cluster.stats().location_requests);
    read_all(&s);
    let outcome = Outcome {
        early,
        late,
        during_reads: dials() - dials_before,
        lookups: s.cluster.stats().location_requests - lookups_before,
        evictions: s.db.reader().peer_evictions(),
    };
    assert!(
        s.cluster.violations().is_empty(),
        "{:?}",
        s.cluster.violations()
    );
    s.sim.block_on(s.db.shutdown()).unwrap();
    outcome
}

/// The evictor forgets the dead server's shards within a sweep of its first failed
/// connects: the probes stop, and the next reads look the shards up again and go to the
/// servers that hold them now.
#[test]
fn a_removed_storage_server_stops_being_probed_and_reads_find_the_new_servers() {
    for seed in 0..6 {
        for mode in [KillMode::Reboot, KillMode::PowerOff] {
            let o = run(seed, true, mode);
            let what = format!("seed {seed}, {mode:?}: {o:?}");
            println!("{what}");
            // Probed while cached (every second, or every connect timeout for a machine
            // that does not answer).
            assert!(o.early >= 4, "{what}");
            // Forgotten within a sweep (60 s) of the failures: no dial after 130 s.
            assert_eq!(o.late, 0, "{what}");
            assert!(o.evictions >= 1, "{what}");
            // The reads of its shards asked for their locations again, and never dialed it.
            assert!(o.lookups >= 1, "{what}");
            assert_eq!(o.during_reads, 0, "{what}");
        }
    }
}

/// Without the evictor (7.4's default) the dead server is probed for as long as its shards
/// stay cached.
#[test]
fn without_the_evictor_a_removed_storage_server_is_probed_until_its_shards_are_read() {
    for seed in 0..3 {
        for mode in [KillMode::Reboot, KillMode::PowerOff] {
            let o = run(seed, false, mode);
            let what = format!("seed {seed}, {mode:?}: {o:?}");
            println!("{what}");
            assert!(o.early >= 4, "{what}");
            // A probe a second, or one per connect timeout and probe interval (3 s) for a
            // machine that does not answer; the bound leaves some slack.
            assert!(o.late >= 120 / 4, "{what}");
            assert_eq!(o.evictions, 0, "{what}");
        }
    }
}
