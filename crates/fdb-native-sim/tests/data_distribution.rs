//! The model cluster's data distribution (6iv): a storage server that stays down is taken
//! out of its teams after a few seconds (a live server takes its place, serving from the
//! version it was copied at), one that comes back within that time is left alone, and one
//! that comes back later rejoins after a delay. A client with the location cache's peer
//! evictor on, reading through all of it, keeps getting the right values and stops dialing
//! the dead server once its shards are looked up again.

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use fdb_native_client::cluster::ConnectionRecord;
use fdb_native_client::{Database, DatabaseOptions};
use fdb_native_sim::cluster::{ClusterConfig, SimCluster};
use fdb_native_sim::{KillMode, Sim, SimRuntime, TraceLevel};

fn cluster(sim: &Sim, data_distribution: bool) -> SimCluster {
    let mut config = ClusterConfig::default();
    config.storage = 4;
    config.shards = 4;
    config.replication = 2;
    config.data_distribution = data_distribution;
    SimCluster::start(sim, config, sim.rng())
}

fn keys() -> Vec<Vec<u8>> {
    (b'a'..=b'z')
        .map(|c| format!("{}/10", c as char).into_bytes())
        .collect()
}

/// The storage servers that appear in a team of some key.
fn named(cluster: &SimCluster) -> Vec<usize> {
    let mut all: Vec<usize> = keys().iter().flat_map(|k| cluster.team_of(k)).collect();
    all.sort_unstable();
    all.dedup();
    all
}

#[test]
fn a_storage_server_that_stays_down_is_taken_out_of_its_teams_and_rejoins() {
    let sim = Sim::new(5, TraceLevel::Quiet);
    let cluster = cluster(&sim, true);
    let ss = cluster.team_of(b"a/10")[0];
    let teams_before: Vec<Vec<usize>> = keys().iter().map(|k| cluster.team_of(k)).collect();
    assert!(teams_before.iter().all(|t| t.len() == 2));

    // Back before the detection delay (2 s at least): nothing moves.
    let process = cluster.storage_process(ss);
    cluster.kill_process(process, KillMode::PowerOff);
    sim.run_for(Duration::from_millis(1500)).unwrap();
    cluster.restart_process(process);
    sim.run_for(Duration::from_secs(30)).unwrap();
    assert!(!cluster.dd_evicted(ss));
    let teams: Vec<Vec<usize>> = keys().iter().map(|k| cluster.team_of(k)).collect();
    assert_eq!(teams, teams_before);

    // Down for good: its shards go to teams without it, of the same size.
    cluster.kill_process(process, KillMode::PowerOff);
    sim.run_for(Duration::from_secs(9)).unwrap();
    assert!(cluster.dd_evicted(ss));
    for k in keys() {
        let team = cluster.team_of(&k);
        assert!(!team.contains(&ss), "{k:?}: {team:?}");
        assert_eq!(team.len(), 2, "{k:?}: {team:?}");
    }
    assert!(!named(&cluster).contains(&ss));

    // Back: it takes shards again after a delay (at most 10 s).
    cluster.restart_process(process);
    assert!(cluster.dd_evicted(ss));
    sim.run_for(Duration::from_secs(11)).unwrap();
    assert!(!cluster.dd_evicted(ss));
    assert!(named(&cluster).contains(&ss));
    for k in keys() {
        assert_eq!(cluster.team_of(&k).len(), 2);
    }
    assert!(cluster.violations().is_empty());
}

#[test]
fn a_shard_with_no_other_live_replica_stays_where_it_is() {
    let sim = Sim::new(6, TraceLevel::Quiet);
    let cluster = cluster(&sim, true);
    let team = cluster.team_of(b"a/10");
    for ss in &team {
        cluster.kill_process(cluster.storage_process(*ss), KillMode::PowerOff);
    }
    sim.run_for(Duration::from_secs(20)).unwrap();
    // Nothing to copy it from: the team is as it was (the data is unavailable, as in FDB).
    assert_eq!(cluster.team_of(b"a/10"), team);
}

#[test]
fn without_data_distribution_the_teams_never_change() {
    let sim = Sim::new(7, TraceLevel::Quiet);
    let cluster = cluster(&sim, false);
    let teams: Vec<Vec<usize>> = keys().iter().map(|k| cluster.team_of(k)).collect();
    cluster.kill_process(cluster.storage_process(0), KillMode::PowerOff);
    sim.run_for(Duration::from_secs(60)).unwrap();
    let after: Vec<Vec<usize>> = keys().iter().map(|k| cluster.team_of(k)).collect();
    assert_eq!(teams, after);
    assert!(!cluster.dd_evicted(0));
}

#[test]
fn a_client_reads_through_the_move_and_stops_dialing_the_dead_server() {
    let sim = Sim::new(8, TraceLevel::Quiet);
    let mut rng = sim.rng();
    let cluster = cluster(&sim, true);
    let client = sim.add_node("client", IpAddr::V4(Ipv4Addr::new(10, 0, 9, 1)));
    let rt: SimRuntime = SimRuntime::new(&sim, client);
    let mut options = DatabaseOptions::default();
    options.peers.seed = Some(rng.next_u64());
    options.monitor.seed = Some(rng.next_u64());
    options.read.seed = Some(rng.next_u64());
    options.read.location_cache_peer_evictor = true;
    options.commit.seed = Some(rng.next_u64());
    let record = ConnectionRecord::memory(cluster.connection_string().parse().unwrap());
    let db = Database::from_record(rt, record, options).expect("the database opens");

    let read_all = || {
        let db = db.clone();
        sim.block_on_within(Duration::from_secs(120), async move {
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
        .expect("read")
    };
    {
        let db = db.clone();
        sim.block_on_within(Duration::from_secs(60), async move {
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
    let want: Vec<Option<Vec<u8>>> = keys().into_iter().map(Some).collect();
    assert_eq!(read_all(), want);

    let addr = |ss: usize| cluster.processes()[cluster.storage_process(ss)].addr;
    let ss = (0..4)
        .find(|&ss| sim.dials(addr(ss)) > 0)
        .expect("a storage server the client read from");
    cluster.kill_process(cluster.storage_process(ss), KillMode::PowerOff);
    // Reads keep working while it is down, before and after the move, and after the evictor's
    // sweep (60 s) forgot the shards that named it.
    for _ in 0..30 {
        assert_eq!(read_all(), want);
        sim.run_for(Duration::from_secs(5)).unwrap();
    }
    assert!(cluster.dd_evicted(ss));
    assert!(db.reader().peer_evictions() >= 1);
    // The shards were looked up again, and the dead server is no longer dialed.
    let dials = sim.dials(addr(ss));
    assert_eq!(read_all(), want);
    sim.run_for(Duration::from_secs(60)).unwrap();
    assert_eq!(read_all(), want);
    assert_eq!(sim.dials(addr(ss)), dials);
    assert!(cluster.violations().is_empty());
    sim.block_on(db.shutdown()).unwrap();
}
