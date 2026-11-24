//! The lifecycle of a transaction and of a database, on the model cluster: nothing of a
//! transaction outlives its last handle, even while its reads cannot finish
//! (`~ReadYourWritesTransaction` cancels them), and nothing of the database outlives
//! `Database::shutdown`, even while the database and a transaction are still held.

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use fdb_native_client::cluster::ConnectionRecord;
use fdb_native_client::error::codes;
use fdb_native_client::{Database, DatabaseOptions, KeySelector, RangeOptions};
use fdb_native_sim::cluster::{ClusterConfig, ProcessClass, SimCluster};
use fdb_native_sim::{NodeId, Sim, SimRuntime, TraceLevel};
use fdb_native_transport::peer::{Generation, PeerHandle, PeerPhase};
use fdb_native_wire::NetworkAddress;

const CLIENT_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 9, 1));

struct Setup {
    sim: Sim,
    cluster: SimCluster,
    db: Database<SimRuntime>,
    client: NodeId,
}

fn setup(seed: u64) -> Setup {
    let sim = Sim::new(seed, TraceLevel::Quiet);
    let mut rng = sim.rng();
    let cluster = SimCluster::start(&sim, ClusterConfig::default(), rng.fork());
    let client = sim.add_node("client", CLIENT_IP);
    let rt = SimRuntime::new(&sim, client);
    let mut options = DatabaseOptions::default();
    options.peers.seed = Some(rng.next_u64());
    options.monitor.seed = Some(rng.next_u64());
    options.read.seed = Some(rng.next_u64());
    options.commit.seed = Some(rng.next_u64());
    let record = ConnectionRecord::memory(cluster.connection_string().parse().unwrap());
    let db = Database::from_record(rt, record, options).expect("the database opens");
    Setup {
        sim,
        cluster,
        db,
        client,
    }
}

/// The client tasks a transaction spawned (its reads, its watches' reads, its read version
/// request) that are still alive.
fn transaction_tasks(s: &Setup) -> Vec<String> {
    s.sim
        .live_tasks(s.client)
        .into_iter()
        .filter(|t| t.location.contains("fdb-native-client/src/transaction"))
        .map(|t| t.to_string())
        .collect()
}

#[test]
fn a_dropped_transaction_ends_its_stuck_reads_and_shutdown_ends_everything() {
    for seed in 0..4 {
        let s = setup(seed);
        let db = s.db.clone();
        s.sim
            .block_on_within(Duration::from_secs(60), async move {
                db.run(|tx, _| async move {
                    tx.set(b"k", b"v");
                    Ok(())
                })
                .await
            })
            .expect("in time")
            .expect("committed");

        // The storage servers cannot be reached: the reads wait (there is no timeout).
        let storage: Vec<IpAddr> = s
            .cluster
            .processes()
            .iter()
            .filter(|p| matches!(p.class, ProcessClass::Storage(_)))
            .map(|p| p.addr.ip())
            .collect();
        s.sim.partition(&[CLIENT_IP], &storage);
        let tx = s.db.create_transaction();
        let get = tx.get(b"k");
        let range = tx.get_range(
            &KeySelector::first_greater_or_equal(b"a".to_vec()),
            &KeySelector::first_greater_or_equal(b"z".to_vec()),
            &RangeOptions::default(),
        );
        let watch = tx.watch(b"w");
        s.sim.run_for(Duration::from_secs(30)).unwrap();
        assert!(
            !transaction_tasks(&s).is_empty(),
            "seed {seed}: the reads are stuck"
        );

        // The last handle goes: the reads end with `transaction_cancelled`, at once.
        drop(tx);
        let results = s
            .sim
            .block_on_within(Duration::from_millis(1), async move {
                (
                    get.await.map(drop),
                    range.await.map(drop),
                    watch.await.map(drop),
                )
            })
            .expect("at once");
        for result in [results.0, results.1, results.2] {
            assert_eq!(
                result.unwrap_err().code(),
                codes::TRANSACTION_CANCELLED,
                "seed {seed}"
            );
        }
        s.sim.run_for(Duration::from_secs(5)).unwrap();
        assert_eq!(transaction_tasks(&s), Vec::<String>::new(), "seed {seed}");

        // `shutdown` with the database and a transaction held: no client task is left (the
        // read version batchers included), and a read version request fails at once.
        s.sim.heal_partition(&[CLIENT_IP], &storage);
        let held = s.db.create_transaction();
        let first = held.get_read_version();
        s.sim
            .block_on_within(Duration::from_secs(60), first)
            .expect("in time")
            .expect("a read version");
        s.sim.block_on(s.db.shutdown()).unwrap();
        s.sim.run_for(Duration::from_secs(5)).unwrap();
        let left: Vec<String> = s
            .sim
            .live_tasks(s.client)
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(left, Vec::<String>::new(), "seed {seed}");
        let late = s.db.create_transaction();
        let e = s
            .sim
            .block_on_within(Duration::from_millis(1), late.get_read_version())
            .expect("at once")
            .unwrap_err();
        assert_eq!(e.code(), codes::OPERATION_CANCELLED, "seed {seed}");
        assert!(s.sim.live_tasks(s.client).is_empty(), "seed {seed}");
        drop(held);
    }
}

/// The live client tasks, as text.
fn client_tasks(sim: &Sim, client: NodeId) -> Vec<String> {
    sim.live_tasks(client)
        .iter()
        .map(ToString::to_string)
        .collect()
}

/// Two databases opened on the client node share its connections (b35.7): the first going
/// (shut down with a transaction of it held, or dropped) leaves the second working on the same
/// connections (none of them reconnects), and nothing of the client is left once the second
/// goes too (shut down, then dropped).
#[test]
fn two_databases_share_connections_and_the_last_one_ends_everything() {
    for seed in 0..4 {
        for shut_down in [true, false] {
            let s = setup(seed);
            let rt = SimRuntime::new(&s.sim, s.client);
            let record = ConnectionRecord::memory(s.cluster.connection_string().parse().unwrap());
            let mut options = DatabaseOptions::default();
            options.peers = s.db.peers().config().clone();
            let other = Database::from_record(rt, record, options).expect("the database opens");
            assert!(s.db.peers().shares_connections_with(other.peers()));
            assert_eq!(other.peers().users(), 2);

            // A write, then a read of it (a storage server), through one database.
            let write = |db: Database<SimRuntime>, value: &'static [u8]| async move {
                db.run(|tx, _| async move {
                    tx.set(b"k", value);
                    Ok(())
                })
                .await?;
                db.run(|tx, _| async move { tx.get(b"k").await }).await
            };
            s.sim
                .block_on_within(Duration::from_secs(60), write(s.db.clone(), b"a"))
                .expect("in time")
                .expect("committed");
            let read = s
                .sim
                .block_on_within(Duration::from_secs(60), write(other.clone(), b"b"))
                .expect("in time")
                .expect("committed");
            assert_eq!(read.as_deref(), Some(&b"b"[..]), "seed {seed}");

            // The connections both used: they must outlive the first database.
            let connected: Vec<(PeerHandle, Generation)> = s
                .cluster
                .processes()
                .iter()
                .map(|p| NetworkAddress::new(p.addr.ip().into(), p.addr.port(), true, false))
                .filter_map(|address| {
                    let peer = other.peers().peer(&address);
                    let status = peer.status();
                    (status.phase == PeerPhase::Connected).then_some((peer, status.generation))
                })
                .collect();
            assert!(!connected.is_empty());

            // The first goes, a transaction of it held: the second still reads and writes, on
            // the same connections.
            let held = s.db.create_transaction();
            let Setup {
                sim,
                cluster: _cluster,
                db,
                client,
            } = s;
            if shut_down {
                sim.block_on(db.shutdown()).unwrap();
                let e = sim
                    .block_on_within(Duration::from_millis(1), held.get_read_version())
                    .expect("at once")
                    .unwrap_err();
                assert_eq!(e.code(), codes::OPERATION_CANCELLED, "seed {seed}");
            }
            drop((held, db));
            sim.run_for(Duration::from_secs(5)).unwrap();
            assert!(!other.is_shut_down());
            assert_eq!(other.peers().users(), 1);
            let read = sim
                .block_on_within(Duration::from_secs(60), {
                    let other = other.clone();
                    async move { other.create_transaction().get(b"k").await }
                })
                .expect("in time")
                .expect("read");
            assert_eq!(read.as_deref(), Some(&b"b"[..]), "seed {seed}");
            sim.block_on_within(Duration::from_secs(60), write(other.clone(), b"c"))
                .expect("in time")
                .expect("committed");
            for (peer, generation) in &connected {
                let status = peer.status();
                assert_eq!(status.phase, PeerPhase::Connected, "seed {seed}");
                assert_eq!(status.generation, *generation, "seed {seed}: reconnected");
            }
            drop(connected);

            // The last one goes: nothing of the client is left.
            let weak = other.peers().downgrade();
            sim.block_on(other.shutdown()).unwrap();
            sim.run_for(Duration::from_secs(5)).unwrap();
            assert!(weak.is_shut_down());
            assert_eq!(
                client_tasks(&sim, client),
                Vec::<String>::new(),
                "seed {seed}"
            );
            drop(other);
            sim.run_for(Duration::from_secs(5)).unwrap();
            assert_eq!(
                client_tasks(&sim, client),
                Vec::<String>::new(),
                "seed {seed}"
            );
        }
    }
}
