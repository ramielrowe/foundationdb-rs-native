//! The load the client puts on a cluster, against the model cluster on a clean network
//! (`docs/well-behaved-client.md`): no more requests than a C++ client sends for the same
//! work. The coordinator long poll is one outstanding request, read versions are batched
//! (and every transaction is counted in a batch's `transactionCount`, which is what
//! ratekeeper meters), and a throttled cluster sees the resource-constrained backoff.

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use fdb_native_client::cluster::ConnectionRecord;
use fdb_native_client::error::codes;
use fdb_native_client::{Database, DatabaseOptions};
use fdb_native_sim::cluster::{ClusterConfig, SimCluster};
use fdb_native_sim::{Sim, SimRuntime, TraceLevel};

struct Setup {
    sim: Sim,
    cluster: SimCluster,
    db: Database<SimRuntime>,
    client: fdb_native_sim::NodeId,
}

fn setup(seed: u64) -> Setup {
    let sim = Sim::new(seed, TraceLevel::Quiet);
    let mut rng = sim.rng();
    let cluster = SimCluster::start(&sim, ClusterConfig::default(), rng.fork());
    let client = sim.add_node("client", IpAddr::V4(Ipv4Addr::new(10, 0, 9, 1)));
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

fn write_one(s: &Setup) {
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
}

/// `monitorProxies` keeps exactly one `OpenDatabaseCoordRequest` outstanding: the first (with
/// no known info) is answered at once, the next waits at the coordinator until the info
/// changes. An idle client sends nothing more; a recovery costs one request per new info.
#[test]
fn the_coordinator_long_poll_is_one_outstanding_request() {
    for seed in 0..4 {
        let s = setup(seed);
        write_one(&s);
        s.sim.run_for(Duration::from_secs(600)).unwrap();
        let idle = s.cluster.stats().open_database_requests;
        assert_eq!(idle, 2, "seed {seed}: an idle client long-polls once");

        s.cluster.recover(false, None);
        write_one(&s);
        s.sim.run_for(Duration::from_secs(600)).unwrap();
        let after = s.cluster.stats().open_database_requests;
        assert_eq!(after, idle + 1, "seed {seed}: one request per new info");
        s.sim.block_on(s.db.shutdown()).unwrap();
    }
}

/// `readVersionBatcher`: concurrent transactions share `GetReadVersionRequest`s, and the
/// requests' `transactionCount`s add up to the transactions, which is what the GRV proxy
/// and ratekeeper count as started.
#[test]
fn read_versions_are_batched_and_every_transaction_is_counted() {
    for seed in 0..4 {
        let s = setup(seed);
        write_one(&s);
        let before = s.cluster.stats();
        const N: u64 = 200;
        let tasks: Vec<_> = (0..N)
            .map(|_| {
                let tx = s.db.create_transaction();
                s.sim
                    .spawn_on(s.client, async move { tx.get_read_version().await })
            })
            .collect();
        s.sim
            .block_on_within(Duration::from_secs(60), async move {
                for t in tasks {
                    t.await.expect("joined").expect("a read version");
                }
            })
            .expect("in time");
        let after = s.cluster.stats();
        let requests = after.grv_requests - before.grv_requests;
        let transactions = after.grv_transactions - before.grv_transactions;
        assert_eq!(transactions, N, "seed {seed}");
        // How many batches depends on how the scheduler interleaves the starts with the
        // batcher's timer (`batchTime` follows the reply latency); one per transaction would
        // be 200.
        assert!(
            requests <= N / 10,
            "seed {seed}: {requests} GRV requests for {N} transactions started together"
        );
        s.sim.block_on(s.db.shutdown()).unwrap();
    }
}

/// `grv_proxy_memory_limit_exceeded` is retried with the backoff growing to
/// `RESOURCE_CONSTRAINED_MAX_BACKOFF` (30 s), not `DEFAULT_MAX_BACKOFF` (1 s): a retry
/// loop asks a throttled cluster a few times a minute, not every half second.
#[test]
fn a_throttled_grv_proxy_sees_the_resource_constrained_backoff() {
    for seed in 0..4 {
        let s = setup(seed);
        write_one(&s);
        let before = s.cluster.stats();
        const THROTTLE: Duration = Duration::from_secs(120);
        s.cluster.throttle_grv(THROTTLE);
        let db = s.db.clone();
        let started = s.sim.now();
        let task = s.sim.spawn_on(s.client, async move {
            db.run(|tx, _| async move { tx.get_read_version().await })
                .await
        });
        let rv = s
            .sim
            .block_on_within(Duration::from_secs(600), task)
            .expect("in time")
            .expect("joined")
            .expect("a read version once the throttle ends");
        assert!(rv > 0);
        let waited = s.sim.now().saturating_duration_since(started);
        assert!(waited >= THROTTLE, "seed {seed}: {waited:?}");
        let after = s.cluster.stats();
        let throttled = after.throttled - before.throttled;
        // 10 ms doubling, a random fraction of it each time: about 15 attempts reach 2 minutes
        // (C++ makes the same number). Capped at 1 s it would be more than 200.
        assert!(
            (5..=40).contains(&throttled),
            "seed {seed}: {throttled} GRV requests while throttled for {THROTTLE:?}"
        );
        s.sim.block_on(s.db.shutdown()).unwrap();
    }
}

/// The same for `commit_proxy_memory_limit_exceeded` on commits.
#[test]
fn a_throttled_commit_proxy_sees_the_resource_constrained_backoff() {
    for seed in 0..4 {
        let s = setup(seed);
        write_one(&s);
        let before = s.cluster.stats();
        const THROTTLE: Duration = Duration::from_secs(120);
        s.cluster.throttle_commits(THROTTLE);
        let db = s.db.clone();
        let task = s.sim.spawn_on(s.client, async move {
            db.run(|tx, _| async move {
                tx.set(b"k", b"w");
                Ok(())
            })
            .await
        });
        let result = s
            .sim
            .block_on_within(Duration::from_secs(600), task)
            .expect("in time")
            .expect("joined");
        match result {
            Ok(()) => {}
            Err(e) => assert_eq!(e.code(), codes::COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED),
        }
        let after = s.cluster.stats();
        let throttled = after.throttled - before.throttled;
        assert!(
            (5..=40).contains(&throttled),
            "seed {seed}: {throttled} commits while throttled for {THROTTLE:?}"
        );
        s.sim.block_on(s.db.shutdown()).unwrap();
    }
}
