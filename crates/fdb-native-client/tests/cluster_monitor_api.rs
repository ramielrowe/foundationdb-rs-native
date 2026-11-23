//! The cluster monitor's waits for the read and commit paths (separate GRV and commit proxy
//! waits, `proxies_changed`, `first_commit_proxy`), a cluster upgrade under a running
//! monitor, a cluster file that can no longer be written, concurrent shutdowns, and the
//! hostname coordinators' references; against fake coordinators under paused time.

mod support;

use std::time::Duration;

use fdb_native_client::{
    ClusterConnectionString, ClusterMonitor, ClusterMonitorConfig, ConnectionRecord,
};
use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::ClientDbInfo;
use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig};
use fdb_native_wire::{ProtocolVersion, Uid};
use support::{Call, FakeCoordinator, MemRuntime, Net, forward, info, sock};

const FAILED_TO_PROGRESS: u16 = 1216;
const SHORT: Duration = Duration::from_millis(100);

fn manager(rt: &MemRuntime, versions: Vec<ProtocolVersion>) -> PeerManager<MemRuntime> {
    let mut config = PeerManagerConfig::default().without_jitter();
    config.supported_versions = versions;
    config.seed = Some(1);
    PeerManager::new(rt.clone(), config, DiscardEvents)
}

fn cs(s: &str) -> ClusterConnectionString {
    s.parse().unwrap()
}

fn start(
    rt: &MemRuntime,
    peers: &PeerManager<MemRuntime>,
    record: ConnectionRecord,
) -> ClusterMonitor<MemRuntime> {
    let mut config = ClusterMonitorConfig::default();
    config.seed = Some(7);
    ClusterMonitor::start(rt.clone(), peers.clone(), record, config)
}

async fn next_any(coordinators: &mut [FakeCoordinator]) -> (usize, Call) {
    for _ in 0..6000 {
        for (i, c) in coordinators.iter_mut().enumerate() {
            if let Some(call) = c.try_call() {
                return (i, call);
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no request within 60 s");
}

fn references(peers: &PeerManager<MemRuntime>, coordinator: &FakeCoordinator) -> usize {
    peers.peer(&coordinator.address()).status().references
}

/// GRV and commit proxies are waited for independently, and `proxies_changed` fires only
/// when the proxies in use change, not for every new info.
#[tokio::test(start_paused = true)]
async fn independent_proxy_waits_and_proxies_changed() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let mut coord = FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(
        &rt,
        &peers,
        ConnectionRecord::memory(cs("test:abc@10.0.0.1:4500")),
    );

    // GRV proxies only.
    let info1 = info(1, 1, 0, SchemaVersion::V7_4);
    coord.next_call().await.reply(&info1);
    let got1 = monitor.grv_proxies(false).await.unwrap();
    assert_eq!(got1.id(), info1.id);
    assert!(got1.first_commit_proxy().is_none());
    assert!(
        tokio::time::timeout(SHORT, monitor.commit_proxies(false))
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(SHORT, monitor.proxies(false))
            .await
            .is_err()
    );

    // A new info with the same proxies: a change, but not of the proxies.
    let same = ClientDbInfo {
        id: Uid::new(0xdb, 100),
        ..info1.clone()
    };
    coord.next_call().await.reply(&same);
    let got2 = monitor.changed_since(&got1).await.unwrap();
    assert_eq!(got2.id(), same.id);
    assert_eq!(got2.grv_proxy_ids(), got1.grv_proxy_ids());
    assert!(
        tokio::time::timeout(SHORT, monitor.proxies_changed(&got1))
            .await
            .is_err()
    );

    // Commit proxies appear: the proxies changed, and commit proxies are available.
    let mut info3 = info1.clone();
    info3.id = Uid::new(0xdb, 3);
    info3.commit_proxies = info(3, 0, 2, SchemaVersion::V7_4).commit_proxies;
    let waiter = async { monitor.proxies_changed(&got2).await.unwrap() };
    let reply = async { coord.next_call().await.reply(&info3) };
    let (got3, ()) = tokio::join!(waiter, reply);
    assert_eq!(got3.id(), info3.id);
    assert_eq!(got3.grv_proxy_ids(), got1.grv_proxy_ids());
    assert_eq!(got3.commit_proxy_ids().len(), 2);
    assert_eq!(monitor.commit_proxies(false).await.unwrap().id(), info3.id);
    assert_eq!(monitor.proxies(false).await.unwrap().id(), info3.id);
    // At once when the current info already differs.
    assert_eq!(monitor.proxies_changed(&got1).await.unwrap().id(), info3.id);
    assert_eq!(
        got3.first_commit_proxy().unwrap().proxy,
        info3.commit_proxies[0]
    );
    monitor.shutdown().await;
}

/// The cluster is upgraded under a running monitor: the coordinator restarts speaking 8.0,
/// the transport switches, and the next info is decoded with 8.0's schema set.
#[tokio::test(start_paused = true)]
async fn cluster_upgrade_switches_the_schema_set() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let mut coord = FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4, ProtocolVersion::V8_0]);
    let monitor = start(
        &rt,
        &peers,
        ConnectionRecord::memory(cs("test:abc@10.0.0.1:4500")),
    );

    let call = coord.next_call().await;
    assert_eq!(call.schema, SchemaVersion::V7_4);
    call.reply(&info(1, 1, 1, SchemaVersion::V7_4));
    let got = monitor.ready().await.unwrap();
    assert_eq!(got.schema, SchemaVersion::V7_4);
    let _long_poll = coord.next_call().await;

    coord.set_version(ProtocolVersion::V8_0);
    coord.close_connections();
    let call = coord.next_call().await;
    assert_eq!(call.schema, SchemaVersion::V8_0);
    assert_eq!(call.request.known_client_info_id, got.id());
    assert_eq!(
        coord.announced(),
        vec![
            ProtocolVersion::V7_4,
            ProtocolVersion::V7_4,
            ProtocolVersion::V8_0
        ],
        "the old version is announced once more, then the new one"
    );
    call.reply(&info(2, 1, 1, SchemaVersion::V8_0));
    let got = monitor.changed_since(&got).await.unwrap();
    assert_eq!(got.schema, SchemaVersion::V8_0);
    assert!(got.protocol_version.is_compatible(ProtocolVersion::V8_0));
    assert_eq!(monitor.schema_version(), Some(SchemaVersion::V8_0));
    assert!(!got.commit_proxies.is_empty());
    for cp in &got.commit_proxies {
        assert_eq!(cp.proxy.schema, SchemaVersion::V8_0);
        assert_eq!(cp.streams.expire_idempotency_id, cp.proxy.base.adjusted(9));
        assert!(cp.streams.get_tenant_id.is_none());
    }
    monitor.shutdown().await;
}

/// A cluster file that can no longer be written: the failure is reported, the new string is
/// used anyway, and later requests carry `incorrect_cluster_file_contents`.
#[tokio::test(start_paused = true)]
async fn failed_persist_is_reported() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let dir = tempfile::tempdir().unwrap();
    let sub = dir.path().join("etc");
    std::fs::create_dir(&sub).unwrap();
    let path = sub.join("fdb.cluster");
    std::fs::write(&path, "test:abc@10.0.0.1:4500\n").unwrap();
    let mut old = FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4);
    let mut new = FakeCoordinator::start(&net, sock(9, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(&rt, &peers, ConnectionRecord::open(&path).unwrap());

    old.next_call()
        .await
        .reply(&forward(1, "test:moved@10.0.0.9:4500"));
    let call = new.next_call().await;
    assert!(call.request.issues.is_empty());
    // The file's directory goes away before the new coordinators answer.
    std::fs::remove_dir_all(&sub).unwrap();
    call.reply(&info(1, 1, 1, SchemaVersion::V7_4));
    let got = monitor.ready().await.unwrap();
    assert_eq!(got.connection_string, cs("test:moved@10.0.0.9:4500"));
    let status = monitor.status();
    let error = status.persist_error.expect("a persist error");
    assert!(error.contains("fdb.cluster"), "{error}");
    assert_eq!(
        status.record_connection_string,
        cs("test:moved@10.0.0.9:4500")
    );

    let call = new.next_call().await;
    assert_eq!(
        call.request.issues,
        vec![b"incorrect_cluster_file_contents".to_vec()]
    );
    monitor.shutdown().await;
}

/// Concurrent shutdowns all return once the task has ended.
#[tokio::test(start_paused = true)]
async fn concurrent_shutdowns_all_wait() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let mut coord = FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(
        &rt,
        &peers,
        ConnectionRecord::memory(cs("test:abc@10.0.0.1:4500")),
    );
    let _call = coord.next_call().await;
    assert!(!monitor.is_closed());
    let check = || {
        assert!(monitor.is_closed());
        assert_eq!(references(&peers, &coord), 0);
    };
    tokio::join!(
        async {
            monitor.shutdown().await;
            check();
        },
        async {
            monitor.shutdown().await;
            check();
        },
        async {
            tokio::task::yield_now().await;
            monitor.shutdown().await;
            check();
        },
    );
}

/// A hostname coordinator's resolved address is referenced only while it is the one asked.
#[tokio::test(start_paused = true)]
async fn hostname_reference_is_released_on_rotation() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    net.set_host("coord.test", vec![sock(5, 4500).ip()]);
    let mut coords = vec![
        FakeCoordinator::start(&net, sock(5, 4500), ProtocolVersion::V7_4),
        FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4),
    ];
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(
        &rt,
        &peers,
        ConnectionRecord::memory(cs("test:abc@coord.test:4500,10.0.0.1:4500")),
    );
    // Get to a request at the hostname coordinator.
    let call = loop {
        let (i, call) = next_any(&mut coords).await;
        if i == 0 {
            break call;
        }
        call.reply_error(FAILED_TO_PROGRESS);
    };
    assert!(references(&peers, &coords[0]) >= 1);
    // Rotating away releases it; the address coordinator stays referenced.
    call.reply_error(FAILED_TO_PROGRESS);
    let (i, _call) = next_any(&mut coords).await;
    assert_eq!(i, 1);
    assert_eq!(references(&peers, &coords[0]), 0);
    assert!(references(&peers, &coords[1]) >= 1);
    monitor.shutdown().await;
}
