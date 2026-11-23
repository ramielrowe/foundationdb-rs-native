//! The cluster monitor against scripted fake coordinators on an in-memory network, under
//! tokio's paused clock: the first info and the long poll, failover between coordinators,
//! the reconnection delay, forwarding (with the cluster file rewritten), cluster file
//! changes, recoveries (no proxies, provisional proxies), version detection, hostnames, and
//! shutdown.

mod support;

use std::time::Duration;

use fdb_native_client::cluster::CLUSTER_FILE_HEADER;
use fdb_native_client::{
    ClusterConnectionString, ClusterMonitor, ClusterMonitorConfig, ConnectionRecord, MonitorClosed,
    MonitorError,
};
use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::{ClientDbInfo, Hostname};
use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig};
use fdb_native_transport::rpc::RequestError;
use fdb_native_wire::{FdbError, ProtocolVersion, Uid};
use support::{Call, FakeCoordinator, MemRuntime, Net, forward, info, sock, until};

const FAILED_TO_PROGRESS: u16 = 1216;

fn manager(rt: &MemRuntime, versions: Vec<ProtocolVersion>) -> PeerManager<MemRuntime> {
    let mut config = PeerManagerConfig::default().without_jitter();
    config.supported_versions = versions;
    config.seed = Some(1);
    PeerManager::new(rt.clone(), config, DiscardEvents)
}

fn config() -> ClusterMonitorConfig {
    let mut config = ClusterMonitorConfig::default();
    config.seed = Some(7);
    config
}

fn cs(s: &str) -> ClusterConnectionString {
    s.parse().unwrap()
}

fn start(
    rt: &MemRuntime,
    peers: &PeerManager<MemRuntime>,
    record: ConnectionRecord,
) -> ClusterMonitor<MemRuntime> {
    ClusterMonitor::start(rt.clone(), peers.clone(), record, config())
}

/// The next request to reach any of `coordinators`, with the index of the one it reached.
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

#[tokio::test(start_paused = true)]
async fn first_info_then_long_poll() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let mut coord = FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4, ProtocolVersion::V8_0]);
    let monitor = start(
        &rt,
        &peers,
        ConnectionRecord::memory(cs("test:abc@10.0.0.1:4500")),
    );
    let mut watch = monitor.watch();

    // The first request: what fdb_c sends, with no info known yet.
    let call = coord.next_call().await;
    let request = &call.request;
    assert_eq!(request.known_client_info_id, Uid::default());
    assert_eq!(request.cluster_key, b"test:abc");
    assert_eq!(request.coordinators, vec![coord.address()]);
    assert!(request.hostnames.is_empty());
    assert!(!request.internal);
    assert!(request.issues.is_empty());
    assert_eq!(request.trace_log_group, b"default");
    assert_eq!(
        request.supported_versions,
        ClusterMonitorConfig::default_supported_versions()
    );
    assert_eq!(request.supported_versions.len(), 2);
    assert_eq!(
        request.supported_versions[0].protocol_version,
        b"fdb00b074000000"
    );
    assert!(monitor.current().is_none());
    assert_eq!(monitor.schema_version(), None);
    assert!(
        references(&peers, &coord) >= 1,
        "the coordinator is referenced"
    );

    let info1 = info(1, 2, 3, SchemaVersion::V7_4);
    call.reply(&info1);
    let got = watch.changed().await.unwrap();
    assert_eq!(got.client_info, info1);
    assert_eq!(got.id(), info1.id);
    assert_eq!(got.sequence, 1);
    assert_eq!(got.schema, SchemaVersion::V7_4);
    assert!(got.protocol_version.is_compatible(ProtocolVersion::V7_4));
    assert_eq!(got.coordinator.addr, sock(1, 4500));
    assert!(!got.coordinator.from_hostname);
    assert_eq!(got.connection_string, cs("test:abc@10.0.0.1:4500"));
    assert_eq!(monitor.schema_version(), Some(SchemaVersion::V7_4));

    // The endpoints, derived with 7.4's indices.
    assert_eq!(got.grv_proxies.len(), 2);
    assert_eq!(got.commit_proxies.len(), 3);
    for grv in &got.grv_proxies {
        assert_eq!(grv.streams.get_consistent_read_version, grv.proxy.base);
        assert_eq!(grv.streams.get_health_metrics, grv.proxy.base.adjusted(2));
    }
    for cp in &got.commit_proxies {
        assert_eq!(cp.streams.commit, cp.proxy.base);
        assert_eq!(
            cp.streams.get_key_servers_locations,
            cp.proxy.base.adjusted(2)
        );
        assert_eq!(cp.streams.expire_idempotency_id, cp.proxy.base.adjusted(10));
        assert!(cp.streams.get_tenant_id.is_some());
    }
    assert!(got.has_proxies(false));

    // The long poll: the same coordinator, with the info's id.
    let call = coord.next_call().await;
    assert_eq!(call.request.known_client_info_id, info1.id);
    let info2 = info(2, 1, 1, SchemaVersion::V7_4);
    call.reply(&info2);
    let got = watch.changed().await.unwrap();
    assert_eq!(got.id(), info2.id);
    assert_eq!(got.sequence, 2);
    assert_eq!(monitor.current().unwrap().id(), info2.id);

    // Also when the coordinator re-sends the same info (after CLIENT_REGISTER_INTERVAL).
    let call = coord.next_call().await;
    assert_eq!(call.request.known_client_info_id, info2.id);
    call.reply(&info2);
    let got = monitor.changed_since(&got).await.unwrap();
    assert_eq!((got.id(), got.sequence), (info2.id, 3));

    let status = monitor.status();
    assert!(status.has_connected);
    assert_eq!(status.failures, 0);
    assert_eq!(status.generation, 1);
    assert_eq!(status.last_error, None);
    assert_eq!(status.coordinator.map(|c| c.addr), Some(sock(1, 4500)));

    // Shutdown: the next long poll is outstanding; it is cancelled, the coordinator released,
    // and waiters are told.
    let _outstanding = coord.next_call().await;
    monitor.shutdown().await;
    assert!(monitor.is_closed());
    assert_eq!(monitor.changed_after(3).await, Err(MonitorClosed));
    // Infos already published are still returned.
    assert_eq!(monitor.changed_after(2).await.unwrap().sequence, 3);
    // The watch still gets what it has not seen, then the end.
    assert_eq!(watch.changed().await.unwrap().sequence, 3);
    assert_eq!(watch.changed().await, Err(MonitorClosed));
    assert_eq!(
        monitor.current().unwrap().id(),
        info2.id,
        "the last info stays"
    );
    assert_eq!(references(&peers, &coord), 0);
    monitor.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn failover_between_coordinators() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let mut coords: Vec<FakeCoordinator> = (1..=3)
        .map(|i| FakeCoordinator::start(&net, sock(i, 4500), ProtocolVersion::V7_4))
        .collect();
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(
        &rt,
        &peers,
        ConnectionRecord::memory(cs("test:abc@10.0.0.1:4500,10.0.0.2:4500,10.0.0.3:4500")),
    );
    // The first one asked cannot reach the cluster: the next one is asked.
    let (a, call) = next_any(&mut coords).await;
    for c in &coords {
        assert!(
            references(&peers, c) >= 1,
            "every coordinator is referenced"
        );
    }
    call.reply_error(FAILED_TO_PROGRESS);
    let (b, call) = next_any(&mut coords).await;
    assert_ne!(a, b);
    let info1 = info(1, 1, 1, SchemaVersion::V7_4);
    call.reply(&info1);
    let got = monitor.ready().await.unwrap();
    assert_eq!(got.coordinator.addr, coords[b].addr);
    let status = monitor.status();
    assert_eq!(status.failures, 1);
    assert_eq!(
        status.last_error,
        Some(MonitorError::Request {
            coordinator: got.connection_string.coordinators()[a].resolved().unwrap(),
            error: RequestError::Fdb(FdbError::new(FAILED_TO_PROGRESS)),
        })
    );

    // The long poll stays with the coordinator that answered.
    let (i, _call) = next_any(&mut coords).await;
    assert_eq!(i, b);
    // It goes down while the request is outstanding: the next coordinator is asked.
    coords[b].stop();
    let (c, call) = next_any(&mut coords).await;
    assert_ne!(c, b);
    assert_eq!(call.request.known_client_info_id, info1.id);
    let status = monitor.status();
    assert_eq!(status.failures, 2);
    assert!(
        matches!(
            &status.last_error,
            Some(MonitorError::Request { coordinator, error: RequestError::MaybeDelivered(_) })
                if coordinator.addr == coords[b].addr
        ),
        "{status:?}"
    );
    let info2 = info(2, 1, 1, SchemaVersion::V7_4);
    call.reply(&info2);
    let got = monitor.changed_after(1).await.unwrap();
    assert_eq!(got.id(), info2.id);
    assert_eq!(got.coordinator.addr, coords[c].addr);

    // A coordinator that refuses connections is skipped too: fail the others until the
    // rotation has come past the stopped one.
    let (i, mut call) = next_any(&mut coords).await;
    assert_eq!(i, c);
    let mut d;
    loop {
        call.reply_error(FAILED_TO_PROGRESS);
        (d, call) = next_any(&mut coords).await;
        assert_ne!(d, b, "the stopped coordinator never answers");
        if matches!(
            &monitor.status().last_error,
            Some(MonitorError::Request { coordinator, error: RequestError::NotSent(_) })
                if coordinator.addr == coords[b].addr
        ) {
            break;
        }
    }
    call.reply(&info(3, 1, 1, SchemaVersion::V7_4));
    assert_eq!(
        monitor.changed_after(2).await.unwrap().coordinator.addr,
        coords[d].addr
    );
    assert!(
        net.connects(coords[b].addr) >= 2,
        "b was dialed again and refused"
    );
    monitor.shutdown().await;
}

/// Once every coordinator has failed in turn the monitor waits
/// `COORDINATOR_RECONNECTION_DELAY` before asking again, and only then.
#[tokio::test(start_paused = true)]
async fn reconnection_delay_after_all_failed() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let mut coords: Vec<FakeCoordinator> = (1..=2)
        .map(|i| FakeCoordinator::start(&net, sock(i, 4500), ProtocolVersion::V7_4))
        .collect();
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(
        &rt,
        &peers,
        ConnectionRecord::memory(cs("test:abc@10.0.0.1:4500,10.0.0.2:4500")),
    );
    let mut times = Vec::new();
    let mut order = Vec::new();
    for _ in 0..5 {
        let (i, call) = next_any(&mut coords).await;
        times.push(tokio::time::Instant::now());
        order.push(i);
        call.reply_error(FAILED_TO_PROGRESS);
    }
    // Rotation: a, b, a, b, a.
    assert_ne!(order[0], order[1]);
    assert_eq!(order[0], order[2]);
    assert_eq!(order[1], order[3]);
    // No delay between the two of a round, 1 s (plus polling slack) between rounds.
    let gap = |i: usize| times[i + 1] - times[i];
    assert!(gap(0) < Duration::from_millis(100), "{:?}", gap(0));
    assert!(gap(1) >= Duration::from_secs(1), "{:?}", gap(1));
    assert!(gap(1) < Duration::from_millis(1100), "{:?}", gap(1));
    assert!(gap(2) < Duration::from_millis(100), "{:?}", gap(2));
    assert!(gap(3) >= Duration::from_secs(1), "{:?}", gap(3));
    until("the last failure", || monitor.status().failures == 5).await;
    assert!(monitor.current().is_none());
    monitor.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn forward_moves_to_new_coordinators_and_rewrites_the_cluster_file() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fdb.cluster");
    std::fs::write(&path, "# my cluster\ntest:abc@10.0.0.1:4500\n").unwrap();
    let mut old = FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4);
    let mut new = FakeCoordinator::start(&net, sock(9, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(&rt, &peers, ConnectionRecord::open(&path).unwrap());

    let call = old.next_call().await;
    call.reply(&forward(1, "test:moved@10.0.0.9:4500"));

    // The new coordinators are asked; the forward itself is not published, and the file is
    // not rewritten before one of them answers.
    let call = new.next_call().await;
    assert_eq!(call.request.cluster_key, b"test:moved");
    assert_eq!(call.request.coordinators, vec![new.address()]);
    assert_eq!(call.request.known_client_info_id, Uid::default());
    assert!(monitor.current().is_none());
    let status = monitor.status();
    assert_eq!(status.generation, 2);
    assert_eq!(status.connection_string, cs("test:moved@10.0.0.9:4500"));
    assert_eq!(
        status.record_connection_string,
        cs("test:abc@10.0.0.1:4500")
    );
    assert_eq!(monitor.connection_string(), cs("test:moved@10.0.0.9:4500"));
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "# my cluster\ntest:abc@10.0.0.1:4500\n"
    );
    assert_eq!(
        references(&peers, &old),
        0,
        "the old generation is released"
    );
    assert!(references(&peers, &new) >= 1);

    let info1 = info(1, 1, 1, SchemaVersion::V7_4);
    call.reply(&info1);
    let got = monitor.ready().await.unwrap();
    assert_eq!(got.id(), info1.id);
    assert_eq!(got.connection_string, cs("test:moved@10.0.0.9:4500"));
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        format!("{CLUSTER_FILE_HEADER}test:moved@10.0.0.9:4500\n")
    );
    let status = monitor.status();
    assert_eq!(
        status.record_connection_string,
        cs("test:moved@10.0.0.9:4500")
    );
    assert_eq!(status.persist_error, None);
    assert!(status.has_connected);

    // The long poll continues with the new coordinator only, and the file is up to date.
    let call = new.next_call().await;
    assert_eq!(call.request.known_client_info_id, info1.id);
    assert!(call.request.issues.is_empty());
    assert!(old.try_call().is_none());
    monitor.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn forward_in_memory_and_invalid_forward() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let mut old = FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4);
    let mut new = FakeCoordinator::start(&net, sock(9, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(
        &rt,
        &peers,
        ConnectionRecord::memory(cs("test:abc@10.0.0.1:4500")),
    );

    // Not a connection string: counted as a failure of that coordinator.
    let call = old.next_call().await;
    call.reply(&forward(1, "nonsense"));
    let call = old.next_call().await;
    assert!(
        matches!(
            monitor.status().last_error,
            Some(MonitorError::InvalidForward { ref forward, .. }) if forward == b"nonsense"
        ),
        "{:?}",
        monitor.status()
    );
    call.reply(&forward(2, "test:moved@10.0.0.9:4500"));
    let call = new.next_call().await;
    call.reply(&info(1, 1, 1, SchemaVersion::V7_4));
    monitor.ready().await.unwrap();
    let status = monitor.status();
    assert_eq!(
        status.record_connection_string,
        cs("test:moved@10.0.0.9:4500")
    );
    assert_eq!(status.persist_error, None);
    monitor.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn cluster_file_changes_are_reported_then_adopted() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fdb.cluster");
    std::fs::write(&path, "test:abc@10.0.0.1:4500\n").unwrap();
    let mut old = FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4);
    let mut new = FakeCoordinator::start(&net, sock(2, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(&rt, &peers, ConnectionRecord::open(&path).unwrap());

    let call = old.next_call().await;
    assert!(call.request.issues.is_empty());
    call.reply(&info(1, 1, 1, SchemaVersion::V7_4));
    monitor.ready().await.unwrap();

    // Someone else rewrites the file (while the long poll is outstanding): the cluster is
    // told with the next request.
    let call = old.next_call().await;
    assert!(call.request.issues.is_empty());
    std::fs::write(&path, "test:abc@10.0.0.2:4500\n").unwrap();
    call.reply(&info(2, 1, 1, SchemaVersion::V7_4));
    monitor.changed_after(1).await.unwrap();
    let call = old.next_call().await;
    assert_eq!(
        call.request.issues,
        vec![b"incorrect_cluster_file_contents".to_vec()]
    );
    // As long as a coordinator answers, the file is not adopted.
    call.reply(&info(3, 1, 1, SchemaVersion::V7_4));
    monitor.changed_after(2).await.unwrap();
    let call = old.next_call().await;
    assert_eq!(
        call.request.issues,
        vec![b"incorrect_cluster_file_contents".to_vec()]
    );
    // Once every coordinator has failed, it is.
    call.reply_error(FAILED_TO_PROGRESS);
    let call = new.next_call().await;
    assert_eq!(call.request.coordinators, vec![new.address()]);
    assert!(call.request.issues.is_empty());
    assert_eq!(monitor.connection_string(), cs("test:abc@10.0.0.2:4500"));
    assert_eq!(monitor.status().generation, 2);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        format!("{CLUSTER_FILE_HEADER}test:abc@10.0.0.2:4500\n")
    );
    call.reply(&info(4, 1, 1, SchemaVersion::V7_4));
    assert_eq!(
        monitor.changed_after(3).await.unwrap().coordinator.addr,
        new.addr
    );
    monitor.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_record_to_persist_is_written_on_first_contact() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("new.cluster");
    let mut coord = FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let record = ConnectionRecord::file(&path, cs("test:abc@10.0.0.1:4500"));
    assert_eq!(record.path(), Some(path.as_path()));
    let monitor = start(&rt, &peers, record);

    let call = coord.next_call().await;
    assert!(
        call.request.issues.is_empty(),
        "not checked before it is written"
    );
    assert!(!path.exists());
    call.reply(&info(1, 1, 1, SchemaVersion::V7_4));
    monitor.ready().await.unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        format!("{CLUSTER_FILE_HEADER}test:abc@10.0.0.1:4500\n")
    );
    let call = coord.next_call().await;
    assert!(call.request.issues.is_empty());
    monitor.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn recovery_without_proxies_then_provisional_then_recovered() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let mut coord = FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(
        &rt,
        &peers,
        ConnectionRecord::memory(cs("test:abc@10.0.0.1:4500")),
    );
    let short = Duration::from_millis(100);

    // Recovering: an info with no proxies is published, but there is nothing to use.
    coord
        .next_call()
        .await
        .reply(&info(1, 0, 0, SchemaVersion::V7_4));
    let got = monitor.ready().await.unwrap();
    assert!(got.grv_proxies.is_empty() && got.commit_proxies.is_empty());
    assert!(!got.has_proxies(true));
    assert!(got.available_grv_proxies(true).is_none());
    assert!(got.available_commit_proxies(true).is_none());
    assert!(
        tokio::time::timeout(short, monitor.proxies(true))
            .await
            .is_err()
    );

    // Provisional proxies: only for transactions that accept them.
    let mut provisional = info(2, 1, 2, SchemaVersion::V7_4);
    provisional.grv_proxies[0].provisional = true;
    for cp in &mut provisional.commit_proxies {
        cp.provisional = true;
    }
    coord.next_call().await.reply(&provisional);
    let got = monitor.changed_since(&got).await.unwrap();
    assert!(got.is_provisional());
    assert!(got.available_grv_proxies(false).is_none());
    assert_eq!(got.available_grv_proxies(true).unwrap().len(), 1);
    assert_eq!(got.available_commit_proxies(true).unwrap().len(), 2);
    assert_eq!(monitor.proxies(true).await.unwrap().id(), provisional.id);
    assert!(
        tokio::time::timeout(short, monitor.proxies(false))
            .await
            .is_err()
    );

    // Recovered.
    let waiter = async { monitor.proxies(false).await.unwrap() };
    let reply = async {
        coord
            .next_call()
            .await
            .reply(&info(3, 1, 1, SchemaVersion::V7_4));
    };
    let (got, ()) = tokio::join!(waiter, reply);
    assert_eq!(got.id(), Uid::new(0xdb, 3));
    assert!(!got.is_provisional());
    monitor.shutdown().await;
}

/// The cluster's version is learned from the coordinator's ConnectPacket, whichever version
/// the client prefers, and the reply is decoded, and the endpoints derived, with it.
#[tokio::test(start_paused = true)]
async fn version_detection_selects_the_schema_set() {
    for (server, preferred) in [
        (ProtocolVersion::V8_0, ProtocolVersion::V7_4),
        (ProtocolVersion::V7_4, ProtocolVersion::V8_0),
    ] {
        let net = Net::new();
        let rt = MemRuntime::new(&net);
        let mut coord = FakeCoordinator::start(&net, sock(1, 4500), server);
        let peers = manager(&rt, vec![preferred, server]);
        let monitor = start(
            &rt,
            &peers,
            ConnectionRecord::memory(cs("test:abc@10.0.0.1:4500")),
        );
        let schema = SchemaVersion::for_protocol_version(server).unwrap();

        let call = coord.next_call().await;
        assert_eq!(call.schema, schema);
        assert_eq!(coord.announced(), vec![preferred, server]);
        let sent: ClientDbInfo = info(1, 1, 2, schema);
        call.reply(&sent);
        let got = monitor.ready().await.unwrap();
        assert_eq!(got.schema, schema, "{server}");
        assert!(got.protocol_version.is_compatible(server));
        assert_eq!(got.client_info, sent, "{server}");
        assert_eq!(monitor.schema_version(), Some(schema));
        let indices = schema.commit_proxy_streams();
        for cp in &got.commit_proxies {
            assert_eq!(cp.proxy.schema, schema);
            assert_eq!(
                cp.streams.expire_idempotency_id,
                cp.proxy.base.adjusted(indices.expire_idempotency_id)
            );
            assert_eq!(
                cp.streams.get_tenant_id.is_some(),
                schema == SchemaVersion::V7_4
            );
        }
        let expire = if schema == SchemaVersion::V7_4 { 10 } else { 9 };
        assert_eq!(indices.expire_idempotency_id, expire);
        monitor.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn hostname_coordinators_are_resolved() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    net.set_host("coord.test", vec![sock(5, 4500).ip()]);
    let mut coord = FakeCoordinator::start(&net, sock(5, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(
        &rt,
        &peers,
        ConnectionRecord::memory(cs("test:abc@coord.test:4500")),
    );

    let call = coord.next_call().await;
    assert!(call.request.coordinators.is_empty());
    assert_eq!(
        call.request.hostnames,
        vec![Hostname {
            host: b"coord.test".to_vec(),
            service: b"4500".to_vec(),
            is_tls: false,
        }]
    );
    call.reply(&info(1, 1, 1, SchemaVersion::V7_4));
    let got = monitor.ready().await.unwrap();
    assert_eq!(got.coordinator.addr, sock(5, 4500));
    assert!(got.coordinator.from_hostname);
    assert!(references(&peers, &coord) >= 1);
    // Resolved again for every request.
    let _call = coord.next_call().await;
    assert_eq!(net.resolves(), vec!["coord.test", "coord.test"]);
    monitor.shutdown().await;
    assert_eq!(references(&peers, &coord), 0);
}

#[tokio::test(start_paused = true)]
async fn unresolvable_hostname_is_skipped() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let mut coord = FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(
        &rt,
        &peers,
        ConnectionRecord::memory(cs("test:abc@nowhere.test:4500,10.0.0.1:4500")),
    );

    // Whichever is first, the address coordinator ends up asked; make it fail so that the
    // hostname is tried, then answer.
    let call = coord.next_call().await;
    call.reply_error(FAILED_TO_PROGRESS);
    until("the hostname to fail", || {
        matches!(
            monitor.status().last_error,
            Some(MonitorError::Resolve { ref host, port: 4500, .. }) if host == "nowhere.test"
        )
    })
    .await;
    let call = coord.next_call().await;
    call.reply(&info(1, 1, 1, SchemaVersion::V7_4));
    assert_eq!(
        monitor.ready().await.unwrap().coordinator.addr,
        sock(1, 4500)
    );
    monitor.shutdown().await;
}

/// Dropping the monitor stops it as shutdown does, without waiting.
#[tokio::test(start_paused = true)]
async fn drop_stops_the_monitor() {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let mut coord = FakeCoordinator::start(&net, sock(1, 4500), ProtocolVersion::V7_4);
    let peers = manager(&rt, vec![ProtocolVersion::V7_4]);
    let monitor = start(
        &rt,
        &peers,
        ConnectionRecord::memory(cs("test:abc@10.0.0.1:4500")),
    );
    let mut watch = monitor.watch();
    let _call = coord.next_call().await;
    assert!(references(&peers, &coord) >= 1);
    drop(monitor);
    assert_eq!(watch.changed().await, Err(MonitorClosed));
    until("the coordinator to be released", || {
        references(&peers, &coord) == 0
    })
    .await;
}
