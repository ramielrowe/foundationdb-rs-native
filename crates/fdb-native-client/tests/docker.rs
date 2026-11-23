//! The cluster monitor against a real, disposable FDB server started by
//! `fdb-native-testkit` (image from `FDB_IMAGE`, default 7.4.10), over the tokio adapter: the
//! `ClientDBInfo` with its proxies, the derived endpoints, one `GetReadVersion` to a GRV
//! proxy, and an update through the long poll after a recovery. Run with
//! `cargo nextest run --run-ignored only`.

use std::collections::BTreeMap;
use std::time::Duration;

use fdb_native_client::{ClusterInfo, ClusterMonitor, ClusterMonitorConfig, ConnectionRecord};
use fdb_native_schema::domain::{GetReadVersion, INVALID_VERSION, ReadVersion};
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_testkit::FdbContainer;
use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig};
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_wire::EncodeError;

/// The schema set the image's release must give.
fn expected_schema(image: &str) -> Option<SchemaVersion> {
    let tag = image.rsplit(':').next()?;
    if tag.starts_with("7.4") {
        Some(SchemaVersion::V7_4)
    } else if tag.starts_with("8.0") {
        Some(SchemaVersion::V8_0)
    } else {
        None
    }
}

/// One `GetReadVersionRequest` to the first GRV proxy, encoded for the connection's version.
async fn read_version(peers: &PeerManager<TokioRuntime>, info: &ClusterInfo) -> ReadVersion {
    let endpoint = info.grv_proxies[0].streams.get_consistent_read_version;
    let request = GetReadVersion {
        transaction_count: 1,
        flags: GetReadVersion::PRIORITY_DEFAULT,
        tags: BTreeMap::new(),
        debug_id: None,
        span_context: Default::default(),
        max_version: INVALID_VERSION,
        max_grv_queue_delay_ms: None,
    };
    let peer = peers.peer_for_endpoint(&endpoint);
    let version = peer.connect().await.expect("GRV proxy connection");
    let schema = SchemaVersion::for_protocol_version(version).expect("supported version");
    let payload = tokio::time::timeout(
        Duration::from_secs(10),
        peer.request(endpoint.token, move |reply, version, out| {
            let schema = SchemaVersion::for_protocol_version(version)
                .ok_or(EncodeError::Invalid("unsupported version"))?;
            request.encode_request_into(schema, reply, out)
        }),
    )
    .await
    .expect("GRV reply within 10 s")
    .expect("GRV reply");
    GetReadVersion::decode_reply(schema, &payload)
        .expect("decodable reply")
        .expect("a read version, not an error")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_monitor_gets_proxies_and_a_read_version() {
    let fdb = tokio::task::spawn_blocking(FdbContainer::start)
        .await
        .unwrap()
        .expect("container");
    let image = fdb.image().to_owned();
    println!("image {image}, cluster {}", fdb.cluster_string());

    let rt = TokioRuntime::new();
    let peers = PeerManager::new(rt.clone(), PeerManagerConfig::default(), DiscardEvents);
    let record = ConnectionRecord::open(fdb.cluster_file()).expect("cluster file");
    let monitor = ClusterMonitor::start(
        rt.clone(),
        peers.clone(),
        record,
        ClusterMonitorConfig::default(),
    );

    let info = tokio::time::timeout(Duration::from_secs(30), monitor.proxies(false))
        .await
        .expect("proxies within 30 s")
        .expect("monitor running");
    let cluster_version = peers.cluster_version().expect("negotiated");
    println!(
        "ClientDBInfo {:?} from {} on {cluster_version} (schema {}): {} GRV proxies, {} commit \
         proxies, cluster id {:?}",
        info.id(),
        info.coordinator,
        info.schema,
        info.grv_proxies.len(),
        info.commit_proxies.len(),
        info.client_info.cluster_id,
    );
    assert_eq!(
        Some(info.schema),
        SchemaVersion::for_protocol_version(cluster_version)
    );
    if let Some(expected) = expected_schema(&image) {
        assert_eq!(info.schema, expected, "{image}");
    }
    assert!(!info.grv_proxies.is_empty());
    assert!(!info.commit_proxies.is_empty());
    assert!(!info.is_provisional());
    assert_eq!(info.client_info.forward, None);
    assert_eq!(info.coordinator.addr, fdb.address());
    for grv in &info.grv_proxies {
        println!(
            "  GRV proxy {:?} at {}: getConsistentReadVersion {:?}",
            grv.proxy.id(),
            grv.streams
                .get_consistent_read_version
                .addresses
                .address
                .socket_addr(),
            grv.streams.get_consistent_read_version.token,
        );
    }
    for cp in &info.commit_proxies {
        assert_eq!(cp.proxy.schema, info.schema);
        let indices = info.schema.commit_proxy_streams();
        assert_eq!(
            cp.streams.get_key_servers_locations,
            cp.proxy.base.adjusted(indices.get_key_servers_locations)
        );
        println!(
            "  commit proxy {:?} at {}: commit {:?}, getKeyServersLocations {:?}, \
             expireIdempotencyId (index {}) {:?}",
            cp.proxy.id(),
            cp.streams.commit.addresses.address.socket_addr(),
            cp.streams.commit.token,
            cp.streams.get_key_servers_locations.token,
            indices.expire_idempotency_id,
            cp.streams.expire_idempotency_id.token,
        );
    }

    // A preview of the read path: the GRV proxy answers on the derived endpoint.
    let rv = read_version(&peers, &info).await;
    println!(
        "read version {} (locked {}, proxy {:?})",
        rv.version, rv.locked, rv.proxy_id
    );
    assert!(rv.version > 0);

    // A recovery (a configuration change) gives a new ClientDBInfo, through the long poll.
    let first = info.id();
    let mut watch = monitor.watch();
    watch.mark_seen();
    let fdb = tokio::task::spawn_blocking(move || {
        let out = fdb.fdbcli("configure commit_proxies=1 grv_proxies=1 resolvers=1");
        (fdb, out)
    })
    .await
    .unwrap();
    let (fdb, out) = fdb;
    println!("fdbcli: {out:?}");
    let next = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let info = watch.changed().await.expect("monitor running");
            if info.id() != first && info.has_proxies(false) {
                return info;
            }
        }
    })
    .await;
    match next {
        Ok(next) => {
            println!(
                "after the recovery: ClientDBInfo {:?}, {} GRV / {} commit proxies",
                next.id(),
                next.grv_proxies.len(),
                next.commit_proxies.len()
            );
            assert_ne!(next.id(), info.id());
            let rv2 = read_version(&peers, &next).await;
            println!("read version after the recovery {}", rv2.version);
            assert!(rv2.version > rv.version);
        }
        Err(_) => panic!("no new ClientDBInfo within 60 s of a configuration change"),
    }
    let status = monitor.status();
    println!("monitor status: {status:?}");
    assert!(status.has_connected);

    monitor.shutdown().await;
    peers.shutdown().await;
    tokio::task::spawn_blocking(move || drop(fdb))
        .await
        .unwrap();
}
