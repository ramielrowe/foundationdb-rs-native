//! End-to-end tests of the transport (peer manager, request/reply layer, failure monitor)
//! against a real FoundationDB server in a disposable `fdb-native-testkit` container, over the
//! tokio adapter. Each test starts its own container, reads its cluster file with
//! `fdb_native_client::cluster_file` and reaches the server at the coordinator address from it.
//!
//! The image under test is the testkit's default: `FDB_IMAGE`, else the pinned 7.4.10. CI runs
//! this binary once per image (7.4.10 and 8.0.1); the expected protocol version is derived
//! from the image tag, and cross-checked against what the server itself reports in a
//! `ProtocolInfoReply`. Run with
//! `FDB_IMAGE=foundationdb/foundationdb:8.0.1 cargo nextest run -p fdb-native-transport
//! --run-ignored only -E 'binary(integration_docker)'`.
//!
//! Only well-formed protocol messages are sent to the server. The tests that need the server
//! to misbehave (stalled, gone, restarted) do it from outside, through the testkit's
//! `pause`, `stop`, `start_stopped` and `restart` (exact container ID only).
//!
//! The test names start with `docker_`, which puts them in nextest's `docker` test group
//! (`.config/nextest.toml`) so that only a few containers run at once.

#![cfg(feature = "tokio")]

use std::collections::hash_map::RandomState;
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::net::Ipv4Addr;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fdb_native_client::cluster_file::read_cluster_file;
use fdb_native_testkit::FdbContainer;
use fdb_native_transport::ConnectionError;
use fdb_native_transport::connection::{TOKEN_STREAM_FLAG, well_known};
use fdb_native_transport::peer::{
    EndpointFailure, EndpointStatus, EventReceiver, Generation, PeerError, PeerEvent, PeerHandle,
    PeerManager, PeerManagerConfig, PeerPhase, ProtocolInfoReply, ProtocolInfoRequest,
    event_channel,
};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_wire::{EncodeError, NetworkAddress, ProtocolVersion, ReplyPromise, Uid};

/// How long a step that should be quick may take before the test gives up on it.
const STEP: Duration = Duration::from_secs(30);

// ---- helpers ---------------------------------------------------------------------------------

/// The protocol version a testkit image runs, from its tag (`...:7.4.10`, `...:8.0.1`).
fn version_of_image(image: &str) -> ProtocolVersion {
    let tag = image.rsplit(':').next().unwrap_or("");
    if tag.starts_with("7.4") {
        ProtocolVersion::V7_4
    } else if tag.starts_with("8.0") {
        ProtocolVersion::V8_0
    } else {
        panic!("FDB_IMAGE must be a 7.4.x or 8.0.x image, got {image:?}")
    }
}

/// The supported version that is not `right`.
fn other(right: ProtocolVersion) -> ProtocolVersion {
    if right == ProtocolVersion::V7_4 {
        ProtocolVersion::V8_0
    } else {
        ProtocolVersion::V7_4
    }
}

/// Fails the test if `future` takes longer than `limit`.
async fn within<T>(limit: Duration, what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(limit, future)
        .await
        .unwrap_or_else(|_| panic!("timed out after {limit:?}: {what}"))
}

/// A disposable single-node cluster, reached through its cluster file.
struct Cluster {
    fdb: Arc<FdbContainer>,
    /// The coordinator, from the cluster file.
    address: NetworkAddress,
    /// The protocol version of the image.
    version: ProtocolVersion,
}

impl Cluster {
    /// Starts a container (the testkit's default image: `FDB_IMAGE`) and takes the
    /// coordinator's address from its cluster file.
    async fn start() -> Cluster {
        let fdb = tokio::task::spawn_blocking(FdbContainer::start)
            .await
            .unwrap()
            .expect("container");
        let string = read_cluster_file(fdb.cluster_file()).expect("cluster file");
        let [coordinator] = string.coordinators() else {
            panic!("expected one coordinator in {string:?}");
        };
        let addr = coordinator.socket_addr().expect("a literal address");
        let resolved = coordinator.with_resolved(addr);
        assert_eq!(
            resolved.addr,
            fdb.address(),
            "the cluster file's coordinator"
        );
        assert!(!resolved.tls && !resolved.from_hostname);
        let version = version_of_image(fdb.image());
        eprintln!("{fdb}: coordinator {resolved}, expecting {version}");
        Cluster {
            address: NetworkAddress::new(
                resolved.addr.ip().into(),
                resolved.addr.port(),
                true,
                resolved.tls,
            ),
            fdb: Arc::new(fdb),
            version,
        }
    }

    /// The default configuration with the image's version preferred, so that a connection does
    /// not start with a version detection (which the first test covers).
    fn config(&self) -> PeerManagerConfig {
        let mut config = PeerManagerConfig::default();
        config.supported_versions = vec![self.version, other(self.version)];
        config
    }

    /// A manager over tokio with `config`, and its event stream.
    fn manager(&self, config: PeerManagerConfig) -> (PeerManager<TokioRuntime>, EventReceiver) {
        let (sink, events) = event_channel();
        (PeerManager::new(TokioRuntime::new(), config, sink), events)
    }

    /// Runs a blocking testkit call off the runtime's threads.
    async fn blocking<T: Send + 'static>(
        &self,
        what: &'static str,
        f: impl FnOnce(&FdbContainer) -> fdb_native_testkit::Result<T> + Send + 'static,
    ) -> T {
        let fdb = self.fdb.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || f(&fdb))
            .await
            .unwrap()
            .unwrap_or_else(|e| panic!("{what}: {e}"));
        eprintln!("{what}: {:?}", started.elapsed());
        result
    }

    /// Like [`blocking`](Self::blocking), for a call that stops or restarts the container:
    /// `None` if Docker started it again with another IP address, which makes the cluster file
    /// and this handle's address stale (Docker gives a starting container the lowest free
    /// address, which another container may have taken or released meanwhile). The test then
    /// starts over with a new container.
    async fn disturb(
        &self,
        what: &'static str,
        f: impl FnOnce(&FdbContainer) -> fdb_native_testkit::Result<()> + Send + 'static,
    ) -> Option<()> {
        let fdb = self.fdb.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || f(&fdb)).await.unwrap();
        match result {
            Ok(()) => {
                eprintln!("{what}: {:?}", started.elapsed());
                Some(())
            }
            Err(e @ fdb_native_testkit::Error::AddressChanged { .. }) => {
                eprintln!("{what}: {e}");
                None
            }
            Err(e) => panic!("{what}: {e}"),
        }
    }

    /// How many connections the server has from outside the container (our clients), read
    /// from the container's own TCP table.
    async fn server_connections(&self) -> usize {
        let id = self.fdb.id().to_owned();
        let own = self.fdb.address().ip();
        tokio::task::spawn_blocking(move || {
            let out = Command::new("docker")
                .args(["exec", &id, "cat", "/proc/net/tcp"])
                .output()
                .expect("docker exec");
            assert!(out.status.success(), "{out:?}");
            established_from_outside(&String::from_utf8_lossy(&out.stdout), own)
        })
        .await
        .unwrap()
    }

    /// Removes the container (blocks, so off the runtime's threads).
    async fn finish(self) {
        let fdb = self.fdb;
        tokio::task::spawn_blocking(move || drop(fdb))
            .await
            .unwrap();
    }
}

/// Counts the ESTABLISHED connections to the server's port 4500 whose remote end is not the
/// container itself (`/proc/net/tcp`: hex addresses in host byte order).
fn established_from_outside(table: &str, own: std::net::IpAddr) -> usize {
    table
        .lines()
        .skip(1)
        .filter(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let [_, local, remote, state, ..] = fields[..] else {
                return false;
            };
            let parse = |s: &str| -> Option<(Ipv4Addr, u16)> {
                let (ip, port) = s.split_once(':')?;
                let ip = u32::from_str_radix(ip, 16).ok()?;
                Some((
                    Ipv4Addr::from(ip.to_le_bytes()),
                    u16::from_str_radix(port, 16).ok()?,
                ))
            };
            let (Some((_, local_port)), Some((remote_ip, _))) = (parse(local), parse(remote))
            else {
                return false;
            };
            state == "01"
                && local_port == fdb_native_testkit::FDB_PORT
                && !remote_ip.is_loopback()
                && std::net::IpAddr::V4(remote_ip) != own
        })
        .count()
}

fn drain(events: &mut EventReceiver) -> Vec<PeerEvent> {
    std::iter::from_fn(|| events.try_next())
        .map(|record| record.event)
        .collect()
}

/// A random token with the stream flag set: it names no endpoint of the server's, which then
/// answers a frame for it with endpoint-not-found.
fn random_stream_token() -> Uid {
    let random = || RandomState::new().build_hasher().finish();
    Uid::new(random() | TOKEN_STREAM_FLAG, random())
}

/// A well-formed `ProtocolInfoRequest` to `endpoint`, the reply token being the one the peer
/// minted for it.
async fn protocol_info_request(peer: &PeerHandle, endpoint: Uid) -> Result<Vec<u8>, RequestError> {
    peer.request(
        endpoint,
        |reply, _version, out| -> Result<(), EncodeError> {
            let request = ProtocolInfoRequest {
                reply: ReplyPromise::new(reply),
            };
            out.extend_from_slice(&fdb_native_wire::encode(&request)?);
            Ok(())
        },
    )
    .await
}

/// Asks the server for its protocol version on the well-known endpoint every fdbserver process
/// serves (token 10, on both releases).
async fn protocol_info(peer: &PeerHandle) -> Result<ProtocolInfoReply, RequestError> {
    peer.request_as::<ProtocolInfoReply, _>(
        well_known::PROTOCOL_INFO,
        |reply, _version, out| -> Result<(), EncodeError> {
            let request = ProtocolInfoRequest {
                reply: ReplyPromise::new(reply),
            };
            out.extend_from_slice(&fdb_native_wire::encode(&request)?);
            Ok(())
        },
    )
    .await
}

/// Waits until `peer` is connected on a connection newer than `after`.
async fn reconnected(peer: &PeerHandle, after: Generation) -> Generation {
    within(
        STEP,
        "the peer to reconnect",
        peer.wait_for(|s| {
            (s.phase == PeerPhase::Connected && s.generation > after).then_some(s.generation)
        }),
    )
    .await
}

type Scenario<'a> = std::pin::Pin<Box<dyn Future<Output = Option<()>> + 'a>>;

/// Runs a scenario that stops or restarts the container, on a new container each time Docker
/// gives the stopped one another IP address (up to three times).
async fn retrying(scenario: for<'a> fn(&'a Cluster) -> Scenario<'a>) {
    for attempt in 1..=3 {
        let cluster = Cluster::start().await;
        let done = scenario(&cluster).await;
        cluster.finish().await;
        if done.is_some() {
            return;
        }
        eprintln!("attempt {attempt}: the container came back at another address, starting over");
    }
    panic!("Docker gave the restarted container another IP address three times");
}

// ---- tests -----------------------------------------------------------------------------------

/// Connect to the coordinator from the cluster file with the wrong version preferred: the
/// manager detects the server's version from its ConnectPacket, reconnects at once (no failure,
/// no backoff) and reports the image's version. A manager that prefers the right version first
/// connects once.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_cluster_file_connect_detects_version() {
    let cluster = Cluster::start().await;
    let right = cluster.version;

    let mut config = PeerManagerConfig::default();
    config.supported_versions = vec![other(right), right];
    let (mgr, mut events) = cluster.manager(config);
    assert_eq!(mgr.cluster_version(), None);
    let peer = mgr.peer(&cluster.address);

    let version = within(STEP, "connect", peer.connect())
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(version, right);
    assert_eq!(mgr.cluster_version(), Some(right));
    let status = peer.status();
    assert_eq!(status.phase, PeerPhase::Connected, "{status:?}");
    assert_eq!(status.version, Some(right));
    assert_eq!(status.known_version, Some(right));
    assert_eq!(
        status.generation,
        Generation(2),
        "wrong version, then right"
    );
    assert_eq!(status.failures, 0, "version detection is no failure");
    assert!(!peer.is_failed());

    let seen = drain(&mut events);
    assert!(
        matches!(
            &seen[..],
            [
                PeerEvent::Closed {
                    error: PeerError::Connection(ConnectionError::Incompatible { peer: said, .. }),
                    established: false,
                    ..
                },
                PeerEvent::Connected { version: v, peer: packet, .. }
            ] if said.without_flags() == right && *v == right && packet.connection_id == 1
        ),
        "{seen:?}"
    );

    // What the server itself says it speaks.
    let reply = protocol_info(&peer).await.unwrap();
    assert_eq!(reply.protocol_version().without_flags(), right);
    assert_eq!(peer.status().generation, Generation(2), "no reconnect");
    assert_eq!(
        cluster.server_connections().await,
        1,
        "one connection survived"
    );

    // The right version first: one connection, no detection.
    let mut config = PeerManagerConfig::default();
    config.supported_versions = vec![right, other(right)];
    let (mgr2, mut events2) = cluster.manager(config);
    let peer2 = mgr2.peer(&cluster.address);
    assert_eq!(within(STEP, "connect", peer2.connect()).await, Ok(right));
    assert_eq!(peer2.status().generation, Generation(1));
    let seen = drain(&mut events2);
    assert!(
        matches!(&seen[..], [PeerEvent::Connected { .. }]),
        "{seen:?}"
    );

    mgr.shutdown().await;
    mgr2.shutdown().await;
    cluster.finish().await;
}

/// A referenced peer held for eight seconds (several ping periods, and four times the ping
/// timeout) stays on its first connection: the server answers our pings and we answer its. No
/// reconnect, no failure, nothing but the one `Connected` event, and the connection still
/// carries requests.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_liveness_referenced_peer_survives_pings() {
    let cluster = Cluster::start().await;
    let (mgr, mut events) = cluster.manager(cluster.config());
    let peer = mgr.peer(&cluster.address);
    let guard = peer.reference();
    assert_eq!(
        within(STEP, "connect", peer.connect()).await,
        Ok(cluster.version)
    );
    let generation = peer.status().generation;
    assert_eq!(generation, Generation(1));
    let disconnects = mgr
        .failure_monitor()
        .address_status(&cluster.address)
        .disconnects;
    assert_eq!(cluster.server_connections().await, 1);

    for second in 1..=8 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let status = peer.status();
        assert_eq!(
            status.phase,
            PeerPhase::Connected,
            "after {second} s: {status:?}"
        );
        assert_eq!(
            status.generation, generation,
            "after {second} s: reconnected"
        );
        assert_eq!(status.failures, 0, "after {second} s: {status:?}");
        assert!(!peer.is_failed(), "after {second} s");
    }
    assert_eq!(status_references(&peer), 1);
    let monitor = mgr.failure_monitor().address_status(&cluster.address);
    assert!(!monitor.failed);
    assert_eq!(monitor.disconnects, disconnects, "no connection ended");
    let seen = drain(&mut events);
    assert!(
        matches!(&seen[..], [PeerEvent::Connected { generation: g, .. }] if *g == generation),
        "{seen:?}"
    );
    assert_eq!(
        cluster.server_connections().await,
        1,
        "the server kept it too"
    );

    let reply = protocol_info(&peer).await.unwrap();
    assert_eq!(reply.protocol_version().without_flags(), cluster.version);
    assert_eq!(peer.status().generation, generation);

    drop(guard);
    mgr.shutdown().await;
    cluster.finish().await;
}

fn status_references(peer: &PeerHandle) -> usize {
    peer.status().references
}

/// A well-formed request to a random stream token the server does not have is answered with
/// endpoint-not-found: the request fails as maybe delivered (it was pending when the notice
/// came), the failure monitor marks the endpoint (and not the address), later requests are
/// refused without being sent, and the connection is unaffected.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_endpoint_not_found_for_random_token() {
    let cluster = Cluster::start().await;
    let (mgr, mut events) = cluster.manager(cluster.config());
    let peer = mgr.peer(&cluster.address);
    within(STEP, "connect", peer.connect()).await.unwrap();
    let generation = peer.status().generation;
    let monitor = mgr.failure_monitor();
    let token = random_stream_token();
    assert_eq!(
        monitor.endpoint_status(&cluster.address, token),
        EndpointStatus::Available
    );
    drain(&mut events);

    let first = within(STEP, "the request", protocol_info_request(&peer, token)).await;
    assert_eq!(
        first,
        Err(RequestError::EndpointNotFound {
            token,
            maybe_delivered: true
        })
    );
    let error = first.unwrap_err();
    assert!(error.maybe_delivered() && !error.certainly_not_delivered() && !error.should_retry());

    // The failure monitor knows: the endpoint, not the address.
    assert_eq!(
        monitor.endpoint_failure(&cluster.address, token),
        Some(EndpointFailure::NotFound)
    );
    assert_eq!(
        monitor.endpoint_status(&cluster.address, token),
        EndpointStatus::EndpointFailed(EndpointFailure::NotFound)
    );
    assert!(!peer.is_failed());
    assert!(!monitor.is_failed(&cluster.address));
    let seen = drain(&mut events);
    assert_eq!(
        seen,
        vec![PeerEvent::EndpointNotFound { token }],
        "reported once"
    );

    // Known gone: refused without being sent, which is safe to resend.
    let second = protocol_info_request(&peer, token).await.unwrap_err();
    assert_eq!(
        second,
        RequestError::EndpointNotFound {
            token,
            maybe_delivered: false
        }
    );
    assert!(second.certainly_not_delivered() && !second.maybe_delivered());
    assert_eq!(drain(&mut events), vec![], "nothing went to the server");

    // The connection is fine, and other endpoints still work.
    let reply = protocol_info(&peer).await.unwrap();
    assert_eq!(reply.protocol_version().without_flags(), cluster.version);
    assert_eq!(peer.status().generation, generation);
    assert_eq!(peer.status().failures, 0);

    // Forgotten, the endpoint is tried again (and is still gone); several at once all fail with
    // endpoint-not-found, those pending when the notice came as maybe delivered.
    assert!(monitor.forget_endpoint(&cluster.address, token));
    let mut requests = Vec::new();
    for _ in 0..10 {
        let peer = peer.clone();
        requests.push(tokio::spawn(async move {
            protocol_info_request(&peer, token).await
        }));
    }
    let mut maybe = 0;
    for request in requests {
        match request.await.unwrap() {
            Err(RequestError::EndpointNotFound {
                token: t,
                maybe_delivered,
            }) => {
                assert_eq!(t, token);
                maybe += usize::from(maybe_delivered);
            }
            other => panic!("expected endpoint-not-found, got {other:?}"),
        }
    }
    assert!(maybe >= 1, "the first one was pending when the notice came");
    assert_eq!(
        drain(&mut events),
        vec![PeerEvent::EndpointNotFound { token }]
    );
    assert_eq!(peer.status().generation, generation);

    mgr.shutdown().await;
    cluster.finish().await;
}

/// A real RPC on the well-known `ProtocolInfoRequest` endpoint, then two hundred of them at
/// once: all succeed, each with its own reply token, on the one connection.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_protocol_info_rpc_many_concurrent() {
    const COUNT: usize = 200;
    let cluster = Cluster::start().await;
    let (mgr, mut events) = cluster.manager(cluster.config());
    let peer = mgr.peer(&cluster.address);

    // The first request connects by itself.
    let reply = within(STEP, "the first request", protocol_info(&peer))
        .await
        .unwrap();
    assert_eq!(reply.protocol_version().without_flags(), cluster.version);
    let generation = peer.status().generation;

    let started = Instant::now();
    let mut requests = Vec::new();
    for _ in 0..COUNT {
        let peer = peer.clone();
        requests.push(tokio::spawn(async move { protocol_info(&peer).await }));
    }
    for request in requests {
        let reply = within(STEP, "a request", request)
            .await
            .unwrap()
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(reply.protocol_version().without_flags(), cluster.version);
    }
    eprintln!("{COUNT} concurrent requests: {:?}", started.elapsed());
    assert_eq!(peer.status().generation, generation, "one connection");
    assert_eq!(peer.status().failures, 0);
    assert_eq!(peer.queued_commands(), 0);
    assert_eq!(cluster.server_connections().await, 1);
    let seen = drain(&mut events);
    assert!(
        matches!(&seen[..], [PeerEvent::Connected { .. }]),
        "nothing but the connection: {seen:?}"
    );

    mgr.shutdown().await;
    cluster.finish().await;
}

/// A server that stalls (frozen: sockets open, nothing answered, no pings) fails the request in
/// flight as maybe delivered once the ping times out, and a request that waits for the
/// connection as not sent. The address is marked failed after `FAILURE_DETECTION_DELAY` of
/// failures. Once the server thaws, the referenced peer probes it, reconnects, and requests
/// succeed again on a new connection.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_stalled_server_fails_in_flight_then_recovers() {
    let cluster = Cluster::start().await;
    let (mgr, mut events) = cluster.manager(cluster.config());
    let peer = mgr.peer(&cluster.address);
    let guard = peer.reference();
    within(STEP, "connect", peer.connect()).await.unwrap();
    let first = peer.status().generation;
    protocol_info(&peer).await.unwrap();
    drain(&mut events);

    cluster.blocking("pause", |f| f.pause()).await;
    let in_flight = {
        let peer = peer.clone();
        tokio::spawn(async move { protocol_info(&peer).await })
    };
    let error = within(STEP, "the in-flight request", in_flight)
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(
            error,
            RequestError::MaybeDelivered(Some(ConnectionError::PingTimeout(_)))
        ),
        "{error:?}"
    );
    assert!(error.maybe_delivered() && !error.certainly_not_delivered() && !error.should_retry());
    assert_eq!(error.fdb_code(), 1030, "request_maybe_delivered");

    // The failures go on, and past the detection delay the address is failed.
    let failed = within(
        STEP,
        "the address to be marked failed",
        mgr.failure_monitor()
            .wait_address_failed(&cluster.address, true),
    )
    .await;
    assert!(failed.failed && peer.is_failed());
    // The attempts that follow the ping timeout fail too (the frozen server never sends its
    // ConnectPacket).
    within(
        STEP,
        "the reconnect attempts to fail",
        peer.wait_for(|s| (s.failures >= 2).then_some(())),
    )
    .await;

    // A request that has to wait for a connection that does not come is not sent.
    let waiting = within(STEP, "the waiting request", protocol_info(&peer))
        .await
        .unwrap_err();
    assert!(matches!(waiting, RequestError::NotSent(_)), "{waiting:?}");
    assert!(waiting.certainly_not_delivered() && waiting.should_retry());

    let events_while_down = drain(&mut events);
    assert!(
        matches!(
            events_while_down.first(),
            Some(PeerEvent::Closed {
                generation, established: true,
                error: PeerError::Connection(ConnectionError::PingTimeout(_)), ..
            }) if *generation == first
        ),
        "{events_while_down:?}"
    );

    // Thawed: the probe (every second, the peer is referenced) finds it, and it is available.
    cluster.blocking("unpause", |f| f.unpause()).await;
    let second = reconnected(&peer, first).await;
    within(
        STEP,
        "the address to be available",
        mgr.failure_monitor()
            .wait_address_failed(&cluster.address, false),
    )
    .await;
    assert!(!peer.is_failed());
    let reply = within(STEP, "a request after recovery", protocol_info(&peer))
        .await
        .unwrap();
    assert_eq!(reply.protocol_version().without_flags(), cluster.version);
    assert_eq!(peer.status().generation, second);
    assert_eq!(peer.version(), Some(cluster.version));
    assert_eq!(
        cluster.server_connections().await,
        1,
        "old connections are gone"
    );

    drop(guard);
    mgr.shutdown().await;
    cluster.finish().await;
}

/// The server process is killed and restarted under a stream of requests. Every request that
/// fails is classified: sent on the dead connection (maybe delivered) or not sent. Nothing else.
/// The referenced peer reconnects (retrying with backoff while the server is down) and later
/// requests succeed on the new connection.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_server_restart_fails_in_flight_then_reconnects() {
    retrying(|c| Box::pin(server_restart(c))).await;
}

async fn server_restart(cluster: &Cluster) -> Option<()> {
    let (mgr, mut events) = cluster.manager(cluster.config());
    let peer = mgr.peer(&cluster.address);
    let guard = peer.reference();
    within(STEP, "connect", peer.connect()).await.unwrap();
    let first = peer.status().generation;
    drain(&mut events);

    let stop = Arc::new(AtomicBool::new(false));
    let ok = Arc::new(AtomicUsize::new(0));
    let maybe = Arc::new(AtomicUsize::new(0));
    let not_sent = Arc::new(AtomicUsize::new(0));
    let unexpected = Arc::new(Mutex::new(Vec::<RequestError>::new()));
    let workers: Vec<_> = (0..16)
        .map(|_| {
            let (peer, stop, ok, maybe, not_sent, unexpected) = (
                peer.clone(),
                stop.clone(),
                ok.clone(),
                maybe.clone(),
                not_sent.clone(),
                unexpected.clone(),
            );
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    match protocol_info(&peer).await {
                        Ok(_) => {
                            ok.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(RequestError::MaybeDelivered(_)) => {
                            maybe.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(RequestError::NotSent(_)) => {
                            not_sent.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        Err(other) => unexpected.lock().unwrap().push(other),
                    }
                }
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        ok.load(Ordering::Relaxed) > 0,
        "requests flow before the restart"
    );
    let restarted = cluster.disturb("restart", |f| f.restart()).await;
    if restarted.is_none() {
        stop.store(true, Ordering::Relaxed);
        for worker in workers {
            let _ = worker.await;
        }
        return None;
    }
    let second = reconnected(&peer, first).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    for worker in workers {
        within(STEP, "a worker", worker).await.unwrap();
    }
    eprintln!(
        "ok {} maybe delivered {} not sent {}",
        ok.load(Ordering::Relaxed),
        maybe.load(Ordering::Relaxed),
        not_sent.load(Ordering::Relaxed)
    );
    assert_eq!(*unexpected.lock().unwrap(), vec![]);
    assert!(
        maybe.load(Ordering::Relaxed) >= 1,
        "requests were in flight when the connection was reset"
    );

    // The old connection ended with a failure (not a clean close), and the peer is healthy.
    let seen = drain(&mut events);
    assert!(
        seen.iter().any(|e| matches!(
            e,
            PeerEvent::Closed { generation, established: true, error, .. }
                if *generation == first && !error.is_clean()
        )),
        "{seen:?}"
    );
    assert!(
        seen.iter()
            .any(|e| matches!(e, PeerEvent::Connected { generation, .. } if *generation == second)),
        "{seen:?}"
    );
    assert!(peer.status().failures >= 1);
    assert!(!peer.is_failed());
    let reply = within(STEP, "a request after the restart", protocol_info(&peer))
        .await
        .unwrap();
    assert_eq!(reply.protocol_version().without_flags(), cluster.version);
    assert_eq!(cluster.server_connections().await, 1);

    drop(guard);
    mgr.shutdown().await;
    Some(())
}

/// A peer whose references and handles are all dropped has its idle connection closed cleanly
/// (no failure), is removed from the manager (`Removed`), and is never dialed again; asking for
/// the address again makes a new peer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_unreferenced_peer_is_removed() {
    let cluster = Cluster::start().await;
    let (mgr, mut events) = cluster.manager(cluster.config());
    let peer = mgr.peer(&cluster.address);
    let id = peer.id();
    let guard = peer.reference();
    within(STEP, "connect", peer.connect()).await.unwrap();
    assert_eq!(cluster.server_connections().await, 1);
    assert_eq!(mgr.peer_count(), 1);
    drain(&mut events);

    // The last reference goes: the idle connection is closed, cleanly, after the delay. The
    // handle keeps the peer.
    drop(guard);
    within(
        STEP,
        "the unreferenced connection to close",
        peer.wait_for(|s| (s.phase == PeerPhase::Idle).then_some(())),
    )
    .await;
    let status = peer.status();
    assert_eq!(
        status.failures, 0,
        "a clean close is no failure: {status:?}"
    );
    assert_eq!(status.references, 0);
    assert_eq!(mgr.peer_count(), 1, "a handle still exists");
    assert_eq!(
        cluster.server_connections().await,
        0,
        "the socket is closed"
    );
    assert!(!peer.is_failed());

    // The last handle goes: the peer is removed.
    drop(peer);
    let removed = within(STEP, "the peer to be removed", async {
        loop {
            let record = events.next().await.expect("events");
            assert_eq!(record.peer, id);
            if record.event == PeerEvent::Removed {
                return record;
            }
        }
    })
    .await;
    assert_eq!(removed.address, cluster.address);
    assert_eq!(mgr.peer_count(), 0);

    // Nothing dials it again.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(drain(&mut events), vec![], "no event after Removed");
    assert_eq!(
        cluster.server_connections().await,
        0,
        "no dial after removal"
    );
    assert_eq!(mgr.peer_count(), 0);

    // A new peer is a new life of the address.
    let again = mgr.peer(&cluster.address);
    assert_ne!(again.id(), id);
    assert_eq!(
        within(STEP, "connect", again.connect()).await,
        Ok(cluster.version)
    );

    mgr.shutdown().await;
    cluster.finish().await;
}

/// A server that disappears is dialed (with backoff) while a caller references the peer, and
/// forgotten once the caller lets go: the peer is removed, and when the server comes back nothing
/// dials it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_vanished_server_peer_is_forgotten() {
    retrying(|c| Box::pin(vanished_server(c))).await;
}

async fn vanished_server(cluster: &Cluster) -> Option<()> {
    let (mgr, mut events) = cluster.manager(cluster.config());
    let peer = mgr.peer(&cluster.address);
    let id = peer.id();
    let guard = peer.reference();
    within(STEP, "connect", peer.connect()).await.unwrap();
    let before = peer.status().failures;

    cluster.disturb("stop", |f| f.stop()).await?;
    // The referenced peer keeps trying.
    within(
        STEP,
        "the peer to keep failing",
        peer.wait_for(|s| (s.failures >= before + 2).then_some(())),
    )
    .await;
    assert!(peer.status().last_error.is_some());
    assert_ne!(peer.status().phase, PeerPhase::Connected);
    assert_eq!(mgr.peer_count(), 1);

    // The caller lets go: the peer is removed when its attempt ends.
    drop(guard);
    drop(peer);
    within(STEP, "the peer to be removed", async {
        loop {
            let record = events.next().await.expect("events");
            if record.peer == id && record.event == PeerEvent::Removed {
                return;
            }
        }
    })
    .await;
    assert_eq!(mgr.peer_count(), 0);
    drain(&mut events);

    // The server returns, and nobody dials it.
    cluster.disturb("start", |f| f.start_stopped()).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        cluster.server_connections().await,
        0,
        "no dial after removal"
    );
    assert_eq!(drain(&mut events), vec![], "no event after Removed");
    assert_eq!(mgr.peer_count(), 0);

    mgr.shutdown().await;
    Some(())
}

/// Shutdown joins everything: with requests in flight to a stalled server, the manager shuts
/// down within its bounds, the requests complete as cancelled (maybe delivered), later commands
/// are refused, the server sees the connection closed once it thaws, and no task is left on the
/// runtime.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_shutdown_joins_everything() {
    let cluster = Cluster::start().await;
    let alive = || {
        tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
    };
    let tasks_before = alive();
    let (mgr, mut events) = cluster.manager(cluster.config());
    let peer = mgr.peer(&cluster.address);
    let guard = peer.reference();
    // A second peer that is dialed to an address nothing listens on, and a third never used.
    let nowhere = NetworkAddress::new(
        std::net::IpAddr::V4(Ipv4Addr::LOCALHOST).into(),
        1,
        true,
        false,
    );
    let dead = mgr.peer(&nowhere);
    let unused = mgr.peer(&NetworkAddress::new(
        std::net::IpAddr::V4(Ipv4Addr::LOCALHOST).into(),
        2,
        true,
        false,
    ));
    assert_eq!(mgr.peer_count(), 3);
    within(STEP, "connect", peer.connect()).await.unwrap();
    let refused = within(STEP, "the request to nowhere", protocol_info(&dead))
        .await
        .unwrap_err();
    assert!(matches!(refused, RequestError::NotSent(_)), "{refused:?}");
    assert!(alive() > tasks_before, "the peers' tasks run");

    cluster.blocking("pause", |f| f.pause()).await;
    let requests: Vec<_> = (0..20)
        .map(|_| {
            let peer = peer.clone();
            tokio::spawn(async move { protocol_info(&peer).await })
        })
        .collect();
    // Let them reach the socket, well within the ping timeout.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(peer.status().phase, PeerPhase::Connected);

    within(Duration::from_secs(10), "shutdown", mgr.shutdown()).await;
    assert!(mgr.is_shut_down());
    for request in requests {
        let error = within(STEP, "a request", request)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error, RequestError::Cancelled);
        assert!(error.maybe_delivered());
    }
    for handle in [&peer, &dead, &unused] {
        assert_eq!(handle.status().phase, PeerPhase::Shutdown);
    }
    assert_eq!(
        protocol_info(&peer).await,
        Err(RequestError::NotSent(None)),
        "refused after shutdown"
    );
    assert_eq!(peer.connect().await, Err(PeerError::Shutdown));
    let seen = drain(&mut events);
    assert!(
        seen.iter().any(|e| matches!(
            e,
            PeerEvent::Closed { established: true, error, .. } if error.is_clean()
        )),
        "{seen:?}"
    );
    // Idempotent.
    within(STEP, "second shutdown", mgr.shutdown()).await;

    // Every task the peers had has ended.
    drop(guard);
    drop((peer, dead, unused));
    within(STEP, "the tasks to end", async {
        while alive() > tasks_before {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    cluster.blocking("unpause", |f| f.unpause()).await;
    within(STEP, "the server to see the close", async {
        while cluster.server_connections().await != 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    cluster.finish().await;
}
