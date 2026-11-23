//! The peer manager over TLS against real, disposable FDB servers started by
//! `fdb-native-testkit` with TLS on (generated CA, server and client certificates): connect,
//! handshake, requests, liveness, `verify_peers` rules, and the rejection cases. Run with
//! `cargo nextest run --run-ignored only --features tls`.
//!
//! Like `peer_docker`, each test runs against both pinned images, 7.4.10 and 8.0.1, so
//! `FDB_IMAGE` does not apply. Test names start with `docker_` (see `.config/nextest.toml`).

#![cfg(all(feature = "tls", feature = "tokio"))]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use fdb_native_testkit::{FdbContainer, TestPki, TlsCluster};
use fdb_native_transport::ConnectionError;
use fdb_native_transport::connection::well_known;
use fdb_native_transport::peer::{
    EventReceiver, Generation, PeerError, PeerEvent, PeerHandle, PeerManager, PeerManagerConfig,
    PeerPhase, ProtocolInfoReply, ProtocolInfoRequest, event_channel,
};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_transport::tls::peer::TlsStreamWrapper;
use fdb_native_transport::tls::{TlsConfig, TlsConnector};
use fdb_native_wire::{EncodeError, NetworkAddress, ProtocolVersion, ReplyPromise};

const IMAGES: [(&str, ProtocolVersion); 2] = [
    ("foundationdb/foundationdb:7.4.10", ProtocolVersion::V7_4),
    ("foundationdb/foundationdb:8.0.1", ProtocolVersion::V8_0),
];

type TlsManager = PeerManager<TokioRuntime, TlsStreamWrapper>;

/// A running TLS cluster (started on a blocking thread: starting and dropping it blocks).
async fn start(image: &'static str) -> FdbContainer {
    start_with(image, |b| b).await
}

async fn start_with(
    image: &'static str,
    configure: impl FnOnce(
        fdb_native_testkit::FdbContainerBuilder,
    ) -> fdb_native_testkit::FdbContainerBuilder
    + Send
    + 'static,
) -> FdbContainer {
    tokio::task::spawn_blocking(move || {
        configure(FdbContainer::builder().image(image).tls())
            .start()
            .unwrap_or_else(|e| panic!("{image}: {e}"))
    })
    .await
    .unwrap()
}

/// Dropping a container blocks; do it off the async workers.
async fn stop(fdb: FdbContainer) {
    tokio::task::spawn_blocking(move || drop(fdb))
        .await
        .unwrap();
}

/// The container's address, with the TLS flag the cluster file's `:tls` means.
fn tls_address(addr: SocketAddr) -> NetworkAddress {
    NetworkAddress::new(addr.ip().into(), addr.port(), true, true)
}

/// What a client of `tls` is configured with. Everything explicit, nothing from the
/// environment or `/etc/foundationdb`.
struct Client<'a> {
    ca_pem: &'a str,
    /// `(certificate PEM, key PEM)`, or none for an anonymous client.
    identity: Option<(&'a str, &'a str)>,
    verify_peers: Option<&'a str>,
}

impl<'a> Client<'a> {
    /// The cluster's CA and client certificate.
    fn of(tls: &'a TlsCluster) -> Self {
        Client {
            ca_pem: tls.ca_pem(),
            identity: Some((tls.client_cert_pem(), tls.client_key_pem())),
            verify_peers: None,
        }
    }

    fn verify_peers(mut self, rules: &'a str) -> Self {
        self.verify_peers = Some(rules);
        self
    }

    fn connector(&self) -> TlsConnector {
        let mut config = TlsConfig::new().ca_bytes(self.ca_pem);
        if let Some((cert, key)) = self.identity {
            config = config.cert_bytes(cert).key_bytes(key);
        }
        if let Some(rules) = self.verify_peers {
            config = config.verify_peers(rules);
        }
        let loaded = config.load_with(|_| None, None).expect("tls config loads");
        TlsConnector::new(&loaded).expect("connector builds")
    }

    fn manager(&self) -> (TlsManager, EventReceiver) {
        self.manager_with(PeerManagerConfig::default())
    }

    fn manager_with(&self, config: PeerManagerConfig) -> (TlsManager, EventReceiver) {
        let (sink, events) = event_channel();
        let manager = PeerManager::with_stream_wrapper(
            TokioRuntime::new(),
            config,
            TlsStreamWrapper::new(self.connector()),
            sink,
        );
        (manager, events)
    }
}

fn drain(events: &mut EventReceiver) -> Vec<PeerEvent> {
    std::iter::from_fn(|| events.try_next())
        .map(|record| record.event)
        .collect()
}

/// Asks the server for its protocol version: a `ProtocolInfoRequest` to the well-known
/// endpoint every fdbserver process serves, on both releases.
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

/// Connects and returns the error the attempt failed with.
async fn connect_error(peer: &PeerHandle, what: &str) -> PeerError {
    match tokio::time::timeout(Duration::from_secs(15), peer.connect()).await {
        Ok(Err(e)) => e,
        Ok(Ok(version)) => panic!("{what}: connected ({version:?}) but must have failed"),
        Err(_) => panic!("{what}: connect() did not finish"),
    }
}

/// Connect over TLS (checksums are off, so the server accepting our frames at all shows the
/// framing is right), detect the version, answer a ProtocolInfoRequest, many at once, then
/// five seconds of the monitor's pings in both directions, and a clean shutdown.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_connect_requests_and_pings() {
    for (image, right) in IMAGES {
        let fdb = start(image).await;
        let tls = fdb.tls().expect("tls cluster");
        let address = tls_address(fdb.address());
        let (mgr, mut events) = Client::of(tls).manager();
        let peer = mgr.peer(&address);

        let version = peer
            .connect()
            .await
            .unwrap_or_else(|e| panic!("{image}: {e}"));
        assert_eq!(version, right, "{image}");
        assert_eq!(mgr.cluster_version(), Some(right));
        let status = peer.status();
        assert_eq!(status.phase, PeerPhase::Connected, "{image}: {status:?}");
        assert_eq!(status.failures, 0, "{image}: {status:?}");
        assert!(!peer.is_failed());
        let generation = status.generation;

        // A frame the server decodes and answers: with the checksum off, as the TLS address
        // says (a server that expected a checksum would fail the frame and close).
        let reply = protocol_info(&peer)
            .await
            .unwrap_or_else(|e| panic!("{image}: {e}"));
        assert_eq!(reply.protocol_version().without_flags(), right, "{image}");

        let mut requests = Vec::new();
        for _ in 0..32 {
            let peer = peer.clone();
            requests.push(tokio::spawn(async move { protocol_info(&peer).await }));
        }
        for request in requests {
            let reply = request
                .await
                .unwrap()
                .unwrap_or_else(|e| panic!("{image}: {e}"));
            assert_eq!(reply.protocol_version().without_flags(), right);
        }

        // Pings: ours are answered and the server's are answered by us (or it would close).
        tokio::time::sleep(Duration::from_secs(5)).await;
        let status = peer.status();
        assert_eq!(status.phase, PeerPhase::Connected, "{image}: {status:?}");
        assert_eq!(status.failures, 0, "{image}: {status:?}");
        assert!(!peer.is_failed());
        let reply = protocol_info(&peer)
            .await
            .unwrap_or_else(|e| panic!("{image}: after pings: {e}"));
        assert_eq!(reply.protocol_version().without_flags(), right);

        let seen = drain(&mut events);
        assert!(
            !seen.iter().any(|e| matches!(
                e,
                PeerEvent::Closed {
                    established: true,
                    ..
                }
            )),
            "{image}: the connection must have stayed up: {seen:?}"
        );
        assert_eq!(status.generation, generation, "{image}: it reconnected");

        mgr.shutdown().await;
        assert_eq!(peer.status().phase, PeerPhase::Shutdown);
        let seen = drain(&mut events);
        assert!(
            seen.iter().any(|e| matches!(
                e,
                PeerEvent::Closed { error, established: true, .. } if error.is_clean()
            )),
            "{image}: {seen:?}"
        );
        stop(fdb).await;
    }
}

/// A server that goes away (the container is stopped, so its sockets close without a TLS
/// `close_notify`, as FoundationDB's do) is a peer close, not an I/O error: the truncated TLS
/// close reads as end of stream.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_server_close_is_a_peer_close() {
    for (image, _) in IMAGES {
        let fdb = start(image).await;
        let tls = fdb.tls().expect("tls cluster");
        let address = tls_address(fdb.address());
        let (mgr, mut events) = Client::of(tls).manager();
        let peer = mgr.peer(&address);
        peer.connect().await.unwrap();
        drain(&mut events);

        // Kill the server process: the kernel closes its sockets without a close_notify.
        let id = fdb.id().to_owned();
        let stopped = tokio::task::spawn_blocking(move || {
            std::process::Command::new("docker")
                .args(["stop", "--time", "1", &id])
                .output()
                .expect("docker")
        })
        .await
        .unwrap();
        assert!(stopped.status.success(), "{image}: docker stop failed");

        let closed = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(record) = events.try_next() {
                    if let PeerEvent::Closed { error, .. } = record.event {
                        return error;
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{image}: the close was not noticed"));
        eprintln!("{image}: closed with {closed:?}");
        assert_eq!(
            closed,
            PeerError::Connection(ConnectionError::PeerClosed {
                during_handshake: false
            }),
            "{image}"
        );
        mgr.shutdown().await;
        stop(fdb).await;
    }
}

/// `verify_peers` rules for the server certificate that accept it, `|` separating rules.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_verify_peers_matching_rules() {
    for (image, right) in IMAGES {
        let fdb = start(image).await;
        let tls = fdb.tls().expect("tls cluster");
        let address = tls_address(fdb.address());

        for rules in [
            "Check.Valid=1",
            "Check.Valid=1,S.CN=fdb-test-server",
            "S.CN=fdb-test-server,S.O=fdb-native-testkit,I.CN=fdb-test-ca",
            "S.CN=somebody-else|S.CN=fdb-test-server",
            "Check.Valid=1,I.O=fdb-native-testkit,R.CN=fdb-test-ca",
        ] {
            let (mgr, _events) = Client::of(tls).verify_peers(rules).manager();
            let peer = mgr.peer(&address);
            let version = peer
                .connect()
                .await
                .unwrap_or_else(|e| panic!("{image}: {rules}: {e}"));
            assert_eq!(version, right, "{image}: {rules}");
            assert!(protocol_info(&peer).await.is_ok(), "{image}: {rules}");
            mgr.shutdown().await;
        }
        stop(fdb).await;
    }
}

/// The server asks for a TLS handshake failure to be remembered: after one failed handshake
/// from an IP it throttles that IP's TLS connections for 9 seconds (`TLS_SERVER_CONNECTION_
/// THROTTLE_TIMEOUT`; both releases), holding and then resetting them. So a test that expects
/// a specific failure gets a container of its own and makes the failing attempt its first.
///
/// `rules` must make the client refuse the (valid) server certificate: the attempt fails as a
/// TLS error before any FoundationDB byte, and nothing falls back to plaintext.
async fn assert_rules_reject_server(rules: &'static str) {
    for (image, _) in IMAGES {
        let fdb = start(image).await;
        let tls = fdb.tls().expect("tls cluster");
        let address = tls_address(fdb.address());

        let (mgr, mut events) = Client::of(tls).verify_peers(rules).manager();
        let peer = mgr.peer(&address);
        let error = connect_error(&peer, &format!("{image}: {rules}")).await;
        assert!(
            matches!(
                &error,
                PeerError::Tls { kind: std::io::ErrorKind::InvalidData, message }
                    if message.contains("certificate")
            ),
            "{image}: {rules}: {error:?}"
        );
        assert_failed_before_connecting(&peer, &mut events, image).await;
        mgr.shutdown().await;
        stop(fdb).await;
    }
}

/// The peer never connected: no connection ran, the failure is the TLS one, and a request is
/// refused without being sent.
async fn assert_failed_before_connecting(
    peer: &PeerHandle,
    events: &mut EventReceiver,
    what: &str,
) {
    let status = peer.status();
    assert_ne!(status.phase, PeerPhase::Connected, "{what}");
    assert_eq!(status.generation, Generation(0), "{what}: a connection ran");
    assert!(status.failures >= 1, "{what}");
    let seen = drain(events);
    assert!(
        seen.iter()
            .all(|e| !matches!(e, PeerEvent::Connected { .. } | PeerEvent::Closed { .. })),
        "{what}: {seen:?}"
    );
    assert!(
        seen.iter().any(|e| matches!(
            e,
            PeerEvent::ConnectFailed {
                error: PeerError::Tls { .. }
            }
        )),
        "{what}: {seen:?}"
    );
    assert!(
        matches!(protocol_info(peer).await, Err(RequestError::NotSent(_))),
        "{what}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_verify_peers_subject_mismatch() {
    assert_rules_reject_server("Check.Valid=1,S.CN=not-the-server").await;
}

/// A rule's criteria all have to match: the common name does, the organization does not.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_verify_peers_partial_match_is_a_mismatch() {
    assert_rules_reject_server("S.CN=fdb-test-server,S.O=somebody-else").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_verify_peers_issuer_mismatch() {
    assert_rules_reject_server("I.CN=not-the-ca").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_verify_peers_root_mismatch() {
    assert_rules_reject_server("Check.Valid=1,R.CN=not-the-root").await;
}

/// A client whose CA is not the server's does not trust it: a TLS error (unknown issuer), not
/// a connection.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_wrong_ca_is_rejected() {
    for (image, _) in IMAGES {
        let fdb = start(image).await;
        let tls = fdb.tls().expect("tls cluster");
        let address = tls_address(fdb.address());
        let other = TestPki::generate_named("some-other-ca").unwrap();

        let mut client = Client::of(tls);
        client.ca_pem = other.ca_pem();
        let (mgr, mut events) = client.manager();
        let peer = mgr.peer(&address);
        let error = connect_error(&peer, image).await;
        assert!(
            matches!(
                &error,
                PeerError::Tls { kind: std::io::ErrorKind::InvalidData, message }
                    if message.to_lowercase().contains("unknownissuer")
            ),
            "{image}: {error:?}"
        );
        assert_failed_before_connecting(&peer, &mut events, image).await;
        mgr.shutdown().await;
        stop(fdb).await;
    }
}

/// The server's side of mTLS: it verifies a client certificate against its CA and its own
/// `verify_peers` (here `S.CN=` rules; its own certificate is on the list because `fdbcli`
/// presents it). The generated client certificate is accepted. A client without any
/// certificate is *untrusted*, not refused: it connects and can use the public ProtocolInfo
/// endpoint.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_server_accepts_client_certificates() {
    for (image, right) in IMAGES {
        let fdb = start_with(image, |b| {
            b.tls_verify_peers("S.CN=fdb-test-client|S.CN=fdb-test-server")
        })
        .await;
        let tls = fdb.tls().expect("tls cluster");
        let address = tls_address(fdb.address());

        for (what, anonymous) in [("client certificate", false), ("no certificate", true)] {
            let mut client = Client::of(tls);
            if anonymous {
                client.identity = None;
            }
            let (mgr, _events) = client.manager();
            assert_eq!(
                mgr.peer(&address).connect().await,
                Ok(right),
                "{image}: {what}"
            );
            let peer = mgr.peer(&address);
            let reply = protocol_info(&peer)
                .await
                .unwrap_or_else(|e| panic!("{image}: {what}: {e}"));
            assert_eq!(reply.protocol_version().without_flags(), right);
            mgr.shutdown().await;
        }
        stop(fdb).await;
    }
}

/// With a server that checks client subjects, a certificate from the right CA but the wrong
/// subject is refused by the server: with TLS 1.3 our handshake has finished by then, so the
/// connection fails on its first use, never reaching the established state.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_server_rejects_unlisted_client_certificate() {
    for (image, _) in IMAGES {
        let fdb = start_with(image, |b| {
            b.tls_verify_peers("S.CN=fdb-test-client|S.CN=fdb-test-server")
        })
        .await;
        let tls = fdb.tls().expect("tls cluster");
        let address = tls_address(fdb.address());

        let intruder = tls.pki().issue("intruder").unwrap();
        let mut client = Client::of(tls);
        client.identity = Some((intruder.cert_pem(), intruder.key_pem()));
        let (mgr, mut events) = client.manager();
        let peer = mgr.peer(&address);
        let error = connect_error(&peer, image).await;
        assert_not_connected(&peer, &mut events, &error, image);
        mgr.shutdown().await;
        stop(fdb).await;
    }
}

/// A client certificate from another CA is not valid for the server at all.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_server_rejects_client_certificate_from_another_ca() {
    for (image, _) in IMAGES {
        let fdb = start(image).await;
        let tls = fdb.tls().expect("tls cluster");
        let address = tls_address(fdb.address());

        let other = TestPki::generate_named("some-other-ca").unwrap();
        let mut client = Client::of(tls);
        client.identity = Some((other.client().cert_pem(), other.client().key_pem()));
        let (mgr, mut events) = client.manager();
        let peer = mgr.peer(&address);
        let error = connect_error(&peer, image).await;
        assert_not_connected(&peer, &mut events, &error, image);
        mgr.shutdown().await;
        stop(fdb).await;
    }
}

/// The attempt failed without ever being established.
fn assert_not_connected(
    peer: &PeerHandle,
    events: &mut EventReceiver,
    error: &PeerError,
    what: &str,
) {
    eprintln!("{what}: rejected with {error:?}");
    assert_ne!(
        peer.status().phase,
        PeerPhase::Connected,
        "{what}: {error:?}"
    );
    let seen = drain(events);
    assert!(
        seen.iter().all(|e| !matches!(
            e,
            PeerEvent::Connected { .. }
                | PeerEvent::Closed {
                    established: true,
                    ..
                }
        )),
        "{what}: {seen:?}"
    );
}

/// A manager with no TLS configured (the default) refuses a TLS address at once, without
/// dialing, and does not fall back to plaintext.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_address_without_tls_configured() {
    for (image, _) in IMAGES {
        let fdb = start(image).await;
        let address = tls_address(fdb.address());
        let (sink, mut events) = event_channel();
        let mgr = PeerManager::new(TokioRuntime::new(), PeerManagerConfig::default(), sink);
        let peer = mgr.peer(&address);

        let started = Instant::now();
        let error = connect_error(&peer, image).await;
        assert_eq!(error, PeerError::TlsNotConfigured, "{image}");
        assert!(started.elapsed() < Duration::from_secs(1), "{image}");
        assert_ne!(peer.status().phase, PeerPhase::Connected);
        let seen = drain(&mut events);
        assert!(
            seen.iter().any(|e| matches!(
                e,
                PeerEvent::ConnectFailed {
                    error: PeerError::TlsNotConfigured
                }
            )),
            "{image}: {seen:?}"
        );
        mgr.shutdown().await;
        stop(fdb).await;
    }
}
