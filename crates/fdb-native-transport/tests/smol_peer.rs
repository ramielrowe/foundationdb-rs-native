//! The peer manager over the smol adapter (`SmolRuntime`, feature `smol`), against a scripted
//! fake FDB server on a real loopback TCP socket: connect and the ConnectPacket handshake,
//! version detection, request/reply with many concurrent requests (tasks spawned on smol),
//! the connection monitor's pings and its timeout, reconnecting after the server hangs up, the
//! handshake timeout, and shutdown. Real time, no tokio anywhere: the futures are driven by
//! `futures_lite::future::block_on` and the transport's tasks and timers are smol's.
//!
//! The server is plain blocking `std` code on threads of its own. The same paths against a
//! real server are in `smol_docker.rs`.

#![cfg(feature = "smol")]

use std::future::Future;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_transport::ConnectionError;
use fdb_native_transport::connection::{PING_REPLY_OK, PingRequest, well_known};
use fdb_native_transport::peer::{
    DiscardEvents, PeerError, PeerHandle, PeerManager, PeerManagerConfig, PeerPhase,
    ProtocolInfoReply, ProtocolInfoRequest,
};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::{Runtime, SmolRuntime};
use fdb_native_transport::{Checksum, ConnectPacket, FrameConfig, FrameDecoder};
use fdb_native_wire::{
    EncodeError, EnsureTable, ErrorOr, NetworkAddress, ProtocolVersion, ReplyPromise, Uid,
};
use futures_lite::future::{block_on, or};

const STEP: Duration = Duration::from_secs(20);

/// What the fake server does, adjustable while it runs.
#[derive(Default)]
struct Knobs {
    /// Connections accepted.
    accepted: AtomicUsize,
    /// Pings answered.
    pings: AtomicUsize,
    /// `ProtocolInfoRequest`s answered.
    requests: AtomicUsize,
    /// Ignore pings.
    mute_pings: AtomicBool,
    /// Hang up on the next request instead of answering it (once).
    hang_up_on_request: AtomicBool,
    /// Never send the server's ConnectPacket.
    silent: AtomicBool,
}

struct Server {
    addr: SocketAddr,
    knobs: Arc<Knobs>,
}

impl Server {
    fn start(version: ProtocolVersion) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let knobs = Arc::new(Knobs::default());
        let shared = knobs.clone();
        std::thread::spawn(move || {
            let held = Mutex::new(Vec::new());
            for sock in listener.incoming() {
                let Ok(sock) = sock else { return };
                shared.accepted.fetch_add(1, Ordering::SeqCst);
                if let Ok(copy) = sock.try_clone() {
                    held.lock().unwrap().push(copy);
                }
                let knobs = shared.clone();
                std::thread::spawn(move || {
                    let _ = serve(sock, addr, version, &knobs);
                });
            }
        });
        Server { addr, knobs }
    }

    fn address(&self) -> NetworkAddress {
        NetworkAddress::new(self.addr.ip().into(), self.addr.port(), true, false)
    }
}

fn write_frame(sock: &mut TcpStream, config: FrameConfig, token: Uid, payload: &[u8]) {
    let mut out = Vec::new();
    config.encode_frame(token, payload, &mut out).unwrap();
    sock.write_all(&out).unwrap();
}

fn serve(
    mut sock: TcpStream,
    listen: SocketAddr,
    version: ProtocolVersion,
    knobs: &Knobs,
) -> std::io::Result<()> {
    let mut hello = [0_u8; ConnectPacket::SIZE];
    sock.read_exact(&mut hello)?;
    let (client, _) = ConnectPacket::decode(&hello).expect("client's ConnectPacket");
    if !knobs.silent.load(Ordering::SeqCst) {
        let packet = ConnectPacket::server(version, listen);
        sock.write_all(&packet.encode().unwrap())?;
    }
    if !client.protocol_version.is_compatible(version) {
        // As fdbserver does: say who we are, then hang up.
        return Ok(());
    }
    let config = FrameConfig::new(Checksum::for_tls(false));
    let mut decoder = FrameDecoder::new(config);
    let mut buf = vec![0_u8; 16 * 1024];
    loop {
        let n = sock.read(&mut buf)?;
        if n == 0 {
            return Ok(());
        }
        decoder.push(&buf[..n]);
        while let Some(frame) = decoder.next_frame().expect("client frames") {
            let frame = frame.into_owned();
            if frame.token == well_known::PING_PACKET {
                if knobs.mute_pings.load(Ordering::SeqCst) {
                    continue;
                }
                let ping: PingRequest = fdb_native_wire::decode(&frame.payload).expect("ping");
                let payload = fdb_native_wire::encode(&PING_REPLY_OK).unwrap();
                write_frame(&mut sock, config, ping.reply.token(), &payload);
                knobs.pings.fetch_add(1, Ordering::SeqCst);
            } else if frame.token == well_known::PROTOCOL_INFO {
                if knobs.hang_up_on_request.swap(false, Ordering::SeqCst) {
                    let _ = sock.shutdown(Shutdown::Both);
                    return Ok(());
                }
                let request: ProtocolInfoRequest =
                    fdb_native_wire::decode(&frame.payload).expect("request");
                let reply: ErrorOr<EnsureTable<ProtocolInfoReply>> =
                    Ok(EnsureTable(ProtocolInfoReply { version: version.0 }));
                let payload = fdb_native_wire::encode(&reply).unwrap();
                write_frame(&mut sock, config, request.reply.token(), &payload);
                knobs.requests.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

/// Asks the server for its protocol version (token 10, as every fdbserver serves).
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

/// Fails the test if `f` takes longer than `STEP` of smol time.
async fn within<T>(rt: &SmolRuntime, what: &str, f: impl Future<Output = T>) -> T {
    or(async { Some(f.await) }, async {
        rt.sleep(STEP).await;
        None
    })
    .await
    .unwrap_or_else(|| panic!("{what}: nothing within {STEP:?}"))
}

/// Polls `cond` every 20 ms for up to `STEP`.
async fn until(rt: &SmolRuntime, what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = rt.now() + STEP;
    while !cond() {
        assert!(rt.now() < deadline, "timed out waiting for {what}");
        rt.sleep(Duration::from_millis(20)).await;
    }
}

fn config(versions: Vec<ProtocolVersion>) -> PeerManagerConfig {
    let mut config = PeerManagerConfig::default().without_jitter();
    config.supported_versions = versions;
    config.seed = Some(1);
    config
}

#[test]
fn connect_detects_the_version_and_requests_get_replies() {
    for right in [ProtocolVersion::V7_4, ProtocolVersion::V8_0] {
        let wrong = if right == ProtocolVersion::V7_4 {
            ProtocolVersion::V8_0
        } else {
            ProtocolVersion::V7_4
        };
        let server = Server::start(right);
        block_on(async {
            let rt = SmolRuntime::new();
            // The wrong version first, so that detection has to work.
            let mgr = PeerManager::new(rt.clone(), config(vec![wrong, right]), DiscardEvents);
            let peer = mgr.peer(&server.address());

            let version = within(&rt, "connect", peer.connect()).await.unwrap();
            assert_eq!(version, right);
            assert_eq!(mgr.cluster_version(), Some(right));
            let status = peer.status();
            assert_eq!(status.phase, PeerPhase::Connected, "{status:?}");
            assert_eq!(server.knobs.accepted.load(Ordering::SeqCst), 2);

            let reply = within(&rt, "request", protocol_info(&peer)).await.unwrap();
            assert_eq!(reply.protocol_version().without_flags(), right);

            // Many at once from tasks spawned on the executor, sharing the connection.
            let tasks: Vec<_> = (0..32)
                .map(|_| {
                    let peer = peer.clone();
                    rt.spawn(async move { protocol_info(&peer).await })
                })
                .collect();
            for task in tasks {
                let reply = within(&rt, "a concurrent request", task)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(reply.protocol_version().without_flags(), right);
            }
            assert_eq!(server.knobs.requests.load(Ordering::SeqCst), 33);
            assert_eq!(
                server.knobs.accepted.load(Ordering::SeqCst),
                2,
                "no reconnect"
            );

            // A frame to a token nobody serves is accepted (the server ignores it).
            let payload = fdb_native_wire::encode(&Uid::new(1, 2)).unwrap();
            peer.send(Uid::new(0x5eed, 0xfeed), payload).await.unwrap();

            within(&rt, "shutdown", mgr.shutdown()).await;
            assert_eq!(peer.status().phase, PeerPhase::Shutdown);
            assert_eq!(protocol_info(&peer).await, Err(RequestError::NotSent(None)));
        });
    }
}

/// The monitor pings on a smol timer, and the connection stays up while they are answered.
#[test]
fn pings_flow_and_keep_the_connection() {
    let server = Server::start(ProtocolVersion::V7_4);
    block_on(async {
        let rt = SmolRuntime::new();
        let mut config = config(vec![ProtocolVersion::V7_4]);
        config.monitor.loop_time = Duration::from_millis(100);
        config.monitor.timeout = Duration::from_millis(600);
        let mgr = PeerManager::new(rt.clone(), config, DiscardEvents);
        let peer = mgr.peer(&server.address());
        peer.connect().await.unwrap();
        let generation = peer.status().generation;
        until(&rt, "five pings", || {
            server.knobs.pings.load(Ordering::SeqCst) >= 5
        })
        .await;
        assert_eq!(peer.status().generation, generation);
        assert_eq!(peer.status().failures, 0);
        mgr.shutdown().await;
    });
}

/// Pings that go unanswered fail the connection (a smol timer fires), and the peer connects
/// again when asked to.
#[test]
fn unanswered_pings_fail_the_connection_and_it_reconnects() {
    let server = Server::start(ProtocolVersion::V8_0);
    block_on(async {
        let rt = SmolRuntime::new();
        let mut config = config(vec![ProtocolVersion::V8_0]);
        config.monitor.loop_time = Duration::from_millis(50);
        config.monitor.timeout = Duration::from_millis(300);
        config.backoff.initial = Duration::from_millis(20);
        let mgr = PeerManager::new(rt.clone(), config, DiscardEvents);
        let peer = mgr.peer(&server.address());
        peer.connect().await.unwrap();
        let generation = peer.status().generation;

        server.knobs.mute_pings.store(true, Ordering::SeqCst);
        until(&rt, "the ping timeout", || peer.status().failures >= 1).await;
        let status = peer.status();
        assert!(
            matches!(
                status.last_error,
                Some(PeerError::Connection(ConnectionError::PingTimeout(_)))
            ),
            "{status:?}"
        );
        server.knobs.mute_pings.store(false, Ordering::SeqCst);
        within(&rt, "reconnect", peer.connect()).await.unwrap();
        assert!(peer.status().generation > generation);
        // Working again.
        within(&rt, "request", protocol_info(&peer)).await.unwrap();
        mgr.shutdown().await;
    });
}

/// A server that hangs up in the middle of a request: the request fails (the peer cannot know
/// whether it was processed), and the peer connects again.
#[test]
fn a_hang_up_fails_the_request_and_the_peer_reconnects() {
    let server = Server::start(ProtocolVersion::V7_4);
    block_on(async {
        let rt = SmolRuntime::new();
        let mut config = config(vec![ProtocolVersion::V7_4]);
        config.backoff.initial = Duration::from_millis(20);
        let mgr = PeerManager::new(rt.clone(), config, DiscardEvents);
        let peer = mgr.peer(&server.address());
        peer.connect().await.unwrap();
        assert_eq!(server.knobs.accepted.load(Ordering::SeqCst), 1);

        server
            .knobs
            .hang_up_on_request
            .store(true, Ordering::SeqCst);
        let err = within(&rt, "the failing request", protocol_info(&peer))
            .await
            .unwrap_err();
        // The request was written before the hang-up: maybe delivered.
        assert!(!err.certainly_not_delivered(), "{err:?}");

        // Retrying (as callers do) works once the peer has connected again.
        let reply = within(&rt, "the retry", async {
            loop {
                match protocol_info(&peer).await {
                    Ok(reply) => return reply,
                    Err(_) => rt.sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await;
        assert_eq!(
            reply.protocol_version().without_flags(),
            ProtocolVersion::V7_4
        );
        assert!(server.knobs.accepted.load(Ordering::SeqCst) >= 2);
        mgr.shutdown().await;
    });
}

/// A server that accepts and never speaks: the handshake times out (a smol timer), the
/// failure is recorded and the peer keeps trying.
#[test]
fn a_silent_server_times_the_handshake_out() {
    let server = Server::start(ProtocolVersion::V7_4);
    server.knobs.silent.store(true, Ordering::SeqCst);
    block_on(async {
        let rt = SmolRuntime::new();
        let mut config = config(vec![ProtocolVersion::V7_4]);
        config.handshake_timeout = Duration::from_millis(200);
        config.backoff.initial = Duration::from_millis(20);
        let mgr = PeerManager::new(rt.clone(), config, DiscardEvents);
        let peer = mgr.peer(&server.address());
        let started = rt.now();
        let attempt = peer.connect();
        let result = or(async { Some(attempt.await) }, async {
            rt.sleep(Duration::from_millis(1500)).await;
            None
        })
        .await;
        // Either connect gave up with an error, or it is still trying: never `Ok`.
        assert!(!matches!(result, Some(Ok(_))), "{result:?}");
        assert!(rt.now() - started >= Duration::from_millis(200));
        until(&rt, "recorded failures", || peer.status().failures >= 1).await;
        assert!(server.knobs.accepted.load(Ordering::SeqCst) >= 1);

        // The server starts talking: the peer gets through.
        server.knobs.silent.store(false, Ordering::SeqCst);
        within(&rt, "connect", peer.connect()).await.unwrap();
        mgr.shutdown().await;
    });
}

/// Connecting to a port nothing listens on fails with an error the manager records, rather
/// than hanging.
#[test]
fn a_refused_connection_is_an_error_not_a_hang() {
    let addr = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    block_on(async {
        let rt = SmolRuntime::new();
        let mgr = PeerManager::new(
            rt.clone(),
            config(vec![ProtocolVersion::V7_4]),
            DiscardEvents,
        );
        let address = NetworkAddress::new(addr.ip().into(), addr.port(), true, false);
        let peer = mgr.peer(&address);
        let result = or(async { Some(peer.connect().await) }, async {
            rt.sleep(Duration::from_millis(800)).await;
            None
        })
        .await;
        assert!(!matches!(result, Some(Ok(_))), "{result:?}");
        until(&rt, "recorded failures", || peer.status().failures >= 1).await;
        mgr.shutdown().await;
    });
}

/// Two users of one manager on smol share the connection, and one user's shutdown leaves the
/// other's requests working; the connection closes with the last user. Smol runtimes are the
/// same runtime when they share the executor.
#[test]
fn users_share_the_connection_on_smol() {
    let server = Server::start(ProtocolVersion::V7_4);
    block_on(async {
        let rt = SmolRuntime::new();
        assert!(rt.same_runtime(&SmolRuntime::new()));
        let own = SmolRuntime::with_executor(Arc::new(async_executor::Executor::new()));
        assert!(!rt.same_runtime(&own));
        let a = PeerManager::new(
            rt.clone(),
            config(vec![ProtocolVersion::V7_4]),
            DiscardEvents,
        );
        assert!(a.downgrade().share_on(&own).is_none());
        let b = a
            .downgrade()
            .share_on(&SmolRuntime::new())
            .expect("same runtime");
        let (pa, pb) = (a.peer(&server.address()), b.peer(&server.address()));
        within(&rt, "a request of a", protocol_info(&pa))
            .await
            .unwrap();
        within(&rt, "a request of b", protocol_info(&pb))
            .await
            .unwrap();
        assert_eq!(server.knobs.accepted.load(Ordering::SeqCst), 1);
        within(&rt, "a's shutdown", a.shutdown()).await;
        assert_eq!(protocol_info(&pa).await, Err(RequestError::NotSent(None)));
        within(&rt, "b after a's shutdown", protocol_info(&pb))
            .await
            .unwrap();
        assert_eq!(pb.status().phase, PeerPhase::Connected);
        assert_eq!(server.knobs.accepted.load(Ordering::SeqCst), 1);
        within(&rt, "b's shutdown", b.shutdown()).await;
        assert_eq!(pb.status().phase, PeerPhase::Shutdown);
    });
}
