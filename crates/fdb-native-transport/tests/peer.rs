//! The peer manager over an in-memory network on tokio's paused clock, against a scripted
//! FDB-like server (see `common`): connect and reuse, requests and replies, reconnect backoff,
//! version detection, failure state, timeouts, backpressure, idle closes, TLS hook, shutdown.
//! One test at the end runs over real loopback TCP. The review findings (starvation, the
//! command queue's bound, peer removal, ...) are in `peer_review.rs`.

#![cfg(feature = "tokio")]

mod common;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::*;
use fdb_native_transport::ConnectionError;
use fdb_native_transport::connection::well_known;
use fdb_native_transport::peer::{
    EndpointFailure, EndpointStatus, EventReceiver, FailureMonitor, Generation, NoTls, PeerError,
    PeerEvent, PeerEventRecord, PeerManager, PeerManagerConfig, PeerPhase, SendError,
    event_channel,
};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_wire::{NetworkAddress, NetworkAddressList, ProtocolVersion, Uid};

const V74: ProtocolVersion = ProtocolVersion::V7_4;
const V80: ProtocolVersion = ProtocolVersion::V8_0;

fn config() -> PeerManagerConfig {
    PeerManagerConfig::default().without_jitter()
}

fn manager(net: &Arc<Net>, config: PeerManagerConfig) -> (PeerManager<MemRuntime>, EventReceiver) {
    let (sink, events) = event_channel();
    (PeerManager::new(MemRuntime::new(net), config, sink), events)
}

fn drain(events: &mut EventReceiver) -> Vec<(NetworkAddress, PeerEvent)> {
    std::iter::from_fn(|| events.try_next())
        .map(|record| (record.address, record.event))
        .collect()
}

/// A monitor that never pings or times out, so a test can leave the peer alone for long.
fn quiet_monitor(c: &mut PeerManagerConfig) {
    c.monitor.loop_time = Duration::from_secs(10_000);
    c.monitor.timeout = Duration::from_secs(10_000);
    c.monitor.idle_timeout = Duration::from_secs(100_000);
}

fn token(n: u64) -> Uid {
    Uid::new(0x5eed_0000_0000_0000 | 1, n)
}

fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

// ---- connect and reuse ---------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn one_connection_is_shared_by_every_user_of_an_address() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::new(V74));
    let (mgr, mut events) = manager(&net, config());

    let a = mgr.peer(&server.address);
    let b = mgr.peer(&server.address);
    assert_eq!(
        a.status().phase,
        PeerPhase::Idle,
        "nothing connects until used"
    );
    assert_eq!(server.connections(), 0);

    assert_eq!(a.connect().await, Ok(V74));
    assert_eq!(b.connect().await, Ok(V74), "already connected");
    for i in 0..50 {
        a.send(token(i), vec![1, 2, 3, i as u8]).await.unwrap();
        b.send(token(1000 + i), vec![9; 100]).await.unwrap();
    }
    until("the server has all frames", || server.frames(0) == 100).await;

    assert_eq!(server.connections(), 1, "all sends shared one connection");
    assert_eq!(server.announced(), vec![Some(V74)]);
    let status = a.status();
    assert_eq!(status.phase, PeerPhase::Connected);
    assert_eq!(status.version, Some(V74));
    assert_eq!(status.generation, Generation(1));
    assert_eq!(status.failures, 0);
    assert_eq!(a.version(), Some(V74));
    assert!(!a.is_failed());
    assert_eq!(mgr.cluster_version(), Some(V74));

    let seen = drain(&mut events);
    assert!(
        matches!(&seen[..], [(addr, PeerEvent::Connected { generation: Generation(1), version, .. })]
            if *addr == server.address && *version == V74),
        "{seen:?}"
    );
    // The same address from another call is the same peer; another address is another peer.
    let other = listen_one(&net, sock(4501), Script::new(V74));
    mgr.peer(&other.address).connect().await.unwrap();
    assert_eq!(other.connections(), 1);
    assert_eq!(server.connections(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_request_connects_by_itself_and_its_reply_completes_it() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V80));
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);

    // The encoder is told the negotiated version, not the first guess (7.4): the first
    // connection is refused as incompatible, and the request waits for the second.
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = seen.clone();
    let reply = peer
        .request_as::<EchoReply, _>(token(7), move |reply, version, out| {
            log.lock().unwrap().push(version);
            echo_encode(5)(reply, version, out)
        })
        .await
        .unwrap();
    assert_eq!(
        reply,
        EchoReply {
            value: 5 + ECHO_ADDS
        }
    );
    assert_eq!(*seen.lock().unwrap(), vec![V80], "encoded once, for 8.0");
    assert_eq!(server.announced(), vec![Some(V74), Some(V80)]);
    assert_eq!(server.echo_values(1), vec![5]);
    assert_eq!(
        server.echo_values(0),
        Vec::<u64>::new(),
        "nothing on the wrong one"
    );
    let sent = server.record(1).frames[0].clone();
    assert_eq!(sent.token, token(7));
    let request = fdb_native_wire::decode::<EchoRequest>(&sent.payload).unwrap();
    assert_ne!(request.reply.token(), Uid::default());
    assert_eq!(
        request.reply.token().first & 1,
        0,
        "a reply token has the stream flag clear"
    );

    // The raw payload and decoding are separate steps too, and a server error is an error.
    let reply = peer.request(token(7), echo_encode(1)).await.unwrap();
    assert_eq!(
        reply.version, V80,
        "the reply says which version it arrived on"
    );
    assert_eq!(
        fdb_native_transport::rpc::decode_reply::<EchoReply>(&reply.payload),
        Ok(EchoReply {
            value: 1 + ECHO_ADDS
        })
    );
    assert_eq!(
        peer.request_as::<EchoReply, _>(token(7), echo_encode(FAILS))
            .await,
        Err(RequestError::Fdb(fdb_native_wire::FdbError::new(1020)))
    );
    assert_eq!(
        peer.request_as::<EchoReply, _>(token(7), echo_encode(BROKEN))
            .await,
        Err(RequestError::BrokenPromise)
    );
    // broken_promise also fails the endpoint, as C++ does: the next request is refused and
    // never sent.
    let frames = server.frames(1);
    assert_eq!(
        peer.request(token(7), echo_encode(2)).await,
        Err(RequestError::EndpointNotFound {
            token: token(7),
            maybe_delivered: false
        })
    );
    assert_eq!(server.frames(1), frames);
    assert!(
        mgr.failure_monitor()
            .endpoint_status(&server.address, token(7))
            .is_failed()
    );
    // A request to another endpoint is fine.
    assert!(peer.request(token(8), echo_encode(3)).await.is_ok());

    // A token can be registered once; well-known tokens cannot.
    let stream = Uid::new(0x1111, 0x2222);
    assert_eq!(peer.register(stream).await, Ok(Generation(2)));
    assert_eq!(
        peer.register(stream).await,
        Err(SendError::TokenUnavailable(stream))
    );
    peer.unregister(stream);
    assert_eq!(
        peer.register(well_known::PING_PACKET).await,
        Err(SendError::TokenUnavailable(well_known::PING_PACKET))
    );
    // A one-way send is told the version too.
    let one_way = Arc::new(std::sync::Mutex::new(None));
    let slot = one_way.clone();
    peer.send_with(token(9), move |version, out| {
        *slot.lock().unwrap() = Some(version);
        out.extend_from_slice(b"fire and forget");
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(*one_way.lock().unwrap(), Some(V80));
}

#[tokio::test(start_paused = true)]
async fn pings_keep_a_connection_open_and_an_unanswered_ping_ends_it() {
    let net = Net::new();
    let good = listen_one(&net, sock(4500), Script::new(V74));
    let mut silent = Script::new(V74);
    silent.answer_pings = false;
    let bad = listen_one(&net, sock(4501), silent);
    let (mgr, mut events) = manager(&net, config());

    let a = mgr.peer(&good.address);
    let b = mgr.peer(&bad.address);
    a.connect().await.unwrap();
    b.connect().await.unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(good.record(0).pings >= 8, "about one ping a second");
    assert_eq!(a.status().phase, PeerPhase::Connected);
    assert_eq!(a.status().generation, Generation(1), "never reconnected");
    assert_eq!(a.status().failures, 0);

    // The silent server never answers: the monitor gives up two seconds after its first ping.
    let status = b.status();
    assert!(status.failures >= 1, "{status:?}");
    assert!(
        matches!(
            status.last_error,
            Some(PeerError::Connection(ConnectionError::PingTimeout(_)))
        ),
        "{status:?}"
    );
    let closed: Vec<_> = drain(&mut events)
        .into_iter()
        .filter(|(addr, e)| *addr == bad.address && matches!(e, PeerEvent::Closed { .. }))
        .collect();
    assert!(!closed.is_empty());
    match &closed[0].1 {
        PeerEvent::Closed {
            generation,
            error,
            established,
        } => {
            assert_eq!(*generation, Generation(1));
            assert!(*established);
            assert!(!error.is_clean());
        }
        other => panic!("{other:?}"),
    }
}

// ---- reconnect backoff ---------------------------------------------------------------------

/// The gaps, in seconds, between consecutive connection attempts the server saw.
fn gaps(server: &Server) -> Vec<f64> {
    let records = server.records.lock().unwrap();
    records
        .windows(2)
        .map(|w| secs(w[1].at - w[0].at))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn quick_failures_back_off_from_60ms_to_500ms() {
    let net = Net::new();
    let mut script = Script::new(V74);
    script.close_before_hello = true;
    let server = listen_one(&net, sock(4500), script);
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);

    for _ in 0..18 {
        let error = peer.connect().await.unwrap_err();
        assert!(
            matches!(
                error,
                PeerError::Connection(ConnectionError::PeerClosed {
                    during_handshake: true
                })
            ),
            "{error:?}"
        );
    }
    let gaps = gaps(&server);
    assert_eq!(gaps.len(), 17);
    // INITIAL 0.05 * GROWTH 1.2 on the first failure, and so on up to MAX 0.5 (the delay counts
    // from the start of the previous attempt; tokio rounds timers up to the millisecond).
    let mut delay: f64 = 0.05;
    for (i, gap) in gaps.iter().enumerate() {
        delay = (delay * 1.2).min(0.5);
        assert!(
            (gap - delay).abs() < 0.003,
            "gap {i} was {gap}, expected {delay}: {gaps:?}"
        );
    }
    assert!((gaps[16] - 0.5).abs() < 0.003, "capped: {gaps:?}");
    assert_eq!(peer.status().failures, 18);
}

#[tokio::test(start_paused = true)]
async fn a_connection_that_lived_longer_than_five_seconds_resets_the_backoff() {
    // Connection 0 lives 6 s, then every connection is hung up on at once.
    let mut long = Script::new(V74);
    long.close_after = Some(Duration::from_secs(6));
    let mut broken = Script::new(V74);
    broken.close_before_hello = true;
    let net = Net::new();
    let server = listen(&net, sock(4500), vec![long, broken], 64 * 1024);
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);

    peer.connect().await.unwrap();
    until("the first connection ends", || peer.status().failures == 1).await;
    for _ in 0..2 {
        peer.connect().await.unwrap_err();
    }
    // Without the reset, the delay after the long connection would have grown from the initial
    // 50 ms to 60 ms and the next failure to 72 ms; with it, 60 ms then 72 ms apply to the
    // *next* attempts: connection 1 follows at once and connection 2 a delay of 60 ms later.
    let gaps = gaps(&server);
    assert!((gaps[0] - 6.0).abs() < 0.05, "{gaps:?}");
    assert!((gaps[1] - 0.06).abs() < 0.003, "{gaps:?}");
}

#[tokio::test(start_paused = true)]
async fn a_short_connection_does_not_reset_the_backoff() {
    let mut short = Script::new(V74);
    short.close_after = Some(Duration::from_secs(1));
    let mut broken = Script::new(V74);
    broken.close_before_hello = true;
    let net = Net::new();
    let server = listen(&net, sock(4500), vec![short, broken], 64 * 1024);
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);

    peer.connect().await.unwrap();
    until("the first connection ends", || peer.status().failures == 1).await;
    for _ in 0..2 {
        peer.connect().await.unwrap_err();
    }
    // 60 ms after the 1 s connection (already over), then 72 ms.
    let gaps = gaps(&server);
    assert!((gaps[0] - 1.0).abs() < 0.05, "{gaps:?}");
    assert!((gaps[1] - 0.072).abs() < 0.003, "{gaps:?}");
}

#[tokio::test(start_paused = true)]
async fn waiting_for_the_backoff_is_a_deadline_not_a_chain_of_sleeps() {
    // Commands sent during the wait do not restart or extend it.
    let net = Net::new();
    let mut script = Script::new(V74);
    script.close_before_hello = true;
    let server = listen_one(&net, sock(4500), script);
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap_err();
    peer.connect().await.unwrap_err();
    // The third attempt is due 72 ms after the second; ask for it repeatedly in between.
    let asking = {
        let peer = peer.clone();
        tokio::spawn(async move {
            for _ in 0..20 {
                let _ = peer.send(token(1), vec![1]).await;
            }
        })
    };
    asking.await.unwrap();
    let gaps = gaps(&server);
    assert!((gaps[1] - 0.072).abs() < 0.003, "{gaps:?}");
}

// ---- version detection ---------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn an_incompatible_cluster_is_reconnected_with_its_version() {
    let net = Net::new();
    let a = listen_one(&net, sock(4500), Script::new(V80));
    let b = listen_one(&net, sock(4501), Script::new(V80));
    let (mgr, mut events) = manager(&net, config());

    let pa = mgr.peer(&a.address);
    assert_eq!(pa.connect().await, Ok(V80));
    // The first guess (7.4, the first supported) was wrong; the second connection is right.
    assert_eq!(a.announced(), vec![Some(V74), Some(V80)]);
    assert_eq!(mgr.cluster_version(), Some(V80));
    let status = pa.status();
    assert_eq!(status.failures, 0, "not a failure: {status:?}");
    assert_eq!(status.known_version, Some(V80));
    assert_eq!(status.generation, Generation(2));
    assert!(!pa.is_failed());
    // No backoff delay: both connections were made at the same instant.
    assert!(secs(a.record(1).at - a.record(0).at) < 0.002);

    let seen: Vec<_> = drain(&mut events).into_iter().map(|(_, e)| e).collect();
    assert!(
        matches!(
            &seen[..],
            [
                PeerEvent::Closed {
                    generation: Generation(1),
                    error: PeerError::Connection(ConnectionError::Incompatible { .. }),
                    established: false
                },
                PeerEvent::Connected {
                    generation: Generation(2),
                    ..
                }
            ]
        ),
        "{seen:?}"
    );

    // Another address of the same cluster announces the cluster's version at once.
    let pb = mgr.peer(&b.address);
    assert_eq!(pb.connect().await, Ok(V80));
    assert_eq!(b.announced(), vec![Some(V80)]);
}

#[tokio::test(start_paused = true)]
async fn clusters_sharing_a_manager_guess_the_first_announcement_per_cluster() {
    let net = Net::new();
    let a1 = listen_one(&net, sock(4500), Script::new(V74));
    let a2 = listen_one(&net, sock(4501), Script::new(V74));
    let b1 = listen_one(&net, sock(4600), Script::new(V80));
    let b2 = listen_one(&net, sock(4601), Script::new(V80));
    let (mgr_a, _events) = manager(&net, config());
    let mgr_b = mgr_a.share().unwrap();
    assert!(mgr_a.set_cluster_key("a:1"));
    assert!(!mgr_a.set_cluster_key("other:2"), "set once");
    assert!(mgr_b.set_cluster_key("b:2"));

    assert_eq!(mgr_a.peer(&a1.address).connect().await, Ok(V74));
    assert_eq!(mgr_b.peer(&b1.address).connect().await, Ok(V80));
    // Each cluster's first connection guessed the manager's version at the time.
    assert_eq!(a1.announced(), vec![Some(V74)]);
    assert_eq!(b1.announced(), vec![Some(V74), Some(V80)]);
    // The manager-wide version is the last one negotiated (8.0), but a new address of cluster
    // A announces A's own version, and so does one of B: no reconnect for either.
    assert_eq!(mgr_a.cluster_version(), Some(V80));
    assert_eq!(mgr_a.peer(&a2.address).connect().await, Ok(V74));
    assert_eq!(a2.announced(), vec![Some(V74)]);
    assert_eq!(mgr_b.peer(&b2.address).connect().await, Ok(V80));
    assert_eq!(b2.announced(), vec![Some(V80)]);
}

#[tokio::test(start_paused = true)]
async fn a_wrong_first_guess_costs_a_reconnect_and_no_request() {
    let net = Net::new();
    let a1 = listen_one(&net, sock(4500), Script::echo(V74));
    let b1 = listen_one(&net, sock(4600), Script::echo(V80));
    let b2 = listen_one(&net, sock(4601), Script::echo(V80));
    let (mgr_a, _events) = manager(&net, config());
    let mgr_b = mgr_a.share().unwrap();
    // B names no cluster, so a new address of it guesses the manager's version: the last
    // negotiated, A's.
    mgr_a.set_cluster_key("a:1");
    assert_eq!(mgr_b.peer(&b1.address).connect().await, Ok(V80));
    assert_eq!(mgr_a.peer(&a1.address).connect().await, Ok(V74));
    assert_eq!(mgr_a.cluster_version(), Some(V74));
    let reply = mgr_b
        .peer(&b2.address)
        .request_as::<EchoReply, _>(token(7), echo_encode(5))
        .await
        .unwrap();
    assert_eq!(
        reply,
        EchoReply {
            value: 5 + ECHO_ADDS
        }
    );
    // The wrong guess (7.4) cost one reconnect; the request was sent once, on the right one.
    assert_eq!(b2.announced(), vec![Some(V74), Some(V80)]);
    assert_eq!(b2.echo_values(0), Vec::<u64>::new());
    assert_eq!(b2.echo_values(1), vec![5]);
}

#[tokio::test(start_paused = true)]
async fn the_preferred_version_is_the_first_supported_one() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::new(V74));
    let mut c = config();
    c.supported_versions = vec![V80, V74];
    let (mgr, _events) = manager(&net, c);
    assert_eq!(mgr.peer(&server.address).connect().await, Ok(V74));
    assert_eq!(server.announced(), vec![Some(V80), Some(V74)]);
}

#[tokio::test(start_paused = true)]
async fn an_unsupported_cluster_version_is_a_clear_error() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::new(V80));
    let mut c = config();
    c.supported_versions = vec![V74];
    let (mgr, _events) = manager(&net, c);
    let peer = mgr.peer(&server.address);
    let error = peer.connect().await.unwrap_err();
    assert_eq!(
        error,
        PeerError::UnsupportedVersion {
            peer: V80,
            supported: vec![V74]
        }
    );
    assert!(error.to_string().contains("not supported"), "{error}");
    assert_eq!(server.connections(), 1, "no pointless retry");
    assert_eq!(peer.status().failures, 1);
    assert_eq!(mgr.cluster_version(), None);
}

#[tokio::test(start_paused = true)]
async fn a_cluster_upgraded_under_a_running_client_is_followed() {
    let net = Net::new();
    let script = Script::new(V74);
    let live = script.version.clone();
    let mut server = listen_one(&net, sock(4500), script);
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    assert_eq!(peer.connect().await, Ok(V74));
    assert_eq!(server.announced(), vec![Some(V74)]);

    // The cluster restarts as 8.0.
    live.store(V80.0, Ordering::SeqCst);
    let conn = server.conns_next().await;
    conn.close();
    until("the old connection is noticed", || {
        peer.status().failures == 1
    })
    .await;
    assert_eq!(peer.connect().await, Ok(V80));
    assert_eq!(peer.version(), Some(V80));
    assert_eq!(mgr.cluster_version(), Some(V80));
    // 7.4 was announced once more, was told 8.0, and the third connection announced that.
    assert_eq!(server.announced(), vec![Some(V74), Some(V74), Some(V80)]);
}

// ---- failure state -------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn an_address_fails_after_four_seconds_of_failures_and_recovers_by_itself() {
    let net = Net::new(); // nothing listens: every connect is refused
    let address = na(sock(4500), false);
    let (mgr, _events) = manager(&net, config());
    let fm: FailureMonitor = mgr.failure_monitor().clone();
    let peer = mgr.peer(&address);
    // Only a referenced peer is probed once it has failed.
    let _reference = peer.reference();
    let t0 = tokio::time::Instant::now();

    assert!(!fm.is_failed(&address), "unknown addresses are available");
    let first_error = peer.connect().await.unwrap_err();
    assert_eq!(
        first_error,
        PeerError::Connect(io::ErrorKind::ConnectionRefused)
    );
    assert!(!fm.is_failed(&address), "one failure is not enough");

    // Keep asking: the address is marked failed once failures have lasted past 4 s.
    let waiter = {
        let fm = fm.clone();
        tokio::spawn(async move { fm.wait_address_failed(&address, true).await })
    };
    while !fm.is_failed(&address) {
        let _ = peer.connect().await;
    }
    let status = waiter.await.unwrap();
    assert!(status.failed);
    assert!(status.since.is_some());
    let elapsed = secs(tokio::time::Instant::now() - t0);
    assert!(
        (4.0..4.6).contains(&elapsed),
        "marked failed {elapsed} s after the first failure"
    );
    assert!(fm.address_status(&address).disconnects >= 5);
    assert!(peer.is_failed());

    // The server comes back. Nobody asks for anything: the failed peer is probed every second.
    let server = listen_one(&net, sock(4500), Script::new(V74));
    let recovered = fm.wait_address_failed(&address, false).await;
    assert!(!recovered.failed);
    assert_eq!(peer.status().phase, PeerPhase::Connected);
    assert_eq!(server.connections(), 1);
    assert!(secs(tokio::time::Instant::now() - t0) < 4.6 + 1.1 + 0.6);
}

#[tokio::test(start_paused = true)]
async fn an_available_peer_that_failed_is_probed_after_four_seconds() {
    let net = Net::new();
    let mut flaky = Script::new(V74);
    flaky.close_after = Some(Duration::from_millis(200));
    let server = listen(&net, sock(4500), vec![flaky, Script::new(V74)], 64 * 1024);
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    let _reference = peer.reference();
    peer.connect().await.unwrap();
    until("the first connection ends", || peer.status().failures == 1).await;
    assert!(!peer.is_failed(), "a single failure leaves it available");
    // Nothing asks for a connection, yet the peer reconnects about 4 s after the failure.
    until("the probe connects", || server.connections() == 2).await;
    let gap = gaps(&server)[0];
    assert!((4.0..4.5).contains(&gap), "{gap}");
    assert_eq!(peer.status().phase, PeerPhase::Connected);
}

#[tokio::test(start_paused = true)]
async fn an_unreferenced_failed_peer_is_not_probed() {
    let net = Net::new();
    let (mgr, _events) = manager(&net, config());
    let address = na(sock(4500), false);
    // Never referenced (C++'s peerReferences == -1): not probed after a failure.
    let never = mgr.peer(&address);
    never.connect().await.unwrap_err();
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(net.connects(sock(4500)), 1);
    assert_eq!(never.status().references, 0);

    // Referenced and released again (peerReferences == 0): neither.
    let other = na(sock(4501), false);
    let released = mgr.peer(&other);
    let reference = released.reference();
    assert_eq!(released.status().references, 1);
    released.connect().await.unwrap_err();
    drop(reference);
    assert_eq!(released.status().references, 0);
    let before = net.connects(sock(4501));
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(net.connects(sock(4501)), before);
}

/// A peer marked failed stops probing while nothing references it; referenced again (a
/// recovery put the proxies back on a host the client had given up on) it is probed again,
/// as C++ does (`connectionKeeper`'s `retryConnect` outlives the references, and a `Peer`
/// made anew resets the address to available). Otherwise the address stays failed for good
/// and everything waiting for it hangs. Found by the deterministic simulation
/// (`fdb-native-sim`, seed 294: GRV and commits waited forever for proxies on a restarted
/// process).
#[tokio::test(start_paused = true)]
async fn a_failed_peer_referenced_again_is_probed() {
    let net = Net::new(); // nothing listens yet
    let address = na(sock(4500), false);
    let (mgr, _events) = manager(&net, config());
    let fm: FailureMonitor = mgr.failure_monitor().clone();
    let peer = mgr.peer(&address);
    let reference = peer.reference();
    while !fm.is_failed(&address) {
        let _ = peer.connect().await;
    }
    // Released: the probes stop.
    drop(reference);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let before = net.connects(sock(4500));
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(net.connects(sock(4500)), before);

    // The server is back, and the peer is referenced again: it is probed and recovers.
    let server = listen_one(&net, sock(4500), Script::new(V74));
    let _again = peer.reference();
    let recovered = tokio::time::timeout(
        Duration::from_secs(5),
        fm.wait_address_failed(&address, false),
    )
    .await
    .expect("the re-referenced peer is probed within 5 s");
    assert!(!recovered.failed);
    assert_eq!(server.connections(), 1);
}

/// A peer made anew for an address marked failed (its old peer was removed: nothing used it)
/// makes the address available again, as C++'s `Peer::Peer` does
/// (`setStatus(destination, FailureStatus(false))`, FlowTransport 7.4:1104-1113). Otherwise
/// a caller that references the peer only while it looks for a live alternative (the
/// committer's read version for its dummy transaction) skips it, drops it, and the peer is
/// removed again before it could probe: the address stays failed for good. Found by the
/// deterministic simulation (`fdb-native-sim`, seed 294).
#[tokio::test(start_paused = true)]
async fn a_new_peer_makes_its_failed_address_available() {
    let net = Net::new(); // nothing listens yet
    let address = na(sock(4500), false);
    let (mgr, mut events) = manager(&net, config());
    let fm: FailureMonitor = mgr.failure_monitor().clone();
    {
        let peer = mgr.peer(&address);
        let _reference = peer.reference();
        while !fm.is_failed(&address) {
            let _ = peer.connect().await;
        }
    }
    // Unreferenced, unused: the peer is removed; the address stays marked failed.
    loop {
        if let PeerEventRecord {
            event: PeerEvent::Removed,
            ..
        } = events.next().await.expect("events")
        {
            break;
        }
    }
    assert!(fm.is_failed(&address));

    // The server is back. A new peer for the address: available, and requests go out.
    let _server = listen_one(&net, sock(4500), Script::echo(V74));
    let peer = mgr.peer(&address);
    assert!(!fm.is_failed(&address), "a new peer resets its address");
    let reply = peer
        .request_as::<EchoReply, _>(token(7), echo_encode(7))
        .await
        .unwrap();
    assert_eq!(reply.value, 7 + ECHO_ADDS);
}

#[tokio::test(start_paused = true)]
async fn endpoint_not_found_marks_the_endpoint_failed_for_good() {
    let net = Net::new();
    let mut server = listen_one(&net, sock(4500), Script::new(V74));
    let (mgr, mut events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();
    let fm = mgr.failure_monitor();

    let gone = Uid::new(0xdead_0000 | 1, 0xbeef);
    assert_eq!(
        fm.endpoint_status(&server.address, gone),
        EndpointStatus::Available
    );
    let waiter = {
        let fm = fm.clone();
        let address = server.address;
        tokio::spawn(async move { fm.on_state_changed(&address, gone).await })
    };
    let conn = server.conns_next().await;
    conn.send(
        well_known::ENDPOINT_NOT_FOUND,
        fdb_native_wire::encode(&gone).unwrap(),
    );
    assert_eq!(
        waiter.await.unwrap(),
        EndpointStatus::EndpointFailed(EndpointFailure::NotFound)
    );
    assert!(
        drain(&mut events)
            .iter()
            .any(|(_, e)| matches!(e, PeerEvent::EndpointNotFound { token } if *token == gone))
    );
    // A request to it is refused without being sent, and forgetting the mark allows it again.
    let frames = server.frames(0);
    assert_eq!(
        peer.request(gone, echo_encode(1)).await,
        Err(RequestError::EndpointNotFound {
            token: gone,
            maybe_delivered: false
        })
    );
    assert_eq!(server.frames(0), frames);
    // The rest of the address is fine, and the mark survives a reconnect.
    assert!(!fm.is_failed(&server.address));
    assert!(!fm.endpoint_status(&server.address, token(1)).is_failed());
    peer.close_connection();
    until("reconnects", || peer.status().phase != PeerPhase::Connected).await;
    assert!(fm.endpoint_status(&server.address, gone).is_failed());
    // The request layer can mark endpoints itself, and forget them.
    let other = Uid::new(0xdead_0000 | 1, 0xf00d);
    assert!(fm.endpoint_not_found(&server.address, other));
    assert!(fm.forget_endpoint(&server.address, other));
}

#[tokio::test(start_paused = true)]
async fn commands_without_a_connection_are_refused_as_not_sent() {
    let net = Net::new();
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&na(sock(4500), false));
    let error = peer.send(token(1), vec![1]).await.unwrap_err();
    assert_eq!(
        error,
        SendError::NotSent(PeerError::Connect(io::ErrorKind::ConnectionRefused))
    );
    let error = peer.register(token(2)).await.unwrap_err();
    assert!(matches!(error, SendError::NotSent(_)), "{error:?}");
    // A request that was never put on a connection is safe to resend.
    let error = peer.request(token(3), echo_encode(1)).await.unwrap_err();
    assert_eq!(error, RequestError::NotSent(None));
    assert!(error.certainly_not_delivered() && error.should_retry());
}

// ---- timeouts ------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_tcp_connect_is_bounded_by_two_seconds() {
    let net = Net::new();
    net.set(sock(4500), Behavior::Hang);
    let (mgr, mut events) = manager(&net, config());
    let peer = mgr.peer(&na(sock(4500), false));
    let t0 = tokio::time::Instant::now();
    let error = peer.connect().await.unwrap_err();
    assert_eq!(
        error,
        PeerError::ConnectTimeout {
            after: Duration::from_secs(2)
        }
    );
    let elapsed = secs(tokio::time::Instant::now() - t0);
    assert!((2.0..2.01).contains(&elapsed), "{elapsed}");
    assert!(drain(&mut events).iter().any(|(_, e)| matches!(
        e,
        PeerEvent::ConnectFailed {
            error: PeerError::ConnectTimeout { .. }
        }
    )));
}

/// `persistentConnectFailedCount`: every failed dial (refused, timed out) counts, a failed
/// FlowTransport handshake after a connect does not, and an address that stops failing is
/// forgotten on a failure once the TTL has passed.
#[tokio::test(start_paused = true)]
async fn failed_connects_are_counted_per_address() {
    let net = Net::new();
    let refused = na(sock(4500), false);
    net.set(sock(4501), Behavior::Hang);
    let hangs = na(sock(4501), false);
    let mut script = Script::new(V74);
    script.hello = false;
    let silent = listen_one(&net, sock(4502), script);
    let mut c = config();
    c.connect_failed_count_ttl = Duration::from_secs(600);
    let (mgr, _events) = manager(&net, c);
    let fm = mgr.failure_monitor().clone();
    let count = |a: &NetworkAddress| {
        fm.connect_failures()
            .into_iter()
            .find(|(x, _)| x == a)
            .map(|(_, n)| n)
    };
    for _ in 0..3 {
        let _ = mgr.peer(&refused).connect().await.unwrap_err();
    }
    let _ = mgr.peer(&hangs).connect().await.unwrap_err();
    let _ = mgr.peer(&silent.address).connect().await.unwrap_err();
    assert_eq!(count(&refused), Some(3));
    assert_eq!(count(&hangs), Some(1));
    assert_eq!(
        count(&silent.address),
        None,
        "it connected; the handshake failed"
    );
    // Ten minutes later, the next failure forgets the addresses that have not failed since.
    tokio::time::sleep(Duration::from_secs(601)).await;
    let _ = mgr.peer(&hangs).connect().await.unwrap_err();
    assert_eq!(count(&refused), None);
    assert_eq!(count(&hangs), Some(2), "it just failed again");
}

#[tokio::test(start_paused = true)]
async fn a_silent_server_hits_the_handshake_timeout() {
    let net = Net::new();
    let mut script = Script::new(V74);
    script.hello = false;
    let server = listen_one(&net, sock(4500), script);
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    let t0 = tokio::time::Instant::now();
    let error = peer.connect().await.unwrap_err();
    assert_eq!(
        error,
        PeerError::Connection(ConnectionError::HandshakeTimeout(Duration::from_secs(3)))
    );
    let elapsed = secs(tokio::time::Instant::now() - t0);
    assert!((3.0..3.01).contains(&elapsed), "{elapsed}");
}

// ---- backpressure --------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn send_waits_while_the_peer_does_not_read_and_resumes_when_it_does() {
    let net = Net::new();
    let mut script = Script::new(V74);
    script.read = false;
    let mut server = listen(&net, sock(4500), vec![script], 256);
    let mut c = config();
    c.max_pending_output = 4096;
    quiet_monitor(&mut c);
    c.handshake_timeout = Duration::from_secs(10_000);
    let (mgr, _events) = manager(&net, c);
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();

    let accepted = Arc::new(AtomicUsize::new(0));
    let sender = {
        let peer = peer.clone();
        let accepted = accepted.clone();
        tokio::spawn(async move {
            for i in 0..40 {
                peer.send(token(i), vec![7; 1000]).await.unwrap();
                accepted.fetch_add(1, Ordering::SeqCst);
            }
        })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    let stuck_at = accepted.load(Ordering::SeqCst);
    assert!(
        (3..=8).contains(&stuck_at),
        "output limit of 4096 bytes accepts a few 1000-byte frames, not {stuck_at}"
    );
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(accepted.load(Ordering::SeqCst), stuck_at, "still waiting");
    assert!(!sender.is_finished());

    // The server starts reading: everything drains.
    server.conns_next().await.start_reading();
    sender.await.unwrap();
    assert_eq!(accepted.load(Ordering::SeqCst), 40);
    until("the server has every frame", || server.frames(0) == 40).await;
}

#[tokio::test(start_paused = true)]
async fn a_dropped_send_is_not_sent() {
    let net = Net::new();
    let mut script = Script::new(V74);
    script.read = false;
    let mut server = listen(&net, sock(4500), vec![script], 256);
    let mut c = config();
    c.max_pending_output = 2048;
    quiet_monitor(&mut c);
    let (mgr, _events) = manager(&net, c);
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();
    // Fill the output, then give up on more sends: they never leave the queue.
    let mut filled = 0;
    loop {
        let send = peer.send(token(filled), vec![7; 1000]);
        match tokio::time::timeout(Duration::from_millis(100), send).await {
            Ok(result) => {
                result.unwrap();
                filled += 1;
            }
            Err(_) => break,
        }
    }
    assert!(filled >= 2, "{filled}");
    assert_eq!(peer.queued_commands(), 0, "the dropped send left the queue");
    for i in 0..3 {
        let send = peer.send(token(1000 + i), vec![0xEE; 1000]);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), send)
                .await
                .is_err()
        );
    }
    assert_eq!(server.frames(0), 0, "the server has read nothing");
    // Now the server reads everything that was sent: exactly the accepted frames, and none
    // of the abandoned ones.
    server.conns_next().await.start_reading();
    until("the server read the accepted frames", || {
        server.frames(0) == filled as usize
    })
    .await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(server.frames(0), filled as usize);
    let frames = server.record(0).frames;
    assert!(
        frames.iter().all(|f| f.payload.iter().all(|b| *b == 7)),
        "an abandoned frame was sent"
    );
    assert!(
        frames
            .iter()
            .all(|f| f.token.first == token(0).first && f.token.second < 1000)
    );
}

// ---- idle and unreferenced -----------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn an_unreferenced_connection_closes_cleanly_and_a_send_reopens_it() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::new(V74));
    let (mgr, mut events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    let reference = peer.reference();
    peer.connect().await.unwrap();
    drop(reference);

    until("the connection is closed", || {
        peer.status().phase == PeerPhase::Idle
    })
    .await;
    let status = peer.status();
    assert_eq!(
        status.failures, 0,
        "a clean close is no failure: {status:?}"
    );
    assert!(!peer.is_failed());
    assert_eq!(
        mgr.failure_monitor()
            .address_status(&server.address)
            .disconnects,
        1
    );
    let closed = drain(&mut events)
        .into_iter()
        .find_map(|(_, e)| match e {
            PeerEvent::Closed { error, .. } => Some(error),
            _ => None,
        })
        .expect("a Closed event");
    assert!(closed.is_clean(), "{closed:?}");
    assert_eq!(closed, PeerError::Connection(ConnectionError::Unreferenced));

    // Nothing probes an idle, clean peer; the next use connects again (generation 2).
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(server.connections(), 1);
    let _reference = peer.reference();
    assert_eq!(peer.send(token(1), vec![1]).await, Ok(Generation(2)));
    assert_eq!(server.connections(), 2);
}

// ---- TLS hook ------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn tls_addresses_go_through_the_stream_wrapper_and_plain_ones_do_not() {
    let net = Net::new();
    let mut tls_script = Script::new(V74);
    tls_script.tls = true;
    let tls_server = listen_one(&net, sock(4501), tls_script);
    let plain_server = listen_one(&net, sock(4500), Script::new(V74));
    let wrapped = Arc::new(AtomicUsize::new(0));
    let counter = wrapped.clone();
    let wrapper = move |stream: MemStream, peer: NetworkAddress| {
        let counter = counter.clone();
        async move {
            assert!(peer.is_tls());
            counter.fetch_add(1, Ordering::SeqCst);
            Ok::<_, io::Error>(stream)
        }
    };
    let (sink, _events) = event_channel();
    let mgr = PeerManager::with_stream_wrapper(MemRuntime::new(&net), config(), wrapper, sink);

    let t = mgr.peer(&tls_server.address);
    assert!(tls_server.address.is_tls());
    assert_eq!(t.connect().await, Ok(V74));
    t.send(token(1), vec![1, 2, 3]).await.unwrap();
    until("the TLS server read a frame", || tls_server.frames(0) == 1).await;
    assert_eq!(wrapped.load(Ordering::SeqCst), 1);

    assert_eq!(mgr.peer(&plain_server.address).connect().await, Ok(V74));
    assert_eq!(wrapped.load(Ordering::SeqCst), 1, "not for a plain address");
    // The plain address and the TLS address of one host are different peers.
    assert_ne!(
        mgr.peer(&na(sock(4500), true)).address().is_tls(),
        mgr.peer(&na(sock(4500), false)).address().is_tls()
    );
}

#[tokio::test(start_paused = true)]
async fn a_tls_address_without_a_wrapper_fails_without_dialing() {
    let net = Net::new();
    let mut tls_script = Script::new(V74);
    tls_script.tls = true;
    let server = listen_one(&net, sock(4500), tls_script);
    let (mgr, _events) = manager(&net, config());
    let error = mgr.peer(&server.address).connect().await.unwrap_err();
    assert_eq!(error, PeerError::TlsNotConfigured);
    assert_eq!(net.connects(sock(4500)), 0);
}

#[tokio::test(start_paused = true)]
async fn a_stuck_or_failing_tls_handshake_is_reported() {
    let net = Net::new();
    let mut tls_script = Script::new(V74);
    tls_script.tls = true;
    let server = listen_one(&net, sock(4500), tls_script);
    let (sink, _events) = event_channel();
    let stuck = |_s: MemStream, _p: NetworkAddress| async move {
        std::future::pending::<io::Result<MemStream>>().await
    };
    let mgr = PeerManager::with_stream_wrapper(MemRuntime::new(&net), config(), stuck, sink);
    assert_eq!(
        mgr.peer(&server.address).connect().await,
        Err(PeerError::TlsTimeout {
            after: Duration::from_secs(2)
        })
    );
    let (sink, _events) = event_channel();
    let failing = |_s: MemStream, _p: NetworkAddress| async move {
        Err::<MemStream, _>(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad certificate",
        ))
    };
    let mgr = PeerManager::with_stream_wrapper(MemRuntime::new(&net), config(), failing, sink);
    match mgr.peer(&server.address).connect().await {
        Err(PeerError::Tls {
            kind,
            message,
            source,
        }) => {
            assert_eq!(kind, io::ErrorKind::InvalidData);
            assert!(message.contains("bad certificate"));
            assert_eq!(source.kind(), io::ErrorKind::InvalidData);
            assert_eq!(source.to_string(), message);
        }
        other => panic!("{other:?}"),
    }
}

/// `TLS_CLIENT_CONNECTION_THROTTLE_*`, as `SSLConnection::connect` (`flow/Net2.actor.cpp`
/// 7.4:927-946): from the second failed handshake with an address within 11 s of the first,
/// connects to it do not dial (they wait the connect timeout and fail) until the window ends.
#[tokio::test(start_paused = true)]
async fn repeated_tls_handshake_failures_throttle_dialing_like_a_cpp_client() {
    let net = Net::new();
    let mut tls_script = Script::new(V74);
    tls_script.tls = true;
    let server = listen_one(&net, sock(4500), tls_script);
    let wraps = Arc::new(AtomicUsize::new(0));
    let counter = wraps.clone();
    let failing = move |_s: MemStream, _p: NetworkAddress| {
        counter.fetch_add(1, Ordering::SeqCst);
        async move {
            Err::<MemStream, _>(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad certificate",
            ))
        }
    };
    let (sink, _events) = event_channel();
    let mgr = PeerManager::with_stream_wrapper(MemRuntime::new(&net), config(), failing, sink);
    let peer = mgr.peer(&server.address);
    let start = tokio::time::Instant::now();

    for n in 1..=2 {
        let error = peer.connect().await.unwrap_err();
        assert!(matches!(error, PeerError::Tls { .. }), "{n}: {error:?}");
        assert_eq!(net.connects(sock(4500)), n);
        assert_eq!(wraps.load(Ordering::SeqCst), n);
    }
    // Throttled: no dial, no handshake, a connect timeout's wait.
    let before = tokio::time::Instant::now();
    assert_eq!(peer.connect().await, Err(PeerError::TlsThrottled));
    assert!(before.elapsed() >= Duration::from_secs(2));
    assert_eq!(net.connects(sock(4500)), 2);
    assert_eq!(wraps.load(Ordering::SeqCst), 2);
    assert!(start.elapsed() < Duration::from_secs(11));

    // The window (11 s from the first failure) ends: dialed again.
    tokio::time::sleep_until(start + Duration::from_secs(12)).await;
    let error = peer.connect().await.unwrap_err();
    assert!(matches!(error, PeerError::Tls { .. }), "{error:?}");
    assert_eq!(net.connects(sock(4500)), 3);

    // Another address (here: another port of the host) is not throttled.
    let mut other = Script::new(V74);
    other.tls = true;
    let other = listen_one(&net, sock(4501), other);
    let error = mgr.peer(&other.address).connect().await.unwrap_err();
    assert!(matches!(error, PeerError::Tls { .. }), "{error:?}");
    assert_eq!(net.connects(sock(4501)), 1);
}

/// A handshake that times out does not count towards the throttle (C++'s outer connect
/// timeout cancels it before it can record anything).
#[tokio::test(start_paused = true)]
async fn tls_handshake_timeouts_do_not_throttle() {
    let net = Net::new();
    let mut tls_script = Script::new(V74);
    tls_script.tls = true;
    let server = listen_one(&net, sock(4500), tls_script);
    let (sink, _events) = event_channel();
    let stuck = |_s: MemStream, _p: NetworkAddress| async move {
        std::future::pending::<io::Result<MemStream>>().await
    };
    let mgr = PeerManager::with_stream_wrapper(MemRuntime::new(&net), config(), stuck, sink);
    let peer = mgr.peer(&server.address);
    for n in 1..=4 {
        let error = peer.connect().await.unwrap_err();
        assert!(matches!(error, PeerError::TlsTimeout { .. }), "{error:?}");
        assert_eq!(net.connects(sock(4500)), n);
    }
}

#[tokio::test(start_paused = true)]
async fn choose_primary_address_prefers_the_plain_address_like_a_cpp_client() {
    let net = Net::new();
    let plain = na(sock(4500), false);
    let tls = na(sock(4501), true);
    let list = NetworkAddressList {
        address: tls,
        secondary_address: Some(plain),
    };
    let (mgr, _events) = manager(&net, config());
    assert_eq!(mgr.primary_address(&list), plain, "no TLS configured");
    let wrapper = |s: MemStream, _p: NetworkAddress| async move { Ok::<_, io::Error>(s) };
    // A libfdb_c client never sets a local address, so even one that has TLS configured
    // prefers the plain address of an endpoint that has both ...
    let (sink, _events) = event_channel();
    let tls_mgr = PeerManager::with_stream_wrapper(MemRuntime::new(&net), config(), wrapper, sink);
    assert_eq!(tls_mgr.primary_address(&list), plain);
    // ... unless it is asked to prefer TLS.
    let (sink, _events) = event_channel();
    let mut c = config();
    c.prefer_tls = true;
    let prefers = PeerManager::with_stream_wrapper(MemRuntime::new(&net), c, wrapper, sink);
    assert_eq!(prefers.primary_address(&list), tls);
    // An endpoint with only a TLS address is connected to over TLS either way.
    let only_tls = NetworkAddressList {
        address: tls,
        secondary_address: None,
    };
    assert_eq!(tls_mgr.primary_address(&only_tls), tls);
    assert_eq!(mgr.primary_address(&only_tls), tls);
    let endpoint = fdb_native_wire::Endpoint::new(list, token(3));
    assert_eq!(mgr.peer_for_endpoint(&endpoint).address(), &plain);
    assert_eq!(tls_mgr.peer_for_endpoint(&endpoint).address(), &plain);
    assert_eq!(prefers.peer_for_endpoint(&endpoint).address(), &tls);
}

// ---- shutdown ------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn shutdown_closes_every_peer_and_leaves_no_tasks() {
    let net = Net::new();
    let a = listen_one(&net, sock(4500), Script::new(V74));
    let b = listen_one(&net, sock(4501), Script::new(V74));
    net.set(sock(4502), Behavior::Hang);
    let baseline = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();

    let (mgr, mut events) = manager(&net, config());
    let pa = mgr.peer(&a.address);
    let pb = mgr.peer(&b.address);
    let hanging = mgr.peer(&na(sock(4502), false));
    let idle = mgr.peer(&na(sock(4503), false));
    pa.connect().await.unwrap();
    pb.connect().await.unwrap();
    pa.send(token(1), vec![1, 2, 3]).await.unwrap();
    let waiting = {
        let hanging = hanging.clone();
        tokio::spawn(async move { hanging.connect().await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
            > baseline
    );

    mgr.shutdown().await;
    assert!(mgr.is_shut_down());
    // The frame that was queued got written, and both servers saw their connection closed.
    assert_eq!(a.frames(0), 1);
    until("servers see the close", || {
        a.record(0).closed_at.is_some() && b.record(0).closed_at.is_some()
    })
    .await;
    assert_eq!(waiting.await.unwrap(), Err(PeerError::Shutdown));

    let closed: Vec<_> = drain(&mut events)
        .into_iter()
        .filter_map(|(addr, e)| match e {
            PeerEvent::Closed { error, .. } => Some((addr, error)),
            _ => None,
        })
        .collect();
    assert_eq!(closed.len(), 2, "{closed:?}");
    for (_, error) in &closed {
        assert_eq!(error, &PeerError::Connection(ConnectionError::LocalClose));
        assert!(error.is_clean());
    }
    assert_eq!(pa.status().phase, PeerPhase::Shutdown);
    assert_eq!(idle.status().phase, PeerPhase::Shutdown);
    assert!(!pa.is_failed(), "a shutdown is no failure");

    // Handles stay valid and refuse everything; new peers are dead on arrival.
    assert_eq!(pa.send(token(2), vec![1]).await, Err(SendError::Shutdown));
    assert_eq!(pa.register(token(3)).await, Err(SendError::Shutdown));
    assert_eq!(pa.connect().await, Err(PeerError::Shutdown));
    let late = mgr.peer(&na(sock(4504), false));
    assert_eq!(late.connect().await, Err(PeerError::Shutdown));
    assert_eq!(net.connects(sock(4504)), 0);
    mgr.shutdown().await; // idempotent

    tokio::task::yield_now().await;
    assert_eq!(
        tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks(),
        baseline,
        "every peer task ended"
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_refuses_commands_waiting_in_backoff() {
    let net = Net::new();
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&na(sock(4500), false));
    peer.connect().await.unwrap_err();
    // The next attempt is 60 ms away; a send waits for it.
    let waiting = {
        let peer = peer.clone();
        tokio::spawn(async move { peer.send(token(1), vec![1]).await })
    };
    tokio::task::yield_now().await;
    mgr.shutdown().await;
    assert_eq!(waiting.await.unwrap(), Err(SendError::Shutdown));
}

#[tokio::test(start_paused = true)]
async fn dropping_the_last_manager_shuts_the_peers_down() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::new(V74));
    let baseline = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let peer = {
        let (mgr, _events) = manager(&net, config());
        let clone = mgr.clone();
        let peer = mgr.peer(&server.address);
        peer.connect().await.unwrap();
        drop(mgr);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(peer.status().phase, PeerPhase::Connected, "a clone is left");
        drop(clone);
        peer
    };
    until("the peer task ends", || {
        tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
            == baseline
    })
    .await;
    until("the server sees the close", || {
        server.record(0).closed_at.is_some()
    })
    .await;
    assert_eq!(peer.status().phase, PeerPhase::Shutdown);
    assert_eq!(peer.send(token(1), vec![1]).await, Err(SendError::Shutdown));
}

// ---- the real tokio adapter over loopback TCP ----------------------------------------------

#[tokio::test]
async fn tcp_loopback_end_to_end() {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let script = Script::echo(V80);
    let accepted = records.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let script = script.clone();
            let accepted = accepted.clone();
            let index = {
                let mut r = accepted.lock().unwrap();
                r.push(ConnRecord {
                    at: tokio::time::Instant::now(),
                    announced: None,
                    connection_id: 0,
                    frames: Vec::new(),
                    pings: 0,
                    closed_at: None,
                });
                r.len() - 1
            };
            let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
            tokio::spawn(async move {
                let _keep = _tx;
                serve(stream, script.clone(), accepted.clone(), index, rx, addr).await
            });
        }
    });

    let (sink, _events) = event_channel();
    let mgr: PeerManager<TokioRuntime, NoTls> =
        PeerManager::new(TokioRuntime::new(), config(), sink);
    let peer = mgr.peer(&na(addr, false));
    // Version detection happens over a real socket too.
    assert_eq!(peer.connect().await, Ok(V80));
    assert_eq!(records.lock().unwrap().len(), 2);
    let mut requests = Vec::new();
    for value in 0..40u64 {
        let peer = peer.clone();
        requests.push(tokio::spawn(async move {
            peer.request_as::<EchoReply, _>(token(5), echo_encode(value))
                .await
        }));
    }
    for (value, request) in requests.into_iter().enumerate() {
        let reply = tokio::time::timeout(Duration::from_secs(5), request)
            .await
            .expect("a reply arrived")
            .unwrap()
            .unwrap();
        assert_eq!(reply.value, value as u64 + ECHO_ADDS);
    }
    mgr.shutdown().await;
}
