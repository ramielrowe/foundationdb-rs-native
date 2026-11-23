//! The peer manager against the findings of its review: fairness between reading and writing,
//! a connect demand that survives a local close, a command queue that stays bounded, peers that
//! are removed, requests across connection failures, and the registry as the one request/reply
//! mechanism. In-memory tests run on tokio's paused clock; the fairness tests run over real
//! loopback TCP on a multi-threaded runtime.

#![cfg(feature = "tokio")]

mod common;

use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use fdb_native_transport::peer::{
    EventReceiver, NoTls, PeerError, PeerEvent, PeerManager, PeerManagerConfig, PeerPhase,
    SendError, event_channel,
};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_transport::{Checksum, ConnectPacket, ConnectionError, FrameConfig};
use fdb_native_wire::{NetworkAddress, ProtocolVersion, Uid};
use futures_lite::future::poll_once;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const V74: ProtocolVersion = ProtocolVersion::V7_4;
const V80: ProtocolVersion = ProtocolVersion::V8_0;

fn config() -> PeerManagerConfig {
    PeerManagerConfig::default().without_jitter()
}

fn manager(net: &Arc<Net>, config: PeerManagerConfig) -> (PeerManager<MemRuntime>, EventReceiver) {
    let (sink, events) = event_channel();
    (PeerManager::new(MemRuntime::new(net), config, sink), events)
}

fn token(n: u64) -> Uid {
    Uid::new(0x5eed_0000_0000_0000 | 1, n)
}

fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

fn frame(token: Uid, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    FrameConfig::new(Checksum::for_tls(false))
        .encode_frame(token, payload, &mut out)
        .unwrap();
    out
}

fn events_of(events: &mut EventReceiver) -> Vec<PeerEvent> {
    std::iter::from_fn(|| events.try_next())
        .map(|record| record.event)
        .collect()
}

// ---- a connect demand survives a local close -----------------------------------------------

#[tokio::test(start_paused = true)]
async fn connect_is_retried_when_the_handshake_is_closed_locally() {
    let net = Net::new();
    let mut silent = Script::new(V74);
    silent.hello = false; // the first connection never answers
    let server = listen(&net, sock(4500), vec![silent, Script::new(V74)], 64 * 1024);
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    let connecting = {
        let peer = peer.clone();
        tokio::spawn(async move { peer.connect().await })
    };
    until("dialled", || server.connections() == 1).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(peer.status().phase, PeerPhase::Connecting);

    peer.close_connection();
    let result = tokio::time::timeout(Duration::from_secs(60), connecting)
        .await
        .expect("connect() hung after a local close during the handshake")
        .unwrap();
    assert_eq!(result, Ok(V74));
    assert_eq!(server.connections(), 2);
    assert_eq!(peer.status().failures, 0, "a local close is no failure");
}

#[tokio::test(start_paused = true)]
async fn connect_to_a_silent_server_still_ends_with_the_handshake_timeout() {
    let net = Net::new();
    let mut silent = Script::new(V74);
    silent.hello = false;
    let server = listen_one(&net, sock(4500), silent);
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    let connecting = {
        let peer = peer.clone();
        tokio::spawn(async move { peer.connect().await })
    };
    until("dialled", || server.connections() == 1).await;
    peer.close_connection(); // retried, and the retry times out like any attempt
    let result = tokio::time::timeout(Duration::from_secs(60), connecting)
        .await
        .expect("connect() hung")
        .unwrap();
    assert!(
        matches!(
            result,
            Err(PeerError::Connection(ConnectionError::HandshakeTimeout(_)))
        ),
        "{result:?}"
    );
}

// ---- the command queue is bounded ----------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn timed_out_sends_neither_accumulate_nor_dial_for_departed_callers() {
    let net = Net::new();
    net.set(sock(4500), Behavior::Hang); // the dial takes its whole 2 s
    let mut c = config();
    c.command_queue_len = 4;
    let (mgr, _events) = manager(&net, c);
    let peer = mgr.peer(&na(sock(4500), false));

    let peak = Arc::new(AtomicUsize::new(0));
    let mut callers = Vec::new();
    for i in 0..2000u64 {
        let peer = peer.clone();
        let peak = peak.clone();
        callers.push(tokio::spawn(async move {
            // Poll once (it queues), then give up after 1 ms.
            let sent = tokio::time::timeout(
                Duration::from_millis(1),
                peer.send(token(i), vec![0u8; 1024]),
            )
            .await;
            peak.fetch_max(peer.queued_commands(), Ordering::SeqCst);
            sent.is_err()
        }));
    }
    for caller in callers {
        assert!(caller.await.unwrap(), "every send timed out");
    }
    assert!(peak.load(Ordering::SeqCst) <= 4, "{peak:?}");
    assert_eq!(peer.queued_commands(), 0, "abandoned sends left the queue");

    // Nobody is waiting: the one attempt that was started ends, and nothing dials again.
    tokio::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(net.connects(sock(4500)), 1);
    // A fresh send is not stuck behind the abandoned ones.
    let error = peer.send(token(1), vec![1]).await.unwrap_err();
    assert!(
        matches!(error, SendError::NotSent(PeerError::ConnectTimeout { .. })),
        "{error:?}"
    );
    assert_eq!(net.connects(sock(4500)), 2);
}

#[tokio::test(start_paused = true)]
async fn a_dead_peer_refuses_every_queued_send_after_one_attempt() {
    let net = Net::new();
    net.set(sock(4500), Behavior::Hang);
    let mut c = config();
    c.command_queue_len = 8;
    let (mgr, _events) = manager(&net, c);
    let peer = mgr.peer(&na(sock(4500), false));
    let t0 = tokio::time::Instant::now();
    let mut callers = Vec::new();
    for i in 0..64u64 {
        let peer = peer.clone();
        callers.push(tokio::spawn(async move {
            let sent = peer.send(token(i), vec![0u8; 16]).await;
            (sent, tokio::time::Instant::now())
        }));
    }
    let mut latest = t0;
    for caller in callers {
        let (sent, at) = caller.await.unwrap();
        assert!(
            matches!(
                sent,
                Err(SendError::NotSent(PeerError::ConnectTimeout { .. }))
            ),
            "{sent:?}"
        );
        latest = latest.max(at);
    }
    assert_eq!(net.connects(sock(4500)), 1, "one attempt refused all 64");
    assert!(secs(latest - t0) < 2.1, "{:?}", latest - t0);
}

#[tokio::test(start_paused = true)]
async fn requests_that_wait_for_a_dead_peer_fail_as_not_sent() {
    let net = Net::new();
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&na(sock(4500), false));
    let mut callers = Vec::new();
    for value in 0..10u64 {
        let peer = peer.clone();
        callers.push(tokio::spawn(async move {
            peer.request(token(1), echo_encode(value)).await
        }));
    }
    for caller in callers {
        let error = caller.await.unwrap().unwrap_err();
        assert_eq!(error, RequestError::NotSent(None));
        assert!(error.certainly_not_delivered());
    }
    assert_eq!(net.connects(sock(4500)), 1);
}

// ---- requests across connection failures ---------------------------------------------------

#[tokio::test(start_paused = true)]
async fn queued_requests_survive_a_version_reconnect_but_not_a_failed_handshake() {
    let net = Net::new();
    // The cluster is 8.0; the first guess is 7.4.
    let server = listen_one(&net, sock(4500), Script::echo(V80));
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    let versions = Arc::new(Mutex::new(Vec::new()));
    let mut callers = Vec::new();
    for value in 0..5u64 {
        let peer = peer.clone();
        let versions = versions.clone();
        callers.push(tokio::spawn(async move {
            peer.request_as::<EchoReply, _>(token(1), move |reply, version, out| {
                versions.lock().unwrap().push(version);
                echo_encode(value)(reply, version, out)
            })
            .await
        }));
    }
    for (value, caller) in callers.into_iter().enumerate() {
        let reply = caller.await.unwrap().unwrap();
        assert_eq!(reply.value, value as u64 + ECHO_ADDS);
    }
    assert_eq!(server.announced(), vec![Some(V74), Some(V80)]);
    assert_eq!(
        *versions.lock().unwrap(),
        vec![V80; 5],
        "every request encoded for 8.0"
    );
    assert_eq!(server.echo_values(0), Vec::<u64>::new());
    assert_eq!(server.echo_values(1).len(), 5);

    // A handshake that fails outright (the peer hangs up) is not retried for them: they
    // were never sent, and say so.
    let net = Net::new();
    let mut broken = Script::new(V74);
    broken.close_before_hello = true;
    let server = listen(&net, sock(4500), vec![broken, Script::echo(V74)], 64 * 1024);
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    let called = Arc::new(AtomicBool::new(false));
    let seen = called.clone();
    let error = peer
        .request(token(1), move |reply, version, out| {
            seen.store(true, Ordering::SeqCst);
            echo_encode(1)(reply, version, out)
        })
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            RequestError::NotSent(Some(ConnectionError::PeerClosed {
                during_handshake: true
            }))
        ),
        "{error:?}"
    );
    assert!(error.certainly_not_delivered() && error.should_retry());
    assert!(!called.load(Ordering::SeqCst), "never encoded, never sent");
    assert_eq!(server.frames(0), 0);
    // Retrying works.
    assert_eq!(
        peer.request_as::<EchoReply, _>(token(1), echo_encode(2))
            .await
            .unwrap()
            .value,
        2 + ECHO_ADDS
    );
}

#[tokio::test(start_paused = true)]
async fn replies_complete_before_the_connection_is_reported_closed() {
    let net = Net::new();
    let mut server = listen_one(&net, sock(4500), Script::echo(V74));
    let (mgr, mut events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();

    let mut answered = Box::pin(peer.request(token(1), echo_encode(1)));
    let mut unanswered = Box::pin(peer.request(token(1), echo_encode(SILENT)));
    assert!(poll_once(&mut answered).await.is_none());
    assert!(poll_once(&mut unanswered).await.is_none());
    until("the server has both requests", || server.frames(0) == 2).await;
    server.conns_next().await.close();

    loop {
        let record = events.next().await.expect("events");
        if matches!(record.event, PeerEvent::Closed { .. }) {
            break;
        }
    }
    // Both futures completed *before* Closed was reported: the reply as an Ok, the other as
    // maybe delivered.
    let first = poll_once(&mut answered)
        .await
        .expect("completed before Closed")
        .expect("the reply");
    assert_eq!(
        fdb_native_transport::rpc::decode_reply::<EchoReply>(&first),
        Ok(EchoReply {
            value: 1 + ECHO_ADDS
        })
    );
    let second = poll_once(&mut unanswered)
        .await
        .expect("completed before Closed")
        .unwrap_err();
    assert!(
        matches!(second, RequestError::MaybeDelivered(Some(_))),
        "{second:?}"
    );
    assert!(second.maybe_delivered() && !second.should_retry());
}

#[tokio::test(start_paused = true)]
async fn closing_a_connection_completes_what_it_carried_and_the_peer_reconnects() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();
    let pending = {
        let peer = peer.clone();
        tokio::spawn(async move { peer.request(token(1), echo_encode(SILENT)).await })
    };
    until("it is on the wire", || server.frames(0) == 1).await;
    peer.close_connection();
    assert_eq!(
        pending.await.unwrap(),
        Err(RequestError::MaybeDelivered(Some(
            ConnectionError::LocalClose
        )))
    );
    // The next request reconnects, with a registry that is clean.
    let reply = peer
        .request_as::<EchoReply, _>(token(1), echo_encode(4))
        .await
        .unwrap();
    assert_eq!(reply.value, 4 + ECHO_ADDS);
    assert_eq!(server.connections(), 2);
    assert_eq!(peer.status().failures, 0);
}

#[tokio::test(start_paused = true)]
async fn a_dropped_request_is_cancelled_and_its_late_reply_is_an_unknown_token() {
    let net = Net::new();
    let mut server = listen_one(&net, sock(4500), Script::echo(V74));
    let (mgr, mut events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();
    let conn = server.conns_next().await;

    let timed_out = tokio::time::timeout(
        Duration::from_millis(100),
        peer.request(token(1), echo_encode(SILENT)),
    )
    .await;
    assert!(timed_out.is_err());
    until("the server has the request", || server.frames(0) == 1).await;
    // The server answers after all: nobody is waiting, the reply is dropped as an unknown
    // token and the connection carries on.
    let reply_to = server.echo_requests(0)[0].reply.token();
    conn.send(reply_to, echo_ok(1));
    let late = loop {
        let record = events.next().await.expect("events");
        if let PeerEvent::UnknownToken { token, reported } = record.event {
            break (token, reported);
        }
    };
    assert_eq!(late.0, reply_to);
    assert!(
        !late.1,
        "a reply token has no stream flag: no notice goes back"
    );
    assert_eq!(peer.status().failures, 0);
    assert_eq!(
        peer.request_as::<EchoReply, _>(token(1), echo_encode(2))
            .await
            .unwrap()
            .value,
        2 + ECHO_ADDS
    );
}

#[tokio::test(start_paused = true)]
async fn the_failure_monitor_is_the_truth_about_endpoints() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    let fm = mgr.failure_monitor().clone();

    // A failure noted by someone else refuses requests, though the registry never saw it.
    assert!(fm.endpoint_not_found(&server.address, token(9)));
    assert_eq!(
        peer.request(token(9), echo_encode(1)).await,
        Err(RequestError::EndpointNotFound {
            token: token(9),
            maybe_delivered: false
        })
    );
    assert_eq!(server.connections(), 1, "it had to connect to find out");
    assert_eq!(server.frames(0), 0, "but nothing was sent");

    // A broken_promise reply fails the endpoint in the registry and in the monitor ...
    assert_eq!(
        peer.request(token(7), echo_encode(BROKEN)).await,
        Ok(echo_error(1100))
    );
    assert!(fm.endpoint_failure(&server.address, token(7)).is_some());
    // ... and forgetting it in the monitor lets requests through again.
    assert!(fm.forget_endpoint(&server.address, token(7)));
    assert_eq!(
        peer.request_as::<EchoReply, _>(token(7), echo_encode(1))
            .await
            .unwrap()
            .value,
        1 + ECHO_ADDS
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_cancels_requests_in_flight_and_refuses_queued_ones() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();
    let in_flight = {
        let peer = peer.clone();
        tokio::spawn(async move { peer.request(token(1), echo_encode(SILENT)).await })
    };
    until("it is on the wire", || server.frames(0) == 1).await;
    mgr.shutdown().await;
    assert_eq!(in_flight.await.unwrap(), Err(RequestError::Cancelled));
    assert_eq!(
        peer.request(token(1), echo_encode(1)).await,
        Err(RequestError::NotSent(None))
    );
}

// ---- failure accounting --------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn each_end_of_a_connection_is_one_disconnect() {
    let net = Net::new();
    let mut server = listen_one(&net, sock(4500), Script::new(V80));
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    let fm = mgr.failure_monitor().clone();
    peer.connect().await.unwrap();
    // The version switch ended one connection.
    assert_eq!(fm.address_status(&server.address).disconnects, 1);
    // An unclean end is one more, not two.
    server.conns_next().await.close();
    server.conns_next().await.close();
    until("the failure is recorded", || peer.status().failures == 1).await;
    assert_eq!(fm.address_status(&server.address).disconnects, 2);
    // A failed attempt is one.
    let refused = na(sock(4501), false);
    let other = mgr.peer(&refused);
    other.connect().await.unwrap_err();
    other.connect().await.unwrap_err();
    assert_eq!(fm.address_status(&refused).disconnects, 2);
    // A clean local close is one too.
    let before = fm.address_status(&server.address).disconnects;
    peer.connect().await.unwrap();
    peer.close_connection();
    until("closed", || peer.status().phase != PeerPhase::Connected).await;
    assert_eq!(fm.address_status(&server.address).disconnects, before + 1);
}

#[tokio::test(start_paused = true)]
async fn one_bound_covers_the_tcp_connect_and_the_tls_handshake() {
    let net = Net::new();
    let mut script = Script::new(V74);
    script.tls = true;
    let server = listen_one(&net, sock(4500), script);
    net.set_connect_delay(Duration::from_millis(1500));
    // The TLS handshake alone would fit in 2 s, the two together do not.
    let slow = |s: MemStream, _p: NetworkAddress| async move {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        Ok::<_, io::Error>(s)
    };
    let (sink, _events) = event_channel();
    let mgr = PeerManager::with_stream_wrapper(MemRuntime::new(&net), config(), slow, sink);
    let t0 = tokio::time::Instant::now();
    let error = mgr.peer(&server.address).connect().await.unwrap_err();
    assert_eq!(
        error,
        PeerError::TlsTimeout {
            after: Duration::from_secs(2)
        }
    );
    let elapsed = secs(tokio::time::Instant::now() - t0);
    assert!((2.0..2.01).contains(&elapsed), "{elapsed}");

    // Within the bound, both parts together succeed.
    let quick = |s: MemStream, _p: NetworkAddress| async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok::<_, io::Error>(s)
    };
    let (sink, _events) = event_channel();
    let mgr = PeerManager::with_stream_wrapper(MemRuntime::new(&net), config(), quick, sink);
    assert_eq!(mgr.peer(&server.address).connect().await, Ok(V74));

    // A plain connect that is slower than the bound is a TCP timeout.
    let plain = listen_one(&net, sock(4501), Script::new(V74));
    net.set_connect_delay(Duration::from_millis(2500));
    let (mgr, _events) = manager(&net, config());
    assert_eq!(
        mgr.peer(&plain.address).connect().await,
        Err(PeerError::ConnectTimeout {
            after: Duration::from_secs(2)
        })
    );
}

// ---- peers are removed ---------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn an_idle_unreferenced_peer_is_removed_once_its_handles_are_gone() {
    let net = Net::new();
    let (mgr, mut events) = manager(&net, config());
    let address = na(sock(4500), false);
    let peer = mgr.peer(&address);
    let id = peer.id();
    let copy = peer.clone();
    assert_eq!(mgr.peer_count(), 1);
    drop(peer);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(mgr.peer_count(), 1, "a handle is still held");
    drop(copy);
    until("the peer is removed", || mgr.peer_count() == 0).await;
    assert!(
        events_of(&mut events)
            .iter()
            .any(|e| matches!(e, PeerEvent::Removed))
    );
    // The address starts over with a new peer.
    let again = mgr.peer(&address);
    assert_ne!(again.id(), id);
    assert_eq!(mgr.peer_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_referenced_peer_is_not_removed_and_is_when_released() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::new(V74));
    let (mgr, _events) = manager(&net, config());
    let reference = mgr.peer(&server.address).reference();
    reference.handle().connect().await.unwrap();
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(mgr.peer_count(), 1);
    assert_eq!(reference.handle().status().references, 1);
    let second = reference.clone();
    assert_eq!(reference.handle().status().references, 2);
    drop(reference);
    assert_eq!(second.handle().status().references, 1);
    drop(second);
    // Released: the idle connection closes, and with no handle left the peer goes.
    until("the peer is removed", || mgr.peer_count() == 0).await;
    until("the connection is closed", || {
        server.record(0).closed_at.is_some()
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_peer_whose_connection_ends_is_removed_when_nothing_uses_it() {
    let net = Net::new();
    let mut server = listen_one(&net, sock(4500), Script::new(V74));
    let (mgr, mut events) = manager(&net, config());
    // Used once, never referenced: the connection stays until it ends.
    mgr.peer(&server.address)
        .send(token(1), vec![1])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(mgr.peer_count(), 1, "the connection is alive");
    net.set(sock(4500), Behavior::Refuse);
    server.conns_next().await.close();
    until("the peer is removed", || mgr.peer_count() == 0).await;
    let dials = net.connects(sock(4500));
    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert_eq!(net.connects(sock(4500)), dials, "not dialled any more");
    assert!(
        events_of(&mut events)
            .iter()
            .any(|e| matches!(e, PeerEvent::Removed))
    );
}

#[tokio::test(start_paused = true)]
async fn a_vanished_server_is_not_dialled_forever() {
    let net = Net::new();
    let mut server = listen_one(&net, sock(4500), Script::new(V74));
    let (mgr, _events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.send(token(1), vec![1]).await.unwrap();
    // The server goes away for good while a handle is still held (but nothing is referenced).
    net.set(sock(4500), Behavior::Refuse);
    server.conns_next().await.close();
    until("the failure is seen", || peer.status().failures == 1).await;
    let before = net.connects(sock(4500));
    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert_eq!(
        net.connects(sock(4500)),
        before,
        "an unreferenced peer is not probed"
    );
    assert_eq!(mgr.peer_count(), 1, "the handle keeps it");
    drop(peer);
    until("the peer is removed", || mgr.peer_count() == 0).await;

    // A referenced peer is probed while its reference lives, and no longer after.
    let reference = mgr.peer(&na(sock(4500), false)).reference();
    reference.handle().connect().await.unwrap_err();
    let start = net.connects(sock(4500));
    tokio::time::sleep(Duration::from_secs(60)).await;
    let probes = net.connects(sock(4500)) - start;
    assert!(
        (50..=61).contains(&probes),
        "about one probe a second: {probes}"
    );
    drop(reference);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(mgr.peer_count(), 0);
    let settled = net.connects(sock(4500));
    tokio::time::sleep(Duration::from_secs(600)).await;
    assert_eq!(net.connects(sock(4500)), settled);
}

// ---- stress over the in-memory network -----------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_close_storm_while_sending_never_leaves_a_caller_hanging() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let (mgr, mut events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    let mut callers = Vec::new();
    for t in 0..8u64 {
        let peer = peer.clone();
        callers.push(tokio::spawn(async move {
            let (mut ok, mut sent_or_unknown) = (0, 0);
            for i in 0..100u64 {
                match peer.request(token(1), echo_encode(t * 1000 + i)).await {
                    Ok(_) => ok += 1,
                    Err(e) => {
                        assert!(
                            matches!(
                                e,
                                RequestError::MaybeDelivered(_) | RequestError::NotSent(_)
                            ),
                            "{e:?}"
                        );
                        sent_or_unknown += 1;
                    }
                }
                tokio::time::sleep(Duration::from_millis(1 + i % 3)).await;
            }
            (ok, sent_or_unknown)
        }));
    }
    let closer = {
        let peer = peer.clone();
        tokio::spawn(async move {
            for i in 0..150u64 {
                peer.close_connection();
                tokio::time::sleep(Duration::from_millis(1 + i % 5)).await;
            }
        })
    };
    let mut total = (0, 0);
    for caller in callers {
        let (ok, other) = tokio::time::timeout(Duration::from_secs(300), caller)
            .await
            .expect("a caller hung")
            .unwrap();
        total.0 += ok;
        total.1 += other;
    }
    closer.await.unwrap();
    assert_eq!(total.0 + total.1, 800);
    assert!(total.0 > 0, "some requests were answered");
    let closed = events_of(&mut events)
        .iter()
        .filter(|e| matches!(e, PeerEvent::Closed { .. }))
        .count();
    assert!(closed > 1, "the connection was closed repeatedly");
    tokio::time::timeout(Duration::from_secs(10), mgr.shutdown())
        .await
        .expect("shutdown hung");
}

#[tokio::test(start_paused = true)]
async fn a_hundred_peers_shut_down_at_once() {
    let net = Net::new();
    let mut servers = Vec::new();
    for p in 0..100u16 {
        if p % 2 == 0 {
            servers.push(listen_one(&net, sock(5000 + p), Script::echo(V74)));
        } else {
            net.set(sock(5000 + p), Behavior::Hang);
        }
    }
    let (mgr, _events) = manager(&net, config());
    let mut callers = Vec::new();
    for p in 0..100u16 {
        let peer = mgr.peer(&na(sock(5000 + p), false));
        callers.push(tokio::spawn(async move {
            peer.request(token(1), echo_encode(p as u64)).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    tokio::time::timeout(Duration::from_secs(10), mgr.shutdown())
        .await
        .expect("shutdown hung");
    for caller in callers {
        let result = tokio::time::timeout(Duration::from_secs(1), caller)
            .await
            .expect("a request hung")
            .unwrap();
        // The even ones were answered before the shutdown; the others were never sent.
        assert!(
            matches!(result, Ok(_) | Err(RequestError::NotSent(_))),
            "{result:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_server_that_closes_in_the_middle_of_a_frame_ends_the_connection() {
    let net = Net::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    net.set(
        sock(4500),
        Behavior::Accept {
            tx,
            capacity: 64 * 1024,
        },
    );
    let addr = sock(4500);
    tokio::spawn(async move {
        while let Some(mut s) = rx.recv().await {
            tokio::spawn(async move {
                let mut hello = [0u8; ConnectPacket::SIZE];
                s.read_exact(&mut hello).await.unwrap();
                s.write_all(&ConnectPacket::server(V74, addr).encode().unwrap())
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
                let f = frame(token(77), &[9u8; 1000]);
                s.write_all(&f[..f.len() / 2]).await.unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
                drop(s);
            });
        }
    });
    let (mgr, mut events) = manager(&net, config());
    let peer = mgr.peer(&na(addr, false));
    peer.connect().await.unwrap();
    peer.register(token(77)).await.unwrap();
    let error = loop {
        let record = events.next().await.expect("events");
        if let PeerEvent::Closed { error, .. } = record.event {
            break error;
        }
    };
    assert!(!error.is_clean(), "{error:?}");
    mgr.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_flood_of_frames_is_delivered_in_full_and_sends_still_go_out() {
    let net = Net::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    net.set(
        sock(4500),
        Behavior::Accept {
            tx,
            capacity: 64 * 1024,
        },
    );
    let addr = sock(4500);
    const FRAMES: usize = 5000;
    tokio::spawn(async move {
        while let Some(mut s) = rx.recv().await {
            tokio::spawn(async move {
                let mut hello = [0u8; ConnectPacket::SIZE];
                s.read_exact(&mut hello).await.unwrap();
                s.write_all(&ConnectPacket::server(V74, addr).encode().unwrap())
                    .await
                    .unwrap();
                // Let the client register its token first: frames for a token nobody has
                // registered are dropped (and the server told).
                tokio::time::sleep(Duration::from_millis(100)).await;
                let f = frame(token(77), &[9u8; 4000]);
                for _ in 0..FRAMES {
                    if s.write_all(&f).await.is_err() {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_secs(1000)).await;
            });
        }
    });
    let (mgr, mut events) = manager(&net, config());
    let peer = mgr.peer(&na(addr, false));
    peer.connect().await.unwrap();
    peer.register(token(77)).await.unwrap();
    let mut got = 0;
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(record) = events.next().await {
            match record.event {
                PeerEvent::Frame(_) => {
                    got += 1;
                    if got == FRAMES {
                        return;
                    }
                }
                PeerEvent::Closed { error, .. } => panic!("closed after {got} frames: {error:?}"),
                _ => {}
            }
        }
    })
    .await
    .expect("every frame arrived");
    // Sending still works after the flood.
    tokio::time::timeout(Duration::from_secs(5), peer.send(token(1), vec![1]))
        .await
        .expect("no hang")
        .unwrap();
    mgr.shutdown().await;
}

// ---- fairness over real TCP on a multi-threaded runtime ------------------------------------

/// A server that reads and discards everything and pushes one tiny frame to token 77 every
/// 10 ms. Returns its address.
async fn ticking_server() -> std::net::SocketAddr {
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (mut reader, mut writer) = stream.into_split();
        let mut hello = [0u8; ConnectPacket::SIZE];
        reader.read_exact(&mut hello).await.unwrap();
        writer
            .write_all(&ConnectPacket::server(V74, addr).encode().unwrap())
            .await
            .unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 1 << 20];
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
            }
        });
        let tick = frame(token(77), b"tick");
        loop {
            if writer.write_all(&tick).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    addr
}

/// Under a continuous load of sends the peer must keep reading: frames arrive at about the
/// rate they do without load. (Before the passes were restructured, a write always ended the
/// pass before the socket was read, and the client read nothing.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_are_not_starved_by_a_continuous_send_load() {
    let addr = ticking_server().await;
    let (sink, mut events) = event_channel();
    let mut c = config();
    c.monitor.loop_time = Duration::from_secs(1000);
    let mgr: PeerManager<TokioRuntime, NoTls> = PeerManager::new(TokioRuntime::new(), c, sink);
    let peer = mgr.peer(&na(addr, false));
    peer.connect().await.unwrap();
    peer.register(token(77)).await.unwrap();

    let count = |events: &mut EventReceiver| {
        std::iter::from_fn(|| events.try_next())
            .filter(|r| matches!(r.event, PeerEvent::Frame(_)))
            .count()
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle = count(&mut events);
    assert!(
        idle >= 30,
        "the server ticks about 100 times a second: {idle}"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let sends = Arc::new(AtomicUsize::new(0));
    let mut senders = Vec::new();
    for t in 0..16u64 {
        let peer = peer.clone();
        let stop = stop.clone();
        let sends = sends.clone();
        senders.push(tokio::spawn(async move {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                peer.send(token(t << 32 | i), vec![0u8; 8192])
                    .await
                    .unwrap();
                sends.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        }));
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let loaded = count(&mut events);
    stop.store(true, Ordering::Relaxed);
    for sender in senders {
        sender.await.unwrap();
    }
    println!(
        "frames read per second: {idle} idle, {loaded} under load ({} sends)",
        sends.load(Ordering::Relaxed)
    );
    assert!(sends.load(Ordering::Relaxed) > 100, "the load really ran");
    assert!(
        loaded * 3 >= idle,
        "{loaded} frames read under load against {idle} idle: reads were starved"
    );
    mgr.shutdown().await;
}

/// The same, with requests: replies keep completing while other requests are written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_complete_under_a_continuous_send_load() {
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let script = Script::echo(V74);
    let records = Arc::new(Mutex::new(Vec::new()));
    {
        let records = records.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            records.lock().unwrap().push(ConnRecord {
                at: tokio::time::Instant::now(),
                announced: None,
                connection_id: 0,
                frames: Vec::new(),
                pings: 0,
                closed_at: None,
            });
            let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let _keep = _tx;
            serve(stream, script, records, 0, rx, addr).await
        });
    }
    let (sink, _events) = event_channel();
    let mgr: PeerManager<TokioRuntime, NoTls> =
        PeerManager::new(TokioRuntime::new(), config(), sink);
    let peer = mgr.peer(&na(addr, false));
    peer.connect().await.unwrap();
    let mut callers = Vec::new();
    for t in 0..8u64 {
        let peer = peer.clone();
        callers.push(tokio::spawn(async move {
            for i in 0..200u64 {
                let value = t * 1000 + i;
                let reply = peer
                    .request_as::<EchoReply, _>(token(1), echo_encode(value))
                    .await
                    .unwrap();
                assert_eq!(reply.value, value + ECHO_ADDS);
            }
        }));
    }
    for caller in callers {
        tokio::time::timeout(Duration::from_secs(60), caller)
            .await
            .expect("requests were starved")
            .unwrap();
    }
    mgr.shutdown().await;
}
