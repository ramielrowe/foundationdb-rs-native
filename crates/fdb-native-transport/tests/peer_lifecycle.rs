//! Regression tests from the as8.5 re-review (as8.9): a panic on a peer's task must not leave
//! callers hanging, an encoder must not be able to damage other frames, replies carry the
//! version they arrived on, a shutdown wakes the failure monitor's waiters, a dropped
//! `register` is undone, `PeerStatus::references` is published consistently, and a request to a
//! failed address fails fast.

#![cfg(feature = "tokio")]

mod common;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::*;
use fdb_native_transport::peer::{
    EventReceiver, Generation, PeerError, PeerEvent, PeerEventRecord, PeerHandle, PeerId,
    PeerManager, PeerManagerConfig, PeerPhase, SendError, event_channel,
};
use fdb_native_transport::rpc::RequestError;
use fdb_native_wire::{EncodeError, NetworkAddress, ProtocolVersion, Uid};
use futures_lite::future::poll_once;

const V74: ProtocolVersion = ProtocolVersion::V7_4;
const V80: ProtocolVersion = ProtocolVersion::V8_0;

fn config() -> PeerManagerConfig {
    PeerManagerConfig::default().without_jitter()
}

fn manager(net: &Arc<Net>, config: PeerManagerConfig) -> (PeerManager<MemRuntime>, EventReceiver) {
    let (sink, events) = event_channel();
    (PeerManager::new(MemRuntime::new(net), config, sink), events)
}

fn ep(n: u64) -> Uid {
    Uid::new(0xE000_0000_0000_0001, n)
}

fn token(n: u64) -> Uid {
    Uid::new(0x5eed_0000_0000_0000 | 1, n)
}

fn all_values(server: &Server) -> Vec<u64> {
    (0..server.connections())
        .flat_map(|i| server.echo_values(i))
        .collect()
}

// ---- reply version ---------------------------------------------------------------------------

/// `request` says which version the reply arrived on: the negotiated one, also after a version
/// switch (the first connection announces 7.4 and is refused by the 8.0 server).
#[tokio::test(start_paused = true)]
async fn a_reply_carries_the_version_it_arrived_on() {
    for server_version in [V74, V80] {
        let net = Net::new();
        let server = listen_one(&net, sock(4500), Script::echo(server_version));
        let (mgr, _ev) = manager(&net, config());
        let peer = mgr.peer(&server.address);
        let reply = peer.request(ep(1), echo_encode(3)).await.unwrap();
        assert_eq!(reply.version, server_version);
        assert_eq!(reply.version, peer.version().unwrap());
        assert_eq!(
            reply.decode::<EchoReply>(),
            Ok(EchoReply {
                value: 3 + ECHO_ADDS
            })
        );
        mgr.shutdown().await;
    }
}

// ---- a panicking encoder ---------------------------------------------------------------------

/// An encoder that panics fails its request (certainly not delivered) and nothing else: the
/// request in flight, the ones queued behind it, and later ones all complete, the connection
/// stays up, and the server sees none of the panicking request's bytes.
#[tokio::test(start_paused = true)]
async fn a_panicking_request_encoder_fails_only_its_request() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let (mgr, _ev) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();
    let id = peer.id();

    // On the connection, awaiting a reply that never comes.
    let p = peer.clone();
    let inflight = tokio::spawn(async move { p.request(ep(1), echo_encode(SILENT)).await });
    tokio::time::sleep(Duration::from_millis(5)).await;
    let p = peer.clone();
    let bad = tokio::spawn(async move {
        p.request(
            ep(1),
            |_, _, out: &mut Vec<u8>| -> Result<(), EncodeError> {
                // Partial bytes first: they must not reach the connection.
                out.extend_from_slice(&[0xAA; 100]);
                panic!("encoder bug")
            },
        )
        .await
    });
    let mut behind = Vec::new();
    for v in 0..5u64 {
        let p = peer.clone();
        behind.push(tokio::spawn(async move {
            p.request_as::<EchoReply, _>(ep(1), echo_encode(v)).await
        }));
    }

    let error = tokio::time::timeout(Duration::from_secs(5), bad)
        .await
        .expect("the panicking request hung")
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, RequestError::Encode(_)), "{error:?}");
    assert!(error.certainly_not_delivered());
    for (v, h) in behind.into_iter().enumerate() {
        let reply = tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .expect("a request behind the panic hung")
            .unwrap()
            .unwrap();
        assert_eq!(reply.value, v as u64 + ECHO_ADDS);
    }
    assert!(
        !inflight.is_finished(),
        "the request in flight is untouched"
    );
    assert_eq!(peer.status().phase, PeerPhase::Connected);
    assert_eq!(peer.status().generation, Generation(1));
    assert_eq!(peer.id(), id);
    let reply = peer
        .request_as::<EchoReply, _>(ep(1), echo_encode(99))
        .await
        .unwrap();
    assert_eq!(reply.value, 99 + ECHO_ADDS);
    assert_eq!(server.connections(), 1);
    assert_eq!(all_values(&server), vec![SILENT, 0, 1, 2, 3, 4, 99]);
    mgr.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_panicking_frame_encoder_fails_only_its_send() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::new(V74));
    let (mgr, _ev) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();

    let result = peer
        .send_with(token(1), |_, out| {
            out.extend_from_slice(&[0xAA; 10]);
            panic!("encoder bug")
        })
        .await;
    assert!(matches!(result, Err(SendError::Encode(_))), "{result:?}");
    peer.send(token(2), vec![1, 2, 3]).await.unwrap();
    until("the second frame arrives", || server.frames(0) == 1).await;
    assert_eq!(server.record(0).frames[0].token, token(2));
    assert_eq!(peer.status().phase, PeerPhase::Connected);
    mgr.shutdown().await;
}

// ---- an encoder that overwrites its vector ---------------------------------------------------

/// An encoder that assigns to the vector instead of appending used to wipe the frames queued
/// before it and write the rest unframed. Now it cannot touch other frames: the 40 requests
/// before it, the encoder's own, and the one after all arrive intact and in order.
#[tokio::test(start_paused = true)]
async fn an_encoder_that_overwrites_its_vector_cannot_damage_other_frames() {
    let net = Net::new();
    let mut paused = Script::echo(V74);
    paused.read = false; // the server does not read: the frames pile up in the output
    let mut server = listen(&net, sock(4500), vec![paused], 1024);
    let (mgr, mut events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();
    let conn = server.conns_next().await;

    let mut hs = Vec::new();
    for v in 0..40u64 {
        let p = peer.clone();
        hs.push(tokio::spawn(async move {
            p.request_as::<EchoReply, _>(ep(1), echo_encode(v)).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    let p = peer.clone();
    let overwriting = tokio::spawn(async move {
        p.request_as::<EchoReply, _>(ep(1), |reply, _v, out: &mut Vec<u8>| {
            *out = fdb_native_wire::encode(&EchoRequest {
                reply: fdb_native_wire::ReplyPromise::new(reply),
                value: 500,
            })?;
            Ok(())
        })
        .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let p = peer.clone();
    let after =
        tokio::spawn(async move { p.request_as::<EchoReply, _>(ep(1), echo_encode(501)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    conn.start_reading();

    for (v, h) in hs.into_iter().enumerate() {
        let reply = tokio::time::timeout(Duration::from_secs(30), h)
            .await
            .expect("hung")
            .unwrap()
            .unwrap();
        assert_eq!(reply.value, v as u64 + ECHO_ADDS);
    }
    for (h, v) in [(overwriting, 500), (after, 501)] {
        let reply = h.await.unwrap().unwrap();
        assert_eq!(reply.value, v + ECHO_ADDS);
    }
    let mut expected: Vec<u64> = (0..40).collect();
    expected.extend([500, 501]);
    assert_eq!(server.echo_values(0), expected);
    assert_eq!(
        server.connections(),
        1,
        "the connection was never disturbed"
    );
    while let Some(PeerEventRecord { event, .. }) = events.try_next() {
        assert!(!matches!(event, PeerEvent::Closed { .. }), "{event:?}");
    }
    mgr.shutdown().await;
}

/// An encoder that *clears* its vector and returns `Ok` sends a frame of no payload, which is
/// the application's mistake; it still cannot hurt the frames around it.
#[tokio::test(start_paused = true)]
async fn an_encoder_that_clears_its_vector_cannot_damage_other_frames() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let (mgr, _ev) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();
    peer.send_with(token(1), |_, out| {
        out.extend_from_slice(b"abc");
        out.clear();
        Ok(())
    })
    .await
    .unwrap();
    let reply = peer
        .request_as::<EchoReply, _>(ep(1), echo_encode(7))
        .await
        .unwrap();
    assert_eq!(reply.value, 7 + ECHO_ADDS);
    assert_eq!(server.connections(), 1);
    assert_eq!(server.echo_values(0), vec![7]);
    mgr.shutdown().await;
}

// ---- a panic on the task: the sink --------------------------------------------------------------

/// A sink that panics on the events it is told to, once.
struct PanicOnce {
    armed: Arc<AtomicBool>,
    when: fn(&PeerEvent) -> bool,
}

impl fdb_native_transport::peer::PeerEventSink for PanicOnce {
    fn on_event(&self, _: PeerId, _: &NetworkAddress, event: PeerEvent) {
        if (self.when)(&event) && self.armed.swap(false, Ordering::SeqCst) {
            panic!("sink bug");
        }
    }
}

fn panicking_manager(
    net: &Arc<Net>,
    armed: &Arc<AtomicBool>,
    when: fn(&PeerEvent) -> bool,
) -> PeerManager<MemRuntime> {
    PeerManager::new(
        MemRuntime::new(net),
        config(),
        PanicOnce {
            armed: armed.clone(),
            when,
        },
    )
}

/// The sink panics when the connection is established, before any command was applied: the
/// peer is abandoned. Queued commands are refused as not sent (and never reach the server), the
/// phase is `Shutdown`, the failure monitor's waiters are woken, the entry leaves the manager
/// and the next `peer()` is a new, working one.
#[tokio::test(start_paused = true)]
async fn a_sink_that_panics_abandons_the_peer_and_refuses_what_was_queued() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let armed = Arc::new(AtomicBool::new(true));
    let mgr = panicking_manager(&net, &armed, |e| matches!(e, PeerEvent::Connected { .. }));
    let peer = mgr.peer(&server.address);
    let old_id = peer.id();
    let fm = peer.failure_monitor().clone();
    let address = server.address;
    let waiter = tokio::spawn(async move { fm.on_disconnect(&address).await });
    tokio::time::sleep(Duration::from_millis(1)).await;

    let mut hs = Vec::new();
    for v in 0..4u64 {
        let p = peer.clone();
        hs.push(tokio::spawn(async move {
            p.request(ep(1), echo_encode(v)).await
        }));
    }
    for h in hs {
        let error = tokio::time::timeout(Duration::from_secs(10), h)
            .await
            .expect("a queued request hung after the task panicked")
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, RequestError::NotSent(_)), "{error:?}");
        assert!(error.certainly_not_delivered());
    }
    assert!(all_values(&server).is_empty(), "nothing was applied");
    assert_eq!(peer.status().phase, PeerPhase::Shutdown);
    assert_eq!(peer.queued_commands(), 0);
    assert_eq!(mgr.peer_count(), 0, "the dead peer left the manager");
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("on_disconnect was not woken")
        .unwrap();
    // The old handle fails at once, whatever it is asked.
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), peer.request(ep(1), echo_encode(1)))
            .await
            .expect("hung on the dead peer"),
        Err(RequestError::NotSent(_))
    ));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), peer.connect())
            .await
            .expect("connect hung on the dead peer"),
        Err(PeerError::Shutdown)
    );

    // The next peer() for the address is a fresh peer that works (the sink panicked once).
    let fresh = mgr.peer(&server.address);
    assert_ne!(fresh.id(), old_id);
    assert_eq!(mgr.peer_count(), 1);
    let reply = fresh
        .request_as::<EchoReply, _>(ep(1), echo_encode(9))
        .await
        .unwrap();
    assert_eq!(reply.value, 9 + ECHO_ADDS);
    tokio::time::timeout(Duration::from_secs(10), mgr.shutdown())
        .await
        .expect("shutdown hung");
}

/// The sink panics while a request is on the connection: its caller is told it may have been
/// delivered (it was written), not left hanging, and the peer is replaced.
#[tokio::test(start_paused = true)]
async fn a_sink_that_panics_makes_requests_in_flight_maybe_delivered() {
    let net = Net::new();
    let mut server = listen_one(&net, sock(4500), Script::echo(V74));
    let armed = Arc::new(AtomicBool::new(false));
    let mgr = panicking_manager(&net, &armed, |e| matches!(e, PeerEvent::Frame(_)));
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();
    let conn = server.conns_next().await;
    peer.register(token(77)).await.unwrap();
    let p = peer.clone();
    let inflight = tokio::spawn(async move { p.request(ep(1), echo_encode(SILENT)).await });
    until("the request is written", || server.frames(0) == 1).await;

    armed.store(true, Ordering::SeqCst);
    conn.send(token(77), vec![1, 2, 3]);
    let error = tokio::time::timeout(Duration::from_secs(10), inflight)
        .await
        .expect("a request in flight hung after the task panicked")
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(error, RequestError::MaybeDelivered(_)),
        "{error:?}"
    );
    assert!(error.maybe_delivered());
    assert_eq!(peer.status().phase, PeerPhase::Shutdown);
    assert_eq!(mgr.peer_count(), 0);

    let fresh = mgr.peer(&server.address);
    let reply = fresh
        .request_as::<EchoReply, _>(ep(1), echo_encode(2))
        .await
        .unwrap();
    assert_eq!(reply.value, 2 + ECHO_ADDS);
    mgr.shutdown().await;
}

// ---- a shutdown wakes the failure monitor ---------------------------------------------------

#[tokio::test(start_paused = true)]
async fn shutdown_wakes_the_on_disconnect_waiters_of_a_connected_address() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let (mgr, _ev) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();
    let fm = peer.failure_monitor().clone();
    let address = server.address;
    let waiter = tokio::spawn(async move { fm.on_disconnect(&address).await });
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(!waiter.is_finished());
    mgr.shutdown().await;
    let count = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("on_disconnect was not woken by the shutdown")
        .unwrap();
    assert_eq!(count, 1, "one disconnect");
    assert_eq!(
        peer.failure_monitor().address_status(&address).disconnects,
        1
    );
}

// ---- a register that was applied and then dropped -----------------------------------------------

/// A `register` whose caller dropped the future after the task applied it (but before it saw
/// the answer) is unregistered by the drop: frames for the token are then unknown tokens, not
/// delivered to the sink as the frames of a registration nobody holds.
#[tokio::test(start_paused = true)]
async fn a_register_that_was_applied_and_dropped_is_unregistered() {
    let net = Net::new();
    let mut server = listen_one(&net, sock(4500), Script::new(V74));
    let (mgr, mut events) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    peer.connect().await.unwrap();
    let conn = server.conns_next().await;

    let mut register = Box::pin(peer.register(token(5)));
    assert!(poll_once(&mut register).await.is_none(), "queued");
    // The task applies it and answers; the caller never polls again.
    tokio::time::sleep(Duration::from_millis(5)).await;
    drop(register);
    tokio::time::sleep(Duration::from_millis(5)).await;

    conn.send(token(5), vec![1, 2, 3]);
    tokio::time::sleep(Duration::from_millis(5)).await;
    let mut frames = 0;
    let mut unknown = 0;
    while let Some(PeerEventRecord { event, .. }) = events.try_next() {
        match event {
            PeerEvent::Frame(_) => frames += 1,
            PeerEvent::UnknownToken { token: t, .. } if t == token(5) => unknown += 1,
            _ => {}
        }
    }
    assert_eq!((frames, unknown), (0, 1));

    // One whose caller saw the answer stays registered until it is unregistered.
    assert_eq!(peer.register(token(6)).await, Ok(Generation(1)));
    conn.send(token(6), vec![4]);
    tokio::time::sleep(Duration::from_millis(5)).await;
    let mut frames = 0;
    while let Some(PeerEventRecord { event, .. }) = events.try_next() {
        frames += usize::from(matches!(event, PeerEvent::Frame(_)));
    }
    assert_eq!(frames, 1);
    peer.unregister(token(6));
    tokio::time::sleep(Duration::from_millis(5)).await;
    // ... and the token can be registered again (a stale record of the old one is gone).
    assert_eq!(peer.register(token(6)).await, Ok(Generation(1)));
    mgr.shutdown().await;
}

// ---- PeerStatus::references -----------------------------------------------------------------

/// Concurrent `PeerRef` churn from many threads: once every guard is gone, the published count
/// is 0 (it used to be possible for a stale value to be published over the final one).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_published_reference_count_is_the_final_count() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::new(V74));
    let (mgr, _ev) = manager(&net, config());
    let peer = mgr.peer(&server.address);
    for _round in 0..100 {
        let mut threads = Vec::new();
        for _ in 0..8 {
            let p = peer.clone();
            threads.push(std::thread::spawn(move || {
                for _ in 0..200 {
                    drop(p.reference());
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(peer.status().references, 0);
    }
    let held: Vec<_> = (0..3).map(|_| peer.reference()).collect();
    assert_eq!(peer.status().references, 3);
    drop(held);
    assert_eq!(peer.status().references, 0);
    mgr.shutdown().await;
}

// ---- failing fast ---------------------------------------------------------------------------------

/// A request to an address the failure monitor has marked failed returns `NotSent` at once: it
/// is not queued and nothing is dialed. An explicit `connect` still probes, and once it
/// succeeds requests flow again.
#[tokio::test(start_paused = true)]
async fn a_request_to_a_failed_address_fails_fast() {
    let net = Net::new();
    let addr = sock(4500);
    net.set(addr, Behavior::Refuse);
    let address = na(addr, false);
    let (mgr, _ev) = manager(&net, config());
    let peer = mgr.peer(&address);
    // Fail for longer than FAILURE_DETECTION_DELAY.
    while !peer.is_failed() {
        let _ = peer.connect().await;
    }
    let dials = net.connects(addr);
    let t0 = tokio::time::Instant::now();

    for _ in 0..3 {
        let error = peer.request(ep(1), echo_encode(1)).await.unwrap_err();
        assert!(matches!(error, RequestError::NotSent(_)), "{error:?}");
        assert!(error.certainly_not_delivered());
    }
    assert_eq!(tokio::time::Instant::now(), t0, "no waiting for a backoff");
    assert_eq!(peer.queued_commands(), 0);
    assert_eq!(net.connects(addr), dials, "nothing was dialed");

    // `connect` still probes.
    assert_eq!(
        peer.connect().await,
        Err(PeerError::Connect(io::ErrorKind::ConnectionRefused))
    );
    assert!(net.connects(addr) > dials);

    // The server comes back: the probe succeeds, the address is available again.
    let _server = listen_one(&net, addr, Script::echo(V74));
    assert_eq!(peer.connect().await, Ok(V74));
    assert!(!peer.is_failed());
    let reply = peer
        .request_as::<EchoReply, _>(ep(1), echo_encode(4))
        .await
        .unwrap();
    assert_eq!(reply.value, 4 + ECHO_ADDS);
    mgr.shutdown().await;
}

// ---- shutdown after a panic ---------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn shutdown_after_a_panic_returns() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let armed = Arc::new(AtomicBool::new(true));
    let mgr = panicking_manager(&net, &armed, |e| matches!(e, PeerEvent::Connected { .. }));
    let peer: PeerHandle = mgr.peer(&server.address);
    let first = tokio::time::timeout(Duration::from_secs(10), peer.request(ep(1), echo_encode(1)))
        .await
        .expect("the request hung");
    assert!(first.is_err());
    tokio::time::timeout(Duration::from_secs(10), mgr.shutdown())
        .await
        .expect("shutdown hung");
}
