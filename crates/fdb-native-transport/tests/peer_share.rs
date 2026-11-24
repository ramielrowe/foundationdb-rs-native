//! Users of one peer manager's connections (`PeerManager::share`, b35.7): one connection per
//! address whichever user asks, one failure monitor, and a shutdown per user that ends only
//! that user's commands (with the at-most-once classification of a dropped command), until
//! the last user closes the connections.

#![cfg(feature = "tokio")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use fdb_native_transport::peer::{PeerError, PeerManager, PeerManagerConfig, PeerPhase};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::{Runtime, TokioRuntime};
use fdb_native_wire::{ProtocolVersion, Uid};

const V74: ProtocolVersion = ProtocolVersion::V7_4;

fn config() -> PeerManagerConfig {
    PeerManagerConfig::default().without_jitter()
}

fn manager(net: &Arc<Net>) -> PeerManager<MemRuntime> {
    PeerManager::new(
        MemRuntime::new(net),
        config(),
        fdb_native_transport::peer::DiscardEvents,
    )
}

fn ep(n: u64) -> Uid {
    Uid::new(0xE000_0000_0000_0001, n)
}

fn all_values(server: &Server) -> Vec<u64> {
    (0..server.connections())
        .flat_map(|i| server.echo_values(i))
        .collect()
}

/// Two users of one manager have one connection to an address, the same peer, the same
/// failure monitor, and are counted.
#[tokio::test(start_paused = true)]
async fn users_share_one_connection_per_address() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let a = manager(&net);
    let b = a.share().expect("a is open");
    assert!(a.shares_connections_with(&b));
    assert!(a.shares_connections_with(&a.clone()));
    assert_eq!(a.users(), 2);
    let (pa, pb) = (a.peer(&server.address), b.peer(&server.address));
    assert_eq!(pa.id(), pb.id());
    let (ra, rb) = futures_lite::future::zip(
        pa.request_as::<EchoReply, _>(ep(1), echo_encode(1)),
        pb.request_as::<EchoReply, _>(ep(1), echo_encode(2)),
    )
    .await;
    assert_eq!(ra.unwrap().value, 1 + ECHO_ADDS);
    assert_eq!(rb.unwrap().value, 2 + ECHO_ADDS);
    assert_eq!(server.connections(), 1);
    assert_eq!(net.connects(sock(4500)), 1);
    assert_eq!(a.peer_count(), 1);
    assert_eq!(b.peer_count(), 1);
    // The failure state is one: an address that fails for one user is failed for the other.
    net.set(sock(4599), Behavior::Refuse);
    let dead = na(sock(4599), false);
    let probe = b.peer(&dead);
    // Fail for longer than FAILURE_DETECTION_DELAY.
    while !a.failure_monitor().is_failed(&dead) {
        let _ = probe.connect().await;
    }
    assert!(a.peer(&dead).is_failed());
    assert_eq!(a.peer(&dead).id(), probe.id());
    drop(probe);
    // A third user, from the weak reference.
    let c = a.downgrade().share().expect("open");
    assert_eq!(c.users(), 3);
    drop(c);
    assert_eq!(a.users(), 2);
    a.shutdown().await;
    b.shutdown().await;
}

/// A user's shutdown ends its own requests at once (one sent: `Cancelled`, maybe delivered)
/// and nothing else: the other user's request in flight on the same connection completes, the
/// connection stays up, and the user that is shut down can no longer send, connect, close the
/// connection or reference the peer.
#[tokio::test(start_paused = true)]
async fn shutting_a_user_down_ends_only_its_own_requests() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let a = manager(&net);
    let b = a.share().expect("open");
    let pa = a.peer(&server.address);
    let pb = b.peer(&server.address);
    pb.connect().await.unwrap();
    let silent = tokio::spawn({
        let pa = pa.clone();
        async move { pa.request(ep(1), echo_encode(SILENT)).await }
    });
    until("the silent request is sent", || {
        all_values(&server).contains(&SILENT)
    })
    .await;
    // B's request in flight when A is shut down.
    let b_request = tokio::spawn({
        let pb = pb.clone();
        async move { pb.request_as::<EchoReply, _>(ep(1), echo_encode(7)).await }
    });
    a.shutdown().await;
    assert!(a.is_shut_down());
    assert!(!b.is_shut_down());
    assert!(!a.connections_shut_down());
    assert_eq!(a.users(), 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(50), silent)
            .await
            .expect("ended at once")
            .unwrap(),
        Err(RequestError::Cancelled)
    );
    assert_eq!(b_request.await.unwrap().unwrap().value, 7 + ECHO_ADDS);
    // A's handles fail without sending; B's go on, on the same connection.
    assert_eq!(
        pa.request(ep(1), echo_encode(8)).await,
        Err(RequestError::NotSent(None))
    );
    assert_eq!(
        a.peer(&server.address).request(ep(1), echo_encode(9)).await,
        Err(RequestError::NotSent(None))
    );
    assert_eq!(pa.connect().await, Err(PeerError::Shutdown));
    let before = pb.status().references;
    let reference = pa.reference();
    assert_eq!(pb.status().references, before);
    drop(reference);
    assert_eq!(pb.status().references, before);
    pa.close_connection();
    assert_eq!(
        pb.request_as::<EchoReply, _>(ep(1), echo_encode(10))
            .await
            .unwrap()
            .value,
        10 + ECHO_ADDS
    );
    assert_eq!(server.connections(), 1);
    assert!(server.record(0).closed_at.is_none());
    assert_eq!(pb.status().phase, PeerPhase::Connected);
    // A user that is shut down starts no peer.
    let other = na(sock(4501), false);
    let dead = a.peer(&other);
    assert_eq!(dead.status().phase, PeerPhase::Shutdown);
    assert_eq!(b.peer_count(), 1);
    assert_eq!(net.connects(sock(4501)), 0);
    b.shutdown().await;
    assert!(b.connections_shut_down());
}

/// A command of a user that is shut down while it is still queued (the peer is connecting)
/// is refused at once, not when the connection is up: `NotSent`, certainly not delivered,
/// and the server never sees it; the other user's command, queued behind it, is sent once
/// the connection is up. So is one that waits for room in the queue.
#[tokio::test(start_paused = true)]
async fn a_queued_command_of_a_user_shut_down_is_not_sent() {
    let net = Net::new();
    net.set_connect_delay(Duration::from_millis(500));
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let a = manager(&net);
    let b = a.share().expect("open");
    let pa = a.peer(&server.address);
    let pb = b.peer(&server.address);
    let queued = tokio::spawn({
        let pa = pa.clone();
        async move { pa.request(ep(1), echo_encode(1)).await }
    });
    let behind = tokio::spawn({
        let pb = pb.clone();
        async move { pb.request_as::<EchoReply, _>(ep(1), echo_encode(2)).await }
    });
    until("both are queued", || pb.queued_commands() == 2).await;
    a.shutdown().await;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(50), queued)
            .await
            .expect("refused at once")
            .unwrap(),
        Err(RequestError::NotSent(None))
    );
    assert_eq!(behind.await.unwrap().unwrap().value, 2 + ECHO_ADDS);
    assert_eq!(all_values(&server), vec![2]);
    b.shutdown().await;

    // A full queue (one place, B's): A's request waits for room, and its shutdown ends the
    // wait, unsent.
    let net = Net::new();
    net.set_connect_delay(Duration::from_millis(500));
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let mut config = config();
    config.command_queue_len = 1;
    let b = PeerManager::new(
        MemRuntime::new(&net),
        config,
        fdb_native_transport::peer::DiscardEvents,
    );
    let a = b.share().expect("open");
    let (pa, pb) = (a.peer(&server.address), b.peer(&server.address));
    let first = tokio::spawn({
        let pb = pb.clone();
        async move { pb.request_as::<EchoReply, _>(ep(1), echo_encode(3)).await }
    });
    until("b's is queued", || pb.queued_commands() == 1).await;
    let waiting = tokio::spawn({
        let pa = pa.clone();
        async move { pa.request(ep(1), echo_encode(4)).await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!waiting.is_finished());
    a.shutdown().await;
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(50), waiting)
            .await
            .expect("ended at once")
            .unwrap(),
        Err(RequestError::NotSent(None))
    );
    assert_eq!(first.await.unwrap().unwrap().value, 3 + ECHO_ADDS);
    assert_eq!(all_values(&server), vec![3]);
    b.shutdown().await;
}

/// The review's race (b35.7): one user's shutdown races both users' requests on one
/// connection, on a multi-thread runtime, with connects that are slow or not, queues of 1 to
/// 5 places and shutdowns at varied moments. Every request of the user shut down is a reply,
/// `NotSent` (and then never reached the server) or `Cancelled`; every request of the other
/// succeeds.
#[test]
fn a_shutdown_racing_requests_keeps_at_most_once() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .build()
        .unwrap();
    let (mut not_sent, mut cancelled) = (0u64, 0u64);
    for iter in 0..150u64 {
        let (ns, ca) = rt.block_on(async move {
            let net = Net::new();
            if iter % 3 == 0 {
                net.set_connect_delay(Duration::from_micros(200 + (iter % 7) * 100));
            }
            let server = listen_one(&net, sock(4500), Script::echo(V74));
            let mut config = config();
            config.command_queue_len = 1 + (iter as usize % 5);
            let a = PeerManager::new(
                MemRuntime::new(&net),
                config,
                fdb_native_transport::peer::DiscardEvents,
            );
            let b = a.share().unwrap();
            let (pa, pb) = (a.peer(&server.address), b.peer(&server.address));
            if iter % 3 == 1 {
                pb.connect().await.unwrap();
            }
            let (mut ha, mut hb) = (Vec::new(), Vec::new());
            for i in 0..40u64 {
                let pa = pa.clone();
                let va = iter * 100_000 + i;
                ha.push((
                    va,
                    tokio::spawn(async move { pa.request(ep(1), echo_encode(va)).await }),
                ));
                let pb = pb.clone();
                let vb = iter * 100_000 + 50_000 + i;
                hb.push(tokio::spawn(async move {
                    pb.request(ep(1), echo_encode(vb)).await
                }));
            }
            for _ in 0..iter % 11 {
                tokio::task::yield_now().await;
            }
            if iter % 5 == 0 {
                tokio::time::sleep(Duration::from_micros(100 * (iter % 9))).await;
            }
            a.shutdown().await;
            let (mut ns, mut ca, mut unsent) = (0, 0, Vec::new());
            for (v, h) in ha {
                match tokio::time::timeout(Duration::from_secs(20), h).await {
                    Ok(Ok(Err(RequestError::NotSent(_)))) => {
                        ns += 1;
                        unsent.push(v);
                    }
                    Ok(Ok(Err(RequestError::Cancelled))) => ca += 1,
                    Ok(Ok(Ok(_))) => {}
                    r => panic!("iter {iter}: a's request {v} ended with {r:?}"),
                }
            }
            for h in hb {
                match tokio::time::timeout(Duration::from_secs(20), h).await {
                    Ok(Ok(Ok(_))) => {}
                    r => panic!("iter {iter}: b's request did not succeed: {r:?}"),
                }
            }
            // Let anything still buffered reach the server.
            tokio::time::sleep(Duration::from_millis(20)).await;
            let seen = all_values(&server);
            for v in &unsent {
                assert!(
                    !seen.contains(v),
                    "iter {iter}: a's request {v} was reported not sent but reached the server"
                );
            }
            b.shutdown().await;
            (ns, ca)
        });
        not_sent += ns;
        cancelled += ca;
    }
    assert!(
        not_sent > 0 && cancelled > 0,
        "both outcomes exercised: {not_sent} not sent, {cancelled} cancelled"
    );
}

/// The connections close with the last user, whether it is shut down or dropped, and are
/// never shared again: a new user needs a new manager.
#[tokio::test(start_paused = true)]
async fn the_last_user_closes_the_connections() {
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::echo(V74));
    let a = manager(&net);
    let b = a.share().expect("open");
    let weak = a.downgrade();
    let pb = b.peer(&server.address);
    let reference = pb.reference();
    pb.connect().await.unwrap();
    // Dropping a user (all its clones) leaves the others their connections.
    let a2 = a.clone();
    drop(a);
    assert_eq!(b.users(), 2);
    drop(a2);
    assert_eq!(b.users(), 1);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(server.record(0).closed_at.is_none());
    assert!(!weak.is_shut_down());
    // The last one closes them.
    drop(b);
    until("the connection is closed", || {
        server.record(0).closed_at.is_some()
    })
    .await;
    assert!(weak.is_shut_down());
    assert!(weak.share().is_none());
    assert_eq!(
        pb.request(ep(1), echo_encode(1)).await,
        Err(RequestError::NotSent(None))
    );
    drop(reference);

    // `shutdown` of the last user, while a clone of another user that was shut down lives.
    let c = manager(&net);
    let d = c.share().expect("open");
    d.peer(&server.address).connect().await.unwrap();
    d.shutdown().await;
    assert!(!c.connections_shut_down());
    c.shutdown().await;
    assert!(d.connections_shut_down());
    assert_eq!(c.users(), 0);
    assert!(c.share().is_none());
    assert!(d.share().is_none());
    until("the second connection is closed", || {
        server.record(1).closed_at.is_some()
    })
    .await;
}

/// Only a user on the same runtime shares: tokio runtimes by id; a runtime type that does not
/// say (the default `same_runtime`) never shares.
#[test]
fn share_on_needs_the_same_runtime() {
    let first = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let second = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    first.block_on(async {
        let rt = TokioRuntime::new();
        let mgr = PeerManager::new(
            rt.clone(),
            config(),
            fdb_native_transport::peer::DiscardEvents,
        );
        let weak = mgr.downgrade();
        assert!(TokioRuntime::new().same_runtime(&rt));
        let shared = weak.share_on(&TokioRuntime::new()).expect("same runtime");
        assert!(shared.shares_connections_with(&mgr));
        let elsewhere = TokioRuntime::with_handle(second.handle().clone());
        assert!(!elsewhere.same_runtime(&rt));
        assert!(weak.share_on(&elsewhere).is_none());
        assert_eq!(mgr.users(), 2);
        drop(shared);
        mgr.shutdown().await;
        assert!(weak.share_on(&rt).is_none());
    });
    first.block_on(async {
        let net = Net::new();
        let mgr = manager(&net);
        let rt = MemRuntime::new(&net);
        assert!(!rt.same_runtime(&rt.clone()));
        assert!(mgr.downgrade().share_on(&rt).is_none());
        assert!(mgr.share().is_some());
    });
}
