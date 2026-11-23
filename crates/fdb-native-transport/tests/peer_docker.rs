//! The peer manager against real, disposable FDB servers started by `fdb-native-testkit`, over
//! the tokio adapter: connect, version detection, liveness over several seconds of pings, and
//! shutdown. Run with `cargo nextest run --run-ignored only`.
//!
//! Each test runs against both pinned images, 7.4.10 and 8.0.1, so `FDB_IMAGE` does not apply.

#![cfg(feature = "tokio")]

use std::net::SocketAddr;
use std::time::Duration;

use fdb_native_testkit::FdbContainer;
use fdb_native_transport::ConnectionError;
use fdb_native_transport::connection::well_known;
use fdb_native_transport::peer::{
    EventReceiver, Generation, PeerError, PeerEvent, PeerHandle, PeerManager, PeerManagerConfig,
    PeerPhase, ProtocolInfoReply, ProtocolInfoRequest, SendError, event_channel,
};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_wire::{EncodeError, NetworkAddress, ProtocolVersion, ReplyPromise, Uid};

const IMAGES: [(&str, ProtocolVersion); 2] = [
    ("foundationdb/foundationdb:7.4.10", ProtocolVersion::V7_4),
    ("foundationdb/foundationdb:8.0.1", ProtocolVersion::V8_0),
];

fn address(addr: SocketAddr) -> NetworkAddress {
    NetworkAddress::new(addr.ip().into(), addr.port(), true, false)
}

fn drain(events: &mut EventReceiver) -> Vec<PeerEvent> {
    std::iter::from_fn(|| events.try_next())
        .map(|record| record.event)
        .collect()
}

/// Asks the server for its protocol version: a `ProtocolInfoRequest` to the well-known
/// endpoint every fdbserver process serves (token 10), on both releases.
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

/// The version that is not `right`, among the two supported.
fn other(right: ProtocolVersion) -> ProtocolVersion {
    if right == ProtocolVersion::V7_4 {
        ProtocolVersion::V8_0
    } else {
        ProtocolVersion::V7_4
    }
}

/// Connect through the manager, with the wrong version preferred so that detection has to
/// work; the connection is reused; a send on it is accepted; shutdown closes it cleanly.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_connect_detect_version_and_shutdown() {
    for (image, right) in IMAGES {
        let fdb = tokio::task::spawn_blocking(move || FdbContainer::start_with_image(image))
            .await
            .unwrap()
            .expect("container");
        let address = address(fdb.address());

        let mut config = PeerManagerConfig::default();
        config.supported_versions = vec![other(right), right];
        let (sink, mut events) = event_channel();
        let mgr = PeerManager::new(TokioRuntime::new(), config, sink);
        let peer = mgr.peer(&address);

        let version = peer
            .connect()
            .await
            .unwrap_or_else(|e| panic!("{image}: {e}"));
        assert_eq!(version, right, "{image}");
        assert_eq!(mgr.cluster_version(), Some(right));
        let status = peer.status();
        assert_eq!(status.phase, PeerPhase::Connected, "{image}: {status:?}");
        assert_eq!(
            status.generation,
            Generation(2),
            "wrong version, then right"
        );
        assert_eq!(
            status.failures, 0,
            "{image}: version detection is no failure"
        );
        assert!(!peer.is_failed());

        let seen = drain(&mut events);
        assert!(
            matches!(
                &seen[..],
                [
                    PeerEvent::Closed {
                        error: PeerError::Connection(ConnectionError::Incompatible { .. }),
                        established: false,
                        ..
                    },
                    PeerEvent::Connected { version: v, peer: packet, .. }
                ] if *v == right && packet.connection_id == 1
            ),
            "{image}: {seen:?}"
        );

        // The connection is shared and stays: connecting again does not reconnect.
        assert_eq!(mgr.peer(&address).connect().await, Ok(right));
        assert_eq!(peer.status().generation, Generation(2));
        // A real request: the server answers with the protocol version it speaks.
        let reply = protocol_info(&peer)
            .await
            .unwrap_or_else(|e| panic!("{image}: {e}"));
        assert_eq!(reply.protocol_version().without_flags(), right, "{image}");
        assert_eq!(peer.status().generation, Generation(2), "{image}");
        // The reply says which version it arrived on: the one this connection speaks.
        let raw = peer
            .request(well_known::PROTOCOL_INFO, |reply, _version, out| {
                let request = ProtocolInfoRequest {
                    reply: ReplyPromise::new(reply),
                };
                out.extend_from_slice(&fdb_native_wire::encode(&request)?);
                Ok::<(), EncodeError>(())
            })
            .await
            .unwrap_or_else(|e| panic!("{image}: {e}"));
        assert_eq!(raw.version, right, "{image}");
        assert_eq!(
            raw.decode::<ProtocolInfoReply>()
                .unwrap()
                .protocol_version()
                .without_flags(),
            right,
            "{image}"
        );
        // Many at once share the connection, each with its own reply token.
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
        assert_eq!(
            peer.status().generation,
            Generation(2),
            "{image}: no reconnect"
        );

        // A frame to a stream token the server does not have is accepted, and the server
        // answers with endpoint-not-found: the failure monitor learns it.
        let missing = Uid::new(0x5eed_1234_5678_9a00 | 1, 0xfeed_0000_dead_0001);
        let payload = fdb_native_wire::encode(&Uid::new(1, 2)).unwrap();
        assert_eq!(peer.send(missing, payload).await, Ok(Generation(2)));
        peer.failure_monitor()
            .on_state_changed(&address, missing)
            .await;
        assert!(
            peer.failure_monitor()
                .endpoint_status(&address, missing)
                .is_failed()
        );
        assert!(!peer.is_failed(), "the address itself is fine");

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
        assert_eq!(peer.send(missing, vec![1]).await, Err(SendError::Shutdown));
        assert_eq!(
            protocol_info(&peer).await,
            Err(RequestError::NotSent(None)),
            "{image}"
        );
    }
}

/// Over five seconds of the monitor's pings the connection stays up with no failure: the
/// server answers ours and we answer its.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_connection_survives_pings() {
    for (image, right) in IMAGES {
        let fdb = tokio::task::spawn_blocking(move || FdbContainer::start_with_image(image))
            .await
            .unwrap()
            .expect("container");
        let address = address(fdb.address());
        let (sink, mut events) = event_channel();
        let mgr = PeerManager::new(TokioRuntime::new(), PeerManagerConfig::default(), sink);
        let peer = mgr.peer(&address);
        // Announce the right version from the start (the cluster version is learned from
        // the first connection of any manager in a real client; here, from this one).
        assert!(peer.connect().await.is_ok());
        let generation = peer.status().generation;

        tokio::time::sleep(Duration::from_secs(5)).await;
        let status = peer.status();
        assert_eq!(status.phase, PeerPhase::Connected, "{image}: {status:?}");
        assert_eq!(status.generation, generation, "{image}: it reconnected");
        assert_eq!(status.failures, 0, "{image}: {status:?}");
        assert_eq!(status.version, Some(right));
        let closed_after_connect = drain(&mut events)
            .into_iter()
            .filter(|e| {
                matches!(
                    e,
                    PeerEvent::Closed {
                        established: true,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(closed_after_connect, 0, "{image}");
        mgr.shutdown().await;
    }
}
