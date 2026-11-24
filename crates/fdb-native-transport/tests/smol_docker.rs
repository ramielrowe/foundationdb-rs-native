//! The peer manager over the smol adapter (`SmolRuntime`, feature `smol`) against real,
//! disposable FDB servers started by `fdb-native-testkit`: connect with version detection,
//! many concurrent requests, liveness over a few seconds of pings, and shutdown. The smol
//! counterpart of `peer_docker.rs`, with no tokio in the binary. Run with
//! `cargo nextest run -p fdb-native-transport --features smol --run-ignored only -E
//! 'binary(smol_docker)'`.
//!
//! Each test runs against both pinned images, 7.4.10 and 8.0.1, so `FDB_IMAGE` does not apply.

#![cfg(feature = "smol")]

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use fdb_native_testkit::FdbContainer;
use fdb_native_transport::connection::well_known;
use fdb_native_transport::peer::{
    DiscardEvents, Generation, PeerHandle, PeerManager, PeerManagerConfig, PeerPhase,
    ProtocolInfoReply, ProtocolInfoRequest,
};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::{Runtime, SmolRuntime};
use fdb_native_wire::{EncodeError, NetworkAddress, ProtocolVersion, ReplyPromise};
use futures_lite::future::{block_on, or};

const IMAGES: [(&str, ProtocolVersion); 2] = [
    ("foundationdb/foundationdb:7.4.10", ProtocolVersion::V7_4),
    ("foundationdb/foundationdb:8.0.1", ProtocolVersion::V8_0),
];
const STEP: Duration = Duration::from_secs(30);

fn address(addr: SocketAddr) -> NetworkAddress {
    NetworkAddress::new(addr.ip().into(), addr.port(), true, false)
}

async fn within<T>(rt: &SmolRuntime, what: &str, f: impl Future<Output = T>) -> T {
    or(async { Some(f.await) }, async {
        rt.sleep(STEP).await;
        None
    })
    .await
    .unwrap_or_else(|| panic!("{what}: nothing within {STEP:?}"))
}

/// A `ProtocolInfoRequest` to the well-known endpoint every fdbserver process serves.
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

/// Connect with the wrong version preferred so that detection has to work; requests, many at
/// once from smol tasks, are answered by the real server; pings keep the connection for a few
/// seconds; shutdown closes it cleanly.
#[test]
#[ignore = "requires docker"]
fn docker_smol_connect_detect_version_requests_pings_and_shutdown() {
    for (image, right) in IMAGES {
        // Blocking start, outside any async context.
        let fdb = FdbContainer::start_with_image(image).expect("container");
        let address = address(fdb.address());
        let wrong = if right == ProtocolVersion::V7_4 {
            ProtocolVersion::V8_0
        } else {
            ProtocolVersion::V7_4
        };

        block_on(async {
            let rt = SmolRuntime::new();
            let mut config = PeerManagerConfig::default();
            config.supported_versions = vec![wrong, right];
            let mgr = PeerManager::new(rt.clone(), config, DiscardEvents);
            let peer = mgr.peer(&address);

            let version = within(&rt, "connect", peer.connect())
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
            assert_eq!(status.failures, 0, "{image}: detection is no failure");

            let reply = within(&rt, "request", protocol_info(&peer))
                .await
                .unwrap_or_else(|e| panic!("{image}: {e}"));
            assert_eq!(reply.protocol_version().without_flags(), right, "{image}");

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
                    .unwrap_or_else(|e| panic!("{image}: {e}"));
                assert_eq!(reply.protocol_version().without_flags(), right);
            }
            assert_eq!(
                peer.status().generation,
                Generation(2),
                "{image}: no reconnect"
            );

            // Pings, ours and the server's, for several seconds on smol timers.
            rt.sleep(Duration::from_secs(5)).await;
            let status = peer.status();
            assert_eq!(status.phase, PeerPhase::Connected, "{image}: {status:?}");
            assert_eq!(status.generation, Generation(2), "{image}: {status:?}");
            assert_eq!(status.failures, 0, "{image}: {status:?}");
            within(&rt, "a request after the pings", protocol_info(&peer))
                .await
                .unwrap();

            within(&rt, "shutdown", mgr.shutdown()).await;
            assert_eq!(peer.status().phase, PeerPhase::Shutdown);
            assert_eq!(protocol_info(&peer).await, Err(RequestError::NotSent(None)));
        });

        drop(fdb);
    }
}
