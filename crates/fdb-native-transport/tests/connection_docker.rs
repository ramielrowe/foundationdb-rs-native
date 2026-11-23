//! Smoke tests of the connection state machine against real, disposable FDB servers started
//! by `fdb-native-testkit`, driven over the tokio runtime adapter: the handshake, one ping
//! exchange, and version detection. Run with `cargo nextest run --run-ignored only`.

#![cfg(feature = "tokio")]

use std::net::SocketAddr;
use std::time::Duration;

use fdb_native_testkit::FdbContainer;
use fdb_native_transport::runtime::{Runtime, TokioRuntime};
use fdb_native_transport::{
    ConnectPacket, Connection, ConnectionConfig, ConnectionError, ConnectionState, Event,
};
use fdb_native_wire::ProtocolVersion;
use futures_lite::{AsyncReadExt, AsyncWriteExt, future};

const IMAGE_7_4: &str = "foundationdb/foundationdb:7.4.10";
const IMAGE_8_0: &str = "foundationdb/foundationdb:8.0.1";

/// One process-wide transport id, as the multi-version client uses.
const TRANSPORT_ID: u64 = 0x5eed_0000_0000_0042;

/// Drives a connection to `addr` until `done` holds or the connection ends (or 15 s pass).
fn drive(
    addr: SocketAddr,
    version: ProtocolVersion,
    done: impl Fn(&Connection) -> bool,
) -> (Connection, Vec<String>) {
    let tokio = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    tokio.block_on(async {
        let rt = TokioRuntime::new();
        let mut stream = rt.connect(addr).await.expect("connect");
        let mut config = ConnectionConfig::new(
            ConnectPacket::multi_version_client(version, TRANSPORT_ID),
            false,
        );
        config.dialed_port = Some(addr.port());
        let mut conn = Connection::new(config, rt.now());
        let give_up = rt.now() + Duration::from_secs(15);
        let mut log = Vec::new();
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let out = conn.pending_output().to_vec();
            if !out.is_empty() {
                match stream.write_all(&out).await {
                    Ok(()) => conn.consume_output(out.len()),
                    Err(e) => conn.io_error(e.kind(), rt.now()),
                }
            }
            while let Some(event) = conn.poll_event() {
                log.push(format!("{event:?}"));
                if let Event::Closed(_) = event {
                    return (conn, log);
                }
            }
            if done(&conn) {
                return (conn, log);
            }
            assert!(rt.now() < give_up, "gave up: {log:?}");
            let deadline = conn.poll_timeout().unwrap_or(give_up).min(give_up);
            enum Wake {
                Read(std::io::Result<usize>),
                Timer,
            }
            let wake = future::or(async { Wake::Read(stream.read(&mut buf).await) }, async {
                rt.sleep_until(deadline).await;
                Wake::Timer
            })
            .await;
            let now = rt.now();
            match wake {
                Wake::Read(Ok(0)) => conn.recv_eof(now),
                Wake::Read(Ok(n)) => conn.recv(&buf[..n], now),
                Wake::Read(Err(e)) => conn.io_error(e.kind(), now),
                Wake::Timer => conn.handle_timeout(now),
            }
        }
    })
}

fn handshake_and_ping(image: &str, version: ProtocolVersion) {
    let fdb = FdbContainer::start_with_image(image).expect("container");
    let (conn, log) = drive(fdb.address(), version, |c| c.last_ping_rtt().is_some());
    assert_eq!(conn.state(), ConnectionState::Established, "{log:?}");
    assert!(log[0].starts_with("Connected"), "{log:?}");
    let peer = conn.peer().expect("peer packet");
    assert!(peer.protocol_version.is_compatible(version));
    assert_eq!(peer.connection_id, 1, "a server's transport id");
    assert_eq!(peer.canonical_remote_address(), Some(fdb.address()));
    eprintln!(
        "{image}: server version {:#x}, ping rtt {:?}",
        peer.protocol_version.0,
        conn.last_ping_rtt()
    );
}

#[test]
#[ignore = "requires docker"]
fn docker_handshake_and_ping_7_4() {
    handshake_and_ping(IMAGE_7_4, ProtocolVersion::V7_4);
}

#[test]
#[ignore = "requires docker"]
fn docker_handshake_and_ping_8_0() {
    handshake_and_ping(IMAGE_8_0, ProtocolVersion::V8_0);
}

/// Announcing the wrong version ends the connection with the server's version, which a
/// second connection then announces successfully.
#[test]
#[ignore = "requires docker"]
fn docker_version_detection() {
    for (image, right, wrong) in [
        (IMAGE_7_4, ProtocolVersion::V7_4, ProtocolVersion::V8_0),
        (IMAGE_8_0, ProtocolVersion::V8_0, ProtocolVersion::V7_4),
    ] {
        let fdb = FdbContainer::start_with_image(image).expect("container");
        let (conn, log) = drive(fdb.address(), wrong, |_| false);
        let error = conn.error().expect("closed").clone();
        assert!(
            matches!(error, ConnectionError::Incompatible { .. }),
            "{image}: {log:?}"
        );
        let detected = error.incompatible_version().unwrap();
        assert!(detected.is_compatible(right), "{image}: {detected}");
        let (conn, log) = drive(fdb.address(), detected, |c| {
            c.state() == ConnectionState::Established
        });
        assert_eq!(
            conn.state(),
            ConnectionState::Established,
            "{image}: {log:?}"
        );
    }
}
