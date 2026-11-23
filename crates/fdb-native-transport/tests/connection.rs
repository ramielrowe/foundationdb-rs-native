//! The sans-IO connection state machine against an in-memory scripted server: handshake,
//! version mismatch, dispatch, pings, liveness, and errors. Ping and endpoint-not-found payloads
//! come from the message fixtures FDB's own serializers wrote
//! (`tools/golden-vectors/fixtures/release-*`).

use std::path::Path;
use std::time::Duration;

use fdb_native_transport::connection::{
    PING_REPLY_OK, PingReply, PingRequest, TOKEN_STREAM_FLAG, well_known,
};
use fdb_native_transport::runtime::Instant;
use fdb_native_transport::{
    Checksum, ConnectPacket, ConnectPacketError, Connection, ConnectionConfig, ConnectionError,
    ConnectionState, Event, FrameConfig, FrameDecoder, FrameError, MonitorConfig, OwnedFrame,
    SendError,
};
use fdb_native_wire::{FileIdentified, ProtocolVersion, Reader, ReplyPromise, Uid, Void};
use proptest::prelude::*;
use serde_json::Value;

const RELEASES: [(&str, ProtocolVersion); 2] = [
    ("release-7.4", ProtocolVersion::V7_4),
    ("release-8.0", ProtocolVersion::V8_0),
];
const PORT: u16 = 4500;
const CONNECTION_ID: u64 = 0x1234_5678_9abc_def0;

// ---- fixtures ------------------------------------------------------------------------------

fn fixture(release: &str, name: &str) -> (Vec<u8>, Value) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tools/golden-vectors/fixtures")
        .join(release)
        .join(format!("{name}.json"));
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    let json: Value = serde_json::from_str(&text).expect("json");
    let hex = json["bytes"].as_str().expect("bytes");
    let bytes = (0..hex.len() / 2)
        .map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).expect("hex"))
        .collect();
    (bytes, json)
}

fn json_uid(v: &Value) -> Uid {
    Uid::new(
        v["first"].as_u64().expect("first"),
        v["second"].as_u64().expect("second"),
    )
}

/// The ping request fixture and the reply token in it.
fn ping_fixture(release: &str) -> (Vec<u8>, Uid) {
    let (bytes, json) = fixture(release, "ping_request");
    let token = json_uid(&json["root"]["value"][0]["value"][0]["value"][0]["value"]);
    (bytes, token)
}

// ---- the scripted peer ---------------------------------------------------------------------

fn t(ms: u64) -> Instant {
    Instant::from_origin(Duration::from_millis(ms))
}

fn config(version: ProtocolVersion, tls: bool) -> ConnectionConfig {
    let mut c = ConnectionConfig::new(
        ConnectPacket::multi_version_client(version, CONNECTION_ID),
        tls,
    );
    c.dialed_port = Some(PORT);
    c.monitor = MonitorConfig::FDB_DEFAULT.without_jitter();
    c.seed = 42;
    c
}

fn server_hello(version: ProtocolVersion) -> Vec<u8> {
    ConnectPacket::server(version, format!("10.0.0.1:{PORT}").parse().unwrap())
        .encode()
        .unwrap()
}

fn frame_config(tls: bool) -> FrameConfig {
    FrameConfig::new(Checksum::for_tls(tls))
}

fn frame(tls: bool, token: Uid, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    frame_config(tls)
        .encode_frame(token, payload, &mut out)
        .unwrap();
    out
}

fn uid_payload(token: Uid) -> Vec<u8> {
    fdb_native_wire::encode(&token).unwrap()
}

/// The server's view of what the client wrote.
struct Server {
    decoder: FrameDecoder,
    hello: Option<ConnectPacket>,
}

impl Server {
    fn new(tls: bool) -> Server {
        Server {
            decoder: FrameDecoder::before_handshake(frame_config(tls)),
            hello: None,
        }
    }

    /// Takes everything the client has written and returns the frames in it.
    fn read(&mut self, conn: &mut Connection) -> Vec<OwnedFrame> {
        let out = conn.pending_output().to_vec();
        conn.consume_output(out.len());
        self.decoder.push(&out);
        if self.hello.is_none() {
            match ConnectPacket::decode(self.decoder.peek()) {
                Ok((packet, n)) => {
                    self.hello = Some(packet);
                    self.decoder.consume(n);
                }
                Err(_) => return Vec::new(),
            }
        }
        let mut frames = Vec::new();
        while let Some(f) = self.decoder.next_frame().expect("client framing") {
            frames.push(f.into_owned());
        }
        frames
    }
}

/// An [`Event`] without the borrow.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Ev {
    Connected(ConnectPacket),
    Frame(OwnedFrame),
    Unknown(Uid, bool),
    NotFound(Uid),
    Unauthorized(Uid),
    Closed(ConnectionError),
}

fn events(conn: &mut Connection) -> Vec<Ev> {
    let mut out = Vec::new();
    while let Some(e) = conn.poll_event() {
        out.push(match e {
            Event::Connected { peer } => Ev::Connected(peer),
            Event::Frame(f) => Ev::Frame(f.into_owned()),
            Event::UnknownToken { token, reported } => Ev::Unknown(token, reported),
            Event::EndpointNotFound { token } => Ev::NotFound(token),
            Event::UnauthorizedEndpoint { token } => Ev::Unauthorized(token),
            Event::Closed(e) => Ev::Closed(e),
            _ => panic!("unexpected event"),
        });
    }
    out
}

/// A connection that has completed its handshake at `t(10)`, with the server's view of it.
fn connected(version: ProtocolVersion, tls: bool) -> (Connection, Server) {
    let mut conn = Connection::new(config(version, tls), t(0));
    let mut server = Server::new(tls);
    assert!(server.read(&mut conn).is_empty());
    conn.recv(&server_hello(version), t(10));
    assert!(matches!(events(&mut conn)[..], [Ev::Connected(_)]));
    (conn, server)
}

/// Sends the ping that is due and returns its reply token.
fn take_ping(conn: &mut Connection, server: &mut Server, now: Instant) -> Uid {
    assert_eq!(conn.poll_timeout(), Some(now), "a ping is due at {now:?}");
    conn.handle_timeout(now);
    let frames = server.read(conn);
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0].token, well_known::PING_PACKET);
    let reader = Reader::new(&frames[0].payload).unwrap();
    assert_eq!(reader.file_identifier(), 4707015);
    let request: PingRequest = reader.decode_root().unwrap();
    request.reply.token()
}

fn reply_to_ping(conn: &mut Connection, token: Uid, tls: bool, now: Instant) {
    let (reply, _) = fixture("release-7.4", "ping_reply");
    conn.recv(&frame(tls, token, &reply), now);
    assert_eq!(events(conn), vec![]);
}

// ---- handshake -----------------------------------------------------------------------------

#[test]
fn handshake_succeeds_on_both_releases() {
    for (_, version) in RELEASES {
        for tls in [false, true] {
            let mut conn = Connection::new(config(version, tls), t(0));
            assert_eq!(conn.state(), ConnectionState::Handshaking);
            // Our ConnectPacket goes out first, before anything else happens.
            let hello = ConnectPacket::decode(conn.pending_output()).unwrap();
            assert_eq!(hello.1, ConnectPacket::SIZE);
            assert_eq!(hello.1, conn.pending_output().len());
            assert_eq!(
                hello.0.protocol_version,
                version.with_object_serializer_flag()
            );
            assert_eq!(hello.0.connection_id, CONNECTION_ID);
            assert_eq!(hello.0.canonical_remote_port, 0);
            assert_eq!(conn.poll_timeout(), Some(t(3000)));
            assert_eq!(conn.poll_event(), None);

            conn.recv(&server_hello(version), t(10));
            let peer = ConnectPacket::server(version, "10.0.0.1:4500".parse().unwrap());
            assert_eq!(events(&mut conn), vec![Ev::Connected(peer)]);
            assert_eq!(conn.state(), ConnectionState::Established);
            assert_eq!(conn.peer_version(), Some(version));
            assert_eq!(conn.peer(), Some(&peer));
            // The monitor's first ping is a loop time after the handshake.
            assert_eq!(conn.poll_timeout(), Some(t(1010)));
        }
    }
}

#[test]
fn a_version_mismatch_reports_the_servers_version() {
    for (client, server) in [
        (ProtocolVersion::V7_4, ProtocolVersion::V8_0),
        (ProtocolVersion::V8_0, ProtocolVersion::V7_4),
    ] {
        let mut conn = Connection::new(config(client, false), t(0));
        conn.send(Uid::new(2, 3), b"held").unwrap();
        let mut bytes = server_hello(server);
        // A frame behind the packet is never parsed.
        bytes.extend(frame(false, well_known::PING_PACKET, b"x"));
        conn.recv(&bytes, t(5));
        let error = ConnectionError::Incompatible {
            local: client.with_object_serializer_flag(),
            peer: server.with_object_serializer_flag(),
        };
        assert_eq!(events(&mut conn), vec![Ev::Closed(error.clone())]);
        assert_eq!(error.incompatible_version(), Some(server));
        assert_eq!(error.fdb_error(), "incompatible_protocol_version");
        assert!(!error.is_clean());
        assert_eq!(conn.state(), ConnectionState::Closed);
        assert_eq!(conn.peer_version(), Some(server));
        assert_eq!(conn.error(), Some(&error));
        // Nothing more goes out: not the held frame, not the old ConnectPacket.
        assert!(conn.pending_output().is_empty());
        assert_eq!(conn.poll_timeout(), None);
        assert_eq!(conn.send(Uid::new(2, 3), b"x"), Err(SendError::Closed));
        assert!(conn.recv_buf(4096).is_empty());
        assert_eq!(conn.poll_event(), None);
    }
}

#[test]
fn a_compatible_patch_version_is_accepted() {
    let mut conn = Connection::new(config(ProtocolVersion::V7_4, false), t(0));
    let patched = ProtocolVersion(ProtocolVersion::V7_4.0 + 0x12);
    conn.recv(&server_hello(patched), t(1));
    assert!(matches!(events(&mut conn)[..], [Ev::Connected(_)]));
    assert_eq!(conn.peer_version(), Some(patched));
}

#[test]
fn garbage_and_bad_connect_packets_are_refused() {
    let v = ProtocolVersion::V7_4;
    let run = |bytes: &[u8]| {
        let mut conn = Connection::new(config(v, false), t(0));
        conn.recv(bytes, t(1));
        events(&mut conn)
    };

    let got = run(b"HTTP/1.1 400 Bad Request\r\n\r\n");
    assert!(
        matches!(
            &got[..],
            [Ev::Closed(ConnectionError::MalformedConnectPacket(_))]
        ),
        "{got:?}"
    );
    // Three bytes are not enough to tell.
    assert_eq!(run(b"HTT"), vec![]);
    // A length that is not 40 is refused as soon as it is in, before the rest arrives.
    let got = run(&41u32.to_le_bytes());
    assert!(matches!(
        &got[..],
        [Ev::Closed(ConnectionError::MalformedConnectPacket(_))]
    ));

    let mut packet = ConnectPacket::server(v, "10.0.0.1:4500".parse().unwrap());
    packet.protocol_version = v; // no object-serializer flag
    assert_eq!(
        run(&packet.encode().unwrap()),
        vec![Ev::Closed(ConnectionError::HandshakeRejected(
            ConnectPacketError::MissingObjectSerializerFlag(v)
        ))]
    );
    packet.protocol_version = ProtocolVersion(0);
    assert_eq!(
        run(&packet.encode().unwrap()),
        vec![Ev::Closed(ConnectionError::HandshakeRejected(
            ConnectPacketError::InvalidVersion(ProtocolVersion(0))
        ))]
    );
    let other_port = ConnectPacket::server(v, "10.0.0.1:4501".parse().unwrap());
    let got = run(&other_port.encode().unwrap());
    assert_eq!(
        got,
        vec![Ev::Closed(ConnectionError::HandshakeRejected(
            ConnectPacketError::PortMismatch {
                dialed: 4500,
                announced: 4501
            }
        ))]
    );
}

#[test]
fn the_port_check_can_be_skipped() {
    let mut c = config(ProtocolVersion::V7_4, false);
    c.dialed_port = None;
    let mut conn = Connection::new(c, t(0));
    let other = ConnectPacket::server(ProtocolVersion::V7_4, "10.0.0.1:4501".parse().unwrap());
    conn.recv(&other.encode().unwrap(), t(1));
    assert!(matches!(events(&mut conn)[..], [Ev::Connected(_)]));
}

#[test]
fn a_connect_packet_split_across_reads() {
    let hello = server_hello(ProtocolVersion::V8_0);
    for cut in 0..=hello.len() {
        let mut conn = Connection::new(config(ProtocolVersion::V8_0, false), t(0));
        conn.recv(&hello[..cut], t(1));
        if cut < hello.len() {
            assert_eq!(events(&mut conn), vec![], "{cut}");
            assert_eq!(conn.state(), ConnectionState::Handshaking);
        }
        // Through the zero-copy path this time.
        let rest = &hello[cut..];
        let buf = conn.recv_buf(4096);
        buf[..rest.len()].copy_from_slice(rest);
        conn.recv_commit(rest.len(), t(2));
        assert!(matches!(events(&mut conn)[..], [Ev::Connected(_)]), "{cut}");
    }
}

#[test]
fn frames_in_the_same_read_as_the_connect_packet() {
    let v = ProtocolVersion::V7_4;
    let mut conn = Connection::new(config(v, false), t(0));
    let a = Uid::new(10, 1);
    let b = Uid::new(12, 1);
    assert!(conn.register(a));
    assert!(conn.register(b));
    let mut bytes = server_hello(v);
    bytes.extend(frame(false, a, b"first"));
    bytes.extend(frame(false, b, b"second"));
    conn.recv(&bytes, t(1));
    let got = events(&mut conn);
    assert!(matches!(got[0], Ev::Connected(_)));
    assert_eq!(
        got[1..],
        [
            Ev::Frame(OwnedFrame {
                token: a,
                payload: b"first".to_vec()
            }),
            Ev::Frame(OwnedFrame {
                token: b,
                payload: b"second".to_vec()
            }),
        ]
    );
}

#[test]
fn the_handshake_times_out() {
    let mut conn = Connection::new(config(ProtocolVersion::V7_4, false), t(0));
    conn.recv(&server_hello(ProtocolVersion::V7_4)[..20], t(100));
    conn.handle_timeout(t(2999));
    assert_eq!(events(&mut conn), vec![]);
    conn.handle_timeout(t(3000));
    let error = ConnectionError::HandshakeTimeout(Duration::from_secs(3));
    assert_eq!(events(&mut conn), vec![Ev::Closed(error.clone())]);
    assert_eq!(error.fdb_error(), "connection_failed");
}

#[test]
fn the_peer_closing_during_the_handshake() {
    let mut conn = Connection::new(config(ProtocolVersion::V7_4, false), t(0));
    conn.recv_eof(t(5));
    assert_eq!(
        events(&mut conn),
        vec![Ev::Closed(ConnectionError::PeerClosed {
            during_handshake: true
        })]
    );
}

#[test]
fn frames_sent_before_the_handshake_are_held() {
    let v = ProtocolVersion::V8_0;
    let mut conn = Connection::new(config(v, false), t(0));
    let mut server = Server::new(false);
    conn.send(Uid::new(100, 7), b"early").unwrap();
    conn.send_message(Uid::new(100, 9), &Uid::new(5, 6))
        .unwrap();
    // Only the ConnectPacket goes out.
    assert_eq!(conn.pending_output().len(), ConnectPacket::SIZE);
    assert!(server.read(&mut conn).is_empty());
    conn.recv(&server_hello(v), t(10));
    assert!(matches!(events(&mut conn)[..], [Ev::Connected(_)]));
    let frames = server.read(&mut conn);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].token, Uid::new(100, 7));
    assert_eq!(frames[0].payload, b"early");
    assert_eq!(frames[1].token, Uid::new(100, 9));
    assert_eq!(
        fdb_native_wire::decode::<Uid>(&frames[1].payload).unwrap(),
        Uid::new(5, 6)
    );
    // After the handshake frames go straight out.
    conn.send(Uid::new(100, 8), b"later").unwrap();
    assert_eq!(server.read(&mut conn)[0].payload, b"later");
}

#[test]
fn held_frames_are_discarded_when_the_handshake_fails() {
    let mut conn = Connection::new(config(ProtocolVersion::V7_4, false), t(0));
    let mut server = Server::new(false);
    server.read(&mut conn);
    conn.send(Uid::new(100, 7), b"early").unwrap();
    conn.recv_eof(t(1));
    assert!(conn.pending_output().is_empty());
}

#[test]
fn send_errors() {
    let (mut conn, _) = connected(ProtocolVersion::V7_4, false);
    let mut c = config(ProtocolVersion::V7_4, false);
    c.max_packet_len = 64;
    let mut small = Connection::new(c, t(0));
    assert!(matches!(
        small.send(Uid::new(2, 2), &[0; 64]),
        Err(SendError::Frame(FrameError::Oversize { .. }))
    ));
    assert_eq!(
        conn.send_with(Uid::new(2, 2), |_| Err::<(), _>("no")),
        Err(SendError::Payload("no"))
    );
    assert!(conn.pending_output().is_empty());
    conn.close();
    assert_eq!(
        events(&mut conn),
        vec![Ev::Closed(ConnectionError::LocalClose)]
    );
    assert_eq!(conn.send(Uid::new(2, 2), b"x"), Err(SendError::Closed));
}

// ---- dispatch ------------------------------------------------------------------------------

#[test]
fn the_servers_ping_is_answered_with_the_fixture_reply() {
    for (release, version) in RELEASES {
        for tls in [false, true] {
            let (mut conn, mut server) = connected(version, tls);
            let (request, reply_token) = ping_fixture(release);
            conn.recv(&frame(tls, well_known::PING_PACKET, &request), t(20));
            assert_eq!(events(&mut conn), vec![], "a ping produces no event");
            let frames = server.read(&mut conn);
            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].token, reply_token);
            let (expected, _) = fixture(release, "ping_reply");
            let reply: PingReply = fdb_native_wire::decode(&frames[0].payload).unwrap();
            assert_eq!(reply, PING_REPLY_OK);
            assert_eq!(
                fdb_native_wire::decode::<PingReply>(&expected).unwrap(),
                reply
            );
        }
    }
}

#[test]
fn ping_messages_match_the_fixtures() {
    for (release, _) in RELEASES {
        let (bytes, token) = ping_fixture(release);
        let request: PingRequest = fdb_native_wire::decode(&bytes).unwrap();
        assert_eq!(request.reply.token(), token);
        assert_eq!(PingRequest::FILE_IDENTIFIER.value(), 4707015);
        let ours = fdb_native_wire::encode(&request).unwrap();
        // Not byte-identical (the vtable layout is the C++ compiler's), but the same fields.
        assert_eq!(
            fdb_native_wire::decode::<PingRequest>(&ours).unwrap(),
            request
        );
        assert_eq!(ours.len(), bytes.len(), "{release}");
        let (reply, json) = fixture(release, "ping_reply");
        assert_eq!(
            <PingReply as FileIdentified>::FILE_IDENTIFIER.value() as u64,
            json["file_identifier"].as_u64().unwrap()
        );
        assert_eq!(
            fdb_native_wire::decode::<PingReply>(&reply).unwrap(),
            PING_REPLY_OK
        );
        let (nf, json) = fixture(release, "endpoint_not_found");
        assert_eq!(
            <Uid as FileIdentified>::FILE_IDENTIFIER.value() as u64,
            json["file_identifier"].as_u64().unwrap()
        );
        let token = json_uid(&json["root"]["value"][0]["value"]);
        assert_eq!(fdb_native_wire::decode::<Uid>(&nf).unwrap(), token);
        assert_eq!(
            fdb_native_wire::decode::<Uid>(&uid_payload(token)).unwrap(),
            token,
            "{release}"
        );
    }
}

#[test]
fn well_known_tokens() {
    assert_eq!(well_known::ENDPOINT_NOT_FOUND, Uid::new(u64::MAX, 0));
    assert_eq!(well_known::PING_PACKET, Uid::new(u64::MAX, 1));
    assert_eq!(well_known::UNAUTHORIZED_ENDPOINT, Uid::new(u64::MAX, 2));
    assert_eq!(well_known::FIRST_AVAILABLE, 3);
    assert_eq!(well_known::CLIENTLEADERREG_GETLEADER, Uid::new(u64::MAX, 3));
    assert_eq!(
        well_known::CLIENTLEADERREG_OPENDATABASE,
        Uid::new(u64::MAX, 4)
    );
    assert_eq!(well_known::PROTOCOL_INFO, Uid::new(u64::MAX, 10));
    assert_eq!(well_known::CONFIGFOLLOWER_LOCK, Uid::new(u64::MAX, 24));
    assert_eq!(well_known::RESERVED_COUNT, 25);
}

#[test]
fn endpoint_not_found_and_unauthorized_from_the_fixture() {
    for (release, version) in RELEASES {
        let (mut conn, _) = connected(version, false);
        let (payload, json) = fixture(release, "endpoint_not_found");
        let token = json_uid(&json["root"]["value"][0]["value"]);
        let mut bytes = frame(false, well_known::ENDPOINT_NOT_FOUND, &payload);
        bytes.extend(frame(false, well_known::UNAUTHORIZED_ENDPOINT, &payload));
        conn.recv(&bytes, t(20));
        assert_eq!(
            events(&mut conn),
            vec![Ev::NotFound(token), Ev::Unauthorized(token)]
        );
        assert!(conn.pending_output().is_empty());
    }
}

#[test]
fn unknown_tokens() {
    let (mut conn, mut server) = connected(ProtocolVersion::V7_4, false);
    let reply = Uid::new(0x10, 5); // a reply token: no stream flag
    let stream = Uid::new(0x11, 5); // a stream token
    let well = well_known::token(5);
    let mut bytes = frame(false, reply, b"late reply");
    bytes.extend(frame(false, stream, b"request"));
    bytes.extend(frame(false, well, b"x"));
    conn.recv(&bytes, t(20));
    assert_eq!(
        events(&mut conn),
        vec![
            Ev::Unknown(reply, false),
            Ev::Unknown(stream, true),
            Ev::Unknown(well, false),
        ]
    );
    // Only the stream token is reported back, on WLTOKEN_ENDPOINT_NOT_FOUND, as FDB does.
    let frames = server.read(&mut conn);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].token, well_known::ENDPOINT_NOT_FOUND);
    assert_eq!(frames[0].payload, uid_payload(stream));
    assert_eq!(stream.first & TOKEN_STREAM_FLAG, 1);
}

#[test]
fn registration() {
    let (mut conn, mut server) = connected(ProtocolVersion::V7_4, false);
    let token = Uid::new(0x20, 1);
    assert!(conn.register(token));
    assert!(!conn.register(token));
    assert!(conn.is_registered(token));
    assert!(!conn.register(well_known::PING_PACKET));
    assert!(!conn.register(well_known::token(10)));
    conn.recv(&frame(false, token, b"reply"), t(20));
    assert_eq!(
        events(&mut conn),
        vec![Ev::Frame(OwnedFrame {
            token,
            payload: b"reply".to_vec()
        })]
    );
    assert!(conn.unregister(token));
    assert!(!conn.unregister(token));
    conn.recv(&frame(false, token, b"again"), t(30));
    assert_eq!(events(&mut conn), vec![Ev::Unknown(token, false)]);
    // The current ping's reply token cannot be taken.
    let ping = take_ping(&mut conn, &mut server, t(1010));
    assert!(!conn.register(ping));
}

#[test]
fn a_malformed_ping_ends_the_connection() {
    let (mut conn, _) = connected(ProtocolVersion::V7_4, false);
    conn.recv(
        &frame(false, well_known::PING_PACKET, b"not a message"),
        t(20),
    );
    let got = events(&mut conn);
    assert!(
        matches!(&got[..], [Ev::Closed(ConnectionError::MalformedMessage { token, .. })] if *token == well_known::PING_PACKET),
        "{got:?}"
    );
}

#[test]
fn framing_errors_end_the_connection_after_the_good_frames() {
    let token = Uid::new(0x30, 1);
    let good = frame(false, token, b"good");

    // Checksum mismatch.
    let (mut conn, _) = connected(ProtocolVersion::V7_4, false);
    conn.register(token);
    let mut bad = frame(false, token, b"bad!");
    let last = bad.len() - 1;
    bad[last] ^= 1;
    conn.recv(&[good.clone(), bad].concat(), t(20));
    let got = events(&mut conn);
    assert_eq!(got.len(), 2, "{got:?}");
    assert!(matches!(&got[0], Ev::Frame(f) if f.payload == b"good"));
    assert!(matches!(
        &got[1],
        Ev::Closed(ConnectionError::Framing(
            FrameError::ChecksumMismatch { .. }
        ))
    ));
    assert_eq!(conn.state(), ConnectionState::Closed);
    assert_eq!(conn.poll_timeout(), None);

    // Oversize and too short lengths.
    for (len, expect_oversize) in [(u32::MAX, true), (200 << 20, true), (15, false), (0, false)] {
        let (mut conn, _) = connected(ProtocolVersion::V8_0, false);
        conn.register(token);
        let mut bytes = good.clone();
        bytes.extend(len.to_le_bytes());
        bytes.extend([0; 40]);
        conn.recv(&bytes, t(20));
        let got = events(&mut conn);
        assert_eq!(got.len(), 2, "{len}: {got:?}");
        assert!(matches!(&got[0], Ev::Frame(_)));
        match &got[1] {
            Ev::Closed(ConnectionError::Framing(FrameError::Oversize { .. })) => {
                assert!(expect_oversize)
            }
            Ev::Closed(ConnectionError::Framing(FrameError::TooShort { .. })) => {
                assert!(!expect_oversize)
            }
            other => panic!("{len}: {other:?}"),
        }
    }

    // A frame with a checksum on a TLS connection is misread, as FDB would misread it.
    let (mut conn, _) = connected(ProtocolVersion::V8_0, true);
    conn.register(token);
    conn.recv(&frame(true, token, b"tls"), t(20));
    assert!(matches!(&events(&mut conn)[..], [Ev::Frame(f)] if f.payload == b"tls"));
}

#[test]
fn the_peer_closing_after_frames_delivers_them_first() {
    let (mut conn, _) = connected(ProtocolVersion::V7_4, false);
    let token = Uid::new(0x40, 1);
    conn.register(token);
    let mut bytes = frame(false, token, b"last words");
    bytes.extend([1, 2, 3]); // the start of a frame that never finishes
    conn.recv(&bytes, t(20));
    conn.recv_eof(t(21));
    assert_eq!(conn.poll_timeout(), None);
    assert_eq!(conn.send(token, b"x"), Err(SendError::Closed));
    let got = events(&mut conn);
    assert_eq!(got.len(), 2);
    assert!(matches!(&got[0], Ev::Frame(f) if f.payload == b"last words"));
    assert_eq!(
        got[1],
        Ev::Closed(ConnectionError::PeerClosed {
            during_handshake: false
        })
    );
}

#[test]
fn io_errors_end_the_connection() {
    let (mut conn, _) = connected(ProtocolVersion::V7_4, false);
    conn.io_error(std::io::ErrorKind::ConnectionReset, t(20));
    assert_eq!(
        events(&mut conn),
        vec![Ev::Closed(ConnectionError::Io(
            std::io::ErrorKind::ConnectionReset
        ))]
    );
}

// ---- ping and liveness ---------------------------------------------------------------------

#[test]
fn our_ping_cycle() {
    for tls in [false, true] {
        let (mut conn, mut server) = connected(ProtocolVersion::V8_0, tls);
        // Nothing before the first deadline.
        conn.handle_timeout(t(1009));
        assert!(server.read(&mut conn).is_empty());
        let first = take_ping(&mut conn, &mut server, t(1010));
        assert_eq!(first.first & TOKEN_STREAM_FLAG, 0, "a reply token");
        assert_eq!(conn.poll_timeout(), Some(t(3010)));
        reply_to_ping(&mut conn, first, tls, t(1060));
        assert_eq!(conn.last_ping_rtt(), Some(Duration::from_millis(50)));
        // The next ping is a loop time after the reply.
        let second = take_ping(&mut conn, &mut server, t(2060));
        assert_ne!(first, second);
        // A late reply to the first ping is not the reply to the second, and is ignored.
        conn.recv(&frame(tls, first, b"late"), t(2070));
        assert_eq!(events(&mut conn), vec![]);
        assert_eq!(conn.poll_timeout(), Some(t(4060)));
        reply_to_ping(&mut conn, second, tls, t(2080));
        assert_eq!(conn.poll_timeout(), Some(t(3080)));
    }
}

#[test]
fn a_silent_peer_fails_the_ping() {
    let (mut conn, mut server) = connected(ProtocolVersion::V7_4, false);
    take_ping(&mut conn, &mut server, t(1010));
    conn.handle_timeout(t(3009));
    assert_eq!(events(&mut conn), vec![]);
    conn.handle_timeout(t(3010));
    let error = ConnectionError::PingTimeout(Duration::from_secs(2));
    assert_eq!(events(&mut conn), vec![Ev::Closed(error.clone())]);
    assert_eq!(error.fdb_error(), "connection_failed");
    assert!(conn.pending_output().is_empty());
}

#[test]
fn any_bytes_extend_a_slow_ping() {
    let (mut conn, mut server) = connected(ProtocolVersion::V7_4, false);
    take_ping(&mut conn, &mut server, t(1010));
    // Some unrelated bytes in the first window: wait another window.
    conn.recv(&frame(false, Uid::new(0x50, 1), b"x"), t(2000));
    events(&mut conn);
    conn.handle_timeout(t(3010));
    assert_eq!(events(&mut conn), vec![]);
    assert_eq!(conn.poll_timeout(), Some(t(5010)));
    // Half a frame counts too.
    conn.recv(&[1, 0], t(4000));
    conn.handle_timeout(t(5010));
    assert_eq!(conn.poll_timeout(), Some(t(7010)));
    conn.handle_timeout(t(7010));
    assert!(matches!(
        events(&mut conn)[..],
        [Ev::Closed(ConnectionError::PingTimeout(_))]
    ));
}

#[test]
fn a_late_driver_is_not_blamed_on_the_peer() {
    let (mut conn, mut server) = connected(ProtocolVersion::V7_4, false);
    take_ping(&mut conn, &mut server, t(1010));
    conn.recv(&[1], t(2000));
    // Handled long after the window: the next window starts now, not in the past.
    conn.handle_timeout(t(9000));
    assert_eq!(conn.poll_timeout(), Some(t(11000)));
    assert_eq!(events(&mut conn), vec![]);
}

/// Answers every ping until `until`, returning the time the connection closed, if it did.
fn run_pings(
    conn: &mut Connection,
    server: &mut Server,
    until: Instant,
    mut each: impl FnMut(&mut Connection, Instant),
) -> Option<(Instant, ConnectionError)> {
    while let Some(at) = conn.poll_timeout() {
        if at > until {
            return None;
        }
        conn.handle_timeout(at);
        each(conn, at);
        if let Some(Ev::Closed(e)) = events(conn).pop() {
            return Some((at, e));
        }
        let frames = server.read(conn);
        let ping = frames
            .iter()
            .find(|f| f.token == well_known::PING_PACKET)
            .expect("a ping");
        let request: PingRequest = fdb_native_wire::decode(&ping.payload).unwrap();
        let replied = at + Duration::from_millis(10);
        conn.recv(
            &frame(
                false,
                request.reply.token(),
                &fixture("release-7.4", "ping_reply").0,
            ),
            replied,
        );
        if let Some(Ev::Closed(e)) = events(conn).pop() {
            return Some((replied, e));
        }
    }
    None
}

#[test]
fn an_idle_connection_is_closed() {
    let (mut conn, mut server) = connected(ProtocolVersion::V7_4, false);
    let (at, error) = run_pings(&mut conn, &mut server, t(200_000), |_, _| {}).expect("closed");
    assert_eq!(error, ConnectionError::Idle);
    assert!(error.is_clean());
    assert_eq!(error.fdb_error(), "connection_idle");
    // The first check after both the open and the last data are 180 s old.
    assert!(at > t(180_010) && at < t(181_100), "{at:?}");
}

#[test]
fn an_outstanding_reply_or_recent_data_keeps_a_connection() {
    let (mut conn, mut server) = connected(ProtocolVersion::V7_4, false);
    assert!(conn.register(Uid::new(0x60, 1)));
    assert!(run_pings(&mut conn, &mut server, t(400_000), |_, _| {}).is_none());

    let (mut conn, mut server) = connected(ProtocolVersion::V7_4, false);
    let mut last = t(0);
    let result = run_pings(&mut conn, &mut server, t(400_000), |conn, at| {
        if at - last > Duration::from_secs(100) {
            conn.send(Uid::new(0x60, 2), b"data").unwrap();
            last = at;
        }
    });
    assert!(result.is_none());
}

#[test]
fn an_unreferenced_connection_is_closed() {
    let (mut conn, mut server) = connected(ProtocolVersion::V7_4, false);
    conn.set_referenced(false);
    let (at, error) = run_pings(&mut conn, &mut server, t(10_000), |_, _| {}).expect("closed");
    assert_eq!(error, ConnectionError::Unreferenced);
    assert!(error.is_clean());
    // Checks follow replies at 1.02 s and 2.03 s; the second is past the 2 s delay.
    assert_eq!(at, t(2030));
}

#[test]
fn dropping_the_reference_resets_a_ping() {
    let (mut conn, mut server) = connected(ProtocolVersion::V7_4, false);
    let ping = take_ping(&mut conn, &mut server, t(1010));
    reply_to_ping(&mut conn, ping, false, t(1020));
    take_ping(&mut conn, &mut server, t(2020));
    // Still referenced: nothing. Unreferenced while the ping is out, 2 s after the last
    // data: the check runs at once (FDB's resetPing).
    conn.recv(&[], t(2500));
    conn.set_referenced(false);
    assert_eq!(
        events(&mut conn),
        vec![Ev::Closed(ConnectionError::Unreferenced)]
    );
}

// ---- splits --------------------------------------------------------------------------------

/// A server byte stream exercising every path, and the tokens registered for it.
fn script(tls: bool) -> (Vec<u8>, Vec<Uid>) {
    let (ping, _) = ping_fixture("release-8.0");
    let (nf, _) = fixture("release-8.0", "endpoint_not_found");
    let a = Uid::new(0x70, 1);
    let b = Uid::new(0x72, 2);
    let mut bytes = server_hello(ProtocolVersion::V8_0);
    bytes.extend(frame(tls, a, b"alpha"));
    bytes.extend(frame(tls, well_known::PING_PACKET, &ping));
    bytes.extend(frame(tls, Uid::new(0x71, 3), b"stream"));
    bytes.extend(frame(tls, well_known::ENDPOINT_NOT_FOUND, &nf));
    bytes.extend(frame(tls, b, &[7; 3000]));
    bytes.extend(frame(tls, Uid::new(0x74, 3), b"late"));
    (bytes, vec![a, b])
}

fn run_split(tls: bool, cuts: &[usize]) -> (Vec<Ev>, Vec<u8>) {
    let (bytes, tokens) = script(tls);
    let mut conn = Connection::new(config(ProtocolVersion::V8_0, tls), t(0));
    for token in tokens {
        conn.register(token);
    }
    let mut got = Vec::new();
    let mut output = Vec::new();
    let mut start = 0;
    let mut cuts: Vec<usize> = cuts.iter().map(|c| c % (bytes.len() + 1)).collect();
    cuts.sort_unstable();
    cuts.push(bytes.len());
    for cut in cuts {
        let chunk = &bytes[start..cut];
        start = cut;
        let buf = conn.recv_buf(4096);
        let n = chunk.len().min(buf.len());
        buf[..n].copy_from_slice(&chunk[..n]);
        conn.recv_commit(n, t(1));
        conn.recv(&chunk[n..], t(1));
        got.extend(events(&mut conn));
        output.extend_from_slice(conn.pending_output());
        let len = conn.pending_output().len();
        conn.consume_output(len);
    }
    (got, output)
}

#[test]
fn the_script_produces_the_expected_events() {
    let (got, _) = run_split(false, &[]);
    assert_eq!(got.len(), 6, "{got:?}");
    assert!(matches!(got[0], Ev::Connected(_)));
    assert!(matches!(&got[1], Ev::Frame(f) if f.payload == b"alpha"));
    assert_eq!(got[2], Ev::Unknown(Uid::new(0x71, 3), true));
    assert!(matches!(got[3], Ev::NotFound(_)));
    assert!(matches!(&got[4], Ev::Frame(f) if f.payload.len() == 3000));
    assert_eq!(got[5], Ev::Unknown(Uid::new(0x74, 3), false));
}

proptest! {
    #[test]
    fn arbitrary_splits_give_the_same_events(
        tls in any::<bool>(),
        cuts in proptest::collection::vec(any::<usize>(), 0..24),
    ) {
        let whole = run_split(tls, &[]);
        let split = run_split(tls, &cuts);
        prop_assert_eq!(split, whole);
    }
}

#[test]
fn ping_request_type_round_trips() {
    let request = PingRequest {
        reply: ReplyPromise::<Void>::new(Uid::new(8, 9)),
    };
    let bytes = fdb_native_wire::encode(&request).unwrap();
    assert_eq!(
        fdb_native_wire::decode::<PingRequest>(&bytes).unwrap(),
        request
    );
}
