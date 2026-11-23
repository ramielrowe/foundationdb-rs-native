//! The request/reply layer against an in-memory scripted server: replies, server errors,
//! endpoint failures, connection loss before and after the handshake, cancellation, limits,
//! and random interleavings checked against a model.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use fdb_native_transport::connection::{TOKEN_STREAM_FLAG, well_known};
use fdb_native_transport::rpc::{
    Completion, EndpointFailure, RegistryConfig, ReplyRegistry, RequestError, RequestState,
    RpcEvent, decode_reply, error_code, reply_result,
};
use fdb_native_transport::runtime::Instant;
use fdb_native_transport::{
    Checksum, ConnectPacket, Connection, ConnectionConfig, ConnectionError, FrameConfig,
    FrameDecoder, FrameError, MonitorConfig, OwnedFrame,
};
use fdb_native_wire::{
    DecodeError, EncodeError, EnsureTable, ErrorOr, FdbError, FileIdentified, ProtocolVersion,
    ReplyPromise, Uid, Wire,
};
use proptest::prelude::*;

const PORT: u16 = 4500;

// ---- messages ------------------------------------------------------------------------------

#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 9_100_001)]
struct EchoReply {
    value: u64,
}

#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 9_100_002)]
struct EchoRequest {
    reply: ReplyPromise<EchoReply>,
    value: u64,
}

/// Another reply type, for the file identifier check.
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 9_100_003)]
struct OtherReply {
    value: u64,
}

fn ok_reply(value: u64) -> Vec<u8> {
    let envelope: ErrorOr<EnsureTable<EchoReply>> = Ok(EnsureTable(EchoReply { value }));
    fdb_native_wire::encode(&envelope).unwrap()
}

fn error_reply(code: u16) -> Vec<u8> {
    let envelope: ErrorOr<EnsureTable<EchoReply>> = Err(FdbError::new(code));
    fdb_native_wire::encode(&envelope).unwrap()
}

// ---- the scripted peer ---------------------------------------------------------------------

fn t(ms: u64) -> Instant {
    Instant::from_origin(Duration::from_millis(ms))
}

fn conn_config() -> ConnectionConfig {
    let mut c = ConnectionConfig::new(
        ConnectPacket::multi_version_client(ProtocolVersion::V7_4, 0x1234_5678),
        false,
    );
    c.dialed_port = Some(PORT);
    c.monitor = MonitorConfig::FDB_DEFAULT.without_jitter();
    c.seed = 42;
    c
}

fn registry_config(seed: u64) -> RegistryConfig {
    let mut c = RegistryConfig::new();
    c.seed = seed;
    c
}

fn server_hello() -> Vec<u8> {
    ConnectPacket::server(
        ProtocolVersion::V7_4,
        format!("10.0.0.1:{PORT}").parse().unwrap(),
    )
    .encode()
    .unwrap()
}

fn frame_config() -> FrameConfig {
    FrameConfig::new(Checksum::for_tls(false))
}

fn frame(token: Uid, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    frame_config()
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
    hello: bool,
}

impl Server {
    fn new() -> Server {
        Server {
            decoder: FrameDecoder::before_handshake(frame_config()),
            hello: false,
        }
    }

    /// Takes everything the client has written and returns the frames in it.
    fn read(&mut self, conn: &mut Connection) -> Vec<OwnedFrame> {
        let out = conn.pending_output().to_vec();
        conn.consume_output(out.len());
        self.decoder.push(&out);
        if !self.hello {
            match ConnectPacket::decode(self.decoder.peek()) {
                Ok((_, n)) => {
                    self.hello = true;
                    self.decoder.consume(n).expect("handshake prefix");
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

    /// The echo requests the client wrote: (endpoint, reply token, value).
    fn requests(&mut self, conn: &mut Connection) -> Vec<(Uid, Uid, u64)> {
        self.read(conn)
            .into_iter()
            .filter(|f| f.token != well_known::PING_PACKET)
            .map(|f| {
                let r: EchoRequest = fdb_native_wire::decode(&f.payload).expect("echo request");
                (f.token, r.reply.token(), r.value)
            })
            .collect()
    }
}

type Registry = ReplyRegistry<u32>;

fn drain(registry: &mut Registry, conn: &mut Connection) -> Vec<RpcEvent<u32>> {
    let mut out = Vec::new();
    while let Some(e) = registry.poll_event(conn) {
        out.push(e);
    }
    out
}

fn completions(events: &[RpcEvent<u32>]) -> Vec<&Completion<u32>> {
    events
        .iter()
        .filter_map(|e| match e {
            RpcEvent::Completion(c) => Some(c),
            _ => None,
        })
        .collect()
}

/// A connection that has completed its handshake, with the server's view of it.
fn connected(registry: &mut Registry) -> (Connection, Server) {
    let mut conn = Connection::new(conn_config(), t(0));
    let mut server = Server::new();
    assert!(server.read(&mut conn).is_empty());
    conn.recv(&server_hello(), t(10));
    let events = drain(registry, &mut conn);
    assert!(
        matches!(events[..], [RpcEvent::Connected { .. }]),
        "{events:?}"
    );
    (conn, server)
}

fn send_echo(
    registry: &mut Registry,
    conn: &mut Connection,
    endpoint: Uid,
    key: u32,
    value: u64,
) -> Result<Uid, RequestError> {
    registry.send_message(conn, endpoint, key, |reply| EchoRequest {
        reply: ReplyPromise::new(reply),
        value,
    })
}

const EP_A: Uid = Uid::new(0x1000 | TOKEN_STREAM_FLAG, 0xa);
const EP_B: Uid = Uid::new(0x2000 | TOKEN_STREAM_FLAG, 0xb);
const EP_C: Uid = Uid::new(0x3000 | TOKEN_STREAM_FLAG, 0xc);

// ---- replies -------------------------------------------------------------------------------

#[test]
fn a_reply_completes_the_request_and_unregisters_its_token() {
    let mut registry = Registry::new(registry_config(1));
    let (mut conn, mut server) = connected(&mut registry);
    let token = send_echo(&mut registry, &mut conn, EP_A, 7, 41).unwrap();
    assert_eq!(registry.state(token), Some(RequestState::Sent));
    assert_eq!(registry.endpoint_of(token), Some(EP_A));
    assert_eq!(registry.pending_to(EP_A), 1);
    assert!(conn.is_registered(token));

    // The server sees the request at the endpoint, with the minted reply token in it.
    let requests = server.requests(&mut conn);
    assert_eq!(requests, vec![(EP_A, token, 41)]);

    conn.recv(&frame(token, &ok_reply(42)), t(20));
    let events = drain(&mut registry, &mut conn);
    assert_eq!(
        events,
        vec![RpcEvent::Completion(Completion {
            key: 7,
            reply_token: token,
            endpoint: EP_A,
            result: Ok(ok_reply(42)),
        })]
    );
    let RpcEvent::Completion(c) = &events[0] else {
        unreachable!()
    };
    let payload = c.result.as_ref().unwrap();
    assert_eq!(
        decode_reply::<EchoReply>(payload),
        Ok(EchoReply { value: 42 })
    );
    assert!(registry.is_empty());
    assert!(!conn.is_registered(token));

    // A duplicate reply is now an unknown token, dropped without a notice.
    conn.recv(&frame(token, &ok_reply(42)), t(30));
    assert_eq!(
        drain(&mut registry, &mut conn),
        vec![RpcEvent::UnknownToken {
            token,
            reported: false
        }]
    );
}

#[test]
fn send_with_writes_the_callers_encoding() {
    let mut registry = Registry::new(registry_config(2));
    let (mut conn, mut server) = connected(&mut registry);
    let token = registry
        .send_with(&mut conn, EP_A, 1, |reply, out| {
            let request = EchoRequest {
                reply: ReplyPromise::new(reply),
                value: 5,
            };
            out.extend(fdb_native_wire::encode(&request)?);
            Ok(())
        })
        .unwrap();
    assert_eq!(server.requests(&mut conn), vec![(EP_A, token, 5)]);

    // A failing encoder sends nothing and leaves nothing registered.
    let err = registry
        .send_with(&mut conn, EP_A, 2, |_, out| {
            out.extend_from_slice(b"partial");
            Err(EncodeError::TooLarge)
        })
        .unwrap_err();
    assert_eq!(err, RequestError::Encode(EncodeError::TooLarge));
    assert!(err.is_retryable() && !err.maybe_delivered());
    assert_eq!(registry.len(), 1);
    assert!(server.read(&mut conn).is_empty());
}

#[test]
fn a_server_error_reply_maps_to_an_fdb_error() {
    let mut registry = Registry::new(registry_config(3));
    let (mut conn, mut server) = connected(&mut registry);
    let token = send_echo(&mut registry, &mut conn, EP_A, 1, 0).unwrap();
    server.requests(&mut conn);
    conn.recv(&frame(token, &error_reply(1020)), t(20));
    let events = drain(&mut registry, &mut conn);
    let [RpcEvent::Completion(c)] = &events[..] else {
        panic!("{events:?}")
    };
    let err = decode_reply::<EchoReply>(c.result.as_ref().unwrap()).unwrap_err();
    assert_eq!(err, RequestError::Fdb(FdbError::new(1020)));
    assert_eq!(err.fdb_code(), 1020);
    assert!(!err.is_retryable() && !err.maybe_delivered());
    // An error reply does not fail the endpoint.
    assert_eq!(registry.endpoint_failure(EP_A), None);
    assert!(send_echo(&mut registry, &mut conn, EP_A, 2, 0).is_ok());
}

#[test]
fn a_reply_with_another_file_identifier_is_a_decode_error() {
    let envelope: ErrorOr<EnsureTable<OtherReply>> = Ok(EnsureTable(OtherReply { value: 1 }));
    let bytes = fdb_native_wire::encode(&envelope).unwrap();
    let err = decode_reply::<EchoReply>(&bytes).unwrap_err();
    assert_eq!(
        err,
        RequestError::Decode(DecodeError::FileIdentifierMismatch {
            expected: <ErrorOr<EnsureTable<EchoReply>>>::FILE_IDENTIFIER.value(),
            found: <ErrorOr<EnsureTable<OtherReply>>>::FILE_IDENTIFIER.value(),
        })
    );
    assert_eq!(err.fdb_code(), error_code::SERIALIZATION_FAILED);
    // The error arm is checked too: it travels with the reply type's identifier.
    let envelope: ErrorOr<EnsureTable<OtherReply>> = Err(FdbError::new(1020));
    let bytes = fdb_native_wire::encode(&envelope).unwrap();
    assert!(matches!(
        decode_reply::<EchoReply>(&bytes),
        Err(RequestError::Decode(
            DecodeError::FileIdentifierMismatch { .. }
        ))
    ));
    assert!(matches!(
        decode_reply::<EchoReply>(b"short"),
        Err(RequestError::Decode(_))
    ));
    // reply_result maps an envelope decoded elsewhere.
    assert_eq!(reply_result::<u8>(Ok(3)), Ok(3));
    assert_eq!(
        reply_result::<u8>(Err(FdbError::new(1100))),
        Err(RequestError::BrokenPromise)
    );
}

#[test]
fn concurrent_requests_to_several_endpoints_complete_independently() {
    let mut registry = Registry::new(registry_config(4));
    let (mut conn, mut server) = connected(&mut registry);
    let mut sent = BTreeMap::new();
    for (key, endpoint) in [(0, EP_A), (1, EP_B), (2, EP_C), (3, EP_A), (4, EP_B)] {
        let token = send_echo(&mut registry, &mut conn, endpoint, key, u64::from(key)).unwrap();
        sent.insert(key, (token, endpoint));
    }
    assert_eq!(registry.len(), 5);
    assert_eq!(registry.pending_to(EP_A), 2);
    let requests = server.requests(&mut conn);
    assert_eq!(requests.len(), 5);
    // Reply in reverse order, all in one read, each echoing its value * 10.
    let mut bytes = Vec::new();
    for (_, reply, value) in requests.iter().rev() {
        bytes.extend(frame(*reply, &ok_reply(value * 10)));
    }
    conn.recv(&bytes, t(20));
    let events = drain(&mut registry, &mut conn);
    let done = completions(&events);
    assert_eq!(
        done.iter().map(|c| c.key).collect::<Vec<_>>(),
        vec![4, 3, 2, 1, 0]
    );
    for c in done {
        let (token, endpoint) = sent[&c.key];
        assert_eq!((c.reply_token, c.endpoint), (token, endpoint));
        let reply: EchoReply = decode_reply(c.result.as_ref().unwrap()).unwrap();
        assert_eq!(reply.value, u64::from(c.key) * 10);
    }
    assert!(registry.is_empty());
}

// ---- endpoint failures ---------------------------------------------------------------------

#[test]
fn endpoint_not_found_fails_every_request_to_that_endpoint() {
    let mut registry = Registry::new(registry_config(5));
    let (mut conn, mut server) = connected(&mut registry);
    let a1 = send_echo(&mut registry, &mut conn, EP_A, 1, 0).unwrap();
    let b = send_echo(&mut registry, &mut conn, EP_B, 2, 0).unwrap();
    let a2 = send_echo(&mut registry, &mut conn, EP_A, 3, 0).unwrap();
    server.requests(&mut conn);

    conn.recv(
        &frame(well_known::ENDPOINT_NOT_FOUND, &uid_payload(EP_A)),
        t(20),
    );
    let pending_error = RequestError::EndpointNotFound {
        token: EP_A,
        maybe_delivered: true,
    };
    assert_eq!(
        drain(&mut registry, &mut conn),
        vec![
            RpcEvent::EndpointFailed {
                token: EP_A,
                failure: EndpointFailure::NotFound
            },
            RpcEvent::Completion(Completion {
                key: 1,
                reply_token: a1,
                endpoint: EP_A,
                result: Err(pending_error.clone()),
            }),
            RpcEvent::Completion(Completion {
                key: 3,
                reply_token: a2,
                endpoint: EP_A,
                result: Err(pending_error.clone()),
            }),
        ]
    );
    assert_eq!(
        pending_error.fdb_code(),
        error_code::REQUEST_MAYBE_DELIVERED
    );
    assert!(pending_error.maybe_delivered() && !pending_error.is_retryable());
    assert!(!conn.is_registered(a1) && !conn.is_registered(a2));
    assert!(conn.is_registered(b));
    assert_eq!(registry.len(), 1);
    assert_eq!(
        registry.endpoint_failure(EP_A),
        Some(EndpointFailure::NotFound)
    );
    assert_eq!(
        registry.failed_endpoints().collect::<Vec<_>>(),
        vec![(EP_A, EndpointFailure::NotFound)]
    );

    // A later request to the failed endpoint is refused without being sent.
    let err = send_echo(&mut registry, &mut conn, EP_A, 4, 0).unwrap_err();
    assert_eq!(
        err,
        RequestError::EndpointNotFound {
            token: EP_A,
            maybe_delivered: false
        }
    );
    assert!(err.is_retryable() && !err.maybe_delivered());
    assert!(server.read(&mut conn).is_empty());

    // A repeated notice is not reported again.
    conn.recv(
        &frame(well_known::ENDPOINT_NOT_FOUND, &uid_payload(EP_A)),
        t(30),
    );
    assert_eq!(drain(&mut registry, &mut conn), vec![]);

    // The other endpoint still works.
    conn.recv(&frame(b, &ok_reply(1)), t(40));
    assert_eq!(completions(&drain(&mut registry, &mut conn))[0].key, 2);

    // Forgetting the failure lets requests through again.
    assert!(registry.forget_failed_endpoint(EP_A));
    assert!(send_echo(&mut registry, &mut conn, EP_A, 5, 0).is_ok());
}

#[test]
fn endpoint_not_found_for_a_well_known_endpoint_is_ignored() {
    let mut registry = Registry::new(registry_config(6));
    let (mut conn, mut server) = connected(&mut registry);
    let coordinator = well_known::CLIENTLEADERREG_OPENDATABASE;
    let token = send_echo(&mut registry, &mut conn, coordinator, 1, 0).unwrap();
    server.requests(&mut conn);
    conn.recv(
        &frame(well_known::ENDPOINT_NOT_FOUND, &uid_payload(coordinator)),
        t(20),
    );
    assert_eq!(drain(&mut registry, &mut conn), vec![]);
    assert_eq!(registry.state(token), Some(RequestState::Sent));
    assert_eq!(registry.endpoint_failure(coordinator), None);
}

#[test]
fn an_unauthorized_endpoint_fails_its_requests() {
    let mut registry = Registry::new(registry_config(7));
    let (mut conn, mut server) = connected(&mut registry);
    let a = send_echo(&mut registry, &mut conn, EP_A, 1, 0).unwrap();
    let b = send_echo(&mut registry, &mut conn, EP_B, 2, 0).unwrap();
    server.requests(&mut conn);
    conn.recv(
        &frame(well_known::UNAUTHORIZED_ENDPOINT, &uid_payload(EP_A)),
        t(20),
    );
    let err = RequestError::Unauthorized { token: EP_A };
    assert_eq!(
        drain(&mut registry, &mut conn),
        vec![
            RpcEvent::EndpointFailed {
                token: EP_A,
                failure: EndpointFailure::Unauthorized
            },
            RpcEvent::Completion(Completion {
                key: 1,
                reply_token: a,
                endpoint: EP_A,
                result: Err(err.clone()),
            }),
        ]
    );
    assert_eq!(err.fdb_code(), error_code::UNAUTHORIZED_ATTEMPT);
    assert!(!err.is_retryable() && !err.maybe_delivered());
    assert_eq!(
        send_echo(&mut registry, &mut conn, EP_A, 3, 0),
        Err(err.clone())
    );
    assert_eq!(registry.state(b), Some(RequestState::Sent));
    // A later not-found notice keeps the recorded reason, as C++'s emplace does.
    conn.recv(
        &frame(well_known::ENDPOINT_NOT_FOUND, &uid_payload(EP_A)),
        t(30),
    );
    assert_eq!(drain(&mut registry, &mut conn), vec![]);
    assert_eq!(
        registry.endpoint_failure(EP_A),
        Some(EndpointFailure::Unauthorized)
    );
}

#[test]
fn a_broken_promise_reply_fails_the_endpoint() {
    let mut registry = Registry::new(registry_config(8));
    let (mut conn, mut server) = connected(&mut registry);
    let a1 = send_echo(&mut registry, &mut conn, EP_A, 1, 0).unwrap();
    let a2 = send_echo(&mut registry, &mut conn, EP_A, 2, 0).unwrap();
    let b = send_echo(&mut registry, &mut conn, EP_B, 3, 0).unwrap();
    server.requests(&mut conn);
    conn.recv(&frame(a1, &error_reply(1100)), t(20));
    let events = drain(&mut registry, &mut conn);
    assert_eq!(
        events,
        vec![
            RpcEvent::Completion(Completion {
                key: 1,
                reply_token: a1,
                endpoint: EP_A,
                result: Ok(error_reply(1100)),
            }),
            RpcEvent::EndpointFailed {
                token: EP_A,
                failure: EndpointFailure::NotFound
            },
            RpcEvent::Completion(Completion {
                key: 2,
                reply_token: a2,
                endpoint: EP_A,
                result: Err(RequestError::EndpointNotFound {
                    token: EP_A,
                    maybe_delivered: true
                }),
            }),
        ]
    );
    let err = decode_reply::<EchoReply>(&error_reply(1100)).unwrap_err();
    assert_eq!(err, RequestError::BrokenPromise);
    assert_eq!(err.fdb_code(), error_code::BROKEN_PROMISE);
    assert!(err.maybe_committed() && !err.is_retryable());
    assert_eq!(registry.state(b), Some(RequestState::Sent));
    assert!(!conn.is_registered(a2));

    // From a well-known endpoint, a broken promise fails only its own request.
    let coordinator = well_known::CLIENTLEADERREG_OPENDATABASE;
    let c1 = send_echo(&mut registry, &mut conn, coordinator, 4, 0).unwrap();
    let c2 = send_echo(&mut registry, &mut conn, coordinator, 5, 0).unwrap();
    server.requests(&mut conn);
    conn.recv(&frame(c1, &error_reply(1100)), t(30));
    let events = drain(&mut registry, &mut conn);
    assert_eq!(completions(&events).len(), 1);
    assert_eq!(registry.state(c2), Some(RequestState::Sent));
    assert_eq!(registry.endpoint_failure(coordinator), None);
}

// ---- connection loss -----------------------------------------------------------------------

#[test]
fn a_connection_lost_before_the_handshake_did_not_send() {
    let mut registry = Registry::new(registry_config(9));
    let mut conn = Connection::new(conn_config(), t(0));
    let a = send_echo(&mut registry, &mut conn, EP_A, 1, 0).unwrap();
    let b = send_echo(&mut registry, &mut conn, EP_B, 2, 0).unwrap();
    assert_eq!(registry.state(a), Some(RequestState::Queued));
    assert_eq!(registry.state(b), Some(RequestState::Queued));
    conn.recv_eof(t(5));
    let error = ConnectionError::PeerClosed {
        during_handshake: true,
    };
    let not_sent = RequestError::NotSent(Some(error.clone()));
    assert_eq!(
        drain(&mut registry, &mut conn),
        vec![
            RpcEvent::Completion(Completion {
                key: 1,
                reply_token: a,
                endpoint: EP_A,
                result: Err(not_sent.clone()),
            }),
            RpcEvent::Completion(Completion {
                key: 2,
                reply_token: b,
                endpoint: EP_B,
                result: Err(not_sent.clone()),
            }),
            RpcEvent::Closed(error),
        ]
    );
    assert_eq!(not_sent.fdb_code(), error_code::CONNECTION_FAILED);
    assert!(not_sent.is_retryable() && !not_sent.maybe_committed());
    assert!(registry.is_empty());
}

#[test]
fn an_incompatible_server_never_sees_the_queued_requests() {
    let mut registry = Registry::new(registry_config(10));
    let mut conn = Connection::new(conn_config(), t(0));
    send_echo(&mut registry, &mut conn, EP_A, 1, 0).unwrap();
    let hello = ConnectPacket::server(
        ProtocolVersion::V8_0,
        format!("10.0.0.1:{PORT}").parse().unwrap(),
    );
    conn.recv(&hello.encode().unwrap(), t(5));
    let events = drain(&mut registry, &mut conn);
    let done = completions(&events);
    assert!(
        matches!(
            done[..],
            [Completion {
                result: Err(RequestError::NotSent(Some(
                    ConnectionError::Incompatible { .. }
                ))),
                ..
            }]
        ),
        "{events:?}"
    );
    assert!(matches!(events.last(), Some(RpcEvent::Closed(_))));
}

#[test]
fn a_connection_lost_after_sending_may_have_delivered() {
    let mut registry = Registry::new(registry_config(11));
    let mut conn = Connection::new(conn_config(), t(0));
    let mut server = Server::new();
    // Queued before the handshake, sent when it completes.
    let a = send_echo(&mut registry, &mut conn, EP_A, 1, 0).unwrap();
    conn.recv(&server_hello(), t(10));
    // Sent between the handshake and the draining of its event: already sent.
    let b = send_echo(&mut registry, &mut conn, EP_B, 2, 0).unwrap();
    assert_eq!(registry.state(a), Some(RequestState::Queued));
    assert_eq!(registry.state(b), Some(RequestState::Sent));
    let events = drain(&mut registry, &mut conn);
    assert!(matches!(events[..], [RpcEvent::Connected { .. }]));
    assert_eq!(registry.state(a), Some(RequestState::Sent));
    let c = send_echo(&mut registry, &mut conn, EP_C, 3, 0).unwrap();
    assert_eq!(registry.state(c), Some(RequestState::Sent));
    assert_eq!(server.requests(&mut conn).len(), 3);

    conn.io_error(std::io::ErrorKind::ConnectionReset, t(20));
    let error = ConnectionError::Io(std::io::ErrorKind::ConnectionReset);
    let events = drain(&mut registry, &mut conn);
    let done = completions(&events);
    assert_eq!(
        done.iter().map(|c| c.key).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    for c in done {
        let err = c.result.as_ref().unwrap_err();
        assert_eq!(err, &RequestError::MaybeDelivered(error.clone()));
        assert_eq!(err.fdb_code(), error_code::REQUEST_MAYBE_DELIVERED);
        assert!(err.maybe_committed() && !err.is_retryable());
    }
    assert_eq!(events.last(), Some(&RpcEvent::Closed(error)));
    assert!(registry.is_empty());
}

#[test]
fn a_request_after_the_close_is_not_sent_and_the_registry_is_reusable() {
    let mut registry = Registry::new(registry_config(12));
    let (mut conn, _) = connected(&mut registry);
    // An endpoint failure is remembered across connections.
    conn.recv(
        &frame(well_known::ENDPOINT_NOT_FOUND, &uid_payload(EP_C)),
        t(15),
    );
    drain(&mut registry, &mut conn);
    conn.close();
    // Not drained yet: the connection is closed, so nothing is sent.
    assert_eq!(
        send_echo(&mut registry, &mut conn, EP_A, 1, 0),
        Err(RequestError::NotSent(Some(ConnectionError::LocalClose)))
    );
    assert_eq!(
        drain(&mut registry, &mut conn),
        vec![RpcEvent::Closed(ConnectionError::LocalClose)]
    );

    // The next connection to the same peer.
    let (mut conn, mut server) = connected(&mut registry);
    let token = send_echo(&mut registry, &mut conn, EP_A, 2, 9).unwrap();
    assert_eq!(server.requests(&mut conn), vec![(EP_A, token, 9)]);
    assert!(matches!(
        send_echo(&mut registry, &mut conn, EP_C, 3, 0),
        Err(RequestError::EndpointNotFound {
            maybe_delivered: false,
            ..
        })
    ));
}

#[test]
fn after_the_peer_closes_its_side_requests_are_not_sent() {
    let mut registry = Registry::new(registry_config(13));
    let (mut conn, _) = connected(&mut registry);
    conn.recv_eof(t(20));
    assert_eq!(
        send_echo(&mut registry, &mut conn, EP_A, 1, 0),
        Err(RequestError::NotSent(None))
    );
    assert!(registry.is_empty());
}

// ---- cancellation --------------------------------------------------------------------------

#[test]
fn a_cancelled_request_drops_its_late_reply() {
    let mut registry = Registry::new(registry_config(14));
    let (mut conn, mut server) = connected(&mut registry);
    let token = send_echo(&mut registry, &mut conn, EP_A, 1, 0).unwrap();
    let other = send_echo(&mut registry, &mut conn, EP_A, 2, 0).unwrap();
    server.requests(&mut conn);
    assert_eq!(registry.cancel(&mut conn, token), Some(1));
    assert_eq!(registry.cancel(&mut conn, token), None);
    assert!(!conn.is_registered(token));
    assert_eq!(registry.len(), 1);
    // Nothing is sent to the server.
    assert!(server.read(&mut conn).is_empty());

    conn.recv(&frame(token, &ok_reply(1)), t(20));
    assert_eq!(
        drain(&mut registry, &mut conn),
        vec![RpcEvent::UnknownToken {
            token,
            reported: false
        }]
    );
    // The other request is unaffected.
    conn.recv(&frame(other, &ok_reply(2)), t(30));
    assert_eq!(completions(&drain(&mut registry, &mut conn))[0].key, 2);
}

#[test]
fn cancel_all_fails_everything_as_cancelled() {
    let mut registry = Registry::new(registry_config(15));
    let (mut conn, _) = connected(&mut registry);
    let a = send_echo(&mut registry, &mut conn, EP_A, 1, 0).unwrap();
    let b = send_echo(&mut registry, &mut conn, EP_B, 2, 0).unwrap();
    registry.cancel_all(&mut conn);
    assert!(registry.is_empty() && registry.has_queued_events());
    assert!(!conn.is_registered(a) && !conn.is_registered(b));
    let events = drain(&mut registry, &mut conn);
    let done = completions(&events);
    assert_eq!(done.len(), 2);
    for c in &done {
        assert_eq!(c.result, Err(RequestError::Cancelled));
    }
    assert_eq!(
        RequestError::Cancelled.fdb_code(),
        error_code::OPERATION_CANCELLED
    );
}

// ---- tokens and limits ---------------------------------------------------------------------

#[test]
fn minted_tokens_are_unique_single_reply_tokens() {
    let mut registry = Registry::new(registry_config(16));
    let mut seen = HashSet::new();
    for _ in 0..100_000 {
        let token = registry.mint_token();
        assert_eq!(token.first & TOKEN_STREAM_FLAG, 0);
        assert!(!well_known::is_well_known(token));
        assert_ne!(token, Uid::default());
        assert!(seen.insert(token), "duplicate {token:?}");
    }
    // Registries seeded alike mint alike; differently, differently.
    let mut a = Registry::new(registry_config(17));
    let mut b = Registry::new(registry_config(17));
    let mut c = Registry::new(registry_config(18));
    let ta = a.mint_token();
    assert_eq!(ta, b.mint_token());
    assert_ne!(ta, c.mint_token());
}

#[test]
fn the_pending_count_is_capped() {
    let mut config = registry_config(19);
    config.max_pending = 3;
    let mut registry = Registry::new(config);
    let (mut conn, mut server) = connected(&mut registry);
    let first = send_echo(&mut registry, &mut conn, EP_A, 0, 0).unwrap();
    for key in 1..3 {
        send_echo(&mut registry, &mut conn, EP_A, key, 0).unwrap();
    }
    assert_eq!(server.requests(&mut conn).len(), 3);
    let err = send_echo(&mut registry, &mut conn, EP_A, 3, 0).unwrap_err();
    assert_eq!(err, RequestError::TooManyPending { limit: 3 });
    assert!(err.is_retryable() && !err.maybe_delivered());
    assert!(server.read(&mut conn).is_empty());
    // A completion makes room.
    conn.recv(&frame(first, &ok_reply(0)), t(20));
    drain(&mut registry, &mut conn);
    assert!(send_echo(&mut registry, &mut conn, EP_A, 4, 0).is_ok());
}

#[test]
fn backpressure_is_passed_on_and_sends_nothing() {
    let mut registry = Registry::new(registry_config(20));
    let mut config = conn_config();
    config.max_pending_output = 256;
    let mut conn = Connection::new(config, t(0));
    conn.recv(&server_hello(), t(10));
    drain(&mut registry, &mut conn);
    let mut sent = 0;
    let err = loop {
        match send_echo(&mut registry, &mut conn, EP_A, sent, 0) {
            Ok(_) => sent += 1,
            Err(e) => break e,
        }
    };
    assert!(sent > 0);
    assert_eq!(err, RequestError::Backpressure);
    assert!(err.is_retryable() && !err.maybe_delivered());
    assert_eq!(registry.len(), sent as usize);
    // Writing the output makes room again.
    let n = conn.pending_output().len();
    conn.consume_output(n);
    assert!(send_echo(&mut registry, &mut conn, EP_A, 99, 0).is_ok());
}

#[test]
fn a_request_larger_than_a_frame_is_refused() {
    let mut registry = Registry::new(registry_config(21));
    let mut config = conn_config();
    config.max_packet_len = 64;
    let mut conn = Connection::new(config, t(0));
    conn.recv(&server_hello(), t(10));
    drain(&mut registry, &mut conn);
    let err = registry
        .send_with(&mut conn, EP_A, 1, |_, out| {
            out.extend_from_slice(&[0; 128]);
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(err, RequestError::TooLarge(FrameError::Oversize { .. })),
        "{err:?}"
    );
    assert_eq!(err.fdb_code(), error_code::PLATFORM_ERROR);
    assert!(registry.is_empty());
}

#[test]
fn error_classification() {
    let conn_err = ConnectionError::Idle;
    let cases: Vec<(RequestError, u16, bool, bool)> = vec![
        (RequestError::Fdb(FdbError::new(1020)), 1020, false, false),
        (RequestError::Fdb(FdbError::new(1021)), 1021, false, true),
        (RequestError::Fdb(FdbError::new(1030)), 1030, false, true),
        (RequestError::BrokenPromise, 1100, false, true),
        (
            RequestError::MaybeDelivered(conn_err.clone()),
            1030,
            false,
            true,
        ),
        (RequestError::NotSent(Some(conn_err)), 1026, true, false),
        (RequestError::NotSent(None), 1026, true, false),
        (
            RequestError::EndpointNotFound {
                token: EP_A,
                maybe_delivered: true,
            },
            1030,
            false,
            true,
        ),
        (
            RequestError::EndpointNotFound {
                token: EP_A,
                maybe_delivered: false,
            },
            1030,
            true,
            false,
        ),
        (
            RequestError::Unauthorized { token: EP_A },
            6001,
            false,
            false,
        ),
        (
            RequestError::Decode(DecodeError::TooShort { len: 0 }),
            1044,
            false,
            false,
        ),
        (
            RequestError::Encode(EncodeError::TooLarge),
            4100,
            true,
            false,
        ),
        (RequestError::Cancelled, 1101, false, true),
        (RequestError::Backpressure, 1026, true, false),
        (RequestError::TooManyPending { limit: 1 }, 1026, true, false),
    ];
    for (err, code, retryable, maybe) in cases {
        assert_eq!(err.fdb_code(), code, "{err:?}");
        assert_eq!(err.fdb_error(), FdbError::new(code), "{err:?}");
        assert_eq!(err.is_retryable(), retryable, "{err:?}");
        assert_eq!(err.maybe_delivered(), maybe, "{err:?}");
        assert_eq!(err.maybe_committed(), maybe, "{err:?}");
        // Never both: a retryable error is one that was certainly not delivered.
        assert!(!(retryable && maybe), "{err:?}");
        assert!(!err.to_string().is_empty());
    }
    assert_eq!(
        RequestError::from_reply_error(FdbError::new(1100)),
        RequestError::BrokenPromise
    );
}

// ---- interleavings -------------------------------------------------------------------------

const ENDPOINTS: [Uid; 3] = [EP_A, EP_B, EP_C];

#[derive(Clone, Debug)]
enum Op {
    Send(usize),
    Handshake,
    Reply(usize),
    ErrorReply(usize, u16),
    NotFound(usize),
    Unauthorized(usize),
    Cancel(usize),
    Close,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0..3usize).prop_map(Op::Send),
        1 => Just(Op::Handshake),
        3 => any::<usize>().prop_map(Op::Reply),
        1 => (any::<usize>(), prop_oneof![Just(1020u16), Just(1100u16)])
            .prop_map(|(i, c)| Op::ErrorReply(i, c)),
        1 => (0..3usize).prop_map(Op::NotFound),
        1 => (0..3usize).prop_map(Op::Unauthorized),
        1 => any::<usize>().prop_map(Op::Cancel),
        1 => Just(Op::Close),
    ]
}

/// The model's view of one request.
#[derive(Clone, Debug)]
struct Model {
    endpoint: usize,
    token: Uid,
    sent: bool,
}

/// Runs `ops` against a registry and a connection, checking the outcome of every request
/// against a model. Returns the events, for determinism checks.
fn run(ops: &[Op]) -> Vec<RpcEvent<u32>> {
    let mut config = registry_config(77);
    config.max_pending = 8;
    let mut registry = Registry::new(config);
    let mut conn = Connection::new(conn_config(), t(0));
    let mut server = Server::new();
    let mut now = 0u64;
    let mut established = false;
    let mut closed = false;
    let mut next_key = 0u32;
    let mut pending: BTreeMap<u32, Model> = BTreeMap::new();
    let mut failed: BTreeMap<usize, EndpointFailure> = BTreeMap::new();
    let mut all_events = Vec::new();
    // Tokens the server has seen a request for.
    let mut delivered: HashSet<Uid> = HashSet::new();

    for op in ops {
        now += 1;
        let mut expect: BTreeMap<u32, Result<Option<u16>, RequestError>> = BTreeMap::new();
        match *op {
            Op::Send(i) => {
                let key = next_key;
                next_key += 1;
                let result = send_echo(&mut registry, &mut conn, ENDPOINTS[i], key, 0);
                // The registry checks its own limits before the connection's state.
                if pending.len() >= 8 {
                    assert_eq!(result, Err(RequestError::TooManyPending { limit: 8 }));
                } else if let Some(f) = failed.get(&i) {
                    let token = ENDPOINTS[i];
                    let want = match f {
                        EndpointFailure::NotFound => RequestError::EndpointNotFound {
                            token,
                            maybe_delivered: false,
                        },
                        _ => RequestError::Unauthorized { token },
                    };
                    assert_eq!(result, Err(want));
                } else if closed {
                    assert!(
                        matches!(result, Err(RequestError::NotSent(Some(_)))),
                        "{result:?}"
                    );
                } else {
                    let token = result.expect("send");
                    assert_eq!(token.first & TOKEN_STREAM_FLAG, 0);
                    assert!(pending.values().all(|m| m.token != token));
                    let state = registry.state(token).unwrap();
                    assert_eq!(state == RequestState::Sent, established);
                    pending.insert(
                        key,
                        Model {
                            endpoint: i,
                            token,
                            sent: established,
                        },
                    );
                }
            }
            Op::Handshake => {
                if !established && !closed {
                    conn.recv(&server_hello(), t(now));
                    established = true;
                    for m in pending.values_mut() {
                        m.sent = true;
                    }
                }
            }
            Op::Reply(n) | Op::ErrorReply(n, _) => {
                let delivered_keys: Vec<u32> = pending
                    .iter()
                    .filter(|(_, m)| delivered.contains(&m.token))
                    .map(|(k, _)| *k)
                    .collect();
                if established && !closed && !delivered_keys.is_empty() {
                    let key = delivered_keys[n % delivered_keys.len()];
                    let m = pending.remove(&key).unwrap();
                    let (payload, code) = match *op {
                        Op::ErrorReply(_, code) => (error_reply(code), Some(code)),
                        _ => (ok_reply(u64::from(key)), None),
                    };
                    conn.recv(&frame(m.token, &payload), t(now));
                    expect.insert(key, Ok(code));
                    if code == Some(1100) && !failed.contains_key(&m.endpoint) {
                        failed.insert(m.endpoint, EndpointFailure::NotFound);
                        fail_endpoint(&mut pending, &mut expect, m.endpoint, |token| {
                            RequestError::EndpointNotFound {
                                token,
                                maybe_delivered: true,
                            }
                        });
                    }
                }
            }
            Op::NotFound(i) | Op::Unauthorized(i) => {
                if established && !closed {
                    let (wk, failure) = match op {
                        Op::NotFound(_) => {
                            (well_known::ENDPOINT_NOT_FOUND, EndpointFailure::NotFound)
                        }
                        _ => (
                            well_known::UNAUTHORIZED_ENDPOINT,
                            EndpointFailure::Unauthorized,
                        ),
                    };
                    conn.recv(&frame(wk, &uid_payload(ENDPOINTS[i])), t(now));
                    let failure = *failed.entry(i).or_insert(failure);
                    fail_endpoint(&mut pending, &mut expect, i, |token| match failure {
                        EndpointFailure::NotFound => RequestError::EndpointNotFound {
                            token,
                            maybe_delivered: true,
                        },
                        _ => RequestError::Unauthorized { token },
                    });
                }
            }
            Op::Cancel(n) => {
                if !pending.is_empty() {
                    let key = *pending.keys().nth(n % pending.len()).unwrap();
                    let m = pending.remove(&key).unwrap();
                    assert_eq!(registry.cancel(&mut conn, m.token), Some(key));
                    assert!(!conn.is_registered(m.token));
                }
            }
            Op::Close => {
                if !closed {
                    conn.close();
                    closed = true;
                    for (key, m) in std::mem::take(&mut pending) {
                        expect.insert(
                            key,
                            Err(if m.sent {
                                RequestError::MaybeDelivered(ConnectionError::LocalClose)
                            } else {
                                RequestError::NotSent(Some(ConnectionError::LocalClose))
                            }),
                        );
                    }
                }
            }
        }

        let events = drain(&mut registry, &mut conn);
        let mut got = BTreeMap::new();
        for e in &events {
            if let RpcEvent::Completion(c) = e {
                let outcome = match &c.result {
                    Ok(payload) => Ok(match decode_reply::<EchoReply>(payload) {
                        Ok(reply) => {
                            assert_eq!(reply.value, u64::from(c.key));
                            None
                        }
                        Err(e) => Some(e.fdb_code()),
                    }),
                    Err(e) => Err(e.clone()),
                };
                assert!(got.insert(c.key, outcome).is_none(), "completed twice");
                assert!(!conn.is_registered(c.reply_token));
            }
        }
        assert_eq!(got, expect, "after {op:?}: {events:?}");
        if let Some(i) = events.iter().position(|e| matches!(e, RpcEvent::Closed(_))) {
            assert_eq!(i, events.len() - 1, "Closed is last");
        }
        all_events.extend(events);

        // The server reads what was written (requests become deliverable).
        if established && !closed {
            for (endpoint, token, _) in server.requests(&mut conn) {
                assert!(ENDPOINTS.contains(&endpoint));
                delivered.insert(token);
            }
        }

        // Invariants.
        assert_eq!(registry.len(), pending.len());
        for m in pending.values() {
            let state = registry.state(m.token).expect("pending");
            assert_eq!(state == RequestState::Sent, m.sent);
            assert_eq!(registry.endpoint_of(m.token), Some(ENDPOINTS[m.endpoint]));
            if !closed {
                assert!(conn.is_registered(m.token));
            }
        }
        for (i, f) in &failed {
            assert_eq!(registry.endpoint_failure(ENDPOINTS[*i]), Some(*f));
        }
    }
    all_events
}

fn fail_endpoint(
    pending: &mut BTreeMap<u32, Model>,
    expect: &mut BTreeMap<u32, Result<Option<u16>, RequestError>>,
    endpoint: usize,
    error: impl Fn(Uid) -> RequestError,
) {
    let keys: Vec<u32> = pending
        .iter()
        .filter(|(_, m)| m.endpoint == endpoint)
        .map(|(k, _)| *k)
        .collect();
    for key in keys {
        pending.remove(&key);
        expect.insert(key, Err(error(ENDPOINTS[endpoint])));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn interleavings_match_the_model(ops in proptest::collection::vec(op(), 0..60)) {
        let first = run(&ops);
        // Deterministic: the same operations give the same events.
        prop_assert_eq!(run(&ops), first);
    }
}
