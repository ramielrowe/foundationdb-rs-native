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
    assert!(err.certainly_not_delivered() && !err.should_retry() && !err.maybe_delivered());
    assert_eq!(registry.len(), 1);
    assert!(server.read(&mut conn).is_empty());
}

/// An encoder that shortens the shared output buffer destroyed the frames before its own: the
/// connection fails (nothing is written) and the request, which was never written, is `NotSent`
/// with the reason. The request queued before it completes with the connection's end: not
/// sent if none of it had been written (its frame was destroyed), maybe delivered if it had.
#[test]
fn an_encoder_that_shortens_the_output_fails_the_connection() {
    for written in [false, true] {
        let (first, mut conn) = shortened_output(written);
        if written {
            assert!(matches!(
                first,
                Err(RequestError::MaybeDelivered(Some(
                    ConnectionError::InternalEncode(_)
                )))
            ));
        } else {
            assert!(matches!(
                first,
                Err(RequestError::NotSent(Some(
                    ConnectionError::InternalEncode(_)
                )))
            ));
        }
        assert!(conn.poll_event().is_none());
    }
}

/// Sends a request, writes it if `written`, then sends one whose encoder shortens the output.
/// Returns the first request's outcome.
fn shortened_output(written: bool) -> (Result<Vec<u8>, RequestError>, Connection) {
    let mut registry = Registry::new(registry_config(2));
    let (mut conn, _server) = connected(&mut registry);
    registry
        .send_with(&mut conn, EP_A, 1, |reply, out| {
            out.extend(fdb_native_wire::encode(&EchoRequest {
                reply: ReplyPromise::new(reply),
                value: 5,
            })?);
            Ok(())
        })
        .unwrap();
    if written {
        // Half of it (writing all of it would empty the buffer, and an encoder that replaces
        // an empty buffer cannot be caught): the rest is in the buffer the next one shortens.
        conn.consume_output(conn.pending_output().len() / 2);
    }
    let err = registry
        .send_with(&mut conn, EP_A, 2, |_, out| {
            *out = vec![1, 2, 3];
            Ok(())
        })
        .unwrap_err();
    let RequestError::NotSent(Some(ConnectionError::InternalEncode(_))) = &err else {
        panic!("{err:?}");
    };
    assert!(err.certainly_not_delivered());
    assert!(conn.pending_output().is_empty());
    let events = drain(&mut registry, &mut conn);
    let done = completions(&events);
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].key, 1);
    let first = done[0].result.clone();
    assert!(registry.is_empty());
    (first, conn)
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
    assert!(!err.certainly_not_delivered() && !err.should_retry() && !err.maybe_delivered());
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
    assert!(err.maybe_committed() && !err.certainly_not_delivered());
    // A reply whose ErrorOr has no arm set decodes as default_error_or: maybe delivered.
    let empty = fdb_native_wire::encode_with_identifier(
        &fdb_native_wire::Void,
        <ErrorOr<EnsureTable<EchoReply>>>::FILE_IDENTIFIER.value(),
    )
    .unwrap();
    let err = decode_reply::<EchoReply>(&empty).unwrap_err();
    assert_eq!(
        err,
        RequestError::Fdb(FdbError::new(error_code::DEFAULT_ERROR_OR))
    );
    assert!(err.maybe_committed() && !err.certainly_not_delivered());
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
    assert!(pending_error.maybe_delivered() && !pending_error.certainly_not_delivered());
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
    assert!(err.should_retry() && err.certainly_not_delivered() && !err.maybe_delivered());
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
    assert!(err.certainly_not_delivered() && !err.should_retry() && !err.maybe_delivered());
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
    // tryGetReply reports a broken promise as request_maybe_delivered.
    assert_eq!(err.fdb_code(), error_code::REQUEST_MAYBE_DELIVERED);
    assert!(err.maybe_committed() && !err.certainly_not_delivered());
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
    assert!(not_sent.should_retry() && !not_sent.maybe_committed());
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
    // Sent between the handshake and the draining of its event: already sent, and the
    // connection says the held request was released, so it is sent too.
    let b = send_echo(&mut registry, &mut conn, EP_B, 2, 0).unwrap();
    assert_eq!(registry.state(a), Some(RequestState::Sent));
    assert_eq!(registry.state(b), Some(RequestState::Sent));
    let events = drain(&mut registry, &mut conn);
    assert!(matches!(events[..], [RpcEvent::Connected { .. }]));
    assert_eq!(registry.state(a), Some(RequestState::Sent));
    let c = send_echo(&mut registry, &mut conn, EP_C, 3, 0).unwrap();
    assert_eq!(registry.state(c), Some(RequestState::Sent));
    assert_eq!(server.requests(&mut conn).len(), 3);

    conn.io_error(std::io::ErrorKind::ConnectionReset, t(20));
    let error = ConnectionError::Io(std::io::ErrorKind::ConnectionReset.into());
    let events = drain(&mut registry, &mut conn);
    let done = completions(&events);
    assert_eq!(
        done.iter().map(|c| c.key).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    for c in done {
        let err = c.result.as_ref().unwrap_err();
        assert_eq!(err, &RequestError::MaybeDelivered(Some(error.clone())));
        assert_eq!(err.fdb_code(), error_code::REQUEST_MAYBE_DELIVERED);
        assert!(err.maybe_committed() && !err.certainly_not_delivered());
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
    assert!(err.should_retry() && !err.maybe_delivered());
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
    assert!(err.should_retry() && !err.maybe_delivered());
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
    let ep_pending = RequestError::EndpointNotFound {
        token: EP_A,
        maybe_delivered: true,
    };
    let ep_refused = RequestError::EndpointNotFound {
        token: EP_A,
        maybe_delivered: false,
    };
    let cases: Vec<(RequestError, u16, bool, bool, bool)> = vec![
        (
            RequestError::Fdb(FdbError::new(1020)),
            1020,
            false,
            false,
            false,
        ),
        (
            RequestError::Fdb(FdbError::new(1021)),
            1021,
            false,
            false,
            true,
        ),
        (
            RequestError::Fdb(FdbError::new(1030)),
            1030,
            false,
            false,
            true,
        ),
        // An ErrorOr with no arm set: delivered, outcome unknown.
        (
            RequestError::Fdb(FdbError::new(1035)),
            1035,
            false,
            false,
            true,
        ),
        (RequestError::BrokenPromise, 1030, false, false, true),
        (
            RequestError::MaybeDelivered(Some(conn_err.clone())),
            1030,
            false,
            false,
            true,
        ),
        (RequestError::MaybeDelivered(None), 1030, false, false, true),
        (
            RequestError::NotSent(Some(conn_err)),
            1026,
            true,
            true,
            false,
        ),
        (RequestError::NotSent(None), 1026, true, true, false),
        (ep_pending, 1030, false, false, true),
        (ep_refused, 1030, true, true, false),
        (
            RequestError::Unauthorized { token: EP_A },
            6001,
            true,
            false,
            false,
        ),
        // A reply came but cannot be read: delivered, outcome unknown.
        (
            RequestError::Decode(DecodeError::TooShort { len: 0 }),
            1044,
            false,
            false,
            true,
        ),
        (
            RequestError::Encode(EncodeError::TooLarge),
            4100,
            true,
            false,
            false,
        ),
        (RequestError::Cancelled, 1101, false, false, true),
        (RequestError::Backpressure, 1026, true, true, false),
        (
            RequestError::TooManyPending { limit: 1 },
            1026,
            true,
            true,
            false,
        ),
    ];
    for (err, code, not_delivered, retry, maybe) in cases {
        assert_eq!(err.fdb_code(), code, "{err:?}");
        assert_eq!(err.fdb_error(), FdbError::new(code), "{err:?}");
        assert_eq!(err.certainly_not_delivered(), not_delivered, "{err:?}");
        assert_eq!(err.should_retry(), retry, "{err:?}");
        assert_eq!(err.maybe_delivered(), maybe, "{err:?}");
        assert_eq!(err.maybe_committed(), maybe, "{err:?}");
        // Never both; and only what was certainly not delivered is retried.
        assert!(!(not_delivered && maybe), "{err:?}");
        assert!(!retry || not_delivered, "{err:?}");
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
                    // broken_promise decodes as BrokenPromise, whose code is 1030.
                    expect.insert(key, Ok(code.map(|c| if c == 1100 { 1030 } else { c })));
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
                                RequestError::MaybeDelivered(Some(ConnectionError::LocalClose))
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

// ---- review reproductions ------------------------------------------------------------------

/// Runs a call that misuses the registry: it `debug_assert`s in a debug build (and still
/// leaves the registry consistent), and goes through otherwise.
fn misuse<R>(f: impl FnOnce() -> R) -> Option<R> {
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    assert_eq!(
        r.is_err(),
        cfg!(debug_assertions),
        "debug_assert in debug builds only"
    );
    r.ok()
}

/// Every reply token in any request frame the client wrote.
#[derive(Default)]
struct Wiretap {
    dec: Option<FrameDecoder>,
    hello: bool,
    seen: HashSet<Uid>,
}

impl Wiretap {
    fn write(&mut self, conn: &mut Connection) {
        let out = conn.pending_output().to_vec();
        conn.consume_output(out.len());
        let dec = self
            .dec
            .get_or_insert_with(|| FrameDecoder::before_handshake(frame_config()));
        dec.push(&out);
        if !self.hello {
            match ConnectPacket::decode(dec.peek()) {
                Ok((_, n)) => {
                    self.hello = true;
                    dec.consume(n).unwrap();
                }
                Err(_) => return,
            }
        }
        while let Some(f) = dec.next_frame().expect("framing") {
            if f.token == well_known::PING_PACKET {
                continue;
            }
            let r: EchoRequest = fdb_native_wire::decode(f.payload).expect("echo");
            self.seen.insert(r.reply.token());
        }
    }
}

#[test]
fn a_connection_dropped_undrained_fails_its_requests_as_maybe_delivered() {
    let mut reg = Registry::new(registry_config(1));
    let mut a = Connection::new(conn_config(), t(0));
    let mut tap = Wiretap::default();
    let tok = send_echo(&mut reg, &mut a, EP_A, 1, 0).unwrap();
    assert_eq!(reg.state(tok), Some(RequestState::Queued));
    a.recv(&server_hello(), t(1)); // established: the held frame is now output
    tap.write(&mut a); // and the server has it
    assert!(tap.seen.contains(&tok));
    // Driver bug: `a` is dropped without draining it through the registry.
    drop(a);
    let mut b = Connection::new(conn_config(), t(2));
    b.io_error(std::io::ErrorKind::ConnectionRefused, t(3));
    let mut events = misuse(|| drain(&mut reg, &mut b)).unwrap_or_default();
    events.extend(drain(&mut reg, &mut b));
    assert_eq!(
        events,
        vec![
            RpcEvent::Completion(Completion {
                key: 1,
                reply_token: tok,
                endpoint: EP_A,
                result: Err(RequestError::MaybeDelivered(None)),
            }),
            RpcEvent::Closed(ConnectionError::Io(
                std::io::ErrorKind::ConnectionRefused.into()
            )),
        ]
    );
    let err = RequestError::MaybeDelivered(None);
    assert!(err.maybe_committed() && !err.certainly_not_delivered());
}

#[test]
fn the_registry_is_bound_to_its_connection() {
    let mut reg = Registry::new(registry_config(1));
    let mut a = Connection::new(conn_config(), t(0));
    let tok = send_echo(&mut reg, &mut a, EP_A, 1, 0).unwrap();
    let mut b = Connection::new(conn_config(), t(0));
    assert_ne!(a.id(), b.id());
    // Cancelling through the wrong connection fails the request instead (its state on `a`
    // is unknown) and returns nothing.
    assert_eq!(misuse(|| reg.cancel(&mut b, tok)).flatten(), None);
    assert!(reg.is_empty());
    assert_eq!(
        completions(&drain(&mut reg, &mut b))
            .iter()
            .map(|c| (c.key, c.result.clone()))
            .collect::<Vec<_>>(),
        vec![(1, Err(RequestError::MaybeDelivered(None)))]
    );
    // The token is still registered on `a` until the registry sees `a` again.
    assert!(a.is_registered(tok));
    assert_eq!(reg.poll_event(&mut a), None);
    assert!(!a.is_registered(tok), "token leaked on the real connection");
}

#[test]
fn a_close_consumed_elsewhere_still_completes_the_requests() {
    let mut reg = Registry::new(registry_config(1));
    let (mut conn, mut server) = connected(&mut reg);
    let tok = send_echo(&mut reg, &mut conn, EP_A, 1, 0).unwrap();
    server.requests(&mut conn);
    conn.io_error(std::io::ErrorKind::ConnectionReset, t(20));
    // Someone polls the connection directly and takes its Closed.
    while conn.poll_event().is_some() {}
    let error = ConnectionError::Io(std::io::ErrorKind::ConnectionReset.into());
    assert_eq!(
        drain(&mut reg, &mut conn),
        vec![RpcEvent::Completion(Completion {
            key: 1,
            reply_token: tok,
            endpoint: EP_A,
            result: Err(RequestError::MaybeDelivered(Some(error))),
        })]
    );
    assert!(reg.is_empty());
}

#[test]
fn a_connected_event_consumed_elsewhere_still_counts_the_handshake() {
    let mut reg = Registry::new(registry_config(1));
    let mut conn = Connection::new(conn_config(), t(0));
    let mut tap = Wiretap::default();
    let tok = send_echo(&mut reg, &mut conn, EP_A, 1, 0).unwrap();
    conn.recv(&server_hello(), t(1));
    // Someone takes the Connected event; the held request goes out.
    while conn.poll_event().is_some() {}
    assert!(conn.was_established());
    tap.write(&mut conn);
    assert!(tap.seen.contains(&tok));
    conn.io_error(std::io::ErrorKind::ConnectionReset, t(2));
    let events = drain(&mut reg, &mut conn);
    let done = completions(&events);
    assert!(
        matches!(
            done[..],
            [Completion {
                result: Err(RequestError::MaybeDelivered(Some(_))),
                ..
            }]
        ),
        "{events:?}"
    );
}

#[test]
fn a_broken_promise_with_another_file_identifier_is_maybe_delivered() {
    let mut reg = Registry::new(registry_config(1));
    let (mut conn, mut server) = connected(&mut reg);
    let tok = send_echo(&mut reg, &mut conn, EP_A, 1, 0).unwrap();
    server.requests(&mut conn);
    let env: ErrorOr<EnsureTable<OtherReply>> = Err(FdbError::new(1100));
    conn.recv(&frame(tok, &fdb_native_wire::encode(&env).unwrap()), t(20));
    let events = drain(&mut reg, &mut conn);
    // The endpoint is failed (the envelope says broken_promise)...
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RpcEvent::EndpointFailed { .. }))
    );
    // ...and the request, whose reply cannot be decoded, is maybe delivered.
    let c = completions(&events);
    let err = decode_reply::<EchoReply>(c[0].result.as_ref().unwrap()).unwrap_err();
    assert!(matches!(err, RequestError::Decode(_)), "{err:?}");
    assert!(err.maybe_committed() && !err.certainly_not_delivered());
}

#[test]
fn a_send_between_the_handshake_and_its_event_is_sent() {
    let mut reg = Registry::new(registry_config(1));
    let mut conn = Connection::new(conn_config(), t(0));
    conn.recv(&server_hello(), t(1));
    let tok = send_echo(&mut reg, &mut conn, EP_A, 1, 0).unwrap();
    assert_eq!(reg.state(tok), Some(RequestState::Sent));
}

#[test]
fn the_failed_endpoint_cap_clears_like_cpp() {
    let mut c = registry_config(1);
    c.max_failed_endpoints = 2;
    let mut reg = Registry::new(c);
    let (mut conn, _s) = connected(&mut reg);
    let mut now = 20;
    let mut notice = |reg: &mut Registry, conn: &mut Connection, wk: Uid, i: u64| {
        now += 1;
        conn.recv(&frame(wk, &uid_payload(Uid::new(0x10 | 1, i))), t(now));
        drain(reg, conn);
    };
    // C++ clears when size() > limit, before inserting: 3 are kept with a limit of 2.
    for i in 0..3 {
        notice(&mut reg, &mut conn, well_known::ENDPOINT_NOT_FOUND, i);
    }
    assert_eq!(reg.failed_endpoints().count(), 3);
    // An unauthorized notice never clears.
    notice(&mut reg, &mut conn, well_known::UNAUTHORIZED_ENDPOINT, 3);
    assert_eq!(reg.failed_endpoints().count(), 4);
    // The next not-found does.
    notice(&mut reg, &mut conn, well_known::ENDPOINT_NOT_FOUND, 4);
    assert_eq!(reg.failed_endpoints().count(), 1);
}

#[test]
fn unauthorized_is_recorded_for_a_well_known_endpoint() {
    let mut reg = Registry::new(registry_config(1));
    let (mut conn, _s) = connected(&mut reg);
    let wk = Uid::new(u64::MAX, 7);
    conn.recv(
        &frame(well_known::UNAUTHORIZED_ENDPOINT, &uid_payload(wk)),
        t(20),
    );
    drain(&mut reg, &mut conn);
    assert_eq!(
        reg.endpoint_failure(wk),
        Some(EndpointFailure::Unauthorized)
    );
}

#[derive(Clone, Debug)]
enum WireOp {
    Send(usize),
    Hello,
    HelloPartial,
    HelloRest,
    Incompatible,
    Garbage,
    Eof,
    Io,
    Close,
    Timeout,
    Write,
    Drain,
    Reply(usize),
    Broken(usize),
    NotFound(usize),
    Cancel(usize),
    CancelAll,
    Reconnect,
}

fn wire_op() -> impl Strategy<Value = WireOp> {
    prop_oneof![
        5 => (0..3usize).prop_map(WireOp::Send),
        2 => Just(WireOp::Hello),
        1 => Just(WireOp::HelloPartial),
        1 => Just(WireOp::HelloRest),
        1 => Just(WireOp::Incompatible),
        1 => Just(WireOp::Garbage),
        1 => Just(WireOp::Eof),
        1 => Just(WireOp::Io),
        1 => Just(WireOp::Close),
        1 => Just(WireOp::Timeout),
        4 => Just(WireOp::Write),
        3 => Just(WireOp::Drain),
        3 => any::<usize>().prop_map(WireOp::Reply),
        1 => any::<usize>().prop_map(WireOp::Broken),
        1 => (0..3usize).prop_map(WireOp::NotFound),
        1 => any::<usize>().prop_map(WireOp::Cancel),
        1 => Just(WireOp::CancelAll),
        1 => Just(WireOp::Reconnect),
    ]
}

/// A driver that writes and drains in arbitrary orders, with the server's wire tap.
struct Harness {
    reg: Registry,
    conn: Connection,
    tap: Wiretap,
    now: u64,
    /// Requests sent and not completed or cancelled: key → reply token.
    live: BTreeMap<u32, Uid>,
    done: HashSet<u32>,
    /// Requests completed as certainly not delivered.
    not_delivered: Vec<(u32, Uid, RequestError)>,
    /// Every reply token any server ever received.
    written: HashSet<Uid>,
    next_key: u32,
    hello_half: bool,
}

impl Harness {
    fn new() -> Harness {
        let mut c = registry_config(5);
        c.max_pending = 6;
        Harness {
            reg: Registry::new(c),
            conn: Connection::new(conn_config(), t(0)),
            tap: Wiretap::default(),
            now: 0,
            live: BTreeMap::new(),
            done: HashSet::new(),
            not_delivered: Vec::new(),
            written: HashSet::new(),
            next_key: 0,
            hello_half: false,
        }
    }

    fn write(&mut self) {
        self.tap.write(&mut self.conn);
        self.written.extend(self.tap.seen.iter().copied());
    }

    fn drain(&mut self) {
        while let Some(e) = self.reg.poll_event(&mut self.conn) {
            match e {
                RpcEvent::Completion(c) => {
                    assert!(self.done.insert(c.key), "completed twice: {}", c.key);
                    assert_eq!(self.live.remove(&c.key), Some(c.reply_token));
                    assert!(!self.conn.is_registered(c.reply_token));
                    if let Err(e) = &c.result {
                        assert!(!(e.certainly_not_delivered() && e.maybe_delivered()));
                        if e.certainly_not_delivered() {
                            self.not_delivered.push((c.key, c.reply_token, e.clone()));
                        }
                    }
                }
                RpcEvent::Closed(_) => {
                    assert!(self.live.is_empty(), "Closed with live requests");
                    assert!(!self.reg.has_queued_events());
                }
                _ => {}
            }
        }
    }

    fn feed(&mut self, bytes: &[u8]) {
        if self.conn.has_undrained_input() {
            self.drain();
        }
        self.now += 1;
        self.conn.recv(bytes, t(self.now));
    }

    fn step(&mut self, op: &WireOp) {
        match *op {
            WireOp::Send(i) => {
                let key = self.next_key;
                self.next_key += 1;
                if let Ok(tok) = send_echo(&mut self.reg, &mut self.conn, ENDPOINTS[i], key, 0) {
                    self.live.insert(key, tok);
                }
            }
            WireOp::Hello => self.feed(&server_hello()),
            WireOp::HelloPartial => {
                if !self.hello_half {
                    self.hello_half = true;
                    self.feed(&server_hello()[..10]);
                }
            }
            WireOp::HelloRest => {
                if self.hello_half {
                    self.hello_half = false;
                    self.feed(&server_hello()[10..]);
                }
            }
            WireOp::Incompatible => self.feed(
                &ConnectPacket::server(
                    ProtocolVersion::V8_0,
                    format!("10.0.0.1:{PORT}").parse().unwrap(),
                )
                .encode()
                .unwrap(),
            ),
            WireOp::Garbage => self.feed(&[0xff; 64]),
            WireOp::Eof => {
                self.now += 1;
                self.conn.recv_eof(t(self.now));
            }
            WireOp::Io => {
                self.now += 1;
                self.conn
                    .io_error(std::io::ErrorKind::ConnectionReset, t(self.now));
            }
            WireOp::Close => self.conn.close(),
            WireOp::Timeout => {
                self.now += 100_000;
                self.conn.handle_timeout(t(self.now));
            }
            WireOp::Write => self.write(),
            WireOp::Drain => self.drain(),
            WireOp::Reply(n) | WireOp::Broken(n) => {
                let candidates: Vec<Uid> = self
                    .live
                    .values()
                    .copied()
                    .filter(|t| self.tap.seen.contains(t))
                    .collect();
                if !candidates.is_empty() {
                    let tok = candidates[n % candidates.len()];
                    let payload = if matches!(op, WireOp::Broken(_)) {
                        error_reply(1100)
                    } else {
                        ok_reply(0)
                    };
                    self.feed(&frame(tok, &payload));
                }
            }
            WireOp::NotFound(i) => {
                if self.tap.hello {
                    self.feed(&frame(
                        well_known::ENDPOINT_NOT_FOUND,
                        &uid_payload(ENDPOINTS[i]),
                    ));
                }
            }
            WireOp::Cancel(n) => {
                if !self.live.is_empty() {
                    let key = *self.live.keys().nth(n % self.live.len()).unwrap();
                    let tok = self.live[&key];
                    if let Some(k) = self.reg.cancel(&mut self.conn, tok) {
                        assert_eq!(k, key);
                        self.live.remove(&key);
                        assert!(self.done.insert(key));
                        assert!(!self.conn.is_registered(tok));
                    }
                }
            }
            WireOp::CancelAll => self.reg.cancel_all(&mut self.conn),
            WireOp::Reconnect => {
                // A correct driver: close and drain the old connection before replacing it.
                self.conn.close();
                self.drain();
                assert!(self.reg.is_empty());
                self.conn = Connection::new(conn_config(), t(self.now));
                self.tap = Wiretap::default();
                self.hello_half = false;
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// No request completed as certainly not delivered ever reached a server, whatever order
    /// the driver writes and drains in; and every request completes exactly once.
    #[test]
    fn certainly_not_delivered_requests_were_never_written(
        ops in proptest::collection::vec(wire_op(), 0..80),
    ) {
        let mut h = Harness::new();
        for op in &ops {
            h.step(op);
            for (key, tok, e) in &h.not_delivered {
                prop_assert!(!h.written.contains(tok), "key {key} {e:?} written after {op:?}");
            }
        }
        h.write();
        h.conn.close();
        h.drain();
        h.write();
        for (key, tok, e) in &h.not_delivered {
            prop_assert!(!h.written.contains(tok), "key {key} {e:?} written at the end");
        }
        prop_assert!(h.live.is_empty() && h.reg.is_empty());
    }
}
