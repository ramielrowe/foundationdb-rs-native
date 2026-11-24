//! Requests whose frames were never written when the connection ended are `NotSent` (as8.8):
//! the connection's written offsets against the registry's frame offsets. Fully unwritten,
//! partially written and written frames at close, the exact boundaries, writes that failed
//! after the transport may have taken part of their buffer, the handshake, reconnects, and a
//! property test over random write chunkings: `NotSent` if and only if no byte of the frame
//! was handed to the transport, and never for a frame any byte of which reached the server.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::time::Duration;

use fdb_native_transport::connection::{TOKEN_STREAM_FLAG, well_known};
use fdb_native_transport::rpc::{
    RegistryConfig, ReplyRegistry, RequestError, RequestState, RpcEvent,
};
use fdb_native_transport::runtime::Instant;
use fdb_native_transport::{
    Checksum, ConnectPacket, Connection, ConnectionConfig, FrameConfig, MonitorConfig,
};
use fdb_native_wire::{ProtocolVersion, ReplyPromise, Uid, Wire};
use proptest::prelude::*;

const PORT: u16 = 4500;

#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 9_300_001)]
struct EchoReply {
    value: u64,
}

#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 9_300_002)]
struct EchoRequest {
    reply: ReplyPromise<EchoReply>,
    value: u64,
}

const EP: Uid = Uid::new(0x1000 | TOKEN_STREAM_FLAG, 0xa);

type Registry = ReplyRegistry<u32>;

fn t(ms: u64) -> Instant {
    Instant::from_origin(Duration::from_millis(ms))
}

/// A connection whose driver (the test) reports everything the transport may have taken.
fn conn_config() -> ConnectionConfig {
    let mut c = default_config();
    c.unwritten_not_sent = true;
    c
}

/// A connection with the default: no promise about the driver's reports.
fn default_config() -> ConnectionConfig {
    let mut c = ConnectionConfig::new(
        ConnectPacket::multi_version_client(ProtocolVersion::V7_4, 0x1234_5678),
        false,
    );
    c.dialed_port = Some(PORT);
    c.monitor = MonitorConfig::FDB_DEFAULT.without_jitter();
    c.seed = 42;
    c
}

fn registry() -> Registry {
    let mut c = RegistryConfig::new();
    c.seed = 9;
    Registry::new(c)
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

/// Sends an echo request and checks that its frame starts where the connection said the
/// next frame would. Returns its reply token and frame offset.
fn send(reg: &mut Registry, conn: &mut Connection, key: u32) -> (Uid, u64) {
    let before = conn.output_offset();
    let token = reg
        .send_message(conn, EP, key, |reply| EchoRequest {
            reply: ReplyPromise::new(reply),
            value: u64::from(key),
        })
        .unwrap();
    assert_eq!(reg.frame_offset(token), Some(before));
    assert!(conn.output_offset() > before);
    (token, before)
}

/// Writes `n` bytes of output (the transport took them).
fn write(conn: &mut Connection, n: u64) {
    let n = usize::try_from(n).unwrap();
    assert!(n <= conn.pending_output().len(), "{n} > pending");
    let before = conn.written();
    conn.consume_output(n);
    assert_eq!(conn.written(), before + n as u64);
}

/// Writes output until the stream offset `to`.
fn write_to(conn: &mut Connection, to: u64) {
    let n = to - conn.written();
    write(conn, n);
}

/// A connection whose handshake is complete and drained, with nothing pending.
fn connected(reg: &mut Registry) -> Connection {
    let mut conn = Connection::new(conn_config(), t(0));
    assert_eq!(conn.output_offset(), ConnectPacket::SIZE as u64);
    write(&mut conn, ConnectPacket::SIZE as u64);
    conn.recv(&server_hello(), t(1));
    let events = drain(reg, &mut conn);
    assert!(
        matches!(events[..], [RpcEvent::Connected { .. }]),
        "{events:?}"
    );
    conn
}

fn drain(reg: &mut Registry, conn: &mut Connection) -> Vec<RpcEvent<u32>> {
    std::iter::from_fn(|| reg.poll_event(conn)).collect()
}

/// The failures of the completed requests, by key.
fn failures(events: &[RpcEvent<u32>]) -> HashMap<u32, RequestError> {
    events
        .iter()
        .filter_map(|e| match e {
            RpcEvent::Completion(c) => Some((c.key, c.result.clone().unwrap_err())),
            _ => None,
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum End {
    Io,
    Eof,
    Close,
    WriteFailed(usize),
}

fn end(conn: &mut Connection, how: End) {
    match how {
        End::Io => conn.io_error(ErrorKind::ConnectionReset, t(50)),
        End::Eof => conn.recv_eof(t(50)),
        End::Close => conn.close(),
        End::WriteFailed(offered) => conn.write_failed(offered, ErrorKind::BrokenPipe, t(50)),
    }
}

fn assert_not_sent(e: &RequestError) {
    assert!(matches!(e, RequestError::NotSent(Some(_))), "{e:?}");
    assert!(e.certainly_not_delivered() && e.should_retry() && !e.maybe_committed());
}

fn assert_maybe(e: &RequestError) {
    assert!(matches!(e, RequestError::MaybeDelivered(Some(_))), "{e:?}");
    assert!(e.maybe_committed() && !e.certainly_not_delivered() && !e.should_retry());
}

#[test]
fn unwritten_partially_written_and_written_frames_at_close() {
    for how in [End::Io, End::Eof, End::Close] {
        let mut reg = registry();
        let mut conn = connected(&mut reg);
        let (_, a) = send(&mut reg, &mut conn, 1);
        let (_, b) = send(&mut reg, &mut conn, 2);
        let (_, c) = send(&mut reg, &mut conn, 3);
        let (_, d) = send(&mut reg, &mut conn, 4);
        assert_eq!(a, ConnectPacket::SIZE as u64);
        // A whole, B half: C and D untouched.
        write_to(&mut conn, b + (c - b) / 2);
        end(&mut conn, how);
        assert_eq!(conn.maybe_written(), b + (c - b) / 2);
        let events = drain(&mut reg, &mut conn);
        let f = failures(&events);
        assert_maybe(&f[&1]);
        assert_maybe(&f[&2]);
        assert_not_sent(&f[&3]);
        assert_not_sent(&f[&4]);
        // The registry's frame offsets match the stream layout.
        assert!(a < b && b < c && c < d);
        assert!(
            matches!(events.last(), Some(RpcEvent::Closed(_))),
            "{how:?}"
        );
        assert!(reg.is_empty());
    }
}

/// One byte of a frame is enough to make it maybe delivered; up to its first byte it is not.
#[test]
fn the_boundary_is_the_first_byte_of_the_frame() {
    for extra in [-1i64, 0, 1] {
        let mut reg = registry();
        let mut conn = connected(&mut reg);
        send(&mut reg, &mut conn, 1);
        let (_, b) = send(&mut reg, &mut conn, 2);
        write_to(&mut conn, b.checked_add_signed(extra).unwrap());
        end(&mut conn, End::Io);
        let f = failures(&drain(&mut reg, &mut conn));
        assert_maybe(&f[&1]);
        if extra > 0 {
            assert_maybe(&f[&2]);
        } else {
            assert_not_sent(&f[&2]);
        }
    }
}

/// A write that failed may have taken any of what it was offered (a TLS stream can take part
/// of its buffer and then fail): exactly those frames are maybe delivered.
#[test]
fn a_failed_write_counts_what_it_was_offered() {
    for extra in [-1i64, 0, 1] {
        let mut reg = registry();
        let mut conn = connected(&mut reg);
        let (_, a) = send(&mut reg, &mut conn, 1);
        let (_, b) = send(&mut reg, &mut conn, 2);
        let (_, c) = send(&mut reg, &mut conn, 3);
        write_to(&mut conn, a + 3);
        // Offered from inside A up to around C's first byte.
        let offered =
            usize::try_from(c.checked_add_signed(extra).unwrap() - conn.written()).unwrap();
        end(&mut conn, End::WriteFailed(offered));
        assert_eq!(conn.written(), a + 3);
        assert_eq!(conn.maybe_written(), a + 3 + offered as u64);
        assert!(b < conn.maybe_written());
        let f = failures(&drain(&mut reg, &mut conn));
        assert_maybe(&f[&1]);
        assert_maybe(&f[&2]);
        if extra > 0 {
            assert_maybe(&f[&3]);
        } else {
            assert_not_sent(&f[&3]);
        }
    }
    // An offer beyond the output is clamped to it.
    let mut reg = registry();
    let mut conn = connected(&mut reg);
    send(&mut reg, &mut conn, 1);
    let end_of_output = conn.output_offset();
    conn.write_failed(usize::MAX, ErrorKind::BrokenPipe, t(5));
    assert_eq!(conn.maybe_written(), end_of_output);
}

/// A write whose outcome is unknown, on a connection that carries on: the bytes stay pending
/// and are written again, and what they carry stays maybe delivered.
#[test]
fn bytes_maybe_written_stay_pending_and_count_as_written() {
    let mut reg = registry();
    let mut conn = connected(&mut reg);
    let (_, a) = send(&mut reg, &mut conn, 1);
    let (_, b) = send(&mut reg, &mut conn, 2);
    conn.output_maybe_written(usize::try_from(b - a + 1).unwrap());
    assert_eq!(conn.written(), a);
    assert_eq!(conn.maybe_written(), b + 1);
    assert_eq!(
        conn.output_offset(),
        conn.written() + conn.pending_output().len() as u64
    );
    // Written again, in part; the mark stays.
    write(&mut conn, 2);
    assert_eq!(conn.maybe_written(), b + 1);
    let (_, c) = send(&mut reg, &mut conn, 3);
    end(&mut conn, End::Close);
    let f = failures(&drain(&mut reg, &mut conn));
    assert_maybe(&f[&1]);
    assert_maybe(&f[&2]);
    assert!(c > b);
    assert_not_sent(&f[&3]);
}

/// Requests sent during the handshake are held, but their offsets are already final: right
/// after our ConnectPacket.
#[test]
fn held_frames_have_their_offsets_before_the_handshake() {
    // The handshake fails, our ConnectPacket half written: not sent.
    let mut reg = registry();
    let mut conn = Connection::new(conn_config(), t(0));
    let (ta, a) = send(&mut reg, &mut conn, 1);
    assert_eq!(a, ConnectPacket::SIZE as u64);
    assert_eq!(reg.state(ta), Some(RequestState::Queued));
    write(&mut conn, 10);
    conn.io_error(ErrorKind::ConnectionReset, t(1));
    assert_not_sent(&failures(&drain(&mut reg, &mut conn))[&1]);

    // The handshake completes, the held frames are released after our ConnectPacket; the
    // connection ends with A half written and B not at all.
    let mut reg = registry();
    let mut conn = Connection::new(conn_config(), t(0));
    let (_, a) = send(&mut reg, &mut conn, 1);
    let (_, b) = send(&mut reg, &mut conn, 2);
    conn.recv(&server_hello(), t(1));
    let (_, c) = send(&mut reg, &mut conn, 3);
    assert_eq!(
        conn.output_offset() - a,
        conn.pending_output().len() as u64 - 44
    );
    // The released output is our ConnectPacket, then A, B and C in that order.
    let pending = conn.pending_output().to_vec();
    let config = frame_config();
    let mut at = ConnectPacket::SIZE;
    for (start, value) in [(a, 1u64), (b, 2), (c, 3)] {
        assert_eq!(at as u64, start);
        let len = u32::from_le_bytes(pending[at..at + 4].try_into().unwrap()) as usize;
        let payload = &pending[at + config.header_len() + 16..at + config.header_len() + len];
        let request: EchoRequest = fdb_native_wire::decode(payload).unwrap();
        assert_eq!(request.value, value);
        at += config.header_len() + len;
    }
    assert_eq!(at, pending.len());
    write_to(&mut conn, a + 5);
    drain(&mut reg, &mut conn);
    conn.close();
    let f = failures(&drain(&mut reg, &mut conn));
    assert_maybe(&f[&1]);
    assert_not_sent(&f[&2]);
    assert_not_sent(&f[&3]);
}

/// Ping and endpoint-not-found frames the connection queues itself move the offsets of the
/// frames after them, and the offsets still match.
#[test]
fn internal_frames_between_requests_keep_the_offsets_exact() {
    let mut reg = registry();
    let mut conn = connected(&mut reg);
    send(&mut reg, &mut conn, 1);
    // The monitor's ping goes out after A.
    conn.handle_timeout(t(60_000));
    let before = conn.output_offset();
    // An unknown stream token makes the connection answer with an endpoint-not-found frame.
    let mut unknown = Vec::new();
    frame_config()
        .encode_frame(
            Uid::new(0x77 | TOKEN_STREAM_FLAG, 1),
            &fdb_native_wire::encode(&Uid::new(7, 7)).unwrap(),
            &mut unknown,
        )
        .unwrap();
    conn.recv(&unknown, t(60_001));
    drain(&mut reg, &mut conn);
    assert!(conn.output_offset() > before);
    let (_, b) = send(&mut reg, &mut conn, 2);
    write_to(&mut conn, b);
    end(&mut conn, End::Io);
    let f = failures(&drain(&mut reg, &mut conn));
    assert_maybe(&f[&1]);
    assert_not_sent(&f[&2]);
}

/// Offsets are per connection: a registry reused on the next connection classifies its
/// requests against that connection's stream.
#[test]
fn offsets_start_over_on_the_next_connection() {
    let mut reg = registry();
    let mut conn = connected(&mut reg);
    send(&mut reg, &mut conn, 1);
    let (_, b) = send(&mut reg, &mut conn, 2);
    write_to(&mut conn, b + 1);
    conn.close();
    let f = failures(&drain(&mut reg, &mut conn));
    assert_maybe(&f[&1]);
    assert_maybe(&f[&2]);
    assert!(reg.is_empty());

    let mut next = connected(&mut reg);
    let (_, a) = send(&mut reg, &mut next, 3);
    assert_eq!(a, ConnectPacket::SIZE as u64);
    next.io_error(ErrorKind::ConnectionReset, t(9));
    assert_not_sent(&failures(&drain(&mut reg, &mut next))[&3]);
}

/// Without `unwritten_not_sent` (a driver that made no promise about its reports, say one
/// that reports a TLS write failure with `io_error`), every request put on an established
/// connection is maybe delivered when it ends, written or not, as before as8.8; only requests
/// held by a handshake that never completed are not sent.
#[test]
fn without_the_opt_in_every_sent_request_is_maybe_delivered() {
    for how in [End::Io, End::Eof, End::Close, End::WriteFailed(0)] {
        let mut reg = registry();
        let mut conn = Connection::new(default_config(), t(0));
        write(&mut conn, ConnectPacket::SIZE as u64);
        conn.recv(&server_hello(), t(1));
        drain(&mut reg, &mut conn);
        let (_, a) = send(&mut reg, &mut conn, 1);
        send(&mut reg, &mut conn, 2);
        write_to(&mut conn, a + 1);
        end(&mut conn, how);
        let f = failures(&drain(&mut reg, &mut conn));
        assert_maybe(&f[&1]);
        assert_maybe(&f[&2]);
    }
    let mut reg = registry();
    let mut conn = Connection::new(default_config(), t(0));
    send(&mut reg, &mut conn, 1);
    conn.io_error(ErrorKind::ConnectionReset, t(1));
    assert_not_sent(&failures(&drain(&mut reg, &mut conn))[&1]);
}

/// An encoder that shortens the shared output destroys the frames before its own, which the
/// connection then never writes: the request before it is not sent if none of it had been
/// written, maybe delivered if some had.
#[test]
fn a_frame_destroyed_by_a_shortening_encoder_is_classified_by_what_was_written() {
    for written in [false, true] {
        let mut reg = registry();
        let mut conn = connected(&mut reg);
        send(&mut reg, &mut conn, 1);
        if written {
            // Half of it: the rest stays in the buffer the next encoder shortens (an encoder
            // that replaces an emptied buffer cannot be caught).
            let half = conn.pending_output().len() as u64 / 2;
            write(&mut conn, half);
        }
        let err = reg
            .send_with(&mut conn, EP, 2, |_, out| {
                *out = vec![1, 2, 3];
                Ok(())
            })
            .unwrap_err();
        assert_not_sent(&err);
        let f = failures(&drain(&mut reg, &mut conn));
        if written {
            assert_maybe(&f[&1]);
        } else {
            assert_not_sent(&f[&1]);
        }
    }
}

// ---- random write chunkings ----------------------------------------------------------------

#[derive(Clone, Debug)]
enum Op {
    Send,
    Hello,
    /// Writes this many bytes (modulo what is pending, plus one).
    Write(usize),
    /// A write of `offered` bytes fails, after the stream delivered `taken` of them (modulo).
    FailWrite {
        offered: usize,
        taken: usize,
    },
    /// A write of `offered` bytes whose outcome is unknown (it may have delivered `taken` of
    /// them); the connection carries on, and the bytes stay pending.
    Unknown {
        offered: usize,
        taken: usize,
    },
    Io,
    Eof,
    Close,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => Just(Op::Send),
        1 => Just(Op::Hello),
        6 => any::<usize>().prop_map(Op::Write),
        1 => (any::<usize>(), any::<usize>())
            .prop_map(|(offered, taken)| Op::FailWrite { offered, taken }),
        1 => (any::<usize>(), any::<usize>())
            .prop_map(|(offered, taken)| Op::Unknown { offered, taken }),
        1 => Just(Op::Io),
        1 => Just(Op::Eof),
        1 => Just(Op::Close),
    ]
}

/// The test's own model of the output stream, independent of the connection's offsets: the
/// bytes written, how far the stream was handed to the transport, how far it may actually
/// have reached the server, and (from parsing the bytes) where each request's frame starts.
#[derive(Default)]
struct Oracle {
    /// The bytes reported written, in order.
    written: Vec<u8>,
    /// How far any byte was handed to the transport (written, or offered to a write that
    /// failed or whose outcome is unknown).
    handed: u64,
    /// How far bytes may actually have reached the server: written, or delivered by a write
    /// that failed or whose outcome is unknown. At most `handed`.
    reached: u64,
    /// Where each request's frame starts, by reply token.
    starts: HashMap<Uid, u64>,
}

impl Oracle {
    fn hand(&mut self, offered: usize, taken: usize) {
        let base = self.written.len() as u64;
        self.handed = self.handed.max(base + offered as u64);
        self.reached = self.reached.max(base + taken as u64);
    }

    /// Looks at the whole stream as it is now (only meaningful once established, when no
    /// frame is held) and records the start of every request frame in it.
    fn scan(&mut self, conn: &Connection) {
        assert_eq!(self.written.len() as u64, conn.written());
        let mut stream = self.written.clone();
        stream.extend_from_slice(conn.pending_output());
        let header = frame_config().header_len();
        let mut at = ConnectPacket::SIZE;
        while at + header <= stream.len() {
            let len = u32::from_le_bytes(stream[at..at + 4].try_into().unwrap()) as usize;
            let end = at + header + len;
            if end > stream.len() {
                break;
            }
            let token_bytes = &stream[at + header..at + header + 16];
            let first = u64::from_le_bytes(token_bytes[..8].try_into().unwrap());
            let token = Uid::new(
                first,
                u64::from_le_bytes(token_bytes[8..].try_into().unwrap()),
            );
            if token == EP {
                let request: EchoRequest =
                    fdb_native_wire::decode(&stream[at + header + 16..end]).unwrap();
                let start = at as u64;
                let old = self.starts.insert(request.reply.token(), start);
                assert!(old.is_none() || old == Some(start), "a frame moved");
            } else {
                assert!(
                    token == well_known::PING_PACKET || token.first & TOKEN_STREAM_FLAG == 0,
                    "{token:?}"
                );
            }
            at = end;
        }
    }
}

struct Run {
    reg: Registry,
    conn: Connection,
    oracle: Oracle,
    now: u64,
    /// key → reply token of the requests sent.
    sent: HashMap<u32, Uid>,
    next_key: u32,
}

impl Run {
    fn step(&mut self, op: &Op) -> Vec<RpcEvent<u32>> {
        self.now += 1;
        let now = t(self.now);
        let pending = self.conn.pending_output().len();
        match *op {
            Op::Send => {
                let key = self.next_key;
                self.next_key += 1;
                let before = self.conn.output_offset();
                if let Ok(token) =
                    self.reg
                        .send_message(&mut self.conn, EP, key, |reply| EchoRequest {
                            reply: ReplyPromise::new(reply),
                            value: u64::from(key),
                        })
                {
                    assert_eq!(self.reg.frame_offset(token), Some(before));
                    self.sent.insert(key, token);
                }
            }
            Op::Hello => self.conn.recv(&server_hello(), now),
            Op::Write(n) => {
                let n = n % (pending + 1);
                self.oracle
                    .written
                    .extend_from_slice(&self.conn.pending_output()[..n]);
                self.oracle.hand(0, 0);
                self.conn.consume_output(n);
            }
            Op::FailWrite { offered, taken } => {
                let offered = offered % (pending + 1);
                let taken = taken % (offered + 1);
                self.oracle.hand(offered, taken);
                self.conn.write_failed(offered, ErrorKind::BrokenPipe, now);
            }
            Op::Unknown { offered, taken } => {
                let offered = offered % (pending + 1);
                let taken = taken % (offered + 1);
                self.oracle.hand(offered, taken);
                self.conn.output_maybe_written(offered);
            }
            Op::Io => self.conn.io_error(ErrorKind::ConnectionReset, now),
            Op::Eof => self.conn.recv_eof(now),
            Op::Close => self.conn.close(),
        }
        if self.conn.was_established() {
            self.oracle.scan(&self.conn);
        }
        drain(&mut self.reg, &mut self.conn)
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3000))]

    /// At the end of a connection, a request is `NotSent` if and only if no byte of its frame
    /// was handed to the transport (written, or offered to a write that failed or whose
    /// outcome is unknown), and never if any byte of it may have reached the server. Both
    /// marks come from the test's own model, not from the connection's offsets.
    #[test]
    fn not_sent_iff_no_byte_of_the_frame_was_handed_over(ops in proptest::collection::vec(op(), 0..60)) {
        let mut run = Run {
            reg: registry(),
            conn: Connection::new(conn_config(), t(0)),
            oracle: Oracle::default(),
            now: 0,
            sent: HashMap::new(),
            next_key: 0,
        };
        let mut events = Vec::new();
        for op in &ops {
            events.extend(run.step(op));
        }
        run.conn.close();
        events.extend(drain(&mut run.reg, &mut run.conn));
        prop_assert!(run.reg.is_empty());
        let written = run.oracle.written.len() as u64;
        let handed = run.oracle.handed.max(written);
        let reached = run.oracle.reached.max(written);
        prop_assert!(reached <= handed);
        prop_assert_eq!(run.conn.written(), written);
        prop_assert_eq!(run.conn.maybe_written(), handed);
        let mut completed = 0;
        for e in &events {
            let RpcEvent::Completion(c) = e else { continue };
            completed += 1;
            let err = c.result.as_ref().unwrap_err();
            prop_assert_eq!(run.sent.get(&c.key), Some(&c.reply_token));
            // Held frames of a handshake that never completed are not in the stream.
            let start = run.oracle.starts.get(&c.reply_token).copied();
            prop_assert!(start.is_some() || !run.conn.was_established(), "{:?}", c);
            let untouched = start.is_none_or(|s| s >= handed);
            let may_have_reached_server = start.is_some_and(|s| s < reached);
            match err {
                RequestError::NotSent(_) => {
                    prop_assert!(untouched, "{:?} at {:?}, handed {}", c, start, handed);
                    prop_assert!(!may_have_reached_server, "{:?} at {:?}, reached {}", c, start, reached);
                }
                RequestError::MaybeDelivered(_) => {
                    prop_assert!(!untouched, "{:?} at {:?}, handed {}", c, start, handed);
                }
                other => prop_assert!(false, "unexpected {:?}", other),
            }
        }
        prop_assert_eq!(completed, run.sent.len());
    }
}
