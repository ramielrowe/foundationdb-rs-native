//! One FlowTransport connection, as a sans-IO state machine: the handshake, frame dispatch,
//! pings and liveness of an outgoing client connection.
//!
//! [`Connection`] does no I/O, has no timers of its own and never reads a clock. A driver
//! (the peer manager) owns the socket and feeds it:
//!
//! * **received bytes**, read straight into the connection's buffer with
//!   [`recv_buf`](Connection::recv_buf) / [`recv_commit`](Connection::recv_commit) (or copied in
//!   with [`recv`](Connection::recv)), the end of the stream ([`recv_eof`](Connection::recv_eof))
//!   or a socket error ([`io_error`](Connection::io_error));
//! * **the time**, an [`Instant`] with every input and in
//!   [`handle_timeout`](Connection::handle_timeout), which it calls once
//!   [`poll_timeout`](Connection::poll_timeout)'s deadline has passed;
//! * **commands**: [`send`](Connection::send) a frame to a remote token,
//!   [`register`](Connection::register) / [`unregister`](Connection::unregister) a local token,
//!   [`set_referenced`](Connection::set_referenced), [`close`](Connection::close).
//!
//! and drains its outputs:
//!
//! * **bytes to write**, [`pending_output`](Connection::pending_output), acknowledged with
//!   [`consume_output`](Connection::consume_output) (and, for a write that failed after the
//!   transport may have taken part of it, [`write_failed`](Connection::write_failed));
//! * **events**, [`poll_event`](Connection::poll_event) until it returns `None` after every
//!   input: [`Event::Connected`] with the peer's ConnectPacket, [`Event::Frame`] for a
//!   registered token (the payload borrows the receive buffer), [`Event::UnknownToken`],
//!   [`Event::EndpointNotFound`], [`Event::UnauthorizedEndpoint`], and finally
//!   [`Event::Closed`] with a [`ConnectionError`];
//! * **the next deadline**, [`poll_timeout`](Connection::poll_timeout).
//!
//! A driver loop, in this order:
//!
//! ```text
//! conn = Connection::new(config, now)
//! loop:
//!     1. input     one of: readable  → n = socket.read(conn.recv_buf(16 * 1024));
//!                                      n == 0 ? conn.recv_eof(now) : conn.recv_commit(n, now)
//!                          deadline  → conn.handle_timeout(now)
//!                          a command → conn.send(..) / register(..) / ...
//!     2. drain     while let Some(event) = conn.poll_event(): handle it (Closed ends the loop)
//!     3. write     conn.pending_output() to the socket; conn.consume_output(written)
//!                  (a write error: conn.write_failed(offered, kind, now); see Written offsets)
//!     4. wait      for: socket readable | conn.poll_timeout() | a command
//! ```
//!
//! * **Drain after every input.** Frames are parsed lazily, in
//!   [`poll_event`](Connection::poll_event): that is also where pings are answered. Until the
//!   events are drained, [`recv_buf`](Connection::recv_buf) gives no room (an empty slice: do not
//!   read into it, it is not the end of the stream), so undrained input cannot pile up; in a
//!   build with debug assertions, feeding bytes to an undrained connection panics.
//!   [`has_undrained_input`](Connection::has_undrained_input) tells.
//! * **Feed bytes before handling a deadline.** If the socket is readable when a deadline
//!   passes, read first: the ping monitor counts bytes received, and the bytes in the socket
//!   would otherwise be missed.
//! * **Read with a fixed size.** Never size reads from anything the peer sent.
//! * **Output is bounded** by [`ConnectionConfig::max_pending_output`] (see
//!   [Sending](self#sending)).
//!
//! Citations `7.4:N` are lines of `fdbrpc/FlowTransport.actor.cpp` on `release-7.4` and `8.0:N`
//! of `fdbrpc/FlowTransport.cpp` on `release-8.0`, unless another file is named.
//!
//! # States
//!
//! ```text
//!               new(config, now): our ConnectPacket is queued for output
//!                                │
//!                                ▼
//!   ┌──────────────────── Handshaking ──────────────────────────────────┐
//!   │  frames sent now are held back       peer's 44 bytes arrive       │
//!   │                                      and ConnectPacket::check ok  │
//!   │                                                │                  │
//!   │ handshake_timeout / EOF / I/O error /          ▼                  │
//!   │ malformed or rejected packet /       Event::Connected; held frames│
//!   │ incompatible version / close()       are released; monitor starts │
//!   │                                                │                  │
//!   ▼                                                ▼                  │
//! Closed ◄── framing error / malformed ping ── Established              │
//!   ▲        PingTimeout / Idle / Unreferenced   │  ▲  monitor:         │
//!   │        EOF (after buffered frames) /       │  │  Waiting ──ping_at──► Pinging
//!   │        I/O error / close()                 │  │     ▲                  │
//!   │                                            └──┘     └─reply or reset───┘
//!   └── Event::Closed(error) is the last event; nothing more goes in or out.
//! ```
//!
//! # Version detection
//!
//! There is no negotiation. Each side sends its ConnectPacket first; a receiver accepts the
//! peer only if its version [is compatible](fdb_native_wire::ProtocolVersion::is_compatible)
//! (`0xFFFF_FFFF_FFFF_0000`) with its own (`connectionReader`, 7.4:1560-1561, 8.0:1692-1693).
//! What happens to a mismatch, in FDB (`FlowTransport.actor.cpp` on 7.4, `FlowTransport.cpp`
//! on 8.0):
//!
//! * The **server** sends its ConnectPacket only after reading ours: an incoming connection
//!   gets a peer from `onConnected`, and `Peer::onIncomingConnection` prepends the server's
//!   packet (7.4:1719 and 1197, 8.0:1853 and 1289). It waits `CONNECTION_MONITOR_TIMEOUT`
//!   (jittered) for ours (7.4:1721, 8.0:1847), so ours goes out first, immediately.
//! * If our version is incompatible, the server hangs up only on a peer without
//!   `hasInexpensiveMultiVersionClient()` (protocol version `0x0FDB00A551000000`, well below
//!   7.4 and 8.0; `throw incompatible_protocol_version()`, 7.4:1597-1607, 8.0:1729-1739).
//!   Otherwise it keeps the connection, marks it incompatible, still answers with its own
//!   ConnectPacket, and from then on parses our frames only because we
//!   `hasStableInterfaces()` (`0x0FDB00B070010000`; 7.4:1663, 8.0:1796) and delivers them
//!   only to receivers whose policy admits our version (`checkCompatible` in `deliver`; the
//!   default policy is `RequirePeer::Exactly`, `FlowTransport.h` 7.4:246-248, so in practice
//!   only pings and `ProtocolInfoRequest`). It sends an incompatible peer nothing but pings
//!   (`sendPacket`, 7.4:2294-2295, 8.0:2451-2452).
//! * **libfdb_c** learns a cluster's version from that ConnectPacket: the client's own
//!   `connectionReader` stores the server's version in `peer->protocolVersion` even for an
//!   incompatible peer, and `getClusterProtocolImpl` reads it from there
//!   (`getCoordinatorProtocolFromConnectPacket`, `fdbclient/NativeAPI.actor.cpp` 7.4:7981,
//!   `NativeAPI.cpp` 8.0:5785). It also sends a `ProtocolInfoRequest` to the coordinator's
//!   `WLTOKEN_PROTOCOL_INFO`, but only "to start a connection" (7.4:8047, 8.0:5837): to an
//!   incompatible client the server's reply would not be sent at all (the `sendPacket` rule
//!   above). The `MultiVersionApi` then switches to the library whose version matches.
//!
//! So this connection does what libfdb_c does to learn the version, and then what a
//! single-version client would do with it: it announces the version in
//! [`ConnectionConfig::local`], reads the server's ConnectPacket, and if the server's
//! version is valid but incompatible it ends with [`ConnectionError::Incompatible`] (FDB's
//! `incompatible_protocol_version`), whose
//! [`incompatible_version`](ConnectionError::incompatible_version) is the server's version.
//! The peer manager reconnects announcing that version if it has a schema set for it. No
//! frame is ever sent before the server's version is known to be compatible (see Sending),
//! so nothing encoded for the wrong version reaches the server. A `ProtocolInfoRequest` is
//! not needed: the ConnectPacket always carries the version, and the request's reply cannot
//! come back on an incompatible connection.
//!
//! # Connection ids
//!
//! The packet's `connectionId` should be one random id above 1 for the whole process
//! ([`ConnectPacket::multi_version_client`]), as the `MultiVersionApi` does
//! (`fdbclient/MultiVersionTransaction.actor.cpp` 7.4:3116-3120). A server remembers ids above
//! 1 that it has seen (`multiVersionConnections`, 7.4:1568-1592 and 1618-1621, 8.0:1721 and
//! 1750) and does not report an incompatible connection from a known id as a rejected
//! incompatible client (`ConnectionRejected`, `incompatiblePeers`): our version probes then
//! look like the multi-version client's own.
//!
//! # Dispatch
//!
//! A frame's token selects, in this order:
//!
//! * [`well_known::PING_PACKET`]: a [`PingRequest`], answered with a [`PingReply`] to its
//!   reply token on this connection (`PingReceiver`, 7.4:248-261, 8.0:351-364). No event.
//! * [`well_known::ENDPOINT_NOT_FOUND`] / [`well_known::UNAUTHORIZED_ENDPOINT`]: the payload
//!   is the [`Uid`] of the endpoint ([`Event::EndpointNotFound`],
//!   [`Event::UnauthorizedEndpoint`]; `EndpointNotFoundReceiver`, `UnauthorizedEndpointReceiver`,
//!   7.4:222-277, 8.0:325-380).
//! * the reply token of the ping this connection is waiting on: the monitor's reply.
//! * a token [registered](Connection::register): [`Event::Frame`].
//! * anything else is unknown ([`Event::UnknownToken`]). As in FDB, a packet for an unknown
//!   *stream* token ([`TOKEN_STREAM_FLAG`] set, not well-known) is answered with an
//!   endpoint-not-found packet carrying the token, and any other unknown packet (a late
//!   reply, typically) is dropped (`scanPackets` and `deliver`, 7.4:1446-1452 and 1288-1316,
//!   8.0:1566-1571 and 1361-1389). A client does have to answer: the server's failure
//!   monitor relies on it to learn that an endpoint is gone.
//!
//! The payload of a frame is not looked at, except for the three FlowTransport messages.
//! FDB checks each receiver's `PeerCompatibilityPolicy`; on a compatible connection every
//! policy admits the peer, so there is nothing to check here.
//!
//! # Ping and liveness
//!
//! The monitor follows `connectionMonitor` for an outgoing client connection, which is the
//! same on both releases (7.4:616-698, 8.0:725-819), with the knobs of [`MonitorConfig`]:
//!
//! 1. When the handshake completes, and after each ping is answered (or reset): if nothing
//!    is outstanding (no output waiting to be written and no registered token, which stands
//!    for FDB's `outstandingReplies` and `reliable`: `RequestStream::getReply` sends with
//!    `sendReliable` and waits in `sendCanceler`, whose `PeerHolder` counts the reply while
//!    it is awaited; `fdbrpc/fdbrpc.h` 7.4:767-772, 8.0:790-791,
//!    `fdbrpc/include/fdbrpc/genericactors.actor.h` 7.4:312-324 and 409), close the connection as
//!    [`Unreferenced`](ConnectionError::Unreferenced) if it is
//!    [unreferenced](Connection::set_referenced) and sent no data for
//!    `unreferenced_close_delay`, or as [`Idle`](ConnectionError::Idle) if it was opened and
//!    last sent data more than `idle_timeout` ago (7.4:646-656, 8.0:775-785). "Data" is any
//!    frame except a ping (`lastDataPacketSentTime`, 7.4:2412-2414, 8.0:2576-2578).
//! 2. Wait `loop_time`, jittered (`delayJittered`, 7.4:659, 8.0:788), then send a
//!    [`PingRequest`] with a fresh reply token.
//! 3. Wait for the reply. Every `timeout` (7.4:669, 8.0:797), if no byte at all arrived in
//!    that window the connection fails ([`PingTimeout`](ConnectionError::PingTimeout),
//!    `connection_failed`); otherwise wait another window. The reply, or a reset (dropping
//!    the last reference, `removePeerReference`, 7.4:2225-2228, 8.0:2364-2367), goes back to
//!    step 1.
//!
//! Deadlines are absolute: a window ends `timeout` after the previous one, not after the
//! moment it was handled (unless the driver is so late that the next one has passed too, in
//! which case it is re-anchored at the current time instead of failing for the driver's
//! delay). Feed received bytes before handling a deadline.
//!
//! Before the handshake the monitor does not run; [`ConnectionConfig::handshake_timeout`]
//! bounds the wait for the server's ConnectPacket instead.
//!
//! # Sending
//!
//! [`send`](Connection::send) frames a payload for a remote token. Frames sent before the
//! handshake completes are **held** and released, in order, right after the peer's
//! ConnectPacket is accepted; if the handshake fails they are discarded, never written. (FDB
//! writes them at once, behind its ConnectPacket. Holding them costs one round trip on a new
//! connection, since the server answers nothing before it has read our ConnectPacket anyway,
//! and in exchange a request is never put on a connection whose version turns out to be
//! wrong, and a request layer knows a request queued before [`Event::Connected`] was not sent
//! if the connection fails first.)
//!
//! **Backpressure.** FDB queues output without limit; here
//! [`ConnectionConfig::max_pending_output`] bounds it. When the output waiting to be written
//! (held frames included) is at or above the limit, [`send`](Connection::send) refuses with
//! [`SendError::Backpressure`] (a frame is accepted whatever its size below the limit), and
//! the advisory endpoint-not-found notices for unknown stream tokens are not sent (the event
//! then says `reported: false`). Ping replies are mandatory: if the peer keeps pinging while
//! not reading, so that the FlowTransport traffic it caused (ping replies and notices) waiting
//! to be written reaches the limit, the connection ends with
//! [`ConnectionError::OutputOverflow`]. Our own pings, one per loop at most, are always
//! queued.
//!
//! **File identifiers.** Send every message with its type's exact file identifier
//! ([`send_message`](Connection::send_message) does). FDB's `ObjectReader::deserialize`
//! compares it with the receiver's type and, apart from a 7.0-to-6.3 downgrade, logs
//! `MismatchedFileIdentifier` at `SevError` and `ASSERT`s (`flow/include/flow/ObjectSerializer.h`
//! 7.4:68-86, 8.0:68-86); in a server process a receiver's error ends in `flushAndExit`
//! (`deliver`, 7.4:1283-1285). This side checks the identifier of the messages it reads
//! itself (pings and endpoint-not-found notices) and ends the connection with
//! [`MalformedMessage`](ConnectionError::MalformedMessage) on a mismatch.
//!
//! The checksum follows the TLS-ness of the connection ([`ConnectionConfig::tls`]), as it does
//! for the server reading the same connection, *provided* replies come back on it: see the
//! [framing documentation](crate::framing). A [`ReplyPromise`]
//! serializes only its token, and FDB resolves the reply endpoint to the address of the
//! connection the request arrived on (`loadedEndpoint`, 7.4:2179-2181, 8.0:2318-2320), so
//! requests whose reply endpoints are `ReplyPromise`s satisfy this.
//!
//! # Written offsets
//!
//! The output is one byte stream over the connection's life: our ConnectPacket from offset 0,
//! then every frame in the order it is written. Frames sent before the handshake are held,
//! but their place is already known: they are released right after the ConnectPacket, before
//! anything else (nothing else is queued during the handshake). So
//! [`output_offset`](Connection::output_offset), read just before a [`send`](Connection::send),
//! is the offset of the frame's first byte, held or not.
//!
//! [`written`](Connection::written) is how far the driver has reported the stream written
//! ([`consume_output`](Connection::consume_output)), and
//! [`maybe_written`](Connection::maybe_written) how far the transport *may* have taken it:
//! `written`, or further after a write that failed or was abandoned with an unknown outcome
//! ([`write_failed`](Connection::write_failed),
//! [`output_maybe_written`](Connection::output_maybe_written)). A frame that starts at or past
//! `maybe_written` when the connection ends was never handed to the transport, not one byte
//! of it, and never will be: the [`ReplyRegistry`](crate::rpc::ReplyRegistry) fails the
//! request it carried as [`NotSent`](crate::rpc::RequestError::NotSent). Any other frame may
//! have been delivered, even if only its first byte was taken.
//!
//! This is only as exact as the driver's reports, which must cover every byte the transport
//! **may** have taken:
//!
//! * bytes a write accepted (`Ok(n)`) are written, whatever becomes of them: a TLS session's
//!   buffer, the socket's send buffer and the network all count as delivered;
//! * a write that returned an error may still have taken part of its buffer, unless the
//!   stream guarantees otherwise: a TLS stream (`futures-rustls`) returns the socket's error
//!   after encrypting part of the buffer into its session. Report it with
//!   [`write_failed`](Connection::write_failed) and the number of bytes offered. Only a
//!   stream that takes nothing when it fails, as `write(2)` and [`std::io::Write::write`]'s
//!   contract say, may be reported with plain [`io_error`](Connection::io_error);
//! * a write that returned `Pending` took nothing;
//! * a write whose outcome is unknown (a write future abandoned halfway, by a driver that
//!   writes from a copy) is [`output_maybe_written`](Connection::output_maybe_written);
//! * report before anything ends the connection: its end discards the output, and the
//!   offsets are final from then on.

mod config;
mod error;
mod messages;

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fdb_native_wire::{
    DecodeError, DecodeOwned, Encode, EncodeError, FileIdentified, ProtocolVersion, ReplyPromise,
    Uid, Writer, binary,
};

pub use config::{ConnectionConfig, DEFAULT_MAX_PENDING_OUTPUT, MonitorConfig};
pub use error::{ConnectionError, IoError, SendError};
pub use messages::{
    EndpointNotFound, PING_REPLY_OK, PingReply, PingRequest, TOKEN_STREAM_FLAG, well_known,
};

use crate::framing::{
    Checksum, Frame, FrameConfig, FrameDecoder, FrameError, FrameWriteError, LENGTH_LEN, TOKEN_LEN,
};
use crate::runtime::Instant;
use crate::{ConnectPacket, ConnectPacketError};

/// Where a [`Connection`] is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    /// Waiting for the peer's ConnectPacket.
    Handshaking,
    /// The handshake is complete; frames flow.
    Established,
    /// The connection has ended ([`Connection::error`] says why).
    Closed,
}

/// Something that happened on a [`Connection`], from [`Connection::poll_event`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event<'a> {
    /// The peer's ConnectPacket was accepted. Its protocol version (flags included) is the
    /// version to read the peer's messages with; [`Connection::peer_version`] gives it without
    /// flags.
    Connected {
        /// The peer's ConnectPacket.
        peer: ConnectPacket,
    },
    /// A frame for a [registered](Connection::register) token. The payload borrows the
    /// connection's receive buffer: decode it or [copy it](Frame::into_owned) before the next
    /// call on the connection.
    Frame(Frame<'a>),
    /// A frame for a token that is neither registered nor a FlowTransport token. It was
    /// dropped. `reported` says whether the peer was sent an endpoint-not-found packet for it
    /// (only for a stream token, see the [module documentation](self#dispatch)).
    UnknownToken {
        /// The token.
        token: Uid,
        /// Whether an endpoint-not-found packet was sent back.
        reported: bool,
    },
    /// The peer does not have the endpoint with this token (a request sent to it cannot be
    /// delivered; FDB's `IFailureMonitor::endpointNotFound`).
    EndpointNotFound {
        /// The token of the remote endpoint.
        token: Uid,
    },
    /// The peer refused to deliver to the endpoint with this token
    /// (`WLTOKEN_UNAUTHORIZED_ENDPOINT`).
    UnauthorizedEndpoint {
        /// The token of the remote endpoint.
        token: Uid,
    },
    /// The connection has ended. This is the last event.
    Closed(ConnectionError),
}

/// Events produced outside the frame stream, in order.
#[derive(Debug)]
enum Queued {
    Connected(ConnectPacket),
    Closed(ConnectionError),
}

impl Queued {
    fn into_event(self) -> Event<'static> {
        match self {
            Queued::Connected(peer) => Event::Connected { peer },
            Queued::Closed(e) => Event::Closed(e),
        }
    }
}

/// The connection monitor's state (see the module documentation).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Monitor {
    /// Not running: before the handshake, after the end, or after the end of the stream.
    Off,
    /// The next ping goes out at `ping_at`.
    Waiting { ping_at: Instant },
    /// A ping with reply token `token` is out.
    Pinging {
        token: Uid,
        sent_at: Instant,
        /// The end of the current window.
        deadline: Instant,
        /// `bytes_received` at the start of the window.
        starting_bytes: u64,
    },
}

/// What a frame's token selects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    Ping,
    EndpointNotFound,
    Unauthorized,
    PingReply,
    StalePingReply,
    Local,
    Unknown,
}

/// A small deterministic generator (SplitMix64) for the jitter and the ping reply tokens.
#[derive(Clone, Debug)]
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number in `[0, 1)` (`random01()`).
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// A sans-IO FlowTransport client connection. See the [module documentation](self).
#[derive(Debug)]
pub struct Connection {
    /// This connection's [`id`](Connection::id).
    id: u64,
    /// The receive buffer. Kept apart from the rest so that a frame borrowed from it can be
    /// returned while the rest of the state changes.
    decoder: FrameDecoder,
    /// Whether `decoder` has been replaced by an empty one after the end.
    decoder_released: bool,
    core: Core,
}

/// Everything but the receive buffer.
#[derive(Debug)]
struct Core {
    config: ConnectionConfig,
    frame_config: FrameConfig,
    state: ConnectionState,
    handshake_deadline: Instant,
    peer: Option<ConnectPacket>,
    error: Option<ConnectionError>,
    /// Output not yet written: `out[out_pos..]`.
    out: Vec<u8>,
    out_pos: usize,
    /// Frames sent before the handshake completed.
    held: Vec<u8>,
    /// Bytes of `out` written so far, over the connection's life: the offset in the output
    /// stream up to which bytes were reported written.
    written_total: u64,
    /// The offset in the output stream up to which bytes may have been taken by the
    /// transport without being reported written (a write that failed). See
    /// [`Connection::maybe_written`].
    maybe_taken: u64,
    /// FlowTransport frames this side queued in response to the peer (ping replies and
    /// endpoint-not-found notices) and not yet written: their end in the stream (counted like
    /// `written_total`) and length.
    internal: VecDeque<(u64, usize)>,
    /// The sum of the lengths in `internal`.
    internal_pending: usize,
    /// The length of the slice `recv_buf` last returned.
    recv_room: usize,
    receivers: HashSet<Uid>,
    events: VecDeque<Queued>,
    /// The peer closed its side; buffered frames are still delivered.
    eof: bool,
    rng: Rng,
    monitor: Monitor,
    /// The reply token of the last ping abandoned by a reset, whose late reply is ignored.
    stale_ping: Option<Uid>,
    last_ping_rtt: Option<Duration>,
    bytes_received: u64,
    /// The latest time given to the connection.
    now: Instant,
    /// `lastConnectTime`.
    opened_at: Instant,
    /// `lastDataPacketSentTime`.
    last_data_sent: Instant,
    referenced: bool,
    /// The handshake completed at some point (the state may since be `Closed`).
    was_established: bool,
    writer: Writer,
}

impl Connection {
    /// A connection on a socket that has just been connected (and, for TLS, has completed the
    /// TLS handshake) at `now`. Our ConnectPacket is the first [output](Self::pending_output).
    pub fn new(config: ConnectionConfig, now: Instant) -> Connection {
        let frame_config = FrameConfig::new(Checksum::for_tls(config.tls))
            .with_max_packet_len(config.max_packet_len);
        let mut core = Core {
            frame_config,
            state: ConnectionState::Handshaking,
            handshake_deadline: now + config.handshake_timeout,
            peer: None,
            error: None,
            out: Vec::new(),
            out_pos: 0,
            held: Vec::new(),
            written_total: 0,
            maybe_taken: 0,
            internal: VecDeque::new(),
            internal_pending: 0,
            recv_room: 0,
            receivers: HashSet::new(),
            events: VecDeque::new(),
            eof: false,
            rng: Rng(config.seed),
            monitor: Monitor::Off,
            stale_ping: None,
            last_ping_rtt: None,
            bytes_received: 0,
            now,
            opened_at: now,
            last_data_sent: now,
            referenced: true,
            was_established: false,
            writer: Writer::new(),
            config,
        };
        match core.config.local.encode() {
            Ok(bytes) => core.out = bytes,
            Err(e) => core.fail(ConnectionError::InternalEncode(e.to_string())),
        }
        Connection {
            id: NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            decoder: FrameDecoder::before_handshake(frame_config),
            decoder_released: false,
            core,
        }
    }

    // ---- state ---------------------------------------------------------------------------

    /// Where the connection is.
    pub fn state(&self) -> ConnectionState {
        self.core.state
    }

    /// An identifier of this connection, unique in the process (a counter): it tells one
    /// connection from the next, for instance to the
    /// [`ReplyRegistry`](crate::rpc::ReplyRegistry) bound to it.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Whether the handshake completed at some point, even if the connection has ended since.
    /// Frames [sent](Self::send) before the handshake were then released to the output (and
    /// may have been written); if it never completed, they were discarded unwritten.
    pub fn was_established(&self) -> bool {
        self.core.was_established
    }

    /// The configuration.
    pub fn config(&self) -> &ConnectionConfig {
        &self.core.config
    }

    /// The version this side announced, without flags.
    pub fn local_version(&self) -> ProtocolVersion {
        self.core.config.local.protocol_version.without_flags()
    }

    /// The peer's ConnectPacket, once read (also when it was refused).
    pub fn peer(&self) -> Option<&ConnectPacket> {
        self.core.peer.as_ref()
    }

    /// The peer's protocol version without flags, once its ConnectPacket was read.
    pub fn peer_version(&self) -> Option<ProtocolVersion> {
        self.core.peer.map(|p| p.protocol_version.without_flags())
    }

    /// Why the connection ended, once it has.
    pub fn error(&self) -> Option<&ConnectionError> {
        self.core.error.as_ref()
    }

    /// Bytes received so far (FDB's `bytesReceived`, which the monitor watches).
    pub fn bytes_received(&self) -> u64 {
        self.core.bytes_received
    }

    /// The round trip of the last ping answered.
    pub fn last_ping_rtt(&self) -> Option<Duration> {
        self.core.last_ping_rtt
    }

    // ---- input ---------------------------------------------------------------------------

    /// Room for at least `min` received bytes: read into the start of it, then call
    /// [`recv_commit`](Self::recv_commit) with the count the read returned. Use a fixed read
    /// size (say 16 KiB; it is clamped to [`MAX_SPARE`](crate::framing::MAX_SPARE)).
    ///
    /// The slice is **empty** once the connection has ended, after the end of the stream, and
    /// while input is undrained ([`has_undrained_input`](Self::has_undrained_input): drain
    /// [`poll_event`](Self::poll_event) first). Never read into an empty slice: a read of 0
    /// bytes into it is not the end of the stream.
    pub fn recv_buf(&mut self, min: usize) -> &mut [u8] {
        self.release_decoder();
        self.core.recv_room = 0;
        if self.core.state == ConnectionState::Closed || self.core.eof || self.has_undrained_input()
        {
            return &mut [];
        }
        let room = self.decoder.spare(min);
        self.core.recv_room = room.len();
        room
    }

    /// Records that `n` bytes were read into the start of [`recv_buf`](Self::recv_buf)'s slice
    /// at `now`. `n` must not exceed the slice's length (it is a bug, which panics in a build
    /// with debug assertions; otherwise only the slice's length is taken).
    pub fn recv_commit(&mut self, n: usize, now: Instant) {
        self.core.advance(now);
        let room = std::mem::take(&mut self.core.recv_room);
        debug_assert!(
            n <= room,
            "recv_commit({n}) is more than the {room} bytes of room given"
        );
        let n = n.min(room);
        if self.core.state == ConnectionState::Closed || self.core.eof || n == 0 {
            return;
        }
        self.decoder.commit(n);
        self.core.bytes_received = self.core.bytes_received.saturating_add(n as u64);
        self.try_handshake();
    }

    /// Copies received bytes in (the same as [`recv_buf`](Self::recv_buf) and
    /// [`recv_commit`](Self::recv_commit), except that bytes given while input is undrained are
    /// still taken, so nothing bounds the buffer then; that is a bug in the driver, which
    /// panics in a build with debug assertions).
    pub fn recv(&mut self, bytes: &[u8], now: Instant) {
        self.core.advance(now);
        if self.core.state == ConnectionState::Closed || self.core.eof || bytes.is_empty() {
            return;
        }
        debug_assert!(
            !self.has_undrained_input(),
            "bytes fed to a connection whose events were not drained"
        );
        self.decoder.push(bytes);
        self.core.bytes_received = self.core.bytes_received.saturating_add(bytes.len() as u64);
        self.try_handshake();
    }

    /// The peer closed its side of the connection (a read returned 0) at `now`. Frames
    /// already received are still delivered, then [`Event::Closed`] with
    /// [`PeerClosed`](ConnectionError::PeerClosed).
    pub fn recv_eof(&mut self, now: Instant) {
        self.core.advance(now);
        match self.core.state {
            ConnectionState::Closed => {}
            ConnectionState::Handshaking => self.core.fail(ConnectionError::PeerClosed {
                during_handshake: true,
            }),
            ConnectionState::Established => {
                self.core.eof = true;
                self.core.monitor = Monitor::Off;
            }
        }
    }

    /// Whether [`poll_event`](Self::poll_event) has something to give: a queued event, or a
    /// whole frame received and not parsed yet. Drain it before feeding more bytes.
    pub fn has_undrained_input(&self) -> bool {
        !self.core.events.is_empty()
            || (self.core.state == ConnectionState::Established
                && (self.decoder.error().is_some() || front_frame_complete(&self.decoder)))
    }

    /// The socket failed at `now`: the connection ends with [`ConnectionError::Io`]. Pass
    /// the [`io::Error`](std::io::Error) itself to keep its cause (a TLS alert, say) in the
    /// error; an [`ErrorKind`](std::io::ErrorKind) alone keeps only the kind.
    ///
    /// For a failed **write** this says that the write took nothing: use it only for a stream
    /// that guarantees so, and [`write_failed`](Self::write_failed) otherwise (see
    /// [Written offsets](self#written-offsets)). A failed read takes no output.
    pub fn io_error(&mut self, error: impl Into<IoError>, now: Instant) {
        self.core.advance(now);
        self.core.fail(ConnectionError::Io(error.into()));
    }

    /// A write of the first `offered` bytes of [`pending_output`](Self::pending_output) failed
    /// with `error` at `now`, and the stream may have taken any part of them first (a TLS
    /// stream can): they count as possibly written
    /// ([`output_maybe_written`](Self::output_maybe_written)), then the connection ends with
    /// [`ConnectionError::Io`], as for [`io_error`](Self::io_error). A frame that starts past
    /// the `offered` bytes was not written.
    pub fn write_failed(&mut self, offered: usize, error: impl Into<IoError>, now: Instant) {
        self.output_maybe_written(offered);
        self.io_error(error, now);
    }

    /// When [`handle_timeout`](Self::handle_timeout) must next be called, if at all. It may be
    /// in the past.
    pub fn poll_timeout(&self) -> Option<Instant> {
        let core = &self.core;
        match core.state {
            ConnectionState::Closed => None,
            ConnectionState::Handshaking => Some(core.handshake_deadline),
            ConnectionState::Established => match core.monitor {
                Monitor::Off => None,
                Monitor::Waiting { ping_at } => Some(ping_at),
                Monitor::Pinging { deadline, .. } => Some(deadline),
            },
        }
    }

    /// Runs whatever is due at `now`: the handshake timeout, sending a ping, a ping window.
    pub fn handle_timeout(&mut self, now: Instant) {
        self.core.advance(now);
        self.core.handle_timeout();
    }

    // ---- commands ------------------------------------------------------------------------

    /// Sends `payload` (a serialized message) to the remote endpoint `token`. Before the
    /// handshake completes the frame is held (see the [module documentation](self#sending)).
    ///
    /// Fails with [`SendError::Backpressure`] when the output waiting to be written has
    /// reached [`ConnectionConfig::max_pending_output`].
    pub fn send(&mut self, token: Uid, payload: &[u8]) -> Result<(), SendError> {
        self.core.check_sendable()?;
        let core = &mut self.core;
        let config = core.frame_config;
        let buf = core.send_buffer();
        config
            .encode_frame(token, payload, buf)
            .map_err(SendError::Frame)?;
        core.note_sent(token);
        Ok(())
    }

    /// Sends a frame to `token` whose payload `write` appends to the vector it is given (for
    /// a serializer that writes in place, such as `Writer::finish_into`). On an error nothing
    /// is sent.
    ///
    /// `write` must only append: the vector is the connection's output, shared with the
    /// frames before it. One that shortens it below where it began (`*out = ...`) destroyed
    /// them; the connection detects that, **fails** with
    /// [`ConnectionError::InternalEncode`] (nothing waiting is written) and returns
    /// [`SendError::Closed`]. One that replaces the contents by something at least as long
    /// cannot be detected: give a writer that is not trusted a vector of its own and
    /// [`send`](Connection::send) what it wrote (the [peer manager](crate::peer) does).
    pub fn send_with<F, E>(&mut self, token: Uid, write: F) -> Result<(), SendError<E>>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<(), E>,
    {
        if let Err(e) = self.core.check_sendable() {
            return Err(match e {
                SendError::Closed => SendError::Closed,
                SendError::Backpressure => SendError::Backpressure,
                SendError::Frame(f) => SendError::Frame(f),
                SendError::Payload(never) => match never {},
            });
        }
        let core = &mut self.core;
        let config = core.frame_config;
        let buf = core.send_buffer();
        let result = config.try_encode_frame_with(token, buf, write);
        core.sent(token, result)
    }

    /// Serializes `message` as a flatbuffers message with its file identifier and sends it to
    /// `token`. The identifier must be exactly the one the receiver's C++ type has (see
    /// [File identifiers](self#sending)).
    pub fn send_message<T>(&mut self, token: Uid, message: &T) -> Result<(), SendError<EncodeError>>
    where
        T: Encode + FileIdentified + ?Sized,
    {
        if let Err(e) = self.core.check_sendable() {
            return Err(match e {
                SendError::Closed => SendError::Closed,
                SendError::Backpressure => SendError::Backpressure,
                SendError::Frame(f) => SendError::Frame(f),
                SendError::Payload(never) => match never {},
            });
        }
        let core = &mut self.core;
        let config = core.frame_config;
        let established = core.state == ConnectionState::Established;
        let Core {
            out, held, writer, ..
        } = &mut *core;
        let buf = if established { out } else { held };
        let result = config.try_encode_frame_with(token, buf, |buf| {
            encode_root(writer, buf, message, T::FILE_IDENTIFIER.value())
        });
        core.sent(token, result)
    }

    /// Registers a local receiver: frames for `token` become [`Event::Frame`]s. Returns
    /// `false`, and registers nothing, if the token is already registered, is well-known, or
    /// is a reply token of this connection's pings.
    ///
    /// A registered token counts as an outstanding reply for the idle rules (see the
    /// [module documentation](self#ping-and-liveness)): unregister it when the reply has come.
    pub fn register(&mut self, token: Uid) -> bool {
        let core = &mut self.core;
        if well_known::is_well_known(token)
            || Some(token) == core.stale_ping
            || matches!(core.monitor, Monitor::Pinging { token: t, .. } if t == token)
        {
            return false;
        }
        core.receivers.insert(token)
    }

    /// Unregisters a local receiver. Returns whether it was registered.
    pub fn unregister(&mut self, token: Uid) -> bool {
        self.core.receivers.remove(&token)
    }

    /// Whether `token` is registered.
    pub fn is_registered(&self, token: Uid) -> bool {
        self.core.receivers.contains(&token)
    }

    /// Whether anything still uses the connection (FDB's `peerReferences`; a connection starts
    /// referenced). An unreferenced connection with nothing outstanding is closed as
    /// [`Unreferenced`](ConnectionError::Unreferenced) once it has sent no data for
    /// `unreferenced_close_delay`. Dropping the reference while a ping is out resets the ping
    /// so that the rule is checked at once, as `removePeerReference` does (while the monitor
    /// waits to send the next ping, the rule is checked after that ping's reply).
    pub fn set_referenced(&mut self, referenced: bool, now: Instant) {
        let core = &mut self.core;
        core.advance(now);
        core.referenced = referenced;
        if !referenced
            && core.state == ConnectionState::Established
            && matches!(core.monitor, Monitor::Pinging { .. })
            && core.nothing_pending()
            && core.now.saturating_duration_since(core.last_data_sent)
                > core.config.monitor.unreferenced_close_delay
        {
            core.monitor_check();
        }
    }

    /// Ends the connection ([`ConnectionError::LocalClose`]). Output not yet written is
    /// discarded.
    pub fn close(&mut self) {
        self.core.fail(ConnectionError::LocalClose);
    }

    // ---- output --------------------------------------------------------------------------

    /// Bytes to write to the socket, oldest first.
    pub fn pending_output(&self) -> &[u8] {
        self.core.out.get(self.core.out_pos..).unwrap_or(&[])
    }

    /// Records that the first `n` bytes of [`pending_output`](Self::pending_output) were
    /// written: handed to the transport, which may deliver them. Report every byte a write
    /// took before doing anything else to the connection (see
    /// [Written offsets](self#written-offsets)).
    pub fn consume_output(&mut self, n: usize) {
        let core = &mut self.core;
        let n = n.min(core.out.len() - core.out_pos);
        core.out_pos += n;
        core.written_total += n as u64;
        while let Some(&(end, len)) = core.internal.front() {
            if end > core.written_total {
                break;
            }
            core.internal.pop_front();
            core.internal_pending -= len;
        }
        if core.out_pos == core.out.len() {
            core.out.clear();
            core.out_pos = 0;
            if core.out.capacity() > SHRINK_ABOVE {
                core.out = Vec::new();
            }
        } else if core.out_pos >= 64 * 1024 && core.out_pos * 2 >= core.out.len() {
            core.out.drain(..core.out_pos);
            core.out_pos = 0;
        }
    }

    /// Records that the transport may have taken the first `n` bytes of
    /// [`pending_output`](Self::pending_output) although no write said so: a write that failed
    /// on a stream that can take part of its buffer before failing (TLS), or a write abandoned
    /// with an unknown outcome. The bytes stay pending (a driver that carries on writes them
    /// again, and reports them with [`consume_output`](Self::consume_output) then), but
    /// [`maybe_written`](Self::maybe_written) covers them, so the requests in them count as
    /// possibly delivered. Call it before the connection ends: its end discards the output
    /// (`n` is clamped to what is pending).
    pub fn output_maybe_written(&mut self, n: usize) {
        let core = &mut self.core;
        let n = n.min(core.out.len() - core.out_pos);
        let end = core.written_total + n as u64;
        core.maybe_taken = core.maybe_taken.max(end);
    }

    /// The offset in the output stream up to which bytes were reported written
    /// ([`consume_output`](Self::consume_output)) over the connection's life, our ConnectPacket
    /// included. See [Written offsets](self#written-offsets).
    pub fn written(&self) -> u64 {
        self.core.written_total
    }

    /// The offset in the output stream up to which bytes may have been handed to the
    /// transport: [`written`](Self::written), or further after a write that failed or was
    /// abandoned ([`write_failed`](Self::write_failed),
    /// [`output_maybe_written`](Self::output_maybe_written)). No byte at or past it was handed
    /// to the transport; once the connection has ended, none ever will be. See
    /// [Written offsets](self#written-offsets).
    pub fn maybe_written(&self) -> u64 {
        self.core.written_total.max(self.core.maybe_taken)
    }

    /// The offset in the output stream at which the next frame [sent](Self::send) will start.
    /// A frame sent during the handshake is held, but its offset is already final: held
    /// frames are released right after our ConnectPacket, before anything else. Once the
    /// connection has ended, a frame that starts at or past
    /// [`maybe_written`](Self::maybe_written) was never handed to the transport. See
    /// [Written offsets](self#written-offsets).
    pub fn output_offset(&self) -> u64 {
        self.core.output_end()
    }

    /// The next event, if any. Call it until it returns `None` after every input: frames are
    /// parsed here, and pings answered.
    pub fn poll_event(&mut self) -> Option<Event<'_>> {
        loop {
            if let Some(q) = self.core.events.pop_front() {
                return Some(q.into_event());
            }
            if self.core.state != ConnectionState::Established {
                self.release_decoder();
                return None;
            }
            if let Some(e) = self.decoder.error() {
                self.core.fail(ConnectionError::Framing(e));
                continue;
            }
            let Some(token) = front_token(&self.decoder) else {
                // Not even the token of a frame is in: no frame is complete.
                if self.core.eof {
                    self.core.fail(ConnectionError::PeerClosed {
                        during_handshake: false,
                    });
                    continue;
                }
                return None;
            };
            if self.core.route(token) == Route::Local {
                // The frame is returned borrowed, so this path must not loop again.
                return match self.decoder.next_frame() {
                    Ok(Some(frame)) if frame.payload.is_empty() => {
                        let token = frame.token;
                        self.core.fail(empty_payload(token));
                        self.core.events.pop_front().map(Queued::into_event)
                    }
                    Ok(Some(frame)) => Some(Event::Frame(frame)),
                    Ok(None) => self.core.eof_or_none(),
                    Err(e) => {
                        self.core.fail(ConnectionError::Framing(e));
                        self.core.events.pop_front().map(Queued::into_event)
                    }
                };
            }
            let event = match self.decoder.next_frame() {
                Ok(Some(frame)) if frame.payload.is_empty() => {
                    self.core.fail(empty_payload(frame.token));
                    continue;
                }
                Ok(Some(frame)) => self.core.internal_frame(frame.token, frame.payload),
                Ok(None) => {
                    if self.core.eof {
                        self.core.fail(ConnectionError::PeerClosed {
                            during_handshake: false,
                        });
                        continue;
                    }
                    return None;
                }
                Err(e) => {
                    self.core.fail(ConnectionError::Framing(e));
                    continue;
                }
            };
            if let Some(event) = event {
                return Some(event);
            }
        }
    }

    // ---- internals -----------------------------------------------------------------------

    /// Reads the peer's ConnectPacket once it is all in.
    fn try_handshake(&mut self) {
        if self.core.state != ConnectionState::Handshaking {
            return;
        }
        match ConnectPacket::decode(self.decoder.peek()) {
            Ok((packet, len)) => {
                self.core.peer = Some(packet);
                let local = self.core.config.local.protocol_version;
                match packet.check(local, self.core.config.dialed_port) {
                    Ok(()) => match self.decoder.consume(len) {
                        Ok(()) => self.core.established(packet),
                        // Not reachable: the decoder is in its handshake phase and holds
                        // the `len` bytes just decoded.
                        Err(e) => self
                            .core
                            .fail(ConnectionError::InternalEncode(format!("consume: {e}"))),
                    },
                    Err(ConnectPacketError::Incompatible { local, peer }) => self
                        .core
                        .fail(ConnectionError::Incompatible { local, peer }),
                    Err(e) => self.core.fail(ConnectionError::HandshakeRejected(e)),
                }
            }
            Err(binary::DecodeError::UnexpectedEof { .. }) => {}
            Err(e) => self.core.fail(ConnectionError::MalformedConnectPacket(e)),
        }
    }

    /// Frees the receive buffer once the connection has ended.
    fn release_decoder(&mut self) {
        if self.core.state == ConnectionState::Closed && !self.decoder_released {
            self.decoder = FrameDecoder::new(self.core.frame_config);
            self.decoder_released = true;
        }
    }
}

/// The token of the frame at the front of the decoder's buffer, once its bytes are in (the
/// frame itself may not be complete).
fn front_token(decoder: &FrameDecoder) -> Option<Uid> {
    let header = decoder.config().header_len();
    let bytes = decoder.peek().get(header..header + TOKEN_LEN)?;
    let (first, second) = bytes.split_at(8);
    Some(Uid::new(
        u64::from_le_bytes(first.try_into().ok()?),
        u64::from_le_bytes(second.try_into().ok()?),
    ))
}

/// Whether a whole frame is buffered at the front of the decoder.
fn front_frame_complete(decoder: &FrameDecoder) -> bool {
    let pending = decoder.peek();
    let Some(len) = pending.first_chunk::<LENGTH_LEN>() else {
        return false;
    };
    let len = u32::from_le_bytes(*len) as usize;
    decoder
        .config()
        .header_len()
        .checked_add(len)
        .is_some_and(|total| pending.len() >= total)
}

/// A frame with nothing after the token: FDB `ASSERT(!reader.empty())`s.
fn empty_payload(token: Uid) -> ConnectionError {
    ConnectionError::MalformedMessage {
        token,
        error: DecodeError::TooShort { len: 0 },
    }
}

/// The next [`Connection::id`].
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Capacity above which an emptied output buffer is released.
const SHRINK_ABOVE: usize = 256 * 1024;

/// Appends `value` as the only member of a flatbuffers message to `out`.
fn encode_root<T: Encode + ?Sized>(
    w: &mut Writer,
    out: &mut Vec<u8>,
    value: &T,
    file_identifier: u32,
) -> Result<(), EncodeError> {
    let mut root = w.table();
    root.field(value)?;
    let root = root.finish()?;
    w.finish_into(out, root, file_identifier)
}

/// Decodes a FlowTransport message, checking its file identifier as FDB's
/// `ObjectReader::deserialize` does (see [File identifiers](self#sending)).
fn decode_message<T: DecodeOwned + FileIdentified>(bytes: &[u8]) -> Result<T, DecodeError> {
    fdb_native_wire::decode(bytes)
}

impl Core {
    fn advance(&mut self, now: Instant) {
        self.now = self.now.max(now);
    }

    /// Ends the connection, unless it has ended already.
    fn fail(&mut self, error: ConnectionError) {
        if self.error.is_some() {
            return;
        }
        self.error = Some(error.clone());
        self.state = ConnectionState::Closed;
        self.monitor = Monitor::Off;
        self.out = Vec::new();
        self.out_pos = 0;
        self.held = Vec::new();
        self.internal.clear();
        self.internal_pending = 0;
        self.events.push_back(Queued::Closed(error));
    }

    /// The offset in the output stream after everything queued, held frames included.
    fn output_end(&self) -> u64 {
        self.written_total + (self.out.len() - self.out_pos) as u64 + self.held.len() as u64
    }

    /// The handshake succeeded.
    fn established(&mut self, packet: ConnectPacket) {
        self.state = ConnectionState::Established;
        self.was_established = true;
        self.events.push_back(Queued::Connected(packet));
        // The held frames go right after what is in `out`, which is only (what is left of) our
        // ConnectPacket, so the offsets `output_offset` gave them during the handshake hold.
        let held = std::mem::take(&mut self.held);
        self.out.extend_from_slice(&held);
        // connectionMonitor: "set this to not immediately close the connection as idle".
        self.last_data_sent = self.now;
        self.monitor_check();
    }

    fn eof_or_none(&mut self) -> Option<Event<'static>> {
        if self.eof {
            self.fail(ConnectionError::PeerClosed {
                during_handshake: false,
            });
            return self.events.pop_front().map(Queued::into_event);
        }
        None
    }

    fn check_sendable(&self) -> Result<(), SendError> {
        if self.state == ConnectionState::Closed || self.eof {
            Err(SendError::Closed)
        } else if self.pending_len() >= self.config.max_pending_output {
            Err(SendError::Backpressure)
        } else {
            Ok(())
        }
    }

    /// Output waiting to be written, held frames included.
    fn pending_len(&self) -> usize {
        (self.out.len() - self.out_pos) + self.held.len()
    }

    fn send_buffer(&mut self) -> &mut Vec<u8> {
        if self.state == ConnectionState::Established {
            &mut self.out
        } else {
            &mut self.held
        }
    }

    /// The outcome of building a frame for the application: counts it as sent, or maps the
    /// refusal. A payload writer that shortened the output buffer below its frame destroyed
    /// the frames before it, which cannot be told apart from garbage: the connection fails
    /// ([`ConnectionError::InternalEncode`]; nothing of what was waiting is written) and the
    /// caller sees [`SendError::Closed`].
    fn sent<E>(
        &mut self,
        token: Uid,
        result: Result<(), FrameWriteError<E>>,
    ) -> Result<(), SendError<E>> {
        match result {
            Ok(()) => {
                self.note_sent(token);
                Ok(())
            }
            Err(FrameWriteError::Frame(e @ FrameError::OutputShrunk { .. })) => {
                self.fail(ConnectionError::InternalEncode(e.to_string()));
                Err(SendError::Closed)
            }
            Err(FrameWriteError::Frame(f)) => Err(SendError::Frame(f)),
            Err(FrameWriteError::Payload(p)) => Err(SendError::Payload(p)),
        }
    }

    /// `lastDataPacketSentTime`: every frame but a ping counts.
    fn note_sent(&mut self, token: Uid) {
        if token != well_known::PING_PACKET {
            self.last_data_sent = self.now;
        }
    }

    /// Sends one of FlowTransport's own messages (only once established). `reply` marks a
    /// frame sent because the peer asked for it (a ping reply or an endpoint-not-found notice),
    /// which counts towards [`ConnectionError::OutputOverflow`].
    fn send_internal<T: Encode + ?Sized>(
        &mut self,
        token: Uid,
        value: &T,
        file_identifier: u32,
        reply: bool,
    ) {
        // Only once established: during the handshake nothing but our ConnectPacket may be in
        // `out`, or the offsets of held frames would move (see Written offsets).
        debug_assert_eq!(self.state, ConnectionState::Established);
        let config = self.frame_config;
        let before = self.out.len();
        let Core { out, writer, .. } = self;
        let result = config.try_encode_frame_with(token, out, |buf| {
            encode_root(writer, buf, value, file_identifier)
        });
        match result {
            Ok(()) => {
                if reply {
                    let len = self.out.len() - before;
                    let end = self.written_total + (self.out.len() - self.out_pos) as u64;
                    self.internal.push_back((end, len));
                    self.internal_pending += len;
                }
                self.note_sent(token)
            }
            Err(FrameWriteError::Payload(e)) => {
                self.fail(ConnectionError::InternalEncode(e.to_string()))
            }
            Err(FrameWriteError::Frame(e)) => self.fail(ConnectionError::Framing(e)),
        }
    }

    /// Nothing waits to be written and no reply is awaited (`reliable.empty() &&
    /// unsent.empty() && outstandingReplies == 0`).
    fn nothing_pending(&self) -> bool {
        self.out_pos == self.out.len() && self.held.is_empty() && self.receivers.is_empty()
    }

    fn jittered(&mut self, d: Duration) -> Duration {
        let m = self.config.monitor;
        if m.jitter_range == 0.0 && m.jitter_offset == 1.0 {
            return d;
        }
        let factor = m.jitter_offset + m.jitter_range * self.rng.unit();
        Duration::try_from_secs_f64(d.as_secs_f64() * factor).unwrap_or(d)
    }

    /// A fresh reply token: not a stream token (like a `ReplyPromise`'s), and not in use.
    fn mint_token(&mut self) -> Uid {
        loop {
            let token = Uid::new(
                self.rng.next_u64() & !TOKEN_STREAM_FLAG,
                self.rng.next_u64(),
            );
            if !self.receivers.contains(&token) && Some(token) != self.stale_ping {
                return token;
            }
        }
    }

    /// The top of `connectionMonitor`'s loop: the idle rules, then schedule the next ping.
    fn monitor_check(&mut self) {
        let now = self.now;
        if let Monitor::Pinging { token, .. } = self.monitor {
            self.stale_ping = Some(token);
        }
        let m = self.config.monitor;
        if self.nothing_pending() {
            let since_data = now.saturating_duration_since(self.last_data_sent);
            if !self.referenced && since_data > m.unreferenced_close_delay {
                self.fail(ConnectionError::Unreferenced);
                return;
            }
            if now.saturating_duration_since(self.opened_at) > m.idle_timeout
                && since_data > m.idle_timeout
            {
                self.fail(ConnectionError::Idle);
                return;
            }
        }
        let wait = self.jittered(m.loop_time);
        self.monitor = Monitor::Waiting {
            ping_at: now + wait,
        };
    }

    fn handle_timeout(&mut self) {
        let now = self.now;
        match self.state {
            ConnectionState::Closed => {}
            ConnectionState::Handshaking => {
                if now >= self.handshake_deadline {
                    self.fail(ConnectionError::HandshakeTimeout(
                        self.config.handshake_timeout,
                    ));
                }
            }
            ConnectionState::Established => match self.monitor {
                Monitor::Off => {}
                Monitor::Waiting { ping_at } => {
                    if now >= ping_at {
                        self.send_ping();
                    }
                }
                Monitor::Pinging {
                    token,
                    sent_at,
                    deadline,
                    starting_bytes,
                } => {
                    if now < deadline {
                        return;
                    }
                    let timeout = self.config.monitor.timeout;
                    if self.bytes_received == starting_bytes {
                        self.fail(ConnectionError::PingTimeout(timeout));
                        return;
                    }
                    let mut next = deadline + timeout;
                    if next <= now {
                        next = now + timeout;
                    }
                    self.monitor = Monitor::Pinging {
                        token,
                        sent_at,
                        deadline: next,
                        starting_bytes: self.bytes_received,
                    };
                }
            },
        }
    }

    fn send_ping(&mut self) {
        let token = self.mint_token();
        let request = PingRequest {
            reply: ReplyPromise::new(token),
        };
        self.send_internal(
            well_known::PING_PACKET,
            &request,
            PingRequest::FILE_IDENTIFIER.value(),
            false,
        );
        if self.state == ConnectionState::Closed {
            return;
        }
        self.monitor = Monitor::Pinging {
            token,
            sent_at: self.now,
            deadline: self.now + self.config.monitor.timeout,
            starting_bytes: self.bytes_received,
        };
    }

    fn route(&self, token: Uid) -> Route {
        if token == well_known::PING_PACKET {
            Route::Ping
        } else if token == well_known::ENDPOINT_NOT_FOUND {
            Route::EndpointNotFound
        } else if token == well_known::UNAUTHORIZED_ENDPOINT {
            Route::Unauthorized
        } else if matches!(self.monitor, Monitor::Pinging { token: t, .. } if t == token) {
            Route::PingReply
        } else if Some(token) == self.stale_ping {
            Route::StalePingReply
        } else if self.receivers.contains(&token) {
            Route::Local
        } else {
            Route::Unknown
        }
    }

    /// Handles a frame that is not for a registered token.
    fn internal_frame(&mut self, token: Uid, payload: &[u8]) -> Option<Event<'static>> {
        match self.route(token) {
            Route::Ping => {
                match decode_message::<PingRequest>(payload) {
                    Ok(_) if self.internal_pending >= self.config.max_pending_output => {
                        self.fail(ConnectionError::OutputOverflow(self.internal_pending))
                    }
                    Ok(request) => self.send_internal(
                        request.reply.token(),
                        &PING_REPLY_OK,
                        <PingReply as FileIdentified>::FILE_IDENTIFIER.value(),
                        true,
                    ),
                    Err(error) => self.fail(ConnectionError::MalformedMessage { token, error }),
                }
                self.events.pop_front().map(Queued::into_event)
            }
            Route::EndpointNotFound | Route::Unauthorized => {
                match decode_message::<EndpointNotFound>(payload) {
                    Ok(remote) if token == well_known::ENDPOINT_NOT_FOUND => {
                        Some(Event::EndpointNotFound { token: remote })
                    }
                    Ok(remote) => Some(Event::UnauthorizedEndpoint { token: remote }),
                    Err(error) => {
                        self.fail(ConnectionError::MalformedMessage { token, error });
                        self.events.pop_front().map(Queued::into_event)
                    }
                }
            }
            Route::PingReply => {
                // Any message on the reply token is the reply; FDB's pings are never answered
                // with an error.
                if let Monitor::Pinging { sent_at, .. } = self.monitor {
                    self.last_ping_rtt = Some(self.now.saturating_duration_since(sent_at));
                }
                self.monitor_check();
                self.events.pop_front().map(Queued::into_event)
            }
            Route::StalePingReply => None,
            // A token registered between the routing and here cannot happen (routing and
            // handling see the same state); treat it as unknown.
            Route::Local | Route::Unknown => {
                // The notice is advisory: it is not sent while output is backed up.
                let reported = token.first & TOKEN_STREAM_FLAG != 0
                    && !well_known::is_well_known(token)
                    && self.pending_len() < self.config.max_pending_output;
                if reported {
                    self.send_internal(
                        well_known::ENDPOINT_NOT_FOUND,
                        &token,
                        <Uid as FileIdentified>::FILE_IDENTIFIER.value(),
                        true,
                    );
                    if let Some(q) = self.events.pop_front() {
                        return Some(q.into_event());
                    }
                }
                Some(Event::UnknownToken { token, reported })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rng_unit_is_in_range() {
        let mut rng = Rng(7);
        for _ in 0..10_000 {
            let u = rng.unit();
            assert!((0.0..1.0).contains(&u));
        }
    }

    #[test]
    fn jitter_stays_within_fdb_bounds() {
        let config = ConnectionConfig::new(ConnectPacket::client(ProtocolVersion::V7_4), false);
        let mut conn = Connection::new(config, Instant::ZERO);
        let d = Duration::from_secs(1);
        for _ in 0..1000 {
            let j = conn.core.jittered(d);
            assert!(
                j >= Duration::from_millis(900) && j <= Duration::from_millis(1100),
                "{j:?}"
            );
        }
        // Nonsense knobs fall back to the plain delay rather than panicking.
        conn.core.config.monitor.jitter_offset = -5.0;
        assert_eq!(conn.core.jittered(d), d);
        conn.core.config.monitor.jitter_offset = f64::NAN;
        assert_eq!(conn.core.jittered(d), d);
    }

    #[test]
    fn minted_tokens_are_reply_tokens() {
        let config = ConnectionConfig::new(ConnectPacket::client(ProtocolVersion::V7_4), false);
        let mut conn = Connection::new(config, Instant::ZERO);
        for _ in 0..100 {
            let t = conn.core.mint_token();
            assert_eq!(t.first & TOKEN_STREAM_FLAG, 0);
            assert!(!well_known::is_well_known(t));
        }
    }
}
