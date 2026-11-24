//! The peer manager: one shared connection per remote address, kept alive, reconnected with
//! backoff, judged healthy or failed, and the request/reply mechanism on top of it. It drives
//! the sans-IO [`Connection`](crate::Connection) cores, and a [`ReplyRegistry`] per peer, over
//! a [`Runtime`].
//!
//! [`PeerManager`] is the entry point. [`PeerManager::peer`] returns the [`PeerHandle`] of an
//! address (creating its task on first use); every endpoint at that address shares it.
//!
//! * [`PeerHandle::request`] sends a request to an endpoint of the peer and resolves with the
//!   reply (see [Requests](#requests)). This is the way to talk to a server.
//! * [`PeerHandle::send_with`] / [`register`](PeerHandle::register) are the frame-level API,
//!   for one-way messages and for streams the server pushes to a token you registered.
//! * Everything that is not a reply (connection life, unsolicited frames, endpoint failures)
//!   goes to the manager's [`PeerEventSink`], for observers.
//!
//! ```text
//!            PeerHandle (clone per user)                        PeerEventSink
//!   request / send_with / register / connect / reference         (observers)
//!      │  ctrl: unbounded   queue: bounded (own count + wakers)        ▲
//!      ▼                                                               │ PeerEvent
//!  ┌────────────────────────── peer task (one per address) ────────────┴──────────┐
//!  │  idle ─ demand ─▶ backoff ─▶ dial (TCP+TLS, 2 s) ─▶ run Connection ─▶ ended  │
//!  │                     ▲   one pass: write, read, timer, ctrl, commands ◀─▶     │
//!  │                     │   ReplyRegistry (kept across connections)              │
//!  │                     └──── failure: backoff.ended, failure monitor, probe ◀───┘ │
//!  └──────────────────────────────────────────────────────────────────────────────┘
//!      │ status (Watch<PeerStatus>)            │ FailureMonitor (shared by all peers)
//!      ▼                                        ▼
//!   PeerHandle::status / connect / wait_for     address and endpoint failure state
//! ```
//!
//! # Tasks and channels
//!
//! Each address has one task (spawned with [`Runtime::spawn`]) that owns the socket, the
//! [`Connection`](crate::Connection) and the peer's [`ReplyRegistry`]. It follows the driver
//! loop of the [`connection`](crate::connection) module with one hand-written `poll_fn` and no
//! executor-specific combinator. Every pass over its sources is, in this order:
//!
//! 1. **write** until the socket would block, then flush;
//! 2. **read** until the socket would block (or the connection stops taking input), feeding the
//!    connection and draining its events after each read;
//! 3. the connection's **timer**, after the bytes: received bytes are fed before a deadline is
//!    handled;
//! 4. a batch of **control** messages (shutdown, cancellations, close, ...);
//! 5. a batch of **commands**, as many as the output has room for.
//!
//! The task returns to the executor only when a pass makes no progress (every source has then
//! been polled and has registered its waker) or after a bounded number of passes (it then wakes
//! itself and yields). So no source starves another: a stream of sends cannot keep replies
//! from being read, and a flood of replies cannot keep sends from being written.
//!
//! Handles talk to the task through two queues, neither tied to an executor:
//!
//! * **control** (unbounded): connect now, a reference added or released, unregister a token,
//!   cancel a request, close the connection, shut down. Never blocks, so it is safe in
//!   destructors.
//! * **commands** (bounded by [`PeerManagerConfig::command_queue_len`], counted by the queue
//!   itself and not by the number of senders): requests, one-way sends and registrations.
//!   A caller that gives up (drops its future) takes its command out of the queue, freeing the
//!   place, so abandoned calls neither accumulate nor cause connection attempts.
//!
//! Shared state is plain: the [`FailureMonitor`] and each peer's [`PeerStatus`] sit behind
//! locks with one waker slot per waiter, returned when the wait is dropped.
//!
//! # Connecting
//!
//! A peer connects **lazily**: when something asks (a [`request`](PeerHandle::request), a
//! [`send_with`](PeerHandle::send_with), a [`register`](PeerHandle::register),
//! [`connect`](PeerHandle::connect)), or when a failed, referenced peer is probed. Dialing is
//! one attempt bounded by [`connect_timeout`](PeerManagerConfig::connect_timeout)
//! (`CONNECTION_MONITOR_TIMEOUT`, 2 s: `FlowTransport.actor.cpp` 7.4:867, `FlowTransport.cpp`
//! 8.0:943) covering the TCP connect **and** the [`StreamWrapper`]'s TLS handshake, then the
//! FlowTransport handshake bounded by [`handshake_timeout`](PeerManagerConfig::handshake_timeout).
//! The [`Connection`](crate::Connection)'s `dialed_port` is the address's port and its `tls` flag
//! the address's.
//!
//! Commands that arrive while there is no established connection wait in the queue (up to
//! [`command_queue_len`](PeerManagerConfig::command_queue_len); their callers wait beyond that)
//! and are applied, in order, once the handshake has completed: applying them earlier would put
//! them on a connection whose version may turn out to be wrong, and the version is part of
//! what a request encodes. If the attempt fails, **everything still queued is refused** (not
//! just a batch), so a dead peer costs one attempt, not one per `command_queue_len`.
//!
//! * A refused command **was not sent**, and no connection was involved.
//! * A request that was applied is `Sent`: it completes with the reply, or with
//!   [`RequestError::MaybeDelivered`] if the connection ends first.
//!
//! The connect demand ([`connect`](PeerHandle::connect), a command) lasts until a connection
//! is established or an attempt fails: a connection closed locally during its handshake is
//! retried, not forgotten.
//!
//! # Requests
//!
//! [`PeerHandle::request`] is `tryGetReply`: at most once, no resend (see the
//! [`rpc`](crate::rpc) module for the full semantics and C++ citations). It takes the remote
//! endpoint's token and a closure that writes the request message, given the reply token
//! the registry minted and **the negotiated protocol version**, so a request is always encoded
//! for the schema set of the connection it goes out on, including after a version switch:
//!
//! ```no_run
//! # use fdb_native_transport::connection::well_known;
//! # use fdb_native_transport::peer::{PeerHandle, ProtocolInfoReply, ProtocolInfoRequest};
//! # use fdb_native_wire::ReplyPromise;
//! # async fn demo(peer: PeerHandle) -> Result<(), Box<dyn std::error::Error>> {
//! let reply = peer
//!     .request(well_known::PROTOCOL_INFO, |reply, _version, out| {
//!         // Append the message: `out` is yours alone, but an encoder should not assume more.
//!         let request = ProtocolInfoRequest { reply: ReplyPromise::new(reply) };
//!         out.extend_from_slice(&fdb_native_wire::encode(&request)?);
//!         Ok(())
//!     })
//!     .await?;
//! // The reply carries the version it arrived on, which schema decoders need.
//! let _version = reply.version;
//! let info: ProtocolInfoReply = reply.decode()?;
//! # let _ = info;
//! # Ok(())
//! # }
//! ```
//!
//! The future resolves with a [`Reply`]: the payload and the [`ProtocolVersion`] of the
//! connection it arrived on (the one the request was encoded for), because decoding a reply
//! can depend on it (`ClientDBInfo` differs between 7.4 and 8.0). [`Reply::decode`] (and
//! [`request_as`](PeerHandle::request_as), which does both) decodes it with [`decode_reply`]
//! for versions that need no schema; otherwise hand `reply.version` and `reply.payload` to the
//! schema's decoder. A failure is a [`RequestError`] whose
//! [`maybe_delivered`](RequestError::maybe_delivered) /
//! [`certainly_not_delivered`](RequestError::certainly_not_delivered) /
//! [`should_retry`](RequestError::should_retry) say what a caller may do. A connect failure
//! before the request was applied is [`RequestError::NotSent`]; the cause is in
//! [`PeerStatus::last_error`] and the [`PeerEvent::ConnectFailed`] event. So is a request
//! whose connection ended before the socket took one byte of it (see
//! [Written offsets](crate::connection#written-offsets); a TLS session's buffer counts as
//! taken, and so does whatever was offered to a failed write unless the runtime's sockets
//! promise [`WRITE_ERRORS_TAKE_NOTHING`](crate::runtime::Runtime::WRITE_ERRORS_TAKE_NOTHING)).
//!
//! **Encoders.** The closure runs on the peer's task, with a vector **of its own** (the
//! manager copies what it wrote into the frame afterwards), so whatever it does to the vector
//! (even `*out = ...`) cannot touch other frames. It should append the message and must not
//! block. If it fails, the request fails with [`RequestError::Encode`]; if it **panics**, the
//! panic is caught and the request fails the same way (nothing was sent, certainly not
//! delivered), and the peer carries on.
//!
//! **Fail fast.** If the address is already marked failed in the [`FailureMonitor`], `request`
//! returns [`RequestError::NotSent`] at once, without queueing or dialing (`tryGetReply` returns
//! at once when `onDisconnectOrFailure` is ready). Nothing then reconnects an *unreferenced*
//! peer by itself: [`connect`](PeerHandle::connect) probes explicitly, and a
//! [`reference`](PeerHandle::reference) makes the peer probe by itself.
//!
//! **Cancellation.** Dropping the future cancels the request: if it was still queued it is
//! never sent; if it was sent, the registry forgets its reply token (a late reply is then an
//! [`UnknownToken`](PeerEvent::UnknownToken) event) and nothing is sent to the server. As in
//! C++ this says nothing about whether the server got it.
//!
//! **Backpressure.** When the connection's output is full
//! ([`max_pending_output`](PeerManagerConfig::max_pending_output)) the task stops taking
//! commands until the socket has taken output; the callers wait in the queue. It is never
//! surfaced as an error (a peer that stops reading while still answering pings makes callers
//! wait: apply your own timeout, and drop the future to give up).
//!
//! **Failure monitor.** The registry reports each endpoint that failed (not found, unauthorized,
//! or a `broken_promise` reply) and the task records it in the [`FailureMonitor`]; before a
//! request is applied the monitor is consulted, so it is the single source of truth (see
//! [`FailureMonitor`]). A request to a failed endpoint is refused without being sent.
//!
//! # Version detection
//!
//! A cluster runs one protocol version and a client must announce exactly it, but it learns
//! the version from the server's ConnectPacket (see the [`connection`](crate::connection)
//! module). The manager is configured with the versions it has schemas for
//! ([`PeerManagerConfig::supported_versions`], most preferred first) and:
//!
//! 1. announces the version last seen at this address, else the one last seen at any
//!    address, else the first supported;
//! 2. on [`ConnectionError::Incompatible`](crate::ConnectionError::Incompatible) whose
//!    [`incompatible_version`](crate::ConnectionError::incompatible_version) is supported, remembers
//!    it for the address and for all addresses and **reconnects at once** announcing it
//!    (no backoff, not a failure; commands still waiting stay queued for the new connection);
//! 3. if it is not supported, fails with [`PeerError::UnsupportedVersion`], which names both
//!    versions.
//!
//! A cluster that is upgraded under a running client is handled by the same rule: the next
//! connection announces the old version, is told the new one, and switches. The negotiated
//! version is [`PeerStatus::version`] and what [`connect`](PeerHandle::connect) returns, and it
//! is what request encoders are given.
//!
//! # Reconnecting
//!
//! `connectionKeeper` (`fdbrpc/FlowTransport.actor.cpp` 7.4:760-1100, `FlowTransport.cpp`
//! 8.0:886-1190) reconnects like this, with knobs from `flow/Knobs.cpp` that are the same on
//! both releases (line numbers 7.4 / 8.0):
//!
//! | what | knob | value | 7.4 / 8.0 |
//! | --- | --- | --- | --- |
//! | first delay | `INITIAL_RECONNECTION_TIME` | 0.05 s | 119 / 122 |
//! | cap | `MAX_RECONNECTION_TIME` | 0.5 s | 120 / 123 |
//! | growth | `RECONNECTION_TIME_GROWTH_RATE` | 1.2 | 121 / 124 |
//! | reset | `RECONNECTION_RESET_TIME` | 5 s | 122 / 125 |
//! | connect bound | `CONNECTION_MONITOR_TIMEOUT` | 2 s | 109 / 112 |
//! | failure threshold | `FAILURE_DETECTION_DELAY` | 4 s | 324 / 323 |
//! | probe, failed | `SERVER_REQUEST_INTERVAL` | 1 s | 62 / 67 |
//! | jitter | `DELAY_JITTER_OFFSET`, `_RANGE` | 0.9, 0.2 | 58-59 / 63-64 |
//!
//! * Before each connect the keeper waits `delayJittered(max(0, lastConnectTime +
//!   reconnectionDelay - now))` (7.4:814-818, 8.0:926-930): the delay counts from the *start*
//!   of the previous attempt (`lastConnectTime = now()` as the attempt begins), so an attempt
//!   that itself took longer than the delay is followed by an immediate one. Here that is a
//!   deadline computed once per attempt, never a chain of sleeps.
//! * When a connection or attempt ends, `reconnectionDelay` becomes the initial value if
//!   the attempt started more than 5 s ago, else `min(max, delay * 1.2)` (7.4:974-979,
//!   8.0:1067-1072). The growth happens *before* the next wait, so after quick failures the
//!   waits are 60 ms, 72 ms, 86 ms, ... up to 500 ms. A clean close ([`PeerError::is_clean`]:
//!   idle, unreferenced or closed locally) resets the delay and counts as no failure.
//! * With nothing to send, a failed *referenced* peer is probed again: after
//!   `FAILURE_DETECTION_DELAY` if still marked available, after `SERVER_REQUEST_INTERVAL` if
//!   marked failed (7.4:783-812, 8.0:909-916; 7.4's `CLIENT_SKIP_AVAILABLE_PEER_PROBE` and
//!   `CLIENT_FAILED_PEER_PROBE_JITTER` knobs default off/0 and are not modelled).
//!   An unreferenced peer is not probed.
//!
//! # Failure state
//!
//! Each address is *available* or *failed* in the shared [`FailureMonitor`]. An address
//! becomes available when a connection completes its handshake (FDB marks it once the socket
//! is connected, `setStatus(false)` at 7.4:844 and 8.0:955; the handshake is the stricter
//! test) and failed once its connections have failed continuously for longer than
//! `FAILURE_DETECTION_DELAY` (7.4:995-1001, 8.0:1088-1094). FDB does this only for
//! `connection_failed`; every unclean end counts here, including an unsupported or
//! incompatible version, since the address cannot be used either way. Clean closes never
//! count. Every end of a connection or failed attempt is one `notifyDisconnect`
//! (7.4:1073, 8.0:1165), which wakes the waiters of that address only.
//!
//! # References and removal
//!
//! C++ counts references to a peer (`peerReferences`, which starts at -1;
//! `addPeerReference` / `removePeerReference`, 7.4:2190-2230). [`PeerHandle::reference`]
//! returns a [`PeerRef`] that does the same: **peers start unreferenced**, and
//! each guard adds one until it is dropped.
//!
//! * A peer that was **never** referenced keeps its connection until it ends (FDB's `-1`);
//!   one whose references were all released has its idle connection closed (cleanly) once
//!   nothing is outstanding
//!   ([`unreferenced_close_delay`](crate::MonitorConfig::unreferenced_close_delay)).
//! * Only a referenced peer is probed after a failure.
//! * <a id="removal"></a>**Removal.** When a connection or attempt ends and the peer is
//!   unreferenced with nothing outstanding (no queued commands), and no [`PeerHandle`] to it
//!   exists any more, the peer is removed from the manager and its task ends
//!   (`connectionKeeper`'s `PeerDestroy`, 7.4:1082-1097). A vanished server is therefore
//!   dialed until its callers stop and then forgotten, not dialed forever. The next
//!   [`PeerManager::peer`] for the address creates a new peer with a new [`PeerId`], and
//!   [`PeerEvent::Removed`] tells observers.
//!
//! # `choosePrimaryAddress`
//!
//! See [`choose_primary_address`]: an endpoint with a secondary address of the other TLS-ness
//! is connected to at the address whose TLS-ness matches this process's. A `libfdb_c` client
//! never sets a local address, so it always prefers the plain one; so does
//! [`PeerManager::peer_for_endpoint`] unless [`PeerManagerConfig::prefer_tls`] is set.
//!
//! # TLS
//!
//! See [`StreamWrapper`]: a hook called for TLS addresses with the connected socket.
//!
//! # Panics
//!
//! Code that runs on a peer's task is the manager's own, plus three hooks of the application: a
//! request or frame **encoder** (the closures of [`request`](PeerHandle::request) and
//! [`send_with`](PeerHandle::send_with)), the [`PeerEventSink`] and the [`StreamWrapper`].
//!
//! * A panicking **encoder** is caught: that one request fails with an encode error (nothing
//!   was written, so it was certainly not delivered) and the peer carries on.
//! * A panicking **sink** or **wrapper** is not caught, and **must not happen**: the connection
//!   is not driven while they run, and their state is the application's. If one does panic
//!   (the task is lost, with the executor's usual report of a panicked task), the peer is
//!   *abandoned* cleanly: its queue is closed, commands still queued are refused as not sent,
//!   requests already on the connection fail as maybe delivered
//!   ([`RequestError::MaybeDelivered`]), its phase becomes [`PeerPhase::Shutdown`], waiters on
//!   the failure monitor are woken, and it is removed from the manager, so the next
//!   [`PeerManager::peer`] for the address creates a new, working peer. Handles to the dead
//!   peer fail their commands (a handle keeps the old state).
//!
//! # Shutdown
//!
//! [`PeerManager::shutdown`] asks every peer to finish: queued commands are refused (never
//! sent), requests that were sent complete with [`RequestError::Cancelled`], what is already
//! queued for writing gets [`shutdown_timeout`](PeerManagerConfig::shutdown_timeout) to be
//! written, the socket is closed, [`PeerEvent::Closed`] (clean) is reported, the address's
//! waiters on the [`FailureMonitor`] are woken (`notifyDisconnect`) if it had a connection, and
//! the task ends.
//! The call returns when all tasks have. Dropping the last [`PeerManager`] clone shuts every
//! peer down the same way (without waiting): a manager that is gone leaves no task behind.
//!
//! Citations `7.4:N` are lines of `fdbrpc/FlowTransport.actor.cpp` on `release-7.4` and `8.0:N`
//! of `fdbrpc/FlowTransport.cpp` on `release-8.0`, unless another file is named.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicIsize, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use fdb_native_wire::{
    DecodeOwned, EncodeError, Endpoint, FileIdentified, NetworkAddress, NetworkAddressList,
    ProtocolVersion, Uid,
};
use futures_channel::{mpsc, oneshot};

use crate::rpc::{RequestError, decode_reply};
use crate::runtime::{Instant, Runtime};

mod address;
mod backoff;
mod config;
mod error;
mod event;
mod failure;
mod messages;
mod queue;
mod stream;
mod task;
mod waiters;
mod watch;

pub use address::{choose_primary_address, choose_primary_endpoint};
pub use backoff::BackoffConfig;
pub use config::PeerManagerConfig;
pub use error::{PeerError, SendError};
pub use event::{
    DiscardEvents, EventReceiver, EventSender, Generation, PeerEvent, PeerEventRecord,
    PeerEventSink, PeerId, event_channel,
};
pub use failure::{AddressStatus, EndpointFailure, EndpointStatus, FailureMonitor};
pub use messages::{ProtocolInfoReply, ProtocolInfoRequest};
pub use stream::{MaybeTls, NoTls, StreamWrapper};

use address::PeerKey;
use queue::{Cmd, CommandQueue, Rejected};
use task::PeerTask;
use watch::Watch;

#[cfg(doc)]
use crate::rpc::ReplyRegistry;

/// Where a peer is in its connection life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeerPhase {
    /// No connection and nothing asking for one (it was never used, was closed as idle or
    /// unreferenced, or failed and waits to be probed).
    Idle,
    /// Something wants a connection; the next attempt starts at `until` (the reconnect
    /// backoff).
    Backoff {
        /// The earliest the next attempt starts.
        until: Instant,
    },
    /// Connecting: TCP, TLS and the FlowTransport handshake.
    Connecting,
    /// The handshake completed: frames flow.
    Connected,
    /// The peer was shut down or removed.
    Shutdown,
}

/// A snapshot of a peer. See [`PeerHandle::status`] and [`PeerHandle::wait_for`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PeerStatus {
    /// Where the peer is.
    pub phase: PeerPhase,
    /// The negotiated protocol version, without flags, while [`Connected`](PeerPhase::Connected).
    pub version: Option<ProtocolVersion>,
    /// The version last negotiated with this address (the one the next connection
    /// announces), also while disconnected.
    pub known_version: Option<ProtocolVersion>,
    /// The latest connection: the one that is established if the phase is
    /// [`Connected`](PeerPhase::Connected). [`Generation(0)`](Generation) before the first.
    pub generation: Generation,
    /// How many connection attempts or connections have failed so far.
    pub failures: u64,
    /// Why the last one failed.
    pub last_error: Option<PeerError>,
    /// When the current connection completed its handshake.
    pub connected_since: Option<Instant>,
    /// How many [`PeerRef`]s are alive.
    pub references: usize,
}

/// Commands that never wait for room.
#[derive(Debug)]
pub(crate) enum Ctrl {
    /// Something wants a connection.
    Connect,
    /// A [`PeerRef`] was added or dropped.
    RefsChanged,
    Unregister(Uid),
    /// The request with this id was dropped after the task took it.
    Cancel(u64),
    CloseConnection,
    /// The last [`PeerHandle`] was dropped.
    Released,
    Shutdown,
}

/// What a peer's handles and its task share.
pub(crate) struct PeerShared {
    pub(crate) id: PeerId,
    pub(crate) address: NetworkAddress,
    pub(crate) status: Watch<PeerStatus>,
    pub(crate) queue: CommandQueue,
    pub(crate) ctrl: mpsc::UnboundedSender<Ctrl>,
    /// C++'s `peerReferences`: -1 until the first reference, then the count.
    references: AtomicIsize,
    /// How many [`PeerHandle`]s exist (the manager's map holds none).
    handles: AtomicUsize,
    next_id: AtomicU64,
}

impl fmt::Debug for PeerShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerShared")
            .field("id", &self.id)
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl PeerShared {
    /// The raw reference count: -1 if never referenced.
    pub(crate) fn references(&self) -> isize {
        self.references.load(Ordering::SeqCst)
    }

    pub(crate) fn handles(&self) -> usize {
        self.handles.load(Ordering::SeqCst)
    }

    fn add_reference(&self) {
        let _ = self
            .references
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |r| {
                Some(if r < 0 { 1 } else { r + 1 })
            });
        self.publish_references();
    }

    fn remove_reference(&self) {
        let _ = self
            .references
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |r| {
                (r > 0).then_some(r - 1)
            });
        self.publish_references();
    }

    /// Publishes the count to the status. It is read **under the status lock**, so of two
    /// concurrent changes whichever publishes last publishes the final count; a value read
    /// before taking the lock could be stale by then and be written over a newer one.
    fn publish_references(&self) {
        self.status.modify(|s| {
            let count = usize::try_from(self.references.load(Ordering::SeqCst)).unwrap_or(0);
            let changed = s.references != count;
            s.references = count;
            (changed, ())
        });
        let _ = self.ctrl.unbounded_send(Ctrl::RefsChanged);
    }
}

/// The reply to a [`request`](PeerHandle::request): the payload and the protocol version of
/// the connection it arrived on.
///
/// The version is the one the request was encoded for (a connection speaks one version, and
/// the reply comes back on the connection the request went out on). A reply can only be
/// decoded knowing it: the layout of `ClientDBInfo`, for one, differs between 7.4 and 8.0.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Reply {
    /// The protocol version, without flags, of the connection the reply arrived on: the
    /// schema set to decode it with.
    pub version: ProtocolVersion,
    /// The reply message, an `ErrorOr<EnsureTable<Reply>>` that [`decode_reply`] (or the
    /// schema's decoder for [`version`](Self::version)) decodes.
    pub payload: Vec<u8>,
}

impl Reply {
    /// The payload decoded as `T` with [`decode_reply`]: a server error becomes
    /// [`RequestError::Fdb`] / [`RequestError::BrokenPromise`], and a payload that is not a
    /// `T` [`RequestError::Decode`] (the request was delivered). For a reply whose layout
    /// depends on the version, use [`version`](Self::version) with the schema's decoder
    /// instead.
    pub fn decode<T>(&self) -> Result<T, RequestError>
    where
        T: DecodeOwned + FileIdentified,
    {
        decode_reply::<T>(&self.payload)
    }
}

/// What a command's caller gets back from the queue.
enum Called<T> {
    /// The task answered.
    Answer(T),
    /// The queue was closed: the command was never queued.
    NeverQueued,
    /// The attempt the command waited for failed before the command found room.
    Refused(PeerError),
    /// The task ended without answering.
    Lost,
}

/// Takes a queued command back if its caller drops the future before the answer.
struct Guard<'a> {
    shared: &'a PeerShared,
    id: u64,
    queued: bool,
    done: bool,
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        if self.queued && !self.done && self.shared.queue.remove(self.id).is_none() {
            // The task took it: it forgets a request it has sent (and ignores the rest).
            let _ = self.shared.ctrl.unbounded_send(Ctrl::Cancel(self.id));
        }
    }
}

/// A cloneable handle to one peer (an address): the way to talk to it and watch it. Handles
/// to the same address share one connection. It holds no executor type and is `Send + Sync`.
///
/// A handle keeps its peer (the task and its state) alive, but not its connection: use
/// [`reference`](PeerHandle::reference) to keep the connection and to have a failed peer
/// probed. A peer is [removed](self#references-and-removal) once no handle to it exists and it
/// is idle and unreferenced.
pub struct PeerHandle {
    shared: Arc<PeerShared>,
    failures: FailureMonitor,
}

impl fmt::Debug for PeerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerHandle")
            .field("id", &self.shared.id)
            .field("address", &self.shared.address)
            .finish_non_exhaustive()
    }
}

impl Clone for PeerHandle {
    fn clone(&self) -> PeerHandle {
        PeerHandle::new(self.shared.clone(), self.failures.clone())
    }
}

impl Drop for PeerHandle {
    fn drop(&mut self) {
        if self.shared.handles.fetch_sub(1, Ordering::SeqCst) == 1 {
            // Let the task see whether the peer can be removed.
            let _ = self.shared.ctrl.unbounded_send(Ctrl::Released);
        }
    }
}

impl PeerHandle {
    fn new(shared: Arc<PeerShared>, failures: FailureMonitor) -> PeerHandle {
        shared.handles.fetch_add(1, Ordering::SeqCst);
        PeerHandle { shared, failures }
    }

    /// The stable id of the peer, as events carry it.
    pub fn id(&self) -> PeerId {
        self.shared.id
    }

    /// The address of the peer.
    pub fn address(&self) -> &NetworkAddress {
        &self.shared.address
    }

    /// The failure monitor shared by all peers of the manager.
    pub fn failure_monitor(&self) -> &FailureMonitor {
        &self.failures
    }

    /// A snapshot of the peer.
    pub fn status(&self) -> PeerStatus {
        self.shared.status.read(PeerStatus::clone)
    }

    /// The negotiated protocol version, while connected.
    pub fn version(&self) -> Option<ProtocolVersion> {
        self.shared.status.read(|s| s.version)
    }

    /// Whether the address is marked failed (shorthand for the failure monitor's state).
    pub fn is_failed(&self) -> bool {
        self.failures.is_failed(&self.shared.address)
    }

    /// Resolves with `f`'s first `Some` result, evaluated on the current status and again
    /// after every change to it. For example, "wait until connected":
    /// `peer.wait_for(|s| (s.phase == PeerPhase::Connected).then_some(s.version)).await`.
    ///
    /// `f` runs **under the peer's status lock**, on every poll: keep it short and pure. It
    /// must not call any method of this handle (they take the same lock).
    pub async fn wait_for<T>(&self, mut f: impl FnMut(&PeerStatus) -> Option<T> + Unpin) -> T {
        self.shared.status.wait_for(|s| f(s)).await
    }

    /// Makes sure the peer is connected and returns the negotiated protocol version, which
    /// is the schema set to encode requests with. Connects if there is no connection
    /// (waiting out the reconnect backoff), including the reconnect that version detection
    /// may need; resolves at once if it is connected.
    ///
    /// Fails with the error of the first attempt that fails after the call (a refused
    /// connection, a timeout, an unsupported version, ...), or [`PeerError::Shutdown`]. A
    /// connection that is closed locally while it handshakes does not end the wait: the
    /// connect is retried.
    pub async fn connect(&self) -> Result<ProtocolVersion, PeerError> {
        let failures_before = self.shared.status.read(|s| s.failures);
        if self.shared.ctrl.unbounded_send(Ctrl::Connect).is_err() {
            return Err(PeerError::Shutdown);
        }
        self.shared
            .status
            .wait_for(|s| {
                if s.phase == PeerPhase::Connected {
                    return s.version.map(Ok);
                }
                if s.phase == PeerPhase::Shutdown {
                    return Some(Err(PeerError::Shutdown));
                }
                if s.failures != failures_before {
                    return Some(Err(s.last_error.clone().unwrap_or(PeerError::Shutdown)));
                }
                None
            })
            .await
    }

    /// Queues a command and waits for the task's answer. Dropping the future takes the
    /// command back if the task has not.
    async fn call<T>(&self, make: impl FnOnce(u64, oneshot::Sender<T>) -> Cmd) -> Called<T> {
        let (tx, rx) = oneshot::channel();
        let id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
        let mut guard = Guard {
            shared: &self.shared,
            id,
            queued: false,
            done: false,
        };
        match self.shared.queue.push(make(id, tx)).await {
            Ok(()) => {}
            Err(Rejected::Closed) => return Called::NeverQueued,
            Err(Rejected::Refused(error)) => return Called::Refused(error),
        }
        guard.queued = true;
        let answer = rx.await;
        guard.done = true;
        match answer {
            Ok(answer) => Called::Answer(answer),
            Err(_) => Called::Lost,
        }
    }

    /// Sends a request to the remote endpoint `endpoint` of this peer and resolves with its
    /// [`Reply`]: the reply payload (an `ErrorOr<EnsureTable<Reply>>` message that
    /// [`Reply::decode`] / [`decode_reply`] decode, as [`request_as`](Self::request_as) does
    /// in one step) and the protocol version it arrived on. Connects first if there is no
    /// connection. See [Requests](self#requests).
    ///
    /// If the address is already marked failed in the [`FailureMonitor`] the request is
    /// refused at once, [`NotSent`](RequestError::NotSent), without being queued or
    /// dialing (as `tryGetReply` returns at once for a failed address). Call
    /// [`connect`](Self::connect) to probe it, or hold a [`reference`](Self::reference) to have
    /// the peer probe by itself.
    ///
    /// `encode` appends the request message to the vector it is given. It receives the reply
    /// token to put in the request's `ReplyPromise`, and the negotiated protocol version, to
    /// pick the schema set. It runs on the peer's task once the connection is established, so
    /// it may be called after this future was polled and not at all if the future is dropped
    /// first. The vector is the encoder's own and what it writes is copied into the frame, so
    /// even one that overwrites it cannot damage other frames; an encoder that returns an error
    /// or **panics** fails only its request, with [`RequestError::Encode`] (nothing was sent).
    ///
    /// The error says whether the request may have been delivered; see [`RequestError`].
    pub async fn request<F>(&self, endpoint: Uid, encode: F) -> Result<Reply, RequestError>
    where
        F: FnOnce(Uid, ProtocolVersion, &mut Vec<u8>) -> Result<(), EncodeError> + Send + 'static,
    {
        if self.is_failed() {
            let status = self.status();
            return Err(match status.last_error {
                Some(error) => error.not_sent(),
                None => RequestError::NotSent(None),
            });
        }
        let called = self
            .call(|id, reply| Cmd::Request {
                id,
                endpoint,
                encode: Box::new(encode),
                reply,
            })
            .await;
        match called {
            Called::Answer(result) => result,
            Called::NeverQueued => Err(RequestError::NotSent(None)),
            Called::Refused(error) => Err(error.not_sent()),
            Called::Lost => Err(RequestError::MaybeDelivered(None)),
        }
    }

    /// [`request`](Self::request), then the reply decoded as `T` ([`Reply::decode`]): a server
    /// error becomes [`RequestError::Fdb`] / [`RequestError::BrokenPromise`], and a payload
    /// that is not a `T` [`RequestError::Decode`] (the request was delivered). For a reply
    /// whose layout depends on the protocol version, call [`request`](Self::request) and use
    /// [`Reply::version`].
    pub async fn request_as<T, F>(&self, endpoint: Uid, encode: F) -> Result<T, RequestError>
    where
        T: DecodeOwned + FileIdentified,
        F: FnOnce(Uid, ProtocolVersion, &mut Vec<u8>) -> Result<(), EncodeError> + Send + 'static,
    {
        self.request(endpoint, encode).await?.decode::<T>()
    }

    /// Sends one frame **with no reply expected** (a one-way message) to the remote endpoint
    /// `token`, connecting first if there is no connection. `payload` is the serialized message:
    /// it is sent as is, so it must be encoded for the version the connection speaks; prefer
    /// [`send_with`](Self::send_with), which is told the version. Resolves once the established
    /// connection has accepted the frame (it is queued for writing), with the [`Generation`] of
    /// that connection. While the connection's output is full this waits.
    ///
    /// If the future is dropped before it resolves, the frame is not sent if it was still
    /// queued; if it was applied already it is sent. For anything that expects a reply use
    /// [`request`](Self::request), which tracks the reply token.
    pub async fn send(&self, token: Uid, payload: Vec<u8>) -> Result<Generation, SendError> {
        self.send_with(token, move |_, out| {
            out.extend_from_slice(&payload);
            Ok(())
        })
        .await
    }

    /// [`send`](Self::send) with the payload written by `encode`, which is given the negotiated
    /// protocol version and appends the serialized message to the vector. As for
    /// [`request`](Self::request), the vector is the encoder's own, and an encoder that fails
    /// or panics fails only this send ([`SendError::Encode`]; nothing was sent).
    pub async fn send_with<F>(&self, token: Uid, encode: F) -> Result<Generation, SendError>
    where
        F: FnOnce(ProtocolVersion, &mut Vec<u8>) -> Result<(), EncodeError> + Send + 'static,
    {
        let called = self
            .call(|id, ack| Cmd::Send {
                id,
                token,
                encode: Box::new(encode),
                ack,
            })
            .await;
        match called {
            Called::Answer(result) => result,
            Called::NeverQueued | Called::Lost => Err(SendError::Shutdown),
            Called::Refused(error) => Err(SendError::NotSent(error)),
        }
    }

    /// Registers `token` as a local receiver for frames the peer pushes (not for replies,
    /// which [`request`](Self::request) handles): they arrive as [`PeerEvent::Frame`].
    /// Connects if there is no connection. The registration belongs to the connection it
    /// returns the [`Generation`] of, and ends with it (a reconnect registers nothing); keep
    /// it only while it is needed, since a registered token counts as outstanding for the idle
    /// rules. If the future is dropped after the registration was applied (the caller never
    /// saw the answer), the registration is undone.
    pub async fn register(&self, token: Uid) -> Result<Generation, SendError> {
        let called = self.call(|id, ack| Cmd::Register { id, token, ack }).await;
        match called {
            Called::Answer(result) => result,
            Called::NeverQueued | Called::Lost => Err(SendError::Shutdown),
            Called::Refused(error) => Err(SendError::NotSent(error)),
        }
    }

    /// How many commands wait in the peer's queue for its task (at most
    /// [`command_queue_len`](PeerManagerConfig::command_queue_len)).
    pub fn queued_commands(&self) -> usize {
        self.shared.queue.len()
    }

    /// Unregisters a token registered with [`register`](Self::register). Never waits. Await
    /// the `register` first: an `unregister` that arrives before the `register` is applied
    /// does nothing. (A `register` whose future is dropped after it was applied is undone by
    /// the drop itself; see [`register`](Self::register).)
    pub fn unregister(&self, token: Uid) {
        let _ = self.shared.ctrl.unbounded_send(Ctrl::Unregister(token));
    }

    /// Adds a reference to the peer (C++'s `addPeerReference`) until the returned guard is
    /// dropped. A referenced peer keeps its connection and is probed after a failure; when the
    /// last reference is released, an idle connection is closed and the peer may be
    /// [removed](self#references-and-removal). Never waits.
    pub fn reference(&self) -> PeerRef {
        self.shared.add_reference();
        PeerRef {
            handle: self.clone(),
        }
    }

    /// Closes the current connection, if any, as a clean local close (no failure is
    /// recorded). Requests that were sent complete as maybe delivered; commands still waiting
    /// reconnect. Never waits.
    pub fn close_connection(&self) {
        let _ = self.shared.ctrl.unbounded_send(Ctrl::CloseConnection);
    }
}

/// A reference to a peer, from [`PeerHandle::reference`]: it counts like C++'s
/// `addPeerReference` / `removePeerReference` while it lives. Cloning adds another.
#[derive(Debug)]
pub struct PeerRef {
    handle: PeerHandle,
}

impl PeerRef {
    /// The referenced peer.
    pub fn handle(&self) -> &PeerHandle {
        &self.handle
    }
}

impl Clone for PeerRef {
    fn clone(&self) -> PeerRef {
        self.handle.reference()
    }
}

impl Drop for PeerRef {
    fn drop(&mut self) {
        self.handle.shared.remove_reference();
    }
}

/// What every peer task shares with its manager.
pub(crate) struct Shared<R: Runtime, W> {
    pub(crate) rt: R,
    pub(crate) config: PeerManagerConfig,
    pub(crate) wrapper: W,
    pub(crate) sink: Box<dyn PeerEventSink>,
    pub(crate) failures: FailureMonitor,
    /// The version of the cluster, as last negotiated with any address.
    pub(crate) cluster_version: Mutex<Option<ProtocolVersion>>,
    pub(crate) seed: u64,
    peers: Mutex<Peers<R>>,
    next_peer: AtomicU64,
    /// How many [`PeerManager`] values (clones) exist.
    managers: AtomicUsize,
}

struct Entry<R: Runtime> {
    shared: Arc<PeerShared>,
    task: Option<R::Task<()>>,
}

struct Peers<R: Runtime> {
    map: HashMap<PeerKey, Entry<R>>,
    shutdown: bool,
}

impl<R: Runtime, W> Shared<R, W> {
    /// The supported version `version` is compatible with.
    pub(crate) fn supported(&self, version: ProtocolVersion) -> Option<ProtocolVersion> {
        self.config
            .supported_versions
            .iter()
            .copied()
            .find(|v| v.is_compatible(version))
    }

    pub(crate) fn cluster_version(&self) -> Option<ProtocolVersion> {
        *self
            .cluster_version
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn set_cluster_version(&self, version: ProtocolVersion) {
        *self
            .cluster_version
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(version);
    }

    fn lock_peers(&self) -> std::sync::MutexGuard<'_, Peers<R>> {
        self.peers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Removes the peer from the manager if nothing uses it: no handle, no reference, nothing
    /// queued. Decided under the map's lock, which [`PeerManager::peer`] also holds to hand
    /// out a handle, so a handle cannot appear for a peer that was removed. Returns whether it
    /// was removed.
    pub(crate) fn try_retire(&self, key: &PeerKey, peer: &Arc<PeerShared>) -> bool {
        let removed = {
            let mut peers = self.lock_peers();
            if peers.shutdown
                || peer.handles() != 0
                || peer.references() > 0
                || !peer.queue.is_empty()
            {
                return false;
            }
            match peers.map.get(key) {
                Some(entry) if Arc::ptr_eq(&entry.shared, peer) => {}
                _ => return false,
            }
            peers.map.remove(key)
        };
        // Nothing can be queued any more: there is no handle.
        peer.queue.close();
        // The entry holds this very task: let go of it outside the lock.
        drop(removed);
        true
    }

    /// Removes the entry of a peer whose task is gone without having retired it (it panicked
    /// or was dropped), if the map still holds this very peer: the next [`PeerManager::peer`]
    /// then creates a new one. Never called with the lock held, and drops the entry (which
    /// holds the task) outside it.
    pub(crate) fn forget(&self, key: &PeerKey, peer: &Arc<PeerShared>) {
        let removed = {
            let mut peers = self.lock_peers();
            match peers.map.get(key) {
                Some(entry) if Arc::ptr_eq(&entry.shared, peer) => peers.map.remove(key),
                _ => None,
            }
        };
        drop(removed);
    }
}

/// One shared connection per remote address over a [`Runtime`]. See the [module
/// documentation](self).
///
/// Cheap to clone (a shared handle). Create it, and call [`peer`](PeerManager::peer), inside
/// the executor (tasks are spawned). `W` is the [`StreamWrapper`] for TLS; the default,
/// [`NoTls`], supports plain addresses only.
pub struct PeerManager<R: Runtime, W = NoTls> {
    inner: Arc<Shared<R, W>>,
}

impl<R: Runtime, W> Clone for PeerManager<R, W> {
    fn clone(&self) -> Self {
        self.inner.managers.fetch_add(1, Ordering::SeqCst);
        PeerManager {
            inner: self.inner.clone(),
        }
    }
}

impl<R: Runtime, W> Drop for PeerManager<R, W> {
    /// Dropping the last clone shuts the peers down, as [`shutdown`](PeerManager::shutdown)
    /// does, without waiting for them: nothing could ever use or end them again. Handles that
    /// outlive the manager fail their commands.
    fn drop(&mut self) {
        if self.inner.managers.fetch_sub(1, Ordering::SeqCst) == 1 {
            // Detach the tasks (they end by themselves), but let go of them outside the lock:
            // a runtime that cancels a task whose handle is dropped runs its destructors
            // (which take the lock) right there.
            let detached: Vec<R::Task<()>> = {
                let mut peers = self.inner.lock_peers();
                peers.shutdown = true;
                peers
                    .map
                    .values_mut()
                    .filter_map(|entry| {
                        let _ = entry.shared.ctrl.unbounded_send(Ctrl::Shutdown);
                        entry.task.take()
                    })
                    .collect()
            };
            drop(detached);
        }
    }
}

impl<R: Runtime, W> fmt::Debug for PeerManager<R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let peers = self.inner.lock_peers();
        f.debug_struct("PeerManager")
            .field("peers", &peers.map.len())
            .field("shutdown", &peers.shutdown)
            .finish_non_exhaustive()
    }
}

impl<R: Runtime> PeerManager<R, NoTls> {
    /// A manager for plain (non-TLS) addresses. Events from every peer go to `sink`.
    ///
    /// # Panics
    ///
    /// If `config.supported_versions` is empty.
    pub fn new(rt: R, config: PeerManagerConfig, sink: impl PeerEventSink) -> Self {
        PeerManager::with_stream_wrapper(rt, config, NoTls, sink)
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> PeerManager<R, W> {
    /// A manager that also connects to TLS addresses, through `wrapper`.
    ///
    /// # Panics
    ///
    /// If `config.supported_versions` is empty.
    pub fn with_stream_wrapper(
        rt: R,
        config: PeerManagerConfig,
        wrapper: W,
        sink: impl PeerEventSink,
    ) -> Self {
        assert!(
            !config.supported_versions.is_empty(),
            "PeerManagerConfig::supported_versions must not be empty"
        );
        let seed = config.seed.unwrap_or_else(|| {
            use std::hash::{BuildHasher, Hasher};
            std::collections::hash_map::RandomState::new()
                .build_hasher()
                .finish()
        });
        PeerManager {
            inner: Arc::new(Shared {
                rt,
                config,
                wrapper,
                sink: Box::new(sink),
                failures: FailureMonitor::new(),
                cluster_version: Mutex::new(None),
                seed,
                peers: Mutex::new(Peers {
                    map: HashMap::new(),
                    shutdown: false,
                }),
                next_peer: AtomicU64::new(1),
                managers: AtomicUsize::new(1),
            }),
        }
    }

    /// The failure monitor all peers report to.
    pub fn failure_monitor(&self) -> &FailureMonitor {
        &self.inner.failures
    }

    /// The configuration.
    pub fn config(&self) -> &PeerManagerConfig {
        &self.inner.config
    }

    /// The protocol version of the cluster as last negotiated with any address, if one has
    /// been.
    pub fn cluster_version(&self) -> Option<ProtocolVersion> {
        self.inner.cluster_version()
    }

    /// How many peers the manager has (removed ones are not counted).
    pub fn peer_count(&self) -> usize {
        self.inner.lock_peers().map.len()
    }

    /// The peer at `address`, created (as an idle task, nothing is connected yet) on first
    /// use. The same address gives handles to the same peer for as long as it exists; a peer
    /// that was [removed](self#references-and-removal) is created anew. After
    /// [`shutdown`](Self::shutdown) it gives a handle whose commands fail.
    ///
    /// Peers are keyed by IP, port and TLS flag. Call it inside the executor.
    pub fn peer(&self, address: &NetworkAddress) -> PeerHandle {
        let key = PeerKey::new(address);
        let shared = &self.inner;
        let mut peers = shared.lock_peers();
        if let Some(entry) = peers.map.get(&key) {
            return PeerHandle::new(entry.shared.clone(), shared.failures.clone());
        }
        let (ctrl_tx, ctrl_rx) = mpsc::unbounded();
        let id = PeerId(shared.next_peer.fetch_add(1, Ordering::Relaxed));
        let status = PeerStatus {
            phase: if peers.shutdown {
                PeerPhase::Shutdown
            } else {
                PeerPhase::Idle
            },
            version: None,
            known_version: None,
            generation: Generation(0),
            failures: 0,
            last_error: None,
            connected_since: None,
            references: 0,
        };
        let peer = Arc::new(PeerShared {
            id,
            address: *address,
            status: Watch::new(status),
            queue: CommandQueue::new(shared.config.command_queue_len),
            ctrl: ctrl_tx,
            references: AtomicIsize::new(-1),
            handles: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
        });
        let handle = PeerHandle::new(peer.clone(), shared.failures.clone());
        if peers.shutdown {
            // Nothing runs: close the queue so that every command fails.
            peer.queue.close();
            return handle;
        }
        let task = PeerTask::new(shared.clone(), key, peer.clone(), ctrl_rx);
        let task = shared.rt.spawn(task.run());
        peers.map.insert(
            key,
            Entry {
                shared: peer,
                task: Some(task),
            },
        );
        handle
    }

    /// The peer an endpoint is reached at, after [`primary_address`](Self::primary_address).
    pub fn peer_for_endpoint(&self, endpoint: &Endpoint) -> PeerHandle {
        let address = self.primary_address(&endpoint.addresses);
        self.peer(&address)
    }

    /// The address to connect to for an endpoint's address list: [`choose_primary_address`],
    /// with the TLS-ness of a `libfdb_c` client (plain) unless
    /// [`prefer_tls`](PeerManagerConfig::prefer_tls) is set, in which case it is the stream
    /// wrapper's.
    pub fn primary_address(&self, addresses: &NetworkAddressList) -> NetworkAddress {
        let local_tls = self.inner.config.prefer_tls && self.inner.wrapper.tls_enabled();
        choose_primary_address(addresses, local_tls).address
    }

    /// Shuts every peer down and waits for their tasks to end (see the [module
    /// documentation](self#shutdown)). Handles stay valid but their commands fail. Idempotent;
    /// a second concurrent call returns without waiting.
    pub async fn shutdown(&self) {
        let tasks: Vec<R::Task<()>> = {
            let mut peers = self.inner.lock_peers();
            peers.shutdown = true;
            peers
                .map
                .values_mut()
                .filter_map(|entry| {
                    let _ = entry.shared.ctrl.unbounded_send(Ctrl::Shutdown);
                    entry.task.take()
                })
                .collect()
        };
        for task in tasks {
            // A task that panicked has nothing left to wait for.
            let _ = task.await;
        }
    }

    /// Whether [`shutdown`](Self::shutdown) was called.
    pub fn is_shut_down(&self) -> bool {
        self.inner.lock_peers().shutdown
    }
}
