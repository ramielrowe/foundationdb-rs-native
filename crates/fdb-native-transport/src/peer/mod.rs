//! The peer manager: one shared connection per remote address, kept alive, reconnected with
//! backoff, and judged healthy or failed. It drives the sans-IO [`Connection`](crate::Connection) cores over a
//! [`Runtime`].
//!
//! [`PeerManager`] is the entry point. [`PeerManager::peer`] returns the [`PeerHandle`] of an
//! address (creating its task on first use); every endpoint at that address shares it. A
//! handle sends frames, registers local tokens, and reports state; everything arriving from
//! the peer goes to the manager's [`PeerEventSink`]: the hook the request/reply layer plugs
//! into.
//!
//! ```text
//!            PeerHandle (clone per user)                        PeerEventSink
//!   send / register / connect / set_referenced                  (request layer)
//!      │  ctrl: unbounded   data: bounded (backpressure)              ▲
//!      ▼                                                              │ PeerEvent
//!  ┌────────────────────────── peer task (one per address) ───────────┴───────────┐
//!  │  idle ─ demand ─▶ backoff ─▶ dial (TCP, TLS hook) ─▶ run Connection ─▶ ended │
//!  │                      ▲          one poll loop: write, read, timer, commands  │
//!  │                      └──── failure: backoff.ended, failure monitor, probe ◀──┘ │
//!  └──────────────────────────────────────────────────────────────────────────────┘
//!      │ status (Watch<PeerStatus>)            │ FailureMonitor (shared by all peers)
//!      ▼                                        ▼
//!   PeerHandle::status / connect / wait_for     address and endpoint failure state
//! ```
//!
//! # Tasks and channels
//!
//! Each address has one task (spawned with [`Runtime::spawn`]) that owns the socket and the
//! [`Connection`](crate::Connection). It follows the driver loop of the [`connection`](crate::connection) module
//! (input, drain events, write output, wait) with one hand-written `poll_fn` that polls the
//! socket's write side, its read side, the connection's timer and the command queues
//! together, so a single task does all of it without splitting the stream and without
//! any executor-specific combinator. Reads are polled before the timer, so received bytes
//! are fed before a deadline is handled.
//!
//! Handles talk to the task over two `futures-channel` queues, neither tied to an executor:
//!
//! * **control** (unbounded): connect now, set referenced, unregister a token, close the
//!   connection, shut down. Never blocks, so it is safe in destructors.
//! * **data** (bounded by [`PeerManagerConfig::command_queue_len`]): `send` and `register`,
//!   each with a oneshot for its acknowledgement. The task takes a command only while it
//!   holds fewer than that many unapplied ones, so a peer that cannot take more output
//!   (see [Backpressure](#backpressure)) stalls its senders instead of buffering without
//!   bound.
//!
//! Shared state is plain: the [`FailureMonitor`] and each peer's [`PeerStatus`] sit behind
//! locks with waker lists, so waiting for a change needs no runtime.
//!
//! # Connecting
//!
//! A peer connects **lazily**: when something asks (a [`send`](PeerHandle::send), a
//! [`register`](PeerHandle::register), [`connect`](PeerHandle::connect)), or when a failed
//! peer is probed. The attempt is a TCP connect bounded by
//! [`connect_timeout`](PeerManagerConfig::connect_timeout) (`CONNECTION_MONITOR_TIMEOUT`, 2 s:
//! `FlowTransport.actor.cpp` 7.4:867, `FlowTransport.cpp` 8.0:943), then, for a TLS address,
//! the [`StreamWrapper`] (also bounded by it), then the FlowTransport handshake bounded by
//! [`handshake_timeout`](PeerManagerConfig::handshake_timeout). The
//! [`Connection`](crate::Connection)'s `dialed_port` is the address's port and its `tls` flag the address's.
//!
//! Commands that arrive while there is no established connection wait in the task (up to
//! [`command_queue_len`](PeerManagerConfig::command_queue_len) of them, then their senders
//! wait) and are applied, in order, once the handshake has completed. (A frame sent before
//! the handshake would only be held by the [`Connection`](crate::Connection), and discarded
//! if the handshake then failed; waiting here makes the acknowledgement mean something.) If
//! the attempt fails they are refused with [`SendError::NotSent`]. So:
//!
//! * a refused command **was not sent**;
//! * an acknowledged one (`Ok(`[`Generation`]`)`) was accepted by the *established*
//!   connection of that generation, which then writes it: it counts as *sent*, and whether
//!   the peer received it is known only if no [`PeerEvent::Closed`] of that generation
//!   follows (after a close it is *maybe sent*: the at-most-once rule applies).
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
//!    (no backoff, not a failure; commands still waiting stay queued for the new connection:
//!    a caller that encodes for a particular version marks its frames with
//!    [`Outgoing::for_version`], which fails the send with [`SendError::VersionMismatch`]
//!    if the connection turns out to speak another);
//! 3. if it is not supported, fails with [`PeerError::UnsupportedVersion`], which names both
//!    versions.
//!
//! A cluster that is upgraded under a running client is handled by the same rule: the next
//! connection announces the old version, is told the new one, and switches. The negotiated
//! version is [`PeerStatus::version`] and what [`connect`](PeerHandle::connect) returns: a
//! request layer calls `connect()`, encodes with the schema set of the version it returns,
//! and sends with `for_version`, so that a cluster that changes version in between cannot
//! be sent the wrong encoding.
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
//! * With nothing to send, a failed peer is probed again: after `FAILURE_DETECTION_DELAY` if
//!   still marked available, after `SERVER_REQUEST_INTERVAL` if marked failed (7.4:783-812,
//!   8.0:909-916; 7.4's `CLIENT_SKIP_AVAILABLE_PEER_PROBE` and
//!   `CLIENT_FAILED_PEER_PROBE_JITTER` knobs default off/0 and are not modelled). An
//!   unreferenced peer is not probed: with nothing outstanding it simply stays idle.
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
//! count. The monitor also holds per-endpoint "not found" marks: the peer manager records
//! every endpoint-not-found and unauthorized notice it receives, as FDB's
//! `EndpointNotFoundReceiver` does, and the request layer may too
//! ([`FailureMonitor::endpoint_not_found`]).
//!
//! # `choosePrimaryAddress`
//!
//! See [`choose_primary_address`]: an endpoint with a secondary address of the other TLS-ness
//! is connected to at the address whose TLS-ness matches this process's.
//! [`PeerManager::peer_for_endpoint`] applies it.
//!
//! # Backpressure
//!
//! The [`Connection`](crate::Connection) refuses a frame once [`PeerManagerConfig::max_pending_output`] bytes
//! wait to be written. [`send`](PeerHandle::send) then waits (the task keeps the command
//! and applies it as soon as the socket takes output) rather than failing; cancel the future
//! to give up (a command whose future was dropped before it was applied is not sent). A peer
//! that stops reading while still answering pings makes `send` wait indefinitely; apply your
//! own timeout.
//!
//! # TLS
//!
//! See [`StreamWrapper`]: a hook called for TLS addresses with the connected socket.
//!
//! # Shutdown
//!
//! [`PeerManager::shutdown`] asks every peer to finish: queued commands are refused with
//! [`SendError::Shutdown`], what is already queued for writing gets
//! [`shutdown_timeout`](PeerManagerConfig::shutdown_timeout) to be written, the socket is
//! closed, [`PeerEvent::Closed`] (clean) is reported, and the task ends. The call returns
//! when all tasks have.
//!
//! Citations `7.4:N` are lines of `fdbrpc/FlowTransport.actor.cpp` on `release-7.4` and `8.0:N`
//! of `fdbrpc/FlowTransport.cpp` on `release-8.0`, unless another file is named.

use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::{Arc, Mutex, PoisonError};

use fdb_native_wire::{Endpoint, NetworkAddress, NetworkAddressList, ProtocolVersion, Uid};
use futures_channel::{mpsc, oneshot};

use crate::runtime::{Instant, Runtime};

mod address;
mod backoff;
mod config;
mod error;
mod event;
mod failure;
mod stream;
mod task;
mod watch;

pub use address::{choose_primary_address, choose_primary_endpoint};
pub use backoff::BackoffConfig;
pub use config::PeerManagerConfig;
pub use error::{PeerError, SendError};
pub use event::{
    DiscardEvents, EventReceiver, EventSender, Generation, PeerEvent, PeerEventSink, event_channel,
};
pub use failure::{AddressStatus, EndpointFailure, EndpointStatus, FailureMonitor};
pub use stream::{MaybeTls, NoTls, StreamWrapper};

use address::PeerKey;
use task::PeerTask;
use watch::Watch;

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
    /// The peer was shut down.
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
    /// Whether the peer is [referenced](PeerHandle::set_referenced).
    pub referenced: bool,
}

/// A frame to send, built with [`Outgoing::new`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outgoing {
    /// The remote endpoint's token.
    pub token: Uid,
    /// The serialized message.
    pub payload: Vec<u8>,
    /// A local token to [register](PeerHandle::register) (atomically, before the frame is
    /// sent) for the reply, if any. It is unregistered again if the frame is refused.
    pub reply_token: Option<Uid>,
    /// The protocol version the payload was encoded for. The send fails with
    /// [`SendError::VersionMismatch`] if the connection announces another.
    pub version: Option<ProtocolVersion>,
}

impl Outgoing {
    /// A frame of `payload` for the remote endpoint `token`.
    pub fn new(token: Uid, payload: Vec<u8>) -> Outgoing {
        Outgoing {
            token,
            payload,
            reply_token: None,
            version: None,
        }
    }

    /// Also registers `token` for the reply before the frame goes out.
    pub fn reply_token(mut self, token: Uid) -> Outgoing {
        self.reply_token = Some(token);
        self
    }

    /// Requires the connection to speak `version` (compared as FDB does, ignoring the low
    /// 16 bits and flags).
    pub fn for_version(mut self, version: ProtocolVersion) -> Outgoing {
        self.version = Some(version);
        self
    }
}

/// Commands that never wait for room.
#[derive(Debug)]
pub(crate) enum Ctrl {
    /// Something wants a connection.
    Connect,
    SetReferenced(bool),
    Unregister(Uid),
    CloseConnection,
    Shutdown,
}

/// Commands that carry data, and wait for room.
#[derive(Debug)]
pub(crate) enum DataCmd {
    Send {
        out: Outgoing,
        reply: oneshot::Sender<Result<Generation, SendError>>,
    },
    Register {
        token: Uid,
        reply: oneshot::Sender<Result<Generation, SendError>>,
    },
}

impl DataCmd {
    /// Whether the caller gave up waiting.
    pub(crate) fn is_cancelled(&self) -> bool {
        match self {
            DataCmd::Send { reply, .. } | DataCmd::Register { reply, .. } => reply.is_canceled(),
        }
    }

    /// Answers the command's caller.
    pub(crate) fn reply(self, result: Result<Generation, SendError>) {
        let (DataCmd::Send { reply, .. } | DataCmd::Register { reply, .. }) = self;
        let _ = reply.send(result);
    }

    /// Refuses the command.
    pub(crate) fn refuse(self, error: SendError) {
        self.reply(Err(error));
    }
}

/// The part of a peer that handles and the task share.
#[derive(Debug)]
pub(crate) struct PeerShared {
    pub(crate) status: Watch<PeerStatus>,
}

/// A cloneable handle to one peer (an address): the way to send to it and watch it. Handles
/// to the same address share one connection. It holds no executor type and is `Send + Sync`.
///
/// Dropping every handle does not close the connection (the manager keeps the peer);
/// [`set_referenced(false)`](PeerHandle::set_referenced) lets an unused connection be closed.
#[derive(Clone, Debug)]
pub struct PeerHandle {
    address: NetworkAddress,
    ctrl: mpsc::UnboundedSender<Ctrl>,
    data: mpsc::Sender<DataCmd>,
    shared: Arc<PeerShared>,
    failures: FailureMonitor,
}

impl PeerHandle {
    /// The address of the peer.
    pub fn address(&self) -> &NetworkAddress {
        &self.address
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
        self.failures.is_failed(&self.address)
    }

    /// Resolves with `f`'s first `Some` result, evaluated on the current status and again
    /// after every change to it. For example, "wait until connected":
    /// `peer.wait_for(|s| (s.phase == PeerPhase::Connected).then_some(s.version)).await`.
    pub async fn wait_for<T>(&self, mut f: impl FnMut(&PeerStatus) -> Option<T> + Unpin) -> T {
        self.shared.status.wait_for(|s| f(s)).await
    }

    /// Makes sure the peer is connected and returns the negotiated protocol version, which
    /// is the schema set to encode requests with. Connects if there is no connection
    /// (waiting out the reconnect backoff), including the reconnect that version detection
    /// may need; resolves at once if it is connected.
    ///
    /// Fails with the error of the first attempt that fails after the call (a refused
    /// connection, a timeout, an unsupported version, ...), or [`PeerError::Shutdown`].
    pub async fn connect(&self) -> Result<ProtocolVersion, PeerError> {
        let failures_before = self.shared.status.read(|s| s.failures);
        if self.ctrl.unbounded_send(Ctrl::Connect).is_err() {
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

    /// Sends `payload` to the remote endpoint `token`: shorthand for
    /// [`send_with`](Self::send_with) with [`Outgoing::new`].
    pub async fn send(&self, token: Uid, payload: Vec<u8>) -> Result<Generation, SendError> {
        self.send_with(Outgoing::new(token, payload)).await
    }

    /// Sends a frame, connecting first if there is no connection. Resolves once the
    /// connection has accepted the frame (it is queued for writing, or held until the
    /// handshake completes), with the [`Generation`] of the connection it went to. While
    /// the connection's output is full this waits (see [backpressure](self#backpressure)).
    ///
    /// If the future is dropped before it resolves, the frame may or may not have been
    /// accepted; if it never reached the connection it is not sent.
    pub async fn send_with(&self, out: Outgoing) -> Result<Generation, SendError> {
        let (reply, ack) = oneshot::channel();
        self.enqueue(DataCmd::Send { out, reply }).await?;
        ack.await.unwrap_or(Err(SendError::Shutdown))
    }

    /// Registers `token` as a local receiver: frames for it arrive as
    /// [`PeerEvent::Frame`]. Connects if there is no connection. The registration belongs to
    /// the connection it returns the [`Generation`] of, and ends with it (a reconnect
    /// registers nothing); keep it only while a reply is outstanding, since a registered
    /// token counts as outstanding for the idle rules.
    pub async fn register(&self, token: Uid) -> Result<Generation, SendError> {
        let (reply, ack) = oneshot::channel();
        self.enqueue(DataCmd::Register { token, reply }).await?;
        ack.await.unwrap_or(Err(SendError::Shutdown))
    }

    /// Unregisters a local token (after its reply came, or was given up on). Never waits.
    /// Await the [`register`](Self::register) or [`send_with`](Self::send_with) that
    /// registered it first: a cancelled one that was already applied is not undone by an
    /// earlier-arriving `unregister`.
    pub fn unregister(&self, token: Uid) {
        let _ = self.ctrl.unbounded_send(Ctrl::Unregister(token));
    }

    /// Whether anything uses this peer, for the idle rules
    /// ([`Connection::set_referenced`](crate::Connection::set_referenced)). A peer starts
    /// referenced. Unreferenced with nothing outstanding, its connection closes cleanly
    /// after [`unreferenced_close_delay`](crate::MonitorConfig::unreferenced_close_delay)
    /// and a failed peer is no longer probed; the next command reconnects it. Never waits.
    pub fn set_referenced(&self, referenced: bool) {
        let _ = self.ctrl.unbounded_send(Ctrl::SetReferenced(referenced));
    }

    /// Closes the current connection, if any, as a clean local close (no failure is
    /// recorded). Commands still waiting reconnect. Never waits.
    pub fn close_connection(&self) {
        let _ = self.ctrl.unbounded_send(Ctrl::CloseConnection);
    }

    async fn enqueue(&self, cmd: DataCmd) -> Result<(), SendError> {
        let mut tx = self.data.clone();
        poll_fn(|cx| tx.poll_ready(cx))
            .await
            .map_err(|_| SendError::Shutdown)?;
        tx.start_send(cmd).map_err(|_| SendError::Shutdown)
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
}

struct Entry<R: Runtime> {
    handle: PeerHandle,
    task: Option<R::Task<()>>,
}

struct Peers<R: Runtime> {
    map: HashMap<PeerKey, Entry<R>>,
    shutdown: bool,
}

struct Inner<R: Runtime, W> {
    shared: Arc<Shared<R, W>>,
    peers: Mutex<Peers<R>>,
}

/// One shared connection per remote address over a [`Runtime`]. See the [module
/// documentation](self).
///
/// Cheap to clone (a shared handle). Create it, and call [`peer`](PeerManager::peer), inside
/// the executor (tasks are spawned). `W` is the [`StreamWrapper`] for TLS; the default,
/// [`NoTls`], supports plain addresses only.
pub struct PeerManager<R: Runtime, W = NoTls> {
    inner: Arc<Inner<R, W>>,
}

impl<R: Runtime, W> Clone for PeerManager<R, W> {
    fn clone(&self) -> Self {
        PeerManager {
            inner: self.inner.clone(),
        }
    }
}

impl<R: Runtime, W> std::fmt::Debug for PeerManager<R, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let peers = self
            .inner
            .peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
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
        let shared = Arc::new(Shared {
            rt,
            config,
            wrapper,
            sink: Box::new(sink),
            failures: FailureMonitor::new(),
            cluster_version: Mutex::new(None),
            seed,
        });
        PeerManager {
            inner: Arc::new(Inner {
                shared,
                peers: Mutex::new(Peers {
                    map: HashMap::new(),
                    shutdown: false,
                }),
            }),
        }
    }

    /// The failure monitor all peers report to.
    pub fn failure_monitor(&self) -> &FailureMonitor {
        &self.inner.shared.failures
    }

    /// The configuration.
    pub fn config(&self) -> &PeerManagerConfig {
        &self.inner.shared.config
    }

    /// The protocol version of the cluster as last negotiated with any address, if one has
    /// been.
    pub fn cluster_version(&self) -> Option<ProtocolVersion> {
        self.inner.shared.cluster_version()
    }

    /// The peer at `address`, created (as an idle task, nothing is connected yet) on first
    /// use. The same address always gives handles to the same peer. After
    /// [`shutdown`](Self::shutdown) it gives a handle whose commands fail with
    /// [`SendError::Shutdown`].
    ///
    /// Peers are keyed by IP, port and TLS flag. Call it inside the executor.
    pub fn peer(&self, address: &NetworkAddress) -> PeerHandle {
        let key = PeerKey::new(address);
        let mut peers = self
            .inner
            .peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = peers.map.get(&key) {
            return entry.handle.clone();
        }
        let shared = &self.inner.shared;
        let (ctrl_tx, ctrl_rx) = mpsc::unbounded();
        let (data_tx, data_rx) = mpsc::channel(shared.config.command_queue_len.max(1));
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
            referenced: true,
        };
        let peer = Arc::new(PeerShared {
            status: Watch::new(status),
        });
        let handle = PeerHandle {
            address: *address,
            ctrl: ctrl_tx,
            data: data_tx,
            shared: peer.clone(),
            failures: shared.failures.clone(),
        };
        if peers.shutdown {
            // Dropping the receivers makes every command fail.
            return handle;
        }
        let task = PeerTask::new(shared.clone(), *address, key, peer, ctrl_rx, data_rx);
        let task = shared.rt.spawn(task.run());
        peers.map.insert(
            key,
            Entry {
                handle: handle.clone(),
                task: Some(task),
            },
        );
        handle
    }

    /// The peer an endpoint is reached at, after
    /// [`choose_primary_address`] (this process's TLS-ness is the wrapper's
    /// [`tls_enabled`](StreamWrapper::tls_enabled)).
    pub fn peer_for_endpoint(&self, endpoint: &Endpoint) -> PeerHandle {
        let address = self.primary_address(&endpoint.addresses);
        self.peer(&address)
    }

    /// The address to connect to for an endpoint's address list: [`choose_primary_address`]
    /// with this manager's TLS-ness.
    pub fn primary_address(&self, addresses: &NetworkAddressList) -> NetworkAddress {
        choose_primary_address(addresses, self.inner.shared.wrapper.tls_enabled()).address
    }

    /// Shuts every peer down and waits for their tasks to end (see the [module
    /// documentation](self#shutdown)). Handles stay valid but their commands fail with
    /// [`SendError::Shutdown`]. Idempotent; a second concurrent call returns without
    /// waiting.
    pub async fn shutdown(&self) {
        let tasks: Vec<R::Task<()>> = {
            let mut peers = self
                .inner
                .peers
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            peers.shutdown = true;
            peers
                .map
                .values_mut()
                .filter_map(|entry| {
                    let _ = entry.handle.ctrl.unbounded_send(Ctrl::Shutdown);
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
        self.inner
            .peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .shutdown
    }
}
