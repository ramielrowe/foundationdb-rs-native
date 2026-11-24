//! The task of one peer: connects, runs the [`Connection`] and its [`ReplyRegistry`],
//! reconnects.

use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use fdb_native_wire::{EncodeError, NetworkAddress, ProtocolVersion, Uid};
use futures_channel::{mpsc, oneshot};
use futures_core::Stream;
use futures_io::{AsyncRead, AsyncWrite};

use super::address::PeerKey;
use super::backoff::Backoff;
use super::queue::{Cmd, Refusal};
use super::stream::{MaybeTls, StreamWrapper};
use super::{
    Ctrl, Generation, PeerError, PeerEvent, PeerPhase, PeerShared, PeerStatus, Reply, SendError,
    Shared,
};
use crate::connection::{
    Connection, ConnectionConfig, ConnectionError, ConnectionState, SendError as FrameRefused,
};
use crate::rpc::{
    Completion, EndpointFailure, RegistryConfig, ReplyRegistry, RequestError, RpcEvent,
};
use crate::runtime::{Instant, Runtime};

/// A boxed timer, re-made when the connection's deadline moves.
type BoxedSleep<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// How much is read from the socket at once.
const READ_SIZE: usize = 16 * 1024;

/// The most offered to one write on a stream that is not trusted to take nothing when it fails
/// or is pending (see [`WriteTrust`]). Such a write counts everything it was offered as
/// possibly delivered, so this cap is what keeps the frames past it certainly unwritten. (A
/// TLS session can take the whole slice in one `poll_write`: `futures-rustls` keeps encrypting
/// while the socket accepts records, so only the size of the slice bounds what it took.)
const UNSURE_WRITE_MAX: usize = 64 * 1024;

/// What a connection's stream promises about writes that report no count (see
/// [Written offsets](crate::connection#written-offsets)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WriteTrust {
    /// An error (or `Ok(0)`) took none of the buffer.
    errors_take_nothing: bool,
    /// A `Pending` took none of the buffer.
    pending_takes_nothing: bool,
}

impl WriteTrust {
    /// The runtime's own socket: what the runtime promises.
    fn plain<R: Runtime>() -> WriteTrust {
        WriteTrust {
            errors_take_nothing: R::WRITE_ERRORS_TAKE_NOTHING,
            pending_takes_nothing: R::PENDING_TAKES_NOTHING,
        }
    }

    /// A TLS stream: errors are never trusted (a session can take plaintext and then fail on
    /// the socket), `Pending` as far as the wrapper promises.
    fn tls<S, W: StreamWrapper<S>>() -> WriteTrust {
        WriteTrust {
            errors_take_nothing: false,
            pending_takes_nothing: W::PENDING_TAKES_NOTHING,
        }
    }

    /// Whether every write that reports no count took nothing.
    fn exact(self) -> bool {
        self.errors_take_nothing && self.pending_takes_nothing
    }
}

/// The capacity above which the vector encoders write into is dropped after use instead of kept.
const SCRATCH_KEEP: usize = 1 << 20;

/// How many times a pass goes round its sources before the task yields to the executor.
const ROUNDS: usize = 8;
/// How many reads, control messages and commands one round takes from each source, so that no
/// source can keep the others waiting.
const READS_PER_ROUND: usize = 8;
const CTRL_PER_ROUND: usize = 64;
const CMDS_PER_ROUND: usize = 64;

/// A small deterministic generator (SplitMix64) for the jitter and the connection seeds.
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

    /// A number in `[0, 1)`.
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Runs an application's encoder on `scratch`, a vector of its own, and appends what it wrote
/// to `out` (the connection's output, shared with other frames) only if it succeeded. So an
/// encoder that overwrites its vector instead of appending, fails halfway, or panics cannot
/// damage the frames around its own: nothing reaches `out` unless it returned `Ok`. A panic is
/// caught and reported as an encode error.
fn encode_isolated(
    scratch: &mut Vec<u8>,
    out: &mut Vec<u8>,
    encode: impl FnOnce(&mut Vec<u8>) -> Result<(), EncodeError>,
) -> Result<(), EncodeError> {
    scratch.clear();
    let result = match catch_unwind(AssertUnwindSafe(|| encode(&mut *scratch))) {
        Ok(result) => result,
        Err(_) => Err(EncodeError::Invalid("the encoder panicked")),
    };
    if result.is_ok() {
        out.extend_from_slice(scratch);
    }
    if scratch.capacity() > SCRATCH_KEEP {
        *scratch = Vec::new();
    } else {
        scratch.clear();
    }
    result
}

/// Raised by [`timeout`] when the time ran out first.
struct Elapsed;

/// Runs `fut`, giving up after `after` on `rt`'s clock.
async fn timeout<R: Runtime, F: Future>(
    rt: &R,
    after: Duration,
    fut: F,
) -> Result<F::Output, Elapsed> {
    let mut fut = pin!(fut);
    let mut sleep = pin!(rt.sleep(after));
    poll_fn(|cx| {
        if let Poll::Ready(v) = fut.as_mut().poll(cx) {
            return Poll::Ready(Ok(v));
        }
        if sleep.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Elapsed));
        }
        Poll::Pending
    })
    .await
}

/// Connects to `address`: TCP, then the TLS hook for a TLS address, all within one
/// `connect_timeout` (as `connectionKeeper`'s `timeoutError(connect, CONNECTION_MONITOR_TIMEOUT)`
/// covers a TLS handshake too). Owns what it needs, so it can be driven while the task keeps
/// taking control messages.
async fn dial<R: Runtime, W: StreamWrapper<R::Stream>>(
    shared: Arc<Shared<R, W>>,
    address: NetworkAddress,
) -> Result<MaybeTls<R::Stream, W::Stream>, PeerError> {
    let tls = address.is_tls();
    if tls && !shared.wrapper.tls_enabled() {
        return Err(PeerError::TlsNotConfigured);
    }
    let after = shared.config.connect_timeout;
    let socket_addr = address.socket_addr();
    if tls
        && shared.tls_throttle.is_throttled(
            socket_addr,
            shared.rt.now(),
            shared.config.tls_throttle_attempts,
        )
    {
        // `SSLConnection::connect`: wait out the connect timeout, then fail, without dialing.
        shared.rt.sleep(after).await;
        return Err(PeerError::TlsThrottled);
    }
    // Which part ran out of time.
    let tcp_done = AtomicBool::new(false);
    let work = async {
        let socket = shared
            .rt
            .connect(address.socket_addr())
            .await
            .map_err(|e| PeerError::Connect(e.kind()))?;
        if !tls {
            return Ok(MaybeTls::Plain(socket));
        }
        tcp_done.store(true, Ordering::SeqCst);
        match shared.wrapper.wrap(socket, address).await {
            Ok(stream) => Ok(MaybeTls::Tls(stream)),
            Err(e) => {
                // `recordHandshakeFailure`. (A handshake that times out is cancelled first in
                // C++, and records nothing: see `tls_throttle`.)
                shared.tls_throttle.record_failure(
                    socket_addr,
                    shared.rt.now(),
                    shared.config.tls_throttle_timeout,
                );
                Err(PeerError::Tls {
                    kind: e.kind(),
                    message: e.to_string(),
                    source: e.into(),
                })
            }
        }
    };
    match timeout(&shared.rt, after, work).await {
        Ok(result) => result,
        Err(Elapsed) if tcp_done.load(Ordering::SeqCst) => Err(PeerError::TlsTimeout { after }),
        Err(Elapsed) => Err(PeerError::ConnectTimeout { after }),
    }
}

/// What [`PeerTask::step`] saw.
enum Step<T> {
    /// The future it was driving finished.
    Done(T),
    /// A message arrived and was absorbed: look at the state again.
    Changed,
    /// The peer is to shut down.
    Shutdown,
}

/// How a connection ended.
enum RunEnd {
    Closed(ConnectionError),
    Shutdown,
}

/// A request that was sent and awaits its reply.
struct Waiter {
    reply: oneshot::Sender<Result<Reply, RequestError>>,
    /// Its reply token in the registry, to cancel it.
    token: Uid,
    /// The version of the connection it went out on, which its reply arrives on.
    version: ProtocolVersion,
}

/// One connection being run: the sans-IO core and its socket.
struct Run<'a, R: Runtime, S> {
    rt: &'a R,
    conn: Connection,
    stream: S,
    generation: Generation,
    /// The version the connection announced.
    announce: ProtocolVersion,
    /// The version negotiated, once the handshake completed.
    negotiated: Option<ProtocolVersion>,
    established: bool,
    needs_flush: bool,
    sleep: Option<(Instant, BoxedSleep<'a>)>,
    /// What `stream` promises about writes that report no count; the bytes offered to a
    /// write it does not vouch for count as possibly delivered.
    trust: WriteTrust,
}

impl<R: Runtime, S: AsyncWrite + Unpin> Run<'_, R, S> {
    /// The bytes to offer to the next write: everything pending, or a bounded prefix of it
    /// on a stream that may take part of its buffer without reporting it.
    fn offer_len(&self) -> usize {
        let pending = self.conn.pending_output().len();
        if self.trust.exact() {
            pending
        } else {
            pending.min(UNSURE_WRITE_MAX)
        }
    }

    /// A write of the first `offered` bytes failed with `error` (or wrote 0): the connection
    /// ends, with the offered bytes counted as possibly delivered unless the stream guarantees
    /// that a failed write takes nothing.
    fn write_error(&mut self, offered: usize, error: std::io::Error) {
        let now = self.rt.now();
        if self.trust.errors_take_nothing {
            self.conn.io_error(error, now);
        } else {
            self.conn.write_failed(offered, error, now);
        }
    }

    /// A write of the first `offered` bytes is pending: unless the stream guarantees that a
    /// pending write took nothing, the offered bytes count as possibly delivered (they stay
    /// pending, and are offered again).
    fn write_pending(&mut self, offered: usize) {
        if !self.trust.pending_takes_nothing {
            self.conn.output_maybe_written(offered);
        }
    }
}

/// The task of one peer. See the [module documentation](super).
pub(crate) struct PeerTask<R: Runtime, W: StreamWrapper<R::Stream>> {
    shared: Arc<Shared<R, W>>,
    key: PeerKey,
    peer: Arc<PeerShared>,
    ctrl: mpsc::UnboundedReceiver<Ctrl>,
    /// Requests sent and awaiting replies, by command id. Empty between connections: the
    /// registry completes every one when a connection ends.
    registry: ReplyRegistry<u64>,
    waiters: HashMap<u64, Waiter>,
    /// Tokens registered on the connection by commands, by command id and by token: a
    /// registration whose caller dropped the future after it was applied is undone by its
    /// cancellation. Cleared with the connection.
    registered: HashMap<u64, Uid>,
    registered_by_token: HashMap<Uid, u64>,
    /// The vector request encoders write into (see [`encode_isolated`]).
    scratch: Vec<u8>,
    backoff: Backoff,
    rng: Rng,
    /// The number of the latest connection.
    generation: u64,
    /// Something wants a connection. Lasts until a connection is established or an attempt
    /// fails.
    demand: bool,
    /// When the next attempt starts, once decided.
    connect_at: Option<Instant>,
    /// When to probe a failed peer that has nothing to send.
    probe_at: Option<Instant>,
    /// When the current run of failures began.
    first_failure: Option<Instant>,
    shutdown: bool,
    /// The peer was removed from its manager.
    retired: bool,
    /// [`finish`](PeerTask::finish) completed: the peer ended in order. If the task is dropped
    /// without it (a panic, or a runtime that drops its tasks), [`Drop`] cleans up.
    finished: bool,
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> PeerTask<R, W> {
    pub(crate) fn new(
        shared: Arc<Shared<R, W>>,
        key: PeerKey,
        peer: Arc<PeerShared>,
        ctrl: mpsc::UnboundedReceiver<Ctrl>,
    ) -> Self {
        // Peers draw different jitter: mix the address into the seed.
        let mixed = shared.seed
            ^ u64::from(key.addr.port())
            ^ match key.addr.ip() {
                std::net::IpAddr::V4(ip) => u64::from(u32::from(ip)) << 16,
                std::net::IpAddr::V6(ip) => (u128::from(ip) as u64).rotate_left(7),
            };
        let mut rng = Rng(mixed);
        let mut registry_config = RegistryConfig::new();
        if shared.config.seed.is_some() {
            registry_config.seed = rng.next_u64();
        }
        PeerTask {
            backoff: Backoff::new(shared.config.backoff),
            shared,
            key,
            peer,
            ctrl,
            registry: ReplyRegistry::new(registry_config),
            waiters: HashMap::new(),
            registered: HashMap::new(),
            registered_by_token: HashMap::new(),
            scratch: Vec::new(),
            rng,
            generation: 0,
            demand: false,
            connect_at: None,
            probe_at: None,
            first_failure: None,
            shutdown: false,
            retired: false,
            finished: false,
        }
    }

    /// Runs the peer until it is shut down or removed.
    pub(crate) async fn run(mut self) {
        loop {
            if !self.wait_for_attempt().await || !self.attempt().await {
                break;
            }
        }
        self.finish();
    }

    fn finish(&mut self) {
        // Nothing is left in the registry (a connection's end completes every request); a
        // waiter that is, was not answered, which its caller reads as maybe delivered.
        self.waiters.clear();
        for cmd in self.peer.queue.close() {
            cmd.refuse(Refusal::Shutdown);
        }
        self.update(|s| {
            let changed = s.phase != PeerPhase::Shutdown || s.version.is_some();
            s.phase = PeerPhase::Shutdown;
            s.version = None;
            s.connected_since = None;
            changed
        });
        self.ctrl.close();
        if self.retired {
            self.emit(PeerEvent::Removed);
        }
        self.finished = true;
    }

    /// The task is going away without having finished: a panic on it (in the event sink, the
    /// stream wrapper, or the manager's own code), or the executor dropping it. Leaves the peer
    /// as `finish` would, as far as callers and the manager can tell, and calls no application
    /// code (it may be running while a panic unwinds):
    ///
    /// * the queue is closed, so nothing more is accepted, and what is queued (never applied)
    ///   is refused as not sent;
    /// * the waiters of requests already on the connection are dropped, so their callers see
    ///   [`Called::Lost`](super::Called), maybe delivered;
    /// * the phase becomes [`Shutdown`](PeerPhase::Shutdown);
    /// * the address's `on_disconnect` waiters are woken if it was connected;
    /// * the entry in the manager's map is removed if it is still this peer, so the next
    ///   [`PeerManager::peer`](super::PeerManager::peer) creates a new one.
    fn abandon(&mut self) {
        let was_connected = self.peer.status.read(|s| s.phase == PeerPhase::Connected);
        for cmd in self.peer.queue.close() {
            cmd.refuse(Refusal::Shutdown);
        }
        self.waiters.clear();
        self.update(|s| {
            let changed = s.phase != PeerPhase::Shutdown || s.version.is_some();
            s.phase = PeerPhase::Shutdown;
            s.version = None;
            s.connected_since = None;
            changed
        });
        self.shared.forget(&self.key, &self.peer);
        if was_connected {
            self.notify_disconnect();
        }
        self.ctrl.close();
    }

    fn rt(&self) -> R {
        self.shared.rt.clone()
    }

    fn emit(&self, event: PeerEvent) {
        self.shared
            .sink
            .on_event(self.peer.id, &self.peer.address, event);
    }

    /// Changes the shared status; `f` returns whether it changed anything.
    fn update(&self, f: impl FnOnce(&mut PeerStatus) -> bool) {
        self.peer.status.modify(|s| (f(s), ()));
    }

    fn set_phase(&self, phase: PeerPhase) {
        self.update(|s| {
            let changed = s.phase != phase;
            s.phase = phase;
            changed
        });
    }

    /// Refuses every command still queued, and every sender still waiting for room (not those
    /// whose callers gave up).
    fn refuse_queue(&self, error: &PeerError) {
        for cmd in self.peer.queue.refuse_waiting(error) {
            if !cmd.is_cancelled() {
                cmd.refuse(Refusal::NotSent(error.clone()));
            }
        }
    }

    // ---- waiting without a connection ----------------------------------------------------

    fn apply_ctrl(&mut self, ctrl: Ctrl) {
        match ctrl {
            Ctrl::Connect => self.demand = true,
            Ctrl::Shutdown => self.shutdown = true,
            // Nothing to do without a connection (a request that is gone from the queue was
            // never applied; the rest look at the state again).
            Ctrl::RefsChanged
            | Ctrl::Released
            | Ctrl::Unregister(_)
            | Ctrl::Cancel(_)
            | Ctrl::CloseConnection => {}
        }
    }

    /// Drives `fut` while taking control messages: returns when it finishes, when a message
    /// was taken (so that the caller can look at the state again), or at shutdown. With
    /// `watch_queue` it also returns when a command arrives.
    async fn step<F: Future>(
        &mut self,
        mut fut: Pin<&mut F>,
        watch_queue: bool,
    ) -> Step<F::Output> {
        poll_fn(|cx| {
            match Pin::new(&mut self.ctrl).poll_next(cx) {
                Poll::Ready(Some(ctrl)) => {
                    self.apply_ctrl(ctrl);
                    return Poll::Ready(if self.shutdown {
                        Step::Shutdown
                    } else {
                        Step::Changed
                    });
                }
                Poll::Ready(None) => {
                    self.shutdown = true;
                    return Poll::Ready(Step::Shutdown);
                }
                Poll::Pending => {}
            }
            if watch_queue {
                match self.peer.queue.poll_nonempty(cx) {
                    Poll::Ready(true) => return Poll::Ready(Step::Changed),
                    Poll::Ready(false) => {
                        self.shutdown = true;
                        return Poll::Ready(Step::Shutdown);
                    }
                    Poll::Pending => {}
                }
            }
            fut.as_mut().poll(cx).map(Step::Done)
        })
        .await
    }

    /// [`step`](Self::step) until `fut` finishes; `None` at shutdown.
    async fn drive<F: Future>(&mut self, fut: F) -> Option<F::Output> {
        let mut fut = pin!(fut);
        loop {
            match self.step(fut.as_mut(), false).await {
                Step::Done(value) => return Some(value),
                Step::Changed => {}
                Step::Shutdown => return None,
            }
        }
    }

    /// Waits until a connection attempt is due. `false` when the peer is to end: shut down,
    /// or removed because nothing uses it.
    async fn wait_for_attempt(&mut self) -> bool {
        let rt = self.rt();
        loop {
            if self.shutdown {
                return false;
            }
            self.peer.queue.purge_cancelled();
            let now = rt.now();
            let wanted = self.demand || !self.peer.queue.is_empty();
            let sleep_until;
            if wanted {
                let at = match self.connect_at {
                    Some(at) => at,
                    None => {
                        let at = self.backoff.next_attempt(now, self.rng.unit());
                        self.connect_at = Some(at);
                        at
                    }
                };
                if at <= now {
                    return true;
                }
                self.set_phase(PeerPhase::Backoff { until: at });
                sleep_until = Some(at);
            } else {
                if self.shared.try_retire(&self.key, &self.peer) {
                    self.retired = true;
                    return false;
                }
                self.set_phase(PeerPhase::Idle);
                // Only a referenced peer is probed again after a failure. One referenced again
                // while its address is marked failed (its probes stopped while it was not) is
                // probed again: C++'s `retryConnect` outlives the references, and a `Peer`
                // made anew resets the address to available (`Peer::Peer`, 7.4:1104-1113).
                if self.peer.references() <= 0 {
                    self.probe_at = None;
                } else if self.probe_at.is_none()
                    && self.shared.failures.is_failed(&self.peer.address)
                {
                    self.probe_at = Some(now + self.shared.config.failed_probe_interval);
                }
                sleep_until = match self.probe_at {
                    Some(at) if at <= now => {
                        self.probe_at = None;
                        self.demand = true;
                        continue;
                    }
                    other => other,
                };
            }
            let step = match sleep_until {
                Some(at) => self.step(pin!(rt.sleep_until(at)), !wanted).await,
                None => self.step(pin!(std::future::pending::<()>()), !wanted).await,
            };
            if matches!(step, Step::Shutdown) {
                return false;
            }
        }
    }

    // ---- an attempt ----------------------------------------------------------------------

    /// The version to announce first.
    fn first_version(&self) -> ProtocolVersion {
        let known = self.peer.status.read(|s| s.known_version);
        known
            .or_else(|| self.shared.cluster_version())
            .unwrap_or(self.shared.config.supported_versions[0])
    }

    fn learn_version(&self, version: ProtocolVersion) {
        self.shared.set_cluster_version(version);
        self.update(|s| {
            let changed = s.known_version != Some(version);
            s.known_version = Some(version);
            changed
        });
    }

    /// One `notifyDisconnect` for the end of an attempt or connection.
    fn notify_disconnect(&self) {
        self.shared.failures.notify_disconnect(&self.peer.address);
    }

    /// Connects, runs the connection and handles its end. `false` at shutdown.
    async fn attempt(&mut self) -> bool {
        let rt = self.rt();
        self.connect_at = None;
        self.probe_at = None;
        self.backoff.attempt_started(rt.now());
        let mut announce = self.first_version();
        let mut tried = vec![announce];
        loop {
            self.set_phase(PeerPhase::Connecting);
            let dialled = dial(self.shared.clone(), self.peer.address);
            let Some(dialled) = self.drive(dialled).await else {
                return false;
            };
            let stream = match dialled {
                Ok(stream) => stream,
                Err(error) => {
                    self.emit(PeerEvent::ConnectFailed {
                        error: error.clone(),
                    });
                    self.notify_disconnect();
                    self.record_failure(error);
                    return true;
                }
            };
            // Only the runtime's own socket can promise that a failed write took nothing: a
            // TLS session may have encrypted part of the buffer before the socket failed.
            let trust = match &stream {
                MaybeTls::Plain(_) => WriteTrust::plain::<R>(),
                MaybeTls::Tls(_) => WriteTrust::tls::<R::Stream, W>(),
            };
            let error = match self.run_connection(stream, announce, trust).await {
                RunEnd::Shutdown => return false,
                RunEnd::Closed(error) => error,
            };
            if let Some(peer_version) = error.incompatible_version() {
                match self.shared.supported(peer_version) {
                    Some(next) if !tried.contains(&next) => {
                        // The cluster runs another version we can speak: announce it now.
                        self.learn_version(next);
                        self.notify_disconnect();
                        tried.push(next);
                        announce = next;
                        continue;
                    }
                    Some(_) => {}
                    None => {
                        self.notify_disconnect();
                        self.record_failure(PeerError::UnsupportedVersion {
                            peer: peer_version,
                            supported: self.shared.config.supported_versions.clone(),
                        });
                        return true;
                    }
                }
            }
            self.connection_ended(error);
            return true;
        }
    }

    /// Bookkeeping for a connection that ended.
    fn connection_ended(&mut self, error: ConnectionError) {
        let now = self.shared.rt.now();
        self.notify_disconnect();
        if error.is_clean() {
            // A close that says nothing about the peer. If it came before the handshake
            // completed, the demand that asked for the connection is still there: it is
            // retried, not forgotten.
            self.backoff.ended(now, true);
            self.first_failure = None;
            self.update(|s| {
                s.phase = PeerPhase::Idle;
                s.version = None;
                s.connected_since = None;
                true
            });
        } else {
            self.record_failure(PeerError::Connection(error));
        }
    }

    /// An attempt or connection failed. (The caller has counted the disconnect.)
    fn record_failure(&mut self, error: PeerError) {
        let shared = self.shared.clone();
        let config = &shared.config;
        let now = shared.rt.now();
        self.backoff.ended(now, false);
        let first = *self.first_failure.get_or_insert(now);
        if now.saturating_duration_since(first) > config.failure_detection_delay {
            shared
                .failures
                .set_address_failed(&self.peer.address, true, now);
        }
        // The demand that asked for this attempt is answered (with the failure), and so is
        // everything queued: not just a batch of it, all of it.
        self.demand = false;
        self.refuse_queue(&error);
        // Without anything to send, a failed referenced peer is probed again: sooner if it is
        // already marked failed (7.4:783-812, 8.0:909-916).
        self.probe_at = (self.peer.references() > 0).then(|| {
            now + if shared.failures.is_failed(&self.peer.address) {
                config.failed_probe_interval
            } else {
                config.failure_detection_delay
            }
        });
        self.update(|s| {
            s.phase = PeerPhase::Idle;
            s.version = None;
            s.connected_since = None;
            s.failures += 1;
            s.last_error = Some(error);
            true
        });
    }

    /// The handshake with the peer completed. Returns the version negotiated.
    fn established(
        &mut self,
        generation: Generation,
        announced: ProtocolVersion,
        version: ProtocolVersion,
    ) -> ProtocolVersion {
        let now = self.shared.rt.now();
        self.first_failure = None;
        self.demand = false;
        // The version we speak is the supported one the peer's is compatible with.
        let negotiated = self.shared.supported(version).unwrap_or(announced);
        self.shared.set_cluster_version(negotiated);
        self.shared
            .failures
            .set_address_failed(&self.peer.address, false, now);
        self.update(|s| {
            s.phase = PeerPhase::Connected;
            s.version = Some(negotiated);
            s.known_version = Some(negotiated);
            s.generation = generation;
            s.connected_since = Some(now);
            true
        });
        negotiated
    }

    // ---- a connection --------------------------------------------------------------------

    /// Hands the events of the connection to their owners, through the registry: replies and
    /// failures complete their requests, the rest goes to the sink. Returns why the connection
    /// ended, if it did (after the completions of every request that was on it).
    fn drain<S>(&mut self, run: &mut Run<'_, R, S>) -> Option<ConnectionError> {
        let mut closed = None;
        while let Some(event) = self.registry.poll_event(&mut run.conn) {
            match event {
                RpcEvent::Connected { peer } => {
                    let version = peer.protocol_version.without_flags();
                    run.established = true;
                    let negotiated = self.established(run.generation, run.announce, version);
                    run.negotiated = Some(negotiated);
                    self.emit(PeerEvent::Connected {
                        generation: run.generation,
                        version: negotiated,
                        peer,
                    });
                }
                RpcEvent::Completion(completion) => self.complete(completion),
                RpcEvent::EndpointFailed { token, failure } => self.endpoint_failed(token, failure),
                RpcEvent::Frame(frame) => self.emit(PeerEvent::Frame(frame)),
                RpcEvent::UnknownToken { token, reported } => {
                    self.emit(PeerEvent::UnknownToken { token, reported });
                }
                RpcEvent::Closed(error) => {
                    self.emit(PeerEvent::Closed {
                        generation: run.generation,
                        error: PeerError::Connection(error.clone()),
                        established: run.established,
                    });
                    closed = Some(error);
                }
            }
        }
        closed
    }

    /// A request finished: hand its caller the result.
    fn complete(&mut self, completion: Completion<u64>) {
        if let Some(waiter) = self.waiters.remove(&completion.key) {
            let version = waiter.version;
            // The caller may have given up.
            let _ = waiter
                .reply
                .send(completion.result.map(|payload| Reply { version, payload }));
        }
    }

    /// The registry found an endpoint of this peer failed: tell the failure monitor, which
    /// keeps the process-wide view, and the observers.
    fn endpoint_failed(&self, token: Uid, failure: EndpointFailure) {
        let address = &self.peer.address;
        match failure {
            EndpointFailure::Unauthorized => {
                self.shared.failures.unauthorized_endpoint(address, token);
                self.emit(PeerEvent::UnauthorizedEndpoint { token });
            }
            _ => {
                self.shared.failures.endpoint_not_found(address, token);
                self.emit(PeerEvent::EndpointNotFound { token });
            }
        }
    }

    /// After something may have changed the connection: hand out its events, and say whether
    /// the run is over.
    fn after<S>(&mut self, run: &mut Run<'_, R, S>) -> Option<RunEnd> {
        if let Some(error) = self.drain(run) {
            return Some(RunEnd::Closed(error));
        }
        if self.shutdown {
            return Some(RunEnd::Shutdown);
        }
        if run.conn.state() == ConnectionState::Closed {
            // Its `Closed` event was given already: this is not reached by a connection that
            // behaves, and must not leave the task waiting for nothing.
            return Some(RunEnd::Closed(
                run.conn
                    .error()
                    .cloned()
                    .unwrap_or(ConnectionError::LocalClose),
            ));
        }
        None
    }

    /// A control message, with a connection running.
    fn handle_ctrl<S>(&mut self, run: &mut Run<'_, R, S>, ctrl: Ctrl) {
        match ctrl {
            Ctrl::Connect => {
                // An established connection satisfies it; one still handshaking that ends
                // cleanly must be retried.
                if !run.established {
                    self.demand = true;
                }
            }
            Ctrl::RefsChanged | Ctrl::Released => {
                run.conn
                    .set_referenced(self.peer.references() != 0, run.rt.now());
            }
            Ctrl::Unregister(token) => {
                run.conn.unregister(token);
                if let Some(id) = self.registered_by_token.remove(&token) {
                    self.registered.remove(&id);
                }
            }
            Ctrl::Cancel(id) => {
                if let Some(waiter) = self.waiters.remove(&id) {
                    // The caller is gone: no completion is owed.
                    self.registry.cancel(&mut run.conn, waiter.token);
                } else if let Some(token) = self.registered.remove(&id) {
                    // A registration that was applied, and whose caller dropped the future
                    // before seeing it: nobody will unregister it.
                    self.registered_by_token.remove(&token);
                    run.conn.unregister(token);
                }
            }
            Ctrl::CloseConnection => run.conn.close(),
            Ctrl::Shutdown => self.shutdown = true,
        }
    }

    /// Applies one command to the established connection.
    fn apply<S>(&mut self, run: &mut Run<'_, R, S>, cmd: Cmd) {
        // Established: the version is negotiated.
        let version = run.negotiated.unwrap_or(run.announce);
        match cmd {
            Cmd::Request {
                id,
                endpoint,
                encode,
                reply,
            } => {
                if reply.is_canceled() {
                    return; // its caller gave up: it is never sent
                }
                // The failure monitor is the process-wide truth about endpoints; the
                // registry's own memory follows it.
                if let Some(failure) = self
                    .shared
                    .failures
                    .endpoint_failure(&self.peer.address, endpoint)
                {
                    let _ = reply.send(Err(match failure {
                        EndpointFailure::Unauthorized => {
                            RequestError::Unauthorized { token: endpoint }
                        }
                        _ => RequestError::EndpointNotFound {
                            token: endpoint,
                            maybe_delivered: false,
                        },
                    }));
                    return;
                }
                if self.registry.endpoint_failure(endpoint).is_some() {
                    self.registry.forget_failed_endpoint(endpoint);
                }
                // The encoder writes into a vector of its own (a panic is caught there too).
                let scratch = &mut self.scratch;
                let sent = self
                    .registry
                    .send_with(&mut run.conn, endpoint, id, |token, out| {
                        encode_isolated(scratch, out, |buf| encode(token, version, buf))
                    });
                match sent {
                    Ok(token) => {
                        self.waiters.insert(
                            id,
                            Waiter {
                                reply,
                                token,
                                version,
                            },
                        );
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error));
                    }
                }
            }
            Cmd::Send {
                token, encode, ack, ..
            } => {
                if ack.is_canceled() {
                    return;
                }
                let scratch = &mut self.scratch;
                let sent = run.conn.send_with(token, |out| {
                    encode_isolated(scratch, out, |buf| encode(version, buf))
                });
                let result = match sent {
                    Ok(()) => Ok(run.generation),
                    Err(FrameRefused::Frame(e)) => Err(SendError::Frame(e)),
                    Err(FrameRefused::Payload(e)) => Err(SendError::Encode(e)),
                    // Closed (the next drain says why), or output full, which the task checks
                    // before taking a command.
                    Err(_) => Err(SendError::NotSent(PeerError::Connection(
                        run.conn
                            .error()
                            .cloned()
                            .unwrap_or(ConnectionError::OutputOverflow(
                                run.conn.config().max_pending_output,
                            )),
                    ))),
                };
                let _ = ack.send(result);
            }
            Cmd::Register { id, token, ack } => {
                if ack.is_canceled() {
                    return;
                }
                let result = if run.conn.register(token) {
                    // Remembered until its caller is known to have the answer: a caller that
                    // drops the future after this cancels it, which undoes the registration.
                    self.registered.insert(id, token);
                    self.registered_by_token.insert(token, id);
                    Ok(run.generation)
                } else {
                    Err(SendError::TokenUnavailable(token))
                };
                let _ = ack.send(result);
            }
        }
    }

    /// One pass over the connection's sources. See the [module documentation](super#tasks-and-channels).
    fn poll_run<S>(&mut self, run: &mut Run<'_, R, S>, cx: &mut Context<'_>) -> Poll<RunEnd>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        for _ in 0..ROUNDS {
            let mut progress = false;

            // 1. Write until the socket would block, then flush.
            progress |= write_stage(run, cx);
            if let Some(end) = self.after(run) {
                return Poll::Ready(end);
            }

            // 2. Read until the socket would block, the connection stops taking input, or
            // this round's share is used up. Bytes are fed before the timer is looked at.
            for _ in 0..READS_PER_ROUND {
                let buf = run.conn.recv_buf(READ_SIZE);
                if buf.is_empty() {
                    break;
                }
                match Pin::new(&mut run.stream).poll_read(cx, buf) {
                    Poll::Ready(Ok(0)) => run.conn.recv_eof(run.rt.now()),
                    Poll::Ready(Ok(n)) => run.conn.recv_commit(n, run.rt.now()),
                    Poll::Ready(Err(e)) => run.conn.io_error(e, run.rt.now()),
                    Poll::Pending => break,
                }
                progress = true;
                if let Some(end) = self.after(run) {
                    return Poll::Ready(end);
                }
            }

            // 3. The connection's deadline.
            match run.conn.poll_timeout() {
                None => run.sleep = None,
                Some(deadline) => {
                    let mut due = deadline <= run.rt.now();
                    if !due {
                        if run.sleep.as_ref().map(|(at, _)| *at) != Some(deadline) {
                            run.sleep = Some((deadline, Box::pin(run.rt.sleep_until(deadline))));
                        }
                        if let Some((_, sleep)) = run.sleep.as_mut() {
                            due = sleep.as_mut().poll(cx).is_ready();
                        }
                    }
                    if due {
                        run.sleep = None;
                        run.conn.handle_timeout(run.rt.now());
                        progress = true;
                    }
                }
            }
            if let Some(end) = self.after(run) {
                return Poll::Ready(end);
            }

            // 4. Control messages: shutdown must not wait behind traffic.
            for _ in 0..CTRL_PER_ROUND {
                match Pin::new(&mut self.ctrl).poll_next(cx) {
                    Poll::Ready(Some(ctrl)) => {
                        self.handle_ctrl(run, ctrl);
                        progress = true;
                    }
                    Poll::Ready(None) => {
                        self.shutdown = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }
            if let Some(end) = self.after(run) {
                return Poll::Ready(end);
            }

            // 5. Commands, as many as the output has room for. Before the handshake they wait
            // in the queue: they are encoded for the negotiated version.
            if run.established {
                let room = run.conn.config().max_pending_output;
                for _ in 0..CMDS_PER_ROUND {
                    if run.conn.pending_output().len() >= room {
                        break; // the write above is pending: it wakes the task
                    }
                    match self.peer.queue.poll_pop(cx) {
                        Poll::Ready(Some(cmd)) => {
                            self.apply(run, cmd);
                            progress = true;
                        }
                        Poll::Ready(None) => {
                            self.shutdown = true;
                            break;
                        }
                        Poll::Pending => break,
                    }
                }
                if let Some(end) = self.after(run) {
                    return Poll::Ready(end);
                }
            }

            if !progress {
                // Every source was polled and registered its waker.
                return Poll::Pending;
            }
        }
        // Still busy: let the executor run others, and come back.
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    /// Runs one connection over `stream`, whose writes `trust` describes, until it ends.
    async fn run_connection<S>(
        &mut self,
        stream: S,
        announce: ProtocolVersion,
        trust: WriteTrust,
    ) -> RunEnd
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let rt = self.rt();
        self.generation += 1;
        let generation = Generation(self.generation);
        self.registered.clear();
        self.registered_by_token.clear();
        self.update(|s| {
            s.generation = generation;
            true
        });

        let config = &self.shared.config;
        let mut cc = ConnectionConfig::for_version(announce, self.peer.address.is_tls());
        cc.dialed_port = Some(self.peer.address.port);
        cc.handshake_timeout = config.handshake_timeout;
        cc.monitor = config.monitor;
        cc.max_pending_output = config.max_pending_output;
        // The write path below reports every byte the stream may have taken (see
        // `WriteTrust`), so requests never handed to it can be reported as not sent.
        cc.unwritten_not_sent = true;
        cc.max_packet_len = config.max_packet_len;
        if config.seed.is_some() {
            cc.seed = self.rng.next_u64();
        }
        let mut conn = Connection::new(cc, rt.now());
        // A peer that was referenced and no longer is: unreferenced (C++ `peerReferences == 0`;
        // one never referenced, -1, is kept).
        if self.peer.references() == 0 {
            conn.set_referenced(false, rt.now());
        }
        let mut run = Run {
            rt: &rt,
            conn,
            stream,
            generation,
            announce,
            negotiated: None,
            established: false,
            needs_flush: false,
            sleep: None,
            trust,
        };

        let end = poll_fn(|cx| self.poll_run(&mut run, cx)).await;
        // Registrations end with their connection.
        self.registered.clear();
        self.registered_by_token.clear();

        if let RunEnd::Shutdown = end {
            // What was sent and not answered completes as cancelled; what was queued is
            // refused when the task finishes.
            self.registry.cancel_all(&mut run.conn);
            self.drain(&mut run);
            self.close_gracefully(&mut run).await;
            run.conn.close();
            self.drain(&mut run);
            // The connection ended: its waiters on the failure monitor are woken, as for any
            // other end (`notifyDisconnect`), not left until the monitor is dropped.
            self.notify_disconnect();
        }
        end
    }

    /// Writes what the connection has queued, bounded by the shutdown timeout, and closes
    /// the stream.
    async fn close_gracefully<S>(&self, run: &mut Run<'_, R, S>)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let rt = self.rt();
        // What each poll of a write returned is reported at once, a pending one included:
        // the timeout can only drop `work` while a write is pending, which is accounted for.
        let work = async {
            while !run.conn.pending_output().is_empty() {
                let offered = run.offer_len();
                let written = poll_fn(|cx| {
                    let buf = &run.conn.pending_output()[..offered];
                    let written = Pin::new(&mut run.stream).poll_write(cx, buf);
                    if written.is_pending() {
                        run.write_pending(offered);
                    }
                    written
                })
                .await;
                match written {
                    Ok(n) if n > 0 => run.conn.consume_output(n),
                    Ok(_) => {
                        run.write_error(offered, std::io::ErrorKind::WriteZero.into());
                        return;
                    }
                    Err(e) => {
                        // What it may have taken counts, though the requests have completed
                        // (as cancelled) already.
                        run.write_error(offered, e);
                        return;
                    }
                }
            }
            let _ = poll_fn(|cx| Pin::new(&mut run.stream).poll_flush(cx)).await;
            let _ = poll_fn(|cx| Pin::new(&mut run.stream).poll_close(cx)).await;
        };
        let _ = timeout(&rt, self.shared.config.shutdown_timeout, work).await;
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Drop for PeerTask<R, W> {
    fn drop(&mut self) {
        if !self.finished {
            // Never let this panic: it can run while another panic unwinds, which aborts.
            let _ = catch_unwind(AssertUnwindSafe(|| self.abandon()));
        }
    }
}

/// Writes the connection's output until the socket would block, then flushes. Returns whether
/// anything was written (or the socket failed).
///
/// Every byte a write may have taken is reported to the connection before anything else
/// happens to it (see [Written offsets](crate::connection#written-offsets)): `Ok(n)` is
/// consumed at once (bytes a TLS session took count, flushed or not); an error or `Ok(0)`,
/// and a `Pending`, took nothing only on a stream that promises so ([`WriteTrust`]); otherwise
/// the bytes offered to it count as possibly delivered.
fn write_stage<R: Runtime, S: AsyncWrite + Unpin>(
    run: &mut Run<'_, R, S>,
    cx: &mut Context<'_>,
) -> bool {
    let mut progress = false;
    while !run.conn.pending_output().is_empty() {
        let offered = run.offer_len();
        let buf = &run.conn.pending_output()[..offered];
        match Pin::new(&mut run.stream).poll_write(cx, buf) {
            Poll::Ready(Ok(0)) => {
                run.write_error(offered, std::io::ErrorKind::WriteZero.into());
                return true;
            }
            Poll::Ready(Ok(n)) => {
                run.conn.consume_output(n);
                run.needs_flush = true;
                progress = true;
            }
            Poll::Ready(Err(e)) => {
                run.write_error(offered, e);
                return true;
            }
            Poll::Pending => {
                run.write_pending(offered);
                return progress;
            }
        }
    }
    if run.needs_flush {
        match Pin::new(&mut run.stream).poll_flush(cx) {
            Poll::Ready(Ok(())) => run.needs_flush = false,
            Poll::Ready(Err(e)) => {
                run.conn.io_error(e, run.rt.now());
                return true;
            }
            Poll::Pending => {}
        }
    }
    progress
}
