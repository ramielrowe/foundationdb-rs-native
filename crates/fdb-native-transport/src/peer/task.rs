//! The task of one peer: connects, runs the [`Connection`], reconnects.

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use fdb_native_wire::{NetworkAddress, ProtocolVersion};
use futures_channel::mpsc;
use futures_core::Stream;
use futures_io::{AsyncRead, AsyncWrite};

use super::address::PeerKey;
use super::backoff::Backoff;
use super::stream::{MaybeTls, StreamWrapper};
use super::{
    Ctrl, DataCmd, Generation, PeerError, PeerEvent, PeerPhase, PeerShared, PeerStatus, SendError,
    Shared,
};
use crate::connection::{Connection, ConnectionConfig, ConnectionError, ConnectionState, Event};
use crate::runtime::{Instant, Runtime};

/// A boxed timer, re-made when the connection's deadline moves.
type BoxedSleep<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// How much is read from the socket at once.
const READ_SIZE: usize = 16 * 1024;

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

/// Connects to `address`: TCP, then the TLS hook for a TLS address. Owns what it needs, so it
/// can be driven while the task keeps taking commands.
async fn dial<R: Runtime, W: StreamWrapper<R::Stream>>(
    shared: Arc<Shared<R, W>>,
    address: NetworkAddress,
) -> Result<MaybeTls<R::Stream, W::Stream>, PeerError> {
    let tls = address.is_tls();
    if tls && !shared.wrapper.tls_enabled() {
        return Err(PeerError::TlsNotConfigured);
    }
    let after = shared.config.connect_timeout;
    let socket = match timeout(&shared.rt, after, shared.rt.connect(address.socket_addr())).await {
        Ok(Ok(socket)) => socket,
        Ok(Err(e)) => return Err(PeerError::Connect(e.kind())),
        Err(Elapsed) => return Err(PeerError::ConnectTimeout { after }),
    };
    if !tls {
        return Ok(MaybeTls::Plain(socket));
    }
    match timeout(&shared.rt, after, shared.wrapper.wrap(socket, address)).await {
        Ok(Ok(stream)) => Ok(MaybeTls::Tls(stream)),
        Ok(Err(e)) => Err(PeerError::Tls {
            kind: e.kind(),
            message: e.to_string(),
        }),
        Err(Elapsed) => Err(PeerError::TlsTimeout { after }),
    }
}

/// What [`PeerTask::step`] saw.
enum Step<T> {
    /// The future it was driving finished.
    Done(T),
    /// A command was absorbed: look at the state again.
    Changed,
    /// The peer is to shut down.
    Shutdown,
}

/// How a connection ended.
enum RunEnd {
    Closed(ConnectionError),
    Shutdown,
}

/// Why the connection's wait loop woke.
enum Wake {
    /// Output was written, input was fed, or an error was recorded: go round.
    Progress,
    Timer,
    Ctrl(Ctrl),
    CtrlClosed,
    Data(DataCmd),
    DataClosed,
}

/// What applying a command to a connection came to.
enum Applied {
    /// It was applied, or refused for good.
    Done(Result<(), SendError>),
    /// The connection's output is full: try again after a write.
    NoRoom,
    /// The connection ended.
    Closed,
}

/// Applies one command to `conn`.
fn apply(conn: &mut Connection, cmd: &DataCmd) -> Applied {
    use crate::connection::SendError as Refused;
    if conn.state() == ConnectionState::Closed {
        return Applied::Closed;
    }
    match cmd {
        DataCmd::Register { token, .. } => Applied::Done(if conn.register(*token) {
            Ok(())
        } else {
            Err(SendError::TokenUnavailable(*token))
        }),
        DataCmd::Send { out, .. } => {
            if let Some(expected) = out.version {
                if !expected.is_compatible(conn.local_version()) {
                    return Applied::Done(Err(SendError::VersionMismatch {
                        expected,
                        actual: conn.local_version(),
                    }));
                }
            }
            if let Some(reply) = out.reply_token {
                if !conn.register(reply) {
                    return Applied::Done(Err(SendError::TokenUnavailable(reply)));
                }
            }
            match conn.send(out.token, &out.payload) {
                Ok(()) => Applied::Done(Ok(())),
                Err(error) => {
                    if let Some(reply) = out.reply_token {
                        conn.unregister(reply);
                    }
                    match error {
                        Refused::Backpressure => Applied::NoRoom,
                        Refused::Frame(e) => Applied::Done(Err(SendError::Frame(e))),
                        // `Closed`: the next drain reports why. (`Payload` holds `Infallible`
                        // and the enum is non-exhaustive.)
                        _ => Applied::Closed,
                    }
                }
            }
        }
    }
}

/// The task of one peer. See the [module documentation](super).
pub(crate) struct PeerTask<R: Runtime, W: StreamWrapper<R::Stream>> {
    shared: Arc<Shared<R, W>>,
    address: NetworkAddress,
    peer: Arc<PeerShared>,
    ctrl: mpsc::UnboundedReceiver<Ctrl>,
    data: mpsc::Receiver<DataCmd>,
    data_closed: bool,
    /// Commands taken from the queue and not applied yet, oldest first.
    pending: VecDeque<DataCmd>,
    backoff: Backoff,
    rng: Rng,
    /// The number of the latest connection.
    generation: u64,
    referenced: bool,
    /// Something wants a connection now.
    demand: bool,
    /// When the next attempt starts, once decided.
    connect_at: Option<Instant>,
    /// When to probe a failed peer that has nothing to send.
    probe_at: Option<Instant>,
    /// When the current run of failures began.
    first_failure: Option<Instant>,
    shutdown: bool,
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> PeerTask<R, W> {
    pub(crate) fn new(
        shared: Arc<Shared<R, W>>,
        address: NetworkAddress,
        key: PeerKey,
        peer: Arc<PeerShared>,
        ctrl: mpsc::UnboundedReceiver<Ctrl>,
        data: mpsc::Receiver<DataCmd>,
    ) -> Self {
        // Peers draw different jitter: mix the address into the seed.
        let mixed = shared.seed
            ^ u64::from(key.addr.port())
            ^ match key.addr.ip() {
                std::net::IpAddr::V4(ip) => u64::from(u32::from(ip)) << 16,
                std::net::IpAddr::V6(ip) => (u128::from(ip) as u64).rotate_left(7),
            };
        PeerTask {
            backoff: Backoff::new(shared.config.backoff),
            shared,
            address,
            peer,
            ctrl,
            data,
            data_closed: false,
            pending: VecDeque::new(),
            rng: Rng(mixed),
            generation: 0,
            referenced: true,
            demand: false,
            connect_at: None,
            probe_at: None,
            first_failure: None,
            shutdown: false,
        }
    }

    /// Runs the peer until it is shut down (or every handle and the manager are gone).
    pub(crate) async fn run(mut self) {
        loop {
            if !self.wait_for_attempt().await || !self.attempt().await {
                break;
            }
        }
        self.finish();
    }

    fn finish(&mut self) {
        self.fail_pending(SendError::Shutdown);
        self.update(|s| {
            let changed = s.phase != PeerPhase::Shutdown || s.version.is_some();
            s.phase = PeerPhase::Shutdown;
            s.version = None;
            s.connected_since = None;
            changed
        });
        self.ctrl.close();
        self.data.close();
        // Anything still queued is dropped here, which cancels its sender's wait.
        while let Ok(cmd) = self.data.try_recv() {
            cmd.refuse(SendError::Shutdown);
        }
    }

    fn rt(&self) -> R {
        self.shared.rt.clone()
    }

    fn emit(&self, event: PeerEvent) {
        self.shared.sink.on_event(&self.address, event);
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

    fn set_referenced(&mut self, referenced: bool) {
        self.referenced = referenced;
        self.update(|s| {
            let changed = s.referenced != referenced;
            s.referenced = referenced;
            changed
        });
    }

    /// Refuses every command that was not applied.
    fn fail_pending(&mut self, error: SendError) {
        for cmd in self.pending.drain(..) {
            cmd.refuse(error.clone());
        }
    }

    // ---- waiting without a connection ----------------------------------------------------

    fn apply_ctrl(&mut self, ctrl: Ctrl) {
        match ctrl {
            Ctrl::Connect => self.demand = true,
            Ctrl::SetReferenced(referenced) => self.set_referenced(referenced),
            Ctrl::Unregister(_) | Ctrl::CloseConnection => {}
            Ctrl::Shutdown => self.shutdown = true,
        }
    }

    /// Drives `fut` while taking commands: returns when it finishes, when a command was
    /// taken (so that the caller can look at the state again), or at shutdown.
    async fn step<F: Future>(&mut self, mut fut: Pin<&mut F>) -> Step<F::Output> {
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
            if !self.data_closed && self.pending.len() < self.shared.config.command_queue_len {
                match Pin::new(&mut self.data).poll_next(cx) {
                    Poll::Ready(Some(cmd)) => {
                        self.pending.push_back(cmd);
                        return Poll::Ready(Step::Changed);
                    }
                    Poll::Ready(None) => self.data_closed = true,
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
            match self.step(fut.as_mut()).await {
                Step::Done(value) => return Some(value),
                Step::Changed => {}
                Step::Shutdown => return None,
            }
        }
    }

    /// Waits until a connection attempt is due. `false` at shutdown.
    async fn wait_for_attempt(&mut self) -> bool {
        let rt = self.rt();
        loop {
            if self.shutdown {
                return false;
            }
            let now = rt.now();
            let wanted = self.demand || !self.pending.is_empty();
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
                self.set_phase(PeerPhase::Idle);
                sleep_until = match (self.probe_at, self.referenced) {
                    (Some(at), true) if at <= now => {
                        self.probe_at = None;
                        self.demand = true;
                        continue;
                    }
                    (Some(at), true) => Some(at),
                    _ => None,
                };
            }
            let step = match sleep_until {
                Some(at) => self.step(pin!(rt.sleep_until(at))).await,
                None => self.step(pin!(std::future::pending::<()>())).await,
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

    /// Connects, runs the connection and handles its end. `false` at shutdown.
    async fn attempt(&mut self) -> bool {
        let rt = self.rt();
        self.demand = false;
        self.connect_at = None;
        self.probe_at = None;
        self.backoff.attempt_started(rt.now());
        let mut announce = self.first_version();
        let mut tried = vec![announce];
        loop {
            self.set_phase(PeerPhase::Connecting);
            let dialled = dial(self.shared.clone(), self.address);
            let Some(dialled) = self.drive(dialled).await else {
                return false;
            };
            let stream = match dialled {
                Ok(stream) => stream,
                Err(error) => {
                    self.emit(PeerEvent::ConnectFailed {
                        error: error.clone(),
                    });
                    self.record_failure(error);
                    return true;
                }
            };
            let error = match self.run_connection(stream, announce).await {
                RunEnd::Shutdown => return false,
                RunEnd::Closed(error) => error,
            };
            if let Some(peer_version) = error.incompatible_version() {
                match self.shared.supported(peer_version) {
                    Some(next) if !tried.contains(&next) => {
                        // The cluster runs another version we can speak: announce it now.
                        self.learn_version(next);
                        self.shared.failures.notify_disconnect(&self.address);
                        tried.push(next);
                        announce = next;
                        continue;
                    }
                    Some(_) => {}
                    None => {
                        let error = PeerError::UnsupportedVersion {
                            peer: peer_version,
                            supported: self.shared.config.supported_versions.clone(),
                        };
                        self.record_failure(error);
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
        self.shared.failures.notify_disconnect(&self.address);
        if error.is_clean() {
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

    /// An attempt or connection failed.
    fn record_failure(&mut self, error: PeerError) {
        let shared = self.shared.clone();
        let config = &shared.config;
        let now = shared.rt.now();
        shared.failures.notify_disconnect(&self.address);
        self.backoff.ended(now, false);
        let first = *self.first_failure.get_or_insert(now);
        if now.saturating_duration_since(first) > config.failure_detection_delay {
            shared.failures.set_address_failed(&self.address, true, now);
        }
        self.fail_pending(SendError::NotSent(error.clone()));
        // Without anything to send, a failed peer is probed again: sooner if it is already
        // marked failed (7.4:783-812, 8.0:909-916).
        self.probe_at = self.referenced.then(|| {
            now + if shared.failures.is_failed(&self.address) {
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

    /// The handshake with the peer completed.
    fn established(
        &mut self,
        generation: Generation,
        announced: ProtocolVersion,
        version: ProtocolVersion,
    ) {
        let now = self.shared.rt.now();
        self.first_failure = None;
        // The version we speak is the supported one the peer's is compatible with.
        let negotiated = self.shared.supported(version).unwrap_or(announced);
        self.shared.set_cluster_version(negotiated);
        self.shared
            .failures
            .set_address_failed(&self.address, false, now);
        self.update(|s| {
            s.phase = PeerPhase::Connected;
            s.version = Some(negotiated);
            s.known_version = Some(negotiated);
            s.generation = generation;
            s.connected_since = Some(now);
            true
        });
    }

    // ---- a connection --------------------------------------------------------------------

    /// Hands the events of `conn` to the sink. Returns why it ended, if it did.
    fn drain(
        &mut self,
        conn: &mut Connection,
        generation: Generation,
        announced: ProtocolVersion,
        established: &mut bool,
    ) -> Option<ConnectionError> {
        let mut closed = None;
        while let Some(event) = conn.poll_event() {
            let event = match event {
                Event::Connected { peer } => {
                    let version = peer.protocol_version.without_flags();
                    *established = true;
                    self.established(generation, announced, version);
                    PeerEvent::Connected {
                        generation,
                        version: self.shared.supported(version).unwrap_or(version),
                        peer,
                    }
                }
                Event::Frame(frame) => PeerEvent::Frame(frame.into_owned()),
                Event::UnknownToken { token, reported } => {
                    PeerEvent::UnknownToken { token, reported }
                }
                Event::EndpointNotFound { token } => {
                    self.shared
                        .failures
                        .endpoint_not_found(&self.address, token);
                    PeerEvent::EndpointNotFound { token }
                }
                Event::UnauthorizedEndpoint { token } => {
                    self.shared
                        .failures
                        .unauthorized_endpoint(&self.address, token);
                    PeerEvent::UnauthorizedEndpoint { token }
                }
                Event::Closed(error) => {
                    self.emit(PeerEvent::Closed {
                        generation,
                        error: PeerError::Connection(error.clone()),
                        established: *established,
                    });
                    closed = Some(error);
                    continue;
                }
            };
            self.emit(event);
        }
        closed
    }

    /// Applies the commands that can be applied, oldest first. Stops at the first that meets
    /// backpressure (it is tried again after the next write) or when the connection ends.
    fn apply_pending(&mut self, conn: &mut Connection, generation: Generation) {
        loop {
            let applied = match self.pending.front() {
                None => return,
                Some(cmd) if cmd.is_cancelled() => {
                    // Its caller gave up before it was applied: it is never sent.
                    self.pending.pop_front();
                    continue;
                }
                Some(cmd) => apply(conn, cmd),
            };
            match applied {
                Applied::NoRoom | Applied::Closed => return,
                Applied::Done(result) => {
                    if let Some(cmd) = self.pending.pop_front() {
                        cmd.reply(result.map(|()| generation));
                    }
                }
            }
        }
    }

    /// Runs one connection over `stream` until it ends.
    async fn run_connection<S>(&mut self, mut stream: S, announce: ProtocolVersion) -> RunEnd
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let rt = self.rt();
        self.generation += 1;
        let generation = Generation(self.generation);
        self.update(|s| {
            s.generation = generation;
            true
        });

        let config = &self.shared.config;
        let mut cc = ConnectionConfig::for_version(announce, self.address.is_tls());
        cc.dialed_port = Some(self.address.port);
        cc.handshake_timeout = config.handshake_timeout;
        cc.monitor = config.monitor;
        cc.max_pending_output = config.max_pending_output;
        cc.max_packet_len = config.max_packet_len;
        if config.seed.is_some() {
            cc.seed = self.rng.next_u64();
        }
        let mut conn = Connection::new(cc, rt.now());
        if !self.referenced {
            conn.set_referenced(false, rt.now());
        }

        let mut established = false;
        let mut needs_flush = false;
        let mut sleep: Option<(Instant, BoxedSleep<'_>)> = None;
        let mut data_first = false;

        let end = loop {
            if let Some(error) = self.drain(&mut conn, generation, announce, &mut established) {
                break RunEnd::Closed(error);
            }
            if self.shutdown {
                break RunEnd::Shutdown;
            }
            if established {
                self.apply_pending(&mut conn, generation);
            }
            if conn.state() == ConnectionState::Closed {
                continue; // the next drain reports why
            }

            data_first = !data_first;
            let wake = poll_fn(|cx| {
                // Control messages first: shutdown must not wait behind traffic.
                match Pin::new(&mut self.ctrl).poll_next(cx) {
                    Poll::Ready(Some(ctrl)) => return Poll::Ready(Wake::Ctrl(ctrl)),
                    Poll::Ready(None) => return Poll::Ready(Wake::CtrlClosed),
                    Poll::Pending => {}
                }
                // Commands are polled before the socket every other time, so that neither a
                // busy peer starves them nor they starve the peer.
                let take_data =
                    !self.data_closed && self.pending.len() < self.shared.config.command_queue_len;
                if take_data && data_first {
                    match Pin::new(&mut self.data).poll_next(cx) {
                        Poll::Ready(Some(cmd)) => return Poll::Ready(Wake::Data(cmd)),
                        Poll::Ready(None) => return Poll::Ready(Wake::DataClosed),
                        Poll::Pending => {}
                    }
                }

                // Write.
                let mut wrote = false;
                loop {
                    let result = if conn.pending_output().is_empty() {
                        break;
                    } else {
                        Pin::new(&mut stream).poll_write(cx, conn.pending_output())
                    };
                    match result {
                        Poll::Ready(Ok(0)) => {
                            conn.io_error(std::io::ErrorKind::WriteZero, rt.now());
                            return Poll::Ready(Wake::Progress);
                        }
                        Poll::Ready(Ok(n)) => {
                            conn.consume_output(n);
                            wrote = true;
                            needs_flush = true;
                        }
                        Poll::Ready(Err(e)) => {
                            conn.io_error(e.kind(), rt.now());
                            return Poll::Ready(Wake::Progress);
                        }
                        Poll::Pending => break,
                    }
                }
                if needs_flush && conn.pending_output().is_empty() {
                    match Pin::new(&mut stream).poll_flush(cx) {
                        Poll::Ready(Ok(())) => needs_flush = false,
                        Poll::Ready(Err(e)) => {
                            conn.io_error(e.kind(), rt.now());
                            return Poll::Ready(Wake::Progress);
                        }
                        Poll::Pending => {}
                    }
                }
                if wrote {
                    return Poll::Ready(Wake::Progress);
                }

                // Read, before looking at the clock: bytes in the socket are fed first.
                let buf = conn.recv_buf(READ_SIZE);
                if !buf.is_empty() {
                    match Pin::new(&mut stream).poll_read(cx, buf) {
                        Poll::Ready(Ok(0)) => {
                            conn.recv_eof(rt.now());
                            return Poll::Ready(Wake::Progress);
                        }
                        Poll::Ready(Ok(n)) => {
                            conn.recv_commit(n, rt.now());
                            return Poll::Ready(Wake::Progress);
                        }
                        Poll::Ready(Err(e)) => {
                            conn.io_error(e.kind(), rt.now());
                            return Poll::Ready(Wake::Progress);
                        }
                        Poll::Pending => {}
                    }
                }

                // The connection's deadline.
                match conn.poll_timeout() {
                    None => sleep = None,
                    Some(deadline) => {
                        if deadline <= rt.now() {
                            return Poll::Ready(Wake::Timer);
                        }
                        if sleep.as_ref().map(|(at, _)| *at) != Some(deadline) {
                            sleep = Some((deadline, Box::pin(rt.sleep_until(deadline))));
                        }
                        if let Some((_, fut)) = sleep.as_mut() {
                            if fut.as_mut().poll(cx).is_ready() {
                                sleep = None;
                                return Poll::Ready(Wake::Timer);
                            }
                        }
                    }
                }

                if take_data && !data_first {
                    match Pin::new(&mut self.data).poll_next(cx) {
                        Poll::Ready(Some(cmd)) => return Poll::Ready(Wake::Data(cmd)),
                        Poll::Ready(None) => return Poll::Ready(Wake::DataClosed),
                        Poll::Pending => {}
                    }
                }
                Poll::Pending
            })
            .await;

            match wake {
                Wake::Progress => {}
                Wake::Timer => conn.handle_timeout(rt.now()),
                Wake::Ctrl(Ctrl::Connect) => {}
                Wake::Ctrl(Ctrl::SetReferenced(referenced)) => {
                    self.set_referenced(referenced);
                    conn.set_referenced(referenced, rt.now());
                }
                Wake::Ctrl(Ctrl::Unregister(token)) => {
                    conn.unregister(token);
                }
                Wake::Ctrl(Ctrl::CloseConnection) => conn.close(),
                Wake::Ctrl(Ctrl::Shutdown) | Wake::CtrlClosed => self.shutdown = true,
                Wake::Data(cmd) => self.pending.push_back(cmd),
                Wake::DataClosed => self.data_closed = true,
            }
        };

        if let RunEnd::Shutdown = end {
            self.fail_pending(SendError::Shutdown);
            self.close_gracefully(&mut stream, &mut conn).await;
            conn.close();
            self.drain(&mut conn, generation, announce, &mut established);
        }
        end
    }

    /// Writes what the connection has queued, bounded by the shutdown timeout, and closes
    /// the stream.
    async fn close_gracefully<S>(&self, stream: &mut S, conn: &mut Connection)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let rt = self.rt();
        let work = async {
            while !conn.pending_output().is_empty() {
                let written =
                    poll_fn(|cx| Pin::new(&mut *stream).poll_write(cx, conn.pending_output()))
                        .await;
                match written {
                    Ok(n) if n > 0 => conn.consume_output(n),
                    _ => return,
                }
            }
            let _ = poll_fn(|cx| Pin::new(&mut *stream).poll_flush(cx)).await;
            let _ = poll_fn(|cx| Pin::new(&mut *stream).poll_close(cx)).await;
        };
        let _ = timeout(&rt, self.shared.config.shutdown_timeout, work).await;
    }
}
