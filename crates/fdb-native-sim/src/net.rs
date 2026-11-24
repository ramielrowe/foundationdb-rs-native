//! The simulated network: TCP-like connections between nodes, with latency, and faults.
//!
//! A connection is two one-way pipes. A write puts a *segment* in the writer's pipe with an
//! arrival time (now + a latency drawn from [`NetConfig`], never before the segment ahead of
//! it, so bytes arrive in order); the reader sees the bytes once the clock reaches it. A
//! reader waiting for a segment sets a timer for its arrival, which is what moves the
//! virtual clock. Everything (latencies, short reads, partial writes, which faults strike)
//! is drawn from the simulation's seeded generator.
//!
//! # Faults
//!
//! | Fault | What the two ends see |
//! | --- | --- |
//! | latency spikes | a segment arrives late (`slow_chance`, `slow_latency`) |
//! | [clogging](Sim::clog_ip) | segments sent through a clogged address or pair arrive no earlier than the end of the clog |
//! | [partitions](Sim::partition) | segments between the two sides are held (TCP retransmits) until the partition heals, and connects hang; nothing is closed: only a ping timeout can tell |
//! | [connection reset](Sim::reset_connection) | each direction keeps a random prefix of what is still in flight (so it is usually cut mid-frame), then the reader gets `ECONNRESET`; writes fail from now on |
//! | write-time reset (`write_reset_chance`) | the same, striking during a write (the write's own fate depends on the trust setting, below) |
//! | [half-open connection](Sim::half_open_connection) | the server end vanishes silently: the client's writes are black-holed (until the send buffer fills) and it never reads anything again, nor EOF |
//! | [crash](Sim::kill_node) (`KillMode::Reboot`) | the node's tasks are aborted; its peers get a FIN or an RST; connecting to it is refused until it listens again |
//! | [power off](Sim::kill_node) (`KillMode::PowerOff`) | as a crash, but its peers get nothing (every connection to it is half-open) and connecting to it hangs |
//! | partial writes, short reads | `poll_write` takes a prefix, `poll_read` returns fewer bytes than are available |
//! | would-block | `poll_write` returns `Pending` and wakes itself a moment later; a full pipe (`pipe_capacity` unread bytes) is `Pending` until the reader reads |
//! | refused connects | `connect_fail_chance` |
//!
//! # Writes and at-most-once
//!
//! The streams the client's [`SimRuntime`](crate::SimRuntime) dials honour the
//! [`Runtime`](fdb_native_transport::runtime::Runtime) write contract exactly as its const
//! parameters declare it:
//!
//! * `Ok(n)` always means the first `n` bytes were taken (and will arrive unless the
//!   connection breaks first).
//! * With `WRITE_ERRORS_TAKE_NOTHING = true` an error (and `Ok(0)`, which the stream never
//!   returns for a non-empty buffer) takes nothing: a write-time reset either takes a prefix
//!   and reports it with `Ok(k)` (the reset then strikes the next write) or takes nothing and
//!   fails. With `false` the stream does what that setting allows: a failing write may first
//!   put any prefix of the buffer on the wire, so the peer can receive whole frames of a
//!   write that reported an error.
//! * With `PENDING_TAKES_NOTHING = true` a `Pending` write took nothing. With `false` a
//!   pending write may put a prefix of the buffer on the wire and remember it, as an
//!   OpenSSL `SSL_write` without partial writes does: the next write must offer a buffer
//!   that starts with those bytes (a different one is recorded as a contract violation in
//!   [`Sim::net_violations`]) and is answered with their count. If the connection ends
//!   first, those bytes were delivered although no write reported them.
//!
//! The streams of the server side (accepted connections) never use those freedoms; they do
//! partial writes and short reads only.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use fdb_native_transport::runtime::Instant;
use futures_io::{AsyncRead, AsyncWrite};

use crate::executor::{NodeId, Shared, Sim, TimerKey, lock};
use crate::rng::SimRng;
use crate::trace::TraceLevel;

/// How the network behaves. [`NetConfig::default`] is a fault-free LAN;
/// [`NetConfig::randomized`] draws a faulty one from a seed.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct NetConfig {
    /// The one-way latency every segment has at least.
    pub latency: Duration,
    /// A uniform extra latency in `0..=jitter`.
    pub jitter: Duration,
    /// How often a segment is slow, and how slow (uniform in the range).
    pub slow_chance: f64,
    /// See [`slow_chance`](Self::slow_chance).
    pub slow_latency: (Duration, Duration),
    /// How often a write takes only a prefix of its buffer.
    pub partial_write_chance: f64,
    /// How often a read returns fewer bytes than are available.
    pub short_read_chance: f64,
    /// How often a client write would block (returns `Pending`) without the pipe being full.
    pub pending_write_chance: f64,
    /// How often a client write resets its connection (mid-frame, usually).
    pub write_reset_chance: f64,
    /// When a client stream that does not promise it may take bytes on an error or a
    /// `Pending`, how often it does.
    pub untrusted_take_chance: f64,
    /// How often a connect is refused although something listens.
    pub connect_fail_chance: f64,
    /// How many unread bytes a pipe holds before writes are `Pending`.
    pub pipe_capacity: usize,
}

impl Default for NetConfig {
    fn default() -> NetConfig {
        NetConfig {
            latency: Duration::from_micros(50),
            jitter: Duration::from_micros(200),
            slow_chance: 0.0,
            slow_latency: (Duration::from_millis(5), Duration::from_millis(50)),
            partial_write_chance: 0.0,
            short_read_chance: 0.0,
            pending_write_chance: 0.0,
            write_reset_chance: 0.0,
            untrusted_take_chance: 0.0,
            connect_fail_chance: 0.0,
            pipe_capacity: 1 << 20,
        }
    }
}

impl NetConfig {
    /// A faulty network drawn from `rng`: every fault probability is either off or somewhere
    /// in its range, so different seeds stress different things.
    pub fn randomized(rng: &mut SimRng) -> NetConfig {
        let mut maybe = |p: f64, lo: f64, hi: f64| {
            if rng.chance(p) {
                lo + (hi - lo) * rng.unit()
            } else {
                0.0
            }
        };
        let slow_chance = maybe(0.6, 0.001, 0.05);
        let partial_write_chance = maybe(0.6, 0.05, 0.5);
        let short_read_chance = maybe(0.6, 0.05, 0.5);
        let pending_write_chance = maybe(0.5, 0.01, 0.2);
        let write_reset_chance = maybe(0.5, 0.002, 0.03);
        let untrusted_take_chance = maybe(0.9, 0.3, 1.0);
        let connect_fail_chance = maybe(0.3, 0.01, 0.2);
        let latency = rng.duration(Duration::from_micros(10), Duration::from_micros(500));
        let jitter = rng.duration(Duration::ZERO, Duration::from_millis(2));
        let pipe_capacity = *rng.pick(&[4 << 10, 64 << 10, 1 << 20]);
        NetConfig {
            latency,
            jitter,
            slow_chance,
            slow_latency: (Duration::from_millis(2), Duration::from_millis(200)),
            partial_write_chance,
            short_read_chance,
            pending_write_chance,
            write_reset_chance,
            untrusted_take_chance,
            connect_fail_chance,
            pipe_capacity,
        }
    }
}

/// What a stream promises about writes that report no count (see the [module
/// documentation](self#writes-and-at-most-once)).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Trust {
    pub(crate) errors_take_nothing: bool,
    pub(crate) pending_takes_nothing: bool,
}

impl Trust {
    const EXACT: Trust = Trust {
        errors_take_nothing: true,
        pending_takes_nothing: true,
    };
}

/// How a node dies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KillMode {
    /// The process crashes; its operating system closes its sockets (each peer gets a FIN or
    /// an RST) and refuses connections until something listens again.
    Reboot,
    /// The machine loses power: its peers hear nothing (their connections become half-open)
    /// and connects to it hang until it is back.
    PowerOff,
}

/// Counters of the faults the network injected.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NetStats {
    /// Connections established.
    pub connects: u64,
    /// Connects refused.
    pub refused: u64,
    /// Writes that took a prefix only.
    pub partial_writes: u64,
    /// Writes that returned `Pending`.
    pub pending_writes: u64,
    /// Connections reset (by a fault or a write-time reset).
    pub resets: u64,
    /// Failed or pending writes that took bytes without reporting them (untrusted streams).
    pub untrusted_takes: u64,
    /// Bytes written.
    pub bytes: u64,
}

struct Seg {
    data: Vec<u8>,
    pos: usize,
    at: Instant,
    held: bool,
}

#[derive(Default)]
struct Pipe {
    segs: VecDeque<Seg>,
    last_at: Instant,
    fin: Option<Instant>,
    reset: Option<Instant>,
    /// The receiving machine is gone without a word: what is written is lost (but fills the
    /// send buffer).
    blackhole: bool,
    reader: Option<Waker>,
    writer: Option<Waker>,
    reader_timer: Option<TimerKey>,
    unread: usize,
}

impl Pipe {
    fn wake_reader(&mut self) {
        if let Some(w) = self.reader.take() {
            w.wake();
        }
    }

    fn wake_writer(&mut self) {
        if let Some(w) = self.writer.take() {
            w.wake();
        }
    }
}

struct End {
    addr: SocketAddr,
    node: NodeId,
    /// The stream was dropped or its node killed.
    gone: bool,
    /// `poll_close` was called.
    write_shut: bool,
    /// Writes fail with this from now on (the connection was reset).
    write_error: Option<io::ErrorKind>,
    /// Bytes an untrusted pending write put on the wire without reporting them.
    stash: Vec<u8>,
}

struct Conn {
    ends: [End; 2],
    /// `pipes[i]` is written by end `i` and read by end `1 - i`.
    pipes: [Pipe; 2],
    trust: Trust,
}

struct Listener {
    id: u64,
    node: NodeId,
    queue: VecDeque<u64>,
    waker: Option<Waker>,
}

/// The network's state, inside the [`Sim`].
pub(crate) struct NetState {
    config: NetConfig,
    rng: SimRng,
    listeners: BTreeMap<SocketAddr, Listener>,
    conns: BTreeMap<u64, Conn>,
    next_id: u64,
    ip_clog: BTreeMap<IpAddr, Instant>,
    pair_clog: BTreeMap<(IpAddr, IpAddr), Instant>,
    partitions: BTreeSet<(IpAddr, IpAddr)>,
    down: BTreeSet<IpAddr>,
    hosts: BTreeMap<String, Vec<IpAddr>>,
    stats: NetStats,
    violations: Vec<String>,
}

fn pair(a: IpAddr, b: IpAddr) -> (IpAddr, IpAddr) {
    if a <= b { (a, b) } else { (b, a) }
}

impl NetState {
    pub(crate) fn new(config: NetConfig, rng: SimRng) -> NetState {
        NetState {
            config,
            rng,
            listeners: BTreeMap::new(),
            conns: BTreeMap::new(),
            next_id: 0,
            ip_clog: BTreeMap::new(),
            pair_clog: BTreeMap::new(),
            partitions: BTreeSet::new(),
            down: BTreeSet::new(),
            hosts: BTreeMap::new(),
            stats: NetStats::default(),
            violations: Vec::new(),
        }
    }

    fn latency(&mut self) -> Duration {
        let c = &self.config;
        let mut d = c.latency + self.rng.duration(Duration::ZERO, c.jitter);
        let (slow, range) = (c.slow_chance, c.slow_latency);
        if self.rng.chance(slow) {
            d += self.rng.duration(range.0, range.1);
        }
        d
    }

    fn partitioned(&self, a: IpAddr, b: IpAddr) -> bool {
        self.partitions.contains(&pair(a, b))
    }

    /// When a segment from `src` to `dst` sent now can arrive at the earliest.
    fn arrival(&mut self, now: Instant, src: IpAddr, dst: IpAddr) -> Instant {
        let mut at = now + self.latency();
        for ip in [src, dst] {
            if let Some(until) = self.ip_clog.get(&ip) {
                at = at.max(*until);
            }
        }
        if let Some(until) = self.pair_clog.get(&(src, dst)) {
            at = at.max(*until);
        }
        at
    }

    /// Puts `data` on the wire of `conn` from end `side`.
    fn push(&mut self, now: Instant, id: u64, side: usize, data: &[u8]) {
        let (src, dst) = {
            let conn = &self.conns[&id];
            (conn.ends[side].addr.ip(), conn.ends[1 - side].addr.ip())
        };
        let held = self.partitioned(src, dst);
        let arrival = self.arrival(now, src, dst);
        self.stats.bytes += data.len() as u64;
        let conn = self.conns.get_mut(&id).expect("live connection");
        let receiver_gone = conn.ends[1 - side].gone;
        let pipe = &mut conn.pipes[side];
        if pipe.blackhole {
            pipe.unread += data.len();
            return;
        }
        if receiver_gone {
            // Data for a closed socket: the receiving kernel answers with an RST.
            let rst_at = arrival + (arrival - now);
            let back = &mut conn.pipes[1 - side];
            if back.reset.is_none() {
                back.reset = Some(rst_at);
                back.wake_reader();
            }
            conn.ends[side].write_error = Some(io::ErrorKind::BrokenPipe);
            return;
        }
        let held = held || pipe.segs.back().is_some_and(|s| s.held);
        let at = arrival.max(pipe.last_at);
        pipe.last_at = at;
        pipe.unread += data.len();
        pipe.segs.push_back(Seg {
            data: data.to_vec(),
            pos: 0,
            at,
            held,
        });
        pipe.wake_reader();
    }

    /// Resets `conn`: each direction keeps a random prefix of what has not arrived yet, then
    /// its reader gets `ECONNRESET`; both ends' writes fail.
    ///
    /// With `keep = Some(side)`, what end `side` sent is all delivered before the RST (a
    /// write that put bytes on the wire and then failed: they did leave).
    fn reset(&mut self, now: Instant, id: u64, keep: Option<usize>) {
        let lat = [self.latency(), self.latency()];
        let mut cuts = [0u64; 2];
        for (d, cut) in cuts.iter_mut().enumerate() {
            let undelivered: usize = self.conns[&id].pipes[d]
                .segs
                .iter()
                .filter(|s| s.held || s.at > now)
                .map(|s| s.data.len() - s.pos)
                .sum();
            *cut = if keep == Some(d) {
                undelivered as u64
            } else {
                self.rng.below(undelivered as u64 + 1)
            };
        }
        self.stats.resets += 1;
        let conn = self.conns.get_mut(&id).expect("live connection");
        for d in 0..2 {
            let pipe = &mut conn.pipes[d];
            let mut keep = cuts[d] as usize;
            let mut kept = VecDeque::new();
            for mut seg in pipe.segs.drain(..) {
                if !seg.held && seg.at <= now {
                    kept.push_back(seg);
                    continue;
                }
                if keep == 0 || seg.held {
                    continue;
                }
                let len = seg.data.len() - seg.pos;
                if len > keep {
                    seg.data.truncate(seg.pos + keep);
                    keep = 0;
                } else {
                    keep -= len;
                }
                kept.push_back(seg);
            }
            pipe.unread = kept.iter().map(|s| s.data.len() - s.pos).sum();
            let last = kept.back().map_or(now, |s| s.at);
            pipe.segs = kept;
            pipe.fin = None;
            pipe.reset = Some(last.max(now + lat[d]));
            pipe.wake_reader();
            pipe.wake_writer();
        }
        for end in &mut conn.ends {
            end.write_error = Some(io::ErrorKind::ConnectionReset);
            end.stash.clear();
        }
    }

    /// End `side` of `conn` vanishes: its node is dead (`silent`: without a word) or its
    /// stream was dropped (a FIN).
    fn end_gone(&mut self, now: Instant, id: u64, side: usize, how: Gone) {
        let lat = self.latency();
        let Some(conn) = self.conns.get_mut(&id) else {
            return;
        };
        let end = &mut conn.ends[side];
        if end.gone {
            return;
        }
        end.gone = true;
        end.stash.clear();
        match how {
            Gone::Fin => {
                let pipe = &mut conn.pipes[side];
                if pipe.fin.is_none() && pipe.reset.is_none() && !end.write_shut {
                    pipe.fin = Some(pipe.last_at.max(now + lat));
                }
                pipe.wake_reader();
            }
            Gone::Rst => {
                let pipe = &mut conn.pipes[side];
                if pipe.reset.is_none() {
                    pipe.reset = Some(pipe.last_at.max(now + lat));
                }
                pipe.wake_reader();
                conn.ends[1 - side].write_error = Some(io::ErrorKind::ConnectionReset);
                conn.pipes[1 - side].wake_writer();
            }
            Gone::Silent => {
                // Nothing more comes from this end; what is sent to it is lost.
                let to_dead = &mut conn.pipes[1 - side];
                to_dead.blackhole = true;
                to_dead.segs.clear();
                to_dead.unread = 0;
                to_dead.wake_writer();
                let from_dead = &mut conn.pipes[side];
                from_dead.segs.retain(|s| !s.held && s.at <= now);
            }
        }
        // Its own reader, if a task of it still reads, sees the end.
        conn.pipes[1 - side].wake_reader();
        if conn.ends.iter().all(|e| e.gone) {
            for pipe in &mut conn.pipes {
                pipe.wake_reader();
                pipe.wake_writer();
            }
            self.conns.remove(&id);
        }
    }

    /// Releases held segments whose path is no longer partitioned.
    fn unhold(&mut self, now: Instant) {
        let ids: Vec<u64> = self.conns.keys().copied().collect();
        for id in ids {
            for side in 0..2 {
                let (src, dst) = {
                    let c = &self.conns[&id];
                    (c.ends[side].addr.ip(), c.ends[1 - side].addr.ip())
                };
                if self.partitioned(src, dst) {
                    continue;
                }
                let held = self.conns[&id].pipes[side].segs.iter().any(|s| s.held);
                if !held {
                    continue;
                }
                let mut last = self.conns[&id].pipes[side].last_at;
                let n = self.conns[&id].pipes[side].segs.len();
                for i in 0..n {
                    if !self.conns[&id].pipes[side].segs[i].held {
                        continue;
                    }
                    let at = self.arrival(now, src, dst).max(last);
                    last = at;
                    let seg = &mut self.conns.get_mut(&id).expect("live").pipes[side].segs[i];
                    seg.held = false;
                    seg.at = at;
                }
                let pipe = &mut self.conns.get_mut(&id).expect("live").pipes[side];
                pipe.last_at = last;
                pipe.wake_reader();
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Gone {
    Fin,
    Rst,
    Silent,
}

/// One end of a simulated connection: a [`futures_io`] byte stream.
pub struct SimStream {
    shared: Arc<Shared>,
    id: u64,
    side: usize,
}

impl fmt::Debug for SimStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimStream")
            .field("conn", &self.id)
            .field("side", &self.side)
            .finish()
    }
}

impl SimStream {
    /// The connection's id (also in the trace).
    pub fn connection_id(&self) -> u64 {
        self.id
    }

    /// The remote address.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        let net = lock(&self.shared.net);
        net.conns.get(&self.id).map(|c| c.ends[1 - self.side].addr)
    }
}

impl Drop for SimStream {
    fn drop(&mut self) {
        let now = self.shared.now();
        let mut net = lock(&self.shared.net);
        let key = net
            .conns
            .get_mut(&self.id)
            .and_then(|c| c.pipes[1 - self.side].reader_timer.take());
        net.end_gone(now, self.id, self.side, Gone::Fin);
        drop(net);
        if let Some(key) = key {
            self.shared.cancel_timer(key);
        }
    }
}

impl AsyncRead for SimStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let now = self.shared.now();
        let mut net = lock(&self.shared.net);
        let short = {
            let chance = net.config.short_read_chance;
            net.rng.chance(chance)
        };
        let short_len = net.rng.next_u64();
        let Some(conn) = net.conns.get_mut(&self.id) else {
            return Poll::Ready(Err(io::ErrorKind::NotConnected.into()));
        };
        if conn.ends[self.side].gone {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        let pipe = &mut conn.pipes[1 - self.side];
        if let Some(front) = pipe.segs.front_mut() {
            if !front.held && front.at <= now {
                let avail = front.data.len() - front.pos;
                let mut n = avail.min(buf.len());
                if short && n > 1 {
                    n = 1 + (short_len % n as u64) as usize;
                }
                buf[..n].copy_from_slice(&front.data[front.pos..front.pos + n]);
                front.pos += n;
                if front.pos == front.data.len() {
                    pipe.segs.pop_front();
                }
                pipe.unread -= n;
                pipe.wake_writer();
                let (id, side) = (self.id, self.side);
                drop(net);
                self.shared.trace(
                    TraceLevel::Verbose,
                    "read",
                    &[id, side as u64, n as u64],
                    || format!("conn {id} side {side}: {n} bytes"),
                );
                return Poll::Ready(Ok(n));
            }
        }
        if pipe.reset.is_some_and(|r| r <= now) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
        }
        if pipe.segs.is_empty() && pipe.fin.is_some_and(|f| f <= now) {
            return Poll::Ready(Ok(0));
        }
        pipe.reader = Some(cx.waker().clone());
        let next = [
            pipe.segs.front().filter(|s| !s.held).map(|s| s.at),
            pipe.reset,
            pipe.fin.filter(|_| pipe.segs.is_empty()),
        ]
        .into_iter()
        .flatten()
        .min();
        let old = pipe.reader_timer.take();
        drop(net);
        if let Some(key) = old {
            self.shared.cancel_timer(key);
        }
        if let Some(at) = next {
            let key = self.shared.add_timer(at, cx.waker().clone());
            let mut net = lock(&self.shared.net);
            if let Some(conn) = net.conns.get_mut(&self.id) {
                conn.pipes[1 - self.side].reader_timer = Some(key);
            } else {
                drop(net);
                self.shared.cancel_timer(key);
            }
        }
        Poll::Pending
    }
}

impl AsyncWrite for SimStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let now = self.shared.now();
        let (id, side) = (self.id, self.side);
        let mut net = lock(&self.shared.net);
        let Some(conn) = net.conns.get_mut(&id) else {
            return Poll::Ready(Err(io::ErrorKind::NotConnected.into()));
        };
        let trust = if side == 0 { conn.trust } else { Trust::EXACT };
        let end = &mut conn.ends[side];
        if end.gone {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        if end.write_shut {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if !end.stash.is_empty() {
            // The retry of a pending write that already put these bytes on the wire.
            let stash = std::mem::take(&mut end.stash);
            if buf.len() < stash.len() || buf[..stash.len()] != stash[..] {
                net.violations.push(format!(
                    "conn {id}: after a pending write that took {} bytes, the next write \
                     offered a different buffer ({} bytes)",
                    stash.len(),
                    buf.len()
                ));
            }
            return Poll::Ready(Ok(stash.len().min(buf.len())));
        }
        if let Some(kind) = end.write_error {
            return Poll::Ready(Err(kind.into()));
        }
        let config = net.config.clone();
        // A reset that strikes during this write (client streams only).
        if side == 0 && net.rng.chance(config.write_reset_chance) {
            let take_unreported =
                !trust.errors_take_nothing && net.rng.chance(config.untrusted_take_chance);
            // An untrusted stream often sent all it was offered (whole frames) before failing.
            let k = if take_unreported && net.rng.chance(0.5) {
                buf.len()
            } else {
                net.rng.range(0, buf.len() as u64) as usize
            };
            let report = !take_unreported && k > 0 && net.rng.chance(0.5);
            let taken = if take_unreported || report { k } else { 0 };
            if taken > 0 {
                net.push(now, id, side, &buf[..taken]);
            }
            if take_unreported && taken > 0 {
                net.stats.untrusted_takes += 1;
            }
            net.reset(now, id, (taken > 0).then_some(side));
            drop(net);
            self.shared.trace(
                TraceLevel::Events,
                "write-reset",
                &[id, taken as u64],
                || {
                    format!(
                        "conn {id}: reset during a write of {} bytes ({taken} put on the wire, {})",
                        buf.len(),
                        if report { "reported" } else { "error returned" }
                    )
                },
            );
            return Poll::Ready(if report {
                Ok(taken)
            } else {
                Err(io::ErrorKind::ConnectionReset.into())
            });
        }
        let full = net.conns[&id].pipes[side].unread >= config.pipe_capacity;
        let spurious = !full && side == 0 && net.rng.chance(config.pending_write_chance);
        if full || spurious {
            net.stats.pending_writes += 1;
            if side == 0
                && !trust.pending_takes_nothing
                && net.rng.chance(config.untrusted_take_chance)
            {
                let k = net.rng.range(1, buf.len() as u64) as usize;
                net.push(now, id, side, &buf[..k]);
                net.stats.untrusted_takes += 1;
                net.conns.get_mut(&id).expect("live").ends[side].stash = buf[..k].to_vec();
            }
            let wake_in = net.rng.duration(Duration::ZERO, Duration::from_millis(2));
            if let Some(conn) = net.conns.get_mut(&id) {
                conn.pipes[side].writer = Some(cx.waker().clone());
            }
            drop(net);
            if spurious {
                // A socket that would block becomes writable again shortly (the timer is
                // not cancelled: a spurious wake is harmless).
                self.shared.add_timer(now + wake_in, cx.waker().clone());
            }
            return Poll::Pending;
        }
        let room = config
            .pipe_capacity
            .saturating_sub(net.conns[&id].pipes[side].unread)
            .max(1);
        let mut k = buf.len().min(room);
        if k > 1 && net.rng.chance(config.partial_write_chance) {
            k = net.rng.range(1, k as u64 - 1) as usize;
            net.stats.partial_writes += 1;
        }
        net.push(now, id, side, &buf[..k]);
        drop(net);
        self.shared.trace(
            TraceLevel::Verbose,
            "write",
            &[id, side as u64, k as u64],
            || format!("conn {id} side {side}: {k} of {} bytes", buf.len()),
        );
        Poll::Ready(Ok(k))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let now = self.shared.now();
        let mut net = lock(&self.shared.net);
        let lat = net.latency();
        let Some(conn) = net.conns.get_mut(&self.id) else {
            return Poll::Ready(Ok(()));
        };
        let end = &mut conn.ends[self.side];
        end.stash.clear();
        if end.write_shut || end.gone {
            return Poll::Ready(Ok(()));
        }
        end.write_shut = true;
        let pipe = &mut conn.pipes[self.side];
        if pipe.fin.is_none() && pipe.reset.is_none() {
            pipe.fin = Some(pipe.last_at.max(now + lat));
        }
        pipe.wake_reader();
        Poll::Ready(Ok(()))
    }
}

/// A listening address of a node: [`accept`](SimListener::accept) yields the server ends of
/// the connections made to it. Dropping it stops listening.
pub struct SimListener {
    shared: Arc<Shared>,
    addr: SocketAddr,
    id: u64,
}

impl fmt::Debug for SimListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimListener")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl SimListener {
    /// The next connection, or `None` once the listener was closed (its node killed, or
    /// another listener took the address).
    pub fn accept(&self) -> impl std::future::Future<Output = Option<SimStream>> + Send + '_ {
        std::future::poll_fn(move |cx| {
            let mut net = lock(&self.shared.net);
            let Some(listener) = net.listeners.get_mut(&self.addr) else {
                return Poll::Ready(None);
            };
            if listener.id != self.id {
                return Poll::Ready(None);
            }
            match listener.queue.pop_front() {
                Some(id) => Poll::Ready(Some(SimStream {
                    shared: self.shared.clone(),
                    id,
                    side: 1,
                })),
                None => {
                    listener.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }
}

impl Drop for SimListener {
    fn drop(&mut self) {
        let now = self.shared.now();
        let mut net = lock(&self.shared.net);
        if net
            .listeners
            .get(&self.addr)
            .is_some_and(|l| l.id == self.id)
        {
            let listener = net.listeners.remove(&self.addr).expect("present");
            for id in listener.queue {
                net.end_gone(now, id, 1, Gone::Rst);
            }
        }
    }
}

/// Dials `addr` from `node`. Used by [`SimRuntime::connect`](crate::SimRuntime).
pub(crate) async fn connect(
    shared: Arc<Shared>,
    node: NodeId,
    addr: SocketAddr,
    trust: Trust,
) -> io::Result<SimStream> {
    let sim = Sim {
        shared: shared.clone(),
    };
    let src_ip = shared.node_ip(node);
    loop {
        let syn = lock(&shared.net).latency();
        sim.sleep(syn).await;
        enum Decision {
            Blocked,
            Refused,
            Connected(SimStream),
        }
        let decision = {
            let mut net = lock(&shared.net);
            let blocked = net.partitioned(src_ip, addr.ip()) || net.down.contains(&addr.ip());
            let fail_chance = net.config.connect_fail_chance;
            if blocked {
                Decision::Blocked
            } else if net.rng.chance(fail_chance) || !net.listeners.contains_key(&addr) {
                net.stats.refused += 1;
                Decision::Refused
            } else {
                net.next_id += 1;
                let id = net.next_id;
                let port = shared.ephemeral_port(node);
                let server_node = net.listeners[&addr].node;
                net.conns.insert(
                    id,
                    Conn {
                        ends: [
                            End {
                                addr: SocketAddr::new(src_ip, port),
                                node,
                                gone: false,
                                write_shut: false,
                                write_error: None,
                                stash: Vec::new(),
                            },
                            End {
                                addr,
                                node: server_node,
                                gone: false,
                                write_shut: false,
                                write_error: None,
                                stash: Vec::new(),
                            },
                        ],
                        pipes: [Pipe::default(), Pipe::default()],
                        trust,
                    },
                );
                net.stats.connects += 1;
                let listener = net.listeners.get_mut(&addr).expect("checked");
                listener.queue.push_back(id);
                if let Some(w) = listener.waker.take() {
                    w.wake();
                }
                Decision::Connected(SimStream {
                    shared: shared.clone(),
                    id,
                    side: 0,
                })
            }
        };
        match decision {
            Decision::Blocked => {
                // SYN retransmits until the path is back (or the caller gives up).
                sim.sleep(Duration::from_millis(200)).await;
            }
            Decision::Refused => {
                shared.trace(TraceLevel::Events, "refused", &[], || {
                    format!("{} -> {addr}", shared.node_name(node))
                });
                let back = lock(&shared.net).latency();
                sim.sleep(back).await;
                return Err(io::ErrorKind::ConnectionRefused.into());
            }
            Decision::Connected(stream) => {
                let id = stream.id;
                shared.trace(TraceLevel::Events, "connect", &[id], || {
                    format!("conn {id}: {} -> {addr}", shared.node_name(node))
                });
                let back = lock(&shared.net).latency();
                sim.sleep(back).await;
                return Ok(stream);
            }
        }
    }
}

/// Resolves `host` (an IP literal, or a name added with [`Sim::set_host`]).
pub(crate) fn resolve(shared: &Shared, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    match lock(&shared.net).hosts.get(host) {
        Some(ips) => Ok(ips.iter().map(|ip| SocketAddr::new(*ip, port)).collect()),
        None => Err(io::Error::new(io::ErrorKind::NotFound, "unknown host")),
    }
}

/// An established connection, as [`Sim::connections`] lists it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionInfo {
    /// Its id.
    pub id: u64,
    /// The dialing end.
    pub client: SocketAddr,
    /// The accepting end (the listening address).
    pub server: SocketAddr,
    /// The dialing node.
    pub client_node: NodeId,
    /// The accepting node.
    pub server_node: NodeId,
}

impl Sim {
    /// Starts listening at `addr` on `node`. A listener already there is replaced.
    pub fn listen(&self, node: NodeId, addr: SocketAddr) -> SimListener {
        let mut net = lock(&self.shared.net);
        net.next_id += 1;
        let id = net.next_id;
        net.listeners.insert(
            addr,
            Listener {
                id,
                node,
                queue: VecDeque::new(),
                waker: None,
            },
        );
        SimListener {
            shared: self.shared.clone(),
            addr,
            id,
        }
    }

    /// Makes `host` resolve to `ips`.
    pub fn set_host(&self, host: &str, ips: Vec<IpAddr>) {
        lock(&self.shared.net).hosts.insert(host.to_owned(), ips);
    }

    /// The established connections that are still open at both ends, in id order.
    pub fn connections(&self) -> Vec<ConnectionInfo> {
        let net = lock(&self.shared.net);
        net.conns
            .iter()
            .filter(|(_, c)| {
                c.ends.iter().all(|e| !e.gone && e.write_error.is_none())
                    && c.pipes.iter().all(|p| !p.blackhole && p.reset.is_none())
            })
            .map(|(id, c)| ConnectionInfo {
                id: *id,
                client: c.ends[0].addr,
                server: c.ends[1].addr,
                client_node: c.ends[0].node,
                server_node: c.ends[1].node,
            })
            .collect()
    }

    /// Resets connection `id` (see the [module documentation](crate::net)).
    pub fn reset_connection(&self, id: u64) {
        let now = self.now();
        let mut net = lock(&self.shared.net);
        if net.conns.contains_key(&id) {
            net.reset(now, id, None);
            drop(net);
            self.trace(TraceLevel::Events, "reset", &[id], || format!("conn {id}"));
        }
    }

    /// Makes connection `id` half-open: its accepting end vanishes without a word.
    pub fn half_open_connection(&self, id: u64) {
        let now = self.now();
        let mut net = lock(&self.shared.net);
        if net.conns.contains_key(&id) {
            net.end_gone(now, id, 1, Gone::Silent);
            drop(net);
            self.trace(TraceLevel::Events, "half-open", &[id], || {
                format!("conn {id}")
            });
        }
    }

    /// Kills `node`: aborts its tasks, closes its listeners and ends its connections as
    /// `mode` says.
    pub fn kill_node(&self, node: NodeId, mode: KillMode) {
        let now = self.now();
        let ip = self.node_ip(node);
        {
            let mut net = lock(&self.shared.net);
            let addrs: Vec<SocketAddr> = net
                .listeners
                .iter()
                .filter(|(_, l)| l.node == node)
                .map(|(a, _)| *a)
                .collect();
            for addr in addrs {
                let listener = net.listeners.remove(&addr).expect("present");
                if let Some(w) = listener.waker {
                    w.wake();
                }
                for id in listener.queue {
                    net.end_gone(now, id, 1, Gone::Rst);
                }
            }
            let ends: Vec<(u64, usize)> = net
                .conns
                .iter()
                .flat_map(|(id, c)| {
                    c.ends
                        .iter()
                        .enumerate()
                        .filter(|(_, e)| e.node == node && !e.gone)
                        .map(move |(side, _)| (*id, side))
                })
                .collect();
            for (id, side) in ends {
                let how = match mode {
                    KillMode::PowerOff => Gone::Silent,
                    KillMode::Reboot if net.rng.chance(0.5) => Gone::Rst,
                    KillMode::Reboot => Gone::Fin,
                };
                net.end_gone(now, id, side, how);
            }
            if mode == KillMode::PowerOff {
                net.down.insert(ip);
            }
        }
        self.abort_node_tasks(node);
        self.trace(TraceLevel::Events, "kill", &[u64::from(node.0)], || {
            format!("{} ({mode:?})", self.node_name(node))
        });
    }

    /// Powers `node`'s machine back on (connects to it are refused until something
    /// listens, instead of hanging).
    pub fn power_on(&self, node: NodeId) {
        let ip = self.node_ip(node);
        lock(&self.shared.net).down.remove(&ip);
    }

    /// Clogs `ip` until `duration` from now: segments to or from it sent until then arrive
    /// no earlier than that.
    pub fn clog_ip(&self, ip: IpAddr, duration: Duration) {
        let until = self.now() + duration;
        let mut net = lock(&self.shared.net);
        let e = net.ip_clog.entry(ip).or_insert(until);
        *e = (*e).max(until);
        drop(net);
        self.trace(
            TraceLevel::Events,
            "clog",
            &[duration.as_micros() as u64],
            || format!("{ip} for {duration:?}"),
        );
    }

    /// Clogs the path from `from` to `to` (one way) until `duration` from now.
    pub fn clog_pair(&self, from: IpAddr, to: IpAddr, duration: Duration) {
        let until = self.now() + duration;
        let mut net = lock(&self.shared.net);
        let e = net.pair_clog.entry((from, to)).or_insert(until);
        *e = (*e).max(until);
        drop(net);
        self.trace(
            TraceLevel::Events,
            "clog-pair",
            &[duration.as_micros() as u64],
            || format!("{from} -> {to} for {duration:?}"),
        );
    }

    /// Cuts every path between an address of `a` and one of `b` until
    /// [`heal`](Self::heal_partition).
    pub fn partition(&self, a: &[IpAddr], b: &[IpAddr]) {
        let mut net = lock(&self.shared.net);
        for x in a {
            for y in b {
                if x != y {
                    net.partitions.insert(pair(*x, *y));
                }
            }
        }
        drop(net);
        self.trace(
            TraceLevel::Events,
            "partition",
            &[a.len() as u64, b.len() as u64],
            || format!("{a:?} | {b:?}"),
        );
    }

    /// Heals the partition between `a` and `b`: held segments are sent on.
    pub fn heal_partition(&self, a: &[IpAddr], b: &[IpAddr]) {
        let now = self.now();
        let mut net = lock(&self.shared.net);
        for x in a {
            for y in b {
                net.partitions.remove(&pair(*x, *y));
            }
        }
        net.unhold(now);
        drop(net);
        self.trace(TraceLevel::Events, "heal", &[], || format!("{a:?} | {b:?}"));
    }

    /// Ends every clog and partition.
    pub fn heal_network(&self) {
        let now = self.now();
        let mut net = lock(&self.shared.net);
        net.ip_clog.clear();
        net.pair_clog.clear();
        net.partitions.clear();
        net.unhold(now);
        drop(net);
        self.trace(TraceLevel::Events, "heal-all", &[], String::new);
    }

    /// Changes how the network behaves from now on.
    pub fn set_net_config(&self, config: NetConfig) {
        lock(&self.shared.net).config = config;
    }

    /// How the network behaves.
    pub fn net_config(&self) -> NetConfig {
        lock(&self.shared.net).config.clone()
    }

    /// The fault counters.
    pub fn net_stats(&self) -> NetStats {
        lock(&self.shared.net).stats
    }

    /// Breaches of the stream contract by the code under test (a write after an untrusted
    /// pending write that did not offer the same bytes again).
    pub fn net_violations(&self) -> Vec<String> {
        lock(&self.shared.net).violations.clone()
    }
}
