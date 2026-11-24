//! Unwritten requests are `NotSent` through the peer manager (as8.8): a socket that accepts
//! N bytes and then fails, a runtime that does not promise that failed writes take nothing,
//! and TLS-like streams that take bytes into their own buffer without sending them (the bytes
//! a TLS writer took count as possibly delivered, and so does what it was offered when it
//! failed, or when it was pending on a stream that does not promise a pending write took
//! nothing).

#![cfg(feature = "tokio")]

mod common;

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use common::*;
use fdb_native_transport::peer::{
    PeerHandle, PeerManager, PeerManagerConfig, StreamWrapper, event_channel,
};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::{Instant, Runtime, TokioTask};
use fdb_native_transport::{Checksum, ConnectPacket, FrameConfig, MonitorConfig};
use fdb_native_wire::{EncodeError, NetworkAddress, ProtocolVersion, Uid};
use futures_io::{AsyncRead, AsyncWrite};
use futures_lite::future::poll_once;

const V74: ProtocolVersion = ProtocolVersion::V7_4;

/// What the peer manager offers at most to one write on a stream it does not fully trust.
const UNSURE_WRITE_MAX: usize = 64 * 1024;

fn endpoint() -> Uid {
    Uid::new(0x5eed_0000_0000_0000 | 1, 1)
}

/// What a [`Faulty`] stream does with writes once its plan's pass-through bytes are written.
#[derive(Clone, Copy, Debug)]
enum Then {
    /// Every write fails, taking nothing.
    Fail,
    /// Writes are taken into a buffer that is never sent (a TLS session whose socket is
    /// stuck), up to this many bytes; then writes are pending (having taken nothing).
    Swallow(usize),
    /// The next write takes up to this many bytes into its buffer and then fails (as
    /// `futures-rustls` does when the socket fails after the session took plaintext).
    SwallowThenFail(usize),
    /// Writes send up to this many bytes to the socket and return `Pending` without reporting
    /// them (as an OpenSSL `SSL_write` that sent a record and then hit `WANT_WRITE`, without
    /// `SSL_MODE_ENABLE_PARTIAL_WRITE`). Pending ever after.
    ForwardThenPending(usize),
    /// The next write takes up to this many bytes into its buffer and then says it wrote 0.
    ZeroAfterTaking(usize),
}

#[derive(Debug)]
struct Plan {
    /// Bytes passed through to the real stream before `then` applies.
    pass: usize,
    then: Then,
    passed: usize,
    /// Bytes taken by `then` (kept, or forwarded unreported).
    swallowed: usize,
    failed: bool,
    /// The largest slice a write was offered once `then` applied.
    max_offered: usize,
}

impl Plan {
    fn new(pass: usize, then: Then) -> Arc<Mutex<Plan>> {
        Arc::new(Mutex::new(Plan {
            pass,
            then,
            passed: 0,
            swallowed: 0,
            failed: false,
            max_offered: 0,
        }))
    }
}

/// A stream that follows a [`Plan`] for its writes.
#[derive(Debug)]
struct Faulty<S> {
    inner: S,
    plan: Option<Arc<Mutex<Plan>>>,
}

impl<S: AsyncRead + Unpin> AsyncRead for Faulty<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Faulty<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let Some(plan) = self.plan.clone() else {
            return Pin::new(&mut self.inner).poll_write(cx, buf);
        };
        let mut plan = plan.lock().unwrap();
        if plan.passed < plan.pass {
            let n = buf.len().min(plan.pass - plan.passed);
            let written = Pin::new(&mut self.inner).poll_write(cx, &buf[..n]);
            if let Poll::Ready(Ok(m)) = written {
                plan.passed += m;
            }
            return written;
        }
        plan.max_offered = plan.max_offered.max(buf.len());
        match plan.then {
            Then::Fail => {
                plan.failed = true;
                Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()))
            }
            Then::Swallow(cap) => {
                let n = buf.len().min(cap - plan.swallowed);
                if n == 0 {
                    return Poll::Pending; // woken by the read side's end
                }
                plan.swallowed += n;
                Poll::Ready(Ok(n))
            }
            Then::SwallowThenFail(take) => {
                plan.swallowed += buf.len().min(take);
                plan.failed = true;
                Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
            }
            Then::ForwardThenPending(k) => {
                let n = buf.len().min(k - plan.swallowed);
                if n > 0 {
                    if let Poll::Ready(Ok(m)) = Pin::new(&mut self.inner).poll_write(cx, &buf[..n])
                    {
                        plan.swallowed += m;
                    }
                    let _ = Pin::new(&mut self.inner).poll_flush(cx);
                }
                Poll::Pending
            }
            Then::ZeroAfterTaking(take) => {
                plan.swallowed += buf.len().min(take);
                plan.failed = true;
                Poll::Ready(Ok(0))
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

/// A [`MemRuntime`] whose first connection follows a plan, and which says whether a failed
/// (`ERRORS`) or pending (`PENDING`, by default the same) write took nothing.
#[derive(Clone)]
struct FaultRuntime<const ERRORS: bool, const PENDING: bool = ERRORS> {
    mem: MemRuntime,
    plan: Arc<Mutex<Option<Arc<Mutex<Plan>>>>>,
}

impl<const ERRORS: bool, const PENDING: bool> FaultRuntime<ERRORS, PENDING> {
    fn new(net: &Arc<Net>, plan: Option<Arc<Mutex<Plan>>>) -> Self {
        FaultRuntime {
            mem: MemRuntime::new(net),
            plan: Arc::new(Mutex::new(plan)),
        }
    }
}

impl<const ERRORS: bool, const PENDING: bool> Runtime for FaultRuntime<ERRORS, PENDING> {
    type Stream = Faulty<MemStream>;
    type Task<T: Send + 'static> = TokioTask<T>;

    const WRITE_ERRORS_TAKE_NOTHING: bool = ERRORS;
    const PENDING_TAKES_NOTHING: bool = PENDING;

    fn now(&self) -> Instant {
        self.mem.now()
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.mem.sleep(duration)
    }

    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send {
        self.mem.sleep_until(deadline)
    }

    fn spawn<T, F>(&self, future: F) -> TokioTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.mem.spawn(future)
    }

    async fn connect(&self, addr: SocketAddr) -> io::Result<Faulty<MemStream>> {
        let inner = self.mem.connect(addr).await?;
        let plan = self.plan.lock().unwrap().take();
        Ok(Faulty { inner, plan })
    }

    async fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        self.mem.resolve(host, port).await
    }
}

/// A "TLS" wrapper whose stream follows a plan, and which says (with `PENDING_TRUSTED`)
/// whether a pending write took nothing, as the in-tree rustls wrapper does.
struct FaultWrapper<const PENDING_TRUSTED: bool> {
    plan: Arc<Mutex<Plan>>,
}

impl<const PENDING_TRUSTED: bool> StreamWrapper<Faulty<MemStream>>
    for FaultWrapper<PENDING_TRUSTED>
{
    type Stream = Faulty<Faulty<MemStream>>;

    const PENDING_TAKES_NOTHING: bool = PENDING_TRUSTED;

    fn tls_enabled(&self) -> bool {
        true
    }

    async fn wrap(
        &self,
        stream: Faulty<MemStream>,
        _peer: NetworkAddress,
    ) -> io::Result<Self::Stream> {
        Ok(Faulty {
            inner: stream,
            plan: Some(self.plan.clone()),
        })
    }
}

fn config() -> PeerManagerConfig {
    let mut config = PeerManagerConfig::default().without_jitter();
    // No pings: the frames on the wire are the requests' only.
    let mut monitor = MonitorConfig::FDB_DEFAULT.without_jitter();
    monitor.loop_time = Duration::from_secs(3600);
    config.monitor = monitor;
    config
}

/// Requests whose payloads are `size` bytes (the server cannot decode them, and does not
/// answer).
fn padded(
    size: usize,
) -> impl FnOnce(Uid, ProtocolVersion, &mut Vec<u8>) -> Result<(), EncodeError> + Send + 'static {
    move |_reply, _version, out| {
        out.resize(out.len() + size, 0xab);
        Ok(())
    }
}

/// The length of a request frame with a `size`-byte payload.
fn frame_len(tls: bool, size: usize) -> usize {
    FrameConfig::new(Checksum::for_tls(tls)).header_len() + 16 + size
}

/// Sends `n` requests of `size` bytes, all queued (in order) before the peer's task runs, so
/// that they are applied together once the handshake completes; returns their outcomes.
async fn send_all(peer: &PeerHandle, n: usize, size: usize) -> Vec<RequestError> {
    let mut futures: Vec<_> = (0..n)
        .map(|_| Box::pin(peer.request(endpoint(), padded(size))))
        .collect();
    for f in &mut futures {
        assert!(poll_once(f).await.is_none());
    }
    let mut out = Vec::new();
    for f in futures {
        out.push(f.await.expect_err("no reply"));
    }
    out
}

/// `NotSent` for the requests at or past `first_not_sent`, maybe delivered before it.
fn check(outcomes: &[RequestError], first_not_sent: usize) {
    for (i, e) in outcomes.iter().enumerate() {
        if i < first_not_sent {
            assert!(
                matches!(e, RequestError::MaybeDelivered(Some(_))) && e.maybe_committed(),
                "request {i}: {e:?}"
            );
        } else {
            assert!(
                matches!(e, RequestError::NotSent(Some(_))) && e.should_retry(),
                "request {i}: {e:?}"
            );
        }
    }
}

/// The index of the first request whose frame starts at or past stream offset `end`, the
/// frames being `len` bytes each, right after the ConnectPacket.
fn first_starting_at_or_past(end: usize, len: usize, n: usize) -> usize {
    (0..n)
        .find(|i| ConnectPacket::SIZE + i * len >= end)
        .unwrap_or(n)
}

/// A socket that accepts the ConnectPacket, the first request and part of the second, then
/// fails. Its writes take nothing when they fail or are pending (as a TCP socket's), so the
/// requests after the part written are not sent, and the server never saw them.
#[tokio::test(start_paused = true)]
async fn a_socket_that_accepts_n_bytes_then_fails() {
    let size = 500;
    let len = frame_len(false, size);
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::new(V74));
    let plan = Plan::new(ConnectPacket::SIZE + len + 10, Then::Fail);
    let rt = FaultRuntime::<true>::new(&net, Some(plan.clone()));
    let (sink, _events) = event_channel();
    let mgr = PeerManager::new(rt, config(), sink);
    let peer = mgr.peer(&server.address);
    let outcomes = send_all(&peer, 4, size).await;
    assert!(plan.lock().unwrap().failed);
    check(&outcomes, 2);
    until("the server read what was written", || {
        server.record(0).closed_at.is_some()
    })
    .await;
    // The first request reached the server; nothing of the third and fourth did.
    assert_eq!(server.frames(0), 1);
}

/// The same socket on a runtime that does not promise that a failed write took nothing: the
/// bytes offered to the failed write (at most 64 KiB) count as possibly delivered; the
/// requests past them are still not sent.
#[tokio::test(start_paused = true)]
async fn a_failed_write_on_an_unsure_stream_counts_what_it_was_offered() {
    let size = 30_000;
    let len = frame_len(false, size);
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::new(V74));
    let pass = ConnectPacket::SIZE + len + 10;
    let plan = Plan::new(pass, Then::Fail);
    let rt = FaultRuntime::<false>::new(&net, Some(plan.clone()));
    let (sink, _events) = event_channel();
    let mgr = PeerManager::new(rt, config(), sink);
    let peer = mgr.peer(&server.address);
    let outcomes = send_all(&peer, 6, size).await;
    let plan = plan.lock().unwrap();
    assert!(plan.failed);
    // The failed write was offered [pass, pass + 64 KiB), no more.
    assert_eq!(plan.max_offered, UNSURE_WRITE_MAX);
    let first_not_sent = first_starting_at_or_past(pass + UNSURE_WRITE_MAX, len, 6);
    assert_eq!(first_not_sent, 4);
    check(&outcomes, first_not_sent);
}

/// Bytes a TLS session took count as delivered even if the session never sent them: here the
/// session takes the first request and part of the second into its buffer and the socket is
/// stuck; the server hangs up. The third request, which the session never took (its writes
/// were pending, and the wrapper promises a pending write took nothing, as the rustls one
/// does), is not sent.
#[tokio::test(start_paused = true)]
async fn bytes_taken_by_a_tls_writer_are_maybe_delivered() {
    let size = 300;
    let len = frame_len(true, size);
    let net = Net::new();
    let mut script = Script::new(V74);
    script.tls = true;
    let mut server = listen_one(&net, sock(4500), script);
    let tls_plan = Plan::new(ConnectPacket::SIZE, Then::Swallow(len + 5));
    let wrapper = FaultWrapper::<true> {
        plan: tls_plan.clone(),
    };
    // The runtime's own socket promises exact writes; the TLS stream's errors are never
    // trusted.
    let rt = FaultRuntime::<true>::new(&net, None);
    let (sink, _events) = event_channel();
    let mgr = PeerManager::with_stream_wrapper(rt, config(), wrapper, sink);
    let peer = mgr.peer(&server.address);
    let sending = tokio::spawn({
        let peer = peer.clone();
        async move { send_all(&peer, 3, size).await }
    });
    let conn = server.conns_next().await;
    until("the session took its fill", || {
        tls_plan.lock().unwrap().swallowed == len + 5
    })
    .await;
    conn.close();
    let outcomes = sending.await.unwrap();
    check(&outcomes, 2);
    assert_eq!(server.frames(0), 0, "the server saw none of it");
}

/// A TLS session that takes part of the buffer and then fails (as `futures-rustls` returns
/// the socket's error after encrypting plaintext): everything offered to that write counts as
/// possibly delivered, even on a runtime whose plain sockets are exact; it was offered no
/// more than 64 KiB, so the frames past that are not sent.
#[tokio::test(start_paused = true)]
async fn a_tls_write_that_fails_after_taking_bytes_counts_what_it_was_offered() {
    let size = 30_000;
    let len = frame_len(true, size);
    let net = Net::new();
    let mut script = Script::new(V74);
    script.tls = true;
    let server = listen_one(&net, sock(4500), script);
    let tls_plan = Plan::new(ConnectPacket::SIZE, Then::SwallowThenFail(10));
    let wrapper = FaultWrapper::<true> {
        plan: tls_plan.clone(),
    };
    let rt = FaultRuntime::<true>::new(&net, None);
    let (sink, _events) = event_channel();
    let mgr = PeerManager::with_stream_wrapper(rt, config(), wrapper, sink);
    let peer = mgr.peer(&server.address);
    let outcomes = send_all(&peer, 5, size).await;
    let plan = tls_plan.lock().unwrap();
    assert!(plan.failed);
    // The failed write was offered [44, 44 + 64 KiB): requests 0, 1 and 2 start in it.
    assert_eq!(plan.max_offered, UNSURE_WRITE_MAX);
    let first_not_sent = first_starting_at_or_past(ConnectPacket::SIZE + UNSURE_WRITE_MAX, len, 5);
    assert_eq!(first_not_sent, 3);
    check(&outcomes, first_not_sent);
}

/// The same, with a session that takes the whole slice it was offered before failing: every
/// frame that starts inside what it took is maybe delivered, whatever the cap.
#[tokio::test(start_paused = true)]
async fn a_tls_write_that_takes_the_whole_slice_and_fails() {
    let size = 30_000;
    let n = 5;
    let len = frame_len(true, size);
    let net = Net::new();
    let mut script = Script::new(V74);
    script.tls = true;
    let server = listen_one(&net, sock(4500), script);
    let tls_plan = Plan::new(ConnectPacket::SIZE, Then::SwallowThenFail(usize::MAX));
    let wrapper = FaultWrapper::<true> {
        plan: tls_plan.clone(),
    };
    let rt = FaultRuntime::<true>::new(&net, None);
    let (sink, _events) = event_channel();
    let mgr = PeerManager::with_stream_wrapper(rt, config(), wrapper, sink);
    let peer = mgr.peer(&server.address);
    let outcomes = send_all(&peer, n, size).await;
    let plan = tls_plan.lock().unwrap();
    assert!(plan.failed);
    assert_eq!(plan.swallowed, plan.max_offered);
    assert!(plan.max_offered <= UNSURE_WRITE_MAX, "{}", plan.max_offered);
    // From what the stream actually took, not from the cap.
    let first_not_sent = first_starting_at_or_past(ConnectPacket::SIZE + plan.swallowed, len, n);
    assert!(first_not_sent < n, "something must be left unwritten");
    check(&outcomes, first_not_sent);
}

/// A wrapper stream that puts bytes on the wire and returns `Pending` without reporting them
/// (an OpenSSL session without partial writes that hits `WANT_WRITE`). The wrapper does not
/// promise that a pending write took nothing (the default), so what it was offered counts:
/// the request the server decoded is never reported not sent.
#[tokio::test(start_paused = true)]
async fn a_pending_write_that_sent_bytes_is_maybe_delivered() {
    let size = 300;
    let len = frame_len(true, size);
    let net = Net::new();
    let mut script = Script::new(V74);
    script.tls = true;
    let mut server = listen_one(&net, sock(4500), script);
    let tls_plan = Plan::new(ConnectPacket::SIZE, Then::ForwardThenPending(len + 5));
    let wrapper = FaultWrapper::<false> {
        plan: tls_plan.clone(),
    };
    let rt = FaultRuntime::<true>::new(&net, None);
    let (sink, _events) = event_channel();
    let mgr = PeerManager::with_stream_wrapper(rt, config(), wrapper, sink);
    let peer = mgr.peer(&server.address);
    let sending = tokio::spawn({
        let peer = peer.clone();
        async move { send_all(&peer, 3, size).await }
    });
    let conn = server.conns_next().await;
    until("forwarded", || {
        tls_plan.lock().unwrap().swallowed == len + 5
    })
    .await;
    until("the server decoded the first request", || {
        server.frames(0) == 1
    })
    .await;
    conn.close();
    let outcomes = sending.await.unwrap();
    // Every frame was in the slice offered to the pending write.
    let plan = tls_plan.lock().unwrap();
    assert!(plan.max_offered <= UNSURE_WRITE_MAX);
    assert!(plan.max_offered >= 3 * len);
    check(&outcomes, 3);
}

/// The same pending-write stream as the runtime's own socket, on a runtime that does not
/// promise a pending write took nothing.
#[tokio::test(start_paused = true)]
async fn a_pending_write_on_an_unsure_runtime_is_maybe_delivered() {
    let size = 300;
    let len = frame_len(false, size);
    let net = Net::new();
    let mut server = listen_one(&net, sock(4500), Script::new(V74));
    let plan = Plan::new(ConnectPacket::SIZE, Then::ForwardThenPending(len + 5));
    let rt = FaultRuntime::<false>::new(&net, Some(plan.clone()));
    let (sink, _events) = event_channel();
    let mgr = PeerManager::new(rt, config(), sink);
    let peer = mgr.peer(&server.address);
    let sending = tokio::spawn({
        let peer = peer.clone();
        async move { send_all(&peer, 3, size).await }
    });
    let conn = server.conns_next().await;
    until("the server decoded the first request", || {
        server.frames(0) == 1
    })
    .await;
    conn.close();
    let outcomes = sending.await.unwrap();
    check(&outcomes, 3);
}

/// A runtime that trusts its failed writes but not its pending ones: a pending write counts
/// what it was offered, so that slice must still be bounded, or every frame in the output
/// would be maybe delivered.
#[tokio::test(start_paused = true)]
async fn a_pending_write_is_offered_a_bounded_slice() {
    let size = 30_000;
    let n = 5;
    let len = frame_len(false, size);
    let net = Net::new();
    let mut server = listen_one(&net, sock(4500), Script::new(V74));
    let plan = Plan::new(ConnectPacket::SIZE, Then::ForwardThenPending(len + 10));
    let rt = FaultRuntime::<true, false>::new(&net, Some(plan.clone()));
    let (sink, _events) = event_channel();
    let mgr = PeerManager::new(rt, config(), sink);
    let peer = mgr.peer(&server.address);
    let sending = tokio::spawn({
        let peer = peer.clone();
        async move { send_all(&peer, n, size).await }
    });
    let conn = server.conns_next().await;
    until("the server decoded the first request", || {
        server.frames(0) == 1
    })
    .await;
    conn.close();
    let outcomes = sending.await.unwrap();
    assert_eq!(plan.lock().unwrap().max_offered, UNSURE_WRITE_MAX);
    let first_not_sent = first_starting_at_or_past(ConnectPacket::SIZE + UNSURE_WRITE_MAX, len, n);
    assert_eq!(first_not_sent, 3);
    check(&outcomes, first_not_sent);
}

/// A TLS write that takes bytes and then reports 0 written: `Ok(0)` from a stream that does
/// not promise exact failures is treated as a failure that may have taken what it was offered.
#[tokio::test(start_paused = true)]
async fn a_zero_write_after_taking_bytes_counts_what_it_was_offered() {
    let size = 30_000;
    let n = 5;
    let len = frame_len(true, size);
    let net = Net::new();
    let mut script = Script::new(V74);
    script.tls = true;
    let server = listen_one(&net, sock(4500), script);
    let tls_plan = Plan::new(ConnectPacket::SIZE, Then::ZeroAfterTaking(10));
    let wrapper = FaultWrapper::<true> {
        plan: tls_plan.clone(),
    };
    let rt = FaultRuntime::<true>::new(&net, None);
    let (sink, _events) = event_channel();
    let mgr = PeerManager::with_stream_wrapper(rt, config(), wrapper, sink);
    let peer = mgr.peer(&server.address);
    let outcomes = send_all(&peer, n, size).await;
    let plan = tls_plan.lock().unwrap();
    assert!(plan.failed);
    assert_eq!(plan.max_offered, UNSURE_WRITE_MAX);
    let first_not_sent = first_starting_at_or_past(ConnectPacket::SIZE + UNSURE_WRITE_MAX, len, n);
    assert_eq!(first_not_sent, 3);
    check(&outcomes, first_not_sent);
}
