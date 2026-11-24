//! Unwritten requests are `NotSent` through the peer manager (as8.8): a socket that accepts
//! N bytes and then fails, a runtime that does not promise that failed writes take nothing,
//! and a TLS-like stream that takes bytes into its own buffer without sending them (the bytes
//! a TLS writer took count as possibly delivered, and so does what it was offered when it
//! failed).

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
use fdb_native_transport::peer::{PeerHandle, PeerManager, PeerManagerConfig, event_channel};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::runtime::{Instant, Runtime, TokioTask};
use fdb_native_transport::{Checksum, ConnectPacket, FrameConfig, MonitorConfig};
use fdb_native_wire::{EncodeError, NetworkAddress, ProtocolVersion, Uid};
use futures_io::{AsyncRead, AsyncWrite};
use futures_lite::future::poll_once;

const V74: ProtocolVersion = ProtocolVersion::V7_4;

fn endpoint() -> Uid {
    Uid::new(0x5eed_0000_0000_0000 | 1, 1)
}

/// What a [`Faulty`] stream does with writes.
#[derive(Clone, Copy, Debug)]
enum Then {
    /// Every write fails, taking nothing.
    Fail,
    /// Writes are taken into a buffer that is never sent (a TLS session whose socket is
    /// stuck), up to this many bytes; then writes are pending.
    Swallow(usize),
    /// The next write takes up to this many bytes into its buffer and then fails (as
    /// `futures-rustls` does when the socket fails after the session took plaintext).
    SwallowThenFail(usize),
}

#[derive(Debug)]
struct Plan {
    /// Bytes passed through to the real stream before `then` applies.
    pass: usize,
    then: Then,
    passed: usize,
    swallowed: usize,
    failed: bool,
}

impl Plan {
    fn new(pass: usize, then: Then) -> Arc<Mutex<Plan>> {
        Arc::new(Mutex::new(Plan {
            pass,
            then,
            passed: 0,
            swallowed: 0,
            failed: false,
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
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

/// A [`MemRuntime`] whose first connection follows a plan, and which says (with `EXACT`)
/// whether a failed write took nothing.
#[derive(Clone)]
struct FaultRuntime<const EXACT: bool> {
    mem: MemRuntime,
    plan: Arc<Mutex<Option<Arc<Mutex<Plan>>>>>,
}

impl<const EXACT: bool> Runtime for FaultRuntime<EXACT> {
    type Stream = Faulty<MemStream>;
    type Task<T: Send + 'static> = TokioTask<T>;

    const WRITE_ERRORS_TAKE_NOTHING: bool = EXACT;

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

/// A socket that accepts the ConnectPacket, the first request and part of the second, then
/// fails. Its writes take nothing when they fail (as a TCP socket's), so the requests after
/// the part written are not sent, and the server never saw them.
#[tokio::test(start_paused = true)]
async fn a_socket_that_accepts_n_bytes_then_fails() {
    let size = 500;
    let len = frame_len(false, size);
    let net = Net::new();
    let server = listen_one(&net, sock(4500), Script::new(V74));
    let plan = Plan::new(ConnectPacket::SIZE + len + 10, Then::Fail);
    let rt = FaultRuntime::<true> {
        mem: MemRuntime::new(&net),
        plan: Arc::new(Mutex::new(Some(plan.clone()))),
    };
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
    let rt = FaultRuntime::<false> {
        mem: MemRuntime::new(&net),
        plan: Arc::new(Mutex::new(Some(plan.clone()))),
    };
    let (sink, _events) = event_channel();
    let mgr = PeerManager::new(rt, config(), sink);
    let peer = mgr.peer(&server.address);
    let outcomes = send_all(&peer, 6, size).await;
    assert!(plan.lock().unwrap().failed);
    // Request i starts at 44 + i * len; the failed write was offered [pass, pass + 64 KiB).
    let offered_end = pass + 64 * 1024;
    let first_not_sent = (0..6)
        .find(|i| ConnectPacket::SIZE + i * len >= offered_end)
        .unwrap();
    assert_eq!(first_not_sent, 4);
    check(&outcomes, first_not_sent);
}

/// Bytes a TLS session took count as delivered even if the session never sent them: here the
/// session takes the first request and part of the second into its buffer and the socket is
/// stuck; the server hangs up. The third request, which the session never took, is not sent.
#[tokio::test(start_paused = true)]
async fn bytes_taken_by_a_tls_writer_are_maybe_delivered() {
    let size = 300;
    let len = frame_len(true, size);
    let net = Net::new();
    let mut script = Script::new(V74);
    script.tls = true;
    let mut server = listen_one(&net, sock(4500), script);
    let tls_plan = Plan::new(ConnectPacket::SIZE, Then::Swallow(len + 5));
    let wrapper_plan = tls_plan.clone();
    let wrapper = move |stream: Faulty<MemStream>, _peer: NetworkAddress| {
        let plan = wrapper_plan.clone();
        async move {
            Ok(Faulty {
                inner: stream,
                plan: Some(plan),
            })
        }
    };
    // The runtime's own socket promises exact errors; the TLS stream on it is never trusted.
    let rt = FaultRuntime::<true> {
        mem: MemRuntime::new(&net),
        plan: Arc::new(Mutex::new(None)),
    };
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
/// possibly delivered, even on a runtime whose plain sockets are exact.
#[tokio::test(start_paused = true)]
async fn a_tls_write_that_fails_after_taking_bytes_counts_what_it_was_offered() {
    let size = 30_000;
    let len = frame_len(true, size);
    let net = Net::new();
    let mut script = Script::new(V74);
    script.tls = true;
    let server = listen_one(&net, sock(4500), script);
    let tls_plan = Plan::new(ConnectPacket::SIZE, Then::SwallowThenFail(10));
    let wrapper_plan = tls_plan.clone();
    let wrapper = move |stream: Faulty<MemStream>, _peer: NetworkAddress| {
        let plan = wrapper_plan.clone();
        async move {
            Ok(Faulty {
                inner: stream,
                plan: Some(plan),
            })
        }
    };
    let rt = FaultRuntime::<true> {
        mem: MemRuntime::new(&net),
        plan: Arc::new(Mutex::new(None)),
    };
    let (sink, _events) = event_channel();
    let mgr = PeerManager::with_stream_wrapper(rt, config(), wrapper, sink);
    let peer = mgr.peer(&server.address);
    let outcomes = send_all(&peer, 5, size).await;
    assert!(tls_plan.lock().unwrap().failed);
    // The failed write was offered [44, 44 + 64 KiB): requests 0, 1 and 2 start in it.
    let offered_end = ConnectPacket::SIZE + 64 * 1024;
    let first_not_sent = (0..5)
        .find(|i| ConnectPacket::SIZE + i * len >= offered_end)
        .unwrap();
    assert_eq!(first_not_sent, 3);
    check(&outcomes, first_not_sent);
}
