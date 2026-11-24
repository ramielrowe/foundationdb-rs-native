//! [`Runtime`] adapter for the smol family of crates (see [`SmolRuntime`]).

use std::future::Future;
use std::io::{self, IoSlice};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use async_executor::{Executor, FallibleTask};
use async_io::{Async, Timer};
use futures_io::{AsyncRead, AsyncWrite};
use futures_lite::FutureExt as _;

use super::{Instant, JoinError, Runtime, TaskHandle};

/// [`Runtime`] backed by an `async-executor` executor, `async-io` sockets and timers.
///
/// Cheap to clone. All `SmolRuntime`s in a process share one clock epoch, so their
/// [`Instant`]s are interchangeable.
///
/// Enabled by the `smol` cargo feature. It is built from the pieces `smol` itself is made of,
/// and does not depend on tokio:
///
/// * [`async-io`](https://docs.rs/async-io) for sockets (`Async<TcpStream>`) and timers
///   (`Timer`). Its reactor runs on its own thread, so sockets and timers work no matter which
///   executor polls the futures that use them.
/// * [`async-executor`](https://docs.rs/async-executor) for tasks.
/// * [`blocking`](https://docs.rs/blocking) for DNS (`getaddrinfo` runs on its thread pool).
///
/// ```
/// use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig};
/// use fdb_native_transport::runtime::{Runtime, SmolRuntime};
///
/// futures_lite::future::block_on(async {
///     // The global executor's threads run the transport's tasks; this future only awaits.
///     let rt = SmolRuntime::new();
///     let peers = PeerManager::new(rt.clone(), PeerManagerConfig::default(), DiscardEvents);
///     rt.sleep(std::time::Duration::from_millis(1)).await;
///     peers.shutdown().await;
/// });
/// ```
///
/// # Executors
///
/// A [`SmolRuntime`] spawns onto an [`async_executor::Executor`] that something must drive.
/// There are two ways to get one:
///
/// * [`SmolRuntime::new`] uses a process-wide executor, like `smol::spawn` does. It starts its
///   driver threads (one per CPU, named `fdb-native-smol-N`) the first time it is used, and
///   they run for the life of the process.
/// * [`SmolRuntime::with_executor`] uses an executor you drive yourself, for example with
///   `smol::block_on(executor.run(...))` on threads you own. Tasks make progress only while
///   it is being run. This is the way to put the client on a small, fixed set of threads, or
///   on an executor that shuts down: tasks still pending when it is dropped resolve to
///   [`JoinError::Aborted`].
///
/// `async-executor` is a public dependency of this crate: [`SmolRuntime::with_executor`] takes
/// its `Executor` (semver `1.x`).
///
/// # Clock
///
/// The clock is the OS monotonic clock (`std::time::Instant`) mapped onto the process-wide
/// [`Instant`] epoch, as for the tokio adapter. There is no virtual time: a test that wants
/// one drives a sans-IO state machine with a [`ManualClock`](super::ManualClock).
///
/// # Streams and at-most-once
///
/// [`SmolStream`] writes one `send(2)`/`writev(2)` per `poll_write` and so sets
/// [`Runtime::WRITE_ERRORS_TAKE_NOTHING`] and [`Runtime::PENDING_TAKES_NOTHING`]; the
/// justification is on the [`Runtime`] impl.
///
/// `async-io`'s own `poll_close` for `Async<T>` only flushes. [`SmolStream`] shuts the write half
/// down instead, as the [`Runtime`] contract requires.
#[derive(Clone, Debug)]
pub struct SmolRuntime {
    executor: Arc<Executor<'static>>,
}

impl SmolRuntime {
    /// A runtime on the process-wide executor, which is driven by background threads started
    /// on first use (one per available CPU).
    ///
    /// Works from any thread, inside or outside an async context.
    pub fn new() -> Self {
        static GLOBAL: OnceLock<Arc<Executor<'static>>> = OnceLock::new();
        let executor = GLOBAL.get_or_init(|| {
            let executor = Arc::new(Executor::new());
            let threads = std::thread::available_parallelism().map_or(1, usize::from);
            for n in 0..threads {
                let executor = Arc::clone(&executor);
                let spawned = std::thread::Builder::new()
                    .name(format!("fdb-native-smol-{n}"))
                    .spawn(move || {
                        // A panic that escapes a task (a destructor's) must not end the driver.
                        loop {
                            let _ = catch_unwind(AssertUnwindSafe(|| {
                                async_io::block_on(executor.run(std::future::pending::<()>()));
                            }));
                        }
                    });
                // Some threads are enough; none is not.
                if n == 0 {
                    spawned.expect("SmolRuntime: cannot start an executor thread");
                }
            }
            executor
        });
        Self {
            executor: Arc::clone(executor),
        }
    }

    /// A runtime that spawns onto `executor`. Tasks run only while something runs the
    /// executor (`executor.run(..)` or `executor.tick()`).
    pub fn with_executor(executor: Arc<Executor<'static>>) -> Self {
        Self { executor }
    }

    /// The executor this runtime spawns onto.
    pub fn executor(&self) -> &Arc<Executor<'static>> {
        &self.executor
    }
}

impl Default for SmolRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime for SmolRuntime {
    type Stream = SmolStream;
    type Task<T: Send + 'static> = SmolTask<T>;

    /// A [`SmolStream`] write is one non-blocking `send(2)` (`writev(2)` for a vectored
    /// write) on the socket, which on an error has written nothing (a partial write returns
    /// its count instead).
    ///
    /// The chain, against the sources of `async-io` 2.6.0 (`src/lib.rs`):
    ///
    /// * `impl AsyncWrite for Async<T>` (lines 1334-1361): `poll_write` is
    ///   `loop { match (*self).get_mut().write(buf) { Err(WouldBlock) => {} res => return
    ///   Poll::Ready(res) }; ready!(self.poll_writable(cx))?; }`, and `poll_write_vectored` is
    ///   the same with `write_vectored`. It buffers nothing and calls the underlying
    ///   `std::net::TcpStream` once per iteration, so whatever it returns as `Ready` is what a
    ///   single `write` returned.
    /// * `std::net::TcpStream::write` is `send(fd, buf, len, MSG_NOSIGNAL)` and `write_vectored`
    ///   is `writev` (`library/std/src/sys/net/connection/socket`): one syscall, which
    ///   returns either the count of bytes the kernel took or -1 with nothing taken (POSIX
    ///   `send`/`writev`; Linux `tcp_sendmsg` copies no bytes on an error return and reports
    ///   a short count instead of an error after a partial copy).
    /// * `poll_writable`'s `?` (line 1344) returns an error from registering interest with
    ///   the reactor; it is reached only after a `write` that returned `WouldBlock`, which took
    ///   nothing.
    ///
    /// So an `Err` (and `Ok(0)`, which `send` returns only for an empty buffer) took nothing.
    const WRITE_ERRORS_TAKE_NOTHING: bool = true;

    /// A [`SmolStream`] write is pending only after the socket's `write` returned
    /// `WouldBlock` (`async-io` 2.6.0 `src/lib.rs` lines 1335-1346: the only path to
    /// `Poll::Pending` is `ready!(self.poll_writable(cx))` after a `write` that failed with
    /// `WouldBlock`), and a would-block `send` took no bytes. The retry after the socket
    /// becomes writable is a fresh `write` of the same buffer, so nothing from an earlier
    /// iteration is double counted either.
    const PENDING_TAKES_NOTHING: bool = true;

    /// The same executor (every [`SmolRuntime::new`] is the process-wide one).
    fn same_runtime(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.executor, &other.executor)
    }

    fn now(&self) -> Instant {
        Instant::from_std(std::time::Instant::now())
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        // The deadline is fixed here, not at first poll (`Timer::after` reads the clock now).
        let timer = Timer::after(duration);
        async move {
            timer.await;
        }
    }

    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send {
        let timer = match deadline.to_std() {
            Some(at) => Timer::at(at),
            // Beyond what `std::time::Instant` can represent: never.
            None => Timer::never(),
        };
        async move {
            timer.await;
        }
    }

    fn spawn<T, F>(&self, future: F) -> SmolTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let state = Arc::new(AbortState::default());
        let body = guarded(future, Arc::clone(&state));
        SmolTask {
            task: Some(self.executor.spawn(body).fallible()),
            state,
        }
    }

    async fn connect(&self, addr: SocketAddr) -> io::Result<SmolStream> {
        let stream = Async::<TcpStream>::connect(addr).await?;
        stream.get_ref().set_nodelay(true)?;
        Ok(SmolStream(stream))
    }

    async fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        // A literal needs no lookup (and no thread).
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        let host = host.to_owned();
        blocking::unblock(move || {
            (host.as_str(), port)
                .to_socket_addrs()
                .map(Iterator::collect)
        })
        .await
    }
}

/// A non-blocking [`TcpStream`] registered with the `async-io` reactor, exposed through the
/// `futures_io` traits.
///
/// The reactor is process-wide, so the stream may be polled from any thread and any executor.
#[derive(Debug)]
pub struct SmolStream(Async<TcpStream>);

impl SmolStream {
    /// The remote address of the connection.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.0.get_ref().peer_addr()
    }

    /// The local address of the connection.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.get_ref().local_addr()
    }

    /// Unwraps the underlying `async-io` stream.
    pub fn into_inner(self) -> Async<TcpStream> {
        self.0
    }
}

impl AsyncRead for SmolStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for SmolStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        // `Async<T>`'s own `poll_close` only flushes; the contract wants the write half shut.
        // `shutdown` never blocks.
        Poll::Ready(self.0.get_ref().shutdown(Shutdown::Write))
    }
}

/// What `abort` tells a task: a flag and the waker to get the task polled to see it.
#[derive(Debug, Default)]
struct AbortState {
    aborted: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

/// The body of a spawned task: the user's future with its panic caught, raced against an
/// abort. The abort side is polled first, so once `abort` is called the future is dropped at
/// the task's next poll (destructors run, nothing more from its body does).
async fn guarded<T, F>(future: F, state: Arc<AbortState>) -> Result<T, JoinError>
where
    F: Future<Output = T>,
{
    let aborted = std::future::poll_fn(|cx| {
        if state.aborted.load(Ordering::SeqCst) {
            return Poll::Ready(Err(JoinError::Aborted));
        }
        {
            let mut waker = state.waker.lock().unwrap();
            if !waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                *waker = Some(cx.waker().clone());
            }
        }
        // `abort` may have run between the check and the registration.
        if state.aborted.load(Ordering::SeqCst) {
            return Poll::Ready(Err(JoinError::Aborted));
        }
        Poll::Pending
    });
    let run = async {
        AssertUnwindSafe(future)
            .catch_unwind()
            .await
            .map_err(|_| JoinError::Panicked)
    };
    futures_lite::future::or(aborted, run).await
}

/// Handle to a task spawned on a [`SmolRuntime`]. Dropping it detaches the task.
#[derive(Debug)]
pub struct SmolTask<T> {
    task: Option<FallibleTask<Result<T, JoinError>>>,
    state: Arc<AbortState>,
}

impl<T> Future for SmolTask<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(task) = self.task.as_mut() else {
            return Poll::Ready(Err(JoinError::Aborted));
        };
        match Pin::new(task).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(result)) => Poll::Ready(result),
            // The executor dropped the task without finishing it.
            Poll::Ready(None) => Poll::Ready(Err(JoinError::Aborted)),
        }
    }
}

impl<T> Drop for SmolTask<T> {
    fn drop(&mut self) {
        // async-executor's task cancels on drop; the contract detaches.
        if let Some(task) = self.task.take() {
            task.detach();
        }
    }
}

impl<T: Send + 'static> TaskHandle<T> for SmolTask<T> {
    fn abort(&self) {
        self.state.aborted.store(true, Ordering::SeqCst);
        let waker = self.state.waker.lock().unwrap().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(FallibleTask::is_finished)
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::io::Read;
    use std::net::TcpListener;
    use std::sync::atomic::AtomicUsize;

    use futures_lite::future::{block_on, or, poll_once};
    use futures_lite::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::runtime::tests::spawn_connection;

    /// Sets a flag when dropped.
    struct SetOnDrop(Arc<AtomicBool>);

    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Binds a loopback listener and spawns `serve` on its first connection.
    fn serve<F, Fut>(rt: &SmolRuntime, serve: F) -> (SocketAddr, SmolTask<()>)
    where
        F: FnOnce(SmolStream) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let listener = Async::<TcpListener>::bind(([127, 0, 0, 1], 0)).unwrap();
        let addr = listener.get_ref().local_addr().unwrap();
        let task = rt.spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            serve(SmolStream(sock)).await;
        });
        (addr, task)
    }

    #[test]
    fn loopback_round_trip() {
        block_on(async {
            let rt = SmolRuntime::new();
            let (addr, server) = serve(&rt, |mut sock| async move {
                let mut buf = [0_u8; 5];
                sock.read_exact(&mut buf).await.unwrap();
                sock.write_all(&buf).await.unwrap();
            });

            let mut stream = rt.connect(addr).await.unwrap();
            assert_eq!(stream.peer_addr().unwrap(), addr);
            assert!(stream.local_addr().unwrap().ip().is_loopback());
            stream.write_all(b"hello").await.unwrap();
            stream.flush().await.unwrap();
            let mut got = [0_u8; 5];
            stream.read_exact(&mut got).await.unwrap();
            assert_eq!(&got, b"hello");

            // The server task finished and dropped its socket: EOF.
            server.await.unwrap();
            let mut rest = Vec::new();
            assert_eq!(stream.read_to_end(&mut rest).await.unwrap(), 0);
            stream.close().await.unwrap();
            assert!(stream.into_inner().get_ref().nodelay().unwrap());
        });
    }

    #[test]
    fn close_shuts_down_the_write_half_only() {
        block_on(async {
            let rt = SmolRuntime::new();
            let (addr, server) = serve(&rt, |mut sock| async move {
                let mut got = Vec::new();
                // EOF arrives after the client's close, while the client still reads.
                sock.read_to_end(&mut got).await.unwrap();
                assert_eq!(got, b"bye");
                sock.write_all(b"ack").await.unwrap();
            });

            let mut stream = rt.connect(addr).await.unwrap();
            stream.write_all(b"bye").await.unwrap();
            stream.close().await.unwrap();
            let mut reply = Vec::new();
            stream.read_to_end(&mut reply).await.unwrap();
            assert_eq!(reply, b"ack");
            server.await.unwrap();
        });
    }

    #[test]
    fn vectored_write_reaches_peer() {
        block_on(async {
            let rt = SmolRuntime::new();
            let (addr, server) = serve(&rt, |mut sock| async move {
                let mut got = Vec::new();
                sock.read_to_end(&mut got).await.unwrap();
                assert_eq!(got, b"foobarbaz");
            });

            let mut stream = rt.connect(addr).await.unwrap();
            let bufs = [
                IoSlice::new(b"foo"),
                IoSlice::new(b""),
                IoSlice::new(b"bar"),
                IoSlice::new(b"baz"),
            ];
            let mut written = 0;
            while written < 9 {
                let mut remaining = bufs;
                let mut slices: &mut [IoSlice<'_>] = &mut remaining;
                IoSlice::advance_slices(&mut slices, written);
                let n = stream.write_vectored(slices).await.unwrap();
                assert!(n > 0);
                written += n;
            }
            stream.close().await.unwrap();
            server.await.unwrap();
        });
    }

    #[test]
    fn short_read_into_small_buffer() {
        block_on(async {
            let rt = SmolRuntime::new();
            let (addr, server) = serve(&rt, |mut sock| async move {
                sock.write_all(b"0123456789").await.unwrap();
                sock.close().await.unwrap();
            });

            let mut stream = rt.connect(addr).await.unwrap();
            let mut small = [0_u8; 3];
            let n = stream.read(&mut small).await.unwrap();
            assert!((1..=3).contains(&n), "read {n} bytes into a 3-byte buffer");
            assert_eq!(&small[..n], &b"0123456789"[..n]);
            let mut rest = Vec::new();
            stream.read_to_end(&mut rest).await.unwrap();
            assert_eq!(rest, &b"0123456789"[n..]);
            server.await.unwrap();
        });
    }

    /// The safety constants' premise: a write that returns `Pending` took nothing, and every
    /// `Ok(n)` took exactly `n`. The peer does not read until the sender's buffers are full,
    /// then everything it receives is compared with what the writes reported.
    #[test]
    fn pending_write_took_nothing_and_counts_are_exact() {
        const _: () = assert!(<SmolRuntime as Runtime>::WRITE_ERRORS_TAKE_NOTHING);
        const _: () = assert!(<SmolRuntime as Runtime>::PENDING_TAKES_NOTHING);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let reader = std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            // Read nothing for a while: the sender has to hit would-block.
            std::thread::sleep(Duration::from_millis(1500));
            let mut got = Vec::new();
            sock.read_to_end(&mut got).unwrap();
            got
        });

        let (sent, data) = block_on(async {
            let rt = SmolRuntime::new();
            let mut stream = rt.connect(addr).await.unwrap();
            // A pattern that makes any duplicated or skipped byte show up.
            let data: Vec<u8> = (0..(1_u32 << 20)).map(|i| (i % 251) as u8).collect();
            let mut sent = 0_usize;
            let mut pendings = 0;
            while pendings < 2 {
                let chunk = &data[sent % data.len()..];
                let r = poll_once(poll_fn(|cx| Pin::new(&mut stream).poll_write(cx, chunk))).await;
                match r {
                    Some(Ok(n)) => {
                        assert!(n > 0 && n <= chunk.len());
                        sent += n;
                    }
                    Some(Err(e)) => panic!("write failed: {e}"),
                    None => {
                        // Would block: the kernel's buffers are full. Give the reader time
                        // to drain some, then go again.
                        pendings += 1;
                        Timer::after(Duration::from_millis(1000)).await;
                    }
                }
            }
            stream.close().await.unwrap();
            (sent, data)
        });

        let got = reader.join().unwrap();
        assert!(sent > 0);
        // Same length and same bytes: nothing taken by a pending write, nothing lost.
        assert_eq!(got.len(), sent);
        for (i, b) in got.iter().enumerate() {
            assert_eq!(*b, data[i % data.len()], "byte {i}");
        }
    }

    #[test]
    fn write_to_a_closed_peer_is_an_error() {
        block_on(async {
            let rt = SmolRuntime::new();
            let (addr, server) = serve(&rt, |sock| async move {
                drop(sock);
            });
            let mut stream = rt.connect(addr).await.unwrap();
            server.await.unwrap();
            // The first writes may succeed (buffered by the kernel); one eventually fails.
            let mut failed = false;
            for _ in 0..200 {
                if stream.write_all(&[7_u8; 1024]).await.is_err() {
                    failed = true;
                    break;
                }
                Timer::after(Duration::from_millis(5)).await;
            }
            assert!(failed, "writes to a closed peer never failed");
        });
    }

    #[test]
    fn connect_refused_is_an_error() {
        block_on(async {
            let rt = SmolRuntime::new();
            let addr = {
                let l = TcpListener::bind("127.0.0.1:0").unwrap();
                l.local_addr().unwrap()
            };
            let err = rt.connect(addr).await.map(drop).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
        });
    }

    #[test]
    fn sleep_waits_at_least_the_duration() {
        block_on(async {
            let rt = SmolRuntime::new();
            let t0 = rt.now();
            rt.sleep(Duration::from_millis(60)).await;
            assert!(rt.now() - t0 >= Duration::from_millis(60));

            let deadline = rt.now() + Duration::from_millis(60);
            rt.sleep_until(deadline).await;
            assert!(rt.now() >= deadline);

            // A deadline in the past completes at once.
            let before = rt.now();
            rt.sleep_until(t0).await;
            assert!(rt.now() - before < Duration::from_secs(5));
        });
    }

    #[test]
    fn sleep_fixes_its_deadline_when_called() {
        block_on(async {
            let rt = SmolRuntime::new();
            let t0 = rt.now();
            let sleep = rt.sleep(Duration::from_millis(80));
            // Not polled for a while: the deadline still counts from the call.
            Timer::after(Duration::from_millis(100)).await;
            let polled = rt.now();
            sleep.await;
            assert!(rt.now() - polled < Duration::from_millis(50));
            assert!(rt.now() - t0 >= Duration::from_millis(100));
        });
    }

    #[test]
    fn dropped_sleep_loser_is_cancelled() {
        block_on(async {
            let rt = SmolRuntime::new();
            let cancelled = Arc::new(AtomicBool::new(false));
            let short = async {
                rt.sleep(Duration::from_millis(30)).await;
                "short"
            };
            let flag = Arc::clone(&cancelled);
            let long = async {
                let _guard = SetOnDrop(flag);
                rt.sleep(Duration::from_secs(3600)).await;
                "long"
            };
            assert_eq!(or(short, long).await, "short");
            assert!(cancelled.load(Ordering::SeqCst), "loser was dropped");
        });
    }

    #[test]
    fn far_future_deadline_does_not_panic() {
        block_on(async {
            let rt = SmolRuntime::new();
            let never = rt.sleep_until(Instant::from_origin(Duration::MAX));
            let short = async {
                rt.sleep(Duration::from_millis(20)).await;
                true
            };
            let never = async {
                never.await;
                false
            };
            assert!(or(short, never).await, "far-future sleep completed");
        });
    }

    #[test]
    fn spawn_returns_output() {
        block_on(async {
            let rt = SmolRuntime::new();
            assert_eq!(rt.spawn(async { 21 * 2 }).await, Ok(42));
        });
    }

    #[test]
    fn spawn_and_abort() {
        block_on(async {
            let rt = SmolRuntime::new();
            let dropped = Arc::new(AtomicBool::new(false));
            let after = Arc::new(AtomicBool::new(false));
            let guard = SetOnDrop(Arc::clone(&dropped));
            let task = {
                let rt = rt.clone();
                let after = Arc::clone(&after);
                rt.clone().spawn(async move {
                    let _guard = guard;
                    rt.sleep(Duration::from_secs(3600)).await;
                    after.store(true, Ordering::SeqCst);
                })
            };
            rt.sleep(Duration::from_millis(50)).await;
            assert!(!task.is_finished());
            assert!(!dropped.load(Ordering::SeqCst));

            task.abort();
            task.abort(); // idempotent
            assert_eq!(task.await, Err(JoinError::Aborted));
            assert!(dropped.load(Ordering::SeqCst), "aborted task was dropped");
            assert!(!after.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn abort_before_the_first_poll_never_runs_the_body() {
        // An executor nothing runs: the task cannot start before the abort.
        let executor = Arc::new(Executor::new());
        let rt = SmolRuntime::with_executor(Arc::clone(&executor));
        let ran = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = SetOnDrop(Arc::clone(&dropped));
        let flag = Arc::clone(&ran);
        let task = rt.spawn(async move {
            let _guard = guard;
            flag.store(true, Ordering::SeqCst);
        });
        task.abort();
        block_on(executor.run(async {
            assert_eq!(task.await, Err(JoinError::Aborted));
        }));
        assert!(!ran.load(Ordering::SeqCst));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn abort_after_completion_keeps_the_output() {
        block_on(async {
            let rt = SmolRuntime::new();
            let task = rt.spawn(async { 5 });
            while !task.is_finished() {
                Timer::after(Duration::from_millis(5)).await;
            }
            task.abort();
            assert_eq!(task.await, Ok(5));
        });
    }

    #[test]
    fn dropping_handle_detaches() {
        block_on(async {
            let rt = SmolRuntime::new();
            let done = Arc::new(AtomicBool::new(false));
            let flag = Arc::clone(&done);
            let task = {
                let rt = rt.clone();
                rt.clone().spawn(async move {
                    rt.sleep(Duration::from_millis(50)).await;
                    flag.store(true, Ordering::SeqCst);
                })
            };
            drop(task);
            rt.sleep(Duration::from_millis(400)).await;
            assert!(
                done.load(Ordering::SeqCst),
                "detached task ran to completion"
            );
        });
    }

    #[test]
    fn panicking_task_reports_panic_and_the_executor_survives() {
        block_on(async {
            let rt = SmolRuntime::new();
            let task = rt.spawn(async { panic!("boom") });
            let err = task.await.unwrap_err();
            assert!(err.is_panic());
            assert!(!err.is_aborted());
            // Still working.
            assert_eq!(rt.spawn(async { 1 }).await, Ok(1));
        });
    }

    #[test]
    fn a_user_executor_runs_tasks_only_while_it_is_driven() {
        let executor = Arc::new(Executor::new());
        let rt = SmolRuntime::with_executor(Arc::clone(&executor));
        let counter = Arc::new(AtomicUsize::new(0));
        let task = {
            let counter = Arc::clone(&counter);
            rt.spawn(async move { counter.fetch_add(1, Ordering::SeqCst) })
        };
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(counter.load(Ordering::SeqCst), 0, "nothing drives it");
        assert!(!task.is_finished());
        assert_eq!(block_on(executor.run(task)), Ok(0));
    }

    #[test]
    fn dropping_the_executor_aborts_pending_tasks() {
        let executor = Arc::new(Executor::new());
        let rt = SmolRuntime::with_executor(Arc::clone(&executor));
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = SetOnDrop(Arc::clone(&dropped));
        let task = rt.spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        // Run it to its first suspension point.
        block_on(executor.run(async {
            Timer::after(Duration::from_millis(50)).await;
        }));
        assert!(!task.is_finished());
        drop(rt);
        drop(executor);
        assert_eq!(block_on(task), Err(JoinError::Aborted));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn resolves_localhost() {
        block_on(async {
            let rt = SmolRuntime::new();
            let addrs = rt.resolve("localhost", 4500).await.unwrap();
            assert!(!addrs.is_empty());
            assert!(
                addrs
                    .iter()
                    .all(|a| a.port() == 4500 && a.ip().is_loopback())
            );
        });
    }

    #[test]
    fn resolves_literals_and_reports_errors() {
        block_on(async {
            let rt = SmolRuntime::new();
            assert_eq!(
                rt.resolve("10.1.2.3", 4500).await.unwrap(),
                ["10.1.2.3:4500".parse::<SocketAddr>().unwrap()]
            );
            assert_eq!(
                rt.resolve("::1", 4501).await.unwrap(),
                ["[::1]:4501".parse::<SocketAddr>().unwrap()]
            );
            // A name that cannot exist (RFC 6761 reserves `.invalid`).
            assert!(rt.resolve("no-such-host.invalid", 4500).await.is_err());
        });
    }

    #[test]
    fn separate_runtimes_share_one_clock() {
        block_on(async {
            let a = SmolRuntime::new();
            let b = SmolRuntime::with_executor(Arc::new(Executor::new()));
            let (ta, tb) = (a.now(), b.now());
            let skew = if ta > tb { ta - tb } else { tb - ta };
            assert!(skew <= Duration::from_millis(50), "{skew:?}");
            let deadline = a.now() + Duration::from_millis(50);
            b.sleep_until(deadline).await;
            assert!(a.now() >= deadline);
        });
    }

    #[test]
    fn generic_code_runs_on_smol() {
        block_on(async {
            let rt = SmolRuntime::new();
            let (addr, server) = serve(&rt, |mut sock| async move {
                let mut buf = [0_u8; 1];
                assert_eq!(sock.read(&mut buf).await.unwrap(), 0);
            });
            spawn_connection(rt, addr).await.unwrap();
            server.await.unwrap();
        });
    }
}
