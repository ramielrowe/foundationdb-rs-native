//! [`Runtime`] adapter for tokio.
//!
//! Enabled by the default-on `tokio` cargo feature. Needs tokio's `net`,
//! `time` and `rt` features, which this crate's feature enables itself.
//!
//! A [`TokioRuntime`] holds a [`tokio::runtime::Handle`], captured by
//! [`TokioRuntime::new`] or supplied to [`TokioRuntime::with_handle`]. Every
//! method enters that handle where tokio needs a runtime context, so the
//! adapter can be used from threads that are not part of the runtime (for
//! example from `futures_lite::future::block_on`), as long as the runtime
//! itself is alive and running.
//!
//! The clock is tokio's: [`TokioRuntime::now`] and the sleep methods read
//! `tokio::time::Instant`, so `tokio::time::pause`/`advance` and
//! `#[tokio::test(start_paused = true)]` (tokio's `test-util` feature) drive
//! them together. tokio rounds timer deadlines up to the millisecond, so a
//! sleep under paused time can finish up to 1ms past its deadline. Real
//! instants are mapped onto the process-wide [`Instant`] epoch, so two
//! `TokioRuntime`s always agree on `now()`.

use std::future::Future;
use std::io::{self, IoSlice};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use super::{Instant, JoinError, Runtime, TaskHandle};

/// How far in the future a deadline that overflows tokio's clock is clamped.
const FAR_FUTURE: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 30);

/// [`Runtime`] backed by a tokio runtime.
///
/// Cheap to clone. All `TokioRuntime`s in a process share one clock epoch, so
/// their [`Instant`]s are interchangeable.
#[derive(Clone, Debug)]
pub struct TokioRuntime {
    handle: Handle,
}

impl TokioRuntime {
    /// Captures the tokio runtime the caller is running in.
    ///
    /// # Panics
    ///
    /// Panics if called outside a tokio runtime context. Use
    /// [`TokioRuntime::with_handle`] there.
    // No `Default`: it would panic outside a runtime.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        match Handle::try_current() {
            Ok(handle) => Self { handle },
            Err(e) => panic!(
                "TokioRuntime::new must be called inside a tokio runtime \
                 (use TokioRuntime::with_handle otherwise): {e}"
            ),
        }
    }

    /// Uses the tokio runtime behind `handle`.
    pub fn with_handle(handle: Handle) -> Self {
        Self { handle }
    }

    /// The tokio runtime handle this adapter drives.
    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    fn tokio_instant(at: Instant) -> tokio::time::Instant {
        match at.to_std() {
            Some(std_at) => tokio::time::Instant::from_std(std_at),
            None => tokio::time::Instant::now() + FAR_FUTURE,
        }
    }
}

impl Runtime for TokioRuntime {
    type Stream = TokioStream;
    type Task<T: Send + 'static> = TokioTask<T>;

    /// A [`TokioStream`] write is one `send`/`write` call on the socket, which on an error has
    /// written nothing (a partial write returns its count instead).
    const WRITE_ERRORS_TAKE_NOTHING: bool = true;

    /// A [`TokioStream`] write is pending only when the socket's `write` would block, which
    /// wrote nothing.
    const PENDING_TAKES_NOTHING: bool = true;

    /// The same tokio runtime ([`Handle::id`], unique among the runtimes alive, and a handle
    /// keeps its runtime's id from being reused).
    fn same_runtime(&self, other: &Self) -> bool {
        self.handle.id() == other.handle.id()
    }

    fn now(&self) -> Instant {
        // The context decides which clock `tokio::time::Instant::now` reads.
        let _enter = self.handle.enter();
        Instant::from_std(tokio::time::Instant::now().into_std())
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        // `Sleep` captures the timer handle at creation, so it may be polled
        // from outside the runtime afterwards.
        let _enter = self.handle.enter();
        tokio::time::sleep(duration)
    }

    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send {
        let _enter = self.handle.enter();
        tokio::time::sleep_until(Self::tokio_instant(deadline))
    }

    fn spawn<T, F>(&self, future: F) -> TokioTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        TokioTask(self.handle.spawn(future))
    }

    async fn connect(&self, addr: SocketAddr) -> io::Result<TokioStream> {
        let stream = Entered::new(&self.handle, TcpStream::connect(addr)).await?;
        stream.set_nodelay(true)?;
        Ok(TokioStream(stream))
    }

    async fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        let addrs = Entered::new(&self.handle, tokio::net::lookup_host((host, port))).await?;
        Ok(addrs.collect())
    }
}

/// Polls `fut` with a tokio runtime context entered.
///
/// tokio's I/O and `spawn_blocking` register with the *current* runtime when
/// first polled, which would panic on a thread outside the runtime.
struct Entered<F> {
    handle: Handle,
    fut: Pin<Box<F>>,
}

impl<F> Entered<F> {
    fn new(handle: &Handle, fut: F) -> Self {
        Self {
            handle: handle.clone(),
            fut: Box::pin(fut),
        }
    }
}

impl<F: Future> Future for Entered<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = &mut *self;
        let _enter = this.handle.enter();
        this.fut.as_mut().poll(cx)
    }
}

/// A tokio [`TcpStream`] exposed through the `futures_io` traits.
///
/// I/O readiness is registered with the runtime that created the stream, so
/// the stream may be polled from any thread.
#[derive(Debug)]
pub struct TokioStream(TcpStream);

impl TokioStream {
    /// The remote address of the connection.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.0.peer_addr()
    }

    /// The local address of the connection.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }

    /// Unwraps the underlying tokio stream.
    pub fn into_inner(self) -> TcpStream {
        self.0
    }
}

impl AsyncRead for TokioStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut read_buf = tokio::io::ReadBuf::new(buf);
        ready!(tokio::io::AsyncRead::poll_read(
            Pin::new(&mut self.0),
            cx,
            &mut read_buf
        ))?;
        Poll::Ready(Ok(read_buf.filled().len()))
    }
}

impl AsyncWrite for TokioStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(Pin::new(&mut self.0), cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write_vectored(Pin::new(&mut self.0), cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(Pin::new(&mut self.0), cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(Pin::new(&mut self.0), cx)
    }
}

/// Handle to a task spawned on tokio. Dropping it detaches the task.
#[derive(Debug)]
pub struct TokioTask<T>(JoinHandle<T>);

impl<T> Future for TokioTask<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|res| {
            res.map_err(|e| {
                if e.is_panic() {
                    JoinError::Panicked
                } else {
                    JoinError::Aborted
                }
            })
        })
    }
}

impl<T: Send + 'static> TaskHandle<T> for TokioTask<T> {
    fn abort(&self) {
        self.0.abort();
    }

    fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

#[cfg(test)]
mod tests {
    use std::io::IoSlice;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

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
    async fn serve<F, Fut>(rt: &TokioRuntime, serve: F) -> (SocketAddr, TokioTask<()>)
    where
        F: FnOnce(TokioStream) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = rt.spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            serve(TokioStream(sock)).await;
        });
        (addr, task)
    }

    #[tokio::test]
    async fn loopback_round_trip() {
        let rt = TokioRuntime::new();
        // Server echoes 5 bytes, then closes.
        let (addr, server) = serve(&rt, |mut sock| async move {
            let mut buf = [0_u8; 5];
            sock.read_exact(&mut buf).await.unwrap();
            sock.write_all(&buf).await.unwrap();
        })
        .await;

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
        assert!(stream.into_inner().nodelay().unwrap());
    }

    #[tokio::test]
    async fn peer_observes_close() {
        let rt = TokioRuntime::new();
        let (addr, server) = serve(&rt, |mut sock| async move {
            let mut buf = [0_u8; 16];
            let n = sock.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"bye");
            // After the client's close() the peer sees EOF, not an error.
            assert_eq!(sock.read(&mut buf).await.unwrap(), 0);
        })
        .await;

        let mut stream = rt.connect(addr).await.unwrap();
        stream.write_all(b"bye").await.unwrap();
        stream.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn vectored_write_reaches_peer() {
        let rt = TokioRuntime::new();
        let (addr, server) = serve(&rt, |mut sock| async move {
            let mut got = Vec::new();
            sock.read_to_end(&mut got).await.unwrap();
            assert_eq!(got, b"foobarbaz");
        })
        .await;

        let mut stream = rt.connect(addr).await.unwrap();
        let bufs = [
            IoSlice::new(b"foo"),
            IoSlice::new(b""),
            IoSlice::new(b"bar"),
            IoSlice::new(b"baz"),
        ];
        let mut written = 0;
        while written < 9 {
            // Loopback may accept a partial vectored write; resume from there.
            let mut remaining = bufs;
            let mut slices: &mut [IoSlice<'_>] = &mut remaining;
            IoSlice::advance_slices(&mut slices, written);
            let n = stream.write_vectored(slices).await.unwrap();
            assert!(n > 0);
            written += n;
        }
        stream.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn short_read_into_small_buffer() {
        let rt = TokioRuntime::new();
        let (addr, server) = serve(&rt, |mut sock| async move {
            sock.write_all(b"0123456789").await.unwrap();
            sock.close().await.unwrap();
        })
        .await;

        let mut stream = rt.connect(addr).await.unwrap();
        let mut small = [0_u8; 3];
        let n = stream.read(&mut small).await.unwrap();
        assert!((1..=3).contains(&n), "read {n} bytes into a 3-byte buffer");
        assert_eq!(&small[..n], &b"0123456789"[..n]);
        // The remainder is still delivered, in order.
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.unwrap();
        assert_eq!(rest, &b"0123456789"[n..]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn connect_refused_is_an_error() {
        let rt = TokioRuntime::new();
        // Bind then drop to find a port nothing listens on.
        let addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        };
        let err = rt.connect(addr).await.map(drop).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
    }

    #[tokio::test(start_paused = true)]
    async fn sleep_uses_paused_clock() {
        let rt = TokioRuntime::new();
        let wall = std::time::Instant::now();
        let t0 = rt.now();

        rt.sleep(Duration::from_secs(30)).await;
        let elapsed = rt.now() - t0;
        assert!(elapsed >= Duration::from_secs(30), "{elapsed:?}");
        assert!(elapsed <= Duration::from_millis(30_001), "{elapsed:?}");

        // sleep_until is absolute on the same clock.
        rt.sleep_until(t0 + Duration::from_secs(90)).await;
        let elapsed = rt.now() - t0;
        assert!(elapsed >= Duration::from_secs(90), "{elapsed:?}");
        assert!(elapsed <= Duration::from_millis(90_001), "{elapsed:?}");

        // A deadline in the past completes without advancing time.
        let before = rt.now();
        rt.sleep_until(t0).await;
        assert_eq!(rt.now(), before);

        // Virtual time: 90s passed on the clock, ~none on the wall.
        assert!(wall.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_sleep_loser_is_cancelled() {
        let rt = TokioRuntime::new();
        let t0 = rt.now();
        let cancelled = Arc::new(AtomicBool::new(false));

        let short = async {
            rt.sleep(Duration::from_millis(100)).await;
            "short"
        };
        let flag = Arc::clone(&cancelled);
        let long = async {
            let _guard = SetOnDrop(flag);
            rt.sleep(Duration::from_secs(3600)).await;
            "long"
        };
        // `or` drops the loser as soon as the winner completes.
        let winner = futures_lite::future::or(short, long).await;

        assert_eq!(winner, "short");
        assert!(cancelled.load(Ordering::SeqCst), "loser was dropped");
        assert!(rt.now() - t0 < Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn far_future_deadline_does_not_panic() {
        let rt = TokioRuntime::new();
        let never = rt.sleep_until(Instant::from_origin(Duration::MAX));
        tokio::pin!(never);
        tokio::select! {
            () = &mut never => panic!("far-future sleep completed"),
            () = rt.sleep(Duration::from_secs(1)) => {}
        }
    }

    #[tokio::test]
    async fn spawn_returns_output() {
        let rt = TokioRuntime::new();
        let task = rt.spawn(async { 21 * 2 });
        assert_eq!(task.await, Ok(42));
    }

    #[tokio::test(start_paused = true)]
    async fn spawn_and_abort() {
        let rt = TokioRuntime::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = SetOnDrop(Arc::clone(&dropped));
        let task = {
            let rt = rt.clone();
            rt.clone().spawn(async move {
                let _guard = guard;
                rt.sleep(Duration::from_secs(3600)).await;
            })
        };
        // Let the task start and park on its sleep.
        rt.sleep(Duration::from_millis(1)).await;
        assert!(!task.is_finished());
        assert!(!dropped.load(Ordering::SeqCst));

        task.abort();
        task.abort(); // idempotent
        assert_eq!(task.await, Err(JoinError::Aborted));
        assert!(dropped.load(Ordering::SeqCst), "aborted task was dropped");
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_handle_detaches() {
        let rt = TokioRuntime::new();
        let done = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&done);
        let task = {
            let rt = rt.clone();
            rt.clone().spawn(async move {
                rt.sleep(Duration::from_secs(10)).await;
                flag.store(true, Ordering::SeqCst);
            })
        };
        drop(task);
        rt.sleep(Duration::from_secs(11)).await;
        assert!(
            done.load(Ordering::SeqCst),
            "detached task ran to completion"
        );
    }

    #[tokio::test]
    async fn panicking_task_reports_panic() {
        let rt = TokioRuntime::new();
        let task = rt.spawn(async { panic!("boom") });
        let err = task.await.unwrap_err();
        assert!(err.is_panic());
    }

    #[tokio::test]
    async fn resolves_localhost() {
        let rt = TokioRuntime::new();
        let addrs = rt.resolve("localhost", 4500).await.unwrap();
        assert!(!addrs.is_empty());
        assert!(
            addrs
                .iter()
                .all(|a| a.port() == 4500 && a.ip().is_loopback())
        );
    }

    #[tokio::test]
    async fn resolves_literals_and_reports_errors() {
        let rt = TokioRuntime::new();
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
    }

    #[tokio::test(start_paused = true)]
    async fn separate_runtimes_share_one_clock() {
        let a = TokioRuntime::new();
        a.sleep(Duration::from_millis(300)).await;
        let b = TokioRuntime::new();
        let (ta, tb) = (a.now(), b.now());
        let skew = if ta > tb { ta - tb } else { tb - ta };
        assert!(skew <= Duration::from_millis(1), "{skew:?}");

        // A deadline from one is honoured by the other: it must not fire early.
        let deadline = a.now() + Duration::from_secs(5);
        b.sleep_until(deadline).await;
        assert!(a.now() >= deadline);
    }

    #[test]
    fn usable_from_threads_outside_the_runtime() {
        let tokio_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let rt = TokioRuntime::with_handle(tokio_rt.handle().clone());

        // This thread is not inside the runtime: no ambient context.
        assert!(Handle::try_current().is_err());
        futures_lite::future::block_on(async {
            let t0 = rt.now();
            rt.sleep(Duration::from_millis(20)).await;
            assert!(rt.now() - t0 >= Duration::from_millis(20));

            let addrs = rt.resolve("localhost", 4500).await.unwrap();
            assert!(!addrs.is_empty());

            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut s, _) = listener.accept().unwrap();
                io::Write::write_all(&mut s, b"ok").unwrap();
            });
            let mut stream = rt.connect(addr).await.unwrap();
            let mut got = [0_u8; 2];
            stream.read_exact(&mut got).await.unwrap();
            assert_eq!(&got, b"ok");
            server.join().unwrap();

            assert_eq!(rt.spawn(async { 7 }).await, Ok(7));
        });
    }

    #[test]
    #[should_panic(expected = "TokioRuntime::new must be called inside a tokio runtime")]
    fn new_outside_runtime_panics_with_clear_message() {
        let _ = TokioRuntime::new();
    }

    #[tokio::test]
    async fn generic_code_runs_on_tokio() {
        let rt = TokioRuntime::new();
        let (addr, server) = serve(&rt, |mut sock| async move {
            let mut buf = [0_u8; 1];
            assert_eq!(sock.read(&mut buf).await.unwrap(), 0);
        })
        .await;
        // `spawn_connection` connects, sleeps briefly, then drops the stream.
        spawn_connection(rt, addr).await.unwrap();
        server.await.unwrap();
    }
}
