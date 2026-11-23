//! [`Runtime`] adapter for tokio.
//!
//! Enabled by the default-on `tokio` cargo feature. Needs tokio's `net`,
//! `time` and `rt` features, which this crate's feature enables itself.
//!
//! All methods must be called from within a tokio runtime context (inside a
//! task, or under `Runtime::enter`/`block_on`), as with `tokio::spawn` and
//! `tokio::time::sleep` themselves.
//!
//! The clock is tokio's: [`TokioRuntime::now`] and the sleep methods read
//! `tokio::time::Instant`, so `tokio::time::pause`/`advance` and
//! `#[tokio::test(start_paused = true)]` (tokio's `test-util` feature) drive
//! them together. tokio rounds timer deadlines up to the millisecond, so a
//! sleep under paused time can finish up to 1ms past its deadline.

use std::future::Future;
use std::io::{self, IoSlice};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use super::{Instant, JoinError, Runtime, TaskHandle};

/// How far in the future a deadline that overflows tokio's clock is clamped.
const FAR_FUTURE: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 30);

/// [`Runtime`] backed by the ambient tokio runtime.
///
/// Cheap to clone; clones share an origin, so their [`Instant`]s are
/// comparable. Two separately constructed `TokioRuntime`s have different
/// origins and their instants must not be mixed.
#[derive(Clone, Debug)]
pub struct TokioRuntime {
    /// Reading of tokio's clock that corresponds to [`Instant::ZERO`].
    origin: tokio::time::Instant,
}

impl TokioRuntime {
    /// Creates an adapter whose clock starts at tokio's current time.
    ///
    /// Create it inside the runtime whose clock it should follow (this matters
    /// only for paused-time tests, where each runtime has its own clock).
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }

    fn tokio_instant(&self, at: Instant) -> tokio::time::Instant {
        self.origin
            .checked_add(at.since_origin())
            .unwrap_or_else(|| tokio::time::Instant::now() + FAR_FUTURE)
    }
}

impl Default for TokioRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime for TokioRuntime {
    type Stream = TokioStream;
    type Task<T: Send + 'static> = TokioTask<T>;

    fn now(&self) -> Instant {
        Instant::from_origin(tokio::time::Instant::now().saturating_duration_since(self.origin))
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(duration)
    }

    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send {
        tokio::time::sleep_until(self.tokio_instant(deadline))
    }

    fn spawn<T, F>(&self, future: F) -> TokioTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        TokioTask(tokio::spawn(future))
    }

    async fn connect(&self, addr: SocketAddr) -> io::Result<TokioStream> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(TokioStream(stream))
    }

    async fn resolve(&self, host_port: &str) -> io::Result<Vec<SocketAddr>> {
        Ok(tokio::net::lookup_host(host_port).await?.collect())
    }
}

/// A tokio [`TcpStream`] exposed through the `futures_io` traits.
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures_lite::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    /// A value is dropped when a task holding it is dropped.
    struct SetOnDrop(Arc<AtomicBool>);

    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn loopback_round_trip() {
        let rt = TokioRuntime::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Server echoes 5 bytes, then closes.
        let server = rt.spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut sock = TokioStream(sock);
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
        assert!(stream.into_inner().nodelay().unwrap());
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
        assert_eq!(t0, Instant::ZERO);

        rt.sleep(Duration::from_secs(30)).await;
        let t1 = rt.now();
        let elapsed = t1 - t0;
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
    async fn sleep_loses_a_race_when_dropped() {
        let rt = TokioRuntime::new();
        let t0 = rt.now();
        // Race a long sleep against a short one; the loser is dropped.
        let short = rt.sleep(Duration::from_millis(100));
        let long = rt.sleep(Duration::from_secs(3600));
        tokio::pin!(short, long);
        tokio::select! {
            () = &mut short => {}
            () = &mut long => panic!("long sleep should not win"),
        }
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
        let addrs = rt.resolve("localhost:4500").await.unwrap();
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
            rt.resolve("10.1.2.3:4500").await.unwrap(),
            ["10.1.2.3:4500".parse::<SocketAddr>().unwrap()]
        );
        assert_eq!(
            rt.resolve("[::1]:4501").await.unwrap(),
            ["[::1]:4501".parse::<SocketAddr>().unwrap()]
        );
        // Missing port.
        assert!(rt.resolve("localhost").await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn clones_share_a_clock() {
        let a = TokioRuntime::new();
        let b = a.clone();
        a.sleep(Duration::from_secs(1)).await;
        assert_eq!(a.now(), b.now());
    }
}
