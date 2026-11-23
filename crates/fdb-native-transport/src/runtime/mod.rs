//! The runtime abstraction the transport is written against.
//!
//! [`Runtime`] bundles the four things the transport needs from an async
//! executor: TCP connect, timers (with a monotonic clock), task spawning, and
//! DNS resolution. No executor-specific type appears in it, so the transport
//! can run on any executor that has an adapter. The tokio adapter,
//! `TokioRuntime`, is behind the default-on `tokio` cargo feature.
//!
//! # Design
//!
//! * **Static dispatch.** The trait uses return-position `impl Future` and a
//!   generic associated type, so it is not object-safe. Transport types are
//!   generic over `R: Runtime`. This avoids boxing every future on the hot
//!   path and lets the stream type stay concrete.
//! * **Byte streams are [`futures_io`] traits.** [`Runtime::Stream`] is
//!   `futures_io::AsyncRead + AsyncWrite`, the lowest-common-denominator
//!   async I/O traits that smol, async-std and `futures-rustls` all speak.
//!   M7 can wrap a stream in a TLS session without touching this trait.
//!   tokio's own I/O traits are adapted inside the tokio adapter's stream.
//! * **Own [`Instant`].** The clock is a plain [`Duration`] since a
//!   process-wide epoch so that virtual clocks (tokio's paused time,
//!   [`ManualClock`]) are representable and every runtime in the process
//!   agrees on time. See [`Instant`].
//! * **`Send` executors only.** Futures are `Send`, so thread-local (`!Send`)
//!   executors such as `tokio::task::LocalSet` or smol's `LocalExecutor` are
//!   not supported.
//!
//! # Contract
//!
//! ## Bounds
//!
//! A `Runtime` is `Clone + Send + Sync + 'static`. Clones are cheap handles to
//! the same executor and share one clock. Every future returned by a method
//! is `Send` (the transport spawns work onto multi-threaded executors) but is
//! not required to be `'static`; it may borrow from `&self` or from arguments.
//! Spawned futures must be `Send + 'static`. [`Runtime::Stream`] is
//! `Send + Unpin + 'static`.
//!
//! ## Cancellation
//!
//! Dropping any future returned by a [`Runtime`] method before it completes
//! abandons the operation and releases its resources, with no other effect.
//!
//! * A dropped [`connect`](Runtime::connect) closes any half-open socket.
//! * A dropped [`resolve`](Runtime::resolve) abandons the lookup. An
//!   implementation may let a blocking `getaddrinfo` finish in the background,
//!   but its result is discarded.
//! * A dropped sleep simply cancels the timer.
//!
//! There is no built-in timeout combinator. To bound `connect` or `resolve`,
//! race it against [`sleep`](Runtime::sleep) and drop the loser.
//!
//! Dropping a [`TaskHandle`] **detaches** the task: it keeps running to
//! completion. Cancelling a task requires an explicit [`TaskHandle::abort`].
//! An aborted task is dropped at its next suspension point: destructors run,
//! but no further code from the task body does.
//!
//! Notes for adapter authors:
//!
//! * An adapter whose native task cancels on drop (smol's `Task`) must
//!   detach in its handle's `Drop`.
//! * `abort` takes `&self`, but such native handles consume `self` to cancel.
//!   Wrap the task in an abortable future (an abort flag or channel the body
//!   observes) rather than relying on the native handle.
//! * A panic in the task must be caught (`catch_unwind`, or the executor's own
//!   mechanism) and surface as [`JoinError::Panicked`] when the handle is
//!   awaited; it must not tear down the executor or the awaiting task.
//!
//! ## Clocks
//!
//! [`Runtime::now`] is monotonic: successive calls never go backwards, and it
//! is unaffected by wall-clock adjustments. [`sleep`](Runtime::sleep),
//! [`sleep_until`](Runtime::sleep_until) and `now` all read the *same* clock,
//! so under a virtual clock they stay consistent with one another.
//! `sleep(d)` fixes its deadline when the method is called (not at first
//! poll) and is equivalent to `sleep_until(now() + d)`. A sleep never
//! completes before its deadline on that clock; it may complete somewhat
//! later (tokio, for instance, rounds up to a millisecond). A deadline in the
//! past completes at once.
//!
//! Sleeps drift if chained: `sleep(period)` in a loop adds the loop body's
//! run time to every cycle. Schedule periodic work from deadlines instead:
//! `next += period; sleep_until(next)`.
//!
//! ## Testing against virtual time
//!
//! For code that awaits timers, run it under tokio's paused clock (a
//! `#[tokio::test(start_paused = true)]`, or `tokio::time::pause()` on a
//! current-thread runtime). The tokio adapter reads that clock, and sleeps
//! auto-advance it when the runtime is otherwise idle, so backoff and
//! ping-timeout tests finish instantly and deterministically. For sans-IO
//! state machines that take a `now: Instant` argument, drive them with a
//! [`ManualClock`] instead.
//!
//! ## Stream behaviour
//!
//! [`Runtime::connect`] returns a stream with Nagle's algorithm disabled
//! (`TCP_NODELAY`), as the FoundationDB client does; the transport frames and
//! batches its own writes. Reads and writes follow the usual `futures_io`
//! rules: `poll_read` returning `Ok(0)` for a non-empty buffer means EOF, and
//! `poll_close` performs a graceful shutdown of the write half.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use futures_io::{AsyncRead, AsyncWrite};

mod time;

#[cfg(feature = "tokio")]
mod tokio_rt;

pub use time::{Instant, ManualClock};
#[cfg(feature = "tokio")]
pub use tokio_rt::{TokioRuntime, TokioStream, TokioTask};

/// Capabilities the transport needs from an async executor.
///
/// See the [module documentation](self) for the full contract.
pub trait Runtime: Clone + Send + Sync + 'static {
    /// A connected, bidirectional byte stream.
    ///
    /// Not necessarily a plain TCP socket: M7 layers TLS on top by wrapping
    /// this type.
    type Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static;

    /// Handle to a task started by [`spawn`](Runtime::spawn), yielding `T`.
    type Task<T: Send + 'static>: TaskHandle<T>;

    /// The current reading of this runtime's monotonic clock.
    fn now(&self) -> Instant;

    /// Completes `duration` from now. Equivalent to
    /// `sleep_until(self.now() + duration)`.
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send;

    /// Completes once the clock reaches `deadline`; immediately if it already
    /// has.
    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send;

    /// Runs `future` to completion on the executor as an independent task.
    ///
    /// Dropping the returned handle detaches the task; call
    /// [`TaskHandle::abort`] to cancel it. Must be called from within the
    /// executor's context for adapters that require one (tokio: inside a
    /// runtime).
    fn spawn<T, F>(&self, future: F) -> Self::Task<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;

    /// Opens a TCP connection to `addr`, with `TCP_NODELAY` set.
    ///
    /// There is no timeout; the OS connect timeout applies unless the caller
    /// races this against [`sleep`](Runtime::sleep).
    fn connect(&self, addr: SocketAddr) -> impl Future<Output = io::Result<Self::Stream>> + Send;

    /// Resolves `host` to every address it maps to, each paired with `port`,
    /// in resolver order. `host` is a DNS name or an IP literal *without*
    /// brackets (`localhost`, `10.0.0.1`, `::1`). Callers should treat an empty
    /// `Vec` as "no addresses".
    ///
    /// IP literals resolve without I/O.
    fn resolve(
        &self,
        host: &str,
        port: u16,
    ) -> impl Future<Output = io::Result<Vec<SocketAddr>>> + Send;
}

/// A handle to a spawned task.
///
/// Awaiting the handle yields the task's output, or a [`JoinError`] if it was
/// aborted or panicked. Dropping the handle detaches the task (it is *not*
/// cancelled).
pub trait TaskHandle<T>: Future<Output = Result<T, JoinError>> + Send + Unpin + 'static {
    /// Requests cancellation. The task is dropped at its next suspension
    /// point and the handle then resolves to [`JoinError::Aborted`]. Has no
    /// effect if the task already finished. Idempotent.
    fn abort(&self);

    /// Whether the task has finished, by completion, panic or abort.
    fn is_finished(&self) -> bool;
}

/// Why a spawned task did not produce a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum JoinError {
    /// The task was cancelled with [`TaskHandle::abort`] (or its executor
    /// shut down) before completing.
    #[error("task was aborted")]
    Aborted,
    /// The task panicked.
    #[error("task panicked")]
    Panicked,
}

impl JoinError {
    /// True for [`JoinError::Aborted`].
    pub fn is_aborted(&self) -> bool {
        matches!(self, JoinError::Aborted)
    }

    /// True for [`JoinError::Panicked`].
    pub fn is_panic(&self) -> bool {
        matches!(self, JoinError::Panicked)
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::*;

    /// A runtime that implements the trait without touching tokio: proves the
    /// trait surface is executor-neutral and implementable by a second
    /// adapter. Time comes from a [`ManualClock`]; I/O and tasks do nothing.
    #[derive(Clone, Debug, Default)]
    struct NullRuntime(ManualClock);

    #[derive(Debug)]
    struct NullStream;

    impl AsyncRead for NullStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(0))
        }
    }

    impl AsyncWrite for NullStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Debug)]
    struct NullTask<T>(PhantomData<fn() -> T>);

    impl<T> Future for NullTask<T> {
        type Output = Result<T, JoinError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Err(JoinError::Aborted))
        }
    }

    impl<T: Send + 'static> TaskHandle<T> for NullTask<T> {
        fn abort(&self) {}
        fn is_finished(&self) -> bool {
            true
        }
    }

    impl Runtime for NullRuntime {
        type Stream = NullStream;
        type Task<T: Send + 'static> = NullTask<T>;

        fn now(&self) -> Instant {
            self.0.now()
        }
        async fn sleep(&self, duration: Duration) {
            self.0.advance(duration);
        }
        async fn sleep_until(&self, deadline: Instant) {
            let now = self.0.now();
            self.0.advance(deadline - now);
        }
        fn spawn<T, F>(&self, _: F) -> NullTask<T>
        where
            F: Future<Output = T> + Send + 'static,
            T: Send + 'static,
        {
            NullTask(PhantomData)
        }
        async fn connect(&self, _: SocketAddr) -> io::Result<NullStream> {
            Ok(NullStream)
        }
        async fn resolve(&self, _: &str, _: u16) -> io::Result<Vec<SocketAddr>> {
            Ok(Vec::new())
        }
    }

    /// Generic code written against the trait, as the transport will be.
    async fn backoff_then_connect<R: Runtime>(rt: &R, addr: SocketAddr) -> io::Result<R::Stream> {
        rt.sleep(Duration::from_millis(50)).await;
        rt.connect(addr).await
    }

    /// Generic code of the shape the transport will have: spawns a task that
    /// owns the runtime handle and a stream. This fails to compile if the
    /// trait's `Send`/`'static` bounds regress.
    pub(crate) fn spawn_connection<R: Runtime>(rt: R, addr: SocketAddr) -> R::Task<()> {
        let rt2 = rt.clone();
        rt.spawn(async move {
            let stream = rt2.connect(addr).await.unwrap();
            rt2.sleep(Duration::from_millis(1)).await;
            drop(stream);
        })
    }

    #[test]
    fn trait_is_implementable_without_tokio() {
        let rt = NullRuntime::default();
        let addr: SocketAddr = "127.0.0.1:4500".parse().unwrap();
        futures_lite::future::block_on(async {
            backoff_then_connect(&rt, addr).await.unwrap();
            let task = rt.spawn(async { 7_u8 });
            assert_eq!(task.await, Err(JoinError::Aborted));
            let task = spawn_connection(rt.clone(), addr);
            assert_eq!(task.await, Err(JoinError::Aborted));
        });
        assert_eq!(rt.now(), Instant::from_origin(Duration::from_millis(50)));
    }

    #[test]
    fn join_error_predicates() {
        assert!(JoinError::Aborted.is_aborted());
        assert!(!JoinError::Aborted.is_panic());
        assert!(JoinError::Panicked.is_panic());
    }
}
