//! The hook that layers TLS on a connected socket.

use std::future::Future;
use std::io::{self, IoSlice, IoSliceMut};
use std::pin::Pin;
use std::task::{Context, Poll};

use fdb_native_wire::NetworkAddress;
use futures_io::{AsyncRead, AsyncWrite};

/// Turns a freshly connected socket into the byte stream a connection to a **TLS address**
/// speaks over: it performs the TLS handshake and returns the encrypted stream. (Plain
/// addresses never go through it.) M7's TLS support is one implementation.
///
/// ```ignore
/// struct Rustls(TlsConnector);
///
/// impl<S: AsyncRead + AsyncWrite + Send + Unpin + 'static> StreamWrapper<S> for Rustls {
///     type Stream = futures_rustls::client::TlsStream<S>;
///     fn tls_enabled(&self) -> bool { true }
///     async fn wrap(&self, stream: S, peer: NetworkAddress) -> io::Result<Self::Stream> {
///         self.0.connect(server_name_for(&peer), stream).await
///     }
/// }
/// ```
///
/// A plain `async` closure-like function also works: any
/// `Fn(S, NetworkAddress) -> impl Future<Output = io::Result<T>>` (with `T` a duplex byte
/// stream) is a `StreamWrapper` that reports TLS as enabled.
///
/// The returned future must be cancel-safe in the usual sense (dropping it abandons the
/// handshake): the manager races it against
/// [`connect_timeout`](super::PeerManagerConfig::connect_timeout) and against shutdown. It is
/// not called for an address without the TLS flag, nor at all when
/// [`tls_enabled`](Self::tls_enabled) is false (the connection then fails with
/// [`PeerError::TlsNotConfigured`](super::PeerError::TlsNotConfigured) without dialing).
pub trait StreamWrapper<S>: Send + Sync + 'static {
    /// The stream a TLS connection reads and writes. Frames over it carry no checksum
    /// ([`Checksum::for_tls`](crate::Checksum::for_tls)), so it must authenticate.
    ///
    /// **Writes and at-most-once.** The peer manager decides from what `poll_write` returns
    /// which requests certainly never left this process (see
    /// [Written offsets](crate::connection#written-offsets)). `Ok(n)` must mean that the
    /// stream took exactly the first `n` bytes (taken bytes count as delivered, even if the
    /// session never sends them). An error or `Ok(0)` is never trusted to have taken nothing:
    /// a TLS session can take plaintext and then fail on the socket (`futures-rustls` does),
    /// so every byte offered to the write (at most 64 KiB) counts as possibly delivered. A
    /// `Pending` is trusted to have taken nothing only with
    /// [`PENDING_TAKES_NOTHING`](Self::PENDING_TAKES_NOTHING).
    type Stream: AsyncRead + AsyncWrite + Send + Unpin + 'static;

    /// Whether a `poll_write` on [`Stream`](Self::Stream) that returns `Pending` took none of
    /// the buffer it was given and sent none of it. With `false` (the default, always safe)
    /// every byte offered to a pending write counts as possibly delivered. Leave it `false` for
    /// a session that can put records on the wire and still return `Pending`, as an OpenSSL
    /// `SSL_write` without `SSL_MODE_ENABLE_PARTIAL_WRITE` does when it hits `WANT_WRITE`.
    /// `tls::peer::TlsStreamWrapper` (feature `tls`) sets it: `futures-rustls` returns
    /// `Pending` only when its session took nothing.
    const PENDING_TAKES_NOTHING: bool = false;

    /// Whether this process speaks TLS: the TLS-ness [`choose_primary_address`](super::choose_primary_address)
    /// compares endpoints against, and whether TLS addresses
    /// can be connected to.
    fn tls_enabled(&self) -> bool;

    /// Performs the TLS handshake over `stream`, a connected socket to `peer`.
    fn wrap(
        &self,
        stream: S,
        peer: NetworkAddress,
    ) -> impl Future<Output = io::Result<Self::Stream>> + Send;
}

impl<S, T, F, Fut> StreamWrapper<S> for F
where
    S: Send + 'static,
    F: Fn(S, NetworkAddress) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = io::Result<T>> + Send,
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Stream = T;

    fn tls_enabled(&self) -> bool {
        true
    }

    fn wrap(&self, stream: S, peer: NetworkAddress) -> impl Future<Output = io::Result<T>> + Send {
        self(stream, peer)
    }
}

/// The default [`StreamWrapper`]: no TLS. Plain addresses connect as usual; a TLS address
/// fails with [`PeerError::TlsNotConfigured`](super::PeerError::TlsNotConfigured).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoTls;

impl<S: AsyncRead + AsyncWrite + Send + Unpin + 'static> StreamWrapper<S> for NoTls {
    type Stream = S;

    fn tls_enabled(&self) -> bool {
        false
    }

    async fn wrap(&self, _stream: S, _peer: NetworkAddress) -> io::Result<S> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "TLS is not configured",
        ))
    }
}

/// The stream of a connection: the plain socket for a plain address, the wrapper's stream for
/// a TLS one.
#[derive(Debug)]
pub enum MaybeTls<A, B> {
    /// A plain socket.
    Plain(A),
    /// A TLS stream.
    Tls(B),
}

impl<A: AsyncRead + Unpin, B: AsyncRead + Unpin> AsyncRead for MaybeTls<A, B> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_read_vectored(cx, bufs),
            MaybeTls::Tls(s) => Pin::new(s).poll_read_vectored(cx, bufs),
        }
    }
}

impl<A: AsyncWrite + Unpin, B: AsyncWrite + Unpin> AsyncWrite for MaybeTls<A, B> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            MaybeTls::Tls(s) => Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_close(cx),
            MaybeTls::Tls(s) => Pin::new(s).poll_close(cx),
        }
    }
}
