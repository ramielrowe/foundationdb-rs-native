//! The peer manager's TLS hook: a [`StreamWrapper`] that runs the FoundationDB TLS client
//! handshake over a freshly connected socket.
//!
//! ```no_run
//! # #[cfg(feature = "tokio")]
//! # fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use fdb_native_transport::peer::{PeerManager, PeerManagerConfig, event_channel};
//! use fdb_native_transport::runtime::TokioRuntime;
//! use fdb_native_transport::tls::peer::TlsStreamWrapper;
//! use fdb_native_transport::tls::{TlsConfig, TlsConnector};
//!
//! let loaded = TlsConfig::new().ca_path("/etc/fdb/ca.pem").load()?;
//! let wrapper = TlsStreamWrapper::new(TlsConnector::new(&loaded)?);
//! let (sink, _events) = event_channel();
//! let manager = PeerManager::with_stream_wrapper(
//!     TokioRuntime::new(),
//!     PeerManagerConfig::default(),
//!     wrapper,
//!     sink,
//! );
//! # drop(manager);
//! # Ok(())
//! # }
//! ```
//!
//! # What it does
//!
//! * [`StreamWrapper::tls_enabled`] is `true`: the manager dials `:tls` addresses, and
//!   [`choose_primary_address`](crate::peer::choose_primary_address) compares endpoints
//!   against a TLS process.
//! * [`wrap`](StreamWrapper::wrap) runs [`TlsConnector::connect`] on the connected socket.
//!   The handshake needs no timeout of its own: the manager races it, together with the TCP
//!   connect, against
//!   [`PeerManagerConfig::connect_timeout`](crate::peer::PeerManagerConfig::connect_timeout)
//!   (the future is dropped on expiry, which abandons the handshake; the error is then
//!   [`PeerError::TlsTimeout`](crate::peer::PeerError::TlsTimeout)). A handshake failure,
//!   including a certificate that `verify_peers` rejects, is
//!   [`PeerError::Tls`](crate::peer::PeerError::Tls) with kind `InvalidData`; the manager
//!   never falls back to a plaintext connection.
//! * Frames over the stream carry no checksum: the manager builds the connection with the
//!   address's TLS flag ([`Checksum::for_tls`](crate::Checksum::for_tls)).
//!
//! # Closing and flushing
//!
//! FoundationDB servers close the TCP socket without a TLS `close_notify`, which rustls
//! reports as [`UnexpectedEof`](io::ErrorKind::UnexpectedEof) rather than end of stream (see
//! the [module documentation](super#closing-truncation-and-flushing)). [`PeerTlsStream`]
//! turns that into `Ok(0)`, so the peer task sees an ordinary peer close instead of an I/O
//! error, as it would on a plain socket. Every other error passes through. The peer task
//! flushes after each write pass (`poll_flush`), which is what pushes rustls' buffered
//! records to the socket.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use fdb_native_wire::NetworkAddress;
use futures_io::{AsyncRead, AsyncWrite};

use super::{TlsConnector, TlsStream};
use crate::peer::StreamWrapper;

/// A [`StreamWrapper`] that speaks TLS with a [`TlsConnector`]: give it to
/// [`PeerManager::with_stream_wrapper`](crate::peer::PeerManager::with_stream_wrapper).
#[derive(Clone, Debug)]
pub struct TlsStreamWrapper {
    connector: TlsConnector,
}

impl TlsStreamWrapper {
    /// A wrapper that connects with `connector`.
    pub fn new(connector: TlsConnector) -> Self {
        Self { connector }
    }

    /// The connector.
    pub fn connector(&self) -> &TlsConnector {
        &self.connector
    }
}

impl From<TlsConnector> for TlsStreamWrapper {
    fn from(connector: TlsConnector) -> Self {
        Self::new(connector)
    }
}

impl<S> StreamWrapper<S> for TlsStreamWrapper
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Stream = PeerTlsStream<S>;

    fn tls_enabled(&self) -> bool {
        true
    }

    async fn wrap(&self, stream: S, _peer: NetworkAddress) -> io::Result<PeerTlsStream<S>> {
        // No SNI and no hostname check, so nothing about the peer matters here.
        let inner = self.connector.connect(stream).await?;
        Ok(PeerTlsStream { inner })
    }
}

/// A [`TlsStream`] for the peer task: reading a truncated close (the peer closed the TCP
/// connection without `close_notify`, as FoundationDB does) is end of stream, not an error.
#[derive(Debug)]
pub struct PeerTlsStream<S> {
    inner: TlsStream<S>,
}

impl<S> PeerTlsStream<S> {
    /// The TLS session, for what the handshake negotiated and the peer's certificates.
    pub fn tls(&self) -> &TlsStream<S> {
        &self.inner
    }

    /// Unwraps the TLS stream.
    pub fn into_inner(self) -> TlsStream<S> {
        self.inner
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for PeerTlsStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::UnexpectedEof => Poll::Ready(Ok(0)),
            other => other,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for PeerTlsStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send_unpin<T: Send + Unpin>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn types_are_send() {
        assert_send_sync::<TlsStreamWrapper>();
        assert_send_unpin::<PeerTlsStream<Vec<u8>>>();
    }
}
