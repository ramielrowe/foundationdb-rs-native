//! The TLS client connector and the stream it produces.

use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_io::{AsyncRead, AsyncWrite};
use rustls::client::Resumption;
use rustls::pki_types::CertificateDer;
use rustls::{CipherSuite, ClientConfig, ProtocolVersion};

use super::config::LoadedTlsConfig;
use super::error::TlsError;
use super::provider::{crypto_provider, placeholder_server_name};
use super::verify_peers::{PeerPolicy, VerifyPeers, VerifyPeersVerifier};

/// Starts TLS client sessions over byte streams, the FoundationDB way.
///
/// A connector is built once from a [`LoadedTlsConfig`] (client certificate,
/// CA bundle, verify-peers rules), then used for every outgoing
/// connection: [`connect`](Self::connect) runs the handshake over any
/// `futures-io` stream, typically a `Runtime::Stream` freshly connected to a
/// `host:port:tls` address. It is cheap to clone and `Send + Sync`.
///
/// Sessions have FoundationDB's client semantics: no SNI, no hostname
/// verification, no ALPN, no session resumption, TLS 1.2 or 1.3. See the
/// [module documentation](super).
#[derive(Clone)]
pub struct TlsConnector {
    inner: futures_rustls::TlsConnector,
    client_certificate: bool,
}

impl TlsConnector {
    /// A connector that verifies servers the way FoundationDB does: the chain
    /// must validate against the CA bundle of `config`, and then the
    /// `verify_peers` rules of `config` ([`LoadedTlsConfig::verify_peers`],
    /// by default `Check.Valid=1`, which is the chain check alone) must accept
    /// the peer's certificates. See [`VerifyPeersVerifier`].
    ///
    /// Errors if there is no CA ([`TlsError::NoTrustAnchors`], the same as
    /// FoundationDB, which then rejects every server), if the CA bundle holds
    /// no usable trust anchor ([`TlsError::InvalidCa`]), if the rules do not
    /// parse ([`TlsError::VerifyPeers`]), or if the client certificate or key
    /// cannot be loaded.
    pub fn new(config: &LoadedTlsConfig) -> Result<Self, TlsError> {
        Self::build(config, None)
    }

    /// Like [`new`](Self::new), with an extra [`PeerPolicy`] that must also
    /// accept the peer. It runs after the chain validation and the rules, which
    /// it cannot bypass.
    pub fn with_policy(
        config: &LoadedTlsConfig,
        policy: Arc<dyn PeerPolicy>,
    ) -> Result<Self, TlsError> {
        Self::build(config, Some(policy))
    }

    fn build(
        config: &LoadedTlsConfig,
        policy: Option<Arc<dyn PeerPolicy>>,
    ) -> Result<Self, TlsError> {
        let rules = VerifyPeers::parse_all(config.verify_peers()).map_err(TlsError::VerifyPeers)?;
        let provider = crypto_provider();
        let roots = config.ca_certificates()?;
        let mut verifier =
            VerifyPeersVerifier::new(rules, roots, &provider).map_err(TlsError::InvalidCa)?;
        if let Some(policy) = policy {
            verifier = verifier.with_policy(policy);
        }
        let identity = config.client_identity()?;
        let builder = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(rustls::DEFAULT_VERSIONS)?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier));
        let client_certificate = identity.is_some();
        let mut client_config = match identity {
            Some((chain, key)) => {
                builder
                    .with_client_auth_cert(chain, key)
                    .map_err(|error| match error {
                        // The provider could not load the key (for example
                        // ECDSA P-521 under ring). Mismatches between key and
                        // certificate are other variants.
                        rustls::Error::General(message) => TlsError::InvalidKey(message),
                        other => TlsError::Rustls(other),
                    })?
            }
            None => builder.with_no_client_auth(),
        };
        // FoundationDB sets no SNI, no ALPN, and never reuses a session.
        client_config.enable_sni = false;
        client_config.alpn_protocols.clear();
        client_config.resumption = Resumption::disabled();
        client_config.enable_early_data = false;
        Ok(Self {
            inner: futures_rustls::TlsConnector::from(Arc::new(client_config)),
            client_certificate,
        })
    }

    /// Whether sessions present a client certificate.
    pub fn has_client_certificate(&self) -> bool {
        self.client_certificate
    }

    /// Runs the TLS client handshake over `stream`, which must already be
    /// connected, and returns the encrypted stream.
    ///
    /// TLS starts immediately: nothing is read or written on `stream` first.
    ///
    /// There is no timeout; race the returned future against a timer (and
    /// drop it to abandon the handshake). Failures are [`io::Error`]s; use
    /// [`rustls_error`] to get at the cause. With TLS 1.3 the handshake
    /// completes from the client's side before the server has validated the
    /// client certificate, so a server that rejects it shows up as an error
    /// on the first read or write instead.
    pub async fn connect<S>(&self, stream: S) -> io::Result<TlsStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let inner = self
            .inner
            .connect(placeholder_server_name(), stream)
            .await?;
        Ok(TlsStream { inner })
    }
}

impl fmt::Debug for TlsConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConnector")
            .field("client_certificate", &self.client_certificate)
            .finish_non_exhaustive()
    }
}

/// The [`rustls::Error`] behind a failed handshake or TLS I/O error, if
/// there is one.
///
/// rustls errors reach callers wrapped in an [`io::Error`] of kind
/// `InvalidData`. Peer rejection (an alert from the server) and certificate
/// validation failures are `rustls::Error`s; plain socket failures are not.
pub fn rustls_error(error: &io::Error) -> Option<&rustls::Error> {
    error.get_ref()?.downcast_ref::<rustls::Error>()
}

/// The alert the peer sent that ended the session, if `error` is one: a server that refuses
/// our client certificate sends `bad_certificate`, `unknown_ca`, `certificate_unknown`,
/// `certificate_required` or `handshake_failure`, depending on what it objected to and its
/// TLS library.
///
/// Reads [`rustls_error`]'s [`rustls::Error::AlertReceived`]. To get it from a failed
/// connection, use [`ConnectionError::Io`](crate::ConnectionError::Io)'s
/// [`IoError::io_error`](crate::IoError::io_error) (or
/// [`PeerError::Tls`](crate::peer::PeerError::Tls)'s `source`).
pub fn alert_received(error: &io::Error) -> Option<rustls::AlertDescription> {
    match rustls_error(error)? {
        rustls::Error::AlertReceived(alert) => Some(*alert),
        _ => None,
    }
}

/// An established TLS client session over `S`.
///
/// Implements the `futures-io` [`AsyncRead`] and [`AsyncWrite`] traits, so
/// the transport can use it wherever it would use the plain stream.
/// `poll_close` sends a TLS `close_notify` alert and then closes `S`; reading
/// the peer's `close_notify` yields `Ok(0)` (EOF). A peer that drops the TCP
/// connection without `close_notify` produces an `UnexpectedEof` error.
pub struct TlsStream<S> {
    inner: futures_rustls::client::TlsStream<S>,
}

impl<S> TlsStream<S> {
    /// The underlying stream.
    pub fn get_ref(&self) -> &S {
        self.inner.get_ref().0
    }

    /// The underlying stream, mutably. Reading or writing it directly
    /// corrupts the TLS session.
    pub fn get_mut(&mut self) -> &mut S {
        self.inner.get_mut().0
    }

    /// The certificate chain the server presented, leaf first.
    pub fn peer_certificates(&self) -> Option<&[CertificateDer<'static>]> {
        self.inner.get_ref().1.peer_certificates()
    }

    /// The negotiated protocol version (TLS 1.2 or 1.3).
    pub fn protocol_version(&self) -> Option<ProtocolVersion> {
        self.inner.get_ref().1.protocol_version()
    }

    /// The negotiated cipher suite.
    pub fn cipher_suite(&self) -> Option<CipherSuite> {
        self.inner
            .get_ref()
            .1
            .negotiated_cipher_suite()
            .map(|suite| suite.suite())
    }

    /// The ALPN protocol the server selected. Always `None`: the connector
    /// offers none.
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.inner.get_ref().1.alpn_protocol()
    }
}

impl<S> fmt::Debug for TlsStream<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsStream")
            .field("protocol_version", &self.protocol_version())
            .field("cipher_suite", &self.cipher_suite())
            .finish_non_exhaustive()
    }
}

impl<S> AsyncRead for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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

    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send_unpin<T: Send + Unpin>() {}

    #[test]
    fn connector_is_send_sync_and_stream_is_send_unpin() {
        assert_send_sync::<TlsConnector>();
        assert_send_unpin::<TlsStream<Vec<u8>>>();
    }
}
