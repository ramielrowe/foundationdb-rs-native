//! Server certificate verification: the default chain-only verifier.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};

use super::config::LoadedTlsConfig;
use super::error::TlsError;

/// The crypto provider every connector in this module uses: `ring`, or
/// `aws-lc-rs` when the `tls-aws-lc-rs` feature is enabled.
///
/// Exposed so a custom [`ServerCertVerifier`] can verify handshake
/// signatures with the same algorithms.
pub fn crypto_provider() -> Arc<CryptoProvider> {
    provider_impl()
}

#[cfg(not(feature = "tls-aws-lc-rs"))]
fn provider_impl() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

#[cfg(feature = "tls-aws-lc-rs")]
fn provider_impl() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// The server name handed to a [`ServerCertVerifier`].
///
/// FoundationDB connects by IP address and never checks the certificate
/// against a name, so there is no meaningful value. The placeholder is the
/// unspecified IPv4 address; verifiers must ignore it. It is never sent on the
/// wire (SNI is disabled).
pub(crate) fn placeholder_server_name() -> ServerName<'static> {
    ServerName::IpAddress(IpAddr::V4(Ipv4Addr::UNSPECIFIED).into())
}

/// Validates a server's certificate chain against the configured CA
/// certificates, and nothing else.
///
/// This is FoundationDB's `Check.Valid=1` behaviour on its own: signatures,
/// validity period and chain building to a configured trust anchor, but **no
/// hostname or IP check** and none of the verify-peers field rules.
///
/// It is the default verifier of [`TlsConnector::new`](super::TlsConnector::new)
/// and the building block for custom ones: a verifier that adds extra rules
/// (verify-peers) calls [`verify_server_cert`](ServerCertVerifier::verify_server_cert)
/// on a `ChainVerifier` first and applies its own checks to the chain it
/// accepted.
///
/// Built on rustls' `WebPkiServerVerifier`, so the usual webpki constraints
/// apply (see the [module documentation](super#differences-from-openssl)).
#[derive(Debug)]
pub struct ChainVerifier {
    inner: Arc<WebPkiServerVerifier>,
}

impl ChainVerifier {
    /// A verifier trusting the CA bundle of `config`.
    ///
    /// Errors with [`TlsError::NoTrustAnchors`] if `config` has no CA.
    pub fn from_config(config: &LoadedTlsConfig) -> Result<Self, TlsError> {
        Self::from_certificates(config.ca_certificates()?)
    }

    /// A verifier trusting exactly `roots`.
    pub fn from_certificates(roots: Vec<CertificateDer<'static>>) -> Result<Self, TlsError> {
        if roots.is_empty() {
            return Err(TlsError::NoTrustAnchors);
        }
        let mut store = RootCertStore::empty();
        for root in roots {
            store.add(root).map_err(TlsError::InvalidCa)?;
        }
        let inner = WebPkiServerVerifier::builder_with_provider(Arc::new(store), crypto_provider())
            .build()?;
        Ok(Self { inner })
    }
}

impl ServerCertVerifier for ChainVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        // rustls' verifier validates the chain first and the name last
        // (`WebPkiServerVerifier::verify_server_cert`), so a name error means
        // the chain was already accepted. Anything else is a real failure.
        let result = self.inner.verify_server_cert(
            end_entity,
            intermediates,
            &placeholder_server_name(),
            ocsp_response,
            now,
        );
        match result {
            Err(Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
