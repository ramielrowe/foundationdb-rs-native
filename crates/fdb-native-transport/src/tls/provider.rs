//! The rustls crypto provider and the placeholder server name.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use rustls::crypto::CryptoProvider;
use rustls::pki_types::ServerName;

/// The crypto provider every connector in this module uses: `ring`, or
/// `aws-lc-rs` when the `tls-aws-lc-rs` feature is enabled.
///
/// Exposed so a custom [`ServerCertVerifier`](rustls::client::danger::ServerCertVerifier)
/// can verify handshake signatures with the same algorithms.
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

/// The server name handed to a [`ServerCertVerifier`](rustls::client::danger::ServerCertVerifier).
///
/// FoundationDB connects by IP address and never checks the certificate
/// against a name, so there is no meaningful value. The placeholder is the
/// unspecified IPv4 address; verifiers must ignore it. It is never sent on the
/// wire (SNI is disabled).
pub(crate) fn placeholder_server_name() -> ServerName<'static> {
    ServerName::IpAddress(IpAddr::V4(Ipv4Addr::UNSPECIFIED).into())
}
