//! Errors raised while loading TLS material and building a connector.

use std::io;
use std::path::PathBuf;

/// Why TLS configuration could not be loaded or a connector could not be
/// built.
///
/// Handshake failures are not reported through this type: they surface as
/// [`io::Error`] from [`TlsConnector::connect`](super::TlsConnector::connect),
/// and [`rustls_error`](super::rustls_error) recovers the underlying
/// [`rustls::Error`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TlsError {
    /// A configured certificate, key or CA file could not be read.
    #[error("cannot read TLS {what} file {}: {source}", path.display())]
    ReadFile {
        /// Which material: `"certificate"`, `"key"` or `"CA"`.
        what: &'static str,
        /// The file that failed.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },
    /// A configured file is larger than FoundationDB's `CERT_FILE_MAX_SIZE`
    /// knob (5 MiB) allows.
    #[error("TLS {what} file {} is larger than {limit} bytes", path.display())]
    FileTooLarge {
        /// Which material: `"certificate"`, `"key"` or `"CA"`.
        what: &'static str,
        /// The file that failed.
        path: PathBuf,
        /// The size limit that was exceeded, in bytes.
        limit: u64,
    },
    /// The PEM text of a certificate, key or CA could not be decoded.
    #[error("invalid PEM in TLS {what}: {source}")]
    Pem {
        /// Which material: `"certificate"`, `"key"` or `"CA"`.
        what: &'static str,
        /// The decode error.
        #[source]
        source: rustls_pki_types::pem::Error,
    },
    /// The certificate or CA PEM held no `CERTIFICATE` section.
    #[error("no certificates found in the TLS {what} PEM")]
    NoCertificates {
        /// Which material: `"certificate"` or `"CA"`.
        what: &'static str,
    },
    /// The key PEM held no private key (PKCS#8, PKCS#1 or SEC1).
    #[error("no private key found in the TLS key PEM")]
    NoPrivateKey,
    /// A client certificate was configured without a private key.
    ///
    /// FoundationDB tolerates this and silently sends no certificate. This
    /// crate reports it, because the resulting connection would be an
    /// untrusted peer.
    #[error("a TLS certificate was configured without a private key")]
    CertificateWithoutKey,
    /// A private key was configured without a certificate.
    #[error("a TLS private key was configured without a certificate")]
    KeyWithoutCertificate,
    /// The private key is encrypted and no password was configured
    /// (`TLS_password` / `FDB_TLS_PASSWORD`).
    #[error("the TLS private key is encrypted but no TLS password is configured")]
    KeyPasswordRequired,
    /// The private key uses OpenSSL's legacy PEM-level encryption
    /// (`Proc-Type: 4,ENCRYPTED`), which is not supported.
    ///
    /// Convert it to an encrypted PKCS#8 key:
    /// `openssl pkcs8 -topk8 -in old.pem -out key.pem`.
    #[error(
        "the TLS private key uses legacy PEM encryption (Proc-Type: 4,ENCRYPTED), which is not \
         supported; convert it with `openssl pkcs8 -topk8`"
    )]
    LegacyEncryptedKey,
    /// The encrypted PKCS#8 key could not be decrypted: wrong password, or an
    /// encryption scheme other than PBES2 with AES-CBC.
    #[error("cannot decrypt the TLS private key (wrong password, or unsupported scheme): {0}")]
    KeyDecrypt(String),
    /// No CA certificates were configured, so there is nothing to validate a
    /// server chain against.
    ///
    /// FoundationDB has no default trust store either; a client with no CA
    /// rejects every server. Supply `TLS_ca_path` / `TLS_ca_bytes` /
    /// `FDB_TLS_CA_FILE`, or install a custom verifier with
    /// [`TlsConnector::with_verifier`](super::TlsConnector::with_verifier).
    #[error("no TLS CA certificates are configured; servers cannot be validated")]
    NoTrustAnchors,
    /// A CA certificate was rejected by the trust-anchor parser.
    #[error("invalid TLS CA certificate: {0}")]
    InvalidCa(#[source] rustls::Error),
    /// The default verifier could not be built.
    #[error("cannot build the TLS certificate verifier: {0}")]
    Verifier(#[from] rustls::client::VerifierBuilderError),
    /// rustls rejected the configuration (for example a key that does not
    /// match the certificate).
    #[error("invalid TLS configuration: {0}")]
    Rustls(#[from] rustls::Error),
}
