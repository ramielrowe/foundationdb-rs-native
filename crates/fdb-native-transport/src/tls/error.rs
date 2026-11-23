//! Errors raised while loading TLS material and building a connector.

use std::io;
use std::path::PathBuf;

use super::verify_peers::{ParseError, RootError};

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
    /// The encrypted PKCS#8 key is in a supported scheme but did not decrypt:
    /// wrong password or a corrupt key.
    #[error("cannot decrypt the TLS private key: {0}")]
    KeyDecrypt(String),
    /// The encrypted PKCS#8 key uses a scheme this crate cannot decrypt (PBES1,
    /// scrypt, or an unknown cipher). The message names it.
    ///
    /// Supported: PBES2 with PBKDF2 (HMAC-SHA1 or SHA-2) and AES-CBC or
    /// 3DES-CBC.
    #[error("unsupported TLS private key encryption: {0}")]
    UnsupportedKeyEncryption(String),
    /// rustls could not load the private key as a signing key.
    ///
    /// The key parsed as PEM/DER but is of a type the crypto provider does not
    /// support. With the default `ring` provider that includes ECDSA P-521
    /// keys (and P-521 certificate signatures); build with the
    /// `tls-aws-lc-rs` feature to use them.
    #[error(
        "the TLS private key is not usable: {0} (ECDSA P-521 needs the `tls-aws-lc-rs` feature)"
    )]
    InvalidKey(String),
    /// No CA certificates were configured, so there is nothing to validate a
    /// server chain against.
    ///
    /// FoundationDB has no default trust store either; a client with no CA
    /// rejects every server. Supply `TLS_ca_path` / `TLS_ca_bytes` /
    /// `FDB_TLS_CA_FILE`.
    #[error("no TLS CA certificates are configured; servers cannot be validated")]
    NoTrustAnchors,
    /// The CA certificates cannot be used: one does not parse, or none of them
    /// is a self-signed root that can anchor a chain (an intermediate CA
    /// certificate alone cannot; include the root).
    #[error("invalid TLS CA certificates: {0}")]
    InvalidCa(#[source] RootError),
    /// The configured `verify_peers` rules do not parse.
    #[error("invalid TLS verify_peers setting: {0}")]
    VerifyPeers(#[source] ParseError),
    /// `FDB_TLS_VERIFY_PEERS` is set to a value that is not valid UTF-8.
    ///
    /// FDB reads the variable as bytes. This crate parses the rules as text and
    /// will not guess: a lossy conversion would replace each invalid byte with
    /// U+FFFD and so change what the rule matches. Write non-ASCII bytes of a
    /// rule as `\XX` escapes instead.
    #[error(
        "FDB_TLS_VERIFY_PEERS is not valid UTF-8; write non-ASCII bytes of a rule as \\XX escapes"
    )]
    VerifyPeersNotUtf8,
    /// rustls rejected the configuration (for example a key that does not
    /// match the certificate).
    #[error("invalid TLS configuration: {0}")]
    Rustls(#[from] rustls::Error),
}
