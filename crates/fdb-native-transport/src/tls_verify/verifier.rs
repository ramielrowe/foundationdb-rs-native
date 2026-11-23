//! The rustls certificate verifier that applies `verify_peers` rules.

use std::fmt;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{
    CryptoProvider, WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature,
};
use rustls::pki_types::{CertificateDer, ServerName, TrustAnchor, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, Error, OtherError, SignatureScheme};
use webpki::{EndEntityCert, KeyUsage};

use super::cert::PeerCert;
use super::eval::{RuleFailure, Verdict};
use super::rules::VerifyPeers;

/// A trust anchor from the configured CA certificates, with the full certificate kept so that
/// `R.` criteria can look at it (a webpki [`TrustAnchor`] has only the subject and key).
#[derive(Debug)]
struct Root {
    der: CertificateDer<'static>,
    cert: PeerCert,
}

/// Why [`VerifyPeersVerifier::new`] refused a root certificate.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("root certificate {index} is not usable: {reason}")]
pub struct RootError {
    /// Position among the roots given.
    pub index: usize,
    /// What was wrong with it.
    pub reason: String,
}

/// The peer's chain validated, but no `verify_peers` rule accepted its certificates. It is
/// carried inside the rustls error ([`CertificateError::Other`]) so that an application can
/// downcast to it and report each rule's reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerRejected {
    /// One failure per rule, in order.
    pub failures: Vec<RuleFailure>,
}

impl fmt::Display for PeerRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("peer certificate rejected by verify_peers: ")?;
        for (i, failure) in self.failures.iter().enumerate() {
            if i > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{failure}")?;
        }
        Ok(())
    }
}

impl std::error::Error for PeerRejected {}

/// A rustls [`ServerCertVerifier`] that behaves like FDB's TLS peer verification: it checks
/// the peer's chain against the configured CA certificates, ignoring the server name, and
/// then applies a [`VerifyPeers`] rule set.
///
/// # What is checked
///
/// FDB's verify callback (`TLSPolicy::verify_peer`) first requires that OpenSSL accepted the
/// chain (`preverified`), *whatever the rules say*, and only then evaluates them. So `Check.Valid=0`
/// does **not** let an untrusted or expired chain through on FDB; it only skips the field
/// criteria. This verifier does the same by default:
///
/// 1. **Chain validation** against the roots (webpki, with `serverAuth` usage, ignoring the
///    server name and revocation). The chain is the peer's certificate and the intermediates it
///    sent, ending at one of the roots. A peer certificate that is itself one of the roots and
///    is self-signed is accepted as a one-certificate chain (OpenSSL does the same, and it is
///    how `openssl req -x509` certificates are commonly used with FDB), provided it is within
///    its validity period.
/// 2. **Rules**: [`VerifyPeers::evaluate`] with the peer's certificate and the root the chain
///    ended at.
///
/// `Check.Unexpired` has no effect: FDB never reads it back, and OpenSSL's expiry check is
/// part of step 1.
///
/// [`check_valid_zero_skips_chain_validation`](Self::check_valid_zero_skips_chain_validation)
/// switches to the reading the FDB documentation suggests ("disable all further verification of
/// a peer certificate"): when any rule has `Check.Valid=0` nothing is checked at all, not even
/// the chain.
///
/// # Signatures
///
/// The handshake signature checks (`verify_tls12_signature`, `verify_tls13_signature`) and the
/// advertised signature schemes come from the [`CryptoProvider`] given to
/// [`new`](Self::new).
///
/// This verifier is for the client side of a connection (verifying FDB servers); an FDB
/// client does not accept connections.
#[derive(Debug)]
pub struct VerifyPeersVerifier {
    verify_peers: VerifyPeers,
    roots: Vec<Root>,
    anchors: Vec<TrustAnchor<'static>>,
    algorithms: WebPkiSupportedAlgorithms,
    skip_chain_when_check_valid_zero: bool,
}

impl VerifyPeersVerifier {
    /// A verifier for `verify_peers` against the CA certificates `roots` (DER), using
    /// `provider` for signature algorithms (for example
    /// `rustls::crypto::ring::default_provider()`).
    ///
    /// Every root must parse as an X.509 certificate usable as a trust anchor.
    pub fn new(
        verify_peers: VerifyPeers,
        roots: impl IntoIterator<Item = CertificateDer<'static>>,
        provider: &CryptoProvider,
    ) -> Result<Self, RootError> {
        let mut parsed = Vec::new();
        let mut anchors = Vec::new();
        for (index, der) in roots.into_iter().enumerate() {
            let err = |reason: String| RootError { index, reason };
            let cert = PeerCert::from_der(der.as_ref()).map_err(|e| err(e.to_string()))?;
            let anchor = webpki::anchor_from_trusted_cert(&der)
                .map_err(|e| err(e.to_string()))?
                .to_owned();
            anchors.push(anchor);
            parsed.push(Root { der, cert });
        }
        Ok(VerifyPeersVerifier {
            verify_peers,
            roots: parsed,
            anchors,
            algorithms: provider.signature_verification_algorithms,
            skip_chain_when_check_valid_zero: false,
        })
    }

    /// Chooses what `Check.Valid=0` does (see the [type docs](Self)). The default, `false`, is
    /// FDB's behavior: the chain is still validated and only the field criteria are skipped.
    /// With `true`, a rule set containing `Check.Valid=0` accepts every peer certificate.
    pub fn check_valid_zero_skips_chain_validation(mut self, skip: bool) -> Self {
        self.skip_chain_when_check_valid_zero = skip;
        self
    }

    /// The rules this verifier applies.
    pub fn verify_peers(&self) -> &VerifyPeers {
        &self.verify_peers
    }

    /// Validates the chain and returns the index of the root it ends at.
    fn validate_chain(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<usize, Error> {
        // A self-signed certificate that is itself a configured root.
        if let Some(index) = self
            .roots
            .iter()
            .position(|r| r.cert.is_self_signed() && r.der.as_ref() == end_entity.as_ref())
        {
            let cert = &self.roots[index].cert;
            let now_secs = i64::try_from(now.as_secs()).unwrap_or(i64::MAX);
            return if now_secs < cert.not_before() {
                Err(CertificateError::NotValidYet.into())
            } else if now_secs > cert.not_after() {
                Err(CertificateError::Expired.into())
            } else {
                Ok(index)
            };
        }

        let end_entity = EndEntityCert::try_from(end_entity).map_err(webpki_error)?;
        let path = end_entity
            .verify_for_usage(
                self.algorithms.all,
                &self.anchors,
                intermediates,
                now,
                KeyUsage::server_auth(),
                None,
                None,
            )
            .map_err(webpki_error)?;
        let anchor = path.anchor();
        self.anchors
            .iter()
            .position(|a| std::ptr::eq(a, anchor))
            .ok_or_else(|| Error::General("verified path ended at an unknown trust anchor".into()))
    }
}

impl ServerCertVerifier for VerifyPeersVerifier {
    /// Validates the chain, then applies the rules. `server_name` and `ocsp_response` are
    /// ignored: FDB does not check host names (except for connections made by host name, which
    /// a caller handles itself) or revocation.
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        let disabled = self.verify_peers.check_valid_disabled();
        if disabled && self.skip_chain_when_check_valid_zero {
            return Ok(ServerCertVerified::assertion());
        }

        let root_index = self.validate_chain(end_entity, intermediates, now)?;

        if disabled || self.verify_peers.rules().is_empty() {
            return Ok(ServerCertVerified::assertion());
        }
        let leaf = PeerCert::from_der(end_entity.as_ref())
            .map_err(|_| Error::from(CertificateError::BadEncoding))?;
        let root = self
            .verify_peers
            .needs_root()
            .then(|| &self.roots[root_index].cert);
        match self.verify_peers.evaluate(&leaf, root) {
            Verdict::Accepted(_) => Ok(ServerCertVerified::assertion()),
            Verdict::Rejected(failures) => Err(Error::InvalidCertificate(CertificateError::Other(
                OtherError(Arc::new(PeerRejected { failures })),
            ))),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// The subset of rustls' own webpki error mapping that chain validation here can produce.
fn webpki_error(error: webpki::Error) -> Error {
    use webpki::Error as W;
    #[allow(deprecated)]
    match error {
        W::BadDer | W::BadDerTime | W::TrailingData(_) => CertificateError::BadEncoding.into(),
        W::CertNotValidYet { time, not_before } => {
            CertificateError::NotValidYetContext { time, not_before }.into()
        }
        W::CertExpired { time, not_after } => {
            CertificateError::ExpiredContext { time, not_after }.into()
        }
        W::InvalidCertValidity => CertificateError::Expired.into(),
        W::UnknownIssuer => CertificateError::UnknownIssuer.into(),
        W::InvalidSignatureForPublicKey => CertificateError::BadSignature.into(),
        W::UnsupportedSignatureAlgorithm | W::UnsupportedSignatureAlgorithmForPublicKey => {
            CertificateError::UnsupportedSignatureAlgorithm.into()
        }
        W::RequiredEkuNotFound | W::RequiredEkuNotFoundContext(_) => {
            CertificateError::InvalidPurpose.into()
        }
        other => CertificateError::Other(OtherError(Arc::new(other))).into(),
    }
}
