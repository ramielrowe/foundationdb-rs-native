//! The rustls certificate verifier that applies `verify_peers` rules.

use std::cell::Cell;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

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
use super::selfsig::{self, SelfSignature};

/// A self-signed certificate from the CA file, with the full certificate kept so that `R.`
/// criteria can look at it (a webpki [`TrustAnchor`] has only the subject and key).
#[derive(Debug)]
struct Root {
    der: CertificateDer<'static>,
    cert: PeerCert,
}

/// Why [`VerifyPeersVerifier::new`] refused the CA certificates.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RootError {
    /// A certificate does not parse, or webpki cannot use it as a trust anchor.
    #[error("root certificate {index} is not usable: {reason}")]
    Invalid {
        /// Position among the certificates given.
        index: usize,
        /// What was wrong with it.
        reason: String,
    },
    /// None of the certificates is a self-signed root, so no chain can end anywhere. As in
    /// OpenSSL, only a self-signed certificate is a trust anchor; the others (intermediate
    /// CAs) are used only to complete a chain to one.
    #[error(
        "none of the {provided} CA certificates is a valid trust anchor: a trust anchor must be \
         self-signed (subject equal to issuer) and, to issue other certificates, a CA; an \
         intermediate CA certificate alone cannot anchor a chain, so include the root that \
         issued it"
    )]
    NoAnchors {
        /// How many certificates were given.
        provided: usize,
    },
}

/// A peer certificate that this verifier cannot accept although OpenSSL, which FDB uses, can.
/// It is carried inside the rustls error ([`CertificateError::Other`]); each message says what
/// to change.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CertificateRejected {
    /// The peer's certificate is a CA certificate (`basicConstraints` `CA:TRUE`) that is not
    /// itself a self-signed certificate of the CA file (typically it was issued by another
    /// CA). webpki refuses a CA certificate as an end entity.
    #[error(
        "the peer's certificate is a CA certificate (basicConstraints CA:TRUE) and is not a \
         self-signed certificate from the CA file; this TLS stack does not accept a CA \
         certificate as a server certificate although OpenSSL does. Reissue it as an \
         end-entity certificate: X.509 version 3 with basicConstraints CA:FALSE (for example \
         `openssl x509 -req ... -extfile` with `basicConstraints = CA:FALSE`). A self-signed \
         certificate that is itself in the CA file is accepted"
    )]
    CaUsedAsEndEntity,
    /// A certificate of the chain is X.509 version 1 (or 2). webpki accepts only version 3.
    #[error(
        "a certificate in the peer's chain (or the CA file) is not X.509 version 3, which this \
         TLS stack requires although OpenSSL accepts version 1; reissue it as version 3 (an \
         end-entity with basicConstraints CA:FALSE, a CA with basicConstraints CA:TRUE). A \
         self-signed version 1 certificate that is itself in the CA file is accepted"
    )]
    UnsupportedVersion,
    /// The peer's certificate is not valid for TLS server use (OpenSSL's `ssl_server`
    /// purpose): the keyUsage or extendedKeyUsage does not allow it.
    #[error("the peer's certificate is not valid for TLS server use: {0}")]
    Purpose(&'static str),
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

/// An extra check on a peer whose chain validated and whose certificates a `verify_peers` rule
/// accepted, run last by [`VerifyPeersVerifier`].
///
/// It can only make verification stricter: the chain validation and the rules always run
/// first and cannot be bypassed. Use it for policy FDB does not have, such as checking a
/// fingerprint or a name. The server name rustls passes to a verifier is a placeholder, so a
/// policy that wants a host name must carry its own.
pub trait PeerPolicy: fmt::Debug + Send + Sync {
    /// Accepts the peer (`Ok`) or rejects it with a rustls error (for example
    /// `rustls::CertificateError::ApplicationVerificationFailure`). `end_entity` and
    /// `intermediates` are what the peer sent, in order.
    fn check(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
    ) -> Result<(), Error>;
}

/// A rustls [`ServerCertVerifier`] that behaves like FDB's TLS peer verification: it checks
/// the peer's chain against the configured CA certificates, ignoring the server name, and
/// then applies a [`VerifyPeers`] rule set.
///
/// # What is checked
///
/// FDB's verify callback (`TLSPolicy::verify_peer`) first requires that OpenSSL accepted the
/// chain (`preverified`), *whatever the rules say*, and only then evaluates them. So `Check.Valid=0`
/// does **not** let an untrusted or expired chain through; it only skips the field criteria.
/// This verifier does the same:
///
/// 1. **Chain validation** against the CA certificates, following OpenSSL's use of its trust
///    store: only a *self-signed* CA certificate (`CA:TRUE`, or a self-signed v1 certificate;
///    a keyUsage, if present, must include `keyCertSign`) is a trust anchor, and its validity
///    period is checked. Every other certificate in the CA file is offered, like a
///    certificate the peer sent, as a possible intermediate. The path must be valid for
///    `serverAuth` and the server name is ignored; revocation is not checked.
/// 2. **Purpose** of the peer's certificate (OpenSSL's `ssl_server`): its keyUsage, if
///    present, must include `digitalSignature`, `keyEncipherment` or `keyAgreement`.
/// 3. **Rules**: [`VerifyPeers::evaluate`] with the peer's certificate and the self-signed
///    root the chain ended at.
///
/// A peer certificate that is itself a self-signed certificate from the CA file is accepted
/// as a one-certificate chain if it is in date and passes the purpose check (the way
/// `openssl req -x509` certificates are commonly used with FDB, with the certificate as its
/// own CA file).
///
/// `Check.Unexpired` has no effect: FDB never reads it back, and the expiry check is part of
/// step 1.
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
    /// Every self-signed certificate of the CA file.
    roots: Vec<Root>,
    /// The CA-capable subset, as webpki anchors, parallel to `anchor_roots`.
    anchors: Vec<TrustAnchor<'static>>,
    /// For each anchor, its index in `roots`.
    anchor_roots: Vec<usize>,
    /// The rest of the CA file: candidate intermediates.
    extra_intermediates: Vec<CertificateDer<'static>>,
    algorithms: WebPkiSupportedAlgorithms,
    policy: Option<Arc<dyn PeerPolicy>>,
}

impl VerifyPeersVerifier {
    /// A verifier for `verify_peers` against the CA certificates `roots` (DER), using
    /// `provider` for signature algorithms (for example
    /// `rustls::crypto::ring::default_provider()`).
    ///
    /// Each certificate must parse. Those that are self-signed (and whose signature does not
    /// contradict it) are the trust anchors, if CA-capable; the others become candidate
    /// intermediates. At least one self-signed certificate is required
    /// ([`RootError::NoAnchors`]).
    pub fn new(
        verify_peers: VerifyPeers,
        roots: impl IntoIterator<Item = CertificateDer<'static>>,
        provider: &CryptoProvider,
    ) -> Result<Self, RootError> {
        let algorithms = provider.signature_verification_algorithms;
        let mut parsed = Vec::new();
        let mut anchors = Vec::new();
        let mut anchor_roots = Vec::new();
        let mut extra = Vec::new();
        let mut provided = 0;
        for (index, der) in roots.into_iter().enumerate() {
            provided += 1;
            let invalid = |reason: String| RootError::Invalid { index, reason };
            let cert = PeerCert::from_der(der.as_ref()).map_err(|e| invalid(e.to_string()))?;
            let self_signed = cert.is_self_signed()
                && selfsig::check(der.as_ref(), algorithms.all) != SelfSignature::Invalid;
            if !self_signed {
                extra.push(der);
                continue;
            }
            if cert.can_be_anchor() {
                let anchor = webpki::anchor_from_trusted_cert(&der)
                    .map_err(|e| invalid(e.to_string()))?
                    .to_owned();
                anchors.push(anchor);
                anchor_roots.push(parsed.len());
            }
            parsed.push(Root { der, cert });
        }
        if parsed.is_empty() {
            return Err(RootError::NoAnchors { provided });
        }
        Ok(VerifyPeersVerifier {
            verify_peers,
            roots: parsed,
            anchors,
            anchor_roots,
            extra_intermediates: extra,
            algorithms,
            policy: None,
        })
    }

    /// Adds a [`PeerPolicy`] that runs after the chain validation and the rules have
    /// accepted a peer, replacing any earlier one.
    #[must_use]
    pub fn with_policy(mut self, policy: Arc<dyn PeerPolicy>) -> Self {
        self.policy = Some(policy);
        self
    }

    /// The rules this verifier applies.
    pub fn verify_peers(&self) -> &VerifyPeers {
        &self.verify_peers
    }

    /// Validates the chain and returns the index (in `roots`) of the self-signed root it
    /// ends at.
    fn validate_chain(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<usize, Error> {
        // A self-signed certificate that is itself in the CA file.
        if let Some(index) = self
            .roots
            .iter()
            .position(|r| r.der.as_ref() == end_entity.as_ref())
        {
            check_validity(&self.roots[index].cert, now).map_err(webpki_error)?;
            return Ok(index);
        }

        let end_entity = EndEntityCert::try_from(end_entity).map_err(webpki_error)?;
        let candidates: Vec<CertificateDer<'_>> = intermediates
            .iter()
            .chain(&self.extra_intermediates)
            .cloned()
            .collect();
        // An anchor is also checked for its validity period, as OpenSSL does; a path that
        // fails here is abandoned and webpki tries the others.
        let chosen = Cell::new(None);
        let verify_path = |path: &webpki::VerifiedPath<'_>| {
            let index = self
                .anchors
                .iter()
                .position(|a| std::ptr::eq(a, path.anchor()))
                .ok_or(webpki::Error::UnknownIssuer)?;
            let root = self.anchor_roots[index];
            check_validity(&self.roots[root].cert, now)?;
            chosen.set(Some(root));
            Ok(())
        };
        end_entity
            .verify_for_usage(
                self.algorithms.all,
                &self.anchors,
                &candidates,
                now,
                KeyUsage::server_auth(),
                None,
                Some(&verify_path),
            )
            .map_err(webpki_error)?;
        chosen
            .get()
            .ok_or_else(|| Error::General("verified path ended at an unknown trust anchor".into()))
    }
}

/// An anchor's (or self-signed peer's) validity period against `now`.
fn check_validity(cert: &PeerCert, now: UnixTime) -> Result<(), webpki::Error> {
    let secs = |s: i64| UnixTime::since_unix_epoch(Duration::from_secs(s.max(0).unsigned_abs()));
    let now_secs = i64::try_from(now.as_secs()).unwrap_or(i64::MAX);
    if now_secs < cert.not_before() {
        Err(webpki::Error::CertNotValidYet {
            time: now,
            not_before: secs(cert.not_before()),
        })
    } else if now_secs > cert.not_after() {
        Err(webpki::Error::CertExpired {
            time: now,
            not_after: secs(cert.not_after()),
        })
    } else {
        Ok(())
    }
}

fn rejected(why: CertificateRejected) -> Error {
    Error::InvalidCertificate(CertificateError::Other(OtherError(Arc::new(why))))
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
        let root_index = self.validate_chain(end_entity, intermediates, now)?;

        let leaf = PeerCert::from_der(end_entity.as_ref())
            .map_err(|_| Error::from(CertificateError::BadEncoding))?;
        if let Some(problem) = leaf.tls_server_purpose_problem() {
            return Err(rejected(CertificateRejected::Purpose(problem)));
        }

        if !(self.verify_peers.check_valid_disabled() || self.verify_peers.rules().is_empty()) {
            let root = self
                .verify_peers
                .needs_root()
                .then(|| &self.roots[root_index].cert);
            if let Verdict::Rejected(failures) = self.verify_peers.evaluate(&leaf, root) {
                return Err(Error::InvalidCertificate(CertificateError::Other(
                    OtherError(Arc::new(PeerRejected { failures })),
                )));
            }
        }
        if let Some(policy) = &self.policy {
            policy.check(end_entity, intermediates)?;
        }
        Ok(ServerCertVerified::assertion())
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

/// The subset of rustls' own webpki error mapping that chain validation here can produce,
/// plus clear errors for the certificates OpenSSL accepts and webpki does not.
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
        W::CaUsedAsEndEntity => rejected(CertificateRejected::CaUsedAsEndEntity),
        W::UnsupportedCertVersion => rejected(CertificateRejected::UnsupportedVersion),
        other => CertificateError::Other(OtherError(Arc::new(other))).into(),
    }
}
