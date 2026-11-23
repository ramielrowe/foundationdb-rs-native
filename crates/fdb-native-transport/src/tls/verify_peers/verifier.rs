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
use super::profile;
use super::rules::VerifyPeers;
use super::selfsig::{self, SelfSignature};

/// A self-signed certificate from the CA file, with the full certificate kept so that `R.`
/// criteria and the OpenSSL-profile path checks can look at it (a webpki [`TrustAnchor`] has
/// only the subject, key and name constraints; the anchor's `pathLenConstraint` is in
/// [`cert`](Self::cert)'s profile).
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
        "none of the {provided} CA certificates is a self-signed root: a trust anchor must be \
         self-signed (subject equal to issuer, an authority key identifier, if any, that does \
         not name another key, and well-formed extensions); an intermediate CA certificate \
         alone cannot anchor a chain, so include the root that issued it"
    )]
    NoAnchors {
        /// How many certificates were given.
        provided: usize,
    },
}

/// A peer certificate that this verifier cannot accept although OpenSSL, which FDB uses, can
/// (or, for [`UnhandledCriticalExtension`](Self::UnhandledCriticalExtension) and
/// [`Purpose`](Self::Purpose), one both refuse, with a reason worth spelling out). It is
/// carried inside the rustls error ([`CertificateError::Other`]); each message says what to
/// change.
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
    /// The peer's certificate, or an intermediate of its chain, is X.509 version 1 (or 2).
    /// webpki, and so rustls, accepts only version 3 for these.
    ///
    /// This includes a self-signed version 1 certificate that is itself in the CA file and is
    /// presented as the server certificate: OpenSSL would accept it, but the handshake could
    /// not complete (rustls parses the server's certificate with webpki), so it is refused up
    /// front with this error. A self-signed version 1 certificate in the CA file is still a
    /// fine trust anchor for other certificates.
    #[error(
        "a certificate in the peer's chain is not X.509 version 3, which this TLS stack \
         requires for the server's certificate and its intermediates although OpenSSL accepts \
         version 1; reissue it as version 3 (an end-entity with basicConstraints CA:FALSE, a \
         CA with basicConstraints CA:TRUE). A self-signed version 1 certificate in the CA file \
         still works as a trust anchor for version 3 certificates"
    )]
    UnsupportedVersion,
    /// The peer's certificate is a self-signed certificate from the CA file that has a
    /// critical extension OpenSSL does not support (`unhandled critical extension`).
    #[error(
        "the peer's certificate has a critical extension that is not one of the extensions \
         OpenSSL's verifier supports, so OpenSSL (and this verifier) refuse it; reissue it \
         without marking that extension critical"
    )]
    UnhandledCriticalExtension,
    /// The peer's certificate is not valid for TLS server use (OpenSSL's `ssl_server`
    /// purpose): the keyUsage or extendedKeyUsage does not allow it.
    #[error("the peer's certificate is not valid for TLS server use: {0}")]
    Purpose(&'static str),
    /// The peer's certificate has an RFC 3779 IP address or AS number extension
    /// (`ipAddrBlock`, `autonomousSysNum`). OpenSSL accepts one when every certificate up the
    /// chain covers the resources it lists; this verifier does not evaluate them and refuses
    /// the certificate.
    #[error(
        "the peer's certificate has an RFC 3779 IP address or AS number extension \
         (ipAddrBlock or autonomousSysNum), which this verifier does not evaluate although \
         OpenSSL does; reissue the server certificate without it"
    )]
    IpAddressOrAsResources,
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
/// 1. **Chain validation** against the CA certificates, by `rustls-webpki` with OpenSSL's own
///    checks (`X509_verify_cert`, purpose `sslserver`, 3.x) added, so that nothing is accepted
///    that `openssl verify -purpose sslserver` rejects, with the exceptions listed under
///    "Differences". The server name is ignored and revocation is not checked.
///
///    *Self-signed* means what OpenSSL's `EXFLAG_SS` means ([`PeerCert::is_self_signed`]):
///    subject and issuer compare equal as `X509_NAME_cmp` compares them (canonically, so ASCII
///    case, runs of white space and the string type do not matter), the authority key
///    identifier, if present, does not contradict the certificate itself (a key identifier
///    that differs from its own subject key identifier, or an issuer name or serial number
///    that differs from its own), and the signature algorithm is one for the certificate's own
///    kind of key. The signature itself is not part of it.
///
///    *Invalid* certificates are those OpenSSL marks `EXFLAG_INVALID`: one of
///    `basicConstraints`, `keyUsage`, `extendedKeyUsage`, `nsCertType`, `subjectKeyIdentifier`,
///    `authorityKeyIdentifier`, `subjectAltName`, `nameConstraints`, `cRLDistributionPoints`,
///    `proxyCertInfo` and the RFC 3779 `ipAddrBlock` and `autonomousSysNum` appears twice or
///    does not decode (as OpenSSL decodes it), the `pathLenConstraint` is negative, or the
///    `keyUsage` has no bit set. OpenSSL can link such a certificate neither to an issuer nor
///    to a subject, so no path through one is accepted, wherever it is: peer, intermediate,
///    trust anchor or the root-as-peer below. *Proxy certificates* (`proxyCertInfo`) are
///    refused wherever they are too, as FDB does not set `X509_V_FLAG_ALLOW_PROXY_CERTS`.
///
///    *Trust anchors.* Only a self-signed CA-file certificate that is neither invalid nor a
///    proxy certificate can be one. It must also be a CA (`CA:TRUE`, or a self-signed v1
///    certificate; a keyUsage, if present, must include `keyCertSign`), have no *critical*
///    extension outside OpenSSL 3.5's supported list (`nsCertType`, `keyUsage`,
///    `subjectAltName`, `basicConstraints`, `certificatePolicies`, `cRLDistributionPoints`,
///    `extendedKeyUsage`, the RFC 3779 `ipAddrBlock` and `autonomousSysNum`,
///    `id-pkix-ocsp-nocheck`, `policyConstraints`, `proxyCertInfo`, `nameConstraints`,
///    `policyMappings` and `inhibitAnyPolicy`; `X509_supported_extension` in
///    `crypto/x509/v3_purp.c`), and, if it has an `extendedKeyUsage`, that must include
///    `serverAuth` (OpenSSL 3 checks the anchor like any certificate of the chain; 1.1.1
///    did not). A self-signed certificate that fails these is not an anchor: certificates
///    chained to it are rejected as having an unknown issuer. Its validity period is checked.
///    Every other certificate in the CA file is offered, like a certificate the peer sent, as
///    a possible intermediate.
///
///    *The peer's certificate* must not be invalid, a proxy certificate, or self-signed
///    (OpenSSL's "self-signed certificate", error 18: it does not look for the issuer of a
///    self-signed certificate, so a leaf that takes its root's name and has no key identifiers
///    of its own is refused although the root signed it), unless it is itself a configured
///    root (below).
///
///    *Each path* webpki builds (signatures, validity, `basicConstraints`, `extendedKeyUsage`,
///    intermediates' `pathLenConstraint`, the name constraints that apply to the leaf's
///    alternative names) is then checked, and abandoned in favour of another candidate if
///    it fails, for:
///    * no certificate of the path being invalid or a proxy certificate;
///    * no intermediate being self-signed (OpenSSL stops building the chain there: "self-signed
///      certificate in certificate chain", error 19) unless it is itself in the CA file, where
///      OpenSSL would take it as the trust anchor. A key-rollover certificate, whose authority
///      key identifier names the key above it, is not self-signed;
///    * the trust anchor's `pathLenConstraint`, which a webpki trust anchor does not carry: at
///      most that many intermediates that are not self-issued (subject and issuer
///      byte-identical, a stricter reading than OpenSSL's canonical comparison);
///    * every intermediate being a CA by OpenSSL's `check_ca`, i.e. a `keyUsage`, if present,
///      has `keyCertSign` (webpki asks only for `CA:TRUE`);
///    * every certificate's authority key identifier fitting its issuer in the path
///      (`X509_check_akid`: key identifier; issuer name, compared canonically; serial number,
///      compared by its content octets, so `00 FF` is not `FF`);
///    * the name constraints (`dNSName` and `rfc822Name` subtrees) of the intermediates and the
///      anchor, applied as OpenSSL's `check_name_constraints` does to the names webpki does not
///      look at: `emailAddress` attributes of every certificate's subject, and, for the peer's
///      certificate when it has no `dNSName` alternative name, each `commonName` that looks
///      like a host name (`NAME_CONSTRAINTS_check_CN`: two or more labels of letters, digits,
///      `_` and interior `-`) as a DNS name. A `commonName` that cannot be converted to
///      UTF-8 or has a NUL that is not trailing, or an `emailAddress` that is not an
///      `IA5String`, is refused as OpenSSL refuses it; so is a certificate whose names
///      (subject attributes plus alternative names) times a constraining CA's subtrees (of
///      every type) exceed 2^20 (`NAME_CHECK_MAX`).
/// 2. **Purpose** of the peer's certificate (OpenSSL's `ssl_server`): its extendedKeyUsage, if
///    present, must include `serverAuth` (or the legacy Netscape and Microsoft server-gated
///    crypto OIDs; `anyExtendedKeyUsage` alone is not enough, as in OpenSSL 3.5), its keyUsage,
///    if present, must include `digitalSignature`, `keyEncipherment` or `keyAgreement`, and
///    its Netscape certificate type (`nsCertType`), if present, must include `sslServer`.
/// 3. **Rules**: [`VerifyPeers::evaluate`] with the peer's certificate and the self-signed
///    root the chain ended at.
///
/// A peer certificate that is itself a self-signed certificate from the CA file (byte for
/// byte, and neither invalid nor a proxy certificate) is accepted as a one-certificate chain
/// if it is in date, has no critical extension outside the list above, and passes the purpose
/// check (the way `openssl req -x509` certificates are commonly used with FDB, with the
/// certificate as its own CA file). This is the only way a self-signed peer certificate is
/// accepted. It must be X.509 version 3 ([`CertificateRejected::UnsupportedVersion`]
/// otherwise): OpenSSL would accept a version 1 certificate there, but rustls parses the
/// server's certificate with webpki, so the handshake would fail anyway.
///
/// `Check.Unexpired` has no effect: FDB never reads it back, and the expiry check is part of
/// step 1.
///
/// # Differences
///
/// * *Stricter* than OpenSSL, because of webpki: see [`CertificateRejected`], and also
///   * a CA-file certificate whose subject equals its issuer but whose own signature does not
///     verify (not treated as a self-signed root);
///   * a critical extension webpki does not process, such as `certificatePolicies` on an
///     intermediate;
///   * name constraints webpki does not evaluate and refuses instead: a `directoryName`
///     constraint anywhere in the path (every certificate beneath it is refused, permitted
///     or excluded); an `rfc822Name` alternative name under `rfc822Name` constraints, and a
///     `uniformResourceIdentifier` one under `uniformResourceIdentifier` constraints, even
///     when the name satisfies them; a DNS name that webpki finds malformed (an empty label, a
///     space or NUL, a misplaced dot or wildcard) in an alternative name or a constraint,
///     under DNS constraints;
///   * an intermediate whose `extendedKeyUsage` lists only the server-gated-crypto OIDs
///     (`nsSGC`, `msSGC`) without `serverAuth`: OpenSSL accepts it, webpki asks for
///     `serverAuth`.
/// * *Stricter* than OpenSSL by choice, where emulating it is not worth the code:
///   * an `id-on-SmtpUTF8Mailbox` alternative name under any `rfc822Name` constraint is
///     refused (OpenSSL compares it with `nc_email_eai`, converting the constraint from
///     A-labels to U-labels);
///   * RFC 3779 IP address or AS number resources on the peer's certificate are refused
///     ([`CertificateRejected::IpAddressOrAsResources`]; OpenSSL checks that the chain covers
///     them). On an intermediate or the anchor they are ignored, as by OpenSSL;
///   * BER encodings that OpenSSL's decoder tolerates in the extensions it decodes for every
///     certificate (a constructed OCTET or character string, an indefinite length) make the
///     certificate invalid here. No certificate tool writes them.
/// * *Certificates OpenSSL cannot decode at all* (`d2i_X509` fails: an OID with a
///   non-minimal subidentifier, a name attribute that is not valid UTF-8 or that is an
///   INTEGER, and the like) are not refused for that alone; webpki and x509-parser accept
///   them, and every other check applies. This is accepted: such a certificate cannot be
///   what an FDB server presents, since the server loads its own certificate with OpenSSL,
///   and it still has to chain, with valid signatures, to a configured anchor and pass every
///   check above, so it lets no certificate in that a configured CA did not issue.
/// * *Path building*: OpenSSL builds one path, choosing each issuer by name and, where the
///   certificates carry them, key identifiers. webpki backtracks over every candidate with
///   a matching name. A chain can therefore be accepted that OpenSSL cannot build: two roots
///   with one subject and no key identifiers when OpenSSL tries the wrong one first, or a
///   cross-signed intermediate sent in both forms where one leads to a configured root.
///   This is accepted: the accepted path still ends at a configured, in-date, CA-capable
///   trust anchor and has passed every check above.
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
            // OpenSSL can use neither an invalid certificate (`EXFLAG_INVALID`) nor a proxy
            // certificate as a trust anchor or as the peer, so such a certificate is no root;
            // as a candidate intermediate it fails the path checks.
            let self_signed = cert.is_self_signed()
                && !cert.is_invalid()
                && !cert.is_proxy()
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

    /// Whether `der` is byte-identical to a certificate of the CA file.
    fn is_in_ca_file(&self, der: &[u8]) -> bool {
        self.roots.iter().any(|r| r.der.as_ref() == der)
            || self.extra_intermediates.iter().any(|c| c.as_ref() == der)
    }

    /// Validates the chain and returns the index (in `roots`) of the self-signed root it
    /// ends at. `leaf` is `end_entity` parsed.
    fn validate_chain(
        &self,
        end_entity: &CertificateDer<'_>,
        leaf: &PeerCert,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<usize, Error> {
        // A self-signed certificate that is itself in the CA file: a chain of one. OpenSSL
        // checks it as an end-entity, so it needs the same freedom from unsupported critical
        // extensions as any certificate; and rustls will parse it with webpki when the
        // handshake continues, which only takes version 3.
        if let Some(index) = self
            .roots
            .iter()
            .position(|r| r.der.as_ref() == end_entity.as_ref())
        {
            let root = &self.roots[index].cert;
            if !root.is_v3() {
                return Err(rejected(CertificateRejected::UnsupportedVersion));
            }
            if root.has_unsupported_critical_extension() {
                return Err(rejected(CertificateRejected::UnhandledCriticalExtension));
            }
            if root.has_ip_or_as_resources() {
                return Err(rejected(CertificateRejected::IpAddressOrAsResources));
            }
            check_validity(root, now).map_err(webpki_error)?;
            return Ok(index);
        }

        // What OpenSSL refuses of the peer's certificate whatever the path: a malformed or
        // duplicated extension of the kinds it decodes (it then finds no issuer), a proxy
        // certificate, and a self-signed certificate that is not itself in the CA file
        // ("self-signed certificate", error 18: OpenSSL does not look for an issuer of a
        // self-signed certificate, so one whose names and key identifiers mimic a root's is
        // not taken as issued by that root). RFC 3779 resources are refused as well, as this
        // verifier does not evaluate them.
        if leaf.is_invalid() {
            return Err(CertificateError::BadEncoding.into());
        }
        if leaf.is_proxy() {
            return Err(webpki_error(webpki::Error::ExtensionValueInvalid));
        }
        if leaf.is_self_signed() {
            return Err(CertificateError::UnknownIssuer.into());
        }
        if leaf.has_ip_or_as_resources() {
            return Err(rejected(CertificateRejected::IpAddressOrAsResources));
        }

        let end_entity = EndEntityCert::try_from(end_entity).map_err(webpki_error)?;
        let candidates: Vec<CertificateDer<'_>> = intermediates
            .iter()
            .chain(&self.extra_intermediates)
            .cloned()
            .collect();
        // What webpki does not check of a path, OpenSSL does: the anchor's validity period, its
        // pathLenConstraint, name constraints on names webpki leaves alone, CA-capable
        // intermediates, authority key identifiers (see `profile::check_path`). A path that
        // fails here is abandoned and webpki tries the others.
        let chosen = Cell::new(None);
        let verify_path = |path: &webpki::VerifiedPath<'_>| {
            let index = self
                .anchors
                .iter()
                .position(|a| std::ptr::eq(a, path.anchor()))
                .ok_or(webpki::Error::UnknownIssuer)?;
            let root = &self.roots[self.anchor_roots[index]];
            check_validity(&root.cert, now)?;
            let intermediates = path
                .intermediate_certificates()
                .map(|c| PeerCert::from_der(c.der().as_ref()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| webpki::Error::BadDer)?;
            let in_ca_file: Vec<bool> = path
                .intermediate_certificates()
                .map(|c| self.is_in_ca_file(c.der().as_ref()))
                .collect();
            profile::check_path(leaf, &intermediates, &in_ca_file, &root.cert)
                .map_err(profile::PathProblem::to_webpki)?;
            chosen.set(Some(self.anchor_roots[index]));
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
        let leaf = PeerCert::from_der(end_entity.as_ref())
            .map_err(|_| Error::from(CertificateError::BadEncoding))?;
        let root_index = self.validate_chain(end_entity, &leaf, intermediates, now)?;

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
