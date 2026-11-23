//! The certificate fields `verify_peers` rules look at, extracted from a DER certificate.

use x509_parser::asn1_rs::Class;
use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::FromDer;
use x509_parser::x509::{AttributeTypeAndValue, X509Name, X509Version};

use super::exts::{self, AuthorityKeyId, GeneralName as Gn};
use super::profile::{self, NameConstraintsInfo};
use super::rules::{Field, MatchType};

/// The [`AsnString::tag`] of a value that is not a universal, primitive ASN.1 type.
pub(super) const NOT_A_STRING: u32 = u32::MAX;

/// An ASN.1 string value as it appears in a certificate: its tag and raw content bytes.
///
/// FDB compares values after OpenSSL's `ASN1_STRING_to_UTF8`, which converts by tag and
/// fails (so the value never matches) for tags that are not character strings. See
/// [`AsnString::to_utf8`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AsnString {
    /// The universal tag number (12 UTF8String, 19 PrintableString, 22 IA5String, ...).
    pub tag: u32,
    /// The content octets.
    pub bytes: Vec<u8>,
}

impl AsnString {
    /// What OpenSSL's `ASN1_STRING_to_UTF8` produces, or `None` where it fails.
    ///
    /// It maps each string type to its encoding, converts to UTF-8, and gives up on other
    /// types (notably OCTET STRING, which is how an `iPAddress` general name is stored, so
    /// IP addresses never match a rule):
    ///
    /// | tag | type | treated as |
    /// |---|---|---|
    /// | 12 | UTF8String | UTF-8, rejected if malformed |
    /// | 18, 19, 20, 22, 23, 24, 26 | Numeric, Printable, T61 (Teletex), IA5, UTCTime, GeneralizedTime, Visible | one byte per character, taken as Latin-1 |
    /// | 30 | BMPString | UTF-16BE without surrogate pairs |
    /// | 28 | UniversalString | UTF-32BE |
    ///
    /// An empty result is also a failure, since FDB requires a length of at least one.
    pub fn to_utf8(&self) -> Option<Vec<u8>> {
        self.to_utf8_allowing_empty().filter(|out| !out.is_empty())
    }

    /// [`to_utf8`](Self::to_utf8) without the FDB length requirement: an empty string converts
    /// (to nothing), as `ASN1_STRING_to_UTF8` does. Only a type that has no conversion, or a
    /// malformed value, is `None`.
    pub(super) fn to_utf8_allowing_empty(&self) -> Option<Vec<u8>> {
        let out = match self.tag {
            12 => std::str::from_utf8(&self.bytes).ok()?.as_bytes().to_vec(),
            18 | 19 | 20 | 22 | 23 | 24 | 26 => latin1_to_utf8(&self.bytes),
            30 => {
                if self.bytes.len() % 2 != 0 {
                    return None;
                }
                let mut s = String::new();
                for pair in self.bytes.chunks_exact(2) {
                    s.push(char::from_u32(u32::from(u16::from_be_bytes([
                        pair[0], pair[1],
                    ])))?);
                }
                s.into_bytes()
            }
            28 => {
                if self.bytes.len() % 4 != 0 {
                    return None;
                }
                let mut s = String::new();
                for quad in self.bytes.chunks_exact(4) {
                    s.push(char::from_u32(u32::from_be_bytes([
                        quad[0], quad[1], quad[2], quad[3],
                    ]))?);
                }
                s.into_bytes()
            }
            _ => return None,
        };
        Some(out)
    }
}

/// Each byte as the Unicode code point of the same value, encoded as UTF-8.
pub(super) fn latin1_to_utf8(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    for &b in bytes {
        if b < 0x80 {
            out.push(b);
        } else {
            out.push(0xC0 | (b >> 6));
            out.push(0x80 | (b & 0x3F));
        }
    }
    out
}

/// `match_criteria_entry`: does `entry` match `criteria` (raw bytes from the rule) the way
/// `match_type` says?
///
/// The criteria is wrapped in an IA5String before conversion, so its bytes are taken as
/// Latin-1: a rule value that is itself non-ASCII UTF-8 expands to two code points per
/// character and so does not match the same text in a UTF8String entry. Both sides must
/// convert to at least one byte.
pub(super) fn match_entry(criteria: &[u8], entry: &AsnString, match_type: MatchType) -> bool {
    let criteria = latin1_to_utf8(criteria);
    if criteria.is_empty() {
        return false;
    }
    let Some(entry) = entry.to_utf8() else {
        return false;
    };
    match match_type {
        MatchType::Exact => criteria == entry,
        MatchType::Prefix => entry.starts_with(&criteria),
        MatchType::Suffix => entry.ends_with(&criteria),
    }
}

/// One attribute of a distinguished name whose type is one of the [`Field`]s rules can name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NameEntry {
    /// Which field the attribute is.
    pub field: Field,
    /// The attribute value.
    pub value: AsnString,
}

/// A subject alternative name entry, as `matchExtensionCriteria` distinguishes them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AltName {
    /// `dNSName`, an IA5String.
    Dns(AsnString),
    /// `rfc822Name`, an IA5String.
    Email(AsnString),
    /// `uniformResourceIdentifier`, an IA5String.
    Uri(AsnString),
    /// `iPAddress`, an OCTET STRING (never matches).
    Ip(Vec<u8>),
    /// One of the types FDB gives up on, with the name it traces
    /// (`GEN_OTHERNAME`, `GEN_X400`, `GEN_DIRNAME`, `GEN_EDIPARTY`, `GEN_RID`).
    Unsupported(&'static str),
    /// Anything else, which FDB skips.
    Other,
}

/// An extension of a certificate, as far as policy checks care.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Ext<T> {
    Absent,
    Present(T),
    /// Present but unparseable, or duplicated.
    Invalid,
}

/// The keyUsage bits TLS cares about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct KeyUsageBits {
    digital_signature: bool,
    key_encipherment: bool,
    key_agreement: bool,
    key_cert_sign: bool,
}

impl KeyUsageBits {
    /// From OpenSSL's `ex_kusage` ([`exts::key_usage`]).
    fn from_openssl(bits: u16) -> KeyUsageBits {
        KeyUsageBits {
            digital_signature: bits & 0x80 != 0,
            key_encipherment: bits & 0x20 != 0,
            key_agreement: bits & 0x08 != 0,
            key_cert_sign: bits & 0x04 != 0,
        }
    }
}

/// What OpenSSL's `X509_check_purpose` and chain checks look at, extracted once.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Profile {
    v1: bool,
    /// Whether the certificate is X.509 version 3, the only version webpki (and so rustls)
    /// accepts for a certificate it validates.
    v3: bool,
    /// OpenSSL's `EXFLAG_INVALID`: one of the extensions OpenSSL decodes for every certificate
    /// is duplicated or does not decode, `pathLenConstraint` is negative, or `keyUsage` has
    /// no bit set (see [`exts`]). OpenSSL cannot link such a certificate to an issuer or to a
    /// subject, so it is never part of a path OpenSSL accepts.
    invalid: bool,
    /// A `proxyCertInfo` extension (RFC 3820), in any form: OpenSSL refuses proxy
    /// certificates unless `X509_V_FLAG_ALLOW_PROXY_CERTS` is set, which FDB does not do.
    proxy: bool,
    /// An RFC 3779 `ipAddrBlock` or `autonomousSysNum` extension.
    ip_or_as_resources: bool,
    /// The signature algorithm is one for the certificate's own kind of key
    /// (`check_sig_alg_match`).
    signature_matches_key: bool,
    /// basicConstraints `cA`.
    ca: Ext<bool>,
    /// basicConstraints `pathLenConstraint`, if `cA` is true and it is present.
    path_len: Option<u64>,
    key_usage: Ext<KeyUsageBits>,
    /// Whether the extendedKeyUsage allows a TLS server: it lists `serverAuth` or one of the
    /// legacy server-gated-crypto OIDs. `anyExtendedKeyUsage` alone does not (see
    /// [`profile`]).
    eku_allows_server: Ext<bool>,
    /// Whether the Netscape certificate type (`nsCertType`) has the `sslServer` bit.
    ns_ssl_server: Ext<bool>,
    /// Some extension marked critical is outside OpenSSL's `X509_supported_extension` list.
    unsupported_critical: bool,
    name_constraints: Ext<NameConstraintsInfo>,
}

impl Profile {
    #[cfg(test)]
    const ABSENT: Profile = Profile {
        v1: false,
        v3: true,
        invalid: false,
        proxy: false,
        ip_or_as_resources: false,
        signature_matches_key: true,
        ca: Ext::Absent,
        path_len: None,
        key_usage: Ext::Absent,
        eku_allows_server: Ext::Absent,
        ns_ssl_server: Ext::Absent,
        unsupported_critical: false,
        name_constraints: Ext::Absent,
    };
}

/// The parts of a certificate that rules can examine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerCert {
    subject: Vec<NameEntry>,
    issuer: Vec<NameEntry>,
    subject_display: String,
    issuer_display: String,
    /// `None`: no (or unparseable) subjectAltName extension.
    alt_names: Option<Vec<AltName>>,
    not_before: i64,
    not_after: i64,
    /// The subject and issuer names are byte-identical. OpenSSL's `EXFLAG_SI` compares them
    /// canonically; this stricter reading is what decides which intermediates the anchor's
    /// `pathLenConstraint` and name constraints skip.
    self_issued: bool,
    /// OpenSSL's `EXFLAG_SS`: see [`is_self_signed`](Self::is_self_signed).
    self_signed: bool,
    profile: Profile,
    /// subjectKeyIdentifier.
    skid: Ext<Vec<u8>>,
    /// authorityKeyIdentifier.
    akid: Ext<AuthorityKeyId>,
    /// The serial number's content octets.
    serial: Vec<u8>,
    /// The issuer name in OpenSSL's canonical form ([`exts::canonical_name`]), `None` if
    /// OpenSSL could not decode it.
    issuer_canonical: Option<Vec<u8>>,
    /// The subject's `emailAddress` attributes.
    subject_emails: Vec<AsnString>,
    /// The number of subject attributes plus the number of subjectAltName entries, which
    /// OpenSSL's `NAME_CONSTRAINTS_check` bounds (`NAME_CHECK_MAX`).
    name_count: usize,
    /// A subjectAltName `otherName` of type `id-on-SmtpUTF8Mailbox`.
    smtp_utf8_mailbox: bool,
}

/// A certificate that could not be parsed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("cannot parse X.509 certificate: {0}")]
pub struct CertParseError(String);

impl PeerCert {
    /// Parses a DER certificate. Trailing bytes after the certificate are an error.
    pub fn from_der(der: &[u8]) -> Result<PeerCert, CertParseError> {
        let (rest, cert) =
            X509Certificate::from_der(der).map_err(|e| CertParseError(e.to_string()))?;
        if !rest.is_empty() {
            return Err(CertParseError("trailing data after certificate".into()));
        }
        Ok(PeerCert::from_x509(&cert))
    }

    pub(super) fn from_x509(cert: &X509Certificate<'_>) -> PeerCert {
        // An entry x509-parser cannot decode (a malformed or unknown general name) makes the
        // whole extension unparseable, as OpenSSL's `X509_get_ext_d2i` would fail: the
        // extension then counts as absent and every `subjectAltName` criterion fails.
        let alt_names = match cert.subject_alternative_name() {
            Ok(Some(ext))
                if !ext
                    .value
                    .general_names
                    .iter()
                    .any(|n| matches!(n, GeneralName::Invalid(..))) =>
            {
                Some(
                    ext.value
                        .general_names
                        .iter()
                        .map(alt_name)
                        .collect::<Vec<_>>(),
                )
            }
            _ => None,
        };
        let san = match decode(find(cert, OID_SAN), exts::subject_alt_name) {
            Ext::Present(names) => names,
            _ => Vec::new(),
        };
        let subject_canonical = exts::canonical_name(cert.subject().as_raw());
        let issuer_canonical = exts::canonical_name(cert.issuer().as_raw());
        let self_issued = cert.subject().as_raw() == cert.issuer().as_raw();
        let mut out = PeerCert {
            subject: name_entries(cert.subject()),
            issuer: name_entries(cert.issuer()),
            subject_display: display_name(cert.subject()),
            issuer_display: display_name(cert.issuer()),
            alt_names,
            not_before: cert.validity().not_before.timestamp(),
            not_after: cert.validity().not_after.timestamp(),
            self_issued,
            self_signed: false,
            profile: profile(cert),
            skid: decode(find(cert, OID_SKID), |v| {
                exts::subject_key_identifier(v).map(<[u8]>::to_vec)
            }),
            akid: decode(find(cert, OID_AKID), exts::authority_key_identifier),
            serial: cert.raw_serial().to_vec(),
            issuer_canonical: issuer_canonical.clone(),
            subject_emails: email_entries(cert.subject()),
            name_count: cert.subject().iter_attributes().count() + san.len(),
            smtp_utf8_mailbox: san.contains(&Gn::SmtpUtf8Mailbox),
        };
        // OpenSSL's `ossl_x509v3_cache_extensions`: a certificate whose subject and issuer
        // compare equal (`X509_NAME_cmp`, canonically) is self-signed unless its authority key
        // identifier names somebody else or its signature algorithm is for another kind of key.
        let names_equal =
            self_issued || (subject_canonical.is_some() && subject_canonical == issuer_canonical);
        out.self_signed = names_equal
            && profile::akid_allows(&out.akid, &out)
            && out.profile.signature_matches_key;
        out
    }

    /// Whether OpenSSL's `check_ca` would call this a CA certificate: a keyUsage extension,
    /// if present, must include `keyCertSign`; then `cA` from basicConstraints, or, without
    /// basicConstraints, a self-signed v1 certificate or one whose keyUsage has `keyCertSign`.
    pub(super) fn is_ca_capable(&self) -> bool {
        let p = &self.profile;
        match p.key_usage {
            Ext::Invalid => return false,
            Ext::Present(ku) if !ku.key_cert_sign => return false,
            _ => {}
        }
        match p.ca {
            Ext::Present(ca) => ca,
            Ext::Invalid => false,
            Ext::Absent => (p.v1 && self.self_signed) || matches!(p.key_usage, Ext::Present(_)),
        }
    }

    /// Whether this certificate can be a trust anchor under OpenSSL 3's rules, which check
    /// every certificate of the chain, the anchor included:
    ///
    /// * self-signed ([`is_self_signed`](Self::is_self_signed));
    /// * [CA-capable](Self::is_ca_capable);
    /// * no critical extension outside OpenSSL's supported list (`unhandled critical
    ///   extension`, error 34);
    /// * an `extendedKeyUsage`, if present, must allow `serverAuth` (`unsuitable
    ///   certificate purpose`, error 26). OpenSSL 1.1.1 checked only the certificates that
    ///   came from the peer, so a CA file certificate with, say, `clientAuth` alone anchored
    ///   a chain there; 3.x does not accept it.
    pub(super) fn can_be_anchor(&self) -> bool {
        self.self_signed
            && self.is_ca_capable()
            && !self.profile.unsupported_critical
            && matches!(
                self.profile.eku_allows_server,
                Ext::Absent | Ext::Present(true)
            )
    }

    /// OpenSSL's `ssl_server` purpose for an end-entity certificate
    /// (`check_purpose_ssl_server`): the extendedKeyUsage, if present, must allow a TLS
    /// server, and the keyUsage, if present, must include `digitalSignature`,
    /// `keyEncipherment` or `keyAgreement`. Returns what is wrong.
    pub(super) fn tls_server_purpose_problem(&self) -> Option<&'static str> {
        // `ns_reject(x, NS_SSL_SERVER)`: a Netscape certificate type without `sslServer`.
        match self.profile.ns_ssl_server {
            Ext::Invalid => return Some("its nsCertType extension is malformed"),
            Ext::Present(false) => {
                return Some("its nsCertType (Netscape certificate type) lacks sslServer");
            }
            _ => {}
        }
        match self.profile.eku_allows_server {
            Ext::Invalid => return Some("its extendedKeyUsage extension is malformed"),
            Ext::Present(false) => {
                return Some("its extendedKeyUsage does not include serverAuth");
            }
            _ => {}
        }
        match self.profile.key_usage {
            Ext::Invalid => Some("its keyUsage extension is malformed"),
            Ext::Present(ku)
                if !(ku.digital_signature || ku.key_encipherment || ku.key_agreement) =>
            {
                Some(
                    "its keyUsage allows none of digitalSignature, keyEncipherment and \
                     keyAgreement, which a TLS server certificate needs",
                )
            }
            _ => None,
        }
    }

    /// A certificate with the given attributes and nothing else, for tests that need shapes
    /// (two `OU`s, odd string types) that are awkward to generate.
    #[cfg(test)]
    pub(super) fn from_parts(
        subject: Vec<NameEntry>,
        issuer: Vec<NameEntry>,
        alt_names: Option<Vec<AltName>>,
    ) -> PeerCert {
        let display = |entries: &[NameEntry]| {
            entries
                .iter()
                .map(|e| {
                    format!(
                        "{} = {}",
                        e.field.short_name(),
                        String::from_utf8_lossy(&e.value.bytes)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        PeerCert {
            subject_display: display(&subject),
            issuer_display: display(&issuer),
            subject,
            issuer,
            alt_names,
            not_before: 0,
            not_after: i64::MAX,
            self_issued: false,
            self_signed: false,
            profile: Profile::ABSENT,
            skid: Ext::Absent,
            akid: Ext::Absent,
            serial: Vec::new(),
            issuer_canonical: None,
            subject_emails: Vec::new(),
            name_count: 0,
            smtp_utf8_mailbox: false,
        }
    }

    /// `notBefore`, as seconds since the Unix epoch.
    pub fn not_before(&self) -> i64 {
        self.not_before
    }

    /// `notAfter`, as seconds since the Unix epoch.
    pub fn not_after(&self) -> i64 {
        self.not_after
    }

    /// Whether OpenSSL would call this certificate self-signed (`EXFLAG_SS`): the subject and
    /// issuer names are equal as `X509_NAME_cmp` compares them (canonically: string type,
    /// ASCII case and runs of white space do not matter), the authority key identifier, if
    /// there is one, does not contradict the certificate itself (`X509_check_akid`: a key
    /// identifier that differs from the subject key identifier, or an issuer name or serial
    /// number that differs from the certificate's own), and the signature algorithm is one for
    /// the certificate's own kind of key (`check_sig_alg_match`).
    ///
    /// The signature itself is not verified.
    pub fn is_self_signed(&self) -> bool {
        self.self_signed
    }

    /// OpenSSL's `EXFLAG_INVALID`: an extension OpenSSL decodes for every certificate is
    /// duplicated or malformed (see [`Profile::invalid`]).
    pub(super) fn is_invalid(&self) -> bool {
        self.profile.invalid
    }

    /// Whether the certificate has a `proxyCertInfo` extension.
    pub(super) fn is_proxy(&self) -> bool {
        self.profile.proxy
    }

    /// Whether the certificate has an RFC 3779 `ipAddrBlock` or `autonomousSysNum` extension.
    pub(super) fn has_ip_or_as_resources(&self) -> bool {
        self.profile.ip_or_as_resources
    }

    /// Whether the subject and issuer names are byte-identical (OpenSSL's `EXFLAG_SI` compares
    /// them canonically), which is all OpenSSL asks of a key-rollover certificate in the middle
    /// of a chain to skip it for the anchor's `pathLenConstraint` and for name constraints.
    /// Byte-identical is the stricter reading for both: fewer certificates are skipped.
    pub(super) fn is_self_issued(&self) -> bool {
        self.self_issued
    }

    /// Whether the certificate is X.509 version 3.
    pub(super) fn is_v3(&self) -> bool {
        self.profile.v3
    }

    /// Whether the certificate has a critical extension that OpenSSL does not support (see
    /// [`profile::is_supported_extension`]) and so would refuse it for.
    pub(super) fn has_unsupported_critical_extension(&self) -> bool {
        self.profile.unsupported_critical
    }

    /// The `pathLenConstraint` of a CA certificate, if it has one.
    pub(super) fn path_len(&self) -> Option<u64> {
        self.profile.path_len
    }

    pub(super) fn name_constraints(&self) -> &Ext<NameConstraintsInfo> {
        &self.profile.name_constraints
    }

    pub(super) fn subject_key_identifier(&self) -> &Ext<Vec<u8>> {
        &self.skid
    }

    pub(super) fn authority_key_identifier(&self) -> &Ext<AuthorityKeyId> {
        &self.akid
    }

    pub(super) fn serial(&self) -> &[u8] {
        &self.serial
    }

    pub(super) fn issuer_canonical(&self) -> Option<&[u8]> {
        self.issuer_canonical.as_deref()
    }

    pub(super) fn subject_emails(&self) -> &[AsnString] {
        &self.subject_emails
    }

    pub(super) fn name_count(&self) -> usize {
        self.name_count
    }

    pub(super) fn has_smtp_utf8_mailbox(&self) -> bool {
        self.smtp_utf8_mailbox
    }

    /// The subject's recognised attributes, in certificate order.
    pub fn subject(&self) -> &[NameEntry] {
        &self.subject
    }

    /// The issuer's recognised attributes, in certificate order.
    pub fn issuer(&self) -> &[NameEntry] {
        &self.issuer
    }

    /// The subject alternative names, or `None` if the certificate has no such extension.
    pub fn alt_names(&self) -> Option<&[AltName]> {
        self.alt_names.as_deref()
    }

    pub(super) fn subject_display(&self) -> &str {
        &self.subject_display
    }

    pub(super) fn issuer_display(&self) -> &str {
        &self.issuer_display
    }
}

const OID_BC: &str = "2.5.29.19";
const OID_KU: &str = "2.5.29.15";
const OID_EKU: &str = "2.5.29.37";
const OID_NS_CERT_TYPE: &str = "2.16.840.1.113730.1.1";
const OID_SKID: &str = "2.5.29.14";
const OID_AKID: &str = "2.5.29.35";
const OID_SAN: &str = "2.5.29.17";
const OID_NC: &str = "2.5.29.30";
const OID_CRLDP: &str = "2.5.29.31";
const OID_PROXY_CERT_INFO: &str = "1.3.6.1.5.5.7.1.14";
const OID_IP_ADDR_BLOCKS: &str = "1.3.6.1.5.5.7.1.7";
const OID_AS_IDENTIFIERS: &str = "1.3.6.1.5.5.7.1.8";

/// The value of the one extension of the certificate with this OID: none, one, or a duplicate
/// (invalid, as `X509_get_ext_d2i` finds it).
fn find<'a>(cert: &X509Certificate<'a>, oid: &str) -> Ext<&'a [u8]> {
    let mut found = cert
        .extensions()
        .iter()
        .filter(|ext| ext.oid.to_id_string() == oid);
    match (found.next(), found.next()) {
        (None, _) => Ext::Absent,
        (Some(ext), None) => Ext::Present(ext.value),
        (Some(_), Some(_)) => Ext::Invalid,
    }
}

/// An extension's value decoded by `f`; a value `f` refuses is invalid.
fn decode<'a, T>(ext: Ext<&'a [u8]>, f: impl FnOnce(&'a [u8]) -> Option<T>) -> Ext<T> {
    match ext {
        Ext::Absent => Ext::Absent,
        Ext::Invalid => Ext::Invalid,
        Ext::Present(value) => f(value).map_or(Ext::Invalid, Ext::Present),
    }
}

fn map<T, U>(ext: Ext<T>, f: impl FnOnce(T) -> U) -> Ext<U> {
    match ext {
        Ext::Absent => Ext::Absent,
        Ext::Invalid => Ext::Invalid,
        Ext::Present(v) => Ext::Present(f(v)),
    }
}

fn profile(cert: &X509Certificate<'_>) -> Profile {
    // Decoded as OpenSSL decodes them: x509-parser is more lenient in places, and its
    // `basic_constraints()` hides a malformed extension.
    let bc = decode(find(cert, OID_BC), exts::basic_constraints);
    let ku = decode(find(cert, OID_KU), exts::key_usage);
    let eku = decode(find(cert, OID_EKU), exts::extended_key_usage);
    let ns = decode(find(cert, OID_NS_CERT_TYPE), exts::ns_cert_type);
    let nc = decode(find(cert, OID_NC), exts::name_constraints);
    let fails = |oid: &str, ok: fn(&[u8]) -> bool| match find(cert, oid) {
        Ext::Absent => false,
        Ext::Present(value) => !ok(value),
        Ext::Invalid => true,
    };
    let invalid = matches!(bc, Ext::Invalid)
        || matches!(bc, Ext::Present(bc) if bc.negative_path_len)
        || matches!(ku, Ext::Invalid | Ext::Present(0))
        || matches!(eku, Ext::Invalid)
        || matches!(ns, Ext::Invalid)
        || matches!(nc, Ext::Invalid)
        || fails(OID_SKID, |v| exts::subject_key_identifier(v).is_some())
        || fails(OID_AKID, |v| exts::authority_key_identifier(v).is_some())
        || fails(OID_SAN, |v| exts::subject_alt_name(v).is_some())
        || fails(OID_CRLDP, exts::crl_distribution_points_ok)
        || fails(OID_IP_ADDR_BLOCKS, exts::ip_addr_blocks_ok)
        || fails(OID_AS_IDENTIFIERS, exts::as_identifiers_ok);
    // serverAuth, and nsSGC and msSGC: the "server gated crypto" OIDs OpenSSL still honours.
    // A bare anyExtendedKeyUsage does not count: `xku_reject` looks for the server bits only,
    // and OpenSSL 3.5 (`openssl verify -purpose sslserver`) refuses a leaf, an intermediate
    // and a trust anchor whose only EKU is anyExtendedKeyUsage, as webpki does.
    const SERVER_EKUS: [&[u8]; 3] = [
        &[0x2B, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x01],
        &[0x60, 0x86, 0x48, 0x01, 0x86, 0xF8, 0x42, 0x04, 0x01],
        &[0x2B, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x0A, 0x03, 0x03],
    ];
    let has = |oid: &str| !matches!(find(cert, oid), Ext::Absent);
    Profile {
        v1: cert.version() == X509Version::V1,
        v3: cert.version() == X509Version::V3,
        invalid,
        proxy: has(OID_PROXY_CERT_INFO),
        ip_or_as_resources: has(OID_IP_ADDR_BLOCKS) || has(OID_AS_IDENTIFIERS),
        signature_matches_key: exts::signature_matches_key(
            &cert.tbs_certificate.signature.algorithm.to_id_string(),
            &cert.public_key().algorithm.algorithm.to_id_string(),
        ),
        ca: map(bc, |bc| bc.ca),
        path_len: match bc {
            Ext::Present(bc) if bc.ca => bc.path_len,
            _ => None,
        },
        key_usage: map(ku, KeyUsageBits::from_openssl),
        eku_allows_server: map(eku, |oids| oids.iter().any(|o| SERVER_EKUS.contains(o))),
        ns_ssl_server: map(ns, |bits| bits & 0x40 != 0),
        unsupported_critical: cert
            .extensions()
            .iter()
            .any(|ext| ext.critical && !profile::is_supported_extension(&ext.oid.to_id_string())),
        name_constraints: map(nc, |subtrees| NameConstraintsInfo::new(&subtrees)),
    }
}

fn name_entries(name: &X509Name<'_>) -> Vec<NameEntry> {
    const FIELDS: [Field; 8] = [
        Field::Cn,
        Field::C,
        Field::L,
        Field::St,
        Field::O,
        Field::Ou,
        Field::Dc,
        Field::Uid,
    ];
    name.iter_attributes()
        .filter_map(|attr| {
            let oid = attr.attr_type().to_id_string();
            let field = FIELDS.into_iter().find(|f| f.oid() == oid)?;
            Some(NameEntry {
                field,
                value: asn_string(attr),
            })
        })
        .collect()
}

/// An attribute's value as the tag and bytes OpenSSL's `ASN1_STRING_to_UTF8` sees. Only a
/// universal, primitive string converts; anything else is a value that can never match.
fn asn_string(attr: &AttributeTypeAndValue<'_>) -> AsnString {
    let any = attr.attr_value();
    let tag = if any.class() == Class::Universal && !any.header.is_constructed() {
        any.tag().0
    } else {
        NOT_A_STRING
    };
    AsnString {
        tag,
        bytes: any.data.to_vec(),
    }
}

/// The values of the `emailAddress` attributes (PKCS#9, `1.2.840.113549.1.9.1`) of a name.
fn email_entries(name: &X509Name<'_>) -> Vec<AsnString> {
    name.iter_attributes()
        .filter(|attr| attr.attr_type().to_id_string() == "1.2.840.113549.1.9.1")
        .map(asn_string)
        .collect()
}

fn alt_name(name: &GeneralName<'_>) -> AltName {
    let ia5 = |s: &str| AsnString {
        tag: 22,
        bytes: s.as_bytes().to_vec(),
    };
    match name {
        GeneralName::RFC822Name(s) => AltName::Email(ia5(s)),
        GeneralName::DNSName(s) => AltName::Dns(ia5(s)),
        GeneralName::URI(s) => AltName::Uri(ia5(s)),
        GeneralName::IPAddress(b) => AltName::Ip(b.to_vec()),
        GeneralName::OtherName(..) => AltName::Unsupported("GEN_OTHERNAME"),
        GeneralName::X400Address(_) => AltName::Unsupported("GEN_X400"),
        GeneralName::DirectoryName(_) => AltName::Unsupported("GEN_DIRNAME"),
        GeneralName::EDIPartyName(_) => AltName::Unsupported("GEN_EDIPARTY"),
        GeneralName::RegisteredID(_) => AltName::Unsupported("GEN_RID"),
        // Filtered out by the caller: an undecodable entry voids the extension.
        GeneralName::Invalid(..) => AltName::Other,
    }
}

/// An approximation of OpenSSL's `X509_NAME_print_ex(XN_FLAG_ONELINE)`, which FDB appends to
/// its failure reasons: `C = US, ST = California, O = Example`.
fn display_name(name: &X509Name<'_>) -> String {
    let mut out = String::new();
    for (i, rdn) in name.iter_rdn().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        for (j, attr) in rdn.iter().enumerate() {
            if j > 0 {
                out.push_str(" + ");
            }
            let oid = attr.attr_type().to_id_string();
            let label = match oid.as_str() {
                "2.5.4.3" => "CN",
                "2.5.4.6" => "C",
                "2.5.4.7" => "L",
                "2.5.4.8" => "ST",
                "2.5.4.10" => "O",
                "2.5.4.11" => "OU",
                "0.9.2342.19200300.100.1.25" => "DC",
                "0.9.2342.19200300.100.1.1" => "UID",
                "1.2.840.113549.1.9.1" => "emailAddress",
                other => other,
            };
            out.push_str(label);
            out.push_str(" = ");
            match attr.as_str() {
                Ok(s) => out.push_str(s),
                Err(_) => out.push_str(&String::from_utf8_lossy(attr.as_slice())),
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(tag: u32, bytes: &[u8]) -> AsnString {
        AsnString {
            tag,
            bytes: bytes.to_vec(),
        }
    }

    #[test]
    fn to_utf8_by_tag() {
        assert_eq!(s(12, "é".as_bytes()).to_utf8().unwrap(), "é".as_bytes());
        assert_eq!(s(12, &[0xFF]).to_utf8(), None);
        assert_eq!(s(19, b"abc").to_utf8().unwrap(), b"abc");
        assert_eq!(s(22, b"a.b").to_utf8().unwrap(), b"a.b");
        // Latin-1 e-acute in a T61String becomes UTF-8 e-acute.
        assert_eq!(s(20, &[0xE9]).to_utf8().unwrap(), "é".as_bytes());
        assert_eq!(s(30, &[0x00, 0xE9]).to_utf8().unwrap(), "é".as_bytes());
        assert_eq!(s(30, &[0x00]).to_utf8(), None);
        assert_eq!(s(30, &[0xD8, 0x00]).to_utf8(), None);
        assert_eq!(s(28, &[0, 0, 0, 0x41]).to_utf8().unwrap(), b"A");
        assert_eq!(s(28, &[0, 0x11, 0, 0]).to_utf8(), None);
        // OCTET STRING (an iPAddress), BOOLEAN, empty strings: no conversion.
        assert_eq!(s(4, &[10, 0, 0, 1]).to_utf8(), None);
        assert_eq!(s(1, &[0xFF]).to_utf8(), None);
        assert_eq!(s(12, b"").to_utf8(), None);
    }

    #[test]
    fn match_entry_semantics() {
        let e = s(12, b"foo.example.com");
        assert!(match_entry(b"foo.example.com", &e, MatchType::Exact));
        assert!(!match_entry(b"foo.example", &e, MatchType::Exact));
        assert!(match_entry(b"foo.", &e, MatchType::Prefix));
        assert!(match_entry(b"foo.example.com", &e, MatchType::Prefix));
        assert!(!match_entry(b"oo.", &e, MatchType::Prefix));
        assert!(match_entry(b".com", &e, MatchType::Suffix));
        assert!(!match_entry(b".org", &e, MatchType::Suffix));
        assert!(!match_entry(
            b"longer.than.the.entry.example.com",
            &e,
            MatchType::Suffix
        ));
        assert!(!match_entry(b"", &e, MatchType::Prefix));
        // Criteria bytes are Latin-1, so a literal UTF-8 e-acute does not match a UTF8String
        // e-acute, but the single byte 0xE9 does.
        let accent = s(12, "é".as_bytes());
        assert!(!match_entry("é".as_bytes(), &accent, MatchType::Exact));
        assert!(match_entry(&[0xE9], &accent, MatchType::Exact));
    }
}
