//! OpenSSL's chain checks that `rustls-webpki` does not make.
//!
//! FDB validates with OpenSSL (`X509_verify_cert`, `-purpose sslserver`). This crate stays on
//! webpki for chain building and signatures (a recorded decision: no hand-written validator),
//! and closes each case where webpki *accepts* what OpenSSL rejects with a check from this
//! module, applied to the candidate path webpki offers (the `verify_path` callback: an error
//! there makes webpki try other paths), to the CA-file certificates when the trust anchors are
//! built, and to a peer that is itself a configured root.
//!
//! The OpenSSL behaviour below is that of 3.x (`crypto/x509/x509_vfy.c`,
//! `crypto/x509/v3_purp.c`, `crypto/x509/v3_ncons.c`), checked against 3.5.5 with
//! `openssl verify -purpose sslserver`.

use super::cert::{AltName, AsnString, Ext, PeerCert};
use super::exts::{self, AuthorityKeyId};
use super::rules::Field;

/// OpenSSL's `X509_supported_extension` list (`crypto/x509/v3_purp.c`, `supported_nids`, as
/// of 3.5): the extensions its verifier "uses", so that a *critical* one is not an "unhandled
/// critical extension" (error 34, `X509_V_ERR_UNHANDLED_CRITICAL_EXTENSION`). Every other
/// critical extension makes a certificate unusable, whatever its role in the chain: this
/// includes `subjectKeyIdentifier`, `authorityKeyIdentifier`, `authorityInfoAccess`,
/// `subjectInfoAccess`, `freshestCRL` and `issuerAltName`, which are normally not critical.
///
/// The two RFC 3779 entries are compiled in unless OpenSSL is built with `no-rfc3779`.
const SUPPORTED_EXTENSIONS: [&str; 15] = [
    "2.16.840.1.113730.1.1", // nsCertType
    "2.5.29.15",             // keyUsage
    "2.5.29.17",             // subjectAltName
    "2.5.29.19",             // basicConstraints
    "2.5.29.32",             // certificatePolicies
    "2.5.29.31",             // cRLDistributionPoints
    "2.5.29.37",             // extendedKeyUsage
    "1.3.6.1.5.5.7.1.7",     // sbgp-ipAddrBlock (RFC 3779)
    "1.3.6.1.5.5.7.1.8",     // sbgp-autonomousSysNum (RFC 3779)
    "1.3.6.1.5.5.7.48.1.5",  // id-pkix-ocsp-nocheck
    "2.5.29.36",             // policyConstraints
    "1.3.6.1.5.5.7.1.14",    // proxyCertInfo
    "2.5.29.30",             // nameConstraints
    "2.5.29.33",             // policyMappings
    "2.5.29.54",             // inhibitAnyPolicy
];

/// Whether OpenSSL's `X509_supported_extension` knows the extension with this dotted OID.
pub(super) fn is_supported_extension(oid: &str) -> bool {
    SUPPORTED_EXTENSIONS.contains(&oid)
}

/// OpenSSL's `X509_check_akid(issuer, akid)`, as `ossl_x509_likely_issued` uses it: could a
/// certificate whose authority key identifier is `akid` come from `issuer`? An issuer whose
/// own subject key identifier is malformed never can (OpenSSL fails to cache its extensions,
/// whatever the child has); otherwise an `Absent` identifier always fits.
///
/// A key identifier that differs from the issuer's subject key identifier (when it has one),
/// a serial number whose content octets are not the issuer's, and an issuer name that is not
/// the *issuer's issuer* (compared canonically, as `X509_NAME_cmp` does) all say no.
pub(super) fn akid_allows(akid: &Ext<AuthorityKeyId>, issuer: &PeerCert) -> bool {
    if matches!(issuer.subject_key_identifier(), Ext::Invalid) {
        return false;
    }
    let akid = match akid {
        Ext::Absent => return true,
        Ext::Invalid => return false,
        Ext::Present(akid) => akid,
    };
    if let (Some(key_id), Ext::Present(skid)) = (&akid.key_id, issuer.subject_key_identifier()) {
        if key_id != skid {
            return false;
        }
    }
    if akid
        .serial
        .as_deref()
        .is_some_and(|serial| serial != issuer.serial())
    {
        return false;
    }
    if let Some(name) = &akid.issuer {
        if issuer.issuer_canonical() != Some(name.as_slice()) {
            return false;
        }
    }
    true
}

/// The `dNSName` and `rfc822Name` subtrees of a certificate's `nameConstraints` (the other
/// kinds are webpki's business), and how many subtrees it has in all.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct NameConstraintsInfo {
    dns: Subtrees,
    email: Subtrees,
    /// Permitted plus excluded subtrees of every name type (`NAME_CHECK_MAX`).
    count: usize,
}

/// The subtrees of one name type: the `base` of each (an IA5String).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Subtrees {
    permitted: Vec<Vec<u8>>,
    excluded: Vec<Vec<u8>>,
    /// Some subtree has a `minimum` other than 0 or a `maximum`, which OpenSSL cannot
    /// evaluate (`X509_V_ERR_SUBTREE_MINMAX`).
    bad_range: bool,
}

impl Subtrees {
    fn is_empty(&self) -> bool {
        self.permitted.is_empty() && self.excluded.is_empty()
    }
}

impl NameConstraintsInfo {
    /// From a decoded `nameConstraints` ([`exts::name_constraints`]).
    pub(super) fn new((permitted, excluded): &exts::Subtrees<'_>) -> NameConstraintsInfo {
        let mut info = NameConstraintsInfo {
            count: permitted.len() + excluded.len(),
            ..NameConstraintsInfo::default()
        };
        for (list, is_permitted) in [(permitted, true), (excluded, false)] {
            for subtree in list {
                let (target, base) = match subtree.base {
                    exts::GeneralName::Dns(base) => (&mut info.dns, base),
                    exts::GeneralName::Rfc822(base) => (&mut info.email, base),
                    _ => continue,
                };
                target.bad_range |= subtree.bad_range;
                if is_permitted {
                    target.permitted.push(base.to_vec());
                } else {
                    target.excluded.push(base.to_vec());
                }
            }
        }
        info
    }
}

/// What one subtree comparison says, as OpenSSL's `nc_dns` and `nc_email` return it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Match {
    Yes,
    No,
    /// `X509_V_ERR_UNSUPPORTED_NAME_SYNTAX`: the name cannot be compared at all.
    Syntax,
}

/// `nc_dns`: `name` is within the domain `base`. An empty base matches everything; otherwise
/// `name` must end with `base`, case-insensitively, and unless `base` starts with a `.` the
/// match must start at a label boundary.
fn dns_match(name: &[u8], base: &[u8]) -> Match {
    if base.is_empty() {
        return Match::Yes;
    }
    if name.len() < base.len() {
        return Match::No;
    }
    let tail = name.len() - base.len();
    if tail > 0 && base[0] != b'.' && name[tail - 1] != b'.' {
        return Match::No;
    }
    if name[tail..].eq_ignore_ascii_case(base) {
        Match::Yes
    } else {
        Match::No
    }
}

/// `nc_email`: `name` (an `rfc822Name`) against the constraint `base`, which is a mailbox
/// (`local@host`: local part case-sensitive, host not), a host (exact, case-insensitive), or a
/// `.domain` (suffix).
fn email_match(name: &[u8], base: &[u8]) -> Match {
    let Some(name_at) = name.iter().rposition(|&b| b == b'@') else {
        return Match::Syntax;
    };
    let base_at = base.iter().rposition(|&b| b == b'@');
    if base_at.is_none() && base.first() == Some(&b'.') {
        // Initial '.': a suffix match, on a name longer than the constraint.
        let matches =
            name.len() > base.len() && name[name.len() - base.len()..].eq_ignore_ascii_case(base);
        return if matches { Match::Yes } else { Match::No };
    }
    let mut base_host = base;
    if let Some(at) = base_at {
        if at != 0 {
            if at != name_at {
                return Match::No;
            }
            let (base_local, name_local) = (&base[..at], &name[..name_at]);
            if base_local.contains(&0) || name_local.contains(&0) {
                return Match::Syntax;
            }
            if base_local != name_local {
                return Match::No;
            }
        }
        base_host = &base[at + 1..];
    }
    if base_host.eq_ignore_ascii_case(&name[name_at + 1..]) {
        Match::Yes
    } else {
        Match::No
    }
}

/// Why a name is refused by name constraints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum NameProblem {
    /// A name is not within any permitted subtree of its type.
    NotPermitted,
    /// A name is within an excluded subtree.
    Excluded,
    /// A subtree has a `minimum` or `maximum`, or a name cannot be compared.
    Unsupported,
}

impl Subtrees {
    /// `nc_match`: if there are permitted subtrees, `name` must be within one of them; it
    /// must not be within an excluded one.
    fn check(&self, name: &[u8], matcher: fn(&[u8], &[u8]) -> Match) -> Result<(), NameProblem> {
        if self.bad_range {
            return Err(NameProblem::Unsupported);
        }
        let mut permitted = self.permitted.is_empty();
        for base in &self.permitted {
            match matcher(name, base) {
                Match::Yes => {
                    permitted = true;
                    break;
                }
                Match::No => {}
                Match::Syntax => return Err(NameProblem::Unsupported),
            }
        }
        if !permitted {
            return Err(NameProblem::NotPermitted);
        }
        for base in &self.excluded {
            match matcher(name, base) {
                Match::Yes => return Err(NameProblem::Excluded),
                Match::No => {}
                Match::Syntax => return Err(NameProblem::Unsupported),
            }
        }
        Ok(())
    }
}

/// What `cn2dnsid` makes of a `commonName`: the value as a DNS name if it looks like one, or
/// `None` if it does not. `Err` if OpenSSL refuses the name: it cannot be converted to UTF-8,
/// or it has a NUL that is not trailing (`X509_V_ERR_UNSUPPORTED_NAME_SYNTAX`).
///
/// A name "looks like a host name" if it has at least two labels of letters, digits,
/// underscores and interior hyphens (`a.b`; not `localhost`, not `a..b`, not `-a.b`), after
/// trailing NULs are dropped. This is deliberately the legacy syntax: OpenSSL checks a CN
/// against DNS constraints only when it would pass for a host name.
fn cn_as_dns_id(cn: &AsnString) -> Result<Option<Vec<u8>>, ()> {
    let mut value = cn.to_utf8_allowing_empty().ok_or(())?;
    while value.last() == Some(&0) {
        value.pop();
    }
    if value.contains(&0) {
        return Err(());
    }
    let n = value.len();
    let mut is_dns_name = false;
    for (i, &c) in value.iter().enumerate() {
        if c.is_ascii_alphanumeric() || c == b'_' {
            continue;
        }
        // A dot or hyphen cannot be first or last.
        if i > 0 && i + 1 < n {
            if c == b'-' {
                continue;
            }
            // Next to a dot, neither neighbour may be a dot or a hyphen; two or more labels
            // make the name plausible.
            if c == b'.' && value[i + 1] != b'.' && value[i - 1] != b'-' && value[i + 1] != b'-' {
                is_dns_name = true;
                continue;
            }
        }
        is_dns_name = false;
        break;
    }
    Ok(is_dns_name.then_some(value))
}

/// OpenSSL's `NAME_CHECK_MAX`: `NAME_CONSTRAINTS_check` refuses a certificate when its names
/// (subject attributes plus alternative names) times the constraint holder's subtrees (of
/// every type) exceed this.
const NAME_CHECK_MAX: usize = 1 << 20;

/// `NAME_CONSTRAINTS_check` (the parts webpki does not do) and `NAME_CONSTRAINTS_check_CN` for
/// `cert` against `nc`: the subject's `emailAddress` attributes, the DNS and e-mail
/// alternative names, and, for the end-entity certificate with no `dNSName` alternative name,
/// each host-name-looking `commonName` as a DNS name.
///
/// An `id-on-SmtpUTF8Mailbox` alternative name under `rfc822Name` subtrees is refused outright:
/// OpenSSL compares it (`nc_email_eai`, with an A-label to U-label conversion of the
/// constraint) and webpki ignores it, so rather than emulate that comparison this is stricter.
fn check_names(
    cert: &PeerCert,
    nc: &NameConstraintsInfo,
    end_entity: bool,
) -> Result<(), NameProblem> {
    if cert.has_smtp_utf8_mailbox() && !nc.email.is_empty() {
        return Err(NameProblem::Unsupported);
    }
    for email in cert.subject_emails() {
        // OpenSSL refuses the whole certificate for an emailAddress that is not an IA5String,
        // whether or not the constraints mention e-mail.
        if email.tag != 22 {
            return Err(NameProblem::Unsupported);
        }
        nc.email.check(&email.bytes, email_match)?;
    }
    let mut has_dns_san = false;
    for name in cert.alt_names().unwrap_or_default() {
        match name {
            AltName::Dns(dns) => {
                has_dns_san = true;
                nc.dns.check(&dns.bytes, dns_match)?;
            }
            AltName::Email(email) => nc.email.check(&email.bytes, email_match)?,
            _ => {}
        }
    }
    if end_entity && !has_dns_san {
        for entry in cert.subject().iter().filter(|e| e.field == Field::Cn) {
            match cn_as_dns_id(&entry.value) {
                Err(()) => return Err(NameProblem::Unsupported),
                Ok(None) => {}
                Ok(Some(id)) => nc.dns.check(&id, dns_match)?,
            }
        }
    }
    Ok(())
}

/// Why a candidate path is not acceptable to OpenSSL although webpki accepted it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PathProblem {
    /// A certificate of the path has a duplicated or malformed extension of the kinds OpenSSL
    /// decodes for every certificate, a negative `pathLenConstraint`, or an empty `keyUsage`
    /// (`EXFLAG_INVALID`; OpenSSL then finds no issuer, error 20).
    InvalidCertificate,
    /// A certificate of the path is a proxy certificate (`proxyCertInfo`; error 40, or 20
    /// when the extension also makes it invalid).
    ProxyCertificate,
    /// An intermediate is self-signed by OpenSSL's reckoning (`EXFLAG_SS`) and is not in the
    /// CA file: OpenSSL stops building there and fails with error 19, "self-signed
    /// certificate in certificate chain".
    SelfSignedIntermediate,
    /// An intermediate is not a CA by OpenSSL's `check_ca`: without `keyCertSign` in a
    /// `keyUsage` extension (`X509_V_ERR_INVALID_CA`, error 79).
    IntermediateNotCa,
    /// A certificate's authority key identifier does not match its issuer in the path
    /// (`X509_check_akid`; OpenSSL would not have chosen this issuer).
    AuthorityKeyMismatch,
    /// More intermediates than the trust anchor's `pathLenConstraint` allows
    /// (`X509_V_ERR_PATH_LENGTH_EXCEEDED`, error 25).
    PathLenExceeded,
    /// A certificate in the path has a `nameConstraints` extension that does not parse.
    MalformedNameConstraints,
    /// A certificate has too many names for the subtrees of a constraint holder above it
    /// (`NAME_CHECK_MAX`; error 1, unspecified).
    TooManyNameChecks,
    /// A name violates the `nameConstraints` of a certificate above it (error 47 or 48).
    NameConstraints(NameProblem),
}

impl PathProblem {
    /// The webpki error to hand back, so that webpki abandons this path and tries others.
    pub(super) fn to_webpki(self) -> webpki::Error {
        match self {
            PathProblem::InvalidCertificate => webpki::Error::MalformedExtensions,
            PathProblem::ProxyCertificate => webpki::Error::ExtensionValueInvalid,
            PathProblem::SelfSignedIntermediate | PathProblem::AuthorityKeyMismatch => {
                webpki::Error::UnknownIssuer
            }
            PathProblem::IntermediateNotCa => webpki::Error::EndEntityUsedAsCa,
            PathProblem::PathLenExceeded => webpki::Error::PathLenConstraintViolated,
            PathProblem::MalformedNameConstraints => webpki::Error::MalformedNameConstraint,
            // webpki's own budget error would not be reported: it ranks with "unknown issuer".
            PathProblem::TooManyNameChecks | PathProblem::NameConstraints(_) => {
                webpki::Error::NameConstraintViolation
            }
        }
    }
}

/// The checks OpenSSL makes on a path webpki has already validated (signatures, validity,
/// `basicConstraints`, `extendedKeyUsage`, the end-entity's names against name constraints,
/// intermediates' `pathLenConstraint`). `intermediates` are in path order, from the
/// end-entity's issuer up, and `in_ca_file` says, for each, whether it is byte-identical to a
/// certificate of the CA file; `anchor` is the self-signed trust anchor the path ends at.
///
/// * no certificate of the path is invalid ([`PeerCert::is_invalid`]) or a proxy
///   certificate;
/// * no intermediate is self-signed ([`PeerCert::is_self_signed`]) unless it is in the CA file
///   (OpenSSL would take that one as the trust anchor and stop there, which only means fewer
///   checks than this path gets);
/// * every intermediate is CA-capable ([`PeerCert::is_ca_capable`]): `check_ca` rejects a
///   `keyUsage` without `keyCertSign` (webpki only asks for `CA:TRUE`);
/// * each certificate's authority key identifier fits its issuer (`X509_check_akid`);
/// * the anchor's `pathLenConstraint` (webpki's `TrustAnchor` has none) bounds the number of
///   intermediates that are not self-issued, as in `check_chain_extensions`;
/// * name constraints from the intermediates and the anchor, applied as `check_name_constraints`
///   does, to the names webpki does not look at (see [`check_names`]), after OpenSSL's bound
///   on the work (`NAME_CHECK_MAX`).
pub(super) fn check_path(
    end_entity: &PeerCert,
    intermediates: &[PeerCert],
    in_ca_file: &[bool],
    anchor: &PeerCert,
) -> Result<(), PathProblem> {
    let chain: Vec<&PeerCert> = std::iter::once(end_entity)
        .chain(intermediates)
        .chain(std::iter::once(anchor))
        .collect();
    if chain.iter().any(|c| c.is_invalid()) {
        return Err(PathProblem::InvalidCertificate);
    }
    if chain.iter().any(|c| c.is_proxy()) {
        return Err(PathProblem::ProxyCertificate);
    }
    if intermediates
        .iter()
        .zip(in_ca_file.iter().chain(std::iter::repeat(&false)))
        .any(|(c, &trusted)| c.is_self_signed() && !trusted)
    {
        return Err(PathProblem::SelfSignedIntermediate);
    }
    if intermediates.iter().any(|c| !c.is_ca_capable()) {
        return Err(PathProblem::IntermediateNotCa);
    }
    for pair in chain.windows(2) {
        if !akid_allows(pair[0].authority_key_identifier(), pair[1]) {
            return Err(PathProblem::AuthorityKeyMismatch);
        }
    }
    if let Some(limit) = anchor.path_len() {
        let below = intermediates.iter().filter(|c| !c.is_self_issued()).count();
        if u64::try_from(below).unwrap_or(u64::MAX) > limit {
            return Err(PathProblem::PathLenExceeded);
        }
    }
    // Name constraints: every certificate but a self-issued intermediate is checked against
    // the constraints of all the certificates above it, the anchor included.
    for (i, cert) in chain.iter().enumerate() {
        if i > 0 && cert.is_self_issued() {
            continue;
        }
        for holder in &chain[i + 1..] {
            match holder.name_constraints() {
                Ext::Absent => {}
                Ext::Invalid => return Err(PathProblem::MalformedNameConstraints),
                Ext::Present(nc) => {
                    let names = cert.name_count();
                    if names > 0 && nc.count > NAME_CHECK_MAX / names {
                        return Err(PathProblem::TooManyNameChecks);
                    }
                    check_names(cert, nc, i == 0).map_err(PathProblem::NameConstraints)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dns_constraints_follow_nc_dns() {
        assert!(dns_match(b"a.example.com", b"example.com") == Match::Yes);
        assert!(dns_match(b"EXAMPLE.com", b"example.com") == Match::Yes);
        // A label boundary is needed unless the base starts with a dot.
        assert!(dns_match(b"badexample.com", b"example.com") == Match::No);
        assert!(dns_match(b"badexample.com", b".example.com") == Match::No);
        assert!(dns_match(b"a.example.com", b".example.com") == Match::Yes);
        // ".example.com" does not cover "example.com" itself.
        assert!(dns_match(b"example.com", b".example.com") == Match::No);
        assert!(dns_match(b"x", b"") == Match::Yes);
        assert!(dns_match(b"evil.org", b"example.com") == Match::No);
    }

    #[test]
    fn email_constraints_follow_nc_email() {
        assert!(email_match(b"a@example.com", b"example.com") == Match::Yes);
        assert!(email_match(b"a@EXAMPLE.com", b"example.com") == Match::Yes);
        assert!(email_match(b"a@x.example.com", b"example.com") == Match::No);
        assert!(email_match(b"a@x.example.com", b".example.com") == Match::Yes);
        assert!(email_match(b"a@example.com", b".example.com") == Match::No);
        assert!(email_match(b"a@example.com", b"a@example.com") == Match::Yes);
        assert!(email_match(b"A@example.com", b"a@example.com") == Match::No);
        assert!(email_match(b"b@example.com", b"a@example.com") == Match::No);
        assert!(email_match(b"example.com", b"example.com") == Match::Syntax);
    }

    fn cn(s: &str) -> AsnString {
        AsnString {
            tag: 12,
            bytes: s.as_bytes().to_vec(),
        }
    }

    #[test]
    fn only_host_name_like_common_names_are_dns_names() {
        let id = |s: &str| cn_as_dns_id(&cn(s)).unwrap();
        assert_eq!(id("evil.org"), Some(b"evil.org".to_vec()));
        assert_eq!(id("a_b.c-d.org"), Some(b"a_b.c-d.org".to_vec()));
        assert_eq!(id("evil.org\0"), Some(b"evil.org".to_vec()));
        assert_eq!(id("node-1"), None); // one label
        assert_eq!(id("evil..org"), None);
        assert_eq!(id("-evil.org"), None);
        assert_eq!(id("evil.-org"), None);
        assert_eq!(id("evil.org."), None); // trailing dot
        assert_eq!(id("Evil Corp.org"), None);
        // An embedded NUL is an error (unsupported name syntax), not "not a host name".
        assert!(cn_as_dns_id(&cn("e\0vil.org")).is_err());
        assert!(cn_as_dns_id(&cn("evil.org\0x")).is_err());
        assert_eq!(id(""), None);
        // A value with no UTF-8 conversion at all is an error, not "not a host name".
        let octets = AsnString {
            tag: 4,
            bytes: b"a.b".to_vec(),
        };
        assert!(cn_as_dns_id(&octets).is_err());
    }

    #[test]
    fn name_constraints_parse_with_min_and_max() {
        // permitted [0] { { dNSName "example.com" } }, excluded [1] { { rfc822Name "a@b",
        // minimum 1 } }
        let der = [
            0x30, 0x20, 0xA0, 0x0F, 0x30, 0x0D, 0x82, 0x0B, b'e', b'x', b'a', b'm', b'p', b'l',
            b'e', b'.', b'c', b'o', b'm', 0xA1, 0x0D, 0x30, 0x0B, 0x81, 0x03, b'a', b'@', b'b',
            0x80, 0x01, 0x01, 0x81, 0x01, 0x05,
        ];
        let parse = |der: &[u8]| exts::name_constraints(der).map(|s| NameConstraintsInfo::new(&s));
        let info = parse(&der).unwrap();
        assert_eq!(info.dns.permitted, [b"example.com".to_vec()]);
        assert!(!info.dns.bad_range);
        assert_eq!(info.email.excluded, [b"a@b".to_vec()]);
        assert!(info.email.bad_range);
        assert_eq!(info.count, 2);
        assert!(parse(&der[..10]).is_none());
        assert!(parse(&[0x30, 0x00]).is_some());
        assert!(parse(&[0x31, 0x00]).is_none());
        // As OpenSSL decodes it: an empty list is fine, excluded before permitted or a repeated
        // list is not, and an explicit `minimum 0` is no range.
        assert!(parse(&[0x30, 0x02, 0xA0, 0x00]).is_some());
        let reversed = [
            0x30, 0x0C, 0xA1, 0x04, 0x30, 0x02, 0x82, 0x00, 0xA0, 0x04, 0x30, 0x02, 0x82, 0x00,
        ];
        assert!(parse(&reversed).is_none());
        let twice = [
            0x30, 0x0C, 0xA0, 0x04, 0x30, 0x02, 0x82, 0x00, 0xA0, 0x04, 0x30, 0x02, 0x82, 0x00,
        ];
        assert!(parse(&twice).is_none());
        let zero_min = [
            0x30, 0x09, 0xA0, 0x07, 0x30, 0x05, 0x82, 0x00, 0x80, 0x01, 0x00,
        ];
        assert!(!parse(&zero_min).unwrap().dns.bad_range);
        // Subtrees of other types count towards NAME_CHECK_MAX.
        let ip = [
            0x30, 0x0C, 0xA0, 0x0A, 0x30, 0x08, 0x87, 0x06, 10, 0, 0, 0, 255, 0,
        ];
        let info = parse(&ip).unwrap();
        assert_eq!((info.count, info.dns.permitted.len()), (1, 0));
    }

    #[test]
    fn supported_extensions_are_openssls_list() {
        for oid in SUPPORTED_EXTENSIONS {
            assert!(is_supported_extension(oid));
        }
        // OpenSSL 3.5 added cRLDistributionPoints and id-pkix-ocsp-nocheck.
        assert!(is_supported_extension("2.5.29.31"));
        assert!(is_supported_extension("1.3.6.1.5.5.7.48.1.5"));
        // subjectKeyIdentifier, authorityKeyIdentifier, freshestCRL, authorityInfoAccess,
        // TLS feature.
        for oid in [
            "2.5.29.14",
            "2.5.29.35",
            "2.5.29.46",
            "1.3.6.1.5.5.7.1.1",
            "1.3.6.1.5.5.7.1.24",
            "1.2.3.4.5",
        ] {
            assert!(!is_supported_extension(oid), "{oid}");
        }
    }
}
