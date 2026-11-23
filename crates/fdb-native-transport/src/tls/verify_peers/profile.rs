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

use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::{GeneralName, ParsedExtension, X509Extension};

use super::cert::{AltName, AsnString, Ext, PeerCert};
use super::rules::Field;
use super::selfsig::tlv;

/// OpenSSL's `X509_supported_extension` list (`crypto/x509/v3_purp.c`, `supported_nids`): the
/// extensions its verifier "uses", so that a *critical* one is not an "unhandled critical
/// extension" (error 34, `X509_V_ERR_UNHANDLED_CRITICAL_EXTENSION`). Every other critical
/// extension makes a certificate unusable, whatever its role in the chain: this includes
/// `subjectKeyIdentifier`, `authorityKeyIdentifier`, `cRLDistributionPoints`,
/// `authorityInfoAccess` and `freshestCRL`, which are normally not critical.
///
/// The two RFC 3779 entries are compiled in unless OpenSSL is built with `no-rfc3779`.
const SUPPORTED_EXTENSIONS: [&str; 13] = [
    "2.16.840.1.113730.1.1", // nsCertType
    "2.5.29.15",             // keyUsage
    "2.5.29.17",             // subjectAltName
    "2.5.29.19",             // basicConstraints
    "2.5.29.32",             // certificatePolicies
    "2.5.29.37",             // extendedKeyUsage
    "1.3.6.1.5.5.7.1.7",     // sbgp-ipAddrBlock (RFC 3779)
    "1.3.6.1.5.5.7.1.8",     // sbgp-autonomousSysNum (RFC 3779)
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

/// The content octets of an `INTEGER` without leading zero octets (a positive serial number
/// compared as a number, as `ASN1_INTEGER_cmp` does).
pub(super) fn trim_leading_zeros(mut bytes: &[u8]) -> &[u8] {
    while let [0, rest @ ..] = bytes {
        bytes = rest;
    }
    bytes
}

/// The one extension of the certificate with this OID: none, one, or a duplicate (invalid).
fn find<'c, 'a>(cert: &'c X509Certificate<'a>, oid: &str) -> Ext<&'c X509Extension<'a>> {
    let mut found = cert
        .extensions()
        .iter()
        .filter(|ext| ext.oid.to_id_string() == oid);
    match (found.next(), found.next()) {
        (None, _) => Ext::Absent,
        (Some(ext), None) => Ext::Present(ext),
        (Some(_), Some(_)) => Ext::Invalid,
    }
}

/// An authorityKeyIdentifier, reduced to what `X509_check_akid` compares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Akid {
    key_id: Option<Vec<u8>>,
    /// The DER of the first `directoryName` of `authorityCertIssuer`. OpenSSL compares it to
    /// the *issuer* name of the issuing certificate (not its subject).
    issuer: Option<Vec<u8>>,
    /// `authorityCertSerialNumber`, leading zero octets removed.
    serial: Option<Vec<u8>>,
}

pub(super) fn authority_key_identifier(cert: &X509Certificate<'_>) -> Ext<Akid> {
    match find(cert, "2.5.29.35") {
        Ext::Absent => Ext::Absent,
        Ext::Present(ext) => match ext.parsed_extension() {
            ParsedExtension::AuthorityKeyIdentifier(akid) => Ext::Present(Akid {
                key_id: akid.key_identifier.as_ref().map(|k| k.0.to_vec()),
                issuer: akid.authority_cert_issuer.as_ref().and_then(|names| {
                    names.iter().find_map(|name| match name {
                        GeneralName::DirectoryName(dn) => Some(dn.as_raw().to_vec()),
                        _ => None,
                    })
                }),
                serial: akid
                    .authority_cert_serial
                    .map(|s| trim_leading_zeros(s).to_vec()),
            }),
            _ => Ext::Invalid,
        },
        Ext::Invalid => Ext::Invalid,
    }
}

pub(super) fn subject_key_identifier(cert: &X509Certificate<'_>) -> Ext<Vec<u8>> {
    match find(cert, "2.5.29.14") {
        Ext::Absent => Ext::Absent,
        Ext::Present(ext) => match ext.parsed_extension() {
            ParsedExtension::SubjectKeyIdentifier(id) => Ext::Present(id.0.to_vec()),
            _ => Ext::Invalid,
        },
        Ext::Invalid => Ext::Invalid,
    }
}

/// OpenSSL's `X509_check_akid(issuer, akid)`: does a certificate whose authority key
/// identifier is `akid` plausibly come from `issuer`? `Absent` always does.
///
/// A key identifier that differs from the issuer's subject key identifier (when it has one),
/// a serial number that is not the issuer's, and an issuer name that is not the *issuer's
/// issuer* all say no.
pub(super) fn akid_allows(akid: &Ext<Akid>, issuer: &PeerCert) -> bool {
    let akid = match akid {
        Ext::Absent => return true,
        Ext::Invalid => return false,
        Ext::Present(akid) => akid,
    };
    match (&akid.key_id, issuer.subject_key_identifier()) {
        (_, Ext::Invalid) => return false,
        (Some(key_id), Ext::Present(skid)) if key_id != skid => return false,
        _ => {}
    }
    if akid
        .serial
        .as_deref()
        .is_some_and(|serial| serial != issuer.serial())
    {
        return false;
    }
    if akid
        .issuer
        .as_deref()
        .is_some_and(|name| name != issuer.issuer_raw())
    {
        return false;
    }
    true
}

/// Whether the Netscape certificate type has the `sslServer` bit (`NS_SSL_SERVER`, `0x40`, the
/// second bit of the first octet), as OpenSSL reads it: only the first octet counts, and an
/// empty bit string is type 0.
pub(super) fn ns_cert_type_ssl_server(cert: &X509Certificate<'_>) -> Ext<bool> {
    match find(cert, "2.16.840.1.113730.1.1") {
        Ext::Absent => Ext::Absent,
        Ext::Invalid => Ext::Invalid,
        Ext::Present(ext) => match tlv(ext.value) {
            // BIT STRING: the first content octet counts the unused bits.
            Some(bits) if bits.tag == 0x03 && bits.rest.is_empty() && !bits.content.is_empty() => {
                Ext::Present(bits.content.get(1).is_some_and(|b| b & 0x40 != 0))
            }
            _ => Ext::Invalid,
        },
    }
}

/// The `dNSName` and `rfc822Name` subtrees of a certificate's `nameConstraints`; the other
/// kinds are webpki's business.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct NameConstraintsInfo {
    dns: Subtrees,
    email: Subtrees,
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

pub(super) fn name_constraints(cert: &X509Certificate<'_>) -> Ext<NameConstraintsInfo> {
    match find(cert, "2.5.29.30") {
        Ext::Absent => Ext::Absent,
        Ext::Invalid => Ext::Invalid,
        Ext::Present(ext) => parse_name_constraints(ext.value).map_or(Ext::Invalid, Ext::Present),
    }
}

/// `NameConstraints ::= SEQUENCE { permittedSubtrees [0] GeneralSubtrees OPTIONAL,
/// excludedSubtrees [1] GeneralSubtrees OPTIONAL }`. `GeneralSubtree ::= SEQUENCE { base
/// GeneralName, minimum [0] BaseDistance DEFAULT 0, maximum [1] BaseDistance OPTIONAL }`.
///
/// Parsed here because x509-parser drops `minimum` and `maximum`.
fn parse_name_constraints(value: &[u8]) -> Option<NameConstraintsInfo> {
    let seq = tlv(value)?;
    if seq.tag != 0x30 || !seq.rest.is_empty() {
        return None;
    }
    let mut info = NameConstraintsInfo::default();
    let mut rest = seq.content;
    while !rest.is_empty() {
        let group = tlv(rest)?;
        rest = group.rest;
        let permitted = match group.tag {
            0xA0 => true,
            0xA1 => false,
            _ => return None,
        };
        let mut subtrees = group.content;
        if subtrees.is_empty() {
            return None;
        }
        while !subtrees.is_empty() {
            let subtree = tlv(subtrees)?;
            subtrees = subtree.rest;
            if subtree.tag != 0x30 {
                return None;
            }
            let base = tlv(subtree.content)?;
            let mut bad_range = false;
            let mut tail = base.rest;
            while !tail.is_empty() {
                let field = tlv(tail)?;
                tail = field.rest;
                match field.tag {
                    // minimum: only 0 is acceptable
                    0x80 => bad_range |= field.content.iter().any(|&b| b != 0),
                    // maximum: any value is
                    0x81 => bad_range = true,
                    _ => return None,
                }
            }
            let target = match base.tag {
                0x82 => &mut info.dns,
                0x81 => &mut info.email,
                _ => continue,
            };
            target.bad_range |= bad_range;
            if permitted {
                target.permitted.push(base.content.to_vec());
            } else {
                target.excluded.push(base.content.to_vec());
            }
        }
    }
    Some(info)
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
/// `None` if it does not. `Err` if the value cannot be converted to UTF-8 at all.
///
/// A name "looks like a host name" if it has at least two labels of letters, digits,
/// underscores and interior hyphens (`a.b`; not `localhost`, not `a..b`, not `-a.b`), after
/// trailing NULs are dropped; an embedded NUL disqualifies it. This is deliberately the
/// legacy syntax: OpenSSL checks a CN against DNS constraints only when it would pass for a
/// host name.
fn cn_as_dns_id(cn: &AsnString) -> Result<Option<Vec<u8>>, ()> {
    let mut value = cn.to_utf8_allowing_empty().ok_or(())?;
    while value.last() == Some(&0) {
        value.pop();
    }
    if value.contains(&0) {
        return Ok(None);
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

/// `NAME_CONSTRAINTS_check` (the parts webpki does not do) and `NAME_CONSTRAINTS_check_CN` for
/// `cert` against `nc`: the subject's `emailAddress` attributes, the DNS and e-mail
/// alternative names, and, for the end-entity certificate with no `dNSName` alternative name,
/// each host-name-looking `commonName` as a DNS name.
fn check_names(
    cert: &PeerCert,
    nc: &NameConstraintsInfo,
    end_entity: bool,
) -> Result<(), NameProblem> {
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
    /// A name violates the `nameConstraints` of a certificate above it (error 47 or 48).
    NameConstraints(NameProblem),
}

impl PathProblem {
    /// The webpki error to hand back, so that webpki abandons this path and tries others.
    pub(super) fn to_webpki(self) -> webpki::Error {
        match self {
            PathProblem::IntermediateNotCa => webpki::Error::EndEntityUsedAsCa,
            PathProblem::AuthorityKeyMismatch => webpki::Error::UnknownIssuer,
            PathProblem::PathLenExceeded => webpki::Error::PathLenConstraintViolated,
            PathProblem::MalformedNameConstraints => webpki::Error::MalformedNameConstraint,
            PathProblem::NameConstraints(_) => webpki::Error::NameConstraintViolation,
        }
    }
}

/// The checks OpenSSL makes on a path webpki has already validated (signatures, validity,
/// `basicConstraints`, `extendedKeyUsage`, the end-entity's names against name constraints,
/// intermediates' `pathLenConstraint`). `intermediates` are in path order, from the
/// end-entity's issuer up; `anchor` is the self-signed trust anchor the path ends at.
///
/// * every intermediate is CA-capable ([`PeerCert::is_ca_capable`]): `check_ca` rejects a
///   `keyUsage` without `keyCertSign` (webpki only asks for `CA:TRUE`);
/// * each certificate's authority key identifier fits its issuer (`X509_check_akid`);
/// * the anchor's `pathLenConstraint` (webpki's `TrustAnchor` has none) bounds the number of
///   intermediates that are not self-issued, as in `check_chain_extensions`;
/// * name constraints from the intermediates and the anchor, applied as `check_name_constraints`
///   does, to the names webpki does not look at: see [`check_names`].
pub(super) fn check_path(
    end_entity: &PeerCert,
    intermediates: &[PeerCert],
    anchor: &PeerCert,
) -> Result<(), PathProblem> {
    if intermediates.iter().any(|c| !c.is_ca_capable()) {
        return Err(PathProblem::IntermediateNotCa);
    }
    let chain: Vec<&PeerCert> = std::iter::once(end_entity)
        .chain(intermediates)
        .chain(std::iter::once(anchor))
        .collect();
    for pair in chain.windows(2) {
        if !akid_allows(pair[0].authority_key_identifier(), pair[1]) {
            return Err(PathProblem::AuthorityKeyMismatch);
        }
    }
    if let Some(limit) = anchor.path_len() {
        let below = intermediates.iter().filter(|c| !c.is_self_issued()).count();
        if below > usize::try_from(limit).unwrap_or(usize::MAX) {
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
        assert_eq!(id("e\0vil.org"), None);
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
        let info = parse_name_constraints(&der).unwrap();
        assert_eq!(info.dns.permitted, [b"example.com".to_vec()]);
        assert!(!info.dns.bad_range);
        assert_eq!(info.email.excluded, [b"a@b".to_vec()]);
        assert!(info.email.bad_range);
        assert!(parse_name_constraints(&der[..10]).is_none());
        assert!(parse_name_constraints(&[0x30, 0x00]).is_some());
        assert!(parse_name_constraints(&[0x31, 0x00]).is_none());
    }

    #[test]
    fn supported_extensions_are_openssls_list() {
        for oid in SUPPORTED_EXTENSIONS {
            assert!(is_supported_extension(oid));
        }
        // subjectKeyIdentifier, authorityKeyIdentifier, cRLDistributionPoints, freshestCRL.
        for oid in [
            "2.5.29.14",
            "2.5.29.35",
            "2.5.29.31",
            "2.5.29.46",
            "1.2.3.4.5",
        ] {
            assert!(!is_supported_extension(oid), "{oid}");
        }
    }

    #[test]
    fn leading_zeros_are_trimmed() {
        assert_eq!(trim_leading_zeros(&[0, 0, 1, 0]), &[1, 0]);
        assert_eq!(trim_leading_zeros(&[0]), &[] as &[u8]);
        assert_eq!(trim_leading_zeros(&[]), &[] as &[u8]);
    }
}
