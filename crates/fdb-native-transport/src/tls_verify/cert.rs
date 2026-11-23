//! The certificate fields `verify_peers` rules look at, extracted from a DER certificate.

use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::FromDer;
use x509_parser::x509::X509Name;

use super::rules::{Field, MatchType};

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
        (!out.is_empty()).then_some(out)
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
    self_signed: bool,
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
        let alt_names = match cert.subject_alternative_name() {
            Ok(Some(ext)) => Some(
                ext.value
                    .general_names
                    .iter()
                    .map(alt_name)
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        };
        PeerCert {
            subject: name_entries(cert.subject()),
            issuer: name_entries(cert.issuer()),
            subject_display: display_name(cert.subject()),
            issuer_display: display_name(cert.issuer()),
            alt_names,
            not_before: cert.validity().not_before.timestamp(),
            not_after: cert.validity().not_after.timestamp(),
            self_signed: cert.subject().as_raw() == cert.issuer().as_raw(),
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
            self_signed: false,
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

    /// Whether the subject and issuer names are byte-identical.
    pub fn is_self_signed(&self) -> bool {
        self.self_signed
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
            let any = attr.attr_value();
            Some(NameEntry {
                field,
                value: AsnString {
                    tag: any.tag().0,
                    bytes: any.data.to_vec(),
                },
            })
        })
        .collect()
}

fn alt_name(name: &GeneralName<'_>) -> AltName {
    let ia5 = |s: &str| AsnString {
        tag: 22,
        bytes: s.as_bytes().to_vec(),
    };
    let ia5_raw = |bytes: &[u8]| AsnString {
        tag: 22,
        bytes: bytes.to_vec(),
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
        // An IA5String entry that is not UTF-8, which x509-parser does not decode.
        GeneralName::Invalid(tag, data) => match tag.0 {
            1 => AltName::Email(ia5_raw(data)),
            2 => AltName::Dns(ia5_raw(data)),
            6 => AltName::Uri(ia5_raw(data)),
            7 => AltName::Ip(data.to_vec()),
            _ => AltName::Other,
        },
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
