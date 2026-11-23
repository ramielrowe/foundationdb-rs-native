//! The certificate extensions OpenSSL decodes for every certificate it verifies
//! (`ossl_x509v3_cache_extensions`, `crypto/x509/v3_purp.c`), decoded here as OpenSSL decodes
//! them, and its canonical form of a distinguished name (`x509_name_canon`,
//! `crypto/x509/x_name.c`).
//!
//! OpenSSL marks a certificate `EXFLAG_INVALID` when any of `basicConstraints`, `keyUsage`,
//! `extendedKeyUsage`, `nsCertType`, `subjectKeyIdentifier`, `authorityKeyIdentifier`,
//! `subjectAltName`, `nameConstraints`, `cRLDistributionPoints`, `proxyCertInfo` or the
//! RFC 3779 `ipAddrBlock` and `autonomousSysNum` appears twice or does not decode, when
//! `pathLenConstraint` is negative, and when `keyUsage` has no bit set in its first two octets.
//! Such a certificate can neither issue nor be issued (`ossl_x509_likely_issued` fails), so
//! OpenSSL never accepts a path through it. The decoders below follow OpenSSL's template
//! decoder where a real CA could plausibly differ from DER (an explicitly encoded `cA FALSE`
//! or `minimum 0`, an empty `subjectAltName`, `nameConstraints` or `extendedKeyUsage`,
//! trailing bytes after the extension value, non-minimal length octets), and are stricter than
//! it only where it accepts BER that no certificate tool writes (constructed strings,
//! indefinite lengths): such a certificate is then refused although OpenSSL takes it.
//!
//! Every function returns `None` where OpenSSL's `d2i` would fail.

use super::cert::AsnString;

/// One BER/DER element as OpenSSL's `ASN1_get_object` reads it (definite lengths only).
#[derive(Clone, Copy)]
pub(super) struct Element<'a> {
    /// 0 universal, 1 application, 2 context-specific, 3 private.
    class: u8,
    constructed: bool,
    number: u32,
    pub(super) content: &'a [u8],
    pub(super) rest: &'a [u8],
}

impl Element<'_> {
    /// Whether this is the low-tag-number element whose identifier octet is `id` (for example
    /// `0x30` for SEQUENCE, `0xA0` for `[0]` constructed).
    pub(super) fn is(&self, id: u8) -> bool {
        u32::from(id & 0x1F) == self.number
            && id & 0x1F != 0x1F
            && self.class == id >> 6
            && self.constructed == (id & 0x20 != 0)
    }
}

/// Reads one element. High tag numbers are read; an indefinite length (BER) is refused.
pub(super) fn element(buf: &[u8]) -> Option<Element<'_>> {
    let (&id, mut rest) = buf.split_first()?;
    let mut number = u32::from(id & 0x1F);
    if number == 0x1F {
        number = 0;
        loop {
            let (&b, tail) = rest.split_first()?;
            rest = tail;
            number = number.checked_mul(128)? | u32::from(b & 0x7F);
            if b & 0x80 == 0 {
                break;
            }
        }
    }
    let (&first, tail) = rest.split_first()?;
    rest = tail;
    let len = if first < 0x80 {
        usize::from(first)
    } else {
        let n = usize::from(first & 0x7F);
        if n == 0 || n > rest.len() {
            return None;
        }
        let (octets, tail) = rest.split_at(n);
        rest = tail;
        // OpenSSL skips leading zero octets of a long-form length.
        let mut len = 0usize;
        for &b in octets {
            len = len.checked_mul(256)? | usize::from(b);
        }
        len
    };
    if len > rest.len() {
        return None;
    }
    let (content, rest) = rest.split_at(len);
    Some(Element {
        class: id >> 6,
        constructed: id & 0x20 != 0,
        number,
        content,
        rest,
    })
}

/// The element of an extension value. OpenSSL's `X509V3_EXT_d2i` decodes the first element
/// and ignores anything after it.
fn top(value: &[u8], id: u8) -> Option<Element<'_>> {
    element(value).filter(|e| e.is(id))
}

/// The elements of a SEQUENCE OF or SET OF.
fn items(mut content: &[u8]) -> impl Iterator<Item = Option<Element<'_>>> {
    std::iter::from_fn(move || {
        if content.is_empty() {
            return None;
        }
        let e = element(content);
        content = e.map_or(&[][..], |e| e.rest);
        Some(e)
    })
}

/// The next element if it is the optional one with identifier `id`.
fn optional<'a>(rest: &mut &'a [u8], id: u8) -> Option<Option<Element<'a>>> {
    if rest.is_empty() {
        return Some(None);
    }
    let e = element(rest)?;
    if !e.is(id) {
        return Some(None);
    }
    *rest = e.rest;
    Some(Some(e))
}

/// The next element, which must have identifier `id`.
fn required<'a>(rest: &mut &'a [u8], id: u8) -> Option<Element<'a>> {
    let e = element(rest).filter(|e| e.is(id))?;
    *rest = e.rest;
    Some(e)
}

/// `c2i_ibuf`: an INTEGER's content must not be empty or padded.
pub(super) fn integer_ok(content: &[u8]) -> bool {
    match content {
        [] => false,
        [0, next, ..] => next & 0x80 != 0,
        [0xFF, next, ..] => next & 0x80 == 0,
        _ => true,
    }
}

/// `c2i_ASN1_OBJECT`: not empty, no subidentifier starting with `0x80`, none unterminated.
fn oid_ok(content: &[u8]) -> bool {
    let Some(&last) = content.last() else {
        return false;
    };
    if last & 0x80 != 0 {
        return false;
    }
    let mut at_start = true;
    for &b in content {
        if at_start && b == 0x80 {
            return false;
        }
        at_start = b & 0x80 == 0;
    }
    true
}

/// `c2i_ASN1_BIT_STRING`: the data octets with the unused bits of the last one cleared, or
/// `None` for more than 7 unused bits or no octet at all.
fn bit_string(content: &[u8]) -> Option<Vec<u8>> {
    let (&unused, data) = content.split_first()?;
    if unused > 7 {
        return None;
    }
    let mut data = data.to_vec();
    if let Some(last) = data.last_mut() {
        *last &= 0xFF << unused;
    }
    Some(data)
}

/// An `ANY` (the value of an `otherName`): universal types OpenSSL decodes into a typed value
/// must be well-formed, the rest are kept as they are.
fn any_ok(e: &Element<'_>) -> bool {
    if e.class != 0 {
        return true;
    }
    if e.constructed {
        // SEQUENCE and SET are kept whole; a constructed string (BER) is refused.
        return matches!(e.number, 16 | 17);
    }
    let len = e.content.len();
    match e.number {
        1 => len == 1,
        2 | 10 => integer_ok(e.content),
        3 => bit_string(e.content).is_some(),
        5 => len == 0,
        6 => oid_ok(e.content),
        23 => len >= 13,
        24 => len >= 15,
        28 => len % 4 == 0,
        30 => len % 2 == 0,
        16 | 17 => false,
        _ => true,
    }
}

/// A `DirectoryString` (`ediPartyName`): PrintableString, TeletexString, BMPString,
/// UniversalString or UTF8String.
fn directory_string_ok(e: &Element<'_>) -> bool {
    !e.constructed
        && e.class == 0
        && match e.number {
            12 | 19 | 20 => true,
            28 => e.content.len() % 4 == 0,
            30 => e.content.len() % 2 == 0,
            _ => false,
        }
}

/// The value of an attribute in a name: a type OpenSSL's `X509_NAME_ENTRY` decodes
/// (`ASN1_PRINTABLE`), as tag and content. Types OpenSSL accepts only as "unknown" are refused.
fn attribute_value<'a>(e: &Element<'a>) -> Option<(u32, &'a [u8])> {
    if e.class != 0 {
        return None;
    }
    let ok = if e.constructed {
        e.number == 16
    } else {
        match e.number {
            3 => bit_string(e.content).is_some(),
            12 | 18 | 19 | 20 | 22 => true,
            28 => e.content.len() % 4 == 0,
            30 => e.content.len() % 2 == 0,
            _ => false,
        }
    };
    ok.then_some((e.number, e.content))
}

/// One `AttributeTypeAndValue`: the OID content and the value.
fn attribute(e: Option<Element<'_>>) -> Option<(&[u8], u32, &[u8])> {
    let e = e.filter(|e| e.is(0x30))?;
    let mut rest = e.content;
    let oid = required(&mut rest, 0x06)?;
    if !oid_ok(oid.content) {
        return None;
    }
    let value = element(rest)?;
    if !value.rest.is_empty() {
        return None;
    }
    let (tag, content) = attribute_value(&value)?;
    Some((oid.content, tag, content))
}

/// Appends `bytes` with a length prefix, so that a sequence of these is unambiguous.
fn push_field(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&u32::try_from(bytes.len()).unwrap_or(u32::MAX).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// The string types `x509_name_canon` converts (`ASN1_MASK_CANON`).
fn is_canonical_string_type(tag: u32) -> bool {
    matches!(tag, 12 | 19 | 20 | 22 | 26 | 28 | 30)
}

/// `asn1_string_canon`: leading and trailing white space removed, inner runs of it replaced by
/// one space, ASCII letters lowercased; other octets unchanged.
fn canonical_text(text: &[u8]) -> Vec<u8> {
    let space = |b: &u8| matches!(*b, b' ' | b'\t' | b'\n' | 0x0B | 0x0C | b'\r');
    let start = text.iter().position(|b| !space(b)).unwrap_or(text.len());
    let end = text
        .iter()
        .rposition(|b| !space(b))
        .map_or(start, |i| i + 1);
    let mut out = Vec::with_capacity(end - start);
    let mut in_space = false;
    for b in &text[start..end] {
        if space(b) {
            if !in_space {
                out.push(b' ');
            }
            in_space = true;
        } else {
            out.push(b.to_ascii_lowercase());
            in_space = false;
        }
    }
    out
}

/// OpenSSL's canonical encoding of a distinguished name (`x509_name_canon`), which
/// `X509_NAME_cmp` compares: string values of the common types converted to UTF-8 and put
/// through [`canonical_text`] (so the string type, ASCII case and runs of white space do not
/// matter), the attributes of each RDN in a fixed order, empty RDNs dropped. Two names compare
/// equal in OpenSSL exactly when these are equal.
///
/// `der` is the whole `Name` element. `None` if it does not decode as OpenSSL decodes a name
/// (which then fails to decode the certificate or extension that holds it).
pub(super) fn canonical_name(der: &[u8]) -> Option<Vec<u8>> {
    let name = element(der).filter(|e| e.is(0x30) && e.rest.is_empty())?;
    let mut out = Vec::new();
    for rdn in items(name.content) {
        let rdn = rdn.filter(|e| e.is(0x31))?;
        let mut entries = Vec::new();
        for attr in items(rdn.content) {
            let (oid, tag, content) = attribute(attr)?;
            let mut entry = Vec::new();
            push_field(&mut entry, oid);
            if is_canonical_string_type(tag) {
                let text = AsnString {
                    tag,
                    bytes: content.to_vec(),
                }
                .to_utf8_allowing_empty()?;
                entry.push(0);
                push_field(&mut entry, &canonical_text(&text));
            } else {
                entry.push(1);
                push_field(&mut entry, &tag.to_be_bytes());
                push_field(&mut entry, content);
            }
            entries.push(entry);
        }
        if entries.is_empty() {
            continue;
        }
        entries.sort();
        let mut set = Vec::new();
        for entry in &entries {
            push_field(&mut set, entry);
        }
        push_field(&mut out, &set);
    }
    Some(out)
}

/// `id-on-SmtpUTF8Mailbox` (1.3.6.1.5.5.7.8.9), as OID content octets.
const SMTP_UTF8_MAILBOX: &[u8] = &[0x2B, 0x06, 0x01, 0x05, 0x05, 0x07, 0x08, 0x09];

/// A decoded `GeneralName`, reduced to what the checks here need.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum GeneralName<'a> {
    Rfc822(&'a [u8]),
    Dns(&'a [u8]),
    /// The canonical form of the name ([`canonical_name`]).
    Directory(Vec<u8>),
    /// An `otherName` of type `id-on-SmtpUTF8Mailbox`, which OpenSSL checks against
    /// `rfc822Name` constraints.
    SmtpUtf8Mailbox,
    /// Any other type.
    Other,
}

/// One `GeneralName`, as `GENERAL_NAME` decodes.
fn general_name(e: Option<Element<'_>>) -> Option<GeneralName<'_>> {
    let e = e?;
    if e.class != 2 {
        return None;
    }
    match (e.number, e.constructed) {
        // otherName: SEQUENCE { type-id OID, value [0] EXPLICIT ANY }
        (0, true) => {
            let mut rest = e.content;
            let type_id = required(&mut rest, 0x06)?;
            let value = required(&mut rest, 0xA0)?;
            let inner = element(value.content)?;
            if !rest.is_empty() || !inner.rest.is_empty() || !oid_ok(type_id.content) {
                return None;
            }
            if !any_ok(&inner) {
                return None;
            }
            if type_id.content == SMTP_UTF8_MAILBOX {
                Some(GeneralName::SmtpUtf8Mailbox)
            } else {
                Some(GeneralName::Other)
            }
        }
        (1, false) => Some(GeneralName::Rfc822(e.content)),
        (2, false) => Some(GeneralName::Dns(e.content)),
        // x400Address: kept as an opaque SEQUENCE.
        (3, true) => Some(GeneralName::Other),
        // directoryName: [4] EXPLICIT Name
        (4, true) => {
            let name = element(e.content)?;
            if !name.rest.is_empty() {
                return None;
            }
            canonical_name(e.content).map(GeneralName::Directory)
        }
        // ediPartyName: SEQUENCE { nameAssigner [0] EXPLICIT OPTIONAL, partyName [1] EXPLICIT }
        (5, true) => {
            let mut rest = e.content;
            let explicit_string = |e: Element<'_>| {
                element(e.content).is_some_and(|s| s.rest.is_empty() && directory_string_ok(&s))
            };
            if let Some(assigner) = optional(&mut rest, 0xA0)? {
                if !explicit_string(assigner) {
                    return None;
                }
            }
            let party = required(&mut rest, 0xA1)?;
            (rest.is_empty() && explicit_string(party)).then_some(GeneralName::Other)
        }
        (6 | 7, false) => Some(GeneralName::Other),
        (8, false) => oid_ok(e.content).then_some(GeneralName::Other),
        _ => None,
    }
}

/// `GeneralNames` (SEQUENCE OF GeneralName, possibly empty, as OpenSSL decodes it) given its
/// content.
fn general_names(content: &[u8]) -> Option<Vec<GeneralName<'_>>> {
    items(content).map(general_name).collect()
}

/// `basicConstraints`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BasicConstraints {
    pub(super) ca: bool,
    /// `pathLenConstraint`, if present and representable; OpenSSL's `ASN1_INTEGER_get`
    /// gives -1, "no limit", above `i64::MAX`.
    pub(super) path_len: Option<u64>,
    /// The `pathLenConstraint` is negative (`EXFLAG_INVALID`).
    pub(super) negative_path_len: bool,
}

/// `BASIC_CONSTRAINTS ::= SEQUENCE { cA BOOLEAN DEFAULT FALSE, pathLenConstraint INTEGER
/// OPTIONAL }`; any non-zero `cA` octet is true.
pub(super) fn basic_constraints(value: &[u8]) -> Option<BasicConstraints> {
    let seq = top(value, 0x30)?;
    let mut rest = seq.content;
    let ca = match optional(&mut rest, 0x01)? {
        Some(b) => match b.content {
            [v] => *v != 0,
            _ => return None,
        },
        None => false,
    };
    let path_len = optional(&mut rest, 0x02)?;
    if !rest.is_empty() {
        return None;
    }
    let mut out = BasicConstraints {
        ca,
        path_len: None,
        negative_path_len: false,
    };
    if let Some(int) = path_len {
        if !integer_ok(int.content) {
            return None;
        }
        if int.content[0] & 0x80 != 0 {
            out.negative_path_len = true;
        } else {
            let digits = match int.content {
                [0, tail @ ..] => tail,
                all => all,
            };
            if digits.len() <= 8 {
                let value = digits
                    .iter()
                    .fold(0u64, |acc, &b| (acc << 8) | u64::from(b));
                out.path_len = (value <= i64::MAX as u64).then_some(value);
            }
        }
    }
    Some(out)
}

/// `keyUsage` as OpenSSL caches it (`ex_kusage`): the first data octet, and the second one
/// shifted left by 8, after the unused bits of the last are cleared.
pub(super) fn key_usage(value: &[u8]) -> Option<u16> {
    let bits = bit_string(top(value, 0x03)?.content)?;
    let first = bits.first().copied().unwrap_or(0);
    let second = bits.get(1).copied().unwrap_or(0);
    Some(u16::from(first) | (u16::from(second) << 8))
}

/// `nsCertType` as OpenSSL caches it (`ex_nscert`): the first data octet, after the unused
/// bits of the last are cleared; 0 when there is none.
pub(super) fn ns_cert_type(value: &[u8]) -> Option<u8> {
    let bits = bit_string(top(value, 0x03)?.content)?;
    Some(bits.first().copied().unwrap_or(0))
}

/// `extendedKeyUsage`: the content octets of each OID.
pub(super) fn extended_key_usage(value: &[u8]) -> Option<Vec<&[u8]>> {
    let seq = top(value, 0x30)?;
    items(seq.content)
        .map(|e| {
            e.filter(|e| e.is(0x06) && oid_ok(e.content))
                .map(|e| e.content)
        })
        .collect()
}

/// `subjectKeyIdentifier`: an OCTET STRING.
pub(super) fn subject_key_identifier(value: &[u8]) -> Option<&[u8]> {
    top(value, 0x04).map(|e| e.content)
}

/// An `authorityKeyIdentifier`, reduced to what `X509_check_akid` compares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct AuthorityKeyId {
    pub(super) key_id: Option<Vec<u8>>,
    /// The canonical form of the first `directoryName` of `authorityCertIssuer`.
    pub(super) issuer: Option<Vec<u8>>,
    /// The content octets of `authorityCertSerialNumber`.
    pub(super) serial: Option<Vec<u8>>,
}

/// `AuthorityKeyIdentifier ::= SEQUENCE { keyIdentifier [0] IMPLICIT OCTET STRING OPTIONAL,
/// authorityCertIssuer [1] IMPLICIT GeneralNames OPTIONAL, authorityCertSerialNumber [2]
/// IMPLICIT INTEGER OPTIONAL }`, in that order.
pub(super) fn authority_key_identifier(value: &[u8]) -> Option<AuthorityKeyId> {
    let seq = top(value, 0x30)?;
    let mut rest = seq.content;
    let key_id = optional(&mut rest, 0x80)?.map(|e| e.content.to_vec());
    let issuer = match optional(&mut rest, 0xA1)? {
        Some(names) => general_names(names.content)?
            .into_iter()
            .find_map(|n| match n {
                GeneralName::Directory(canon) => Some(canon),
                _ => None,
            }),
        None => None,
    };
    let serial = match optional(&mut rest, 0x82)? {
        Some(int) if integer_ok(int.content) => Some(int.content.to_vec()),
        Some(_) => return None,
        None => None,
    };
    rest.is_empty().then_some(AuthorityKeyId {
        key_id,
        issuer,
        serial,
    })
}

/// A `subjectAltName`: its names.
pub(super) fn subject_alt_name(value: &[u8]) -> Option<Vec<GeneralName<'_>>> {
    general_names(top(value, 0x30)?.content)
}

/// One `GeneralSubtree` of a `nameConstraints` extension.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Subtree<'a> {
    pub(super) base: GeneralName<'a>,
    /// A `minimum` other than 0, or any `maximum`: OpenSSL cannot evaluate it
    /// (`X509_V_ERR_SUBTREE_MINMAX`).
    pub(super) bad_range: bool,
}

/// `NameConstraints`: the permitted and the excluded subtrees.
pub(super) type Subtrees<'a> = (Vec<Subtree<'a>>, Vec<Subtree<'a>>);

/// `NameConstraints ::= SEQUENCE { permittedSubtrees [0] IMPLICIT GeneralSubtrees OPTIONAL,
/// excludedSubtrees [1] IMPLICIT GeneralSubtrees OPTIONAL }` with `GeneralSubtree ::= SEQUENCE
/// { base GeneralName, minimum [0] IMPLICIT INTEGER DEFAULT 0, maximum [1] IMPLICIT INTEGER
/// OPTIONAL }`. Either list may be empty.
pub(super) fn name_constraints(value: &[u8]) -> Option<Subtrees<'_>> {
    let seq = top(value, 0x30)?;
    let mut rest = seq.content;
    let mut lists = Vec::new();
    for id in [0xA0, 0xA1] {
        let list = match optional(&mut rest, id)? {
            Some(group) => items(group.content)
                .map(|e| {
                    let e = e.filter(|e| e.is(0x30))?;
                    let base_element = element(e.content)?;
                    let base = general_name(Some(base_element))?;
                    let mut tail = base_element.rest;
                    let minimum = optional(&mut tail, 0x80)?;
                    let maximum = optional(&mut tail, 0x81)?;
                    if !tail.is_empty() {
                        return None;
                    }
                    let mut bad_range = maximum.is_some();
                    for int in minimum.iter().chain(maximum.iter()) {
                        if !integer_ok(int.content) {
                            return None;
                        }
                    }
                    if let Some(min) = minimum {
                        bad_range |= min.content.iter().any(|&b| b != 0);
                    }
                    Some(Subtree { base, bad_range })
                })
                .collect::<Option<Vec<_>>>()?,
            None => Vec::new(),
        };
        lists.push(list);
    }
    if !rest.is_empty() {
        return None;
    }
    let excluded = lists.pop()?;
    let permitted = lists.pop()?;
    Some((permitted, excluded))
}

/// `cRLDistributionPoints` decodes, and each point has a `distributionPoint` or a non-empty
/// `cRLIssuer` (`setup_dp`).
///
/// `DistributionPoint ::= SEQUENCE { distributionPoint [0] EXPLICIT DistributionPointName
/// OPTIONAL, reasons [1] IMPLICIT BIT STRING OPTIONAL, cRLIssuer [2] IMPLICIT GeneralNames
/// OPTIONAL }`, `DistributionPointName ::= CHOICE { fullName [0] IMPLICIT GeneralNames,
/// nameRelativeToCRLIssuer [1] IMPLICIT SET OF AttributeTypeAndValue }`.
pub(super) fn crl_distribution_points_ok(value: &[u8]) -> bool {
    let point_ok = |e: Option<Element<'_>>| -> Option<()> {
        let e = e.filter(|e| e.is(0x30))?;
        let mut rest = e.content;
        let name = optional(&mut rest, 0xA0)?;
        if let Some(name) = name {
            let choice = element(name.content)?;
            if !choice.rest.is_empty() {
                return None;
            }
            if choice.is(0xA0) {
                general_names(choice.content)?;
            } else if choice.is(0xA1) {
                for attr in items(choice.content) {
                    attribute(attr)?;
                }
            } else {
                return None;
            }
        }
        if let Some(reasons) = optional(&mut rest, 0x81)? {
            bit_string(reasons.content)?;
        }
        let issuers = match optional(&mut rest, 0xA2)? {
            Some(names) => general_names(names.content)?.len(),
            None => 0,
        };
        (rest.is_empty() && (name.is_some() || issuers > 0)).then_some(())
    };
    top(value, 0x30).is_some_and(|seq| items(seq.content).all(|p| point_ok(p).is_some()))
}

/// RFC 3779 `IPAddrBlocks ::= SEQUENCE OF IPAddressFamily`, `IPAddressFamily ::= SEQUENCE {
/// addressFamily OCTET STRING, ipAddressChoice CHOICE { inherit NULL, addressesOrRanges
/// SEQUENCE OF CHOICE { addressPrefix BIT STRING, addressRange SEQUENCE { min BIT STRING, max
/// BIT STRING } } } }` decodes.
pub(super) fn ip_addr_blocks_ok(value: &[u8]) -> bool {
    let bits_ok =
        |e: Option<Element<'_>>| e.is_some_and(|e| e.is(0x03) && bit_string(e.content).is_some());
    let range_ok = |e: Option<Element<'_>>| -> bool {
        let Some(e) = e else { return false };
        if e.is(0x03) {
            return bit_string(e.content).is_some();
        }
        if !e.is(0x30) {
            return false;
        }
        let min = element(e.content);
        let max = min.and_then(|m| element(m.rest));
        bits_ok(min) && bits_ok(max) && max.is_some_and(|m| m.rest.is_empty())
    };
    let family_ok = |e: Option<Element<'_>>| -> bool {
        let Some(e) = e.filter(|e| e.is(0x30)) else {
            return false;
        };
        let Some(family) = element(e.content).filter(|f| f.is(0x04)) else {
            return false;
        };
        let Some(choice) = element(family.rest) else {
            return false;
        };
        choice.rest.is_empty()
            && ((choice.is(0x05) && choice.content.is_empty())
                || (choice.is(0x30) && items(choice.content).all(range_ok)))
    };
    top(value, 0x30).is_some_and(|seq| items(seq.content).all(family_ok))
}

/// RFC 3779 `ASIdentifiers ::= SEQUENCE { asnum [0] EXPLICIT ASIdentifierChoice OPTIONAL, rdi
/// [1] EXPLICIT ASIdentifierChoice OPTIONAL }`, `ASIdentifierChoice ::= CHOICE { inherit NULL,
/// asIdsOrRanges SEQUENCE OF CHOICE { id INTEGER, range SEQUENCE { min INTEGER, max INTEGER }
/// } }` decodes.
pub(super) fn as_identifiers_ok(value: &[u8]) -> bool {
    let int_ok = |e: Option<Element<'_>>| e.is_some_and(|e| e.is(0x02) && integer_ok(e.content));
    let id_ok = |e: Option<Element<'_>>| -> bool {
        let Some(e) = e else { return false };
        if e.is(0x02) {
            return integer_ok(e.content);
        }
        let min = element(e.content);
        let max = min.and_then(|m| element(m.rest));
        e.is(0x30) && int_ok(min) && int_ok(max) && max.is_some_and(|m| m.rest.is_empty())
    };
    let choice_ok = |e: Element<'_>| -> bool {
        element(e.content).is_some_and(|c| {
            c.rest.is_empty()
                && ((c.is(0x05) && c.content.is_empty())
                    || (c.is(0x30) && items(c.content).all(id_ok)))
        })
    };
    let Some(seq) = top(value, 0x30) else {
        return false;
    };
    let mut rest = seq.content;
    for id in [0xA0, 0xA1] {
        match optional(&mut rest, id) {
            Some(Some(e)) if choice_ok(e) => {}
            Some(None) => {}
            _ => return false,
        }
    }
    rest.is_empty()
}

/// Whether the signature algorithm `sig_oid` (dotted) is one for keys of the algorithm
/// `key_oid`, as OpenSSL's `check_sig_alg_match` decides (`OBJ_find_sigid_algs` and
/// `EVP_PKEY_is_a`). An algorithm not listed here does not match, as in OpenSSL for one it
/// does not know; every algorithm the crypto providers verify is listed.
pub(super) fn signature_matches_key(sig_oid: &str, key_oid: &str) -> bool {
    const RSA: &str = "1.2.840.113549.1.1.1";
    const RSA_PSS: &str = "1.2.840.113549.1.1.10";
    const EC: &str = "1.2.840.10045.2.1";
    const DSA: &str = "1.2.840.10040.4.1";
    const ED25519: &str = "1.3.101.112";
    const ED448: &str = "1.3.101.113";
    let key_for_signature = match sig_oid {
        // md2, md4, md5, sha1, sha256, sha384, sha512, sha224, sha512-224, sha512-256 with RSA
        "1.2.840.113549.1.1.2"
        | "1.2.840.113549.1.1.3"
        | "1.2.840.113549.1.1.4"
        | "1.2.840.113549.1.1.5"
        | "1.2.840.113549.1.1.11"
        | "1.2.840.113549.1.1.12"
        | "1.2.840.113549.1.1.13"
        | "1.2.840.113549.1.1.14"
        | "1.2.840.113549.1.1.15"
        | "1.2.840.113549.1.1.16"
        // sha1WithRSA (OIW), ripemd160WithRSA, sha3-224..512 with RSA
        | "1.3.14.3.2.29"
        | "1.3.36.3.3.1.2"
        | "2.16.840.1.101.3.4.3.13"
        | "2.16.840.1.101.3.4.3.14"
        | "2.16.840.1.101.3.4.3.15"
        | "2.16.840.1.101.3.4.3.16" => RSA,
        // RSASSA-PSS: an RSA-PSS key, or a plain RSA key
        RSA_PSS => return key_oid == RSA_PSS || key_oid == RSA,
        // ecdsa-with-SHA1, -SHA224, -SHA256, -SHA384, -SHA512, -SHA3-224..512
        "1.2.840.10045.4.1"
        | "1.2.840.10045.4.3.1"
        | "1.2.840.10045.4.3.2"
        | "1.2.840.10045.4.3.3"
        | "1.2.840.10045.4.3.4"
        | "2.16.840.1.101.3.4.3.9"
        | "2.16.840.1.101.3.4.3.10"
        | "2.16.840.1.101.3.4.3.11"
        | "2.16.840.1.101.3.4.3.12" => EC,
        // dsaWithSHA1, dsa-with-SHA224..512, dsa-with-SHA3-224..512
        "1.2.840.10040.4.3"
        | "2.16.840.1.101.3.4.3.1"
        | "2.16.840.1.101.3.4.3.2"
        | "2.16.840.1.101.3.4.3.3"
        | "2.16.840.1.101.3.4.3.4"
        | "2.16.840.1.101.3.4.3.5"
        | "2.16.840.1.101.3.4.3.6"
        | "2.16.840.1.101.3.4.3.7"
        | "2.16.840.1.101.3.4.3.8" => DSA,
        ED25519 => ED25519,
        ED448 => ED448,
        _ => return false,
    };
    key_oid == key_for_signature
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_follow_c2i_ibuf() {
        assert!(integer_ok(&[0]));
        assert!(integer_ok(&[0x7F]));
        assert!(integer_ok(&[0x00, 0x80]));
        assert!(integer_ok(&[0xFF]));
        assert!(integer_ok(&[0xFF, 0x7F]));
        assert!(!integer_ok(&[]));
        assert!(!integer_ok(&[0x00, 0x01]));
        assert!(!integer_ok(&[0xFF, 0x80]));
    }

    #[test]
    fn object_identifiers_follow_c2i_asn1_object() {
        assert!(oid_ok(&[0x2B, 0x06, 0x01]));
        assert!(oid_ok(&[0x2B, 0x86, 0x01]));
        assert!(!oid_ok(&[]));
        assert!(!oid_ok(&[0x2B, 0x86]));
        assert!(!oid_ok(&[0x2B, 0x80, 0x01]));
        assert!(!oid_ok(&[0x80, 0x01]));
    }

    #[test]
    fn bit_strings_clear_unused_bits() {
        assert_eq!(bit_string(&[7, 0x40]), Some(vec![0x00]));
        assert_eq!(bit_string(&[6, 0x40]), Some(vec![0x40]));
        assert_eq!(bit_string(&[1, 0xFF, 0xFF]), Some(vec![0xFF, 0xFE]));
        assert_eq!(bit_string(&[7]), Some(vec![]));
        assert_eq!(bit_string(&[8, 0x80]), None);
        assert_eq!(bit_string(&[]), None);
        // keyUsage digitalSignature (0x80), decipherOnly (bit 8), and none.
        assert_eq!(key_usage(&[0x03, 0x02, 0x07, 0x80]), Some(0x80));
        assert_eq!(key_usage(&[0x03, 0x03, 0x07, 0x00, 0x80]), Some(0x8000));
        assert_eq!(key_usage(&[0x03, 0x02, 0x01, 0x01]), Some(0));
        // nsCertType sslServer with 7 unused bits is not sslServer.
        assert_eq!(ns_cert_type(&[0x03, 0x02, 0x07, 0x40]), Some(0));
        assert_eq!(ns_cert_type(&[0x03, 0x02, 0x06, 0x40]), Some(0x40));
        assert_eq!(ns_cert_type(&[0x03, 0x02, 0x08, 0x40]), None);
    }

    #[test]
    fn elements_with_long_lengths_and_high_tags() {
        // Non-minimal long-form length, as OpenSSL reads it.
        let e = element(&[0x04, 0x81, 0x01, b'a', 0xFF]).unwrap();
        assert!(e.is(0x04));
        assert_eq!(e.content, b"a");
        assert_eq!(e.rest, &[0xFF]);
        // [UNIVERSAL 34], high tag number form.
        let e = element(&[0x1F, 0x22, 0x01, b'x']).unwrap();
        assert_eq!((e.class, e.number), (0, 34));
        assert!(!e.is(0x1F));
        // Indefinite length and truncation.
        assert!(element(&[0x30, 0x80, 0x00, 0x00]).is_none());
        assert!(element(&[0x04, 0x02, b'a']).is_none());
        assert!(element(&[0x04, 0x84, 0xFF, 0xFF, 0xFF, 0xFF]).is_none());
    }

    #[test]
    fn basic_constraints_as_openssl_reads_them() {
        let bc = |v: &[u8]| basic_constraints(v);
        assert_eq!(
            bc(&[0x30, 0x03, 0x01, 0x01, 0xFF]),
            Some(BasicConstraints {
                ca: true,
                path_len: None,
                negative_path_len: false
            })
        );
        // Explicit cA FALSE (not DER, but common enough), and any non-zero octet is TRUE.
        assert!(!bc(&[0x30, 0x03, 0x01, 0x01, 0x00]).unwrap().ca);
        assert!(bc(&[0x30, 0x03, 0x01, 0x01, 0x01]).unwrap().ca);
        assert_eq!(
            bc(&[0x30, 0x06, 0x01, 0x01, 0xFF, 0x02, 0x01, 0x02])
                .unwrap()
                .path_len,
            Some(2)
        );
        assert!(
            bc(&[0x30, 0x06, 0x01, 0x01, 0xFF, 0x02, 0x01, 0xFF])
                .unwrap()
                .negative_path_len
        );
        assert!(bc(&[0x30, 0x00]).is_some());
        // pathLen before cA, an unknown element, a padded or empty INTEGER, a 2-octet BOOLEAN.
        assert!(bc(&[0x30, 0x06, 0x02, 0x01, 0x01, 0x01, 0x01, 0xFF]).is_none());
        assert!(bc(&[0x30, 0x03, 0x00, 0x01, 0xFF]).is_none());
        assert!(bc(&[0x30, 0x07, 0x01, 0x01, 0xFF, 0x02, 0x02, 0x00, 0x01]).is_none());
        assert!(bc(&[0x30, 0x05, 0x01, 0x01, 0xFF, 0x02, 0x00]).is_none());
        assert!(bc(&[0x30, 0x04, 0x01, 0x02, 0xFF, 0xFF]).is_none());
        assert!(bc(&[0x31, 0x00]).is_none());
    }

    #[test]
    fn authority_key_identifiers_keep_the_serial_octets() {
        // keyIdentifier, then serial 0x00FF and 0xFF (-1) differ.
        let akid =
            authority_key_identifier(&[0x30, 0x07, 0x80, 0x01, b'k', 0x82, 0x02, 0x00, 0xFF])
                .unwrap();
        assert_eq!(akid.key_id, Some(b"k".to_vec()));
        assert_eq!(akid.serial, Some(vec![0x00, 0xFF]));
        let akid = authority_key_identifier(&[0x30, 0x03, 0x82, 0x01, 0xFF]).unwrap();
        assert_eq!(akid.serial, Some(vec![0xFF]));
        // Out of order, a padded serial, a wrong tag.
        assert!(
            authority_key_identifier(&[0x30, 0x06, 0x82, 0x01, 0x01, 0x80, 0x01, b'k']).is_none()
        );
        assert!(authority_key_identifier(&[0x30, 0x04, 0x82, 0x02, 0x00, 0x01]).is_none());
        assert!(authority_key_identifier(&[0x30, 0x03, 0x00, 0x01, b'k']).is_none());
        assert!(authority_key_identifier(&[0x30, 0x00]).is_some());
    }

    #[test]
    fn names_compare_canonically() {
        fn name(entries: &[(u8, &[u8])]) -> Vec<u8> {
            // CN attributes, one per RDN.
            let mut seq = Vec::new();
            for (tag, value) in entries {
                let mut atv = vec![0x06, 0x03, 0x55, 0x04, 0x03, *tag, value.len() as u8];
                atv.extend_from_slice(value);
                let mut set = vec![0x30, atv.len() as u8];
                set.extend(atv);
                seq.push(0x31);
                seq.push(set.len() as u8);
                seq.extend(set);
            }
            let mut out = vec![0x30, seq.len() as u8];
            out.extend(seq);
            out
        }
        let c = |entries: &[(u8, &[u8])]| canonical_name(&name(entries)).unwrap();
        assert_eq!(c(&[(0x0C, b"Same")]), c(&[(0x13, b"same")]));
        assert_eq!(c(&[(0x0C, b"  a \t\n b ")]), c(&[(0x16, b"A B")]));
        assert_eq!(c(&[(0x14, &[0xE9])]), c(&[(0x0C, "\u{e9}".as_bytes())]));
        assert_eq!(c(&[(0x1E, &[0, b'A'])]), c(&[(0x0C, b"a")]));
        // Not folded: non-ASCII case, no-break space, NUL; NumericString is not converted.
        assert_ne!(
            c(&[(0x0C, "\u{e9}".as_bytes())]),
            c(&[(0x0C, "\u{c9}".as_bytes())])
        );
        assert_ne!(c(&[(0x0C, "a\u{a0}b".as_bytes())]), c(&[(0x0C, b"a b")]));
        assert_ne!(c(&[(0x0C, b"a\0")]), c(&[(0x0C, b"a")]));
        assert_ne!(c(&[(0x12, b"12")]), c(&[(0x0C, b"12")]));
        assert_eq!(c(&[(0x12, b"12")]), c(&[(0x12, b"12")]));
        assert_ne!(
            c(&[(0x0C, b"a"), (0x0C, b"b")]),
            c(&[(0x0C, b"b"), (0x0C, b"a")])
        );
        // An empty RDN vanishes.
        assert_eq!(
            canonical_name(&[0x30, 0x02, 0x31, 0x00]),
            canonical_name(&[0x30, 0x00])
        );
        // A value OpenSSL cannot convert makes the name undecodable.
        assert_eq!(canonical_name(&name(&[(0x0C, &[0xFF])])), None);
        assert_eq!(canonical_name(&name(&[(0x02, &[0x01])])), None);
    }

    #[test]
    fn signature_algorithms_match_their_keys() {
        let rsa = "1.2.840.113549.1.1.1";
        let ec = "1.2.840.10045.2.1";
        assert!(signature_matches_key("1.2.840.113549.1.1.11", rsa));
        assert!(signature_matches_key("1.2.840.113549.1.1.10", rsa));
        assert!(signature_matches_key("1.2.840.10045.4.3.2", ec));
        assert!(signature_matches_key("1.3.101.112", "1.3.101.112"));
        assert!(!signature_matches_key("1.2.840.10045.4.3.2", rsa));
        assert!(!signature_matches_key("1.2.840.113549.1.1.11", ec));
        assert!(!signature_matches_key("1.3.101.112", ec));
        assert!(!signature_matches_key("1.2.3.4", ec));
    }
}
