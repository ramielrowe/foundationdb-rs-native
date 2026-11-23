//! The `verify_peers` rule language: grammar and parser.
//!
//! This is a port of FDB's `TLSPolicy::set_verify_peers` and `TLSPolicy::Rule::Rule`
//! (`flow/TLSConfig.actor.cpp` on `release-7.4`, `flow/TLSConfig.cpp` on `release-8.0`; the
//! two branches parse identically), with the `de4514` and `abbrevToNID` helpers they use.
//! See the [module docs](super) for the grammar and for where this port departs from FDB.

use std::collections::BTreeSet;
use std::fmt;

/// How a criterion's value is compared with the certificate's (`MatchType` in
/// `flow/include/flow/TLSConfig.actor.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MatchType {
    /// `field=value`: the whole value.
    Exact,
    /// `field>=value`: the certificate's value starts with `value`.
    Prefix,
    /// `field<=value`: the certificate's value ends with `value`.
    Suffix,
}

/// Where in the certificate a [`Field`] lives (`X509Location`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Location {
    /// An attribute of the subject or issuer distinguished name.
    Name,
    /// An X.509 extension (only `subjectAltName`).
    Extension,
}

/// A certificate field a criterion can name: the nine short names FDB's `abbrevToNID`
/// accepts. Names are case sensitive and OIDs are not accepted.
///
/// The variants are declared in the order of the OpenSSL NIDs FDB keys its criteria sets by
/// ([`Field::nid`]), so the derived `Ord` is the order FDB iterates (and reports) criteria
/// in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Field {
    /// `CN`, commonName (2.5.4.3).
    Cn,
    /// `C`, countryName (2.5.4.6).
    C,
    /// `L`, localityName (2.5.4.7).
    L,
    /// `ST`, stateOrProvinceName (2.5.4.8).
    St,
    /// `O`, organizationName (2.5.4.10).
    O,
    /// `OU`, organizationalUnitName (2.5.4.11).
    Ou,
    /// `subjectAltName`, an extension (2.5.29.17).
    SubjectAltName,
    /// `DC`, domainComponent (0.9.2342.19200300.100.1.25).
    Dc,
    /// `UID`, userId (0.9.2342.19200300.100.1.1).
    Uid,
}

impl Field {
    /// The field named by `name`, exactly as `abbrevToNID` spells them.
    pub fn from_short_name(name: &str) -> Option<Field> {
        Some(match name {
            "CN" => Field::Cn,
            "C" => Field::C,
            "L" => Field::L,
            "ST" => Field::St,
            "O" => Field::O,
            "OU" => Field::Ou,
            "subjectAltName" => Field::SubjectAltName,
            "DC" => Field::Dc,
            "UID" => Field::Uid,
            _ => return None,
        })
    }

    /// The short name rules spell the field with.
    pub fn short_name(self) -> &'static str {
        match self {
            Field::Cn => "CN",
            Field::C => "C",
            Field::L => "L",
            Field::St => "ST",
            Field::O => "O",
            Field::Ou => "OU",
            Field::SubjectAltName => "subjectAltName",
            Field::Dc => "DC",
            Field::Uid => "UID",
        }
    }

    /// The OpenSSL NID FDB identifies the field by (`OBJ_sn2nid`); FDB prints it in a rule's
    /// `toString`.
    pub fn nid(self) -> i32 {
        match self {
            Field::Cn => 13,
            Field::C => 14,
            Field::L => 15,
            Field::St => 16,
            Field::O => 17,
            Field::Ou => 18,
            Field::SubjectAltName => 85,
            Field::Dc => 391,
            Field::Uid => 458,
        }
    }

    /// The field's OID in dotted form.
    pub fn oid(self) -> &'static str {
        match self {
            Field::Cn => "2.5.4.3",
            Field::C => "2.5.4.6",
            Field::L => "2.5.4.7",
            Field::St => "2.5.4.8",
            Field::O => "2.5.4.10",
            Field::Ou => "2.5.4.11",
            Field::SubjectAltName => "2.5.29.17",
            Field::Dc => "0.9.2342.19200300.100.1.25",
            Field::Uid => "0.9.2342.19200300.100.1.1",
        }
    }

    /// Where the field lives (`locationForNID`): the extension is the only field outside the
    /// distinguished name.
    pub fn location(self) -> Location {
        match self {
            Field::SubjectAltName => Location::Extension,
            _ => Location::Name,
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.short_name())
    }
}

/// One `field[<>]?=value` condition (FDB's `std::pair<NID, Criteria>`).
///
/// Ordering is FDB's: by field, then value, then match type, then location.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Criterion {
    field: Field,
    value: Vec<u8>,
    match_type: MatchType,
    location: Location,
}

impl Criterion {
    /// A criterion on `field`; its location is the field's.
    pub fn new(field: Field, match_type: MatchType, value: impl Into<Vec<u8>>) -> Self {
        Criterion {
            field,
            value: value.into(),
            match_type,
            location: field.location(),
        }
    }

    /// The field the criterion tests.
    pub fn field(&self) -> Field {
        self.field
    }

    /// The unescaped value, as raw bytes: a `\XX` escape can produce any byte, so the value
    /// is not necessarily UTF-8. For `subjectAltName` it still carries the `DNS:`/`URI:`/...
    /// prefix.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// How the value is compared.
    pub fn match_type(&self) -> MatchType {
        self.match_type
    }

    /// Where the field lives.
    pub fn location(&self) -> Location {
        self.location
    }
}

/// Which certificate of the peer's chain a criterion applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Scope {
    /// `S.` or `Subject.`, or no prefix: the subject name of the peer's own certificate.
    Subject,
    /// `I.` or `Issuer.`: the issuer name of the peer's own certificate.
    Issuer,
    /// `R.` or `Root.`: the subject name of the last certificate of the verified chain.
    Root,
}

/// One rule: the criteria between two unescaped `|`s (FDB's `TLSPolicy::Rule`).
///
/// The criteria sets are sets, so repeating a criterion is harmless (FDB logs a warning
/// when an identical key is inserted twice, which here is silent), and two criteria on the
/// same field with different values must both match.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    verify_cert: bool,
    verify_time: bool,
    subject: BTreeSet<Criterion>,
    issuer: BTreeSet<Criterion>,
    root: BTreeSet<Criterion>,
}

impl Default for Rule {
    /// The rule of an empty string: `Check.Valid=1`, `Check.Unexpired=1`, no criteria.
    fn default() -> Self {
        Rule {
            verify_cert: true,
            verify_time: true,
            subject: BTreeSet::new(),
            issuer: BTreeSet::new(),
            root: BTreeSet::new(),
        }
    }
}

impl Rule {
    /// `Check.Valid` (`verify_cert`): the rule's setting, default `true`.
    ///
    /// FDB reads this flag across *all* rules: if any rule has it false, no rule's criteria
    /// are evaluated (see [`VerifyPeers::check_valid_disabled`]).
    pub fn check_valid(&self) -> bool {
        self.verify_cert
    }

    /// `Check.Unexpired` (`verify_time`): the rule's setting, default `true`. FDB parses and
    /// stores it but never reads it back (see the [module docs](super)).
    pub fn check_unexpired(&self) -> bool {
        self.verify_time
    }

    /// The criteria for `scope`, in FDB's evaluation order.
    pub fn criteria(&self, scope: Scope) -> impl Iterator<Item = &Criterion> {
        self.set(scope).iter()
    }

    pub(super) fn set(&self, scope: Scope) -> &BTreeSet<Criterion> {
        match scope {
            Scope::Subject => &self.subject,
            Scope::Issuer => &self.issuer,
            Scope::Root => &self.root,
        }
    }

    fn set_mut(&mut self, scope: Scope) -> &mut BTreeSet<Criterion> {
        match scope {
            Scope::Subject => &mut self.subject,
            Scope::Issuer => &mut self.issuer,
            Scope::Root => &mut self.root,
        }
    }

    /// Whether the rule has any criteria on the root certificate.
    pub fn has_root_criteria(&self) -> bool {
        !self.root.is_empty()
    }
}

/// Formats like FDB's `Rule::toString`:
/// `Rule{ verify_cert=1, verify_time=1, Subject=[ { NID=13, Criteria=x}, ], Issuer=[ ], Root=[ ] }`.
/// Criteria bytes are shown lossily.
impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Rule{{ verify_cert={}, verify_time={}",
            u8::from(self.verify_cert),
            u8::from(self.verify_time)
        )?;
        for (label, set) in [
            ("Subject", &self.subject),
            ("Issuer", &self.issuer),
            ("Root", &self.root),
        ] {
            write!(f, ", {label}=[")?;
            for c in set {
                write!(
                    f,
                    " {{ NID={}, Criteria={}}},",
                    c.field.nid(),
                    String::from_utf8_lossy(&c.value)
                )?;
            }
            f.write_str(" ]")?;
        }
        f.write_str(" }")
    }
}

/// Why a `verify_peers` string did not parse. FDB reports every one of these as the same
/// `tls_error`, after tracing the whole string as `FDBLibTLSVerifyPeersParseError`; the
/// kinds here follow its `throw` sites.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseErrorKind {
    /// A criterion has no `=` after it.
    #[error("criterion has no '='")]
    MissingEquals,
    /// `Check.X=` is the end of the rule, with no value after the `=`.
    #[error("Check setting has no value")]
    CheckValueMissing,
    /// A `Check.X=` value is longer than one character.
    #[error("Check setting value must be a single character followed by ',' or the end")]
    CheckValueTrailing,
    /// `Check.X>=` or `Check.X<=`: FDB's "cannot prefix match Check".
    #[error("cannot prefix or suffix match a Check setting")]
    CheckPrefixMatch,
    /// `Check.` followed by something other than `Valid` or `Unexpired`.
    #[error("unknown Check setting `{0}`")]
    UnknownCheck(String),
    /// A Check value other than `0` or `1`.
    #[error("Check setting value must be 0 or 1")]
    InvalidCheckValue,
    /// A scope prefix other than `S`, `Subject`, `I`, `Issuer`, `R`, `Root`.
    #[error("unknown scope `{0}` (expected S, Subject, I, Issuer, R or Root)")]
    UnknownScope(String),
    /// The value is empty, or starts with `#` (the RFC 4514 "raw" form, unsupported) or a
    /// space.
    #[error("criterion has an empty value (or one starting with '#' or ' ')")]
    EmptyValue,
    /// The field name is not one of `C`, `CN`, `L`, `ST`, `O`, `OU`, `UID`, `DC`,
    /// `subjectAltName`.
    #[error("unknown field `{0}`")]
    UnknownField(String),
    /// The value ends at something other than `,` or the end of the rule: an unescaped
    /// special character (`"` `+` `;` `<` `>` or NUL), a bad or truncated `\` escape, or
    /// trailing unescaped spaces.
    #[error("value ends in an unescaped special character, a bad escape or trailing spaces")]
    InvalidValue,
}

/// A `verify_peers` string that did not parse.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid verify_peers {spec:?}: {kind} (at byte {offset})")]
pub struct ParseError {
    /// What was wrong.
    pub kind: ParseErrorKind,
    /// Which of the strings given to [`VerifyPeers::parse_all`] (0 for
    /// [`VerifyPeers::parse`]).
    pub spec_index: usize,
    /// The whole string that failed, as FDB traces it (`Config=`).
    pub spec: String,
    /// Byte offset into `spec` where the problem was noticed. Approximate for
    /// [`ParseErrorKind::InvalidValue`] (FDB's value scanner reports where the value
    /// stopped, after trimming trailing spaces).
    pub offset: usize,
}

/// The parsed `verify_peers` setting: every rule of every string, in order. A peer is
/// accepted if any rule matches (see [`VerifyPeers::evaluate`]).
///
/// ```
/// # #[cfg(feature = "tls")] {
/// use fdb_native_transport::tls_verify::VerifyPeers;
///
/// let vp = VerifyPeers::parse("Check.Valid=1,S.CN=fdb|I.O=Example\\, Inc.,R.CN=Root CA").unwrap();
/// assert_eq!(vp.rules().len(), 2);
/// assert!(VerifyPeers::parse("S.CN").is_err());
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifyPeers {
    rules: Vec<Rule>,
}

impl Default for VerifyPeers {
    /// FDB's default, `Check.Valid=1`: chain validation only, no field criteria.
    fn default() -> Self {
        VerifyPeers {
            rules: vec![Rule::default()],
        }
    }
}

impl VerifyPeers {
    /// The setting with no rules at all. FDB never constructs this (every string yields at
    /// least one rule), but its evaluator accepts it ("No rule defined"): the peer need only
    /// have a valid chain.
    pub fn empty() -> Self {
        VerifyPeers { rules: Vec::new() }
    }

    /// Parses one verify-peers string (`--tls-verify-peers`, `FDB_TLS_VERIFY_PEERS`, the
    /// `TLS_verify_peers` network option).
    pub fn parse(spec: &str) -> Result<Self, ParseError> {
        Self::parse_all([spec])
    }

    /// Parses several strings (FDB accepts the option repeatedly) and concatenates their
    /// rules, as `TLSPolicy::set_verify_peers` does. Any failure fails the whole setting.
    pub fn parse_all<I>(specs: I) -> Result<Self, ParseError>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let mut rules = Vec::new();
        for (spec_index, spec) in specs.into_iter().enumerate() {
            let spec = spec.as_ref();
            for (base, text) in split_rules(spec) {
                match parse_rule(text) {
                    Ok(rule) => rules.push(rule),
                    Err((kind, at)) => {
                        return Err(ParseError {
                            kind,
                            spec_index,
                            spec: spec.to_owned(),
                            offset: base + at,
                        });
                    }
                }
            }
        }
        Ok(VerifyPeers { rules })
    }

    /// What FDB uses when the application gave no verify-peers string
    /// (`LoadedTLSConfig::getVerifyPeers`): the explicitly configured strings if there are
    /// any, otherwise the `FDB_TLS_VERIFY_PEERS` value (`env`), otherwise `Check.Valid=1`.
    /// A *set but empty* environment variable is one empty rule, which is equivalent to the
    /// default.
    pub fn resolve<S: AsRef<str>>(explicit: &[S], env: Option<&str>) -> Result<Self, ParseError> {
        if !explicit.is_empty() {
            Self::parse_all(explicit)
        } else if let Some(env) = env {
            Self::parse(env)
        } else {
            Ok(Self::default())
        }
    }

    /// The rules, in order.
    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }

    /// Whether any rule says `Check.Valid=0`. FDB then accepts the peer without evaluating
    /// any criteria of any rule (`PeerVerifier::verify`).
    pub fn check_valid_disabled(&self) -> bool {
        self.rules.iter().any(|r| !r.verify_cert)
    }
}

/// Splits a string at every `|` not preceded by a backslash, as `set_verify_peers` does,
/// returning each piece with its byte offset. The pieces keep their escapes: the rule parser
/// unescapes `\|` itself.
///
/// FDB's loop never advances past an escaped `|` and spins forever on one (it only moves
/// `start` when it splits), so any string containing `\|` hangs a real FDB process. The
/// evident intent, followed here, is to search on from the escaped bar. Like FDB, this
/// treats `\\|` (an escaped backslash, then a bar) as an escaped bar, and a string ending in
/// `|` yields a trailing empty rule.
fn split_rules(spec: &str) -> Vec<(usize, &str)> {
    let bytes = spec.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    let mut search = 0;
    while start < bytes.len() {
        let Some(rel) = bytes[search..].iter().position(|&b| b == b'|') else {
            break;
        };
        let split = search + rel;
        if split == start || bytes[split - 1] != b'\\' {
            out.push((start, &spec[start..split]));
            start = split + 1;
            search = start;
        } else {
            search = split + 1;
        }
    }
    out.push((start, &spec[start..]));
    out
}

fn hex_value(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// `de4514`: unescapes a value starting at `start`, stopping at the first unescaped
/// `" + , ; < >` or NUL. Returns the value and the offset where scanning stopped, which for a
/// well-formed value is the `,` after it or the end of the string.
///
/// Only the escaped form of RFC 4514 is supported, not the raw `#hex` form. A leading `#`
/// or space yields an empty value at `start`. Unescaped trailing spaces are dropped from
/// the value *and* from the returned offset, so the caller sees a space where it expects
/// `,` and rejects the value.
fn unescape_value(input: &[u8], start: usize) -> (Vec<u8>, usize) {
    if matches!(input.get(start), Some(b'#' | b' ')) {
        return (Vec::new(), start);
    }
    let mut out = Vec::new();
    let mut space_count = 0;
    let mut p = start;
    let mut end = input.len();
    while p < input.len() {
        match input[p] {
            b'\\' => {
                // A backslash escaping nothing.
                let Some(&next) = input.get(p + 1) else {
                    end = p;
                    break;
                };
                if matches!(
                    next,
                    b' ' | b'"' | b'#' | b'+' | b',' | b';' | b'<' | b'=' | b'>' | b'|' | b'\\'
                ) {
                    out.push(next);
                    p += 2;
                    space_count = 0;
                    continue;
                }
                // Otherwise `\` and two hex digits.
                let pair = input
                    .get(p + 2)
                    .and_then(|&lo| Some((hex_value(next)?, hex_value(lo)?)));
                match pair {
                    Some((hi, lo)) => {
                        out.push(hi * 16 + lo);
                        p += 3;
                        space_count = 0;
                    }
                    None => {
                        end = p;
                        break;
                    }
                }
            }
            b'"' | b'+' | b',' | b';' | b'<' | b'>' | 0 => {
                end = p;
                break;
            }
            c => {
                out.push(c);
                if c == b' ' {
                    space_count += 1;
                } else {
                    space_count = 0;
                }
                p += 1;
            }
        }
    }
    out.truncate(out.len() - space_count);
    (out, end - space_count)
}

/// Parses one rule (the text between unescaped `|`s). On failure returns the kind and the
/// byte offset within `input`.
fn parse_rule(input: &str) -> Result<Rule, (ParseErrorKind, usize)> {
    let bytes = input.as_bytes();
    let len = bytes.len();
    let mut rule = Rule::default();
    let mut s = 0;
    // Slicing at an ASCII delimiter is on a char boundary; `get` keeps the parser total even
    // if some offset is not.
    let text = |from: usize, to: usize| input.get(from..to).unwrap_or("");

    while s < len {
        let Some(rel) = bytes[s..].iter().position(|&b| b == b'=') else {
            return Err((ParseErrorKind::MissingEquals, s));
        };
        let eq = s + rel;

        let mut match_type = MatchType::Exact;
        if eq > s {
            match bytes[eq - 1] {
                b'>' => match_type = MatchType::Prefix,
                b'<' => match_type = MatchType::Suffix,
                _ => {}
            }
        }
        let term_end = eq - usize::from(match_type != MatchType::Exact);
        let term = text(s, term_end);

        if term.starts_with("Check.") {
            if eq + 2 > len {
                return Err((ParseErrorKind::CheckValueMissing, eq));
            }
            if eq + 2 != len && bytes[eq + 2] != b',' {
                return Err((ParseErrorKind::CheckValueTrailing, eq + 2));
            }
            if match_type != MatchType::Exact {
                return Err((ParseErrorKind::CheckPrefixMatch, eq));
            }
            let flag = match term {
                "Check.Valid" => &mut rule.verify_cert,
                "Check.Unexpired" => &mut rule.verify_time,
                _ => return Err((ParseErrorKind::UnknownCheck(term.to_owned()), s)),
            };
            *flag = match bytes[eq + 1] {
                b'0' => false,
                b'1' => true,
                _ => return Err((ParseErrorKind::InvalidCheckValue, eq + 1)),
            };
            s = eq + 3;
        } else {
            let mut scope = Scope::Subject;
            let mut field_name = term;
            if let Some((scope_name, rest)) = term.split_once('.') {
                scope = match scope_name {
                    "S" | "Subject" => Scope::Subject,
                    "I" | "Issuer" => Scope::Issuer,
                    "R" | "Root" => Scope::Root,
                    _ => return Err((ParseErrorKind::UnknownScope(scope_name.to_owned()), s)),
                };
                field_name = rest;
            }

            let (value, remain) = unescape_value(bytes, eq + 1);
            if remain == eq + 1 {
                return Err((ParseErrorKind::EmptyValue, eq + 1));
            }
            let Some(field) = Field::from_short_name(field_name) else {
                return Err((ParseErrorKind::UnknownField(field_name.to_owned()), s));
            };
            rule.set_mut(scope)
                .insert(Criterion::new(field, match_type, value));

            if remain != len && bytes[remain] != b',' {
                return Err((ParseErrorKind::InvalidValue, remain));
            }
            s = remain + 1;
        }
    }
    Ok(rule)
}
