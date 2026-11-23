//! Tests for the verify_peers language and verifier.
//!
//! FDB has no unit tests for its verify_peers code on either branch (`release-7.4`,
//! `release-8.0`). What exists is ported here: the three worked examples in
//! `documentation/sphinx/source/tls.rst` (identical on both branches), the default
//! (`Check.Valid=1`, which `tests/TestRunner` passes), and the strings the network smoke
//! test and the mTLS benchmark pass (`Root.CN=networktest-smoke`, `Root.CN=dummy-ca`). The
//! rest exercises each branch of `TLSConfig.actor.cpp`'s parser and evaluator, and the
//! verifier on generated certificate chains.

use super::cert::{AltName, AsnString, NameEntry, PeerCert};
use super::*;

mod support;

mod parse {
    use super::*;

    fn rule(spec: &str) -> Rule {
        let vp = VerifyPeers::parse(spec).unwrap_or_else(|e| panic!("{spec:?}: {e}"));
        assert_eq!(vp.rules().len(), 1, "{spec:?}");
        vp.rules()[0].clone()
    }

    fn crit(rule: &Rule, scope: Scope) -> Vec<(Field, MatchType, String)> {
        rule.criteria(scope)
            .map(|c| {
                (
                    c.field(),
                    c.match_type(),
                    String::from_utf8_lossy(c.value()).into_owned(),
                )
            })
            .collect()
    }

    fn kind(spec: &str) -> ParseErrorKind {
        VerifyPeers::parse(spec)
            .expect_err(&format!("{spec:?} should not parse"))
            .kind
    }

    #[test]
    fn default_rules() {
        // `LoadedTLSConfig::getVerifyPeers`: the default is Check.Valid=1 (the explicit,
        // environment and default sources are resolved by `tls::LoadedTlsConfig`).
        let default = VerifyPeers::default();
        assert_eq!(default, VerifyPeers::parse("Check.Valid=1").unwrap());
        assert_eq!(default.rules().len(), 1);
        assert!(default.rules()[0].check_valid());
        assert!(default.rules()[0].check_unexpired());
        assert!(!default.check_valid_disabled());

        assert!(VerifyPeers::empty().rules().is_empty());
    }

    #[test]
    fn check_settings() {
        let r = rule("Check.Valid=0");
        assert!(!r.check_valid());
        assert!(r.check_unexpired());
        let r = rule("Check.Unexpired=0");
        assert!(r.check_valid());
        assert!(!r.check_unexpired());
        let r = rule("Check.Unexpired=0,Check.Valid=0,");
        assert!(!r.check_valid() && !r.check_unexpired());
        // The last setting wins.
        assert!(!rule("Check.Valid=1,Check.Valid=0").check_valid());
        assert!(rule("Check.Valid=0,Check.Valid=1").check_valid());
        assert!(
            VerifyPeers::parse("Check.Valid=0")
                .unwrap()
                .check_valid_disabled()
        );
        assert!(
            VerifyPeers::parse("S.CN=a|Check.Valid=0")
                .unwrap()
                .check_valid_disabled()
        );
    }

    #[test]
    fn check_errors() {
        assert_eq!(kind("Check.Valid="), ParseErrorKind::CheckValueMissing);
        assert_eq!(kind("Check.Valid=10"), ParseErrorKind::CheckValueTrailing);
        assert_eq!(kind("Check.Valid=1x"), ParseErrorKind::CheckValueTrailing);
        assert_eq!(kind("Check.Valid=2"), ParseErrorKind::InvalidCheckValue);
        assert_eq!(kind("Check.Valid=x"), ParseErrorKind::InvalidCheckValue);
        assert_eq!(kind("Check.Valid>=1"), ParseErrorKind::CheckPrefixMatch);
        assert_eq!(kind("Check.Valid<=1"), ParseErrorKind::CheckPrefixMatch);
        assert_eq!(
            kind("Check.Other=1"),
            ParseErrorKind::UnknownCheck("Check.Other".into())
        );
        assert_eq!(
            kind("Check.valid=1"),
            ParseErrorKind::UnknownCheck("Check.valid".into())
        );
        assert_eq!(
            kind("Check.=1"),
            ParseErrorKind::UnknownCheck("Check.".into())
        );
        // Not a Check at all, so it is a field name.
        assert_eq!(
            kind("Check=1"),
            ParseErrorKind::UnknownField("Check".into())
        );
    }

    /// The three worked examples in tls.rst, "Verification Examples".
    #[test]
    fn documentation_examples() {
        let r = rule("Check.Unexpired=0,I.C=US,C=US,S.O=Apple Inc.");
        assert!(r.check_valid());
        assert!(!r.check_unexpired());
        assert_eq!(
            crit(&r, Scope::Issuer),
            [(Field::C, MatchType::Exact, "US".into())]
        );
        assert_eq!(
            crit(&r, Scope::Subject),
            [
                (Field::C, MatchType::Exact, "US".into()),
                (Field::O, MatchType::Exact, "Apple Inc.".into()),
            ]
        );
        assert!(crit(&r, Scope::Root).is_empty());

        let r = rule("S.OU>=FDB,S.OU<=Team,S.subjectAltName=DNS:test.foundationdb.org");
        assert_eq!(
            crit(&r, Scope::Subject),
            [
                (Field::Ou, MatchType::Prefix, "FDB".into()),
                (Field::Ou, MatchType::Suffix, "Team".into()),
                (
                    Field::SubjectAltName,
                    MatchType::Exact,
                    "DNS:test.foundationdb.org".into()
                ),
            ]
        );
        assert_eq!(
            r.criteria(Scope::Subject).last().unwrap().location(),
            Location::Extension
        );
        assert_eq!(
            r.criteria(Scope::Subject).next().unwrap().location(),
            Location::Name
        );

        let r = rule("S.subjectAltName>=DNS:prod.,S.subjectAltName<=DNS:.org");
        // (Iteration is in FDB's set order, which sorts by value.)
        assert_eq!(
            crit(&r, Scope::Subject),
            [
                (Field::SubjectAltName, MatchType::Suffix, "DNS:.org".into()),
                (Field::SubjectAltName, MatchType::Prefix, "DNS:prod.".into()),
            ]
        );
    }

    /// Strings FDB's own scripts pass.
    #[test]
    fn strings_used_by_fdb_scripts() {
        for s in ["Check.Valid=1", "Check.Valid=0"] {
            VerifyPeers::parse(s).unwrap();
        }
        for (s, cn) in [
            ("Root.CN=networktest-smoke", "networktest-smoke"),
            ("Root.CN=dummy-ca", "dummy-ca"),
        ] {
            let r = rule(s);
            assert_eq!(
                crit(&r, Scope::Root),
                [(Field::Cn, MatchType::Exact, cn.into())]
            );
            assert!(r.has_root_criteria());
        }
    }

    #[test]
    fn scopes_and_fields() {
        for (prefix, scope) in [
            ("", Scope::Subject),
            ("S.", Scope::Subject),
            ("Subject.", Scope::Subject),
            ("I.", Scope::Issuer),
            ("Issuer.", Scope::Issuer),
            ("R.", Scope::Root),
            ("Root.", Scope::Root),
        ] {
            let r = rule(&format!("{prefix}CN=x"));
            assert_eq!(crit(&r, scope), [(Field::Cn, MatchType::Exact, "x".into())]);
        }
        for name in [
            "C",
            "CN",
            "L",
            "ST",
            "O",
            "OU",
            "UID",
            "DC",
            "subjectAltName",
        ] {
            let f = Field::from_short_name(name).unwrap();
            assert_eq!(f.short_name(), name);
            assert_eq!(f.to_string(), name);
            let r = rule(&format!("{name}=DNS:x"));
            assert_eq!(r.criteria(Scope::Subject).count(), 1, "{name}");
        }
        assert_eq!(Field::Cn.nid(), 13);
        assert_eq!(Field::SubjectAltName.nid(), 85);
        // `abbrevToNID` takes exactly these names.
        for bad in [
            "cn",
            "Cn",
            "commonName",
            "issuerAltName",
            "emailAddress",
            "SAN",
            "E",
        ] {
            assert_eq!(
                kind(&format!("{bad}=x")),
                ParseErrorKind::UnknownField(bad.into()),
                "{bad}"
            );
        }
        for bad in ["s", "X", "Sub", "subject", "root", ""] {
            assert_eq!(
                kind(&format!("{bad}.CN=x")),
                ParseErrorKind::UnknownScope(bad.into()),
                "{bad:?}"
            );
        }
        // An OID reads as a scope, then a field: both are wrong.
        assert_eq!(kind("2.5.4.3=x"), ParseErrorKind::UnknownScope("2".into()));
        // Everything after the first dot is the field name.
        assert_eq!(kind("S.a.b=x"), ParseErrorKind::UnknownField("a.b".into()));
    }

    #[test]
    fn match_operators() {
        let r = rule("CN=a,OU>=b,O<=c");
        assert_eq!(
            crit(&r, Scope::Subject),
            [
                (Field::Cn, MatchType::Exact, "a".into()),
                (Field::O, MatchType::Suffix, "c".into()),
                (Field::Ou, MatchType::Prefix, "b".into()),
            ]
        );
        // The operator is only the character before the first `=`; the value may contain
        // `=` and `.` but not an unescaped `<` or `>`.
        let r = rule("CN=a=b.c");
        assert_eq!(crit(&r, Scope::Subject)[0].2, "a=b.c".to_string());
        assert_eq!(
            rule("CN=a\\>\\=b")
                .criteria(Scope::Subject)
                .next()
                .unwrap()
                .value(),
            b"a>=b"
        );
        assert_eq!(kind("CN=a>=b"), ParseErrorKind::InvalidValue);
    }

    #[test]
    fn repeated_criteria() {
        let r = rule("CN=a,CN=a,S.CN=a");
        assert_eq!(r.criteria(Scope::Subject).count(), 1);
        // Different values on one field are both kept: all must match.
        let r = rule("CN=a,CN=b,CN>=a");
        assert_eq!(r.criteria(Scope::Subject).count(), 3);
    }

    #[test]
    fn rule_splitting() {
        let n = |s: &str| VerifyPeers::parse(s).unwrap().rules().len();
        assert_eq!(n(""), 1);
        assert_eq!(n("CN=a"), 1);
        assert_eq!(n("CN=a|CN=b"), 2);
        assert_eq!(n("CN=a|"), 2);
        assert_eq!(n("|CN=a"), 2);
        assert_eq!(n("|"), 2);
        assert_eq!(n("||"), 3);
        assert_eq!(n("CN=a||CN=b"), 3);
        assert_eq!(n("Check.Valid=1|Check.Valid=0|S.CN=x"), 3);

        // Several strings are concatenated.
        let both = VerifyPeers::parse_all(["CN=a|CN=b", "CN=c"]).unwrap();
        assert_eq!(both.rules().len(), 3);
        let joined = VerifyPeers::parse("CN=a|CN=b|CN=c").unwrap();
        assert_eq!(both, joined);

        // An escaped bar does not split, and is part of the value. (FDB hangs here.)
        let r = rule("CN=a\\|b");
        assert_eq!(crit(&r, Scope::Subject)[0].2, "a|b");
        let vp = VerifyPeers::parse("CN=a\\|b|CN=c\\|d|CN=e").unwrap();
        assert_eq!(vp.rules().len(), 3);
        // `\\|` is "escaped backslash, bar" but FDB looks only at the preceding character.
        let vp = VerifyPeers::parse("CN=a\\\\|CN=b").unwrap();
        assert_eq!(vp.rules().len(), 1);
        assert_eq!(
            vp.rules()[0]
                .criteria(Scope::Subject)
                .next()
                .unwrap()
                .value(),
            b"a\\|CN=b"
        );
        // A value cannot contain an unescaped bar: that starts the next rule.
        assert_eq!(kind("CN=a|b"), ParseErrorKind::MissingEquals);
    }

    #[test]
    fn value_escapes() {
        let v = |spec: &str| -> Vec<u8> {
            rule(spec)
                .criteria(Scope::Subject)
                .next()
                .unwrap()
                .value()
                .to_vec()
        };
        // Spaces inside are kept; `.` and `=` and `#` after the first character are literal.
        assert_eq!(v("O=Apple Inc."), b"Apple Inc.");
        assert_eq!(v("O=a  b"), b"a  b");
        assert_eq!(v("O=a#b"), b"a#b");
        assert_eq!(v("O=a=b"), b"a=b");
        // The escaped specials.
        for c in [' ', '"', '#', '+', ',', ';', '<', '=', '>', '|', '\\'] {
            assert_eq!(
                v(&format!("O=x\\{c}y")),
                format!("x{c}y").into_bytes(),
                "{c:?}"
            );
        }
        // Escaped leading space and hash.
        assert_eq!(v("O=\\ x"), b" x");
        assert_eq!(v("O=\\#x"), b"#x");
        // Hex escapes, either case, any byte.
        assert_eq!(v("O=\\41\\4a\\4B"), b"AJK");
        assert_eq!(v("O=\\00"), [0u8]);
        assert_eq!(v("O=\\C3\\A9"), "é".as_bytes());
        assert_eq!(v("O=\\ff"), [0xFFu8]);
        // An escaped trailing space is kept; unescaped ones are an error.
        assert_eq!(v("O=a\\ "), b"a ");
        assert_eq!(v("O=a\\ \\ "), b"a  ");
        // Literal non-ASCII is passed through as its UTF-8 bytes.
        assert_eq!(v("O=é"), "é".as_bytes());
        // A value runs to the next comma, which separates items.
        let r = rule("O=a\\,b,CN=c");
        assert_eq!(crit(&r, Scope::Subject).len(), 2);
        assert_eq!(
            rule("O=a\\,b")
                .criteria(Scope::Subject)
                .next()
                .unwrap()
                .value(),
            b"a,b"
        );
    }

    #[test]
    fn value_errors() {
        use ParseErrorKind::*;
        assert_eq!(kind("CN"), MissingEquals);
        assert_eq!(kind("CN=a,OU"), MissingEquals);
        assert_eq!(kind("Check.Valid=1,CN"), MissingEquals);
        assert_eq!(kind("CN="), EmptyValue);
        assert_eq!(kind("CN=,OU=a"), EmptyValue);
        assert_eq!(kind("CN= a"), EmptyValue);
        assert_eq!(kind("CN=#a"), EmptyValue);
        assert_eq!(kind("CN=#"), EmptyValue);
        // Empty value is reported before an unknown field.
        assert_eq!(kind("XX="), EmptyValue);
        // Unescaped specials end the value without a comma: error.
        for c in ['"', '+', ';', '<', '>'] {
            assert_eq!(kind(&format!("CN=a{c}b")), InvalidValue, "{c:?}");
        }
        assert_eq!(kind("CN=a\0b"), InvalidValue);
        // Bad and truncated escapes.
        assert_eq!(kind("CN=a\\"), InvalidValue);
        assert_eq!(kind("CN=a\\4"), InvalidValue);
        assert_eq!(kind("CN=a\\zz"), InvalidValue);
        assert_eq!(kind("CN=a\\4z"), InvalidValue);
        assert_eq!(kind("CN=\\"), EmptyValue);
        // Trailing unescaped spaces.
        assert_eq!(kind("CN=a "), InvalidValue);
        assert_eq!(kind("CN=a  ,OU=b"), InvalidValue);
        // Items need a comma between them, and cannot be empty.
        assert_eq!(kind("CN=a,,OU=b"), UnknownField(",OU".into()));
        assert_eq!(kind(",CN=a"), UnknownField(",CN".into()));
        // Whitespace around the term is part of it.
        assert_eq!(kind(" CN=a"), UnknownField(" CN".into()));
        assert_eq!(kind("CN =a"), UnknownField("CN ".into()));
        assert_eq!(kind("CN=a, OU=b"), UnknownField(" OU".into()));
        // `=` first.
        assert_eq!(kind("=a"), UnknownField("".into()));
        assert_eq!(kind(">=a"), UnknownField("".into()));
        // Errors in a later rule or spec are found, and located.
        let e = VerifyPeers::parse("CN=a|CN=b|bad").unwrap_err();
        assert_eq!(e.kind, MissingEquals);
        assert_eq!(e.offset, 10);
        assert_eq!(e.spec, "CN=a|CN=b|bad");
        assert!(e.to_string().contains("no '='"), "{e}");
        let e = VerifyPeers::parse_all(["CN=a", "CN=b", "XX=c"]).unwrap_err();
        assert_eq!(e.spec_index, 2);
        assert_eq!(e.spec, "XX=c");
    }

    #[test]
    fn rule_display_matches_fdb_to_string() {
        let r = rule("Check.Unexpired=0,CN=a,I.O=b,R.OU>=c");
        assert_eq!(
            r.to_string(),
            "Rule{ verify_cert=1, verify_time=0, Subject=[ { NID=13, Criteria=a}, ], \
             Issuer=[ { NID=17, Criteria=b}, ], Root=[ { NID=18, Criteria=c}, ] }"
        );
        assert_eq!(
            Rule::default().to_string(),
            "Rule{ verify_cert=1, verify_time=1, Subject=[ ], Issuer=[ ], Root=[ ] }"
        );
    }
}

mod eval {
    use super::support::*;
    use super::*;

    fn entry(field: Field, tag: u32, bytes: &[u8]) -> NameEntry {
        NameEntry {
            field,
            value: AsnString {
                tag,
                bytes: bytes.to_vec(),
            },
        }
    }

    fn utf8(field: Field, s: &str) -> NameEntry {
        entry(field, 12, s.as_bytes())
    }

    fn ia5(s: &str) -> AsnString {
        AsnString {
            tag: 22,
            bytes: s.as_bytes().to_vec(),
        }
    }

    fn run(spec: &str, leaf: &PeerCert, root: Option<&PeerCert>) -> Verdict {
        VerifyPeers::parse(spec).unwrap().evaluate(leaf, root)
    }

    fn accepts(spec: &str, leaf: &PeerCert, root: Option<&PeerCert>) -> bool {
        run(spec, leaf, root).is_accepted()
    }

    fn reason(spec: &str, leaf: &PeerCert, root: Option<&PeerCert>) -> String {
        match run(spec, leaf, root) {
            Verdict::Rejected(f) => {
                assert_eq!(f.len(), VerifyPeers::parse(spec).unwrap().rules().len());
                f.iter()
                    .map(|f| f.reason.clone())
                    .collect::<Vec<_>>()
                    .join(" | ")
            }
            v => panic!("{spec:?} should be rejected, got {v:?}"),
        }
    }

    /// The certificate of tls.rst's examples: subject = issuer = `C=US, ST=California,
    /// L=Cupertino, O=Apple Inc., OU=FDB Team`, with two DNS alternative names.
    fn doc_cert() -> PeerCert {
        let key = new_key();
        let params = params(
            &[
                ("C", "US"),
                ("ST", "California"),
                ("L", "Cupertino"),
                ("O", "Apple Inc."),
                ("OU", "FDB Team"),
            ],
            &["test.foundationdb.org", "prod.foundationdb.com"],
        );
        let cert = params.self_signed(&key).unwrap();
        PeerCert::from_der(cert.der()).unwrap()
    }

    #[test]
    fn documentation_examples_pass() {
        let cert = doc_cert();
        for spec in [
            "Check.Unexpired=0,I.C=US,C=US,S.O=Apple Inc.",
            "S.OU>=FDB,S.OU<=Team,S.subjectAltName=DNS:test.foundationdb.org",
            "S.subjectAltName>=DNS:prod.,S.subjectAltName<=DNS:.org",
            "S.subjectAltName>=DNS:prod.,S.subjectAltName<=DNS:.com",
        ] {
            assert!(accepts(spec, &cert, Some(&cert)), "{spec}");
        }
    }

    #[test]
    fn documentation_examples_that_must_fail() {
        let cert = doc_cert();
        for spec in [
            "S.O=Apple",
            "S.O=apple inc.",
            "S.OU>=Team",
            "S.OU<=FDB",
            "I.C=UK",
            "S.subjectAltName=DNS:other.foundationdb.org",
            "S.subjectAltName=test.foundationdb.org",
            "S.subjectAltName>=DNS:test.foundationdb.org.extra",
            "S.subjectAltName<=DNS:prod.foundationdb.com.",
            "S.subjectAltName=URI:test.foundationdb.org",
            "S.CN=anything",
        ] {
            assert!(!accepts(spec, &cert, Some(&cert)), "{spec}");
        }
    }

    #[test]
    fn name_criteria_basics() {
        let leaf = PeerCert::from_parts(
            vec![
                utf8(Field::Cn, "node1.example.com"),
                utf8(Field::O, "Example"),
            ],
            vec![utf8(Field::Cn, "Example Intermediate")],
            None,
        );
        assert!(accepts("CN=node1.example.com", &leaf, None));
        assert!(accepts("CN>=node1", &leaf, None));
        assert!(accepts("CN<=.com", &leaf, None));
        assert!(accepts("CN=node1.example.com,O=Example", &leaf, None));
        assert!(accepts("I.CN=Example Intermediate", &leaf, None));
        assert!(accepts("S.CN>=node,I.CN<=Intermediate", &leaf, None));
        // Case sensitive; exact means exact.
        assert!(!accepts("CN=NODE1.example.com", &leaf, None));
        assert!(!accepts("CN=node1.example", &leaf, None));
        assert!(!accepts("CN>=ode1", &leaf, None));
        assert!(!accepts("CN<=.co", &leaf, None));
        // A prefix longer than the value does not match.
        assert!(!accepts("CN>=node1.example.com.au", &leaf, None));
        assert!(!accepts("CN<=x.node1.example.com", &leaf, None));
        // Missing fields.
        assert!(!accepts("OU=x", &leaf, None));
        assert!(!accepts("I.O=Example", &leaf, None));
        // The issuer and subject are distinct.
        assert!(!accepts("S.CN=Example Intermediate", &leaf, None));
        // All criteria must hold.
        assert!(!accepts("CN=node1.example.com,O=Other", &leaf, None));
        assert!(!accepts("CN=node1.example.com,CN=other", &leaf, None));
        assert!(accepts("CN>=node1,CN<=.com", &leaf, None));
        // OR across rules, in order.
        assert!(accepts("CN=no|CN=node1.example.com", &leaf, None));
        assert!(accepts("CN=node1.example.com|CN=no", &leaf, None));
        assert!(!accepts("CN=no|O=no", &leaf, None));
        // An empty rule matches anything.
        assert!(accepts("CN=no|", &leaf, None));
        assert!(accepts("", &leaf, None));
        assert!(accepts("Check.Valid=1", &leaf, None));
        assert!(accepts("Check.Unexpired=0", &leaf, None));
    }

    #[test]
    fn multiple_entries_never_match() {
        // Two OUs (or the usual two DCs): FDB requires exactly one entry of the field.
        let leaf = PeerCert::from_parts(
            vec![
                utf8(Field::Cn, "n"),
                utf8(Field::Ou, "one"),
                utf8(Field::Ou, "two"),
                entry(Field::Dc, 22, b"example"),
                entry(Field::Dc, 22, b"com"),
            ],
            vec![],
            None,
        );
        assert!(accepts("CN=n", &leaf, None));
        assert!(!accepts("OU=one", &leaf, None));
        assert!(!accepts("OU=two", &leaf, None));
        assert!(!accepts("DC=com", &leaf, None));
        assert!(!accepts("OU>=o", &leaf, None));
        assert!(reason("OU=one", &leaf, None).starts_with("Rule.Cert.Subject.Name.MultipleNames"));
    }

    #[test]
    fn string_types() {
        let leaf = PeerCert::from_parts(
            vec![
                entry(Field::Cn, 19, b"printable"),
                entry(Field::O, 12, "Caf\u{e9}".as_bytes()),
                entry(Field::Ou, 20, &[b'C', b'a', b'f', 0xE9]),
                entry(Field::L, 30, &[0x00, b'H', 0x00, 0xE9]),
                entry(Field::St, 28, &[0, 0, 0, b'S', 0, 0, 0, 0xE9]),
                entry(Field::C, 4, &[1, 2]),
                entry(Field::Uid, 22, b"u1"),
            ],
            vec![],
            None,
        );
        assert!(accepts("CN=printable", &leaf, None));
        assert!(!accepts("O=Caf\\C3\\A9", &leaf, None));
        // Criteria are Latin-1: an e-acute is the single byte E9, which becomes C3 A9 once
        // converted, the same as a UTF8String's.
        assert!(accepts("O=Caf\\E9", &leaf, None));
        assert!(accepts("OU=Caf\\E9", &leaf, None));
        assert!(accepts("L=H\\E9", &leaf, None));
        assert!(accepts("ST=S\\E9", &leaf, None));
        // Literal UTF-8 in the rule is two Latin-1 characters.
        assert!(!accepts("O=Café", &leaf, None));
        // Not a string type.
        assert!(!accepts("C=\\01\\02", &leaf, None));
        assert!(accepts("UID=u1", &leaf, None));
    }

    #[test]
    fn subject_alt_name() {
        let sans = |names: Vec<AltName>| PeerCert::from_parts(vec![], vec![], Some(names));
        let dns = |s: &str| AltName::Dns(ia5(s));
        let leaf = sans(vec![
            dns("a.example.com"),
            AltName::Email(ia5("ops@example.com")),
            AltName::Uri(ia5("spiffe://example/fdb")),
            AltName::Ip(vec![10, 0, 0, 1]),
            dns("b.example.com"),
        ]);
        for spec in [
            "subjectAltName=DNS:a.example.com",
            "subjectAltName=DNS:b.example.com",
            "subjectAltName=EMAIL:ops@example.com",
            "subjectAltName=URI:spiffe://example/fdb",
            "subjectAltName>=DNS:b.",
            "subjectAltName<=DNS:.com",
            "subjectAltName>=URI:spiffe://",
            "subjectAltName<=EMAIL:@example.com",
            // The scope does not change what is read.
            "I.subjectAltName=DNS:a.example.com",
            // Multiple criteria on the extension: each is satisfied by some entry.
            "subjectAltName=DNS:a.example.com,subjectAltName=DNS:b.example.com",
            "subjectAltName=DNS:a.example.com,subjectAltName=EMAIL:ops@example.com",
        ] {
            assert!(accepts(spec, &leaf, None), "{spec}");
        }
        for spec in [
            "subjectAltName=DNS:c.example.com",
            "subjectAltName=a.example.com",
            "subjectAltName=dns:a.example.com",
            "subjectAltName=EMAIL:a.example.com",
            "subjectAltName=URI:a.example.com",
            "subjectAltName=DNS:",
            "subjectAltName>=DNS:",
            "subjectAltName=DNS",
            // An IP address is an octet string, which never converts: never matches.
            "subjectAltName=IP:10.0.0.1",
            "subjectAltName>=IP:10",
            "subjectAltName=IP:\\0a\\00\\00\\01",
            "subjectAltName=IP:",
        ] {
            assert!(!accepts(spec, &leaf, None), "{spec}");
        }
        assert!(
            reason("subjectAltName=DNS:zzz", &leaf, None)
                .starts_with("Rule.Cert.Subject.Extension.NoMatchingEntry[Extension=DNS:zzz]")
        );

        // No extension at all.
        let none = PeerCert::from_parts(vec![], vec![], None);
        assert!(!accepts("subjectAltName=DNS:a.example.com", &none, None));
        assert_eq!(
            reason("subjectAltName=DNS:a", &none, None),
            "Rule.Cert.Subject.Extension.EmptySans[Extension=DNS:a]"
        );
        // An empty extension.
        let empty = sans(vec![]);
        assert!(reason("subjectAltName=DNS:a", &empty, None).contains("NoMatchingEntry"));

        // A name of an unsupported type fails the criterion at once: the first match wins if
        // it comes first, but a later match is never reached.
        let before = sans(vec![
            dns("a.example.com"),
            AltName::Unsupported("GEN_OTHERNAME"),
        ]);
        let after = sans(vec![
            AltName::Unsupported("GEN_OTHERNAME"),
            dns("a.example.com"),
        ]);
        assert!(accepts("subjectAltName=DNS:a.example.com", &before, None));
        assert!(!accepts("subjectAltName=DNS:a.example.com", &after, None));
        assert!(
            reason("subjectAltName=DNS:a.example.com", &after, None)
                .starts_with("Rule.Cert.Subject.Extension.GEN_OTHERNAME")
        );
        // Other types are skipped over.
        let other = sans(vec![AltName::Other, dns("a.example.com")]);
        assert!(accepts("subjectAltName=DNS:a.example.com", &other, None));
        for kind in ["GEN_X400", "GEN_DIRNAME", "GEN_EDIPARTY", "GEN_RID"] {
            let c = sans(vec![AltName::Unsupported(kind), dns("a.example.com")]);
            assert!(
                reason("subjectAltName=DNS:a.example.com", &c, None).contains(kind),
                "{kind}"
            );
        }
    }

    #[test]
    fn root_criteria() {
        let leaf = PeerCert::from_parts(
            vec![utf8(Field::Cn, "leaf")],
            vec![utf8(Field::Cn, "Intermediate CA")],
            None,
        );
        let root = PeerCert::from_parts(
            vec![utf8(Field::Cn, "Root CA"), utf8(Field::O, "Example")],
            vec![utf8(Field::Cn, "Root CA")],
            Some(vec![AltName::Dns(ia5("root.example.com"))]),
        );
        assert!(accepts("R.CN=Root CA", &leaf, Some(&root)));
        assert!(accepts(
            "Root.CN=Root CA,Root.O=Example",
            &leaf,
            Some(&root)
        ));
        assert!(accepts(
            "R.CN>=Root,R.O<=ple,CN=leaf,I.CN=Intermediate CA",
            &leaf,
            Some(&root)
        ));
        assert!(accepts(
            "R.subjectAltName=DNS:root.example.com",
            &leaf,
            Some(&root)
        ));
        assert!(!accepts("R.CN=leaf", &leaf, Some(&root)));
        assert!(!accepts("R.CN=Intermediate CA", &leaf, Some(&root)));
        assert!(!accepts("R.OU=x", &leaf, Some(&root)));
        assert!(!accepts("CN=Root CA", &leaf, Some(&root)));
        assert_eq!(
            reason("R.CN=other", &leaf, Some(&root)),
            "Rule.RootCert.Subject.Name.NameMismatch[Name=CN = Root CA, O = Example]"
        );
        // Without a root, root criteria cannot hold; a rule without them is unaffected.
        assert!(!accepts("R.CN=Root CA", &leaf, None));
        assert!(accepts("CN=leaf", &leaf, None));
        assert!(reason("R.CN=Root CA", &leaf, None).contains("RootCert.Subject.Missing"));

        assert!(VerifyPeers::parse("R.CN=x").unwrap().needs_root());
        assert!(VerifyPeers::parse("CN=a|R.CN=x").unwrap().needs_root());
        assert!(!VerifyPeers::parse("CN=a").unwrap().needs_root());
        assert!(
            !VerifyPeers::parse("R.CN=x,Check.Valid=0")
                .unwrap()
                .needs_root()
        );
        assert!(!VerifyPeers::default().needs_root());
    }

    #[test]
    fn check_valid_zero_short_circuits_every_rule() {
        let leaf = PeerCert::from_parts(vec![utf8(Field::Cn, "leaf")], vec![], None);
        assert_eq!(
            run("Check.Valid=0", &leaf, None),
            Verdict::Accepted(AcceptReason::CheckValidDisabled)
        );
        // Any rule with Check.Valid=0 accepts, whatever the others say, and whichever order.
        assert_eq!(
            run("CN=nope|Check.Valid=0", &leaf, None),
            Verdict::Accepted(AcceptReason::CheckValidDisabled)
        );
        assert_eq!(
            run("Check.Valid=0,CN=nope", &leaf, None),
            Verdict::Accepted(AcceptReason::CheckValidDisabled)
        );
        // But Check.Valid=1 does nothing special, and Check.Unexpired=0 never short-circuits.
        assert!(!accepts("CN=nope,Check.Valid=1", &leaf, None));
        assert!(!accepts("CN=nope,Check.Unexpired=0", &leaf, None));
    }

    #[test]
    fn verdict_reasons_and_indexes() {
        let leaf = PeerCert::from_parts(
            vec![utf8(Field::Cn, "leaf")],
            vec![utf8(Field::Cn, "ca")],
            None,
        );
        assert_eq!(
            run("CN=a|I.CN=ca|CN=leaf", &leaf, None),
            Verdict::Accepted(AcceptReason::RuleMatched(1))
        );
        assert_eq!(
            AcceptReason::RuleMatched(1).to_string(),
            "Rule 1 matched successfully"
        );
        assert_eq!(
            run("", &leaf, None),
            Verdict::Accepted(AcceptReason::RuleMatched(0))
        );
        assert_eq!(
            VerifyPeers::empty().evaluate(&leaf, None),
            Verdict::Accepted(AcceptReason::NoRules)
        );
        assert_eq!(AcceptReason::NoRules.to_string(), "No rule defined");

        let Verdict::Rejected(failures) = run("CN=x|I.CN=y|OU=z", &leaf, None) else {
            panic!()
        };
        assert_eq!(
            failures,
            vec![
                RuleFailure {
                    rule: 0,
                    reason: "Rule.Cert.Subject.Name.NameMismatch[Name=CN = leaf]".into()
                },
                RuleFailure {
                    rule: 1,
                    reason: "Rule.Cert.Issuer.Name.NameMismatch[Name=CN = ca]".into()
                },
                RuleFailure {
                    rule: 2,
                    reason: "Rule.Cert.Subject.Name.Missing".into()
                },
            ]
        );
        assert_eq!(
            failures[0].to_string(),
            "rule 0: Rule.Cert.Subject.Name.NameMismatch[Name=CN = leaf]"
        );
    }

    #[test]
    fn generated_chain_subject_issuer_root() {
        let pki = Pki::new();
        let leaf = PeerCert::from_der(pki.leaf.der()).unwrap();
        let root = PeerCert::from_der(pki.root.der()).unwrap();
        assert_eq!(leaf.subject().len(), 3);
        for spec in [
            "S.CN=node-1",
            "CN=node-1,O=Leaf Org,OU=Databases",
            "I.CN=Test Intermediate",
            "I.O=CA Org",
            "R.CN=Test Root",
            "R.CN=Test Root,I.CN=Test Intermediate,S.CN=node-1",
            "S.subjectAltName=DNS:node-1.example.com",
            "S.subjectAltName=URI:spiffe://example.com/node-1",
            "S.subjectAltName=EMAIL:ops@example.com",
            "S.subjectAltName>=DNS:node-,subjectAltName<=DNS:.example.com",
        ] {
            assert!(accepts(spec, &leaf, Some(&root)), "{spec}");
        }
        for spec in [
            "R.CN=Test Intermediate",
            "I.CN=Test Root",
            "S.CN=Test Root",
            "S.subjectAltName=IP:192.0.2.1",
            "S.subjectAltName=DNS:node-2.example.com",
        ] {
            assert!(!accepts(spec, &leaf, Some(&root)), "{spec}");
        }
    }

    #[test]
    fn generated_names_with_odd_string_types() {
        use rcgen::{DnType, DnValue};
        let key = new_key();
        let mut p = params(&[("CN", "typed")], &[]);
        p.distinguished_name.push(
            DnType::OrganizationName,
            DnValue::PrintableString("Printable Org".try_into().unwrap()),
        );
        p.distinguished_name.push(
            DnType::OrganizationalUnitName,
            DnValue::TeletexString("Teletex OU".try_into().unwrap()),
        );
        p.distinguished_name.push(
            DnType::LocalityName,
            DnValue::BmpString("Bmp Town".try_into().unwrap()),
        );
        p.distinguished_name.push(
            DnType::StateOrProvinceName,
            DnValue::UniversalString("Univ State".try_into().unwrap()),
        );
        p.distinguished_name.push(
            DnType::CustomDnType(vec![0, 9, 2342, 19200300, 100, 1, 25]),
            DnValue::Ia5String("example".try_into().unwrap()),
        );
        p.distinguished_name.push(
            DnType::CustomDnType(vec![0, 9, 2342, 19200300, 100, 1, 1]),
            DnValue::Utf8String("user-7".into()),
        );
        let cert = PeerCert::from_der(p.self_signed(&key).unwrap().der()).unwrap();
        for spec in [
            "CN=typed",
            "O=Printable Org",
            "OU=Teletex OU",
            "L=Bmp Town",
            "ST=Univ State",
            "DC=example",
            "UID=user-7",
        ] {
            assert!(accepts(spec, &cert, Some(&cert)), "{spec}");
        }
    }

    #[test]
    fn generated_alt_name_shapes() {
        use rcgen::{OtherNameValue, SanType};
        let key = new_key();
        let mut p = params(&[("CN", "sans")], &[]);
        p.subject_alt_names = vec![
            SanType::DnsName("a.example.com".try_into().unwrap()),
            SanType::IpAddress("10.1.2.3".parse().unwrap()),
            SanType::Rfc822Name("me@example.com".try_into().unwrap()),
            SanType::URI("https://example.com/x".try_into().unwrap()),
            SanType::OtherName((
                vec![1, 3, 6, 1, 4, 1, 311, 20, 2, 3],
                OtherNameValue::Utf8String("upn@example.com".into()),
            )),
            SanType::DnsName("late.example.com".try_into().unwrap()),
        ];
        let cert = PeerCert::from_der(p.self_signed(&key).unwrap().der()).unwrap();
        let names = cert.alt_names().unwrap();
        assert_eq!(names.len(), 6);
        assert!(matches!(names[4], AltName::Unsupported("GEN_OTHERNAME")));
        // Entries before the otherName match; the one after it is unreachable.
        assert!(accepts("subjectAltName=DNS:a.example.com", &cert, None));
        assert!(accepts("subjectAltName=EMAIL:me@example.com", &cert, None));
        assert!(accepts(
            "subjectAltName=URI:https://example.com/x",
            &cert,
            None
        ));
        assert!(!accepts("subjectAltName=DNS:late.example.com", &cert, None));
        assert!(!accepts("subjectAltName=IP:10.1.2.3", &cert, None));
    }

    /// Replaces every occurrence of `from` in `der` with `to` (same length); a self-signed
    /// certificate repeats its subject as the issuer.
    fn patch(der: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
        assert_eq!(from.len(), to.len());
        let mut out = der.to_vec();
        let mut found = false;
        for at in 0..der.len().saturating_sub(from.len() - 1) {
            if &der[at..at + from.len()] == from {
                out[at..at + to.len()].copy_from_slice(to);
                found = true;
            }
        }
        assert!(found, "pattern in certificate");
        out
    }

    /// OpenSSL fails to decode the whole subjectAltName extension if one name is malformed,
    /// so every `subjectAltName` criterion then fails, even one that names a good entry.
    #[test]
    fn an_undecodable_alt_name_voids_the_extension() {
        let key = new_key();
        let p = params(&[("CN", "sans")], &["good.example.com", "bad.example.com"]);
        let der = p.self_signed(&key).unwrap().der().to_vec();
        let good = PeerCert::from_der(&der).unwrap();
        assert_eq!(good.alt_names().unwrap().len(), 2);
        assert!(accepts("subjectAltName=DNS:good.example.com", &good, None));

        // A dNSName that is not valid text.
        let bad =
            PeerCert::from_der(&patch(&der, b"bad.example.com", b"\xffad.example.com")).unwrap();
        assert!(bad.alt_names().is_none());
        assert!(!accepts("subjectAltName=DNS:good.example.com", &bad, None));
        assert!(reason("subjectAltName=DNS:good.example.com", &bad, None).contains("EmptySans"));
    }

    /// A name attribute value must be a universal, primitive string to be comparable.
    #[test]
    fn attribute_values_must_be_universal_primitive_strings() {
        let key = new_key();
        let der = params(&[("CN", "tagged-value")], &[])
            .self_signed(&key)
            .unwrap()
            .der()
            .to_vec();
        let ok = PeerCert::from_der(&der).unwrap();
        assert!(accepts("CN=tagged-value", &ok, None));
        // The UTF8String tag (0x0c) sits just before the length and the text; make the
        // value context-specific (0x8c) and then constructed universal (0x2c).
        let mut pattern = vec![0x0c, "tagged-value".len() as u8];
        pattern.extend_from_slice(b"tagged-value");
        for tag in [0x8c, 0xcc, 0x4c] {
            let mut replacement = pattern.clone();
            replacement[0] = tag;
            let cert = PeerCert::from_der(&patch(&der, &pattern, &replacement)).unwrap();
            assert_eq!(cert.subject().len(), 1);
            assert!(!accepts("CN=tagged-value", &cert, None), "tag {tag:#x}");
        }
        // Constructed universal.
        let mut replacement = pattern.clone();
        replacement[0] = 0x2c;
        if let Ok(cert) = PeerCert::from_der(&patch(&der, &pattern, &replacement)) {
            assert!(!accepts("CN=tagged-value", &cert, None));
        }
    }

    #[test]
    fn certificate_without_alt_names_or_with_bad_der() {
        let key = new_key();
        let p = params(&[("CN", "plain")], &[]);
        let cert = PeerCert::from_der(p.self_signed(&key).unwrap().der()).unwrap();
        assert!(cert.alt_names().is_none());
        assert!(cert.is_self_signed());
        assert!(cert.not_before() < cert.not_after());
        assert!(PeerCert::from_der(b"not a certificate").is_err());
        assert!(PeerCert::from_der(&[]).is_err());
        let mut der = p.self_signed(&key).unwrap().der().to_vec();
        der.push(0);
        assert!(PeerCert::from_der(&der).is_err());
    }
}

mod openssl_profile;
mod verifier;

mod props {
    use proptest::prelude::*;

    use super::*;

    /// A criterion's value with every byte written as a `\XX` escape.
    fn hex_escape(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("\\{b:02X}")).collect()
    }

    /// A real certificate to mutate.
    fn base_cert() -> &'static Vec<u8> {
        static CERT: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
        CERT.get_or_init(|| {
            let pki = super::support::Pki::new();
            pki.leaf.der().to_vec()
        })
    }

    fn arb_asn_string() -> impl Strategy<Value = AsnString> {
        (
            prop::sample::select(vec![0u32, 4, 12, 18, 19, 20, 22, 26, 28, 30, 31, u32::MAX]),
            prop::collection::vec(any::<u8>(), 0..10),
        )
            .prop_map(|(tag, bytes)| AsnString { tag, bytes })
    }

    fn arb_name() -> impl Strategy<Value = Vec<NameEntry>> {
        prop::collection::vec(
            (arb_field(), arb_asn_string()).prop_map(|(field, value)| NameEntry { field, value }),
            0..6,
        )
    }

    fn arb_alt_name() -> impl Strategy<Value = AltName> {
        prop_oneof![
            arb_asn_string().prop_map(AltName::Dns),
            arb_asn_string().prop_map(AltName::Email),
            arb_asn_string().prop_map(AltName::Uri),
            prop::collection::vec(any::<u8>(), 0..17).prop_map(AltName::Ip),
            prop::sample::select(vec!["GEN_OTHERNAME", "GEN_X400", "GEN_RID"])
                .prop_map(AltName::Unsupported),
            Just(AltName::Other),
        ]
    }

    fn arb_peer_cert() -> impl Strategy<Value = PeerCert> {
        (
            arb_name(),
            arb_name(),
            prop::option::of(prop::collection::vec(arb_alt_name(), 0..5)),
        )
            .prop_map(|(subject, issuer, sans)| PeerCert::from_parts(subject, issuer, sans))
    }

    fn arb_field() -> impl Strategy<Value = Field> {
        prop::sample::select(vec![
            Field::Cn,
            Field::C,
            Field::L,
            Field::St,
            Field::O,
            Field::Ou,
            Field::SubjectAltName,
            Field::Dc,
            Field::Uid,
        ])
    }

    fn arb_scope() -> impl Strategy<Value = (Scope, &'static str)> {
        prop::sample::select(vec![
            (Scope::Subject, ""),
            (Scope::Subject, "S."),
            (Scope::Subject, "Subject."),
            (Scope::Issuer, "I."),
            (Scope::Issuer, "Issuer."),
            (Scope::Root, "R."),
            (Scope::Root, "Root."),
        ])
    }

    fn arb_match() -> impl Strategy<Value = (MatchType, &'static str)> {
        prop::sample::select(vec![
            (MatchType::Exact, "="),
            (MatchType::Prefix, ">="),
            (MatchType::Suffix, "<="),
        ])
    }

    /// Fragments that stress the parser: its operators and escapes in random order.
    fn arb_fragmenty() -> impl Strategy<Value = String> {
        prop::collection::vec(
            prop::sample::select(vec![
                "Check.Valid",
                "Check.Unexpired",
                "Check.",
                "S.",
                "I.",
                "R.",
                "Root.",
                "Issuer.",
                "Subject.",
                "CN",
                "O",
                "OU",
                "C",
                "L",
                "ST",
                "DC",
                "UID",
                "subjectAltName",
                "=",
                ">=",
                "<=",
                ">",
                "<",
                ",",
                "|",
                "\\",
                "\\|",
                "\\,",
                "\\41",
                "\\4",
                " ",
                "#",
                "0",
                "1",
                "DNS:",
                "a",
                "é",
                "\u{0}",
                "+",
                ";",
                "\"",
                ".",
            ]),
            0..24,
        )
        .prop_map(|v| v.concat())
    }

    proptest! {
        /// The parser is total: whatever the string, it returns a result.
        #[test]
        fn parse_never_panics(s in any::<String>()) {
            let _ = VerifyPeers::parse(&s);
            let _ = VerifyPeers::parse_all([s.as_str(), s.as_str()]);
        }

        #[test]
        fn parse_never_panics_on_operator_soup(s in arb_fragmenty()) {
            if let Ok(vp) = VerifyPeers::parse(&s) {
                for r in vp.rules() {
                    let _ = r.to_string();
                }
            }
        }

        #[test]
        fn parse_never_panics_on_control_text(s in "[\\x00-\\x7f]{0,64}") {
            let _ = VerifyPeers::parse(&s);
        }

        /// Whatever the result, a failure names a position inside the string.
        #[test]
        fn errors_are_located(s in arb_fragmenty()) {
            if let Err(e) = VerifyPeers::parse(&s) {
                prop_assert!(e.offset <= s.len(), "{e}");
                prop_assert_eq!(e.spec, s);
            }
        }

        /// Hex-escaping every byte of a value makes it parse back to the same bytes, for any
        /// field, scope, operator and value, joined into several rules.
        #[test]
        fn escaped_values_round_trip(
            items in prop::collection::vec(
                (arb_scope(), arb_field(), arb_match(), prop::collection::vec(any::<u8>(), 1..12)),
                1..5,
            ),
            more in 1usize..4,
        ) {
            let rule: String = items
                .iter()
                .map(|((_, sp), f, (_, op), v)| format!("{sp}{f}{op}{}", hex_escape(v)))
                .collect::<Vec<_>>()
                .join(",");
            let spec = vec![rule.as_str(); more].join("|");
            let vp = VerifyPeers::parse(&spec).unwrap();
            prop_assert_eq!(vp.rules().len(), more);
            for r in vp.rules() {
                for scope in [Scope::Subject, Scope::Issuer, Scope::Root] {
                    let mut expected: Vec<Criterion> = items
                        .iter()
                        .filter(|((s, _), ..)| *s == scope)
                        .map(|(_, f, (m, _), v)| Criterion::new(*f, *m, v.clone()))
                        .collect();
                    expected.sort();
                    expected.dedup();
                    let got: Vec<Criterion> = r.criteria(scope).cloned().collect();
                    prop_assert_eq!(got, expected);
                }
            }
        }

        /// The splitter and parser agree: joining parseable rules with `|` gives all of them.
        #[test]
        fn joining_rules_adds_rules(a in "[A-Za-z0-9 ]{1,8}", b in "[A-Za-z0-9 ]{1,8}") {
            let a = a.trim().to_owned();
            let b = b.trim().to_owned();
            prop_assume!(!a.is_empty() && !b.is_empty());
            let x = format!("CN={a}");
            let y = format!("O={b}");
            let joined = VerifyPeers::parse(&format!("{x}|{y}")).unwrap();
            let both = VerifyPeers::parse_all([x.as_str(), y.as_str()]).unwrap();
            prop_assert_eq!(&joined, &both);
            prop_assert_eq!(joined.rules().len(), 2);
        }

        /// Evaluation is total over arbitrary parsed rules and certificates.
        #[test]
        fn evaluate_never_panics(s in arb_fragmenty(), leaf in arb_peer_cert(), root in arb_peer_cert()) {
            if let Ok(vp) = VerifyPeers::parse(&s) {
                let _ = vp.evaluate(&leaf, Some(&root));
                let _ = vp.evaluate(&leaf, None);
            }
        }

        /// Whatever the rules, a certificate matches the rule built from its own fields, and
        /// evaluating twice agrees.
        #[test]
        fn a_certificate_matches_its_own_exact_fields(leaf in arb_peer_cert()) {
            let mut items = Vec::new();
            for field in [Field::Cn, Field::O, Field::Ou, Field::L, Field::St, Field::C] {
                let mut entries = leaf.subject().iter().filter(|e| e.field == field);
                if let (Some(e), None) = (entries.next(), entries.next()) {
                    // Criteria are Latin-1 bytes; compare against the UTF-8 conversion when it
                    // is plain ASCII.
                    if let Some(text) = e.value.to_utf8().filter(|t| t.is_ascii()) {
                        items.push(format!("{field}={}", hex_escape(&text)));
                    }
                }
            }
            let spec = items.join(",");
            let vp = VerifyPeers::parse(&spec).unwrap();
            prop_assert!(vp.evaluate(&leaf, None).is_accepted(), "{spec}");
            prop_assert_eq!(vp.evaluate(&leaf, None), vp.evaluate(&leaf, None));
        }

        /// Parsing certificates is total: a certificate with bytes flipped, inserted or
        /// removed is an error or some certificate, never a panic.
        #[test]
        fn from_der_never_panics_on_mutated_certificates(
            flips in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 0..8),
            cut in prop::option::of(any::<prop::sample::Index>()),
            insert in prop::option::of((any::<prop::sample::Index>(), any::<u8>())),
        ) {
            let mut der = base_cert().clone();
            for (i, b) in flips {
                let at = i.index(der.len());
                der[at] = b;
            }
            if let Some((i, b)) = insert {
                der.insert(i.index(der.len() + 1), b);
            }
            if let Some(i) = cut {
                der.truncate(i.index(der.len() + 1));
            }
            if let Ok(cert) = PeerCert::from_der(&der) {
                let vp = VerifyPeers::parse("CN=a,S.subjectAltName>=DNS:a,I.O<=b,R.OU=c").unwrap();
                let _ = vp.evaluate(&cert, Some(&cert));
                let _ = cert.is_self_signed();
            }
        }

        #[test]
        fn from_der_never_panics_on_arbitrary_bytes(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
            let _ = PeerCert::from_der(&bytes);
        }
    }
}
