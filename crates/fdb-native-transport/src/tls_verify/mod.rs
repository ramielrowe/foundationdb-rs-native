//! FDB's `verify_peers` language (`--tls-verify-peers`, `FDB_TLS_VERIFY_PEERS`, the
//! `TLS_verify_peers` network option) and a rustls certificate verifier that applies it.
//!
//! * [`VerifyPeers`] parses the language ([`VerifyPeers::parse`]) and evaluates it
//!   ([`VerifyPeers::evaluate`]) over the fields of a [`PeerCert`], with no TLS stack involved.
//! * [`VerifyPeersVerifier`] is a rustls [`ServerCertVerifier`](rustls::client::danger::ServerCertVerifier)
//!   that validates the peer's chain against the configured CA certificates (ignoring the
//!   server name) and then applies the rules.
//!
//! This is a port of `TLSPolicy` in `flow/TLSConfig.actor.cpp` and
//! `flow/include/flow/TLSConfig.actor.h` on FDB `release-7.4`, which are
//! `flow/TLSConfig.cpp` and `flow/include/flow/TLSConfig.h` on `release-8.0`
//! (line numbers below are 7.4's, with 8.0's after).
//! The verification code is identical on both branches (8.0 only renamed the files, replaced
//! the actor compiler, and changed `output +=` to `push_back`); see
//! [Differences between 7.4 and 8.0](#differences-between-74-and-80) for what is not.
//! FDB has no unit tests of its own for this code, and neither branch has a test that passes a
//! verify-peers string anywhere except cluster harnesses (`Check.Valid=1`, and
//! `Root.CN=networktest-smoke` / `Root.CN=dummy-ca` for the network test and the mTLS
//! benchmark).
//! The user documentation is `documentation/sphinx/source/tls.rst`, "Peer verification"; where
//! it and the code disagree, this port follows the code.
//!
//! # Grammar
//!
//! ```text
//! verify-peers = rule *( "|" rule )          ; OR; `\|` does not split
//! rule         = [ item *( "," item ) [ "," ] ]
//! item         = check / criterion
//! check        = ( "Check.Valid" / "Check.Unexpired" ) "=" ( "0" / "1" )
//! criterion    = [ scope "." ] field [ ">" / "<" ] "=" value
//! scope        = "S" / "Subject" / "I" / "Issuer" / "R" / "Root"     ; default Subject
//! field        = "CN" / "C" / "L" / "ST" / "O" / "OU" / "UID" / "DC" / "subjectAltName"
//! ```
//!
//! (`set_verify_peers`, 537 (8.0: 515); `Rule::Rule`, 562 (8.0: 540); `abbrevToNID`, 512 (8.0: 490).)
//!
//! * Rules are separated by `|`; a rule *matches* if all its items hold, and a peer is accepted
//!   if any rule matches. Several strings (the option repeated) are the same as joining them
//!   with `|`. An empty rule (the empty string, a leading, doubled or trailing `|`) is valid
//!   and matches everything: it has no criteria and the default checks.
//! * `Check.Valid` and `Check.Unexpired` default to 1. The value is exactly one character, so
//!   `Check.Valid=10` is an error, and they cannot take `>` or `<`.
//! * A criterion is `field=value` (exact), `field>=value` (the certificate's value *starts
//!   with* `value`) or `field<=value` (*ends with*). `>=` is "prefix" and `<=` is "suffix".
//! * Fields are the nine short names above, case sensitive. Dotted OIDs, `emailAddress`,
//!   `issuerAltName` and the like are not accepted, whatever the user documentation hints.
//! * The scope selects what the criterion is tested against: the peer's own certificate's
//!   subject (`S.`), its issuer (`I.`), or the subject of the *root* of the verified chain
//!   (`R.`). A bare field is a subject criterion. `S.CN=a,S.CN=b` requires both, as does
//!   `S.OU>=FDB,S.OU<=Team`; repeating an identical criterion is harmless.
//! * `subjectAltName` criteria take the form `PREFIX:value` with `PREFIX` one of `DNS`, `URI`,
//!   `EMAIL`, `IP`. The scope does not change what is examined: the extension is always read
//!   from the peer's own certificate (`S.` and `I.`), or from the root (`R.`).
//! * A value is RFC 4514 escaped text, up to the next `,`. A backslash escapes one of
//!   `` space " # + , ; < = > | \ `` (so a `,` or `|` in a value is written `\,` or `\|`), or
//!   starts a two-digit hex byte (`\C3\A9`). The "raw" form (`#` then hex) is rejected, as is a
//!   value beginning with a space. Unescaped `" + ; < >` end the value, and so cannot appear
//!   in it (the `,` after it is required, so they are errors). Unescaped trailing spaces are an
//!   error (write `\ `). `=` and `.` need no escape, and values may contain spaces
//!   (`S.O=Apple Inc.`). A trailing `,` is allowed.
//! * The empty `verify_peers` setting is not "no rules": FDB uses `FDB_TLS_VERIFY_PEERS` if no
//!   string was configured and `Check.Valid=1` if neither is set
//!   ([`VerifyPeers::resolve`], `LoadedTLSConfig::getVerifyPeers`, 63 (8.0: 62)).
//!
//! # Evaluation
//!
//! (`PeerVerifier`, 701 (8.0: 679); `TLSPolicy::verify_peer`, 1003 (8.0: 981).)
//!
//! 1. The chain must already be valid. FDB's OpenSSL verify callback returns false when
//!    `preverified` is false, *before* looking at any rule, so every rule set, including one
//!    that says `Check.Valid=0`, requires a chain that OpenSSL accepts: trusted, in date, and
//!    for a connection made by host name, matching the name.
//! 2. With no rules, accept. If *any* rule says `Check.Valid=0`, accept without evaluating any
//!    criteria of any rule (so `Check.Valid=0|S.CN=x` evaluates nothing).
//! 3. Otherwise try each rule in order and accept at the first that matches: all subject
//!    criteria against the peer's subject, all issuer criteria against the peer's issuer, all
//!    root criteria against the root's subject. The *root* is the last certificate of the chain
//!    OpenSSL built, which is the trust anchor from the CA store (the CA is not normally sent by
//!    the peer), or the peer's own certificate when it is self-signed and trusted.
//! 4. A name criterion needs **exactly one** attribute of that field in the name (none, or two
//!    `OU`s, or the usual two `DC`s, never match), and compares its value as OpenSSL's
//!    `ASN1_STRING_to_UTF8` renders it. Both sides are treated as Latin-1 text unless the
//!    certificate's string is a UTF8String, BMPString or UniversalString (see
//!    [`AsnString::to_utf8`]); the rule's value is always Latin-1, so a literal non-ASCII
//!    character in a rule does not match the same character in a UTF8String. `\XX` escapes are
//!    how to write the Latin-1 byte.
//! 5. A `subjectAltName` criterion scans the extension's general names in order; the first
//!    that has the right prefix (`DNS:` against a `dNSName`, `EMAIL:` against an `rfc822Name`,
//!    `URI:` against a `uniformResourceIdentifier`) and matches, accepts. A name of type
//!    `otherName`, `x400Address`, `directoryName`, `ediPartyName` or `registeredID` makes the
//!    criterion fail on the spot, even if a later name would match. `IP:` criteria parse but
//!    never match, because an `iPAddress` is an octet string. A certificate with no
//!    subjectAltName fails the criterion.
//! 6. `Check.Unexpired` is parsed and stored (`verify_time`) but nothing reads it: expiry is
//!    only ever checked by step 1, so `Check.Unexpired=0` does not disable date checking
//!    despite the documentation.
//!
//! # Differences from FDB in this port
//!
//! * **Escaped `|`.** FDB's rule splitter loops forever on a `\|` (`set_verify_peers` never
//!   advances past an escaped bar; any such string hangs the process on both branches). This
//!   port splits correctly there.
//! * **Rule parse positions.** FDB only reports `tls_error`; [`ParseError`] adds a kind and
//!   offset.
//! * **Chain validation** is webpki's, shaped to follow OpenSSL's use of its trust store
//!   (see [`VerifyPeersVerifier`]): only a *self-signed*, CA-capable certificate of the CA file
//!   is a trust anchor and its validity period is checked; the other CA-file certificates
//!   (intermediates) only complete chains; `R.` criteria see the self-signed root. The peer's
//!   certificate must also be valid for TLS server use (`keyUsage` and `extendedKeyUsage`, as
//!   OpenSSL's `ssl_server` purpose). A peer that is itself a configured, self-signed,
//!   in-date root is accepted as OpenSSL does (webpki alone would reject a `CA:TRUE`
//!   certificate as an end entity). Revocation is not checked, as in FDB.
//! * **Certificates webpki is stricter about** than OpenSSL are refused, with errors that say
//!   what to change ([`CertificateRejected`]): an end-entity certificate with `CA:TRUE`
//!   issued by another CA, and X.509 version 1 end-entity or intermediate certificates (a
//!   self-signed v1 root in the CA file is fine). Also unsupported by the underlying
//!   libraries: RSA keys under 2048 bits, SHA-1 signatures, DSA, Ed448, and (with the `ring`
//!   provider) P-521.
//! * **Name matching of malformed certificates.** A subjectAltName with any undecodable name
//!   is treated as absent (OpenSSL fails to decode the extension), and a name attribute whose
//!   value is not a universal primitive string never matches.
//! * There is no option to skip chain validation for `Check.Valid=0`: FDB does not skip it.
//!
//! # Differences between 7.4 and 8.0
//!
//! None in the parser or the evaluator. 8.0's `Net2.cpp` differs at the connection level: a
//! connection made by host name (`sni_hostname` set) checks the name with `SSL_set1_host`
//! on both branches, but 8.0 then does *not* apply the verify-peers rules to it (chain plus
//! host name only: `doConnectHandshake`, "An external server (S3, a KMS) is authenticated by
//! CA chain plus the SSL_set1_host() check"), whereas 7.4 applies both. This module does not
//! check host names; a caller that wants the 8.0 behavior for named connections builds
//! the verifier with an empty rule set ([`VerifyPeers::empty`]) and checks the name itself.

mod cert;
mod eval;
mod rules;
mod selfsig;
mod verifier;

pub use cert::{AltName, AsnString, CertParseError, NameEntry, PeerCert};
pub use eval::{AcceptReason, RuleFailure, Verdict};
pub use rules::{
    Criterion, Field, Location, MatchType, ParseError, ParseErrorKind, Rule, Scope, VerifyPeers,
};
pub use verifier::{CertificateRejected, PeerRejected, RootError, VerifyPeersVerifier};

#[cfg(test)]
mod tests;
