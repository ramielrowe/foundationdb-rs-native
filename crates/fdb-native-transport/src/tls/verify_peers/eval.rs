//! Evaluating parsed rules against a peer's certificates: a port of FDB's `PeerVerifier`
//! (`flow/TLSConfig.actor.cpp` on `release-7.4`, `flow/TLSConfig.cpp` on `release-8.0`).

use std::fmt;

use super::cert::{AltName, NameEntry, PeerCert, match_entry};
use super::rules::{Criterion, Field, Location, Rule, Scope, VerifyPeers};

/// Why [`VerifyPeers::evaluate`] accepted a peer (FDB's `getSuccessReason`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceptReason {
    /// There are no rules at all ("No rule defined").
    NoRules,
    /// Some rule has `Check.Valid=0`, so no criteria were evaluated ("At least one
    /// certificate verfications rule disabled" in FDB's spelling).
    CheckValidDisabled,
    /// The rule at this index matched ("Rule matched successfully").
    RuleMatched(usize),
}

impl fmt::Display for AcceptReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AcceptReason::NoRules => f.write_str("No rule defined"),
            AcceptReason::CheckValidDisabled => {
                f.write_str("At least one certificate verification rule disabled")
            }
            AcceptReason::RuleMatched(i) => write!(f, "Rule {i} matched successfully"),
        }
    }
}

/// Why one rule did not match, in FDB's trace format: a dotted path to the failing criterion,
/// such as `Rule.Cert.Subject.Name.NameMismatch[Name=C = US, O = Example]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuleFailure {
    /// Index of the rule in [`VerifyPeers::rules`].
    pub rule: usize,
    /// The path and context.
    pub reason: String,
}

impl fmt::Display for RuleFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rule {}: {}", self.rule, self.reason)
    }
}

/// The outcome of evaluating the rules against a peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The peer is accepted.
    Accepted(AcceptReason),
    /// No rule matched; one failure per rule, in rule order (FDB traces each as
    /// `TLSPolicyFailureNN`).
    Rejected(Vec<RuleFailure>),
}

impl Verdict {
    /// Whether the peer is accepted.
    pub fn is_accepted(&self) -> bool {
        matches!(self, Verdict::Accepted(_))
    }
}

impl VerifyPeers {
    /// Evaluates the rules (`PeerVerifier::verify`). This is *only* the field matching:
    /// the chain must already have been validated (see
    /// [`VerifyPeersVerifier`](super::VerifyPeersVerifier), which does both).
    ///
    /// * With no rules, the peer is accepted.
    /// * If any rule has `Check.Valid=0`, the peer is accepted without looking at any
    ///   criteria of any rule.
    /// * Otherwise the peer is accepted if some rule matches, trying them in order. A rule
    ///   matches if every subject criterion holds for `leaf`'s subject, every issuer criterion
    ///   for `leaf`'s issuer, and every root criterion for `root`'s subject.
    ///
    /// `leaf` is the peer's own certificate and `root` the last certificate of the verified
    /// chain (the trust anchor; the peer's own certificate when it is self-signed). `root` may
    /// be `None` if no rule has root criteria ([`VerifyPeers::needs_root`]); a rule with root
    /// criteria then fails with `Missing`.
    pub fn evaluate(&self, leaf: &PeerCert, root: Option<&PeerCert>) -> Verdict {
        if self.rules().is_empty() {
            return Verdict::Accepted(AcceptReason::NoRules);
        }
        if self.check_valid_disabled() {
            return Verdict::Accepted(AcceptReason::CheckValidDisabled);
        }
        let mut failures = Vec::new();
        for (index, rule) in self.rules().iter().enumerate() {
            let mut state = State::default();
            if verify_rule(rule, leaf, root, &mut state) {
                return Verdict::Accepted(AcceptReason::RuleMatched(index));
            }
            failures.push(RuleFailure {
                rule: index,
                reason: state.reason(),
            });
        }
        Verdict::Rejected(failures)
    }

    /// Whether evaluating needs the root certificate: some rule has root criteria and the
    /// evaluation is not short-circuited by `Check.Valid=0`.
    pub fn needs_root(&self) -> bool {
        !self.check_valid_disabled() && self.rules().iter().any(Rule::has_root_criteria)
    }
}

/// FDB's `m_verifyState`, `m_currentName` and `m_currentExtension`, which together make the
/// failure reason.
#[derive(Default)]
struct State {
    path: Vec<&'static str>,
    current_name: String,
    current_extension: String,
}

impl State {
    fn reason(&self) -> String {
        let mut out = self.path.join(".");
        if !self.current_name.is_empty() {
            out.push_str("[Name=");
            out.push_str(&self.current_name);
            out.push(']');
        }
        if !self.current_extension.is_empty() {
            out.push_str("[Extension=");
            out.push_str(&self.current_extension);
            out.push(']');
        }
        out
    }
}

fn verify_rule(rule: &Rule, leaf: &PeerCert, root: Option<&PeerCert>, st: &mut State) -> bool {
    st.path.push("Rule");

    st.path.push("Cert");
    st.path.push("Subject");
    if !verify_criteria(
        leaf,
        leaf.subject(),
        leaf.subject_display(),
        rule,
        Scope::Subject,
        st,
    ) {
        return false;
    }
    st.path.pop();

    st.path.push("Issuer");
    if !verify_criteria(
        leaf,
        leaf.issuer(),
        leaf.issuer_display(),
        rule,
        Scope::Issuer,
        st,
    ) {
        return false;
    }
    st.path.pop();
    st.path.pop();

    st.path.push("RootCert.Subject");
    match root {
        Some(root) => {
            if !verify_criteria(
                root,
                root.subject(),
                root.subject_display(),
                rule,
                Scope::Root,
                st,
            ) {
                return false;
            }
        }
        None if rule.has_root_criteria() => {
            st.path.push("Missing");
            return false;
        }
        None => {}
    }
    st.path.pop();

    st.path.pop();
    true
}

fn verify_criteria(
    cert: &PeerCert,
    name: &[NameEntry],
    name_display: &str,
    rule: &Rule,
    scope: Scope,
    st: &mut State,
) -> bool {
    rule.set(scope)
        .iter()
        .all(|criterion| match_criterion(cert, name, name_display, criterion, st))
}

fn match_criterion(
    cert: &PeerCert,
    name: &[NameEntry],
    name_display: &str,
    criterion: &Criterion,
    st: &mut State,
) -> bool {
    let matched = match criterion.location() {
        Location::Name => {
            st.path.push("Name");
            match_name(name, name_display, criterion, st)
        }
        Location::Extension => {
            st.path.push("Extension");
            st.current_extension = String::from_utf8_lossy(criterion.value()).into_owned();
            match_extension(cert, criterion, st)
        }
    };
    if matched {
        st.current_name.clear();
        st.current_extension.clear();
        st.path.pop();
    }
    matched
}

/// `matchNameCriteria`: the name must have exactly one attribute of the field (so a name with
/// two `OU`s, or the usual two `DC`s, can never satisfy a criterion on that field), and its
/// value must match.
fn match_name(
    name: &[NameEntry],
    name_display: &str,
    criterion: &Criterion,
    st: &mut State,
) -> bool {
    let mut entries = name.iter().filter(|e| e.field == criterion.field());
    let Some(entry) = entries.next() else {
        st.path.push("Missing");
        return false;
    };
    if entries.next().is_some() {
        st.path.push("MultipleNames");
        return false;
    }
    st.current_name = name_display.to_owned();
    if match_entry(criterion.value(), &entry.value, criterion.match_type()) {
        return true;
    }
    st.path.push("NameMismatch");
    false
}

/// `matchExtensionCriteria`, which only supports `subjectAltName`. The general names are
/// scanned in order: the first match accepts, and the first name of an unsupported type
/// (`otherName`, `x400Address`, `directoryName`, `ediPartyName`, `registeredID`) rejects, even
/// if a later name would have matched. `iPAddress` names are skipped over in effect: they
/// are octet strings, which never match.
fn match_extension(cert: &PeerCert, criterion: &Criterion, st: &mut State) -> bool {
    if criterion.field() != Field::SubjectAltName {
        st.path.push("UnsupportedNIDExtensionType");
        return false;
    }
    let Some(names) = cert.alt_names() else {
        st.path.push("EmptySans");
        return false;
    };
    let value = criterion.value();
    let check = |prefix: &[u8], entry: &super::cert::AsnString| {
        value.starts_with(prefix)
            && match_entry(&value[prefix.len()..], entry, criterion.match_type())
    };
    for name in names {
        match name {
            AltName::Unsupported(kind) => {
                st.path.push(kind);
                return false;
            }
            AltName::Email(e) if check(b"EMAIL:", e) => return true,
            AltName::Dns(e) if check(b"DNS:", e) => return true,
            AltName::Uri(e) if check(b"URI:", e) => return true,
            // `IP:`: the entry is an OCTET STRING, which has no UTF-8 conversion.
            _ => {}
        }
    }
    st.path.push("NoMatchingEntry");
    false
}
