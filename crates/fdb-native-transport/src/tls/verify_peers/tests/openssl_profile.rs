//! The checks that make the verifier refuse what OpenSSL (`openssl verify -purpose
//! sslserver`, 3.5) refuses although `rustls-webpki` does not: one test per case of the
//! review, on generated chains.
//!
//! Every accept/reject expectation here was checked against OpenSSL 3.5.5 with equivalent
//! certificates (the review's probe matrix); the comments say where this verifier is
//! deliberately stricter or, for path building, more permissive.

use std::net::{IpAddr, Ipv4Addr};

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, CustomExtension, DnType, DnValue,
    ExtendedKeyUsagePurpose, GeneralSubtree, IsCa, KeyUsagePurpose, NameConstraints, SanType,
    SerialNumber,
};
use rustls::client::danger::ServerCertVerifier;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, Error, OtherError};

use super::super::*;
use super::support::*;

fn provider() -> rustls::crypto::CryptoProvider {
    rustls::crypto::ring::default_provider()
}

fn der(cert: &Certificate) -> CertificateDer<'static> {
    cert.der().clone()
}

fn try_verifier(roots: &[&Certificate]) -> Result<VerifyPeersVerifier, RootError> {
    VerifyPeersVerifier::new(
        VerifyPeers::default(),
        roots.iter().map(|c| der(c)),
        &provider(),
    )
}

fn verifier(roots: &[&Certificate]) -> VerifyPeersVerifier {
    try_verifier(roots).unwrap()
}

fn verify(
    v: &VerifyPeersVerifier,
    leaf: &Certificate,
    intermediates: &[&Certificate],
) -> Result<(), Error> {
    verify_der(v, &der(leaf), intermediates)
}

fn verify_der(
    v: &VerifyPeersVerifier,
    leaf: &CertificateDer<'_>,
    intermediates: &[&Certificate],
) -> Result<(), Error> {
    let intermediates: Vec<_> = intermediates.iter().map(|c| der(c)).collect();
    v.verify_server_cert(
        leaf,
        &intermediates,
        &ServerName::try_from("x.invalid").unwrap(),
        &[],
        UnixTime::now(),
    )
    .map(|_| ())
}

/// The webpki error behind a rejection that webpki reported.
fn webpki_cause(err: &Error) -> Option<&webpki::Error> {
    match err {
        Error::InvalidCertificate(CertificateError::Other(OtherError(e))) => {
            e.downcast_ref::<webpki::Error>()
        }
        _ => None,
    }
}

fn rejected_as(err: &Error) -> Option<&CertificateRejected> {
    match err {
        Error::InvalidCertificate(CertificateError::Other(OtherError(e))) => {
            e.downcast_ref::<CertificateRejected>()
        }
        _ => None,
    }
}

fn is_unknown_issuer(err: &Error) -> bool {
    matches!(
        err,
        Error::InvalidCertificate(CertificateError::UnknownIssuer)
    )
}

fn root(cn: &str, tweak: impl FnOnce(&mut CertificateParams)) -> Ca {
    let mut p = ca_params(&[("CN", cn)]);
    tweak(&mut p);
    Ca::self_signed(p)
}

fn ca_under(parent: &Ca, cn: &str, tweak: impl FnOnce(&mut CertificateParams)) -> Ca {
    let mut p = ca_params(&[("CN", cn)]);
    tweak(&mut p);
    let key = new_key();
    let cert = parent.sign(&p, &key);
    Ca {
        params: p,
        key,
        cert,
    }
}

/// A server certificate with the usual usages, whose parameters `tweak` may change.
fn server(
    parent: &Ca,
    subject: &[(&str, &str)],
    dns: &[&str],
    tweak: impl FnOnce(&mut CertificateParams),
) -> Certificate {
    let mut p = params(subject, dns);
    tweak(&mut p);
    parent.issue(p).0
}

fn extension(oid: &[u64], critical: bool, content: &[u8]) -> CustomExtension {
    let mut ext = CustomExtension::from_oid_content(oid, content.to_vec());
    ext.set_criticality(critical);
    ext
}

fn name_constraints(
    permitted: Vec<GeneralSubtree>,
    excluded: Vec<GeneralSubtree>,
) -> NameConstraints {
    NameConstraints {
        permitted_subtrees: permitted,
        excluded_subtrees: excluded,
    }
}

fn dns_subtree(name: &str) -> GeneralSubtree {
    GeneralSubtree::DnsName(name.to_owned())
}

fn email_subtree(name: &str) -> GeneralSubtree {
    GeneralSubtree::Rfc822Name(name.to_owned())
}

const OID_EMAIL_ADDRESS: [u64; 7] = [1, 2, 840, 113549, 1, 9, 1];
const OID_AKID: [u64; 4] = [2, 5, 29, 35];
const OID_NS_CERT_TYPE: [u64; 7] = [2, 16, 840, 1, 113730, 1, 1];
const OID_FRESHEST_CRL: [u64; 4] = [2, 5, 29, 46];
const OID_INHIBIT_ANY_POLICY: [u64; 4] = [2, 5, 29, 54];
const OID_CERTIFICATE_POLICIES: [u64; 4] = [2, 5, 29, 32];

/// An authorityKeyIdentifier with only a key identifier that is not anyone's.
const AKID_OTHER_KEY: [u8; 8] = [0x30, 0x06, 0x80, 0x04, 1, 2, 3, 4];

fn add_email(p: &mut CertificateParams, email: &str) {
    p.distinguished_name.push(
        DnType::CustomDnType(OID_EMAIL_ADDRESS.to_vec()),
        DnValue::Ia5String(email.try_into().unwrap()),
    );
}

// 1. The trust anchor's pathLenConstraint -------------------------------------------------------

#[test]
fn anchor_path_len_bounds_the_intermediates() {
    // pathLen 0: the root may sign end-entities only.
    let r0 = root("PL0 Root", |p| {
        p.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    });
    let i = ca_under(&r0, "Under PL0", |_| {});
    let under_i = server(&i, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    let v = verifier(&[&r0.cert]);
    let err = verify(&v, &under_i, &[&i.cert]).unwrap_err();
    assert_eq!(
        webpki_cause(&err),
        Some(&webpki::Error::PathLenConstraintViolated),
        "{err:?}"
    );
    // The CA file supplying the intermediate does not change that.
    let v = verifier(&[&r0.cert, &i.cert]);
    assert!(verify(&v, &under_i, &[]).is_err());
    // An end-entity directly under it is fine.
    let direct = server(&r0, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    verify(&verifier(&[&r0.cert]), &direct, &[]).unwrap();

    // pathLen 1: one intermediate, not two.
    let r1 = root("PL1 Root", |p| {
        p.is_ca = IsCa::Ca(BasicConstraints::Constrained(1));
    });
    let a = ca_under(&r1, "PL1 A", |_| {});
    let b = ca_under(&a, "PL1 B", |_| {});
    let v = verifier(&[&r1.cert]);
    let under_a = server(&a, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    verify(&v, &under_a, &[&a.cert]).unwrap();
    let under_b = server(&b, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    let err = verify(&v, &under_b, &[&b.cert, &a.cert]).unwrap_err();
    assert_eq!(
        webpki_cause(&err),
        Some(&webpki::Error::PathLenConstraintViolated),
        "{err:?}"
    );
}

#[test]
fn a_twin_anchor_without_the_limit_still_anchors_the_path() {
    // Same name and key, one with pathLen 0: a path that fails at one is retried at the other.
    let key = new_key();
    let mut limited = ca_params(&[("CN", "Twin Root")]);
    limited.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    let limited_cert = limited.self_signed(&key).unwrap();
    let open = ca_params(&[("CN", "Twin Root")]);
    let open_cert = open.self_signed(&key).unwrap();
    let signer = Ca {
        params: open,
        key,
        cert: open_cert.clone(),
    };
    let i = ca_under(&signer, "Twin Inter", |_| {});
    let leaf = server(&i, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    for roots in [[&limited_cert, &open_cert], [&open_cert, &limited_cert]] {
        verify(&verifier(&roots), &leaf, &[&i.cert]).unwrap();
    }
    assert!(verify(&verifier(&[&limited_cert]), &leaf, &[&i.cert]).is_err());
}

#[test]
fn intermediate_path_len_is_enforced_by_webpki() {
    let r = root("Root", |_| {});
    let a = ca_under(&r, "PL0 Inter", |p| {
        p.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    });
    let b = ca_under(&a, "Below PL0", |_| {});
    let leaf = server(&b, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    let err = verify(&verifier(&[&r.cert]), &leaf, &[&b.cert, &a.cert]).unwrap_err();
    assert_eq!(
        webpki_cause(&err),
        Some(&webpki::Error::PathLenConstraintViolated),
        "{err:?}"
    );
}

// 2. Name constraints on the subject DN ----------------------------------------------------------

#[test]
fn dns_name_constraints_apply_to_a_host_name_like_common_name() {
    let permitted = root("NC Root", |p| {
        p.name_constraints = Some(name_constraints(vec![dns_subtree("example.com")], vec![]));
    });
    let v = verifier(&[&permitted.cert]);
    let leaf = |subject: &[(&str, &str)], dns: &[&str]| server(&permitted, subject, dns, |_| {});
    // Without a dNSName alternative name the CN is checked...
    assert!(verify(&v, &leaf(&[("CN", "evil.org")], &[]), &[]).is_err());
    assert!(verify(&v, &leaf(&[("CN", "EVIL.ORG")], &[]), &[]).is_err());
    assert!(verify(&v, &leaf(&[("CN", "evil_host.org")], &[]), &[]).is_err());
    verify(&v, &leaf(&[("CN", "node.example.com")], &[]), &[]).unwrap();
    // ...every CN of it...
    assert!(verify(&v, &leaf(&[("CN", "node-1"), ("CN", "evil.org")], &[]), &[]).is_err());
    // ...but only if it looks like a host name: a single label, a trailing dot, or a space
    // makes it not one.
    verify(&v, &leaf(&[("CN", "node-1")], &[]), &[]).unwrap();
    verify(&v, &leaf(&[("CN", "evil.org.")], &[]), &[]).unwrap();
    verify(&v, &leaf(&[("CN", "evil corp.org")], &[]), &[]).unwrap();
    // With a dNSName alternative name (which webpki checks) the CN is not looked at.
    verify(
        &v,
        &leaf(&[("CN", "evil.org")], &["node-1.example.com"]),
        &[],
    )
    .unwrap();
    // Other kinds of alternative name do not stop it from being checked.
    let ip_only = server(&permitted, &[("CN", "evil.org")], &[], |p| {
        p.subject_alt_names
            .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))));
    });
    assert!(verify(&v, &ip_only, &[]).is_err());
}

#[test]
fn excluded_dns_subtrees_apply_to_the_common_name() {
    let excluded = root("NC Excluding Root", |p| {
        p.name_constraints = Some(name_constraints(vec![], vec![dns_subtree("evil.org")]));
    });
    let v = verifier(&[&excluded.cert]);
    let bad = server(&excluded, &[("CN", "node.evil.org")], &[], |_| {});
    assert!(verify(&v, &bad, &[]).is_err());
    let good = server(&excluded, &[("CN", "node.good.org")], &[], |_| {});
    verify(&v, &good, &[]).unwrap();
}

#[test]
fn name_constraints_of_an_intermediate_bind_the_common_name_too() {
    let r = root("Plain Root", |_| {});
    let constrained = ca_under(&r, "Constrained Inter", |p| {
        p.name_constraints = Some(name_constraints(vec![dns_subtree("example.com")], vec![]));
    });
    let v = verifier(&[&r.cert]);
    let evil = server(&constrained, &[("CN", "evil.org")], &[], |_| {});
    assert!(verify(&v, &evil, &[&constrained.cert]).is_err());
    let fine = server(&constrained, &[("CN", "a.example.com")], &[], |_| {});
    verify(&v, &fine, &[&constrained.cert]).unwrap();

    // An intermediate's own CN is not checked (OpenSSL checks the CN of the end-entity only).
    let r = root("NC Root", |p| {
        p.name_constraints = Some(name_constraints(vec![dns_subtree("example.com")], vec![]));
    });
    let odd = ca_under(&r, "inter.evil.org", |_| {});
    let leaf = server(&odd, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    verify(&verifier(&[&r.cert]), &leaf, &[&odd.cert]).unwrap();
}

#[test]
fn email_name_constraints_apply_to_the_subject_email_address() {
    let r = root("Mail Root", |p| {
        p.name_constraints = Some(name_constraints(vec![email_subtree("example.com")], vec![]));
    });
    let v = verifier(&[&r.cert]);
    let with_email = |email: &str| server(&r, &[("CN", "node-1")], &[], |p| add_email(p, email));
    verify(&v, &with_email("a@example.com"), &[]).unwrap();
    assert!(verify(&v, &with_email("a@evil.org"), &[]).is_err());
    assert!(verify(&v, &with_email("a@sub.example.com"), &[]).is_err());
    // The same address as an alternative name is webpki's (and was refused before).
    let san = server(&r, &[("CN", "node-1")], &[], |p| {
        p.subject_alt_names
            .push(SanType::Rfc822Name("a@evil.org".try_into().unwrap()));
    });
    assert!(verify(&v, &san, &[]).is_err());
    // An emailAddress that is not an IA5String cannot be compared, which OpenSSL refuses
    // (unsupported name syntax).
    let utf8 = server(&r, &[("CN", "node-1")], &[], |p| {
        p.distinguished_name.push(
            DnType::CustomDnType(OID_EMAIL_ADDRESS.to_vec()),
            DnValue::Utf8String("a@example.com".to_owned()),
        );
    });
    assert!(verify(&v, &utf8, &[]).is_err());

    // Excluded subtrees.
    let r = root("Mail Excluding Root", |p| {
        p.name_constraints = Some(name_constraints(vec![], vec![email_subtree("evil.org")]));
    });
    let v = verifier(&[&r.cert]);
    let bad = server(&r, &[("CN", "node-1")], &[], |p| add_email(p, "a@evil.org"));
    assert!(verify(&v, &bad, &[]).is_err());
    let good = server(&r, &[("CN", "node-1")], &[], |p| add_email(p, "a@good.org"));
    verify(&v, &good, &[]).unwrap();

    // An intermediate whose own subject address violates the root's constraints.
    let r = root("Mail Root", |p| {
        p.name_constraints = Some(name_constraints(vec![email_subtree("example.com")], vec![]));
    });
    let inter = ca_under(&r, "Mail Inter", |p| add_email(p, "a@evil.org"));
    let leaf = server(&inter, &[("CN", "node-1")], &[], |_| {});
    assert!(verify(&verifier(&[&r.cert]), &leaf, &[&inter.cert]).is_err());
}

/// Stricter than OpenSSL, which webpki makes it: it does not implement `directoryName`
/// subtrees and refuses every certificate below one, permitted or excluded, even a leaf whose
/// subject satisfies it (OpenSSL accepts; the probe's `ncdir_ok`).
#[test]
fn directory_name_constraints_are_stricter_than_openssl() {
    let org = || {
        let mut dn = rcgen::DistinguishedName::new();
        dn.push(DnType::OrganizationName, "Org");
        dn
    };
    let permitted = root("DirNC Root", |p| {
        p.name_constraints = Some(name_constraints(
            vec![GeneralSubtree::DirectoryName(org())],
            vec![],
        ));
    });
    let leaf = server(&permitted, &[("O", "Org"), ("CN", "node-1")], &[], |_| {});
    assert!(verify(&verifier(&[&permitted.cert]), &leaf, &[]).is_err());

    let excluded = root("DirNC Excluding Root", |p| {
        p.name_constraints = Some(name_constraints(
            vec![],
            vec![GeneralSubtree::DirectoryName(org())],
        ));
    });
    let leaf = server(&excluded, &[("O", "Other"), ("CN", "node-1")], &[], |_| {});
    assert!(verify(&verifier(&[&excluded.cert]), &leaf, &[]).is_err());
}

// 3. Intermediates must be able to sign ----------------------------------------------------------

#[test]
fn every_intermediate_needs_key_cert_sign_if_it_has_a_key_usage() {
    let r = root("Root", |_| {});
    let v = verifier(&[&r.cert]);
    let leaf_for = |inter: &Ca| server(inter, &[("CN", "node-1")], &["node-1.example.com"], |_| {});

    let no_sign = ca_under(&r, "No Cert Sign", |p| {
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature, KeyUsagePurpose::CrlSign];
    });
    assert!(verify(&v, &leaf_for(&no_sign), &[&no_sign.cert]).is_err());
    // ...in the CA file just the same.
    let v_in_file = verifier(&[&r.cert, &no_sign.cert]);
    assert!(verify(&v_in_file, &leaf_for(&no_sign), &[]).is_err());
    // Deeper in the path too.
    let ok = ca_under(&r, "Signs", |_| {});
    let below = ca_under(&ok, "Below, No Cert Sign", |p| {
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    });
    assert!(verify(&v, &leaf_for(&below), &[&below.cert, &ok.cert]).is_err());

    // No keyUsage extension at all is fine (CA:TRUE says enough).
    let no_ku = ca_under(&r, "No Key Usage", |p| p.key_usages = vec![]);
    verify(&v, &leaf_for(&no_ku), &[&no_ku.cert]).unwrap();
    // keyCertSign among others is fine.
    let both = ca_under(&r, "Both", |p| {
        p.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
        ];
    });
    verify(&v, &leaf_for(&both), &[&both.cert]).unwrap();
}

// 4. Critical extensions ----------------------------------------------------------------------------

#[test]
fn an_anchor_with_an_unsupported_critical_extension_is_not_an_anchor() {
    // freshestCRL is not on OpenSSL's list of extensions the verifier handles, so critical it is
    // "unhandled" (error 34), as is any unknown OID.
    for oid in [&OID_FRESHEST_CRL[..], &[1, 2, 3, 4, 5][..]] {
        let r = root("Critical Root", |p| {
            p.custom_extensions = vec![extension(oid, true, &[0x30, 0x00])];
        });
        // It is self-signed, so the CA file loads, but nothing chains to it.
        let leaf = server(&r, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
        let err = verify(&verifier(&[&r.cert]), &leaf, &[]).unwrap_err();
        assert!(is_unknown_issuer(&err), "{err:?}");
        // Another anchor in the file leaves this one unusable, not the file.
        let good = root("Good Root", |_| {});
        let v = verifier(&[&r.cert, &good.cert]);
        assert!(verify(&v, &leaf, &[]).is_err());
        let leaf = server(&good, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
        verify(&v, &leaf, &[]).unwrap();
    }
    // The same extension, not critical, is fine.
    let r = root("Not Critical Root", |p| {
        p.custom_extensions = vec![extension(&OID_FRESHEST_CRL, false, &[0x30, 0x00])];
    });
    let leaf = server(&r, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    verify(&verifier(&[&r.cert]), &leaf, &[]).unwrap();
    // A critical extension on OpenSSL's list is fine (inhibitAnyPolicy; the review also tried
    // certificatePolicies, nsCertType and policyConstraints).
    let r = root("Supported Critical Root", |p| {
        p.custom_extensions = vec![extension(
            &OID_INHIBIT_ANY_POLICY,
            true,
            &[0x02, 0x01, 0x00],
        )];
    });
    let leaf = server(&r, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    verify(&verifier(&[&r.cert]), &leaf, &[]).unwrap();
}

#[test]
fn a_self_signed_peer_with_an_unsupported_critical_extension_is_refused() {
    let mut p = params(&[("CN", "self-signed")], &["node-1.example.com"]);
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
    ];
    let fine = Ca::self_signed(p.clone());
    verify(&verifier(&[&fine.cert]), &fine.cert, &[]).unwrap();

    p.custom_extensions = vec![extension(&OID_FRESHEST_CRL, true, &[0x30, 0x00])];
    let critical = Ca::self_signed(p);
    let other = root("Other Root", |_| {});
    let v = verifier(&[&critical.cert, &other.cert]);
    let err = verify(&v, &critical.cert, &[]).unwrap_err();
    assert_eq!(
        rejected_as(&err),
        Some(&CertificateRejected::UnhandledCriticalExtension),
        "{err:?}"
    );
}

/// Certificate extensions that webpki cannot handle when critical stay refused on certificates
/// from the peer (stricter than OpenSSL for the ones on its list).
#[test]
fn critical_certificate_policies_on_an_intermediate_are_stricter_than_openssl() {
    let r = root("Root", |_| {});
    let inter = ca_under(&r, "Policy Inter", |p| {
        p.custom_extensions = vec![extension(
            &OID_CERTIFICATE_POLICIES,
            true,
            &[0x30, 0x07, 0x30, 0x05, 0x06, 0x03, 0x2A, 0x03, 0x04],
        )];
    });
    let leaf = server(&inter, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    assert!(verify(&verifier(&[&r.cert]), &leaf, &[&inter.cert]).is_err());
}

// 5. nsCertType ---------------------------------------------------------------------------------

#[test]
fn a_netscape_certificate_type_must_include_ssl_server() {
    let r = root("Root", |_| {});
    let v = verifier(&[&r.cert]);
    let with_type = |bits: &[u8]| {
        server(&r, &[("CN", "node-1")], &["node-1.example.com"], |p| {
            p.custom_extensions = vec![extension(&OID_NS_CERT_TYPE, false, bits)];
        })
    };
    // BIT STRING (unused bits, then the byte): bit 0 sslClient, bit 1 sslServer.
    let client = with_type(&[0x03, 0x02, 0x07, 0x80]);
    let err = verify(&v, &client, &[]).unwrap_err();
    assert!(
        matches!(rejected_as(&err), Some(CertificateRejected::Purpose(why)) if why.contains("nsCertType")),
        "{err:?}"
    );
    // S/MIME alone is refused, and an empty bit string is type 0.
    assert!(verify(&v, &with_type(&[0x03, 0x02, 0x05, 0x20]), &[]).is_err());
    assert!(verify(&v, &with_type(&[0x03, 0x01, 0x00]), &[]).is_err());
    // A bit string that does not parse counts as a malformed extension.
    assert!(verify(&v, &with_type(&[0x04, 0x01, 0x40]), &[]).is_err());
    verify(&v, &with_type(&[0x03, 0x02, 0x06, 0x40]), &[]).unwrap();
    verify(&v, &with_type(&[0x03, 0x02, 0x06, 0xC0]), &[]).unwrap();

    // The root-as-peer shortcut applies it as well.
    let mut p = params(&[("CN", "self-signed")], &[]);
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
    ];
    let fine = Ca::self_signed(p.clone());
    verify(&verifier(&[&fine.cert]), &fine.cert, &[]).unwrap();
    p.custom_extensions = vec![extension(
        &OID_NS_CERT_TYPE,
        false,
        &[0x03, 0x02, 0x07, 0x80],
    )];
    let client_only = Ca::self_signed(p);
    let err = verify(&verifier(&[&client_only.cert]), &client_only.cert, &[]).unwrap_err();
    assert!(
        matches!(rejected_as(&err), Some(CertificateRejected::Purpose(_))),
        "{err:?}"
    );
}

// 6. The anchor's extendedKeyUsage -----------------------------------------------------------------

#[test]
fn an_anchor_with_an_extended_key_usage_needs_server_auth() {
    let usable = |eku: Vec<ExtendedKeyUsagePurpose>| {
        let r = root("EKU Root", |p| p.extended_key_usages = eku);
        let leaf = server(&r, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
        verify(&verifier(&[&r.cert]), &leaf, &[]).is_ok()
    };
    assert!(usable(vec![]));
    assert!(usable(vec![ExtendedKeyUsagePurpose::ServerAuth]));
    assert!(usable(vec![
        ExtendedKeyUsagePurpose::ClientAuth,
        ExtendedKeyUsagePurpose::ServerAuth
    ]));
    // OpenSSL 3 checks the anchor (1.1.1 did not, and accepted these).
    assert!(!usable(vec![ExtendedKeyUsagePurpose::ClientAuth]));
    // anyExtendedKeyUsage alone does not allow serverAuth to OpenSSL's `ssl_server` purpose
    // (3.5.5: error 26 for a leaf, an intermediate and an anchor).
    assert!(!usable(vec![ExtendedKeyUsagePurpose::Any]));
}

#[test]
fn any_extended_key_usage_alone_is_not_a_server_certificate() {
    let r = root("Root", |_| {});
    let v = verifier(&[&r.cert]);
    // Through webpki...
    let mut p = params(&[("CN", "node-1")], &["node-1.example.com"]);
    p.extended_key_usages = vec![ExtendedKeyUsagePurpose::Any];
    let key = new_key();
    let leaf = r.sign(&p, &key);
    assert!(verify(&v, &leaf, &[]).is_err());
    // ...and as a self-signed peer.
    let mut p = params(&[("CN", "self-signed")], &[]);
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
    ];
    p.extended_key_usages = vec![ExtendedKeyUsagePurpose::Any];
    let any = Ca::self_signed(p);
    let other = root("Other", |_| {});
    let err = verify(&verifier(&[&any.cert, &other.cert]), &any.cert, &[]).unwrap_err();
    assert!(
        matches!(rejected_as(&err), Some(CertificateRejected::Purpose(_))),
        "{err:?}"
    );
}

// 7. Self-signed means self-signed ------------------------------------------------------------------

#[test]
fn an_authority_key_identifier_of_another_key_makes_a_certificate_not_self_signed() {
    // rcgen writes a subjectKeyIdentifier for every CA; this authorityKeyIdentifier differs.
    let r = root("Odd AKID Root", |p| {
        p.custom_extensions = vec![extension(&OID_AKID, false, &AKID_OTHER_KEY)];
    });
    assert!(!PeerCert::from_der(r.cert.der()).unwrap().is_self_signed());
    assert_eq!(
        try_verifier(&[&r.cert]).unwrap_err(),
        RootError::NoAnchors { provided: 1 }
    );
    // The CA file's other certificates are unaffected, and a certificate with an identical
    // key identifier is self-signed.
    let good = root("Good", |p| p.use_authority_key_identifier_extension = true);
    assert!(
        PeerCert::from_der(good.cert.der())
            .unwrap()
            .is_self_signed()
    );
    let leaf = server(&good, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    verify(&verifier(&[&r.cert, &good.cert]), &leaf, &[]).unwrap();
    // Offered as the peer's own certificate, it is not a configured root, just a CA
    // certificate that is not an end-entity.
    let err = verify(&verifier(&[&r.cert, &good.cert]), &r.cert, &[]).unwrap_err();
    assert_eq!(
        rejected_as(&err),
        Some(&CertificateRejected::CaUsedAsEndEntity),
        "{err:?}"
    );
}

#[test]
fn an_authority_serial_of_another_certificate_makes_a_certificate_not_self_signed() {
    // authorityKeyIdentifier { authorityCertSerialNumber [2] }
    let akid = |serial: u8| [0x30, 0x03, 0x82, 0x01, serial];
    let serial_of = |serial: u8, akid_serial: u8| {
        root("Serial Root", |p| {
            p.serial_number = Some(SerialNumber::from(u64::from(serial)));
            p.custom_extensions = vec![extension(&OID_AKID, false, &akid(akid_serial))];
        })
    };
    let own = serial_of(0x42, 0x42);
    assert!(PeerCert::from_der(own.cert.der()).unwrap().is_self_signed());
    try_verifier(&[&own.cert]).unwrap();
    let other = serial_of(0x42, 0x43);
    assert!(
        !PeerCert::from_der(other.cert.der())
            .unwrap()
            .is_self_signed()
    );
    assert!(try_verifier(&[&other.cert]).is_err());
}

#[test]
fn a_certificates_authority_key_identifier_must_fit_its_issuer() {
    let r = root("Root", |_| {});
    let inter = ca_under(&r, "Inter", |_| {});
    let v = verifier(&[&r.cert]);
    let leaf_with = |tweak: fn(&mut CertificateParams)| {
        server(&inter, &[("CN", "node-1")], &["node-1.example.com"], tweak)
    };
    // The identifier of the intermediate's key, as rcgen derives it, is accepted.
    verify(
        &v,
        &leaf_with(|p| p.use_authority_key_identifier_extension = true),
        &[&inter.cert],
    )
    .unwrap();
    // Someone else's is not, however well the signature verifies: OpenSSL would not have
    // taken this intermediate as the issuer, and finds no other.
    let wrong = leaf_with(|p| {
        p.custom_extensions = vec![extension(&OID_AKID, false, &AKID_OTHER_KEY)];
    });
    let err = verify(&v, &wrong, &[&inter.cert]).unwrap_err();
    assert!(is_unknown_issuer(&err), "{err:?}");
    // The same at the top of the path: the intermediate names another key than the root's.
    let odd_inter = ca_under(&r, "Odd Inter", |p| {
        p.custom_extensions = vec![extension(&OID_AKID, false, &AKID_OTHER_KEY)];
    });
    let leaf = server(
        &odd_inter,
        &[("CN", "node-1")],
        &["node-1.example.com"],
        |_| {},
    );
    assert!(verify(&v, &leaf, &[&odd_inter.cert]).is_err());
}

// 8. A version 1 certificate as the server certificate ----------------------------------------------

mod version_one {
    use super::*;

    fn pem(text: &str) -> CertificateDer<'static> {
        pem_certs(text).remove(0)
    }

    /// A version 1, self-signed certificate (`CN=V1Root`, made with the review's `mkv1.py`,
    /// since neither rcgen nor current OpenSSL tools write one) and a version 3 server
    /// certificate it issued. `openssl verify -purpose sslserver` accepts both uses.
    fn v1_root() -> CertificateDer<'static> {
        pem(include_str!("fixtures/openssl_v1root.pem"))
    }

    fn leaf() -> CertificateDer<'static> {
        pem(include_str!("fixtures/openssl_leaf_v1root.pem"))
    }

    fn now() -> UnixTime {
        let cert = PeerCert::from_der(leaf().as_ref()).unwrap();
        UnixTime::since_unix_epoch(std::time::Duration::from_secs(
            u64::try_from(cert.not_before()).unwrap() + 3600,
        ))
    }

    fn check(
        roots: Vec<CertificateDer<'static>>,
        peer: &CertificateDer<'static>,
    ) -> Result<(), Error> {
        VerifyPeersVerifier::new(VerifyPeers::default(), roots, &provider())
            .unwrap()
            .verify_server_cert(
                peer,
                &[],
                &ServerName::try_from("x.invalid").unwrap(),
                &[],
                now(),
            )
            .map(|_| ())
    }

    #[test]
    fn a_v1_root_anchors_version_3_certificates() {
        assert!(!PeerCert::from_der(v1_root().as_ref()).unwrap().is_v3());
        check(vec![v1_root()], &leaf()).unwrap();
    }

    #[test]
    fn a_v1_root_is_refused_as_the_server_certificate() {
        // OpenSSL accepts it, but rustls would parse it with webpki and fail the handshake
        // anyway, so the verifier says why.
        let err = check(vec![v1_root()], &v1_root()).unwrap_err();
        assert_eq!(
            rejected_as(&err),
            Some(&CertificateRejected::UnsupportedVersion),
            "{err:?}"
        );
    }
}

// Path building: where webpki finds a path that OpenSSL does not -------------------------------------

/// Documented divergence: two roots with one name and no identifiers. OpenSSL tries the first
/// of them only and fails on the signature when it is the wrong one; webpki tries both.
#[test]
fn twin_roots_without_key_identifiers_are_a_documented_path_building_difference() {
    let a = root("Twin", |_| {});
    let b = root("Twin", |_| {});
    let inter = ca_under(&b, "Twin Inter", |_| {});
    let leaf = server(&inter, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    // `a` first is OpenSSL's failing order (probe: twin_noakid_AB).
    verify(&verifier(&[&a.cert, &b.cert]), &leaf, &[&inter.cert]).unwrap();
    verify(&verifier(&[&b.cert, &a.cert]), &leaf, &[&inter.cert]).unwrap();
    // Either way the path ends at a configured, in-date, CA-capable anchor: without b there is
    // none.
    assert!(verify(&verifier(&[&a.cert]), &leaf, &[&inter.cert]).is_err());
}

/// Documented divergence: a cross-signed intermediate. The peer sends both certificates and the
/// CA file has the root of the second; OpenSSL builds its path from the first and gives up,
/// webpki finds the other.
#[test]
fn cross_signed_intermediates_are_a_documented_path_building_difference() {
    let root_x = root("Root X", |_| {});
    let root_y = root("Root Y", |_| {});
    let cross_key = new_key();
    let cross_params = ca_params(&[("CN", "Cross")]);
    let cross_x = root_x.sign(&cross_params, &cross_key);
    let cross_y = root_y.sign(&cross_params, &cross_key);
    let cross = Ca {
        params: cross_params,
        key: cross_key,
        cert: cross_y.clone(),
    };
    let leaf = server(&cross, &[("CN", "node-1")], &["node-1.example.com"], |_| {});
    let v = verifier(&[&root_y.cert]);
    verify(&v, &leaf, &[&cross_x, &cross_y]).unwrap();
    verify(&v, &leaf, &[&cross_y, &cross_x]).unwrap();
    // Without the intermediate that leads to a configured root, there is no path.
    assert!(verify(&v, &leaf, &[&cross_x]).is_err());
}
