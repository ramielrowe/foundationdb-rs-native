//! `VerifyPeersVerifier` against generated chains, directly and through handshakes.

use std::sync::Arc;

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, date_time_ymd,
};
use rustls::client::danger::ServerCertVerifier;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::sign::{CertifiedKey, SingleCertAndKey};
use rustls::{
    CertificateError, ClientConfig, ClientConnection, Connection, Error, OtherError, ServerConfig,
    ServerConnection, SignatureScheme,
};

use super::super::*;
use super::support::*;

fn provider() -> rustls::crypto::CryptoProvider {
    rustls::crypto::ring::default_provider()
}

fn der(cert: &Certificate) -> CertificateDer<'static> {
    cert.der().clone()
}

fn verifier(spec: &str, roots: &[&Certificate]) -> VerifyPeersVerifier {
    VerifyPeersVerifier::new(
        VerifyPeers::parse(spec).unwrap(),
        roots.iter().map(|c| der(c)),
        &provider(),
    )
    .unwrap()
}

fn verify(
    v: &VerifyPeersVerifier,
    leaf: &Certificate,
    intermediates: &[&Certificate],
) -> Result<(), Error> {
    verify_named(v, leaf, intermediates, "does-not-match.invalid")
}

fn verify_named(
    v: &VerifyPeersVerifier,
    leaf: &Certificate,
    intermediates: &[&Certificate],
    name: &str,
) -> Result<(), Error> {
    let intermediates: Vec<_> = intermediates.iter().map(|c| der(c)).collect();
    v.verify_server_cert(
        &der(leaf),
        &intermediates,
        &ServerName::try_from(name.to_owned()).unwrap(),
        &[],
        UnixTime::now(),
    )
    .map(|_| ())
}

fn peer_rejected(err: &Error) -> &PeerRejected {
    match err {
        Error::InvalidCertificate(CertificateError::Other(OtherError(e))) => e
            .downcast_ref::<PeerRejected>()
            .unwrap_or_else(|| panic!("not a PeerRejected: {e}")),
        other => panic!("expected a rule rejection, got {other:?}"),
    }
}

#[test]
fn default_accepts_a_valid_chain_ignoring_the_server_name() {
    let pki = Pki::new();
    let v = verifier("Check.Valid=1", &[&pki.root]);
    verify(&v, &pki.leaf, &[&pki.intermediate.cert]).unwrap();
    // Hostnames and IPs alike are ignored.
    verify_named(
        &v,
        &pki.leaf,
        &[&pki.intermediate.cert],
        "node-1.example.com",
    )
    .unwrap();
    verify_named(
        &v,
        &pki.leaf,
        &[&pki.intermediate.cert],
        "other.example.org",
    )
    .unwrap();
    let ip = v.verify_server_cert(
        &der(&pki.leaf),
        &[der(&pki.intermediate.cert)],
        &ServerName::try_from("192.0.2.55").unwrap(),
        &[],
        UnixTime::now(),
    );
    assert!(ip.is_ok());
    // The default rule set and an explicit empty one validate the chain only.
    let v =
        VerifyPeersVerifier::new(VerifyPeers::default(), [der(&pki.root)], &provider()).unwrap();
    verify(&v, &pki.leaf, &[&pki.intermediate.cert]).unwrap();
    let v = VerifyPeersVerifier::new(VerifyPeers::empty(), [der(&pki.root)], &provider()).unwrap();
    verify(&v, &pki.leaf, &[&pki.intermediate.cert]).unwrap();
    assert!(v.verify_peers().rules().is_empty());
}

#[test]
fn chain_must_reach_a_configured_root() {
    let pki = Pki::new();
    let v = verifier("Check.Valid=1", &[&pki.root]);
    // Missing intermediate.
    let err = verify(&v, &pki.leaf, &[]).unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidCertificate(CertificateError::UnknownIssuer)
        ),
        "{err:?}"
    );
    // Some other CA's root.
    let other = Pki::other();
    let v = verifier("Check.Valid=1", &[&other.root]);
    let err = verify(&v, &pki.leaf, &[&pki.intermediate.cert]).unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidCertificate(CertificateError::UnknownIssuer)
        ),
        "{err:?}"
    );
    // No roots at all.
    let v = verifier("Check.Valid=1", &[]);
    assert!(verify(&v, &pki.leaf, &[&pki.intermediate.cert]).is_err());
    // An intermediate configured as the root also works: a trust anchor can be any CA.
    let v = verifier("Check.Valid=1", &[&pki.intermediate.cert]);
    verify(&v, &pki.leaf, &[]).unwrap();
    // Several roots, one of them right.
    let v = verifier("R.CN=Test Root", &[&other.root, &pki.root]);
    verify(&v, &pki.leaf, &[&pki.intermediate.cert]).unwrap();
}

#[test]
fn expired_and_not_yet_valid_are_rejected() {
    let pki = Pki::new();
    let v = verifier("Check.Valid=1", &[&pki.root]);

    let mut p = params(&[("CN", "old")], &[]);
    p.not_before = date_time_ymd(2000, 1, 1);
    p.not_after = date_time_ymd(2001, 1, 1);
    let (expired, _) = pki.intermediate.issue(p);
    let err = verify(&v, &expired, &[&pki.intermediate.cert]).unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidCertificate(CertificateError::ExpiredContext { .. })
        ),
        "{err:?}"
    );

    let mut p = params(&[("CN", "future")], &[]);
    p.not_before = date_time_ymd(3000, 1, 1);
    let (future, _) = pki.intermediate.issue(p);
    let err = verify(&v, &future, &[&pki.intermediate.cert]).unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidCertificate(CertificateError::NotValidYetContext { .. })
        ),
        "{err:?}"
    );

    // FDB parses Check.Unexpired but never acts on it, and neither do we.
    let v = verifier("Check.Unexpired=0", &[&pki.root]);
    assert!(verify(&v, &expired, &[&pki.intermediate.cert]).is_err());
    assert!(verify(&v, &future, &[&pki.intermediate.cert]).is_err());
    verify(&v, &pki.leaf, &[&pki.intermediate.cert]).unwrap();

    // An expired intermediate fails the chain too.
    let old_ca = pki.root_ca.intermediate(&[("CN", "Old CA")]);
    let mut p = ca_params(&[("CN", "Old CA 2")]);
    p.not_before = date_time_ymd(2000, 1, 1);
    p.not_after = date_time_ymd(2001, 1, 1);
    let key = new_key();
    let old_cert = pki.root_ca.sign(&p, &key);
    let old = Ca {
        params: p,
        key,
        cert: old_cert,
    };
    let (leaf, _) = old.issue(params(&[("CN", "under-old")], &[]));
    let v = verifier("Check.Valid=1", &[&pki.root]);
    assert!(verify(&v, &leaf, &[&old.cert]).is_err());
    drop(old_ca);
}

#[test]
fn wrong_extended_key_usage_is_rejected() {
    let pki = Pki::new();
    let v = verifier("Check.Valid=1", &[&pki.root]);
    let mut p = params(&[("CN", "client-only")], &[]);
    p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let key = new_key();
    let cert = pki.intermediate.sign(&p, &key);
    let err = verify(&v, &cert, &[&pki.intermediate.cert]).unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidCertificate(
                CertificateError::InvalidPurpose | CertificateError::InvalidPurposeContext { .. }
            )
        ),
        "{err:?}"
    );
}

#[test]
fn a_ca_certificate_cannot_be_the_peer_unless_it_is_a_root() {
    let pki = Pki::new();
    let v = verifier("Check.Valid=1", &[&pki.root]);
    // The intermediate as the peer certificate, signed by the root: webpki rejects a CA used
    // as an end entity.
    assert!(verify(&v, &pki.intermediate.cert, &[]).is_err());
}

#[test]
fn rules_apply_after_chain_validation() {
    let pki = Pki::new();
    let inter = [&pki.intermediate.cert];
    for (spec, ok) in [
        ("S.CN=node-1", true),
        ("S.CN=node-2", false),
        ("I.CN=Test Intermediate", true),
        ("I.CN=Test Root", false),
        // The root comes from the trust store; the peer never sent it.
        ("R.CN=Test Root", true),
        ("R.CN=Test Intermediate", false),
        ("R.O=Root Org,R.C=US,I.O=CA Org,S.OU=Databases", true),
        ("S.subjectAltName=DNS:node-1.example.com", true),
        ("S.subjectAltName=DNS:node-2.example.com", false),
        ("S.subjectAltName=URI:spiffe://example.com/node-1", true),
        ("S.subjectAltName>=EMAIL:ops@", true),
        // OR.
        ("S.CN=node-2|R.CN=Test Root", true),
        ("S.CN=node-2|R.CN=Test Intermediate", false),
        ("S.CN=node-2|I.CN=nope|S.OU=Databases", true),
        // A passing rule that follows `Check.Valid=1` and `Check.Unexpired`.
        ("Check.Valid=1,Check.Unexpired=1,S.CN=node-1", true),
        ("Check.Unexpired=0,S.CN=node-2", false),
    ] {
        let v = verifier(spec, &[&pki.root]);
        let r = verify(&v, &pki.leaf, &inter);
        assert_eq!(r.is_ok(), ok, "{spec}: {r:?}");
        if !ok {
            let rejected = peer_rejected(&r.unwrap_err()).clone();
            assert_eq!(rejected.failures.len(), spec.split('|').count(), "{spec}");
            assert!(rejected.to_string().contains("rule 0: Rule."), "{rejected}");
        }
    }
}

#[test]
fn rejection_carries_the_reasons() {
    let pki = Pki::new();
    let v = verifier("S.CN=a|I.CN=b", &[&pki.root]);
    let err = verify(&v, &pki.leaf, &[&pki.intermediate.cert]).unwrap_err();
    let rejected = peer_rejected(&err);
    assert_eq!(rejected.failures.len(), 2);
    assert!(
        rejected.failures[0]
            .reason
            .starts_with("Rule.Cert.Subject.Name.NameMismatch[Name=")
    );
    assert!(
        rejected.failures[1]
            .reason
            .starts_with("Rule.Cert.Issuer.Name.NameMismatch[Name=")
    );
    assert!(
        rejected.to_string().contains("rejected by verify_peers"),
        "{rejected}"
    );
}

#[test]
fn rules_do_not_rescue_a_bad_chain() {
    let pki = Pki::new();
    let other = Pki::other();
    // Rules that would match, against a chain that does not validate.
    let v = verifier("S.CN=node-1", &[&other.root]);
    assert!(matches!(
        verify(&v, &pki.leaf, &[&pki.intermediate.cert]).unwrap_err(),
        Error::InvalidCertificate(CertificateError::UnknownIssuer)
    ));
}

#[test]
fn check_valid_zero_still_needs_a_valid_chain_as_in_fdb() {
    let pki = Pki::new();
    let other = Pki::other();
    // `Check.Valid=0` accepts without evaluating any criteria...
    let v = verifier("Check.Valid=0|S.CN=nope", &[&pki.root]);
    verify(&v, &pki.leaf, &[&pki.intermediate.cert]).unwrap();
    let v = verifier("S.CN=nope|Check.Valid=0", &[&pki.root]);
    verify(&v, &pki.leaf, &[&pki.intermediate.cert]).unwrap();
    // ...but, as in FDB (`verify_peer` rejects `!preverified` first), not for a chain that
    // OpenSSL would reject: untrusted or expired.
    let v = verifier("Check.Valid=0", &[&other.root]);
    assert!(verify(&v, &pki.leaf, &[&pki.intermediate.cert]).is_err());
    let v = verifier("Check.Valid=0", &[&pki.root]);
    assert!(verify(&v, &pki.leaf, &[]).is_err());
    // A self-signed certificate that is not in the roots is such a chain.
    let key = new_key();
    let selfsigned = params(&[("CN", "self")], &["self.example.com"])
        .self_signed(&key)
        .unwrap();
    assert!(verify(&v, &selfsigned, &[]).is_err());
}

#[test]
fn check_valid_zero_can_skip_chain_validation() {
    let pki = Pki::new();
    let key = new_key();
    let selfsigned = params(&[("CN", "self")], &[]).self_signed(&key).unwrap();
    let mut old = params(&[("CN", "old")], &[]);
    old.not_after = date_time_ymd(2001, 1, 1);
    let (expired, _) = pki.intermediate.issue(old);

    let v = verifier("Check.Valid=0", &[&pki.root]).check_valid_zero_skips_chain_validation(true);
    verify(&v, &selfsigned, &[]).unwrap();
    verify(&v, &expired, &[]).unwrap();
    verify(&v, &pki.leaf, &[]).unwrap();
    // Any rule with Check.Valid=0 counts, as in FDB.
    let v = verifier("S.CN=x|Check.Valid=0", &[&pki.root])
        .check_valid_zero_skips_chain_validation(true);
    verify(&v, &selfsigned, &[]).unwrap();
    // Without the setting, or without Check.Valid=0, the chain is validated.
    let v = verifier("Check.Valid=1", &[&pki.root]).check_valid_zero_skips_chain_validation(true);
    assert!(verify(&v, &selfsigned, &[]).is_err());
    let v = verifier("Check.Valid=0", &[&pki.root]).check_valid_zero_skips_chain_validation(false);
    assert!(verify(&v, &selfsigned, &[]).is_err());
}

/// The way FDB's own single-certificate setups work (`openssl req -x509` and
/// `--tls_ca_file` pointing at the same file): the peer's certificate is itself the root.
#[test]
fn self_signed_certificate_that_is_its_own_root() {
    let ca = Ca::root(&[("CN", "networktest-smoke")]);
    let v = verifier("Root.CN=networktest-smoke", &[&ca.cert]);
    verify(&v, &ca.cert, &[]).unwrap();
    let v = verifier("Root.CN=dummy-ca", &[&ca.cert]);
    assert!(
        peer_rejected(&verify(&v, &ca.cert, &[]).unwrap_err()).failures[0]
            .reason
            .contains("RootCert.Subject.Name.NameMismatch")
    );
    // The same certificate is the leaf, the issuer and the root.
    let v = verifier(
        "S.CN=networktest-smoke,I.CN=networktest-smoke,R.CN=networktest-smoke",
        &[&ca.cert],
    );
    verify(&v, &ca.cert, &[]).unwrap();
    // Not configured as a root: the CA:TRUE self-signed certificate is not accepted.
    let other = Ca::root(&[("CN", "other")]);
    let v = verifier("Check.Valid=1", &[&other.cert]);
    assert!(verify(&v, &ca.cert, &[]).is_err());
    let v = verifier("Check.Valid=0", &[&other.cert]);
    assert!(verify(&v, &ca.cert, &[]).is_err());

    // Expired and not-yet-valid trusted self-signed certificates are still refused.
    for (nb, na) in [((2000, 1, 1), (2001, 1, 1)), ((3000, 1, 1), (3001, 1, 1))] {
        let mut p = ca_params(&[("CN", "dated")]);
        p.not_before = date_time_ymd(nb.0, nb.1, nb.2);
        p.not_after = date_time_ymd(na.0, na.1, na.2);
        let key = new_key();
        let cert = p.self_signed(&key).unwrap();
        let v = verifier("Check.Valid=1", &[&cert]);
        let err = verify(&v, &cert, &[]).unwrap_err();
        assert!(
            matches!(
                err,
                Error::InvalidCertificate(
                    CertificateError::Expired | CertificateError::NotValidYet
                )
            ),
            "{err:?}"
        );
    }

    // A self-signed leaf that is not a CA, trusted directly.
    let key = new_key();
    let leaf = params(&[("CN", "leaf-root")], &["x.example.com"])
        .self_signed(&key)
        .unwrap();
    let v = verifier(
        "R.CN=leaf-root,S.subjectAltName=DNS:x.example.com",
        &[&leaf],
    );
    verify(&v, &leaf, &[]).unwrap();
}

#[test]
fn invalid_roots_are_reported() {
    let err = VerifyPeersVerifier::new(
        VerifyPeers::default(),
        [CertificateDer::from(vec![1, 2, 3])],
        &provider(),
    )
    .unwrap_err();
    assert_eq!(err.index, 0);
    let pki = Pki::new();
    let err = VerifyPeersVerifier::new(
        VerifyPeers::default(),
        [der(&pki.root), CertificateDer::from(vec![0x30, 0x00])],
        &provider(),
    )
    .unwrap_err();
    assert_eq!(err.index, 1);
    assert!(err.to_string().contains("root certificate 1"), "{err}");
}

#[test]
fn garbage_peer_certificates_are_errors_not_panics() {
    let pki = Pki::new();
    let v = verifier("S.CN=x", &[&pki.root]);
    for junk in [vec![], vec![0], vec![0x30, 0x03, 1, 2, 3], vec![0xFF; 64]] {
        let r = v.verify_server_cert(
            &CertificateDer::from(junk),
            &[],
            &ServerName::try_from("x.invalid").unwrap(),
            &[],
            UnixTime::now(),
        );
        assert!(r.is_err());
    }
}

#[test]
fn supported_schemes_come_from_the_provider() {
    let pki = Pki::new();
    let v = verifier("Check.Valid=1", &[&pki.root]);
    let schemes = v.supported_verify_schemes();
    assert!(schemes.contains(&SignatureScheme::ECDSA_NISTP256_SHA256));
    assert!(schemes.contains(&SignatureScheme::ED25519));
    assert_eq!(
        schemes,
        provider()
            .signature_verification_algorithms
            .supported_schemes()
    );
}

/// Moves whatever `from` has to say to `to`; whether there was anything.
fn pump(from: &mut Connection, to: &mut Connection) -> Result<bool, Error> {
    let mut buf = Vec::new();
    while from.wants_write() {
        from.write_tls(&mut buf).unwrap();
    }
    let mut rd = &buf[..];
    while !rd.is_empty() {
        to.read_tls(&mut rd).unwrap();
        to.process_new_packets()?;
    }
    Ok(!buf.is_empty())
}

/// An in-memory handshake: the verifier does the whole job including the CertificateVerify
/// signature, on both protocol versions.
fn handshake(
    v: VerifyPeersVerifier,
    leaf: &Certificate,
    leaf_key: &rcgen::KeyPair,
    chain_tail: &[&Certificate],
    versions: &[&'static rustls::SupportedProtocolVersion],
    server_name: &str,
) -> Result<(), Error> {
    let provider = Arc::new(provider());
    let client_config = ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(versions)
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(v))
        .with_no_client_auth();
    let mut chain = vec![der(leaf)];
    chain.extend(chain_tail.iter().map(|c| der(c)));
    // Not `with_single_cert`, which would refuse a key that does not match the certificate.
    let key = provider
        .key_provider
        .load_private_key(PrivatePkcs8KeyDer::from(leaf_key.serialize_der()).into())
        .unwrap();
    let server_config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(versions)
        .unwrap()
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(SingleCertAndKey::from(CertifiedKey::new(
            chain, key,
        ))));

    let mut client: Connection = ClientConnection::new(
        Arc::new(client_config),
        ServerName::try_from(server_name.to_owned()).unwrap(),
    )
    .unwrap()
    .into();
    let mut server: Connection = ServerConnection::new(Arc::new(server_config))
        .unwrap()
        .into();

    for _ in 0..16 {
        let a = pump(&mut client, &mut server)?;
        let b = pump(&mut server, &mut client)?;
        if !client.is_handshaking() && !server.is_handshaking() {
            return Ok(());
        }
        if !a && !b {
            break;
        }
    }
    panic!("handshake neither finished nor failed");
}

#[test]
fn full_handshake_on_tls12_and_tls13() {
    let pki = Pki::new();
    for versions in [
        &[&rustls::version::TLS13][..],
        &[&rustls::version::TLS12][..],
    ] {
        let v = verifier("S.CN=node-1,R.CN=Test Root", &[&pki.root]);
        // The server name does not match anything in the certificate.
        handshake(
            v,
            &pki.leaf,
            &pki.leaf_key,
            &[&pki.intermediate.cert],
            versions,
            "some.other.name",
        )
        .unwrap_or_else(|e| panic!("{versions:?}: {e:?}"));
    }
}

#[test]
fn handshake_fails_when_rules_reject() {
    let pki = Pki::new();
    for versions in [
        &[&rustls::version::TLS13][..],
        &[&rustls::version::TLS12][..],
    ] {
        let v = verifier("S.CN=someone-else", &[&pki.root]);
        let err = handshake(
            v,
            &pki.leaf,
            &pki.leaf_key,
            &[&pki.intermediate.cert],
            versions,
            "x.invalid",
        )
        .unwrap_err();
        // The client reports the verifier's error.
        let _ = peer_rejected(&err);
    }
}

#[test]
fn handshake_fails_for_an_untrusted_chain() {
    let pki = Pki::new();
    let other = Pki::other();
    let v = verifier("Check.Valid=0", &[&other.root]);
    let err = handshake(
        v,
        &pki.leaf,
        &pki.leaf_key,
        &[&pki.intermediate.cert],
        &[&rustls::version::TLS13],
        "x.invalid",
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            Error::InvalidCertificate(CertificateError::UnknownIssuer)
        ),
        "{err:?}"
    );
}

#[test]
fn handshake_signature_must_match_the_certificate_key() {
    // The server presents the right chain but signs with another key; the signature check
    // the verifier delegates to the provider catches it, on both protocol versions.
    let pki = Pki::new();
    let imposter = new_key();
    for versions in [
        &[&rustls::version::TLS13][..],
        &[&rustls::version::TLS12][..],
    ] {
        let v = verifier("Check.Valid=1", &[&pki.root]);
        let err = handshake(
            v,
            &pki.leaf,
            &imposter,
            &[&pki.intermediate.cert],
            versions,
            "x.invalid",
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::InvalidCertificate(CertificateError::BadSignature)
            ),
            "{versions:?}: {err:?}"
        );
    }
}

#[test]
fn ca_only_params_are_not_servers() {
    // Sanity: the helper's CA params really are CA certificates.
    let p: CertificateParams = ca_params(&[("CN", "ca")]);
    assert_eq!(p.is_ca, IsCa::Ca(BasicConstraints::Unconstrained));
}
