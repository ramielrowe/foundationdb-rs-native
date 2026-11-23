//! Certificate generation for the tests.

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType,
};

pub(super) fn new_key() -> KeyPair {
    KeyPair::generate().unwrap()
}

pub(super) fn dn(pairs: &[(&str, &str)]) -> DistinguishedName {
    let mut dn = DistinguishedName::new();
    for (name, value) in pairs {
        let ty = match *name {
            "CN" => DnType::CommonName,
            "C" => DnType::CountryName,
            "L" => DnType::LocalityName,
            "ST" => DnType::StateOrProvinceName,
            "O" => DnType::OrganizationName,
            "OU" => DnType::OrganizationalUnitName,
            other => panic!("no such DN type {other}"),
        };
        dn.push(ty, *value);
    }
    dn
}

/// Parameters for an end-entity certificate with the given subject and DNS alternative names.
pub(super) fn params(subject: &[(&str, &str)], dns: &[&str]) -> CertificateParams {
    let mut p = CertificateParams::default();
    p.distinguished_name = dn(subject);
    p.subject_alt_names = dns
        .iter()
        .map(|d| SanType::DnsName((*d).try_into().unwrap()))
        .collect();
    p
}

pub(super) fn ca_params(subject: &[(&str, &str)]) -> CertificateParams {
    let mut p = params(subject, &[]);
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    p
}

/// A certificate with its key and the parameters it was made from (an `Issuer` needs them).
pub(super) struct Ca {
    pub(super) params: CertificateParams,
    pub(super) key: KeyPair,
    pub(super) cert: Certificate,
}

impl Ca {
    pub(super) fn root(subject: &[(&str, &str)]) -> Ca {
        let params = ca_params(subject);
        let key = new_key();
        let cert = params.self_signed(&key).unwrap();
        Ca { params, key, cert }
    }

    pub(super) fn intermediate(&self, subject: &[(&str, &str)]) -> Ca {
        let params = ca_params(subject);
        let key = new_key();
        let cert = self.sign(&params, &key);
        Ca { params, key, cert }
    }

    pub(super) fn sign(&self, params: &CertificateParams, key: &KeyPair) -> Certificate {
        params
            .signed_by(key, &Issuer::from_params(&self.params, &self.key))
            .unwrap()
    }

    /// A server certificate with the usual extended key usage.
    pub(super) fn issue(&self, mut params: CertificateParams) -> (Certificate, KeyPair) {
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let key = new_key();
        let cert = self.sign(&params, &key);
        (cert, key)
    }
}

/// `Test Root` -> `Test Intermediate` -> leaf `node-1`.
pub(super) struct Pki {
    pub(super) root: Certificate,
    pub(super) root_ca: Ca,
    pub(super) intermediate: Ca,
    pub(super) leaf: Certificate,
    pub(super) leaf_key: KeyPair,
}

impl Pki {
    pub(super) fn new() -> Pki {
        Self::named("Test Root", "Test Intermediate")
    }

    /// An unrelated PKI whose CAs have different names (webpki matches issuers by name
    /// first, so same-named CAs would fail on the signature instead).
    pub(super) fn other() -> Pki {
        Self::named("Other Root", "Other Intermediate")
    }

    fn named(root: &str, intermediate: &str) -> Pki {
        let root_ca = Ca::root(&[("C", "US"), ("O", "Root Org"), ("CN", root)]);
        let intermediate = root_ca.intermediate(&[("O", "CA Org"), ("CN", intermediate)]);
        let mut p = params(
            &[("O", "Leaf Org"), ("OU", "Databases"), ("CN", "node-1")],
            &["node-1.example.com"],
        );
        p.subject_alt_names.push(SanType::URI(
            "spiffe://example.com/node-1".try_into().unwrap(),
        ));
        p.subject_alt_names
            .push(SanType::Rfc822Name("ops@example.com".try_into().unwrap()));
        let (leaf, leaf_key) = intermediate.issue(p);
        Pki {
            root: root_ca.cert.clone(),
            root_ca,
            intermediate,
            leaf,
            leaf_key,
        }
    }
}

impl Ca {
    /// A self-signed certificate from arbitrary parameters.
    pub(super) fn self_signed(params: CertificateParams) -> Ca {
        let key = new_key();
        let cert = params.self_signed(&key).unwrap();
        Ca { params, key, cert }
    }
}

/// Decodes the certificates of a PEM file.
pub(super) fn pem_certs(pem: &str) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    let mut out = Vec::new();
    let mut b64 = String::new();
    let mut inside = false;
    for line in pem.lines() {
        if line.starts_with("-----BEGIN CERTIFICATE") {
            inside = true;
            b64.clear();
        } else if line.starts_with("-----END CERTIFICATE") {
            inside = false;
            out.push(rustls::pki_types::CertificateDer::from(base64(&b64)));
        } else if inside {
            b64.push_str(line.trim());
        }
    }
    out
}

fn base64(s: &str) -> Vec<u8> {
    let value = |c: u8| -> u32 {
        match c {
            b'A'..=b'Z' => u32::from(c - b'A'),
            b'a'..=b'z' => u32::from(c - b'a') + 26,
            b'0'..=b'9' => u32::from(c - b'0') + 52,
            b'+' => 62,
            b'/' => 63,
            _ => panic!("bad base64"),
        }
    };
    let bytes: Vec<u8> = s.bytes().filter(|&c| c != b'=').collect();
    let mut out = Vec::new();
    for chunk in bytes.chunks(4) {
        let mut v = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            v |= value(c) << (18 - 6 * i);
        }
        for i in 0..chunk.len() * 6 / 8 {
            out.push((v >> (16 - 8 * i)) as u8);
        }
    }
    out
}
