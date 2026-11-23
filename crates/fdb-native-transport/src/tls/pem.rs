//! PEM decoding for certificates, CA bundles and private keys.
//!
//! Certificates and unencrypted keys go through `rustls-pki-types`' PEM
//! reader, which accepts PKCS#8 (`PRIVATE KEY`), PKCS#1 (`RSA PRIVATE KEY`)
//! and SEC1 (`EC PRIVATE KEY`) keys. Encrypted PKCS#8 keys
//! (`ENCRYPTED PRIVATE KEY`) are decrypted with the pure-Rust `pkcs8` crate
//! (PBES2: PBKDF2 with HMAC-SHA1/SHA2, AES-CBC or 3DES-CBC), which is what
//! `openssl pkcs8 -topk8` and `openssl genpkey -aes256` produce. PBES1 and
//! scrypt-keyed files are reported as unsupported by name.

use rustls_pki_types::pem::{self, PemObject};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use zeroize::Zeroizing;

use super::error::TlsError;

const ENCRYPTED_BEGIN: &[u8] = b"-----BEGIN ENCRYPTED PRIVATE KEY-----";
const ENCRYPTED_END: &[u8] = b"-----END ENCRYPTED PRIVATE KEY-----";
const LEGACY_ENCRYPTED_HEADER: &[u8] = b"Proc-Type: 4,ENCRYPTED";

/// Every `CERTIFICATE` section of `pem`, in order. Other sections (such as a
/// private key in a combined file) are skipped. Errors if there are none.
pub(crate) fn certificates(
    what: &'static str,
    pem: &[u8],
) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsError::Pem { what, source })?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificates { what });
    }
    Ok(certs)
}

/// The first private key in `pem`, decrypting it with `password` if it is an
/// encrypted PKCS#8 key.
pub(crate) fn private_key(
    pem: &[u8],
    password: Option<&[u8]>,
) -> Result<PrivateKeyDer<'static>, TlsError> {
    if let Some(block) = find_section(pem, ENCRYPTED_BEGIN, ENCRYPTED_END) {
        return decrypt_pkcs8(block, password);
    }
    if find(pem, LEGACY_ENCRYPTED_HEADER).is_some() {
        return Err(TlsError::LegacyEncryptedKey);
    }
    PrivateKeyDer::from_pem_slice(pem).map_err(|source| match source {
        pem::Error::NoItemsFound => TlsError::NoPrivateKey,
        source => TlsError::Pem {
            what: "key",
            source,
        },
    })
}

fn decrypt_pkcs8(
    block: &[u8],
    password: Option<&[u8]>,
) -> Result<PrivateKeyDer<'static>, TlsError> {
    use pkcs8::pkcs5::EncryptionScheme;

    let password = password
        .filter(|p| !p.is_empty())
        .ok_or(TlsError::KeyPasswordRequired)?;
    let (_label, der) =
        pkcs8::der::pem::decode_vec(block).map_err(|e| TlsError::KeyDecrypt(e.to_string()))?;
    let der = Zeroizing::new(der);
    let info = pkcs8::EncryptedPrivateKeyInfo::try_from(der.as_slice()).map_err(|e| {
        // An unknown cipher or KDF, or a PBES1 variant the parser does not
        // know, fails here, before any decryption.
        TlsError::UnsupportedKeyEncryption(match scheme_oid(&der) {
            Some(oid) => format!("{} ({e})", describe_scheme(&oid)),
            None => format!("cannot parse the PKCS#8 envelope: {e}"),
        })
    })?;
    // Name the scheme up front, so a wrong password is not confused with an
    // unsupported algorithm.
    let scheme = match &info.encryption_algorithm {
        EncryptionScheme::Pbes1(_) => {
            return Err(TlsError::UnsupportedKeyEncryption(
                "PBES1 (for example pbeWithSHA1And3-KeyTripleDES-CBC, written by \
                 `openssl pkcs8 -topk8 -v1`); re-encrypt with PBES2: \
                 `openssl pkcs8 -topk8 -v2 aes-256-cbc`"
                    .to_owned(),
            ));
        }
        EncryptionScheme::Pbes2(params) => {
            format!("PBES2 with cipher OID {}", params.encryption.oid())
        }
        _ => {
            return Err(TlsError::UnsupportedKeyEncryption(
                "an unknown PKCS#5 scheme".to_owned(),
            ));
        }
    };
    let plain = info.decrypt(password).map_err(|e| match e {
        // pkcs5 reports a CBC padding failure, the usual sign of a wrong
        // password, as either variant.
        pkcs8::Error::EncryptedPrivateKey(
            pkcs8::pkcs5::Error::DecryptFailed | pkcs8::pkcs5::Error::EncryptFailed,
        ) => TlsError::KeyDecrypt(format!("wrong password or corrupt key ({scheme})")),
        other => TlsError::UnsupportedKeyEncryption(format!("{scheme}: {other}")),
    })?;
    // `plain` zeroizes itself on drop; the copy handed to rustls is not ours
    // to wipe.
    Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        plain.as_bytes().to_vec(),
    )))
}

/// The algorithm OID at the head of an `EncryptedPrivateKeyInfo`, read without
/// interpreting the parameters.
fn scheme_oid(der: &[u8]) -> Option<String> {
    use pkcs8::der::{Reader, SliceReader};
    use pkcs8::spki::AlgorithmIdentifierRef;

    let mut reader = SliceReader::new(der).ok()?;
    reader
        .sequence(|reader| {
            let algorithm: AlgorithmIdentifierRef<'_> = reader.decode()?;
            // The encrypted data; a sequence must be read to its end.
            let _data: pkcs8::der::asn1::AnyRef<'_> = reader.decode()?;
            Ok(algorithm.oid.to_string())
        })
        .ok()
}

fn describe_scheme(oid: &str) -> String {
    if oid == "1.2.840.113549.1.5.13" {
        format!(
            "PBES2 with an unsupported cipher or key derivation function, such as scrypt (OID {oid})"
        )
    } else if oid.starts_with("1.2.840.113549.1.5.") || oid.starts_with("1.2.840.113549.1.12.1.") {
        format!(
            "PBES1-family scheme (OID {oid}, for example pbeWithSHA1And3-KeyTripleDES-CBC, \
             written by `openssl pkcs8 -topk8 -v1`); re-encrypt with PBES2: \
             `openssl pkcs8 -topk8 -v2 aes-256-cbc`"
        )
    } else {
        format!("unknown encryption scheme (OID {oid})")
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// The bytes from the first `begin` marker through the following `end`
/// marker, inclusive.
fn find_section<'a>(pem: &'a [u8], begin: &[u8], end: &[u8]) -> Option<&'a [u8]> {
    let start = find(pem, begin)?;
    let tail = &pem[start..];
    let stop = find(tail, end)? + end.len();
    Some(&tail[..stop])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An EC P-256 key, PKCS#8, generated with `openssl genpkey`.
    const PLAIN_KEY: &str = include_str!("../../tests/fixtures/ec_key_plain.pem");

    /// `PLAIN_KEY` encrypted with
    /// `openssl pkcs8 -topk8 -v2 aes-256-cbc -passout pass:hunter2`
    /// (OpenSSL 3.5: PBES2, PBKDF2-HMAC-SHA256, AES-256-CBC).
    const ENCRYPTED_KEY: &str = include_str!("../../tests/fixtures/ec_key_aes256_hunter2.pem");

    fn der_of(key: &PrivateKeyDer<'_>) -> Vec<u8> {
        key.secret_der().to_vec()
    }

    #[test]
    fn decrypts_openssl_pbes2_key() {
        let plain = private_key(PLAIN_KEY.as_bytes(), None).unwrap();
        let decrypted = private_key(ENCRYPTED_KEY.as_bytes(), Some(&b"hunter2"[..])).unwrap();
        assert_eq!(der_of(&plain), der_of(&decrypted));
    }

    #[test]
    fn encrypted_key_needs_the_right_password() {
        assert!(matches!(
            private_key(ENCRYPTED_KEY.as_bytes(), None),
            Err(TlsError::KeyPasswordRequired)
        ));
        assert!(matches!(
            private_key(ENCRYPTED_KEY.as_bytes(), Some(&b""[..])),
            Err(TlsError::KeyPasswordRequired)
        ));
        assert!(matches!(
            private_key(ENCRYPTED_KEY.as_bytes(), Some(&b"wrong"[..])),
            Err(TlsError::KeyDecrypt(_))
        ));
    }

    fn assert_decrypts(fixture: &str) {
        let plain = private_key(PLAIN_KEY.as_bytes(), None).unwrap();
        let decrypted = private_key(fixture.as_bytes(), Some(&b"hunter2"[..])).unwrap();
        assert_eq!(der_of(&plain), der_of(&decrypted));
    }

    #[test]
    fn decrypts_3des_and_hmac_sha1_prf_keys() {
        // OpenSSL 1.x wrote PBKDF2-HMAC-SHA1 by default; 3DES is still common.
        assert_decrypts(include_str!("../../tests/fixtures/ec_key_des3_hunter2.pem"));
        assert_decrypts(include_str!(
            "../../tests/fixtures/ec_key_aes128_sha1prf_hunter2.pem"
        ));
    }

    #[test]
    fn pbes1_is_reported_by_name_not_as_a_wrong_password() {
        let pbes1 = include_str!("../../tests/fixtures/ec_key_pbes1_hunter2.pem");
        match private_key(pbes1.as_bytes(), Some(&b"hunter2"[..])) {
            Err(TlsError::UnsupportedKeyEncryption(message)) => {
                assert!(message.contains("PBES1"), "{message}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn wrong_password_names_the_scheme() {
        match private_key(ENCRYPTED_KEY.as_bytes(), Some(&b"nope"[..])) {
            Err(TlsError::KeyDecrypt(message)) => {
                assert!(message.contains("PBES2"), "{message}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn password_is_ignored_for_plain_keys() {
        let key = private_key(PLAIN_KEY.as_bytes(), Some(&b"unused"[..])).unwrap();
        assert!(matches!(key, PrivateKeyDer::Pkcs8(_)));
    }

    #[test]
    fn key_may_follow_a_certificate_in_a_combined_file() {
        let combined =
            format!("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n{PLAIN_KEY}");
        assert!(private_key(combined.as_bytes(), None).is_ok());
    }

    #[test]
    fn missing_key_is_reported() {
        assert!(matches!(
            private_key(b"nothing here", None),
            Err(TlsError::NoPrivateKey)
        ));
    }

    #[test]
    fn legacy_pem_encryption_is_reported() {
        let legacy = "-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\n\
                      DEK-Info: AES-128-CBC,00\n\nAAAA\n-----END RSA PRIVATE KEY-----\n";
        assert!(matches!(
            private_key(legacy.as_bytes(), Some(&b"pw"[..])),
            Err(TlsError::LegacyEncryptedKey)
        ));
    }

    #[test]
    fn certificates_require_at_least_one() {
        assert!(matches!(
            certificates("CA", PLAIN_KEY.as_bytes()),
            Err(TlsError::NoCertificates { what: "CA" })
        ));
    }
}
