//! PEM decoding for certificates, CA bundles and private keys.
//!
//! Certificates and unencrypted keys go through `rustls-pki-types`' PEM
//! reader, which accepts PKCS#8 (`PRIVATE KEY`), PKCS#1 (`RSA PRIVATE KEY`)
//! and SEC1 (`EC PRIVATE KEY`) keys. Encrypted PKCS#8 keys
//! (`ENCRYPTED PRIVATE KEY`) are decrypted with the pure-Rust `pkcs8` crate
//! (PBES2: PBKDF2 with HMAC-SHA1/SHA2 and AES-CBC), which is what
//! `openssl pkcs8 -topk8` and `openssl genpkey -aes256` produce.

use rustls_pki_types::pem::{self, PemObject};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

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
    password: Option<&str>,
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

fn decrypt_pkcs8(block: &[u8], password: Option<&str>) -> Result<PrivateKeyDer<'static>, TlsError> {
    let password = password
        .filter(|p| !p.is_empty())
        .ok_or(TlsError::KeyPasswordRequired)?;
    let (_label, der) =
        pkcs8::der::pem::decode_vec(block).map_err(|e| TlsError::KeyDecrypt(e.to_string()))?;
    let info = pkcs8::EncryptedPrivateKeyInfo::try_from(der.as_slice())
        .map_err(|e| TlsError::KeyDecrypt(e.to_string()))?;
    let plain = info
        .decrypt(password)
        .map_err(|e| TlsError::KeyDecrypt(e.to_string()))?;
    Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        plain.as_bytes().to_vec(),
    )))
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
        let decrypted = private_key(ENCRYPTED_KEY.as_bytes(), Some("hunter2")).unwrap();
        assert_eq!(der_of(&plain), der_of(&decrypted));
    }

    #[test]
    fn encrypted_key_needs_the_right_password() {
        assert!(matches!(
            private_key(ENCRYPTED_KEY.as_bytes(), None),
            Err(TlsError::KeyPasswordRequired)
        ));
        assert!(matches!(
            private_key(ENCRYPTED_KEY.as_bytes(), Some("")),
            Err(TlsError::KeyPasswordRequired)
        ));
        assert!(matches!(
            private_key(ENCRYPTED_KEY.as_bytes(), Some("wrong")),
            Err(TlsError::KeyDecrypt(_))
        ));
    }

    #[test]
    fn password_is_ignored_for_plain_keys() {
        let key = private_key(PLAIN_KEY.as_bytes(), Some("unused")).unwrap();
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
            private_key(legacy.as_bytes(), Some("pw")),
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
