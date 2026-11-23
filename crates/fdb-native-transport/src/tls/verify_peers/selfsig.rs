//! Checking a certificate's signature against its own key, with the signature algorithms the
//! crypto provider supplies (webpki does not expose this for CA certificates).

use rustls::pki_types::SignatureVerificationAlgorithm;

/// The result of [`check`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SelfSignature {
    /// The signature verifies under the certificate's own public key.
    Valid,
    /// An algorithm that handles this certificate rejected the signature.
    Invalid,
    /// No supported algorithm handles it (an old hash, a curve the provider lacks), or the
    /// certificate is not shaped as expected.
    Unverifiable,
}

/// One DER element: its content, what follows it, and the whole element.
pub(super) struct Tlv<'a> {
    pub(super) tag: u8,
    pub(super) content: &'a [u8],
    pub(super) rest: &'a [u8],
    pub(super) whole: &'a [u8],
}

pub(super) fn tlv(buf: &[u8]) -> Option<Tlv<'_>> {
    let tag = *buf.first()?;
    let first = *buf.get(1)?;
    let (len, header) = if first < 0x80 {
        (usize::from(first), 2)
    } else {
        let n = usize::from(first & 0x7F);
        if n == 0 || n > 4 {
            return None;
        }
        let mut len = 0usize;
        for i in 0..n {
            len = (len << 8) | usize::from(*buf.get(2 + i)?);
        }
        (len, 2 + n)
    };
    let end = header.checked_add(len)?;
    Some(Tlv {
        tag,
        content: buf.get(header..end)?,
        rest: buf.get(end..)?,
        whole: buf.get(..end)?,
    })
}

/// Verifies `der`'s signature with its own subjectPublicKey.
pub(super) fn check(
    der: &[u8],
    algorithms: &[&dyn SignatureVerificationAlgorithm],
) -> SelfSignature {
    verify(der, algorithms).unwrap_or(SelfSignature::Unverifiable)
}

fn verify(der: &[u8], algorithms: &[&dyn SignatureVerificationAlgorithm]) -> Option<SelfSignature> {
    // Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, signatureValue }
    let cert = tlv(der)?;
    let tbs = tlv(cert.content)?;
    let sig_alg = tlv(tbs.rest)?;
    let sig_value = tlv(sig_alg.rest)?;
    let (&unused_bits, signature) = sig_value.content.split_first()?;
    if sig_value.tag != 0x03 || unused_bits != 0 {
        return None;
    }

    // TBSCertificate: [0] version, serial, signature, issuer, validity, subject, spki.
    let mut fields = tlv(tbs.content)?;
    if fields.tag == 0xA0 {
        fields = tlv(fields.rest)?;
    }
    for _ in 0..4 {
        // serial to subject: four steps
        fields = tlv(fields.rest)?;
    }
    let spki = tlv(fields.rest)?;
    let spki_alg = tlv(spki.content)?;
    let key_bits = tlv(spki_alg.rest)?;
    let (&unused_bits, public_key) = key_bits.content.split_first()?;
    if key_bits.tag != 0x03 || unused_bits != 0 {
        return None;
    }

    let algorithm = algorithms.iter().find(|a| {
        a.signature_alg_id().as_ref() == sig_alg.content
            && a.public_key_alg_id().as_ref() == spki_alg.content
    })?;
    Some(
        match algorithm.verify_signature(public_key, tbs.whole, signature) {
            Ok(()) => SelfSignature::Valid,
            Err(_) => SelfSignature::Invalid,
        },
    )
}
