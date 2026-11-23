//! Idempotency-id records (`\xff\x02/idmp/`).

use fdb_native_wire::ProtocolVersion;
use fdb_native_wire::binary::{DecodeError, EncodeError, Reader, Writer};

/// `idempotencyIdKeys.begin`: the prefix of the keys the commit proxies store idempotency
/// ids under (`\xff\x02/idmp/`).
pub const IDEMPOTENCY_ID_KEYS_BEGIN: &[u8] = b"\xff\x02/idmp/";

/// The key of an idempotency-id record, from `makeIdempotencySingleKeyRange`: the prefix,
/// the commit version as a big-endian `i64`, and the high-order byte of the batch index.
pub fn idempotency_key(commit_version: i64, high_order_batch_index: u8) -> Vec<u8> {
    let mut key = IDEMPOTENCY_ID_KEYS_BEGIN.to_vec();
    key.extend_from_slice(&commit_version.to_be_bytes());
    key.push(high_order_batch_index);
    key
}

/// `decodeIdempotencyKey`: the commit version and high-order batch-index byte of a record's
/// key. The key's prefix is skipped, not checked, as in C++.
pub fn decode_idempotency_key(key: &[u8]) -> Result<(i64, u8), DecodeError> {
    let mut r = Reader::unversioned(key);
    r.read_bytes(IDEMPOTENCY_ID_KEYS_BEGIN.len())?;
    let version = i64::from_be_bytes(r.read_array()?);
    Ok((version, r.read_u8()?))
}

/// The key range `determineCommitStatus` reads to look for a transaction's idempotency id
/// when the commit version is known to be in `min..=max`: the prefix followed by `min` and
/// by `max + 1`, each a big-endian `i64`. `None` if `max + 1` overflows.
pub fn idempotency_scan_range(
    min_commit_version: i64,
    max_commit_version: i64,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let end = max_commit_version.checked_add(1)?;
    let key = |v: i64| [IDEMPOTENCY_ID_KEYS_BEGIN, &v.to_be_bytes()[..]].concat();
    Some((key(min_commit_version), key(end)))
}

/// One idempotency id in an [`IdempotencyValue`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdempotencyEntry<'a> {
    /// The id: 16 to 255 bytes, borrowed from the value.
    pub id: &'a [u8],
    /// The low-order byte of the transaction's batch index in the commit.
    pub low_order_batch_index: u8,
}

/// The value of an idempotency-id record (`IdempotencyIdKVBuilder`), written by a commit
/// proxy and read by a client that retries a commit after `commit_unknown_result`. Records
/// exist only when a transaction used the `IDEMPOTENCY_ID` or `AUTOMATIC_IDEMPOTENCY`
/// option.
///
/// ```text
/// u64 protocol version   IncludeVersion() (no object-serializer flag)
/// i64 timestamp          the proxy's now(), whole seconds
/// repeated: u8 id length, id bytes, u8 low-order batch index
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotencyValue<'a> {
    /// The protocol version of the proxy that wrote the value.
    pub protocol_version: ProtocolVersion,
    /// `int64_t(now())` when the first id was added.
    pub timestamp: i64,
    /// The ids committed at this version with this batch-index high byte.
    pub entries: Vec<IdempotencyEntry<'a>>,
}

impl<'a> IdempotencyValue<'a> {
    /// Reads a record's value (`kvContainsIdempotencyId` does the same loop).
    pub fn decode(value: &'a [u8]) -> Result<IdempotencyValue<'a>, DecodeError> {
        let mut r = Reader::include_version(value, ProtocolVersion::MIN_INVALID_8_0)?;
        let protocol_version = r
            .protocol_version()
            .ok_or(DecodeError::Invalid("missing protocol version"))?;
        let timestamp = r.read()?;
        let mut entries = Vec::new();
        while !r.is_empty() {
            let len = usize::from(r.read_u8()?);
            let id = r.read_bytes(len)?;
            entries.push(IdempotencyEntry {
                id,
                low_order_batch_index: r.read_u8()?,
            });
        }
        Ok(IdempotencyValue {
            protocol_version,
            timestamp,
            entries,
        })
    }

    /// Writes the value as a proxy of `self.protocol_version` does.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut w = Writer::include_version(self.protocol_version)?;
        w.write(&self.timestamp)?;
        for entry in &self.entries {
            let len = u8::try_from(entry.id.len()).map_err(|_| EncodeError::TooLarge)?;
            w.write(&len)?;
            w.write_bytes(entry.id)?;
            w.write(&entry.low_order_batch_index)?;
        }
        Ok(w.into_bytes())
    }

    /// `kvContainsIdempotencyId`: the batch-index low byte stored with `id`, if the value has it.
    pub fn find(&self, id: &[u8]) -> Option<u8> {
        self.entries
            .iter()
            .find(|e| e.id == id)
            .map(|e| e.low_order_batch_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip() {
        let key = idempotency_key(6_000_000_000_500, 5);
        assert_eq!(
            decode_idempotency_key(&key).unwrap(),
            (6_000_000_000_500, 5)
        );
        assert!(decode_idempotency_key(&key[..key.len() - 1]).is_err());
        let (begin, end) = idempotency_scan_range(10, 19).unwrap();
        assert_eq!(
            decode_idempotency_key(&[&begin[..], &[0]].concat())
                .unwrap()
                .0,
            10
        );
        assert_eq!(
            decode_idempotency_key(&[&end[..], &[0]].concat())
                .unwrap()
                .0,
            20
        );
        assert_eq!(idempotency_scan_range(0, i64::MAX), None);
    }

    #[test]
    fn value_round_trip() {
        let value = IdempotencyValue {
            protocol_version: ProtocolVersion::V7_4,
            timestamp: 1_700_000_000,
            entries: vec![
                IdempotencyEntry {
                    id: &[1; 16],
                    low_order_batch_index: 1,
                },
                IdempotencyEntry {
                    id: &[2; 20],
                    low_order_batch_index: 255,
                },
            ],
        };
        let bytes = value.encode().unwrap();
        assert_eq!(IdempotencyValue::decode(&bytes).unwrap(), value);
        assert_eq!(value.find(&[2; 20]), Some(255));
        assert_eq!(value.find(&[3; 16]), None);
        // A truncated entry is an error, not a shorter list.
        assert!(IdempotencyValue::decode(&bytes[..bytes.len() - 1]).is_err());
    }
}
