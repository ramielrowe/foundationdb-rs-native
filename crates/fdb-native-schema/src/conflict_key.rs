//! The keys a commit adds so that it conflicts with itself.

use fdb_native_wire::Uid;
use fdb_native_wire::binary::{EncodeError, Writer};

/// The prefix of the key `Transaction::makeSelfConflicting` and the idempotency-id commit
/// path add as a read and write conflict range.
pub const SELF_CONFLICT_PREFIX: &[u8] = b"\xff/SC/";

/// The key `Transaction::makeSelfConflicting` makes a transaction conflict with itself on:
/// `\xff/SC/` followed by a random UID in the binary format, 21 bytes. The key is added as
/// both a read and a write conflict range (the single-key range `key..key + \x00`).
///
/// A client does this when a commit is not `causalWriteRisky` and none of its write
/// conflict ranges intersects a read conflict range, so that a commit that returns
/// `commit_unknown_result` can later be settled by committing a conflicting transaction.
/// The server never parses the key (it is only a conflict range); it has to be unique, and a
/// faithful client writes the same bytes.
pub fn self_conflict_key(unique_id: Uid) -> Result<Vec<u8>, EncodeError> {
    let mut w = Writer::unversioned();
    w.write_bytes(SELF_CONFLICT_PREFIX)?;
    w.write(&unique_id)?;
    Ok(w.into_bytes())
}

/// The self-conflict key of a transaction with an idempotency id (`IDEMPOTENCY_ID` or
/// `AUTOMATIC_IDEMPOTENCY`): `\xff/SC/` followed by the id's raw bytes (16 to 255 of them, no
/// length), added unconditionally, whatever the `causalWriteRisky` setting.
pub fn self_conflict_key_for_idempotency_id(id: &[u8]) -> Vec<u8> {
    [SELF_CONFLICT_PREFIX, id].concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys() {
        let key = self_conflict_key(Uid::new(1, 2)).unwrap();
        assert_eq!(key.len(), 21);
        assert_eq!(&key[..5], b"\xff/SC/");
        assert_eq!(&key[5..13], &1u64.to_le_bytes());
        assert_eq!(&key[13..], &2u64.to_le_bytes());
        assert_eq!(
            self_conflict_key_for_idempotency_id(&[7; 16]),
            [&b"\xff/SC/"[..], &[7; 16]].concat()
        );
    }
}
