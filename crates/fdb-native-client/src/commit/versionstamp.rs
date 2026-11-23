//! Versionstamped mutations: the offset encoding and the checks the client does on it.
//!
//! `SetVersionstampedKey` and `SetVersionstampedValue` carry, as the **last 4 bytes** of the
//! key (resp. the value), the position of a 10-byte placeholder as a little-endian `i32`
//! (the encoding of API version 520 and later; older API versions used 2 bytes, which this
//! client does not support, like `fdb-native-tuple`'s packing). The commit proxy overwrites
//! the 10 bytes at that position with the transaction's versionstamp (the commit version, 8
//! bytes big-endian, then the transaction's index in its commit batch, 2 bytes big-endian),
//! strips the 4 bytes and turns the mutation into a `SetValue` (`transformVersionstampMutation`,
//! `Atomic.h` 7.4:303-318; `CommitProxyServer.actor.cpp` 7.4:215-219).

use fdb_native_schema::domain::MutationType;

use super::error::invalid_versionstamp;
use crate::error::Error;

/// The length of the placeholder (`placeVersionstamp`): 8 bytes of commit version and 2 of
/// batch index.
pub const VERSIONSTAMP_LEN: usize = 10;

/// `data` with the 4-byte little-endian `offset` appended: the form a versionstamped key or
/// value takes (`data` has ten placeholder bytes at `offset`).
///
/// ```
/// use fdb_native_client::commit::with_versionstamp_offset;
///
/// // The key `log/` + 10 placeholder bytes + `/x`: the stamp goes at offset 4.
/// let mut key = b"log/".to_vec();
/// key.extend_from_slice(&[0; 10]);
/// key.extend_from_slice(b"/x");
/// let key = with_versionstamp_offset(key, 4);
/// assert_eq!(&key[key.len() - 4..], &[4, 0, 0, 0]);
/// ```
pub fn with_versionstamp_offset(mut data: Vec<u8>, offset: u32) -> Vec<u8> {
    data.extend_from_slice(&offset.to_le_bytes());
    data
}

/// The checks `ReadYourWritesTransaction::atomicOp` does on a versionstamped mutation
/// (`ReadYourWrites.actor.cpp` 7.4:2263-2290, 8.0:2179-2200; `getVersionstampKeyRange`,
/// `Atomic.h` 7.4:268-290), all failing with `client_invalid_operation` (2000):
///
/// * the key (`SetVersionstampedKey`) or the value (`SetVersionstampedValue`) is at least 4
///   bytes long;
/// * its offset, the little-endian `i32` of the last 4 bytes, is not negative;
/// * the 10 placeholder bytes fit before the offset: `offset + 10 <= len - 4`.
///
/// Other mutation types pass.
pub(crate) fn check_offset(kind: MutationType, key: &[u8], value: &[u8]) -> Result<(), Error> {
    let (what, bytes) = match kind {
        MutationType::SET_VERSIONSTAMPED_KEY => ("a versionstamped key", key),
        MutationType::SET_VERSIONSTAMPED_VALUE => ("a versionstamped value", value),
        _ => return Ok(()),
    };
    let Some(split) = bytes.len().checked_sub(4) else {
        return Err(invalid_versionstamp(what, bytes.len(), None));
    };
    let (body, suffix) = bytes.split_at(split);
    let offset = i32::from_le_bytes(suffix.try_into().expect("4 bytes"));
    // `pos < 0 || pos + 10 > size`, without overflow.
    let fits = usize::try_from(offset).is_ok_and(|pos| pos + VERSIONSTAMP_LEN <= body.len());
    if !fits {
        return Err(invalid_versionstamp(what, bytes.len(), Some(offset)));
    }
    Ok(())
}
