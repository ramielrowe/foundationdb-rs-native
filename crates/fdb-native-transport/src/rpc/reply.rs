//! Decoding a reply envelope, `ErrorOr<EnsureTable<T>>`.

use fdb_native_wire::{
    Decode, DecodeError, EnsureTable, ErrorOr, FdbError, FileIdentified, Reader, Shape, Wire,
};

use super::RequestError;

/// Decodes a reply payload as FDB sends it, `ErrorOr<EnsureTable<T>>` (what `NetSAV::receive`
/// reads, `fdbrpc/include/fdbrpc/fdbrpc.h` 7.4:129-140, 8.0:130-141), and maps the error arm
/// to a [`RequestError`] with [`RequestError::from_reply_error`].
///
/// The message's file identifier must be `ErrorOr<EnsureTable<T>>`'s, for an error reply as
/// well as a value (the server serializes both as that type): otherwise this fails with
/// [`RequestError::Decode`] ([`DecodeError::FileIdentifierMismatch`]), as FDB's
/// `ObjectReader` would refuse it.
///
/// `T` may borrow from `payload` (a `T: Decode<'de>`).
///
/// ```
/// use fdb_native_transport::rpc::{RequestError, decode_reply};
/// use fdb_native_wire::{EnsureTable, ErrorOr, FdbError, Void};
///
/// let ok: ErrorOr<EnsureTable<Void>> = Ok(EnsureTable(Void));
/// assert_eq!(decode_reply::<Void>(&fdb_native_wire::encode(&ok)?), Ok(Void));
///
/// let err: ErrorOr<EnsureTable<Void>> = Err(FdbError::new(1020));
/// assert_eq!(
///     decode_reply::<Void>(&fdb_native_wire::encode(&err)?),
///     Err(RequestError::Fdb(FdbError::new(1020)))
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn decode_reply<'de, T>(payload: &'de [u8]) -> Result<T, RequestError>
where
    T: Decode<'de> + FileIdentified,
{
    let envelope = fdb_native_wire::decode::<ErrorOr<EnsureTable<T>>>(payload)?;
    reply_result(envelope.map(|EnsureTable(value)| value))
}

/// Maps a decoded reply envelope to a result: the error arm becomes a [`RequestError`]
/// ([`RequestError::from_reply_error`]). For replies decoded by other means, such as
/// `fdb_native_schema::Request::decode_reply`.
pub fn reply_result<T>(envelope: ErrorOr<T>) -> Result<T, RequestError> {
    envelope.map_err(RequestError::from_reply_error)
}

/// The error a reply envelope carries, if it is well formed and its error arm is set.
/// Neither the file identifier nor the value arm is looked at.
pub(crate) fn peek_reply_error(payload: &[u8]) -> Option<FdbError> {
    let reader = Reader::new(payload).ok()?;
    match reader.decode_root::<ErrorOr<EnsureTable<Opaque>>>() {
        Ok(Err(e)) => Some(e),
        _ => None,
    }
}

/// A table that is not read.
struct Opaque;

impl Wire for Opaque {
    const SHAPE: Shape = Shape::Table;
}

impl<'de> Decode<'de> for Opaque {
    fn absent() -> Self {
        Opaque
    }

    fn decode_object(_: &Reader<'de>, _: usize) -> Result<Self, DecodeError> {
        Ok(Opaque)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fdb_native_wire::{Uid, Void};

    #[test]
    fn peeks_the_error_arm_only() {
        let err: ErrorOr<EnsureTable<Uid>> = Err(FdbError::new(1100));
        let bytes = fdb_native_wire::encode(&err).unwrap();
        assert_eq!(peek_reply_error(&bytes), Some(FdbError::new(1100)));
        let ok: ErrorOr<EnsureTable<Uid>> = Ok(EnsureTable(Uid::new(1, 2)));
        let bytes = fdb_native_wire::encode(&ok).unwrap();
        assert_eq!(peek_reply_error(&bytes), None);
        let ok: ErrorOr<EnsureTable<Void>> = Ok(EnsureTable(Void));
        let bytes = fdb_native_wire::encode(&ok).unwrap();
        assert_eq!(peek_reply_error(&bytes), None);
        assert_eq!(peek_reply_error(b"junk"), None);
    }
}
