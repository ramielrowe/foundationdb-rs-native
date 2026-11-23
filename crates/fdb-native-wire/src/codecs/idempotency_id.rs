//! FDB's `IdempotencyIdRef`.

use crate::{
    Decode, DecodeError, Encode, EncodeError, FileIdentified, FileIdentifier, ObjectRef, Reader,
    Shape, Wire, Writer,
};

/// The bytes are neither empty nor 16 to 255 bytes long.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "an idempotency id is empty (invalid) or {} to {} bytes, not {len}",
    IdempotencyIdRef::MIN_LEN,
    IdempotencyIdRef::MAX_LEN
)]
pub struct InvalidIdempotencyId {
    /// The rejected length.
    pub len: usize,
}

/// FDB's `IdempotencyIdRef` (`IdempotencyId.h`): the id a client attaches to a commit so
/// that it can ask whether a commit with an unknown outcome happened. Either empty (FDB's
/// "invalid" id, what a transaction without an id carries) or 16 to 255 bytes.
///
/// On the wire it is a plain string blob (`dynamic_size_traits<IdempotencyIdRef>`, the
/// same on 7.4 and 8.0), so an invalid id is the shared empty blob. `CommitTransactionRequest`
/// holds it directly (not in an `Optional`). FDB's loader `ASSERT`s on a length outside
/// the valid range, which kills the process; here [`Decode`] fails with
/// [`DecodeError::Invalid`] and [`IdempotencyIdRef::new`] with [`InvalidIdempotencyId`].
///
/// ```
/// use fdb_native_wire::IdempotencyIdRef;
///
/// let id = IdempotencyIdRef::new(&[7; 16])?;
/// assert!(id.is_valid() && !IdempotencyIdRef::INVALID.is_valid());
/// assert!(IdempotencyIdRef::new(&[7; 15]).is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdempotencyIdRef<'a> {
    bytes: &'a [u8],
}

impl FileIdentified for IdempotencyIdRef<'_> {
    const FILE_IDENTIFIER: FileIdentifier = FileIdentifier::new(3858470);
}

impl<'a> IdempotencyIdRef<'a> {
    /// The shortest valid id.
    pub const MIN_LEN: usize = 16;
    /// The longest valid id.
    pub const MAX_LEN: usize = 255;
    /// The invalid (empty) id, `IdempotencyIdRef()`.
    pub const INVALID: IdempotencyIdRef<'static> = IdempotencyIdRef { bytes: &[] };

    /// An id of these bytes, which must be empty (the invalid id) or 16 to 255 long.
    pub fn new(bytes: &'a [u8]) -> Result<IdempotencyIdRef<'a>, InvalidIdempotencyId> {
        if bytes.is_empty() || (Self::MIN_LEN..=Self::MAX_LEN).contains(&bytes.len()) {
            Ok(IdempotencyIdRef { bytes })
        } else {
            Err(InvalidIdempotencyId { len: bytes.len() })
        }
    }

    /// Whether this is a real id (not the empty invalid id).
    pub fn is_valid(&self) -> bool {
        !self.bytes.is_empty()
    }

    /// The id's bytes; empty for the invalid id.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Copies the id.
    pub fn into_owned(self) -> IdempotencyId {
        IdempotencyId {
            bytes: self.bytes.to_vec(),
        }
    }
}

impl Wire for IdempotencyIdRef<'_> {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for IdempotencyIdRef<'_> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.blob(self.bytes)
    }
}

impl<'de: 'a, 'a> Decode<'de> for IdempotencyIdRef<'a> {
    fn absent() -> Self {
        IdempotencyIdRef::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        IdempotencyIdRef::new(r.blob(at)?)
            .map_err(|_| DecodeError::Invalid("an idempotency id must be empty or 16 to 255 bytes"))
    }
}

/// FDB's `IdempotencyId` (`Standalone<IdempotencyIdRef>`): an owned id.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdempotencyId {
    bytes: Vec<u8>,
}

impl IdempotencyId {
    /// An id of these bytes, which must be empty (the invalid id) or 16 to 255 long.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<IdempotencyId, InvalidIdempotencyId> {
        let bytes = bytes.into();
        IdempotencyIdRef::new(&bytes)?;
        Ok(IdempotencyId { bytes })
    }

    /// Borrows the id.
    pub fn borrowed(&self) -> IdempotencyIdRef<'_> {
        IdempotencyIdRef { bytes: &self.bytes }
    }

    /// The id's bytes; empty for the invalid id.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl FileIdentified for IdempotencyId {
    const FILE_IDENTIFIER: FileIdentifier = FileIdentifier::new(3858470);
}

impl Wire for IdempotencyId {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for IdempotencyId {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        self.borrowed().encode_object(w)
    }
}

impl<'de> Decode<'de> for IdempotencyId {
    fn absent() -> Self {
        IdempotencyId::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        IdempotencyIdRef::decode_object(r, at).map(IdempotencyIdRef::into_owned)
    }
}
