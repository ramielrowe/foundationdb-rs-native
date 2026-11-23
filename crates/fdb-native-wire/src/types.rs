//! Wire types from flow: `UID`, `Error`, `ErrorOr`, `Void`, `EnsureTable`.

use crate::{
    Decode, DecodeError, Encode, EncodeError, FileIdentified, FileIdentifier, ObjectRef, Reader,
    Shape, UnionRef, Wire, Wrapper, Writer,
};

/// FDB's `UID`: a 16-byte scalar, `first` then `second`, little-endian, aligned to 8.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Uid {
    /// The first half.
    pub first: u64,
    /// The second half.
    pub second: u64,
}

impl Uid {
    /// A UID from its two halves.
    pub const fn new(first: u64, second: u64) -> Uid {
        Uid { first, second }
    }
}

impl Wire for Uid {
    const SHAPE: Shape = Shape::Inline { size: 16, align: 8 };
}

impl Encode for Uid {
    fn encode_inline(&self, out: &mut [u8]) -> Result<(), EncodeError> {
        let (first, second) = match out.split_at_mut_checked(8) {
            Some((first, second)) if second.len() == 8 => (first, second),
            _ => return Err(EncodeError::InvalidShape("UID size")),
        };
        first.copy_from_slice(&self.first.to_le_bytes());
        second.copy_from_slice(&self.second.to_le_bytes());
        Ok(())
    }
}

impl<'de> Decode<'de> for Uid {
    fn absent() -> Self {
        Uid::default()
    }

    fn decode_inline(bytes: &'de [u8]) -> Result<Self, DecodeError> {
        let (first, second) = (bytes.get(..8), bytes.get(8..));
        match (
            first.and_then(|b| b.try_into().ok()),
            second.and_then(|b| b.try_into().ok()),
        ) {
            (Some(first), Some(second)) => Ok(Uid::new(
                u64::from_le_bytes(first),
                u64::from_le_bytes(second),
            )),
            _ => Err(DecodeError::Invalid("UID size")),
        }
    }
}

/// FDB's `Error` as it travels in an [`ErrorOr`]: a table with one `u16` field,
/// `error_code`. (The flags are not serialized.)
///
/// A missing `ErrorOr` reads as error [`FdbError::DEFAULT_ERROR_OR`]; a missing bare
/// `Error` as [`FdbError::INVALID`]; an `Error` table whose `error_code` slot is missing
/// as code 0, all as in C++.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FdbError {
    /// The error code.
    pub code: u16,
}

impl FdbError {
    /// `default_error_or` (1035): what an absent or malformed `ErrorOr` decodes to.
    pub const DEFAULT_ERROR_OR: FdbError = FdbError { code: 1035 };
    /// `invalid_error_code` (65535): a default-constructed `Error`.
    pub const INVALID: FdbError = FdbError { code: 0xffff };

    /// An error with the given code.
    pub const fn new(code: u16) -> FdbError {
        FdbError { code }
    }
}

impl FileIdentified for FdbError {
    const FILE_IDENTIFIER: FileIdentifier = FileIdentifier::new(14065384);
}

impl Wire for FdbError {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for FdbError {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.code)?;
        t.finish()
    }
}

impl<'de> Decode<'de> for FdbError {
    fn absent() -> Self {
        FdbError::INVALID
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(FdbError { code: t.field()? })
    }
}

/// FDB's `ErrorOr<T>`: a union of `Error` (tag 1) and `T` (tag 2). Replies travel as
/// `ErrorOr<EnsureTable<Reply>>`.
///
/// An absent value, a zero tag or an unknown tag reads as
/// `Err(FdbError::DEFAULT_ERROR_OR)`, as in C++. Its file identifier composes `T`'s
/// with [`Wrapper::ErrorOr`].
pub type ErrorOr<T> = Result<T, FdbError>;

impl<T: Wire> Wire for ErrorOr<T> {
    const SHAPE: Shape = {
        assert!(
            T::SHAPE.is_plain(),
            "a union alternative cannot be a union; use EnsureTable"
        );
        Shape::Union
    };
}

impl<T: Encode> Encode for ErrorOr<T> {
    fn encode_union(&self, w: &mut Writer) -> Result<UnionRef, EncodeError> {
        match self {
            Err(error) => w.union_value(1, error),
            Ok(value) => w.union_value(2, value),
        }
    }
}

impl<'de, T: Decode<'de>> Decode<'de> for ErrorOr<T> {
    fn absent() -> Self {
        Err(FdbError::DEFAULT_ERROR_OR)
    }

    fn decode_union(r: &Reader<'de>, tag: u8, at: usize) -> Result<Self, DecodeError> {
        match tag {
            1 => r.alternative(at).map(Err),
            2 => r.alternative(at).map(Ok),
            _ => Ok(Self::absent()),
        }
    }
}

impl<T: FileIdentified> FileIdentified for ErrorOr<T> {
    const FILE_IDENTIFIER: FileIdentifier = T::FILE_IDENTIFIER.compose(Wrapper::ErrorOr);
}

/// FDB's `Void`: a table with no fields (vtable `{4, 4}`, the table is only its soffset).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Void;

impl FileIdentified for Void {
    const FILE_IDENTIFIER: FileIdentifier = FileIdentifier::new(2010442);
}

impl Wire for Void {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for Void {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.table().finish()
    }
}

impl<'de> Decode<'de> for Void {
    fn absent() -> Self {
        Void
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        r.empty_table(at).map(|()| Void)
    }
}

/// FDB's `EnsureTable<T>`: `T` itself if `T` is a table, otherwise `T` wrapped in a
/// one-member table (the same shape as the root table). It lets a union hold a
/// non-table, in particular another union. It has `T`'s file identifier.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EnsureTable<T>(pub T);

impl<T: Wire> Wire for EnsureTable<T> {
    const SHAPE: Shape = Shape::Table;
}

impl<T: Encode> Encode for EnsureTable<T> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        if T::SHAPE.is_table() {
            self.0.encode_object(w)
        } else {
            let mut t = w.table();
            t.field(&self.0)?;
            t.finish()
        }
    }
}

impl<'de, T: Decode<'de>> Decode<'de> for EnsureTable<T> {
    fn absent() -> Self {
        EnsureTable(T::absent())
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        if T::SHAPE.is_table() {
            T::decode_object(r, at).map(EnsureTable)
        } else {
            r.table(at)?.field().map(EnsureTable)
        }
    }
}

impl<T: FileIdentified> FileIdentified for EnsureTable<T> {
    const FILE_IDENTIFIER: FileIdentifier = T::FILE_IDENTIFIER.ensure_table();
}
