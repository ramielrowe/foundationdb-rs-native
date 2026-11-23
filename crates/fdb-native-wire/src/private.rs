//! Support code for the code `#[derive(Wire)]` generates. Not public API: it can change
//! in any release.

use crate::{Decode, DecodeError, Encode, EncodeError, Shape};

/// The byte range of member `index` inside a struct with the given member shapes.
fn member_range(shapes: &[Shape], index: usize, size: usize) -> core::ops::Range<usize> {
    let at = Shape::struct_member_offset(shapes, index);
    at..at + size
}

/// Writes member `index` of an inline struct into `out`, the struct's bytes.
pub fn encode_struct_member<T: Encode + ?Sized>(
    out: &mut [u8],
    shapes: &[Shape],
    index: usize,
    value: &T,
) -> Result<(), EncodeError> {
    let Shape::Inline { size, .. } = T::SHAPE else {
        return Err(EncodeError::InvalidShape("struct members must be inline"));
    };
    let member = out
        .get_mut(member_range(shapes, index, size))
        .ok_or(EncodeError::InvalidShape("struct size"))?;
    value.encode_inline(member)
}

/// Reads member `index` of an inline struct from `bytes`, the struct's bytes.
pub fn decode_struct_member<'de, T: Decode<'de>>(
    bytes: &'de [u8],
    shapes: &[Shape],
    index: usize,
) -> Result<T, DecodeError> {
    let Shape::Inline { size, .. } = T::SHAPE else {
        return Err(DecodeError::Invalid("struct members must be inline"));
    };
    let member = bytes
        .get(member_range(shapes, index, size))
        .ok_or(DecodeError::Invalid("struct size"))?;
    T::decode_inline(member)
}
