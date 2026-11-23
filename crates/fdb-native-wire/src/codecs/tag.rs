//! FDB's `Tag`.

use core::fmt;

use crate::{Decode, DecodeError, Encode, EncodeError, Shape, Wire};

/// FDB's `Tag` (`fdbclient/FDBTypes.h`): which storage server or tLog a mutation or
/// version belongs to.
///
/// On the wire it is a struct (`struct_like_traits<Tag>`), `u16 id` at offset 0 then
/// `i8 locality` at offset 2, 4 bytes aligned to 2. Tags order by locality (signed), then
/// id, which is the order of `VersionVector`'s entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tag {
    /// The locality: the DC of a storage server tag when positive, the kind of tLog set
    /// (`tagLocalityLogRouter` = -2, ...) when negative.
    pub locality: i8,
    /// The id within the locality.
    pub id: u16,
}

impl Tag {
    /// `invalidTag`: locality `tagLocalitySpecial` (-1), id 0; also `Tag()`.
    pub const INVALID: Tag = Tag {
        locality: -1,
        id: 0,
    };

    /// A tag with the given locality and id.
    pub const fn new(locality: i8, id: u16) -> Tag {
        Tag { locality, id }
    }
}

impl Default for Tag {
    fn default() -> Tag {
        Tag::INVALID
    }
}

impl fmt::Display for Tag {
    /// As C++ prints it (`Tag::toString`): `locality:id`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.locality, self.id)
    }
}

const MEMBERS: [Shape; 2] = [<u16 as Wire>::SHAPE, <i8 as Wire>::SHAPE];

impl Wire for Tag {
    const SHAPE: Shape = Shape::of_struct(&MEMBERS);
    const STRUCT: bool = true;
}

impl Encode for Tag {
    fn encode_inline(&self, out: &mut [u8]) -> Result<(), EncodeError> {
        let out: &mut [u8; 4] = out
            .try_into()
            .map_err(|_| EncodeError::InvalidShape("Tag size"))?;
        let [id0, id1] = self.id.to_le_bytes();
        // The padding byte stays zero.
        *out = [id0, id1, self.locality.to_le_bytes()[0], 0];
        Ok(())
    }
}

impl<'de> Decode<'de> for Tag {
    /// A default-constructed `Tag`: `invalidTag`.
    fn absent() -> Self {
        Tag::INVALID
    }

    fn decode_inline(bytes: &'de [u8]) -> Result<Self, DecodeError> {
        match *bytes {
            [id0, id1, locality, _] => Ok(Tag {
                locality: i8::from_le_bytes([locality]),
                id: u16::from_le_bytes([id0, id1]),
            }),
            _ => Err(DecodeError::Invalid("Tag size")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_is_id_then_locality() {
        assert_eq!(Tag::SHAPE, Shape::Inline { size: 4, align: 2 });
        let mut out = [0xaa; 4];
        Tag::new(-2, 300).encode_inline(&mut out).unwrap();
        assert_eq!(out, [0x2c, 0x01, 0xfe, 0x00]);
        assert_eq!(Tag::decode_inline(&out).unwrap(), Tag::new(-2, 300));
        assert!(Tag::decode_inline(&out[..3]).is_err());
    }

    #[test]
    fn orders_by_signed_locality_then_id() {
        let mut tags = [
            Tag::new(1, 2),
            Tag::new(-2, 300),
            Tag::new(1, 1),
            Tag::new(0, 9),
        ];
        tags.sort();
        assert_eq!(
            tags,
            [
                Tag::new(-2, 300),
                Tag::new(0, 9),
                Tag::new(1, 1),
                Tag::new(1, 2)
            ]
        );
    }
}
