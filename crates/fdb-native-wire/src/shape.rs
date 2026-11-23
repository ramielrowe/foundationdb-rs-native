//! Wire shapes and the layout arithmetic shared by the writer and the reader.

/// How a type is stored when it appears as a table field, a vector element or a union
/// alternative.
///
/// The shape decides how many vtable slots a field takes, how big and how aligned it is
/// inside its table, and how the reader reaches it. It is the only type-level information
/// the codec needs; every [`Encode`](crate::Encode) and [`Decode`](crate::Decode) impl
/// states it through [`Wire::SHAPE`](crate::Wire::SHAPE).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Shape {
    /// Stored inline: a scalar (`bool`, integers, floats, [`Uid`](crate::Uid)) or a struct
    /// (`std::tuple` in FDB, a Rust tuple here). One slot. `size` is at least 1.
    Inline {
        /// Byte size on the wire.
        size: usize,
        /// Alignment inside a table or vector: 1, 2, 4 or 8.
        align: usize,
    },
    /// A table, reached through a `u32` offset. One slot.
    Table,
    /// A string, blob or vector, reached through a `u32` offset to its `u32` length
    /// prefix. One slot.
    Object,
    /// A union (`Optional`, `std::variant`, `ErrorOr`): a `u8` tag slot followed by a
    /// `u32` offset slot. Two slots.
    Union,
    /// A vector of unions: an offset to a `vector<u8>` of tags followed by an offset to a
    /// vector of offsets. Two slots.
    UnionVector,
}

impl Shape {
    /// The shape of a scalar of `size` bytes, aligned like FDB aligns it (to `size`
    /// rounded up to 1, 2, 4 or 8).
    pub const fn scalar(size: usize) -> Shape {
        Shape::Inline {
            size,
            align: align_to_power_of_two(size),
        }
    }

    /// Number of vtable slots a field of this shape takes.
    pub const fn slots(self) -> usize {
        match self {
            Shape::Union | Shape::UnionVector => 2,
            _ => 1,
        }
    }

    /// Whether this is [`Shape::Union`].
    pub const fn is_union(self) -> bool {
        matches!(self, Shape::Union)
    }

    /// Whether this is [`Shape::Table`].
    pub const fn is_table(self) -> bool {
        matches!(self, Shape::Table)
    }

    /// The shape of a struct (FDB `std::tuple`) with the given member shapes.
    ///
    /// Members are laid out in order, each aligned to its own size; the struct is aligned
    /// to its largest member alignment and its size is rounded up to that (minimum 1).
    ///
    /// # Panics
    ///
    /// Panics if a member is not [`Shape::Inline`] (FDB does not support nested structs
    /// or non-scalar struct members). Use it in a constant so the panic is a compile
    /// error.
    pub const fn of_struct(members: &[Shape]) -> Shape {
        let mut end = 0;
        let mut align = 1;
        let mut i = 0;
        while i < members.len() {
            let Shape::Inline { size, align: a } = members[i] else {
                panic!("struct members must be inline scalars");
            };
            end = right_align(end, size) + size;
            if a > align {
                align = a;
            }
            i += 1;
        }
        let size = right_align(end, align);
        Shape::Inline {
            size: if size == 0 { 1 } else { size },
            align,
        }
    }

    /// Byte offset of member `index` inside a struct with the given member shapes (see
    /// [`Shape::of_struct`]). Non-inline members count as zero-sized; an out-of-range
    /// `index` gives the end of the last member.
    pub const fn struct_member_offset(members: &[Shape], index: usize) -> usize {
        let mut end = 0;
        let mut i = 0;
        while i < members.len() {
            let size = match members[i] {
                Shape::Inline { size, .. } => size,
                _ => 0,
            };
            let offset = right_align(end, size);
            if i == index {
                return offset;
            }
            end = offset + size;
            i += 1;
        }
        end
    }

    /// Byte size of a field of this shape inside a table (the first slot's for a union).
    pub(crate) const fn field_size(self) -> usize {
        match self {
            Shape::Inline { size, .. } => size,
            Shape::Union => 1,
            _ => 4,
        }
    }
}

/// `AlignToPowerOfTwo` from `flat_buffers.h`.
pub(crate) const fn align_to_power_of_two(size: usize) -> usize {
    if size > 4 {
        8
    } else if size > 2 {
        4
    } else if size > 1 {
        2
    } else {
        1
    }
}

/// `RightAlign` from `flat_buffers.h`: `offset` rounded up to a multiple of `alignment`.
pub(crate) const fn right_align(offset: usize, alignment: usize) -> usize {
    if alignment == 0 || offset % alignment == 0 {
        offset
    } else {
        (offset / alignment + 1) * alignment
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const I16: Shape = Shape::scalar(2);
    const BOOL: Shape = Shape::scalar(1);
    const I64: Shape = Shape::scalar(8);
    const U8: Shape = Shape::scalar(1);
    const U32: Shape = Shape::scalar(4);

    #[test]
    fn struct_layout_matches_flat_buffers_h() {
        // static_asserts from flat_buffers.h.
        assert_eq!(Shape::struct_member_offset(&[U32], 0), 0);
        assert_eq!(Shape::struct_member_offset(&[U32, BOOL], 1), 4);
        assert_eq!(Shape::struct_member_offset(&[U32, BOOL, I64], 2), 8);
        assert_eq!(Shape::of_struct(&[]), Shape::Inline { size: 1, align: 1 });
        assert_eq!(
            Shape::of_struct(&[U32]),
            Shape::Inline { size: 4, align: 4 }
        );
        assert_eq!(
            Shape::of_struct(&[U32, BOOL]),
            Shape::Inline { size: 8, align: 4 }
        );
        assert_eq!(
            Shape::of_struct(&[U32, BOOL, I64]),
            Shape::Inline { size: 16, align: 8 }
        );
        // README examples.
        assert_eq!(
            Shape::of_struct(&[I16, BOOL, I64]),
            Shape::Inline { size: 16, align: 8 }
        );
        assert_eq!(Shape::struct_member_offset(&[I16, BOOL, I64], 1), 2);
        assert_eq!(
            Shape::of_struct(&[U8, U32]),
            Shape::Inline { size: 8, align: 4 }
        );
        assert_eq!(Shape::struct_member_offset(&[U8, U32], 1), 4);
    }

    #[test]
    fn scalar_alignment() {
        assert_eq!(Shape::scalar(16), Shape::Inline { size: 16, align: 8 });
        assert_eq!(Shape::scalar(3), Shape::Inline { size: 3, align: 4 });
    }
}
