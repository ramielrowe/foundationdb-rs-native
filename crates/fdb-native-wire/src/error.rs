//! Encode and decode errors.

use core::fmt;

/// A check the decoder makes beyond FDB `main`'s reader. Each is one of the "permitted
/// strict checks" of `tools/golden-vectors/README.md`, and [`StrictCheck::name`] is its
/// name in that vocabulary.
///
/// The decoder does not implement the other permitted checks: `vtable_alignment` (all
/// loads are byte-wise, so an odd vtable is harmless), `union_tag_range`, `union_slots`
/// and `union_vector_count_mismatch` (it reads those unions the way C++ does: as their
/// default value, or by the offsets vector's count).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StrictCheck {
    /// The root offset, a followed offset or a vtable lands inside the 8-byte header.
    OffsetIntoHeader,
    /// A followed offset (other than the root offset) is 0, so it points at itself.
    ZeroUoffset,
    /// A vtable's size field is below 4 or odd.
    VtableSize,
    /// A table's size field is below 4, the table runs past the buffer, or a present field
    /// extends past the table size.
    TableSize,
    /// A vtable entry is 1, 2 or 3 (inside the table's soffset).
    VtableEntry,
    /// A scalar `bool` byte is neither 0 nor 1.
    BoolValue,
    /// The offset to a zero-field table (such as [`Void`](crate::Void)) leaves no room
    /// for the table's soffset.
    EmptyTableOffset,
    /// Tables nest more than 64 deep.
    TooDeep,
}

impl StrictCheck {
    /// The check's name in the golden-vectors `violations` vocabulary.
    pub const fn name(self) -> &'static str {
        match self {
            StrictCheck::OffsetIntoHeader => "offset_into_header",
            StrictCheck::ZeroUoffset => "zero_uoffset",
            StrictCheck::VtableSize => "vtable_size",
            StrictCheck::TableSize => "table_size",
            StrictCheck::VtableEntry => "vtable_entry",
            StrictCheck::BoolValue => "bool_value",
            StrictCheck::EmptyTableOffset => "empty_table_offset",
            StrictCheck::TooDeep => "too_deep",
        }
    }
}

impl fmt::Display for StrictCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Why a buffer was rejected.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// The buffer is shorter than the 8-byte header.
    #[error("buffer of {len} bytes is too short for the 8-byte header")]
    TooShort {
        /// The buffer length.
        len: usize,
    },
    /// The file identifier at bytes 4..8 is not the expected one.
    #[error("file identifier mismatch: expected {expected:#010x}, found {found:#010x}")]
    FileIdentifierMismatch {
        /// The identifier the caller expected.
        expected: u32,
        /// The identifier in the buffer.
        found: u32,
    },
    /// A read would go outside the buffer.
    #[error("read of {len} bytes at offset {pos} is outside the buffer")]
    OutOfBounds {
        /// Where the read starts.
        pos: usize,
        /// How many bytes it needs.
        len: u64,
    },
    /// A fixed-size array was encoded with a different number of elements.
    #[error("array length mismatch: expected {expected}, found {found}")]
    ArrayLength {
        /// The array's length.
        expected: usize,
        /// The encoded count.
        found: u32,
    },
    /// An element index past the end of a [`VectorView`](crate::VectorView).
    #[error("index {index} is out of range for a vector of {len} elements")]
    IndexOutOfRange {
        /// The index asked for.
        index: usize,
        /// The vector's element count.
        len: usize,
    },
    /// A vector of unions has its tags slot but not its offsets slot.
    #[error("union vector has a tags slot but no offsets slot")]
    UnionVectorMissingOffsets,
    /// A [`StrictCheck`] failed.
    #[error("strict check failed: {0}")]
    Strict(StrictCheck),
    /// Decoding would visit more than 4 × the buffer length + 1024 units of objects (see
    /// [`Reader`](crate::Reader#work-limit)): the message references the same objects
    /// many times over. FDB's writer never produces such messages, but FDB `main`'s reader
    /// accepts them; this is the `work_limit` check of the golden-vectors README.
    #[error("decoding work limit exceeded (objects are referenced too many times)")]
    WorkLimit,
    /// The bytes are well-formed but not a valid value of the type being decoded (for
    /// example a blob codec's payload is inconsistent), or the type cannot be decoded in
    /// this position.
    #[error("invalid value: {0}")]
    Invalid(&'static str),
}

impl DecodeError {
    /// The permitted strict check (in the golden-vectors `violations` vocabulary) that
    /// this error corresponds to, if it is one. Out-of-bounds reads map to
    /// `out_of_bounds`, which C++ also rejects except for offsets it never follows, and
    /// [`DecodeError::WorkLimit`] to `work_limit`.
    pub fn check_name(&self) -> Option<&'static str> {
        match self {
            DecodeError::Strict(check) => Some(check.name()),
            DecodeError::OutOfBounds { .. } => Some("out_of_bounds"),
            DecodeError::WorkLimit => Some("work_limit"),
            _ => None,
        }
    }
}

/// Why a value could not be encoded.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EncodeError {
    /// The message would be larger than 2<sup>31</sup> − 1 bytes, or a table larger than
    /// a vtable can describe.
    #[error("message too large")]
    TooLarge,
    /// A union tag of 0 was given for a present alternative.
    #[error("union tag 0 is reserved for an empty union")]
    InvalidUnionTag,
    /// A type was used where its [`Shape`](crate::Shape) is not allowed (for example a
    /// union as a union alternative), or an impl does not match its shape.
    #[error("invalid shape: {0}")]
    InvalidShape(&'static str),
    /// An [`ObjectRef`](crate::ObjectRef) does not belong to this writer's current message.
    #[error("object reference does not belong to this message")]
    ForeignRef,
}
