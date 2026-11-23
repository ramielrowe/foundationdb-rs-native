//! Error type for the tuple and subspace layers.

/// Errors produced when packing or unpacking tuples, or using a
/// [`Subspace`](crate::Subspace).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The input ended in the middle of an element.
    #[error("unexpected end of tuple data at offset {offset}")]
    UnexpectedEnd {
        /// Byte offset at which more data was expected.
        offset: usize,
    },
    /// A type code that this implementation does not decode.
    #[error("unknown or unsupported tuple type code {code:#04x} at offset {offset}")]
    UnknownTypeCode {
        /// The offending type code.
        code: u8,
        /// Byte offset of the type code.
        offset: usize,
    },
    /// A unicode string element was not valid UTF-8.
    #[error("invalid UTF-8 in string element ending at offset {offset}")]
    InvalidUtf8 {
        /// Byte offset just past the string element.
        offset: usize,
    },
    /// Nested tuples were nested deeper than [`MAX_DEPTH`](crate::MAX_DEPTH).
    #[error("tuple nesting deeper than {}", crate::MAX_DEPTH)]
    NestingTooDeep,
    /// An integer needs more than 255 bytes and cannot be encoded.
    #[error("integer magnitude of {bytes} bytes exceeds the 255 byte limit")]
    IntegerTooLarge {
        /// Size of the magnitude in bytes.
        bytes: usize,
    },
    /// A tuple containing an incomplete versionstamp was packed with plain `pack`.
    #[error("tuple contains an incomplete versionstamp; use pack_with_versionstamp")]
    IncompleteVersionstamp,
    /// `pack_with_versionstamp` was given a tuple with no incomplete versionstamp.
    #[error("tuple contains no incomplete versionstamp")]
    NoIncompleteVersionstamp,
    /// `pack_with_versionstamp` was given more than one incomplete versionstamp.
    #[error("tuple contains more than one incomplete versionstamp")]
    MultipleIncompleteVersionstamps,
    /// The versionstamp position does not fit in the 4-byte offset suffix.
    #[error("versionstamp offset {offset} does not fit in 32 bits")]
    VersionstampOffsetOverflow {
        /// The position that did not fit.
        offset: usize,
    },
    /// A key passed to [`Subspace::unpack`](crate::Subspace::unpack) does not
    /// start with the subspace prefix.
    #[error("key is not within the subspace")]
    KeyNotInSubspace,
}

/// Result alias for this crate.
pub type Result<T> = std::result::Result<T, Error>;
