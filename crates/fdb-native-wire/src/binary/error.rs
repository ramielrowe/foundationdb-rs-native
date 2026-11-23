//! Errors of the legacy binary format.

/// Why bytes could not be read as a binary-format value.
///
/// FDB's `BinaryReader` throws `serialization_failed` (a short read) or
/// `incompatible_protocol_version` (an `IncludeVersion` prefix it rejects), asserts on a
/// variant index out of range, and reads other malformed input (an out-of-range `bool`
/// or enum) as garbage. This reader rejects all of it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BinaryDecodeError {
    /// A read needs more bytes than the input has left (`serialization_failed` in C++).
    #[error("read of {needed} bytes at offset {pos} but only {remaining} remain")]
    UnexpectedEof {
        /// Where the read starts.
        pos: usize,
        /// How many bytes it needs.
        needed: u64,
        /// How many bytes the input has left.
        remaining: usize,
    },
    /// Input remains after the value (`BinaryReader::assertEnd`).
    #[error("{0} trailing bytes after the value")]
    TrailingBytes(usize),
    /// A `bool` byte that is neither 0 nor 1.
    #[error("invalid bool byte {0:#04x}")]
    InvalidBool(u8),
    /// A value that is not one of an enum's variants.
    #[error("invalid value {value} for enum {ty}")]
    InvalidEnum {
        /// The enum's name.
        ty: &'static str,
        /// The value read.
        value: u64,
    },
    /// A `std::variant` index at or past the number of alternatives (an `ASSERT` in C++).
    #[error("variant index {index} out of range for {alternatives} alternatives")]
    InvalidVariantIndex {
        /// The index read.
        index: u8,
        /// How many alternatives the variant has.
        alternatives: usize,
    },
    /// An `IncludeVersion` prefix that is not a valid protocol version
    /// (`incompatible_protocol_version` in C++).
    #[error("invalid protocol version {0:#018x}")]
    InvalidProtocolVersion(u64),
    /// An `IncludeVersion` prefix at or above the first version that cannot be downgraded
    /// from (`incompatible_protocol_version` in C++).
    #[error("protocol version {0:#018x} is too new")]
    FutureProtocolVersion(u64),
    /// A string, vector, map or set count above `i32::MAX`. (FDB reads counts as `int`
    /// or `uint32_t` and caps `VectorRef` at 100 MiB; no real value is that large.)
    #[error("count {0} is too large")]
    CountTooLarge(u32),
    /// Values nest more than [`MAX_DEPTH`](crate::binary::MAX_DEPTH) deep.
    #[error("values nest too deeply")]
    TooDeep,
    /// The reader decoded too many values that take no bytes (such as elements of a
    /// `Vec<()>`), which would make decoding time unrelated to the input size.
    #[error("decoding work limit exceeded (too many zero-width values)")]
    WorkLimit,
    /// A string that is not UTF-8, read as a `String` or `&str`. (C++'s `std::string` holds
    /// any bytes; read those as `Vec<u8>` or `&[u8]`.)
    #[error("string is not valid UTF-8")]
    InvalidUtf8,
    /// The bytes are well-formed but not a valid value of the type being decoded.
    #[error("invalid value: {0}")]
    Invalid(&'static str),
}

/// Why a value could not be written in the binary format.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BinaryEncodeError {
    /// A string, vector, map or set with more than `i32::MAX` elements or bytes, or output
    /// that would exceed `i32::MAX` bytes (`BinaryWriter` keeps its length in an `int`).
    #[error("value or output too large")]
    TooLarge,
    /// A protocol version that is not valid (`IncludeVersion` and `AssumeVersion`
    /// assert on one in C++).
    #[error("invalid protocol version {0:#018x}")]
    InvalidProtocolVersion(u64),
}
