//! The traits of the binary format, and the macro for enums.

use super::error::{BinaryDecodeError, BinaryEncodeError};
use super::reader::BinaryReader;
use super::writer::BinaryWriter;

/// A type FDB's `BinaryWriter` can write: C++'s `save(ar, value)`.
///
/// Implement it for a C++ struct with `serialize(Ar&) { serializer(ar, a, b, c); }` by
/// writing the fields in `serializer` argument order, each with [`BinaryWriter::write`]
/// (skipping `Arena`, which writes nothing). The format has no framing or alignment, so a
/// struct is just its fields back to back.
///
/// ```
/// use fdb_native_wire::binary::{
///     from_slice, to_vec, BinaryDecodeError, BinaryDeserialize, BinaryEncodeError,
///     BinaryReader, BinarySerialize, BinaryWriter, Versioning,
/// };
///
/// // struct Point { int32_t x; int32_t y; std::string label;
/// //     template <class Ar> void serialize(Ar& ar) { serializer(ar, x, y, label); } };
/// #[derive(Debug, PartialEq)]
/// struct Point {
///     x: i32,
///     y: i32,
///     label: String,
/// }
///
/// impl BinarySerialize for Point {
///     fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
///         w.write(&self.x)?;
///         w.write(&self.y)?;
///         w.write(&self.label)
///     }
/// }
///
/// impl BinaryDeserialize<'_> for Point {
///     fn deserialize(r: &mut BinaryReader<'_>) -> Result<Self, BinaryDecodeError> {
///         Ok(Point { x: r.read()?, y: r.read()?, label: r.read()? })
///     }
/// }
///
/// let p = Point { x: -1, y: 2, label: "a".into() };
/// let bytes = to_vec(&p, Versioning::Unversioned)?;
/// assert_eq!(bytes, [0xff, 0xff, 0xff, 0xff, 2, 0, 0, 0, 1, 0, 0, 0, b'a']);
/// assert_eq!(from_slice::<Point>(&bytes, Versioning::Unversioned)?, p);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait BinarySerialize {
    /// Writes `self` to `w`.
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError>;

    /// Writes `items` back to back, with no count. A hook so that `[u8]` (a `StringRef`,
    /// a `std::string`, a `std::vector<uint8_t>`) is one copy; no other type overrides it.
    #[doc(hidden)]
    fn serialize_elements(items: &[Self], w: &mut BinaryWriter) -> Result<(), BinaryEncodeError>
    where
        Self: Sized,
    {
        for item in items {
            w.write(item)?;
        }
        Ok(())
    }
}

/// A type FDB's `BinaryReader` can read: C++'s `load(ar, value)`.
///
/// Implement it like [`BinarySerialize`]: read the fields in `serializer` order with
/// [`BinaryReader::read`] (not by calling `deserialize` directly, which skips the reader's
/// depth and work limits). `'de` is the lifetime of the input, for types that borrow from
/// it (`&'de [u8]`, `&'de str`).
///
/// An implementation can read the [`BinaryReader::protocol_version`] to pick the layout of
/// the version that wrote the value, as FDB's `serialize` functions do with
/// `ar.protocolVersion().hasXxx()`.
pub trait BinaryDeserialize<'de>: Sized {
    /// Reads a value from `r`.
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError>;

    /// Reads `count` values. The default reserves at most 64 KiB before the first element,
    /// whatever `count` says; `u8` overrides it to copy once, after checking the input has
    /// the bytes.
    #[doc(hidden)]
    fn deserialize_elements(
        r: &mut BinaryReader<'de>,
        count: usize,
    ) -> Result<Vec<Self>, BinaryDecodeError> {
        let mut items = Vec::with_capacity(crate::codec::preallocation::<Self>(count));
        for _ in 0..count {
            items.push(r.read()?);
        }
        Ok(items)
    }
}

/// A type that can be read from input of any lifetime (it owns its data).
pub trait BinaryDeserializeOwned: for<'de> BinaryDeserialize<'de> {}

impl<T> BinaryDeserializeOwned for T where T: for<'de> BinaryDeserialize<'de> {}

/// Implements [`BinarySerialize`] and [`BinaryDeserialize`] for a fieldless enum, as FDB
/// does for any `enum` (`Serializer<Archive, T, enable_if_t<is_enum_v<T>>>`): the value of
/// the enum's underlying type, little-endian.
///
/// The enum is listed with its values; reading any other value is
/// [`BinaryDecodeError::InvalidEnum`] (C++ would take the value as is).
///
/// ```
/// use fdb_native_wire::binary::{from_slice, to_vec, Versioning};
///
/// // enum class TraceFlags : uint8_t { unsampled = 0, sampled = 1 };
/// #[derive(Clone, Copy, Debug, PartialEq)]
/// enum TraceFlags {
///     Unsampled,
///     Sampled,
/// }
///
/// fdb_native_wire::binary_enum!(TraceFlags: u8 {
///     TraceFlags::Unsampled = 0,
///     TraceFlags::Sampled = 1,
/// });
///
/// let bytes = to_vec(&TraceFlags::Sampled, Versioning::Unversioned)?;
/// assert_eq!(bytes, [1]);
/// assert_eq!(from_slice::<TraceFlags>(&bytes, Versioning::Unversioned)?, TraceFlags::Sampled);
/// assert!(from_slice::<TraceFlags>(&[2], Versioning::Unversioned).is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[macro_export]
macro_rules! binary_enum {
    ($ty:ty : $repr:ty { $($variant:path = $value:expr),+ $(,)? }) => {
        impl $crate::binary::BinarySerialize for $ty {
            fn serialize(
                &self,
                w: &mut $crate::binary::BinaryWriter,
            ) -> ::core::result::Result<(), $crate::binary::BinaryEncodeError> {
                let value: $repr = match self {
                    $($variant => $value,)+
                };
                w.write(&value)
            }
        }

        impl<'de> $crate::binary::BinaryDeserialize<'de> for $ty {
            fn deserialize(
                r: &mut $crate::binary::BinaryReader<'de>,
            ) -> ::core::result::Result<Self, $crate::binary::BinaryDecodeError> {
                let value: $repr = r.read()?;
                $(
                    if value == $value {
                        return Ok($variant);
                    }
                )+
                Err($crate::binary::BinaryDecodeError::InvalidEnum {
                    ty: stringify!($ty),
                    value: value as u64,
                })
            }
        }
    };
}
