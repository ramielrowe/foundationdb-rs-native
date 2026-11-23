//! `BinarySerialize` and `BinaryDeserialize` for the types `serialize.h` handles.

use std::collections::{BTreeMap, BTreeSet};

use super::error::{BinaryDecodeError, BinaryEncodeError};
use super::reader::BinaryReader;
use super::traits::{BinaryDeserialize, BinarySerialize};
use super::writer::BinaryWriter;
use crate::{FdbError, ProtocolVersion, Uid};

// --- scalars ----------------------------------------------------------------------------

macro_rules! scalar {
    ($($ty:ty),+) => {$(
        impl BinarySerialize for $ty {
            fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
                w.write_bytes(&self.to_le_bytes())
            }
        }

        impl<'de> BinaryDeserialize<'de> for $ty {
            fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
                r.read_array().map(<$ty>::from_le_bytes)
            }
        }
    )+};
}

// `BINARY_SERIALIZABLE` in serialize.h: the fixed-width integers, `bool`, `double` and
// `ProtocolVersion`. (`float` is not.) Written as the raw little-endian memory.
scalar!(i8, u16, i16, u32, i32, u64, i64, f64);

impl BinarySerialize for u8 {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write_u8(*self)
    }

    fn serialize_elements(items: &[u8], w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write_bytes(items)
    }
}

impl<'de> BinaryDeserialize<'de> for u8 {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        r.read_u8()
    }

    fn deserialize_elements(
        r: &mut BinaryReader<'de>,
        count: usize,
    ) -> Result<Vec<u8>, BinaryDecodeError> {
        // The bytes must be there before anything is allocated.
        r.read_bytes(count).map(<[u8]>::to_vec)
    }
}

impl BinarySerialize for bool {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write_u8(u8::from(*self))
    }
}

impl<'de> BinaryDeserialize<'de> for bool {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        match r.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            b => Err(BinaryDecodeError::InvalidBool(b)),
        }
    }
}

/// `ProtocolVersion` is serialized as `versionWithFlags()`, a `uint64_t`.
impl BinarySerialize for ProtocolVersion {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write_u64(self.0)
    }
}

impl<'de> BinaryDeserialize<'de> for ProtocolVersion {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        r.read_u64().map(ProtocolVersion)
    }
}

// --- flow types -------------------------------------------------------------------------

/// `UID::serialize_unversioned`: `serializer(ar, part[0], part[1])`, 16 bytes. (Key
/// definitions depend on it, so it never changes.)
impl BinarySerialize for Uid {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write_u64(self.first)?;
        w.write_u64(self.second)
    }
}

impl<'de> BinaryDeserialize<'de> for Uid {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        Ok(Uid::new(r.read_u64()?, r.read_u64()?))
    }
}

/// `Error::serialize`: `serializer(ar, error_code)`, a `uint16_t`.
impl BinarySerialize for FdbError {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write_u16(self.code)
    }
}

impl<'de> BinaryDeserialize<'de> for FdbError {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        r.read_u16().map(FdbError::new)
    }
}

// --- Optional, wrappers -----------------------------------------------------------------

/// `Optional<T>` (`Arena.h`): a `bool` for present, then the value if it is.
impl<T: BinarySerialize> BinarySerialize for Option<T> {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        match self {
            Some(value) => {
                w.write(&true)?;
                w.write(value)
            }
            None => w.write(&false),
        }
    }
}

impl<'de, T: BinaryDeserialize<'de>> BinaryDeserialize<'de> for Option<T> {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        if r.read::<bool>()? {
            r.read().map(Some)
        } else {
            Ok(None)
        }
    }
}

impl<T: BinarySerialize + ?Sized> BinarySerialize for &T {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        (**self).serialize(w)
    }
}

impl<T: BinarySerialize + ?Sized> BinarySerialize for Box<T> {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        (**self).serialize(w)
    }
}

impl<'de, T: BinaryDeserialize<'de>> BinaryDeserialize<'de> for Box<T> {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        r.read().map(Box::new)
    }
}

// --- strings and sequences --------------------------------------------------------------

/// A string: `StringRef` and `Standalone<StringRef>` (`uint32_t` length), and
/// `std::string` (`int32_t` length), then the bytes. The two lengths are the same bytes for
/// every string under 2 GiB, the only ones this crate reads or writes.
impl BinarySerialize for str {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write_count(self.len())?;
        w.write_bytes(self.as_bytes())
    }
}

impl BinarySerialize for String {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        self.as_str().serialize(w)
    }
}

impl<'de> BinaryDeserialize<'de> for String {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        <&str>::deserialize(r).map(str::to_owned)
    }
}

impl<'de> BinaryDeserialize<'de> for &'de str {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        let count = r.read_count()?;
        std::str::from_utf8(r.read_bytes(count)?).map_err(|_| BinaryDecodeError::InvalidUtf8)
    }
}

impl<'de> BinaryDeserialize<'de> for &'de [u8] {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        let count = r.read_count()?;
        r.read_bytes(count)
    }
}

/// A sequence: `std::vector`, `VectorRef` (a count, then the elements) and,
/// for `[u8]`, a `StringRef` or `std::string`.
impl<T: BinarySerialize> BinarySerialize for [T] {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write_count(self.len())?;
        T::serialize_elements(self, w)
    }
}

impl<T: BinarySerialize> BinarySerialize for Vec<T> {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        self.as_slice().serialize(w)
    }
}

impl<'de, T: BinaryDeserialize<'de>> BinaryDeserialize<'de> for Vec<T> {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        let count = r.read_count()?;
        T::deserialize_elements(r, count)
    }
}

/// `std::array<T, N>`: the elements, with no count.
impl<T: BinarySerialize, const N: usize> BinarySerialize for [T; N] {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        T::serialize_elements(self, w)
    }
}

impl<'de, T: BinaryDeserialize<'de>, const N: usize> BinaryDeserialize<'de> for [T; N] {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        T::deserialize_elements(r, N)?
            .try_into()
            .map_err(|_| BinaryDecodeError::Invalid("array length"))
    }
}

// --- maps and sets ----------------------------------------------------------------------

/// `std::set<T>`: a count, then the elements in order. A reader keeps the first of equal
/// elements and accepts any order, as C++ does.
impl<T: BinarySerialize> BinarySerialize for BTreeSet<T> {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write_count(self.len())?;
        for item in self {
            w.write(item)?;
        }
        Ok(())
    }
}

impl<'de, T: BinaryDeserialize<'de> + Ord> BinaryDeserialize<'de> for BTreeSet<T> {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        let count = r.read_count()?;
        let mut set = BTreeSet::new();
        for _ in 0..count {
            set.insert(r.read()?);
        }
        Ok(set)
    }
}

/// `std::map<K, V>` (and `boost::container::flat_map<K, V>`): a count, then each key and
/// its value (not wrapped in a pair; the bytes are the same), in key order. A reader
/// keeps the first of equal keys and accepts any order, as C++ does.
impl<K: BinarySerialize, V: BinarySerialize> BinarySerialize for BTreeMap<K, V> {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write_count(self.len())?;
        for (key, value) in self {
            w.write(key)?;
            w.write(value)?;
        }
        Ok(())
    }
}

impl<'de, K: BinaryDeserialize<'de> + Ord, V: BinaryDeserialize<'de>> BinaryDeserialize<'de>
    for BTreeMap<K, V>
{
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        let count = r.read_count()?;
        let mut map = BTreeMap::new();
        for _ in 0..count {
            let key = r.read()?;
            let value = r.read()?;
            map.entry(key).or_insert(value);
        }
        Ok(map)
    }
}

// --- pairs and tuples -------------------------------------------------------------------

/// `std::pair`, and the Rust model of a struct whose `serialize` is
/// `serializer(ar, a, b, ...)`: the members back to back.
macro_rules! tuple {
    ($(($($name:ident),+))+) => {$(
        #[allow(non_snake_case)]
        impl<$($name: BinarySerialize),+> BinarySerialize for ($($name,)+) {
            fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
                let ($($name,)+) = self;
                $(w.write($name)?;)+
                Ok(())
            }
        }

        impl<'de, $($name: BinaryDeserialize<'de>),+> BinaryDeserialize<'de> for ($($name,)+) {
            fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
                Ok(($(r.read::<$name>()?,)+))
            }
        }
    )+};
}

tuple! {
    (A, B)
    (A, B, C)
    (A, B, C, D)
    (A, B, C, D, E)
    (A, B, C, D, E, F)
}
