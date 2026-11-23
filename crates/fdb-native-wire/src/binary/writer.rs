//! `BinaryWriter`.

use super::error::EncodeError;
use super::traits::Encode;
use super::version::Versioning;
use crate::ProtocolVersion;

/// Largest output, in bytes: `BinaryWriter` keeps its size in an `int`.
const MAX_LEN: usize = i32::MAX as usize;

/// FDB's `BinaryWriter` (`flow/include/flow/serialize.h`): appends values to a byte buffer in
/// the legacy binary format.
///
/// Scalars are little-endian and unaligned, there is no padding, and containers carry a
/// 4-byte count; see the [module docs](crate::binary) for the whole format.
///
/// ```
/// use fdb_native_wire::binary::{Writer, Versioning};
///
/// let mut w = Writer::new(Versioning::Unversioned)?;
/// w.write(&7u16)?;
/// w.write(&Some("hi"))?;
/// assert_eq!(w.into_bytes(), [7, 0, 1, 2, 0, 0, 0, b'h', b'i']);
/// # Ok::<(), fdb_native_wire::binary::EncodeError>(())
/// ```
#[derive(Clone, Debug)]
pub struct Writer {
    buf: Vec<u8>,
    version: Option<ProtocolVersion>,
}

impl Writer {
    /// A writer with the given version option. `Include(v)` writes `v` as the first 8
    /// bytes. Both `Include` and `Assume` need a [valid](ProtocolVersion::is_valid)
    /// version, as in C++.
    pub fn new(versioning: Versioning) -> Result<Writer, EncodeError> {
        let mut w = Writer {
            buf: Vec::new(),
            version: None,
        };
        match versioning {
            Versioning::Unversioned => {}
            Versioning::Include(v) => {
                w.set_version(v)?;
                w.write_u64(v.0)?;
            }
            Versioning::Assume(v) => w.set_version(v)?,
        }
        Ok(w)
    }

    /// `BinaryWriter(Unversioned())`.
    pub fn unversioned() -> Writer {
        Writer {
            buf: Vec::new(),
            version: None,
        }
    }

    /// `BinaryWriter(IncludeVersion(version))`.
    pub fn include_version(version: ProtocolVersion) -> Result<Writer, EncodeError> {
        Writer::new(Versioning::Include(version))
    }

    /// `BinaryWriter(AssumeVersion(version))`.
    pub fn assume_version(version: ProtocolVersion) -> Result<Writer, EncodeError> {
        Writer::new(Versioning::Assume(version))
    }

    fn set_version(&mut self, v: ProtocolVersion) -> Result<(), EncodeError> {
        if !v.is_valid() {
            return Err(EncodeError::InvalidProtocolVersion(v.0));
        }
        self.version = Some(v);
        Ok(())
    }

    /// The archive's protocol version (`protocolVersion()`), for values whose layout depends
    /// on it; `None` when [`Unversioned`](Versioning::Unversioned).
    pub fn protocol_version(&self) -> Option<ProtocolVersion> {
        self.version
    }

    /// Serializes `value` (`ar << value`).
    pub fn write<T: Encode + ?Sized>(&mut self, value: &T) -> Result<(), EncodeError> {
        value.serialize(self)
    }

    /// Appends raw bytes (`serializeBytes`). Use it for a prefix that is not a
    /// serialized value (a key's literal prefix) and for a flatbuffers payload after an
    /// `IncludeVersion` prefix with the object-serializer flag.
    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        if bytes.len() > MAX_LEN - self.buf.len() {
            return Err(EncodeError::TooLarge);
        }
        self.buf.extend_from_slice(bytes);
        Ok(())
    }

    /// Appends one byte.
    pub fn write_u8(&mut self, v: u8) -> Result<(), EncodeError> {
        self.write_bytes(&[v])
    }

    /// Appends a little-endian `u16`.
    pub fn write_u16(&mut self, v: u16) -> Result<(), EncodeError> {
        self.write_bytes(&v.to_le_bytes())
    }

    /// Appends a little-endian `u32`.
    pub fn write_u32(&mut self, v: u32) -> Result<(), EncodeError> {
        self.write_bytes(&v.to_le_bytes())
    }

    /// Appends a little-endian `u64`.
    pub fn write_u64(&mut self, v: u64) -> Result<(), EncodeError> {
        self.write_bytes(&v.to_le_bytes())
    }

    /// Appends the 4-byte count of a string, vector, map or set. A count above `i32::MAX` is
    /// an error (C++ writes the size as an `int`).
    pub fn write_count(&mut self, count: usize) -> Result<(), EncodeError> {
        match i32::try_from(count) {
            Ok(count) => self.write_bytes(&count.to_le_bytes()),
            Err(_) => Err(EncodeError::TooLarge),
        }
    }

    /// Appends the index of a `std::variant` alternative (a `uint8_t`); the alternative's
    /// value follows. See [`Reader::read_variant_index`](crate::binary::Reader::read_variant_index).
    pub fn write_variant_index(&mut self, index: u8) -> Result<(), EncodeError> {
        self.write_u8(index)
    }

    /// The bytes written so far.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// How many bytes have been written (`getLength()`).
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether nothing has been written.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The finished output (`toValue()`).
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}

/// `BinaryWriter::toValue(value, versioning)`: serializes one value into a new buffer.
pub fn to_vec<T: Encode + ?Sized>(
    value: &T,
    versioning: Versioning,
) -> Result<Vec<u8>, EncodeError> {
    let mut w = Writer::new(versioning)?;
    w.write(value)?;
    Ok(w.into_bytes())
}
