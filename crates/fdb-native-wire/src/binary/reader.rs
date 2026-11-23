//! `BinaryReader`.

use super::error::DecodeError;
use super::traits::Decode;
use super::version::ReadVersioning;
use crate::{MAX_DEPTH, ProtocolVersion};

/// Zero-width values (`()`, [`Void`](crate::Void), a struct with no fields) a reader decodes
/// per input byte, on top of [`ZERO_WIDTH_SLACK`]. The budget is what keeps `Vec<()>` with
/// a count of 2<sup>31</sup> − 1 from running for minutes on a 4-byte input.
const ZERO_WIDTH_PER_BYTE: usize = 4;
/// See [`ZERO_WIDTH_PER_BYTE`].
const ZERO_WIDTH_SLACK: usize = 1024;

/// FDB's `BinaryReader` (`flow/include/flow/serialize.h`): reads values in the legacy binary
/// format from a byte slice, borrowing from it.
///
/// Every read is bounds-checked and returns an error instead of panicking. Container
/// counts are never trusted for allocation: a `Vec` reserves at most 64 KiB up front and
/// grows as elements decode, and reading a count that the input cannot satisfy fails at the
/// first element that is missing. [`read`](Reader::read) bounds nesting
/// ([`MAX_DEPTH`], the limit of the flatbuffers reader too) and the number of zero-width
/// values.
///
/// ```
/// use fdb_native_wire::binary::{ReadVersioning, Reader};
///
/// let mut r = Reader::new(&[7, 0, 1, 2, 0, 0, 0, b'h', b'i'], ReadVersioning::Unversioned)?;
/// assert_eq!(r.read::<u16>()?, 7);
/// assert_eq!(r.read::<Option<&str>>()?, Some("hi"));
/// r.finish()?;
/// # Ok::<(), fdb_native_wire::binary::DecodeError>(())
/// ```
#[derive(Clone, Debug)]
pub struct Reader<'de> {
    data: &'de [u8],
    pos: usize,
    version: Option<ProtocolVersion>,
    depth: usize,
    zero_width_budget: usize,
}

impl<'de> Reader<'de> {
    /// A reader over `data` with the given version option.
    ///
    /// `Include` reads and checks the 8-byte version prefix like FDB's `IncludeVersion`: it
    /// must be [valid](ProtocolVersion::is_valid) and, flags aside, below the limit the
    /// option names (an `IncludeVersion` reader of a release refuses that release's
    /// `minInvalidProtocolVersion` and newer). `Assume` must be a valid version, as in C++.
    pub fn new(data: &'de [u8], versioning: ReadVersioning) -> Result<Self, DecodeError> {
        let mut r = Reader::unversioned(data);
        match versioning {
            ReadVersioning::Unversioned => {}
            ReadVersioning::Include { min_invalid } => r.read_version_prefix(min_invalid)?,
            ReadVersioning::Assume(v) => {
                if !v.is_valid() {
                    return Err(DecodeError::InvalidProtocolVersion(v.0));
                }
                r.version = Some(v);
            }
        }
        Ok(r)
    }

    /// `BinaryReader(data, Unversioned())`.
    pub fn unversioned(data: &'de [u8]) -> Self {
        Reader {
            data,
            pos: 0,
            version: None,
            depth: 0,
            zero_width_budget: data
                .len()
                .saturating_mul(ZERO_WIDTH_PER_BYTE)
                .saturating_add(ZERO_WIDTH_SLACK),
        }
    }

    /// `BinaryReader(data, IncludeVersion())` for a release whose
    /// `minInvalidProtocolVersion` is `min_invalid` ([`ProtocolVersion::MIN_INVALID_7_4`] or
    /// [`ProtocolVersion::MIN_INVALID_8_0`]): a version at or above it is
    /// [`FutureProtocolVersion`](DecodeError::FutureProtocolVersion).
    pub fn include_version(
        data: &'de [u8],
        min_invalid: ProtocolVersion,
    ) -> Result<Self, DecodeError> {
        Reader::new(data, ReadVersioning::Include { min_invalid })
    }

    /// `BinaryReader(data, AssumeVersion(version))`.
    pub fn assume_version(data: &'de [u8], version: ProtocolVersion) -> Result<Self, DecodeError> {
        Reader::new(data, ReadVersioning::Assume(version))
    }

    /// `_IncludeVersion::read`.
    fn read_version_prefix(&mut self, min_invalid: ProtocolVersion) -> Result<(), DecodeError> {
        let v = ProtocolVersion(self.read_u64()?);
        if !v.is_valid() {
            return Err(DecodeError::InvalidProtocolVersion(v.0));
        }
        if v.version() >= min_invalid.version() {
            return Err(DecodeError::FutureProtocolVersion(v.0));
        }
        self.version = Some(v);
        Ok(())
    }

    /// The archive's protocol version (`protocolVersion()`): the `IncludeVersion` prefix
    /// (with its flags) or the `AssumeVersion` version; `None` when unversioned.
    pub fn protocol_version(&self) -> Option<ProtocolVersion> {
        self.version
    }

    /// Reads one value (`ar >> value`).
    ///
    /// Types that read their fields should do it through this method, not by calling
    /// [`Decode::deserialize`] directly: it is what enforces [`MAX_DEPTH`] and
    /// the zero-width work limit.
    pub fn read<T: Decode<'de>>(&mut self) -> Result<T, DecodeError> {
        if self.depth >= MAX_DEPTH {
            return Err(DecodeError::TooDeep);
        }
        let start = self.pos;
        self.depth += 1;
        let value = T::deserialize(self);
        self.depth -= 1;
        let value = value?;
        if self.pos == start {
            self.zero_width_budget = self
                .zero_width_budget
                .checked_sub(1)
                .ok_or(DecodeError::WorkLimit)?;
        }
        Ok(value)
    }

    /// Reads `len` raw bytes, borrowed from the input (`readBytes`).
    pub fn read_bytes(&mut self, len: usize) -> Result<&'de [u8], DecodeError> {
        match self
            .pos
            .checked_add(len)
            .and_then(|end| self.data.get(self.pos..end))
        {
            Some(bytes) => {
                self.pos += len;
                Ok(bytes)
            }
            None => Err(DecodeError::UnexpectedEof {
                pos: self.pos,
                needed: len as u64,
                remaining: self.remaining_len(),
            }),
        }
    }

    /// Reads `N` bytes as an array.
    pub fn read_array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let bytes = self.read_bytes(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(bytes);
        Ok(out)
    }

    /// Reads one byte.
    pub fn read_u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.read_array::<1>()?[0])
    }

    /// Reads a little-endian `u16`.
    pub fn read_u16(&mut self) -> Result<u16, DecodeError> {
        self.read_array().map(u16::from_le_bytes)
    }

    /// Reads a little-endian `u32`.
    pub fn read_u32(&mut self) -> Result<u32, DecodeError> {
        self.read_array().map(u32::from_le_bytes)
    }

    /// Reads a little-endian `u64`.
    pub fn read_u64(&mut self) -> Result<u64, DecodeError> {
        self.read_array().map(u64::from_le_bytes)
    }

    /// Reads the 4-byte count of a string, vector, map or set. A count above `i32::MAX` is
    /// an error. The count is not checked against the bytes left, because elements can be
    /// zero-width; whoever reads the elements fails at the first one that is missing.
    pub fn read_count(&mut self) -> Result<usize, DecodeError> {
        let count = self.read_u32()?;
        if count > i32::MAX as u32 {
            return Err(DecodeError::CountTooLarge(count));
        }
        Ok(count as usize)
    }

    /// Reads the index of a `std::variant` alternative and checks it is below
    /// `alternatives`; the alternative's value follows.
    pub fn read_variant_index(&mut self, alternatives: usize) -> Result<u8, DecodeError> {
        let index = self.read_u8()?;
        if usize::from(index) >= alternatives {
            return Err(DecodeError::InvalidVariantIndex {
                index,
                alternatives,
            });
        }
        Ok(index)
    }

    /// The input not read yet, without consuming it. After an `IncludeVersion` prefix with
    /// the [object-serializer flag](ProtocolVersion::has_object_serializer_flag), this is a
    /// flatbuffers-dialect message: give it to [`Reader::new`](crate::Reader::new) (C++'s
    /// `Reader` hands it to an `ObjectReader` the same way).
    pub fn remaining(&self) -> &'de [u8] {
        self.data.get(self.pos..).unwrap_or_default()
    }

    /// How many bytes are left (`remainingBytes()`).
    pub fn remaining_len(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    /// Whether the input is used up (`empty()`).
    pub fn is_empty(&self) -> bool {
        self.remaining_len() == 0
    }

    /// How many bytes have been read, counting the version prefix.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Consumes and returns the rest of the input.
    pub fn take_remaining(&mut self) -> &'de [u8] {
        let rest = self.remaining();
        self.pos = self.data.len();
        rest
    }

    /// Fails if input is left (`assertEnd()`).
    pub fn finish(&self) -> Result<(), DecodeError> {
        match self.remaining_len() {
            0 => Ok(()),
            n => Err(DecodeError::TrailingBytes(n)),
        }
    }
}

/// `BinaryReader::fromStringRef<T>(bytes, versioning)`, except that unread bytes are an
/// error (C++ ignores them): reads one value and requires the input to end there.
pub fn from_slice<'de, T: Decode<'de>>(
    bytes: &'de [u8],
    versioning: ReadVersioning,
) -> Result<T, DecodeError> {
    let mut r = Reader::new(bytes, versioning)?;
    let value = r.read()?;
    r.finish()?;
    Ok(value)
}
