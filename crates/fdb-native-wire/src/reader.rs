//! The decoder: a bounds-checked, layout-tolerant reader.

use core::cell::Cell;

use crate::{Decode, DecodeError, Shape, StrictCheck};

/// Tables may nest at most this deep below the root table (the `too_deep` check).
pub const MAX_DEPTH: usize = 64;

/// Reads one message.
///
/// A `Reader` wraps the whole buffer and hands out [`TableReader`]s for its tables.
/// [`Decode`] impls receive it with the absolute position of the field they decode, and
/// borrow strings and blobs straight from the buffer (lifetime `'de`).
///
/// Every read is bounds-checked against the buffer and all offset arithmetic is done in
/// `usize`/`u64` with overflow checks, so no input can make it panic or read out of
/// bounds. Nesting is limited to [`MAX_DEPTH`] tables, and the total work (and so the
/// total allocation of owned decoding) is limited to a small multiple of the buffer size,
/// which a message without aliased objects never reaches.
///
/// ```
/// use fdb_native_wire::{encode_with_identifier, Reader};
///
/// let bytes = encode_with_identifier(&42u32, 0x000f_db64)?;
/// let reader = Reader::new(&bytes)?;
/// reader.check_file_identifier(0x000f_db64)?;
/// assert_eq!(reader.decode_root::<u32>()?, 42);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct Reader<'de> {
    buf: &'de [u8],
    depth: Cell<usize>,
    budget: Cell<usize>,
}

impl<'de> Reader<'de> {
    /// A reader for `bytes`, which must hold at least the 8-byte header.
    pub fn new(bytes: &'de [u8]) -> Result<Reader<'de>, DecodeError> {
        if bytes.len() < 8 {
            return Err(DecodeError::TooShort { len: bytes.len() });
        }
        Ok(Reader {
            buf: bytes,
            depth: Cell::new(0),
            budget: Cell::new(bytes.len().saturating_mul(4).saturating_add(1024)),
        })
    }

    /// The whole message.
    pub fn bytes(&self) -> &'de [u8] {
        self.buf
    }

    /// The file identifier at bytes 4..8.
    pub fn file_identifier(&self) -> u32 {
        u32::from_le_bytes([self.buf[4], self.buf[5], self.buf[6], self.buf[7]])
    }

    /// Fails unless the file identifier is `expected`.
    pub fn check_file_identifier(&self, expected: u32) -> Result<(), DecodeError> {
        let found = self.file_identifier();
        if found == expected {
            Ok(())
        } else {
            Err(DecodeError::FileIdentifierMismatch { expected, found })
        }
    }

    /// The synthetic root table. Its fields are the items the message was written with
    /// (usually one).
    pub fn root(&self) -> Result<TableReader<'_, 'de>, DecodeError> {
        self.table(0)
    }

    /// Decodes the first (usually only) member of the root table, without checking the
    /// file identifier.
    pub fn decode_root<T: Decode<'de>>(&self) -> Result<T, DecodeError> {
        self.root()?.field()
    }

    /// Opens the table pointed at by the offset at position `at`.
    ///
    /// Checks the table and its vtable against the buffer, and the `table_size`,
    /// `vtable_size`, `offset_into_header`, `zero_uoffset` and `too_deep` strict checks.
    /// Use [`Reader::empty_table`] for zero-field tables, which C++ never follows.
    pub fn table(&self, at: usize) -> Result<TableReader<'_, 'de>, DecodeError> {
        if self.depth.get() > MAX_DEPTH {
            return Err(DecodeError::Strict(StrictCheck::TooDeep));
        }
        self.charge(4)?;
        let table = self.follow(at, 4)?;
        let soffset = i32::from_le_bytes(self.array(table)?);
        let vtable = i64::try_from(table)
            .ok()
            .and_then(|t| t.checked_sub(i64::from(soffset)))
            .and_then(|v| usize::try_from(v).ok())
            .ok_or(DecodeError::OutOfBounds { pos: table, len: 4 })?;
        let [s0, s1, t0, t1] = self.array::<4>(vtable)?;
        if vtable < 8 {
            return Err(DecodeError::Strict(StrictCheck::OffsetIntoHeader));
        }
        let vtable_size = usize::from(u16::from_le_bytes([s0, s1]));
        let table_size = usize::from(u16::from_le_bytes([t0, t1]));
        if vtable_size < 4 || vtable_size % 2 != 0 {
            return Err(DecodeError::Strict(StrictCheck::VtableSize));
        }
        self.slice(vtable, vtable_size)?;
        if table_size < 4
            || table
                .checked_add(table_size)
                .is_none_or(|end| end > self.buf.len())
        {
            return Err(DecodeError::Strict(StrictCheck::TableSize));
        }
        self.depth.set(self.depth.get() + 1);
        Ok(TableReader {
            r: self,
            table,
            vtable,
            vtable_len: vtable_size / 2,
            table_size,
            next: 0,
        })
    }

    /// Checks the offset at position `at` to a zero-field table such as
    /// [`Void`](crate::Void). C++ never follows it; this only applies the
    /// `out_of_bounds`, `empty_table_offset`, `zero_uoffset`, `offset_into_header` and
    /// `too_deep` strict checks.
    pub fn empty_table(&self, at: usize) -> Result<(), DecodeError> {
        if self.depth.get() > MAX_DEPTH {
            return Err(DecodeError::Strict(StrictCheck::TooDeep));
        }
        self.charge(4)?;
        let target = self.follow_raw(at)?;
        if target > self.buf.len() {
            return Err(DecodeError::OutOfBounds {
                pos: target,
                len: 0,
            });
        }
        if self.buf.len() - target < 4 {
            return Err(DecodeError::Strict(StrictCheck::EmptyTableOffset));
        }
        Ok(())
    }

    /// The string or blob pointed at by the offset at position `at`: a `u32` length and
    /// that many bytes, borrowed from the buffer. Blob codecs decode their payload from
    /// this.
    pub fn blob(&self, at: usize) -> Result<&'de [u8], DecodeError> {
        let start = self.follow(at, 4)?;
        let len = u32::from_le_bytes(self.array(start)?) as usize;
        let bytes = self.slice(start + 4, len)?;
        self.charge(len + 4)?;
        Ok(bytes)
    }

    /// Decodes a union alternative whose offset is at position `at`: a table, string or
    /// vector is decoded as usual, a scalar or struct from the raw bytes the offset points
    /// at (which need not be aligned).
    pub fn alternative<T: Decode<'de>>(&self, at: usize) -> Result<T, DecodeError> {
        match T::SHAPE {
            Shape::Inline { size, .. } => {
                let target = self.follow_raw(at)?;
                let bytes = self.slice(target, size)?;
                self.charge(size)?;
                T::decode_inline(bytes)
            }
            Shape::Table | Shape::Object => T::decode_object(self, at),
            Shape::Union | Shape::UnionVector => Err(DecodeError::Invalid(
                "a union alternative cannot be a union",
            )),
        }
    }

    /// Decodes a vector of non-union elements whose offset is at position `at`.
    pub(crate) fn vector<T: Decode<'de>, C>(
        &self,
        at: usize,
        mut push: impl FnMut(&mut C, T),
        new: impl FnOnce(usize) -> Result<C, DecodeError>,
    ) -> Result<C, DecodeError> {
        let element_size = match T::SHAPE {
            // A zero-size element would let the count alone decide the allocation.
            Shape::Inline { size: 0, .. } => {
                return Err(DecodeError::Invalid("zero-size vector element"));
            }
            Shape::Inline { size, .. } => size,
            Shape::Table | Shape::Object => 4,
            Shape::Union | Shape::UnionVector => {
                return Err(DecodeError::Invalid(
                    "a vector of unions cannot appear here",
                ));
            }
        };
        let start = self.follow(at, 4)?;
        let count = u32::from_le_bytes(self.array(start)?);
        let len = u64::from(count) * element_size as u64;
        self.check(start + 4, len)?;
        // In bounds, so count * element_size <= buffer length.
        self.charge(len as usize + 4)?;
        let count = count as usize;
        let mut out = new(count)?;
        for i in 0..count {
            push(
                &mut out,
                T::decode_element(self, start + 4 + i * element_size)?,
            );
        }
        Ok(out)
    }

    /// Decodes a vector of unions from its tags slot and offsets slot positions.
    pub(crate) fn union_vector<T: Decode<'de>>(
        &self,
        tags_at: usize,
        offsets_at: usize,
    ) -> Result<Vec<T>, DecodeError> {
        // As on FDB main: the tags vector's own count is not used; the offsets vector's
        // count bounds both.
        let tags = self.follow(tags_at, 4)? + 4;
        let offsets = self.follow(offsets_at, 4)?;
        let count = u32::from_le_bytes(self.array(offsets)?);
        let offsets = offsets + 4;
        let tag_bytes = self.slice(tags, count as usize)?;
        self.check(offsets, u64::from(count) * 4)?;
        self.charge((count as usize).saturating_mul(5))?;
        let mut out = Vec::with_capacity(count as usize);
        for (i, &tag) in tag_bytes.iter().enumerate() {
            out.push(if tag == 0 {
                T::absent()
            } else {
                T::decode_union(self, tag, offsets + 4 * i)?
            });
        }
        Ok(out)
    }

    /// Follows the offset at `at` to an object whose first `target_len` bytes must be in
    /// the buffer.
    fn follow(&self, at: usize, target_len: usize) -> Result<usize, DecodeError> {
        let target = self.follow_raw(at)?;
        self.slice(target, target_len)?;
        Ok(target)
    }

    /// Follows the offset at `at`, checking only the offset itself.
    fn follow_raw(&self, at: usize) -> Result<usize, DecodeError> {
        let offset = u32::from_le_bytes(self.array(at)?);
        if offset == 0 && at >= 8 {
            return Err(DecodeError::Strict(StrictCheck::ZeroUoffset));
        }
        let target = at
            .checked_add(offset as usize)
            .ok_or(DecodeError::OutOfBounds {
                pos: at,
                len: u64::from(offset),
            })?;
        if target < 8 {
            return Err(DecodeError::Strict(StrictCheck::OffsetIntoHeader));
        }
        Ok(target)
    }

    fn check(&self, pos: usize, len: u64) -> Result<(), DecodeError> {
        let end = (pos as u64).checked_add(len);
        if pos <= self.buf.len() && end.is_some_and(|end| end <= self.buf.len() as u64) {
            Ok(())
        } else {
            Err(DecodeError::OutOfBounds { pos, len })
        }
    }

    fn slice(&self, pos: usize, len: usize) -> Result<&'de [u8], DecodeError> {
        pos.checked_add(len)
            .and_then(|end| self.buf.get(pos..end))
            .ok_or(DecodeError::OutOfBounds {
                pos,
                len: len as u64,
            })
    }

    fn array<const N: usize>(&self, pos: usize) -> Result<[u8; N], DecodeError> {
        let bytes = self.slice(pos, N)?;
        let mut out = [0; N];
        out.copy_from_slice(bytes);
        Ok(out)
    }

    pub(crate) fn byte(&self, pos: usize) -> Result<u8, DecodeError> {
        self.buf
            .get(pos)
            .copied()
            .ok_or(DecodeError::OutOfBounds { pos, len: 1 })
    }

    pub(crate) fn inline(&self, pos: usize, size: usize) -> Result<&'de [u8], DecodeError> {
        self.slice(pos, size)
    }

    fn charge(&self, units: usize) -> Result<(), DecodeError> {
        match self.budget.get().checked_sub(units) {
            Some(left) => {
                self.budget.set(left);
                Ok(())
            }
            None => Err(DecodeError::WorkLimit),
        }
    }
}

/// A table being read; see [`Reader::table`].
///
/// Each [`field`](TableReader::field) call reads the next vtable slot(s) in order, so call
/// it once per `serializer(ar, ...)` argument, in argument order (skip `Arena`). A field
/// whose slot is past the end of the vtable, or whose vtable entry is 0, reads as
/// [`Decode::absent`]; fields the schema does not know about are never read.
#[derive(Debug)]
pub struct TableReader<'r, 'de> {
    r: &'r Reader<'de>,
    table: usize,
    vtable: usize,
    /// The vtable length in u16 entries, header included.
    vtable_len: usize,
    table_size: usize,
    /// The next slot.
    next: usize,
}

impl<'de> TableReader<'_, 'de> {
    /// Decodes the next field.
    pub fn field<T: Decode<'de>>(&mut self) -> Result<T, DecodeError> {
        let slot = self.next;
        self.next += T::SHAPE.slots();
        match T::SHAPE {
            Shape::Inline { size, .. } => match self.entry(slot, size)? {
                Some(at) => T::decode_inline(self.r.inline(at, size)?),
                None => Ok(T::absent()),
            },
            Shape::Table | Shape::Object => match self.entry(slot, 4)? {
                Some(at) => T::decode_object(self.r, at),
                None => Ok(T::absent()),
            },
            Shape::Union => {
                let tag_at = self.entry(slot, 1)?;
                let offset_at = self.entry(slot + 1, 4)?;
                let Some(tag_at) = tag_at else {
                    return Ok(T::absent());
                };
                let tag = self.r.byte(tag_at)?;
                match offset_at {
                    // Tag 0, or a tag without an offset slot, reads as the default (C++).
                    Some(offset_at) if tag != 0 => T::decode_union(self.r, tag, offset_at),
                    _ => Ok(T::absent()),
                }
            }
            Shape::UnionVector => {
                let tags_at = self.entry(slot, 4)?;
                let offsets_at = self.entry(slot + 1, 4)?;
                match (tags_at, offsets_at) {
                    (None, _) => Ok(T::absent()),
                    (Some(_), None) => Err(DecodeError::UnionVectorMissingOffsets),
                    (Some(tags), Some(offsets)) => T::decode_union_vector(self.r, tags, offsets),
                }
            }
        }
    }

    /// The position of slot `slot`'s field, or `None` if it is absent.
    fn entry(&self, slot: usize, size: usize) -> Result<Option<usize>, DecodeError> {
        let index = slot + 2;
        if index >= self.vtable_len {
            return Ok(None);
        }
        let [lo, hi] = self.r.array(self.vtable + 2 * index)?;
        let entry = usize::from(u16::from_le_bytes([lo, hi]));
        match entry {
            0 => Ok(None),
            1..=3 => Err(DecodeError::Strict(StrictCheck::VtableEntry)),
            _ if entry + size > self.table_size => Err(DecodeError::Strict(StrictCheck::TableSize)),
            _ => Ok(Some(self.table + entry)),
        }
    }
}

impl Drop for TableReader<'_, '_> {
    fn drop(&mut self) {
        self.r.depth.set(self.r.depth.get().saturating_sub(1));
    }
}
