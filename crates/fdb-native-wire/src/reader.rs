//! The decoder: a bounds-checked, layout-tolerant reader.

use core::cell::{Cell, RefCell};
use core::ops::Range;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::{Decode, DecodeError, ProtocolVersion, Shape, StrictCheck, Wire};

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
/// bounds. Nesting is limited to [`MAX_DEPTH`] tables.
///
/// # Work limit
///
/// Each decode from the root ([`Reader::root`], and so [`Reader::decode_root`] and
/// [`decode`](crate::decode)) may visit at most 4 × the buffer length + 1024 *units*,
/// otherwise it fails with [`DecodeError::WorkLimit`]. Opening a table or following an
/// offset to a zero-field table costs 4, a string or blob 4 + its length, a vector
/// 4 + its count × its element's wire size (the inline size, or 4 for an offset), a vector
/// of unions 5 × its count, and an out-of-line union scalar or struct its size. A message
/// whose objects are each referenced once costs at most its length; FDB's writer only
/// shares the empty blob, which keeps its messages below 2 units per byte. Only a message
/// that points at the same objects over and over (which FDB `main` accepts) reaches the
/// limit: this is the `work_limit` check of the golden-vectors README.
///
/// The limit bounds the work, and so the number of objects decoded, but not the size of
/// the decoded value: a table with no fields present costs a 4-byte offset in a vector
/// (the elements may all point at one table) yet decodes into a whole Rust struct of
/// absent values, so a message of `n` bytes can decode into about `n / 4 ×
/// size_of::<T>()` bytes of `T` (about 128 × `n` for a 512-byte `T`). Bound the size of untrusted messages before decoding them. Vectors
/// reserve at most 64 KiB before their elements are decoded, so a large count in a
/// malformed message does not allocate by itself. (A vector of integers, floats or
/// `bool`s is decoded in one pass over bytes the header showed to be in the message, and
/// is allocated at its final size, never more than the message holds of it: a `Vec<u8>`
/// of a 1 MiB blob is one 1 MiB copy.)
///
/// The budget is shared with the [`VectorView`](crate::VectorView)s a decode creates: a
/// view charges it as its elements are read, after the decode that created it has
/// returned, and the views of one decode draw on one budget. Calling [`Reader::root`]
/// again starts a new budget for the new decode; views of earlier decodes keep theirs
/// (spent or not).
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
    protocol_version: Option<ProtocolVersion>,
    depth: Cell<usize>,
    /// Work units left, until a lazy view is created (`is_shared`).
    budget: Cell<usize>,
    /// Whether the budget lives in `shared` now. A decode that creates no lazy view never
    /// pays for atomics or an allocation.
    is_shared: Cell<bool>,
    /// The budget once shared (atomic) with the lazy views of the current decode, which
    /// outlive the borrow of the reader that created them. Dropped by [`Reader::root`], so
    /// each decode's views keep their own budget.
    shared: RefCell<Option<Arc<AtomicUsize>>>,
}

impl<'de> Reader<'de> {
    /// A reader for `bytes`, which must hold at least the 8-byte header, with no protocol
    /// version.
    pub fn new(bytes: &'de [u8]) -> Result<Reader<'de>, DecodeError> {
        if bytes.len() < 8 {
            return Err(DecodeError::TooShort { len: bytes.len() });
        }
        Ok(Reader {
            buf: bytes,
            protocol_version: None,
            depth: Cell::new(0),
            budget: Cell::new(work_limit(bytes.len())),
            is_shared: Cell::new(false),
            shared: RefCell::new(None),
        })
    }

    /// A reader for a message from a peer speaking `version`; [`Decode`] impls whose
    /// layout differs between versions read it from [`Reader::protocol_version`].
    pub fn with_protocol_version(
        bytes: &'de [u8],
        version: ProtocolVersion,
    ) -> Result<Reader<'de>, DecodeError> {
        let mut reader = Reader::new(bytes)?;
        reader.protocol_version = Some(version);
        Ok(reader)
    }

    /// The protocol version the message was written with, if the reader was given one.
    pub fn protocol_version(&self) -> Option<ProtocolVersion> {
        self.protocol_version
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
    /// (usually one). Each call starts a new [work limit](Reader#work-limit) budget.
    pub fn root(&self) -> Result<TableReader<'_, 'de>, DecodeError> {
        // A fresh budget: views of earlier decodes keep (and may have spent) theirs.
        self.budget.set(work_limit(self.buf.len()));
        self.is_shared.set(false);
        self.shared.replace(None);
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
    /// Use [`Reader::empty_table`] for tables whose `serializer(ar)` has no arguments,
    /// which C++ never follows (a table whose only argument is an `Arena` is followed and
    /// uses this method).
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

    /// Where the payload of the string or blob pointed at by the offset at position `at`
    /// lies in [`Reader::bytes`]: the same checks and work-limit charge as
    /// [`Reader::blob`], but the position instead of a borrow.
    ///
    /// A [`Decode`] impl for an owned zero-copy type (say a `bytes::Bytes` slice of a
    /// reference-counted packet, which has no lifetime to borrow with) takes the range
    /// here and slices its own handle on the packet with it. The range is within the
    /// buffer, so `reader.bytes()[range]` cannot panic.
    ///
    /// ```
    /// use std::ops::Range;
    /// use fdb_native_wire::{Decode, DecodeError, Reader, Shape, Wire};
    ///
    /// /// A string as a range of the packet instead of a borrow of it.
    /// #[derive(Debug, PartialEq)]
    /// struct Span(Range<usize>);
    ///
    /// impl Wire for Span {
    ///     const SHAPE: Shape = Shape::Object;
    /// }
    ///
    /// impl<'de> Decode<'de> for Span {
    ///     fn absent() -> Self {
    ///         Span(0..0)
    ///     }
    ///
    ///     fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
    ///         r.blob_range(at).map(Span)
    ///     }
    /// }
    ///
    /// let packet = fdb_native_wire::encode_with_identifier(&&b"hello"[..], 7)?;
    /// let reader = Reader::new(&packet)?;
    /// let span: Span = reader.decode_root()?;
    /// assert_eq!(&packet[span.0], b"hello");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn blob_range(&self, at: usize) -> Result<Range<usize>, DecodeError> {
        let start = self.follow(at, 4)?;
        let len = u32::from_le_bytes(self.array(start)?) as usize;
        self.slice(start + 4, len)?;
        self.charge(len + 4)?;
        Ok(start + 4..start + 4 + len)
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

    /// Checks the vector of non-union `T` elements whose offset is at position `at` and
    /// returns where its elements are.
    ///
    /// This is the low-level accessor behind every collection impl and behind
    /// [`VectorView`](crate::VectorView), for custom collections that decode their
    /// elements one at a time. Decode element `i` with [`Decode::decode_element`] at
    /// [`VectorHeader::position`]:
    ///
    /// ```
    /// use std::collections::VecDeque;
    /// use fdb_native_wire::{Decode, DecodeError, Reader, Shape, Wire};
    ///
    /// // A `std::deque<T>`-like collection: the `vector<T>` wire form.
    /// struct Deque<T>(VecDeque<T>);
    ///
    /// impl<T> Wire for Deque<T> {
    ///     const SHAPE: Shape = Shape::Object;
    /// }
    ///
    /// impl<'de, T: Decode<'de>> Decode<'de> for Deque<T> {
    ///     fn absent() -> Self {
    ///         Deque(VecDeque::new())
    ///     }
    ///
    ///     fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
    ///         let header = r.vector_header::<T>(at)?;
    ///         // Not `with_capacity(header.len())`: the count is untrusted.
    ///         let mut items = VecDeque::new();
    ///         for i in 0..header.len() {
    ///             let pos = header.position(i).ok_or(DecodeError::Invalid("index"))?;
    ///             items.push_back(T::decode_element(r, pos)?);
    ///         }
    ///         Ok(Deque(items))
    ///     }
    /// }
    ///
    /// let bytes = fdb_native_wire::encode_with_identifier(&vec![1u16, 2, 3], 1)?;
    /// let reader = Reader::new(&bytes)?;
    /// let deque: Deque<u16> = reader.decode_root()?;
    /// assert_eq!(deque.0, [1, 2, 3]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// The header is checked the way eager decoding checks it: the offset, the count and
    /// that `count` elements of `T`'s wire size lie inside the buffer (in 64-bit
    /// arithmetic). A zero-size element and a union element (use a vector of unions)
    /// are rejected. It charges the [work limit](Reader#work-limit) 4 + `count` × the
    /// element's wire size; each [`Decode::decode_element`] call charges the element
    /// it decodes (an inline element is already paid for here). The count is untrusted
    /// until the elements decode, so do not reserve memory for it.
    pub fn vector_header<T: Wire>(&self, at: usize) -> Result<VectorHeader, DecodeError> {
        let stride = match T::SHAPE {
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
        let len = u64::from(count) * stride as u64;
        self.check(start + 4, len)?;
        // In bounds, so count * stride <= buffer length.
        self.charge(len as usize + 4)?;
        Ok(VectorHeader {
            len: count as usize,
            first: start + 4,
            stride,
        })
    }

    /// Decodes a vector of non-union elements whose offset is at position `at`, for the
    /// array and container impls. Inline elements are decoded as scalars of their type
    /// (so a `bool` must be 0 or 1, as `std::array<bool, N>` and the elements of a
    /// `std::set<bool>` are in C++: only `std::vector<bool>` is exempt, and that is
    /// [`Vec<bool>`](Vec), which does not come through here).
    pub(crate) fn vector<T: Decode<'de>, C>(
        &self,
        at: usize,
        mut push: impl FnMut(&mut C, T),
        new: impl FnOnce(usize) -> Result<C, DecodeError>,
    ) -> Result<C, DecodeError> {
        let header = self.vector_header::<T>(at)?;
        let mut out = new(header.len)?;
        for i in 0..header.len {
            push(
                &mut out,
                self.container_element(header.first + i * header.stride)?,
            );
        }
        Ok(out)
    }

    /// Decodes the elements of the vector `header` describes one at a time: the default of
    /// [`Decode::decode_elements`].
    pub(crate) fn decode_elements<T: Decode<'de>>(
        &self,
        header: &VectorHeader,
    ) -> Result<Vec<T>, DecodeError> {
        let mut out = Vec::with_capacity(crate::codec::preallocation::<T>(header.len));
        for i in 0..header.len {
            out.push(T::decode_element(self, header.first + i * header.stride)?);
        }
        Ok(out)
    }

    /// Element `pos` of an array or container: the scalar decoding for inline types (not
    /// [`Decode::decode_element`], which reads a `bool` as `std::vector<bool>` does).
    fn container_element<T: Decode<'de>>(&self, pos: usize) -> Result<T, DecodeError> {
        match T::SHAPE {
            Shape::Inline { size, .. } => T::decode_inline(self.inline(pos, size)?),
            _ => T::decode_element(self, pos),
        }
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
        let mut out = Vec::with_capacity(crate::codec::preallocation::<T>(count as usize));
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

    /// The byte at `pos`, for [`Decode::decode_element`] impls that read their element
    /// bytes themselves (as `bool` does).
    pub fn byte(&self, pos: usize) -> Result<u8, DecodeError> {
        self.buf
            .get(pos)
            .copied()
            .ok_or(DecodeError::OutOfBounds { pos, len: 1 })
    }

    /// The `size` bytes at `pos`, for [`Decode::decode_element`] impls that parse inline
    /// elements themselves.
    pub fn inline(&self, pos: usize, size: usize) -> Result<&'de [u8], DecodeError> {
        self.slice(pos, size)
    }

    fn charge(&self, units: usize) -> Result<(), DecodeError> {
        if !self.is_shared.get() {
            return match self.budget.get().checked_sub(units) {
                Some(left) => {
                    self.budget.set(left);
                    Ok(())
                }
                None => Err(DecodeError::WorkLimit),
            };
        }
        match &*self.shared.borrow() {
            Some(shared) => shared
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                    left.checked_sub(units)
                })
                .map(|_| ())
                .map_err(|_| DecodeError::WorkLimit),
            None => Err(DecodeError::WorkLimit),
        }
    }

    /// What a lazy view needs to read more of the message after this borrow of the
    /// reader has ended. The first call of a decode moves the remaining budget into a
    /// shared atomic, which every view of the decode (and this reader) charges from then
    /// on.
    pub(crate) fn detach(&self) -> Detached<'de> {
        let mut shared = self.shared.borrow_mut();
        let budget = shared.get_or_insert_with(|| {
            self.is_shared.set(true);
            Arc::new(AtomicUsize::new(self.budget.get()))
        });
        Detached {
            buf: self.buf,
            protocol_version: self.protocol_version,
            depth: self.depth.get(),
            budget: Arc::clone(budget),
        }
    }
}

/// A [`Reader`]'s state without its borrow: the buffer, protocol version, nesting depth
/// and the shared work budget, enough to build a reader that continues where the original
/// was, for lazy decoding.
#[derive(Clone, Debug)]
pub(crate) struct Detached<'de> {
    buf: &'de [u8],
    protocol_version: Option<ProtocolVersion>,
    depth: usize,
    budget: Arc<AtomicUsize>,
}

impl<'de> Detached<'de> {
    /// A reader at the nesting depth of the one this was detached from, charging the same
    /// budget.
    pub(crate) fn reader(&self) -> Reader<'de> {
        Reader {
            buf: self.buf,
            protocol_version: self.protocol_version,
            depth: Cell::new(self.depth),
            budget: Cell::new(0),
            is_shared: Cell::new(true),
            shared: RefCell::new(Some(Arc::clone(&self.budget))),
        }
    }
}

/// The checked header of a vector: how many elements it has and where they are; see
/// [`Reader::vector_header`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct VectorHeader {
    len: usize,
    first: usize,
    stride: usize,
}

impl VectorHeader {
    /// The header of an absent vector.
    pub(crate) const EMPTY: VectorHeader = VectorHeader {
        len: 0,
        first: 0,
        stride: 4,
    };

    /// The number of elements.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the vector has no elements.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The position of the first element in [`Reader::bytes`] (for an empty vector, the
    /// position its first element would have).
    pub fn first(&self) -> usize {
        self.first
    }

    /// The distance between elements: the element's inline size, or 4 for an offset.
    pub fn stride(&self) -> usize {
        self.stride
    }

    /// The position of element `index` for [`Decode::decode_element`], or `None` if
    /// `index >= len()`. Every position returned lies within the buffer, with the whole
    /// element (inline) or its offset (tables, strings, vectors) inside it. That holds
    /// for the [`Reader`] (buffer) that produced this header only: a position is not
    /// checked against any other message.
    pub fn position(&self, index: usize) -> Option<usize> {
        // len * stride was checked to fit in the buffer, so this cannot overflow.
        (index < self.len).then(|| self.first + index * self.stride)
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
    /// Skips the next `slots` vtable slots without reading them, for serializer arguments
    /// the type does not model (for example a field that only some protocol versions
    /// have). A union or a vector of unions takes two slots.
    pub fn skip(&mut self, slots: usize) {
        self.next = self.next.saturating_add(slots);
    }

    /// Decodes the next field.
    pub fn field<T: Decode<'de>>(&mut self) -> Result<T, DecodeError> {
        let slot = self.next;
        self.next = self.next.saturating_add(T::SHAPE.slots());
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
        let index = slot.saturating_add(2);
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

/// The work budget for one decode of a `len`-byte message (see [`Reader`]).
fn work_limit(len: usize) -> usize {
    len.saturating_mul(4).saturating_add(1024)
}
