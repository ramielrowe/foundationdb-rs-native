//! The encoder: a back-to-front message builder.

use crate::shape::right_align;
use crate::{Encode, EncodeError, ProtocolVersion, Shape, Wire};

/// Largest message the writer produces (offsets and soffsets must fit an `i32`).
const MAX_MESSAGE: usize = i32::MAX as usize;

/// A table with at most this many slots is laid out in stack arrays; a wider one in the
/// writer's scratch vectors.
const STACK_SLOTS: usize = 16;

/// A written object (table, string, blob, vector or out-of-line union payload), as
/// returned by [`Encode::encode_object`] and the [`Writer`] methods.
///
/// It records the object's distance from the end of the message, which is fixed once the
/// object is written because the writer grows the message towards its start. It is only
/// meaningful for the writer (and message) that returned it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ObjectRef(usize);

/// An encoded union value, as returned by [`Encode::encode_union`]: the wire tag (0 for
/// an empty union, k for alternative k − 1) and the alternative's object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UnionRef {
    tag: u8,
    value: Option<ObjectRef>,
}

impl UnionRef {
    /// An empty union (an absent `Optional`): tag 0, offset 0.
    pub const EMPTY: UnionRef = UnionRef {
        tag: 0,
        value: None,
    };

    /// The wire tag.
    pub fn tag(&self) -> u8 {
        self.tag
    }

    /// The alternative's object, or `None` for an empty union.
    pub fn value(&self) -> Option<ObjectRef> {
        self.value
    }
}

#[derive(Clone, Copy, Debug)]
enum Slot {
    /// Inline bytes kept in `Writer::scratch[start..start + size]`.
    Inline {
        start: usize,
        size: usize,
        align: usize,
    },
    /// A union tag.
    Tag(u8),
    /// An offset; `None` writes 0 (an empty union's offset slot).
    Offset(Option<ObjectRef>),
    /// A slot left absent (vtable entry 0, no bytes in the table).
    Absent,
}

impl Slot {
    fn size_align(self) -> (usize, usize) {
        match self {
            Slot::Inline { size, align, .. } => (size, align),
            Slot::Tag(_) => (1, 1),
            Slot::Offset(_) => (4, 4),
            Slot::Absent => (0, 1),
        }
    }
}

/// Builds one message at a time.
///
/// The writer works back to front, like FDB's: every object is written before (at a
/// higher address than) the objects that point at it, so every `u32` offset is positive.
/// A table's children are therefore written while its fields are added, and the table
/// itself when [`TableWriter::finish`] is called. Vtables are deduplicated and collected
/// into a block right after the 8-byte header when [`Writer::finish`] assembles the
/// message; table soffsets are patched then. Every empty string and vector points at one
/// shared empty blob.
///
/// Most callers use [`encode`](crate::encode); build a message by hand only for a root
/// with several members:
///
/// ```
/// use fdb_native_wire::Writer;
///
/// let mut w = Writer::new();
/// let mut root = w.table();
/// root.field(&7u8)?;
/// root.field(&b"abc"[..])?;
/// let root = root.finish()?;
/// let bytes = w.finish(root, 0x000f_db66)?;
/// assert_eq!(&bytes[4..8], &0x000f_db66u32.to_le_bytes());
/// # Ok::<(), fdb_native_wire::EncodeError>(())
/// ```
///
/// After an error the writer holds a partial message; call [`Writer::reset`] (or use a
/// new writer) before writing the next one.
#[derive(Default)]
pub struct Writer {
    /// The message body is `buf[head..]`.
    buf: Vec<u8>,
    head: usize,
    /// Every byte of `buf[..clean_below]` is zero: nothing has been written below it since
    /// the buffer was allocated, so a fresh writer never has to clear what it claims, and
    /// a reused one only the part an earlier message wrote.
    clean_below: usize,
    /// The distinct vtables of the message so far.
    vtables: VtableSet,
    /// `end_table`'s working memory for tables wider than `STACK_SLOTS` (narrower ones
    /// use the stack), kept between tables so that a table costs no allocation: the slot
    /// order (stably sorted by size) and the vtable being built.
    order_scratch: Vec<usize>,
    vtable_scratch: Vec<u16>,
    /// `(table, vtable index)` for every table whose soffset is patched by `finish`.
    fixups: Vec<(ObjectRef, usize)>,
    /// Fields of the tables being built (a stack: nested tables push above their parent).
    slots: Vec<Slot>,
    /// Inline field bytes of the tables being built.
    scratch: Vec<u8>,
    /// Element objects of the vectors being built.
    refs: Vec<ObjectRef>,
    empty_blob: Option<ObjectRef>,
    protocol_version: Option<ProtocolVersion>,
}

/// Sizes and counts only: a reused writer's buffer still holds the bytes of earlier
/// messages, which must not end up in logs.
impl core::fmt::Debug for Writer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Writer")
            .field("used", &self.used())
            .field("capacity", &self.buf.len())
            .field("vtables", &self.vtables.entries.len())
            .field("protocol_version", &self.protocol_version)
            .finish_non_exhaustive()
    }
}

impl Writer {
    /// An empty writer with no protocol version.
    pub fn new() -> Writer {
        Writer::default()
    }

    /// An empty writer for messages to a peer speaking `version`; [`Encode`] impls whose
    /// layout differs between versions read it from [`Writer::protocol_version`].
    pub fn with_protocol_version(version: ProtocolVersion) -> Writer {
        Writer {
            protocol_version: Some(version),
            ..Writer::default()
        }
    }

    /// The protocol version messages are written for, if the writer was given one.
    pub fn protocol_version(&self) -> Option<ProtocolVersion> {
        self.protocol_version
    }

    /// The bytes the writer's message buffer holds: about the largest message it has
    /// written. A caller that keeps a writer for reuse can drop one that grew large.
    pub fn buffer_capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// Discards the message being built, keeping the allocated memory and the protocol
    /// version.
    pub fn reset(&mut self) {
        self.head = self.buf.len();
        self.vtables.clear();
        self.fixups.clear();
        self.slots.clear();
        self.scratch.clear();
        self.refs.clear();
        self.empty_blob = None;
    }

    /// Starts a table. Add its fields in slot order with [`TableWriter::field`], then call
    /// [`TableWriter::finish`].
    pub fn table(&mut self) -> TableWriter<'_> {
        TableWriter {
            slot_base: self.slots.len(),
            scratch_base: self.scratch.len(),
            w: self,
        }
    }

    /// Writes a string or blob: a `u32` length, the bytes, and zero padding to a multiple
    /// of 4. An empty blob is shared with every other empty string and vector.
    pub fn blob(&mut self, bytes: &[u8]) -> Result<ObjectRef, EncodeError> {
        self.blob_slice(bytes)
    }

    /// Writes a blob of `len` bytes that `fill` writes in place; for blob codecs whose
    /// payload is not already in one slice. `fill` is given exactly `len` zeroed bytes.
    pub fn blob_with(
        &mut self,
        len: usize,
        fill: impl FnOnce(&mut [u8]),
    ) -> Result<ObjectRef, EncodeError> {
        if len == 0 {
            return self.empty_vector();
        }
        let count = u32::try_from(len).map_err(|_| EncodeError::TooLarge)?;
        self.pad_for(len + 4, 4)?;
        let start = self.reserve(len)?;
        fill(&mut self.buf[start..start + len]);
        self.push_u32(count)
    }

    /// `blob_with` for bytes already in a slice, which needs no zeroing first.
    fn blob_slice(&mut self, bytes: &[u8]) -> Result<ObjectRef, EncodeError> {
        let len = bytes.len();
        if len == 0 {
            return self.empty_vector();
        }
        let count = u32::try_from(len).map_err(|_| EncodeError::TooLarge)?;
        self.pad_for(len + 4, 4)?;
        let start = self.reserve_overwritten(len)?;
        self.buf[start..start + len].copy_from_slice(bytes);
        self.push_u32(count)
    }

    /// Writes a vector of non-union elements: a `u32` count, then the elements inline
    /// (scalars, structs) or as `u32` offsets (tables, strings, vectors). The elements
    /// start at a multiple of max(4, element alignment).
    pub fn vector<T: Encode>(&mut self, items: &[T]) -> Result<ObjectRef, EncodeError> {
        match T::SHAPE {
            // The elements are written as one slice (`Encode::encode_inline_slice`), a
            // memcpy for `u8`; the same bytes, order of checks and errors as
            // `vector_counted` for a slice.
            Shape::Inline { size, align } if !items.is_empty() => {
                let count32 = u32::try_from(items.len()).map_err(|_| EncodeError::TooLarge)?;
                let len = items.len().checked_mul(size).ok_or(EncodeError::TooLarge)?;
                self.pad_for(len, align.max(4))?;
                let start = self.reserve(len)?;
                T::encode_inline_slice(items, &mut self.buf[start..start + len])?;
                self.push_u32(count32)
            }
            _ => self.vector_counted(items.len(), items),
        }
    }

    /// Writes a vector of non-union elements from an iterator of known length, without
    /// collecting the elements first: the wire form of [`Writer::vector`]. This is what the
    /// collection impls (`BTreeMap`, `HashSet`, ...) use, and what a custom collection
    /// uses to encode itself from its own iterator.
    ///
    /// The items may be values or references (`&T` is [`Encode`] wherever `T` is); an
    /// item that is a table, string or vector is written as the iterator yields it. The
    /// writer does remember one 8-byte reference per item of an out-of-line element (the
    /// offsets are written after the elements, as the writer works back to front).
    ///
    /// ```
    /// use fdb_native_wire::{
    ///     encode_with_identifier, Encode, EncodeError, ObjectRef, Reader, Shape, Wire, Writer,
    /// };
    ///
    /// /// The first `n` multiples of 10, written without building a `Vec`.
    /// struct Tens(u32);
    ///
    /// impl Wire for Tens {
    ///     const SHAPE: Shape = Shape::Object;
    /// }
    ///
    /// impl Encode for Tens {
    ///     fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
    ///         w.vector_iter((0..self.0).map(|i| i * 10))
    ///     }
    /// }
    ///
    /// let bytes = encode_with_identifier(&Tens(3), 1)?;
    /// assert_eq!(Reader::new(&bytes)?.decode_root::<Vec<u32>>()?, [0, 10, 20]);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// [`EncodeError::InvalidShape`] if the iterator's `len()` was wrong (it ended early or
    /// had more items), and the errors of [`Writer::vector`].
    pub fn vector_iter<I>(&mut self, items: I) -> Result<ObjectRef, EncodeError>
    where
        I: IntoIterator,
        I::IntoIter: ExactSizeIterator,
        I::Item: Encode,
    {
        let items = items.into_iter();
        self.vector_counted(items.len(), items)
    }

    /// Writes a vector of `count` non-union elements taken from `items`, which must yield
    /// exactly `count` items (for an iterator that does not know its length, such as a
    /// filtered one, whose count the caller already has). See [`Writer::vector_iter`].
    pub fn vector_counted<I>(&mut self, count: usize, items: I) -> Result<ObjectRef, EncodeError>
    where
        I: IntoIterator,
        I::Item: Encode,
    {
        const SHORT: EncodeError =
            EncodeError::InvalidShape("vector iterator ended before its count");
        const LONG: EncodeError =
            EncodeError::InvalidShape("vector iterator has more items than its count");
        let mut items = items.into_iter();
        if count == 0 {
            return match items.next() {
                None => self.empty_vector(),
                Some(_) => Err(LONG),
            };
        }
        // Do not reserve space for items that cannot come.
        if items.size_hint().1.is_some_and(|most| most < count) {
            return Err(SHORT);
        }
        let count32 = u32::try_from(count).map_err(|_| EncodeError::TooLarge)?;
        match <I::Item as Wire>::SHAPE {
            Shape::Inline { size, align } => {
                let len = count.checked_mul(size).ok_or(EncodeError::TooLarge)?;
                self.pad_for(len, align.max(4))?;
                let start = self.reserve(len)?;
                for i in 0..count {
                    let item = items.next().ok_or(SHORT)?;
                    let at = start + i * size;
                    item.encode_inline(&mut self.buf[at..at + size])?;
                }
            }
            Shape::Table | Shape::Object => {
                let base = self.refs.len();
                for _ in 0..count {
                    let item = items.next().ok_or(SHORT)?;
                    let r = item.encode_object(self)?;
                    self.refs.push(r);
                }
                let len = count.checked_mul(4).ok_or(EncodeError::TooLarge)?;
                self.pad_for(len, 4)?;
                let start = self.reserve(len)?;
                let first = self.used();
                for i in 0..count {
                    let target = self.refs[base + i];
                    let value = relative(first - 4 * i, target)?;
                    let at = start + 4 * i;
                    self.buf[at..at + 4].copy_from_slice(&value.to_le_bytes());
                }
                self.refs.truncate(base);
            }
            Shape::Union | Shape::UnionVector => {
                return Err(EncodeError::InvalidShape(
                    "vector element is a union; use union_vector",
                ));
            }
        }
        if items.next().is_some() {
            return Err(LONG);
        }
        self.push_u32(count32)
    }

    /// Writes a vector of unions: returns the `vector<u8>` of tags and the vector of
    /// offsets (0 for an empty element), which go in the field's two slots.
    pub fn union_vector<T: Encode>(
        &mut self,
        items: &[T],
    ) -> Result<(ObjectRef, ObjectRef), EncodeError> {
        let mut parts = Vec::with_capacity(items.len());
        for item in items {
            parts.push(item.encode_union(self)?);
        }
        if parts.is_empty() {
            let empty = self.empty_vector()?;
            return Ok((empty, empty));
        }
        let count = u32::try_from(parts.len()).map_err(|_| EncodeError::TooLarge)?;
        let len = parts.len().checked_mul(4).ok_or(EncodeError::TooLarge)?;
        self.pad_for(len, 4)?;
        let start = self.reserve(len)?;
        let first = self.used();
        for (i, part) in parts.iter().enumerate() {
            let value = match part.value {
                Some(target) => relative(first - 4 * i, target)?,
                None => 0,
            };
            let at = start + 4 * i;
            self.buf[at..at + 4].copy_from_slice(&value.to_le_bytes());
        }
        let offsets = self.push_u32(count)?;
        self.pad_for(parts.len() + 4, 4)?;
        let start = self.reserve(parts.len())?;
        for (i, part) in parts.iter().enumerate() {
            self.buf[start + i] = part.tag;
        }
        let tags = self.push_u32(count)?;
        Ok((tags, offsets))
    }

    /// Writes `value` as a union alternative: a table, string or vector is written as
    /// usual; a scalar or struct is written out of line as its raw bytes, unaligned.
    /// Unions cannot hold unions (wrap them in [`EnsureTable`](crate::EnsureTable)).
    pub fn alternative<T: Encode + ?Sized>(&mut self, value: &T) -> Result<ObjectRef, EncodeError> {
        match T::SHAPE {
            Shape::Inline { size, .. } => {
                let start = self.reserve(size)?;
                value.encode_inline(&mut self.buf[start..start + size])?;
                Ok(self.current())
            }
            Shape::Table | Shape::Object => value.encode_object(self),
            Shape::Union | Shape::UnionVector => Err(EncodeError::InvalidShape(
                "a union alternative cannot be a union",
            )),
        }
    }

    /// Writes `value` as alternative `tag - 1` of a union ([`Writer::alternative`]) and
    /// returns the union value. `tag` must not be 0.
    pub fn union_value<T: Encode + ?Sized>(
        &mut self,
        tag: u8,
        value: &T,
    ) -> Result<UnionRef, EncodeError> {
        if tag == 0 {
            return Err(EncodeError::InvalidUnionTag);
        }
        let r = self.alternative(value)?;
        Ok(UnionRef {
            tag,
            value: Some(r),
        })
    }

    /// Completes the message: `root` (a table written by this writer, normally the
    /// synthetic root table) is pointed at from byte 0, `file_identifier` is written at
    /// bytes 4..8, and the vtables follow the header. Returns the message and resets the
    /// writer.
    pub fn finish(
        &mut self,
        root: ObjectRef,
        file_identifier: u32,
    ) -> Result<Vec<u8>, EncodeError> {
        let mut out = Vec::new();
        let result = self.assemble(&mut out, root, file_identifier);
        self.reset();
        result.map(|()| out)
    }

    /// [`finish`](Writer::finish), appending the message to `out` instead of returning a new
    /// vector: the bytes appended are those `finish` would return. This lets a message be
    /// serialized straight into a buffer that already holds other bytes, such as a frame's
    /// header (`fdb-native-transport`'s `FrameConfig::encode_frame_with`). On an error `out`
    /// is left as it was. The writer is reset either way.
    pub fn finish_into(
        &mut self,
        out: &mut Vec<u8>,
        root: ObjectRef,
        file_identifier: u32,
    ) -> Result<(), EncodeError> {
        let result = self.assemble(out, root, file_identifier);
        self.reset();
        result
    }

    /// Appends the message to `out`, leaving `out` as it was on an error.
    fn assemble(
        &mut self,
        out: &mut Vec<u8>,
        root: ObjectRef,
        file_identifier: u32,
    ) -> Result<(), EncodeError> {
        // An even body keeps the vtable block 2-aligned.
        self.pad_for(0, 4)?;
        let body = self.used();
        let vtable_bytes = 2 * self.vtables.pool.len();
        let pad = (8 - (8 + vtable_bytes + body) % 8) % 8;
        let total = 8 + pad + vtable_bytes + body;
        if total > MAX_MESSAGE {
            return Err(EncodeError::TooLarge);
        }
        if root.0 > body || root.0 == 0 {
            return Err(EncodeError::ForeignRef);
        }
        let base = out.len();
        out.reserve(total);
        out.extend_from_slice(&((total - root.0) as u32).to_le_bytes());
        out.extend_from_slice(&file_identifier.to_le_bytes());
        out.resize(base + 8 + pad, 0);
        for entry in &self.vtables.pool {
            out.extend_from_slice(&entry.to_le_bytes());
        }
        out.extend_from_slice(&self.buf[self.head..]);
        for &(table, vtable) in &self.fixups {
            let table_at = total - table.0;
            let vtable_at = 8 + pad + 2 * self.vtables.start(vtable);
            // Vtables precede every table, so the soffset is positive and fits an i32.
            let soffset = (table_at - vtable_at) as i32;
            let at = base + table_at;
            out[at..at + 4].copy_from_slice(&soffset.to_le_bytes());
        }
        Ok(())
    }

    /// Lays out and writes the table whose fields are `slots[slot_base..]`.
    fn end_table(
        &mut self,
        slot_base: usize,
        scratch_base: usize,
    ) -> Result<ObjectRef, EncodeError> {
        let n = self.slots.len() - slot_base;
        if n <= STACK_SLOTS {
            // The usual table: its working arrays live on the stack.
            let mut order = [0; STACK_SLOTS];
            let mut vtable = [0; STACK_SLOTS + 2];
            return self.layout_table(
                slot_base,
                scratch_base,
                &mut order[..n],
                &mut vtable[..n + 2],
            );
        }
        // A wide table: the working vectors are moved out of `self` for the duration (a
        // move of three words, not an allocation) and put back whether or not the table
        // fits. They keep their capacity from table to table.
        let mut order = core::mem::take(&mut self.order_scratch);
        let mut vtable = core::mem::take(&mut self.vtable_scratch);
        order.clear();
        order.resize(n, 0);
        vtable.clear();
        vtable.resize(n + 2, 0);
        let table = self.layout_table(slot_base, scratch_base, &mut order, &mut vtable);
        self.order_scratch = order;
        self.vtable_scratch = vtable;
        table
    }

    /// `end_table` with its working memory given: `order` is `n` zeros and `vtable` is
    /// `n + 2` zeros, `n` being the number of slots.
    fn layout_table(
        &mut self,
        slot_base: usize,
        scratch_base: usize,
        order: &mut [usize],
        vtable: &mut [u16],
    ) -> Result<ObjectRef, EncodeError> {
        let n = order.len();
        // generate_vtable (flow/flat_buffers.cpp): stable sort by size, descending; absent
        // (zero-size) slots get no space and keep entry 0.
        for (i, entry) in order.iter_mut().enumerate() {
            *entry = i;
        }
        order.sort_by(|&a, &b| {
            let size = |i: usize| self.slots[slot_base + i].size_align().0;
            size(b).cmp(&size(a))
        });
        let mut offset = 0usize;
        let mut max_align = 4;
        for &i in order.iter() {
            let (size, align) = self.slots[slot_base + i].size_align();
            if size == 0 {
                continue;
            }
            let at = right_align(offset, align);
            vtable[i + 2] = u16::try_from(at + 4).map_err(|_| EncodeError::TooLarge)?;
            offset = at + size;
            max_align = max_align.max(align);
        }
        let table_size = offset + 4;
        vtable[0] = u16::try_from(2 * n + 4).map_err(|_| EncodeError::TooLarge)?;
        vtable[1] = u16::try_from(table_size).map_err(|_| EncodeError::TooLarge)?;

        // Place the table so that its start + 4 is aligned to its largest field alignment.
        self.pad_for(table_size - 4, max_align)?;
        let start = self.reserve(table_size)?;
        let table = self.current();
        for i in 0..n {
            let at = start + usize::from(vtable[i + 2]);
            match self.slots[slot_base + i] {
                Slot::Inline { start: s, size, .. } => {
                    self.buf[at..at + size].copy_from_slice(&self.scratch[s..s + size]);
                }
                Slot::Tag(tag) => self.buf[at] = tag,
                Slot::Offset(Some(target)) => {
                    let value = relative(table.0 - usize::from(vtable[i + 2]), target)?;
                    self.buf[at..at + 4].copy_from_slice(&value.to_le_bytes());
                }
                Slot::Offset(None) | Slot::Absent => {}
            }
        }
        let index = self.vtables.intern(vtable);
        self.fixups.push((table, index));
        self.slots.truncate(slot_base);
        self.scratch.truncate(scratch_base);
        Ok(table)
    }

    fn empty_vector(&mut self) -> Result<ObjectRef, EncodeError> {
        if let Some(r) = self.empty_blob {
            return Ok(r);
        }
        self.pad_for(4, 4)?;
        let r = self.push_u32(0)?;
        self.empty_blob = Some(r);
        Ok(r)
    }

    /// Bytes written so far (the current object's distance from the end).
    fn used(&self) -> usize {
        self.buf.len() - self.head
    }

    fn current(&self) -> ObjectRef {
        ObjectRef(self.used())
    }

    /// Prepends `n` zero bytes and returns the index of the first one in `buf`.
    fn reserve(&mut self, n: usize) -> Result<usize, EncodeError> {
        self.claim(n, true)
    }

    /// Prepends `n` bytes the caller overwrites completely (their old contents are
    /// whatever an earlier message left) and returns the index of the first one in `buf`.
    fn reserve_overwritten(&mut self, n: usize) -> Result<usize, EncodeError> {
        self.claim(n, false)
    }

    fn claim(&mut self, n: usize, zero: bool) -> Result<usize, EncodeError> {
        let used = self.used();
        let needed = used
            .checked_add(n)
            .filter(|&s| s <= MAX_MESSAGE)
            .ok_or(EncodeError::TooLarge)?;
        if self.head < n {
            let len = needed.max(2 * self.buf.len()).max(256);
            let mut grown = vec![0u8; len];
            grown[len - used..].copy_from_slice(&self.buf[self.head..]);
            self.buf = grown;
            self.head = len - used;
            // Fresh memory: all of it below the message is zero.
            self.clean_below = self.head;
        }
        self.head -= n;
        if zero {
            // Only memory an earlier message wrote needs clearing.
            let stale = self.head.max(self.clean_below);
            if stale < self.head + n {
                self.buf[stale..self.head + n].fill(0);
            }
        }
        self.clean_below = self.clean_below.min(self.head);
        Ok(self.head)
    }

    /// Pads with zeros so that, after `extra` more bytes are prepended, the message
    /// start is a multiple of `align` from the end.
    fn pad_for(&mut self, extra: usize, align: usize) -> Result<(), EncodeError> {
        let pad = (align - (self.used() + extra) % align) % align;
        self.reserve(pad).map(|_| ())
    }

    fn push_u32(&mut self, value: u32) -> Result<ObjectRef, EncodeError> {
        let at = self.reserve(4)?;
        self.buf[at..at + 4].copy_from_slice(&value.to_le_bytes());
        Ok(self.current())
    }
}

/// Up to this many distinct vtables are found by comparing against each in turn, which
/// costs no memory and beats hashing for the handful a typical message has. Past it the
/// set builds a hash index.
const VTABLE_LINEAR_MAX: usize = 8;

/// An empty slot of the vtable hash index.
const NO_VTABLE: u32 = u32::MAX;

/// The distinct vtables of a message, in first-use order (which is the order they are
/// written in, so the output does not depend on how they are looked up).
///
/// Lookup is a scan while there are at most [`VTABLE_LINEAR_MAX`] vtables and an
/// open-addressing hash table over their contents after that, so a message with many
/// distinct vtables does not pay a scan of all of them per table. The hash is a fixed
/// function (no random state): equal vtables always find each other, and a collision only
/// costs a comparison of the vtable contents, never a wrong match.
#[derive(Debug, Default)]
struct VtableSet {
    /// Every distinct vtable, back to back.
    pool: Vec<u16>,
    /// `(start, len)` of each distinct vtable in `pool`.
    entries: Vec<(usize, usize)>,
    /// Indices into `entries`, `NO_VTABLE` where empty; a power-of-two length at most half
    /// full, or empty while the set is small enough to scan.
    index: Vec<u32>,
}

impl VtableSet {
    fn clear(&mut self) {
        self.pool.clear();
        self.entries.clear();
        self.index.clear();
    }

    /// Where vtable `i` starts in the pool, in `u16` entries.
    fn start(&self, i: usize) -> usize {
        self.entries[i].0
    }

    fn get(&self, i: usize) -> &[u16] {
        let (start, len) = self.entries[i];
        &self.pool[start..start + len]
    }

    /// The index of `vtable`, adding it if the message has none like it yet.
    fn intern(&mut self, vtable: &[u16]) -> usize {
        self.intern_with(vtable, hash_vtable)
    }

    /// [`intern`](Self::intern) with the hash function given, so that tests can force
    /// collisions.
    fn intern_with(&mut self, vtable: &[u16], hash: impl Fn(&[u16]) -> u64) -> usize {
        if self.index.is_empty() {
            if let Some(i) = (0..self.entries.len()).find(|&i| self.get(i) == vtable) {
                return i;
            }
            let i = self.push(vtable);
            if self.entries.len() > VTABLE_LINEAR_MAX {
                self.rebuild_index(&hash);
            }
            return i;
        }
        let mask = self.index.len() - 1;
        let mut at = hash(vtable) as usize & mask;
        while self.index[at] != NO_VTABLE {
            let i = self.index[at] as usize;
            if self.get(i) == vtable {
                return i;
            }
            at = (at + 1) & mask;
        }
        let i = self.push(vtable);
        // At most 2^32 - 1 vtables (every table is at least 4 bytes of a 2 GiB message).
        self.index[at] = i as u32;
        if 2 * self.entries.len() > self.index.len() {
            self.rebuild_index(&hash);
        }
        i
    }

    fn push(&mut self, vtable: &[u16]) -> usize {
        self.entries.push((self.pool.len(), vtable.len()));
        self.pool.extend_from_slice(vtable);
        self.entries.len() - 1
    }

    /// Sizes the index for the current entries (at most a quarter full) and fills it.
    fn rebuild_index(&mut self, hash: &impl Fn(&[u16]) -> u64) {
        let size = (4 * self.entries.len()).next_power_of_two().max(32);
        self.index.clear();
        self.index.resize(size, NO_VTABLE);
        let mask = size - 1;
        for i in 0..self.entries.len() {
            let mut at = hash(self.get(i)) as usize & mask;
            while self.index[at] != NO_VTABLE {
                at = (at + 1) & mask;
            }
            self.index[at] = i as u32;
        }
    }
}

/// A multiplicative hash of a vtable's entries (the high bits of the product are the well
/// mixed ones, so they are folded down for the table's low-bit mask).
fn hash_vtable(vtable: &[u16]) -> u64 {
    let mut h = 0x243f_6a88_85a3_08d3_u64;
    for &entry in vtable {
        h = (h.rotate_left(5) ^ u64::from(entry)).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    }
    h ^ (h >> 32)
}

/// The `u32` offset from a slot `slot` bytes from the end to `target`.
fn relative(slot: usize, target: ObjectRef) -> Result<u32, EncodeError> {
    match slot.checked_sub(target.0) {
        Some(value) if value > 0 => u32::try_from(value).map_err(|_| EncodeError::TooLarge),
        _ => Err(EncodeError::ForeignRef),
    }
}

/// A table being built by a [`Writer`]; see [`Writer::table`].
///
/// Each [`field`](TableWriter::field) call takes the next vtable slot(s) in order, so call
/// it once per `serializer(ar, ...)` argument, in argument order (skip `Arena`).
#[derive(Debug)]
pub struct TableWriter<'w> {
    w: &'w mut Writer,
    slot_base: usize,
    scratch_base: usize,
}

impl TableWriter<'_> {
    /// Adds the next field, writing its out-of-line parts (children) now.
    pub fn field<T: Encode + ?Sized>(&mut self, value: &T) -> Result<(), EncodeError> {
        match T::SHAPE {
            Shape::Inline { size, align } => {
                let start = self.w.scratch.len();
                self.w.scratch.resize(start + size, 0);
                value.encode_inline(&mut self.w.scratch[start..start + size])?;
                self.w.slots.push(Slot::Inline { start, size, align });
            }
            Shape::Table | Shape::Object => {
                let r = value.encode_object(self.w)?;
                self.w.slots.push(Slot::Offset(Some(r)));
            }
            Shape::Union => {
                let u = value.encode_union(self.w)?;
                self.w.slots.push(Slot::Tag(u.tag));
                self.w.slots.push(Slot::Offset(u.value));
            }
            Shape::UnionVector => {
                let (tags, offsets) = value.encode_union_vector(self.w)?;
                self.w.slots.push(Slot::Offset(Some(tags)));
                self.w.slots.push(Slot::Offset(Some(offsets)));
            }
        }
        Ok(())
    }

    /// Leaves the next `slots` vtable slots absent (entry 0), for serializer arguments the
    /// type does not model; readers decode them as their default values. FDB's writer never
    /// omits a field, but every FDB reader accepts absent slots. A union or a vector of
    /// unions takes two slots.
    pub fn skip(&mut self, slots: usize) {
        for _ in 0..slots {
            self.w.slots.push(Slot::Absent);
        }
    }

    /// Writes the table and returns it.
    pub fn finish(self) -> Result<ObjectRef, EncodeError> {
        self.w.end_table(self.slot_base, self.scratch_base)
    }
}

impl Drop for TableWriter<'_> {
    fn drop(&mut self) {
        // A no-op after finish; drops the fields of an abandoned table.
        self.w.slots.truncate(self.slot_base);
        self.w.scratch.truncate(self.scratch_base);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Decode, DecodeError, Reader};

    fn offset_field(t: &mut TableWriter<'_>, r: ObjectRef) {
        t.w.slots.push(Slot::Offset(Some(r)));
    }

    #[test]
    fn worked_example_layout() {
        // README "Worked example": Inner{a: u32 = 1, b: "x"} as the root's only member.
        let mut w = Writer::new();
        let b = w.blob(b"x").unwrap();
        let mut inner = w.table();
        inner.field(&1u32).unwrap();
        offset_field(&mut inner, b);
        let inner = inner.finish().unwrap();
        let mut root = w.table();
        offset_field(&mut root, inner);
        let root = root.finish().unwrap();
        let bytes = w.finish(root, 0x000f_db0a).unwrap();
        // The same vtables as C++ ({8, 12, 4, 8} and {6, 8, 4}) and the same 56 bytes.
        assert_eq!(bytes.len(), 56);
        let vtables: Vec<u16> = bytes[14..28]
            .chunks(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();
        assert_eq!(vtables, [8, 12, 4, 8, 6, 8, 4]);
    }

    #[test]
    fn vtables_are_shared_by_shape() {
        let mut w = Writer::new();
        let mut a = w.table();
        a.field(&1i32).unwrap();
        let a = a.finish().unwrap();
        let mut b = w.table();
        b.field(&2.5f32).unwrap();
        let b = b.finish().unwrap();
        let s = w.blob(b"s").unwrap();
        let mut root = w.table();
        for r in [a, b, s] {
            offset_field(&mut root, r);
        }
        root.finish().unwrap();
        // {6, 8, 4} for both one-field tables, {10, 16, 4, 8, 12} for the root.
        assert_eq!(w.vtables.entries.len(), 2);
        assert_eq!(w.vtables.pool, [6, 8, 4, 10, 16, 4, 8, 12]);
    }

    #[test]
    fn fields_are_stable_sorted_by_size_and_aligned() {
        // `Scalars` (b, u8, i8, u16, i16, u32, i32, u64, i64, f32, f64): the vtable C++
        // writes in scalars_typical.json.
        let mut w = Writer::new();
        let mut t = w.table();
        t.field(&true).unwrap();
        t.field(&1u8).unwrap();
        t.field(&1i8).unwrap();
        t.field(&1u16).unwrap();
        t.field(&1i16).unwrap();
        t.field(&1u32).unwrap();
        t.field(&1i32).unwrap();
        t.field(&1u64).unwrap();
        t.field(&1i64).unwrap();
        t.field(&1f32).unwrap();
        t.field(&1f64).unwrap();
        let table = t.finish().unwrap();
        assert_eq!(
            w.vtables.pool,
            [26, 47, 44, 45, 46, 40, 42, 28, 32, 4, 12, 36, 20]
        );
        // The fields start at table + 4, 8-aligned from the end (and so in the message).
        assert_eq!((table.0 - 4) % 8, 0);
    }

    /// `n` distinct vtables of assorted lengths, several sharing a length and some a
    /// prefix with others.
    fn distinct_vtables(n: usize) -> Vec<Vec<u16>> {
        (0..n)
            .map(|k| {
                let len = 2 + k % 7;
                (0..len).map(|j| (k * 31 + j * 7 + k / 7) as u16).collect()
            })
            .collect()
    }

    fn check_set(set: &mut VtableSet, vtables: &[Vec<u16>], hash: impl Fn(&[u16]) -> u64 + Copy) {
        // First use assigns indices in order, whichever way they are looked up.
        for (i, v) in vtables.iter().enumerate() {
            assert_eq!(set.intern_with(v, hash), i, "first use of vtable {i}");
        }
        // Every vtable is found again, from any position and any number of times.
        for round in 0..3 {
            for (i, v) in vtables.iter().enumerate().rev() {
                assert_eq!(set.intern_with(v, hash), i, "round {round}, vtable {i}");
            }
        }
        assert_eq!(set.entries.len(), vtables.len());
        assert_eq!(set.pool, vtables.concat());
        for (i, v) in vtables.iter().enumerate() {
            assert_eq!(set.get(i), v.as_slice());
            assert_eq!(
                set.start(i),
                vtables[..i].iter().map(Vec::len).sum::<usize>()
            );
        }
    }

    #[test]
    fn vtable_set_scans_while_small_then_hashes() {
        for n in [
            0,
            1,
            VTABLE_LINEAR_MAX,
            VTABLE_LINEAR_MAX + 1,
            40,
            500,
            5000,
        ] {
            let mut set = VtableSet::default();
            let vtables = distinct_vtables(n);
            check_set(&mut set, &vtables, hash_vtable);
            // The index appears with the first vtable past the scan limit and stays at
            // most half full.
            assert_eq!(set.index.is_empty(), n <= VTABLE_LINEAR_MAX, "{n} vtables");
            assert!(set.index.len() >= 2 * n || set.index.is_empty());
            // Clearing starts over, keeping the first-use order.
            set.clear();
            check_set(&mut set, &vtables, hash_vtable);
        }
    }

    #[test]
    fn vtable_set_survives_hash_collisions() {
        // Every vtable hashes alike, so each lookup walks the whole probe sequence and
        // only the contents tell the vtables apart.
        for constant in [0u64, 1, u64::MAX, 0x8000_0000_0000_0000] {
            let mut set = VtableSet::default();
            check_set(&mut set, &distinct_vtables(300), |_| constant);
        }
        // Two hash values for 300 vtables: clusters wrap around the end of the index.
        let mut set = VtableSet::default();
        check_set(&mut set, &distinct_vtables(300), |v| {
            if v.len() % 2 == 0 { 0 } else { u64::MAX }
        });
        // A vtable that is a prefix of another, or differs only in its last entry, is not
        // the other vtable.
        let mut set = VtableSet::default();
        let long: Vec<u16> = (0..20).collect();
        let mut near = long.clone();
        near[19] += 1;
        let mut vtables = distinct_vtables(20);
        vtables.extend([long[..10].to_vec(), long.clone(), near, long[..19].to_vec()]);
        check_set(&mut set, &vtables, |_| 7);
    }

    #[test]
    fn many_distinct_vtables_are_numbered_in_first_use_order() {
        // Tables whose field sizes differ, so each shape has its own vtable, repeated in
        // a different order: the writer's vtable block lists each shape once, in the order
        // the shapes were first written.
        let mut w = Writer::new();
        let shapes = 60;
        let mut first_use = Vec::new();
        for round in 0..3 {
            for k in 0..shapes {
                let k = if round == 1 { shapes - 1 - k } else { k };
                let mut t = w.table();
                for field in 0..=k % 12 {
                    if (k / 12 + field) % 2 == 0 {
                        t.field(&(field as u8)).unwrap();
                    } else {
                        t.field(&(field as u32)).unwrap();
                    }
                }
                t.finish().unwrap();
                if round == 0 {
                    first_use.push(w.vtables.entries.len());
                }
            }
        }
        let distinct = w.vtables.entries.len();
        assert!(distinct > VTABLE_LINEAR_MAX, "{distinct} distinct vtables");
        assert!(!w.vtables.index.is_empty());
        // Later rounds found only existing vtables.
        assert_eq!(first_use.last().copied(), Some(distinct));
        let mut seen = std::collections::HashSet::new();
        for i in 0..distinct {
            assert!(
                seen.insert(w.vtables.get(i).to_vec()),
                "vtable {i} is a duplicate"
            );
        }
    }

    /// A table of 300 `u16` fields, of which a message has the first `n` (field `i` holds
    /// `i`) and the rest are absent (zero).
    struct Wide(Vec<u16>);

    impl Wire for Wide {
        const SHAPE: Shape = Shape::Table;
    }

    impl<'de> Decode<'de> for Wide {
        fn absent() -> Self {
            Wide(vec![0; 300])
        }

        fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
            let mut t = r.table(at)?;
            Ok(Wide((0..300).map(|_| t.field()).collect::<Result<_, _>>()?))
        }
    }

    fn check_wide_table(n: usize) {
        let mut w = Writer::new();
        let mut t = w.table();
        for i in 0..n {
            t.field(&(i as u16)).unwrap();
        }
        let table = t.finish().unwrap();
        let mut root = w.table();
        offset_field(&mut root, table);
        let root = root.finish().unwrap();
        let bytes = w.finish(root, 1).unwrap();
        let wide: Wide = Reader::new(&bytes).unwrap().decode_root().unwrap();
        let want: Vec<u16> = (0..300).map(|i| if i < n { i as u16 } else { 0 }).collect();
        assert_eq!(wide.0, want, "{n} fields");
    }

    #[test]
    fn tables_wider_than_the_stack_arrays_lay_out_alike() {
        for n in [0, 1, STACK_SLOTS - 1, STACK_SLOTS, STACK_SLOTS + 1, 40, 300] {
            check_wide_table(n);
        }
    }

    #[test]
    fn a_reused_writer_writes_the_bytes_a_new_one_does() {
        // Messages of different sizes and shapes through one writer: the stale bytes of
        // earlier messages must never show through padding or reserved space.
        fn message(w: &mut Writer, n: usize) -> Vec<u8> {
            let s = w.blob(&vec![0xAB; n]).unwrap();
            let v = w.vector(&vec![0xCDu8; n / 2 + 1]).unwrap();
            let mut inner = w.table();
            inner.field(&0xEFu8).unwrap();
            offset_field(&mut inner, s);
            inner.field(&0x1122_3344_5566_7788u64).unwrap();
            let inner = inner.finish().unwrap();
            let mut root = w.table();
            offset_field(&mut root, inner);
            offset_field(&mut root, v);
            let root = root.finish().unwrap();
            w.finish(root, 9).unwrap()
        }
        let mut reused = Writer::new();
        for n in [1000, 3, 500, 1, 4096, 2, 7, 9000, 5, 1000] {
            assert_eq!(
                message(&mut reused, n),
                message(&mut Writer::new(), n),
                "{n}"
            );
        }
        // An abandoned message leaves a writer that still writes clean bytes.
        let mut w = Writer::new();
        message(&mut w, 100);
        let _ = w.blob(&[0xFF; 300]).unwrap();
        w.reset();
        assert_eq!(message(&mut w, 11), message(&mut Writer::new(), 11));
    }

    #[test]
    fn debug_shows_sizes_never_buffer_contents() {
        let mut w = Writer::new();
        w.blob(b"TOP-SECRET-PAYLOAD").unwrap();
        let mut t = w.table();
        t.field(&0xABCD_EF01u32).unwrap();
        let shown = format!("{w:?}", w = t.w);
        assert!(
            shown.starts_with("Writer {") && shown.contains("used:"),
            "{shown}"
        );
        drop(t);
        w.reset();
        // After a reset the buffer still holds the payload; the output must not.
        assert!(w.buf.windows(10).any(|win| win == b"TOP-SECRET"));
        let shown = format!("{w:?}");
        assert!(!shown.contains("SECRET"), "{shown}");
        assert!(shown.contains("used: 0"), "{shown}");
        assert!(format!("{:?}", w.table()).starts_with("TableWriter"));
    }
}
