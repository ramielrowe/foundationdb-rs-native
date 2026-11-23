//! The encoder: a back-to-front message builder.

use crate::shape::right_align;
use crate::{Encode, EncodeError, ProtocolVersion, Shape, Wire};

/// Largest message the writer produces (offsets and soffsets must fit an `i32`).
const MAX_MESSAGE: usize = i32::MAX as usize;

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
#[derive(Debug, Default)]
pub struct Writer {
    /// The message body is `buf[head..]`.
    buf: Vec<u8>,
    head: usize,
    /// Every distinct vtable, back to back.
    vtable_pool: Vec<u16>,
    /// `(start, len)` of each distinct vtable in `vtable_pool`.
    vtables: Vec<(usize, usize)>,
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

    /// Discards the message being built, keeping the allocated memory and the protocol
    /// version.
    pub fn reset(&mut self) {
        self.head = self.buf.len();
        self.vtable_pool.clear();
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
        self.blob_with(bytes.len(), |out| out.copy_from_slice(bytes))
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

    /// Writes a vector of non-union elements: a `u32` count, then the elements inline
    /// (scalars, structs) or as `u32` offsets (tables, strings, vectors). The elements
    /// start at a multiple of max(4, element alignment).
    pub fn vector<T: Encode>(&mut self, items: &[T]) -> Result<ObjectRef, EncodeError> {
        self.vector_counted(items.len(), items)
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
        let result = self.assemble(root, file_identifier);
        self.reset();
        result
    }

    fn assemble(&mut self, root: ObjectRef, file_identifier: u32) -> Result<Vec<u8>, EncodeError> {
        // An even body keeps the vtable block 2-aligned.
        self.pad_for(0, 4)?;
        let body = self.used();
        let vtable_bytes = 2 * self.vtable_pool.len();
        let pad = (8 - (8 + vtable_bytes + body) % 8) % 8;
        let total = 8 + pad + vtable_bytes + body;
        if total > MAX_MESSAGE {
            return Err(EncodeError::TooLarge);
        }
        if root.0 > body || root.0 == 0 {
            return Err(EncodeError::ForeignRef);
        }
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&((total - root.0) as u32).to_le_bytes());
        out.extend_from_slice(&file_identifier.to_le_bytes());
        out.resize(8 + pad, 0);
        for entry in &self.vtable_pool {
            out.extend_from_slice(&entry.to_le_bytes());
        }
        out.extend_from_slice(&self.buf[self.head..]);
        for &(table, vtable) in &self.fixups {
            let table_at = total - table.0;
            let vtable_at = 8 + pad + 2 * self.vtables[vtable].0;
            // Vtables precede every table, so the soffset is positive and fits an i32.
            let soffset = (table_at - vtable_at) as i32;
            out[table_at..table_at + 4].copy_from_slice(&soffset.to_le_bytes());
        }
        Ok(out)
    }

    /// Lays out and writes the table whose fields are `slots[slot_base..]`.
    fn end_table(
        &mut self,
        slot_base: usize,
        scratch_base: usize,
    ) -> Result<ObjectRef, EncodeError> {
        let n = self.slots.len() - slot_base;
        // generate_vtable (flow/flat_buffers.cpp): stable sort by size, descending; absent
        // (zero-size) slots get no space and keep entry 0.
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by(|&a, &b| {
            let size = |i: usize| self.slots[slot_base + i].size_align().0;
            size(b).cmp(&size(a))
        });
        let mut vtable = vec![0u16; n + 2];
        let mut offset = 0usize;
        let mut max_align = 4;
        for &i in &order {
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
        let index = self.intern_vtable(&vtable);
        self.fixups.push((table, index));
        self.slots.truncate(slot_base);
        self.scratch.truncate(scratch_base);
        Ok(table)
    }

    fn intern_vtable(&mut self, vtable: &[u16]) -> usize {
        for (i, &(start, len)) in self.vtables.iter().enumerate() {
            if &self.vtable_pool[start..start + len] == vtable {
                return i;
            }
        }
        self.vtables.push((self.vtable_pool.len(), vtable.len()));
        self.vtable_pool.extend_from_slice(vtable);
        self.vtables.len() - 1
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
        }
        self.head -= n;
        self.buf[self.head..self.head + n].fill(0);
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
        assert_eq!(w.vtables.len(), 2);
        assert_eq!(w.vtable_pool, [6, 8, 4, 10, 16, 4, 8, 12]);
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
            w.vtable_pool,
            [26, 47, 44, 45, 46, 40, 42, 28, 32, 4, 12, 36, 20]
        );
        // The fields start at table + 4, 8-aligned from the end (and so in the message).
        assert_eq!((table.0 - 4) % 8, 0);
    }
}
