//! Sans-IO codec for FoundationDB's flatbuffers dialect (`flow/include/flow/flat_buffers.h`),
//! the format of every FlowTransport packet body on FDB 7.4 and 8.0.
//!
//! The dialect looks like flatbuffers but is not compatible with it: the root is a
//! synthetic table holding the message, vtable slots follow `serializer(ar, ...)` argument
//! order, unions take two slots and may hold bare scalars and blobs, and the file
//! identifier is a little-endian number. `tools/golden-vectors/README.md` describes the
//! format; this crate is tested against the vectors that tool generates from FDB's own
//! C++ code, and its output against FDB's readers.
//!
//! The legacy `BinaryWriter` format is not implemented yet.
//!
//! # Usage
//!
//! ```
//! use fdb_native_wire::{
//!     decode, encode, Decode, DecodeError, Encode, EncodeError, ErrorOr, EnsureTable,
//!     FileIdentified, FileIdentifier, ObjectRef, Reader, Shape, Wire, Writer,
//! };
//!
//! // struct Inner { uint32_t a; std::string b;
//! //     template <class Ar> void serialize(Ar& ar) { serializer(ar, a, b); } };
//! #[derive(Debug, PartialEq)]
//! struct Inner<'a> {
//!     a: u32,
//!     b: &'a [u8],
//! }
//!
//! impl FileIdentified for Inner<'_> {
//!     const FILE_IDENTIFIER: FileIdentifier = FileIdentifier::new(1039114);
//! }
//!
//! impl Wire for Inner<'_> {
//!     const SHAPE: Shape = Shape::Table;
//! }
//!
//! impl Encode for Inner<'_> {
//!     fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
//!         let mut t = w.table();
//!         t.field(&self.a)?;
//!         t.field(&self.b)?;
//!         t.finish()
//!     }
//! }
//!
//! impl<'de: 'a, 'a> Decode<'de> for Inner<'a> {
//!     fn absent() -> Self {
//!         Inner { a: Decode::absent(), b: Decode::absent() }
//!     }
//!
//!     fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
//!         let mut t = r.table(at)?;
//!         Ok(Inner { a: t.field()?, b: t.field()? })
//!     }
//! }
//!
//! let bytes = encode(&Inner { a: 1, b: b"x" })?;
//! assert_eq!(&bytes[4..8], &0x000f_db0au32.to_le_bytes());
//! let inner: Inner = decode(&bytes)?; // `b` borrows from `bytes`
//! assert_eq!(inner, Inner { a: 1, b: b"x" });
//!
//! // A reply as it travels on the wire.
//! let reply: ErrorOr<EnsureTable<Inner>> = Ok(EnsureTable(inner));
//! let bytes = encode(&reply)?;
//! assert_eq!(&bytes[4..8], &0x020f_db0au32.to_le_bytes());
//! assert_eq!(decode::<ErrorOr<EnsureTable<Inner>>>(&bytes)?, reply);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Traits
//!
//! - [`Wire`] gives a type's [`Shape`]: inline (scalars, structs), table, object (strings,
//!   blobs, vectors), union, or vector of unions. The shape decides the vtable slots, size
//!   and alignment of a field, and how it is reached.
//! - [`Encode`] and [`Decode<'de>`](Decode) have one method per shape; an impl implements
//!   the one(s) for its shape. `Decode::absent` is the value of a missing field (C++'s
//!   default-constructed value), which is what makes old and new schemas interoperate.
//! - [`FileIdentified`] gives a root type's [`FileIdentifier`], composed by wrapper impls.
//!
//! The crate implements them for integers, floats, `bool`, [`Uid`], tuples (FDB
//! `std::tuple` structs), `&[u8]` (zero-copy strings), `Vec<T>`, `[T]` and `[T; N]`
//! (vectors, `std::array`), `Option<T>` (`Optional`), [`ErrorOr<T>`] (`Result<T,
//! FdbError>`), [`EnsureTable<T>`], [`Void`] and [`FdbError`], plus `&T` and `Box<T>`.
//!
//! Tables, unions and structs are normally derived with
//! [`#[derive(Wire)]`](derive@Wire) (the `derive` feature, on by default), whose docs are
//! the attribute reference. **Field order is `serializer(ar, ...)` argument order.**
//!
//! By hand, **tables** are mechanical: one
//! [`TableWriter::field`] / [`TableReader::field`] call per `serializer(ar, ...)` argument,
//! in argument order, skipping `Arena` (which takes no slot). A table whose
//! `serializer(ar)` call has no arguments at all (such as [`Void`]) decodes with
//! [`Reader::empty_table`], because C++ never follows the offset to it; a table whose only
//! argument is an `Arena` is followed, so it still uses [`Reader::table`] (with no `field`
//! calls). Slots a type does not model are stepped over with [`TableReader::skip`] and
//! written absent with [`TableWriter::skip`]; a layout that depends on the peer's version
//! reads [`Reader::protocol_version`] / [`Writer::protocol_version`]. Wrapper impls reject
//! shapes FDB cannot serialize at compile time (a union directly inside `Option`,
//! `ErrorOr` or an array, a struct inside a struct). **Unions**
//! (`std::variant`) are enums: `encode_union` returns [`UnionRef::EMPTY`] or
//! `w.union_value(k + 1, alternative_k)`, `decode_union` maps tag `k + 1` to
//! `r.alternative(at)` and any other tag to `absent()` (alternative 0's absent value for a
//! `std::variant`). **Structs** use [`Shape::of_struct`] and
//! [`Shape::struct_member_offset`]. **Blob codecs** (a type whose wire form is a single
//! string-framed blob, such as FDB's pre-serialized `VectorRef<KeyValueRef>`) are types of
//! shape [`Shape::Object`] that write with [`Writer::blob_with`] and read their payload
//! from [`Reader::blob`]; they are always written by hand.
//!
//! # Writer
//!
//! [`Writer`] builds a message back to front, as FDB does, so every child is at a higher
//! address than its parent and every `u32` offset is positive. Children are written as
//! their parent's fields are added; a table is laid out exactly like `generate_vtable`
//! does (fields stable-sorted by size, descending, each aligned; the table placed so
//! that its fields are aligned in the buffer). Vtables are deduplicated by content and put
//! in one block after the header; empty strings and vectors share one empty blob. The
//! output decodes to the same fields under FDB's readers but is not byte-identical to
//! FDB's (whose vtable order depends on the C++ compiler).
//!
//! # Reader
//!
//! [`Reader`] decodes in place. It is layout-tolerant like FDB's reader: fields may be
//! anywhere in their table, vtables anywhere in the buffer, a field beyond the end of
//! its vtable (or with entry 0) reads as `absent()`, and unknown fields are ignored. It
//! makes every bounds check FDB `main`'s reader makes (the 7.4 and 8.0 readers make
//! none), with 64-bit arithmetic and byte-wise little-endian loads, and never panics.
//!
//! It is stricter than FDB in the ways the golden-vectors README permits (see
//! [`StrictCheck`]): `offset_into_header`, `zero_uoffset`, `vtable_size`, `table_size`,
//! `vtable_entry`, `bool_value`, `empty_table_offset`, `out_of_bounds` (for offsets C++
//! does not follow) and `too_deep` (more than [`MAX_DEPTH`] nested tables). It reads
//! out-of-range union tags, a union tag without its offset slot, and a vector of unions
//! whose tags count differs from its offsets count the way C++ does.
//!
//! It also enforces a work limit ([`DecodeError::WorkLimit`], the README's `work_limit`
//! check): one decode visits at most 4 × the message length + 1024 units of objects (see
//! [`Reader`'s docs](Reader#work-limit) for the units). FDB's writer stays below 2 units
//! per byte; only messages that reference the same objects over and over reach it, and
//! FDB `main` would accept those. The limit bounds work, not output: a message full of
//! offsets to one table with no fields present decodes into one whole Rust struct per
//! offset, about `size_of::<T>() / 4` times the message size. Bound the size of
//! untrusted messages before decoding them.
//!
//! # File identifiers
//!
//! [`encode`] writes `T::FILE_IDENTIFIER` and [`decode`] checks it; use
//! [`encode_with_identifier`] and [`Reader`] (with or without
//! [`Reader::check_file_identifier`]) to choose the identifier or skip the check.

#![warn(missing_docs)]

mod codec;
mod error;
mod file_identifier;
mod protocol_version;
mod reader;
mod shape;
mod types;
mod writer;

#[doc(hidden)]
#[path = "private.rs"]
pub mod __private;

pub use codec::{Decode, DecodeOwned, Encode, Wire};
pub use error::{DecodeError, EncodeError, StrictCheck};
#[cfg(feature = "derive")]
pub use fdb_native_wire_derive::Wire;
pub use file_identifier::{FileIdentified, FileIdentifier, Wrapper};
pub use protocol_version::ProtocolVersion;
pub use reader::{MAX_DEPTH, Reader, TableReader};
pub use shape::Shape;
pub use types::{EnsureTable, ErrorOr, FdbError, Uid, Void};
pub use writer::{ObjectRef, TableWriter, UnionRef, Writer};

/// Encodes `value` as the only member of a message with `T`'s file identifier.
pub fn encode<T: Encode + FileIdentified + ?Sized>(value: &T) -> Result<Vec<u8>, EncodeError> {
    encode_with_identifier(value, T::FILE_IDENTIFIER.value())
}

/// Encodes `value` as the only member of a message with the given file identifier.
pub fn encode_with_identifier<T: Encode + ?Sized>(
    value: &T,
    file_identifier: u32,
) -> Result<Vec<u8>, EncodeError> {
    let mut w = Writer::new();
    let mut root = w.table();
    root.field(value)?;
    let root = root.finish()?;
    w.finish(root, file_identifier)
}

/// Decodes a message whose only (or first) member is a `T`, checking that its file
/// identifier is `T`'s.
pub fn decode<'de, T: Decode<'de> + FileIdentified>(bytes: &'de [u8]) -> Result<T, DecodeError> {
    let reader = Reader::new(bytes)?;
    reader.check_file_identifier(T::FILE_IDENTIFIER.value())?;
    reader.decode_root()
}
