//! The codec traits and their impls for standard types.

use crate::{
    DecodeError, EncodeError, ObjectRef, Reader, Shape, StrictCheck, UnionRef, VectorHeader, Writer,
};

/// A type with a wire representation; states its [`Shape`].
///
/// [`Encode`] and [`Decode`] dispatch on the shape: a field, vector element or union
/// alternative of shape [`Shape::Inline`] goes through `encode_inline`/`decode_inline`,
/// [`Shape::Table`] and [`Shape::Object`] through `encode_object`/`decode_object`, and so
/// on. Each impl overrides exactly the methods its shape uses.
///
/// Impls for wrapper types check at compile time that they wrap a type FDB can serialize
/// there: a union alternative (`Option<T>`, [`ErrorOr<T>`](crate::ErrorOr)) or a
/// fixed-size array element cannot be a union, and struct members cannot be structs.
///
/// ```compile_fail
/// // Optional<Optional<int>> does not compile in C++ either; use EnsureTable.
/// fdb_native_wire::encode_with_identifier(&Some(Some(1u8)), 1).unwrap();
/// ```
///
/// ```compile_fail
/// // ErrorOr<Optional<T>> needs EnsureTable: ErrorOr<EnsureTable<Optional<T>>>.
/// let reply: fdb_native_wire::ErrorOr<Option<u8>> = Ok(None);
/// let _ = fdb_native_wire::encode_with_identifier(&reply, 1);
/// ```
///
/// ```compile_fail
/// let _ = fdb_native_wire::encode_with_identifier(&[Some(1u8); 2], 1);
/// ```
///
/// ```compile_fail
/// let _ = fdb_native_wire::encode_with_identifier(&((1u8, 2u8), 3u8), 1);
/// ```
///
/// ```
/// use fdb_native_wire::EnsureTable;
/// let _ = fdb_native_wire::encode_with_identifier(&Some(EnsureTable(Some(1u8))), 1);
/// ```
pub trait Wire {
    /// How values of this type are stored.
    const SHAPE: Shape;

    /// Whether this is a struct (FDB `std::tuple`); structs cannot be struct members.
    const STRUCT: bool = false;
}

/// A type that can be written.
///
/// Implement the method(s) for the type's [`Wire::SHAPE`]; the defaults of the others
/// fail with [`EncodeError::InvalidShape`] and are never called for a consistent impl.
///
/// | shape | method | what it does |
/// | --- | --- | --- |
/// | `Inline` | [`encode_inline`](Encode::encode_inline) | writes `size` bytes into `out` |
/// | `Table` | [`encode_object`](Encode::encode_object) | `w.table()`, one `field` per serializer argument, `finish` |
/// | `Object` | [`encode_object`](Encode::encode_object) | `w.blob(..)`, `w.blob_with(..)` or `w.vector(..)` |
/// | `Union` | [`encode_union`](Encode::encode_union) | [`UnionRef::EMPTY`] or `w.union_value(tag, alternative)` |
/// | `UnionVector` | [`encode_union_vector`](Encode::encode_union_vector) | `w.union_vector(..)` |
pub trait Encode: Wire {
    /// Writes an inline value into `out`, which is exactly `size` bytes of its
    /// [`Shape::Inline`] and zeroed.
    fn encode_inline(&self, out: &mut [u8]) -> Result<(), EncodeError> {
        let _ = out;
        Err(EncodeError::InvalidShape(
            "encode_inline is not implemented for this type",
        ))
    }

    /// Writes a table, string, blob or vector and returns it.
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let _ = w;
        Err(EncodeError::InvalidShape(
            "encode_object is not implemented for this type",
        ))
    }

    /// Writes a union's alternative (if any) and returns the union value.
    fn encode_union(&self, w: &mut Writer) -> Result<UnionRef, EncodeError> {
        let _ = w;
        Err(EncodeError::InvalidShape(
            "encode_union is not implemented for this type",
        ))
    }

    /// Writes a vector of unions and returns its tags and offsets vectors.
    fn encode_union_vector(&self, w: &mut Writer) -> Result<(ObjectRef, ObjectRef), EncodeError> {
        let _ = w;
        Err(EncodeError::InvalidShape(
            "encode_union_vector is not implemented for this type",
        ))
    }

    /// Writes `items`, the elements of a vector of an inline type, into `out`, which is
    /// exactly `items.len()` times the type's inline size, zeroed. [`Writer::vector`]
    /// calls it for vectors of [`Shape::Inline`] elements.
    ///
    /// The default calls [`encode_inline`](Encode::encode_inline) on each element in turn.
    /// There is no need to override it unless a whole slice can be written faster than
    /// element by element: the integer, float and `bool` impls do (a `Vec<u8>` is a
    /// memcpy), which is the way around the lack of specialization.
    fn encode_inline_slice(items: &[Self], out: &mut [u8]) -> Result<(), EncodeError>
    where
        Self: Sized,
    {
        let Shape::Inline { size, .. } = Self::SHAPE else {
            return Err(EncodeError::InvalidShape(
                "inline vector of a non-inline type",
            ));
        };
        if items.len().checked_mul(size) != Some(out.len()) {
            return Err(EncodeError::InvalidShape("inline vector size"));
        }
        for (i, item) in items.iter().enumerate() {
            let chunk = out
                .get_mut(i * size..(i + 1) * size)
                .ok_or(EncodeError::InvalidShape("inline vector size"))?;
            item.encode_inline(chunk)?;
        }
        Ok(())
    }
}

/// A type that can be read from a buffer with lifetime `'de`, possibly borrowing from it.
///
/// Implement [`absent`](Decode::absent) and the method(s) for the type's
/// [`Wire::SHAPE`]; the defaults of the others fail with [`DecodeError::Invalid`] and are
/// never called for a consistent impl.
///
/// | shape | method | what it does |
/// | --- | --- | --- |
/// | `Inline` | [`decode_inline`](Decode::decode_inline) | parses exactly `size` bytes |
/// | `Table` | [`decode_object`](Decode::decode_object) | `r.table(at)`, one `field` per serializer argument (`r.empty_table(at)` if `serializer(ar)` has no arguments at all) |
/// | `Object` | [`decode_object`](Decode::decode_object) | `r.blob(at)` and parse, or a vector impl |
/// | `Union` | [`decode_union`](Decode::decode_union) | `r.alternative(at)` for the tag's alternative |
/// | `UnionVector` | [`decode_union_vector`](Decode::decode_union_vector) | a vector impl |
///
/// A table whose `serializer(ar)` call has no arguments (such as `Void`) must use
/// [`Reader::empty_table`]: C++ never follows the offset to it. A table whose only
/// argument is an `Arena` does have an argument, so C++ follows the offset and reads its
/// (empty) vtable; it uses [`Reader::table`] with no `field` calls.
///
/// A message's layout can depend on the protocol version (FDB 8.0 removed fields from the
/// middle of some structs); impls can read it from [`Reader::protocol_version`] and
/// [`Writer::protocol_version`], and step over slots they do not model with
/// [`TableReader::skip`](crate::TableReader::skip) and
/// [`TableWriter::skip`](crate::TableWriter::skip).
///
/// Types that never borrow implement `Decode<'de>` for every `'de`; see [`DecodeOwned`].
pub trait Decode<'de>: Wire + Sized {
    /// The value of a field whose slot is absent: what C++ default-constructs (0, false,
    /// empty, an absent `Optional`, a table of absent fields, ...).
    fn absent() -> Self;

    /// Parses an inline value from exactly `size` bytes of its [`Shape::Inline`].
    fn decode_inline(bytes: &'de [u8]) -> Result<Self, DecodeError> {
        let _ = bytes;
        Err(DecodeError::Invalid(
            "decode_inline is not implemented for this type",
        ))
    }

    /// Decodes a table, string, blob or vector from the `u32` offset at position `at`.
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let _ = (r, at);
        Err(DecodeError::Invalid(
            "decode_object is not implemented for this type",
        ))
    }

    /// Decodes a union with nonzero wire `tag` whose offset is at position `at`. A tag
    /// past the last alternative must give [`Decode::absent`] (as C++ does).
    fn decode_union(r: &Reader<'de>, tag: u8, at: usize) -> Result<Self, DecodeError> {
        let _ = (r, tag, at);
        Err(DecodeError::Invalid(
            "decode_union is not implemented for this type",
        ))
    }

    /// Decodes a vector of unions from the positions of its tags and offsets slots.
    fn decode_union_vector(
        r: &Reader<'de>,
        tags_at: usize,
        offsets_at: usize,
    ) -> Result<Self, DecodeError> {
        let _ = (r, tags_at, offsets_at);
        Err(DecodeError::Invalid(
            "decode_union_vector is not implemented for this type",
        ))
    }

    /// Decodes the elements of the vector whose `header` was checked (and charged to the
    /// work limit) by [`Reader::vector_header`] into a `Vec`. `Vec<Self>` calls it.
    ///
    /// The default decodes element by element with
    /// [`decode_element`](Decode::decode_element), reserving at most 64 KiB up front
    /// however large the count is. There is no need to override it unless a run of
    /// elements can be decoded faster than one at a time: the integer, float and `bool`
    /// impls do (a `Vec<u8>` is a memcpy), which is the way around the lack of
    /// specialization. An override must decode exactly what the default would, return
    /// exactly `header.len()` elements (`Vec<T>` fails with [`DecodeError::Invalid`]
    /// otherwise), charge nothing more (inline elements are paid for by the header), and
    /// not reserve more memory than the bytes it reads from the message. The header is
    /// the one [`Reader::vector_header`] returned for `Self`'s vector; the scalar impls
    /// fail if its stride is not their size.
    fn decode_elements(r: &Reader<'de>, header: &VectorHeader) -> Result<Vec<Self>, DecodeError> {
        r.decode_elements(header)
    }

    /// Decodes a vector element stored at position `pos`. The default dispatches on the
    /// shape; `bool` overrides it because `std::vector<bool>` reads any nonzero byte as
    /// `true`, where a scalar `bool` must be 0 or 1.
    fn decode_element(r: &Reader<'de>, pos: usize) -> Result<Self, DecodeError> {
        match Self::SHAPE {
            Shape::Inline { size, .. } => Self::decode_inline(r.inline(pos, size)?),
            Shape::Table | Shape::Object => Self::decode_object(r, pos),
            Shape::Union | Shape::UnionVector => Err(DecodeError::Invalid(
                "a vector element cannot be a union here",
            )),
        }
    }
}

/// A type that decodes without borrowing from the buffer.
pub trait DecodeOwned: for<'de> Decode<'de> {}

impl<T: for<'de> Decode<'de>> DecodeOwned for T {}

// ---------------------------------------------------------------------------------------
// Scalars.

macro_rules! scalar {
    ($($t:ty),*) => {$(
        impl Wire for $t {
            const SHAPE: Shape = Shape::scalar(size_of::<$t>());
        }

        impl Encode for $t {
            fn encode_inline(&self, out: &mut [u8]) -> Result<(), EncodeError> {
                copy_inline(out, &self.to_le_bytes())
            }

            fn encode_inline_slice(items: &[Self], out: &mut [u8]) -> Result<(), EncodeError> {
                if items.len().checked_mul(size_of::<$t>()) != Some(out.len()) {
                    return Err(EncodeError::InvalidShape("inline vector size"));
                }
                for (item, chunk) in items.iter().zip(out.chunks_exact_mut(size_of::<$t>())) {
                    chunk.copy_from_slice(&item.to_le_bytes());
                }
                Ok(())
            }
        }

        impl<'de> Decode<'de> for $t {
            fn absent() -> Self {
                0 as $t
            }

            fn decode_inline(bytes: &'de [u8]) -> Result<Self, DecodeError> {
                let bytes = bytes.try_into().map_err(|_| DecodeError::Invalid("scalar size"))?;
                Ok(<$t>::from_le_bytes(bytes))
            }

            fn decode_elements(
                r: &Reader<'de>,
                header: &VectorHeader,
            ) -> Result<Vec<Self>, DecodeError> {
                // The header was checked: `len * size` bytes are in the message, so the
                // vector is no larger than the bytes it is read from.
                check_stride(header, size_of::<$t>())?;
                let bytes = r.inline(header.first(), header.len().saturating_mul(size_of::<$t>()))?;
                Ok(bytes
                    .chunks_exact(size_of::<$t>())
                    .map(|chunk| {
                        let mut le = [0; size_of::<$t>()];
                        le.copy_from_slice(chunk);
                        <$t>::from_le_bytes(le)
                    })
                    .collect())
            }
        }
    )*};
}

scalar!(i8, u16, i16, u32, i32, u64, i64, f32, f64);

impl Wire for u8 {
    const SHAPE: Shape = Shape::scalar(1);
}

impl Encode for u8 {
    fn encode_inline(&self, out: &mut [u8]) -> Result<(), EncodeError> {
        copy_inline(out, &[*self])
    }

    /// A `Vec<u8>` (a string, a blob) is one copy.
    fn encode_inline_slice(items: &[Self], out: &mut [u8]) -> Result<(), EncodeError> {
        if out.len() != items.len() {
            return Err(EncodeError::InvalidShape("inline vector size"));
        }
        out.copy_from_slice(items);
        Ok(())
    }
}

impl<'de> Decode<'de> for u8 {
    fn absent() -> Self {
        0
    }

    fn decode_inline(bytes: &'de [u8]) -> Result<Self, DecodeError> {
        match bytes {
            [b] => Ok(*b),
            _ => Err(DecodeError::Invalid("scalar size")),
        }
    }

    /// A `Vec<u8>` is one copy of bytes the header showed to be in the message.
    fn decode_elements(r: &Reader<'de>, header: &VectorHeader) -> Result<Vec<Self>, DecodeError> {
        check_stride(header, 1)?;
        Ok(r.inline(header.first(), header.len())?.to_vec())
    }
}

impl Wire for bool {
    const SHAPE: Shape = Shape::scalar(1);
}

impl Encode for bool {
    fn encode_inline(&self, out: &mut [u8]) -> Result<(), EncodeError> {
        copy_inline(out, &[u8::from(*self)])
    }

    fn encode_inline_slice(items: &[Self], out: &mut [u8]) -> Result<(), EncodeError> {
        if out.len() != items.len() {
            return Err(EncodeError::InvalidShape("inline vector size"));
        }
        for (item, byte) in items.iter().zip(out) {
            *byte = u8::from(*item);
        }
        Ok(())
    }
}

impl<'de> Decode<'de> for bool {
    fn absent() -> Self {
        false
    }

    fn decode_inline(bytes: &'de [u8]) -> Result<Self, DecodeError> {
        match bytes {
            [0] => Ok(false),
            [1] => Ok(true),
            [_] => Err(DecodeError::Strict(StrictCheck::BoolValue)),
            _ => Err(DecodeError::Invalid("scalar size")),
        }
    }

    fn decode_element(r: &Reader<'de>, pos: usize) -> Result<Self, DecodeError> {
        Ok(r.byte(pos)? != 0)
    }

    fn decode_elements(r: &Reader<'de>, header: &VectorHeader) -> Result<Vec<Self>, DecodeError> {
        // As `decode_element`: any nonzero byte is `true`.
        check_stride(header, 1)?;
        Ok(r.inline(header.first(), header.len())?
            .iter()
            .map(|&b| b != 0)
            .collect())
    }
}

// ---------------------------------------------------------------------------------------
// Structs (FDB std::tuple).

macro_rules! tuple {
    ($($t:ident $v:ident $i:tt),+) => {
        impl<$($t: Wire),+> Wire for ($($t,)+) {
            const SHAPE: Shape = {
                // flat_buffers.h: static_assert(!is_struct_like<T>, "Nested structs not supported yet").
                assert!($(!$t::STRUCT)&&+, "nested structs are not supported");
                Shape::of_struct(&[$($t::SHAPE),+])
            };
            const STRUCT: bool = true;
        }

        impl<$($t: Encode),+> Encode for ($($t,)+) {
            fn encode_inline(&self, out: &mut [u8]) -> Result<(), EncodeError> {
                let shapes = [$($t::SHAPE),+];
                $(
                    let at = Shape::struct_member_offset(&shapes, $i);
                    let size = $t::SHAPE.field_size();
                    let member = out
                        .get_mut(at..at + size)
                        .ok_or(EncodeError::InvalidShape("struct size"))?;
                    self.$i.encode_inline(member)?;
                )+
                Ok(())
            }
        }

        impl<'de, $($t: Decode<'de>),+> Decode<'de> for ($($t,)+) {
            fn absent() -> Self {
                ($($t::absent(),)+)
            }

            fn decode_inline(bytes: &'de [u8]) -> Result<Self, DecodeError> {
                let shapes = [$($t::SHAPE),+];
                $(
                    let at = Shape::struct_member_offset(&shapes, $i);
                    let size = $t::SHAPE.field_size();
                    let $v = $t::decode_inline(
                        bytes.get(at..at + size).ok_or(DecodeError::Invalid("struct size"))?,
                    )?;
                )+
                Ok(($($v,)+))
            }
        }
    };
}

tuple!(A a 0);
tuple!(A a 0, B b 1);
tuple!(A a 0, B b 1, C c 2);
tuple!(A a 0, B b 1, C c 2, D d 3);
tuple!(A a 0, B b 1, C c 2, D d 3, E e 4);
tuple!(A a 0, B b 1, C c 2, D d 3, E e 4, F f 5);

// ---------------------------------------------------------------------------------------
// References and boxes.

impl<T: Wire + ?Sized> Wire for &T {
    const SHAPE: Shape = T::SHAPE;
    const STRUCT: bool = T::STRUCT;
}

impl<T: Encode + ?Sized> Encode for &T {
    fn encode_inline(&self, out: &mut [u8]) -> Result<(), EncodeError> {
        (**self).encode_inline(out)
    }

    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        (**self).encode_object(w)
    }

    fn encode_union(&self, w: &mut Writer) -> Result<UnionRef, EncodeError> {
        (**self).encode_union(w)
    }

    fn encode_union_vector(&self, w: &mut Writer) -> Result<(ObjectRef, ObjectRef), EncodeError> {
        (**self).encode_union_vector(w)
    }
}

impl<T: Wire + ?Sized> Wire for Box<T> {
    const SHAPE: Shape = T::SHAPE;
    const STRUCT: bool = T::STRUCT;
}

impl<T: Encode + ?Sized> Encode for Box<T> {
    fn encode_inline(&self, out: &mut [u8]) -> Result<(), EncodeError> {
        (**self).encode_inline(out)
    }

    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        (**self).encode_object(w)
    }

    fn encode_union(&self, w: &mut Writer) -> Result<UnionRef, EncodeError> {
        (**self).encode_union(w)
    }

    fn encode_union_vector(&self, w: &mut Writer) -> Result<(ObjectRef, ObjectRef), EncodeError> {
        (**self).encode_union_vector(w)
    }
}

impl<'de, T: Decode<'de>> Decode<'de> for Box<T> {
    fn absent() -> Self {
        Box::new(T::absent())
    }

    fn decode_inline(bytes: &'de [u8]) -> Result<Self, DecodeError> {
        T::decode_inline(bytes).map(Box::new)
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        T::decode_object(r, at).map(Box::new)
    }

    fn decode_union(r: &Reader<'de>, tag: u8, at: usize) -> Result<Self, DecodeError> {
        T::decode_union(r, tag, at).map(Box::new)
    }

    fn decode_union_vector(
        r: &Reader<'de>,
        tags_at: usize,
        offsets_at: usize,
    ) -> Result<Self, DecodeError> {
        T::decode_union_vector(r, tags_at, offsets_at).map(Box::new)
    }

    fn decode_element(r: &Reader<'de>, pos: usize) -> Result<Self, DecodeError> {
        T::decode_element(r, pos).map(Box::new)
    }
}

/// Borrowed bytes (`StringRef`, `std::string`): zero-copy.
impl<'de: 'a, 'a> Decode<'de> for &'a [u8] {
    fn absent() -> Self {
        &[]
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        r.blob(at)
    }
}

// ---------------------------------------------------------------------------------------
// Vectors and arrays.

impl<T: Wire> Wire for [T] {
    const SHAPE: Shape = match T::SHAPE {
        Shape::Union => Shape::UnionVector,
        Shape::UnionVector => panic!("a vector element cannot be a vector of unions"),
        _ => Shape::Object,
    };
}

/// A slice is a vector (`std::vector`, `VectorRef`, ...); a slice of unions is a vector
/// of unions. `[u8]` has the same wire form as a string.
impl<T: Encode> Encode for [T] {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.vector(self)
    }

    fn encode_union_vector(&self, w: &mut Writer) -> Result<(ObjectRef, ObjectRef), EncodeError> {
        w.union_vector(self)
    }
}

impl<T: Wire> Wire for Vec<T> {
    const SHAPE: Shape = <[T]>::SHAPE;
}

impl<T: Encode> Encode for Vec<T> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.vector(self)
    }

    fn encode_union_vector(&self, w: &mut Writer) -> Result<(ObjectRef, ObjectRef), EncodeError> {
        w.union_vector(self)
    }
}

/// An owned vector. `Vec<bool>` reads like `std::vector<bool>` (any nonzero byte is
/// `true`); `Vec<u8>` reads strings as well as `vector<u8>`.
impl<'de, T: Decode<'de>> Decode<'de> for Vec<T> {
    fn absent() -> Self {
        Vec::new()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let header = r.vector_header::<T>(at)?;
        let items = T::decode_elements(r, &header)?;
        // An override that returns another number of elements than the vector has is a
        // bug in it; do not pass its output on as the message's vector.
        if items.len() == header.len() {
            Ok(items)
        } else {
            Err(DecodeError::Invalid("decode_elements length"))
        }
    }

    fn decode_union_vector(
        r: &Reader<'de>,
        tags_at: usize,
        offsets_at: usize,
    ) -> Result<Self, DecodeError> {
        r.union_vector(tags_at, offsets_at)
    }
}

impl<T: Wire, const N: usize> Wire for [T; N] {
    const SHAPE: Shape = {
        assert!(T::SHAPE.is_plain(), "an array element cannot be a union");
        Shape::Object
    };
}

/// `std::array<T, N>`: a vector whose count must be `N`.
impl<T: Encode, const N: usize> Encode for [T; N] {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.vector(self)
    }
}

/// The count must be `N`. Unlike `Vec<bool>`, the `bool`s of a `[bool; N]` must be 0 or 1
/// (the `bool_value` strict check), as a scalar `bool` field is.
impl<'de, T: Decode<'de>, const N: usize> Decode<'de> for [T; N] {
    fn absent() -> Self {
        core::array::from_fn(|_| T::absent())
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let items = r.vector(at, Vec::push, |count| {
            if count == N {
                Ok(Vec::with_capacity(preallocation::<T>(N)))
            } else {
                Err(DecodeError::ArrayLength {
                    expected: N,
                    found: count as u32,
                })
            }
        })?;
        items
            .try_into()
            .map_err(|_| DecodeError::Invalid("array length"))
    }
}

// ---------------------------------------------------------------------------------------
// Optional.

impl<T: Wire> Wire for Option<T> {
    const SHAPE: Shape = {
        assert!(
            T::SHAPE.is_plain(),
            "a union alternative cannot be a union; use EnsureTable"
        );
        Shape::Union
    };
}

/// `Optional<T>`: a one-alternative union, empty when `None`.
impl<T: Encode> Encode for Option<T> {
    fn encode_union(&self, w: &mut Writer) -> Result<UnionRef, EncodeError> {
        match self {
            None => Ok(UnionRef::EMPTY),
            Some(value) => w.union_value(1, value),
        }
    }
}

impl<'de, T: Decode<'de>> Decode<'de> for Option<T> {
    fn absent() -> Self {
        None
    }

    fn decode_union(r: &Reader<'de>, tag: u8, at: usize) -> Result<Self, DecodeError> {
        match tag {
            1 => r.alternative(at).map(Some),
            _ => Ok(None),
        }
    }
}

/// Fails unless `header` describes a vector whose elements are `size` bytes apart: the
/// bulk scalar [`Decode::decode_elements`] impls read `size`-byte elements, and a header
/// of another type's vector (which only a caller of the hook can pass) would make them
/// read a different number of bytes than the header showed to be in the message.
fn check_stride(header: &VectorHeader, size: usize) -> Result<(), DecodeError> {
    if header.stride() == size {
        Ok(())
    } else {
        Err(DecodeError::Invalid("decode_elements stride"))
    }
}

/// Copies an inline value's bytes into `out`, which must be exactly as long.
fn copy_inline(out: &mut [u8], bytes: &[u8]) -> Result<(), EncodeError> {
    if out.len() != bytes.len() {
        return Err(EncodeError::InvalidShape("inline size"));
    }
    out.copy_from_slice(bytes);
    Ok(())
}

/// Largest number of bytes a decoder reserves for a vector before decoding its elements.
const MAX_PREALLOCATION: usize = 64 * 1024;

/// How many elements to reserve for a vector of `count` elements: the count comes from
/// the message, and an element can be much larger in memory than on the wire, so reserve
/// at most [`MAX_PREALLOCATION`] bytes and let the vector grow as elements decode.
pub(crate) fn preallocation<T>(count: usize) -> usize {
    count.min(MAX_PREALLOCATION / size_of::<T>().max(1))
}

/// How many elements to reserve in a hash container for a vector of `count` elements.
/// A hash table allocates its capacity times 8/7 rounded up to a power of two, plus a
/// control byte per bucket, so this reserves a third of what [`preallocation`] would
/// (counting the control byte) to keep the allocation within [`MAX_PREALLOCATION`].
pub(crate) fn hash_preallocation<T>(count: usize) -> usize {
    count.min(MAX_PREALLOCATION / (3 * (size_of::<T>() + 1)))
}
