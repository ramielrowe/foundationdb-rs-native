//! The codec traits and their impls for standard types.

use crate::{DecodeError, EncodeError, ObjectRef, Reader, Shape, StrictCheck, UnionRef, Writer};

/// A type with a wire representation; states its [`Shape`].
///
/// [`Encode`] and [`Decode`] dispatch on the shape: a field, vector element or union
/// alternative of shape [`Shape::Inline`] goes through `encode_inline`/`decode_inline`,
/// [`Shape::Table`] and [`Shape::Object`] through `encode_object`/`decode_object`, and so
/// on. Each impl overrides exactly the methods its shape uses.
pub trait Wire {
    /// How values of this type are stored.
    const SHAPE: Shape;
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
    fn encode_inline(&self, out: &mut [u8]) {
        let _ = out;
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
/// | `Table` | [`decode_object`](Decode::decode_object) | `r.table(at)`, one `field` per serializer argument (or `r.empty_table(at)` with no fields) |
/// | `Object` | [`decode_object`](Decode::decode_object) | `r.blob(at)` and parse, or a vector impl |
/// | `Union` | [`decode_union`](Decode::decode_union) | `r.alternative(at)` for the tag's alternative |
/// | `UnionVector` | [`decode_union_vector`](Decode::decode_union_vector) | a vector impl |
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
            fn encode_inline(&self, out: &mut [u8]) {
                out.copy_from_slice(&self.to_le_bytes());
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
        }
    )*};
}

scalar!(u8, i8, u16, i16, u32, i32, u64, i64, f32, f64);

impl Wire for bool {
    const SHAPE: Shape = Shape::scalar(1);
}

impl Encode for bool {
    fn encode_inline(&self, out: &mut [u8]) {
        out.fill(u8::from(*self));
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
}

// ---------------------------------------------------------------------------------------
// Structs (FDB std::tuple).

macro_rules! tuple {
    ($($t:ident $v:ident $i:tt),+) => {
        impl<$($t: Wire),+> Wire for ($($t,)+) {
            const SHAPE: Shape = Shape::of_struct(&[$($t::SHAPE),+]);
        }

        impl<$($t: Encode),+> Encode for ($($t,)+) {
            fn encode_inline(&self, out: &mut [u8]) {
                const { assert!(matches!(Self::SHAPE, Shape::Inline { .. })) };
                let shapes = [$($t::SHAPE),+];
                $(
                    let at = Shape::struct_member_offset(&shapes, $i);
                    let size = $t::SHAPE.field_size();
                    if let Some(out) = out.get_mut(at..at + size) {
                        self.$i.encode_inline(out);
                    }
                )+
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
}

impl<T: Encode + ?Sized> Encode for &T {
    fn encode_inline(&self, out: &mut [u8]) {
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
}

impl<T: Encode + ?Sized> Encode for Box<T> {
    fn encode_inline(&self, out: &mut [u8]) {
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
    const SHAPE: Shape = if T::SHAPE.is_union() {
        Shape::UnionVector
    } else {
        Shape::Object
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
        r.vector(at, Vec::push, |count| Ok(Vec::with_capacity(count)))
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
    const SHAPE: Shape = Shape::Object;
}

/// `std::array<T, N>`: a vector whose count must be `N`.
impl<T: Encode, const N: usize> Encode for [T; N] {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.vector(self)
    }
}

impl<'de, T: Decode<'de>, const N: usize> Decode<'de> for [T; N] {
    fn absent() -> Self {
        core::array::from_fn(|_| T::absent())
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let items = r.vector(at, Vec::push, |count| {
            if count == N {
                Ok(Vec::with_capacity(N))
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
    const SHAPE: Shape = Shape::Union;
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
