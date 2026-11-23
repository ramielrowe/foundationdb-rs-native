//! The [`Tuple`] container and the [`TuplePack`] conversion trait.

use std::borrow::Cow;
use std::ops::Deref;

use crate::element::Element;
use crate::encode::encode;
use crate::error::{Error, Result};

/// An ordered sequence of [`Element`]s.
///
/// `Tuple` dereferences to `[Element]`, so slice methods such as `len`,
/// `iter` and `get` are available. Its `Ord` matches the byte order of the
/// packed form (lexicographic by element, shorter prefix first).
///
/// ```
/// use fdb_native_tuple::{Element, Tuple};
///
/// let mut t = Tuple::new();
/// t.push("users");
/// t.push(42);
/// assert_eq!(t.len(), 2);
/// assert_eq!(t[1], Element::Int(42));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Tuple(Vec<Element>);

impl Tuple {
    /// Creates an empty tuple.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a tuple from a vector of elements.
    pub fn from_elements(elements: Vec<Element>) -> Self {
        Self(elements)
    }

    /// Appends an element.
    pub fn push(&mut self, element: impl Into<Element>) {
        self.0.push(element.into());
    }

    /// Returns the tuple with `element` appended; convenient for chaining.
    #[must_use]
    pub fn with(mut self, element: impl Into<Element>) -> Self {
        self.push(element);
        self
    }

    /// Returns the elements as a slice.
    pub fn as_slice(&self) -> &[Element] {
        &self.0
    }

    /// Consumes the tuple, returning its elements.
    pub fn into_vec(self) -> Vec<Element> {
        self.0
    }

    /// Returns `true` if any element, including inside nested tuples, is an
    /// incomplete versionstamp.
    pub fn has_incomplete_versionstamp(&self) -> bool {
        self.0.iter().any(|e| match e {
            Element::Versionstamp(v) => !v.is_complete(),
            Element::Tuple(t) => t.has_incomplete_versionstamp(),
            _ => false,
        })
    }

    /// Packs the tuple. See [`pack`](crate::pack).
    pub fn pack(&self) -> Result<Vec<u8>> {
        crate::pack(self)
    }

    /// Packs a tuple containing one incomplete versionstamp. See
    /// [`pack_with_versionstamp`](crate::pack_with_versionstamp).
    pub fn pack_with_versionstamp(&self) -> Result<Vec<u8>> {
        crate::pack_with_versionstamp(self)
    }

    /// Returns the key range of all tuples that have this tuple as a strict
    /// prefix. See [`range`](crate::range).
    pub fn range(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        crate::range(self)
    }

    /// Unpacks a tuple. See [`unpack`](crate::unpack).
    pub fn unpack(bytes: &[u8]) -> Result<Self> {
        crate::unpack(bytes)
    }
}

impl Deref for Tuple {
    type Target = [Element];

    fn deref(&self) -> &[Element] {
        &self.0
    }
}

impl From<Vec<Element>> for Tuple {
    fn from(v: Vec<Element>) -> Self {
        Self(v)
    }
}

impl IntoIterator for Tuple {
    type Item = Element;
    type IntoIter = std::vec::IntoIter<Element>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Tuple {
    type Item = &'a Element;
    type IntoIter = std::slice::Iter<'a, Element>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<E: Into<Element>> FromIterator<E> for Tuple {
    fn from_iter<I: IntoIterator<Item = E>>(iter: I) -> Self {
        Self(iter.into_iter().map(Into::into).collect())
    }
}

impl<E: Into<Element>> Extend<E> for Tuple {
    fn extend<I: IntoIterator<Item = E>>(&mut self, iter: I) {
        self.0.extend(iter.into_iter().map(Into::into));
    }
}

/// Builds a [`Tuple`] from a list of values convertible to [`Element`].
///
/// ```
/// use fdb_native_tuple::{tuple, Element};
///
/// let t = tuple!["a", 1, true];
/// assert_eq!(t.len(), 3);
/// assert_eq!(t[0], Element::String("a".into()));
/// ```
#[macro_export]
macro_rules! tuple {
    ($($e:expr),* $(,)?) => {
        $crate::Tuple::from_elements(::std::vec![$($crate::Element::from($e)),*])
    };
}

/// Types that can be viewed as a sequence of tuple elements for packing.
///
/// Implemented for [`Tuple`], `[Element]`, `Vec<Element>`, `()` (the empty
/// tuple) and Rust tuples of up to twelve values that are `Clone + Into<Element>`.
///
/// ```
/// use fdb_native_tuple::pack;
///
/// assert_eq!(pack(&("a", 1)).unwrap(), b"\x02a\x00\x15\x01");
/// ```
pub trait TuplePack {
    /// Returns the elements, borrowed when already stored as a slice.
    fn to_elements(&self) -> Cow<'_, [Element]>;
}

impl TuplePack for Tuple {
    fn to_elements(&self) -> Cow<'_, [Element]> {
        Cow::Borrowed(&self.0)
    }
}

impl TuplePack for [Element] {
    fn to_elements(&self) -> Cow<'_, [Element]> {
        Cow::Borrowed(self)
    }
}

impl TuplePack for Vec<Element> {
    fn to_elements(&self) -> Cow<'_, [Element]> {
        Cow::Borrowed(self)
    }
}

impl TuplePack for () {
    fn to_elements(&self) -> Cow<'_, [Element]> {
        Cow::Borrowed(&[])
    }
}

impl<T: TuplePack + ?Sized> TuplePack for &T {
    fn to_elements(&self) -> Cow<'_, [Element]> {
        (**self).to_elements()
    }
}

macro_rules! tuple_pack_impl {
    ($($name:ident $idx:tt),+) => {
        impl<$($name: Clone + Into<Element>),+> TuplePack for ($($name,)+) {
            fn to_elements(&self) -> Cow<'_, [Element]> {
                Cow::Owned(vec![$(self.$idx.clone().into()),+])
            }
        }
    };
}

tuple_pack_impl!(A 0);
tuple_pack_impl!(A 0, B 1);
tuple_pack_impl!(A 0, B 1, C 2);
tuple_pack_impl!(A 0, B 1, C 2, D 3);
tuple_pack_impl!(A 0, B 1, C 2, D 3, E 4);
tuple_pack_impl!(A 0, B 1, C 2, D 3, E 4, F 5);
tuple_pack_impl!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
tuple_pack_impl!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
tuple_pack_impl!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
tuple_pack_impl!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
tuple_pack_impl!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
tuple_pack_impl!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);

/// Packs `elements` after `prefix`, rejecting incomplete versionstamps.
pub(crate) fn pack_prefixed(prefix: &[u8], elements: &[Element]) -> Result<Vec<u8>> {
    Ok(encode(prefix, elements, false)?.bytes)
}

/// Packs `elements` after `prefix`, requiring exactly one incomplete
/// versionstamp, and appends its 4-byte little-endian offset.
pub(crate) fn pack_prefixed_with_versionstamp(
    prefix: &[u8],
    elements: &[Element],
) -> Result<Vec<u8>> {
    let encoded = encode(prefix, elements, true)?;
    let pos = encoded
        .incomplete_pos
        .ok_or(Error::NoIncompleteVersionstamp)?;
    let offset =
        u32::try_from(pos).map_err(|_| Error::VersionstampOffsetOverflow { offset: pos })?;
    let mut bytes = encoded.bytes;
    bytes.extend_from_slice(&offset.to_le_bytes());
    Ok(bytes)
}
