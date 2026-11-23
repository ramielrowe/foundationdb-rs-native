//! A lazy vector: the elements are decoded as they are asked for.

use core::fmt;
use core::iter::FusedIterator;
use core::marker::PhantomData;

use crate::codec::preallocation;
use crate::reader::Detached;
use crate::{Decode, DecodeError, Reader, Shape, VectorHeader, Wire};

/// A vector whose elements are decoded when they are read, not when the vector is.
///
/// As a field type it takes the place of `Vec<T>`: the wire form is the same
/// (`std::vector<T>`, `VectorRef<T>`, ...), but decoding the table that holds it only
/// checks the vector's header, so a large reply can be walked one element at a time
/// without ever holding all of it in memory as `T`s, or an element can be fetched by index.
///
/// ```
/// use fdb_native_wire::{encode_with_identifier, DecodeError, Reader, VectorView};
///
/// let bytes = encode_with_identifier(&vec![5u32, 6, 7], 1)?;
/// let reader = Reader::new(&bytes)?;
/// let view: VectorView<u32> = reader.decode_root()?;
/// assert_eq!(view.len(), 3);
/// assert_eq!(view.get(1)?, 6);
/// assert_eq!(view.get(3), Err(DecodeError::IndexOutOfRange { index: 3, len: 3 }));
/// let sum: u32 = view.iter().sum::<Result<u32, _>>()?;
/// assert_eq!(sum, 18);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Safety against hostile messages
///
/// Reading is as strict as eager decoding, because it is the same code:
///
/// - **Bounds and strictness.** The header (offset, count, and that `count` elements fit
///   in the message) is checked when the view is decoded. Each element is checked when it
///   is read, with every check [`Reader`] makes (offsets, vtables, bool bytes, nesting
///   depth, ...), so a bad element is an `Err` from [`VectorView::get`] or the iterator at
///   that element, never a panic and never a read outside the message. Elements that are
///   never read are never checked: a view that decoded proves only that its header is
///   sound. Run [`iter`](VectorView::iter) to the end if you need the whole vector
///   validated.
/// - **Work limit.** The view shares the [work budget](Reader#work-limit) of the reader
///   that decoded it. Decoding the view charges the header (4 + count × element wire size,
///   as for a `Vec`); reading an element charges what decoding that element costs,
///   **every time it is read**. A message that aliases one large object from many
///   elements, or nests aliases, therefore stops with [`DecodeError::WorkLimit`] after the
///   same total work as an eager decode however the elements are visited, and re-reading
///   an element over and over cannot do unbounded work either. The price: a loop that
///   re-reads one table element spends the budget (about the element's size each time)
///   and ends in `WorkLimit` after roughly 4 × the message length units, so keep a
///   decoded element if you need it again. Scalar elements are paid for by the header and
///   can be re-read freely. Calling [`Reader::root`] again refills the budget.
/// - **No allocation from the count.** The view holds the header and a handle on the
///   message, nothing sized by the count, and the lower bound of the iterator's
///   `size_hint` is capped at 64 KiB of results, so `collect` on a message claiming
///   billions of elements reserves little and allocates only as elements actually decode.
/// - **Depth.** Elements are decoded at the nesting depth of the table that held the view,
///   so the limit of [`MAX_DEPTH`](crate::MAX_DEPTH) tables applies as in an eager decode.
///
/// The elements must not be unions (use `Vec` for a vector of unions). A view is `Send`
/// and `Sync` and cheap to clone (clones share the message and the budget). It is decode
/// only.
pub struct VectorView<'de, T> {
    /// What element reads continue from; `None` for an empty or absent vector.
    source: Option<Detached<'de>>,
    header: VectorHeader,
    items: PhantomData<fn() -> T>,
}

impl<T> Clone for VectorView<'_, T> {
    fn clone(&self) -> Self {
        VectorView {
            source: self.source.clone(),
            header: self.header,
            items: PhantomData,
        }
    }
}

impl<T> fmt::Debug for VectorView<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorView")
            .field("len", &self.header.len())
            .finish_non_exhaustive()
    }
}

impl<'de, T: Decode<'de>> VectorView<'de, T> {
    /// The number of elements, as the message states it (checked to fit in the message).
    pub fn len(&self) -> usize {
        self.header.len()
    }

    /// Whether the vector has no elements.
    pub fn is_empty(&self) -> bool {
        self.header.is_empty()
    }

    /// Decodes element `index`.
    ///
    /// # Errors
    ///
    /// [`DecodeError::IndexOutOfRange`] if `index >= len()`, [`DecodeError::WorkLimit`] if
    /// the work budget is spent, and any error decoding the element.
    pub fn get(&self, index: usize) -> Result<T, DecodeError> {
        let (Some(pos), Some(source)) = (self.header.position(index), &self.source) else {
            return Err(DecodeError::IndexOutOfRange {
                index,
                len: self.len(),
            });
        };
        T::decode_element(&source.reader(), pos)
    }

    /// An iterator over the elements, decoding each as it is reached. It ends after the
    /// first error.
    pub fn iter(&self) -> VectorIter<'_, 'de, T> {
        VectorIter {
            view: self,
            reader: self.source.as_ref().map(Detached::reader),
            next: 0,
            failed: false,
        }
    }
}

impl<'v, 'de, T: Decode<'de>> IntoIterator for &'v VectorView<'de, T> {
    type Item = Result<T, DecodeError>;
    type IntoIter = VectorIter<'v, 'de, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: Wire> Wire for VectorView<'_, T> {
    const SHAPE: Shape = {
        assert!(
            T::SHAPE.is_plain(),
            "a VectorView element cannot be a union; use Vec"
        );
        Shape::Object
    };
}

impl<'de, T: Decode<'de>> Decode<'de> for VectorView<'de, T> {
    fn absent() -> Self {
        VectorView {
            source: None,
            header: VectorHeader::EMPTY,
            items: PhantomData,
        }
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let header = r.vector_header::<T>(at)?;
        Ok(VectorView {
            source: (!header.is_empty()).then(|| r.detach()),
            header,
            items: PhantomData,
        })
    }
}

/// The iterator of a [`VectorView`]; see [`VectorView::iter`].
pub struct VectorIter<'v, 'de, T> {
    view: &'v VectorView<'de, T>,
    reader: Option<Reader<'de>>,
    next: usize,
    failed: bool,
}

impl<T> fmt::Debug for VectorIter<'_, '_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorIter")
            .field("next", &self.next)
            .field("len", &self.view.header.len())
            .finish_non_exhaustive()
    }
}

impl<'de, T: Decode<'de>> Iterator for VectorIter<'_, 'de, T> {
    type Item = Result<T, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let pos = self.view.header.position(self.next)?;
        self.next += 1;
        let item = match &self.reader {
            Some(reader) => T::decode_element(reader, pos),
            None => Err(DecodeError::Invalid("vector view without a message")),
        };
        self.failed = item.is_err();
        Some(item)
    }

    /// The upper bound is the number of elements left; the lower bound is capped (at 64 KiB
    /// of results) so that collecting a hostile count cannot reserve more.
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.failed {
            return (0, Some(0));
        }
        let left = self.view.header.len().saturating_sub(self.next);
        (preallocation::<Result<T, DecodeError>>(left), Some(left))
    }
}

impl<'de, T: Decode<'de>> FusedIterator for VectorIter<'_, 'de, T> {}
