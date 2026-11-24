//! `KeyValueRef` and the pre-serialized key-value vector.

use core::iter::FusedIterator;

use bytes::Bytes;

use super::Cursor;
use crate::{Decode, DecodeError, Encode, EncodeError, ObjectRef, Reader, Shape, Wire, Writer};

/// FDB's `KeyValueRef`, borrowing its key and value.
///
/// Outside a pre-serialized vector (see [`KeyValueVectorRef`]) it is a table with two
/// string fields, `serializer(ar, key, value)`; that is its [`Wire`] form.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyValueRef<'a> {
    /// The key.
    pub key: &'a [u8],
    /// The value.
    pub value: &'a [u8],
}

impl<'a> KeyValueRef<'a> {
    /// A pair of borrowed bytes.
    pub const fn new(key: &'a [u8], value: &'a [u8]) -> KeyValueRef<'a> {
        KeyValueRef { key, value }
    }

    /// Copies the key and value (`KeyValue`, a `Standalone<KeyValueRef>`).
    pub fn into_owned(self) -> KeyValue {
        KeyValue {
            key: Bytes::copy_from_slice(self.key),
            value: Bytes::copy_from_slice(self.value),
        }
    }
}

impl Wire for KeyValueRef<'_> {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for KeyValueRef<'_> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.key)?;
        t.field(&self.value)?;
        t.finish()
    }
}

impl<'de: 'a, 'a> Decode<'de> for KeyValueRef<'a> {
    fn absent() -> Self {
        KeyValueRef::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(KeyValueRef {
            key: t.field()?,
            value: t.field()?,
        })
    }
}

/// FDB's `KeyValue` (`Standalone<KeyValueRef>`): a key and a value that own their bytes.
///
/// The bytes are [`Bytes`]: reference-counted, and possibly a slice of a larger shared
/// buffer (the rows a [`SharedKeyValueVector`] gives are slices of one reply, as C++'s rows
/// point into the reply's arena). Cloning a pair is two reference counts, not a copy. A pair
/// keeps the whole buffer it is a slice of alive: for a range read's row, its whole reply,
/// which is about 80 kB (the client's `REPLY_BYTE_LIMIT`) but soft, so up to that plus the
/// row that crosses the limit (about 190 kB with a 10 kB key and a 100 kB value). Copy it
/// ([`Bytes::copy_from_slice`]) to keep a few rows of a large read for long.
///
/// ```
/// use fdb_native_wire::KeyValue;
///
/// let kv = KeyValue::new(b"key".to_vec(), &b"value"[..]);
/// assert_eq!(kv.key, b"key"[..]);
/// assert_eq!(&kv.value[..], b"value");
/// assert_eq!(kv.borrowed().value, b"value");
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyValue {
    /// The key.
    pub key: Bytes,
    /// The value.
    pub value: Bytes,
}

impl KeyValue {
    /// A pair of these bytes (a `Vec<u8>`, a `&'static [u8]`, a `String` or a [`Bytes`]
    /// are taken without a copy; [`KeyValueRef::into_owned`] copies borrowed ones).
    pub fn new(key: impl Into<Bytes>, value: impl Into<Bytes>) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Borrows the pair.
    pub fn borrowed(&self) -> KeyValueRef<'_> {
        KeyValueRef {
            key: &self.key,
            value: &self.value,
        }
    }
}

impl From<KeyValueRef<'_>> for KeyValue {
    fn from(kv: KeyValueRef<'_>) -> KeyValue {
        kv.into_owned()
    }
}

impl Wire for KeyValue {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for KeyValue {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        self.borrowed().encode_object(w)
    }
}

impl<'de> Decode<'de> for KeyValue {
    fn absent() -> Self {
        KeyValue::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        KeyValueRef::decode_object(r, at).map(KeyValueRef::into_owned)
    }
}

// ---------------------------------------------------------------------------------------
// The pre-serialized vector.

/// The payload of an empty vector: a zero count.
const EMPTY_PAYLOAD: [u8; 4] = [0; 4];

const TRUNCATED: &str = "preserialized KeyValue vector is truncated";

/// A pre-serialized `VectorRef<KeyValueRef, VecSerStrategy::String>` (used by
/// `GetKeyValuesReply::data`): a view of the validated payload of one blob, borrowing it
/// from the message. Nothing is copied or allocated; [`iter`](Self::iter) walks the
/// payload. [`KeyValueVector`] is the owned form.
///
/// The payload is a `u32` item count, then per item a `u32` key length, the key, a `u32`
/// value length and the value, with no padding or alignment. It is the same on 7.4 and
/// 8.0. An empty vector is a 4-byte blob holding a zero count (not the shared empty
/// blob), which is what [`encode_object`](Encode::encode_object) writes for it.
///
/// FDB's loader trusts the lengths and only `ASSERT`s afterwards that they add up to the
/// blob; [`KeyValueVectorRef::parse`] and the [`Decode`] impl check them first and reject
/// a count the blob cannot hold, an item that overruns the blob, a blob shorter than its
/// count, and trailing bytes (and a blob shorter than 4 bytes, such as the shared empty
/// blob, which FDB never writes for this type). An absent field is the empty vector.
/// Empty keys and values are fine.
///
/// The format has one encoding per value, so equality compares payload bytes and
/// re-encoding a decoded vector is byte-identical.
///
/// ```
/// use fdb_native_wire::{KeyValueRef, KeyValueVector, KeyValueVectorRef};
///
/// let mut buf = KeyValueVector::new();
/// buf.try_push(b"a", b"1")?;
/// buf.try_push(b"b", b"")?;
/// let view = KeyValueVectorRef::parse(buf.payload())?;
/// assert_eq!(view.len(), 2);
/// assert_eq!(view.iter().last(), Some(KeyValueRef::new(b"b", b"")));
/// assert_eq!(view.into_owned(), buf);
/// assert!(KeyValueVectorRef::parse(&buf.payload()[1..]).is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct KeyValueVectorRef<'a> {
    /// The whole payload, count included; validated.
    payload: &'a [u8],
    count: u32,
}

impl<'a> KeyValueVectorRef<'a> {
    /// The empty vector.
    pub const EMPTY: KeyValueVectorRef<'static> = KeyValueVectorRef {
        payload: &EMPTY_PAYLOAD,
        count: 0,
    };

    /// Validates a blob payload (without the string framing) and views it.
    pub fn parse(payload: &'a [u8]) -> Result<KeyValueVectorRef<'a>, DecodeError> {
        let mut c = Cursor::new(payload, TRUNCATED);
        let count = c.u32()?;
        // Every item takes at least its two length prefixes; this also bounds the loop
        // by the blob's size.
        if u64::from(count) * 8 > c.remaining() as u64 {
            return Err(DecodeError::Invalid(
                "preserialized KeyValue vector count exceeds the blob",
            ));
        }
        for _ in 0..count {
            for _ in 0..2 {
                let len = c.u32()?;
                c.take(usize::try_from(len).map_err(|_| DecodeError::Invalid(TRUNCATED))?)?;
            }
        }
        if !c.is_empty() {
            return Err(DecodeError::Invalid(
                "preserialized KeyValue vector has trailing bytes",
            ));
        }
        Ok(KeyValueVectorRef { payload, count })
    }

    /// The number of pairs.
    pub fn len(&self) -> usize {
        self.count as usize
    }

    /// Whether there are no pairs.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The blob payload, the count included.
    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }

    /// The pairs in order, borrowing from the payload.
    pub fn iter(&self) -> KeyValueIter<'a> {
        KeyValueIter {
            rest: self.payload.get(4..).unwrap_or_default(),
            remaining: self.count,
        }
    }

    /// Copies the payload.
    pub fn into_owned(self) -> KeyValueVector {
        KeyValueVector {
            bytes: self.payload.to_vec(),
            count: self.count,
        }
    }
}

impl Default for KeyValueVectorRef<'_> {
    fn default() -> Self {
        KeyValueVectorRef::EMPTY
    }
}

impl<'a> IntoIterator for KeyValueVectorRef<'a> {
    type Item = KeyValueRef<'a>;
    type IntoIter = KeyValueIter<'a>;

    fn into_iter(self) -> KeyValueIter<'a> {
        self.iter()
    }
}

impl<'a> IntoIterator for &KeyValueVectorRef<'a> {
    type Item = KeyValueRef<'a>;
    type IntoIter = KeyValueIter<'a>;

    fn into_iter(self) -> KeyValueIter<'a> {
        self.iter()
    }
}

/// The pairs of a [`KeyValueVectorRef`].
#[derive(Clone, Debug)]
pub struct KeyValueIter<'a> {
    rest: &'a [u8],
    remaining: u32,
}

impl<'a> Iterator for KeyValueIter<'a> {
    type Item = KeyValueRef<'a>;

    fn next(&mut self) -> Option<KeyValueRef<'a>> {
        if self.remaining == 0 {
            return None;
        }
        let mut c = Cursor::new(self.rest, TRUNCATED);
        let mut field = || {
            let len = usize::try_from(c.u32().ok()?).ok()?;
            c.take(len).ok()
        };
        // The payload was validated; a failure here would be a bug, and ends the
        // iteration rather than panicking.
        let item = field().and_then(|key| Some(KeyValueRef::new(key, field()?)));
        match item {
            Some(item) => {
                self.remaining -= 1;
                self.rest = self
                    .rest
                    .get(self.rest.len() - c.remaining()..)
                    .unwrap_or_default();
                Some(item)
            }
            None => {
                self.remaining = 0;
                None
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.remaining as usize;
        (n, Some(n))
    }
}

impl ExactSizeIterator for KeyValueIter<'_> {}

impl FusedIterator for KeyValueIter<'_> {}

impl Wire for KeyValueVectorRef<'_> {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for KeyValueVectorRef<'_> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.blob(self.payload)
    }
}

impl<'de: 'a, 'a> Decode<'de> for KeyValueVectorRef<'a> {
    fn absent() -> Self {
        KeyValueVectorRef::EMPTY
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        KeyValueVectorRef::parse(r.blob(at)?)
    }
}

/// An owned pre-serialized key-value vector, for building replies (and for decoding
/// without borrowing). It holds the encoded payload (so it is as large as the blob, and
/// encoding it is a copy); [`borrowed`](Self::borrowed) views it as a
/// [`KeyValueVectorRef`].
///
/// ```
/// use fdb_native_wire::{KeyValueRef, KeyValueVector};
///
/// let buf = KeyValueVector::from_pairs([KeyValueRef::new(b"k", b"v"), KeyValueRef::new(b"k2", b"")])?;
/// assert_eq!(buf.len(), 2);
/// assert_eq!(buf.payload().len(), 4 + (8 + 2) + (8 + 2));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct KeyValueVector {
    /// The payload with the count kept up to date in its first 4 bytes.
    bytes: Vec<u8>,
    count: u32,
}

impl Default for KeyValueVector {
    fn default() -> Self {
        KeyValueVector::new()
    }
}

impl KeyValueVector {
    /// An empty vector.
    pub fn new() -> KeyValueVector {
        KeyValueVector {
            bytes: EMPTY_PAYLOAD.to_vec(),
            count: 0,
        }
    }

    /// Appends a pair. Fails with [`EncodeError::TooLarge`], leaving the vector as it was,
    /// if a length or the count does not fit a `u32` or the blob would exceed what a
    /// message can hold (`u32::MAX` bytes).
    pub fn try_push(&mut self, key: &[u8], value: &[u8]) -> Result<(), EncodeError> {
        let (Ok(key_len), Ok(value_len), Some(count)) = (
            u32::try_from(key.len()),
            u32::try_from(value.len()),
            self.count.checked_add(1),
        ) else {
            return Err(EncodeError::TooLarge);
        };
        let new_len = self
            .bytes
            .len()
            .checked_add(8)
            .and_then(|n| n.checked_add(key.len()))
            .and_then(|n| n.checked_add(value.len()))
            .filter(|&n| u32::try_from(n).is_ok())
            .ok_or(EncodeError::TooLarge)?;
        self.bytes.reserve(new_len - self.bytes.len());
        self.bytes.extend_from_slice(&key_len.to_le_bytes());
        self.bytes.extend_from_slice(key);
        self.bytes.extend_from_slice(&value_len.to_le_bytes());
        self.bytes.extend_from_slice(value);
        self.count = count;
        if let Some(header) = self.bytes.first_chunk_mut::<4>() {
            *header = count.to_le_bytes();
        }
        Ok(())
    }

    /// A vector of these pairs, or [`EncodeError::TooLarge`] as for
    /// [`try_push`](Self::try_push).
    pub fn from_pairs<'a>(
        pairs: impl IntoIterator<Item = KeyValueRef<'a>>,
    ) -> Result<KeyValueVector, EncodeError> {
        let mut out = KeyValueVector::new();
        for kv in pairs {
            out.try_push(kv.key, kv.value)?;
        }
        Ok(out)
    }

    /// The number of pairs.
    pub fn len(&self) -> usize {
        self.count as usize
    }

    /// Whether there are no pairs.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The blob payload, the count included.
    pub fn payload(&self) -> &[u8] {
        &self.bytes
    }

    /// The blob payload.
    pub fn into_payload(self) -> Vec<u8> {
        self.bytes
    }

    /// Views the pairs.
    pub fn borrowed(&self) -> KeyValueVectorRef<'_> {
        KeyValueVectorRef {
            payload: &self.bytes,
            count: self.count,
        }
    }

    /// The pairs in order.
    pub fn iter(&self) -> KeyValueIter<'_> {
        self.borrowed().iter()
    }
}

impl From<KeyValueVectorRef<'_>> for KeyValueVector {
    fn from(view: KeyValueVectorRef<'_>) -> Self {
        view.into_owned()
    }
}

impl Wire for KeyValueVector {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for KeyValueVector {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        self.borrowed().encode_object(w)
    }
}

impl<'de> Decode<'de> for KeyValueVector {
    fn absent() -> Self {
        KeyValueVector::new()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        KeyValueVectorRef::decode_object(r, at).map(KeyValueVectorRef::into_owned)
    }
}

/// A validated pre-serialized key-value vector in a shared buffer ([`Bytes`]), for keeping
/// a decoded reply's rows without copying them one by one: [`iter`](Self::iter) borrows the
/// pairs, and [`shared`](Self::shared) gives them as [`KeyValue`]s that are slices of the
/// buffer (a reference count each, no allocation).
///
/// [`slice_of`](Self::slice_of) takes the payload out of the received message's buffer
/// without a copy; [`copy_from`](Self::copy_from) copies it once.
///
/// ```
/// use bytes::Bytes;
/// use fdb_native_wire::{KeyValueRef, KeyValueVector, KeyValueVectorRef, SharedKeyValueVector};
///
/// let message = Bytes::from(KeyValueVector::from_pairs([KeyValueRef::new(b"a", b"1")])?.into_payload());
/// let view = KeyValueVectorRef::parse(&message)?;
/// let rows = SharedKeyValueVector::slice_of(&message, view);
/// let kv = rows.shared().next().unwrap();
/// assert_eq!((&kv.key[..], &kv.value[..]), (&b"a"[..], &b"1"[..]));
/// // The row is a slice of the message, not a copy.
/// assert_eq!(kv.key.as_ptr(), message[8..].as_ptr());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SharedKeyValueVector {
    /// The whole payload, count included; validated.
    payload: Bytes,
    count: u32,
}

impl Default for SharedKeyValueVector {
    fn default() -> Self {
        SharedKeyValueVector {
            payload: Bytes::from_static(&EMPTY_PAYLOAD),
            count: 0,
        }
    }
}

impl SharedKeyValueVector {
    /// The pairs of `view`, which borrows from `buffer`: the payload becomes a slice of
    /// `buffer` (no copy). A `view` that does not borrow from `buffer` is copied.
    pub fn slice_of(buffer: &Bytes, view: KeyValueVectorRef<'_>) -> SharedKeyValueVector {
        let base = buffer.as_ptr() as usize;
        let start = view.payload.as_ptr() as usize;
        let inside = start >= base
            && start
                .checked_add(view.payload.len())
                .is_some_and(|end| end <= base + buffer.len());
        if !inside {
            return SharedKeyValueVector::copy_from(view);
        }
        SharedKeyValueVector {
            payload: buffer.slice_ref(view.payload),
            count: view.count,
        }
    }

    /// The pairs of `view`, copied into a buffer of their own.
    pub fn copy_from(view: KeyValueVectorRef<'_>) -> SharedKeyValueVector {
        SharedKeyValueVector {
            payload: Bytes::copy_from_slice(view.payload),
            count: view.count,
        }
    }

    /// The number of pairs.
    pub fn len(&self) -> usize {
        self.count as usize
    }

    /// Whether there are no pairs.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The blob payload, the count included.
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// Views the pairs.
    pub fn borrowed(&self) -> KeyValueVectorRef<'_> {
        KeyValueVectorRef {
            payload: &self.payload,
            count: self.count,
        }
    }

    /// The pairs in order, borrowed.
    pub fn iter(&self) -> KeyValueIter<'_> {
        self.borrowed().iter()
    }

    /// The pairs in order, each a [`KeyValue`] whose key and value are slices of the
    /// buffer.
    pub fn shared(&self) -> SharedKeyValueIter<'_> {
        SharedKeyValueIter {
            payload: &self.payload,
            inner: self.iter(),
        }
    }
}

impl From<KeyValueVector> for SharedKeyValueVector {
    /// Takes the vector's buffer (no copy).
    fn from(v: KeyValueVector) -> Self {
        SharedKeyValueVector {
            payload: Bytes::from(v.bytes),
            count: v.count,
        }
    }
}

/// The pairs of a [`SharedKeyValueVector`] as [`KeyValue`]s that share its buffer.
#[derive(Clone, Debug)]
pub struct SharedKeyValueIter<'a> {
    payload: &'a Bytes,
    inner: KeyValueIter<'a>,
}

impl Iterator for SharedKeyValueIter<'_> {
    type Item = KeyValue;

    fn next(&mut self) -> Option<KeyValue> {
        let kv = self.inner.next()?;
        Some(KeyValue {
            key: self.payload.slice_ref(kv.key),
            value: self.payload.slice_ref(kv.value),
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for SharedKeyValueIter<'_> {}

impl FusedIterator for SharedKeyValueIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_layout_and_iteration() {
        let mut buf = KeyValueVector::new();
        assert_eq!(buf.payload(), [0, 0, 0, 0]);
        buf.try_push(b"ab", b"").unwrap();
        buf.try_push(b"", b"xyz").unwrap();
        assert_eq!(
            buf.payload(),
            [
                2, 0, 0, 0, // count
                2, 0, 0, 0, b'a', b'b', 0, 0, 0, 0, // "ab" -> ""
                0, 0, 0, 0, 3, 0, 0, 0, b'x', b'y', b'z', // "" -> "xyz"
            ]
        );
        let view = KeyValueVectorRef::parse(buf.payload()).unwrap();
        assert_eq!(view, buf.borrowed());
        assert_eq!(
            view.iter().collect::<Vec<_>>(),
            [KeyValueRef::new(b"ab", b""), KeyValueRef::new(b"", b"xyz")]
        );
        assert_eq!(view.iter().len(), 2);
        assert_eq!(view.into_owned(), buf);
    }

    #[test]
    fn empty_keys_and_values() {
        // One item with an empty key and an empty value is just two zero lengths.
        let payload = [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let view = KeyValueVectorRef::parse(&payload).unwrap();
        assert_eq!(
            view.iter().collect::<Vec<_>>(),
            [KeyValueRef::new(b"", b"")]
        );
        let mut buf = KeyValueVector::new();
        buf.try_push(b"", b"").unwrap();
        assert_eq!(buf.payload(), payload);
    }

    #[test]
    fn rejects_malformed_payloads() {
        // Too short for the count.
        for len in 0..4 {
            assert!(KeyValueVectorRef::parse(&[0; 4][..len]).is_err());
        }
        // The count says one item; none follows.
        assert!(KeyValueVectorRef::parse(&[1, 0, 0, 0]).is_err());
        // A count no blob could hold.
        assert!(KeyValueVectorRef::parse(&[0xff; 4]).is_err());
        // A key longer than the blob.
        assert!(KeyValueVectorRef::parse(&[1, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0]).is_err());
        // Trailing bytes.
        assert!(KeyValueVectorRef::parse(&[0, 0, 0, 0, 0]).is_err());
        assert_eq!(
            KeyValueVectorRef::parse(&[0; 4]).unwrap(),
            KeyValueVectorRef::EMPTY
        );
    }

    #[test]
    fn shared_vector_slices_the_message_buffer() {
        let buf = KeyValueVector::from_pairs([
            KeyValueRef::new(b"ab", b""),
            KeyValueRef::new(b"", b"xyz"),
            KeyValueRef::new(b"k", b"v"),
        ])
        .unwrap();
        // A message with the payload in its middle.
        let mut message = b"head".to_vec();
        message.extend_from_slice(buf.payload());
        message.extend_from_slice(b"tail");
        let message = Bytes::from(message);
        let view = KeyValueVectorRef::parse(&message[4..message.len() - 4]).unwrap();
        let shared = SharedKeyValueVector::slice_of(&message, view);
        assert_eq!(shared.len(), 3);
        assert_eq!(shared.borrowed(), view);
        assert_eq!(shared.payload().as_ptr(), message[4..].as_ptr());
        assert_eq!(
            shared.iter().collect::<Vec<_>>(),
            view.iter().collect::<Vec<_>>()
        );
        let rows: Vec<KeyValue> = shared.shared().collect();
        assert_eq!(
            rows,
            view.iter().map(KeyValueRef::into_owned).collect::<Vec<_>>()
        );
        // Not copies: the key and value of the last row point into the message.
        assert_eq!(
            rows[2].key.as_ptr(),
            view.iter().last().unwrap().key.as_ptr()
        );
        assert_eq!(shared.shared().len(), 3);

        // A view of another buffer is copied.
        let other = SharedKeyValueVector::slice_of(&Bytes::from_static(b"x"), view);
        assert_eq!(other, shared);
        assert_ne!(other.payload().as_ptr(), shared.payload().as_ptr());
        assert_eq!(SharedKeyValueVector::copy_from(view), shared);

        let owned: SharedKeyValueVector = buf.clone().into();
        assert_eq!(owned.borrowed(), buf.borrowed());
        assert_eq!(
            SharedKeyValueVector::default().borrowed(),
            KeyValueVectorRef::EMPTY
        );
        assert!(SharedKeyValueVector::default().shared().next().is_none());
    }
}
