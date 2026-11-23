//! `KeyValueRef` and the pre-serialized key-value vector.

use core::iter::FusedIterator;

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
            key: self.key.to_vec(),
            value: self.value.to_vec(),
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

/// FDB's `KeyValue` (`Standalone<KeyValueRef>`): an owned key and value.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyValue {
    /// The key.
    pub key: Vec<u8>,
    /// The value.
    pub value: Vec<u8>,
}

impl KeyValue {
    /// A pair that owns its bytes.
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> KeyValue {
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
}
