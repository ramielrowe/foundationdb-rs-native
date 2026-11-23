//! `KeyValueRef` and the pre-serialized key-value vector.

use core::iter::FusedIterator;

use super::Cursor;
use crate::{Decode, DecodeError, Encode, EncodeError, ObjectRef, Reader, Shape, Wire, Writer};

/// FDB's `KeyValueRef`, borrowing its key and value.
///
/// Outside a pre-serialized vector (see [`KeyValueVector`]) it is a table with two
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
/// payload.
///
/// The payload is a `u32` item count, then per item a `u32` key length, the key, a `u32`
/// value length and the value, with no padding or alignment. It is the same on 7.4 and
/// 8.0. An empty vector is a 4-byte blob holding a zero count (not the shared empty
/// blob), which is what [`encode_object`](Encode::encode_object) writes for it.
///
/// FDB's loader trusts the lengths and only `ASSERT`s afterwards that they add up to the
/// blob; [`KeyValueVector::parse`] and the [`Decode`] impl check them first and reject a
/// count the blob cannot hold, an item that overruns the blob, a blob shorter than its
/// count, and trailing bytes (and a blob shorter than 4 bytes, such as the shared empty
/// blob, which FDB never writes for this type). An absent field is the empty vector.
///
/// The format has one encoding per value, so equality compares payload bytes and
/// re-encoding a decoded vector is byte-identical.
///
/// ```
/// use fdb_native_wire::{KeyValueRef, KeyValueVector, KeyValueVectorBuf};
///
/// let mut buf = KeyValueVectorBuf::new();
/// buf.push(b"a", b"1");
/// buf.push(b"b", b"");
/// let view = KeyValueVector::parse(buf.payload())?;
/// assert_eq!(view.len(), 2);
/// assert_eq!(view.iter().last(), Some(KeyValueRef::new(b"b", b"")));
/// assert!(KeyValueVector::parse(&buf.payload()[1..]).is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct KeyValueVector<'a> {
    /// The whole payload, count included; validated.
    payload: &'a [u8],
    count: u32,
}

impl<'a> KeyValueVector<'a> {
    /// The empty vector.
    pub const EMPTY: KeyValueVector<'static> = KeyValueVector {
        payload: &EMPTY_PAYLOAD,
        count: 0,
    };

    /// Validates a blob payload (without the string framing) and views it.
    pub fn parse(payload: &'a [u8]) -> Result<KeyValueVector<'a>, DecodeError> {
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
        Ok(KeyValueVector { payload, count })
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

    /// Copies the pairs out.
    pub fn to_vec(&self) -> Vec<KeyValue> {
        self.iter().map(KeyValueRef::into_owned).collect()
    }
}

impl Default for KeyValueVector<'_> {
    fn default() -> Self {
        KeyValueVector::EMPTY
    }
}

impl<'a> IntoIterator for KeyValueVector<'a> {
    type Item = KeyValueRef<'a>;
    type IntoIter = KeyValueIter<'a>;

    fn into_iter(self) -> KeyValueIter<'a> {
        self.iter()
    }
}

impl<'a> IntoIterator for &KeyValueVector<'a> {
    type Item = KeyValueRef<'a>;
    type IntoIter = KeyValueIter<'a>;

    fn into_iter(self) -> KeyValueIter<'a> {
        self.iter()
    }
}

/// The pairs of a [`KeyValueVector`].
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
                self.rest = &self.rest[self.rest.len() - c.remaining()..];
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

impl Wire for KeyValueVector<'_> {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for KeyValueVector<'_> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.blob(self.payload)
    }
}

impl<'de: 'a, 'a> Decode<'de> for KeyValueVector<'a> {
    fn absent() -> Self {
        KeyValueVector::EMPTY
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        KeyValueVector::parse(r.blob(at)?)
    }
}

/// An owned pre-serialized key-value vector, for building replies (and for decoding
/// without borrowing). It holds the encoded payload, so encoding it is a copy.
///
/// ```
/// use fdb_native_wire::{KeyValueRef, KeyValueVectorBuf};
///
/// let buf: KeyValueVectorBuf =
///     [KeyValueRef::new(b"k", b"v"), KeyValueRef::new(b"k2", b"")].into_iter().collect();
/// assert_eq!(buf.len(), 2);
/// assert_eq!(buf.payload().len(), 4 + (8 + 2) + (8 + 2));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct KeyValueVectorBuf {
    /// The payload with the count kept up to date in its first 4 bytes.
    bytes: Vec<u8>,
    count: u32,
    /// A length or the count did not fit a `u32`; encoding fails.
    overflow: bool,
}

impl Default for KeyValueVectorBuf {
    fn default() -> Self {
        KeyValueVectorBuf::new()
    }
}

impl KeyValueVectorBuf {
    /// An empty vector.
    pub fn new() -> KeyValueVectorBuf {
        KeyValueVectorBuf {
            bytes: EMPTY_PAYLOAD.to_vec(),
            count: 0,
            overflow: false,
        }
    }

    /// Appends a pair. A key, a value or a count that does not fit a `u32` is dropped and
    /// makes encoding fail with [`EncodeError::TooLarge`].
    pub fn push(&mut self, key: &[u8], value: &[u8]) {
        let (Ok(key_len), Ok(value_len), Some(count)) = (
            u32::try_from(key.len()),
            u32::try_from(value.len()),
            self.count.checked_add(1),
        ) else {
            self.overflow = true;
            return;
        };
        self.bytes.extend_from_slice(&key_len.to_le_bytes());
        self.bytes.extend_from_slice(key);
        self.bytes.extend_from_slice(&value_len.to_le_bytes());
        self.bytes.extend_from_slice(value);
        self.count = count;
        if let Some(header) = self.bytes.first_chunk_mut::<4>() {
            *header = count.to_le_bytes();
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
    pub fn payload(&self) -> &[u8] {
        &self.bytes
    }

    /// The blob payload.
    pub fn into_payload(self) -> Vec<u8> {
        self.bytes
    }

    /// Views the pairs.
    pub fn as_view(&self) -> KeyValueVector<'_> {
        KeyValueVector {
            payload: &self.bytes,
            count: self.count,
        }
    }

    /// The pairs in order.
    pub fn iter(&self) -> KeyValueIter<'_> {
        self.as_view().iter()
    }
}

impl<'a> Extend<KeyValueRef<'a>> for KeyValueVectorBuf {
    fn extend<I: IntoIterator<Item = KeyValueRef<'a>>>(&mut self, iter: I) {
        for kv in iter {
            self.push(kv.key, kv.value);
        }
    }
}

impl<'a> FromIterator<KeyValueRef<'a>> for KeyValueVectorBuf {
    fn from_iter<I: IntoIterator<Item = KeyValueRef<'a>>>(iter: I) -> Self {
        let mut buf = KeyValueVectorBuf::new();
        buf.extend(iter);
        buf
    }
}

impl From<KeyValueVector<'_>> for KeyValueVectorBuf {
    fn from(view: KeyValueVector<'_>) -> Self {
        KeyValueVectorBuf {
            bytes: view.payload.to_vec(),
            count: view.count,
            overflow: false,
        }
    }
}

impl Wire for KeyValueVectorBuf {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for KeyValueVectorBuf {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        if self.overflow {
            return Err(EncodeError::TooLarge);
        }
        self.as_view().encode_object(w)
    }
}

impl<'de> Decode<'de> for KeyValueVectorBuf {
    fn absent() -> Self {
        KeyValueVectorBuf::new()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        KeyValueVector::decode_object(r, at).map(KeyValueVectorBuf::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_layout_and_iteration() {
        let mut buf = KeyValueVectorBuf::new();
        assert_eq!(buf.payload(), [0, 0, 0, 0]);
        buf.push(b"ab", b"");
        buf.push(b"", b"xyz");
        assert_eq!(
            buf.payload(),
            [
                2, 0, 0, 0, // count
                2, 0, 0, 0, b'a', b'b', 0, 0, 0, 0, // "ab" -> ""
                0, 0, 0, 0, 3, 0, 0, 0, b'x', b'y', b'z', // "" -> "xyz"
            ]
        );
        let view = KeyValueVector::parse(buf.payload()).unwrap();
        assert_eq!(view, buf.as_view());
        assert_eq!(
            view.iter().collect::<Vec<_>>(),
            [KeyValueRef::new(b"ab", b""), KeyValueRef::new(b"", b"xyz")]
        );
        assert_eq!(view.iter().len(), 2);
    }

    #[test]
    fn rejects_malformed_payloads() {
        // Too short for the count.
        for len in 0..4 {
            assert!(KeyValueVector::parse(&[0; 4][..len]).is_err());
        }
        // The count says one item; none follows.
        assert!(KeyValueVector::parse(&[1, 0, 0, 0]).is_err());
        // A count no blob could hold.
        assert!(KeyValueVector::parse(&[0xff; 4]).is_err());
        // A key longer than the blob.
        assert!(KeyValueVector::parse(&[1, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0]).is_err());
        // Trailing bytes.
        assert!(KeyValueVector::parse(&[0, 0, 0, 0, 0]).is_err());
        assert_eq!(
            KeyValueVector::parse(&[0; 4]).unwrap(),
            KeyValueVector::EMPTY
        );
    }
}
