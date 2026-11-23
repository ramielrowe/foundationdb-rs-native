//! FDB's `TagSet`.

use super::{Cursor, Out};
use crate::{Decode, DecodeError, Encode, EncodeError, ObjectRef, Reader, Shape, Wire, Writer};

/// FDB's `TagSet` (`TransactionTag`s, throttling tags) borrowing its tags: an ordered list
/// of byte strings, borrowed from the message when decoded. [`TagSet`] is the owned form.
///
/// On the wire it is one blob (`dynamic_size_traits<TagSet>`, the same on 7.4 and 8.0):
/// per tag a `u8` length then the tag's bytes, in order, with no count and no padding. An
/// empty set is a zero-length blob, so it is written as the shared empty blob (and an
/// `Optional<TagSet>` distinguishes "no tag set" from an empty one). The format cannot
/// hold a tag of 256 bytes or more; encoding one fails.
///
/// [`push`](Self::push) deduplicates like C++ `TagSet::addTag` (an exact repeat is not
/// added), because servers assume a set holds each tag once. Decoding does not: FDB's
/// loader trusts the sender, so a decoded set holds whatever the blob holds. FDB's loader
/// also reads each length unchecked and `ASSERT`s at the end that it stopped exactly at
/// the blob's end; here a length that overruns the blob is rejected. API limits on tags
/// (the `MAX_TRANSACTION_TAG_LENGTH` and `MAX_TAGS_PER_TRANSACTION` knobs) are not wire
/// rules and are not checked.
///
/// A decoded set takes 16 bytes per tag, so at most 16 times the blob (the worst case is
/// a blob of empty tags, 1 byte each); tags themselves are borrowed.
///
/// ```
/// use fdb_native_wire::TagSetRef;
///
/// let mut set = TagSetRef::default();
/// assert!(set.push(b"tagA"));
/// assert!(set.push(b"tag-b"));
/// assert!(!set.push(b"tagA")); // already there
/// let payload = set.to_payload()?;
/// assert_eq!(payload, b"\x04tagA\x05tag-b");
/// assert_eq!(TagSetRef::from_payload(&payload)?, set);
/// assert!(TagSetRef::from_payload(b"\x05abc").is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct TagSetRef<'a> {
    tags: Vec<&'a [u8]>,
}

impl<'a> TagSetRef<'a> {
    /// Appends a tag unless the set already holds it; returns whether it was added.
    pub fn push(&mut self, tag: &'a [u8]) -> bool {
        if self.tags.contains(&tag) {
            false
        } else {
            self.tags.push(tag);
            true
        }
    }

    /// The tags, in order.
    pub fn tags(&self) -> &[&'a [u8]] {
        &self.tags
    }

    /// The number of tags.
    pub fn len(&self) -> usize {
        self.tags.len()
    }

    /// Whether there are no tags.
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// The tags in order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &'a [u8]> + '_ {
        self.tags.iter().copied()
    }

    /// Copies the tags.
    pub fn into_owned(self) -> TagSet {
        TagSet {
            tags: self.tags.iter().map(|t| t.to_vec()).collect(),
        }
    }

    /// The size of the encoded blob payload: one length byte and the bytes per tag.
    pub fn encoded_len(&self) -> Result<usize, EncodeError> {
        let mut len: usize = 0;
        for tag in &self.tags {
            if tag.len() > usize::from(u8::MAX) {
                return Err(EncodeError::InvalidShape(
                    "a TagSet tag is longer than 255 bytes",
                ));
            }
            len = len
                .checked_add(1 + tag.len())
                .ok_or(EncodeError::TooLarge)?;
        }
        Ok(len)
    }

    fn write_payload(&self, out: &mut [u8]) {
        let mut out = Out::new(out);
        for tag in &self.tags {
            out.put(&[tag.len() as u8]);
            out.put(tag);
        }
    }

    /// Encodes the blob payload (without the string framing).
    pub fn to_payload(&self) -> Result<Vec<u8>, EncodeError> {
        let mut out = vec![0; self.encoded_len()?];
        self.write_payload(&mut out);
        Ok(out)
    }

    /// Decodes a blob payload (without the string framing), borrowing the tags from it.
    pub fn from_payload(payload: &'a [u8]) -> Result<TagSetRef<'a>, DecodeError> {
        const TRUNCATED: &str = "TagSet tag overruns the blob";
        // A first pass validates and counts, so that the allocation is exact.
        let mut c = Cursor::new(payload, TRUNCATED);
        let mut count = 0usize;
        while !c.is_empty() {
            let len = usize::from(c.u8()?);
            c.take(len)?;
            count += 1;
        }
        let mut c = Cursor::new(payload, TRUNCATED);
        let mut tags = Vec::with_capacity(count);
        while !c.is_empty() {
            let len = usize::from(c.u8()?);
            tags.push(c.take(len)?);
        }
        Ok(TagSetRef { tags })
    }
}

/// Collects the tags in order, skipping repeats.
impl<'a> FromIterator<&'a [u8]> for TagSetRef<'a> {
    fn from_iter<I: IntoIterator<Item = &'a [u8]>>(iter: I) -> Self {
        let mut set = TagSetRef::default();
        for tag in iter {
            set.push(tag);
        }
        set
    }
}

impl Wire for TagSetRef<'_> {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for TagSetRef<'_> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let len = self.encoded_len()?;
        w.blob_with(len, |out| self.write_payload(out))
    }
}

impl<'de: 'a, 'a> Decode<'de> for TagSetRef<'a> {
    fn absent() -> Self {
        TagSetRef::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        TagSetRef::from_payload(r.blob(at)?)
    }
}

/// FDB's `TagSet`, owning its tags (see [`TagSetRef`] for the format).
///
/// ```
/// use fdb_native_wire::TagSet;
///
/// let mut set = TagSet::default();
/// assert!(set.push("tagA"));
/// assert!(!set.push("tagA"));
/// assert_eq!(set.borrowed().to_payload()?, b"\x04tagA");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct TagSet {
    tags: Vec<Vec<u8>>,
}

impl TagSet {
    /// Appends a tag unless the set already holds it; returns whether it was added.
    pub fn push(&mut self, tag: impl Into<Vec<u8>>) -> bool {
        let tag = tag.into();
        if self.tags.contains(&tag) {
            false
        } else {
            self.tags.push(tag);
            true
        }
    }

    /// The tags, in order.
    pub fn tags(&self) -> &[Vec<u8>] {
        &self.tags
    }

    /// The number of tags.
    pub fn len(&self) -> usize {
        self.tags.len()
    }

    /// Whether there are no tags.
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// Borrows the tags.
    pub fn borrowed(&self) -> TagSetRef<'_> {
        TagSetRef {
            tags: self.tags.iter().map(Vec::as_slice).collect(),
        }
    }
}

/// Collects the tags in order, skipping repeats.
impl<T: Into<Vec<u8>>> FromIterator<T> for TagSet {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut set = TagSet::default();
        for tag in iter {
            set.push(tag);
        }
        set
    }
}

impl Wire for TagSet {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for TagSet {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        self.borrowed().encode_object(w)
    }
}

impl<'de> Decode<'de> for TagSet {
    fn absent() -> Self {
        TagSet::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        TagSetRef::decode_object(r, at).map(TagSetRef::into_owned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing() {
        // A set with an empty tag: its length byte is the only thing on the wire.
        let mut set = TagSetRef::default();
        set.push(b"");
        set.push(b"x");
        assert_eq!(set.to_payload().unwrap(), [0, 1, b'x']);
        assert_eq!(TagSetRef::from_payload(&[0, 1, b'x']).unwrap(), set);
        assert_eq!(TagSetRef::from_payload(&[0, 0, 0]).unwrap().len(), 3);
        assert_eq!(TagSetRef::from_payload(&[]).unwrap(), TagSetRef::default());
        let long = [7u8; 256];
        let set: TagSetRef = [&long[..]].into_iter().collect();
        assert!(set.to_payload().is_err());
        let max = [7u8; 255];
        let set: TagSetRef = [&max[..]].into_iter().collect();
        assert_eq!(
            TagSetRef::from_payload(&set.to_payload().unwrap()).unwrap(),
            set
        );
    }

    #[test]
    fn push_deduplicates_and_decode_does_not() {
        let set: TagSetRef = [&b"a"[..], b"b", b"a"].into_iter().collect();
        assert_eq!(set.tags(), [&b"a"[..], b"b"]);
        let owned: TagSet = ["a", "b", "a", "c"].into_iter().collect();
        assert_eq!(owned.len(), 3);
        // The wire may repeat a tag; C++ loads it as is.
        let decoded = TagSetRef::from_payload(&[1, b'a', 1, b'a']).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.into_owned().len(), 2);
    }

    #[test]
    fn rejects_overrunning_tags() {
        assert!(TagSetRef::from_payload(&[1]).is_err());
        assert!(TagSetRef::from_payload(&[2, b'a']).is_err());
        assert!(TagSetRef::from_payload(&[255]).is_err());
        assert!(TagSetRef::from_payload(&[1, b'a', 1]).is_err());
    }
}
