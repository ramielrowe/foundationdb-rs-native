//! FDB's `TagSet`.

use super::{Cursor, Out};
use crate::{Decode, DecodeError, Encode, EncodeError, ObjectRef, Reader, Shape, Wire, Writer};

/// FDB's `TagSet` (`TransactionTag`s, throttling tags): an ordered list of byte strings,
/// borrowed from the message when decoded.
///
/// On the wire it is one blob (`dynamic_size_traits<TagSet>`, the same on 7.4 and 8.0):
/// per tag a `u8` length then the tag's bytes, in order, with no count and no padding. An
/// empty set is a zero-length blob, so it is written as the shared empty blob (and an
/// `Optional<TagSet>` distinguishes "no tag set" from an empty one). The format cannot
/// hold a tag of 256 bytes or more; encoding one fails.
///
/// FDB's loader reads each length unchecked and `ASSERT`s at the end that it stopped
/// exactly at the blob's end; here a length that overruns the blob is rejected. FDB
/// assumes the tags are already deduplicated and does not check; neither does this. API
/// limits on tags (at most 16 bytes, at most 5 per transaction) are not wire rules.
///
/// ```
/// use fdb_native_wire::TagSet;
///
/// let set = TagSet::new(vec![&b"tagA"[..], b"tag-b"]);
/// let payload = set.to_payload()?;
/// assert_eq!(payload, b"\x04tagA\x05tag-b");
/// assert_eq!(TagSet::from_payload(&payload)?, set);
/// assert!(TagSet::from_payload(b"\x05abc").is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct TagSet<'a> {
    tags: Vec<&'a [u8]>,
}

impl<'a> TagSet<'a> {
    /// A set of the given tags, in order.
    pub fn new(tags: Vec<&'a [u8]>) -> TagSet<'a> {
        TagSet { tags }
    }

    /// Appends a tag.
    pub fn push(&mut self, tag: &'a [u8]) {
        self.tags.push(tag);
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
    pub fn from_payload(payload: &'a [u8]) -> Result<TagSet<'a>, DecodeError> {
        let mut c = Cursor::new(payload, "TagSet tag overruns the blob");
        let mut tags = Vec::new();
        while !c.is_empty() {
            let len = usize::from(c.u8()?);
            tags.push(c.take(len)?);
        }
        Ok(TagSet { tags })
    }
}

impl<'a> FromIterator<&'a [u8]> for TagSet<'a> {
    fn from_iter<I: IntoIterator<Item = &'a [u8]>>(iter: I) -> Self {
        TagSet {
            tags: iter.into_iter().collect(),
        }
    }
}

impl Wire for TagSet<'_> {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for TagSet<'_> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let len = self.encoded_len()?;
        w.blob_with(len, |out| self.write_payload(out))
    }
}

impl<'de: 'a, 'a> Decode<'de> for TagSet<'a> {
    fn absent() -> Self {
        TagSet::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        TagSet::from_payload(r.blob(at)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing() {
        let set = TagSet::new(vec![b"", b"x"]);
        assert_eq!(set.to_payload().unwrap(), [0, 1, b'x']);
        assert_eq!(TagSet::from_payload(&[0, 1, b'x']).unwrap(), set);
        assert_eq!(TagSet::from_payload(&[]).unwrap(), TagSet::default());
        let long = [7u8; 256];
        assert!(TagSet::new(vec![&long[..]]).to_payload().is_err());
        let max = [7u8; 255];
        let set = TagSet::new(vec![&max[..]]);
        assert_eq!(
            TagSet::from_payload(&set.to_payload().unwrap()).unwrap(),
            set
        );
    }

    #[test]
    fn rejects_overrunning_tags() {
        assert!(TagSet::from_payload(&[1]).is_err());
        assert!(TagSet::from_payload(&[2, b'a']).is_err());
        assert!(TagSet::from_payload(&[255]).is_err());
        assert!(TagSet::from_payload(&[1, b'a', 1]).is_err());
    }
}
