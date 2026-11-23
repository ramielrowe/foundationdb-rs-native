//! FDB's `VersionVector`.

use std::collections::BTreeMap;

use super::{Cursor, Out, Tag};
use crate::{
    Decode, DecodeError, Encode, EncodeError, FileIdentified, FileIdentifier, ObjectRef, Reader,
    Shape, Wire, Writer,
};

/// A commit version (`Version` in FDB).
pub type Version = i64;

/// `invalidVersion`: the `maxVersion` of a default-constructed `VersionVector`.
pub const INVALID_VERSION: Version = -1;

/// `tagLocalityInvalid`: the encoder's "no locality yet" marker. FDB asserts that every
/// tag in a version vector has a locality above it, so [`VersionVector`] rejects tags at
/// or below it.
pub const TAG_LOCALITY_INVALID: i8 = -99;

/// FDB's `VersionVector` (`fdbclient/VersionVector.h`): the latest commit version of each
/// storage server tag a client has seen, plus the maximum version.
///
/// On the wire it is one blob (`dynamic_size_traits<VersionVector>`, the same on 7.4 and
/// 8.0), all little-endian with `size_t` = 8 bytes:
///
/// | field | size | |
/// | --- | --- | --- |
/// | `utlCount` | u64 | number of distinct tag localities |
/// | per locality, in `Tag` order | i8, u16 | the locality and its run length |
/// | tag id width | u8 | 1 if every id is at most 255, otherwise 2 |
/// | version delta width | u8 | 1, 2, 4 or 8: the smallest holding `max - min` of the entries' versions |
/// | `min` | i64 | the smallest entry version |
/// | entry count | u64 | |
/// | per entry, in `Tag` order | tag id width, delta width | the id and `version - min` |
/// | `maxVersion` | i64 | |
///
/// The three middle groups are only present when there is at least one entry; an empty
/// vector is 16 bytes. The encoder always picks the narrowest widths, so its output is
/// canonical, and `encode(decode(blob)) == blob` for every blob FDB writes.
///
/// # Decoding
///
/// [`VersionVector::from_payload`] (and the [`Decode`] impl) accept every width pair (a
/// writer other than FDB's may use wider than necessary) and rebuild the entries. Beyond
/// FDB's loader (which trusts the counts and asserts afterwards) they reject: counts or
/// widths that do not fit the blob exactly (including trailing bytes), a tag id width
/// other than 1 or 2, a delta width other than 1, 2, 4 or 8, a locality run of length 0,
/// localities that do not strictly ascend, ids that do not strictly ascend within a
/// locality, a locality at or below [`TAG_LOCALITY_INVALID`], a run total that differs
/// from the entry count, and a version that overflows `i64`. An absent field decodes as
/// [`VersionVector::default`] (no entries, `maxVersion` = -1) as in C++.
///
/// ```
/// use fdb_native_wire::{Tag, VersionVector};
///
/// let mut vv = VersionVector::new(1200);
/// vv.versions.insert(Tag::new(0, 2), 1000);
/// vv.versions.insert(Tag::new(0, 3), 1200);
/// let blob = vv.to_payload()?;
/// assert_eq!(blob.len(), 8 + 3 + 1 + 1 + 8 + 8 + 2 * 2 + 8);
/// assert_eq!(VersionVector::from_payload(&blob)?, vv);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct VersionVector {
    /// The entries, ordered by tag. Tags must have a locality above
    /// [`TAG_LOCALITY_INVALID`] to be encoded.
    pub versions: BTreeMap<Tag, Version>,
    /// The maximum version; there may or may not be an entry for it.
    pub max_version: Version,
}

impl Default for VersionVector {
    /// `VersionVector()`: no entries, `maxVersion` = [`INVALID_VERSION`].
    fn default() -> Self {
        VersionVector::new(INVALID_VERSION)
    }
}

impl FileIdentified for VersionVector {
    const FILE_IDENTIFIER: FileIdentifier = FileIdentifier::new(5253554);
}

/// What the encoder derives from the entries.
struct Layout {
    localities: usize,
    tag_width: usize,
    delta_width: usize,
    min_version: Version,
}

impl VersionVector {
    /// A vector with no entries and the given `maxVersion`.
    pub fn new(max_version: Version) -> VersionVector {
        VersionVector {
            versions: BTreeMap::new(),
            max_version,
        }
    }

    /// Checks the entries and works out the encoding: `None` for no entries.
    fn layout(&self) -> Result<Option<Layout>, EncodeError> {
        let mut min = Version::MAX;
        let mut max = Version::MIN;
        let mut max_id = 0;
        let mut localities = 0;
        let mut locality = TAG_LOCALITY_INVALID;
        let mut run: usize = 0;
        for (tag, &version) in &self.versions {
            if tag.locality <= TAG_LOCALITY_INVALID {
                return Err(EncodeError::InvalidShape(
                    "VersionVector tag locality must be above tagLocalityInvalid (-99)",
                ));
            }
            if tag.locality != locality {
                locality = tag.locality;
                localities += 1;
                run = 0;
            }
            // C++ stores the run length in a u16 and would wrap.
            run += 1;
            if run > usize::from(u16::MAX) {
                return Err(EncodeError::TooLarge);
            }
            max_id = max_id.max(tag.id);
            min = min.min(version);
            max = max.max(version);
        }
        if localities == 0 {
            return Ok(None);
        }
        // The spread of two i64s always fits a u64.
        let spread = (i128::from(max) - i128::from(min)) as u64;
        Ok(Some(Layout {
            localities,
            tag_width: if max_id <= u16::from(u8::MAX) { 1 } else { 2 },
            delta_width: match spread {
                0..=0xff => 1,
                0x100..=0xffff => 2,
                0x1_0000..=0xffff_ffff => 4,
                _ => 8,
            },
            min_version: min,
        }))
    }

    /// The size in bytes of the encoded blob (FDB's `getEncodedSize`).
    pub fn encoded_len(&self) -> Result<usize, EncodeError> {
        let Some(layout) = self.layout()? else {
            return Ok(16);
        };
        // utlCount, runs, two widths, min, pair count, pairs, maxVersion.
        layout
            .localities
            .checked_mul(3)
            .and_then(|runs| {
                let pairs = self
                    .versions
                    .len()
                    .checked_mul(layout.tag_width + layout.delta_width)?;
                runs.checked_add(pairs)?.checked_add(8 + 2 + 8 + 8 + 8)
            })
            .ok_or(EncodeError::TooLarge)
    }

    /// Writes the blob into `out`, which must be exactly [`encoded_len`](Self::encoded_len)
    /// bytes. Entries that cannot be encoded are what `encoded_len` rejects.
    fn write_payload(&self, layout: Option<&Layout>, out: &mut [u8]) {
        let mut out = Out::new(out);
        let Some(layout) = layout else {
            out.put(&0u64.to_le_bytes());
            out.put(&self.max_version.to_le_bytes());
            return;
        };
        out.put(&(layout.localities as u64).to_le_bytes());
        let mut runs = self.versions.keys().peekable();
        while let Some(first) = runs.next() {
            let mut len: u16 = 1;
            while runs.next_if(|t| t.locality == first.locality).is_some() {
                len = len.saturating_add(1);
            }
            out.put(&first.locality.to_le_bytes());
            out.put(&len.to_le_bytes());
        }
        out.put(&[layout.tag_width as u8, layout.delta_width as u8]);
        out.put(&layout.min_version.to_le_bytes());
        out.put(&(self.versions.len() as u64).to_le_bytes());
        for (tag, version) in &self.versions {
            out.put(&tag.id.to_le_bytes()[..layout.tag_width]);
            let delta = (i128::from(*version) - i128::from(layout.min_version)) as u64;
            out.put(&delta.to_le_bytes()[..layout.delta_width]);
        }
        out.put(&self.max_version.to_le_bytes());
    }

    /// Encodes the blob payload (without the string framing).
    pub fn to_payload(&self) -> Result<Vec<u8>, EncodeError> {
        let layout = self.layout()?;
        let mut out = vec![0; self.encoded_len()?];
        self.write_payload(layout.as_ref(), &mut out);
        Ok(out)
    }

    /// Decodes a blob payload (without the string framing); see the type's docs for
    /// what is rejected.
    pub fn from_payload(payload: &[u8]) -> Result<VersionVector, DecodeError> {
        const TRUNCATED: &str = "VersionVector blob is truncated";
        let mut c = Cursor::new(payload, TRUNCATED);
        let locality_count = c.u64()?;
        let mut vv = VersionVector::default();
        if locality_count > 0 {
            // Each run takes 3 bytes: the count cannot be larger than what is left.
            if locality_count > (c.remaining() / 3) as u64 {
                return Err(DecodeError::Invalid(
                    "VersionVector locality count exceeds the blob",
                ));
            }
            let mut runs = Vec::with_capacity(locality_count as usize);
            let mut total: u64 = 0;
            for _ in 0..locality_count {
                let locality = c.i8()?;
                let len = c.u16()?;
                if locality <= TAG_LOCALITY_INVALID {
                    return Err(DecodeError::Invalid("VersionVector locality is invalid"));
                }
                if len == 0 {
                    return Err(DecodeError::Invalid("VersionVector locality run is empty"));
                }
                if runs.last().is_some_and(|&(prev, _)| prev >= locality) {
                    return Err(DecodeError::Invalid(
                        "VersionVector localities are not in ascending order",
                    ));
                }
                total += u64::from(len);
                runs.push((locality, len));
            }
            let tag_width = c.u8()?;
            let delta_width = c.u8()?;
            if !matches!(tag_width, 1 | 2) {
                return Err(DecodeError::Invalid(
                    "VersionVector tag id width is not 1 or 2",
                ));
            }
            if !matches!(delta_width, 1 | 2 | 4 | 8) {
                return Err(DecodeError::Invalid(
                    "VersionVector version delta width is not 1, 2, 4 or 8",
                ));
            }
            let min_version = c.i64()?;
            let pairs = c.u64()?;
            if pairs != total {
                return Err(DecodeError::Invalid(
                    "VersionVector entry count differs from its locality runs",
                ));
            }
            // The entries and maxVersion must fill the rest exactly. No overflow: the
            // runs total at most 3 * 65535 per 3 bytes of blob, so `pairs` is small.
            let pair_width = u64::from(tag_width) + u64::from(delta_width);
            if pairs.checked_mul(pair_width).and_then(|n| n.checked_add(8))
                != Some(c.remaining() as u64)
            {
                return Err(DecodeError::Invalid(
                    "VersionVector entries do not fill the blob",
                ));
            }
            for (locality, len) in runs {
                let mut prev_id = None;
                for _ in 0..len {
                    let id = if tag_width == 1 {
                        u16::from(c.u8()?)
                    } else {
                        c.u16()?
                    };
                    let delta = match delta_width {
                        1 => u64::from(c.u8()?),
                        2 => u64::from(c.u16()?),
                        4 => u64::from(c.u32()?),
                        _ => c.u64()?,
                    };
                    if prev_id.is_some_and(|prev| prev >= id) {
                        return Err(DecodeError::Invalid(
                            "VersionVector tag ids are not in ascending order",
                        ));
                    }
                    prev_id = Some(id);
                    let version = i64::try_from(i128::from(min_version) + i128::from(delta))
                        .map_err(|_| DecodeError::Invalid("VersionVector version overflows"))?;
                    vv.versions.insert(Tag::new(locality, id), version);
                }
            }
        }
        vv.max_version = c.i64()?;
        if !c.is_empty() {
            return Err(DecodeError::Invalid(
                "VersionVector blob has trailing bytes",
            ));
        }
        Ok(vv)
    }
}

impl Wire for VersionVector {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for VersionVector {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let layout = self.layout()?;
        let len = self.encoded_len()?;
        w.blob_with(len, |out| self.write_payload(layout.as_ref(), out))
    }
}

impl<'de> Decode<'de> for VersionVector {
    fn absent() -> Self {
        VersionVector::default()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        VersionVector::from_payload(r.blob(at)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vv(entries: &[(i8, u16, i64)], max: i64) -> VersionVector {
        let mut vv = VersionVector::new(max);
        for &(l, id, v) in entries {
            vv.versions.insert(Tag::new(l, id), v);
        }
        vv
    }

    #[test]
    fn empty_is_sixteen_bytes() {
        let blob = VersionVector::new(7).to_payload().unwrap();
        let mut expected = vec![0; 8];
        expected.extend(7i64.to_le_bytes());
        assert_eq!(blob, expected);
        assert_eq!(
            VersionVector::from_payload(&blob).unwrap(),
            VersionVector::new(7)
        );
    }

    #[test]
    fn widths_follow_the_spread() {
        // (entries, tag width, delta width)
        type Entries = &'static [(i8, u16, i64)];
        let cases: [(Entries, u8, u8); 6] = [
            (&[(0, 1, 10)], 1, 1),
            (&[(0, 1, 10), (0, 2, 10 + 255)], 1, 1),
            (&[(0, 1, 10), (0, 2, 10 + 256)], 1, 2),
            (&[(0, 256, 10), (0, 257, 10 + 65536)], 2, 4),
            (&[(0, 1, 10), (0, 2, 10 + 0x1_0000_0000)], 1, 8),
            (&[(-1, 1, i64::MIN), (0, 2, i64::MAX)], 1, 8),
        ];
        for (entries, tag_width, delta_width) in cases {
            let v = vv(entries, 99);
            let blob = v.to_payload().unwrap();
            assert_eq!(blob.len(), v.encoded_len().unwrap());
            let runs = entries
                .iter()
                .map(|e| e.0)
                .collect::<std::collections::BTreeSet<_>>()
                .len();
            assert_eq!(blob[8 + 3 * runs], tag_width);
            assert_eq!(blob[8 + 3 * runs + 1], delta_width);
            assert_eq!(VersionVector::from_payload(&blob).unwrap(), v);
        }
    }

    #[test]
    fn rejects_invalid_localities_when_encoding() {
        let bad = vv(&[(TAG_LOCALITY_INVALID, 1, 5)], 5);
        assert!(bad.to_payload().is_err());
        let bad = vv(&[(0, 1, 5), (i8::MIN, 1, 5)], 5);
        assert!(bad.encoded_len().is_err());
    }
}
