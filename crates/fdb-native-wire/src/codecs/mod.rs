//! FDB's special-case serializers, as pluggable blob codecs.
//!
//! Most FDB types are tables, but a few take over their own serialization and are
//! written as one string-framed blob ([`Shape::Object`](crate::Shape::Object); in C++ a
//! `dynamic_size_traits` or `string_serialized_traits` specialization). Each type here
//! writes its payload with [`Writer::blob_with`](crate::Writer::blob_with) and reads it
//! from [`Reader::blob`](crate::Reader::blob), so it works as a table field, a vector
//! element or an `Optional` alternative like any other type, and the reader's work limit
//! and bounds checks apply to its blob.
//!
//! | C++ type | Rust type | where it travels |
//! | --- | --- | --- |
//! | `VectorRef<KeyValueRef, VecSerStrategy::String>` | [`KeyValueVectorRef`] (borrowed), [`KeyValueVector`] (owned), [`SharedKeyValueVector`] (shared buffer) | `GetKeyValuesReply::data`, `GetKeyValuesStreamReply::data` |
//! | `VersionVector` | [`VersionVector`] (with [`Tag`]) | `ssLatestCommitVersions` of the read requests, `GetReadVersionReply::ssVersionVectorDelta` |
//! | `TagSet` | [`TagSetRef`] (borrowed), [`TagSet`] (owned) | `Optional<TagSet> tags` of the read requests, `CommitTransactionRequest::tagSet` |
//! | `IdempotencyIdRef` | [`IdempotencyIdRef`] (borrowed), [`IdempotencyId`] (owned) | `CommitTransactionRequest::idempotencyId` |
//!
//! [`Tag`] (`struct_like_traits<Tag>`: `u16 id` at 0, `i8 locality` at 2) is an inline
//! struct, not a blob; `VersionVector` is built from it. [`KeyValueRef`] / [`KeyValue`]
//! are also tables `{key, value}` (`serializer(ar, key, value)`), which is how
//! `KeyValueRef` travels when it is not in a preserialized vector.
//!
//! # Decoding is strict
//!
//! FDB's loaders for these blobs trust the lengths inside them (the KV vector, `TagSet`
//! and `VersionVector` `load`s read first and `ASSERT` afterwards that they consumed the
//! blob exactly, which is too late on bad input). The decoders here check every length
//! against the blob first, reject trailing bytes and never allocate from an unchecked
//! count. They accept every blob the C++ writers produce.
//!
//! Memory used by a decode, as a multiple of the blob (the blob itself is bounded by the
//! message size and the reader's work limit; bound untrusted messages): the KV view and
//! `IdempotencyIdRef` borrow it (0, and `KeyValueVector` and `IdempotencyId` copy it, 1;
//! `SharedKeyValueVector` shares the message's buffer, 0, or copies it, 1);
//! a `VersionVector` keeps 16 bytes per entry and an entry takes at least 2 bytes of the
//! blob (a 1-byte tag id and delta), so at most 8; a `TagSetRef` keeps a 16-byte slice per
//! tag and a tag takes at least 1 byte (an empty tag), so at most 16, and an owned
//! `TagSet` adds an allocation per tag on top.
//!
//! # 7.4 versus 8.0
//!
//! None of these formats differs between the two release branches: the C++ of
//! `string_serialized_traits<KeyValueRef>` and `VectorRef<_, String>`'s
//! `dynamic_size_traits` (`flow/Arena.h`, `fdbclient/FDBTypes.h`),
//! `dynamic_size_traits<VersionVector>` (`fdbclient/VersionVector.h`), `TagSet`
//! (`TagThrottle.actor.h` on 7.4, `TagThrottle.h` on 8.0) and `IdempotencyIdRef`
//! (`IdempotencyId.actor.h` / `IdempotencyId.h`) is the same on both apart from
//! formatting, and so is the file identifier of each. (`tagLocalityRangePartitionedBackup`
//! and `tagLocalityCDC` are new tag localities on 8.0; `Tag` carries any `i8`.) The
//! messages that carry these types have different slot layouts, which the message
//! tables, not these codecs, handle.

mod idempotency_id;
mod kv;
mod tag;
mod tag_set;
mod version_vector;

pub use idempotency_id::{IdempotencyId, IdempotencyIdRef, InvalidIdempotencyId};
pub use kv::{
    KeyValue, KeyValueIter, KeyValueRef, KeyValueVector, KeyValueVectorRef, SharedKeyValueIter,
    SharedKeyValueVector,
};
pub use tag::Tag;
pub use tag_set::{TagSet, TagSetRef};
pub use version_vector::{INVALID_VERSION, TAG_LOCALITY_INVALID, Version, VersionVector};

use crate::DecodeError;

/// A bounds-checked little-endian reader over a blob's payload. Every read that does not
/// fit fails with the `truncated` message instead of panicking.
pub(crate) struct Cursor<'a> {
    rest: &'a [u8],
    truncated: &'static str,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(bytes: &'a [u8], truncated: &'static str) -> Cursor<'a> {
        Cursor {
            rest: bytes,
            truncated,
        }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.rest.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rest.is_empty()
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let (head, tail) = self
            .rest
            .split_at_checked(n)
            .ok_or(DecodeError::Invalid(self.truncated))?;
        self.rest = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let truncated = self.truncated;
        self.take(N)?
            .try_into()
            .map_err(|_| DecodeError::Invalid(truncated))
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        self.array().map(|[b]| b)
    }

    pub(crate) fn i8(&mut self) -> Result<i8, DecodeError> {
        self.array().map(i8::from_le_bytes)
    }

    pub(crate) fn u16(&mut self) -> Result<u16, DecodeError> {
        self.array().map(u16::from_le_bytes)
    }

    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        self.array().map(u32::from_le_bytes)
    }

    pub(crate) fn u64(&mut self) -> Result<u64, DecodeError> {
        self.array().map(u64::from_le_bytes)
    }

    pub(crate) fn i64(&mut self) -> Result<i64, DecodeError> {
        self.array().map(i64::from_le_bytes)
    }
}

/// A cursor over a blob's bytes that are being filled in. Writes that do not fit are
/// dropped (the callers size the blob exactly), so it cannot panic.
pub(crate) struct Out<'a> {
    buf: &'a mut [u8],
    at: usize,
}

impl<'a> Out<'a> {
    pub(crate) fn new(buf: &'a mut [u8]) -> Out<'a> {
        Out { buf, at: 0 }
    }

    pub(crate) fn put(&mut self, bytes: &[u8]) {
        if let Some(dst) = self
            .at
            .checked_add(bytes.len())
            .and_then(|end| self.buf.get_mut(self.at..end))
        {
            dst.copy_from_slice(bytes);
        }
        self.at = self.at.saturating_add(bytes.len());
    }
}
