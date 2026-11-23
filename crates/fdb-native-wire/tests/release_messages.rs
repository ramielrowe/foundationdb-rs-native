//! `#[derive(Wire)]` on real messages: `GetValueRequest` and `GetValueReply` for FDB 7.4
//! and 8.0, written as per-protocol-version schema sets, checked against the message
//! fixtures FDB's own serializer wrote (`tools/golden-vectors/fixtures/release-7.4` and
//! `release-8.0`).
//!
//! 8.0 removed `tenantInfo` from the middle of `GetValueRequest`, moving every later slot
//! down by one, so the two versions get two Rust types (`v7_4::GetValueRequest` and
//! `v8_0::GetValueRequest`) that each derive their exact layout, and client logic uses the
//! version-neutral `domain::GetValue`. Types whose layout did not change are shared.

#[macro_use]
mod support;

use std::path::Path;

use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, EnsureTable, ErrorOr, FdbError, FileIdentified,
    ObjectRef, Reader, ReplyPromise, Shape, Uid, Wire, Writer, decode, encode,
};
use serde_json::{Value, json};
use support::*;

/// Types identical on both versions.
mod common {
    use super::*;

    /// `TagSet`: one string-framed blob holding, per tag, a u8 length and the tag. A blob
    /// codec, so written by hand.
    #[derive(Clone, Debug, PartialEq)]
    pub struct TagSet<'a>(pub Vec<&'a [u8]>);

    impl Wire for TagSet<'_> {
        const SHAPE: Shape = Shape::Object;
    }

    impl Encode for TagSet<'_> {
        fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
            if self.0.iter().any(|t| t.len() > 255) {
                return Err(EncodeError::InvalidShape("a tag is at most 255 bytes"));
            }
            let len = self.0.iter().map(|t| 1 + t.len()).sum();
            w.blob_with(len, |out| {
                let mut at = 0;
                for tag in &self.0 {
                    out[at] = tag.len() as u8;
                    out[at + 1..at + 1 + tag.len()].copy_from_slice(tag);
                    at += 1 + tag.len();
                }
            })
        }
    }

    impl<'de: 'a, 'a> Decode<'de> for TagSet<'a> {
        fn absent() -> Self {
            TagSet(Vec::new())
        }

        fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
            let mut blob = r.blob(at)?;
            let mut tags = Vec::new();
            while let Some((&len, rest)) = blob.split_first() {
                let (tag, rest) = rest
                    .split_at_checked(usize::from(len))
                    .ok_or(DecodeError::Invalid("tag overruns the TagSet blob"))?;
                tags.push(tag);
                blob = rest;
            }
            Ok(TagSet(tags))
        }
    }

    /// `VersionVector`: one string-framed blob with its own encoding, kept opaque (the
    /// client forwards the one it got from the proxy); [`VersionVector::parse`] reads it.
    #[derive(Clone, Debug, PartialEq)]
    pub struct VersionVector<'a>(pub &'a [u8]);

    impl VersionVector<'_> {
        /// No entries, `maxVersion` 0.
        pub const EMPTY: VersionVector<'static> = VersionVector(&[0; 16]);

        /// The fixtures' JSON form (golden-vectors README, "VersionVector blob").
        pub fn parse(&self) -> Option<Value> {
            let mut b = self.0;
            let mut take = |n: usize| -> Option<u64> {
                let (head, tail) = b.split_at_checked(n)?;
                b = tail;
                let mut le = [0u8; 8];
                le[..n].copy_from_slice(head);
                Some(u64::from_le_bytes(le))
            };
            let localities = take(8)?;
            let mut runs = Vec::new();
            for _ in 0..localities {
                runs.push((take(1)? as u8 as i8, take(2)?));
            }
            let mut versions = Vec::new();
            if runs.iter().any(|&(_, n)| n > 0) {
                let id_width = usize::try_from(take(1)?).ok()?;
                let delta_width = usize::try_from(take(1)?).ok()?;
                let min = take(8)? as i64;
                let count = take(8)?;
                if count != runs.iter().map(|&(_, n)| n).sum::<u64>() {
                    return None;
                }
                for &(locality, n) in &runs {
                    for _ in 0..n {
                        let id = take(id_width)?;
                        let delta = take(delta_width)?;
                        versions.push(json!({
                            "locality": locality,
                            "id": id,
                            "version": min.checked_add(i64::try_from(delta).ok()?)?,
                        }));
                    }
                }
            }
            let max = take(8)? as i64;
            b.is_empty()
                .then(|| json!({ "versions": versions, "maxVersion": max }))
        }
    }

    impl Wire for VersionVector<'_> {
        const SHAPE: Shape = Shape::Object;
    }

    impl Encode for VersionVector<'_> {
        fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
            w.blob(self.0)
        }
    }

    impl<'de: 'a, 'a> Decode<'de> for VersionVector<'a> {
        fn absent() -> Self {
            VersionVector::EMPTY
        }

        fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
            r.blob(at).map(VersionVector)
        }
    }

    /// `SpanContext`: `serializer(ar, traceID, spanID, m_Flags)`.
    #[derive(Wire, Clone, Debug, PartialEq)]
    pub struct SpanContext {
        pub trace_id: Uid,
        pub span_id: u64,
        pub flags: u8,
    }

    /// `GetValueReply`: `serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error,
    /// value, cached)`. Replies travel as `ErrorOr<EnsureTable<GetValueReply>>`, whose file
    /// identifier the wrappers compose from this one.
    #[derive(Wire, Clone, Debug, PartialEq)]
    #[fdb(file_identifier = 1378929)]
    pub struct GetValueReply<'a> {
        pub penalty: f64,
        pub error: Option<FdbError>,
        pub value: Option<&'a [u8]>,
        pub cached: bool,
    }

    /// `ReplyPromise<GetValueReply>`: the reply endpoint's token.
    pub type Reply = ReplyPromise<GetValueReply<'static>>;

    dump_table!(@no_arb "SpanContext" SpanContext {
        trace_id = "traceID", span_id = "spanID", flags = "m_Flags",
    });
    dump_table!(@no_arb "GetValueReply" GetValueReply<'a> {
        penalty = "penalty", error = "error", value = "value", cached = "cached",
    });
}

/// The FDB 7.4 schema set.
pub mod v7_4 {
    use super::*;
    pub use common::{GetValueReply, Reply, SpanContext, TagSet, VersionVector};

    /// `WipedString`: `serializer(ar, ws, arena)`, `ws` a local copy of the string.
    #[derive(Wire, Clone, Debug, PartialEq)]
    pub struct WipedString<'a> {
        pub ws: &'a [u8],
        #[fdb(arena)]
        pub arena: (),
    }

    /// `TenantInfo` (7.4 only): `serializer(ar, tenantId, token, arena)`.
    #[derive(Wire, Clone, Debug, PartialEq)]
    pub struct TenantInfo<'a> {
        pub tenant_id: i64,
        pub token: Option<WipedString<'a>>,
        #[fdb(arena)]
        pub arena: (),
    }

    impl TenantInfo<'_> {
        /// No tenant (`TenantInfo::INVALID_TENANT`).
        pub const NONE: TenantInfo<'static> = TenantInfo {
            tenant_id: -1,
            token: None,
            arena: (),
        };
    }

    /// `ReadOptions`: `serializer(ar, type, cacheResult, debugID,
    /// consistencyCheckStartVersion, lockAware)`; `type` is an `int32_t` enum on 7.4.
    #[derive(Wire, Clone, Debug, PartialEq)]
    pub struct ReadOptions {
        pub read_type: i32,
        pub cache_result: bool,
        pub debug_id: Option<Uid>,
        pub consistency_check_start_version: Option<i64>,
        pub lock_aware: bool,
    }

    /// `GetValueRequest`: `serializer(ar, key, version, tags, reply, spanContext,
    /// tenantInfo, options, ssLatestCommitVersions)`.
    #[derive(Wire, Clone, Debug, PartialEq)]
    #[fdb(file_identifier = 8454530)]
    pub struct GetValueRequest<'a> {
        pub key: &'a [u8],
        pub version: i64,
        pub tags: Option<TagSet<'a>>,
        pub reply: Reply,
        pub span_context: SpanContext,
        pub tenant_info: TenantInfo<'a>,
        pub options: Option<ReadOptions>,
        pub ss_latest_commit_versions: VersionVector<'a>,
    }

    dump_table!(@no_arb "WipedString" WipedString<'a> { ws = "ws" } @arena);
    dump_table!(@no_arb "TenantInfo" TenantInfo<'a> { tenant_id = "tenantId", token = "token" } @arena);
    dump_table!(@no_arb "ReadOptions" ReadOptions {
        read_type = "type", cache_result = "cacheResult", debug_id = "debugID",
        consistency_check_start_version = "consistencyCheckStartVersion",
        lock_aware = "lockAware",
    });
    dump_table!(@no_arb "GetValueRequest" GetValueRequest<'a> {
        key = "key", version = "version", tags = "tags", reply = "reply",
        span_context = "spanContext", tenant_info = "tenantInfo", options = "options",
        ss_latest_commit_versions = "ssLatestCommitVersions",
    });

    impl<'a> From<&domain::GetValue<'a>> for GetValueRequest<'a> {
        fn from(g: &domain::GetValue<'a>) -> Self {
            GetValueRequest {
                key: g.key,
                version: g.version,
                tags: (!g.tags.is_empty()).then(|| TagSet(g.tags.clone())),
                reply: ReplyPromise::new(g.reply_token),
                span_context: SpanContext::absent(),
                tenant_info: TenantInfo::NONE,
                options: g.debug_id.map(|id| ReadOptions {
                    read_type: 0,
                    cache_result: true,
                    debug_id: Some(id),
                    consistency_check_start_version: None,
                    lock_aware: false,
                }),
                ss_latest_commit_versions: VersionVector::EMPTY,
            }
        }
    }

    impl<'a> From<GetValueRequest<'a>> for domain::GetValue<'a> {
        fn from(r: GetValueRequest<'a>) -> Self {
            domain::GetValue {
                key: r.key,
                version: r.version,
                tags: r.tags.map_or_else(Vec::new, |t| t.0),
                debug_id: r.options.and_then(|o| o.debug_id),
                reply_token: r.reply.token(),
            }
        }
    }
}

/// The FDB 8.0 schema set.
pub mod v8_0 {
    use super::*;
    pub use common::{GetValueReply, Reply, SpanContext, TagSet, VersionVector};

    /// `ReadOptions`: as on 7.4, but `type` is a `uint32_t` enum.
    #[derive(Wire, Clone, Debug, PartialEq)]
    pub struct ReadOptions {
        pub read_type: u32,
        pub cache_result: bool,
        pub debug_id: Option<Uid>,
        pub consistency_check_start_version: Option<i64>,
        pub lock_aware: bool,
    }

    /// `GetValueRequest`: `serializer(ar, key, version, tags, reply, spanContext, options,
    /// ssLatestCommitVersions)`, 7.4's without `tenantInfo`.
    #[derive(Wire, Clone, Debug, PartialEq)]
    #[fdb(file_identifier = 8454530)]
    pub struct GetValueRequest<'a> {
        pub key: &'a [u8],
        pub version: i64,
        pub tags: Option<TagSet<'a>>,
        pub reply: Reply,
        pub span_context: SpanContext,
        pub options: Option<ReadOptions>,
        pub ss_latest_commit_versions: VersionVector<'a>,
    }

    dump_table!(@no_arb "ReadOptions" ReadOptions {
        read_type = "type", cache_result = "cacheResult", debug_id = "debugID",
        consistency_check_start_version = "consistencyCheckStartVersion",
        lock_aware = "lockAware",
    });
    dump_table!(@no_arb "GetValueRequest" GetValueRequest<'a> {
        key = "key", version = "version", tags = "tags", reply = "reply",
        span_context = "spanContext", options = "options",
        ss_latest_commit_versions = "ssLatestCommitVersions",
    });

    impl<'a> From<&domain::GetValue<'a>> for GetValueRequest<'a> {
        fn from(g: &domain::GetValue<'a>) -> Self {
            GetValueRequest {
                key: g.key,
                version: g.version,
                tags: (!g.tags.is_empty()).then(|| TagSet(g.tags.clone())),
                reply: ReplyPromise::new(g.reply_token),
                span_context: SpanContext::absent(),
                options: g.debug_id.map(|id| ReadOptions {
                    read_type: 0,
                    cache_result: true,
                    debug_id: Some(id),
                    consistency_check_start_version: None,
                    lock_aware: false,
                }),
                ss_latest_commit_versions: VersionVector::EMPTY,
            }
        }
    }

    impl<'a> From<GetValueRequest<'a>> for domain::GetValue<'a> {
        fn from(r: GetValueRequest<'a>) -> Self {
            domain::GetValue {
                key: r.key,
                version: r.version,
                tags: r.tags.map_or_else(Vec::new, |t| t.0),
                debug_id: r.options.and_then(|o| o.debug_id),
                reply_token: r.reply.token(),
            }
        }
    }
}

/// What the client logic works with, whatever the server's version.
mod domain {
    use fdb_native_wire::Uid;

    #[derive(Clone, Debug, PartialEq)]
    pub struct GetValue<'a> {
        pub key: &'a [u8],
        pub version: i64,
        pub tags: Vec<&'a [u8]>,
        pub debug_id: Option<Uid>,
        pub reply_token: Uid,
    }
}

impl Dump for common::TagSet<'_> {
    fn type_name() -> String {
        "tag_set".into()
    }
    fn node(&self) -> Value {
        let tags = self.0.iter().map(|t| Value::from(hex_encode(t))).collect();
        node(Self::type_name(), Value::Array(tags))
    }
}

impl Dump for common::VersionVector<'_> {
    fn type_name() -> String {
        "version_vector".into()
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            self.parse().expect("a valid VersionVector blob"),
        )
    }
}

// ---------------------------------------------------------------------------------------

fn fixture(release: &str, name: &str) -> Value {
    read_json(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tools/golden-vectors/fixtures")
            .join(release)
            .join(format!("{name}.json")),
    )
}

/// Decodes the fixture as `$ty` (checking its file identifier), compares the fields with
/// the fixture's dump, and checks that the value survives a Rust round trip.
macro_rules! check_fixture {
    ($ty:ty, $release:literal, $name:literal) => {{
        let context = concat!($release, "/", $name);
        let record = fixture($release, $name);
        assert_eq!(record["cpp_type"], <$ty as CppName>::NAME, "{context}");
        assert_eq!(
            record_fid(&record),
            <$ty as FileIdentified>::FILE_IDENTIFIER.value(),
            "{context}"
        );
        let bytes = record_bytes(&record);
        let value: $ty = decode(&bytes).unwrap_or_else(|e| panic!("{context}: {e}"));
        assert_json_eq(context, &record["root"], &root1(&value));
        let again = encode(&value).unwrap_or_else(|e| panic!("{context}: {e}"));
        assert_eq!(decode::<$ty>(&again).as_ref(), Ok(&value), "{context}");
    }};
}

trait CppName {
    const NAME: &'static str;
}

impl CppName for v7_4::GetValueRequest<'_> {
    const NAME: &'static str = "GetValueRequest";
}

impl CppName for v8_0::GetValueRequest<'_> {
    const NAME: &'static str = "GetValueRequest";
}

impl CppName for common::GetValueReply<'_> {
    const NAME: &'static str = "GetValueReply";
}

impl CppName for ErrorOr<EnsureTable<common::GetValueReply<'_>>> {
    const NAME: &'static str = "ErrorOr<EnsureTable<GetValueReply>>";
}

#[test]
fn release_7_4_get_value() {
    use v7_4::{GetValueReply, GetValueRequest};
    check_fixture!(GetValueRequest, "release-7.4", "get_value_request");
    check_fixture!(GetValueRequest, "release-7.4", "get_value_request_minimal");
    check_fixture!(GetValueReply, "release-7.4", "get_value_reply");
    check_fixture!(
        ErrorOr<EnsureTable<GetValueReply>>,
        "release-7.4",
        "get_value_reply_envelope"
    );
    check_fixture!(
        ErrorOr<EnsureTable<GetValueReply>>,
        "release-7.4",
        "get_value_reply_absent_value"
    );
    check_fixture!(
        ErrorOr<EnsureTable<GetValueReply>>,
        "release-7.4",
        "get_value_reply_error"
    );
}

#[test]
fn release_8_0_get_value() {
    use v8_0::{GetValueReply, GetValueRequest};
    check_fixture!(GetValueRequest, "release-8.0", "get_value_request");
    check_fixture!(GetValueRequest, "release-8.0", "get_value_request_minimal");
    check_fixture!(GetValueReply, "release-8.0", "get_value_reply");
    check_fixture!(
        ErrorOr<EnsureTable<GetValueReply>>,
        "release-8.0",
        "get_value_reply_envelope"
    );
    check_fixture!(
        ErrorOr<EnsureTable<GetValueReply>>,
        "release-8.0",
        "get_value_reply_absent_value"
    );
    check_fixture!(
        ErrorOr<EnsureTable<GetValueReply>>,
        "release-8.0",
        "get_value_reply_error"
    );
}

/// Both versions use one file identifier, so only the peer's protocol version says which
/// schema a request needs. With the wrong one every slot after `spanContext` is misread:
/// in these vectors a shifted slot reads a 4-byte offset from a 1-byte union tag at the
/// end of the table, which the `table_size` check rejects (in general a misread can also
/// go unnoticed).
#[test]
fn the_wrong_version_misreads_a_request() {
    for name in ["get_value_request", "get_value_request_minimal"] {
        let bytes = record_bytes(&fixture("release-7.4", name));
        assert!(decode::<v7_4::GetValueRequest>(&bytes).is_ok());
        assert_eq!(
            decode::<v8_0::GetValueRequest>(&bytes),
            Err(DecodeError::Strict(fdb_native_wire::StrictCheck::TableSize)),
            "release-7.4/{name}"
        );
        let bytes = record_bytes(&fixture("release-8.0", name));
        assert!(decode::<v8_0::GetValueRequest>(&bytes).is_ok());
        assert_eq!(
            decode::<v7_4::GetValueRequest>(&bytes),
            Err(DecodeError::Strict(fdb_native_wire::StrictCheck::TableSize)),
            "release-8.0/{name}"
        );
    }
}

#[test]
fn domain_requests_round_trip_through_either_schema() {
    let request = domain::GetValue {
        key: b"user/key",
        version: 42,
        tags: vec![b"tagA"],
        debug_id: Some(Uid::new(7, 8)),
        reply_token: Uid::new(1, 2),
    };

    let bytes = encode(&v7_4::GetValueRequest::from(&request)).unwrap();
    let wire: v7_4::GetValueRequest = decode(&bytes).unwrap();
    assert_eq!(domain::GetValue::from(wire), request);

    let bytes = encode(&v8_0::GetValueRequest::from(&request)).unwrap();
    let wire: v8_0::GetValueRequest = decode(&bytes).unwrap();
    assert_eq!(domain::GetValue::from(wire), request);

    assert_eq!(
        common::VersionVector::EMPTY.parse(),
        Some(json!({ "versions": [], "maxVersion": 0 }))
    );
}
