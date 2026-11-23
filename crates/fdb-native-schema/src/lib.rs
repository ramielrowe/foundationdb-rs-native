//! Per-protocol-version FoundationDB message schemas (7.4 and 8.0), built on `fdb-native-wire`.
//!
//! # Messages: schema sets and the domain layer
//!
//! FDB 7.4 and 8.0 send the same messages with the same file identifiers, but 8.0 removed
//! fields from the middle of several (the tenant fields of the read and commit requests), so
//! the slot layouts differ and nothing in a message says which one it has. Each release
//! therefore has its own **schema set**, [`v7_4`] and [`v8_0`]: `#[derive(Wire)]` types with
//! exactly that release's layout (field order is `serializer(ar, ...)` order), checked against
//! the messages each release's own C++ writes (`tools/golden-vectors/fixtures/release-*`).
//! Types identical on both are defined once and re-exported by both.
//!
//! Client logic does not use the versioned types. It works on the version-neutral
//! [`domain`] types and goes through [`Request`], which encodes a request and decodes its
//! reply with the schema set of a [`SchemaVersion`], chosen once per connection from the
//! peer's protocol version:
//!
//! ```
//! use fdb_native_schema::domain::{Commit, KeyRange, Mutation, MutationType};
//! use fdb_native_schema::{Request, SchemaVersion};
//! use fdb_native_wire::{ProtocolVersion, Uid};
//!
//! let version = SchemaVersion::for_protocol_version(ProtocolVersion::V7_4).unwrap();
//! let commit = Commit {
//!     mutations: vec![Mutation::new(MutationType::SET_VALUE, "k", "v")?],
//!     write_conflict_ranges: vec![KeyRange::single_key(b"k")],
//!     read_snapshot: 1000,
//!     ..Commit::default()
//! };
//! let reply_token = Uid::new(7, 8); // from the request layer's reply endpoint
//! let bytes = commit.encode_request(version, reply_token)?; // with the 7.4 tenant-less TenantInfo
//! assert_eq!(Commit::decode_request(version, &bytes)?, (commit, reply_token));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! The vertical slice covers `OpenDatabaseCoordRequest` → `ClientDBInfo` (with
//! `GrvProxyInterface` and `CommitProxyInterface`), `GetReadVersionRequest` →
//! `GetReadVersionReply`, `GetKeyServerLocationsRequest` → `GetKeyServerLocationsReply`
//! (with `StorageServerInterface`), `GetValueRequest` → `GetValueReply` and
//! `CommitTransactionRequest` → `CommitID`. Interfaces carry one endpoint each; the others
//! are adjusted endpoints whose indices are in [`streams`] (the domain interfaces'
//! `endpoints()` computes them).
//!
//! Wire quirks handled here: single-key compression of `KeyRangeRef` and `MutationRef`, and
//! the refusal to encode the inverted ranges and clears C++ cannot serialize
//! ([`v7_4::KeyRangeRef`], [`v7_4::MutationRef`]); `MutationRef`'s optional checksum suffixes;
//! the tenant-less `TenantInfo` a 7.4 request must carry ([`v7_4::TenantInfo::NONE`]); and
//! `ClientTagThrottleLimits`, which travels as a duration relative to the sender's clock
//! ([`domain::TagThrottleLimits::expires_at`]).
//!
//! # Binary-format values
//!
//! A few values a client reads or writes are not flatbuffers messages but FDB's legacy binary
//! format (see [`fdb_native_wire::binary`], which has the format and the table of where the
//! protocol uses it). The ones a client of 7.4 and 8.0 meets are here, identical on both
//! releases:
//!
//! - [`SpanContext`], the value of the `SPAN_PARENT` transaction option (and, as a
//!   flatbuffers table, the span context of every request);
//! - [`HealthMetrics`], the `serialized` blob of a `GetHealthMetricsReply`;
//! - [`IdempotencyValue`] and the `\xff\x02/idmp/` key helpers, which a client reads to learn
//!   whether a commit that returned `commit_unknown_result` happened;
//! - [`self_conflict_key`] and [`self_conflict_key_for_idempotency_id`], the conflict ranges
//!   a commit adds so that it conflicts with itself.
//!
//! The connection handshake packet, also binary, is `fdb-native-transport`'s
//! `ConnectPacket`.

mod common;
mod conflict_key;
pub mod domain;
mod health_metrics;
mod idempotency;
mod request;
mod span_context;
pub mod streams;
pub mod v7_4;
pub mod v8_0;
mod version;

pub use conflict_key::{
    SELF_CONFLICT_PREFIX, self_conflict_key, self_conflict_key_for_idempotency_id,
};
pub use health_metrics::{HealthMetrics, StorageStats};
pub use idempotency::{
    IDEMPOTENCY_ID_KEYS_BEGIN, IdempotencyEntry, IdempotencyValue, decode_idempotency_key,
    idempotency_key, idempotency_scan_range,
};
pub use request::{Request, decode_reply, encode_request, encode_request_into};
pub use span_context::{SpanContext, TraceFlags};
pub use version::SchemaVersion;
