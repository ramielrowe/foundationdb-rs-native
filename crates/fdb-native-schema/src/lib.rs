//! Per-protocol-version FoundationDB message schemas (7.4 and 8.0), built on `fdb-native-wire`.
//!
//! # Binary-format values
//!
//! A few values a client reads or writes are not flatbuffers messages but FDB's legacy binary
//! format (see [`fdb_native_wire::binary`], which has the format and the table of where the
//! protocol uses it). The ones a client of 7.4 and 8.0 meets are here, identical on both
//! releases:
//!
//! - [`SpanContext`], the value of the `SPAN_PARENT` transaction option;
//! - [`HealthMetrics`], the `serialized` blob of a `GetHealthMetricsReply`;
//! - [`IdempotencyValue`] and the `\xff\x02/idmp/` key helpers, which a client reads to learn
//!   whether a commit that returned `commit_unknown_result` happened;
//! - [`self_conflict_key`] and [`self_conflict_key_for_idempotency_id`], the conflict ranges
//!   a commit adds so that it conflicts with itself.
//!
//! The connection handshake packet, also binary, is `fdb-native-transport`'s
//! `ConnectPacket`.

mod conflict_key;
mod health_metrics;
mod idempotency;
mod span_context;

pub use conflict_key::{
    SELF_CONFLICT_PREFIX, self_conflict_key, self_conflict_key_for_idempotency_id,
};
pub use health_metrics::{HealthMetrics, StorageStats};
pub use idempotency::{
    IDEMPOTENCY_ID_KEYS_BEGIN, IdempotencyEntry, IdempotencyValue, decode_idempotency_key,
    idempotency_key, idempotency_scan_range,
};
pub use span_context::{SpanContext, TraceFlags};

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold() {}
}
