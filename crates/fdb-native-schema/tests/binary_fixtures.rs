//! The binary-format values of this crate against the vectors FDB's own code produced
//! (tools/golden-vectors/fixtures/binary-release-*), and property tests.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use fdb_native_schema::{
    HealthMetrics, IdempotencyEntry, IdempotencyValue, SpanContext, StorageStats, TraceFlags,
    decode_idempotency_key, idempotency_key, idempotency_scan_range, self_conflict_key,
    self_conflict_key_for_idempotency_id,
};
use fdb_native_wire::binary::DecodeError;
use fdb_native_wire::{ProtocolVersion, Uid};
use proptest::prelude::*;
use serde_json::Value;

const RELEASES: [&str; 2] = ["release-7.4", "release-8.0"];

fn fixtures_dir(release: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tools/golden-vectors/fixtures")
        .join(format!("binary-{release}"))
}

fn read_json(path: &Path) -> Value {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parsing {}: {e}", path.display()))
}

fn from_hex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).expect("hex"))
        .collect()
}

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The records of the given fixture types, as `(release, record)`.
fn records(types: &[&str]) -> Vec<(&'static str, Value)> {
    let mut out = Vec::new();
    for release in RELEASES {
        let dir = fixtures_dir(release);
        for f in read_json(&dir.join("index.json"))["positive"]
            .as_array()
            .unwrap()
        {
            let record = read_json(&dir.join(f.as_str().unwrap()));
            if types.contains(&record["type"].as_str().unwrap()) {
                out.push((release, record));
            }
        }
    }
    out
}

fn uid(v: &Value) -> Uid {
    Uid::new(v["first"].as_u64().unwrap(), v["second"].as_u64().unwrap())
}

#[test]
fn span_contexts() {
    let records = records(&["span_context"]);
    assert!(records.len() >= 4);
    for (release, record) in records {
        let name = record["name"].as_str().unwrap();
        let bytes = from_hex(record["bytes"].as_str().unwrap());
        let v = &record["value"];
        let expected = SpanContext {
            trace_id: uid(&v["trace_id"]),
            span_id: v["span_id"].as_u64().unwrap(),
            flags: TraceFlags(u8::try_from(v["flags"].as_u64().unwrap()).unwrap()),
        };
        assert_eq!(
            SpanContext::from_option_value(&bytes).unwrap(),
            expected,
            "{release}/{name}"
        );
        let version = ProtocolVersion(record["versioning"]["protocol_version"].as_u64().unwrap());
        assert_eq!(
            to_hex(&expected.to_option_value(version).unwrap()),
            to_hex(&bytes)
        );
    }
}

fn health_metrics_of(v: &Value) -> HealthMetrics {
    HealthMetrics {
        worst_storage_queue: v["worst_storage_queue"].as_i64().unwrap(),
        limiting_storage_queue: v["limiting_storage_queue"].as_i64().unwrap(),
        worst_storage_durability_lag: v["worst_storage_durability_lag"].as_i64().unwrap(),
        limiting_storage_durability_lag: v["limiting_storage_durability_lag"].as_i64().unwrap(),
        worst_t_log_queue: v["worst_t_log_queue"].as_i64().unwrap(),
        tps_limit: v["tps_limit"].as_f64().unwrap(),
        batch_limited: v["batch_limited"].as_bool().unwrap(),
        storage_stats: v["storage_stats"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| {
                let s = &p[1];
                (
                    uid(&p[0]),
                    StorageStats {
                        storage_queue: s["storage_queue"].as_i64().unwrap(),
                        storage_durability_lag: s["storage_durability_lag"].as_i64().unwrap(),
                        disk_usage: s["disk_usage"].as_f64().unwrap(),
                        cpu_usage: s["cpu_usage"].as_f64().unwrap(),
                    },
                )
            })
            .collect(),
        t_log_queue: v["t_log_queue"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| (uid(&p[0]), p[1].as_i64().unwrap()))
            .collect(),
    }
}

#[test]
fn health_metrics() {
    let records = records(&["health_metrics"]);
    assert!(records.len() >= 4);
    for (release, record) in records {
        let name = record["name"].as_str().unwrap();
        let bytes = from_hex(record["bytes"].as_str().unwrap());
        let expected = health_metrics_of(&record["value"]);
        assert_eq!(
            HealthMetrics::from_blob(&bytes).unwrap(),
            expected,
            "{release}/{name}"
        );
        let version = ProtocolVersion(record["versioning"]["protocol_version"].as_u64().unwrap());
        assert_eq!(to_hex(&expected.to_blob(version).unwrap()), to_hex(&bytes));
        // The truncated blob is an error, never a shorter value.
        for cut in 0..bytes.len() {
            assert!(
                HealthMetrics::from_blob(&bytes[..cut]).is_err(),
                "{name}: {cut}"
            );
        }
    }
}

#[test]
fn idempotency_records() {
    let records = records(&["idempotency_value"]);
    assert!(records.len() >= 4);
    for (release, record) in records {
        let name = record["name"].as_str().unwrap();
        let bytes = from_hex(record["bytes"].as_str().unwrap());
        let v = &record["value"];
        let value = IdempotencyValue::decode(&bytes).unwrap();
        assert_eq!(
            value.protocol_version.0,
            v["protocol_version"].as_u64().unwrap()
        );
        assert_eq!(value.timestamp, v["timestamp"].as_i64().unwrap());
        let ids: Vec<(Vec<u8>, u8)> = v["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| {
                (
                    from_hex(e["id"].as_str().unwrap()),
                    u8::try_from(e["low_order_batch_index"].as_u64().unwrap()).unwrap(),
                )
            })
            .collect();
        let want: Vec<_> = ids
            .iter()
            .map(|(id, low)| IdempotencyEntry {
                id,
                low_order_batch_index: *low,
            })
            .collect();
        assert_eq!(value.entries, want, "{release}/{name}");
        assert_eq!(to_hex(&value.encode().unwrap()), to_hex(&bytes));

        // The key: makeIdempotencySingleKeyRange(...).begin and decodeIdempotencyKey.
        let key = from_hex(v["key"].as_str().unwrap());
        let commit_version = v["commit_version"].as_i64().unwrap();
        let high = u8::try_from(v["high_order_batch_index"].as_u64().unwrap()).unwrap();
        assert_eq!(
            decode_idempotency_key(&key).unwrap(),
            (commit_version, high)
        );
        assert_eq!(idempotency_key(commit_version, high), key);

        // kvContainsIdempotencyId finds each id with the batch index C++ computed.
        for f in v["found"].as_array().unwrap() {
            let low = value
                .find(&from_hex(f["id"].as_str().unwrap()))
                .expect("id is in the value");
            assert_eq!(
                u16::from(high) << 8 | u16::from(low),
                u16::try_from(f["batch_index"].as_u64().unwrap()).unwrap()
            );
            assert_eq!(f["commit_version"].as_i64().unwrap(), commit_version);
        }
        assert_eq!(value.find(&[0xee; 16]), None);

        // A record cut anywhere is an error or fewer whole entries, never garbage.
        for cut in 0..bytes.len() {
            if let Ok(shorter) = IdempotencyValue::decode(&bytes[..cut]) {
                assert!(shorter.entries.len() < value.entries.len(), "{name}: {cut}");
            }
        }
    }
}

#[test]
fn idempotency_scan_ranges() {
    let records = records(&["idempotency_scan_range"]);
    assert!(!records.is_empty());
    for (_, record) in records {
        let v = &record["value"];
        let (begin, end) = idempotency_scan_range(
            v["min_commit_version"].as_i64().unwrap(),
            v["max_commit_version"].as_i64().unwrap(),
        )
        .unwrap();
        assert_eq!(to_hex(&begin), v["begin"].as_str().unwrap());
        assert_eq!(to_hex(&end), v["end"].as_str().unwrap());
        assert_eq!(begin, from_hex(record["bytes"].as_str().unwrap()));
    }
}

#[test]
fn self_conflict_keys() {
    let records = records(&["self_conflict_key", "self_conflict_key_id"]);
    assert!(records.len() >= 4);
    for (release, record) in records {
        let name = record["name"].as_str().unwrap();
        let bytes = from_hex(record["bytes"].as_str().unwrap());
        let key = match record["type"].as_str().unwrap() {
            "self_conflict_key" => self_conflict_key(uid(&record["value"]["unique_id"])).unwrap(),
            _ => self_conflict_key_for_idempotency_id(&from_hex(
                record["value"]["idempotency_id"].as_str().unwrap(),
            )),
        };
        assert_eq!(to_hex(&key), to_hex(&bytes), "{release}/{name}");
    }
}

#[test]
fn negative_inputs_are_errors() {
    let value = SpanContext {
        trace_id: Uid::new(1, 2),
        span_id: 3,
        flags: TraceFlags::SAMPLED,
    }
    .to_option_value(ProtocolVersion::V8_0)
    .unwrap();
    for cut in 0..value.len() {
        assert!(
            SpanContext::from_option_value(&value[..cut]).is_err(),
            "{cut}"
        );
    }
    let mut longer = value.clone();
    longer.push(0);
    assert!(SpanContext::from_option_value(&longer).is_err());
    // An invalid version prefix.
    let mut bad = value;
    bad[..8].copy_from_slice(&1u64.to_le_bytes());
    assert!(matches!(
        SpanContext::from_option_value(&bad),
        Err(DecodeError::InvalidProtocolVersion(1))
    ));
}

proptest! {
    #[test]
    fn health_metrics_round_trip(q in any::<[i64; 5]>(), tps in -1e12f64..1e12, batch in any::<bool>(),
        stats in proptest::collection::btree_map(
            (any::<u64>(), any::<u64>()), (any::<i64>(), any::<i64>(), -1e6f64..1e6, 0f64..1.0), 0..4),
        tlogs in proptest::collection::btree_map((any::<u64>(), any::<u64>()), any::<i64>(), 0..4)) {
        let m = HealthMetrics {
            worst_storage_queue: q[0],
            limiting_storage_queue: q[1],
            worst_storage_durability_lag: q[2],
            limiting_storage_durability_lag: q[3],
            worst_t_log_queue: q[4],
            tps_limit: tps,
            batch_limited: batch,
            storage_stats: stats
                .into_iter()
                .map(|((a, b), (sq, lag, disk, cpu))| {
                    (Uid::new(a, b), StorageStats {
                        storage_queue: sq,
                        storage_durability_lag: lag,
                        disk_usage: disk,
                        cpu_usage: cpu,
                    })
                })
                .collect(),
            t_log_queue: tlogs.into_iter().map(|((a, b), v)| (Uid::new(a, b), v)).collect::<BTreeMap<_, _>>(),
        };
        let blob = m.to_blob(ProtocolVersion::V7_4).unwrap();
        prop_assert_eq!(HealthMetrics::from_blob(&blob).unwrap(), m);
    }

    #[test]
    fn idempotency_values_round_trip(ts in any::<i64>(),
        ids in proptest::collection::vec((proptest::collection::vec(any::<u8>(), 16..40), any::<u8>()), 0..5)) {
        let value = IdempotencyValue {
            protocol_version: ProtocolVersion::V7_4,
            timestamp: ts,
            entries: ids.iter().map(|(id, low)| IdempotencyEntry { id, low_order_batch_index: *low }).collect(),
        };
        let bytes = value.encode().unwrap();
        prop_assert_eq!(IdempotencyValue::decode(&bytes).unwrap(), value);
    }

    #[test]
    fn span_contexts_round_trip(a in any::<u64>(), b in any::<u64>(), span in any::<u64>(), flags in any::<u8>()) {
        let ctx = SpanContext { trace_id: Uid::new(a, b), span_id: span, flags: TraceFlags(flags) };
        let bytes = ctx.to_option_value(ProtocolVersion::V8_0).unwrap();
        prop_assert_eq!(bytes.len(), SpanContext::OPTION_VALUE_LEN);
        prop_assert_eq!(SpanContext::from_option_value(&bytes).unwrap(), ctx);
    }

    #[test]
    fn random_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..96)) {
        let _ = SpanContext::from_option_value(&bytes);
        let _ = HealthMetrics::from_blob(&bytes);
        let _ = IdempotencyValue::decode(&bytes);
        let _ = decode_idempotency_key(&bytes);
    }
}
