//! Data-driven tests against the legacy binary-format vectors in
//! tools/golden-vectors/fixtures/binary-release-*, written by each release's own
//! `BinaryWriter` (see tools/golden-vectors/messages/binary_vectors.cpp).
//!
//! The vectors of the usage sites that have types (`ConnectPacket`, `SpanContext`,
//! `HealthMetrics`, idempotency records, self-conflict keys) are tested where the types are,
//! in fdb-native-transport and fdb-native-schema. This file covers the generic codec and
//! `ProtocolVersion`.

mod binary_support;

use std::collections::BTreeSet;

use binary_support::*;
use fdb_native_wire::ProtocolVersion;
use fdb_native_wire::binary::{DecodeError, ReadVersioning, Reader, from_slice};

/// The fixture types this crate does not test: the usage sites of other crates.
const OTHER_CRATES: [&str; 6] = [
    "connect_packet",
    "span_context",
    "health_metrics",
    "idempotency_value",
    "idempotency_scan_range",
    "self_conflict_key",
];

fn is_other_crates(ty: &str) -> bool {
    OTHER_CRATES.contains(&ty) || ty == "self_conflict_key_id"
}

#[test]
fn every_fixture_file_is_indexed() {
    for release in RELEASES {
        let dir = fixtures_dir(release);
        let index = read_json(&dir.join("index.json"));
        let mut listed = BTreeSet::new();
        for key in ["positive", "negative"] {
            for f in index[key].as_array().unwrap() {
                assert!(
                    listed.insert(f.as_str().unwrap().to_owned()),
                    "{f} listed twice"
                );
            }
        }
        let mut on_disk = BTreeSet::new();
        for sub in [dir.clone(), dir.join("negative")] {
            for entry in std::fs::read_dir(&sub).unwrap() {
                let path = entry.unwrap().path();
                if path.extension().is_some_and(|e| e == "json")
                    && path.file_name().is_some_and(|n| n != "index.json")
                {
                    on_disk.insert(
                        path.strip_prefix(&dir)
                            .unwrap()
                            .to_string_lossy()
                            .into_owned(),
                    );
                }
            }
        }
        assert_eq!(listed, on_disk, "{release}");
        assert!(
            listed.len() >= 60,
            "{release}: only {} vectors",
            listed.len()
        );
    }
}

#[test]
fn positive_vectors_decode_and_encode() {
    for release in RELEASES {
        let records = records(release, "positive");
        assert!(!records.is_empty());
        let mut checked = 0;
        for record in records {
            if is_other_crates(record["type"].as_str().unwrap()) {
                continue;
            }
            check_record(release, &record);
            checked += 1;
        }
        assert!(checked >= 50, "{release}: only {checked} vectors");
    }
}

#[test]
fn no_prefix_of_a_vector_decodes_as_the_same_type() {
    // A count or a length is not satisfied by fewer bytes than it says. (A value that is a
    // list to its end, with no count, has valid shorter prefixes: key-servers values.)
    for release in RELEASES {
        for record in records(release, "positive") {
            let ty = record["type"].as_str().unwrap();
            let name = record["name"].as_str().unwrap();
            if is_other_crates(ty) {
                continue;
            }
            let bytes = record_bytes(&record);
            let read = read_versioning(release, &record);
            for cut in 0..bytes.len() {
                if let Some(Ok(())) = decode_any(ty, &bytes[..cut], read) {
                    assert!(
                        ty == "key_servers_value",
                        "{release}/{name}: the {cut}-byte prefix of {} bytes decodes",
                        bytes.len()
                    );
                }
            }
        }
    }
}

#[test]
fn negative_vectors_are_rejected() {
    for release in RELEASES {
        let records = records(release, "negative");
        assert!(
            records.len() >= 10,
            "{release}: only {} negative vectors",
            records.len()
        );
        let mut checked = 0;
        for record in records {
            let name = record["name"].as_str().unwrap();
            let ty = record["type"].as_str().unwrap();
            if is_other_crates(ty) {
                continue;
            }
            assert_eq!(record["expect"], "reject", "{name}");
            let bytes = record_bytes(&record);
            let result = decode_any(ty, &bytes, read_versioning(release, &record))
                .unwrap_or_else(|| panic!("{release}/{name}: no decoder for {ty}"));
            let err = result
                .err()
                .unwrap_or_else(|| panic!("{release}/{name}: decoded"));

            // The same family of failure C++ reports.
            let reason = record["cpp_reason"].as_str().unwrap();
            let ok = match reason {
                // A short read.
                "serialization_failed" => matches!(
                    err,
                    DecodeError::UnexpectedEof { .. } | DecodeError::Invalid(_)
                ),
                "incompatible_protocol_version" => matches!(
                    err,
                    DecodeError::InvalidProtocolVersion(_) | DecodeError::FutureProtocolVersion(_)
                ),
                // An ASSERT on a variant index out of range.
                "internal_error" => matches!(err, DecodeError::InvalidVariantIndex { .. }),
                other => panic!("{name}: unknown reason {other}"),
            };
            assert!(ok, "{release}/{name}: C++ says {reason}, got {err:?}");
            checked += 1;
        }
        assert!(checked >= 9, "{release}: only {checked} negative vectors");
    }
}

#[test]
fn versions_are_the_releases_own() {
    // The 7.4 and 8.0 fixtures carry each release's own versions and limits.
    for release in RELEASES {
        let index = read_json(&fixtures_dir(release).join("index.json"));
        let current = index["current_protocol_version"].as_u64().unwrap();
        let (v, m) = match release {
            "release-7.4" => (ProtocolVersion::V7_4, ProtocolVersion::MIN_INVALID_7_4),
            _ => (ProtocolVersion::V8_0, ProtocolVersion::MIN_INVALID_8_0),
        };
        assert_eq!(current, v.0, "{release}");
        assert_eq!(min_invalid(release), m, "{release}");
    }
}

#[test]
fn include_version_limits_are_the_releases() {
    // 8.0's reader accepts what 7.4's refuses.
    let bytes = ProtocolVersion::MIN_INVALID_7_4.0.to_le_bytes();
    assert!(Reader::include_version(&bytes, ProtocolVersion::MIN_INVALID_8_0).is_ok());
    assert!(matches!(
        Reader::include_version(&bytes, ProtocolVersion::MIN_INVALID_7_4),
        Err(DecodeError::FutureProtocolVersion(_))
    ));
    assert!(from_slice::<Unit>(&bytes, ReadVersioning::INCLUDE_8_0).is_ok());
    assert!(from_slice::<Unit>(&bytes, ReadVersioning::INCLUDE_7_4).is_err());
    let bytes = ProtocolVersion::MIN_INVALID_8_0.0.to_le_bytes();
    assert!(matches!(
        Reader::include_version(&bytes, ProtocolVersion::MIN_INVALID_8_0),
        Err(DecodeError::FutureProtocolVersion(_))
    ));
    // The limit compares versions without flags.
    let flagged = ProtocolVersion::MIN_INVALID_7_4.with_object_serializer_flag();
    assert!(matches!(
        Reader::include_version(&flagged.0.to_le_bytes(), ProtocolVersion::MIN_INVALID_7_4),
        Err(DecodeError::FutureProtocolVersion(_))
    ));
}

#[test]
fn extra_bytes_after_a_value_are_rejected_by_from_slice() {
    let mut bytes = 5u32.to_le_bytes().to_vec();
    bytes.push(0);
    assert_eq!(
        from_slice::<u32>(&bytes, ReadVersioning::Unversioned),
        Err(DecodeError::TrailingBytes(1))
    );
}

#[test]
fn every_fixture_type_is_tested_somewhere() {
    // Every fixture names a type these tests or another crate's tests handle (catches a new
    // vector nobody checks).
    for release in RELEASES {
        for key in ["positive", "negative"] {
            for record in records(release, key) {
                let ty = record["type"].as_str().unwrap();
                let known = is_other_crates(ty)
                    || decode_any(ty, &[], ReadVersioning::Unversioned).is_some()
                    || matches!(
                        ty,
                        "key_servers_value" | "sw_version_value" | "protocol_version_compat"
                    );
                assert!(known, "{release}: fixture type {ty}");
            }
        }
    }
}
