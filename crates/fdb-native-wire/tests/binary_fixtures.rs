//! Data-driven tests against the legacy binary-format vectors in
//! tools/golden-vectors/fixtures/binary-release-*, written by each release's own
//! `BinaryWriter` (see tools/golden-vectors/messages/binary_vectors.cpp).

mod binary_support;

use std::collections::BTreeSet;

use binary_support::*;
use fdb_native_wire::ProtocolVersion;
use fdb_native_wire::binary::{BinaryDecodeError, BinaryReader, Versioning, from_slice};
use serde_json::Value;

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
        for record in records {
            // The record's own version options are the release's.
            check_record(&record);
        }
    }
}

#[test]
fn positive_vectors_round_trip_from_the_logical_value() {
    // For each decodable type the generic encoder reproduces the bytes (check_record did the
    // value-to-bytes direction); here every vector must also survive truncation: no prefix
    // of its bytes decodes, except where the format has no length (a shorter value is valid).
    for release in RELEASES {
        for record in records(release, "positive") {
            let ty = record["type"].as_str().unwrap();
            let name = record["name"].as_str().unwrap();
            let bytes = record_bytes(&record);
            let versioning = versioning(&record);
            for cut in 0..bytes.len() {
                if let Some(result) = decode_any(ty, &bytes[..cut], versioning) {
                    // A prefix may be a valid shorter value only for a sequence at its end
                    // (idempotency ids, a key-servers value's optional last UID).
                    if result.is_ok() {
                        assert!(
                            ty == "idempotency_value" || ty == "key_servers_value",
                            "{release}/{name}: the {cut}-byte prefix of {} bytes decodes",
                            bytes.len()
                        );
                    }
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
        for record in records {
            let name = record["name"].as_str().unwrap();
            let ty = record["type"].as_str().unwrap();
            assert_eq!(record["expect"], "reject", "{name}");
            let bytes = record_bytes(&record);
            let versioning = versioning(&record);
            // `from_slice` limits an IncludeVersion prefix to the newest release this crate
            // knows; the fixture's own release limit is its minInvalidProtocolVersion.
            let min_invalid = read_json(&fixtures_dir(release).join("index.json"))
                ["min_invalid_protocol_version"]
                .as_u64()
                .unwrap();
            let prefix = match versioning {
                Versioning::Include(_) => {
                    BinaryReader::include_version_below(&bytes, ProtocolVersion(min_invalid))
                        .map(drop)
                }
                _ => Ok(()),
            };
            let result = match prefix {
                Err(e) => Err(e),
                Ok(()) => decode_any(ty, &bytes, versioning)
                    .unwrap_or_else(|| panic!("{release}/{name}: no decoder for {ty}")),
            };
            let err = result
                .err()
                .unwrap_or_else(|| panic!("{release}/{name}: decoded"));

            // The same family of failure C++ reports.
            let reason = record["cpp_reason"].as_str().unwrap();
            let ok = match reason {
                // A short read.
                "serialization_failed" => matches!(
                    err,
                    BinaryDecodeError::UnexpectedEof { .. } | BinaryDecodeError::Invalid(_)
                ),
                "incompatible_protocol_version" => matches!(
                    err,
                    BinaryDecodeError::InvalidProtocolVersion(_)
                        | BinaryDecodeError::FutureProtocolVersion(_)
                ),
                // An ASSERT on a variant index out of range.
                "internal_error" => matches!(err, BinaryDecodeError::InvalidVariantIndex { .. }),
                other => panic!("{name}: unknown reason {other}"),
            };
            assert!(ok, "{release}/{name}: C++ says {reason}, got {err:?}");
        }
    }
}

#[test]
fn versions_are_the_releases_own() {
    // The 7.4 and 8.0 fixtures differ only in the protocol version they carry.
    let mut seen = Vec::new();
    for release in RELEASES {
        let index = read_json(&fixtures_dir(release).join("index.json"));
        let current = index["current_protocol_version"].as_u64().unwrap();
        let min_invalid = index["min_invalid_protocol_version"].as_u64().unwrap();
        let (v, m) = match release {
            "release-7.4" => (0x0FDB_00B0_7400_0000_u64, 0x0FDB_00B0_8100_0000_u64),
            _ => (0x0FDB_00B0_8000_0000_u64, 0x0FDB_00B0_9100_0000_u64),
        };
        assert_eq!((current, min_invalid), (v, m), "{release}");
        seen.push(current);
    }
    assert_eq!(seen.len(), 2);
}

#[test]
fn a_future_version_is_rejected_by_include_version() {
    // Each release's reader limit is its own minInvalidProtocolVersion; the crate's default
    // is 8.0's, so the 7.4 limit is checked with include_version_below.
    let bytes = 0x0FDB_00B0_8100_0000_u64.to_le_bytes();
    assert!(BinaryReader::include_version(&bytes).is_ok());
    assert!(matches!(
        BinaryReader::include_version_below(
            &bytes,
            fdb_native_wire::ProtocolVersion(0x0FDB_00B0_8100_0000)
        ),
        Err(BinaryDecodeError::FutureProtocolVersion(_))
    ));
    let bytes = 0x0FDB_00B0_9100_0000_u64.to_le_bytes();
    assert!(matches!(
        BinaryReader::include_version(&bytes),
        Err(BinaryDecodeError::FutureProtocolVersion(_))
    ));
}

#[test]
fn extra_bytes_after_a_value_are_rejected_by_from_slice() {
    let mut bytes = 5u32.to_le_bytes().to_vec();
    bytes.push(0);
    assert_eq!(
        from_slice::<u32>(&bytes, fdb_native_wire::binary::Versioning::Unversioned),
        Err(BinaryDecodeError::TrailingBytes(1))
    );
}

#[test]
fn every_logical_value_has_a_type() {
    // Every fixture names a type the tests know (catches a new vector nobody checks).
    for release in RELEASES {
        for key in ["positive", "negative"] {
            for record in records(release, key) {
                let ty = record["type"].as_str().unwrap();
                let known = decode_any(ty, &[], fdb_native_wire::binary::Versioning::Unversioned)
                    .is_some()
                    || matches!(
                        ty,
                        "idempotency_scan_range"
                            | "self_conflict_key"
                            | "key_servers_value"
                            | "sw_version_value"
                    );
                assert!(known, "{release}: fixture type {ty}");
            }
        }
    }
    let _: Value = Value::Null;
}
