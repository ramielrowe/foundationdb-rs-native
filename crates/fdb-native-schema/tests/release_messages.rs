//! The 7.4 and 8.0 schema sets against the message fixtures each release's own C++ wrote
//! (`tools/golden-vectors/fixtures/release-7.4` and `release-8.0`).
//!
//! Every fixture of the vertical slice decodes into its version's Rust type, whose dump
//! matches the fixture's `root` field for field; the value re-encodes and decodes to the
//! same dump. The domain layer decodes and re-encodes each request and reply envelope
//! through [`Request`]. With the release decoders built, the ignored
//! `cpp_crosscheck_release_readers_decode_rust_messages` feeds the Rust encodings back to C++.

#[macro_use]
mod support;

use fdb_native_schema::domain::{
    Commit, GetKeyServerLocations, GetReadVersion, GetValue, OpenDatabaseCoord,
};
use fdb_native_schema::{Request, SchemaVersion, v7_4, v8_0};
use fdb_native_wire::{DecodeError, EnsureTable, ErrorOr, FileIdentified, decode, encode};
use serde_json::Value;
use support::*;

/// The local clock the reply conversions get (any value round-trips durations).
const NOW: f64 = 1000.0;

/// One fixture checked: its bytes, the Rust re-encoding, and the domain layer's encoding.
struct Outcome {
    release: &'static str,
    name: &'static str,
    cpp_type: &'static str,
    /// The fixture's `root`.
    root: Value,
    /// The fixture's bytes, re-encoded by the Rust wire type.
    reencoded: Vec<u8>,
    /// Whether `reencoded` is byte-identical to the fixture.
    identical: bool,
    /// The fixture decoded into the domain, encoded again through [`Request`], and the
    /// dump of that encoding as the Rust wire type reads it.
    domain: Option<(Vec<u8>, Value)>,
}

/// Decodes the fixture as `$ty` (checking its file identifier), compares the dump with the
/// fixture's `root`, re-encodes it and checks the re-encoding decodes to the same dump.
macro_rules! check {
    ($ty:ty, $release:literal, $name:literal, $cpp:literal) => {{
        let context = concat!($release, "/", $name);
        let record = fixture($release, $name);
        assert_eq!(record["cpp_type"], $cpp, "{context}");
        assert_eq!(
            record_fid(&record),
            <$ty as FileIdentified>::FILE_IDENTIFIER.value(),
            "{context}"
        );
        let bytes = record_bytes(&record);
        let value: $ty = decode(&bytes).unwrap_or_else(|e| panic!("{context}: {e}"));
        assert_json_eq(context, &record["root"], &root1(&value));
        let reencoded = encode(&value).unwrap_or_else(|e| panic!("{context}: {e}"));
        let again: $ty = decode(&reencoded).unwrap_or_else(|e| panic!("{context}: {e}"));
        assert_json_eq(
            &format!("{context} (re-encoded)"),
            &record["root"],
            &root1(&again),
        );
        Outcome {
            release: $release,
            name: $name,
            cpp_type: $cpp,
            root: record["root"].clone(),
            identical: reencoded == bytes,
            reencoded,
            domain: None,
        }
    }};
}

/// `check!` for a request, plus: decode it into the domain type `$domain`, encode that
/// again, and check that it decodes to the same domain value.
macro_rules! check_request {
    ($ty:ty, $domain:ty, $version:expr, $release:literal, $name:literal, $cpp:literal) => {{
        let mut outcome = check!($ty, $release, $name, $cpp);
        let bytes = record_bytes(&fixture($release, $name));
        let request =
            <$domain>::decode_request($version, &bytes).expect(concat!($name, ": domain"));
        let encoded = request.encode_request($version).expect($name);
        assert_eq!(
            <$domain>::decode_request($version, &encoded).as_ref(),
            Ok(&request),
            "{}/{}",
            $release,
            $name
        );
        let wire: $ty = decode(&encoded).expect($name);
        outcome.domain = Some((encoded.clone(), root1(&wire)));
        outcome
    }};
}

/// `check!` for a reply envelope `ErrorOr<EnsureTable<$ty>>`, plus the domain round trip of
/// `$domain`'s reply.
macro_rules! check_reply {
    ($ty:ty, $domain:ty, $version:expr, $release:literal, $name:literal, $cpp:literal) => {{
        let mut outcome = check!(ErrorOr<EnsureTable<$ty>>, $release, $name, $cpp);
        let bytes = record_bytes(&fixture($release, $name));
        let reply =
            <$domain>::decode_reply($version, &bytes, NOW).expect(concat!($name, ": domain"));
        let encoded = <$domain>::encode_reply($version, &reply, NOW).expect($name);
        assert_eq!(
            <$domain>::decode_reply($version, &encoded, NOW).as_ref(),
            Ok(&reply),
            "{}/{}",
            $release,
            $name
        );
        let wire: ErrorOr<EnsureTable<$ty>> = decode(&encoded).expect($name);
        outcome.domain = Some((encoded.clone(), root1(&wire)));
        outcome
    }};
}

fn release_7_4() -> Vec<Outcome> {
    use v7_4::*;
    const V: SchemaVersion = SchemaVersion::V7_4;
    vec![
        check_request!(
            OpenDatabaseCoordRequest,
            OpenDatabaseCoord,
            V,
            "release-7.4",
            "open_database_coord_request",
            "OpenDatabaseCoordRequest"
        ),
        check!(
            ClientDBInfo,
            "release-7.4",
            "client_db_info",
            "ClientDBInfo"
        ),
        check_reply!(
            ClientDBInfo,
            OpenDatabaseCoord,
            V,
            "release-7.4",
            "client_db_info_envelope",
            "ErrorOr<EnsureTable<ClientDBInfo>>"
        ),
        check_request!(
            GetReadVersionRequest,
            GetReadVersion,
            V,
            "release-7.4",
            "get_read_version_request",
            "GetReadVersionRequest"
        ),
        check!(
            GetReadVersionReply,
            "release-7.4",
            "get_read_version_reply",
            "GetReadVersionReply"
        ),
        check_reply!(
            GetReadVersionReply,
            GetReadVersion,
            V,
            "release-7.4",
            "get_read_version_reply_envelope",
            "ErrorOr<EnsureTable<GetReadVersionReply>>"
        ),
        check_request!(
            GetKeyServerLocationsRequest,
            GetKeyServerLocations,
            V,
            "release-7.4",
            "get_key_server_locations_request",
            "GetKeyServerLocationsRequest"
        ),
        check!(
            GetKeyServerLocationsReply,
            "release-7.4",
            "get_key_server_locations_reply",
            "GetKeyServerLocationsReply"
        ),
        check_reply!(
            GetKeyServerLocationsReply,
            GetKeyServerLocations,
            V,
            "release-7.4",
            "get_key_server_locations_reply_envelope",
            "ErrorOr<EnsureTable<GetKeyServerLocationsReply>>"
        ),
        check_request!(
            GetValueRequest,
            GetValue,
            V,
            "release-7.4",
            "get_value_request",
            "GetValueRequest"
        ),
        check_request!(
            GetValueRequest,
            GetValue,
            V,
            "release-7.4",
            "get_value_request_minimal",
            "GetValueRequest"
        ),
        check!(
            GetValueReply,
            "release-7.4",
            "get_value_reply",
            "GetValueReply"
        ),
        check_reply!(
            GetValueReply,
            GetValue,
            V,
            "release-7.4",
            "get_value_reply_envelope",
            "ErrorOr<EnsureTable<GetValueReply>>"
        ),
        check_reply!(
            GetValueReply,
            GetValue,
            V,
            "release-7.4",
            "get_value_reply_absent_value",
            "ErrorOr<EnsureTable<GetValueReply>>"
        ),
        check_reply!(
            GetValueReply,
            GetValue,
            V,
            "release-7.4",
            "get_value_reply_error",
            "ErrorOr<EnsureTable<GetValueReply>>"
        ),
        check_request!(
            CommitTransactionRequest,
            Commit,
            V,
            "release-7.4",
            "commit_transaction_request",
            "CommitTransactionRequest"
        ),
        check_request!(
            CommitTransactionRequest,
            Commit,
            V,
            "release-7.4",
            "commit_transaction_request_single_key_clear",
            "CommitTransactionRequest"
        ),
        check!(CommitID, "release-7.4", "commit_id", "CommitID"),
        check_reply!(
            CommitID,
            Commit,
            V,
            "release-7.4",
            "commit_id_envelope",
            "ErrorOr<EnsureTable<CommitID>>"
        ),
        check_reply!(
            CommitID,
            Commit,
            V,
            "release-7.4",
            "commit_id_error",
            "ErrorOr<EnsureTable<CommitID>>"
        ),
    ]
}

fn release_8_0() -> Vec<Outcome> {
    use v8_0::*;
    const V: SchemaVersion = SchemaVersion::V8_0;
    vec![
        check_request!(
            OpenDatabaseCoordRequest,
            OpenDatabaseCoord,
            V,
            "release-8.0",
            "open_database_coord_request",
            "OpenDatabaseCoordRequest"
        ),
        check!(
            ClientDBInfo,
            "release-8.0",
            "client_db_info",
            "ClientDBInfo"
        ),
        check_reply!(
            ClientDBInfo,
            OpenDatabaseCoord,
            V,
            "release-8.0",
            "client_db_info_envelope",
            "ErrorOr<EnsureTable<ClientDBInfo>>"
        ),
        check_request!(
            GetReadVersionRequest,
            GetReadVersion,
            V,
            "release-8.0",
            "get_read_version_request",
            "GetReadVersionRequest"
        ),
        check!(
            GetReadVersionReply,
            "release-8.0",
            "get_read_version_reply",
            "GetReadVersionReply"
        ),
        check_reply!(
            GetReadVersionReply,
            GetReadVersion,
            V,
            "release-8.0",
            "get_read_version_reply_envelope",
            "ErrorOr<EnsureTable<GetReadVersionReply>>"
        ),
        check_request!(
            GetKeyServerLocationsRequest,
            GetKeyServerLocations,
            V,
            "release-8.0",
            "get_key_server_locations_request",
            "GetKeyServerLocationsRequest"
        ),
        check!(
            GetKeyServerLocationsReply,
            "release-8.0",
            "get_key_server_locations_reply",
            "GetKeyServerLocationsReply"
        ),
        check_reply!(
            GetKeyServerLocationsReply,
            GetKeyServerLocations,
            V,
            "release-8.0",
            "get_key_server_locations_reply_envelope",
            "ErrorOr<EnsureTable<GetKeyServerLocationsReply>>"
        ),
        check_request!(
            GetValueRequest,
            GetValue,
            V,
            "release-8.0",
            "get_value_request",
            "GetValueRequest"
        ),
        check_request!(
            GetValueRequest,
            GetValue,
            V,
            "release-8.0",
            "get_value_request_minimal",
            "GetValueRequest"
        ),
        check!(
            GetValueReply,
            "release-8.0",
            "get_value_reply",
            "GetValueReply"
        ),
        check_reply!(
            GetValueReply,
            GetValue,
            V,
            "release-8.0",
            "get_value_reply_envelope",
            "ErrorOr<EnsureTable<GetValueReply>>"
        ),
        check_reply!(
            GetValueReply,
            GetValue,
            V,
            "release-8.0",
            "get_value_reply_absent_value",
            "ErrorOr<EnsureTable<GetValueReply>>"
        ),
        check_reply!(
            GetValueReply,
            GetValue,
            V,
            "release-8.0",
            "get_value_reply_error",
            "ErrorOr<EnsureTable<GetValueReply>>"
        ),
        check_request!(
            CommitTransactionRequest,
            Commit,
            V,
            "release-8.0",
            "commit_transaction_request",
            "CommitTransactionRequest"
        ),
        check_request!(
            CommitTransactionRequest,
            Commit,
            V,
            "release-8.0",
            "commit_transaction_request_single_key_clear",
            "CommitTransactionRequest"
        ),
        check!(CommitID, "release-8.0", "commit_id", "CommitID"),
        check_reply!(
            CommitID,
            Commit,
            V,
            "release-8.0",
            "commit_id_envelope",
            "ErrorOr<EnsureTable<CommitID>>"
        ),
        check_reply!(
            CommitID,
            Commit,
            V,
            "release-8.0",
            "commit_id_error",
            "ErrorOr<EnsureTable<CommitID>>"
        ),
    ]
}

/// Fixtures of messages outside the vertical slice (range reads, `getKey`, watches) and of
/// the transport layer.
const NOT_IN_THE_SLICE: [&str; 13] = [
    "get_key_values_request",
    "get_key_values_reply",
    "get_key_values_reply_envelope",
    "get_key_values_reply_empty",
    "get_key_request",
    "get_key_reply",
    "get_key_reply_envelope",
    "watch_value_request",
    "watch_value_reply",
    "watch_value_reply_envelope",
    "ping_request",
    "ping_reply",
    "endpoint_not_found",
];

fn report(outcomes: &[Outcome]) {
    let identical = outcomes.iter().filter(|o| o.identical).count();
    println!(
        "{}: {} fixtures match their dumps; {identical} re-encode byte-identically",
        outcomes[0].release,
        outcomes.len()
    );
    for o in outcomes {
        println!(
            "  {:<48} {}",
            o.name,
            if o.identical {
                "identical bytes"
            } else {
                "same fields, different layout"
            }
        );
    }
}

#[test]
fn release_7_4_fixtures() {
    report(&release_7_4());
}

#[test]
fn release_8_0_fixtures() {
    report(&release_8_0());
}

/// Every message fixture is either checked above or explicitly outside the slice.
#[test]
fn every_fixture_is_accounted_for() {
    for (release, outcomes) in [
        ("release-7.4", release_7_4()),
        ("release-8.0", release_8_0()),
    ] {
        let index = read_json(&fixtures_dir(release).join("index.json"));
        for file in index["positive"].as_array().expect("positive") {
            let name = file.as_str().expect("name").trim_end_matches(".json");
            assert!(
                outcomes.iter().any(|o| o.name == name) || NOT_IN_THE_SLICE.contains(&name),
                "{release}/{name} is neither checked nor listed as outside the slice"
            );
        }
    }
}

/// Both versions use one file identifier per message, so only the peer's protocol version
/// says which schema a request needs; with the wrong one, slots after the removed tenant
/// field are misread (here every case is caught by a strict check or a type error, which is
/// not guaranteed in general).
#[test]
fn the_wrong_version_misreads_requests() {
    for name in ["get_value_request", "get_value_request_minimal"] {
        let bytes = record_bytes(&fixture("release-7.4", name));
        assert!(
            decode::<v8_0::GetValueRequest>(&bytes).is_err(),
            "7.4 {name}"
        );
        let bytes = record_bytes(&fixture("release-8.0", name));
        assert!(
            decode::<v7_4::GetValueRequest>(&bytes).is_err(),
            "8.0 {name}"
        );
    }
    let bytes = record_bytes(&fixture("release-8.0", "commit_transaction_request"));
    assert!(matches!(
        decode::<v7_4::CommitTransactionRequest>(&bytes),
        Err(DecodeError::Strict(_) | DecodeError::OutOfBounds { .. } | DecodeError::Invalid(_))
    ));
}

/// The ignored C++ cross-check: the Rust encoding of every fixture (and the domain layer's
/// encoding of every request and reply envelope) is decoded by the release's own reader,
/// whose dump must match. Build the decoders with
/// `FDB_SRC=/path/to/foundationdb tools/golden-vectors/regen_messages.sh`, then:
///
/// ```text
/// FDB_GOLDEN_MESSAGES_BIN_7_4=.../build/msg/build-release-7.4/bin/golden_vectors_messages \
/// FDB_GOLDEN_MESSAGES_BIN_8_0=.../build/msg/build-release-8.0/bin/golden_vectors_messages \
///     cargo nextest run -p fdb-native-schema --run-ignored only -E 'test(cpp_crosscheck)'
/// ```
#[test]
#[ignore = "requires the message golden-vectors decoders"]
fn cpp_crosscheck_release_readers_decode_rust_messages() {
    let mut checked = 0;
    for (outcomes, var) in [
        (release_7_4(), "FDB_GOLDEN_MESSAGES_BIN_7_4"),
        (release_8_0(), "FDB_GOLDEN_MESSAGES_BIN_8_0"),
    ] {
        let bin =
            std::env::var_os(var).unwrap_or_else(|| panic!("set {var} (see this test's docs)"));
        for o in &outcomes {
            let what = format!("{}/{}", o.release, o.name);
            let dump = cpp_decode(&bin, o.cpp_type, &o.reencoded, &what);
            assert_json_eq(
                &format!("{what}: C++ reading the Rust re-encoding"),
                &o.root,
                &dump,
            );
            checked += 1;
            if let Some((bytes, rust_dump)) = &o.domain {
                let dump = cpp_decode(&bin, o.cpp_type, bytes, &what);
                assert_json_eq(
                    &format!("{what}: C++ reading the domain encoding"),
                    rust_dump,
                    &dump,
                );
                checked += 1;
            }
        }
    }
    println!("{checked} Rust-encoded messages read identically by the release readers");
}

/// The `root` the release's decoder prints for `bytes` as `cpp_type`.
fn cpp_decode(bin: &std::ffi::OsStr, cpp_type: &str, bytes: &[u8], what: &str) -> Value {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new(bin)
        .args(["decode", cpp_type])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn decoder");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(hex_encode(bytes).as_bytes())
        .expect("write");
    let out = child.wait_with_output().expect("decoder");
    assert!(
        out.status.success(),
        "{what}: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let dump: Value = serde_json::from_slice(&out.stdout).expect("decoder JSON");
    dump["root"].clone()
}
