//! The custom blob codecs (`KeyValueVector`, `VersionVector`, `TagSet`,
//! `IdempotencyIdRef`) and `Tag`, against the message-level golden vectors of both
//! releases, with hand-built blobs for the cases the vectors do not cover, hostile input
//! and proptest round trips.
//!
//! The enclosing messages are not modelled: [`At`] is a table whose only modelled field
//! is the one at a given slot (taken from the fixture's own dump), so these tests do not
//! depend on message definitions.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, EnsureTable, ErrorOr, IdempotencyId,
    IdempotencyIdRef, KeyValue, KeyValueRef, KeyValueVector, KeyValueVectorBuf, ObjectRef, Reader,
    Shape, Tag, TagSet, Uid, VersionVector, Wire, Writer, encode_with_identifier,
};
use proptest::prelude::*;
use serde_json::{Value, json};

const RELEASES: [&str; 2] = ["release-7.4", "release-8.0"];

// ---------------------------------------------------------------------------------------
// Helpers.

fn fixtures_dir(release: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tools/golden-vectors/fixtures")
        .join(release)
}

fn hex_decode(s: &str) -> Vec<u8> {
    assert!(s.len() % 2 == 0, "odd hex length");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Every message record of a release: (name, record).
fn records(release: &str) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(fixtures_dir(release)).expect("fixtures dir") {
        let path = entry.expect("entry").path();
        let name = path
            .file_stem()
            .expect("stem")
            .to_string_lossy()
            .to_string();
        if path.extension().is_none_or(|e| e != "json") || name == "index" || name == "schemas" {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("read fixture");
        out.push((name, serde_json::from_str(&text).expect("parse fixture")));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn record_bytes(record: &Value) -> Vec<u8> {
    hex_decode(record["bytes"].as_str().expect("bytes"))
}

/// Every field node (`{"field", "slot", "type", "value"}`) named `field` in the dump.
fn field_nodes<'a>(node: &'a Value, field: &str, out: &mut Vec<&'a Value>) {
    match node {
        Value::Object(m) => {
            if m.get("field").and_then(Value::as_str) == Some(field) && m.contains_key("slot") {
                out.push(node);
            }
            m.values().for_each(|v| field_nodes(v, field, out));
        }
        Value::Array(a) => a.iter().for_each(|v| field_nodes(v, field, out)),
        _ => {}
    }
}

/// A table whose only modelled field is at slot `SLOT`.
#[derive(Debug, PartialEq)]
struct At<T, const SLOT: usize>(T);

impl<T, const SLOT: usize> Wire for At<T, SLOT> {
    const SHAPE: Shape = Shape::Table;
}

impl<T: Encode, const SLOT: usize> Encode for At<T, SLOT> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.skip(SLOT);
        t.field(&self.0)?;
        t.finish()
    }
}

impl<'de, T: Decode<'de>, const SLOT: usize> Decode<'de> for At<T, SLOT> {
    fn absent() -> Self {
        At(T::absent())
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        t.skip(SLOT);
        Ok(At(t.field()?))
    }
}

fn probe<'a, T: Decode<'a>, const SLOT: usize>(
    bytes: &'a [u8],
    envelope: bool,
) -> Result<T, DecodeError> {
    let r = Reader::new(bytes)?;
    if envelope {
        match r.decode_root::<ErrorOr<EnsureTable<At<T, SLOT>>>>()? {
            Ok(EnsureTable(At(value))) => Ok(value),
            Err(_) => Err(DecodeError::Invalid("error envelope")),
        }
    } else {
        r.decode_root::<At<T, SLOT>>().map(|At(value)| value)
    }
}

/// Decodes the field at `slot` of the message's first table (inside the reply envelope if
/// `envelope`).
fn read_at<'a, T: Decode<'a>>(
    bytes: &'a [u8],
    slot: usize,
    envelope: bool,
) -> Result<T, DecodeError> {
    macro_rules! dispatch {
        ($($n:literal)*) => {
            match slot {
                $($n => probe::<T, $n>(bytes, envelope),)*
                _ => panic!("slot {slot} is not wired up in this test"),
            }
        };
    }
    dispatch!(0 1 2 3 4 5 6 7 8 9 10 11 12 13)
}

fn write_at<T: Encode>(value: &T, slot: usize) -> Vec<u8> {
    macro_rules! dispatch {
        ($($n:literal)*) => {
            match slot {
                $($n => encode_with_identifier(&At::<&T, $n>(value), 0x000f_db99),)*
                _ => panic!("slot {slot} is not wired up in this test"),
            }
        };
    }
    dispatch!(0 1 2 3 4 5 6 7 8 9 10 11 12 13).expect("encode")
}

/// The raw blob at `slot` (or `None` for an absent `Optional`).
fn raw_blob(bytes: &[u8], slot: usize, envelope: bool, optional: bool) -> Option<Vec<u8>> {
    if optional {
        read_at::<Option<&[u8]>>(bytes, slot, envelope)
            .expect("raw optional")
            .map(<[u8]>::to_vec)
    } else {
        Some(
            read_at::<&[u8]>(bytes, slot, envelope)
                .expect("raw blob")
                .to_vec(),
        )
    }
}

/// Decodes the field and dumps it in the fixtures' node format.
type DumpFn<'f> = &'f dyn Fn(&[u8], usize, bool) -> Result<Value, DecodeError>;
/// Decodes the field and writes it into a new message.
type ReencodeFn<'f> = &'f dyn Fn(&[u8], usize, bool) -> Result<Vec<u8>, DecodeError>;

/// The two closures of [`check_field`] for a decodable and encodable type.
macro_rules! codec_fns {
    ($t:ty, $dump:expr) => {{
        let dump: DumpFn<'static> = &|bytes: &[u8], slot: usize, envelope: bool| {
            read_at::<$t>(bytes, slot, envelope).map(|v| ($dump)(&v))
        };
        let reencode: ReencodeFn<'static> = &|bytes: &[u8], slot: usize, envelope: bool| {
            read_at::<$t>(bytes, slot, envelope).map(|v| write_at(&v, slot))
        };
        (dump, reencode)
    }};
}

/// The checks every fixture field gets: decodes to the dump, re-encodes to the same blob,
/// and the re-encoded message decodes to the same dump.
fn check_field(
    what: &str,
    bytes: &[u8],
    node: &Value,
    envelope: bool,
    optional: bool,
    (dump, reencode): (DumpFn<'_>, ReencodeFn<'_>),
) {
    let slot = node["slot"].as_u64().expect("slot") as usize;
    let value = dump(bytes, slot, envelope).unwrap_or_else(|e| panic!("{what}: {e}"));
    assert_eq!(value, node["value"], "{what}");

    let reencoded = reencode(bytes, slot, envelope).unwrap_or_else(|e| panic!("{what}: {e}"));
    assert_eq!(
        raw_blob(bytes, slot, envelope, optional),
        raw_blob(&reencoded, slot, false, optional),
        "{what}: re-encoded blob is not byte-identical"
    );
    let again = dump(&reencoded, slot, false).unwrap_or_else(|e| panic!("{what}: {e}"));
    assert_eq!(value, again, "{what}");
}

// ---------------------------------------------------------------------------------------
// Fixtures.

fn kv_dump(v: &KeyValueVector<'_>) -> Value {
    Value::Array(
        v.iter()
            .map(|kv| json!({ "key": hex_encode(kv.key), "value": hex_encode(kv.value) }))
            .collect(),
    )
}

fn vv_dump(v: &VersionVector) -> Value {
    let versions: Vec<Value> = v
        .versions
        .iter()
        .map(|(t, version)| json!({ "locality": t.locality, "id": t.id, "version": version }))
        .collect();
    json!({ "versions": versions, "maxVersion": v.max_version })
}

fn tags_dump(v: &Option<TagSet<'_>>) -> Value {
    match v {
        None => Value::Null,
        Some(set) => {
            json!({ "type": "tag_set", "value": set.iter().map(hex_encode).collect::<Vec<_>>() })
        }
    }
}

/// One blob-codec field of a fixture message, with the closures to decode it.
struct Case<'a> {
    /// "kv", "vv", "tags" or "idempotency".
    kind: &'static str,
    field: String,
    node: &'a Value,
    /// An `Optional` field (its raw form is a union).
    optional: bool,
    fns: (DumpFn<'static>, ReencodeFn<'static>),
}

/// The fields of a message fixture that use the blob codecs. A field with two Rust
/// types (borrowed and owned) gives two cases.
fn blob_cases(record: &Value) -> Vec<Case<'_>> {
    let mut cases = Vec::new();
    let mut nodes = Vec::new();

    field_nodes(&record["root"], "data", &mut nodes);
    for node in nodes.drain(..) {
        if node["type"] == "preserialized_kv_vector" {
            for fns in [
                codec_fns!(KeyValueVector, kv_dump),
                codec_fns!(KeyValueVectorBuf, |v: &KeyValueVectorBuf| kv_dump(
                    &v.as_view()
                )),
            ] {
                cases.push(Case {
                    kind: "kv",
                    field: "data".into(),
                    node,
                    optional: false,
                    fns,
                });
            }
        }
    }
    for field in ["ssLatestCommitVersions", "ssVersionVectorDelta"] {
        field_nodes(&record["root"], field, &mut nodes);
    }
    for node in nodes.drain(..) {
        assert_eq!(node["type"], "version_vector");
        let field = node["field"].as_str().expect("field").to_string();
        let fns = codec_fns!(VersionVector, vv_dump);
        cases.push(Case {
            kind: "vv",
            field,
            node,
            optional: false,
            fns,
        });
    }
    for field in ["tags", "tagSet"] {
        field_nodes(&record["root"], field, &mut nodes);
    }
    for node in nodes.drain(..) {
        // GetReadVersionRequest::tags is a different `tags` (a map).
        if node["type"] == "optional<tag_set>" {
            let field = node["field"].as_str().expect("field").to_string();
            let fns = codec_fns!(Option<TagSet>, tags_dump);
            cases.push(Case {
                kind: "tags",
                field,
                node,
                optional: true,
                fns,
            });
        }
    }
    field_nodes(&record["root"], "idempotencyId", &mut nodes);
    for node in nodes.drain(..) {
        for fns in [
            codec_fns!(IdempotencyIdRef, |id: &IdempotencyIdRef| json!(hex_encode(
                id.as_bytes()
            ))),
            codec_fns!(IdempotencyId, |id: &IdempotencyId| json!(hex_encode(
                id.as_bytes()
            ))),
        ] {
            let field = "idempotencyId".to_string();
            cases.push(Case {
                kind: "idempotency",
                field,
                node,
                optional: false,
                fns,
            });
        }
    }
    cases
}

fn is_envelope(record: &Value) -> bool {
    record["cpp_type"]
        .as_str()
        .is_some_and(|t| t.starts_with("ErrorOr<"))
}

#[test]
fn message_fixtures_decode_and_reencode_blobs() {
    let mut covered: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for release in RELEASES {
        for (name, record) in records(release) {
            let bytes = record_bytes(&record);
            for case in blob_cases(&record) {
                let what = format!("{release}/{name} {}", case.field);
                check_field(
                    &what,
                    &bytes,
                    case.node,
                    is_envelope(&record),
                    case.optional,
                    case.fns,
                );
                covered.entry(case.kind).or_default().push(what);
            }
        }
    }
    // Per release: 3 KV vectors (each with two Rust types); version vectors in
    // get_key_request, get_key_values_request, get_read_version_reply and its envelope,
    // get_value_request and its minimal form; tag sets in get_key_request,
    // get_key_values_request, get_value_request (two fields: present and absent),
    // watch_value_request and the two commit requests; idempotency ids in the two commit
    // requests (borrowed and owned).
    assert_eq!(covered["kv"].len(), 2 * 3 * 2, "{:?}", covered["kv"]);
    assert_eq!(covered["vv"].len(), 2 * 6, "{:?}", covered["vv"]);
    assert_eq!(covered["tags"].len(), 2 * 7, "{:?}", covered["tags"]);
    assert_eq!(
        covered["idempotency"].len(),
        2 * 2 * 2,
        "{:?}",
        covered["idempotency"]
    );
}

/// Ignored by default: sends every non-envelope fixture's blob fields, re-encoded by this
/// crate into a message of the fixture's own type, to the release's own C++ reader
/// (`golden_vectors_messages decode`, built by `tools/golden-vectors/regen_messages.sh`)
/// and compares what it reads with the fixture's dump.
///
/// ```text
/// FDB_GOLDEN_MESSAGES_BIN_7_4=.../build/msg/build-release-7.4/bin/golden_vectors_messages \
/// FDB_GOLDEN_MESSAGES_BIN_8_0=.../build/msg/build-release-8.0/bin/golden_vectors_messages \
///     cargo nextest run -p fdb-native-wire --run-ignored only -E 'binary(blob_codecs)'
/// ```
#[test]
#[ignore = "requires the message golden-vectors decoders"]
fn release_readers_decode_reencoded_blobs() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut checked = 0;
    for (release, var) in [
        ("release-7.4", "FDB_GOLDEN_MESSAGES_BIN_7_4"),
        ("release-8.0", "FDB_GOLDEN_MESSAGES_BIN_8_0"),
    ] {
        let bin =
            std::env::var_os(var).unwrap_or_else(|| panic!("set {var} (see this test's docs)"));
        for (name, record) in records(release) {
            if is_envelope(&record) {
                continue;
            }
            let cpp_type = record["cpp_type"].as_str().expect("cpp_type");
            let fid = u32::try_from(record["file_identifier"].as_u64().expect("fid")).expect("fid");
            let bytes = record_bytes(&record);
            for case in blob_cases(&record) {
                let slot = case.node["slot"].as_u64().expect("slot") as usize;
                let (_, reencode) = case.fns;
                let mut message = reencode(&bytes, slot, false).expect("re-encode");
                message[4..8].copy_from_slice(&fid.to_le_bytes());
                let mut child = Command::new(&bin)
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
                    .write_all(hex_encode(&message).as_bytes())
                    .expect("write");
                let out = child.wait_with_output().expect("decoder");
                let what = format!("{release}/{name} {}", case.field);
                assert!(
                    out.status.success(),
                    "{what}: {}{}",
                    String::from_utf8_lossy(&out.stdout),
                    String::from_utf8_lossy(&out.stderr)
                );
                let dump: Value = serde_json::from_slice(&out.stdout).expect("decoder JSON");
                let mut found = Vec::new();
                field_nodes(&dump["root"], &case.field, &mut found);
                let theirs = found
                    .iter()
                    .find(|n| n["slot"] == case.node["slot"])
                    .unwrap_or_else(|| panic!("{what}: field not in the C++ dump"));
                assert_eq!(theirs["value"], case.node["value"], "{what}");
                checked += 1;
            }
        }
    }
    println!("{checked} blob fields read identically by the release readers");
}

/// A table `pair<A, B>` as `std::pair` serializes in a container.
#[derive(Debug, PartialEq)]
struct Pair<A, B>(A, B);

impl<A, B> Wire for Pair<A, B> {
    const SHAPE: Shape = Shape::Table;
}

impl<A: Encode, B: Encode> Encode for Pair<A, B> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.0)?;
        t.field(&self.1)?;
        t.finish()
    }
}

impl<'de, A: Decode<'de>, B: Decode<'de>> Decode<'de> for Pair<A, B> {
    fn absent() -> Self {
        Pair(A::absent(), B::absent())
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(Pair(t.field()?, t.field()?))
    }
}

#[test]
fn tag_structs_in_key_server_locations_reply() {
    let mut seen = 0;
    for release in RELEASES {
        for (name, record) in records(release) {
            if !name.starts_with("get_key_server_locations_reply") {
                continue;
            }
            let envelope = name.ends_with("_envelope");
            let bytes = record_bytes(&record);
            let mut nodes = Vec::new();
            field_nodes(&record["root"], "resultsTagMapping", &mut nodes);
            assert_eq!(nodes.len(), 1);
            let slot = nodes[0]["slot"].as_u64().expect("slot") as usize;
            let mapping: Vec<Pair<Uid, Tag>> = read_at(&bytes, slot, envelope).expect("decode");
            let dumped: Vec<(u64, u64, i64, u64)> = nodes[0]["value"]
                .as_array()
                .expect("elements")
                .iter()
                .map(|e| {
                    let pair = e["value"].as_array().expect("pair");
                    let uid = &pair[0]["value"];
                    let tag = pair[1]["value"].as_array().expect("tag");
                    (
                        uid["first"].as_u64().expect("first"),
                        uid["second"].as_u64().expect("second"),
                        tag[1]["value"].as_i64().expect("locality"),
                        tag[0]["value"].as_u64().expect("id"),
                    )
                })
                .collect();
            let decoded: Vec<_> = mapping
                .iter()
                .map(|Pair(uid, tag)| {
                    (
                        uid.first,
                        uid.second,
                        i64::from(tag.locality),
                        u64::from(tag.id),
                    )
                })
                .collect();
            assert_eq!(decoded, dumped, "{release}/{name}");
            assert!(
                decoded
                    .iter()
                    .any(|&(_, _, locality, id)| locality == -2 && id == 300)
            );

            let again = write_at(&mapping, slot);
            let back: Vec<Pair<Uid, Tag>> = read_at(&again, slot, false).expect("re-decode");
            assert_eq!(back, mapping);
            seen += 1;
        }
    }
    assert_eq!(seen, 4);
}

// ---------------------------------------------------------------------------------------
// Messages with arbitrary blobs.

/// Decodes `blob` as the field of a message that carries it as a plain string.
fn through_message<R>(
    blob: &[u8],
    f: impl FnOnce(&[u8]) -> Result<R, DecodeError>,
) -> Result<R, DecodeError> {
    f(&write_at(&blob, 0))
}

fn kv_blob(items: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut out = (items.len() as u32).to_le_bytes().to_vec();
    for (k, v) in items {
        out.extend((k.len() as u32).to_le_bytes());
        out.extend(*k);
        out.extend((v.len() as u32).to_le_bytes());
        out.extend(*v);
    }
    out
}

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

fn decode_kv(blob: &[u8]) -> Result<Rows, DecodeError> {
    through_message(blob, |msg| {
        let v: KeyValueVector = read_at(msg, 0, false)?;
        Ok(v.iter()
            .map(|kv| (kv.key.to_vec(), kv.value.to_vec()))
            .collect())
    })
}

#[test]
fn kv_vector_decodes_c_plus_plus_payloads() {
    // GetKeyValuesReply fixture payload: three rows, one with an empty value.
    let blob = kv_blob(&[
        (b"range/b", b"value-b"),
        (b"range/c", b""),
        (b"range/d\xff", b"\x00\x01"),
    ]);
    let rows = decode_kv(&blob).expect("valid");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[1], (b"range/c".to_vec(), vec![]));
    // The empty vector is a 4-byte blob, not the shared empty blob.
    let empty = write_at(&KeyValueVector::EMPTY, 0);
    assert_eq!(raw_blob(&empty, 0, false, false), Some(vec![0; 4]));
    assert_eq!(decode_kv(&[0; 4]).expect("empty"), vec![]);
    // Owned and borrowed views agree.
    let buf: KeyValueVectorBuf = [KeyValueRef::new(b"a", b"1")].into_iter().collect();
    assert_eq!(buf.iter().next(), Some(KeyValueRef::new(b"a", b"1")));
    assert_eq!(buf.as_view().to_vec(), vec![KeyValue::new("a", "1")]);
}

#[test]
fn kv_vector_rejects_hostile_blobs() {
    let good = kv_blob(&[(b"k1", b"v1"), (b"k2", b"")]);
    assert!(decode_kv(&good).is_ok());
    // Every strict prefix, including the shared empty blob.
    for len in 0..good.len() {
        assert!(decode_kv(&good[..len]).is_err(), "prefix {len}");
    }
    // Trailing bytes.
    let mut trailing = good.clone();
    trailing.push(0);
    assert!(decode_kv(&trailing).is_err());
    // A count bigger than the blob, at the extremes (no allocation from it).
    for count in [3u32, 1 << 20, u32::MAX / 8, u32::MAX / 8 + 1, u32::MAX] {
        let mut blob = good.clone();
        blob[..4].copy_from_slice(&count.to_le_bytes());
        assert!(decode_kv(&blob).is_err(), "count {count}");
    }
    // A fixed-size blob whose count promises more items than bytes (each takes at least 8).
    let mut small = vec![0u8; 4 + 8];
    small[..4].copy_from_slice(&2u32.to_le_bytes());
    assert!(decode_kv(&small).is_err());
    // Lengths that overrun, or that overflow when added to the position.
    for bad in [u32::MAX, u32::MAX - 3, 1 << 31, good.len() as u32] {
        let mut key = good.clone();
        key[4..8].copy_from_slice(&bad.to_le_bytes());
        assert!(decode_kv(&key).is_err(), "key length {bad}");
        let mut value = good.clone();
        value[4 + 4 + 2..4 + 4 + 2 + 4].copy_from_slice(&bad.to_le_bytes());
        assert!(decode_kv(&value).is_err(), "value length {bad}");
    }
    // An item shorter than it claims followed by another item that "fixes" the sum.
    let mut crossed = kv_blob(&[(b"abcd", b"")]);
    crossed[4..8].copy_from_slice(&0u32.to_le_bytes());
    assert!(decode_kv(&crossed).is_err());
}

#[test]
fn version_vector_hostile_blobs_are_rejected() {
    let good = vv_blob(&VvSpec::default());
    assert!(VersionVector::from_payload(&good).is_ok());
    for len in 0..good.len() {
        assert!(
            VersionVector::from_payload(&good[..len]).is_err(),
            "prefix {len}"
        );
    }
    let mut trailing = good.clone();
    trailing.push(0);
    assert!(VersionVector::from_payload(&trailing).is_err());

    let reject = |name: &str, spec: VvSpec| {
        let blob = vv_blob(&spec);
        assert!(
            VersionVector::from_payload(&blob).is_err(),
            "{name}: accepted {}",
            hex_encode(&blob)
        );
    };
    for bad in [0u8, 3, 4, 8, 255] {
        reject(
            "tag width",
            VvSpec {
                tag_width: bad,
                ..VvSpec::default()
            },
        );
    }
    for bad in [0u8, 3, 5, 6, 7, 16, 255] {
        reject(
            "delta width",
            VvSpec {
                delta_width: bad,
                ..VvSpec::default()
            },
        );
    }
    reject(
        "pair count too big",
        VvSpec {
            pairs: Some(1 << 40),
            ..VvSpec::default()
        },
    );
    reject(
        "pair count too small",
        VvSpec {
            pairs: Some(1),
            ..VvSpec::default()
        },
    );
    reject(
        "pair count max",
        VvSpec {
            pairs: Some(u64::MAX),
            ..VvSpec::default()
        },
    );
    for count in [0u64, 1, 5, 1 << 40, u64::MAX / 3 + 1, u64::MAX] {
        // A locality count the blob cannot hold (0 is the empty form, which then has
        // trailing bytes).
        reject(
            "locality count",
            VvSpec {
                localities: Some(count),
                ..VvSpec::default()
            },
        );
    }
    reject(
        "zero-length run",
        VvSpec {
            runs: vec![(0, 0), (1, 3)],
            ..VvSpec::default()
        },
    );
    reject(
        "run total",
        VvSpec {
            runs: vec![(0, 2), (1, 2)],
            ..VvSpec::default()
        },
    );
    reject(
        "unsorted localities",
        VvSpec {
            runs: vec![(1, 1), (0, 2)],
            ..VvSpec::default()
        },
    );
    reject(
        "repeated locality",
        VvSpec {
            runs: vec![(0, 1), (0, 2)],
            ..VvSpec::default()
        },
    );
    reject(
        "invalid locality",
        VvSpec {
            runs: vec![(-99, 3)],
            ..VvSpec::default()
        },
    );
    reject(
        "invalid locality",
        VvSpec {
            runs: vec![(i8::MIN, 3)],
            ..VvSpec::default()
        },
    );
    reject(
        "unsorted ids",
        VvSpec {
            entries: vec![(5, 0), (4, 1), (9, 2)],
            ..VvSpec::default()
        },
    );
    reject(
        "repeated ids",
        VvSpec {
            entries: vec![(5, 0), (5, 1), (9, 2)],
            ..VvSpec::default()
        },
    );
    // Runs whose lengths are huge but whose entries are absent.
    reject(
        "run of 65535",
        VvSpec {
            runs: vec![(0, u16::MAX)],
            pairs: Some(65535),
            entries: vec![],
            ..VvSpec::default()
        },
    );
    // A delta that pushes the version past i64::MAX.
    reject(
        "version overflow",
        VvSpec {
            delta_width: 8,
            min: i64::MAX - 1,
            entries: vec![(1, 0), (2, 1), (3, 2)],
            ..VvSpec::default()
        },
    );
    // An empty vector with anything after maxVersion, or less than it.
    assert!(VersionVector::from_payload(&[0; 15]).is_err());
    assert!(VersionVector::from_payload(&[0; 17]).is_err());
    assert!(VersionVector::from_payload(&[]).is_err());
}

/// The knobs of a hand-built VersionVector blob (every field the C++ writer emits).
#[derive(Clone)]
struct VvSpec {
    /// The locality count, if different from `runs.len()`.
    localities: Option<u64>,
    runs: Vec<(i8, u16)>,
    tag_width: u8,
    delta_width: u8,
    min: i64,
    /// The pair count, if different from the sum of the runs.
    pairs: Option<u64>,
    /// (tag id, version delta) per entry.
    entries: Vec<(u16, u64)>,
    max: i64,
}

impl Default for VvSpec {
    /// Two localities with three entries: (-2: ids 5 and 9; 1: id 4).
    fn default() -> Self {
        VvSpec {
            localities: None,
            runs: vec![(-2, 2), (1, 1)],
            tag_width: 1,
            delta_width: 2,
            min: 1_000,
            pairs: None,
            entries: vec![(5, 0), (9, 300), (4, 17)],
            max: 5_000,
        }
    }
}

fn vv_blob(spec: &VvSpec) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(
        spec.localities
            .unwrap_or(spec.runs.len() as u64)
            .to_le_bytes(),
    );
    for &(locality, len) in &spec.runs {
        out.extend(locality.to_le_bytes());
        out.extend(len.to_le_bytes());
    }
    if !spec.runs.is_empty() || spec.localities.is_some() {
        out.push(spec.tag_width);
        out.push(spec.delta_width);
        out.extend(spec.min.to_le_bytes());
        let total: u64 = spec.runs.iter().map(|r| u64::from(r.1)).sum();
        out.extend(spec.pairs.unwrap_or(total).to_le_bytes());
        for &(id, delta) in &spec.entries {
            match spec.tag_width {
                1 => out.push(id as u8),
                _ => out.extend(id.to_le_bytes()),
            }
            match spec.delta_width {
                1 => out.push(delta as u8),
                2 => out.extend((delta as u16).to_le_bytes()),
                4 => out.extend((delta as u32).to_le_bytes()),
                _ => out.extend(delta.to_le_bytes()),
            }
        }
    }
    out.extend(spec.max.to_le_bytes());
    out
}

#[test]
fn version_vector_decodes_every_width_combination() {
    for tag_width in [1u8, 2] {
        for delta_width in [1u8, 2, 4, 8] {
            let spec = VvSpec {
                tag_width,
                delta_width,
                entries: vec![(5, 0), (9, 200), (4, 255)],
                ..VvSpec::default()
            };
            let blob = vv_blob(&spec);
            let vv = VersionVector::from_payload(&blob)
                .unwrap_or_else(|e| panic!("widths {tag_width}/{delta_width}: {e}"));
            let expected: BTreeMap<Tag, i64> = [
                (Tag::new(-2, 5), 1_000),
                (Tag::new(-2, 9), 1_200),
                (Tag::new(1, 4), 1_255),
            ]
            .into_iter()
            .collect();
            assert_eq!(vv.versions, expected, "widths {tag_width}/{delta_width}");
            assert_eq!(vv.max_version, 5_000);
            // The encoder re-picks the narrowest widths; that decodes the same, and is
            // byte-identical exactly when the blob was already narrowest (1/1, as the
            // spread is 255 and the ids are below 256).
            let again = vv.to_payload().expect("encode");
            assert_eq!(VersionVector::from_payload(&again).expect("re-decode"), vv);
            assert_eq!(again == blob, (tag_width, delta_width) == (1, 1));
        }
    }
}

#[test]
fn version_vector_wide_values_and_extremes() {
    // 2-byte ids need the wide form, and deltas past u32 the 8-byte one.
    let spec = VvSpec {
        runs: vec![(-98, 1), (127, 2)],
        tag_width: 2,
        delta_width: 8,
        min: i64::MIN,
        entries: vec![(300, 0), (0, u64::MAX >> 1), (65535, u64::MAX)],
        max: i64::MAX,
        ..VvSpec::default()
    };
    let blob = vv_blob(&spec);
    let vv = VersionVector::from_payload(&blob).expect("decode");
    assert_eq!(vv.versions[&Tag::new(-98, 300)], i64::MIN);
    assert_eq!(vv.versions[&Tag::new(127, 0)], -1);
    assert_eq!(vv.versions[&Tag::new(127, 65535)], i64::MAX);
    assert_eq!(vv.to_payload().expect("encode"), blob);

    // The empty vector, and a default (maxVersion -1).
    assert_eq!(
        VersionVector::from_payload(&vv_blob(&VvSpec {
            runs: vec![],
            entries: vec![],
            max: -1,
            ..VvSpec::default()
        }))
        .expect("empty"),
        VersionVector::default()
    );
    assert_eq!(VersionVector::default().max_version, -1);
}

#[test]
fn version_vector_encoder_matches_the_documented_layout() {
    // The fixture of get_key_values_request: negative locality, id 300, 8-byte-range deltas.
    let mut vv = VersionVector::new(2_000_000_000_000);
    vv.versions.insert(Tag::new(-2, 300), 1_000_000_000_000);
    vv.versions.insert(Tag::new(1, 2), 1_999_999_999_999);
    let spec = VvSpec {
        runs: vec![(-2, 1), (1, 1)],
        tag_width: 2,
        delta_width: 8, // 999_999_999_999 > u32::MAX
        min: 1_000_000_000_000,
        entries: vec![(300, 0), (2, 999_999_999_999)],
        max: 2_000_000_000_000,
        ..VvSpec::default()
    };
    assert_eq!(vv.to_payload().expect("encode"), vv_blob(&spec));
}

#[test]
fn tag_set_hostile_blobs_are_rejected() {
    let decode = |blob: &[u8]| {
        through_message(blob, |msg| {
            read_at::<TagSet>(msg, 0, false)
                .map(|s| s.iter().map(<[u8]>::to_vec).collect::<Vec<_>>())
        })
    };
    assert_eq!(decode(b"").expect("empty"), Vec::<Vec<u8>>::new());
    assert_eq!(
        decode(b"\x01a\x00\x02bc").expect("tags"),
        vec![b"a".to_vec(), vec![], b"bc".to_vec()]
    );
    for bad in [&b"\x01"[..], b"\x02a", b"\xff", b"\x01a\x03xy", b"\x00\x05"] {
        assert!(decode(bad).is_err(), "{bad:?}");
    }
    // 255-byte tags are the longest the format holds; the encoder refuses longer ones.
    let long = vec![9u8; 256];
    let set = TagSet::new(vec![&long]);
    assert!(matches!(
        set.to_payload(),
        Err(EncodeError::InvalidShape(_))
    ));
    let mut w = Writer::new();
    let mut t = w.table();
    assert!(t.field(&set).is_err());
}

#[test]
fn idempotency_id_lengths() {
    let decode = |blob: &[u8]| {
        through_message(blob, |msg| {
            read_at::<IdempotencyIdRef>(msg, 0, false).map(|id| id.as_bytes().len())
        })
    };
    assert_eq!(decode(b"").expect("invalid id"), 0);
    for len in 1..16 {
        assert!(decode(&vec![1; len]).is_err(), "len {len}");
    }
    for len in [16, 17, 25, 128, 255] {
        assert_eq!(decode(&vec![1; len]).expect("valid"), len);
    }
    for len in [256, 257, 1000] {
        assert!(decode(&vec![1; len]).is_err(), "len {len}");
    }
    assert!(IdempotencyIdRef::new(&[0; 15]).is_err());
    assert!(IdempotencyId::new(vec![0; 256]).is_err());
    assert!(IdempotencyId::new(Vec::new()).is_ok_and(|id| !id.borrowed().is_valid()));
}

#[test]
fn absent_fields_decode_as_c_plus_plus_defaults() {
    // A table with every slot absent.
    let bytes = encode_with_identifier(&At::<u8, 7>(0), 1).expect("encode");
    assert_eq!(
        read_at::<VersionVector>(&bytes, 0, false).expect("vv"),
        VersionVector::default()
    );
    assert_eq!(VersionVector::default().max_version, -1);
    assert_eq!(
        read_at::<KeyValueVector>(&bytes, 1, false).expect("kv"),
        KeyValueVector::EMPTY
    );
    assert_eq!(
        read_at::<TagSet>(&bytes, 2, false).expect("tags"),
        TagSet::default()
    );
    assert_eq!(
        read_at::<Option<TagSet>>(&bytes, 3, false).expect("tags"),
        None
    );
    assert!(
        !read_at::<IdempotencyIdRef>(&bytes, 4, false)
            .expect("id")
            .is_valid()
    );
    assert_eq!(
        read_at::<Tag>(&bytes, 5, false).expect("tag"),
        Tag::new(-1, 0)
    );
}

#[test]
fn present_but_empty_optional_tag_set_stays_present() {
    let some = Some(TagSet::default());
    let bytes = write_at(&some, 2);
    assert_eq!(
        read_at::<Option<TagSet>>(&bytes, 2, false).expect("decode"),
        some
    );
    let none: Option<TagSet> = None;
    let bytes = write_at(&none, 2);
    assert_eq!(
        read_at::<Option<TagSet>>(&bytes, 2, false).expect("decode"),
        none
    );
}

// ---------------------------------------------------------------------------------------
// Hostile messages.

/// Every truncation and a sweep of single-byte corruptions of the fixture messages decode
/// to a value or an error, never a panic, whatever slot is read.
#[test]
fn corrupted_fixture_messages_never_panic() {
    for release in RELEASES {
        for (name, record) in records(release) {
            let bytes = record_bytes(&record);
            let envelope = record["cpp_type"]
                .as_str()
                .is_some_and(|t| t.starts_with("ErrorOr<"));
            let mut mutants = Vec::new();
            for len in 0..bytes.len() {
                mutants.push(bytes[..len].to_vec());
            }
            for at in 0..bytes.len() {
                for xor in [0x01u8, 0x80, 0xff] {
                    let mut m = bytes.clone();
                    m[at] ^= xor;
                    mutants.push(m);
                }
            }
            for m in &mutants {
                for slot in 0..13 {
                    let _ = read_at::<KeyValueVector>(m, slot, envelope).map(|v| v.iter().count());
                    let _ = read_at::<VersionVector>(m, slot, envelope);
                    let _ = read_at::<Option<TagSet>>(m, slot, envelope);
                    let _ = read_at::<IdempotencyIdRef>(m, slot, envelope);
                    let _ = read_at::<Vec<Pair<Uid, Tag>>>(m, slot, envelope);
                }
            }
            let _ = name;
        }
    }
}

// ---------------------------------------------------------------------------------------
// Property tests.

fn tag_strategy() -> impl Strategy<Value = Tag> {
    (-98i8..=127, prop_oneof![0u16..300, any::<u16>()]).prop_map(|(l, id)| Tag::new(l, id))
}

fn vv_strategy() -> impl Strategy<Value = VersionVector> {
    let spread = prop_oneof![
        Just(0u64),
        Just(200),
        Just(65_535),
        Just(1 << 32),
        Just(1 << 40),
        Just(u64::MAX >> 1)
    ];
    (any::<i64>(), spread, any::<i64>()).prop_flat_map(|(base, spread, max)| {
        // base + delta stays inside i64.
        let base = base.clamp(i64::MIN, i64::MAX - (spread as i64).max(0));
        prop::collection::btree_map(tag_strategy(), 0..=spread, 0..40).prop_map(move |m| {
            let mut vv = VersionVector::new(max);
            for (tag, delta) in m {
                vv.versions.insert(tag, base + delta as i64);
            }
            vv
        })
    })
}

fn model_kv(items: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let refs: Vec<(&[u8], &[u8])> = items.iter().map(|(k, v)| (&k[..], &v[..])).collect();
    kv_blob(&refs)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn version_vector_round_trips(vv in vv_strategy()) {
        let blob = vv.to_payload().unwrap();
        prop_assert_eq!(blob.len(), vv.encoded_len().unwrap());
        let back = VersionVector::from_payload(&blob).unwrap();
        prop_assert_eq!(&back, &vv);
        // Canonical: decoding then encoding is byte-identical.
        prop_assert_eq!(back.to_payload().unwrap(), blob.clone());
        // Through a message, as a table field.
        let msg = write_at(&vv, 3);
        prop_assert_eq!(read_at::<VersionVector>(&msg, 3, false).unwrap(), vv);
        prop_assert_eq!(raw_blob(&msg, 3, false, false), Some(blob));
    }

    #[test]
    fn version_vector_mutations_never_panic(
        vv in vv_strategy(),
        edits in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 0..4),
        cut in prop::option::of(any::<prop::sample::Index>()),
    ) {
        let mut blob = vv.to_payload().unwrap();
        for (at, byte) in edits {
            let i = at.index(blob.len());
            blob[i] = byte;
        }
        if let Some(cut) = cut {
            blob.truncate(cut.index(blob.len() + 1));
        }
        if let Ok(decoded) = VersionVector::from_payload(&blob) {
            // Whatever is accepted re-encodes to something that decodes the same.
            let again = decoded.to_payload().unwrap();
            prop_assert_eq!(VersionVector::from_payload(&again).unwrap(), decoded);
        }
    }

    #[test]
    fn arbitrary_bytes_never_panic(blob in prop::collection::vec(any::<u8>(), 0..80)) {
        let _ = VersionVector::from_payload(&blob);
        let _ = TagSet::from_payload(&blob);
        if let Ok(kv) = KeyValueVector::parse(&blob) {
            prop_assert_eq!(kv.payload(), &blob[..]);
            prop_assert_eq!(kv.iter().count(), kv.len());
        }
        let _ = through_message(&blob, |m| read_at::<IdempotencyIdRef>(m, 0, false).map(drop));
    }

    #[test]
    fn kv_vector_round_trips(
        items in prop::collection::vec(
            (prop::collection::vec(any::<u8>(), 0..12), prop::collection::vec(any::<u8>(), 0..12)),
            0..10,
        )
    ) {
        let model = model_kv(&items);
        let buf: KeyValueVectorBuf =
            items.iter().map(|(k, v)| KeyValueRef::new(k, v)).collect();
        prop_assert_eq!(buf.payload(), &model[..]);

        let msg = write_at(&buf, 3);
        prop_assert_eq!(raw_blob(&msg, 3, false, false), Some(model.clone()));
        let view: KeyValueVector = read_at(&msg, 3, false).unwrap();
        let decoded: Vec<(Vec<u8>, Vec<u8>)> =
            view.iter().map(|kv| (kv.key.to_vec(), kv.value.to_vec())).collect();
        prop_assert_eq!(&decoded, &items);
        prop_assert_eq!(view, buf.as_view());
        let owned: KeyValueVectorBuf = read_at(&msg, 3, false).unwrap();
        prop_assert_eq!(owned, buf);
        // And as a table element, the other way KeyValueRef travels.
        if let Some((k, v)) = items.first() {
            let table = write_at(&KeyValueRef::new(k, v), 0);
            let kv: KeyValueRef = read_at(&table, 0, false).unwrap();
            prop_assert_eq!(kv, KeyValueRef::new(k, v));
        }
    }

    #[test]
    fn tag_set_round_trips(
        tags in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..20), 0..6),
        present in any::<bool>(),
    ) {
        let set: TagSet = tags.iter().map(Vec::as_slice).collect();
        let payload = set.to_payload().unwrap();
        let model: Vec<u8> = tags.iter().flat_map(|t| std::iter::once(t.len() as u8).chain(t.iter().copied())).collect();
        prop_assert_eq!(&payload, &model);
        prop_assert_eq!(TagSet::from_payload(&payload).unwrap(), set.clone());

        let optional = present.then(|| set.clone());
        let msg = write_at(&optional, 2);
        prop_assert_eq!(read_at::<Option<TagSet>>(&msg, 2, false).unwrap(), optional);
        let msg = write_at(&set, 5);
        prop_assert_eq!(read_at::<TagSet>(&msg, 5, false).unwrap(), set);
    }

    #[test]
    fn idempotency_id_round_trips(bytes in prop_oneof![Just(vec![]), prop::collection::vec(any::<u8>(), 16..=255)]) {
        let id = IdempotencyIdRef::new(&bytes).unwrap();
        let msg = write_at(&id, 4);
        prop_assert_eq!(read_at::<IdempotencyIdRef>(&msg, 4, false).unwrap(), id);
        let owned: IdempotencyId = read_at(&msg, 4, false).unwrap();
        prop_assert_eq!(owned.as_bytes(), &bytes[..]);
        prop_assert_eq!(raw_blob(&msg, 4, false, false), Some(bytes.clone()));
    }

    #[test]
    fn tags_round_trip_in_vectors(tags in prop::collection::vec(tag_strategy(), 0..10)) {
        let msg = write_at(&tags.iter().map(|t| Pair(Uid::default(), *t)).collect::<Vec<_>>(), 1);
        let back: Vec<Pair<Uid, Tag>> = read_at(&msg, 1, false).unwrap();
        prop_assert_eq!(back.iter().map(|p| p.1).collect::<Vec<_>>(), tags);
    }
}
