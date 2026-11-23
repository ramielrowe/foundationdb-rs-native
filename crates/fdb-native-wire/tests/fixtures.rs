//! Data-driven tests against the golden vectors in tools/golden-vectors/fixtures/engine.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use fdb_native_wire::{DecodeError, Encode, EncodeError, ObjectRef, Reader, Shape, Wire, Writer};
use serde_json::Value;
use support::*;

#[test]
fn registry_matches_schemas() {
    let schemas = read_json(&fixtures_dir().join("schemas.json"));
    let types = schemas["types"].as_object().expect("types");
    let tables = schemas["tables"].as_object().expect("tables");
    let registry = registry_map();

    let schema_names: BTreeSet<&str> = types.keys().map(String::as_str).collect();
    let registry_names: BTreeSet<&str> = registry.keys().copied().collect();
    assert_eq!(
        schema_names, registry_names,
        "registry and schemas.json list different types"
    );

    let mut tables_checked = BTreeSet::new();
    for (name, entry) in &registry {
        let schema = &types[*name];
        assert_eq!(
            u64::from(entry.fid),
            schema["file_identifier"].as_u64().expect("fid"),
            "{name}"
        );
        // The root member list, from the dump of the absent value.
        let absent = (entry.absent_root)();
        assert_json_eq(name, &schema["root"], &strip_values(&absent["value"]));
        // Every table the absent value contains has the schema's member list.
        check_tables(&absent, tables, &mut tables_checked);
    }
    // The decoded positives reach the rest (union alternatives, vector elements).
    for record in records("positive") {
        check_tables(&record["root"], tables, &mut tables_checked);
    }
    let all: BTreeSet<&str> = tables.keys().map(String::as_str).collect();
    let checked: BTreeSet<&str> = tables_checked.iter().map(String::as_str).collect();
    let missing: Vec<_> = all.difference(&checked).collect();
    // Tables only reachable through an empty vector or an untaken alternative.
    assert!(
        missing
            .iter()
            .all(|t| ["table<Small>"].contains(t) || t.is_empty()),
        "{missing:?}"
    );
}

/// The `{field, slot, type}` list of a table's member nodes.
fn strip_values(members: &Value) -> Value {
    let members = members.as_array().expect("members");
    Value::Array(
        members
            .iter()
            .map(|m| {
                let mut out = serde_json::Map::new();
                for key in ["field", "slot", "type"] {
                    out.insert(key.into(), m[key].clone());
                }
                Value::Object(out)
            })
            .collect(),
    )
}

fn check_tables(
    node: &Value,
    tables: &serde_json::Map<String, Value>,
    seen: &mut BTreeSet<String>,
) {
    match node {
        Value::Object(m) => {
            if let (Some(Value::String(ty)), Some(Value::Array(_))) =
                (m.get("type"), m.get("value"))
            {
                if ty.starts_with("table<") && ty != "table<root>" {
                    let schema = tables
                        .get(ty)
                        .unwrap_or_else(|| panic!("no schema for {ty}"));
                    assert_json_eq(ty, schema, &strip_values(&m["value"]));
                    seen.insert(ty.clone());
                }
            }
            m.values().for_each(|v| check_tables(v, tables, seen));
        }
        Value::Array(a) => a.iter().for_each(|v| check_tables(v, tables, seen)),
        _ => {}
    }
}

#[test]
fn positives_decode_to_root() {
    let registry = registry_map();
    let positives = records("positive");
    assert!(positives.len() >= 51);
    for record in &positives {
        let name = record["name"].as_str().expect("name");
        let entry = &registry[record["cpp_type"].as_str().expect("cpp_type")];
        assert_eq!(entry.fid, record_fid(record), "{name}");
        let root = (entry.decode)(&record_bytes(record), true)
            .unwrap_or_else(|e| panic!("{name}: rejected: {e}"));
        assert_json_eq(name, &record["root"], &root);
    }
    println!("{} positives decode to their root", positives.len());
}

#[test]
fn positives_round_trip() {
    let registry = registry_map();
    for record in records("positive") {
        let name = record["name"].as_str().expect("name");
        let entry = &registry[record["cpp_type"].as_str().expect("cpp_type")];
        let bytes =
            (entry.reencode)(&record_bytes(&record)).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(bytes.len() % 8, 0, "{name}: size not a multiple of 8");
        assert_eq!(&bytes[4..8], &entry.fid.to_le_bytes(), "{name}");
        let root = (entry.decode)(&bytes, true)
            .unwrap_or_else(|e| panic!("{name}: re-encoding rejected: {e}"));
        assert_json_eq(name, &record["root"], &root);
        // Encoding is deterministic.
        assert_eq!((entry.reencode)(&bytes).expect("reencode"), bytes, "{name}");
    }
}

#[test]
fn negatives_are_rejected() {
    let registry = registry_map();
    let negatives = records("negative");
    assert!(negatives.len() >= 26);
    let mut reasons = BTreeMap::<String, usize>::new();
    for record in &negatives {
        let name = record["name"].as_str().expect("name");
        let entry = &registry[record["cpp_type"].as_str().expect("cpp_type")];
        assert_eq!(entry.fid, record_fid(record), "{name}");
        match (entry.decode)(&record_bytes(record), true) {
            Ok(root) => panic!("{name}: accepted as {root}"),
            Err(DecodeError::WorkLimit) => panic!("{name}: rejected only by the work limit"),
            Err(e) => *reasons.entry(error_kind(&e)).or_default() += 1,
        }
    }
    println!("{} negatives rejected: {reasons:?}", negatives.len());
}

fn error_kind(e: &DecodeError) -> String {
    match e {
        DecodeError::Strict(check) => format!("strict:{check}"),
        other => format!("{other:?}")
            .split([' ', '(', '{'])
            .next()
            .unwrap_or("")
            .to_string(),
    }
}

#[test]
fn file_identifier_check_can_be_skipped() {
    let registry = registry_map();
    let entry = &registry["Untrusted"];
    let wrong = read_json(&fixtures_dir().join("negative/wrong_file_identifier.json"));
    let bytes = record_bytes(&wrong);
    assert!(matches!(
        (entry.decode)(&bytes, true),
        Err(DecodeError::FileIdentifierMismatch {
            expected: 4242,
            found: 4243
        })
    ));
    // It is a default-constructed Untrusted (as `golden-vectors decode
    // --ignore-file-identifier` also shows).
    let root = (entry.decode)(&bytes, false).expect("decodes without the check");
    assert_json_eq("wrong_file_identifier", &(entry.absent_root)(), &root);
    assert_eq!(Reader::new(&bytes).expect("header").file_identifier(), 4243);
}

#[test]
fn corpus_obeys_policy() {
    let registry = registry_map();
    // (file, class) -> (accepted, rejected)
    let mut tally = BTreeMap::<(String, String), (usize, usize)>::new();
    let mut structural_rejections = BTreeMap::<String, usize>::new();
    let mut total = 0;
    for (file, line) in corpus() {
        total += 1;
        let name = format!("{file}:{}", line["name"].as_str().expect("name"));
        let entry = &registry[line["cpp_type"].as_str().expect("cpp_type")];
        assert_eq!(entry.fid, record_fid(&line), "{name}");
        let result = (entry.decode)(&record_bytes(&line), true);
        let expect = line["expect"].as_str().expect("expect");
        let class = if expect == "reject" {
            "reject".to_string()
        } else {
            line["accept_class"]
                .as_str()
                .expect("accept_class")
                .to_string()
        };
        let counts = tally.entry((file.clone(), class.clone())).or_default();
        match (class.as_str(), result) {
            ("reject", Ok(root)) => panic!(
                "{name}: C++ rejects ({}), accepted as {root}",
                line["reason"]
            ),
            ("reject", Err(_)) => counts.1 += 1,
            ("data" | "padding", Err(e)) => panic!("{name}: {class} accept rejected: {e}"),
            ("structural", Err(e)) => {
                let violations: Vec<&str> = line["violations"]
                    .as_array()
                    .expect("violations")
                    .iter()
                    .map(|v| v.as_str().expect("violation"))
                    .collect();
                let check = e
                    .check_name()
                    .unwrap_or_else(|| panic!("{name}: rejected by {e}, not a strict check"));
                assert!(
                    violations.contains(&check),
                    "{name}: rejected by {check}, violations {violations:?}"
                );
                *structural_rejections.entry(check.to_string()).or_default() += 1;
                counts.1 += 1;
            }
            (class, Ok(root)) => {
                // This crate implements the work limit, so an entry whose only violation is
                // `work_limit` (an aliasing bomb) must not be accepted.
                if class == "structural"
                    && line["violations"].as_array().map(Vec::as_slice)
                        == Some(&[Value::from("work_limit")])
                {
                    panic!("{name}: accepted, but exceeds the work limit");
                }
                assert_json_eq(&name, &line["root"], &root);
                counts.0 += 1;
            }
            (other, Err(e)) => panic!("{name}: unknown class {other}: {e}"),
        }
    }
    println!("{total} corpus entries; (file, class) -> (accepted, rejected):");
    for ((file, class), (accepted, rejected)) in &tally {
        println!("  {file:40} {class:10} accepted {accepted:4} rejected {rejected:4}");
    }
    println!("structural entries rejected, by check: {structural_rejections:?}");
}

// ---------------------------------------------------------------------------------------
// Targeted reader tests.

#[test]
fn aliasing_bombs_hit_the_work_limit() {
    let mut w = Writer::new();
    let blob = w.blob(&[7u8; 1000]).expect("blob");
    let outer = w.vector(&vec![Alias(blob); 1000]).expect("vector");
    let mut root = w.table();
    root.field(&Alias(outer)).expect("field");
    let root = root.finish().expect("root");
    let bytes = w.finish(root, 1).expect("finish");
    assert!(bytes.len() < 6000, "{}", bytes.len());
    let r = Reader::new(&bytes).expect("header");
    assert_eq!(r.decode_root::<Vec<Vec<u8>>>(), Err(DecodeError::WorkLimit));
    let r = Reader::new(&bytes).expect("header");
    assert_eq!(r.decode_root::<Vec<&[u8]>>(), Err(DecodeError::WorkLimit));
    // A few aliases are fine.
    let mut w = Writer::new();
    let blob = w.blob(&[7u8; 1000]).expect("blob");
    let outer = w.vector(&[Alias(blob); 3]).expect("vector");
    let mut root = w.table();
    root.field(&Alias(outer)).expect("field");
    let root = root.finish().expect("root");
    let bytes = w.finish(root, 1).expect("finish");
    let r = Reader::new(&bytes).expect("header");
    assert_eq!(r.decode_root::<Vec<&[u8]>>().expect("decodes").len(), 3);
}

/// A recursive table: `struct Chain { Optional<Chain> next; }` (via a box).
#[derive(Debug, PartialEq)]
struct Chain(Option<Box<Chain>>);

impl Wire for Chain {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for Chain {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.0)?;
        t.finish()
    }
}

impl<'de> fdb_native_wire::Decode<'de> for Chain {
    fn absent() -> Self {
        Chain(None)
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(Chain(t.field()?))
    }
}

fn chain(depth: usize) -> Chain {
    (0..depth).fold(Chain(None), |c, _| Chain(Some(Box::new(c))))
}

#[test]
fn nesting_is_limited_to_64_tables() {
    // The root table is depth 0; a chain of n tables reaches depth n.
    for (depth, ok) in [(64, true), (65, false)] {
        let bytes = fdb_native_wire::encode_with_identifier(&chain(depth - 1), 1).expect("encode");
        let r = Reader::new(&bytes).expect("header");
        let result = r.decode_root::<Chain>();
        if ok {
            assert_eq!(result.expect("decodes"), chain(depth - 1));
        } else {
            assert_eq!(
                result,
                Err(DecodeError::Strict(fdb_native_wire::StrictCheck::TooDeep))
            );
        }
    }
}

#[test]
fn scalar_bool_must_be_0_or_1() {
    let mut bytes = fdb_native_wire::encode_with_identifier(&true, 1).expect("encode");
    let r = Reader::new(&bytes).expect("header");
    assert!(r.decode_root::<bool>().expect("decodes"));
    let at = bytes.iter().rposition(|&b| b == 1).expect("the bool byte");
    bytes[at] = 2;
    let r = Reader::new(&bytes).expect("header");
    assert_eq!(
        r.decode_root::<bool>(),
        Err(DecodeError::Strict(fdb_native_wire::StrictCheck::BoolValue))
    );
}

#[test]
fn short_buffers_are_rejected() {
    for len in 0..8 {
        assert_eq!(
            Reader::new(&[0; 8][..len]).err(),
            Some(DecodeError::TooShort { len })
        );
    }
}
