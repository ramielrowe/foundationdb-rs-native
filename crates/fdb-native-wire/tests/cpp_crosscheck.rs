//! Encoder verification against FDB's C++ readers (`golden-vectors decode`).
//!
//! Ignored by default: it needs the dev-only C++ tool. Build it, then point
//! `FDB_GOLDEN_VECTORS_BIN` at one or more decoders (a `:`-separated list):
//!
//! ```text
//! make -C tools/golden-vectors FDB_SRC=/path/to/foundationdb
//! FDB_GOLDEN_VECTORS_BIN=$PWD/tools/golden-vectors/build/golden-vectors \
//!     cargo nextest run -p fdb-native-wire --run-ignored only -E 'binary(cpp_crosscheck)'
//! ```
//!
//! `tools/golden-vectors/regen.sh --check` also builds the release-7.4 and release-8.0
//! readers (`build/rel-release-7.4/golden-vectors`, `build/rel-release-8.0/golden-vectors`).
//! Those have no bounds checks; every buffer sent to them here is first accepted by this
//! crate's decoder, which is at least as strict as FDB main's.

mod map_support;
mod support;

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use fdb_native_wire::{Reader, decode, encode};
use map_support::{HashContainers, MapContainers};
use proptest::prelude::prop;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, TestRunner};
use serde_json::Value;
use support::*;

fn decoders() -> Vec<PathBuf> {
    let paths = std::env::var_os("FDB_GOLDEN_VECTORS_BIN").expect(
        "set FDB_GOLDEN_VECTORS_BIN to the golden-vectors decoder(s) (see this file's docs)",
    );
    let paths: Vec<PathBuf> = std::env::split_paths(&paths).collect();
    for p in &paths {
        assert!(p.is_file(), "{} does not exist", p.display());
    }
    paths
}

/// Decodes `bytes` as `cpp_type` with the C++ decoder and returns its root dump.
fn cpp_decode(bin: &PathBuf, cpp_type: &str, bytes: &[u8]) -> Result<Value, String> {
    let mut child = Command::new(bin)
        .args(["decode", cpp_type])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawning {}: {e}", bin.display()))?;
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(hex_encode(bytes).as_bytes())
        .map_err(|e| e.to_string())?;
    let out = child.wait_with_output().map_err(|e| e.to_string())?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() {
        return Err(format!(
            "{}: {stdout}{}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    let v: Value = serde_json::from_str(&stdout).map_err(|e| format!("bad JSON {e}: {stdout}"))?;
    Ok(v["root"].clone())
}

#[test]
#[ignore = "requires golden-vectors tool"]
fn reencoded_positives_decode_identically_in_cpp() {
    let registry = registry_map();
    let positives = records("positive");
    for bin in decoders() {
        for record in &positives {
            let name = record["name"].as_str().expect("name");
            let cpp_type = record["cpp_type"].as_str().expect("cpp_type");
            let entry = &registry[cpp_type];
            let bytes =
                (entry.reencode)(&record_bytes(record)).unwrap_or_else(|e| panic!("{name}: {e}"));
            // Never send a release reader anything this crate's strict decoder rejects.
            (entry.decode)(&bytes, true)
                .unwrap_or_else(|e| panic!("{name}: own output rejected: {e}"));
            let root = cpp_decode(&bin, cpp_type, &bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_json_eq(
                &format!("{name} via {}", bin.display()),
                &record["root"],
                &root,
            );
        }
        println!(
            "{}: {} re-encoded positives decode identically",
            bin.display(),
            positives.len()
        );
    }
}

#[test]
#[ignore = "requires golden-vectors tool"]
fn generated_values_decode_identically_in_cpp() {
    const CASES: usize = 24;
    let registry = registry();
    for bin in decoders() {
        let mut runner = TestRunner::new(Config {
            failure_persistence: None,
            ..Config::default()
        });
        for entry in &registry {
            let strategy = (entry.arb)();
            for _ in 0..CASES {
                let generated = strategy.new_tree(&mut runner).expect("value").current();
                (entry.decode)(&generated.bytes, true)
                    .unwrap_or_else(|e| panic!("{}: own output rejected: {e}", entry.name));
                let root = cpp_decode(&bin, entry.name, &generated.bytes)
                    .unwrap_or_else(|e| panic!("{}: {e}\nvalue: {}", entry.name, generated.root));
                assert_json_eq(
                    &format!("{} via {}", entry.name, bin.display()),
                    &generated.root,
                    &root,
                );
            }
        }
        println!(
            "{}: {} generated values decode identically",
            bin.display(),
            CASES * registry.len()
        );
    }
}

/// The divergence `alias_data.rs` documents: C++ accepts a message that references one
/// string 20 times (a `data` accept), which this crate rejects with `work_limit`. Every
/// offset in it is in bounds, so it is safe for the unchecked release readers too.
#[test]
#[ignore = "requires golden-vectors tool"]
fn aliased_strings_are_accepted_by_cpp() {
    let (value, bytes) = aliased_untrusted();
    for bin in decoders() {
        let root = cpp_decode(&bin, "Untrusted", &bytes).unwrap_or_else(|e| panic!("{e}"));
        assert_json_eq(
            &format!("aliased Untrusted via {}", bin.display()),
            &root1(&value),
            &root,
        );
    }
}

/// Sends `bytes` to every C++ decoder as `Containers` and compares its dump with `want`.
fn assert_cpp_dumps(label: &str, bytes: &[u8], want: &Value) {
    for bin in decoders() {
        let got = cpp_decode(&bin, "Containers", bytes).unwrap_or_else(|e| panic!("{label}: {e}"));
        assert_json_eq(&format!("{label} via {}", bin.display()), want, &got);
    }
}

fn generate<T: Arb>(runner: &mut TestRunner) -> T {
    T::arb().new_tree(runner).expect("value").current()
}

/// Values written from real `std::map`-like and `std::set`-like Rust containers (sorted
/// and hash-ordered) decode in C++ to the same `std::map` and `std::set`.
#[test]
#[ignore = "requires golden-vectors tool"]
fn map_and_set_encoded_values_decode_identically_in_cpp() {
    const CASES: usize = 64;
    let mut runner = TestRunner::new(Config {
        failure_persistence: None,
        ..Config::default()
    });
    for _ in 0..CASES {
        let m: MapContainers = generate(&mut runner);
        let bytes = encode(&m).expect("encode");
        decode::<MapContainers>(&bytes).expect("own output rejected");
        assert_cpp_dumps("BTreeMap/BTreeSet", &bytes, &root1(&m));

        // C++ reads hash order into its sorted containers.
        let h: HashContainers = generate(&mut runner);
        let bytes = encode(&h).expect("encode");
        decode::<HashContainers>(&bytes).expect("own output rejected");
        assert_cpp_dumps("HashMap/HashSet", &bytes, &root1(&h));
    }
    println!("{CASES} + {CASES} map/set-encoded values decode identically");
}

/// The engine's own `containers` bytes, read into real maps and sets and written again.
#[test]
#[ignore = "requires golden-vectors tool"]
fn containers_fixture_rewritten_from_real_maps_decodes_in_cpp() {
    let record = records("positive")
        .into_iter()
        .find(|r| r["name"] == "containers")
        .expect("containers fixture");
    let m: MapContainers = decode(&record_bytes(&record)).expect("decode");
    assert_cpp_dumps("containers", &encode(&m).expect("encode"), &record["root"]);
    let h: HashContainers = decode(&record_bytes(&record)).expect("decode");
    assert_cpp_dumps(
        "containers (hash)",
        &encode(&h).expect("encode"),
        &record["root"],
    );
}

/// Unsorted entries and duplicate keys on the wire (which FDB's writers never produce):
/// this crate's `BTreeMap`/`HashMap`/`BTreeSet`/`HashSet` end up with exactly what the C++
/// `std::map` and `std::set` end up with, so "the first entry for a key wins" holds there
/// too.
#[test]
#[ignore = "requires golden-vectors tool"]
fn duplicate_and_unsorted_entries_decode_like_cpp() {
    let mut cases = vec![
        Containers {
            m: vec![
                Pair {
                    first: 3,
                    second: Bytes(b"first three".to_vec()),
                },
                Pair {
                    first: 1,
                    second: Bytes(b"one".to_vec()),
                },
                Pair {
                    first: 3,
                    second: Bytes(b"second three".to_vec()),
                },
                Pair {
                    first: 1,
                    second: Bytes(b"another one".to_vec()),
                },
                Pair {
                    first: -7,
                    second: Bytes(vec![]),
                },
            ],
            s: vec![30, 10, 30, 20, 10, 0, u32::MAX, 0],
            nested: vec![],
        },
        Containers {
            m: vec![],
            s: vec![],
            nested: vec![vec![1]],
        },
    ];
    // And generated ones: few distinct keys, so most have duplicates.
    let mut runner = TestRunner::new(Config {
        failure_persistence: None,
        ..Config::default()
    });
    let entry = (-3i32..3, Bytes::arb()).prop_map(|(first, second)| Pair { first, second });
    let strategy = (
        prop::collection::vec(entry, 0..12),
        prop::collection::vec(0u32..6, 0..12),
    );
    for _ in 0..64 {
        let (m, s) = strategy.new_tree(&mut runner).expect("value").current();
        cases.push(Containers {
            m,
            s,
            nested: vec![],
        });
    }
    for (i, wire) in cases.iter().enumerate() {
        let bytes = encode(wire).expect("encode");
        let r = Reader::new(&bytes).expect("reader");
        let m: MapContainers = r.decode_root().expect("decode as BTreeMap");
        let h: HashContainers = r.decode_root().expect("decode as HashMap");
        assert_eq!(h.sorted(), m, "case {i}: hash and tree containers differ");
        assert_cpp_dumps(&format!("duplicates case {i}"), &bytes, &root1(&m));
    }
}
