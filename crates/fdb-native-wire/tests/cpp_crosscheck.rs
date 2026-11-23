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

mod support;

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

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
/// string 20 times (a `structural` accept, violation `work_limit`), which this crate rejects. Every
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
