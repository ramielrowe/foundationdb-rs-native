//! Property tests: round trips over generated values of every engine type, and decoding
//! arbitrary or mutated bytes never panics (and is self-consistent when it succeeds).

mod support;

use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use support::*;

fn runner(cases: u32) -> TestRunner {
    TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    })
}

#[test]
fn generated_values_round_trip() {
    for entry in registry() {
        runner(128)
            .run(&(entry.arb)(), |generated| {
                let root = (entry.decode)(&generated.bytes, true)
                    .map_err(|e| TestCaseError::fail(format!("{}: rejected: {e}", entry.name)))?;
                if let Some(d) = json_diff(&generated.root, &root) {
                    return Err(TestCaseError::fail(format!("{}: {d}", entry.name)));
                }
                Ok(())
            })
            .unwrap_or_else(|e| panic!("{}: {e}", entry.name));
    }
}

/// If a buffer decodes, re-encoding the value and decoding that gives the same value.
fn check_consistent(entry: &TypeEntry, bytes: &[u8]) -> Result<(), TestCaseError> {
    let Ok(root) = (entry.decode)(bytes, false) else {
        return Ok(());
    };
    let again = (entry.reencode)(bytes).map_err(|e| {
        TestCaseError::fail(format!(
            "{}: decoded but re-encoding failed: {e}",
            entry.name
        ))
    })?;
    let root2 = (entry.decode)(&again, true)
        .map_err(|e| TestCaseError::fail(format!("{}: re-encoding rejected: {e}", entry.name)))?;
    match json_diff(&root, &root2) {
        Some(d) => Err(TestCaseError::fail(format!("{}: {d}", entry.name))),
        None => Ok(()),
    }
}

#[test]
fn arbitrary_bytes_never_panic() {
    let registry = registry();
    let strategy = (
        0..registry.len(),
        prop::collection::vec(any::<u8>(), 0..160),
    );
    runner(20_000)
        .run(&strategy, |(i, mut bytes)| {
            // Point the root offset somewhere plausible half the time.
            if bytes.len() >= 8 && bytes[0] & 1 == 0 {
                let root = (u32::from(bytes[1]) % bytes.len() as u32) & !3;
                bytes[..4].copy_from_slice(&root.to_le_bytes());
            }
            check_consistent(&registry[i], &bytes)
        })
        .unwrap();
}

#[derive(Clone, Debug)]
enum Mutation {
    Byte(usize, u8),
    Word(usize, u32),
    Truncate(usize),
}

fn mutation() -> impl Strategy<Value = Mutation> {
    let hostile = prop::sample::select(vec![
        0u32,
        1,
        2,
        3,
        4,
        8,
        0xffff,
        0x7fff_fff0,
        0x8000_0000,
        0xffff_fff0,
        u32::MAX,
    ]);
    prop_oneof![
        (any::<usize>(), any::<u8>()).prop_map(|(p, v)| Mutation::Byte(p, v)),
        (any::<usize>(), hostile).prop_map(|(p, v)| Mutation::Word(p, v)),
        any::<usize>().prop_map(Mutation::Truncate),
    ]
}

#[test]
fn mutated_fixtures_never_panic() {
    let registry = registry_map();
    let positives: Vec<(String, Vec<u8>)> = records("positive")
        .iter()
        .map(|r| {
            (
                r["cpp_type"].as_str().expect("cpp_type").to_string(),
                record_bytes(r),
            )
        })
        .collect();
    let strategy = (0..positives.len(), prop::collection::vec(mutation(), 1..4));
    runner(20_000)
        .run(&strategy, |(i, mutations)| {
            let (cpp_type, original) = &positives[i];
            let mut bytes = original.clone();
            for m in mutations {
                if bytes.is_empty() {
                    break;
                }
                match m {
                    Mutation::Byte(p, v) => {
                        let p = p % bytes.len();
                        bytes[p] = v;
                    }
                    Mutation::Word(p, v) => {
                        let p = (p % bytes.len()) & !3;
                        let n = (bytes.len() - p).min(4);
                        bytes[p..p + n].copy_from_slice(&v.to_le_bytes()[..n]);
                    }
                    Mutation::Truncate(len) => bytes.truncate(len % (bytes.len() + 1)),
                }
            }
            check_consistent(&registry[cpp_type.as_str()], &bytes)
        })
        .unwrap();
}
