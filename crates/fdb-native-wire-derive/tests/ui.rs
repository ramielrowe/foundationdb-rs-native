//! The derive's compile errors: each `tests/ui/*.rs` must fail with its `.stderr`.
//! Regenerate the expected output with `TRYBUILD=overwrite cargo test -p
//! fdb-native-wire-derive --test ui` and review the diff.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
