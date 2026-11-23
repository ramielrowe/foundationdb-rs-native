//! The derive's compile errors: each `tests/ui/*.rs` must fail with its `.stderr`.
//! Regenerate the expected output with `TRYBUILD=overwrite cargo test -p
//! fdb-native-wire-derive --test ui` and review the diff.

/// Errors the derive itself reports (`compile_error!` with spans): stable across
/// compilers back to the workspace's rust-version.
#[test]
fn derive_errors() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/absent.rs");
    t.compile_fail("tests/ui/field_attributes.rs");
    t.compile_fail("tests/ui/file_identifier.rs");
    t.compile_fail("tests/ui/unions.rs");
    t.compile_fail("tests/ui/unknown_attributes.rs");
}

/// Shape checks that fail during constant evaluation, reported at the offending member.
/// rustc words const-evaluation panics differently across versions (1.85 says
/// "evaluation of constant value failed"), so this snapshot is for current stable, which
/// is what CI tests with.
#[test]
fn const_evaluation_errors() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/const_checks.rs");
    // A trait-bound error from rustc, worded by the compiler (and with compiler-written
    // help), so also a current-stable snapshot.
    t.compile_fail("tests/ui/absent_without_default.rs");
}
