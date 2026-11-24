//! The command line, without a cluster: bad arguments are refused before the database is
//! opened.

use std::process::Command;

fn tester() -> Command {
    Command::new(env!("CARGO_BIN_EXE_fdb-native-stacktester"))
}

#[test]
fn api_versions_before_520_are_refused() {
    let out = tester()
        .args(["prefix", "510", "/nonexistent/fdb.cluster"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("API version 510 is not supported"),
        "{stderr}"
    );
    assert!(stderr.contains("minimum is 520"), "{stderr}");
}

#[test]
fn missing_arguments_print_the_usage() {
    let out = tester().arg("prefix").output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("usage: fdb-native-stacktester"), "{stderr}");
}

#[test]
fn a_missing_cluster_file_is_a_failure() {
    let out = tester()
        .args(["prefix", "740", "/nonexistent/fdb.cluster"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("opening the database"), "{stderr}");
}
