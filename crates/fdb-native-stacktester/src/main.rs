//! The stack machine of FoundationDB's bindingtester, driving the native client.
//!
//! ```text
//! fdb-native-stacktester <prefix> <api_version> [cluster_file]
//! ```
//!
//! It reads the instructions stored under `fdb.tuple.range((prefix,))` and runs them as
//! `bindings/bindingtester/spec/bindingApiTester.md` describes, behaving as the Python
//! tester (`bindings/python/tests/tester.py`) does, against the cluster of `cluster_file`
//! (or the default cluster file). It exits with 0 once every stack machine has finished, and
//! with 1 if one failed (an instruction it cannot run, a failed `UNIT_TESTS`, ...).
//!
//! The API version must be at least 520: the tuple layer only has the 4-byte versionstamp
//! offsets of API version 520 and later. It is otherwise unused: the client has one
//! behaviour, that of the latest API version.
//!
//! Not supported: the directory layer (`DIRECTORY_*`) and tenants (run the bindingtester
//! with tenants off, as it does by default for a tester it does not know). The storage
//! metrics instructions (`GET_ESTIMATED_RANGE_SIZE`, `GET_RANGE_SPLIT_POINTS`) are stubs
//! until the client has the calls (see `storage_metrics`). `UNIT_TESTS` runs the parts of
//! the Python binding's unit tests that the client can do.

mod machine;
mod stack;
mod storage_metrics;
mod unit_tests;
mod values;

use std::fmt;
use std::path::PathBuf;
use std::process::ExitCode;

use fdb_native_client::Database;

use crate::machine::{Shared, StackMachine};

/// The lowest API version the tester accepts (see the crate documentation).
const MIN_API_VERSION: i32 = 520;
/// The highest: release-8.0's.
const MAX_API_VERSION: i32 = 800;

const USAGE: &str = "usage: fdb-native-stacktester <prefix> <api_version> [cluster_file]";

/// A failure the tester does not survive (what the Python tester does not catch).
#[derive(Debug)]
pub(crate) struct Fatal(String);

impl Fatal {
    pub(crate) fn new(message: impl Into<String>) -> Fatal {
        Fatal(message.into())
    }

    /// The failure, with where it happened in front.
    pub(crate) fn context(self, context: impl fmt::Display) -> Fatal {
        Fatal(format!("{context}: {}", self.0))
    }
}

impl fmt::Display for Fatal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The command line.
#[derive(Debug, PartialEq, Eq)]
struct Args {
    prefix: Vec<u8>,
    cluster_file: Option<PathBuf>,
}

fn parse_args(args: &[String]) -> Result<Args, String> {
    let (prefix, api_version, cluster_file) = match args {
        [prefix, version] => (prefix, version, None),
        [prefix, version, cluster_file] => (prefix, version, Some(PathBuf::from(cluster_file))),
        _ => return Err(format!("expected 2 or 3 arguments, got {}", args.len())),
    };
    let api_version: i32 = api_version
        .parse()
        .map_err(|_| format!("invalid API version {api_version:?}"))?;
    if api_version < MIN_API_VERSION {
        return Err(format!(
            "API version {api_version} is not supported: the minimum is {MIN_API_VERSION} \
             (the tuple layer only has the 4-byte versionstamp offsets of API version \
             {MIN_API_VERSION} and later)"
        ));
    }
    if api_version > MAX_API_VERSION {
        return Err(format!(
            "API version {api_version} is not supported: the maximum is {MAX_API_VERSION}"
        ));
    }
    Ok(Args {
        prefix: prefix.as_bytes().to_vec(),
        cluster_file,
    })
}

async fn run(args: Args) -> Result<(), Fatal> {
    let db = Database::open(args.cluster_file.as_deref())
        .map_err(|e| Fatal::new(format!("opening the database: {e}")))?;
    let result = match StackMachine::new(Shared::new(db.clone()), args.prefix) {
        Ok(machine) => machine.run().await,
        Err(e) => Err(e),
    };
    db.shutdown().await;
    result
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse_args(&args) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("fdb-native-stacktester: {e}");
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("fdb-native-stacktester: starting the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("fdb-native-stacktester: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_the_command_line() {
        assert_eq!(
            parse_args(&args(&["pfx", "740"])).unwrap(),
            Args {
                prefix: b"pfx".to_vec(),
                cluster_file: None
            }
        );
        assert_eq!(
            parse_args(&args(&["pfx", "800", "/tmp/fdb.cluster"]))
                .unwrap()
                .cluster_file,
            Some(PathBuf::from("/tmp/fdb.cluster"))
        );
        assert!(parse_args(&args(&["pfx"])).is_err());
        assert!(parse_args(&args(&["pfx", "x"])).is_err());
        assert!(parse_args(&args(&["pfx", "810"])).is_err());
    }

    #[test]
    fn rejects_api_versions_before_520() {
        let e = parse_args(&args(&["pfx", "510"])).unwrap_err();
        assert!(e.contains("minimum is 520"), "{e}");
        assert!(parse_args(&args(&["pfx", "520"])).is_ok());
    }
}
