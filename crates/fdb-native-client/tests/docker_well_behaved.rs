//! How the cluster sees this client (`docs/well-behaved-client.md`), against a real,
//! disposable FDB server started by `fdb-native-testkit` (image from `FDB_IMAGE`, default
//! 7.4.10): `status json`'s `cluster.clients` section lists the client, with its versions,
//! protocol versions and log group, while its coordinator long poll is outstanding. Run with
//! `cargo nextest run -p fdb-native-client --run-ignored only -E 'binary(docker_well_behaved)'`.

#![cfg(feature = "tokio")]

use std::time::{Duration, Instant};

use fdb_native_client::runtime::TokioRuntime;
use fdb_native_client::{Database, DatabaseOptions};
use fdb_native_schema::SchemaVersion;
use fdb_native_testkit::FdbContainer;
use fdb_native_testkit::serde_json::Value;

/// The `supported_versions` entries of `status json` whose `client_version` is this client's.
fn our_versions(status: &Value) -> Vec<Value> {
    status["cluster"]["clients"]["supported_versions"]
        .as_array()
        .map(|versions| {
            versions
                .iter()
                .filter(|v| {
                    v["client_version"]
                        .as_str()
                        .is_some_and(|s| s.contains("fdb-native-client"))
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_status_json_lists_the_client() {
    let fdb = tokio::task::spawn_blocking(FdbContainer::start)
        .await
        .unwrap()
        .expect("container");
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());

    const LOG_GROUP: &str = "fdb-native-audit";
    let mut options = DatabaseOptions::default();
    options.monitor.trace_log_group = LOG_GROUP.as_bytes().to_vec();
    let db = Database::open_with(TokioRuntime::new(), Some(fdb.cluster_file()), options)
        .expect("database");
    db.run(|tx, _| async move {
        tx.set(b"well-behaved", b"yes");
        Ok(())
    })
    .await
    .expect("a transaction");

    // The coordinator records the client while its long poll is outstanding and hands the
    // records to the cluster controller about once a second (`MAX_CLIENT_STATUS_AGE`).
    let fdb = std::sync::Arc::new(fdb);
    let deadline = Instant::now() + Duration::from_secs(60);
    let (status, ours) = loop {
        let f = fdb.clone();
        let status = tokio::task::spawn_blocking(move || f.status_json())
            .await
            .unwrap();
        if let Ok(status) = status {
            let ours = our_versions(&status);
            if ours.len() == SchemaVersion::ALL.len() {
                break (status, ours);
            }
        }
        assert!(
            Instant::now() < deadline,
            "the client is not in status json's clients within 60 s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let clients = &status["cluster"]["clients"];
    println!(
        "clients: {}",
        fdb_native_testkit::serde_json::to_string_pretty(clients).unwrap()
    );
    assert!(clients["count"].as_u64().unwrap_or(0) >= 1, "{clients}");

    let cluster_protocol = format!(
        "{:x}",
        SchemaVersion::for_protocol_version(db.peers().cluster_version().expect("connected"))
            .expect("a supported version")
            .protocol_version()
            .0
    );
    let newest = SchemaVersion::ALL
        .iter()
        .map(|s| format!("{:x}", s.protocol_version().0))
        .max()
        .unwrap();
    for schema in SchemaVersion::ALL {
        let protocol = format!("{:x}", schema.protocol_version().0);
        let entry = ours
            .iter()
            .find(|v| v["protocol_version"] == protocol.as_str())
            .unwrap_or_else(|| panic!("no entry for protocol {protocol}: {ours:?}"));
        assert_eq!(entry["source_version"], "fdb-native-client", "{entry}");
        assert!(
            entry["client_version"]
                .as_str()
                .unwrap()
                .starts_with(&format!("{schema}.0 (fdb-native-client ")),
            "{entry}"
        );
        let connected = entry["connected_clients"].as_array().expect("clients");
        assert!(
            connected.iter().any(|c| c["log_group"] == LOG_GROUP),
            "{entry}"
        );
        // The cluster's own protocol is among ours, and the newest we speak is what an
        // upgrade check (`max_protocol_clients`) sees.
        if protocol == newest {
            let max = entry["max_protocol_clients"].as_array().expect("max");
            assert!(max.iter().any(|c| c["log_group"] == LOG_GROUP), "{entry}");
        }
    }
    assert!(
        ours.iter()
            .any(|v| v["protocol_version"] == cluster_protocol.as_str()),
        "{ours:?}"
    );

    db.shutdown().await;
    tokio::task::spawn_blocking(move || drop(fdb))
        .await
        .unwrap();
}
