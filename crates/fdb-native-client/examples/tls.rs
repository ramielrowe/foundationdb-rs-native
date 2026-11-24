//! Connecting to a TLS cluster (needs the `tls` feature).
//!
//! ```text
//! cargo run -p fdb-native-client --features tls --example tls -- CLUSTER_FILE [CA CERT KEY]
//! ```
//!
//! A cluster whose coordinators end in `:tls` in the cluster file is reached over TLS. The
//! certificates are configured as FoundationDB configures them: explicitly (here the CA,
//! certificate and key files given after the cluster file), else from the `FDB_TLS_*`
//! environment variables (`FDB_TLS_CA_FILE`, `FDB_TLS_CERTIFICATE_FILE`,
//! `FDB_TLS_KEY_FILE`, `FDB_TLS_PASSWORD`, `FDB_TLS_VERIFY_PEERS`), else `cert.pem` and
//! `key.pem` in the default configuration directory.

use std::path::{Path, PathBuf};

use fdb_native_client::runtime::TokioRuntime;
use fdb_native_client::{Database, DatabaseOptions, Error, TlsConfig};

const KEY: &[u8] = b"examples/tls/key";

/// The options of a database whose certificates are these files.
pub(crate) fn options(ca: &Path, cert: &Path, key: &Path) -> DatabaseOptions {
    let mut options = DatabaseOptions::default();
    options.tls = Some(
        TlsConfig::new()
            .ca_path(ca)
            .cert_path(cert)
            .key_path(key)
            // The peer verification rules of FoundationDB's `TLS_VERIFY_PEERS`; this is the
            // default (a certificate chain valid for the CA).
            .verify_peers("Check.Valid=1"),
    );
    options
}

pub(crate) async fn run(db: &Database) -> Result<(), Error> {
    db.run(|tx, _| async move {
        tx.set(KEY, b"over TLS");
        Ok(())
    })
    .await?;
    let value = db.run(|tx, _| async move { tx.get(KEY).await }).await?;
    assert_eq!(value.as_deref(), Some(&b"over TLS"[..]));
    println!("{}", String::from_utf8_lossy(&value.unwrap_or_default()));
    db.run(|tx, _| async move {
        tx.clear(KEY);
        Ok(())
    })
    .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<PathBuf> = std::env::args().skip(1).map(PathBuf::from).collect();
    let db = match args.as_slice() {
        // The certificates from the environment (`FDB_TLS_*`).
        [cluster_file] => Database::open(Some(cluster_file))?,
        [cluster_file, ca, cert, key] => Database::open_with(
            TokioRuntime::new(),
            Some(cluster_file),
            options(ca, cert, key),
        )?,
        _ => return Err("usage: tls CLUSTER_FILE [CA CERT KEY]".into()),
    };
    run(&db).await?;
    db.shutdown().await;
    Ok(())
}
