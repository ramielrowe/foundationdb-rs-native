//! The client on smol: no tokio, `SmolRuntime` (async-io sockets and timers, async-executor
//! tasks) and `futures_lite::future::block_on` to drive the main future.
//!
//! ```text
//! cargo run -p fdb-native-client --no-default-features --features smol --example smol -- [CLUSTER_FILE]
//! ```
//!
//! The cluster file is the first argument, else the default one (`FDB_CLUSTER_FILE`,
//! `./fdb.cluster`, `/etc/foundationdb/fdb.cluster`). The example writes under
//! `examples/smol/` and clears it again.
//!
//! `SmolRuntime::new()` runs the client's tasks on a process-wide executor with threads of
//! its own, so any way of waiting for the future works (`block_on` here; `smol::block_on`
//! would do). To drive the tasks yourself, use `SmolRuntime::with_executor`.

use std::path::PathBuf;

use fdb_native_client::runtime::SmolRuntime;
use fdb_native_client::{Database, DatabaseOptions, Error};

pub(crate) async fn run(db: &Database<SmolRuntime>) -> Result<(), Error> {
    db.run(|tx, _| async move {
        tx.set(b"examples/smol/hello", b"world");
        Ok(())
    })
    .await?;
    let hello = db
        .run(|tx, _| async move { tx.get(b"examples/smol/hello").await })
        .await?;
    println!(
        "hello = {}",
        String::from_utf8_lossy(&hello.unwrap_or_default())
    );
    db.run(|tx, _| async move {
        tx.clear_range(b"examples/smol/", b"examples/smol0");
        Ok(())
    })
    .await
}

fn main() -> Result<(), Error> {
    let cluster_file = std::env::args().nth(1).map(PathBuf::from);
    futures_lite::future::block_on(async {
        let db = Database::open_with(
            SmolRuntime::new(),
            cluster_file.as_deref(),
            DatabaseOptions::default(),
        )?;
        let result = run(&db).await;
        db.shutdown().await;
        result
    })
}
