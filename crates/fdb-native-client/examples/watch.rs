//! Watches: wait for a key to change.
//!
//! ```text
//! cargo run -p fdb-native-client --example watch -- [CLUSTER_FILE]
//! ```
//!
//! A watch is made in a transaction and starts when the transaction commits; it then
//! resolves once the key's value differs from the one the transaction saw. The example
//! watches `examples/watch/config`, changes it from another task, and waits.

use std::path::PathBuf;
use std::time::Duration;

use fdb_native_client::{Database, Error};

const KEY: &[u8] = b"examples/watch/config";

pub(crate) async fn run(db: &Database) -> Result<(), Error> {
    db.run(|tx, _| async move {
        tx.set(KEY, b"v1");
        Ok(())
    })
    .await?;

    // Read the value and watch the key in one transaction: the watch fires on any change
    // after this read. `run` commits the transaction, which starts the watch.
    let (value, watch) = db
        .run(|tx, _| async move {
            let value = tx.get(KEY).await?;
            let watch = tx.watch(KEY).await?;
            Ok((value, watch))
        })
        .await?;
    println!("config = {:?}", value.map(String::from_utf8));

    // Another client changes the key a little later.
    let writer = {
        let db = db.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            db.run(|tx, _| async move {
                tx.set(KEY, b"v2");
                Ok(())
            })
            .await
        })
    };

    // A `Watch` is a future: it resolves with `Ok(())` once the value changed (dropping it
    // cancels the watch). Bound the wait as you would any future.
    match tokio::time::timeout(Duration::from_secs(30), watch).await {
        Ok(result) => result?,
        Err(_) => panic!("the watch did not fire"),
    }
    writer.await.expect("the writer task")?;

    // A watch says that the value changed, not what it is: read it again.
    let value = db.run(|tx, _| async move { tx.get(KEY).await }).await?;
    println!("config changed to {:?}", value.map(String::from_utf8));

    db.run(|tx, _| async move {
        tx.clear(KEY);
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cluster_file = std::env::args().nth(1).map(PathBuf::from);
    let db = Database::open(cluster_file.as_deref())?;
    run(&db).await?;
    db.shutdown().await;
    Ok(())
}
