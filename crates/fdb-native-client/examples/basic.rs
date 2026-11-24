//! Basic reads and writes in `Database::run`, the retry loop.
//!
//! ```text
//! cargo run -p fdb-native-client --example basic -- [CLUSTER_FILE]
//! ```
//!
//! The cluster file is the first argument, else the default one (`FDB_CLUSTER_FILE`,
//! `./fdb.cluster`, `/etc/foundationdb/fdb.cluster`). The example writes under
//! `examples/basic/` and clears it again.

use std::path::PathBuf;

use fdb_native_client::{Database, Error};

/// The example's keys.
const PREFIX: &[u8] = b"examples/basic/";

fn key(name: &str) -> Vec<u8> {
    [PREFIX, name.as_bytes()].concat()
}

pub(crate) async fn run(db: &Database) -> Result<(), Error> {
    // Writes: `set`, `clear` and `clear_range` return nothing; the commit `run` makes at the
    // end of the closure sends them. The closure may run more than once (a conflict, a
    // `commit_unknown_result`, ...), so it only writes fixed values: running it twice is
    // harmless.
    db.run(|tx, _maybe_committed| async move {
        tx.set(&key("hello"), b"world");
        tx.set(&key("answer"), b"42");
        tx.clear(&key("stale"));
        Ok(())
    })
    .await?;

    // A read. `run` returns what the closure returns.
    let hello = db
        .run(|tx, _| async move { tx.get(&key("hello")).await })
        .await?;
    assert_eq!(hello.as_deref(), Some(&b"world"[..]));
    println!(
        "hello = {}",
        String::from_utf8_lossy(&hello.unwrap_or_default())
    );

    // Read-modify-write. The read adds a read conflict range: if another transaction changes
    // `answer` before this one commits, the commit fails with `not_committed` (1020) and
    // `run` runs the closure again, on a new read version.
    let doubled = db
        .run(|tx, _| async move {
            let answer = tx.get(&key("answer")).await?.unwrap_or_default();
            let n: u64 = String::from_utf8_lossy(&answer).parse().unwrap_or(0);
            // Reads see the transaction's own writes.
            tx.set(&key("answer"), (n * 2).to_string().as_bytes());
            let mine = tx.get(&key("answer")).await?;
            Ok(mine)
        })
        .await?;
    println!(
        "answer = {}",
        String::from_utf8_lossy(&doubled.unwrap_or_default())
    );

    // Several reads at once: each read starts when it is called, so these run concurrently.
    let (hello, answer) = db
        .run(|tx, _| async move {
            let hello = tx.get(&key("hello"));
            let answer = tx.get(&key("answer"));
            Ok((hello.await?, answer.await?))
        })
        .await?;
    assert!(hello.is_some() && answer.is_some());

    // Clean up: everything under the prefix.
    db.run(|tx, _| async move {
        tx.clear_range(PREFIX, b"examples/basic0");
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
