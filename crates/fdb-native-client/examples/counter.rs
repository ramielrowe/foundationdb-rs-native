//! An atomic counter: `atomic_op` with `MutationType::Add`, incremented concurrently by
//! many tasks without conflicts.
//!
//! ```text
//! cargo run -p fdb-native-client --example counter -- [CLUSTER_FILE]
//! ```
//!
//! A read-modify-write increment (`get`, then `set`) makes concurrent transactions conflict
//! and retry. An atomic `Add` does not read: the storage servers apply it to whatever value
//! the key has at the commit, so concurrent increments never conflict. The example uses the
//! key `examples/counter` and clears it again.

use std::path::PathBuf;

use fdb_native_client::{Database, Error, MutationType};

const KEY: &[u8] = b"examples/counter";
const TASKS: u64 = 20;
const INCREMENTS: u64 = 10;

/// The counter's value: a little-endian integer, as `Add` reads and writes it.
fn decode(value: Option<Vec<u8>>) -> i64 {
    let mut bytes = [0; 8];
    if let Some(value) = value {
        let n = value.len().min(8);
        bytes[..n].copy_from_slice(&value[..n]);
    }
    i64::from_le_bytes(bytes)
}

/// Adds `delta` to the counter. Note that `Add` is not idempotent: if `run` retries after a
/// commit that may have happened (`commit_unknown_result`), the increment may be applied
/// twice. Counters that must be exact also record a unique id of each increment.
async fn add(db: &Database, delta: i64) -> Result<(), Error> {
    db.run(|tx, _maybe_committed| async move {
        tx.atomic_op(KEY, &delta.to_le_bytes(), MutationType::Add);
        Ok(())
    })
    .await
}

pub(crate) async fn run(db: &Database) -> Result<(), Error> {
    db.run(|tx, _| async move {
        tx.clear(KEY);
        Ok(())
    })
    .await?;

    let tasks: Vec<_> = (0..TASKS)
        .map(|_| {
            let db = db.clone();
            tokio::spawn(async move {
                for _ in 0..INCREMENTS {
                    add(&db, 1).await?;
                }
                Ok::<_, Error>(())
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("an increment task")?;
    }

    let value = decode(db.run(|tx, _| async move { tx.get(KEY).await }).await?);
    println!("counter = {value}");
    assert_eq!(value, (TASKS * INCREMENTS) as i64);

    // A transaction's reads see its own atomic operations (read-your-writes) too, applied to
    // the value it reads.
    let seen = db
        .run(|tx, _| async move {
            tx.atomic_op(KEY, &(-5i64).to_le_bytes(), MutationType::Add);
            Ok(decode(tx.get(KEY).await?))
        })
        .await?;
    assert_eq!(seen, (TASKS * INCREMENTS) as i64 - 5);

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
