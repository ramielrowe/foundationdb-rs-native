//! Options, timeouts and retry limits, and the errors they give.
//!
//! ```text
//! cargo run -p fdb-native-client --example options -- [CLUSTER_FILE]
//! ```
//!
//! Database options set defaults for every transaction created afterwards; transaction
//! options apply to one transaction. `Timeout`, `RetryLimit` and `MaxRetryDelay` survive the
//! retries of `on_error` (and so of `Database::run`), the other options last one attempt.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::{Database, DatabaseOption, Error, TransactionOption};

const KEY: &[u8] = b"examples/options/key";

pub(crate) async fn run(db: &Database) -> Result<(), Error> {
    // Defaults for every new transaction: a 10 s timeout, so that an unreachable cluster
    // fails with `transaction_timed_out` instead of waiting forever, and at most 20 retries.
    db.set_option(DatabaseOption::TransactionTimeout(Duration::from_secs(10)))?;
    db.set_option(DatabaseOption::TransactionRetryLimit(20))?;

    // Per transaction: options set in the closure apply to that attempt (setting the
    // persistent ones again on every attempt is fine).
    db.run(|tx, _| async move {
        tx.set_option(TransactionOption::Timeout(Duration::from_secs(5)))?;
        // Low priority: throttled first when the cluster is saturated.
        tx.set_option(TransactionOption::PriorityBatch)?;
        // A smaller size limit than the cluster's 10 MB.
        tx.set_option(TransactionOption::SizeLimit(100_000))?;
        tx.set(KEY, b"value");
        Ok(())
    })
    .await?;

    // A value out of range is refused at once: `invalid_option_value` (2006).
    let tx = db.create_transaction();
    let e = tx.set_option(TransactionOption::SizeLimit(1)).unwrap_err();
    assert_eq!(e.code(), codes::INVALID_OPTION_VALUE);
    println!("SizeLimit(1): {e}");

    // A timeout counts from the transaction's creation (or last reset): once it has passed,
    // every operation fails with `transaction_timed_out` (1031), which `on_error` does not
    // retry.
    let tx = db.create_transaction();
    tx.set_option(TransactionOption::Timeout(Duration::from_millis(1)))?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let e = tx.get(KEY).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TIMED_OUT);
    assert!(!e.is_retryable());
    println!("after the timeout: {e}");

    // A retry limit bounds `run`: the closure fails with a conflict every time, `on_error`
    // retries it twice, then `run` returns the error.
    let attempts = AtomicU32::new(0);
    let e = db
        .run(|tx, _| {
            let attempts = &attempts;
            async move {
                tx.set_option(TransactionOption::RetryLimit(2))?;
                tx.set_option(TransactionOption::MaxRetryDelay(Duration::from_millis(50)))?;
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(Error::new(codes::NOT_COMMITTED))
            }
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::NOT_COMMITTED);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    // No attempt may have committed: the predicates say so (the code alone does not; `run`
    // returns a 1020 marked maybe committed if an earlier attempt ended in 1021).
    assert!(e.is_retryable() && e.is_retryable_not_committed() && !e.is_maybe_committed());
    println!("gave up after 3 attempts: {e}");

    // System keys need an option: `key_outside_legal_range` (2004) without it.
    let e = db
        .run(|tx, _| async move { tx.get(b"\xff/conf/storage_engine").await })
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::KEY_OUTSIDE_LEGAL_RANGE);
    let engine = db
        .run(|tx, _| async move {
            tx.set_option(TransactionOption::ReadSystemKeys)?;
            tx.get(b"\xff/conf/storage_engine").await
        })
        .await?;
    println!("storage engine (a system key): {engine:?}");

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
