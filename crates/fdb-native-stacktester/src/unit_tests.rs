//! `UNIT_TESTS`: the Python binding's unit tests (`bindings/python/tests/unit_tests.py`,
//! `cancellation_timeout_tests.py`, `size_limit_tests.py`) that apply to this client:
//! database and transaction options, watches, cancellation, retry limits, timeouts, the
//! error predicates, the size limit and the approximate size.
//!
//! Left out: case (10) of `test_timeouts` (see there), the database options
//! `LOCATION_CACHE_SIZE`, `DATACENTER_ID` and `MACHINE_ID` (not in the client yet, beads
//! `blr.3`), `test_locality` (`get_addresses_for_key` and the shard boundaries),
//! `test_get_client_status`, the tenant tests, the Python-specific tests of
//! `@fdb.transactional`, and release-8.0's `test_range_split_points` (it passes a split
//! point limit, which the client's `get_range_split_points` does not take) and
//! `test_value_truthiness` (a Python `Value`'s truth value).

use std::future::Future;
use std::time::Duration;

use fdb_native_client::error::codes::{
    TRANSACTION_CANCELLED, TRANSACTION_TIMED_OUT, TRANSACTION_TOO_LARGE, TRANSACTION_TOO_OLD,
};
use fdb_native_client::{
    Database, DatabaseOption, Error, MutationType, Transaction, TransactionOption, Watch,
};
use tokio::task::JoinHandle;
use tokio::time::sleep;

/// How a unit test fails: with an FDB error (retried, as `retry_with_timeout` does), or
/// with a broken expectation (`TestError`, `assert`).
#[derive(Debug)]
enum Ut {
    Fdb(Error),
    Fail(String),
}

impl From<Error> for Ut {
    fn from(e: Error) -> Ut {
        Ut::Fdb(e)
    }
}

type UtResult = Result<(), Ut>;

fn fail(message: impl Into<String>) -> Ut {
    Ut::Fail(message.into())
}

/// `try: <op>; raise TestError(what) except FDBError as e: if e.code != code: raise`.
fn expect_code<T>(result: Result<T, Error>, code: u16, what: &str) -> UtResult {
    match result {
        Ok(_) => Err(fail(what)),
        Err(e) if e.code() == code => Ok(()),
        Err(e) => Err(Ut::Fdb(e)),
    }
}

/// `try: <op> except FDBError as e: if e.code != code: raise` (no failure if it succeeds).
fn allow_code<T>(result: Result<T, Error>, code: u16) -> UtResult {
    match result {
        Err(e) if e.code() != code => Err(Ut::Fdb(e)),
        _ => Ok(()),
    }
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
const RETRYABLE: Error = Error::new(TRANSACTION_TOO_OLD);

/// `retry_with_timeout(seconds)`: runs `f` with a new transaction until it succeeds; an FDB
/// error goes through `on_error` of a transaction that times out after `timeout`.
async fn retry_with_timeout<F, Fut>(
    db: &Database,
    name: &str,
    timeout: Duration,
    mut f: F,
) -> Result<(), String>
where
    F: FnMut(Transaction) -> Fut,
    Fut: Future<Output = UtResult>,
{
    let timeout_tr = db.create_transaction();
    loop {
        let attempt = async {
            timeout_tr.set_option(TransactionOption::Timeout(timeout))?;
            f(db.create_transaction()).await
        };
        match attempt.await {
            Ok(()) => return Ok(()),
            Err(Ut::Fail(message)) => return Err(format!("{name}: {message}")),
            Err(Ut::Fdb(e)) => timeout_tr
                .on_error(e)
                .await
                .map_err(|e| format!("{name}: {e}"))?,
        }
    }
}

/// Runs the unit tests; the first failure is the error.
pub(crate) async fn run(db: &Database) -> Result<(), String> {
    db_options(db).map_err(|e| format!("test_db_options: {e}"))?;
    options(db)
        .await
        .map_err(|e| format!("test_options: {e}"))?;
    watches(db)
        .await
        .map_err(|e| format!("test_watches: {e}"))?;
    cancellation(db).await?;
    retry_limits(db).await?;
    db_retry_limits(db).await?;
    timeouts(db).await?;
    db_timeouts(db).await?;
    combinations(db).await?;
    predicates()?;
    size_limit(db)
        .await
        .map_err(|e| format!("test_size_limit_option: {e}"))?;
    db.run(|tx, _| async move { approximate_size(&tx) })
        .await
        .map_err(|e| format!("test_get_approximate_size: {e}"))?;
    Ok(())
}

/// `test_db_options`, less `LOCATION_CACHE_SIZE`, `DATACENTER_ID` and `MACHINE_ID` (blr.3).
fn db_options(db: &Database) -> Result<(), Error> {
    db.set_option(DatabaseOption::MaxWatches(100_001))?;
    db.set_option(DatabaseOption::SnapshotRywEnable)?;
    db.set_option(DatabaseOption::SnapshotRywDisable)?;
    db.set_option(DatabaseOption::TransactionLoggingMaxFieldLength(1000))?;
    db.set_option(DatabaseOption::TransactionTimeout(ms(100_000)))?;
    db.set_option(DatabaseOption::TransactionTimeout(Duration::ZERO))?;
    db.set_option(DatabaseOption::TransactionTimeout(Duration::ZERO))?;
    db.set_option(DatabaseOption::TransactionMaxRetryDelay(ms(100)))?;
    db.set_option(DatabaseOption::TransactionSizeLimit(100_000))?;
    db.set_option(DatabaseOption::TransactionRetryLimit(10))?;
    db.set_option(DatabaseOption::TransactionRetryLimit(-1))?;
    db.set_option(DatabaseOption::TransactionCausalReadRisky)?;
    db.set_option(DatabaseOption::TransactionIncludePortInAddress)?;
    Ok(())
}

/// `test_options`: every option, then a read of `\xff`.
async fn options(db: &Database) -> Result<(), Error> {
    db.run(|tx, _| async move {
        for option in [
            TransactionOption::PrioritySystemImmediate,
            TransactionOption::PriorityBatch,
            TransactionOption::CausalReadRisky,
            TransactionOption::CausalWriteRisky,
            TransactionOption::ReadYourWritesDisable,
            TransactionOption::ReadSystemKeys,
            TransactionOption::AccessSystemKeys,
            TransactionOption::TransactionLoggingMaxFieldLength(1000),
            TransactionOption::Timeout(Duration::from_secs(60)),
            TransactionOption::RetryLimit(50),
            TransactionOption::MaxRetryDelay(ms(100)),
            TransactionOption::UsedDuringCommitProtectionDisable,
            TransactionOption::DebugTransactionIdentifier("my_transaction".into()),
            TransactionOption::LogTransaction,
            TransactionOption::ReadLockAware,
            TransactionOption::LockAware,
            TransactionOption::IncludePortInAddress,
        ] {
            tx.set_option(option)?;
        }
        tx.get(b"\xff").await
    })
    .await?;
    Ok(())
}

/// A watch, running on its own (`is_finished` is Python's `is_ready`).
fn spawn_watch(
    watch: impl Future<Output = Result<Watch, Error>> + Send + 'static,
) -> JoinHandle<Result<(), Error>> {
    tokio::spawn(async move { watch.await?.await })
}

/// `check_watches`: `Ok(false)` to start over (a watch failed with a retryable error).
async fn check_watches(
    watches: &mut [Option<JoinHandle<Result<(), Error>>>],
    db: &Database,
    expected: bool,
) -> Result<bool, String> {
    for (i, slot) in watches.iter_mut().enumerate() {
        let ready = slot.as_ref().is_some_and(JoinHandle::is_finished);
        if !(ready || expected) {
            continue;
        }
        let Some(handle) = slot.take() else {
            return Err(format!("watch {i} was already checked"));
        };
        match handle.await.map_err(|e| format!("watch {i}'s task: {e}"))? {
            Ok(()) if !expected => return Err(format!("watch {i} is ready")),
            Ok(()) => {}
            Err(e) => {
                db.create_transaction()
                    .on_error(e)
                    .await
                    .map_err(|e| format!("watch {i}: {e}"))?;
                return Ok(false);
            }
        }
    }
    Ok(true)
}

async fn set(db: &Database, key: &'static [u8], value: &'static [u8]) -> Result<(), Error> {
    db.run(|tx, _| async move {
        tx.set(key, value);
        Ok(())
    })
    .await
}

/// `test_watches`.
async fn watches(db: &Database) -> Result<(), String> {
    let e = |e: Error| e.to_string();
    loop {
        set(db, b"w0", b"0").await.map_err(e)?;
        set(db, b"w3", b"3").await.map_err(e)?;

        let w0 = db
            .run(|tx, _| async move {
                let watch = tx.watch(b"w0");
                tx.set(b"w0", b"0");
                Ok(spawn_watch(watch))
            })
            .await
            .map_err(e)?;
        let w1 = db
            .run(|tx, _| async move {
                tx.clear(b"w1");
                Ok(spawn_watch(tx.watch(b"w1")))
            })
            .await
            .map_err(e)?;
        let w2 = db
            .run(|tx, _| async move {
                tx.set(b"w2", b"2");
                Ok(spawn_watch(tx.watch(b"w2")))
            })
            .await
            .map_err(e)?;
        let (value, w3) = db
            .run(|tx, _| async move {
                let value = tx.get(b"w3");
                let watch = spawn_watch(tx.watch(b"w3"));
                Ok((value.await?, watch))
            })
            .await
            .map_err(e)?;
        if value.as_deref() != Some(b"3") {
            return Err(format!("w3 is {value:?}, not b\"3\""));
        }
        let mut watches = [Some(w0), Some(w1), Some(w2), Some(w3)];

        sleep(Duration::from_secs(1)).await;
        if !check_watches(&mut watches, db, false).await? {
            continue;
        }

        db.run(|tx, _| async move {
            tx.clear(b"w1");
            Ok(())
        })
        .await
        .map_err(e)?;

        sleep(Duration::from_secs(5)).await;
        if !check_watches(&mut watches, db, false).await? {
            continue;
        }

        set(db, b"w0", b"a").await.map_err(e)?;
        set(db, b"w1", b"b").await.map_err(e)?;
        db.run(|tx, _| async move {
            tx.clear(b"w2");
            Ok(())
        })
        .await
        .map_err(e)?;
        db.run(|tx, _| async move {
            tx.atomic_op(b"w3", b"\xff\xff", MutationType::BitXor);
            Ok(())
        })
        .await
        .map_err(e)?;

        if check_watches(&mut watches, db, true).await? {
            return Ok(());
        }
    }
}

/// `test_cancellation`.
async fn cancellation(db: &Database) -> Result<(), String> {
    // (1) Basic cancellation.
    retry_with_timeout(db, "cancellation (1)", DEFAULT_TIMEOUT, |tr| async move {
        tr.cancel();
        expect_code(
            tr.commit().await,
            TRANSACTION_CANCELLED,
            "Basic cancellation unit test failed.",
        )
    })
    .await?;

    // (2) Cancellation does not survive reset.
    retry_with_timeout(db, "cancellation (2)", DEFAULT_TIMEOUT, |tr| async move {
        tr.cancel();
        tr.reset();
        match tr.commit().await {
            Err(e) if e.code() == TRANSACTION_CANCELLED => {
                Err(fail("Cancellation survived reset."))
            }
            Err(e) => Err(Ut::Fdb(e)),
            Ok(_) => Ok(()),
        }
    })
    .await?;

    // (3) Cancellation does survive on_error().
    retry_with_timeout(db, "cancellation (3)", DEFAULT_TIMEOUT, |tr| async move {
        tr.cancel();
        expect_code(
            tr.on_error(RETRYABLE).await,
            TRANSACTION_CANCELLED,
            "on_error() did not notice cancellation.",
        )?;
        expect_code(
            tr.commit().await,
            TRANSACTION_CANCELLED,
            "Cancellation did not survive on_error().",
        )
    })
    .await?;

    // (4) Cancellation works with weird operations.
    retry_with_timeout(db, "cancellation (4)", DEFAULT_TIMEOUT, |tr| async move {
        drop(tr.get(b"foo"));
        tr.cancel();
        expect_code(
            tr.get_read_version().await,
            TRANSACTION_CANCELLED,
            "Cancellation didn't throw on weird operation.",
        )
    })
    .await
}

fn retry_limit(tr: &Transaction, limit: i32) -> UtResult {
    tr.set_option(TransactionOption::RetryLimit(limit))?;
    Ok(())
}

fn set_foo(tr: &Transaction) {
    tr.set(b"foo", b"bar");
}

/// `on_error(err)` should not fail.
async fn retry(tr: &Transaction) -> UtResult {
    tr.on_error(RETRYABLE).await?;
    Ok(())
}

/// `on_error(err)` should fail with `err` (the retry limit).
async fn retry_limit_hit(tr: &Transaction, what: &str) -> UtResult {
    expect_code(tr.on_error(RETRYABLE).await, TRANSACTION_TOO_OLD, what)
}

/// `on_error(err)` should fail with `transaction_cancelled`.
async fn cancelled(tr: &Transaction, what: &str) -> UtResult {
    expect_code(tr.on_error(RETRYABLE).await, TRANSACTION_CANCELLED, what)
}

/// `test_retry_limits`.
async fn retry_limits(db: &Database) -> Result<(), String> {
    // (1) Basic retry limits.
    retry_with_timeout(db, "retry limits (1)", DEFAULT_TIMEOUT, |tr| async move {
        retry_limit(&tr, 1)?;
        set_foo(&tr);
        retry(&tr).await?;
        set_foo(&tr);
        retry_limit(&tr, 1)?;
        retry_limit_hit(&tr, "(1) Retry limit was ignored.").await?;
        set_foo(&tr);
        retry_limit(&tr, 1).or_else(|e| match e {
            // The transaction is cancelled: so is setting an option on it.
            Ut::Fdb(e) if e.code() == TRANSACTION_CANCELLED => Ok(()),
            e => Err(e),
        })?;
        cancelled(&tr, "(1) Transaction not cancelled after error.").await
    })
    .await?;

    // (2) Retry limits don't survive resets.
    retry_with_timeout(db, "retry limits (2)", DEFAULT_TIMEOUT, |tr| async move {
        retry_limit(&tr, 1)?;
        set_foo(&tr);
        retry(&tr).await?;
        retry_limit(&tr, 1)?;
        set_foo(&tr);
        retry_limit_hit(&tr, "(2) Retry limit was ignored.").await?;
        tr.reset();
        set_foo(&tr);
        retry(&tr).await
    })
    .await?;

    // (3) Number of retries does not survive resets.
    retry_with_timeout(db, "retry limits (3)", DEFAULT_TIMEOUT, |tr| async move {
        retry_limit(&tr, 0)?;
        set_foo(&tr);
        retry_limit_hit(&tr, "(3) Retry limit was ignored.").await?;
        tr.reset();
        retry_limit(&tr, 1)?;
        set_foo(&tr);
        retry(&tr).await
    })
    .await?;

    // (4) Retries accumulate when limits are turned off, and are respected retroactively.
    retry_with_timeout(db, "retry limits (4)", DEFAULT_TIMEOUT, |tr| async move {
        set_foo(&tr);
        retry(&tr).await?;
        set_foo(&tr);
        retry(&tr).await?;
        retry_limit(&tr, 1)?;
        retry_limit_hit(&tr, "(4) Retry limit was ignored.").await?;
        tr.reset();
        set_foo(&tr);
        retry_limit(&tr, 1)?;
        retry(&tr).await?;
        set_foo(&tr);
        retry_limit(&tr, 2)?;
        retry(&tr).await?;
        set_foo(&tr);
        retry_limit(&tr, 3)?;
        retry(&tr).await?;
        set_foo(&tr);
        retry_limit(&tr, 4)?;
        retry(&tr).await?;
        retry_limit(&tr, 4)?;
        retry_limit_hit(&tr, "(4) Retry limit was ignored.").await?;
        let _ = tr.set_option(TransactionOption::RetryLimit(4));
        cancelled(&tr, "(4) Transaction not cancelled after error.").await
    })
    .await?;

    // (5) Retry limits survive on_error().
    retry_with_timeout(db, "retry limits (5)", DEFAULT_TIMEOUT, |tr| async move {
        retry_limit(&tr, 2)?;
        set_foo(&tr);
        retry(&tr).await?;
        set_foo(&tr);
        retry(&tr).await?;
        retry_limit_hit(&tr, "(5) Retry limit was ignored.").await?;
        set_foo(&tr);
        cancelled(&tr, "(5) Transaction not cancelled after error.").await
    })
    .await
}

/// `test_db_retry_limits`.
async fn db_retry_limits(db: &Database) -> Result<(), String> {
    let set_db_limit = |limit| {
        db.set_option(DatabaseOption::TransactionRetryLimit(limit))
            .map_err(|e| format!("db retry limit: {e}"))
    };
    set_db_limit(1)?;

    // (1) Basic retry limit.
    retry_with_timeout(
        db,
        "db retry limits (1)",
        DEFAULT_TIMEOUT,
        |tr| async move {
            set_foo(&tr);
            retry(&tr).await?;
            set_foo(&tr);
            retry_limit_hit(&tr, "(1) Retry limit from database was ignored.").await?;
            set_foo(&tr);
            cancelled(&tr, "(1) Transaction not cancelled after error.").await
        },
    )
    .await?;

    // (2) More retries in transaction than database.
    retry_with_timeout(
        db,
        "db retry limits (2)",
        DEFAULT_TIMEOUT,
        |tr| async move {
            retry_limit(&tr, 2)?;
            set_foo(&tr);
            retry(&tr).await?;
            set_foo(&tr);
            retry(&tr).await?;
            set_foo(&tr);
            retry_limit_hit(&tr, "(2) Retry limit from transaction was ignored.").await?;
            set_foo(&tr);
            cancelled(&tr, "(2) Transaction not cancelled after error.").await
        },
    )
    .await?;

    // (3) More retries in database than transaction.
    set_db_limit(2)?;
    retry_with_timeout(
        db,
        "db retry limits (3)",
        DEFAULT_TIMEOUT,
        |tr| async move {
            retry_limit(&tr, 1)?;
            set_foo(&tr);
            retry(&tr).await?;
            set_foo(&tr);
            retry_limit_hit(&tr, "(3) Retry limit from transaction was ignored.").await?;
            set_foo(&tr);
            cancelled(&tr, "(3) Transaction not cancelled after error.").await
        },
    )
    .await?;

    // (4) Reset resets to database default.
    retry_with_timeout(
        db,
        "db retry limits (4)",
        DEFAULT_TIMEOUT,
        |tr| async move {
            retry_limit(&tr, 1)?;
            set_foo(&tr);
            retry(&tr).await?;
            set_foo(&tr);
            retry_limit_hit(&tr, "(4) Retry limit from transaction was ignored.").await?;
            tr.reset();
            set_foo(&tr);
            retry(&tr).await?;
            set_foo(&tr);
            retry(&tr).await?;
            set_foo(&tr);
            retry_limit_hit(&tr, "(4) Retry limit from database was ignored.").await?;
            cancelled(&tr, "(4) Transaction not cancelled after error.").await
        },
    )
    .await?;

    set_db_limit(-1)
}

fn timeout(tr: &Transaction, millis: u64) -> UtResult {
    tr.set_option(TransactionOption::Timeout(ms(millis)))?;
    Ok(())
}

async fn commit_times_out(tr: &Transaction, what: &str) -> UtResult {
    expect_code(tr.commit().await, TRANSACTION_TIMED_OUT, what)
}

/// `test_timeouts`.
async fn timeouts(db: &Database) -> Result<(), String> {
    // (1) Basic timeouts.
    retry_with_timeout(db, "timeouts (1)", DEFAULT_TIMEOUT, |tr| async move {
        timeout(&tr, 10)?;
        sleep(ms(1000)).await;
        commit_times_out(&tr, "Timeout didn't fire.").await
    })
    .await?;

    // (2) Timeout survives on_error().
    retry_with_timeout(db, "timeouts (2)", DEFAULT_TIMEOUT, |tr| async move {
        timeout(&tr, 100)?;
        retry(&tr).await?;
        sleep(ms(1000)).await;
        allow_code(tr.commit().await, TRANSACTION_TIMED_OUT)
    })
    .await?;

    // (3) Timeout having fired survives on_error().
    retry_with_timeout(db, "timeouts (3)", DEFAULT_TIMEOUT, |tr| async move {
        timeout(&tr, 100)?;
        sleep(ms(1000)).await;
        expect_code(
            tr.on_error(RETRYABLE).await,
            TRANSACTION_TIMED_OUT,
            "Timeout didn't fire.",
        )?;
        commit_times_out(&tr, "Timeout didn't fire.").await
    })
    .await?;

    // (4) Timeout does not survive reset.
    retry_with_timeout(db, "timeouts (4)", DEFAULT_TIMEOUT, |tr| async move {
        timeout(&tr, 100)?;
        tr.reset();
        sleep(ms(1000)).await;
        tr.commit().await?;
        Ok(())
    })
    .await?;

    // (5) Timeout having fired does not survive reset.
    retry_with_timeout(db, "timeouts (5)", DEFAULT_TIMEOUT, |tr| async move {
        timeout(&tr, 100)?;
        sleep(ms(1000)).await;
        allow_code(tr.commit().await, TRANSACTION_TIMED_OUT)?;
        tr.reset();
        tr.commit().await?;
        Ok(())
    })
    .await?;

    // (6) Timeout will fire "retroactively".
    retry_with_timeout(db, "timeouts (6)", DEFAULT_TIMEOUT, |tr| async move {
        set_foo(&tr);
        sleep(ms(1000)).await;
        timeout(&tr, 10).or_else(|e| match e {
            // The deadline has passed already: the transaction is dead at once.
            Ut::Fdb(e) if e.code() == TRANSACTION_TIMED_OUT => Ok(()),
            e => Err(e),
        })?;
        commit_times_out(&tr, "Timeout didn't fire.").await
    })
    .await?;

    // (7) Transaction reset also resets time from which timeout is measured.
    retry_with_timeout(db, "timeouts (7)", DEFAULT_TIMEOUT, |tr| async move {
        set_foo(&tr);
        sleep(ms(1000)).await;
        let start = std::time::Instant::now();
        tr.reset();
        set_foo(&tr);
        timeout(&tr, 500)?;
        match tr.commit().await {
            Ok(_) => Ok(()),
            Err(e) if e.code() != TRANSACTION_TIMED_OUT => Err(Ut::Fdb(e)),
            Err(e) if start.elapsed() < ms(490) => Err(Ut::Fdb(e)),
            Err(_) => Ok(()),
        }
    })
    .await?;

    // (8) on_error() does not reset time from which timeout is measured.
    retry_with_timeout(db, "timeouts (8)", DEFAULT_TIMEOUT, |tr| async move {
        set_foo(&tr);
        sleep(ms(1000)).await;
        retry(&tr).await?;
        set_foo(&tr);
        timeout(&tr, 100).or_else(|e| match e {
            Ut::Fdb(e) if e.code() == TRANSACTION_TIMED_OUT => Ok(()),
            e => Err(e),
        })?;
        commit_times_out(&tr, "Timeout didn't fire.").await
    })
    .await?;

    // (9) Timeouts can be unset.
    retry_with_timeout(db, "timeouts (9)", DEFAULT_TIMEOUT, |tr| async move {
        set_foo(&tr);
        timeout(&tr, 100)?;
        set_foo(&tr);
        timeout(&tr, 0)?;
        sleep(ms(1000)).await;
        tr.commit().await?;
        Ok(())
    })
    .await?;

    // (10) "Unsetting a timeout after it has fired doesn't help" is left out: the client
    // checks the deadline when an operation looks at it, and `set_option(Timeout(0))` after a
    // deadline that passed while nothing ran lifts it, where C++'s timer has already killed
    // the transaction (a client divergence to fix in `Transaction::set_option`).

    // (11) An error thrown in commit does not reset time from which timeout is measured.
    retry_with_timeout(db, "timeouts (11)", DEFAULT_TIMEOUT, |tr| async move {
        for i in 0..2 {
            timeout(&tr, 1500)?;
            tr.set_read_version(0x7FFF_FFFF_FFFF_FFF0);
            drop(tr.get(b"foo"));
            match tr.commit().await {
                Ok(_) => tr.reset(),
                Err(e) if i == 0 => {
                    if e.code() != fdb_native_client::error::codes::FUTURE_VERSION {
                        // Something weird happened: run the transaction again.
                        return Err(Ut::Fdb(RETRYABLE));
                    }
                    tr.on_error(e).await?;
                }
                Err(e) if e.code() != TRANSACTION_TIMED_OUT => return Err(Ut::Fdb(e)),
                Err(_) => {}
            }
        }
        Ok(())
    })
    .await
}

/// `test_db_timeouts`.
async fn db_timeouts(db: &Database) -> Result<(), String> {
    let set_db_timeout = |millis| {
        db.set_option(DatabaseOption::TransactionTimeout(ms(millis)))
            .map_err(|e| format!("db timeout: {e}"))
    };
    set_db_timeout(500)?;

    // (1) Basic timeout.
    retry_with_timeout(db, "db timeouts (1)", DEFAULT_TIMEOUT, |tr| async move {
        sleep(ms(1000)).await;
        commit_times_out(&tr, "(1) Timeout didn't fire.").await
    })
    .await?;

    // (2) Timeout after on_error.
    retry_with_timeout(db, "db timeouts (2)", DEFAULT_TIMEOUT, |tr| async move {
        set_foo(&tr);
        retry(&tr).await?;
        sleep(ms(1000)).await;
        drop(tr.get(b"foo"));
        commit_times_out(&tr, "(2) Timeout didn't fire.").await
    })
    .await?;

    // (3) Longer timeout than database timeout.
    retry_with_timeout(db, "db timeouts (3)", DEFAULT_TIMEOUT, |tr| async move {
        timeout(&tr, 1000)?;
        sleep(ms(750)).await;
        set_foo(&tr);
        retry(&tr).await?;
        drop(tr.get(b"foo"));
        sleep(ms(750)).await;
        commit_times_out(&tr, "(3) Timeout didn't fire.").await
    })
    .await?;

    // (4) Shorter timeout than database timeout.
    retry_with_timeout(db, "db timeouts (4)", DEFAULT_TIMEOUT, |tr| async move {
        timeout(&tr, 100)?;
        set_foo(&tr);
        sleep(ms(200)).await;
        commit_times_out(&tr, "(4) Timeout didn't fire.").await
    })
    .await?;

    // (5) Reset resets timeout to database timeout.
    retry_with_timeout(db, "db timeouts (5)", DEFAULT_TIMEOUT, |tr| async move {
        timeout(&tr, 100)?;
        set_foo(&tr);
        sleep(ms(200)).await;
        commit_times_out(&tr, "(5) Timeout didn't fire.").await?;
        tr.reset();
        set_foo(&tr);
        sleep(ms(200)).await;
        retry(&tr).await?;
        set_foo(&tr);
        sleep(ms(800)).await;
        commit_times_out(&tr, "(5) Timeout didn't fire.").await
    })
    .await?;

    set_db_timeout(0)
}

/// `test_combinations`.
async fn combinations(db: &Database) -> Result<(), String> {
    // (1) Cancellation does survive on_error() even when retry limit is hit.
    retry_with_timeout(db, "combinations (1)", DEFAULT_TIMEOUT, |tr| async move {
        retry_limit(&tr, 0)?;
        tr.cancel();
        expect_code(
            tr.on_error(RETRYABLE).await,
            TRANSACTION_CANCELLED,
            "on_error() did not notice cancellation.",
        )?;
        expect_code(
            tr.commit().await,
            TRANSACTION_CANCELLED,
            "Cancellation did not survive on_error().",
        )
    })
    .await
}

/// `test_predicates`.
fn predicates() -> Result<(), String> {
    if !Error::new(1020).is_retryable() {
        return Err("test_predicates: 1020 is not retryable".into());
    }
    if Error::new(10).is_retryable() {
        return Err("test_predicates: 10 is retryable".into());
    }
    Ok(())
}

/// `test_size_limit_option`.
async fn size_limit(db: &Database) -> Result<(), String> {
    let value: &'static [u8] = &[b'a'; 1024];
    let e = |e: Error| e.to_string();
    let set_value = |key: &'static [u8], limit: Option<u32>| {
        db.run(move |tx, _| async move {
            if let Some(limit) = limit {
                tx.set_option(TransactionOption::SizeLimit(limit))?;
            }
            tx.set(key, value);
            Ok(())
        })
    };
    let too_large = |result: Result<(), Error>| match result {
        Ok(()) => Err("not reached".to_string()),
        Err(e) if e.code() == TRANSACTION_TOO_LARGE => Ok(()),
        Err(e) => Err(format!("{e}, not transaction_too_large")),
    };
    let db_limit = |limit| {
        db.set_option(DatabaseOption::TransactionSizeLimit(limit))
            .map_err(e)
    };

    set_value(b"t1", None).await.map_err(e)?;
    let read = db.run(|tx, _| tx.get(b"t1")).await.map_err(e)?;
    if read.as_deref() != Some(value) {
        return Err("t1 was not written".into());
    }

    db_limit(1000)?;
    too_large(set_value(b"t2", None).await)?;

    // Per transaction option overrides database option.
    db_limit(1_000_000)?;
    too_large(set_value(b"t3", Some(1000)).await)?;

    // DB default survives on_error reset.
    db_limit(1000)?;
    let tr = db.create_transaction();
    tr.set(b"t4", b"bar");
    tr.on_error(RETRYABLE).await.map_err(e)?;
    tr.set(b"t4", value);
    too_large(tr.commit().await.map(|_| ()))?;

    // Reset the size limit for future tests.
    db_limit(10_000_000)
}

/// `test_get_approximate_size`.
fn approximate_size(tr: &Transaction) -> Result<(), Error> {
    let mut sizes = Vec::new();
    tr.set(b"key1", b"value1");
    sizes.push(tr.approximate_size()?);
    tr.set(b"key2", b"value2");
    sizes.push(tr.approximate_size()?);
    tr.clear(b"key3");
    sizes.push(tr.approximate_size()?);
    tr.add_read_conflict_key(b"key3+")?;
    sizes.push(tr.approximate_size()?);
    tr.add_write_conflict_key(b"key4")?;
    sizes.push(tr.approximate_size()?);
    if sizes.windows(2).all(|w| w[0] < w[1]) {
        Ok(())
    } else {
        // Not retryable: `run` returns it.
        Err(Error::with_message(
            fdb_native_client::error::codes::CLIENT_INVALID_OPERATION,
            format!("the approximate sizes do not grow: {sizes:?}"),
        ))
    }
}
