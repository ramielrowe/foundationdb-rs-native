//! `Database` and `Transaction` against scripted fake GRV and commit proxies on an in-memory
//! network under tokio's paused clock, with a real `ClusterMonitor` fed by a fake coordinator:
//! the read version a commit asks for, `on_error`'s backoff, retry limit and cancellation,
//! timeouts (also during a commit, which may have committed), cancellation, deferred errors,
//! `used_during_commit`, and the `run` retry loop. Reads from storage servers are covered by
//! the docker tests.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::{
    ConnectionRecord, Database, DatabaseOptions, Error, MaybeCommitted, MutationType, Transaction,
    TransactionOption,
};
use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::{CommitId, GetReadVersion, MutationType as Mt};
use fdb_native_transport::peer::PeerManagerConfig;
use fdb_native_wire::ProtocolVersion;
use support::proxy::FakeProxy;
use support::{FakeCoordinator, MemRuntime, Net, info, sock};
use tokio::time::Instant;

const SCHEMAS: [SchemaVersion; 2] = [SchemaVersion::V7_4, SchemaVersion::V8_0];

struct Env {
    db: Database<MemRuntime>,
    _coord: FakeCoordinator,
    grv: FakeProxy,
    commit: FakeProxy,
}

async fn env(schema: SchemaVersion) -> Env {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let version = schema.protocol_version();
    let mut coord = FakeCoordinator::start(&net, sock(1, 4500), version);
    let commit = FakeProxy::start(&net, sock(150, 4500), version);
    let grv = FakeProxy::start(&net, sock(100, 4500), version);
    let mut options = DatabaseOptions::default();
    options.peers = PeerManagerConfig::default().without_jitter();
    options.peers.supported_versions = vec![ProtocolVersion::V7_4, ProtocolVersion::V8_0];
    options.peers.seed = Some(1);
    options.monitor.seed = Some(7);
    options.commit.seed = Some(3);
    let record = ConnectionRecord::memory("test:abc@10.0.0.1:4500".parse().unwrap());
    let db = Database::from_record(rt, record, options).expect("database");
    coord.next_call().await.reply(&info(1, 1, 1, schema));
    db.cluster_monitor().proxies(false).await.unwrap();
    Env {
        db,
        _coord: coord,
        grv,
        commit,
    }
}

fn spawn<T: Send + 'static>(
    f: impl std::future::Future<Output = T> + Send + 'static,
) -> tokio::task::JoinHandle<T> {
    tokio::spawn(f)
}

async fn done<T>(handle: tokio::task::JoinHandle<T>) -> T {
    tokio::time::timeout(Duration::from_secs(120), handle)
        .await
        .expect("finished within 120 s")
        .expect("no panic")
}

fn committed(version: i64, batch: u16) -> Result<CommitId, u16> {
    Ok(CommitId {
        version,
        txn_batch_id: batch,
        ..CommitId::default()
    })
}

#[tokio::test(start_paused = true)]
async fn a_write_only_commit_reads_a_causal_read_risky_version_and_commits() {
    for schema in SCHEMAS {
        let mut env = env(schema).await;
        let tx = env.db.create_transaction();
        tx.set(b"k", b"v");
        tx.clear_range(b"a", b"b");
        tx.atomic_op(b"n", &1u64.to_le_bytes(), MutationType::ADD_VALUE);
        let handle = spawn({
            let tx = tx.clone();
            async move { tx.commit().await }
        });
        let grv = env.grv.next().await;
        let (request, _) = grv.grv();
        assert_ne!(request.flags & GetReadVersion::FLAG_CAUSAL_READ_RISKY, 0);
        grv.reply_grv(100);
        let call = env.commit.next().await;
        let (commit, _) = call.commit();
        assert_eq!(commit.read_snapshot, 100);
        let kinds: Vec<_> = commit.mutations.iter().map(|m| m.kind).collect();
        assert_eq!(kinds, vec![Mt::SET_VALUE, Mt::CLEAR_RANGE, Mt::ADD_VALUE]);
        // A blind write: the self-conflicting key is a read and a write conflict range.
        assert_eq!(commit.read_conflict_ranges.len(), 1);
        assert_eq!(commit.flags, 0);
        call.reply_commit(committed(120, 3));
        let result = done(handle).await.unwrap();
        assert_eq!(result.committed_version(), 120);
        assert_eq!(
            result.versionstamp(),
            Some([0, 0, 0, 0, 0, 0, 0, 120, 0, 3])
        );
        // The commit started: using the transaction again is `used_during_commit`.
        tx.set(b"k", b"v2");
        assert_eq!(
            tx.commit().await.unwrap_err().code(),
            codes::USED_DURING_COMMIT
        );
        tx.reset();
        assert_eq!(tx.commit().await.unwrap().committed_version(), -1);
    }
}

#[tokio::test(start_paused = true)]
async fn a_transaction_with_nothing_to_write_sends_nothing() {
    let mut env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let result = tx.commit().await.unwrap();
    assert_eq!(result.committed_version(), -1);
    assert_eq!(result.versionstamp(), None);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(env.grv.try_next().is_none());
    assert!(env.commit.try_next().is_none());
}

#[tokio::test(start_paused = true)]
async fn options_reach_the_requests_and_a_set_read_version_is_used() {
    let mut env = env(SchemaVersion::V7_4).await;
    let tx = env.db.create_transaction();
    tx.set_read_version(77);
    tx.set_option(TransactionOption::LockAware).unwrap();
    tx.set_option(TransactionOption::CausalWriteRisky).unwrap();
    tx.set_option(TransactionOption::ReportConflictingKeys)
        .unwrap();
    tx.add_read_conflict_range(b"r", b"s");
    tx.set(b"k", b"v");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    let call = env.commit.next().await;
    let (commit, _) = call.commit();
    assert_eq!(commit.read_snapshot, 77, "no read version was requested");
    assert!(commit.report_conflicting_keys);
    assert_ne!(
        commit.flags & fdb_native_schema::domain::Commit::FLAG_IS_LOCK_AWARE,
        0
    );
    // Causal write risky: no self-conflicting key.
    assert_eq!(commit.read_conflict_ranges.len(), 1);
    call.reply_commit(Ok(CommitId {
        version: -1,
        conflicting_kr_indices: Some(vec![0]),
        ..CommitId::default()
    }));
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::NOT_COMMITTED);
    assert_eq!(e.conflicting_ranges().unwrap()[0].begin, b"r");
    assert!(env.grv.try_next().is_none());

    // on_error forgets the read version and the (non-persistent) options.
    tx.on_error(e).await.unwrap();
    tx.set(b"k", b"v");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    env.grv.next().await.reply_grv(200);
    let call = env.commit.next().await;
    let (commit, _) = call.commit();
    assert_eq!(commit.read_snapshot, 200);
    assert!(!commit.report_conflicting_keys);
    assert_eq!(commit.flags, 0);
    // A self-conflicting key again.
    assert_eq!(commit.read_conflict_ranges.len(), 1);
    assert!(
        commit.read_conflict_ranges[0]
            .begin
            .starts_with(b"\xff/SC/")
    );
    call.reply_commit(committed(210, 0));
    done(handle).await.unwrap();

    // A second read version is an error, deferred to the commit.
    let tx = env.db.create_transaction();
    tx.set_read_version(5);
    tx.set_read_version(6);
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::READ_VERSION_ALREADY_SET
    );
    let tx = env.db.create_transaction();
    tx.set_read_version(0);
    assert_eq!(
        tx.get_read_version().await.unwrap_err().code(),
        codes::VERSION_INVALID
    );
}

#[tokio::test(start_paused = true)]
async fn write_errors_are_deferred_to_the_commit() {
    let mut env = env(SchemaVersion::V8_0).await;
    type Write = fn(&Transaction<MemRuntime>);
    let cases: [(Write, u16); 6] = [
        (|tx| tx.set(&[b'k'; 10_001], b"v"), codes::KEY_TOO_LARGE),
        (|tx| tx.set(b"k", &[0; 100_001]), codes::VALUE_TOO_LARGE),
        (
            |tx| tx.set(b"\xffsystem", b"v"),
            codes::KEY_OUTSIDE_LEGAL_RANGE,
        ),
        (|tx| tx.clear(b"\xff\xff"), codes::KEY_OUTSIDE_LEGAL_RANGE),
        (|tx| tx.clear_range(b"b", b"a"), codes::INVERTED_RANGE),
        (
            |tx| tx.clear_range(b"a", b"\xff\x00"),
            codes::KEY_OUTSIDE_LEGAL_RANGE,
        ),
    ];
    for (write, code) in cases {
        let tx = env.db.create_transaction();
        tx.set(b"fine", b"v");
        write(&tx);
        // The first error stays.
        tx.set(&[b'k'; 20_000], b"v");
        let e = tx.commit().await.unwrap_err();
        assert_eq!(e.code(), code, "{e}");
        assert!(!e.is_retryable());
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(env.grv.try_next().is_none(), "nothing was sent");
    assert!(env.commit.try_next().is_none(), "nothing was sent");

    // Reads too: a key past `\xff` without the option.
    let tx = env.db.create_transaction();
    assert_eq!(
        tx.get(b"\xffkey").await.unwrap_err().code(),
        codes::KEY_OUTSIDE_LEGAL_RANGE
    );
    // A clear range up to `\xff` itself is legal.
    let tx = env.db.create_transaction();
    tx.clear_range(b"", b"\xff");
    tx.set_option(TransactionOption::AccessSystemKeys).unwrap();
    tx.set(b"\xffsystem", b"v");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    env.grv.next().await.reply_grv(10);
    let call = env.commit.next().await;
    assert_eq!(call.commit().0.mutations.len(), 2);
    call.reply_commit(committed(11, 0));
    done(handle).await.unwrap();

    // Out-of-range option values are refused at once.
    let tx = env.db.create_transaction();
    for option in [
        TransactionOption::RetryLimit(-2),
        TransactionOption::SizeLimit(31),
        TransactionOption::SizeLimit(10_000_001),
        TransactionOption::Timeout(Duration::from_secs(1 << 40)),
    ] {
        assert_eq!(
            tx.set_option(option).unwrap_err().code(),
            codes::INVALID_OPTION_VALUE
        );
    }
}

/// Runs `on_error(code)` and returns how long it waited.
async fn timed_on_error(tx: &Transaction<MemRuntime>, code: u16) -> Result<Duration, Error> {
    let start = Instant::now();
    tx.on_error(Error::new(code)).await?;
    Ok(start.elapsed())
}

#[tokio::test(start_paused = true)]
async fn on_error_backs_off_as_cpp_does() {
    let env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    // getBackoff: a random fraction of 10 ms, 20 ms, 40 ms, ... up to 1 s.
    let mut cap = Duration::from_millis(10);
    let mut total = Duration::ZERO;
    for i in 0..12 {
        let waited = timed_on_error(&tx, codes::NOT_COMMITTED).await.unwrap();
        assert!(
            waited <= cap + Duration::from_millis(1),
            "retry {i}: {waited:?} > {cap:?}"
        );
        total += waited;
        cap = (cap * 2).min(Duration::from_secs(1));
    }
    assert!(total > Duration::from_millis(100), "{total:?}");
    assert_eq!(tx.retries(), 12);
    // future_version: FUTURE_VERSION_RETRY_DELAY, not the backoff.
    let waited = timed_on_error(&tx, codes::FUTURE_VERSION).await.unwrap();
    assert!(
        (Duration::from_millis(10)..=Duration::from_millis(11)).contains(&waited),
        "{waited:?}"
    );
    // MAX_RETRY_DELAY caps both (and persists across on_error). As in C++ it caps the
    // backoff's growth: the next wait is still a fraction of the backoff reached so far.
    tx.set_option(TransactionOption::MaxRetryDelay(Duration::from_millis(2)))
        .unwrap();
    let waited = timed_on_error(&tx, codes::NOT_COMMITTED).await.unwrap();
    assert!(waited <= Duration::from_millis(1001), "{waited:?}");
    for _ in 0..5 {
        let waited = timed_on_error(&tx, codes::NOT_COMMITTED).await.unwrap();
        assert!(waited <= Duration::from_millis(3), "{waited:?}");
        let waited = timed_on_error(&tx, codes::TRANSACTION_TOO_OLD)
            .await
            .unwrap();
        assert!(waited <= Duration::from_millis(3), "{waited:?}");
    }
    // A full reset starts the backoff (and the options) again.
    tx.reset();
    assert_eq!(tx.retries(), 0);
    let waited = timed_on_error(&tx, codes::NOT_COMMITTED).await.unwrap();
    assert!(waited <= Duration::from_millis(11), "{waited:?}");
    // `success` is a misuse.
    assert_eq!(
        tx.on_error(Error::new(0)).await.unwrap_err().code(),
        codes::CLIENT_INVALID_OPERATION
    );
}

#[tokio::test(start_paused = true)]
async fn the_retry_limit_and_errors_that_are_not_retried_cancel_the_transaction() {
    let env = env(SchemaVersion::V7_4).await;
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::RetryLimit(2)).unwrap();
    timed_on_error(&tx, codes::NOT_COMMITTED).await.unwrap();
    timed_on_error(&tx, codes::COMMIT_UNKNOWN_RESULT)
        .await
        .unwrap();
    let e = tx
        .on_error(Error::new(codes::NOT_COMMITTED))
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::NOT_COMMITTED, "the error itself");
    assert_eq!(
        tx.get_read_version().await.unwrap_err().code(),
        codes::TRANSACTION_CANCELLED
    );
    assert_eq!(
        tx.on_error(Error::new(codes::NOT_COMMITTED))
            .await
            .unwrap_err()
            .code(),
        codes::TRANSACTION_CANCELLED
    );
    tx.reset();
    timed_on_error(&tx, codes::NOT_COMMITTED).await.unwrap();

    // Not retried: returned as is, and the transaction is cancelled.
    let e = tx
        .on_error(Error::new(codes::KEY_TOO_LARGE))
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::KEY_TOO_LARGE);
    tx.set(b"k", b"v");
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::TRANSACTION_CANCELLED
    );
    // A plain `connection_failed` is not retried; a commit that was not delivered is.
    tx.reset();
    assert!(
        tx.on_error(Error::new(codes::CONNECTION_FAILED))
            .await
            .is_err()
    );
    tx.reset();
    let not_sent =
        Error::from_commit_request(fdb_native_transport::rpc::RequestError::NotSent(None));
    tx.on_error(not_sent).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_timeout_fails_everything_until_a_reset() {
    let mut env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::Timeout(Duration::from_millis(100)))
        .unwrap();
    let start = Instant::now();
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.get_read_version().await }
    });
    let _unanswered = env.grv.next().await;
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TIMED_OUT);
    assert!(!e.is_maybe_committed());
    let elapsed = start.elapsed();
    assert!(
        (Duration::from_millis(100)..Duration::from_millis(102)).contains(&elapsed),
        "{elapsed:?}"
    );
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::TRANSACTION_TIMED_OUT
    );
    assert_eq!(
        tx.on_error(Error::new(codes::NOT_COMMITTED))
            .await
            .unwrap_err()
            .code(),
        codes::TRANSACTION_TIMED_OUT
    );
    tx.reset();
    assert_eq!(tx.commit().await.unwrap().committed_version(), -1);

    // The timeout counts from the creation, not from on_error, and interrupts its wait.
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::Timeout(Duration::from_millis(50)))
        .unwrap();
    tx.set_option(TransactionOption::MaxRetryDelay(Duration::from_secs(10)))
        .unwrap();
    let mut timed_out = None;
    for _ in 0..20 {
        if let Err(e) = tx.on_error(Error::new(codes::NOT_COMMITTED)).await {
            timed_out = Some(e);
            break;
        }
    }
    assert_eq!(timed_out.unwrap().code(), codes::TRANSACTION_TIMED_OUT);
}

#[tokio::test(start_paused = true)]
async fn a_commit_interrupted_after_it_was_sent_may_have_committed() {
    let mut env = env(SchemaVersion::V7_4).await;
    // Timed out.
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::Timeout(Duration::from_secs(1)))
        .unwrap();
    tx.set(b"k", b"v");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    env.grv.next().await.reply_grv(5);
    let _unanswered = env.commit.next().await;
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TIMED_OUT);
    assert!(e.is_maybe_committed(), "{e}");
    assert!(!e.is_retryable());
    assert!(e.to_string().contains("may have committed"), "{e}");

    // Cancelled.
    let tx = env.db.create_transaction();
    tx.set(b"k", b"v");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    env.grv.next().await.reply_grv(5);
    let _unanswered = env.commit.next().await;
    tx.cancel();
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_CANCELLED);
    assert!(e.is_maybe_committed());

    // Cancelled while it waited for its read version: nothing was sent.
    let tx = env.db.create_transaction();
    tx.set(b"k", b"v");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    let _unanswered = env.grv.next().await;
    tx.cancel();
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_CANCELLED);
    assert!(!e.is_maybe_committed());
}

#[tokio::test(start_paused = true)]
async fn concurrent_reads_of_the_read_version_share_one_request() {
    let mut env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let handles: Vec<_> = (0..5)
        .map(|_| {
            let tx = tx.clone();
            spawn(async move { tx.get_read_version().await })
        })
        .collect();
    env.grv.next().await.reply_grv(42);
    for handle in handles {
        assert_eq!(done(handle).await.unwrap(), 42);
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(env.grv.try_next().is_none(), "one request");
    assert_eq!(tx.get_read_version().await.unwrap(), 42);

    // An abandoned request lets the next reader ask again.
    let tx = env.db.create_transaction();
    let first = spawn({
        let tx = tx.clone();
        async move { tx.get_read_version().await }
    });
    let _unanswered = env.grv.next().await;
    first.abort();
    let _ = first.await;
    let second = spawn({
        let tx = tx.clone();
        async move { tx.get_read_version().await }
    });
    env.grv.next().await.reply_grv(43);
    assert_eq!(done(second).await.unwrap(), 43);
}

#[tokio::test(start_paused = true)]
async fn run_retries_conflicts_and_never_hides_a_maybe_committed_attempt() {
    let mut env = env(SchemaVersion::V8_0).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let handle = spawn({
        let db = env.db.clone();
        let attempts = attempts.clone();
        async move {
            db.run(|tx, maybe| {
                let attempts = attempts.clone();
                async move {
                    assert_eq!(maybe, MaybeCommitted(false));
                    attempts.fetch_add(1, Ordering::SeqCst);
                    tx.set(b"k", b"v");
                    Ok("done")
                }
            })
            .await
        }
    });
    // A conflict, then success.
    env.grv.next().await.reply_grv(5);
    env.commit
        .next()
        .await
        .reply_commit(Err(codes::NOT_COMMITTED));
    env.grv.next().await.reply_grv(6);
    let call = env.commit.next().await;
    assert_eq!(call.commit().0.read_snapshot, 6);
    call.reply_commit(committed(7, 0));
    assert_eq!(done(handle).await.unwrap(), "done");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    // A lost connection after the send: commit_unknown_result (no dummy transaction with
    // CAUSAL_WRITE_RISKY), retried with MaybeCommitted(true); the closure then gives up with
    // an error that is not retried, which is reported as maybe committed.
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handle = spawn({
        let db = env.db.clone();
        let seen = seen.clone();
        async move {
            db.run(|tx, maybe| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(maybe.0);
                    if maybe.0 {
                        return Err(Error::new(codes::KEY_TOO_LARGE));
                    }
                    tx.set_option(TransactionOption::CausalWriteRisky)?;
                    tx.set(b"k", b"v");
                    Ok(())
                }
            })
            .await
        }
    });
    env.grv.next().await.reply_grv(8);
    env.commit.next().await.close();
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::KEY_TOO_LARGE);
    assert!(e.is_maybe_committed(), "{e:?}");
    assert_eq!(*seen.lock().unwrap(), vec![false, true]);

    // An error that is not retried ends the loop at once.
    let calls = Arc::new(AtomicUsize::new(0));
    let e = env
        .db
        .run(|_tx, _| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(Error::new(codes::INVERTED_RANGE))
            }
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::INVERTED_RANGE);
    assert!(!e.is_maybe_committed());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_shut_down_database_fails_its_transactions() {
    let env = env(SchemaVersion::V7_4).await;
    let tx = env.db.create_transaction();
    env.db.shutdown().await;
    assert!(env.db.is_shut_down());
    assert_eq!(
        tx.get_read_version().await.unwrap_err().code(),
        codes::OPERATION_CANCELLED
    );
    tx.set(b"k", b"v");
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::OPERATION_CANCELLED
    );
}

#[tokio::test]
async fn open_errors() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.cluster");
    let e = Database::open(Some(&missing)).unwrap_err();
    assert_eq!(e.code(), codes::NO_CLUSTER_FILE_FOUND, "{e}");
    let bad = dir.path().join("bad.cluster");
    std::fs::write(&bad, "not a connection string").unwrap();
    assert_eq!(
        Database::open(Some(&bad)).unwrap_err().code(),
        codes::CONNECTION_STRING_INVALID
    );
    assert_eq!(
        Database::from_connection_string("nope").unwrap_err().code(),
        codes::CONNECTION_STRING_INVALID
    );
    #[cfg(not(feature = "tls"))]
    assert_eq!(
        Database::from_connection_string("a:b@127.0.0.1:4500:tls")
            .unwrap_err()
            .code(),
        codes::TLS_ERROR
    );
}
