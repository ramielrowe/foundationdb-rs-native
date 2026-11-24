//! `Database` and `Transaction` against scripted fake GRV and commit proxies on an in-memory
//! network under tokio's paused clock, with a real `ClusterMonitor` fed by a fake coordinator:
//! the read version a commit asks for, `on_error`'s backoff, retry limit and cancellation,
//! timeouts (also during a commit, which may have committed), cancellation, deferred errors,
//! `used_during_commit`, and the `run` retry loop. Reads from storage servers are covered by
//! the docker tests.

#![cfg(feature = "tokio")]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use fdb_native_client::cluster::ConnectionRecord;
use fdb_native_client::error::codes;
use fdb_native_client::{
    Database, DatabaseOptions, Error, MaybeCommitted, MutationType, Transaction, TransactionOption,
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
        tx.atomic_op(b"n", &1u64.to_le_bytes(), MutationType::Add);
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
        // From the write map, as C++'s `writeRangeToNativeTransaction` sends them: the
        // cleared ranges first, then each key's operations in key order.
        assert_eq!(kinds, vec![Mt::CLEAR_RANGE, Mt::SET_VALUE, Mt::ADD_VALUE]);
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
    tx.add_read_conflict_range(b"r", b"s").unwrap();
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

/// `fdb_transaction_add_conflict_range`: an inverted range is `inverted_range` at once (the
/// `KeyRangeRef` that `fdb_c` builds first), before the legal range is looked at, and leaves
/// the transaction as it was; a range out of the legal keys is a deferred error.
#[tokio::test(start_paused = true)]
async fn conflict_ranges_return_inverted_range_at_once_and_defer_the_rest() {
    let mut env = env(SchemaVersion::V8_0).await;
    for read in [true, false] {
        let add = |tx: &Transaction<MemRuntime>, b: &[u8], e: &[u8]| {
            if read {
                tx.add_read_conflict_range(b, e)
            } else {
                tx.add_write_conflict_range(b, e)
            }
        };
        let tx = env.db.create_transaction();
        for (b, e) in [(&b"b"[..], &b"a"[..]), (b"\xff\xff\x01", b"\xff\xff\x00")] {
            assert_eq!(
                add(&tx, b, e).unwrap_err().code(),
                codes::INVERTED_RANGE,
                "{read} {b:?}"
            );
        }
        // Not deferred: the transaction commits (nothing to write).
        assert_eq!(tx.commit().await.unwrap().committed_version(), -1);

        let tx = env.db.create_transaction();
        add(&tx, b"a", b"\xff\xff\x00").unwrap();
        assert_eq!(
            tx.commit().await.unwrap_err().code(),
            codes::KEY_OUTSIDE_LEGAL_RANGE,
            "{read}"
        );
    }
    assert!(env.commit.try_next().is_none(), "nothing was sent");

    // The single-key forms are `[key, key\x00)`.
    let tx = env.db.create_transaction();
    tx.add_read_conflict_key(b"r").unwrap();
    tx.add_write_conflict_key(b"w").unwrap();
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    env.grv.next().await.reply_grv(10);
    let call = env.commit.next().await;
    let (commit, _) = call.commit();
    assert!(
        commit
            .read_conflict_ranges
            .iter()
            .any(|r| r.begin == b"r" && r.end == b"r\x00"),
        "{:?}",
        commit.read_conflict_ranges
    );
    assert!(
        commit
            .write_conflict_ranges
            .iter()
            .any(|r| r.begin == b"w" && r.end == b"w\x00"),
        "{:?}",
        commit.write_conflict_ranges
    );
    call.reply_commit(committed(11, 0));
    done(handle).await.unwrap();
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

    // The request does not depend on the reader that started it: one that is dropped, or
    // parked without being polled, leaves it to the others.
    let tx = env.db.create_transaction();
    let first = spawn({
        let tx = tx.clone();
        async move { tx.get_read_version().await }
    });
    let request = env.grv.next().await;
    first.abort();
    let _ = first.await;
    let second = spawn({
        let tx = tx.clone();
        async move { tx.get_read_version().await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    request.reply_grv(43);
    assert_eq!(done(second).await.unwrap(), 43);
    assert!(env.grv.try_next().is_none(), "one request");

    // (Review finding f2.) A reader polled once and then parked.
    let tx = env.db.create_transaction();
    let mut parked = Box::pin(tx.get_read_version());
    assert!(
        tokio::time::timeout(Duration::from_millis(1), &mut parked)
            .await
            .is_err()
    );
    env.grv.next().await.reply_grv(44);
    let second = tokio::time::timeout(Duration::from_secs(30), tx.get_read_version()).await;
    assert_eq!(second.expect("not stranded").unwrap(), 44);
    drop(parked);

    // The write-only commit's read version too.
    let tx = env.db.create_transaction();
    tx.set(b"k", b"v");
    let mut parked = Box::pin(tx.get_read_version());
    assert!(
        tokio::time::timeout(Duration::from_millis(1), &mut parked)
            .await
            .is_err()
    );
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    env.grv.next().await.reply_grv(45);
    let call = env.commit.next().await;
    assert_eq!(call.commit().0.read_snapshot, 45);
    call.reply_commit(committed(46, 0));
    assert_eq!(done(handle).await.unwrap().committed_version(), 46);
    drop(parked);
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

// ---- review regressions (let.6) -------------------------------------------------------------

/// (f1) `run` with a commit that times out after it was sent: the error may hide a commit.
#[tokio::test(start_paused = true)]
async fn run_with_a_timeout_during_the_commit_is_maybe_committed() {
    let mut env = env(SchemaVersion::V7_4).await;
    let handle = spawn({
        let db = env.db.clone();
        async move {
            db.run(|tx, _| async move {
                tx.set_option(TransactionOption::Timeout(Duration::from_secs(1)))?;
                tx.set(b"k", b"v");
                Ok(())
            })
            .await
        }
    });
    env.grv.next().await.reply_grv(5);
    let _unanswered = env.commit.next().await;
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TIMED_OUT);
    assert!(e.is_maybe_committed(), "{e:?}");
}

/// (f1b) `commit_unknown_result`, then the timeout ends the backoff: still maybe committed,
/// from `on_error` itself and from `run`.
#[tokio::test(start_paused = true)]
async fn a_timeout_during_the_backoff_after_an_unknown_result_is_maybe_committed() {
    let mut env = env(SchemaVersion::V7_4).await;
    let handle = spawn({
        let db = env.db.clone();
        async move {
            db.run(|tx, _| async move {
                tx.set_option(TransactionOption::Timeout(Duration::from_millis(500)))?;
                tx.set_option(TransactionOption::CausalWriteRisky)?;
                tx.set(b"k", b"v");
                Ok(())
            })
            .await
        }
    });
    env.grv.next().await.reply_grv(5);
    let call = env.commit.next().await;
    tokio::time::sleep(Duration::from_millis(499)).await;
    call.reply_commit(Err(codes::COMMIT_UNKNOWN_RESULT));
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TIMED_OUT, "{e:?}");
    assert!(e.is_maybe_committed(), "{e:?}");

    // `on_error` alone: a long backoff interrupted by the timeout.
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::Timeout(Duration::from_millis(30)))
        .unwrap();
    let mut last = None;
    for _ in 0..20 {
        if let Err(e) = tx.on_error(Error::new(codes::COMMIT_UNKNOWN_RESULT)).await {
            last = Some(e);
            break;
        }
    }
    let e = last.expect("timed out");
    assert_eq!(e.code(), codes::TRANSACTION_TIMED_OUT);
    assert!(e.is_maybe_committed());
    // A dead transaction given a maybe-committed error says so too.
    let e = tx
        .on_error(Error::new(codes::COMMIT_UNKNOWN_RESULT))
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TIMED_OUT);
    assert!(e.is_maybe_committed());
    // An error that is not maybe committed is not marked.
    let e = tx
        .on_error(Error::new(codes::NOT_COMMITTED))
        .await
        .unwrap_err();
    assert!(!e.is_maybe_committed());
}

/// (f3) A timeout set while an operation is in progress applies to it.
#[tokio::test(start_paused = true)]
async fn a_timeout_set_during_an_operation_interrupts_it() {
    let mut env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let start = Instant::now();
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.get_read_version().await }
    });
    let _unanswered = env.grv.next().await;
    tx.set_option(TransactionOption::Timeout(Duration::from_millis(100)))
        .unwrap();
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TIMED_OUT);
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_millis(102), "{elapsed:?}");

    // A deadline that has already passed fails it at once; a longer one is still waited
    // for.
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::Timeout(Duration::from_secs(100)))
        .unwrap();
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.get_read_version().await }
    });
    let _unanswered = env.grv.next().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx.set_option(TransactionOption::Timeout(Duration::from_secs(200)))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!handle.is_finished());
    let before = Instant::now();
    tx.set_option(TransactionOption::Timeout(Duration::from_millis(10)))
        .unwrap();
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TIMED_OUT);
    assert!(before.elapsed() < Duration::from_millis(2));
}

/// A deadline that passed while nothing ran has killed the transaction (C++'s `timebomb`
/// fires on its own): unsetting or lengthening the timeout afterwards does not bring it
/// back, and a `cancel` does not replace the error (the bindingtester's `test_timeouts`
/// case 10).
#[tokio::test(start_paused = true)]
async fn a_timeout_that_fired_while_idle_cannot_be_unset() {
    let env = env(SchemaVersion::V7_4).await;
    for change in [
        Some(Duration::ZERO),
        Some(Duration::from_secs(1000)),
        None, // cancel
    ] {
        let tx = env.db.create_transaction();
        tx.set(b"foo", b"bar");
        tx.set_option(TransactionOption::Timeout(Duration::from_millis(100)))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        match change {
            Some(timeout) => tx.set_option(TransactionOption::Timeout(timeout)).unwrap(),
            None => tx.cancel(),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        // The fake proxy never answers: a commit that is sent does not end.
        let e = tokio::time::timeout(Duration::from_secs(10), tx.commit())
            .await
            .expect("the commit fails at once, without a request")
            .unwrap_err();
        assert_eq!(e.code(), codes::TRANSACTION_TIMED_OUT, "{change:?}");
        assert!(!e.is_maybe_committed());
        assert_eq!(
            tx.get_read_version().await.unwrap_err().code(),
            codes::TRANSACTION_TIMED_OUT
        );
        // A reset starts over.
        tx.reset();
        assert_eq!(tx.commit().await.unwrap().committed_version(), -1);
    }

    // Unset before the deadline, the timeout is gone (case 9).
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::Timeout(Duration::from_millis(100)))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    tx.set_option(TransactionOption::Timeout(Duration::ZERO))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(tx.commit().await.unwrap().committed_version(), -1);
}

/// (f4) Shutting the database down ends the operations in progress; a commit that was
/// sent may have committed.
#[tokio::test(start_paused = true)]
async fn shutdown_ends_the_operations_in_progress() {
    let mut env = env(SchemaVersion::V7_4).await;
    let tx = env.db.create_transaction();
    let grv = spawn({
        let tx = tx.clone();
        async move { tx.get_read_version().await }
    });
    let _g = env.grv.next().await;
    let tx2 = env.db.create_transaction();
    tx2.set_read_version(9);
    tx2.set(b"k", b"v");
    let commit = spawn({
        let tx2 = tx2.clone();
        async move { tx2.commit().await }
    });
    let _c = env.commit.next().await;
    env.db.shutdown().await;
    let e = done(grv).await.unwrap_err();
    assert_eq!(e.code(), codes::OPERATION_CANCELLED, "{e:?}");
    let e = done(commit).await.unwrap_err();
    assert!(e.is_maybe_committed(), "{e:?}");
}

/// (f5) A closure that commits itself: `run`'s own commit is `used_during_commit`, which may
/// not be mistaken for a commit that did not happen.
#[tokio::test(start_paused = true)]
async fn a_second_commit_after_a_sent_one_is_maybe_committed() {
    let mut env = env(SchemaVersion::V8_0).await;
    let handle = spawn({
        let db = env.db.clone();
        async move {
            db.run(|tx, _| async move {
                tx.set_read_version(3);
                tx.set(b"k", b"v");
                let result = tx.commit().await?;
                assert_eq!(tx.committed_version(), result.committed_version());
                assert_eq!(tx.versionstamp(), result.versionstamp());
                Ok(())
            })
            .await
        }
    });
    env.commit.next().await.reply_commit(committed(10, 2));
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    assert!(e.is_maybe_committed(), "{e:?}");

    // The accessors.
    let tx = env.db.create_transaction();
    assert_eq!(tx.committed_version(), -1);
    assert_eq!(tx.versionstamp(), None);
    tx.set_read_version(3);
    tx.set(b"k", b"v");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    env.commit.next().await.reply_commit(committed(11, 1));
    done(handle).await.unwrap();
    assert_eq!(tx.committed_version(), 11);
    assert_eq!(tx.versionstamp(), Some([0, 0, 0, 0, 0, 0, 0, 11, 0, 1]));
    // After a successful commit, any use is `used_during_commit` (maybe committed: it did).
    let e = tx.get(b"k").await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    assert!(e.is_maybe_committed());
    tx.reset();
    assert_eq!(tx.committed_version(), -1);

    // A commit that failed before anything was sent: a second commit is not maybe
    // committed.
    let tx = env.db.create_transaction();
    tx.set(&[b'k'; 20_000], b"v");
    assert_eq!(tx.commit().await.unwrap_err().code(), codes::KEY_TOO_LARGE);
    let e = tx.commit().await.unwrap_err();
    assert_eq!(
        e.code(),
        codes::KEY_TOO_LARGE,
        "the deferred error comes first"
    );
}

/// A read started during a commit is `used_during_commit`; it interrupts the commit, which
/// may have been sent.
#[tokio::test(start_paused = true)]
async fn a_read_during_the_commit_is_used_during_commit() {
    let mut env = env(SchemaVersion::V7_4).await;
    let tx = env.db.create_transaction();
    tx.set_read_version(5);
    tx.set(b"k", b"v");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    let _unanswered = env.commit.next().await;
    let e = tx.get(b"k").await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    assert!(e.is_maybe_committed());
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    assert!(e.is_maybe_committed());
    assert_eq!(
        tx.get_read_version().await.unwrap_err().code(),
        codes::USED_DURING_COMMIT
    );

    // Before the request may have left (waiting for its read version), it is not.
    let tx = env.db.create_transaction();
    tx.set(b"k", b"v");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    let _unanswered = env.grv.next().await;
    let e = tx.get_read_version().await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    assert!(!e.is_maybe_committed());
    assert!(!done(handle).await.unwrap_err().is_maybe_committed());
}

#[tokio::test(start_paused = true)]
async fn the_resource_constrained_backoff_grows_to_30_seconds() {
    let env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let mut cap = Duration::from_millis(10);
    let mut longest = Duration::ZERO;
    for i in 0..20 {
        let waited = timed_on_error(&tx, codes::COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED)
            .await
            .unwrap();
        assert!(
            waited <= cap + Duration::from_millis(1),
            "retry {i}: {waited:?} > {cap:?}"
        );
        longest = longest.max(waited);
        cap = (cap * 2).min(Duration::from_secs(30));
    }
    assert!(
        longest > Duration::from_secs(1),
        "past MAX_RETRY_DELAY: {longest:?}"
    );
    // The next ordinary error caps the backoff back at the max retry delay, after one more
    // wait of up to the backoff reached.
    timed_on_error(&tx, codes::NOT_COMMITTED).await.unwrap();
    for _ in 0..5 {
        let waited = timed_on_error(&tx, codes::NOT_COMMITTED).await.unwrap();
        assert!(waited <= Duration::from_millis(1001), "{waited:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn the_system_key_boundaries() {
    let mut env = env(SchemaVersion::V7_4).await;
    let legality = |tx: &Transaction<MemRuntime>| {
        let tx = tx.clone();
        async move {
            match tx.commit().await {
                Err(e) if e.code() == codes::KEY_OUTSIDE_LEGAL_RANGE => Err(e),
                _ => Ok(()),
            }
        }
    };
    // ReadSystemKeys: reads (and read conflict ranges) up to `\xff\xff`, writes below `\xff`.
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::ReadSystemKeys).unwrap();
    assert_eq!(
        tx.get(b"\xff\xff").await.unwrap_err().code(),
        codes::KEY_OUTSIDE_LEGAL_RANGE
    );
    let read = spawn({
        let tx = tx.clone();
        async move { tx.get(b"\xff\xfe").await }
    });
    let _unanswered = env.grv.next().await;
    read.abort();
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::ReadSystemKeys).unwrap();
    tx.add_read_conflict_range(b"a", b"\xff\xff").unwrap();
    tx.set_read_version(1);
    tx.set(b"k", b"v");
    let committing = spawn(legality(&tx));
    env.commit.next().await.reply_commit(committed(2, 0));
    assert!(done(committing).await.is_ok());
    for write in [
        |tx: &Transaction<MemRuntime>| tx.set(b"\xff", b""),
        |tx: &Transaction<MemRuntime>| tx.clear_range(b"a", b"\xff\x00"),
        |tx: &Transaction<MemRuntime>| tx.add_write_conflict_range(b"a", b"\xff\x00").unwrap(),
    ] {
        let tx = env.db.create_transaction();
        tx.set_option(TransactionOption::ReadSystemKeys).unwrap();
        write(&tx);
        assert!(legality(&tx).await.is_err());
    }
    // AccessSystemKeys: writes up to `\xff\xff` (a range may end there), not past it.
    for (write, legal) in [
        (
            (|tx: &Transaction<MemRuntime>| tx.set(b"\xff\xfe", b"")) as fn(&_),
            true,
        ),
        (
            |tx: &Transaction<MemRuntime>| tx.clear_range(b"\xff", b"\xff\xff"),
            true,
        ),
        (
            |tx: &Transaction<MemRuntime>| tx.set(b"\xff\xff", b""),
            false,
        ),
        (|tx: &Transaction<MemRuntime>| tx.clear(b"\xff\xff"), false),
        (
            |tx: &Transaction<MemRuntime>| tx.clear_range(b"\xff", b"\xff\xff\x00"),
            false,
        ),
        (
            |tx: &Transaction<MemRuntime>| {
                tx.add_read_conflict_range(b"\xff", b"\xff\xff\x00")
                    .unwrap()
            },
            false,
        ),
    ] {
        let tx = env.db.create_transaction();
        tx.set_option(TransactionOption::AccessSystemKeys).unwrap();
        tx.set_read_version(1);
        write(&tx);
        let committing = spawn(legality(&tx));
        if legal {
            env.commit.next().await.reply_commit(committed(3, 0));
        }
        assert_eq!(done(committing).await.is_ok(), legal);
    }
}

#[tokio::test(start_paused = true)]
async fn the_size_limit_is_not_persistent() {
    let mut env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::SizeLimit(100)).unwrap();
    tx.set_read_version(1);
    tx.set(b"k", &[0; 200]);
    let e = tx.commit().await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TOO_LARGE);
    // TRANSACTION_TOO_LARGE is not retried; a conflict resets the options.
    tx.reset();
    tx.set_option(TransactionOption::SizeLimit(100)).unwrap();
    tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    tx.set_read_version(1);
    tx.set(b"k", &[0; 200]);
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    env.commit.next().await.reply_commit(committed(2, 0));
    done(handle).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn the_deferred_error_comes_before_the_cancellation() {
    let env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    tx.set(&[b'k'; 20_000], b"v");
    tx.cancel();
    assert_eq!(tx.get(b"k").await.unwrap_err().code(), codes::KEY_TOO_LARGE);
    assert_eq!(
        tx.get_read_version().await.unwrap_err().code(),
        codes::KEY_TOO_LARGE
    );
    // The commit too (`ThreadSafeTransaction::commit` checks the deferred error first): the
    // bindingtester's `api` test, seed 1004 at 2000 operations on 8.0, sets an invalid read
    // version (`version_invalid`, deferred), cancels, and commits.
    assert_eq!(tx.commit().await.unwrap_err().code(), codes::KEY_TOO_LARGE);
    let tx = env.db.create_transaction();
    tx.set_read_version(-1);
    tx.cancel();
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::VERSION_INVALID
    );
    assert_eq!(
        tx.get_versionstamp().await.unwrap_err().code(),
        codes::VERSION_INVALID
    );
    // A write after the cancellation is still checked (C++'s writes do not look at
    // `resetPromise`), and its error is deferred: the bindingtester's `api` test, seed 1059
    // at 2000 operations on 8.0 (CANCEL, SET of a value too large, GET).
    let tx = env.db.create_transaction();
    tx.cancel();
    tx.set(b"k", &[0; 200_000]);
    assert_eq!(
        tx.get(b"k").await.unwrap_err().code(),
        codes::VALUE_TOO_LARGE
    );
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::VALUE_TOO_LARGE
    );
    // And applied: only the approximate size sees it.
    let tx = env.db.create_transaction();
    tx.cancel();
    let before = tx.approximate_size().unwrap();
    tx.set(b"k", b"v");
    tx.clear_range(b"a", b"b");
    tx.atomic_op(b"n", &1u64.to_le_bytes(), MutationType::Add);
    assert!(tx.approximate_size().unwrap() > before);
    assert_eq!(
        tx.get(b"k").await.unwrap_err().code(),
        codes::TRANSACTION_CANCELLED
    );
    tx.reset();
    assert_eq!(tx.approximate_size().unwrap(), 0);
    // A timeout that has passed does not hide it either.
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::Timeout(Duration::from_millis(10)))
        .unwrap();
    tx.set_read_version(0);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::VERSION_INVALID
    );
    assert_eq!(
        tx.get(b"k").await.unwrap_err().code(),
        codes::VERSION_INVALID
    );
}

/// A handle of an earlier `run` attempt cannot reach the attempt in progress.
#[tokio::test(start_paused = true)]
async fn a_stale_run_handle_cannot_touch_the_next_attempt() {
    let mut env = env(SchemaVersion::V7_4).await;
    let stale = Arc::new(std::sync::Mutex::new(None::<Transaction<MemRuntime>>));
    let attempts = Arc::new(AtomicUsize::new(0));
    let handle = spawn({
        let db = env.db.clone();
        let (stale, attempts) = (stale.clone(), attempts.clone());
        async move {
            db.run(|tx, _| {
                let (stale, attempts) = (stale.clone(), attempts.clone());
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    tx.set_read_version(5);
                    tx.set(b"k", b"v");
                    match attempt {
                        0 => *stale.lock().unwrap() = Some(tx),
                        1 => {
                            let old = stale.lock().unwrap().clone().unwrap();
                            assert_eq!(
                                old.get(b"x").await.unwrap_err().code(),
                                codes::TRANSACTION_CANCELLED
                            );
                            assert!(old.set_option(TransactionOption::LockAware).is_err());
                            old.cancel();
                            old.reset();
                            // Not cancelled or reset by the stale handle: this attempt goes
                            // on, until the stale write.
                            assert_eq!(tx.get_read_version().await.unwrap(), 5);
                            old.set(b"leak", b"!");
                        }
                        _ => unreachable!(),
                    }
                    Ok(())
                }
            })
            .await
        }
    });
    env.commit
        .next()
        .await
        .reply_commit(Err(codes::NOT_COMMITTED));
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_CANCELLED, "{e:?}");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        env.commit.try_next().is_none(),
        "the second attempt sent nothing"
    );
}
