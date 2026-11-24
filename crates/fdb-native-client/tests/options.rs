//! Transaction and database options against scripted fake GRV and commit proxies on an
//! in-memory network under tokio's paused clock: what each option puts in the requests, which
//! options survive `on_error` and `reset`, the database's defaults, the validation of option
//! values, and the `\xff/metadataVersion` read path and cache. The docker tests check the
//! options against real clusters.

#![cfg(feature = "tokio")]

mod support;

use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::{
    ConnectionRecord, Database, DatabaseOption, DatabaseOptions, Error, MutationType, Transaction,
    TransactionOption,
};
use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::{Commit, CommitId, GetReadVersion, KeyRange};
use fdb_native_transport::peer::PeerManagerConfig;
use fdb_native_wire::ProtocolVersion;
use support::proxy::FakeProxy;
use support::{FakeCoordinator, MemRuntime, Net, info, sock};

const SCHEMAS: [SchemaVersion; 2] = [SchemaVersion::V7_4, SchemaVersion::V8_0];
const METADATA_VERSION: &[u8] = b"\xff/metadataVersion";

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

/// The `GetReadVersion` the transaction's read version request is (the request is answered
/// with version 10).
async fn read_version_request(env: &mut Env, tx: &Transaction<MemRuntime>) -> GetReadVersion {
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.get_read_version().await }
    });
    let call = env.grv.next().await;
    let (request, _) = call.grv();
    call.reply_grv(10);
    assert_eq!(done(handle).await.unwrap(), 10);
    request
}

/// Commits `tx` (which has a read version and a write) and returns the request.
async fn commit_request(env: &mut Env, tx: &Transaction<MemRuntime>) -> Commit {
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    let call = env.commit.next().await;
    let (commit, _) = call.commit();
    call.reply_commit(committed(20, 0));
    done(handle).await.unwrap();
    commit
}

fn priority(request: &GetReadVersion) -> u32 {
    request.flags & GetReadVersion::FLAG_PRIORITY_MASK
}

fn opt(tx: &Transaction<MemRuntime>, option: TransactionOption) {
    tx.set_option(option).expect("option");
}

#[test]
fn option_codes_and_persistence_are_those_of_fdb_options() {
    use TransactionOption::*;
    let table: Vec<(TransactionOption, i32, bool)> = vec![
        (CausalWriteRisky, 10, false),
        (CausalReadRisky, 20, false),
        (CausalReadDisable, 21, false),
        (IncludePortInAddress, 23, false),
        (NextWriteNoWriteConflictRange, 30, false),
        (CommitOnFirstProxy, 40, false),
        (ReadYourWritesDisable, 51, false),
        (PrioritySystemImmediate, 200, false),
        (PriorityBatch, 201, false),
        (InitializeNewDatabase, 300, false),
        (AccessSystemKeys, 301, false),
        (ReadSystemKeys, 302, false),
        (RawAccess, 303, false),
        (BypassStorageQuota, 304, false),
        (DebugRetryLogging(None), 401, false),
        (DebugTransactionIdentifier("a".into()), 403, false),
        (LogTransaction, 404, false),
        (TransactionLoggingMaxFieldLength(1), 405, false),
        (ServerRequestTracing, 406, false),
        (Timeout(Duration::ZERO), 500, true),
        (RetryLimit(0), 501, true),
        (MaxRetryDelay(Duration::ZERO), 502, true),
        (SizeLimit(100), 503, false),
        (ReadServerSideCacheEnable, 507, false),
        (ReadServerSideCacheDisable, 508, false),
        (ReadPriorityNormal, 509, false),
        (ReadPriorityLow, 510, false),
        (ReadPriorityHigh, 511, false),
        (SnapshotRywEnable, 600, false),
        (SnapshotRywDisable, 601, false),
        (LockAware, 700, false),
        (UsedDuringCommitProtectionDisable, 701, false),
        (ReadLockAware, 702, false),
        (FirstInBatch, 710, false),
        (UseProvisionalProxies, 711, false),
        (ReportConflictingKeys, 712, false),
        (Tag("t".into()), 800, false),
        (AutoThrottleTag("t".into()), 801, false),
        (SpanParent(vec![0; 33]), 900, false),
        (ExpensiveClearCostEstimationEnable, 1000, false),
        (BypassUnreadable, 1100, false),
        (UseGrvCache, 1101, false),
        (MaxGrvQueueDelay(Duration::ZERO), 1103, false),
    ];
    for (option, code, persistent) in table {
        assert_eq!(option.code(), code, "{option:?}");
        assert_eq!(option.is_persistent(), persistent, "{option:?}");
    }
    let database: Vec<(DatabaseOption, i32, Option<TransactionOption>)> = vec![
        (DatabaseOption::MaxWatches(1), 20, None),
        (DatabaseOption::SnapshotRywEnable, 26, None),
        (DatabaseOption::SnapshotRywDisable, 27, None),
        (
            DatabaseOption::TransactionTimeout(Duration::from_secs(1)),
            500,
            Some(Timeout(Duration::from_secs(1))),
        ),
        (
            DatabaseOption::TransactionRetryLimit(3),
            501,
            Some(RetryLimit(3)),
        ),
        (
            DatabaseOption::TransactionMaxRetryDelay(Duration::from_secs(2)),
            502,
            Some(MaxRetryDelay(Duration::from_secs(2))),
        ),
        (
            DatabaseOption::TransactionSizeLimit(64),
            503,
            Some(SizeLimit(64)),
        ),
        (
            DatabaseOption::TransactionCausalReadRisky,
            504,
            Some(CausalReadRisky),
        ),
        (
            DatabaseOption::TransactionIncludePortInAddress,
            505,
            Some(IncludePortInAddress),
        ),
        (
            DatabaseOption::TransactionBypassUnreadable,
            700,
            Some(BypassUnreadable),
        ),
        (
            DatabaseOption::TransactionUsedDuringCommitProtectionDisable,
            701,
            Some(UsedDuringCommitProtectionDisable),
        ),
        (
            DatabaseOption::TransactionReportConflictingKeys,
            702,
            Some(ReportConflictingKeys),
        ),
    ];
    for (option, code, default_for) in database {
        assert_eq!(option.code(), code, "{option:?}");
        assert_eq!(option.default_for(), default_for, "{option:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn priorities_and_max_grv_queue_delay_reach_the_read_version_request() {
    for schema in SCHEMAS {
        let mut env = env(schema).await;
        let tx = env.db.create_transaction();
        let request = read_version_request(&mut env, &tx).await;
        assert_eq!(priority(&request), GetReadVersion::PRIORITY_DEFAULT);
        assert_eq!(request.max_grv_queue_delay_ms, None);

        let cases = [
            (
                vec![TransactionOption::PriorityBatch],
                GetReadVersion::PRIORITY_BATCH,
            ),
            (
                vec![TransactionOption::PrioritySystemImmediate],
                GetReadVersion::PRIORITY_SYSTEM_IMMEDIATE,
            ),
            // The last one wins.
            (
                vec![
                    TransactionOption::PriorityBatch,
                    TransactionOption::PrioritySystemImmediate,
                ],
                GetReadVersion::PRIORITY_SYSTEM_IMMEDIATE,
            ),
            (
                vec![
                    TransactionOption::PrioritySystemImmediate,
                    TransactionOption::PriorityBatch,
                ],
                GetReadVersion::PRIORITY_BATCH,
            ),
        ];
        for (options, expected) in cases {
            let tx = env.db.create_transaction();
            for option in options.clone() {
                opt(&tx, option);
            }
            let request = read_version_request(&mut env, &tx).await;
            assert_eq!(priority(&request), expected, "{options:?} {schema:?}");
        }

        // Not persistent: `on_error` forgets it.
        let tx = env.db.create_transaction();
        opt(&tx, TransactionOption::PriorityBatch);
        tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
        let request = read_version_request(&mut env, &tx).await;
        assert_eq!(priority(&request), GetReadVersion::PRIORITY_DEFAULT);

        // The read version of a blind write's commit has the priority too.
        let tx = env.db.create_transaction();
        opt(&tx, TransactionOption::PriorityBatch);
        tx.set(b"k", b"v");
        let handle = spawn({
            let tx = tx.clone();
            async move { tx.commit().await }
        });
        let call = env.grv.next().await;
        let (request, _) = call.grv();
        assert_eq!(priority(&request), GetReadVersion::PRIORITY_BATCH);
        assert_ne!(request.flags & GetReadVersion::FLAG_CAUSAL_READ_RISKY, 0);
        call.reply_grv(10);
        env.commit.next().await.reply_commit(committed(11, 0));
        done(handle).await.unwrap();

        // MAX_GRV_QUEUE_DELAY: 8.0 only (a 7.4 GRV proxy has no such field).
        let tx = env.db.create_transaction();
        opt(
            &tx,
            TransactionOption::MaxGrvQueueDelay(Duration::from_millis(250)),
        );
        let request = read_version_request(&mut env, &tx).await;
        let expected = (schema == SchemaVersion::V8_0).then_some(250);
        assert_eq!(request.max_grv_queue_delay_ms, expected, "{schema:?}");
        assert_eq!(
            tx.set_option(TransactionOption::MaxGrvQueueDelay(Duration::from_secs(
                1 << 40
            )))
            .unwrap_err()
            .code(),
            codes::INVALID_OPTION_VALUE
        );
        // Not persistent either.
        tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
        let request = read_version_request(&mut env, &tx).await;
        assert_eq!(request.max_grv_queue_delay_ms, None);
    }
}

#[tokio::test(start_paused = true)]
async fn lock_aware_and_read_lock_aware() {
    for schema in SCHEMAS {
        let mut env = env(schema).await;
        // A locked database refuses a transaction that is not lock aware.
        let tx = env.db.create_transaction();
        let handle = spawn({
            let tx = tx.clone();
            async move { tx.get_read_version().await }
        });
        env.grv.next().await.reply_grv_with(10, true, None);
        assert_eq!(
            done(handle).await.unwrap_err().code(),
            codes::DATABASE_LOCKED
        );

        // READ_LOCK_AWARE reads, but is read-only: a commit with writes is refused (and
        // sends nothing).
        let tx = env.db.create_transaction();
        opt(&tx, TransactionOption::ReadLockAware);
        let handle = spawn({
            let tx = tx.clone();
            async move { tx.get_read_version().await }
        });
        env.grv.next().await.reply_grv_with(10, true, None);
        assert_eq!(done(handle).await.unwrap(), 10);
        tx.set(b"k", b"v");
        assert_eq!(
            tx.commit().await.unwrap_err().code(),
            codes::TRANSACTION_READ_ONLY
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(env.commit.try_next().is_none());
        // A read-only transaction with nothing to write commits.
        tx.reset();
        opt(&tx, TransactionOption::ReadLockAware);
        assert_eq!(tx.commit().await.unwrap().committed_version(), -1);

        // With LOCK_AWARE, in either order, the transaction writes (and says it is lock
        // aware).
        for options in [
            [
                TransactionOption::ReadLockAware,
                TransactionOption::LockAware,
            ],
            [
                TransactionOption::LockAware,
                TransactionOption::ReadLockAware,
            ],
        ] {
            let tx = env.db.create_transaction();
            for option in options.clone() {
                opt(&tx, option);
            }
            let handle = spawn({
                let tx = tx.clone();
                async move { tx.get_read_version().await }
            });
            env.grv.next().await.reply_grv_with(10, true, None);
            assert_eq!(done(handle).await.unwrap(), 10);
            tx.set(b"k", b"v");
            let commit = commit_request(&mut env, &tx).await;
            assert_eq!(commit.flags & Commit::FLAG_IS_LOCK_AWARE, 1, "{options:?}");
        }

        // `on_error` forgets both.
        let tx = env.db.create_transaction();
        opt(&tx, TransactionOption::ReadLockAware);
        tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
        tx.set_read_version(5);
        tx.set(b"k", b"v");
        let commit = commit_request(&mut env, &tx).await;
        assert_eq!(commit.flags & Commit::FLAG_IS_LOCK_AWARE, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn commit_options_reach_the_commit_request() {
    for schema in SCHEMAS {
        let mut env = env(schema).await;
        let tx = env.db.create_transaction();
        tx.set_read_version(5);
        opt(&tx, TransactionOption::FirstInBatch);
        opt(&tx, TransactionOption::BypassStorageQuota);
        opt(&tx, TransactionOption::CommitOnFirstProxy);
        opt(&tx, TransactionOption::ReportConflictingKeys);
        tx.set(b"k", b"v");
        let commit = commit_request(&mut env, &tx).await;
        assert_eq!(
            commit.flags,
            Commit::FLAG_FIRST_IN_BATCH | Commit::FLAG_BYPASS_STORAGE_QUOTA,
            "{schema:?}"
        );
        assert!(commit.report_conflicting_keys);

        // None of them is persistent.
        tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
        tx.set_read_version(5);
        tx.set(b"k", b"v");
        let commit = commit_request(&mut env, &tx).await;
        assert_eq!(commit.flags, 0);
        assert!(!commit.report_conflicting_keys);

        // CAUSAL_READ_RISKY reaches the read version request.
        let tx = env.db.create_transaction();
        opt(&tx, TransactionOption::CausalReadRisky);
        let handle = spawn({
            let tx = tx.clone();
            async move { tx.get_read_version().await }
        });
        let call = env.grv.next().await;
        let (request, _) = call.grv();
        assert_eq!(
            request.flags & !GetReadVersion::FLAG_PRIORITY_MASK,
            GetReadVersion::FLAG_CAUSAL_READ_RISKY
        );
        call.reply_grv(10);
        done(handle).await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn only_the_persistent_options_survive_on_error_and_a_reset_clears_them_too() {
    let mut env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    opt(&tx, TransactionOption::Timeout(Duration::from_secs(10)));
    opt(&tx, TransactionOption::RetryLimit(2));
    opt(
        &tx,
        TransactionOption::MaxRetryDelay(Duration::from_millis(5)),
    );
    opt(&tx, TransactionOption::SizeLimit(100));
    opt(&tx, TransactionOption::CausalWriteRisky);
    opt(&tx, TransactionOption::AccessSystemKeys);
    opt(&tx, TransactionOption::PriorityBatch);
    opt(&tx, TransactionOption::UsedDuringCommitProtectionDisable);
    tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();

    // The non-persistent options are gone: the system keys are off limits, the size limit
    // is back (a 200-byte write commits), the transaction is not causal write risky (a blind
    // write is self-conflicting), the priority is the default.
    tx.set(b"\xffsystem", b"v");
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::KEY_OUTSIDE_LEGAL_RANGE
    );
    let tx2 = env.db.create_transaction();
    opt(&tx2, TransactionOption::Timeout(Duration::from_secs(10)));
    opt(&tx2, TransactionOption::SizeLimit(100));
    tx2.on_error(Error::new(codes::NOT_COMMITTED))
        .await
        .unwrap();
    tx2.set_read_version(5);
    tx2.set(b"k", &[0; 200]);
    let commit = commit_request(&mut env, &tx2).await;
    assert_eq!(commit.read_conflict_ranges.len(), 1, "self-conflicting");
    let tx2 = env.db.create_transaction();
    opt(&tx2, TransactionOption::PriorityBatch);
    tx2.on_error(Error::new(codes::NOT_COMMITTED))
        .await
        .unwrap();
    let request = read_version_request(&mut env, &tx2).await;
    assert_eq!(priority(&request), GetReadVersion::PRIORITY_DEFAULT);

    // The persistent ones stay: the retry limit (2: one retry was used)...
    tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    assert_eq!(
        tx.on_error(Error::new(codes::NOT_COMMITTED))
            .await
            .unwrap_err()
            .code(),
        codes::NOT_COMMITTED
    );
    // ... the timeout (which counts from the creation)...
    tx.reset();
    opt(&tx, TransactionOption::Timeout(Duration::from_secs(10)));
    opt(&tx, TransactionOption::RetryLimit(5));
    tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;
    assert_eq!(
        tx.get_read_version().await.unwrap_err().code(),
        codes::TRANSACTION_TIMED_OUT
    );
    // ... and the maximum retry delay: waits are fractions of 5 ms, however many retries.
    tx.reset();
    opt(
        &tx,
        TransactionOption::MaxRetryDelay(Duration::from_millis(5)),
    );
    let start = tokio::time::Instant::now();
    for _ in 0..10 {
        tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    }
    // 10 retries: the backoff would reach 1 s without the limit (10 + 20 + 40 + ... ms).
    assert!(start.elapsed() < Duration::from_millis(100));

    // A full reset forgets the persistent options.
    tx.reset();
    for _ in 0..4 {
        tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(60)).await;
    tx.set_read_version(7);
    assert_eq!(tx.get_read_version().await.unwrap(), 7);
}

#[tokio::test(start_paused = true)]
async fn database_defaults_apply_to_new_transactions_and_persist_across_on_error() {
    let mut env = env(SchemaVersion::V8_0).await;
    // A transaction made before the defaults keeps what it started with.
    let before = env.db.create_transaction();
    env.db
        .set_option(DatabaseOption::TransactionSizeLimit(100))
        .unwrap();
    env.db
        .set_option(DatabaseOption::TransactionReportConflictingKeys)
        .unwrap();
    env.db
        .set_option(DatabaseOption::TransactionCausalReadRisky)
        .unwrap();

    // The size limit, which is not persistent, comes back after `on_error` all the same, and
    // the transaction's own beats it until then.
    let tx = env.db.create_transaction();
    tx.set_read_version(5);
    tx.set(b"k", &[0; 200]);
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::TRANSACTION_TOO_LARGE
    );
    tx.reset();
    opt(&tx, TransactionOption::SizeLimit(10_000));
    tx.set_read_version(5);
    tx.set(b"k", &[0; 200]);
    let commit = commit_request(&mut env, &tx).await;
    assert!(commit.report_conflicting_keys, "a default, reset or not");
    tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    tx.set_read_version(5);
    tx.set(b"k", &[0; 200]);
    let refused = tokio::time::timeout(Duration::from_secs(60), tx.commit())
        .await
        .expect("refused without being sent");
    assert_eq!(
        refused.unwrap_err().code(),
        codes::TRANSACTION_TOO_LARGE,
        "the default is back"
    );
    // The transaction made before is unaffected, until it is reset.
    before.set_read_version(5);
    before.set(b"k", &[0; 200]);
    let commit = commit_request(&mut env, &before).await;
    assert!(!commit.report_conflicting_keys);
    before.reset();
    before.set_read_version(5);
    before.set(b"k", &[0; 200]);
    assert_eq!(
        before.commit().await.unwrap_err().code(),
        codes::TRANSACTION_TOO_LARGE
    );

    // CAUSAL_READ_RISKY is a default too.
    let tx = env.db.create_transaction();
    let request = read_version_request(&mut env, &tx).await;
    assert_ne!(request.flags & GetReadVersion::FLAG_CAUSAL_READ_RISKY, 0);
    tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    let request = read_version_request(&mut env, &tx).await;
    assert_ne!(request.flags & GetReadVersion::FLAG_CAUSAL_READ_RISKY, 0);

    // Timeout and retry limit (persistent options, which the defaults feed too).
    env.db
        .set_option(DatabaseOption::TransactionTimeout(Duration::from_secs(2)))
        .unwrap();
    env.db
        .set_option(DatabaseOption::TransactionRetryLimit(1))
        .unwrap();
    let tx = env.db.create_transaction();
    tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    assert_eq!(
        tx.on_error(Error::new(codes::NOT_COMMITTED))
            .await
            .unwrap_err()
            .code(),
        codes::NOT_COMMITTED
    );
    let tx = env.db.create_transaction();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        tx.get_read_version().await.unwrap_err().code(),
        codes::TRANSACTION_TIMED_OUT
    );
    // A transaction that was made before them keeps what it started with, until it is reset.
    let before = env.db.create_transaction();
    env.db
        .set_option(DatabaseOption::TransactionTimeout(Duration::from_secs(2)))
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        before.get_read_version().await.unwrap_err().code(),
        codes::TRANSACTION_TIMED_OUT
    );
    // The transaction's own option overrides: 0 disables the timeout.
    let tx = env.db.create_transaction();
    opt(&tx, TransactionOption::Timeout(Duration::ZERO));
    tokio::time::sleep(Duration::from_secs(3)).await;
    tx.set_read_version(5);
    assert_eq!(tx.get_read_version().await.unwrap(), 5);

    // The same option again replaces its default.
    env.db
        .set_option(DatabaseOption::TransactionTimeout(Duration::from_secs(100)))
        .unwrap();
    env.db
        .set_option(DatabaseOption::TransactionRetryLimit(-1))
        .unwrap();
    env.db
        .set_option(DatabaseOption::TransactionSizeLimit(10_000_000))
        .unwrap();
    let tx = env.db.create_transaction();
    tokio::time::sleep(Duration::from_secs(3)).await;
    tx.set_read_version(5);
    assert_eq!(tx.get_read_version().await.unwrap(), 5);
    for _ in 0..5 {
        tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    }
    tx.set_read_version(5);
    tx.set(b"k", &[0; 200]);
    commit_request(&mut env, &tx).await;
}

#[tokio::test(start_paused = true)]
async fn database_options_validate_their_values() {
    let env = env(SchemaVersion::V7_4).await;
    for option in [
        DatabaseOption::TransactionSizeLimit(31),
        DatabaseOption::TransactionSizeLimit(10_000_001),
        DatabaseOption::TransactionRetryLimit(-2),
        DatabaseOption::TransactionTimeout(Duration::from_secs(1 << 40)),
        DatabaseOption::TransactionMaxRetryDelay(Duration::from_secs(1 << 40)),
        DatabaseOption::MaxWatches(1_000_001),
    ] {
        assert_eq!(
            env.db.set_option(option.clone()).unwrap_err().code(),
            codes::INVALID_OPTION_VALUE,
            "{option:?}"
        );
    }
    // Nothing was set: a new transaction has no timeout, retry limit or size limit.
    let tx = env.db.create_transaction();
    tokio::time::sleep(Duration::from_secs(100_000)).await;
    tx.set_read_version(5);
    assert_eq!(tx.get_read_version().await.unwrap(), 5);
    tx.set(b"k", &[0; 200]);
    for _ in 0..5 {
        tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    }
    for option in [
        DatabaseOption::MaxWatches(0),
        DatabaseOption::MaxWatches(10),
        DatabaseOption::SnapshotRywEnable,
        DatabaseOption::SnapshotRywDisable,
        DatabaseOption::TransactionIncludePortInAddress,
        DatabaseOption::TransactionBypassUnreadable,
        DatabaseOption::TransactionUsedDuringCommitProtectionDisable,
    ] {
        env.db
            .set_option(option.clone())
            .unwrap_or_else(|e| panic!("{option:?}: {e}"));
    }
}

#[tokio::test(start_paused = true)]
async fn the_next_write_adds_no_write_conflict_range() {
    for schema in SCHEMAS {
        let mut env = env(schema).await;
        type Write = fn(&Transaction<MemRuntime>);
        let writes: [(&str, Write, &[u8]); 4] = [
            ("set", |tx| tx.set(b"a", b"1"), b"a"),
            ("clear", |tx| tx.clear(b"a"), b"a"),
            ("clear_range", |tx| tx.clear_range(b"a", b"b"), b"a"),
            (
                "atomic_op",
                |tx| tx.atomic_op(b"a", &1u64.to_le_bytes(), MutationType::ADD_VALUE),
                b"a",
            ),
        ];
        for (name, write, key) in writes {
            let tx = env.db.create_transaction();
            tx.set_read_version(5);
            opt(&tx, TransactionOption::NextWriteNoWriteConflictRange);
            write(&tx);
            // It was used up: the second write has its range.
            tx.set(b"z", b"2");
            let commit = commit_request(&mut env, &tx).await;
            assert_eq!(commit.mutations.len(), 2, "{name}");
            assert!(
                commit
                    .write_conflict_ranges
                    .iter()
                    .all(|r| !r.begin.starts_with(key) || r.begin.starts_with(b"\xff")),
                "{name}: {:?}",
                commit.write_conflict_ranges
            );
            assert!(
                commit
                    .write_conflict_ranges
                    .contains(&KeyRange::single_key(b"z")),
                "{name}"
            );
        }

        // A write that fails uses it up too (the deferred error still fails the commit).
        let tx = env.db.create_transaction();
        opt(&tx, TransactionOption::NextWriteNoWriteConflictRange);
        tx.set(&[b'k'; 10_001], b"v");
        tx.set(b"a", b"1");
        assert_eq!(tx.commit().await.unwrap_err().code(), codes::KEY_TOO_LARGE);

        // `add_write_conflict_range` is not a write: it neither uses the option up nor is
        // affected by it.
        let tx = env.db.create_transaction();
        tx.set_read_version(5);
        opt(&tx, TransactionOption::NextWriteNoWriteConflictRange);
        tx.add_write_conflict_range(b"w", b"x");
        tx.set(b"a", b"1");
        tx.set(b"b", b"2");
        let commit = commit_request(&mut env, &tx).await;
        let ranges = &commit.write_conflict_ranges;
        assert!(ranges.contains(&KeyRange::new(b"w", b"x").unwrap()));
        assert!(!ranges.contains(&KeyRange::single_key(b"a")));
        assert!(ranges.contains(&KeyRange::single_key(b"b")));

        // Not persistent.
        let tx = env.db.create_transaction();
        tx.set_read_version(5);
        opt(&tx, TransactionOption::NextWriteNoWriteConflictRange);
        tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
        tx.set_read_version(5);
        tx.set(b"a", b"1");
        let commit = commit_request(&mut env, &tx).await;
        assert!(
            commit
                .write_conflict_ranges
                .contains(&KeyRange::single_key(b"a"))
        );
    }
}

#[tokio::test(start_paused = true)]
async fn used_during_commit_protection_can_be_disabled() {
    let mut env = env(SchemaVersion::V8_0).await;
    // By default the offending operation poisons the transaction, and the commit with it.
    let tx = env.db.create_transaction();
    tx.set_read_version(5);
    tx.set(b"k", b"v");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    let call = env.commit.next().await;
    assert_eq!(
        tx.get(b"k").await.unwrap_err().code(),
        codes::USED_DURING_COMMIT
    );
    call.reply_commit(committed(9, 0));
    assert_eq!(
        done(handle).await.unwrap_err().code(),
        codes::USED_DURING_COMMIT
    );

    // With the option only the operation fails; the commit completes.
    let tx = env.db.create_transaction();
    opt(&tx, TransactionOption::UsedDuringCommitProtectionDisable);
    tx.set_read_version(5);
    tx.set(b"k", b"v");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    let call = env.commit.next().await;
    let e = tx.get(b"k").await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    assert!(e.is_maybe_committed());
    tx.set(b"k2", b"v");
    call.reply_commit(committed(9, 0));
    assert_eq!(done(handle).await.unwrap().committed_version(), 9);

    // As a database default.
    env.db
        .set_option(DatabaseOption::TransactionUsedDuringCommitProtectionDisable)
        .unwrap();
    let tx = env.db.create_transaction();
    tx.set_read_version(5);
    tx.set(b"k", b"v");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    let call = env.commit.next().await;
    assert_eq!(
        tx.get(b"k").await.unwrap_err().code(),
        codes::USED_DURING_COMMIT
    );
    call.reply_commit(committed(9, 0));
    done(handle).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn options_with_checks_of_their_own() {
    let mut env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let code = |tx: &Transaction<MemRuntime>, option| tx.set_option(option).unwrap_err().code();

    // The transaction identifier: 1 to 100 bytes, and it cannot change.
    assert_eq!(
        code(&tx, TransactionOption::LogTransaction),
        codes::CLIENT_INVALID_OPERATION,
        "needs the identifier first"
    );
    for id in [String::new(), "x".repeat(101)] {
        assert_eq!(
            code(&tx, TransactionOption::DebugTransactionIdentifier(id)),
            codes::INVALID_OPTION_VALUE
        );
    }
    opt(
        &tx,
        TransactionOption::DebugTransactionIdentifier("x".repeat(100)),
    );
    opt(
        &tx,
        TransactionOption::DebugTransactionIdentifier("x".repeat(100)),
    );
    assert_eq!(
        code(
            &tx,
            TransactionOption::DebugTransactionIdentifier("y".into())
        ),
        codes::CLIENT_INVALID_OPERATION
    );
    opt(&tx, TransactionOption::LogTransaction);
    // `on_error` forgets it.
    tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    assert_eq!(
        code(&tx, TransactionOption::LogTransaction),
        codes::CLIENT_INVALID_OPERATION
    );
    opt(
        &tx,
        TransactionOption::DebugTransactionIdentifier("y".into()),
    );

    // The logged field length is -1 or positive.
    for n in [0, -2] {
        assert_eq!(
            code(&tx, TransactionOption::TransactionLoggingMaxFieldLength(n)),
            codes::INVALID_OPTION_VALUE
        );
    }
    opt(&tx, TransactionOption::TransactionLoggingMaxFieldLength(-1));
    opt(&tx, TransactionOption::TransactionLoggingMaxFieldLength(10));

    // The span parent is 33 bytes.
    assert_eq!(
        code(&tx, TransactionOption::SpanParent(vec![0; 32])),
        codes::INVALID_OPTION_VALUE
    );
    opt(&tx, TransactionOption::SpanParent(vec![0; 33]));

    // Tags are not supported, and say so.
    for option in [
        TransactionOption::Tag("tag".into()),
        TransactionOption::AutoThrottleTag("tag".into()),
    ] {
        let e = tx.set_option(option.clone()).unwrap_err();
        assert_eq!(e.code(), codes::INVALID_OPTION, "{option:?}");
        assert!(e.to_string().contains("tags"), "{e}");
    }
    // READ_YOUR_WRITES_DISABLE: fine on a new transaction, an error after a write or a read.
    let tx = env.db.create_transaction();
    opt(&tx, TransactionOption::ReadYourWritesDisable);
    tx.set(b"k", b"v");
    assert_eq!(
        code(&tx, TransactionOption::ReadYourWritesDisable),
        codes::CLIENT_INVALID_OPERATION
    );
    tx.reset();
    let read = spawn({
        let tx = tx.clone();
        async move { tx.get(b"k").await }
    });
    let _unanswered = env.grv.next().await;
    assert_eq!(
        code(&tx, TransactionOption::ReadYourWritesDisable),
        codes::CLIENT_INVALID_OPERATION
    );
    tx.cancel();
    assert_eq!(
        done(read).await.unwrap_err().code(),
        codes::TRANSACTION_CANCELLED
    );

    // Everything that is a no-op is accepted, and changes nothing.
    let tx = env.db.create_transaction();
    for option in [
        TransactionOption::CausalReadDisable,
        TransactionOption::IncludePortInAddress,
        TransactionOption::ReadServerSideCacheEnable,
        TransactionOption::ReadServerSideCacheDisable,
        TransactionOption::ReadPriorityNormal,
        TransactionOption::ReadPriorityLow,
        TransactionOption::ReadPriorityHigh,
        TransactionOption::RawAccess,
        TransactionOption::DebugRetryLogging(None),
        TransactionOption::DebugRetryLogging(Some("name".into())),
        TransactionOption::ServerRequestTracing,
        TransactionOption::SnapshotRywEnable,
        TransactionOption::SnapshotRywDisable,
        TransactionOption::ExpensiveClearCostEstimationEnable,
        TransactionOption::BypassUnreadable,
        TransactionOption::UseGrvCache,
    ] {
        tx.set_option(option.clone()).expect("a no-op option");
    }
    let request = read_version_request(&mut env, &tx).await;
    assert_eq!(priority(&request), GetReadVersion::PRIORITY_DEFAULT);
    assert_eq!(request.flags & !GetReadVersion::FLAG_PRIORITY_MASK, 0);
}

#[tokio::test(start_paused = true)]
async fn initialize_new_database_reads_at_version_zero_and_is_causal_write_risky() {
    let mut env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    opt(&tx, TransactionOption::InitializeNewDatabase);
    opt(&tx, TransactionOption::AccessSystemKeys);
    assert_eq!(tx.get_read_version().await.unwrap(), 0);
    tx.set(b"\xff/init", b"v");
    let commit = commit_request(&mut env, &tx).await;
    assert_eq!(commit.read_snapshot, 0);
    assert!(
        commit.read_conflict_ranges.is_empty(),
        "not self-conflicting"
    );
    assert!(env.grv.try_next().is_none(), "no read version request");

    // A transaction that has a read version refuses it.
    let tx = env.db.create_transaction();
    tx.set_read_version(5);
    assert_eq!(
        tx.set_option(TransactionOption::InitializeNewDatabase)
            .unwrap_err()
            .code(),
        codes::READ_VERSION_ALREADY_SET
    );
    // And after it, a read version is refused.
    let tx = env.db.create_transaction();
    opt(&tx, TransactionOption::InitializeNewDatabase);
    tx.set_read_version(5);
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::READ_VERSION_ALREADY_SET
    );
}

#[tokio::test(start_paused = true)]
async fn the_metadata_version_comes_from_the_read_version_reply_and_the_commit() {
    for schema in SCHEMAS {
        let mut env = env(schema).await;
        // The key needs no system key option; its value is the one the GRV proxy reported,
        // with no storage server asked (there is none here).
        let tx = env.db.create_transaction();
        let read = spawn({
            let tx = tx.clone();
            async move { tx.get(METADATA_VERSION).await }
        });
        let call = env.grv.next().await;
        call.reply_grv_with(100, false, Some(b"mv-100"));
        assert_eq!(done(read).await.unwrap(), Some(b"mv-100".to_vec()));
        // Again, and as a snapshot read: from the transaction's reply.
        assert_eq!(
            tx.get(METADATA_VERSION).await.unwrap(),
            Some(b"mv-100".to_vec())
        );
        assert_eq!(
            tx.snapshot().get(METADATA_VERSION).await.unwrap(),
            Some(b"mv-100".to_vec())
        );
        // Another key of the system key space is still out of bounds.
        assert_eq!(
            tx.get(b"\xff/metadataVersion\x00")
                .await
                .unwrap_err()
                .code(),
            codes::KEY_OUTSIDE_LEGAL_RANGE
        );
        assert_eq!(
            tx.get(b"\xff/other").await.unwrap_err().code(),
            codes::KEY_OUTSIDE_LEGAL_RANGE
        );
        // The read is a read: its conflict range is sent.
        tx.set(b"w", b"1");
        let commit = commit_request(&mut env, &tx).await;
        assert!(
            commit
                .read_conflict_ranges
                .contains(&KeyRange::single_key(METADATA_VERSION)),
            "{:?}",
            commit.read_conflict_ranges
        );

        // The database remembers the pair: a transaction that reads at that version needs
        // neither a read version nor a storage server.
        assert_eq!(
            env.db.reader().cached_metadata_version(100),
            Some(Some(b"mv-100".to_vec()))
        );
        let tx2 = env.db.create_transaction();
        tx2.set_read_version(100);
        assert_eq!(
            tx2.get(METADATA_VERSION).await.unwrap(),
            Some(b"mv-100".to_vec())
        );
        assert!(env.grv.try_next().is_none());

        // A key that is not set, as the GRV proxy says.
        let tx3 = env.db.create_transaction();
        tx3.set_option(TransactionOption::PriorityBatch).unwrap();
        let read = spawn({
            let tx = tx3.clone();
            async move { tx.get(METADATA_VERSION).await }
        });
        env.grv.next().await.reply_grv_with(300, false, None);
        assert_eq!(done(read).await.unwrap(), None);
        assert_eq!(env.db.reader().cached_metadata_version(300), Some(None));

        // A commit tells the database the metadata version at its commit version (when that
        // is newer than what it knows).
        let tx4 = env.db.create_transaction();
        tx4.set_read_version(300);
        tx4.set(b"w", b"1");
        let handle = spawn({
            let tx = tx4.clone();
            async move { tx.commit().await }
        });
        env.commit.next().await.reply_commit(Ok(CommitId {
            version: 400,
            txn_batch_id: 1,
            metadata_version: Some(b"mv-400".to_vec()),
            ..CommitId::default()
        }));
        done(handle).await.unwrap();
        assert_eq!(
            env.db.reader().cached_metadata_version(400),
            Some(Some(b"mv-400".to_vec()))
        );
        let tx5 = env.db.create_transaction();
        tx5.set_read_version(400);
        assert_eq!(
            tx5.get(METADATA_VERSION).await.unwrap(),
            Some(b"mv-400".to_vec())
        );
        // An older commit changes nothing.
        let tx6 = env.db.create_transaction();
        tx6.set_read_version(300);
        tx6.set(b"w", b"1");
        let handle = spawn({
            let tx = tx6.clone();
            async move { tx.commit().await }
        });
        env.commit.next().await.reply_commit(Ok(CommitId {
            version: 350,
            metadata_version: Some(b"stale".to_vec()),
            ..CommitId::default()
        }));
        done(handle).await.unwrap();
        assert_eq!(env.db.reader().cached_metadata_version(350), None);
        assert_eq!(
            env.db.reader().cached_metadata_version(400),
            Some(Some(b"mv-400".to_vec()))
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_sub_millisecond_timeout_is_not_zero() {
    let env = env(SchemaVersion::V8_0).await;
    // C++ takes whole milliseconds, and 0 disables the timeout: a fraction rounds up.
    let tx = env.db.create_transaction();
    opt(&tx, TransactionOption::Timeout(Duration::from_micros(500)));
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        tx.get_read_version().await.unwrap_err().code(),
        codes::TRANSACTION_TIMED_OUT
    );
    let tx = env.db.create_transaction();
    opt(&tx, TransactionOption::Timeout(Duration::ZERO));
    tokio::time::sleep(Duration::from_millis(5)).await;
    tx.set_read_version(1);
    assert_eq!(tx.get_read_version().await.unwrap(), 1);
}
