//! Versionstamped operations and `get_versionstamp` against a scripted fake commit proxy on an
//! in-memory network (the docker tests check the real stamps): the request a transaction
//! sends on both protocol versions, the deferred errors of bad offsets, and when the future of
//! `get_versionstamp` resolves and with what.

#![cfg(feature = "tokio")]

mod support;

use std::time::Duration;

use fdb_native_client::commit::with_versionstamp_offset;
use fdb_native_client::error::codes;
use fdb_native_client::{ConnectionRecord, Database, DatabaseOptions, Error, MutationType};
use fdb_native_schema::SchemaVersion;
use fdb_native_schema::domain::{CommitId, KeyRange, Mutation, MutationType as Mt};
use fdb_native_transport::peer::PeerManagerConfig;
use fdb_native_wire::ProtocolVersion;
use support::proxy::FakeProxy;
use support::{FakeCoordinator, MemRuntime, Net, info, sock};

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

/// `prefix`, a 10-byte placeholder, `tail`, and the offset of the placeholder.
fn with_stamp(prefix: &[u8], tail: &[u8]) -> Vec<u8> {
    let mut v = prefix.to_vec();
    v.extend_from_slice(&[0xff; 10]);
    v.extend_from_slice(tail);
    with_versionstamp_offset(v, prefix.len() as u32)
}

#[tokio::test(start_paused = true)]
async fn versionstamped_writes_reach_the_proxy_with_their_offsets_and_get_versionstamp_resolves() {
    for schema in SCHEMAS {
        let mut env = env(schema).await;
        let tx = env.db.create_transaction();
        let key = with_stamp(b"log/", b"/x");
        let value = with_stamp(b"v:", b"!");
        tx.set_versionstamped_key(&key, b"payload");
        tx.set_versionstamped_value(b"stamped", &value);
        let stamp = spawn(tx.get_versionstamp());
        let handle = spawn({
            let tx = tx.clone();
            async move { tx.commit().await }
        });
        let grv = env.grv.next().await;
        grv.reply_grv(100);
        let call = env.commit.next().await;
        let (commit, _) = call.commit();
        // The key goes with its placeholder filled with the smallest stamp it may get
        // (`transformVersionstampKey`: zeros, without a read version at the time of the
        // write), as C++ sends it; the proxy overwrites the 10 bytes.
        let mut sent_key = key.clone();
        sent_key[4..14].fill(0);
        assert_eq!(
            commit.mutations,
            vec![
                Mutation::new(Mt::SET_VERSIONSTAMPED_KEY, sent_key, "payload").unwrap(),
                Mutation::new(Mt::SET_VERSIONSTAMPED_VALUE, "stamped", value.clone()).unwrap(),
            ],
            "{schema:?}"
        );
        // Only the value's key is a write conflict range (and the self-conflicting key).
        assert_eq!(commit.write_conflict_ranges.len(), 2);
        assert_eq!(
            commit.write_conflict_ranges[0],
            KeyRange::single_key(b"stamped")
        );
        assert!(
            commit.write_conflict_ranges[1]
                .begin
                .starts_with(b"\xff/SC/")
        );
        call.reply_commit(committed(0x0102_0304_0506, 0x0708));
        let result = done(handle).await.unwrap();
        let expected = [0, 0, 1, 2, 3, 4, 5, 6, 7, 8];
        assert_eq!(result.versionstamp(), Some(expected));
        assert_eq!(done(stamp).await.unwrap(), expected);
        assert_eq!(tx.versionstamp(), Some(expected));
        // After the commit: at once, and not a use of the committed transaction.
        assert_eq!(tx.get_versionstamp().await.unwrap(), expected);
        assert_eq!(tx.committed_version(), 0x0102_0304_0506);
    }
}

#[tokio::test(start_paused = true)]
async fn bad_offsets_are_deferred_client_invalid_operation_and_nothing_is_sent() {
    let mut env = env(SchemaVersion::V7_4).await;
    for bad in [
        b"abc".to_vec(),
        with_versionstamp_offset(vec![0; 9], 0),
        with_versionstamp_offset(vec![0; 10], 1),
        vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff],
    ] {
        for key_op in [true, false] {
            let tx = env.db.create_transaction();
            if key_op {
                tx.set_versionstamped_key(&bad, b"v");
            } else {
                tx.set_versionstamped_value(b"k", &bad);
            }
            let e = tx.commit().await.unwrap_err();
            assert_eq!(e.code(), codes::CLIENT_INVALID_OPERATION, "{bad:?}");
        }
    }
    // Not an atomic operation.
    let tx = env.db.create_transaction();
    tx.atomic_op(b"k", b"v", MutationType::SET_VALUE);
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::INVALID_MUTATION_TYPE
    );
    // The legal range applies to the raw key.
    let tx = env.db.create_transaction();
    tx.set_versionstamped_key(&with_stamp(b"\xff", b""), b"v");
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::KEY_OUTSIDE_LEGAL_RANGE
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(env.grv.try_next().is_none());
    assert!(env.commit.try_next().is_none());
}

#[tokio::test(start_paused = true)]
async fn a_versionstamped_key_alone_is_sent_and_a_read_only_transaction_has_no_commit_version() {
    let mut env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    tx.set_read_version(5);
    tx.set_versionstamped_key(&with_stamp(b"k", b""), b"");
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    let call = env.commit.next().await;
    let (commit, _) = call.commit();
    assert_eq!(commit.mutations.len(), 1);
    // No write conflict range for the key (it is not known yet): the only one is the
    // self-conflicting key a blind write gets, which is also a read conflict range.
    assert_eq!(commit.write_conflict_ranges.len(), 1);
    assert!(
        commit.write_conflict_ranges[0]
            .begin
            .starts_with(b"\xff/SC/")
    );
    assert_eq!(commit.read_conflict_ranges, commit.write_conflict_ranges);
    call.reply_commit(committed(9, 0));
    done(handle).await.unwrap();
    assert_eq!(tx.versionstamp(), Some([0, 0, 0, 0, 0, 0, 0, 9, 0, 0]));

    // Nothing to write: `no_commit_version`, as C++.
    let tx = env.db.create_transaction();
    let stamp = tx.get_versionstamp();
    tx.commit().await.unwrap();
    assert_eq!(stamp.await.unwrap_err().code(), codes::NO_COMMIT_VERSION);
    assert_eq!(
        tx.get_versionstamp().await.unwrap_err().code(),
        codes::NO_COMMIT_VERSION
    );
}

#[tokio::test(start_paused = true)]
async fn the_future_fails_when_the_commit_fails_or_the_transaction_is_reset() {
    let mut env = env(SchemaVersion::V7_4).await;
    // A failed commit: `transaction_invalid_version`.
    let tx = env.db.create_transaction();
    tx.set_read_version(5);
    tx.set_versionstamped_key(&with_stamp(b"k", b""), b"");
    let stamp = spawn(tx.get_versionstamp());
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    env.commit
        .next()
        .await
        .reply_commit(Err(codes::NOT_COMMITTED));
    assert_eq!(done(handle).await.unwrap_err().code(), codes::NOT_COMMITTED);
    assert_eq!(
        done(stamp).await.unwrap_err().code(),
        codes::TRANSACTION_INVALID_VERSION
    );
    assert_eq!(tx.versionstamp(), None);

    // A reset before the commit.
    let tx = env.db.create_transaction();
    let stamp = spawn(tx.get_versionstamp());
    tx.reset();
    assert_eq!(
        done(stamp).await.unwrap_err().code(),
        codes::TRANSACTION_CANCELLED
    );
    // `on_error` resets too; and a cancelled transaction answers at once.
    let stamp = spawn(tx.get_versionstamp());
    tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    assert_eq!(
        done(stamp).await.unwrap_err().code(),
        codes::TRANSACTION_CANCELLED
    );
    tx.cancel();
    assert_eq!(
        tx.get_versionstamp().await.unwrap_err().code(),
        codes::TRANSACTION_CANCELLED
    );
    // A dropped transaction answers `transaction_cancelled` rather than hanging.
    let tx = env.db.create_transaction();
    let stamp = tx.get_versionstamp();
    drop(tx);
    assert_eq!(
        stamp.await.unwrap_err().code(),
        codes::TRANSACTION_CANCELLED
    );
}

#[tokio::test(start_paused = true)]
async fn asking_while_the_commit_is_in_progress_is_used_during_commit() {
    let mut env = env(SchemaVersion::V7_4).await;
    let tx = env.db.create_transaction();
    tx.set_read_version(5);
    tx.set_versionstamped_key(&with_stamp(b"k", b""), b"");
    let early = spawn(tx.get_versionstamp());
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    let call = env.commit.next().await;
    // The commit is on its way: a late call fails, as `checkUsedDuringCommit`, and poisons the
    // transaction and so the waiting future too.
    let e = tx.get_versionstamp().await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    assert_eq!(
        done(early).await.unwrap_err().code(),
        codes::USED_DURING_COMMIT
    );
    call.reply_commit(committed(9, 0));
    let _ = done(handle).await;
}

#[tokio::test(start_paused = true)]
async fn a_deferred_error_answers_the_waiting_futures_and_later_calls() {
    let mut env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let early = spawn(tx.get_versionstamp());
    tx.set_versionstamped_key(&with_stamp(b"k", b""), b"v");
    tx.set(&[b'k'; 10_001], b"v");
    // C++ would leave the future pending until the reset; here the commit that refuses to
    // start answers it with the error that stops it.
    assert_eq!(tx.commit().await.unwrap_err().code(), codes::KEY_TOO_LARGE);
    assert_eq!(done(early).await.unwrap_err().code(), codes::KEY_TOO_LARGE);
    // And a call made now gets the deferred error at once (it would never be answered).
    assert_eq!(
        tx.get_versionstamp().await.unwrap_err().code(),
        codes::KEY_TOO_LARGE
    );
    // A reset clears it all: the next future waits.
    tx.reset();
    let waiting = tx.get_versionstamp();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .is_err()
    );
    assert!(env.commit.try_next().is_none());
}

#[tokio::test(start_paused = true)]
async fn a_second_commit_fails_the_futures_with_used_during_commit_not_2020() {
    let mut env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    tx.set_read_version(5);
    tx.set_versionstamped_key(&with_stamp(b"k", b""), b"");
    let early = spawn(tx.get_versionstamp());
    let first = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    let call = env.commit.next().await;
    let e = tx.commit().await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    assert!(e.is_maybe_committed());
    assert_eq!(
        done(early).await.unwrap_err().code(),
        codes::USED_DURING_COMMIT
    );
    call.reply_commit(committed(9, 0));
    // The first commit was poisoned too (C++'s `resetPromise`).
    assert_eq!(
        done(first).await.unwrap_err().code(),
        codes::USED_DURING_COMMIT
    );
}

#[tokio::test(start_paused = true)]
async fn the_timeout_and_a_cancellation_end_the_future_without_another_operation() {
    use fdb_native_client::TransactionOption::Timeout;
    let mut env = env(SchemaVersion::V7_4).await;
    // The timeout passes while nothing else happens on the transaction.
    let tx = env.db.create_transaction();
    tx.set_option(Timeout(Duration::from_secs(5))).unwrap();
    let stamp = spawn(tx.get_versionstamp());
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(!stamp.is_finished());
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        done(stamp).await.unwrap_err().code(),
        codes::TRANSACTION_TIMED_OUT
    );
    // The transaction died with it.
    assert_eq!(
        tx.get_read_version().await.unwrap_err().code(),
        codes::TRANSACTION_TIMED_OUT
    );

    // A timeout set after the call is the one that counts, also if shorter.
    let tx = env.db.create_transaction();
    tx.set_option(Timeout(Duration::from_secs(100))).unwrap();
    let stamp = spawn(tx.get_versionstamp());
    tokio::time::sleep(Duration::from_secs(1)).await;
    tx.set_option(Timeout(Duration::from_secs(3))).unwrap();
    assert!(!stamp.is_finished());
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        done(stamp).await.unwrap_err().code(),
        codes::TRANSACTION_TIMED_OUT
    );

    // The timeout during a commit that was sent: the commit and the future both fail, and the
    // commit may have committed.
    let tx = env.db.create_transaction();
    tx.set_option(Timeout(Duration::from_secs(5))).unwrap();
    tx.set_read_version(5);
    tx.set_versionstamped_key(&with_stamp(b"k", b""), b"");
    let stamp = spawn(tx.get_versionstamp());
    let handle = spawn({
        let tx = tx.clone();
        async move { tx.commit().await }
    });
    let _unanswered = env.commit.next().await;
    let e = done(handle).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TIMED_OUT);
    assert!(e.is_maybe_committed());
    assert_eq!(
        done(stamp).await.unwrap_err().code(),
        codes::TRANSACTION_TIMED_OUT
    );

    // A cancellation.
    let tx = env.db.create_transaction();
    let stamp = spawn(tx.get_versionstamp());
    tx.cancel();
    assert_eq!(
        done(stamp).await.unwrap_err().code(),
        codes::TRANSACTION_CANCELLED
    );
    // A future dropped before the answer is not kept; another one still gets its answer.
    let tx = env.db.create_transaction();
    drop(tx.get_versionstamp());
    let stamp = spawn(tx.get_versionstamp());
    tx.reset();
    assert_eq!(
        done(stamp).await.unwrap_err().code(),
        codes::TRANSACTION_CANCELLED
    );
}

#[tokio::test(start_paused = true)]
async fn an_unknown_commit_result_is_transaction_invalid_version_for_the_future() {
    for schema in SCHEMAS {
        let mut env = env(schema).await;
        let tx = env.db.create_transaction();
        // No dummy transaction to settle the result.
        tx.set_option(fdb_native_client::TransactionOption::CausalWriteRisky)
            .unwrap();
        tx.set_read_version(5);
        tx.set_versionstamped_key(&with_stamp(b"k", b""), b"");
        let stamp = spawn(tx.get_versionstamp());
        let handle = spawn({
            let tx = tx.clone();
            async move { tx.commit().await }
        });
        env.commit
            .next()
            .await
            .reply_commit(Err(codes::COMMIT_UNKNOWN_RESULT));
        let e = done(handle).await.unwrap_err();
        assert_eq!(e.code(), codes::COMMIT_UNKNOWN_RESULT, "{schema:?}");
        assert!(e.is_maybe_committed());
        assert_eq!(
            done(stamp).await.unwrap_err().code(),
            codes::TRANSACTION_INVALID_VERSION,
            "{schema:?}"
        );
        assert_eq!(tx.versionstamp(), None);
    }
}
