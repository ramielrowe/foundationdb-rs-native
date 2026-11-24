//! Transaction and database options against a real, disposable FDB server started by
//! `fdb-native-testkit` (image from `FDB_IMAGE`, default 7.4.10; also run with 8.0.1):
//! timeouts (`transaction_timed_out`), retry and size limits, the read version priorities and
//! flags a real GRV proxy accepts, a locked database with `lock_aware` and `read_lock_aware`
//! (locked with `fdbcli`), the database's transaction defaults, and the
//! `\xff/metadataVersion` read path against a real cluster. Run with
//! `cargo nextest run -p fdb-native-client --run-ignored only`.

#![cfg(feature = "tokio")]

use std::sync::Arc;
use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::{Database, DatabaseOption, Error, MutationType, TransactionOption};
use fdb_native_testkit::FdbContainer;

const LIMIT: Duration = Duration::from_secs(120);
const METADATA_VERSION: &[u8] = b"\xff/metadataVersion";

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {LIMIT:?}"))
}

struct Cluster {
    fdb: Arc<FdbContainer>,
    db: Database,
}

impl Cluster {
    async fn start() -> Cluster {
        let fdb = tokio::task::spawn_blocking(FdbContainer::start)
            .await
            .unwrap()
            .expect("container");
        println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
        let db = Database::open(Some(fdb.cluster_file())).expect("database");
        Cluster {
            fdb: Arc::new(fdb),
            db,
        }
    }

    async fn cli(&self, command: &str) -> String {
        let fdb = self.fdb.clone();
        let command = command.to_owned();
        tokio::task::spawn_blocking(move || fdb.fdbcli(&command))
            .await
            .unwrap()
            .unwrap_or_else(|e| panic!("fdbcli: {e}"))
    }

    fn is_8_0(&self) -> bool {
        self.fdb.image().contains(":8.")
    }

    async fn finish(self) {
        self.db.shutdown().await;
        let Cluster { fdb, db } = self;
        drop(db);
        tokio::task::spawn_blocking(move || drop(fdb))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_timeout_retry_limit_and_size_limit() {
    let c = Cluster::start().await;

    // A timeout counts from the creation: once it has passed, every operation fails with
    // `transaction_timed_out`, a read to the cluster among them.
    let tx = c.db.create_transaction();
    tx.set_option(TransactionOption::Timeout(Duration::from_millis(100)))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let e = within("get", tx.get(b"k")).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TIMED_OUT, "{e}");
    assert_eq!(
        within("commit", tx.commit()).await.unwrap_err().code(),
        codes::TRANSACTION_TIMED_OUT
    );
    // ... and after a reset it works again (with a timeout that is long enough).
    tx.reset();
    tx.set_option(TransactionOption::Timeout(Duration::from_secs(30)))
        .unwrap();
    assert_eq!(within("get", tx.get(b"k")).await.unwrap(), None);

    // `run` gives up with the timeout error when a closure takes too long, whatever it does.
    let started = std::time::Instant::now();
    let result: Result<(), Error> = within(
        "run",
        c.db.run(|tx, _| async move {
            tx.set_option(TransactionOption::Timeout(Duration::from_millis(200)))?;
            tx.get(b"k").await?;
            // Longer than the timeout: the next read is too late.
            tokio::time::sleep(Duration::from_millis(400)).await;
            tx.get(b"k").await?;
            Ok(())
        }),
    )
    .await;
    assert_eq!(result.unwrap_err().code(), codes::TRANSACTION_TIMED_OUT);
    assert!(started.elapsed() < Duration::from_secs(20));

    // The retry limit: `on_error` returns the error once it was retried that many times, and
    // the limit persists across `on_error` (and is forgotten by a reset).
    let tx = c.db.create_transaction();
    tx.set_option(TransactionOption::RetryLimit(2)).unwrap();
    tx.set_option(TransactionOption::MaxRetryDelay(Duration::from_millis(5)))
        .unwrap();
    for _ in 0..2 {
        within("on_error", tx.on_error(Error::new(codes::NOT_COMMITTED)))
            .await
            .unwrap();
    }
    let e = within("on_error", tx.on_error(Error::new(codes::NOT_COMMITTED)))
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::NOT_COMMITTED);
    tx.reset();
    for _ in 0..4 {
        within("on_error", tx.on_error(Error::new(codes::NOT_COMMITTED)))
            .await
            .unwrap();
    }

    // The size limit: `transaction_too_large` for a transaction over it, from the commit;
    // 10 000 000 is the largest, and it is not persistent.
    let tx = c.db.create_transaction();
    tx.set_option(TransactionOption::SizeLimit(1000)).unwrap();
    tx.set(b"big", &[7; 2000]);
    let e = within("commit", tx.commit()).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TOO_LARGE, "{e}");
    let tx = c.db.create_transaction();
    tx.set_option(TransactionOption::SizeLimit(1000)).unwrap();
    tx.on_error(Error::new(codes::NOT_COMMITTED)).await.unwrap();
    tx.set(b"big", &[7; 2000]);
    within("commit", tx.commit()).await.unwrap();

    // The database's defaults: the same limits for every transaction made after.
    c.db.set_option(DatabaseOption::TransactionSizeLimit(1000))
        .unwrap();
    c.db.set_option(DatabaseOption::TransactionRetryLimit(0))
        .unwrap();
    c.db.set_option(DatabaseOption::TransactionTimeout(Duration::from_secs(60)))
        .unwrap();
    let tx = c.db.create_transaction();
    tx.set(b"big", &[7; 2000]);
    let e = within("commit", tx.commit()).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TOO_LARGE, "{e}");
    // The default is back after `on_error`, and the retry limit 0 refuses the retry.
    let e = within("on_error", tx.on_error(Error::new(codes::NOT_COMMITTED)))
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::NOT_COMMITTED);
    // A transaction's own options beat the defaults.
    let tx = c.db.create_transaction();
    tx.set_option(TransactionOption::SizeLimit(10_000)).unwrap();
    tx.set(b"big", &[7; 2000]);
    within("commit", tx.commit()).await.unwrap();

    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_priorities_and_read_version_flags_are_accepted() {
    let c = Cluster::start().await;
    let mut options = vec![
        vec![TransactionOption::PriorityBatch],
        vec![TransactionOption::PrioritySystemImmediate],
        vec![TransactionOption::CausalReadRisky],
        vec![
            TransactionOption::PriorityBatch,
            TransactionOption::CausalReadRisky,
        ],
        // Ignored by 7.4, a delay the proxy accepts on 8.0.
        vec![TransactionOption::MaxGrvQueueDelay(Duration::from_secs(60))],
        vec![TransactionOption::LockAware],
        vec![TransactionOption::ReadLockAware],
    ];
    if c.is_8_0() {
        options.push(vec![
            TransactionOption::PriorityBatch,
            TransactionOption::MaxGrvQueueDelay(Duration::from_secs(60)),
        ]);
    }
    let mut last = 0;
    for options in options {
        let tx = c.db.create_transaction();
        for option in options.clone() {
            tx.set_option(option).unwrap();
        }
        let version = within("GRV", tx.get_read_version())
            .await
            .unwrap_or_else(|e| panic!("{options:?}: {e}"));
        assert!(version > 0);
        // A read at that version works.
        assert_eq!(within("get", tx.get(b"nothing here")).await.unwrap(), None);
        last = version;
    }
    assert!(last > 0);

    // A write and a read with each priority.
    for (i, option) in [
        TransactionOption::PriorityBatch,
        TransactionOption::PrioritySystemImmediate,
    ]
    .into_iter()
    .enumerate()
    {
        let key = format!("prio/{i}");
        let tx = c.db.create_transaction();
        tx.set_option(option).unwrap();
        tx.set(key.as_bytes(), b"v");
        within("commit", tx.commit()).await.unwrap();
        let tx = c.db.create_transaction();
        assert_eq!(
            within("get", tx.get(key.as_bytes()))
                .await
                .unwrap()
                .as_deref(),
            Some(&b"v"[..])
        );
    }
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_a_locked_database_needs_lock_aware_transactions() {
    let c = Cluster::start().await;
    within(
        "write",
        c.db.run(|tx, _| async move {
            tx.set(b"before", b"1");
            Ok(())
        }),
    )
    .await
    .unwrap();

    let out = c.cli("lock").await;
    println!("{out}");
    let uid = out
        .lines()
        .find_map(|l| l.strip_prefix("Locking database with lockUID: "))
        .unwrap_or_else(|| panic!("a lock UID in {out}"))
        .trim()
        .to_owned();
    assert!(out.contains("Database locked."), "{out}");

    // Not lock aware: `database_locked` (1038), at the read version.
    let tx = c.db.create_transaction();
    let e = within("GRV", tx.get_read_version()).await.unwrap_err();
    assert_eq!(e.code(), codes::DATABASE_LOCKED, "{e}");
    assert_eq!(
        within("get", tx.get(b"before")).await.unwrap_err().code(),
        codes::DATABASE_LOCKED
    );

    // READ_LOCK_AWARE reads, and refuses to write: `transaction_read_only`.
    let tx = c.db.create_transaction();
    tx.set_option(TransactionOption::ReadLockAware).unwrap();
    assert_eq!(
        within("get", tx.get(b"before")).await.unwrap().as_deref(),
        Some(&b"1"[..])
    );
    tx.set(b"during", b"1");
    let e = within("commit", tx.commit()).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_READ_ONLY, "{e}");

    // LOCK_AWARE reads and writes, with either option before it.
    let tx = c.db.create_transaction();
    tx.set_option(TransactionOption::ReadLockAware).unwrap();
    tx.set_option(TransactionOption::LockAware).unwrap();
    tx.set(b"during", b"2");
    within("commit", tx.commit()).await.unwrap();
    let tx = c.db.create_transaction();
    tx.set_option(TransactionOption::LockAware).unwrap();
    assert_eq!(
        within("get", tx.get(b"during")).await.unwrap().as_deref(),
        Some(&b"2"[..])
    );
    // `run` with the option set in the closure (it applies to one attempt).
    within(
        "run",
        c.db.run(|tx, _| async move {
            tx.set_option(TransactionOption::LockAware)?;
            tx.set(b"during", b"3");
            Ok(())
        }),
    )
    .await
    .unwrap();

    // Unlock it as a tool without `fdbcli`'s passphrase would: a lock aware transaction that
    // may access the system keys clears the lock key (`\xff/dbLocked`, which holds the UID).
    let tx = c.db.create_transaction();
    tx.set_option(TransactionOption::LockAware).unwrap();
    tx.set_option(TransactionOption::AccessSystemKeys).unwrap();
    let lock = within("get", tx.get(b"\xff/dbLocked")).await.unwrap();
    assert!(lock.is_some(), "the lock ({uid}) is set");
    tx.clear(b"\xff/dbLocked");
    within("commit", tx.commit()).await.unwrap();

    // Ordinary transactions work again, and see what the lock aware ones wrote.
    let tx = c.db.create_transaction();
    assert_eq!(
        within("get", tx.get(b"during")).await.unwrap().as_deref(),
        Some(&b"3"[..])
    );
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_the_metadata_version_is_read_from_the_grv_reply_the_cache_and_storage() {
    let c = Cluster::start().await;

    // A key that was never written.
    let tx = c.db.create_transaction();
    let first = within("get", tx.get(METADATA_VERSION)).await.unwrap();
    println!("metadata version before: {first:?}");

    // Bump it as clients that cache metadata do: a versionstamped value of the 14 bytes
    // C++ requires (10 placeholder bytes at offset 0), which the commit proxy turns into the
    // transaction's versionstamp.
    let tx = c.db.create_transaction();
    tx.set_option(TransactionOption::AccessSystemKeys).unwrap();
    tx.atomic_op(
        METADATA_VERSION,
        &[0; 14],
        MutationType::SET_VERSIONSTAMPED_VALUE,
    );
    let stamp = tx.get_versionstamp();
    let result = within("commit", tx.commit()).await.unwrap();
    let stamp = within("stamp", stamp).await.unwrap();
    let commit_version = result.committed_version();
    println!("committed at {commit_version}, stamp {stamp:?}");

    // A new transaction reads it from the GRV reply: the stamp, with no option for system
    // keys.
    let tx = c.db.create_transaction();
    let value = within("get", tx.get(METADATA_VERSION)).await.unwrap();
    assert_eq!(value.as_deref(), Some(&stamp[..]));
    let read_version = tx.get_read_version().await.unwrap();
    assert!(read_version >= commit_version);

    // At exactly the commit version (the database remembers the pair from the commit reply,
    // or from the GRV reply if that was that version): from the cache.
    let tx = c.db.create_transaction();
    tx.set_read_version(read_version);
    assert_eq!(
        within("get", tx.get(METADATA_VERSION))
            .await
            .unwrap()
            .as_deref(),
        Some(&stamp[..])
    );
    assert!(
        c.db.reader()
            .cached_metadata_version(read_version)
            .is_some()
    );

    // At a version the database has never seen a reply for: read from a storage server.
    let tx = c.db.create_transaction();
    tx.set_read_version(commit_version);
    let cached = c.db.reader().cached_metadata_version(commit_version);
    let value = within("get", tx.get(METADATA_VERSION)).await.unwrap();
    assert_eq!(value.as_deref(), Some(&stamp[..]), "cached: {cached:?}");
    // Before the commit it was what it was.
    let tx = c.db.create_transaction();
    tx.set_read_version(commit_version - 1);
    assert_eq!(
        within("get", tx.get(METADATA_VERSION)).await.unwrap(),
        first
    );
    assert!(
        c.db.reader()
            .cached_metadata_version(commit_version - 1)
            .is_none()
    );

    c.finish().await;
}
