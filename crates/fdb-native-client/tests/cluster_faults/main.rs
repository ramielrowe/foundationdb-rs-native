//! Fault injection: the client against multi-process FDB clusters (`fdb-native-testkit`'s
//! [`FdbCluster`], image from `FDB_IMAGE`, default 7.4.10) while processes are killed,
//! paused, excluded, the coordinators and the redundancy change, the database is locked and
//! the whole cluster restarts, with a [workload](workload) whose invariants are checked at the
//! end.
//!
//! This binary is in the nextest `cluster` test group (`.config/nextest.toml`, at most 2 tests
//! at once): every test starts its own 3 or 4 `fdbserver` containers. **They are long**: each
//! takes 50 s to 3 minutes (about 40 s of that is the cluster's start; the longest is
//! `fault_exclude_storage_during_range_reads`), about 8.5 minutes for the binary with two at
//! a time, on 7.4.10 and 8.0.1 alike. Run with
//!
//! ```text
//! cargo nextest run -p fdb-native-client --test cluster_faults --run-ignored only
//! FDB_IMAGE=foundationdb/foundationdb:8.0.1 cargo nextest run -p fdb-native-client \
//!     --test cluster_faults --run-ignored only
//! ```
//!
//! # Scenarios
//!
//! | Test | Fault |
//! |---|---|
//! | `fault_kill_master_then_commit_proxy` | SIGKILL the master's process, then a commit proxy's (two recoveries) |
//! | `fault_kill_storage_server` | SIGKILL a storage server that is not the master |
//! | `fault_pause_process` | freeze a commit proxy's process (its sockets stay open: a black hole) |
//! | `fault_coordinator_change` | `coordinators` to another set, then kill the old coordinator; the client's cluster file follows |
//! | `fault_exclude_storage_during_range_reads` | data split into shards under the readers, then a storage process excluded (shards move) and included |
//! | `fault_database_lock` | `fdbcli lock`: `database_locked`, lock-aware transactions go on; unlocked by the client |
//! | `fault_redundancy_change` | `configure single`, then `configure double` |
//! | `fault_full_cluster_restart` | every process killed, then started again (no commit lost) |
//! | `fault_none_baseline` | none: the workload and its checker on a healthy cluster |
//!
//! # What is asserted
//!
//! * The workload's invariants (see [`workload`]): no lost and no duplicated commit (the
//!   counter equals its markers, the balances equal the transfer records, the total is
//!   conserved), every error reported as not committed really did not commit, watches fire
//!   or fail retryably within [`workload::WATCH_BOUND`] of the change (and never before it),
//!   range reads are consistent snapshots.
//! * Watches held through the fault ([`HeldWatches`]: registered before it, their keys
//!   changed during or after it) fire within the same bound.
//! * Progress resumes within [`RECOVERY`] of each fault (or of its healing, for the faults
//!   that stop the database).
//! * No panic anywhere (a panic hook counts them, also in the client's own tasks), no task
//!   left once the database is shut down, no watch left, and at most one connection per
//!   server process.

#![cfg(feature = "tokio")]

mod workload;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use fdb_native_client::error::codes;
use fdb_native_client::{Database, TransactionOption};
use fdb_native_testkit::{FdbCluster, FdbClusterBuilder};

use workload::{Config, HeldWatches, Kind, Layout, Workload};

/// How long the workload may make no progress after a fault (or its healing).
const RECOVERY: Duration = Duration::from_secs(90);
/// How long waits for the cluster's health may take.
const WAIT: Duration = Duration::from_secs(240);
/// How long the workload runs under a fault that it should ride through.
const UNDER_FAULT: Duration = Duration::from_secs(8);
/// How many watches are held through a fault.
const HELD: usize = 10;

static PANICS: AtomicUsize = AtomicUsize::new(0);

/// Counts every panic of the process (the tests' own and the client's spawned tasks', which
/// tokio would otherwise swallow).
fn count_panics() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            PANICS.fetch_add(1, Ordering::SeqCst);
            default(info);
        }));
    });
}

struct Harness {
    name: &'static str,
    cluster: Arc<FdbCluster>,
    db: Database,
    tasks_before: usize,
    started: Instant,
}

impl Harness {
    async fn start(name: &'static str, builder: FdbClusterBuilder) -> Harness {
        count_panics();
        let started = Instant::now();
        let cluster = tokio::task::spawn_blocking(move || builder.start())
            .await
            .unwrap()
            .expect("cluster");
        println!(
            "[{name}] {} processes of {} up after {:.1?}: {}",
            cluster.process_count(),
            cluster.image(),
            started.elapsed(),
            cluster.cluster_string()
        );
        let tasks_before = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let db = Database::open(Some(cluster.cluster_file())).expect("database");
        Harness {
            name,
            cluster: Arc::new(cluster),
            db,
            tasks_before,
            started,
        }
    }

    /// Runs `f` on the cluster (blocking, off the runtime); panics on its error.
    async fn cluster<T: Send + 'static>(
        &self,
        what: &str,
        f: impl FnOnce(&FdbCluster) -> fdb_native_testkit::Result<T> + Send + 'static,
    ) -> T {
        let cluster = self.cluster.clone();
        let t = Instant::now();
        let out = tokio::task::spawn_blocking(move || f(&cluster))
            .await
            .unwrap()
            .unwrap_or_else(|e| panic!("[{}] {what}: {e}", self.name));
        println!("[{}] {what} ({:.1?})", self.name, t.elapsed());
        out
    }

    /// A process that has `role` and none of `avoid`, else any with `role`.
    async fn pick(&self, role: &'static str, avoid: &'static [&'static str]) -> usize {
        self.cluster(&format!("find a {role}"), move |c| {
            let with_role = c.processes_with_role(role)?;
            for &i in &with_role {
                let roles = c.process_roles(i)?;
                if !roles.iter().any(|r| avoid.contains(&r.as_str())) {
                    return Ok(i);
                }
            }
            with_role.first().copied().ok_or_else(|| {
                fdb_native_testkit::Error::InvalidConfig(format!("no process is a {role}"))
            })
        })
        .await
    }

    async fn wait_healthy(&self) {
        self.cluster("wait until healthy", |c| c.wait_healthy(WAIT))
            .await;
    }

    async fn progress(&self, wl: &Workload, what: &str) {
        wl.progress(&format!("{}: {what}", self.name), RECOVERY)
            .await;
    }

    /// Stops and checks the workload, shuts the database down and checks that nothing is
    /// left of it, then removes the cluster.
    async fn finish(self, wl: Workload) {
        let Harness {
            name,
            cluster,
            db,
            tasks_before,
            started,
        } = self;
        wl.finish(cluster.process_count()).await;
        // The watches are gone with their workers.
        let t = Instant::now();
        while db.outstanding_watches() != 0 || db.watched_keys() != 0 {
            assert!(
                t.elapsed() < Duration::from_secs(10),
                "[{name}] {} watches outstanding, {} keys watched",
                db.outstanding_watches(),
                db.watched_keys()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        db.shutdown().await;
        assert!(db.is_shut_down());
        drop(db);
        // Every task of the database ends.
        let metrics = tokio::runtime::Handle::current().metrics();
        let t = Instant::now();
        while metrics.num_alive_tasks() > tasks_before {
            assert!(
                t.elapsed() < Duration::from_secs(10),
                "[{name}] {} tasks alive after the shutdown, {tasks_before} before the database",
                metrics.num_alive_tasks()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            PANICS.load(Ordering::SeqCst),
            0,
            "[{name}] something panicked"
        );
        tokio::task::spawn_blocking(move || drop(cluster))
            .await
            .unwrap();
        println!("[{name}] done in {:.1?}", started.elapsed());
    }
}

/// Runs the workload for `d`.
async fn run_for(d: Duration) {
    tokio::time::sleep(d).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn fault_kill_master_then_commit_proxy() {
    let h = Harness::start("kill master", FdbCluster::builder()).await;
    let wl = Workload::start(&h.db, Config::default()).await;
    h.progress(&wl, "warm-up").await;

    // The master: the cluster recovers on the two other processes.
    let held = HeldWatches::register(&h.db, "master", HELD).await;
    let master = h.pick("master", &[]).await;
    h.cluster(&format!("kill master {master}"), move |c| c.kill(master))
        .await;
    h.progress(&wl, "master killed").await;
    run_for(UNDER_FAULT).await;
    h.cluster(&format!("start {master}"), move |c| c.start_process(master))
        .await;
    h.progress(&wl, "master restarted").await;
    held.change_and_check(&h.db, false).await;

    // A commit proxy of the new generation, commits in flight.
    let held = HeldWatches::register(&h.db, "proxy", HELD).await;
    let proxy = h.pick("commit_proxy", &[]).await;
    h.cluster(&format!("kill commit proxy {proxy}"), move |c| {
        c.kill(proxy)
    })
    .await;
    h.progress(&wl, "commit proxy killed").await;
    run_for(UNDER_FAULT).await;
    h.cluster(&format!("start {proxy}"), move |c| c.start_process(proxy))
        .await;
    h.wait_healthy().await;
    h.progress(&wl, "healthy again").await;
    held.change_and_check(&h.db, false).await;
    h.finish(wl).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn fault_kill_storage_server() {
    let h = Harness::start("kill storage", FdbCluster::builder()).await;
    let wl = Workload::start(&h.db, Config::default()).await;
    h.progress(&wl, "warm-up").await;

    // Watches on the storage servers (some on the one killed), changed while it is down.
    let held = HeldWatches::register(&h.db, "storage-down", HELD).await;
    let storage = h.pick("storage", &["master", "cluster_controller"]).await;
    h.cluster(&format!("kill storage {storage}"), move |c| c.kill(storage))
        .await;
    h.progress(&wl, "storage killed").await;
    run_for(UNDER_FAULT).await;
    h.progress(&wl, "storage still down").await;
    held.change_and_check(&h.db, false).await;
    // Watches registered while it is down, changed once it is back.
    let held = HeldWatches::register(&h.db, "storage-back", HELD).await;
    h.cluster(&format!("start {storage}"), move |c| {
        c.start_process(storage)
    })
    .await;
    h.wait_healthy().await;
    h.progress(&wl, "healthy again").await;
    held.change_and_check(&h.db, false).await;
    h.finish(wl).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn fault_pause_process() {
    let h = Harness::start("pause", FdbCluster::builder()).await;
    let wl = Workload::start(&h.db, Config::default()).await;
    h.progress(&wl, "warm-up").await;

    // Frozen: connections to it stay open and nothing is answered, until the cluster (and the
    // client's failure monitor) gives up on it.
    // Some of the watches are on the frozen process, changed while it is frozen.
    let held = HeldWatches::register(&h.db, "paused", HELD).await;
    let target = h.pick("commit_proxy", &["cluster_controller"]).await;
    h.cluster(&format!("pause {target}"), move |c| c.pause(target))
        .await;
    h.progress(&wl, "paused").await;
    run_for(UNDER_FAULT).await;
    h.progress(&wl, "still paused").await;
    held.change_and_check(&h.db, false).await;
    let held = HeldWatches::register(&h.db, "unpaused", HELD).await;
    h.cluster(&format!("unpause {target}"), move |c| c.unpause(target))
        .await;
    h.wait_healthy().await;
    h.progress(&wl, "unpaused").await;
    held.change_and_check(&h.db, false).await;
    h.finish(wl).await;
}

/// The cluster string in a cluster file's contents (the last line that is not a comment).
fn cluster_string_of(contents: &str) -> Option<String> {
    contents
        .lines()
        .map(str::trim)
        .rfind(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_owned)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn fault_coordinator_change() {
    let h = Harness::start("coordinators", FdbCluster::builder().processes(4)).await;
    let wl = Workload::start(&h.db, Config::default()).await;
    h.progress(&wl, "warm-up").await;
    let file = h.cluster.cluster_file().to_owned();
    let before = std::fs::read_to_string(&file).unwrap();
    let held = HeldWatches::register(&h.db, "coordinators", HELD).await;

    let new = h
        .cluster("coordinators 1 2 3", |c| c.set_coordinators(&[1, 2, 3]))
        .await;
    println!("new coordinators: {new}");
    h.progress(&wl, "coordinators changed").await;

    // The client follows the change and rewrites its cluster file.
    let t = Instant::now();
    loop {
        let contents = std::fs::read_to_string(&file).unwrap();
        if cluster_string_of(&contents).as_deref() == Some(new.as_str()) {
            println!(
                "the client rewrote its cluster file after {:.1?}",
                t.elapsed()
            );
            break;
        }
        assert!(
            t.elapsed() < RECOVERY,
            "the cluster file still says {contents:?} (was {before:?}, want {new})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        h.db.cluster_monitor().connection_string().to_string(),
        new,
        "the monitor's connection string"
    );

    // The old coordinator goes away: only the new ones are left to ask.
    h.cluster("kill 0 (no longer a coordinator)", |c| c.kill(0))
        .await;
    h.progress(&wl, "old coordinator killed").await;
    // A new database from the rewritten file works without the old coordinator.
    let fresh = Database::open(Some(&file)).expect("database from the rewritten file");
    let value = tokio::time::timeout(
        RECOVERY,
        fresh.run(|tx, _| async move {
            tx.set_option(TransactionOption::Timeout(Duration::from_secs(30)))?;
            tx.get(b"ctr/value").await
        }),
    )
    .await
    .expect("a read through the new coordinators")
    .expect("a read through the new coordinators");
    assert!(value.is_some());
    fresh.shutdown().await;
    drop(fresh);
    held.change_and_check(&h.db, false).await;

    h.cluster("start 0", |c| c.start_process(0)).await;
    h.wait_healthy().await;
    h.progress(&wl, "healthy again").await;
    h.finish(wl).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn fault_exclude_storage_during_range_reads() {
    const FILLER: usize = 3000;
    let h = Harness::start("exclude", FdbCluster::builder().processes(4).small_shards()).await;
    // 3 MB in `k00000000`..: split into shards by data distribution, which starts as the
    // readers do.
    h.cluster("write 3000 keys of 1 KB", |c| {
        c.write_keys("k", FILLER, 1000)
    })
    .await;
    let cfg = Config {
        accounts: 100,
        layout: Layout::Spread {
            stride: FILLER / 100,
            filler: FILLER,
        },
        padding: 200,
        range_readers: 2,
        ..Config::default()
    };
    let wl = Workload::start(&h.db, cfg).await;
    h.progress(&wl, "warm-up").await;
    let shards = h
        .cluster("wait for the shards", |c| c.wait_for_partitions(4, WAIT))
        .await;
    println!("{shards} shards");
    let reads_before = wl.successes(Kind::RangeRead);

    // Shards move off the excluded process while the readers read them (the held watches'
    // among them).
    let held = HeldWatches::register(&h.db, "exclude", HELD).await;
    let storage = h.pick("storage", &["master", "cluster_controller"]).await;
    h.cluster(&format!("exclude {storage}"), move |c| c.exclude(storage))
        .await;
    h.progress(&wl, "excluded").await;
    held.change_and_check(&h.db, false).await;
    h.cluster(&format!("include {storage}"), move |c| c.include(storage))
        .await;
    h.wait_healthy().await;
    h.progress(&wl, "included").await;
    run_for(UNDER_FAULT).await;
    let reads = wl.successes(Kind::RangeRead) - reads_before;
    let cached = h.db.reader().cached_locations();
    println!("{reads} range reads during the moves, {cached} shard locations cached");
    assert!(reads >= 5, "only {reads} range reads during the moves");
    assert!(cached >= 2, "the reads crossed several shards ({cached})");
    h.finish(wl).await;
}

/// The value `\xff/dbLocked` has for the lock UID `uid` (32 hex digits, as fdbcli prints
/// it), after the 10-byte versionstamp: `BinaryWriter::toValue(uid, Unversioned())`.
fn lock_uid_bytes(uid: &str) -> Vec<u8> {
    let first = u64::from_str_radix(&uid[..16], 16).unwrap();
    let second = u64::from_str_radix(&uid[16..32], 16).unwrap();
    [first.to_le_bytes(), second.to_le_bytes()].concat()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn fault_database_lock() {
    const LOCKED_KEY: &[u8] = b"\xff/dbLocked";
    let h = Harness::start("lock", FdbCluster::builder()).await;
    let wl = Workload::start(&h.db, Config::default()).await;
    h.progress(&wl, "warm-up").await;

    let held = HeldWatches::register(&h.db, "locked", HELD).await;
    let out = h.cluster("fdbcli lock", |c| c.fdbcli("lock")).await;
    assert!(out.contains("Database locked"), "{out}");
    let uid = out
        .split("lockUID:")
        .nth(1)
        .and_then(|s| s.split_whitespace().next())
        .unwrap_or_else(|| panic!("no lock UID in {out}"))
        .to_owned();
    assert_eq!(uid.len(), 32, "{uid}");

    // A read version: `database_locked`, not retried by a transaction with a retry limit.
    let tx = h.db.create_transaction();
    tx.set_option(TransactionOption::RetryLimit(0)).unwrap();
    let e = tx.get_read_version().await.expect_err("locked");
    assert_eq!(e.code(), codes::DATABASE_LOCKED, "{e}");
    assert!(e.is_retryable_not_committed() && !e.is_maybe_committed());
    // (A transaction holds the database: the check for tasks left needs it gone.)
    drop(tx);
    // A write in `run` whose retries run out: `database_locked`, certainly not committed.
    let e =
        h.db.run(|tx, _| async move {
            tx.set_option(TransactionOption::RetryLimit(2))?;
            tx.set(b"lock/denied", b"x");
            Ok(())
        })
        .await
        .expect_err("locked");
    assert_eq!(e.code(), codes::DATABASE_LOCKED, "{e}");
    assert!(!e.is_maybe_committed(), "{e}");
    // A lock-aware transaction reads and commits.
    let lock_aware =
        h.db.run(|tx, _| async move {
            tx.set_option(TransactionOption::Timeout(Duration::from_secs(30)))?;
            tx.set_option(TransactionOption::LockAware)?;
            let counter = tx.get(b"ctr/value").await?;
            tx.set(b"lock/aware", b"x");
            Ok(counter)
        })
        .await
        .expect("a lock-aware transaction commits");
    assert!(lock_aware.is_some());

    // Nothing else commits while the database is locked. (A commit that the resolvers
    // accepted in the batch that saw the lock is `not_committed`, not `database_locked`:
    // `CommitProxyServer` replies so to a transaction that is not lock-aware.)
    run_for(Duration::from_secs(2)).await;
    wl.stalled("locked", Duration::from_secs(6)).await;
    // Watches go on: a lock-aware change fires them.
    held.change_and_check(&h.db, true).await;

    // Unlocked by the client (fdbcli's `unlock` asks for a passphrase), as C++'s
    // `unlockDatabase`: lock-aware, the UID checked, the key cleared.
    let expected = lock_uid_bytes(&uid);
    h.db.run(|tx, _| {
        let expected = expected.clone();
        async move {
            tx.set_option(TransactionOption::Timeout(Duration::from_secs(30)))?;
            tx.set_option(TransactionOption::LockAware)?;
            tx.set_option(TransactionOption::AccessSystemKeys)?;
            let value = tx.get(LOCKED_KEY).await?.expect("the lock key");
            assert_eq!(&value[10..], &expected[..], "the lock UID");
            tx.clear(LOCKED_KEY);
            Ok(())
        }
    })
    .await
    .expect("unlock");
    h.progress(&wl, "unlocked").await;
    let status = h.cluster("status json", |c| c.status_json()).await;
    assert_eq!(
        status["cluster"]["database_lock_state"]["locked"].as_bool(),
        Some(false),
        "{}",
        status["cluster"]["database_lock_state"]
    );
    assert!(
        wl.error_count(Kind::Limited, codes::DATABASE_LOCKED) > 0,
        "the limited workers saw database_locked"
    );
    assert!(
        wl.error_count(Kind::Raw, codes::DATABASE_LOCKED) > 0,
        "the raw workers saw database_locked"
    );
    h.finish(wl).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn fault_redundancy_change() {
    let h = Harness::start("redundancy", FdbCluster::builder()).await;
    let wl = Workload::start(&h.db, Config::default()).await;
    h.progress(&wl, "warm-up").await;
    for mode in ["single", "double"] {
        let held = HeldWatches::register(&h.db, mode, HELD).await;
        let out = h
            .cluster(&format!("configure {mode}"), move |c| {
                c.fdbcli_with_timeout(&format!("configure {mode}"), Duration::from_secs(60))
            })
            .await;
        assert!(out.contains("Configuration changed"), "{out}");
        h.progress(&wl, &format!("configured {mode}")).await;
        h.wait_healthy().await;
        h.progress(&wl, &format!("healthy in {mode}")).await;
        held.change_and_check(&h.db, false).await;
    }
    let status = h.cluster("status json", |c| c.status_json()).await;
    assert_eq!(
        status["cluster"]["configuration"]["redundancy_mode"].as_str(),
        Some("double")
    );
    h.finish(wl).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn fault_full_cluster_restart() {
    let h = Harness::start("full restart", FdbCluster::builder()).await;
    let wl = Workload::start(&h.db, Config::default()).await;
    h.progress(&wl, "warm-up").await;

    let held = HeldWatches::register(&h.db, "restart", HELD).await;
    let n = h.cluster.process_count();
    h.cluster("kill every process", move |c| {
        for i in 0..n {
            c.kill(i)?;
        }
        Ok(())
    })
    .await;
    run_for(Duration::from_secs(3)).await;
    wl.stalled("cluster down", Duration::from_secs(5)).await;
    // Started together: none of them can come up alone (no coordinator quorum).
    h.cluster("start every process", move |c| {
        c.start_processes(&(0..n).collect::<Vec<_>>())
    })
    .await;
    h.progress(&wl, "restarted").await;
    held.change_and_check(&h.db, false).await;
    h.wait_healthy().await;
    h.progress(&wl, "healthy again").await;
    // The checker reads every marker: nothing committed before the restart is lost.
    h.finish(wl).await;
}

/// Errors are classified even when nothing is wrong with the cluster: a sanity check of the
/// workload and the checker, and the baseline the fault tests are compared with.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn fault_none_baseline() {
    let h = Harness::start("baseline", FdbCluster::builder()).await;
    let wl = Workload::start(&h.db, Config::default()).await;
    h.progress(&wl, "warm-up").await;
    run_for(Duration::from_secs(10)).await;
    h.finish(wl).await;
}
