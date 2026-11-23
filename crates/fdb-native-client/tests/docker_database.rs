//! `Database` and `Transaction` against a real, disposable FDB server started by
//! `fdb-native-testkit` (image from `FDB_IMAGE`, default 7.4.10), checked against `fdbcli`:
//! Rust writes read by `fdbcli` and the other way round, clears and clear ranges both ways,
//! binary keys and values, concurrent read-modify-write transactions through `run` (the
//! conflicts are detected and retried: the counter ends at exactly N), a transaction near the
//! size limits, a recovery in the middle of a workload, and (with the `tls` feature) a TLS
//! cluster. Run with `cargo nextest run -p fdb-native-client --run-ignored only` (add
//! `--features tls` for the TLS test).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::{Database, Error, TransactionOption};
use fdb_native_testkit::FdbContainer;

const LIMIT: Duration = Duration::from_secs(120);

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {LIMIT:?}"))
}

/// `printable()` (`flow`): how `fdbcli` prints keys and values.
fn printable(bytes: &[u8]) -> String {
    let mut out = String::new();
    for &b in bytes {
        match b {
            b'\\' => out.push_str("\\\\"),
            32..=126 => out.push(b as char),
            _ => out.push_str(&format!("\\x{b:02x}")),
        }
    }
    out
}

/// Every byte as `\xNN`, for an `fdbcli` command.
fn escaped(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("\\x{b:02x}")).collect()
}

struct Cluster {
    fdb: Arc<FdbContainer>,
    db: Database,
}

impl Cluster {
    async fn start() -> Cluster {
        Cluster::start_with(FdbContainer::start).await
    }

    async fn start_with(
        start: impl FnOnce() -> fdb_native_testkit::Result<FdbContainer> + Send + 'static,
    ) -> Cluster {
        let fdb = tokio::task::spawn_blocking(start)
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

    async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let key = key.to_vec();
        within(
            "get",
            self.db.run(move |tx, _| {
                let key = key.clone();
                async move { tx.get(&key).await }
            }),
        )
        .await
        .expect("get")
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
async fn docker_writes_and_clears_are_seen_by_fdbcli_and_the_other_way_round() {
    let c = Cluster::start().await;

    // Rust writes, fdbcli reads.
    let committed = within(
        "write",
        c.db.run(|tx, _| async move {
            for i in 0..10 {
                tx.set(
                    format!("rust/{i:02}").as_bytes(),
                    format!("value {i}").as_bytes(),
                );
            }
            tx.set(b"rust/bin\x00\xff\\", b"\x00\x01\xfe\xff");
            tx.set(b"rust/empty", b"");
            Ok(())
        }),
    )
    .await;
    committed.expect("committed");
    let out = c.cli("getrange rust/ rust0").await;
    println!("{out}");
    for i in 0..10 {
        let line = format!("`rust/{i:02}' is `value {i}'");
        assert!(out.contains(&line), "{line} in {out}");
    }
    let line = format!(
        "`{}' is `{}'",
        printable(b"rust/bin\x00\xff\\"),
        printable(b"\x00\x01\xfe\xff")
    );
    assert!(out.contains(&line), "{line} in {out}");
    assert!(out.contains("`rust/empty' is `'"), "{out}");

    // fdbcli writes, Rust reads.
    let out = c
        .cli(&format!(
            "writemode on; set cli/a 1; set cli/b 2; set cli/c 3; set cli/d 4; set {} {}",
            escaped(b"cli/bin\x00\x01\xff"),
            escaped(b"\xff\x00 \x7f")
        ))
        .await;
    assert!(out.contains("Committed"), "{out}");
    assert_eq!(c.get(b"cli/a").await.as_deref(), Some(&b"1"[..]));
    assert_eq!(c.get(b"cli/d").await.as_deref(), Some(&b"4"[..]));
    assert_eq!(
        c.get(b"cli/bin\x00\x01\xff").await.as_deref(),
        Some(&b"\xff\x00 \x7f"[..])
    );
    assert_eq!(c.get(b"cli/missing").await, None);

    // Rust clears and clear ranges, seen by fdbcli.
    within(
        "clear",
        c.db.run(|tx, _| async move {
            tx.clear(b"rust/00");
            tx.clear_range(b"rust/03", b"rust/07");
            tx.clear(b"rust/bin\x00\xff\\");
            Ok(())
        }),
    )
    .await
    .expect("cleared");
    let out = c.cli("getrange rust/ rust0").await;
    println!("{out}");
    for gone in [
        "rust/00'", "rust/03'", "rust/04'", "rust/05'", "rust/06'", "rust/bin",
    ] {
        assert!(!out.contains(gone), "{gone} was cleared: {out}");
    }
    for kept in [
        "`rust/01'",
        "`rust/02'",
        "`rust/07'",
        "`rust/09'",
        "`rust/empty'",
    ] {
        assert!(
            out.contains(kept),
            "{kept} is end-exclusive / untouched: {out}"
        );
    }

    // fdbcli clears and clear ranges, seen by Rust.
    let out = c
        .cli("writemode on; clear cli/a; clearrange cli/b cli/d")
        .await;
    assert!(out.contains("Committed"), "{out}");
    assert_eq!(c.get(b"cli/a").await, None);
    assert_eq!(c.get(b"cli/b").await, None);
    assert_eq!(c.get(b"cli/c").await, None);
    assert_eq!(c.get(b"cli/d").await.as_deref(), Some(&b"4"[..]));

    // A transaction by hand: get, set, commit, and the read sees the commit's version.
    let tx = c.db.create_transaction();
    let rv = within("GRV", tx.get_read_version()).await.unwrap();
    tx.set(b"hand", b"made");
    let result = within("commit", tx.commit()).await.unwrap();
    assert!(result.committed_version() > rv);
    let stamp = result.versionstamp().expect("a versionstamp");
    assert_eq!(&stamp[..8], &result.committed_version().to_be_bytes());
    let out = c.cli("get hand").await;
    assert!(out.contains("`hand' is `made'"), "{out}");
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_binary_keys_and_values_round_trip() {
    let c = Cluster::start().await;
    let all: Vec<u8> = (0..=255u8).collect();
    let key: Vec<u8> = [&b"bin/"[..], &all[..0xff]].concat();
    let value: Vec<u8> = all.iter().rev().copied().collect();
    within(
        "write",
        c.db.run(|tx, _| {
            let (key, value) = (key.clone(), value.clone());
            async move {
                tx.set(&key, &value);
                for b in 0..=255u8 {
                    tx.set(&[b'b', b'/', b], &[b, b, b]);
                }
                Ok(())
            }
        }),
    )
    .await
    .unwrap();
    assert_eq!(c.get(&key).await, Some(value.clone()));
    for b in [0u8, 1, 0x5c, 0x7f, 0x80, 0xfe, 0xff] {
        assert_eq!(c.get(&[b'b', b'/', b]).await, Some(vec![b, b, b]), "{b}");
    }
    let out = c.cli(&format!("get {}", escaped(&key))).await;
    assert!(out.contains(&printable(&value)), "{out}");
    let out = c.cli(&format!("get {}", escaped(b"b/\x00"))).await;
    assert!(out.contains("`b/\\x00' is `\\x00\\x00\\x00'"), "{out}");
    // fdbcli sees all 256 single-byte keys.
    let out = c.cli("getrange b/ b0 1000").await;
    assert_eq!(out.matches("`b/").count(), 256, "{out}");
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_concurrent_increments_conflict_and_are_retried() {
    const N: usize = 40;
    let c = Cluster::start().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    // Every task's first attempt reads the counter before any of them commits, so all but
    // one of the first attempts must conflict.
    let barrier = Arc::new(tokio::sync::Barrier::new(N));
    let tasks: Vec<_> = (0..N)
        .map(|_| {
            let db = c.db.clone();
            let attempts = attempts.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
                db.run(|tx, maybe_committed| {
                    let (attempts, barrier, first) =
                        (attempts.clone(), barrier.clone(), first.clone());
                    async move {
                        // Not idempotent: an unknown result would make the count wrong.
                        assert!(!maybe_committed.0, "commit_unknown_result");
                        attempts.fetch_add(1, Ordering::SeqCst);
                        let n = match tx.get(b"counter").await? {
                            Some(v) => u64::from_le_bytes(v.try_into().expect("8 bytes")),
                            None => 0,
                        };
                        if first.swap(false, Ordering::SeqCst) {
                            barrier.wait().await;
                        }
                        tx.set(b"counter", &(n + 1).to_le_bytes());
                        Ok(n + 1)
                    }
                })
                .await
            })
        })
        .collect();
    for task in tasks {
        within("increment", task).await.unwrap().expect("committed");
    }
    let attempts = attempts.load(Ordering::SeqCst);
    println!("{N} increments in {attempts} attempts");
    assert!(attempts > N, "conflicts were detected and retried");
    let value = c.get(b"counter").await.expect("a counter");
    assert_eq!(u64::from_le_bytes(value.try_into().unwrap()), N as u64);
    let out = c.cli("get counter").await;
    assert!(out.contains(&printable(&(N as u64).to_le_bytes())), "{out}");

    // Snapshot reads add no conflict range: the same race with snapshot reads loses updates
    // instead of conflicting (C++'s semantics), which the barrier makes certain.
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let tasks: Vec<_> = (0..2)
        .map(|_| {
            let db = c.db.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                let tx = db.create_transaction();
                let n = tx.snapshot().get(b"snap").await?.map_or(0, |v| v[0]);
                barrier.wait().await;
                tx.set(b"snap", &[n + 1]);
                tx.commit().await
            })
        })
        .collect();
    for task in tasks {
        within("snapshot", task)
            .await
            .unwrap()
            .expect("no conflict");
    }
    assert_eq!(c.get(b"snap").await, Some(vec![1]));
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_a_transaction_near_the_size_limits() {
    let c = Cluster::start().await;
    let big_key = vec![b'k'; 10_000];
    let big_value = vec![0xab; 100_000];
    // 95 values of 100 000 bytes: about 9.5 MB of the 10 MB limit.
    let result = within(
        "big commit",
        c.db.run(|tx, _| {
            let (big_key, big_value) = (big_key.clone(), big_value.clone());
            async move {
                tx.set(&big_key, b"the longest key");
                for i in 0..95u8 {
                    let mut value = big_value.clone();
                    value[0] = i;
                    tx.set(&[b'v', b'/', i], &value);
                }
                Ok(())
            }
        }),
    )
    .await;
    result.expect("committed");
    assert_eq!(
        c.get(&big_key).await.as_deref(),
        Some(&b"the longest key"[..])
    );
    for i in [0u8, 47, 94] {
        let value = c.get(&[b'v', b'/', i]).await.expect("a value");
        assert_eq!(value.len(), 100_000);
        assert_eq!(value[0], i);
        assert!(value[1..].iter().all(|&b| b == 0xab));
    }
    let out = c.cli("getrange v/ v0 1000").await;
    assert_eq!(out.matches("`v/").count(), 95);

    // Past the limits: refused before anything is sent, and not retried.
    let e =
        c.db.run(|tx, _| async move {
            for i in 0..101u8 {
                tx.set(&[b'w', b'/', i], &[0; 100_000]);
            }
            Ok(())
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TOO_LARGE, "{e}");
    let e =
        c.db.run(|tx, _| async move {
            tx.set(&[b'k'; 10_001], b"v");
            Ok(())
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::KEY_TOO_LARGE);
    let e =
        c.db.run(|tx, _| async move {
            tx.set(b"k", &[0; 100_001]);
            Ok(())
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::VALUE_TOO_LARGE);
    // A smaller SIZE_LIMIT.
    let e =
        c.db.run(|tx, _| async move {
            tx.set_option(TransactionOption::SizeLimit(1000))?;
            tx.set(b"k", &[0; 2000]);
            Ok(())
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TOO_LARGE);
    let out = c.cli("getrange w/ w0").await;
    assert!(!out.contains("`w/"), "{out}");
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_run_completes_through_a_recovery() {
    const WRITES: usize = 400;
    let c = Cluster::start().await;
    let retries = Arc::new(AtomicUsize::new(0));
    let progress = Arc::new(AtomicUsize::new(0));
    // An idempotent workload (a retried unknown result writes the same values again): each
    // transaction reads the previous key and writes the next.
    let workload = tokio::spawn({
        let db = c.db.clone();
        let retries = retries.clone();
        let progress = progress.clone();
        async move {
            for i in 0..WRITES {
                db.run(|tx, _| {
                    let retries = retries.clone();
                    async move {
                        if tx.retries() > 0 {
                            retries.fetch_add(1, Ordering::SeqCst);
                        }
                        tx.set_option(TransactionOption::Timeout(Duration::from_secs(60)))?;
                        if i > 0 {
                            let previous = tx.get(format!("rec/{:04}", i - 1).as_bytes()).await?;
                            assert_eq!(previous, Some(format!("{}", i - 1).into_bytes()));
                        }
                        tx.set(format!("rec/{i:04}").as_bytes(), format!("{i}").as_bytes());
                        Ok(())
                    }
                })
                .await?;
                progress.store(i + 1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, Error>(())
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let monitor = c.db.cluster_monitor();
    for config in [
        "configure commit_proxies=2 grv_proxies=2",
        "configure commit_proxies=1 grv_proxies=1 resolvers=1 logs=2",
    ] {
        let before = monitor.current().expect("an info");
        let done = progress.load(Ordering::SeqCst);
        assert!(done > 0 && done < WRITES, "the workload is running: {done}");
        let out = c.cli(config).await;
        println!("fdbcli {config}: {out:?}");
        let after = within("new proxies", monitor.proxies_changed(&before))
            .await
            .expect("monitor running");
        println!(
            "recovered: {} GRV / {} commit proxies",
            after.grv_proxies.len(),
            after.commit_proxies.len()
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let done = progress.load(Ordering::SeqCst);
    assert!(
        done < WRITES,
        "the workload outlasted the recoveries: {done}"
    );
    within("workload", workload)
        .await
        .unwrap()
        .expect("every transaction committed");
    println!(
        "{WRITES} transactions, {} retried attempts",
        retries.load(Ordering::SeqCst)
    );
    let count = within(
        "count",
        c.db.run(|tx, _| async move {
            let mut n = 0;
            for i in 0..WRITES {
                if tx.get(format!("rec/{i:04}").as_bytes()).await?
                    == Some(format!("{i}").into_bytes())
                {
                    n += 1;
                }
            }
            Ok(n)
        }),
    )
    .await
    .unwrap();
    assert_eq!(count, WRITES);
    let out = c.cli(&format!("getrange rec/ rec0 {}", WRITES + 10)).await;
    assert_eq!(out.matches("`rec/").count(), WRITES);
    c.finish().await;
}

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_tls_cluster() {
    use fdb_native_client::DatabaseOptions;
    use fdb_native_transport::runtime::TokioRuntime;
    use fdb_native_transport::tls::TlsConfig;

    let fdb = tokio::task::spawn_blocking(|| {
        FdbContainer::builder()
            .image(fdb_native_testkit::default_image())
            .tls()
            .start()
    })
    .await
    .unwrap()
    .expect("TLS container");
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
    assert!(fdb.cluster_string().ends_with(":tls"));
    let tls = fdb.tls().expect("TLS files");
    let mut options = DatabaseOptions::default();
    options.tls = Some(
        TlsConfig::new()
            .ca_path(tls.ca_path())
            .cert_path(tls.client_cert_path())
            .key_path(tls.client_key_path()),
    );
    let db = Database::open_with(TokioRuntime::new(), Some(fdb.cluster_file()), options)
        .expect("database");
    let c = Cluster {
        fdb: Arc::new(fdb),
        db,
    };
    within(
        "write",
        c.db.run(|tx, _| async move {
            tx.set(b"tls/key", b"over TLS");
            tx.set(b"tls/bin", b"\x00\xff");
            Ok(())
        }),
    )
    .await
    .expect("committed");
    let out = c.cli("get tls/key").await;
    assert!(out.contains("`tls/key' is `over TLS'"), "{out}");
    let out = c.cli("writemode on; set tls/cli yes; clear tls/bin").await;
    assert!(out.contains("Committed"), "{out}");
    assert_eq!(c.get(b"tls/cli").await.as_deref(), Some(&b"yes"[..]));
    assert_eq!(c.get(b"tls/bin").await, None);
    assert_eq!(c.get(b"tls/key").await.as_deref(), Some(&b"over TLS"[..]));
    c.finish().await;
}
