//! The read path against a real, disposable FDB server started by `fdb-native-testkit`
//! (image from `FDB_IMAGE`, default 7.4.10): values written with `fdbcli` are read back at a
//! fresh read version, a missing key is `None`, many reads run at once, and reads go on
//! after a configuration change recruits new proxies (a recovery). Run with
//! `cargo nextest run --run-ignored only`.

use std::sync::Arc;
use std::time::Duration;

use fdb_native_client::read::{ReadConfig, Reader};
use fdb_native_client::{ClusterMonitor, ClusterMonitorConfig, ConnectionRecord};
use fdb_native_testkit::FdbContainer;
use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig};
use fdb_native_transport::runtime::TokioRuntime;

const LIMIT: Duration = Duration::from_secs(60);

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing within {LIMIT:?}"))
}

/// Runs `fdbcli --exec` in the container, off the executor.
async fn fdbcli(fdb: &Arc<FdbContainer>, command: &str) -> String {
    let fdb = fdb.clone();
    let command = command.to_owned();
    tokio::task::spawn_blocking(move || fdb.fdbcli(&command))
        .await
        .unwrap()
        .unwrap_or_else(|e| panic!("fdbcli: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_reads_values_written_by_fdbcli() {
    let fdb = Arc::new(
        tokio::task::spawn_blocking(FdbContainer::start)
            .await
            .unwrap()
            .expect("container"),
    );
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());

    let mut writes = String::from("writemode on;");
    for i in 0..50 {
        writes.push_str(&format!(" set key{i:02} value{i};"));
    }
    writes.push_str(r" set bin\x00\xff \x01\x02;");
    let out = fdbcli(&fdb, &writes).await;
    assert!(out.contains("Committed"), "{out}");

    let rt = TokioRuntime::new();
    let peers = PeerManager::new(rt.clone(), PeerManagerConfig::default(), DiscardEvents);
    let record = ConnectionRecord::open(fdb.cluster_file()).expect("cluster file");
    let monitor = Arc::new(ClusterMonitor::start(
        rt.clone(),
        peers.clone(),
        record,
        ClusterMonitorConfig::default(),
    ));
    let reader = Reader::new(rt, peers.clone(), monitor.clone(), ReadConfig::default());

    // A fresh read version sees what fdbcli committed.
    let v1 = within("GRV", reader.get_read_version()).await.expect("GRV");
    println!("read version {v1}");
    assert!(v1 > 0);
    assert_eq!(
        within("get", reader.get(b"key07", v1)).await.unwrap(),
        Some(b"value7".to_vec())
    );
    assert_eq!(
        within("get", reader.get(b"bin\x00\xff", v1)).await.unwrap(),
        Some(vec![1, 2])
    );
    // A missing key.
    assert_eq!(
        within("get", reader.get(b"no such key", v1)).await.unwrap(),
        None
    );
    let location = within("locate", reader.locate(b"key07")).await.unwrap();
    println!(
        "key07 is in [{:?}, {:?}) on {} server(s)",
        String::from_utf8_lossy(&location.range.begin),
        String::from_utf8_lossy(&location.range.end),
        location.servers.len()
    );
    assert!(!location.servers.is_empty());

    // Many reads at once: read versions are batched, locations are cached.
    let reads: Vec<_> = (0..500)
        .map(|i| {
            let reader = reader.clone();
            tokio::spawn(async move {
                let snapshot = reader.snapshot().await?;
                let key = format!("key{:02}", i % 60);
                let value = snapshot.get(key.as_bytes()).await?;
                Ok::<_, fdb_native_client::Error>((i % 60, value))
            })
        })
        .collect();
    for read in reads {
        let (i, value) = within("concurrent read", read).await.unwrap().unwrap();
        let expected = (i < 50).then(|| format!("value{i}").into_bytes());
        assert_eq!(value, expected, "key{i:02}");
    }
    println!("cached shard locations: {}", reader.cached_locations());

    // A configuration change recruits new proxies (a recovery). Reads go on: read versions
    // from the new GRV proxies, and new writes are seen.
    let before = monitor.current().expect("an info");
    // Reads keep going through the recovery; a failure must be one a transaction retries.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let background = tokio::spawn({
        let reader = reader.clone();
        let stop = stop.clone();
        async move {
            let (mut ok, mut retryable) = (0, 0);
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                let result = async {
                    let snapshot = reader.snapshot().await?;
                    snapshot.get(b"key03").await
                }
                .await;
                match result {
                    Ok(value) => {
                        assert_eq!(value, Some(b"value3".to_vec()));
                        ok += 1;
                    }
                    Err(e) => {
                        assert!(e.is_retryable(), "{e}");
                        retryable += 1;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
            (ok, retryable)
        }
    });
    let out = fdbcli(&fdb, "configure commit_proxies=2 grv_proxies=2").await;
    println!("fdbcli: {out:?}");
    let after = within("new proxies", monitor.proxies_changed(&before))
        .await
        .expect("monitor running");
    println!(
        "after the recovery: {} GRV / {} commit proxies",
        after.grv_proxies.len(),
        after.commit_proxies.len()
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let (ok, retryable) = within("background reads", background).await.unwrap();
    println!("reads during the recovery: {ok} ok, {retryable} retryable errors");
    assert!(ok > 0);
    let out = fdbcli(&fdb, "writemode on; set after recovery").await;
    assert!(out.contains("Committed"), "{out}");
    let v2 = within("GRV after the recovery", reader.get_read_version())
        .await
        .expect("GRV");
    assert!(v2 > v1, "{v2} > {v1}");
    assert_eq!(
        within("get after the recovery", reader.get(b"after", v2))
            .await
            .unwrap(),
        Some(b"recovery".to_vec())
    );
    assert_eq!(
        within("get after the recovery", reader.get(b"key42", v2))
            .await
            .unwrap(),
        Some(b"value42".to_vec())
    );
    // The old version may be too old by now, or still readable: either answer is FDB's.
    match within("get at the old version", reader.get(b"key01", v1)).await {
        Ok(value) => assert_eq!(value, Some(b"value1".to_vec())),
        Err(e) => {
            println!("read at {v1}: {e}");
            assert!(e.is_retryable(), "{e}");
        }
    }

    drop(reader);
    monitor.shutdown().await;
    peers.shutdown().await;
    tokio::task::spawn_blocking(move || drop(fdb))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_priorities_and_metadata_version() {
    use fdb_native_client::read::{METADATA_VERSION_KEY, Priority, ReadVersionOptions};

    let fdb = Arc::new(
        tokio::task::spawn_blocking(FdbContainer::start)
            .await
            .unwrap()
            .expect("container"),
    );
    println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
    let out = fdbcli(&fdb, "writemode on; set k v").await;
    assert!(out.contains("Committed"), "{out}");

    let rt = TokioRuntime::new();
    let peers = PeerManager::new(rt.clone(), PeerManagerConfig::default(), DiscardEvents);
    let record = ConnectionRecord::open(fdb.cluster_file()).expect("cluster file");
    let monitor = Arc::new(ClusterMonitor::start(
        rt.clone(),
        peers.clone(),
        record,
        ClusterMonitorConfig::default(),
    ));
    let reader = Reader::new(rt, peers.clone(), monitor.clone(), ReadConfig::default());

    // Every priority gets a read version (concurrently, so that they are batched by priority).
    let versions: Vec<_> = [Priority::Batch, Priority::Default, Priority::Immediate]
        .into_iter()
        .flat_map(|priority| (0..5).map(move |_| priority))
        .map(|priority| {
            let reader = reader.clone();
            tokio::spawn(async move {
                let mut options = ReadVersionOptions::default();
                options.priority = priority;
                reader.get_read_version_with(options).await
            })
        })
        .collect();
    for v in versions {
        assert!(within("GRV", v).await.unwrap().unwrap() > 0);
    }

    // The metadata version the GRV proxy reports is what a storage server has for the key.
    let (version, from_proxy) = within(
        "GRV",
        reader.get_read_version_and_metadata(ReadVersionOptions::default()),
    )
    .await
    .unwrap();
    println!("metadataVersion at {version}: {from_proxy:?}");
    assert_eq!(
        reader.cached_metadata_version(version),
        Some(from_proxy.clone())
    );
    let from_storage = within("get", reader.get(METADATA_VERSION_KEY, version))
        .await
        .unwrap();
    assert_eq!(from_proxy, from_storage);
    assert_eq!(
        within("metadata", reader.metadata_version(version))
            .await
            .unwrap(),
        from_storage
    );
}
