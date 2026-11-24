//! The commit path against a real, disposable FDB server started by `fdb-native-testkit`
//! (image from `FDB_IMAGE`, default 7.4.10): sets, clears, clear ranges and atomic operations
//! committed through the [`Committer`] and read back with `fdbcli`, conflicts between two
//! transactions, blind writes and commits with nothing to write. Run with
//! `cargo nextest run --run-ignored only`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fdb_native_client::Error;
use fdb_native_client::cluster::{ClusterMonitor, ClusterMonitorConfig, ConnectionRecord};
use fdb_native_client::commit::{CommitBuffer, CommitOutcome};
use fdb_native_client::commit::{CommitOptions, Committer, CommitterConfig};
use fdb_native_schema::domain::{GetReadVersion, INVALID_VERSION, MutationType};
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_testkit::FdbContainer;
use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig};
use fdb_native_transport::runtime::TokioRuntime;
use fdb_native_wire::EncodeError;

struct Cluster {
    fdb: Arc<FdbContainer>,
    peers: PeerManager<TokioRuntime>,
    monitor: Arc<ClusterMonitor<TokioRuntime>>,
    committer: Committer<TokioRuntime>,
}

impl Cluster {
    async fn start() -> Cluster {
        let fdb = tokio::task::spawn_blocking(FdbContainer::start)
            .await
            .unwrap()
            .expect("container");
        println!("image {}, cluster {}", fdb.image(), fdb.cluster_string());
        let rt = TokioRuntime::new();
        let peers = PeerManager::new(rt.clone(), PeerManagerConfig::default(), DiscardEvents);
        let record = ConnectionRecord::open(fdb.cluster_file()).expect("cluster file");
        let monitor = Arc::new(ClusterMonitor::start(
            rt.clone(),
            peers.clone(),
            record,
            ClusterMonitorConfig::default(),
        ));
        tokio::time::timeout(Duration::from_secs(30), monitor.proxies(false))
            .await
            .expect("proxies within 30 s")
            .expect("monitor running");
        let committer = Committer::new(
            rt,
            peers.clone(),
            monitor.clone(),
            CommitterConfig::default(),
        );
        Cluster {
            fdb: Arc::new(fdb),
            peers,
            monitor,
            committer,
        }
    }

    fn schema(&self) -> SchemaVersion {
        self.monitor.schema_version().expect("an info")
    }

    /// One `GetReadVersionRequest` to the first GRV proxy (the read path, let.4, will have the
    /// real one).
    async fn read_version(&self) -> i64 {
        let info = self.monitor.current().unwrap();
        let endpoint = info.grv_proxies[0].streams.get_consistent_read_version;
        let request = GetReadVersion {
            transaction_count: 1,
            flags: GetReadVersion::PRIORITY_DEFAULT,
            tags: BTreeMap::new(),
            debug_id: None,
            span_context: Default::default(),
            max_version: INVALID_VERSION,
            max_grv_queue_delay_ms: None,
        };
        let peer = self.peers.peer_for_endpoint(&endpoint);
        let reply = tokio::time::timeout(
            Duration::from_secs(10),
            peer.request(endpoint.token, move |reply, version, out| {
                let schema = SchemaVersion::for_protocol_version(version)
                    .ok_or(EncodeError::Invalid("unsupported version"))?;
                request.encode_request_into(schema, reply, out)
            }),
        )
        .await
        .expect("GRV reply within 10 s")
        .expect("GRV reply");
        let schema = SchemaVersion::for_protocol_version(reply.version).unwrap();
        GetReadVersion::decode_reply(schema, &reply.payload)
            .expect("decodable")
            .expect("a read version")
            .version
    }

    async fn commit(
        &self,
        buffer: CommitBuffer,
        read_version: i64,
        options: &CommitOptions,
    ) -> Result<CommitOutcome, Error> {
        tokio::time::timeout(
            Duration::from_secs(30),
            self.committer.commit_buffer(buffer, read_version, options),
        )
        .await
        .expect("commit within 30 s")
    }

    fn buffer(&self) -> CommitBuffer {
        CommitBuffer::new(self.committer.limits())
    }

    async fn cli(&self, command: &str) -> String {
        let fdb = self.fdb.clone();
        let command = command.to_owned();
        tokio::task::spawn_blocking(move || fdb.fdbcli(&command).expect("fdbcli"))
            .await
            .unwrap()
    }

    async fn finish(self) {
        self.monitor.shutdown().await;
        self.peers.shutdown().await;
        let Cluster { fdb, .. } = self;
        tokio::task::spawn_blocking(move || drop(fdb))
            .await
            .unwrap();
    }
}

fn opts(f: impl FnOnce(&mut CommitOptions)) -> CommitOptions {
    let mut o = CommitOptions::default();
    f(&mut o);
    o
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_commit_sets_clears_and_clear_ranges() {
    let c = Cluster::start().await;
    println!("schema {}", c.schema());

    // Blind writes: no reads, so the commit carries a self-conflict key.
    let rv = c.read_version().await;
    let mut b = c.buffer();
    for (k, v) in [
        ("t/a", "1"),
        ("t/b", "2"),
        ("t/c", "3"),
        ("t/d", "4"),
        ("t/e", "5"),
    ] {
        b.set(k.as_bytes(), v.as_bytes()).unwrap();
    }
    b.set(b"t/empty", b"").unwrap();
    let outcome = c.commit(b, rv, &CommitOptions::default()).await.unwrap();
    let v1 = outcome.version();
    println!(
        "committed at {v1} (read version {rv}), versionstamp {:?}",
        outcome.versionstamp()
    );
    assert!(v1 > rv);
    let out = c.cli("getrange t/ t0").await;
    println!("{out}");
    for line in [
        "`t/a' is `1'",
        "`t/b' is `2'",
        "`t/c' is `3'",
        "`t/d' is `4'",
        "`t/e' is `5'",
        "`t/empty' is `'",
    ] {
        assert!(out.contains(line), "{line} in {out}");
    }

    // Clear one key and a range, set one more.
    let rv = c.read_version().await;
    assert!(rv >= v1);
    let mut b = c.buffer();
    b.clear(b"t/b");
    b.clear_range(b"t/c", b"t/e").unwrap();
    b.set(b"t/f", b"6").unwrap();
    let v2 = c
        .commit(b, rv, &CommitOptions::default())
        .await
        .unwrap()
        .version();
    assert!(v2 > v1);
    let out = c.cli("getrange t/ t0").await;
    println!("{out}");
    assert!(out.contains("`t/a' is `1'"));
    assert!(
        out.contains("`t/e' is `5'"),
        "the clear range is end-exclusive"
    );
    assert!(out.contains("`t/f' is `6'"));
    for gone in ["t/b'", "t/c'", "t/d'"] {
        assert!(!out.contains(gone), "{gone} was cleared: {out}");
    }

    // Atomic operations (AddValue twice) and a lock-aware, first-in-batch, risky commit.
    for add in [1u64, 2] {
        let rv = c.read_version().await;
        let mut b = c.buffer();
        b.atomic_op(b"t/counter", &add.to_le_bytes(), MutationType::ADD_VALUE)
            .unwrap();
        let options = opts(|o| {
            o.lock_aware = true;
            o.first_in_batch = true;
            o.causal_write_risky = true;
        });
        c.commit(b, rv, &options).await.unwrap();
    }
    let out = c.cli("get t/counter").await;
    println!("{out}");
    assert!(out.contains(r"\x03\x00\x00\x00\x00\x00\x00\x00"), "{out}");

    // A clear range over everything.
    let rv = c.read_version().await;
    let mut b = c.buffer();
    b.clear_range(b"t/", b"t0").unwrap();
    c.commit(b, rv, &CommitOptions::default()).await.unwrap();
    let out = c.cli("getrange t/ t0").await;
    println!("{out}");
    assert!(!out.contains("`t/"), "{out}");
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_overlapping_conflict_ranges_give_not_committed() {
    let c = Cluster::start().await;
    println!("schema {}", c.schema());
    let rv = c.read_version().await;

    // Transaction A, at `rv`, reads `x` (a read conflict range) and writes it.
    let mut a = c.buffer();
    a.add_read_conflict_range(b"x", b"x\0").unwrap();
    a.set(b"x", b"from-a").unwrap();
    // Transaction B, also at `rv`, read the range [w, y) and writes `y`.
    let mut b = c.buffer();
    b.add_read_conflict_range(b"w", b"y").unwrap();
    b.add_read_conflict_range(b"p", b"q").unwrap();
    b.set(b"y", b"from-b").unwrap();
    // Transaction C reads only the unrelated range [p, q).
    let mut unrelated = c.buffer();
    unrelated.add_read_conflict_range(b"p", b"q").unwrap();
    unrelated.set(b"z", b"from-c").unwrap();

    let va = c
        .commit(a, rv, &CommitOptions::default())
        .await
        .unwrap()
        .version();
    assert!(va > rv);

    // B's read range [w, y) contains x, which A wrote after B's read version.
    let options = opts(|o| o.report_conflicting_keys = true);
    let e = c.commit(b.clone(), rv, &options).await.unwrap_err();
    println!("B: {e} / {e:?}");
    assert_eq!(e.code(), 1020);
    // Index 0 is [w, y) (the read conflict ranges are sent sorted by begin: [p, q),
    // [w, y), then the added range).
    let ranges = e.conflicting_ranges().expect("conflicting ranges");
    assert_eq!(ranges.len(), 1);
    assert_eq!(
        (&ranges[0].begin[..], &ranges[0].end[..]),
        (&b"w"[..], &b"y"[..])
    );
    assert!(e.is_retryable() && !e.is_maybe_committed());
    // Without reportConflictingKeys the same conflict has no ranges.
    let e = c
        .commit(b.clone(), rv, &CommitOptions::default())
        .await
        .unwrap_err();
    assert_eq!(e.code(), 1020);
    let out = c.cli("get y").await;
    assert!(out.contains("not found"), "B did not commit: {out}");

    // C's ranges do not overlap what A wrote, so it commits even at the old version.
    c.commit(unrelated, rv, &CommitOptions::default())
        .await
        .unwrap();

    // B, retried at a new read version (nothing changed [w, y) since), commits.
    let rv2 = c.read_version().await;
    let vb = c
        .commit(b, rv2, &CommitOptions::default())
        .await
        .unwrap()
        .version();
    assert!(vb > va);
    for line in ["`x' is `from-a'", "`y' is `from-b'", "`z' is `from-c'"] {
        let key = line.split('\'').next().unwrap().trim_start_matches('`');
        let out = c.cli(&format!("get {key}")).await;
        assert!(out.contains(line), "{line} in {out}");
    }

    // Blind writers (no read ranges; the self-conflict key is their only read range) do not
    // conflict with each other even at the same old read version.
    let mut w1 = c.buffer();
    w1.set(b"blind", b"1").unwrap();
    let mut w2 = c.buffer();
    w2.set(b"blind", b"2").unwrap();
    c.commit(w1, rv2, &CommitOptions::default()).await.unwrap();
    c.commit(w2, rv2, &CommitOptions::default()).await.unwrap();
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn docker_blind_and_empty_commits() {
    let c = Cluster::start().await;
    let rv = c.read_version().await;

    // Nothing to commit: no request, no version.
    let outcome = c
        .commit(c.buffer(), rv, &CommitOptions::default())
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::NothingToCommit);
    let mut reads_only = c.buffer();
    reads_only.add_read_conflict_range(b"a", b"b").unwrap();
    assert_eq!(
        c.commit(reads_only, rv, &CommitOptions::default())
            .await
            .unwrap(),
        CommitOutcome::NothingToCommit
    );

    // A write conflict range and no mutation commits (it has something to write).
    let mut ranges_only = c.buffer();
    ranges_only.add_write_conflict_range(b"a", b"b").unwrap();
    let id = c
        .commit(ranges_only, rv, &CommitOptions::default())
        .await
        .unwrap();
    assert!(id.version() > rv);

    // A blind write with CAUSAL_WRITE_RISKY (no self-conflict key either).
    let mut blind = c.buffer();
    blind.set(b"blind/risky", b"yes").unwrap();
    let id = c
        .commit(blind, rv, &opts(|o| o.causal_write_risky = true))
        .await
        .unwrap();
    assert!(id.version() > rv);
    let out = c.cli("get blind/risky").await;
    assert!(out.contains("`blind/risky' is `yes'"), "{out}");

    // Commit on the first proxy.
    let mut b = c.buffer();
    b.set(b"blind/first", b"yes").unwrap();
    c.commit(b, rv, &opts(|o| o.commit_on_first_proxy = true))
        .await
        .unwrap();
    let out = c.cli("get blind/first").await;
    assert!(out.contains("`blind/first' is `yes'"), "{out}");

    // Limits are enforced before anything is sent.
    let mut big = c.buffer();
    assert_eq!(big.set(&vec![b'k'; 10_001], b"v").unwrap_err().code(), 2102);
    assert_eq!(big.set(b"k", &vec![0u8; 100_001]).unwrap_err().code(), 2103);
    c.finish().await;
}
