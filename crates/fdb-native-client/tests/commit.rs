//! The committer against scripted fake commit and GRV proxies on an in-memory network under
//! tokio's paused clock, with a real `ClusterMonitor` fed by a fake coordinator: what is sent,
//! success, conflicts, server errors, a connection lost after the send (unknown result and the
//! dummy transaction), a connection that fails before the handshake, proxies changing while the
//! commit is outstanding, and the client-side limits.

mod support;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_client::commit::{CommitBuffer, CommitError, CommitOutcome};
use fdb_native_client::{
    ClusterMonitor, ClusterMonitorConfig, CommitOptions, Committer, CommitterConfig,
    ConnectionRecord,
};
use fdb_native_schema::domain::{
    Commit, CommitId, GetReadVersion, KeyRange, Mutation, MutationType, ReadVersion,
};
use fdb_native_schema::{Request, SchemaVersion, self_conflict_key};
use fdb_native_transport::connection::{PING_REPLY_OK, PingRequest, well_known};
use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig};
use fdb_native_transport::{Checksum, ConnectPacket, FrameConfig, FrameDecoder};
use fdb_native_wire::{FdbError, ProtocolVersion, Uid};
use support::{FakeCoordinator, MemRuntime, Net, info, sock};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::mpsc;

// ---- a fake proxy -------------------------------------------------------------------------

enum Cmd {
    Send(Uid, Vec<u8>),
    Close,
}

/// A request that reached a fake proxy, as raw bytes.
struct Incoming {
    token: Uid,
    payload: Vec<u8>,
    schema: SchemaVersion,
    conn: mpsc::UnboundedSender<Cmd>,
}

impl Incoming {
    fn commit(&self) -> (Commit, Uid) {
        Commit::decode_request(self.schema, &self.payload).expect("a CommitTransactionRequest")
    }

    fn grv(&self) -> (GetReadVersion, Uid) {
        GetReadVersion::decode_request(self.schema, &self.payload).expect("a GetReadVersion")
    }

    fn reply_commit(&self, reply: Result<CommitId, u16>) {
        let (_, token) = self.commit();
        let reply = reply.map_err(FdbError::new);
        let payload = Commit::encode_reply(self.schema, &reply).unwrap();
        let _ = self.conn.send(Cmd::Send(token, payload));
    }

    fn reply_grv(&self, version: i64) {
        let (_, token) = self.grv();
        let rv = ReadVersion {
            process_busy_time: 0,
            version,
            locked: false,
            metadata_version: None,
            tag_throttle_info: Default::default(),
            mid_shard_size: 0,
            rk_default_throttled: false,
            rk_batch_throttled: false,
            ss_version_vector_delta: Default::default(),
            proxy_id: Uid::new(9, 9),
            proxy_tag_throttled_duration: 0.0,
        };
        let payload = GetReadVersion::encode_reply(self.schema, &Ok(rv)).unwrap();
        let _ = self.conn.send(Cmd::Send(token, payload));
    }

    /// Hangs up the connection the request came on.
    fn close(&self) {
        let _ = self.conn.send(Cmd::Close);
    }
}

struct FakeProxy {
    addr: SocketAddr,
    rx: mpsc::UnboundedReceiver<Incoming>,
    /// Hang up on every new connection before the handshake.
    hang_up: Arc<Mutex<bool>>,
}

impl FakeProxy {
    fn start(net: &Arc<Net>, addr: SocketAddr, version: ProtocolVersion) -> FakeProxy {
        let mut listener = net.listen(addr);
        let (tx, rx) = mpsc::unbounded_channel();
        let hang_up = Arc::new(Mutex::new(false));
        let hang = hang_up.clone();
        tokio::spawn(async move {
            while let Some(stream) = listener.recv().await {
                if *hang.lock().unwrap() {
                    drop(stream);
                    continue;
                }
                tokio::spawn(serve(stream, addr, version, tx.clone()));
            }
        });
        FakeProxy { addr, rx, hang_up }
    }

    fn hang_up(&self, on: bool) {
        *self.hang_up.lock().unwrap() = on;
    }

    async fn next(&mut self) -> Incoming {
        tokio::time::timeout(Duration::from_secs(60), self.rx.recv())
            .await
            .unwrap_or_else(|_| panic!("no request reached {} within 60 s", self.addr))
            .expect("proxy running")
    }

    fn try_next(&mut self) -> Option<Incoming> {
        self.rx.try_recv().ok()
    }
}

async fn serve(
    mut stream: DuplexStream,
    listen: SocketAddr,
    version: ProtocolVersion,
    incoming: mpsc::UnboundedSender<Incoming>,
) {
    let mut hello = [0u8; ConnectPacket::SIZE];
    if stream.read_exact(&mut hello).await.is_err() {
        return;
    }
    let (client, _) = ConnectPacket::decode(&hello).expect("client's ConnectPacket");
    let packet = ConnectPacket::server(version, listen);
    if stream.write_all(&packet.encode().unwrap()).await.is_err() {
        return;
    }
    if !client.protocol_version.is_compatible(version) {
        return;
    }
    let schema = SchemaVersion::for_protocol_version(version).expect("a supported version");
    let config = FrameConfig::new(Checksum::for_tls(false));
    let mut decoder = FrameDecoder::new(config);
    let (cmd_tx, mut cmds) = mpsc::unbounded_channel();
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        tokio::select! {
            read = stream.read(&mut buf) => {
                let Ok(n) = read else { return };
                if n == 0 {
                    return;
                }
                decoder.push(&buf[..n]);
                while let Some(frame) = decoder.next_frame().expect("client frames") {
                    let frame = frame.into_owned();
                    if frame.token == well_known::PING_PACKET {
                        let ping: PingRequest =
                            fdb_native_wire::decode(&frame.payload).expect("ping");
                        let payload = fdb_native_wire::encode(&PING_REPLY_OK).unwrap();
                        if write_frame(&mut stream, config, ping.reply.token(), &payload).await.is_err() {
                            return;
                        }
                    } else {
                        let _ = incoming.send(Incoming {
                            token: frame.token,
                            payload: frame.payload.to_vec(),
                            schema,
                            conn: cmd_tx.clone(),
                        });
                    }
                }
            }
            cmd = cmds.recv() => match cmd {
                None | Some(Cmd::Close) => return,
                Some(Cmd::Send(token, payload)) => {
                    if write_frame(&mut stream, config, token, &payload).await.is_err() {
                        return;
                    }
                }
            },
        }
    }
}

async fn write_frame(
    stream: &mut DuplexStream,
    config: FrameConfig,
    token: Uid,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut out = Vec::new();
    config
        .encode_frame(token, payload, &mut out)
        .expect("frame");
    stream.write_all(&out).await
}

// ---- the environment ----------------------------------------------------------------------

struct Env {
    committer: Arc<Committer<MemRuntime>>,
    coord: FakeCoordinator,
    monitor: Arc<ClusterMonitor<MemRuntime>>,
    commit_proxies: Vec<FakeProxy>,
    grv_proxies: Vec<FakeProxy>,
    schema: SchemaVersion,
}

fn version_of(schema: SchemaVersion) -> ProtocolVersion {
    schema.protocol_version()
}

/// A cluster of `schema`'s release with `commit` commit proxies and `grv` GRV proxies, and a
/// committer over it.
async fn env(schema: SchemaVersion, commit: usize, grv: usize) -> Env {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let mut coord = FakeCoordinator::start(&net, sock(1, 4500), version_of(schema));
    let commit_proxies = (0..commit)
        .map(|i| FakeProxy::start(&net, sock(150 + i as u8, 4500), version_of(schema)))
        .collect();
    let grv_proxies = (0..grv)
        .map(|i| FakeProxy::start(&net, sock(100 + i as u8, 4500), version_of(schema)))
        .collect();
    let mut config = PeerManagerConfig::default().without_jitter();
    config.supported_versions = vec![ProtocolVersion::V7_4, ProtocolVersion::V8_0];
    config.seed = Some(1);
    let peers = PeerManager::new(rt.clone(), config, DiscardEvents);
    let mut mconfig = ClusterMonitorConfig::default();
    mconfig.seed = Some(7);
    let monitor = Arc::new(ClusterMonitor::start(
        rt.clone(),
        peers.clone(),
        ConnectionRecord::memory("test:abc@10.0.0.1:4500".parse().unwrap()),
        mconfig,
    ));
    coord.next_call().await.reply(&info(1, grv, commit, schema));
    monitor.proxies(false).await.unwrap();
    let mut cconfig = CommitterConfig::default();
    cconfig.seed = Some(3);
    let committer = Arc::new(Committer::new(rt, peers, monitor.clone(), cconfig));
    Env {
        committer,
        coord,
        monitor,
        commit_proxies,
        grv_proxies,
        schema,
    }
}

type CommitResult = Result<CommitOutcome, CommitError>;

fn spawn_commit(
    committer: &Arc<Committer<MemRuntime>>,
    read_version: i64,
    buffer: CommitBuffer,
    options: CommitOptions,
) -> tokio::task::JoinHandle<CommitResult> {
    let committer = committer.clone();
    tokio::spawn(async move {
        committer
            .commit_buffer(buffer, read_version, &options)
            .await
    })
}

async fn done(handle: tokio::task::JoinHandle<CommitResult>) -> CommitResult {
    tokio::time::timeout(Duration::from_secs(120), handle)
        .await
        .expect("the commit finished within 120 s")
        .unwrap()
}

fn buffer(env: &Env) -> CommitBuffer {
    CommitBuffer::new(env.committer.limits())
}

/// Which commit proxy gets a request: polls them all.
async fn next_commit(proxies: &mut [FakeProxy]) -> (usize, Incoming) {
    for _ in 0..6000 {
        for (i, p) in proxies.iter_mut().enumerate() {
            if let Some(call) = p.try_next() {
                return (i, call);
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no request within 60 s");
}

fn opts(f: impl FnOnce(&mut CommitOptions)) -> CommitOptions {
    let mut o = CommitOptions::default();
    f(&mut o);
    o
}

const SCHEMAS: [SchemaVersion; 2] = [SchemaVersion::V7_4, SchemaVersion::V8_0];

// ---- tests ----------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn request_contents_and_success() {
    for schema in SCHEMAS {
        let mut env = env(schema, 1, 1).await;
        let mut b = buffer(&env);
        b.set(b"k1", b"v1").unwrap();
        b.clear(b"k2");
        b.clear_range(b"a", b"c").unwrap();
        b.add_read_conflict_range(b"x", b"y").unwrap();
        let options = opts(|o| {
            o.lock_aware = true;
            o.report_conflicting_keys = true;
        });
        let handle = spawn_commit(&env.committer, 4242, b, options);
        let call = env.commit_proxies[0].next().await;
        let (commit, _) = call.commit();
        assert_eq!(
            call.token,
            env.monitor.current().unwrap().commit_proxies[0]
                .streams
                .commit
                .token
        );
        assert_eq!(commit.read_snapshot, 4242);
        assert_eq!(
            commit.mutations,
            vec![
                Mutation::new(MutationType::SET_VALUE, "k1", "v1").unwrap(),
                Mutation::new(MutationType::CLEAR_RANGE, "k2", "k2\0").unwrap(),
                Mutation::new(MutationType::CLEAR_RANGE, "a", "c").unwrap(),
            ]
        );
        // The writes' ranges, sorted by `begin` (C++'s `intersects` sorts the request's lists in
        // place), then (no read range intersects them) the self-conflict key as both a read and
        // a write range.
        assert_eq!(commit.write_conflict_ranges.len(), 4);
        assert_eq!(
            &commit.write_conflict_ranges[..3],
            &[
                KeyRange::new("a", "c").unwrap(),
                KeyRange::single_key(b"k1"),
                KeyRange::single_key(b"k2"),
            ]
        );
        let sc = commit.write_conflict_ranges[3].clone();
        assert_eq!(sc.begin.len(), 21);
        assert!(sc.begin.starts_with(b"\xff/SC/"));
        assert_eq!(sc, KeyRange::single_key(&sc.begin));
        assert_eq!(
            commit.read_conflict_ranges,
            vec![KeyRange::new("x", "y").unwrap(), sc]
        );
        assert_eq!(commit.flags, Commit::FLAG_IS_LOCK_AWARE);
        assert!(commit.report_conflicting_keys);
        assert!(!commit.lock_aware);

        call.reply_commit(Ok(CommitId {
            version: 777,
            txn_batch_id: 5,
            ..CommitId::default()
        }));
        let outcome = done(handle).await.unwrap();
        let id = outcome.commit_id().unwrap();
        assert_eq!((id.version, id.txn_batch_id), (777, 5));
        assert_eq!(outcome.version(), 777);
        assert_eq!(outcome.versionstamp(), Some([0, 0, 0, 0, 0, 0, 3, 9, 0, 5]));
        assert!(env.commit_proxies[0].try_next().is_none());
        assert!(env.grv_proxies[0].try_next().is_none());
        let _ = env.schema;
    }
}

#[tokio::test(start_paused = true)]
async fn an_intersecting_read_range_adds_no_self_conflict() {
    let mut env = env(SchemaVersion::V7_4, 1, 1).await;
    let mut b = buffer(&env);
    b.set(b"m", b"v").unwrap();
    b.add_read_conflict_range(b"a", b"z").unwrap();
    let handle = spawn_commit(&env.committer, 10, b, CommitOptions::default());
    let call = env.commit_proxies[0].next().await;
    let (commit, _) = call.commit();
    assert_eq!(
        commit.read_conflict_ranges,
        vec![KeyRange::new("a", "z").unwrap()]
    );
    assert_eq!(
        commit.write_conflict_ranges,
        vec![KeyRange::single_key(b"m")]
    );
    call.reply_commit(Ok(CommitId {
        version: 11,
        ..CommitId::default()
    }));
    done(handle).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn nothing_to_commit_sends_nothing() {
    let mut env = env(SchemaVersion::V8_0, 1, 1).await;
    let mut b = buffer(&env);
    b.add_read_conflict_range(b"a", b"b").unwrap();
    let outcome = env
        .committer
        .commit_buffer(b, 5, &CommitOptions::default())
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::NothingToCommit);
    assert_eq!(outcome.version(), -1);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(env.commit_proxies[0].try_next().is_none());
}

#[tokio::test(start_paused = true)]
async fn conflicts() {
    for schema in SCHEMAS {
        let mut env = env(schema, 1, 1).await;

        // not_committed as an error reply.
        let mut b = buffer(&env);
        b.set(b"k", b"v").unwrap();
        let handle = spawn_commit(&env.committer, 1, b, CommitOptions::default());
        env.commit_proxies[0].next().await.reply_commit(Err(1020));
        let e = done(handle).await.unwrap_err();
        assert_eq!(
            e,
            CommitError::NotCommitted {
                conflicting_ranges: None
            }
        );
        assert_eq!(e.code(), 1020);
        assert!(e.is_retryable() && !e.maybe_committed());

        // An invalid version with the conflicting read ranges (report_conflicting_keys).
        let mut b = buffer(&env);
        b.set(b"k", b"v").unwrap();
        b.add_read_conflict_range(b"a", b"b").unwrap();
        b.add_read_conflict_range(b"c", b"d").unwrap();
        b.add_read_conflict_range(b"e", b"f").unwrap();
        let options = opts(|o| {
            o.report_conflicting_keys = true;
        });
        let handle = spawn_commit(&env.committer, 1, b, options);
        env.commit_proxies[0]
            .next()
            .await
            .reply_commit(Ok(CommitId {
                version: -1,
                conflicting_kr_indices: Some(vec![2, 0, 2, 99]),
                ..CommitId::default()
            }));
        match done(handle).await.unwrap_err() {
            CommitError::NotCommitted { conflicting_ranges } => assert_eq!(
                conflicting_ranges.unwrap(),
                vec![
                    KeyRange::new("a", "b").unwrap(),
                    KeyRange::new("e", "f").unwrap()
                ]
            ),
            e => panic!("{e:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn server_errors_are_mapped_and_not_resent() {
    let mut env = env(SchemaVersion::V7_4, 1, 1).await;
    type Check = fn(&CommitError) -> bool;
    let cases: [(u16, Check); 4] = [
        (1007u16, |e: &CommitError| {
            *e == CommitError::TransactionTooOld
        }),
        (1038, |e: &CommitError| {
            matches!(e, CommitError::Server(f) if f.code == 1038) && e.is_retryable()
        }),
        (1042, |e: &CommitError| e.code() == 1042 && e.is_retryable()),
        (2101, |e: &CommitError| {
            e.code() == 2101 && !e.is_retryable()
        }),
    ];
    for (code, check) in cases {
        let mut b = buffer(&env);
        b.set(b"k", b"v").unwrap();
        let handle = spawn_commit(&env.committer, 1, b, CommitOptions::default());
        env.commit_proxies[0].next().await.reply_commit(Err(code));
        let e = done(handle).await.unwrap_err();
        assert!(check(&e), "{code}: {e:?}");
        assert!(e.is_safe_to_retry());
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(env.commit_proxies[0].try_next().is_none(), "no resend");
        assert!(
            env.grv_proxies[0].try_next().is_none(),
            "no dummy transaction"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_lost_connection_after_the_send_is_an_unknown_result_never_resent() {
    for schema in SCHEMAS {
        let mut env = env(schema, 1, 1).await;
        let mut b = buffer(&env);
        b.set(b"k", b"v").unwrap();
        let options = opts(|o| {
            o.causal_write_risky = true;
        });
        let handle = spawn_commit(&env.committer, 1, b, options);
        env.commit_proxies[0].next().await.close();
        let e = done(handle).await.unwrap_err();
        assert_eq!(e, CommitError::CommitUnknownResult);
        assert_eq!(e.code(), 1021);
        assert!(e.maybe_committed());
        // Causal write risky: no dummy transaction, and the request is not sent again.
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(env.commit_proxies[0].try_next().is_none());
        assert!(env.grv_proxies[0].try_next().is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn broken_promise_and_commit_unknown_result_replies_are_unknown_results() {
    for code in [1100u16, 1021, 1030] {
        // (A `broken_promise` marks the endpoint failed, so each code gets its own cluster.)
        let mut env = env(SchemaVersion::V7_4, 1, 1).await;
        let mut b = buffer(&env);
        b.set(b"k", b"v").unwrap();
        let options = opts(|o| {
            o.causal_write_risky = true;
        });
        let handle = spawn_commit(&env.committer, 1, b, options);
        env.commit_proxies[0].next().await.reply_commit(Err(code));
        assert_eq!(
            done(handle).await.unwrap_err(),
            CommitError::CommitUnknownResult,
            "{code}"
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(env.commit_proxies[0].try_next().is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn an_unknown_result_commits_a_dummy_transaction_first() {
    for schema in SCHEMAS {
        let mut env = env(schema, 1, 1).await;
        let mut b = buffer(&env);
        b.set(b"k", b"v").unwrap();
        let handle = spawn_commit(&env.committer, 100, b, CommitOptions::default());
        let call = env.commit_proxies[0].next().await;
        let (original, _) = call.commit();
        let sc = original.read_conflict_ranges[0].clone();
        call.close();

        // The dummy transaction: a causal-risky read version, then a lock-aware commit of a read
        // and a write conflict range on the single key in the intersection, with no mutation.
        let grv = env.grv_proxies[0].next().await;
        let (request, _) = grv.grv();
        assert_eq!(
            request.flags & GetReadVersion::FLAG_CAUSAL_READ_RISKY,
            GetReadVersion::FLAG_CAUSAL_READ_RISKY
        );
        assert!(!handle.is_finished());
        grv.reply_grv(900);
        let dummy_call = env.commit_proxies[0].next().await;
        let (dummy, _) = dummy_call.commit();
        assert!(dummy.mutations.is_empty());
        assert_eq!(dummy.read_snapshot, 900);
        assert_eq!(
            dummy.read_conflict_ranges,
            vec![KeyRange::single_key(&sc.begin)]
        );
        assert_eq!(dummy.write_conflict_ranges, dummy.read_conflict_ranges);
        assert_eq!(dummy.flags, Commit::FLAG_IS_LOCK_AWARE);
        assert!(!handle.is_finished(), "waits for the dummy transaction");
        // A conflict of the dummy is retried, with a new read version.
        dummy_call.reply_commit(Err(1020));
        let grv = env.grv_proxies[0].next().await;
        grv.reply_grv(950);
        let dummy_call = env.commit_proxies[0].next().await;
        assert_eq!(dummy_call.commit().0.read_snapshot, 950);
        dummy_call.reply_commit(Ok(CommitId {
            version: 960,
            ..CommitId::default()
        }));
        assert_eq!(
            done(handle).await.unwrap_err(),
            CommitError::CommitUnknownResult
        );
        assert!(env.commit_proxies[0].try_next().is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn a_failure_before_the_handshake_is_not_sent() {
    for schema in SCHEMAS {
        let mut env = env(schema, 1, 1).await;
        env.commit_proxies[0].hang_up(true);
        let mut b = buffer(&env);
        b.set(b"k", b"v").unwrap();
        let e = done(spawn_commit(&env.committer, 1, b, CommitOptions::default()))
            .await
            .unwrap_err();
        assert!(matches!(e, CommitError::NotSent(_)), "{e:?}");
        assert_eq!(e.code(), 1026);
        assert!(e.is_safe_to_retry() && !e.maybe_committed());
        assert!(env.commit_proxies[0].try_next().is_none());
        assert!(
            env.grv_proxies[0].try_next().is_none(),
            "no dummy transaction"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_proxy_that_fails_before_the_handshake_is_skipped() {
    let mut env = env(SchemaVersion::V7_4, 3, 1).await;
    env.commit_proxies[0].hang_up(true);
    env.commit_proxies[1].hang_up(true);
    for round in 0..6 {
        let mut b = buffer(&env);
        b.set(b"k", b"v").unwrap();
        let handle = spawn_commit(&env.committer, 1, b, CommitOptions::default());
        let (which, call) = next_commit(&mut env.commit_proxies).await;
        assert_eq!(which, 2, "round {round}");
        call.reply_commit(Ok(CommitId {
            version: 10 + round,
            ..CommitId::default()
        }));
        assert_eq!(done(handle).await.unwrap().version(), 10 + round);
        assert!(
            env.commit_proxies
                .iter_mut()
                .all(|p| p.try_next().is_none())
        );
    }
}

#[tokio::test(start_paused = true)]
async fn proxies_changing_mid_commit_is_an_unknown_result() {
    for schema in SCHEMAS {
        let mut env = env(schema, 1, 1).await;
        let mut b = buffer(&env);
        b.set(b"k", b"v").unwrap();
        let options = opts(|o| {
            o.causal_write_risky = true;
        });
        let handle = spawn_commit(&env.committer, 1, b, options);
        let call = env.commit_proxies[0].next().await;

        // The same proxies in a new info (another id) change nothing.
        let long_poll = env.coord.next_call().await;
        let mut same = info(1, 1, 1, schema);
        same.id = Uid::new(0xdb, 50);
        long_poll.reply(&same);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!handle.is_finished());

        // New commit proxies (other tokens): the commit is abandoned.
        let long_poll = env.coord.next_call().await;
        long_poll.reply(&info(2, 1, 1, schema));
        let e = done(handle).await.unwrap_err();
        assert_eq!(e, CommitError::CommitUnknownResult);

        // A late reply to the abandoned request is ignored; it is not sent again.
        call.reply_commit(Ok(CommitId {
            version: 5,
            ..CommitId::default()
        }));
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(env.commit_proxies[0].try_next().is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn proxies_changing_mid_commit_runs_the_dummy_transaction_on_the_new_proxies() {
    let schema = SchemaVersion::V7_4;
    let mut env = env(schema, 1, 1).await;
    let mut b = buffer(&env);
    b.set(b"k", b"v").unwrap();
    let handle = spawn_commit(&env.committer, 100, b, CommitOptions::default());
    let call = env.commit_proxies[0].next().await;
    let (original, _) = call.commit();
    let old_token = call.token;
    let long_poll = env.coord.next_call().await;
    long_poll.reply(&info(2, 1, 1, schema));

    env.grv_proxies[0].next().await.reply_grv(300);
    let dummy_call = env.commit_proxies[0].next().await;
    assert_ne!(dummy_call.token, old_token, "the new proxy's endpoint");
    let (dummy, _) = dummy_call.commit();
    assert_eq!(dummy.read_snapshot, 300);
    assert_eq!(
        dummy.read_conflict_ranges,
        vec![KeyRange::single_key(
            &original.read_conflict_ranges[0].begin
        )]
    );
    dummy_call.reply_commit(Ok(CommitId {
        version: 301,
        ..CommitId::default()
    }));
    assert_eq!(
        done(handle).await.unwrap_err(),
        CommitError::CommitUnknownResult
    );
    drop(env.monitor.clone());
}

#[tokio::test(start_paused = true)]
async fn commit_on_first_proxy() {
    let mut env = env(SchemaVersion::V7_4, 3, 1).await;
    let first = env.monitor.current().unwrap().client_info.commit_proxies[0].base;
    for _ in 0..5 {
        let mut b = buffer(&env);
        b.set(b"k", b"v").unwrap();
        let options = opts(|o| {
            o.commit_on_first_proxy = true;
        });
        let handle = spawn_commit(&env.committer, 1, b, options);
        let (which, call) = next_commit(&mut env.commit_proxies).await;
        assert_eq!(call.token, first.token);
        assert_eq!(which, 0);
        call.reply_commit(Ok(CommitId {
            version: 2,
            ..CommitId::default()
        }));
        done(handle).await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn limits_are_checked_before_anything_is_sent() {
    let mut env = env(SchemaVersion::V7_4, 1, 1).await;
    let c = env.committer.clone();
    let defaults = CommitOptions::default();

    // Raw mutations are checked at commit as well as when buffered.
    let big_key = Mutation::new(MutationType::SET_VALUE, vec![b'k'; 10_001], "v").unwrap();
    let e = c
        .commit(1, vec![big_key], vec![], vec![], &defaults)
        .await
        .unwrap_err();
    assert_eq!(e.code(), 2102);

    let mut b = buffer(&env);
    assert_eq!(b.set(b"k", &vec![0; 100_001]).unwrap_err().code(), 2103);

    // 10 MB of values in 100 kB pieces: over the transaction limit.
    for i in 0..100u32 {
        b.set(&i.to_be_bytes(), &vec![0; 100_000]).unwrap();
    }
    let e = c.commit_buffer(b, 1, &defaults).await.unwrap_err();
    assert!(
        matches!(
            e,
            CommitError::TransactionTooLarge {
                limit: 10_000_000,
                ..
            }
        ),
        "{e:?}"
    );
    assert_eq!(e.code(), 2101);
    assert!(e.is_safe_to_retry());

    let mut b = buffer(&env);
    b.set(b"k", b"v").unwrap();
    let read_only = opts(|o| {
        o.read_only = true;
    });
    let e = c.commit_buffer(b, 1, &read_only).await.unwrap_err();
    assert_eq!(e.code(), 2023);

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(env.commit_proxies[0].try_next().is_none());
    // The self-conflict key is what schema builds.
    assert_eq!(self_conflict_key(Uid::new(1, 2)).unwrap().len(), 21);
}
