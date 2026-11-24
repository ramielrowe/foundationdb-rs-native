//! The storage metrics reads (`getEstimatedRangeSizeBytes`, `getRangeSplitPoints`) against a
//! scripted fake cluster (a coordinator, a GRV proxy, a commit proxy and storage servers on
//! the in-memory network) under tokio's paused clock, on both schema sets.
//!
//! The fake storage servers answer `WaitMetricsRequest` and `SplitRangeRequest` as
//! `serveStorageMetricsRequests` does: `wrong_shard_server` (as the envelope's error: neither
//! reply is a `LoadBalancedReply`) for a range they do not hold, otherwise from a "byte
//! sample" that counts every key's and value's bytes; split points are the keys at which
//! a chunk's bytes reach the chunk size. The commit proxy answers range location requests
//! from a shard map that the tests change under a client's cached locations.

#![cfg(feature = "tokio")]

mod support;

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_client::cluster::ConnectionRecord;
use fdb_native_client::error::codes;
use fdb_native_client::read::KEY_SPACE_END;
use fdb_native_client::{Database, DatabaseOptions, Transaction, TransactionOption};
use fdb_native_schema::domain::{
    ClientDbInfo, Commit, CommitId, CommitProxy, GetKeyServerLocations, GetReadVersion, GrvProxy,
    KeyRange, KeyServerLocations, LATEST_VERSION, ReadVersion, SplitRange, SplitRangeReply,
    StorageMetrics, StorageServer, TenantMode, WaitMetrics,
};
use fdb_native_schema::streams::STORAGE_SERVER_STREAMS;
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::peer::PeerManagerConfig;
use fdb_native_wire::{FdbError, ProtocolVersion, Uid, VersionVector};
use support::process::{Action, FakeProcess, Incoming};
use support::{FakeCoordinator, MemRuntime, Net, sock};

const SCHEMAS: [SchemaVersion; 2] = [SchemaVersion::V7_4, SchemaVersion::V8_0];
const LIMIT: Duration = Duration::from_secs(120);
const READ_VERSION: i64 = 1000;

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: no result within {LIMIT:?}"))
}

fn range(begin: &[u8], end: &[u8]) -> KeyRange {
    KeyRange {
        begin: begin.to_vec(),
        end: end.to_vec(),
    }
}

fn ks(list: &[&[u8]]) -> Vec<Vec<u8>> {
    list.iter().map(|k| k.to_vec()).collect()
}

type Data = Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>;

/// What one fake storage server does and received.
#[derive(Default)]
struct StorageState {
    /// The ranges it holds.
    owns: Vec<KeyRange>,
    /// Errors to answer the next requests with, in order.
    fail_next: VecDeque<u16>,
    /// The `WaitMetricsRequest`s received.
    wait_metrics: Vec<WaitMetrics>,
    /// The `SplitRangeRequest`s received.
    split_ranges: Vec<SplitRange>,
    /// How long replies take.
    delay: Duration,
}

impl StorageState {
    /// `isReadable(keys)`: one held range contains them.
    fn holds(&self, keys: &KeyRange) -> bool {
        self.owns
            .iter()
            .any(|r| r.begin <= keys.begin && keys.end <= r.end)
    }

    /// The error to answer with, if any.
    fn error(&mut self, keys: &KeyRange) -> Option<u16> {
        if let Some(code) = self.fail_next.pop_front() {
            return Some(code);
        }
        (!self.holds(keys)).then_some(codes::WRONG_SHARD_SERVER)
    }
}

/// The fake byte sample: the bytes of a key and its value.
fn size(k: &[u8], v: &[u8]) -> i64 {
    (k.len() + v.len()) as i64
}

fn bytes_in(data: &BTreeMap<Vec<u8>, Vec<u8>>, keys: &KeyRange) -> i64 {
    data.range(keys.begin.clone()..keys.end.clone())
        .map(|(k, v)| size(k, v))
        .sum()
}

/// The fake `getSplitPoints`: a key is a split point when the bytes from the previous one
/// (or the range's begin) up to it reach `chunk`; never the range's begin or end.
fn split_points(data: &BTreeMap<Vec<u8>, Vec<u8>>, keys: &KeyRange, chunk: i64) -> Vec<Vec<u8>> {
    let mut points = Vec::new();
    let mut acc = 0;
    for (k, v) in data.range(keys.begin.clone()..keys.end.clone()) {
        if acc >= chunk && *k != keys.begin {
            points.push(k.clone());
            acc = 0;
        }
        acc += size(k, v);
    }
    points
}

struct FakeStorage {
    _process: FakeProcess,
    server: StorageServer,
    state: Arc<Mutex<StorageState>>,
}

struct Env {
    db: Database<MemRuntime>,
    data: Data,
    storage: Vec<FakeStorage>,
    shards: Arc<Mutex<Vec<(KeyRange, usize)>>>,
    locations: Arc<Mutex<Vec<GetKeyServerLocations>>>,
    grv_requests: Arc<AtomicUsize>,
    /// The most shards the commit proxy returns for a range (`usize::MAX`: as asked).
    proxy_limit: Arc<AtomicUsize>,
    commits: Arc<Mutex<Vec<Commit>>>,
    _processes: Vec<FakeProcess>,
}

fn read_version_reply(version: i64) -> ReadVersion {
    ReadVersion {
        process_busy_time: 0,
        version,
        locked: false,
        metadata_version: None,
        tag_throttle_info: BTreeMap::new(),
        mid_shard_size: 0,
        rk_default_throttled: false,
        rk_batch_throttled: false,
        ss_version_vector_delta: VersionVector::default(),
        proxy_id: Uid::new(9, 9),
        proxy_tag_throttled_duration: 0.0,
    }
}

impl Env {
    /// A cluster with `n` storage servers; the first holds everything until
    /// [`layout`](Self::layout) says otherwise.
    async fn new(schema: SchemaVersion, n: usize) -> Env {
        let net = Net::new();
        let rt = MemRuntime::new(&net);
        let version: ProtocolVersion = schema.protocol_version();
        let mut coordinator = FakeCoordinator::start(&net, sock(1, 4500), version);
        let data: Data = Arc::new(Mutex::new(BTreeMap::new()));

        let storage: Vec<FakeStorage> = (0..n)
            .map(|i| {
                let process = FakeProcess::start(&net, sock(30 + i as u8, 4500), version);
                let server = StorageServer {
                    id: Uid::new(0x55, i as u64),
                    locality: BTreeMap::new(),
                    base: process.endpoint(Uid::new(0x5500 + i as u64, 0x30)),
                    tss_pair_id: None,
                    accepting_requests: true,
                };
                let state = Arc::new(Mutex::new(StorageState::default()));
                let token = server
                    .base
                    .adjusted(STORAGE_SERVER_STREAMS.wait_metrics)
                    .token;
                {
                    let state = state.clone();
                    let data = data.clone();
                    process.handle(token, move |incoming: Incoming| {
                        let (request, reply) =
                            WaitMetrics::decode_request(incoming.schema, &incoming.payload)
                                .unwrap();
                        let mut st = state.lock().unwrap();
                        st.wait_metrics.push(request.clone());
                        let answer = match st.error(&request.keys) {
                            Some(code) => Err(FdbError::new(code)),
                            None => Ok(StorageMetrics {
                                bytes: bytes_in(&data.lock().unwrap(), &request.keys),
                                ..StorageMetrics::default()
                            }),
                        };
                        let payload = WaitMetrics::encode_reply(incoming.schema, &answer).unwrap();
                        Action::Reply {
                            token: reply,
                            payload,
                            delay: st.delay,
                        }
                    });
                }
                let token = server
                    .base
                    .adjusted(STORAGE_SERVER_STREAMS.get_range_split_points)
                    .token;
                {
                    let state = state.clone();
                    let data = data.clone();
                    process.handle(token, move |incoming: Incoming| {
                        let (request, reply) =
                            SplitRange::decode_request(incoming.schema, &incoming.payload).unwrap();
                        let mut st = state.lock().unwrap();
                        st.split_ranges.push(request.clone());
                        let answer = match st.error(&request.keys) {
                            Some(code) => Err(FdbError::new(code)),
                            None => Ok(SplitRangeReply {
                                split_points: split_points(
                                    &data.lock().unwrap(),
                                    &request.keys,
                                    request.chunk_size,
                                ),
                            }),
                        };
                        let payload = SplitRange::encode_reply(incoming.schema, &answer).unwrap();
                        Action::Reply {
                            token: reply,
                            payload,
                            delay: st.delay,
                        }
                    });
                }
                FakeStorage {
                    _process: process,
                    server,
                    state,
                }
            })
            .collect();
        storage[0].state.lock().unwrap().owns = vec![range(b"", KEY_SPACE_END)];
        let shards = Arc::new(Mutex::new(vec![(range(b"", KEY_SPACE_END), 0)]));

        let grv_process = FakeProcess::start(&net, sock(10, 4500), version);
        let grv = GrvProxy {
            process_id: Some(b"grv".to_vec()),
            provisional: false,
            base: grv_process.endpoint(Uid::new(0x6001, 0x10)),
        };
        let grv_requests = Arc::new(AtomicUsize::new(0));
        {
            let grv_requests = grv_requests.clone();
            grv_process.handle(grv.base.token, move |incoming: Incoming| {
                grv_requests.fetch_add(1, Ordering::SeqCst);
                let (_, reply) =
                    GetReadVersion::decode_request(incoming.schema, &incoming.payload).unwrap();
                let payload = GetReadVersion::encode_reply(
                    incoming.schema,
                    &Ok(read_version_reply(READ_VERSION)),
                )
                .unwrap();
                Action::reply(reply, payload)
            });
        }

        let commit_process = FakeProcess::start(&net, sock(20, 4500), version);
        let commit = CommitProxy {
            process_id: Some(b"cp".to_vec()),
            provisional: false,
            base: commit_process.endpoint(Uid::new(0x7001, 0x20)),
            schema,
        };
        let locations = Arc::new(Mutex::new(Vec::new()));
        let proxy_limit = Arc::new(AtomicUsize::new(usize::MAX));
        {
            let locations = locations.clone();
            let proxy_limit = proxy_limit.clone();
            let shards = shards.clone();
            let servers: Vec<StorageServer> = storage.iter().map(|s| s.server.clone()).collect();
            let token = commit.endpoints().get_key_servers_locations.token;
            commit_process.handle(token, move |incoming: Incoming| {
                let (request, reply) =
                    GetKeyServerLocations::decode_request(incoming.schema, &incoming.payload)
                        .unwrap();
                locations.lock().unwrap().push(request.clone());
                let shards = shards.lock().unwrap();
                let end = request.end.clone().expect("a range location request");
                assert!(!request.reverse);
                let results = shards
                    .iter()
                    .filter(|(r, _)| r.end > request.begin && r.begin < end)
                    .take((request.limit as usize).min(proxy_limit.load(Ordering::SeqCst)))
                    .map(|(r, owner)| (r.clone(), vec![servers[*owner].clone()]))
                    .collect();
                let payload = GetKeyServerLocations::encode_reply(
                    incoming.schema,
                    &Ok(KeyServerLocations {
                        results,
                        tss_mapping: Vec::new(),
                        tag_mapping: Vec::new(),
                    }),
                )
                .unwrap();
                Action::reply(reply, payload)
            });
        }
        let commits = Arc::new(Mutex::new(Vec::new()));
        {
            let commits = commits.clone();
            commit_process.handle(commit.base.token, move |incoming: Incoming| {
                let (request, reply) =
                    Commit::decode_request(incoming.schema, &incoming.payload).unwrap();
                commits.lock().unwrap().push(request);
                let payload = Commit::encode_reply(
                    incoming.schema,
                    &Ok(CommitId {
                        version: 2000,
                        ..CommitId::default()
                    }),
                )
                .unwrap();
                Action::reply(reply, payload)
            });
        }

        let info = ClientDbInfo {
            id: Uid::new(0xdb, 1),
            grv_proxies: vec![grv],
            commit_proxies: vec![commit],
            forward: None,
            history: Vec::new(),
            cluster_id: Uid::new(0xc1, 0xd),
            cluster_type: 0,
            tenant_mode: match schema {
                SchemaVersion::V7_4 => Some(TenantMode::DISABLED),
                SchemaVersion::V8_0 => None,
            },
        };
        tokio::spawn(async move {
            let mut pending = Vec::new();
            loop {
                let Some(call) = coordinator.try_call() else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                };
                if call.request.known_client_info_id != info.id {
                    call.reply(&info);
                } else {
                    pending.push(call);
                }
            }
        });

        let mut options = DatabaseOptions::default();
        options.peers = PeerManagerConfig::default().without_jitter();
        options.peers.supported_versions = vec![ProtocolVersion::V7_4, ProtocolVersion::V8_0];
        options.peers.seed = Some(1);
        options.monitor.seed = Some(7);
        options.commit.seed = Some(3);
        options.read.seed = Some(11);
        let record = ConnectionRecord::memory("test:abc@10.0.0.1:4500".parse().unwrap());
        let db = Database::from_record(rt, record, options).expect("database");
        within("proxies", db.cluster_monitor().proxies(false))
            .await
            .unwrap();
        Env {
            db,
            data,
            storage,
            shards,
            locations,
            grv_requests,
            proxy_limit,
            commits,
            _processes: vec![grv_process, commit_process],
        }
    }

    /// Sets each key to a value of `value_len` bytes.
    fn set(&self, keys: &[&[u8]], value_len: usize) {
        let mut data = self.data.lock().unwrap();
        for k in keys {
            data.insert(k.to_vec(), vec![b'v'; value_len]);
        }
    }

    /// The shard map: `(begin, end, server)`; each server then holds exactly its shards.
    fn layout(&self, shards: &[(&[u8], &[u8], usize)]) {
        for (i, s) in self.storage.iter().enumerate() {
            s.state.lock().unwrap().owns = shards
                .iter()
                .filter(|(_, _, owner)| *owner == i)
                .map(|(b, e, _)| range(b, e))
                .collect();
        }
        *self.shards.lock().unwrap() = shards
            .iter()
            .map(|(b, e, owner)| (range(b, e), *owner))
            .collect();
    }

    fn wait_metrics(&self) -> Vec<WaitMetrics> {
        self.storage
            .iter()
            .flat_map(|s| s.state.lock().unwrap().wait_metrics.clone())
            .collect()
    }

    fn split_ranges(&self) -> Vec<SplitRange> {
        self.storage
            .iter()
            .flat_map(|s| s.state.lock().unwrap().split_ranges.clone())
            .collect()
    }

    fn clear_requests(&self) {
        for s in &self.storage {
            let mut st = s.state.lock().unwrap();
            st.wait_metrics.clear();
            st.split_ranges.clear();
        }
        self.locations.lock().unwrap().clear();
    }

    fn bytes(&self, keys: &KeyRange) -> i64 {
        bytes_in(&self.data.lock().unwrap(), keys)
    }

    async fn estimate(&self, begin: &[u8], end: &[u8]) -> i64 {
        within(
            "estimate",
            self.db
                .create_transaction()
                .get_estimated_range_size_bytes(begin, end),
        )
        .await
        .unwrap_or_else(|e| panic!("{begin:?}..{end:?}: {e}"))
    }

    async fn split(&self, begin: &[u8], end: &[u8], chunk: i64) -> Vec<Vec<u8>> {
        within(
            "split points",
            self.db
                .create_transaction()
                .get_range_split_points(begin, end, chunk),
        )
        .await
        .unwrap_or_else(|e| panic!("{begin:?}..{end:?}: {e}"))
    }
}

/// Keys `a`..`z`, one letter each.
fn letters() -> Vec<Vec<u8>> {
    (b'a'..=b'z').map(|c| vec![c]).collect()
}

const THREE_SHARDS: [(&[u8], &[u8], usize); 3] =
    [(b"", b"g", 0), (b"g", b"p", 1), (b"p", KEY_SPACE_END, 2)];

fn code<T: std::fmt::Debug>(r: Result<T, fdb_native_client::Error>) -> u16 {
    r.unwrap_err().code()
}

fn set_letters(env: &Env, value_len: usize) {
    let keys = letters();
    let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    env.set(&refs, value_len);
}

// ---- estimated size ----------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn estimate_sums_the_parts_of_every_shard() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 3).await;
        set_letters(&env, 9);
        env.layout(&THREE_SHARDS);
        let got = env.estimate(b"b", b"r").await;
        assert_eq!(got, env.bytes(&range(b"b", b"r")), "{schema}");
        assert_eq!(got, 16 * 10);
        // One request per shard, for its part of the range, as `doGetStorageMetrics` sends it.
        let mut sent = env.wait_metrics();
        sent.sort_by(|a, b| a.keys.cmp(&b.keys));
        let parts: Vec<KeyRange> = sent.iter().map(|w| w.keys.clone()).collect();
        assert_eq!(
            parts,
            vec![range(b"b", b"g"), range(b"g", b"p"), range(b"p", b"r")],
            "{schema}"
        );
        for w in &sent {
            assert_eq!(w, &WaitMetrics::immediate(w.keys.clone(), LATEST_VERSION));
        }
        // The location request: the whole range, every shard, at `latestVersion`.
        let locations = env.locations.lock().unwrap().clone();
        assert_eq!(locations.len(), 1);
        assert_eq!(locations[0].begin, b"b");
        assert_eq!(locations[0].end.as_deref(), Some(&b"r"[..]));
        assert_eq!(locations[0].limit, i32::MAX);
        assert_eq!(locations[0].min_tenant_version, LATEST_VERSION);
        // No read version was needed.
        assert_eq!(env.grv_requests.load(Ordering::SeqCst), 0);

        // Again: from the cache. The whole key space; system keys need no option.
        env.clear_requests();
        env.set(&[b"\xff/sys"], 3);
        let all = env.estimate(b"", b"\xff\xff\xff").await;
        assert_eq!(all, env.bytes(&range(b"", KEY_SPACE_END)));
        assert!(env.locations.lock().unwrap().len() <= 1);
    }
}

#[tokio::test(start_paused = true)]
async fn estimate_after_shards_moved_relocates_the_parts() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 3).await;
        set_letters(&env, 1);
        // Cache one shard on server 0, then split and move it.
        assert_eq!(
            env.estimate(b"a", b"z").await,
            env.bytes(&range(b"a", b"z"))
        );
        env.layout(&THREE_SHARDS);
        env.clear_requests();
        let got = env.estimate(b"c", b"x").await;
        assert_eq!(got, env.bytes(&range(b"c", b"x")), "{schema}");
        // The stale part was refused, its location invalidated and asked again (once for the
        // part, after `WRONG_SHARD_SERVER_DELAY`), and then every shard asked.
        let locations = env.locations.lock().unwrap().clone();
        assert_eq!(locations.len(), 1, "{locations:?}");
        assert_eq!(locations[0].begin, b"c");
        let parts: Vec<KeyRange> = env.wait_metrics().into_iter().map(|w| w.keys).collect();
        assert!(
            parts.contains(&range(b"c", b"x")),
            "the stale request: {parts:?}"
        );
        assert!(parts.contains(&range(b"g", b"p")), "{parts:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn estimate_retries_future_version_and_returns_other_errors() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        set_letters(&env, 1);
        env.storage[0]
            .state
            .lock()
            .unwrap()
            .fail_next
            .extend([codes::FUTURE_VERSION, codes::FUTURE_VERSION]);
        assert_eq!(
            env.estimate(b"a", b"z").await,
            env.bytes(&range(b"a", b"z"))
        );
        assert_eq!(env.wait_metrics().len(), 3);

        // Any other error is the read's.
        env.storage[0]
            .state
            .lock()
            .unwrap()
            .fail_next
            .push_back(codes::TRANSACTION_TOO_OLD);
        let e = within(
            "estimate",
            env.db
                .create_transaction()
                .get_estimated_range_size_bytes(b"a", b"z"),
        )
        .await
        .unwrap_err();
        assert_eq!(e.code(), codes::TRANSACTION_TOO_OLD, "{schema}");
    }
}

// ---- split points ------------------------------------------------------------------------------

/// What C++ returns: the begin, each shard's split points with the boundaries between
/// shards, the end.
fn expected_points(
    env: &Env,
    begin: &[u8],
    end: &[u8],
    chunk: i64,
    shards: &[(&[u8], &[u8], usize)],
) -> Vec<Vec<u8>> {
    let data = env.data.lock().unwrap();
    let mut out = vec![begin.to_vec()];
    for (i, (b, e, _)) in shards
        .iter()
        .filter(|(b, e, _)| *e > begin && *b < end)
        .enumerate()
    {
        let part = range((*b).max(begin), (*e).min(end));
        if i > 0 {
            out.push(part.begin.clone());
        }
        out.extend(split_points(&data, &part, chunk));
    }
    if out.last().map(Vec::as_slice) != Some(end) {
        out.push(end.to_vec());
    }
    out
}

#[tokio::test(start_paused = true)]
async fn split_points_cover_every_shard_with_its_boundaries() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 3).await;
        set_letters(&env, 9);
        env.layout(&THREE_SHARDS);
        for (begin, end, chunk) in [
            (&b"b"[..], &b"r"[..], 25),
            (b"a", b"z", 10),
            (b"a", b"z", 1_000_000),
            (b"h", b"k", 15),
            (b"", b"\xff", 50),
        ] {
            let points = env.split(begin, end, chunk).await;
            assert_eq!(
                points,
                expected_points(&env, begin, end, chunk, &THREE_SHARDS),
                "{schema} {begin:?}..{end:?} by {chunk}"
            );
            assert_eq!(points.first().map(Vec::as_slice), Some(begin));
            assert_eq!(points.last().map(Vec::as_slice), Some(end));
            assert!(points.windows(2).all(|w| w[0] < w[1]), "{points:?}");
        }
        // The shard boundaries are always there, even with a huge chunk.
        assert_eq!(
            env.split(b"b", b"r", 1 << 40).await,
            ks(&[b"b", b"g", b"p", b"r"])
        );
        // The requests: each shard's part, the chunk size, no limit (8.0's -1; 7.4 has no
        // field and decodes as -1); no read version was requested.
        for r in env.split_ranges() {
            assert_eq!(r.limit, -1);
            assert!(r.chunk_size > 0);
        }
        assert_eq!(env.grv_requests.load(Ordering::SeqCst), 0);
        let locations = env.locations.lock().unwrap().clone();
        assert!(locations.iter().all(|l| l.limit == 1_000_000), "TOO_MANY");
    }
}

#[tokio::test(start_paused = true)]
async fn split_points_after_shards_moved_start_over() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 3).await;
        set_letters(&env, 9);
        assert_eq!(
            env.split(b"a", b"z", 1 << 40).await,
            ks(&[b"a", b"z"]),
            "one shard"
        );
        env.layout(&THREE_SHARDS);
        env.clear_requests();
        let points = env.split(b"a", b"z", 30).await;
        assert_eq!(
            points,
            expected_points(&env, b"a", b"z", 30, &THREE_SHARDS),
            "{schema}"
        );
        assert_eq!(env.locations.lock().unwrap().len(), 1, "located once more");
        // A request with a cached location that a server answers with `wrong_shard_server`
        // was made first.
        assert!(
            env.split_ranges()
                .iter()
                .any(|r| r.keys == range(b"a", b"z"))
        );
    }
}

#[tokio::test(start_paused = true)]
async fn split_points_continue_where_a_location_reply_stopped() {
    // A proxy that returns fewer shards than asked for (one at a time): the points go on
    // from the last shard's end (8.0's loop), with that boundary in them.
    for schema in SCHEMAS {
        let env = Env::new(schema, 3).await;
        set_letters(&env, 9);
        env.layout(&THREE_SHARDS);
        env.proxy_limit.store(1, Ordering::SeqCst);
        let points = env.split(b"a", b"z", 30).await;
        assert_eq!(
            points,
            expected_points(&env, b"a", b"z", 30, &THREE_SHARDS),
            "{schema}"
        );
        let begins: Vec<Vec<u8>> = env
            .locations
            .lock()
            .unwrap()
            .iter()
            .map(|l| l.begin.clone())
            .collect();
        assert_eq!(begins, ks(&[b"a", b"g", b"p"]));
        // The estimate asks for every shard at once, so it needs every shard in the reply
        // (C++ makes no second request either): with the cache filled, it is right.
        assert_eq!(
            env.estimate(b"a", b"z").await,
            env.bytes(&range(b"a", b"z"))
        );
    }
}

#[tokio::test(start_paused = true)]
async fn split_points_return_errors_other_than_wrong_shard() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        set_letters(&env, 9);
        env.storage[0]
            .state
            .lock()
            .unwrap()
            .fail_next
            .push_back(codes::ALL_ALTERNATIVES_FAILED);
        assert_eq!(
            env.split(b"a", b"z", 1 << 40).await,
            ks(&[b"a", b"z"]),
            "all_alternatives_failed is retried"
        );
        env.storage[0]
            .state
            .lock()
            .unwrap()
            .fail_next
            .push_back(codes::FUTURE_VERSION);
        let e = within(
            "split points",
            env.db
                .create_transaction()
                .get_range_split_points(b"a", b"z", 100),
        )
        .await
        .unwrap_err();
        assert_eq!(e.code(), codes::FUTURE_VERSION, "{schema}");
    }
}

// ---- the transaction ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn transaction_checks() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        set_letters(&env, 1);
        let tx = env.db.create_transaction();
        let est = |tx: &Transaction<MemRuntime>, b: &[u8], e: &[u8]| {
            let f = tx.get_estimated_range_size_bytes(b, e);
            async move { within("estimate", f).await }
        };
        let split = |tx: &Transaction<MemRuntime>, b: &[u8], e: &[u8], c: i64| {
            let f = tx.get_range_split_points(b, e, c);
            async move { within("split", f).await }
        };
        // Inverted ranges (fdb_c's KeyRangeRef), before anything else.
        assert_eq!(code(est(&tx, b"b", b"a").await), codes::INVERTED_RANGE);
        assert_eq!(
            code(split(&tx, b"b", b"a", 10).await),
            codes::INVERTED_RANGE
        );
        // Empty ranges: no request.
        env.clear_requests();
        assert_eq!(est(&tx, b"k", b"k").await.unwrap(), 0);
        assert_eq!(split(&tx, b"k", b"k", 10).await.unwrap(), ks(&[b"k"]));
        assert!(env.wait_metrics().is_empty() && env.split_ranges().is_empty());
        // Split points: the keys the transaction may read (the bound itself allowed).
        assert_eq!(
            code(split(&tx, b"a", b"\xff\x00", 10).await),
            codes::KEY_OUTSIDE_LEGAL_RANGE
        );
        assert_eq!(
            split(&tx, b"y", b"\xff", 1 << 40).await.unwrap(),
            ks(&[b"y", b"\xff"])
        );
        assert_eq!(
            code(split(&tx, b"a", b"b", 0).await),
            codes::CLIENT_INVALID_OPERATION
        );
        // The estimate takes any keys.
        assert_eq!(est(&tx, b"y", b"\xff\x01").await.unwrap(), 2 * 2);
        let sys = env.db.create_transaction();
        sys.set_option(TransactionOption::ReadSystemKeys).unwrap();
        assert_eq!(
            split(&sys, b"y", KEY_SPACE_END, 1 << 40).await.unwrap(),
            ks(&[b"y", KEY_SPACE_END])
        );
        assert_eq!(
            code(split(&sys, b"y", b"\xff\xff\x00", 10).await),
            codes::KEY_OUTSIDE_LEGAL_RANGE
        );
        // No read version, and no read conflict range: a commit with a write sends only the
        // write's ranges.
        assert_eq!(env.grv_requests.load(Ordering::SeqCst), 0);
        tx.set(b"w", b"1");
        within("commit", tx.commit()).await.unwrap();
        let commit = env.commits.lock().unwrap()[0].clone();
        assert!(
            commit
                .read_conflict_ranges
                .iter()
                .all(|r| r.begin.starts_with(b"\xff/SC/")),
            "{:?}",
            commit.read_conflict_ranges
        );
        // The transaction's rules, as for reads.
        assert_eq!(code(est(&tx, b"a", b"b").await), codes::USED_DURING_COMMIT);
        let tx = env.db.create_transaction();
        tx.set(b"\xff/nope", b"");
        assert_eq!(
            code(est(&tx, b"a", b"b").await),
            codes::KEY_OUTSIDE_LEGAL_RANGE,
            "the deferred error"
        );
        let tx = env.db.create_transaction();
        tx.cancel();
        assert_eq!(
            code(split(&tx, b"a", b"b", 10).await),
            codes::TRANSACTION_CANCELLED,
            "{schema}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn transaction_reads_start_at_call_and_end_with_the_transaction() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        set_letters(&env, 1);
        env.storage[0].state.lock().unwrap().delay = Duration::from_secs(600);
        // Started when called: the request is sent even if the future is never polled.
        let tx = env.db.create_transaction();
        let unpolled = tx.get_estimated_range_size_bytes(b"a", b"z");
        support::until("the request", || !env.wait_metrics().is_empty()).await;
        drop(unpolled);
        // The commit does not wait for them (they are not in `reading`).
        let slow = tx.get_range_split_points(b"a", b"z", 10);
        tx.set(b"w", b"1");
        let started = tokio::time::Instant::now();
        within("commit", tx.commit()).await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(60), "{schema}");
        // Its result still arrives (the futures borrow nothing).
        let points = tokio::time::timeout(Duration::from_secs(1200), slow)
            .await
            .expect("the split points")
            .unwrap();
        assert_eq!(points.first().map(Vec::as_slice), Some(&b"a"[..]));
        // A reset ends one in progress.
        let tx = env.db.create_transaction();
        let pending = tx.get_estimated_range_size_bytes(b"a", b"z");
        tx.reset();
        assert_eq!(
            within("estimate", pending).await.unwrap_err().code(),
            codes::TRANSACTION_CANCELLED
        );
        // A timeout too.
        let tx = env.db.create_transaction();
        tx.set_option(TransactionOption::Timeout(Duration::from_secs(1)))
            .unwrap();
        let pending = tx.get_range_split_points(b"a", b"z", 10);
        assert_eq!(
            within("split", pending).await.unwrap_err().code(),
            codes::TRANSACTION_TIMED_OUT
        );
    }
}

#[tokio::test(start_paused = true)]
async fn split_points_carry_the_read_version_when_the_transaction_has_one() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        set_letters(&env, 1);
        let tx = env.db.create_transaction();
        tx.set_read_version(READ_VERSION);
        within("split", tx.get_range_split_points(b"a", b"z", 10))
            .await
            .unwrap();
        let locations = env.locations.lock().unwrap().clone();
        assert_eq!(locations.last().unwrap().min_tenant_version, READ_VERSION);
        // The estimate never does (C++ passes no transaction state without a tenant).
        env.db
            .reader()
            .invalidate_locations(&range(b"", KEY_SPACE_END));
        within("estimate", tx.get_estimated_range_size_bytes(b"a", b"z"))
            .await
            .unwrap();
        let locations = env.locations.lock().unwrap().clone();
        assert_eq!(
            locations.last().unwrap().min_tenant_version,
            LATEST_VERSION,
            "{schema}"
        );
    }
}
