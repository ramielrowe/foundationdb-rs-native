//! The read path against a scripted fake cluster (a coordinator, GRV and commit proxies, and
//! storage servers on the in-memory network), under tokio's paused clock: read versions
//! (batching, failover between proxies, proxy changes, errors), the location cache (fill,
//! hit, invalidation on `wrong_shard_server` and `all_alternatives_failed`), and `GetValue`
//! (replica failover, penalty-based choice, `future_version` and `transaction_too_old`
//! surfaced).

mod support;

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::read::{KEY_SPACE_END, ReadConfig, ReadVersionOptions, Reader};
use fdb_native_client::{
    ClusterConnectionString, ClusterMonitor, ClusterMonitorConfig, ConnectionRecord, Error,
    TransactionRetry,
};
use fdb_native_schema::domain::{
    ClientDbInfo, CommitProxy, GetKeyServerLocations, GetReadVersion, GetValue, GetValueReply,
    GrvProxy, KeyRange, KeyServerLocations, ReadVersion, StorageServer, TenantMode,
};
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig};
use fdb_native_wire::{FdbError, ProtocolVersion, Uid, VersionVector};
use support::process::{Action, FakeProcess, Incoming};
use support::{Call, FakeCoordinator, MemRuntime, Net, sock};

/// The shard map the fake commit proxies answer from.
type Shards = Vec<(KeyRange, Vec<StorageServer>)>;

const LIMIT: Duration = Duration::from_secs(120);

async fn within<T>(what: &str, f: impl Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: no result within {LIMIT:?}"))
}

fn protocol(schema: SchemaVersion) -> ProtocolVersion {
    schema.protocol_version()
}

fn range(begin: &[u8], end: &[u8]) -> KeyRange {
    KeyRange {
        begin: begin.to_vec(),
        end: end.to_vec(),
    }
}

/// What a fake storage server does.
#[derive(Default)]
struct StorageBehaviour {
    /// The penalty sent with every reply (1.0 when `None`).
    penalty: Option<f64>,
    /// An error to answer with (in the reply's `error`, as a storage server sends it).
    error: Option<u16>,
    /// Hang up instead of answering.
    close: bool,
    /// How long to take.
    delay: Duration,
    /// If set, keys outside it get `wrong_shard_server`.
    owns: Option<KeyRange>,
    /// The requests received.
    requests: Vec<GetValue>,
    /// The token each request was sent to.
    tokens: Vec<Uid>,
}

struct FakeStorage {
    process: FakeProcess,
    server: StorageServer,
    behaviour: Arc<Mutex<StorageBehaviour>>,
    handler: Arc<dyn Fn(Incoming) -> Action + Send + Sync>,
}

impl FakeStorage {
    fn set(&self, f: impl FnOnce(&mut StorageBehaviour)) {
        f(&mut self.behaviour.lock().unwrap());
    }

    fn served(&self) -> usize {
        self.behaviour.lock().unwrap().requests.len()
    }

    /// The server restarts its storage role: new endpoints, same id and locality. Its old
    /// `getValue` endpoint answers `wrong_shard_server` from now on.
    fn move_to(&mut self, token: Uid) {
        let handler = self.handler.clone();
        self.process
            .handle(token, move |incoming| handler(incoming));
        self.process
            .handle(self.server.base.token, |incoming: Incoming| {
                let (_, reply) =
                    GetValue::decode_request(incoming.schema, &incoming.payload).unwrap();
                let answer = GetValueReply {
                    error: Some(FdbError::new(codes::WRONG_SHARD_SERVER)),
                    ..GetValueReply::default()
                };
                let payload = GetValue::encode_reply(incoming.schema, &Ok(answer)).unwrap();
                Action::reply(reply, payload)
            });
        self.server.base = self.process.endpoint(token);
    }
}

struct FakeGrv {
    process: FakeProcess,
    proxy: GrvProxy,
    /// Requests received.
    requests: Arc<Mutex<Vec<GetReadVersion>>>,
    /// What to answer: `Ok(locked)` with the next version, or an error.
    answer: Arc<Mutex<Result<bool, Option<u16>>>>,
}

struct FakeCommit {
    process: FakeProcess,
    proxy: CommitProxy,
    requests: Arc<Mutex<Vec<GetKeyServerLocations>>>,
    /// Errors to answer the next requests with, in order.
    errors: Arc<Mutex<VecDeque<u16>>>,
}

/// A whole fake cluster.
struct Cluster {
    schema: SchemaVersion,
    net: Arc<Net>,
    rt: MemRuntime,
    peers: PeerManager<MemRuntime>,
    monitor: Arc<ClusterMonitor<MemRuntime>>,
    info: Arc<Mutex<ClientDbInfo>>,
    pending: Arc<Mutex<Vec<Call>>>,
    version: Arc<AtomicI64>,
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    shards: Arc<Mutex<Shards>>,
    storage: Vec<FakeStorage>,
    next_grv: u64,
}

impl Cluster {
    /// A cluster of `schema`'s version with `storage` storage servers, the first holding
    /// everything, and no proxies yet.
    async fn new(schema: SchemaVersion, storage: usize) -> Cluster {
        let net = Net::new();
        let rt = MemRuntime::new(&net);
        let mut coordinator = FakeCoordinator::start(&net, sock(1, 4500), protocol(schema));
        let mut config = PeerManagerConfig::default().without_jitter();
        config.supported_versions = vec![ProtocolVersion::V7_4, ProtocolVersion::V8_0];
        config.seed = Some(1);
        let peers = PeerManager::new(rt.clone(), config, DiscardEvents);
        let mut monitor_config = ClusterMonitorConfig::default();
        monitor_config.seed = Some(7);
        let cs: ClusterConnectionString = "test:abc@10.0.0.1:4500".parse().unwrap();
        let monitor = Arc::new(ClusterMonitor::start(
            rt.clone(),
            peers.clone(),
            ConnectionRecord::memory(cs),
            monitor_config,
        ));
        let info = Arc::new(Mutex::new(ClientDbInfo {
            id: Uid::new(0xdb, 1),
            grv_proxies: Vec::new(),
            commit_proxies: Vec::new(),
            forward: None,
            history: Vec::new(),
            cluster_id: Uid::new(0xc1, 0xd),
            cluster_type: 0,
            tenant_mode: match schema {
                SchemaVersion::V7_4 => Some(TenantMode::DISABLED),
                SchemaVersion::V8_0 => None,
            },
        }));
        let pending: Arc<Mutex<Vec<Call>>> = Arc::new(Mutex::new(Vec::new()));
        {
            // The coordinator: answers the long poll with the current info.
            let info = info.clone();
            let pending = pending.clone();
            tokio::spawn(async move {
                loop {
                    // Not `next_call`, which gives up after 60 s without a request.
                    let Some(call) = coordinator.try_call() else {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    };
                    let current = info.lock().unwrap().clone();
                    if call.request.known_client_info_id != current.id {
                        call.reply(&current);
                    } else {
                        pending.lock().unwrap().push(call);
                    }
                }
            });
        }
        let data = Arc::new(Mutex::new(BTreeMap::new()));
        let storage: Vec<FakeStorage> = (0..storage)
            .map(|i| FakeStorage::start(&net, schema, i, data.clone()))
            .collect();
        let shards = Arc::new(Mutex::new(vec![(
            range(b"", KEY_SPACE_END),
            storage
                .first()
                .map(|s| vec![s.server.clone()])
                .unwrap_or_default(),
        )]));
        Cluster {
            schema,
            net,
            rt,
            peers,
            monitor,
            info,
            pending,
            version: Arc::new(AtomicI64::new(1000)),
            data,
            shards,
            storage,
            next_grv: 0,
        }
    }

    /// Publishes a new info with these proxies.
    fn publish(&self, grv: &[&FakeGrv], commit: &[&FakeCommit]) {
        self.publish_with(grv, commit, false);
    }

    /// Publishes a new info with these proxies, provisional (a recovery in progress) or not.
    fn publish_with(&self, grv: &[&FakeGrv], commit: &[&FakeCommit], provisional: bool) {
        let mut info = self.info.lock().unwrap();
        info.id = Uid::new(0xdb, info.id.second + 1);
        info.grv_proxies = grv
            .iter()
            .map(|g| GrvProxy {
                provisional,
                ..g.proxy.clone()
            })
            .collect();
        info.commit_proxies = commit
            .iter()
            .map(|c| CommitProxy {
                provisional,
                ..c.proxy.clone()
            })
            .collect();
        let current = info.clone();
        drop(info);
        for call in self.pending.lock().unwrap().drain(..) {
            call.reply(&current);
        }
    }

    fn grv_proxy(&mut self, last: u8) -> FakeGrv {
        self.next_grv += 1;
        let process = FakeProcess::start(&self.net, sock(last, 4500), protocol(self.schema));
        let token = Uid::new(0x6000 + self.next_grv, 0x10);
        let proxy = GrvProxy {
            process_id: Some(format!("grv-{last}").into_bytes()),
            provisional: false,
            base: process.endpoint(token),
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let answer = Arc::new(Mutex::new(Ok(false)));
        {
            let requests = requests.clone();
            let answer = answer.clone();
            let version = self.version.clone();
            let proxy_id = token;
            process.handle(token, move |incoming: Incoming| {
                let (request, reply) =
                    GetReadVersion::decode_request(incoming.schema, &incoming.payload).unwrap();
                requests.lock().unwrap().push(request);
                let reply_value = match *answer.lock().unwrap() {
                    Ok(locked) => Ok(read_version(
                        version.fetch_add(10, Ordering::SeqCst),
                        locked,
                        proxy_id,
                    )),
                    Err(None) => return Action::Close,
                    Err(Some(code)) => Err(FdbError::new(code)),
                };
                let payload = GetReadVersion::encode_reply(incoming.schema, &reply_value).unwrap();
                Action::reply(reply, payload)
            });
        }
        FakeGrv {
            process,
            proxy,
            requests,
            answer,
        }
    }

    fn commit_proxy(&self, last: u8, n: u64) -> FakeCommit {
        let process = FakeProcess::start(&self.net, sock(last, 4500), protocol(self.schema));
        let proxy = CommitProxy {
            process_id: Some(format!("cp-{last}").into_bytes()),
            provisional: false,
            base: process.endpoint(Uid::new(0x7000 + n, 0x20)),
            schema: self.schema,
        };
        let token = proxy.endpoints().get_key_servers_locations.token;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(Mutex::new(VecDeque::new()));
        {
            let requests = requests.clone();
            let shards = self.shards.clone();
            let errors = errors.clone();
            process.handle(token, move |incoming: Incoming| {
                let (request, reply) =
                    GetKeyServerLocations::decode_request(incoming.schema, &incoming.payload)
                        .unwrap();
                let key = request.begin.clone();
                requests.lock().unwrap().push(request);
                if let Some(code) = errors.lock().unwrap().pop_front() {
                    let payload = GetKeyServerLocations::encode_reply(
                        incoming.schema,
                        &Err(FdbError::new(code)),
                    )
                    .unwrap();
                    return Action::reply(reply, payload);
                }
                let shards = shards.lock().unwrap();
                let shard = shards
                    .iter()
                    .find(|(r, _)| r.begin <= key && key < r.end)
                    .cloned()
                    .expect("a shard for every key");
                let locations = KeyServerLocations {
                    results: vec![shard],
                    tss_mapping: Vec::new(),
                    tag_mapping: Vec::new(),
                };
                let payload =
                    GetKeyServerLocations::encode_reply(incoming.schema, &Ok(locations)).unwrap();
                Action::reply(reply, payload)
            });
        }
        FakeCommit {
            process,
            proxy,
            requests,
            errors,
        }
    }

    /// The shards: `(range, storage server indices)`.
    fn set_shards(&self, shards: &[(&[u8], &[u8], &[usize])]) {
        *self.shards.lock().unwrap() = shards
            .iter()
            .map(|(b, e, servers)| {
                (
                    range(b, e),
                    servers
                        .iter()
                        .map(|&i| self.storage[i].server.clone())
                        .collect(),
                )
            })
            .collect();
    }

    fn set(&self, key: &[u8], value: &[u8]) {
        self.data
            .lock()
            .unwrap()
            .insert(key.to_vec(), value.to_vec());
    }

    fn reader(&self) -> Reader<MemRuntime> {
        let mut config = ReadConfig::default();
        config.seed = Some(11);
        self.reader_with(config)
    }

    fn reader_with(&self, config: ReadConfig) -> Reader<MemRuntime> {
        Reader::new(
            self.rt.clone(),
            self.peers.clone(),
            self.monitor.clone(),
            config,
        )
    }

    /// A cluster with one GRV proxy and one commit proxy, published.
    async fn ready(schema: SchemaVersion, storage: usize) -> (Cluster, FakeGrv, FakeCommit) {
        let mut cluster = Cluster::new(schema, storage).await;
        let grv = cluster.grv_proxy(10);
        let commit = cluster.commit_proxy(20, 1);
        cluster.publish(&[&grv], &[&commit]);
        (cluster, grv, commit)
    }
}

fn read_version(version: i64, locked: bool, proxy_id: Uid) -> ReadVersion {
    ReadVersion {
        process_busy_time: 0,
        version,
        locked,
        metadata_version: None,
        tag_throttle_info: BTreeMap::new(),
        mid_shard_size: 0,
        rk_default_throttled: false,
        rk_batch_throttled: false,
        ss_version_vector_delta: VersionVector::default(),
        proxy_id,
        proxy_tag_throttled_duration: 0.0,
    }
}

impl FakeStorage {
    fn start(
        net: &Arc<Net>,
        schema: SchemaVersion,
        i: usize,
        data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    ) -> FakeStorage {
        let process = FakeProcess::start(net, sock(30 + i as u8, 4500), protocol(schema));
        let token = Uid::new(0x5500 + i as u64, 0x30);
        let server = StorageServer {
            id: Uid::new(0x55, i as u64),
            locality: BTreeMap::new(),
            base: process.endpoint(token),
            tss_pair_id: None,
            accepting_requests: true,
        };
        let behaviour = Arc::new(Mutex::new(StorageBehaviour::default()));
        let handler: Arc<dyn Fn(Incoming) -> Action + Send + Sync> = {
            let behaviour = behaviour.clone();
            Arc::new(move |incoming: Incoming| {
                let (request, reply) =
                    GetValue::decode_request(incoming.schema, &incoming.payload).unwrap();
                let mut b = behaviour.lock().unwrap();
                b.requests.push(request.clone());
                b.tokens.push(incoming.token);
                if b.close {
                    return Action::Close;
                }
                let wrong_shard = b
                    .owns
                    .as_ref()
                    .is_some_and(|r| !(r.begin <= request.key && request.key < r.end));
                let error = if wrong_shard {
                    Some(codes::WRONG_SHARD_SERVER)
                } else {
                    b.error
                };
                let data = data.lock().unwrap();
                let value = data.get(&request.key);
                let answer = GetValueReply {
                    penalty: b.penalty.unwrap_or(1.0),
                    error: error.map(FdbError::new),
                    value: if error.is_some() {
                        None
                    } else {
                        value.map(Vec::as_slice)
                    },
                    cached: false,
                };
                let payload = GetValue::encode_reply(incoming.schema, &Ok(answer)).unwrap();
                Action::Reply {
                    token: reply,
                    payload,
                    delay: b.delay,
                }
            })
        };
        {
            let handler = handler.clone();
            process.handle(token, move |incoming| handler(incoming));
        }
        FakeStorage {
            process,
            server,
            behaviour,
            handler,
        }
    }
}

// ---- read versions -----------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn grv_batches_concurrent_requests() {
    for schema in SchemaVersion::ALL {
        let (cluster, grv, _commit) = Cluster::ready(schema, 1).await;
        let reader = cluster.reader();
        let first = within("first GRV", reader.get_read_version())
            .await
            .unwrap();
        assert_eq!(first, 1000);
        {
            let requests = grv.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            // A default transaction's request (`getConsistentReadVersion`).
            let r = &requests[0];
            assert_eq!(r.flags, GetReadVersion::PRIORITY_DEFAULT);
            assert_eq!(r.transaction_count, 1);
            assert!(r.tags.is_empty());
            assert_eq!(r.max_version, -1);
            assert_eq!(r.debug_id, None);
        }

        // Twenty at once: batched into fewer requests, counting every transaction.
        let reads: Vec<_> = (0..20)
            .map(|_| {
                let reader = reader.clone();
                tokio::spawn(async move { reader.get_read_version().await })
            })
            .collect();
        let mut versions = Vec::new();
        for read in reads {
            versions.push(within("GRV", read).await.unwrap().unwrap());
        }
        assert!(versions.iter().all(|v| *v > first), "{versions:?}");
        let requests = grv.requests.lock().unwrap();
        let batches = &requests[1..];
        assert!(batches.len() < 20, "{} batches", batches.len());
        assert_eq!(
            batches.iter().map(|r| r.transaction_count).sum::<u32>(),
            20,
            "{schema}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn grv_fails_over_between_proxies() {
    let mut cluster = Cluster::new(SchemaVersion::V7_4, 1).await;
    let a = cluster.grv_proxy(10);
    let b = cluster.grv_proxy(11);
    let commit = cluster.commit_proxy(20, 1);
    cluster.publish(&[&a, &b], &[&commit]);
    // A hangs up on every request (`request_maybe_delivered`): the read versions come from B.
    *a.answer.lock().unwrap() = Err(None);
    let reader = cluster.reader();
    for _ in 0..10 {
        let v = within("GRV", reader.get_read_version()).await.unwrap();
        assert!(v >= 1000);
    }
    assert_eq!(b.requests.lock().unwrap().len(), 10);
    let tried_a = a.process.requests();
    assert!(tried_a >= 1, "A was tried (and hung up)");

    // A is down altogether: still B.
    a.process.stop();
    for _ in 0..5 {
        within("GRV", reader.get_read_version()).await.unwrap();
    }
    assert_eq!(b.requests.lock().unwrap().len(), 15);
    assert_eq!(a.process.requests(), tried_a);
}

#[tokio::test(start_paused = true)]
async fn grv_errors_are_surfaced() {
    let (cluster, grv, _commit) = Cluster::ready(SchemaVersion::V8_0, 1).await;
    let reader = cluster.reader();
    *grv.answer.lock().unwrap() = Err(Some(codes::GRV_PROXY_MEMORY_LIMIT_EXCEEDED));
    let e = within("GRV", reader.get_read_version()).await.unwrap_err();
    assert_eq!(e.code(), codes::GRV_PROXY_MEMORY_LIMIT_EXCEEDED);
    assert!(e.is_retryable());
    assert_eq!(e.transaction_retry(), Some(TransactionRetry::Backoff));

    // A locked database: `database_locked`, unless lock aware.
    *grv.answer.lock().unwrap() = Ok(true);
    let e = within("GRV", reader.get_read_version()).await.unwrap_err();
    assert_eq!(e, Error::new(codes::DATABASE_LOCKED));
    let mut options = ReadVersionOptions::default();
    options.lock_aware = true;
    assert!(
        within("GRV", reader.get_read_version_with(options))
            .await
            .is_ok()
    );
}

#[tokio::test(start_paused = true)]
async fn grv_waits_for_proxies_and_follows_changes() {
    let mut cluster = Cluster::new(SchemaVersion::V7_4, 1).await;
    let reader = cluster.reader();
    // No proxies yet (a recovery): the request waits.
    let pending = tokio::spawn({
        let reader = reader.clone();
        async move { reader.get_read_version().await }
    });
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(!pending.is_finished());

    // A proxy that never answers is published, then replaced.
    let a = cluster.grv_proxy(10);
    let commit = cluster.commit_proxy(20, 1);
    a.process.handle(a.proxy.base.token, |_| Action::Ignore);
    cluster.publish(&[&a], &[&commit]);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(!pending.is_finished());
    assert_eq!(a.process.requests(), 1);

    let b = cluster.grv_proxy(11);
    cluster.publish(&[&b], &[&commit]);
    let v = within("GRV after the change", pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v, 1000);
    assert_eq!(b.requests.lock().unwrap().len(), 1);
}

// ---- the location cache ------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn location_cache_fill_hit_and_wrong_shard_server() {
    let (cluster, _grv, commit) = Cluster::ready(SchemaVersion::V7_4, 2).await;
    cluster.set_shards(&[(b"", b"m", &[0]), (b"m", KEY_SPACE_END, &[1])]);
    cluster.set(b"apple", b"red");
    cluster.set(b"banana", b"yellow");
    cluster.set(b"plum", b"purple");
    let reader = cluster.reader();
    let v = within("GRV", reader.get_read_version()).await.unwrap();

    // Fill.
    assert_eq!(reader.cached_locations(), 0);
    assert_eq!(
        within("get", reader.get(b"apple", v)).await.unwrap(),
        Some(b"red".to_vec())
    );
    assert_eq!(commit.requests.lock().unwrap().len(), 1);
    {
        let requests = commit.requests.lock().unwrap();
        let r = &requests[0];
        assert_eq!(r.begin, b"apple");
        assert_eq!(r.end, None);
        assert_eq!(r.limit, 100);
        assert!(!r.reverse);
        assert_eq!(r.min_tenant_version, v, "the read version, as C++ sends it");
    }
    let location = reader.cached_location(b"apple").unwrap();
    assert_eq!(location.range, range(b"", b"m"));
    assert_eq!(location.servers, vec![cluster.storage[0].server.clone()]);
    assert_eq!(reader.cached_locations(), 1);

    // Hit: same shard, no new request.
    assert_eq!(
        within("get", reader.get(b"banana", v)).await.unwrap(),
        Some(b"yellow".to_vec())
    );
    assert_eq!(within("get", reader.get(b"cherry", v)).await.unwrap(), None);
    assert_eq!(commit.requests.lock().unwrap().len(), 1);
    // Another shard.
    assert_eq!(
        within("get", reader.get(b"plum", v)).await.unwrap(),
        Some(b"purple".to_vec())
    );
    assert_eq!(commit.requests.lock().unwrap().len(), 2);
    assert_eq!(reader.cached_locations(), 2);
    assert_eq!(cluster.storage[0].served(), 3);
    assert_eq!(cluster.storage[1].served(), 1);

    // The shard moves to server 1; server 0 answers `wrong_shard_server` for it now. The
    // read invalidates the cached location, locates the key again, and reads from server 1.
    cluster.set_shards(&[(b"", KEY_SPACE_END, &[1])]);
    cluster.storage[0].set(|b| b.owns = Some(range(b"zzz", b"zzz")));
    assert_eq!(
        within("get after the move", reader.get(b"apple", v))
            .await
            .unwrap(),
        Some(b"red".to_vec())
    );
    assert_eq!(cluster.storage[0].served(), 4);
    assert_eq!(cluster.storage[1].served(), 2);
    assert_eq!(commit.requests.lock().unwrap().len(), 3);
    let location = reader.cached_location(b"apple").unwrap();
    assert_eq!(location.range, range(b"", KEY_SPACE_END));
    assert_eq!(location.servers, vec![cluster.storage[1].server.clone()]);
    // The new shard covers the old ones.
    assert_eq!(reader.cached_locations(), 1);

    // Explicit invalidation.
    reader.invalidate_location(b"x");
    assert_eq!(reader.cached_locations(), 0);
    assert!(reader.cached_location(b"apple").is_none());
    let located = within("locate", reader.locate(b"apple")).await.unwrap();
    assert_eq!(located.range, range(b"", KEY_SPACE_END));
    assert_eq!(commit.requests.lock().unwrap().len(), 4);

    // Outside the key space.
    let e = within("get", reader.get(b"\xff\xff", v)).await.unwrap_err();
    assert_eq!(e.code(), codes::KEY_OUTSIDE_LEGAL_RANGE);
    let e = within("get", reader.get(b"a", 0)).await.unwrap_err();
    assert_eq!(e.code(), codes::CLIENT_INVALID_OPERATION);
}

#[tokio::test(start_paused = true)]
async fn location_requests_fail_over_between_commit_proxies() {
    let mut cluster = Cluster::new(SchemaVersion::V8_0, 1).await;
    let grv = cluster.grv_proxy(10);
    let a = cluster.commit_proxy(20, 1);
    let b = cluster.commit_proxy(21, 2);
    cluster.publish(&[&grv], &[&a, &b]);
    a.process.stop();
    cluster.set(b"k", b"v");
    let reader = cluster.reader();
    let v = within("GRV", reader.get_read_version()).await.unwrap();
    for key in [b"k".as_slice(), b"l", b"m"] {
        reader.invalidate_location(key);
        let value = within("get", reader.get(key, v)).await.unwrap();
        assert_eq!(value.is_some(), key == b"k");
    }
    assert_eq!(b.requests.lock().unwrap().len(), 3);
    assert!(
        cluster.net.connects(a.process.addr) >= 1,
        "the stopped proxy was tried"
    );
}

// ---- GetValue ----------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn get_value_fails_over_between_replicas() {
    let (cluster, _grv, commit) = Cluster::ready(SchemaVersion::V7_4, 3).await;
    cluster.set_shards(&[(b"", KEY_SPACE_END, &[0, 1, 2])]);
    cluster.set(b"k", b"v");
    let reader = cluster.reader();
    let v = within("GRV", reader.get_read_version()).await.unwrap();
    assert_eq!(
        within("get", reader.get(b"k", v)).await.unwrap(),
        Some(b"v".to_vec())
    );

    // One replica hangs up on every request, another is down: every read still succeeds.
    cluster.storage[0].set(|b| b.close = true);
    cluster.storage[1].process.stop();
    for _ in 0..20 {
        assert_eq!(
            within("get", reader.get(b"k", v)).await.unwrap(),
            Some(b"v".to_vec())
        );
    }
    assert!(cluster.storage[2].served() >= 20);
    // The location was never invalidated: failover is within the shard's replicas.
    assert_eq!(reader.cached_locations(), 1);
    assert_eq!(commit.requests.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn get_value_prefers_replicas_without_penalty() {
    let (cluster, _grv, _commit) = Cluster::ready(SchemaVersion::V8_0, 2).await;
    cluster.set_shards(&[(b"", KEY_SPACE_END, &[0, 1])]);
    cluster.set(b"k", b"v");
    for s in &cluster.storage {
        s.set(|b| b.delay = Duration::from_millis(100));
    }
    // Server 1 asks for less load.
    cluster.storage[1].set(|b| b.penalty = Some(10.0));
    let reader = cluster.reader();
    let v = within("GRV", reader.get_read_version()).await.unwrap();
    for _ in 0..60 {
        within("get", reader.get(b"k", v)).await.unwrap();
    }
    let (s0, s1) = (cluster.storage[0].served(), cluster.storage[1].served());
    println!("served: {s0} without penalty, {s1} with penalty 10");
    assert_eq!(s0 + s1, 60);
    assert!(s0 > 4 * s1, "{s0} vs {s1}");
    assert!(s1 >= 1, "the penalized replica is still used now and then");
}

#[tokio::test(start_paused = true)]
async fn future_version_and_too_old_are_surfaced() {
    let (cluster, _grv, _commit) = Cluster::ready(SchemaVersion::V7_4, 2).await;
    cluster.set_shards(&[(b"", KEY_SPACE_END, &[0, 1])]);
    cluster.set(b"k", b"v");
    for s in &cluster.storage {
        s.set(|b| b.error = Some(codes::FUTURE_VERSION));
    }
    let reader = cluster.reader();
    let v = within("GRV", reader.get_read_version()).await.unwrap();
    let e = within("get", reader.get(b"k", v)).await.unwrap_err();
    assert_eq!(e.code(), codes::FUTURE_VERSION);
    assert!(e.is_retryable());
    assert_eq!(e.transaction_retry(), Some(TransactionRetry::FutureVersion));
    // Returned at once, not retried on the other replica.
    assert_eq!(cluster.storage[0].served() + cluster.storage[1].served(), 1);

    for s in &cluster.storage {
        s.set(|b| b.error = Some(codes::TRANSACTION_TOO_OLD));
    }
    let e = within("get", reader.get(b"k", v)).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_TOO_OLD);
    assert_eq!(cluster.storage[0].served() + cluster.storage[1].served(), 2);

    // `process_behind` moves on to the other replica, and is returned once all were tried.
    cluster.storage[0].set(|b| b.error = Some(codes::PROCESS_BEHIND));
    cluster.storage[1].set(|b| b.error = None);
    for _ in 0..5 {
        assert_eq!(
            within("get", reader.get(b"k", v)).await.unwrap(),
            Some(b"v".to_vec())
        );
    }
    cluster.storage[1].set(|b| b.error = Some(codes::PROCESS_BEHIND));
    let e = within("get", reader.get(b"k", v)).await.unwrap_err();
    assert_eq!(e.code(), codes::PROCESS_BEHIND);
    // The cached location stayed.
    assert_eq!(reader.cached_locations(), 1);
}

#[tokio::test(start_paused = true)]
async fn all_alternatives_failed_relocates_and_recovers() {
    let (cluster, _grv, commit) = Cluster::ready(SchemaVersion::V7_4, 2).await;
    cluster.set_shards(&[(b"", KEY_SPACE_END, &[0, 1])]);
    cluster.set(b"k", b"v");
    let reader = cluster.reader();
    let v = within("GRV", reader.get_read_version()).await.unwrap();
    within("get", reader.get(b"k", v)).await.unwrap();
    assert_eq!(commit.requests.lock().unwrap().len(), 1);

    // Both replicas go down. Once the failure monitor marks them failed (4 s), the read gets
    // `all_alternatives_failed`, invalidates the location and asks the commit proxy again,
    // over and over.
    for s in &cluster.storage {
        s.process.stop();
    }
    let read = tokio::spawn({
        let reader = reader.clone();
        async move { reader.get(b"k", v).await }
    });
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert!(!read.is_finished());
    let relocations = commit.requests.lock().unwrap().len();
    assert!(relocations > 2, "{relocations} location requests");

    // A replica comes back (its peer is referenced by the cache, so it is probed): the read
    // completes.
    cluster.storage[1].process.restart();
    assert_eq!(
        within("read after recovery", read).await.unwrap().unwrap(),
        Some(b"v".to_vec())
    );
}

#[tokio::test(start_paused = true)]
async fn snapshot_reads_at_its_version() {
    let (cluster, _grv, _commit) = Cluster::ready(SchemaVersion::V8_0, 1).await;
    cluster.set(b"k", b"v");
    let reader = cluster.reader();
    let snapshot = within("snapshot", reader.snapshot()).await.unwrap();
    assert_eq!(snapshot.version(), 1000);
    assert_eq!(
        within("get", snapshot.get(b"k")).await.unwrap(),
        Some(b"v".to_vec())
    );
    assert_eq!(within("get", snapshot.get(b"nope")).await.unwrap(), None);
    let requests = &cluster.storage[0].behaviour.lock().unwrap().requests;
    assert!(requests.iter().all(|r| r.version == 1000));
    assert_eq!(requests[0].key, b"k");
    assert_eq!(requests[0].tags, None);
    assert_eq!(requests[0].options, None);
}

// ---- review additions -------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn grv_batches_are_capped_and_errors_reach_every_waiter() {
    let (cluster, grv, _commit) = Cluster::ready(SchemaVersion::V7_4, 1).await;
    let mut config = ReadConfig::default();
    config.seed = Some(11);
    config.max_batch_size = 4;
    let reader = cluster.reader_with(config);
    let reads: Vec<_> = (0..10)
        .map(|_| {
            let reader = reader.clone();
            tokio::spawn(async move { reader.get_read_version().await })
        })
        .collect();
    for read in reads {
        within("GRV", read).await.unwrap().unwrap();
    }
    {
        let requests = grv.requests.lock().unwrap();
        let counts: Vec<u32> = requests.iter().map(|r| r.transaction_count).collect();
        assert!(counts.iter().all(|&c| c <= 4), "{counts:?}");
        assert_eq!(counts.iter().sum::<u32>(), 10);
        assert!(counts.len() >= 3, "{counts:?}");
    }

    // A batch that fails fails for each of its transactions.
    *grv.answer.lock().unwrap() = Err(Some(codes::BATCH_TRANSACTION_THROTTLED));
    let before = grv.requests.lock().unwrap().len();
    let reads: Vec<_> = (0..4)
        .map(|_| {
            let reader = reader.clone();
            tokio::spawn(async move { reader.get_read_version().await })
        })
        .collect();
    for read in reads {
        let e = within("GRV", read).await.unwrap().unwrap_err();
        assert_eq!(e.code(), codes::BATCH_TRANSACTION_THROTTLED);
    }
    let requests = grv.requests.lock().unwrap();
    assert_eq!(requests.len(), before + 1, "one batch");
    assert_eq!(requests[before].transaction_count, 4);
}

#[tokio::test(start_paused = true)]
async fn location_requests_back_off_while_commit_proxies_are_out_of_memory() {
    let (cluster, _grv, commit) = Cluster::ready(SchemaVersion::V8_0, 1).await;
    cluster.set(b"k", b"v");
    let reader = cluster.reader();
    let v = within("GRV", reader.get_read_version()).await.unwrap();
    commit
        .errors
        .lock()
        .unwrap()
        .extend([codes::COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED; 3]);
    let start = tokio::time::Instant::now();
    assert_eq!(
        within("get", reader.get(b"k", v)).await.unwrap(),
        Some(b"v".to_vec())
    );
    // Three failures, then success: waits of 10, 20 and 40 ms before the retries.
    assert_eq!(commit.requests.lock().unwrap().len(), 4);
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(70), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(200), "{elapsed:?}");

    // The backoff halves on success (to 20 ms) and is waited before the next request.
    reader.invalidate_location(b"k");
    let start = tokio::time::Instant::now();
    within("get", reader.get(b"k", v)).await.unwrap();
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(20), "{elapsed:?}");
    assert_eq!(commit.requests.lock().unwrap().len(), 5);
}

#[tokio::test(start_paused = true)]
async fn an_endpoint_failed_on_a_healthy_server_refreshes_the_location() {
    let (cluster, _grv, commit) = Cluster::ready(SchemaVersion::V7_4, 2).await;
    cluster.set_shards(&[(b"", KEY_SPACE_END, &[0, 1])]);
    cluster.set(b"k", b"v");
    let reader = cluster.reader();
    let v = within("GRV", reader.get_read_version()).await.unwrap();
    within("get", reader.get(b"k", v)).await.unwrap();
    within("get", reader.get(b"k", v)).await.unwrap();
    assert_eq!(commit.requests.lock().unwrap().len(), 1, "cached");

    // Server 0's getValue endpoint is gone while its process is fine (its storage role
    // restarted): the cached location is refreshed on every read during the grace period
    // (60 s), and the reads go to server 1.
    let server = &cluster.storage[0].server;
    let address = cluster.peers.primary_address(&server.base.addresses);
    cluster
        .peers
        .failure_monitor()
        .endpoint_not_found(&address, server.base.token);
    let served = cluster.storage[0].served();
    within("get", reader.get(b"k", v)).await.unwrap();
    within("get", reader.get(b"k", v)).await.unwrap();
    assert_eq!(commit.requests.lock().unwrap().len(), 3);
    assert_eq!(
        cluster.storage[0].served(),
        served,
        "the failed endpoint is skipped"
    );

    // After the grace period, once per 60 s.
    tokio::time::sleep(Duration::from_secs(61)).await;
    within("get", reader.get(b"k", v)).await.unwrap();
    assert_eq!(commit.requests.lock().unwrap().len(), 4);
    for _ in 0..3 {
        within("get", reader.get(b"k", v)).await.unwrap();
    }
    assert_eq!(commit.requests.lock().unwrap().len(), 4);
    tokio::time::sleep(Duration::from_secs(61)).await;
    within("get", reader.get(b"k", v)).await.unwrap();
    assert_eq!(commit.requests.lock().unwrap().len(), 5);

    // The endpoint is announced again: no more refreshes.
    cluster
        .peers
        .failure_monitor()
        .forget_endpoint(&address, server.base.token);
    for _ in 0..3 {
        within("get", reader.get(b"k", v)).await.unwrap();
    }
    assert_eq!(commit.requests.lock().unwrap().len(), 5);
}

#[tokio::test(start_paused = true)]
async fn the_location_cache_is_bounded() {
    let (cluster, _grv, commit) = Cluster::ready(SchemaVersion::V8_0, 2).await;
    let bounds: Vec<Vec<u8>> = (0..=10u8).map(|i| vec![b'a' + i]).collect();
    let mut shards: Vec<(&[u8], &[u8], &[usize])> = Vec::new();
    shards.push((b"", &bounds[0], &[0]));
    for w in bounds.windows(2) {
        shards.push((&w[0], &w[1], &[1]));
    }
    shards.push((&bounds[10], KEY_SPACE_END, &[0]));
    cluster.set_shards(&shards);
    let mut config = ReadConfig::default();
    config.seed = Some(11);
    config.location_cache_size = 3;
    let reader = cluster.reader_with(config);
    let v = within("GRV", reader.get_read_version()).await.unwrap();
    for b in &bounds {
        within("get", reader.get(b, v)).await.unwrap();
        assert!(
            reader.cached_locations() <= 3,
            "{}",
            reader.cached_locations()
        );
        assert!(reader.cached_location(b).is_some(), "the latest stays");
    }
    assert_eq!(reader.cached_locations(), 3);
    assert_eq!(commit.requests.lock().unwrap().len(), 11);
    // An evicted shard is located again.
    let evicted = bounds
        .iter()
        .find(|b| reader.cached_location(b).is_none())
        .unwrap();
    within("get", reader.get(evicted, v)).await.unwrap();
    assert_eq!(commit.requests.lock().unwrap().len(), 12);
}

#[tokio::test(start_paused = true)]
async fn new_endpoints_of_a_server_reach_every_cached_shard() {
    let (mut cluster, _grv, commit) = Cluster::ready(SchemaVersion::V7_4, 1).await;
    cluster.set_shards(&[(b"", b"m", &[0]), (b"m", KEY_SPACE_END, &[0])]);
    cluster.set(b"a", b"1");
    cluster.set(b"x", b"2");
    let reader = cluster.reader();
    let v = within("GRV", reader.get_read_version()).await.unwrap();
    within("get", reader.get(b"a", v)).await.unwrap();
    within("get", reader.get(b"x", v)).await.unwrap();
    assert_eq!(commit.requests.lock().unwrap().len(), 2);

    // Server 0 restarts its storage role: same id and locality, new getValue endpoint.
    let old = cluster.storage[0].server.base.token;
    let new = Uid::new(0x5599, 0x31);
    cluster.storage[0].move_to(new);
    cluster.set_shards(&[(b"", b"m", &[0]), (b"m", KEY_SPACE_END, &[0])]);

    // The first shard's read gets `wrong_shard_server` at the old endpoint and is located
    // again; the reply's interface updates the server's entry in place...
    assert_eq!(
        within("get", reader.get(b"a", v)).await.unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(commit.requests.lock().unwrap().len(), 3);
    // ... so the other cached shard uses the new endpoint without being located again.
    assert_eq!(
        reader.cached_location(b"x").unwrap().servers[0].base.token,
        new
    );
    assert_eq!(
        within("get", reader.get(b"x", v)).await.unwrap(),
        Some(b"2".to_vec())
    );
    assert_eq!(commit.requests.lock().unwrap().len(), 3);
    let tokens = cluster.storage[0].behaviour.lock().unwrap().tokens.clone();
    // (The old endpoint's `wrong_shard_server` answer is not the server's handler: two reads
    // before the move, two after.)
    assert_eq!(tokens, vec![old, old, new, new]);
    assert_eq!(cluster.storage[0].process.requests(), 5);
}

#[tokio::test(start_paused = true)]
async fn provisional_proxies_only_when_asked() {
    let mut cluster = Cluster::new(SchemaVersion::V8_0, 1).await;
    let grv = cluster.grv_proxy(10);
    let commit = cluster.commit_proxy(20, 1);
    cluster.publish_with(&[&grv], &[&commit], true);
    cluster.set(b"k", b"v");
    let reader = cluster.reader();

    // A default read version waits for the recovery to finish.
    let waiting = tokio::spawn({
        let reader = reader.clone();
        async move { reader.get_read_version().await }
    });
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(!waiting.is_finished());

    // With USE_PROVISIONAL_PROXIES, the provisional proxies serve the read version and the
    // key location.
    let mut options = ReadVersionOptions::default();
    options.use_provisional_proxies = true;
    let snapshot = within("snapshot", reader.snapshot_with(options))
        .await
        .unwrap();
    assert_eq!(
        within("get", snapshot.get(b"k")).await.unwrap(),
        Some(b"v".to_vec())
    );
    assert_eq!(
        grv.requests.lock().unwrap()[0].flags,
        GetReadVersion::PRIORITY_DEFAULT | GetReadVersion::FLAG_USE_PROVISIONAL_PROXIES
    );
    assert_eq!(commit.requests.lock().unwrap().len(), 1);

    // A plain read does not use them.
    reader.invalidate_location(b"k");
    let plain = tokio::spawn({
        let reader = reader.clone();
        let version = snapshot.version();
        async move { reader.get(b"k", version).await }
    });
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(!plain.is_finished());
    assert_eq!(commit.requests.lock().unwrap().len(), 1);

    // The recovery completes.
    cluster.publish(&[&grv], &[&commit]);
    within("plain get", plain).await.unwrap().unwrap();
    within("default GRV", waiting).await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_ends_parked_requests() {
    let (cluster, grv, _commit) = Cluster::ready(SchemaVersion::V7_4, 1).await;
    let reader = cluster.reader();
    within("GRV", reader.get_read_version()).await.unwrap();

    // The only GRV proxy is down: once it is marked failed, a read version request waits
    // for it to come back.
    grv.process.stop();
    let parked = tokio::spawn({
        let reader = reader.clone();
        async move { reader.get_read_version().await }
    });
    tokio::time::sleep(Duration::from_secs(20)).await;
    assert!(!parked.is_finished());

    cluster.peers.shutdown().await;
    let e = within("parked GRV", parked).await.unwrap().unwrap_err();
    assert_eq!(e.code(), codes::OPERATION_CANCELLED);
    let e = within("GRV", reader.get_read_version()).await.unwrap_err();
    assert_eq!(e.code(), codes::OPERATION_CANCELLED);
}

#[tokio::test(start_paused = true)]
async fn monitor_shutdown_ends_parked_location_requests() {
    let (cluster, _grv, commit) = Cluster::ready(SchemaVersion::V8_0, 1).await;
    let reader = cluster.reader();
    let v = within("GRV", reader.get_read_version()).await.unwrap();
    commit.process.stop();
    let parked = tokio::spawn({
        let reader = reader.clone();
        async move { reader.get(b"k", v).await }
    });
    tokio::time::sleep(Duration::from_secs(20)).await;
    assert!(!parked.is_finished());
    cluster.monitor.shutdown().await;
    let e = within("parked get", parked).await.unwrap().unwrap_err();
    assert_eq!(e.code(), codes::OPERATION_CANCELLED);
}
