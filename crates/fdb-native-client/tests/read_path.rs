//! The read path against a scripted fake cluster (a coordinator, GRV and commit proxies, and
//! storage servers on the in-memory network), under tokio's paused clock: read versions
//! (batching, failover between proxies, proxy changes, errors), the location cache (fill,
//! hit, invalidation on `wrong_shard_server` and `all_alternatives_failed`), and `GetValue`
//! (replica failover, penalty-based choice, `future_version` and `transaction_too_old`
//! surfaced).

mod support;

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::read::{
    KEY_SPACE_END, METADATA_VERSION_KEY, Priority, ReadConfig, ReadVersionOptions, Reader,
};
use fdb_native_client::{
    ClusterConnectionString, ClusterMonitor, ClusterMonitorConfig, ConnectionRecord,
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
    /// The `processBusyTime` of the replies.
    busy: Arc<AtomicI32>,
    /// The `metadataVersion` of the replies.
    metadata: Arc<Mutex<Option<Vec<u8>>>>,
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
        let busy = Arc::new(AtomicI32::new(0));
        let metadata = Arc::new(Mutex::new(None));
        {
            let requests = requests.clone();
            let answer = answer.clone();
            let version = self.version.clone();
            let busy = busy.clone();
            let metadata = metadata.clone();
            let proxy_id = token;
            process.handle(token, move |incoming: Incoming| {
                let (request, reply) =
                    GetReadVersion::decode_request(incoming.schema, &incoming.payload).unwrap();
                requests.lock().unwrap().push(request);
                let reply_value = match *answer.lock().unwrap() {
                    Ok(locked) => Ok(ReadVersion {
                        process_busy_time: busy.load(Ordering::SeqCst),
                        metadata_version: metadata.lock().unwrap().clone(),
                        ..read_version(version.fetch_add(10, Ordering::SeqCst), locked, proxy_id)
                    }),
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
            busy,
            metadata,
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
    assert_eq!(e.code(), codes::DATABASE_LOCKED);
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

// ---- load balancing: second requests, priorities, GRV proxy choice, metadata version --------

/// Reads `key` at `version`, with the virtual time it took.
async fn timed_get(
    reader: &Reader<MemRuntime>,
    key: &[u8],
    version: i64,
) -> (Option<Vec<u8>>, Duration) {
    let start = tokio::time::Instant::now();
    let value = within("get", reader.get(key, version)).await.unwrap();
    (value, start.elapsed())
}

#[tokio::test(start_paused = true)]
async fn a_slow_replica_is_raced_by_a_second_request() {
    for schema in SchemaVersion::ALL {
        let (cluster, _grv, _commit) = Cluster::ready(schema, 2).await;
        cluster.set_shards(&[(b"", KEY_SPACE_END, &[0, 1])]);
        cluster.set(b"k", b"v");
        let reader = cluster.reader();
        let version = within("GRV", reader.get_read_version()).await.unwrap();

        // Replies to first requests earn the budget for second ones (0.05 each).
        for _ in 0..400 {
            let (value, _) = timed_get(&reader, b"k", version).await;
            assert_eq!(value.as_deref(), Some(&b"v"[..]));
        }

        // With no time passing in a reply, the replicas look equally loaded and the first
        // in the location's order is the first choice. Make that one slow.
        let order = reader.locate(b"k").await.unwrap().servers;
        let slow = cluster
            .storage
            .iter()
            .position(|s| s.server.id == order[0].id)
            .unwrap();
        let fast = 1 - slow;
        const SLOW: Duration = Duration::from_secs(5);
        cluster.storage[slow].set(|b| b.delay = SLOW);
        let served = |i: usize| cluster.storage[i].served();

        // The first request is given `latency + BASE_SECOND_REQUEST_TIME` (0.5 ms), then a
        // second goes to the other replica, which answers at once.
        let (slow_before, fast_before) = (served(slow), served(fast));
        let (value, took) = timed_get(&reader, b"k", version).await;
        assert_eq!(value.as_deref(), Some(&b"v"[..]), "{schema}");
        assert!(took >= Duration::from_micros(500), "{schema}: {took:?}");
        assert!(took < Duration::from_millis(50), "{schema}: {took:?}");
        assert_eq!(
            (served(slow) - slow_before, served(fast) - fast_before),
            (1, 1),
            "{schema}: both were asked"
        );

        // The slow request goes on without the read (a lagging request) and ends on its own;
        // the model learned from it.
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Later, the fast replica gets a little slower, so that the slow one (its load
        // decayed) is again the first choice, now known to take 5 s: the second request
        // goes out at once. Whatever the first choice, no read waits for the slow replica.
        cluster.storage[fast].set(|b| b.delay = Duration::from_millis(1));
        let mut raced = 0;
        for attempt in 0..40 {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let slow_before = served(slow);
            let (value, took) = timed_get(&reader, b"k", version).await;
            assert_eq!(
                value.as_deref(),
                Some(&b"v"[..]),
                "{schema} attempt {attempt}"
            );
            assert!(
                took < Duration::from_millis(100),
                "{schema} attempt {attempt}: {took:?}"
            );
            raced += served(slow) - slow_before;
        }
        assert!(raced >= 1, "{schema}: the slow replica was not asked again");
    }
}

#[tokio::test(start_paused = true)]
async fn no_budget_no_second_request() {
    let (cluster, _grv, _commit) = Cluster::ready(SchemaVersion::V7_4, 2).await;
    cluster.set_shards(&[(b"", KEY_SPACE_END, &[0, 1])]);
    cluster.set(b"k", b"v");
    const SLOW: Duration = Duration::from_secs(3);
    cluster.storage[0].set(|b| b.delay = SLOW);
    let version = {
        let reader = cluster.reader();
        within("GRV", reader.get_read_version()).await.unwrap()
    };
    // A reader with nothing earned yet waits for the slow replica when it is first.
    let mut waited = 0;
    for seed in 0..40 {
        let mut config = ReadConfig::default();
        config.seed = Some(seed);
        let reader = cluster.reader_with(config);
        let (slow_before, fast_before) = (cluster.storage[0].served(), cluster.storage[1].served());
        let (value, took) = timed_get(&reader, b"k", version).await;
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
        if cluster.storage[0].served() > slow_before {
            assert!(took >= SLOW, "seed {seed}: {took:?}");
            assert_eq!(
                cluster.storage[1].served(),
                fast_before,
                "no second request"
            );
            waited += 1;
            if waited == 3 {
                break;
            }
        } else {
            assert!(took < Duration::from_secs(1));
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    assert_eq!(waited, 3);
}

#[tokio::test(start_paused = true)]
async fn the_lagging_request_is_not_abandoned_by_its_replica() {
    // The loser of a hedged read still completes on its server (its reply feeds the model);
    // the read does not wait for it, and shutting the reader down ends it.
    let (cluster, _grv, _commit) = Cluster::ready(SchemaVersion::V8_0, 2).await;
    cluster.set_shards(&[(b"", KEY_SPACE_END, &[0, 1])]);
    cluster.set(b"k", b"v");
    let reader = cluster.reader();
    let version = within("GRV", reader.get_read_version()).await.unwrap();
    for _ in 0..200 {
        timed_get(&reader, b"k", version).await;
    }
    cluster.storage[0].set(|b| b.delay = Duration::from_secs(30));
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_secs(40)).await;
        let (value, took) = timed_get(&reader, b"k", version).await;
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
        assert!(took < Duration::from_secs(1), "{took:?}");
    }
    drop(reader);
    // Dropping the reader cancels what was left; nothing is left to panic or hang.
    tokio::time::sleep(Duration::from_secs(60)).await;
}

#[tokio::test(start_paused = true)]
async fn read_versions_batch_separately_by_priority() {
    for schema in SchemaVersion::ALL {
        let (cluster, grv, _commit) = Cluster::ready(schema, 1).await;
        let reader = cluster.reader();
        let with = |priority: Priority, causal_read_risky: bool, lock_aware: bool| {
            let mut options = ReadVersionOptions::default();
            options.priority = priority;
            options.causal_read_risky = causal_read_risky;
            options.lock_aware = lock_aware;
            options
        };
        // Concurrent requests of every priority, and a flag that changes the request.
        let mut requests = Vec::new();
        for (options, count) in [
            (with(Priority::Batch, false, false), 3),
            (with(Priority::Default, false, false), 4),
            // Lock awareness is not part of the request: it batches with the default ones.
            (with(Priority::Default, false, true), 2),
            (with(Priority::Immediate, false, false), 5),
            (with(Priority::Default, true, false), 2),
            // 8.0's `MAX_GRV_QUEUE_DELAY` is part of the request, and of the batcher's key.
            (
                {
                    let mut options = with(Priority::Default, false, false);
                    options.max_grv_queue_delay_ms = Some(50);
                    options
                },
                3,
            ),
        ] {
            for _ in 0..count {
                let reader = reader.clone();
                requests.push(tokio::spawn(async move {
                    reader.get_read_version_with(options).await
                }));
            }
        }
        for request in requests {
            within("GRV", request).await.unwrap().unwrap();
        }
        let sent = grv.requests.lock().unwrap().clone();
        let count = |flags: u32| -> u32 {
            sent.iter()
                .filter(|r| r.flags == flags)
                .map(|r| r.transaction_count)
                .sum()
        };
        assert_eq!(count(GetReadVersion::PRIORITY_BATCH), 3, "{schema}");
        // (A 7.4 request cannot carry the delay.)
        let delayed = if schema == SchemaVersion::V8_0 { 3 } else { 0 };
        assert_eq!(count(GetReadVersion::PRIORITY_DEFAULT), 9, "{schema}");
        assert_eq!(
            sent.iter()
                .filter(|r| r.max_grv_queue_delay_ms == Some(50))
                .map(|r| r.transaction_count)
                .sum::<u32>(),
            delayed,
            "{schema}"
        );
        // Whatever the cluster, the delayed requests were batched on their own: the default
        // ones (9 transactions in all, 4 + 2 + 3) made at least two batches.
        assert!(
            sent.iter()
                .filter(|r| r.flags == GetReadVersion::PRIORITY_DEFAULT)
                .count()
                >= 2,
            "{schema}"
        );
        assert_eq!(
            count(GetReadVersion::PRIORITY_SYSTEM_IMMEDIATE),
            5,
            "{schema}"
        );
        assert_eq!(
            count(GetReadVersion::PRIORITY_DEFAULT | GetReadVersion::FLAG_CAUSAL_READ_RISKY),
            2,
            "{schema}"
        );
        assert_eq!(
            sent.iter().map(|r| r.transaction_count).sum::<u32>(),
            19,
            "every transaction is in exactly one batch"
        );
        // Each request has one priority only (the batchers do not mix them).
        for r in &sent {
            let priority = r.flags & GetReadVersion::FLAG_PRIORITY_MASK;
            assert!(
                [
                    GetReadVersion::PRIORITY_BATCH,
                    GetReadVersion::PRIORITY_DEFAULT,
                    GetReadVersion::PRIORITY_SYSTEM_IMMEDIATE
                ]
                .contains(&priority),
                "{:#x}",
                r.flags
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn busy_grv_proxies_are_chosen_less_often() {
    let mut cluster = Cluster::new(SchemaVersion::V7_4, 1).await;
    let busy = cluster.grv_proxy(10);
    let idle = cluster.grv_proxy(11);
    let commit = cluster.commit_proxy(20, 1);
    cluster.publish(&[&busy, &idle], &[&commit]);
    // `processBusyTime` is the recent request count times 10 000 plus the CPU busyness.
    busy.busy.store(80 * 10_000 + 1_000, Ordering::SeqCst);
    idle.busy.store(20 * 10_000 + 9_000, Ordering::SeqCst);
    let reader = cluster.reader();
    // Probabilities start even, and move by at most 0.1 of the load difference every 10 s
    // when both proxies reported within the last 5 s.
    for _ in 0..30 {
        within("GRV", reader.get_read_version()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let (busy_early, idle_early) = (
        busy.requests.lock().unwrap().len(),
        idle.requests.lock().unwrap().len(),
    );
    assert!(
        busy_early >= 6 && idle_early >= 6,
        "{busy_early} {idle_early}"
    );
    for _ in 0..200 {
        within("GRV", reader.get_read_version()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let (busy_before, idle_before) = (
        busy.requests.lock().unwrap().len(),
        idle.requests.lock().unwrap().len(),
    );
    // By now the busy proxy's probability is at its floor, 1 / (MAX_PROB * 2) = 0.25 of the
    // total before normalization (0.25 / 1.25 = 0.2 after).
    for _ in 0..200 {
        within("GRV", reader.get_read_version()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let busy_late = busy.requests.lock().unwrap().len() - busy_before;
    let idle_late = idle.requests.lock().unwrap().len() - idle_before;
    println!("late share: {busy_late} to the busy proxy, {idle_late} to the idle one");
    assert!(busy_late + idle_late >= 200);
    assert!(
        busy_late * 10 < (busy_late + idle_late) * 4,
        "{busy_late} vs {idle_late}"
    );
    assert!(busy_late > 0, "the busy proxy is still used");
}

#[tokio::test(start_paused = true)]
async fn grv_proxies_without_load_stay_uniform() {
    let mut cluster = Cluster::new(SchemaVersion::V8_0, 1).await;
    let a = cluster.grv_proxy(10);
    let b = cluster.grv_proxy(11);
    let commit = cluster.commit_proxy(20, 1);
    cluster.publish(&[&a, &b], &[&commit]);
    // Little load (less than 20 requests per proxy): no adjustment, whatever the CPU says.
    a.busy.store(5 * 10_000 + 9_000, Ordering::SeqCst);
    b.busy.store(1_000, Ordering::SeqCst);
    let reader = cluster.reader();
    for _ in 0..300 {
        within("GRV", reader.get_read_version()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let (na, nb) = (
        a.requests.lock().unwrap().len(),
        b.requests.lock().unwrap().len(),
    );
    assert!(na > 100 && nb > 100, "{na} {nb}");
}

#[tokio::test(start_paused = true)]
async fn metadata_version_comes_from_read_versions_and_commits() {
    for schema in SchemaVersion::ALL {
        let (cluster, grv, _commit) = Cluster::ready(schema, 1).await;
        // What the storage server has for the key is not what the proxies report: reads
        // answered by the cache are told apart.
        cluster.set(METADATA_VERSION_KEY, b"from storage");
        let reader = cluster.reader();
        let served = || cluster.storage[0].served();

        assert_eq!(reader.cached_metadata_version(1000), None);
        *grv.metadata.lock().unwrap() = Some(b"m1".to_vec());
        let (v1, m1) = within(
            "GRV",
            reader.get_read_version_and_metadata(ReadVersionOptions::default()),
        )
        .await
        .unwrap();
        assert_eq!(m1.as_deref(), Some(&b"m1"[..]));
        assert_eq!(
            reader.cached_metadata_version(v1),
            Some(Some(b"m1".to_vec()))
        );
        assert_eq!(
            within("metadata", reader.metadata_version(v1))
                .await
                .unwrap()
                .as_deref(),
            Some(&b"m1"[..])
        );
        assert_eq!(served(), 0, "{schema}: served from the cache");

        // The key not being set is cached too.
        *grv.metadata.lock().unwrap() = None;
        let v2 = within("GRV", reader.get_read_version()).await.unwrap();
        assert!(v2 > v1);
        assert_eq!(reader.cached_metadata_version(v2), Some(None));
        assert_eq!(
            within("metadata", reader.metadata_version(v2))
                .await
                .unwrap(),
            None
        );
        assert_eq!(served(), 0);
        // And the older one is still there.
        assert_eq!(
            reader.cached_metadata_version(v1),
            Some(Some(b"m1".to_vec()))
        );

        // A version no reply was for is read from the storage server.
        assert_eq!(reader.cached_metadata_version(v2 + 1), None);
        let read = within("metadata", reader.metadata_version(v2 + 1))
            .await
            .unwrap();
        assert_eq!(read.as_deref(), Some(&b"from storage"[..]));
        assert_eq!(served(), 1);

        // A locked database fails the read version for a transaction that is not lock aware,
        // before its reply is cached (the proxy's version moves on); a lock-aware one
        // caches it.
        *grv.answer.lock().unwrap() = Ok(true);
        *grv.metadata.lock().unwrap() = Some(b"locked".to_vec());
        let e = within("GRV", reader.get_read_version()).await.unwrap_err();
        assert_eq!(e.code(), codes::DATABASE_LOCKED);
        let mut options = ReadVersionOptions::default();
        options.lock_aware = true;
        let (v3, m3) = within("GRV", reader.get_read_version_and_metadata(options))
            .await
            .unwrap();
        assert_eq!(m3.as_deref(), Some(&b"locked"[..]));
        assert_eq!(v3, v2 + 20, "the refused reply consumed a version");
        assert_eq!(reader.cached_metadata_version(v3 - 10), None);
        assert_eq!(
            reader.cached_metadata_version(v3),
            Some(Some(b"locked".to_vec()))
        );

        // A commit's version and metadata version are remembered, if newer than the rest.
        reader.note_committed_version(v3 + 100, Some(b"c".to_vec()));
        assert_eq!(
            within("metadata", reader.metadata_version(v3 + 100))
                .await
                .unwrap()
                .as_deref(),
            Some(&b"c"[..])
        );
        reader.note_committed_version(v3 + 50, Some(b"stale".to_vec()));
        assert_eq!(
            reader.cached_metadata_version(v3 + 50),
            None,
            "older than the newest"
        );
        assert_eq!(served(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn the_metadata_version_cache_is_bounded() {
    let (cluster, grv, _commit) = Cluster::ready(SchemaVersion::V7_4, 1).await;
    let mut config = ReadConfig::default();
    config.metadata_version_cache_size = 3;
    let reader = cluster.reader_with(config);
    let mut versions = Vec::new();
    for i in 0..5u8 {
        *grv.metadata.lock().unwrap() = Some(vec![i]);
        versions.push(within("GRV", reader.get_read_version()).await.unwrap());
    }
    for (i, v) in versions.iter().enumerate() {
        let cached = reader.cached_metadata_version(*v);
        if i < 2 {
            assert_eq!(cached, None, "{i}");
        } else {
            assert_eq!(cached, Some(Some(vec![i as u8])), "{i}");
        }
    }
}
