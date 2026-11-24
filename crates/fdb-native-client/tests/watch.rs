//! Watches against a scripted fake cluster (a coordinator, a GRV proxy, a commit proxy that
//! also serves key locations and applies commits, and storage servers that serve `GetValue`
//! and `WatchValue`) on the in-memory network under tokio's paused clock, with a real
//! `Database`: a watch starts when its transaction commits and fires when the value changes;
//! it re-sends after `wrong_shard_server`, `timed_out`, `watch_cancelled`, `future_version`
//! and a broken connection; the database-wide watch map joins watches of one key and value;
//! the cap; cancellation; and shutdown. The real server's behaviour is in `docker_watch.rs`.

#![cfg(feature = "tokio")]

mod support;

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_client::atomic;
use fdb_native_client::cluster::ConnectionRecord;
use fdb_native_client::error::codes;
use fdb_native_client::read::KEY_SPACE_END;
use fdb_native_client::watch::WatchConfig;
use fdb_native_client::{Database, DatabaseOptions, Error, MutationType, Watch};
use fdb_native_schema::domain::{
    Commit, CommitId, GetKeyServerLocations, GetReadVersion, GetValue, GetValueReply, KeyRange,
    KeyServerLocations, Mutation, MutationType as Mt, ReadVersion, StorageServer, WatchValue,
    WatchValueReply,
};
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::peer::PeerManagerConfig;
use fdb_native_wire::{FdbError, ProtocolVersion, Uid};
use support::process::{Action, FakeProcess, Incoming, Responder};
use support::{FakeCoordinator, MemRuntime, Net, info, sock, until};
use tokio::time::Instant;

const SCHEMAS: [SchemaVersion; 2] = [SchemaVersion::V7_4, SchemaVersion::V8_0];
const LIMIT: Duration = Duration::from_secs(120);

/// What a storage server does with its next `WatchValue` request, in place of the normal.
#[derive(Clone, Copy, Debug)]
enum Script {
    /// Answers with an error.
    Error(u16),
    /// Hangs up.
    Close,
}

struct PendingWatch {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    reply: Uid,
    responder: Responder,
    schema: SchemaVersion,
}

/// What the fake cluster knows.
struct World {
    schema: SchemaVersion,
    data: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    version: AtomicI64,
    /// How far the GRV proxy's versions lag behind the storage servers'.
    grv_lag: AtomicI64,
    grv_requests: AtomicUsize,
    /// The versions `GetValue` was asked at.
    reads: Mutex<Vec<i64>>,
    /// The `WatchValue` requests, the server (index) and the time they reached.
    watch_requests: Mutex<Vec<(WatchValue, usize, Instant)>>,
    pending: Mutex<Vec<PendingWatch>>,
    script: Mutex<VecDeque<Script>>,
    location_requests: AtomicUsize,
    commits: Mutex<Vec<Commit>>,
    commit_errors: Mutex<VecDeque<u16>>,
    /// The server that holds the shard.
    shard: AtomicUsize,
    servers: Mutex<Vec<StorageServer>>,
}

impl World {
    fn requests(&self) -> Vec<(WatchValue, usize, Instant)> {
        self.watch_requests.lock().unwrap().clone()
    }

    fn request_count(&self) -> usize {
        self.watch_requests.lock().unwrap().len()
    }

    /// Changes `key`'s value like a commit would, and answers the watches it fires.
    fn set_value(&self, key: &[u8], value: Option<&[u8]>) {
        let version = self.set_quietly(key, value);
        self.fire(version);
    }

    /// Changes `key`'s value without telling the watching storage servers.
    fn set_quietly(&self, key: &[u8], value: Option<&[u8]>) -> i64 {
        let mut data = self.data.lock().unwrap();
        match value {
            Some(v) => data.insert(key.to_vec(), v.to_vec()),
            None => data.remove(key),
        };
        self.version.fetch_add(10, Ordering::SeqCst) + 10
    }

    /// The watches whose value differs now are answered.
    fn fire(&self, version: i64) {
        let data = self.data.lock().unwrap();
        self.pending.lock().unwrap().retain(|p| {
            if data.get(&p.key) == p.value.as_ref() {
                return true;
            }
            let reply = WatchValue::encode_reply(
                p.schema,
                &Ok(WatchValueReply {
                    version,
                    cached: false,
                }),
            )
            .unwrap();
            p.responder.send(p.reply, reply);
            false
        });
    }

    fn script(&self, script: Script) {
        self.script.lock().unwrap().push_back(script);
    }

    fn set_shard(&self, server: usize) {
        self.shard.store(server, Ordering::SeqCst);
    }
}

struct Env {
    world: Arc<World>,
    db: Database<MemRuntime>,
    _coord: FakeCoordinator,
    _processes: Vec<FakeProcess>,
}

impl std::ops::Deref for Env {
    type Target = World;
    fn deref(&self) -> &World {
        &self.world
    }
}

fn storage_handlers(world: &Arc<World>, process: &FakeProcess, index: usize) -> StorageServer {
    let schema = world.schema;
    let server = StorageServer {
        id: Uid::new(0x55, index as u64),
        locality: BTreeMap::new(),
        base: process.endpoint(Uid::new(0x5500 + index as u64, 0x30)),
        tss_pair_id: None,
        accepting_requests: true,
    };
    let streams = server.endpoints();
    {
        let world = world.clone();
        process.handle(streams.get_value.token, move |incoming: Incoming| {
            let (request, reply) =
                GetValue::decode_request(incoming.schema, &incoming.payload).unwrap();
            world.reads.lock().unwrap().push(request.version);
            let data = world.data.lock().unwrap();
            let answer = GetValueReply {
                value: data.get(&request.key).map(Vec::as_slice),
                ..GetValueReply::default()
            };
            let payload = GetValue::encode_reply(incoming.schema, &Ok(answer)).unwrap();
            Action::reply(reply, payload)
        });
    }
    {
        let world = world.clone();
        process.handle(streams.watch_value.token, move |incoming: Incoming| {
            let (request, reply) =
                WatchValue::decode_request(incoming.schema, &incoming.payload).unwrap();
            world
                .watch_requests
                .lock()
                .unwrap()
                .push((request.clone(), index, Instant::now()));
            let scripted = world.script.lock().unwrap().pop_front();
            match scripted {
                Some(Script::Close) => return Action::Close,
                Some(Script::Error(code)) => {
                    let payload =
                        WatchValue::encode_reply(incoming.schema, &Err(FdbError::new(code)))
                            .unwrap();
                    return Action::reply(reply, payload);
                }
                None => {}
            }
            let version = world.version.load(Ordering::SeqCst);
            let differs = world.data.lock().unwrap().get(&request.key) != request.value.as_ref();
            if differs {
                let payload = WatchValue::encode_reply(
                    incoming.schema,
                    &Ok(WatchValueReply {
                        version,
                        cached: false,
                    }),
                )
                .unwrap();
                return Action::reply(reply, payload);
            }
            world.pending.lock().unwrap().push(PendingWatch {
                key: request.key,
                value: request.value,
                reply,
                responder: incoming.responder,
                schema: incoming.schema,
            });
            Action::Ignore
        });
    }
    let _ = schema;
    server
}

async fn env(schema: SchemaVersion, servers: usize, watch: WatchConfig) -> Env {
    let net = Net::new();
    let rt = MemRuntime::new(&net);
    let version = schema.protocol_version();
    let mut coord = FakeCoordinator::start(&net, sock(1, 4500), version);
    let info = info(1, 1, 1, schema);

    let world = Arc::new(World {
        schema,
        data: Mutex::new(BTreeMap::new()),
        version: AtomicI64::new(1000),
        grv_lag: AtomicI64::new(0),
        grv_requests: AtomicUsize::new(0),
        reads: Mutex::new(Vec::new()),
        watch_requests: Mutex::new(Vec::new()),
        pending: Mutex::new(Vec::new()),
        script: Mutex::new(VecDeque::new()),
        location_requests: AtomicUsize::new(0),
        commits: Mutex::new(Vec::new()),
        commit_errors: Mutex::new(VecDeque::new()),
        shard: AtomicUsize::new(0),
        servers: Mutex::new(Vec::new()),
    });
    let mut processes = Vec::new();

    // Storage servers.
    for i in 0..servers {
        let process = FakeProcess::start(&net, sock(30 + i as u8, 4500), version);
        let server = storage_handlers(&world, &process, i);
        world.servers.lock().unwrap().push(server);
        processes.push(process);
    }

    // The GRV proxy.
    let grv = FakeProcess::start(&net, sock(100, 4500), version);
    {
        let world = world.clone();
        grv.handle(info.grv_proxies[0].base.token, move |incoming: Incoming| {
            let (_, reply) =
                GetReadVersion::decode_request(incoming.schema, &incoming.payload).unwrap();
            world.grv_requests.fetch_add(1, Ordering::SeqCst);
            let version =
                world.version.load(Ordering::SeqCst) - world.grv_lag.load(Ordering::SeqCst);
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
            let payload = GetReadVersion::encode_reply(incoming.schema, &Ok(rv)).unwrap();
            Action::reply(reply, payload)
        });
    }
    processes.push(grv);

    // The commit proxy: commits and locations.
    let proxy = FakeProcess::start(&net, sock(150, 4500), version);
    let streams = info.commit_proxies[0].endpoints();
    {
        let world = world.clone();
        proxy.handle(streams.commit.token, move |incoming: Incoming| {
            let (request, reply) =
                Commit::decode_request(incoming.schema, &incoming.payload).unwrap();
            let scripted = world.commit_errors.lock().unwrap().pop_front();
            let answer = match scripted {
                Some(code) => Err(FdbError::new(code)),
                None => {
                    {
                        let mut data = world.data.lock().unwrap();
                        for Mutation {
                            kind,
                            param1,
                            param2,
                        } in &request.mutations
                        {
                            match *kind {
                                Mt::SET_VALUE => {
                                    data.insert(param1.clone(), param2.clone());
                                }
                                Mt::CLEAR_RANGE => data.retain(|k, _| !(param1 <= k && k < param2)),
                                Mt::SET_VERSIONSTAMPED_KEY | Mt::SET_VERSIONSTAMPED_VALUE => {}
                                // The storage server's `convertAtomicOp`.
                                op => {
                                    let old = data.get(param1).map(Vec::as_slice);
                                    match atomic::apply(op, old, param2, atomic::VALUE_SIZE_LIMIT)
                                        .unwrap()
                                    {
                                        Some(v) => data.insert(param1.clone(), v),
                                        None => data.remove(param1),
                                    };
                                }
                            }
                        }
                    }
                    world.commits.lock().unwrap().push(request);
                    let version = world.version.fetch_add(10, Ordering::SeqCst) + 10;
                    world.fire(version);
                    Ok(CommitId {
                        version,
                        txn_batch_id: 0,
                        ..CommitId::default()
                    })
                }
            };
            let payload = Commit::encode_reply(incoming.schema, &answer).unwrap();
            Action::reply(reply, payload)
        });
    }
    {
        let world = world.clone();
        proxy.handle(
            streams.get_key_servers_locations.token,
            move |incoming: Incoming| {
                let (_, reply) =
                    GetKeyServerLocations::decode_request(incoming.schema, &incoming.payload)
                        .unwrap();
                world.location_requests.fetch_add(1, Ordering::SeqCst);
                let server =
                    world.servers.lock().unwrap()[world.shard.load(Ordering::SeqCst)].clone();
                let locations = KeyServerLocations {
                    results: vec![(
                        KeyRange {
                            begin: Vec::new(),
                            end: KEY_SPACE_END.to_vec(),
                        },
                        vec![server],
                    )],
                    tss_mapping: Vec::new(),
                    tag_mapping: Vec::new(),
                };
                let payload =
                    GetKeyServerLocations::encode_reply(incoming.schema, &Ok(locations)).unwrap();
                Action::reply(reply, payload)
            },
        );
    }
    processes.push(proxy);

    let mut options = DatabaseOptions::default();
    options.peers = PeerManagerConfig::default().without_jitter();
    options.peers.supported_versions = vec![ProtocolVersion::V7_4, ProtocolVersion::V8_0];
    options.peers.seed = Some(1);
    options.monitor.seed = Some(7);
    options.commit.seed = Some(3);
    options.read.seed = Some(11);
    options.watch = watch;
    let record = ConnectionRecord::memory("test:abc@10.0.0.1:4500".parse().unwrap());
    let db = Database::from_record(rt, record, options).expect("database");
    coord.next_call().await.reply(&info);
    db.cluster_monitor().proxies(false).await.unwrap();
    Env {
        world,
        db,
        _coord: coord,
        _processes: processes,
    }
}

async fn standard(schema: SchemaVersion) -> Env {
    env(schema, 1, WatchConfig::default()).await
}

async fn within<T>(what: &str, f: impl Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: no result within {LIMIT:?}"))
}

/// Whether `watch` is still pending after a short while.
async fn pending(watch: &mut Watch) -> bool {
    tokio::time::timeout(Duration::from_millis(100), &mut *watch)
        .await
        .is_err()
}

/// The watch's result if it is ready now.
async fn ready(watch: &mut Watch) -> Option<Result<(), Error>> {
    tokio::time::timeout(Duration::ZERO, &mut *watch).await.ok()
}

async fn resolved(what: &str, watch: &mut Watch) -> Result<(), Error> {
    within(what, watch).await
}

fn code(result: Result<(), Error>) -> u16 {
    result.unwrap_err().code()
}

#[tokio::test(start_paused = true)]
async fn a_watch_starts_at_commit_and_fires_when_the_value_changes() {
    for schema in SCHEMAS {
        let env = standard(schema).await;
        let tx = env.db.create_transaction();
        let mut watch = tx.watch(b"k").await.unwrap();
        assert_eq!(env.db.outstanding_watches(), 1);

        // Nothing is watched before the commit.
        assert!(pending(&mut watch).await);
        assert_eq!(env.request_count(), 0);

        // A read-only transaction commits without the cluster, and starts its watches at its
        // read version, with the value it read.
        let result = tx.commit().await.unwrap();
        assert_eq!(result.committed_version(), -1);
        until("the watch request", || env.request_count() == 1).await;
        let (request, server, _) = env.requests().remove(0);
        assert_eq!(server, 0);
        assert_eq!(request.key, b"k");
        assert_eq!(request.value, None);
        let reads = env.reads.lock().unwrap().clone();
        assert_eq!(reads.len(), 1, "the watch read the key");
        assert_eq!(request.version, reads[0]);
        assert!(pending(&mut watch).await);

        env.set_value(b"k", Some(b"v"));
        resolved("the watch", &mut watch).await.unwrap();
        // Done: nothing outstanding or watched.
        assert_eq!(env.db.outstanding_watches(), 0);
        until("the map to empty", || env.db.watched_keys() == 0).await;
    }
}

/// `watch` starts when it is called (C++'s watch future): a watch whose future is awaited
/// only after the commit was registered by that commit.
#[tokio::test(start_paused = true)]
async fn a_watch_starts_when_called_and_may_be_awaited_after_the_commit() {
    let env = standard(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let watch = tx.watch(b"k");
    tx.commit().await.unwrap();
    let mut watch = within("the watch", watch).await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    assert!(pending(&mut watch).await);
    env.set_value(b"k", Some(b"v"));
    resolved("the watch", &mut watch).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_watch_of_a_retry_loop_returns_pending_and_fires() {
    let env = standard(SchemaVersion::V8_0).await;
    env.set_value(b"k", Some(b"old"));
    let mut watch = env
        .db
        .run(|tx, _| async move { tx.watch(b"k").await })
        .await
        .unwrap();
    until("the watch request", || env.request_count() == 1).await;
    assert_eq!(env.requests()[0].0.value.as_deref(), Some(&b"old"[..]));
    assert!(pending(&mut watch).await);
    env.set_value(b"k", Some(b"new"));
    resolved("the watch", &mut watch).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_watch_of_a_written_key_watches_the_written_value_at_the_commit_version() {
    for schema in SCHEMAS {
        let env = standard(schema).await;
        env.set_value(b"k", Some(b"before"));
        let tx = env.db.create_transaction();
        tx.set(b"k", b"mine");
        let mut watch = tx.watch(b"k").await.unwrap();
        assert!(
            env.reads.lock().unwrap().is_empty(),
            "the transaction's own write is the value: nothing is read"
        );
        let result = tx.commit().await.unwrap();
        assert!(result.committed_version() > 0);
        until("the watch request", || env.request_count() == 1).await;
        let (request, _, _) = env.requests().remove(0);
        assert_eq!(request.value.as_deref(), Some(&b"mine"[..]));
        assert_eq!(request.version, result.committed_version());
        // The commit's own write does not fire it.
        assert!(pending(&mut watch).await);
        env.set_value(b"k", Some(b"theirs"));
        resolved("the watch", &mut watch).await.unwrap();

        // A cleared key is a key with no value.
        let tx = env.db.create_transaction();
        tx.clear(b"k");
        let mut watch = tx.watch(b"k").await.unwrap();
        tx.commit().await.unwrap();
        until("the second watch request", || env.request_count() == 2).await;
        assert_eq!(env.requests()[1].0.value, None);
        assert!(pending(&mut watch).await);
        env.set_value(b"k", Some(b"again"));
        resolved("the watch", &mut watch).await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn a_pending_atomic_operation_applies_to_the_value_at_the_read_version() {
    for schema in SCHEMAS {
        let env = standard(schema).await;
        env.set_value(b"n", Some(&5u64.to_le_bytes()));
        let tx = env.db.create_transaction();
        tx.atomic_op(b"n", &1u64.to_le_bytes(), MutationType::Add);
        let mut watch = tx.watch(b"n").await.unwrap();
        // The key was read at the read version (a dependent write: RYW reads the database).
        let read_version = tx.get_read_version().await.unwrap();
        assert_eq!(env.reads.lock().unwrap().clone(), vec![read_version]);
        let result = tx.commit().await.unwrap();
        // With a read conflict range on the key (`updateConflictMap`).
        let commit = env.commits.lock().unwrap().last().unwrap().clone();
        assert!(
            commit
                .read_conflict_ranges
                .contains(&KeyRange::single_key(b"n")),
            "{:?}",
            commit.read_conflict_ranges
        );
        until("the watch request", || env.request_count() == 1).await;
        let (request, _, _) = env.requests().remove(0);
        // The value at the read version with the transaction's own addition applied.
        assert_eq!(request.value, Some(6u64.to_le_bytes().to_vec()));
        assert_eq!(request.version, result.committed_version());
        assert_eq!(
            env.reads.lock().unwrap().len(),
            1,
            "nothing read after the commit"
        );
        // The commit's own addition does not fire it, another change does.
        assert!(pending(&mut watch).await);
        env.set_value(b"n", Some(&7u64.to_le_bytes()));
        resolved("the watch", &mut watch).await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn atomic_operations_fold_into_the_value_the_transaction_wrote() {
    let env = standard(SchemaVersion::V8_0).await;
    env.set_value(b"a", Some(b"db"));
    let tx = env.db.create_transaction();
    // A set, then atomic operations: known without a read.
    tx.set(b"a", &5u16.to_le_bytes());
    tx.atomic_op(b"a", &3u16.to_le_bytes(), MutationType::Add);
    tx.atomic_op(b"a", &4u16.to_le_bytes(), MutationType::Max);
    // A clear, then an operation: from no value (`MinV2` of nothing is the operand).
    tx.clear(b"b");
    tx.atomic_op(b"b", b"\x09", MutationType::Min);
    // A clear, then a compare-and-clear of nothing: still nothing.
    tx.clear(b"c");
    tx.atomic_op(b"c", b"x", MutationType::CompareAndClear);
    // Dependent operations, in order, on the database's value.
    env.set_value(b"d", Some(b"\x01\x02"));
    tx.atomic_op(b"d", b"\xff\x00", MutationType::Add);
    tx.atomic_op(b"d", b"q", MutationType::AppendIfFits);
    let mut watches = Vec::new();
    for key in [b"a", b"b", b"c", b"d"] {
        watches.push(tx.watch(key).await.unwrap());
    }
    let read_version = tx.get_read_version().await.unwrap();
    assert_eq!(
        env.reads.lock().unwrap().clone(),
        vec![read_version],
        "only `d` depends on the database"
    );
    tx.commit().await.unwrap();
    until("the watch requests", || env.request_count() == 4).await;
    let mut requests: Vec<_> = env
        .requests()
        .into_iter()
        .map(|(r, _, _)| (r.key, r.value))
        .collect();
    requests.sort();
    assert_eq!(
        requests,
        vec![
            (b"a".to_vec(), Some(8u16.to_le_bytes().to_vec())),
            (b"b".to_vec(), Some(b"\x09".to_vec())),
            (b"c".to_vec(), None),
            (b"d".to_vec(), Some(b"\x00\x03q".to_vec())),
        ]
    );
    // Those values are what the commit wrote (the fake proxy applies `Atomic.h` too): no
    // watch fired.
    for watch in &mut watches {
        assert!(pending(watch).await);
    }
}

#[tokio::test(start_paused = true)]
async fn a_later_write_fires_a_watch_of_a_key_with_a_pending_atomic_operation() {
    let env = standard(SchemaVersion::V7_4).await;
    let tx = env.db.create_transaction();
    tx.atomic_op(b"n", &1u64.to_le_bytes(), MutationType::Add);
    tx.atomic_op(b"m", &1u64.to_le_bytes(), MutationType::Add);
    let mut by_set = tx.watch(b"n").await.unwrap();
    let mut by_clear = tx.watch(b"m").await.unwrap();
    // The value the operation gives (the key is not set: 1): nothing changes.
    tx.set(b"n", &1u64.to_le_bytes());
    assert!(ready(&mut by_set).await.is_none());
    // Another value, a clear: they fire at once.
    tx.set(b"n", &2u64.to_le_bytes());
    tx.clear(b"m");
    assert!(ready(&mut by_set).await.unwrap().is_ok());
    assert!(ready(&mut by_clear).await.unwrap().is_ok());
    assert_eq!(env.db.outstanding_watches(), 0);
}

#[tokio::test(start_paused = true)]
async fn concurrent_increments_fire_each_others_watches() {
    // Two transactions increment a counter and watch it, and both commit (the fake proxy
    // resolves no conflicts, as if both were in one batch without the read conflict range):
    // the first to commit sees the other's increment as a change. Its watch value is its own
    // view (read version + its addition), not the value at its commit version, which already
    // has both increments.
    for schema in SCHEMAS {
        let env = standard(schema).await;
        let (t1, t2) = (env.db.create_transaction(), env.db.create_transaction());
        for tx in [&t1, &t2] {
            tx.atomic_op(b"n", &1u64.to_le_bytes(), MutationType::Add);
        }
        let mut w1 = t1.watch(b"n").await.unwrap();
        let mut w2 = t2.watch(b"n").await.unwrap();
        t2.commit().await.unwrap();
        t1.commit().await.unwrap();
        assert_eq!(
            env.data.lock().unwrap().get(&b"n"[..]),
            Some(&2u64.to_le_bytes().to_vec())
        );
        // Both watched 1; the key is 2 by the time either watch reaches the server.
        resolved("the first watch", &mut w1).await.unwrap();
        resolved("the second watch", &mut w2).await.unwrap();
        // Both commits carried the key as a read conflict range: a real resolver fails one.
        for commit in env.commits.lock().unwrap().iter() {
            assert!(
                commit
                    .read_conflict_ranges
                    .contains(&KeyRange::single_key(b"n"))
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn writes_after_the_watch_that_change_the_value_fire_it_at_once() {
    let env = standard(SchemaVersion::V8_0).await;
    env.set_value(b"a", Some(b"1"));
    env.set_value(b"b", Some(b"1"));
    let tx = env.db.create_transaction();
    let mut by_set = tx.watch(b"a").await.unwrap();
    let mut same = tx.watch(b"b").await.unwrap();
    let mut by_clear_range = tx.watch(b"c").await.unwrap(); // not set
    let mut by_atomic = tx.watch(b"d").await.unwrap(); // not set
    let untouched = tx.watch(b"e").await.unwrap();
    assert_eq!(env.db.outstanding_watches(), 5);
    assert!(pending(&mut by_set).await);

    tx.set(b"a", b"2");
    tx.set(b"b", b"1"); // the value it has: nothing changed
    tx.clear_range(b"c", b"d"); // clearing what is not there changes nothing
    tx.atomic_op(b"d", &[1], MutationType::Add); // its result is not known
    assert!(ready(&mut by_set).await.unwrap().is_ok());
    assert!(ready(&mut by_atomic).await.unwrap().is_ok());
    assert!(ready(&mut same).await.is_none());
    assert!(ready(&mut by_clear_range).await.is_none());
    // The ones that fired no longer count.
    assert_eq!(env.db.outstanding_watches(), 3);

    // A set and then another value: the second fires the watch of `b`.
    tx.set(b"b", b"3");
    assert!(ready(&mut same).await.unwrap().is_ok());
    // A clear range that covers a value that is set does.
    let tx2 = env.db.create_transaction();
    let mut covered = tx2.watch(b"a").await.unwrap();
    tx2.clear_range(b"a", b"b");
    assert!(ready(&mut covered).await.unwrap().is_ok());

    // The others start at the commit.
    tx.commit().await.unwrap();
    until("the watch requests", || env.request_count() == 2).await;
    let mut keys: Vec<_> = env.requests().into_iter().map(|(r, _, _)| r.key).collect();
    keys.sort();
    assert_eq!(keys, vec![b"c".to_vec(), b"e".to_vec()]);
    drop((untouched, by_clear_range));
}

#[tokio::test(start_paused = true)]
async fn a_failed_commit_fails_its_watches_with_its_error() {
    let env = standard(SchemaVersion::V8_0).await;
    env.commit_errors
        .lock()
        .unwrap()
        .push_back(codes::NOT_COMMITTED);
    let tx = env.db.create_transaction();
    tx.set(b"x", b"1");
    let mut watch = tx.watch(b"k").await.unwrap();
    let e = tx.commit().await.unwrap_err();
    assert_eq!(e.code(), codes::NOT_COMMITTED);
    assert_eq!(
        code(resolved("the watch", &mut watch).await),
        codes::NOT_COMMITTED
    );
    assert_eq!(env.request_count(), 0);
    assert_eq!(env.db.outstanding_watches(), 0);

    // An unknown result too: the watch gets the commit's error.
    env.commit_errors
        .lock()
        .unwrap()
        .push_back(codes::COMMIT_UNKNOWN_RESULT);
    let tx = env.db.create_transaction();
    tx.set(b"x", b"1");
    let mut watch = tx.watch(b"k").await.unwrap();
    tx.commit().await.unwrap_err();
    assert_eq!(
        code(resolved("the watch", &mut watch).await),
        codes::COMMIT_UNKNOWN_RESULT
    );
    assert_eq!(env.request_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_reset_or_dropped_transaction_cancels_its_watches() {
    let env = standard(SchemaVersion::V8_0).await;

    let tx = env.db.create_transaction();
    let mut by_reset = tx.watch(b"k").await.unwrap();
    assert!(pending(&mut by_reset).await);
    tx.reset();
    assert_eq!(
        code(resolved("the watch", &mut by_reset).await),
        codes::TRANSACTION_CANCELLED
    );

    let mut by_drop = tx.watch(b"k").await.unwrap();
    drop(tx);
    assert_eq!(
        code(resolved("the watch", &mut by_drop).await),
        codes::TRANSACTION_CANCELLED
    );

    // on_error resets the transaction: the watches of the failed attempt are cancelled, and a
    // retry loop's later attempt makes its own.
    let attempts = Arc::new(Mutex::new(Vec::<Watch>::new()));
    let kept = attempts.clone();
    let mut last = env
        .db
        .run(move |tx, _| {
            let kept = kept.clone();
            async move {
                let watch = tx.watch(b"k").await?;
                if kept.lock().unwrap().is_empty() {
                    kept.lock().unwrap().push(watch);
                    return Err(Error::new(codes::NOT_COMMITTED));
                }
                Ok(watch)
            }
        })
        .await
        .unwrap();
    let mut first = attempts.lock().unwrap().remove(0);
    assert_eq!(
        code(resolved("the first attempt's watch", &mut first).await),
        codes::TRANSACTION_CANCELLED
    );
    until("the second attempt's watch", || env.request_count() == 1).await;
    assert!(pending(&mut last).await);
    assert_eq!(env.db.outstanding_watches(), 1);
}

#[tokio::test(start_paused = true)]
async fn wrong_shard_server_invalidates_the_location_and_asks_again() {
    for schema in SCHEMAS {
        let env = env(schema, 2, WatchConfig::default()).await;
        let tx = env.db.create_transaction();
        let mut watch = tx.watch(b"k").await.unwrap();
        assert_eq!(env.location_requests.load(Ordering::SeqCst), 1);
        // The shard moves; the first server says so.
        env.set_shard(1);
        env.script(Script::Error(codes::WRONG_SHARD_SERVER));
        tx.commit().await.unwrap();
        until("the second watch request", || env.request_count() == 2).await;
        let requests = env.requests();
        assert_eq!(requests[0].1, 0);
        assert_eq!(requests[1].1, 1, "the watch followed the shard");
        assert_eq!(env.location_requests.load(Ordering::SeqCst), 2);
        let waited = requests[1].2 - requests[0].2;
        assert!(waited >= Duration::from_millis(10), "{waited:?}");
        assert!(pending(&mut watch).await);
        env.set_value(b"k", Some(b"v"));
        resolved("the watch", &mut watch).await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn the_watch_is_sent_again_after_the_errors_c_plus_plus_retries() {
    // (error, the wait before the second request)
    let cases = [
        (codes::TIMED_OUT, Duration::from_millis(10)),
        (codes::WATCH_CANCELLED, Duration::from_secs(1)),
        (codes::PROCESS_BEHIND, Duration::from_secs(1)),
        (codes::FUTURE_VERSION, Duration::from_millis(10)),
        (codes::SERVER_OVERLOADED, Duration::from_millis(0)),
    ];
    for schema in SCHEMAS {
        for (error, wait) in cases {
            let env = standard(schema).await;
            env.script(Script::Error(error));
            let tx = env.db.create_transaction();
            let mut watch = tx.watch(b"k").await.unwrap();
            tx.commit().await.unwrap();
            until("the second watch request", || env.request_count() == 2).await;
            let requests = env.requests();
            let waited = requests[1].2 - requests[0].2;
            assert!(waited >= wait, "error {error}: {waited:?} < {wait:?}");
            assert!(
                waited < wait + Duration::from_millis(500),
                "error {error}: {waited:?}"
            );
            assert_eq!(requests[0].0, requests[1].0, "the same request");
            assert!(pending(&mut watch).await, "error {error}");
            env.set_value(b"k", Some(b"v"));
            resolved("the watch", &mut watch).await.unwrap();
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_hung_up_connection_sends_the_watch_again() {
    let env = standard(SchemaVersion::V7_4).await;
    env.script(Script::Close);
    let tx = env.db.create_transaction();
    let mut watch = tx.watch(b"k").await.unwrap();
    tx.commit().await.unwrap();
    until("the second watch request", || env.request_count() == 2).await;
    assert!(pending(&mut watch).await);
    env.set_value(b"k", Some(b"v"));
    resolved("the watch", &mut watch).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn another_error_fails_the_watch() {
    let env = standard(SchemaVersion::V8_0).await;
    env.script(Script::Error(codes::TRANSACTION_TOO_OLD));
    let tx = env.db.create_transaction();
    let mut watch = tx.watch(b"k").await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        code(resolved("the watch", &mut watch).await),
        codes::TRANSACTION_TOO_OLD
    );
    assert_eq!(env.request_count(), 1);
    assert_eq!(env.db.outstanding_watches(), 0);
    until("the map to empty", || env.db.watched_keys() == 0).await;
}

#[tokio::test(start_paused = true)]
async fn a_change_counts_once_its_version_is_committed() {
    let env = standard(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let mut watch = tx.watch(b"k").await.unwrap();
    tx.commit().await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    // The storage server saw the change, but the GRV proxies' committed version is behind
    // it (it may be rolled back): the client waits until they catch up.
    env.grv_lag.store(1000, Ordering::SeqCst);
    let before = env.grv_requests.load(Ordering::SeqCst);
    env.set_value(b"k", Some(b"v"));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(pending(&mut watch).await);
    assert!(env.grv_requests.load(Ordering::SeqCst) >= before + 10);
    env.grv_lag.store(0, Ordering::SeqCst);
    resolved("the watch", &mut watch).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn the_cap_refuses_one_more_watch() {
    let mut config = WatchConfig::default();
    config.max_outstanding_watches = 2;
    let env = env(SchemaVersion::V8_0, 1, config).await;
    let tx = env.db.create_transaction();
    let first = tx.watch(b"a").await.unwrap();
    let mut second = tx.watch(b"b").await.unwrap();
    assert_eq!(env.db.outstanding_watches(), 2);
    let e = tx.watch(b"c").await.unwrap_err();
    assert_eq!(e.code(), codes::TOO_MANY_WATCHES);
    assert_eq!(e.code(), 1032);
    // The failed one is not counted, and another transaction is refused too.
    assert_eq!(env.db.outstanding_watches(), 2);
    let other = env.db.create_transaction();
    assert_eq!(
        other.watch(b"c").await.unwrap_err().code(),
        codes::TOO_MANY_WATCHES
    );

    // A dropped watch frees its place.
    drop(first);
    assert_eq!(env.db.outstanding_watches(), 1);
    let third = other.watch(b"c").await.unwrap();
    assert_eq!(env.db.outstanding_watches(), 2);
    assert!(other.watch(b"d").await.is_err());

    // So does a watch that fires.
    tx.commit().await.unwrap();
    until("the watch request", || env.request_count() >= 1).await;
    env.set_value(b"b", Some(b"v"));
    resolved("the watch", &mut second).await.unwrap();
    assert_eq!(env.db.outstanding_watches(), 1);
    drop(third);
    assert_eq!(env.db.outstanding_watches(), 0);

    // The cap can be changed; what is outstanding stays.
    let mut kept = Vec::new();
    env.db.set_max_watches(3).unwrap();
    let tx = env.db.create_transaction();
    for key in [b"1", b"2", b"3"] {
        kept.push(tx.watch(key).await.unwrap());
    }
    assert_eq!(
        tx.watch(b"4").await.unwrap_err().code(),
        codes::TOO_MANY_WATCHES
    );
    env.db.set_max_watches(1).unwrap();
    assert_eq!(env.db.outstanding_watches(), 3);
    assert_eq!(
        env.db.set_max_watches(1_000_001).unwrap_err().code(),
        codes::INVALID_OPTION_VALUE
    );
    env.db.set_max_watches(1_000_000).unwrap();
    assert!(tx.watch(b"4").await.is_ok());
}

#[tokio::test(start_paused = true)]
async fn dropping_a_watch_cancels_it() {
    let env = standard(SchemaVersion::V8_0).await;

    // Dropped before the commit: never started.
    let tx = env.db.create_transaction();
    let watch = tx.watch(b"k").await.unwrap();
    drop(watch);
    assert_eq!(env.db.outstanding_watches(), 0);
    tx.commit().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(env.request_count(), 0);

    // Dropped while watching: the storage watch is given up, and the map forgets the key.
    let tx = env.db.create_transaction();
    let watch = tx.watch(b"k").await.unwrap();
    tx.commit().await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    assert_eq!(env.db.watched_keys(), 1);
    drop(watch);
    until("the map to empty", || env.db.watched_keys() == 0).await;
    assert_eq!(env.db.outstanding_watches(), 0);
    env.set_value(b"k", Some(b"v"));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(env.request_count(), 1, "no new request");
}

#[tokio::test(start_paused = true)]
async fn watches_of_one_key_and_value_share_one_request() {
    let env = standard(SchemaVersion::V8_0).await;
    let (a, b) = (env.db.create_transaction(), env.db.create_transaction());
    let mut wa = a.watch(b"k").await.unwrap();
    let mut wb = b.watch(b"k").await.unwrap();
    a.commit().await.unwrap();
    b.commit().await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(env.request_count(), 1, "one request for both");
    assert_eq!(env.db.watched_keys(), 1);
    assert_eq!(env.db.outstanding_watches(), 2);

    // One going away leaves the other.
    let c = env.db.create_transaction();
    let wc = c.watch(b"k").await.unwrap();
    c.commit().await.unwrap();
    drop(wc);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(env.db.watched_keys(), 1);
    assert!(pending(&mut wa).await);

    env.set_value(b"k", Some(b"v"));
    resolved("the first watch", &mut wa).await.unwrap();
    resolved("the second watch", &mut wb).await.unwrap();
    until("the map to empty", || env.db.watched_keys() == 0).await;
    assert_eq!(env.request_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_watch_of_another_value_replaces_an_older_one() {
    // `getWatchFuture`'s case 3: the new watch read a different value at a newer version, so
    // the value changed between the two, and the old watch fires.
    let env = standard(SchemaVersion::V8_0).await;
    let a = env.db.create_transaction();
    let mut wa = a.watch(b"k").await.unwrap();
    a.commit().await.unwrap();
    until("the first watch request", || env.request_count() == 1).await;
    env.set_quietly(b"k", Some(b"x"));
    let b = env.db.create_transaction();
    let mut wb = b.watch(b"k").await.unwrap();
    b.commit().await.unwrap();
    resolved("the old watch", &mut wa).await.unwrap();
    until("the second watch request", || env.request_count() == 2).await;
    assert_eq!(env.requests()[1].0.value.as_deref(), Some(&b"x"[..]));
    assert!(pending(&mut wb).await);
    env.set_value(b"k", Some(b"y"));
    resolved("the new watch", &mut wb).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_watch_of_another_value_at_an_older_version_fires_at_once() {
    // Case 4: the older watch read another value than the map's: it changed since.
    let env = standard(SchemaVersion::V8_0).await;
    let a = env.db.create_transaction();
    let mut wa = a.watch(b"k").await.unwrap();
    a.commit().await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    env.set_quietly(b"k", Some(b"x"));
    let b = env.db.create_transaction();
    b.set_read_version(5);
    let mut wb = b.watch(b"k").await.unwrap();
    b.commit().await.unwrap();
    resolved("the older watch", &mut wb).await.unwrap();
    assert!(pending(&mut wa).await);
    assert_eq!(env.request_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_watch_of_another_value_at_the_same_version_is_settled_by_a_read() {
    // Case 5: `sameVersionDiffValue` reads the key to see which of the two is stale.
    let env = standard(SchemaVersion::V8_0).await;
    let a = env.db.create_transaction();
    let mut wa = a.watch(b"k").await.unwrap();
    a.commit().await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    // The value changes without a new version (so the second watch has the same one).
    env.data
        .lock()
        .unwrap()
        .insert(b"k".to_vec(), b"x".to_vec());
    let b = env.db.create_transaction();
    let mut wb = b.watch(b"k").await.unwrap();
    b.commit().await.unwrap();
    // The map's watch is stale; the new one is the map's now.
    resolved("the stale watch", &mut wa).await.unwrap();
    until("the second watch request", || env.request_count() == 2).await;
    assert_eq!(env.requests()[1].0.value.as_deref(), Some(&b"x"[..]));
    assert!(pending(&mut wb).await);
    env.set_value(b"k", Some(b"y"));
    resolved("the new watch", &mut wb).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_fails_the_watches() {
    let env = standard(SchemaVersion::V8_0).await;
    let started = env.db.create_transaction();
    let mut watching = started.watch(b"k").await.unwrap();
    started.commit().await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    let unstarted = env.db.create_transaction();
    let mut waiting = unstarted.watch(b"j").await.unwrap();

    env.db.shutdown().await;
    assert_eq!(
        code(resolved("the watching watch", &mut watching).await),
        codes::OPERATION_CANCELLED
    );
    // A watch not committed yet fails when its transaction commits, or at once.
    let _ = unstarted.commit().await;
    assert_eq!(
        code(resolved("the waiting watch", &mut waiting).await),
        codes::OPERATION_CANCELLED
    );
    assert_eq!(env.db.outstanding_watches(), 0);
}

#[tokio::test(start_paused = true)]
async fn watch_checks_its_key_and_the_transaction() {
    let env = standard(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    assert_eq!(
        tx.watch(b"\xffsystem").await.unwrap_err().code(),
        codes::KEY_OUTSIDE_LEGAL_RANGE
    );
    assert_eq!(
        tx.watch(b"\xff\xff").await.unwrap_err().code(),
        codes::KEY_OUTSIDE_LEGAL_RANGE
    );
    assert_eq!(
        tx.watch(&[b'k'; 10_001]).await.unwrap_err().code(),
        codes::KEY_TOO_LARGE
    );
    // A deferred error of a write comes first, as for a read.
    tx.set(&[b'k'; 10_001], b"v");
    assert_eq!(
        tx.watch(b"k").await.unwrap_err().code(),
        codes::KEY_TOO_LARGE
    );
    assert_eq!(env.db.outstanding_watches(), 0);

    // `\xff/metadataVersion` may be watched without the system key options, as C++ allows
    // (`ReadYourWritesTransaction::watch`, 7.4:2451); a key next to it may not.
    let tx = env.db.create_transaction();
    assert_eq!(
        tx.watch(b"\xff/metadataVersion\x00")
            .await
            .unwrap_err()
            .code(),
        codes::KEY_OUTSIDE_LEGAL_RANGE
    );
    let mut watch = tx.watch(b"\xff/metadataVersion").await.unwrap();
    tx.commit().await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    let (request, _, _) = env.requests().remove(0);
    assert_eq!(request.key, b"\xff/metadataVersion");
    assert!(pending(&mut watch).await);
    env.set_value(b"\xff/metadataVersion", Some(&[1; 10]));
    resolved("the metadata version watch", &mut watch)
        .await
        .unwrap();

    // After the commit started, a watch is `used_during_commit`.
    let tx = env.db.create_transaction();
    tx.commit().await.unwrap();
    assert_eq!(
        tx.watch(b"k").await.unwrap_err().code(),
        codes::USED_DURING_COMMIT
    );
}

#[tokio::test(start_paused = true)]
async fn versionstamped_operations_trigger_watches_and_make_keys_unreadable() {
    let env = standard(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let read_version = tx.get_read_version().await.unwrap();
    let mut by_value = tx.watch(b"v").await.unwrap();
    let mut by_key = tx.watch(b"k").await.unwrap();
    // `k` + placeholder + offset 1.
    let mut key = b"k".to_vec();
    key.extend_from_slice(&[0xff; 10]);
    key.extend_from_slice(&1i32.to_le_bytes());
    // C++ triggers the key `transformVersionstampKey` makes: the placeholder stamped with
    // the read version + 1, the offset kept.
    let stamped = atomic::transform_versionstamp_key(&key, read_version + 1, 0).unwrap();
    let mut by_stamped = tx.watch(&stamped).await.unwrap();
    tx.set_versionstamped_key(&key, b"x");
    assert!(ready(&mut by_key).await.is_none());
    assert!(ready(&mut by_stamped).await.unwrap().is_ok());

    // The keys the versionstamped key may write are unreadable until the commit.
    let range = atomic::versionstamp_key_range(&key, Some(read_version), b"\xff").unwrap();
    let mut inside = b"k".to_vec();
    inside.extend_from_slice(&(read_version + 5).to_be_bytes());
    inside.extend_from_slice(&[0, 0]);
    assert!(range.begin <= inside && inside < range.end);
    for unreadable in [&range.begin, &inside, &stamped] {
        assert_eq!(
            tx.watch(unreadable).await.unwrap_err().code(),
            codes::ACCESSED_UNREADABLE
        );
    }
    // Outside the range, a watch reads as usual.
    let mut before = b"k".to_vec();
    before.extend_from_slice(&read_version.to_be_bytes());
    let before_watch = tx.watch(&before).await.unwrap();
    let end = range.end.clone();
    let end_watch = tx.watch(&end).await.unwrap();
    // A set keeps a key unreadable; a clear makes it readable (the value: none).
    tx.set(&inside, b"mine");
    assert_eq!(
        tx.watch(&inside).await.unwrap_err().code(),
        codes::ACCESSED_UNREADABLE
    );
    tx.clear(&inside);
    let inside_watch = tx.watch(&inside).await.unwrap();

    // A versionstamped value's result is not known: the watch fires at once, and the key is
    // unreadable, also after a set, until a clear.
    let mut value = vec![0xff; 10];
    value.extend_from_slice(&0i32.to_le_bytes());
    tx.set_versionstamped_value(b"v", &value);
    assert!(ready(&mut by_value).await.unwrap().is_ok());
    assert_eq!(
        tx.watch(b"v").await.unwrap_err().code(),
        codes::ACCESSED_UNREADABLE
    );
    tx.set(b"v", b"plain");
    assert_eq!(
        tx.watch(b"v").await.unwrap_err().code(),
        codes::ACCESSED_UNREADABLE
    );
    tx.clear_range(b"v", b"w");
    let v_watch = tx.watch(b"v").await.unwrap();
    assert_eq!(env.db.outstanding_watches(), 5);

    tx.commit().await.unwrap();
    until("the watch requests", || env.request_count() == 5).await;
    let mut keys: Vec<_> = env.requests().into_iter().map(|(r, _, _)| r.key).collect();
    keys.sort();
    let mut expected = vec![b"k".to_vec(), b"v".to_vec(), before, end, inside];
    expected.sort();
    assert_eq!(keys, expected);
    drop((before_watch, end_watch, inside_watch, v_watch));

    // Without a read version, the range starts at the zero stamp.
    let tx = env.db.create_transaction();
    tx.set_versionstamped_key(&key, b"x");
    let mut zero = b"k".to_vec();
    zero.extend_from_slice(&[0; 10]);
    assert_eq!(
        tx.watch(&zero).await.unwrap_err().code(),
        codes::ACCESSED_UNREADABLE
    );
}

#[tokio::test(start_paused = true)]
async fn a_write_that_fails_its_checks_triggers_no_watch() {
    let env = standard(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let mut watch = tx.watch(b"k").await.unwrap();
    tx.set(b"k", &vec![b'v'; 100_001]); // value_too_large
    tx.atomic_op(&[b'k'; 10_001], b"x", MutationType::Add); // key_too_large
    let mut stamp = b"k".to_vec();
    stamp.extend_from_slice(&5i32.to_le_bytes());
    tx.set_versionstamped_value(b"k", &stamp); // client_invalid_operation
    tx.clear_range(b"k", b"a"); // inverted_range
    assert!(ready(&mut watch).await.is_none());
    // The first error is the transaction's.
    assert_eq!(
        tx.commit().await.unwrap_err().code(),
        codes::VALUE_TOO_LARGE
    );
    assert!(ready(&mut watch).await.is_none());
}

#[tokio::test(start_paused = true)]
async fn a_commit_started_while_a_watch_reads_waits_for_it() {
    // (9q8.3 review) The watch's read is in progress (polled once) when the commit starts:
    // as C++'s `reading`, the commit waits for it, and the watch is registered and started.
    for schema in SCHEMAS {
        let env = standard(schema).await;
        let tx = env.db.create_transaction();
        tx.set(b"w", b"1");
        let watch = tx.watch(b"k");
        let mut watch = std::pin::pin!(watch);
        let reading =
            std::future::poll_fn(|cx| std::task::Poll::Ready(watch.as_mut().poll(cx).is_pending()))
                .await;
        assert!(reading);
        let commit = within("the commit", tx.commit()).await;
        assert!(commit.is_ok(), "{schema}: {commit:?}");
        assert_eq!(
            env.data.lock().unwrap().get(&b"w"[..]),
            Some(&b"1".to_vec())
        );
        let mut watch = within("the watch", watch).await.unwrap();
        until("the watch request", || env.request_count() == 1).await;
        assert!(pending(&mut watch).await);
        env.set_value(b"k", Some(b"v"));
        resolved("the watch", &mut watch).await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn a_watch_future_no_longer_polled_does_not_hold_the_commit() {
    let env = standard(SchemaVersion::V7_4).await;
    let tx = env.db.create_transaction();
    tx.set(b"w", b"1");
    let watch = tx.watch(b"k");
    let mut watch = std::pin::pin!(watch);
    let pending =
        std::future::poll_fn(|cx| std::task::Poll::Ready(watch.as_mut().poll(cx).is_pending()))
            .await;
    assert!(pending);
    // The watch future is kept but not polled: its read ends on its own.
    within("the commit", tx.commit()).await.unwrap();
    let mut watch = within("the watch", watch).await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    env.set_value(b"k", Some(b"v"));
    resolved("the watch", &mut watch).await.unwrap();

    // The kept watch's read added its range.
    let commit = env.commits.lock().unwrap().last().unwrap().clone();
    assert!(
        commit
            .read_conflict_ranges
            .contains(&KeyRange::single_key(b"k")),
        "{:?}",
        commit.read_conflict_ranges
    );

    // Dropped before its read ended: the read is cancelled, as C++'s `watch` actor is with
    // its future: the commit goes on, without the watch's read conflict range; nothing is
    // left counted or started.
    let tx = env.db.create_transaction();
    tx.set(b"w", b"2");
    {
        let watch = tx.watch(b"j");
        let mut watch = std::pin::pin!(watch);
        let _ = std::future::poll_fn(|cx| std::task::Poll::Ready(watch.as_mut().poll(cx))).await;
    }
    within("the commit", tx.commit()).await.unwrap();
    let commit = env.commits.lock().unwrap().last().unwrap().clone();
    assert!(
        !commit
            .read_conflict_ranges
            .iter()
            .any(|r| r.begin.as_slice() <= &b"j"[..] && &b"j"[..] < r.end.as_slice()),
        "{:?}",
        commit.read_conflict_ranges
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(env.db.outstanding_watches(), 0);
    assert_eq!(env.request_count(), 1);
}

/// A set during the read of a watch whose key has a pending atomic operation (a dependent
/// read) is compared with the value read with the writes as of the `watch` call: C++'s
/// `it->skip(k)` after the read (RYW 7.4:128, 8.0:137) looks the key up again in the
/// iterator's snapshot of the write map (`WriteMap.h` 7.4:158-160, 8.0:186-188). The folded
/// value does not fire the watch; another value fires it when the read ends.
#[tokio::test(start_paused = true)]
async fn a_set_during_a_dependent_watch_read_is_compared_with_the_value_read() {
    for (set, fires) in [(6u64, false), (9u64, true)] {
        let env = standard(SchemaVersion::V8_0).await;
        env.set_value(b"n", Some(&5u64.to_le_bytes()));
        let tx = env.db.create_transaction();
        tx.atomic_op(b"n", &1u64.to_le_bytes(), MutationType::Add);
        let watch = tx.watch(b"n");
        let mut watch = std::pin::pin!(watch);
        let reading =
            std::future::poll_fn(|cx| std::task::Poll::Ready(watch.as_mut().poll(cx).is_pending()))
                .await;
        assert!(reading);
        tx.set(b"n", &set.to_le_bytes());
        let mut watch = within("the watch", watch).await.unwrap();
        if fires {
            resolved("the watch", &mut watch).await.unwrap();
            continue;
        }
        assert!(pending(&mut watch).await);
        tx.commit().await.unwrap();
        until("the watch request", || env.request_count() == 1).await;
        assert_eq!(env.requests()[0].0.value, Some(6u64.to_le_bytes().to_vec()));
        assert!(pending(&mut watch).await);
    }
}

/// `ReadYourWritesTransaction::watch` calls `checkUsedDuringCommit` (7.4:2441-2446,
/// 8.0:2357-2362), as every operation does: a watch after the commit started fails, and
/// so does the commit, unless `USED_DURING_COMMIT_PROTECTION_DISABLE` is set.
#[tokio::test(start_paused = true)]
async fn a_watch_after_the_commit_started_fails_the_commit() {
    let env = standard(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    tx.set(b"w", b"1");
    let commit = tx.commit();
    let mut commit = std::pin::pin!(commit);
    let started =
        std::future::poll_fn(|cx| std::task::Poll::Ready(commit.as_mut().poll(cx).is_pending()))
            .await;
    assert!(started);
    let e = tx.watch(b"k").await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    assert!(!e.is_maybe_committed(), "the request was not sent yet");
    let e = within("the commit", commit).await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    assert!(!e.is_maybe_committed(), "the request was not sent yet");
    assert_eq!(env.db.outstanding_watches(), 0);

    // With the option only the watch fails; the commit completes.
    let tx = env.db.create_transaction();
    tx.set_option(fdb_native_client::TransactionOption::UsedDuringCommitProtectionDisable)
        .unwrap();
    tx.set(b"w", b"1");
    let commit = tx.commit();
    let mut commit = std::pin::pin!(commit);
    let started =
        std::future::poll_fn(|cx| std::task::Poll::Ready(commit.as_mut().poll(cx).is_pending()))
            .await;
    assert!(started);
    let e = tx.watch(b"k").await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    within("the commit", commit).await.unwrap();
    // After the commit: maybe committed (it did commit).
    let e = tx.watch(b"k").await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    assert!(e.is_maybe_committed());
    assert_eq!(env.db.outstanding_watches(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_fired_watch_frees_its_slot_without_being_polled() {
    let env = standard(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let _watch = tx.watch(b"k").await.unwrap();
    tx.commit().await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    assert_eq!(env.db.outstanding_watches(), 1);
    env.set_value(b"k", Some(b"v"));
    until("the slot to free", || env.db.outstanding_watches() == 0).await;

    // So does a watch fired by the transaction's own write.
    let tx = env.db.create_transaction();
    let _watch = tx.watch(b"k").await.unwrap();
    assert_eq!(env.db.outstanding_watches(), 1);
    tx.set(b"k", b"other");
    assert_eq!(env.db.outstanding_watches(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_reset_during_a_watch_read_cancels_it_and_not_a_later_watch() {
    let env = standard(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let first = tx.watch(b"k");
    let mut first = std::pin::pin!(first);
    let reading =
        std::future::poll_fn(|cx| std::task::Poll::Ready(first.as_mut().poll(cx).is_pending()))
            .await;
    assert!(reading);
    tx.reset();
    // A watch of the same key after the reset (the watch ids of the new state must not
    // meet the old one's).
    let mut second = tx.watch(b"k").await.unwrap();
    assert_eq!(
        within("the first watch", first).await.unwrap_err().code(),
        codes::TRANSACTION_CANCELLED
    );
    assert_eq!(env.db.outstanding_watches(), 1);
    tx.commit().await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    assert!(pending(&mut second).await);
    env.set_value(b"k", Some(b"v"));
    resolved("the second watch", &mut second).await.unwrap();
}

/// `ReadYourWritesTransaction::watch` with read-your-writes disabled: `watches_disabled`
/// (7.4:2447-2448, 8.0:2364-2365).
#[tokio::test(start_paused = true)]
async fn watches_are_disabled_without_read_your_writes() {
    let env = standard(SchemaVersion::V7_4).await;
    let tx = env.db.create_transaction();
    tx.set_option(fdb_native_client::TransactionOption::ReadYourWritesDisable)
        .unwrap();
    assert_eq!(
        tx.watch(b"k").await.unwrap_err().code(),
        codes::WATCHES_DISABLED
    );
    assert_eq!(env.db.outstanding_watches(), 0);
    assert!(env.reads.lock().unwrap().is_empty(), "nothing read");
    // The transaction goes on.
    tx.set(b"k", b"v");
    tx.commit().await.unwrap();
}

/// The watch's value is a read-your-writes `get` (`readWithConflictRange(GetValueReq)`):
/// what the transaction read already comes from its cache, with the read conflict range
/// once.
#[tokio::test(start_paused = true)]
async fn a_watch_reads_through_the_transactions_cache() {
    for schema in SCHEMAS {
        let env = standard(schema).await;
        env.set_value(b"k", Some(b"db"));
        env.set_value(b"n", Some(&5u64.to_le_bytes()));
        let tx = env.db.create_transaction();
        assert_eq!(tx.get(b"k").await.unwrap(), Some(b"db".to_vec()));
        assert_eq!(env.reads.lock().unwrap().len(), 1);
        let mut watch = tx.watch(b"k").await.unwrap();
        assert_eq!(env.reads.lock().unwrap().len(), 1, "the cache answers");
        // A dependent write over a cached value: no read either.
        assert!(tx.get(b"n").await.unwrap().is_some());
        tx.atomic_op(b"n", &1u64.to_le_bytes(), MutationType::Add);
        let mut by_n = tx.watch(b"n").await.unwrap();
        assert_eq!(env.reads.lock().unwrap().len(), 2);
        tx.set(b"other", b"x");
        let result = tx.commit().await.unwrap();
        let commit = env.commits.lock().unwrap().last().unwrap().clone();
        let mut reads = commit.read_conflict_ranges.clone();
        reads.retain(|r| !r.begin.starts_with(b"\xff/SC/"));
        reads.sort_by(|a, b| a.begin.cmp(&b.begin));
        assert_eq!(
            reads,
            vec![KeyRange::single_key(b"k"), KeyRange::single_key(b"n")]
        );
        until("the watch requests", || env.request_count() == 2).await;
        let mut requests: Vec<_> = env.requests().into_iter().map(|(r, _, _)| r).collect();
        requests.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(requests[0].value.as_deref(), Some(&b"db"[..]));
        assert_eq!(requests[1].value, Some(6u64.to_le_bytes().to_vec()));
        assert!(
            requests
                .iter()
                .all(|r| r.version == result.committed_version())
        );
        env.set_value(b"k", Some(b"theirs"));
        resolved("the watch of k", &mut watch).await.unwrap();
        assert!(pending(&mut by_n).await);
        env.set_value(b"n", None);
        resolved("the watch of n", &mut by_n).await.unwrap();
    }
}

/// Dropping the last handle of a transaction cancels its watches and frees their slots
/// (`outstanding_watches`): one registered but waiting for the commit, and one whose read is
/// still in progress. A watch the commit started survives the drop and frees its slot when it
/// fires.
#[tokio::test(start_paused = true)]
async fn dropping_the_handles_cancels_the_watches_and_frees_their_slots() {
    for schema in SCHEMAS {
        let env = standard(schema).await;
        let tx = env.db.create_transaction();
        let mut registered = tx.watch(b"k").await.unwrap();
        let reading = tx.watch(b"j");
        assert_eq!(
            env.db.outstanding_watches(),
            1,
            "{schema}: the registered one"
        );
        drop(tx);
        assert_eq!(
            code(resolved("registered", &mut registered).await),
            codes::TRANSACTION_CANCELLED,
            "{schema}"
        );
        assert_eq!(
            within("reading", reading).await.unwrap_err().code(),
            codes::TRANSACTION_CANCELLED,
            "{schema}"
        );
        assert_eq!(
            env.db.outstanding_watches(),
            0,
            "{schema}: the slots are free"
        );

        // A watch registered, then committed, survives the drop of the transaction.
        let tx = env.db.create_transaction();
        let mut watch = tx.watch(b"k").await.unwrap();
        tx.commit().await.unwrap();
        drop(tx);
        until("the watch request", || env.request_count() == 1).await;
        assert!(pending(&mut watch).await, "{schema}");
        assert_eq!(env.db.outstanding_watches(), 1, "{schema}");
        env.set_value(b"k", Some(b"v"));
        resolved("fires", &mut watch).await.unwrap();
        assert_eq!(env.db.outstanding_watches(), 0, "{schema}");
    }
}
