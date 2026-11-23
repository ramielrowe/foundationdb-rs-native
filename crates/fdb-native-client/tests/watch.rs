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

use fdb_native_client::error::codes;
use fdb_native_client::read::KEY_SPACE_END;
use fdb_native_client::{
    ConnectionRecord, Database, DatabaseOptions, Error, MutationType, Watch, WatchConfig,
};
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
                                _ => {}
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
async fn a_pending_atomic_operation_makes_the_value_the_one_at_the_commit() {
    let env = standard(SchemaVersion::V7_4).await;
    env.set_value(b"n", Some(b"abc"));
    let tx = env.db.create_transaction();
    tx.atomic_op(b"n", &1u64.to_le_bytes(), MutationType::ADD_VALUE);
    let mut watch = tx.watch(b"n").await.unwrap();
    let result = tx.commit().await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    let (request, _, _) = env.requests().remove(0);
    // The key's value at the commit version was read, and that is what is watched.
    assert_eq!(request.value.as_deref(), Some(&b"abc"[..]));
    assert_eq!(request.version, result.committed_version());
    assert_eq!(
        env.reads.lock().unwrap().clone(),
        vec![result.committed_version()]
    );
    assert!(pending(&mut watch).await);
    env.set_value(b"n", None);
    resolved("the watch", &mut watch).await.unwrap();
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
    tx.atomic_op(b"d", &[1], MutationType::ADD_VALUE); // its result is not known
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

    // After the commit started, a watch is `used_during_commit`.
    let tx = env.db.create_transaction();
    tx.commit().await.unwrap();
    assert_eq!(
        tx.watch(b"k").await.unwrap_err().code(),
        codes::USED_DURING_COMMIT
    );
}

#[tokio::test(start_paused = true)]
async fn versionstamped_operations_trigger_watches_as_atomic_operations_do() {
    let env = standard(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    let mut by_value = tx.watch(b"v").await.unwrap();
    let mut by_key = tx.watch(b"k").await.unwrap();
    // The key a versionstamped key writes is not known before the commit: no watch is
    // triggered (C++ triggers on the key with the placeholder, which nobody watches).
    let mut key = b"k".to_vec();
    key.extend_from_slice(&[0xff; 10]);
    key.extend_from_slice(&1i32.to_le_bytes());
    tx.set_versionstamped_key(&key, b"x");
    assert!(ready(&mut by_key).await.is_none());
    // A versionstamped value's result is not known: the watch fires at once.
    let mut value = vec![0xff; 10];
    value.extend_from_slice(&0i32.to_le_bytes());
    tx.set_versionstamped_value(b"v", &value);
    assert!(ready(&mut by_value).await.unwrap().is_ok());
    tx.commit().await.unwrap();
    until("the watch request", || env.request_count() == 1).await;
    assert_eq!(env.requests()[0].0.key, b"k");
}
