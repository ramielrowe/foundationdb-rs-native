//! The client on the smol runtime (`SmolRuntime`, feature `smol`) against a scripted fake
//! cluster: a coordinator, a GRV proxy, a commit proxy that also serves key locations and
//! applies commits to a map, and a storage server that serves `GetValue`, `GetKeyValues` and
//! `WatchValue`, on the in-memory network of `support`.
//!
//! Everything under test runs on smol: the cluster monitor, the peer connections, the read
//! and commit paths and the watches are tasks of a `SmolRuntime`, and their timers are
//! async-io's, in real time. Only the fake servers and the test body are on tokio (a
//! multi-threaded runtime, real time too), which the client under test shares nothing with
//! but streams and channels. So this proves the client's runtime-agnosticism: awaiting its
//! futures from a foreign executor, spawning, aborting and timing through the adapter.
//! The same paths against a real server are in `docker_smol.rs`.

#![cfg(feature = "smol")]

mod support;

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_client::cluster::ConnectionRecord;
use fdb_native_client::error::codes;
use fdb_native_client::runtime::SmolRuntime;
use fdb_native_client::{
    Database, DatabaseOptions, KeySelector, RangeOptions, StreamingMode, TransactionOption,
};
use fdb_native_schema::domain::{
    Commit, CommitId, GetKeyServerLocations, GetKeyValues, GetKeyValuesReply, GetReadVersion,
    GetValue, GetValueReply, KeyRange, KeyServerLocations, Mutation, MutationType as Mt,
    ReadVersion, StorageServer, WatchValue, WatchValueReply,
};
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::peer::PeerManagerConfig;
use fdb_native_wire::{FdbError, KeyValueRef, KeyValueVector, ProtocolVersion, Uid};
use support::process::{Action, FakeProcess, Incoming, Responder};
use support::{FakeCoordinator, MemRuntime, Net, info, sock};

const SCHEMAS: [SchemaVersion; 2] = [SchemaVersion::V7_4, SchemaVersion::V8_0];
const LIMIT: Duration = Duration::from_secs(60);
/// `sizeof(KeyValueRef)` on a 64-bit server: what `readRange` charges a row besides its key
/// and value.
const ROW_OVERHEAD: usize = 32;

type Rt = MemRuntime<SmolRuntime>;
/// Key-value rows a storage server returns.
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

struct PendingWatch {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    reply: Uid,
    responder: Responder,
    schema: SchemaVersion,
}

/// What the fake cluster knows.
#[derive(Default)]
struct World {
    data: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    version: AtomicUsize,
    pending: Mutex<Vec<PendingWatch>>,
    commits: AtomicUsize,
    /// Errors the commit proxy answers the next commits with.
    commit_errors: Mutex<Vec<u16>>,
}

impl World {
    fn version(&self) -> i64 {
        self.version.load(Ordering::SeqCst) as i64
    }

    /// Changes `key` like a commit would, and answers the watches it fires.
    fn set_value(&self, key: &[u8], value: &[u8]) {
        self.data
            .lock()
            .unwrap()
            .insert(key.to_vec(), value.to_vec());
        let version = self.version.fetch_add(10, Ordering::SeqCst) as i64 + 10;
        self.fire(version);
    }

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
}

/// What a selector resolves to, by definition, among the keys: `""` before the first key,
/// `\xff\xff` after the last.
fn resolve(keys: &[Vec<u8>], sel: &KeySelector) -> Vec<u8> {
    let below = keys
        .iter()
        .filter(|k| {
            if sel.or_equal {
                **k <= sel.key
            } else {
                **k < sel.key
            }
        })
        .count() as i64;
    let target = below - 1 + sel.offset as i64;
    if target < 0 {
        Vec::new()
    } else if target as usize >= keys.len() {
        b"\xff\xff".to_vec()
    } else {
        keys[target as usize].clone()
    }
}

/// `getKeyValuesQ` for one shard that holds every key: the rows between the resolved
/// selectors, the row limit (negative: reverse), and the byte limit at row granularity.
fn get_key_values(data: &BTreeMap<Vec<u8>, Vec<u8>>, req: &GetKeyValues) -> (Rows, bool) {
    let keys: Vec<Vec<u8>> = data.keys().cloned().collect();
    let (begin, end) = (resolve(&keys, &req.begin), resolve(&keys, &req.end));
    if begin >= end {
        return (Vec::new(), false);
    }
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = data
        .range(begin..end)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if req.limit < 0 {
        rows.reverse();
    }
    let mut limit = req.limit.unsigned_abs() as usize;
    let mut bytes = req.limit_bytes as i64;
    let mut out = Vec::new();
    for row in rows {
        if limit == 0 || bytes <= 0 {
            break;
        }
        bytes -= (ROW_OVERHEAD + row.0.len() + row.1.len()) as i64;
        limit -= 1;
        out.push(row);
    }
    (out, limit == 0 || bytes <= 0)
}

struct Env {
    world: Arc<World>,
    db: Database<Rt>,
    coord: FakeCoordinator,
    schema: SchemaVersion,
    _processes: Vec<FakeProcess>,
}

async fn env(schema: SchemaVersion) -> Env {
    let net = Net::new();
    let rt = MemRuntime::with_runtime(&net, SmolRuntime::new());
    let version = schema.protocol_version();
    let mut coord = FakeCoordinator::start(&net, sock(1, 4500), version);
    let info = info(1, 1, 1, schema);
    let world = Arc::new(World::default());
    world.version.store(1000, Ordering::SeqCst);
    let mut processes = Vec::new();

    // The storage server.
    let storage = FakeProcess::start(&net, sock(30, 4500), version);
    let server = StorageServer {
        id: Uid::new(0x55, 0),
        locality: BTreeMap::new(),
        base: storage.endpoint(Uid::new(0x5500, 0x30)),
        tss_pair_id: None,
        accepting_requests: true,
    };
    let streams = server.endpoints();
    {
        let world = world.clone();
        storage.handle(streams.get_value.token, move |incoming: Incoming| {
            let (request, reply) =
                GetValue::decode_request(incoming.schema, &incoming.payload).unwrap();
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
        storage.handle(streams.get_key_values.token, move |incoming: Incoming| {
            let (request, reply) =
                GetKeyValues::decode_request(incoming.schema, &incoming.payload).unwrap();
            let (rows, more) = get_key_values(&world.data.lock().unwrap(), &request);
            let vector =
                KeyValueVector::from_pairs(rows.iter().map(|(k, v)| KeyValueRef::new(k, v)))
                    .unwrap();
            let answer = GetKeyValuesReply {
                data: vector.borrowed(),
                more,
                version: request.version,
                ..GetKeyValuesReply::default()
            };
            let payload = GetKeyValues::encode_reply(incoming.schema, &Ok(answer)).unwrap();
            Action::reply(reply, payload)
        });
    }
    {
        let world = world.clone();
        storage.handle(streams.watch_value.token, move |incoming: Incoming| {
            let (request, reply) =
                WatchValue::decode_request(incoming.schema, &incoming.payload).unwrap();
            let differs = world.data.lock().unwrap().get(&request.key) != request.value.as_ref();
            if differs {
                let payload = WatchValue::encode_reply(
                    incoming.schema,
                    &Ok(WatchValueReply {
                        version: world.version(),
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
    processes.push(storage);

    // The GRV proxy.
    let grv = FakeProcess::start(&net, sock(100, 4500), version);
    {
        let world = world.clone();
        grv.handle(info.grv_proxies[0].base.token, move |incoming: Incoming| {
            let (_, reply) =
                GetReadVersion::decode_request(incoming.schema, &incoming.payload).unwrap();
            let rv = ReadVersion {
                process_busy_time: 0,
                version: world.version(),
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
            let scripted = {
                let mut errors = world.commit_errors.lock().unwrap();
                (!errors.is_empty()).then(|| errors.remove(0))
            };
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
                                other => panic!("unexpected mutation {other:?}"),
                            }
                        }
                    }
                    world.commits.fetch_add(1, Ordering::SeqCst);
                    let version = world.version.fetch_add(10, Ordering::SeqCst) as i64 + 10;
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
        let server = server.clone();
        proxy.handle(
            streams.get_key_servers_locations.token,
            move |incoming: Incoming| {
                let (_, reply) =
                    GetKeyServerLocations::decode_request(incoming.schema, &incoming.payload)
                        .unwrap();
                let locations = KeyServerLocations {
                    results: vec![(
                        KeyRange {
                            begin: Vec::new(),
                            end: b"\xff\xff".to_vec(),
                        },
                        vec![server.clone()],
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
    let record = ConnectionRecord::memory("test:abc@10.0.0.1:4500".parse().unwrap());
    let db = Database::from_record(rt, record, options).expect("database");
    coord.next_call().await.reply(&info);
    within("the proxies", db.cluster_monitor().proxies(false))
        .await
        .unwrap();
    Env {
        world,
        db,
        coord,
        schema,
        _processes: processes,
    }
}

async fn within<T>(what: &str, f: impl Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: no result within {LIMIT:?}"))
}

/// The cluster monitor learns the proxies from the coordinator over the handshake and the
/// pings of a smol connection, and follows a new info.
#[tokio::test(flavor = "multi_thread")]
async fn the_monitor_opens_the_database_and_follows_new_infos() {
    for schema in SCHEMAS {
        let mut env = env(schema).await;
        let first = env.db.cluster_monitor().proxies(false).await.unwrap();
        assert_eq!(first.grv_proxy_ids().len(), 1);
        assert_eq!(first.commit_proxy_ids().len(), 1);

        // The coordinator's long poll is answered when the info changes.
        let call = env.coord.next_call().await;
        assert_eq!(call.request.known_client_info_id, first.id());
        let next = info(2, 1, 2, env.schema);
        call.reply(&next);
        let got = within(
            "the new info",
            env.db.cluster_monitor().proxies_changed(&first),
        )
        .await
        .unwrap();
        assert_eq!(got.id(), next.id);
        assert_eq!(got.commit_proxy_ids().len(), 2);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn set_commit_get_clear_round_trip() {
    for schema in SCHEMAS {
        let env = env(schema).await;
        let tx = env.db.create_transaction();
        assert_eq!(within("get", tx.get(b"k")).await.unwrap(), None);
        tx.set(b"k", b"v");
        // Read-your-writes.
        assert_eq!(
            within("ryw get", tx.get(b"k")).await.unwrap(),
            Some(b"v".to_vec())
        );
        let result = within("commit", tx.commit()).await.unwrap();
        assert!(result.committed_version() > 1000);
        assert_eq!(env.world.commits.load(Ordering::SeqCst), 1);

        // Another transaction sees it through the storage server.
        let tx = env.db.create_transaction();
        assert_eq!(
            within("get", tx.get(b"k")).await.unwrap(),
            Some(b"v".to_vec())
        );
        tx.clear(b"k");
        within("commit", tx.commit()).await.unwrap();
        let tx = env.db.create_transaction();
        assert_eq!(within("get", tx.get(b"k")).await.unwrap(), None);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn range_reads_with_limits_and_reverse() {
    for schema in SCHEMAS {
        let env = env(schema).await;
        for i in 0..20u8 {
            env.world.set_value(&[b'r', i], &[i; 4]);
        }
        let begin = KeySelector::first_greater_or_equal(b"r");
        let end = KeySelector::first_greater_or_equal(b"s");
        let tx = env.db.create_transaction();

        let all = within(
            "all",
            tx.get_range(
                &begin,
                &end,
                &RangeOptions::default().mode(StreamingMode::WantAll),
            ),
        )
        .await
        .unwrap();
        assert_eq!(all.kvs.len(), 20);
        assert!(!all.more);
        assert_eq!(all.kvs[0].key, [b'r', 0]);
        assert_eq!(all.kvs[19].value, [19; 4]);

        let last3 = within(
            "last three",
            tx.get_range(
                &begin,
                &end,
                &RangeOptions::with_limit(3)
                    .reverse(true)
                    .mode(StreamingMode::WantAll),
            ),
        )
        .await
        .unwrap();
        let keys: Vec<_> = last3.kvs.iter().map(|kv| kv.key.clone()).collect();
        assert_eq!(keys, [[b'r', 19], [b'r', 18], [b'r', 17]]);
        assert!(last3.more);

        // A write of the transaction shows in the range.
        tx.set(&[b'r', 100], b"mine");
        let with_mine = within(
            "with a local write",
            tx.get_range(
                &begin,
                &end,
                &RangeOptions::default().mode(StreamingMode::WantAll),
            ),
        )
        .await
        .unwrap();
        assert_eq!(with_mine.kvs.len(), 21);
        assert_eq!(with_mine.kvs[20].value, b"mine");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_watch_fires_when_the_value_changes() {
    for schema in SCHEMAS {
        let env = env(schema).await;
        let tx = env.db.create_transaction();
        let mut watch = within("watch", tx.watch(b"w")).await.unwrap();
        within("commit", tx.commit()).await.unwrap();
        // Armed on the (fake) storage server, not fired.
        for _ in 0..100 {
            if !env.world.pending.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(env.world.pending.lock().unwrap().len(), 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut watch)
                .await
                .is_err()
        );

        // A commit of another transaction fires it.
        let other = env.db.create_transaction();
        other.set(b"w", b"changed");
        within("commit", other.commit()).await.unwrap();
        within("the watch", watch).await.unwrap();
        assert_eq!(env.db.outstanding_watches(), 0);
    }
}

/// `on_error`'s backoff is a smol timer: a conflict is retried by `run`, which commits at the
/// second attempt.
#[tokio::test(flavor = "multi_thread")]
async fn run_retries_a_conflict_after_the_backoff() {
    let env = env(SchemaVersion::V8_0).await;
    env.world
        .commit_errors
        .lock()
        .unwrap()
        .push(codes::NOT_COMMITTED);
    let attempts = Arc::new(AtomicUsize::new(0));
    let started = std::time::Instant::now();
    within(
        "run",
        env.db.run(|tx, _| {
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                tx.set(b"retried", b"yes");
                Ok(())
            }
        }),
    )
    .await
    .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    // C++'s first backoff is 10 ms (with jitter, at least one smol timer's worth).
    assert!(started.elapsed() >= Duration::from_millis(1));
    assert_eq!(
        env.world.data.lock().unwrap().get(b"retried".as_slice()),
        Some(&b"yes".to_vec())
    );
}

/// A transaction timeout is an async-io timer, and cancels a read that never gets an answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_timeout_fires_on_a_smol_timer() {
    let env = env(SchemaVersion::V7_4).await;
    // The coordinator and proxies are up, but the storage server stops answering.
    for process in &env._processes {
        process.stop();
    }
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::Timeout(Duration::from_millis(300)))
        .unwrap();
    let started = std::time::Instant::now();
    let err = within("get", tx.get(b"k")).await.unwrap_err();
    assert_eq!(err.code(), codes::TRANSACTION_TIMED_OUT);
    assert!(started.elapsed() >= Duration::from_millis(250));
}

/// Shutting the database down aborts its tasks on the smol executor and returns.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_stops_the_tasks() {
    let env = env(SchemaVersion::V8_0).await;
    let tx = env.db.create_transaction();
    tx.set(b"a", b"b");
    within("commit", tx.commit()).await.unwrap();
    within("shutdown", env.db.shutdown()).await;
    let tx = env.db.create_transaction();
    assert!(within("get after shutdown", tx.get(b"a")).await.is_err());
}
