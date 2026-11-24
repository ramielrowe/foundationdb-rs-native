//! The transaction read options (`READ_PRIORITY_*`, `READ_SERVER_SIDE_CACHE_*`, and the
//! `lockAware` of `LOCK_AWARE` / `READ_LOCK_AWARE`) against a scripted fake cluster (a
//! coordinator, a GRV proxy, a commit proxy and a storage server on the in-memory network,
//! under tokio's paused clock): which `ReadOptions` every `GetValueRequest`, `GetKeyRequest`
//! and `GetKeyValuesRequest` carries, as C++'s `trState->readOptions` (`NativeAPI`
//! 7.4:7420-7440 and 7561-7579): none until one of the options is set, then `ReadOptions()`
//! with each option's change, forgotten by a reset. The docker tests read with each option
//! on real clusters.

#![cfg(feature = "tokio")]

mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_client::cluster::ConnectionRecord;
use fdb_native_client::read::{RangeOptions, ReadOptions};
use fdb_native_client::{Database, DatabaseOptions, KeySelector, Transaction, TransactionOption};
use fdb_native_schema::domain::{
    ClientDbInfo, CommitProxy, GetKey, GetKeyReply, GetKeyServerLocations, GetKeyValues,
    GetKeyValuesReply, GetReadVersion, GetValue, GetValueReply, GrvProxy, KeyRange,
    KeyServerLocations, ReadVersion, StorageServer, TenantMode,
};
use fdb_native_schema::streams::STORAGE_SERVER_STREAMS;
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::peer::PeerManagerConfig;
use fdb_native_wire::{KeyValueRef, KeyValueVector, ProtocolVersion, Uid, VersionVector};
use support::process::{Action, FakeProcess, Incoming};
use support::{FakeCoordinator, MemRuntime, Net, sock};

const LIMIT: Duration = Duration::from_secs(120);
const READ_VERSION: i64 = 1000;

async fn within<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: no result within {LIMIT:?}"))
}

/// A storage read the fake server received: its kind and its `options`.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Seen {
    Value(Option<ReadOptions>),
    Key(Option<ReadOptions>),
    Range(Option<ReadOptions>),
}

impl Seen {
    fn options(&self) -> Option<ReadOptions> {
        match self {
            Seen::Value(o) | Seen::Key(o) | Seen::Range(o) => *o,
        }
    }
}

type Data = Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>;

struct Env {
    db: Database<MemRuntime>,
    seen: Arc<Mutex<Vec<Seen>>>,
    _processes: Vec<FakeProcess>,
}

impl Env {
    /// A cluster whose one storage server holds every key; the keys `a` to `e` are set.
    async fn new(schema: SchemaVersion) -> Env {
        let net = Net::new();
        let rt = MemRuntime::new(&net);
        let version: ProtocolVersion = schema.protocol_version();
        let mut coordinator = FakeCoordinator::start(&net, sock(1, 4500), version);
        let data: Data = Arc::new(Mutex::new(
            [b"a", b"b", b"c", b"d", b"e"]
                .iter()
                .map(|k| (k.to_vec(), b"v".to_vec()))
                .collect(),
        ));
        let seen = Arc::new(Mutex::new(Vec::new()));

        let storage = FakeProcess::start(&net, sock(30, 4500), version);
        let server = StorageServer {
            id: Uid::new(0x55, 0),
            locality: BTreeMap::new(),
            base: storage.endpoint(Uid::new(0x5500, 0x30)),
            tss_pair_id: None,
            accepting_requests: true,
        };
        {
            let (seen, data) = (seen.clone(), data.clone());
            storage.handle(server.base.token, move |incoming: Incoming| {
                let (request, reply) =
                    GetValue::decode_request(incoming.schema, &incoming.payload).unwrap();
                seen.lock().unwrap().push(Seen::Value(request.options));
                let data = data.lock().unwrap();
                let answer = GetValueReply {
                    value: data.get(&request.key).map(Vec::as_slice),
                    ..GetValueReply::default()
                };
                let payload = GetValue::encode_reply(incoming.schema, &Ok(answer)).unwrap();
                Action::reply(reply, payload)
            });
        }
        {
            let (seen, data) = (seen.clone(), data.clone());
            let token = server.base.adjusted(STORAGE_SERVER_STREAMS.get_key).token;
            storage.handle(token, move |incoming: Incoming| {
                let (request, reply) =
                    GetKey::decode_request(incoming.schema, &incoming.payload).unwrap();
                seen.lock().unwrap().push(Seen::Key(request.options));
                // `findKey` over one shard holding everything.
                let data = data.lock().unwrap();
                let keys: Vec<&Vec<u8>> = data.keys().collect();
                let sel = &request.sel;
                let below = keys
                    .iter()
                    .filter(|k| {
                        if sel.or_equal {
                            k.as_slice() <= sel.key.as_slice()
                        } else {
                            k.as_slice() < sel.key.as_slice()
                        }
                    })
                    .count() as i64;
                let index = below + i64::from(sel.offset) - 1;
                let key = if index < 0 {
                    Vec::new()
                } else {
                    keys.get(index as usize)
                        .map_or(b"\xff\xff".to_vec(), |k| k.to_vec())
                };
                let sel = KeySelector::new(key, true, 0);
                let answer = GetKeyReply {
                    sel: sel.borrowed(),
                    ..GetKeyReply::default()
                };
                let payload = GetKey::encode_reply(incoming.schema, &Ok(answer)).unwrap();
                Action::reply(reply, payload)
            });
        }
        {
            let (seen, data) = (seen.clone(), data.clone());
            let token = server
                .base
                .adjusted(STORAGE_SERVER_STREAMS.get_key_values)
                .token;
            storage.handle(token, move |incoming: Incoming| {
                let (request, reply) =
                    GetKeyValues::decode_request(incoming.schema, &incoming.payload).unwrap();
                seen.lock().unwrap().push(Seen::Range(request.options));
                // The selectors the reads below send are `firstGreaterOrEqual`.
                assert!(!request.begin.or_equal && request.begin.offset == 1);
                assert!(!request.end.or_equal && request.end.offset == 1);
                let data = data.lock().unwrap();
                let mut rows: Vec<(Vec<u8>, Vec<u8>)> = data
                    .range(request.begin.key.clone()..request.end.key.clone())
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                if request.limit < 0 {
                    rows.reverse();
                }
                rows.truncate(request.limit.unsigned_abs() as usize);
                let kvs =
                    KeyValueVector::from_pairs(rows.iter().map(|(k, v)| KeyValueRef::new(k, v)))
                        .unwrap();
                let answer = GetKeyValuesReply {
                    data: kvs.borrowed(),
                    more: false,
                    version: READ_VERSION,
                    ..GetKeyValuesReply::default()
                };
                let payload = GetKeyValues::encode_reply(incoming.schema, &Ok(answer)).unwrap();
                Action::reply(reply, payload)
            });
        }

        let grv_process = FakeProcess::start(&net, sock(10, 4500), version);
        let grv = GrvProxy {
            process_id: Some(b"grv".to_vec()),
            provisional: false,
            base: grv_process.endpoint(Uid::new(0x6001, 0x10)),
        };
        grv_process.handle(grv.base.token, |incoming: Incoming| {
            let (_, reply) =
                GetReadVersion::decode_request(incoming.schema, &incoming.payload).unwrap();
            let answer = ReadVersion {
                process_busy_time: 0,
                version: READ_VERSION,
                locked: false,
                metadata_version: None,
                tag_throttle_info: BTreeMap::new(),
                mid_shard_size: 0,
                rk_default_throttled: false,
                rk_batch_throttled: false,
                ss_version_vector_delta: VersionVector::default(),
                proxy_id: Uid::new(9, 9),
                proxy_tag_throttled_duration: 0.0,
            };
            let payload = GetReadVersion::encode_reply(incoming.schema, &Ok(answer)).unwrap();
            Action::reply(reply, payload)
        });

        let commit_process = FakeProcess::start(&net, sock(20, 4500), version);
        let commit = CommitProxy {
            process_id: Some(b"cp".to_vec()),
            provisional: false,
            base: commit_process.endpoint(Uid::new(0x7001, 0x20)),
            schema,
        };
        {
            let token = commit.endpoints().get_key_servers_locations.token;
            commit_process.handle(token, move |incoming: Incoming| {
                let (_, reply) =
                    GetKeyServerLocations::decode_request(incoming.schema, &incoming.payload)
                        .unwrap();
                let everything = KeyRange {
                    begin: Vec::new(),
                    end: b"\xff\xff".to_vec(),
                };
                let locations = KeyServerLocations {
                    results: vec![(everything, vec![server.clone()])],
                    tss_mapping: Vec::new(),
                    tag_mapping: Vec::new(),
                };
                let payload =
                    GetKeyServerLocations::encode_reply(incoming.schema, &Ok(locations)).unwrap();
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
            seen,
            _processes: vec![storage, grv_process, commit_process],
        }
    }

    fn take_seen(&self) -> Vec<Seen> {
        std::mem::take(&mut *self.seen.lock().unwrap())
    }
}

/// A `get`, a `get_key` and a `get_range` of `tx`, then what the storage server received
/// for them: with read-your-writes disabled (`plain`), exactly one request of each kind;
/// otherwise at least one request (the read-your-writes layer resolves selectors with range
/// reads, and caches what it read).
async fn read_all(env: &Env, tx: &Transaction<MemRuntime>, plain: bool) -> Vec<Seen> {
    env.take_seen();
    assert_eq!(
        within("get", tx.get(b"b")).await.unwrap(),
        Some(b"v".to_vec())
    );
    assert_eq!(
        within(
            "get_key",
            tx.get_key(&KeySelector::new(b"b".to_vec(), true, 2))
        )
        .await
        .unwrap(),
        b"d".to_vec()
    );
    let rows = within(
        "get_range",
        tx.get_range(
            &KeySelector::first_greater_or_equal(b"a".to_vec()),
            &KeySelector::first_greater_or_equal(b"d".to_vec()),
            &RangeOptions::default(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(rows.kvs.len(), 3);
    let seen = env.take_seen();
    if plain {
        assert!(
            matches!(
                seen.as_slice(),
                [Seen::Value(_), Seen::Key(_), Seen::Range(_)]
            ),
            "{seen:?}"
        );
    } else {
        assert!(!seen.is_empty());
    }
    seen
}

/// Every storage read of `seen` carried `want`.
#[track_caller]
fn assert_all(seen: &[Seen], want: Option<ReadOptions>) {
    for s in seen {
        assert_eq!(s.options(), want, "{seen:?}");
    }
}

fn read_options(read_type: u32, cache_result: bool, lock_aware: bool) -> Option<ReadOptions> {
    Some(ReadOptions {
        read_type,
        cache_result,
        lock_aware,
        ..ReadOptions::default()
    })
}

/// A transaction with read-your-writes disabled, so that each read is one storage request.
fn plain_tx(env: &Env) -> Transaction<MemRuntime> {
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::ReadYourWritesDisable)
        .unwrap();
    tx
}

#[tokio::test(start_paused = true)]
async fn without_read_options_the_requests_carry_none() {
    for schema in SchemaVersion::ALL {
        let env = Env::new(schema).await;
        let tx = plain_tx(&env);
        assert_all(&read_all(&env, &tx, true).await, None);
        // Read-your-writes reads too, and options that are not read options change nothing.
        let tx = env.db.create_transaction();
        tx.set_option(TransactionOption::CausalReadRisky).unwrap();
        tx.set_option(TransactionOption::PriorityBatch).unwrap();
        assert_all(&read_all(&env, &tx, false).await, None);
    }
}

#[tokio::test(start_paused = true)]
async fn read_priority_and_cache_options_reach_every_storage_read() {
    use TransactionOption::*;
    let cases: Vec<(Vec<TransactionOption>, Option<ReadOptions>)> = vec![
        // A background scan.
        (
            vec![ReadPriorityLow, ReadServerSideCacheDisable],
            read_options(ReadOptions::LOW, false, false),
        ),
        // Setting a default still sends `ReadOptions()` (present), as C++ does.
        (
            vec![ReadPriorityNormal],
            read_options(ReadOptions::NORMAL, true, false),
        ),
        (
            vec![ReadServerSideCacheEnable],
            read_options(ReadOptions::NORMAL, true, false),
        ),
        (
            vec![ReadPriorityHigh],
            read_options(ReadOptions::HIGH, true, false),
        ),
        // The last one set wins.
        (
            vec![
                ReadPriorityLow,
                ReadPriorityHigh,
                ReadServerSideCacheDisable,
                ReadServerSideCacheEnable,
            ],
            read_options(ReadOptions::HIGH, true, false),
        ),
        // `LOCK_AWARE` and `READ_LOCK_AWARE` set `lockAware` (NativeAPI 7.4:7420-7438).
        (
            vec![LockAware],
            read_options(ReadOptions::NORMAL, true, true),
        ),
        (
            vec![ReadLockAware, ReadPriorityLow],
            read_options(ReadOptions::LOW, true, true),
        ),
    ];
    for schema in SchemaVersion::ALL {
        let env = Env::new(schema).await;
        for (options, want) in &cases {
            for plain in [true, false] {
                let tx = if plain {
                    plain_tx(&env)
                } else {
                    env.db.create_transaction()
                };
                for o in options {
                    tx.set_option(o.clone()).unwrap();
                }
                assert_all(&read_all(&env, &tx, plain).await, *want);
                // Snapshot reads carry them too.
                within("snapshot get", tx.snapshot().get(b"e"))
                    .await
                    .unwrap();
                let seen = env.take_seen();
                assert_eq!(seen.len(), 1, "{seen:?}");
                assert_all(&seen, *want);
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_reset_or_on_error_forgets_the_read_options() {
    for schema in SchemaVersion::ALL {
        let env = Env::new(schema).await;
        let tx = plain_tx(&env);
        tx.set_option(TransactionOption::ReadPriorityLow).unwrap();
        assert_all(
            &read_all(&env, &tx, true).await,
            read_options(ReadOptions::LOW, true, false),
        );
        // (`READ_YOUR_WRITES_DISABLE` is forgotten too.)
        tx.reset();
        assert_all(&read_all(&env, &tx, false).await, None);

        let tx = plain_tx(&env);
        tx.set_option(TransactionOption::ReadServerSideCacheDisable)
            .unwrap();
        assert_all(
            &read_all(&env, &tx, true).await,
            read_options(ReadOptions::NORMAL, false, false),
        );
        let conflict =
            fdb_native_client::Error::new(fdb_native_client::error::codes::NOT_COMMITTED);
        within("on_error", tx.on_error(conflict)).await.unwrap();
        assert_all(&read_all(&env, &tx, false).await, None);
    }
}

#[tokio::test(start_paused = true)]
async fn the_options_of_a_read_are_those_set_when_it_was_called() {
    for schema in SchemaVersion::ALL {
        let env = Env::new(schema).await;
        let tx = plain_tx(&env);
        // Called before the option is set: sent without, although it may go out after.
        let before = tx.get(b"a");
        tx.set_option(TransactionOption::ReadPriorityHigh).unwrap();
        let after = tx.get(b"b");
        within("before", before).await.unwrap();
        within("after", after).await.unwrap();
        let mut seen = env.take_seen();
        seen.sort_by_key(|s| s.options().is_some());
        assert_eq!(
            seen,
            vec![
                Seen::Value(None),
                Seen::Value(read_options(ReadOptions::HIGH, true, false))
            ]
        );
    }
}
