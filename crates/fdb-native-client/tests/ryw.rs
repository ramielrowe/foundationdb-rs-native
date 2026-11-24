//! Read-your-writes reads against a scripted fake cluster (a coordinator, a GRV proxy, a
//! commit proxy and storage servers that answer `GetValue`, `GetKey` and `GetKeyValues` as
//! the storage server does, on the in-memory network, under tokio's paused clock): reads see
//! the transaction's sets, clears and atomic operations (the dependent ones read the
//! database first), selectors resolve through them, forward and reverse ranges merge
//! cached, uncached and written segments, the cache saves storage reads, versionstamped
//! keys are unreadable, the snapshot and read-your-writes options, a read's view is taken
//! when it is called; a randomized comparison with a model (the database with the writes
//! applied, `atomic` for the atomic operations) over random shard maps; and a corpus of the
//! read conflict ranges a commit sends, against what C++'s `RYWImpl::addConflictRange` and
//! `updateConflictMap` give (worked out from `ReadYourWrites.actor.cpp` 7.4:218-351).

#![cfg(feature = "tokio")]
// The fake cluster is shared with `get_range.rs`; not every part of it is used here.
#![allow(dead_code)]

mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::read::{KEY_SPACE_END, RangeLimits, RangeOptions, RangeRead, StreamingMode};
use fdb_native_client::{
    ConnectionRecord, Database, DatabaseOptions, KeySelector, KeyValue, Transaction,
    TransactionOption,
};
use fdb_native_schema::domain::{
    ClientDbInfo, Commit, CommitId, CommitProxy, GetKey, GetKeyReply, GetKeyServerLocations,
    GetKeyValues, GetKeyValuesReply, GetReadVersion, GetValue, GetValueReply, GrvProxy, KeyRange,
    KeyServerLocations, MutationType, ReadVersion, StorageServer, TenantMode,
};
use fdb_native_schema::streams::STORAGE_SERVER_STREAMS;
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::peer::PeerManagerConfig;
use fdb_native_wire::{FdbError, KeyValueRef, KeyValueVector, ProtocolVersion, Uid, VersionVector};
use support::process::{Action, FakeProcess, Incoming};
use support::{FakeCoordinator, MemRuntime, Net, sock};

const SCHEMAS: [SchemaVersion; 2] = [SchemaVersion::V7_4, SchemaVersion::V8_0];
const LIMIT: Duration = Duration::from_secs(120);
const READ_VERSION: i64 = 1000;
/// `sizeof(KeyValueRef)` on a 64-bit server: what `readRange` charges a row besides its key
/// and value.
const SERVER_ROW_OVERHEAD: usize = 32;

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

fn key_after(key: &[u8]) -> Vec<u8> {
    if key == KEY_SPACE_END {
        return key.to_vec();
    }
    let mut k = key.to_vec();
    k.push(0);
    k
}

type Data = Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>;
/// Rows a storage server returns.
type Rows = Vec<(Vec<u8>, Vec<u8>)>;
/// A transaction range read: its selectors and options, the keys it returns, and its read
/// conflict range.
type Case<'a> = (
    KeySelector,
    KeySelector,
    RangeOptions,
    Vec<&'a [u8]>,
    Option<KeyRange>,
);
/// Shards and the indices of their storage servers.
type ShardMap = Vec<(KeyRange, Vec<usize>)>;

// ---- what a storage server answers ---------------------------------------------------------

/// A `GetKeyValuesRequest` a fake storage server received.
#[derive(Clone, Debug)]
struct KvRequest {
    begin: KeySelector,
    end: KeySelector,
    limit: i32,
    limit_bytes: i32,
    task_id: Option<i32>,
}

/// What one fake storage server does.
#[derive(Default)]
struct StorageState {
    /// The readable ranges it holds, coalesced.
    owns: Vec<KeyRange>,
    /// The most rows a `findKey` scan returns for a selector whose offset is not 0 or 1
    /// (`STORAGE_LIMIT_BYTES` in rows); `None` is unlimited.
    row_limit: Option<usize>,
    /// The selectors of the `GetKeyRequest`s received.
    key_requests: Vec<KeySelector>,
    /// The `GetKeyValuesRequest`s received.
    kv_requests: Vec<KvRequest>,
    /// The keys of the `GetValueRequest`s received.
    value_requests: Vec<Vec<u8>>,
    /// How long replies take.
    delay: Duration,
    /// An error to reply to every request instead.
    error: Option<u16>,
}

fn is_backward(sel: &KeySelector) -> bool {
    !sel.or_equal && sel.offset <= 0
}

fn is_fge(sel: &KeySelector) -> bool {
    !sel.or_equal && sel.offset == 1
}

/// `getShardKeyRange`: the owned range the selector starts in; `None` is
/// `wrong_shard_server`.
fn shard_range(owns: &[KeyRange], sel: &KeySelector) -> Option<KeyRange> {
    let k = sel.key.as_slice();
    owns.iter()
        .find(|r| {
            if is_backward(sel) {
                r.begin.as_slice() < k && k <= r.end.as_slice()
            } else {
                r.begin.as_slice() <= k && k < r.end.as_slice()
            }
        })
        .cloned()
}

/// `selectorInRange`.
fn selector_in_range(sel: &KeySelector, r: &KeyRange) -> bool {
    sel.key >= r.begin
        && if is_backward(sel) {
            sel.key <= r.end
        } else {
            sel.key < r.end
        }
}

/// Up to `wanted` keys of `range` (descending if `reverse`), at most `limit`; and whether
/// the range has more keys than returned.
fn scan(
    data: &BTreeMap<Vec<u8>, Vec<u8>>,
    range: &KeyRange,
    reverse: bool,
    wanted: usize,
    limit: Option<usize>,
) -> (Vec<Vec<u8>>, bool) {
    if range.begin >= range.end {
        return (Vec::new(), false);
    }
    let mut keys: Vec<Vec<u8>> = data
        .range(range.begin.clone()..range.end.clone())
        .map(|(k, _)| k.clone())
        .collect();
    if reverse {
        keys.reverse();
    }
    let take = wanted.min(limit.unwrap_or(usize::MAX));
    let more = keys.len() > take;
    keys.truncate(take);
    (keys, more)
}

/// `findKey` (`storageserver.actor.cpp` 7.4:4526-4626): the key and the offset left (0 when
/// found).
fn find_key(
    data: &BTreeMap<Vec<u8>, Vec<u8>>,
    sel: &KeySelector,
    range: &KeyRange,
    row_limit: Option<usize>,
) -> (Vec<u8>, i64) {
    let forward = sel.offset > 0;
    let skip_equal = sel.or_equal == forward;
    let distance = if forward {
        sel.offset as i64
    } else {
        1 - sel.offset as i64
    };
    let wanted = (distance + skip_equal as i64) as usize;
    let limit = if (0..=1).contains(&sel.offset) {
        None
    } else {
        row_limit
    };
    let scan_range = if forward {
        KeyRange {
            begin: sel.key.clone(),
            end: range.end.clone(),
        }
    } else {
        KeyRange {
            begin: range.begin.clone(),
            end: key_after(&sel.key),
        }
    };
    let (mut rows, rep_more) = scan(data, &scan_range, !forward, wanted, limit);
    let mut more = rep_more && rows.len() != wanted;
    if more && !forward && rows.len() == 1 {
        let (rows2, rep_more2) = scan(data, &scan_range, true, 2, None);
        rows = rows2;
        more = rep_more2 && rows.len() != wanted;
    }
    let mut index = distance - 1;
    if skip_equal && rows.first() == Some(&sel.key) {
        index += 1;
    }
    if (index as usize) < rows.len() {
        return (rows[index as usize].clone(), 0);
    }
    let mut offset = index - rows.len() as i64 + 1;
    if !forward {
        offset = -offset;
    }
    let key = if more {
        let last = rows.last().expect("rows").clone();
        if forward { key_after(&last) } else { last }
    } else if forward {
        range.end.clone()
    } else {
        range.begin.clone()
    };
    (key, offset)
}

/// `getKeyQ`'s reply selector.
fn get_key_reply(key: Vec<u8>, offset: i64) -> KeySelector {
    match offset {
        0 => KeySelector::new(key, true, 0),
        o if o < 0 => KeySelector::new(key, false, (1 + o) as i32),
        o => KeySelector::new(key, false, o as i32),
    }
}

/// `getKeyValuesQ` and `readRange` (7.4:4730-4900, 4273-4524): the rows and `more`.
fn get_key_values(
    data: &BTreeMap<Vec<u8>, Vec<u8>>,
    owns: &[KeyRange],
    req: &GetKeyValues,
    row_limit: Option<usize>,
) -> Result<(Rows, bool), u16> {
    let shard = shard_range(owns, &req.begin).ok_or(codes::WRONG_SHARD_SERVER)?;
    if !selector_in_range(&req.end, &shard) && !(is_fge(&req.end) && req.end.key == shard.end) {
        return Err(codes::WRONG_SHARD_SERVER);
    }
    let (begin, off1) = if is_fge(&req.begin) {
        (req.begin.key.clone(), 0)
    } else {
        find_key(data, &req.begin, &shard, row_limit)
    };
    let (end, off2) = if is_fge(&req.end) {
        (req.end.key.clone(), 0)
    } else {
        find_key(data, &req.end, &shard, row_limit)
    };
    if (off1 != 0 && off1 != 1) || (off2 != 0 && off2 != 1) {
        return Err(codes::WRONG_SHARD_SERVER);
    }
    if begin >= end {
        return Ok((Vec::new(), false));
    }
    let reverse = req.limit < 0;
    let mut limit = req.limit.unsigned_abs() as usize;
    let mut bytes = req.limit_bytes as i64;
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = data
        .range(begin..end)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if reverse {
        rows.reverse();
    }
    let mut out = Vec::new();
    for row in rows {
        if limit == 0 || bytes <= 0 {
            break;
        }
        bytes -= (SERVER_ROW_OVERHEAD + row.0.len() + row.1.len()) as i64;
        limit -= 1;
        out.push(row);
    }
    Ok((out, limit == 0 || bytes <= 0))
}

/// What a selector resolves to, by definition: `""` before the first key, `\xff\xff` after
/// the last.
fn reference(keys: &[Vec<u8>], sel: &KeySelector) -> Vec<u8> {
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
        KEY_SPACE_END.to_vec()
    } else {
        keys[target as usize].clone()
    }
}

/// The rows of `[begin, end)` by definition, in the order of the read.
fn model(
    data: &BTreeMap<Vec<u8>, Vec<u8>>,
    begin: &KeySelector,
    end: &KeySelector,
    reverse: bool,
) -> Vec<KeyValue> {
    let keys: Vec<Vec<u8>> = data.keys().cloned().collect();
    let b = reference(&keys, begin);
    let e = reference(&keys, end);
    if b >= e {
        return Vec::new();
    }
    let mut rows: Vec<KeyValue> = data
        .range(b..e)
        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
        .collect();
    if reverse {
        rows.reverse();
    }
    rows
}

// ---- the fake cluster ----------------------------------------------------------------------

struct FakeStorage {
    _process: FakeProcess,
    server: StorageServer,
    state: Arc<Mutex<StorageState>>,
}

impl FakeStorage {
    fn kv_requests(&self) -> Vec<KvRequest> {
        self.state.lock().unwrap().kv_requests.clone()
    }

    fn key_requests(&self) -> usize {
        self.state.lock().unwrap().key_requests.len()
    }
}

struct Env {
    db: Database<MemRuntime>,
    data: Data,
    storage: Vec<FakeStorage>,
    shards: Arc<Mutex<ShardMap>>,
    locations: Arc<Mutex<Vec<GetKeyServerLocations>>>,
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

fn reply_after(token: Uid, payload: Vec<u8>, delay: Duration) -> Action {
    Action::Reply {
        token,
        payload,
        delay,
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
                let token = server.base.adjusted(STORAGE_SERVER_STREAMS.get_key).token;
                {
                    let state = state.clone();
                    let data = data.clone();
                    process.handle(token, move |incoming: Incoming| {
                        let (request, reply) =
                            GetKey::decode_request(incoming.schema, &incoming.payload).unwrap();
                        assert_eq!(request.version, READ_VERSION);
                        let mut st = state.lock().unwrap();
                        st.key_requests.push(request.sel.clone());
                        let answer = match (st.error, shard_range(&st.owns, &request.sel)) {
                            (Some(code), _) => Err(code),
                            (None, None) => Err(codes::WRONG_SHARD_SERVER),
                            (None, Some(r)) => {
                                let (k, o) =
                                    find_key(&data.lock().unwrap(), &request.sel, &r, st.row_limit);
                                Ok(get_key_reply(k, o))
                            }
                        };
                        let reply_value = match &answer {
                            Ok(sel) => GetKeyReply {
                                sel: sel.borrowed(),
                                ..GetKeyReply::default()
                            },
                            Err(code) => GetKeyReply {
                                error: Some(FdbError::new(*code)),
                                ..GetKeyReply::default()
                            },
                        };
                        let payload =
                            GetKey::encode_reply(incoming.schema, &Ok(reply_value)).unwrap();
                        reply_after(reply, payload, st.delay)
                    });
                }
                let token = server
                    .base
                    .adjusted(STORAGE_SERVER_STREAMS.get_key_values)
                    .token;
                {
                    let state = state.clone();
                    let data = data.clone();
                    process.handle(token, move |incoming: Incoming| {
                        let (request, reply) =
                            GetKeyValues::decode_request(incoming.schema, &incoming.payload)
                                .unwrap();
                        assert_eq!(request.version, READ_VERSION);
                        let mut st = state.lock().unwrap();
                        st.kv_requests.push(KvRequest {
                            begin: request.begin.clone(),
                            end: request.end.clone(),
                            limit: request.limit,
                            limit_bytes: request.limit_bytes,
                            task_id: request.task_id,
                        });
                        let answer = match st.error {
                            Some(code) => Err(code),
                            None => get_key_values(
                                &data.lock().unwrap(),
                                &st.owns,
                                &request,
                                st.row_limit,
                            ),
                        };
                        let rows;
                        let reply_value = match &answer {
                            Ok((r, more)) => {
                                rows = KeyValueVector::from_pairs(
                                    r.iter().map(|(k, v)| KeyValueRef::new(k, v)),
                                )
                                .unwrap();
                                GetKeyValuesReply {
                                    data: rows.borrowed(),
                                    more: *more,
                                    version: READ_VERSION,
                                    ..GetKeyValuesReply::default()
                                }
                            }
                            Err(code) => GetKeyValuesReply {
                                error: Some(FdbError::new(*code)),
                                ..GetKeyValuesReply::default()
                            },
                        };
                        let payload =
                            GetKeyValues::encode_reply(incoming.schema, &Ok(reply_value)).unwrap();
                        reply_after(reply, payload, st.delay)
                    });
                }
                let token = server.base.adjusted(STORAGE_SERVER_STREAMS.get_value).token;
                {
                    let state = state.clone();
                    let data = data.clone();
                    process.handle(token, move |incoming: Incoming| {
                        let (request, reply) =
                            GetValue::decode_request(incoming.schema, &incoming.payload).unwrap();
                        assert_eq!(request.version, READ_VERSION);
                        let mut st = state.lock().unwrap();
                        st.value_requests.push(request.key.clone());
                        let owned = st
                            .owns
                            .iter()
                            .any(|r| r.begin <= request.key && request.key < r.end);
                        let data = data.lock().unwrap();
                        let error = match (st.error, owned) {
                            (Some(code), _) => Some(code),
                            (None, false) => Some(codes::WRONG_SHARD_SERVER),
                            (None, true) => None,
                        };
                        let answer = GetValueReply {
                            penalty: 1.0,
                            error: error.map(FdbError::new),
                            value: if error.is_some() {
                                None
                            } else {
                                data.get(&request.key).map(Vec::as_slice)
                            },
                            cached: false,
                        };
                        let payload = GetValue::encode_reply(incoming.schema, &Ok(answer)).unwrap();
                        reply_after(reply, payload, st.delay)
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
        let shards = Arc::new(Mutex::new(vec![(range(b"", KEY_SPACE_END), vec![0])]));

        let grv_process = FakeProcess::start(&net, sock(10, 4500), version);
        let grv = GrvProxy {
            process_id: Some(b"grv".to_vec()),
            provisional: false,
            base: grv_process.endpoint(Uid::new(0x6001, 0x10)),
        };
        grv_process.handle(grv.base.token, |incoming: Incoming| {
            let (_, reply) =
                GetReadVersion::decode_request(incoming.schema, &incoming.payload).unwrap();
            let payload = GetReadVersion::encode_reply(
                incoming.schema,
                &Ok(read_version_reply(READ_VERSION)),
            )
            .unwrap();
            Action::reply(reply, payload)
        });

        // The commit proxy: locations (`doKeyServerLocationRequest`) and commits.
        let commit_process = FakeProcess::start(&net, sock(20, 4500), version);
        let commit = CommitProxy {
            process_id: Some(b"cp".to_vec()),
            provisional: false,
            base: commit_process.endpoint(Uid::new(0x7001, 0x20)),
            schema,
        };
        let locations = Arc::new(Mutex::new(Vec::new()));
        {
            let locations = locations.clone();
            let shards = shards.clone();
            let servers: Vec<StorageServer> = storage.iter().map(|s| s.server.clone()).collect();
            let token = commit.endpoints().get_key_servers_locations.token;
            commit_process.handle(token, move |incoming: Incoming| {
                let (request, reply) =
                    GetKeyServerLocations::decode_request(incoming.schema, &incoming.payload)
                        .unwrap();
                locations.lock().unwrap().push(request.clone());
                let shards = shards.lock().unwrap();
                let team = |t: &[usize]| t.iter().map(|&i| servers[i].clone()).collect();
                let key = request.begin.clone();
                let results: Vec<(KeyRange, Vec<StorageServer>)> = match &request.end {
                    None => {
                        let (r, t) = shards
                            .iter()
                            .find(|(r, _)| {
                                if request.reverse {
                                    r.begin < key && key <= r.end
                                } else {
                                    r.begin <= key && key < r.end
                                }
                            })
                            .expect("a shard for every key");
                        vec![(r.clone(), team(t))]
                    }
                    Some(end) if !request.reverse => shards
                        .iter()
                        .filter(|(r, _)| r.end > key && r.begin < *end)
                        .take(request.limit as usize)
                        .map(|(r, t)| (r.clone(), team(t)))
                        .collect(),
                    Some(end) => shards
                        .iter()
                        .rev()
                        .filter(|(r, _)| r.begin < *end && key < r.end)
                        .take(request.limit as usize)
                        .map(|(r, t)| (r.clone(), team(t)))
                        .collect(),
                };
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
            commits,
            _processes: vec![grv_process, commit_process],
        }
    }

    fn set(&self, keys: &[&[u8]]) {
        let mut data = self.data.lock().unwrap();
        for k in keys {
            data.insert(k.to_vec(), b"v".to_vec());
        }
    }

    fn set_value(&self, key: &[u8], value: &[u8]) {
        self.data
            .lock()
            .unwrap()
            .insert(key.to_vec(), value.to_vec());
    }

    /// The shard map: `(begin, end, server)`; each server then holds the union of its
    /// shards.
    fn layout(&self, shards: &[(&[u8], &[u8], usize)]) {
        for (i, s) in self.storage.iter().enumerate() {
            let mut owns: Vec<KeyRange> = Vec::new();
            for (b, e, _) in shards.iter().filter(|(_, _, owner)| *owner == i) {
                match owns.last_mut() {
                    Some(last) if last.end.as_slice() == *b => last.end = e.to_vec(),
                    _ => owns.push(range(b, e)),
                }
            }
            s.state.lock().unwrap().owns = owns;
        }
        *self.shards.lock().unwrap() = shards
            .iter()
            .map(|(b, e, owner)| (range(b, e), vec![*owner]))
            .collect();
    }

    fn row_limit(&self, limit: Option<usize>) {
        for s in &self.storage {
            s.state.lock().unwrap().row_limit = limit;
        }
    }

    fn delay(&self, delay: Duration) {
        for s in &self.storage {
            s.state.lock().unwrap().delay = delay;
        }
    }

    fn fail_with(&self, error: Option<u16>) {
        for s in &self.storage {
            s.state.lock().unwrap().error = error;
        }
    }

    fn kv_requests(&self) -> Vec<KvRequest> {
        self.storage
            .iter()
            .flat_map(FakeStorage::kv_requests)
            .collect()
    }

    fn clear_requests(&self) {
        for s in &self.storage {
            let mut st = s.state.lock().unwrap();
            st.kv_requests.clear();
            st.key_requests.clear();
            st.value_requests.clear();
        }
    }

    fn key_requests(&self) -> usize {
        self.storage.iter().map(FakeStorage::key_requests).sum()
    }

    /// The keys of the `GetValueRequest`s the storage servers received.
    fn value_requests(&self) -> Vec<Vec<u8>> {
        self.storage
            .iter()
            .flat_map(|s| s.state.lock().unwrap().value_requests.clone())
            .collect()
    }

    /// The storage requests of any kind so far.
    fn requests(&self) -> usize {
        self.kv_requests().len() + self.key_requests() + self.value_requests().len()
    }

    /// The commits received.
    fn commit_count(&self) -> usize {
        self.commits.lock().unwrap().len()
    }

    fn snapshot(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        self.data.lock().unwrap().clone()
    }

    async fn read(
        &self,
        begin: &KeySelector,
        end: &KeySelector,
        limits: RangeLimits,
        reverse: bool,
    ) -> RangeRead {
        within(
            "get_range",
            self.db.reader().get_range_with_limits(
                begin,
                end,
                limits,
                reverse,
                READ_VERSION,
                false,
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("{begin:?}..{end:?}: {e}"))
    }

    /// A (read-your-writes) transaction at the fake cluster's read version.
    fn transaction(&self) -> Transaction<MemRuntime> {
        let tx = self.db.create_transaction();
        tx.set_read_version(READ_VERSION);
        tx
    }

    /// The read conflict ranges of the commits, sorted, without the self-conflicting key.
    fn sent_read_conflicts(&self, commit: usize) -> Vec<KeyRange> {
        let commits = self.commits.lock().unwrap();
        let mut sent: Vec<KeyRange> = commits[commit]
            .read_conflict_ranges
            .iter()
            .filter(|r| !r.begin.starts_with(b"\xff/SC/"))
            .cloned()
            .collect();
        sent.sort_by(|a, b| (&a.begin, &a.end).cmp(&(&b.begin, &b.end)));
        sent
    }
}

fn fge(key: &[u8]) -> KeySelector {
    KeySelector::first_greater_or_equal(key.to_vec())
}
fn fgt(key: &[u8]) -> KeySelector {
    KeySelector::first_greater_than(key.to_vec())
}
fn llt(key: &[u8]) -> KeySelector {
    KeySelector::last_less_than(key.to_vec())
}
fn lle(key: &[u8]) -> KeySelector {
    KeySelector::last_less_or_equal(key.to_vec())
}
fn plus(sel: KeySelector, n: i32) -> KeySelector {
    KeySelector::new(sel.key, sel.or_equal, sel.offset + n)
}

fn keys_of(read: &RangeRead) -> Vec<Vec<u8>> {
    read.iter().map(|kv| kv.key.to_vec()).collect()
}

fn keys(rows: &[KeyValue]) -> Vec<Vec<u8>> {
    rows.iter().map(|kv| kv.key.clone()).collect()
}

fn ks(list: &[&[u8]]) -> Vec<Vec<u8>> {
    list.iter().map(|k| k.to_vec()).collect()
}

/// Keys `a f k` on the first shard, `n q x` on the second (in the two-shard layout).
const KEYS: [&[u8]; 6] = [b"a", b"f", b"k", b"n", b"q", b"x"];
const TWO_SHARDS: [(&[u8], &[u8], usize); 2] = [(b"", b"m", 0), (b"m", KEY_SPACE_END, 1)];
const THREE_SHARDS: [(&[u8], &[u8], usize); 3] =
    [(b"", b"g", 0), (b"g", b"p", 1), (b"p", KEY_SPACE_END, 2)];

// ---- helpers --------------------------------------------------------------------------------

fn keys_list(list: &[&[u8]]) -> Vec<Vec<u8>> {
    list.iter().map(|k| k.to_vec()).collect()
}

/// The error code of a read that must fail.
fn code_of<T: std::fmt::Debug>(r: Result<T, fdb_native_client::Error>) -> u16 {
    r.unwrap_err().code()
}

const ADD: MutationType = MutationType::ADD_VALUE;

async fn get(tx: &Transaction<MemRuntime>, key: &[u8]) -> Option<Vec<u8>> {
    within("get", tx.get(key))
        .await
        .unwrap_or_else(|e| panic!("get {key:?}: {e}"))
}

async fn get_key(tx: &Transaction<MemRuntime>, sel: &KeySelector) -> Vec<u8> {
    within("get_key", tx.get_key(sel))
        .await
        .unwrap_or_else(|e| panic!("get_key {sel:?}: {e}"))
}

async fn range_of(
    tx: &Transaction<MemRuntime>,
    begin: &KeySelector,
    end: &KeySelector,
    options: RangeOptions,
) -> (Vec<KeyValue>, bool) {
    let r = within("get_range", tx.get_range(begin, end, &options))
        .await
        .unwrap_or_else(|e| panic!("get_range {begin:?} {end:?}: {e}"));
    (r.kvs, r.more)
}

/// A row limit and no byte target (`EXACT`; 0 is 10 000 rows): one batch has every row up
/// to the limit, across shards. (With a byte target, a batch ends with the first shard that
/// returned rows, and the read-your-writes layer stops at the first range it would have to
/// read once it has rows.)
fn exact(limit: u32) -> RangeOptions {
    RangeOptions::with_limit(if limit == 0 { 10_000 } else { limit }).mode(StreamingMode::Exact)
}

async fn all_rows(tx: &Transaction<MemRuntime>, reverse: bool) -> Vec<KeyValue> {
    range_of(tx, &fge(b""), &fge(b"\xff"), exact(0).reverse(reverse))
        .await
        .0
}

fn kv(k: &[u8], v: &[u8]) -> KeyValue {
    KeyValue::new(k.to_vec(), v.to_vec())
}

/// Commits `tx` (with a write conflict range far from the keys read, so that the commit is
/// sent) and returns the read conflict ranges it sent.
async fn commit_reads(env: &Env, tx: &Transaction<MemRuntime>) -> Vec<KeyRange> {
    tx.add_write_conflict_range(b"\xfe", b"\xfe\x00");
    let n = env.commit_count();
    within("commit", tx.commit()).await.expect("commit");
    assert_eq!(env.commit_count(), n + 1);
    env.sent_read_conflicts(n)
}

fn ranges(list: &[(&[u8], &[u8])]) -> Vec<KeyRange> {
    list.iter().map(|(b, e)| range(b, e)).collect()
}

// ---- reads see the transaction's writes ------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn reads_see_own_sets_clears_and_atomic_ops() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&KEYS);
        env.layout(&TWO_SHARDS);
        let tx = env.transaction();
        tx.set(b"b", b"own-b");
        tx.clear(b"f");
        tx.clear_range(b"m", b"q");
        // A dependent operation: `v` (0x76) + 1.
        tx.atomic_op(b"k", &[1], ADD);
        // On a key the database does not have: the operand.
        tx.atomic_op(b"c", &[5], ADD);
        // On a cleared key: over nothing, no read needed.
        tx.atomic_op(b"n", &[7], ADD);

        assert_eq!(get(&tx, b"a").await, Some(b"v".to_vec()));
        assert_eq!(get(&tx, b"b").await, Some(b"own-b".to_vec()));
        assert_eq!(get(&tx, b"f").await, None);
        assert_eq!(get(&tx, b"n").await, Some(vec![7]));
        assert_eq!(get(&tx, b"k").await, Some(vec![0x77]));
        assert_eq!(get(&tx, b"c").await, Some(vec![5]));
        // Only the database's keys that the writes do not decide were read.
        let mut asked = env.value_requests();
        asked.sort();
        assert_eq!(asked, ks(&[b"a", b"c", b"k"]), "{schema:?}");
        // Read again: from the cache.
        assert_eq!(get(&tx, b"a").await, Some(b"v".to_vec()));
        assert_eq!(get(&tx, b"k").await, Some(vec![0x77]));
        assert_eq!(env.value_requests().len(), 3);

        let expected = vec![
            kv(b"a", b"v"),
            kv(b"b", b"own-b"),
            kv(b"c", &[5]),
            kv(b"k", &[0x77]),
            kv(b"n", &[7]),
            kv(b"q", b"v"),
            kv(b"x", b"v"),
        ];
        assert_eq!(all_rows(&tx, false).await, expected, "{schema:?}");
        let mut reversed = expected.clone();
        reversed.reverse();
        // The whole range is cached now: no storage read.
        let before = env.requests();
        assert_eq!(all_rows(&tx, true).await, reversed);
        assert_eq!(env.requests(), before);

        // Later writes are seen by later reads.
        tx.set(b"a", b"again");
        tx.atomic_op(b"k", &[1], ADD);
        assert_eq!(get(&tx, b"a").await, Some(b"again".to_vec()));
        assert_eq!(get(&tx, b"k").await, Some(vec![0x78]));
        assert_eq!(env.requests(), before);
    }
}

#[tokio::test(start_paused = true)]
async fn clear_range_then_get_range_across_shards() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 3).await;
        env.set(&KEYS);
        env.layout(&THREE_SHARDS);
        let tx = env.transaction();
        tx.clear_range(b"g", b"r");
        let (rows, more) = range_of(&tx, &fge(b""), &fge(b"\xff"), exact(0)).await;
        assert_eq!(keys(&rows), ks(&[b"a", b"f", b"x"]), "{schema:?}");
        assert!(!more);
        let (rows, more) = range_of(&tx, &fge(b""), &fge(b"\xff"), exact(2)).await;
        assert_eq!(keys(&rows), ks(&[b"a", b"f"]));
        assert!(more);
        let (rows, more) = range_of(&tx, &fge(b""), &fge(b"\xff"), exact(2).reverse(true)).await;
        assert_eq!(keys(&rows), ks(&[b"x", b"f"]));
        assert!(more);
        // With a byte target, a batch stops at the first shard boundary once it has rows
        // (in a new transaction: `tx` has the whole range cached now, and answers it all).
        let (rows, more) = range_of(
            &tx,
            &fge(b""),
            &fge(b"\xff"),
            RangeOptions::default().reverse(true),
        )
        .await;
        assert_eq!(keys(&rows), ks(&[b"x", b"f", b"a"]));
        assert!(!more);
        let fresh = env.transaction();
        fresh.clear_range(b"g", b"r");
        let (rows, more) = range_of(
            &fresh,
            &fge(b""),
            &fge(b"\xff"),
            RangeOptions::default().reverse(true),
        )
        .await;
        assert_eq!(keys(&rows), ks(&[b"x"]));
        assert!(more);
        // A range inside the clear: nothing, and no storage read.
        let before = env.requests();
        let (rows, more) = range_of(&tx, &fge(b"h"), &fge(b"p"), RangeOptions::default()).await;
        assert!(rows.is_empty() && !more);
        assert_eq!(env.requests(), before);
    }
}

#[tokio::test(start_paused = true)]
async fn reverse_ranges_over_cached_uncached_and_written_segments() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 3).await;
        env.set(&KEYS);
        env.layout(&THREE_SHARDS);
        let tx = env.transaction();
        // Cache `[f, n)` (f, k), then write around and inside it.
        let (rows, _) = range_of(&tx, &fge(b"f"), &fge(b"n"), exact(0)).await;
        assert_eq!(keys(&rows), ks(&[b"f", b"k"]));
        tx.set(b"h", b"own-h");
        tx.clear(b"q");
        tx.atomic_op(b"x", &[1], ADD);
        tx.set(b"b", b"own-b");
        // `[f, n)` is known: read again, it needs no storage read.
        let before = env.requests();
        let (rows, _) = range_of(&tx, &fge(b"f"), &fge(b"n"), exact(0)).await;
        assert_eq!(keys(&rows), ks(&[b"f", b"h", b"k"]));
        assert_eq!(env.requests(), before);
        let mut model = env.snapshot();
        model.insert(b"h".to_vec(), b"own-h".to_vec());
        model.remove(b"q".as_slice());
        model.insert(b"x".to_vec(), vec![0x77]);
        model.insert(b"b".to_vec(), b"own-b".to_vec());
        let selectors = [
            fge(b""),
            fge(b"c"),
            fgt(b"f"),
            lle(b"k"),
            llt(b"n"),
            plus(fge(b"g"), 2),
            plus(llt(b"x"), -1),
            fge(b"z"),
        ];
        for begin in &selectors {
            for end in &selectors {
                for limit in [0, 1, 2, 3] {
                    for reverse in [false, true] {
                        let options = exact(limit).reverse(reverse);
                        let (rows, more) = range_of(&tx, begin, end, options).await;
                        let mut expected = model_of(&model, begin, end, reverse);
                        let full = expected.len();
                        if limit != 0 {
                            expected.truncate(limit as usize);
                        }
                        assert_eq!(
                            rows, expected,
                            "{schema:?} {begin:?} {end:?} limit {limit} reverse {reverse}"
                        );
                        if expected.len() < full {
                            assert!(more, "{begin:?} {end:?} {limit} {reverse}");
                        }
                    }
                }
            }
        }
    }
}

/// The rows of `[begin, end)` in `model` (selectors resolved in it, `\xff` the bound), in
/// the order of the read.
fn model_of(
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    begin: &KeySelector,
    end: &KeySelector,
    reverse: bool,
) -> Vec<KeyValue> {
    let keys: Vec<Vec<u8>> = model.keys().cloned().collect();
    let clamp = |k: Vec<u8>| {
        if k.as_slice() > b"\xff".as_slice() {
            b"\xff".to_vec()
        } else {
            k
        }
    };
    let b = clamp(reference(&keys, begin));
    let e = clamp(reference(&keys, end));
    if b >= e {
        return Vec::new();
    }
    let mut rows: Vec<KeyValue> = model
        .range(b..e)
        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
        .collect();
    if reverse {
        rows.reverse();
    }
    rows
}

#[tokio::test(start_paused = true)]
async fn key_selectors_resolve_through_own_writes() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&KEYS);
        env.layout(&TWO_SHARDS);
        let tx = env.transaction();
        tx.set(b"b", b"own");
        tx.clear(b"f");
        // The transaction's keys: a b k n q x.
        assert_eq!(get_key(&tx, &fgt(b"a")).await, b"b");
        assert_eq!(get_key(&tx, &lle(b"g")).await, b"b");
        assert_eq!(get_key(&tx, &fge(b"c")).await, b"k");
        assert_eq!(get_key(&tx, &llt(b"k")).await, b"b");
        assert_eq!(get_key(&tx, &plus(fge(b"a"), 2)).await, b"k");
        assert_eq!(get_key(&tx, &plus(llt(b"n"), -2)).await, b"a");
        // Off the ends.
        assert_eq!(get_key(&tx, &llt(b"a")).await, b"");
        assert_eq!(get_key(&tx, &fgt(b"x")).await, b"\xff");
        assert_eq!(get_key(&tx, &plus(fge(b"a"), 10)).await, b"\xff");
        // A range whose selectors resolve through the writes.
        let (rows, _) = range_of(&tx, &fgt(b"a"), &plus(fge(b"a"), 3), exact(0)).await;
        assert_eq!(keys(&rows), ks(&[b"b", b"k"]), "{schema:?}");
        let (rows, _) = range_of(
            &tx,
            &plus(llt(b"n"), -1),
            &lle(b"q"),
            exact(0).reverse(true),
        )
        .await;
        assert_eq!(keys(&rows), ks(&[b"n", b"k", b"b"]));
    }
}

/// A read takes its view when it is called: writes made before the future is polled are
/// not seen (C++'s read actors start at once).
#[tokio::test(start_paused = true)]
async fn a_read_sees_the_writes_made_before_it_was_called() {
    let env = Env::new(SchemaVersion::V8_0, 1).await;
    env.set(&KEYS);
    let tx = env.transaction();
    let value = tx.get(b"k");
    let key = tx.get_key(&fgt(b"f"));
    let rows = tx.get_range(&fge(b""), &fge(b"\xff"), &exact(0));
    tx.set(b"k", b"new");
    tx.set(b"g", b"new");
    tx.clear_range(b"", b"\xff");
    assert_eq!(within("get", value).await.unwrap(), Some(b"v".to_vec()));
    assert_eq!(within("get_key", key).await.unwrap(), b"k");
    let rows = within("get_range", rows).await.unwrap();
    assert_eq!(keys(&rows.kvs), keys_list(&KEYS));
    // Reads called now see the writes.
    assert_eq!(get(&tx, b"k").await, None);
    assert!(all_rows(&tx, false).await.is_empty());
}

// ---- conflict ranges, snapshot reads, options --------------------------------------------

#[tokio::test(start_paused = true)]
async fn snapshot_reads_add_no_conflict_ranges_serializable_reads_skip_own_writes() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&KEYS);
        env.layout(&TWO_SHARDS);
        let tx = env.transaction();
        tx.set(b"b", b"own");
        assert_eq!(get(&tx, b"a").await, Some(b"v".to_vec()));
        assert_eq!(get(&tx, b"b").await, Some(b"own".to_vec()));
        let snapshot = tx.snapshot();
        assert_eq!(
            within("snapshot get", snapshot.get(b"k")).await.unwrap(),
            Some(b"v".to_vec())
        );
        let (rows, _) = range_of(&tx, &fge(b"e"), &fge(b"h"), exact(0)).await;
        assert_eq!(keys(&rows), ks(&[b"f"]));
        let rows = within(
            "snapshot get_range",
            snapshot.get_range(&fge(b"p"), &fge(b"r"), &RangeOptions::default()),
        )
        .await
        .unwrap();
        assert_eq!(keys(&rows.kvs), ks(&[b"q"]));
        assert_eq!(
            within("snapshot get_key", snapshot.get_key(&fgt(b"q")))
                .await
                .unwrap(),
            b"x"
        );
        assert_eq!(get_key(&tx, &fgt(b"n")).await, b"q");
        assert_eq!(
            commit_reads(&env, &tx).await,
            ranges(&[(b"a", b"a\x00"), (b"e", b"h"), (b"n\x00", b"q\x00")]),
            "{schema:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn snapshot_ryw_disable_makes_snapshot_reads_ignore_own_writes() {
    let env = Env::new(SchemaVersion::V8_0, 1).await;
    env.set(&KEYS);
    let tx = env.transaction();
    tx.set(b"k", b"own");
    tx.clear(b"a");
    tx.set_option(TransactionOption::SnapshotRywDisable)
        .unwrap();
    let snapshot = tx.snapshot();
    assert_eq!(
        within("get", snapshot.get(b"k")).await.unwrap(),
        Some(b"v".to_vec())
    );
    let rows = within(
        "range",
        snapshot.get_range(&fge(b""), &fge(b"\xff"), &exact(0)),
    )
    .await
    .unwrap();
    assert_eq!(keys(&rows.kvs), keys_list(&KEYS));
    assert_eq!(
        within("get_key", snapshot.get_key(&fge(b"")))
            .await
            .unwrap(),
        b"a"
    );
    // Serializable reads still see them.
    assert_eq!(get(&tx, b"k").await, Some(b"own".to_vec()));
    assert_eq!(get(&tx, b"a").await, None);
    // A count: enabled again.
    tx.set_option(TransactionOption::SnapshotRywEnable).unwrap();
    assert_eq!(
        within("get", tx.snapshot().get(b"k")).await.unwrap(),
        Some(b"own".to_vec())
    );
    tx.set_option(TransactionOption::SnapshotRywEnable).unwrap();
    tx.set_option(TransactionOption::SnapshotRywDisable)
        .unwrap();
    assert_eq!(
        within("get", tx.snapshot().get(b"k")).await.unwrap(),
        Some(b"own".to_vec())
    );

    // The database's default, for transactions created (or reset) afterwards.
    env.db
        .set_option(fdb_native_client::DatabaseOption::SnapshotRywDisable)
        .unwrap();
    let tx = env.transaction();
    tx.set(b"k", b"own");
    assert_eq!(
        within("get", tx.snapshot().get(b"k")).await.unwrap(),
        Some(b"v".to_vec())
    );
    tx.set_option(TransactionOption::SnapshotRywEnable).unwrap();
    assert_eq!(
        within("get", tx.snapshot().get(b"k")).await.unwrap(),
        Some(b"own".to_vec())
    );
    // `on_error` starts again from the database's count.
    within(
        "on_error",
        tx.on_error(fdb_native_client::Error::new(codes::NOT_COMMITTED)),
    )
    .await
    .unwrap();
    tx.set_read_version(READ_VERSION);
    tx.set(b"k", b"own");
    assert_eq!(
        within("get", tx.snapshot().get(b"k")).await.unwrap(),
        Some(b"v".to_vec())
    );
    env.db
        .set_option(fdb_native_client::DatabaseOption::SnapshotRywEnable)
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn read_your_writes_disable_reads_the_database_with_native_conflict_ranges() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&KEYS);
        env.layout(&TWO_SHARDS);
        let tx = env.transaction();
        tx.set_option(TransactionOption::ReadYourWritesDisable)
            .unwrap();
        tx.set(b"k", b"own");
        tx.clear(b"a");
        assert_eq!(get(&tx, b"k").await, Some(b"v".to_vec()));
        assert_eq!(get(&tx, b"a").await, Some(b"v".to_vec()));
        assert_eq!(keys(&all_rows(&tx, false).await), keys_list(&KEYS));
        assert_eq!(get_key(&tx, &fge(b"")).await, b"a");
        // No cache: the same read asks again.
        let before = env.value_requests().len();
        assert_eq!(get(&tx, b"k").await, Some(b"v".to_vec()));
        assert_eq!(env.value_requests().len(), before + 1);
        // NativeAPI's ranges: the written keys too, `getRangeFinished`'s range.
        let sent = commit_reads(&env, &tx).await;
        assert_eq!(
            sent,
            ranges(&[
                (b"", b"a\x00"),
                (b"", b"\xff"),
                (b"a", b"a\x00"),
                (b"k", b"k\x00"),
                (b"k", b"k\x00"),
            ]),
            "{schema:?}"
        );

        // Only before the first read or write.
        let tx = env.transaction();
        get(&tx, b"a").await;
        let e = tx
            .set_option(TransactionOption::ReadYourWritesDisable)
            .unwrap_err();
        assert_eq!(e.code(), codes::CLIENT_INVALID_OPERATION);
        let tx = env.transaction();
        tx.set(b"a", b"x");
        let e = tx
            .set_option(TransactionOption::ReadYourWritesDisable)
            .unwrap_err();
        assert_eq!(e.code(), codes::CLIENT_INVALID_OPERATION);
        // A read refused by its checks leaves nothing behind.
        let tx = env.transaction();
        assert_eq!(
            within("get", tx.get(b"\xff\xff/x"))
                .await
                .unwrap_err()
                .code(),
            codes::KEY_OUTSIDE_LEGAL_RANGE
        );
        tx.set_option(TransactionOption::ReadYourWritesDisable)
            .unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn versionstamped_keys_are_unreadable() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        env.set(&KEYS);
        let tx = env.transaction();
        // `vs` + a 10-byte placeholder + `z`, the placeholder at offset 2.
        let mut key = b"vs".to_vec();
        key.extend([0xff; 10]);
        key.push(b'z');
        key.extend(2i32.to_le_bytes());
        tx.set_versionstamped_key(&key, b"value");
        let unreadable =
            fdb_native_client::atomic::versionstamp_key_range(&key, Some(READ_VERSION), b"\xff")
                .unwrap();
        assert_eq!(
            code_of(within("get", tx.get(&unreadable.begin)).await),
            codes::ACCESSED_UNREADABLE,
            "{schema:?}"
        );
        assert_eq!(
            code_of(
                within(
                    "range",
                    tx.get_range(&fge(b"u"), &fge(b"w"), &RangeOptions::default())
                )
                .await
            ),
            codes::ACCESSED_UNREADABLE
        );
        assert_eq!(
            code_of(within("get_key", tx.get_key(&fgt(b"u"))).await),
            codes::ACCESSED_UNREADABLE
        );
        // Outside the range: readable.
        assert_eq!(get(&tx, b"q").await, Some(b"v".to_vec()));
        let (rows, _) = range_of(&tx, &fge(b""), &fge(b"u"), exact(0)).await;
        assert_eq!(keys(&rows), ks(&[b"a", b"f", b"k", b"n", b"q"]));

        // A versionstamped value: its key is unreadable, but for `BypassUnreadable`'s `get`.
        let mut value = b"0123456789".to_vec();
        value.extend(0i32.to_le_bytes());
        tx.set_versionstamped_value(b"k", &value);
        assert_eq!(
            code_of(within("get", tx.get(b"k")).await),
            codes::ACCESSED_UNREADABLE
        );
        tx.set_option(TransactionOption::BypassUnreadable).unwrap();
        assert_eq!(get(&tx, b"k").await, Some(value.clone()));
        // Range reads are not bypassed (C++).
        assert_eq!(
            code_of(
                within(
                    "range",
                    tx.get_range(&fge(b"j"), &fge(b"l"), &RangeOptions::default())
                )
                .await
            ),
            codes::ACCESSED_UNREADABLE
        );
        // A clear makes the keys readable again.
        tx.clear(b"k");
        tx.clear_range(&unreadable.begin, &unreadable.end);
        let (rows, _) = range_of(&tx, &fge(b"j"), &fge(b"\xff"), exact(0)).await;
        assert_eq!(keys(&rows), ks(&[b"n", b"q", b"x"]));
        // The failed reads added no conflict range.
        assert_eq!(
            commit_reads(&env, &tx).await,
            // `["", u)` and `[j, \xff)` less `k` and the unreadable range (cleared), joined.
            vec![
                range(b"", &unreadable.begin),
                range(&unreadable.end, b"\xff"),
            ],
            "{schema:?}"
        );
    }
}

// ---- the randomized model test ---------------------------------------------------------------

/// A small deterministic generator.
struct Gen(u64);

impl Gen {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn key(&mut self) -> Vec<u8> {
        let mut k = vec![b'a' + self.below(16) as u8];
        let zeros = self.below(3) as usize;
        k.extend(std::iter::repeat_n(0, zeros));
        k
    }

    fn value(&mut self) -> Vec<u8> {
        (0..self.below(3)).map(|_| self.below(4) as u8).collect()
    }

    fn selector(&mut self) -> KeySelector {
        KeySelector::new(self.key(), self.below(2) == 0, self.below(8) as i32 - 3)
    }
}

const MODEL_OPS: [MutationType; 9] = [
    MutationType::ADD_VALUE,
    MutationType::AND_V2,
    MutationType::OR,
    MutationType::XOR,
    MutationType::MAX,
    MutationType::MIN_V2,
    MutationType::BYTE_MIN,
    MutationType::BYTE_MAX,
    MutationType::APPEND_IF_FITS,
];

/// Random writes and reads (get, get_key, forward and reverse ranges with limits) in
/// transactions over random databases and shard maps, against the database with the writes
/// applied (`atomic` for the atomic operations; not `CompareAndClear`, whose cleared keys
/// still count for selectors in C++'s cache, see the unit tests).
#[tokio::test(start_paused = true)]
async fn random_reads_and_writes_match_a_model() {
    let mut g = Gen(0x9e37_79b9_7f4a_7c15);
    for round in 0..12 {
        let schema = SCHEMAS[round % 2];
        let env = Env::new(schema, 3).await;
        for _ in 0..g.below(14) {
            let (k, v) = (g.key(), g.value());
            env.set_value(&k, &v);
        }
        // A random shard map over the three servers.
        let mut cuts: Vec<Vec<u8>> = (0..g.below(4)).map(|_| g.key()).collect();
        cuts.sort();
        cuts.dedup();
        cuts.retain(|c| !c.is_empty());
        let mut bounds: Vec<Vec<u8>> = vec![Vec::new()];
        bounds.extend(cuts);
        bounds.push(KEY_SPACE_END.to_vec());
        let shards: Vec<(Vec<u8>, Vec<u8>, usize)> = bounds
            .windows(2)
            .enumerate()
            .map(|(i, w)| (w[0].clone(), w[1].clone(), i % 3))
            .collect();
        let refs: Vec<(&[u8], &[u8], usize)> = shards
            .iter()
            .map(|(b, e, s)| (b.as_slice(), e.as_slice(), *s))
            .collect();
        env.layout(&refs);

        for _ in 0..3 {
            let tx = env.transaction();
            let mut model = env.snapshot();
            for step in 0..40 {
                match g.below(8) {
                    0 => {
                        let (k, v) = (g.key(), g.value());
                        tx.set(&k, &v);
                        model.insert(k, v);
                    }
                    1 => {
                        let k = g.key();
                        tx.clear(&k);
                        model.remove(&k);
                    }
                    2 => {
                        let (mut a, mut b) = (g.key(), g.key());
                        if a > b {
                            std::mem::swap(&mut a, &mut b);
                        }
                        tx.clear_range(&a, &b);
                        let doomed: Vec<Vec<u8>> = model
                            .range(a.clone()..b.clone())
                            .map(|(k, _)| k.clone())
                            .collect();
                        for k in doomed {
                            model.remove(&k);
                        }
                    }
                    3 => {
                        let (k, v) = (g.key(), g.value());
                        let op = MODEL_OPS[g.below(MODEL_OPS.len() as u64) as usize];
                        tx.atomic_op(&k, &v, op);
                        let new = fdb_native_client::atomic::apply(
                            op,
                            model.get(&k).map(Vec::as_slice),
                            &v,
                            fdb_native_client::atomic::VALUE_SIZE_LIMIT,
                        )
                        .unwrap();
                        match new {
                            Some(new) => model.insert(k, new),
                            None => model.remove(&k),
                        };
                    }
                    4 => {
                        let k = g.key();
                        assert_eq!(
                            get(&tx, &k).await,
                            model.get(&k).cloned(),
                            "round {round} step {step} get {k:?}"
                        );
                    }
                    5 => {
                        let sel = g.selector();
                        let keys: Vec<Vec<u8>> = model.keys().cloned().collect();
                        let mut expected = reference(&keys, &sel);
                        if expected.as_slice() > b"\xff".as_slice() {
                            expected = b"\xff".to_vec();
                        }
                        assert_eq!(
                            get_key(&tx, &sel).await,
                            expected,
                            "round {round} step {step} get_key {sel:?}"
                        );
                    }
                    _ => {
                        let (begin, end) = (g.selector(), g.selector());
                        let mode = [
                            StreamingMode::WantAll,
                            StreamingMode::Small,
                            StreamingMode::Iterator,
                            StreamingMode::Exact,
                        ][g.below(4) as usize];
                        let limit = match (mode, [0, 1, 2, 5][g.below(4) as usize]) {
                            (StreamingMode::Exact, 0) => 10_000,
                            (_, limit) => limit,
                        };
                        let reverse = g.below(2) == 0;
                        let options = RangeOptions::with_limit(limit).mode(mode).reverse(reverse);
                        let (rows, more) = range_of(&tx, &begin, &end, options).await;
                        let expected = model_of(&model, &begin, &end, reverse);
                        let what = format!(
                            "round {round} step {step} {begin:?} {end:?} limit {limit} \
                             {mode:?} reverse {reverse}"
                        );
                        assert!(
                            rows.len() <= expected.len(),
                            "{what}: {rows:?} {expected:?}"
                        );
                        assert_eq!(rows, expected[..rows.len()], "{what}");
                        if limit != 0 {
                            assert!(rows.len() <= limit as usize, "{what}");
                        }
                        if !expected.is_empty() {
                            assert!(!rows.is_empty(), "{what}");
                        }
                        if mode == StreamingMode::Exact {
                            // No byte target: every row up to the limit.
                            assert_eq!(rows.len(), expected.len().min(limit as usize), "{what}");
                        }
                        if rows.len() < expected.len() {
                            assert!(more, "{what}");
                        }
                    }
                }
            }
            // The commit's read conflict ranges are disjoint and ordered.
            let sent = commit_reads(&env, &tx).await;
            for w in sent.windows(2) {
                assert!(w[0].end < w[1].begin, "{sent:?}");
            }
        }
    }
}

// ---- the conflict range corpus ---------------------------------------------------------------

/// One step of a corpus case.
#[derive(Clone, Debug)]
enum C {
    Set(&'static [u8]),
    Clear(&'static [u8]),
    ClearRange(&'static [u8], &'static [u8]),
    Add(&'static [u8]),
    Get(&'static [u8]),
    SnapshotGet(&'static [u8]),
    GetKey(KeySelector),
    /// A range read with a row limit (0: 10 000) and no byte target.
    Range(KeySelector, KeySelector, u32, bool),
    /// A range read with the default options (the `ITERATOR` mode's byte target).
    RangeIterator(KeySelector, KeySelector),
    SnapshotRange(KeySelector, KeySelector),
    ReadConflict(&'static [u8], &'static [u8]),
}

/// The corpus: steps on the database `a f k n q x` (one shard), and the read conflict
/// ranges C++'s read-your-writes layer sends at the commit, worked out by hand from
/// `RYWImpl::addConflictRange` / `updateConflictMap` and `CoalescedKeyRefRangeMap::insert`.
fn corpus() -> Vec<(&'static str, Vec<C>, Vec<KeyRange>)> {
    use C::*;
    vec![
        ("get", vec![Get(b"a")], ranges(&[(b"a", b"a\x00")])),
        (
            "get of a missing key",
            vec![Get(b"b")],
            ranges(&[(b"b", b"b\x00")]),
        ),
        ("get of an own set", vec![Set(b"a"), Get(b"a")], vec![]),
        ("get of an own clear", vec![Clear(b"a"), Get(b"a")], vec![]),
        // A dependent write needs the database's value.
        (
            "get of an own atomic op",
            vec![Add(b"a"), Get(b"a")],
            ranges(&[(b"a", b"a\x00")]),
        ),
        // An atomic op on a cleared key is independent.
        (
            "get of an atomic op after a clear",
            vec![Clear(b"a"), Add(b"a"), Get(b"a")],
            vec![],
        ),
        // Touching ranges coalesce (`[a, a\0)` + `[a\0, a\0\0)`), others stay apart.
        (
            "gets coalesce",
            vec![Get(b"a"), Get(b"a\x00"), Get(b"c")],
            ranges(&[(b"a", b"a\x00\x00"), (b"c", b"c\x00")]),
        ),
        (
            "snapshot reads",
            vec![SnapshotGet(b"a"), SnapshotRange(fge(b""), fge(b"\xff"))],
            vec![],
        ),
        (
            "range",
            vec![Range(fge(b"b"), fge(b"m"), 0, false)],
            ranges(&[(b"b", b"m")]),
        ),
        // `more` with a limit: up to after the last row (the end key is `begin` then
        // `keyAfter(last)`).
        (
            "range cut by a limit",
            vec![Range(fge(b"b"), fge(b"m"), 1, false)],
            ranges(&[(b"b", b"f\x00")]),
        ),
        // Reverse, cut: from the last row read (the begin offset is 1) to the end.
        (
            "reverse range cut by a limit",
            vec![Range(fge(b"b"), fge(b"m"), 1, true)],
            ranges(&[(b"k", b"m")]),
        ),
        (
            "reverse range",
            vec![Range(fge(b"b"), fge(b"m"), 0, true)],
            ranges(&[(b"b", b"m")]),
        ),
        // Own independent writes are left out.
        (
            "range over an own set",
            vec![Set(b"g"), Range(fge(b"b"), fge(b"m"), 0, false)],
            ranges(&[(b"b", b"g"), (b"g\x00", b"m")]),
        ),
        (
            "range over an own clear",
            vec![
                ClearRange(b"c", b"h"),
                Range(fge(b"a"), fge(b"z"), 0, false),
            ],
            ranges(&[(b"a", b"c"), (b"h", b"z")]),
        ),
        // A dependent write is in.
        // With a byte target, the read stops before the range it would have to read once it
        // has rows (`hasSatisfiedMinRows`): `[a]`, `more`; the range ends after it.
        (
            "byte-limited range over an own clear",
            vec![ClearRange(b"c", b"h"), RangeIterator(fge(b"a"), fge(b"z"))],
            ranges(&[(b"a", b"a\x00")]),
        ),
        (
            "range over an own atomic op",
            vec![Add(b"k"), Range(fge(b"b"), fge(b"m"), 0, false)],
            ranges(&[(b"b", b"m")]),
        ),
        // A begin offset of 0: from the first row.
        (
            "range from lastLessThan",
            vec![Range(llt(b"f"), fge(b"n"), 0, false)],
            ranges(&[(b"a", b"n")]),
        ),
        // An end offset above 1, not cut: to the end key, then after the last row.
        (
            "range to an offset end",
            vec![Range(fge(b"b"), plus(fge(b"k"), 1), 0, false)],
            ranges(&[(b"b", b"k\x00")]),
        ),
        (
            "whole range",
            vec![Range(fge(b""), fge(b"\xff"), 0, false)],
            ranges(&[(b"", b"\xff")]),
        ),
        // `getKey`: `[keyAfter(k), keyAfter(result))` for `firstGreaterThan(k)`.
        (
            "get_key forward",
            vec![GetKey(fgt(b"f"))],
            ranges(&[(b"f\x00", b"k\x00")]),
        ),
        (
            "get_key backward",
            vec![GetKey(llt(b"k"))],
            ranges(&[(b"f", b"k")]),
        ),
        (
            "get_key or_equal backward",
            vec![GetKey(lle(b"k"))],
            ranges(&[(b"k", b"k\x00")]),
        ),
        (
            "get_key through an own set",
            vec![Set(b"g"), GetKey(fgt(b"f"))],
            ranges(&[(b"f\x00", b"g")]),
        ),
        // Off the end: the result is `\xff`, `keyAfter(\xff)` is `\xff\x00`.
        (
            "get_key off the end",
            vec![GetKey(fgt(b"x"))],
            ranges(&[(b"x\x00", b"\xff\x00")]),
        ),
        (
            "get_key off the beginning",
            vec![GetKey(llt(b"a"))],
            ranges(&[(b"", b"a")]),
        ),
        // `addReadConflictRange` leaves out own writes too.
        (
            "add_read_conflict_range",
            vec![Set(b"f"), ReadConflict(b"a", b"z")],
            ranges(&[(b"a", b"f"), (b"f\x00", b"z")]),
        ),
        (
            "range over an own set, cut",
            vec![Set(b"g"), Range(fge(b"b"), fge(b"m"), 2, false)],
            ranges(&[(b"b", b"g")]),
        ),
        (
            "get in an own clear",
            vec![ClearRange(b"a", b"c"), Get(b"b")],
            vec![],
        ),
        (
            "empty range",
            vec![Range(fge(b"b"), fge(b"c"), 10, false)],
            ranges(&[(b"b", b"c")]),
        ),
        (
            "range around an own single clear",
            vec![Clear(b"k"), Range(fge(b"f"), fge(b"n"), 0, false)],
            ranges(&[(b"f", b"k"), (b"k\x00", b"n")]),
        ),
        // A write after a read leaves the read's range.
        (
            "read, write, read",
            vec![Get(b"a"), Set(b"a"), Get(b"a")],
            ranges(&[(b"a", b"a\x00")]),
        ),
        (
            "range, then a write in it",
            vec![Range(fge(b"a"), fge(b"c"), 10, false), Set(b"b")],
            ranges(&[(b"a", b"c")]),
        ),
        (
            "ranges over keys with zero bytes",
            vec![
                Set(b"f\x00"),
                Range(fge(b"f"), fge(b"f\x00\x00"), 10, false),
            ],
            ranges(&[(b"f", b"f\x00")]),
        ),
        (
            "reads and an explicit range coalesce",
            vec![
                Get(b"a"),
                Range(fge(b"b"), fge(b"c"), 0, false),
                ReadConflict(b"a\x00", b"b"),
            ],
            ranges(&[(b"a", b"c")]),
        ),
    ]
}

#[tokio::test(start_paused = true)]
async fn read_conflict_ranges_match_cpp_for_a_corpus() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        env.set(&KEYS);
        for (name, steps, expected) in corpus() {
            let tx = env.transaction();
            for step in steps {
                match step {
                    C::Set(k) => tx.set(k, b"own"),
                    C::Clear(k) => tx.clear(k),
                    C::ClearRange(b, e) => tx.clear_range(b, e),
                    C::Add(k) => tx.atomic_op(k, &[1], ADD),
                    C::Get(k) => {
                        get(&tx, k).await;
                    }
                    C::SnapshotGet(k) => {
                        within("snapshot get", tx.snapshot().get(k)).await.unwrap();
                    }
                    C::GetKey(sel) => {
                        get_key(&tx, &sel).await;
                    }
                    C::Range(b, e, limit, reverse) => {
                        range_of(&tx, &b, &e, exact(limit).reverse(reverse)).await;
                    }
                    C::RangeIterator(b, e) => {
                        range_of(&tx, &b, &e, RangeOptions::default()).await;
                    }
                    C::SnapshotRange(b, e) => {
                        within(
                            "snapshot range",
                            tx.snapshot().get_range(&b, &e, &RangeOptions::default()),
                        )
                        .await
                        .unwrap();
                    }
                    C::ReadConflict(b, e) => tx.add_read_conflict_range(b, e),
                }
            }
            assert_eq!(commit_reads(&env, &tx).await, expected, "{schema:?} {name}");
        }
    }
}
