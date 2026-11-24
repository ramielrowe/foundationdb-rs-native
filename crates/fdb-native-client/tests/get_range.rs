//! `getRange` against a scripted fake cluster (a coordinator, a GRV proxy, a commit proxy and
//! storage servers on the in-memory network) under tokio's paused clock. The fake storage
//! servers answer `GetKeyValuesRequest`s as `getKeyValuesQ` and `readRange` do (the
//! readable range a server holds, the end selector's shard check, the begin and end selectors
//! resolved with `findKey` and `wrong_shard_server` for an offset they cannot resolve, the
//! row limit, and the byte limit at row granularity with `sizeof(KeyValueRef)` per row), and
//! `GetKeyRequest`s as `findKey`; the commit proxy answers single-key and range location
//! requests (forward and reverse, with a shard limit) from a shard map.
//!
//! Covered: multi-shard forward and reverse reads, row and byte limits, the streaming modes'
//! batch sizes, `more` and the continuation of the range streams, selector ends that need
//! `getKey` (`getRangeFallback`), `wrong_shard_server` with a stale location cache, empty
//! ranges, the transaction's checks and read conflict ranges, a randomized comparison with a
//! `BTreeMap` over random shard maps, and the commit waiting for the reads in progress
//! (`reading`).

#![cfg(feature = "tokio")]

mod support;

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
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
    GetKeyValues, GetKeyValuesReply, GetReadVersion, GrvProxy, KeyRange, KeyServerLocations,
    ReadVersion, StorageServer, TenantMode,
};
use fdb_native_schema::streams::STORAGE_SERVER_STREAMS;
use fdb_native_schema::{Request, SchemaVersion};
use fdb_native_transport::peer::PeerManagerConfig;
use fdb_native_wire::{FdbError, KeyValueRef, KeyValueVector, ProtocolVersion, Uid, VersionVector};
use futures_core::Stream;
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
        }
    }

    fn key_requests(&self) -> usize {
        self.storage.iter().map(FakeStorage::key_requests).sum()
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

    /// A transaction at the fake cluster's read version, **with read-your-writes disabled**:
    /// these tests check the reads C++'s `readThrough` passes to NativeAPI (their requests and
    /// NativeAPI's read conflict ranges); `ryw.rs` tests the read-your-writes reads.
    fn transaction(&self) -> Transaction<MemRuntime> {
        let tx = self.db.create_transaction();
        tx.set_option(TransactionOption::ReadYourWritesDisable)
            .expect("before any read or write");
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

async fn collect<S: Stream + Unpin>(mut stream: S) -> Vec<S::Item> {
    let mut out = Vec::new();
    while let Some(item) = within("stream", poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))).await
    {
        out.push(item);
    }
    out
}

// ---- the reader ------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn multi_shard_reads_forward_and_reverse() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 3).await;
        env.set(&KEYS);
        env.layout(&THREE_SHARDS);
        let all = env
            .read(
                &fge(b""),
                &fge(KEY_SPACE_END),
                RangeLimits::UNLIMITED,
                false,
            )
            .await;
        assert_eq!(keys_of(&all), ks(&KEYS), "{schema}");
        assert!(!all.more());
        assert!(
            !all.read_to_begin(),
            "`firstGreaterOrEqual(\"\")` is not before the beginning"
        );
        // `lastLessThan("")` is (`begin.offset < 1`).
        let all = env
            .read(
                &llt(b""),
                &fge(KEY_SPACE_END),
                RangeLimits::UNLIMITED,
                false,
            )
            .await;
        assert_eq!(all.len(), KEYS.len());
        assert!(
            all.read_to_begin(),
            "a begin before `\"\"` reads to the beginning"
        );
        let rev = env
            .read(&fge(b""), &fge(KEY_SPACE_END), RangeLimits::UNLIMITED, true)
            .await;
        let mut want = ks(&KEYS);
        want.reverse();
        assert_eq!(keys_of(&rev), want, "{schema}");
        assert!(!rev.more());
        // A middle range spanning the three shards.
        let mid = env
            .read(&fge(b"b"), &fge(b"r"), RangeLimits::UNLIMITED, false)
            .await;
        assert_eq!(keys_of(&mid), ks(&[b"f", b"k", b"n", b"q"]));
        let mid = env
            .read(&fge(b"b"), &fge(b"r"), RangeLimits::UNLIMITED, true)
            .await;
        assert_eq!(keys_of(&mid), ks(&[b"q", b"n", b"k", b"f"]));
        // Each server got requests with its shard's bounds as modified selectors.
        let first = env.storage[0].kv_requests();
        assert_eq!(first[0].begin, fge(b"").clone());
        assert_eq!(
            first[0].end,
            fge(b"g"),
            "the end is cut at the shard boundary"
        );
        let task = match schema {
            SchemaVersion::V7_4 => None,
            SchemaVersion::V8_0 => Some(5000),
        };
        assert!(
            env.kv_requests().iter().all(|r| r.task_id == task),
            "{schema}"
        );
        assert!(env.kv_requests().iter().all(|r| r.limit.abs() == 80_000));
    }
}

#[tokio::test(start_paused = true)]
async fn row_and_byte_limits() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&KEYS);
        env.layout(&TWO_SHARDS);
        // Rows: across the boundary, forward and reverse.
        let r = env
            .read(
                &fge(b""),
                &fge(b"z"),
                RangeLimits::new(Some(4), None),
                false,
            )
            .await;
        assert_eq!(keys_of(&r), ks(&[b"a", b"f", b"k", b"n"]));
        assert!(r.more(), "a reached limit says more");
        let r = env
            .read(&fge(b""), &fge(b"z"), RangeLimits::new(Some(4), None), true)
            .await;
        assert_eq!(keys_of(&r), ks(&[b"x", b"q", b"n", b"k"]));
        assert!(r.more());
        // The request's row limit is what is left: 4, then 1 on the second shard (forward);
        // -4, then -1 on the first (reverse).
        let limits = |i: usize| -> Vec<i32> {
            env.storage[i]
                .kv_requests()
                .iter()
                .map(|r| r.limit)
                .collect()
        };
        assert_eq!((limits(0), limits(1)), (vec![4, -1], vec![1, -4]));
        // A row limit of exactly the rows: `more` (C++ cannot tell).
        let r = env
            .read(
                &fge(b""),
                &fge(b"z"),
                RangeLimits::new(Some(6), None),
                false,
            )
            .await;
        assert_eq!(r.len(), 6);
        assert!(r.more());
        // Zero rows: nothing, no request.
        env.clear_requests();
        let r = env
            .read(
                &fge(b""),
                &fge(b"z"),
                RangeLimits::new(Some(0), None),
                false,
            )
            .await;
        assert!(r.is_empty() && !r.more());
        assert!(env.kv_requests().is_empty());

        // Bytes: each row is 1 + 1 + 8 = 10 bytes to the client, 34 to the server.
        // A byte limit returns after the first shard with rows.
        let r = env
            .read(
                &fge(b""),
                &fge(b"z"),
                RangeLimits::new(None, Some(1000)),
                false,
            )
            .await;
        assert_eq!(keys_of(&r), ks(&[b"a", b"f", b"k"]));
        assert!(r.more());
        assert_eq!(
            r.read_through(),
            Some(b"m".as_slice()),
            "stopped at the boundary"
        );
        assert_eq!(r.continuation(false).unwrap(), b"m");
        // 50 bytes: two rows on the server (68 >= 50).
        let r = env
            .read(
                &fge(b""),
                &fge(b"z"),
                RangeLimits::new(None, Some(50)),
                false,
            )
            .await;
        assert_eq!(keys_of(&r), ks(&[b"a", b"f"]));
        assert!(r.more() && r.read_through().is_none());
        assert_eq!(r.continuation(false).unwrap(), b"f\x00");
        // 1 byte: one row.
        let r = env
            .read(&fge(b""), &fge(b"z"), RangeLimits::new(None, Some(1)), true)
            .await;
        assert_eq!(keys_of(&r), ks(&[b"x"]));
        // A byte limit of 0 still reads one row (`minRows`).
        let r = env
            .read(
                &fge(b"b"),
                &fge(b"z"),
                RangeLimits::new(None, Some(0)),
                false,
            )
            .await;
        assert_eq!(keys_of(&r), ks(&[b"f"]));
        assert!(r.more());
        // An empty first shard: the read goes on to the next.
        let r = env
            .read(
                &fge(b"l"),
                &fge(b"z"),
                RangeLimits::new(None, Some(1)),
                false,
            )
            .await;
        assert_eq!(keys_of(&r), ks(&[b"n"]));
    }
}

#[tokio::test(start_paused = true)]
async fn streaming_modes_set_the_batch_sizes() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        // 2000 rows of 4-byte keys and 100-byte values: 136 bytes each on the server.
        for i in 0..2000u32 {
            let k = format!("{:04}", i);
            env.set_value(k.as_bytes(), &[b'v'; 100]);
        }
        let row = (SERVER_ROW_OVERHEAD + 4 + 100) as u32;
        let rows_for = |bytes: u32| bytes.div_ceil(row) as usize;
        let tx = env.transaction();
        let read = |mode: StreamingMode, iteration: u32| {
            let tx = tx.clone();
            async move {
                let o = RangeOptions::default().mode(mode).iteration(iteration);
                within("get_range", tx.get_range(&fge(b""), &fge(b"\xff"), &o))
                    .await
                    .unwrap()
            }
        };
        assert_eq!(read(StreamingMode::Small, 1).await.kvs.len(), rows_for(256));
        assert_eq!(
            read(StreamingMode::Medium, 1).await.kvs.len(),
            rows_for(1000)
        );
        assert_eq!(
            read(StreamingMode::Large, 1).await.kvs.len(),
            rows_for(4096)
        );
        // SERIAL / WANT_ALL: 80 000 (7.4) or 120 000 (8.0) bytes, but one request asks for
        // `REPLY_BYTE_LIMIT` (80 000) at most and the soft byte limit ends the batch.
        let serial = read(StreamingMode::Serial, 1).await;
        assert_eq!(serial.kvs.len(), rows_for(80_000));
        assert!(serial.more);
        assert_eq!(
            read(StreamingMode::WantAll, 1).await.kvs.len(),
            rows_for(80_000)
        );
        assert_eq!(env.kv_requests().last().unwrap().limit_bytes, 80_000);
        for (iteration, bytes) in [(1, 4096), (2, 6144), (3, 9216), (9, 80_000), (25, 80_000)] {
            let r = read(StreamingMode::Iterator, iteration).await;
            assert_eq!(
                r.kvs.len(),
                rows_for(bytes),
                "{schema} iteration {iteration}"
            );
        }
        // EXACT: the row limit, no byte target.
        let o = RangeOptions::with_limit(650).mode(StreamingMode::Exact);
        let r = within("exact", tx.get_range(&fge(b""), &fge(b"\xff"), &o))
            .await
            .unwrap();
        // One request returns at most `REPLY_BYTE_LIMIT` bytes; EXACT goes on until the
        // limit (no byte limit: no early return).
        assert_eq!(r.kvs.len(), 650);
        assert!(r.more);
        let reqs = env.kv_requests();
        let exact: Vec<(i32, i32)> = reqs[reqs.len() - 2..]
            .iter()
            .map(|r| (r.limit, r.limit_bytes))
            .collect();
        assert_eq!(
            exact,
            vec![(650, 80_000), (650 - rows_for(80_000) as i32, 80_000)]
        );
        // A target below the mode's wins.
        let o = RangeOptions::default()
            .mode(StreamingMode::Large)
            .target_bytes(300);
        let r = within("target", tx.get_range(&fge(b""), &fge(b"\xff"), &o))
            .await
            .unwrap();
        assert_eq!(r.kvs.len(), rows_for(300));
        // Errors of `validate_and_update_parameters`.
        let o = RangeOptions::default().mode(StreamingMode::Exact);
        let e = tx
            .get_range(&fge(b""), &fge(b"\xff"), &o)
            .await
            .unwrap_err();
        assert_eq!(e.code(), codes::EXACT_MODE_WITHOUT_LIMITS);
        let o = RangeOptions::default().iteration(0);
        let e = tx
            .get_range(&fge(b""), &fge(b"\xff"), &o)
            .await
            .unwrap_err();
        assert_eq!(e.code(), codes::CLIENT_INVALID_OPERATION);

        // The iterator's progression through a stream: the request byte limits.
        env.clear_requests();
        let batches = collect(tx.get_ranges(fge(b""), fge(b"\xff"), RangeOptions::default())).await;
        let sizes: Vec<usize> = batches
            .iter()
            .map(|b| b.as_ref().unwrap().kvs.len())
            .collect();
        assert_eq!(sizes.iter().sum::<usize>(), 2000);
        let progression: Vec<i32> = env.kv_requests().iter().map(|r| r.limit_bytes).collect();
        assert_eq!(
            progression[..9],
            [4096, 6144, 9216, 13824, 20736, 31104, 46656, 69984, 80000],
            "{schema}"
        );
        assert_eq!(sizes[0], rows_for(4096));
        assert_eq!(sizes[1], rows_for(6144));
    }
}

#[tokio::test(start_paused = true)]
async fn streams_continue_after_more_in_both_directions() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 3).await;
        let all: Vec<Vec<u8>> = (0..60u8)
            .map(|i| vec![b'a' + i / 10, b'0' + i % 10])
            .collect();
        for k in &all {
            env.set_value(k, b"value");
        }
        env.layout(&[(b"", b"b5", 0), (b"b5", b"d", 1), (b"d", KEY_SPACE_END, 2)]);
        let tx = env.transaction();
        for reverse in [false, true] {
            let o = RangeOptions::default()
                .mode(StreamingMode::Small)
                .reverse(reverse);
            let rows: Vec<KeyValue> = collect(tx.range_stream(fge(b""), fge(b"\xff"), o))
                .await
                .into_iter()
                .map(Result::unwrap)
                .collect();
            let mut want = all.clone();
            if reverse {
                want.reverse();
            }
            assert_eq!(keys(&rows), want, "{schema} reverse {reverse}");
            // A row limit across batches.
            let o = RangeOptions::with_limit(25)
                .mode(StreamingMode::Small)
                .reverse(reverse);
            let batches: Vec<_> = collect(tx.get_ranges(fge(b""), fge(b"\xff"), o))
                .await
                .into_iter()
                .map(Result::unwrap)
                .collect();
            let rows: Vec<KeyValue> = batches.iter().flat_map(|b| b.kvs.clone()).collect();
            assert_eq!(keys(&rows), want[..25].to_vec());
            assert!(batches.len() > 3, "{}", batches.len());
        }
        // Snapshot streams add no conflict range.
        let rows = collect(tx.snapshot().range_stream(
            fge(b"c"),
            fge(b"d"),
            RangeOptions::default(),
        ))
        .await;
        assert_eq!(rows.len(), 10);
    }
}

#[tokio::test(start_paused = true)]
async fn selector_ends_that_need_get_key_fall_back() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&KEYS);
        env.layout(&TWO_SHARDS);
        let data = env.snapshot();
        let cases = [
            // An end selector resolved in the other shard: `wrong_shard_server`, then the
            // fallback resolves both ends with `getKey`.
            (fge(b"b"), llt(b"q")),
            (fge(b"b"), lle(b"q")),
            // A begin offset the server cannot resolve.
            (plus(fge(b"a"), 3), fge(b"z")),
            (plus(fge(b"a"), 3), plus(fge(b"a"), 5)),
            (plus(llt(b"q"), -2), plus(lle(b"q"), 1)),
            // Ends beyond the database.
            (plus(llt(b"b"), -5), plus(fge(b"x"), 5)),
        ];
        for (b, e) in cases {
            for reverse in [false, true] {
                env.clear_requests();
                let r = env.read(&b, &e, RangeLimits::UNLIMITED, reverse).await;
                let want = model(&data, &b, &e, reverse);
                assert_eq!(
                    keys_of(&r),
                    keys(&want),
                    "{schema} {b:?}..{e:?} reverse {reverse}"
                );
                assert!(!r.more());
            }
        }
        // `lastLessThan(q)` as the end: the fallback asked `getKey`.
        env.clear_requests();
        env.read(&fge(b"b"), &llt(b"q"), RangeLimits::UNLIMITED, false)
            .await;
        assert!(env.key_requests() > 0);
        // The fallback's flags: an end beyond the last key reads through the end.
        let r = env
            .read(
                &fge(b"b"),
                &plus(fge(b"x"), 5),
                RangeLimits::UNLIMITED,
                false,
            )
            .await;
        assert!(r.read_through_end());
        let r = env
            .read(
                &plus(llt(b"b"), -3),
                &fge(b"g"),
                RangeLimits::UNLIMITED,
                false,
            )
            .await;
        assert!(r.read_to_begin());
        // The fallback with limits: `getExactRange` keeps them.
        let r = env
            .read(
                &plus(fge(b"a"), 1),
                &llt(b"x"),
                RangeLimits::new(Some(3), None),
                true,
            )
            .await;
        assert_eq!(keys_of(&r), ks(&[b"n", b"k", b"f"]));
        assert!(r.more());
    }
}

#[tokio::test(start_paused = true)]
async fn wrong_shard_server_invalidates_and_reads_again() {
    for schema in SCHEMAS {
        for reverse in [false, true] {
            let env = Env::new(schema, 2).await;
            env.set(&KEYS);
            // Cache the whole key space on server 0, then split it.
            let r = env
                .read(&fge(b""), &fge(b"z"), RangeLimits::UNLIMITED, false)
                .await;
            assert_eq!(r.len(), 6);
            env.layout(&TWO_SHARDS);
            let before = env.locations.lock().unwrap().len();
            let r = env
                .read(&fge(b"b"), &fge(b"r"), RangeLimits::UNLIMITED, reverse)
                .await;
            let mut want = ks(&[b"f", b"k", b"n", b"q"]);
            if reverse {
                want.reverse();
            }
            assert_eq!(keys_of(&r), want, "{schema} reverse {reverse}");
            assert!(
                env.locations.lock().unwrap().len() > before,
                "located again"
            );
            assert!(!env.storage[1].kv_requests().is_empty());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn empty_ranges() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&KEYS);
        env.layout(&TWO_SHARDS);
        env.clear_requests();
        for (b, e) in [
            (fge(b"k"), fge(b"k")),
            (fge(b"q"), fge(b"b")),
            (fgt(b"k"), fge(b"k")),
            (fge(b"b"), fge(b"")),
            (fge(b"a"), llt(b"")),
        ] {
            for reverse in [false, true] {
                let r = env.read(&b, &e, RangeLimits::UNLIMITED, reverse).await;
                assert!(r.is_empty() && !r.more(), "{b:?}..{e:?}");
            }
        }
        assert!(env.kv_requests().is_empty(), "no request for those");
        // A range with no keys, within and across shards.
        for (b, e) in [
            (fge(b"g"), fge(b"j")),
            (fge(b"l"), fge(b"m\x01")),
            (fgt(b"x"), fge(b"z")),
        ] {
            for reverse in [false, true] {
                let r = env.read(&b, &e, RangeLimits::UNLIMITED, reverse).await;
                assert!(r.is_empty() && !r.more(), "{b:?}..{e:?}");
            }
        }
        // The reader's checks.
        let reader = env.db.reader();
        let e = reader
            .get_range_with_limits(
                &fge(b"a"),
                &fge(b"b"),
                RangeLimits::UNLIMITED,
                false,
                0,
                false,
            )
            .await
            .unwrap_err();
        assert_eq!(e.code(), codes::CLIENT_INVALID_OPERATION);
        let e = reader
            .get_range_with_limits(
                &fge(b"a"),
                &fge(b"\xff\xff\x00"),
                RangeLimits::UNLIMITED,
                false,
                READ_VERSION,
                false,
            )
            .await
            .unwrap_err();
        assert_eq!(e.code(), codes::KEY_OUTSIDE_LEGAL_RANGE);
        // A snapshot reads with the cluster's streaming-mode targets.
        let snap = reader.snapshot_at(READ_VERSION);
        let r = snap
            .get_range(&fge(b""), &fge(b"z"), &RangeOptions::default())
            .await
            .unwrap();
        assert_eq!(r.len(), 3, "the first shard (a byte target ends the batch)");
    }
}

// ---- the transaction -------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn transaction_read_conflict_ranges_follow_cpp() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&KEYS);
        env.layout(&TWO_SHARDS);
        let tx = env.transaction();
        let exact = |n| RangeOptions::with_limit(n).mode(StreamingMode::Exact);
        let cases: Vec<Case<'_>> = vec![
            // Everything: the selectors' keys.
            (
                fge(b"b"),
                fge(b"p"),
                exact(10),
                vec![b"f", b"k", b"n"],
                Some(range(b"b", b"p")),
            ),
            // WANT_ALL has a byte target: the batch ends with the first shard (`more`), and
            // the range ends after its last row.
            (
                fge(b"b"),
                fge(b"p"),
                RangeOptions::default().mode(StreamingMode::WantAll),
                vec![b"f", b"k"],
                Some(range(b"b", b"k\x00")),
            ),
            // A row limit: up to after the last row.
            (
                fge(b"b"),
                fge(b"z"),
                exact(2),
                vec![b"f", b"k"],
                Some(range(b"b", b"k\x00")),
            ),
            // Reverse with a limit: from the last row read.
            (
                fge(b"b"),
                fge(b"z"),
                exact(2).reverse(true),
                vec![b"x", b"q"],
                Some(range(b"q", b"z")),
            ),
            // `orEqual` begin: from the key after.
            (
                fgt(b"f"),
                fge(b"m"),
                exact(10),
                vec![b"k"],
                Some(range(b"f\x00", b"m")),
            ),
            // A begin offset of 0: from the first row read.
            (
                llt(b"b"),
                fge(b"g"),
                exact(10),
                vec![b"a", b"f"],
                Some(range(b"a", b"g")),
            ),
            // An end past the last key: the fallback reads through the end (`\xff\xff`).
            (
                fge(b"r"),
                plus(fge(b"x"), 3),
                exact(10),
                vec![b"x"],
                Some(range(b"r", KEY_SPACE_END)),
            ),
            // A byte target that stops at the shard boundary: `more`, up to after the last
            // row.
            (
                fge(b"b"),
                fge(b"z"),
                RangeOptions::default().target_bytes(1),
                vec![b"f"],
                Some(range(b"b", b"f\x00")),
            ),
        ];
        let mut expected = Vec::new();
        for (b, e, o, want, conflict) in cases {
            let r = within("get_range", tx.get_range(&b, &e, &o)).await.unwrap();
            assert_eq!(keys(&r.kvs), ks(&want), "{schema} {b:?}..{e:?} {o:?}");
            expected.extend(conflict);
        }
        // An inverted range: nothing, no conflict range.
        let r = tx
            .get_range(&fge(b"q"), &fge(b"b"), &exact(5))
            .await
            .unwrap();
        assert!(r.kvs.is_empty() && !r.more);
        // A reverse read whose end offset is above 1 resolves the end with `getKey` (and
        // its conflict range) first.
        let r = within(
            "reverse",
            tx.get_range(&fge(b"b"), &plus(fge(b"k"), 1), &exact(10).reverse(true)),
        )
        .await
        .unwrap();
        assert_eq!(keys(&r.kvs), ks(&[b"k", b"f"]));
        expected.push(range(b"k", b"n\x00")); // getKey(k + 2) = n: `[k, keyAfter(n))`
        expected.push(range(b"b", b"n")); // the range read: `[b, n)`
        // Snapshot reads add nothing.
        tx.snapshot()
            .get_range(&fge(b"a"), &fge(b"z"), &RangeOptions::default())
            .await
            .unwrap();
        tx.set(b"w", b"1");
        within("commit", tx.commit()).await.unwrap();
        expected.sort_by(|a, b| (&a.begin, &a.end).cmp(&(&b.begin, &b.end)));
        assert_eq!(env.sent_read_conflicts(0), expected, "{schema}");
    }
}

#[tokio::test(start_paused = true)]
async fn transaction_checks_and_system_keys() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        env.set(&KEYS);
        env.set(&[b"\xff/a", b"\xff/b"]);
        let tx = env.transaction();
        let o = RangeOptions::default();
        let e = tx
            .get_range(&fge(b"a"), &fge(b"\xff\x00"), &o)
            .await
            .unwrap_err();
        assert_eq!(e.code(), codes::KEY_OUTSIDE_LEGAL_RANGE);
        // `\xff` itself is allowed; a forward read that goes past it drops those rows and
        // says no more.
        let r = tx
            .get_range(
                &fge(b"q"),
                &plus(fge(b"\xff"), 2),
                &RangeOptions::with_limit(10),
            )
            .await
            .unwrap();
        assert_eq!(keys(&r.kvs), ks(&[b"q", b"x"]));
        assert!(!r.more);
        // A reverse read with such an end: the end is resolved, clamped to `\xff`.
        let r = tx
            .get_range(
                &fge(b"q"),
                &plus(fge(b"\xff"), 2),
                &RangeOptions::with_limit(10).reverse(true),
            )
            .await
            .unwrap();
        assert_eq!(keys(&r.kvs), ks(&[b"x", b"q"]));
        // With the system keys option.
        let tx = env.transaction();
        tx.set_option(TransactionOption::ReadSystemKeys).unwrap();
        let r = tx
            .get_range(&fge(b"x"), &fge(KEY_SPACE_END), &RangeOptions::default())
            .await
            .unwrap();
        assert_eq!(keys(&r.kvs), ks(&[b"x", b"\xff/a", b"\xff/b"]));
        let e = tx
            .get_range(&fge(b"a"), &fge(b"\xff\xff\x00"), &o)
            .await
            .unwrap_err();
        assert_eq!(e.code(), codes::KEY_OUTSIDE_LEGAL_RANGE);
        // The transaction's rules: a deferred error first, used during commit, cancelled.
        let tx = env.transaction();
        tx.set(b"\xff/nope", b"");
        assert_eq!(
            tx.get_range(&fge(b"a"), &fge(b"b"), &o)
                .await
                .unwrap_err()
                .code(),
            codes::KEY_OUTSIDE_LEGAL_RANGE
        );
        let tx = env.transaction();
        tx.set(b"a", b"1");
        within("commit", tx.commit()).await.unwrap();
        assert_eq!(
            tx.get_range(&fge(b"a"), &fge(b"b"), &o)
                .await
                .unwrap_err()
                .code(),
            codes::USED_DURING_COMMIT
        );
        let tx = env.transaction();
        tx.cancel();
        assert_eq!(
            tx.get_range(&fge(b"a"), &fge(b"b"), &o)
                .await
                .unwrap_err()
                .code(),
            codes::TRANSACTION_CANCELLED
        );
        // Without a read version, the first read asks for one.
        let tx = env.db.create_transaction();
        tx.set_option(TransactionOption::ReadYourWritesDisable)
            .unwrap();
        let r = tx.get_range(&fge(b"a"), &fge(b"g"), &o).await.unwrap();
        assert_eq!(keys(&r.kvs), ks(&[b"a", b"f"]));
    }
}

#[tokio::test(start_paused = true)]
async fn random_ranges_match_a_btreemap_over_random_shard_maps() {
    let mut seed: u64 = 0x5eed_1234;
    let mut next = move |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    let mut fallbacks = 0;
    let mut cut = 0;
    for schema in SCHEMAS {
        for round in 0..10 {
            let env = Env::new(schema, 3).await;
            let alphabet: &[u8] = b"bdfhjlnprtvx";
            for _ in 0..30 {
                let len = 1 + next(2) as usize;
                let k: Vec<u8> = (0..len)
                    .map(|_| alphabet[next(alphabet.len() as u64) as usize])
                    .collect();
                let v = vec![b'v'; next(40) as usize];
                env.set_value(&k, &v);
            }
            let mut bounds: Vec<Vec<u8>> = (0..next(5))
                .map(|_| vec![alphabet[next(alphabet.len() as u64) as usize], b'm'])
                .collect();
            bounds.sort();
            bounds.dedup();
            let mut edges: Vec<Vec<u8>> = vec![Vec::new()];
            edges.extend(bounds);
            edges.push(KEY_SPACE_END.to_vec());
            let layout: Vec<(Vec<u8>, Vec<u8>, usize)> = edges
                .windows(2)
                .map(|w| (w[0].clone(), w[1].clone(), next(3) as usize))
                .collect();
            let layout_refs: Vec<(&[u8], &[u8], usize)> = layout
                .iter()
                .map(|(b, e, o)| (b.as_slice(), e.as_slice(), *o))
                .collect();
            env.layout(&layout_refs);
            env.row_limit(match round % 3 {
                0 => None,
                1 => Some(1),
                _ => Some(3),
            });
            let data = env.snapshot();
            let all: Vec<Vec<u8>> = data.keys().cloned().collect();
            let tx = env.transaction();
            for _ in 0..40 {
                let selector = |next: &mut dyn FnMut(u64) -> u64| {
                    let key: Vec<u8> = match next(8) {
                        0 => Vec::new(),
                        1 => b"\xff".to_vec(),
                        2 => all[next(all.len() as u64) as usize].clone(),
                        _ => (0..1 + next(2))
                            .map(|_| alphabet[next(alphabet.len() as u64) as usize] - next(2) as u8)
                            .collect(),
                    };
                    let offset = match next(10) {
                        0 => 30,
                        1 => -30,
                        _ => next(9) as i32 - 3,
                    };
                    KeySelector::new(key, next(2) == 1, offset)
                };
                let b = selector(&mut next);
                let e = selector(&mut next);
                let reverse = next(2) == 1;
                let rows = match next(3) {
                    0 => None,
                    _ => Some(1 + next(12) as u32),
                };
                let bytes = match next(3) {
                    0 => Some(next(300) as u32),
                    _ => None,
                };
                let want = model(&data, &b, &e, reverse);
                let before = env.key_requests();
                let r = env
                    .read(&b, &e, RangeLimits::new(rows, bytes), reverse)
                    .await;
                if env.key_requests() > before {
                    fallbacks += 1;
                }
                let got = keys_of(&r);
                let what = format!(
                    "{schema} round {round} {b:?}..{e:?} reverse {reverse} rows {rows:?} bytes {bytes:?} layout {layout:?}"
                );
                // A prefix of the rows, in order.
                assert!(got.len() <= want.len(), "{what}: {got:?} vs {want:?}");
                assert_eq!(got, keys(&want[..got.len()]), "{what}");
                if let Some(n) = rows {
                    assert!(got.len() <= n as usize, "{what}");
                }
                if !r.more() {
                    assert_eq!(got.len(), want.len(), "{what}: not more, but rows missing");
                } else {
                    cut += 1;
                }
                if !want.is_empty() {
                    assert!(!got.is_empty() || r.more(), "{what}");
                }
                if bytes.is_none() {
                    // Without a byte limit, only the row limit stops it.
                    let n = rows.map_or(want.len(), |n| (n as usize).min(want.len()));
                    assert_eq!(got.len(), n, "{what}");
                }
                // The whole range through a stream (the iterator mode, a row limit).
                let o = RangeOptions::with_limit(rows.unwrap_or(0)).reverse(reverse);
                let streamed: Vec<KeyValue> =
                    collect(tx.snapshot().range_stream(b.clone(), e.clone(), o))
                        .await
                        .into_iter()
                        .map(|r| r.unwrap_or_else(|e| panic!("{what}: {e}")))
                        .collect();
                let n = rows.map_or(want.len(), |n| (n as usize).min(want.len()));
                // The transaction never returns system keys (none here but `\xff`-bounded
                // selectors are clamped the same way).
                assert_eq!(streamed, want[..n].to_vec(), "{what} (stream)");
            }
        }
    }
    assert!(fallbacks > 50, "{fallbacks} reads fell back to getKey");
    assert!(cut > 50, "{cut} reads were cut by a limit");
}

// ---- the commit and the reads in progress -------------------------------------------------

#[tokio::test(start_paused = true)]
async fn get_key_racing_commit_is_in_the_commit() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        env.set(&KEYS);
        env.delay(Duration::from_millis(100));
        let tx = env.transaction();
        tx.set(b"w", b"1");
        let sel = fgt(b"f");
        let (k, c) = within("race", async {
            tokio::join!(tx.get_key(&sel), async {
                tokio::task::yield_now().await;
                // The read is in progress: the commit waits for it.
                let commit = tx.commit();
                let mut commit = std::pin::pin!(commit);
                let early = tokio::time::timeout(Duration::from_millis(50), &mut commit).await;
                assert!(early.is_err(), "the commit waited for the read");
                assert!(env.commits.lock().unwrap().is_empty());
                commit.await
            })
        })
        .await;
        assert_eq!(k.unwrap(), b"k");
        c.unwrap();
        assert_eq!(
            env.sent_read_conflicts(0),
            vec![range(b"f\x00", b"k\x00")],
            "{schema}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn get_range_racing_commit_is_in_the_commit() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&KEYS);
        env.layout(&TWO_SHARDS);
        env.delay(Duration::from_millis(100));
        let tx = env.transaction();
        tx.set(b"w", b"1");
        let o = RangeOptions::with_limit(2).mode(StreamingMode::Exact);
        let (sel_b, sel_z) = (fge(b"b"), fge(b"z"));
        let (r, c) = within("race", async {
            tokio::join!(tx.get_range(&sel_b, &sel_z, &o), async {
                tokio::task::yield_now().await;
                tx.commit().await
            })
        })
        .await;
        assert_eq!(keys(&r.unwrap().kvs), ks(&[b"f", b"k"]));
        c.unwrap();
        assert_eq!(
            env.sent_read_conflicts(0),
            vec![range(b"b", b"k\x00")],
            "{schema}"
        );
        // A read started after the commit is `used_during_commit`.
        let tx = env.transaction();
        tx.set(b"w", b"1");
        let (r, c) = within("after", async {
            tokio::join!(
                async {
                    tokio::task::yield_now().await;
                    tx.get_range(&sel_b, &sel_z, &o).await
                },
                tx.commit()
            )
        })
        .await;
        assert_eq!(r.unwrap_err().code(), codes::USED_DURING_COMMIT);
        // The commit may or may not have been sent by then; it is not waiting for that read.
        let _ = c;
    }
}

#[tokio::test(start_paused = true)]
async fn a_failed_read_does_not_block_the_commit() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        env.set(&KEYS);
        env.delay(Duration::from_millis(100));
        env.fail_with(Some(codes::TRANSACTION_TOO_OLD));
        let tx = env.transaction();
        tx.set(b"w", b"1");
        let o = RangeOptions::with_limit(2).mode(StreamingMode::Exact);
        let (sel_b, sel_z, sel_a) = (fge(b"b"), fge(b"z"), fgt(b"a"));
        let (r, k, c) = within("race", async {
            tokio::join!(
                tx.get_range(&sel_b, &sel_z, &o),
                tx.get_key(&sel_a),
                async {
                    tokio::task::yield_now().await;
                    tx.commit().await
                }
            )
        })
        .await;
        assert_eq!(r.unwrap_err().code(), codes::TRANSACTION_TOO_OLD);
        assert_eq!(k.unwrap_err().code(), codes::TRANSACTION_TOO_OLD);
        c.unwrap();
        assert_eq!(
            env.sent_read_conflicts(0),
            vec![],
            "{schema}: no range of a failed read"
        );
        // A read abandoned (its future dropped) still ends, as a C++ read actor does: the
        // commit waits for it, and carries its range.
        env.fail_with(None);
        env.delay(Duration::from_secs(5));
        let tx = env.transaction();
        tx.set(b"w", b"1");
        let abandoned =
            tokio::time::timeout(Duration::from_millis(10), tx.get_range(&sel_b, &sel_z, &o)).await;
        assert!(abandoned.is_err());
        within("commit", tx.commit()).await.unwrap();
        assert_eq!(
            env.sent_read_conflicts(1),
            vec![range(b"b", b"k\x00")],
            "{schema}: the abandoned read's range"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_reset_during_a_read_keeps_it_out_of_the_next_attempt() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        env.set(&KEYS);
        env.delay(Duration::from_millis(100));
        let tx = env.transaction();
        let o = RangeOptions::with_limit(2).mode(StreamingMode::Exact);
        let (sel_b, sel_z, sel_a) = (fge(b"b"), fge(b"z"), fgt(b"a"));
        let (r, k, ()) = within("reset", async {
            tokio::join!(
                tx.get_range(&sel_b, &sel_z, &o),
                tx.get_key(&sel_a),
                async {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    tx.reset();
                }
            )
        })
        .await;
        assert_eq!(r.unwrap_err().code(), codes::TRANSACTION_CANCELLED);
        assert_eq!(k.unwrap_err().code(), codes::TRANSACTION_CANCELLED);
        // The next attempt: neither read is in it, and its commit does not wait for them.
        tx.set_read_version(READ_VERSION);
        tx.set(b"w", b"1");
        within("commit", tx.commit()).await.unwrap();
        assert_eq!(env.sent_read_conflicts(0), vec![], "{schema}");
        // The same through `on_error`.
        let tx = env.transaction();
        let (r, retry) = within("on_error", async {
            tokio::join!(tx.get_range(&sel_b, &sel_z, &o), async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                tx.on_error(fdb_native_client::Error::new(codes::NOT_COMMITTED))
                    .await
            })
        })
        .await;
        retry.unwrap();
        assert_eq!(r.unwrap_err().code(), codes::TRANSACTION_CANCELLED);
    }
}

#[tokio::test(start_paused = true)]
async fn read_conflict_ranges_of_reads_do_not_count_toward_the_size() {
    let env = Env::new(SchemaVersion::V8_0, 1).await;
    env.set(&KEYS);
    let tx = env.transaction();
    let long = vec![b'k'; 9_000];
    // A get_key range of ~9 KB and a get_range range of ~9 KB, a write of a few bytes.
    tx.get_key(&fge(&long)).await.unwrap();
    tx.get_range(&fge(b"a"), &fge(&long), &RangeOptions::default())
        .await
        .unwrap();
    tx.set(b"w", b"1");
    tx.set_option(TransactionOption::SizeLimit(1000)).unwrap();
    within("commit", tx.commit()).await.unwrap();
    assert_eq!(env.sent_read_conflicts(0).len(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_forward_read_through_the_end_of_the_key_space_conflicts_up_to_it() {
    // (9q8.3 review) The begin selector resolves at `\xff\xff` (the last shard's rows are
    // read, the end is two keys past the end of the database): the read went through the end,
    // so its conflict range ends at `\xff\xff`, not after its last row.
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&[b"a", b"\xff/a", b"\xff/b"]);
        env.layout(&[(b"", b"\xff", 0), (b"\xff", KEY_SPACE_END, 1)]);
        let tx = env.transaction();
        tx.set_option(TransactionOption::ReadSystemKeys).unwrap();
        let exact = RangeOptions::with_limit(100).mode(StreamingMode::Exact);
        let r = within(
            "get_range",
            tx.get_range(&fge(b"\xff/"), &plus(fge(KEY_SPACE_END), 1), &exact),
        )
        .await
        .unwrap();
        assert_eq!(keys(&r.kvs), vec![b"\xff/a".to_vec(), b"\xff/b".to_vec()]);
        assert!(!r.more);
        tx.set(b"w", b"1");
        within("commit", tx.commit()).await.unwrap();
        assert_eq!(
            env.sent_read_conflicts(0),
            vec![range(b"\xff/", KEY_SPACE_END)],
            "{schema}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_read_future_no_longer_polled_does_not_hold_the_commit() {
    // (9q8.3 review) Non-snapshot reads polled once, then kept but never polled again (the
    // losers of a `select`): they run on as tasks of their own, so the commit, which waits
    // for them, goes on once they end, with their ranges.
    let env = Env::new(SchemaVersion::V8_0, 2).await;
    env.set(&KEYS);
    env.layout(&TWO_SHARDS);
    env.delay(Duration::from_millis(50));
    let tx = env.transaction();
    let (b, e, o) = (fge(b"a"), fge(b"z"), RangeOptions::default());
    let read = tx.get_range(&b, &e, &o);
    let mut read = std::pin::pin!(read);
    let selector = fge(b"g");
    let key = tx.get_key(&selector);
    let mut key = std::pin::pin!(key);
    let polled = poll_fn(|cx| {
        let read = read.as_mut().poll(cx).is_pending();
        let key = key.as_mut().poll(cx).is_pending();
        std::task::Poll::Ready(read && key)
    })
    .await;
    assert!(polled, "the reads should be pending");
    tx.set(b"w", b"1");
    within("commit", tx.commit()).await.unwrap();
    assert_eq!(
        env.sent_read_conflicts(0),
        vec![range(b"a", b"k\x00"), range(b"g", b"k\x00")]
    );
    // The futures still give their results (the first shard's rows: a shard boundary).
    assert_eq!(
        keys(&read.await.unwrap().kvs),
        keys_list(&[b"a", b"f", b"k"])
    );
    assert_eq!(key.await.unwrap(), b"k");

    // A range stream inside a `select` with a timer that wins.
    let tx = env.transaction();
    let mut rows = tx.range_stream(fge(b"a"), fge(b"z"), RangeOptions::default());
    let next = poll_fn(|cx| Pin::new(&mut rows).poll_next(cx));
    let timer = tokio::time::sleep(Duration::from_millis(1));
    tokio::select! {
        biased;
        _ = timer => {}
        _ = next => panic!("the read should be slower than the timer"),
    }
    tx.set(b"w", b"1");
    within("commit after a select", tx.commit()).await.unwrap();
    assert_eq!(env.sent_read_conflicts(1), vec![range(b"a", b"k\x00")]);
}

fn keys_list(keys: &[&[u8]]) -> Vec<Vec<u8>> {
    keys.iter().map(|k| k.to_vec()).collect()
}
