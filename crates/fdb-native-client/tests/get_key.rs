//! `getKey` against a scripted fake cluster (a coordinator, a GRV proxy, a commit proxy and
//! storage servers on the in-memory network) under tokio's paused clock. The fake storage
//! servers resolve selectors as `findKey` does (within the contiguous range they own, with
//! an optional row limit standing in for `STORAGE_LIMIT_BYTES`), and the commit proxy answers
//! forward and reverse location requests from a shard map. Covered: a single shard, selectors
//! crossing shard boundaries forward and backward, large offsets, the beginning and end of the
//! database, `wrong_shard_server` with a stale location cache (both directions), the reader's
//! argument checks, and the transaction's clamping, key checks and read conflict ranges.

#![cfg(feature = "tokio")]

mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_client::error::codes;
use fdb_native_client::read::KEY_SPACE_END;
use fdb_native_client::{
    ConnectionRecord, Database, DatabaseOptions, KeySelector, Transaction, TransactionOption,
};
use fdb_native_schema::domain::{
    ClientDbInfo, Commit, CommitId, CommitProxy, GetKey, GetKeyReply, GetKeyServerLocations,
    GetReadVersion, GrvProxy, KeyRange, KeyServerLocations, ReadVersion, StorageServer, TenantMode,
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

fn key_after(key: &[u8]) -> Vec<u8> {
    let mut k = key.to_vec();
    k.push(0);
    k
}

type Data = Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>;
/// Shards and the indices of their storage servers.
type ShardMap = Vec<(KeyRange, Vec<usize>)>;

// ---- what a storage server answers ---------------------------------------------------------

/// What one fake storage server does.
#[derive(Default)]
struct StorageState {
    /// The readable ranges it holds, coalesced (C++'s `getShardKeyRange` extends a shard over
    /// its readable neighbours).
    owns: Vec<KeyRange>,
    /// The most rows a scan returns for a selector whose offset is not 0 or 1
    /// (`STORAGE_LIMIT_BYTES` in rows); `None` is unlimited.
    row_limit: Option<usize>,
    /// The selectors received.
    requests: Vec<KeySelector>,
}

fn is_backward(sel: &KeySelector) -> bool {
    !sel.or_equal && sel.offset <= 0
}

/// `getShardKeyRange`: the owned range the selector starts in (for a backward selector, the
/// one containing the keys just before its key); `None` is `wrong_shard_server`.
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

/// `readRange`: up to `wanted` keys of `range` (descending if `reverse`), at most `limit`;
/// and whether the range has more keys than returned.
fn read_range(
    data: &BTreeMap<Vec<u8>, Vec<u8>>,
    range: &KeyRange,
    reverse: bool,
    wanted: usize,
    limit: Option<usize>,
) -> (Vec<Vec<u8>>, bool) {
    let keys: Vec<Vec<u8>> = data
        .range(range.begin.clone()..range.end.clone())
        .map(|(k, _)| k.clone())
        .collect();
    let mut keys = keys;
    if reverse {
        keys.reverse();
    }
    let take = wanted.min(limit.unwrap_or(usize::MAX));
    let more = keys.len() > take;
    keys.truncate(take);
    (keys, more)
}

/// `findKey` and `getKeyQ`'s reply (`storageserver.actor.cpp` 7.4:4526-4626, 7569-7666).
fn find_key(
    data: &BTreeMap<Vec<u8>, Vec<u8>>,
    sel: &KeySelector,
    range: &KeyRange,
    row_limit: Option<usize>,
) -> KeySelector {
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
    let scan = if forward {
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
    let (mut rows, rep_more) = read_range(data, &scan, !forward, wanted, limit);
    let mut more = rep_more && rows.len() != wanted;
    if more && !forward && rows.len() == 1 {
        let (rows2, rep_more2) = read_range(data, &scan, true, 2, None);
        rows = rows2;
        more = rep_more2 && rows.len() != wanted;
    }
    let mut index = distance - 1;
    if skip_equal && rows.first() == Some(&sel.key) {
        index += 1;
    }
    let (key, offset) = if (index as usize) < rows.len() {
        (rows[index as usize].clone(), 0i64)
    } else {
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
    };
    match offset {
        0 => KeySelector::new(key, true, 0),
        o if o < 0 => KeySelector::new(key, false, (1 + o) as i32),
        o => KeySelector::new(key, false, o as i32),
    }
}

/// What a selector resolves to, by definition: the `offset`-th key after the last key less
/// than (or equal to) its key, `""` before the first key and `\xff\xff` after the last.
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

// ---- the fake cluster ----------------------------------------------------------------------

struct FakeStorage {
    _process: FakeProcess,
    server: StorageServer,
    state: Arc<Mutex<StorageState>>,
}

impl FakeStorage {
    fn served(&self) -> usize {
        self.state.lock().unwrap().requests.len()
    }

    fn requests(&self) -> Vec<KeySelector> {
        self.state.lock().unwrap().requests.clone()
    }
}

struct Env {
    db: Database<MemRuntime>,
    data: Data,
    storage: Vec<FakeStorage>,
    /// The commit proxy's shard map: ranges and the indices of their servers.
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

impl Env {
    /// A cluster with `n` storage servers; the first holds everything until
    /// [`layout`](Self::layout) says otherwise.
    async fn new(schema: SchemaVersion, n: usize) -> Env {
        let net = Net::new();
        let rt = MemRuntime::new(&net);
        let version: ProtocolVersion = schema.protocol_version();
        let mut coordinator = FakeCoordinator::start(&net, sock(1, 4500), version);
        let data: Data = Arc::new(Mutex::new(BTreeMap::new()));

        // Storage servers: `getKey` as `findKey`.
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
                        st.requests.push(request.sel.clone());
                        let answer = match shard_range(&st.owns, &request.sel) {
                            None => Err(codes::WRONG_SHARD_SERVER),
                            Some(r) => Ok(find_key(
                                &data.lock().unwrap(),
                                &request.sel,
                                &r,
                                st.row_limit,
                            )),
                        };
                        let payload = match &answer {
                            Ok(sel) => GetKey::encode_reply(
                                incoming.schema,
                                &Ok(GetKeyReply {
                                    sel: sel.borrowed(),
                                    ..GetKeyReply::default()
                                }),
                            ),
                            Err(code) => GetKey::encode_reply(
                                incoming.schema,
                                &Ok(GetKeyReply {
                                    error: Some(FdbError::new(*code)),
                                    ..GetKeyReply::default()
                                }),
                            ),
                        }
                        .unwrap();
                        Action::reply(reply, payload)
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

        // The GRV proxy.
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

        // The commit proxy: locations (forward and reverse) and commits.
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
                assert_eq!(request.end, None);
                let key = request.begin.clone();
                let reverse = request.reverse;
                locations.lock().unwrap().push(request);
                let shards = shards.lock().unwrap();
                let (r, team) = shards
                    .iter()
                    .find(|(r, _)| {
                        if reverse {
                            r.begin < key && key <= r.end
                        } else {
                            r.begin <= key && key < r.end
                        }
                    })
                    .cloned()
                    .expect("a shard for every key");
                let results = vec![(r, team.iter().map(|&i| servers[i].clone()).collect())];
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

        // The coordinator answers the first long poll; the others stay pending.
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

    fn keys(&self) -> Vec<Vec<u8>> {
        self.data.lock().unwrap().keys().cloned().collect()
    }

    /// The shard map: `(begin, end, server)` (one replica per shard). Each server then holds
    /// the union of its shards (and the commit proxy answers from the map).
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

    fn location_requests(&self) -> Vec<(Vec<u8>, bool)> {
        self.locations
            .lock()
            .unwrap()
            .iter()
            .map(|r| (r.begin.clone(), r.reverse))
            .collect()
    }

    async fn get_key(&self, sel: &KeySelector) -> Vec<u8> {
        within("get_key", self.db.reader().get_key(sel, READ_VERSION))
            .await
            .unwrap_or_else(|e| panic!("{sel:?}: {e}"))
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

/// Keys `a f k` on the first shard, `n q x` on the second.
const KEYS: [&[u8]; 6] = [b"a", b"f", b"k", b"n", b"q", b"x"];

// ---- the reader ------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_single_shard_resolves_every_kind_of_selector() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        env.set(&KEYS);
        let cases: Vec<(KeySelector, &[u8])> = vec![
            (fge(b"g"), b"k"),
            (fge(b"k"), b"k"),
            (fgt(b"k"), b"n"),
            (llt(b"k"), b"f"),
            (lle(b"k"), b"k"),
            (lle(b"j"), b"f"),
            (plus(fge(b"a"), 2), b"k"),
            (plus(llt(b"x"), -2), b"k"),
            (KeySelector::new(b"k".to_vec(), true, -1), b"f"),
        ];
        for (sel, want) in cases {
            assert_eq!(env.get_key(&sel).await, want, "{schema} {sel:?}");
        }
        // One location request for the one shard, all forward.
        assert_eq!(env.location_requests(), vec![(b"g".to_vec(), false)]);
    }
}

#[tokio::test(start_paused = true)]
async fn selectors_cross_shard_boundaries_forward_and_backward() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&KEYS);
        env.layout(&[(b"", b"m", 0), (b"m", KEY_SPACE_END, 1)]);

        // Forward: nothing after `k` on the first shard; the second resolves the rest.
        assert_eq!(env.get_key(&fgt(b"k")).await, b"n");
        assert_eq!(
            env.storage[0].requests(),
            vec![fgt(b"k")],
            "the first shard's server sees the selector"
        );
        assert_eq!(
            env.storage[1].requests(),
            vec![KeySelector::new(b"m".to_vec(), false, 1)],
            "the second continues from the boundary"
        );

        // Backward: nothing before `n` on the second shard (from `m` on); the first resolves
        // `lastLessThan(m)`, located with a reverse request.
        assert_eq!(env.get_key(&llt(b"n")).await, b"k");
        assert_eq!(
            env.storage[1].requests().last(),
            Some(&llt(b"n")),
            "a backward selector at `n` is in the second shard"
        );
        assert_eq!(env.storage[0].requests().last(), Some(&llt(b"m")));
        // `lastLessOrEqual(m)` is not backward: the shard containing `m`, then the one
        // before it.
        assert_eq!(env.get_key(&lle(b"m")).await, b"k");
        // `lastLessThan(m)` itself is located before `m`: the first shard, cached by now.
        let before = env.location_requests().len();
        assert_eq!(env.get_key(&llt(b"m")).await, b"k");
        assert_eq!(env.location_requests().len(), before);

        let requests = env.location_requests();
        assert_eq!(
            requests,
            vec![(b"k".to_vec(), false), (b"m".to_vec(), false)],
            "two shards, located once each"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_reverse_location_request_finds_the_shard_before_a_boundary() {
    let env = Env::new(SchemaVersion::V8_0, 2).await;
    env.set(&KEYS);
    env.layout(&[(b"", b"m", 0), (b"m", KEY_SPACE_END, 1)]);
    // A cold cache: `lastLessThan(m)` is located with `reverse` set, and lands on `["", m)`.
    assert_eq!(env.get_key(&llt(b"m")).await, b"k");
    assert_eq!(env.location_requests(), vec![(b"m".to_vec(), true)]);
    assert_eq!(env.storage[1].served(), 0);
    // `lastLessThan(\xff\xff)`: the last shard, located before the end of the key space.
    assert_eq!(env.get_key(&llt(KEY_SPACE_END)).await, b"x");
    assert_eq!(
        env.location_requests().last(),
        Some(&(KEY_SPACE_END.to_vec(), true))
    );
}

#[tokio::test(start_paused = true)]
async fn large_offsets_walk_several_shards_and_stop_at_the_ends() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 3).await;
        env.set(&KEYS);
        env.layout(&[(b"", b"g", 0), (b"g", b"p", 1), (b"p", KEY_SPACE_END, 2)]);
        for (sel, want) in [
            (plus(fge(b"a"), 4), b"q".as_slice()),
            (plus(fge(b"a"), 5), b"x"),
            (plus(fge(b""), 0), b"a"),
            (plus(llt(b"x"), -4), b"a"),
            (plus(llt(b"x"), -3), b"f"),
            (plus(lle(b"x"), -1), b"q"),
        ] {
            assert_eq!(env.get_key(&sel).await, want, "{schema} {sel:?}");
        }
        // Beyond the database: the end of the key space, and the empty key.
        assert_eq!(env.get_key(&plus(fge(b"a"), 6)).await, KEY_SPACE_END);
        assert_eq!(
            env.get_key(&plus(fge(b"a"), 1_000_000)).await,
            KEY_SPACE_END
        );
        assert_eq!(env.get_key(&plus(llt(b"x"), -5)).await, b"");
        assert_eq!(env.get_key(&plus(llt(b"x"), -1_000_000)).await, b"");
        assert_eq!(env.get_key(&fgt(b"x")).await, KEY_SPACE_END);
        assert_eq!(env.get_key(&llt(b"a")).await, b"");
        assert_eq!(env.get_key(&fge(b"")).await, b"a");
    }
}

#[tokio::test(start_paused = true)]
async fn the_ends_of_the_key_space_need_no_request() {
    let env = Env::new(SchemaVersion::V7_4, 1).await;
    env.set(&KEYS);
    for sel in [
        fge(KEY_SPACE_END),
        fgt(KEY_SPACE_END),
        plus(fge(KEY_SPACE_END), 7),
        llt(b""),
        lle(b""),
        plus(llt(b""), -3),
    ] {
        let want: &[u8] = if sel.key.is_empty() {
            b""
        } else {
            KEY_SPACE_END
        };
        assert_eq!(env.get_key(&sel).await, want, "{sel:?}");
    }
    assert_eq!(env.storage[0].served(), 0);
    assert!(env.location_requests().is_empty());
    // `lastLessOrEqual(\xff\xff)` drops its `orEqual` and is resolved backward.
    assert_eq!(env.get_key(&lle(KEY_SPACE_END)).await, b"x");
    assert_eq!(env.storage[0].requests(), vec![llt(KEY_SPACE_END)]);
}

#[tokio::test(start_paused = true)]
async fn partial_replies_of_a_byte_limited_scan_are_continued() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        let keys: Vec<Vec<u8>> = (0..40u8)
            .map(|i| vec![b'k', b'0' + i / 10, b'0' + i % 10])
            .collect();
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        env.set(&refs);
        env.row_limit(Some(3));
        assert_eq!(env.get_key(&plus(fge(b"k"), 20)).await, keys[20]);
        // About 20 / 3 requests on the one server.
        let served = env.storage[0].served();
        assert!((7..=8).contains(&served), "{served}");
        assert_eq!(env.get_key(&plus(llt(b"l"), -25)).await, keys[39 - 25]);
        assert_eq!(env.get_key(&plus(llt(b"l"), -45)).await, b"");
        assert_eq!(env.get_key(&plus(fge(b"k"), 45)).await, KEY_SPACE_END);
    }
}

#[tokio::test(start_paused = true)]
async fn random_selectors_match_the_definition_over_random_shard_maps() {
    let mut seed: u64 = 0x9e37_79b9;
    let mut next = move |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    // Selectors that took more than one storage request (crossed a shard or were continued
    // after a limited scan).
    let mut chained = 0;
    for schema in SCHEMAS {
        for round in 0..12 {
            let env = Env::new(schema, 3).await;
            let alphabet: &[u8] = b"bdfhjlnprtvx";
            let keys: Vec<Vec<u8>> = (0..25)
                .map(|_| {
                    let len = 1 + next(2) as usize;
                    (0..len)
                        .map(|_| alphabet[next(alphabet.len() as u64) as usize])
                        .collect()
                })
                .collect();
            let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
            env.set(&refs);
            // 1 to 4 shards with random boundaries and owners (adjacent shards of one owner
            // are one readable range for it).
            let mut bounds: Vec<Vec<u8>> = (0..next(4))
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
                _ => Some(2),
            });
            let all = env.keys();
            for _ in 0..60 {
                let key: Vec<u8> = match next(8) {
                    0 => Vec::new(),
                    1 => KEY_SPACE_END.to_vec(),
                    2 => all[next(all.len() as u64) as usize].clone(),
                    _ => (0..1 + next(2))
                        .map(|_| alphabet[next(alphabet.len() as u64) as usize] - next(2) as u8)
                        .collect(),
                };
                let offset = match next(10) {
                    0 => 1000,
                    1 => -1000,
                    _ => next(17) as i32 - 8,
                };
                let sel = KeySelector::new(key, next(2) == 1, offset);
                let served = |env: &Env| env.storage.iter().map(FakeStorage::served).sum::<usize>();
                let before = served(&env);
                assert_eq!(
                    env.get_key(&sel).await,
                    reference(&all, &sel),
                    "{schema} round {round} {sel:?} layout {layout:?}"
                );
                if served(&env) > before + 1 {
                    chained += 1;
                }
            }
        }
    }
    assert!(
        chained > 100,
        "{chained} selectors took more than one request"
    );
}

#[tokio::test(start_paused = true)]
async fn wrong_shard_server_invalidates_and_relocates_in_both_directions() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&KEYS);
        // Cache the whole key space on server 0, then split it: `[m, \xff\xff)` moves to 1.
        assert_eq!(env.get_key(&fge(b"a")).await, b"a");
        env.layout(&[(b"", b"m", 0), (b"m", KEY_SPACE_END, 1)]);

        // Forward from server 0, which stops at `m`; the stale cache sends `m` to 0 again
        // (`wrong_shard_server`), and the selector is located again.
        assert_eq!(env.get_key(&fgt(b"k")).await, b"n");
        assert_eq!(
            env.storage[0].requests()[1..],
            [fgt(b"k"), KeySelector::new(b"m".to_vec(), false, 1)]
        );
        assert_eq!(env.storage[1].served(), 1);
        assert_eq!(
            env.location_requests(),
            vec![(b"a".to_vec(), false), (b"m".to_vec(), false)]
        );

        // Backward through a stale cache: refresh `[\"\", \xff\xff)` on server 0 again, then
        // ask for the last key before `x`, in the moved shard.
        let fresh = Env::new(schema, 2).await;
        fresh.set(&KEYS);
        assert_eq!(fresh.get_key(&llt(b"x")).await, b"q");
        fresh.layout(&[(b"", b"m", 0), (b"m", KEY_SPACE_END, 1)]);
        assert_eq!(fresh.get_key(&llt(b"x")).await, b"q");
        assert_eq!(
            fresh.storage[0].served(),
            2,
            "the stale server was asked first"
        );
        assert_eq!(fresh.storage[1].requests(), vec![llt(b"x")]);
        assert_eq!(
            fresh.location_requests(),
            vec![(b"x".to_vec(), true), (b"x".to_vec(), true)],
            "the shard before `x` was invalidated and located again"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn the_reader_checks_its_arguments() {
    let env = Env::new(SchemaVersion::V8_0, 1).await;
    let reader = env.db.reader();
    let e = reader.get_key(&fge(b"a"), 0).await.unwrap_err();
    assert_eq!(e.code(), codes::CLIENT_INVALID_OPERATION);
    let e = reader.get_key(&fge(b"a"), -2).await.unwrap_err();
    assert_eq!(e.code(), codes::CLIENT_INVALID_OPERATION);
    let e = reader
        .get_key(&fge(b"\xff\xff\x00"), READ_VERSION)
        .await
        .unwrap_err();
    assert_eq!(e.code(), codes::KEY_OUTSIDE_LEGAL_RANGE);
    // A snapshot reads at its version.
    let snapshot = reader.snapshot_at(READ_VERSION);
    env.set(&KEYS);
    assert_eq!(snapshot.get_key(&fgt(b"a")).await.unwrap(), b"f");
}

// ---- the transaction -------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn transaction_read_conflict_ranges_follow_cpp() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 2).await;
        env.set(&KEYS);
        env.layout(&[(b"", b"m", 0), (b"m", KEY_SPACE_END, 1)]);
        let tx = env.transaction();
        let cases: Vec<(KeySelector, &[u8], Option<KeyRange>)> = vec![
            // Offset > 0: `[k, r\x00)`, `[k\x00, r\x00)` with `or_equal`.
            (fge(b"g"), b"k", Some(range(b"g", b"k\x00"))),
            (fgt(b"k"), b"n", Some(range(b"k\x00", b"n\x00"))),
            // Offset <= 0: `[r, k)`, `[r, k\x00)` with `or_equal`.
            (llt(b"n"), b"k", Some(range(b"k", b"n"))),
            (lle(b"n"), b"n", Some(range(b"n", b"n\x00"))),
            (plus(llt(b"x"), -2), b"k", Some(range(b"k", b"x"))),
            // Before the first key: from `""`.
            (llt(b"a"), b"", Some(range(b"", b"a"))),
            // Past the last key: `\xff\xff` from the storage servers, clamped to `\xff` for
            // the caller; the conflict range is C++'s, unclamped (`keyAfter(\xff\xff)` is
            // `\xff\xff`).
            (fgt(b"x"), b"\xff", Some(range(b"x\x00", b"\xff\xff"))),
            // An empty range is not added.
            (llt(b""), b"", None),
        ];
        let mut expected = Vec::new();
        for (sel, want, conflict) in cases {
            assert_eq!(
                within("get_key", tx.get_key(&sel)).await.unwrap(),
                want,
                "{schema} {sel:?}"
            );
            expected.extend(conflict);
        }
        // Snapshot reads add nothing.
        assert_eq!(tx.snapshot().get_key(&fgt(b"f")).await.unwrap(), b"k");
        tx.set(b"w", b"1");
        within("commit", tx.commit()).await.unwrap();
        let commits = env.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        // The commit sorts the ranges (`intersects`); the write to `w` is inside `[k, x)`,
        // so there is no self-conflicting key.
        let mut sent = commits[0].read_conflict_ranges.clone();
        sent.sort_by(|a, b| (&a.begin, &a.end).cmp(&(&b.begin, &b.end)));
        expected.sort_by(|a, b| (&a.begin, &a.end).cmp(&(&b.begin, &b.end)));
        assert_eq!(sent, expected, "{schema}");
        assert_eq!(commits[0].read_snapshot, READ_VERSION);
    }
}

#[tokio::test(start_paused = true)]
async fn transaction_key_checks_clamping_and_long_keys() {
    for schema in SCHEMAS {
        let env = Env::new(schema, 1).await;
        env.set(&KEYS);
        env.set(&[b"\xff/sys"]);

        // Without system key access: keys after `\xff` are outside the legal range, `\xff`
        // itself is allowed, and results in the system keys are clamped to `\xff`.
        let tx = env.transaction();
        let e = tx.get_key(&fge(b"\xff\x00")).await.unwrap_err();
        assert_eq!(e.code(), codes::KEY_OUTSIDE_LEGAL_RANGE);
        assert_eq!(tx.get_key(&fge(b"\xff")).await.unwrap(), b"\xff");
        assert_eq!(tx.get_key(&fgt(b"x")).await.unwrap(), b"\xff");
        assert_eq!(tx.get_key(&llt(b"\xff")).await.unwrap(), b"x");

        // With it: up to `\xff\xff`.
        let tx = env.transaction();
        tx.set_option(TransactionOption::ReadSystemKeys).unwrap();
        assert_eq!(tx.get_key(&fgt(b"x")).await.unwrap(), b"\xff/sys");
        assert_eq!(tx.get_key(&fge(b"\xff\x00")).await.unwrap(), b"\xff/sys");
        assert_eq!(tx.get_key(&fgt(b"\xff/sys")).await.unwrap(), KEY_SPACE_END);
        assert_eq!(
            tx.get_key(&fgt(KEY_SPACE_END)).await.unwrap(),
            KEY_SPACE_END
        );
        let e = tx.get_key(&fge(b"\xff\xff\x00")).await.unwrap_err();
        assert_eq!(e.code(), codes::KEY_OUTSIDE_LEGAL_RANGE);

        // A key longer than any key is shortened to one byte over the limit
        // (`KeySelectorRef::setKey`; the limit has the tenant prefix slack on 7.4).
        let tx = env.transaction();
        let long = vec![b'f'; 20_000];
        assert_eq!(tx.get_key(&fge(&long)).await.unwrap(), b"k");
        let limit = match schema {
            SchemaVersion::V7_4 => 10_008,
            SchemaVersion::V8_0 => 10_000,
        };
        let sent = env.storage[0].requests().last().unwrap().key.len();
        assert_eq!(sent, limit + 1, "{schema}");
        let e = tx.get_key(&fge(b"\xff\x01")).await.unwrap_err();
        assert_eq!(e.code(), codes::KEY_OUTSIDE_LEGAL_RANGE);
    }
}

#[tokio::test(start_paused = true)]
async fn transaction_get_key_follows_the_transaction_rules() {
    let env = Env::new(SchemaVersion::V7_4, 1).await;
    env.set(&KEYS);
    // A deferred error comes first.
    let tx = env.transaction();
    tx.set(b"\xff/nope", b"");
    let e = tx.get_key(&fge(b"a")).await.unwrap_err();
    assert_eq!(e.code(), codes::KEY_OUTSIDE_LEGAL_RANGE);
    // A cancelled transaction.
    let tx = env.transaction();
    tx.cancel();
    let e = tx.get_key(&fge(b"a")).await.unwrap_err();
    assert_eq!(e.code(), codes::TRANSACTION_CANCELLED);
    // After the commit started: `used_during_commit`.
    let tx = env.transaction();
    tx.set(b"a", b"1");
    within("commit", tx.commit()).await.unwrap();
    let e = tx.get_key(&fge(b"a")).await.unwrap_err();
    assert_eq!(e.code(), codes::USED_DURING_COMMIT);
    // Without a read version set, the first read asks for one.
    let tx = env.db.create_transaction();
    tx.set_option(TransactionOption::ReadYourWritesDisable)
        .unwrap();
    assert_eq!(tx.get_key(&fgt(b"a")).await.unwrap(), b"f");
    assert_eq!(tx.get_read_version().await.unwrap(), READ_VERSION);
}
