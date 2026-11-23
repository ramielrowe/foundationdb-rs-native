//! The read path: read versions from the GRV proxies, key locations from the commit proxies
//! (with the location cache), and values from the storage servers.
//!
//! [`Reader`] is the entry point. It works over the [`PeerManager`] and the
//! [`ClusterMonitor`] the rest of the client shares, on the version-neutral domain types: each
//! request is encoded for the schema set of the connection it goes out on.
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use fdb_native_client::read::{ReadConfig, Reader};
//! use fdb_native_client::{ClusterMonitor, ClusterMonitorConfig, ConnectionRecord};
//! use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig};
//! use fdb_native_transport::runtime::TokioRuntime;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let rt = TokioRuntime::new();
//! let peers = PeerManager::new(rt.clone(), PeerManagerConfig::default(), DiscardEvents);
//! let record = ConnectionRecord::open("/etc/foundationdb/fdb.cluster")?;
//! let monitor = Arc::new(ClusterMonitor::start(
//!     rt.clone(),
//!     peers.clone(),
//!     record,
//!     ClusterMonitorConfig::default(),
//! ));
//! let reader = Reader::new(rt, peers, monitor, ReadConfig::default());
//!
//! let version = reader.get_read_version().await?;
//! let value = reader.get(b"hello", version).await?;
//! println!("hello = {value:?} at {version}");
//!
//! // The same, as a snapshot.
//! let snapshot = reader.snapshot().await?;
//! assert!(snapshot.version() >= version);
//! let _ = snapshot.get(b"hello").await?;
//! # Ok(())
//! # }
//! ```
//!
//! Errors are [`ClientError`]s with C++'s codes. Those a transaction retries
//! ([`ClientError::is_retryable`]: `future_version`, `transaction_too_old`,
//! `database_locked`, `grv_proxy_memory_limit_exceeded`, `process_behind`, ...) are returned
//! to the caller, as C++ returns them to `Transaction::onError`; the reader retries by itself
//! only what C++'s read path retries internally (below).
//!
//! # What C++ does, and what is ported
//!
//! Citations are `file:line` on `origin/release-7.4` / `origin/release-8.0` of the
//! FoundationDB repository. `NativeAPI` is `fdbclient/NativeAPI.actor.cpp` (7.4) /
//! `fdbclient/NativeAPI.cpp` (8.0); `LB` is `fdbrpc/include/fdbrpc/LoadBalance.actor.h` /
//! `LoadBalance.h`; `ClientKnobs` is `fdbclient/ClientKnobs.cpp`; `FlowKnobs` is
//! `flow/Knobs.cpp`. The logic is the same on both releases unless noted (8.0 converted the
//! actors to coroutines and removed tenants).
//!
//! ## Read versions
//!
//! * **The request.** `TransactionState::getReadVersion` (NativeAPI 7.4:7865-7948,
//!   8.0:5665-5755) adds the priority to the flags (`PRIORITY_DEFAULT` for a default
//!   transaction, so `flags` is `0x0800_0000`) and hands the request to the batcher of those
//!   flags. `getConsistentReadVersion` (7.4:7595-7669, 8.0:5288-5381) sends one
//!   `GetReadVersionRequest` per batch, with `transactionCount` the batch size, no tags,
//!   `maxVersion` from the version-vector cache (`invalidVersion` here: version vectors are
//!   off), and on 8.0 no `maxGrvQueueDelayMS` unless the transaction option is set
//!   ([`ReadVersionOptions`] covers the priority, `CAUSAL_READ_RISKY`,
//!   `USE_PROVISIONAL_PROXIES` and lock awareness).
//! * **Load balancing.** `basicLoadBalance` (LB 7.4:1031-1131, 8.0:645-760) over the GRV
//!   proxies the client uses (the monitor's sample): start with the "best" one, walk the
//!   others, skip those the failure monitor says are failed, and on `broken_promise` or
//!   `request_maybe_delivered` (a broken connection) try the next one, backing off from 10 ms
//!   doubling to 5 s once every proxy was tried (`LOAD_BALANCE_*_BACKOFF`, FlowKnobs
//!   7.4:285-287). Any other error (`grv_proxy_memory_limit_exceeded`,
//!   `batch_transaction_throttled`, ...) is returned. When every proxy is failed it waits for
//!   one to come back. The whole is raced against `onProxiesChanged`
//!   ([`ClusterMonitor::proxies_changed`]): new proxies get the request again. While there
//!   are no (non-provisional) GRV proxies it waits for some
//!   ([`ClusterMonitor::grv_proxies`]).
//! * **Batching.** `readVersionBatcher` (7.4:7671-7761, 8.0:5383-5482): one batcher per set
//!   of flags collects requests and sends a batch when it has `MAX_BATCH_SIZE` (1000) of
//!   them, or `batchTime` after the first; `batchTime` starts at 0 (a batch is what arrives
//!   before the batcher runs again) and follows half the reply latency, smoothed
//!   (`0.1 * target + 0.9 * batchTime`) and capped at `GRV_BATCH_TIMEOUT` (5 ms; ClientKnobs
//!   7.4:94-96, 8.0:204-206). Every transaction of the batch gets the same reply.
//! * **The reply.** `extractReadVersion` (7.4:7763-7863): `database_locked` if the database
//!   is locked and the transaction is not lock aware; the version otherwise.
//!
//! ## Key locations
//!
//! * **Lookup.** `getKeyLocation` (7.4:3357-3390, 8.0:1343-1373) looks in the cache first. A
//!   hit whose replicas include an endpoint that failed on a healthy server
//!   (`checkOnlyEndpointFailed`, 7.4:3329-3355: the server restarted its storage role) is
//!   invalidated and fetched again, during `LOCATION_CACHE_ENDPOINT_FAILURE_GRACE_PERIOD`
//!   (60 s) and then once every `LOCATION_CACHE_FAILED_ENDPOINT_RETRY_INTERVAL` (60 s).
//! * **Fetch.** `getKeyLocation_internal` (7.4:3268-3325, 8.0:1250-1312) sends
//!   `GetKeyServerLocationsRequest(begin = key, end = none, limit = 100, reverse = false)`,
//!   with the transaction's read version when it has one (`latestVersion` otherwise), to the
//!   commit proxies with `basicLoadBalance`, raced against `onProxiesChanged`, after the
//!   database's backoff: `commit_proxy_memory_limit_exceeded` is retried after a backoff that
//!   starts at `DEFAULT_BACKOFF` (10 ms) and doubles up to
//!   `RESOURCE_CONSTRAINED_MAX_BACKOFF` (30 s), and halves on each success
//!   (`updateBackoff`, 7.4:3404-3433). The one shard of the reply is cached.
//! * **The cache.** `DatabaseContext::locationCache` (7.4:2171-2277): a range map from
//!   shard to replicas. `setCachedLocation` overwrites what the new range overlaps (the parts
//!   of an older range outside it keep their old replicas); `invalidateCache(key)` forgets
//!   the whole range containing the key; `invalidateCache(range)` every range intersecting
//!   it. Each replica is interned by server id (`StorageServerInfo::getInterface`) and holds a
//!   reference to its peer, so connections to the servers of cached shards stay up and a
//!   failed one is probed until it comes back.
//! * **Bound.** Before an insert, while the cache holds `LOCATION_CACHE_EVICTION_SIZE`
//!   ranges ([`ReadConfig::location_cache_size`], 600 000; ClientKnobs 7.4:99,
//!   8.0:209), a random range is evicted (at most 100 per insert, as C++). Unlike C++, whose
//!   `KeyRangeMap` keeps the boundaries of evicted and invalidated ranges, adjacent unknown
//!   ranges are merged, so the map itself stays within twice the bound.
//! * **Not ported:** the TSS mapping (`resultsTssMapping` is ignored: testing storage
//!   servers only shadow reads for comparison), the tag mapping (version vectors), the
//!   periodic peer evictor (`LOCATION_CACHE_PEER_EVICTOR_*`, off on 7.4), and tenants.
//!
//! ## Values
//!
//! * **`getValue`** (7.4:3891-4035, 8.0:1687-1820): locate the key, then `loadBalance` a
//!   `GetValueRequest(key, version)` (no tags, no read options, no version vector) over its
//!   replicas. `wrong_shard_server` (1001) and `all_alternatives_failed` (1006) invalidate the
//!   key's cached location, wait `WRONG_SHARD_SERVER_DELAY` (10 ms;
//!   [`ReadConfig::wrong_shard_server_delay`]) and start again; any other error is returned:
//!   `future_version` (1009) and `transaction_too_old` (1007) go to the transaction, which
//!   retries with a new read version. A version of 0 or less is refused with
//!   `client_invalid_operation` (`validateVersion` throws it for 0 and asserts for the rest).
//! * **`loadBalance`** (LB 7.4:686-1022, 8.0:307-620), with the `QueueModel`
//!   (`fdbrpc/QueueModel.cpp`; C++ clients enable it, `EnableLocalityLoadBalance::True`):
//!   - The replicas are ordered by distance (`loadBalanceDistance`: the same `dcid` as
//!     [`ReadConfig::client_dc_id`] is `SAME_DC`, anything else `DISTANT`), shuffled within
//!     a distance. The first choice is the replica with the fewest outstanding requests,
//!     each counted as the server's last reported **penalty** and smoothed over 2 s
//!     (`QUEUE_MODEL_SMOOTHING_AMOUNT`); the nearest replicas only, unless more than
//!     `LOAD_BALANCE_MAX_BAD_OPTIONS` (1) of them are bad: failed, penalized
//!     (`penalty > 1.001`) or backing off after `future_version` (`failedUntil`, 1 s doubling
//!     to 8 s). The others follow in turn.
//!   - Each reply's `penalty` updates the model. Replicas the failure monitor marks failed
//!     (address down for 4 s, or endpoint gone) are skipped.
//!   - `checkAndProcessResult`: success returns; `server_overloaded` and a connection-level
//!     failure (`request_maybe_delivered`, `broken_promise`: reads are idempotent, so a
//!     request that may have been delivered is simply sent again) try the next replica,
//!     with the same backoff as above once all were tried; `process_behind` too, until all
//!     were tried, then it is returned; any other error a replica answers is returned.
//!   - When every replica is failed, `allAlternativesFailedDelay` (`fdbrpc/LoadBalance.cpp`
//!     8.0:29-53) waits for one to come back for 50 ms, growing to 1 s (and, after minutes,
//!     30 s) while failures continue, jittered, then fails with `all_alternatives_failed`.
//!
//! ## Deliberate differences
//!
//! * **No second requests.** C++'s `loadBalance` sends a speculative second request to the
//!   next replica when the first is slow (`secondDelay`, the `secondMultiplier` /
//!   `secondBudget` of the model, `INSTANT_SECOND_REQUEST_MULTIPLIER`,
//!   `BASE_SECOND_REQUEST_TIME`), and keeps processing "lagging" replies for the model. Here
//!   one request is outstanding at a time: a slow replica is waited for (the transport's
//!   failure detection still ends a dead one). The model's latency is tracked but unused.
//! * **GRV proxy choice.** `ModelInterface::getBest` weights the proxies by their reported
//!   `processBusyTime`, re-evaluated every 10 s (`BASIC_LOAD_BALANCE_*`); here the first
//!   choice is uniform at random.
//! * **Not modelled:** the GRV cache (`USE_GRV_CACHE`), tag throttling (no tags are sent),
//!   version vectors, TSS duplicate requests and replica consistency checks
//!   (`enableReplicaConsistencyCheck`), `connectionFileChanged` (a `switchConnectionRecord`
//!   makes C++ fail reads with `transaction_too_old`), debug ids and metrics, and the
//!   `LoadBalanceTooLong` trace.
//! * **Shutdown.** Once the [`PeerManager`] is shut down, requests fail with
//!   `operation_cancelled` (1101) instead of being retried; once the monitor is shut down,
//!   a wait for proxies fails the same way.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use fdb_native_schema::Request;
use fdb_native_schema::domain::{
    GetKeyServerLocations, GetValue, KeyRange, LATEST_VERSION, StorageServer,
};
use fdb_native_transport::peer::{NoTls, PeerManager, PeerRef, StreamWrapper};
use fdb_native_transport::runtime::{Runtime, TaskHandle};
use fdb_native_wire::{Endpoint, NetworkAddress, Uid, VersionVector};
use futures_channel::oneshot;

use crate::cluster::{ClusterInfo, ClusterMonitor};
use crate::error::ClientError;
use crate::error::codes::*;

mod balance;
mod grv;
mod location;
mod util;

pub use grv::{Priority, ReadVersionOptions};
pub use location::KEY_SPACE_END;

use grv::Batcher;
use location::{Location, LocationCache};
use util::{Either, Rng, never, now_secs, race};

/// `LOCATION_CACHE_ENDPOINT_FAILURE_GRACE_PERIOD` (60 s; ClientKnobs 7.4:101, 8.0:211).
const ENDPOINT_FAILURE_GRACE_PERIOD: f64 = 60.0;
/// `LOCATION_CACHE_FAILED_ENDPOINT_RETRY_INTERVAL` (60 s; ClientKnobs 7.4:102, 8.0:212).
const FAILED_ENDPOINT_RETRY_INTERVAL: f64 = 60.0;
/// `DEFAULT_BACKOFF` (10 ms; ClientKnobs 7.4:70, 8.0:175).
const DEFAULT_BACKOFF: f64 = 0.01;
/// `BACKOFF_GROWTH_RATE` (2; ClientKnobs 7.4:72, 8.0:177).
const BACKOFF_GROWTH_RATE: f64 = 2.0;
/// `RESOURCE_CONSTRAINED_MAX_BACKOFF` (30 s; ClientKnobs 7.4:73, 8.0:178).
const RESOURCE_CONSTRAINED_MAX_BACKOFF: f64 = 30.0;

/// The read path's knobs. [`ReadConfig::default`] has C++'s values.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ReadConfig {
    /// `MAX_BATCH_SIZE` (1000): the most read version requests sent as one. 1 sends each
    /// request on its own.
    pub max_batch_size: usize,
    /// `GRV_BATCH_TIMEOUT` (5 ms): the longest a read version request waits for others to
    /// batch with.
    pub grv_batch_timeout: Duration,
    /// `LOCATION_CACHE_EVICTION_SIZE` (600 000): the most shards whose location is cached.
    pub location_cache_size: usize,
    /// `WRONG_SHARD_SERVER_DELAY` (10 ms): the wait before locating a key again after
    /// `wrong_shard_server` or `all_alternatives_failed`.
    pub wrong_shard_server_delay: Duration,
    /// The client's data center id (C++'s client locality `dcid`, the `datacenter_id`
    /// network option): replicas in it are preferred. `None` (the default) prefers none.
    pub client_dc_id: Option<Vec<u8>>,
    /// Seed of the random choices (replica order, eviction, jitter). `None` draws one.
    pub seed: Option<u64>,
}

impl Default for ReadConfig {
    fn default() -> Self {
        ReadConfig {
            max_batch_size: 1000,
            grv_batch_timeout: Duration::from_millis(5),
            location_cache_size: 600_000,
            wrong_shard_server_delay: Duration::from_millis(10),
            client_dc_id: None,
            seed: None,
        }
    }
}

/// Where a key is: its shard and the shard's replicas, as the location cache has them.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct KeyLocation {
    /// The shard.
    pub range: KeyRange,
    /// Its storage servers, in the order the load balancer considers them (nearest first).
    pub servers: Vec<StorageServer>,
}

/// `allAlternativesFailedDelay`'s process-wide state (`g_network->networkInfo`).
#[derive(Debug)]
struct AlternativesFailures {
    oldest: f64,
    newest: f64,
    last_skip: f64,
}

/// When an endpoint was first seen failed on a healthy server, and last refreshed.
#[derive(Clone, Copy, Debug)]
struct EndpointFailureInfo {
    start: f64,
    last_refresh: f64,
}

#[derive(Debug)]
struct State {
    alternatives: AlternativesFailures,
    /// `DatabaseContext::backoffDelay`, for key location requests.
    location_backoff: f64,
    /// `failedEndpointsOnHealthyServersInfo`.
    failed_endpoints: HashMap<(NetworkAddress, Uid), EndpointFailureInfo>,
    /// References to the peers of the proxies of the info last used, so that they are
    /// connected and probed while the client uses them.
    proxy_references: Option<(u64, Vec<PeerRef>)>,
}

/// What the reader's handles and its batcher tasks share.
pub(crate) struct Shared<R: Runtime, W> {
    rt: R,
    peers: PeerManager<R, W>,
    monitor: Arc<ClusterMonitor<R>>,
    config: ReadConfig,
    rng: Mutex<Rng>,
    locations: Mutex<LocationCache>,
    model: Mutex<balance::QueueModel>,
    state: Mutex<State>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Inner<R: Runtime, W> {
    shared: Arc<Shared<R, W>>,
    batchers: Mutex<HashMap<u32, Batcher<R>>>,
}

impl<R: Runtime, W> Drop for Inner<R, W> {
    fn drop(&mut self) {
        for (_, batcher) in lock(&self.batchers).drain() {
            batcher.task.abort();
        }
    }
}

/// The read path of a database: read versions, key locations and values. See the [module
/// documentation](self) for what it does and how it follows C++.
///
/// Cheap to clone (a shared handle). Create it inside the executor: it spawns a task per set
/// of read version flags in use (the batchers), which end when the last clone is dropped.
pub struct Reader<R: Runtime, W = NoTls> {
    inner: Arc<Inner<R, W>>,
}

impl<R: Runtime, W> Clone for Reader<R, W> {
    fn clone(&self) -> Self {
        Reader {
            inner: self.inner.clone(),
        }
    }
}

impl<R: Runtime, W> fmt::Debug for Reader<R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let locations = lock(&self.inner.shared.locations).len();
        f.debug_struct("Reader")
            .field("cached_locations", &locations)
            .finish_non_exhaustive()
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Reader<R, W> {
    /// A reader over `peers`, finding the proxies through `monitor`.
    pub fn new(
        rt: R,
        peers: PeerManager<R, W>,
        monitor: Arc<ClusterMonitor<R>>,
        config: ReadConfig,
    ) -> Reader<R, W> {
        let shared = Shared {
            rt,
            peers,
            monitor,
            rng: Mutex::new(Rng::new(config.seed)),
            locations: Mutex::new(LocationCache::new(config.location_cache_size)),
            model: Mutex::new(balance::QueueModel::default()),
            state: Mutex::new(State {
                alternatives: AlternativesFailures {
                    oldest: f64::NEG_INFINITY,
                    newest: f64::NEG_INFINITY,
                    last_skip: f64::NEG_INFINITY,
                },
                location_backoff: 0.0,
                failed_endpoints: HashMap::new(),
                proxy_references: None,
            }),
            config,
        };
        Reader {
            inner: Arc::new(Inner {
                shared: Arc::new(shared),
                batchers: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The configuration.
    pub fn config(&self) -> &ReadConfig {
        &self.inner.shared.config
    }

    /// A read version for a default transaction (`getReadVersion`): batched with the other
    /// requests of the moment, from a GRV proxy. Fails with `database_locked` if the
    /// database is locked.
    pub async fn get_read_version(&self) -> Result<i64, ClientError> {
        self.get_read_version_with(ReadVersionOptions::default())
            .await
    }

    /// A read version, requested with `options`.
    pub async fn get_read_version_with(
        &self,
        options: ReadVersionOptions,
    ) -> Result<i64, ClientError> {
        let flags = options.flags();
        let (tx, rx) = oneshot::channel();
        let mut waiter = Some(tx);
        for _ in 0..2 {
            let sent = {
                let mut batchers = lock(&self.inner.batchers);
                let batcher = batchers
                    .entry(flags)
                    .or_insert_with(|| Batcher::start(self.inner.shared.clone(), flags));
                match batcher
                    .sender
                    .unbounded_send(waiter.take().expect("a waiter"))
                {
                    Ok(()) => true,
                    Err(e) => {
                        // The batcher ended (it panicked): start another.
                        waiter = Some(e.into_inner());
                        if let Some(old) = batchers.remove(&flags) {
                            old.task.abort();
                        }
                        false
                    }
                }
            };
            if sent {
                break;
            }
        }
        if waiter.is_some() {
            return Err(ClientError::new(INTERNAL_ERROR));
        }
        let reply = rx
            .await
            .map_err(|_| ClientError::new(OPERATION_CANCELLED))??;
        if reply.locked && !options.lock_aware {
            return Err(ClientError::new(DATABASE_LOCKED));
        }
        Ok(reply.version)
    }

    /// A [`ReadSnapshot`] at a new read version.
    pub async fn snapshot(&self) -> Result<ReadSnapshot<R, W>, ClientError> {
        let version = self.get_read_version().await?;
        Ok(self.snapshot_at(version))
    }

    /// A [`ReadSnapshot`] at `version`.
    pub fn snapshot_at(&self, version: i64) -> ReadSnapshot<R, W> {
        ReadSnapshot {
            reader: self.clone(),
            version,
        }
    }

    /// The value of `key` at `version` (`getValue`), `None` if the key is not set. `key` must
    /// be below [`KEY_SPACE_END`] (`key_outside_legal_range`); reading the system keys
    /// (`\xff...`) is not checked here (that is a transaction option).
    pub async fn get(&self, key: &[u8], version: i64) -> Result<Option<Vec<u8>>, ClientError> {
        if version <= 0 {
            return Err(ClientError::new(CLIENT_INVALID_OPERATION));
        }
        if key >= KEY_SPACE_END {
            return Err(ClientError::new(KEY_OUTSIDE_LEGAL_RANGE));
        }
        let shared = &self.inner.shared;
        // The location of the previous attempt stays alive until the next one is known (as
        // C++'s `state locationInfo` does), so that its servers keep their peer references
        // (and their probes) through an invalidation.
        let mut _previous: Option<Arc<Location>> = None;
        loop {
            let (_, location) = shared.key_location(key, version).await?;
            _previous = Some(location.clone());
            let request = GetValue {
                key: key.to_vec(),
                version,
                tags: None,
                span_context: Default::default(),
                options: None,
                ss_latest_commit_versions: VersionVector::default(),
            };
            match shared.load_balance_get_value(&location, request).await {
                Ok(value) => return Ok(value),
                Err(e) if e.code() == WRONG_SHARD_SERVER || e.code() == ALL_ALTERNATIVES_FAILED => {
                    lock(&shared.locations).invalidate_key(key);
                    shared
                        .rt
                        .sleep(shared.config.wrong_shard_server_delay)
                        .await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Where `key` is (`getKeyLocation`): from the cache, or asked of a commit proxy and
    /// cached.
    pub async fn locate(&self, key: &[u8]) -> Result<KeyLocation, ClientError> {
        if key >= KEY_SPACE_END {
            return Err(ClientError::new(KEY_OUTSIDE_LEGAL_RANGE));
        }
        let (range, location) = self.inner.shared.key_location(key, LATEST_VERSION).await?;
        Ok(KeyLocation {
            range,
            servers: location.storage_servers(),
        })
    }

    /// The cached location of `key`, without asking anyone.
    pub fn cached_location(&self, key: &[u8]) -> Option<KeyLocation> {
        let (range, location) = lock(&self.inner.shared.locations).get(key)?;
        Some(KeyLocation {
            range,
            servers: location.storage_servers(),
        })
    }

    /// Forgets the cached location of the shard containing `key` (`invalidateCache`).
    pub fn invalidate_location(&self, key: &[u8]) {
        lock(&self.inner.shared.locations).invalidate_key(key);
    }

    /// Forgets the cached locations of every shard intersecting `range`.
    pub fn invalidate_locations(&self, range: &KeyRange) {
        lock(&self.inner.shared.locations).invalidate_range(range);
    }

    /// How many shard locations are cached.
    pub fn cached_locations(&self) -> usize {
        lock(&self.inner.shared.locations).len()
    }
}

/// A read version and the reader to read at it: what a read-only transaction needs.
pub struct ReadSnapshot<R: Runtime, W = NoTls> {
    reader: Reader<R, W>,
    version: i64,
}

impl<R: Runtime, W> Clone for ReadSnapshot<R, W> {
    fn clone(&self) -> Self {
        ReadSnapshot {
            reader: self.reader.clone(),
            version: self.version,
        }
    }
}

impl<R: Runtime, W> fmt::Debug for ReadSnapshot<R, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadSnapshot")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> ReadSnapshot<R, W> {
    /// The read version.
    pub fn version(&self) -> i64 {
        self.version
    }

    /// The reader.
    pub fn reader(&self) -> &Reader<R, W> {
        &self.reader
    }

    /// The value of `key` at the snapshot's version ([`Reader::get`]).
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        self.reader.get(key, self.version).await
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Shared<R, W> {
    /// Keeps references to the peers of `info`'s proxies (replacing those of an older info).
    fn hold_proxy_references(&self, info: &ClusterInfo) {
        let mut state = lock(&self.state);
        if matches!(&state.proxy_references, Some((seq, _)) if *seq == info.sequence) {
            return;
        }
        let endpoints = info
            .grv_proxies
            .iter()
            .map(|p| p.proxy.base)
            .chain(info.commit_proxies.iter().map(|p| p.proxy.base));
        let references = endpoints
            .map(|e| self.peers.peer_for_endpoint(&e).reference())
            .collect();
        state.proxy_references = Some((info.sequence, references));
    }

    /// `getKeyLocation`: the cached location of `key`, unless one of its replicas has an
    /// endpoint that failed on a healthy server and is due a refresh; else a fetched one.
    async fn key_location(
        &self,
        key: &[u8],
        version: i64,
    ) -> Result<(KeyRange, Arc<Location>), ClientError> {
        let cached = lock(&self.locations).get(key);
        if let Some((range, location)) = cached {
            let mut refresh = false;
            for server in &location.servers {
                if self.check_only_endpoint_failed(server.address, server.get_value.token) {
                    refresh = true;
                }
            }
            if !refresh {
                return Ok((range, location));
            }
            lock(&self.locations).invalidate_key(key);
        }
        self.fetch_key_location(key, version).await
    }

    /// `checkOnlyEndpointFailed` (NativeAPI 7.4:3329-3355, 8.0:1315-1341).
    fn check_only_endpoint_failed(&self, address: NetworkAddress, token: Uid) -> bool {
        let failures = self.peers.failure_monitor();
        let only_endpoint =
            failures.endpoint_failure(&address, token).is_some() && !failures.is_failed(&address);
        let mut state = lock(&self.state);
        if only_endpoint {
            let now = now_secs(self.rt.now());
            let info =
                state
                    .failed_endpoints
                    .entry((address, token))
                    .or_insert(EndpointFailureInfo {
                        start: now,
                        last_refresh: now,
                    });
            if now - info.start < ENDPOINT_FAILURE_GRACE_PERIOD
                || now - info.last_refresh > FAILED_ENDPOINT_RETRY_INTERVAL
            {
                info.last_refresh = now;
                return true;
            }
        } else {
            state.failed_endpoints.remove(&(address, token));
        }
        false
    }

    /// `getKeyLocation_internal`.
    async fn fetch_key_location(
        &self,
        key: &[u8],
        version: i64,
    ) -> Result<(KeyRange, Arc<Location>), ClientError> {
        loop {
            let backoff = lock(&self.state).location_backoff;
            if backoff > 0.0 {
                self.rt.sleep(util::secs(backoff)).await;
            }
            let info = self
                .monitor
                .commit_proxies(false)
                .await
                .map_err(|_| ClientError::new(OPERATION_CANCELLED))?;
            self.hold_proxy_references(&info);
            let alternatives: Vec<Endpoint> = info
                .available_commit_proxies(false)
                .unwrap_or_default()
                .iter()
                .map(|p| p.streams.get_key_servers_locations)
                .collect();
            if alternatives.is_empty() {
                continue;
            }
            let request = GetKeyServerLocations {
                begin: key.to_vec(),
                end: None,
                limit: 100,
                reverse: false,
                span_context: Default::default(),
                min_tenant_version: version,
            };
            let reply = self.basic_load_balance(
                &alternatives,
                move |schema, reply, out| request.encode_request_into(schema, reply, out),
                GetKeyServerLocations::decode_reply,
            );
            let changed = async {
                match self.monitor.proxies_changed(&info).await {
                    Ok(_) => (),
                    Err(_) => never().await,
                }
            };
            match race(reply, changed).await {
                Either::Left(Ok(reply)) => {
                    self.update_location_backoff(true);
                    let Some((range, servers)) = reply.results.into_iter().next() else {
                        // C++ asserts exactly one result.
                        return Err(ClientError::new(INTERNAL_ERROR));
                    };
                    let location = self.cache_location(&range, &servers);
                    return Ok((range, location));
                }
                Either::Left(Err(e)) if e.code() == COMMIT_PROXY_MEMORY_LIMIT_EXCEEDED => {
                    self.update_location_backoff(false);
                }
                Either::Left(Err(e)) => return Err(e),
                Either::Right(()) => {}
            }
        }
    }

    /// `DatabaseContext::updateBackoff` (NativeAPI 7.4:3404-3433, 8.0:1391-1420).
    fn update_location_backoff(&self, success: bool) {
        let mut state = lock(&self.state);
        let b = &mut state.location_backoff;
        if success {
            *b /= BACKOFF_GROWTH_RATE;
            if *b < DEFAULT_BACKOFF {
                *b = 0.0;
            }
        } else if *b == 0.0 {
            *b = DEFAULT_BACKOFF;
        } else {
            *b = (*b * BACKOFF_GROWTH_RATE).min(RESOURCE_CONSTRAINED_MAX_BACKOFF);
        }
    }

    /// `setCachedLocation`.
    fn cache_location(&self, range: &KeyRange, servers: &[StorageServer]) -> Arc<Location> {
        let client_dc = self.config.client_dc_id.as_deref();
        let mut cache = lock(&self.locations);
        let infos = servers
            .iter()
            .map(|s| {
                cache.server_info(s, client_dc, |endpoint| {
                    let address = self.peers.primary_address(&endpoint.addresses);
                    (address, self.peers.peer(&address).reference())
                })
            })
            .collect();
        let mut rng = lock(&self.rng);
        let location = Arc::new(Location::new(infos, &mut rng));
        cache.insert(range, location.clone(), &mut rng);
        location
    }
}
