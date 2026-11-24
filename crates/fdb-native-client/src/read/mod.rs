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
//! use fdb_native_client::cluster::{ClusterMonitor, ClusterMonitorConfig, ConnectionRecord};
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
//! Errors are [`Error`]s with C++'s codes. Those a transaction retries
//! ([`Error::is_retryable`]: `future_version`, `transaction_too_old`,
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
//!   `USE_PROVISIONAL_PROXIES`, lock awareness and, on 8.0, `MAX_GRV_QUEUE_DELAY`).
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
//! * **Which proxy first.** `ModelInterface::getBest` (`fdbrpc/MultiInterface.h` 7.4:116-165,
//!   8.0:111-160): the GRV proxies (`GrvProxyInfo`, built with `BalanceOnRequests::True` on
//!   7.4, NativeAPI 2366, 2382, 3019, and as `ProxyGrvMetric` on 8.0,
//!   `ProxyLoadBalanceMetrics.h`) are chosen with probabilities that start even and follow
//!   the `processBusyTime` of their replies (`updateRecent`, LB 7.4:1102, 8.0:729): a GRV
//!   proxy packs the recent request count in the quotient by
//!   `BASIC_LOAD_BALANCE_COMPUTE_PRECISION` (10 000) and its CPU busyness in the remainder
//!   (`GrvProxyServer` 7.4:695-700, 8.0:755-760); every `BASIC_LOAD_BALANCE_UPDATE_RATE`
//!   (10 s), if every proxy reported in the last half of that and together they released at
//!   least `BASIC_LOAD_BALANCE_MIN_REQUESTS` (20) each, a proxy's probability moves by
//!   `BASIC_LOAD_BALANCE_MAX_CHANGE` (0.1) times (its fair share minus its share of the
//!   requests), within `1 / (MAX_PROB * n)` and `MAX_PROB / n` (`MAX_PROB` is 2), and the
//!   probabilities are normalized (FlowKnobs 7.4:307-313, 8.0:306-312). A new proxy set
//!   (a new `ClientDBInfo`) starts over with even probabilities. The commit proxies' model
//!   is by CPU (`processBusyTime % PRECISION`), but their replies carry no
//!   `processBusyTime` (a `GetKeyServerLocationsReply` is not a `BasicLoadBalancedReply`),
//!   so their choice stays uniform.
//! * **Batching and priorities.** `readVersionBatcher` (7.4:7671-7761, 8.0:5383-5482): one
//!   batcher per set of request flags (`cx->versionBatcher[flags]`, 7.4:7935; on 8.0 the
//!   key also has `maxGrvQueueDelayMS`, 8.0:5741). The flags are the priority in the high
//!   byte (`PRIORITY_BATCH` `1 << 24`, `PRIORITY_DEFAULT` `8 << 24`,
//!   `PRIORITY_SYSTEM_IMMEDIATE` `15 << 24`, set from the transaction's priority in
//!   `getReadVersion`, 7.4:7884-7899) and `FLAG_CAUSAL_READ_RISKY` / `FLAG_USE_PROVISIONAL_PROXIES`
//!   (7.4:7309, 7455), so requests of different priorities are never sent together, and
//!   each batch's request has one priority ([`Priority`], [`ReadVersionOptions`]). A batcher
//!   sends a batch when it has `MAX_BATCH_SIZE` (1000) of them, or `batchTime` after the
//!   first; `batchTime` starts at 0 (a batch is what arrives before the batcher runs again)
//!   and follows half the reply latency, smoothed (`0.1 * target + 0.9 * batchTime`) and
//!   capped at `GRV_BATCH_TIMEOUT` (5 ms; ClientKnobs 7.4:94-96, 8.0:204-206). A
//!   `batchTime` below the runtime's
//!   [`TIMER_RESOLUTION`](fdb_native_transport::runtime::Runtime::TIMER_RESOLUTION) (tokio's
//!   1 ms tick) is waited out by re-reading the clock between yields rather than on the
//!   timer, as flow's precise `delay` would wait it. Every transaction of the batch gets the
//!   same reply.
//! * **The reply.** `extractReadVersion` (7.4:7763-7863): `database_locked` if the database
//!   is locked and the transaction is not lock aware; the version otherwise.
//!
//! ## The metadata version
//!
//! `\xff/metadataVersion` ([`METADATA_VERSION_KEY`]) is the one key a transaction may read
//! without a storage server. Every GRV reply carries its value at the reply's version
//! (`GetReadVersionReply::metadataVersion`), and a commit reply carries it at the commit
//! version. `DatabaseContext::metadataVersionCache` (`METADATA_VERSION_CACHE_SIZE`, 1000
//! entries, ClientKnobs 7.4:83, 8.0:188) keeps `(version, value)` for the latest of those:
//! `extractReadVersion` adds the reply's (7.4:7830-7837, 8.0:5557-5562) after the
//! `database_locked` check, `tryCommit` the commit's (7.4:6990-6996, 8.0:4672), each only if
//! its version is greater than the newest in the cache. A transaction reading the key
//! (`Transaction::get`, 7.4:5860-5897, 8.0:3647-3677) is answered from its own GRV reply if
//! that is not in yet, else from the cache if its read version is exactly one of the cached
//! ones (a binary search over the ring), else it reads the key from a storage server like
//! any other. [`Reader::get_read_version_and_metadata`] is the first,
//! [`Reader::cached_metadata_version`] the second and [`Reader::metadata_version`] all
//! three; [`Reader::note_committed_version`] is for the commit path.
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
//!   failed one is probed until it comes back (or [the peer evictor](#the-peer-evictor)
//!   forgets the ranges that name it).
//! * **Bound.** Before an insert, while the cache holds `LOCATION_CACHE_EVICTION_SIZE`
//!   ranges ([`ReadConfig::location_cache_size`], 600 000; ClientKnobs 7.4:99,
//!   8.0:209; the `LOCATION_CACHE_SIZE` database option changes it,
//!   [`Reader::set_location_cache_size`]), a random range is evicted (at most 100 per insert, as C++). C++'s cache is a
//!   `CoalescedKeyRangeMap` (`DatabaseContext.h` 7.4:546), so an eviction or a range
//!   invalidation (both `insert` an empty value) merges the range with unknown neighbours,
//!   as here; only `invalidateCache(key)`, which assigns the value in place, keeps the
//!   boundaries in C++. Here that merges too, so the map stays within about twice the bound.
//! * **Interfaces.** A location reply that lists a known server (by id) with new endpoints
//!   and the same locality updates its cached entry in place, so every cached range uses the
//!   new endpoints (`getInterface`, 7.4:405-433); a different locality makes a new entry.
//!   Server entries no range uses any more, their queue model data and their endpoint
//!   failure records are pruned once they could outnumber the live ones.
//! * **Not ported:** the TSS mapping (`resultsTssMapping` is ignored: testing storage
//!   servers only shadow reads for comparison), the tag mapping (version vectors), and
//!   tenants.
//!
//! ## The peer evictor
//!
//! A cached range keeps its servers' peers referenced, and a referenced peer that failed is
//! probed every `SERVER_REQUEST_INTERVAL` (1 s) until it comes back. So a storage server that
//! is gone for good (excluded and removed, its machine dead) would be dialed every second
//! for as long as a cached range names it: until a read of that range fails over and
//! invalidates it, which an idle client or one reading other keys never does.
//!
//! * **C++.** 8.0 runs `locationCachePeerEvictorActor` (`DatabaseContext.cpp` 8.0:1260-1336,
//!   started at 1414-1416) when `LOCATION_CACHE_PEER_EVICTOR_ENABLED` (true on 8.0,
//!   ClientKnobs 8.0:213; 7.4 has the same code, NativeAPI 7.4:1169-1362 and 1826-1828,
//!   with the knob false, ClientKnobs 7.4:103). Every `LOCATION_CACHE_PEER_EVICTOR_DELAY`
//!   (60 s) it reads FlowTransport's per-address count of failed connects
//!   (`getPersistentConnectFailedCounts`: every failed TCP connect or TLS handshake,
//!   `FlowTransport.actor.cpp` 7.4:886-923, `FlowTransport.cpp` 8.0:980-1016; an address is
//!   forgotten once it has not failed for `PERSISTENT_CONNECT_FAILED_COUNT_TTL`, 600 s, FlowKnobs
//!   7.4:56, 8.0:61), takes the addresses whose count grew by more than
//!   `LOCATION_CACHE_PEER_EVICTOR_FAILED_THRESHOLD` (0) since the last sweep (counting from
//!   zero if it went down), and `invalidateCacheByAddresses` forgets every cached range with
//!   a replica at one of them (8.0:1144-1259), which drops the peer references and so the
//!   probes. The next read of such a range asks a commit proxy, which no longer lists a
//!   removed server.
//! * **Here.** The same, with the transport's
//!   [`FailureMonitor::connect_failures`](fdb_native_transport::peer::FailureMonitor::connect_failures)
//!   and [`ReadConfig::location_cache_peer_evictor`],
//!   [`location_cache_peer_evictor_delay`](ReadConfig::location_cache_peer_evictor_delay) and
//!   [`location_cache_peer_evictor_failed_threshold`](ReadConfig::location_cache_peer_evictor_failed_threshold).
//!   The task starts with the first cached location and ends with the reader, its
//!   [`shutdown`](Reader::shutdown), its peer manager's shutdown or its cluster monitor's. The scan is one step:
//!   `LOCATION_CACHE_PEER_EVICTOR_SCAN_CHUNK` (1 000 000) is above the cache's bound, so C++
//!   does not yield in it either. A server still in use by a read in progress keeps its peer
//!   until that read ends.
//! * **Both releases.** The evictor is on by default whichever release the cluster runs, a
//!   choice: it only changes what the client caches, so it is the client's behaviour (a C++
//!   client's is that of the library it loaded, 8.0's for an 8.0 cluster), and on a 7.4
//!   cluster it spares the cluster the same dials of dead servers for the cost of one
//!   location lookup per evicted range on its next read, which a C++ 7.4 client pays anyway
//!   once its read fails over. A server that is only flapping loses its ranges too, as in
//!   8.0; set `location_cache_peer_evictor` to `false` for 7.4's behaviour.
//!
//! ## Values
//!
//! * **`getValue`** (7.4:3891-4035, 8.0:1687-1820): locate the key, then `loadBalance` a
//!   `GetValueRequest(key, version)` (no tags, no version vector; the transaction's
//!   [read options](#read-options)) over its replicas. `wrong_shard_server` (1001) and `all_alternatives_failed` (1006) invalidate the
//!   key's cached location, wait `WRONG_SHARD_SERVER_DELAY` (10 ms;
//!   [`ReadConfig::wrong_shard_server_delay`]) and start again; any other error is returned:
//!   `future_version` (1009) and `transaction_too_old` (1007) go to the transaction, which
//!   retries with a new read version. A version of 0 or less is refused with
//!   `client_invalid_operation` (`validateVersion` throws it for 0 and asserts for the rest),
//!   including `latestVersion` (-2), which C++ only lets the cluster's own processes use.
//!   Key locations are asked with the read version, and of provisional commit proxies only
//!   if the transaction uses them (`trState->useProvisionalProxies`, 7.4:3294;
//!   [`Reader::get_with`], [`Reader::snapshot_with`]).
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
//!     **With a single replica** (as in a single-process cluster) "all tried" is never
//!     reached (`triedAllOptions` is set when the walk wraps around to its start, which one
//!     alternative never does), so, as in C++, `process_behind` is retried with the backoff
//!     (up to 5 s apart) until the server catches up or the read is abandoned.
//!   - **Second (hedged) requests** (LB 7.4:795-815, 957-1017, 8.0:405-420, 575-621). The
//!     model's second-best replica (the next with the fewest outstanding requests) gets a
//!     second request if the first is slow: after `secondMultiplier * latency(second) +
//!     BASE_SECOND_REQUEST_TIME` (0.5 ms) since the replicas were picked, or at once if the
//!     first replica's last latency is more than `INSTANT_SECOND_REQUEST_MULTIPLIER` (2)
//!     times that. Only if the model's `secondBudget` has a whole unit: each reply to a first
//!     request adds `SECOND_REQUEST_BUDGET_GROWTH` (0.05; capped at
//!     `SECOND_REQUEST_MAX_BUDGET`, 100) and lowers `secondMultiplier` by
//!     `SECOND_REQUEST_MULTIPLIER_DECAY` (0.00025; not below 1), and a second request costs 1
//!     and raises `secondMultiplier` by `SECOND_REQUEST_MULTIPLIER_GROWTH` (0.01); the timer
//!     is spent either way. The first good reply of the two wins; an error reply that is
//!     returned (`wrong_shard_server`, ...) ends the call too; one that is retried
//!     (`server_overloaded`, `process_behind` before all were tried, a lost connection)
//!     leaves the other to answer, or goes on to the next replica. A request that no
//!     longer matters is not cancelled but becomes a *lagging request* (`~RequestData`,
//!     `makeLaggingRequest`, LB 7.4:649-678, 8.0:265-287): it is kept (up to
//!     `MAX_LAGGING_REQUESTS_OUTSTANDING`, 100 000; FlowKnobs 7.4:288, 8.0:287;
//!     then all are dropped) only to feed its reply to the model (its latency, penalty and
//!     `future_version` backoff, and its outstanding count until it ends), so a slow
//!     replica is learned to be slow and avoided. Reads only: this balancer is never
//!     `AtMostOnce` (commits use their own path), so a request that may have been delivered
//!     is just sent again. An unsent request still waiting out its backoff is dropped.
//!   - When every replica is failed, `allAlternativesFailedDelay` (`fdbrpc/LoadBalance.cpp`
//!     8.0:29-53) waits for one to come back for 50 ms, growing to 1 s (and, after minutes,
//!     30 s) while failures continue, jittered, then fails with `all_alternatives_failed`.
//!   - The same balancer serves `getKey`, on the replicas' `getKey` endpoints (the failure
//!     monitor and the queue model are asked about those: as in C++ (`getAdjustedEndpoint`)
//!     the queue model is kept per stream endpoint, so a server's `getKeyValues` latency
//!     does not decide the hedging, penalties or `future_version` backoff of its `getValue`
//!     and `getKey`).
//!
//! ## Key selectors
//!
//! A [`KeySelector`](fdb_native_schema::domain::KeySelector) `(key, orEqual, offset)` names
//! the `offset`-th key after the last key `< key` (`<= key` if `orEqual`); it is "backward"
//! (`isBackward`, `FDBTypes.h` 7.4:656-658) when `!orEqual && offset <= 0`.
//!
//! * **`getKey`** (NativeAPI 7.4:4034-4140, 8.0:1826-1947; [`Reader::get_key`]) loops: a
//!   selector at `allKeys.end` (`\xff\xff`) with `offset > 0` is `\xff\xff`, otherwise its
//!   `orEqual` is dropped; a selector at `""` with `offset <= 0` is `""`. Else it locates the
//!   shard of the selector's key, or for a backward selector the shard of the keys just
//!   before it (`getKeyLocation(..., Reverse{isBackward})`: `rangeContainingKeyBefore` in the
//!   cache, a `GetKeyServerLocationsRequest` with `reverse` set), and `loadBalance`s a
//!   `GetKeyRequest(sel, version)` over its replicas' `getKey` endpoints. The reply's
//!   selector replaces the selector; `(key, true, 0)` is the answer, anything else is the
//!   rest of the way, and the loop goes on with it. `wrong_shard_server` and
//!   `all_alternatives_failed` invalidate the cached shard (in the selector's direction),
//!   wait `WRONG_SHARD_SERVER_DELAY` and go on; any other error is returned.
//! * **What a server answers** (`getKeyQ` and `findKey`, `fdbserver/storageserver.actor.cpp`
//!   7.4:7569-7666 and 4526-4626, `fdbserver/storageserver/storageserver.cpp` 8.0:6585-6668
//!   and 3415-3516): it resolves the selector within the largest readable range around it
//!   that it holds (`getShardKeyRange`, 7.4:4628-4656, 8.0:3518-3546). Found: `(key, true, 0)`.
//!   Not found in that range: the range's end (forward) or begin (backward), with what is
//!   left of the offset (`firstGreaterOrEqual(end) + n - 1`, `firstGreaterOrEqual(begin) - n`).
//!   A selector with `|offset| > 1` scans at most `STORAGE_LIMIT_BYTES`; when the scan stops
//!   early the reply is relative to the last key read (`keyAfter(last)` forward, `last`
//!   backward). A server's shards span `["", \xff\xff)`, so walking off the database's end
//!   reaches `\xff\xff` (and the start `""`), which the loop above turns into the answer: an
//!   offset beyond the database is clamped, as C++ returns `allKeys.end` / `allKeys.begin`.
//! * **Not ported:** the trace events, tags, the version vector and
//!   `connectionFileChanged`, as for values. The endpoint-failure refresh of a reverse
//!   lookup invalidates the cached range containing the key (not the one before it), as C++
//!   does (`getKeyLocation`, 7.4:3381, 8.0:1365).
//!
//! ## Ranges
//!
//! [`Reader::get_range_with_limits`] is NativeAPI's `Transaction::getRange` (snapshot);
//! [`Reader::get_range`] adds `fdb_c`'s parameters ([`RangeOptions`]).
//!
//! * **`fdb_c`** (`validate_and_update_parameters`, `bindings/c/fdb_c.cpp` 7.4:977-1033,
//!   8.0:1006-1062): a limit or byte target of 0 is none; `EXACT` without a limit is
//!   `exact_mode_without_limits`; the [`StreamingMode`]'s byte target (`SMALL` 256, `MEDIUM`
//!   1000, `LARGE` 4096, `SERIAL`/`WANT_ALL` 80 000 on 7.4 and 120 000 on 8.0, `ITERATOR`
//!   4096 growing by 1.5 per iteration to 120 000 from the tenth, iteration 0 being
//!   `client_invalid_operation`) replaces no byte target or lowers a larger one. The release
//!   is the cluster's (the client a multi-version C++ client would load).
//! * **`GetRangeLimits`** (`FDBTypes.h` 7.4:725-755; NativeAPI 7.4:3071-3137,
//!   8.0:1086-1137): rows, bytes and `minRows` (1). A batch costs its keys and values plus 8
//!   bytes a row; the limits are reached at 0 rows, or 0 bytes once `minRows` rows were
//!   read. `transformRangeLimits` (7.4:4517-4536, 8.0:2330-2349) asks each request for at
//!   most `REPLY_BYTE_LIMIT` (80 000) rows and bytes, or `minRows` rows when no bytes are
//!   left. A storage server (`getKeyValuesQ`/`readRange`, `storageserver.actor.cpp`
//!   7.4:4730-4950, 4273-4524; `storageserver.cpp` 8.0:3621-3830, 3178-3412) adds rows while
//!   it has bytes left (counting `sizeof(KeyValueRef)` a row), so the row that crosses the
//!   byte limit is included, and says `more` when it stopped at a limit
//!   (`limit == 0 || limitBytes <= 0`, even if nothing is left).
//! * **`getRange`** (NativeAPI 7.4:4903-5208, 8.0:2709-3026): `orEqual` selectors become
//!   `(keyAfter(key), false, offset)` and an inverted range is empty (`getRangeInternal`).
//!   A begin at `""` with offset below 1 sets `readToBegin`. Then shard by shard: locate the
//!   begin key (forward) or the end key (reverse; the shard before it if `end - 1` is
//!   backward), cut the request's selectors at the shard (`firstGreaterOrEqual` of its
//!   boundary when the selector is definitely beyond it: `modifiedSelectors`), and
//!   `loadBalance` a `GetKeyValuesRequest` (8.0 sends `taskID` `DefaultEndpoint`, 5000). The
//!   batch is finished when a limit is reached, when an unmodified request has no `more`, or
//!   once the soft byte limit has its `minRows` (a byte limit returns after the first shard
//!   that had rows); `more` is then `modifiedSelectors || reached || rep.more`, and a read
//!   that stopped at a boundary sets `readThrough` to it. Otherwise the read goes on after the
//!   last row (`firstGreaterThan(last)` forward, `firstGreaterOrEqual(last)` reverse), or at
//!   the boundary.
//! * **`getRangeFallback`** (7.4:4761-4837, 8.0:2572-2647): on `wrong_shard_server` (the
//!   server cannot resolve a selector: an end in another shard, an offset that leaves the
//!   shard), or a modified request that returned nothing, the location is invalidated and the
//!   read starts over: both selectors are resolved with `getKey` (`resolveKey`: none for
//!   `firstGreaterOrEqual`/`firstGreaterThan`), then `getExactRange` (7.4:4549-4749,
//!   8.0:2362-2560) reads the keys between, `GET_RANGE_SHARD_LIMIT` (2) shard locations at a
//!   time (`getKeyRangeLocations`: the cache if it has them all, else a ranged
//!   `GetKeyServerLocationsRequest`), with `wrong_shard_server` and `all_alternatives_failed`
//!   invalidating the rest of the range and retrying after `WRONG_SHARD_SERVER_DELAY`. It
//!   sets `readToBegin` if the begin resolved to `""` and `readThroughEnd` if the end resolved
//!   to `\xff\xff` (and nothing was cut on that side). `all_alternatives_failed` in
//!   `getRange` itself invalidates and retries the same step after the delay.
//! * **Read conflict ranges** (`getRangeFinished`, 7.4:4849-4895, 8.0:2650-2702) are the
//!   transaction's ([`Transaction::get_range`](crate::Transaction::get_range)).
//! * **Not ported:** mapped ranges (`getMappedRange`), `getRangeStream`, the tenant prefix,
//!   the version vector, tags, BUGGIFY's random truncation, and trace events.
//!   Not in C++: a forward read whose begin resolves at the end of the key space returns
//!   what it has (C++ would look up the shard of `\xff\xff`), and a reverse one whose end
//!   does reads up to `\xff\xff`. A reply with `more` but no rows, or with more rows than
//!   asked, is `internal_error` (C++ asserts).
//!
//! ## Storage metrics
//!
//! [`Reader::get_estimated_range_size_bytes`] and [`Reader::get_range_split_points`] are
//! what `Transaction::getEstimatedRangeSizeBytes` and `getRangeSplitPoints` do below the
//! read-your-writes layer (`ReadYourWrites.actor.cpp` 7.4:1853-1880,
//! `ReadYourWrites.cpp` 8.0:1840-1865, which pass them through: no read version, no conflict
//! range, not counted in `reading`). Both locate the range's shards with
//! `getKeyRangeLocations` (the cache if it has them all, checked against the failed
//! endpoints of the request's own stream, else a ranged `GetKeyServerLocationsRequest`), and
//! send each shard its part of the range, all at once (`waitForAll`). Neither reply is a
//! `LoadBalancedReply`: the queue model learns no penalty from them, and a server's error
//! (`wrong_shard_server` when it does not hold the part: `serveStorageMetricsRequests`,
//! `fdbserver/include/fdbserver/StorageMetrics.actor.h` 7.4:247-290,
//! `fdbserver/core/include/fdbserver/core/StorageMetrics.h` 8.0:314-321, and the server's
//! handlers) is the envelope's.
//!
//! * **Estimated size.** `DatabaseContext::getStorageMetrics(keys, -1)` (NativeAPI
//!   7.4:8502-8519, 8.0:6292-6309) is `getStorageMetricsLargeKeyRange` (7.4:8202-8239,
//!   8.0:5993-6025): every shard (`std::numeric_limits<int>::max()` locations), at
//!   `latestVersion` (the transaction is not passed without a tenant, 7.4:1860-1863), with
//!   the regular commit proxies, then `doGetStorageMetrics` (7.4:8172-8200,
//!   8.0:5964-5991) per shard: a `WaitMetricsRequest` with `min` zero and `max.bytes = -1`,
//!   which the server answers at once with its byte-sample estimate of the part
//!   (`StorageServerMetrics::waitMetrics`: a reply as soon as `min <= metrics <= max` does
//!   not hold). `wrong_shard_server` and `all_alternatives_failed` invalidate the part's
//!   cached locations and, after `WRONG_SHARD_SERVER_DELAY`, compute the part again as a
//!   range of its own; `future_version` does the same after `FUTURE_VERSION_RETRY_DELAY`
//!   (10 ms); any other error is returned. The parts' `bytes` are summed. There is no
//!   sampling or accuracy knob on the client: the estimate is the servers' byte sample
//!   (`BYTE_SAMPLING_*` server knobs), which a write reaches only after the server applied
//!   it.
//! * **Split points.** `getRangeSplitPoints` (7.4:8544-8606; 8.0:6376-6455, with
//!   `RangeSplitPointsBuilder` 6332-6374) asks for at most `TOO_MANY` (1 000 000) shards,
//!   sends each a `SplitRangeRequest(part, chunkSize)` (8.0 adds `limit`, -1 here: the API
//!   has no limit), and builds `begin`, then for every shard after the first its begin (a
//!   shard boundary is always a split point) and the server's points, then `end` unless it
//!   is last already. The server walks its byte sample in steps of `chunkSize`
//!   (`StorageServerMetrics::getSplitPoints`, `fdbserver/StorageMetrics.actor.cpp`
//!   7.4:544-580, `fdbserver/core/StorageMetrics.cpp` 8.0:606-644). `wrong_shard_server` and
//!   `all_alternatives_failed` invalidate the range's cached locations and start over after
//!   `WRONG_SHARD_SERVER_DELAY`; any other error (`future_version` too) is returned. The
//!   locations are asked with the transaction's read version if it has one, and of
//!   provisional proxies if it uses them.
//! * **Differences.** A boundary equal to the last point is left out (8.0's
//!   `appendShardBoundary`; 7.4 repeats it when a 7.4 server returned the shard's end as a
//!   split point, which its `> range.end` test allows), and the points continue from where
//!   the shards stopped if a location reply did not reach the end (8.0; 7.4 assumes it
//!   does), on both releases. An empty range is 0 / `[begin]` without a request (C++
//!   asserts the range is not empty), the estimate's range is cut at `\xff\xff` (no shard
//!   goes beyond it), and a `chunk_size` below 1 is `client_invalid_operation` (a storage
//!   server's walk of its byte sample may never end for a negative one). The parts' sum saturates.
//!
//! ## Read options
//!
//! A storage read request has an `Optional<ReadOptions> options` (`StorageServerInterface.h`:
//! `GetValueRequest`, `GetKeyRequest`, `GetKeyValuesRequest`, and `GetMappedKeyValuesRequest`,
//! which this client does not send; `WatchValueRequest` has none): `type` (`ReadType`),
//! `cacheResult`, `debugID`, `consistencyCheckStartVersion` and `lockAware` (`FDBTypes.h`
//! 7.4:1730-1759, 8.0:1652-1681; `type` is an `int32_t` enum on 7.4 and a `uint32_t` one on
//! 8.0, with the same values).
//!
//! * **When it is sent.** C++ keeps it in `trState->readOptions`, absent until a transaction
//!   option makes it present: `READ_PRIORITY_LOW/NORMAL/HIGH` set `type`
//!   (`ReadType::LOW`/`NORMAL`/`HIGH`), `READ_SERVER_SIDE_CACHE_ENABLE/DISABLE` set
//!   `cacheResult`, each on `readOptions.withDefault(ReadOptions())` (NativeAPI 7.4:7561-7579,
//!   8.0:5254-5272), and `LOCK_AWARE` / `READ_LOCK_AWARE` set `lockAware` (7.4:7420-7438,
//!   8.0:5124-5142). So a transaction that set any of them sends `ReadOptions()` with those
//!   changes, even when they are the defaults (`READ_PRIORITY_NORMAL` alone sends
//!   `{NORMAL, cacheResult true}`), and a transaction that set none sends none. A reset (and
//!   `on_error`) forgets them: they live in the `TransactionState` that `resetImpl` replaces
//!   (7.4:6491-6499, 8.0:4235-4243), and the options are not persistent. The database's
//!   `LOCK_AWARE` default does not set them (`TransactionOptions::reset` copies only
//!   `lockAware`).
//! * **Where.** `getValue` (7.4:3915-3959, 8.0:1706-1755), `getKey` (7.4:4041-4088,
//!   8.0:1832-1887), `getExactRange` (7.4:4603, 8.0:2407) and `getRange` (7.4:4959,
//!   8.0:2764) copy it into every request, and so do the reads of `getRangeFallback`
//!   (`getKey` and `getExactRange`). Here: [`ReadParams::read_options`], which
//!   [`Reader::get_with`], [`Reader::get_key_with`] and [`Reader::get_range_with_limits`]
//!   take, and the transaction fills from its options when a read is called.
//! * **`debugID`** is never sent: C++ sets it only for a transaction being traced
//!   (`SERVER_REQUEST_TRACING`, `debugTransaction`), which this client accepts without
//!   effect. `consistencyCheckStartVersion` is for the consistency checker.
//! * **What a storage server does with it** (`fdbserver/storageserver.actor.cpp` 7.4,
//!   `fdbserver/storageserver/storageserver.cpp` 8.0): it takes its read lock
//!   (`getReadLock`, 7.4:1660-1664, 8.0:1415-1419) at the rank of the type
//!   (`STORAGESERVER_READTYPE_PRIORITY_MAP` into `STORAGESERVER_READ_PRIORITIES`, a
//!   `PriorityMultiLock` whose higher ranks get more of the concurrent reads; a type outside
//!   the ranks is clamped), so a low priority scan yields to normal reads; `cacheResult`
//!   false asks the storage engine not to keep the pages it read; `lockAware` matters only
//!   for tenant locks.
//!
//! The messages, as this client builds them, are checked against each release's C++ reader
//! (`tools/golden-vectors` fixtures `get_value_request_read_options`,
//! `get_key_request_read_options` and `get_key_values_request_read_options`, and the
//! `cpp_crosscheck` test of `fdb-native-schema`).
//!
//! ## Deliberate differences
//!
//! * **Lagging requests** are tasks on the runtime ([`Runtime::spawn`]), one per request, where
//!   C++ keeps them in one `actorCollection`; the reader's last handle dropping cancels them.
//!   The periodic `ModelInterface` update is evaluated lazily, in order, when the model is
//!   next used, instead of by a timer.
//! * **The queue model's entries** are dropped when no cached location (or read in
//!   progress) uses their endpoint any more (C++ keeps them for the life of the model), except
//!   while a request counted in one is outstanding, which a lagging request can be after its
//!   shard was evicted.
//! * **Not modelled:** the GRV cache (`USE_GRV_CACHE`), tag throttling (no tags are sent),
//!   version vectors, TSS duplicate requests and replica consistency checks
//!   (`enableReplicaConsistencyCheck`), `connectionFileChanged` (a `switchConnectionRecord`
//!   makes C++ fail reads with `transaction_too_old`), debug ids and metrics, and the
//!   `LoadBalanceTooLong` trace.
//! * **Shutdown.** Once the [`PeerManager`] is shut down or the [`ClusterMonitor`] closed,
//!   the reader fails with `operation_cancelled` (1101) where it would otherwise wait for
//!   something that cannot happen any more: a wait for proxies (when there are none), a
//!   load balancer waiting for a failed alternative to come back, and the retry loops
//!   (checked before each attempt). The peer manager has no shutdown notification, so a
//!   parked wait notices its shutdown within a second. A request already sent is not
//!   interrupted: the transport ends it. [`Reader::shutdown`] (which
//!   [`Database::shutdown`](crate::Database::shutdown) calls) also stops the read version
//!   batchers and the lagging requests at once, even while clones of the reader are held: the
//!   read version requests waiting fail with `operation_cancelled`, and so does every later
//!   one, without starting a batcher.

use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use fdb_native_schema::Request;
use fdb_native_schema::domain::{
    GetKeyServerLocations, GetValue, KeyRange, LATEST_VERSION, StorageServer,
};
use fdb_native_transport::peer::{NoTls, PeerManager, PeerRef, StreamWrapper};
use fdb_native_transport::runtime::{Runtime, TaskHandle};
use fdb_native_wire::{Endpoint, NetworkAddress, Uid, VersionVector};
use futures_channel::oneshot;
use rustc_hash::FxHashMap;

use crate::cluster::{ClusterInfo, ClusterMonitor};
use crate::error::Error;
use crate::error::codes::*;

mod balance;
mod evictor;
mod grv;
mod key;
mod location;
mod metrics;
mod range;
pub(crate) mod util;
mod watch_lb;

#[doc(hidden)]
pub use balance::{EndpointStats, LoadBalanceStats};
pub use fdb_native_schema::domain::ReadOptions;
pub use grv::{METADATA_VERSION_KEY, Priority, ReadVersionOptions};
pub use location::KEY_SPACE_END;
pub(crate) use range::{
    Limits, REPLY_BYTE_LIMIT, is_first_greater_or_equal, key_after, read_conflict_range,
    remove_or_equal,
};
pub use range::{RangeLimits, RangeOptions, RangeRead, StreamingMode};
pub use watch_lb::WatchTimings;

use grv::{Batcher, BatcherKey, MetadataVersionCache};
use location::{Location, LocationCache};
use util::{Either, Rng, never, now_secs, race};

/// How often a parked wait checks whether the peer manager was shut down.
const SHUTDOWN_POLL: Duration = Duration::from_secs(1);

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
    /// `LOCATION_CACHE_EVICTION_SIZE` (600 000): the most shards whose location is cached
    /// ([`Reader::set_location_cache_size`] changes it).
    pub location_cache_size: usize,
    /// `WRONG_SHARD_SERVER_DELAY` (10 ms): the wait before locating a key again after
    /// `wrong_shard_server` or `all_alternatives_failed`.
    pub wrong_shard_server_delay: Duration,
    /// `METADATA_VERSION_CACHE_SIZE` (1000): how many read versions' `\xff/metadataVersion`
    /// the reader remembers (see [`Reader::metadata_version`]).
    pub metadata_version_cache_size: usize,
    /// The client's data center id (C++'s client locality `dcid`, the `DATACENTER_ID`
    /// database option): replicas in it are preferred. `None` (the default) prefers none.
    /// [`Reader::set_client_dc_id`] changes it.
    pub client_dc_id: Option<Vec<u8>>,
    /// `LOCATION_CACHE_PEER_EVICTOR_ENABLED`: whether cached locations whose storage
    /// servers keep failing to connect are forgotten periodically (see [the peer
    /// evictor](self#the-peer-evictor)). **Default `true`**, as 8.0 (ClientKnobs 8.0:213; 7.4
    /// has the same evictor off, 7.4:103), whichever release the cluster runs.
    pub location_cache_peer_evictor: bool,
    /// `LOCATION_CACHE_PEER_EVICTOR_DELAY` (60 s; ClientKnobs 7.4:104, 8.0:214): the time
    /// between two sweeps of the peer evictor.
    pub location_cache_peer_evictor_delay: Duration,
    /// `LOCATION_CACHE_PEER_EVICTOR_FAILED_THRESHOLD` (0; 7.4:105, 8.0:215): an address
    /// whose failed connects grew by more than this since the last sweep loses its cached
    /// locations.
    pub location_cache_peer_evictor_failed_threshold: u64,
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
            metadata_version_cache_size: 1000,
            client_dc_id: None,
            location_cache_peer_evictor: true,
            location_cache_peer_evictor_delay: Duration::from_secs(60),
            location_cache_peer_evictor_failed_threshold: 0,
            seed: None,
        }
    }
}

/// What a storage read carries from its transaction besides the read version: whether keys
/// are located with provisional commit proxies too (`trState->useProvisionalProxies`, the
/// transaction option `USE_PROVISIONAL_PROXIES`), and the `ReadOptions` the storage servers
/// get (`trState->readOptions`, see the [module documentation](self#read-options)).
///
/// The default is neither. A `bool` converts to the first alone, so
/// `reader.get_with(key, version, true)` still reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReadParams {
    /// Locate keys with provisional commit proxies too.
    pub use_provisional_proxies: bool,
    /// The `options` of every `GetValueRequest`, `GetKeyRequest` and `GetKeyValuesRequest`;
    /// `None` sends none (a storage server then reads as for `ReadOptions()`).
    pub read_options: Option<ReadOptions>,
}

impl ReadParams {
    /// Neither provisional proxies nor read options.
    pub fn new() -> ReadParams {
        ReadParams::default()
    }

    /// The same, with `use_provisional_proxies`.
    #[must_use]
    pub fn use_provisional_proxies(mut self, on: bool) -> ReadParams {
        self.use_provisional_proxies = on;
        self
    }

    /// The same, with `read_options`.
    #[must_use]
    pub fn read_options(mut self, read_options: Option<ReadOptions>) -> ReadParams {
        self.read_options = read_options;
        self
    }
}

impl From<bool> for ReadParams {
    /// `use_provisional_proxies` alone.
    fn from(use_provisional_proxies: bool) -> ReadParams {
        ReadParams::new().use_provisional_proxies(use_provisional_proxies)
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
    /// FxHash: looked up for each storage request (the keys are the cluster's addresses and
    /// endpoint tokens, not chosen by a caller).
    failed_endpoints: FxHashMap<(NetworkAddress, Uid), EndpointFailureInfo>,
    /// References to the peers of the proxies of the info last used, so that they are
    /// connected and probed while the client uses them.
    proxy_references: Option<(u64, Vec<PeerRef>)>,
    /// Cached ranges the peer evictor forgot.
    peer_evictions: u64,
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
    /// This, for the tasks that outlive the call that starts them (lagging requests).
    this: Weak<Shared<R, W>>,
    /// The tasks of the lagging requests of [`Shared::load_balance`].
    lagging: Mutex<balance::Lagging<R>>,
    /// `ModelInterface` of the GRV proxies: their probabilities.
    grv_model: Mutex<balance::GrvModel>,
    /// `DatabaseContext::metadataVersionCache`.
    metadata_versions: Mutex<MetadataVersionCache>,
    /// The peer evictor's task, started with the first cached location.
    evictor: Mutex<Evictor<R>>,
    /// [`Reader::shutdown`] was called: the reader references no server any more (a
    /// location or proxy list that arrives after is used, not kept). Read under the lock of
    /// what it guards, after which `shutdown` empties it.
    released: AtomicBool,
}

/// The peer evictor's task, and whether the reader was [shut down](Reader::shutdown) (it is
/// then never started again).
struct Evictor<R: Runtime> {
    task: Option<R::Task<()>>,
    stopped: bool,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Inner<R: Runtime, W> {
    shared: Arc<Shared<R, W>>,
    batchers: Mutex<Batchers<R>>,
}

/// The read version batchers, one per [`BatcherKey`] in use.
struct Batchers<R: Runtime> {
    /// FxHash: looked up for every read version request; the keys are the client's.
    running: FxHashMap<BatcherKey, Batcher<R>>,
    /// [`Reader::shutdown`] was called: no batcher starts any more.
    shut_down: bool,
}

impl<R: Runtime, W> Inner<R, W> {
    /// Stops the batchers (their waiting requests fail with `operation_cancelled`), the
    /// lagging requests and the peer evictor; with `shut_down`, for good.
    fn stop(&self, shut_down: bool) {
        let running = {
            let mut batchers = lock(&self.batchers);
            batchers.shut_down |= shut_down;
            std::mem::take(&mut batchers.running)
        };
        for (_, batcher) in running {
            batcher.task.abort();
        }
        self.shared.abort_lagging();
        let evictor = {
            let mut evictor = lock(&self.shared.evictor);
            evictor.stopped |= shut_down;
            evictor.task.take()
        };
        if let Some(task) = evictor {
            task.abort();
        }
    }
}

impl<R: Runtime, W> Drop for Inner<R, W> {
    fn drop(&mut self) {
        self.stop(false);
    }
}

/// The read path of a database: read versions, key locations and values. See the [module
/// documentation](self) for what it does and how it follows C++.
///
/// Cheap to clone (a shared handle). Create it inside the executor: it spawns a task per set
/// of read version flags in use (the batchers), which end when the last clone is dropped, or
/// at [`shutdown`](Self::shutdown).
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
        let shared = Arc::new_cyclic(|this| Shared {
            rt,
            peers,
            monitor,
            rng: Mutex::new(Rng::new(config.seed)),
            locations: Mutex::new({
                let mut cache = LocationCache::new(config.location_cache_size);
                cache.set_locality(location::ClientLocality {
                    dc_id: config.client_dc_id.clone(),
                    zone_id: None,
                });
                cache
            }),
            model: Mutex::new(balance::QueueModel::default()),
            state: Mutex::new(State {
                alternatives: AlternativesFailures {
                    oldest: f64::NEG_INFINITY,
                    newest: f64::NEG_INFINITY,
                    last_skip: f64::NEG_INFINITY,
                },
                location_backoff: 0.0,
                failed_endpoints: FxHashMap::default(),
                proxy_references: None,
                peer_evictions: 0,
            }),
            this: this.clone(),
            lagging: Mutex::new(balance::Lagging::default()),
            grv_model: Mutex::new(balance::GrvModel::default()),
            metadata_versions: Mutex::new(MetadataVersionCache::new(
                config.metadata_version_cache_size,
            )),
            released: AtomicBool::new(false),
            evictor: Mutex::new(Evictor {
                task: None,
                stopped: false,
            }),
            config,
        });
        Reader {
            inner: Arc::new(Inner {
                shared,
                batchers: Mutex::new(Batchers {
                    running: FxHashMap::default(),
                    shut_down: false,
                }),
            }),
        }
    }

    /// The configuration.
    pub fn config(&self) -> &ReadConfig {
        &self.inner.shared.config
    }

    /// Stops the reader's tasks for good, while clones of it may still be held: the read
    /// version batchers (the requests waiting for a read version fail with
    /// `operation_cancelled`, 1101), the lagging requests of the load balancer and the
    /// [peer evictor](self#the-peer-evictor). Every
    /// later read version request fails with `operation_cancelled` at once. The other reads
    /// fail with it once the peer manager is shut down or the cluster monitor closed (see the
    /// [module documentation](self)). [`Database::shutdown`](crate::Database::shutdown)
    /// calls this. Idempotent.
    ///
    /// It also lets go of the reader's [references](fdb_native_transport::peer::PeerRef) to
    /// servers: the location cache is emptied and the proxies are no longer referenced, so
    /// connections that other databases share are not kept (or probed) for this one.
    pub fn shutdown(&self) {
        self.inner.stop(true);
        // Before emptying: a location or proxy list being kept now is either emptied below
        // or sees the flag (both look under the same locks).
        self.inner
            .shared
            .released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let references = lock(&self.inner.shared.state).proxy_references.take();
        drop(references);
        let emptied = {
            let mut cache = lock(&self.inner.shared.locations);
            let capacity = cache.capacity();
            std::mem::replace(&mut *cache, LocationCache::new(capacity))
        };
        drop(emptied);
    }

    /// A read version for a default transaction (`getReadVersion`): batched with the other
    /// requests of the moment, from a GRV proxy. Fails with `database_locked` if the
    /// database is locked.
    pub async fn get_read_version(&self) -> Result<i64, Error> {
        self.get_read_version_with(ReadVersionOptions::default())
            .await
    }

    /// A read version, requested with `options`.
    pub async fn get_read_version_with(&self, options: ReadVersionOptions) -> Result<i64, Error> {
        Ok(self.get_read_version_and_metadata(options).await?.0)
    }

    /// A read version requested with `options`, and the value of `\xff/metadataVersion` at
    /// it (`None`: the key is not set), as the GRV proxy reported it
    /// (`GetReadVersionReply::metadataVersion`): what a transaction's first read of the key
    /// needs, with no read of the key (C++'s `TransactionState::metadataVersion` promise).
    /// Like [`get_read_version_with`](Self::get_read_version_with), it remembers the pair for
    /// [`metadata_version`](Self::metadata_version).
    pub async fn get_read_version_and_metadata(
        &self,
        options: ReadVersionOptions,
    ) -> Result<(i64, Option<Vec<u8>>), Error> {
        options.validate()?;
        let key = options.batcher_key();
        let (tx, rx) = oneshot::channel();
        let mut waiter = Some(tx);
        for _ in 0..2 {
            let sent = {
                let mut batchers = lock(&self.inner.batchers);
                if batchers.shut_down {
                    return Err(Error::new(OPERATION_CANCELLED));
                }
                let batcher = batchers
                    .running
                    .entry(key)
                    .or_insert_with(|| Batcher::start(self.inner.shared.clone(), key));
                match batcher
                    .sender
                    .unbounded_send(waiter.take().expect("a waiter"))
                {
                    Ok(()) => true,
                    Err(e) => {
                        // The batcher ended (it panicked): start another.
                        waiter = Some(e.into_inner());
                        if let Some(old) = batchers.running.remove(&key) {
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
            return Err(Error::new(INTERNAL_ERROR));
        }
        let reply = rx.await.map_err(|_| Error::new(OPERATION_CANCELLED))??;
        if reply.locked && !options.lock_aware {
            return Err(Error::new(DATABASE_LOCKED));
        }
        // `extractReadVersion` (NativeAPI 7.4:7830-7837, 8.0:5557-5562).
        lock(&self.inner.shared.metadata_versions)
            .insert(reply.version, reply.metadata_version.clone());
        Ok((reply.version, reply.metadata_version.clone()))
    }

    /// The value of `\xff/metadataVersion` at `version`: from what the reader has seen of
    /// read versions and commits ([`cached_metadata_version`](Self::cached_metadata_version)),
    /// else read from a storage server like any key. `None`: the key is not set.
    ///
    /// This is how C++ serves a transaction's read of the key
    /// (`Transaction::get`, NativeAPI 7.4:5860-5897): the key is the one read that is
    /// answered without a storage server whenever the transaction's read version is one the
    /// proxies reported it for.
    pub async fn metadata_version(&self, version: i64) -> Result<Option<Vec<u8>>, Error> {
        self.metadata_version_with(version, false).await
    }

    /// [`metadata_version`](Self::metadata_version), read with `params` when it has to be
    /// read ([`get_with`](Self::get_with)).
    pub async fn metadata_version_with(
        &self,
        version: i64,
        params: impl Into<ReadParams>,
    ) -> Result<Option<Vec<u8>>, Error> {
        if let Some(cached) = self.cached_metadata_version(version) {
            return Ok(cached);
        }
        self.get_with(METADATA_VERSION_KEY, version, params).await
    }

    /// The value of `\xff/metadataVersion` at `version` if the reader knows it without asking:
    /// `Some(value)` (the value itself `None` if the key is not set) if `version` is one of
    /// the last [`ReadConfig::metadata_version_cache_size`] versions a GRV proxy reply or a
    /// commit ([`note_committed_version`](Self::note_committed_version)) reported it for.
    pub fn cached_metadata_version(&self, version: i64) -> Option<Option<Vec<u8>>> {
        lock(&self.inner.shared.metadata_versions).get(version)
    }

    /// Remembers that a commit at `version` (a `CommitID` reply) left `\xff/metadataVersion`
    /// as `metadata_version`, as C++'s `tryCommit` does (NativeAPI 7.4:6990-6996): later
    /// transactions reading at exactly that version need not read the key. Only a version
    /// newer than every one remembered is kept.
    pub fn note_committed_version(&self, version: i64, metadata_version: Option<Vec<u8>>) {
        lock(&self.inner.shared.metadata_versions).insert(version, metadata_version);
    }

    /// A [`ReadSnapshot`] at a new read version.
    pub async fn snapshot(&self) -> Result<ReadSnapshot<R, W>, Error> {
        self.snapshot_with(ReadVersionOptions::default()).await
    }

    /// A [`ReadSnapshot`] at a new read version requested with `options`; its reads use
    /// provisional commit proxies if `options.use_provisional_proxies`.
    pub async fn snapshot_with(
        &self,
        options: ReadVersionOptions,
    ) -> Result<ReadSnapshot<R, W>, Error> {
        let version = self.get_read_version_with(options).await?;
        Ok(ReadSnapshot {
            reader: self.clone(),
            version,
            params: ReadParams::from(options.use_provisional_proxies),
        })
    }

    /// A [`ReadSnapshot`] at `version`.
    pub fn snapshot_at(&self, version: i64) -> ReadSnapshot<R, W> {
        ReadSnapshot {
            reader: self.clone(),
            version,
            params: ReadParams::new(),
        }
    }

    /// The value of `key` at `version` (`getValue`), `None` if the key is not set.
    ///
    /// `version` must be a read version (positive): 0 and `LATEST_VERSION` (-2) are refused
    /// with `client_invalid_operation`. `key` must be below [`KEY_SPACE_END`]
    /// (`key_outside_legal_range`); reading the system keys (`\xff...`) is not checked here
    /// (that is a transaction option).
    pub async fn get(&self, key: &[u8], version: i64) -> Result<Option<Vec<u8>>, Error> {
        self.get_with(key, version, false).await
    }

    /// [`get`](Self::get) with `params`: the key is located with provisional commit proxies
    /// too if [`use_provisional_proxies`](ReadParams::use_provisional_proxies) (the
    /// transaction option `USE_PROVISIONAL_PROXIES`), and the request carries
    /// [`read_options`](ReadParams::read_options). A `bool` is `use_provisional_proxies`.
    pub async fn get_with(
        &self,
        key: &[u8],
        version: i64,
        params: impl Into<ReadParams>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let params = params.into();
        if version <= 0 {
            return Err(Error::new(CLIENT_INVALID_OPERATION));
        }
        if key >= KEY_SPACE_END {
            return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
        }
        let shared = &self.inner.shared;
        // The location of the previous attempt stays alive until the next one is known (as
        // C++'s `state locationInfo` does), so that its servers keep their peer references
        // (and their probes) through an invalidation.
        let mut _previous: Option<Arc<Location>> = None;
        loop {
            let (_, location) = shared
                .key_location(key, version, params.use_provisional_proxies)
                .await?;
            _previous = Some(location.clone());
            let request = GetValue {
                key: key.to_vec(),
                version,
                tags: None,
                span_context: Default::default(),
                options: params.read_options,
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
    pub async fn locate(&self, key: &[u8]) -> Result<KeyLocation, Error> {
        if key >= KEY_SPACE_END {
            return Err(Error::new(KEY_OUTSIDE_LEGAL_RANGE));
        }
        let (range, location) = self
            .inner
            .shared
            .key_location(key, LATEST_VERSION, false)
            .await?;
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

    /// The bound of the location cache: [`ReadConfig::location_cache_size`] until
    /// [`set_location_cache_size`](Self::set_location_cache_size) changes it.
    pub fn location_cache_size(&self) -> usize {
        lock(&self.inner.shared.locations).capacity()
    }

    /// `LOCATION_CACHE_SIZE` (`DatabaseContext::setOption`): a new bound of the location
    /// cache, applied when the next locations are cached (nothing is evicted at once, as in
    /// C++). 0 is taken as 1.
    pub fn set_location_cache_size(&self, size: usize) {
        lock(&self.inner.shared.locations).set_capacity(size);
    }

    /// The client's data center id: [`ReadConfig::client_dc_id`] until
    /// [`set_client_dc_id`](Self::set_client_dc_id) changes it.
    pub fn client_dc_id(&self) -> Option<Vec<u8>> {
        lock(&self.inner.shared.locations).locality().dc_id.clone()
    }

    /// `DATACENTER_ID` (`DatabaseContext::setOption`): the client's data center id
    /// (`None`: none), whose replicas are preferred from now on. As in C++, the location
    /// cache and its server entries are forgotten, so that every replica's distance is
    /// computed again.
    pub fn set_client_dc_id(&self, dc_id: Option<Vec<u8>>) {
        let mut cache = lock(&self.inner.shared.locations);
        let locality = location::ClientLocality {
            dc_id,
            ..cache.locality().clone()
        };
        cache.set_locality(locality);
    }

    /// `MACHINE_ID` (`DatabaseContext::setOption`): the client's zone id. C++ uses it to
    /// find replicas on the same machine only when `LOAD_BALANCE_ZONE_ID_LOCALITY_ENABLED`
    /// is set (off by default), which this client does not offer, so it is only stored; as
    /// in C++, the location cache and its server entries are forgotten.
    pub fn set_client_zone_id(&self, zone_id: Option<Vec<u8>>) {
        let mut cache = lock(&self.inner.shared.locations);
        let locality = location::ClientLocality {
            zone_id,
            ..cache.locality().clone()
        };
        cache.set_locality(locality);
    }

    /// The client's zone id ([`set_client_zone_id`](Self::set_client_zone_id)).
    pub fn client_zone_id(&self) -> Option<Vec<u8>> {
        lock(&self.inner.shared.locations)
            .locality()
            .zone_id
            .clone()
    }

    /// What the load balancer's queue model and lagging requests hold. For tests.
    #[doc(hidden)]
    pub fn load_balance_stats(&self) -> LoadBalanceStats {
        self.inner.shared.load_balance_stats()
    }

    /// How many cached ranges the peer evictor forgot. For tests.
    #[doc(hidden)]
    pub fn peer_evictions(&self) -> u64 {
        lock(&self.inner.shared.state).peer_evictions
    }
}

/// A read version and the reader to read at it: what a read-only transaction needs.
pub struct ReadSnapshot<R: Runtime, W = NoTls> {
    reader: Reader<R, W>,
    version: i64,
    params: ReadParams,
}

impl<R: Runtime, W> Clone for ReadSnapshot<R, W> {
    fn clone(&self) -> Self {
        ReadSnapshot {
            reader: self.reader.clone(),
            version: self.version,
            params: self.params,
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

    /// What the snapshot's reads carry ([`ReadParams`]).
    pub fn params(&self) -> ReadParams {
        self.params
    }

    /// The same snapshot, whose reads send `read_options` to the storage servers.
    #[must_use]
    pub fn with_read_options(mut self, read_options: Option<ReadOptions>) -> Self {
        self.params.read_options = read_options;
        self
    }

    /// The value of `key` at the snapshot's version ([`Reader::get_with`]).
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.reader.get_with(key, self.version, self.params).await
    }
}

impl<R: Runtime, W: StreamWrapper<R::Stream>> Shared<R, W> {
    /// Keeps references to the peers of `info`'s proxies (replacing those of an older info).
    fn hold_proxy_references(&self, info: &ClusterInfo) {
        let mut state = lock(&self.state);
        if self.released.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
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
        use_provisional: bool,
    ) -> Result<(KeyRange, Arc<Location>), Error> {
        let stream = fdb_native_schema::streams::STORAGE_SERVER_STREAMS.get_value;
        self.key_location_for(key, version, use_provisional, false, stream)
            .await
    }

    /// `getKeyLocation(key, member, isBackward)`: [`key_location`](Self::key_location) for
    /// a request to the storage stream `stream` (whose endpoints are the ones checked), of the
    /// shard containing `key`, or if `reverse` the keys just before it.
    pub(crate) async fn key_location_for(
        &self,
        key: &[u8],
        version: i64,
        use_provisional: bool,
        reverse: bool,
        stream: u32,
    ) -> Result<(KeyRange, Arc<Location>), Error> {
        let cached = if reverse {
            lock(&self.locations).get_before(key)
        } else {
            lock(&self.locations).get(key)
        };
        if let Some((range, location)) = cached {
            let mut refresh = false;
            for server in &location.servers {
                let (address, token) = server.target_for(stream);
                if self.check_only_endpoint_failed(address, token) {
                    refresh = true;
                }
            }
            if !refresh {
                return Ok((range, location));
            }
            // C++ invalidates the range containing `key` here, even for a reverse lookup.
            lock(&self.locations).invalidate_key(key);
        }
        self.fetch_key_location(key, version, use_provisional, reverse)
            .await
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

    /// `getKeyLocation_internal`: the shard containing `key`, or if `reverse` the keys just
    /// before it.
    async fn fetch_key_location(
        &self,
        key: &[u8],
        version: i64,
        use_provisional: bool,
        reverse: bool,
    ) -> Result<(KeyRange, Arc<Location>), Error> {
        let results = self
            .fetch_key_locations(key, None, 100, reverse, version, use_provisional)
            .await?;
        // C++ asserts exactly one result.
        results
            .into_iter()
            .next()
            .ok_or_else(|| Error::new(INTERNAL_ERROR))
    }

    /// `getKeyLocation_internal` (`end` `None`: the shard of `begin`, the first result only)
    /// and `getKeyRangeLocations_internal` (NativeAPI 7.4:3435-3497, 8.0:1415-1491; `end`
    /// set: at most `limit` shards of `[begin, end)`, from its end if `reverse`): asks a
    /// commit proxy and caches the shards of the reply, which it returns whole (not
    /// intersected with the range).
    pub(crate) async fn fetch_key_locations(
        &self,
        begin: &[u8],
        end: Option<&[u8]>,
        limit: i32,
        reverse: bool,
        version: i64,
        use_provisional: bool,
    ) -> Result<Vec<(KeyRange, Arc<Location>)>, Error> {
        loop {
            self.check_open()?;
            let backoff = lock(&self.state).location_backoff;
            if backoff > 0.0 {
                self.rt.sleep(util::secs(backoff)).await;
            }
            let info = self
                .monitor
                .commit_proxies(use_provisional)
                .await
                .map_err(|_| Error::new(OPERATION_CANCELLED))?;
            self.hold_proxy_references(&info);
            let alternatives: Vec<Endpoint> = info
                .available_commit_proxies(use_provisional)
                .unwrap_or_default()
                .iter()
                .map(|p| p.streams.get_key_servers_locations)
                .collect();
            if alternatives.is_empty() {
                continue;
            }
            let request = GetKeyServerLocations {
                begin: begin.to_vec(),
                end: end.map(<[u8]>::to_vec),
                limit,
                reverse,
                span_context: Default::default(),
                min_tenant_version: version,
            };
            // Commit proxies are balanced by CPU (`ProxyCpuMetric`), but their replies carry no
            // `processBusyTime`, so the choice stays uniform.
            let reply = self.basic_load_balance(
                &alternatives,
                move |schema, reply, out| request.encode_request_into(schema, reply, out),
                GetKeyServerLocations::decode_reply,
                None,
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
                    // A single key's lookup caches its first result only (C++ asserts there
                    // is exactly one).
                    let take = if end.is_some() { usize::MAX } else { 1 };
                    let results = reply
                        .results
                        .into_iter()
                        .take(take)
                        .map(|(range, servers)| {
                            let location = self.cache_location(&range, &servers);
                            (range, location)
                        })
                        .collect();
                    return Ok(results);
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
        let mut cache = lock(&self.locations);
        let client_dc = cache.locality().dc_id.clone();
        let infos = servers
            .iter()
            .map(|s| {
                cache.server_info(s, client_dc.as_deref(), |endpoint| {
                    let address = self.peers.primary_address(&endpoint.addresses);
                    (address, self.peers.peer(&address).reference())
                })
            })
            .collect();
        let mut rng = lock(&self.rng);
        let location = Arc::new(Location::new(infos, &mut rng));
        if self.released.load(std::sync::atomic::Ordering::SeqCst) {
            // The reader was shut down while the location was fetched (a reply that landed
            // during `Database::shutdown`): this read uses it, the cache does not keep it, so
            // its servers are not referenced for the reader's handles that remain.
            cache.clear();
            drop(rng);
            drop(cache);
            return location;
        }
        cache.insert(range, location.clone(), &mut rng);
        drop(rng);
        drop(cache);
        self.prune();
        self.start_peer_evictor();
        location
    }

    /// Starts the peer evictor, once, if it is enabled (C++ starts it with the
    /// `DatabaseContext`; here it waits for something to evict).
    fn start_peer_evictor(&self) {
        if !self.config.location_cache_peer_evictor {
            return;
        }
        let mut slot = lock(&self.evictor);
        if slot.task.is_some() || slot.stopped {
            return;
        }
        slot.task = Some(self.rt.spawn(evictor::run(
            self.rt.clone(),
            self.monitor.clone(),
            self.this.clone(),
            self.config.location_cache_peer_evictor_delay,
            self.config.location_cache_peer_evictor_failed_threshold,
        )));
    }

    /// Drops the queue model data and endpoint failure records of servers no cached range
    /// (or read in progress) uses any more, once they could outnumber the live servers.
    fn prune(&self) {
        let bound = 2 * lock(&self.locations).server_entries() + 64;
        let endpoints = lock(&self.state).failed_endpoints.len();
        let model = lock(&self.model).len();
        // A server has an endpoint failure record per stream (`live_targets`: six), and a
        // queue model entry per load-balanced stream (`getValue`, `getKey`, `getKeyValues`,
        // `waitMetrics`, `getRangeSplitPoints`).
        if endpoints <= 6 * bound && model <= 5 * bound {
            return;
        }
        let live = lock(&self.locations).live_targets();
        lock(&self.state)
            .failed_endpoints
            .retain(|target, _| live.contains(target));
        let ids: HashSet<u64> = live.iter().map(|(_, token)| token.first).collect();
        lock(&self.model).retain(|id| ids.contains(&id));
    }

    /// `operation_cancelled` once the peer manager is shut down or the monitor closed.
    pub(crate) fn check_open(&self) -> Result<(), Error> {
        if self.peers.is_shut_down() || self.monitor.is_closed() {
            Err(Error::new(OPERATION_CANCELLED))
        } else {
            Ok(())
        }
    }

    /// Resolves once the peer manager is shut down or the monitor closed (the former is
    /// polled every [`SHUTDOWN_POLL`]).
    pub(crate) async fn closed(&self) {
        loop {
            if self.check_open().is_err() {
                return;
            }
            let monitor_closed = self.monitor.wait_for(|_| None::<()>);
            if let Either::Left(_) = race(monitor_closed, self.rt.sleep(SHUTDOWN_POLL)).await {
                return;
            }
        }
    }
}
