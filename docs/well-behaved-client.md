# Well-behaved client checklist

How this client behaves toward a FoundationDB cluster, item by item, against what the C++ client
(`libfdb_c`: NativeAPI, MonitorLeader, FlowTransport, `CLIENT_KNOBS` / `FLOW_KNOBS`) does on 7.4
and 8.0. The question for every item is: does the cluster see this client as it would see a C++
client doing the same work, and does it never get more load from it?

Audit: bead `foundationdb-rs-native-b35.4` (2026-11). The modules cited under "this client" have
the full port notes, with more citations; this page is the summary.

**Status:** **OK** (matches C++), **Fixed** (fixed by the audit), **Gap** (differs; bead filed),
**Deferred** (a feature left out of v1 on purpose, epic `hj0`; listed with what it means for the
cluster), **N/A** (C++ does it for a feature this client does not have, at no cost to the
cluster).

## Citations

`file:line` on `origin/release-7.4` (7.4) and `origin/release-8.0` (8.0) of
[apple/foundationdb](https://github.com/apple/foundationdb). The logic is the same on both
releases unless a row says otherwise (8.0 mostly turned the actors into coroutines and moved
server files).

| Short | 7.4 | 8.0 |
| --- | --- | --- |
| NativeAPI | `fdbclient/NativeAPI.actor.cpp` | `fdbclient/NativeAPI.cpp` |
| DBC | `fdbclient/NativeAPI.actor.cpp` | `fdbclient/DatabaseContext.cpp` |
| ML | `fdbclient/MonitorLeader.actor.cpp` | `fdbclient/MonitorLeader.cpp` |
| Coord | `fdbserver/Coordination.actor.cpp` | `fdbserver/coordinator/Coordination.cpp` |
| Status | `fdbserver/Status.actor.cpp` | `fdbserver/clustercontroller/Status.cpp` |
| GRVP | `fdbserver/GrvProxyServer.actor.cpp` | `fdbserver/grvproxy/GrvProxyServer.cpp` |
| LB | `fdbrpc/include/fdbrpc/LoadBalance.actor.h` | `fdbrpc/include/fdbrpc/LoadBalance.h` |
| FT | `fdbrpc/FlowTransport.actor.cpp` | `fdbrpc/FlowTransport.cpp` |
| Net2 | `flow/Net2.actor.cpp` | `flow/Net2.cpp` |
| CK | `fdbclient/ClientKnobs.cpp` | `fdbclient/ClientKnobs.cpp` |
| FK | `flow/Knobs.cpp` | `flow/Knobs.cpp` |

## Summary

| Area | Item | Status |
| --- | --- | --- |
| Coordinators | Long poll (`OpenDatabaseCoordRequest`) kept outstanding | OK |
| Coordinators | `supportedVersions`, `traceLogGroup`, `issues`, `internal` | OK |
| Coordinators | Failover, `COORDINATOR_RECONNECTION_DELAY` | OK |
| Coordinators | Cluster file: forwards, rewrite, adopting a changed file | OK |
| Client status | `status json` `cluster.clients` lists the client correctly | OK (verified live) |
| GRV | Batching (`MAX_BATCH_SIZE`, `GRV_BATCH_TIMEOUT`), `transactionCount` | OK |
| GRV | Priorities (batch, default, immediate) and their throttling | OK |
| GRV | `grv_proxy_memory_limit_exceeded`, `batch_transaction_throttled` | OK |
| GRV | GRV cache (`USE_GRV_CACHE`) | Deferred (option is a no-op) |
| Tags | Tag throttling | Deferred (tags rejected; nothing needed) |
| Backoff | `onError` backoff, `RESOURCE_CONSTRAINED_MAX_BACKOFF` | OK |
| Backoff | Location backoff on `commit_proxy_memory_limit_exceeded` | OK |
| Backoff | Read retries (`WRONG_SHARD_SERVER_DELAY`, load-balancer backoff) | OK |
| Connections | One connection per address, proxy connection limits | OK |
| Connections | Reconnect backoff, connect timeout, pings, idle close | OK |
| Connections | Failure monitor | OK |
| Connections | TLS handshake throttle (`TLS_CLIENT_CONNECTION_THROTTLE_*`) | **Fixed** |
| Connections | Several `Database`s on one cluster share their connections | **Fixed** (b35.7) |
| Location cache | Size, eviction, endpoint-failure refresh | OK |
| Location cache | Peer evictor for dead servers (8.0 default on) | **Fixed** (b35.6; on for both releases) |
| Reads | `READ_PRIORITY_*` / `READ_SERVER_SIDE_CACHE_*` sent to storage | **Fixed** (b35.8) |
| Errors | `process_behind`, `future_version`, `database_locked`, ... | OK |
| Errors | `cluster_version_changed` | N/A (never produced) |
| Protocol | Version announcement, incompatible servers | OK |
| Protocol | Upgrades (single library, two protocols) | OK (documented below) |
| Idempotency | Automatic idempotency ids | Deferred (off by default in C++) |
| Load | Request coalescing (GRV, watches; locations are not coalesced in C++ either) | OK |
| Load | Background traffic (client profiling, cache list, metrics) | N/A (none sent) |

## Coordinators

| Item | C++ | This client | Status |
| --- | --- | --- | --- |
| The long poll | `monitorProxies` / `monitorProxiesOneGeneration` (ML 7.4:890-1055, 8.0:978-1142): one `OpenDatabaseCoordRequest` at a time to one coordinator, `knownClientInfoID` = the info it has; the coordinator holds it until the info changes or `CLIENT_REGISTER_INTERVAL` (600 s) passes (Coord 7.4:220-272, 8.0:203-255), and on a reply the client asks the same coordinator again. | `cluster::ClusterMonitor` (`crates/fdb-native-client/src/cluster/monitor.rs`), the same loop. The sim test `load::the_coordinator_long_poll_is_one_outstanding_request` checks that an idle client sends 2 requests in 10 minutes (the first, answered at once, and the one held) and one more per new info. | OK |
| What the request carries | `supportedVersions` (the multi-version client's libraries), `traceLogGroup` (`"default"`, NativeAPI 7.4:167, 8.0:134), `issues`, `internal = false` (ML 7.4:921-931). The coordinator records them for `status` only while the request is held, and only when `supportedVersions` is non-empty and `internal` is false (Coord 7.4:231-234 and 257-259, 8.0:214-217). | `ClusterMonitorConfig` (`cluster/config.rs`): one `ClientVersion` per schema set (`"7.4.0 (fdb-native-client <ver>)"` and `"8.0.0 (...)"`, source `"fdb-native-client"`, protocol in `%llx` form), `trace_log_group` (`"default"`, settable through `DatabaseOptions::monitor`), `issues`, `internal = false`. | OK |
| Failover | On an error, the next coordinator (shuffled order); once every one failed in turn, sleep `COORDINATOR_RECONNECTION_DELAY` (1 s, CK 7.4:52, 8.0:157) (ML 7.4:1019-1030). | Same (`cluster/monitor.rs`; test `cluster_monitor::reconnection_delay_after_all_failed`). Dead coordinators are found by the transport's connect and ping timeouts, as in C++. | OK |
| Cluster file | Forward: follow the new connection string, persist it once a new coordinator answers (`setAndPersistConnectionString`). Before every request compare the file with the string in memory: adopt the file if every coordinator just failed, else report `incorrect_cluster_file_contents` (ML 7.4:933-1018). | Same (`cluster/record.rs`, `cluster/monitor.rs`; tests `forward_moves_to_new_coordinators_and_rewrites_the_cluster_file`, `cluster_file_changes_are_reported_then_adopted`). `switchConnectionRecord` is not offered. | OK |

## Client status (`status json`)

The cluster controller builds `cluster.clients` from what the coordinators recorded
(`ClientData::getRequest`, ML 7.4:680-705, 8.0:767-790; `clientStatusFetcher`, Status
7.4:1160-1226, 8.0:1099-1165): per `ClientVersionRef`, the count, the clients (address and log
group, up to `CLIENT_EXAMPLE_AMOUNT` = 20, CK 7.4:53), and per client its highest protocol
version (`max_protocol_clients`), which is what upgrade checks (such as the Kubernetes operator's)
look at.

Verified on testkit containers (7.4.10 and 8.0.1) by
`crates/fdb-native-client/tests/docker_well_behaved.rs`: while a `Database` is open, `status json`
lists it under both of its versions, with the log group it was given, and under
`max_protocol_clients` of the 8.0 protocol. On a 7.4.10 cluster:

```json
{ "client_version": "7.4.0 (fdb-native-client 0.0.0)", "protocol_version": "fdb00b074000000",
  "source_version": "fdb-native-client", "count": 1,
  "connected_clients": [ { "address": "172.17.0.1:54726", "log_group": "fdb-native-audit" } ] },
{ "client_version": "8.0.0 (fdb-native-client 0.0.0)", "protocol_version": "fdb00b080000000",
  "source_version": "fdb-native-client", "count": 1,
  "connected_clients":    [ { "address": "172.17.0.1:54726", "log_group": "fdb-native-audit" } ],
  "max_protocol_clients": [ { "address": "172.17.0.1:54726", "log_group": "fdb-native-audit" } ] }
```

Reporting both versions is accurate: this one library speaks both protocols (see
[Protocol](#protocol-and-upgrades)), as a multi-version C++ client with a 7.4 and an 8.0 library
loaded would report both. It reports nothing newer, so an upgrade check for a release after 8.0
correctly lists it as not ready. The address is the client's socket address as the coordinator
sees it, as for C++ clients. Status: **OK**.

## Read versions (GRV)

| Item | C++ | This client | Status |
| --- | --- | --- | --- |
| Batching | `readVersionBatcher` (NativeAPI 7.4:7671-7761, 8.0:5383-5482): one batcher per request flags (the priority is in the flags; 8.0 adds `maxGrvQueueDelayMS`), a batch goes out at `MAX_BATCH_SIZE` (1000) or `batchTime` after its first request, `batchTime` = smoothed half reply latency capped at `GRV_BATCH_TIMEOUT` (5 ms) (CK 7.4:94-95, 8.0:204-205). One `GetReadVersionRequest` per batch with `transactionCount` = the batch size, which is what the GRV proxy and ratekeeper count as transactions started. | `read/grv.rs`, the same batcher. `delay(batchTime)` is precise in flow; a `batchTime` below the runtime's `TIMER_RESOLUTION` (tokio: 1 ms, where `sleep(20us)` takes 1.05 ms) is waited out on the clock, yielding between polls, so batches collect what C++'s would without a millisecond per transaction (`BatchDelay`). Sim test `load::read_versions_are_batched_and_every_transaction_is_counted`: 200 transactions started together cost at most 20 requests, whose `transactionCount`s add up to exactly 200; `read_path::grv_requests_arriving_during_the_batch_time_join_the_batch`: the same on tokio's real clock for 200 requests arriving over 4 ms with `batchTime` below 1 ms. A write-only transaction gets its read version at commit with `CAUSAL_READ_RISKY`, through the batcher, as `commitMutations` does. | OK |
| Priorities and ratekeeper | Ratekeeper throttles by delaying GRV replies at the proxy (default and batch queues); batch-priority requests get `batch_transaction_throttled` when the batch rate is too low (GRVP 7.4:578-583, 8.0:630-635). Immediate priority is never throttled. | The same requests with the same priority bits (`ReadVersionOptions`, `PRIORITY_BATCH` / `PRIORITY_SYSTEM_IMMEDIATE` options), so the proxy throttles this client like a C++ one; nothing bypasses the GRV path except `set_read_version`, which C++ has too. | OK |
| Errors | `grv_proxy_memory_limit_exceeded` (GRVP 7.4:476, 8.0:468), `batch_transaction_throttled`, and on 7.4 `proxy_tag_throttled` (tagged requests only) are returned to `onError` (NativeAPI 7.4:7662-7666). 8.0's `transaction_grv_queue_rejected` (GRVP 8.0:491-503) is only sent for requests with `maxGrvQueueDelayMS`, and `onError` does not retry it. | Same codes, same handling (`error.rs`); `MaxGrvQueueDelay` is supported on 8.0 and the error is not retried, as in C++. | OK |
| Backoff on throttling | `getBackoff` (NativeAPI 7.4:6409-6444, 8.0:4152-4187): a random fraction of the backoff, which doubles up to `RESOURCE_CONSTRAINED_MAX_BACKOFF` (30 s) for `grv_proxy_memory_limit_exceeded`, `commit_proxy_memory_limit_exceeded`, `transaction_throttled_hot_shard`, `transaction_rejected_range_locked`, and up to the max retry delay (`DEFAULT_MAX_BACKOFF`, 1 s) for the others, `batch_transaction_throttled` included. | Same (`Transaction::on_error`, `Error::is_resource_constrained`). Sim tests `load::a_throttled_grv_proxy_sees_the_resource_constrained_backoff` and `..._commit_proxy_...`: a retry loop against a cluster throttled for 2 minutes sends 5 to 40 requests in that time (a 1 s cap would make it more than 200; checked by breaking the rule). | OK |
| GRV cache | `USE_GRV_CACHE` (NativeAPI 7.4:7494-7500) reads a cached version kept fresh by `backgroundGrvUpdater` (DBC 7.4:1490), off by default (`DEBUG_USE_GRV_CACHE_CHANCE` -1, CK 7.4:140-142). It lowers GRV load when enabled. | Not implemented; `UseGrvCache` is accepted as a no-op, so every transaction asks the proxies, as a C++ client without the option does. | Deferred |

## Tags

C++ keeps tag throttles on the client only for transactions with tags: the GRV reply's
`tagThrottleInfo` updates `throttledTags` for the request's tags (NativeAPI 7.4:7622-7647, 8.0
around 5344), and a tagged transaction fails with `tag_throttled` before or after its GRV
(7.4:7815 and 7920, 8.0:5542 and 5720), with a backoff of up to `TAG_THROTTLE_RECHECK_INTERVAL`
(`getBackoff`, 7.4:6412-6428). The GRV proxies' own tag throttler (`proxy_tag_throttled`,
`GrvProxyTagThrottler.actor.cpp` 7.4:57) only queues requests that carry tags (`isTagged()`,
GRVP 7.4:566 and 584, 8.0:821).

This client rejects the `Tag` / `AutoThrottleTag` options (`invalid_option`) and sends no tags in
GRV, read or commit requests, so no tag throttle ever applies to it and the cluster expects
nothing from it: untagged requests are throttled only by ratekeeper's priority rates, which apply
(above). `ExpensiveClearCostEstimationEnable` only refines tag costs and is a no-op. Status:
**Deferred** (tags are not in v1); nothing is owed to the cluster.

## Backoff and limits

| Item | C++ | This client | Status |
| --- | --- | --- | --- |
| `onError` | Retry with `getBackoff` for the retryable codes; `FUTURE_VERSION_RETRY_DELAY` (10 ms) for `future_version` / `transaction_too_old` (NativeAPI 7.4:8112-8167, 8.0:5908-5958). Knobs `DEFAULT_BACKOFF` 10 ms, `DEFAULT_MAX_BACKOFF` 1 s, `BACKOFF_GROWTH_RATE` 2, `RESOURCE_CONSTRAINED_MAX_BACKOFF` 30 s (CK 7.4:70-73, 8.0:175-178). | `Transaction::on_error`, `Error::transaction_retry` (`transaction.rs`, `error.rs`); the knobs are `CommitterConfig`'s. | OK |
| Key locations | `getKeyLocation_internal` waits the database's backoff before each request; `commit_proxy_memory_limit_exceeded` grows it from `DEFAULT_BACKOFF` up to 30 s, a success halves it (`updateBackoff`, NativeAPI 7.4:3268-3325 and 3404-3433). | Same (`read/mod.rs`, `location_backoff`). | OK |
| Reads | `loadBalance` (LB 7.4:686-1022, 8.0:307-620): `wrong_shard_server` / `all_alternatives_failed` invalidate and wait `WRONG_SHARD_SERVER_DELAY` (10 ms); `server_overloaded`, `process_behind` and lost connections try the next replica with backoff 10 ms doubling to 5 s once all were tried; `future_version` backs a replica off 1 s doubling; hedged second requests limited by a budget. | Ported in full (`read/balance.rs`; see `read/mod.rs`). | OK |
| Watches | One storage watch per key shared by every watcher (`watchMap`), `MAX_WATCHES` (10 000), `WATCH_POLLING_TIME` (1 s, CK 7.4:129, 8.0:237), `waitForCommittedVersion` with `GRV_ERROR_RETRY_DELAY` (5 s). | Ported (`watch.rs`, `read/watch_lb.rs`). | OK |

## Connections

| Item | C++ | This client | Status |
| --- | --- | --- | --- |
| Connections per server | One per address in the process (`FlowTransport::getOrOpenPeer`). At most `MAX_COMMIT_PROXY_CONNECTIONS` (5) commit and `MAX_GRV_PROXY_CONNECTIONS` (3) GRV proxies, a sample kept while the list is unchanged (`shrinkProxyList`, ML 7.4:800-888; CK 7.4:55-57, 8.0:160-162). | One per address, the same proxy sampling. **Was** one per address per `Database`: a process that opened N databases of a cluster (instead of cloning one handle) had N connections to every server, each pinged every second, and N failure monitors. Now the databases of a process share one `PeerManager` (connections and failure monitor) when they run on the same runtime (`Runtime::same_runtime`) with the same `DatabaseOptions::peers` and the same TLS material, through a registry of weak references (`database/connections.rs`; see the client's `database` module, "Sharing connections"); each database keeps its own long poll, proxies, location cache, peer evictor, GRV batchers, watches and options, as each C++ `DatabaseContext` does. The sharing is counted: `Database::shutdown` of one ends only its own requests (`PeerManager::share`, transport `peer` module, "Sharing": the peers' tasks refuse its queued commands as not sent and cancel its requests in flight, with no cost on a request's path), and the connections close with the last database. The per-connection limits (command queue, requests in flight) are shared too, as in C++. `DatabaseOptions::share_connections = false` opts out. Tests: `fdb-native-transport` `tests/peer_share.rs`, `fdb-native-client` `tests/sharing.rs` (two databases, one connection to the coordinator and to each proxy; one shut down while both commit: the other's commit completes, its own is `commit_unknown_result`; each TLS setting alone keeps managers apart), sim test `lifecycle::two_databases_share_connections_and_the_last_one_ends_everything` and a third of the seed sweep's seeds (two databases, the first shut down while the second keeps working, no task left), `tests/docker_sharing.rs` (7.4.10 and 8.0.1). | **Fixed** |
| Reconnects | `connectionKeeper` (FT 7.4:760-1100, 8.0:886-1190): connect bounded by `CONNECTION_MONITOR_TIMEOUT` (2 s), reconnect delay 50 ms growing ×1.2 to 500 ms, reset after 5 s, a failed referenced peer probed every `SERVER_REQUEST_INTERVAL` (1 s) (FK 7.4:108-122, 8.0:111-125). | Same (`fdb-native-transport` `peer` module, "Reconnecting"). | OK |
| Pings and idle close | `connectionMonitor` (FT 7.4:616-698, 8.0:725-819): ping every `CONNECTION_MONITOR_LOOP_TIME` (1 s), fail after `CONNECTION_MONITOR_TIMEOUT` of silence, close idle connections after 180 s and unreferenced ones after 2 s. | Same (`connection` module, "Ping and liveness"). | OK |
| Failure monitor | Clients use `SimpleFailureMonitor` fed by the transport (no `failureMonitorClient` in 7.4/8.0 clients): an address is failed after `FAILURE_DETECTION_DELAY` (4 s) of failed connects; endpoints fail on `broken_promise` / not found (FT 7.4:995-1001). | Same (`peer::FailureMonitor`). | OK |
| TLS handshake throttle | After a failed client handshake with an `ip:port`, a second failure within `TLS_CLIENT_CONNECTION_THROTTLE_TIMEOUT` (11 s) makes later connects to it wait `CONNECTION_MONITOR_TIMEOUT` and fail without dialing until the window ends (`TLS_CLIENT_CONNECTION_THROTTLE_ATTEMPTS` = 1; Net2 7.4:927-946, 1194-1226, 1342-1353; 8.0:919-939, 1160-1197; FK 7.4:137-139, 8.0:140-142). Handshake timeouts do not count in practice (the outer connect timeout cancels them first). | **Was missing**: a client whose certificate check failed re-handshook every reconnect (up to twice a second per address). Now `peer/tls_throttle.rs`, `PeerManagerConfig::tls_throttle_timeout` / `tls_throttle_attempts`, `PeerError::TlsThrottled`; tests `peer::repeated_tls_handshake_failures_throttle_dialing_like_a_cpp_client`, `peer::tls_handshake_timeouts_do_not_throttle` and unit tests. | **Fixed** |

## Location cache

| Item | C++ | This client | Status |
| --- | --- | --- | --- |
| Size and eviction | `LOCATION_CACHE_EVICTION_SIZE` 600 000 ranges, random eviction before an insert (CK 7.4:99, 8.0:209). | Same (`ReadConfig::location_cache_size`, `read/location.rs`). | OK |
| Failed endpoints | A cached location whose endpoint failed on a healthy server is refreshed, for `LOCATION_CACHE_ENDPOINT_FAILURE_GRACE_PERIOD` (60 s) and then every `LOCATION_CACHE_FAILED_ENDPOINT_RETRY_INTERVAL` (60 s), so a dead role does not make every read ask a proxy (NativeAPI 7.4:3329-3355; CK 7.4:101-102, 8.0:211-212). | Same (`read/mod.rs`, "Key locations"). | OK |
| Dead servers | Cached interfaces hold peer references, so their servers are probed (every `SERVER_REQUEST_INTERVAL`, 1 s) while cached. 8.0 turns on `locationCachePeerEvictorActor` (`LOCATION_CACHE_PEER_EVICTOR_ENABLED`: false on 7.4, CK 7.4:103, the same code in NativeAPI 7.4:1169-1362; **true on 8.0**, CK 8.0:213-216; DBC 8.0:1144-1336, started at 1414-1416): every `LOCATION_CACHE_PEER_EVICTOR_DELAY` (60 s) it reads FlowTransport's per-address count of failed connects (`persistentConnectFailedCount`, FT 7.4:886-923, 8.0:980-1016, kept while an address fails at least once per `PERSISTENT_CONNECT_FAILED_COUNT_TTL`, 600 s, FK 7.4:56, 8.0:61) and invalidates every cached range with a replica at an address whose count grew by more than `..._FAILED_THRESHOLD` (0) since the last sweep, which drops the peer references and stops the probes; the next read of such a range looks it up again. | **Was missing** (7.4's behaviour: a server gone for good was probed every second while a cached shard named it). Now the same as 8.0: the transport counts failed dials per address (`FailureMonitor::connect_failures`, `PeerManagerConfig::connect_failed_count_ttl`), and the reader runs the evictor (`read/evictor.rs`, `LocationCache::invalidate_addresses`; `ReadConfig::location_cache_peer_evictor`, `_delay`, `_failed_threshold`). **On by default for 7.4 clusters too** (a client-side choice: it spares a 7.4 cluster the same probes, for one location lookup per evicted range; `false` restores 7.4's behaviour). As in 8.0, a dead server the shard map still lists is rediscovered (and found failed again) after each sweep that evicts it. Sim tests `peer_evictor::*` (a removed storage server, killed or powered off, is dialed 5 to 16 times in its first 20 s, never after one sweep, and the next reads of its shards make fresh lookups and never dial it; without the evictor it is probed for as long as the client runs); the seed sweep runs half its seeds with the evictor. | **Fixed** |

## Reads

| Item | C++ | This client | Status |
| --- | --- | --- | --- |
| Read options | `READ_PRIORITY_LOW/NORMAL/HIGH` and `READ_SERVER_SIDE_CACHE_ENABLE/DISABLE` set `type` and `cacheResult` of `trState->readOptions.withDefault(ReadOptions())` (NativeAPI 7.4:7561-7579, 8.0:5254-5272), and `LOCK_AWARE` / `READ_LOCK_AWARE` its `lockAware` (7.4:7420-7438, 8.0:5124-5142); it is absent until one of them is set, and a reset drops it. `getValue` (7.4:3915-3959, 8.0:1706-1755), `getKey` (7.4:4041-4088, 8.0:1832-1887), `getRange` (7.4:4959, 8.0:2764) and `getExactRange` (7.4:4603, 8.0:2407) put it in every request (`WatchValueRequest` has no options: a watch sends only the debug id). A storage server takes its read lock at the type's rank (`getReadLock`, `fdbserver/storageserver.actor.cpp` 7.4:1660-1664, `fdbserver/storageserver/storageserver.cpp` 8.0:1415-1419). | **Was missing**: the options were accepted but every request went out with `options` absent, so a scan marked low priority competed with foreground reads at normal priority, and a cache-disable hint was lost. Now the same: `ReadParams::read_options` (`read/mod.rs`, "Read options"), filled from the transaction's options when a read is called, sent on `GetValueRequest`, `GetKeyRequest` and `GetKeyValuesRequest` on both releases; no `debugID` (`SERVER_REQUEST_TRACING` is a no-op here). Wire safety: golden vectors `get_value_request_read_options`, `get_key_request_read_options`, `get_key_values_request_read_options` written by each release's C++, and `cpp_crosscheck_release_readers_decode_client_read_options` (every type, cache and lock-aware combination, decoded by the 7.4 and 8.0 readers). Tests: `tests/read_options.rs` (fake cluster: which options each request carries), `tests/docker_read_options.rs` (7.4.10 and 8.0.1: reads with each option). | **Fixed** |

## Errors

| Error | C++ | This client | Status |
| --- | --- | --- | --- |
| `process_behind` (1037) | Storage: the load balancer tries other replicas, then returns it (LB 7.4:594-620); `onError` retries with backoff. | Same (`read/balance.rs`, `error.rs`). | OK |
| `future_version` (1009) | Load balancer backs the replica off; `onError` retries after `FUTURE_VERSION_RETRY_DELAY` with a new read version. | Same. | OK |
| `grv_proxy_memory_limit_exceeded` (1078), `commit_proxy_memory_limit_exceeded` (1042) | `onError` with the resource-constrained backoff; key location requests eat 1042 with the database backoff. | Same; sim tests above. | OK |
| `batch_transaction_throttled` (1051) | `onError` with the normal backoff (API ≥ 630). | Same. | OK |
| `tag_throttled` (1213), `proxy_tag_throttled` (1223, 7.4) | Tagged transactions only. | Never received (no tags); retried by `onError` like C++ if it were. | OK |
| `database_locked` (1038) | `extractReadVersion` throws it for a locked database unless lock aware (NativeAPI 7.4:7790, 8.0:5517); commit proxies too; `onError` retries with backoff. | Same (`read/grv.rs`, `LockAware` / `ReadLockAware`). | OK |
| `cluster_version_changed` (1039) | Produced by the multi-version client when the cluster's protocol changes under an operation (`fdbclient/include/fdbclient/MultiVersionAssignmentVars.h` 7.4:73; handled in `MultiVersionTransaction.actor.cpp` 7.4:1741-1750); `fdb_error_predicate` says maybe committed; native `onError` rethrows it. | Never produced: a protocol change shows up as lost connections (`request_maybe_delivered` for reads, retried; `commit_unknown_result` for a commit in flight, which is retried as maybe committed). Predicates kept for API parity. | N/A |

## Protocol and upgrades

| Item | C++ | This client | Status |
| --- | --- | --- | --- |
| Announcement | The ConnectPacket carries the client's protocol version; a server accepts only a compatible one (`0xFFFF_FFFF_FFFF_0000` mask), and keeps an incompatible multi-version-capable client connected for pings and `ProtocolInfoRequest` only (FT 7.4:1560-1607, 8.0:1692-1739). `libfdb_c` learns the cluster's version from that packet (`getCoordinatorProtocolFromConnectPacket`, NativeAPI 7.4:7981, 8.0:5785) and loads the matching library. | The transport announces the version last seen (else 7.4), and on an incompatible but supported version (7.4 or 8.0) reconnects at once announcing it; an unsupported version fails with `PeerError::UnsupportedVersion` and is retried with the normal reconnect backoff. Connect packets otherwise match a C++ client's (`canonicalRemotePort` 0). See `fdb-native-transport` `connection` and `peer` ("Version detection"). | OK |
| Upgrade 7.4 → 8.0 | The multi-version client keeps a connection per protocol to the coordinators, switches libraries when the cluster's protocol changes, and fails in-flight work with `cluster_version_changed`. | One library speaks both: when the servers restart on 8.0, each reconnect is told the new version and switches (one extra connection attempt per address), the coordinator long poll then receives 8.0 `ClientDBInfo`s, and transactions retry through lost connections as for any recovery. `status json` already listed the client as 8.0-capable (`max_protocol_clients`). | OK |
| Upgrade to a release after 8.0 | A multi-version client without a library for it waits, reporting the incompatible connection. | `UnsupportedVersion` for every server: the client cannot talk to the cluster, operations wait (until their timeout) and the coordinators are retried at the normal pace (about one attempt per coordinator per second). `status json`'s upgrade checks see that the client's highest protocol is 8.0's, so the upgrade is flagged before it happens. Upgrade the client library first. | OK (documented) |

## Idempotency ids

Automatic idempotency (`AUTOMATIC_IDEMPOTENCY`, NativeAPI 7.4:7545-7558, 8.0:5238) is off by
default in C++. When on, the client must clean up after itself: after a successful commit it
sends `expireIdempotencyId` to the commit proxy (7.4:7023-7028, 8.0:4710), otherwise the ids stay
in the database until the cleaner's `IDEMPOTENCY_IDS_MIN_AGE_SECONDS` (one week,
`fdbclient/ServerKnobs.cpp` 7.4:1428). This client offers neither `IDEMPOTENCY_ID` nor
`AUTOMATIC_IDEMPOTENCY` (epic `hj0`); `commit_unknown_result` is settled with
`commitDummyTransaction`, as a C++ client without ids does. When ids are added, the expiry
message is part of being well behaved. Status: **Deferred**.

## Load

| Item | C++ | This client | Status |
| --- | --- | --- | --- |
| GRV coalescing | Batched (above). | Batched. | OK |
| Key locations | Not coalesced: every cache miss sends its own `GetKeyServerLocationsRequest` (NativeAPI 7.4:3268-3325); a range lookup asks for up to `GET_RANGE_SHARD_LIMIT` shards at once. | The same requests: concurrent misses on one shard each ask, as in C++. | OK |
| Watches | Coalesced per key (`watchMap`). | Coalesced per key. | OK |
| Hedged reads | Second requests within a budget (`SECOND_REQUEST_*`). | Same budget, so no more duplicate reads than C++. | OK |
| Background traffic | `clientStatusUpdateActor` writes sampled transaction profiles to `\xff\x02/fdbClientInfo/` (off unless a sample rate is configured; NativeAPI 7.4:849-960, DBC 8.0:730), `monitorCacheList` reads the storage cache list only when caches exist (7.4:1367), `databaseLogger` only traces. | None of these: no background requests at all besides the coordinator long poll, pings, and probes of referenced failed peers. | N/A |

## Fixes and tests from the audit

* **TLS handshake throttle** (`fdb-native-transport`): `peer/tls_throttle.rs`, wired into
  `peer/task.rs` `dial`; `PeerManagerConfig::tls_throttle_timeout` (11 s) and
  `tls_throttle_attempts` (1); `PeerError::TlsThrottled`. Tests in `tests/peer.rs` and the module.
* **Status check** (`fdb-native-client/tests/docker_well_behaved.rs`, needs docker): the
  `cluster.clients` section of `status json` lists the client, its versions, protocol versions
  and log group. `FdbContainer::status_json` was added to the testkit for it.
* **Load regression tests** (`fdb-native-sim/tests/load.rs`, deterministic): the long poll is
  one outstanding request; GRV batching and `transactionCount` accounting; the
  resource-constrained backoff against throttled GRV and commit proxies. The model cluster now
  counts `open_database_requests`, `grv_requests`, `grv_transactions` and `location_requests`.
* **Read options** (b35.8, `fdb-native-client` `read` and `transaction`): see the Reads
  row. `ReadParams` carries them through the read path; `ReadSnapshot::with_read_options`
  for readers.
* **Location cache peer evictor** (b35.6, `fdb-native-transport` failure monitor and
  `fdb-native-client` `read/evictor.rs`): see the Location cache row. The model cluster can
  remove a storage server for good (`SimCluster::remove_storage`) and the simulated network
  counts dials per address (`Sim::dials`).
