//! The coordinator connection: the long poll that keeps a client's `ClientDBInfo` (its GRV and
//! commit proxies) current, and the derived proxy endpoints.
//!
//! [`ClusterMonitor`] is C++'s `monitorProxies`. It is started over a
//! [`PeerManager`](fdb_native_transport::peer::PeerManager) with a [`ConnectionRecord`] (the
//! connection string and, optionally, the cluster file it came from), and publishes each
//! `ClientDBInfo` a coordinator sends as a [`ClusterInfo`]: the info as received, the
//! [`SchemaVersion`](fdb_native_schema::SchemaVersion) it was decoded with, and the proxies the
//! client should use with all their endpoints derived for that version.
//!
//! ```no_run
//! use fdb_native_client::{ClusterMonitor, ClusterMonitorConfig, ConnectionRecord};
//! use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig};
//! use fdb_native_transport::runtime::TokioRuntime;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let rt = TokioRuntime::new();
//! let peers = PeerManager::new(rt.clone(), PeerManagerConfig::default(), DiscardEvents);
//! let record = ConnectionRecord::open("/etc/foundationdb/fdb.cluster")?;
//! let monitor = ClusterMonitor::start(rt, peers.clone(), record, ClusterMonitorConfig::default());
//!
//! // The first info with usable proxies (this waits out recoveries).
//! let info = monitor.proxies(false).await?;
//! println!("cluster speaks {}: {} GRV proxies", info.schema, info.grv_proxies.len());
//! let grv = info.grv_proxies[0].streams.get_consistent_read_version;
//! let peer = peers.peer_for_endpoint(&grv);
//! // peer.request(grv.token, |reply, version, out| /* GetReadVersion, encoded for version */)
//!
//! monitor.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! # What C++ does
//!
//! Citations are `file:line` on `origin/release-7.4` (7.4) and `origin/release-8.0` (8.0) of
//! the FoundationDB repository. `ML` is `fdbclient/MonitorLeader.actor.cpp` (7.4) /
//! `fdbclient/MonitorLeader.cpp` (8.0); `Coord` is `fdbserver/Coordination.actor.cpp` (7.4) /
//! `fdbserver/coordinator/Coordination.cpp` (8.0); `NativeAPI` is
//! `fdbclient/NativeAPI.actor.cpp` / `NativeAPI.cpp`; `CCF` is
//! `fdbclient/ClusterConnectionFile.actor.cpp` / `ClusterConnectionFile.cpp`; `ClientKnobs`
//! and `ServerKnobs` are `fdbclient/ClientKnobs.cpp` and `fdbclient/ServerKnobs.cpp` (7.4) /
//! `fdbserver/core/ServerKnobs.cpp` (8.0). The logic is **the same on both releases** (8.0
//! only converted the actors to coroutines), except for one knob default noted below.
//!
//! ## The client: `monitorProxies` / `monitorProxiesOneGeneration`
//!
//! `Database::createDatabase` starts `monitorProxies(connRecord, clientInfo, coordinator,
//! networkOptions.supportedVersions, traceLogGroup, internal)` (NativeAPI 7.4:2633; 8.0:572).
//! `monitorProxies` (ML 7.4:1034-1055; 8.0:1122-1142) runs one *generation* after another;
//! a generation (`monitorProxiesOneGeneration`, ML 7.4:890-1032; 8.0:978-1120) talks to the
//! coordinators of one connection string and ends when the cluster moves to another.
//!
//! 1. **Coordinators.** One `ClientLeaderRegInterface` per hostname and per address of the
//!    connection string, **shuffled** (`randomShuffle`, 7.4:919; 8.0:1007). `index` (the one
//!    asked) and `successIndex` (the last one that answered) start at 0.
//! 2. **The request** (7.4:921-931; 8.0:1009-1019): an `OpenDatabaseCoordRequest` with
//!    `clusterKey = description:ID`, `hostnames` and `coordinators` from the connection string,
//!    `knownClientInfoID` = the id of the `ClientDBInfo` the client has (`UID()`, zero, at
//!    first), `supportedVersions` (empty unless the `SUPPORTED_CLIENT_VERSIONS` network option
//!    was set, which the multi-version client always does: one `ClientVersionRef` per client
//!    library, parsed from `"<version>,<source version>,<protocol version as %llx>"`;
//!    `fdbclient/ClientVersion.h` 7.4:27-50, `ThreadSafeApi::getClientVersion`,
//!    `fdbclient/ThreadSafeTransaction.cpp` 7.4:805-809), `traceLogGroup` (default
//!    `"default"`, NativeAPI 7.4:167; 8.0:134), `issues` (below) and `internal` (`false` for
//!    `fdb_c` databases). The coordinator records `supportedVersions`, `issues` and
//!    `traceLogGroup` for `status` while the request waits, only when `supportedVersions` is
//!    non-empty and `internal` is false (Coord 7.4:231-234; 8.0:214-217).
//! 3. **Cluster file check** (7.4:933-962; 8.0:1021-1050), only for a file record:
//!    `upToDate` reads the file and compares its connection string with the one in memory
//!    (CCF 7.4:71-86). If they differ and every coordinator has just failed
//!    (`allConnectionsFailed`) and the file's string has coordinators, the client adopts the
//!    file (`setAndPersistConnectionString`) and starts a new generation: "it can be that all
//!    the coordinators have changed, but the client missed that". Otherwise the request
//!    carries the issue `incorrect_cluster_file_contents`.
//! 4. **Sending** (7.4:968-977; 8.0:1056-1065): `tryGetReply` (at most once, no timeout) to
//!    the well-known endpoint `WLTOKEN_CLIENTLEADERREG_OPENDATABASE` (token 4) of the address;
//!    for a hostname, `tryGetReplyFromHostname` resolves it first (`Hostname::resolve`: one
//!    address picked at random from the lookup, flags reset, `:tls` re-applied,
//!    `fromHostname` set; `flow/Hostname.actor.cpp` 7.4:63-77, `flow/Hostname.cpp` 8.0:61-75)
//!    and fails with `lookup_failed` if that fails; a `request_maybe_delivered` drops the
//!    DNS cache entry (`fdbrpc/include/fdbrpc/genericactors.actor.h` 7.4:111-133,
//!    `genericactors.h` 8.0:96-118).
//! 5. **A reply** (7.4:988-1018; 8.0:1076-1106):
//!    * with `forward` set: the cluster has moved to the connection string in `forward`. The
//!      generation ends and the next one uses an *intermediate record* holding the new
//!      string (`makeIntermediateRecord`, CCF 7.4:92-95), not yet written anywhere. Nothing
//!      is published.
//!    * otherwise, if the generation runs on an intermediate record, the new string becomes
//!      the record's and is **persisted** (`setAndPersistConnectionString`: the cluster file is
//!      atomically replaced with `# DO NOT EDIT!\n# This file is auto-generated, it is not to
//!      be edited by hand\n<string>\n`, CCF 7.4:54-58 and 148-178; a failure to write is only
//!      logged). Then `hasConnected`, `notifyConnected` (persists a record that was created
//!      with a string and never written), `shrinkProxyList`, `clientInfo->setUnconditional`
//!      (published even if the id did not change), `successIndex = index`, and the loop asks
//!      **the same coordinator** again with the new `knownClientInfoID`: that is the long poll.
//! 6. **An error** (7.4:1019-1030; 8.0:1107-1118), whatever it is (a broken connection
//!    `request_maybe_delivered`, `lookup_failed`, or an error the coordinator replied with):
//!    `index = (index + 1) % n`, and when that comes back round to `successIndex` the client
//!    sleeps `COORDINATOR_RECONNECTION_DELAY` (1 s, ClientKnobs 7.4:52; 8.0:157), unjittered,
//!    and sets `allConnectionsFailed`. There is no other backoff and no timeout: a dead
//!    coordinator is detected by the transport (connect timeout, ping timeout).
//!
//! `monitorProxies` also starts a new generation when the record itself is replaced
//! (`connRecord->onChange()`, 7.4:1049; 8.0:1134), which only the `switchConnectionRecord`
//! API does; it is not modelled here.
//!
//! ## `shrinkProxyList` (ML 7.4:800-888; 8.0:886-975)
//!
//! A client talks to at most `MAX_COMMIT_PROXY_CONNECTIONS` (5) commit proxies and
//! `MAX_GRV_PROXY_CONNECTIONS` (3) GRV proxies (ClientKnobs 7.4:55-56; 8.0:160-161). When a
//! list is longer, a random sample of that size is used, and kept for as long as the list's
//! proxy ids do not change, so that every `ClientDBInfo` update does not reshuffle the
//! connections; the first commit proxy of the full list is kept aside (`firstCommitProxy`,
//! for `commitOnFirstProxy`). When the list is at or below the limit, the cached sample is
//! dropped if `SHRINK_PROXY_LIST_CLEAR_CACHE_BELOW_THRESHOLD` is set: **false on 7.4
//! (ClientKnobs 7.4:57), true on 8.0 (8.0:162)**. Below the limit the cache is unused either
//! way, so the only effect is that a later return above the limit resamples; this module
//! always clears (8.0's behaviour, which releases the killed proxies' interfaces).
//!
//! ## The coordinator: `openDatabase` (Coord 7.4:220-272; 8.0:203-255)
//!
//! The coordinator answers at once if it has a valid `ClientDBInfo` whose id differs from
//! `knownClientInfoID` (or that carries `forward`) (`leaderRegister`, Coord 7.4:327-339; 8.0:
//! 346-362). Otherwise it holds the request until the info changes, or for
//! `delayJittered(CLIENT_REGISTER_INTERVAL)` (600 s, ServerKnobs 7.4:835; 8.0:984), after
//! which it replies with the unchanged info, or, if it still has none (no cluster
//! controller yet), with an empty `ErrorOr`, which the client receives as `default_error_or`
//! (1035). If it cannot reach the leader for `COORDINATOR_LEADER_CONNECTION_TIMEOUT` (20 s,
//! ServerKnobs 7.4:1187; 8.0:1359) it replies `failed_to_progress`. `leaderServer` answers
//! straight away with a fresh `ClientDBInfo` holding only a random id and `forward` when the
//! coordinators were changed away from it, and with `wrong_connection_file` when the request's
//! cluster key has another description (Coord 7.4:642-662; 8.0:704-722). The coordinator gets
//! the info itself from the cluster controller (`monitorLeaderAndGetClientInfo` /
//! `getClientInfoFromLeader`, ML 7.4:709-798), which publishes a new one on every recovery.
//!
//! ## Recovery: no proxies yet
//!
//! During a recovery the published `ClientDBInfo` has no proxies, or *provisional* ones
//! (recruited by a master that has not finished recovering). `DatabaseContext::updateProxies`
//! (NativeAPI 7.4:3006-3026; 8.0:956-975) builds the proxy sets only when non-empty, and
//! `getGrvProxies` / `getCommitProxies` (7.4:3028-3042; 8.0:1043-1057) return none when the
//! proxies are provisional unless the transaction set `USE_PROVISIONAL_PROXIES`.
//! Every request path then waits for `onProxiesChanged()`, which `monitorClientDBInfoChange`
//! (7.4:1010-1052) triggers when either proxy list of a new info differs.
//! [`ClusterInfo::available_grv_proxies`], [`ClusterInfo::available_commit_proxies`] and
//! [`ClusterMonitor::proxies`] are that rule.
//!
//! ## Endpoints
//!
//! `GrvProxyInterface` and `CommitProxyInterface` serialize only their first stream; the
//! others are `getAdjustedEndpoint(index)`, with the commit proxy's indices different on 8.0.
//! See `fdb_native_schema::streams`; [`GrvProxyEndpoints`] and [`CommitProxyEndpoints`] hold
//! them, computed with the indices of the version the info was decoded with.
//!
//! # This implementation
//!
//! * **Version detection.** The client does not know the cluster's protocol version: the
//!   transport learns it from the coordinator's ConnectPacket and reconnects announcing it
//!   (see `fdb_native_transport::peer`, "Version detection"). The request encoder is told the
//!   connection's version, picks the schema set with
//!   [`SchemaVersion::for_protocol_version`](fdb_native_schema::SchemaVersion::for_protocol_version),
//!   and the reply, which comes on that same connection, is decoded with it. The schema set
//!   is part of every published [`ClusterInfo`]. (C++'s multi-version client instead asks a
//!   coordinator for its protocol version and loads the matching library.)
//! * **Peer references.** The monitor holds a `PeerRef` to every address coordinator of the
//!   current generation, and to the last resolved address of each hostname coordinator, as
//!   C++'s `ClientLeaderRegInterface`s do: their connections stay up and failed ones are
//!   probed. A new generation (a forward) releases the old ones.
//! * **Cluster file.** A [`ConnectionRecord`] with a path behaves like
//!   `ClusterConnectionFile`: checked before each request and rewritten after a forward
//!   (steps 3 and 5). The checks and the write are synchronous file I/O on the monitor's task,
//!   as in C++ (they are rare and the file is at most 60000 bytes). A record without a path
//!   is `ClusterConnectionMemoryRecord`: never checked, never written.
//! * **Deliberate differences.** A `forward` that is not a valid connection string makes C++'s
//!   `ClusterConnectionString` constructor throw, which ends the monitor actor for good; here
//!   it counts as a failure of that coordinator ([`MonitorError::InvalidForward`]) and the
//!   next one is asked. Hostnames are resolved on every attempt (there is no DNS cache to
//!   invalidate). `switchConnectionRecord` is not supported.
//! * **Shutdown.** [`ClusterMonitor::shutdown`] (or dropping the monitor) aborts the task: an
//!   outstanding request is cancelled (the transport forgets its reply token; nothing is sent
//!   to the coordinator), the peer references are released, and every waiter gets
//!   [`MonitorClosed`].

mod config;
mod error;
mod info;
mod monitor;
mod record;
mod rng;
mod watch;

pub use config::ClusterMonitorConfig;
pub use error::{MonitorClosed, MonitorError};
pub use info::{ClusterInfo, CommitProxyEndpoints, GrvProxyEndpoints};
pub use monitor::{ClusterMonitor, MonitorStatus};
pub use record::{CLUSTER_FILE_HEADER, ConnectionRecord};
pub use watch::ClusterInfoWatch;

#[cfg(test)]
mod tests;
