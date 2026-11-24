//! The request streams of the interfaces in `ClientDBInfo` and `GetKeyServerLocationsReply`,
//! by adjusted-endpoint index.
//!
//! An FDB interface registers all of its streams at once (`initEndpoints`, through
//! `FlowTransport::addEndpoints`), so their tokens are consecutive, and it serializes only its
//! first stream (the *base endpoint*). A reader rebuilds the others with
//! `getAdjustedEndpoint(i)` ([`Endpoint::adjusted`](fdb_native_wire::Endpoint::adjusted)) in
//! its `serialize`; index `i` is the stream's position in `initEndpoints`. The structs here
//! name every stream of an interface: with `T = u32` they are the index tables
//! ([`GRV_PROXY_STREAMS`], [`STORAGE_SERVER_STREAMS`],
//! [`v7_4::COMMIT_PROXY_STREAMS`](crate::v7_4::COMMIT_PROXY_STREAMS),
//! [`v8_0::COMMIT_PROXY_STREAMS`](crate::v8_0::COMMIT_PROXY_STREAMS)), and with
//! `T = Endpoint` the endpoints of one interface (the domain types' `endpoints()`).
//!
//! | interface | serialized stream (index 0) | 7.4 | 8.0 |
//! | --- | --- | --- | --- |
//! | `GrvProxyInterface` | `getConsistentReadVersion` | 4 streams | the same |
//! | `CommitProxyInterface` | `commit` | 14 streams | 11 streams: `getHealthMetrics`, `getTenantId` and `getBlobGranuleLocations` removed, so every later index moves down |
//! | `StorageServerInterface` | `getValue` | 27 streams | the same |
//!
//! Source: `GrvProxyInterface.h`, `CommitProxyInterface.h` and `StorageServerInterface.h`
//! (8.0: `StorageServerInterface::initEndpointsFromGetValue` in
//! `StorageServerInterface.cpp`) on each release branch; the `serialize` of each interface
//! and its `initEndpoints` agree.

/// The streams of `GrvProxyInterface`. Identical on 7.4 and 8.0: [`GRV_PROXY_STREAMS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GrvProxyStreams<T> {
    /// `getConsistentReadVersion` (`GetReadVersionRequest`), the base endpoint.
    pub get_consistent_read_version: T,
    /// `waitFailure`.
    pub wait_failure: T,
    /// `getHealthMetrics` (`GetHealthMetricsRequest`).
    pub get_health_metrics: T,
    /// `refreshGlobalConfig` (`GlobalConfigRefreshRequest`).
    pub refresh_global_config: T,
}

/// `GrvProxyInterface`'s indices, on 7.4 and 8.0.
pub const GRV_PROXY_STREAMS: GrvProxyStreams<u32> = GrvProxyStreams {
    get_consistent_read_version: 0,
    wait_failure: 1,
    get_health_metrics: 2,
    refresh_global_config: 3,
};

impl<T> GrvProxyStreams<T> {
    /// Applies `f` to every stream.
    pub fn map<U>(self, mut f: impl FnMut(T) -> U) -> GrvProxyStreams<U> {
        GrvProxyStreams {
            get_consistent_read_version: f(self.get_consistent_read_version),
            wait_failure: f(self.wait_failure),
            get_health_metrics: f(self.get_health_metrics),
            refresh_global_config: f(self.refresh_global_config),
        }
    }
}

/// The streams of `CommitProxyInterface`. The indices differ between versions:
/// [`v7_4::COMMIT_PROXY_STREAMS`](crate::v7_4::COMMIT_PROXY_STREAMS) and
/// [`v8_0::COMMIT_PROXY_STREAMS`](crate::v8_0::COMMIT_PROXY_STREAMS) (or
/// [`SchemaVersion::commit_proxy_streams`](crate::SchemaVersion::commit_proxy_streams)).
/// `None`: the version has no such stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CommitProxyStreams<T> {
    /// `commit` (`CommitTransactionRequest`), the base endpoint.
    pub commit: T,
    /// `getConsistentReadVersion` on 7.4, `legacyGetConsistentReadVersion` on 8.0, where the
    /// index is only reserved: clients ask GRV proxies for read versions.
    pub get_consistent_read_version: T,
    /// `getKeyServersLocations` (`GetKeyServerLocationsRequest`).
    pub get_key_servers_locations: T,
    /// `getStorageServerRejoinInfo`.
    pub get_storage_server_rejoin_info: T,
    /// `waitFailure`.
    pub wait_failure: T,
    /// `txnState`.
    pub txn_state: T,
    /// `getHealthMetrics` (7.4 only; on 8.0 it is on the GRV proxy).
    pub get_health_metrics: Option<T>,
    /// `proxySnapReq`.
    pub proxy_snap_req: T,
    /// `exclusionSafetyCheckReq`.
    pub exclusion_safety_check_req: T,
    /// `getDDMetrics`.
    pub get_dd_metrics: T,
    /// `expireIdempotencyId` (`ExpireIdempotencyIdRequest`).
    pub expire_idempotency_id: T,
    /// `getTenantId` (7.4 only).
    pub get_tenant_id: Option<T>,
    /// `getBlobGranuleLocations` (7.4 only).
    pub get_blob_granule_locations: Option<T>,
    /// `setThrottledShard`.
    pub set_throttled_shard: T,
}

impl<T> CommitProxyStreams<T> {
    /// Applies `f` to every stream.
    pub fn map<U>(self, mut f: impl FnMut(T) -> U) -> CommitProxyStreams<U> {
        CommitProxyStreams {
            commit: f(self.commit),
            get_consistent_read_version: f(self.get_consistent_read_version),
            get_key_servers_locations: f(self.get_key_servers_locations),
            get_storage_server_rejoin_info: f(self.get_storage_server_rejoin_info),
            wait_failure: f(self.wait_failure),
            txn_state: f(self.txn_state),
            get_health_metrics: self.get_health_metrics.map(&mut f),
            proxy_snap_req: f(self.proxy_snap_req),
            exclusion_safety_check_req: f(self.exclusion_safety_check_req),
            get_dd_metrics: f(self.get_dd_metrics),
            expire_idempotency_id: f(self.expire_idempotency_id),
            get_tenant_id: self.get_tenant_id.map(&mut f),
            get_blob_granule_locations: self.get_blob_granule_locations.map(&mut f),
            set_throttled_shard: f(self.set_throttled_shard),
        }
    }
}

/// The streams of `StorageServerInterface`. Identical on 7.4 and 8.0:
/// [`STORAGE_SERVER_STREAMS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StorageServerStreams<T> {
    /// `getValue` (`GetValueRequest`), the base endpoint.
    pub get_value: T,
    /// `getKey` (`GetKeyRequest`).
    pub get_key: T,
    /// `getKeyValues` (`GetKeyValuesRequest`).
    pub get_key_values: T,
    /// `getShardState`.
    pub get_shard_state: T,
    /// `waitMetrics` (`WaitMetricsRequest`).
    pub wait_metrics: T,
    /// `splitMetrics`.
    pub split_metrics: T,
    /// `getStorageMetrics`.
    pub get_storage_metrics: T,
    /// `waitFailure`.
    pub wait_failure: T,
    /// `getQueuingMetrics`.
    pub get_queuing_metrics: T,
    /// `getKeyValueStoreType`.
    pub get_key_value_store_type: T,
    /// `watchValue` (`WatchValueRequest`).
    pub watch_value: T,
    /// `getReadHotRanges`.
    pub get_read_hot_ranges: T,
    /// `getRangeSplitPoints` (`SplitRangeRequest`).
    pub get_range_split_points: T,
    /// `getKeyValuesStream`.
    pub get_key_values_stream: T,
    /// `getMappedKeyValues`.
    pub get_mapped_key_values: T,
    /// `changeFeedStream`.
    pub change_feed_stream: T,
    /// `overlappingChangeFeeds`.
    pub overlapping_change_feeds: T,
    /// `changeFeedPop`.
    pub change_feed_pop: T,
    /// `changeFeedVersionUpdate`.
    pub change_feed_version_update: T,
    /// `checkpoint`.
    pub checkpoint: T,
    /// `fetchCheckpoint`.
    pub fetch_checkpoint: T,
    /// `fetchCheckpointKeyValues`.
    pub fetch_checkpoint_key_values: T,
    /// `updateCommitCostRequest`.
    pub update_commit_cost_request: T,
    /// `auditStorage`.
    pub audit_storage: T,
    /// `getHotShards`.
    pub get_hot_shards: T,
    /// `getCheckSum`.
    pub get_check_sum: T,
    /// `bulkdump`.
    pub bulkdump: T,
}

/// `StorageServerInterface`'s indices, on 7.4 and 8.0.
pub const STORAGE_SERVER_STREAMS: StorageServerStreams<u32> = StorageServerStreams {
    get_value: 0,
    get_key: 1,
    get_key_values: 2,
    get_shard_state: 3,
    wait_metrics: 4,
    split_metrics: 5,
    get_storage_metrics: 6,
    wait_failure: 7,
    get_queuing_metrics: 8,
    get_key_value_store_type: 9,
    watch_value: 10,
    get_read_hot_ranges: 11,
    get_range_split_points: 12,
    get_key_values_stream: 13,
    get_mapped_key_values: 14,
    change_feed_stream: 15,
    overlapping_change_feeds: 16,
    change_feed_pop: 17,
    change_feed_version_update: 18,
    checkpoint: 19,
    fetch_checkpoint: 20,
    fetch_checkpoint_key_values: 21,
    update_commit_cost_request: 22,
    audit_storage: 23,
    get_hot_shards: 24,
    get_check_sum: 25,
    bulkdump: 26,
};

impl<T> StorageServerStreams<T> {
    /// Applies `f` to every stream.
    pub fn map<U>(self, mut f: impl FnMut(T) -> U) -> StorageServerStreams<U> {
        StorageServerStreams {
            get_value: f(self.get_value),
            get_key: f(self.get_key),
            get_key_values: f(self.get_key_values),
            get_shard_state: f(self.get_shard_state),
            wait_metrics: f(self.wait_metrics),
            split_metrics: f(self.split_metrics),
            get_storage_metrics: f(self.get_storage_metrics),
            wait_failure: f(self.wait_failure),
            get_queuing_metrics: f(self.get_queuing_metrics),
            get_key_value_store_type: f(self.get_key_value_store_type),
            watch_value: f(self.watch_value),
            get_read_hot_ranges: f(self.get_read_hot_ranges),
            get_range_split_points: f(self.get_range_split_points),
            get_key_values_stream: f(self.get_key_values_stream),
            get_mapped_key_values: f(self.get_mapped_key_values),
            change_feed_stream: f(self.change_feed_stream),
            overlapping_change_feeds: f(self.overlapping_change_feeds),
            change_feed_pop: f(self.change_feed_pop),
            change_feed_version_update: f(self.change_feed_version_update),
            checkpoint: f(self.checkpoint),
            fetch_checkpoint: f(self.fetch_checkpoint),
            fetch_checkpoint_key_values: f(self.fetch_checkpoint_key_values),
            update_commit_cost_request: f(self.update_commit_cost_request),
            audit_storage: f(self.audit_storage),
            get_hot_shards: f(self.get_hot_shards),
            get_check_sum: f(self.get_check_sum),
            bulkdump: f(self.bulkdump),
        }
    }
}
