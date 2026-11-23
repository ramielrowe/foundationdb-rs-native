//! The FDB 8.0 schema set (`release-8.0`, protocol version `0x0FDB00B080000000`).
//!
//! Every type has its fields in `serializer(ar, ...)` argument order, which is the vtable
//! slot order (`tools/golden-vectors/fixtures/release-8.0/schemas.json` lists them). Types
//! whose layout is the same on 7.4 are re-exported from a shared module; the ones defined here
//! differ (see [`crate::v7_4`] for the 7.4 side):
//!
//! | type | 8.0 |
//! | --- | --- |
//! | [`GetValueRequest`] | no `tenantInfo`: `options` and `ssLatestCommitVersions` move down a slot |
//! | [`CommitTransactionRequest`] | no `tenantInfo`: `idempotencyId` moves down a slot |
//! | [`GetKeyServerLocationsRequest`] | no `tenant`; `minTenantVersion` renamed `legacyVersion` |
//! | [`GetReadVersionRequest`] | `maxGrvQueueDelayMS` appended |
//! | [`GetReadVersionReply`] | no `proxyTagThrottledDuration` |
//! | [`ClientDBInfo`] | no `tenantMode`, `encryptKeyProxy`, `metaclusterName`; native CDC fields appended |
//! | [`ReadOptions`] | `type` is a `uint32_t` enum |
//! | [`COMMIT_PROXY_STREAMS`] | 11 streams: from `proxySnapReq` on, one to three indices lower than on 7.4 |

use std::collections::BTreeMap;

use fdb_native_wire::{
    IdempotencyIdRef, ReplyPromise, RequestStream, TagSet, TagSetRef, Uid, VersionVector, Wire,
};

pub use crate::common::{
    ClientTagThrottleLimits, ClientTrCommitCostEstimation, ClientVersionRef, CommitID,
    CommitProxyInterface, CommitTransactionRef, GetKeyServerLocationsReply, GetValueReply,
    GrvProxyInterface, Hostname, KeyRangeRef, KeyServerLocation, LocalityData, MutationRef,
    OpenDatabaseCoordRequest, Pair, SpanContext, StorageServerInterface, TraceFlags,
    VersionHistory,
};
use crate::streams::CommitProxyStreams;
use crate::{SchemaVersion, domain};

/// `CommitProxyInterface`'s adjusted-endpoint indices on 8.0.
pub const COMMIT_PROXY_STREAMS: CommitProxyStreams<u32> = CommitProxyStreams {
    commit: 0,
    get_consistent_read_version: 1,
    get_key_servers_locations: 2,
    get_storage_server_rejoin_info: 3,
    wait_failure: 4,
    txn_state: 5,
    get_health_metrics: None,
    proxy_snap_req: 6,
    exclusion_safety_check_req: 7,
    get_dd_metrics: 8,
    expire_idempotency_id: 9,
    get_tenant_id: None,
    get_blob_granule_locations: None,
    set_throttled_shard: 10,
};

// ---------------------------------------------------------------------------------------
// Coordinator.

/// The request type of [`CdcProxyInterface::consume`] (`CDCConsumeRequest`), which is not
/// modelled. Only a type parameter.
#[derive(Debug)]
pub enum CdcConsumeRequest {}

/// `CDCProxyInterface` (`serializer(ar, processId, consume, supportsOrderedStreams)`; its
/// other streams are adjusted endpoints 1 to 9 of `consume`).
///
/// Absent: no constructor, `supportsOrderedStreams = false`: zero.
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 6689609)]
pub struct CdcProxyInterface<'a> {
    /// `processId`.
    pub process_id: Option<&'a [u8]>,
    /// `consume`, the base endpoint.
    pub consume: RequestStream<CdcConsumeRequest>,
    /// `supportsOrderedStreams`.
    pub supports_ordered_streams: bool,
}

/// `ClientDBInfo` on 8.0 (the flatbuffers branch: `serializer(ar, grvProxies, commitProxies,
/// id, forward, history, clusterId, clusterType, nativeCdcEnabled, nativeCdcTagCount,
/// cdcProxies, streamToCDCProxyId)`), the reply to [`OpenDatabaseCoordRequest`].
///
/// Absent: `ClientDBInfo() = default` with `clusterType = STANDALONE` (0), `nativeCdcEnabled =
/// false`, `nativeCdcTagCount = 0`: zero.
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 5355080)]
pub struct ClientDBInfo<'a> {
    /// `grvProxies`.
    pub grv_proxies: Vec<GrvProxyInterface<'a>>,
    /// `commitProxies`.
    pub commit_proxies: Vec<CommitProxyInterface<'a>>,
    /// `id`.
    pub id: Uid,
    /// `forward`.
    pub forward: Option<&'a [u8]>,
    /// `history`.
    pub history: Vec<VersionHistory<'a>>,
    /// `clusterId`.
    pub cluster_id: Uid,
    /// `clusterType` (`ClusterType`, an `int` enum).
    pub cluster_type: i32,
    /// `nativeCdcEnabled`.
    pub native_cdc_enabled: bool,
    /// `nativeCdcTagCount`.
    pub native_cdc_tag_count: i32,
    /// `cdcProxies`.
    pub cdc_proxies: Vec<CdcProxyInterface<'a>>,
    /// `streamToCDCProxyId` (`std::map<CDCStreamId, UID>`, `CDCStreamId` a `uint64_t`).
    pub stream_to_cdc_proxy_id: BTreeMap<u64, Uid>,
}

impl From<ClientDBInfo<'_>> for domain::ClientDbInfo {
    fn from(info: ClientDBInfo<'_>) -> Self {
        domain::ClientDbInfo {
            id: info.id,
            grv_proxies: info.grv_proxies.into_iter().map(Into::into).collect(),
            commit_proxies: info
                .commit_proxies
                .iter()
                .map(|p| p.to_domain(SchemaVersion::V8_0))
                .collect(),
            forward: info.forward.map(<[u8]>::to_vec),
            history: info.history.into_iter().map(Into::into).collect(),
            cluster_id: info.cluster_id,
            cluster_type: info.cluster_type,
            tenant_mode: None,
        }
    }
}

impl<'a> From<&'a domain::ClientDbInfo> for ClientDBInfo<'a> {
    /// The native CDC fields are written as their defaults; `tenant_mode` is not sent.
    fn from(info: &'a domain::ClientDbInfo) -> Self {
        ClientDBInfo {
            grv_proxies: info.grv_proxies.iter().map(Into::into).collect(),
            commit_proxies: info.commit_proxies.iter().map(Into::into).collect(),
            id: info.id,
            forward: info.forward.as_deref(),
            history: info.history.iter().map(Into::into).collect(),
            cluster_id: info.cluster_id,
            cluster_type: info.cluster_type,
            native_cdc_enabled: false,
            native_cdc_tag_count: 0,
            cdc_proxies: Vec::new(),
            stream_to_cdc_proxy_id: BTreeMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------------------
// GRV proxy.

/// `GetReadVersionRequest` on 8.0 (`serializer(ar, transactionCount, flags, tags, debugID,
/// reply, spanContext, maxVersion, maxGrvQueueDelayMS)`).
///
/// Absent: `GetReadVersionRequest() : transactionCount(1), flags(0), maxVersion(invalidVersion)`,
/// so `transaction_count` is 1 and `max_version` -1. [`Default`] is that too.
#[derive(Wire, Clone, Debug, PartialEq, Eq)]
#[fdb(file_identifier = 838566, absent = "default")]
pub struct GetReadVersionRequest<'a> {
    /// `transactionCount`.
    pub transaction_count: u32,
    /// `flags`.
    pub flags: u32,
    /// `tags` (`TransactionTagMap<uint32_t>`).
    pub tags: BTreeMap<&'a [u8], u32>,
    /// `debugID`.
    pub debug_id: Option<Uid>,
    /// `reply`.
    pub reply: ReplyPromise<domain::ReadVersion>,
    /// `spanContext`.
    pub span_context: SpanContext,
    /// `maxVersion`.
    pub max_version: i64,
    /// `maxGrvQueueDelayMS`.
    pub max_grv_queue_delay_ms: Option<i64>,
}

impl Default for GetReadVersionRequest<'_> {
    /// `GetReadVersionRequest()`: one transaction, `maxVersion = invalidVersion`.
    fn default() -> Self {
        GetReadVersionRequest {
            transaction_count: 1,
            flags: 0,
            tags: BTreeMap::new(),
            debug_id: None,
            reply: ReplyPromise::default(),
            span_context: SpanContext::default(),
            max_version: domain::INVALID_VERSION,
            max_grv_queue_delay_ms: None,
        }
    }
}

impl<'a> GetReadVersionRequest<'a> {
    /// The request for `r`, whose reply goes to the endpoint with token `reply`.
    pub fn from_domain(r: &'a domain::GetReadVersion, reply: Uid) -> Self {
        GetReadVersionRequest {
            transaction_count: r.transaction_count,
            flags: r.flags,
            tags: r.tags.iter().map(|(k, v)| (k.as_slice(), *v)).collect(),
            debug_id: r.debug_id,
            reply: ReplyPromise::new(reply),
            span_context: r.span_context,
            max_version: r.max_version,
            max_grv_queue_delay_ms: r.max_grv_queue_delay_ms,
        }
    }
}

impl From<GetReadVersionRequest<'_>> for domain::GetReadVersion {
    fn from(r: GetReadVersionRequest<'_>) -> Self {
        domain::GetReadVersion {
            transaction_count: r.transaction_count,
            flags: r.flags,
            tags: r.tags.into_iter().map(|(k, v)| (k.to_vec(), v)).collect(),
            debug_id: r.debug_id,
            span_context: r.span_context,
            max_version: r.max_version,
            max_grv_queue_delay_ms: r.max_grv_queue_delay_ms,
        }
    }
}

/// `GetReadVersionReply` on 8.0 (`serializer(ar, BasicLoadBalancedReply::processBusyTime,
/// version, locked, metadataVersion, tagThrottleInfo, midShardSize, rkDefaultThrottled,
/// rkBatchThrottled, ssVersionVectorDelta, proxyId)`).
///
/// Absent: `GetReadVersionReply() : version(invalidVersion), locked(false)`, so `version` is
/// -1. [`Default`] is that too.
#[derive(Wire, Clone, Debug, PartialEq)]
#[fdb(file_identifier = 15709388, absent = "default")]
pub struct GetReadVersionReply<'a> {
    /// `processBusyTime`.
    pub process_busy_time: i32,
    /// `version`.
    pub version: i64,
    /// `locked`.
    pub locked: bool,
    /// `metadataVersion`.
    pub metadata_version: Option<&'a [u8]>,
    /// `tagThrottleInfo` (`TransactionTagMap<ClientTagThrottleLimits>`).
    pub tag_throttle_info: BTreeMap<&'a [u8], ClientTagThrottleLimits>,
    /// `midShardSize`.
    pub mid_shard_size: i64,
    /// `rkDefaultThrottled`.
    pub rk_default_throttled: bool,
    /// `rkBatchThrottled`.
    pub rk_batch_throttled: bool,
    /// `ssVersionVectorDelta`.
    pub ss_version_vector_delta: VersionVector,
    /// `proxyId`.
    pub proxy_id: Uid,
}

impl Default for GetReadVersionReply<'_> {
    /// `GetReadVersionReply()`: `version = invalidVersion`.
    fn default() -> Self {
        GetReadVersionReply {
            process_busy_time: 0,
            version: domain::INVALID_VERSION,
            locked: false,
            metadata_version: None,
            tag_throttle_info: BTreeMap::new(),
            mid_shard_size: 0,
            rk_default_throttled: false,
            rk_batch_throttled: false,
            ss_version_vector_delta: VersionVector::default(),
            proxy_id: Uid::default(),
        }
    }
}

impl<'a> GetReadVersionReply<'a> {
    /// The domain reply (throttle limits keep their durations).
    /// `proxy_tag_throttled_duration` is 0.
    pub fn to_domain(self) -> domain::ReadVersion {
        domain::ReadVersion {
            process_busy_time: self.process_busy_time,
            version: self.version,
            locked: self.locked,
            metadata_version: self.metadata_version.map(<[u8]>::to_vec),
            tag_throttle_info: self
                .tag_throttle_info
                .into_iter()
                .map(|(tag, limits)| (tag.to_vec(), limits.into()))
                .collect(),
            mid_shard_size: self.mid_shard_size,
            rk_default_throttled: self.rk_default_throttled,
            rk_batch_throttled: self.rk_batch_throttled,
            ss_version_vector_delta: self.ss_version_vector_delta,
            proxy_id: self.proxy_id,
            proxy_tag_throttled_duration: 0.0,
        }
    }

    /// The reply for `r`.
    /// `proxy_tag_throttled_duration` does not exist on 8.0 and is not sent.
    pub fn from_domain(r: &'a domain::ReadVersion) -> Self {
        GetReadVersionReply {
            process_busy_time: r.process_busy_time,
            version: r.version,
            locked: r.locked,
            metadata_version: r.metadata_version.as_deref(),
            tag_throttle_info: r
                .tag_throttle_info
                .iter()
                .map(|(tag, limits)| (tag.as_slice(), ClientTagThrottleLimits::from(limits)))
                .collect(),
            mid_shard_size: r.mid_shard_size,
            rk_default_throttled: r.rk_default_throttled,
            rk_batch_throttled: r.rk_batch_throttled,
            ss_version_vector_delta: r.ss_version_vector_delta.clone(),
            proxy_id: r.proxy_id,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Key server locations.

/// `GetKeyServerLocationsRequest` on 8.0 (`serializer(ar, begin, end, limit, reverse, reply,
/// spanContext, legacyVersion, arena)`).
///
/// Absent: `GetKeyServerLocationsRequest() : limit(0), reverse(false),
/// legacyVersion(latestVersion)`, so `legacy_version` is -2. [`Default`] is that too.
#[derive(Wire, Clone, Debug, PartialEq, Eq)]
#[fdb(file_identifier = 9144680, absent = "default")]
pub struct GetKeyServerLocationsRequest<'a> {
    /// `begin`.
    pub begin: &'a [u8],
    /// `end`.
    pub end: Option<&'a [u8]>,
    /// `limit`.
    pub limit: i32,
    /// `reverse`.
    pub reverse: bool,
    /// `reply`.
    pub reply: ReplyPromise<domain::KeyServerLocations>,
    /// `spanContext`.
    pub span_context: SpanContext,
    /// `legacyVersion` (7.4's `minTenantVersion`).
    pub legacy_version: i64,
    /// `arena`.
    #[fdb(arena)]
    pub arena: (),
}

impl Default for GetKeyServerLocationsRequest<'_> {
    /// `GetKeyServerLocationsRequest()`: `legacyVersion = latestVersion` (-2).
    fn default() -> Self {
        GetKeyServerLocationsRequest {
            begin: b"",
            end: None,
            limit: 0,
            reverse: false,
            reply: ReplyPromise::default(),
            span_context: SpanContext::default(),
            legacy_version: domain::LATEST_VERSION,
            arena: (),
        }
    }
}

impl<'a> GetKeyServerLocationsRequest<'a> {
    /// The request for `r`, whose reply goes to the endpoint with token `reply`.
    pub fn from_domain(r: &'a domain::GetKeyServerLocations, reply: Uid) -> Self {
        GetKeyServerLocationsRequest {
            begin: &r.begin,
            end: r.end.as_deref(),
            limit: r.limit,
            reverse: r.reverse,
            reply: ReplyPromise::new(reply),
            span_context: r.span_context,
            legacy_version: r.min_tenant_version,
            arena: (),
        }
    }
}

impl From<GetKeyServerLocationsRequest<'_>> for domain::GetKeyServerLocations {
    fn from(r: GetKeyServerLocationsRequest<'_>) -> Self {
        domain::GetKeyServerLocations {
            begin: r.begin.to_vec(),
            end: r.end.map(<[u8]>::to_vec),
            limit: r.limit,
            reverse: r.reverse,
            span_context: r.span_context,
            min_tenant_version: r.legacy_version,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Storage server.

/// `ReadOptions` on 8.0 (`serializer(ar, type, cacheResult, debugID,
/// consistencyCheckStartVersion, lockAware)`); `type` is a `uint32_t` enum.
///
/// Absent: `ReadOptions()` (every constructor argument defaulted): `read_type` is `NORMAL` (3)
/// and `cache_result` `true`. [`Default`] is that too.
#[derive(Wire, Clone, Copy, Debug, PartialEq, Eq)]
#[fdb(absent = "default")]
pub struct ReadOptions {
    /// `type` (`ReadType`).
    pub read_type: u32,
    /// `cacheResult`.
    pub cache_result: bool,
    /// `debugID`.
    pub debug_id: Option<Uid>,
    /// `consistencyCheckStartVersion`.
    pub consistency_check_start_version: Option<i64>,
    /// `lockAware`.
    pub lock_aware: bool,
}

impl Default for ReadOptions {
    /// `ReadOptions()`: `ReadType::NORMAL`, `CacheResult::True`.
    fn default() -> Self {
        ReadOptions {
            read_type: domain::ReadOptions::NORMAL,
            cache_result: true,
            debug_id: None,
            consistency_check_start_version: None,
            lock_aware: false,
        }
    }
}

impl From<&domain::ReadOptions> for ReadOptions {
    fn from(o: &domain::ReadOptions) -> Self {
        ReadOptions {
            read_type: o.read_type,
            cache_result: o.cache_result,
            debug_id: o.debug_id,
            consistency_check_start_version: o.consistency_check_start_version,
            lock_aware: o.lock_aware,
        }
    }
}

impl From<ReadOptions> for domain::ReadOptions {
    fn from(o: ReadOptions) -> Self {
        domain::ReadOptions {
            read_type: o.read_type,
            cache_result: o.cache_result,
            debug_id: o.debug_id,
            consistency_check_start_version: o.consistency_check_start_version,
            lock_aware: o.lock_aware,
        }
    }
}

/// `GetValueRequest` on 8.0 (`serializer(ar, key, version, tags, reply, spanContext,
/// options, ssLatestCommitVersions)`).
///
/// Absent: `GetValueRequest() = default`: zero.
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 8454530)]
pub struct GetValueRequest<'a> {
    /// `key`.
    pub key: &'a [u8],
    /// `version`.
    pub version: i64,
    /// `tags`.
    pub tags: Option<TagSetRef<'a>>,
    /// `reply`.
    pub reply: ReplyPromise<domain::GetValueReply<'static>>,
    /// `spanContext`.
    pub span_context: SpanContext,
    /// `options`.
    pub options: Option<ReadOptions>,
    /// `ssLatestCommitVersions`.
    pub ss_latest_commit_versions: VersionVector,
}

impl<'a> GetValueRequest<'a> {
    /// The request for `r`, whose reply goes to the endpoint with token `reply`.
    pub fn from_domain(r: &'a domain::GetValue, reply: Uid) -> Self {
        GetValueRequest {
            key: &r.key,
            version: r.version,
            tags: r.tags.as_ref().map(TagSet::borrowed),
            reply: ReplyPromise::new(reply),
            span_context: r.span_context,
            options: r.options.as_ref().map(ReadOptions::from),
            ss_latest_commit_versions: r.ss_latest_commit_versions.clone(),
        }
    }
}

impl From<GetValueRequest<'_>> for domain::GetValue {
    fn from(r: GetValueRequest<'_>) -> Self {
        domain::GetValue {
            key: r.key.to_vec(),
            version: r.version,
            tags: r.tags.map(TagSetRef::into_owned),
            span_context: r.span_context,
            options: r.options.map(Into::into),
            ss_latest_commit_versions: r.ss_latest_commit_versions,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Commit.

/// `CommitTransactionRequest` on 8.0 (`serializer(ar, transaction, reply, flags, debugID,
/// commitCostEstimation, tagSet, spanContext, idempotencyId, arena)`).
///
/// Absent: `CommitTransactionRequest() : flags(0)`: zero.
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 93948)]
pub struct CommitTransactionRequest<'a> {
    /// `transaction`.
    pub transaction: CommitTransactionRef<'a>,
    /// `reply`.
    pub reply: ReplyPromise<domain::CommitId>,
    /// `flags`.
    pub flags: u32,
    /// `debugID`.
    pub debug_id: Option<Uid>,
    /// `commitCostEstimation`.
    pub commit_cost_estimation: Option<ClientTrCommitCostEstimation>,
    /// `tagSet`.
    pub tag_set: Option<TagSetRef<'a>>,
    /// `spanContext`.
    pub span_context: SpanContext,
    /// `idempotencyId` (empty: none).
    pub idempotency_id: IdempotencyIdRef<'a>,
    /// `arena`.
    #[fdb(arena)]
    pub arena: (),
}

impl<'a> CommitTransactionRequest<'a> {
    /// The request for `c`, whose reply goes to the endpoint with token `reply`.
    pub fn from_domain(c: &'a domain::Commit, reply: Uid) -> Self {
        CommitTransactionRequest {
            transaction: CommitTransactionRef::from(c),
            reply: ReplyPromise::new(reply),
            flags: c.flags,
            debug_id: c.debug_id,
            commit_cost_estimation: c.commit_cost_estimation.as_ref().map(Into::into),
            tag_set: c.tag_set.as_ref().map(TagSet::borrowed),
            span_context: c.span_context,
            idempotency_id: c
                .idempotency_id
                .as_ref()
                .map_or(IdempotencyIdRef::INVALID, |id| id.borrowed()),
            arena: (),
        }
    }
}

impl From<CommitTransactionRequest<'_>> for domain::Commit {
    fn from(r: CommitTransactionRequest<'_>) -> Self {
        let mut commit = domain::Commit {
            flags: r.flags,
            debug_id: r.debug_id,
            commit_cost_estimation: r.commit_cost_estimation.map(Into::into),
            tag_set: r.tag_set.map(TagSetRef::into_owned),
            span_context: r.span_context,
            idempotency_id: r
                .idempotency_id
                .is_valid()
                .then(|| r.idempotency_id.into_owned()),
            ..domain::Commit::default()
        };
        r.transaction.fill_domain(&mut commit);
        commit
    }
}
