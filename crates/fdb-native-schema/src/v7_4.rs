//! The FDB 7.4 schema set (`release-7.4`, protocol version `0x0FDB00B074000000`).
//!
//! Every type has its fields in `serializer(ar, ...)` argument order, which is the vtable
//! slot order (`tools/golden-vectors/fixtures/release-7.4/schemas.json` lists them). Types
//! whose layout is the same on 8.0 are re-exported from a shared module; the ones defined here
//! differ:
//!
//! | type | 7.4 only |
//! | --- | --- |
//! | [`GetValueRequest`] | `tenantInfo` between `spanContext` and `options` |
//! | [`CommitTransactionRequest`] | `tenantInfo` between `spanContext` and `idempotencyId` |
//! | [`GetKeyServerLocationsRequest`] | `tenant` and `minTenantVersion` (8.0: `legacyVersion`) |
//! | [`GetReadVersionRequest`] | no `maxGrvQueueDelayMS` |
//! | [`GetReadVersionReply`] | `proxyTagThrottledDuration`, the last slot |
//! | [`ClientDBInfo`] | `tenantMode`, `encryptKeyProxy`, `metaclusterName`; no native CDC fields |
//! | [`ReadOptions`] | `type` is an `int32_t` enum |
//! | [`COMMIT_PROXY_STREAMS`] | 14 streams: `getHealthMetrics` (6), `getTenantId` (11), `getBlobGranuleLocations` (12) |
//!
//! **Tenants.** A request that has a `TenantInfo` must carry one even from a client that does
//! not use tenants: [`TenantInfo::NONE`] (`tenantId = -1`, no token), which is what every
//! conversion from a [`domain`] request writes. (An absent `TenantInfo` table
//! would read as `tenantId = 0`, a real tenant id.) Decoding a request into the domain drops
//! its tenant.

use std::collections::BTreeMap;

use fdb_native_wire::{
    IdempotencyIdRef, ReplyPromise, RequestStream, TagSet, TagSetRef, Uid, VersionVector, Void,
    Wire,
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

/// `CommitProxyInterface`'s adjusted-endpoint indices on 7.4.
pub const COMMIT_PROXY_STREAMS: CommitProxyStreams<u32> = CommitProxyStreams {
    commit: 0,
    get_consistent_read_version: 1,
    get_key_servers_locations: 2,
    get_storage_server_rejoin_info: 3,
    wait_failure: 4,
    txn_state: 5,
    get_health_metrics: Some(6),
    proxy_snap_req: 7,
    exclusion_safety_check_req: 8,
    get_dd_metrics: 9,
    expire_idempotency_id: 10,
    get_tenant_id: Some(11),
    get_blob_granule_locations: Some(12),
    set_throttled_shard: 13,
};

// ---------------------------------------------------------------------------------------
// Tenants.

/// `WipedString` (`serializer(ar, ws, arena)`; `ws` is a local copy of the string): a
/// tenant's authorization token.
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WipedString<'a> {
    /// `ws`.
    pub ws: &'a [u8],
    /// `arena`.
    #[fdb(arena)]
    pub arena: (),
}

/// `TenantInfo` (`serializable_traits<TenantInfo>`: `serializer(ar, tenantId, token,
/// arena)`).
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TenantInfo<'a> {
    /// `tenantId` ([`TenantInfo::INVALID_TENANT`] for none).
    pub tenant_id: i64,
    /// `token`.
    pub token: Option<WipedString<'a>>,
    /// `arena`.
    #[fdb(arena)]
    pub arena: (),
}

impl TenantInfo<'_> {
    /// `TenantInfo::INVALID_TENANT`.
    pub const INVALID_TENANT: i64 = -1;

    /// `TenantInfo()`: no tenant, no token. What a tenant-less client sends.
    pub const NONE: TenantInfo<'static> = TenantInfo {
        tenant_id: Self::INVALID_TENANT,
        token: None,
        arena: (),
    };
}

/// `TenantMode` (`serializer(ar, mode)`): 0 disabled, 1 optional, 2 required.
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TenantMode {
    /// `mode`.
    pub mode: u32,
}

// ---------------------------------------------------------------------------------------
// Coordinator.

/// `EncryptKeyProxyInterface` (`serializer(ar, locality, myId, waitFailure)`; its other
/// streams are adjusted endpoints 1 to 5 of `waitFailure`).
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 1303419)]
pub struct EncryptKeyProxyInterface<'a> {
    /// `locality`.
    pub locality: LocalityData<'a>,
    /// `myId`.
    pub my_id: Uid,
    /// `waitFailure`, the base endpoint.
    pub wait_failure: RequestStream<ReplyPromise<Void>>,
}

/// `ClientDBInfo` on 7.4 (`serializer(ar, grvProxies, commitProxies, id, forward, history,
/// tenantMode, encryptKeyProxy, clusterId, clusterType, metaclusterName)`), the reply to
/// [`OpenDatabaseCoordRequest`].
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
    /// `tenantMode`.
    pub tenant_mode: TenantMode,
    /// `encryptKeyProxy`.
    pub encrypt_key_proxy: Option<EncryptKeyProxyInterface<'a>>,
    /// `clusterId`.
    pub cluster_id: Uid,
    /// `clusterType` (`ClusterType`, an `int` enum).
    pub cluster_type: i32,
    /// `metaclusterName`.
    pub metacluster_name: Option<&'a [u8]>,
}

impl From<ClientDBInfo<'_>> for domain::ClientDbInfo {
    fn from(info: ClientDBInfo<'_>) -> Self {
        domain::ClientDbInfo {
            id: info.id,
            grv_proxies: info.grv_proxies.into_iter().map(Into::into).collect(),
            commit_proxies: info
                .commit_proxies
                .iter()
                .map(|p| p.to_domain(SchemaVersion::V7_4))
                .collect(),
            forward: info.forward.map(<[u8]>::to_vec),
            history: info.history.into_iter().map(Into::into).collect(),
            cluster_id: info.cluster_id,
            cluster_type: info.cluster_type,
        }
    }
}

impl<'a> From<&'a domain::ClientDbInfo> for ClientDBInfo<'a> {
    /// `tenantMode`, `encryptKeyProxy` and `metaclusterName` are written as their defaults.
    fn from(info: &'a domain::ClientDbInfo) -> Self {
        ClientDBInfo {
            grv_proxies: info.grv_proxies.iter().map(Into::into).collect(),
            commit_proxies: info.commit_proxies.iter().map(Into::into).collect(),
            id: info.id,
            forward: info.forward.as_deref(),
            history: info.history.iter().map(Into::into).collect(),
            tenant_mode: TenantMode::default(),
            encrypt_key_proxy: None,
            cluster_id: info.cluster_id,
            cluster_type: info.cluster_type,
            metacluster_name: None,
        }
    }
}

// ---------------------------------------------------------------------------------------
// GRV proxy.

/// `GetReadVersionRequest` on 7.4 (`serializer(ar, transactionCount, flags, tags, debugID,
/// reply, spanContext, maxVersion)`).
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 838566)]
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
}

impl<'a> From<&'a domain::GetReadVersion> for GetReadVersionRequest<'a> {
    /// `max_grv_queue_delay_ms` does not exist on 7.4 and is not sent.
    fn from(r: &'a domain::GetReadVersion) -> Self {
        GetReadVersionRequest {
            transaction_count: r.transaction_count,
            flags: r.flags,
            tags: r.tags.iter().map(|(k, v)| (k.as_slice(), *v)).collect(),
            debug_id: r.debug_id,
            reply: ReplyPromise::new(r.reply),
            span_context: r.span_context,
            max_version: r.max_version,
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
            reply: r.reply.token(),
            span_context: r.span_context,
            max_version: r.max_version,
            max_grv_queue_delay_ms: None,
        }
    }
}

/// `GetReadVersionReply` on 7.4 (`serializer(ar, BasicLoadBalancedReply::processBusyTime,
/// version, locked, metadataVersion, tagThrottleInfo, midShardSize, rkDefaultThrottled,
/// rkBatchThrottled, ssVersionVectorDelta, proxyId, proxyTagThrottledDuration)`).
#[derive(Wire, Clone, Debug, Default, PartialEq)]
#[fdb(file_identifier = 15709388)]
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
    /// `proxyTagThrottledDuration`.
    pub proxy_tag_throttled_duration: f64,
}

impl<'a> GetReadVersionReply<'a> {
    /// The domain reply; throttle expirations are `now + duration`.
    pub fn to_domain(self, now: f64) -> domain::ReadVersion {
        domain::ReadVersion {
            process_busy_time: self.process_busy_time,
            version: self.version,
            locked: self.locked,
            metadata_version: self.metadata_version.map(<[u8]>::to_vec),
            tag_throttle_info: self
                .tag_throttle_info
                .into_iter()
                .map(|(tag, limits)| (tag.to_vec(), limits.to_domain(now)))
                .collect(),
            mid_shard_size: self.mid_shard_size,
            rk_default_throttled: self.rk_default_throttled,
            rk_batch_throttled: self.rk_batch_throttled,
            ss_version_vector_delta: self.ss_version_vector_delta,
            proxy_id: self.proxy_id,
            proxy_tag_throttled_duration: self.proxy_tag_throttled_duration,
        }
    }

    /// The reply as written at `now`: throttle durations are `expiration - now`.
    pub fn from_domain(r: &'a domain::ReadVersion, now: f64) -> Self {
        GetReadVersionReply {
            process_busy_time: r.process_busy_time,
            version: r.version,
            locked: r.locked,
            metadata_version: r.metadata_version.as_deref(),
            tag_throttle_info: r
                .tag_throttle_info
                .iter()
                .map(|(tag, limits)| {
                    (
                        tag.as_slice(),
                        ClientTagThrottleLimits::from_domain(limits, now),
                    )
                })
                .collect(),
            mid_shard_size: r.mid_shard_size,
            rk_default_throttled: r.rk_default_throttled,
            rk_batch_throttled: r.rk_batch_throttled,
            ss_version_vector_delta: r.ss_version_vector_delta.clone(),
            proxy_id: r.proxy_id,
            proxy_tag_throttled_duration: r.proxy_tag_throttled_duration,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Key server locations.

/// `GetKeyServerLocationsRequest` on 7.4 (`serializer(ar, begin, end, limit, reverse, reply,
/// spanContext, tenant, minTenantVersion, arena)`).
#[derive(Wire, Clone, Debug, Default, PartialEq, Eq)]
#[fdb(file_identifier = 9144680)]
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
    /// `tenant`.
    pub tenant: TenantInfo<'a>,
    /// `minTenantVersion`.
    pub min_tenant_version: i64,
    /// `arena`.
    #[fdb(arena)]
    pub arena: (),
}

impl<'a> From<&'a domain::GetKeyServerLocations> for GetKeyServerLocationsRequest<'a> {
    fn from(r: &'a domain::GetKeyServerLocations) -> Self {
        GetKeyServerLocationsRequest {
            begin: &r.begin,
            end: r.end.as_deref(),
            limit: r.limit,
            reverse: r.reverse,
            reply: ReplyPromise::new(r.reply),
            span_context: r.span_context,
            tenant: TenantInfo::NONE,
            min_tenant_version: r.min_tenant_version,
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
            reply: r.reply.token(),
            span_context: r.span_context,
            min_tenant_version: r.min_tenant_version,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Storage server.

/// `ReadOptions` on 7.4 (`serializer(ar, type, cacheResult, debugID,
/// consistencyCheckStartVersion, lockAware)`); `type` is an `int32_t` enum.
#[derive(Wire, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadOptions {
    /// `type` (`ReadType`).
    pub read_type: i32,
    /// `cacheResult`.
    pub cache_result: bool,
    /// `debugID`.
    pub debug_id: Option<Uid>,
    /// `consistencyCheckStartVersion`.
    pub consistency_check_start_version: Option<i64>,
    /// `lockAware`.
    pub lock_aware: bool,
}

impl From<&domain::ReadOptions> for ReadOptions {
    fn from(o: &domain::ReadOptions) -> Self {
        ReadOptions {
            // Same size and bits; the values are small.
            read_type: o.read_type as i32,
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
            read_type: o.read_type as u32,
            cache_result: o.cache_result,
            debug_id: o.debug_id,
            consistency_check_start_version: o.consistency_check_start_version,
            lock_aware: o.lock_aware,
        }
    }
}

/// `GetValueRequest` on 7.4 (`serializer(ar, key, version, tags, reply, spanContext,
/// tenantInfo, options, ssLatestCommitVersions)`).
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
    /// `tenantInfo`.
    pub tenant_info: TenantInfo<'a>,
    /// `options`.
    pub options: Option<ReadOptions>,
    /// `ssLatestCommitVersions`.
    pub ss_latest_commit_versions: VersionVector,
}

impl<'a> From<&'a domain::GetValue> for GetValueRequest<'a> {
    fn from(r: &'a domain::GetValue) -> Self {
        GetValueRequest {
            key: &r.key,
            version: r.version,
            tags: r.tags.as_ref().map(TagSet::borrowed),
            reply: ReplyPromise::new(r.reply),
            span_context: r.span_context,
            tenant_info: TenantInfo::NONE,
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
            reply: r.reply.token(),
            span_context: r.span_context,
            options: r.options.map(Into::into),
            ss_latest_commit_versions: r.ss_latest_commit_versions,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Commit.

/// `CommitTransactionRequest` on 7.4 (`serializer(ar, transaction, reply, flags, debugID,
/// commitCostEstimation, tagSet, spanContext, tenantInfo, idempotencyId, arena)`).
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
    /// `tenantInfo`.
    pub tenant_info: TenantInfo<'a>,
    /// `idempotencyId` (empty: none).
    pub idempotency_id: IdempotencyIdRef<'a>,
    /// `arena`.
    #[fdb(arena)]
    pub arena: (),
}

impl<'a> From<&'a domain::Commit> for CommitTransactionRequest<'a> {
    fn from(c: &'a domain::Commit) -> Self {
        CommitTransactionRequest {
            transaction: CommitTransactionRef::from(c),
            reply: ReplyPromise::new(c.reply),
            flags: c.flags,
            debug_id: c.debug_id,
            commit_cost_estimation: c.commit_cost_estimation.as_ref().map(Into::into),
            tag_set: c.tag_set.as_ref().map(TagSet::borrowed),
            span_context: c.span_context,
            tenant_info: TenantInfo::NONE,
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
            reply: r.reply.token(),
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
