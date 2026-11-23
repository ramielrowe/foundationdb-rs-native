//! Version-neutral messages: what the client logic works with, whatever the server's
//! version.
//!
//! Each type converts to and from the wire types of every schema set (`From`/`TryFrom` impls
//! in [`crate::v7_4`] and [`crate::v8_0`]), and the [`Request`](crate::Request) impls encode
//! and decode them for a [`SchemaVersion`]. Fields that exist on only one version are
//! documented as such; a field the domain does not carry is written as the C++ default (on 7.4,
//! the tenant-less `TenantInfo`). Request types own their data (a commit's mutations can
//! be moved in from the transaction's buffer and are encoded without a copy); the reply to the
//! hottest request, [`GetValueReply`], borrows its value from the received message.

use std::collections::BTreeMap;

use fdb_native_wire::{
    Endpoint, FdbError, IdempotencyId, NetworkAddress, Tag, TagSet, Uid, VersionVector,
};

use crate::SchemaVersion;
use crate::span_context::SpanContext;
use crate::streams::{
    CommitProxyStreams, GRV_PROXY_STREAMS, GrvProxyStreams, STORAGE_SERVER_STREAMS,
    StorageServerStreams,
};

/// `latestVersion`: "the latest version", in requests that take a version.
pub const LATEST_VERSION: i64 = -2;

/// `invalidVersion`.
pub const INVALID_VERSION: i64 = -1;

// ---------------------------------------------------------------------------------------
// Keys and mutations.

/// A key range `[begin, end)`.
///
/// On the wire (`KeyRangeRef`) a single-key range `[k, k\x00)` is compressed; that is the
/// wire types' business, not this type's.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyRange {
    /// The first key in the range.
    pub begin: Vec<u8>,
    /// The first key after the range.
    pub end: Vec<u8>,
}

impl KeyRange {
    /// `[begin, end)`.
    pub fn new(begin: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> KeyRange {
        KeyRange {
            begin: begin.into(),
            end: end.into(),
        }
    }

    /// `singleKeyRange(key)`: `[key, key\x00)`.
    pub fn single_key(key: &[u8]) -> KeyRange {
        let mut end = Vec::with_capacity(key.len() + 1);
        end.extend_from_slice(key);
        end.push(0);
        KeyRange {
            begin: key.to_vec(),
            end,
        }
    }
}

/// `MutationRef::Type`: a mutation's type byte.
///
/// The value is kept as it is on the wire. Types with the checksum flags (bits 7 and 6,
/// written only when the `ENABLE_MUTATION_CHECKSUM` client knob is set) are not interpreted:
/// see [`crate::v7_4::MutationRef`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MutationType(pub u8);

impl MutationType {
    /// `SetValue`.
    pub const SET_VALUE: MutationType = MutationType(0);
    /// `ClearRange`: clears `[param1, param2)`.
    pub const CLEAR_RANGE: MutationType = MutationType(1);
    /// `AddValue`.
    pub const ADD_VALUE: MutationType = MutationType(2);
    /// `And`.
    pub const AND: MutationType = MutationType(6);
    /// `Or`.
    pub const OR: MutationType = MutationType(7);
    /// `Xor`.
    pub const XOR: MutationType = MutationType(8);
    /// `AppendIfFits`.
    pub const APPEND_IF_FITS: MutationType = MutationType(9);
    /// `Max`.
    pub const MAX: MutationType = MutationType(12);
    /// `Min`.
    pub const MIN: MutationType = MutationType(13);
    /// `SetVersionstampedKey`.
    pub const SET_VERSIONSTAMPED_KEY: MutationType = MutationType(14);
    /// `SetVersionstampedValue`.
    pub const SET_VERSIONSTAMPED_VALUE: MutationType = MutationType(15);
    /// `ByteMin`.
    pub const BYTE_MIN: MutationType = MutationType(16);
    /// `ByteMax`.
    pub const BYTE_MAX: MutationType = MutationType(17);
    /// `MinV2`.
    pub const MIN_V2: MutationType = MutationType(18);
    /// `AndV2`.
    pub const AND_V2: MutationType = MutationType(19);
    /// `CompareAndClear`.
    pub const COMPARE_AND_CLEAR: MutationType = MutationType(20);
}

/// A mutation (`MutationRef`): a type and two parameters, whose meaning depends on the type
/// (key and value for `SetValue`, begin and end for `ClearRange`, ...).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Mutation {
    /// The type.
    pub kind: MutationType,
    /// `param1`.
    pub param1: Vec<u8>,
    /// `param2`.
    pub param2: Vec<u8>,
}

impl Mutation {
    /// A mutation of type `kind`.
    pub fn new(
        kind: MutationType,
        param1: impl Into<Vec<u8>>,
        param2: impl Into<Vec<u8>>,
    ) -> Mutation {
        Mutation {
            kind,
            param1: param1.into(),
            param2: param2.into(),
        }
    }
}

// ---------------------------------------------------------------------------------------
// Interfaces.

/// `LocalityData`: a process's locality (`zoneid`, `dcid`, `machineid`, `processid`, ...),
/// key to optional value.
pub type Locality = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

/// A GRV proxy (`GrvProxyInterface`), as `ClientDBInfo` lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrvProxy {
    /// `processId`.
    pub process_id: Option<Vec<u8>>,
    /// `provisional`.
    pub provisional: bool,
    /// The base endpoint, `getConsistentReadVersion`.
    pub base: Endpoint,
}

impl GrvProxy {
    /// `id()`: the base endpoint's token.
    pub const fn id(&self) -> Uid {
        self.base.token
    }

    /// Every stream's endpoint ([`GRV_PROXY_STREAMS`]).
    pub fn endpoints(&self) -> GrvProxyStreams<Endpoint> {
        GRV_PROXY_STREAMS.map(|i| self.base.adjusted(i))
    }
}

/// A commit proxy (`CommitProxyInterface`), as `ClientDBInfo` lists it.
///
/// The adjusted-endpoint indices depend on the version, so the proxy remembers the schema
/// set it was decoded with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitProxy {
    /// `processId`.
    pub process_id: Option<Vec<u8>>,
    /// `provisional`.
    pub provisional: bool,
    /// The base endpoint, `commit`.
    pub base: Endpoint,
    /// The version whose stream indices apply.
    pub schema: SchemaVersion,
}

impl CommitProxy {
    /// `id()`: the base endpoint's token.
    pub const fn id(&self) -> Uid {
        self.base.token
    }

    /// Every stream's endpoint, with this proxy's version's indices
    /// ([`SchemaVersion::commit_proxy_streams`]).
    pub fn endpoints(&self) -> CommitProxyStreams<Endpoint> {
        self.schema
            .commit_proxy_streams()
            .map(|i| self.base.adjusted(i))
    }
}

/// A storage server (`StorageServerInterface`), as `GetKeyServerLocationsReply` lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageServer {
    /// `uniqueID`, the server's id.
    pub id: Uid,
    /// `locality`.
    pub locality: Locality,
    /// The base endpoint, `getValue`.
    pub base: Endpoint,
    /// `tssPairID`: set if this is a testing storage server, to the id of the server it
    /// shadows.
    pub tss_pair_id: Option<Uid>,
    /// `acceptingRequests`.
    pub accepting_requests: bool,
}

impl StorageServer {
    /// Every stream's endpoint ([`STORAGE_SERVER_STREAMS`]).
    pub fn endpoints(&self) -> StorageServerStreams<Endpoint> {
        STORAGE_SERVER_STREAMS.map(|i| self.base.adjusted(i))
    }
}

// ---------------------------------------------------------------------------------------
// Coordinator: OpenDatabaseCoordRequest -> ClientDBInfo.

/// `ClientVersionRef`: a client library version the client supports, reported to the
/// cluster for status.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientVersion {
    /// `clientVersion`, such as `7.4.10`.
    pub client_version: Vec<u8>,
    /// `sourceVersion`: the source revision.
    pub source_version: Vec<u8>,
    /// `protocolVersion`, as hex text.
    pub protocol_version: Vec<u8>,
}

/// `Hostname`: a coordinator given by name in the cluster file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Hostname {
    /// `host`.
    pub host: Vec<u8>,
    /// `service`: the port.
    pub service: Vec<u8>,
    /// `isTLS`.
    pub is_tls: bool,
}

/// `OpenDatabaseCoordRequest`: the client's long poll for [`ClientDbInfo`], sent to a
/// coordinator's well-known endpoint `UID(-1, 4)`. The coordinator replies when the
/// cluster's `ClientDBInfo` id differs from `known_client_info_id`. Identical on 7.4 and 8.0.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OpenDatabaseCoord {
    /// `issues`: client issues to report in status.
    pub issues: Vec<Vec<u8>>,
    /// `supportedVersions`.
    pub supported_versions: Vec<ClientVersion>,
    /// `traceLogGroup`.
    pub trace_log_group: Vec<u8>,
    /// `knownClientInfoID`: the id of the `ClientDBInfo` the client has (zero at first).
    pub known_client_info_id: Uid,
    /// `clusterKey`: `description:id` from the cluster file.
    pub cluster_key: Vec<u8>,
    /// `coordinators`: the cluster file's addresses.
    pub coordinators: Vec<NetworkAddress>,
    /// The reply token.
    pub reply: Uid,
    /// `hostnames`: the cluster file's hostnames.
    pub hostnames: Vec<Hostname>,
    /// `internal`: whether the client is internal to the cluster (`true` in C++'s default).
    pub internal: bool,
}

/// `VersionHistory`: global configuration mutations made at one version.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VersionHistory {
    /// `version`.
    pub version: i64,
    /// `mutations`.
    pub mutations: Vec<Mutation>,
}

/// `ClientDBInfo`: the proxies a client talks to, from the coordinators.
///
/// Version-specific fields a client does not use are not carried, and are written as their
/// defaults: on 7.4 `tenantMode`, `encryptKeyProxy` and `metaclusterName`, on 8.0 the
/// native CDC fields (`nativeCdcEnabled`, `nativeCdcTagCount`, `cdcProxies`,
/// `streamToCDCProxyId`). The wire types have them all.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientDbInfo {
    /// `id`: changes whenever anything else does.
    pub id: Uid,
    /// `grvProxies`.
    pub grv_proxies: Vec<GrvProxy>,
    /// `commitProxies`.
    pub commit_proxies: Vec<CommitProxy>,
    /// `forward`: a connection string the client should switch to.
    pub forward: Option<Vec<u8>>,
    /// `history`: recent global configuration changes.
    pub history: Vec<VersionHistory>,
    /// `clusterId`.
    pub cluster_id: Uid,
    /// `clusterType` (`ClusterType`: 0 = standalone).
    pub cluster_type: i32,
}

// ---------------------------------------------------------------------------------------
// GRV proxy: GetReadVersionRequest -> GetReadVersionReply.

/// `GetReadVersionRequest`, sent to a GRV proxy's `getConsistentReadVersion`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetReadVersion {
    /// `transactionCount`: how many transactions this request batches.
    pub transaction_count: u32,
    /// `flags`: a `PRIORITY_*` value in the high byte and `FLAG_*` bits.
    pub flags: u32,
    /// `tags`: transaction tag to number of transactions with it.
    pub tags: BTreeMap<Vec<u8>, u32>,
    /// `debugID`.
    pub debug_id: Option<Uid>,
    /// The reply token.
    pub reply: Uid,
    /// `spanContext`.
    pub span_context: SpanContext,
    /// `maxVersion`: the largest version in the client's version-vector cache
    /// ([`INVALID_VERSION`] when it has none).
    pub max_version: i64,
    /// `maxGrvQueueDelayMS` (8.0 only; not sent to 7.4).
    pub max_grv_queue_delay_ms: Option<i64>,
}

impl GetReadVersion {
    /// `PRIORITY_SYSTEM_IMMEDIATE`.
    pub const PRIORITY_SYSTEM_IMMEDIATE: u32 = 15 << 24;
    /// `PRIORITY_DEFAULT`.
    pub const PRIORITY_DEFAULT: u32 = 8 << 24;
    /// `PRIORITY_BATCH`.
    pub const PRIORITY_BATCH: u32 = 1 << 24;
    /// `FLAG_PRIORITY_MASK`.
    pub const FLAG_PRIORITY_MASK: u32 = Self::PRIORITY_SYSTEM_IMMEDIATE;
    /// `FLAG_CAUSAL_READ_RISKY`.
    pub const FLAG_CAUSAL_READ_RISKY: u32 = 1;
    /// `FLAG_USE_PROVISIONAL_PROXIES`.
    pub const FLAG_USE_PROVISIONAL_PROXIES: u32 = 2;
    /// `FLAG_USE_MIN_KNOWN_COMMITTED_VERSION`.
    pub const FLAG_USE_MIN_KNOWN_COMMITTED_VERSION: u32 = 4;
}

/// `ClientTagThrottleLimits`, with its expiration on the local clock.
///
/// On the wire the limit carries a duration (`expiration - now()` on the sender's clock);
/// the conversions take the receiver's `now` and store `now + duration` here, as C++ does.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TagThrottleLimits {
    /// `tpsRate`.
    pub tps_rate: f64,
    /// `expiration`, in seconds on the clock whose `now` was given to the decode.
    pub expiration: f64,
}

/// `GetReadVersionReply`.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadVersion {
    /// `processBusyTime` (`BasicLoadBalancedReply`).
    pub process_busy_time: i32,
    /// `version`: the read version.
    pub version: i64,
    /// `locked`: whether the database is locked.
    pub locked: bool,
    /// `metadataVersion`: the value of `\xff/metadataVersion`.
    pub metadata_version: Option<Vec<u8>>,
    /// `tagThrottleInfo`.
    pub tag_throttle_info: BTreeMap<Vec<u8>, TagThrottleLimits>,
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
    /// `proxyTagThrottledDuration` (7.4 only; 0 from 8.0, and not sent to 8.0).
    pub proxy_tag_throttled_duration: f64,
}

// ---------------------------------------------------------------------------------------
// Commit proxy: GetKeyServerLocationsRequest -> GetKeyServerLocationsReply.

/// `GetKeyServerLocationsRequest`, sent to a commit proxy's `getKeyServersLocations`: the
/// storage servers of the shards from `begin` (or, with `reverse`, ending at `begin`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetKeyServerLocations {
    /// `begin`.
    pub begin: Vec<u8>,
    /// `end`: where to stop (`None`: only the shard holding `begin`).
    pub end: Option<Vec<u8>>,
    /// `limit`: the most shards to return.
    pub limit: i32,
    /// `reverse`.
    pub reverse: bool,
    /// The reply token.
    pub reply: Uid,
    /// `spanContext`.
    pub span_context: SpanContext,
    /// `minTenantVersion` on 7.4, `legacyVersion` on 8.0 (the same slot's value, renamed when
    /// tenants were removed). Clients send [`LATEST_VERSION`].
    pub min_tenant_version: i64,
}

/// `GetKeyServerLocationsReply`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyServerLocations {
    /// `results`: each shard's range and its storage servers.
    pub results: Vec<(KeyRange, Vec<StorageServer>)>,
    /// `resultsTssMapping`: storage server id to its testing storage server.
    pub tss_mapping: Vec<(Uid, StorageServer)>,
    /// `resultsTagMapping`: storage server id to its tag.
    pub tag_mapping: Vec<(Uid, Tag)>,
}

// ---------------------------------------------------------------------------------------
// Storage server: GetValueRequest -> GetValueReply.

/// `ReadOptions`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadOptions {
    /// `type`, a `ReadType` (`EAGER` = 0, `FETCH`, `LOW`, `NORMAL`, `HIGH`). An `int32_t` enum
    /// on 7.4 and a `uint32_t` one on 8.0; the bits are kept.
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

/// `GetValueRequest`, sent to a storage server's `getValue`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetValue {
    /// `key`.
    pub key: Vec<u8>,
    /// `version`: the read version.
    pub version: i64,
    /// `tags`: the transaction's tags.
    pub tags: Option<TagSet>,
    /// The reply token.
    pub reply: Uid,
    /// `spanContext`.
    pub span_context: SpanContext,
    /// `options`.
    pub options: Option<ReadOptions>,
    /// `ssLatestCommitVersions`: the version vector entries the client knows for the
    /// storage servers it reads from (empty unless version vectors are enabled).
    pub ss_latest_commit_versions: VersionVector,
}

/// `GetValueReply`. Borrows the value from the reply message.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct GetValueReply<'a> {
    /// `penalty` (`LoadBalancedReply`): the server's load, for load balancing.
    pub penalty: f64,
    /// `error` (`LoadBalancedReply`): set when the server could not serve the read (the
    /// load balancer retries elsewhere).
    pub error: Option<FdbError>,
    /// `value`: `None` if the key is not set.
    pub value: Option<&'a [u8]>,
    /// `cached`.
    pub cached: bool,
}

// ---------------------------------------------------------------------------------------
// Commit proxy: CommitTransactionRequest -> CommitID.

/// `ClientTrCommitCostEstimation`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitCostEstimation {
    /// `opsCount`.
    pub ops_count: i32,
    /// `writeCosts`.
    pub write_costs: u64,
    /// `clearIdxCosts`: mutation index to cost.
    pub clear_idx_costs: Vec<(i32, u64)>,
    /// `expensiveCostEstCount`.
    pub expensive_cost_est_count: u32,
}

/// `CommitTransactionRequest` (with its `CommitTransactionRef`), sent to a commit proxy's
/// `commit`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Commit {
    /// `transaction.read_conflict_ranges`.
    pub read_conflict_ranges: Vec<KeyRange>,
    /// `transaction.write_conflict_ranges`.
    pub write_conflict_ranges: Vec<KeyRange>,
    /// `transaction.mutations`.
    pub mutations: Vec<Mutation>,
    /// `transaction.read_snapshot`: the read version.
    pub read_snapshot: i64,
    /// `transaction.report_conflicting_keys`.
    pub report_conflicting_keys: bool,
    /// `transaction.lock_aware`.
    pub lock_aware: bool,
    /// `transaction.spanContext`.
    pub transaction_span_context: Option<SpanContext>,
    /// The reply token.
    pub reply: Uid,
    /// `flags`: [`Commit::FLAG_IS_LOCK_AWARE`], ...
    pub flags: u32,
    /// `debugID`.
    pub debug_id: Option<Uid>,
    /// `commitCostEstimation`.
    pub commit_cost_estimation: Option<CommitCostEstimation>,
    /// `tagSet`.
    pub tag_set: Option<TagSet>,
    /// `spanContext`.
    pub span_context: SpanContext,
    /// `idempotencyId` (written as an empty string when `None`).
    pub idempotency_id: Option<IdempotencyId>,
}

impl Commit {
    /// `FLAG_IS_LOCK_AWARE`.
    pub const FLAG_IS_LOCK_AWARE: u32 = 0x1;
    /// `FLAG_FIRST_IN_BATCH`.
    pub const FLAG_FIRST_IN_BATCH: u32 = 0x2;
    /// `FLAG_BYPASS_STORAGE_QUOTA`.
    pub const FLAG_BYPASS_STORAGE_QUOTA: u32 = 0x4;
}

/// `CommitID`: the reply to a commit.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitId {
    /// `version`: the commit version ([`INVALID_VERSION`] if the transaction conflicted).
    pub version: i64,
    /// `txnBatchId`: the transaction's index in its batch (for versionstamps).
    pub txn_batch_id: u16,
    /// `metadataVersion`.
    pub metadata_version: Option<Vec<u8>>,
    /// `conflictingKRIndices`: the indices of the read conflict ranges that conflicted, when
    /// `report_conflicting_keys` was set.
    pub conflicting_kr_indices: Option<Vec<i32>>,
}
