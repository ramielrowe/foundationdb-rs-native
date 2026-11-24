//! Absent tables read as what C++ default-constructs.
//!
//! `LoadMember` (`flow/include/flow/flat_buffers.h`, identical on 7.4 and 8.0) assigns
//! `std::decay_t<decltype(member)>{}` to a member whose slot is absent: for a table-typed
//! member the type's constructor and member initializers apply (`LoadBalancedReply`'s
//! `penalty = 1.0`, `GetReadVersionReply`'s `version = invalidVersion`, ...), for a scalar
//! it is zero. So the same type reads differently when its *table* is absent (the C++
//! default) and when the table is present but its vtable does not reach a scalar slot
//! (zero). Each test here builds both messages by hand with the real types.

use fdb_native_schema::domain::{
    INVALID_VERSION, LATEST_VERSION, ReadOptions as DomainReadOptions,
};
use std::sync::LazyLock;

use fdb_native_schema::{SpanContext, v7_4, v8_0};
use fdb_native_wire::{Decode, Uid, Wire, decode, encode};

// Shared by both releases; both schema sets re-export them.
use v7_4::{
    ClientTagThrottleLimits, ClientTrCommitCostEstimation, ClientVersionRef, CommitID,
    CommitProxyInterface, CommitTransactionRef, GetKeyReply, GetKeyServerLocationsReply,
    GetKeyValuesReply, GetValueReply, GrvProxyInterface, Hostname, KeyRangeRef, KeySelectorRef,
    LocalityData, MutationRef, OpenDatabaseCoordRequest, SplitRangeReply, StorageMetrics,
    StorageServerInterface, VersionHistory, WatchValueReply,
};

/// A table with `value` first, as the reader sees it: `serializer(ar, value, tail)`.
#[derive(Wire)]
#[fdb(file_identifier = 0x000fdb50)]
struct Reads<T> {
    value: T,
    tail: u8,
}

/// The same message with `value`'s slot absent (a table-typed member takes one slot).
#[derive(Wire)]
#[fdb(file_identifier = 0x000fdb50)]
#[allow(dead_code)] // written, never read
struct WithoutValue {
    #[fdb(skip_slots = 1)]
    value: (),
    tail: u8,
}

/// A present table with no slots at all.
#[derive(Wire)]
#[allow(dead_code)] // written, never read
struct NoSlots {
    #[fdb(arena)]
    arena: (),
}

/// The same message with `value` a present, empty table: every slot of it is missing.
#[derive(Wire)]
#[fdb(file_identifier = 0x000fdb50)]
struct WithEmptyValue {
    value: NoSlots,
    tail: u8,
}

/// The message whose table member is absent.
static ABSENT: LazyLock<Vec<u8>> =
    LazyLock::new(|| encode(&WithoutValue { value: (), tail: 7 }).unwrap());

/// The message whose table member is present with no slots.
static EMPTY: LazyLock<Vec<u8>> = LazyLock::new(|| {
    encode(&WithEmptyValue {
        value: NoSlots { arena: () },
        tail: 7,
    })
    .unwrap()
});

/// `T` as it reads from a message whose table member is absent.
macro_rules! absent_in_message {
    ($t:ty) => {{
        let read: Reads<$t> = decode(&ABSENT[..]).unwrap();
        assert_eq!(read.tail, 7);
        read.value
    }};
}

/// `T` as it reads from a message whose table member is present with no slots.
macro_rules! empty_in_message {
    ($t:ty) => {{
        let read: Reads<$t> = decode(&EMPTY[..]).unwrap();
        assert_eq!(read.tail, 7);
        read.value
    }};
}

// ---------------------------------------------------------------------------------------
// Types shared by both releases.

#[test]
fn get_value_reply() {
    // LoadBalancedReply() : penalty(1.0)
    let reply = absent_in_message!(GetValueReply);
    assert_eq!(reply, GetValueReply::default());
    assert_eq!(reply.penalty, 1.0);
    assert_eq!(<GetValueReply as Decode>::absent(), reply);

    // Present, without a `penalty` slot: a scalar, zero.
    let reply = empty_in_message!(GetValueReply);
    assert_eq!(reply.penalty, 0.0);
    assert_eq!(reply.error, None);
    assert_eq!(reply.value, None);
    assert!(!reply.cached);
}

#[test]
fn get_key_reply() {
    // LoadBalancedReply() : penalty(1.0); GetKeyReply() : cached(false), a KeySelector()
    let reply = absent_in_message!(GetKeyReply);
    assert_eq!(reply, GetKeyReply::default());
    assert_eq!(reply.penalty, 1.0);
    assert_eq!(reply.sel, KeySelectorRef::default());
    assert_eq!(<GetKeyReply as Decode>::absent(), reply);

    // Present, without a `penalty` slot: a scalar, zero.
    let reply = empty_in_message!(GetKeyReply);
    assert_eq!(reply.penalty, 0.0);
    assert_eq!(reply.error, None);
    assert_eq!(reply.sel, KeySelectorRef::default());
    assert!(!reply.cached);
}

#[test]
fn get_key_values_reply() {
    // LoadBalancedReply() : penalty(1.0); GetKeyValuesReply() : version(invalidVersion),
    // more(false), cached(false)
    let reply = absent_in_message!(GetKeyValuesReply);
    assert_eq!(reply, GetKeyValuesReply::default());
    assert_eq!(reply.penalty, 1.0);
    assert_eq!(reply.version, INVALID_VERSION);
    assert!(reply.data.is_empty());
    assert!(!reply.more && !reply.cached);
    assert_eq!(<GetKeyValuesReply as Decode>::absent(), reply);

    // Present, with no slots: scalars are zero, and an absent blob is the empty vector.
    let reply = empty_in_message!(GetKeyValuesReply);
    assert_eq!((reply.penalty, reply.version), (0.0, 0));
    assert!(reply.data.is_empty());
}

#[test]
fn watch_value_reply() {
    // WatchValueReply() = default, `cached = false`: zero.
    assert_eq!(
        absent_in_message!(WatchValueReply),
        WatchValueReply::default()
    );
    assert_eq!(
        empty_in_message!(WatchValueReply),
        WatchValueReply::default()
    );
    assert_eq!(WatchValueReply::default().version, 0);
}

#[test]
fn key_selector_ref() {
    // KeySelectorRef() : orEqual(false), offset(0)
    assert_eq!(
        absent_in_message!(KeySelectorRef),
        KeySelectorRef::default()
    );
    assert_eq!(empty_in_message!(KeySelectorRef), KeySelectorRef::default());
}

#[test]
fn commit_id() {
    // CommitID() : version(invalidVersion), txnBatchId(0)
    let id = absent_in_message!(CommitID);
    assert_eq!(id, CommitID::default());
    assert_eq!(id.version, INVALID_VERSION);

    assert_eq!(empty_in_message!(CommitID).version, 0);
}

#[test]
fn open_database_coord_request() {
    // `bool internal{ true }`
    let request = absent_in_message!(OpenDatabaseCoordRequest);
    assert_eq!(request, OpenDatabaseCoordRequest::default());
    assert!(request.internal);
    assert!(request.trace_log_group.is_empty());
    assert_eq!(request.known_client_info_id, Uid::default());

    assert!(!empty_in_message!(OpenDatabaseCoordRequest).internal);
}

#[test]
fn client_version_ref() {
    // ClientVersionRef() { initUnknown(); }
    let version = absent_in_message!(ClientVersionRef);
    assert_eq!(version, ClientVersionRef::default());
    assert_eq!(version.client_version, b"Unknown");
    assert_eq!(version.source_version, b"Unknown");
    assert_eq!(version.protocol_version, b"Unknown");

    let version = empty_in_message!(ClientVersionRef);
    assert_eq!(version.client_version, b"");
}

#[test]
fn mutation_ref() {
    // MutationRef() : type(MAX_ATOMIC_OP)
    let mutation = absent_in_message!(MutationRef);
    assert_eq!(mutation.kind, MutationRef::MAX_ATOMIC_OP);
    assert_eq!(mutation.kind, 24);
    assert!(mutation.param1.is_empty() && mutation.param2.is_empty());

    // Present, `type` missing: a scalar, 0 (SetValue).
    assert_eq!(empty_in_message!(MutationRef).kind, 0);
}

/// Types whose C++ default constructor leaves everything zero or empty: an absent table
/// and a present empty one agree.
#[test]
fn zero_default_types() {
    assert_eq!(absent_in_message!(KeyRangeRef), KeyRangeRef::default());
    assert_eq!(empty_in_message!(KeyRangeRef), KeyRangeRef::default());
    assert_eq!(absent_in_message!(LocalityData), LocalityData::default());
    assert_eq!(
        absent_in_message!(GrvProxyInterface),
        GrvProxyInterface::default()
    );
    assert_eq!(
        absent_in_message!(CommitProxyInterface),
        CommitProxyInterface::default()
    );
    assert_eq!(
        absent_in_message!(VersionHistory),
        VersionHistory::default()
    );
    assert_eq!(absent_in_message!(Hostname), Hostname::default());
    assert_eq!(
        absent_in_message!(ClientTagThrottleLimits),
        ClientTagThrottleLimits::default()
    );
    assert_eq!(
        absent_in_message!(ClientTrCommitCostEstimation),
        ClientTrCommitCostEstimation::default()
    );
    assert_eq!(
        absent_in_message!(CommitTransactionRef),
        CommitTransactionRef::default()
    );
    assert_eq!(absent_in_message!(SpanContext), SpanContext::default());
    // `StorageServerInterface()` has a random `uniqueID`; every other member is zero.
    assert_eq!(
        absent_in_message!(StorageServerInterface),
        StorageServerInterface::default()
    );
    let reply = absent_in_message!(GetKeyServerLocationsReply);
    assert_eq!(reply.results.len(), 0);
    assert!(reply.results_tss_mapping.is_empty() && reply.results_tag_mapping.is_empty());
    // StorageMetrics: every member `= 0`; SplitRangeReply(): no split points.
    assert_eq!(
        absent_in_message!(StorageMetrics),
        StorageMetrics::default()
    );
    assert_eq!(empty_in_message!(StorageMetrics), StorageMetrics::default());
    assert_eq!(
        absent_in_message!(SplitRangeReply),
        SplitRangeReply::default()
    );
}

// ---------------------------------------------------------------------------------------
// 7.4.

#[test]
fn v7_4_tenant_info() {
    // TenantInfo() : tenantId(INVALID_TENANT)
    let tenant = absent_in_message!(v7_4::TenantInfo);
    assert_eq!(tenant, v7_4::TenantInfo::NONE);
    assert_eq!(tenant.tenant_id, -1);

    assert_eq!(empty_in_message!(v7_4::TenantInfo).tenant_id, 0);
}

#[test]
fn v7_4_get_read_version_request() {
    // GetReadVersionRequest() : transactionCount(1), flags(0), maxVersion(invalidVersion)
    let request = absent_in_message!(v7_4::GetReadVersionRequest);
    assert_eq!(request, v7_4::GetReadVersionRequest::default());
    assert_eq!(request.transaction_count, 1);
    assert_eq!(request.max_version, INVALID_VERSION);

    let request = empty_in_message!(v7_4::GetReadVersionRequest);
    assert_eq!((request.transaction_count, request.max_version), (0, 0));
}

#[test]
fn v7_4_get_read_version_reply() {
    // GetReadVersionReply() : version(invalidVersion), locked(false)
    let reply = absent_in_message!(v7_4::GetReadVersionReply);
    assert_eq!(reply, v7_4::GetReadVersionReply::default());
    assert_eq!(reply.version, INVALID_VERSION);

    assert_eq!(empty_in_message!(v7_4::GetReadVersionReply).version, 0);
}

#[test]
fn v7_4_get_key_server_locations_request() {
    // GetKeyServerLocationsRequest() : limit(0), reverse(false), minTenantVersion(latestVersion)
    // and a TenantInfo() member.
    let request = absent_in_message!(v7_4::GetKeyServerLocationsRequest);
    assert_eq!(request, v7_4::GetKeyServerLocationsRequest::default());
    assert_eq!(request.min_tenant_version, LATEST_VERSION);
    assert_eq!(request.tenant, v7_4::TenantInfo::NONE);

    // Present but empty: a missing scalar is 0, a missing `tenant` table is TenantInfo().
    let request = empty_in_message!(v7_4::GetKeyServerLocationsRequest);
    assert_eq!(request.min_tenant_version, 0);
    assert_eq!(request.tenant, v7_4::TenantInfo::NONE);
}

#[test]
fn v7_4_read_options() {
    // ReadOptions(): type NORMAL, cacheResult true
    let options = absent_in_message!(v7_4::ReadOptions);
    assert_eq!(options, v7_4::ReadOptions::default());
    assert_eq!(options.read_type, DomainReadOptions::NORMAL as i32);
    assert!(options.cache_result);
    assert!(!options.lock_aware);

    let options = empty_in_message!(v7_4::ReadOptions);
    assert_eq!(options.read_type, 0);
    assert!(!options.cache_result);
}

/// A table of a nested absent member: the containing type's own absent value is built from
/// each field's, so `TenantInfo()` shows through.
#[test]
fn v7_4_requests_compose_nested_defaults() {
    let request = absent_in_message!(v7_4::GetKeyRequest);
    assert_eq!(request.tenant_info, v7_4::TenantInfo::NONE);
    assert_eq!(request.sel, KeySelectorRef::default());
    assert_eq!(
        request.ss_latest_commit_versions.max_version,
        INVALID_VERSION
    );
    assert_eq!(request, v7_4::GetKeyRequest::default());

    let request = absent_in_message!(v7_4::GetKeyValuesRequest);
    assert_eq!(request.tenant_info, v7_4::TenantInfo::NONE);
    assert_eq!(
        (request.limit, request.limit_bytes, request.version),
        (0, 0, 0)
    );
    assert_eq!(
        request.ss_latest_commit_versions.max_version,
        INVALID_VERSION
    );
    assert_eq!(request, v7_4::GetKeyValuesRequest::default());

    let request = absent_in_message!(v7_4::WatchValueRequest);
    assert_eq!(request.tenant_info, v7_4::TenantInfo::NONE);
    assert_eq!(request, v7_4::WatchValueRequest::default());
    assert_eq!(
        empty_in_message!(v7_4::WatchValueRequest).tenant_info,
        v7_4::TenantInfo::NONE
    );

    let request = absent_in_message!(v7_4::GetValueRequest);
    assert_eq!(request.tenant_info, v7_4::TenantInfo::NONE);
    assert_eq!(request.version, 0);
    assert_eq!(request.options, None);
    // `VersionVector()`: no entries, `maxVersion = invalidVersion`; every other field is zero.
    assert_eq!(
        request.ss_latest_commit_versions.max_version,
        INVALID_VERSION
    );
    assert_eq!(request, v7_4::GetValueRequest::default());

    let request = absent_in_message!(v7_4::CommitTransactionRequest);
    assert_eq!(request.tenant_info, v7_4::TenantInfo::NONE);
    assert_eq!(request, v7_4::CommitTransactionRequest::default());
    assert_eq!(
        empty_in_message!(v7_4::CommitTransactionRequest).tenant_info,
        v7_4::TenantInfo::NONE
    );

    assert_eq!(
        absent_in_message!(v7_4::ClientDBInfo),
        v7_4::ClientDBInfo::default()
    );
    assert_eq!(
        absent_in_message!(v7_4::TenantMode),
        v7_4::TenantMode::default()
    );

    // WaitMetricsRequest() {} and SplitRangeRequest() {}: zero but `tenantInfo`.
    let request = absent_in_message!(v7_4::WaitMetricsRequest);
    assert_eq!(request.tenant_info, v7_4::TenantInfo::NONE);
    assert_eq!(request, v7_4::WaitMetricsRequest::default());
    let request = absent_in_message!(v7_4::SplitRangeRequest);
    assert_eq!(request.tenant_info, v7_4::TenantInfo::NONE);
    assert_eq!(request, v7_4::SplitRangeRequest::default());
}

// ---------------------------------------------------------------------------------------
// 8.0.

#[test]
fn v8_0_get_read_version_request() {
    let request = absent_in_message!(v8_0::GetReadVersionRequest);
    assert_eq!(request, v8_0::GetReadVersionRequest::default());
    assert_eq!(request.transaction_count, 1);
    assert_eq!(request.max_version, INVALID_VERSION);
    assert_eq!(request.max_grv_queue_delay_ms, None);

    let request = empty_in_message!(v8_0::GetReadVersionRequest);
    assert_eq!((request.transaction_count, request.max_version), (0, 0));
}

#[test]
fn v8_0_get_read_version_reply() {
    let reply = absent_in_message!(v8_0::GetReadVersionReply);
    assert_eq!(reply, v8_0::GetReadVersionReply::default());
    assert_eq!(reply.version, INVALID_VERSION);

    assert_eq!(empty_in_message!(v8_0::GetReadVersionReply).version, 0);
}

#[test]
fn v8_0_get_key_server_locations_request() {
    // GetKeyServerLocationsRequest() : limit(0), reverse(false), legacyVersion(latestVersion)
    let request = absent_in_message!(v8_0::GetKeyServerLocationsRequest);
    assert_eq!(request, v8_0::GetKeyServerLocationsRequest::default());
    assert_eq!(request.legacy_version, LATEST_VERSION);

    assert_eq!(
        empty_in_message!(v8_0::GetKeyServerLocationsRequest).legacy_version,
        0
    );
}

#[test]
fn v8_0_storage_metrics_requests() {
    let request = absent_in_message!(v8_0::WaitMetricsRequest);
    assert_eq!(request, v8_0::WaitMetricsRequest::default());
    assert_eq!(request.legacy_version, 0);

    // SplitRangeRequest() = default with `int limit = -1`.
    let request = absent_in_message!(v8_0::SplitRangeRequest);
    assert_eq!(request, v8_0::SplitRangeRequest::default());
    assert_eq!(request.limit, -1);
    assert_eq!(empty_in_message!(v8_0::SplitRangeRequest).limit, 0);
}

#[test]
fn v8_0_read_options() {
    let options = absent_in_message!(v8_0::ReadOptions);
    assert_eq!(options, v8_0::ReadOptions::default());
    assert_eq!(options.read_type, DomainReadOptions::NORMAL);
    assert!(options.cache_result);

    let options = empty_in_message!(v8_0::ReadOptions);
    assert_eq!(options.read_type, 0);
    assert!(!options.cache_result);
}

#[test]
fn v8_0_zero_default_types() {
    assert_eq!(
        absent_in_message!(v8_0::ClientDBInfo),
        v8_0::ClientDBInfo::default()
    );
    assert_eq!(
        absent_in_message!(v8_0::CdcProxyInterface),
        v8_0::CdcProxyInterface::default()
    );
    let request = absent_in_message!(v8_0::GetValueRequest);
    assert_eq!(request, v8_0::GetValueRequest::default());
    // `VersionVector()`: `maxVersion = invalidVersion`.
    assert_eq!(
        request.ss_latest_commit_versions.max_version,
        INVALID_VERSION
    );
    assert_eq!(
        absent_in_message!(v8_0::CommitTransactionRequest),
        v8_0::CommitTransactionRequest::default()
    );
    // `VersionVector()`: `maxVersion = invalidVersion`; every other field is zero.
    let request = absent_in_message!(v8_0::GetKeyRequest);
    assert_eq!(request, v8_0::GetKeyRequest::default());
    assert_eq!(
        request.ss_latest_commit_versions.max_version,
        INVALID_VERSION
    );
    let request = absent_in_message!(v8_0::GetKeyValuesRequest);
    assert_eq!(request, v8_0::GetKeyValuesRequest::default());
    assert_eq!(
        request.ss_latest_commit_versions.max_version,
        INVALID_VERSION
    );
    assert_eq!(request.task_id, None);
    assert_eq!(
        absent_in_message!(v8_0::WatchValueRequest),
        v8_0::WatchValueRequest::default()
    );
}

/// Both schema sets, for what they share: the two releases agree on every default here
/// (the C++ constructors did not change between them).
#[test]
fn both_releases_agree() {
    assert_eq!(
        absent_in_message!(v7_4::GetReadVersionReply).version,
        absent_in_message!(v8_0::GetReadVersionReply).version
    );
    assert_eq!(
        absent_in_message!(v7_4::ReadOptions).read_type as u32,
        absent_in_message!(v8_0::ReadOptions).read_type
    );
}
