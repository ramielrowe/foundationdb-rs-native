//! Shared test support for the message tests: fixture loading, a dumper that turns the
//! schema types into the fixtures' JSON node format (tools/golden-vectors/README.md,
//! "Nodes"), and a JSON comparator. Modelled on fdb-native-wire's tests/support.
//!
//! The `Dump` impls restate each type's C++ name and field names; comparing a decoded value's
//! dump with a fixture's `root` therefore checks the Rust field order, names, types and
//! values against what the release's C++ wrote.

#![allow(dead_code, unreachable_pub)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use fdb_native_schema::{SpanContext, TraceFlags, v7_4};
use fdb_native_wire::{
    Decode, Endpoint, EnsureTable, ErrorOr, FdbError, IdempotencyIdRef, IpAddress, NetworkAddress,
    NetworkAddressList, ReplyPromise, RequestStream, Tag, TagSetRef, Uid, VectorView,
    VersionVector, Wire,
};
use serde_json::{Map, Value, json};

pub const RELEASES: [&str; 2] = ["release-7.4", "release-8.0"];

// ---------------------------------------------------------------------------------------
// Fixtures.

pub fn fixtures_dir(release: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tools/golden-vectors/fixtures")
        .join(release)
}

pub fn read_json(path: &Path) -> Value {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parsing {}: {e}", path.display()))
}

pub fn fixture(release: &str, name: &str) -> Value {
    read_json(&fixtures_dir(release).join(format!("{name}.json")))
}

pub fn hex_decode(s: &str) -> Vec<u8> {
    assert!(s.len() % 2 == 0, "odd hex length");
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).expect("hex"))
        .collect()
}

pub fn hex_encode(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn record_bytes(record: &Value) -> Vec<u8> {
    hex_decode(record["bytes"].as_str().expect("bytes"))
}

pub fn record_fid(record: &Value) -> u32 {
    u32::try_from(record["file_identifier"].as_u64().expect("file_identifier")).expect("u32")
}

// ---------------------------------------------------------------------------------------
// JSON comparison: exact, except that numbers where either side is a float compare by
// f64 bits (so -0.0 != 0.0), as the README requires.

pub fn json_diff(want: &Value, got: &Value) -> Option<String> {
    diff_at("$", want, got)
}

fn diff_at(path: &str, want: &Value, got: &Value) -> Option<String> {
    match (want, got) {
        (Value::Number(a), Value::Number(b)) => {
            let same = if a.is_f64() || b.is_f64() {
                a.as_f64().map(f64::to_bits) == b.as_f64().map(f64::to_bits)
            } else {
                a.as_i64() == b.as_i64() && a.as_u64() == b.as_u64()
            };
            (!same).then(|| format!("{path}: want {a}, got {b}"))
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return Some(format!(
                    "{path}: want {} elements, got {}",
                    a.len(),
                    b.len()
                ));
            }
            a.iter()
                .zip(b)
                .enumerate()
                .find_map(|(i, (x, y))| diff_at(&format!("{path}[{i}]"), x, y))
        }
        (Value::Object(a), Value::Object(b)) => {
            let keys_a: Vec<&String> = a.keys().collect();
            let keys_b: Vec<&String> = b.keys().collect();
            if keys_a != keys_b {
                return Some(format!("{path}: want keys {keys_a:?}, got {keys_b:?}"));
            }
            a.iter().find_map(|(k, x)| {
                let at = match x.get("field").and_then(Value::as_str) {
                    Some(f) => format!("{path}.{k}({f})"),
                    None => format!("{path}.{k}"),
                };
                diff_at(&at, x, &b[k])
            })
        }
        _ => (want != got).then(|| format!("{path}: want {want}, got {got}")),
    }
}

pub fn assert_json_eq(context: &str, want: &Value, got: &Value) {
    if let Some(d) = json_diff(want, got) {
        panic!("{context}: {d}");
    }
}

/// The leaves of a dump as `path → value`: the path is the field names (and vector indices)
/// from the root, e.g. `/0/transaction/mutations[1]/param1`; a union's tag is the leaf
/// `<path>#tag`. Two dumps that differ only in some fields differ only in the leaves under
/// those fields' paths.
pub fn leaves(root: &Value) -> BTreeMap<String, String> {
    fn walk(x: &Value, path: &str, out: &mut BTreeMap<String, String>) {
        match x {
            Value::Object(m) if m.contains_key("type") => {
                let path = match m.get("field").and_then(Value::as_str) {
                    Some(f) => format!("{path}/{f}"),
                    None => path.to_string(),
                };
                if let Some(tag) = m.get("tag") {
                    out.insert(format!("{path}#tag"), tag.to_string());
                }
                walk(&m["value"], &path, out);
            }
            Value::Array(items) => {
                for (i, item) in items.iter().enumerate() {
                    if item.get("field").is_some() {
                        // A table or struct member: named by its field.
                        walk(item, path, out);
                    } else {
                        walk(item, &format!("{path}[{i}]"), out);
                    }
                }
                if items.is_empty() {
                    out.insert(format!("{path}[]"), "empty".into());
                }
            }
            other => {
                out.insert(path.to_string(), other.to_string());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, "", &mut out);
    out
}

/// The leaf paths where two dumps differ.
pub fn differing_leaves(want: &Value, got: &Value) -> Vec<String> {
    let (a, b) = (leaves(want), leaves(got));
    a.keys()
        .chain(b.keys())
        .filter(|k| a.get(*k) != b.get(*k))
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

// ---------------------------------------------------------------------------------------
// The dumper.

pub trait Dump {
    fn type_name() -> String;
    /// `{"type", "tag"?, "value"}`.
    fn node(&self) -> Value;
}

pub fn node(ty: String, value: Value) -> Value {
    json!({ "type": ty, "value": value })
}

pub fn tagged(ty: String, tag: u8, value: Value) -> Value {
    json!({ "type": ty, "tag": tag, "value": value })
}

/// A table or struct member: the node with `field` and `slot`/`offset` added.
pub fn member(field: &str, key: &str, position: Value, node: Value) -> Value {
    let mut m = Map::new();
    m.insert("field".into(), Value::from(field));
    m.insert(key.into(), position);
    if let Value::Object(n) = node {
        m.extend(n);
    }
    Value::Object(m)
}

/// The members of a table: `(name, slots taken (0 = arena), node)`.
pub fn table_value(members: Vec<(&str, usize, Value)>) -> Value {
    let mut slot = 0;
    let mut out = Vec::new();
    for (name, slots, n) in members {
        let position = if slots == 0 {
            Value::Null
        } else {
            Value::from(slot)
        };
        out.push(member(name, "slot", position, n));
        slot += slots;
    }
    Value::Array(out)
}

pub fn arena_node() -> Value {
    json!({ "type": "arena", "value": null })
}

/// The synthetic root table holding one message.
pub fn root1<T: Dump + Wire>(v: &T) -> Value {
    node(
        "table<root>".into(),
        table_value(vec![("0", T::SHAPE.slots(), v.node())]),
    )
}

pub fn slots_of<T: Wire>(_: &T) -> usize {
    T::SHAPE.slots()
}

macro_rules! dump_int {
    ($($t:ty => $n:literal),*) => {$(
        impl Dump for $t {
            fn type_name() -> String { $n.into() }
            fn node(&self) -> Value { node(Self::type_name(), Value::from(*self)) }
        }
    )*};
}

dump_int!(u8 => "u8", i8 => "i8", u16 => "u16", i16 => "i16", u32 => "u32", i32 => "i32",
          u64 => "u64", i64 => "i64", bool => "bool");

impl Dump for f64 {
    fn type_name() -> String {
        "f64".into()
    }
    fn node(&self) -> Value {
        node(Self::type_name(), Value::from(*self))
    }
}

impl Dump for Uid {
    fn type_name() -> String {
        "uid".into()
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            json!({ "first": self.first, "second": self.second }),
        )
    }
}

impl Dump for &[u8] {
    fn type_name() -> String {
        "bytes".into()
    }
    fn node(&self) -> Value {
        node(Self::type_name(), Value::from(hex_encode(self)))
    }
}

impl<T: Dump> Dump for Vec<T> {
    fn type_name() -> String {
        format!("vector<{}>", T::type_name())
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            Value::Array(self.iter().map(Dump::node).collect()),
        )
    }
}

impl<'a, T: Dump + Decode<'a>> Dump for VectorView<'a, T> {
    fn type_name() -> String {
        format!("vector<{}>", T::type_name())
    }
    fn node(&self) -> Value {
        let elements = self
            .iter()
            .map(|e| e.expect("view element").node())
            .collect();
        node(Self::type_name(), Value::Array(elements))
    }
}

impl<T: Dump, const N: usize> Dump for [T; N] {
    fn type_name() -> String {
        format!("array<{},{N}>", T::type_name())
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            Value::Array(self.iter().map(Dump::node).collect()),
        )
    }
}

impl<T: Dump> Dump for Option<T> {
    fn type_name() -> String {
        format!("optional<{}>", T::type_name())
    }
    fn node(&self) -> Value {
        match self {
            None => tagged(Self::type_name(), 0, Value::Null),
            Some(v) => tagged(Self::type_name(), 1, v.node()),
        }
    }
}

impl<T: Dump> Dump for ErrorOr<T> {
    fn type_name() -> String {
        format!("error_or<{}>", T::type_name())
    }
    fn node(&self) -> Value {
        match self {
            Err(e) => tagged(Self::type_name(), 1, e.node()),
            Ok(v) => tagged(Self::type_name(), 2, v.node()),
        }
    }
}

impl<T: Dump> Dump for EnsureTable<T> {
    fn type_name() -> String {
        format!("ensure_table<{}>", T::type_name())
    }
    fn node(&self) -> Value {
        node(Self::type_name(), self.0.node())
    }
}

impl Dump for FdbError {
    fn type_name() -> String {
        "table<Error>".into()
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            table_value(vec![("error_code", 1, self.code.node())]),
        )
    }
}

/// `std::map` / `std::unordered_map`: a vector of `pair` tables.
impl<K: Dump + Wire, V: Dump + Wire> Dump for BTreeMap<K, V> {
    fn type_name() -> String {
        format!("vector<table<pair<{},{}>>>", K::type_name(), V::type_name())
    }
    fn node(&self) -> Value {
        let ty = format!("table<pair<{},{}>>", K::type_name(), V::type_name());
        let entries = self
            .iter()
            .map(|(k, v)| {
                node(
                    ty.clone(),
                    table_value(vec![
                        ("first", K::SHAPE.slots(), k.node()),
                        ("second", V::SHAPE.slots(), v.node()),
                    ]),
                )
            })
            .collect();
        node(Self::type_name(), Value::Array(entries))
    }
}

impl<A: Dump + Wire, B: Dump + Wire> Dump for v7_4::Pair<A, B> {
    fn type_name() -> String {
        format!("table<pair<{},{}>>", A::type_name(), B::type_name())
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            table_value(vec![
                ("first", A::SHAPE.slots(), self.first.node()),
                ("second", B::SHAPE.slots(), self.second.node()),
            ]),
        )
    }
}

impl<T> Dump for ReplyPromise<T> {
    fn type_name() -> String {
        "table<reply_promise>".into()
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            table_value(vec![("token", 1, self.token().node())]),
        )
    }
}

impl<T> Dump for RequestStream<T> {
    fn type_name() -> String {
        "table<request_stream>".into()
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            table_value(vec![("endpoint", 1, self.endpoint().node())]),
        )
    }
}

impl Dump for Endpoint {
    fn type_name() -> String {
        "table<Endpoint>".into()
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            table_value(vec![
                ("addresses", 1, self.addresses.node()),
                ("token", 1, self.token.node()),
            ]),
        )
    }
}

impl Dump for NetworkAddressList {
    fn type_name() -> String {
        "table<NetworkAddressList>".into()
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            table_value(vec![
                ("address", 1, self.address.node()),
                ("secondaryAddress", 2, self.secondary_address.node()),
            ]),
        )
    }
}

impl Dump for NetworkAddress {
    fn type_name() -> String {
        "table<NetworkAddress>".into()
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            table_value(vec![
                ("ip", 1, self.ip.node()),
                ("port", 1, self.port.node()),
                ("flags", 1, self.flags.node()),
                ("fromHostname", 1, self.from_hostname.node()),
            ]),
        )
    }
}

impl Dump for IpAddress {
    fn type_name() -> String {
        "table<IPAddress>".into()
    }
    fn node(&self) -> Value {
        let ty = "variant<u32,array<u8,16>>".to_string();
        let addr = match self {
            IpAddress::V4(v) => tagged(ty, 1, v.node()),
            IpAddress::V6(b) => tagged(ty, 2, b.node()),
        };
        node(Self::type_name(), table_value(vec![("addr", 2, addr)]))
    }
}

impl Dump for TagSetRef<'_> {
    fn type_name() -> String {
        "tag_set".into()
    }
    fn node(&self) -> Value {
        let tags = self.iter().map(|t| Value::from(hex_encode(t))).collect();
        node(Self::type_name(), Value::Array(tags))
    }
}

impl Dump for VersionVector {
    fn type_name() -> String {
        "version_vector".into()
    }
    fn node(&self) -> Value {
        let versions: Vec<Value> = self
            .iter()
            .map(|(tag, version)| json!({ "locality": tag.locality, "id": tag.id, "version": version }))
            .collect();
        node(
            Self::type_name(),
            json!({ "versions": versions, "maxVersion": self.max_version }),
        )
    }
}

impl Dump for IdempotencyIdRef<'_> {
    fn type_name() -> String {
        "bytes".into()
    }
    fn node(&self) -> Value {
        node(Self::type_name(), Value::from(hex_encode(self.as_bytes())))
    }
}

impl Dump for Tag {
    fn type_name() -> String {
        "struct<u16,i8>".into()
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            json!([
                member("0", "offset", Value::from(0), self.id.node()),
                member("1", "offset", Value::from(2), self.locality.node()),
            ]),
        )
    }
}

impl Dump for TraceFlags {
    fn type_name() -> String {
        "u8".into()
    }
    fn node(&self) -> Value {
        self.0.node()
    }
}

/// `KeyRangeRef` and `MutationRef` dump their wire form: the fixtures' dumper runs the writer
/// side of `serialize()`, which compresses single-key ranges.
impl Dump for v7_4::KeyRangeRef<'_> {
    fn type_name() -> String {
        "table<KeyRangeRef>".into()
    }
    fn node(&self) -> Value {
        let (begin, end) = self.wire_form().expect("a valid range");
        node(
            Self::type_name(),
            table_value(vec![("begin", 1, begin.node()), ("end", 1, end.node())]),
        )
    }
}

impl Dump for v7_4::MutationRef<'_> {
    fn type_name() -> String {
        "table<MutationRef>".into()
    }
    fn node(&self) -> Value {
        let (param1, param2) = self.wire_form().expect("a valid mutation");
        node(
            Self::type_name(),
            table_value(vec![
                ("type", 1, self.kind.node()),
                ("param1", 1, param1.node()),
                ("param2", 1, param2.node()),
            ]),
        )
    }
}

/// `Dump` for a derived table: the C++ type name, then its serialized fields in order with
/// their C++ names. `@arena` names the `#[fdb(arena)]` field.
macro_rules! dump_table {
    ($cpp:literal $name:ident $(<$lt:lifetime>)? { $($f:ident = $fname:literal),* $(,)? } $(@ $arena:ident)?) => {
        impl$(<$lt>)? Dump for $name$(<$lt>)? {
            fn type_name() -> String {
                format!("table<{}>", $cpp)
            }
            fn node(&self) -> Value {
                #[allow(unused_mut)]
                let mut members = vec![$(($fname, slots_of(&self.$f), self.$f.node())),*];
                $(
                    let () = self.$arena;
                    members.push((stringify!($arena), 0, arena_node()));
                )?
                node(Self::type_name(), table_value(members))
            }
        }
    };
}

// Shared by both versions (v7_4 re-exports them, as does v8_0).
use v7_4::{
    ClientTagThrottleLimits, ClientTrCommitCostEstimation, ClientVersionRef, CommitID,
    CommitProxyInterface, CommitTransactionRef, GetValueReply, GrvProxyInterface, Hostname,
    LocalityData, OpenDatabaseCoordRequest, StorageServerInterface, VersionHistory,
};

dump_table!("SpanContext" SpanContext { trace_id = "traceID", span_id = "spanID", flags = "m_Flags" });
dump_table!("LocalityData" LocalityData<'a> { data = "_data" });
dump_table!("GrvProxyInterface" GrvProxyInterface<'a> {
    process_id = "processId", provisional = "provisional",
    get_consistent_read_version = "getConsistentReadVersion",
});
dump_table!("CommitProxyInterface" CommitProxyInterface<'a> {
    process_id = "processId", provisional = "provisional", commit = "commit",
});
dump_table!("StorageServerInterface" StorageServerInterface<'a> {
    unique_id = "uniqueID", locality = "locality", get_value = "getValue",
    tss_pair_id = "tssPairID", accepting_requests = "acceptingRequests",
});
dump_table!("VersionHistory" VersionHistory<'a> { mutations = "mutations", version = "version" });
dump_table!("ClientVersionRef" ClientVersionRef<'a> {
    client_version = "clientVersion", source_version = "sourceVersion",
    protocol_version = "protocolVersion",
});
dump_table!("Hostname" Hostname<'a> { host = "host", service = "service", is_tls = "isTLS" });
dump_table!("OpenDatabaseCoordRequest" OpenDatabaseCoordRequest<'a> {
    issues = "issues", supported_versions = "supportedVersions",
    trace_log_group = "traceLogGroup", known_client_info_id = "knownClientInfoID",
    cluster_key = "clusterKey", coordinators = "coordinators", reply = "reply",
    hostnames = "hostnames", internal = "internal",
});
dump_table!("ClientTagThrottleLimits" ClientTagThrottleLimits { tps_rate = "tpsRate", duration = "duration" });
dump_table!("GetValueReply" GetValueReply<'a> {
    penalty = "penalty", error = "error", value = "value", cached = "cached",
});
dump_table!("ClientTrCommitCostEstimation" ClientTrCommitCostEstimation {
    ops_count = "opsCount", write_costs = "writeCosts", clear_idx_costs = "clearIdxCosts",
    expensive_cost_est_count = "expensiveCostEstCount",
});
dump_table!("CommitTransactionRef" CommitTransactionRef<'a> {
    read_conflict_ranges = "read_conflict_ranges", write_conflict_ranges = "write_conflict_ranges",
    mutations = "mutations", read_snapshot = "read_snapshot",
    report_conflicting_keys = "report_conflicting_keys", lock_aware = "lock_aware",
    span_context = "spanContext", tenant_ids = "tenantIds",
});
dump_table!("CommitID" CommitID<'a> {
    version = "version", txn_batch_id = "txnBatchId", metadata_version = "metadataVersion",
    conflicting_kr_indices = "conflictingKRIndices",
});

impl<R: Dump + Wire> Dump for v7_4::GetKeyServerLocationsReply<'_, R> {
    fn type_name() -> String {
        "table<GetKeyServerLocationsReply>".into()
    }
    fn node(&self) -> Value {
        let () = self.arena;
        node(
            Self::type_name(),
            table_value(vec![
                ("results", 1, self.results.node()),
                ("resultsTssMapping", 1, self.results_tss_mapping.node()),
                ("resultsTagMapping", 1, self.results_tag_mapping.node()),
                ("arena", 0, arena_node()),
            ]),
        )
    }
}

/// The 7.4-only types.
mod release_7_4 {
    use super::*;
    use fdb_native_schema::v7_4::{
        ClientDBInfo, CommitTransactionRequest, EncryptKeyProxyInterface,
        GetKeyServerLocationsRequest, GetReadVersionReply, GetReadVersionRequest, GetValueRequest,
        ReadOptions, TenantInfo, TenantMode, WipedString,
    };

    dump_table!("WipedString" WipedString<'a> { ws = "ws" } @arena);
    dump_table!("TenantInfo" TenantInfo<'a> { tenant_id = "tenantId", token = "token" } @arena);
    dump_table!("TenantMode" TenantMode { mode = "mode" });
    dump_table!("EncryptKeyProxyInterface" EncryptKeyProxyInterface<'a> {
        locality = "locality", my_id = "myId", wait_failure = "waitFailure",
    });
    dump_table!("ClientDBInfo" ClientDBInfo<'a> {
        grv_proxies = "grvProxies", commit_proxies = "commitProxies", id = "id",
        forward = "forward", history = "history", tenant_mode = "tenantMode",
        encrypt_key_proxy = "encryptKeyProxy", cluster_id = "clusterId",
        cluster_type = "clusterType", metacluster_name = "metaclusterName",
    });
    dump_table!("GetReadVersionRequest" GetReadVersionRequest<'a> {
        transaction_count = "transactionCount", flags = "flags", tags = "tags",
        debug_id = "debugID", reply = "reply", span_context = "spanContext",
        max_version = "maxVersion",
    });
    dump_table!("GetReadVersionReply" GetReadVersionReply<'a> {
        process_busy_time = "processBusyTime", version = "version", locked = "locked",
        metadata_version = "metadataVersion", tag_throttle_info = "tagThrottleInfo",
        mid_shard_size = "midShardSize", rk_default_throttled = "rkDefaultThrottled",
        rk_batch_throttled = "rkBatchThrottled", ss_version_vector_delta = "ssVersionVectorDelta",
        proxy_id = "proxyId", proxy_tag_throttled_duration = "proxyTagThrottledDuration",
    });
    dump_table!("GetKeyServerLocationsRequest" GetKeyServerLocationsRequest<'a> {
        begin = "begin", end = "end", limit = "limit", reverse = "reverse", reply = "reply",
        span_context = "spanContext", tenant = "tenant", min_tenant_version = "minTenantVersion",
    } @arena);
    dump_table!("ReadOptions" ReadOptions {
        read_type = "type", cache_result = "cacheResult", debug_id = "debugID",
        consistency_check_start_version = "consistencyCheckStartVersion",
        lock_aware = "lockAware",
    });
    dump_table!("GetValueRequest" GetValueRequest<'a> {
        key = "key", version = "version", tags = "tags", reply = "reply",
        span_context = "spanContext", tenant_info = "tenantInfo", options = "options",
        ss_latest_commit_versions = "ssLatestCommitVersions",
    });
    dump_table!("CommitTransactionRequest" CommitTransactionRequest<'a> {
        transaction = "transaction", reply = "reply", flags = "flags", debug_id = "debugID",
        commit_cost_estimation = "commitCostEstimation", tag_set = "tagSet",
        span_context = "spanContext", tenant_info = "tenantInfo",
        idempotency_id = "idempotencyId",
    } @arena);
}

/// The 8.0-only types.
mod release_8_0 {
    use super::*;
    use fdb_native_schema::v8_0::{
        CdcProxyInterface, ClientDBInfo, CommitTransactionRequest, GetKeyServerLocationsRequest,
        GetReadVersionReply, GetReadVersionRequest, GetValueRequest, ReadOptions,
    };

    dump_table!("CDCProxyInterface" CdcProxyInterface<'a> {
        process_id = "processId", consume = "consume",
        supports_ordered_streams = "supportsOrderedStreams",
    });
    dump_table!("ClientDBInfo" ClientDBInfo<'a> {
        grv_proxies = "grvProxies", commit_proxies = "commitProxies", id = "id",
        forward = "forward", history = "history", cluster_id = "clusterId",
        cluster_type = "clusterType", native_cdc_enabled = "nativeCdcEnabled",
        native_cdc_tag_count = "nativeCdcTagCount", cdc_proxies = "cdcProxies",
        stream_to_cdc_proxy_id = "streamToCDCProxyId",
    });
    dump_table!("GetReadVersionRequest" GetReadVersionRequest<'a> {
        transaction_count = "transactionCount", flags = "flags", tags = "tags",
        debug_id = "debugID", reply = "reply", span_context = "spanContext",
        max_version = "maxVersion", max_grv_queue_delay_ms = "maxGrvQueueDelayMS",
    });
    dump_table!("GetReadVersionReply" GetReadVersionReply<'a> {
        process_busy_time = "processBusyTime", version = "version", locked = "locked",
        metadata_version = "metadataVersion", tag_throttle_info = "tagThrottleInfo",
        mid_shard_size = "midShardSize", rk_default_throttled = "rkDefaultThrottled",
        rk_batch_throttled = "rkBatchThrottled", ss_version_vector_delta = "ssVersionVectorDelta",
        proxy_id = "proxyId",
    });
    dump_table!("GetKeyServerLocationsRequest" GetKeyServerLocationsRequest<'a> {
        begin = "begin", end = "end", limit = "limit", reverse = "reverse", reply = "reply",
        span_context = "spanContext", legacy_version = "legacyVersion",
    } @arena);
    dump_table!("ReadOptions" ReadOptions {
        read_type = "type", cache_result = "cacheResult", debug_id = "debugID",
        consistency_check_start_version = "consistencyCheckStartVersion",
        lock_aware = "lockAware",
    });
    dump_table!("GetValueRequest" GetValueRequest<'a> {
        key = "key", version = "version", tags = "tags", reply = "reply",
        span_context = "spanContext", options = "options",
        ss_latest_commit_versions = "ssLatestCommitVersions",
    });
    dump_table!("CommitTransactionRequest" CommitTransactionRequest<'a> {
        transaction = "transaction", reply = "reply", flags = "flags", debug_id = "debugID",
        commit_cost_estimation = "commitCostEstimation", tag_set = "tagSet",
        span_context = "spanContext", idempotency_id = "idempotencyId",
    } @arena);
}
