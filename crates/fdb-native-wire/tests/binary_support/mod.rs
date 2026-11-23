//! Support for the binary-format tests: the fixtures of `tools/golden-vectors/fixtures/binary-*`
//! (written by FDB's own `BinaryWriter`), the Rust types they hold, and a dispatcher from the
//! fixtures' `type` names to those types.

#![allow(dead_code, unreachable_pub)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::path::{Path, PathBuf};

use fdb_native_wire::binary::{
    BinaryDecodeError, BinaryDeserialize, BinaryDeserializeOwned, BinaryEncodeError, BinaryReader,
    BinarySerialize, BinaryWriter, ConnectPacket, HealthMetrics, IdempotencyEntry,
    IdempotencyValue, SpanContext, StorageStats, TraceFlags, Versioning, decode_idempotency_key,
    from_slice, idempotency_key, idempotency_scan_range, self_conflict_key, to_vec,
};
use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, FdbError, FileIdentified, FileIdentifier, ObjectRef,
    ProtocolVersion, Reader, Shape, Uid, Wire, Writer,
};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------------------
// Fixtures.

/// The releases that have fixtures.
pub const RELEASES: [&str; 2] = ["release-7.4", "release-8.0"];

pub fn fixtures_dir(release: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tools/golden-vectors/fixtures")
        .join(format!("binary-{release}"))
}

pub fn read_json(path: &Path) -> Value {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parsing {}: {e}", path.display()))
}

/// Every record listed under `key` ("positive" or "negative") in a release's index.json.
pub fn records(release: &str, key: &str) -> Vec<Value> {
    let dir = fixtures_dir(release);
    read_json(&dir.join("index.json"))[key]
        .as_array()
        .expect("index list")
        .iter()
        .map(|f| read_json(&dir.join(f.as_str().expect("file name"))))
        .collect()
}

pub fn from_hex(s: &str) -> Vec<u8> {
    assert!(s.len() % 2 == 0, "odd hex length");
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).expect("hex"))
        .collect()
}

pub fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn versioning(record: &Value) -> Versioning {
    let v = &record["versioning"];
    let pv = || ProtocolVersion(v["protocol_version"].as_u64().expect("protocol_version"));
    match v["mode"].as_str().expect("mode") {
        "unversioned" => Versioning::Unversioned,
        "include" => Versioning::Include(pv()),
        "assume" => Versioning::Assume(pv()),
        other => panic!("mode {other}"),
    }
}

pub fn record_bytes(record: &Value) -> Vec<u8> {
    from_hex(record["bytes"].as_str().expect("bytes"))
}

// ---------------------------------------------------------------------------------------
// Logical values: the JSON each fixture records, for the types the fixtures use.

pub trait Logical: BinarySerialize + BinaryDeserializeOwned + Debug + PartialEq + Sized {
    fn to_json(&self) -> Value;
    fn from_json(v: &Value) -> Self;
}

macro_rules! logical_int {
    ($($ty:ty => $get:ident),+) => {$(
        impl Logical for $ty {
            fn to_json(&self) -> Value {
                json!(*self)
            }
            fn from_json(v: &Value) -> Self {
                v.$get().expect(stringify!($ty)).try_into().expect(stringify!($ty))
            }
        }
    )+};
}
logical_int!(u8 => as_u64, u16 => as_u64, u32 => as_u64, u64 => as_u64, i8 => as_i64, i16 => as_i64, i32 => as_i64, i64 => as_i64);

impl Logical for f64 {
    fn to_json(&self) -> Value {
        json!(*self)
    }
    fn from_json(v: &Value) -> Self {
        v.as_f64().expect("f64")
    }
}

impl Logical for bool {
    fn to_json(&self) -> Value {
        json!(*self)
    }
    fn from_json(v: &Value) -> Self {
        v.as_bool().expect("bool")
    }
}

impl Logical for String {
    fn to_json(&self) -> Value {
        json!(self)
    }
    fn from_json(v: &Value) -> Self {
        v.as_str().expect("string").to_owned()
    }
}

impl Logical for Uid {
    fn to_json(&self) -> Value {
        json!({"first": self.first, "second": self.second})
    }
    fn from_json(v: &Value) -> Self {
        Uid::new(
            v["first"].as_u64().expect("first"),
            v["second"].as_u64().expect("second"),
        )
    }
}

impl Logical for ProtocolVersion {
    fn to_json(&self) -> Value {
        json!(self.0)
    }
    fn from_json(v: &Value) -> Self {
        ProtocolVersion(v.as_u64().expect("version"))
    }
}

impl Logical for FdbError {
    fn to_json(&self) -> Value {
        json!(self.code)
    }
    fn from_json(v: &Value) -> Self {
        FdbError::new(v.as_u64().expect("code").try_into().expect("code"))
    }
}

/// A `StringRef`: bytes, as hex in the fixtures.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bytes(pub Vec<u8>);

impl BinarySerialize for Bytes {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        self.0.serialize(w)
    }
}

impl<'de> BinaryDeserialize<'de> for Bytes {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        Vec::<u8>::deserialize(r).map(Bytes)
    }
}

impl Logical for Bytes {
    fn to_json(&self) -> Value {
        json!(to_hex(&self.0))
    }
    fn from_json(v: &Value) -> Self {
        Bytes(from_hex(v.as_str().expect("hex")))
    }
}

impl<T: Logical> Logical for Option<T> {
    fn to_json(&self) -> Value {
        self.as_ref().map_or(Value::Null, Logical::to_json)
    }
    fn from_json(v: &Value) -> Self {
        (!v.is_null()).then(|| T::from_json(v))
    }
}

impl<T: Logical> Logical for Vec<T> {
    fn to_json(&self) -> Value {
        Value::Array(self.iter().map(Logical::to_json).collect())
    }
    fn from_json(v: &Value) -> Self {
        v.as_array()
            .expect("array")
            .iter()
            .map(T::from_json)
            .collect()
    }
}

impl<T: Logical, const N: usize> Logical for [T; N] {
    fn to_json(&self) -> Value {
        Value::Array(self.iter().map(Logical::to_json).collect())
    }
    fn from_json(v: &Value) -> Self {
        let items: Vec<T> = v
            .as_array()
            .expect("array")
            .iter()
            .map(T::from_json)
            .collect();
        items.try_into().expect("array length")
    }
}

impl<T: Logical + Ord> Logical for BTreeSet<T> {
    fn to_json(&self) -> Value {
        Value::Array(self.iter().map(Logical::to_json).collect())
    }
    fn from_json(v: &Value) -> Self {
        v.as_array()
            .expect("array")
            .iter()
            .map(T::from_json)
            .collect()
    }
}

impl<K: Logical + Ord, V: Logical> Logical for BTreeMap<K, V> {
    fn to_json(&self) -> Value {
        Value::Array(
            self.iter()
                .map(|(k, v)| json!([k.to_json(), v.to_json()]))
                .collect(),
        )
    }
    fn from_json(v: &Value) -> Self {
        v.as_array()
            .expect("array")
            .iter()
            .map(|p| (K::from_json(&p[0]), V::from_json(&p[1])))
            .collect()
    }
}

impl<A: Logical, B: Logical> Logical for (A, B) {
    fn to_json(&self) -> Value {
        json!([self.0.to_json(), self.1.to_json()])
    }
    fn from_json(v: &Value) -> Self {
        (A::from_json(&v[0]), B::from_json(&v[1]))
    }
}

// enum class Color : uint8_t { red = 1, green = 2, blue = 200 };
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Color {
    Red,
    Green,
    Blue,
}

fdb_native_wire::binary_enum!(Color: u8 {
    Color::Red = 1,
    Color::Green = 2,
    Color::Blue = 200,
});

impl Logical for Color {
    fn to_json(&self) -> Value {
        json!(match self {
            Color::Red => 1,
            Color::Green => 2,
            Color::Blue => 200,
        })
    }
    fn from_json(v: &Value) -> Self {
        match v.as_u64().expect("color") {
            1 => Color::Red,
            2 => Color::Green,
            200 => Color::Blue,
            other => panic!("color {other}"),
        }
    }
}

// enum Level : int32_t { low = -5, mid = 0, high = 70000 };
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    Low,
    Mid,
    High,
}

fdb_native_wire::binary_enum!(Level: i32 {
    Level::Low = -5,
    Level::Mid = 0,
    Level::High = 70000,
});

impl Logical for Level {
    fn to_json(&self) -> Value {
        json!(match self {
            Level::Low => -5,
            Level::Mid => 0,
            Level::High => 70000,
        })
    }
    fn from_json(v: &Value) -> Self {
        match v.as_i64().expect("level") {
            -5 => Level::Low,
            0 => Level::Mid,
            70000 => Level::High,
            other => panic!("level {other}"),
        }
    }
}

// struct Point3 { int32_t x; int64_t y; std::string label; serializer(ar, x, y, label) }
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Point3 {
    pub x: i32,
    pub y: i64,
    pub label: String,
}

impl BinarySerialize for Point3 {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write(&self.x)?;
        w.write(&self.y)?;
        w.write(&self.label)
    }
}

impl<'de> BinaryDeserialize<'de> for Point3 {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        Ok(Point3 {
            x: r.read()?,
            y: r.read()?,
            label: r.read()?,
        })
    }
}

impl Logical for Point3 {
    fn to_json(&self) -> Value {
        json!([self.x, self.y, self.label])
    }
    fn from_json(v: &Value) -> Self {
        Point3 {
            x: i32::from_json(&v[0]),
            y: i64::from_json(&v[1]),
            label: String::from_json(&v[2]),
        }
    }
}

// std::variant<int32_t, std::string, uint8_t>
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Variant3 {
    Int(i32),
    Str(String),
    Byte(u8),
}

impl BinarySerialize for Variant3 {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        match self {
            Variant3::Int(v) => {
                w.write_variant_index(0)?;
                w.write(v)
            }
            Variant3::Str(v) => {
                w.write_variant_index(1)?;
                w.write(v)
            }
            Variant3::Byte(v) => {
                w.write_variant_index(2)?;
                w.write(v)
            }
        }
    }
}

impl<'de> BinaryDeserialize<'de> for Variant3 {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        match r.read_variant_index(3)? {
            0 => r.read().map(Variant3::Int),
            1 => r.read().map(Variant3::Str),
            _ => r.read().map(Variant3::Byte),
        }
    }
}

impl Logical for Variant3 {
    fn to_json(&self) -> Value {
        match self {
            Variant3::Int(v) => json!({"index": 0, "value": v}),
            Variant3::Str(v) => json!({"index": 1, "value": v}),
            Variant3::Byte(v) => json!({"index": 2, "value": v}),
        }
    }
    fn from_json(v: &Value) -> Self {
        match v["index"].as_u64().expect("index") {
            0 => Variant3::Int(i32::from_json(&v["value"])),
            1 => Variant3::Str(String::from_json(&v["value"])),
            _ => Variant3::Byte(u8::from_json(&v["value"])),
        }
    }
}

// ---------------------------------------------------------------------------------------
// The generic check: fixture <-> Rust value.

/// Reads the fixture's bytes as `T` and requires the fixture's logical value; writes the
/// logical value and requires the fixture's bytes.
pub fn check_positive<T: Logical>(record: &Value) {
    let name = record["name"].as_str().unwrap_or("?");
    let versioning = versioning(record);
    let bytes = record_bytes(record);
    let expected = T::from_json(&record["value"]);
    let decoded: T =
        from_slice(&bytes, versioning).unwrap_or_else(|e| panic!("{name}: decode: {e}"));
    assert_eq!(decoded, expected, "{name}: decoded value");
    assert_eq!(
        decoded.to_json(),
        record["value"],
        "{name}: json of the decoded value"
    );
    let encoded = to_vec(&expected, versioning).unwrap_or_else(|e| panic!("{name}: encode: {e}"));
    assert_eq!(to_hex(&encoded), to_hex(&bytes), "{name}: encoded bytes");
}

/// Reads `bytes` as the fixture type `ty`; the value is dropped. `None` for a type that
/// needs more than `from_slice` (see [`check_record`]).
pub fn decode_any(
    ty: &str,
    bytes: &[u8],
    versioning: Versioning,
) -> Option<Result<(), BinaryDecodeError>> {
    macro_rules! d {
        ($t:ty) => {
            Some(from_slice::<$t>(bytes, versioning).map(drop))
        };
    }
    match ty {
        "u8" => d!(u8),
        "i8" => d!(i8),
        "u16" => d!(u16),
        "i16" => d!(i16),
        "u32" => d!(u32),
        "i32" => d!(i32),
        "u64" => d!(u64),
        "i64" => d!(i64),
        "f64" => d!(f64),
        "bool" => d!(bool),
        "enum_color" => d!(Color),
        "enum_level" => d!(Level),
        "uid" => d!(Uid),
        "protocol_version" => d!(ProtocolVersion),
        "error" => d!(FdbError),
        "string" => d!(String),
        "bytes" => d!(Bytes),
        "vec<i32>" => d!(Vec<i32>),
        "vec<u8>" => d!(Vec<u8>),
        "vec<string>" => d!(Vec<String>),
        "vec<vec<u16>>" => d!(Vec<Vec<u16>>),
        "vec<bytes>" => d!(Vec<Bytes>),
        "vec<key_value>" => d!(Vec<(Bytes, Bytes)>),
        "array<u16;3>" => d!([u16; 3]),
        "array<u8;4>" => d!([u8; 4]),
        "set<i32>" => d!(BTreeSet<i32>),
        "map<string,i64>" => d!(BTreeMap<String, i64>),
        "map<uid,i64>" => d!(BTreeMap<Uid, i64>),
        "optional<i32>" => d!(Option<i32>),
        "optional<string>" => d!(Option<String>),
        "pair<u8,string>" => d!((u8, String)),
        "point3" => d!(Point3),
        "variant3" => d!(Variant3),
        "key_range" => d!((Bytes, Bytes)),
        "span_context" => d!(SpanContext),
        "health_metrics" => d!(HealthMetrics),
        "connect_packet" => Some(ConnectPacket::decode(bytes).map(drop)),
        "idempotency_value" => Some(IdempotencyValue::decode(bytes).map(drop)),
        _ => None,
    }
}

/// Checks one positive fixture against the Rust type its `type` names.
pub fn check_record(record: &Value) {
    let ty = record["type"].as_str().expect("type");
    match ty {
        "u8" => check_positive::<u8>(record),
        "i8" => check_positive::<i8>(record),
        "u16" => check_positive::<u16>(record),
        "i16" => check_positive::<i16>(record),
        "u32" => check_positive::<u32>(record),
        "i32" => check_positive::<i32>(record),
        "u64" => check_positive::<u64>(record),
        "i64" => check_positive::<i64>(record),
        "f64" => check_positive::<f64>(record),
        "bool" => check_positive::<bool>(record),
        "enum_color" => check_positive::<Color>(record),
        "enum_level" => check_positive::<Level>(record),
        "uid" => check_positive::<Uid>(record),
        "protocol_version" => check_positive::<ProtocolVersion>(record),
        "error" => check_positive::<FdbError>(record),
        "string" => check_positive::<String>(record),
        "bytes" => check_positive::<Bytes>(record),
        "vec<i32>" => check_positive::<Vec<i32>>(record),
        "vec<u8>" => check_positive::<Vec<u8>>(record),
        "vec<string>" => check_positive::<Vec<String>>(record),
        "vec<vec<u16>>" => check_positive::<Vec<Vec<u16>>>(record),
        "vec<bytes>" => check_positive::<Vec<Bytes>>(record),
        "vec<key_value>" => check_positive::<Vec<(Bytes, Bytes)>>(record),
        "array<u16;3>" => check_positive::<[u16; 3]>(record),
        "array<u8;4>" => check_positive::<[u8; 4]>(record),
        "set<i32>" => check_positive::<BTreeSet<i32>>(record),
        "map<string,i64>" => check_positive::<BTreeMap<String, i64>>(record),
        "map<uid,i64>" => check_positive::<BTreeMap<Uid, i64>>(record),
        "optional<i32>" => check_positive::<Option<i32>>(record),
        "optional<string>" => check_positive::<Option<String>>(record),
        "pair<u8,string>" => check_positive::<(u8, String)>(record),
        "point3" => check_positive::<Point3>(record),
        "variant3" => check_positive::<Variant3>(record),
        "key_range" => check_positive::<(Bytes, Bytes)>(record),
        "connect_packet" => check_connect_packet(record),
        "span_context" => check_span_context(record),
        "health_metrics" => check_health_metrics(record),
        "idempotency_value" => check_idempotency_value(record),
        "idempotency_scan_range" => check_idempotency_scan_range(record),
        "self_conflict_key" => check_self_conflict_key(record),
        "key_servers_value" => check_key_servers_value(record),
        "sw_version_value" => check_sw_version_value(record),
        other => panic!("fixture type {other:?} has no Rust check"),
    }
}

// ---------------------------------------------------------------------------------------
// Usage-site fixtures.

fn check_connect_packet(record: &Value) {
    let bytes = record_bytes(record);
    let v = &record["value"];
    let expected = ConnectPacket {
        protocol_version: ProtocolVersion(v["protocol_version"].as_u64().unwrap()),
        canonical_remote_port: u16::from_json(&v["canonical_remote_port"]),
        connection_id: v["connection_id"].as_u64().unwrap(),
        canonical_remote_ip4: u32::from_json(&v["canonical_remote_ip4"]),
        flags: u16::from_json(&v["flags"]),
        canonical_remote_ip6: from_hex(v["canonical_remote_ip6"].as_str().unwrap())
            .try_into()
            .unwrap(),
    };
    assert_eq!(v["connect_packet_length"], ConnectPacket::LENGTH);
    assert_eq!(v["total_packet_size"], ConnectPacket::SIZE);
    // A longer buffer is fine: the packet says how long it is.
    let mut longer = bytes.clone();
    longer.extend_from_slice(b"next packet");
    let (decoded, used) = ConnectPacket::decode(&longer).unwrap();
    assert_eq!((decoded, used), (expected, bytes.len()));
    assert_eq!(ConnectPacket::decode(&bytes).unwrap().0, expected);
    assert_eq!(to_hex(&expected.encode().unwrap()), to_hex(&bytes));
    assert!(expected.protocol_version.has_object_serializer_flag());
    match expected.canonical_remote_address() {
        None => {
            assert!(v["canonical_remote_ip"].is_null());
            assert_eq!(expected.canonical_remote_port, 0);
        }
        Some((ip, port)) => {
            assert_eq!(v["canonical_remote_ip"].as_str().unwrap(), ip.to_string());
            assert_eq!(port, expected.canonical_remote_port);
        }
    }
}

fn check_span_context(record: &Value) {
    let bytes = record_bytes(record);
    let v = &record["value"];
    let expected = SpanContext {
        trace_id: Uid::from_json(&v["trace_id"]),
        span_id: v["span_id"].as_u64().unwrap(),
        flags: match v["flags"].as_u64().unwrap() {
            0 => TraceFlags::Unsampled,
            1 => TraceFlags::Sampled,
            other => panic!("flags {other}"),
        },
    };
    assert_eq!(SpanContext::from_option_value(&bytes).unwrap(), expected);
    let Versioning::Include(version) = versioning(record) else {
        panic!("SPAN_PARENT values are IncludeVersion");
    };
    assert_eq!(
        to_hex(&expected.to_option_value(version).unwrap()),
        to_hex(&bytes)
    );
}

fn health_metrics_from_json(v: &Value) -> HealthMetrics {
    HealthMetrics {
        worst_storage_queue: v["worst_storage_queue"].as_i64().unwrap(),
        limiting_storage_queue: v["limiting_storage_queue"].as_i64().unwrap(),
        worst_storage_durability_lag: v["worst_storage_durability_lag"].as_i64().unwrap(),
        limiting_storage_durability_lag: v["limiting_storage_durability_lag"].as_i64().unwrap(),
        worst_t_log_queue: v["worst_t_log_queue"].as_i64().unwrap(),
        tps_limit: v["tps_limit"].as_f64().unwrap(),
        batch_limited: v["batch_limited"].as_bool().unwrap(),
        storage_stats: v["storage_stats"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| {
                let s = &p[1];
                (
                    Uid::from_json(&p[0]),
                    StorageStats {
                        storage_queue: s["storage_queue"].as_i64().unwrap(),
                        storage_durability_lag: s["storage_durability_lag"].as_i64().unwrap(),
                        disk_usage: s["disk_usage"].as_f64().unwrap(),
                        cpu_usage: s["cpu_usage"].as_f64().unwrap(),
                    },
                )
            })
            .collect(),
        t_log_queue: BTreeMap::<Uid, i64>::from_json(&v["t_log_queue"]),
    }
}

fn check_health_metrics(record: &Value) {
    let bytes = record_bytes(record);
    let expected = health_metrics_from_json(&record["value"]);
    assert_eq!(HealthMetrics::from_blob(&bytes).unwrap(), expected);
    let Versioning::Include(version) = versioning(record) else {
        panic!("the blob is IncludeVersion");
    };
    assert_eq!(to_hex(&expected.to_blob(version).unwrap()), to_hex(&bytes));
}

fn check_idempotency_value(record: &Value) {
    let bytes = record_bytes(record);
    let v = &record["value"];
    let value = IdempotencyValue::decode(&bytes).unwrap();
    assert_eq!(
        value.protocol_version.0,
        v["protocol_version"].as_u64().unwrap()
    );
    assert_eq!(value.timestamp, v["timestamp"].as_i64().unwrap());
    let ids: Vec<(Vec<u8>, u8)> = v["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                from_hex(e["id"].as_str().unwrap()),
                u8::from_json(&e["low_order_batch_index"]),
            )
        })
        .collect();
    assert_eq!(
        value.entries,
        ids.iter()
            .map(|(id, low)| IdempotencyEntry {
                id,
                low_order_batch_index: *low
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(to_hex(&value.encode().unwrap()), to_hex(&bytes));

    // The key: C++'s makeIdempotencySingleKeyRange(...).begin and decodeIdempotencyKey.
    let key = from_hex(v["key"].as_str().unwrap());
    let commit_version = v["commit_version"].as_i64().unwrap();
    let high = u8::from_json(&v["high_order_batch_index"]);
    assert_eq!(
        decode_idempotency_key(&key).unwrap(),
        (commit_version, high)
    );
    assert_eq!(idempotency_key(commit_version, high), key);
    // kvContainsIdempotencyId: the id is found with the batch index C++ computed.
    for f in v["found"].as_array().unwrap() {
        let low = value
            .find(&from_hex(f["id"].as_str().unwrap()))
            .expect("id is in the value");
        assert_eq!(
            u16::from(high) << 8 | u16::from(low),
            u16::from_json(&f["batch_index"])
        );
        assert_eq!(f["commit_version"].as_i64().unwrap(), commit_version);
    }
    assert_eq!(value.find(&[0xee; 16]), None);
}

fn check_idempotency_scan_range(record: &Value) {
    let v = &record["value"];
    let (begin, end) = idempotency_scan_range(
        v["min_commit_version"].as_i64().unwrap(),
        v["max_commit_version"].as_i64().unwrap(),
    )
    .unwrap();
    assert_eq!(to_hex(&begin), v["begin"].as_str().unwrap());
    assert_eq!(to_hex(&end), v["end"].as_str().unwrap());
    assert_eq!(begin, record_bytes(record));
}

fn check_self_conflict_key(record: &Value) {
    let id = Uid::from_json(&record["value"]["unique_id"]);
    assert_eq!(self_conflict_key(id).unwrap(), record_bytes(record));
}

// A `\xff/keyServers/` value: IncludeVersion, then `vector<UID> src, vector<UID> dest,
// UID srcId` and, when `dest` is not empty, `UID destId`.
fn check_key_servers_value(record: &Value) {
    let bytes = record_bytes(record);
    let v = &record["value"];
    let mut r = BinaryReader::include_version(&bytes).unwrap();
    assert_eq!(
        r.protocol_version().unwrap().0,
        v["protocol_version"].as_u64().unwrap()
    );
    let src: Vec<Uid> = r.read().unwrap();
    let dest: Vec<Uid> = r.read().unwrap();
    let src_id: Uid = r.read().unwrap();
    let dest_id: Uid = if r.is_empty() {
        Uid::default()
    } else {
        r.read().unwrap()
    };
    r.finish().unwrap();
    assert_eq!(src, Vec::<Uid>::from_json(&v["src"]));
    assert_eq!(dest, Vec::<Uid>::from_json(&v["dest"]));
    assert_eq!(src_id, Uid::from_json(&v["src_id"]));
    assert_eq!(dest_id, Uid::from_json(&v["dest_id"]));

    let Versioning::Include(version) = versioning(record) else {
        panic!("IncludeVersion");
    };
    let mut w = BinaryWriter::include_version(version).unwrap();
    w.write(&src).unwrap();
    w.write(&dest).unwrap();
    w.write(&src_id).unwrap();
    if !dest.is_empty() {
        w.write(&dest_id).unwrap();
    }
    assert_eq!(to_hex(w.as_bytes()), to_hex(&bytes));
}

/// `SWVersion` (`ProtocolVersion.h`): three `uint64_t`s in a flatbuffers table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SwVersion {
    newest: u64,
    last_run: u64,
    lowest_compatible: u64,
}

impl FileIdentified for SwVersion {
    const FILE_IDENTIFIER: FileIdentifier = FileIdentifier::new(13943914);
}

impl Wire for SwVersion {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for SwVersion {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.newest)?;
        t.field(&self.last_run)?;
        t.field(&self.lowest_compatible)?;
        t.finish()
    }
}

impl<'de> Decode<'de> for SwVersion {
    fn absent() -> Self {
        SwVersion {
            newest: 0,
            last_run: 0,
            lowest_compatible: 0,
        }
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(SwVersion {
            newest: t.field()?,
            last_run: t.field()?,
            lowest_compatible: t.field()?,
        })
    }
}

// A value whose IncludeVersion prefix has the object-serializer flag: the binary reader
// stops after the prefix and the rest is a flatbuffers message.
fn check_sw_version_value(record: &Value) {
    let bytes = record_bytes(record);
    let v = &record["value"];
    let expected = SwVersion {
        newest: v["newest_protocol_version"].as_u64().unwrap(),
        last_run: v["last_run_protocol_version"].as_u64().unwrap(),
        lowest_compatible: v["lowest_compatible_protocol_version"].as_u64().unwrap(),
    };
    let r = BinaryReader::include_version(&bytes).unwrap();
    let prefix = r.protocol_version().unwrap();
    assert_eq!(prefix.0, v["prefix"].as_u64().unwrap());
    assert!(prefix.has_object_serializer_flag());
    assert_eq!(to_hex(r.remaining()), v["payload"].as_str().unwrap());
    let decoded: SwVersion = fdb_native_wire::decode(r.remaining()).unwrap();
    assert_eq!(decoded, expected);
    assert_eq!(
        fdb_native_wire::FileIdentifier::new(
            u32::try_from(v["file_identifier"].as_u64().unwrap()).unwrap()
        ),
        SwVersion::FILE_IDENTIFIER
    );

    // The same value written here reads back the same.
    let mut w = BinaryWriter::include_version(prefix).unwrap();
    w.write_bytes(&fdb_native_wire::encode(&expected).unwrap())
        .unwrap();
    let written = w.into_bytes();
    assert_eq!(&written[..8], &bytes[..8]);
    let r = BinaryReader::include_version(&written).unwrap();
    assert_eq!(
        fdb_native_wire::decode::<SwVersion>(r.remaining()).unwrap(),
        expected
    );
}

// ---------------------------------------------------------------------------------------
// Test types that take no bytes, and that nest without end.

/// A struct with no members (`serializer(ar)` with no arguments), which takes no bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Unit;

impl BinarySerialize for Unit {
    fn serialize(&self, _: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        Ok(())
    }
}

impl<'de> BinaryDeserialize<'de> for Unit {
    fn deserialize(_: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        Ok(Unit)
    }
}

/// `struct Node { Optional<Node> next; }`, as a boxed chain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Node {
    pub next: Option<Box<Node>>,
}

impl BinarySerialize for Node {
    fn serialize(&self, w: &mut BinaryWriter) -> Result<(), BinaryEncodeError> {
        w.write(&self.next)
    }
}

impl<'de> BinaryDeserialize<'de> for Node {
    fn deserialize(r: &mut BinaryReader<'de>) -> Result<Self, BinaryDecodeError> {
        Ok(Node { next: r.read()? })
    }
}
