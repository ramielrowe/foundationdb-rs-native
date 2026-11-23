//! Support for the binary-format tests: the fixtures of `tools/golden-vectors/fixtures/binary-*`
//! (written by FDB's own `Writer`), the Rust types they hold, and a dispatcher from the
//! fixtures' `type` names to those types.

#![allow(dead_code, unreachable_pub)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::path::{Path, PathBuf};

use fdb_native_wire::binary::{
    Decode, DecodeError, DecodeOwned, Encode, EncodeError, ReadVersioning, Reader, Versioning,
    Writer, from_slice, to_vec,
};
use fdb_native_wire::{
    FdbError, FileIdentified, FileIdentifier, ObjectRef, ProtocolVersion, Shape, Uid, Wire,
    Writer as FlatWriter,
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

/// The writer's version option of a record.
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

/// The release's `minInvalidProtocolVersion`: what its IncludeVersion readers refuse.
pub fn min_invalid(release: &str) -> ProtocolVersion {
    ProtocolVersion(
        read_json(&fixtures_dir(release).join("index.json"))["min_invalid_protocol_version"]
            .as_u64()
            .expect("min_invalid_protocol_version"),
    )
}

/// The reader's version option of a record.
pub fn read_versioning(release: &str, record: &Value) -> ReadVersioning {
    match versioning(record) {
        Versioning::Unversioned => ReadVersioning::Unversioned,
        Versioning::Include(_) => ReadVersioning::Include {
            min_invalid: min_invalid(release),
        },
        Versioning::Assume(v) => ReadVersioning::Assume(v),
    }
}

pub fn record_bytes(record: &Value) -> Vec<u8> {
    from_hex(record["bytes"].as_str().expect("bytes"))
}

// ---------------------------------------------------------------------------------------
// Logical values: the JSON each fixture records, for the types the fixtures use.

pub trait Logical: Encode + DecodeOwned + Debug + PartialEq + Sized {
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

impl Encode for Bytes {
    fn serialize(&self, w: &mut Writer) -> Result<(), EncodeError> {
        self.0.serialize(w)
    }
}

impl<'de> Decode<'de> for Bytes {
    fn deserialize(r: &mut Reader<'de>) -> Result<Self, DecodeError> {
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

fdb_native_wire::binary::binary_enum!(Color: u8 {
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

fdb_native_wire::binary::binary_enum!(Level: i32 {
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

impl Encode for Point3 {
    fn serialize(&self, w: &mut Writer) -> Result<(), EncodeError> {
        w.write(&self.x)?;
        w.write(&self.y)?;
        w.write(&self.label)
    }
}

impl<'de> Decode<'de> for Point3 {
    fn deserialize(r: &mut Reader<'de>) -> Result<Self, DecodeError> {
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

impl Encode for Variant3 {
    fn serialize(&self, w: &mut Writer) -> Result<(), EncodeError> {
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

impl<'de> Decode<'de> for Variant3 {
    fn deserialize(r: &mut Reader<'de>) -> Result<Self, DecodeError> {
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
pub fn check_positive<T: Logical>(release: &str, record: &Value) {
    let name = record["name"].as_str().unwrap_or("?");
    let write = versioning(record);
    let read = read_versioning(release, record);
    let bytes = record_bytes(record);
    let expected = T::from_json(&record["value"]);
    let decoded: T = from_slice(&bytes, read).unwrap_or_else(|e| panic!("{name}: decode: {e}"));
    assert_eq!(decoded, expected, "{name}: decoded value");
    assert_eq!(
        decoded.to_json(),
        record["value"],
        "{name}: json of the decoded value"
    );
    let encoded = to_vec(&expected, write).unwrap_or_else(|e| panic!("{name}: encode: {e}"));
    assert_eq!(to_hex(&encoded), to_hex(&bytes), "{name}: encoded bytes");
}

/// Reads `bytes` as the fixture type `ty`; the value is dropped. `None` for a type that
/// needs more than `from_slice` (see [`check_record`]).
pub fn decode_any(
    ty: &str,
    bytes: &[u8],
    versioning: ReadVersioning,
) -> Option<Result<(), DecodeError>> {
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
        _ => None,
    }
}

/// Checks one positive fixture against the Rust type its `type` names.
pub fn check_record(release: &str, record: &Value) {
    let ty = record["type"].as_str().expect("type");
    match ty {
        "u8" => check_positive::<u8>(release, record),
        "i8" => check_positive::<i8>(release, record),
        "u16" => check_positive::<u16>(release, record),
        "i16" => check_positive::<i16>(release, record),
        "u32" => check_positive::<u32>(release, record),
        "i32" => check_positive::<i32>(release, record),
        "u64" => check_positive::<u64>(release, record),
        "i64" => check_positive::<i64>(release, record),
        "f64" => check_positive::<f64>(release, record),
        "bool" => check_positive::<bool>(release, record),
        "enum_color" => check_positive::<Color>(release, record),
        "enum_level" => check_positive::<Level>(release, record),
        "uid" => check_positive::<Uid>(release, record),
        "protocol_version" => check_positive::<ProtocolVersion>(release, record),
        "error" => check_positive::<FdbError>(release, record),
        "string" => check_positive::<String>(release, record),
        "bytes" => check_positive::<Bytes>(release, record),
        "vec<i32>" => check_positive::<Vec<i32>>(release, record),
        "vec<u8>" => check_positive::<Vec<u8>>(release, record),
        "vec<string>" => check_positive::<Vec<String>>(release, record),
        "vec<vec<u16>>" => check_positive::<Vec<Vec<u16>>>(release, record),
        "vec<bytes>" => check_positive::<Vec<Bytes>>(release, record),
        "vec<key_value>" => check_positive::<Vec<(Bytes, Bytes)>>(release, record),
        "array<u16;3>" => check_positive::<[u16; 3]>(release, record),
        "array<u8;4>" => check_positive::<[u8; 4]>(release, record),
        "set<i32>" => check_positive::<BTreeSet<i32>>(release, record),
        "map<string,i64>" => check_positive::<BTreeMap<String, i64>>(release, record),
        "map<uid,i64>" => check_positive::<BTreeMap<Uid, i64>>(release, record),
        "optional<i32>" => check_positive::<Option<i32>>(release, record),
        "optional<string>" => check_positive::<Option<String>>(release, record),
        "pair<u8,string>" => check_positive::<(u8, String)>(release, record),
        "point3" => check_positive::<Point3>(release, record),
        "variant3" => check_positive::<Variant3>(release, record),
        "key_range" => check_positive::<(Bytes, Bytes)>(release, record),
        "key_servers_value" => check_key_servers_value(release, record),
        "sw_version_value" => check_sw_version_value(release, record),
        "protocol_version_compat" => check_protocol_version_compat(record),
        other => panic!("fixture type {other:?} has no Rust check"),
    }
}

// ---------------------------------------------------------------------------------------
// Fixtures that need more than a type: two usage sites read with the generic codec only (the
// types for the others are in fdb-native-schema and fdb-native-transport, with their tests),
// and ProtocolVersion's truth table.

// A `\xff/keyServers/` value: IncludeVersion, then `vector<UID> src, vector<UID> dest,
// UID srcId` and, when `dest` is not empty, `UID destId`.
fn check_key_servers_value(release: &str, record: &Value) {
    let bytes = record_bytes(record);
    let v = &record["value"];
    let mut r = Reader::include_version(&bytes, min_invalid(release)).unwrap();
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
    let mut w = Writer::include_version(version).unwrap();
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

impl fdb_native_wire::Encode for SwVersion {
    fn encode_object(&self, w: &mut FlatWriter) -> Result<ObjectRef, fdb_native_wire::EncodeError> {
        let mut t = w.table();
        t.field(&self.newest)?;
        t.field(&self.last_run)?;
        t.field(&self.lowest_compatible)?;
        t.finish()
    }
}

impl<'de> fdb_native_wire::Decode<'de> for SwVersion {
    fn absent() -> Self {
        SwVersion {
            newest: 0,
            last_run: 0,
            lowest_compatible: 0,
        }
    }

    fn decode_object(
        r: &fdb_native_wire::Reader<'de>,
        at: usize,
    ) -> Result<Self, fdb_native_wire::DecodeError> {
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
fn check_sw_version_value(release: &str, record: &Value) {
    let bytes = record_bytes(record);
    let v = &record["value"];
    let expected = SwVersion {
        newest: v["newest_protocol_version"].as_u64().unwrap(),
        last_run: v["last_run_protocol_version"].as_u64().unwrap(),
        lowest_compatible: v["lowest_compatible_protocol_version"].as_u64().unwrap(),
    };
    let r = Reader::include_version(&bytes, min_invalid(release)).unwrap();
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
    let mut w = Writer::include_version(prefix).unwrap();
    w.write_bytes(&fdb_native_wire::encode(&expected).unwrap())
        .unwrap();
    let written = w.into_bytes();
    assert_eq!(&written[..8], &bytes[..8]);
    let r = Reader::include_version(&written, min_invalid(release)).unwrap();
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

impl Encode for Unit {
    fn serialize(&self, _: &mut Writer) -> Result<(), EncodeError> {
        Ok(())
    }
}

impl<'de> Decode<'de> for Unit {
    fn deserialize(_: &mut Reader<'de>) -> Result<Self, DecodeError> {
        Ok(Unit)
    }
}

/// `struct Node { Optional<Node> next; }`, as a boxed chain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Node {
    pub next: Option<Box<Node>>,
}

impl Encode for Node {
    fn serialize(&self, w: &mut Writer) -> Result<(), EncodeError> {
        w.write(&self.next)
    }
}

impl<'de> Decode<'de> for Node {
    fn deserialize(r: &mut Reader<'de>) -> Result<Self, DecodeError> {
        Ok(Node { next: r.read()? })
    }
}

// ---------------------------------------------------------------------------------------
// ProtocolVersion: C++'s isValid, isCompatible, version() and hasObjectSerializerFlag.

fn check_protocol_version_compat(record: &Value) {
    let v = &record["value"];
    for version in v["versions"].as_array().unwrap() {
        let pv = ProtocolVersion(version["value"].as_u64().unwrap());
        assert_eq!(pv.version(), version["version"].as_u64().unwrap(), "{pv}");
        assert_eq!(pv.is_valid(), version["valid"].as_bool().unwrap(), "{pv}");
        assert_eq!(
            pv.has_object_serializer_flag(),
            version["has_object_serializer_flag"].as_bool().unwrap(),
            "{pv}"
        );
    }
    let pairs = v["pairs"].as_array().unwrap();
    assert!(pairs.len() >= 100);
    let mut compatible = 0;
    for pair in pairs {
        let a = ProtocolVersion(pair["a"].as_u64().unwrap());
        let b = ProtocolVersion(pair["b"].as_u64().unwrap());
        let want = pair["compatible"].as_bool().unwrap();
        assert_eq!(a.is_compatible(b), want, "{a} vs {b}");
        compatible += usize::from(want);
    }
    // The table has both answers.
    assert!(compatible > 0 && compatible < pairs.len());
}
