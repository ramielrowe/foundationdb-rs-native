//! Shared test support: Rust equivalents of every engine `cpp_type` in
//! `tools/golden-vectors/fixtures/engine/schemas.json`, a dumper that turns them into the
//! fixtures' JSON node format, a JSON comparator, fixture loading, and proptest strategies.
//!
//! The tables and unions use `#[derive(Wire)]`, so the fixtures, the corpus, the proptests
//! and the C++ cross-check all test the derive's output.

#![allow(dead_code, unreachable_pub)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, EnsureTable, ErrorOr, FdbError, FileIdentified,
    ObjectRef, Reader, ReplyPromise, Shape, Uid, VectorRef, Void, Wire, Writer,
    encode_with_identifier,
};
use proptest::prelude::*;
use serde_json::{Map, Value, json};

// ---------------------------------------------------------------------------------------
// Fixtures.

pub fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tools/golden-vectors/fixtures/engine")
}

pub fn read_json(path: &Path) -> Value {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parsing {}: {e}", path.display()))
}

pub fn index() -> Value {
    read_json(&fixtures_dir().join("index.json"))
}

/// Every record listed under `key` ("positive" or "negative") in index.json.
pub fn records(key: &str) -> Vec<Value> {
    index()[key]
        .as_array()
        .expect("index list")
        .iter()
        .map(|f| read_json(&fixtures_dir().join(f.as_str().expect("file name"))))
        .collect()
}

/// Every corpus line, with the corpus file name.
pub fn corpus() -> Vec<(String, Value)> {
    let mut out = Vec::new();
    for c in index()["corpus"].as_array().expect("corpus list") {
        let file = c["file"].as_str().expect("corpus file");
        let text = std::fs::read_to_string(fixtures_dir().join(file)).expect("corpus");
        let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(
            lines.len() as u64,
            c["entries"].as_u64().expect("entries"),
            "{file}"
        );
        for line in lines {
            out.push((
                file.to_string(),
                serde_json::from_str(line).expect("corpus line"),
            ));
        }
    }
    out
}

pub fn hex_decode(s: &str) -> Vec<u8> {
    let s: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    assert!(s.len() % 2 == 0, "odd hex length");
    s.chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).expect("hex"), 16).expect("hex"))
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
            a.iter()
                .find_map(|(k, x)| diff_at(&format!("{path}.{k}"), x, &b[k]))
        }
        _ => (want != got).then(|| format!("{path}: want {want}, got {got}")),
    }
}

pub fn assert_json_eq(context: &str, want: &Value, got: &Value) {
    if let Some(d) = json_diff(want, got) {
        panic!("{context}: {d}\nwant: {want}\n got: {got}");
    }
}

// ---------------------------------------------------------------------------------------
// The dumper: the fixtures' node format (see the README's "Nodes").

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

/// The synthetic root table holding `members` (slot counts per member).
pub fn root_dump(members: Vec<(usize, Value)>) -> Value {
    let names: Vec<String> = (0..members.len()).map(|i| i.to_string()).collect();
    let members = members
        .into_iter()
        .zip(&names)
        .map(|((s, n), name)| (name.as_str(), s, n))
        .collect();
    node("table<root>".into(), table_value(members))
}

pub fn root1<T: Dump + Wire>(v: &T) -> Value {
    root_dump(vec![(T::SHAPE.slots(), v.node())])
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

impl Dump for f32 {
    fn type_name() -> String {
        "f32".into()
    }
    fn node(&self) -> Value {
        node(Self::type_name(), Value::from(f64::from(*self)))
    }
}

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

impl Dump for Void {
    fn type_name() -> String {
        "table<Void>".into()
    }
    fn node(&self) -> Value {
        node(Self::type_name(), Value::Array(vec![]))
    }
}

macro_rules! dump_tuple {
    ($($t:ident $i:tt),+) => {
        impl<$($t: Dump + Wire),+> Dump for ($($t,)+) {
            fn type_name() -> String {
                format!("struct<{}>", [$($t::type_name()),+].join(","))
            }
            fn node(&self) -> Value {
                let shapes = [$($t::SHAPE),+];
                let members = vec![$(
                    member(stringify!($i), "offset",
                           Value::from(Shape::struct_member_offset(&shapes, $i)), self.$i.node())
                ),+];
                node(Self::type_name(), Value::Array(members))
            }
        }
    };
}

dump_tuple!(A 0, B 1);
dump_tuple!(A 0, B 1, C 2);

// ---------------------------------------------------------------------------------------
// Proptest strategies.

pub trait Arb: Sized + Debug + 'static {
    fn arb() -> BoxedStrategy<Self>;
}

macro_rules! arb_any {
    ($($t:ty),*) => {$(
        impl Arb for $t {
            fn arb() -> BoxedStrategy<Self> { any::<$t>().boxed() }
        }
    )*};
}

arb_any!(u8, i8, u16, i16, u32, i32, u64, i64, bool, [u8; 16]);

impl Arb for f32 {
    fn arb() -> BoxedStrategy<Self> {
        use proptest::num::f32::*;
        (POSITIVE | NEGATIVE | NORMAL | SUBNORMAL | ZERO).boxed()
    }
}

impl Arb for f64 {
    fn arb() -> BoxedStrategy<Self> {
        use proptest::num::f64::*;
        (POSITIVE | NEGATIVE | NORMAL | SUBNORMAL | ZERO).boxed()
    }
}

impl Arb for Uid {
    fn arb() -> BoxedStrategy<Self> {
        (any::<u64>(), any::<u64>())
            .prop_map(|(a, b)| Uid::new(a, b))
            .boxed()
    }
}

impl Arb for &'static [u8] {
    fn arb() -> BoxedStrategy<Self> {
        let samples: Vec<&'static [u8]> = vec![
            b"",
            b"a",
            b"key",
            b"\x00\xff\x01",
            b"four",
            b"a longer value",
        ];
        prop::sample::select(samples).boxed()
    }
}

impl<T: Arb> Arb for Vec<T> {
    fn arb() -> BoxedStrategy<Self> {
        prop::collection::vec(T::arb(), 0..4).boxed()
    }
}

impl<T: Arb> Arb for Option<T> {
    fn arb() -> BoxedStrategy<Self> {
        prop::option::of(T::arb()).boxed()
    }
}

impl<T: Arb> Arb for ErrorOr<T> {
    fn arb() -> BoxedStrategy<Self> {
        prop_oneof![
            any::<u16>().prop_map(|c| Err(FdbError::new(c))),
            T::arb().prop_map(Ok)
        ]
        .boxed()
    }
}

impl<T: Arb> Arb for EnsureTable<T> {
    fn arb() -> BoxedStrategy<Self> {
        T::arb().prop_map(EnsureTable).boxed()
    }
}

impl Arb for Void {
    fn arb() -> BoxedStrategy<Self> {
        Just(Void).boxed()
    }
}

impl<A: Arb, B: Arb> Arb for (A, B) {
    fn arb() -> BoxedStrategy<Self> {
        (A::arb(), B::arb()).boxed()
    }
}

impl<A: Arb, B: Arb, C: Arb> Arb for (A, B, C) {
    fn arb() -> BoxedStrategy<Self> {
        (A::arb(), B::arb(), C::arb()).boxed()
    }
}

// ---------------------------------------------------------------------------------------
// Bytes types.

/// `std::string`: owned bytes, dumped as "bytes" (a `Vec<u8>` is a `vector<u8>`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Bytes(pub Vec<u8>);

impl Wire for Bytes {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for Bytes {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.blob(&self.0)
    }
}

impl<'de> Decode<'de> for Bytes {
    fn absent() -> Self {
        Bytes::default()
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        r.blob(at).map(|b| Bytes(b.to_vec()))
    }
}

impl Dump for Bytes {
    fn type_name() -> String {
        "bytes".into()
    }
    fn node(&self) -> Value {
        node(Self::type_name(), Value::from(hex_encode(&self.0)))
    }
}

impl Arb for Bytes {
    fn arb() -> BoxedStrategy<Self> {
        prop::collection::vec(any::<u8>(), 0..12)
            .prop_map(Bytes)
            .boxed()
    }
}

/// `VectorRef<KeyValueRef, VecSerStrategy::String>`: a blob codec. The payload is a u32
/// item count, then per item a u32 key length, the key, a u32 value length, the value.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KvVector(pub Vec<(Vec<u8>, Vec<u8>)>);

impl Wire for KvVector {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for KvVector {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let len = 4 + self
            .0
            .iter()
            .map(|(k, v)| 8 + k.len() + v.len())
            .sum::<usize>();
        w.blob_with(len, |out| {
            let mut at = 0;
            let mut put = |bytes: &[u8]| {
                out[at..at + bytes.len()].copy_from_slice(bytes);
                at += bytes.len();
            };
            put(&(self.0.len() as u32).to_le_bytes());
            for (k, v) in &self.0 {
                put(&(k.len() as u32).to_le_bytes());
                put(k);
                put(&(v.len() as u32).to_le_bytes());
                put(v);
            }
        })
    }
}

impl<'de> Decode<'de> for KvVector {
    fn absent() -> Self {
        KvVector::default()
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        const OVERRUN: DecodeError =
            DecodeError::Invalid("preserialized vector item overruns blob");
        fn take<'a>(blob: &mut &'a [u8], n: usize) -> Result<&'a [u8], DecodeError> {
            let (head, tail) = blob.split_at_checked(n).ok_or(OVERRUN)?;
            *blob = tail;
            Ok(head)
        }
        fn get32(blob: &mut &[u8]) -> Result<usize, DecodeError> {
            let b = take(blob, 4)?;
            Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
        }
        let mut blob = r.blob(at)?;
        let count = get32(&mut blob)?;
        // Not Vec::with_capacity(count): the count is untrusted.
        let mut items = Vec::new();
        for _ in 0..count {
            let k = get32(&mut blob)?;
            let key = take(&mut blob, k)?.to_vec();
            let v = get32(&mut blob)?;
            let value = take(&mut blob, v)?.to_vec();
            items.push((key, value));
        }
        if !blob.is_empty() {
            return Err(DecodeError::Invalid(
                "preserialized vector has trailing bytes",
            ));
        }
        Ok(KvVector(items))
    }
}

impl Dump for KvVector {
    fn type_name() -> String {
        "preserialized_kv_vector".into()
    }
    fn node(&self) -> Value {
        let items = self
            .0
            .iter()
            .map(|(k, v)| json!({ "key": hex_encode(k), "value": hex_encode(v) }))
            .collect();
        node(Self::type_name(), Value::Array(items))
    }
}

impl Arb for KvVector {
    fn arb() -> BoxedStrategy<Self> {
        prop::collection::vec(
            (
                prop::collection::vec(any::<u8>(), 0..6),
                prop::collection::vec(any::<u8>(), 0..6),
            ),
            0..4,
        )
        .prop_map(KvVector)
        .boxed()
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

impl<T: Debug + 'static> Arb for ReplyPromise<T> {
    fn arb() -> BoxedStrategy<Self> {
        Uid::arb().prop_map(ReplyPromise::new).boxed()
    }
}

impl<T: Dump> Dump for VectorRef<T> {
    fn type_name() -> String {
        <Vec<T> as Dump>::type_name()
    }
    fn node(&self) -> Value {
        self.0.node()
    }
}

impl<T: Arb> Arb for VectorRef<T> {
    fn arb() -> BoxedStrategy<Self> {
        Vec::<T>::arb().prop_map(VectorRef).boxed()
    }
}

impl Arb for () {
    fn arb() -> BoxedStrategy<Self> {
        Just(()).boxed()
    }
}

// ---------------------------------------------------------------------------------------
// Tables and unions.
//
// The wire impls are `#[derive(Wire)]`. `dump_table!` and `dump_union!` add the test-only
// `Dump` and `Arb` impls; they restate the C++ names (and, for unions, the tags), which the
// fixtures then check against what the derive does.

/// The strategy of a field, whose type is inferred from the struct literal it builds.
pub fn arb_field<T: Arb>(_name: &str) -> BoxedStrategy<T> {
    T::arb()
}

/// The number of slots a field takes.
pub fn slots_of<T: Wire>(_: &T) -> usize {
    T::SHAPE.slots()
}

/// `Dump` and (unless `@no_arb`) `Arb` for a derived table: the C++ type name, then its
/// serialized fields in order with their C++ names. `@arena` names the `#[fdb(arena)]`
/// field, dumped as an `arena` member. Other test files use it via `#[macro_use] mod
/// support;` and `use support::*;`.
macro_rules! dump_table {
    (@no_arb $cpp:literal $name:ident $(<$lt:lifetime>)? { $($f:ident = $fname:literal),* $(,)? } $(@ $arena:ident)?) => {
        dump_table!(@dump $cpp, $name, [$($lt)?], { $($f = $fname),* } $(@ $arena)?);
    };
    ($cpp:literal $name:ident { $($f:ident = $fname:literal),* $(,)? } $(@ $arena:ident)?) => {
        dump_table!(@dump $cpp, $name, [], { $($f = $fname),* } $(@ $arena)?);
        dump_table!(@arb $name, [], { $($f),* } $(@ $arena)?);
    };
    ($cpp:literal $name:ident<$lt:lifetime> { $($f:ident = $fname:literal),* $(,)? } $(@ $arena:ident)?) => {
        dump_table!(@dump $cpp, $name, [$lt], { $($f = $fname),* } $(@ $arena)?);
        dump_table!(@arb $name, ['static], { $($f),* } $(@ $arena)?);
    };
    (@dump $cpp:literal, $name:ident, [$($lt:lifetime)?], { $($f:ident = $fname:literal),* } $(@ $arena:ident)?) => {
        impl<$($lt)?> Dump for $name<$($lt)?> {
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
    (@arb $name:ident, [$($lt:lifetime)?], { $($f:ident),* } $(@ $arena:ident)?) => {
        impl Arb for $name<$($lt)?> {
            fn arb() -> BoxedStrategy<Self> {
                ($(arb_field(stringify!($f)),)*)
                    .prop_map(|($($f,)*)| $name { $($f,)* $($arena: (),)? })
                    .boxed()
            }
        }
    };
}

/// `Dump` and `Arb` for a derived union (`std::variant`): its alternatives with their
/// expected wire tags.
macro_rules! dump_union {
    ($name:ident { $($var:ident($ty:ty) = $tag:literal),+ $(,)? }) => {
        impl Dump for $name {
            fn type_name() -> String {
                let alts: Vec<String> = vec![$(<$ty>::type_name()),+];
                format!("variant<{}>", alts.join(","))
            }
            fn node(&self) -> Value {
                match self {
                    $($name::$var(v) => tagged(Self::type_name(), $tag, v.node()),)+
                }
            }
        }

        impl Arb for $name {
            fn arb() -> BoxedStrategy<Self> {
                prop_oneof![$(<$ty>::arb().prop_map($name::$var)),+].boxed()
            }
        }
    };
}

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb0a)]
pub struct Inner {
    pub a: u32,
    pub b: Bytes,
}
dump_table!("Inner" Inner { a = "a", b = "b" });

#[derive(Clone, Debug, PartialEq, Wire)]
pub struct Small {
    pub x: i32,
    pub name: Bytes,
}
dump_table!("Small" Small { x = "x", name = "name" });

#[derive(Clone, Debug, PartialEq, Wire)]
pub struct Nested2 {
    pub a: u8,
    pub b: Vec<Bytes>,
    pub c: i32,
}
dump_table!("Nested2" Nested2 { a = "a", b = "b", c = "c" });

#[derive(Clone, Debug, PartialEq, Wire)]
pub struct Nested {
    pub a: u8,
    pub b: Bytes,
    pub nested: Nested2,
    pub c: Vec<u64>,
}
dump_table!("Nested" Nested { a = "a", b = "b", nested = "nested", c = "c" });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb08)]
pub struct Root {
    pub a: u8,
    pub b: Vec<Nested2>,
    pub c: Nested,
}
dump_table!("Root" Root { a = "a", b = "b", c = "c" });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb01)]
pub struct Scalars {
    pub b: bool,
    pub u8: u8,
    pub i8: i8,
    pub u16: u16,
    pub i16: i16,
    pub u32: u32,
    pub i32: i32,
    pub u64: u64,
    pub i64: i64,
    pub f32: f32,
    pub f64: f64,
}
dump_table!("Scalars" Scalars {
    b = "b", u8 = "u8", i8 = "i8", u16 = "u16", i16 = "i16", u32 = "u32", i32 = "i32",
    u64 = "u64", i64 = "i64", f32 = "f32", f64 = "f64",
});

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb02)]
pub struct Strings<'a> {
    pub s: Bytes,
    pub empty: Bytes,
    pub ref_: &'a [u8],
    pub empty_ref: &'a [u8],
    pub binary: Bytes,
    pub len4: Bytes,
}
dump_table!("Strings" Strings<'a> {
    s = "s", empty = "empty", ref_ = "ref", empty_ref = "emptyRef", binary = "binary",
    len4 = "len4",
});

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb04)]
pub struct Vectors {
    pub v_u8: Vec<u8>,
    pub v_u16: Vec<u16>,
    pub v_i32: Vec<i32>,
    pub v_u64: Vec<u64>,
    pub v_f64: Vec<f64>,
    pub v_str: Vec<Bytes>,
    pub v_table: Vec<Small>,
    pub v_empty_u32: Vec<u32>,
    pub v_empty_str: Vec<Bytes>,
    pub v_empty_table: Vec<Small>,
    pub v_bool: Vec<bool>,
    pub fixed16: [u8; 16],
}
dump_table!("Vectors" Vectors {
    v_u8 = "v_u8", v_u16 = "v_u16", v_i32 = "v_i32", v_u64 = "v_u64", v_f64 = "v_f64",
    v_str = "v_str", v_table = "v_table", v_empty_u32 = "v_empty_u32",
    v_empty_str = "v_empty_str", v_empty_table = "v_empty_table", v_bool = "v_bool",
    fixed16 = "fixed16",
});

#[derive(Clone, Debug, PartialEq, Wire)]
pub struct Pair {
    pub first: i32,
    pub second: Bytes,
}
dump_table!("pair<i32,bytes>" Pair { first = "first", second = "second" });

/// `m` is a std::map and `s` a std::set: C++ sorts them and drops duplicate keys.
#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb05)]
pub struct Containers {
    pub m: Vec<Pair>,
    pub s: Vec<u32>,
    pub nested: Vec<Vec<u16>>,
}
dump_table!(@no_arb "Containers" Containers { m = "m", s = "s", nested = "nested" });

impl Arb for Containers {
    fn arb() -> BoxedStrategy<Self> {
        let m = prop::collection::btree_map(any::<i32>(), Bytes::arb(), 0..4).prop_map(|m| {
            m.into_iter()
                .map(|(first, second)| Pair { first, second })
                .collect()
        });
        let s =
            prop::collection::btree_set(any::<u32>(), 0..4).prop_map(|s| s.into_iter().collect());
        (m, s, Vec::<Vec<u16>>::arb())
            .prop_map(|(m, s, nested)| Containers { m, s, nested })
            .boxed()
    }
}

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb09)]
pub struct Structs {
    pub tag: u8,
    pub inline3: (i16, bool, i64),
    pub inline2: (u8, u32),
    pub many: Vec<(i16, bool, i64)>,
    pub after: u16,
}
dump_table!("Structs" Structs {
    tag = "tag", inline3 = "inline3", inline2 = "inline2", many = "many", after = "after",
});

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb0b)]
pub struct Optionals {
    pub int_present: Option<i32>,
    pub int_absent: Option<i32>,
    pub str_present: Option<Bytes>,
    pub str_absent: Option<Bytes>,
    pub table_present: Option<Inner>,
    pub table_absent: Option<Inner>,
    pub f64_present: Option<f64>,
    pub uid_present: Option<Uid>,
    pub vec_of_opt: Vec<Option<i32>>,
}
dump_table!("Optionals" Optionals {
    int_present = "int_present", int_absent = "int_absent", str_present = "str_present",
    str_absent = "str_absent", table_present = "table_present", table_absent = "table_absent",
    f64_present = "f64_present", uid_present = "uid_present", vec_of_opt = "vec_of_opt",
});

#[derive(Clone, Debug, PartialEq, Wire)]
pub enum HolderVariant {
    I32(i32),
    F64(f64),
    Nested2(Nested2),
    Bytes(Bytes),
    U8(u8),
}
dump_union!(HolderVariant { I32(i32) = 1, F64(f64) = 2, Nested2(Nested2) = 3, Bytes(Bytes) = 4, U8(u8) = 5 });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb0c)]
pub struct VariantHolder {
    pub before: u8,
    pub v: HolderVariant,
    pub after: u16,
}
dump_table!("VariantHolder" VariantHolder { before = "before", v = "v", after = "after" });

#[derive(Clone, Debug, PartialEq, Wire)]
pub enum RootVariant {
    I32(i32),
    F64(f64),
    Nested2(Nested2),
}
dump_union!(RootVariant { I32(i32) = 1, F64(f64) = 2, Nested2(Nested2) = 3 });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb0d)]
pub struct Payload {
    pub version: i64,
    pub value: Bytes,
    pub id: Option<Uid>,
}
dump_table!("Payload" Payload { version = "version", value = "value", id = "id" });

#[derive(Clone, Debug, PartialEq, Wire)]
pub enum WrappedAlt {
    I32(EnsureTable<i32>),
    Inner(Inner),
}
dump_union!(WrappedAlt { I32(EnsureTable<i32>) = 1, Inner(Inner) = 2 });

#[derive(Clone, Debug, PartialEq, Wire)]
pub enum BareAlt {
    I32(i32),
    Inner(Inner),
}
dump_union!(BareAlt { I32(i32) = 1, Inner(Inner) = 2 });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb0e)]
pub struct EnsureTables {
    pub scalar: EnsureTable<i32>,
    pub table: EnsureTable<Inner>,
    pub str: EnsureTable<Bytes>,
    pub vec: EnsureTable<Vec<u16>>,
    pub wrapped_alt: WrappedAlt,
    pub bare_alt: BareAlt,
}
dump_table!("EnsureTables" EnsureTables {
    scalar = "scalar", table = "table", str = "str", vec = "vec", wrapped_alt = "wrapped_alt",
    bare_alt = "bare_alt",
});

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb0f)]
pub struct RequestLike<'a> {
    pub key: &'a [u8],
    pub version: i64,
    pub debug_id: Option<Uid>,
    pub reply: ReplyPromise<Inner>,
    #[fdb(arena)]
    pub arena: (),
}
dump_table!("RequestLike" RequestLike<'a> {
    key = "key", version = "version", debug_id = "debugID", reply = "reply",
} @arena);

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb10)]
pub struct OrderDemo {
    pub d: u16,
    pub c: Bytes,
    pub a: u8,
    pub b: u64,
}
dump_table!("OrderDemo" OrderDemo { d = "d", c = "c", a = "a", b = "b" });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb11)]
pub struct ArenaHolder<'a> {
    pub keys: Vec<&'a [u8]>,
    pub single: &'a [u8],
    pub nums: Vec<i64>,
    #[fdb(arena)]
    pub arena: (),
}
dump_table!("ArenaHolder" ArenaHolder<'a> { keys = "keys", single = "single", nums = "nums" } @arena);

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb12)]
pub struct KvHolder {
    pub data: KvVector,
    pub more: bool,
}
dump_table!("KVHolder" KvHolder { data = "data", more = "more" });

#[derive(Clone, Debug, PartialEq, Wire)]
pub struct DedupA {
    pub x: i32,
}
dump_table!("DedupA" DedupA { x = "x" });

#[derive(Clone, Debug, PartialEq, Wire)]
pub struct DedupB {
    pub y: u32,
}
dump_table!("DedupB" DedupB { y = "y" });

#[derive(Clone, Debug, PartialEq, Wire)]
pub struct DedupC {
    pub z: f32,
}
dump_table!("DedupC" DedupC { z = "z" });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb16)]
pub struct DedupHolder {
    pub a: DedupA,
    pub b: DedupB,
    pub c: DedupC,
    pub va: Vec<DedupA>,
}
dump_table!("DedupHolder" DedupHolder { a = "a", b = "b", c = "c", va = "va" });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb17)]
pub struct SchemaV1 {
    pub a: i32,
    pub b: Bytes,
}
dump_table!("SchemaV1" SchemaV1 { a = "a", b = "b" });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb17)]
pub struct SchemaV2 {
    pub a: i32,
    pub b: Bytes,
    pub c: u64,
    pub d: Option<Bytes>,
    pub e: Vec<u32>,
}
dump_table!("SchemaV2" SchemaV2 { a = "a", b = "b", c = "c", d = "d", e = "e" });

#[derive(Clone, Debug, PartialEq, Wire)]
pub struct Y1 {
    pub a: i32,
}
dump_table!("Y1" Y1 { a = "a" });

#[derive(Clone, Debug, PartialEq, Wire)]
pub enum Y2Variant {
    I32(i32),
}
dump_union!(Y2Variant { I32(i32) = 1 });

#[derive(Clone, Debug, PartialEq, Wire)]
pub struct Y2 {
    pub a: i32,
    pub b: Y2Variant,
}
dump_table!("Y2" Y2 { a = "a", b = "b" });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb69)]
pub struct XY1 {
    pub a: i32,
    pub b: Y1,
    pub c: i32,
}
dump_table!("X<Y1>" XY1 { a = "a", b = "b", c = "c" });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000fdb69)]
pub struct XY2 {
    pub a: i32,
    pub b: Y2,
    pub c: i32,
}
dump_table!("X<Y2>" XY2 { a = "a", b = "b", c = "c" });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x00001092)]
pub struct Untrusted {
    pub s: Bytes,
    pub v: Vec<i64>,
    pub o: Option<i32>,
    pub vo: Vec<Option<i32>>,
    pub b: Vec<bool>,
    pub strs: Vec<Bytes>,
}
dump_table!("Untrusted" Untrusted { s = "s", v = "v", o = "o", vo = "vo", b = "b", strs = "strs" });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000010f7)]
pub struct FixedBytes {
    pub a: [u8; 16],
}
dump_table!("FixedBytes" FixedBytes { a = "a" });

#[derive(Clone, Debug, PartialEq, Wire)]
#[fdb(file_identifier = 0x000010f7)]
pub struct WireBytes {
    pub v: Vec<u8>,
}
dump_table!("WireBytes" WireBytes { v = "v" });

/// `RootMembers`: a message written with three root members.
#[derive(Clone, Debug, PartialEq)]
pub struct RootMembers {
    pub a: u8,
    pub b: Vec<Nested2>,
    pub c: Nested,
}

impl RootMembers {
    pub const FILE_IDENTIFIER: u32 = 0x000fdb66;

    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut w = Writer::new();
        let mut root = w.table();
        root.field(&self.a)?;
        root.field(&self.b)?;
        root.field(&self.c)?;
        let root = root.finish()?;
        w.finish(root, Self::FILE_IDENTIFIER)
    }

    pub fn decode(r: &Reader<'_>) -> Result<Self, DecodeError> {
        let mut root = r.root()?;
        Ok(RootMembers {
            a: root.field()?,
            b: root.field()?,
            c: root.field()?,
        })
    }

    pub fn root_dump(&self) -> Value {
        root_dump(vec![
            (1, self.a.node()),
            (1, self.b.node()),
            (1, self.c.node()),
        ])
    }
}

impl Arb for RootMembers {
    fn arb() -> BoxedStrategy<Self> {
        (u8::arb(), Vec::<Nested2>::arb(), Nested::arb())
            .prop_map(|(a, b, c)| RootMembers { a, b, c })
            .boxed()
    }
}

// ---------------------------------------------------------------------------------------
// The registry: every cpp_type.

/// A generated value: its expected root dump and its encoding.
#[derive(Clone, Debug)]
pub struct Generated {
    pub root: Value,
    pub bytes: Vec<u8>,
}

pub struct TypeEntry {
    pub name: &'static str,
    /// The identifier used to encode and expected when decoding.
    pub fid: u32,
    /// Whether `fid` came from the Rust type's `FileIdentified` impl.
    pub typed: bool,
    /// Decodes a message to its root dump; checks the file identifier if asked.
    pub decode: fn(&[u8], bool) -> Result<Value, DecodeError>,
    /// Decodes a message (no file identifier check) and encodes the value again.
    pub reencode: fn(&[u8]) -> Result<Vec<u8>, String>,
    /// Random values of the type.
    pub arb: fn() -> BoxedStrategy<Generated>,
    /// The root dump of the type's absent value (every field absent).
    pub absent_root: fn() -> Value,
}

macro_rules! entry {
    (typed $name:literal, $ty:ty) => {
        entry!(@make $name, <$ty as FileIdentified>::FILE_IDENTIFIER.value(), true, $ty)
    };
    ($name:literal, $fid:expr, $ty:ty) => {
        entry!(@make $name, $fid, false, $ty)
    };
    (@make $name:literal, $fid:expr, $typed:expr, $ty:ty) => {
        TypeEntry {
            name: $name,
            fid: $fid,
            typed: $typed,
            decode: |bytes, check| {
                let r = Reader::new(bytes)?;
                if check {
                    r.check_file_identifier($fid)?;
                }
                let v: $ty = r.decode_root()?;
                Ok(root1(&v))
            },
            reencode: |bytes| {
                let r = Reader::new(bytes).map_err(|e| e.to_string())?;
                let v: $ty = r.decode_root().map_err(|e| e.to_string())?;
                encode_with_identifier(&v, $fid).map_err(|e| e.to_string())
            },
            arb: || {
                <$ty as Arb>::arb()
                    .prop_map(|v| Generated {
                        root: root1(&v),
                        bytes: encode_with_identifier(&v, $fid).expect("encode"),
                    })
                    .boxed()
            },
            absent_root: || {
                let v: $ty = Decode::absent();
                root1(&v)
            },
        }
    };
}

pub fn registry() -> Vec<TypeEntry> {
    vec![
        entry!(typed "ArenaHolder", ArenaHolder<'_>),
        entry!(typed "Containers", Containers),
        entry!(typed "DedupHolder", DedupHolder),
        entry!(typed "EnsureTables", EnsureTables),
        entry!(typed "ErrorOr<EnsureTable<Optional<Inner>>>", ErrorOr<EnsureTable<Option<Inner>>>),
        entry!(typed "ErrorOr<EnsureTable<Payload>>", ErrorOr<EnsureTable<Payload>>),
        entry!(typed "ErrorOr<EnsureTable<Void>>", ErrorOr<EnsureTable<Void>>),
        entry!(typed "ErrorOr<Inner>", ErrorOr<Inner>),
        entry!(typed "ErrorOr<Payload>", ErrorOr<Payload>),
        entry!(typed "FixedBytes", FixedBytes),
        entry!(typed "Inner", Inner),
        entry!(typed "KVHolder", KvHolder),
        entry!(typed "Optional<Inner>", Option<Inner>),
        entry!(typed "Optionals", Optionals),
        entry!(typed "OrderDemo", OrderDemo),
        entry!(typed "ReplyPromise<Inner>", ReplyPromise<Inner>),
        entry!(typed "RequestLike", RequestLike<'_>),
        entry!(typed "Root", Root),
        TypeEntry {
            name: "RootMembers",
            fid: RootMembers::FILE_IDENTIFIER,
            typed: false,
            decode: |bytes, check| {
                let r = Reader::new(bytes)?;
                if check {
                    r.check_file_identifier(RootMembers::FILE_IDENTIFIER)?;
                }
                Ok(RootMembers::decode(&r)?.root_dump())
            },
            reencode: |bytes| {
                let r = Reader::new(bytes).map_err(|e| e.to_string())?;
                let v = RootMembers::decode(&r).map_err(|e| e.to_string())?;
                v.encode().map_err(|e| e.to_string())
            },
            arb: || {
                RootMembers::arb()
                    .prop_map(|v| Generated {
                        root: v.root_dump(),
                        bytes: v.encode().expect("encode"),
                    })
                    .boxed()
            },
            absent_root: || {
                RootMembers {
                    a: Decode::absent(),
                    b: Decode::absent(),
                    c: Decode::absent(),
                }
                .root_dump()
            },
        },
        entry!(typed "Scalars", Scalars),
        entry!(typed "SchemaV1", SchemaV1),
        entry!(typed "SchemaV2", SchemaV2),
        entry!(typed "Strings", Strings<'_>),
        entry!(typed "Structs", Structs),
        entry!(typed "Untrusted", Untrusted),
        entry!(typed "VariantHolder", VariantHolder),
        entry!(typed "VectorRef<Inner>", VectorRef<Inner>),
        entry!(typed "VectorRef<StringRef>", VectorRef<&[u8]>),
        entry!(typed "Vectors", Vectors),
        entry!(typed "WireBytes", WireBytes),
        entry!(typed "X<Y1>", XY1),
        entry!(typed "X<Y2>", XY2),
        entry!("int32_t", 0x000fdb64, i32),
        entry!("std::variant<int,double,Nested2>", 0x000fdb68, RootVariant),
        entry!(typed "std::vector<Inner>", Vec<Inner>),
        entry!("std::vector<bool>", 0x000fdb65, Vec<bool>),
        entry!(
            "std::vector<std::tuple<int16_t,bool,int64_t>>",
            0x000fdb67,
            Vec<(i16, bool, i64)>
        ),
    ]
}

pub fn registry_map() -> BTreeMap<&'static str, TypeEntry> {
    registry().into_iter().map(|e| (e.name, e)).collect()
}

// ---------------------------------------------------------------------------------------
// Aliased buffers.

/// An `Encode` that points at an object already written, to build buffers that reference
/// one object many times (which FDB's writer never does, but its reader accepts). Tables
/// and objects are both reached through an offset, so one shape serves for both.
#[derive(Clone, Copy)]
pub struct Alias(pub ObjectRef);

impl Wire for Alias {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for Alias {
    fn encode_object(&self, _: &mut Writer) -> Result<ObjectRef, EncodeError> {
        Ok(self.0)
    }
}

/// An 824-byte `Untrusted` whose `strs` holds 20 offsets to one 600-byte string, and the
/// value C++ decodes it to. Under the README's classes this is a `data` accept; this crate
/// rejects it with the `work_limit` check (20 × 604 units > 4 × 824 + 1024).
pub fn aliased_untrusted() -> (Untrusted, Vec<u8>) {
    let s = Bytes(vec![0x61; 600]);
    let value = Untrusted {
        s: Bytes(b"x".to_vec()),
        v: vec![1],
        o: Some(5),
        vo: vec![Some(1)],
        b: vec![true],
        strs: vec![s.clone(); 20],
    };
    let mut w = Writer::new();
    let blob = w.blob(&s.0).expect("blob");
    let strs = w.vector(&[Alias(blob); 20]).expect("strs");
    let mut t = w.table();
    t.field(&value.s).expect("s");
    t.field(&value.v).expect("v");
    t.field(&value.o).expect("o");
    t.field(&value.vo).expect("vo");
    t.field(&value.b).expect("b");
    t.field(&Alias(strs)).expect("strs");
    let t = t.finish().expect("table");
    let mut root = w.table();
    root.field(&Alias(t)).expect("root field");
    let root = root.finish().expect("root");
    let bytes = w
        .finish(root, Untrusted::FILE_IDENTIFIER.value())
        .expect("finish");
    (value, bytes)
}
