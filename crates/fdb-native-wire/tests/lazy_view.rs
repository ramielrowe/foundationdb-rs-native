//! The public lazy vector reader: `VectorView`, `Reader::vector_header` and
//! `Reader::blob_range`.
//!
//! The central property is equivalence with eager decoding (same values, same acceptance,
//! also on mutated and hostile bytes); the rest are the hostile-input cases the view's
//! safety argument rests on: huge counts, aliasing, out-of-bounds elements read lazily,
//! re-reading, nesting depth.

mod map_support;
mod support;

use std::fmt::Debug;
use std::ops::Range;

use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, ObjectRef, Reader, Shape, StrictCheck, Uid,
    VectorHeader, VectorView, Wire, Writer, encode, encode_with_identifier,
};
use map_support::MapContainers;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use support::*;

fn runner(cases: u32) -> TestRunner {
    TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    })
}

fn fail(e: impl std::fmt::Display) -> TestCaseError {
    TestCaseError::fail(e.to_string())
}

/// A value's identity for comparisons: its `Debug` form, which tells `NaN`s (and `-0.0`)
/// apart from the values they should equal where `PartialEq` cannot.
fn same(value: &impl Debug) -> String {
    format!("{value:?}")
}

/// Every element of the view, in order, stopping at the first error.
fn all<'de, T: Decode<'de>>(view: &VectorView<'de, T>) -> Result<Vec<T>, DecodeError> {
    view.iter().collect()
}

/// Decoding `bytes` as a root `Vec<T>` and as a root `VectorView<T>` agree: the same values
/// if eager decoding accepts, and some error (at decode or while reading elements) if not.
fn check_equivalent<'de, T: Decode<'de> + Debug>(
    bytes: &'de [u8],
    must_decode: bool,
) -> Result<(), TestCaseError> {
    let eager = Reader::new(bytes).map_err(fail)?.decode_root::<Vec<T>>();
    let reader = Reader::new(bytes).map_err(fail)?;
    let lazy = reader.decode_root::<VectorView<T>>();
    match (eager, lazy) {
        (Ok(items), Ok(view)) => {
            prop_assert_eq!(view.len(), items.len());
            prop_assert_eq!(view.is_empty(), items.is_empty());
            let collected = all(&view).map_err(fail)?;
            prop_assert_eq!(same(&collected), same(&items));
            for (i, item) in items.iter().enumerate() {
                prop_assert_eq!(same(&view.get(i).map_err(fail)?), same(item));
            }
            prop_assert_eq!(
                view.get(items.len()).err(),
                Some(DecodeError::IndexOutOfRange {
                    index: items.len(),
                    len: items.len()
                })
            );
            // Cloning shares everything.
            prop_assert_eq!(same(&all(&view.clone()).map_err(fail)?), same(&items));
        }
        (Err(_), Err(_)) => {
            prop_assert!(!must_decode);
        }
        (Err(e), Ok(view)) => {
            prop_assert!(!must_decode, "eager decode failed: {e}");
            // The header is sound; some element is not.
            prop_assert!(
                all(&view).is_err(),
                "eager decode failed ({e}) but the view reads"
            );
        }
        (Ok(_), Err(e)) => {
            return Err(fail(format!(
                "eager decode succeeded but the view failed: {e}"
            )));
        }
    }
    Ok(())
}

macro_rules! equivalence {
    ($($name:ident: $arb:ty => $dec:ty),* $(,)?) => {$(
        #[test]
        fn $name() {
            // Generated values: always accepted, always equal.
            runner(48)
                .run(&prop::collection::vec(<$arb as Arb>::arb(), 0..5), |items| {
                    let bytes = encode_with_identifier(&items, 1).map_err(fail)?;
                    check_equivalent::<$dec>(&bytes, true)
                })
                .unwrap_or_else(|e| panic!("{}: {e}", stringify!($name)));
            // The same bytes with a few words and bytes overwritten, truncated, or with a
            // stray offset: whatever eager decoding does, the view does too.
            let strategy = (
                prop::collection::vec(<$arb as Arb>::arb(), 0..5),
                prop::collection::vec(mutation(), 1..4),
            );
            runner(200)
                .run(&strategy, |(items, mutations)| {
                    let mut bytes = encode_with_identifier(&items, 1).map_err(fail)?;
                    mutate(&mut bytes, &mutations);
                    if bytes.len() < 8 {
                        return Ok(());
                    }
                    check_equivalent::<$dec>(&bytes, false)
                })
                .unwrap_or_else(|e| panic!("{} (mutated): {e}", stringify!($name)));
        }
    )*};
}

#[derive(Clone, Debug)]
enum Mutation {
    Byte(usize, u8),
    Word(usize, u32),
    Truncate(usize),
}

fn mutation() -> impl Strategy<Value = Mutation> {
    let hostile = prop::sample::select(vec![
        0u32,
        1,
        2,
        3,
        4,
        8,
        0xffff,
        0x7fff_fff0,
        0x8000_0000,
        0xffff_fff0,
        u32::MAX,
    ]);
    prop_oneof![
        (any::<usize>(), any::<u8>()).prop_map(|(p, v)| Mutation::Byte(p, v)),
        (any::<usize>(), hostile).prop_map(|(p, v)| Mutation::Word(p, v)),
        any::<usize>().prop_map(Mutation::Truncate),
    ]
}

fn mutate(bytes: &mut Vec<u8>, mutations: &[Mutation]) {
    for m in mutations {
        if bytes.is_empty() {
            break;
        }
        match *m {
            Mutation::Byte(p, v) => {
                let p = p % bytes.len();
                bytes[p] = v;
            }
            Mutation::Word(p, v) => {
                let p = (p % bytes.len()) & !3;
                let n = (bytes.len() - p).min(4);
                bytes[p..p + n].copy_from_slice(&v.to_le_bytes()[..n]);
            }
            Mutation::Truncate(len) => bytes.truncate(len % (bytes.len() + 1)),
        }
    }
}

equivalence! {
    equivalent_u8: u8 => u8,
    equivalent_u64: u64 => u64,
    equivalent_f64: f64 => f64,
    equivalent_bool: bool => bool,
    equivalent_uid: Uid => Uid,
    equivalent_bytes: Bytes => Bytes,
    equivalent_borrowed_bytes: &'static [u8] => &[u8],
    equivalent_vec_u16: Vec<u16> => Vec<u16>,
    equivalent_struct: (i16, bool, i64) => (i16, bool, i64),
    equivalent_inner: Inner => Inner,
    equivalent_small: Small => Small,
    equivalent_nested2: Nested2 => Nested2,
    equivalent_nested: Nested => Nested,
    equivalent_root: Root => Root,
    equivalent_scalars: Scalars => Scalars,
    equivalent_strings: Strings<'static> => Strings<'_>,
    equivalent_vectors: Vectors => Vectors,
    equivalent_containers: Containers => Containers,
    equivalent_map_containers: MapContainers => MapContainers,
    equivalent_structs: Structs => Structs,
    equivalent_optionals: Optionals => Optionals,
    equivalent_variant_holder: VariantHolder => VariantHolder,
    equivalent_payload: Payload => Payload,
    equivalent_ensure_tables: EnsureTables => EnsureTables,
    equivalent_request_like: RequestLike<'static> => RequestLike<'_>,
    equivalent_order_demo: OrderDemo => OrderDemo,
    equivalent_arena_holder: ArenaHolder<'static> => ArenaHolder<'_>,
    equivalent_kv_holder: KvHolder => KvHolder,
    equivalent_dedup_holder: DedupHolder => DedupHolder,
    equivalent_schema_v2: SchemaV2 => SchemaV2,
    equivalent_x_y2: XY2 => XY2,
    equivalent_untrusted: Untrusted => Untrusted,
    equivalent_fixed_bytes: FixedBytes => FixedBytes,
    equivalent_wire_bytes: WireBytes => WireBytes,
}

/// Fixtures and arbitrary bytes, read as vectors of various elements: never a panic, and
/// the same equivalence.
#[test]
fn fixtures_and_noise_read_the_same_lazily() {
    let positives: Vec<Vec<u8>> = records("positive").iter().map(record_bytes).collect();
    let strategy = (
        0..positives.len(),
        prop::collection::vec(mutation(), 0..4),
        any::<bool>(),
        prop::collection::vec(any::<u8>(), 8..96),
    );
    runner(4000)
        .run(&strategy, |(i, mutations, noise, random)| {
            let mut bytes = if noise { random } else { positives[i].clone() };
            mutate(&mut bytes, &mutations);
            if bytes.len() < 8 {
                return Ok(());
            }
            if noise {
                let root = 8 + (u32::from(bytes[1]) % 8) * 4;
                bytes[..4].copy_from_slice(&root.to_le_bytes());
            }
            check_equivalent::<u8>(&bytes, false)?;
            check_equivalent::<u32>(&bytes, false)?;
            check_equivalent::<bool>(&bytes, false)?;
            check_equivalent::<(i16, bool, i64)>(&bytes, false)?;
            check_equivalent::<&[u8]>(&bytes, false)?;
            check_equivalent::<Vec<u16>>(&bytes, false)?;
            check_equivalent::<Inner>(&bytes, false)?;
            check_equivalent::<Nested2>(&bytes, false)?;
            check_equivalent::<Untrusted>(&bytes, false)?;
            check_equivalent::<Uid>(&bytes, false)
        })
        .unwrap();
}

// ---------------------------------------------------------------------------------------
// A view as a table field.

/// `Vectors` with every vector a `VectorView` (slot for slot).
#[derive(Debug)]
struct LazyVectors<'de> {
    v_u8: VectorView<'de, u8>,
    v_u16: VectorView<'de, u16>,
    v_i32: VectorView<'de, i32>,
    v_u64: VectorView<'de, u64>,
    v_f64: VectorView<'de, f64>,
    v_str: VectorView<'de, Bytes>,
    v_table: VectorView<'de, Small>,
    v_empty_u32: VectorView<'de, u32>,
    v_empty_str: VectorView<'de, Bytes>,
    v_empty_table: VectorView<'de, Small>,
    v_bool: VectorView<'de, bool>,
    fixed16: [u8; 16],
}

impl Wire for LazyVectors<'_> {
    const SHAPE: Shape = Shape::Table;
}

impl<'de> Decode<'de> for LazyVectors<'de> {
    fn absent() -> Self {
        LazyVectors {
            v_u8: Decode::absent(),
            v_u16: Decode::absent(),
            v_i32: Decode::absent(),
            v_u64: Decode::absent(),
            v_f64: Decode::absent(),
            v_str: Decode::absent(),
            v_table: Decode::absent(),
            v_empty_u32: Decode::absent(),
            v_empty_str: Decode::absent(),
            v_empty_table: Decode::absent(),
            v_bool: Decode::absent(),
            fixed16: Decode::absent(),
        }
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(LazyVectors {
            v_u8: t.field()?,
            v_u16: t.field()?,
            v_i32: t.field()?,
            v_u64: t.field()?,
            v_f64: t.field()?,
            v_str: t.field()?,
            v_table: t.field()?,
            v_empty_u32: t.field()?,
            v_empty_str: t.field()?,
            v_empty_table: t.field()?,
            v_bool: t.field()?,
            fixed16: t.field()?,
        })
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn view_fields_in_a_table_read_like_vec_fields(v in Vectors::arb()) {
        let bytes = encode(&v).map_err(fail)?;
        let lazy: LazyVectors = Reader::new(&bytes).and_then(|r| r.decode_root()).map_err(fail)?;
        prop_assert_eq!(all(&lazy.v_u8).map_err(fail)?, v.v_u8);
        prop_assert_eq!(all(&lazy.v_u16).map_err(fail)?, v.v_u16);
        prop_assert_eq!(all(&lazy.v_i32).map_err(fail)?, v.v_i32);
        prop_assert_eq!(all(&lazy.v_u64).map_err(fail)?, v.v_u64);
        prop_assert_eq!(all(&lazy.v_f64).map_err(fail)?, v.v_f64);
        prop_assert_eq!(all(&lazy.v_str).map_err(fail)?, v.v_str);
        prop_assert_eq!(all(&lazy.v_table).map_err(fail)?, v.v_table);
        prop_assert_eq!(all(&lazy.v_empty_u32).map_err(fail)?, v.v_empty_u32);
        prop_assert_eq!(all(&lazy.v_empty_str).map_err(fail)?, v.v_empty_str);
        prop_assert_eq!(all(&lazy.v_empty_table).map_err(fail)?, v.v_empty_table);
        prop_assert_eq!(all(&lazy.v_bool).map_err(fail)?, v.v_bool);
        prop_assert_eq!(lazy.fixed16, v.fixed16);
    }
}

#[test]
fn absent_and_empty_views() {
    // Every field of an old writer's table that a newer reader has as a view is absent.
    let bytes = encode(&SchemaV1 {
        a: 1,
        b: Bytes::default(),
    })
    .unwrap();
    struct Newer<'de>(VectorView<'de, u32>);
    impl Wire for Newer<'_> {
        const SHAPE: Shape = Shape::Table;
    }
    impl<'de> Decode<'de> for Newer<'de> {
        fn absent() -> Self {
            Newer(Decode::absent())
        }
        fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
            let mut t = r.table(at)?;
            t.skip(2);
            Ok(Newer(t.field()?))
        }
    }
    let newer: Newer = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(newer.0.len(), 0);
    assert!(newer.0.is_empty());
    assert!(newer.0.iter().next().is_none());
    assert_eq!(newer.0.iter().size_hint(), (0, Some(0)));
    assert_eq!(
        newer.0.get(0),
        Err(DecodeError::IndexOutOfRange { index: 0, len: 0 })
    );
    assert_eq!(format!("{:?}", newer.0), "VectorView { len: 0, .. }");

    let bytes = encode_with_identifier(&Vec::<u32>::new(), 1).unwrap();
    let view: VectorView<u32> = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert!(view.is_empty());
    assert_eq!(all(&view).unwrap(), Vec::<u32>::new());
}

#[test]
fn views_are_send_sync_and_outlive_their_reader() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<VectorView<'static, Small>>();
    assert_send_sync::<VectorView<'static, VectorView<'static, u8>>>();

    let items = vec![
        Small {
            x: 1,
            name: Bytes(b"one".to_vec()),
        },
        Small {
            x: 2,
            name: Bytes(b"two".to_vec()),
        },
    ];
    let bytes = encode_with_identifier(&items, 1).unwrap();
    let view = {
        let reader = Reader::new(&bytes).unwrap();
        reader.decode_root::<VectorView<Small>>().unwrap()
        // `reader` is dropped; the view keeps what it needs.
    };
    let moved = std::thread::scope(|s| s.spawn(|| all(&view)).join().unwrap());
    assert_eq!(moved.unwrap(), items);
}

// ---------------------------------------------------------------------------------------
// The low-level accessors.

/// Captures a vector's header, for tests that need element positions.
#[derive(Debug)]
struct Header(VectorHeader);

impl Wire for Header {
    const SHAPE: Shape = Shape::Object;
}

impl<'de> Decode<'de> for Header {
    fn absent() -> Self {
        panic!("a root member is present")
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        r.vector_header::<&[u8]>(at).map(Header)
    }
}

#[test]
fn vector_header_gives_checked_positions() {
    let items: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
    let bytes = encode_with_identifier(&items, 1).unwrap();
    let Header(h) = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!((h.len(), h.is_empty(), h.stride()), (3, false, 4));
    assert_eq!(h.position(0), Some(h.first()));
    assert_eq!(h.position(2), Some(h.first() + 8));
    assert_eq!(h.position(3), None);
    assert_eq!(h.position(usize::MAX), None);
    // The positions are what decode_element wants.
    let r = Reader::new(&bytes).unwrap();
    let got: Vec<&[u8]> = (0..h.len())
        .map(|i| <&[u8]>::decode_element(&r, h.position(i).unwrap()).unwrap())
        .collect();
    assert_eq!(got, items);

    // An element type with no size is refused, as is a union.
    #[derive(Debug)]
    struct Nothing;
    impl Wire for Nothing {
        const SHAPE: Shape = Shape::Inline { size: 0, align: 1 };
    }
    impl<'de> Decode<'de> for Nothing {
        fn absent() -> Self {
            Nothing
        }
    }
    let r = Reader::new(&bytes).unwrap();
    let at = first_field_position(&r);
    assert_eq!(
        r.vector_header::<Nothing>(at),
        Err(DecodeError::Invalid("zero-size vector element"))
    );
    assert!(matches!(
        r.vector_header::<Option<u8>>(at),
        Err(DecodeError::Invalid(_))
    ));
}

/// The position of the root table's first field (the offset to the root member).
fn first_field_position(r: &Reader<'_>) -> usize {
    // Root table at the root offset; its vtable's first entry gives the field offset.
    let bytes = r.bytes();
    let table = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let soffset = i32::from_le_bytes(bytes[table..table + 4].try_into().unwrap());
    let vtable = (table as i64 - i64::from(soffset)) as usize;
    let entry = u16::from_le_bytes(bytes[vtable + 4..vtable + 6].try_into().unwrap()) as usize;
    table + entry
}

#[derive(Debug, PartialEq)]
struct Span(Range<usize>);

impl Wire for Span {
    const SHAPE: Shape = Shape::Object;
}

impl<'de> Decode<'de> for Span {
    fn absent() -> Self {
        Span(0..0)
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        r.blob_range(at).map(Span)
    }
}

#[test]
fn blob_range_matches_blob() {
    // Well-formed and mutated: the range is exactly the borrowed slice, and the two accept
    // and reject the same messages with the same errors.
    let strategy = (Bytes::arb(), prop::collection::vec(mutation(), 0..4));
    runner(5000)
        .run(&strategy, |(blob, mutations)| {
            let mut bytes = encode_with_identifier(&blob, 1).map_err(fail)?;
            mutate(&mut bytes, &mutations);
            if bytes.len() < 8 {
                return Ok(());
            }
            let borrowed = Reader::new(&bytes).map_err(fail)?.decode_root::<&[u8]>();
            let span = Reader::new(&bytes).map_err(fail)?.decode_root::<Span>();
            match (borrowed, span) {
                (Ok(b), Ok(Span(range))) => {
                    prop_assert!(range.end <= bytes.len());
                    prop_assert_eq!(&bytes[range], b);
                }
                (Err(a), Err(b)) => prop_assert_eq!(a, b),
                (a, b) => return Err(fail(format!("blob {a:?} but blob_range {b:?}"))),
            }
            Ok(())
        })
        .unwrap();
}

// ---------------------------------------------------------------------------------------
// Hostile input.

/// A root whose only field is `v`.
fn root_of(w: &mut Writer, v: ObjectRef) -> Vec<u8> {
    let mut root = w.table();
    root.field(&Alias(v)).unwrap();
    let root = root.finish().unwrap();
    w.finish(root, 1).unwrap()
}

#[test]
fn huge_counts_are_rejected_when_the_view_is_decoded() {
    let items: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
    let bytes = encode_with_identifier(&items, 1).unwrap();
    let Header(h) = Reader::new(&bytes).unwrap().decode_root().unwrap();
    let count_at = h.first() - 4;
    for count in [
        4u32,
        1 << 20,
        0x4000_0001,
        0x8000_0003,
        0xffff_fff0,
        u32::MAX,
    ] {
        let mut bad = bytes.clone();
        bad[count_at..count_at + 4].copy_from_slice(&count.to_le_bytes());
        let r = Reader::new(&bad).unwrap();
        let lazy = r.decode_root::<VectorView<&[u8]>>();
        let eager = r.decode_root::<Vec<&[u8]>>();
        // 4 elements need 16 bytes: the message has them (it runs on into the strings), so
        // that one is a well-formed header; what matters is that both agree and a view
        // never claims more elements than the buffer holds.
        match (lazy, eager) {
            (Ok(view), Ok(items)) => assert_eq!(view.len(), items.len()),
            (Ok(view), Err(_)) => {
                assert!(view.len() <= bad.len() / 4);
                assert!(all(&view).is_err());
            }
            (Err(a), Err(b)) => assert_eq!(a, b, "count {count:#x}"),
            (Err(e), Ok(_)) => panic!("count {count:#x}: view failed ({e}) but Vec decoded"),
        }
    }
    let mut bad = bytes.clone();
    bad[count_at..count_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        Reader::new(&bad)
            .unwrap()
            .decode_root::<VectorView<&[u8]>>(),
        Err(DecodeError::OutOfBounds { .. })
    ));
}

/// A 4 KiB table type: a view of a million of these must not reserve anything for them.
#[derive(Debug)]
#[allow(dead_code)]
struct Big([Uid; 256]);

impl Wire for Big {
    const SHAPE: Shape = Shape::Table;
}

impl<'de> Decode<'de> for Big {
    fn absent() -> Self {
        Big([Uid::default(); 256])
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        let mut out = [Uid::default(); 256];
        for u in &mut out {
            *u = t.field()?;
        }
        Ok(Big(out))
    }
}

#[test]
fn a_large_count_allocates_nothing_and_fails_at_the_first_bad_element() {
    // A million zero offsets: the header is in bounds, every element is invalid.
    let mut w = Writer::new();
    let v = w.vector(&vec![0u32; 1 << 20]).unwrap();
    let bytes = root_of(&mut w, v);
    let r = Reader::new(&bytes).unwrap();
    assert!(r.decode_root::<Vec<Big>>().is_err());

    let view: VectorView<Big> = r.decode_root().unwrap();
    assert_eq!(view.len(), 1 << 20);
    assert_eq!(
        view.get(0).err(),
        Some(DecodeError::Strict(StrictCheck::ZeroUoffset))
    );
    assert_eq!(
        view.get(999_999).err(),
        Some(DecodeError::Strict(StrictCheck::ZeroUoffset))
    );
    // The size hint, which `collect` reserves from, is capped at 64 KiB of results.
    let (lower, upper) = view.iter().size_hint();
    assert_eq!(upper, Some(1 << 20));
    assert!(
        lower * size_of::<Result<Big, DecodeError>>() <= 64 * 1024,
        "{lower}"
    );
    // One error, then the iterator is done.
    let results: Vec<Result<Big, DecodeError>> = view.iter().collect();
    assert_eq!(results.len(), 1);
    assert!(results[0].is_err());
    assert!(all(&view).is_err());
    let mut it = view.iter();
    assert!(it.next().is_some());
    assert!(it.next().is_none());
    assert!(it.next().is_none());
    assert_eq!(it.size_hint(), (0, Some(0)));
}

#[test]
fn elements_are_checked_when_read_not_before() {
    let items: Vec<&[u8]> = vec![b"first", b"second", b"third"];
    let bytes = encode_with_identifier(&items, 1).unwrap();
    let Header(h) = Reader::new(&bytes).unwrap().decode_root().unwrap();
    let pos = |i| h.position(i).unwrap();
    let offset_at = |bytes: &[u8], i| {
        u32::from_le_bytes(bytes[pos(i)..pos(i) + 4].try_into().unwrap()) as usize
    };

    // Each of these breaks only element 1.
    let mut break_offset_far = bytes.clone();
    break_offset_far[pos(1)..pos(1) + 4].copy_from_slice(&0x7fff_fff0u32.to_le_bytes());
    let mut break_offset_zero = bytes.clone();
    break_offset_zero[pos(1)..pos(1) + 4].copy_from_slice(&0u32.to_le_bytes());
    let mut break_offset_max = bytes.clone();
    break_offset_max[pos(1)..pos(1) + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut break_length = bytes.clone();
    let target = pos(1) + offset_at(&bytes, 1);
    break_length[target..target + 4].copy_from_slice(&0xffff_fff0u32.to_le_bytes());
    let mut break_end = bytes.clone();
    // An offset to the last 2 bytes: a length that does not fit.
    let to_end = (break_end.len() - 2 - pos(1)) as u32;
    break_end[pos(1)..pos(1) + 4].copy_from_slice(&to_end.to_le_bytes());

    for (name, bad) in [
        ("far offset", break_offset_far),
        ("zero offset", break_offset_zero),
        ("max offset", break_offset_max),
        ("huge length", break_length),
        ("offset to the end", break_end),
    ] {
        let r = Reader::new(&bad).unwrap();
        // The view decodes: only the header is checked.
        let view: VectorView<&[u8]> = r.decode_root().unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(view.len(), 3, "{name}");
        // Eager decoding of the same bytes fails.
        assert!(r.decode_root::<Vec<&[u8]>>().is_err(), "{name}");
        // Reading the intact elements works, in any order, and the broken one is an error
        // every time, never a panic.
        assert_eq!(view.get(2).unwrap(), b"third", "{name}");
        assert_eq!(view.get(0).unwrap(), b"first", "{name}");
        assert!(view.get(1).is_err(), "{name}");
        assert!(view.get(1).is_err(), "{name}");
        assert_eq!(view.get(2).unwrap(), b"third", "{name}");
        // The iterator yields the intact prefix, the error, and ends.
        let seen: Vec<_> = view.iter().collect();
        assert_eq!(seen.len(), 2, "{name}");
        assert_eq!(*seen[0].as_ref().unwrap(), b"first", "{name}");
        assert!(seen[1].is_err(), "{name}");
    }
}

#[test]
fn aliasing_is_charged_per_read_like_eager_decoding() {
    // 2000 offsets to one 4000-byte string: 8 MB of strings from 12 KB of message.
    let mut w = Writer::new();
    let blob = w.blob(&[b'x'; 4000]).unwrap();
    let v = w.vector(&vec![Alias(blob); 2000]).unwrap();
    let bytes = root_of(&mut w, v);
    let budget = 4 * bytes.len() + 1024;
    let r = Reader::new(&bytes).unwrap();
    assert_eq!(r.decode_root::<Vec<&[u8]>>(), Err(DecodeError::WorkLimit));

    let view: VectorView<&[u8]> = r.decode_root().unwrap();
    assert_eq!(view.len(), 2000);
    let mut read = 0;
    let mut end = None;
    for item in &view {
        match item {
            Ok(s) => {
                assert_eq!(s.len(), 4000);
                read += 1;
            }
            Err(e) => end = Some(e),
        }
    }
    assert_eq!(end, Some(DecodeError::WorkLimit));
    // The root table (4), the header (4 + 2000 * 4) and the strings read (4 + 4000 each)
    // fit the budget; one more string would not.
    assert!(read > 0);
    let used = 4 + 8004 + read * 4004;
    assert!(
        used <= budget && used + 4004 > budget,
        "{read} reads, budget {budget}"
    );

    // Reading one element over and over spends the same budget.
    let r = Reader::new(&bytes).unwrap();
    let view: VectorView<&[u8]> = r.decode_root().unwrap();
    let mut gets = 0;
    while view.get(0).is_ok() {
        gets += 1;
        assert!(gets < 1000, "re-reading an element never stops");
    }
    assert_eq!(view.get(1), Err(DecodeError::WorkLimit));
    // A new decode from the root refills the budget for this reader's views.
    drop(r.root().unwrap());
    assert!(view.get(0).is_ok());
}

#[test]
fn nested_aliasing_bomb_stops_in_a_view_too() {
    // 2000 offsets to one vector of 2000 offsets to the empty blob: 4 million strings.
    let mut w = Writer::new();
    let blob = w.blob(b"").unwrap();
    let inner = w.vector(&vec![Alias(blob); 2000]).unwrap();
    let outer = w.vector(&vec![Alias(inner); 2000]).unwrap();
    let bytes = root_of(&mut w, outer);
    let budget = 4 * bytes.len() + 1024;
    let r = Reader::new(&bytes).unwrap();
    assert_eq!(
        r.decode_root::<Vec<Vec<&[u8]>>>(),
        Err(DecodeError::WorkLimit)
    );

    let outer: VectorView<VectorView<&[u8]>> = r.decode_root().unwrap();
    let mut strings = 0usize;
    let mut result = Ok(());
    'walk: for inner in &outer {
        let inner = match inner {
            Ok(inner) => inner,
            Err(e) => {
                result = Err(e);
                break;
            }
        };
        for s in &inner {
            match s {
                Ok(_) => strings += 1,
                Err(e) => {
                    result = Err(e);
                    break 'walk;
                }
            }
        }
    }
    assert_eq!(result, Err(DecodeError::WorkLimit));
    // Every string costs 4 units and an inner view 4 + 8000: far fewer than 4 million.
    assert!(
        strings * 4 <= budget,
        "{strings} strings for a budget of {budget}"
    );
}

#[test]
fn scalar_elements_can_be_reread_freely() {
    let bytes = encode_with_identifier(&vec![7u64; 16], 1).unwrap();
    let r = Reader::new(&bytes).unwrap();
    let view: VectorView<u64> = r.decode_root().unwrap();
    for i in 0..200_000 {
        assert_eq!(view.get(i % 16).unwrap(), 7);
    }
}

#[test]
fn work_is_charged_to_the_budget_of_the_decode_that_made_the_view() {
    // The part of the message that is not lazy and the part that is draw on one budget:
    // `aliased_untrusted` is rejected as a whole by eager decoding (work_limit), and the
    // lazy `strs` field decodes but stops in the same place.
    let (_, bytes) = aliased_untrusted();
    let r = Reader::new(&bytes).unwrap();
    assert_eq!(r.decode_root::<Untrusted>(), Err(DecodeError::WorkLimit));

    struct LazyStrs<'de>(VectorView<'de, &'de [u8]>);
    impl Wire for LazyStrs<'_> {
        const SHAPE: Shape = Shape::Table;
    }
    impl<'de> Decode<'de> for LazyStrs<'de> {
        fn absent() -> Self {
            LazyStrs(Decode::absent())
        }
        fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
            let mut t = r.table(at)?;
            // s, v, o (two slots), vo (two slots), b
            t.skip(7);
            Ok(LazyStrs(t.field()?))
        }
    }
    let lazy: LazyStrs = r.decode_root().unwrap();
    assert_eq!(lazy.0.len(), 20);
    let read = lazy.0.iter().take_while(Result::is_ok).count();
    assert!((1..20).contains(&read), "{read}");
    assert_eq!(all(&lazy.0), Err(DecodeError::WorkLimit));
}

// ---------------------------------------------------------------------------------------
// Nesting depth.

#[derive(Debug)]
struct Node {
    kids: Vec<Node>,
}

impl Wire for Node {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for Node {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.kids)?;
        t.finish()
    }
}

impl<'de> Decode<'de> for Node {
    fn absent() -> Self {
        Node { kids: vec![] }
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        Ok(Node {
            kids: r.table(at)?.field()?,
        })
    }
}

struct LazyNode<'de> {
    kids: VectorView<'de, LazyNode<'de>>,
}

impl Wire for LazyNode<'_> {
    const SHAPE: Shape = Shape::Table;
}

impl<'de> Decode<'de> for LazyNode<'de> {
    fn absent() -> Self {
        LazyNode {
            kids: Decode::absent(),
        }
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        Ok(LazyNode {
            kids: r.table(at)?.field()?,
        })
    }
}

fn walk(node: &LazyNode<'_>) -> Result<usize, DecodeError> {
    let mut depth = 1;
    for kid in &node.kids {
        depth = depth.max(1 + walk(&kid?)?);
    }
    Ok(depth)
}

#[test]
fn lazy_elements_keep_the_nesting_limit() {
    // A chain of `levels` tables below the root table. Eager decoding accepts it up to
    // MAX_DEPTH; the view, which decodes the chain one level at a time long after the
    // enclosing tables were left, must stop at the same level.
    let mut boundary = None;
    for levels in 1..=70 {
        let mut node = Node { kids: vec![] };
        for _ in 1..levels {
            node = Node { kids: vec![node] };
        }
        let bytes = encode_with_identifier(&node, 1).unwrap();
        let eager = Reader::new(&bytes).unwrap().decode_root::<Node>();
        let lazy = Reader::new(&bytes)
            .unwrap()
            .decode_root::<LazyNode>()
            .and_then(|n| walk(&n));
        match (&eager, &lazy) {
            (Ok(_), Ok(depth)) => assert_eq!(*depth, levels),
            (Err(a), Err(b)) => {
                assert_eq!(a, b, "{levels} levels");
                assert_eq!(*b, DecodeError::Strict(StrictCheck::TooDeep));
                boundary.get_or_insert(levels);
            }
            _ => panic!("{levels} levels: eager {eager:?}, lazy {lazy:?}"),
        }
    }
    assert!(boundary.is_some(), "the chain never got too deep");
}
