//! The bulk paths of inline vectors: `Encode::encode_inline_slice` (what `Writer::vector`
//! writes a `Vec<u8>` or `Vec<u32>` with) and `Decode::decode_elements` (what a `Vec<T>`
//! of scalars is read with), against the element-by-element code they replace.
//!
//! The element-wise reference for decoding is `VectorView<T>`, which reads through
//! `Decode::decode_element` and charges the work budget as the elements are read; for
//! encoding it is a newtype whose impl leaves `encode_inline_slice` at its default. The
//! properties: the same values (NaNs included, bit for bit), the same bytes, the same
//! errors on hostile headers, and the same work-budget charge.

use std::fmt::Debug;

use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, ObjectRef, Reader, Shape, StrictCheck, VectorHeader,
    VectorView, Wire, Writer, encode_with_identifier,
};
use proptest::prelude::*;

/// A scalar whose `Encode` and `Decode` impls forward to `T`'s inline methods and leave
/// the bulk hooks at their defaults: the element-wise code.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Slow<T>(T);

impl<T: Wire> Wire for Slow<T> {
    const SHAPE: Shape = T::SHAPE;
}

impl<T: Encode> Encode for Slow<T> {
    fn encode_inline(&self, out: &mut [u8]) -> Result<(), EncodeError> {
        self.0.encode_inline(out)
    }
}

impl<'de, T: Decode<'de>> Decode<'de> for Slow<T> {
    fn absent() -> Self {
        Slow(T::absent())
    }

    fn decode_inline(bytes: &'de [u8]) -> Result<Self, DecodeError> {
        T::decode_inline(bytes).map(Slow)
    }

    fn decode_element(r: &Reader<'de>, pos: usize) -> Result<Self, DecodeError> {
        T::decode_element(r, pos).map(Slow)
    }
}

/// Points at an object already written.
#[derive(Clone, Copy)]
struct Alias(ObjectRef);

impl Wire for Alias {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for Alias {
    fn encode_object(&self, _: &mut Writer) -> Result<ObjectRef, EncodeError> {
        Ok(self.0)
    }
}

/// A value's identity for comparisons, which tells `NaN`s apart from each other.
fn same(value: &impl Debug) -> String {
    format!("{value:?}")
}

fn encode_vec<T: Encode>(items: &[T]) -> Vec<u8> {
    encode_with_identifier(&items, 1).unwrap()
}

/// `T`'s vectors written by the bulk hook and by the default element-wise one are the
/// same bytes, and decode (bulk, view and element-wise) to the same values.
fn check_vector<T>(items: &[T])
where
    T: Encode + for<'de> Decode<'de> + Copy + Debug,
{
    let slow: Vec<Slow<T>> = items.iter().copied().map(Slow).collect();
    let bytes = encode_vec(items);
    assert_eq!(bytes, encode_vec(&slow), "encoded bytes");

    let reader = Reader::new(&bytes).unwrap();
    let eager: Vec<T> = reader.decode_root().unwrap();
    assert_eq!(same(&eager), same(&items));
    let reader = Reader::new(&bytes).unwrap();
    let element_wise: Vec<Slow<T>> = reader.decode_root().unwrap();
    assert_eq!(
        same(&element_wise.iter().map(|s| s.0).collect::<Vec<_>>()),
        same(&eager)
    );
    let reader = Reader::new(&bytes).unwrap();
    let view: VectorView<T> = reader.decode_root().unwrap();
    assert_eq!(
        same(&view.iter().collect::<Result<Vec<_>, _>>().unwrap()),
        same(&eager)
    );
}

macro_rules! scalar_vectors {
    ($($name:ident: $t:ty = $strategy:expr),* $(,)?) => {$(
        proptest! {
            #[test]
            fn $name(items in proptest::collection::vec($strategy, 0..300)) {
                check_vector::<$t>(&items);
            }
        }
    )*};
}

scalar_vectors! {
    vec_u8: u8 = any::<u8>(),
    vec_i8: i8 = any::<i8>(),
    vec_bool: bool = any::<bool>(),
    vec_u16: u16 = any::<u16>(),
    vec_i16: i16 = any::<i16>(),
    vec_u32: u32 = any::<u32>(),
    vec_i32: i32 = any::<i32>(),
    vec_u64: u64 = any::<u64>(),
    vec_i64: i64 = any::<i64>(),
    // Every bit pattern, signalling and quiet NaNs included.
    vec_f32: f32 = any::<u32>().prop_map(f32::from_bits),
    vec_f64: f64 = any::<u64>().prop_map(f64::from_bits),
}

#[test]
fn edge_lengths_round_trip() {
    for n in [
        0usize, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 255, 256, 65_535, 65_537,
    ] {
        let bytes: Vec<u8> = (0..n).map(|i| (i * 31 + 7) as u8).collect();
        check_vector(&bytes);
        let words: Vec<u32> = (0..n as u32)
            .map(|i| i.wrapping_mul(2_654_435_761))
            .collect();
        check_vector(&words);
    }
}

#[test]
fn struct_vectors_use_the_default_hook() {
    // A tuple has no bulk impl: it goes through `Encode::encode_inline_slice`'s default.
    let items: Vec<(u8, u32)> = (0..50u32).map(|i| (i as u8, i * 3)).collect();
    let bytes = encode_vec(&items);
    let reader = Reader::new(&bytes).unwrap();
    let back: Vec<(u8, u32)> = reader.decode_root().unwrap();
    assert_eq!(back, items);
}

/// Any nonzero byte of a `vector<bool>` is `true`, whichever way it is read; a scalar
/// `bool` field must be 0 or 1.
#[test]
fn bool_vectors_read_nonzero_bytes_as_true() {
    let raw: Vec<u8> = (0..=255u8).collect();
    let bytes = encode_vec(&raw);
    let reader = Reader::new(&bytes).unwrap();
    let eager: Vec<bool> = reader.decode_root().unwrap();
    assert_eq!(eager, raw.iter().map(|&b| b != 0).collect::<Vec<_>>());
    let reader = Reader::new(&bytes).unwrap();
    let view: VectorView<bool> = reader.decode_root().unwrap();
    assert_eq!(view.iter().collect::<Result<Vec<_>, _>>().unwrap(), eager);
}

#[test]
fn slice_hooks_reject_a_wrong_output_size() {
    fn rejects<T: Encode>(items: &[T]) {
        let size = match T::SHAPE {
            Shape::Inline { size, .. } => size,
            _ => unreachable!(),
        };
        for wrong in [0, 1, size * items.len() + 1, size * items.len() - 1] {
            if wrong == size * items.len() {
                continue;
            }
            let mut out = vec![0u8; wrong];
            assert_eq!(
                T::encode_inline_slice(items, &mut out),
                Err(EncodeError::InvalidShape("inline vector size")),
                "{} bytes for {} items",
                wrong,
                items.len()
            );
        }
        let mut out = vec![0u8; size * items.len()];
        T::encode_inline_slice(items, &mut out).unwrap();
    }
    rejects(&[1u8, 2, 3]);
    rejects(&[1i8, 2, 3]);
    rejects(&[true, false, true]);
    rejects(&[1u16, 2, 3]);
    rejects(&[1u32, 2, 3]);
    rejects(&[1u64, 2, 3]);
    rejects(&[1f32, 2.0, 3.0]);
    rejects(&[(1u8, 2u32), (3, 4)]);
}

// ---------------------------------------------------------------------------------------
// Hostile headers.

/// A root holding one vector of `items` (written with the unique `marker` pattern at its
/// start), returning the message and the position of the vector's count.
fn message_with_vector<T: Encode>(items: &[T]) -> (Vec<u8>, usize) {
    let bytes = encode_vec(items);
    let count = (items.len() as u32).to_le_bytes();
    // The count is the only place the count is followed by the first element's bytes;
    // find the last occurrence to skip the root table's own bytes.
    let at = bytes
        .windows(4)
        .rposition(|w| w == count)
        .expect("vector count in message");
    (bytes, at)
}

fn decode_all(bytes: &[u8]) -> Vec<Result<(), DecodeError>> {
    fn one<T: for<'de> Decode<'de>>(bytes: &[u8]) -> Result<(), DecodeError> {
        Reader::new(bytes)?.decode_root::<Vec<T>>().map(|_| ())
    }
    vec![
        one::<u8>(bytes),
        one::<i8>(bytes),
        one::<bool>(bytes),
        one::<u16>(bytes),
        one::<u32>(bytes),
        one::<u64>(bytes),
        one::<f32>(bytes),
        one::<f64>(bytes),
        one::<Slow<u32>>(bytes),
    ]
}

#[test]
fn hostile_counts_fail_in_the_header_check() {
    let items: Vec<u8> = (0..4096).map(|i| i as u8).collect();
    let (bytes, at) = message_with_vector(&items);
    // A count past the end of the message, for every element size.
    for count in [4097u32, 5000, 1 << 20, 0x4000_0000, 0x8000_0000, u32::MAX] {
        let mut bad = bytes.clone();
        bad[at..at + 4].copy_from_slice(&count.to_le_bytes());
        for (i, result) in decode_all(&bad).into_iter().enumerate() {
            // A byte-sized vector (`u8`, `i8`, `bool`) of a few more bytes than the 4096 written still fits in the
            // message (the bytes after the vector are the root table's, which it then
            // reads); every wider type's claim is past the end.
            if i <= 2 && usize::try_from(count).unwrap() + at + 4 <= bad.len() {
                continue;
            }
            assert!(
                matches!(result, Err(DecodeError::OutOfBounds { .. })),
                "count {count:#x}, type #{i}: {result:?}"
            );
        }
        // The lazy path rejects the same header.
        let reader = Reader::new(&bad).unwrap();
        let view: Result<VectorView<u32>, _> = reader.decode_root();
        assert!(matches!(view, Err(DecodeError::OutOfBounds { .. })));
    }
}

/// The same hostile counts, in a child process whose address space is capped: a decode
/// that reserved memory for the claimed count (4 GiB of `u8`, 32 GiB of `f64`) would abort.
#[cfg(target_os = "linux")]
mod capped {
    use super::*;

    const CHILD: &str = "FDB_WIRE_BULK_CHILD";

    fn child() {
        // A real 16 MiB vector decodes under the cap: the copy is the size of the data.
        let items: Vec<u8> = (0..16 << 20).map(|i| i as u8).collect();
        let (bytes, at) = message_with_vector(&items);
        let reader = Reader::new(&bytes).unwrap();
        let back: Vec<u8> = reader.decode_root().unwrap();
        assert_eq!(back.len(), items.len());
        assert!(back == items);
        // And a count of 4 billion does not reserve anything.
        for count in [0x4000_0000u32, 0xffff_fff0, u32::MAX] {
            let mut bad = bytes.clone();
            bad[at..at + 4].copy_from_slice(&count.to_le_bytes());
            for (i, result) in decode_all(&bad).into_iter().enumerate() {
                assert!(
                    matches!(result, Err(DecodeError::OutOfBounds { .. })),
                    "count {count:#x}, type #{i}: {result:?}"
                );
            }
        }
    }

    #[test]
    fn hostile_counts_do_not_reserve_memory() {
        if std::env::var_os(CHILD).is_some() {
            child();
            return;
        }
        let exe = std::env::current_exe().unwrap();
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg(
                r#"ulimit -v 1048576 && exec "$0" --exact capped::hostile_counts_do_not_reserve_memory --test-threads=1"#,
            )
            .arg(exe)
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(
            status.success(),
            "decoding under a 1 GiB address-space limit failed: {status}"
        );
    }
}

// ---------------------------------------------------------------------------------------
// The work budget.

/// How many times a root table whose fields all alias one vector decodes before the work
/// limit stops it, and how it ends.
fn decodes_until_the_limit<T: for<'de> Decode<'de>>(bytes: &[u8]) -> (usize, DecodeError) {
    let reader = Reader::new(bytes).unwrap();
    let mut root = reader.root().unwrap();
    for done in 0.. {
        if let Err(e) = root.field::<Vec<T>>() {
            return (done, e);
        }
    }
    unreachable!()
}

fn aliased(items: &[impl Encode], fields: usize) -> Vec<u8> {
    let mut w = Writer::new();
    let v = w.vector(items).unwrap();
    let mut root = w.table();
    for _ in 0..fields {
        root.field(&Alias(v)).unwrap();
    }
    let root = root.finish().unwrap();
    w.finish(root, 1).unwrap()
}

/// The bulk impls charge exactly what the element-wise ones do (4 + count × size, once per
/// decode): a message that aliases one vector from many fields stops at the same field.
#[test]
fn bulk_decodes_charge_what_element_wise_decodes_charge() {
    fn same_budget<T: Encode + for<'de> Decode<'de> + Copy>(item: T) {
        let items = vec![item; 700];
        let bytes = aliased(&items, 3000);
        let slow: Vec<Slow<T>> = items.iter().copied().map(Slow).collect();
        let slow_bytes = aliased(&slow, 3000);
        assert_eq!(bytes, slow_bytes);
        let bulk = decodes_until_the_limit::<T>(&bytes);
        let element_wise = decodes_until_the_limit::<Slow<T>>(&bytes);
        assert_eq!(bulk, element_wise);
        assert_eq!(bulk.1, DecodeError::WorkLimit);
        assert!(bulk.0 > 1, "the limit was hit at once: {bulk:?}");
    }
    same_budget(7u8);
    same_budget(-7i8);
    same_budget(true);
    same_budget(7u16);
    same_budget(7i16);
    same_budget(7u32);
    same_budget(-7i32);
    same_budget(7u64);
    same_budget(7i64);
    same_budget(7.5f32);
    same_budget(7.5f64);
}

// ---------------------------------------------------------------------------------------
// The contract of `Decode::decode_elements`.

/// A one-byte type whose `decode_elements` returns the wrong number of elements.
#[derive(Debug)]
struct Liar<const EXTRA: bool>(#[allow(dead_code)] u8);

impl<const EXTRA: bool> Wire for Liar<EXTRA> {
    const SHAPE: Shape = Shape::scalar(1);
}

impl<'de, const EXTRA: bool> Decode<'de> for Liar<EXTRA> {
    fn absent() -> Self {
        Liar(0)
    }

    fn decode_inline(bytes: &'de [u8]) -> Result<Self, DecodeError> {
        Ok(Liar(bytes[0]))
    }

    fn decode_elements(r: &Reader<'de>, header: &VectorHeader) -> Result<Vec<Self>, DecodeError> {
        let mut items: Vec<Self> = r
            .inline(header.first(), header.len())?
            .iter()
            .map(|&b| Liar(b))
            .collect();
        if EXTRA {
            items.push(Liar(0));
        } else {
            items.pop();
        }
        Ok(items)
    }
}

#[test]
fn vec_rejects_a_decode_elements_override_of_the_wrong_length() {
    let bytes = encode_vec(&[1u8, 2, 3]);
    let decode = |bytes: &[u8]| {
        let reader = Reader::new(bytes).unwrap();
        (
            reader.decode_root::<Vec<Liar<true>>>().map(|v| v.len()),
            reader.decode_root::<Vec<Liar<false>>>().map(|v| v.len()),
        )
    };
    let wrong = Err(DecodeError::Invalid("decode_elements length"));
    assert_eq!(decode(&bytes), (wrong.clone(), wrong));
    // An empty vector is not exempt (a pop on it is a no-op, the push is not).
    let (extra, _) = decode(&encode_vec::<u8>(&[]));
    assert_eq!(extra, Err(DecodeError::Invalid("decode_elements length")));
}

/// Decodes the vector at `at` as `Vec<U>` with the header of a `Vec<H>`.
struct Mismatched<H, U>(Result<Vec<U>, DecodeError>, std::marker::PhantomData<H>);

impl<H: Wire, U> Wire for Mismatched<H, U> {
    const SHAPE: Shape = Shape::Object;
}

impl<'de, H: Wire, U: Decode<'de>> Decode<'de> for Mismatched<H, U> {
    fn absent() -> Self {
        Mismatched(Ok(Vec::new()), std::marker::PhantomData)
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let header = r.vector_header::<H>(at)?;
        Ok(Mismatched(
            U::decode_elements(r, &header),
            std::marker::PhantomData,
        ))
    }
}

#[test]
fn scalar_decode_elements_reject_a_header_of_another_stride() {
    fn mismatched<H: Encode + Wire + Default + Copy, U: for<'de> Decode<'de>>()
    -> Result<usize, DecodeError> {
        let bytes = encode_vec(&[H::default(); 16]);
        let reader = Reader::new(&bytes).unwrap();
        let m: Mismatched<H, U> = reader.decode_root().unwrap();
        m.0.map(|v| v.len())
    }
    let wrong = Err(DecodeError::Invalid("decode_elements stride"));
    // A `u8` header (16 bytes) given to wider element types would read past what it checked.
    assert_eq!(mismatched::<u8, u64>(), wrong);
    assert_eq!(mismatched::<u8, u16>(), wrong);
    assert_eq!(mismatched::<u8, i32>(), wrong);
    assert_eq!(mismatched::<u8, f64>(), wrong);
    // And the reverse: a `u64` header given to byte types.
    assert_eq!(mismatched::<u64, u8>(), wrong);
    assert_eq!(mismatched::<u64, i8>(), wrong);
    assert_eq!(mismatched::<u64, bool>(), wrong);
    assert_eq!(mismatched::<u32, u64>(), wrong);
    // The matching stride is fine.
    assert_eq!(mismatched::<u8, u8>(), Ok(16));
    assert_eq!(mismatched::<i8, bool>(), Ok(16));
    assert_eq!(mismatched::<u32, f32>(), Ok(16));
    assert_eq!(mismatched::<u64, i64>(), Ok(16));
}

// ---------------------------------------------------------------------------------------
// `bool`s: only `std::vector<bool>` reads any nonzero byte as true.

#[test]
fn bools_outside_a_vec_must_be_zero_or_one() {
    use std::collections::{BTreeSet, HashSet};

    let strict = Some(DecodeError::Strict(StrictCheck::BoolValue));
    for bad in [2u8, 0x80, 0xff] {
        let bytes = encode_vec(&[0u8, bad, 1]);
        let reader = Reader::new(&bytes).unwrap();
        // `Vec<bool>` and a lazy view of it are `std::vector<bool>`: any nonzero is true.
        assert_eq!(
            reader.decode_root::<Vec<bool>>(),
            Ok(vec![false, true, true])
        );
        let view: VectorView<bool> = reader.decode_root().unwrap();
        assert_eq!(
            view.iter().collect::<Result<Vec<_>, _>>(),
            Ok(vec![false, true, true])
        );
        // `std::array<bool, N>` and the elements of a set are scalar bools.
        assert_eq!(reader.decode_root::<[bool; 3]>().err(), strict);
        assert_eq!(reader.decode_root::<BTreeSet<bool>>().err(), strict);
        assert_eq!(reader.decode_root::<HashSet<bool>>().err(), strict);
        // As is a bool in a struct, in a vector of structs and in a field.
        let pairs = encode_vec(&[(bad, 1u8)]);
        let reader = Reader::new(&pairs).unwrap();
        assert_eq!(reader.decode_root::<Vec<(bool, u8)>>().err(), strict);
    }
    // 0 and 1 are fine everywhere.
    let bytes = encode_vec(&[0u8, 1, 1]);
    let reader = Reader::new(&bytes).unwrap();
    assert_eq!(reader.decode_root::<[bool; 3]>(), Ok([false, true, true]));
    assert_eq!(
        reader.decode_root::<BTreeSet<bool>>(),
        Ok(BTreeSet::from([false, true]))
    );
}
