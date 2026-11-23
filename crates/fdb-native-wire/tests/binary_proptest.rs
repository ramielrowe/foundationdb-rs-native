//! Property tests of the legacy binary format: round trips, the exact byte layout of the
//! containers, and hostile input (truncation, huge counts, deep nesting, random bytes).

mod binary_support;

use std::collections::{BTreeMap, BTreeSet};

use binary_support::*;
use fdb_native_wire::MAX_DEPTH;
use fdb_native_wire::binary::{
    DecodeError, DecodeOwned, Encode, ReadVersioning, Reader, Versioning, from_slice, to_vec,
};
use fdb_native_wire::{FdbError, ProtocolVersion, Uid};
use proptest::prelude::*;

const V: ProtocolVersion = ProtocolVersion::V7_4;
const ASSUME: Versioning = Versioning::Assume(V);

/// The reader's option for what `versioning` wrote.
fn reading(versioning: Versioning) -> ReadVersioning {
    match versioning {
        Versioning::Unversioned => ReadVersioning::Unversioned,
        Versioning::Include(_) => ReadVersioning::INCLUDE_8_0,
        Versioning::Assume(v) => ReadVersioning::Assume(v),
    }
}

fn uid() -> impl Strategy<Value = Uid> {
    (any::<u64>(), any::<u64>()).prop_map(|(a, b)| Uid::new(a, b))
}

fn round_trip<T>(value: &T, versioning: Versioning) -> Result<Vec<u8>, TestCaseError>
where
    T: Encode + DecodeOwned + PartialEq + std::fmt::Debug,
{
    let bytes = to_vec(value, versioning).expect("encode");
    let back: T = from_slice(&bytes, reading(versioning)).expect("decode");
    prop_assert_eq!(&back, value);
    Ok(bytes)
}

fn variant3() -> impl Strategy<Value = Variant3> {
    prop_oneof![
        any::<i32>().prop_map(Variant3::Int),
        ".{0,12}".prop_map(Variant3::Str),
        any::<u8>().prop_map(Variant3::Byte),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn scalars(a in any::<u8>(), b in any::<i8>(), c in any::<u16>(), d in any::<i16>(),
               e in any::<u32>(), f in any::<i32>(), g in any::<u64>(), h in any::<i64>(),
               i in any::<bool>(), j in any::<u16>()) {
        // The raw little-endian bytes, nothing else.
        prop_assert_eq!(round_trip(&a, Versioning::Unversioned)?, a.to_le_bytes());
        prop_assert_eq!(round_trip(&b, Versioning::Unversioned)?, b.to_le_bytes());
        prop_assert_eq!(round_trip(&c, Versioning::Unversioned)?, c.to_le_bytes());
        prop_assert_eq!(round_trip(&d, Versioning::Unversioned)?, d.to_le_bytes());
        prop_assert_eq!(round_trip(&e, Versioning::Unversioned)?, e.to_le_bytes());
        prop_assert_eq!(round_trip(&f, Versioning::Unversioned)?, f.to_le_bytes());
        prop_assert_eq!(round_trip(&g, Versioning::Unversioned)?, g.to_le_bytes());
        prop_assert_eq!(round_trip(&h, Versioning::Unversioned)?, h.to_le_bytes());
        prop_assert_eq!(round_trip(&i, Versioning::Unversioned)?, [u8::from(i)]);
        prop_assert_eq!(round_trip(&FdbError::new(j), Versioning::Unversioned)?, j.to_le_bytes());
    }

    #[test]
    fn doubles_by_bits(bits in any::<u64>()) {
        let x = f64::from_bits(bits);
        let bytes = to_vec(&x, Versioning::Unversioned).unwrap();
        prop_assert_eq!(&bytes[..], &bits.to_le_bytes());
        let back: f64 = from_slice(&bytes, ReadVersioning::Unversioned).unwrap();
        prop_assert_eq!(back.to_bits(), bits);
    }

    #[test]
    fn uids(u in uid()) {
        let bytes = round_trip(&u, Versioning::Unversioned)?;
        prop_assert_eq!(&bytes[..8], &u.first.to_le_bytes());
        prop_assert_eq!(&bytes[8..], &u.second.to_le_bytes());
    }

    #[test]
    fn strings(s in ".{0,40}") {
        let bytes = round_trip(&s, ASSUME)?;
        prop_assert_eq!(&bytes[..4], &u32::try_from(s.len()).unwrap().to_le_bytes());
        prop_assert_eq!(&bytes[4..], s.as_bytes());
    }

    #[test]
    fn byte_strings(b in proptest::collection::vec(any::<u8>(), 0..64)) {
        let bytes = round_trip(&Bytes(b.clone()), ASSUME)?;
        prop_assert_eq!(&bytes[..4], &u32::try_from(b.len()).unwrap().to_le_bytes());
        prop_assert_eq!(&bytes[4..], &b[..]);
        // A Vec<u8> is the same bytes.
        prop_assert_eq!(round_trip(&b, ASSUME)?, bytes);
    }

    #[test]
    fn vectors(v in proptest::collection::vec(any::<i32>(), 0..32),
               w in proptest::collection::vec(proptest::collection::vec(any::<u16>(), 0..4), 0..6),
               s in proptest::collection::vec(".{0,6}", 0..6)) {
        let bytes = round_trip(&v, ASSUME)?;
        prop_assert_eq!(bytes.len(), 4 + 4 * v.len());
        round_trip(&w, ASSUME)?;
        round_trip(&s, ASSUME)?;
    }

    #[test]
    fn arrays_have_no_count(a in any::<[u16; 5]>()) {
        let bytes = round_trip(&a, ASSUME)?;
        prop_assert_eq!(bytes.len(), 10);
    }

    #[test]
    fn sets_and_maps(set in proptest::collection::btree_set(any::<i32>(), 0..16),
                     map in proptest::collection::btree_map(".{0,5}", any::<i64>(), 0..8),
                     umap in proptest::collection::btree_map(uid(), any::<i64>(), 0..8)) {
        let bytes = round_trip(&set, ASSUME)?;
        prop_assert_eq!(bytes.len(), 4 + 4 * set.len());
        round_trip(&map, ASSUME)?;
        let bytes = round_trip(&umap, ASSUME)?;
        prop_assert_eq!(bytes.len(), 4 + 24 * umap.len());
    }

    #[test]
    fn optionals_and_pairs(o in proptest::option::of(any::<i32>()),
                           p in (any::<u8>(), ".{0,6}"),
                           oo in proptest::option::of(proptest::option::of(any::<u8>()))) {
        let bytes = round_trip(&o, ASSUME)?;
        prop_assert_eq!(bytes.len(), if o.is_some() { 5 } else { 1 });
        round_trip(&p, ASSUME)?;
        round_trip(&oo, ASSUME)?;
    }

    #[test]
    fn variants_enums_and_structs(v in variant3(), x in any::<i32>(), y in any::<i64>(), label in ".{0,6}") {
        let bytes = round_trip(&v, ASSUME)?;
        prop_assert!(bytes[0] < 3);
        for c in [Color::Red, Color::Green, Color::Blue] {
            round_trip(&c, ASSUME)?;
        }
        for l in [Level::Low, Level::Mid, Level::High] {
            round_trip(&l, ASSUME)?;
        }
        round_trip(&Point3 { x, y, label }, ASSUME)?;
    }

    #[test]
    fn include_version_prefix(x in any::<u32>(), v in proptest::collection::vec(any::<i64>(), 0..8)) {
        let include = Versioning::Include(V);
        let bytes = round_trip(&x, include)?;
        prop_assert_eq!(&bytes[..8], &V.0.to_le_bytes());
        prop_assert_eq!(&bytes[8..], &x.to_le_bytes());
        let bytes = round_trip(&v, include)?;
        prop_assert_eq!(&bytes[..8], &V.0.to_le_bytes());
        // Assume writes no prefix, Include writes 8 bytes: the rest is the same.
        prop_assert_eq!(&bytes[8..], &to_vec(&v, ASSUME).unwrap()[..]);
    }

    // --- hostile input ------------------------------------------------------------------

    /// Random bytes never panic any reader, whatever the versioning.
    #[test]
    fn random_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..96),
                                which in 0u8..3) {
        let versioning = match which {
            0 => Versioning::Unversioned,
            1 => Versioning::Assume(V),
            _ => Versioning::Include(V),
        };
        for ty in ["u32", "bool", "string", "bytes", "vec<i32>", "vec<string>", "vec<vec<u16>>",
                   "vec<key_value>", "map<string,i64>", "map<uid,i64>", "set<i32>", "optional<string>",
                   "pair<u8,string>", "point3", "variant3", "enum_color", "enum_level", "key_range"] {
            let _ = decode_any(ty, &bytes, reading(versioning)).expect("known type");
        }
        let _ = from_slice::<Vec<Vec<Vec<Option<(u8, String)>>>>>(&bytes, reading(versioning));
        let _ = from_slice::<Node>(&bytes, reading(versioning));
        let _ = from_slice::<Vec<Unit>>(&bytes, reading(versioning));
    }

    /// Every proper prefix of a valid encoding is rejected (a count or a length would
    /// otherwise be satisfied by fewer bytes than it says).
    #[test]
    fn every_truncation_fails(v in proptest::collection::vec(
            (".{0,5}", proptest::option::of(any::<i64>()), variant3()), 1..5)) {
        let bytes = to_vec(&v, ASSUME).unwrap();
        for cut in 0..bytes.len() {
            let r = from_slice::<Vec<(String, Option<i64>, Variant3)>>(&bytes[..cut], reading(ASSUME));
            prop_assert!(r.is_err(), "{cut} of {} bytes decoded", bytes.len());
        }
        prop_assert!(from_slice::<Vec<(String, Option<i64>, Variant3)>>(&bytes, reading(ASSUME)).is_ok());
    }
}

// ---------------------------------------------------------------------------------------
// Targeted hostile input.

fn read<T: DecodeOwned>(bytes: &[u8]) -> Result<T, DecodeError> {
    from_slice(bytes, reading(ASSUME))
}

#[test]
fn a_huge_count_does_not_allocate() {
    // 2^31 - 1 elements claimed, none present: an error, with no allocation of 8 GiB.
    let bytes = 0x7fff_ffffu32.to_le_bytes();
    assert!(matches!(
        read::<Vec<u32>>(&bytes),
        Err(DecodeError::UnexpectedEof { .. })
    ));
    assert!(matches!(
        read::<Vec<u8>>(&bytes),
        Err(DecodeError::UnexpectedEof { .. })
    ));
    assert!(matches!(
        read::<String>(&bytes),
        Err(DecodeError::UnexpectedEof { .. })
    ));
    assert!(matches!(
        read::<Bytes>(&bytes),
        Err(DecodeError::UnexpectedEof { .. })
    ));
    assert!(matches!(
        read::<Vec<Vec<Vec<u64>>>>(&bytes),
        Err(DecodeError::UnexpectedEof { .. })
    ));
    assert!(matches!(
        read::<BTreeMap<u64, u64>>(&bytes),
        Err(DecodeError::UnexpectedEof { .. })
    ));
    assert!(matches!(
        read::<BTreeSet<u64>>(&bytes),
        Err(DecodeError::UnexpectedEof { .. })
    ));
    assert!(read::<[u8; 4]>(&bytes).is_ok());
}

#[test]
fn a_count_above_i32_max_is_refused() {
    for count in [0x8000_0000u32, 0xffff_ffff] {
        let bytes = count.to_le_bytes();
        assert_eq!(
            read::<Vec<u8>>(&bytes),
            Err(DecodeError::CountTooLarge(count))
        );
        assert_eq!(
            read::<String>(&bytes),
            Err(DecodeError::CountTooLarge(count))
        );
        assert_eq!(
            read::<Vec<Unit>>(&bytes),
            Err(DecodeError::CountTooLarge(count))
        );
    }
}

#[test]
fn a_count_larger_than_the_input_is_cut_short_early() {
    // A million elements claimed over a 12-byte input.
    let mut bytes = 1_000_000u32.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[0; 8]);
    assert!(matches!(
        read::<Vec<u32>>(&bytes),
        Err(DecodeError::UnexpectedEof { .. })
    ));
    assert!(matches!(
        read::<Vec<(u64, u64)>>(&bytes),
        Err(DecodeError::UnexpectedEof { .. })
    ));
}

#[test]
fn zero_width_elements_are_bounded() {
    // A vector of a million no-byte values over a 4-byte input: C++ would loop a million
    // times; this reader's work limit (4 x the input + 1024) stops it.
    let bytes = 1_000_000u32.to_le_bytes();
    assert_eq!(read::<Vec<Unit>>(&bytes), Err(DecodeError::WorkLimit));
    // Within the limit it is fine.
    let bytes = 100u32.to_le_bytes();
    assert_eq!(read::<Vec<Unit>>(&bytes).unwrap().len(), 100);
    // The same through nesting.
    let bytes = 1_000_000u32.to_le_bytes();
    assert_eq!(
        read::<Vec<Option<Unit>>>(&bytes),
        Err(DecodeError::UnexpectedEof {
            pos: 4,
            needed: 1,
            remaining: 0
        })
    );
}

#[test]
fn nesting_is_bounded() {
    // An Optional chain `1 1 1 1 ...` as deep as the input allows.
    let deep = vec![1u8; 100_000];
    assert_eq!(read::<Node>(&deep), Err(DecodeError::TooDeep));
    // A shallow chain decodes; the limit counts nested `read`s (several per Node), so
    // somewhere between the two the reader gives up with TooDeep and never anything else.
    let mut last_ok = 0;
    for links in 0..MAX_DEPTH {
        let mut chain = vec![1u8; links];
        chain.push(0);
        match read::<Node>(&chain) {
            Ok(_) => last_ok = links,
            Err(e) => {
                assert_eq!(e, DecodeError::TooDeep, "{links} links");
                break;
            }
        }
    }
    assert!(last_ok >= MAX_DEPTH / 4, "only {last_ok} links decode");
    assert!(last_ok < MAX_DEPTH - 1, "the depth limit never triggered");
    // Vectors of vectors: a count of 1 at each level.
    let mut nested = Vec::new();
    for _ in 0..100_000 {
        nested.extend_from_slice(&1u32.to_le_bytes());
    }
    assert!(read::<Vec<Vec<Vec<Vec<Vec<Vec<u8>>>>>>>(&nested).is_err());
}

#[test]
fn invalid_values_are_rejected() {
    assert_eq!(read::<bool>(&[2]), Err(DecodeError::InvalidBool(2)));
    assert_eq!(read::<bool>(&[255]), Err(DecodeError::InvalidBool(255)));
    assert!(matches!(
        read::<Option<u8>>(&[2, 0]),
        Err(DecodeError::InvalidBool(2))
    ));
    assert!(matches!(
        read::<Color>(&[0]),
        Err(DecodeError::InvalidEnum { .. })
    ));
    assert!(matches!(
        read::<Color>(&[3]),
        Err(DecodeError::InvalidEnum { .. })
    ));
    assert!(matches!(
        read::<Level>(&1i32.to_le_bytes()),
        Err(DecodeError::InvalidEnum { .. })
    ));
    assert!(matches!(
        read::<Variant3>(&[3, 0, 0, 0, 0, 0]),
        Err(DecodeError::InvalidVariantIndex {
            index: 3,
            alternatives: 3
        })
    ));
    assert_eq!(
        read::<String>(&[1, 0, 0, 0, 0xff]),
        Err(DecodeError::InvalidUtf8)
    );
    // The same bytes read as bytes are fine.
    assert_eq!(
        read::<Bytes>(&[1, 0, 0, 0, 0xff]).unwrap(),
        Bytes(vec![0xff])
    );
}

#[test]
fn trailing_bytes_are_an_error_and_prefix_reads_are_not() {
    assert_eq!(read::<u8>(&[1, 2]), Err(DecodeError::TrailingBytes(1)));
    let mut r = Reader::assume_version(&[1, 2], V).unwrap();
    assert_eq!(r.read::<u8>().unwrap(), 1);
    assert_eq!(r.remaining(), &[2]);
    assert_eq!(r.finish(), Err(DecodeError::TrailingBytes(1)));
}

#[test]
fn include_version_rejects_bad_prefixes() {
    let include = ReadVersioning::INCLUDE_8_0;
    for (bytes, ok) in [
        (0u64.to_le_bytes(), false),
        (0x0FDB_00A2_0006_0000u64.to_le_bytes(), false), // just below the first valid version
        (0x0FDB_00A2_0006_0001u64.to_le_bytes(), true),
        (V.0.to_le_bytes(), true),
        (u64::MAX.to_le_bytes(), false),
    ] {
        let mut data = bytes.to_vec();
        data.extend_from_slice(&7u32.to_le_bytes());
        assert_eq!(
            from_slice::<u32>(&data, include).is_ok(),
            ok,
            "{bytes:02x?}"
        );
    }
    for len in 0..8 {
        assert!(matches!(
            from_slice::<u32>(&vec![0xab; len], include),
            Err(DecodeError::UnexpectedEof { .. })
        ));
    }
}

#[test]
fn a_flagged_version_is_the_same_version() {
    // The object-serializer flag is not part of the version's value.
    let flagged = V.with_object_serializer_flag();
    let mut data = flagged.0.to_le_bytes().to_vec();
    data.extend_from_slice(&7u32.to_le_bytes());
    let mut r = Reader::include_version(&data, ProtocolVersion::MIN_INVALID_8_0).unwrap();
    assert_eq!(r.protocol_version().unwrap(), flagged);
    assert!(flagged.has_object_serializer_flag());
    // The reader still reads the binary rest; handing it to a flatbuffers reader is the caller's job.
    assert_eq!(r.read::<u32>().unwrap(), 7);
}

#[test]
fn writer_refuses_invalid_versions() {
    use fdb_native_wire::binary::{EncodeError, Writer};
    assert!(matches!(
        Writer::include_version(ProtocolVersion(1)),
        Err(EncodeError::InvalidProtocolVersion(1))
    ));
    assert!(matches!(
        Writer::assume_version(ProtocolVersion(0)),
        Err(EncodeError::InvalidProtocolVersion(0))
    ));
    assert!(Writer::unversioned().is_empty());
}
