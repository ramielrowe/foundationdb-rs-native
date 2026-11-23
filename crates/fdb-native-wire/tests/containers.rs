//! Maps and sets (`BTreeMap`, `BTreeSet`, `HashMap`, `HashSet`): round trips, the wire form,
//! the engine's `containers` fixture, C++'s duplicate handling, file identifiers, and the
//! iterator-based vector writer they are built on.

mod map_support;
mod support;

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{BuildHasherDefault, Hash};

use fdb_native_wire::{
    Decode, DecodeError, DecodeOwned, Encode, EncodeError, FileIdentified, ObjectRef, Reader,
    Shape, Wire, Writer, decode, encode, encode_with_identifier,
};
use map_support::{HashContainers, MapContainers};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use support::*;

fn cases(n: u32) -> ProptestConfig {
    ProptestConfig {
        cases: n,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

fn fail(e: impl std::fmt::Display) -> TestCaseError {
    TestCaseError::fail(e.to_string())
}

/// `value` encodes and decodes to itself, as the root's member.
fn trip<T: Encode + DecodeOwned + PartialEq + Debug>(value: &T) -> Result<(), TestCaseError> {
    let bytes = encode_with_identifier(value, 1).map_err(fail)?;
    let back: T = Reader::new(&bytes)
        .and_then(|r| r.decode_root())
        .map_err(fail)?;
    prop_assert_eq!(&back, value);
    Ok(())
}

type Det = BuildHasherDefault<DefaultHasher>;

proptest! {
    #![proptest_config(cases(200))]

    #[test]
    fn btree_map_round_trips(
        a in prop::collection::btree_map(any::<u32>(), Bytes::arb(), 0..40),
        b in prop::collection::btree_map(prop::collection::vec(any::<u8>(), 0..4), Vec::<u16>::arb(), 0..10),
        c in prop::collection::btree_map(any::<i64>(), Option::<i32>::arb(), 0..20),
        d in prop::collection::btree_map(any::<bool>(), any::<u8>(), 0..3),
        e in prop::collection::btree_map((any::<u8>(), any::<u32>()), (any::<i16>(), any::<bool>(), any::<i64>()), 0..10),
        f in prop::collection::btree_map(any::<u8>(), prop::collection::btree_set(any::<u32>(), 0..5), 0..6),
    ) {
        trip(&a)?;
        trip(&b)?;
        trip(&c)?;
        trip(&d)?;
        trip(&e)?;
        trip(&f)?;
        trip(&vec![a.clone(), a])?;
    }

    #[test]
    fn btree_set_round_trips(
        a in prop::collection::btree_set(any::<u64>(), 0..40),
        b in prop::collection::btree_set(prop::collection::vec(any::<u8>(), 0..4), 0..10),
        c in prop::collection::btree_set((any::<i16>(), any::<bool>(), any::<i64>()), 0..10),
        d in prop::collection::btree_set(any::<bool>(), 0..3),
        e in prop::collection::btree_set(any::<i8>(), 0..40),
    ) {
        trip(&a)?;
        trip(&b)?;
        trip(&c)?;
        trip(&d)?;
        trip(&e)?;
    }

    #[test]
    fn hash_map_round_trips(
        a in prop::collection::hash_map(any::<u32>(), Bytes::arb(), 0..40),
        b in prop::collection::hash_map(prop::collection::vec(any::<u8>(), 0..4), any::<i64>(), 0..10),
        c in prop::collection::hash_map(any::<u16>(), Option::<Bytes>::arb(), 0..10),
        d in prop::collection::hash_map(any::<u8>(), prop::collection::hash_set(any::<u32>(), 0..5), 0..6),
    ) {
        trip(&a)?;
        trip(&b)?;
        trip(&c)?;
        trip(&d)?;
        // Another hasher: the map type is chosen by the caller.
        let det: HashMap<u32, Bytes, Det> = a.into_iter().collect();
        trip(&det)?;
    }

    #[test]
    fn hash_set_round_trips(
        a in prop::collection::hash_set(any::<u64>(), 0..40),
        b in prop::collection::hash_set(prop::collection::vec(any::<u8>(), 0..4), 0..10),
        c in prop::collection::hash_set((any::<i16>(), any::<bool>(), any::<i64>()), 0..10),
        d in prop::collection::hash_set(any::<bool>(), 0..3),
    ) {
        trip(&a)?;
        trip(&b)?;
        trip(&c)?;
        trip(&d)?;
        let det: HashSet<u64, Det> = a.into_iter().collect();
        trip(&det)?;
    }

    #[test]
    fn containers_tables_round_trip(m in MapContainers::arb(), h in HashContainers::arb()) {
        trip(&m)?;
        trip(&h)?;
    }

    #[test]
    fn borrowed_keys_and_values_round_trip(
        entries in prop::collection::vec(
            (prop::collection::vec(any::<u8>(), 0..6), prop::collection::vec(any::<u8>(), 0..6)),
            0..20,
        ),
    ) {
        let map: BTreeMap<&[u8], &[u8]> =
            entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        let bytes = encode_with_identifier(&map, 1).map_err(fail)?;
        let back: BTreeMap<&[u8], &[u8]> =
            Reader::new(&bytes).and_then(|r| r.decode_root()).map_err(fail)?;
        prop_assert_eq!(back, map);
        let set: BTreeSet<&[u8]> = entries.iter().map(|(k, _)| k.as_slice()).collect();
        let bytes = encode_with_identifier(&set, 1).map_err(fail)?;
        let back: BTreeSet<&[u8]> =
            Reader::new(&bytes).and_then(|r| r.decode_root()).map_err(fail)?;
        prop_assert_eq!(back, set);
    }

    /// The wire form is the vector form: a set is the vector of its keys, a map the vector
    /// of `{first, second}` pair tables, byte for byte.
    #[test]
    fn maps_and_sets_are_vectors_on_the_wire(
        m in prop::collection::btree_map(any::<i32>(), Bytes::arb(), 0..12),
        s in prop::collection::btree_set(any::<u32>(), 0..12),
        h in prop::collection::hash_set(any::<u32>(), 0..12),
    ) {
        let pairs: Vec<Pair> = m
            .iter()
            .map(|(k, v)| Pair { first: *k, second: v.clone() })
            .collect();
        prop_assert_eq!(
            encode_with_identifier(&m, 9).map_err(fail)?,
            encode_with_identifier(&pairs, 9).map_err(fail)?
        );
        let keys: Vec<u32> = s.iter().copied().collect();
        prop_assert_eq!(
            encode_with_identifier(&s, 9).map_err(fail)?,
            encode_with_identifier(&keys, 9).map_err(fail)?
        );
        let in_hash_order: Vec<u32> = h.iter().copied().collect();
        prop_assert_eq!(
            encode_with_identifier(&h, 9).map_err(fail)?,
            encode_with_identifier(&in_hash_order, 9).map_err(fail)?
        );
    }

    /// `Containers` written from real maps and sets is byte for byte what the Vec stand-ins
    /// write (and so what the C++ crosscheck verifies for them).
    #[test]
    fn containers_match_the_vec_stand_ins(m in MapContainers::arb()) {
        let stand_in = Containers {
            m: m.m.iter().map(|(k, v)| Pair { first: *k, second: v.clone() }).collect(),
            s: m.s.iter().copied().collect(),
            nested: m.nested.clone(),
        };
        prop_assert_eq!(
            encode(&m).map_err(fail)?,
            encode(&stand_in).map_err(fail)?
        );
    }
}

// ---------------------------------------------------------------------------------------
// The engine fixture.

fn containers_record() -> serde_json::Value {
    records("positive")
        .into_iter()
        .find(|r| r["name"] == "containers")
        .expect("the containers fixture")
}

#[test]
fn containers_fixture_decodes_into_real_maps_and_sets() {
    let record = containers_record();
    let bytes = record_bytes(&record);

    let map: MapContainers = decode(&bytes).expect("decode as BTreeMap/BTreeSet");
    assert_json_eq("containers", &record["root"], &root1(&map));
    assert_eq!(
        map.m,
        BTreeMap::from([
            (-1, Bytes(b"minus one".to_vec())),
            (3, Bytes(b"three".to_vec()))
        ])
    );
    assert_eq!(map.s, BTreeSet::from([10, 20, 30]));
    assert_eq!(map.nested, vec![vec![1, 2], vec![], vec![3]]);

    let hash: HashContainers = decode(&bytes).expect("decode as HashMap/HashSet");
    assert_json_eq("containers (hash)", &record["root"], &root1(&hash));
    assert_eq!(hash.sorted(), map);

    // And the other way: what we write from the maps is what the fixture's reader
    // accepts, with the same root dump (the cpp_crosscheck test sends it to C++).
    let again = encode(&map).unwrap();
    let again: MapContainers = decode(&again).unwrap();
    assert_json_eq("containers re-encoded", &record["root"], &root1(&again));
}

// ---------------------------------------------------------------------------------------
// Duplicates, order, file identifiers.

#[test]
fn first_entry_for_a_key_wins_like_cpp() {
    // C++ builds the map with `insert(end(), value)` in wire order, which keeps the first
    // entry for a key. The wire order need not be sorted, and duplicates are not an error.
    let wire = Containers {
        m: vec![
            Pair {
                first: 3,
                second: Bytes(b"first three".to_vec()),
            },
            Pair {
                first: 1,
                second: Bytes(b"one".to_vec()),
            },
            Pair {
                first: 3,
                second: Bytes(b"second three".to_vec()),
            },
            Pair {
                first: 1,
                second: Bytes(b"another one".to_vec()),
            },
        ],
        s: vec![30, 10, 30, 20, 10],
        nested: vec![],
    };
    let bytes = encode(&wire).unwrap();
    let want_m = BTreeMap::from([
        (1, Bytes(b"one".to_vec())),
        (3, Bytes(b"first three".to_vec())),
    ]);
    let want_s = BTreeSet::from([10, 20, 30]);

    let m: MapContainers = decode(&bytes).unwrap();
    assert_eq!((&m.m, &m.s), (&want_m, &want_s));
    let h: HashContainers = decode(&bytes).unwrap();
    assert_eq!(h.sorted().m, want_m);
    assert_eq!(h.sorted().s, want_s);
}

/// An element that is equal by key only, to see which of several equal elements survives.
#[derive(Clone, Copy, Debug)]
struct Tagged {
    key: u8,
    tag: u8,
}

impl PartialEq for Tagged {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl Eq for Tagged {}
impl PartialOrd for Tagged {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Tagged {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}
impl Hash for Tagged {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl Wire for Tagged {
    const SHAPE: Shape = Shape::of_struct(&[Shape::scalar(1), Shape::scalar(1)]);
}
impl Encode for Tagged {
    fn encode_inline(&self, out: &mut [u8]) -> Result<(), EncodeError> {
        out.copy_from_slice(&[self.key, self.tag]);
        Ok(())
    }
}
impl<'de> Decode<'de> for Tagged {
    fn absent() -> Self {
        Tagged { key: 0, tag: 0 }
    }
    fn decode_inline(bytes: &'de [u8]) -> Result<Self, DecodeError> {
        match bytes {
            [key, tag] => Ok(Tagged {
                key: *key,
                tag: *tag,
            }),
            _ => Err(DecodeError::Invalid("size")),
        }
    }
}

#[test]
fn first_of_equal_set_elements_wins() {
    let wire = vec![
        Tagged { key: 1, tag: 10 },
        Tagged { key: 0, tag: 5 },
        Tagged { key: 1, tag: 20 },
        Tagged { key: 0, tag: 6 },
    ];
    let bytes = encode_with_identifier(&wire, 1).unwrap();
    let r = Reader::new(&bytes).unwrap();
    let btree: BTreeSet<Tagged> = r.decode_root().unwrap();
    let tags: Vec<u8> = btree.iter().map(|t| t.tag).collect();
    assert_eq!(tags, [5, 10]);
    let hash: HashSet<Tagged> = r.decode_root().unwrap();
    let mut tags: Vec<u8> = hash.iter().map(|t| t.tag).collect();
    tags.sort_unstable();
    assert_eq!(tags, [5, 10]);
}

#[test]
fn only_unordered_set_has_a_file_identifier() {
    // `FileIdentifierFor<std::unordered_set<K>>` composes with code 6 (serialize.h); the
    // other containers have none, so they are not FileIdentified (see the compile_fail
    // doctests of the collections module).
    let id = <HashSet<Inner> as FileIdentified>::FILE_IDENTIFIER;
    assert_eq!(id.value(), 0x060f_db0a);
    assert_eq!(id.composition_depth(), 1);
    assert_eq!(
        <Vec<HashSet<Inner>> as FileIdentified>::FILE_IDENTIFIER.value(),
        0x560f_db0a
    );
}

#[test]
fn hash_set_round_trips_through_the_typed_entry_points() {
    // A hashable table type with an identifier.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct Key(u32);
    impl Wire for Key {
        const SHAPE: Shape = Shape::Table;
    }
    impl FileIdentified for Key {
        const FILE_IDENTIFIER: fdb_native_wire::FileIdentifier =
            fdb_native_wire::FileIdentifier::new(0x000f_db0a);
    }
    impl Encode for Key {
        fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
            let mut t = w.table();
            t.field(&self.0)?;
            t.finish()
        }
    }
    impl<'de> Decode<'de> for Key {
        fn absent() -> Self {
            Key(0)
        }
        fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
            Ok(Key(r.table(at)?.field()?))
        }
    }

    let set: HashSet<Key> = (0..5).map(Key).collect();
    let bytes = encode(&set).unwrap();
    assert_eq!(&bytes[4..8], &0x060f_db0au32.to_le_bytes());
    assert_eq!(decode::<HashSet<Key>>(&bytes).unwrap(), set);
    // A message of another type is rejected by the identifier.
    assert!(matches!(
        decode::<Vec<Key>>(&bytes),
        Err(DecodeError::FileIdentifierMismatch { .. })
    ));
}

#[test]
fn absent_maps_and_sets_are_empty() {
    // Fields past the end of a shorter writer's vtable.
    let v1 = SchemaV1 {
        a: 1,
        b: Bytes::default(),
    };
    let bytes = encode(&v1).unwrap();
    #[derive(Debug)]
    struct Newer {
        m: BTreeMap<u8, u8>,
        s: HashSet<u8>,
    }
    impl Wire for Newer {
        const SHAPE: Shape = Shape::Table;
    }
    impl<'de> Decode<'de> for Newer {
        fn absent() -> Self {
            Newer {
                m: Decode::absent(),
                s: Decode::absent(),
            }
        }
        fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
            let mut t = r.table(at)?;
            t.skip(2);
            Ok(Newer {
                m: t.field()?,
                s: t.field()?,
            })
        }
    }
    let newer: Newer = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert!(newer.m.is_empty() && newer.s.is_empty());
}

// ---------------------------------------------------------------------------------------
// Hostile input for containers.

#[test]
fn hostile_map_counts_are_rejected_without_allocating() {
    // A million zero offsets: every pair table is invalid, and the count must not drive an
    // allocation before the first element fails.
    let mut w = Writer::new();
    let v = w.vector(&vec![0u32; 1 << 20]).unwrap();
    let mut root = w.table();
    root.field(&Alias(v)).unwrap();
    let root = root.finish().unwrap();
    let bytes = w.finish(root, 1).unwrap();
    let r = Reader::new(&bytes).unwrap();
    assert!(r.decode_root::<BTreeMap<u32, u32>>().is_err());
    assert!(r.decode_root::<HashMap<u32, u32>>().is_err());
    assert!(r.decode_root::<HashSet<u32>>().is_ok(), "a million zeros");
    // Eight-byte elements do not fit.
    assert!(r.decode_root::<BTreeSet<u64>>().is_err());

    // A count past the end of the message.
    let mut bytes = encode_with_identifier(&BTreeSet::from([1u32, 2, 3]), 1).unwrap();
    let at = bytes
        .windows(8)
        .position(|w| w == [3, 0, 0, 0, 1, 0, 0, 0])
        .expect("count and first element");
    bytes[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    let r = Reader::new(&bytes).unwrap();
    assert!(r.decode_root::<BTreeSet<u32>>().is_err());
    assert!(r.decode_root::<HashSet<u32>>().is_err());
    assert!(r.decode_root::<HashMap<u32, u32>>().is_err());
}

#[test]
fn map_with_aliased_pairs_decodes_within_the_work_limit() {
    // 100k offsets to one pair table: the pairs decode (same key, first wins), within the
    // work budget, into one entry.
    let mut w = Writer::new();
    let mut pair = w.table();
    pair.field(&7u32).unwrap();
    pair.field(&9u32).unwrap();
    let pair = pair.finish().unwrap();
    let v = w.vector(&vec![Alias(pair); 100_000]).unwrap();
    let mut root = w.table();
    root.field(&Alias(v)).unwrap();
    let root = root.finish().unwrap();
    let bytes = w.finish(root, 1).unwrap();
    let r = Reader::new(&bytes).unwrap();
    // 100k × (4 + 4) units for the offsets and tables: about 800k of 1.6M.
    let map: BTreeMap<u32, u32> = r.decode_root().unwrap();
    assert_eq!(map, BTreeMap::from([(7, 9)]));
}

// ---------------------------------------------------------------------------------------
// The iterator-based writer.

fn finish_vector(w: &mut Writer, v: ObjectRef) -> Vec<u8> {
    let mut root = w.table();
    root.field(&Alias(v)).unwrap();
    let root = root.finish().unwrap();
    w.finish(root, 1).unwrap()
}

proptest! {
    #![proptest_config(cases(200))]

    #[test]
    fn vector_iter_is_vector(
        a in Vec::<u32>::arb(),
        b in Vec::<Bytes>::arb(),
        c in Vec::<(i16, bool, i64)>::arb(),
        d in Vec::<Small>::arb(),
        e in Vec::<Vec<u8>>::arb(),
    ) {
        fn same<T: Encode>(items: &[T]) -> Result<(), TestCaseError> {
            let mut w = Writer::new();
            let v = w.vector(items).map_err(fail)?;
            let by_slice = finish_vector(&mut w, v);
            let v = w.vector_iter(items.iter()).map_err(fail)?;
            let by_iter = finish_vector(&mut w, v);
            let v = w.vector_counted(items.len(), items).map_err(fail)?;
            let by_count = finish_vector(&mut w, v);
            prop_assert_eq!(&by_iter, &by_slice);
            prop_assert_eq!(&by_count, &by_slice);
            Ok(())
        }
        same(&a)?;
        same(&b)?;
        same(&c)?;
        same(&d)?;
        same(&e)?;
    }
}

#[test]
fn vector_counted_checks_the_count() {
    let mut w = Writer::new();
    // Too few items, for inline, offset and table elements.
    assert!(matches!(
        w.vector_counted(5, 0..3u32),
        Err(EncodeError::InvalidShape(_))
    ));
    w.reset();
    assert!(matches!(
        w.vector_counted(3, [Bytes::default(), Bytes::default()].iter()),
        Err(EncodeError::InvalidShape(_))
    ));
    w.reset();
    assert!(matches!(
        w.vector_counted(2, std::iter::empty::<&Small>()),
        Err(EncodeError::InvalidShape(_))
    ));
    w.reset();
    // Too many.
    assert!(matches!(
        w.vector_counted(2, 0..3u32),
        Err(EncodeError::InvalidShape(_))
    ));
    w.reset();
    assert!(matches!(
        w.vector_counted(0, 0..1u32),
        Err(EncodeError::InvalidShape(_))
    ));
    w.reset();
    // The writer is usable again after reset, and a filtered iterator with a known count
    // writes the same as the collected vector.
    let items = [1u32, 2, 3, 4, 5, 6];
    let v = w
        .vector_counted(3, items.iter().filter(|x| **x % 2 == 0))
        .unwrap();
    let bytes = finish_vector(&mut w, v);
    let back: Vec<u32> = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(back, [2, 4, 6]);
    assert!(w.vector_counted(0, std::iter::empty::<u8>()).is_ok());
    // Unions are not vector elements here.
    assert!(matches!(
        w.vector_iter(vec![Some(1u8)]),
        Err(EncodeError::InvalidShape(_))
    ));
}
