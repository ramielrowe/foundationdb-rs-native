//! Maps and sets: `std::map`, `std::set`, `std::unordered_map` and `std::unordered_set`.
//!
//! All four are vectors on the wire (`vector_like_traits` in `flat_buffers.h`): a set is a
//! vector of its keys, a map a vector of offsets to `std::pair` tables with two slots
//! (`first`, `second`). `std::pair` is a table whatever its members are (only `std::tuple`
//! is a struct), so a map of scalars still has one table per entry.
//!
//! Decoding follows the C++ readers, which build the container with `insert(hint, value)`
//! in wire order: an entry whose key is already present is dropped, so **the first of
//! several entries with the same key wins** (the wire order need not be sorted, and
//! duplicates are not an error). FDB's writers never produce duplicates; a hand-built
//! message might.
//!
//! Only `std::unordered_set` has a file identifier in FDB (`FileIdentifierFor` in
//! `serialize.h`, composition code 6), so only [`HashSet`] is [`FileIdentified`]; a map or
//! a `std::set` cannot be the root of a message, only a field of one.
//!
//! ```
//! use std::collections::{BTreeMap, HashSet};
//! use fdb_native_wire::{encode_with_identifier, Reader};
//!
//! // A map or set is a member of the root table like any other value; only the typed
//! // `encode`/`decode` entry points need an identifier (see below).
//! let m = BTreeMap::from([(1u32, 10u32), (2, 20)]);
//! let bytes = encode_with_identifier(&m, 7)?;
//! assert_eq!(Reader::new(&bytes)?.decode_root::<BTreeMap<u32, u32>>()?, m);
//! let s = HashSet::from([1u32]);
//! let bytes = encode_with_identifier(&s, 7)?;
//! assert_eq!(Reader::new(&bytes)?.decode_root::<HashSet<u32>>()?, s);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ```compile_fail
//! // std::map has no file identifier in FDB, so it cannot be the typed root of a message.
//! let _ = fdb_native_wire::encode(&std::collections::BTreeMap::<u8, u8>::new());
//! ```
//!
//! ```compile_fail
//! // A set of unions has no wire form (use a Vec).
//! let set: std::collections::BTreeSet<Option<u8>> = Default::default();
//! let _ = fdb_native_wire::encode_with_identifier(&set, 1);
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher, Hash};

use crate::codec::preallocation;
use crate::{
    Decode, DecodeError, Encode, EncodeError, FileIdentified, FileIdentifier, ObjectRef, Reader,
    Shape, Wire, Wrapper, Writer,
};

/// A `std::pair<K, V>` being written: a table of `first` and `second`.
struct PairRef<'a, K, V>(&'a K, &'a V);

impl<K, V> Wire for PairRef<'_, K, V> {
    const SHAPE: Shape = Shape::Table;
}

impl<K: Encode, V: Encode> Encode for PairRef<'_, K, V> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(self.0)?;
        t.field(self.1)?;
        t.finish()
    }
}

/// A `std::pair<K, V>` being read.
struct Pair<K, V>(K, V);

impl<K, V> Wire for Pair<K, V> {
    const SHAPE: Shape = Shape::Table;
}

impl<'de, K: Decode<'de>, V: Decode<'de>> Decode<'de> for Pair<K, V> {
    fn absent() -> Self {
        Pair(K::absent(), V::absent())
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(Pair(t.field()?, t.field()?))
    }
}

// ---------------------------------------------------------------------------------------
// std::map.

impl<K, V> Wire for BTreeMap<K, V> {
    const SHAPE: Shape = Shape::Object;
}

/// `std::map<K, V>`: a vector of pair tables, in key order.
impl<K: Encode, V: Encode> Encode for BTreeMap<K, V> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.vector_iter(self.iter().map(|(k, v)| PairRef(k, v)))
    }
}

/// Entries are inserted in wire order, in any order, and the first entry for a key wins.
impl<'de, K: Decode<'de> + Ord, V: Decode<'de>> Decode<'de> for BTreeMap<K, V> {
    fn absent() -> Self {
        BTreeMap::new()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        r.vector(
            at,
            |map: &mut Self, Pair(k, v)| {
                map.entry(k).or_insert(v);
            },
            |_| Ok(BTreeMap::new()),
        )
    }
}

// ---------------------------------------------------------------------------------------
// std::set.

impl<T: Wire> Wire for BTreeSet<T> {
    const SHAPE: Shape = {
        assert!(T::SHAPE.is_plain(), "a set element cannot be a union");
        Shape::Object
    };
}

/// `std::set<T>`: a vector of its elements, in order.
impl<T: Encode> Encode for BTreeSet<T> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.vector_iter(self)
    }
}

/// Elements are inserted in wire order, in any order, and the first of equal elements wins.
impl<'de, T: Decode<'de> + Ord> Decode<'de> for BTreeSet<T> {
    fn absent() -> Self {
        BTreeSet::new()
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        r.vector(
            at,
            |set: &mut Self, item| {
                set.insert(item);
            },
            |_| Ok(BTreeSet::new()),
        )
    }
}

// ---------------------------------------------------------------------------------------
// std::unordered_map and std::unordered_set.

impl<K, V, S> Wire for HashMap<K, V, S> {
    const SHAPE: Shape = Shape::Object;
}

/// `std::unordered_map<K, V>`: the wire form of `std::map`, in the hash map's iteration
/// order (which is not canonical in C++ either).
impl<K: Encode, V: Encode, S> Encode for HashMap<K, V, S> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.vector_iter(self.iter().map(|(k, v)| PairRef(k, v)))
    }
}

/// The first entry for a key wins. Reserves at most 64 KiB before the entries decode.
impl<'de, K, V, S> Decode<'de> for HashMap<K, V, S>
where
    K: Decode<'de> + Eq + Hash,
    V: Decode<'de>,
    S: BuildHasher + Default,
{
    fn absent() -> Self {
        HashMap::with_hasher(S::default())
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        r.vector(
            at,
            |map: &mut Self, Pair(k, v)| {
                map.entry(k).or_insert(v);
            },
            |count| {
                Ok(HashMap::with_capacity_and_hasher(
                    preallocation::<(K, V)>(count),
                    S::default(),
                ))
            },
        )
    }
}

impl<T: Wire, S> Wire for HashSet<T, S> {
    const SHAPE: Shape = {
        assert!(T::SHAPE.is_plain(), "a set element cannot be a union");
        Shape::Object
    };
}

/// `std::unordered_set<T>`: a vector of its elements, in iteration order.
impl<T: Encode, S> Encode for HashSet<T, S> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.vector_iter(self)
    }
}

/// The first of equal elements wins. Reserves at most 64 KiB before the elements decode.
impl<'de, T, S> Decode<'de> for HashSet<T, S>
where
    T: Decode<'de> + Eq + Hash,
    S: BuildHasher + Default,
{
    fn absent() -> Self {
        HashSet::with_hasher(S::default())
    }

    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        r.vector(
            at,
            |set: &mut Self, item| {
                set.insert(item);
            },
            |count| {
                Ok(HashSet::with_capacity_and_hasher(
                    preallocation::<T>(count),
                    S::default(),
                ))
            },
        )
    }
}

/// `std::unordered_set<T>` composes `T`'s identifier with code 6
/// ([`Wrapper::UnorderedSet`]); no other map or set has an identifier.
impl<T: FileIdentified, S> FileIdentified for HashSet<T, S> {
    const FILE_IDENTIFIER: FileIdentifier = T::FILE_IDENTIFIER.compose(Wrapper::UnorderedSet);
}
