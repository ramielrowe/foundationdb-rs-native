//! Test types shared by the map/set tests: the engine's `Containers` table (see
//! `tools/golden-vectors/src/engine_vectors.cpp`) with real `std::map`/`std::set`
//! equivalents instead of `support`'s sorted-`Vec` stand-ins, and `Dump` for the
//! containers. Needs `mod support;` in the including test.

#![allow(dead_code, unreachable_pub)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, FileIdentified, FileIdentifier, ObjectRef, Reader,
    Shape, Wire, Writer,
};
use proptest::prelude::*;
use serde_json::Value;

use crate::support::{Arb, Bytes, Dump, node, table_value};

impl<K: Dump + Wire, V: Dump + Wire> Dump for BTreeMap<K, V> {
    fn type_name() -> String {
        format!("vector<{}>", pair_name::<K, V>())
    }
    fn node(&self) -> Value {
        let entries = self
            .iter()
            .map(|(k, v)| {
                node(
                    pair_name::<K, V>(),
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

fn pair_name<K: Dump, V: Dump>() -> String {
    format!("table<pair<{},{}>>", K::type_name(), V::type_name())
}

impl<T: Dump> Dump for BTreeSet<T> {
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

/// `Containers` with `m` a `std::map<int32_t, std::string>` and `s` a
/// `std::set<uint32_t>`.
#[derive(Clone, Debug, PartialEq)]
pub struct MapContainers {
    pub m: BTreeMap<i32, Bytes>,
    pub s: BTreeSet<u32>,
    pub nested: Vec<Vec<u16>>,
}

impl Wire for MapContainers {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for MapContainers {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.m)?;
        t.field(&self.s)?;
        t.field(&self.nested)?;
        t.finish()
    }
}

impl<'de> Decode<'de> for MapContainers {
    fn absent() -> Self {
        MapContainers {
            m: Decode::absent(),
            s: Decode::absent(),
            nested: Decode::absent(),
        }
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(MapContainers {
            m: t.field()?,
            s: t.field()?,
            nested: t.field()?,
        })
    }
}

impl FileIdentified for MapContainers {
    const FILE_IDENTIFIER: FileIdentifier = FileIdentifier::new(0x000f_db05);
}

impl Dump for MapContainers {
    fn type_name() -> String {
        "table<Containers>".into()
    }
    fn node(&self) -> Value {
        node(
            Self::type_name(),
            table_value(vec![
                ("m", 1, self.m.node()),
                ("s", 1, self.s.node()),
                ("nested", 1, self.nested.node()),
            ]),
        )
    }
}

impl Arb for MapContainers {
    fn arb() -> BoxedStrategy<Self> {
        (
            prop::collection::btree_map(any::<i32>(), Bytes::arb(), 0..8),
            prop::collection::btree_set(any::<u32>(), 0..8),
            Vec::<Vec<u16>>::arb(),
        )
            .prop_map(|(m, s, nested)| MapContainers { m, s, nested })
            .boxed()
    }
}

/// `Containers` with `m` a `std::unordered_map` and `s` a `std::unordered_set`: the same
/// wire form in hash order, which the C++ reader sorts into its `std::map`.
#[derive(Clone, Debug, PartialEq)]
pub struct HashContainers {
    pub m: HashMap<i32, Bytes>,
    pub s: HashSet<u32>,
    pub nested: Vec<Vec<u16>>,
}

impl HashContainers {
    pub fn sorted(&self) -> MapContainers {
        MapContainers {
            m: self.m.iter().map(|(k, v)| (*k, v.clone())).collect(),
            s: self.s.iter().copied().collect(),
            nested: self.nested.clone(),
        }
    }
}

impl Wire for HashContainers {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for HashContainers {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.m)?;
        t.field(&self.s)?;
        t.field(&self.nested)?;
        t.finish()
    }
}

impl<'de> Decode<'de> for HashContainers {
    fn absent() -> Self {
        HashContainers {
            m: Decode::absent(),
            s: Decode::absent(),
            nested: Decode::absent(),
        }
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(HashContainers {
            m: t.field()?,
            s: t.field()?,
            nested: t.field()?,
        })
    }
}

impl FileIdentified for HashContainers {
    const FILE_IDENTIFIER: FileIdentifier = FileIdentifier::new(0x000f_db05);
}

impl Dump for HashContainers {
    fn type_name() -> String {
        MapContainers::type_name()
    }
    /// What the C++ reader dumps: its sorted `std::map` and `std::set`.
    fn node(&self) -> Value {
        self.sorted().node()
    }
}

impl Arb for HashContainers {
    fn arb() -> BoxedStrategy<Self> {
        (
            prop::collection::hash_map(any::<i32>(), Bytes::arb(), 0..8),
            prop::collection::hash_set(any::<u32>(), 0..8),
            Vec::<Vec<u16>>::arb(),
        )
            .prop_map(|(m, s, nested)| HashContainers { m, s, nested })
            .boxed()
    }
}
