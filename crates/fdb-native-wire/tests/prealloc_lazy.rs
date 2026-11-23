//! Hostile counts must not reserve memory in the new containers, views and iterator writer
//! (the same approach as `prealloc.rs`: the test runs itself again in a child process whose
//! address space is capped with `ulimit -v`, because the workspace forbids `unsafe` and so
//! a counting allocator).

#![cfg(target_os = "linux")]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::process::Command;

use fdb_native_wire::{
    Decode, DecodeError, EncodeError, ObjectRef, Reader, Shape, VectorView, Wire, Writer,
};

/// A 4 KiB table type, so an unchecked reservation of the count is 4 GiB.
#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Big([u64; 512]);

impl Wire for Big {
    const SHAPE: Shape = Shape::Table;
}

impl<'de> Decode<'de> for Big {
    fn absent() -> Self {
        Big([0; 512])
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        r.table(at)?;
        Ok(Big([0; 512]))
    }
}

/// Points at an object already written.
#[derive(Clone, Copy)]
struct Alias(ObjectRef);

impl Wire for Alias {
    const SHAPE: Shape = Shape::Object;
}

impl fdb_native_wire::Encode for Alias {
    fn encode_object(&self, _: &mut Writer) -> Result<ObjectRef, EncodeError> {
        Ok(self.0)
    }
}

const COUNT: usize = 1 << 20;

/// A root whose only field is a vector of `COUNT` zero offsets (each invalid).
fn zero_offsets() -> Vec<u8> {
    let mut w = Writer::new();
    let v = w.vector(&vec![0u32; COUNT]).unwrap();
    let mut root = w.table();
    root.field(&Alias(v)).unwrap();
    let root = root.finish().unwrap();
    w.finish(root, 1).unwrap()
}

fn child() {
    let bytes = zero_offsets();
    let r = Reader::new(&bytes).unwrap();

    // Views: a header over a million elements, then the first bad one.
    let view: VectorView<Big> = r.decode_root().unwrap();
    assert_eq!(view.len(), COUNT);
    assert!(view.get(0).is_err());
    let results: Vec<Result<Big, DecodeError>> = view.iter().collect();
    assert_eq!(results.len(), 1);
    assert!(view.iter().collect::<Result<Vec<Big>, _>>().is_err());
    let nested: VectorView<VectorView<Big>> = r.decode_root().unwrap();
    assert!(nested.iter().next().unwrap().is_err());

    // Containers: pair tables / sets of a table type, all invalid at element 0.
    assert!(r.decode_root::<BTreeMap<u32, Big>>().is_err());
    assert!(r.decode_root::<HashMap<u32, Big>>().is_err());
    assert!(r.decode_root::<HashMap<Big, Big>>().is_err());
    assert!(r.decode_root::<BTreeSet<Big>>().is_err());
    assert!(r.decode_root::<HashSet<Big>>().is_err());
    // Small elements that do decode: a million zeros is one entry.
    assert_eq!(r.decode_root::<HashSet<u32>>().unwrap().len(), 1);

    // The iterator writer reserves nothing for a count the iterator cannot meet.
    let mut w = Writer::new();
    assert!(matches!(
        w.vector_counted(1 << 30, 0..3u64),
        Err(EncodeError::InvalidShape(_))
    ));
    assert!(matches!(
        w.vector_counted(1 << 30, [Big([0; 512])].iter().map(|_| 0u64)),
        Err(EncodeError::InvalidShape(_))
    ));
}

#[test]
fn hostile_counts_do_not_reserve_memory() {
    if std::env::var_os("FDB_WIRE_PREALLOC_LAZY_CHILD").is_some() {
        child();
        return;
    }
    // 1 GiB of address space: plenty for the test, far below the 4 GiB an uncapped
    // reservation of the count would take.
    let exe = std::env::current_exe().unwrap();
    let status = Command::new("sh")
        .arg("-c")
        .arg(r#"ulimit -v 1048576 && exec "$0" --exact hostile_counts_do_not_reserve_memory --test-threads=1"#)
        .arg(exe)
        .env("FDB_WIRE_PREALLOC_LAZY_CHILD", "1")
        .status()
        .unwrap();
    assert!(
        status.success(),
        "decoding under a 1 GiB address-space limit failed: {status}"
    );
}
