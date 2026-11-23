//! A vector's element count comes from the message; decoding must not reserve memory for
//! it before the elements decode (review finding: a 4 MiB message decoded as a vector of
//! 4 KiB structs reserved 4 GiB, then failed on element 0).
//!
//! The workspace forbids `unsafe`, so a counting global allocator is not an option. The test
//! instead runs itself again in a child process whose address space is capped with
//! `ulimit -v`: a multi-GiB reservation makes the child abort.

#![cfg(target_os = "linux")]

use std::process::Command;

use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, ObjectRef, Reader, Shape, Uid, Wire, Writer,
};

/// A 4 KiB table type (an interface struct full of endpoint tokens, say).
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

/// A root whose only field is a vector of `COUNT` unions with tag 1 and offset 0.
fn zero_union_offsets() -> Vec<u8> {
    let mut w = Writer::new();
    let offsets = w.vector(&vec![0u32; COUNT]).unwrap();
    let tags = w.vector(&vec![1u8; COUNT]).unwrap();
    let mut root = w.table();
    root.field(&Alias(tags)).unwrap();
    root.field(&Alias(offsets)).unwrap();
    let root = root.finish().unwrap();
    w.finish(root, 1).unwrap()
}

fn child() {
    let bytes = zero_offsets();
    let r = Reader::new(&bytes).unwrap();
    assert!(r.decode_root::<Vec<Big>>().is_err());
    let bytes = zero_union_offsets();
    let r = Reader::new(&bytes).unwrap();
    assert!(r.decode_root::<Vec<Option<Big>>>().is_err());
}

#[test]
fn large_counts_do_not_preallocate() {
    if std::env::var_os("FDB_WIRE_PREALLOC_CHILD").is_some() {
        child();
        return;
    }
    // 1 GiB of address space: plenty for the test (a few tens of MiB), far below the 4 GiB
    // an uncapped `Vec::with_capacity(COUNT)` of `Big` would reserve.
    let exe = std::env::current_exe().unwrap();
    let status = Command::new("sh")
        .arg("-c")
        .arg(r#"ulimit -v 1048576 && exec "$0" --exact large_counts_do_not_preallocate --test-threads=1"#)
        .arg(exe)
        .env("FDB_WIRE_PREALLOC_CHILD", "1")
        .status()
        .unwrap();
    assert!(
        status.success(),
        "decoding under a 1 GiB address-space limit failed: {status}"
    );
}
