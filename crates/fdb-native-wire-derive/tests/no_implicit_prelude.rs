//! The generated code names everything by absolute path, so it compiles without the
//! prelude (and with the wire crate under another name, via `#[fdb(crate = ...)]`).

#![no_implicit_prelude]
#![allow(dead_code)]

extern crate core;
extern crate fdb_native_wire as fw;
extern crate std;

use fw::Wire;

#[derive(Wire, std::fmt::Debug, core::cmp::PartialEq)]
#[fdb(crate = "fw", file_identifier = 5)]
struct Table {
    a: u8,
    b: u64,
}

#[derive(Wire)]
#[fdb(crate = "fw", file_identifier = 6)]
struct Borrowed<'a> {
    a: &'a [u8],
    #[fdb(arena)]
    arena: (),
}

#[derive(Wire)]
#[fdb(crate = "fw")]
struct Empty;

#[derive(Wire, core::clone::Clone, core::marker::Copy)]
#[fdb(crate = "fw", struct)]
struct Inline {
    a: u8,
    b: u32,
}

#[derive(Wire)]
#[fdb(crate = "fw")]
enum Union {
    A(u8),
    B(Inline),
}

#[derive(Wire)]
#[fdb(crate = "fw")]
struct Mixed {
    #[fdb(skip_slots = 2)]
    unmodelled: (),
    union: Union,
    #[fdb(skip)]
    local: u8,
}

#[test]
fn derives_without_the_prelude() {
    let value = Table { a: 1, b: 2 };
    let bytes = fw::encode(&value).unwrap();
    std::assert_eq!(fw::decode::<Table>(&bytes).unwrap(), value);
    std::assert_eq!(<Inline as fw::Wire>::SHAPE, <(u8, u32) as fw::Wire>::SHAPE);
    let _ = <Borrowed<'static> as fw::Wire>::SHAPE;
    let _ = <Empty as fw::Wire>::SHAPE;
    let _ = <Mixed as fw::Wire>::SHAPE;
}
