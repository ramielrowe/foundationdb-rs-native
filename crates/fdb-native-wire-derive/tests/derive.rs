//! Behaviour of `#[derive(Wire)]` beyond what the wire crate's fixture tests cover (they
//! derive every engine type): empty tables vs. Arena-only tables, `skip`, `skip_slots`,
//! generics, the crate path override and unknown union tags.

use std::marker::PhantomData;

use fdb_native_wire::{
    Decode, DecodeError, EnsureTable, ErrorOr, FileIdentified, Reader, Shape, Uid, Void, Wire,
    Writer, decode, encode, encode_with_identifier,
};

#[derive(Wire, Debug, PartialEq)]
#[fdb(file_identifier = 0x000fdb0a)]
struct Inner<'a> {
    a: u32,
    b: &'a [u8],
}

#[test]
fn table_matches_the_readme_worked_example() {
    let bytes = encode(&Inner { a: 1, b: b"x" }).unwrap();
    // The README's worked example is 56 bytes with vtables {8, 12, 4, 8} and {6, 8, 4}.
    assert_eq!(bytes.len(), 56);
    assert_eq!(&bytes[4..8], &0x000f_db0au32.to_le_bytes());
    assert_eq!(decode::<Inner>(&bytes).unwrap(), Inner { a: 1, b: b"x" });
    assert_eq!(
        <ErrorOr<EnsureTable<Inner>> as FileIdentified>::FILE_IDENTIFIER.value(),
        0x020f_db0a
    );
}

// ---------------------------------------------------------------------------------------
// Empty tables and Arena.

/// `serializer(ar)`: an empty table, like `Void`.
#[derive(Wire, Debug, PartialEq, Default)]
#[fdb(file_identifier = 2010442)]
struct Unit;

#[derive(Wire, Debug, PartialEq, Default)]
#[fdb(file_identifier = 2010442)]
struct Braces {}

/// Only skipped fields: still `serializer(ar)`.
#[derive(Wire, Debug, PartialEq, Default)]
#[fdb(file_identifier = 2010442)]
struct OnlySkipped {
    #[fdb(skip)]
    cache: Vec<u8>,
}

/// `serializer(ar, arena)`: not empty; C++ follows the offset.
#[derive(Wire, Debug, PartialEq, Default)]
#[fdb(file_identifier = 2010442)]
struct OnlyArena {
    #[fdb(arena)]
    arena: (),
}

/// A message whose root field points at a table whose soffset is garbage: a reader that
/// follows the offset fails, one that treats it as an empty table does not.
fn broken_empty_table() -> Vec<u8> {
    let mut bytes = encode(&Void).unwrap();
    let good = decode::<Void>(&bytes);
    assert_eq!(good, Ok(Void));
    // Find the Void table: the root's only field points at it.
    let root_at = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let soffset = i32::from_le_bytes(bytes[root_at..root_at + 4].try_into().unwrap());
    let vtable = (root_at as i64 - i64::from(soffset)) as usize;
    let field_entry = u16::from_le_bytes(bytes[vtable + 4..vtable + 6].try_into().unwrap());
    let field_at = root_at + usize::from(field_entry);
    let table =
        field_at + u32::from_le_bytes(bytes[field_at..field_at + 4].try_into().unwrap()) as usize;
    bytes[table..table + 4].copy_from_slice(&0x7fff_0000i32.to_le_bytes());
    bytes
}

#[test]
fn empty_tables_are_not_followed_but_arena_tables_are() {
    let bytes = broken_empty_table();
    assert_eq!(decode::<Void>(&bytes), Ok(Void));
    assert_eq!(decode::<Unit>(&bytes), Ok(Unit));
    assert_eq!(decode::<Braces>(&bytes), Ok(Braces {}));
    assert_eq!(decode::<OnlySkipped>(&bytes), Ok(OnlySkipped::default()));
    assert!(matches!(
        decode::<OnlyArena>(&bytes),
        Err(DecodeError::OutOfBounds { .. } | DecodeError::Strict(_))
    ));

    // All of them write the same `{4, 4}` table as Void.
    let void = encode(&Void).unwrap();
    assert_eq!(encode(&Unit).unwrap(), void);
    assert_eq!(encode(&Braces {}).unwrap(), void);
    assert_eq!(
        encode(&OnlySkipped {
            cache: vec![1, 2, 3]
        })
        .unwrap(),
        void
    );
    assert_eq!(encode(&OnlyArena::default()).unwrap(), void);
    assert_eq!(decode::<OnlyArena>(&void), Ok(OnlyArena::default()));
}

// ---------------------------------------------------------------------------------------
// skip and skip_slots.

/// serializer(ar, a, b, c, d) with `b` (one slot) and `c` (a union, two slots) not modelled.
#[derive(Wire, Debug, PartialEq)]
#[fdb(file_identifier = 0x000fdb20)]
struct Full {
    a: i32,
    b: u64,
    c: Option<u32>,
    d: u16,
}

#[derive(Wire, Debug, PartialEq)]
#[fdb(file_identifier = 0x000fdb20)]
struct Partial {
    a: i32,
    #[fdb(skip_slots = 1)]
    _b: (),
    #[fdb(skip)]
    local: String,
    #[fdb(skip_slots = 2)]
    _c: (),
    d: u16,
}

#[test]
fn skip_slots_step_over_unmodelled_fields() {
    let full = Full {
        a: -5,
        b: 9,
        c: Some(3),
        d: 7,
    };
    let partial: Partial = decode(&encode(&full).unwrap()).unwrap();
    assert_eq!(
        partial,
        Partial {
            a: -5,
            _b: (),
            local: String::new(),
            _c: (),
            d: 7,
        }
    );

    // Written back, the unmodelled slots are absent and read as defaults.
    let partial = Partial {
        local: "not on the wire".into(),
        ..partial
    };
    let full: Full = decode(&encode(&partial).unwrap()).unwrap();
    assert_eq!(
        full,
        Full {
            a: -5,
            b: 0,
            c: None,
            d: 7,
        }
    );
    assert_eq!(Partial::absent().local, "");
}

// ---------------------------------------------------------------------------------------
// Tuple structs, structs and unions.

#[derive(Wire, Debug, PartialEq)]
#[fdb(file_identifier = 0x000fdb20)]
struct FullTuple(i32, u64, Option<u32>, u16);

#[test]
fn tuple_structs_are_tables() {
    let bytes = encode(&FullTuple(1, 2, None, 4)).unwrap();
    assert_eq!(
        decode::<Full>(&bytes).unwrap(),
        Full {
            a: 1,
            b: 2,
            c: None,
            d: 4
        }
    );
}

#[derive(Wire, Clone, Copy, Debug, PartialEq)]
#[fdb(struct)]
struct Triple {
    a: i16,
    #[fdb(skip)]
    local: u8,
    b: bool,
    c: i64,
}

#[derive(Wire, Clone, Copy, Debug, PartialEq, Default)]
#[fdb(struct)]
struct Nothing;

#[test]
fn structs_match_tuples() {
    assert_eq!(<Triple as Wire>::SHAPE, <(i16, bool, i64) as Wire>::SHAPE);
    const { assert!(<Triple as Wire>::STRUCT) };
    assert_eq!(
        <Nothing as Wire>::SHAPE,
        Shape::Inline { size: 1, align: 1 }
    );

    let t = Triple {
        a: -2,
        local: 9,
        b: true,
        c: 1 << 40,
    };
    let bytes = encode_with_identifier(&vec![t, t], 1).unwrap();
    let tuples: Vec<(i16, bool, i64)> = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(tuples, vec![(-2, true, 1 << 40); 2]);
    let back: Vec<Triple> = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(back, vec![Triple { local: 0, ..t }; 2]);
}

#[derive(Wire, Debug, PartialEq)]
enum V3 {
    I32(i32),
    F64(f64),
    Inner(EnsureTable<Option<u8>>),
}

#[derive(Wire, Debug, PartialEq)]
enum V2 {
    I32(i32),
    F64(f64),
}

#[test]
fn unions_use_index_plus_one_and_read_unknown_tags_as_absent() {
    for v in [V3::I32(-1), V3::F64(0.5), V3::Inner(EnsureTable(Some(4)))] {
        let bytes = encode_with_identifier(&v, 1).unwrap();
        let back: V3 = Reader::new(&bytes).unwrap().decode_root().unwrap();
        assert_eq!(back, v);
    }
    // Tag 3 is past V2's last alternative: alternative 0's absent value, as in C++.
    let bytes = encode_with_identifier(&V3::Inner(EnsureTable(None)), 1).unwrap();
    let v2: V2 = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(v2, V2::I32(0));
    // Tag 2 is F64 in both.
    let bytes = encode_with_identifier(&V3::F64(2.0), 1).unwrap();
    let v2: V2 = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(v2, V2::F64(2.0));
    assert_eq!(V3::absent(), V3::I32(0));
}

// ---------------------------------------------------------------------------------------
// Generics.

#[derive(Wire, Debug, PartialEq)]
struct Generic<'a, T, const N: usize> {
    items: Vec<T>,
    fixed: [u8; N],
    name: &'a [u8],
    #[fdb(skip)]
    marker: PhantomData<fn() -> T>,
}

#[derive(Wire, Debug, PartialEq)]
enum GenericUnion<A, B> {
    A(A),
    B(B),
}

/// A type parameter only used in a skipped field needs no `Wire` bound.
#[derive(Wire, Debug, PartialEq)]
struct Tagged<T> {
    token: Uid,
    #[fdb(skip)]
    reply: PhantomData<T>,
}

#[derive(Debug, PartialEq)]
struct NotWire;

#[test]
fn generic_types() {
    let g: Generic<'_, Uid, 2> = Generic {
        items: vec![Uid::new(1, 2)],
        fixed: [3, 4],
        name: b"n",
        marker: PhantomData,
    };
    let bytes = encode_with_identifier(&g, 1).unwrap();
    let back: Generic<'_, Uid, 2> = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(back, g);

    let u: GenericUnion<u8, Inner<'_>> = GenericUnion::B(Inner { a: 1, b: b"b" });
    let bytes = encode_with_identifier(&u, 1).unwrap();
    let back: GenericUnion<u8, Inner<'_>> = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(back, u);

    let t: Tagged<NotWire> = Tagged {
        token: Uid::new(5, 6),
        reply: PhantomData,
    };
    let bytes = encode_with_identifier(&t, 1).unwrap();
    let back: Tagged<NotWire> = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(back, t);
}

#[test]
fn borrowed_fields_borrow_from_the_buffer() {
    let bytes = encode(&Inner {
        a: 1,
        b: b"borrowed",
    })
    .unwrap();
    let inner: Inner<'_> = decode(&bytes).unwrap();
    let range = bytes.as_ptr_range();
    assert!(range.contains(&inner.b.as_ptr()));
}

// ---------------------------------------------------------------------------------------
// The crate path.

mod renamed {
    pub(crate) use fdb_native_wire as wire;
}

#[derive(fdb_native_wire::Wire, Debug, PartialEq)]
#[fdb(crate = "renamed::wire", file_identifier = ID)]
struct Renamed {
    x: u8,
}

const ID: u32 = 0x00ab_cdef;

#[test]
fn crate_path_and_constant_file_identifiers() {
    assert_eq!(Renamed::FILE_IDENTIFIER.value(), ID);
    let bytes = encode(&Renamed { x: 3 }).unwrap();
    assert_eq!(decode::<Renamed>(&bytes).unwrap(), Renamed { x: 3 });
}

/// Hand-written code can still use the derived impls piecewise.
#[test]
fn derived_tables_nest_in_hand_written_ones() {
    let mut w = Writer::new();
    let mut root = w.table();
    root.field(&Inner { a: 9, b: b"" }).unwrap();
    root.field(&V2::F64(1.5)).unwrap();
    let root = root.finish().unwrap();
    let bytes = w.finish(root, 1).unwrap();
    let r = Reader::new(&bytes).unwrap();
    let mut t = r.root().unwrap();
    assert_eq!(t.field::<Inner<'_>>().unwrap(), Inner { a: 9, b: b"" });
    assert_eq!(t.field::<V2>().unwrap(), V2::F64(1.5));
}

// ---------------------------------------------------------------------------------------
// Hygiene.

/// The derive inside a `macro_rules!` whose field types arrive as `ident`, `ty` and `tt`
/// fragments (review finding: tokens spanned at such a type cannot see the impl's
/// locals or `self`).
mod from_macro_rules {
    use fdb_native_wire::Wire;

    macro_rules! make_ident {
        ($table:ident, $union:ident, $strukt:ident, $t:ident) => {
            #[derive(Wire, Debug, PartialEq)]
            #[fdb(file_identifier = 0x000fdb30)]
            pub(crate) struct $table {
                pub(crate) a: $t,
                pub(crate) b: Vec<$t>,
            }

            #[derive(Wire, Debug, PartialEq)]
            pub(crate) enum $union {
                A($t),
                B(Vec<$t>),
            }

            #[derive(Wire, Clone, Copy, Debug, PartialEq)]
            #[fdb(struct)]
            pub(crate) struct $strukt {
                pub(crate) a: $t,
                pub(crate) b: u8,
            }
        };
    }

    macro_rules! make_ty {
        ($table:ident, $t:ty) => {
            #[derive(Wire, Debug, PartialEq)]
            pub(crate) struct $table {
                pub(crate) a: $t,
                pub(crate) b: u8,
            }
        };
    }

    macro_rules! make_tt {
        ($table:ident, $field:ident, $($t:tt)*) => {
            #[derive(Wire, Debug, PartialEq)]
            pub(crate) struct $table {
                pub(crate) $field: $($t)*,
            }
        };
    }

    make_ident!(Table, Union, Struct, u32);
    make_ty!(TableTy, Vec<u16>);
    make_tt!(TableTt, zzz, Option<u64>);
}

/// Names the generated code must not depend on (it uses absolute paths, and `__fdb_`
/// locals that cannot collide with field names).
mod shadowed {
    #![allow(dead_code, non_camel_case_types)]

    use ::fdb_native_wire::Wire;

    struct Result;
    struct Ok;
    struct Err;
    struct Some;
    struct None;
    struct Default;
    struct Option;
    struct Box;
    struct Vec;
    mod fdb_native_wire {}
    mod core {}

    #[derive(Wire, Debug, PartialEq)]
    pub(crate) struct Collide {
        pub(crate) __fdb_t: u8,
        pub(crate) __fdb_w: u8,
        pub(crate) __fdb_r: u8,
        pub(crate) __fdb_f0: u8,
        pub(crate) __fdb_at: u8,
        pub(crate) r#type: u8,
        #[fdb(skip)]
        pub(crate) skipped: u8,
    }

    #[derive(Wire, Debug, PartialEq)]
    pub(crate) enum Tags {
        __fdb_v(u8),
        __fdb_tag(u16),
    }
}

#[test]
fn derive_is_hygienic() {
    use from_macro_rules::*;

    let t = Table {
        a: 1,
        b: vec![2, 3],
    };
    assert_eq!(decode::<Table>(&encode(&t).unwrap()).unwrap(), t);
    for u in [Union::A(4), Union::B(vec![5])] {
        let bytes = encode_with_identifier(&u, 1).unwrap();
        let back: Union = Reader::new(&bytes).unwrap().decode_root().unwrap();
        assert_eq!(back, u);
    }
    assert_eq!(<Struct as Wire>::SHAPE, <(u32, u8) as Wire>::SHAPE);
    let s = Struct { a: 6, b: 7 };
    let bytes = encode_with_identifier(&vec![s], 1).unwrap();
    let back: Vec<Struct> = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(back, vec![s]);

    let t = TableTy { a: vec![1], b: 2 };
    let bytes = encode_with_identifier(&t, 1).unwrap();
    let back: TableTy = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(back, t);
    let t = TableTt { zzz: Some(9) };
    let bytes = encode_with_identifier(&t, 1).unwrap();
    let back: TableTt = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(back, t);

    let c = shadowed::Collide {
        __fdb_t: 1,
        __fdb_w: 2,
        __fdb_r: 3,
        __fdb_f0: 4,
        __fdb_at: 5,
        r#type: 6,
        skipped: 0,
    };
    let bytes = encode_with_identifier(&c, 1).unwrap();
    let back: shadowed::Collide = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(back, c);
    let u = shadowed::Tags::__fdb_tag(7);
    let bytes = encode_with_identifier(&u, 1).unwrap();
    let back: shadowed::Tags = Reader::new(&bytes).unwrap().decode_root().unwrap();
    assert_eq!(back, u);
}
