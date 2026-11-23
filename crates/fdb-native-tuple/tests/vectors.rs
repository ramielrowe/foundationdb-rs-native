//! Fixed test vectors from the specification and edge cases.

use fdb_native_tuple::{
    Element, Error, MAX_DEPTH, Subspace, Tuple, Uuid, Versionstamp, pack, pack_with_versionstamp,
    range, tuple, unpack,
};
use num_bigint::BigInt;

fn p(t: Tuple) -> Vec<u8> {
    pack(&t).unwrap()
}

#[test]
fn spec_vectors() {
    assert_eq!(p(tuple![b"foo\x00bar"]), b"\x01foo\x00\xffbar\x00");
    assert_eq!(
        p(tuple!["F\u{d4}O\x00bar"]),
        b"\x02F\xc3\x94O\x00\xffbar\x00"
    );
    assert_eq!(p(tuple![-5_551_212]), b"\x11\xabK\x93");
    assert_eq!(p(tuple![-42.0f32]), b"\x20\x3d\xd7\xff\xff");
    let nested = tuple![Tuple::from_elements(vec![
        Element::Bytes(b"foo\x00bar".to_vec()),
        Element::Null,
        Element::Tuple(Tuple::new()),
    ])];
    assert_eq!(p(nested), b"\x05\x01foo\x00\xffbar\x00\x00\xff\x05\x00\x00");
}

#[test]
fn empty_and_null_nested_tuples() {
    // The empty tuple is 05 00; the tuple containing only null is 05 00 ff 00.
    assert_eq!(p(tuple![Tuple::new()]), b"\x05\x00");
    assert_eq!(
        p(tuple![Tuple::new().with(Element::Null)]),
        b"\x05\x00\xff\x00"
    );
    assert_eq!(p(tuple![Element::Null]), b"\x00");
    assert!(tuple![Tuple::new()] < tuple![Tuple::new().with(Element::Null)]);
    assert_eq!(unpack(b"\x05\x00").unwrap(), tuple![Tuple::new()]);
    assert_eq!(unpack(b"").unwrap(), Tuple::new());
    assert_eq!(p(Tuple::new()), b"");
}

#[test]
fn small_integers() {
    assert_eq!(p(tuple![0]), b"\x14");
    assert_eq!(p(tuple![1]), b"\x15\x01");
    assert_eq!(p(tuple![-1]), b"\x13\xfe");
    assert_eq!(p(tuple![255]), b"\x15\xff");
    assert_eq!(p(tuple![256]), b"\x16\x01\x00");
    assert_eq!(p(tuple![-255]), b"\x13\x00");
    assert_eq!(p(tuple![-256]), b"\x12\xfe\xff");
    assert_eq!(p(tuple![i64::MAX]), b"\x1c\x7f\xff\xff\xff\xff\xff\xff\xff");
    assert_eq!(p(tuple![i64::MIN]), b"\x0c\x7f\xff\xff\xff\xff\xff\xff\xff");
}

#[test]
fn integers_around_64_bit_boundaries() {
    // u64::MAX and -(2^64 - 1) use the 8-byte codes (`1c ff*8` / `0c 00*8`), as the
    // specification, Java and Go do. The Python binding instead emits the long
    // forms `1d 08 ff*8` / `0b f7 00*8` for exactly these two values; that is a
    // known divergence we deliberately do not copy. Decoding accepts both.
    let umax = tuple![u64::MAX];
    assert_eq!(
        unpack(b"\x1d\x08\xff\xff\xff\xff\xff\xff\xff\xff").unwrap()[0],
        Element::BigInt(BigInt::from(u64::MAX))
    );
    assert_eq!(
        unpack(b"\x0b\xf7\x00\x00\x00\x00\x00\x00\x00\x00").unwrap()[0],
        Element::BigInt(-BigInt::from(u64::MAX))
    );
    assert_eq!(p(umax.clone()), b"\x1c\xff\xff\xff\xff\xff\xff\xff\xff");
    assert_eq!(
        unpack(&p(umax)).unwrap()[0],
        Element::BigInt(BigInt::from(u64::MAX))
    );

    // i64::MAX + 1 is the first value that is not a plain Int.
    let first_big = u64::try_from(i64::MAX).unwrap() + 1;
    assert!(matches!(Element::from(first_big), Element::BigInt(_)));
    assert!(matches!(Element::from(first_big - 1), Element::Int(_)));
    assert_eq!(
        unpack(b"\x1c\x80\x00\x00\x00\x00\x00\x00\x00").unwrap()[0],
        Element::BigInt(BigInt::from(first_big))
    );
    assert_eq!(
        unpack(b"\x1c\x7f\xff\xff\xff\xff\xff\xff\xff").unwrap()[0],
        Element::Int(i64::MAX)
    );

    // The most negative 8-byte value, -(2^64 - 1), uses 0x0c with zero payload.
    let neg_max = -BigInt::from(u64::MAX);
    let t = tuple![neg_max.clone()];
    assert_eq!(p(t.clone()), b"\x0c\x00\x00\x00\x00\x00\x00\x00\x00");
    assert_eq!(unpack(&p(t)).unwrap()[0], Element::BigInt(neg_max));

    // i64::MIN - 1 needs 0x0c with 0x7f..fe.
    let below = BigInt::from(i64::MIN) - 1u32;
    assert_eq!(
        p(tuple![below.clone()]),
        b"\x0c\x7f\xff\xff\xff\xff\xff\xff\xfe"
    );
    assert_eq!(
        unpack(&p(tuple![below.clone()])).unwrap()[0],
        Element::BigInt(below)
    );
}

#[test]
fn arbitrary_precision_integers() {
    let two64 = BigInt::from(1u32) << 64u32;
    assert_eq!(
        p(tuple![two64.clone()]),
        b"\x1d\x09\x01\x00\x00\x00\x00\x00\x00\x00\x00"
    );
    assert_eq!(
        p(tuple![-two64.clone()]),
        b"\x0b\xf6\xfe\xff\xff\xff\xff\xff\xff\xff\xff"
    );
    for v in [
        two64.clone(),
        -two64.clone(),
        BigInt::from(1u32) << 1000u32,
        -(BigInt::from(1u32) << 1000u32),
    ] {
        let packed = pack(&(v.clone(),));
        if v.bits() > 8 * 255 {
            assert!(matches!(packed, Err(Error::IntegerTooLarge { .. })));
        } else {
            assert_eq!(unpack(&packed.unwrap()).unwrap()[0], Element::BigInt(v));
        }
    }
    // 255 bytes is the maximum magnitude.
    let max = (BigInt::from(1u32) << (8u32 * 255)) - 1u32;
    assert!(pack(&(max.clone(),)).is_ok());
    assert!(pack(&(-max,)).is_ok());
    let too_big = BigInt::from(1u32) << (8u32 * 255);
    assert_eq!(
        pack(&(too_big,)),
        Err(Error::IntegerTooLarge { bytes: 256 })
    );
    // An un-normalised BigInt packs and compares like the equivalent Int.
    let sloppy = Element::BigInt(BigInt::from(5));
    assert_eq!(pack(&(sloppy.clone(),)).unwrap(), b"\x15\x05");
    assert_eq!(sloppy, Element::Int(5));
    // Big ints that fit in i64 decode to Int, even if encoded in long form.
    assert_eq!(unpack(b"\x1d\x02\x01\x00").unwrap()[0], Element::Int(256));
    assert_eq!(unpack(b"\x15\x00").unwrap()[0], Element::Int(0));
}

#[test]
fn floats() {
    assert_eq!(p(tuple![1.0f32]), b"\x20\xbf\x80\x00\x00");
    assert_eq!(p(tuple![0.0f64]), b"\x21\x80\x00\x00\x00\x00\x00\x00\x00");
    assert_eq!(p(tuple![-0.0f64]), b"\x21\x7f\xff\xff\xff\xff\xff\xff\xff");
    assert!(Element::Double(-0.0) < Element::Double(0.0));
    assert!(Element::Float(-0.0) < Element::Float(0.0));
    assert!(Element::Double(f64::NEG_INFINITY) < Element::Double(-f64::MAX));
    assert!(Element::Double(f64::MAX) < Element::Double(f64::INFINITY));
    // NaN is not canonicalised: it round-trips by bit pattern and is equal to itself.
    let nan = f64::from_bits(0x7ff8_0000_0000_1234);
    let packed = p(tuple![nan]);
    match &unpack(&packed).unwrap()[0] {
        Element::Double(d) => assert_eq!(d.to_bits(), nan.to_bits()),
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(Element::Double(nan), Element::Double(nan));
    assert!(Element::Double(f64::NAN) > Element::Double(f64::INFINITY));
    assert!(Element::Double(-f64::NAN) < Element::Double(f64::NEG_INFINITY));
    // Float and double are distinct types, ordered by type code.
    assert!(Element::Float(1e30) < Element::Double(-1e30));
}

#[test]
fn type_ordering_follows_type_codes() {
    let ordered = [
        Element::Null,
        Element::Bytes(vec![]),
        Element::String(String::new()),
        Element::Tuple(Tuple::new()),
        Element::Int(i64::MIN),
        Element::Int(0),
        Element::BigInt(BigInt::from(1u32) << 70u32),
        Element::Float(0.0),
        Element::Double(0.0),
        Element::Bool(false),
        Element::Bool(true),
        Element::Uuid(Uuid::default()),
        Element::Versionstamp(Versionstamp::complete([0; 10], 0)),
    ];
    for w in ordered.windows(2) {
        assert!(w[0] < w[1], "{:?} < {:?}", w[0], w[1]);
        assert!(p(tuple![w[0].clone()]) < p(tuple![w[1].clone()]));
    }
}

#[test]
fn strings_and_bytes_escape_nulls() {
    assert_eq!(p(tuple!["a\x00b"]), b"\x02a\x00\xffb\x00");
    assert_eq!(p(tuple![&b"\x00\x00"[..]]), b"\x01\x00\xff\x00\xff\x00");
    // Nulls inside a nested byte string are escaped too; a nested null element is 00 ff.
    let inner = Tuple::new().with(&b"\x00"[..]).with(Element::Null);
    assert_eq!(
        p(tuple![inner.clone()]),
        b"\x05\x01\x00\xff\x00\x00\xff\x00"
    );
    assert_eq!(unpack(&p(tuple![inner.clone()])).unwrap(), tuple![inner]);
}

#[test]
fn uuid_and_bool() {
    let u = Uuid::from_bytes(*b"0123456789abcdef");
    assert_eq!(p(tuple![u]), b"\x300123456789abcdef");
    assert_eq!(p(tuple![false, true]), b"\x26\x27");
    assert_eq!(unpack(b"\x26\x27").unwrap(), tuple![false, true]);
}

#[test]
fn versionstamps() {
    let complete = Versionstamp::complete([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0x0102);
    assert_eq!(
        p(tuple![complete]),
        b"\x33\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x01\x02"
    );
    assert_eq!(
        unpack(&p(tuple![complete])).unwrap()[0],
        Element::Versionstamp(complete)
    );

    // Plain pack refuses incomplete versionstamps.
    let inc = Versionstamp::incomplete(0x0304);
    assert_eq!(pack(&("a", inc)), Err(Error::IncompleteVersionstamp));
    assert_eq!(range(&(inc,)), Err(Error::IncompleteVersionstamp));

    // Offset points at the first byte of the transaction version.
    let key = pack_with_versionstamp(&("a", inc)).unwrap();
    let mut expected = b"\x02a\x00\x33".to_vec();
    expected.extend_from_slice(&[0xff; 10]);
    expected.extend_from_slice(&[0x03, 0x04]);
    expected.extend_from_slice(&4u32.to_le_bytes());
    assert_eq!(key, expected);

    // Inside a nested tuple the position accounts for the nested type code.
    let nested = tuple![Tuple::new().with(inc)];
    let key = pack_with_versionstamp(&nested).unwrap();
    assert_eq!(&key[key.len() - 4..], &2u32.to_le_bytes());

    assert_eq!(
        pack_with_versionstamp(&("a", 1)),
        Err(Error::NoIncompleteVersionstamp)
    );
    assert_eq!(
        pack_with_versionstamp(&("a", complete)),
        Err(Error::NoIncompleteVersionstamp)
    );
    assert_eq!(
        pack_with_versionstamp(&(inc, inc)),
        Err(Error::MultipleIncompleteVersionstamps)
    );
    // A complete stamp alongside one incomplete stamp is fine.
    assert!(pack_with_versionstamp(&(complete, inc)).is_ok());
}

#[test]
fn versionstamp_unset_tr_version_is_incomplete() {
    // The bindingtester feeds exactly these bytes to TUPLE_PACK_WITH_VERSIONSTAMP.
    let bytes = b"\x02a\x00\x33\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x05";
    let t = unpack(bytes).unwrap();
    assert_eq!(t[1], Element::Versionstamp(Versionstamp::incomplete(5)));
    assert!(t.has_incomplete_versionstamp());
    let key = pack_with_versionstamp(&t).unwrap();
    assert_eq!(&key[..bytes.len()], bytes);
    assert_eq!(&key[bytes.len()..], &4u32.to_le_bytes());

    // Stripping the offset suffix from pack_with_versionstamp output round-trips.
    let t = tuple![
        "x",
        Tuple::new().with(1).with(Versionstamp::incomplete(0x0102)),
        2
    ];
    let key = pack_with_versionstamp(&t).unwrap();
    assert_eq!(unpack(&key[..key.len() - 4]).unwrap(), t);

    // Plain pack refuses a transaction version of all 0xff, however it was built.
    assert_eq!(
        pack(&(Versionstamp::complete([0xff; 10], 0),)),
        Err(Error::IncompleteVersionstamp)
    );
}

#[test]
fn versionstamp_ordering() {
    let a = Versionstamp::complete([0; 10], 5);
    let b = Versionstamp::complete([0; 10], 6);
    let inc = Versionstamp::incomplete(0);
    assert!(a < b && b < inc);
    assert_eq!(a, a);
    // All-0xff transaction version is the incomplete marker, as in the other bindings.
    assert_eq!(inc, Versionstamp::complete([0xff; 10], 0));
    assert!(!Versionstamp::complete([0xff; 10], 3).is_complete());
    assert_eq!(Versionstamp::complete([0xff; 10], 3).tr_version(), None);
    let mut almost = [0xff; 10];
    almost[9] = 0xfe;
    assert!(Versionstamp::complete(almost, 0).is_complete());
    assert!(b < inc);
    assert!(a.is_complete() && !inc.is_complete());
    assert_eq!(Versionstamp::from_bytes(a.to_bytes()), a);
}

#[test]
fn range_bounds() {
    let (b, e) = range(&("a", 1)).unwrap();
    assert_eq!(b, b"\x02a\x00\x15\x01\x00");
    assert_eq!(e, b"\x02a\x00\x15\x01\xff");
    let (b, e) = range(&()).unwrap();
    assert_eq!((b, e), (vec![0x00], vec![0xff]));
}

#[test]
fn rust_tuple_conversions() {
    let t: Tuple = [
        Element::from(1i32),
        Element::from(2u64),
        Element::from("s"),
        Element::from(String::from("t")),
        Element::from(&b"b"[..]),
        Element::from(vec![1u8]),
        Element::from(true),
        Element::from(1.5f32),
        Element::from(2.5f64),
        Element::from(None::<i32>),
        Element::from(Some(3u8)),
    ]
    .into_iter()
    .collect();
    assert_eq!(t.len(), 11);
    assert_eq!(
        pack(&(
            1i32,
            2u64,
            "s",
            String::from("t"),
            &b"b"[..],
            vec![1u8],
            true,
            1.5f32
        ))
        .unwrap(),
        pack(&t[..8]).unwrap()
    );
    assert_eq!(pack(&(1,)).unwrap(), pack(&tuple![1]).unwrap());
    let twelve = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    assert_eq!(pack(&twelve).unwrap().len(), 24);
}

#[test]
fn unpack_errors() {
    type Case<'a> = (&'a [u8], fn(&Error) -> bool);
    let cases: &[Case] = &[
        (b"\x01abc", |e| matches!(e, Error::UnexpectedEnd { .. })),
        (b"\x02", |e| matches!(e, Error::UnexpectedEnd { .. })),
        (b"\x05\x01a\x00", |e| {
            matches!(e, Error::UnexpectedEnd { .. })
        }),
        (b"\x16\x01", |e| matches!(e, Error::UnexpectedEnd { .. })),
        (b"\x1d", |e| matches!(e, Error::UnexpectedEnd { .. })),
        (b"\x1d\x05\x01", |e| {
            matches!(e, Error::UnexpectedEnd { .. })
        }),
        (b"\x0b\xf0\x01", |e| {
            matches!(e, Error::UnexpectedEnd { .. })
        }),
        (b"\x20\x00\x00", |e| {
            matches!(e, Error::UnexpectedEnd { .. })
        }),
        (b"\x21", |e| matches!(e, Error::UnexpectedEnd { .. })),
        (b"\x30\x00", |e| matches!(e, Error::UnexpectedEnd { .. })),
        (b"\x33\x00", |e| matches!(e, Error::UnexpectedEnd { .. })),
        (b"\x02\xff\x00", |e| matches!(e, Error::InvalidUtf8 { .. })),
        (b"\x03\x04", |e| {
            matches!(e, Error::UnknownTypeCode { code: 0x03, .. })
        }),
        (b"\xff", |e| {
            matches!(e, Error::UnknownTypeCode { code: 0xff, .. })
        }),
        (b"\x40", |e| {
            matches!(e, Error::UnknownTypeCode { code: 0x40, .. })
        }),
        (b"\x14\x25", |e| {
            matches!(
                e,
                Error::UnknownTypeCode {
                    code: 0x25,
                    offset: 1
                }
            )
        }),
    ];
    for (bytes, check) in cases {
        let err = unpack(bytes).unwrap_err();
        assert!(check(&err), "{bytes:?} gave {err:?}");
        assert!(!err.to_string().is_empty());
    }
}

#[test]
fn nesting_depth_limit() {
    let mut deep = Tuple::new();
    for _ in 0..MAX_DEPTH {
        deep = Tuple::new().with(deep);
    }
    // MAX_DEPTH levels of nesting inside the top level is accepted ...
    let packed = pack(&deep).unwrap();
    assert_eq!(unpack(&packed).unwrap(), deep);
    // ... one more is rejected on both sides.
    let too_deep = Tuple::new().with(deep);
    assert_eq!(pack(&too_deep), Err(Error::NestingTooDeep));
    let mut hostile = vec![0x05; 100_000];
    hostile.push(0x00);
    assert_eq!(unpack(&hostile), Err(Error::NestingTooDeep));
}

#[test]
fn subspace_basics() {
    let root = Subspace::new(b"\x01root\x00".to_vec());
    assert_eq!(root.bytes(), b"\x01root\x00");
    let key = root.pack(&("a", 7)).unwrap();
    assert_eq!(key, b"\x01root\x00\x02a\x00\x15\x07");
    assert!(root.contains(&key));
    assert!(!root.contains(b"\x01roo"));
    assert_eq!(root.unpack(&key).unwrap(), tuple!["a", 7]);
    assert_eq!(root.unpack(b"\x01root\x00").unwrap(), Tuple::new());
    assert_eq!(root.unpack(b"\x02other"), Err(Error::KeyNotInSubspace));
    assert!(matches!(
        root.unpack(b"\x01root\x00\x02a"),
        Err(Error::UnexpectedEnd { .. })
    ));

    let child = root.subspace(&("a",)).unwrap();
    assert_eq!(child.pack(&(7,)).unwrap(), key);
    assert_eq!(Subspace::from_tuple(&("x",)).unwrap().bytes(), b"\x02x\x00");

    let (b, e) = root.range(&("a",)).unwrap();
    assert_eq!(b, b"\x01root\x00\x02a\x00\x00");
    assert_eq!(e, b"\x01root\x00\x02a\x00\xff");
    assert!(b < key && key < e);
    let (b, e) = root.range_all();
    assert_eq!(b, b"\x01root\x00\x00");
    assert_eq!(e, b"\x01root\x00\xff");
    assert_eq!(Subspace::default().bytes(), b"");
}

#[test]
fn subspace_versionstamp_offset_includes_prefix() {
    let root = Subspace::new(b"pre".to_vec());
    let key = root
        .pack_with_versionstamp(&("a", Versionstamp::incomplete(1)))
        .unwrap();
    // "pre" (3) + 02 61 00 (3) + 33 (1) = 7
    assert_eq!(&key[key.len() - 4..], &7u32.to_le_bytes());
    assert_eq!(&key[..3], b"pre");
    assert_eq!(
        root.pack(&(Versionstamp::incomplete(1),)),
        Err(Error::IncompleteVersionstamp)
    );
}

#[test]
fn accessors_and_hash() {
    use std::collections::HashSet;
    assert!(Element::Null.is_null());
    assert_eq!(Element::from("x").as_str(), Some("x"));
    assert_eq!(Element::from(&b"x"[..]).as_bytes(), Some(&b"x"[..]));
    assert_eq!(Element::from(5).as_i64(), Some(5));
    assert_eq!(Element::BigInt(BigInt::from(5)).as_i64(), Some(5));
    assert_eq!(Element::from(u64::MAX).as_i64(), None);
    assert!(Element::from(tuple![1]).as_tuple().is_some());
    let mut set = HashSet::new();
    set.insert(Element::Int(5));
    assert!(set.contains(&Element::BigInt(BigInt::from(5))));
    set.insert(Element::Double(f64::NAN));
    assert!(set.contains(&Element::Double(f64::NAN)));
    assert!(!tuple![Versionstamp::complete([0; 10], 0)].has_incomplete_versionstamp());
    assert!(tuple![Tuple::new().with(Versionstamp::incomplete(0))].has_incomplete_versionstamp());
}
