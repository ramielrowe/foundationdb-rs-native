//! Property tests: round trips, packed-order equivalence, and robustness on
//! arbitrary input.

use fdb_native_tuple::{Element, Subspace, Tuple, Uuid, Versionstamp, pack, unpack};
use num_bigint::{BigInt, Sign};
use proptest::prelude::*;

fn arb_bigint() -> impl Strategy<Value = BigInt> {
    prop_oneof![
        // Random magnitude up to 40 bytes, either sign.
        (any::<bool>(), proptest::collection::vec(any::<u8>(), 0..40)).prop_map(|(neg, mag)| {
            BigInt::from_bytes_be(if neg { Sign::Minus } else { Sign::Plus }, &mag)
        }),
        // Values hugging the 8-byte and i64/u64 boundaries.
        (-3i32..=3, 0u32..=72, any::<bool>()).prop_map(|(delta, shift, neg)| {
            let v = (BigInt::from(1) << shift) + delta;
            if neg { -v } else { v }
        }),
    ]
}

fn arb_leaf() -> impl Strategy<Value = Element> {
    prop_oneof![
        Just(Element::Null),
        proptest::collection::vec(prop_oneof![Just(0u8), Just(0xff), any::<u8>()], 0..12)
            .prop_map(Element::Bytes),
        "[\\x00a-c\u{ff}\u{20ac}\u{1f600}]{0,8}".prop_map(Element::String),
        any::<i64>().prop_map(Element::Int),
        prop_oneof![Just(i64::MIN), Just(i64::MAX), Just(0), Just(-1), Just(1)]
            .prop_map(Element::Int),
        arb_bigint().prop_map(Element::from_bigint),
        any::<u64>().prop_map(Element::from),
        any::<u32>().prop_map(|b| Element::Float(f32::from_bits(b))),
        any::<u64>().prop_map(|b| Element::Double(f64::from_bits(b))),
        prop_oneof![
            Just(Element::Float(0.0)),
            Just(Element::Float(-0.0)),
            Just(Element::Float(f32::NAN)),
            Just(Element::Float(f32::INFINITY)),
            Just(Element::Float(f32::NEG_INFINITY)),
            Just(Element::Double(0.0)),
            Just(Element::Double(-0.0)),
            Just(Element::Double(f64::NAN)),
            Just(Element::Double(-f64::NAN)),
            Just(Element::Double(f64::INFINITY)),
            Just(Element::Double(f64::NEG_INFINITY)),
        ],
        any::<bool>().prop_map(Element::Bool),
        any::<[u8; 16]>().prop_map(|b| Element::Uuid(Uuid::from_bytes(b))),
        (any::<[u8; 10]>(), any::<u16>())
            .prop_map(|(t, u)| Element::Versionstamp(Versionstamp::complete(t, u))),
    ]
}

fn arb_element() -> impl Strategy<Value = Element> {
    arb_leaf().prop_recursive(3, 24, 4, |inner| {
        proptest::collection::vec(inner, 0..4).prop_map(|v| Element::Tuple(Tuple::from_elements(v)))
    })
}

fn arb_tuple() -> impl Strategy<Value = Tuple> {
    proptest::collection::vec(arb_element(), 0..5).prop_map(Tuple::from_elements)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn unpack_pack_round_trips(t in arb_tuple()) {
        let packed = pack(&t).unwrap();
        let back = unpack(&packed).unwrap();
        prop_assert_eq!(&back, &t);
        // Bit-exact: re-packing gives identical bytes (NaN payloads included).
        prop_assert_eq!(pack(&back).unwrap(), packed);
    }

    #[test]
    fn order_matches_packed_order(a in arb_tuple(), b in arb_tuple()) {
        let (pa, pb) = (pack(&a).unwrap(), pack(&b).unwrap());
        prop_assert_eq!(a.cmp(&b), pa.cmp(&pb));
        prop_assert_eq!(a == b, pa == pb);
    }

    #[test]
    fn element_order_matches_packed_order(a in arb_element(), b in arb_element()) {
        let pa = pack(&(a.clone(),)).unwrap();
        let pb = pack(&(b.clone(),)).unwrap();
        prop_assert_eq!(a.cmp(&b), pa.cmp(&pb));
    }

    #[test]
    fn subspace_round_trips(prefix in arb_tuple(), t in arb_tuple()) {
        let space = Subspace::from_tuple(&prefix).unwrap();
        let key = space.pack(&t).unwrap();
        prop_assert!(space.contains(&key));
        prop_assert_eq!(space.unpack(&key).unwrap(), t.clone());
        let (begin, end) = space.range(&t).unwrap();
        prop_assert!(key < begin && begin < end);
        let child = space.subspace(&t).unwrap();
        prop_assert_eq!(child.bytes(), &key[..]);
        prop_assert!(begin.starts_with(&key) && end.starts_with(&key));
    }

    #[test]
    fn unpack_arbitrary_bytes_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
        if let Ok(t) = unpack(&bytes) {
            // Whatever decodes must also encode and round-trip.
            let repacked = pack(&t).unwrap();
            prop_assert_eq!(unpack(&repacked).unwrap(), t);
        }
    }

    #[test]
    fn unpack_structured_garbage_never_panics(
        bytes in proptest::collection::vec(
            prop_oneof![
                Just(0x00u8), Just(0x01), Just(0x02), Just(0x05), Just(0x0b), Just(0x1d),
                Just(0x0c), Just(0x1c), Just(0x20), Just(0x21), Just(0x30), Just(0x33),
                Just(0xff), any::<u8>(),
            ],
            0..96,
        )
    ) {
        if let Ok(t) = unpack(&bytes) {
            let repacked = pack(&t).unwrap();
            prop_assert_eq!(unpack(&repacked).unwrap(), t);
        }
    }

    #[test]
    fn truncated_valid_input_never_panics(t in arb_tuple(), cut in 0usize..64) {
        let packed = pack(&t).unwrap();
        let cut = cut.min(packed.len());
        let _ = unpack(&packed[..cut]);
    }

    #[test]
    fn subspace_unpack_arbitrary_never_panics(key in proptest::collection::vec(any::<u8>(), 0..32)) {
        let space = Subspace::new(vec![0x01, 0x02]);
        let _ = space.unpack(&key);
    }
}
