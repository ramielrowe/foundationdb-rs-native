//! Checks against the Go binding's `tuples.golden` fixture.
//!
//! The fixture is a Go `encoding/gob` stream of a `map[string][]byte`; a
//! minimal decoder for just that shape lives here. See
//! `tests/fixtures/README.md` for provenance.

use std::collections::BTreeMap;

use fdb_native_tuple::{Element, Error, Tuple, Uuid, pack, tuple, unpack};

const GOLDEN: &[u8] = include_bytes!("fixtures/tuples.golden");

struct Gob<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Gob<'a> {
    fn byte(&mut self) -> u8 {
        let b = self.data[self.pos];
        self.pos += 1;
        b
    }

    /// Gob unsigned integer: one byte below 128, else a negated byte count
    /// followed by that many big-endian bytes.
    fn uint(&mut self) -> u64 {
        let b = self.byte();
        if b < 128 {
            return u64::from(b);
        }
        let n = usize::from(b.wrapping_neg());
        let mut v = 0u64;
        for _ in 0..n {
            v = (v << 8) | u64::from(self.byte());
        }
        v
    }

    fn int(&mut self) -> i64 {
        let u = self.uint();
        if u & 1 == 1 {
            !((u >> 1) as i64)
        } else {
            (u >> 1) as i64
        }
    }

    fn bytes(&mut self) -> &'a [u8] {
        let n = self.uint() as usize;
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        s
    }
}

fn load_golden() -> BTreeMap<String, Vec<u8>> {
    let mut gob = Gob {
        data: GOLDEN,
        pos: 0,
    };
    let mut out = BTreeMap::new();
    while gob.pos < GOLDEN.len() {
        let len = gob.uint() as usize;
        let end = gob.pos + len;
        let type_id = gob.int();
        if type_id < 0 {
            // Type definition message; skip.
            gob.pos = end;
            continue;
        }
        assert_eq!(gob.byte(), 0, "singleton value marker");
        let count = gob.uint();
        for _ in 0..count {
            let key = String::from_utf8(gob.bytes().to_vec()).unwrap();
            let value = gob.bytes().to_vec();
            out.insert(key, value);
        }
        assert_eq!(gob.pos, end);
    }
    out
}

const TEST_UUID: Uuid = Uuid::from_bytes([
    0x11, 0x00, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x00, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
]);

fn repeat(e: Element, n: usize) -> Tuple {
    Tuple::from_elements(vec![e; n])
}

/// Entries whose tuples are fully deterministic in the Go test.
fn deterministic() -> Vec<(&'static str, Tuple)> {
    vec![
        (
            "Simple",
            tuple![TEST_UUID, "foobarbaz", 1234, Element::Null],
        ),
        (
            "Namespaces",
            tuple![TEST_UUID, "github", "com", "apple", "foundationdb", "tree"],
        ),
        ("ManyStrings", repeat("namespace".into(), 8)),
        (
            "ManyBytes",
            repeat(Element::Bytes(b"namespace".to_vec()), 20),
        ),
        (
            "ManyBytesNil",
            repeat(Element::Bytes(vec![0xFF, 0x00, 0xFF]), 20),
        ),
        (
            "LargeBytes",
            tuple![TEST_UUID, Element::Bytes(b"abcd".repeat(20))],
        ),
        (
            "LargeBytesNil",
            tuple![TEST_UUID, Element::Bytes([0xFF, 0x00, 0xFF].repeat(20))],
        ),
        (
            "UUIDs",
            tuple![
                TEST_UUID, true, TEST_UUID, false, TEST_UUID, true, TEST_UUID, false, TEST_UUID,
                true
            ],
        ),
    ]
}

#[test]
fn golden_has_expected_entries() {
    let golden = load_golden();
    let names: Vec<&str> = golden.keys().map(String::as_str).collect();
    // BTreeMap iterates in sorted order.
    assert_eq!(
        names,
        [
            "Doubles",
            "Floats",
            "Integers",
            "LargeBytes",
            "LargeBytesNil",
            "ManyBytes",
            "ManyBytesNil",
            "ManyStrings",
            "ManyStringsNil",
            "Namespaces",
            "Nested",
            "NilCases",
            "Simple",
            "UUIDs",
        ]
    );
}

#[test]
fn golden_deterministic_entries_pack_identically() {
    let golden = load_golden();
    for (name, tuple) in deterministic() {
        let expected = &golden[name];
        assert_eq!(&pack(&tuple).unwrap(), expected, "pack mismatch for {name}");
        assert_eq!(
            &unpack(expected).unwrap(),
            &tuple,
            "unpack mismatch for {name}"
        );
    }
}

/// Go strings are arbitrary bytes, and the Go test packs `"nam\x00es\xFFpace"`,
/// `"\x00\xFF"` and `"\xFF\x00"` with the raw byte 0xFF, which is not valid
/// UTF-8. A Rust `String` cannot hold those, so `unpack` rejects them with a
/// typed error. To still verify every byte of those two entries, re-read them
/// with the string type code (0x02) swapped for the byte-string code (0x01).
#[test]
fn golden_entries_with_non_utf8_strings() {
    let golden = load_golden();
    let as_bytes = |name: &str| -> Tuple {
        let swapped: Vec<u8> = golden[name]
            .iter()
            .map(|&b| if b == 0x02 { 0x01 } else { b })
            .collect();
        unpack(&swapped).unwrap()
    };

    for name in ["ManyStringsNil", "NilCases"] {
        assert!(
            matches!(unpack(&golden[name]), Err(Error::InvalidUtf8 { .. })),
            "{name} must be rejected as invalid UTF-8"
        );
    }

    let expected_many = repeat(Element::Bytes(b"nam\x00es\xFFpace".to_vec()), 8);
    assert_eq!(as_bytes("ManyStringsNil"), expected_many);

    let expected_nil: Tuple = [&b"\x00"[..], b"\x00\xFF", b"\x00\x00\x00", b"\xFF\x00", b""]
        .into_iter()
        .collect();
    assert_eq!(as_bytes("NilCases"), expected_nil);
    assert_eq!(
        &pack(&expected_nil)
            .unwrap()
            .iter()
            .map(|&b| if b == 0x01 { 0x02 } else { b })
            .collect::<Vec<u8>>(),
        &golden["NilCases"]
    );
}

#[test]
fn golden_random_entries_round_trip() {
    let golden = load_golden();

    let ints = unpack(&golden["Integers"]).unwrap();
    assert_eq!(ints.len(), 20);
    assert!(ints.iter().all(|e| matches!(e, Element::Int(v) if *v >= 0)));
    assert_eq!(&pack(&ints).unwrap(), &golden["Integers"]);

    let floats = unpack(&golden["Floats"]).unwrap();
    assert_eq!(floats.len(), 20);
    assert!(floats.iter().all(|e| matches!(e, Element::Float(_))));
    assert_eq!(&pack(&floats).unwrap(), &golden["Floats"]);

    let doubles = unpack(&golden["Doubles"]).unwrap();
    assert_eq!(doubles.len(), 20);
    assert!(doubles.iter().all(|e| matches!(e, Element::Double(_))));
    assert_eq!(&pack(&doubles).unwrap(), &golden["Doubles"]);

    let nested = unpack(&golden["Nested"]).unwrap();
    assert_eq!(nested.len(), 7);
    assert_eq!(nested[0], Element::Uuid(TEST_UUID));
    assert!(nested[2].is_null() && nested[4].is_null() && nested[6].is_null());
    let ints = nested[1].as_tuple().unwrap();
    assert_eq!(ints.len(), 4);
    assert!(ints.iter().all(|e| matches!(e, Element::Int(_))));
    assert_eq!(
        nested[3],
        Element::Tuple(repeat(Element::Bytes(b"namespace".to_vec()), 4))
    );
    let doubles = nested[5].as_tuple().unwrap();
    assert_eq!(doubles.len(), 4);
    assert!(doubles.iter().all(|e| matches!(e, Element::Double(_))));
    assert_eq!(&pack(&nested).unwrap(), &golden["Nested"]);
}
