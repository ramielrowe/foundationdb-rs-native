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

// Go's math/rand stream (seeded with 1) is not reproducible from Rust, so the
// values below were decoded once from the fixture with the Python binding's
// tuple.py, an independent implementation. Integers are the
// rand.NewSource(1).Int63() sequence; floats are exact IEEE bit patterns.
const GO_INTEGERS: [i64; 20] = [
    5577006791947779410,
    8674665223082153551,
    6129484611666145821,
    4037200794235010051,
    3916589616287113937,
    6334824724549167320,
    605394647632969758,
    1443635317331776148,
    894385949183117216,
    2775422040480279449,
    4751997750760398084,
    7504504064263669287,
    1976235410884491574,
    3510942875414458836,
    2933568871211445515,
    4324745483838182873,
    2610529275472644968,
    2703387474910584091,
    6263450610539110790,
    2015796113853353331,
];
const GO_FLOATS: [u32; 20] = [
    0x3edcf198, 0x3f7fe77f, 0xbfc3115f, 0xbea2112b, 0x3ff1d9f7, 0x3f8ce4b2, 0xbf7e246b, 0x3f7d5da9,
    0xbf1d7f49, 0xbfb7af9e, 0xc009b123, 0x3e0ca594, 0x3ee2b9a6, 0xbf5899a4, 0xbda99071, 0x3e1fdfa0,
    0xbfb9ae06, 0x3e8f3a92, 0xbfde947c, 0x3f33e771,
];
const GO_DOUBLES: [u64; 20] = [
    0x3fd62723c1b6f6dd,
    0xbff11cdba3cb181a,
    0xbfeaa9ffe8699578,
    0x3fd52376175b0736,
    0x3ffbee8ee1d3faa0,
    0xbff1ee71e66d68b5,
    0x3fe837b2346c3b49,
    0x3fedac6dd7b20068,
    0xbff749c0762e56f1,
    0x3fef2318fffccd44,
    0xbfd2e4c7cf20d402,
    0x3fe052df057667ba,
    0xbfde377d4dee9078,
    0x3fd0193b69b7949a,
    0xbfb4f9daa88db968,
    0x3fc5561f19268128,
    0x3fd74c83e596dc08,
    0xbffa37dfb4f468c2,
    0x3feabdcb90f74ea6,
    0x3ff277babaceb8a4,
];
const GO_NESTED_INTS: [i64; 4] = [
    8603989663476771718,
    6842348953158377901,
    7388428680384065704,
    6735196588112087610,
];
const GO_NESTED_DOUBLES: [u64; 4] = [
    0x3fe580903870639c,
    0x3ff6b976fe5e3384,
    0xbfd79a10f91843dc,
    0xbfd526341ee49c36,
];

fn ints(values: &[i64]) -> Tuple {
    values.iter().map(|&v| Element::Int(v)).collect()
}

fn floats(bits: &[u32]) -> Tuple {
    bits.iter()
        .map(|&b| Element::Float(f32::from_bits(b)))
        .collect()
}

fn doubles(bits: &[u64]) -> Tuple {
    bits.iter()
        .map(|&b| Element::Double(f64::from_bits(b)))
        .collect()
}

#[test]
fn golden_random_entries_pack_identically() {
    let golden = load_golden();
    let nested = tuple![
        TEST_UUID,
        ints(&GO_NESTED_INTS),
        Element::Null,
        repeat(Element::Bytes(b"namespace".to_vec()), 4),
        Element::Null,
        doubles(&GO_NESTED_DOUBLES),
        Element::Null
    ];
    let cases = [
        ("Integers", ints(&GO_INTEGERS)),
        ("Floats", floats(&GO_FLOATS)),
        ("Doubles", doubles(&GO_DOUBLES)),
        ("Nested", nested),
    ];
    for (name, expected) in cases {
        assert_eq!(&pack(&expected).unwrap(), &golden[name], "pack {name}");
        assert_eq!(unpack(&golden[name]).unwrap(), expected, "unpack {name}");
    }
}
