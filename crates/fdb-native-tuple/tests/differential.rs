//! Differential test against vectors generated from the FoundationDB Python
//! binding's `tuple.py` (see `tests/fixtures/gen_vectors.py` for the generator
//! and the line format).
//!
//! Each case is checked in both directions: `pack(value) == bytes` and
//! `unpack(bytes) == value`. Floats are compared by bit pattern.

use fdb_native_tuple::{Element, Subspace, Tuple, Uuid, Versionstamp, pack, unpack};
use num_bigint::BigInt;

const VECTORS: &str = include_str!("fixtures/python_vectors.txt");

fn unhex(s: &str) -> Vec<u8> {
    assert!(s.len() % 2 == 0, "odd hex length: {s}");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn array<const N: usize>(s: &str) -> [u8; N] {
    unhex(s).try_into().unwrap()
}

fn parse_tuple<'a>(tokens: &mut impl Iterator<Item = &'a str>, top: bool) -> Tuple {
    let mut out = Vec::new();
    while let Some(tok) = tokens.next() {
        let (tag, val) = tok.split_once(':').unwrap_or((tok, ""));
        out.push(match tag {
            ")" if !top => return Tuple::from_elements(out),
            "(" => Element::Tuple(parse_tuple(tokens, false)),
            "N" => Element::Null,
            "B" => Element::Bytes(unhex(val)),
            "S" => Element::String(String::from_utf8(unhex(val)).unwrap()),
            "I" => Element::from_bigint(BigInt::parse_bytes(val.as_bytes(), 10).unwrap()),
            "F" => Element::Float(f32::from_be_bytes(array(val))),
            "D" => Element::Double(f64::from_be_bytes(array(val))),
            "Z" => Element::Bool(val == "1"),
            "U" => Element::Uuid(Uuid::from_bytes(array(val))),
            "V" => Element::Versionstamp(Versionstamp::from_bytes(array(val))),
            other => panic!("bad token {other:?}"),
        });
    }
    assert!(top, "unterminated nested tuple");
    Tuple::from_elements(out)
}

#[test]
fn python_vectors() {
    let (mut plain, mut stamped) = (0, 0);
    for (n, line) in VECTORS.lines().enumerate() {
        let mut fields = line.splitn(4, '\t');
        let (kind, prefix, packed, toks) = (
            fields.next().unwrap(),
            unhex(fields.next().unwrap()),
            unhex(fields.next().unwrap()),
            fields.next().unwrap(),
        );
        let tuple = parse_tuple(&mut toks.split_whitespace(), true);
        let ctx = format!("line {}: {line}", n + 1);
        let space = Subspace::new(prefix.clone());
        match kind {
            "P" => {
                plain += 1;
                assert_eq!(pack(&tuple).unwrap(), packed, "pack {ctx}");
                assert_eq!(space.pack(&tuple).unwrap(), [&prefix[..], &packed].concat());
                assert_eq!(unpack(&packed).unwrap(), tuple, "unpack {ctx}");
                // Bit-exact, not just equal (NaN payloads, -0.0).
                assert_eq!(
                    pack(&unpack(&packed).unwrap()).unwrap(),
                    packed,
                    "repack {ctx}"
                );
            }
            "V" => {
                stamped += 1;
                assert!(tuple.has_incomplete_versionstamp(), "{ctx}");
                assert_eq!(
                    space.pack_with_versionstamp(&tuple).unwrap(),
                    packed,
                    "pack {ctx}"
                );
                // Without the 4-byte offset suffix the key unpacks back to the tuple,
                // with the all-0xff transaction version read as incomplete.
                let body = &packed[prefix.len()..packed.len() - 4];
                assert_eq!(
                    space.unpack(&packed[..packed.len() - 4]).unwrap(),
                    tuple,
                    "unpack {ctx}"
                );
                assert_eq!(unpack(body).unwrap(), tuple, "unpack body {ctx}");
            }
            other => panic!("bad kind {other:?}"),
        }
    }
    assert!(
        plain > 1000 && stamped >= 200,
        "{plain} plain, {stamped} stamped"
    );
}
