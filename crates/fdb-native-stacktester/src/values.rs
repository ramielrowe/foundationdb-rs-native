//! Stack values: conversions of the popped items, and the instructions that only compute
//! (`SUB`, `CONCAT`, the tuple and float instructions), as the Python tester does them.

use fdb_native_tuple::{Element, Subspace, Tuple};
use num_bigint::BigInt;

use crate::Fatal;

/// `RESULT_NOT_PRESENT`: a future with no value, or a `GET` of a key that is not set.
pub(crate) const RESULT_NOT_PRESENT: &[u8] = b"RESULT_NOT_PRESENT";

/// A byte string.
pub(crate) fn bytes(b: &[u8]) -> Element {
    Element::Bytes(b.to_vec())
}

/// The packed tuple `(b"ERROR", b"<code>")` an FDB error becomes on the stack
/// (`fdb.tuple.pack((b"ERROR", str(e.code).encode("ascii")))`).
pub(crate) fn error_value(code: u16) -> Element {
    let tuple = Tuple::from_elements(vec![
        bytes(b"ERROR"),
        Element::Bytes(code.to_string().into_bytes()),
    ]);
    Element::Bytes(tuple.pack().expect("a tuple of two byte strings packs"))
}

/// The operand of an instruction that wants a byte string (keys, values, prefixes).
pub(crate) fn into_bytes(value: Element) -> Result<Vec<u8>, Fatal> {
    match value {
        Element::Bytes(b) => Ok(b),
        other => Err(Fatal::new(format!("expected a byte string, got {other:?}"))),
    }
}

/// An integer operand (a Python `bool` is an `int` too).
pub(crate) fn to_i64(value: &Element) -> Result<i64, Fatal> {
    match value {
        Element::Int(i) => Ok(*i),
        Element::Bool(b) => Ok(i64::from(*b)),
        other => Err(Fatal::new(format!(
            "expected an integer that fits in 64 bits, got {other:?}"
        ))),
    }
}

/// An integer operand that must fit an `i32` (selector offsets, row limits).
pub(crate) fn to_i32(value: &Element) -> Result<i32, Fatal> {
    let i = to_i64(value)?;
    i32::try_from(i).map_err(|_| Fatal::new(format!("{i} does not fit in 32 bits")))
}

/// A count of items to pop.
pub(crate) fn to_count(value: &Element) -> Result<usize, Fatal> {
    let i = to_i64(value)?;
    usize::try_from(i).map_err(|_| Fatal::new(format!("invalid item count {i}")))
}

/// A flag (`or_equal`, `reverse`): Python's truth value of an integer.
pub(crate) fn truthy(value: &Element) -> Result<bool, Fatal> {
    match value {
        Element::Int(i) => Ok(*i != 0),
        Element::Bool(b) => Ok(*b),
        Element::BigInt(_) => Ok(true),
        other => Err(Fatal::new(format!(
            "expected an integer flag, got {other:?}"
        ))),
    }
}

fn to_bigint(value: &Element) -> Result<BigInt, Fatal> {
    match value {
        Element::Int(i) => Ok(BigInt::from(*i)),
        Element::Bool(b) => Ok(BigInt::from(u8::from(*b))),
        Element::BigInt(i) => Ok(i.clone()),
        other => Err(Fatal::new(format!("expected an integer, got {other:?}"))),
    }
}

/// `SUB`: `a - b` in arbitrary precision (Python integers).
pub(crate) fn sub(a: &Element, b: &Element) -> Result<Element, Fatal> {
    Ok(Element::from_bigint(to_bigint(a)? - to_bigint(b)?))
}

/// `CONCAT`: `a + b` for two byte strings, two unicode strings (or two tuples, as Python's
/// `+` would).
pub(crate) fn concat(a: Element, b: Element) -> Result<Element, Fatal> {
    match (a, b) {
        (Element::Bytes(mut a), Element::Bytes(b)) => {
            a.extend_from_slice(&b);
            Ok(Element::Bytes(a))
        }
        (Element::String(mut a), Element::String(b)) => {
            a.push_str(&b);
            Ok(Element::String(a))
        }
        (Element::Tuple(a), Element::Tuple(b)) => {
            let mut items = a.into_vec();
            items.extend(b.into_vec());
            Ok(Element::Tuple(Tuple::from_elements(items)))
        }
        (a, b) => Err(Fatal::new(format!("cannot CONCAT {a:?} and {b:?}"))),
    }
}

/// `fdb.tuple.pack(items)`; a tuple Python cannot pack (an incomplete versionstamp, ...) is a
/// failure of the tester (a `ValueError` in Python).
pub(crate) fn pack(items: Vec<Element>) -> Result<Vec<u8>, Fatal> {
    Tuple::from_elements(items)
        .pack()
        .map_err(|e| Fatal::new(format!("tuple pack: {e}")))
}

/// `fdb.tuple.unpack(packed)`.
pub(crate) fn unpack(packed: &[u8]) -> Result<Tuple, Fatal> {
    Tuple::unpack(packed).map_err(|e| Fatal::new(format!("tuple unpack: {e}")))
}

/// `TUPLE_PACK_WITH_VERSIONSTAMP`: what to push, in order: `OK` and the packed key, or
/// `ERROR: NONE` / `ERROR: MULTIPLE`.
pub(crate) fn pack_with_versionstamp(prefix: &[u8], items: Vec<Element>) -> Vec<Element> {
    let tuple = Tuple::from_elements(items);
    if !tuple.has_incomplete_versionstamp() {
        // Python pushes this one way or the other (at random, it tries to pack first).
        return vec![bytes(b"ERROR: NONE")];
    }
    match Subspace::new(prefix.to_vec()).pack_with_versionstamp(&tuple) {
        Ok(packed) => vec![bytes(b"OK"), Element::Bytes(packed)],
        Err(fdb_native_tuple::Error::NoIncompleteVersionstamp) => vec![bytes(b"ERROR: NONE")],
        // Python's `except ValueError`: any other error is "multiple".
        Err(_) => vec![bytes(b"ERROR: MULTIPLE")],
    }
}

/// `TUPLE_SORT`: the packed tuples, unpacked and sorted by their packed form, packed again
/// (in ascending order: the caller pushes them so that the greatest ends on top).
pub(crate) fn sort_packed(items: Vec<Element>) -> Result<Vec<Vec<u8>>, Fatal> {
    let mut packed = items
        .into_iter()
        .map(|item| {
            let tuple = unpack(&into_bytes(item)?)?;
            tuple
                .pack()
                .map_err(|e| Fatal::new(format!("tuple pack: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    packed.sort();
    Ok(packed)
}

/// `TUPLE_RANGE`: `(pack(items) + 0x00, pack(items) + 0xff)`.
pub(crate) fn tuple_range(items: Vec<Element>) -> Result<(Vec<u8>, Vec<u8>), Fatal> {
    Tuple::from_elements(items)
        .range()
        .map_err(|e| Fatal::new(format!("tuple range: {e}")))
}

/// `ENCODE_FLOAT`: 4 big-endian bytes to a single-precision float.
pub(crate) fn encode_float(value: Element) -> Result<Element, Fatal> {
    let b = into_bytes(value)?;
    let b: [u8; 4] = b
        .try_into()
        .map_err(|b: Vec<u8>| Fatal::new(format!("ENCODE_FLOAT of {} bytes", b.len())))?;
    Ok(Element::Float(f32::from_be_bytes(b)))
}

/// `ENCODE_DOUBLE`: 8 big-endian bytes to a double.
pub(crate) fn encode_double(value: Element) -> Result<Element, Fatal> {
    let b = into_bytes(value)?;
    let b: [u8; 8] = b
        .try_into()
        .map_err(|b: Vec<u8>| Fatal::new(format!("ENCODE_DOUBLE of {} bytes", b.len())))?;
    Ok(Element::Double(f64::from_be_bytes(b)))
}

/// `DECODE_FLOAT`: a single-precision float to its 4 big-endian bytes.
pub(crate) fn decode_float(value: Element) -> Result<Element, Fatal> {
    match value {
        Element::Float(f) => Ok(Element::Bytes(f.to_be_bytes().to_vec())),
        other => Err(Fatal::new(format!("DECODE_FLOAT of {other:?}"))),
    }
}

/// `DECODE_DOUBLE`: a double (or an integer, as `struct.pack(">d", n)` takes) to its 8
/// big-endian bytes.
pub(crate) fn decode_double(value: Element) -> Result<Element, Fatal> {
    let d = match value {
        Element::Double(d) => d,
        #[allow(clippy::cast_precision_loss)]
        Element::Int(i) => i as f64,
        other => return Err(Fatal::new(format!("DECODE_DOUBLE of {other:?}"))),
    };
    Ok(Element::Bytes(d.to_be_bytes().to_vec()))
}

/// The `GET_KEY` result as the tester pushes it: the key if it starts with `prefix`, else
/// `prefix` if the key sorts before it, else `strinc(prefix)` (a failure of the tester if
/// `prefix` has no successor, as Python's `strinc` raises).
pub(crate) fn clamp_to_prefix(key: Vec<u8>, prefix: &[u8]) -> Result<Vec<u8>, Fatal> {
    if key.starts_with(prefix) {
        Ok(key)
    } else if key.as_slice() < prefix {
        Ok(prefix.to_vec())
    } else {
        strinc(prefix)
    }
}

/// `strinc(prefix)`: the first key after every key that starts with `prefix`.
pub(crate) fn strinc(prefix: &[u8]) -> Result<Vec<u8>, Fatal> {
    fdb_native_client::strinc(prefix).ok_or_else(|| {
        Fatal::new(format!(
            "strinc({prefix:?}): the key must contain a byte other than 0xff"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fdb_native_tuple::Versionstamp;

    #[test]
    fn error_value_is_the_packed_error_tuple() {
        assert_eq!(error_value(1020), bytes(b"\x01ERROR\x00\x011020\x00"));
    }

    #[test]
    fn sub_uses_arbitrary_precision() {
        let max = Element::Int(i64::MAX);
        let r = sub(&max, &Element::Int(-1)).unwrap();
        assert_eq!(r, Element::from_bigint(BigInt::from(i64::MAX) + 1));
        assert_eq!(sub(&r, &Element::Int(1)).unwrap(), max);
        assert_eq!(
            sub(&Element::Int(0xFFFF_FFFF), &Element::Int(-100)).unwrap(),
            Element::Int(0xFFFF_FFFF + 100)
        );
        assert!(sub(&bytes(b"a"), &Element::Int(1)).is_err());
    }

    #[test]
    fn concat_strings_and_bytes() {
        assert_eq!(concat(bytes(b"ab"), bytes(b"cd")).unwrap(), bytes(b"abcd"));
        assert_eq!(
            concat(Element::String("a".into()), Element::String("b".into())).unwrap(),
            Element::String("ab".into())
        );
        assert!(concat(bytes(b"a"), Element::String("b".into())).is_err());
    }

    #[test]
    fn floats_round_trip_their_bits() {
        let nan = bytes(&[0x7f, 0x80, 0x00, 0x01]);
        let f = encode_float(nan.clone()).unwrap();
        assert_eq!(decode_float(f).unwrap(), nan);
        let d = bytes(&(-0.0f64).to_be_bytes());
        assert_eq!(decode_double(encode_double(d.clone()).unwrap()).unwrap(), d);
        assert!(encode_float(bytes(b"abc")).is_err());
        assert!(decode_float(Element::Double(1.0)).is_err());
    }

    #[test]
    fn pack_with_versionstamp_outcomes() {
        let none = pack_with_versionstamp(b"p", vec![Element::Int(1)]);
        assert_eq!(none, vec![bytes(b"ERROR: NONE")]);
        let incomplete = Element::Versionstamp(Versionstamp::incomplete(5));
        let two = pack_with_versionstamp(b"p", vec![incomplete.clone(), incomplete.clone()]);
        assert_eq!(two, vec![bytes(b"ERROR: MULTIPLE")]);
        let one = pack_with_versionstamp(b"p", vec![bytes(b"a"), incomplete]);
        assert_eq!(one[0], bytes(b"OK"));
        let Element::Bytes(key) = &one[1] else {
            panic!("{one:?}")
        };
        // b"p", then 0x01 'a' 0x00, then 0x33 and the stamp at offset 5.
        assert_eq!(&key[key.len() - 4..], &[5, 0, 0, 0]);
        assert!(key.starts_with(b"p\x01a\x00\x33"));
    }

    #[test]
    fn sort_packed_orders_by_packed_bytes() {
        let a = pack(vec![Element::Int(2)]).unwrap();
        let b = pack(vec![Element::Int(-1), bytes(b"x")]).unwrap();
        let c = pack(vec![Element::Null]).unwrap();
        let sorted = sort_packed(vec![bytes(&a), bytes(&b), bytes(&c)]).unwrap();
        assert_eq!(sorted, vec![c, b, a]);
    }

    #[test]
    fn get_key_prefix_rules() {
        assert_eq!(clamp_to_prefix(b"pa".to_vec(), b"p").unwrap(), b"pa");
        assert_eq!(clamp_to_prefix(b"a".to_vec(), b"p").unwrap(), b"p");
        assert_eq!(clamp_to_prefix(b"z".to_vec(), b"p").unwrap(), b"q");
        assert_eq!(clamp_to_prefix(b"".to_vec(), b"").unwrap(), b"");
        assert_eq!(clamp_to_prefix(b"b".to_vec(), b"a\xff").unwrap(), b"b");
        assert!(strinc(b"\xff\xff").is_err());
    }

    #[test]
    fn flags_and_integers() {
        assert!(truthy(&Element::Int(2)).unwrap());
        assert!(!truthy(&Element::Int(0)).unwrap());
        assert!(truthy(&bytes(b"")).is_err());
        assert_eq!(to_i32(&Element::Int(-5)).unwrap(), -5);
        assert!(to_i32(&Element::Int(1 << 40)).is_err());
        assert!(to_count(&Element::Int(-1)).is_err());
    }
}
