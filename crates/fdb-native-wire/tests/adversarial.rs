//! Resource limits and version-dependent layouts (from the vvh.1 review's scratch tests).

mod support;

use fdb_native_wire::{
    Decode, DecodeError, Encode, EncodeError, EnsureTable, ObjectRef, ProtocolVersion, Reader,
    Shape, Uid, Wire, Writer, encode_with_identifier,
};
use support::Alias;

#[test]
fn work_limit_is_per_decode() {
    // A million empty strings all point at the shared empty blob: legitimate (FDB's writer
    // does the same), about 2 units per byte. Decoding it repeatedly from one Reader works.
    let n = 1_000_000;
    let v: Vec<&[u8]> = vec![b""; n];
    let bytes = encode_with_identifier(&v, 1).unwrap();
    let r = Reader::new(&bytes).unwrap();
    for _ in 0..5 {
        assert_eq!(r.decode_root::<Vec<&[u8]>>().unwrap().len(), n);
    }
    let v: Vec<Option<&[u8]>> = vec![Some(b""); n];
    let bytes = encode_with_identifier(&v, 1).unwrap();
    let r = Reader::new(&bytes).unwrap();
    for _ in 0..5 {
        assert_eq!(r.decode_root::<Vec<Option<&[u8]>>>().unwrap().len(), n);
    }
}

#[test]
fn nested_aliasing_bomb_hits_the_work_limit() {
    // 2000 offsets to one vector of 2000 offsets to one empty blob: 4 million strings from
    // a 16 KiB message.
    let mut w = Writer::new();
    let blob = w.blob(b"").unwrap();
    let inner = w.vector(&vec![Alias(blob); 2000]).unwrap();
    let outer = w.vector(&vec![Alias(inner); 2000]).unwrap();
    let mut root = w.table();
    root.field(&Alias(outer)).unwrap();
    let root = root.finish().unwrap();
    let bytes = w.finish(root, 1).unwrap();
    let r = Reader::new(&bytes).unwrap();
    assert_eq!(
        r.decode_root::<Vec<Vec<&[u8]>>>(),
        Err(DecodeError::WorkLimit)
    );
}

/// A 512-byte table type (only its size matters).
#[derive(Debug)]
#[allow(dead_code)]
struct Big([Uid; 32]);

impl Wire for Big {
    const SHAPE: Shape = Shape::Table;
}

impl<'de> Decode<'de> for Big {
    fn absent() -> Self {
        Big([Uid::default(); 32])
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        let mut out = [Uid::default(); 32];
        for u in &mut out {
            *u = t.field()?;
        }
        Ok(Big(out))
    }
}

#[test]
fn output_amplification_is_bounded_by_type_size() {
    // Every element points at one table with no fields: the work limit does not stop it,
    // and the output is about size_of::<Big>() / 4 = 128 times the message (as documented
    // on Reader).
    let mut w = Writer::new();
    let t = w.table().finish().unwrap();
    let n = 250_000;
    let outer = w.vector(&vec![Alias(t); n]).unwrap();
    let mut root = w.table();
    root.field(&Alias(outer)).unwrap();
    let root = root.finish().unwrap();
    let bytes = w.finish(root, 1).unwrap();
    let r = Reader::new(&bytes).unwrap();
    let v = r.decode_root::<Vec<Big>>().unwrap();
    let ratio = v.len() * size_of::<Big>() / bytes.len();
    assert!((100..=128).contains(&ratio), "{ratio}");
}

/// A recursive table carrying 256 bytes per level.
#[derive(Debug)]
struct Chain(Option<Box<Chain>>, [Uid; 16]);

impl Wire for Chain {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for Chain {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.0)?;
        for u in &self.1 {
            t.field(u)?;
        }
        t.finish()
    }
}

impl<'de> Decode<'de> for Chain {
    fn absent() -> Self {
        Chain(None, [Uid::default(); 16])
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        let next = t.field()?;
        let mut a = [Uid::default(); 16];
        for u in &mut a {
            *u = t.field()?;
        }
        Ok(Chain(next, a))
    }
}

#[test]
fn maximum_nesting_fits_a_small_stack() {
    let chain = (0..63).fold(Chain(None, [Uid::default(); 16]), |c, _| {
        Chain(Some(Box::new(c)), [Uid::new(1, 2); 16])
    });
    let bytes = encode_with_identifier(&chain, 1).unwrap();
    let decoded = std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(move || Reader::new(&bytes).unwrap().decode_root::<Chain>().is_ok())
        .unwrap()
        .join()
        .unwrap();
    assert!(decoded);
}

/// `struct Rec { Optional<EnsureTable<Rec>> next; }`
#[derive(Debug)]
struct Rec(Option<EnsureTable<Box<Rec>>>);

impl Wire for Rec {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for Rec {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        t.field(&self.0)?;
        t.finish()
    }
}

impl<'de> Decode<'de> for Rec {
    fn absent() -> Self {
        Rec(None)
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(Rec(t.field()?))
    }
}

#[test]
fn backward_offsets_terminate() {
    // Offsets that wrap around to earlier objects (cycles) must still terminate.
    let orig = encode_with_identifier(&Rec(Some(EnsureTable(Box::new(Rec(None))))), 1).unwrap();
    for p in (8..orig.len()).step_by(4) {
        for v in [
            0xffff_fffcu32,
            0xffff_fff8,
            0xffff_fff4,
            0xffff_fff0,
            0xffff_ffec,
        ] {
            let mut bytes = orig.clone();
            bytes[p..p + 4].copy_from_slice(&v.to_le_bytes());
            let _ = Reader::new(&bytes).unwrap().decode_root::<Rec>();
        }
    }
}

// ---------------------------------------------------------------------------------------
// Version-dependent layouts.

/// A message whose middle field (`b: u32`) only exists before version 0x0FDB00B080000000.
#[derive(Debug, PartialEq)]
struct Versioned {
    a: u8,
    b: u32,
    c: u64,
}

const V80: ProtocolVersion = ProtocolVersion(0x0FDB_00B0_8000_0000);

impl Wire for Versioned {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for Versioned {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let removed = w.protocol_version().is_some_and(|v| v >= V80);
        let mut t = w.table();
        t.field(&self.a)?;
        if removed {
            t.skip(1);
        } else {
            t.field(&self.b)?;
        }
        t.field(&self.c)?;
        t.finish()
    }
}

impl<'de> Decode<'de> for Versioned {
    fn absent() -> Self {
        Versioned { a: 0, b: 0, c: 0 }
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        let a = t.field()?;
        let b = if r.protocol_version().is_some_and(|v| v >= V80) {
            t.skip(1);
            0
        } else {
            t.field()?
        };
        Ok(Versioned {
            a,
            b,
            c: t.field()?,
        })
    }
}

fn encode_for(version: ProtocolVersion, value: &Versioned) -> Vec<u8> {
    let mut w = Writer::with_protocol_version(version);
    assert_eq!(w.protocol_version(), Some(version));
    let mut root = w.table();
    root.field(value).unwrap();
    let root = root.finish().unwrap();
    let bytes = w.finish(root, 1).unwrap();
    assert_eq!(
        w.protocol_version(),
        Some(version),
        "finish keeps the version"
    );
    bytes
}

#[test]
fn protocol_version_selects_the_layout() {
    let value = Versioned { a: 1, b: 2, c: 3 };
    let old = ProtocolVersion(0x0FDB_00B0_7400_0000);

    let bytes = encode_for(old, &value);
    let r = Reader::with_protocol_version(&bytes, old).unwrap();
    assert_eq!(r.protocol_version(), Some(old));
    assert_eq!(r.decode_root::<Versioned>().unwrap(), value);

    // The 8.0 layout leaves slot 1 absent but keeps `c` in slot 2.
    let bytes = encode_for(V80, &value);
    let r = Reader::with_protocol_version(&bytes, V80).unwrap();
    assert_eq!(
        r.decode_root::<Versioned>().unwrap(),
        Versioned { b: 0, ..value }
    );
    // An old reader sees `b` as absent.
    let r = Reader::with_protocol_version(&bytes, old).unwrap();
    assert_eq!(
        r.decode_root::<Versioned>().unwrap(),
        Versioned { b: 0, ..value }
    );
    assert_eq!(Reader::new(&bytes).unwrap().protocol_version(), None);
}

/// Five one-slot fields, read in full.
#[derive(Debug, PartialEq)]
struct Five(u8, u16, u32, u64, u8);

impl Wire for Five {
    const SHAPE: Shape = Shape::Table;
}

impl<'de> Decode<'de> for Five {
    fn absent() -> Self {
        Five(0, 0, 0, 0, 0)
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        Ok(Five(
            t.field()?,
            t.field()?,
            t.field()?,
            t.field()?,
            t.field()?,
        ))
    }
}

/// The first and last of `Five`'s fields, skipping the three in between.
#[derive(Debug, PartialEq)]
struct Ends(u8, u8);

impl Wire for Ends {
    const SHAPE: Shape = Shape::Table;
}

impl<'de> Decode<'de> for Ends {
    fn absent() -> Self {
        Ends(0, 0)
    }
    fn decode_object(r: &Reader<'de>, at: usize) -> Result<Self, DecodeError> {
        let mut t = r.table(at)?;
        let first = t.field()?;
        t.skip(3);
        Ok(Ends(first, t.field()?))
    }
}

#[test]
fn skipped_slots_are_absent_and_take_no_space() {
    let mut w = Writer::new();
    let mut t = w.table();
    t.field(&7u8).unwrap();
    t.skip(3);
    t.field(&9u8).unwrap();
    let t = t.finish().unwrap();
    let mut root = w.table();
    root.field(&Alias(t)).unwrap();
    let root = root.finish().unwrap();
    let bytes = w.finish(root, 1).unwrap();
    // The table's vtable: 5 slots (14 bytes), table size 6, entries 4, 0, 0, 0, 5.
    let words: Vec<u16> = bytes
        .chunks(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    assert!(
        words.windows(7).any(|w| w == [14, 6, 4, 0, 0, 0, 5]),
        "{words:?}"
    );
    let r = Reader::new(&bytes).unwrap();
    assert_eq!(r.decode_root::<Five>().unwrap(), Five(7, 0, 0, 0, 9));
    assert_eq!(r.decode_root::<Ends>().unwrap(), Ends(7, 9));
}
