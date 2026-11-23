//! Pins of the writer's output that no decoder would notice drifting.
//!
//! - A reused `Writer` (which keeps its buffers, its vtable index and its scratch vectors
//!   between messages, and clears only memory an earlier message wrote) must write exactly
//!   the bytes a new one does, whatever it was used for before, including messages that
//!   were abandoned half written or failed with an error.
//! - The bytes of a set of seeded messages, covering tables wider than the writer's stack
//!   arrays (16 slots) and messages with many more than 8 distinct vtables (where the
//!   writer switches from scanning to its hash index), are fixed: their digests were
//!   recorded from the encoder before the performance pass (vvh.8), whose writer scanned
//!   every vtable and allocated per table, and have not changed.
//!
//! The messages are built from a seed with a small generator of table fields; nothing here
//! needs the derive.

use std::fmt::Write as _;

use fdb_native_wire::{Encode, EncodeError, ObjectRef, Shape, Wire, Writer};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

/// A deterministic generator (xorshift64), so a seed always makes the same message.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
}

/// One table field.
#[derive(Clone, Debug)]
enum Val {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F64(f64),
    Bool(bool),
    Pair((u8, u32)),
    Trip((bool, u64, u16)),
    Blob(Vec<u8>),
    VecU8(Vec<u8>),
    VecU16(Vec<u16>),
    VecU64(Vec<u64>),
    VecF32(Vec<f32>),
    VecBool(Vec<bool>),
    VecPair(Vec<(u8, u32)>),
    VecTrip(Vec<(bool, u64, u16)>),
    VecBlob(Vec<Vec<u8>>),
    Table(Vec<Val>),
    VecTable(Vec<Vec<Val>>),
    Skip(usize),
    Opt(Option<u32>),
    OptBlob(Option<Vec<u8>>),
    OptU64(Option<u64>),
    Iter(Vec<u32>),
}

/// A table of fields.
struct Fields<'a>(&'a [Val]);

impl Wire for Fields<'_> {
    const SHAPE: Shape = Shape::Table;
}

impl Encode for Fields<'_> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        let mut t = w.table();
        for v in self.0 {
            match v {
                Val::U8(x) => t.field(x)?,
                Val::U16(x) => t.field(x)?,
                Val::U32(x) => t.field(x)?,
                Val::U64(x) => t.field(x)?,
                Val::F64(x) => t.field(x)?,
                Val::Bool(x) => t.field(x)?,
                Val::Pair(x) => t.field(x)?,
                Val::Trip(x) => t.field(x)?,
                Val::Blob(x) => t.field(&Blob(x))?,
                Val::VecU8(x) => t.field(x)?,
                Val::VecU16(x) => t.field(x)?,
                Val::VecU64(x) => t.field(x)?,
                Val::VecF32(x) => t.field(x)?,
                Val::VecBool(x) => t.field(x)?,
                Val::VecPair(x) => t.field(x)?,
                Val::VecTrip(x) => t.field(x)?,
                Val::VecBlob(x) => {
                    let v: Vec<Blob<'_>> = x.iter().map(|b| Blob(b)).collect();
                    t.field(&v)?
                }
                Val::Table(x) => t.field(&Fields(x))?,
                Val::VecTable(x) => {
                    let v: Vec<Fields<'_>> = x.iter().map(|f| Fields(f)).collect();
                    t.field(&v)?
                }
                Val::Skip(n) => t.skip(*n),
                Val::Opt(x) => t.field(x)?,
                Val::OptU64(x) => t.field(x)?,
                Val::OptBlob(x) => t.field(&x.as_deref().map(Blob))?,
                Val::Iter(x) => t.field(&Iter(x))?,
            }
        }
        t.finish()
    }
}

/// A string, written with `blob` or (when it starts with 7) `blob_with`.
struct Blob<'a>(&'a [u8]);

impl Wire for Blob<'_> {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for Blob<'_> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        if self.0.first() == Some(&7) {
            let bytes = self.0;
            w.blob_with(bytes.len(), |out| out.copy_from_slice(bytes))
        } else {
            w.blob(self.0)
        }
    }
}

/// A vector written from an iterator.
struct Iter<'a>(&'a [u32]);

impl Wire for Iter<'_> {
    const SHAPE: Shape = Shape::Object;
}

impl Encode for Iter<'_> {
    fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
        w.vector_iter(self.0.iter().map(|x| x.wrapping_mul(3)))
    }
}

fn gen_val(r: &mut Rng, depth: u32) -> Val {
    let n = |r: &mut Rng| {
        let k = r.below(10);
        if k < 2 {
            0
        } else if k < 8 {
            r.below(9) as usize
        } else {
            r.below(300) as usize
        }
    };
    match r.below(if depth > 2 { 18 } else { 24 }) {
        0 => Val::U8(r.next() as u8),
        1 => Val::U16(r.next() as u16),
        2 => Val::U32(r.next() as u32),
        3 => Val::U64(r.next()),
        4 => Val::F64(f64::from_bits(r.next())),
        5 => Val::Bool(r.next() & 1 == 1),
        6 => Val::Pair((r.next() as u8, r.next() as u32)),
        7 => Val::Trip((r.next() & 1 == 1, r.next(), r.next() as u16)),
        8 => {
            let k = n(r);
            Val::Blob(r.bytes(k))
        }
        9 => {
            let k = n(r);
            Val::VecU8(r.bytes(k))
        }
        10 => {
            let k = n(r);
            Val::VecU16((0..k).map(|_| r.next() as u16).collect())
        }
        11 => {
            let k = n(r);
            Val::VecU64((0..k).map(|_| r.next()).collect())
        }
        12 => {
            let k = n(r);
            Val::VecF32((0..k).map(|_| f32::from_bits(r.next() as u32)).collect())
        }
        13 => {
            let k = n(r);
            Val::VecBool((0..k).map(|_| r.next() & 1 == 1).collect())
        }
        14 => {
            let k = n(r);
            Val::VecPair((0..k).map(|_| (r.next() as u8, r.next() as u32)).collect())
        }
        15 => {
            let k = n(r);
            Val::VecTrip(
                (0..k)
                    .map(|_| (r.next() & 1 == 1, r.next(), r.next() as u16))
                    .collect(),
            )
        }
        16 => Val::Skip(r.below(3) as usize),
        17 => Val::Opt(if r.next() & 1 == 1 {
            Some(r.next() as u32)
        } else {
            None
        }),
        18 => Val::OptU64(if r.next() & 1 == 1 {
            Some(r.next())
        } else {
            None
        }),
        19 => {
            let k = r.below(5) as usize;
            Val::VecBlob(
                (0..k)
                    .map(|_| {
                        let l = n(r);
                        r.bytes(l)
                    })
                    .collect(),
            )
        }
        20 => Val::Table(gen_fields(r, depth + 1)),
        21 => {
            let k = r.below(4) as usize;
            Val::VecTable((0..k).map(|_| gen_fields(r, depth + 1)).collect())
        }
        22 => Val::OptBlob(if r.next() & 1 == 1 {
            let l = n(r);
            Some(r.bytes(l))
        } else {
            None
        }),
        _ => {
            let k = n(r);
            Val::Iter((0..k).map(|_| r.next() as u32).collect())
        }
    }
}

/// The fields of a table: one in twelve has more than 16, the rest at most 7.
fn gen_fields(r: &mut Rng, depth: u32) -> Vec<Val> {
    let wide = r.below(12) == 0;
    let k = if wide { 17 + r.below(30) } else { r.below(8) } as usize;
    (0..k).map(|_| gen_val(r, depth)).collect()
}

/// A message of many tables of many shapes: well over 8 distinct vtables.
fn many_shapes(seed: u64) -> Vec<Val> {
    let mut r = Rng(seed * 7919 + 1);
    (0..40)
        .map(|_| Val::VecTable((0..20).map(|_| gen_fields(&mut r, 1)).collect()))
        .collect()
}

fn message(w: &mut Writer, fields: &[Val], file_identifier: u32) -> Result<Vec<u8>, EncodeError> {
    let root = Fields(fields).encode_object(w)?;
    w.finish(root, file_identifier)
}

/// Junk written to a writer, then abandoned: a mix of finished objects, a table dropped
/// half written and calls that fail, ending with a reset.
fn abandon(w: &mut Writer, r: &mut Rng) {
    for _ in 0..r.below(4) {
        match r.below(5) {
            0 => {
                let k = r.below(5000) as usize;
                let _ = w.blob(&vec![0xA5; k]);
            }
            1 => {
                let k = r.below(3000) as usize;
                let _ = w.vector(&vec![0xDEAD_BEEF_u32; k]);
            }
            2 => {
                // Fails after reserving space: the iterator is shorter than its count.
                let k = 1 + r.below(500) as usize;
                let _ = w.vector_counted(k * 2, std::iter::repeat_n(0x5A5A_5A5A_5A5A_5A5Au64, k));
            }
            3 => {
                let f = gen_fields(r, 0);
                let _ = Fields(&f).encode_object(w);
            }
            _ => {
                let mut t = w.table();
                let _ = t.field(&0xFFFF_FFFF_FFFF_FFFFu64);
                let _ = t.field(&vec![0xEEu8; 77]);
                drop(t);
            }
        }
    }
    w.reset();
}

/// FNV-1a.
fn digest(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3)
    })
}

// ---------------------------------------------------------------------------------------
// Reuse.

/// Runs a sequence of messages from `seed` through one writer, abandoning junk between some
/// of them, and compares each with the message a new writer writes.
fn check_reuse(seed: u64, messages: usize) -> Result<(), TestCaseError> {
    let mut r = Rng(seed | 1);
    let mut reused = Writer::new();
    for i in 0..messages {
        let fields = if r.below(10) == 0 {
            many_shapes(r.below(1000))
        } else {
            gen_fields(&mut r, 0)
        };
        let fresh = message(&mut Writer::new(), &fields, i as u32).map_err(|e| {
            TestCaseError::fail(format!("seed {seed:#x}, message {i}: new writer: {e}"))
        })?;
        if r.below(3) == 0 {
            abandon(&mut reused, &mut r);
        }
        let again = message(&mut reused, &fields, i as u32).map_err(|e| {
            TestCaseError::fail(format!("seed {seed:#x}, message {i}: reused writer: {e}"))
        })?;
        prop_assert!(
            fresh == again,
            "seed {seed:#x}, message {i}: a reused writer wrote different bytes ({} vs {})",
            fresh.len(),
            again.len()
        );
    }
    Ok(())
}

#[test]
fn a_reused_writer_writes_the_bytes_a_new_one_does() {
    // Seeded: the same runs every time (no persistence of failures either).
    let mut runner = TestRunner::new(Config {
        cases: 60,
        failure_persistence: None,
        rng_seed: proptest::test_runner::RngSeed::Fixed(0x005e_ed0f_u64),
        rng_algorithm: proptest::test_runner::RngAlgorithm::ChaCha,
        ..Config::default()
    });
    runner
        .run(&any::<u64>(), |seed| check_reuse(seed, 12))
        .unwrap();
}

#[test]
fn a_writer_that_hit_an_error_writes_clean_messages_after_reset() {
    let mut w = Writer::new();
    let mut r = Rng(99);
    for round in 0..20 {
        // A table abandoned with its big fields written, then calls that fail after they
        // reserved space (an iterator shorter or longer than its count)...
        let mut t = w.table();
        t.field(&r.bytes(3000)).unwrap();
        t.field(&Iter(&[1, 2, 3])).unwrap();
        drop(t);
        assert!(w.vector_counted(10, std::iter::repeat_n(7u64, 3)).is_err());
        assert!(w.vector_counted(2, std::iter::repeat_n(7u64, 3)).is_err());
        // ... and the writer is clean after a reset.
        w.reset();
        let fields = gen_fields(&mut r, 0);
        assert_eq!(
            message(&mut w, &fields, round).unwrap(),
            message(&mut Writer::new(), &fields, round).unwrap(),
            "round {round}"
        );
    }
}

// ---------------------------------------------------------------------------------------
// Digests.

/// `(seed, length, digest)` of `message(gen_fields(Rng(seed)))`: recorded from the encoder
/// of commit 0a8b3d3 (before vvh.8).
const SEEDED: &[(u64, usize, u64)] = &[
    (0, 40, 0x477e20ae884c5cac),
    (1, 192, 0x5ff616211d172e64),
    (2, 24, 0xdf13f78157fb6c56),
    (3, 280, 0x35708df59fafd867),
    (4, 2232, 0x2266e0de991a917c),
    (5, 16, 0x1a66c640f0f16d98),
    (6, 256, 0x58c913b635faf3cb),
    (7, 976, 0xc857b38777d44b43),
    (8, 24, 0x5748370b9bb608e1),
    (9, 40, 0xe1148d8d5e50ea6f),
    (10, 64, 0x3f9599cab599e7c3),
    (11, 48, 0xeb0ad32a9f7b8dcd),
    (12, 88, 0xc8a752078f955319),
    (13, 344, 0xa3197f16565fa542),
    (14, 520, 0xcbbc1fe9598a2dc5),
    (15, 16, 0x5ad1b07ed01c1972),
    (16, 552, 0x06a5c500f2a81ee0),
    (17, 112, 0xdf283ec08fb30d85),
    (18, 16, 0xe3e37f8e82d9ad9f),
    (19, 128, 0x8d70468bcd451fca),
    (20, 40, 0x4207a3c56975256f),
    (21, 272, 0xa89e8da14bd566d8),
    (22, 392, 0x41d7c0b56d9ea0a2),
    (23, 280, 0xc7cb480b47dc05c2),
    (24, 11320, 0xeead4cb212ca9771),
    (25, 160, 0x3c6804fa54674ef4),
    (26, 648, 0x7c810d1c4bc240c7),
    (27, 64, 0x5eb6e5d97a03b013),
    (28, 336, 0x1240e6f878000d10),
    (29, 40, 0x0ae10348df634508),
    (30, 176, 0xde5534c297feee73),
    (31, 56, 0xce55453d684e5a0b),
    (32, 216, 0x8243b389c1cbb974),
    (33, 32, 0x6768b517a4a0de6d),
    (34, 6672, 0x70c64299d4d339f6),
    (35, 16, 0x9a67c1e87b27e9de),
    (36, 288, 0x56f96173a550b51c),
    (37, 160, 0x0db85d387fca79e3),
    (38, 56, 0xbd93039e95b2601f),
    (39, 5048, 0x6ea71786bf0b6e51),
];

/// The same for `message(many_shapes(seed))`.
const MANY: &[(u64, usize, u64)] = &[
    (0, 756688, 0x7b04b85047863302),
    (1, 821048, 0xa17767d473ea12ac),
    (2, 983448, 0xc44704f3685cb753),
    (3, 914112, 0x069d66f66e8602b4),
    (4, 716736, 0x407561f72ae298af),
    (5, 729832, 0x3f80b4063157b820),
    (6, 789528, 0xdb01150d52255197),
    (7, 721824, 0xf56702aa28046888),
    (8, 911824, 0x84ddf2b95e613d23),
    (9, 797448, 0xd89550e852500a58),
    (10, 881944, 0x6ea2720fcd0da326),
    (11, 775440, 0x23b1749349408022),
];

fn seeded_message(seed: u64) -> Vec<u8> {
    let fields = gen_fields(&mut Rng(seed * 2_654_435_761 + 17), 0);
    message(&mut Writer::new(), &fields, seed as u32).unwrap()
}

fn many_message(seed: u64) -> Vec<u8> {
    message(&mut Writer::new(), &many_shapes(seed), 9).unwrap()
}

#[test]
fn seeded_messages_have_their_recorded_bytes() {
    // Print mode, for recording: FDB_WRITER_DIGESTS=1 cargo test ... -- --nocapture
    if std::env::var_os("FDB_WRITER_DIGESTS").is_some() {
        let mut out = String::new();
        for seed in 0..40u64 {
            let b = seeded_message(seed);
            writeln!(out, "    ({seed}, {}, {:#018x}),", b.len(), digest(&b)).unwrap();
        }
        writeln!(out, "MANY").unwrap();
        for seed in 0..12u64 {
            let b = many_message(seed);
            writeln!(out, "    ({seed}, {}, {:#018x}),", b.len(), digest(&b)).unwrap();
        }
        println!("{out}");
        return;
    }
    assert!(SEEDED.len() >= 30 && MANY.len() >= 10);
    for &(seed, len, want) in SEEDED {
        let b = seeded_message(seed);
        assert_eq!((b.len(), digest(&b)), (len, want), "seed {seed}");
    }
    for &(seed, len, want) in MANY {
        let b = many_message(seed);
        assert_eq!((b.len(), digest(&b)), (len, want), "many {seed}");
    }
}

/// The recorded messages cover what they are meant to: tables wider than 16 slots, and
/// messages with more than 8 distinct vtables (the bytes between the header and the first
/// table are the vtable block).
#[test]
fn recorded_messages_cover_wide_tables_and_many_vtables() {
    let wide = SEEDED
        .iter()
        .filter(|&&(seed, ..)| {
            let fields = gen_fields(&mut Rng(seed * 2_654_435_761 + 17), 0);
            fields.len() > 16 || has_wide_table(&fields)
        })
        .count();
    assert!(wide >= 3, "only {wide} recorded messages have a wide table");
    for &(seed, ..) in MANY {
        let fields = many_shapes(seed);
        let shapes = count_shapes(&fields);
        assert!(
            shapes > 8,
            "many {seed}: only {shapes} distinct table shapes"
        );
    }
}

fn has_wide_table(fields: &[Val]) -> bool {
    fields.iter().any(|v| match v {
        Val::Table(f) => f.len() > 16 || has_wide_table(f),
        Val::VecTable(ts) => ts.iter().any(|f| f.len() > 16 || has_wide_table(f)),
        _ => false,
    })
}

/// The number of distinct (field kind, count) signatures of the tables in a message: a
/// lower bound on its distinct vtables, as two tables with different field kinds in
/// order have different vtables only when their sizes differ, but the signatures of this
/// generator differ in field count or in sizes almost always.
fn count_shapes(fields: &[Val]) -> usize {
    fn size_of_val(v: &Val) -> usize {
        match v {
            Val::U8(_) | Val::Bool(_) => 1,
            Val::U16(_) => 2,
            Val::U32(_) => 4,
            Val::U64(_) | Val::F64(_) => 8,
            Val::Pair(_) => 8,
            Val::Trip(_) => 16,
            Val::Skip(_) => 0,
            Val::Opt(_) | Val::OptU64(_) | Val::OptBlob(_) => 5,
            _ => 4,
        }
    }
    fn walk(fields: &[Val], out: &mut std::collections::HashSet<Vec<usize>>) {
        out.insert(fields.iter().map(size_of_val).collect());
        for v in fields {
            match v {
                Val::Table(f) => walk(f, out),
                Val::VecTable(ts) => ts.iter().for_each(|f| walk(f, out)),
                _ => {}
            }
        }
    }
    let mut set = std::collections::HashSet::new();
    walk(fields, &mut set);
    set.len()
}

/// `finish_into` appends the bytes `finish` returns, after whatever `out` holds, whatever
/// the prefix (the table offsets are relative to the message, not to the vector).
#[test]
fn finish_into_appends_the_bytes_finish_returns() {
    for seed in 0..40u64 {
        let fields = gen_fields(&mut Rng(seed * 2_654_435_761 + 17), 0);
        let expected = message(&mut Writer::new(), &fields, seed as u32).unwrap();
        for prefix_len in [0usize, 1, 3, 12, 13, 100] {
            let prefix: Vec<u8> = (0..prefix_len).map(|i| i as u8 ^ 0xA5).collect();
            let mut out = prefix.clone();
            let mut w = Writer::new();
            let root = Fields(&fields).encode_object(&mut w).unwrap();
            w.finish_into(&mut out, root, seed as u32).unwrap();
            assert_eq!(out[..prefix_len], prefix[..], "seed {seed}");
            assert_eq!(
                out[prefix_len..],
                expected[..],
                "seed {seed} prefix {prefix_len}"
            );
            // The writer was reset: it writes the next message as a new one does.
            let again = message(&mut w, &fields, seed as u32).unwrap();
            assert_eq!(again, expected);
        }
    }
}

#[test]
fn finish_into_leaves_the_buffer_alone_on_an_error() {
    let mut other = Writer::new();
    let fields = gen_fields(&mut Rng(99), 0);
    let root = Fields(&fields).encode_object(&mut other).unwrap();
    // A root that this writer never wrote.
    let mut w = Writer::new();
    let mut out = b"frame header".to_vec();
    let err = w.finish_into(&mut out, root, 7);
    assert!(err.is_err());
    assert_eq!(out, b"frame header");
}
