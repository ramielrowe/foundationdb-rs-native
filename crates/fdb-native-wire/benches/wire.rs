//! Encode and decode benchmarks for representative FDB messages.
//!
//! Run with `cargo bench -p fdb-native-wire --bench wire` (a name filter after `--`
//! selects benchmarks, e.g. `-- commit`). Every message type uses `#[derive(Wire)]`, so
//! the numbers are those of the code the derive generates.
//!
//! | group | message | what it stresses |
//! | --- | --- | --- |
//! | `small` | `GetValueRequest`-like: a key, a version, some ids | per-message fixed costs: writer setup, one table, vtable |
//! | `kv_reply` | `GetKeyValuesReply`-like: 1000 pairs in a `KeyValueVector` blob | blob copy and payload validation |
//! | `kv_table` | the same pairs as a vector of `{key, value}` tables | table-per-element encode, eager vs lazy decode |
//! | `commit` | `CommitTransactionRequest`-like: 1000 mutation tables | table vector encode (vtable dedup), eager vs `VectorView` decode |
//! | `blob` | a 1 MiB `Vec<u8>` / `&[u8]` | bulk byte decode |
//! | `strings_200` | a vector of 200 strings | offset vectors of blobs |
//! | `vtables` | 2000 tables of few or many distinct shapes | vtable deduplication |

use std::hint::black_box;

use divan::Bencher;
use divan::counter::BytesCount;
use fdb_native_wire::{
    KeyValueRef, KeyValueVector, KeyValueVectorRef, VectorView, Wire, decode, encode,
};

fn main() {
    divan::main();
}

// ---------------------------------------------------------------------------------------
// Deterministic data.

/// A cheap deterministic byte generator (xorshift), so every run benchmarks the same bytes.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }
}

const KV_PAIRS: usize = 1000;
const MUTATIONS: usize = 1000;
const STRINGS: usize = 200;
const BLOB_LEN: usize = 1 << 20;

fn kv_pairs() -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    (0..KV_PAIRS)
        .map(|i| {
            let mut key = format!("\x15user/{i:08}/").into_bytes();
            key.extend(rng.bytes(8));
            (key, rng.bytes(32 + (i % 5) * 16))
        })
        .collect()
}

type MutationData = (u8, Vec<u8>, Vec<u8>);

fn mutations() -> Vec<MutationData> {
    let mut rng = Rng(0xdead_beef_cafe_f00d);
    (0..MUTATIONS)
        .map(|i| {
            let key = format!("\x15data/{i:06}").into_bytes();
            // SetValue (0) mostly, with a few ClearRange (1) and AddValue (2).
            let kind = [0, 0, 0, 1, 2][i % 5];
            (kind, key, rng.bytes(8 + (i % 4) * 8))
        })
        .collect()
}

fn strings() -> Vec<Vec<u8>> {
    let mut rng = Rng(0x0123_4567_89ab_cdef);
    (0..STRINGS).map(|i| rng.bytes(4 + (i * 7) % 60)).collect()
}

// ---------------------------------------------------------------------------------------
// Message types.

/// `GetValueRequest`, reduced to what a small request carries.
#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db01)]
struct GetValue<'a> {
    key: &'a [u8],
    version: i64,
    span: (u64, u64),
    debug_id: Option<(u64, u64)>,
    flags: u32,
}

/// `GetKeyValuesReply`: `serializer(ar, data, version, more, cached)`.
#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db02)]
struct KvReply<'a> {
    data: KeyValueVectorRef<'a>,
    version: i64,
    more: bool,
    cached: bool,
}

#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db02)]
struct KvReplyOwned {
    data: KeyValueVector,
    version: i64,
    more: bool,
    cached: bool,
}

/// The same pairs as tables.
#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db03)]
struct KvTable<'a> {
    data: Vec<KeyValueRef<'a>>,
    version: i64,
}

#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db03)]
struct KvTableView<'a> {
    data: VectorView<'a, KeyValueRef<'a>>,
    version: i64,
}

/// `MutationRef`: `serializer(ar, type, param1, param2)`.
#[derive(Wire)]
struct Mutation<'a> {
    kind: u8,
    param1: &'a [u8],
    param2: &'a [u8],
}

/// `CommitTransactionRef`, reduced to its read version and mutations.
#[derive(Wire)]
struct Transaction<'a> {
    read_snapshot: i64,
    mutations: Vec<Mutation<'a>>,
}

#[derive(Wire)]
struct TransactionView<'a> {
    read_snapshot: i64,
    mutations: VectorView<'a, Mutation<'a>>,
}

/// `CommitTransactionRequest`, reduced to the transaction and a couple of fields.
#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db04)]
struct Commit<'a> {
    transaction: Transaction<'a>,
    flags: u32,
    debug_id: Option<(u64, u64)>,
}

#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db04)]
struct CommitView<'a> {
    transaction: TransactionView<'a>,
    flags: u32,
    debug_id: Option<(u64, u64)>,
}

#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db05)]
struct OwnedBlob {
    data: Vec<u8>,
}

#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db05)]
struct BorrowedBlob<'a> {
    data: &'a [u8],
}

#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db05)]
struct ViewBlob<'a> {
    data: VectorView<'a, u8>,
}

#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db07)]
struct Words {
    data: Vec<u32>,
}

#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db06)]
struct Strings<'a> {
    names: Vec<&'a [u8]>,
}

#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db06)]
struct StringsView<'a> {
    names: VectorView<'a, &'a [u8]>,
}

#[derive(Wire)]
#[fdb(file_identifier = 0x000f_db06)]
struct StringsOwned {
    names: Vec<Vec<u8>>,
}

// ---------------------------------------------------------------------------------------
// small

mod small {
    use super::*;

    fn message(key: &[u8]) -> GetValue<'_> {
        GetValue {
            key,
            version: 123_456_789_012,
            span: (0x1111_2222_3333_4444, 0x5555_6666_7777_8888),
            debug_id: Some((7, 8)),
            flags: 3,
        }
    }

    #[divan::bench]
    fn encode_(bencher: Bencher) {
        let key = b"\x15users/alice/profile".to_vec();
        let msg = message(&key);
        let len = encode(&msg).unwrap().len();
        bencher
            .counter(BytesCount::new(len))
            .bench(|| encode(black_box(&msg)).unwrap());
    }

    #[divan::bench]
    fn decode_(bencher: Bencher) {
        let key = b"\x15users/alice/profile".to_vec();
        let bytes = encode(&message(&key)).unwrap();
        bencher
            .counter(BytesCount::new(bytes.len()))
            .bench(|| decode::<GetValue>(black_box(&bytes)).unwrap().version);
    }
}

// ---------------------------------------------------------------------------------------
// kv_reply

mod kv_reply {
    use super::*;

    fn payload() -> KeyValueVector {
        let pairs = kv_pairs();
        KeyValueVector::from_pairs(pairs.iter().map(|(k, v)| KeyValueRef::new(k, v))).unwrap()
    }

    fn reply(data: &KeyValueVector) -> KvReply<'_> {
        KvReply {
            data: data.borrowed(),
            version: 99,
            more: false,
            cached: false,
        }
    }

    #[divan::bench]
    fn encode_(bencher: Bencher) {
        let data = payload();
        let msg = reply(&data);
        let len = encode(&msg).unwrap().len();
        bencher
            .counter(BytesCount::new(len))
            .bench(|| encode(black_box(&msg)).unwrap());
    }

    /// Decode and walk every pair, borrowing from the message.
    #[divan::bench]
    fn decode_eager(bencher: Bencher) {
        let data = payload();
        let bytes = encode(&reply(&data)).unwrap();
        bencher.counter(BytesCount::new(bytes.len())).bench(|| {
            decode::<KvReply>(black_box(&bytes))
                .unwrap()
                .data
                .iter()
                .map(|kv| kv.key.len() + kv.value.len())
                .sum::<usize>()
        });
    }

    /// Decode into the owned `KeyValueVector` (copies the payload).
    #[divan::bench]
    fn decode_owned(bencher: Bencher) {
        let data = payload();
        let bytes = encode(&reply(&data)).unwrap();
        bencher.counter(BytesCount::new(bytes.len())).bench(|| {
            decode::<KvReplyOwned>(black_box(&bytes))
                .unwrap()
                .data
                .len()
        });
    }
}

// ---------------------------------------------------------------------------------------
// kv_table

mod kv_table {
    use super::*;

    fn message(pairs: &[(Vec<u8>, Vec<u8>)]) -> KvTable<'_> {
        KvTable {
            data: pairs.iter().map(|(k, v)| KeyValueRef::new(k, v)).collect(),
            version: 99,
        }
    }

    #[divan::bench]
    fn encode_(bencher: Bencher) {
        let pairs = kv_pairs();
        let msg = message(&pairs);
        let len = encode(&msg).unwrap().len();
        bencher
            .counter(BytesCount::new(len))
            .bench(|| encode(black_box(&msg)).unwrap());
    }

    #[divan::bench]
    fn decode_eager(bencher: Bencher) {
        let pairs = kv_pairs();
        let bytes = encode(&message(&pairs)).unwrap();
        bencher.counter(BytesCount::new(bytes.len())).bench(|| {
            decode::<KvTable>(black_box(&bytes))
                .unwrap()
                .data
                .iter()
                .map(|kv| kv.key.len() + kv.value.len())
                .sum::<usize>()
        });
    }

    #[divan::bench]
    fn decode_view(bencher: Bencher) {
        let pairs = kv_pairs();
        let bytes = encode(&message(&pairs)).unwrap();
        bencher.counter(BytesCount::new(bytes.len())).bench(|| {
            let msg = decode::<KvTableView>(black_box(&bytes)).unwrap();
            let mut total = 0;
            for kv in &msg.data {
                let kv = kv.unwrap();
                total += kv.key.len() + kv.value.len();
            }
            total
        });
    }
}

// ---------------------------------------------------------------------------------------
// commit

mod commit {
    use super::*;

    fn message(data: &[MutationData]) -> Commit<'_> {
        Commit {
            transaction: Transaction {
                read_snapshot: 5_000_000,
                mutations: data
                    .iter()
                    .map(|(kind, p1, p2)| Mutation {
                        kind: *kind,
                        param1: p1,
                        param2: p2,
                    })
                    .collect(),
            },
            flags: 1,
            debug_id: None,
        }
    }

    #[divan::bench]
    fn encode_(bencher: Bencher) {
        let data = mutations();
        let msg = message(&data);
        let len = encode(&msg).unwrap().len();
        bencher
            .counter(BytesCount::new(len))
            .bench(|| encode(black_box(&msg)).unwrap());
    }

    #[divan::bench]
    fn decode_eager(bencher: Bencher) {
        let data = mutations();
        let bytes = encode(&message(&data)).unwrap();
        bencher.counter(BytesCount::new(bytes.len())).bench(|| {
            decode::<Commit>(black_box(&bytes))
                .unwrap()
                .transaction
                .mutations
                .iter()
                .map(|m| usize::from(m.kind) + m.param1.len() + m.param2.len())
                .sum::<usize>()
        });
    }

    #[divan::bench]
    fn decode_view(bencher: Bencher) {
        let data = mutations();
        let bytes = encode(&message(&data)).unwrap();
        bencher.counter(BytesCount::new(bytes.len())).bench(|| {
            let msg = decode::<CommitView>(black_box(&bytes)).unwrap();
            let mut total = 0;
            for m in &msg.transaction.mutations {
                let m = m.unwrap();
                total += usize::from(m.kind) + m.param1.len() + m.param2.len();
            }
            total
        });
    }
}

// ---------------------------------------------------------------------------------------
// blob

mod blob {
    use super::*;

    #[divan::bench]
    fn encode_(bencher: Bencher) {
        let data = Rng(42).bytes(BLOB_LEN);
        let msg = BorrowedBlob { data: &data };
        let len = encode(&msg).unwrap().len();
        bencher
            .counter(BytesCount::new(len))
            .bench(|| encode(black_box(&msg)).unwrap());
    }

    /// `Vec<u8>`: an owned copy of the blob.
    #[divan::bench]
    fn decode_owned(bencher: Bencher) {
        let data = Rng(42).bytes(BLOB_LEN);
        let bytes = encode(&BorrowedBlob { data: &data }).unwrap();
        bencher
            .counter(BytesCount::new(BLOB_LEN))
            .bench(|| decode::<OwnedBlob>(black_box(&bytes)).unwrap().data);
    }

    /// The same bytes as a `VectorView<u8>`, read through the view one element at a time
    /// (what a caller of the lazy reader pays for scalars).
    #[divan::bench]
    fn decode_view(bencher: Bencher) {
        let data = Rng(42).bytes(BLOB_LEN);
        let bytes = encode(&BorrowedBlob { data: &data }).unwrap();
        bencher.counter(BytesCount::new(BLOB_LEN)).bench(|| {
            let msg = decode::<ViewBlob>(black_box(&bytes)).unwrap();
            msg.data
                .iter()
                .fold(0u64, |sum, b| sum + u64::from(b.unwrap()))
        });
    }

    /// Zero-copy `&[u8]`, for reference: validation only.
    #[divan::bench]
    fn decode_borrowed(bencher: Bencher) {
        let data = Rng(42).bytes(BLOB_LEN);
        let bytes = encode(&BorrowedBlob { data: &data }).unwrap();
        bencher.counter(BytesCount::new(BLOB_LEN)).bench(|| {
            decode::<BorrowedBlob>(black_box(&bytes))
                .unwrap()
                .data
                .len()
        });
    }

    /// A `Vec<u32>` of the same size in bytes, for the non-byte scalar path.
    #[divan::bench]
    fn decode_owned_u32(bencher: Bencher) {
        let words = Words {
            data: (0..BLOB_LEN as u32 / 4).collect(),
        };
        let bytes = encode(&words).unwrap();
        bencher
            .counter(BytesCount::new(BLOB_LEN))
            .bench(|| decode::<Words>(black_box(&bytes)).unwrap().data);
    }
}

// ---------------------------------------------------------------------------------------
// strings_200

mod strings_200 {
    use super::*;

    fn message(data: &[Vec<u8>]) -> Strings<'_> {
        Strings {
            names: data.iter().map(Vec::as_slice).collect(),
        }
    }

    #[divan::bench]
    fn encode_(bencher: Bencher) {
        let data = strings();
        let msg = message(&data);
        let len = encode(&msg).unwrap().len();
        bencher
            .counter(BytesCount::new(len))
            .bench(|| encode(black_box(&msg)).unwrap());
    }

    #[divan::bench]
    fn decode_eager(bencher: Bencher) {
        let data = strings();
        let bytes = encode(&message(&data)).unwrap();
        bencher.counter(BytesCount::new(bytes.len())).bench(|| {
            decode::<Strings>(black_box(&bytes))
                .unwrap()
                .names
                .iter()
                .map(|s| s.len())
                .sum::<usize>()
        });
    }

    /// Owned strings: `Vec<Vec<u8>>`.
    #[divan::bench]
    fn decode_owned(bencher: Bencher) {
        let data = strings();
        let bytes = encode(&message(&data)).unwrap();
        bencher
            .counter(BytesCount::new(bytes.len()))
            .bench(|| decode::<StringsOwned>(black_box(&bytes)).unwrap().names);
    }

    #[divan::bench]
    fn decode_view(bencher: Bencher) {
        let data = strings();
        let bytes = encode(&message(&data)).unwrap();
        bencher.counter(BytesCount::new(bytes.len())).bench(|| {
            let msg = decode::<StringsView>(black_box(&bytes)).unwrap();
            let mut total = 0;
            for s in &msg.names {
                total += s.unwrap().len();
            }
            total
        });
    }
}

// ---------------------------------------------------------------------------------------
// vtables

/// A vector of tables with `variety` different shapes, which no derive produces (a derived
/// table has one shape, or one per union variant): the worst case for vtable
/// deduplication, which must find each table's vtable among all the distinct ones so far.
mod vtables {
    use fdb_native_wire::{Encode, EncodeError, ObjectRef, Shape, Writer};

    use super::*;

    struct Shapes {
        tables: usize,
        variety: usize,
    }

    /// A table with a field per bit of its number: a `u32` for a set bit, a `u8` for a
    /// clear one, so every number has its own vtable.
    struct Table(usize);

    impl Wire for Table {
        const SHAPE: Shape = Shape::Table;
    }

    impl Encode for Table {
        fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
            let mut t = w.table();
            for bit in 0..usize::BITS - self.0.leading_zeros() {
                if self.0 >> bit & 1 == 1 {
                    t.field(&bit)?;
                } else {
                    t.field(&(bit as u8))?;
                }
            }
            t.finish()
        }
    }

    impl Wire for Shapes {
        const SHAPE: Shape = Shape::Object;
    }

    impl Encode for Shapes {
        fn encode_object(&self, w: &mut Writer) -> Result<ObjectRef, EncodeError> {
            let tables: Vec<Table> = (0..self.tables)
                .map(|i| Table(1 + (i.wrapping_mul(2_654_435_761) % self.variety)))
                .collect();
            w.vector(&tables)
        }
    }

    fn bench_encode(bencher: Bencher, variety: usize) {
        let msg = Shapes {
            tables: 2000,
            variety,
        };
        let len = fdb_native_wire::encode_with_identifier(&msg, 1)
            .unwrap()
            .len();
        bencher
            .counter(BytesCount::new(len))
            .bench(|| fdb_native_wire::encode_with_identifier(black_box(&msg), 1).unwrap());
    }

    /// 2000 tables of at most 4 distinct shapes: the common case, a few vtables.
    #[divan::bench]
    fn encode_4_shapes(bencher: Bencher) {
        bench_encode(bencher, 4);
    }

    /// 2000 tables of up to 1000 distinct shapes.
    #[divan::bench]
    fn encode_1000_shapes(bencher: Bencher) {
        bench_encode(bencher, 1000);
    }
}
