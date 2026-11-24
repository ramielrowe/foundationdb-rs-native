//! Benchmarks of the read-your-writes write map (`WriteMap` and its iterator).
//!
//! Run in release mode with
//! `cargo bench -p fdb-native-client --features bench-internals --bench ryw` (a name filter after
//! `--` selects benchmarks, e.g. `-- live_snapshot`). The benches go through
//! `fdb_native_client::bench_support`, a hidden facade, because the write map is crate-private.
//!
//! | group | what it measures |
//! | --- | --- |
//! | `sequential_writes` | `n` sets in key order, no iterator alive |
//! | `random_writes` | `n` sets in pseudo-random order, no iterator alive |
//! | `live_snapshot` | `n` sets, an iterator over the map made before each (the pipelined "reads in flight while writing" pattern: each write finds the map shared) |
//! | `mixed` | 10 000 steps over a 10 000 key map: sets, atomic adds and range clears, with a few reads in flight (iterators held and walked) |
//! | `range_iteration` | a forward and a backward walk over every segment of a map of `n` keys, reading each segment's bounds and flags |
//! | `point_reads` | `n` iterators positioned at pseudo-random keys of a map of `n` keys, each read once |

use std::hint::black_box;

use divan::Bencher;
use fdb_native_client::bench_support::{Cursor, WriteMap};

fn main() {
    divan::main();
}

/// A cheap deterministic generator (xorshift), so every run benchmarks the same keys.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("user/{i:08}").into_bytes()
}

const VALUE: &[u8] = b"a value of moderate size, 40 bytes long.";

const SIZES: [usize; 3] = [1_000, 8_000, 32_000];

fn keys(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(key).collect()
}

fn shuffled_keys(n: usize) -> Vec<Vec<u8>> {
    let mut keys = keys(n);
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for i in (1..n).rev() {
        keys.swap(i, rng.below(i + 1));
    }
    keys
}

/// A map of `n` sets, one in sixteen keys followed by a cleared range.
fn populated(n: usize) -> WriteMap {
    let mut wm = WriteMap::new();
    for i in 0..n {
        wm.set(&key(i), VALUE);
    }
    for i in (0..n).step_by(16) {
        let mut begin = key(i);
        begin.push(b'/');
        let mut end = begin.clone();
        end.push(0xff);
        wm.clear(&begin, &end);
    }
    wm
}

#[divan::bench(args = SIZES)]
fn sequential_writes(bencher: Bencher, n: usize) {
    let keys = keys(n);
    bencher
        .with_inputs(WriteMap::new)
        .bench_local_values(|mut wm| {
            for key in &keys {
                wm.set(key, VALUE);
            }
            wm
        });
}

#[divan::bench(args = SIZES)]
fn random_writes(bencher: Bencher, n: usize) {
    let keys = shuffled_keys(n);
    bencher
        .with_inputs(WriteMap::new)
        .bench_local_values(|mut wm| {
            for key in &keys {
                wm.set(key, VALUE);
            }
            wm
        });
}

#[divan::bench(args = SIZES)]
fn live_snapshot(bencher: Bencher, n: usize) {
    let keys = keys(n);
    bencher
        .with_inputs(WriteMap::new)
        .bench_local_values(|mut wm| {
            let mut pending = None;
            for key in &keys {
                pending = Some(wm.cursor_at(key));
                wm.set(key, VALUE);
            }
            (wm, pending)
        });
}

#[divan::bench]
fn mixed(bencher: Bencher) {
    const KEYS: usize = 10_000;
    const STEPS: usize = 10_000;
    const IN_FLIGHT: usize = 8;
    bencher
        .with_inputs(|| populated(KEYS))
        .bench_local_values(|mut wm| {
            let mut rng = Rng(0x2545_f491_4f6c_dd1d);
            let mut reads: Vec<Cursor> = Vec::with_capacity(IN_FLIGHT);
            let mut sum = 0;
            for step in 0..STEPS {
                let k = key(rng.below(KEYS));
                match step % 10 {
                    // A read starts (the iterator lives until it is replaced), and walks a bit.
                    0 | 5 => {
                        let mut cursor = wm.cursor_at(&k);
                        for _ in 0..4 {
                            sum += cursor.digest();
                            if !cursor.advance() {
                                break;
                            }
                        }
                        if reads.len() < IN_FLIGHT {
                            reads.push(cursor);
                        } else {
                            reads[step % IN_FLIGHT] = cursor;
                        }
                    }
                    // A read in flight resumes.
                    3 | 8 => {
                        if let Some(cursor) = reads.get_mut(step % IN_FLIGHT) {
                            sum += cursor.digest();
                            let _ = cursor.advance();
                        }
                    }
                    1 | 6 => wm.add(&k, &[1, 0, 0, 0]),
                    9 => {
                        let mut end = k.clone();
                        end.push(b'/');
                        end.push(0xff);
                        wm.clear(&k, &end);
                    }
                    _ => wm.set(&k, VALUE),
                }
            }
            black_box(sum);
            (wm, reads)
        });
}

#[divan::bench(args = SIZES)]
fn range_iteration(bencher: Bencher, n: usize) {
    let wm = populated(n);
    bencher.bench_local(|| {
        let mut sum = 0;
        let mut cursor = wm.cursor_at(b"");
        loop {
            sum += cursor.digest();
            if !cursor.advance() {
                break;
            }
        }
        while cursor.retreat() {
            sum += cursor.digest();
        }
        black_box(sum)
    });
}

#[divan::bench(args = SIZES)]
fn point_reads(bencher: Bencher, n: usize) {
    let wm = populated(n);
    let keys = shuffled_keys(n);
    bencher.bench_local(|| {
        let mut sum = 0;
        for key in &keys {
            sum += wm.cursor_at(key).digest();
        }
        black_box(sum)
    });
}
