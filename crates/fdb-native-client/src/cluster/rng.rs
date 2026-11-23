//! A small seeded random number generator (SplitMix64) for shuffles and picks; nothing here
//! needs cryptographic quality, only C++'s `deterministicRandom()` uses.

#[derive(Clone, Debug)]
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: Option<u64>) -> Rng {
        Rng(seed.unwrap_or_else(|| {
            use std::hash::{BuildHasher, Hasher};
            std::collections::hash_map::RandomState::new()
                .build_hasher()
                .finish()
        }))
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number in `0..n` (`n > 0`).
    pub(crate) fn below(&mut self, n: usize) -> usize {
        // The modulo bias is irrelevant for lists of a few coordinators or proxies.
        (self.next_u64() % n as u64) as usize
    }

    /// Fisher-Yates, like `DeterministicRandom::randomShuffle`.
    pub(crate) fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i + 1);
            items.swap(i, j);
        }
    }
}
