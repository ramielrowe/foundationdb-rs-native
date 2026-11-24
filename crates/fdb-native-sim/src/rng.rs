//! The simulator's random numbers: SplitMix64, seeded, with the helpers the simulator and
//! the workloads draw from. Nothing here needs more than statistical quality; what matters
//! is that a seed gives the same numbers on every run and platform.

use std::time::Duration;

/// A seeded SplitMix64 generator.
#[derive(Clone, Debug)]
pub struct SimRng(u64);

impl SimRng {
    /// A generator seeded with `seed`.
    pub fn new(seed: u64) -> SimRng {
        SimRng(seed)
    }

    /// The next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// An independent generator, seeded from this one (for a separate stream of decisions).
    pub fn fork(&mut self) -> SimRng {
        SimRng(self.next_u64())
    }

    /// A number in `0..n` (`0` if `n` is 0), without modulo bias worth speaking of.
    pub fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// An index into a collection of `len` items (`len > 0`).
    pub fn index(&mut self, len: usize) -> usize {
        self.below(len as u64) as usize
    }

    /// A number in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        lo + self.below(hi - lo + 1)
    }

    /// A number in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && self.unit() < p
    }

    /// A duration uniformly in `lo..=hi`, to the microsecond.
    pub fn duration(&mut self, lo: Duration, hi: Duration) -> Duration {
        let lo_us = lo.as_micros() as u64;
        let hi_us = hi.as_micros() as u64;
        Duration::from_micros(self.range(lo_us, hi_us))
    }

    /// An exponentially distributed duration with mean `mean`, capped at 20 means.
    pub fn exponential(&mut self, mean: Duration) -> Duration {
        let u = self.unit().max(1e-12);
        mean.mul_f64((-u.ln()).min(20.0))
    }

    /// One of `items` (`items` is not empty).
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.index(items.len())]
    }

    /// Picks an index with probability proportional to its weight (`weights` not all zero).
    pub fn weighted(&mut self, weights: &[u32]) -> usize {
        let total: u64 = weights.iter().map(|w| u64::from(*w)).sum();
        let mut at = self.below(total.max(1));
        for (i, w) in weights.iter().enumerate() {
            let w = u64::from(*w);
            if at < w {
                return i;
            }
            at -= w;
        }
        weights.len() - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_numbers() {
        let mut a = SimRng::new(42);
        let mut b = SimRng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        assert_ne!(SimRng::new(1).next_u64(), SimRng::new(2).next_u64());
    }

    #[test]
    fn helpers_stay_in_range() {
        let mut rng = SimRng::new(7);
        for _ in 0..1000 {
            assert!(rng.below(10) < 10);
            let r = rng.range(5, 9);
            assert!((5..=9).contains(&r));
            let u = rng.unit();
            assert!((0.0..1.0).contains(&u));
            let d = rng.duration(Duration::from_millis(1), Duration::from_millis(2));
            assert!(d >= Duration::from_millis(1) && d <= Duration::from_millis(2));
            assert!(rng.weighted(&[0, 3, 0, 1]) % 2 == 1);
        }
        assert_eq!(rng.below(0), 0);
        assert_eq!(rng.range(3, 3), 3);
    }
}
