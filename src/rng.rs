// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use rand::RngCore;
use rand::SeedableRng;
use rand_pcg::Pcg64Mcg;

/// A thin wrapper around PCG64 for deterministic generation.
///
/// Python passes a master seed; this RNG can produce sub-seeds for
/// parallel batches via `fork()`.
#[derive(Clone, Debug)]
pub struct Rng {
    inner: Pcg64Mcg,
}

impl Rng {
    /// Create a new RNG from a 64-bit seed.
    pub fn new(seed: u64) -> Self {
        Self {
            inner: Pcg64Mcg::seed_from_u64(seed),
        }
    }

    /// Return a random `u64`.
    pub fn next_u64(&mut self) -> u64 {
        self.inner.next_u64()
    }

    /// Return a random `usize` in `0..bound`.
    pub fn next_usize(&mut self, bound: usize) -> usize {
        if bound <= 1 {
            return 0;
        }
        (self.next_u64() as usize) % bound
    }

    /// Return a random `f64` in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        // convert to [0, 1) — 53 bits of precision
        (self.next_u64() >> 11) as f64 * (1.0 / 9007199254740992.0)
    }

    /// Fork: create a fresh RNG seeded with a value from this one.
    /// Useful for spawning per-batch or per-column RNGs.
    pub fn fork(&mut self) -> Self {
        Self::new(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn test_fork_differs() {
        let mut parent = Rng::new(42);
        let mut child = parent.fork();
        // parent and child should produce different sequences
        assert_ne!(parent.next_u64(), child.next_u64());
    }

    #[test]
    fn test_next_usize_bounded() {
        let mut rng = Rng::new(0);
        for _ in 0..1000 {
            let v = rng.next_usize(10);
            assert!(v < 10, "value {v} out of range");
        }
    }

    #[test]
    fn test_next_f64_range() {
        let mut rng = Rng::new(1);
        for _ in 0..1000 {
            let v = rng.next_f64();
            assert!((0.0..1.0).contains(&v), "value {v} out of [0,1)");
        }
    }
}
