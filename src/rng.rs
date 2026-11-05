// DupeHell -- MIT License
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// No liability for misuse.

use rand::Rng as _; // `next_u64` (named `RngCore` before rand 0.10)
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
    ///
    /// Uses Lemire's multiply-shift method instead of `% bound`, which would
    /// otherwise skew low values for any `bound` that doesn't divide 2^64.
    pub fn next_usize(&mut self, bound: usize) -> usize {
        if bound <= 1 {
            return 0;
        }
        ((self.next_u64() as u128 * bound as u128) >> 64) as usize
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

/// SplitMix64's output function: a bijective avalanche mix of a `u64`.
fn mix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Seed for one independent random stream of a run, identified by `path`
/// (a [`stream`] tag first, then e.g. an entity key and a batch index).
///
/// Replaces offset arithmetic such as `seed + batch_idx * 1000 + 1000`,
/// which handed every entity the same seed for the same batch index (so
/// column k of two entities drew identical values row by row) and made
/// nearby seeds overlap (seed 42 batch 1 == seed 1042 batch 0). Every
/// distinct `(seed, path)` here maps to an unrelated stream.
pub fn derive_seed(seed: u64, path: &[u64]) -> u64 {
    path.iter().fold(mix64(seed), |h, &p| mix64(h ^ p))
}

/// Stable 64-bit key for a name (FNV-1a), to give each entity its own
/// [`derive_seed`] streams independently of its position in the schema.
pub fn name_key(name: &str) -> u64 {
    name.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// Stream tags for [`derive_seed`]: one per independent use of randomness.
pub mod stream {
    /// Column values of one batch of an entity's base rows.
    pub const ENTITY_BATCH: u64 = 1;
    /// Which masters get duplicated, and the noise applied to them.
    pub const DUPLICATES: u64 = 2;
    /// FK remap targets of an entity's rows.
    pub const FK_REMAP: u64 = 3;
    /// Pairs drawn for one `hard_neg_types` entry.
    pub const HARD_NEG: u64 = 4;
    /// Seed of one `--chunk-size` chunk.
    pub const CHUNK: u64 = 5;
    /// Reservoir sampling of an entity's FK identifier pool.
    pub const FK_POOL: u64 = 6;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_seed_separates_paths_and_seeds() {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for seed in 0..50u64 {
            for entity in 0..5u64 {
                for batch in 0..20u64 {
                    assert!(seen.insert(derive_seed(seed, &[stream::ENTITY_BATCH, entity, batch])));
                }
            }
        }
        // The old `seed + batch_idx * 1000 + 1000` scheme collided here.
        assert_ne!(
            derive_seed(42, &[stream::ENTITY_BATCH, 0, 1]),
            derive_seed(1042, &[stream::ENTITY_BATCH, 0, 0])
        );
        assert_ne!(name_key("contact"), name_key("lead"));
    }

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
