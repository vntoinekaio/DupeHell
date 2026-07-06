# Demo: Benchmarking Splink v4 against DupeHell

## Purpose

This demo shows how DupeHell can be used to validate a real-world deduplication
pipeline. The core idea is:

1. **DupeHell** generates a synthetic dataset with known ground truth (exact
   duplicates, hard negatives, singletons) and a **theoretical F1 upper bound**
   based on noise and schema design.
2. **Splink** (v4) runs its probabilistic deduplication on the same dataset.
3. The gap between Splink's real F1 and the theoretical F1 max tells you whether
   your pipeline is noise-limited (good) or suboptimal (can improve).

## Workflow

```
Estimate F1 max (DupeHell)
        │
        ▼
Generate dataset + ground truth (DupeHell)
        │
        ▼
Run Splink deduplication
        │
        ▼
Compare Splink F1 vs theoretical F1 max
        │
        ├─ Near max  → bottleneck = noise in data (fundamental limit)
        └─ Far below → algorithm can still be improved
```

## How to run

```bash
pip install dupehell pandas pyarrow splink
python demo/splink_benchmark.py
```

Without Splink installed, the script runs the theoretical bound estimation only.

## Results observed (KYC, 10K, hell)

| Metric | Theoretical max | Splink real | Gap |
|--------|----------------|-------------|-----|
| Precision | 0.979 | 0.612 | 0.367 |
| Recall | 0.715 | 0.308 | 0.407 |
| F1 | 0.826 | 0.410 | 0.416 |

The gap is expected and primarily caused by:

- **Blocking limitations**: Splink must use blocking rules to avoid Cartesian
  product. Noisy records often fall outside every blocking rule.
- **Noise destructiveness**: In "hell" mode, name columns (util=1.0 for
  matching) receive heavy noise (damage ~0.3–0.5), making exact and even
  fuzzy comparisons unreliable.
- **Conservative EM training**: The model is fully trained (all m/u values
  estimated), but the posterior match probabilities remain moderate for
  heavily corrupted records.

The theoretical bound assumes an oracle that can examine the optimal set of
columns simultaneously — not achievable in practice, but valuable as a
ceiling to measure against.

## Caveats

- The theoretical F1 max is a **noise ceiling**: no algorithm can exceed it on
  this dataset. It reflects the information lost to noise before any linking
  occurs.
- Splink v4 was used with default settings. Tuning (e.g. custom comparison
  levels, different training blocks, ensemble of models) could reduce the gap.
- The gap is specific to this domain, difficulty, and dataset size. On `light`
  difficulty, Splink reaches ~95% of the theoretical F1 max.