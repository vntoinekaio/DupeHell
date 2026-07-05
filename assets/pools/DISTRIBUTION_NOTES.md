<!-- DupeHell -- MIT License . Educational Use Only -->
<!-- EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ../../ETHICS.md for prohibited uses. -->

# Pool Distribution Notes

## Design choice: uniform sampling

All pool JSONs are flat arrays. Values are sampled uniformly
via `rng.next_usize(len)`. This is intentional:

- **Record linkage benchmarking** needs unbiased frequency distributions
  to avoid giving advantage to common names in match scoring.
- Weighted/frequency-based distributions would create dataset-specific
  biases that don't generalize across benchmark runs.

## What this means

- `first_name.json`: "James" and "Babatunde" have equal probability.
  This does NOT reflect real-world name frequencies.
- `last_name.json`: Same — uniform across all 524 entries.
- `gender.json`: Short list of 6 common values. For diversity-oriented
  use cases, see `gender_inclusive.json`.

## Do NOT use for

- ML training on real-world data (synthetic distributions don't generalize)
- Demographic analysis (no frequency weighting)
- Any application requiring statistically representative populations

## Gender distribution

- `gender.json` (6 values): Realistic short list for most domains
- `gender_inclusive.json` (90+ values): Exhaustive diversity-oriented list

## Ethical use

These distribution choices exist for benchmarking fairness only — they are
not a statement about real-world demographics. See [ETHICS.md](../../ETHICS.md)
at the project root for the full policy and prohibited uses.
