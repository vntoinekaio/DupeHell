<!-- DupeHell -- MIT License . Educational Use Only -->
<!-- EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses. -->

# Contributing to DupeHell2

## Quick start

```bash
git clone https://github.com/vntoinekaio/DupeHell
cd dupehell2
cargo build --release
cargo test
./target/release/dupehell2 --domain kyc --size 1000 --seed 42
```

---

## Architecture

DupeHell2 generates synthetic datasets for record linkage benchmarking across
41 domains. Available as a Rust CLI binary or a Python package (`pip install dupehell`).

```
dupehell2/
├── src/
│   ├── lib.rs              # Library root + PyO3 bindings
│   ├── main.rs             # CLI entry point (clap)
│   ├── schema.rs           # Schema loading + pipeline config builder
│   ├── pipeline.rs         # Streaming per-batch per-entity pipeline
│   ├── context.rs          # Runtime context (pools, schemas, watermark)
│   ├── entity_gen.rs       # Entity generation (BATCH_SIZE = 500K)
│   ├── column_gen.rs       # Column value generation dispatch
│   ├── fast_template.rs    # ~40 template functions (SSN, phone, email, …)
│   ├── buf_gen.rs          # Byte-buffer generators (barcode, ICCID, …)
│   ├── fk_remap.rs         # Foreign key remapping
│   ├── hn_common.rs        # Hard negative generation
│   ├── gt.rs               # Ground truth computation + IPC/Parquet write
│   ├── pool_lookup.rs      # Pool asset loading
│   ├── rng.rs              # PRNG helpers
│   ├── difficulty.rs       # Theoretical max F1 estimation
│   └── noise/              # 9 noise modules
│       ├── mod.rs
│       ├── typos.rs
│       ├── visual.rs
│       ├── names.rs
│       ├── dates.rs
│       ├── identifiers.rs
│       ├── addresses.rs
│       ├── companies.rs
│       └── extra.rs
├── pyproject.toml          # Python packaging (maturin)
├── schemas/*.json          # 41 domain schemas
├── assets/pools/           # 132 pool files (multi-lang)
├── docs/                  # Documentation
└── CODE_OF_CONDUCT.md      # Contributor Covenant
```

---

## Testing

```bash
cargo test          # 110 tests, ~30s
```

---

## Adding a new domain

1. Add `schemas/<name>.json` — define entities, columns, FK remaps, HN types
2. (Optional) Add pool files in `assets/pools/` if new vocabulary is needed
3. Test: `cargo test && cargo run --release -- --domain <name> --size 200`

---

## Output format

- **Default**: IPC (`*.ipc`) — dataset + ground truth
- **Parquet**: `--parquet` or `--output-format parquet` — ZSTD(3)
  compression for both dataset and GT

---

## Performance

See [BENCHMARK.md](./BENCHMARK.md) for detailed metrics (up to 75M records,
~630K rec/s).