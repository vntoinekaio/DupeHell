# Contributing to DupeHell2

## Quick Start

```bash
git clone https://github.com/anomalyco/dupehell2
cd dupehell2
cargo build --release
cargo test
./target/release/dupehell2 --domain kyc --size 1000 --seed 42
```

## Architecture

DupeHell2 is a standalone Rust binary (no Python, no PyO3, no Polars) that generates synthetic datasets for record linkage benchmarking across 37 domains.

```
dupehell2/
├── src/
│   ├── main.rs           # CLI (clap), config builder
│   ├── pipeline.rs       # Streaming per-batch per-entity pipeline
│   ├── entity_gen.rs     # Entity generation (BATCH_SIZE=500K)
│   ├── column_gen.rs     # Column value generation dispatch
│   ├── fast_template.rs  # ~40 template functions (SSN, phone, email, …)
│   ├── buf_gen.rs        # Byte-buffer generators (barcode, ICCID, …)
│   ├── fk_remap.rs       # Foreign key remapping
│   ├── hn_common.rs      # Hard negative generation
│   ├── gt.rs             # Ground truth computation + IPC/Parquet write
│   ├── ipc_sink.rs       # IPC file sink
│   ├── sink.rs           # Standalone sink utilities
│   ├── faker.rs          # Address/location generation
│   ├── pool_lookup.rs    # Pool asset loading
│   ├── rng.rs            # PRNG helpers
│   ├── context.rs        # Runtime context (pools, schemas)
│   └── noise/            # 9 noise modules
│       ├── mod.rs
│       ├── typos.rs
│       ├── visual.rs
│       ├── names.rs
│       ├── dates.rs
│       ├── identifiers.rs
│       ├── addresses.rs
│       ├── companies.rs
│       └── extra.rs
├── schemas/*.json        # 37 domain schemas
├── assets/pools/         # 134 pool files (multi-lang)
└── ROADMAP.md            # Perf optimisation tracking
```

## Testing

```bash
cargo test          # 114 tests, ~30s
```

## Adding a new domain

1. Add `schemas/<name>.json` — define entities, columns, FK remaps, HN types
2. (Optional) Add new pool files in `assets/pools/` if the domain needs new vocabulary
3. Test: `cargo test && cargo run --release -- --domain <name> --size 200`

## Output format

- Default: IPC (`*.ipc`) — dataset + ground truth
- `--parquet` or `--output-format parquet` : Parquet ZSTD(3) — both dataset and GT

## Performance

Current benchmark (10M KYC medium) : **~660K rec/s**, **~4.5 GB RAM peak**

Output via `sink_parquet` IPC→Parquet conversion (ZSTD level 3) at end of pipeline.
