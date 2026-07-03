# DupeHell2

**Synthetic data generator for record linkage benchmarking.**  
Rust + Python — 41 domains, 500K+ rec/s, 110 tests.

Generate realistic multi-entity synthetic datasets with controlled duplicates, hard negatives, and ground-truth labels. Designed for benchmarking entity resolution (deduplication) and record linkage pipelines.

## Quick start

### Python (pip)

```bash
pip install dupehell
```

```python
from dupehell import generate

r = generate(
    domain="publishing",
    size=10000,
    seed=42,
    difficulty="hard",
    output_dir="./data",
    pools_dir="./assets/pools",
    schemas_dir="./schemas",
)
print(r.dataset)       # ./data/publishing_<hash>.ipc
print(r.ground_truth)  # ./data/publishing_<hash>_ground_truth.ipc
print(r.total_records) # 1030
```

### CLI (Rust)

```bash
cargo run --release -- --domain kyc --size 100000 --seed 42
```

### Output

| Format | Extension | Notes |
|---|---|---|
| IPC (Arrow) | `.ipc` | Default, fastest write |
| Parquet | `.parquet` | Via `--parquet` flag |

Each run produces:
- `{domain}_{hash}.ipc` — main dataset
- `{domain}_{hash}_ground_truth.ipc` — ground-truth labels

### CLI options

```
--domain <DOMAIN>                      [default: kyc]
--size <SIZE>                          [default: 1000000]
--seed <SEED>                          [default: 42]
--difficulty <medium|hard|hell>        [default: medium]
--output-format <ipc|parquet>          [default: ipc]
--output-dir <PATH>                    [default: .]
--hard-neg-ratio <FLOAT>               [default: 0.3]
--singleton-master-fraction <FLOAT>    [default: 0.1]
```

## Features

- **41 domains** — KYC, publishing, fintech, blockchain, technology, banking, healthcare, ecommerce, automotive, cybersecurity, gaming, and 31 more
- **Multi-entity schemas** — 3–5 entity types per domain (e.g. Person, Account, Address, Transaction)
- **Controlled noise** — typos, OCR errors, homoglyphs, date swaps, phonetic variants, Unicode pollution
- **Hard negatives** — Rust-native `hn_common.rs` with `same_field`, `mix_identifier`, `mix_arrays` primitives
- **Ground truth** — full match labels (exact_dup, hard_neg, singleton) with cluster statistics
- **Deterministic** — seeded RNG (`rand_pcg`) for reproducible output across runs
- **Watermarking** — SHA256-based 3-layer fingerprinting (metadata, canary records, numeric steganography)

## Performance

Domaine **kyc**, difficulté **medium**, single-thread :

| Taille | Records | Temps | rec/s |
|---|---|---|---|
| 100 K | 101 506 | 2,6 s | 38 K |
| 1 M | 1 015 006 | 3,9 s | 261 K |
| 10 M | 10 150 006 | 17,3 s | 586 K |
| 50 M | 50 750 006 | 74,5 s | 681 K |
| 75 M | 76 125 006 | 121,1 s | 628 K |

(Voir [BENCHMARK.md](BENCHMARK.md) pour le détail complet.)

## Architecture

```
lib.rs / main.rs → Context (141 pools) → PipelineConfig → run_pipeline()
                                                              │
                     ┌────────────────────────────────────────┼────────────────────┐
                     ▼                                        ▼                    ▼
              entity_gen.rs                            fk_remap.rs           hn_common.rs
              (IPC batch gen)                          (FK cross-ref)       (hard negatives)
                     │                                        │                    │
                     └────────────────────────────────────────┴────────────────────┘
                                                              ▼
                                                     pipeline.rs
                                              (merge + ground truth + IPC write)
                                                              ▼
                                                     {domain}.ipc + GT.ipc
```

## Development

### Rust

```bash
cargo test          # 110 tests
cargo build --release
cargo clippy        # 0 warnings
cargo fmt --check   # formatted
```

### Python

```bash
pip install maturin
maturin build --release
pip install target/wheels/dupehell-*.whl
```

## Domains

Academia, Agriculture, Automotive, Aviation, Banking, Biotech, Blockchain, Construction, CRM, Cybersecurity, Ecommerce, Education, Energy, Fashion, Fintech, Food & Beverage, Gaming, Government, Healthcare, Hospitality, HR, Insurance, KYC, Legal, Logistics, Manufacturing, Maritime, Media, Mining, Nonprofit, Pharma, Publishing, Real Estate, Renewable Energy, Retail, Social Media, Sports, Supply Chain, Technology, Telecom, Travel.

## License

MIT
