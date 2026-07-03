# DupeHell2

**Synthetic data generator for record linkage benchmarking.**  
Standalone Rust binary — 37 domains, ~660K records/s, 114+ tests.

Generate realistic multi-entity synthetic datasets with controlled duplicates, hard negatives, and ground-truth labels. Designed for benchmarking entity resolution (deduplication) and record linkage pipelines.

## Features

- **37 domains** — KYC, banking, healthcare, ecommerce, automotive, cybersecurity, gaming, and 30 more
- **Multi-entity schemas** — 3–5 entity types per domain (e.g. Person, Account, Address, Transaction)
- **Controlled noise** — typos, OCR errors, homoglyphs, date swaps, phonetic variants, Unicode pollution
- **Hard negatives** — Rust-native `hn_common.rs` with `same_field`, `mix_identifier`, `mix_arrays` primitives
- **Ground truth** — full match labels (exact_dup, hard_neg, singleton) with cluster statistics
- **Deterministic** — seeded RNG (`rand_pcg`) for reproducible output across runs
- **Watermarking** — SHA256-based 3-layer fingerprinting (metadata, canary records, numeric steganography)

## Quick start

```bash
# Generate a KYC dataset
cargo run --release -- --domain kyc --size 100000 --seed 42

# List available domains
cargo run --release -- --help
```

### Output

| Format | Extension | Notes |
|---|---|---|
| IPC (Arrow) | `.ipc` | Default, fastest write (~3.8s for 10M) |
| Parquet | `.parquet` | Via `--parquet`, IPC→Polars `sink_parquet` |

Each run produces:
- `{domain}_{hash}.ipc` — main dataset
- `{domain}_{hash}_ground_truth.ipc` — ground-truth labels

### CLI options

```
--domain <DOMAIN>                      [default: kyc]
--size <SIZE>                          [default: 1000000]
--seed <SEED>                          [default: 42]
--difficulty <medium|hard|extreme>     [default: medium]
--output-format <ipc|parquet>          [default: ipc]
--output-dir <PATH>                    [default: .]
--hard-neg-ratio <FLOAT>               [default: 0.3]
--singleton-master-fraction <FLOAT>    [default: 0.1]
```

## Architecture

```
main.rs → Context (134 pools) → PipelineConfig → run_pipeline()
                                                      │
                     ┌────────────────────────────────┼────────────────────┐
                     ▼                                ▼                    ▼
              entity_gen.rs                    fk_remap.rs            hn_common.rs
              (IPC batch gen)                  (FK cross-ref)        (hard negatives)
                     │                                │                    │
                     └────────────────────────────────┴────────────────────┘
                                                      ▼
                                              sink.rs / ipc_sink.rs
                                              (merge + ground truth)
                                                      ▼
                                             {domain}.ipc + GT.ipc
```

## Domains

Academia, Agriculture, Automotive, Aviation, Banking, Biotech, Construction, CRM, Cybersecurity, Ecommerce, Education, Energy, Fashion, Food & Beverage, Gaming, Government, Healthcare, Hospitality, HR, Insurance, KYC, Legal, Logistics, Manufacturing, Maritime, Media, Mining, Nonprofit, Pharma, Real Estate, Renewable Energy, Retail, Social Media, Sports, Supply Chain, Telecom, Travel.

## Performance

| Size | Time | Rec/s | Peak RAM |
|---|---|---|---|
| 1M KYC | ~1.5s | ~660K | ~4.5 GB |
| 10M KYC | ~15.5s | ~645K | ~5.9 GB |

(Pipeline IPC + streaming, no materialization)

## Development

```bash
cargo test          # 114+ Rust tests
cargo build --release
```

### Watermark verification

```python
python check_watermarks.py  # per-domain, per-size SHA256 validation
```

## License

MIT
