<!-- DupeHell -- MIT License -->

<img src="https://raw.githubusercontent.com/vntoinekaio/DupeHell/master/docs/assets/logo_dupehell-3-w.png" alt="DupeHell Logo" width="400">

**Synthetic data generator for record linkage benchmarking.**  
Rust + Python — 40 domains, 500K+ rec/s.

Generate synthetic multi-entity datasets with realistic schemas, controlled duplicates,
hard negatives, and ground-truth labels. Designed for benchmarking entity
resolution (deduplication) and record linkage pipelines.

---

## Quick start

### Python (pip)

```bash
pip install dupehell
```

```python
from dupehell import generate

r = generate(domain="publishing", size=10000, seed=42, difficulty="hell")
print(r.dataset)       # ./publishing_<hash>.parquet
print(r.ground_truth)  # ./publishing_<hash>_ground_truth.parquet
print(r.total_records) # ~10150
```

### CLI (Rust)

```bash
cargo run --release -- --domain kyc --size 100000 --seed 42
```

### Output

| Format | Extension | Notes |
|--------|-----------|-------|
| Parquet | `.parquet` | Default, ZSTD compressed |
| IPC (Arrow) | `.ipc` | Via `--output-format ipc`, fastest write |

Each run produces:
- `{domain}_{hash}.parquet` — main dataset
- `{domain}_{hash}_ground_truth.parquet` — ground-truth labels
- `{domain}_{hash}_nodes.parquet` / `{domain}_{hash}_edges.parquet` — property graph
  (only with `--graph` / `generate_graph=True`, see below)

### CLI options

| Option | Default | Description |
|--------|---------|-------------|
| `--domain` | `kyc` | Domain name |
| `--size` | `1000000` | Base records |
| `--seed` | `42` | PRNG seed |
| `--difficulty` | `medium` | `light` / `medium` / `hell` |
| `--output-format` | `parquet` | `parquet` or `ipc` |
| `--output-dir` | `.` | Output directory |
| `--graph` | off | Also emit a property graph (nodes + edges) |
| `--graph-format` | `parquet` | `parquet` or `ipc`, only used with `--graph` |

### Graph generation

Add `--graph` (CLI) or `generate_graph=True` (Python) to additionally emit a
property graph alongside the usual tabular dataset — nodes (one per record,
same attributes as the dataset) and typed edges (`fk`, `exact_dup`,
`hard_neg`) linking `record_id`s that a record-linkage/graph pipeline should
resolve to the same entity or a hard negative. Disabled by default: tabular
output, RNG sequence, and memory footprint are unchanged when omitted.

```python
r = generate(domain="fintech", size=10000, seed=42, generate_graph=True)
print(r.nodes)  # ./fintech_<hash>_nodes.parquet
print(r.edges)  # ./fintech_<hash>_edges.parquet
```

```bash
cargo run --release -- --domain fintech --size 10000 --seed 42 --graph
```

---

## Features

- **40 domains** — KYC, publishing, fintech, blockchain, technology, banking,
  healthcare, ecommerce, automotive, cybersecurity, gaming, and 30 more
- **Multi-entity schemas** — 3–5 entity types per domain (person, account,
  address, transaction)
- **Controlled noise** — typos, OCR errors, homoglyphs, date swaps, phonetic
  variants, Unicode pollution
- **Hard negatives** — `same_field`, `mix_identifier`, `mix_arrays` primitives
- **Ground truth** — full match labels (exact_dup, hard_neg, singleton) with
  cluster statistics
- **Graph generation** — optional property graph output (nodes, typed edges)
  for graph-based entity resolution and community detection benchmarking
- **Deterministic** — seeded RNG (`rand_pcg`) for reproducible output

---

## Performance

All runs on Lenovo ThinkPad P16 Gen 2 — Intel Core i7 13th,
32 GB DDR5, SK Hynix 1 TB NVMe. Difficulty **hell**, IPC format.
Throughput averaged across all 40 domains.

### Multi-domain throughput (hell, IPC)

| Size | Ø rec/s | Fastest domain | Slowest domain | Range |
|------|---------|----------------|----------------|-------|
| 1M | 695,476 | renewable_energy 1.0s | kyc 3.2s | 2.2s |
| 10M | 718,822 | renewable_energy 9.6s | kyc 34.7s | 25.1s |
| 20M | 704,888 | renewable_energy 20.1s | kyc 72.6s | 52.5s |
| 50M | 659,987 | social_media 56.4s | kyc 165.2s | 108.8s |

Refreshed against the current pipeline, all 40 domains, single consistent
sweep (see [docs/BENCHMARK.md](docs/BENCHMARK.md) for the full per-domain
table). **kyc** is a persistent outlier at every scale — its
`natural_person` entity is the widest, most PII-dense schema in the crate,
and at `hell` all 9 noise categories are simultaneously active on it (vs.
often a single active category on sparser schemas). Excluding kyc, the 1M
average is 705,333 rec/s. Throughput drops noticeably at 50M for the
heavier/denser schemas (`crm`, `ecommerce`, `insurance`, `technology`,
`supplychain`, `telecom`) — a known non-monotonic slowdown at higher
record counts, tracked separately.

### IPC vs Parquet

Difficulty **hell**, domain-average throughput across all 40 domains.
Parquet is the CLI default (ZSTD(3) compressed); IPC is the
internal/streaming-oriented format.

| Size | IPC | Parquet | Overhead |
|------|-----|---------|---------|
| 1M | 695.5K rec/s | 650.9K rec/s | -6.4% |
| 5M | 734.8K rec/s | 705.8K rec/s | -3.9% |
| 10M | 718.8K rec/s | 700.2K rec/s | -2.6% |
| 20M | 704.9K rec/s | 690.2K rec/s | -2.1% |

Parquet's cost shrinks as scale grows — the fixed per-file ZSTD
compression overhead gets amortized over more rows. Parquet is a
reasonable default at any scale tested here; IPC remains the better
choice for pure raw-throughput or internal streaming use cases.

See [docs/BENCHMARK.md](docs/BENCHMARK.md) for the full per-domain matrix
(all 3 difficulties, `--graph` overhead, FK edge weighting) and single-domain
metrics at scale.

---

## Architecture

```
lib.rs / main.rs → Context (133 pools) → PipelineConfig → run_pipeline()
                                                          │
         ┌────────────────────────────────────────────────┼────────────────────┐
         ▼                                                ▼                    ▼
  entity_gen.rs                                    fk_remap.rs           hn_common.rs
  (batch gen)                                      (FK cross-ref)        (hard negatives)
         │                                                │                    │
         └────────────────────────────────────────────────┴────────────────────┘
                                                          ▼
                                                     pipeline.rs
                                               (merge + GT + IPC write)
                                                          ▼
                                               {domain}.ipc + GT.ipc
```

---

## Documentation

| File | Description |
|------|-------------|
| [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) | Installation, quick start, output formats |
| [docs/API.md](docs/API.md) | Full Python & Rust API reference |
| [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) | Architecture, development workflow |
| [docs/BENCHMARK.md](docs/BENCHMARK.md) | Performance metrics (up to 75M records) |
| [docs/SECURITY.md](docs/SECURITY.md) | Security policy & vulnerability reporting |

---

## Domains

Academia · Agriculture · Automotive · Aviation · Banking · Biotech ·
Blockchain · Construction · CRM · Cybersecurity · Ecommerce · Education ·
Energy · Fashion · Fintech · Food & Beverage · Gaming ·
Healthcare · Hospitality · HR · Insurance · KYC · Legal · Logistics ·
Manufacturing · Maritime · Media · Mining · Nonprofit · Pharma · Publishing ·
Real Estate · Renewable Energy · Retail · Social Media · Sports · Supply Chain ·
Technology · Telecom · Travel

---

## Development

```bash
cargo test        # 113 tests, ~30s
cargo build --release
cargo clippy      # 0 warnings
cargo fmt --check # all formatted
```

### Python wheel

```bash
pip install maturin
maturin build --release
pip install target/wheels/dupehell-*.whl
```

---

## License

MIT.

This software is intended for research and educational use: generating
synthetic data for record linkage / entity resolution benchmarking. It must
not be used for fraud, identity theft, surveillance, or any illegal activity.
See [ETHICS.md](ETHICS.md) for the full list of prohibited uses and
responsible disclosure policy.

If you use DupeHell in your research, please cite:

```bibtex
@software{dupehell2026,
  author = {DupeHell Contributors},
  title = {DupeHell: Synthetic Multi-Domain Dataset Generator for
           Record Linkage Benchmarking},
  year = {2026},
  url = {https://github.com/vntoinekaio/DupeHell}
}
```
