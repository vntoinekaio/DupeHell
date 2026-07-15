<!-- DupeHell -- MIT License . Educational Use Only -->
<!-- EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses. -->

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

r = generate(domain="publishing", size=10000, seed=42, difficulty="hard")
print(r.dataset)       # ./publishing_<hash>.ipc
print(r.ground_truth)  # ./publishing_<hash>_ground_truth.ipc
print(r.total_records) # ~10150
```

### CLI (Rust)

```bash
cargo run --release -- --domain kyc --size 100000 --seed 42
```

### Output

| Format | Extension | Notes |
|--------|-----------|-------|
| IPC (Arrow) | `.ipc` | Default, fastest write |
| Parquet | `.parquet` | Via `--parquet` flag |

Each run produces:
- `{domain}_{hash}.ipc` — main dataset
- `{domain}_{hash}_ground_truth.ipc` — ground-truth labels
- `{domain}_{hash}_nodes.ipc` / `{domain}_{hash}_edges.ipc` — property graph
  (only with `--graph` / `generate_graph=True`, see below)

### CLI options

| Option | Default | Description |
|--------|---------|-------------|
| `--domain` | `kyc` | Domain name |
| `--size` | `1000000` | Base records |
| `--seed` | `42` | PRNG seed |
| `--difficulty` | `medium` | `light` / `medium` / `hard` / `hell` |
| `--output-format` | `ipc` | `ipc` or `parquet` |
| `--output-dir` | `.` | Output directory |
| `--graph` | off | Also emit a property graph (nodes + edges) |
| `--graph-format` | `ipc` | `ipc` or `parquet`, only used with `--graph` |

### Graph generation

Add `--graph` (CLI) or `generate_graph=True` (Python) to additionally emit a
property graph alongside the usual tabular dataset — nodes (one per record,
same attributes as the dataset) and typed edges (`fk`, `exact_dup`,
`hard_neg`) linking `record_id`s that a record-linkage/graph pipeline should
resolve to the same entity or a hard negative. Disabled by default: tabular
output, RNG sequence, and memory footprint are unchanged when omitted.

```python
r = generate(domain="fintech", size=10000, seed=42, generate_graph=True)
print(r.nodes)  # ./fintech_<hash>_nodes.ipc
print(r.edges)  # ./fintech_<hash>_edges.ipc
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
| 1M | 280,175 | academia 3.2s | supplychain 4.5s | 1.3s |
| 5M | 632,487 | aviation 6.8s | crm 10.5s | 3.7s |
| 10M | 677,579 | academia 11.8s | manufacturing 23.6s | 11.8s |
| 20M | 746,520 | academia 21.6s | kyc 34.6s | 13.0s |

### IPC vs Parquet

Difficulty **hell**, domain-average throughput.

| Size | IPC | Parquet |
|------|-----|---------|
| 1M | 280.2K rec/s | 228.6K rec/s |
| 5M | 632.5K rec/s | 445.5K rec/s |
| 10M | 677.6K rec/s | 456.1K rec/s |
| 20M | 746.5K rec/s | — |

See [docs/BENCHMARK.md](docs/BENCHMARK.md) for KYC medium-difficulty
single-domain metrics and full per-domain breakdowns at all sizes.

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

## Roadmap

- **Synthetic identity module** — generate realistic digital identities
  (browser fingerprints, device profiles, network patterns) for
  cybersecurity simulation and threat detection research
- **Performance** — continue pushing throughput via smarter batching,
  column-level parallelism, and reduced allocations

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

MIT — **Educational Use Only**. 

This software generates synthetic data for research and educational purposes
only. It must not be used for fraud, identity theft, surveillance, or any
illegal activity. See [ETHICS.md](ETHICS.md) for the full list of prohibited
uses and responsible disclosure policy.

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
