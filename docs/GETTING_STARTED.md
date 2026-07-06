<!-- DupeHell -- MIT License . Educational Use Only -->
<!-- EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses. -->

# Getting Started

## Installation

### Python (pip)

```bash
pip install dupehell
```

### Rust (from source)

```bash
git clone https://github.com/vntoinekaio/DupeHell
cd dupehell
cargo build --release
./target/release/dupehell --domain kyc --size 1000 --seed 42
```

---

## Quick start

### Python

```python
from dupehell import generate

r = generate(
    domain="publishing", size=10000, seed=42, difficulty="hard",
    output_dir="./data", pools_dir="./assets/pools", schemas_dir="./schemas",
)
print(r.dataset)       # ./data/publishing_<hash>.ipc
print(r.ground_truth)  # ./data/publishing_<hash>_ground_truth.ipc
print(r.total_records) # ~10150 (size + dups + hard negatives)
```

### CLI

```bash
# Minimal
dupehell --domain kyc --size 100000 --seed 42

# Full options
dupehell --domain kyc --size 1000000 --seed 42 \
  --difficulty hell --output-format parquet --output-dir ./output

# Help
dupehell --help
```

---

## Output

Each run produces:
- `{domain}_{hash}.ipc` — main dataset
- `{domain}_{hash}_ground_truth.ipc` — ground-truth labels

| Format | Extension | Notes |
|--------|-----------|-------|
| IPC (Arrow) | `.ipc` | Default, fastest write |
| Parquet | `.parquet` | Via `--parquet` or `--output-format parquet` |

---

## Schema validation

Schemas are validated with Pydantic before generation:

```python
from dupehell import load_and_validate

schema = load_and_validate("schemas/kyc.json")
print(schema.domain)    # "kyc"
print(schema.entities)  # list of entity definitions
print(schema.hn_types)  # hard-negative type configs
```

---

## Ethics

DupeHell generates **synthetic data** for **educational and research purposes
only** — specifically for benchmarking entity resolution algorithms.

- All data is procedurally generated — no real PII is used or distributed
- You may **not** use it for fraud, impersonation, surveillance, or any
  illegal activity
- See [ETHICS.md](../ETHICS.md) for the full policy and prohibited uses

---

## Next steps

- [API reference](API.md) — full Python & Rust API documentation
- [Architecture overview](CONTRIBUTING.md) — codebase structure
- [Benchmarks](BENCHMARK.md) — performance metrics