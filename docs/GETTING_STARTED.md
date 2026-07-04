# Getting Started

## Installation

### Python (pip)

```bash
pip install dupehell
```

### Rust (from source)

```bash
git clone https://github.com/vntoinekaio/DupeHell
cd dupehell2
cargo build --release
./target/release/dupehell2 --domain kyc --size 1000 --seed 42
```

## Quick start

### Python

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
print(r.total_records) # ~10150 (size + dups + hard negatives)
```

### CLI

```bash
# Minimal
dupehell2 --domain kyc --size 100000 --seed 42

# Full options
dupehell2 --domain kyc --size 1000000 --seed 42 \
  --difficulty hell \
  --output-format parquet \
  --output-dir ./output \
  --hard-neg-ratio 0.3

# Help
dupehell2 --help
```

## Output

Each run produces:
- `{domain}_{hash}.ipc` — main dataset
- `{domain}_{hash}_ground_truth.ipc` — ground-truth labels

### Formats

| Format | Extension | Notes |
|---|---|---|
| IPC (Arrow) | `.ipc` | Default, fastest write |
| Parquet | `.parquet` | Via `--parquet` or `--output-format parquet` |

## Schema validation

Schemas are validated with Pydantic before generation:

```python
from dupehell import load_and_validate

schema = load_and_validate("schemas/kyc.json")
print(schema.domain)          # "kyc"
print(schema.entities)        # list of entity definitions
print(schema.hn_types)        # hard-negative type configs
```

## Next steps

- [API reference](API.md) — full Python & Rust API documentation
- [Architecture overview](CONTRIBUTING.md) — codebase structure
- [Benchmarks](BENCHMARK.md) — performance metrics
