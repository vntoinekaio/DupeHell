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

### `{entity}_id` columns are structural keys, not attributes to match on

Every entity carries a primary-key column named `{entity}_id` (e.g.
`researcher_id`, `vehicle_id`). It plays two roles at once:

1. It is the foreign key that ties an entity's flattened child tables back
   together (e.g. `publication.researcher_id` → `researcher.researcher_id`).
2. It is also emitted as a plain column on the entity's own rows.

Because of role (1), this column is **never** noised and stays byte-identical
across every duplicate of the same `master_id`, including at `hell`
difficulty — noising it would break the join to the entity's child tables.

This means `{entity}_id` is not a realistic stand-in for a cross-source
record-linkage attribute: two independent source systems would never share
the same internal ID. If you feed it into an ER model as a candidate
matching field, you will get a free, unrealistic signal that inflates
recall/precision relative to what a true cross-source scenario would allow.
**Exclude `{entity}_id` columns from ER feature/blocking inputs** — use
`record_id` (which does vary per duplicate row) as the row identifier
instead, and treat `{entity}_id` purely as a structural join key for
reassembling an entity's child tables.

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