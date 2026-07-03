# DupeHell2 — Watermarking & Provenance

**Purpose:** Sign synthetic datasets to claim "DupeHell — Educational Use Only" provenance in a legally defensible way, without telemetry or call-home.

**Principle:** 3 independent layers, from most visible to most undetectable. Each layer adds an additional proof of provenance.

---

## Layer 1 — File Metadata (schema-level KV)

**Status:** ✅ Implemented

Key-value pairs are injected into the Arrow IPC schema and Parquet metadata via `build_metadata_map()`.

### Injection points

| File | Line | Mechanism |
|---|---|---|
| `src/pipeline.rs` | 1018 | `build_full_schema()` → `.with_metadata(build_metadata_map(config))` |
| `src/pipeline.rs` | 394-395 | IPC FileWriter uses `full_arc` → metadata injected automatically |
| `src/pipeline.rs` | 898-908 | IPC→Parquet conversion → `set_key_value_metadata(meta_kv)` |
| `src/gt.rs` | 102, 154 | GT schema → `.with_metadata(metadata.clone())` |

### Injected metadata

```rust
HashMap::from([
  ("dupehell.generator",  "DupeHell v0.4"),
  ("dupehell.provenance", "dupehell-synthetic-data"),
  ("dupehell.license",    "MIT"),
  ("dupehell.purpose",    "Educational Use Only — Record Linkage Benchmarking"),
  ("dupehell.url",        "https://github.com/anomalyco/dupehell"),
  ("dupehell.timestamp",  chrono_now()),
  ("dupehell.domain",     config.domain),
  ("dupehell.size",       &config.size.to_string()),
  ("dupehell.seed",       &config.seed.to_string()),
  ("dupehell.run_id",     &config.run_id),
])
```

### Verification

```bash
# IPC
python -c "import pyarrow as pa; f=pa.ipc.open_file('dataset.ipc'); print(f.schema.metadata)"

# Parquet
python -c "import pyarrow.parquet as pq; print(pq.read_schema('dataset.parquet').metadata)"
```

### Robustness

**Low.** Anyone re-exporting via Pandas/PyArrow loses the metadata. Good-faith layer only.

---

## Layer 2 — Canary records

**Status:** ✅ Implemented

### Principle

Inject N dummy rows (`CANARY_COUNT = 3` default) per entity into the dataset. These rows have deterministic values impossible to generate accidentally, including a cryptographic signature. The signature binds the domain, size, seed, and a secret.

### Cryptography

- New dependency: `sha2 = "0.10"` in `Cargo.toml`
- Secret: `CANARY_SECRET = sha256("DupeHell-CANARY-v0.4-educational-use-only-2026")`
- Signature for a run: `sig = sha256(CANARY_SECRET + domain + size.to_string() + seed.to_string())[:16 hex chars]`

### Canary schema

For each entity, inject `CANARY_COUNT` rows with:

| Field | Value |
|---|---|
| `record_id` | `"CANARY-{i}-{sig[:8]}"` (i = canary index) |
| `domain` | `config.domain` |
| `entity_type` | `plan.name` |
| `master_id` | `"CANARY-MASTER-{sig}"` |
| `first_name` | `"DupeHellCanary"` |
| `last_name` | `"Verify-{i}"` |
| `email` | `"{sig[:12]}@canary.dupehell.data"` |
| `dob` | `"2000-01-01"` |
| `ssn` | `"000-00-{sig[:4]}"` |
| `phone` | `"+1-000-000-{sig[:4]}"` |
| *All other fields* | `NULL` or default |

Canaries are aligned with the full schema via `add_metadata_and_align` → missing columns as NULL.

### Injection point

In `src/pipeline.rs`, after the entity loop (Phase 1) and before Phase 2 (HN):

1. Build a `RecordBatch` with `CANARY_COUNT` rows per entity
2. Pass through `add_metadata_and_align` for schema alignment
3. `writer.write(&canary_rb)` — write to the IPC stream
4. Canaries do not participate in GT (they get `match_type = singleton`)

### Exclusions

- Canaries MUST NOT be in FK pools (no FK references to them)
- Canaries MUST NOT be in HN pools
- Canaries have no duplicates
- GT treats them as natural singletons (unique master_id)

### Verification

```bash
dupehell2 verify --dataset kyc_*.ipc
# → ✓ Canary found: domain=kyc size=10000000 seed=42 (3 records)
# → ✓ Signature valid: sig=4a1f... matches computed hash
```

Verification algorithm:
1. Read the file (IPC or Parquet)
2. Filter rows where `email` ends with `@canary.dupehell.data`
3. Extract `sig` from the email prefix
4. Recompute `sha256(CANARY_SECRET + domain + size + seed)` from schema data
5. Compare first 16 hex chars
6. Verify `first_name == "DupeHellCanary"` and `last_name` pattern

### Robustness

**Medium.** Survives any format (CSV, Parquet, IPC, SQL database). An attacker who knows the pattern can remove rows. But provable on raw copies.

---

## Layer 3 — Numeric watermark in identifiers

**Status:** ✅ Implemented

### Principle

Encode a watermark into the **last 1–3 digits** of generated numeric identifiers (SSN, phone, PAN, account_number, etc.). These digits are currently purely random — we replace the last N with a deterministic hash. The alteration is below 0.1% per field.

### Watermarked fields

| Generator | File | Line | Watermark position |
|---|---|---|---|
| `gen_ssn` | `buf_gen.rs` | 110-127 | last 3 digits |
| `gen_phone` | `buf_gen.rs` | 88-107 | last 3 digits |
| `gen_pan` | `buf_gen.rs` | 144-163 | last 2 digits |
| `gen_medicare` | `buf_gen.rs` | 166-184 | last 2 digits |
| `gen_office_phone` | `buf_gen.rs` | 187-212 | last 3 digits |
| `gen_passport` | `buf_gen.rs` | 215-228 | last 2 digits |
| `gen_acct_num` | `buf_gen.rs` | 243-251 | last 2 digits |
| `gen_barcode` | `fast_template.rs` | 220-224 | last 3 digits (via `buf_digits`) |
| `gen_iccid` | `fast_template.rs` | 338-344 | last 3 digits (via `buf_digits`) |
| `gen_upc` | `fast_template.rs` | 398-402 | last 2 digits (via `buf_digits`) |

### Algorithm

```rust
fn watermark_value(raw_value: u64, width: usize, col_seed: u64, config: &PipelineConfig) -> u64 {
    let wm = compute_watermark(config, col_seed);   // u64, same for all columns in a run
    let wm_bits = watermark_bits(width);              // how many LSB digits to replace
    let mask = 10u64.pow(wm_bits);
    (raw_value / mask) * mask + (wm % mask)
}

fn compute_watermark(config: &PipelineConfig, col_seed: u64) -> u64 {
    let input = format!(
        "{}|{}|{}|{}|{}",
        WATERMARK_SECRET, config.domain, config.size, config.seed, col_seed
    );
    let hash = sha256(input.as_bytes());
    u64::from_le_bytes(hash[..8].try_into().unwrap())
}
```

The `col_seed` is a per-generator constant (e.g. `42` for SSN, `137` for phone, etc.) → each column type receives a different watermark, making cross-column correlation impossible.

### Modified functions

- **`context.rs`**: Stores `watermark_map` (HashMap<col_tag, masked_value>) computed via `enable_watermark()`
- **`buf_gen.rs`**: 10 generators call `ctx.watermark_3digits(tag)` or `ctx.watermark_2digits(tag)` before `buf_digits`
- **`fast_template.rs`**: 3 templates (barcode, ICCID, UPC) use `ctx.watermark_3digits(tag)` via `buf_digits`

### Watermark context passing

`WatermarkCtx` is stored in `Context` at startup (`ctx.enable_watermark(domain, size, seed)`) and accessible from all generators via `&Context`.

### Verification

```rust
fn verify_watermark(rb: &RecordBatch, domain: &str, size: usize, seed: u64) -> bool {
    for col_idx in WATERMARKED_COLUMNS {
        let col = rb.column(col_idx);
        // Extract last N digits from each value
        // Verify sha256(secret + domain + size + seed + col_seed)
    }
}
```

```bash
dupehell2 verify --dataset kyc_*.parquet
# → ✓ Numeric watermark verified: 10/10 columns match
```

### Robustness

**High.** Impossible to remove without modifying the data itself. Any transformation that preserves values (copy, format conversion) retains the watermark. Only intentional data alteration (regenerating identifiers) destroys it.

---

## Summary table

| Layer | Effort | Robustness | Survives CSV? | Survives re-export? | Legal proof |
|---|---|---|---|---|---|
| 1 — Metadata | 10 min | ❌ Very low | No | No | None (good faith) |
| 2 — Canary | 30 min | ✅ Medium | Yes | No (if rows removed) | ✅ Possible (raw copy) |
| 3 — Numeric | 1-2h | ✅✅ High | Yes | Yes (values unchanged) | ✅✅ Strong (alteration needed) |
| **2+3 combined** | **~2h** | **✅✅ Very high** | **Yes** | **Yes** | **✅✅✅ Very strong** |

## Dependency

```toml
sha2 = "0.10"
```

## Verification command

```bash
dupehell2 verify --dataset <path.ipc|path.parquet>
```

New clap subcommand in `src/main.rs`:
```rust
#[derive(Subcommand)]
enum Commands {
    Generate(CliGenerate),
    Verify(VerifyArgs),
}

#[derive(Args)]
struct VerifyArgs {
    #[arg(long)]
    dataset: String,
}
```

## Implementation order

1. **Layer 1** (metadata) — 10 min, prerequisite for others
2. **Layer 2** (canaries) — 30 min, adds `sha2`, `verify` subcommand, injection + verification logic
3. **Layer 3** (numeric watermark) — 1-2h, modification of 10 generators, extended verification
