<!-- DupeHell -- MIT License . Educational Use Only -->
<!-- EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses. -->

# Watermarking & Provenance

**Purpose:** Sign synthetic datasets to claim "DupeHell — Educational Use Only"
provenance in a legally defensible way, without telemetry or call-home.

**Principle:** 3 independent layers, from most visible to most undetectable.
Each layer adds an additional proof of provenance.

---

## Layer 1 — File metadata (schema-level KV)

**Status:** ✅ Implemented

Key-value pairs are injected into the Arrow IPC schema and Parquet metadata
via `build_metadata_map()` in `src/pipeline.rs`.

### Injection points

| File | Mechanism |
|------|-----------|
| `src/pipeline.rs` | `build_full_schema()` → `.with_metadata(build_metadata_map(config))` |
| `src/pipeline.rs` | IPC `FileWriter` uses the metadata-carrying schema → injected automatically |
| `src/pipeline.rs` | IPC → Parquet conversion → `set_key_value_metadata(meta_kv)` |
| `src/gt.rs` | GT schema → `.with_metadata(metadata.clone())` (`write_gt_ipc` / `write_gt_parquet`) |

### Injected metadata

```rust
HashMap::from([
  ("dupehell.generator",  "DupeHell v0.4"),
  ("dupehell.provenance", "dupehell-synthetic-data"),
  ("dupehell.license",    "MIT"),
  ("dupehell.purpose",    "Educational Use Only — Record Linkage Benchmarking"),
  ("dupehell.url",        "https://github.com/vntoinekaio/DupeHell"),
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

**Low.** Anyone re-exporting via Pandas / PyArrow loses the metadata.
Good-faith layer only.

---

## Layer 2 — Canary records

**Status:** ✅ Implemented

### Principle

Inject N dummy rows (`CANARY_COUNT = 3` default) per entity into the dataset.
These rows have deterministic values impossible to generate accidentally,
including a cryptographic signature. The signature binds the domain, size, seed,
and a secret.

### Cryptography

- Dependency: `sha2 = "0.10"` in `Cargo.toml`
- Secret: `CANARY_SECRET = "DupeHell-CANARY-v0.4-educational-use-only-2026"`
- Signature for a run: `sig = sha256(CANARY_SECRET + domain + size.to_string() + seed.to_string())`, hex-encoded, first 8 bytes (`compute_sig()` in `src/canary.rs`)

### What's actually generated

For each entity, `generate_all()` (`src/canary.rs`) generates `CANARY_COUNT = 3`
rows through the **normal entity generator** (so they look like ordinary
records), then overrides two fields:

| Field | Value |
|-------|-------|
| `record_id` | normal record-id sequence (not canary-specific) |
| `master_id` | `"CANARY-{sig}-{j:03}-{ent_idx}"` (`j` = canary index, `ent_idx` = entity index) |
| `email` (whichever of `email_address`/`business_email`/`email` exists) | `"{sig}-{ent_idx}-{i}@canary.dupehell.data"` |
| every other column | left as normally generated — **not** overridden |

Canaries are aligned with the full schema via `add_metadata_and_align`.

### Injection point

In `src/pipeline.rs`, after the entity loop and before the ground-truth
concat step, `canary::generate_all()` is called once per pipeline run:

1. Generate `CANARY_COUNT` rows per entity via the normal entity generator
2. Override the email column with the canary signature
3. Align to the full schema and `writer.write(&aligned)` into the IPC stream
4. Canaries get `match_type = "canary"` in the ground truth (excluded from
   `exact_dup`/`hard_neg`/`unique` counts)

### Exclusions

- Canaries are generated independently of the base entity loop — not
  inserted into FK pools or HN pools
- Canaries have no duplicates

### Manual verification

There is no `verify` CLI subcommand — canaries are checked by inspecting the
data directly:

1. Filter rows where `email` ends with `@canary.dupehell.data`
2. Extract `sig` from the email prefix
3. Recompute `sha256(CANARY_SECRET + domain + size + seed)`, hex-encode, and
   compare the first 16 hex chars
4. Confirm `master_id` matches the `"CANARY-{sig}-..."` pattern

### Robustness

**Medium.** Survives any format (CSV, Parquet, IPC, SQL database). An attacker
who knows the pattern can remove rows. But provable on raw copies.

---

## Layer 3 — Numeric watermark in identifiers

**Status:** ✅ Implemented

### Principle

Encode a watermark into the **last 1–3 digits** of generated numeric identifiers
(SSN, phone, PAN, account_number, etc.). These digits are currently purely
random — we replace the last N with a deterministic hash. The alteration is
below 0.1 % per field.

### Watermarked fields

| Generator (`buf_gen.rs`) | Watermark position |
|---------------------------|--------------------|
| `buf_ssn` | last 3 digits |
| `buf_phone` | last 3 digits |
| `buf_pan` | last 2 digits |
| `buf_medicare` | last 2 digits |
| `buf_office_phone` | last 3 digits |
| `buf_passport` | last 2 digits |
| `buf_acct_num` | last 2 digits |

Plus 3 templates in `fast_template.rs` (`gen_barcode`, `gen_iccid`, `gen_upc`)
that call `buf_digits()` with the watermark mask.

### Algorithm

`Context::enable_watermark(domain, size, seed)` (`src/context.rs`) computes,
for each of the 10 field tags (fixed hex codes such as `0x53534e` for "SSN",
`0x50484f4e` for "PHONE", …):

```rust
let input = format!("{secret}{domain}{size}{seed}{tag}");
let hash = Sha256::digest(input.as_bytes());
let wm = u64::from_le_bytes(hash[..8].try_into().unwrap());
```

and stores `wm` in `watermark_map: HashMap<tag, wm>`. Generators then fetch
the masked digits for their tag via `ctx.watermark_3digits(tag)` (`wm % 1000`)
or `ctx.watermark_2digits(tag)` (`wm % 100`) and splice them into the last N
digits of the generated value through `buf_digits()`. Each tag hashes
independently, so no two column types share the same watermark.

### Modified functions

- **`context.rs`**: `Context::enable_watermark()` builds the `watermark_map`;
  `watermark_3digits()` / `watermark_2digits()` expose it to generators
- **`buf_gen.rs`**: 7 generators (`buf_ssn`, `buf_phone`, `buf_pan`,
  `buf_medicare`, `buf_office_phone`, `buf_passport`, `buf_acct_num`) call
  `ctx.watermark_3digits()`/`watermark_2digits()` before `buf_digits()`
- **`fast_template.rs`**: 3 templates (`gen_barcode`, `gen_iccid`, `gen_upc`)
  do the same via `buf_digits()`

`enable_watermark()` is called once in `main.rs` after building the pipeline
config, before `run_pipeline()`.

### Manual verification

There is no `verify` CLI subcommand — checking a column means recomputing
`sha256(secret + domain + size + seed + tag)` for the relevant field tag and
comparing its masked digits against the last N digits of each value.

### Robustness

**High.** Impossible to remove without modifying the data itself. Any transformation
that preserves values (copy, format conversion) retains the watermark. Only
intentional data alteration (regenerating identifiers) destroys it.

---

## Summary table

| Layer | Effort | Robustness | Survives CSV? | Survives re-export? | Legal proof |
|-------|--------|------------|---------------|---------------------|-------------|
| 1 — Metadata | 10 min | ❌ Very low | No | No | None (good faith) |
| 2 — Canary | 30 min | ✅ Medium | Yes | No (if rows removed) | ✅ Possible (raw copy) |
| 3 — Numeric | 1–2 h | ✅✅ High | Yes | Yes (values unchanged) | ✅✅ Strong (alteration needed) |
| **2 + 3 combined** | **~2 h** | **✅✅ Very high** | **Yes** | **Yes** | **✅✅✅ Very strong** |

---

## Dependency

```toml
sha2 = "0.10"
```

---

## Implementation order

1. **Layer 1** (metadata) — 10 min, prerequisite for others
2. **Layer 2** (canaries) — 30 min, adds `sha2`, injection logic
3. **Layer 3** (numeric watermark) — 1–2 h, modification of 10 generators