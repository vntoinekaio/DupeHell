# DupeHell2 — Optimization Roadmap

**Current state (July 3, 2026):** 10M KYC in **~15.5s — ~660K rec/s** (peak RAM ~4.5 GB)

Architecture: standalone Rust binary (`dupehell2`), pipeline IPC streaming per-batch per-entity, 41 domain schemas, 110 tests pass.

---

## Timing 10M KYC

| Section | Time | % |
|---|---|---|
| alloc | 0.75s | 6% |
| **sink** (gen + remap + dup + write) | **10.9s** | **84%** |
| hn | 0.05s | 0.4% |
| gt (compute + write) | 1.30s | 10% |
| **Total** | **12.9s** | **100%** |

Of which:
- dup (parallel noise via std::thread::scope) : **4.7s**
- IPC write (FileWriter::write) : **3.8s**

**Sink dominates at 84%** — dup is the largest sub-block.

---

## HIGH — Estimated speedup > 5%

### H1. `add_metadata_and_align` — col_lookup already implemented (Phase 14)

**Status: RESOLVED** in Phase 14 streaming. `col_lookup: Vec<Option<usize>>` pre-built from the first batch, no `index_of` per call.

---

### H2. Double `Vec` allocations in hot paths dup/HN

**File:** `src/pipeline.rs:570-597`

**Problem:** Selection indices for noise are generated in a `UInt64Builder` then passed to threads. Each thread rebuilds `Vec<String>` for master_ids.

**Solution:** Use `indices.value(j) as usize % mslice.len()` directly (already done), but each thread still allocates `mb: Vec<String>` with `with_capacity(cnt)`.

**Estimated gain:** ~3-6% dup (~150-280ms)

---

### H3. `serde_json::json!` + `to_string` rebuilds column config per batch

**File:** `src/pipeline.rs:444-448`

**Problem:** `format!(r#"{{"entity_name":"{}","n":{},"seed":{},"columns":{}}}"#, ...)` serializes `col_json_str` (already a string) every batch.

**Solution:** Pre-build `format!(r#"{{"entity_name":"{}","seed":{},"columns":{}}}"#)` once, interpolate `n` per batch → saves JSON wrapper reconstruction.

**Estimated gain:** ~2-5% sink (~200-500ms)

---

### H4. `get_template` — heap allocation per column per batch

**File:** `src/fast_template.rs:648`

**Problem:**
```rust
pub fn get_template(name: &str) -> Option<TemplateFn> {
    let key = &name.to_lowercase().replace(' ', "_");  // heap alloc per call
    REGISTRY.get(key.as_str()).copied()
}
```
~80 calls for 10M → 80 unnecessary heap allocations (REGISTRY keys are already normalized).

**Solution:** Normalize column name once upstream in `column_gen.rs`, pass pre-computed key.

**Estimated gain:** ~1-3% gen (included in sink)

---

### H5. `buf_digits` allocates `Vec<u8>` per element

**File:** `src/buf_gen.rs:53`

**Problem:**
```rust
let mut s = vec![b'0'; width];  // HEAP ALLOC per element (5M for 10M)
```

**Solution:** Move buffer before the loop, reuse by overwrite.

**Estimated gain:** ~1-3% gen

---

## MEDIUM — Estimated speedup 1-5%

### M1. `pick_rows` — no intermediate Vec (already UInt64Builder)

**Status: RESOLVED** — indices are written directly to `UInt64Builder` (Phase 13c).

---

### M2. HN pool — unique concatenation (already done)

**Status: RESOLVED** — Phase 14 concatenates eager in `HnPool.batch`.

---

### M3. `compute_gt` — `Vec<u32>` suffix index (already done)

**Status: RESOLVED** — Phase 12.1: `Vec<u32>` replaces `HashMap<&str, usize>`.

---

### M4. `add_metadata_and_align` — `vec![domain; n]` redundant

**File:** `src/pipeline.rs:865`

**Problem:** `StringArray::from(vec![domain; n])` creates a `Vec<&str>` of size n.

**Solution:** `StringArray::from_iter_values(std::iter::repeat(domain).take(n))`.

**Estimated gain:** ~1% sink

---

### M5. `apply_noise_to_batch` — partial `Vec<Option<ArrayRef>>`

**File:** `src/pipeline.rs:281-282`

**Problem:** Clones 38 columns to modify 2-3.

**Solution:** `Vec<Option<ArrayRef>>`, clone only modified columns.

**Estimated gain:** ~1-2% dup

---

### M6. FK pool — `StringBuilder` already used (Phase 14)

**Status: RESOLVED** — Phase 14 uses `StringBuilder` instead of `Vec<String>`.

---

## LOW — < 1% (for reference)

| # | Pattern | File | Note |
|---|---|---|---|
| L1 | `entity_prefix` uses `format!` | `pipeline.rs:125` | Called 1× per entity, negligible |
| L2 | `random_indices` allocates `Vec<usize>` | `rng.rs:62` | Not in hot paths |
| L3 | `PoolStore::load` via `from_reader` | `context.rs:33` | One-time startup, negligible |
| L4 | `generate_null_mask` per-element RNG | `column_gen.rs:231` | Inherent to algorithm |
| L5 | `apply_null_rate` per-element StringBuilder | `column_gen.rs:244` | Inherent to algorithm |
| L6 | Duplicate `build` / `build_string_array` | `fast_template.rs` / `buf_gen.rs` | Hygiene, not perf |

---

## Next optimizations (diminishing ROI)

| Priority | ID | Estimated gain | Effort |
|---|---|---|---|
| 1 | **H2** | 3-6% sink | 20min |
| 2 | **H3** | 2-5% sink | 10min |
| 3 | **H5** | 1-3% gen | 5min |
| 4 | **H4** | 1-3% gen | 5min |
| 5 | M4 | 1% sink | 5min |
| 6 | M5 | 1-2% dup | 20min |

**Target H2+H3+H5+H4: ~7-14% → ~11-12s pipeline → ~800K+ rec/s**

---

## CLI

```bash
dupehell2 --domain kyc --size 1000000 [--seed 42] [--difficulty medium|hard|hell]
          [--output-format ipc|parquet] [--parquet] [--output-dir .]
          [--hard-neg-ratio 0.3] [--singleton-master-fraction 0.10]
          [--pools-dir ../dupehell/assets/pools] [--schemas-dir schemas]
```

- `--parquet` : alias for `--output-format parquet` (dataset + GT as .parquet ZSTD(3))
- Default: IPC for dataset and GT
