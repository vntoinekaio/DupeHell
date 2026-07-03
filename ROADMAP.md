# DupeHell2 — Roadmap d'optimisation

**État actuel (3 Juillet 2026) :** 1M KYC en **2.25s — 427K rec/s**

Architecture : standalone Rust binary, pipeline full IPC, 114 tests pass.

---

## Timing 1M KYC

| Phase | Temps | % |
|---|---|---|
| alloc | 0.231s | 10% |
| gen | 0.208s | 9% |
| **sink** | **1.539s** | **68%** |
| gt | 0.274s | 12% |
| **Total** | **2.252s** | **100%** |

Le **sink domine à 68%** — toutes les optimisations à fort ROI sont concentrées là.

---

## HIGH — Rendement > 5% estimé

### H1. `add_metadata_and_align` — scan linéaire dans `index_of`

**Fichier :** `src/pipeline.rs:869`

**Problème :**
```rust
for field in full_schema.fields().iter().skip(4) {
    match rb_schema.index_of(field.name()) {  // O(n) scan par colonne
        Ok(idx) => all_arrays.push(rb.column(idx).clone()),
        Err(_) => all_arrays.push(new_null_array(...)),
    }
}
```
`Schema::index_of` scanne linéairement tous les champs. Appelé **deux fois par batch**, chaque appel scannant 40+ champs.

**Solution :** Pré-construire `HashMap<String, usize>` une fois par batch, lookup O(1).

**Gain estimé :** ~5-8% sink (~80-120ms)

---

### H2. Doubles allocations `Vec` dans les hot paths dup/HN

**Fichier :** `src/pipeline.rs:500-685`

**Problème :** 3 occurrences du même anti-pattern :
```rust
// a) Ligne 500
let rid_slice: Vec<&str> = ids.record_ids[offset..offset + n]
    .iter().map(|s| s.as_str()).collect();

// b) Lignes 563-573 — double allocation Vec<String> puis Vec<&str>
let dup_rids: Vec<String> = (0..dup_total).map(|i| { ... }).collect();
let dup_rids_ref: Vec<&str> = dup_rids.iter().map(|s| s.as_str()).collect();

// c) Lignes 667-685 — idem pour HN
let hn_rids: Vec<String> = (0..n_hn).map(|i| { ... }).collect();
let hn_rids_ref: Vec<&str> = hn_rids.iter().map(|s| s.as_str()).collect();
```

**Solution :** Utiliser `&[String]` directement depuis `ids.record_ids` ; pour dup/HN, construire directement en `Vec<&str>`.

**Gain estimé :** ~3-6% sink (~50-90ms)

---

### H3. `serde_json::json!` + `to_string` reconstruit la config colonnes par batch

**Fichier :** `src/pipeline.rs:405-412`

**Problème :**
```rust
let request = serde_json::json!({
    "entity_name": plan.name,
    "n": batch_n,
    "seed": batch_seed,
    "columns": columns_val,     // sérialisé À CHAQUE BATCH
});
let request_json = serde_json::to_string(&request)?;
```

**Solution :** Pré-sérialiser les colonnes une fois, utiliser `format!()` pour le wrapper (seuls `n` et `seed` changent).

**Gain estimé :** ~2-5% gen (~10ms)

---

### H4. `get_template` — allocation heap par colonne par batch

**Fichier :** `src/fast_template.rs:648`

**Problème :**
```rust
pub fn get_template(name: &str) -> Option<TemplateFn> {
    let key = &name.to_lowercase().replace(' ', "_");  // heap alloc par appel
    REGISTRY.get(key.as_str()).copied()
}
```
~80 appels pour 1M records → 80 heap allocations inutiles (les clés REGISTRY sont déjà normalisées).

**Solution :** Normaliser le nom de colonne une fois en amont dans `column_gen.rs`, passer la clé pré-calculée.

**Gain estimé :** ~1-3% gen (élimine ~80 allocs heap)

---

### H5. `buf_digits` alloue `Vec<u8>` par élément

**Fichier :** `src/buf_gen.rs:53`

**Problème :**
```rust
pub fn buf_digits(nums: &[u64], width: usize) -> ArrayRef {
    let mut builder = StringBuilder::new();
    for num in nums {
        let mut s = vec![b'0'; width];   // HEAP ALLOC par élément !
        // ...
    }
}
```
Utilisé par `gen_barcode`, `gen_cc`, `gen_jersey`, `gen_upc`, `gen_iccid`. Pour 1M records avec 5 colonnes → **5M tiny heap allocations**.

**Solution :** Déplacer `let mut s = vec![b'0'; width];` avant la boucle, réutiliser le buffer.

**Gain estimé :** ~1-3% gen

---

## MEDIUM — Rendement 1-5% estimé

### M1. `pick_rows` — `Vec<usize>` intermédiaire

**Fichier :** `src/pipeline.rs:543,882`

**Problème :** Les indices sont générés dans `Vec<usize>` puis convertis en `UInt64Array`.

**Solution :** Écrire directement dans un `UInt64Builder` → skip l'allocation `Vec<usize>`.

**Gain estimé :** ~1-2% sink

---

### M2. HN pool — re-concaténation redondante

**Fichier :** `src/pipeline.rs:632-648`

**Problème :** `HnPool::batches` stocke `Vec<RecordBatch>` ; Phase 2 re-concatène.

**Solution :** Concaténer eagerly dans `HnPool`, stocker un seul `RecordBatch`.

**Gain estimé :** ~1-2% total

---

### M3. `compute_gt` — `HashMap` vs sort + scan linéaire

**Fichier :** `src/gt.rs:14-25`

**Problème :** 1M inserts dans `HashMap<&str, usize>` pour compter les clusters.

**Solution :** Trier les master_ids + scan linéaire → plus cache-friendly.

**Gain estimé :** ~1-2% GT (~3-5ms)

---

### M4. `add_metadata_and_align` — `vec![domain; n]` redondant

**Fichier :** `src/pipeline.rs:862-865`

**Problème :** `StringArray::from(vec![domain; n])` crée un `Vec<&str>` de taille n.

**Solution :** `StringArray::from_iter_values(std::iter::repeat(domain).take(n))`.

**Gain estimé :** ~1% sink

---

### M5. `apply_noise_to_batch` — clone toutes les colonnes

**Fichier :** `src/pipeline.rs:281-282`

**Problème :** Clone 38 colonnes pour en modifier 2-3.

**Solution :** `Vec<Option<ArrayRef>>`, ne cloner que les colonnes modifiées.

**Gain estimé :** ~1-2% sink

---

### M6. FK pool — `to_string()` par élément

**Fichier :** `src/pipeline.rs:128`

**Problème :** `all_ids.push(s.value(i).to_string())` alloue une String par FK.

**Solution :** `StringBuilder` au lieu de `Vec<String>`.

**Gain estimé :** ~1% total

---

## LOW — < 1% (pour info)

| # | Pattern | Fichier | Note |
|---|---|---|---|
| L1 | `entity_prefix` utilise `format!` | `pipeline.rs:102` | Appelé 1× par entité, négligeable |
| L2 | `random_indices` alloue `Vec<usize>` | `rng.rs:62` | Pas dans les hot paths |
| L3 | `PoolStore::load` via `from_reader` | `context.rs:33` | One-time startup, négligeable |
| L4 | `generate_null_mask` per-element RNG | `column_gen.rs:231` | Inhérent à l'algo |
| L5 | `apply_null_rate` per-element StringBuilder | `column_gen.rs:244` | Inhérent à l'algo |
| L6 | `build` / `build_string_array` dupliquées | `fast_template.rs` / `buf_gen.rs` | Hygiène, pas perf |

---

## Synthèse — ROI décroissant

| Priorité | ID | Gain estimé | Effort | Gain brut |
|---|---|---|---|---|
| 1 | **H1** | 5-8% sink | 15min | ~80-120ms |
| 2 | **H2** | 3-6% sink | 20min | ~50-90ms |
| 3 | **H5** | 1-3% gen | 5min | ~20-60ms |
| 4 | **H4** | 1-3% gen | 5min | ~20-60ms |
| 5 | **H3** | 2-5% gen | 10min | ~10ms |
| 6 | M1 | 1-2% sink | 10min | ~15-30ms |
| 7 | M3 | 1-2% GT | 15min | ~3-5ms |
| 8 | M4 | 1% sink | 5min | ~15ms |
| 9 | M5 | 1-2% sink | 20min | ~15-30ms |
| 10 | M2 | 1-2% total | 10min | ~15-30ms |
| 11 | M6 | 1% total | 5min | ~15ms |

**Objectif H1+H2+H5+H4+H3 : 12-18% → ~1.85-1.98s (500K+ rec/s)**

**Objectif full stack (H1-H5 + M1-M6) : ~20-25% → ~1.7-1.8s (560K+ rec/s)**
