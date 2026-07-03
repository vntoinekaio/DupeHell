# DupeHell2 — Roadmap d'optimisation

**État actuel (3 Juillet 2026) :** 10M KYC en **~15.5s — ~660K rec/s** (peak RAM ~4.5 GB)

Architecture : standalone Rust binary (`dupehell2`), pipeline IPC streaming per-batch per-entity, 37 domain schemas, 114 tests pass.

---

## Timing 10M KYC

| Section | Temps | % |
|---|---|---|
| alloc | 0.75s | 6% |
| **sink** (gen + remap + dup + write) | **10.9s** | **84%** |
| hn | 0.05s | 0.4% |
| gt (compute + write) | 1.30s | 10% |
| **Total** | **12.9s** | **100%** |

Dont :
- dup (bruit parallèle std::thread::scope) : **4.7s**
- IPC write (FileWriter::write) : **3.8s**

Le **sink domine à 84%** — le sous-détail dup est le plus gros bloc.

---

## HIGH — Rendement > 5% estimé

### H1. `add_metadata_and_align` — col_lookup déjà implémenté (Phase 14)

**État : RÉSOLU** dans Phase 14 streaming. `col_lookup: Vec<Option<usize>>` pré-construit depuis le premier batch, pas de `index_of` par appel.

---

### H2. Doubles allocations `Vec` dans les hot paths dup/HN

**Fichier :** `src/pipeline.rs:570-597`

**Problème :** Les indices de sélection pour le bruit sont générés dans un `UInt64Builder` puis passés aux threads. Chaque thread reconstruit `Vec<String>` pour les master_ids.

**Solution :** Utiliser `indices.value(j) as usize % mslice.len()` directement (déjà fait), mais chaque thread alloue toujours `mb: Vec<String>` avec `with_capacity(cnt)`.

**Gain estimé :** ~3-6% dup (~150-280ms)

---

### H3. `serde_json::json!` + `to_string` reconstruit la config colonnes par batch

**Fichier :** `src/pipeline.rs:444-448`

**Problème :** `format!(r#"{{"entity_name":"{}","n":{},"seed":{},"columns":{}}}"#, ...)` sérialise `col_json_str` (déjà une string) à chaque batch.

**Solution :** Pré-construire `format!(r#"{{"entity_name":"{}","seed":{},"columns":{}}}"#)` une seule fois, interpoller `n` par batch → économise la reconstruction du wrapper JSON.

**Gain estimé :** ~2-5% sink (~200-500ms)

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
~80 appels pour 10M → 80 heap allocations inutiles (les clés REGISTRY sont déjà normalisées).

**Solution :** Normaliser le nom de colonne une fois en amont dans `column_gen.rs`, passer la clé pré-calculée.

**Gain estimé :** ~1-3% gen (inclus dans sink)

---

### H5. `buf_digits` alloue `Vec<u8>` par élément

**Fichier :** `src/buf_gen.rs:53`

**Problème :**
```rust
let mut s = vec![b'0'; width];  // HEAP ALLOC par élément (5M pour 10M)
```

**Solution :** Déplacer le buffer avant la boucle, réutiliser par overwrite.

**Gain estimé :** ~1-3% gen

---

## MEDIUM — Rendement 1-5% estimé

### M1. `pick_rows` — pas de Vec intermédiaire (déjà UInt64Builder)

**État : RÉSOLU** — les indices sont écrits directement dans `UInt64Builder` (Phase 13c).

---

### M2. HN pool — concaténation unique (déjà fait)

**État : RÉSOLU** — Phase 14 concatène eager dans `HnPool.batch`.

---

### M3. `compute_gt` — `Vec<u32>` suffix index (déjà fait)

**État : RÉSOLU** — Phase 12.1 : `Vec<u32>` remplace `HashMap<&str, usize>`.

---

### M4. `add_metadata_and_align` — `vec![domain; n]` redondant

**Fichier :** `src/pipeline.rs:865`

**Problème :** `StringArray::from(vec![domain; n])` crée un `Vec<&str>` de taille n.

**Solution :** `StringArray::from_iter_values(std::iter::repeat(domain).take(n))`.

**Gain estimé :** ~1% sink

---

### M5. `apply_noise_to_batch` — `Vec<Option<ArrayRef>>` partiel

**Fichier :** `src/pipeline.rs:281-282`

**Problème :** Clone 38 colonnes pour en modifier 2-3.

**Solution :** `Vec<Option<ArrayRef>>`, ne cloner que les colonnes modifiées.

**Gain estimé :** ~1-2% dup

---

### M6. FK pool — `StringBuilder` déjà utilisé (Phase 14)

**État : RÉSOLU** — Phase 14 utilise `StringBuilder` au lieu de `Vec<String>`.

---

## LOW — < 1% (pour info)

| # | Pattern | Fichier | Note |
|---|---|---|---|
| L1 | `entity_prefix` utilise `format!` | `pipeline.rs:125` | Appelé 1× par entité, négligeable |
| L2 | `random_indices` alloue `Vec<usize>` | `rng.rs:62` | Pas dans les hot paths |
| L3 | `PoolStore::load` via `from_reader` | `context.rs:33` | One-time startup, négligeable |
| L4 | `generate_null_mask` per-element RNG | `column_gen.rs:231` | Inhérent à l'algo |
| L5 | `apply_null_rate` per-element StringBuilder | `column_gen.rs:244` | Inhérent à l'algo |
| L6 | `build` / `build_string_array` dupliquées | `fast_template.rs` / `buf_gen.rs` | Hygiène, pas perf |

---

## Prochaines optimisations (ROI décroissant)

| Priorité | ID | Gain estimé | Effort |
|---|---|---|---|
| 1 | **H2** | 3-6% sink | 20min |
| 2 | **H3** | 2-5% sink | 10min |
| 3 | **H5** | 1-3% gen | 5min |
| 4 | **H4** | 1-3% gen | 5min |
| 5 | M4 | 1% sink | 5min |
| 6 | M5 | 1-2% dup | 20min |

**Objectif H2+H3+H5+H4 : ~7-14% → ~11-12s pipeline → ~800K+ rec/s**

---

## CLI

```bash
dupehell2 --domain kyc --size 1000000 [--seed 42] [--difficulty medium|hard|hell]
          [--output-format ipc|parquet] [--parquet] [--output-dir .]
          [--hard-neg-ratio 0.3] [--singleton-master-fraction 0.10]
          [--pools-dir ../dupehell/assets/pools] [--schemas-dir schemas]
```

- `--parquet` : alias pour `--output-format parquet` (dataset + GT en .parquet ZSTD(3))
- Défaut : IPC pour dataset et GT
