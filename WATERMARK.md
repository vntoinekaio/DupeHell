# DupeHell2 — Watermarking & Provenance

**Objectif :** Signer les datasets synthétiques pour revendiquer la provenance « DupeHell — Educational Use Only » de manière juridiquement défendable, sans télémétrie ni call home.

**Principe :** 3 couches indépendantes, de la plus visible à la plus indétectable. Chaque couche ajoute une preuve de provenance supplémentaire.

---

## Couche 1 — Métadonnées de fichier (schema-level KV)

**Statut :** ❌ Non implémenté (le code mort `inject_parquet_metadata` dans `sink.rs:110` n'est pas appelé)

### Travail

Injecter des paires clé-valeur dans le schema Arrow IPC et les métadonnées Parquet.

### Fichiers modifiés

| Fichier | Modification |
|---|---|
| `src/pipeline.rs:851-875` | `build_full_schema()` → ajouter `.with_metadata(meta_map)` |
| `src/pipeline.rs:388-391` | IPC FileWriter utilise `full_arc` → metadata injectée automatiquement |
| `src/pipeline.rs:795-810` | IPC→Parquet conversion → ajouter `set_key_value_metadata(meta_kv)` dans WriterProperties |
| `src/gt.rs:93-113` | `write_gt_ipc()` → schema avec metadata |
| `src/gt.rs:140-168` | `write_gt_parquet()` → schema avec metadata |

### Métadonnées injectées

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

### Vérification

```bash
# IPC
python -c "import pyarrow as pa; f=pa.ipc.open_file('dataset.ipc'); print(f.schema.metadata)"

# Parquet
python -c "import pyarrow.parquet as pq; print(pq.read_schema('dataset.parquet').metadata)"
```

### Robustesse

**Faible.** Quiconque ré-exporte via Pandas/PyArrow perd les métadonnées. Couche de bonne foi uniquement.

---

## Couche 2 — Canary records

**Statut :** ❌ Non implémenté

### Principe

Injecter N lignes factices (`CANARY_COUNT = 3` par défaut) par entité dans le dataset. Ces lignes ont des valeurs déterministes impossibles à générer accidentellement, incluant une signature cryptographique. La signature lie le domaine, la taille, la seed et un secret.

### Cryptographie

- Nouvelle dépendance : `sha2 = "0.10"` dans `Cargo.toml`
- Secret : `CANARY_SECRET = sha256("DupeHell-CANARY-v0.4-educational-use-only-2026")`
- Signature pour un run : `sig = sha256(CANARY_SECRET + domain + size.to_string() + seed.to_string())[:16 hex chars]`

### Schéma des canaries

Pour chaque entité, injecter `CANARY_COUNT` lignes avec :

| Champ | Valeur |
|---|---|
| `record_id` | `"CANARY-{i}-{sig[:8]}"` (i = index de la canarie) |
| `domain` | `config.domain` |
| `entity_type` | `plan.name` |
| `master_id` | `"CANARY-MASTER-{sig}"` |
| `first_name` | `"DupeHellCanary"` |
| `last_name` | `"Verify-{i}"` |
| `email` | `"{sig[:12]}@canary.dupehell.data"` |
| `dob` | `"2000-01-01"` |
| `ssn` | `"000-00-{sig[:4]}"` |
| `phone` | `"+1-000-000-{sig[:4]}"` |
| *Tous les autres champs* | `NULL` ou valeur par défaut |

Les canaries sont alignées sur le schema complet via `add_metadata_and_align` → colonnes manquantes en NULL.

### Point d'injection

Dans `src/pipeline.rs`, après la boucle d'entité (Phase 1) et avant Phase 2 (HN) :

1. Construire un `RecordBatch` avec `CANARY_COUNT` lignes par entité
2. Passer par `add_metadata_and_align` pour l'alignement de schema
3. `writer.write(&canary_rb)` — écriture dans le flux IPC
4. Les canaries ne participent pas à GT (elles auront `match_type = singleton`)

### Exclusions

- Les canaries NE doivent PAS être dans les pools FK (pas de FK vers elles)
- Les canaries NE doivent PAS être dans les pools HN
- Les canaries n'ont pas de duplicates
- GT les traite comme des singletons naturels (master_id unique)

### Vérification

```bash
dupehell2 verify --dataset kyc_*.ipc
# → ✓ Canary found: domain=kyc size=10000000 seed=42 (3 records)
# → ✓ Signature valid: sig=4a1f... matches computed hash
```

Algorithme de vérification :
1. Lire le fichier (IPC ou Parquet)
2. Filtrer les lignes où `email` se termine par `@canary.dupehell.data`
3. Extraire `sig` du prefix email
4. Recalculer `sha256(CANARY_SECRET + domain + size + seed)` à partir des données du schema
5. Comparer les 16 premiers hex chars
6. Vérifier `first_name == "DupeHellCanary"` et `last_name` pattern

### Robustesse

**Moyenne.** Survit à tout format (CSV, Parquet, IPC, base SQL). Un attaquant qui connaît le pattern peut supprimer les lignes. Mais prouvable en cas de copie brute.

---

## Couche 3 — Tatouage numérique dans les identifiants

**Statut :** ❌ Non implémenté

### Principe

Encoder un watermark dans les **1-3 derniers digits** des identifiants numériques générés (SSN, phone, PAN, account_number, etc.). Ces digits sont actuellement purement aléatoires — on remplace les N derniers par un hash déterministe. L'altération est inférieure à 0.1% par champ.

### Champs tatoués

| Générateur | Fichier | Ligne | Position watermark |
|---|---|---|---|
| `gen_ssn` | `buf_gen.rs` | 110-127 | 3 derniers digits |
| `gen_phone` | `buf_gen.rs` | 88-107 | 3 derniers digits |
| `gen_pan` | `buf_gen.rs` | 144-163 | 2 derniers digits |
| `gen_medicare` | `buf_gen.rs` | 166-184 | 2 derniers digits |
| `gen_office_phone` | `buf_gen.rs` | 187-212 | 3 derniers digits |
| `gen_passport` | `buf_gen.rs` | 215-228 | 2 derniers digits |
| `gen_acct_num` | `buf_gen.rs` | 243-251 | 2 derniers digits |
| `gen_barcode` | `fast_template.rs` | 220-224 | 3 derniers digits (via `buf_digits`) |
| `gen_iccid` | `fast_template.rs` | 338-344 | 3 derniers digits (via `buf_digits`) |
| `gen_upc` | `fast_template.rs` | 398-402 | 2 derniers digits (via `buf_digits`) |

### Algorithme

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
    // Take first 8 bytes as u64
    u64::from_le_bytes(hash[..8].try_into().unwrap())
}
```

Le `col_seed` est un per-générateur constant (ex: `42` pour SSN, `137` pour phone, etc.) → chaque type de colonne reçoit un watermark différent, rendant la corrélation entre colonnes impossible.

### Fonctions modifiées

- **`buf_gen.rs`** : Ajouter une fonction utilitaire `watermark_last_digits(value: u64, width: usize, col_tag: u64, ctx: &WatermarkCtx) -> u64` appelée par chaque générateur avant `buf_digits`
- **`fast_template.rs`** : Même principe pour les templates qui utilisent `buf_digits` (barcode, ICCID, UPC)
- **`pipeline.rs`** : Construire `WatermarkCtx { domain, size, seed, secret }` et le passer via `Context` ou directement dans `PipelineConfig`
- **`context.rs`** : Optionnel — stocker `WatermarkCtx` dans `Context`

### Passage du watermark context

Deux options :

**Option A — Via PipelineConfig (recommandé) :**
- Ajouter `watermark_secret: String` optionnelle dans `PipelineConfig`
- Le watermark est calculé dans `run_pipeline()` et passé aux générateurs via un `&WatermarkCtx` dans les appels

**Option B — Via Context global :**
- Stocker dans `Context` au chargement
- Accessible depuis `fast_template.rs` et `buf_gen.rs`

### Vérification

```rust
fn verify_watermark(rb: &RecordBatch, domain: &str, size: usize, seed: u64) -> bool {
    for col_idx in WATERMARKED_COLUMNS {
        let col = rb.column(col_idx);
        // Extraire les N derniers digits de chaque valeur
        // Vérifier sha256(secret + domain + size + seed + col_seed)
    }
}
```

```bash
dupehell2 verify --dataset kyc_*.parquet
# → ✓ Numeric watermark verified: 10/10 columns match
```

### Robustesse

**Élevée.** Impossible à supprimer sans modifier les données elles-mêmes. Une transformation qui préserve les valeurs (copie, conversion de format) conserve le watermark. Seule une altération intentionnelle des données (regénération des identifiants) le détruit.

---

## Tableau récapitulatif

| Couche | Effort | Robustesse | Survit à CSV ? | Survit à ré-export ? | Preuve légale |
|---|---|---|---|---|---|
| 1 — Metadata | 10 min | ❌ Très faible | Non | Non | Aucune (bonne foi) |
| 2 — Canary | 30 min | ✅ Moyenne | Oui | Non (si lignes supprimées) | ✅ Possible (copie brute) |
| 3 — Numérique | 1-2h | ✅✅ Haute | Oui | Oui (valeurs inchangées) | ✅✅ Forte (altération nécessaire) |
| **2+3 combiné** | **~2h** | **✅✅ Très haute** | **Oui** | **Oui** | **✅✅✅ Très forte** |

## Dépendance à ajouter

```toml
sha2 = "0.10"
```

## Commande de vérification

```bash
dupehell2 verify --dataset <path.ipc|path.parquet>
```

Nouveau subcommand clap dans `src/main.rs` :
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

## Ordre d'implémentation

1. **Couche 1** (métadonnées) — 10 min, precondition pour les autres
2. **Couche 2** (canaries) — 30 min, ajoute `sha2`, `verify` subcommand, logique d'injection + vérification
3. **Couche 3** (tatouage numérique) — 1-2h, modification des 10 générateurs, vérification étendue
