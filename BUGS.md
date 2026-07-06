# DupeHell — Bug Report & Technical Debrief

Generated from 9 waves of blind CLI/Python testing (613 tests, 597 passed, 16 failed indicators).
**7 bugs open**, 60 bugs fixed to date.

---

## 🔴 CRITICAL

### C5 — Null rates systématiquement trop bas

| Métrique | Valeur |
|----------|--------|
| Découvert | Vague 9, Agent 9.3 |
| Fichier suspect | `src/column_gen.rs` (génération du masque null) |
| Tests échoués | 5/8 domains |

**Constat :** Les colonnes avec un `null_rate_default` élevé dans le schéma sont beaucoup moins nulles que spécifié.

| Colonne | Schéma attendu | Réel | Delta |
|---------|:---:|:---:|:---:|
| `patient.death_date` | 95% | 3.8% | **−91.2 pts** |
| `customer.date_of_birth` | 75% | 1.1% | **−73.9 pts** |
| `author.birth_date` | 30% | 0.0% | **−30.0 pts** |
| `dispute.resolution_date` | 40% | 0.0% | **−40.0 pts** |

**Piste :** Le mécanisme d'application du `null_mask` dans `column_gen.rs` semble ignorer le `null_rate_default` pour les colonnes générées via certains templates (date en particulier). Vérifier si le null_mask est appliqué *après* la génération de valeur, ou si le template court-circuite le masque.

---

### C3 — Dates aberrantes (format `DD HH:MM:SS-MM-YYYY`)

| Métrique | Valeur |
|----------|--------|
| Découvert | Vague 9, Agent 9.1 |
| Fichier suspect | `src/buf_gen.rs` ou `src/noise/dates.rs` |
| Occurrences | `fintech.transaction_date`, `banking.transaction_date` |

**Constat :** 1 valeur sur chaque colonne date dans fintech/banking a le format aberrant `DD HH:MM:SS-MM-YYYY` (ex: `"01 20:00:00-07-2024"`) au lieu de `YYYY-MM-DD`.

**Piste :** Probablement une corruption par le noise `dates` (`src/noise/dates.rs`) qui applique une transformation incorrecte sur certaines dates. Le format ressemble à une concaténation maladroite entre un timestamp Unix et des composants de date brute. Vérifier la fonction `noise_date` ou `age_impossible`.

---

## 🟠 HIGH

### C2 — `support_phone` alimenté par un pool de mots

| Métrique | Valeur |
|----------|--------|
| Découvert | Vague 5 (spot-check), reconfirmé Vague 9 |
| Fichier suspect | `schemas/fintech.json` |
| Statut | Non corrigé depuis la Vague 5 |

**Constat :** `fintech.support_phone` est défini dans le schéma avec `type: "string"`, `pool: "adjectives"` — pas de template phone. Les 74 valeurs non-nulles sont des mots dictionnaire (`anchor`, `cider`, `saturn`, `bee`, etc.), pas des numéros de téléphone.

**Fix attendu :** Remplacer la définition de la colonne dans `schemas/fintech.json` par un template phone :
```json
{"name": "support_email", "type": "string", "nullable": true, "null_rate_default": 0.3, "template": "phone"}
```

---

## 🟡 MEDIUM

### C4 — `difficulty` non validé dans Python API

| Métrique | Valeur |
|----------|--------|
| Découvert | Vague 9, Agent 9.2 |
| Fichier suspect | `python/__init__.py` (pas de validation en entrée) |
| CLI | Rejette via clap enum (`light/medium/hard/hell`) ✅ |
| Python API | Accepte `"totallyfake"` silencieusement, pipeline utilise le défaut ❌ |

**Fix attendu :** Ajouter une validation dans `generate()` côté Python (`__init__.py`) :
```python
VALID_DIFFICULTIES = {"light", "medium", "hard", "hell"}
if difficulty not in VALID_DIFFICULTIES:
    raise ValueError(f"difficulty must be one of {VALID_DIFFICULTIES}, got '{difficulty}'")
```

---

### C6 — `locale` non validé dans Python API

| Métrique | Valeur |
|----------|--------|
| Découvert | Vague 9, Agent 9.4 |
| Fichier suspect | `python/__init__.py` |
| CLI | Rejette via clap enum (`en/fr/de/es/it/pt`) ✅ |
| Python API | Accepte `"zh"`, `""`, `"en-us"` avec fallback silencieux à `"en"` ❌ |

**Fix attendu :** Ajouter une validation dans `generate()` côté Python :
```python
VALID_LOCALES = {"en", "fr", "de", "es", "it", "pt"}
if locale and locale.lower() not in VALID_LOCALES:
    raise ValueError(f"locale must be one of {VALID_LOCALES}, got '{locale}'")
```

---

### C7 — Hash fichier non déterministe entre IPC et Parquet

| Métrique | Valeur |
|----------|--------|
| Découvert | Vague 9, Agent 9.10 |
| Fichier suspect | `src/pipeline.rs` (génération du run hash) |
| Impact | Faible (cosmétique), mais trompeur |

**Constat :** Même seed + domain + size produit des hashs de fichier différents entre IPC et Parquet (`6a4bec30` vs `6a4bec33`). Les hashs sont séquentiels (30, 33, 35, 38, 3b, 3e), suggérant un compteur de processus incrémental plutôt qu'un hash déterministe du contenu.

**Piste :** Le hash semble être incrémenté à chaque *fichier* généré dans le processus, pas basé sur le contenu. Pour un déterminisme strict, le hash devrait être dérivé de (domain, seed, size, difficulty).

---

### C1 — `singleton_master_fraction` dead parameter

| Métrique | Valeur |
|----------|--------|
| Découvert | Vague 8, Agent 8.6 |
| Fichier suspect | `src/lib.rs:136` (non transmis à `build_pipeline_config`) |
| Tests échoués | 2 (singleton=0.0 → 138 uniques au lieu de 0) |
| Statut | Non corrigé |

**Constat :** `singleton_master_fraction` est accepté par l'API Python (`generate(singleton_master_fraction=0.0)`) mais **jamais transmis** au pipeline Rust. Le paramètre est déclaré dans `__init__.py`, reçu dans `lib.rs`, mais n'est pas passé à `build_pipeline_config()`. Le taux de singleton est fixé par `DifficultySettings` (medium = 0.30).

**Fix attendu :** Dans `src/lib.rs` (fonction de bridge Python → Rust), passer `singleton_master_fraction` à `build_pipeline_config()` :

```rust
// Actuellement : manquant
// Ajouter : singleton_master_fraction: options.singleton_master_fraction.unwrap_or(0.10),
```

Puis dans `src/pipeline.rs`, l'utiliser dans `EntityPlan` pour remplacer la valeur hardcodée de `DifficultySettings`.

---

## Observations à documenter

| Observation | Détail |
|-------------|--------|
| **F1 estimate vs actual (light)** | L'écart moyen \|delta\|=0.1272, max=0.2755 (light). L'estimate calcule en pair-wise, le test en record-level. Les deux sont corrects mais différents. À documenter dans le README. |
| **F1 réel non-monotonique** | Le F1 record-level augmente avec la difficulté (light: 0.67, hell: 0.93) car hell génère plus de duplicates (4430) que light (2577). Contre-intuitif mais attendu. |
| **Pool missing/empty → chaînes vides** | Comportement délibéré (graceful degradation), mais mérite une doc. |

---

## Résumé par fichier

| Fichier | Bugs |
|---------|------|
| `src/column_gen.rs` | **C5** — Null mask ignoré pour certains templates |
| `src/noise/dates.rs` | **C3** — Transformation date incorrecte |
| `schemas/fintech.json` | **C2** — `support_phone` pas un phone |
| `python/__init__.py` | **C4**, **C6** — Validations manquantes |
| `src/lib.rs` | **C1** — Paramètre mort |
| `src/pipeline.rs` | **C7** — Hash non déterministe |

---

## Métriques globales

| Métrique | Valeur |
|----------|:------:|
| Tests exécutés | 613 |
| Passés | 597 (97.4%) |
| Indicateurs d'échec | 16 |
| Bugs ouverts | **7** (C1–C7) |
| Bugs corrigés (v1–v6) | 60 |
| Découvert en v7 | 0 |
| Découvert en v8 | 1 (C1) |
| Découvert en v9 | 6 (C2–C7) |
