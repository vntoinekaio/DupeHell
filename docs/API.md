<!-- DupeHell -- MIT License . Educational Use Only -->
<!-- EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses. -->

# API Reference

## Python API

### `generate()`

```python
def generate(
    domain: str,
    size: int,
    seed: int = 42,
    difficulty: str = "medium",
    output_dir: str = ".",
    pools_dir: str = "./assets/pools",
    schemas_dir: str = "./schemas",
) -> GenerateResult
```

Generate a synthetic dataset for a given domain.

**Parameters:**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `domain` | `str` | — | Domain name (e.g. `"kyc"`, `"publishing"`). Must match a file in `schemas/`. |
| `size` | `int` | — | Number of base records (before duplicates & hard negatives). |
| `seed` | `int` | `42` | PRNG seed for deterministic output. |
| `difficulty` | `str` | `"medium"` | One of `"light"`, `"medium"`, `"hard"`, `"hell"`. Controls duplicate ratios and noise intensity. |
| `output_dir` | `str` | `"."` | Directory for output `.ipc` / `.parquet` files. |
| `pools_dir` | `str` | `"./assets/pools"` | Path to pool data files. |
| `schemas_dir` | `str` | `"./schemas"` | Path to domain schema files. |

**Returns:** [`GenerateResult`](#generateresult)

**Raises:** `PyValueError` (Rust), `ValidationError` (Pydantic) if schema is invalid.

---

### `GenerateResult`

Returned by `generate()`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `dataset` | `str` | Path to the generated dataset file |
| `ground_truth` | `str` | Path to the ground-truth labels file |
| `total_records` | `int` | Total records in the dataset |
| `exact_dups` | `int` | Exact duplicate rows |
| `hard_negs` | `int` | Hard negative pairs |
| `uniques` | `int` | Unique / singleton records |
| `masters` | `int` | Distinct master entities |

---

### `estimate_difficulty()`

```python
def estimate_difficulty(
    domain: str,
    size: int = 1_000_000,
    seed: int = 42,
    difficulty: str = "medium",
    schemas_dir: str = "./schemas",
) -> DifficultyReport
```

Estimate the theoretical maximum F1 score for a given configuration without
generating data.

**Parameters:**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `domain` | `str` | — | Domain name |
| `size` | `int` | `1000000` | Number of base records |
| `seed` | `int` | `42` | PRNG seed |
| `difficulty` | `str` | `"medium"` | `light`, `medium`, `hard`, or `hell` |
| `schemas_dir` | `str` | `"./schemas"` | Path to schema files |

**Returns:** [`DifficultyReport`](#difficultyreport)

---

### `load_and_validate()`

```python
def load_and_validate(path: str | Path) -> DomainSchema
```

Load and validate a domain schema file with Pydantic.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `path` | `str` or `Path` | Path to a schema JSON file |

**Returns:** [`DomainSchema`](#domainschema)

**Raises:** `FileNotFoundError`, `ValidationError`

---

### Models

#### `DomainSchema`

| Field | Type | Description |
|-------|------|-------------|
| `domain` | `str` | Domain name |
| `entities` | `list[EntitySchema]` | Entity definitions (min 1) |
| `hn_types` | `list[HnSchema]` | Hard-negative type configurations |

#### `EntitySchema`

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Entity name |
| `columns` | `list[ColumnDef]` | Column definitions (min 1) |
| `fk_remaps` | `list[FkRemap]` | Foreign key remapping rules |

#### `ColumnDef`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | — | Column name |
| `type` | `str` | `"string"` | One of `string`, `int`, `float`, `boolean`, `date`, `datetime` |
| `pool_name` | `str` or `None` | `None` | Pool for random value selection |
| `nullable` | `bool` | `True` | Whether the column allows nulls |
| `null_rate_default` | `float` | `0.0` | Default null rate |
| `conditions` | `list[ColCondition]` | `[]` | Conditional column logic |

#### `ColCondition`

| Field | Type | Description |
|-------|------|-------------|
| `depends_on` | `str` | Name of the dependency column |
| `op` | `str` | One of `eq`, `ne`, `in`, `not_in`, `gt`, `gte`, `lt`, `lte` |
| `value` | `Any` | Comparison value |
| `action` | `str` | One of `set_null`, `set_value`, `set_pool` |
| `action_value` | `Any` or `None` | Value to apply (for `set_value` / `set_pool`) |

#### `FkRemap`

| Field | Type | Description |
|-------|------|-------------|
| `source_col` | `str` | Source column to remap |
| `target_entity` | `str` | Target entity to reference |

#### `HnSchema`

| Field | Type | Description |
|-------|------|-------------|
| `entity_type` | `str` | Entity type for hard negatives |
| `config_json` | `str` | JSON string with HN configuration |

---

### `DifficultyReport`

| Field | Type | Description |
|-------|------|-------------|
| `domain` | `str` | Domain name |
| `difficulty` | `str` | Difficulty level |
| `size` | `int` | Requested record count |
| `total_true_pairs` | `int` | Number of true duplicate pairs |
| `total_hard_neg_pairs` | `int` | Number of hard-negative pairs |
| `total_guaranteed_fp` | `int` | Estimated unavoidable false positives |
| `total_guaranteed_fn` | `int` | Estimated unavoidable false negatives |
| `precision_max` | `float` | Theoretical maximum precision |
| `recall_max` | `float` | Theoretical maximum recall |
| `f1_max` | `float` | Theoretical maximum F1 score |
| `entities` | `list[EntityDifficulty]` | Per-entity breakdown |

#### `EntityDifficulty`

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Entity name |
| `n_base` | `int` | Unique entity count |
| `n_dup` | `int` | Duplicate record count |
| `true_pairs` | `int` | True duplicate pairs |
| `hard_neg_pairs` | `int` | Hard-negative pairs targeting this entity |
| `guaranteed_fp` | `int` | Estimated unavoidable FP |
| `guaranteed_fn` | `int` | Estimated unavoidable FN |
| `columns` | `list[ColReliability]` | Per-column reliability scores |

#### `ColReliability`

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Column name |
| `col_type` | `str` | Column data type |
| `noise_damage` | `float` | Probability this column is corrupted by noise (0–1) |
| `hn_risk` | `float` | Whether this column is a hard-negative ID field (0 or 1) |
| `reliability` | `float` | Combined reliability for matching (0–1) |

---

## Rust API (binary)

### CLI

```bash
dupehell2 [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--domain <DOMAIN>` | `kyc` | Domain schema to use |
| `--size <SIZE>` | `1000000` | Number of base records |
| `--seed <SEED>` | `42` | PRNG seed |
| `--difficulty <LEVEL>` | `medium` | `light`, `medium`, `hard`, or `hell` |
| `--estimate` | — | Estimate theoretical max F1 and exit (no data) |
| `--output-format <FMT>` | `ipc` | `ipc` or `parquet` |
| `--parquet` | — | Shorthand for `--output-format parquet` |
| `--output-dir <PATH>` | `.` | Output directory |
| `--hard-neg-ratio <FLOAT>` | `0.3` | Hard negative ratio |
| `--singleton-master-fraction <FLOAT>` | `0.1` | Singleton fraction |
| `--pools-dir <PATH>` | `../dupehell/assets/pools` | Pool data directory |
| `--schemas-dir <PATH>` | `schemas` | Schema directory |

### Library

The Rust crate exposes:

| Function | Module | Description |
|----------|--------|-------------|
| `load_schema()` | `schema` | Load a domain schema from JSON |
| `build_pipeline_config()` | `schema` | Build pipeline configuration |
| `run_pipeline()` | `pipeline` | Run the generation pipeline |
| `estimate_difficulty()` | `difficulty` | Estimate theoretical max F1 |
| `Context` | `context` | Runtime context (pools, watermark)