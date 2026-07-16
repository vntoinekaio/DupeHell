// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::collections::HashMap;
use std::path::Path;

use crate::pipeline::PipelineConfig;

#[derive(serde::Deserialize)]
pub struct DomainSchema {
    pub entities: Vec<EntitySchema>,
    pub hn_types: Vec<HnSchema>,
}

#[derive(serde::Deserialize)]
pub struct EntitySchema {
    pub name: String,
    pub columns: Vec<serde_json::Value>,
    #[serde(default)]
    pub fk_remaps: Vec<serde_json::Value>,
}

#[derive(serde::Deserialize)]
pub struct HnSchema {
    pub entity_type: String,
    pub config_json: String,
}

#[derive(Debug, Clone)]
struct DifficultySettings {
    singleton: f64,
    doublet: f64,
    /// Noise types active for this difficulty, weighted equally. Curated
    /// explicitly per level (rather than derived from a truncated,
    /// order-dependent countdown) so that "broad" noise types — `typo` and
    /// `visual`, which both corrupt name+address+phone+company on the same
    /// record at once (see `pipeline::noise_type_targets_column`) — never
    /// end up a *larger* share of the mix at a lower difficulty than at a
    /// higher one. A prior countdown-weight scheme caused exactly that:
    /// medium (4 active types) put 70% of its noise budget on typo+visual,
    /// vs. only ~42% for hell (8 active types), making medium duplicates
    /// *more* likely to have every strong matching field wiped out at once
    /// than hell duplicates, despite medium being meant as the easier tier.
    noise_types: &'static [&'static str],
}

const DIFFICULTY_MAP: [(&str, DifficultySettings); 4] = [
    (
        "light",
        DifficultySettings {
            singleton: 0.50,
            doublet: 0.30,
            noise_types: &["names", "dates"],
        },
    ),
    (
        "medium",
        DifficultySettings {
            singleton: 0.30,
            doublet: 0.40,
            noise_types: &["typo", "names", "dates", "identifiers"],
        },
    ),
    (
        "hard",
        DifficultySettings {
            singleton: 0.20,
            doublet: 0.30,
            noise_types: &[
                "typo",
                "visual",
                "names",
                "dates",
                "identifiers",
                "addresses",
                "companies",
                "extra",
            ],
        },
    ),
    (
        "hell",
        DifficultySettings {
            singleton: 0.10,
            doublet: 0.20,
            noise_types: &[
                "typo",
                "visual",
                "names",
                "dates",
                "identifiers",
                "addresses",
                "companies",
                "extra",
            ],
        },
    ),
];

pub fn default_singleton_master_fraction(difficulty: &str) -> f64 {
    difficulty_settings(difficulty).singleton
}

fn difficulty_settings(difficulty: &str) -> DifficultySettings {
    DIFFICULTY_MAP
        .iter()
        .find(|(name, _)| *name == difficulty)
        .map(|(_, s)| s.clone())
        .unwrap_or_else(|| DIFFICULTY_MAP[1].1.clone())
}

/// Generate a domain-unique run ID based on the current Unix timestamp (hex).
pub fn chrono_now() -> String {
    let start = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{:x}", start.as_secs())
}

/// Deterministic run ID derived from generation parameters, so the same
/// (domain, size, seed, difficulty, hard_neg_ratio) always produces the same
/// output filename regardless of output format (IPC vs Parquet) or how many
/// times it's run.
pub fn deterministic_run_id(
    domain: &str,
    size: usize,
    seed: u64,
    difficulty: &str,
    hard_neg_ratio: f64,
) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    domain.hash(&mut hasher);
    size.hash(&mut hasher);
    seed.hash(&mut hasher);
    difficulty.hash(&mut hasher);
    hard_neg_ratio.to_bits().hash(&mut hasher);
    format!("{}_{:x}", domain, hasher.finish())
}

/// Load and parse a domain schema JSON file.
///
/// On failure, the error message includes the path attempted and a hint listing
/// available domains found in the same directory.
pub fn load_schema(domain: &str, schemas_dir: &Path) -> Result<DomainSchema, String> {
    let path = schemas_dir.join(format!("{domain}.json"));
    // Case-sensitive exact match against the actual schema file names, so
    // "KYC" is rejected the same way on every OS — on a case-insensitive
    // filesystem (Windows), `read_to_string` alone would silently succeed
    // for "KYC" via the "kyc.json" file, producing a different run hash
    // than "kyc" for what the user intended to be the same domain.
    let available = available_domain_names(schemas_dir);
    if !available.iter().any(|d| d == domain) {
        let hint = if available.is_empty() {
            "no schemas found".to_string()
        } else {
            available.join(", ")
        };
        return Err(format!(
            "schema file not found for domain '{domain}' at {path:?}. \
             Available domains ({hint})"
        ));
    }
    let data =
        std::fs::read_to_string(&path).map_err(|e| format!("cannot read schema {path:?}: {e}"))?;
    serde_json::from_str(&data).map_err(|e| format!("cannot parse schema {domain}.json: {e}"))
}

/// List available domain names (without .json extension) in a directory.
fn available_domain_names(dir: &Path) -> Vec<String> {
    match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "json")
                    .unwrap_or(false)
            })
            .filter_map(|e| {
                e.path()
                    .file_stem()
                    .map(|s| s.to_string_lossy().to_string())
            })
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Build a `PipelineConfig` from CLI / Python parameters and a parsed schema.
///
/// Validates `size >= 10`, distributes singleton/doublet/triplet records
/// across entities, and assigns noise weights per difficulty setting.
#[allow(clippy::too_many_arguments)]
pub fn build_pipeline_config(
    domain: &str,
    size: usize,
    seed: u64,
    difficulty: &str,
    hard_neg_ratio: f64,
    singleton_master_fraction: f64,
    schema: &DomainSchema,
    run_id: &str,
    output_format: &str,
    graph_enabled: bool,
    graph_format: &str,
) -> Result<PipelineConfig, String> {
    if size < 10 {
        return Err(format!("size must be >= 10, got {size}"));
    }
    if schema.entities.is_empty() {
        return Err(format!("schema for domain '{domain}' has no entities"));
    }
    if !(0.0..=1.0).contains(&singleton_master_fraction) {
        return Err(format!(
            "singleton_master_fraction must be in [0.0, 1.0], got {singleton_master_fraction}"
        ));
    }
    let ds = difficulty_settings(difficulty);
    let total = size;

    let n_singleton = (total as f64 * singleton_master_fraction) as usize;
    let n_doublet_float = total as f64 * ds.doublet;
    let mut n_doublet = n_doublet_float as usize;
    if !n_doublet.is_multiple_of(2) {
        n_doublet -= 1;
    }
    if n_singleton + n_doublet > total {
        return Err(format!(
            "singleton_master_fraction {singleton_master_fraction} leaves no room for this \
             difficulty's doublet share ({:.2}); reduce singleton_master_fraction",
            ds.doublet
        ));
    }
    let mut n_triplet = total - n_singleton - n_doublet;
    let r = n_triplet % 3;
    if r != 0 {
        n_triplet -= r;
    }
    let total_unique = n_singleton + n_doublet / 2 + n_triplet / 3;
    let n_duplicates = total.max(total_unique) - total_unique;

    let total_ratio: f64 = schema.entities.iter().map(|_| 1.0).sum::<f64>();
    let raw_floats: Vec<(&str, f64)> = schema
        .entities
        .iter()
        .map(|e| (e.name.as_str(), total_unique as f64 / total_ratio))
        .collect();

    let mut floor_map: HashMap<&str, usize> = HashMap::new();
    for (name, r) in &raw_floats {
        floor_map.insert(name, r.max(2.0) as usize);
    }
    let floor_sum: usize = floor_map.values().sum();
    let need = total_unique.max(floor_sum) - floor_sum;
    if need > 0 {
        let mut remainders: Vec<(&str, f64)> = raw_floats
            .iter()
            .map(|(n, r)| (*n, r - r.floor()))
            .collect();
        remainders.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        for (name, _) in remainders.iter().take(need) {
            *floor_map.get_mut(name).unwrap_or(&mut 0) += 1;
        }
    }

    let dup_ratios: Vec<(&str, f64)> = schema
        .entities
        .iter()
        .map(|e| {
            (
                e.name.as_str(),
                *floor_map.get(e.name.as_str()).unwrap_or(&2) as f64 / total_unique as f64,
            )
        })
        .collect();
    let mut dup_floor: HashMap<&str, usize> = HashMap::new();
    for (name, r) in &dup_ratios {
        dup_floor.insert(name, (n_duplicates as f64 * r) as usize);
    }
    let dup_sum: usize = dup_floor.values().sum();
    let dup_need = n_duplicates.max(dup_sum) - dup_sum;
    if dup_need > 0 {
        let mut remainders: Vec<(&str, f64)> = dup_ratios
            .iter()
            .map(|(n, r)| (*n, r - r.floor()))
            .collect();
        remainders.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        for (name, _) in remainders.iter().take(dup_need) {
            *dup_floor.get_mut(name).unwrap_or(&mut 0) += 1;
        }
    }

    let noise_count = ds.noise_types.len();
    let noise_weights: Vec<f64> = vec![1.0 / noise_count as f64; noise_count];

    let mut entity_plans = Vec::new();
    for entity in &schema.entities {
        let n_base = *floor_map.get(entity.name.as_str()).unwrap_or(&2);
        let n_dup = *dup_floor.get(entity.name.as_str()).unwrap_or(&0);

        let mut noise_entries = Vec::new();
        if n_dup > 0 {
            let mut counts: Vec<usize> = noise_weights
                .iter()
                .map(|w| (w * n_dup as f64) as usize)
                .collect();
            let count_sum: usize = counts.iter().sum();
            if count_sum < n_dup {
                *counts.last_mut().unwrap_or(&mut 0) += n_dup - count_sum;
            }
            for (i, noise_type) in ds.noise_types.iter().enumerate() {
                if counts[i] == 0 {
                    continue;
                }
                noise_entries.push(serde_json::json!({
                    "noise_type": noise_type,
                    "columns": [],
                    "count": counts[i],
                }));
            }
        }

        // Infer identifier column: prefer {entity_name}_id, then id, then first _id column
        let identifier_col: Option<String> = {
            let entity_id_name = format!("{}_id", entity.name);
            let col_names: Vec<&str> = entity
                .columns
                .iter()
                .filter_map(|c| c.get("name").and_then(|v| v.as_str()))
                .collect();
            if col_names.contains(&entity_id_name.as_str()) {
                Some(entity_id_name)
            } else if col_names.contains(&"id") {
                Some("id".to_string())
            } else {
                col_names
                    .iter()
                    .find(|n| n.ends_with("_id"))
                    .map(|n| (*n).to_string())
            }
        };

        let columns_json = serde_json::to_string(&entity.columns)
            .map_err(|e| format!("serialize columns: {e}"))?;

        entity_plans.push(serde_json::json!({
            "name": entity.name,
            "n_base": n_base,
            "n_dup": n_dup,
            "identifier_col": identifier_col,
            "columns_json": columns_json,
            "noise_types": noise_entries,
            "fk_remaps": entity.fk_remaps,
        }));
    }

    let n_hard_neg = (size as f64 * hard_neg_ratio * 0.05) as usize;
    let hn_per_type = n_hard_neg / schema.hn_types.len().max(1);
    let hard_neg_types: Vec<serde_json::Value> = schema
        .hn_types
        .iter()
        .map(|hn| {
            serde_json::json!({
                "entity_type": hn.entity_type,
                "config_json": hn.config_json,
                "count": hn_per_type,
            })
        })
        .collect();

    let config = serde_json::json!({
        "domain": domain,
        "size": size,
        "seed": seed,
        "difficulty": difficulty,
        "output_format": output_format,
        "run_id": run_id,
        "entity_plans": entity_plans,
        "hard_neg_types": hard_neg_types,
        "hard_neg_ratio": hard_neg_ratio,
        "graph_enabled": graph_enabled,
        "graph_format": graph_format,
    });

    serde_json::from_value(config).map_err(|e| format!("build PipelineConfig: {e}"))
}
