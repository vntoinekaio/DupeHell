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
    dup_noise_count: usize,
}

const DIFFICULTY_MAP: [(&str, DifficultySettings); 4] = [
    (
        "light",
        DifficultySettings {
            singleton: 0.50,
            doublet: 0.30,
            dup_noise_count: 2,
        },
    ),
    (
        "medium",
        DifficultySettings {
            singleton: 0.30,
            doublet: 0.40,
            dup_noise_count: 4,
        },
    ),
    (
        "hard",
        DifficultySettings {
            singleton: 0.20,
            doublet: 0.30,
            dup_noise_count: 8,
        },
    ),
    (
        "hell",
        DifficultySettings {
            singleton: 0.10,
            doublet: 0.20,
            dup_noise_count: 12,
        },
    ),
];

fn difficulty_settings(difficulty: &str) -> DifficultySettings {
    DIFFICULTY_MAP
        .iter()
        .find(|(name, _)| *name == difficulty)
        .map(|(_, s)| s.clone())
        .unwrap_or_else(|| DIFFICULTY_MAP[1].1.clone())
}

const NOISE_TYPES: &[&str] = &[
    "typo",
    "visual",
    "names",
    "dates",
    "identifiers",
    "addresses",
    "companies",
    "extra",
];

pub fn chrono_now() -> String {
    let start = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{:x}", start.as_secs())
}

pub fn load_schema(domain: &str, schemas_dir: &Path) -> Result<DomainSchema, String> {
    let path = schemas_dir.join(format!("{domain}.json"));
    let data =
        std::fs::read_to_string(&path).map_err(|e| format!("cannot load schema {path:?}: {e}"))?;
    serde_json::from_str(&data).map_err(|e| format!("cannot parse schema {domain}.json: {e}"))
}

pub fn build_pipeline_config(
    domain: &str,
    size: usize,
    seed: u64,
    difficulty: &str,
    hard_neg_ratio: f64,
    schema: &DomainSchema,
    run_id: &str,
) -> Result<PipelineConfig, String> {
    let ds = difficulty_settings(difficulty);
    let total = size;

    let n_singleton = (total as f64 * ds.singleton) as usize;
    let n_doublet_float = total as f64 * ds.doublet;
    let mut n_doublet = n_doublet_float as usize;
    if !n_doublet.is_multiple_of(2) {
        n_doublet -= 1;
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

    let noise_count = ds.dup_noise_count.min(NOISE_TYPES.len());
    let noise_weights_str: Vec<f64> = (0..noise_count).map(|i| (noise_count - i) as f64).collect();
    let w_sum: f64 = noise_weights_str.iter().sum();
    let noise_weights: Vec<f64> = noise_weights_str.iter().map(|w| w / w_sum).collect();

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
            for (i, noise_type) in NOISE_TYPES.iter().enumerate().take(noise_count) {
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

        let columns_json = serde_json::to_string(&entity.columns)
            .map_err(|e| format!("serialize columns: {e}"))?;

        entity_plans.push(serde_json::json!({
            "name": entity.name,
            "n_base": n_base,
            "identifier_col": null,
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
        "output_format": "ipc",
        "run_id": run_id,
        "entity_plans": entity_plans,
        "hard_neg_types": hard_neg_types,
        "hard_neg_ratio": hard_neg_ratio,
    });

    serde_json::from_value(config).map_err(|e| format!("build PipelineConfig: {e}"))
}
