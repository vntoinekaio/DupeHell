mod buf_gen;
mod canary;
mod column_gen;
mod context;
mod entity_gen;
mod fast_template;
mod fk_remap;
mod gt;
mod hn_common;
mod pipeline;
mod pool_lookup;
mod rng;

mod noise;

use std::collections::HashMap;
use std::path::PathBuf;

use clap::Parser;

use context::Context;
use pipeline::{PipelineConfig, run_pipeline};

/// Difficulty preset controlling duplicate rate and noise intensity.
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

#[derive(Parser)]
#[command(name = "dupehell2", version = "0.4.0")]
struct Cli {
    #[arg(long, default_value = "kyc")]
    domain: String,

    #[arg(long, default_value_t = 1_000_000)]
    size: usize,

    #[arg(long, default_value_t = 42)]
    seed: u64,

    #[arg(long, default_value = "medium")]
    difficulty: String,

    #[arg(long, default_value = "ipc")]
    output_format: String,

    #[arg(long)]
    parquet: bool,

    #[arg(long, default_value = ".")]
    output_dir: PathBuf,

    #[arg(long, default_value_t = 0.3)]
    hard_neg_ratio: f64,

    #[arg(long, default_value_t = 0.10)]
    singleton_master_fraction: f64,

    #[arg(long, default_value = "../dupehell/assets/pools")]
    pools_dir: PathBuf,

    #[arg(long, default_value = "schemas")]
    schemas_dir: PathBuf,
}

#[derive(serde::Deserialize)]
struct DomainSchema {
    entities: Vec<EntitySchema>,
    hn_types: Vec<HnSchema>,
}

#[derive(serde::Deserialize)]
struct EntitySchema {
    name: String,
    columns: Vec<serde_json::Value>,
    #[serde(default)]
    fk_remaps: Vec<serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct HnSchema {
    entity_type: String,
    config_json: String,
}

fn load_schema(domain: &str, schemas_dir: &PathBuf) -> Result<DomainSchema, String> {
    let path = schemas_dir.join(format!("{domain}.json"));
    let data =
        std::fs::read_to_string(&path).map_err(|e| format!("cannot load schema {path:?}: {e}"))?;
    serde_json::from_str(&data).map_err(|e| format!("cannot parse schema {domain}.json: {e}"))
}

fn difficulty_settings(difficulty: &str) -> DifficultySettings {
    DIFFICULTY_MAP
        .iter()
        .find(|(name, _)| *name == difficulty)
        .map(|(_, s)| s.clone())
        .unwrap_or_else(|| DIFFICULTY_MAP[1].1.clone()) // default medium
}

/// Noise types for duplicate records — simulates real-world data entry errors.
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

fn build_pipeline_config(cli: &Cli, schema: &DomainSchema) -> Result<PipelineConfig, String> {
    let ds = difficulty_settings(&cli.difficulty);
    let total = cli.size;

    // Compute singleton/doublet/triplet counts
    let n_singleton = (total as f64 * ds.singleton) as usize;
    let n_doublet_float = total as f64 * ds.doublet;
    let mut n_doublet = n_doublet_float as usize;
    if n_doublet % 2 != 0 {
        n_doublet -= 1;
    }
    let mut n_triplet = total - n_singleton - n_doublet;
    let r = n_triplet % 3;
    if r != 0 {
        n_triplet -= r;
    }
    let total_unique = n_singleton + n_doublet / 2 + n_triplet / 3;
    let n_duplicates = total.max(total_unique) - total_unique;

    // Compute entity counts via largest-remainder proportional distribution
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

    // Distribute n_duplicates across entities (same ratio as base)
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

    // Build entity plans
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

    // Build HN type configs
    let n_hard_neg = (cli.size as f64 * cli.hard_neg_ratio * 0.05) as usize;
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
        "domain": cli.domain,
        "size": cli.size,
        "seed": cli.seed,
        "difficulty": cli.difficulty,
        "output_format": if cli.parquet { "parquet" } else { &cli.output_format },
        "run_id": format!("{}_{}", cli.domain, chrono_now()),
        "entity_plans": entity_plans,
        "hard_neg_types": hard_neg_types,
        "hard_neg_ratio": cli.hard_neg_ratio,
    });

    serde_json::from_value(config).map_err(|e| format!("build PipelineConfig: {e}"))
}

fn chrono_now() -> String {
    let start = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{:x}", start.as_secs())
}

fn main() {
    let cli = Cli::parse();

    let schema = match load_schema(&cli.domain, &cli.schemas_dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    let mut ctx = match Context::new(&cli.domain, &cli.pools_dir.to_string_lossy()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error loading pools: {e}");
            std::process::exit(1);
        }
    };

    let config = match build_pipeline_config(&cli, &schema) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error building config: {e}");
            std::process::exit(1);
        }
    };

    ctx.enable_watermark(&config.domain, config.size, config.seed);

    eprintln!(
        "DupeHell v0.4 — {} domain, {} records [{}]",
        cli.domain.to_uppercase(),
        cli.size,
        cli.difficulty
    );
    eprintln!(
        "Entities: {} types, {} HN types",
        config.entity_plans.len(),
        config.hard_neg_types.len()
    );
    eprintln!("Output: {}/{}", cli.output_dir.display(), config.run_id);

    let t0 = std::time::Instant::now();
    let output = match run_pipeline(&ctx, &config, &cli.output_dir.to_string_lossy()) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Pipeline failed: {e}");
            std::process::exit(1);
        }
    };
    let elapsed = t0.elapsed().as_secs_f64();

    let n = output.stats.total_records;
    eprintln!(
        "\nDone in {:.3}s — {} records ({:.0} rec/s)",
        elapsed,
        n,
        n as f64 / elapsed
    );
    eprintln!(
        "  exact_dups={} hard_negs={} uniques={} masters={}",
        output.stats.exact_dups, output.stats.hard_negs, output.stats.uniques, output.stats.masters
    );
    eprintln!("  Dataset: {}", output.output_files[0]);
    eprintln!("  GT:      {}", output.gt_file);
}
