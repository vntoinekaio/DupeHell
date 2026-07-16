// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use serde::Serialize;

use crate::schema::{DomainSchema, build_pipeline_config};

/// Noise destructiveness per column name pattern.
fn contains_any(s: &str, patterns: &[&str]) -> bool {
    patterns.iter().any(|p| s.contains(p))
}

/// How useful is this column for record linkage (0.0 = useless, 1.0 = excellent).
fn match_utility(col_name: &str, col_type: &str) -> f64 {
    match col_type {
        "boolean" => 0.0,
        "int" | "float" => 0.2,
        _ => {
            let lower = col_name.to_lowercase();
            if contains_any(&lower, &["record_id", "row_id"]) {
                0.0
            } else if lower.ends_with("_id") {
                0.3
            } else if contains_any(&lower, &["name", "email", "address", "phone", "ssn", "tax"]) {
                1.0
            } else if contains_any(
                &lower,
                &["date", "birth", "city", "country", "state", "postal"],
            ) {
                0.7
            } else if contains_any(&lower, &["company", "legal", "trading", "registration"]) {
                0.8
            } else {
                0.5
            }
        }
    }
}

/// Noise destructiveness per column name pattern (0.0 = never, 1.0 = always destroyed).
fn base_noise_damage(col_name: &str, col_type: &str) -> f64 {
    let lower = col_name.to_lowercase();
    match col_type {
        "boolean" => 0.0,
        "int" | "float" => 0.0,
        "date" | "datetime" => {
            if contains_any(&lower, &["birth", "dob"]) {
                0.3
            } else {
                0.2
            }
        }
        _ => {
            if contains_any(&lower, &["email", "ssn", "phone", "mobile", "telephone"]) {
                0.8
            } else if contains_any(
                &lower,
                &[
                    "tax_id",
                    "registration",
                    "national_id",
                    "passport",
                    "account",
                    "barcode",
                    "pan",
                    "medicare",
                ],
            ) {
                0.6
            } else if contains_any(
                &lower,
                &[
                    "first_name",
                    "last_name",
                    "given_name",
                    "family_name",
                    "middle_name",
                ],
            ) {
                0.4
            } else if contains_any(
                &lower,
                &["address", "street", "city", "postal", "state", "country"],
            ) {
                0.5
            } else if contains_any(&lower, &["date", "birth", "dob"]) {
                0.3
            } else {
                // "company"/"legal"/"trading" and any other column fall back
                // to the same default weight.
                0.4
            }
        }
    }
}

// ── HN column poisoning ──────────────────────────────────────────────────

fn parse_hn_id_fields(config_json: &str) -> Vec<String> {
    #[derive(serde::Deserialize)]
    struct HnConfigLight {
        #[serde(default)]
        pattern: String,
        #[serde(default)]
        id_fields: Vec<String>,
        #[serde(default)]
        mix_field: String,
        #[serde(default)]
        first_name_col: String,
        #[serde(default)]
        last_name_col: String,
        #[serde(default)]
        dob_col: String,
        #[serde(default)]
        email_col: String,
        #[serde(default)]
        ssn_col: String,
        #[serde(default)]
        phone_col: String,
        #[serde(default)]
        address_fields: Vec<String>,
    }

    let cfg: HnConfigLight = serde_json::from_str(config_json).unwrap_or(HnConfigLight {
        pattern: String::new(),
        id_fields: vec![],
        mix_field: String::new(),
        first_name_col: String::new(),
        last_name_col: String::new(),
        dob_col: String::new(),
        email_col: String::new(),
        ssn_col: String::new(),
        phone_col: String::new(),
        address_fields: vec![],
    });

    match cfg.pattern.as_str() {
        "same_field" => cfg.id_fields,
        "mix_identifier" => {
            if cfg.mix_field.is_empty() {
                vec![]
            } else {
                vec![cfg.mix_field]
            }
        }
        "same_name_different_everything" => vec![cfg.first_name_col, cfg.last_name_col],
        "same_email" => vec![cfg.email_col],
        "same_ssn" => vec![cfg.ssn_col],
        "same_phone" => vec![cfg.phone_col],
        "same_address" => {
            if cfg.address_fields.is_empty() {
                vec!["address_line1".into(), "postal_code".into()]
            } else {
                cfg.address_fields
            }
        }
        "same_name_dob" => vec![cfg.first_name_col, cfg.last_name_col, cfg.dob_col],
        _ => vec![],
    }
    .into_iter()
    .filter(|s| !s.is_empty())
    .collect()
}

// ── Column descriptors ──────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct ColReliability {
    pub name: String,
    pub col_type: String,
    pub noise_damage: f64,
    pub hn_risk: f64,
    pub reliability: f64,
}

#[derive(Debug, Serialize)]
pub struct EntityDifficulty {
    pub name: String,
    pub n_base: usize,
    pub n_dup: usize,
    pub true_pairs: usize,
    pub hard_neg_pairs: usize,
    pub guaranteed_fp: usize,
    pub guaranteed_fn: usize,
    pub columns: Vec<ColReliability>,
}

#[derive(Debug, Serialize)]
pub struct DifficultyReport {
    pub domain: String,
    pub difficulty: String,
    pub size: usize,
    pub total_true_pairs: usize,
    pub total_hard_neg_pairs: usize,
    pub total_guaranteed_fp: usize,
    pub total_guaranteed_fn: usize,
    pub precision_max: f64,
    pub recall_max: f64,
    pub f1_max: f64,
    pub entities: Vec<EntityDifficulty>,
}

// ── Estimator ────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct ColDefLight {
    name: String,
    #[serde(rename = "type", default = "default_str")]
    col_type: String,
}

fn default_str() -> String {
    "string".to_string()
}

pub fn estimate_difficulty(
    domain: &str,
    size: usize,
    seed: u64,
    difficulty: &str,
    hard_neg_ratio: f64,
    schema: &DomainSchema,
) -> Result<DifficultyReport, String> {
    let run_id =
        crate::schema::deterministic_run_id(domain, size, seed, difficulty, hard_neg_ratio);
    let singleton_master_fraction = crate::schema::default_singleton_master_fraction(difficulty);
    let config = build_pipeline_config(
        domain,
        size,
        seed,
        difficulty,
        hard_neg_ratio,
        singleton_master_fraction,
        schema,
        &run_id,
        "ipc",
        false,
        "ipc",
    )?;

    // Build a map: entity_name -> HN id_fields
    let mut hn_id_fields: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for hn in &schema.hn_types {
        let fields = parse_hn_id_fields(&hn.config_json);
        hn_id_fields
            .entry(hn.entity_type.clone())
            .or_default()
            .extend(fields);
    }

    let mut entities = Vec::new();
    let mut total_true_pairs = 0usize;
    let mut total_hard_neg_pairs = 0usize;
    let mut total_guaranteed_fp = 0usize;
    let mut total_guaranteed_fn = 0usize;

    for plan in &config.entity_plans {
        let poisoned: std::collections::HashSet<String> = hn_id_fields
            .get(&plan.name)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default();

        // Parse columns from columns_json
        let cols: Vec<ColDefLight> = serde_json::from_str(&plan.columns_json)
            .map_err(|e| format!("parse columns for '{}': {}", plan.name, e))?;

        // Count HN pairs targeting this entity
        let hn_pairs: usize = config
            .hard_neg_types
            .iter()
            .filter(|h| h.entity_type == plan.name)
            .map(|h| h.count)
            .sum();

        // True duplicate pairs per entity = n_dup / 2 (all duplicates are paired)
        let n_dup: usize = plan.noise_types.iter().map(|n| n.count).sum();
        let true_pairs = n_dup / 2;

        // Column analysis: each duplicate is hit by exactly one of `plan.noise_types`
        // (see `pipeline::apply_noise_to_batch`), so a column's chance of being
        // touched is the summed weight of the active types that actually target it
        // — not a flat scalar. Reuses `pipeline::noise_type_targets_column`, the
        // same predicate real generation uses, so this can't drift from reality.
        let n_dup_f = n_dup.max(1) as f64;
        let mut col_reliability = Vec::new();
        let mut best_fn_reliability = 0.0f64;
        let mut best_fp_reliability = 0.0f64; // higher = more FP-safe

        for col in &cols {
            let base_damage = base_noise_damage(&col.name, &col.col_type);
            let p_touched: f64 = plan
                .noise_types
                .iter()
                .filter(|n| crate::pipeline::noise_type_targets_column(&n.noise_type, &col.name))
                .map(|n| n.count as f64 / n_dup_f)
                .sum::<f64>()
                .min(1.0);
            let damage = base_damage * p_touched;
            let util = match_utility(&col.name, &col.col_type);
            let is_hn_id = poisoned.contains(&col.name);
            let hn_risk = if is_hn_id { 1.0 } else { 0.0 };

            // Reliability for finding TRUE matches: utility × noise survival
            let rel_fn = util * (1.0 - damage);

            // Reliability for AVOIDING false positives: utility × freedom from HN poisoning
            let rel_fp = util * (1.0 - hn_risk) * (1.0 - damage);

            if rel_fn > best_fn_reliability {
                best_fn_reliability = rel_fn;
            }
            if rel_fp > best_fp_reliability {
                best_fp_reliability = rel_fp;
            }

            col_reliability.push(ColReliability {
                name: col.name.clone(),
                col_type: col.col_type.clone(),
                noise_damage: damage,
                hn_risk,
                reliability: rel_fn.min(rel_fp),
            });
        }

        let guaranteed_fp = if hn_pairs > 0 {
            // FP guaranteed when even the best non-HN-safe column is unreliable
            (hn_pairs as f64 * (1.0 - best_fp_reliability)) as usize
        } else {
            0
        };

        let guaranteed_fn = (true_pairs as f64 * (1.0 - best_fn_reliability)) as usize;

        total_true_pairs += true_pairs;
        total_hard_neg_pairs += hn_pairs;
        total_guaranteed_fp += guaranteed_fp;
        total_guaranteed_fn += guaranteed_fn;

        entities.push(EntityDifficulty {
            name: plan.name.clone(),
            n_base: plan.n_base,
            n_dup,
            true_pairs,
            hard_neg_pairs: hn_pairs,
            guaranteed_fp,
            guaranteed_fn,
            columns: col_reliability,
        });
    }

    let tp = total_true_pairs.max(1) as f64;
    let fp = total_guaranteed_fp as f64;
    let fn_ = total_guaranteed_fn as f64;

    let precision_max = tp / (tp + fp);
    let recall_max = tp / (tp + fn_);
    let f1_max = 2.0 * precision_max * recall_max / (precision_max + recall_max);

    Ok(DifficultyReport {
        domain: domain.to_string(),
        difficulty: difficulty.to_string(),
        size,
        total_true_pairs,
        total_hard_neg_pairs,
        total_guaranteed_fp,
        total_guaranteed_fn,
        precision_max,
        recall_max,
        f1_max,
        entities,
    })
}
