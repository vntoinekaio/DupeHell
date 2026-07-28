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
                // Unclassified generic/categorical columns (occupation,
                // risk_score, document_type, source_system, ...) are
                // descriptive, not identifying -- most duplicates of
                // different people share the same value. Kept low
                // (below the weak `_id`-suffix bucket) so a handful of
                // them can't noisy-OR their way to near-certain recall
                // when combined with genuinely informative columns.
                0.1
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
    let singleton_master_fraction = crate::schema::default_singleton_master_fraction(difficulty);
    // `run_id` is never used to name a file on this path (`--estimate` never
    // writes output) — `locale` is passed as a fixed placeholder since it
    // doesn't affect the column-level estimation model either way.
    let run_id = crate::schema::deterministic_run_id(
        domain,
        size,
        seed,
        difficulty,
        hard_neg_ratio,
        singleton_master_fraction,
        "en",
    );
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
    // Float accumulators for the precision/recall/F1 calculation itself:
    // summing per-entity `usize` counts first would floor sub-1 expected
    // failures (e.g. 1416 pairs * 3e-4 fail chance = 0.42) to exactly 0,
    // silently rounding f1_max up to a false 1.0 once enough columns are
    // combined via noisy-OR. `guaranteed_fp`/`guaranteed_fn` stay `usize`
    // in the report (display only); the ratios below use these instead.
    let mut total_guaranteed_fp_f = 0.0f64;
    let mut total_guaranteed_fn_f = 0.0f64;

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

        // Column analysis: each duplicate is hit by `config.noise_passes`
        // independent noise_type draws from `plan.noise_types` (see
        // `pipeline::apply_noise_with_retry` and the multi-pass loop around
        // it), so a column's chance of being touched *in a single pass* is
        // the summed weight of the active types that actually target it —
        // not a flat scalar. Reuses `pipeline::noise_type_targets_column`,
        // the same predicate real generation uses, so this can't drift from
        // reality. The chance of being touched at least once across all
        // passes is `1 - (1 - p_single)^passes`: this — not the raw
        // single-pass sum — is what actually differentiates a
        // higher-`passes` tier (e.g. hell) from one with more noise_types
        // categories active but fewer passes (e.g. medium). Adding
        // categories to a single pass necessarily *dilutes* every
        // category's weight (each entry's weight is `1 /
        // noise_types.len()`), including whichever one happens to guard a
        // domain's single most reliable column — so more categories alone
        // can make a tier easier on some schemas. More passes can't: the
        // union-of-independent-events probability strictly increases with
        // `passes` regardless of how small any one category's weight is.
        let n_dup_f = n_dup.max(1) as f64;
        let passes = config.noise_passes.max(1) as i32;
        let mut col_reliability = Vec::new();
        // A real ER pipeline combines evidence across columns instead of
        // relying on a single "best" one: blocking is typically OR'd
        // across a few strong candidate keys, and matching then combines
        // ALL informative surviving columns. We model both stages as
        // noisy-OR combinations (complement of the product of individual
        // failure probabilities) rather than a single max, and chain them:
        // recall = recall_blocking * recall_matching. See the plan in
        // `nifty-spinning-rocket.md` for the full rationale.
        const BLOCKING_UTILITY_THRESHOLD: f64 = 0.7;
        let mut blocking_fn_fail = 1.0f64;
        let mut matching_fn_fail = 1.0f64;
        let mut blocking_fp_fail = 1.0f64; // higher = more chance NO strong column catches the HN
        let mut matching_fp_fail = 1.0f64;

        for col in &cols {
            let base_damage = base_noise_damage(&col.name, &col.col_type);
            let p_single: f64 = plan
                .noise_types
                .iter()
                .filter(|n| crate::pipeline::noise_type_targets_column(&n.noise_type, &col.name))
                .map(|n| n.count as f64 / n_dup_f)
                .sum::<f64>()
                .min(1.0);
            let p_touched = 1.0 - (1.0 - p_single).powi(passes);
            let damage = base_damage * p_touched;
            let util = match_utility(&col.name, &col.col_type);
            let is_hn_id = poisoned.contains(&col.name);
            let hn_risk = if is_hn_id { 1.0 } else { 0.0 };

            // Reliability for finding TRUE matches: utility × noise survival
            let rel_fn = util * (1.0 - damage);

            // Reliability for AVOIDING false positives: utility × freedom from HN poisoning
            let rel_fp = util * (1.0 - hn_risk) * (1.0 - damage);

            if util > 0.0 {
                matching_fn_fail *= 1.0 - rel_fn;
                matching_fp_fail *= 1.0 - rel_fp;
                if util >= BLOCKING_UTILITY_THRESHOLD {
                    blocking_fn_fail *= 1.0 - rel_fn;
                    blocking_fp_fail *= 1.0 - rel_fp;
                }
            }

            col_reliability.push(ColReliability {
                name: col.name.clone(),
                col_type: col.col_type.clone(),
                noise_damage: damage,
                hn_risk,
                reliability: rel_fn.min(rel_fp),
            });
        }

        // No column reaches the blocking threshold on this entity: fall
        // back to the matching-stage combination alone (no separate
        // blocking gate to model).
        let recall_blocking = if blocking_fn_fail < 1.0 {
            1.0 - blocking_fn_fail
        } else {
            1.0
        };
        let precision_blocking = if blocking_fp_fail < 1.0 {
            1.0 - blocking_fp_fail
        } else {
            1.0
        };
        let recall_combined = recall_blocking * (1.0 - matching_fn_fail);
        let precision_combined = precision_blocking * (1.0 - matching_fp_fail);

        let guaranteed_fp_f = if hn_pairs > 0 {
            // FP guaranteed when NO combination of reliable columns catches the poison
            hn_pairs as f64 * (1.0 - precision_combined)
        } else {
            0.0
        };
        let guaranteed_fn_f = true_pairs as f64 * (1.0 - recall_combined);
        let guaranteed_fp = guaranteed_fp_f as usize;
        let guaranteed_fn = guaranteed_fn_f as usize;

        total_true_pairs += true_pairs;
        total_hard_neg_pairs += hn_pairs;
        total_guaranteed_fp += guaranteed_fp;
        total_guaranteed_fn += guaranteed_fn;
        total_guaranteed_fp_f += guaranteed_fp_f;
        total_guaranteed_fn_f += guaranteed_fn_f;

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
    let fp = total_guaranteed_fp_f;
    let fn_ = total_guaranteed_fn_f;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::load_schema;

    fn kyc_schema() -> DomainSchema {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("schemas");
        load_schema("kyc", &dir).expect("load kyc.json")
    }

    #[test]
    fn test_estimate_difficulty_basic_shape() {
        let schema = kyc_schema();
        let report = estimate_difficulty("kyc", 1000, 42, "medium", 0.1, &schema).unwrap();
        assert_eq!(report.domain, "kyc");
        assert_eq!(report.difficulty, "medium");
        assert_eq!(report.entities.len(), 2);
        for e in &report.entities {
            assert!(!e.columns.is_empty());
        }
    }

    #[test]
    fn test_estimate_difficulty_f1_bounds() {
        let schema = kyc_schema();
        let report = estimate_difficulty("kyc", 5000, 42, "hell", 0.1, &schema).unwrap();
        assert!(report.precision_max > 0.0 && report.precision_max <= 1.0);
        assert!(report.recall_max > 0.0 && report.recall_max <= 1.0);
        assert!(report.f1_max > 0.0 && report.f1_max <= 1.0);
    }

    #[test]
    fn test_estimate_difficulty_hell_harder_than_light() {
        // "hell" has more noise types active and a lower singleton fraction
        // than "light" — its theoretical max F1 should never be easier.
        let schema = kyc_schema();
        let light = estimate_difficulty("kyc", 5000, 42, "light", 0.1, &schema).unwrap();
        let hell = estimate_difficulty("kyc", 5000, 42, "hell", 0.1, &schema).unwrap();
        assert!(hell.f1_max <= light.f1_max);
    }

    #[test]
    fn test_estimate_difficulty_tiers_ordered() {
        // The full light > medium > hell chain, not just the endpoints —
        // this is the ordering "hard" used to violate before being folded
        // into "hell" (see the comment on `DIFFICULTY_MAP` in schema.rs).
        let schema = kyc_schema();
        let light = estimate_difficulty("kyc", 5000, 42, "light", 0.1, &schema).unwrap();
        let medium = estimate_difficulty("kyc", 5000, 42, "medium", 0.1, &schema).unwrap();
        let hell = estimate_difficulty("kyc", 5000, 42, "hell", 0.1, &schema).unwrap();
        assert!(medium.f1_max <= light.f1_max);
        assert!(hell.f1_max <= medium.f1_max);
    }

    #[test]
    fn test_estimate_difficulty_tiers_ordered_healthcare() {
        // Same chain as `test_estimate_difficulty_tiers_ordered`, but on the
        // domain that actually motivated the noise-passes redesign: with
        // the old "more categories = harder" model (single pass, hell
        // listing more noise_types than medium), healthcare's medium.f1_max
        // (0.8971) came out *lower* than hell's (0.9114 with the old
        // category-duplication patch, worse still without it) — hell
        // looked easier than medium on this schema specifically, because
        // healthcare's most reliable column across entities happens to be
        // protected by a category ("dates") that got diluted by hell's
        // larger category list. The compounding passes model must not
        // regress this.
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("schemas");
        let schema = load_schema("healthcare", &dir).expect("load healthcare.json");
        let light = estimate_difficulty("healthcare", 5000, 42, "light", 0.1, &schema).unwrap();
        let medium = estimate_difficulty("healthcare", 5000, 42, "medium", 0.1, &schema).unwrap();
        let hell = estimate_difficulty("healthcare", 5000, 42, "hell", 0.1, &schema).unwrap();
        assert!(medium.f1_max <= light.f1_max);
        assert!(hell.f1_max <= medium.f1_max);
    }

    #[test]
    fn test_estimate_difficulty_tiers_ordered_aviation() {
        // Aviation exposed a second, distinct root cause from the
        // categories/passes one above: `n_hard_neg` used to be a flat
        // `size * hard_neg_ratio * 0.05`, independent of tier, while
        // `total_true_pairs` scales a lot with the tier's singleton/doublet
        // split (light ~0.28*size, medium ~0.40*size, hell ~0.57*size).
        // With guaranteed_fp roughly constant across tiers and true_pairs
        // growing, `precision_max = tp/(tp+fp)` mechanically rose with
        // duplication volume regardless of injected noise — enough to flip
        // light (0.8280) below medium (0.8328) on this schema specifically.
        // Fixed by scaling `n_hard_neg` off `n_duplicates` instead of raw
        // `size` (`schema::build_pipeline_config`), which keeps the
        // fp/tp ratio comparable across tiers.
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("schemas");
        let schema = load_schema("aviation", &dir).expect("load aviation.json");
        let light = estimate_difficulty("aviation", 5000, 42, "light", 0.1, &schema).unwrap();
        let medium = estimate_difficulty("aviation", 5000, 42, "medium", 0.1, &schema).unwrap();
        let hell = estimate_difficulty("aviation", 5000, 42, "hell", 0.1, &schema).unwrap();
        assert!(medium.f1_max <= light.f1_max);
        assert!(hell.f1_max <= medium.f1_max);
    }

    #[test]
    fn test_estimate_difficulty_tiers_ordered_all_domains() {
        // The kyc/healthcare/aviation tests above each caught a real,
        // distinct bug -- but each only covered one hand-picked domain.
        // That gap is exactly what let 19/40 domains violate
        // `medium <= light` (categories dilution: medium's 4 equally-
        // weighted noise_types, at passes=1, diluted below light's 2)
        // stay hidden until scripts/validate_all_domains.py swept all 40
        // schemas. This test is the permanent, in-repo version of that
        // sweep so a future schema/passes change can't reintroduce it
        // without `cargo test` catching it immediately.
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("schemas");
        let eps = 1e-6;
        let mut violations = Vec::new();
        for entry in std::fs::read_dir(&dir).expect("read schemas dir") {
            let path = entry.expect("dir entry").path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let domain = path.file_stem().unwrap().to_str().unwrap().to_string();
            let schema = load_schema(&domain, &dir).expect("load schema");
            let light = estimate_difficulty(&domain, 5000, 42, "light", 0.1, &schema).unwrap();
            let medium = estimate_difficulty(&domain, 5000, 42, "medium", 0.1, &schema).unwrap();
            let hell = estimate_difficulty(&domain, 5000, 42, "hell", 0.1, &schema).unwrap();
            if medium.f1_max > light.f1_max + eps {
                violations.push(format!(
                    "{domain}: medium ({:.4}) > light ({:.4})",
                    medium.f1_max, light.f1_max
                ));
            }
            if hell.f1_max > medium.f1_max + eps {
                violations.push(format!(
                    "{domain}: hell ({:.4}) > medium ({:.4})",
                    hell.f1_max, medium.f1_max
                ));
            }
        }
        assert!(
            violations.is_empty(),
            "monotonicity violations:\n{}",
            violations.join("\n")
        );
    }

    #[test]
    fn test_estimate_difficulty_deterministic() {
        let schema = kyc_schema();
        let a = estimate_difficulty("kyc", 1000, 42, "medium", 0.1, &schema).unwrap();
        let b = estimate_difficulty("kyc", 1000, 42, "medium", 0.1, &schema).unwrap();
        assert_eq!(a.total_true_pairs, b.total_true_pairs);
        assert_eq!(a.f1_max, b.f1_max);
    }

    #[test]
    fn test_estimate_difficulty_zero_hard_neg_ratio_no_fp() {
        let schema = kyc_schema();
        let report = estimate_difficulty("kyc", 1000, 42, "medium", 0.0, &schema).unwrap();
        assert_eq!(report.total_hard_neg_pairs, 0);
        assert_eq!(report.total_guaranteed_fp, 0);
    }
}
