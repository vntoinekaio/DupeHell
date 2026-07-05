// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, AsArray, StringArray, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde::Deserialize;

use crate::context::Context;
use crate::rng::Rng;

// ── Config types ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    pub domain: String,
    pub size: usize,
    pub seed: u64,
    pub difficulty: String,
    pub output_format: String,
    pub run_id: String,

    pub entity_plans: Vec<EntityPlan>,
    pub hard_neg_types: Vec<HnTypeConfig>,

    pub hard_neg_ratio: f64,
}

#[derive(Debug, Deserialize)]
pub struct EntityPlan {
    pub name: String,
    pub n_base: usize,
    pub identifier_col: Option<String>,
    pub columns_json: String,
    pub noise_types: Vec<NoisePlanEntry>,
    pub fk_remaps: Vec<FkRemapEntry>,
}

#[derive(Debug, Deserialize)]
pub struct NoisePlanEntry {
    pub noise_type: String,
    pub columns: Vec<String>,
    pub count: usize,
}

#[derive(Debug, Deserialize)]
pub struct FkRemapEntry {
    pub source_col: String,
    pub target_entity: String,
}

#[derive(Debug, Deserialize)]
pub struct HnTypeConfig {
    pub entity_type: String,
    pub config_json: String,
    pub count: usize,
}

#[derive(Debug)]
pub struct PipelineOutput {
    pub output_files: Vec<String>,
    pub gt_file: String,
    pub stats: PipelineStats,
}

#[derive(Debug)]
pub struct PipelineStats {
    pub total_records: usize,
    pub exact_dups: usize,
    pub hard_negs: usize,
    pub uniques: usize,
    pub masters: usize,
}

// ── Pre-allocated ID pools ─────────────────────────────────────────────────

pub(crate) struct IdPools {
    pub(crate) record_ids: Vec<String>,
    pub(crate) pad_7: Vec<String>,
}

fn preallocate_ids(total: usize) -> IdPools {
    let mut record_ids = Vec::with_capacity(total);
    let mut pad_7 = Vec::with_capacity(total);

    // Reusable buffers — write digits manually, avoid format! overhead
    let mut rid_buf: Vec<u8> = b"R-0000000000000".to_vec();
    let mut pad_buf: Vec<u8> = b"0000000000000".to_vec();

    for i in 0..total {
        // Write 13-digit counter into rid_buf[2..15]
        let mut n = i;
        for j in (2..15).rev() {
            rid_buf[j] = b'0' + (n % 10) as u8;
            n /= 10;
        }
        record_ids.push(String::from_utf8(rid_buf.clone()).unwrap());

        // Write 13-digit counter into pad_buf
        let mut n = i;
        for j in (0..13).rev() {
            pad_buf[j] = b'0' + (n % 10) as u8;
            n /= 10;
        }
        pad_7.push(String::from_utf8(pad_buf.clone()).unwrap());
    }

    IdPools { record_ids, pad_7 }
}

// ── Entity prefix ──────────────────────────────────────────────────────────

fn entity_prefix(name: &str) -> String {
    let h: u64 = name
        .bytes()
        .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
    format!("E{:05}", h % 100000)
}

// ── FK / HN pool types ─────────────────────────────────────────────────────

type FkPoolMap = HashMap<String, RecordBatch>;

struct HnPool {
    batch: RecordBatch,
    total_count: usize,
}

// ── Noise column matching ─────────────────────────────────────────────────

/// Heuristic: determine target columns for a noise type based on name patterns.
/// Used when plan_cols is empty (legacy configs without column lists).
fn match_noise_columns(schema: &Schema, noise_type: &str, exclude_cols: &[String]) -> Vec<String> {
    let mut matched: Vec<String> = Vec::new();
    for field in schema.fields() {
        if !matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            continue;
        }
        if exclude_cols.contains(field.name()) {
            continue;
        }
        let lower = field.name().to_lowercase();
        match noise_type {
            "typo" | "typo_aggressive" | "typo_extreme" | "qwerty_azerty" | "visual"
            | "homoglyph" | "unicode_pollution" | "ocr_errors" | "case_swap" | "char_dropout"
            | "language_mix" | "blocking_fail" => {
                // Skip email-like columns — typo/visual noise destroys '@'
                if lower.contains("email") {
                    continue;
                }
                if contains_any(
                    &lower,
                    &[
                        "name", "first", "last", "given", "family", "address", "street", "city",
                        "phone", "company", "legal", "trading",
                    ],
                ) {
                    matched.push(field.name().clone());
                }
            }
            "names" | "nickname" | "initials" | "partial" | "name_compound" => {
                if contains_any(&lower, &["name", "first", "last", "given", "family"]) {
                    matched.push(field.name().clone());
                }
            }
            "swap" | "full_swap" => {
                if contains_any(&lower, &["name", "first", "last", "given", "family"]) {
                    matched.push(field.name().clone());
                }
            }
            "dates" | "date_error" | "date_chaotic" | "date_format_mix" | "age_impossible" => {
                if contains_any(&lower, &["date", "birth", "incorporation", "founding"]) {
                    matched.push(field.name().clone());
                }
            }
            "missing" | "missing_pattern" => {
                if contains_any(&lower, &["phone", "email", "mobile", "address", "street"]) {
                    matched.push(field.name().clone());
                }
            }
            "identifiers"
            | "corrupt_email"
            | "corrupt_phone"
            | "corrupt_national_id"
            | "corrupt_siren"
            | "national_id_corrupt"
            | "phone_corrupt"
            | "email_corrupt"
            | "siren_corrupt" => {
                if contains_any(
                    &lower,
                    &[
                        "email",
                        "phone",
                        "ssn",
                        "pan",
                        "passport",
                        "account",
                        "medicare",
                        "license",
                        "siren",
                        "national_id",
                    ],
                ) {
                    matched.push(field.name().clone());
                }
            }
            "extra"
            | "name_null"
            | "dob_null"
            | "blocking_fail_initial"
            | "blocking_fail_partial"
            | "fuzzy_match"
            | "phonetic" => {
                // Skip email-like columns — extra noise destroys '@'
                if lower.contains("email") {
                    continue;
                }
                if contains_any(
                    &lower,
                    &[
                        "name", "first", "last", "given", "family", "phone", "address", "street",
                        "city", "company", "legal", "trading", "note", "comment",
                    ],
                ) {
                    matched.push(field.name().clone());
                }
            }
            "companies" | "acronym" | "legal_form_drop" | "word_dropout" | "company_scramble" => {
                if contains_any(&lower, &["company", "legal", "trading", "name"]) {
                    matched.push(field.name().clone());
                }
            }
            "addresses" | "address_scramble" | "postal_corrupt" => {
                if contains_any(&lower, &["address", "street", "postal", "city"]) {
                    matched.push(field.name().clone());
                }
            }
            "exact" | "english_name" | "estonian_name" | "lithuanian_name" | "slovak_name"
            | "serbian_name" | "norwegian_name" | "swedish_name" | "dutch_name" | "czech_name"
            | "albanian_name" | "polish_name" | "romanian_name" | "hungarian_name"
            | "german_name" | "italian_name" | "spanish_name" | "portuguese_name"
            | "combo_hard" | "combo_extreme" | "combo_ultimate" | "french_address" => {
                // These noise types are handled inline or are no-ops — skip
            }
            _ => {
                // Default: apply to all string columns (except FK columns)
                matched.push(field.name().clone());
            }
        }
    }
    // For swap, keep exactly 2 columns
    if matches!(noise_type, "swap" | "full_swap") && matched.len() >= 2 {
        matched.truncate(2);
    }
    matched.sort();
    matched.dedup();
    matched
}

fn contains_any(s: &str, patterns: &[&str]) -> bool {
    patterns.iter().any(|p| s.contains(p))
}

// ── Noise application ──────────────────────────────────────────────────────

fn apply_noise_to_batch(
    rb: &RecordBatch,
    noise_type: &str,
    plan_cols: &[String],
    rng: &mut Rng,
    exclude_cols: &[String],
) -> Result<RecordBatch, String> {
    let schema = rb.schema();
    let n_cols = rb.num_columns();

    // Fallback to heuristic when plan_cols is empty
    let target_cols: Vec<String> = if plan_cols.is_empty() {
        match_noise_columns(&schema, noise_type, exclude_cols)
    } else {
        plan_cols.to_vec()
    };

    // Start with None; only clone/modify targeted columns
    let mut new_columns: Vec<Option<ArrayRef>> = (0..n_cols).map(|_| None).collect();

    let get_col = |new_columns: &[Option<ArrayRef>], idx: usize, rb: &RecordBatch| -> ArrayRef {
        new_columns[idx]
            .clone()
            .unwrap_or_else(|| rb.column(idx).clone())
    };

    match noise_type {
        "blocking_fail" => {
            for col_name in &target_cols {
                let lower = col_name.to_lowercase();
                if let Ok(idx) = schema.index_of(col_name) {
                    let col = get_col(&new_columns, idx, rb);
                    if lower.contains("first") || lower.contains("given") {
                        new_columns[idx] =
                            Some(crate::noise::extra::apply_blocking_initial(&*col, rng));
                    } else {
                        new_columns[idx] =
                            Some(crate::noise::extra::apply_blocking_partial(&*col, rng));
                    }
                }
            }
        }
        "swap" | "full_swap" => {
            if target_cols.len() >= 2 {
                let name_a = &target_cols[0];
                let name_b = &target_cols[1];
                if let (Ok(idx_a), Ok(idx_b)) = (schema.index_of(name_a), schema.index_of(name_b)) {
                    let a = get_col(&new_columns, idx_a, rb);
                    let b = get_col(&new_columns, idx_b, rb);
                    new_columns[idx_a] = Some(b);
                    new_columns[idx_b] = Some(a);
                }
            }
        }
        "missing_pattern" => {
            let mut phone_idx: Option<usize> = None;
            let mut email_idx: Option<usize> = None;
            for col_name in &target_cols {
                let lower = col_name.to_lowercase();
                if let Ok(idx) = schema.index_of(col_name) {
                    if lower.contains("phone") || lower.contains("mobile") {
                        phone_idx = Some(idx);
                    } else if lower.contains("email") {
                        email_idx = Some(idx);
                    }
                }
            }
            if let (Some(pi), Some(ei)) = (phone_idx, email_idx) {
                let phone_arr = get_col(&new_columns, pi, rb);
                let email_arr = get_col(&new_columns, ei, rb);
                let phone_s = phone_arr.as_string::<i32>();
                let email_s = email_arr.as_string::<i32>();
                let n = phone_s.len();
                let mut builder = arrow::array::StringBuilder::with_capacity(n, 32);
                for i in 0..n {
                    let pv = if phone_s.is_null(i) {
                        ""
                    } else {
                        phone_s.value(i)
                    };
                    let ev = if email_s.is_null(i) {
                        ""
                    } else {
                        email_s.value(i)
                    };
                    if !ev.is_empty() && rng.next_usize(2) == 0 {
                        builder.append_value("");
                    } else {
                        builder.append_value(pv);
                    }
                }
                new_columns[pi] = Some(Arc::new(builder.finish()));
            }
        }
        _ => {
            for col_name in &target_cols {
                if let Ok(col_idx) = schema.index_of(col_name) {
                    let col = get_col(&new_columns, col_idx, rb);
                    let result = crate::noise::apply_noise_to_column(&*col, noise_type, rng)
                        .map_err(|e| format!("noise '{noise_type}' on '{col_name}': {e}"))?;
                    new_columns[col_idx] = Some(result);
                }
            }
        }
    }

    // Fill remaining from original, unwrap all
    let final_columns: Vec<ArrayRef> = (0..n_cols)
        .map(|i| {
            new_columns[i]
                .clone()
                .unwrap_or_else(|| rb.column(i).clone())
        })
        .collect();

    RecordBatch::try_new(schema, final_columns).map_err(|e| format!("RecordBatch: {e}"))
}

// ── Topological sort for FK dependency ordering ────────────────────────────

/// Order entity plans so that FK target entities are processed before dependents.
fn topological_sort(
    plans: &[EntityPlan],
    name_to_idx: &HashMap<&str, usize>,
) -> Result<Vec<usize>, String> {
    let n = plans.len();
    let mut in_degree = vec![0usize; n];
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    for i in 0..n {
        for remap in &plans[i].fk_remaps {
            let Some(&target) = name_to_idx.get(remap.target_entity.as_str()) else {
                continue;
            };
            if target == i {
                continue;
            }
            adj[target].push(i); // target → i means target must come before i
            in_degree[i] += 1;
        }
    }
    let mut queue: Vec<usize> = (0..n).filter(|i| in_degree[*i] == 0).collect();
    let mut order = Vec::with_capacity(n);
    let mut idx = 0;
    while idx < queue.len() {
        let v = queue[idx];
        idx += 1;
        order.push(v);
        for &next in &adj[v] {
            in_degree[next] -= 1;
            if in_degree[next] == 0 {
                queue.push(next);
            }
        }
    }
    if order.len() != n {
        return Err("FK dependency cycle detected among entities".into());
    }
    Ok(order)
}

// ── Main pipeline ──────────────────────────────────────────────────────────

pub fn run_pipeline(
    ctx: &Context,
    config: &PipelineConfig,
    output_dir: &str,
) -> Result<PipelineOutput, String> {
    std::fs::create_dir_all(output_dir)
        .map_err(|e| format!("create output directory {output_dir:?}: {e}"))?;
    let t_start = std::time::Instant::now();

    // Pre-allocate IDs (base + dups + HN + safety margin)
    let max_hn = (config.hard_neg_ratio * config.size as f64) as usize;
    let total_ids = config.size + max_hn + 10_000;
    let ids = preallocate_ids(total_ids);
    let t_alloc = t_start.elapsed().as_secs_f64();

    // ── Phase 14: Streaming pipeline ─────────────────────────────────────
    // Generates, extracts FK/HN, remaps, and sinks each batch per entity
    // without storing all entity batches in memory.
    let t1e = std::time::Instant::now();

    // Build the full schema once (union of all entity columns + metadata)
    let full_arc = Arc::new(build_full_schema(config));
    let mut dataset_path = format!("{}/{}.ipc", output_dir, config.run_id);
    let out_file =
        std::fs::File::create(&dataset_path).map_err(|e| format!("create {dataset_path}: {e}"))?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(out_file, &full_arc)
        .map_err(|e| format!("IPC FileWriter error: {e}"))?;

    // Topological sort for FK dependency processing order
    let name_to_idx: HashMap<&str, usize> = config
        .entity_plans
        .iter()
        .enumerate()
        .map(|(i, p)| (p.name.as_str(), i))
        .collect();
    let entity_order = topological_sort(&config.entity_plans, &name_to_idx)?;

    // Cross-entity storage (lightweight)
    let mut fk_pools: FkPoolMap = HashMap::new();
    let mut hn_pools: HashMap<String, HnPool> = HashMap::new();
    let mut master_id_pool: HashMap<String, Vec<String>> = HashMap::new();

    // GT collection
    let mut gt_record_id_arrs: Vec<ArrayRef> = Vec::new();
    let mut gt_master_id_arrs: Vec<ArrayRef> = Vec::new();
    let mut gt_entity_type_arrs: Vec<ArrayRef> = Vec::new();

    let mut global_rid_offset = 0usize;
    let mut _t_write = 0.0f64;
    let mut _t_meta = 0.0f64;
    let mut _t_dup = 0.0f64;
    let mut _write_calls = 0u64;
    // Compute HN pool size: need at least 2× the max hn_cfg.count for any entity type
    let hn_pool_sizing: std::collections::HashMap<&str, usize> =
        config
            .hard_neg_types
            .iter()
            .fold(std::collections::HashMap::new(), |mut m, hn| {
                let entry = m.entry(hn.entity_type.as_str()).or_insert(0);
                *entry = (*entry).max(hn.count);
                m
            });
    let hn_max_pool: usize = hn_pool_sizing.values().max().copied().unwrap_or(50_000) * 2 + 1_000;

    for &plan_idx in &entity_order {
        let plan = &config.entity_plans[plan_idx];
        if plan.n_base == 0 {
            continue;
        }

        let prefix = entity_prefix(&plan.name);
        let col_json_str = &plan.columns_json;

        // Per-entity streaming state
        let mut fk_builder = arrow::array::StringBuilder::new();
        let mut hn_slices: Vec<RecordBatch> = Vec::new();
        let mut mids = Vec::with_capacity(plan.n_base);
        let mut last_batch: Option<(RecordBatch, usize, usize)> = None;
        let mut batch_rng = Rng::new(config.seed.wrapping_add(100));
        let mut fk_rng = Rng::new(config.seed.wrapping_add(42));

        // Build col_lookup from first batch (it has the entity's schema)
        let mut col_lookup: Option<Vec<Option<usize>>> = None;
        // Phase 12.3: null cache per entity plan
        let mut null_cache: HashMap<(DataType, usize), ArrayRef> = HashMap::new();

        // Stream: generate → FK remap → FK extract → HN sample → IPC write → GT collect
        for (batch_idx, offset) in (0..plan.n_base)
            .step_by(crate::entity_gen::BATCH_SIZE)
            .enumerate()
        {
            let batch_n = (plan.n_base - offset).min(crate::entity_gen::BATCH_SIZE);
            let batch_seed = config.seed.wrapping_add(batch_idx as u64 * 1000 + 1000);

            // Generate batch
            let request_json = format!(
                r#"{{"entity_name":"{}","n":{},"seed":{},"columns":{}}}"#,
                plan.name, batch_n, batch_seed, col_json_str,
            );
            let rb = crate::entity_gen::generate_entity_batch(ctx, &request_json)?;

            // FK remap (uses pools from previously processed entities)
            let rb = if !plan.fk_remaps.is_empty() {
                let mut r = rb;
                for remap in &plan.fk_remaps {
                    if let Some(pool) = fk_pools.get(&remap.target_entity) {
                        r = crate::fk_remap::fk_remap_batch(
                            &r,
                            pool,
                            &remap.source_col,
                            &mut fk_rng,
                        )?;
                    }
                }
                r
            } else {
                rb
            };

            // Extract FK identifiers for this entity's pool
            if let Some(ref id_col) = plan.identifier_col
                && let Some(col) = rb.column_by_name(id_col)
            {
                let s = col.as_string::<i32>();
                for i in 0..s.len() {
                    if !s.is_null(i) {
                        fk_builder.append_value(s.value(i));
                    }
                }
            }

            // HN pool: accumulate slices up to max pool size
            let hn_accum: usize = hn_slices.iter().map(|s| s.num_rows()).sum();
            if hn_accum < hn_max_pool {
                let take = (hn_max_pool - hn_accum).min(batch_n);
                hn_slices.push(rb.slice(0, take));
            }

            // Build col_lookup from first batch's schema
            if col_lookup.is_none() {
                let plan_schema = rb.schema();
                col_lookup = Some(
                    full_arc
                        .fields()
                        .iter()
                        .skip(4)
                        .map(|f| plan_schema.column_with_name(f.name()).map(|(idx, _)| idx))
                        .collect(),
                );
            }

            // Master IDs for this batch
            let batch_mids: Vec<String> = (offset..offset + batch_n)
                .map(|i| format!("{}-{}", prefix, ids.pad_7[i]))
                .collect();
            let batch_mids_slice = &batch_mids[..];

            // Add metadata + IPC write
            let rid_slice: &[String] =
                &ids.record_ids[global_rid_offset..global_rid_offset + batch_n];
            let t_m0 = std::time::Instant::now();
            let base_rb = add_metadata_and_align(
                &rb,
                &config.domain,
                &plan.name,
                rid_slice,
                batch_mids_slice,
                &full_arc,
                col_lookup.as_ref().unwrap(),
                &mut null_cache,
            );
            _t_meta += t_m0.elapsed().as_secs_f64();

            let t_w0 = std::time::Instant::now();
            writer
                .write(&base_rb)
                .map_err(|e| format!("write base: {e}"))?;
            _t_write += t_w0.elapsed().as_secs_f64();
            _write_calls += 1;

            // GT collect
            gt_record_id_arrs.push(base_rb.column(0).clone());
            gt_entity_type_arrs.push(base_rb.column(2).clone());
            gt_master_id_arrs.push(base_rb.column(3).clone());

            global_rid_offset += batch_n;
            mids.extend(batch_mids);
            last_batch = Some((rb, batch_n, offset));
        }

        // Save FK pool for cross-entity remapping
        if let Some(id_col) = &plan.identifier_col
            && fk_builder.len() > 0
        {
            let schema = Arc::new(Schema::new(vec![Field::new(id_col, DataType::Utf8, true)]));
            let arr = Arc::new(fk_builder.finish()) as ArrayRef;
            if let Ok(pool_rb) = RecordBatch::try_new(schema, vec![arr]) {
                fk_pools.insert(plan.name.clone(), pool_rb);
            }
        }

        // Build HN pool from collected slices
        if !hn_slices.is_empty() {
            let total_count: usize = hn_slices.iter().map(|s| s.num_rows()).sum();
            let batch = if hn_slices.len() == 1 {
                hn_slices.into_iter().next().unwrap()
            } else {
                let schema = hn_slices[0].schema();
                let n_fields = schema.fields().len();
                let mut concat_arrays = Vec::with_capacity(n_fields);
                for i in 0..n_fields {
                    let refs: Vec<&dyn Array> =
                        hn_slices.iter().map(|pb| pb.column(i).as_ref()).collect();
                    concat_arrays.push(arrow::compute::concat(&refs).expect("hn pool concat col"));
                }
                RecordBatch::try_new(schema, concat_arrays).expect("hn pool concat batch")
            };
            hn_pools.insert(plan.name.clone(), HnPool { batch, total_count });
        }

        // Save master_ids for FK remap and dup mid cloning
        master_id_pool.insert(plan.name.clone(), mids);

        // ── Dups (use last_batch) ──────────────────────────────────────────
        let has_dups: bool = plan.noise_types.iter().any(|n| n.count > 0);
        if has_dups && let Some((ref last_rb, last_n, last_offset)) = last_batch {
            let t_d0 = std::time::Instant::now();
            let mut dup_batches: Vec<RecordBatch> = Vec::new();
            let mut dup_mids_buf: Vec<String> = Vec::new();
            let mids_ref = master_id_pool.get(&plan.name).unwrap();

            // Collect FK columns to exclude from noise
            let fk_exclude_cols: Vec<String> = plan
                .fk_remaps
                .iter()
                .map(|r| r.source_col.clone())
                .collect();

            // Pre-generate indices + parallel noise (Phase 13c)
            let ndata: Vec<(UInt64Array, u64, &str, &[String], usize)> = plan
                .noise_types
                .iter()
                .filter(|n| n.count > 0)
                .map(|n| {
                    let mut b = UInt64Builder::with_capacity(n.count);
                    for _ in 0..n.count {
                        b.append_value(batch_rng.next_usize(last_n) as u64);
                    }
                    (
                        b.finish(),
                        batch_rng.next_u64(),
                        n.noise_type.as_str(),
                        n.columns.as_slice(),
                        n.count,
                    )
                })
                .collect();

            let mut results: Vec<Result<(RecordBatch, Vec<String>), String>> =
                Vec::with_capacity(ndata.len());
            std::thread::scope(|s| {
                let mut handles = Vec::with_capacity(ndata.len());
                for (indices, seed, ntype, cols, cnt) in &ndata {
                    let rb = last_rb.clone();
                    let idxs = indices.clone();
                    let cols_v: Vec<String> = cols.to_vec();
                    let mslice: &[String] = mids_ref.as_slice();
                    let exclude = fk_exclude_cols.clone();
                    handles.push(s.spawn(move || {
                        let dup = pick_rows(&rb, &idxs)?;
                        let mut rng = Rng::new(*seed);
                        let noisy = apply_noise_to_batch(&dup, ntype, &cols_v, &mut rng, &exclude)?;
                        let mut mb = Vec::with_capacity(*cnt);
                        for j in 0..*cnt {
                            // `idxs` are local to `last_rb` (0..last_n); the matching
                            // master_id lives at `last_offset + local_idx` in the
                            // full per-entity master_id array, not at `local_idx`.
                            mb.push(mslice[last_offset + idxs.value(j) as usize].clone());
                        }
                        Ok((noisy, mb))
                    }));
                }
                for h in handles {
                    results.push(h.join().unwrap());
                }
            });
            for res in results {
                let (rb, mb) = res?;
                dup_batches.push(rb);
                dup_mids_buf.extend(mb);
            }
            _t_dup += t_d0.elapsed().as_secs_f64();

            // Write dups
            if !dup_batches.is_empty() {
                let dup_total = dup_mids_buf.len();
                let dup_rids: &[String] =
                    &ids.record_ids[global_rid_offset..global_rid_offset + dup_total];

                let concated = if dup_batches.len() == 1 {
                    dup_batches.into_iter().next().unwrap()
                } else {
                    let schema = dup_batches[0].schema();
                    let n_fields = dup_batches[0].num_columns();
                    let mut concat_arrays = Vec::with_capacity(n_fields);
                    for i in 0..n_fields {
                        let refs: Vec<&dyn Array> =
                            dup_batches.iter().map(|b| b.column(i).as_ref()).collect();
                        concat_arrays.push(
                            arrow::compute::concat(&refs)
                                .map_err(|e| format!("concat dup col {i}: {e}"))?,
                        );
                    }
                    RecordBatch::try_new(schema, concat_arrays)
                        .map_err(|e| format!("concat dups: {e}"))?
                };

                let dup_rb_full = add_metadata_and_align(
                    &concated,
                    &config.domain,
                    &plan.name,
                    dup_rids,
                    &dup_mids_buf,
                    &full_arc,
                    col_lookup.as_ref().unwrap(),
                    &mut null_cache,
                );

                let t_wd = std::time::Instant::now();
                writer
                    .write(&dup_rb_full)
                    .map_err(|e| format!("write dups: {e}"))?;
                _t_write += t_wd.elapsed().as_secs_f64();
                _write_calls += 1;

                gt_record_id_arrs.push(dup_rb_full.column(0).clone());
                gt_entity_type_arrs.push(dup_rb_full.column(2).clone());
                gt_master_id_arrs.push(dup_rb_full.column(3).clone());
                global_rid_offset += dup_total;
            }
        }
    }
    let t1e_elapsed = t1e.elapsed().as_secs_f64();
    log::debug!("[sink_profile] write={_t_write:.3}s calls={_write_calls}");

    // ── Phase 2: Hard negatives ────────────────────────────────────────────
    let t2 = std::time::Instant::now();
    // Phase 12.3: separate null cache for HN section (different entity types)
    let mut hn_null_cache: HashMap<(DataType, usize), ArrayRef> = HashMap::new();
    for (hn_idx, hn_cfg) in config.hard_neg_types.iter().enumerate() {
        if hn_cfg.count == 0 {
            continue;
        }
        let Some(pool_data) = hn_pools.get(&hn_cfg.entity_type) else {
            continue;
        };
        let hn_total = pool_data.total_count;

        let pool_rb = &pool_data.batch;
        let n_hn_max = hn_cfg.count.min(hn_total / 2);
        if n_hn_max == 0 {
            continue;
        }

        let hn_rb = crate::hn_common::generate_hard_negatives(
            pool_rb,
            &hn_cfg.config_json,
            n_hn_max,
            config.seed.wrapping_add(200),
        )?;

        let n_hn = hn_rb.num_rows();
        if n_hn == 0 {
            continue;
        }

        let hn_rids: &[String] = &ids.record_ids[global_rid_offset..global_rid_offset + n_hn];

        let mut hn_mid_rng = Rng::new(config.seed.wrapping_add(300 + hn_idx as u64));
        let hn_mids: Vec<String> = (0..n_hn)
            .map(|_i| {
                let pad = hn_mid_rng.next_usize(10_000_000);
                format!("HN-{:07}", pad)
            })
            .collect();

        // Build col lookup for the HN entity type schema
        let hn_schema = hn_rb.schema();
        let hn_col_lookup: Vec<Option<usize>> = full_arc
            .fields()
            .iter()
            .skip(4)
            .map(|f| hn_schema.column_with_name(f.name()).map(|(idx, _)| idx))
            .collect();

        let hn_rb_full = add_metadata_and_align(
            &hn_rb,
            &config.domain,
            &hn_cfg.entity_type,
            hn_rids,
            &hn_mids,
            &full_arc,
            &hn_col_lookup,
            &mut hn_null_cache,
        );

        let t_wh = std::time::Instant::now();
        writer
            .write(&hn_rb_full)
            .map_err(|e| format!("write hn: {e}"))?;
        _t_write += t_wh.elapsed().as_secs_f64();
        _write_calls += 1;

        // Phase 13: collect ArrayRefs instead of per-row StringBuilder
        gt_record_id_arrs.push(hn_rb_full.column(0).clone());
        gt_entity_type_arrs.push(hn_rb_full.column(2).clone());
        gt_master_id_arrs.push(hn_rb_full.column(3).clone());

        global_rid_offset += n_hn;
    }
    let t2_elapsed = t2.elapsed().as_secs_f64();

    // ── Canary records ────────────────────────────────────────────────────
    {
        let mut canary_null_cache: HashMap<(DataType, usize), ArrayRef> = HashMap::new();
        crate::canary::generate_all(
            ctx,
            config,
            &full_arc,
            &mut canary_null_cache,
            &mut global_rid_offset,
            &ids,
            &fk_pools,
            &mut writer,
            &mut gt_record_id_arrs,
            &mut gt_entity_type_arrs,
            &mut gt_master_id_arrs,
        )?;
    }

    // ── Phase 3: Finalize + GT (use accumulated arrays) ────────────────
    let t3 = std::time::Instant::now();
    writer
        .finish()
        .map_err(|e| format!("finish IPC writer: {e}"))?;
    let t3a_elapsed = t3.elapsed().as_secs_f64();

    let t3b = std::time::Instant::now();

    // Phase 13: concat once, keep ArrayRef alive, as_string borrows from it
    let rid_arr = arrow::compute::concat(
        &gt_record_id_arrs
            .iter()
            .map(|a| a.as_ref())
            .collect::<Vec<_>>(),
    )
    .map_err(|e| format!("concat rid: {e}"))?;
    let et_arr = arrow::compute::concat(
        &gt_entity_type_arrs
            .iter()
            .map(|a| a.as_ref())
            .collect::<Vec<_>>(),
    )
    .map_err(|e| format!("concat et: {e}"))?;
    let mid_arr = arrow::compute::concat(
        &gt_master_id_arrs
            .iter()
            .map(|a| a.as_ref())
            .collect::<Vec<_>>(),
    )
    .map_err(|e| format!("concat mid: {e}"))?;

    // Downcast once, reuse references
    let record_id_arr: &StringArray = rid_arr.as_string::<i32>();
    let entity_type_arr: &StringArray = et_arr.as_string::<i32>();
    let master_id_arr: &StringArray = mid_arr.as_string::<i32>();

    let t_gt0 = std::time::Instant::now();
    let (gt_match_types, n_exact_dup, n_hard_neg, n_unique) =
        crate::gt::compute_gt(record_id_arr, master_id_arr, entity_type_arr);
    let _t_gt_compute = t_gt0.elapsed().as_secs_f64();

    let gt_ext = if config.output_format == "parquet" {
        "parquet"
    } else {
        "ipc"
    };
    let gt_path = format!("{}/{}_ground_truth.{}", output_dir, config.run_id, gt_ext);

    let _gt_meta = build_metadata_map(config);
    let t_gt1 = std::time::Instant::now();
    if config.output_format == "parquet" {
        crate::gt::write_gt_parquet(
            record_id_arr,
            master_id_arr,
            entity_type_arr,
            &gt_match_types,
            config.difficulty.as_str(),
            &gt_path,
            &_gt_meta,
        )?;
    } else {
        crate::gt::write_gt_ipc(
            record_id_arr,
            master_id_arr,
            entity_type_arr,
            &gt_match_types,
            config.difficulty.as_str(),
            &gt_path,
            &_gt_meta,
        )?;
    }
    let _t_gt_write = t_gt1.elapsed().as_secs_f64();
    let t3b_elapsed = t3b.elapsed().as_secs_f64();
    let _t3_elapsed = t3.elapsed().as_secs_f64();

    // ── Phase 3b: Materialize IPC → Parquet (Rust, no Polars) ───────────
    if config.output_format == "parquet" {
        use arrow::ipc::reader::FileReader;

        let parquet_path = dataset_path.replace(".ipc", ".parquet");
        let ipc_file = std::fs::File::open(&dataset_path)
            .map_err(|e| format!("open IPC for conversion: {e}"))?;
        let ipc_reader = FileReader::try_new(ipc_file, None)
            .map_err(|e| format!("IPC reader for conversion: {e}"))?;
        let schema = ipc_reader.schema();

        let parquet_file = std::fs::File::create(&parquet_path)
            .map_err(|e| format!("create {parquet_path}: {e}"))?;
        let zstd_level = parquet::basic::ZstdLevel::try_new(3).map_err(|e| format!("zstd: {e}"))?;
        let meta_kv: Vec<parquet::file::metadata::KeyValue> = build_metadata_map(config)
            .into_iter()
            .map(|(k, v)| parquet::file::metadata::KeyValue {
                key: k,
                value: Some(v),
            })
            .collect();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(zstd_level))
            .set_max_row_group_row_count(Some(usize::MAX / 2))
            .set_key_value_metadata(Some(meta_kv))
            .build();
        let mut parquet_writer =
            parquet::arrow::ArrowWriter::try_new(parquet_file, schema, Some(props))
                .map_err(|e| format!("ArrowWriter for conversion: {e}"))?;

        for batch_result in ipc_reader {
            let batch = batch_result.map_err(|e| format!("IPC batch for conversion: {e}"))?;
            parquet_writer
                .write(&batch)
                .map_err(|e| format!("write parquet conversion: {e}"))?;
        }
        parquet_writer
            .close()
            .map_err(|e| format!("close parquet conversion: {e}"))?;

        std::fs::remove_file(&dataset_path).map_err(|e| format!("remove IPC temp file: {e}"))?;
        dataset_path = parquet_path;
    }

    let duration = t_start.elapsed().as_secs_f64();

    log::debug!(
        "[pipeline_timing] alloc={:.3}s  sink={:.3}s  hn={:.3}s  merge={:.3}s  meta={:.3}s/dup={:.3}s/write={:.3}s  gt(comp={:.3}s+write={:.3}s)={:.3}s  total={:.3}s",
        t_alloc,
        t1e_elapsed,
        t2_elapsed,
        t3a_elapsed,
        _t_meta,
        _t_dup,
        _t_write,
        _t_gt_compute,
        _t_gt_write,
        t3b_elapsed,
        duration,
    );

    let master_set: std::collections::HashSet<&str> = (0..master_id_arr.len())
        .map(|i| master_id_arr.value(i))
        .collect();

    let stats = PipelineStats {
        total_records: global_rid_offset,
        exact_dups: n_exact_dup,
        hard_negs: n_hard_neg,
        uniques: n_unique,
        masters: master_set.len(),
    };

    Ok(PipelineOutput {
        output_files: vec![dataset_path],
        gt_file: gt_path,
        stats,
    })
}

// ── Metadata injection ──────────────────────────────────────────────────────

fn build_metadata_map(config: &PipelineConfig) -> HashMap<String, String> {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "unknown".into());
    HashMap::from([
        (
            "dupehell.generator".into(),
            format!("DupeHell v{}", env!("CARGO_PKG_VERSION")),
        ),
        (
            "dupehell.provenance".into(),
            "dupehell-synthetic-data".into(),
        ),
        ("dupehell.license".into(), "MIT".into()),
        (
            "dupehell.purpose".into(),
            "Educational Use Only — Record Linkage Benchmarking".into(),
        ),
        (
            "dupehell.url".into(),
            "https://github.com/vntoinekaio/DupeHell".into(),
        ),
        ("dupehell.domain".into(), config.domain.clone()),
        ("dupehell.size".into(), config.size.to_string()),
        ("dupehell.seed".into(), config.seed.to_string()),
        ("dupehell.run_id".into(), config.run_id.clone()),
        ("dupehell.timestamp".into(), ts),
    ])
}

// ── Schema alignment ───────────────────────────────────────────────────────

/// Build the union schema from all entity plans (all columns + metadata).
fn build_full_schema(config: &PipelineConfig) -> Schema {
    let mut field_map: Vec<(String, DataType, bool)> = Vec::new();
    let metadata_fields = ["record_id", "domain", "entity_type", "master_id"];
    for mf in &metadata_fields {
        field_map.push((mf.to_string(), DataType::Utf8, false));
    }
    for plan in &config.entity_plans {
        let cols: Vec<serde_json::Value> =
            serde_json::from_str(&plan.columns_json).unwrap_or_default();
        for col in &cols {
            let name = col["name"].as_str().unwrap_or("").to_string();
            if name.is_empty() || field_map.iter().any(|(n, _, _)| n == &name) {
                continue;
            }
            let col_type = col_type_from_request(col);
            field_map.push((name, col_type, true));
        }
    }
    let fields: Vec<Field> = field_map
        .into_iter()
        .map(|(n, dt, nullable)| Field::new(&n, dt, nullable))
        .collect();
    Schema::new(fields).with_metadata(build_metadata_map(config))
}

/// Map a column's JSON type string to Arrow DataType.
fn col_type_from_request(v: &serde_json::Value) -> DataType {
    match v["type"].as_str() {
        Some("int") => DataType::Int64,
        Some("float") => DataType::Float64,
        Some("boolean") => DataType::Boolean,
        Some("date") | Some("datetime") => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub(crate) fn add_metadata_and_align(
    rb: &RecordBatch,
    domain: &str,
    entity_type: &str,
    record_ids: &[String],
    master_ids: &[String],
    full_arc: &Arc<Schema>,
    col_lookup: &[Option<usize>],
    null_cache: &mut HashMap<(DataType, usize), ArrayRef>,
) -> RecordBatch {
    let n = rb.num_rows();
    let domain_arr = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        domain, n,
    ))) as ArrayRef;
    let et_arr = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        entity_type,
        n,
    ))) as ArrayRef;
    let rid_arr = Arc::new(StringArray::from_iter_values(
        record_ids.iter().map(|s| s.as_str()),
    )) as ArrayRef;
    let mid_arr = Arc::new(StringArray::from_iter_values(
        master_ids.iter().map(|s| s.as_str()),
    )) as ArrayRef;

    let mut all_arrays: Vec<ArrayRef> = Vec::with_capacity(4 + col_lookup.len());
    all_arrays.push(rid_arr);
    all_arrays.push(domain_arr);
    all_arrays.push(et_arr);
    all_arrays.push(mid_arr);

    // P4: Vec<Option<usize>> eliminates HashMap lookup per field (just index)
    // Phase 12.3: cache null arrays per (DataType, n) — avoids new_null_array overhead per batch
    for (i, maybe_idx) in col_lookup.iter().enumerate() {
        match maybe_idx {
            Some(idx) => all_arrays.push(rb.column(*idx).clone()),
            None => {
                let dt = full_arc.field(i + 4).data_type();
                let arr = null_cache
                    .entry((dt.clone(), n))
                    .or_insert_with(|| arrow::array::new_null_array(dt, n));
                all_arrays.push(Arc::clone(arr));
            }
        }
    }

    // P3: Arc::clone(full_arc) avoids full schema clone (just atomic increment)
    RecordBatch::try_new(Arc::clone(full_arc), all_arrays)
        .expect("add_metadata_and_align RecordBatch")
}

fn pick_rows(rb: &RecordBatch, indices: &UInt64Array) -> Result<RecordBatch, String> {
    use arrow::compute::take;
    let new_columns: Vec<ArrayRef> = (0..rb.num_columns())
        .map(|i| take(rb.column(i), indices, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("take error: {e}"))?;

    RecordBatch::try_new(rb.schema(), new_columns).map_err(|e| format!("RecordBatch: {e}"))
}
