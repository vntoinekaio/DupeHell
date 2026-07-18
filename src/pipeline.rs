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

    pub graph_enabled: bool,
    pub graph_format: String,
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
    pub nodes: Option<String>,
    pub edges: Option<String>,
}

#[derive(Debug)]
pub struct PipelineStats {
    pub total_records: usize,
    pub exact_dups: usize,
    pub hard_negs: usize,
    pub uniques: usize,
    pub masters: usize,
}

// ── Fixed-width ASCII IDs ────────────────────────────────────────────────

// `record_id`/`pad` are pure functions of the row index — "R-" + 13 digits
// and 13 digits respectively. Earlier this was a struct precomputing both
// for every id up front (bounded per-2-big-allocations, better than one
// `String` per id, but still O(total_ids) resident for the whole run — at
// 200M+ ids that's several GB held for nothing). Computing on demand, only
// for the batch currently being written, removes that fixed cost entirely;
// the transient `Vec<String>` built per batch is bounded by `BATCH_SIZE`
// and freed right after use, same as `batch_mids`/`dup_mids_buf`/`hn_mids`
// elsewhere in this file.
const RID_LEN: usize = 15; // "R-" + 13 digits
const PAD_LEN: usize = 13; // 13 digits

#[inline]
fn record_id_string(i: usize) -> String {
    let mut buf = [0u8; RID_LEN];
    buf[0] = b'R';
    buf[1] = b'-';
    let mut n = i;
    for j in (2..RID_LEN).rev() {
        buf[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    String::from_utf8(buf.to_vec()).expect("record_id_string: buffer is 'R', '-' and ASCII digits")
}

#[inline]
fn pad_string(i: usize) -> String {
    let mut buf = [0u8; PAD_LEN];
    let mut n = i;
    for j in (0..PAD_LEN).rev() {
        buf[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    String::from_utf8(buf.to_vec()).expect("pad_string: buffer is always ASCII digits")
}

pub(crate) fn record_id_strs(range: std::ops::Range<usize>) -> Vec<String> {
    range.map(record_id_string).collect()
}

// ── Entity prefix ──────────────────────────────────────────────────────────

/// Per-entity master_id prefix, derived from the entity's position in
/// `config.entity_plans` rather than a hash of its name: a name hash mod
/// 100000 had a real (if small) chance of two entities in the same domain
/// colliding on the same prefix, which would make `gt.rs`'s master_id
/// counting conflate masters from different entity types. An index is
/// unique by construction — zero collision risk regardless of domain size.
fn entity_prefix(entity_idx: usize) -> String {
    format!("E{:05}", entity_idx)
}

// ── FK / HN pool types ─────────────────────────────────────────────────────

type FkPoolMap = HashMap<String, RecordBatch>;

struct HnPool {
    batch: RecordBatch,
    total_count: usize,
    // Global record_ids aligned positionally with `batch` rows (only populated
    // when graph output is enabled). Used to emit `hard_neg` edges.
    record_ids: Vec<String>,
}

// ── Noise column matching ─────────────────────────────────────────────────

/// Column-name fragments for columns holding a person's name as free text,
/// even when the column isn't literally called `*_name` (e.g. `operator`,
/// `technician`, populated from the `first_name` pool across several
/// domain schemas — see the 40-domain schema audit in the 2026-07 session).
const PERSON_NAME_WORDS: &[&str] = &[
    "name",
    "first",
    "last",
    "given",
    "family",
    "operator",
    "technician",
    "inspector",
    "claimant",
    "consignee",
    "investigator",
    "collected_by",
    "performed_by",
    "assigned_to",
];

/// Column-name fragments for columns holding a company/organization name,
/// even when the column isn't literally called `*_name`/`company*` (e.g.
/// `supplier`, `manufacturer`, populated from the `company` pool across
/// several domain schemas — see the 40-domain schema audit in the 2026-07
/// session).
const COMPANY_NAME_WORDS: &[&str] = &[
    "company",
    "legal",
    "trading",
    "name",
    "supplier",
    "manufacturer",
    "sponsor",
    "institution",
    "journal",
    "funder",
    "employer",
    "law_firm",
    "affiliation",
    "reported_by",
    "buyer",
    "shipper",
    "airline",
];

/// Does this noise category ever target a column with this (lowercased) name?
///
/// Pure name-pattern predicate shared with `estimate_difficulty` (see
/// `crate::difficulty`) so the theoretical column-reliability model can never
/// silently drift from what real generation actually does to each column.
pub(crate) fn noise_type_targets_column(noise_type: &str, col_name: &str) -> bool {
    let lower = col_name.to_lowercase();
    match noise_type {
        "typo" | "typo_aggressive" | "typo_extreme" | "qwerty_azerty" | "visual" | "homoglyph"
        | "unicode_pollution" | "ocr_errors" | "case_swap" | "char_dropout" | "language_mix"
        | "blocking_fail" => {
            // Skip email-like columns — typo/visual noise destroys '@'
            if lower.contains("email") {
                return false;
            }
            contains_any(&lower, &["address", "street", "city", "phone"])
                || contains_any(&lower, PERSON_NAME_WORDS)
                || contains_any(&lower, COMPANY_NAME_WORDS)
        }
        "names" | "nickname" | "initials" | "partial" | "name_compound" | "swap" | "full_swap" => {
            contains_any(&lower, PERSON_NAME_WORDS)
        }
        "dates" | "date_error" | "date_chaotic" | "date_format_mix" | "age_impossible" => {
            contains_any(&lower, &["date", "birth", "incorporation", "founding"])
        }
        "missing" | "missing_pattern" => {
            contains_any(&lower, &["phone", "email", "mobile", "address", "street"])
        }
        "identifiers"
        | "corrupt_email"
        | "corrupt_phone"
        | "corrupt_national_id"
        | "corrupt_siren"
        | "national_id_corrupt"
        | "phone_corrupt"
        | "email_corrupt"
        | "siren_corrupt" => contains_any(
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
        ),
        "extra"
        | "name_null"
        | "dob_null"
        | "blocking_fail_initial"
        | "blocking_fail_partial"
        | "fuzzy_match"
        | "phonetic" => {
            // Skip email-like columns — extra noise destroys '@'
            if lower.contains("email") {
                return false;
            }
            contains_any(
                &lower,
                &["phone", "address", "street", "city", "note", "comment"],
            ) || contains_any(&lower, PERSON_NAME_WORDS)
                || contains_any(&lower, COMPANY_NAME_WORDS)
        }
        "companies" | "acronym" | "legal_form_drop" | "word_dropout" | "company_scramble" => {
            contains_any(&lower, COMPANY_NAME_WORDS)
        }
        "addresses" | "address_scramble" | "postal_corrupt" => {
            contains_any(&lower, &["address", "street", "postal", "city"])
        }
        "exact" | "english_name" | "estonian_name" | "lithuanian_name" | "slovak_name"
        | "serbian_name" | "norwegian_name" | "swedish_name" | "dutch_name" | "czech_name"
        | "albanian_name" | "polish_name" | "romanian_name" | "hungarian_name" | "german_name"
        | "italian_name" | "spanish_name" | "portuguese_name" | "combo_hard" | "combo_extreme"
        | "combo_ultimate" | "french_address" => {
            // These noise types are handled inline or are no-ops
            false
        }
        // Default: applies to all string columns (except FK columns)
        _ => true,
    }
}

fn match_noise_columns(schema: &Schema, noise_type: &str, exclude_cols: &[String]) -> Vec<String> {
    let mut matched: Vec<String> = Vec::new();
    for field in schema.fields() {
        if !matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            continue;
        }
        if exclude_cols.contains(field.name()) {
            continue;
        }
        if noise_type_targets_column(noise_type, field.name()) {
            matched.push(field.name().clone());
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

/// Dataset sink: writes directly in the requested output format, avoiding
/// an uncompressed IPC intermediate + full reread when `parquet` is
/// requested (mirrors the `GtSink` pattern in `gt.rs`).
pub(crate) enum DatasetWriter {
    Ipc(Box<arrow::ipc::writer::FileWriter<std::fs::File>>),
    Parquet(Box<parquet::arrow::ArrowWriter<std::fs::File>>),
}

impl DatasetWriter {
    fn new(
        output_format: &str,
        path: &str,
        schema: &Arc<Schema>,
        metadata: &HashMap<String, String>,
    ) -> Result<Self, String> {
        let file = std::fs::File::create(path).map_err(|e| format!("create {path}: {e}"))?;
        if output_format == "parquet" {
            use parquet::basic::{Compression, ZstdLevel};
            use parquet::file::properties::WriterProperties;
            let zstd = ZstdLevel::try_new(3).map_err(|e| format!("zstd: {e}"))?;
            let meta_kv: Vec<parquet::file::metadata::KeyValue> = metadata
                .iter()
                .map(|(k, v)| parquet::file::metadata::KeyValue {
                    key: k.clone(),
                    value: Some(v.clone()),
                })
                .collect();
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(zstd))
                // 1M rows/group (parquet-rs' own default) bounds the
                // resident, uncompressed column-chunk buffers to one row
                // group's worth at a time instead of the whole dataset.
                .set_max_row_group_row_count(Some(1_000_000))
                .set_key_value_metadata(Some(meta_kv))
                .build();
            let writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), Some(props))
                .map_err(|e| format!("ArrowWriter error: {e}"))?;
            Ok(DatasetWriter::Parquet(Box::new(writer)))
        } else {
            let writer = arrow::ipc::writer::FileWriter::try_new(file, schema)
                .map_err(|e| format!("IPC FileWriter error: {e}"))?;
            Ok(DatasetWriter::Ipc(Box::new(writer)))
        }
    }

    pub(crate) fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        match self {
            DatasetWriter::Ipc(w) => w.write(batch).map_err(|e| format!("write ipc: {e}")),
            DatasetWriter::Parquet(w) => w.write(batch).map_err(|e| format!("write parquet: {e}")),
        }
    }

    fn finish(self) -> Result<(), String> {
        match self {
            DatasetWriter::Ipc(mut w) => w.finish().map_err(|e| format!("finish ipc: {e}")),
            DatasetWriter::Parquet(w) => w
                .close()
                .map(|_| ())
                .map_err(|e| format!("close parquet: {e}")),
        }
    }
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

    let t_alloc = t_start.elapsed().as_secs_f64();

    // ── Phase 14: Streaming pipeline ─────────────────────────────────────
    // Generates, extracts FK/HN, remaps, and sinks each batch per entity
    // without storing all entity batches in memory.
    let t1e = std::time::Instant::now();

    // Metadata (including the `dupehell.timestamp` snapshot) is computed
    // once here and reused for every output file (dataset, graph, GT,
    // parquet conversions) — computing it per-file let `SystemTime::now()`
    // drift across a Unix-second boundary on longer runs, making
    // `dupehell.timestamp` inconsistent between files from the same run.
    let metadata = build_metadata_map(config);
    // Build the full schema once (union of all entity columns + metadata)
    let full_arc = Arc::new(build_full_schema(config, &metadata));
    let dataset_ext = if config.output_format == "parquet" {
        "parquet"
    } else {
        "ipc"
    };
    let dataset_path = format!("{}/{}.{}", output_dir, config.run_id, dataset_ext);
    let mut writer =
        DatasetWriter::new(&config.output_format, &dataset_path, &full_arc, &metadata)?;

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

    // GT streaming: fed one batch at a time as the dataset is generated
    // (see `crate::gt::GtAccumulator`) instead of accumulating full-dataset
    // arrays in RAM.
    let gt_ext = if config.output_format == "parquet" {
        "parquet"
    } else {
        "ipc"
    };
    let gt_path = format!("{}/{}_ground_truth.{}", output_dir, config.run_id, gt_ext);
    let gt_draft_path = format!("{}/{}_gt_draft.ipc", output_dir, config.run_id);
    let mut gt_acc = crate::gt::GtAccumulator::new(&gt_draft_path)?;

    let mut global_rid_offset = 0usize;

    // ── Graph output (opt-in via --graph) ───────────────────────────────
    const GRAPH_MAX_CLUSTER_EDGES: usize = 10_000;
    let graph_fmt = crate::graph_gen::GraphFormat::from_str(&config.graph_format);
    let mut node_writer: Option<crate::graph_gen::NodeWriter> = None;
    let mut edge_writer: Option<crate::graph_gen::EdgeWriter> = None;
    let mut graph_nodes_path: Option<String> = None;
    let mut graph_edges_path: Option<String> = None;
    if config.graph_enabled {
        let nodes_ipc = format!("{}/{}_nodes.ipc", output_dir, config.run_id);
        let edges_ipc = format!("{}/{}_edges.ipc", output_dir, config.run_id);
        node_writer = Some(
            crate::graph_gen::NodeWriter::new(&nodes_ipc, &full_arc, &metadata)
                .map_err(|e| format!("init node writer: {e}"))?,
        );
        edge_writer = Some(
            crate::graph_gen::EdgeWriter::new(&edges_ipc, &metadata)
                .map_err(|e| format!("init edge writer: {e}"))?,
        );
        graph_nodes_path = Some(nodes_ipc);
        graph_edges_path = Some(edges_ipc);
    }

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
    // FK remap (fk_remap.rs) samples uniformly at random from this pool via
    // `take` — it has no need to see every identifier the entity ever
    // produced, only a large-enough sample for the target diversity. Capping
    // it (same idea as `hn_max_pool` above) keeps its RAM bounded instead of
    // O(n_base) per entity with an identifier column.
    const FK_POOL_CAP: usize = 200_000;

    // Entities whose FK pool is ever read (`fk_pools.get(&remap.target_entity)`
    // below, and the identical lookup in `canary::generate_all`, which reuses
    // the same `fk_remaps`). Extracting identifiers for an entity outside
    // this set is pure waste — the pool built for it is never consulted.
    let fk_targets: std::collections::HashSet<&str> = config
        .entity_plans
        .iter()
        .flat_map(|p| p.fk_remaps.iter().map(|r| r.target_entity.as_str()))
        .collect();

    for &plan_idx in &entity_order {
        let plan = &config.entity_plans[plan_idx];
        if plan.n_base == 0 {
            continue;
        }

        let prefix = entity_prefix(plan_idx);
        let col_json_str = &plan.columns_json;

        // Per-entity streaming state
        let fk_targeted = fk_targets.contains(plan.name.as_str());
        let mut fk_builder = arrow::array::StringBuilder::new();
        let mut fk_rid_builder = arrow::array::StringBuilder::new();
        let mut fk_count: usize = 0;
        // Only entities actually referenced by a `hard_neg_types` entry ever
        // have their HN pool read (see `hn_pools.get(&hn_cfg.entity_type)`
        // below); accumulating slices for the others just pins whole-batch
        // Arrow buffers in `hn_pools` for the rest of the run for nothing.
        let hn_targeted = hn_pool_sizing.contains_key(plan.name.as_str());
        let mut hn_slices: Vec<RecordBatch> = Vec::new();
        let mut hn_slice_rids: Vec<Vec<String>> = Vec::new();
        let mut last_batch: Option<(RecordBatch, usize, usize)> = None;
        let mut batch_rng = Rng::new(config.seed.wrapping_add(100));
        let mut fk_rng = Rng::new(config.seed.wrapping_add(42));

        // Build col_lookup from first batch (it has the entity's schema)
        let mut col_lookup: Option<Vec<Option<usize>>> = None;
        // Phase 12.3: null cache per entity plan
        let mut null_cache: HashMap<(DataType, usize), ArrayRef> = HashMap::new();
        let mut const_arr_cache: HashMap<(String, usize), ArrayRef> = HashMap::new();

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
            let mut fk_edges_by_remap: Vec<(String, Vec<String>)> = Vec::new();
            let rb = if !plan.fk_remaps.is_empty() {
                let mut r = rb;
                for remap in &plan.fk_remaps {
                    if let Some(pool) = fk_pools.get(&remap.target_entity) {
                        let (remapped, target_rids) = crate::fk_remap::fk_remap_batch(
                            &r,
                            pool,
                            &remap.source_col,
                            &mut fk_rng,
                            config.graph_enabled,
                        )?;
                        r = remapped;
                        if config.graph_enabled
                            && let Some(rids) = target_rids
                        {
                            fk_edges_by_remap.push((remap.source_col.clone(), rids));
                        }
                    }
                }
                r
            } else {
                rb
            };

            // Extract FK identifiers for this entity's pool, capped at
            // FK_POOL_CAP (see comment above).
            if fk_targeted
                && fk_count < FK_POOL_CAP
                && let Some(ref id_col) = plan.identifier_col
                && let Some(col) = rb.column_by_name(id_col)
            {
                let s = col.as_string::<i32>();
                for i in 0..s.len() {
                    if fk_count >= FK_POOL_CAP {
                        break;
                    }
                    if !s.is_null(i) {
                        fk_builder.append_value(s.value(i));
                        if config.graph_enabled {
                            fk_rid_builder.append_value(record_id_string(global_rid_offset + i));
                        }
                        fk_count += 1;
                    }
                }
            }

            // HN pool: accumulate slices up to max pool size
            let hn_accum: usize = hn_slices.iter().map(|s| s.num_rows()).sum();
            if hn_targeted && hn_accum < hn_max_pool {
                let take = (hn_max_pool - hn_accum).min(batch_n);
                hn_slices.push(rb.slice(0, take));
                // Capture the global record_ids for these sliced rows so the
                // HN pool can later resolve `hard_neg` edge targets. Use
                // `global_rid_offset`, never the per-entity `offset`.
                if config.graph_enabled {
                    hn_slice_rids.push(record_id_strs(global_rid_offset..global_rid_offset + take));
                }
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
                .map(|i| format!("{}-{}", prefix, pad_string(i)))
                .collect();
            let batch_mids_slice = &batch_mids[..];

            // Add metadata + IPC write
            let rid_slice = record_id_strs(global_rid_offset..global_rid_offset + batch_n);
            let t_m0 = std::time::Instant::now();
            let base_rb = add_metadata_and_align(
                &rb,
                &config.domain,
                &plan.name,
                &rid_slice,
                batch_mids_slice,
                &full_arc,
                col_lookup.as_ref().unwrap(),
                &mut null_cache,
                &mut const_arr_cache,
            )?;
            _t_meta += t_m0.elapsed().as_secs_f64();

            // Graph: write base nodes + FK edges (only when --graph)
            if config.graph_enabled
                && let (Some(nw), Some(ew)) = (node_writer.as_mut(), edge_writer.as_mut())
            {
                nw.write_batch(&base_rb)
                    .map_err(|e| format!("write node: {e}"))?;
                for i in 0..batch_n {
                    for (subtype, target_rids) in &fk_edges_by_remap {
                        ew.push(&rid_slice[i], &target_rids[i], "fk", subtype, 1.0)
                            .map_err(|e| format!("push fk edge: {e}"))?;
                    }
                }
            }

            let t_w0 = std::time::Instant::now();
            writer
                .write(&base_rb)
                .map_err(|e| format!("write base: {e}"))?;
            _t_write += t_w0.elapsed().as_secs_f64();
            _write_calls += 1;

            // GT collect
            gt_acc.push_base_batch(base_rb.column(0), base_rb.column(2), base_rb.column(3))?;

            global_rid_offset += batch_n;
            last_batch = Some((rb, batch_n, offset));
        }

        // Save FK pool for cross-entity remapping
        if let Some(id_col) = &plan.identifier_col
            && fk_builder.len() > 0
        {
            let arr = Arc::new(fk_builder.finish()) as ArrayRef;
            if config.graph_enabled {
                // Graph mode: also persist the target record_ids (column 1)
                // so FK edges can be emitted by later-source entities. Same
                // bounds as the identifier column above (FK_POOL_CAP gated).
                let rid_arr = Arc::new(fk_rid_builder.finish()) as ArrayRef;
                let schema = Arc::new(Schema::new(vec![
                    Field::new(id_col, DataType::Utf8, true),
                    Field::new("record_id", DataType::Utf8, true),
                ]));
                if let Ok(pool_rb) = RecordBatch::try_new(schema, vec![arr, rid_arr]) {
                    fk_pools.insert(plan.name.clone(), pool_rb);
                }
            } else {
                let schema = Arc::new(Schema::new(vec![Field::new(id_col, DataType::Utf8, true)]));
                if let Ok(pool_rb) = RecordBatch::try_new(schema, vec![arr]) {
                    fk_pools.insert(plan.name.clone(), pool_rb);
                }
            }
        }

        // Build HN pool from collected slices
        if !hn_slices.is_empty() {
            let total_count: usize = hn_slices.iter().map(|s| s.num_rows()).sum();
            let batch = if hn_slices.len() == 1 {
                // `rb.slice(0, take)` (pushed above) is zero-copy: it keeps
                // the *entire* source batch's buffers alive (up to
                // BATCH_SIZE=500_000 rows × all columns) for the rest of the
                // run just to serve `take` rows. Re-materializing through the
                // `take` kernel (same helper as `pick_rows` below) builds
                // compact, right-sized buffers instead.
                let only = hn_slices.into_iter().next().unwrap();
                let idx = UInt64Array::from_iter_values(0..only.num_rows() as u64);
                pick_rows(&only, &idx)?
            } else {
                let schema = hn_slices[0].schema();
                let n_fields = schema.fields().len();
                let mut concat_arrays = Vec::with_capacity(n_fields);
                for i in 0..n_fields {
                    let refs: Vec<&dyn Array> =
                        hn_slices.iter().map(|pb| pb.column(i).as_ref()).collect();
                    concat_arrays.push(
                        arrow::compute::concat(&refs)
                            .map_err(|e| format!("hn pool concat col {i}: {e}"))?,
                    );
                }
                RecordBatch::try_new(schema, concat_arrays)
                    .map_err(|e| format!("hn pool concat batch: {e}"))?
            };
            // `record_ids` aligns positionally with the concatenated pool rows.
            let record_ids = if config.graph_enabled {
                hn_slice_rids.into_iter().flatten().collect::<Vec<String>>()
            } else {
                Vec::new()
            };
            hn_pools.insert(
                plan.name.clone(),
                HnPool {
                    batch,
                    total_count,
                    record_ids,
                },
            );
        }

        // ── Dups (use last_batch) ──────────────────────────────────────────
        let has_dups: bool = plan.noise_types.iter().any(|n| n.count > 0);
        if has_dups && let Some((ref last_rb, last_n, last_offset)) = last_batch {
            let t_d0 = std::time::Instant::now();
            let mut dup_batches: Vec<RecordBatch> = Vec::new();
            let mut dup_mids_buf: Vec<String> = Vec::new();

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
            let fk_exclude_ref = &fk_exclude_cols;
            std::thread::scope(|s| {
                let mut handles = Vec::with_capacity(ndata.len());
                for (indices, seed, ntype, cols, cnt) in &ndata {
                    // `thread::scope` lets each spawned closure borrow
                    // `last_rb`/`indices`/`cols`/`fk_exclude_cols` directly —
                    // the scope guarantees every thread joins before these
                    // borrows end, so the per-thread `.clone()` of the
                    // batch, index array, column list and exclude list
                    // (all cheap Arc/Vec clones, but still real allocations
                    // times the noise-type count) was unnecessary.
                    let prefix_ref: &str = &prefix;
                    handles.push(s.spawn(move || {
                        let dup = pick_rows(last_rb, indices)?;
                        let mut rng = Rng::new(*seed);
                        let noisy =
                            apply_noise_to_batch(&dup, ntype, cols, &mut rng, fk_exclude_ref)?;
                        let mut mb = Vec::with_capacity(*cnt);
                        for j in 0..*cnt {
                            // `indices` are local to `last_rb` (0..last_n); the master_id
                            // is a pure function of (prefix, global index) — same
                            // formula as `batch_mids` above — so it's recomputed here
                            // instead of being cloned out of a retained master_id_pool.
                            let global_idx = last_offset + indices.value(j) as usize;
                            mb.push(format!("{}-{}", prefix_ref, pad_string(global_idx)));
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
                let dup_rids = record_id_strs(global_rid_offset..global_rid_offset + dup_total);

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
                    &dup_rids,
                    &dup_mids_buf,
                    &full_arc,
                    col_lookup.as_ref().unwrap(),
                    &mut null_cache,
                    &mut const_arr_cache,
                )?;

                let t_wd = std::time::Instant::now();
                writer
                    .write(&dup_rb_full)
                    .map_err(|e| format!("write dups: {e}"))?;
                _t_write += t_wd.elapsed().as_secs_f64();
                _write_calls += 1;

                // Graph: write duplicate nodes (edges emitted post-GT)
                if config.graph_enabled
                    && let Some(nw) = node_writer.as_mut()
                {
                    nw.write_batch(&dup_rb_full)
                        .map_err(|e| format!("write dup node: {e}"))?;
                }

                gt_acc.push_dup_batch(
                    dup_rb_full.column(0),
                    dup_rb_full.column(2),
                    dup_rb_full.column(3),
                )?;
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
    let mut hn_const_arr_cache: HashMap<(String, usize), ArrayRef> = HashMap::new();
    // Global counter for HN master_ids (shared across every hard_neg_types
    // entry in this run) instead of a random draw from a fixed 10M space:
    // at tens/hundreds of thousands of HN rows, `next_usize(10_000_000)`
    // has a real birthday-paradox chance of two unrelated HN rows landing
    // on the same master_id, which `gt.rs` would then count as sharing a
    // master — silently mislabeling a hard negative as a match. A counter
    // is unique by construction, eliminating the collision entirely.
    let mut hn_master_id_counter: u64 = 0;
    for hn_cfg in config.hard_neg_types.iter() {
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

        let (hn_rb, hn_src) = crate::hn_common::generate_hard_negatives(
            pool_rb,
            &hn_cfg.config_json,
            n_hn_max,
            config.seed.wrapping_add(200),
            config.graph_enabled,
        )?;

        let n_hn = hn_rb.num_rows();
        if n_hn == 0 {
            continue;
        }

        let hn_rids = record_id_strs(global_rid_offset..global_rid_offset + n_hn);

        let hn_mids: Vec<String> = (0..n_hn)
            .map(|_i| {
                let id = hn_master_id_counter;
                hn_master_id_counter += 1;
                format!("HN-{:09}", id)
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
            &hn_rids,
            &hn_mids,
            &full_arc,
            &hn_col_lookup,
            &mut hn_null_cache,
            &mut hn_const_arr_cache,
        )?;

        let t_wh = std::time::Instant::now();
        writer
            .write(&hn_rb_full)
            .map_err(|e| format!("write hn: {e}"))?;
        _t_write += t_wh.elapsed().as_secs_f64();
        _write_calls += 1;

        // Graph: write HN nodes + hard_neg edges (source = HN rid,
        // target = the pool row referenced by idx_a)
        if config.graph_enabled
            && let (Some(nw), Some(ew)) = (node_writer.as_mut(), edge_writer.as_mut())
        {
            nw.write_batch(&hn_rb_full)
                .map_err(|e| format!("write hn node: {e}"))?;
            if let Some((idx_a, pattern)) = hn_src {
                for i in 0..n_hn {
                    let tgt = &pool_data.record_ids[idx_a[i]];
                    ew.push(&hn_rids[i], tgt, "hard_neg", &pattern, 1.0)
                        .map_err(|e| format!("push hn edge: {e}"))?;
                }
            }
        }

        // Phase 13: collect ArrayRefs instead of per-row StringBuilder
        gt_acc.push_other_batch(
            hn_rb_full.column(0),
            hn_rb_full.column(2),
            hn_rb_full.column(3),
        )?;

        global_rid_offset += n_hn;
    }
    let t2_elapsed = t2.elapsed().as_secs_f64();

    // ── Canary records ────────────────────────────────────────────────────
    {
        let mut canary_null_cache: HashMap<(DataType, usize), ArrayRef> = HashMap::new();
        let mut canary_const_arr_cache: HashMap<(String, usize), ArrayRef> = HashMap::new();
        crate::canary::generate_all(
            ctx,
            config,
            &full_arc,
            &mut canary_null_cache,
            &mut canary_const_arr_cache,
            &mut global_rid_offset,
            &fk_pools,
            &mut writer,
            &mut node_writer,
            &mut gt_acc,
        )?;
    }

    // ── Phase 3: Finalize + GT (streamed, see crate::gt::GtAccumulator) ────
    let t3 = std::time::Instant::now();
    writer
        .finish()
        .map_err(|e| format!("finish dataset writer: {e}"))?;
    let t3a_elapsed = t3.elapsed().as_secs_f64();

    let t3b = std::time::Instant::now();
    let t_gt0 = std::time::Instant::now();
    let crate::gt::GtResult {
        n_exact_dup,
        n_hard_neg,
        n_unique,
        n_masters,
        cluster_map,
    } = gt_acc.finish(
        config.difficulty.as_str(),
        &config.output_format,
        &gt_path,
        &metadata,
    )?;
    let _t_gt_compute = 0.0f64;
    let _t_gt_write = t_gt0.elapsed().as_secs_f64();
    let t3b_elapsed = t3b.elapsed().as_secs_f64();
    let _t3_elapsed = t3.elapsed().as_secs_f64();

    // ── Graph: emit duplicate-cluster edges from the post-GT cluster_map ──
    if config.graph_enabled
        && let Some(ew) = edge_writer.as_mut()
    {
        crate::graph_gen::push_dup_clusters(ew, &cluster_map, GRAPH_MAX_CLUSTER_EDGES)
            .map_err(|e| format!("push dup clusters: {e}"))?;
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

    let stats = PipelineStats {
        total_records: global_rid_offset,
        exact_dups: n_exact_dup,
        hard_negs: n_hard_neg,
        uniques: n_unique,
        masters: n_masters,
    };

    // ── Finalize graph writers (base nodes + FK edges emitted so far) ──
    let mut graph_nodes_final: Option<String> = None;
    let mut graph_edges_final: Option<String> = None;
    if let (Some(nw), Some(ew)) = (node_writer, edge_writer) {
        nw.finish()
            .map_err(|e| format!("finish node writer: {e}"))?;
        ew.finish()
            .map_err(|e| format!("finish edge writer: {e}"))?;
        let mut np = graph_nodes_path.clone().unwrap();
        let mut ep = graph_edges_path.clone().unwrap();
        if graph_fmt == crate::graph_gen::GraphFormat::Parquet {
            let np_pq = format!("{}/{}_nodes.parquet", output_dir, config.run_id);
            let ep_pq = format!("{}/{}_edges.parquet", output_dir, config.run_id);
            convert_ipc_to_parquet(&np, &np_pq).map_err(|e| format!("graph nodes parquet: {e}"))?;
            convert_ipc_to_parquet(&ep, &ep_pq).map_err(|e| format!("graph edges parquet: {e}"))?;
            std::fs::remove_file(&np).ok();
            std::fs::remove_file(&ep).ok();
            np = np_pq;
            ep = ep_pq;
        }
        graph_nodes_final = Some(np);
        graph_edges_final = Some(ep);
    }

    Ok(PipelineOutput {
        output_files: vec![dataset_path],
        gt_file: gt_path,
        stats,
        nodes: graph_nodes_final,
        edges: graph_edges_final,
    })
}

/// IPC → Parquet (ZSTD) conversion for graph files, mirroring the dataset
/// conversion above. The node IPC already carries the `dupehell.*` metadata,
/// which is forwarded to the Parquet key/value metadata.
fn convert_ipc_to_parquet(ipc_path: &str, parquet_path: &str) -> Result<(), String> {
    use arrow::ipc::reader::FileReader;

    let ipc_file =
        std::fs::File::open(ipc_path).map_err(|e| format!("open IPC {ipc_path}: {e}"))?;
    let ipc_reader =
        FileReader::try_new(ipc_file, None).map_err(|e| format!("IPC reader {ipc_path}: {e}"))?;
    let schema = ipc_reader.schema();

    let parquet_file =
        std::fs::File::create(parquet_path).map_err(|e| format!("create {parquet_path}: {e}"))?;
    let zstd_level = parquet::basic::ZstdLevel::try_new(3).map_err(|e| format!("zstd: {e}"))?;
    let meta_kv: Vec<parquet::file::metadata::KeyValue> = schema
        .metadata()
        .iter()
        .map(|(k, v)| parquet::file::metadata::KeyValue {
            key: k.clone(),
            value: Some(v.clone()),
        })
        .collect();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(zstd_level))
        .set_max_row_group_row_count(Some(1_000_000))
        .set_key_value_metadata(Some(meta_kv))
        .build();
    let mut parquet_writer =
        parquet::arrow::ArrowWriter::try_new(parquet_file, schema, Some(props))
            .map_err(|e| format!("ArrowWriter {parquet_path}: {e}"))?;

    for batch_result in ipc_reader {
        let batch = batch_result.map_err(|e| format!("IPC batch {ipc_path}: {e}"))?;
        parquet_writer
            .write(&batch)
            .map_err(|e| format!("write parquet {parquet_path}: {e}"))?;
    }
    parquet_writer
        .close()
        .map_err(|e| format!("close parquet {parquet_path}: {e}"))?;
    Ok(())
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
        ("dupehell.difficulty".into(), config.difficulty.clone()),
        ("dupehell.run_id".into(), config.run_id.clone()),
        ("dupehell.timestamp".into(), ts),
        (
            "dupehell.structural_key_columns".into(),
            structural_key_columns_note(config),
        ),
    ])
}

/// Lists the `{entity}_id` columns that are structural join keys (linking an
/// entity's rows to its flattened child tables), not attributes to feed an
/// ER model — they never receive noise and stay identical across all
/// duplicates of the same master_id, unlike `record_id`.
fn structural_key_columns_note(config: &PipelineConfig) -> String {
    let cols: Vec<&str> = config
        .entity_plans
        .iter()
        .filter_map(|p| p.identifier_col.as_deref())
        .collect();
    format!(
        "{} — structural keys for joining child tables, not ER match attributes; use record_id as the row identifier instead",
        cols.join(", ")
    )
}

// ── Schema alignment ───────────────────────────────────────────────────────

/// Build the union schema from all entity plans (all columns + metadata).
fn build_full_schema(config: &PipelineConfig, metadata: &HashMap<String, String>) -> Schema {
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
    Schema::new(fields).with_metadata(metadata.clone())
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
    const_arr_cache: &mut HashMap<(String, usize), ArrayRef>,
) -> Result<RecordBatch, String> {
    let n = rb.num_rows();
    // `domain` is constant for the whole run and `entity_type` repeats
    // across every batch of a given entity (and often across HN/canary
    // sections too) — cache the built repeat-array per (value, n) instead
    // of rebuilding an n-length StringArray on every call, same idea as
    // `null_cache` above for the always-null columns.
    let domain_arr = const_arr_cache
        .entry((domain.to_string(), n))
        .or_insert_with(|| {
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                domain, n,
            ))) as ArrayRef
        })
        .clone();
    let et_arr = const_arr_cache
        .entry((entity_type.to_string(), n))
        .or_insert_with(|| {
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                entity_type,
                n,
            ))) as ArrayRef
        })
        .clone();
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
        .map_err(|e| format!("add_metadata_and_align RecordBatch: {e}"))
}

fn pick_rows(rb: &RecordBatch, indices: &UInt64Array) -> Result<RecordBatch, String> {
    use arrow::compute::take;
    let new_columns: Vec<ArrayRef> = (0..rb.num_columns())
        .map(|i| take(rb.column(i), indices, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("take error: {e}"))?;

    RecordBatch::try_new(rb.schema(), new_columns).map_err(|e| format!("RecordBatch: {e}"))
}
