// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::collections::HashMap;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::context::Context;
use crate::entity_gen;
use crate::pipeline::{self, PipelineConfig};

const CANARY_COUNT: usize = 3;
const CANARY_SECRET: &str = "DupeHell-CANARY-v0.4-educational-use-only-2026";

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Deterministic signature for this run: sha256(secret + domain + size + seed)[:16]
pub fn compute_sig(domain: &str, size: usize, seed: u64) -> String {
    let input = format!("{}{}{}{}", CANARY_SECRET, domain, size, seed);
    let hash = Sha256::digest(input.as_bytes());
    to_hex(&hash[..8])
}

/// Generate canary records using the entity generator (so they look normal),
/// then override the email column with a verifiable canary signature.
#[allow(clippy::too_many_arguments)]
pub fn generate_all(
    ctx: &Context,
    config: &PipelineConfig,
    full_arc: &Arc<Schema>,
    null_cache: &mut HashMap<(DataType, usize), ArrayRef>,
    const_arr_cache: &mut HashMap<(String, usize), ArrayRef>,
    global_rid_offset: &mut usize,
    fk_pools: &HashMap<String, RecordBatch>,
    writer: &mut pipeline::DatasetWriter,
    node_writer: &mut Option<crate::graph_gen::NodeWriter>,
    gt_acc: &mut crate::gt::GtAccumulator,
) -> Result<(), String> {
    let sig = compute_sig(&config.domain, config.size, config.seed);
    let canary_seed = u64::from_str_radix(&sig, 16).unwrap();

    for (ent_idx, plan) in config.entity_plans.iter().enumerate() {
        let n = CANARY_COUNT;
        let batch_seed = canary_seed.wrapping_add(ent_idx as u64 * 1000);

        // Generate normally via entity generator
        let request_json = format!(
            r#"{{"entity_name":"{}","n":{},"seed":{},"columns":{}}}"#,
            plan.name, n, batch_seed, plan.columns_json,
        );
        let mut rb = entity_gen::generate_entity_batch(ctx, &request_json)?;

        // FK remap (same logic as pipeline.rs lines 513-528)
        let plan = &config.entity_plans[ent_idx];
        if !plan.fk_remaps.is_empty() {
            let mut fk_rng = crate::rng::Rng::new(batch_seed.wrapping_add(42));
            for remap in &plan.fk_remaps {
                if let Some(pool) = fk_pools.get(&remap.target_entity) {
                    let (remapped, _) = crate::fk_remap::fk_remap_batch(
                        &rb,
                        pool,
                        &remap.source_col,
                        &mut fk_rng,
                        false,
                    )?;
                    rb = remapped;
                }
            }
        }

        // Override every email-like column (whichever name(s) it has in this
        // domain — some entities carry more than one, e.g. `contact_email`
        // + `business_email`) so the canary signature is recoverable
        // regardless of the schema's naming, instead of only the 3 exact
        // names this used to special-case.
        let rb_schema = rb.schema();
        let email_idxs: Vec<usize> = rb_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                *f.data_type() == DataType::Utf8 && f.name().to_lowercase().contains("email")
            })
            .map(|(idx, _)| idx)
            .collect();

        if !email_idxs.is_empty() {
            let emails: Vec<String> = (0..n)
                .map(|i| format!("{}-{}-{}@canary.dupehell.data", sig, ent_idx, i))
                .collect();
            let new_col = Arc::new(StringArray::from(emails)) as ArrayRef;
            let schema = rb.schema();
            let mut columns: Vec<ArrayRef> = rb.columns().to_vec();
            for idx in email_idxs {
                columns[idx] = new_col.clone();
            }
            rb = RecordBatch::try_new(schema, columns)
                .map_err(|e| format!("rebuild canary batch: {e}"))?;
        }

        // Build col_lookup for alignment
        let col_lookup: Vec<Option<usize>> = full_arc
            .fields()
            .iter()
            .skip(4)
            .map(|f| rb.schema().column_with_name(f.name()).map(|(idx, _)| idx))
            .collect();

        let canary_rids = pipeline::record_id_strs(*global_rid_offset..*global_rid_offset + n);
        let canary_mids: Vec<String> = (0..n)
            .map(|j| format!("CANARY-{}-{:03}-{}", sig, j, ent_idx))
            .collect();

        let aligned = pipeline::add_metadata_and_align(
            &rb,
            &config.domain,
            &plan.name,
            &canary_rids,
            &canary_mids,
            full_arc,
            &col_lookup,
            null_cache,
            const_arr_cache,
        )?;

        writer
            .write(&aligned)
            .map_err(|e| format!("write canary batch: {e}"))?;

        // Graph: canary records become nodes; FK edges of canaries are
        // intentionally omitted (v1).
        if let Some(nw) = node_writer {
            nw.write_batch(&aligned)
                .map_err(|e| format!("write canary node: {e}"))?;
        }

        gt_acc.push_other_batch(aligned.column(0), aligned.column(2), aligned.column(3))?;

        *global_rid_offset += n;
    }

    Ok(())
}
