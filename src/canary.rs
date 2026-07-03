use std::collections::HashMap;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use crate::context::Context;
use crate::entity_gen;
use crate::pipeline::{self, IdPools, PipelineConfig};

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
pub fn generate_all(
    ctx: &Context,
    config: &PipelineConfig,
    full_arc: &Arc<Schema>,
    null_cache: &mut HashMap<(DataType, usize), ArrayRef>,
    global_rid_offset: &mut usize,
    ids: &IdPools,
    writer: &mut arrow::ipc::writer::FileWriter<std::fs::File>,
    gt_record_id_arrs: &mut Vec<ArrayRef>,
    gt_entity_type_arrs: &mut Vec<ArrayRef>,
    gt_master_id_arrs: &mut Vec<ArrayRef>,
) -> Result<(), String> {
    let sig = compute_sig(&config.domain, config.size, config.seed);
    let canary_seed = u64::from_le_bytes(sig.as_bytes()[..8].try_into().unwrap());

    for (ent_idx, plan) in config.entity_plans.iter().enumerate() {
        let n = CANARY_COUNT;
        let batch_seed = canary_seed.wrapping_add(ent_idx as u64 * 1000);

        // Generate normally via entity generator
        let request_json = format!(
            r#"{{"entity_name":"{}","n":{},"seed":{},"columns":{}}}"#,
            plan.name, n, batch_seed, plan.columns_json,
        );
        let mut rb = entity_gen::generate_entity_batch(ctx, &request_json)?;

        // Override the email column (whichever name it has in this domain)
        let rb_schema = rb.schema();
        let email_idx_opt = rb_schema
            .column_with_name("email_address")
            .or_else(|| rb_schema.column_with_name("business_email"))
            .or_else(|| rb_schema.column_with_name("email"))
            .map(|(idx, _)| idx);

        if let Some(email_idx) = email_idx_opt {
            let emails: Vec<String> = (0..n)
                .map(|i| format!("{}-{}-{}@canary.dupehell.data", sig, ent_idx, i))
                .collect();
            let new_col = Arc::new(StringArray::from(emails)) as ArrayRef;
            let schema = rb.schema();
            let mut columns: Vec<ArrayRef> = rb.columns().to_vec();
            columns[email_idx] = new_col;
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

        let canary_rids: Vec<String> = (0..n)
            .map(|j| ids.record_ids[*global_rid_offset + j].clone())
            .collect();
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
        );

        writer
            .write(&aligned)
            .map_err(|e| format!("write canary batch: {e}"))?;

        gt_record_id_arrs.push(aligned.column(0).clone());
        gt_entity_type_arrs.push(aligned.column(2).clone());
        gt_master_id_arrs.push(aligned.column(3).clone());

        *global_rid_offset += n;
    }

    Ok(())
}
