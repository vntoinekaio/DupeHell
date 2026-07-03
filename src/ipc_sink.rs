// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::collections::HashMap;
use std::fs::File;

use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;

use crate::context::Context;
use crate::entity_gen;
use crate::fk_remap;
use crate::rng::Rng;

/// Write a RecordBatch to an Arrow IPC file.
pub fn write_ipc_direct(rb: &RecordBatch, output_path: &str) -> Result<(), String> {
    let file = File::create(output_path)
        .map_err(|e| format!("cannot create {output_path}: {e}"))?;

    let schema = rb.schema();
    let mut writer = FileWriter::try_new(file, &schema)
        .map_err(|e| format!("FileWriter error: {e}"))?;

    writer
        .write(rb)
        .map_err(|e| format!("write IPC error: {e}"))?;

    writer
        .finish()
        .map_err(|e| format!("finish IPC error: {e}"))?;

    Ok(())
}

/// Generate an entity batch and write it to an Arrow IPC (Feather) file.
pub fn generate_and_write_ipc(
    ctx: &Context,
    request_json: &str,
    output_path: &str,
) -> Result<(), String> {
    let rb = entity_gen::generate_entity_batch(ctx, request_json)?;
    write_ipc_direct(&rb, output_path)
}

/// Generate an entity batch, write it to IPC, AND return the RecordBatch.
/// Used by the main pipeline so Python gets the dict (for FK/HN pools)
/// AND the IPC file (for mmap-based sink via `sink_from_ipc`).
pub fn generate_and_return_with_ipc(
    ctx: &Context,
    request_json: &str,
    output_path: &str,
) -> Result<RecordBatch, String> {
    let rb = entity_gen::generate_entity_batch(ctx, request_json)?;
    write_ipc_direct(&rb, output_path)?;
    Ok(rb)
}

/// Generate an entity batch, remap FK columns, write IPC, AND return the RecordBatch.
///
/// `fk_pools` maps column name → single-column RecordBatch of FK identifier values.
/// `seed` is used for deterministic random index generation into each pool.
/// The IPC file contains the FK-remapped data so sink_from_ipc needs no override_cols.
pub fn generate_and_return_with_ipc_fk(
    ctx: &Context,
    request_json: &str,
    output_path: &str,
    fk_pools: HashMap<String, RecordBatch>,
    remap_cols: Vec<String>,
    seed: u64,
) -> Result<RecordBatch, String> {
    let rb = entity_gen::generate_entity_batch(ctx, request_json)?;
    if !fk_pools.is_empty() && !remap_cols.is_empty() {
        let mut rng = Rng::new(seed);
        let remapped = fk_remap::fk_remap_batch_from_map(&rb, &fk_pools, &remap_cols, &mut rng)?;
        write_ipc_direct(&remapped, output_path)?;
        Ok(remapped)
    } else {
        write_ipc_direct(&rb, output_path)?;
        Ok(rb)
    }
}
