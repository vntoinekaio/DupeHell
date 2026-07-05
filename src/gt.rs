// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use arrow::array::{Array, AsArray, StringArray};
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::collections::HashMap;

/// Compute ground truth: returns (match_types, n_exact_dup, n_hard_neg, n_unique).
///
/// Duplicate detection is keyed on the **full** master_id string (entity prefix
/// included), not just a numeric suffix: master_ids are assigned per-entity-plan
/// starting from index 0, so two unrelated entities of different types can share
/// the same numeric suffix. Keying on the full string avoids counting those as
/// duplicates of each other.
pub fn compute_gt(
    record_ids: &dyn Array,
    master_ids: &dyn Array,
    _entity_types: &dyn Array,
) -> (Vec<&'static str>, usize, usize, usize) {
    let n = record_ids.len();
    let mids = master_ids.as_string::<i32>();

    // First pass: count non-HN/non-CANARY master_ids by their full string.
    let mut counts: HashMap<&str, u32> = HashMap::with_capacity(n);
    for i in 0..n {
        if !mids.is_null(i) {
            let mid = mids.value(i);
            if !mid.starts_with("HN-") && !mid.starts_with("CANARY-") {
                *counts.entry(mid).or_insert(0) += 1;
            }
        }
    }

    // Second pass: classify
    let mut match_types = Vec::with_capacity(n);
    let mut n_exact_dup = 0usize;
    let mut n_hard_neg = 0usize;
    let mut n_unique = 0usize;

    for i in 0..n {
        let mid = if mids.is_null(i) { "" } else { mids.value(i) };
        let mt = if mid.starts_with("HN-") {
            n_hard_neg += 1;
            "hard_neg"
        } else if mid.starts_with("CANARY-") {
            "canary"
        } else if counts.get(mid).copied().unwrap_or(0) > 1 {
            n_exact_dup += 1;
            "exact_dup"
        } else {
            n_unique += 1;
            "unique"
        };
        match_types.push(mt);
    }

    (match_types, n_exact_dup, n_hard_neg, n_unique)
}

/// Write ground truth as IPC.
pub fn write_gt_ipc(
    record_ids: &StringArray,
    master_ids: &StringArray,
    entity_types: &StringArray,
    match_types: &[&str],
    difficulty: &str,
    path: &str,
    metadata: &HashMap<String, String>,
) -> Result<(), String> {
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use std::sync::Arc;

    let n = record_ids.len();
    let schema = Arc::new(
        Schema::new(vec![
            Field::new("record_id", DataType::Utf8, false),
            Field::new("master_id", DataType::Utf8, false),
            Field::new("entity_type", DataType::Utf8, false),
            Field::new("match_type", DataType::Utf8, false),
            Field::new("difficulty", DataType::Utf8, false),
        ])
        .with_metadata(metadata.clone()),
    );

    let mt_arr = StringArray::from(match_types.to_vec());
    let diff_arr = StringArray::from_iter_values(std::iter::repeat_n(difficulty, n));

    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(record_ids.clone()),
            Arc::new(master_ids.clone()),
            Arc::new(entity_types.clone()),
            Arc::new(mt_arr),
            Arc::new(diff_arr),
        ],
    )
    .map_err(|e| format!("build gt batch: {e}"))?;

    let file = std::fs::File::create(path).map_err(|e| format!("create {path}: {e}"))?;
    let mut writer = FileWriter::try_new(file, &schema).map_err(|e| format!("ipc writer: {e}"))?;
    writer
        .write(&batch)
        .map_err(|e| format!("write gt ipc: {e}"))?;
    writer.finish().map_err(|e| format!("finish gt ipc: {e}"))?;
    Ok(())
}

/// Write ground truth as Parquet (ZSTD compressed).
pub fn write_gt_parquet(
    record_ids: &StringArray,
    master_ids: &StringArray,
    entity_types: &StringArray,
    match_types: &[&str],
    difficulty: &str,
    path: &str,
    metadata: &HashMap<String, String>,
) -> Result<(), String> {
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let n = record_ids.len();
    let schema = Arc::new(
        Schema::new(vec![
            Field::new("record_id", DataType::Utf8, false),
            Field::new("master_id", DataType::Utf8, false),
            Field::new("entity_type", DataType::Utf8, false),
            Field::new("match_type", DataType::Utf8, false),
            Field::new("difficulty", DataType::Utf8, false),
        ])
        .with_metadata(metadata.clone()),
    );

    let mt_arr = StringArray::from(match_types.to_vec());
    let diff_arr = StringArray::from_iter_values(std::iter::repeat_n(difficulty, n));

    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(record_ids.clone()),
            Arc::new(master_ids.clone()),
            Arc::new(entity_types.clone()),
            Arc::new(mt_arr),
            Arc::new(diff_arr),
        ],
    )
    .map_err(|e| format!("build gt batch: {e}"))?;

    let file = std::fs::File::create(path).map_err(|e| format!("create {path}: {e}"))?;
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
        .set_data_page_size_limit(1_048_576)
        .set_key_value_metadata(Some(meta_kv))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| format!("arrow writer: {e}"))?;
    writer.write(&batch).map_err(|e| format!("write gt: {e}"))?;
    writer.close().map_err(|e| format!("close gt: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_compute_gt() {
        let rids = StringArray::from(vec!["R1", "R2", "R3", "R4", "R5", "R6"]);
        let mids = StringArray::from(vec![
            "M-0000001",
            "M-0000001",
            "M-0000002",
            "HN-0000003",
            "HN-0000004",
            "M-0000005",
        ]);
        let ets = StringArray::from(vec![
            "person", "person", "person", "person", "person", "person",
        ]);
        let (mt, ed, hn, un) = compute_gt(&rids, &mids, &ets);
        assert_eq!(mt.len(), 6);
        assert_eq!(mt[0], "exact_dup");
        assert_eq!(mt[1], "exact_dup");
        assert_eq!(mt[2], "unique");
        assert_eq!(mt[3], "hard_neg");
        assert_eq!(mt[4], "hard_neg");
        assert_eq!(mt[5], "unique");
        assert_eq!(ed, 2);
        assert_eq!(hn, 2);
        assert_eq!(un, 2);
    }

    #[test]
    fn test_no_dedup_by_record_id() {
        let rids = StringArray::from(vec!["R1", "R1", "R2"]);
        let mids = StringArray::from(vec!["M-0000001", "M-0000001", "M-0000002"]);
        let ets = StringArray::from(vec!["person", "person", "person"]);
        let (mt, _ed, _hn, _un) = compute_gt(&rids, &mids, &ets);
        assert_eq!(mt.len(), 3);
    }

    /// Regression test: two different entity types can produce master_ids that
    /// share the same numeric suffix (each entity's index restarts at 0), but
    /// must not be counted as duplicates of each other.
    #[test]
    fn test_no_cross_entity_suffix_collision() {
        let rids = StringArray::from(vec!["R1", "R2"]);
        let mids = StringArray::from(vec!["PERSON-0000001", "ACCOUNT-0000001"]);
        let ets = StringArray::from(vec!["person", "account"]);
        let (mt, ed, _hn, un) = compute_gt(&rids, &mids, &ets);
        assert_eq!(mt[0], "unique");
        assert_eq!(mt[1], "unique");
        assert_eq!(ed, 0);
        assert_eq!(un, 2);
    }
}
