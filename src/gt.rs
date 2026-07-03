use arrow::array::{Array, AsArray, StringArray};
use std::collections::HashMap;

/// Extract 7-digit numeric suffix from a master_id (e.g. "E00001-0000123" → 123).
/// Returns None if the suffix is not 7 ASCII digits.
#[inline(always)]
fn parse_suffix(mid: &str) -> Option<usize> {
    let bytes = mid.as_bytes();
    if bytes.len() < 7 {
        return None;
    }
    let start = bytes.len() - 7;
    let mut n = 0usize;
    for &b in &bytes[start..] {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n * 10 + (b - b'0') as usize;
    }
    Some(n)
}

/// Compute ground truth: returns (match_types, n_exact_dup, n_hard_neg, n_unique).
/// Uses a Vec<u32> indexed by the 7-digit suffix, eliminating all hash table overhead.
pub fn compute_gt(
    record_ids: &dyn Array,
    master_ids: &dyn Array,
    _entity_types: &dyn Array,
) -> (Vec<&'static str>, usize, usize, usize) {
    let n = record_ids.len();
    let mids = master_ids.as_string::<i32>();

    // Pre-allocated Vec covers all possible 7-digit suffixes (0..n, plus HN range)
    let max_id = n.max(10_000_000);
    let mut counts = vec![0u32; max_id + 1];

    // First pass: count non-HN master_ids via numeric suffix
    for i in 0..n {
        if !mids.is_null(i) {
            let mid = mids.value(i);
            if !mid.starts_with("HN-") {
                if let Some(num) = parse_suffix(mid) {
                    if num <= max_id {
                        counts[num] += 1;
                    }
                }
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
        } else if let Some(num) = parse_suffix(mid) {
            if num <= max_id && counts[num] > 1 {
                n_exact_dup += 1;
                "exact_dup"
            } else {
                n_unique += 1;
                "unique"
            }
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
    let diff_arr = StringArray::from_iter_values(std::iter::repeat(difficulty).take(n));

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
    let diff_arr = StringArray::from_iter_values(std::iter::repeat(difficulty).take(n));

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
}
