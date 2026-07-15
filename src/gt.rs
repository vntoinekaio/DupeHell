// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use arrow::array::{Array, ArrayRef, AsArray, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

fn draft_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("record_id", DataType::Utf8, false),
        Field::new("master_id", DataType::Utf8, false),
        Field::new("entity_type", DataType::Utf8, false),
    ]))
}

/// Streaming, two-pass ground-truth builder.
///
/// Duplicate detection needs a genuinely global view of which masters are
/// duplicated (a master can be duplicated anywhere in the record stream),
/// but that's the *only* thing that needs to be global — everything else
/// can be processed one batch at a time. Previously the whole run
/// accumulated three full-dataset `Vec<ArrayRef>` (record_id/entity_type/
/// master_id) in RAM to concat and classify at the very end; at 200M+
/// records that's multiple GB held for the entire run just for GT
/// bookkeeping.
///
/// This accumulator instead:
/// 1. streams each batch straight to a small "draft" IPC file as it's
///    generated. The pipeline already knows, at the point each batch is
///    produced, whether its rows introduce brand-new masters
///    (`push_base_batch`) or are duplicate copies of masters seen earlier
///    (`push_dup_batch`) — that's how `pipeline::run_pipeline` builds
///    `dup_mids_buf` today. So instead of *discovering* duplication by
///    folding occurrence counts over every master ever seen (O(n_base) —
///    one entry per base record, duplicated or not), this only records the
///    (much smaller) set of masters that are *actually* duplicated —
///    O(n_dup_masters), a fraction of the dataset determined by the
///    doublet/triplet ratios, not the full base population. HN/CANARY rows
///    (`push_other_batch`) need no bookkeeping at all: their prefix alone
///    determines their classification.
/// 2. on `finish`, re-reads that draft file batch by batch — now that the
///    full duplicated-master set is known — to classify each row and
///    stream the definitive ground-truth file (IPC or Parquet), same
///    batch-by-batch pattern already used elsewhere in this codebase for
///    the IPC→Parquet dataset conversion (see `pipeline::run_pipeline`).
///
/// Duplicate detection is keyed on the **full** master_id string (entity
/// prefix included), not just a numeric suffix: master_ids are assigned
/// per-entity-plan starting from index 0, so two unrelated entities of
/// different types can share the same numeric suffix. Keying on the full
/// string avoids counting those as duplicates of each other.
pub struct GtAccumulator {
    draft_path: String,
    writer: arrow::ipc::writer::FileWriter<std::fs::File>,
    schema: Arc<Schema>,
    dup_masters: rustc_hash::FxHashSet<String>,
    n_base_masters: usize,
}

impl GtAccumulator {
    pub fn new(draft_path: &str) -> Result<Self, String> {
        let schema = draft_schema();
        let file = std::fs::File::create(draft_path)
            .map_err(|e| format!("create gt draft {draft_path}: {e}"))?;
        let writer = arrow::ipc::writer::FileWriter::try_new(file, &schema)
            .map_err(|e| format!("gt draft writer: {e}"))?;
        Ok(Self {
            draft_path: draft_path.to_string(),
            writer,
            schema,
            dup_masters: rustc_hash::FxHashSet::default(),
            n_base_masters: 0,
        })
    }

    fn write_draft(
        &mut self,
        record_ids: &ArrayRef,
        entity_types: &ArrayRef,
        master_ids: &ArrayRef,
    ) -> Result<(), String> {
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![record_ids.clone(), master_ids.clone(), entity_types.clone()],
        )
        .map_err(|e| format!("build gt draft batch: {e}"))?;
        self.writer
            .write(&batch)
            .map_err(|e| format!("write gt draft: {e}"))
    }

    /// Feed a batch of rows that each introduce a brand-new, not-yet-seen
    /// master (base records — one master per row, never a duplicate copy).
    pub fn push_base_batch(
        &mut self,
        record_ids: &ArrayRef,
        entity_types: &ArrayRef,
        master_ids: &ArrayRef,
    ) -> Result<(), String> {
        let mids = master_ids.as_string::<i32>();
        for i in 0..mids.len() {
            if !mids.is_null(i) {
                self.n_base_masters += 1;
            }
        }
        self.write_draft(record_ids, entity_types, master_ids)
    }

    /// Feed a batch of duplicate-copy rows (each `master_id` matches an
    /// existing base record already fed via `push_base_batch`). Records the
    /// *set* of duplicated masters — every row sharing one of these
    /// master_ids (the original base row included) is classified
    /// `exact_dup` at `finish`.
    pub fn push_dup_batch(
        &mut self,
        record_ids: &ArrayRef,
        entity_types: &ArrayRef,
        master_ids: &ArrayRef,
    ) -> Result<(), String> {
        let mids = master_ids.as_string::<i32>();
        for i in 0..mids.len() {
            if !mids.is_null(i) {
                let mid = mids.value(i);
                if !self.dup_masters.contains(mid) {
                    self.dup_masters.insert(mid.to_string());
                }
            }
        }
        self.write_draft(record_ids, entity_types, master_ids)
    }

    /// Feed a batch of rows whose classification is fully determined by
    /// their `master_id` prefix (hard negatives `HN-...`, canaries
    /// `CANARY-...`) — no bookkeeping needed beyond streaming to the draft.
    pub fn push_other_batch(
        &mut self,
        record_ids: &ArrayRef,
        entity_types: &ArrayRef,
        master_ids: &ArrayRef,
    ) -> Result<(), String> {
        self.write_draft(record_ids, entity_types, master_ids)
    }

    /// Consumes the accumulator: closes the draft, re-reads it batch by
    /// batch to classify each row now that the full duplicated-master set
    /// is known, and streams the definitive ground-truth file (IPC or
    /// Parquet). Returns `(n_exact_dup, n_hard_neg, n_unique, n_masters)`.
    pub fn finish(
        self,
        difficulty: &str,
        output_format: &str,
        final_path: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<(usize, usize, usize, usize), String> {
        let GtAccumulator {
            draft_path,
            mut writer,
            dup_masters,
            n_base_masters,
            ..
        } = self;
        writer
            .finish()
            .map_err(|e| format!("finish gt draft: {e}"))?;
        drop(writer);

        let final_schema = Arc::new(
            Schema::new(vec![
                Field::new("record_id", DataType::Utf8, false),
                Field::new("master_id", DataType::Utf8, false),
                Field::new("entity_type", DataType::Utf8, false),
                Field::new("match_type", DataType::Utf8, false),
                Field::new("difficulty", DataType::Utf8, false),
            ])
            .with_metadata(metadata.clone()),
        );

        let draft_file = std::fs::File::open(&draft_path)
            .map_err(|e| format!("reopen gt draft {draft_path}: {e}"))?;
        let reader = arrow::ipc::reader::FileReader::try_new(draft_file, None)
            .map_err(|e| format!("gt draft reader: {e}"))?;

        let mut sink = GtSink::new(output_format, final_path, &final_schema, metadata)?;

        let mut n_exact_dup = 0usize;
        let mut n_hard_neg = 0usize;
        let mut n_unique = 0usize;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| format!("read gt draft batch: {e}"))?;
            let n = batch.num_rows();
            let mid_col = batch.column(1).as_string::<i32>();

            let mut mt_builder = StringBuilder::with_capacity(n, n * 10);
            for i in 0..n {
                let mid = if mid_col.is_null(i) {
                    ""
                } else {
                    mid_col.value(i)
                };
                let mt = if mid.starts_with("HN-") {
                    n_hard_neg += 1;
                    "hard_neg"
                } else if mid.starts_with("CANARY-") {
                    "canary"
                } else if dup_masters.contains(mid) {
                    n_exact_dup += 1;
                    "exact_dup"
                } else {
                    n_unique += 1;
                    "unique"
                };
                mt_builder.append_value(mt);
            }
            let mt_arr: ArrayRef = Arc::new(mt_builder.finish());
            let diff_arr: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                difficulty, n,
            )));

            let final_batch = RecordBatch::try_new(
                final_schema.clone(),
                vec![
                    batch.column(0).clone(),
                    batch.column(1).clone(),
                    batch.column(2).clone(),
                    mt_arr,
                    diff_arr,
                ],
            )
            .map_err(|e| format!("build final gt batch: {e}"))?;

            sink.write(&final_batch)?;
        }

        sink.finish()?;
        std::fs::remove_file(&draft_path).ok();

        Ok((n_exact_dup, n_hard_neg, n_unique, n_base_masters))
    }
}

enum GtSink {
    Ipc(Box<arrow::ipc::writer::FileWriter<std::fs::File>>),
    Parquet(Box<parquet::arrow::ArrowWriter<std::fs::File>>),
}

impl GtSink {
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
                .set_data_page_size_limit(1_048_576)
                .set_key_value_metadata(Some(meta_kv))
                .build();
            let writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), Some(props))
                .map_err(|e| format!("gt parquet writer: {e}"))?;
            Ok(GtSink::Parquet(Box::new(writer)))
        } else {
            let writer = arrow::ipc::writer::FileWriter::try_new(file, schema)
                .map_err(|e| format!("gt ipc writer: {e}"))?;
            Ok(GtSink::Ipc(Box::new(writer)))
        }
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<(), String> {
        match self {
            GtSink::Ipc(w) => w.write(batch).map_err(|e| format!("write gt ipc: {e}")),
            GtSink::Parquet(w) => w.write(batch).map_err(|e| format!("write gt parquet: {e}")),
        }
    }

    fn finish(self) -> Result<(), String> {
        match self {
            GtSink::Ipc(mut w) => w.finish().map_err(|e| format!("finish gt ipc: {e}")),
            GtSink::Parquet(w) => w
                .close()
                .map(|_| ())
                .map_err(|e| format!("close gt parquet: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    fn tmp_path(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "dupehell_gt_test_{name}_{}.ipc",
            std::process::id()
        ));
        p.to_string_lossy().into_owned()
    }

    fn arr(values: Vec<&str>) -> ArrayRef {
        Arc::new(StringArray::from(values))
    }

    /// Reads the final GT file into a `record_id -> match_type` map. A map
    /// (rather than a positional `Vec`) keeps the assertions independent of
    /// draft row order, which is caller-determined (base/dup/other batches
    /// are pushed as separate, non-interleaved groups in the real pipeline).
    fn read_match_types(final_path: &str) -> HashMap<String, String> {
        let file = std::fs::File::open(final_path).unwrap();
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
        let mut out = HashMap::new();
        for batch in reader {
            let batch = batch.unwrap();
            let rid = batch
                .column_by_name("record_id")
                .unwrap()
                .as_string::<i32>();
            let mt = batch
                .column_by_name("match_type")
                .unwrap()
                .as_string::<i32>();
            for i in 0..batch.num_rows() {
                out.insert(rid.value(i).to_string(), mt.value(i).to_string());
            }
        }
        out
    }

    #[test]
    fn test_gt_accumulator_basic() {
        let draft = tmp_path("basic_draft");
        let final_path = tmp_path("basic_final");

        let mut acc = GtAccumulator::new(&draft).unwrap();
        // Base rows: R1 (master M-0000001, later duplicated), R3 (singleton
        // M-0000002), R6 (singleton M-0000005).
        acc.push_base_batch(
            &arr(vec!["R1", "R3", "R6"]),
            &arr(vec!["person", "person", "person"]),
            &arr(vec!["M-0000001", "M-0000002", "M-0000005"]),
        )
        .unwrap();
        // Dup row: R2 duplicates M-0000001.
        acc.push_dup_batch(
            &arr(vec!["R2"]),
            &arr(vec!["person"]),
            &arr(vec!["M-0000001"]),
        )
        .unwrap();
        // Hard negatives: R4, R5.
        acc.push_other_batch(
            &arr(vec!["R4", "R5"]),
            &arr(vec!["person", "person"]),
            &arr(vec!["HN-0000003", "HN-0000004"]),
        )
        .unwrap();

        let (ed, hn, un, masters) = acc
            .finish("medium", "ipc", &final_path, &HashMap::new())
            .unwrap();
        assert_eq!(ed, 2);
        assert_eq!(hn, 2);
        assert_eq!(un, 2);
        assert_eq!(masters, 3); // M-0000001, M-0000002, M-0000005

        let match_types = read_match_types(&final_path);
        assert_eq!(match_types["R1"], "exact_dup");
        assert_eq!(match_types["R2"], "exact_dup");
        assert_eq!(match_types["R3"], "unique");
        assert_eq!(match_types["R4"], "hard_neg");
        assert_eq!(match_types["R5"], "hard_neg");
        assert_eq!(match_types["R6"], "unique");

        std::fs::remove_file(&final_path).ok();
    }

    #[test]
    fn test_gt_accumulator_multi_batch() {
        let draft = tmp_path("multi_draft");
        let final_path = tmp_path("multi_final");

        let mut acc = GtAccumulator::new(&draft).unwrap();
        // Base batch: two masters, one of which (M-0000001) is duplicated
        // in a later, separate dup batch — simulating a master's duplicate
        // landing in a different batch than its base row.
        acc.push_base_batch(
            &arr(vec!["R1", "R3"]),
            &arr(vec!["person", "person"]),
            &arr(vec!["M-0000001", "M-0000002"]),
        )
        .unwrap();
        acc.push_dup_batch(
            &arr(vec!["R2"]),
            &arr(vec!["person"]),
            &arr(vec!["M-0000001"]),
        )
        .unwrap();

        let (ed, hn, un, masters) = acc
            .finish("medium", "ipc", &final_path, &HashMap::new())
            .unwrap();
        assert_eq!(ed, 2); // both rows of M-0000001
        assert_eq!(hn, 0);
        assert_eq!(un, 1);
        assert_eq!(masters, 2);

        std::fs::remove_file(&final_path).ok();
    }

    /// Regression: two different entity types can produce master_ids that
    /// share the same numeric suffix (each entity's index restarts at 0),
    /// but must not be counted as duplicates of each other.
    #[test]
    fn test_no_cross_entity_suffix_collision() {
        let draft = tmp_path("suffix_draft");
        let final_path = tmp_path("suffix_final");

        let mut acc = GtAccumulator::new(&draft).unwrap();
        acc.push_base_batch(
            &arr(vec!["R1", "R2"]),
            &arr(vec!["person", "account"]),
            &arr(vec!["PERSON-0000001", "ACCOUNT-0000001"]),
        )
        .unwrap();

        let (ed, _hn, un, masters) = acc
            .finish("medium", "ipc", &final_path, &HashMap::new())
            .unwrap();
        assert_eq!(ed, 0);
        assert_eq!(un, 2);
        assert_eq!(masters, 2);

        std::fs::remove_file(&final_path).ok();
    }
}
