// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// Result of [`GtAccumulator::finish`].
pub struct GtResult {
    /// Cluster members that are genuinely byte-for-byte identical to their
    /// master (the master itself, plus any duplicate copy whose assigned
    /// noise ended up a no-op — see `pipeline::unchanged_row_mask`).
    pub n_exact_dup: usize,
    /// Cluster members that are duplicates of a master but differ from it
    /// on at least one column (noise was actually applied). Distinguished
    /// from `n_exact_dup` so a consumer can't mistake a fuzzy duplicate for
    /// a trivial byte-for-byte match.
    pub n_fuzzy_dup: usize,
    pub n_hard_neg: usize,
    pub n_unique: usize,
    pub n_masters: usize,
    /// Maps each duplicated `master_id` to the full set of `(record_id,
    /// is_identical)` pairs in its cluster (base + duplicate copies, exact
    /// and fuzzy alike) — `is_identical` mirrors that record's own
    /// `exact_dup`/`fuzzy_dup` classification. Consumed by
    /// `graph_gen::push_dup_clusters` to decide, per edge, whether the pair
    /// it connects is `exact_dup` (both ends byte-identical to the master,
    /// hence to each other) or `fuzzy_dup` (at least one end was noised).
    pub cluster_map: HashMap<String, Vec<(String, bool)>>,
}

fn draft_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("record_id", DataType::Utf8, false),
        Field::new("master_id", DataType::Utf8, false),
        Field::new("entity_type", DataType::Utf8, false),
        // `true` for base/master rows (identical to themselves by
        // definition) and for duplicate-copy rows whose assigned noise
        // ended up not changing anything visible; `false` for duplicate
        // copies that were genuinely altered. Meaningless for hard_neg/
        // canary rows (classified by master_id prefix regardless).
        Field::new("is_identical", DataType::Boolean, false),
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
        is_identical: &ArrayRef,
    ) -> Result<(), String> {
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                record_ids.clone(),
                master_ids.clone(),
                entity_types.clone(),
                is_identical.clone(),
            ],
        )
        .map_err(|e| format!("build gt draft batch: {e}"))?;
        self.writer
            .write(&batch)
            .map_err(|e| format!("write gt draft: {e}"))
    }

    /// Feed a batch of rows that each introduce a brand-new, not-yet-seen
    /// master (base records — one master per row, never a duplicate copy).
    /// A base row is trivially identical to itself, so it always carries
    /// `is_identical = true`.
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
        let all_true: ArrayRef = Arc::new(BooleanArray::from(vec![true; mids.len()]));
        self.write_draft(record_ids, entity_types, master_ids, &all_true)
    }

    /// Feed a batch of duplicate-copy rows (each `master_id` matches an
    /// existing base record already fed via `push_base_batch`). Records the
    /// *set* of duplicated masters — every row sharing one of these
    /// master_ids (the original base row included) belongs to a duplicated
    /// cluster at `finish`, classified `exact_dup` or `fuzzy_dup` per-row
    /// based on `is_identical` (`true` when the noise assigned to this copy
    /// ended up a no-op — see `pipeline::unchanged_row_mask` — `false` when
    /// it produced a real, visible change).
    pub fn push_dup_batch(
        &mut self,
        record_ids: &ArrayRef,
        entity_types: &ArrayRef,
        master_ids: &ArrayRef,
        is_identical: &ArrayRef,
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
        self.write_draft(record_ids, entity_types, master_ids, is_identical)
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
        // `is_identical` is meaningless for hard_neg/canary rows (classified
        // by master_id prefix regardless), so the value doesn't matter.
        let all_false: ArrayRef = Arc::new(BooleanArray::from(vec![false; record_ids.len()]));
        self.write_draft(record_ids, entity_types, master_ids, &all_false)
    }

    /// Consumes the accumulator: closes the draft, re-reads it batch by
    /// batch to classify each row now that the full duplicated-master set
    /// is known, and streams the definitive ground-truth file (IPC or
    /// Parquet). Returns a [`GtResult`].
    pub fn finish(
        self,
        difficulty: &str,
        output_format: &str,
        final_path: &str,
        metadata: &HashMap<String, String>,
    ) -> Result<GtResult, String> {
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
        let mut n_fuzzy_dup = 0usize;
        let mut n_hard_neg = 0usize;
        let mut n_unique = 0usize;
        // Cluster membership for duplicated masters (base + duplicate copies,
        // exact and fuzzy alike), for emitting duplicate-cluster edges after
        // this pass.
        let mut cluster_map: HashMap<String, Vec<(String, bool)>> = HashMap::new();

        for batch_result in reader {
            let batch = batch_result.map_err(|e| format!("read gt draft batch: {e}"))?;
            let n = batch.num_rows();
            let mid_col = batch.column(1).as_string::<i32>();
            let rid_col = batch.column(0).as_string::<i32>();
            let ident_col = batch.column(3).as_boolean();

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
                    // Per-row, not per-cluster: a cluster can mix a base row,
                    // a copy the noise happened not to change, and a copy
                    // that's genuinely different — each is classified on its
                    // own `is_identical` value, not the cluster's as a whole.
                    if !ident_col.is_null(i) && ident_col.value(i) {
                        n_exact_dup += 1;
                        "exact_dup"
                    } else {
                        n_fuzzy_dup += 1;
                        "fuzzy_dup"
                    }
                } else {
                    n_unique += 1;
                    "unique"
                };
                mt_builder.append_value(mt);
                // Every row of a duplicated master belongs to its cluster,
                // tagged with its own identical/fuzzy status.
                if dup_masters.contains(mid) {
                    let is_identical = !ident_col.is_null(i) && ident_col.value(i);
                    cluster_map
                        .entry(mid.to_string())
                        .or_default()
                        .push((rid_col.value(i).to_string(), is_identical));
                }
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

        Ok(GtResult {
            n_exact_dup,
            n_fuzzy_dup,
            n_hard_neg,
            n_unique,
            n_masters: n_base_masters,
            cluster_map,
        })
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

    fn barr(values: Vec<bool>) -> ArrayRef {
        Arc::new(BooleanArray::from(values))
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
        // Dup row: R2 duplicates M-0000001, unchanged by its noise pass.
        acc.push_dup_batch(
            &arr(vec!["R2"]),
            &arr(vec!["person"]),
            &arr(vec!["M-0000001"]),
            &barr(vec![true]),
        )
        .unwrap();
        // Hard negatives: R4, R5.
        acc.push_other_batch(
            &arr(vec!["R4", "R5"]),
            &arr(vec!["person", "person"]),
            &arr(vec!["HN-0000003", "HN-0000004"]),
        )
        .unwrap();

        let GtResult {
            n_exact_dup: ed,
            n_hard_neg: hn,
            n_unique: un,
            n_masters: masters,
            ..
        } = acc
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

    /// A duplicate copy whose noise pass actually changed something
    /// (`is_identical = false`) must be classified `fuzzy_dup`, not
    /// `exact_dup` — the master itself, and any sibling copy that was left
    /// unchanged, keep `exact_dup` independently, per row.
    #[test]
    fn test_gt_accumulator_fuzzy_dup() {
        let draft = tmp_path("fuzzy_draft");
        let final_path = tmp_path("fuzzy_final");

        let mut acc = GtAccumulator::new(&draft).unwrap();
        // Triplet: R1 is the master, R2 was left unchanged by its noise
        // pass, R3 was genuinely altered.
        acc.push_base_batch(
            &arr(vec!["R1"]),
            &arr(vec!["person"]),
            &arr(vec!["M-0000001"]),
        )
        .unwrap();
        acc.push_dup_batch(
            &arr(vec!["R2", "R3"]),
            &arr(vec!["person", "person"]),
            &arr(vec!["M-0000001", "M-0000001"]),
            &barr(vec![true, false]),
        )
        .unwrap();

        let GtResult {
            n_exact_dup: ed,
            n_fuzzy_dup: fd,
            n_unique: un,
            n_masters: masters,
            ..
        } = acc
            .finish("medium", "ipc", &final_path, &HashMap::new())
            .unwrap();
        assert_eq!(ed, 2); // R1 (master) + R2 (unchanged copy)
        assert_eq!(fd, 1); // R3 (genuinely noised copy)
        assert_eq!(un, 0);
        assert_eq!(masters, 1);

        let match_types = read_match_types(&final_path);
        assert_eq!(match_types["R1"], "exact_dup");
        assert_eq!(match_types["R2"], "exact_dup");
        assert_eq!(match_types["R3"], "fuzzy_dup");

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
            &barr(vec![true]),
        )
        .unwrap();

        let GtResult {
            n_exact_dup: ed,
            n_hard_neg: hn,
            n_unique: un,
            n_masters: masters,
            ..
        } = acc
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

        let GtResult {
            n_exact_dup: ed,
            n_unique: un,
            n_masters: masters,
            ..
        } = acc
            .finish("medium", "ipc", &final_path, &HashMap::new())
            .unwrap();
        assert_eq!(ed, 0);
        assert_eq!(un, 2);
        assert_eq!(masters, 2);

        std::fs::remove_file(&final_path).ok();
    }

    /// `finish()` returns a `cluster_map` mapping each duplicated master_id
    /// to the full set of `(record_id, is_identical)` pairs in its cluster
    /// (base + duplicate copies). This is the structure consumed by
    /// `push_dup_clusters` to decide, per edge, `exact_dup` vs `fuzzy_dup`.
    #[test]
    fn test_cluster_map_contents() {
        let draft = tmp_path("cm_draft");
        let final_path = tmp_path("cm_final");

        let mut acc = GtAccumulator::new(&draft).unwrap();
        acc.push_base_batch(
            &arr(vec!["R1", "R3", "R6"]),
            &arr(vec!["person", "person", "person"]),
            &arr(vec!["M-0000001", "M-0000002", "M-0000005"]),
        )
        .unwrap();
        // R2 (M-0000001) stayed identical; R7 (M-0000005) was genuinely noised.
        acc.push_dup_batch(
            &arr(vec!["R2", "R7"]),
            &arr(vec!["person", "person"]),
            &arr(vec!["M-0000001", "M-0000005"]),
            &barr(vec![true, false]),
        )
        .unwrap();
        acc.push_other_batch(
            &arr(vec!["R4", "R5"]),
            &arr(vec!["person", "person"]),
            &arr(vec!["HN-0000003", "HN-0000004"]),
        )
        .unwrap();

        let GtResult {
            cluster_map: cm, ..
        } = acc
            .finish("medium", "ipc", &final_path, &HashMap::new())
            .unwrap();

        // Only duplicated masters appear; singletons (M-0000002) do not.
        assert_eq!(cm.len(), 2);
        let mut m1 = cm.get("M-0000001").unwrap().clone();
        m1.sort();
        assert_eq!(m1, vec![("R1".to_string(), true), ("R2".to_string(), true)]);
        let mut m5 = cm.get("M-0000005").unwrap().clone();
        m5.sort();
        assert_eq!(
            m5,
            vec![("R6".to_string(), true), ("R7".to_string(), false)]
        );
        assert!(!cm.contains_key("M-0000002"));

        std::fs::remove_file(&final_path).ok();
    }
}
