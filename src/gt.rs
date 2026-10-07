// DupeHell -- MIT License
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// No liability for misuse.

use arrow::array::{Array, ArrayRef, AsArray, Int32Array, Int32Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// Bit-packed boolean buffer — 1 bit per entry instead of `Vec<bool>`'s 1
/// byte, an 8x reduction on `ClusterCsr::is_identical`, which parallels
/// `records` (potentially tens of millions of entries at 100M+ hell scale
/// with `--graph`, a low singleton fraction).
struct Bitset {
    words: Vec<u64>,
    len: usize,
}

impl Bitset {
    fn with_capacity(n: usize) -> Self {
        Self {
            words: vec![0u64; n.div_ceil(64).max(1)],
            len: 0,
        }
    }

    fn push(&mut self, val: bool) {
        let word_idx = self.len / 64;
        if word_idx >= self.words.len() {
            self.words.push(0);
        }
        if val {
            self.words[word_idx] |= 1u64 << (self.len % 64);
        }
        self.len += 1;
    }

    fn get(&self, i: usize) -> bool {
        (self.words[i / 64] >> (i % 64)) & 1 != 0
    }
}

/// View into a contiguous range of a [`Bitset`], returned per-cluster by
/// [`ClusterCsr::groups`] — parallel to the `&[u64]` record-index slice for
/// the same cluster.
pub struct BitspanRef<'a> {
    bits: &'a Bitset,
    start: usize,
    end: usize,
}

impl BitspanRef<'_> {
    /// `i` is local to this span (`0..self.len()`), matching how callers
    /// already index the parallel `&[u64]` records slice.
    pub fn get(&self, i: usize) -> bool {
        self.bits.get(self.start + i)
    }

    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    #[cfg(test)]
    fn to_vec(&self) -> Vec<bool> {
        (0..self.len()).map(|i| self.get(i)).collect()
    }
}

/// Compressed-sparse-row representation of the duplicate-cluster
/// membership. Built once, in `GtAccumulator::finish`, from every
/// `(master, record, is_identical)` triple belonging to a duplicated master.
///
/// Avoids one `String` allocation per `record_id` and one per distinct
/// `master_id` (the dominant RAM cost of a string-keyed cluster map at 200M+
/// records): `master_id`/`record_id` are packed into `u64`s
/// (`pipeline::pack_master_parts` / the global record index) and stored in
/// two flat, contiguous buffers instead of a hash map of per-cluster `Vec`s.
///
/// `master_id` itself is never needed downstream as a string — the only
/// consumer (`graph_gen::push_dup_clusters`) only ever used it to get a
/// deterministic cluster iteration order, and numeric comparison of the
/// packed key preserves the lexicographic order of the original string
/// (both segments are zero-padded to a fixed width).
pub struct ClusterCsr {
    /// `offsets.len() == n_clusters + 1`; cluster `k`'s members are
    /// `records[offsets[k]..offsets[k+1]]` (and the parallel span of
    /// `is_identical`). Sorted ascending by packed master key.
    offsets: Vec<u32>,
    /// Flat, cluster-grouped buffer of global record indices (see
    /// `pipeline::record_id_string`/`parse_record_idx`), ascending within
    /// each cluster.
    records: Vec<u64>,
    /// Parallel to `records`: whether that record is byte-identical to its
    /// cluster's master.
    is_identical: Bitset,
}

/// `record_idx` is bounded by `PAD_LEN` (13) decimal digits — `< 10^13 ≈
/// 2^43.2` — so bit 63 is always free to carry `is_identical` alongside it
/// in one `u64`, the same trick `pack_master_parts` already uses for its own
/// high bits. Packing this into `ClusterCsr::build`'s transient pair list
/// drops it from `(u64, u64, bool)` (24 bytes/entry — `bool` pads the tuple
/// to the next 8-byte alignment boundary) to `(u64, u64)` (16 bytes/entry,
/// −33%), at potentially tens of millions of entries (every row of every
/// duplicated cluster, `--graph` mode).
const RIDX_IDENTICAL_BIT: u64 = 1 << 63;
const RIDX_MASK: u64 = !RIDX_IDENTICAL_BIT;

/// Packs `(record_idx, is_identical)` into `ClusterCsr::build`'s pair
/// representation — see `RIDX_IDENTICAL_BIT`.
pub(crate) fn pack_ridx_identical(record_idx: u64, is_identical: bool) -> u64 {
    debug_assert_eq!(
        record_idx & RIDX_IDENTICAL_BIT,
        0,
        "record_idx must fit in the low 63 bits"
    );
    record_idx | if is_identical { RIDX_IDENTICAL_BIT } else { 0 }
}

impl ClusterCsr {
    /// Builds the CSR from an unordered flat list of `(packed_master_key,
    /// packed_ridx_identical)` pairs (see `pack_ridx_identical`). Sorting
    /// once by `(master_key, record_idx)` — masking off the packed
    /// `is_identical` bit for the comparison, so it can't perturb member
    /// order within a cluster — both groups every pair by cluster and
    /// orders each cluster's members ascending, in one pass; no need to
    /// pre-count group sizes or build the structure incrementally.
    ///
    /// Sorted in parallel (`rayon`, already a warm dependency by this point
    /// in the run): the sort key `(master_key, record_idx)` is unique per
    /// entry (one record belongs to exactly one cluster, and a
    /// `record_idx` is unique across the whole run), so an unstable
    /// parallel sort produces the exact same total order as a
    /// single-threaded one — determinism doesn't depend on stability here.
    pub(crate) fn build(mut pairs: Vec<(u64, u64)>) -> Self {
        use rayon::slice::ParallelSliceMut;
        pairs.par_sort_unstable_by_key(|&(mk, r)| (mk, r & RIDX_MASK));

        let mut offsets = Vec::new();
        let mut records = Vec::with_capacity(pairs.len());
        let mut is_identical = Bitset::with_capacity(pairs.len());
        let mut last_key: Option<u64> = None;

        for (mk, r) in pairs {
            if last_key != Some(mk) {
                offsets.push(records.len() as u32);
                last_key = Some(mk);
            }
            records.push(r & RIDX_MASK);
            is_identical.push(r & RIDX_IDENTICAL_BIT != 0);
        }
        offsets.push(records.len() as u32);

        Self {
            offsets,
            records,
            is_identical,
        }
    }

    /// Iterates clusters in ascending packed-master-key order, yielding each
    /// cluster's `(record_indices, is_identical)` pair. A cluster's
    /// `record_indices` are already ascending, so callers
    /// (`push_dup_clusters`) don't need to sort members themselves.
    pub fn groups(&self) -> impl Iterator<Item = (&[u64], BitspanRef<'_>)> {
        self.offsets.windows(2).map(move |w| {
            let (start, end) = (w[0] as usize, w[1] as usize);
            (
                &self.records[start..end],
                BitspanRef {
                    bits: &self.is_identical,
                    start,
                    end,
                },
            )
        })
    }

    #[cfg(test)]
    fn n_clusters(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }
}

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
    /// Every duplicated master's full cluster (base + duplicate copies,
    /// exact and fuzzy alike), each member tagged with its own
    /// `exact_dup`/`fuzzy_dup` status. Consumed by
    /// `graph_gen::push_dup_clusters` to decide, per edge, whether the pair
    /// it connects is `exact_dup` (both ends byte-identical to the master,
    /// hence to each other) or `fuzzy_dup` (at least one end was noised).
    /// Only populated when cluster tracking is enabled (`--graph`).
    pub cluster_map: ClusterCsr,
}

/// `Dictionary(Int32, Utf8)` for `entity_type`/`match_type`/`difficulty` —
/// an explicit output-contract change (not bit-identical: the declared
/// column type changes from `Utf8`). `entity_type` here must match
/// `pipeline::low_cardinality_dict_type` exactly — this column's data is
/// `add_metadata_and_align`'s `et_arr`, passed straight through, so the
/// schema must declare the same type it's actually built as.
fn low_cardinality_dict_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

/// One entity batch's base rows: every row introduces a brand-new master.
/// Masters are contiguous — row `j`'s master is global index
/// `first_master_idx + j` of entity `entity_idx`, and its record is global
/// record index `first_record_idx + j` (the same numbers the pipeline
/// formats into `master_id`/`record_id`).
pub struct BaseRows<'a> {
    pub record_ids: &'a ArrayRef,
    pub entity_types: &'a ArrayRef,
    pub master_ids: &'a ArrayRef,
    pub entity_idx: u64,
    pub first_master_idx: u64,
    pub first_record_idx: u64,
}

/// The duplicate copies generated from one [`BaseRows`] batch. Every copy's
/// master belongs to that same batch (`pipeline.rs` samples a batch's
/// duplicates exclusively from its own masters), which is what lets the
/// whole batch be classified as soon as both are known.
pub struct DupRows<'a> {
    pub record_ids: &'a ArrayRef,
    pub entity_types: &'a ArrayRef,
    pub master_ids: &'a ArrayRef,
    /// Global master index of each copy, within
    /// `base.first_master_idx..base.first_master_idx + base.len()`.
    pub master_idx: &'a [usize],
    /// `true` when the noise assigned to that copy ended up a no-op (see
    /// `pipeline::unchanged_row_mask`), `false` when it produced a real,
    /// visible change.
    pub is_identical: &'a [bool],
    pub first_record_idx: u64,
}

/// Per-batch scratch bitset over a base batch's masters (`0..n` local
/// indices), reused across batches instead of reallocated.
#[derive(Default)]
struct LocalBits {
    words: Vec<u64>,
}

impl LocalBits {
    fn reset(&mut self, n: usize) {
        self.words.clear();
        self.words.resize(n.div_ceil(64), 0);
    }

    fn set(&mut self, i: usize) {
        self.words[i / 64] |= 1u64 << (i % 64);
    }

    fn get(&self, i: usize) -> bool {
        (self.words[i / 64] >> (i % 64)) & 1 != 0
    }
}

/// Streaming, single-pass ground-truth builder.
///
/// `pipeline.rs` draws every duplicate copy of a batch exclusively from that
/// same batch's masters, and writes the copies right after the batch. So at
/// the end of each batch, whether each of its masters is duplicated — and
/// whether at least one copy is byte-identical to it — is already final: no
/// later batch can add a copy of it. Each batch is therefore classified and
/// streamed straight to the final ground-truth file (IPC or Parquet) as soon
/// as its duplicates are known, with only two per-batch scratch bitsets as
/// state. Hard negatives (`HN-…`) and canaries (`CANARY-…`) are never
/// duplicated and are classified by kind.
///
/// This replaces an earlier two-pass design (draft file of every row +
/// global duplicated-master hash sets + full re-read and reclassification
/// at the end), which an isolated benchmark measured at ~4x the cost
/// (`examples/profile_gt_single_pass.rs`, byte-identical output).
pub struct GtAccumulator {
    sink: GtSink,
    schema: Arc<Schema>,
    match_type_dict: crate::pipeline::DictValues,
    difficulty_dict: crate::pipeline::DictValues,
    difficulty: String,
    // `match_type` dictionary keys, resolved once instead of hashing the
    // value string for every row.
    k_hard_neg: i32,
    k_canary: i32,
    k_exact_dup: i32,
    k_fuzzy_dup: i32,
    k_unique: i32,
    /// `difficulty` is constant for the whole run and batch sizes only take
    /// a handful of values — cache the built column by length instead of
    /// rebuilding it every batch.
    diff_arr_cache: Option<(usize, ArrayRef)>,
    n_exact_dup: usize,
    n_fuzzy_dup: usize,
    n_hard_neg: usize,
    n_unique: usize,
    n_masters: usize,
    /// Cluster membership for duplicated masters, only built when `--graph`
    /// needs it (`graph_gen::push_dup_clusters` is its sole consumer) —
    /// one entry per duplicate-cluster row, the majority of the dataset at
    /// hell's low singleton fraction, so it's pure wasted RAM otherwise.
    track_clusters: bool,
    cluster_pairs: Vec<(u64, u64)>,
    is_dup: LocalBits,
    has_exact_copy: LocalBits,
}

impl GtAccumulator {
    pub fn new(
        difficulty: &str,
        output_format: &str,
        final_path: &str,
        metadata: &HashMap<String, String>,
        track_clusters: bool,
    ) -> Result<Self, String> {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("record_id", DataType::Utf8, false),
                Field::new("master_id", DataType::Utf8, false),
                Field::new("entity_type", low_cardinality_dict_type(), false),
                Field::new("match_type", low_cardinality_dict_type(), false),
                Field::new("difficulty", low_cardinality_dict_type(), false),
            ])
            .with_metadata(metadata.clone()),
        );
        let sink = GtSink::new(output_format, final_path, &schema, metadata)?;
        // Fixed, fully-known value sets — built once and reused for every
        // batch's `DictionaryArray` (see `pipeline::DictValues`'s doc comment
        // for why a *shared* dictionary is required for IPC output).
        let match_type_dict = crate::pipeline::DictValues::new([
            "hard_neg",
            "canary",
            "exact_dup",
            "fuzzy_dup",
            "unique",
        ]);
        let difficulty_dict = crate::pipeline::DictValues::new([difficulty.to_string()]);
        Ok(Self {
            sink,
            schema,
            k_hard_neg: match_type_dict.key("hard_neg"),
            k_canary: match_type_dict.key("canary"),
            k_exact_dup: match_type_dict.key("exact_dup"),
            k_fuzzy_dup: match_type_dict.key("fuzzy_dup"),
            k_unique: match_type_dict.key("unique"),
            match_type_dict,
            difficulty_dict,
            difficulty: difficulty.to_string(),
            diff_arr_cache: None,
            n_exact_dup: 0,
            n_fuzzy_dup: 0,
            n_hard_neg: 0,
            n_unique: 0,
            n_masters: 0,
            track_clusters,
            cluster_pairs: Vec::new(),
            is_dup: LocalBits::default(),
            has_exact_copy: LocalBits::default(),
        })
    }

    fn write(
        &mut self,
        record_ids: &ArrayRef,
        master_ids: &ArrayRef,
        entity_types: &ArrayRef,
        match_type_keys: Int32Array,
    ) -> Result<(), String> {
        let n = record_ids.len();
        let diff_arr = match &self.diff_arr_cache {
            Some((cached_n, arr)) if *cached_n == n => arr.clone(),
            _ => {
                let arr = self.difficulty_dict.const_array(&self.difficulty, n);
                self.diff_arr_cache = Some((n, arr.clone()));
                arr
            }
        };
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                record_ids.clone(),
                master_ids.clone(),
                entity_types.clone(),
                self.match_type_dict.finish_keys(match_type_keys),
                diff_arr,
            ],
        )
        .map_err(|e| format!("build gt batch: {e}"))?;
        self.sink.write(&batch)
    }

    /// Classifies and writes one entity batch: its base rows first, then its
    /// duplicate copies (`dups`, `None` when the batch has none) — the same
    /// order they appear in the dataset.
    ///
    /// - A base row is `unique` if no copy of its master exists, otherwise
    ///   `exact_dup` if at least one copy is byte-identical to it (a base row
    ///   has no noise of its own: it reads as an exact duplicate only if the
    ///   cluster genuinely contains an identical twin), else `fuzzy_dup`.
    /// - A copy is `exact_dup` if its noise was a no-op, else `fuzzy_dup`.
    pub fn push_entity_batch(
        &mut self,
        base: &BaseRows,
        dups: Option<&DupRows>,
    ) -> Result<(), String> {
        let n = base.record_ids.len();
        debug_assert_ids(
            base.master_ids,
            base.record_ids,
            base.entity_idx,
            base.first_master_idx,
            base.first_record_idx,
        );
        self.n_masters += n;
        self.is_dup.reset(n);
        self.has_exact_copy.reset(n);
        if let Some(d) = dups {
            if d.master_idx.len() != d.record_ids.len()
                || d.is_identical.len() != d.record_ids.len()
            {
                return Err(format!(
                    "gt: duplicate batch length mismatch (rows={}, master_idx={}, is_identical={})",
                    d.record_ids.len(),
                    d.master_idx.len(),
                    d.is_identical.len()
                ));
            }
            for (i, (&g, &ident)) in d.master_idx.iter().zip(d.is_identical).enumerate() {
                let local = (g as u64)
                    .checked_sub(base.first_master_idx)
                    .filter(|&l| l < n as u64)
                    .ok_or_else(|| {
                        format!(
                            "gt: duplicate row {i} references master {g}, outside its base \
                             batch [{}, {}) — duplicates must come from their own batch",
                            base.first_master_idx,
                            base.first_master_idx + n as u64
                        )
                    })? as usize;
                self.is_dup.set(local);
                if ident {
                    self.has_exact_copy.set(local);
                }
            }
        }

        let mut keys = Int32Builder::with_capacity(n);
        for j in 0..n {
            let key = if !self.is_dup.get(j) {
                self.n_unique += 1;
                self.k_unique
            } else {
                let ident = self.has_exact_copy.get(j);
                if self.track_clusters {
                    self.cluster_pairs.push((
                        crate::pipeline::pack_master_parts(
                            base.entity_idx,
                            base.first_master_idx + j as u64,
                        ),
                        pack_ridx_identical(base.first_record_idx + j as u64, ident),
                    ));
                }
                if ident {
                    self.n_exact_dup += 1;
                    self.k_exact_dup
                } else {
                    self.n_fuzzy_dup += 1;
                    self.k_fuzzy_dup
                }
            };
            keys.append_value(key);
        }
        self.write(
            base.record_ids,
            base.master_ids,
            base.entity_types,
            keys.finish(),
        )?;

        if let Some(d) = dups {
            let nd = d.record_ids.len();
            if nd > 0 {
                debug_assert_ids(
                    d.master_ids,
                    d.record_ids,
                    base.entity_idx,
                    d.master_idx[0] as u64,
                    d.first_record_idx,
                );
            }
            let mut keys = Int32Builder::with_capacity(nd);
            for (i, (&g, &ident)) in d.master_idx.iter().zip(d.is_identical).enumerate() {
                if self.track_clusters {
                    self.cluster_pairs.push((
                        crate::pipeline::pack_master_parts(base.entity_idx, g as u64),
                        pack_ridx_identical(d.first_record_idx + i as u64, ident),
                    ));
                }
                keys.append_value(if ident {
                    self.n_exact_dup += 1;
                    self.k_exact_dup
                } else {
                    self.n_fuzzy_dup += 1;
                    self.k_fuzzy_dup
                });
            }
            self.write(d.record_ids, d.master_ids, d.entity_types, keys.finish())?;
        }
        Ok(())
    }

    /// Writes a batch of hard-negative rows (`HN-…` masters): always
    /// `hard_neg`, never part of a duplicate cluster.
    pub fn push_hard_neg_batch(
        &mut self,
        record_ids: &ArrayRef,
        entity_types: &ArrayRef,
        master_ids: &ArrayRef,
    ) -> Result<(), String> {
        debug_assert_prefix(master_ids, "HN-");
        let n = record_ids.len();
        self.n_hard_neg += n;
        let keys = Int32Array::from(vec![self.k_hard_neg; n]);
        self.write(record_ids, master_ids, entity_types, keys)
    }

    /// Writes a batch of canary rows (`CANARY-…` masters, see `canary.rs`):
    /// always `canary`, not counted in any statistic.
    pub fn push_canary_batch(
        &mut self,
        record_ids: &ArrayRef,
        entity_types: &ArrayRef,
        master_ids: &ArrayRef,
    ) -> Result<(), String> {
        debug_assert_prefix(master_ids, "CANARY-");
        let keys = Int32Array::from(vec![self.k_canary; record_ids.len()]);
        self.write(record_ids, master_ids, entity_types, keys)
    }

    /// Closes the ground-truth file and returns the run's statistics (plus
    /// the duplicate-cluster map, when tracked).
    pub fn finish(self) -> Result<GtResult, String> {
        self.sink.finish()?;
        Ok(GtResult {
            n_exact_dup: self.n_exact_dup,
            n_fuzzy_dup: self.n_fuzzy_dup,
            n_hard_neg: self.n_hard_neg,
            n_unique: self.n_unique,
            n_masters: self.n_masters,
            cluster_map: ClusterCsr::build(self.cluster_pairs),
        })
    }
}

/// Debug-only guard that the numeric ids handed to `push_entity_batch` are
/// the ones actually formatted into the first row's `master_id`/`record_id`
/// strings — the classification and `cluster_map` keys rely on that
/// correspondence instead of re-parsing every string.
fn debug_assert_ids(
    master_ids: &ArrayRef,
    record_ids: &ArrayRef,
    entity_idx: u64,
    master_idx: u64,
    record_idx: u64,
) {
    if cfg!(debug_assertions) && !master_ids.is_empty() {
        let mid = master_ids.as_string::<i32>().value(0);
        let rid = record_ids.as_string::<i32>().value(0);
        assert_eq!(
            crate::pipeline::pack_master_key(mid),
            Some(crate::pipeline::pack_master_parts(entity_idx, master_idx)),
            "gt: master_id {mid:?} doesn't match entity {entity_idx} / master {master_idx}"
        );
        assert_eq!(
            crate::pipeline::parse_record_idx(rid),
            Some(record_idx),
            "gt: record_id {rid:?} doesn't match record index {record_idx}"
        );
    }
}

fn debug_assert_prefix(master_ids: &ArrayRef, prefix: &str) {
    if cfg!(debug_assertions) && !master_ids.is_empty() {
        let mid = master_ids.as_string::<i32>().value(0);
        assert!(
            mid.starts_with(prefix),
            "gt: master_id {mid:?} should start with {prefix:?}"
        );
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

    fn arr_owned(values: Vec<String>) -> ArrayRef {
        Arc::new(StringArray::from(values))
    }

    /// `entity_type` fixtures: the real column is `Dictionary(Int32, Utf8)`,
    /// built from `add_metadata_and_align`'s `entity_type_dict`. `dict` must
    /// be the SAME `DictValues` instance across every `dict_arr` call feeding
    /// one file — a fresh dictionary per call is exactly the "Dictionary
    /// replacement" error IPC raises in the real pipeline too.
    fn dict_arr(dict: &crate::pipeline::DictValues, value: &str, n: usize) -> ArrayRef {
        dict.const_array(value, n)
    }

    /// Reads the GT file into a `record_id -> match_type` map.
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
            // `match_type` is `Dictionary(Int32, Utf8)` — cast back to plain
            // `Utf8` for this test-only comparison.
            let mt_col =
                arrow::compute::cast(batch.column_by_name("match_type").unwrap(), &DataType::Utf8)
                    .unwrap();
            let mt = mt_col.as_string::<i32>();
            for i in 0..batch.num_rows() {
                out.insert(rid.value(i).to_string(), mt.value(i).to_string());
            }
        }
        out
    }

    /// Fixed-width master_id of `entity`'s global master `n`, as the
    /// pipeline formats it.
    fn mid(entity: usize, n: usize) -> String {
        format!(
            "{}-{}",
            crate::pipeline::entity_prefix(entity),
            crate::pipeline::pad_string(n)
        )
    }

    fn rid(i: usize) -> String {
        crate::pipeline::record_id_string(i)
    }

    /// Contiguous base batch fixture: masters `first_master..first_master+n`
    /// of `entity`, records `first_rid..first_rid+n`.
    struct Base {
        rids: ArrayRef,
        ets: ArrayRef,
        mids: ArrayRef,
        entity: u64,
        first_master: u64,
        first_rid: u64,
    }

    impl Base {
        fn new(
            et: &crate::pipeline::DictValues,
            name: &str,
            entity: usize,
            first_master: usize,
            first_rid: usize,
            n: usize,
        ) -> Self {
            Self {
                rids: arr_owned((first_rid..first_rid + n).map(rid).collect()),
                ets: dict_arr(et, name, n),
                mids: arr_owned(
                    (first_master..first_master + n)
                        .map(|m| mid(entity, m))
                        .collect(),
                ),
                entity: entity as u64,
                first_master: first_master as u64,
                first_rid: first_rid as u64,
            }
        }

        fn rows(&self) -> BaseRows<'_> {
            BaseRows {
                record_ids: &self.rids,
                entity_types: &self.ets,
                master_ids: &self.mids,
                entity_idx: self.entity,
                first_master_idx: self.first_master,
                first_record_idx: self.first_rid,
            }
        }
    }

    /// Duplicate-copies fixture: one copy per `(master, is_identical)`,
    /// records contiguous from `first_rid`.
    struct Dups {
        rids: ArrayRef,
        ets: ArrayRef,
        mids: ArrayRef,
        master_idx: Vec<usize>,
        ident: Vec<bool>,
        first_rid: u64,
    }

    impl Dups {
        fn new(
            et: &crate::pipeline::DictValues,
            name: &str,
            entity: usize,
            first_rid: usize,
            copies: &[(usize, bool)],
        ) -> Self {
            let n = copies.len();
            Self {
                rids: arr_owned((first_rid..first_rid + n).map(rid).collect()),
                ets: dict_arr(et, name, n),
                mids: arr_owned(copies.iter().map(|&(m, _)| mid(entity, m)).collect()),
                master_idx: copies.iter().map(|&(m, _)| m).collect(),
                ident: copies.iter().map(|&(_, i)| i).collect(),
                first_rid: first_rid as u64,
            }
        }

        fn rows(&self) -> DupRows<'_> {
            DupRows {
                record_ids: &self.rids,
                entity_types: &self.ets,
                master_ids: &self.mids,
                master_idx: &self.master_idx,
                is_identical: &self.ident,
                first_record_idx: self.first_rid,
            }
        }
    }

    fn hn(et: &crate::pipeline::DictValues, first_rid: usize, n: usize) -> [ArrayRef; 3] {
        [
            arr_owned((first_rid..first_rid + n).map(rid).collect()),
            dict_arr(et, "person", n),
            arr_owned((0..n).map(|i| format!("HN-{i:09}")).collect()),
        ]
    }

    fn acc(path: &str, track_clusters: bool) -> GtAccumulator {
        GtAccumulator::new("medium", "ipc", path, &HashMap::new(), track_clusters).unwrap()
    }

    #[test]
    fn test_gt_accumulator_basic() {
        let path = tmp_path("basic");
        let et = crate::pipeline::DictValues::new(["person"]);
        let mut acc = acc(&path, false);
        // Masters 0..3 (records 0..3); master 0 gets one unchanged copy
        // (record 3); then 2 hard negatives (records 4, 5).
        let base = Base::new(&et, "person", 0, 0, 0, 3);
        let dups = Dups::new(&et, "person", 0, 3, &[(0, true)]);
        acc.push_entity_batch(&base.rows(), Some(&dups.rows()))
            .unwrap();
        let [r, e, m] = hn(&et, 4, 2);
        acc.push_hard_neg_batch(&r, &e, &m).unwrap();

        let res = acc.finish().unwrap();
        assert_eq!(res.n_exact_dup, 2);
        assert_eq!(res.n_fuzzy_dup, 0);
        assert_eq!(res.n_hard_neg, 2);
        assert_eq!(res.n_unique, 2);
        assert_eq!(res.n_masters, 3);

        let mt = read_match_types(&path);
        assert_eq!(mt[&rid(0)], "exact_dup");
        assert_eq!(mt[&rid(1)], "unique");
        assert_eq!(mt[&rid(2)], "unique");
        assert_eq!(mt[&rid(3)], "exact_dup");
        assert_eq!(mt[&rid(4)], "hard_neg");
        assert_eq!(mt[&rid(5)], "hard_neg");
        std::fs::remove_file(&path).ok();
    }

    /// A copy whose noise actually changed something is `fuzzy_dup`; the
    /// master and an unchanged sibling copy stay `exact_dup`, per row.
    #[test]
    fn test_gt_accumulator_fuzzy_dup() {
        let path = tmp_path("fuzzy");
        let et = crate::pipeline::DictValues::new(["person"]);
        let mut acc = acc(&path, false);
        let base = Base::new(&et, "person", 0, 0, 0, 1);
        let dups = Dups::new(&et, "person", 0, 1, &[(0, true), (0, false)]);
        acc.push_entity_batch(&base.rows(), Some(&dups.rows()))
            .unwrap();

        let res = acc.finish().unwrap();
        assert_eq!(res.n_exact_dup, 2); // master + unchanged copy
        assert_eq!(res.n_fuzzy_dup, 1); // genuinely noised copy
        assert_eq!(res.n_unique, 0);
        assert_eq!(res.n_masters, 1);

        let mt = read_match_types(&path);
        assert_eq!(mt[&rid(0)], "exact_dup");
        assert_eq!(mt[&rid(1)], "exact_dup");
        assert_eq!(mt[&rid(2)], "fuzzy_dup");
        std::fs::remove_file(&path).ok();
    }

    /// A master whose only copies were all genuinely noised has no
    /// byte-identical twin: its base row is `fuzzy_dup`, not `exact_dup`.
    #[test]
    fn test_base_row_without_identical_copy_is_fuzzy() {
        let path = tmp_path("base_fuzzy");
        let et = crate::pipeline::DictValues::new(["person"]);
        let mut acc = acc(&path, false);
        let base = Base::new(&et, "person", 0, 0, 0, 1);
        let dups = Dups::new(&et, "person", 0, 1, &[(0, false), (0, false)]);
        acc.push_entity_batch(&base.rows(), Some(&dups.rows()))
            .unwrap();

        let res = acc.finish().unwrap();
        assert_eq!(res.n_exact_dup, 0);
        assert_eq!(res.n_fuzzy_dup, 3);
        assert_eq!(read_match_types(&path)[&rid(0)], "fuzzy_dup");
        std::fs::remove_file(&path).ok();
    }

    /// Several batches, offset masters/records, some without any duplicate:
    /// classification only depends on each batch's own copies.
    #[test]
    fn test_gt_accumulator_multi_batch() {
        let path = tmp_path("multi");
        let et = crate::pipeline::DictValues::new(["person"]);
        let mut acc = acc(&path, false);
        // Batch 1: masters 0..2 (records 0..2), master 1 copied (record 2).
        let b1 = Base::new(&et, "person", 0, 0, 0, 2);
        let d1 = Dups::new(&et, "person", 0, 2, &[(1, false)]);
        acc.push_entity_batch(&b1.rows(), Some(&d1.rows())).unwrap();
        // Batch 2: masters 2..4 (records 3..5), no duplicates at all.
        let b2 = Base::new(&et, "person", 0, 2, 3, 2);
        acc.push_entity_batch(&b2.rows(), None).unwrap();

        let res = acc.finish().unwrap();
        assert_eq!(res.n_masters, 4);
        assert_eq!(res.n_unique, 3);
        assert_eq!(res.n_fuzzy_dup, 2);
        let mt = read_match_types(&path);
        assert_eq!(mt[&rid(0)], "unique");
        assert_eq!(mt[&rid(1)], "fuzzy_dup");
        assert_eq!(mt[&rid(2)], "fuzzy_dup");
        assert_eq!(mt[&rid(3)], "unique");
        assert_eq!(mt[&rid(4)], "unique");
        std::fs::remove_file(&path).ok();
    }

    /// Two entities share the same local master index (each restarts at
    /// 0): duplicating one must not mark the other as duplicated, and their
    /// clusters must stay separate.
    #[test]
    fn test_no_cross_entity_suffix_collision() {
        let path = tmp_path("suffix");
        let et = crate::pipeline::DictValues::new(["person", "account"]);
        let mut acc = acc(&path, true);
        let person = Base::new(&et, "person", 0, 0, 0, 1);
        let person_dups = Dups::new(&et, "person", 0, 1, &[(0, true)]);
        acc.push_entity_batch(&person.rows(), Some(&person_dups.rows()))
            .unwrap();
        let account = Base::new(&et, "account", 1, 0, 2, 1);
        acc.push_entity_batch(&account.rows(), None).unwrap();

        let res = acc.finish().unwrap();
        assert_eq!(res.n_exact_dup, 2);
        assert_eq!(res.n_unique, 1);
        assert_eq!(res.n_masters, 2);
        assert_eq!(res.cluster_map.n_clusters(), 1);
        assert_eq!(read_match_types(&path)[&rid(2)], "unique");
        std::fs::remove_file(&path).ok();
    }

    /// A duplicate referencing a master outside its own base batch breaks
    /// the single-pass invariant — rejected loudly instead of misclassified.
    #[test]
    fn test_duplicate_outside_its_batch_is_rejected() {
        let path = tmp_path("outside");
        let et = crate::pipeline::DictValues::new(["person"]);
        let mut acc = acc(&path, false);
        let base = Base::new(&et, "person", 0, 10, 0, 2);
        let mut dups = Dups::new(&et, "person", 0, 2, &[(10, true)]);
        dups.master_idx = vec![12]; // batch covers masters 10..12
        let err = acc
            .push_entity_batch(&base.rows(), Some(&dups.rows()))
            .unwrap_err();
        assert!(err.contains("outside its base batch"), "{err}");
        drop(acc);
        std::fs::remove_file(&path).ok();
    }

    /// `cluster_map` holds every duplicated master's members (base + copies)
    /// with their own identical/fuzzy status, in ascending master order;
    /// singletons and hard negatives don't appear.
    #[test]
    fn test_cluster_map_contents() {
        let path = tmp_path("cm");
        let et = crate::pipeline::DictValues::new(["person"]);
        let mut acc = acc(&path, true);
        // Masters 0..3 (records 0..3). Master 0: one unchanged copy (record
        // 3). Master 2: one genuinely noised copy (record 4).
        let base = Base::new(&et, "person", 0, 0, 0, 3);
        let dups = Dups::new(&et, "person", 0, 3, &[(2, false), (0, true)]);
        acc.push_entity_batch(&base.rows(), Some(&dups.rows()))
            .unwrap();
        let [r, e, m] = hn(&et, 5, 2);
        acc.push_hard_neg_batch(&r, &e, &m).unwrap();

        let cm = acc.finish().unwrap().cluster_map;
        assert_eq!(cm.n_clusters(), 2);
        let mut groups = cm.groups();
        let (records0, idents0) = groups.next().unwrap();
        assert_eq!(records0, &[0, 4]);
        assert_eq!(idents0.to_vec(), vec![true, true]);
        // Master 2's only copy was noised: no byte-identical pair, so its
        // base row must not read as identical either.
        let (records2, idents2) = groups.next().unwrap();
        assert_eq!(records2, &[2, 3]);
        assert_eq!(idents2.to_vec(), vec![false, false]);
        assert!(groups.next().is_none());
        std::fs::remove_file(&path).ok();
    }
}
