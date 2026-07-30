// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;

/// Output format for the generated property-graph files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GraphFormat {
    Ipc,
    Parquet,
}

impl GraphFormat {
    pub fn from_str(s: &str) -> GraphFormat {
        match s {
            "parquet" => GraphFormat::Parquet,
            _ => GraphFormat::Ipc,
        }
    }
}

/// Writes `_nodes.{ext}` directly to the final file.
///
/// The node schema is the pipeline `full_arc` with column 0 (`record_id`)
/// renamed `node_id`; all other columns are kept positionally identical.
pub struct NodeWriter {
    writer: FileWriter<File>,
    schema: Arc<Schema>,
}

impl NodeWriter {
    /// `path` is the final file (written directly, no draft/rename).
    /// `full_schema` is the pipeline `full_arc` (record_id in column 0).
    /// `metadata` is the `dupehell.*` map copied from the dataset.
    pub fn new(
        path: &str,
        full_schema: &Schema,
        metadata: &HashMap<String, String>,
    ) -> Result<Self, String> {
        let fields: Vec<Field> = full_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == 0 {
                    Field::new("node_id", f.data_type().clone(), f.is_nullable())
                        .with_metadata(f.metadata().clone())
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        let schema = Arc::new(Schema::new(fields).with_metadata(metadata.clone()));

        let file = File::create(path).map_err(|e| format!("create node file {path}: {e}"))?;
        let writer = FileWriter::try_new(file, &schema)
            .map_err(|e| format!("node FileWriter {path}: {e}"))?;
        Ok(NodeWriter { writer, schema })
    }

    /// `batch` is a base/dup/hn/canary record batch in `full_arc` layout
    /// (record_id in column 0). Rebuilt positionally with the node schema
    /// (column 0 renamed `node_id`).
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), String> {
        let rb = RecordBatch::try_new(self.schema.clone(), batch.columns().to_vec())
            .map_err(|e| format!("rebuild node batch: {e}"))?;
        self.writer
            .write(&rb)
            .map_err(|e| format!("write node batch: {e}"))
    }

    pub fn finish(mut self) -> Result<(), String> {
        self.writer
            .finish()
            .map_err(|e| format!("finish node writer: {e}"))
    }
}

fn edge_schema(metadata: &HashMap<String, String>) -> Arc<Schema> {
    Arc::new(
        Schema::new(vec![
            Field::new("source_node_id", DataType::Utf8, false),
            Field::new("target_node_id", DataType::Utf8, false),
            Field::new("edge_type", DataType::Utf8, false),
            Field::new("subtype", DataType::Utf8, false),
            Field::new("weight", DataType::Float64, false),
        ])
        .with_metadata(metadata.clone()),
    )
}

/// Writes `_edges.{ext}` directly; flushes in bounded batches.
pub struct EdgeWriter {
    writer: FileWriter<File>,
    schema: Arc<Schema>,
    src_buf: StringBuilder,
    tgt_buf: StringBuilder,
    etype_buf: StringBuilder,
    subtype_buf: StringBuilder,
    weight_buf: Float64Builder,
    count: usize,
}

const EDGE_FLUSH: usize = 100_000;

impl EdgeWriter {
    pub fn new(path: &str, metadata: &HashMap<String, String>) -> Result<Self, String> {
        let schema = edge_schema(metadata);
        let file = File::create(path).map_err(|e| format!("create edge file {path}: {e}"))?;
        let writer = FileWriter::try_new(file, &schema)
            .map_err(|e| format!("edge FileWriter {path}: {e}"))?;
        Ok(EdgeWriter {
            writer,
            schema,
            src_buf: StringBuilder::new(),
            tgt_buf: StringBuilder::new(),
            etype_buf: StringBuilder::new(),
            subtype_buf: StringBuilder::new(),
            weight_buf: Float64Builder::new(),
            count: 0,
        })
    }

    pub fn push(
        &mut self,
        src: &str,
        tgt: &str,
        etype: &str,
        subtype: &str,
        weight: f64,
    ) -> Result<(), String> {
        self.src_buf.append_value(src);
        self.tgt_buf.append_value(tgt);
        self.etype_buf.append_value(etype);
        self.subtype_buf.append_value(subtype);
        self.weight_buf.append_value(weight);
        self.count += 1;
        if self.count >= EDGE_FLUSH {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        if self.count == 0 {
            return Ok(());
        }
        let rb = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.src_buf.finish()) as ArrayRef,
                Arc::new(self.tgt_buf.finish()) as ArrayRef,
                Arc::new(self.etype_buf.finish()) as ArrayRef,
                Arc::new(self.subtype_buf.finish()) as ArrayRef,
                Arc::new(self.weight_buf.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| format!("build edge batch: {e}"))?;
        self.writer
            .write(&rb)
            .map_err(|e| format!("write edge batch: {e}"))?;
        // `ArrayBuilder::finish` already takes `&mut self` and resets the
        // builder's internal buffers — no need to replace it with a fresh
        // one, that was just throwing away and reallocating empty builders.
        self.count = 0;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), String> {
        self.flush()?;
        self.writer
            .finish()
            .map_err(|e| format!("finish edge writer: {e}"))
    }
}

/// Edge type for a pair of cluster members: `exact_dup` only when *both*
/// ends are byte-for-byte identical to the cluster's master (transitively
/// identical to each other too); `fuzzy_dup` as soon as either end was
/// genuinely noised, since two fuzzy copies (or a fuzzy copy and the master)
/// aren't guaranteed to match each other exactly.
fn pair_edge_type(a_identical: bool, b_identical: bool) -> &'static str {
    if a_identical && b_identical {
        "exact_dup"
    } else {
        "fuzzy_dup"
    }
}

/// Emit duplicate-cluster edges. For a cluster of size `k`, emit the full
/// `k(k-1)/2` complete graph unless it exceeds `max_edges`, in which case a
/// deterministic spanning tree (sorted order) is emitted instead. Each
/// edge's `edge_type` reflects whether both endpoints are genuinely
/// byte-identical (`exact_dup`) or at least one was noised (`fuzzy_dup`) —
/// see `pair_edge_type`.
pub fn push_dup_clusters(
    ew: &mut EdgeWriter,
    clusters: &crate::gt::ClusterCsr,
    max_edges: usize,
) -> Result<(), String> {
    for (records, idents) in clusters.groups() {
        let k = records.len();
        if k < 2 {
            continue;
        }
        // `records` is already ascending within its cluster (guaranteed by
        // `ClusterCsr::build`'s sort), matching the previous
        // `sorted.sort_by(|a, b| a.0.cmp(&b.0))` on the record_id string --
        // no re-sort needed here.
        let n_edges = k * (k - 1) / 2;

        if n_edges > max_edges {
            log::warn!(
                "dup cluster has {n_edges} edges > {max_edges}, using spanning tree fallback"
            );
            for i in 0..k - 1 {
                let etype = pair_edge_type(idents.get(i), idents.get(i + 1));
                let src = crate::pipeline::record_id_string(records[i] as usize);
                let tgt = crate::pipeline::record_id_string(records[i + 1] as usize);
                ew.push(&src, &tgt, etype, "spanning_tree", 1.0)?;
            }
        } else {
            for i in 0..k {
                for j in (i + 1)..k {
                    let etype = pair_edge_type(idents.get(i), idents.get(j));
                    let src = crate::pipeline::record_id_string(records[i] as usize);
                    let tgt = crate::pipeline::record_id_string(records[j] as usize);
                    ew.push(&src, &tgt, etype, "complete", 1.0)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use std::collections::HashSet;

    fn read_edges(path: &str) -> Vec<(String, String, String, String, f64)> {
        let file = File::open(path).unwrap();
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
        let mut out = Vec::new();
        for b in reader {
            let b = b.unwrap();
            let src = b.column(0).as_string::<i32>();
            let tgt = b.column(1).as_string::<i32>();
            let et = b.column(2).as_string::<i32>();
            let st = b.column(3).as_string::<i32>();
            let w = b.column(4).as_primitive::<arrow::datatypes::Float64Type>();
            for i in 0..b.num_rows() {
                out.push((
                    src.value(i).to_string(),
                    tgt.value(i).to_string(),
                    et.value(i).to_string(),
                    st.value(i).to_string(),
                    w.value(i),
                ));
            }
        }
        out
    }

    fn temp_path(name: &str) -> String {
        let mut p = std::env::temp_dir();
        p.push(format!("dupehell_test_{}_{}", name, std::process::id()));
        p.to_string_lossy().to_string()
    }

    /// Alias for `pipeline::record_id_string`, for brevity in fixtures.
    fn rid(i: usize) -> String {
        crate::pipeline::record_id_string(i)
    }

    /// Builds a single-cluster `ClusterCsr` for a test, from a master's
    /// local index (entity index fixed at 0 -- these tests only cover one
    /// entity type) and its `(record_idx, is_identical)` members.
    fn one_cluster(master_local_idx: usize, members: Vec<(usize, bool)>) -> crate::gt::ClusterCsr {
        let master_key = crate::pipeline::pack_master_key(&format!(
            "{}-{}",
            crate::pipeline::entity_prefix(0),
            crate::pipeline::pad_string(master_local_idx)
        ))
        .unwrap();
        let pairs = members
            .into_iter()
            .map(|(ridx, ident)| (master_key, ridx as u64, ident))
            .collect();
        crate::gt::ClusterCsr::build(pairs)
    }

    #[test]
    fn push_dup_clusters_complete() {
        let path = temp_path("edges_complete.ipc");
        let _ = std::fs::remove_file(&path);
        let mut ew = EdgeWriter::new(&path, &HashMap::new()).unwrap();
        let clusters = one_cluster(1, vec![(1, true), (2, true), (3, true), (4, true)]);
        push_dup_clusters(&mut ew, &clusters, 10_000).unwrap();
        ew.finish().unwrap();

        let edges = read_edges(&path);
        assert_eq!(edges.len(), 6, "4 records -> 6 complete edges");
        assert!(
            edges
                .iter()
                .all(|e| e.2 == "exact_dup" && e.3 == "complete")
        );
        assert!(edges.iter().all(|e| (e.4 - 1.0).abs() < 1e-9));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn push_dup_clusters_fuzzy_edge_type() {
        let path = temp_path("edges_fuzzy.ipc");
        let _ = std::fs::remove_file(&path);
        let mut ew = EdgeWriter::new(&path, &HashMap::new()).unwrap();
        // rid(1) (master) and rid(2) stayed identical; rid(3) was genuinely noised.
        let clusters = one_cluster(1, vec![(1, true), (2, true), (3, false)]);
        push_dup_clusters(&mut ew, &clusters, 10_000).unwrap();
        ew.finish().unwrap();

        let edges = read_edges(&path);
        assert_eq!(edges.len(), 3);
        let by_pair: HashMap<(String, String), String> = edges
            .iter()
            .map(|e| ((e.0.clone(), e.1.clone()), e.2.clone()))
            .collect();
        assert_eq!(by_pair[&(rid(1), rid(2))], "exact_dup");
        assert_eq!(by_pair[&(rid(1), rid(3))], "fuzzy_dup");
        assert_eq!(by_pair[&(rid(2), rid(3))], "fuzzy_dup");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn push_dup_clusters_spanning_tree() {
        let path = temp_path("edges_spanning.ipc");
        let _ = std::fs::remove_file(&path);
        let mut ew = EdgeWriter::new(&path, &HashMap::new()).unwrap();
        let members: Vec<(usize, bool)> = (0..200).map(|i| (i, true)).collect();
        let clusters = one_cluster(1, members.clone());
        // 200*199/2 = 19900 edges > 10000 -> spanning tree fallback (199 edges)
        push_dup_clusters(&mut ew, &clusters, 10_000).unwrap();
        ew.finish().unwrap();

        let edges = read_edges(&path);
        assert_eq!(
            edges.len(),
            199,
            "200-record cluster -> 199 spanning-tree edges"
        );
        assert!(
            edges
                .iter()
                .all(|e| e.2 == "exact_dup" && e.3 == "spanning_tree")
        );
        // Adjacent ascending pairs only (members are already 0..200 ascending).
        let got: HashSet<(String, String)> =
            edges.iter().map(|e| (e.0.clone(), e.1.clone())).collect();
        for w in members.windows(2) {
            assert!(got.contains(&(rid(w[0].0), rid(w[1].0))));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn push_dup_clusters_skips_singletons() {
        let path = temp_path("edges_singleton.ipc");
        let _ = std::fs::remove_file(&path);
        let mut ew = EdgeWriter::new(&path, &HashMap::new()).unwrap();
        let clusters = one_cluster(1, vec![(1, true)]);
        push_dup_clusters(&mut ew, &clusters, 10_000).unwrap();
        ew.finish().unwrap();
        assert!(read_edges(&path).is_empty());
        let _ = std::fs::remove_file(&path);
    }
}
