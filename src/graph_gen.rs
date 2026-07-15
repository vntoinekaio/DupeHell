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
        self.src_buf = StringBuilder::new();
        self.tgt_buf = StringBuilder::new();
        self.etype_buf = StringBuilder::new();
        self.subtype_buf = StringBuilder::new();
        self.weight_buf = Float64Builder::new();
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

/// Emit duplicate-cluster edges. For a cluster of size `k`, emit the full
/// `k(k-1)/2` complete graph unless it exceeds `max_edges`, in which case a
/// deterministic spanning tree (sorted order) is emitted instead.
///
/// Wired into `run_pipeline` in a later phase (post-GT `cluster_map`).
#[allow(dead_code)]
pub fn push_dup_clusters(
    ew: &mut EdgeWriter,
    clusters: &HashMap<String, Vec<String>>,
    max_edges: usize,
) -> Result<(), String> {
    for record_ids in clusters.values() {
        let k = record_ids.len();
        if k < 2 {
            continue;
        }
        let n_edges = k * (k - 1) / 2;
        let mut sorted = record_ids.clone();
        sorted.sort();

        if n_edges > max_edges {
            log::warn!(
                "dup cluster has {n_edges} edges > {max_edges}, using spanning tree fallback"
            );
            for w in sorted.windows(2) {
                ew.push(&w[0], &w[1], "exact_dup", "spanning_tree", 1.0)?;
            }
        } else {
            for i in 0..k {
                for j in (i + 1)..k {
                    ew.push(&sorted[i], &sorted[j], "exact_dup", "complete", 1.0)?;
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

    #[test]
    fn push_dup_clusters_complete() {
        let path = temp_path("edges_complete.ipc");
        let _ = std::fs::remove_file(&path);
        let mut ew = EdgeWriter::new(&path, &HashMap::new()).unwrap();
        let mut clusters = HashMap::new();
        clusters.insert(
            "M1".to_string(),
            vec!["R1".into(), "R2".into(), "R3".into(), "R4".into()],
        );
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
    fn push_dup_clusters_spanning_tree() {
        let path = temp_path("edges_spanning.ipc");
        let _ = std::fs::remove_file(&path);
        let mut ew = EdgeWriter::new(&path, &HashMap::new()).unwrap();
        let mut clusters = HashMap::new();
        let ids: Vec<String> = (0..200).map(|i| format!("R{i:04}")).collect();
        clusters.insert("M-BIG".to_string(), ids);
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
        // Adjacent sorted pairs only.
        let got: HashSet<(String, String)> =
            edges.iter().map(|e| (e.0.clone(), e.1.clone())).collect();
        let mut sorted = clusters["M-BIG"].clone();
        sorted.sort();
        for w in sorted.windows(2) {
            assert!(got.contains(&(w[0].clone(), w[1].clone())));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn push_dup_clusters_skips_singletons() {
        let path = temp_path("edges_singleton.ipc");
        let _ = std::fs::remove_file(&path);
        let mut ew = EdgeWriter::new(&path, &HashMap::new()).unwrap();
        let mut clusters = HashMap::new();
        clusters.insert("M-ONLY".to_string(), vec!["R1".into()]);
        push_dup_clusters(&mut ew, &clusters, 10_000).unwrap();
        ew.finish().unwrap();
        assert!(read_edges(&path).is_empty());
        let _ = std::fs::remove_file(&path);
    }
}
