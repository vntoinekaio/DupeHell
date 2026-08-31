// Isolated micro-benchmark for hunt3108_graph/H1: quantifies the cost of
// writing every dataset batch a second time to the `_nodes` IPC file
// (`graph_gen::NodeWriter::write_batch`, called right after
// `writer.write(&base_rb)` in `pipeline.rs`, strictly sequential today), and
// checks how much of that cost a background-thread overlap could recover.
//
// Doesn't touch crate-private code (`graph_gen` isn't `pub mod`) -- rebuilds
// the same shape of work directly against `arrow::ipc::writer::FileWriter`,
// which is exactly what `NodeWriter` is a thin wrapper over. The synthetic
// schemas below mirror the two real domains measured in the parent hunt:
// `aviation` (narrow, ~9 cols, has fk_remaps) and `kyc` (wide, 23 cols, zero
// fk_remaps) -- since kyc's measured overhead (+71-98%) already excludes any
// FK-edge cost, comparing the two isolates the node-duplication cost alone.
//
// Not wired into the crate's normal build -- run explicitly:
//   cargo run --release --example profile_node_writer

use std::fs::File;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Instant;

use arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;

const BATCH_SIZE: usize = 500_000;
const N_BATCHES: usize = 10; // 5M rows total, matches one of the measured tiers

fn make_schema(n_cols: usize) -> Arc<Schema> {
    let mut fields = vec![Field::new("record_id", DataType::Utf8, false)];
    for i in 0..n_cols {
        fields.push(match i % 3 {
            0 => Field::new(format!("str_col_{i}"), DataType::Utf8, false),
            1 => Field::new(format!("int_col_{i}"), DataType::Int32, false),
            _ => Field::new(format!("f64_col_{i}"), DataType::Float64, false),
        });
    }
    Arc::new(Schema::new(fields))
}

fn make_batch(schema: &Arc<Schema>, n_cols: usize, batch_idx: usize) -> RecordBatch {
    let ids: StringArray = (0..BATCH_SIZE)
        .map(|i| Some(format!("R-{:013}", batch_idx * BATCH_SIZE + i)))
        .collect();
    let mut cols: Vec<ArrayRef> = vec![Arc::new(ids)];
    for i in 0..n_cols {
        let col: ArrayRef = match i % 3 {
            0 => Arc::new(
                (0..BATCH_SIZE)
                    .map(|r| Some(format!("value-{r}-{i}")))
                    .collect::<StringArray>(),
            ),
            1 => Arc::new(Int32Array::from_iter_values(
                (0..BATCH_SIZE).map(|r| (r % 9973) as i32),
            )),
            _ => Arc::new(Float64Array::from_iter_values(
                (0..BATCH_SIZE).map(|r| (r as f64) * 1.0001),
            )),
        };
        cols.push(col);
    }
    RecordBatch::try_new(schema.clone(), cols).unwrap()
}

fn node_schema(dataset_schema: &Schema) -> Arc<Schema> {
    let fields: Vec<Field> = dataset_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            if i == 0 {
                Field::new("node_id", f.data_type().clone(), f.is_nullable())
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Baseline: write only the "dataset" file, N batches.
fn bench_dataset_only(schema: &Arc<Schema>, batches: &[RecordBatch], path: &str) -> f64 {
    let file = File::create(path).unwrap();
    let mut writer = FileWriter::try_new(file, schema).unwrap();
    let t0 = Instant::now();
    for b in batches {
        writer.write(b).unwrap();
    }
    writer.finish().unwrap();
    t0.elapsed().as_secs_f64()
}

/// Current behavior: node writer + dataset writer, strictly sequential in
/// the same thread (mirrors pipeline.rs:1518+1537 call order exactly).
fn bench_sequential_double(
    schema: &Arc<Schema>,
    node_schema: &Arc<Schema>,
    batches: &[RecordBatch],
    dataset_path: &str,
    nodes_path: &str,
) -> f64 {
    let df = File::create(dataset_path).unwrap();
    let mut dw = FileWriter::try_new(df, schema).unwrap();
    let nf = File::create(nodes_path).unwrap();
    let mut nw = FileWriter::try_new(nf, node_schema).unwrap();

    let t0 = Instant::now();
    for b in batches {
        // Same column data, just a schema with column 0 renamed -- exactly
        // what NodeWriter::write_batch does (rebuild via try_new, same
        // ArrayRefs, no data copy at the Arc level).
        let node_rb = RecordBatch::try_new(node_schema.clone(), b.columns().to_vec()).unwrap();
        nw.write(&node_rb).unwrap();
        dw.write(b).unwrap();
    }
    dw.finish().unwrap();
    nw.finish().unwrap();
    t0.elapsed().as_secs_f64()
}

/// Proposed lever: node writer runs on a dedicated thread, fed via channel,
/// overlapping with the dataset writer instead of blocking it.
fn bench_threaded_overlap(
    schema: &Arc<Schema>,
    node_schema: &Arc<Schema>,
    batches: &[RecordBatch],
    dataset_path: &str,
    nodes_path: &str,
) -> f64 {
    let df = File::create(dataset_path).unwrap();
    let mut dw = FileWriter::try_new(df, schema).unwrap();
    let nf = File::create(nodes_path).unwrap();
    let mut nw = FileWriter::try_new(nf, node_schema).unwrap();
    let node_schema2 = node_schema.clone();

    let (tx, rx) = mpsc::channel::<RecordBatch>();
    let node_thread = std::thread::spawn(move || {
        for rb in rx {
            nw.write(&rb).unwrap();
        }
        nw.finish().unwrap();
    });

    let t0 = Instant::now();
    for b in batches {
        let node_rb = RecordBatch::try_new(node_schema2.clone(), b.columns().to_vec()).unwrap();
        tx.send(node_rb).unwrap();
        dw.write(b).unwrap();
    }
    drop(tx);
    dw.finish().unwrap();
    node_thread.join().unwrap();
    t0.elapsed().as_secs_f64()
}

fn run_for(label: &str, n_cols: usize) {
    let schema = make_schema(n_cols);
    let nschema = node_schema(&schema);
    let batches: Vec<RecordBatch> = (0..N_BATCHES)
        .map(|i| make_batch(&schema, n_cols, i))
        .collect();

    let tmp = std::env::temp_dir();
    let p_dataset = tmp.join(format!("dupehell_profile_{label}_dataset.ipc"));
    let p_nodes = tmp.join(format!("dupehell_profile_{label}_nodes.ipc"));
    let p_dataset2 = tmp.join(format!("dupehell_profile_{label}_dataset2.ipc"));
    let p_nodes2 = tmp.join(format!("dupehell_profile_{label}_nodes2.ipc"));
    let p_dataset3 = tmp.join(format!("dupehell_profile_{label}_dataset3.ipc"));

    let baseline_s = bench_dataset_only(&schema, &batches, p_dataset3.to_str().unwrap());
    let sequential_s = bench_sequential_double(
        &schema,
        &nschema,
        &batches,
        p_dataset.to_str().unwrap(),
        p_nodes.to_str().unwrap(),
    );
    let threaded_s = bench_threaded_overlap(
        &schema,
        &nschema,
        &batches,
        p_dataset2.to_str().unwrap(),
        p_nodes2.to_str().unwrap(),
    );

    for p in [&p_dataset, &p_nodes, &p_dataset2, &p_nodes2, &p_dataset3] {
        let _ = std::fs::remove_file(p);
    }

    let overhead_seq = (sequential_s / baseline_s - 1.0) * 100.0;
    let overhead_thr = (threaded_s / baseline_s - 1.0) * 100.0;
    let recovered = if sequential_s > baseline_s {
        (1.0 - (threaded_s - baseline_s) / (sequential_s - baseline_s)) * 100.0
    } else {
        0.0
    };

    println!(
        "{label:10} ({n_cols:2} cols)  baseline={baseline_s:6.3}s  \
         sequential(current)={sequential_s:6.3}s (+{overhead_seq:5.1}%)  \
         threaded(proposed)={threaded_s:6.3}s (+{overhead_thr:5.1}%)  \
         overhead recovered by threading={recovered:5.1}%"
    );
}

fn main() {
    println!(
        "profile_node_writer: {N_BATCHES} batches x {BATCH_SIZE} rows, \
         comparing dataset-only vs current-sequential-double-write vs \
         threaded-overlap for the NodeWriter duplication (hunt3108_graph/H1)\n"
    );
    // aviation-like: narrow schema (9 payload cols + record_id = 10 total,
    // matches aviation's 8-10 cols per entity noted in the hunt).
    run_for("aviation", 9);
    // kyc-like: wide, PII-dense schema (23 cols, matches natural_person).
    run_for("kyc", 23);

    std::io::stdout().flush().ok();
}
