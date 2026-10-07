// Isolated micro-benchmark: quantifies the cost of
// emitting FK edges via `EdgeWriter::push` in a scalar `for i in 0..batch_n`
// loop (the original pipeline.rs pattern, one push() per row per fk_remap) against a
// vectorized alternative that builds whole edge columns at once instead of
// appending value-by-value.
//
// Doesn't touch crate-private code (`graph_gen`/`DictValues` aren't `pub`)
// -- reimplements the same dict-encoding mechanics `DictValues`/`EdgeWriter`
// use (fixed value set, HashMap<String,i32> index, DictionaryArray output)
// directly in this file, scoped to what's needed to compare the two
// push strategies on realistic data shapes.
//
// Workload shape: aviation/hell has 4 fk_remaps and BATCH_SIZE=500_000 --
// this reproduces exactly that (500_000 rows x 4 remaps = 2,000,000 edge
// pushes per batch, 10 batches = one of the measured tiers' ballpark).
//
// Not wired into the crate's normal build -- run explicitly:
//   cargo run --release --example profile_edge_push

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, DictionaryArray, Float64Array, Float64Builder, Int32Array, Int32Builder, StringArray,
    StringBuilder,
};
use arrow::datatypes::Int32Type;

const BATCH_SIZE: usize = 500_000;
const N_REMAPS: usize = 4; // aviation/hell has 4 fk_remaps
const N_BATCHES: usize = 10;
const EDGE_FLUSH: usize = 100_000;

/// Minimal reimplementation of `DictValues`: fixed value set, built once.
struct SimpleDict {
    index: HashMap<String, i32>,
    values: ArrayRef,
}
impl SimpleDict {
    fn new(vals: &[&str]) -> Self {
        let index = vals
            .iter()
            .enumerate()
            .map(|(i, v)| (v.to_string(), i as i32))
            .collect();
        let values: ArrayRef = Arc::new(StringArray::from(vals.to_vec()));
        SimpleDict { index, values }
    }
    fn key(&self, v: &str) -> i32 {
        self.index[v]
    }
    fn finish_keys(&self, keys: Int32Array) -> ArrayRef {
        Arc::new(DictionaryArray::<Int32Type>::new(keys, self.values.clone()))
    }
}

/// Mirrors `EdgeWriter`'s scalar push path exactly (graph_gen.rs:160-179):
/// one `append_value` per field per edge, flushed every EDGE_FLUSH.
struct ScalarEdgeSink {
    etype_dict: SimpleDict,
    subtype_dict: SimpleDict,
    src_buf: StringBuilder,
    tgt_buf: StringBuilder,
    etype_keys: Int32Builder,
    subtype_keys: Int32Builder,
    weight_buf: Float64Builder,
    count: usize,
    flushed_batches: usize,
}
impl ScalarEdgeSink {
    fn new(subtypes: &[&str]) -> Self {
        ScalarEdgeSink {
            etype_dict: SimpleDict::new(&["fk", "hard_neg", "exact_dup", "fuzzy_dup"]),
            subtype_dict: SimpleDict::new(subtypes),
            src_buf: StringBuilder::new(),
            tgt_buf: StringBuilder::new(),
            etype_keys: Int32Builder::new(),
            subtype_keys: Int32Builder::new(),
            weight_buf: Float64Builder::new(),
            count: 0,
            flushed_batches: 0,
        }
    }
    fn push(&mut self, src: &str, tgt: &str, etype: &str, subtype: &str, weight: f64) {
        self.src_buf.append_value(src);
        self.tgt_buf.append_value(tgt);
        self.etype_keys.append_value(self.etype_dict.key(etype));
        self.subtype_keys
            .append_value(self.subtype_dict.key(subtype));
        self.weight_buf.append_value(weight);
        self.count += 1;
        if self.count >= EDGE_FLUSH {
            self.flush();
        }
    }
    fn flush(&mut self) {
        if self.count == 0 {
            return;
        }
        let _src = Arc::new(self.src_buf.finish()) as ArrayRef;
        let _tgt = Arc::new(self.tgt_buf.finish()) as ArrayRef;
        let _et = self.etype_dict.finish_keys(self.etype_keys.finish());
        let _st = self.subtype_dict.finish_keys(self.subtype_keys.finish());
        let _w = Arc::new(self.weight_buf.finish()) as ArrayRef;
        self.count = 0;
        self.flushed_batches += 1;
        std::hint::black_box((&_src, &_tgt, &_et, &_st, &_w));
    }
}

/// Vectorized alternative: build one edge-batch worth of columns per
/// fk_remap directly from the already-materialized `src_arr`/`tgt_arr`
/// (real code already holds these as full StringArrays at the call site --
/// `rid_str_arr` and `target_rids`, pipeline.rs:1523/1526 -- so reusing them
/// via Arc is zero-copy, vs. re-extracting `.value(i)` and re-appending
/// through a StringBuilder scalar-by-scalar). `etype`/`subtype`/`weight` are
/// constant for an entire fk_remap call, so they're built as a single
/// dictionary-key-broadcast + a constant-filled Float64Array instead of one
/// HashMap lookup and one Vec push per row.
struct VectorizedEdgeSink {
    etype_dict: SimpleDict,
    subtype_dict: SimpleDict,
    flushed_batches: usize,
}
impl VectorizedEdgeSink {
    fn new(subtypes: &[&str]) -> Self {
        VectorizedEdgeSink {
            etype_dict: SimpleDict::new(&["fk", "hard_neg", "exact_dup", "fuzzy_dup"]),
            subtype_dict: SimpleDict::new(subtypes),
            flushed_batches: 0,
        }
    }
    /// Pushes a whole fk_remap's worth of edges (one column of `src`, one of
    /// `tgt`, both already-built StringArrays) in one shot.
    fn push_batch(
        &mut self,
        src_arr: &ArrayRef,
        tgt_arr: &ArrayRef,
        etype: &str,
        subtype: &str,
        weight: f64,
    ) {
        let n = src_arr.len();
        let et_key = self.etype_dict.key(etype);
        let st_key = self.subtype_dict.key(subtype);
        let et_keys = Int32Array::from(vec![et_key; n]);
        let st_keys = Int32Array::from(vec![st_key; n]);
        let _et = self.etype_dict.finish_keys(et_keys);
        let _st = self.subtype_dict.finish_keys(st_keys);
        let _w: ArrayRef = Arc::new(Float64Array::from(vec![weight; n]));
        // Zero-copy reuse -- no re-encoding of already-materialized strings.
        let _src = src_arr.clone();
        let _tgt = tgt_arr.clone();
        self.flushed_batches += 1;
        std::hint::black_box((&_src, &_tgt, &_et, &_st, &_w));
    }
}

fn make_rid_array(offset: usize, n: usize) -> ArrayRef {
    Arc::new(
        (0..n)
            .map(|i| Some(format!("R-{:013}", offset + i)))
            .collect::<StringArray>(),
    )
}

fn bench_scalar() -> f64 {
    let subtypes: Vec<&str> = vec!["airline_id", "aircraft_id", "flight_id", "passenger_id"];
    let mut sink = ScalarEdgeSink::new(&subtypes);
    let batches: Vec<(ArrayRef, Vec<ArrayRef>)> = (0..N_BATCHES)
        .map(|b| {
            let rid = make_rid_array(b * BATCH_SIZE, BATCH_SIZE);
            let targets: Vec<ArrayRef> = (0..N_REMAPS)
                .map(|r| make_rid_array(b * BATCH_SIZE + r * 7_919, BATCH_SIZE))
                .collect();
            (rid, targets)
        })
        .collect();

    let t0 = Instant::now();
    for (rid, targets) in &batches {
        let rid_str = rid.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..BATCH_SIZE {
            let src = rid_str.value(i);
            for (r, tgt_arr) in targets.iter().enumerate() {
                let tgt_str = tgt_arr.as_any().downcast_ref::<StringArray>().unwrap();
                sink.push(src, tgt_str.value(i), "fk", subtypes[r], 1.0);
            }
        }
    }
    sink.flush();
    let elapsed = t0.elapsed().as_secs_f64();
    println!("  (scalar: {} flushed edge-batches)", sink.flushed_batches);
    elapsed
}

fn bench_vectorized() -> f64 {
    let subtypes: Vec<&str> = vec!["airline_id", "aircraft_id", "flight_id", "passenger_id"];
    let mut sink = VectorizedEdgeSink::new(&subtypes);
    let batches: Vec<(ArrayRef, Vec<ArrayRef>)> = (0..N_BATCHES)
        .map(|b| {
            let rid = make_rid_array(b * BATCH_SIZE, BATCH_SIZE);
            let targets: Vec<ArrayRef> = (0..N_REMAPS)
                .map(|r| make_rid_array(b * BATCH_SIZE + r * 7_919, BATCH_SIZE))
                .collect();
            (rid, targets)
        })
        .collect();

    let t0 = Instant::now();
    for (rid, targets) in &batches {
        for (r, tgt_arr) in targets.iter().enumerate() {
            sink.push_batch(rid, tgt_arr, "fk", subtypes[r], 1.0);
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    println!(
        "  (vectorized: {} flushed edge-batches)",
        sink.flushed_batches
    );
    elapsed
}

fn main() {
    println!(
        "profile_edge_push: {N_BATCHES} batches x {BATCH_SIZE} rows x {N_REMAPS} fk_remaps \
         (= {} total edge pushes), comparing scalar row-by-row EdgeWriter::push \
         (original pipeline.rs behavior) vs the vectorized column-batch version \
         now in use\n",
        N_BATCHES * BATCH_SIZE * N_REMAPS
    );

    let scalar_s = bench_scalar();
    let vectorized_s = bench_vectorized();
    let speedup = scalar_s / vectorized_s;

    println!("\nscalar (original)  : {scalar_s:7.3}s");
    println!("vectorized (batch) : {vectorized_s:7.3}s");
    println!("speedup             : {speedup:.2}x");
}
