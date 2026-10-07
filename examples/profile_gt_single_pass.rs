// Isolated micro-benchmark: is the two-pass ground-truth build (draft IPC
// file + global `FxHashSet` duplicated-master sets + full re-read +
// single-threaded reclassification + final write, `gt::GtAccumulator`)
// replaceable by a single pass that classifies each entity batch as soon as
// its duplicates are known?
//
// The premise: `pipeline.rs` draws a batch's duplicate rows exclusively from
// that same batch's masters (`batch_rng.next_usize(batch_n)`) and writes them
// right after the batch, so every master's duplicated / has-exact-copy status
// is final at the end of its own batch — no global view needed.
//
// Doesn't touch crate-private code (`gt`/`pipeline` helpers aren't `pub`) --
// reimplements the same data shapes and the same per-row work (`R-` + 13
// digit record_ids, `E{5}-{13}` master_ids parsed via the same
// `pack_master_key` logic, `HN-{9}` hard negatives, shared dictionaries for
// entity_type/match_type/difficulty) directly in this file.
//
// Three variants, each fed the exact same deterministic synthetic stream:
//   gen  — generate the stream only (baseline, subtracted from the others)
//   two  — current algorithm (draft + sets + finish)
//   one  — single pass (per-batch local bitsets, one final write)
// The final GT files of `two` and `one` are SHA-256 compared: they must be
// byte-identical, otherwise the single-pass design isn't a drop-in.
//
// Usage:
//   cargo run --release --example profile_gt_single_pass -- [N_BASE_MASTERS] [OUT_DIR] [REPS]
// Defaults: 8_700_000 masters (~20M total rows, same mix as the real
// aviation/hell/passenger run), OUT_DIR = system temp dir, REPS = 1.
//
// Caveat: at the default size the draft fits in the OS page cache, so its
// re-read is served from RAM — this UNDERSTATES the I/O cost of the
// two-pass design compared to a real 750M-row run (~42 GB draft, larger
// than RAM). Run with a bigger N_BASE_MASTERS to see the I/O-bound regime.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, DictionaryArray, Int32Array, Int32Builder, StringArray,
    StringBuilder,
};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;
use rustc_hash::FxHashSet;
use sha2::{Digest, Sha256};

const BATCH: usize = 500_000;
const PAD: usize = 13;
// Mix measured on the real aviation/hell/passenger 750M run:
// base masters 325M, duplicate rows ~425M, exact (no-op noise) dup rows
// ~0.3%, hard negatives ~2.1% of all rows.
const DUPS_PER_MASTER: f64 = 425.0 / 325.0;
const EXACT_RATE: f64 = 0.003;
const HN_PER_MASTER: f64 = 15.9 / 325.0;

// ── Synthetic stream ───────────────────────────────────────────────────────

struct Dicts {
    entity_type: Arc<StringArray>,
    match_type: Arc<StringArray>,
    difficulty: Arc<StringArray>,
}

impl Dicts {
    fn new() -> Self {
        Self {
            entity_type: Arc::new(StringArray::from(vec!["passenger"])),
            match_type: Arc::new(StringArray::from(vec![
                "hard_neg",
                "canary",
                "exact_dup",
                "fuzzy_dup",
                "unique",
            ])),
            difficulty: Arc::new(StringArray::from(vec!["hell"])),
        }
    }
}

const K_HARD_NEG: i32 = 0;
const K_EXACT_DUP: i32 = 2;
const K_FUZZY_DUP: i32 = 3;
const K_UNIQUE: i32 = 4;

fn dict_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

fn dict_array(values: &Arc<StringArray>, keys: Int32Array) -> ArrayRef {
    Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values.clone() as ArrayRef).unwrap())
}

fn const_dict(values: &Arc<StringArray>, n: usize) -> ArrayRef {
    dict_array(values, Int32Array::from(vec![0i32; n]))
}

fn write_digits(buf: &mut Vec<u8>, mut v: u64, width: usize) {
    let start = buf.len();
    buf.resize(start + width, b'0');
    for j in (0..width).rev() {
        buf[start + j] = b'0' + (v % 10) as u8;
        v /= 10;
    }
}

fn id_array(prefix: &[u8], width: usize, ids: impl Iterator<Item = u64>, n: usize) -> ArrayRef {
    let mut b = StringBuilder::with_capacity(n, n * (prefix.len() + width));
    let mut buf = Vec::with_capacity(prefix.len() + width);
    for id in ids {
        buf.clear();
        buf.extend_from_slice(prefix);
        write_digits(&mut buf, id, width);
        b.append_value(std::str::from_utf8(&buf).unwrap());
    }
    Arc::new(b.finish())
}

/// One entity batch as the pipeline produces it: `base_n` brand-new masters,
/// then the duplicate rows drawn from those same masters.
struct EntityBatch {
    base_rid: ArrayRef,
    base_mid: ArrayRef,
    base_et: ArrayRef,
    dup_rid: ArrayRef,
    dup_mid: ArrayRef,
    dup_et: ArrayRef,
    /// Local (0..base_n) master index of each duplicate row.
    dup_local: Vec<u32>,
    dup_identical: Vec<bool>,
}

/// Deterministic stream: entity batches, then hard-negative batches.
enum Item {
    Entity(EntityBatch),
    HardNeg {
        rid: ArrayRef,
        mid: ArrayRef,
        et: ArrayRef,
    },
}

fn stream(n_base: usize, dicts: &Dicts, mut f: impl FnMut(Item)) {
    let mut rng = Pcg64::seed_from_u64(42);
    let mut rid_next: u64 = 0;
    let mut offset = 0usize;
    while offset < n_base {
        let base_n = (n_base - offset).min(BATCH);
        let n_dup = (base_n as f64 * DUPS_PER_MASTER).round() as usize;
        let base_rid = id_array(b"R-", PAD, rid_next..rid_next + base_n as u64, base_n);
        rid_next += base_n as u64;
        let base_mid = id_array(
            b"E00000-",
            PAD,
            (offset as u64)..(offset + base_n) as u64,
            base_n,
        );
        let dup_local: Vec<u32> = (0..n_dup)
            .map(|_| rng.gen_range(0..base_n) as u32)
            .collect();
        let dup_identical: Vec<bool> = (0..n_dup).map(|_| rng.gen_bool(EXACT_RATE)).collect();
        let dup_rid = id_array(b"R-", PAD, rid_next..rid_next + n_dup as u64, n_dup);
        rid_next += n_dup as u64;
        let dup_mid = id_array(
            b"E00000-",
            PAD,
            dup_local.iter().map(|&l| (offset + l as usize) as u64),
            n_dup,
        );
        f(Item::Entity(EntityBatch {
            base_et: const_dict(&dicts.entity_type, base_n),
            dup_et: const_dict(&dicts.entity_type, n_dup),
            base_rid,
            base_mid,
            dup_rid,
            dup_mid,
            dup_local,
            dup_identical,
        }));
        offset += base_n;
    }
    let n_hn = (n_base as f64 * HN_PER_MASTER).round() as usize;
    let mut done = 0usize;
    while done < n_hn {
        let n = (n_hn - done).min(BATCH);
        let rid = id_array(b"R-", PAD, rid_next..rid_next + n as u64, n);
        rid_next += n as u64;
        let mid = id_array(b"HN-", 9, (done as u64)..(done + n) as u64, n);
        f(Item::HardNeg {
            rid,
            mid,
            et: const_dict(&dicts.entity_type, n),
        });
        done += n;
    }
}

// ── Shared output pieces ───────────────────────────────────────────────────

fn final_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("record_id", DataType::Utf8, false),
        Field::new("master_id", DataType::Utf8, false),
        Field::new("entity_type", dict_type(), false),
        Field::new("match_type", dict_type(), false),
        Field::new("difficulty", dict_type(), false),
    ]))
}

#[derive(Default, Debug, PartialEq, Clone, Copy)]
struct Counts {
    exact: usize,
    fuzzy: usize,
    unique: usize,
    hard_neg: usize,
    rows: usize,
}

struct FinalWriter {
    w: FileWriter<File>,
    schema: Arc<Schema>,
    diff_cache: Option<(usize, ArrayRef)>,
}

impl FinalWriter {
    fn new(path: &Path) -> Self {
        let schema = final_schema();
        let w = FileWriter::try_new(File::create(path).unwrap(), &schema).unwrap();
        Self {
            w,
            schema,
            diff_cache: None,
        }
    }

    fn write(
        &mut self,
        dicts: &Dicts,
        rid: &ArrayRef,
        mid: &ArrayRef,
        et: &ArrayRef,
        mt: Int32Array,
    ) {
        let n = rid.len();
        let diff = match &self.diff_cache {
            Some((c, a)) if *c == n => a.clone(),
            _ => {
                let a = const_dict(&dicts.difficulty, n);
                self.diff_cache = Some((n, a.clone()));
                a
            }
        };
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                rid.clone(),
                mid.clone(),
                et.clone(),
                dict_array(&dicts.match_type, mt),
                diff,
            ],
        )
        .unwrap();
        self.w.write(&batch).unwrap();
    }

    fn finish(mut self) {
        self.w.finish().unwrap();
    }
}

// ── Variant: gen only ──────────────────────────────────────────────────────

fn run_gen(n_base: usize) -> usize {
    let dicts = Dicts::new();
    let mut rows = 0usize;
    stream(n_base, &dicts, |item| match item {
        Item::Entity(b) => rows += b.base_rid.len() + b.dup_rid.len(),
        Item::HardNeg { rid, .. } => rows += rid.len(),
    });
    rows
}

// ── Variant: two-pass (current `gt::GtAccumulator`) ───────────────────────

/// Same logic as `pipeline::pack_master_key`.
fn pack_master_key(mid: &str) -> Option<u64> {
    let bytes = mid.as_bytes();
    if bytes.len() != 1 + 5 + 1 + PAD || bytes[0] != b'E' || bytes[6] != b'-' {
        return None;
    }
    let entity_idx: u64 = mid[1..6].parse().ok()?;
    let local_idx: u64 = mid[7..7 + PAD].parse().ok()?;
    Some((entity_idx << 44) | local_idx)
}

fn bools(n: usize, v: bool) -> ArrayRef {
    let buf = if v {
        BooleanBuffer::new_set(n)
    } else {
        BooleanBuffer::new_unset(n)
    };
    Arc::new(BooleanArray::new(buf, None))
}

fn run_two_pass(n_base: usize, dir: &Path, out: &Path) -> (Counts, u64, usize) {
    let dicts = Dicts::new();
    let draft_path = dir.join("profile_gt_draft.ipc");
    let draft_schema = Arc::new(Schema::new(vec![
        Field::new("record_id", DataType::Utf8, false),
        Field::new("master_id", DataType::Utf8, false),
        Field::new("entity_type", dict_type(), false),
        Field::new("is_identical", DataType::Boolean, false),
        Field::new("is_base", DataType::Boolean, false),
    ]));
    let mut draft = FileWriter::try_new(File::create(&draft_path).unwrap(), &draft_schema).unwrap();
    let mut dup_masters: FxHashSet<u64> = FxHashSet::default();
    let mut exact_masters: FxHashSet<u64> = FxHashSet::default();
    let mut write_draft =
        |rid: &ArrayRef, mid: &ArrayRef, et: &ArrayRef, ident: ArrayRef, base: ArrayRef| {
            let b = RecordBatch::try_new(
                draft_schema.clone(),
                vec![rid.clone(), mid.clone(), et.clone(), ident, base],
            )
            .unwrap();
            draft.write(&b).unwrap();
        };

    stream(n_base, &dicts, |item| match item {
        Item::Entity(b) => {
            let n = b.base_rid.len();
            write_draft(
                &b.base_rid,
                &b.base_mid,
                &b.base_et,
                bools(n, false),
                bools(n, true),
            );
            // push_dup_batch: parse every dup master_id, insert into the sets.
            let mids = b.dup_mid.as_string::<i32>();
            for i in 0..mids.len() {
                let key = pack_master_key(mids.value(i)).unwrap();
                dup_masters.insert(key);
                if b.dup_identical[i] {
                    exact_masters.insert(key);
                }
            }
            let nd = b.dup_rid.len();
            let ident: ArrayRef = Arc::new(BooleanArray::from(b.dup_identical));
            write_draft(&b.dup_rid, &b.dup_mid, &b.dup_et, ident, bools(nd, false));
        }
        Item::HardNeg { rid, mid, et } => {
            let n = rid.len();
            write_draft(&rid, &mid, &et, bools(n, false), bools(n, false));
        }
    });
    draft.finish().unwrap();
    drop(draft);
    let set_len = dup_masters.len() + exact_masters.len();
    let draft_bytes = std::fs::metadata(&draft_path).unwrap().len();

    // finish(): re-read the draft, classify every row, write the final file.
    let reader = FileReader::try_new(File::open(&draft_path).unwrap(), None).unwrap();
    let mut fw = FinalWriter::new(out);
    let mut c = Counts::default();
    for batch in reader {
        let batch = batch.unwrap();
        let n = batch.num_rows();
        let mid_col = batch.column(1).as_string::<i32>();
        let ident_col = batch.column(3).as_boolean();
        let base_col = batch.column(4).as_boolean();
        let mut mt = Int32Builder::with_capacity(n);
        for i in 0..n {
            let mid = mid_col.value(i);
            let is_base = base_col.value(i);
            let is_hn = mid.starts_with("HN-");
            let is_canary = !is_hn && mid.starts_with("CANARY-");
            let key = if is_hn || is_canary {
                None
            } else {
                pack_master_key(mid)
            };
            let is_identical = if is_base {
                key.is_some_and(|k| exact_masters.contains(&k))
            } else {
                ident_col.value(i)
            };
            let is_dup = key.is_some_and(|k| dup_masters.contains(&k));
            let k = if is_hn {
                c.hard_neg += 1;
                K_HARD_NEG
            } else if is_dup {
                if is_identical {
                    c.exact += 1;
                    K_EXACT_DUP
                } else {
                    c.fuzzy += 1;
                    K_FUZZY_DUP
                }
            } else {
                c.unique += 1;
                K_UNIQUE
            };
            mt.append_value(k);
        }
        c.rows += n;
        fw.write(
            &dicts,
            batch.column(0),
            batch.column(1),
            batch.column(2),
            mt.finish(),
        );
    }
    fw.finish();
    std::fs::remove_file(&draft_path).ok();
    (c, draft_bytes, set_len)
}

// ── Variant: single pass (hunt0710_gt/H1) ─────────────────────────────────

struct LocalBits {
    words: Vec<u64>,
}

impl LocalBits {
    fn reset(&mut self, n: usize) {
        self.words.clear();
        self.words.resize(n.div_ceil(64), 0);
    }
    fn set(&mut self, i: usize) {
        self.words[i / 64] |= 1 << (i % 64);
    }
    fn get(&self, i: usize) -> bool {
        (self.words[i / 64] >> (i % 64)) & 1 != 0
    }
}

fn run_single_pass(n_base: usize, out: &Path) -> Counts {
    let dicts = Dicts::new();
    let mut fw = FinalWriter::new(out);
    let mut c = Counts::default();
    let mut is_dup = LocalBits { words: Vec::new() };
    let mut has_exact = LocalBits { words: Vec::new() };

    stream(n_base, &dicts, |item| match item {
        Item::Entity(b) => {
            let n = b.base_rid.len();
            is_dup.reset(n);
            has_exact.reset(n);
            for (&l, &ident) in b.dup_local.iter().zip(&b.dup_identical) {
                is_dup.set(l as usize);
                if ident {
                    has_exact.set(l as usize);
                }
            }
            let mut mt = Int32Builder::with_capacity(n);
            for j in 0..n {
                let k = if !is_dup.get(j) {
                    c.unique += 1;
                    K_UNIQUE
                } else if has_exact.get(j) {
                    c.exact += 1;
                    K_EXACT_DUP
                } else {
                    c.fuzzy += 1;
                    K_FUZZY_DUP
                };
                mt.append_value(k);
            }
            fw.write(&dicts, &b.base_rid, &b.base_mid, &b.base_et, mt.finish());

            let nd = b.dup_rid.len();
            let mut mt = Int32Builder::with_capacity(nd);
            for &ident in &b.dup_identical {
                if ident {
                    c.exact += 1;
                    mt.append_value(K_EXACT_DUP);
                } else {
                    c.fuzzy += 1;
                    mt.append_value(K_FUZZY_DUP);
                }
            }
            fw.write(&dicts, &b.dup_rid, &b.dup_mid, &b.dup_et, mt.finish());
            c.rows += n + nd;
        }
        Item::HardNeg { rid, mid, et } => {
            let n = rid.len();
            c.hard_neg += n;
            c.rows += n;
            fw.write(
                &dicts,
                &rid,
                &mid,
                &et,
                Int32Array::from(vec![K_HARD_NEG; n]),
            );
        }
    });
    fw.finish();
    c
}

// ── Driver ─────────────────────────────────────────────────────────────────

fn sha256_file(p: &Path) -> String {
    let mut f = File::open(p).unwrap();
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let k = f.read(&mut buf).unwrap();
        if k == 0 {
            break;
        }
        h.update(&buf[..k]);
    }
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n_base: usize = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(8_700_000);
    let dir: PathBuf = args
        .get(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("dh_profile_gt"));
    let reps: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
    std::fs::create_dir_all(&dir).unwrap();
    let out_two = dir.join("profile_gt_two_pass.ipc");
    let out_one = dir.join("profile_gt_single_pass.ipc");

    println!(
        "profile_gt_single_pass: {n_base} base masters, batch {BATCH}, {reps} rep(s), dir {}",
        dir.display()
    );

    let mut t_gen = f64::MAX;
    let mut t_two = f64::MAX;
    let mut t_one = f64::MAX;
    let mut rows = 0;
    let (mut c_two, mut c_one) = (Counts::default(), Counts::default());
    let (mut draft_bytes, mut set_len) = (0u64, 0usize);
    for rep in 0..reps {
        let t = Instant::now();
        rows = run_gen(n_base);
        t_gen = t_gen.min(t.elapsed().as_secs_f64());

        let t = Instant::now();
        (c_two, draft_bytes, set_len) = run_two_pass(n_base, &dir, &out_two);
        t_two = t_two.min(t.elapsed().as_secs_f64());

        let t = Instant::now();
        c_one = run_single_pass(n_base, &out_one);
        t_one = t_one.min(t.elapsed().as_secs_f64());
        println!("  rep {rep}: done");
    }

    let final_bytes = std::fs::metadata(&out_one).unwrap().len();
    let h_two = sha256_file(&out_two);
    let h_one = sha256_file(&out_one);

    println!("\nrows: {rows}  (counts two={c_two:?})");
    println!("counts identical : {}", c_two == c_one);
    println!(
        "GT files identical (SHA-256): {}  [{}…]",
        h_two == h_one,
        &h_two[..16]
    );
    println!(
        "draft: {:.2} GB written+re-read ({:.1} B/row) | final GT: {:.2} GB ({:.1} B/row)",
        draft_bytes as f64 / 1e9,
        draft_bytes as f64 / rows as f64,
        final_bytes as f64 / 1e9,
        final_bytes as f64 / rows as f64
    );
    println!("two-pass set entries held until finish: {set_len}");
    println!("\n(best of {reps})");
    println!("gen only        : {t_gen:8.3}s");
    println!(
        "two-pass (now)  : {t_two:8.3}s   GT cost = {:8.3}s",
        t_two - t_gen
    );
    println!(
        "single pass (H1): {t_one:8.3}s   GT cost = {:8.3}s",
        t_one - t_gen
    );
    println!(
        "GT cost ratio single/two: {:.2}x  ({:.0}% less GT time)",
        (t_one - t_gen) / (t_two - t_gen),
        (1.0 - (t_one - t_gen) / (t_two - t_gen)) * 100.0
    );

    std::fs::remove_file(&out_two).ok();
    std::fs::remove_file(&out_one).ok();
}
