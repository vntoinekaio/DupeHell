// Isolated micro-benchmark for hunt3007/H5: compares raw OS thread spawn
// (`std::thread::scope` + `s.spawn`, the pattern used in
// `pipeline.rs::run_pipeline_with_progress` for per-batch parallel noise
// application) against `rayon::scope` (pooled worker threads) at a spawn
// count/workload shape representative of a real run.
//
// Not wired into the crate's normal build -- run explicitly:
//   cargo run --release --example profile_thread_spawn

use std::time::Instant;

// ~100 batches (50M records / 500K BATCH_SIZE) x ~6 active noise types
// (hell tier) = ballpark of the 434 threads VTune measured on the real
// 50M aviation/hell run.
const BATCHES: usize = 100;
const SPAWNS_PER_BATCH: usize = 6;

// Trivial workload: touches memory (not a true no-op the optimizer could
// elide) but does no real allocation of its own, so the measured time is
// dominated by spawn/join overhead, not by the work itself.
fn trivial_work(seed: u64) -> u64 {
    let mut acc = seed;
    for _ in 0..1000 {
        acc = acc.wrapping_mul(6364136223846793005).wrapping_add(1);
    }
    acc
}

fn bench_std_thread_scope() -> u64 {
    let mut total = 0u64;
    for b in 0..BATCHES {
        std::thread::scope(|s| {
            let handles: Vec<_> = (0..SPAWNS_PER_BATCH)
                .map(|i| s.spawn(move || trivial_work((b * SPAWNS_PER_BATCH + i) as u64)))
                .collect();
            for h in handles {
                total = total.wrapping_add(h.join().unwrap());
            }
        });
    }
    total
}

fn bench_rayon_scope() -> u64 {
    let mut total = 0u64;
    for b in 0..BATCHES {
        rayon::scope(|s| {
            let (tx, rx) = std::sync::mpsc::channel();
            for i in 0..SPAWNS_PER_BATCH {
                let tx = tx.clone();
                s.spawn(move |_| {
                    let r = trivial_work((b * SPAWNS_PER_BATCH + i) as u64);
                    tx.send(r).unwrap();
                });
            }
            drop(tx);
            for r in rx {
                total = total.wrapping_add(r);
            }
        });
    }
    total
}

fn main() {
    // Warm up rayon's global pool once, outside the timed region -- a
    // fresh process would pay first-spawn pool-init cost regardless of
    // which approach is used, so it's not part of what we're comparing.
    rayon::scope(|s| s.spawn(|_| {}));

    let t0 = Instant::now();
    let a = bench_std_thread_scope();
    let std_elapsed = t0.elapsed();

    let t1 = Instant::now();
    let b = bench_rayon_scope();
    let rayon_elapsed = t1.elapsed();

    println!(
        "std::thread::scope : {:>8.3}ms  ({} spawns, checksum={a})",
        std_elapsed.as_secs_f64() * 1000.0,
        BATCHES * SPAWNS_PER_BATCH,
    );
    println!(
        "rayon::scope        : {:>8.3}ms  ({} spawns, checksum={b})",
        rayon_elapsed.as_secs_f64() * 1000.0,
        BATCHES * SPAWNS_PER_BATCH,
    );
    println!(
        "ratio (std/rayon)   : {:.2}x",
        std_elapsed.as_secs_f64() / rayon_elapsed.as_secs_f64()
    );
}
