// Isolated micro-benchmark: is `from_utf8().unwrap()` (buf_gen.rs's
// `build_string_array`, called once per generated string cell across the
// whole crate) worth replacing with `from_utf8_unchecked` behind a
// `debug_assert!`? Isolated A/B: same row-building closure (mix of ASCII
// literals + a pool `&str` slice, same shape as `fast_template::gen_street`
// — the realistic case, not a synthetic all-ASCII-literal one), timed with
// validation on vs. off. Not a conclusion drawn from a full pipeline run.

use std::hint::black_box;
use std::time::Instant;

const N: usize = 5_000_000;

fn pool() -> Vec<String> {
    // A handful of pool-like strings, ASCII (matches `street_names` in the
    // "en" locale — the accented case is rarer and would only strengthen
    // the case for keeping the debug_assert, not change the timing shape).
    vec![
        "Main".to_string(),
        "Oak".to_string(),
        "Pine".to_string(),
        "Elm".to_string(),
        "Maple".to_string(),
    ]
}

fn build_row(buf: &mut Vec<u8>, i: usize, streets: &[String]) {
    let num = 100 + (i % 9900);
    let s = num.to_string();
    buf.extend_from_slice(s.as_bytes());
    buf.push(b' ');
    buf.extend_from_slice(streets[i % streets.len()].as_bytes());
    buf.push(b' ');
    buf.extend_from_slice(b"St");
}

fn run_checked(streets: &[String]) -> u64 {
    let mut buf: Vec<u8> = Vec::with_capacity(32);
    let mut acc: u64 = 0;
    for i in 0..N {
        buf.clear();
        build_row(&mut buf, i, streets);
        let s = std::str::from_utf8(&buf).unwrap();
        acc = acc.wrapping_add(s.len() as u64);
    }
    acc
}

fn run_unchecked(streets: &[String]) -> u64 {
    let mut buf: Vec<u8> = Vec::with_capacity(32);
    let mut acc: u64 = 0;
    for i in 0..N {
        buf.clear();
        build_row(&mut buf, i, streets);
        debug_assert!(std::str::from_utf8(&buf).is_ok());
        let s = unsafe { std::str::from_utf8_unchecked(&buf) };
        acc = acc.wrapping_add(s.len() as u64);
    }
    acc
}

fn main() {
    let streets = pool();
    // warm-up
    black_box(run_checked(&streets));
    black_box(run_unchecked(&streets));

    for iter in 0..5 {
        let t = Instant::now();
        let a = black_box(run_checked(&streets));
        let ta = t.elapsed().as_secs_f64() * 1000.0;

        let t = Instant::now();
        let b = black_box(run_unchecked(&streets));
        let tb = t.elapsed().as_secs_f64() * 1000.0;

        assert_eq!(a, b, "checksum mismatch — checked vs unchecked disagree");
        println!(
            "iter {iter}: checked={ta:.1} ms | unchecked={tb:.1} ms | speedup={:.3}x",
            ta / tb
        );
    }
}
