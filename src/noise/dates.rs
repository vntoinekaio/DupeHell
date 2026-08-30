// DupeHell -- MIT License
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// No liability for misuse.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder};

use crate::rng::Rng;

/// 50% rows get one of 4 operations: flip DD-MM-YYYY ↔ YYYY/MM/DD,
/// fuzz year, normalize separators, or swap day and month.
pub fn noise_dates(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    // NOTE (perf-hunt hunt1708, H2): drawing `op` eagerly for every row
    // BEFORE the loop, rather than inline at the top of the loop body, is
    // load-bearing here, not just a style choice — `fuzz_year` (op==1)
    // consumes extra draws from `rng2` mid-loop. Moving the primary draw
    // inline would interleave it with those secondary draws differently
    // than today, changing every row's `op` from that point on (verified:
    // broke an A/B checksum). Keep this precompute as-is.
    let ops: Vec<usize> = (0..n).map(|_| rng2.next_usize(4)).collect();

    let mut builder = StringBuilder::with_capacity(n, n * 16);
    for (i, &op) in ops.iter().enumerate().take(n) {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        // Only operate on date-like strings with at least 8 chars
        if s.len() < 8 {
            builder.append_value(s);
            continue;
        }
        let result = match op {
            0 => flip_format(s),          // DD-MM-YYYY ↔ YYYY/MM/DD
            1 => fuzz_year(s, &mut rng2), // fuzz year
            2 => normalize_dash(s),       // normalize: DD-MM-YYYY
            3 => swap_dm(s),              // swap day/month
            _ => s.to_string(),
        };
        builder.append_value(&result);
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Splits a "YYYY-MM-DD HH:MM:SS"-style string into its date part and an
/// optional " HH:MM:SS" time suffix, so date-only transforms never mangle
/// the time component (a naive split on '-'/'/'  spills the time digits
/// into the date fields, e.g. "01 20:00:00-07-2024").
fn split_datetime(s: &str) -> (&str, Option<&str>) {
    match s.split_once(' ') {
        Some((date_part, time_part)) => (date_part, Some(time_part)),
        None => (s, None),
    }
}

fn rejoin_datetime(date_part: String, time_part: Option<&str>) -> String {
    match time_part {
        Some(t) => format!("{date_part} {t}"),
        None => date_part,
    }
}

/// Reformats DD-MM-YYYY to YYYY/MM/DD (or reverse).
fn flip_format(s: &str) -> String {
    let (date_part, time_part) = split_datetime(s);
    // Try DD-MM-YYYY → YYYY/MM/DD
    let parts: Vec<&str> = date_part.split(['-', '/']).collect();
    let result = if parts.len() == 3 {
        if parts[0].len() == 2 && parts[2].len() == 4 {
            // DD-MM-YYYY → YYYY/MM/DD
            format!("{}/{}/{}", parts[2], parts[1], parts[0])
        } else if parts[0].len() == 4 {
            // YYYY/MM/DD → DD-MM-YYYY
            format!("{}-{}-{}", parts[2], parts[1], parts[0])
        } else {
            date_part.to_string()
        }
    } else {
        date_part.to_string()
    };
    rejoin_datetime(result, time_part)
}

/// Fuzz year by ±{10, 1, decade, year}, clamped 1930-2025.
fn fuzz_year(s: &str, rng: &mut Rng) -> String {
    let (date_part, time_part) = split_datetime(s);
    let parts: Vec<&str> = date_part.split(['-', '/']).collect();
    if parts.len() != 3 {
        return s.to_string();
    }
    // Find the year part (4-digit)
    let year_idx = if parts[0].len() == 4 {
        0
    } else if parts[2].len() == 4 {
        2
    } else {
        return s.to_string();
    };
    let year: i32 = match parts[year_idx].parse() {
        Ok(y) => y,
        Err(_) => return s.to_string(),
    };
    let offset: i32 = match rng.next_usize(4) {
        0 => 10,
        1 => -10,
        2 => 1,
        _ => -1,
    };
    let new_year = (year + offset).clamp(1930, 2025);
    use std::fmt::Write;
    let mut result = String::with_capacity(date_part.len() + 1);
    for (j, p) in parts.iter().enumerate() {
        if j > 0 {
            result.push('-');
        }
        if j == year_idx {
            write!(result, "{:04}", new_year).ok();
        } else {
            result.push_str(p);
        }
    }
    rejoin_datetime(result, time_part)
}

/// Normalize separators to DD-MM-YYYY format.
fn normalize_dash(s: &str) -> String {
    let (date_part, time_part) = split_datetime(s);
    rejoin_datetime(date_part.replace('/', "-"), time_part)
}

/// Swap day and month in a date string. Detects which end holds the year
/// the same way `fuzz_year` does, since the generator's native format is
/// `YYYY-MM-DD` (`column_gen.rs`'s `gen_date`/`gen_datetime`) — the previous
/// `parts[0].len() <= 2` check only ever matched `DD-MM-YYYY`, making this a
/// guaranteed no-op on every freshly generated date.
fn swap_dm(s: &str) -> String {
    let (date_part, time_part) = split_datetime(s);
    let mut parts: Vec<&str> = date_part.split(['-', '/']).collect();
    if parts.len() == 3 {
        if parts[0].len() == 4 {
            // YYYY-MM-DD: day/month are parts[1]/parts[2].
            parts.swap(1, 2);
        } else if parts[2].len() == 4 && parts[0].len() <= 2 && parts[1].len() <= 2 {
            // DD-MM-YYYY: day/month are parts[0]/parts[1].
            parts.swap(0, 1);
        }
    }
    rejoin_datetime(parts.join("-"), time_part)
}

/// Mix date formats: randomly choose one of 4 format variants.
pub fn noise_dates_mix(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let mut builder = StringBuilder::with_capacity(n, n * 16);
    for i in 0..n {
        // Same draw-order note as `noise_dates` above.
        let fmt = rng2.next_usize(4);
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        let (date_part, time_part) = split_datetime(s);
        let parts: Vec<&str> = date_part.split(['-', '/']).collect();
        if parts.len() != 3 {
            builder.append_value(s);
            continue;
        }
        let day = parts[0];
        let month = parts[1];
        let year = parts[2];
        let year_short = if year.len() == 4 {
            year.get(2..).unwrap_or(year)
        } else {
            year
        };
        let result = match fmt {
            0 => format!("{}/{}/{}", day, month, year),
            1 => format!("{}/{}/{}", month, day, year),
            2 => format!("{}/{}/{}", year, month, day),
            3 => format!("{}/{}/{}", day, month, year_short),
            _ => date_part.to_string(),
        };
        builder.append_value(rejoin_datetime(result, time_part));
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Shift year to impossible values: +121-150, -20-50, or 1800-1900.
pub fn apply_age_impossible(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    // NOTE (perf-hunt hunt1708, H2): kept as an eager precompute, not
    // inline — the `new_year` match arms below draw extra values from
    // `rng2` mid-loop (`0`/`1`/`_` arms), so inlining `strategy`'s draw
    // would change the RNG interleaving for every row after the first one
    // that hits this branch. See `noise_dates`'s note for the same reason.
    let strategies: Vec<usize> = (0..n).map(|_| rng2.next_usize(3)).collect();

    let mut builder = StringBuilder::with_capacity(n, n * 16);
    for (i, &strategy) in strategies.iter().enumerate().take(n) {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        let (date_part, time_part) = split_datetime(s);
        let parts: Vec<&str> = date_part.split(['-', '/']).collect();
        if parts.len() != 3 {
            builder.append_value(s);
            continue;
        }
        let year_idx = if parts[0].len() == 4 {
            0
        } else if parts[2].len() == 4 {
            2
        } else {
            builder.append_value(s);
            continue;
        };
        let year: i32 = match parts[year_idx].parse() {
            Ok(y) => y,
            Err(_) => {
                builder.append_value(s);
                continue;
            }
        };
        let new_year = match strategy {
            0 => year + rng2.next_usize(30) as i32 + 121, // impossibly old
            1 => year - rng2.next_usize(31) as i32 - 20,  // negative age
            _ => rng2.next_usize(101) as i32 + 1800,      // 1800-1900
        };
        use std::fmt::Write;
        let mut result = String::with_capacity(date_part.len() + 1);
        for (j, p) in parts.iter().enumerate() {
            if j > 0 {
                result.push('-');
            }
            if j == year_idx {
                write!(result, "{:04}", new_year).ok();
            } else {
                result.push_str(p);
            }
        }
        builder.append_value(rejoin_datetime(result, time_part));
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, StringArray};

    fn test_rng() -> Rng {
        Rng::new(42)
    }
    fn make_arr(vals: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(vals.to_vec()))
    }

    #[test]
    fn test_noise_dates() {
        let arr = make_arr(&["15-03-2020", "01-01-1990", "2025-12-31"]);
        let mut rng = test_rng();
        let result = noise_dates(&*arr, &mut rng);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_noise_dates_mix() {
        let arr = make_arr(&["15-03-2020", "01-01-1990"]);
        let mut rng = test_rng();
        let result = noise_dates_mix(&*arr, &mut rng);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_age_impossible() {
        let arr = make_arr(&["15-03-1990", "01-01-2000"]);
        let mut rng = test_rng();
        let result = apply_age_impossible(&*arr, &mut rng);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_swap_dm_year_first_format() {
        // `column_gen::gen_date`/`gen_datetime` always produce YYYY-MM-DD —
        // before the fix, `swap_dm` only recognized DD-MM-YYYY and was a
        // guaranteed no-op on this format.
        assert_eq!(swap_dm("2020-03-15"), "2020-15-03");
        // DD-MM-YYYY still swaps correctly (unchanged behavior).
        assert_eq!(swap_dm("15-03-2020"), "03-15-2020");
    }

    #[test]
    fn test_deterministic() {
        let arr = make_arr(&["15-03-2020", "01-01-1990"]);
        let a = noise_dates(&*arr, &mut Rng::new(42));
        let b = noise_dates(&*arr, &mut Rng::new(42));
        let sa = a.as_string::<i32>();
        let sb = b.as_string::<i32>();
        assert_eq!(sa.value(0), sb.value(0));
    }
}
