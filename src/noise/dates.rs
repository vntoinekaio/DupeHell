// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
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

    let ops: Vec<usize> = (0..n).map(|_| rng2.next_usize(4)).collect();

    let mut builder = StringBuilder::with_capacity(n, 16);
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
    let result = parts
        .iter()
        .enumerate()
        .map(|(j, p)| {
            if j == year_idx {
                format!("{:04}", new_year)
            } else {
                p.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("-");
    rejoin_datetime(result, time_part)
}

/// Normalize separators to DD-MM-YYYY format.
fn normalize_dash(s: &str) -> String {
    let (date_part, time_part) = split_datetime(s);
    rejoin_datetime(date_part.replace('/', "-"), time_part)
}

/// Swap day and month in a date string.
fn swap_dm(s: &str) -> String {
    let (date_part, time_part) = split_datetime(s);
    let mut parts: Vec<&str> = date_part.split(['-', '/']).collect();
    if parts.len() == 3 && parts[0].len() <= 2 && parts[1].len() <= 2 {
        parts.swap(0, 1);
    }
    rejoin_datetime(parts.join("-"), time_part)
}

/// Mix date formats: randomly choose one of 4 format variants.
pub fn noise_dates_mix(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let formats: Vec<usize> = (0..n).map(|_| rng2.next_usize(4)).collect();

    let mut builder = StringBuilder::with_capacity(n, 16);
    for (i, &fmt) in formats.iter().enumerate().take(n) {
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

    let strategies: Vec<usize> = (0..n).map(|_| rng2.next_usize(3)).collect();

    let mut builder = StringBuilder::with_capacity(n, 16);
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
        let mut new_parts: Vec<String> = parts.iter().map(|p| p.to_string()).collect();
        new_parts[year_idx] = format!("{:04}", new_year);
        builder.append_value(rejoin_datetime(new_parts.join("-"), time_part));
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
    fn test_deterministic() {
        let arr = make_arr(&["15-03-2020", "01-01-1990"]);
        let a = noise_dates(&*arr, &mut Rng::new(42));
        let b = noise_dates(&*arr, &mut Rng::new(42));
        let sa = a.as_string::<i32>();
        let sb = b.as_string::<i32>();
        assert_eq!(sa.value(0), sb.value(0));
    }
}
