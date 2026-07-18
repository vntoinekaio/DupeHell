// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder};

use crate::rng::Rng;

use super::{MIN_LEN_DROPOUT, MIN_LEN_UNICODE, get_chars_into};

static ZERO_WIDTH_CHARS: &[char] = &['\u{200b}', '\u{200c}', '\u{200d}', '\u{feff}'];

/// Visually-similar substitutions per char — a `match` over 22 entries
/// beats a `LazyLock<HashMap<char, Vec<char>>>` (no hashing, no lazy-init
/// check, no per-entry heap `Vec`; each arm is a `&'static [char]`).
fn homoglyphs(c: char) -> Option<&'static [char]> {
    match c {
        'a' => Some(&['а', 'α', '@']),
        'A' => Some(&['А', 'Α', '@']),
        'c' => Some(&['с', 'с']),
        'C' => Some(&['С', 'С']),
        'e' => Some(&['е', 'ε', '3']),
        'E' => Some(&['Е', 'Ε', '3']),
        'o' => Some(&['о', '0', 'ο']),
        'O' => Some(&['О', 'Ο', '0']),
        'p' => Some(&['р', 'ρ', 'р']),
        'P' => Some(&['Р', 'Ρ']),
        's' => Some(&['ѕ', '5', '$']),
        'S' => Some(&['Ѕ', '5', '$']),
        'x' => Some(&['х', 'χ']),
        'X' => Some(&['Х', 'Χ']),
        'y' => Some(&['у', 'γ']),
        'Y' => Some(&['У', 'Υ']),
        '0' => Some(&['О', 'ο', 'O']),
        '1' => Some(&['l', 'I', '|']),
        'l' => Some(&['1', 'I', '|']),
        'I' => Some(&['1', 'l', '|']),
        '5' => Some(&['S', '$', 'ѕ']),
        '8' => Some(&['Β', 'B']),
        _ => None,
    }
}

/// OCR-style misreads per char — a `match` over 11 entries beats a
/// `LazyLock<HashMap>`.
fn ocr_replacement(c: char) -> Option<char> {
    match c {
        'o' => Some('0'),
        'O' => Some('0'),
        '0' => Some('O'),
        'l' => Some('1'),
        'I' => Some('1'),
        '1' => Some('l'),
        'S' => Some('5'),
        's' => Some('5'),
        '5' => Some('S'),
        'B' => Some('8'),
        '8' => Some('B'),
        _ => None,
    }
}

/// Replace 1-2 chars with visually similar Unicode chars.
pub fn apply_homoglyph(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let n_ops: Vec<usize> = (0..n).map(|_| rng2.next_usize(2) + 1).collect();
    let positions: Vec<usize> = (0..n * 2).map(|_| rng2.next_usize(30)).collect();

    let mut builder = StringBuilder::with_capacity(n, n * 16);
    let mut chars: Vec<char> = Vec::new();
    for i in 0..n {
        if get_chars_into(src, i, 2, &mut chars) {
            for &p in positions[i * 2..i * 2 + 2].iter().take(n_ops[i].min(2)) {
                let pos = p % chars.len();
                if let Some(alternatives) = homoglyphs(chars[pos]) {
                    let idx = rng2.next_usize(alternatives.len());
                    chars[pos] = alternatives[idx];
                }
            }
            builder.append_value(chars.iter().collect::<String>());
        } else if src.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(src.value(i));
        }
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Insert 1-3 zero-width characters.
pub fn apply_unicode_pollution(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let n_ops: Vec<usize> = (0..n).map(|_| rng2.next_usize(3) + 1).collect();
    let positions: Vec<usize> = (0..n * 31).map(|_| rng2.next_usize(31)).collect();
    let zw_idx: Vec<usize> = (0..n * 3)
        .map(|_| rng2.next_usize(ZERO_WIDTH_CHARS.len()))
        .collect();

    let mut builder = StringBuilder::with_capacity(n, n * 16);
    let mut chars: Vec<char> = Vec::new();
    for (i, &n_op) in n_ops.iter().enumerate() {
        if get_chars_into(src, i, MIN_LEN_UNICODE, &mut chars) {
            let pos_base = i * 31;
            let zw_base = i * 3;
            for j in 0..n_op.min(3) {
                let pos = positions[pos_base + j] % (chars.len() + 1);
                let zc = ZERO_WIDTH_CHARS[zw_idx[zw_base + j] % ZERO_WIDTH_CHARS.len()];
                chars.insert(pos, zc);
            }
            builder.append_value(chars.iter().collect::<String>());
        } else if src.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(src.value(i));
        }
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// 1-3 OCR-style replacements (o↔0, l↔1, S↔5, B↔8).
pub fn apply_ocr_errors(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let n_ops: Vec<usize> = (0..n).map(|_| rng2.next_usize(3) + 1).collect();
    let positions: Vec<usize> = (0..n * 3).map(|_| rng2.next_usize(30)).collect();

    let mut builder = StringBuilder::with_capacity(n, n * 16);
    let mut chars: Vec<char> = Vec::new();
    for i in 0..n {
        if get_chars_into(src, i, 1, &mut chars) {
            for &p in positions[i * 3..i * 3 + 3].iter().take(n_ops[i].min(3)) {
                let pos = p % chars.len();
                if let Some(replacement) = ocr_replacement(chars[pos]) {
                    chars[pos] = replacement;
                }
            }
            builder.append_value(chars.iter().collect::<String>());
        } else if src.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(src.value(i));
        }
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// 33% uppercase, 33% lowercase, 33% random half uppercase.
pub fn apply_case_swap(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let r_vals: Vec<f64> = (0..n).map(|_| rng2.next_f64()).collect();

    let mut builder = StringBuilder::with_capacity(n, n * 16);
    for (i, &r) in r_vals.iter().enumerate().take(n) {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        if r < 0.33 {
            builder.append_value(s.to_uppercase());
        } else if r < 0.66 {
            builder.append_value(s.to_lowercase());
        } else {
            let chars: Vec<char> = s.chars().collect();
            let mut result = String::with_capacity(chars.len());
            for &c in &chars {
                if rng2.next_usize(2) == 0 {
                    result.push(c.to_uppercase().next().unwrap_or(c));
                } else {
                    result.push(c.to_lowercase().next().unwrap_or(c));
                }
            }
            builder.append_value(&result);
        }
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Remove 1..max(2, len/4) random characters from strings with len >= 4.
pub fn apply_char_dropout(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let mut builder = StringBuilder::with_capacity(n, n * 16);
    let mut chars: Vec<char> = Vec::new();
    let mut alive: Vec<bool> = Vec::new();
    for i in 0..n {
        if get_chars_into(src, i, MIN_LEN_DROPOUT, &mut chars) {
            let len = chars.len();
            let max_remove = (len / 4).max(2);
            let n_to_remove = rng2.next_usize(max_remove) + 1;

            // Mark-and-filter instead of repeated `Vec::remove` (which
            // shifts the tail on every call): each draw still picks the
            // p-th *currently alive* char in original order — same RNG
            // draws, same elements dropped — but the string is rebuilt in
            // a single pass at the end instead of one shift per removal.
            alive.clear();
            alive.resize(len, true);
            let mut remaining = len;
            for _ in 0..n_to_remove {
                if remaining > 2 {
                    let mut p = rng2.next_usize(remaining);
                    let idx = alive
                        .iter()
                        .position(|&a| {
                            if a {
                                if p == 0 {
                                    return true;
                                }
                                p -= 1;
                            }
                            false
                        })
                        .expect("remaining count tracks alive entries");
                    alive[idx] = false;
                    remaining -= 1;
                }
            }
            let s: String = chars
                .iter()
                .zip(alive.iter())
                .filter(|&(_, &a)| a)
                .map(|(&c, _)| c)
                .collect();
            builder.append_value(s);
        } else if src.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(src.value(i));
        }
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
    fn test_homoglyph() {
        let arr = make_arr(&["hello", "world", "aeiou"]);
        let mut rng = test_rng();
        let result = apply_homoglyph(&*arr, &mut rng);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_unicode_pollution() {
        let arr = make_arr(&["hello", "world", "abcdef"]);
        let mut rng = test_rng();
        let result = apply_unicode_pollution(&*arr, &mut rng);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_ocr_errors() {
        let arr = make_arr(&["hello", "world", "test"]);
        let mut rng = test_rng();
        let result = apply_ocr_errors(&*arr, &mut rng);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_case_swap() {
        let arr = make_arr(&["Hello", "WORLD", "Test"]);
        let mut rng = test_rng();
        let result = apply_case_swap(&*arr, &mut rng);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_char_dropout() {
        let arr = make_arr(&["hello", "world", "abcdefgh"]);
        let mut rng = test_rng();
        let result = apply_char_dropout(&*arr, &mut rng);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_visual_deterministic() {
        let arr = make_arr(&["hello", "world"]);
        let a = apply_homoglyph(&*arr, &mut Rng::new(42));
        let b = apply_homoglyph(&*arr, &mut Rng::new(42));
        let sa = a.as_string::<i32>();
        let sb = b.as_string::<i32>();
        assert_eq!(sa.value(0), sb.value(0));
    }
}
