// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use arrow::array::{Array, ArrayRef, StringBuilder};

use crate::rng::Rng;

use super::{MIN_LEN_DROPOUT, MIN_LEN_UNICODE, get_chars};

static ZERO_WIDTH_CHARS: &[char] = &['\u{200b}', '\u{200c}', '\u{200d}', '\u{feff}'];

static HOMOGLYPHS: LazyLock<HashMap<char, Vec<char>>> = LazyLock::new(|| {
    let mut m: HashMap<char, Vec<char>> = HashMap::new();
    m.insert('a', vec!['а', 'α', '@']);
    m.insert('A', vec!['А', 'Α', '@']);
    m.insert('c', vec!['с', 'с']);
    m.insert('C', vec!['С', 'С']);
    m.insert('e', vec!['е', 'ε', '3']);
    m.insert('E', vec!['Е', 'Ε', '3']);
    m.insert('o', vec!['о', '0', 'ο']);
    m.insert('O', vec!['О', 'Ο', '0']);
    m.insert('p', vec!['р', 'ρ', 'р']);
    m.insert('P', vec!['Р', 'Ρ']);
    m.insert('s', vec!['ѕ', '5', '$']);
    m.insert('S', vec!['Ѕ', '5', '$']);
    m.insert('x', vec!['х', 'χ']);
    m.insert('X', vec!['Х', 'Χ']);
    m.insert('y', vec!['у', 'γ']);
    m.insert('Y', vec!['У', 'Υ']);
    m.insert('0', vec!['О', 'ο', 'O']);
    m.insert('1', vec!['l', 'I', '|']);
    m.insert('l', vec!['1', 'I', '|']);
    m.insert('I', vec!['1', 'l', '|']);
    m.insert('5', vec!['S', '$', 'ѕ']);
    m.insert('8', vec!['Β', 'B']);
    m
});

static OCR_REPLACEMENTS: LazyLock<HashMap<char, char>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert('o', '0');
    m.insert('O', '0');
    m.insert('0', 'O');
    m.insert('l', '1');
    m.insert('I', '1');
    m.insert('1', 'l');
    m.insert('S', '5');
    m.insert('s', '5');
    m.insert('5', 'S');
    m.insert('B', '8');
    m.insert('8', 'B');
    m
});

/// Replace 1-2 chars with visually similar Unicode chars.
pub fn apply_homoglyph(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let n_ops: Vec<usize> = (0..n).map(|_| rng2.next_usize(2) + 1).collect();
    let positions: Vec<usize> = (0..n * 2).map(|_| rng2.next_usize(30)).collect();

    let mut builder = StringBuilder::with_capacity(n, n * 16);
    for i in 0..n {
        match get_chars(src, i, 2) {
            Some(mut chars) => {
                for &p in positions[i * 2..i * 2 + 2].iter().take(n_ops[i].min(2)) {
                    let pos = p % chars.len();
                    if let Some(alternatives) = HOMOGLYPHS.get(&chars[pos]) {
                        let idx = rng2.next_usize(alternatives.len());
                        chars[pos] = alternatives[idx];
                    }
                }
                builder.append_value(chars.into_iter().collect::<String>());
            }
            None => {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
            }
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
    for (i, &n_op) in n_ops.iter().enumerate() {
        match get_chars(src, i, MIN_LEN_UNICODE) {
            Some(mut chars) => {
                let pos_base = i * 31;
                let zw_base = i * 3;
                for j in 0..n_op.min(3) {
                    let pos = positions[pos_base + j] % (chars.len() + 1);
                    let zc = ZERO_WIDTH_CHARS[zw_idx[zw_base + j] % ZERO_WIDTH_CHARS.len()];
                    chars.insert(pos, zc);
                }
                builder.append_value(chars.into_iter().collect::<String>());
            }
            None => {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
            }
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
    for i in 0..n {
        match get_chars(src, i, 1) {
            Some(mut chars) => {
                for &p in positions[i * 3..i * 3 + 3].iter().take(n_ops[i].min(3)) {
                    let pos = p % chars.len();
                    if let Some(&replacement) = OCR_REPLACEMENTS.get(&chars[pos]) {
                        chars[pos] = replacement;
                    }
                }
                builder.append_value(chars.into_iter().collect::<String>());
            }
            None => {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
            }
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
    for i in 0..n {
        match get_chars(src, i, MIN_LEN_DROPOUT) {
            Some(mut chars) => {
                let max_remove = (chars.len() / 4).max(2);
                let n_to_remove = rng2.next_usize(max_remove) + 1;
                for _ in 0..n_to_remove {
                    if chars.len() > 2 {
                        let p = rng2.next_usize(chars.len());
                        chars.remove(p);
                    }
                }
                builder.append_value(chars.into_iter().collect::<String>());
            }
            None => {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
            }
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
