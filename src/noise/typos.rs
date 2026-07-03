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

use super::{MIN_LEN_AGGR, MIN_LEN_EXTREME, MIN_LEN_TYPO, get_chars};

static QWERTY_AZERTY: LazyLock<HashMap<char, char>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert('q', 'a');
    m.insert('w', 'z');
    m.insert('a', 'q');
    m.insert('z', 'w');
    m.insert('Q', 'A');
    m.insert('W', 'Z');
    m.insert('A', 'Q');
    m.insert('Z', 'W');
    m
});

// ── Single-char operations ────────────────────────────────────────

fn op_replace(chars: &mut Vec<char>, pos: usize, c: char) {
    if !chars.is_empty() {
        let idx = pos % chars.len();
        chars[idx] = c;
    }
}

fn op_insert(chars: &mut Vec<char>, pos: usize, c: char) {
    let p = if chars.is_empty() {
        0
    } else {
        pos % chars.len()
    };
    chars.insert(p, c);
}

fn op_duplicate(chars: &mut Vec<char>, pos: usize) {
    if chars.len() > 1 {
        let p = pos % chars.len();
        chars.insert(p, chars[p]);
    }
}

fn op_swap(chars: &mut Vec<char>, pos: usize) {
    if chars.len() > 1 {
        let p = pos % (chars.len() - 1);
        chars.swap(p, p + 1);
    }
}

fn op_delete_pop(chars: &mut Vec<char>, pos: usize) {
    if chars.len() > 2 {
        chars.remove(pos % chars.len());
    }
}

// ── Aggressive helpers ────────────────────────────────────────────

fn apply_swaps(chars: &mut Vec<char>, n_swap: usize, swap_positions: &[usize]) {
    for j in 0..n_swap {
        if chars.len() > 2 && j < swap_positions.len() {
            let p = swap_positions[j] % (chars.len() - 2);
            chars.swap(p, p + 1);
        }
    }
}

fn apply_ops_with_randchar(
    chars: &mut Vec<char>,
    n: usize,
    ops: &[usize],
    positions: &[usize],
    rand_chars: &[char],
) {
    for j in 0..n {
        if j >= ops.len() || j >= positions.len() || j >= rand_chars.len() {
            break;
        }
        let pos = if chars.is_empty() {
            0
        } else {
            positions[j] % chars.len()
        };
        match ops[j] % 4 {
            0 => op_delete_pop(chars, pos),
            1 => op_replace(chars, pos, rand_chars[j]),
            2 => op_insert(chars, pos, rand_chars[j]),
            3 => op_duplicate(chars, pos),
            _ => {}
        }
    }
}

// ── Public noise functions ────────────────────────────────────────

/// Light typo: 1-2 delete/replace/insert ops per string.
pub fn apply_typos_str(arr: &dyn arrow::array::Array, rng: &mut Rng, max_dist: usize) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    // Pre-allocate random arrays
    let n_ops: Vec<usize> = (0..n).map(|_| rng2.next_usize(max_dist) + 1).collect();
    let ops: Vec<Vec<usize>> = (0..n)
        .map(|_| (0..max_dist).map(|_| rng2.next_usize(3)).collect())
        .collect();
    let positions: Vec<Vec<usize>> = (0..n)
        .map(|_| (0..max_dist).map(|_| rng2.next_usize(30)).collect())
        .collect();
    let rand_chars: Vec<Vec<char>> = (0..n)
        .map(|_| {
            (0..max_dist)
                .map(|_| rng2.next_usize(26) as u8 + 97)
                .map(|c| c as char)
                .collect()
        })
        .collect();

    let mut builder = StringBuilder::with_capacity(n, 16);
    for i in 0..n {
        match get_chars(&src, i, MIN_LEN_TYPO) {
            Some(mut chars) => {
                for j in 0..n_ops[i].min(max_dist) {
                    let pos = positions[i][j] % chars.len();
                    let op_idx = ops[i][j] % 3;
                    match op_idx {
                        0 => op_delete_pop(&mut chars, pos),
                        1 => op_replace(&mut chars, pos, rand_chars[i][j]),
                        2 => op_insert(&mut chars, pos, rand_chars[i][j]),
                        _ => {}
                    }
                }
                builder.append_value(&chars.into_iter().collect::<String>());
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

/// Aggressive typos: 1-2 swaps + 2-4 typo ops.
pub fn apply_typos_aggressive(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let n_swap: Vec<usize> = (0..n).map(|_| rng2.next_usize(2) + 1).collect();
    let swap_pos: Vec<Vec<usize>> = (0..n)
        .map(|_| (0..2).map(|_| rng2.next_usize(28)).collect())
        .collect();
    let n_typo: Vec<usize> = (0..n).map(|_| rng2.next_usize(3) + 2).collect();
    let typo_ops: Vec<Vec<usize>> = (0..n)
        .map(|_| (0..5).map(|_| rng2.next_usize(4)).collect())
        .collect();
    let typo_pos: Vec<Vec<usize>> = (0..n)
        .map(|_| (0..5).map(|_| rng2.next_usize(30)).collect())
        .collect();
    let typo_chars: Vec<Vec<char>> = (0..n)
        .map(|_| {
            (0..5)
                .map(|_| (rng2.next_usize(26) as u8 + 97) as char)
                .collect()
        })
        .collect();

    let mut builder = StringBuilder::with_capacity(n, 16);
    for i in 0..n {
        match get_chars(&src, i, MIN_LEN_AGGR) {
            Some(mut chars) => {
                apply_swaps(&mut chars, n_swap[i], &swap_pos[i]);
                apply_ops_with_randchar(
                    &mut chars,
                    n_typo[i],
                    &typo_ops[i],
                    &typo_pos[i],
                    &typo_chars[i],
                );
                builder.append_value(&chars.into_iter().collect::<String>());
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

/// Extreme typos: 4-7 ops from all 5 operation types.
pub fn apply_typos_extreme(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let n_ops: Vec<usize> = (0..n).map(|_| rng2.next_usize(4) + 4).collect();
    let ops: Vec<Vec<usize>> = (0..n)
        .map(|_| (0..8).map(|_| rng2.next_usize(5)).collect())
        .collect();
    let positions: Vec<Vec<usize>> = (0..n)
        .map(|_| (0..8).map(|_| rng2.next_usize(30)).collect())
        .collect();
    let rand_chars: Vec<Vec<char>> = (0..n)
        .map(|_| {
            (0..8)
                .map(|_| (rng2.next_usize(26) as u8 + 97) as char)
                .collect()
        })
        .collect();

    let mut builder = StringBuilder::with_capacity(n, 16);
    for i in 0..n {
        match get_chars(&src, i, MIN_LEN_EXTREME) {
            Some(mut chars) => {
                for j in 0..n_ops[i].min(8) {
                    if chars.is_empty() {
                        break;
                    }
                    let pos = positions[i][j] % chars.len();
                    match ops[i][j] % 5 {
                        0 => op_delete_pop(&mut chars, pos),
                        1 => op_replace(&mut chars, pos, rand_chars[i][j]),
                        2 => op_insert(&mut chars, pos, rand_chars[i][j]),
                        3 => op_duplicate(&mut chars, pos),
                        4 => op_swap(&mut chars, pos),
                        _ => {}
                    }
                }
                builder.append_value(&chars.into_iter().collect::<String>());
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

/// QWERTY → AZERTY substitution.
pub fn apply_qwerty_azerty(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let n_ops: Vec<usize> = (0..n).map(|_| rng2.next_usize(2) + 1).collect();
    let positions: Vec<Vec<usize>> = (0..n)
        .map(|_| (0..2).map(|_| rng2.next_usize(30)).collect())
        .collect();

    let mut builder = StringBuilder::with_capacity(n, 16);
    for i in 0..n {
        match get_chars(&src, i, MIN_LEN_TYPO) {
            Some(mut chars) => {
                for j in 0..n_ops[i].min(2) {
                    let pos = positions[i][j] % chars.len();
                    if let Some(&replacement) = QWERTY_AZERTY.get(&chars[pos]) {
                        chars[pos] = replacement;
                    }
                }
                builder.append_value(&chars.into_iter().collect::<String>());
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
    fn test_typos_str() {
        let arr = make_arr(&["hello", "world", "abc"]);
        let mut rng = test_rng();
        let result = apply_typos_str(&*arr, &mut rng, 2);
        let s = result.as_string::<i32>();
        assert_eq!(s.len(), 3);
        assert!(!s.value(0).is_empty());
    }

    #[test]
    fn test_typos_aggressive() {
        let arr = make_arr(&["hello", "world", "test"]);
        let mut rng = test_rng();
        let result = apply_typos_aggressive(&*arr, &mut rng);
        assert_eq!(result.len(), 3);
        assert!(!result.as_string::<i32>().value(0).is_empty());
    }

    #[test]
    fn test_typos_extreme() {
        let arr = make_arr(&["hello world", "testing", "abcdefgh"]);
        let mut rng = test_rng();
        let result = apply_typos_extreme(&*arr, &mut rng);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_qwerty_azerty() {
        let arr = make_arr(&["quick", "welcome", "azerty"]);
        let mut rng = test_rng();
        let result = apply_qwerty_azerty(&*arr, &mut rng);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_typos_deterministic() {
        let arr = make_arr(&["hello", "world"]);
        let a = apply_typos_str(&*arr, &mut Rng::new(42), 2);
        let b = apply_typos_str(&*arr, &mut Rng::new(42), 2);
        let sa = a.as_string::<i32>();
        let sb = b.as_string::<i32>();
        assert_eq!(sa.value(0), sb.value(0));
        assert_eq!(sa.value(1), sb.value(1));
    }

    #[test]
    fn test_typos_short_strings() {
        let arr = make_arr(&["a", "ab", "", "hello"]);
        let mut rng = test_rng();
        let result = apply_typos_str(&*arr, &mut rng, 2);
        let s = result.as_string::<i32>();
        // "a" is too short (MIN_LEN_TYPO=2), should pass through
        assert_eq!(s.value(0), "a");
        // "ab" is length 2 → meets threshold
        // "" should pass through
        assert_eq!(s.value(2), "");
    }
}
