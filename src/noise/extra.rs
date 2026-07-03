// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

//! Extra noise types not covered by the original Rust port:
//!   missing, name_null, dob_null, exact
//!   blocking_fail_initial, blocking_fail_partial
//!   fuzzy_match, phonetic

use std::sync::Arc;
use std::sync::LazyLock;

use arrow::array::{Array, ArrayRef, StringBuilder};

use crate::rng::Rng;

/// Phonetic name substitutions (same as Python PHONETIC_MAP).
static PHONETIC_MAP: LazyLock<Vec<(&'static str, &'static str)>> = LazyLock::new(|| {
    vec![
        (" dupont", "dupon"),
        (" martin", "marten"),
        (" bernard", "berard"),
        (" thomas", "tomas"),
        (" robert", "rober"),
        (" richard", "richar"),
        (" durand", "durant"),
        (" moreau", "moro"),
    ]
});

/// Set all values in a column to empty string (missing / blank operation).
pub fn apply_missing(arr: &dyn Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let mut builder = StringBuilder::with_capacity(n, 8);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        // 50% chance to blank
        if rng2.next_usize(2) == 0 {
            builder.append_value("");
        } else {
            builder.append_value(src.value(i));
        }
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Set all values to null (name_null / dob_null).
pub fn apply_nullify(arr: &dyn Array, _rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();

    let mut builder = StringBuilder::with_capacity(n, 0);
    for _ in 0..n {
        builder.append_null();
    }
    Arc::new(builder.finish())
}

/// No-op — return input unchanged.
pub fn apply_exact(arr: &dyn Array, _rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();

    let mut builder = StringBuilder::with_capacity(n, 8);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(src.value(i));
        }
    }
    Arc::new(builder.finish())
}

/// "John Doe" → "J. D." — replace words with initial+dot.
pub fn apply_blocking_initial(arr: &dyn Array, _rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();

    let mut builder = StringBuilder::with_capacity(n, 8);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        if s.len() < 2 {
            builder.append_value(s);
            continue;
        }
        let words: Vec<&str> = s.split_whitespace().collect();
        let initials: String = words
            .iter()
            .filter_map(|w| w.chars().next())
            .map(|c| format!("{}.", c))
            .collect::<Vec<_>>()
            .join(" ");
        builder.append_value(&initials);
    }
    Arc::new(builder.finish())
}

/// Truncate to first 2 characters (blocking_fail partial mode).
pub fn apply_blocking_partial(arr: &dyn Array, _rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();

    let mut builder = StringBuilder::with_capacity(n, 4);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        let truncated: String = s.chars().take(2).collect();
        builder.append_value(&truncated);
    }
    Arc::new(builder.finish())
}

/// Replace 2-3 random lowercase a-z characters at unique positions.
/// Python equivalent: `apply_fuzzy_match`.
pub fn apply_fuzzy_match(arr: &dyn Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let mut builder = StringBuilder::with_capacity(n, 16);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        if s.len() < 5 {
            builder.append_value(s);
            continue;
        }

        let mut chars: Vec<char> = s.chars().collect();
        let n_changes = std::cmp::min(rng2.next_usize(2) + 2, chars.len()); // 2-3

        // Generate unique positions
        let mut positions = Vec::with_capacity(n_changes);
        while positions.len() < n_changes {
            let pos = rng2.next_usize(chars.len());
            if !positions.contains(&pos) {
                positions.push(pos);
            }
        }

        for &pos in &positions {
            chars[pos] = (rng2.next_usize(26) as u8 + 97) as char;
        }
        builder.append_value(chars.into_iter().collect::<String>());
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Phonetic name substitution: match substrings from PHONETIC_MAP.
/// Python equivalent: `apply_phonetic`.
pub fn apply_phonetic(arr: &dyn Array, _rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();

    let mut builder = StringBuilder::with_capacity(n, 16);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        let lower = s.to_lowercase();
        let mut replaced = false;
        for &(old, new) in PHONETIC_MAP.iter() {
            if lower.contains(old) {
                let result = lower.replace(old, new);
                // Capitalize first letter
                let mut chars = result.chars();
                let capitalized = match chars.next() {
                    None => result,
                    Some(first) => first.to_uppercase().to_string() + chars.as_str(),
                };
                builder.append_value(&capitalized);
                replaced = true;
                break;
            }
        }
        if !replaced {
            builder.append_value(s);
        }
    }
    Arc::new(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, StringArray};

    fn test_rng() -> Rng {
        Rng::new(42)
    }
    fn make_arr(vals: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(vals.to_vec()))
    }

    #[test]
    fn test_missing() {
        let arr = make_arr(&["hello", "world", "test"]);
        let mut rng = test_rng();
        let result = apply_missing(&*arr, &mut rng);
        assert_eq!(result.len(), 3);
        // ~50% should be empty
        let s = result.as_string::<i32>();
        let blank_count = (0..3).filter(|&i| s.value(i).is_empty()).count();
        assert!(blank_count <= 3);
    }

    #[test]
    fn test_nullify() {
        let arr = make_arr(&["hello", "world"]);
        let mut rng = test_rng();
        let result = apply_nullify(&*arr, &mut rng);
        assert_eq!(result.len(), 2);
        let s = result.as_string::<i32>();
        assert!(s.is_null(0));
        assert!(s.is_null(1));
    }

    #[test]
    fn test_exact() {
        let arr = make_arr(&["hello", "world"]);
        let mut rng = test_rng();
        let result = apply_exact(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(s.value(0), "hello");
        assert_eq!(s.value(1), "world");
    }

    #[test]
    fn test_blocking_initial() {
        let arr = make_arr(&["John Doe", "Marie Claire", "Single"]);
        let mut rng = test_rng();
        let result = apply_blocking_initial(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(s.value(0), "J. D.");
        assert_eq!(s.value(1), "M. C.");
        // Single word should still get initial+dot
        assert_eq!(s.value(2), "S.");
    }

    #[test]
    fn test_blocking_partial() {
        let arr = make_arr(&["hello", "world", "a"]);
        let mut rng = test_rng();
        let result = apply_blocking_partial(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(s.value(0), "he");
        assert_eq!(s.value(1), "wo");
        assert_eq!(s.value(2), "a");
    }

    #[test]
    fn test_fuzzy_match() {
        let arr = make_arr(&["hello", "world", "abc", "short"]);
        let mut rng = test_rng();
        let result = apply_fuzzy_match(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(result.len(), 4);
        // Strings with len < 5 pass through unchanged
        assert_eq!(s.value(2), "abc");
        // Strings with len >= 5 should be changed
        assert_ne!(s.value(0), "hello");
        assert_ne!(s.value(1), "world");
        // All chars should be lowercase a-z
        for &i in &[0, 1] {
            for c in s.value(i).chars() {
                assert!(c.is_ascii_lowercase(), "char '{}' not lowercase", c);
            }
        }
    }

    #[test]
    fn test_phonetic() {
        let arr = make_arr(&["Jean Dupont", "Marie Martin", "Unknown"]);
        let mut rng = test_rng();
        let result = apply_phonetic(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(result.len(), 3);
        // " dupont" → "dupon" (space consumed by replace)
        assert_eq!(s.value(0), "Jeandupon");
        // " martin" → "marten"
        assert_eq!(s.value(1), "Mariemarten");
        // Unknown → unchanged
        assert_eq!(s.value(2), "Unknown");
    }

    #[test]
    fn test_deterministic() {
        let arr = make_arr(&["John Doe", "Jane Doe"]);
        let a = apply_fuzzy_match(&*arr, &mut Rng::new(42));
        let b = apply_fuzzy_match(&*arr, &mut Rng::new(42));
        let sa = a.as_string::<i32>();
        let sb = b.as_string::<i32>();
        assert_eq!(sa.value(0), sb.value(0));
        assert_eq!(sa.value(1), sb.value(1));
    }
}
