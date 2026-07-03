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

static NICKNAMES_MAP: LazyLock<HashMap<&'static str, Vec<&'static str>>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert("Marie", vec!["Marie", "Marie", "MARY", "Maite"]);
    m.insert("Jean", vec!["Jean", "JEAN", "J.", "JM"]);
    m.insert("Pierre", vec!["Pierre", "Pierrot", "Pi", "PY"]);
    m.insert("François", vec!["François", "Francois", "FR", "F."]);
    m
});

static MALE_NAMES: &[&str] = &[
    "Jean",
    "Pierre",
    "Michel",
    "François",
    "Laurent",
    "Bruno",
    "Philippe",
    "Nicolas",
    "Alexandre",
    "Thomas",
    "Kevin",
    "Samuel",
    "Romain",
    "Julien",
    "Maxime",
    "Benoît",
    "Christophe",
    "Stéphane",
    "Sébastien",
    "Olivier",
];

static FEMALE_NAMES: &[&str] = &[
    "Marie",
    "Catherine",
    "Sophie",
    "Isabelle",
    "Nathalie",
    "Christine",
    "Valérie",
    "Françoise",
    "Muriel",
    "Michèle",
    "Patricia",
    "Anne",
    "Sandrine",
    "Céline",
    "Aurélie",
    "Julie",
    "Emilie",
    "Laura",
    "Pauline",
    "Anna",
];

/// Replace names with random nickname variants (50%) or uppercase (50%).
pub fn apply_nickname(arr: &dyn Array, rng: &mut Rng) -> ArrayRef {
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
        if let Some(alternatives) = NICKNAMES_MAP.get(s) {
            if rng2.next_usize(2) == 0 {
                let idx = rng2.next_usize(alternatives.len());
                builder.append_value(alternatives[idx]);
            } else {
                builder.append_value(&s.to_uppercase());
            }
        } else {
            builder.append_value(s);
        }
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Convert multi-word names to initials (e.g. "Jean Dupont" → "JD").
pub fn apply_initials(arr: &dyn Array) -> ArrayRef {
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
        if s.contains(' ') {
            let initials: String = s
                .split_whitespace()
                .filter_map(|w| w.chars().next())
                .map(|c| c.to_uppercase().next().unwrap_or(c))
                .collect();
            builder.append_value(&initials);
        } else {
            builder.append_value(s);
        }
    }
    Arc::new(builder.finish())
}

/// Truncate to random length (2..min(4, len)) characters.
pub fn apply_partial(arr: &dyn Array, rng: &mut Rng) -> ArrayRef {
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
        let s = src.value(i);
        if s.len() < 3 {
            builder.append_value(s);
            continue;
        }
        let max_len = 4.min(s.len());
        let n_chars = rng2.next_usize(max_len - 2) + 2; // 2..max_len
        let truncated: String = s.chars().take(n_chars).collect();
        builder.append_value(&truncated);
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Merge multi-word names: 50% concatenate first two words, 50% first + last 3 chars.
pub fn apply_name_compound(arr: &dyn Array, rng: &mut Rng) -> ArrayRef {
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
        if !s.contains(' ') {
            builder.append_value(s);
            continue;
        }
        let words: Vec<&str> = s.split_whitespace().collect();
        if words.len() < 2 {
            builder.append_value(s);
            continue;
        }
        if rng2.next_usize(2) == 0 {
            builder.append_value(&format!("{}{}", words[0], words[1]));
        } else {
            let last = words[words.len() - 1];
            let suffix: String = last.chars().take(3).collect();
            builder.append_value(&format!("{}{}", words[0], suffix));
        }
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Swap male ↔ female names at 50% probability.
pub fn apply_gender_swap(arr: &dyn Array, rng: &mut Rng) -> ArrayRef {
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
        let r = rng2.next_f64();
        if MALE_NAMES.contains(&s) && r < 0.5 {
            let idx = rng2.next_usize(FEMALE_NAMES.len());
            builder.append_value(FEMALE_NAMES[idx]);
        } else if FEMALE_NAMES.contains(&s) && r < 0.5 {
            let idx = rng2.next_usize(MALE_NAMES.len());
            builder.append_value(MALE_NAMES[idx]);
        } else {
            builder.append_value(s);
        }
    }
    *rng = rng2;
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
    fn test_nickname() {
        let arr = make_arr(&["Marie", "Jean", "Pierre", "Unknown"]);
        let mut rng = test_rng();
        let result = apply_nickname(&*arr, &mut rng);
        assert_eq!(result.len(), 4);
        // Known names should be transformed
        let s = result.as_string::<i32>();
        assert_ne!(s.value(0), "Marie"); // 50% chance of variant or uppercase
    }

    #[test]
    fn test_initials() {
        let arr = make_arr(&["Jean Dupont", "Marie Claire", "Single", ""]);
        let result = apply_initials(&*arr);
        let s = result.as_string::<i32>();
        assert_eq!(s.value(0), "JD");
        assert_eq!(s.value(1), "MC");
        assert_eq!(s.value(2), "Single");
    }

    #[test]
    fn test_partial() {
        let arr = make_arr(&["hello", "world", "ab", ""]);
        let mut rng = test_rng();
        let result = apply_partial(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(result.len(), 4);
        assert!(s.value(0).len() < 5);
    }

    #[test]
    fn test_name_compound() {
        let arr = make_arr(&["Jean Pierre", "Marie Claire", "Single"]);
        let mut rng = test_rng();
        let result = apply_name_compound(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(result.len(), 3);
        // Compound names should have no spaces
        assert!(!s.value(0).contains(' '));
        assert!(!s.value(1).contains(' '));
        // Single word passes through
        assert_eq!(s.value(2), "Single");
    }

    #[test]
    fn test_gender_swap() {
        let arr = make_arr(&["Jean", "Marie", "Unknown", "Pierre"]);
        let mut rng = test_rng();
        let result = apply_gender_swap(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(result.len(), 4);
        // Unknown should pass through unchanged
        assert_eq!(s.value(2), "Unknown");
    }

    #[test]
    fn test_deterministic() {
        let arr = make_arr(&["Marie", "Jean", "Pierre"]);
        let a = apply_nickname(&*arr, &mut Rng::new(42));
        let b = apply_nickname(&*arr, &mut Rng::new(42));
        let sa = a.as_string::<i32>();
        let sb = b.as_string::<i32>();
        assert_eq!(sa.value(0), sb.value(0));
    }
}
