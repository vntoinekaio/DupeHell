// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder};

use crate::rng::Rng;

static LEGAL_FORM_SUFFIXES: &[&str] = &[" SA", " SARL", " SAS", " EURL", " SNC", " SCA"];

/// Drop legal form suffixes from company names.
pub fn drop_legal_form(arr: &dyn Array) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();

    let mut builder = StringBuilder::with_capacity(n, 32);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        let mut stripped = s.to_string();
        for &suff in LEGAL_FORM_SUFFIXES {
            if stripped.ends_with(suff) {
                stripped.truncate(stripped.len() - suff.len());
                break;
            }
        }
        builder.append_value(&stripped);
    }
    Arc::new(builder.finish())
}

/// Randomly drop 1+ words from multi-word company names.
pub fn apply_word_dropout(arr: &dyn Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let mut builder = StringBuilder::with_capacity(n, 32);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        let words: Vec<&str> = s.split_whitespace().collect();
        if words.len() <= 1 {
            builder.append_value(s);
            continue;
        }
        let max_remove = (words.len() / 2).max(1);
        let n_remove = rng2.next_usize(max_remove) + 1;
        let n_remove = n_remove.min(words.len() - 1);
        if n_remove == 0 {
            builder.append_value(s);
            continue;
        }
        // Generate random indices to remove
        let mut indices: Vec<usize> = (0..words.len()).collect();
        // Fisher-Yates partial shuffle
        for j in (1..words.len()).rev() {
            let k = rng2.next_usize(j + 1);
            indices.swap(j, k);
        }
        let mut remaining: Vec<&str> = Vec::with_capacity(words.len() - n_remove);
        for j in 0..words.len() {
            if j >= n_remove {
                remaining.push(words[indices[j]]);
            }
        }
        if remaining.is_empty() {
            builder.append_value(s);
        } else {
            builder.append_value(&remaining.join(" "));
        }
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Shuffle the word order of company names.
pub fn apply_company_scramble(arr: &dyn Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let mut builder = StringBuilder::with_capacity(n, 32);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        let mut words: Vec<&str> = s.split_whitespace().collect();
        if words.len() <= 1 {
            builder.append_value(s);
            continue;
        }
        // Fisher-Yates shuffle
        for j in (1..words.len()).rev() {
            let k = rng2.next_usize(j + 1);
            words.swap(j, k);
        }
        builder.append_value(&words.join(" "));
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Create acronym from random words + 2-digit number.
pub fn apply_acronym(arr: &dyn Array, rng: &mut Rng) -> ArrayRef {
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
        let words: Vec<&str> = s.split_whitespace().collect();
        if words.len() <= 1 {
            // Short name: take first 6 chars + number
            let prefix: String = s.chars().take(6).collect();
            let num = rng2.next_usize(90) + 10;
            builder.append_value(&format!("{prefix}{num}"));
            continue;
        }
        let n_words = rng2.next_usize(2) + 2; // 2-3 words
        let n_words = n_words.min(words.len());
        let mut selected: Vec<&str> = Vec::with_capacity(n_words);
        let mut used: Vec<bool> = vec![false; words.len()];
        for _ in 0..n_words {
            loop {
                let idx = rng2.next_usize(words.len());
                if !used[idx] {
                    selected.push(words[idx]);
                    used[idx] = true;
                    break;
                }
            }
        }
        let initials: String = selected
            .iter()
            .filter_map(|w| w.chars().next())
            .map(|c| c.to_uppercase().next().unwrap_or(c))
            .collect();
        let num = rng2.next_usize(90) + 10;
        builder.append_value(&format!("{initials}{num}"));
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
    fn test_drop_legal_form() {
        let arr = make_arr(&["Acme SARL", "Global SA", "NoSuffix", "Test SAS"]);
        let result = drop_legal_form(&*arr);
        let s = result.as_string::<i32>();
        assert_eq!(s.value(0), "Acme");
        assert_eq!(s.value(1), "Global");
        assert_eq!(s.value(2), "NoSuffix");
        assert_eq!(s.value(3), "Test");
    }

    #[test]
    fn test_word_dropout() {
        let arr = make_arr(&["Acme Corporation International", "Single", "A B C D"]);
        let mut rng = test_rng();
        let result = apply_word_dropout(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(result.len(), 3);
        // Single word passes through
        assert_eq!(s.value(1), "Single");
        // Multi-word should have fewer words
        assert!(s.value(0).split_whitespace().count() < 3);
    }

    #[test]
    fn test_company_scramble() {
        let arr = make_arr(&["Acme Corp International", "Single"]);
        let mut rng = test_rng();
        let result = apply_company_scramble(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(result.len(), 2);
        // Single word passes through
        assert_eq!(s.value(1), "Single");
        // Multi-word should have different order (high probability)
        let words: Vec<&str> = s.value(0).split_whitespace().collect();
        assert_eq!(words.len(), 3);
    }

    #[test]
    fn test_acronym() {
        let arr = make_arr(&["Acme Corporation", "Single", "International Business Machines"]);
        let mut rng = test_rng();
        let result = apply_acronym(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(result.len(), 3);
        // Should have uppercase letters + 2 digits
        let last = s.value(0);
        let digits: String = last.chars().filter(|c| c.is_ascii_digit()).collect();
        assert_eq!(digits.len(), 2);
    }

    #[test]
    fn test_deterministic() {
        let arr = make_arr(&["Acme Corp", "Global Inc"]);
        let a = apply_company_scramble(&*arr, &mut Rng::new(42));
        let b = apply_company_scramble(&*arr, &mut Rng::new(42));
        let sa = a.as_string::<i32>();
        let sb = b.as_string::<i32>();
        assert_eq!(sa.value(0), sb.value(0));
    }
}
