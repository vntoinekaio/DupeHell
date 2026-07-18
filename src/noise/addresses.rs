// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder};

use crate::rng::Rng;

/// Ordered (not a HashMap) so iteration order — and therefore RNG
/// consumption in `apply_language_mix` — is deterministic across processes.
/// `std::collections::HashMap` randomizes its iteration order per process
/// (RandomState), which previously made `apply_language_mix` non-reproducible
/// across runs sharing the same `--seed`.
static FR_TO_EN: &[(&str, &str)] = &[
    ("Rue", "Street"),
    ("Avenue", "Avenue"),
    ("Boulevard", "Boulevard"),
    ("Place", "Square"),
    ("Chemin", "Road"),
    ("Route", "Route"),
    ("Allée", "Lane"),
    ("Passage", "Passage"),
    ("de la", "of the"),
    ("du", "of the"),
    ("des", "of the"),
];

/// Randomize the street number portion of an address.
pub fn apply_address_scramble(arr: &dyn Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let mut builder = StringBuilder::with_capacity(n, n * 32);
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
        let parts: Vec<&str> = s.splitn(2, ' ').collect();
        if parts.len() >= 2 && parts[0].chars().all(|c| c.is_ascii_digit()) {
            let new_num = rng2.next_usize(998) + 1; // 1..999
            builder.append_value(format!("{} {}", new_num, parts[1]));
        } else {
            builder.append_value(s);
        }
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// French → English street type substitution (30% per token).
pub fn apply_language_mix(arr: &dyn Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let mut builder = StringBuilder::with_capacity(n, n * 32);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        let mut result = s.to_string();
        for &(fr, en) in FR_TO_EN.iter() {
            if result.contains(fr) && rng2.next_f64() < 0.3 {
                result = result.replacen(fr, en, 1);
            }
        }
        builder.append_value(&result);
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Corrupt one digit position in a postal code.
pub fn apply_postal_corrupt(arr: &dyn Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let mut builder = StringBuilder::with_capacity(n, n * 16);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        let mut chars: Vec<char> = s.chars().collect();
        if chars.len() < 5 {
            builder.append_value(s);
            continue;
        }
        // Find a digit position to corrupt: count digits in one pass, draw
        // an index among them, then locate that one digit in a second pass
        // — avoids materializing a `Vec<usize>` of every digit position.
        let digit_count = chars.iter().filter(|c| c.is_ascii_digit()).count();
        if digit_count == 0 {
            builder.append_value(s);
            continue;
        }
        let target = rng2.next_usize(digit_count);
        let pos = chars
            .iter()
            .enumerate()
            .filter(|(_, c)| c.is_ascii_digit())
            .nth(target)
            .map(|(j, _)| j)
            .expect("target < digit_count");
        let new_digit = (rng2.next_usize(10) as u8 + 48) as char;
        chars[pos] = new_digit;
        builder.append_value(chars.into_iter().collect::<String>());
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
    fn test_address_scramble() {
        let arr = make_arr(&["123 Main Street", "42 Rue de Paris", "NoNumber", ""]);
        let mut rng = test_rng();
        let result = apply_address_scramble(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(result.len(), 4);
        // Street number should be changed (likely different)
        assert_ne!(s.value(0), "123 Main Street");
        assert_ne!(s.value(1), "42 Rue de Paris");
        // No number prefix → pass through
        assert_eq!(s.value(2), "NoNumber");
    }

    #[test]
    fn test_language_mix() {
        let arr = make_arr(&["Rue de la Paix", "Avenue des Champs", "English Road"]);
        let mut rng = test_rng();
        let result = apply_language_mix(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(result.len(), 3);
        // at least some should be modified (30% per token)
        assert_ne!(s.value(0), "English Road");
    }

    #[test]
    fn test_postal_corrupt() {
        let arr = make_arr(&["75001", "12345", "ABC", "ABCDE"]);
        let mut rng = test_rng();
        let result = apply_postal_corrupt(&*arr, &mut rng);
        let s = result.as_string::<i32>();
        assert_eq!(result.len(), 4);
        // "ABC" has no digits → pass through
        assert_eq!(s.value(2), "ABC");
    }

    #[test]
    fn test_deterministic() {
        let arr = make_arr(&["123 Main St", "456 Oak Ave"]);
        let a = apply_address_scramble(&*arr, &mut Rng::new(42));
        let b = apply_address_scramble(&*arr, &mut Rng::new(42));
        let sa = a.as_string::<i32>();
        let sb = b.as_string::<i32>();
        assert_eq!(sa.value(0), sb.value(0));
    }
}
