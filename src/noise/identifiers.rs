// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder};

use crate::rng::Rng;

/// Corrupt email: 33% trim domain, 33% replace domain, 33% mask first 3 chars.
pub fn corrupt_email(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let strategies: Vec<usize> = (0..n).map(|_| rng2.next_usize(3)).collect();

    let mut builder = StringBuilder::with_capacity(n, 24);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        let at_pos = s.find('@');
        if at_pos.is_none() {
            builder.append_value(s);
            continue;
        }
        let at = at_pos.unwrap();
        let local = &s[..at];
        let domain = &s[at + 1..];

        let result = match strategies[i] {
            0 => {
                // Trim domain: "gmail.com" → "gmali.com" / "hotmail" → "hotmai"
                let dot_pos = domain.find('.');
                if let Some(dot) = dot_pos {
                    let name = &domain[..dot];
                    let tld = &domain[dot..];
                    if name.len() > 2 {
                        // Corrupt last char of name
                        let mut name_chars: Vec<char> = name.chars().collect();
                        let last = name_chars.len() - 1;
                        name_chars[last] = (rng2.next_usize(26) as u8 + 97) as char;
                        format!(
                            "{}@{}{}",
                            local,
                            name_chars.into_iter().collect::<String>(),
                            tld
                        )
                    } else {
                        s.to_string()
                    }
                } else {
                    s.to_string()
                }
            }
            1 => {
                // Replace domain with alternative
                let alt_domain =
                    ["yahoo.com", "outlook.com", "proton.me", "mail.com"][rng2.next_usize(4)];
                format!("{}@{}", local, alt_domain)
            }
            _ => {
                // Mask first 3 chars: "abcdef" → "abc***"
                if local.len() > 3 {
                    format!("{}***@{}", &local[..3], domain)
                } else {
                    format!("{}***@{}", local, domain)
                }
            }
        };
        builder.append_value(&result);
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Corrupt phone: 40% space-separated pairs, 30% digit corruption + "+33 ",
/// 30% "+33 (XX) XXX-XXXX" format.
pub fn corrupt_phone(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let strategies: Vec<usize> = (0..n).map(|_| rng2.next_usize(10)).collect();

    let mut builder = StringBuilder::with_capacity(n, 20);
    for i in 0..n {
        if src.is_null(i) {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        // Extract digits
        let digits: Vec<char> = s.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.len() < 6 {
            builder.append_value(s);
            continue;
        }

        let result = match strategies[i] {
            0..=3 => {
                // Space-separated pairs
                let mut out = String::with_capacity(digits.len() + digits.len() / 2);
                for (j, &d) in digits.iter().enumerate() {
                    if j > 0 && j % 2 == 0 {
                        out.push(' ');
                    }
                    out.push(d);
                }
                out
            }
            4..=6 => {
                // Digit corruption + "+33 " prefix
                let mut corrupted: Vec<char> = digits.clone();
                let n_corrupt = rng2.next_usize(3) + 1;
                for _ in 0..n_corrupt {
                    if corrupted.is_empty() {
                        break;
                    }
                    let pos = rng2.next_usize(corrupted.len());
                    corrupted[pos] = (rng2.next_usize(10) as u8 + 48) as char;
                }
                format!("+33 {}", corrupted.into_iter().collect::<String>())
            }
            _ => {
                // "+33 (XX) XXX-XXXX" format
                let mut out = String::from("+33 (");
                for (j, &d) in digits.iter().enumerate() {
                    if j == 2 {
                        out.push_str(") ");
                    }
                    if j == 5 {
                        out.push('-');
                    }
                    out.push(d);
                }
                out
            }
        };
        builder.append_value(&result);
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Single-position corruption: digit → (digit + delta) mod 10, letter → random letter.
pub fn corrupt_national_id(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
    use arrow::array::AsArray;
    let src = arr.as_string::<i32>();
    let n = src.len();
    let mut rng2 = rng.fork();

    let mut builder = StringBuilder::with_capacity(n, 16);
    for i in 0..n {
        if src.is_null(i) || src.value(i).is_empty() {
            builder.append_null();
            continue;
        }
        let s = src.value(i);
        let mut chars: Vec<char> = s.chars().collect();
        let pos = rng2.next_usize(chars.len());
        let c = chars[pos];
        if c.is_ascii_digit() {
            let d = c as u8 - 48;
            let delta = rng2.next_usize(9) as u8 + 1;
            chars[pos] = ((d + delta) % 10 + 48) as char;
        } else if c.is_ascii_alphabetic() {
            chars[pos] =
                (rng2.next_usize(26) as u8 + if c.is_ascii_uppercase() { 65 } else { 97 }) as char;
        }
        builder.append_value(&chars.into_iter().collect::<String>());
    }
    *rng = rng2;
    Arc::new(builder.finish())
}

/// Replace last digit with random (for 9-digit SIREN-like numbers).
pub fn corrupt_siren(arr: &dyn arrow::array::Array, rng: &mut Rng) -> ArrayRef {
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
        let digits: Vec<char> = s.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.len() == 9 {
            let mut result: Vec<char> = s.chars().collect();
            // Find the position of the 9th digit from the end
            let mut digit_count = 0;
            for j in (0..result.len()).rev() {
                if result[j].is_ascii_digit() {
                    digit_count += 1;
                    if digit_count == 9 {
                        result[j] = (rng2.next_usize(10) as u8 + 48) as char;
                        break;
                    }
                }
            }
            builder.append_value(&result.into_iter().collect::<String>());
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
    use arrow::array::{Array, AsArray, StringArray};

    fn test_rng() -> Rng {
        Rng::new(42)
    }
    fn make_arr(vals: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(vals.to_vec()))
    }

    #[test]
    fn test_corrupt_email() {
        let arr = make_arr(&["john@gmail.com", "jane@hotmail.com", "test@yahoo.com"]);
        let mut rng = test_rng();
        let result = corrupt_email(&*arr, &mut rng);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_corrupt_phone() {
        let arr = make_arr(&["+1-555-123-4567", "+1-555-987-6543"]);
        let mut rng = test_rng();
        let result = corrupt_phone(&*arr, &mut rng);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_corrupt_national_id() {
        let arr = make_arr(&["123-45-6789", "ABC-12-DEF"]);
        let mut rng = test_rng();
        let result = corrupt_national_id(&*arr, &mut rng);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_corrupt_siren() {
        let arr = make_arr(&["123456789", "987654321"]);
        let mut rng = test_rng();
        let result = corrupt_siren(&*arr, &mut rng);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_deterministic() {
        let arr = make_arr(&["john@gmail.com", "jane@hotmail.com"]);
        let a = corrupt_email(&*arr, &mut Rng::new(42));
        let b = corrupt_email(&*arr, &mut Rng::new(42));
        let sa = a.as_string::<i32>();
        let sb = b.as_string::<i32>();
        assert_eq!(sa.value(0), sb.value(0));
    }
}
