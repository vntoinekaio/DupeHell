// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringBuilder};

#[cfg(test)]
use arrow::array::{Array, AsArray};

use crate::context::Context;
use crate::rng::Rng;

// ── Character tables ──────────────────────────────────────────────────────

const ALPHA_UPPER_NO_IOQ: &[u8] = b"ABCDEFGHJKLMNPRSTUVWXYZ";
#[cfg(test)]
const ALPHA_UPPER: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";

// ── Helpers ───────────────────────────────────────────────────────────────

/// Build a StringArray by appending rows using a closure that writes into &mut Vec<u8>.
fn build_string_array<F>(n: usize, avg_width: usize, mut build_row: F) -> ArrayRef
where
    F: FnMut(&mut Vec<u8>),
{
    let mut builder = StringBuilder::with_capacity(n, n * avg_width);
    let mut buf = Vec::with_capacity(avg_width);
    for _ in 0..n {
        buf.clear();
        build_row(&mut buf);
        builder.append_value(std::str::from_utf8(&buf).unwrap());
    }
    Arc::new(builder.finish())
}

/// Return a random byte from `chars`.
fn rand_char(chars: &[u8], rng: &mut Rng) -> u8 {
    chars[rng.next_usize(chars.len())]
}

// ── buf_digits: generic digit string from integer ─────────────────────────

/// Produce an Arrow StringArray where each entry is `num` zero-padded to `width` digits.
/// If `watermark_mask` is Some, the last 3 digits are masked with the given value.
pub fn buf_digits(nums: &[u64], width: usize, watermark_mask: Option<u64>) -> ArrayRef {
    let n = nums.len();
    let mut builder = StringBuilder::with_capacity(n, n * width);
    let mut s = vec![b'0'; width];
    for num in nums {
        let mut val = *num;
        for j in (0..width).rev() {
            s[j] = b'0' + (val % 10) as u8;
            val /= 10;
        }
        if let Some(wm) = watermark_mask
            && width >= 3
        {
            let start = width - 3;
            s[start] = b'0' + ((wm / 100) % 10) as u8;
            s[start + 1] = b'0' + ((wm / 10) % 10) as u8;
            s[start + 2] = b'0' + (wm % 10) as u8;
        }
        builder.append_value(std::str::from_utf8(&s).unwrap());
    }
    Arc::new(builder.finish())
}

// ── bytes_strings: random fixed-length strings from a charset ─────────────

/// Generate random fixed-length strings from a byte character table.
pub fn bytes_strings(chars: &[u8], n: usize, length: usize, rng: &mut Rng) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(n, n * length);
    let mut s = vec![0u8; length];
    for _ in 0..n {
        for b in s.iter_mut() {
            *b = rand_char(chars, rng);
        }
        builder.append_value(std::str::from_utf8(&s).unwrap());
    }
    Arc::new(builder.finish())
}

/// Replace the last `n_digits` bytes of `buf` with the given value (0-padded).
fn watermark_buf(buf: &mut [u8], n_digits: usize, value: u64) {
    let start = buf.len() - n_digits;
    let mut v = value;
    for j in (0..n_digits).rev() {
        buf[start + j] = b'0' + (v % 10) as u8;
        v /= 10;
    }
}

// ── Buffer generators ─────────────────────────────────────────────────────

macro_rules! push_digit {
    ($buf:ident, $val:expr) => {
        $buf.push(b'0' + ($val as u8));
    };
}

/// `+1-XXX-XXX-XXXX` (16 bytes)
pub fn buf_phone(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let wm = ctx.watermark_3digits(0x50484f4e);
    build_string_array(n, 16, |buf| {
        let a = rng.next_usize(800) + 200; // 200-999
        let b = rng.next_usize(900) + 100; // 100-999
        let c = rng.next_usize(9000) + 1000; // 1000-9999
        buf.push(b'+');
        buf.push(b'1');
        buf.push(b'-');
        push_digit!(buf, a / 100);
        push_digit!(buf, (a / 10) % 10);
        push_digit!(buf, a % 10);
        buf.push(b'-');
        push_digit!(buf, b / 100);
        push_digit!(buf, (b / 10) % 10);
        push_digit!(buf, b % 10);
        buf.push(b'-');
        push_digit!(buf, c / 1000);
        push_digit!(buf, (c / 100) % 10);
        push_digit!(buf, (c / 10) % 10);
        push_digit!(buf, c % 10);
        watermark_buf(buf, 3, wm);
    })
}

/// `XXX-XX-XXXX` (11 bytes)
pub fn buf_ssn(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let wm = ctx.watermark_3digits(0x53534e);
    build_string_array(n, 11, |buf| {
        let a = rng.next_usize(900) + 100; // 100-999
        let b = rng.next_usize(90) + 10; // 10-99
        let c = rng.next_usize(9000) + 1000; // 1000-9999
        push_digit!(buf, a / 100);
        push_digit!(buf, (a / 10) % 10);
        push_digit!(buf, a % 10);
        buf.push(b'-');
        push_digit!(buf, b / 10);
        push_digit!(buf, b % 10);
        buf.push(b'-');
        push_digit!(buf, c / 1000);
        push_digit!(buf, (c / 100) % 10);
        push_digit!(buf, (c / 10) % 10);
        push_digit!(buf, c % 10);
        watermark_buf(buf, 3, wm);
    })
}

/// `userXXXXX@mail.com` (18 bytes)
pub fn buf_email(n: usize, rng: &mut Rng) -> ArrayRef {
    build_string_array(n, 18, |buf| {
        let user_num = rng.next_usize(90000) + 10000;
        buf.extend_from_slice(b"user");
        push_digit!(buf, user_num / 10000);
        push_digit!(buf, (user_num / 1000) % 10);
        push_digit!(buf, (user_num / 100) % 10);
        push_digit!(buf, (user_num / 10) % 10);
        push_digit!(buf, user_num % 10);
        buf.extend_from_slice(b"@mail.com");
    })
}

/// `XXX-XX-XXXXXX` (13 bytes, PAN)
pub fn buf_pan(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let wm = ctx.watermark_3digits(0x50414e);
    build_string_array(n, 13, |buf| {
        let a = rng.next_usize(900) + 100; // 100-999
        let b = rng.next_usize(90) + 10; // 10-99
        let c = rng.next_usize(900000) + 100000; // 100000-999999
        push_digit!(buf, a / 100);
        push_digit!(buf, (a / 10) % 10);
        push_digit!(buf, a % 10);
        buf.push(b'-');
        push_digit!(buf, b / 10);
        push_digit!(buf, b % 10);
        buf.push(b'-');
        push_digit!(buf, c / 100000);
        push_digit!(buf, (c / 10000) % 10);
        push_digit!(buf, (c / 1000) % 10);
        push_digit!(buf, (c / 100) % 10);
        push_digit!(buf, (c / 10) % 10);
        push_digit!(buf, c % 10);
        watermark_buf(buf, 3, wm);
    })
}

/// `XXXX-XXX-XXX` (12 bytes, Medicare)
pub fn buf_medicare(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let wm = ctx.watermark_3digits(0x4d4544);
    build_string_array(n, 12, |buf| {
        let a = rng.next_usize(9000) + 1000; // 1000-9999
        let b = rng.next_usize(900) + 100; // 100-999
        let c = rng.next_usize(900) + 100; // 100-999
        push_digit!(buf, a / 1000);
        push_digit!(buf, (a / 100) % 10);
        push_digit!(buf, (a / 10) % 10);
        push_digit!(buf, a % 10);
        buf.push(b'-');
        push_digit!(buf, b / 100);
        push_digit!(buf, (b / 10) % 10);
        push_digit!(buf, b % 10);
        buf.push(b'-');
        push_digit!(buf, c / 100);
        push_digit!(buf, (c / 10) % 10);
        push_digit!(buf, c % 10);
        watermark_buf(buf, 2, wm / 10);
    })
}

/// `+1-XXX-XXX-XXXX xXXXX` (21 bytes, office phone with extension)
pub fn buf_office_phone(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let wm = ctx.watermark_3digits(0x4f4643);
    build_string_array(n, 21, |buf| {
        let a = rng.next_usize(800) + 200; // 200-999
        let b = rng.next_usize(900) + 100; // 100-999
        let c = rng.next_usize(9000) + 1000; // 1000-9999
        let x = rng.next_usize(9900) + 100; // 100-9999
        buf.push(b'+');
        buf.push(b'1');
        buf.push(b'-');
        push_digit!(buf, a / 100);
        push_digit!(buf, (a / 10) % 10);
        push_digit!(buf, a % 10);
        buf.push(b'-');
        push_digit!(buf, b / 100);
        push_digit!(buf, (b / 10) % 10);
        push_digit!(buf, b % 10);
        buf.push(b'-');
        push_digit!(buf, c / 1000);
        push_digit!(buf, (c / 100) % 10);
        push_digit!(buf, (c / 10) % 10);
        push_digit!(buf, c % 10);
        buf.push(b' ');
        buf.push(b'x');
        push_digit!(buf, x / 1000);
        push_digit!(buf, (x / 100) % 10);
        push_digit!(buf, (x / 10) % 10);
        push_digit!(buf, x % 10);
        watermark_buf(buf, 3, wm);
    })
}

/// `LXXXXXXX` (8 bytes, passport: 1 letter + 7 digits)
pub fn buf_passport(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let wm = ctx.watermark_3digits(0x504153);
    build_string_array(n, 8, |buf| {
        let letter = rand_char(ALPHA_UPPER_NO_IOQ, rng);
        let nums = rng.next_usize(9000000) + 1000000;
        buf.push(letter);
        push_digit!(buf, nums / 1000000);
        push_digit!(buf, (nums / 100000) % 10);
        push_digit!(buf, (nums / 10000) % 10);
        push_digit!(buf, (nums / 1000) % 10);
        push_digit!(buf, (nums / 100) % 10);
        push_digit!(buf, (nums / 10) % 10);
        push_digit!(buf, nums % 10);
        watermark_buf(buf, 3, wm);
    })
}

/// `XXX-XX-XXXX` with X replacements (SSN last 4)
pub fn buf_ssn_last4(n: usize, rng: &mut Rng) -> ArrayRef {
    build_string_array(n, 11, |buf| {
        let nums = rng.next_usize(9000) + 1000;
        buf.extend_from_slice(b"XXX-XX-");
        push_digit!(buf, nums / 1000);
        push_digit!(buf, (nums / 100) % 10);
        push_digit!(buf, (nums / 10) % 10);
        push_digit!(buf, nums % 10);
    })
}

/// `****XXX` (7 bytes, masked account number)
pub fn buf_acct_num(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let wm = ctx.watermark_2digits(0x414354);
    build_string_array(n, 7, |buf| {
        let nums = rng.next_usize(900) + 100;
        buf.extend_from_slice(b"****");
        push_digit!(buf, nums / 100);
        push_digit!(buf, (nums / 10) % 10);
        push_digit!(buf, nums % 10);
        watermark_buf(buf, 2, wm);
    })
}

/// `LXXX` (4 bytes, branch code: letter + 3 digits)
pub fn buf_branch(n: usize, rng: &mut Rng) -> ArrayRef {
    build_string_array(n, 4, |buf| {
        let letter = rand_char(ALPHA_UPPER_NO_IOQ, rng);
        let nums = rng.next_usize(900) + 100;
        buf.push(letter);
        push_digit!(buf, nums / 100);
        push_digit!(buf, (nums / 10) % 10);
        push_digit!(buf, nums % 10);
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_rng() -> Rng {
        Rng::new(42)
    }

    #[test]
    fn test_buf_phone() {
        let mut rng = test_rng();
        let ctx = Context::test();
        let arr = buf_phone(5, &mut rng, &ctx);
        assert_eq!(arr.len(), 5);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            let v = s.value(i);
            assert_eq!(v.len(), 15, "phone[{i}] = {v:?}");
            assert!(v.starts_with("+1-"), "phone[{i}] = {v:?}");
        }
    }

    #[test]
    fn test_buf_ssn() {
        let mut rng = test_rng();
        let ctx = Context::test();
        let arr = buf_ssn(5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            assert_eq!(s.value(i).len(), 11, "ssn[{i}] = {:?}", s.value(i));
        }
    }

    #[test]
    fn test_buf_email() {
        let mut rng = test_rng();
        let arr = buf_email(5, &mut rng);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            let v = s.value(i);
            assert_eq!(v.len(), 18, "email[{i}] = {v:?}");
            assert!(v.ends_with("@mail.com"));
            assert!(v.starts_with("user"));
        }
    }

    #[test]
    fn test_buf_pan() {
        let mut rng = test_rng();
        let ctx = Context::test();
        let arr = buf_pan(5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            assert_eq!(s.value(i).len(), 13, "pan[{i}] = {:?}", s.value(i));
        }
    }

    #[test]
    fn test_buf_medicare() {
        let mut rng = test_rng();
        let ctx = Context::test();
        let arr = buf_medicare(5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            assert_eq!(s.value(i).len(), 12, "medicare[{i}] = {:?}", s.value(i));
        }
    }

    #[test]
    fn test_buf_office_phone() {
        let mut rng = test_rng();
        let ctx = Context::test();
        let arr = buf_office_phone(5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            let v = s.value(i);
            assert_eq!(v.len(), 21, "office_phone[{i}] = {v:?}");
            assert!(v.contains(" x"), "office_phone[{i}] = {v:?}");
        }
    }

    #[test]
    fn test_buf_passport() {
        let mut rng = test_rng();
        let ctx = Context::test();
        let arr = buf_passport(5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            let v = s.value(i);
            assert_eq!(v.len(), 8, "passport[{i}] = {v:?}");
            assert!(v.as_bytes()[0].is_ascii_uppercase());
            for j in 1..8 {
                assert!(v.as_bytes()[j].is_ascii_digit(), "passport[{i}] = {v:?}");
            }
        }
    }

    #[test]
    fn test_buf_ssn_last4() {
        let mut rng = test_rng();
        let arr = buf_ssn_last4(5, &mut rng);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            let v = s.value(i);
            assert_eq!(v.len(), 11, "ssn_last4[{i}] = {v:?}");
            assert!(v.starts_with("XXX-XX-"));
            assert!(v.as_bytes()[3] == b'-' && v.as_bytes()[6] == b'-');
            for j in 7..11 {
                assert!(v.as_bytes()[j].is_ascii_digit(), "ssn_last4[{i}] = {v:?}");
            }
        }
    }

    #[test]
    fn test_buf_acct_num() {
        let mut rng = test_rng();
        let ctx = Context::test();
        let arr = buf_acct_num(5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            let v = s.value(i);
            assert_eq!(v.len(), 7, "acct_num[{i}] = {v:?}");
            assert!(v.starts_with("****"));
            assert!(v.as_bytes()[4].is_ascii_digit());
        }
    }

    #[test]
    fn test_buf_branch() {
        let mut rng = test_rng();
        let arr = buf_branch(5, &mut rng);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            let v = s.value(i);
            assert_eq!(v.len(), 4, "branch[{i}] = {v:?}");
            assert!(v.as_bytes()[0].is_ascii_uppercase());
            for j in 1..4 {
                assert!(v.as_bytes()[j].is_ascii_digit(), "branch[{i}] = {v:?}");
            }
        }
    }

    #[test]
    fn test_bytes_strings() {
        let mut rng = test_rng();
        let arr = bytes_strings(ALPHA_UPPER, 10, 5, &mut rng);
        let s = arr.as_string::<i32>();
        assert_eq!(s.len(), 10);
        for i in 0..10 {
            let v = s.value(i);
            assert_eq!(v.len(), 5, "bytes_strings[{i}] = {v:?}");
            for c in v.bytes() {
                assert!(c.is_ascii_uppercase(), "bytes_strings[{i}] bad byte = {c}");
            }
        }
    }

    #[test]
    fn test_buf_digits() {
        let nums = vec![123, 45, 7890, 0, 9999];
        let arr = buf_digits(&nums, 4, None);
        let s = arr.as_string::<i32>();
        assert_eq!(s.value(0), "0123");
        assert_eq!(s.value(1), "0045");
        assert_eq!(s.value(2), "7890");
        assert_eq!(s.value(3), "0000");
        assert_eq!(s.value(4), "9999");
    }

    #[test]
    fn test_deterministic() {
        let mut rng_a = Rng::new(42);
        let mut rng_b = Rng::new(42);
        let ctx = Context::test();
        let a = buf_phone(100, &mut rng_a, &ctx);
        let b = buf_phone(100, &mut rng_b, &ctx);
        let a_arr = a.as_string::<i32>();
        let b_arr = b.as_string::<i32>();
        for i in 0..100 {
            assert_eq!(a_arr.value(i), b_arr.value(i), "mismatch at {i}");
        }
    }
}
