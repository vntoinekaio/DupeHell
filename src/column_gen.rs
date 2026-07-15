// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array};

use crate::buf_gen::build_string_array;
use crate::context::Context;
use crate::fast_template::{get_template, write_zpad};
use crate::pool_lookup::{guess_pool_name, pool_values, strip_prefix};
use crate::rng::Rng;

/// Column type for dispatch.
#[derive(Debug, Clone, PartialEq)]
pub enum ColType {
    String,
    Int,
    Float,
    Boolean,
    Date,
    Datetime,
}

/// Simplified column definition for generation.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColType,
    pub pool_name: Option<String>,
    pub nullable: bool,
    pub null_rate: f64,
}

#[cfg(test)]
impl ColumnDef {
    pub fn new(name: &str, col_type: ColType) -> Self {
        Self {
            name: name.to_string(),
            col_type,
            pool_name: None,
            nullable: true,
            null_rate: 0.0,
        }
    }

    pub fn with_pool(mut self, pool: &str) -> Self {
        self.pool_name = Some(pool.to_string());
        self
    }
}

// ── Int range lookup ──────────────────────────────────────────────────────

fn int_range(name: &str) -> Option<(i64, i64)> {
    let clean = name.to_lowercase().replace(' ', "_").replace('_', "");
    for (pattern, (lo, hi)) in INT_RANGES {
        if clean.contains(pattern) {
            return Some((*lo, *hi));
        }
    }
    None
}

static INT_RANGES: &[(&str, (i64, i64))] = &[
    ("yearbuilt", (1900, 2024)),
    ("creditscore", (300, 850)),
    ("credits", (1, 6)),
    ("starrating", (1, 5)),
    ("rating", (1, 5)),
    ("leadscore", (0, 100)),
    ("daysonmarket", (0, 365)),
    ("bedrooms", (0, 20)),
    ("taxyear", (2018, 2024)),
    ("dependentcount", (0, 12)),
    ("headcount", (5, 5000)),
    ("inventoryquantity", (0, 500)),
    ("quantityonhand", (0, 5000)),
    ("quantity", (1, 50)),
    ("maxenrollment", (10, 500)),
    ("totalrooms", (10, 2000)),
    ("lifetimestays", (0, 500)),
    ("pointsbalance", (0, 500000)),
    ("squarefeet", (300, 10000)),
    ("lotsize", (1000, 500000)),
    ("dataallowance", (1, 100)),
    ("durationseconds", (5, 3600)),
    ("storagecapacity", (1000, 500000)),
    ("reorderpoint", (10, 1000)),
    ("itemcount", (10, 50000)),
    ("employeecount", (5, 10000)),
    ("numemployees", (5, 10000)),
];

// ── Float range lookup ────────────────────────────────────────────────────

fn float_range(name: &str) -> Option<(f64, f64)> {
    let clean = name.to_lowercase().replace(' ', "_").replace('_', "");
    for (pattern, (lo, hi)) in FLOAT_RANGES {
        if clean.contains(pattern) {
            return Some((*lo, *hi));
        }
    }
    None
}

static FLOAT_RANGES: &[(&str, (f64, f64))] = &[
    ("bathroom", (1.0, 20.0)),
    ("gpa", (0.0, 4.0)),
    ("latitude", (-90.0, 90.0)),
    ("longitude", (-180.0, 180.0)),
    ("commissionrate", (0.0, 100.0)),
    ("ontimerate", (0.0, 100.0)),
    ("ownershippercent", (0.0, 100.0)),
    ("mortgageamount", (50000.0, 2_000_000.0)),
    ("saleprice", (50000.0, 5_000_000.0)),
    ("listingprice", (50000.0, 5_000_000.0)),
    ("assessedvalue", (50000.0, 3_000_000.0)),
    ("premiumamount", (200.0, 20000.0)),
    ("deductible", (100.0, 10000.0)),
    ("shippingprice", (0.0, 50.0)),
    ("monthlycharge", (10.0, 300.0)),
    ("discountamount", (0.0, 500.0)),
    ("budget", (100000.0, 50_000_000.0)),
    ("grossincome", (10000.0, 1_000_000.0)),
    ("monthlyamount", (200.0, 5000.0)),
    ("salary", (20000.0, 500000.0)),
    ("grosspay", (500.0, 20000.0)),
    ("netpay", (400.0, 15000.0)),
    ("deductions", (50.0, 5000.0)),
    ("hoursworked", (0.0, 200.0)),
    ("totalcharge", (50.0, 5000.0)),
    ("unitcost", (0.50, 500.0)),
    ("totalweight", (0.1, 50000.0)),
    ("coveragelimit", (50000.0, 5_000_000.0)),
    ("claimamount", (500.0, 500000.0)),
    ("settlementamount", (0.0, 500000.0)),
    ("currentbalance", (-10000.0, 1_000_000.0)),
    ("amount", (0.01, 100000.0)),
    ("subtotalprice", (5.0, 5000.0)),
    ("totalprice", (10.0, 6000.0)),
    ("totaltax", (0.0, 500.0)),
    ("price", (1.0, 5000.0)),
    ("unitprice", (1.0, 2000.0)),
    ("hourlyrate", (15.0, 500.0)),
];

// ── Type generators ───────────────────────────────────────────────────────

fn gen_int64(col_name: &str, n: usize, rng: &mut Rng) -> ArrayRef {
    let (lo, hi) = int_range(col_name).unwrap_or((0, 100000));
    let range = (hi - lo + 1) as usize;
    let mut builder = Int64Array::builder(n);
    for _ in 0..n {
        builder.append_value(lo + rng.next_usize(range) as i64);
    }
    Arc::new(builder.finish())
}

fn gen_float64(col_name: &str, n: usize, rng: &mut Rng) -> ArrayRef {
    let (lo, hi) = float_range(col_name).unwrap_or((0.0, 10000.0));
    let is_gpa = col_name.to_lowercase().contains("gpa");
    let mut builder = arrow::array::Float64Builder::with_capacity(n);
    if is_gpa {
        for _ in 0..n {
            let v = (rng.next_f64() * (hi - lo) + lo) * 100.0;
            builder.append_value(v.round() / 100.0);
        }
    } else {
        for _ in 0..n {
            builder.append_value(rng.next_f64() * (hi - lo) + lo);
        }
    }
    Arc::new(builder.finish())
}

fn gen_boolean(n: usize, rng: &mut Rng) -> ArrayRef {
    let mut builder = BooleanArray::builder(n);
    for _ in 0..n {
        builder.append_value(rng.next_usize(2) == 0);
    }
    Arc::new(builder.finish())
}

fn days_in_month(year: i64, month: usize) -> usize {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => unreachable!(),
    }
}

fn gen_date(n: usize, rng: &mut Rng) -> ArrayRef {
    build_string_array(n, 10, |buf| {
        let y = 1940 + rng.next_usize(66);
        let m = rng.next_usize(12) + 1;
        let max_days = days_in_month(y as i64, m);
        let d = rng.next_usize(max_days) + 1;
        write_zpad(buf, y, 4);
        buf.push(b'-');
        write_zpad(buf, m, 2);
        buf.push(b'-');
        write_zpad(buf, d, 2);
    })
}

fn gen_datetime(n: usize, rng: &mut Rng) -> ArrayRef {
    build_string_array(n, 19, |buf| {
        let y = 2020 + rng.next_usize(6);
        let m = rng.next_usize(12) + 1;
        let max_days = days_in_month(y as i64, m);
        let d = rng.next_usize(max_days) + 1;
        let hr = rng.next_usize(24);
        write_zpad(buf, y, 4);
        buf.push(b'-');
        write_zpad(buf, m, 2);
        buf.push(b'-');
        write_zpad(buf, d, 2);
        buf.push(b' ');
        write_zpad(buf, hr, 2);
        buf.extend_from_slice(b":00:00");
    })
}

// ── Null mask ─────────────────────────────────────────────────────────────

/// Generate a boolean mask where `rate` fraction are true (null).
pub fn generate_null_mask(n: usize, rate: f64, rng: &mut Rng) -> BooleanArray {
    if rate <= 0.0 {
        return BooleanArray::from(vec![false; n]);
    }
    let mut builder = BooleanArray::builder(n);
    for _ in 0..n {
        builder.append_value(rng.next_f64() < rate);
    }
    builder.finish()
}

/// Apply nulls to any Arrow array based on rate.
/// Works for all column types (String, Date/Datetime as strings, Int, Float, Boolean).
pub fn apply_null_rate(arr: &dyn arrow::array::Array, rate: f64, rng: &mut Rng) -> ArrayRef {
    if rate <= 0.0 {
        return arr.slice(0, arr.len());
    }
    let n = arr.len();
    let raw_mask = generate_null_mask(n, rate, rng);
    // Ensure first element is not null (Polars schema inference reads row 0).
    let mask = if raw_mask.value(0) {
        let swap_idx = (1..n).find(|&i| !raw_mask.value(i));
        let mut builder = BooleanArray::builder(n);
        for i in 0..n {
            let is_null = if i == 0 {
                false
            } else if Some(i) == swap_idx {
                true
            } else {
                raw_mask.value(i)
            };
            builder.append_value(is_null);
        }
        builder.finish()
    } else {
        raw_mask
    };
    arrow::compute::nullif(arr, &mask).expect("nullif: mask length must match array length")
}

// ── Main dispatch ─────────────────────────────────────────────────────────

/// Generate an Arrow array for a single column.
pub fn generate_column(col: &ColumnDef, n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    // Pre-compute normalized name once (avoids alloc per template lookup)
    let norm_name = col.name.to_lowercase().replace(' ', "_");

    // Stage 1: Fast template match
    if let Some(template) = get_template(&norm_name) {
        let arr = template(n, rng, ctx);
        if col.nullable && col.null_rate > 0.0 {
            return apply_null_rate(&*arr, col.null_rate, rng);
        }
        return arr;
    }

    // Stage 2: Stripped prefix + template
    let stripped = strip_prefix(&col.name);
    if stripped != norm_name
        && let Some(template) = get_template(&stripped)
    {
        let arr = template(n, rng, ctx);
        if col.nullable && col.null_rate > 0.0 {
            return apply_null_rate(&*arr, col.null_rate, rng);
        }
        return arr;
    }

    // Stage 3: Type-based dispatch
    let arr: ArrayRef = match col.col_type {
        ColType::Boolean => gen_boolean(n, rng),
        ColType::Int => gen_int64(&col.name, n, rng),
        ColType::Float => gen_float64(&col.name, n, rng),
        ColType::Date => gen_date(n, rng),
        ColType::Datetime => gen_datetime(n, rng),
        ColType::String => {
            // Pool lookup
            if let Some(ref pn) = col.pool_name {
                pool_values(pn, n, rng, ctx)
            } else if let Some(pn) = guess_pool_name(&col.name) {
                pool_values(pn, n, rng, ctx)
            } else if col.name.ends_with("_id") || col.name == "id" {
                // _id fallback (after pool lookup, before word fallback).
                // `i` is a per-batch row index (< BATCH_SIZE = 500_000), so
                // it always fits within the 7-digit zero-padded width below
                // — `write_zpad` truncates rather than widens, unlike
                // `format!("{:07}", i)`, but that only matters past 10M.
                let prefix: String = col.name.chars().take(4).collect::<String>().to_uppercase();
                let mut i: usize = 0;
                build_string_array(n, 12, |buf| {
                    buf.extend_from_slice(prefix.as_bytes());
                    buf.push(b'-');
                    write_zpad(buf, i, 7);
                    i += 1;
                })
            } else {
                // Fallback: "word" pool
                pool_values("word", n, rng, ctx)
            }
        }
    };
    if col.nullable && col.null_rate > 0.0 {
        apply_null_rate(&*arr, col.null_rate, rng)
    } else {
        arr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};

    fn test_ctx() -> Context {
        let pools_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("assets/pools");
        Context::new("kyc", "en", pools_dir.to_str().unwrap()).unwrap()
    }

    fn test_rng() -> Rng {
        Rng::new(42)
    }

    #[test]
    fn test_int_range() {
        let (lo, hi) = int_range("credit_score").unwrap();
        assert_eq!(lo, 300);
        assert_eq!(hi, 850);
        assert_eq!(int_range("unknown"), None);
    }

    #[test]
    fn test_float_range() {
        let (lo, hi) = float_range("sale_price").unwrap();
        assert!((lo - 50000.0).abs() < 1.0);
        assert!((hi - 5_000_000.0).abs() < 1.0);
        assert_eq!(float_range("unknown"), None);
    }

    #[test]
    fn test_gen_int64_default() {
        let mut rng = test_rng();
        let arr = gen_int64("unknown_col", 10, &mut rng);
        let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.len(), 10);
        for i in 0..10 {
            let v = a.value(i);
            assert!((0..100000).contains(&v), "int64[{i}] = {v}");
        }
    }

    #[test]
    fn test_gen_int64_ranged() {
        let mut rng = test_rng();
        let arr = gen_int64("credit_score", 10, &mut rng);
        let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.len(), 10);
        for i in 0..10 {
            let v = a.value(i);
            assert!((300..=850).contains(&v), "credit_score[{i}] = {v}");
        }
    }

    #[test]
    fn test_gen_float64() {
        let mut rng = test_rng();
        let arr = gen_float64("sale_price", 10, &mut rng);
        let a = arr
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(a.len(), 10);
        for i in 0..10 {
            let v = a.value(i);
            assert!(
                (50000.0..=5_000_000.0).contains(&v),
                "sale_price[{i}] = {v}"
            );
        }
    }

    #[test]
    fn test_gen_boolean() {
        let mut rng = test_rng();
        let arr = gen_boolean(10, &mut rng);
        let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(a.len(), 10);
    }

    #[test]
    fn test_gen_date() {
        let mut rng = test_rng();
        let arr = gen_date(10, &mut rng);
        let s = arr.as_string::<i32>();
        assert_eq!(s.len(), 10);
        for i in 0..10 {
            let v = s.value(i);
            assert_eq!(v.len(), 10, "date[{i}] = {v:?}");
            assert!(
                v.starts_with("19") || v.starts_with("20"),
                "date[{i}] = {v:?}"
            );
        }
    }

    #[test]
    fn test_null_mask() {
        let mut rng = test_rng();
        let mask = generate_null_mask(1000, 0.5, &mut rng);
        let count = (0..1000).filter(|&i| mask.value(i)).count();
        assert!(count > 300 && count < 700, "null count = {count}");
    }

    #[test]
    fn test_null_mask_zero_rate() {
        let mut rng = test_rng();
        let mask = generate_null_mask(100, 0.0, &mut rng);
        for i in 0..100 {
            assert!(!mask.value(i), "null at {i} with 0 rate");
        }
    }

    #[test]
    fn test_generate_column_template() {
        let ctx = test_ctx();
        let mut rng = test_rng();
        let col = ColumnDef::new("phone", ColType::String);
        let arr = generate_column(&col, 5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        assert_eq!(s.len(), 5);
        for i in 0..5 {
            assert!(s.value(i).starts_with("+1-"), "phone[{i}]");
        }
    }

    #[test]
    fn test_generate_column_id() {
        let ctx = test_ctx();
        let mut rng = test_rng();
        let col = ColumnDef::new("person_id", ColType::String);
        let arr = generate_column(&col, 5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        assert_eq!(s.len(), 5);
        assert!(s.value(0).starts_with("PERS"), "id[{0}] = {:?}", s.value(0));
    }

    #[test]
    fn test_generate_column_int() {
        let ctx = test_ctx();
        let mut rng = test_rng();
        let col = ColumnDef::new("credit_score", ColType::Int);
        let arr = generate_column(&col, 10, &mut rng, &ctx);
        let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.len(), 10);
        for i in 0..10 {
            let v = a.value(i);
            assert!((300..=850).contains(&v), "credit_score[{i}] = {v}");
        }
    }

    #[test]
    fn test_generate_column_pool() {
        let ctx = test_ctx();
        let mut rng = test_rng();
        let col = ColumnDef::new("first_name", ColType::String).with_pool("first_name");
        let arr = generate_column(&col, 10, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        assert_eq!(s.len(), 10);
        for i in 0..10 {
            assert!(!s.value(i).is_empty(), "first_name[{i}] empty");
        }
    }

    #[test]
    fn test_generate_column_guess_pool() {
        let ctx = test_ctx();
        let mut rng = test_rng();
        // "city" should match guess_pool_name → "city" pool
        let col = ColumnDef::new("city", ColType::String);
        let arr = generate_column(&col, 5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        assert_eq!(s.len(), 5);
        for i in 0..5 {
            assert!(!s.value(i).is_empty(), "city[{i}] empty");
        }
    }

    #[test]
    fn test_apply_null_rate() {
        use arrow::array::StringArray;
        let mut rng = test_rng();
        let src = Arc::new(StringArray::from(vec!["a"; 100])) as ArrayRef;
        let result = apply_null_rate(&*src, 0.3, &mut rng);
        let s = result.as_string::<i32>();
        let null_count = (0..100).filter(|&i| s.is_null(i)).count();
        assert!(
            null_count > 10 && null_count < 60,
            "null count = {null_count}"
        );
        // First element should never be null
        assert!(s.is_valid(0), "first element should not be null");
    }

    #[test]
    fn test_deterministic() {
        let ctx = test_ctx();
        let col = ColumnDef::new("phone", ColType::String);
        let a = generate_column(&col, 100, &mut Rng::new(42), &ctx);
        let b = generate_column(&col, 100, &mut Rng::new(42), &ctx);
        let sa = a.as_string::<i32>();
        let sb = b.as_string::<i32>();
        for i in 0..100 {
            assert_eq!(sa.value(i), sb.value(i), "mismatch at {i}");
        }
    }

    #[test]
    fn test_gen_date_valid() {
        use std::collections::HashSet;
        let mut rng = test_rng();
        let arr = gen_date(10000, &mut rng);
        let s = arr.as_string::<i32>();
        assert_eq!(s.len(), 10000);
        let mut months_seen = HashSet::new();
        let mut days_seen = HashSet::new();
        for i in 0..10000 {
            let v = s.value(i);
            assert_eq!(v.len(), 10, "date[{i}] = {v:?}");
            let parts: Vec<&str> = v.split('-').collect();
            let y: i64 = parts[0].parse().unwrap();
            let m: usize = parts[1].parse().unwrap();
            let d: usize = parts[2].parse().unwrap();
            assert!((1940..=2005).contains(&y), "year {y} out of range");
            assert!((1..=12).contains(&m), "month {m} out of range");
            let max = match m {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                2 => {
                    if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                        29
                    } else {
                        28
                    }
                }
                _ => unreachable!(),
            };
            assert!(d >= 1 && d <= max, "date[{i}] = {v}: day {d} > max {max}");
            months_seen.insert(m);
            days_seen.insert(d);
        }
        assert!(months_seen.len() >= 11, "only saw months: {months_seen:?}");
        assert!(days_seen.len() >= 28, "only saw days: {days_seen:?}");
    }

    #[test]
    fn test_gen_datetime_varied() {
        use std::collections::HashSet;
        let mut rng = test_rng();
        let arr = gen_datetime(10000, &mut rng);
        let s = arr.as_string::<i32>();
        assert_eq!(s.len(), 10000);
        let mut months_seen = HashSet::new();
        let mut days_seen = HashSet::new();
        for i in 0..10000 {
            let v = s.value(i);
            assert_eq!(v.len(), 19, "datetime[{i}] = {v:?}");
            let date_part = &v[..10];
            let parts: Vec<&str> = date_part.split('-').collect();
            let _y: i64 = parts[0].parse().unwrap();
            let m: usize = parts[1].parse().unwrap();
            let d: usize = parts[2].parse().unwrap();
            months_seen.insert(m);
            days_seen.insert(d);
        }
        assert!(months_seen.len() >= 11, "only saw months: {months_seen:?}");
        assert!(days_seen.len() >= 28, "only saw days: {days_seen:?}");
    }

    #[test]
    fn test_department_id_from_pool() {
        let ctx = test_ctx();
        let mut rng = test_rng();
        let col = ColumnDef::new("department_id", ColType::String);
        let arr = generate_column(&col, 100, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        assert_eq!(s.len(), 100);
        for i in 0..100 {
            let v = s.value(i);
            assert!(
                !v.starts_with("DEPA-"),
                "department_id[{i}] = {v:?} (should be pool value, not generated ID)"
            );
            assert!(!v.is_empty(), "department_id[{i}] empty");
        }
    }
}
