// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, StringBuilder, UInt64Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde::Deserialize;

use crate::rng::Rng;

#[derive(Debug, Deserialize)]
struct HnConfig {
    #[serde(default)]
    pattern: String,
    #[serde(default)]
    id_fields: Vec<String>,
    #[serde(default)]
    attr_fields: Vec<String>,
    #[serde(default)]
    mix_field: String,
    #[serde(default = "default_f")]
    first_name_col: String,
    #[serde(default = "default_l")]
    last_name_col: String,
    #[serde(default = "default_d")]
    dob_col: String,
    #[serde(default = "default_e")]
    email_col: String,
    #[serde(default = "default_p")]
    phone_col: String,
    #[serde(default = "default_s")]
    ssn_col: String,
    #[serde(default)]
    address_fields: Vec<String>,
}

fn default_f() -> String {
    "first_name".into()
}
fn default_l() -> String {
    "last_name".into()
}
fn default_d() -> String {
    "date_of_birth".into()
}
fn default_e() -> String {
    "email".into()
}
fn default_p() -> String {
    "phone".into()
}
fn default_s() -> String {
    "ssn".into()
}

/// Generate hard negatives from a pool of base records.
///
/// # Arguments
/// * `pool` - A `RecordBatch` containing all base columns for an entity type
/// * `config_json` - JSON string with `pattern`, `id_fields`, `attr_fields`, etc.
/// * `count` - Number of hard negative records to generate
/// * `seed` - RNG seed
pub fn generate_hard_negatives(
    pool: &RecordBatch,
    config_json: &str,
    count: usize,
    seed: u64,
) -> Result<RecordBatch, String> {
    let config: HnConfig =
        serde_json::from_str(config_json).map_err(|e| format!("HN config parse error: {e}"))?;

    let n_base = pool.num_rows();
    if n_base < 2 {
        return Err("Pool too small for hard negatives (need ≥2)".into());
    }
    if count == 0 {
        return Err("Count must be > 0".into());
    }
    let n = count.min(n_base / 2);

    let mut rng = Rng::new(seed);

    let idx_a_raw: Vec<usize> = (0..n).map(|_| rng.next_usize(n_base)).collect();
    let mut idx_b_raw: Vec<usize> = (0..n).map(|_| rng.next_usize(n_base)).collect();
    for i in 0..n {
        if idx_b_raw[i] == idx_a_raw[i] {
            idx_b_raw[i] = (idx_b_raw[i] + 1) % n_base;
        }
    }
    // Built once here instead of once per field inside `col_take` (id_fields
    // + attr_fields each re-derive the same indices today).
    let idx_a = UInt64Array::from_iter_values(idx_a_raw.iter().map(|&i| i as u64));
    let idx_b = UInt64Array::from_iter_values(idx_b_raw.iter().map(|&i| i as u64));

    match config.pattern.as_str() {
        "same_field" => same_field(pool, &config, &idx_a, &idx_b, n),
        "mix_identifier" => mix_identifier(pool, &config, &idx_a, &idx_b, &mut rng, n),
        "same_name_different_everything" => {
            factory_same_name_diff(pool, &config, &idx_a, &idx_b, n)
        }
        "same_email" => factory_same_email(pool, &config, &idx_a, &idx_b, n),
        "same_ssn" => factory_same_ssn(pool, &config, &idx_a, &idx_b, n),
        "same_phone" => factory_same_phone(pool, &config, &idx_a, &idx_b, n),
        "same_address" => factory_same_address(pool, &config, &idx_a, &idx_b, n),
        "same_name_dob" => factory_same_name_dob(pool, &config, &idx_a, &idx_b, n),
        _ => Err(format!("Unknown HN pattern: {}", config.pattern)),
    }
}

// ── Core primitive ────────────────────────────────────────────────

/// `hn_same_field`: copy `id_fields` from A, `attr_fields` from B.
fn same_field(
    pool: &RecordBatch,
    config: &HnConfig,
    idx_a: &UInt64Array,
    idx_b: &UInt64Array,
    n: usize,
) -> Result<RecordBatch, String> {
    let id_set: HashSet<&str> = config.id_fields.iter().map(|s| s.as_str()).collect();
    let mut columns: Vec<ArrayRef> = Vec::new();
    let mut fields: Vec<Field> = Vec::new();

    for f in &config.id_fields {
        let col = col_take(pool, f, idx_a, n)?;
        let dt = col.data_type().clone();
        fields.push(Field::new(f, dt, true));
        columns.push(col);
    }
    for f in &config.attr_fields {
        if id_set.contains(f.as_str()) {
            continue;
        }
        let col = col_take(pool, f, idx_b, n)?;
        let dt = col.data_type().clone();
        fields.push(Field::new(f, dt, true));
        columns.push(col);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| format!("RecordBatch: {e}"))
}

/// `hn_mix_identifier`: 50/50 row-wise mix of one field, attr_fields from B.
fn mix_identifier(
    pool: &RecordBatch,
    config: &HnConfig,
    idx_a: &UInt64Array,
    idx_b: &UInt64Array,
    rng: &mut Rng,
    n: usize,
) -> Result<RecordBatch, String> {
    let mix = &config.mix_field;
    let col_a = col_take(pool, mix, idx_a, n)?;
    let col_b = col_take(pool, mix, idx_b, n)?;
    let mixed = mix_arrays(&col_a, &col_b, rng);

    let id_set: HashSet<&str> = [mix.as_str()].into();
    let mut columns: Vec<ArrayRef> = vec![mixed];
    let mut fields: Vec<Field> = vec![Field::new(mix, col_a.data_type().clone(), true)];

    for f in &config.attr_fields {
        if id_set.contains(f.as_str()) {
            continue;
        }
        let col = col_take(pool, f, idx_b, n)?;
        let dt = col.data_type().clone();
        fields.push(Field::new(f, dt, true));
        columns.push(col);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| format!("RecordBatch: {e}"))
}

// ── Factory functions (delegates to same_field) ───────────────────

fn factory_same_name_diff(
    pool: &RecordBatch,
    config: &HnConfig,
    idx_a: &UInt64Array,
    idx_b: &UInt64Array,
    n: usize,
) -> Result<RecordBatch, String> {
    let c = HnConfig {
        id_fields: vec![config.first_name_col.clone(), config.last_name_col.clone()],
        attr_fields: config.attr_fields.clone(),
        ..config.clone()
    };
    same_field(pool, &c, idx_a, idx_b, n)
}

fn factory_same_email(
    pool: &RecordBatch,
    config: &HnConfig,
    idx_a: &UInt64Array,
    idx_b: &UInt64Array,
    n: usize,
) -> Result<RecordBatch, String> {
    let c = HnConfig {
        id_fields: vec![config.email_col.clone()],
        attr_fields: config.attr_fields.clone(),
        ..config.clone()
    };
    same_field(pool, &c, idx_a, idx_b, n)
}

fn factory_same_ssn(
    pool: &RecordBatch,
    config: &HnConfig,
    idx_a: &UInt64Array,
    idx_b: &UInt64Array,
    n: usize,
) -> Result<RecordBatch, String> {
    let c = HnConfig {
        id_fields: vec![config.ssn_col.clone()],
        attr_fields: config.attr_fields.clone(),
        ..config.clone()
    };
    same_field(pool, &c, idx_a, idx_b, n)
}

fn factory_same_phone(
    pool: &RecordBatch,
    config: &HnConfig,
    idx_a: &UInt64Array,
    idx_b: &UInt64Array,
    n: usize,
) -> Result<RecordBatch, String> {
    let c = HnConfig {
        id_fields: vec![config.phone_col.clone()],
        attr_fields: config.attr_fields.clone(),
        ..config.clone()
    };
    same_field(pool, &c, idx_a, idx_b, n)
}

fn factory_same_address(
    pool: &RecordBatch,
    config: &HnConfig,
    idx_a: &UInt64Array,
    idx_b: &UInt64Array,
    n: usize,
) -> Result<RecordBatch, String> {
    let fields = if config.address_fields.is_empty() {
        vec!["address_line1".into(), "postal_code".into()]
    } else {
        config.address_fields.clone()
    };
    let c = HnConfig {
        id_fields: fields,
        attr_fields: config.attr_fields.clone(),
        ..config.clone()
    };
    same_field(pool, &c, idx_a, idx_b, n)
}

fn factory_same_name_dob(
    pool: &RecordBatch,
    config: &HnConfig,
    idx_a: &UInt64Array,
    idx_b: &UInt64Array,
    n: usize,
) -> Result<RecordBatch, String> {
    let c = HnConfig {
        id_fields: vec![
            config.first_name_col.clone(),
            config.last_name_col.clone(),
            config.dob_col.clone(),
        ],
        attr_fields: config.attr_fields.clone(),
        ..config.clone()
    };
    same_field(pool, &c, idx_a, idx_b, n)
}

// ── Helpers ───────────────────────────────────────────────────────

fn col_take(
    pool: &RecordBatch,
    name: &str,
    indices: &UInt64Array,
    _n: usize,
) -> Result<ArrayRef, String> {
    let col = pool
        .column_by_name(name)
        .ok_or_else(|| format!("Column '{name}' not found in pool"))?;
    take(col.as_ref(), indices, None).map_err(|e| format!("take({name}): {e}"))
}

/// Interleave two equal-length arrays 50/50 based on random bits.
#[allow(clippy::if_same_then_else)]
fn mix_arrays(a: &ArrayRef, b: &ArrayRef, rng: &mut Rng) -> ArrayRef {
    let n = a.len();
    if a.data_type() != &DataType::Utf8 {
        return a.slice(0, n);
    }
    let sa = a.as_string::<i32>();
    let sb = b.as_string::<i32>();
    let mut builder = StringBuilder::with_capacity(n, n * 24);
    for i in 0..n {
        if sa.is_null(i) && sb.is_null(i) {
            builder.append_null();
        } else if sa.is_null(i) {
            builder.append_value(sb.value(i));
        } else if sb.is_null(i) {
            builder.append_value(sa.value(i));
        } else if rng.next_usize(2) == 0 {
            builder.append_value(sa.value(i));
        } else {
            builder.append_value(sb.value(i));
        }
    }
    Arc::new(builder.finish())
}

impl Clone for HnConfig {
    fn clone(&self) -> Self {
        Self {
            pattern: self.pattern.clone(),
            id_fields: self.id_fields.clone(),
            attr_fields: self.attr_fields.clone(),
            mix_field: self.mix_field.clone(),
            first_name_col: self.first_name_col.clone(),
            last_name_col: self.last_name_col.clone(),
            dob_col: self.dob_col.clone(),
            email_col: self.email_col.clone(),
            phone_col: self.phone_col.clone(),
            ssn_col: self.ssn_col.clone(),
            address_fields: self.address_fields.clone(),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    fn make_pool() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("given_name", DataType::Utf8, true),
            Field::new("family_name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
            Field::new("ssn", DataType::Utf8, true),
            Field::new("phone", DataType::Utf8, true),
            Field::new("birth_date", DataType::Utf8, true),
            Field::new("address_line1", DataType::Utf8, true),
            Field::new("postal_code", DataType::Utf8, true),
            Field::new("city", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));
        let given_name = StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);
        let family_name = StringArray::from(vec!["Smith", "Jones", "Brown", "Taylor", "Wilson"]);
        let email = StringArray::from(vec!["a@x.com", "b@x.com", "c@x.com", "d@x.com", "e@x.com"]);
        let ssn = StringArray::from(vec![
            "111-11-1111",
            "222-22-2222",
            "333-33-3333",
            "444-44-4444",
            "555-55-5555",
        ]);
        let phone = StringArray::from(vec![
            "555-0101", "555-0102", "555-0103", "555-0104", "555-0105",
        ]);
        let birth_date = StringArray::from(vec![
            "1990-01-01",
            "1985-05-15",
            "2000-12-25",
            "1975-03-20",
            "1995-07-07",
        ]);
        let address_line1 = StringArray::from(vec![
            "1 Main St",
            "2 Oak Ave",
            "3 Pine Rd",
            "4 Elm Dr",
            "5 Maple Ln",
        ]);
        let postal_code = StringArray::from(vec!["10001", "10002", "10003", "10004", "10005"]);
        let city = StringArray::from(vec!["NYC", "LA", "Chicago", "Houston", "Phoenix"]);
        let age = Int64Array::from(vec![34, 39, 24, 49, 29]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(given_name),
                Arc::new(family_name),
                Arc::new(email),
                Arc::new(ssn),
                Arc::new(phone),
                Arc::new(birth_date),
                Arc::new(address_line1),
                Arc::new(postal_code),
                Arc::new(city),
                Arc::new(age),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_same_field_basic() {
        let pool = make_pool();
        let config = r#"{"pattern":"same_field","id_fields":["email"],"attr_fields":["given_name","family_name","birth_date"]}"#;
        let rb = generate_hard_negatives(&pool, config, 3, 42).unwrap();
        assert_eq!(rb.num_rows(), 2); // clamped to n_base/2 = 2
        assert_eq!(rb.num_columns(), 4); // email + given_name + family_name + birth_date
    }

    #[test]
    fn test_same_field_email() {
        let pool = make_pool();
        let config = r#"{"pattern":"same_field","id_fields":["email","ssn"],"attr_fields":["given_name","family_name","birth_date","city"]}"#;
        let rb = generate_hard_negatives(&pool, config, 3, 42).unwrap();
        assert_eq!(rb.num_rows(), 2); // clamped to n_base/2 = 2
        // id_fields(2) + attr_fields(4) minus overlap = 6
        assert_eq!(rb.num_columns(), 6);
        assert!(rb.column_by_name("email").is_some());
        assert!(rb.column_by_name("ssn").is_some());
        assert!(rb.column_by_name("given_name").is_some());
        assert!(rb.column_by_name("city").is_some());
    }

    #[test]
    fn test_mix_identifier() {
        let pool = make_pool();
        let config = r#"{"pattern":"mix_identifier","mix_field":"ssn","attr_fields":["given_name","family_name","birth_date"]}"#;
        let rb = generate_hard_negatives(&pool, config, 10, 42).unwrap();
        assert_eq!(rb.num_rows(), 2); // n_base/2 = 5/2 = 2
        assert_eq!(rb.num_columns(), 4);
        assert!(rb.column_by_name("ssn").is_some());
    }

    #[test]
    fn test_same_name_diff_everything() {
        let pool = make_pool();
        let config = r#"{"pattern":"same_name_different_everything","first_name_col":"given_name","last_name_col":"family_name","attr_fields":["email","birth_date","address_line1","city"]}"#;
        let rb = generate_hard_negatives(&pool, config, 2, 42).unwrap();
        assert_eq!(rb.num_rows(), 2);
        assert_eq!(rb.num_columns(), 6);
    }

    #[test]
    fn test_same_email() {
        let pool = make_pool();
        let config = r#"{"pattern":"same_email","email_col":"email","attr_fields":["given_name","family_name","birth_date"]}"#;
        let rb = generate_hard_negatives(&pool, config, 2, 42).unwrap();
        assert_eq!(rb.num_rows(), 2);
    }

    #[test]
    fn test_same_ssn() {
        let pool = make_pool();
        let config =
            r#"{"pattern":"same_ssn","ssn_col":"ssn","attr_fields":["given_name","family_name"]}"#;
        let rb = generate_hard_negatives(&pool, config, 2, 42).unwrap();
        assert_eq!(rb.num_rows(), 2);
    }

    #[test]
    fn test_same_phone() {
        let pool = make_pool();
        let config = r#"{"pattern":"same_phone","phone_col":"phone","attr_fields":["given_name","family_name"]}"#;
        let rb = generate_hard_negatives(&pool, config, 2, 42).unwrap();
        assert_eq!(rb.num_rows(), 2);
    }

    #[test]
    fn test_same_address() {
        let pool = make_pool();
        let config = r#"{"pattern":"same_address","address_fields":["address_line1","postal_code","city"],"attr_fields":["given_name","family_name","email"]}"#;
        let rb = generate_hard_negatives(&pool, config, 2, 42).unwrap();
        assert_eq!(rb.num_rows(), 2);
    }

    #[test]
    fn test_same_name_dob() {
        let pool = make_pool();
        let config = r#"{"pattern":"same_name_dob","first_name_col":"given_name","last_name_col":"family_name","dob_col":"birth_date","attr_fields":["email","phone","address_line1","city"]}"#;
        let rb = generate_hard_negatives(&pool, config, 2, 42).unwrap();
        assert_eq!(rb.num_rows(), 2);
    }

    #[test]
    fn test_deterministic() {
        let pool = make_pool();
        let config = r#"{"pattern":"same_field","id_fields":["email"],"attr_fields":["given_name","family_name"]}"#;
        let a = generate_hard_negatives(&pool, config, 5, 42).unwrap();
        let b = generate_hard_negatives(&pool, config, 5, 42).unwrap();
        assert_eq!(a.num_rows(), b.num_rows());
        let sa = a.column_by_name("email").unwrap().as_string::<i32>();
        let sb = b.column_by_name("email").unwrap().as_string::<i32>();
        for i in 0..a.num_rows() {
            assert_eq!(sa.value(i), sb.value(i));
        }
    }

    #[test]
    fn test_pool_too_small() {
        let pool = make_pool();
        let config =
            r#"{"pattern":"same_field","id_fields":["email"],"attr_fields":["given_name"]}"#;
        let result = generate_hard_negatives(&pool, config, 0, 42);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_pattern() {
        let pool = make_pool();
        let config =
            r#"{"pattern":"nonexistent","id_fields":["email"],"attr_fields":["given_name"]}"#;
        let result = generate_hard_negatives(&pool, config, 2, 42);
        assert!(result.is_err());
    }
}
