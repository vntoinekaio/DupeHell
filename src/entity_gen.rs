// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use serde::Deserialize;

use crate::column_gen::{self, ColType, ColumnDef};
use crate::context::Context;
use crate::rng::Rng;

/// Default batch size for entity generation (matches Python BATCH_SIZE=500000).
pub const BATCH_SIZE: usize = 500_000;

// ── JSON-deserializable column definition ─────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColCondition {
    depends_on: String,
    op: String,
    #[serde(default)]
    value: serde_json::Value,
    action: String,
    #[serde(default)]
    action_value: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColDefJson {
    name: String,
    #[serde(rename = "type", default = "default_col_type")]
    col_type: String,
    #[serde(default)]
    pool_name: Option<String>,
    #[serde(default = "default_true")]
    nullable: bool,
    #[serde(default)]
    null_rate_default: f64,
    #[serde(default)]
    conditions: Vec<ColCondition>,
}

fn default_col_type() -> String {
    "string".to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityBatchRequest {
    #[allow(dead_code)]
    entity_name: String,
    n: usize,
    seed: u64,
    columns: Vec<ColDefJson>,
}

// ── Type mapping ──────────────────────────────────────────────────────────

fn col_type_from_str(s: &str) -> ColType {
    match s {
        "int" => ColType::Int,
        "float" => ColType::Float,
        "boolean" => ColType::Boolean,
        "date" => ColType::Date,
        "datetime" => ColType::Datetime,
        _ => ColType::String,
    }
}

fn col_type_to_arrow(s: &str) -> DataType {
    match s {
        "int" => DataType::Int64,
        "float" => DataType::Float64,
        "boolean" => DataType::Boolean,
        "date" => DataType::Utf8,
        "datetime" => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

// ── Column conditions ─────────────────────────────────────────────────────

fn apply_column_conditions(
    batch: &mut HashMap<String, ArrayRef>,
    columns: &[ColDefJson],
    ctx: &Context,
    rng: &mut Rng,
) {
    for col in columns {
        if col.conditions.is_empty() {
            continue;
        }
        let target = match batch.get(&col.name) {
            Some(arr) => arr,
            None => continue,
        };
        let n = target.len();

        for cond in &col.conditions {
            let dep = match batch.get(&cond.depends_on) {
                Some(arr) => arr,
                None => continue,
            };
            if dep.len() != n {
                continue;
            }

            // Build mask
            let mask = build_condition_mask(dep, cond, n);
            if mask.is_empty() {
                continue;
            }

            // Apply action
            match cond.action.as_str() {
                "set_null" => {
                    apply_action_set_null(batch, &col.name, &mask, n);
                }
                "set_value" => {
                    if let Some(ref av) = cond.action_value {
                        apply_action_set_value(batch, &col.name, &mask, av, n);
                    }
                }
                "set_pool" => {
                    if let Some(ref av) = cond.action_value
                        && let Some(pool_name) = av.as_str()
                    {
                        apply_action_set_pool(batch, &col.name, &mask, pool_name, n, ctx, rng);
                    }
                }
                _ => {}
            }
        }
    }
}

fn build_condition_mask(dep: &ArrayRef, cond: &ColCondition, n: usize) -> Vec<bool> {
    match cond.op.as_str() {
        "eq" | "in" => {
            let vals = match &cond.value {
                serde_json::Value::Array(arr) => arr.iter().map(val_to_string).collect::<Vec<_>>(),
                v => vec![val_to_string(v)],
            };
            let dep_str = array_to_strings(dep, n);
            (0..n)
                .map(|i| vals.iter().any(|v| dep_str[i] == *v))
                .collect()
        }
        "ne" | "not_in" => {
            let vals = match &cond.value {
                serde_json::Value::Array(arr) => arr.iter().map(val_to_string).collect::<Vec<_>>(),
                v => vec![val_to_string(v)],
            };
            let dep_str = array_to_strings(dep, n);
            (0..n)
                .map(|i| !vals.iter().any(|v| dep_str[i] == *v))
                .collect()
        }
        "gt" => {
            let threshold = cond.value.as_f64().unwrap_or(0.0);
            let dep_vals = array_to_f64s(dep, n);
            (0..n).map(|i| dep_vals[i] > threshold).collect()
        }
        "gte" => {
            let threshold = cond.value.as_f64().unwrap_or(0.0);
            let dep_vals = array_to_f64s(dep, n);
            (0..n).map(|i| dep_vals[i] >= threshold).collect()
        }
        "lt" => {
            let threshold = cond.value.as_f64().unwrap_or(0.0);
            let dep_vals = array_to_f64s(dep, n);
            (0..n).map(|i| dep_vals[i] < threshold).collect()
        }
        "lte" => {
            let threshold = cond.value.as_f64().unwrap_or(0.0);
            let dep_vals = array_to_f64s(dep, n);
            (0..n).map(|i| dep_vals[i] <= threshold).collect()
        }
        _ => vec![],
    }
}

fn val_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => v.to_string(),
    }
}

fn array_to_strings(arr: &ArrayRef, n: usize) -> Vec<String> {
    use arrow::array::AsArray;
    let s = arr.as_string::<i32>();
    (0..n).map(|i| s.value(i).to_string()).collect()
}

fn array_to_f64s(arr: &ArrayRef, n: usize) -> Vec<f64> {
    if let Some(int_arr) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return (0..n).map(|i| int_arr.value(i) as f64).collect();
    }
    if let Some(float_arr) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return (0..n).map(|i| float_arr.value(i)).collect();
    }
    vec![0.0; n]
}

fn apply_action_set_null(
    batch: &mut HashMap<String, ArrayRef>,
    col_name: &str,
    mask: &[bool],
    n: usize,
) {
    let arr = batch.get(col_name).unwrap();
    let dt = arr.data_type();
    if *dt == DataType::Int64 {
        use arrow::array::Int64Array;
        let src = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        let mut builder = arrow::array::Int64Builder::with_capacity(n);
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_null();
            } else {
                builder.append_value(src.value(i));
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else if *dt == DataType::Float64 {
        use arrow::array::Float64Array;
        let src = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let mut builder = arrow::array::Float64Builder::with_capacity(n);
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_null();
            } else {
                builder.append_value(src.value(i));
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else if *dt == DataType::Boolean {
        use arrow::array::BooleanArray;
        let src = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        let mut builder = arrow::array::BooleanBuilder::with_capacity(n);
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_null();
            } else {
                builder.append_value(src.value(i));
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else {
        let mut builder = StringBuilder::with_capacity(n, 16);
        let src = arr.as_string::<i32>();
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_null();
            } else {
                builder.append_value(src.value(i));
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    }
}

fn apply_action_set_value(
    batch: &mut HashMap<String, ArrayRef>,
    col_name: &str,
    mask: &[bool],
    action_value: &serde_json::Value,
    n: usize,
) {
    let arr = batch.get(col_name).unwrap();
    let dt = arr.data_type();
    let new_val_str = val_to_string(action_value);
    if *dt == DataType::Int64 {
        use arrow::array::Int64Array;
        let src = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        let parsed: i64 = new_val_str.parse().unwrap_or(0);
        let mut builder = arrow::array::Int64Builder::with_capacity(n);
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_value(parsed);
            } else {
                builder.append_value(src.value(i));
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else if *dt == DataType::Float64 {
        use arrow::array::Float64Array;
        let src = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let parsed: f64 = new_val_str.parse().unwrap_or(0.0);
        let mut builder = arrow::array::Float64Builder::with_capacity(n);
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_value(parsed);
            } else {
                builder.append_value(src.value(i));
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else if *dt == DataType::Boolean {
        use arrow::array::BooleanArray;
        let src = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        let parsed: bool = new_val_str.parse().unwrap_or(false);
        let mut builder = arrow::array::BooleanBuilder::with_capacity(n);
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_value(parsed);
            } else {
                builder.append_value(src.value(i));
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else {
        let mut builder = StringBuilder::with_capacity(n, 16);
        let src = arr.as_string::<i32>();
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_value(&new_val_str);
            } else {
                builder.append_value(src.value(i));
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    }
}

fn apply_action_set_pool(
    batch: &mut HashMap<String, ArrayRef>,
    col_name: &str,
    mask: &[bool],
    pool_name: &str,
    n: usize,
    ctx: &Context,
    rng: &mut Rng,
) {
    let arr = batch.get(col_name).unwrap();
    let dt = arr.data_type();
    let mask_count = mask.iter().filter(|&&m| m).count();
    let pool_strs = if mask_count > 0 {
        let pool = crate::pool_lookup::pool_values(pool_name, mask_count, rng, ctx);
        let pool_s = pool.as_string::<i32>();
        (0..mask_count)
            .map(|i| pool_s.value(i).to_string())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    if *dt == DataType::Int64 {
        use arrow::array::Int64Array;
        let src = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        let mut builder = arrow::array::Int64Builder::with_capacity(n);
        let mut pool_idx = 0;
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                if pool_idx < pool_strs.len() {
                    let parsed: i64 = pool_strs[pool_idx].parse().unwrap_or(0);
                    builder.append_value(parsed);
                    pool_idx += 1;
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_value(src.value(i));
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else if *dt == DataType::Float64 {
        use arrow::array::Float64Array;
        let src = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        let mut builder = arrow::array::Float64Builder::with_capacity(n);
        let mut pool_idx = 0;
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                if pool_idx < pool_strs.len() {
                    let parsed: f64 = pool_strs[pool_idx].parse().unwrap_or(0.0);
                    builder.append_value(parsed);
                    pool_idx += 1;
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_value(src.value(i));
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else {
        let mut builder = StringBuilder::with_capacity(n, 16);
        let src = arr.as_string::<i32>();
        let mut pool_idx = 0;
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                if pool_idx < pool_strs.len() {
                    builder.append_value(&pool_strs[pool_idx]);
                    pool_idx += 1;
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_value(src.value(i));
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    }
}

// ── Main generation ───────────────────────────────────────────────────────

/// Generate an entity batch from a JSON request.
/// Returns a RecordBatch with the generated columns.
pub fn generate_entity_batch(ctx: &Context, request_json: &str) -> Result<RecordBatch, String> {
    let req: EntityBatchRequest =
        serde_json::from_str(request_json).map_err(|e| format!("invalid request JSON: {e}"))?;

    let n = req.n;
    let mut rng = Rng::new(req.seed);

    // Generate columns in parallel — fork sub-RNGs for each column
    let col_count = req.columns.len();
    let mut col_defs: Vec<ColumnDef> = Vec::with_capacity(col_count);
    let mut field_infos: Vec<(String, DataType, bool)> = Vec::with_capacity(col_count);
    for col_def in &req.columns {
        let ct = col_type_from_str(&col_def.col_type);
        let nullable = col_def.nullable;
        col_defs.push(ColumnDef {
            name: col_def.name.clone(),
            col_type: ct,
            pool_name: col_def.pool_name.clone(),
            nullable,
            null_rate: col_def.null_rate_default,
        });
        field_infos.push((
            col_def.name.clone(),
            col_type_to_arrow(&col_def.col_type),
            nullable,
        ));
    }

    let mut sub_rngs: Vec<Rng> = (0..col_count).map(|_| rng.fork()).collect();

    let mut results: Vec<(String, ArrayRef)> = col_defs
        .into_par_iter()
        .zip(sub_rngs.par_iter_mut())
        .map(|(col, col_rng)| {
            let arr = column_gen::generate_column(&col, n, col_rng, ctx);
            (col.name.clone(), arr)
        })
        .collect();

    let mut fields: Vec<Field> = Vec::with_capacity(col_count);
    let mut batch_map: HashMap<String, ArrayRef> = HashMap::new();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(col_count);
    for ((name, arr), (_, dt, nullable)) in results.drain(..).zip(field_infos) {
        fields.push(Field::new(&name, dt, nullable));
        batch_map.insert(name.clone(), arr.clone());
        arrays.push(arr);
    }

    // Apply column conditions
    apply_column_conditions(&mut batch_map, &req.columns, ctx, &mut rng);

    // Rebuild arrays array from potentially modified batch_map
    let final_arrays: Vec<ArrayRef> = req
        .columns
        .iter()
        .map(|c| batch_map.remove(&c.name).unwrap())
        .collect();

    let schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(schema), final_arrays)
        .map_err(|e| format!("RecordBatch error: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};

    fn test_ctx() -> Context {
        let pools_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("dupehell/assets/pools");
        Context::new("kyc", "en", pools_dir.to_str().unwrap()).unwrap()
    }

    #[test]
    fn test_generate_entity_basic() {
        let ctx = test_ctx();
        let json = r#"{
            "entity_name": "person",
            "n": 5,
            "seed": 42,
            "columns": [
                {"name": "first_name", "type": "string", "pool_name": "first_name"},
                {"name": "last_name", "type": "string", "pool_name": "last_name"},
                {"name": "phone", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }"#;
        let batch = generate_entity_batch(&ctx, json).unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 4);

        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "first_name");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_generate_entity_with_null_rate() {
        let ctx = test_ctx();
        let json = r#"{
            "entity_name": "person",
            "n": 100,
            "seed": 42,
            "columns": [
                {"name": "first_name", "type": "string", "pool_name": "first_name", "nullable": true, "null_rate_default": 0.3}
            ]
        }"#;
        let batch = generate_entity_batch(&ctx, json).unwrap();
        use arrow::array::AsArray;
        let arr = batch.column(0).as_string::<i32>();
        let null_count = (0..100).filter(|&i| arr.is_null(i)).count();
        assert!(
            null_count > 10 && null_count < 70,
            "null count = {null_count}"
        );
        assert!(arr.is_valid(0), "first element should not be null");
    }

    #[test]
    fn test_generate_entity_deterministic() {
        let ctx = test_ctx();
        let json = r#"{
            "entity_name": "person",
            "n": 10,
            "seed": 42,
            "columns": [
                {"name": "phone", "type": "string"},
                {"name": "email", "type": "string"}
            ]
        }"#;
        let a = generate_entity_batch(&ctx, json).unwrap();
        let b = generate_entity_batch(&ctx, json).unwrap();
        let sa = a.column(0).as_string::<i32>();
        let sb = b.column(0).as_string::<i32>();
        for i in 0..10 {
            assert_eq!(sa.value(i), sb.value(i), "mismatch at {i}");
        }
    }

    #[test]
    fn test_generate_all_types() {
        let ctx = test_ctx();
        let json = r#"{
            "entity_name": "test",
            "n": 5,
            "seed": 42,
            "columns": [
                {"name": "txt", "type": "string"},
                {"name": "num", "type": "int"},
                {"name": "flt", "type": "float"},
                {"name": "bln", "type": "boolean"},
                {"name": "dt", "type": "date"}
            ]
        }"#;
        let batch = generate_entity_batch(&ctx, json).unwrap();
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 5);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Float64);
        assert_eq!(batch.schema().field(3).data_type(), &DataType::Boolean);
        assert_eq!(batch.schema().field(4).data_type(), &DataType::Utf8);
    }
}
