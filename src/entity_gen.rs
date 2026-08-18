// DupeHell -- MIT License
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// No liability for misuse.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
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

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ColDefJson {
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
    /// Position of this batch's first row within the entity's full row
    /// range across all batches. Needed so per-row counters (e.g. the `_id`
    /// fallback in `column_gen::generate_column`) stay globally unique
    /// instead of restarting at 0 for every batch.
    #[serde(default)]
    row_offset: usize,
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
            // Validate the action itself BEFORE building the O(n) mask
            // (hunt1808/H3): `set_value`/`set_pool` without a usable
            // `action_value` can never do anything — checking that first
            // means an inert condition costs one string compare, not a
            // full mask scan (`Vec<bool>` + n comparisons) that's built
            // only to be thrown away by the `if let Some(...)` below.
            let pool_name = match cond.action.as_str() {
                "set_null" => None,
                "set_value" => {
                    if cond.action_value.is_none() {
                        log::warn!(
                            "column '{}' condition (depends_on='{}') has action 'set_value' \
                             with no action_value — this condition can never do anything, \
                             check the schema",
                            col.name,
                            cond.depends_on
                        );
                        continue;
                    }
                    None
                }
                "set_pool" => match cond.action_value.as_ref().and_then(|av| av.as_str()) {
                    Some(p) => Some(p),
                    None => {
                        log::warn!(
                            "column '{}' condition (depends_on='{}') has action 'set_pool' \
                             with no usable action_value (must be a pool name string) — this \
                             condition can never do anything, check the schema",
                            col.name,
                            cond.depends_on
                        );
                        continue;
                    }
                },
                _ => continue,
            };

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
                    if let Some(pool_name) = pool_name {
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
            use arrow::array::AsArray;
            let dep_str = dep.as_string::<i32>();
            (0..n)
                .map(|i| vals.iter().any(|v| dep_str.value(i) == v))
                .collect()
        }
        "ne" | "not_in" => {
            let vals = match &cond.value {
                serde_json::Value::Array(arr) => arr.iter().map(val_to_string).collect::<Vec<_>>(),
                v => vec![val_to_string(v)],
            };
            use arrow::array::AsArray;
            let dep_str = dep.as_string::<i32>();
            (0..n)
                .map(|i| !vals.iter().any(|v| dep_str.value(i) == v))
                .collect()
        }
        "gt" | "gte" | "lt" | "lte" => {
            let threshold = cond.value.as_f64().unwrap_or(0.0);
            let cmp: fn(f64, f64) -> bool = match cond.op.as_str() {
                "gt" => |a, b| a > b,
                "gte" => |a, b| a >= b,
                "lt" => |a, b| a < b,
                _ => |a, b| a <= b,
            };
            // Single downcast + direct per-element comparison on the typed
            // Arrow array (hunt1808/H4) instead of materializing a
            // `Vec<f64>` of the whole column via `array_to_f64s` just to
            // immediately re-read it once per row.
            if let Some(int_arr) = dep.as_any().downcast_ref::<arrow::array::Int64Array>() {
                (0..n)
                    .map(|i| cmp(int_arr.value(i) as f64, threshold))
                    .collect()
            } else if let Some(float_arr) =
                dep.as_any().downcast_ref::<arrow::array::Float64Array>()
            {
                (0..n).map(|i| cmp(float_arr.value(i), threshold)).collect()
            } else {
                // Matches `array_to_f64s`'s fallback of `0.0` for any other
                // type — every row compares equal, so the mask is uniform.
                vec![cmp(0.0, threshold); n]
            }
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

fn apply_action_set_null(
    batch: &mut HashMap<String, ArrayRef>,
    col_name: &str,
    mask: &[bool],
    n: usize,
) {
    let arr = batch
        .get(col_name)
        .unwrap_or_else(|| panic!("apply_action_set_null: column '{col_name}' not in batch"));
    let dt = arr.data_type();
    if *dt == DataType::Int64 {
        use arrow::array::Int64Array;
        let src = arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("apply_action_set_null: data_type()==Int64 but downcast failed");
        let mut builder = arrow::array::Int64Builder::with_capacity(n);
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_null();
            } else {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else if *dt == DataType::Float64 {
        use arrow::array::Float64Array;
        let src = arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("apply_action_set_null: data_type()==Float64 but downcast failed");
        let mut builder = arrow::array::Float64Builder::with_capacity(n);
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_null();
            } else {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else if *dt == DataType::Boolean {
        use arrow::array::BooleanArray;
        let src = arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("apply_action_set_null: data_type()==Boolean but downcast failed");
        let mut builder = arrow::array::BooleanBuilder::with_capacity(n);
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_null();
            } else {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
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
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
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
    let arr = batch
        .get(col_name)
        .unwrap_or_else(|| panic!("apply_action_set_value: column '{col_name}' not in batch"));
    let dt = arr.data_type();
    let new_val_str = val_to_string(action_value);
    if *dt == DataType::Int64 {
        use arrow::array::Int64Array;
        let src = arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("apply_action_set_value: data_type()==Int64 but downcast failed");
        let parsed: i64 = new_val_str.parse().unwrap_or(0);
        let mut builder = arrow::array::Int64Builder::with_capacity(n);
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_value(parsed);
            } else {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else if *dt == DataType::Float64 {
        use arrow::array::Float64Array;
        let src = arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("apply_action_set_value: data_type()==Float64 but downcast failed");
        let parsed: f64 = new_val_str.parse().unwrap_or(0.0);
        let mut builder = arrow::array::Float64Builder::with_capacity(n);
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_value(parsed);
            } else {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else if *dt == DataType::Boolean {
        use arrow::array::BooleanArray;
        let src = arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("apply_action_set_value: data_type()==Boolean but downcast failed");
        let parsed: bool = new_val_str.parse().unwrap_or(false);
        let mut builder = arrow::array::BooleanBuilder::with_capacity(n);
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                builder.append_value(parsed);
            } else {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
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
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
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
    let arr = batch
        .get(col_name)
        .unwrap_or_else(|| panic!("apply_action_set_pool: column '{col_name}' not in batch"));
    let dt = arr.data_type();
    let mask_count = mask.iter().filter(|&&m| m).count();
    // Keep the sampled pool as an Arrow array and read `&str`s straight out
    // of it instead of copying every value into a `Vec<String>` up front —
    // `pool_arr` (unrelated to `batch`) can stay alive alongside `arr`'s
    // borrow of `batch` without any lifetime conflict.
    let pool_arr = if mask_count > 0 {
        Some(crate::pool_lookup::pool_values(
            pool_name, mask_count, rng, ctx,
        ))
    } else {
        None
    };
    let pool_s = pool_arr.as_ref().map(|p| p.as_string::<i32>());
    let pool_len = pool_s.as_ref().map_or(0, |p| p.len());

    if *dt == DataType::Int64 {
        use arrow::array::Int64Array;
        let src = arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("apply_action_set_pool: data_type()==Int64 but downcast failed");
        let mut builder = arrow::array::Int64Builder::with_capacity(n);
        let mut pool_idx = 0;
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                if pool_idx < pool_len {
                    // `pool_idx < pool_len` and `pool_len = pool_s.map_or(0, |p| p.len())`
                    // together guarantee `pool_s.is_some()` here.
                    let parsed: i64 = pool_s
                        .expect("pool_len > 0 implies pool_s is Some")
                        .value(pool_idx)
                        .parse()
                        .unwrap_or(0);
                    builder.append_value(parsed);
                    pool_idx += 1;
                } else {
                    builder.append_null();
                }
            } else {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else if *dt == DataType::Float64 {
        use arrow::array::Float64Array;
        let src = arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("apply_action_set_pool: data_type()==Float64 but downcast failed");
        let mut builder = arrow::array::Float64Builder::with_capacity(n);
        let mut pool_idx = 0;
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                if pool_idx < pool_len {
                    let parsed: f64 = pool_s
                        .expect("pool_len > 0 implies pool_s is Some")
                        .value(pool_idx)
                        .parse()
                        .unwrap_or(0.0);
                    builder.append_value(parsed);
                    pool_idx += 1;
                } else {
                    builder.append_null();
                }
            } else {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    } else {
        let mut builder = StringBuilder::with_capacity(n, n * 16);
        let src = arr.as_string::<i32>();
        let mut pool_idx = 0;
        for (i, &m) in mask.iter().enumerate().take(n) {
            if m {
                if pool_idx < pool_len {
                    builder.append_value(
                        pool_s
                            .expect("pool_len > 0 implies pool_s is Some")
                            .value(pool_idx),
                    );
                    pool_idx += 1;
                } else {
                    builder.append_null();
                }
            } else {
                if src.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(src.value(i));
                }
            }
        }
        batch.insert(col_name.to_string(), Arc::new(builder.finish()));
    }
}

// ── Main generation ───────────────────────────────────────────────────────

/// Generate an entity batch from a JSON request.
/// Returns a RecordBatch with the generated columns.
/// Parses a `plan.columns_json` array once — hunt1808/H8: the previous
/// interface reformatted a full JSON request string (columns array
/// included) and re-parsed it via `generate_entity_batch` on *every batch*
/// of an entity (up to hundreds per run), a vestige of when
/// `generate_entity_batch` was only ever called from Python. `pipeline.rs`
/// parses once per entity plan (before its batch loop) and passes the
/// already-deserialized columns to `generate_entity_batch_parsed` instead.
pub(crate) fn parse_columns(columns_json: &str) -> Result<Vec<ColDefJson>, String> {
    serde_json::from_str(columns_json).map_err(|e| format!("invalid columns JSON: {e}"))
}

pub fn generate_entity_batch(ctx: &Context, request_json: &str) -> Result<RecordBatch, String> {
    let req: EntityBatchRequest =
        serde_json::from_str(request_json).map_err(|e| format!("invalid request JSON: {e}"))?;
    generate_entity_batch_parsed(ctx, &req.columns, req.n, req.seed, req.row_offset)
}

/// Core generation logic, shared by the JSON-string entrypoint
/// (`generate_entity_batch`, kept for the Python bindings / tests) and
/// `pipeline.rs`'s per-batch calls (which parse `columns_json` once per
/// entity via `parse_columns`, not per batch — see its doc comment).
pub(crate) fn generate_entity_batch_parsed(
    ctx: &Context,
    columns: &[ColDefJson],
    n: usize,
    seed: u64,
    row_offset: usize,
) -> Result<RecordBatch, String> {
    let mut rng = Rng::new(seed);

    // Generate columns in parallel — fork sub-RNGs for each column
    let col_count = columns.len();
    let mut col_defs: Vec<ColumnDef> = Vec::with_capacity(col_count);
    let mut field_infos: Vec<(String, DataType, bool)> = Vec::with_capacity(col_count);
    for col_def in columns {
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
            let arr = column_gen::generate_column(&col, n, col_rng, ctx, row_offset);
            (col.name.clone(), arr)
        })
        .collect();

    let mut fields: Vec<Field> = Vec::with_capacity(col_count);
    let mut batch_map: HashMap<String, ArrayRef> = HashMap::new();
    for ((name, arr), (_, dt, nullable)) in results.drain(..).zip(field_infos) {
        fields.push(Field::new(&name, dt, nullable));
        batch_map.insert(name, arr);
    }

    // Apply column conditions
    apply_column_conditions(&mut batch_map, columns, ctx, &mut rng);

    // Rebuild arrays array from potentially modified batch_map
    let final_arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|c| {
            batch_map
                .remove(&c.name)
                .unwrap_or_else(|| panic!("generate_entity_batch_parsed: column '{}' missing from batch_map (built from the same columns just above)", c.name))
        })
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
        let pools_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("assets/pools");
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

    /// hunt1808/H3 correction: `apply_action_set_null`/`set_value`'s
    /// pass-through branch (`mask[i] == false`) used to do
    /// `builder.append_value(src.value(i))` unconditionally, silently
    /// promoting an existing null to a bogus concrete value (`0`/`""`/
    /// `false`) whenever a nullable column went through *any* condition —
    /// dormant only because no shipped schema's conditions ever reached
    /// this code path. Exercises the fix directly against the private
    /// helpers (rather than through random generation, which can't
    /// deterministically guarantee a null lands on an unmasked row).
    #[test]
    fn test_condition_actions_preserve_existing_nulls_on_passthrough_rows() {
        use arrow::array::{Int64Array, StringArray};

        let mut batch: HashMap<String, ArrayRef> = HashMap::new();
        // Row 1 is null and unmasked (mask[1] == false) -- must stay null.
        let src = Int64Array::from(vec![Some(10), None, Some(30)]);
        batch.insert("amount".to_string(), Arc::new(src));
        let mask = [true, false, false];

        apply_action_set_null(&mut batch, "amount", &mask, 3);
        let after = batch
            .get("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(after.is_null(0), "masked row: set_null must null it");
        assert!(
            after.is_null(1),
            "unmasked null row must stay null, not become 0"
        );
        assert_eq!(
            after.value(2),
            30,
            "unmasked non-null row must pass through unchanged"
        );

        let mut batch2: HashMap<String, ArrayRef> = HashMap::new();
        let src2 = StringArray::from(vec![Some("a"), None, Some("c")]);
        batch2.insert("label".to_string(), Arc::new(src2));
        let action_value = serde_json::json!("forced");
        apply_action_set_value(&mut batch2, "label", &mask, &action_value, 3);
        let after2 = batch2
            .get("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            after2.value(0),
            "forced",
            "masked row: set_value must overwrite it"
        );
        assert!(
            after2.is_null(1),
            "unmasked null row must stay null, not become an empty string"
        );
        assert_eq!(
            after2.value(2),
            "c",
            "unmasked non-null row must pass through unchanged"
        );
    }

    #[test]
    fn test_generate_entity_row_offset_no_id_collision() {
        // Two successive batches of the same entity, as pipeline.rs would
        // emit them: the second batch's row_offset must make its `_id`
        // fallback values continue where the first left off, not restart.
        let ctx = test_ctx();
        let json_a = r#"{
            "entity_name": "customer",
            "n": 20,
            "seed": 42,
            "columns": [{"name": "customer_id", "type": "string"}]
        }"#;
        let json_b = r#"{
            "entity_name": "customer",
            "n": 20,
            "seed": 999,
            "columns": [{"name": "customer_id", "type": "string"}],
            "row_offset": 20
        }"#;
        let batch_a = generate_entity_batch(&ctx, json_a).unwrap();
        let batch_b = generate_entity_batch(&ctx, json_b).unwrap();

        use arrow::array::AsArray;
        let ids_a: std::collections::HashSet<String> = (0..batch_a.num_rows())
            .map(|i| batch_a.column(0).as_string::<i32>().value(i).to_string())
            .collect();
        let ids_b: std::collections::HashSet<String> = (0..batch_b.num_rows())
            .map(|i| batch_b.column(0).as_string::<i32>().value(i).to_string())
            .collect();

        assert!(
            ids_a.is_disjoint(&ids_b),
            "batch B must not recycle batch A's customer_id values: {ids_a:?} vs {ids_b:?}"
        );
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
