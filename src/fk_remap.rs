use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::take;
use arrow::record_batch::RecordBatch;

use crate::rng::Rng;

/// Remap a single column in a batch by randomly sampling from a FK pool.
///
/// # Arguments
/// * `batch` - The entity batch whose column will be remapped
/// * `pool_rb` - A single-column RecordBatch containing FK identifier values
/// * `src_col` - Name of the column to remap in the batch
/// * `rng` - RNG for random index generation into the pool
pub fn fk_remap_batch(
    batch: &RecordBatch,
    pool_rb: &RecordBatch,
    src_col: &str,
    rng: &mut Rng,
) -> Result<RecordBatch, String> {
    let n = batch.num_rows();
    let pool_col = pool_rb.column(0);
    let pool_n = pool_col.len();

    if pool_n == 0 {
        return Err("FK pool is empty".into());
    }

    // Generate random indices into the FK pool
    let indices: Vec<usize> = (0..n).map(|_| rng.next_usize(pool_n)).collect();
    let idx_arr = UInt64Array::from_iter_values(indices.iter().copied().map(|i| i as u64));

    // Sample values from pool using Arrow take kernel
    let remapped: ArrayRef = take(pool_col, &idx_arr, None)
        .map_err(|e| format!("take error while remapping '{src_col}': {e}"))?;

    // Find column index in batch schema
    let schema = batch.schema();
    let col_idx = schema
        .index_of(src_col)
        .map_err(|_| format!("Column '{src_col}' not found in batch"))?;

    // Rebuild column array with remapped column replaced
    let new_columns: Vec<ArrayRef> = (0..batch.num_columns())
        .map(|i| {
            if i == col_idx {
                remapped.clone()
            } else {
                batch.column(i).clone()
            }
        })
        .collect();

    RecordBatch::try_new(schema, new_columns).map_err(|e| format!("RecordBatch error: {e}"))
}

/// Apply FK remaps to a batch using a map of pool column name → pool RecordBatch.
///
/// `fk_pools` maps each pool column name (e.g. "customer_id") to a single-column
/// RecordBatch of identifier values. Each entry in `remap_cols` specifies which
/// column in `batch` should be remapped using the corresponding pool.
///
/// Returns the remapped RecordBatch.
pub fn fk_remap_batch_from_map(
    batch: &RecordBatch,
    fk_pools: &std::collections::HashMap<String, RecordBatch>,
    remap_cols: &[String],
    rng: &mut Rng,
) -> Result<RecordBatch, String> {
    let mut result = batch.clone();
    for col_name in remap_cols {
        let pool = fk_pools
            .get(col_name)
            .ok_or_else(|| format!("FK pool for '{col_name}' not found"))?;
        result = fk_remap_batch(&result, pool, col_name, rng)?;
    }
    Ok(result)
}

/// Build a single-column FK pool RecordBatch from an identifier column in a batch.
pub fn build_fk_pool(batch: &RecordBatch, id_col: &str) -> Result<RecordBatch, String> {
    let col = batch
        .column_by_name(id_col)
        .ok_or_else(|| format!("Column '{id_col}' not found"))?;
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
        id_col,
        col.data_type().clone(),
        true,
    )]));
    RecordBatch::try_new(schema, vec![col.clone()]).map_err(|e| format!("RecordBatch: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, Int64Array, AsArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, true),
            Field::new("customer_id", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
        ]));
        let order_id = StringArray::from(vec!["O001", "O002", "O003", "O004"]);
        let customer_id = StringArray::from(vec!["X001", "X002", "X003", "X004"]);
        let amount = Int64Array::from(vec![100, 200, 300, 400]);
        RecordBatch::try_new(schema, vec![
            Arc::new(order_id),
            Arc::new(customer_id),
            Arc::new(amount),
        ]).unwrap()
    }

    fn make_fk_pool() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, true),
        ]));
        let vals = StringArray::from(vec![
            "C001", "C002", "C003", "C004", "C005", "C006", "C007", "C008", "C009", "C010",
        ]);
        RecordBatch::try_new(schema, vec![Arc::new(vals)]).unwrap()
    }

    #[test]
    fn test_fk_remap_basic() {
        let batch = make_batch();
        let pool = make_fk_pool();
        let mut rng = Rng::new(42);

        let remapped = fk_remap_batch(&batch, &pool, "customer_id", &mut rng).unwrap();
        assert_eq!(remapped.num_rows(), 4);
        assert_eq!(remapped.num_columns(), 3);

        // Check that customer_id values now come from the pool
        let col = remapped.column_by_name("customer_id").unwrap().as_string::<i32>();
        for i in 0..4 {
            let val = col.value(i);
            assert!(val.starts_with("C"), "expected pool value, got {val}");
            assert_ne!(val, "X001", "should not retain original synthetic value");
        }

        // order_id and amount should be unchanged
        let oid = remapped.column_by_name("order_id").unwrap().as_string::<i32>();
        assert_eq!(oid.value(0), "O001");
        assert_eq!(oid.value(3), "O004");

        let amt = remapped.column_by_name("amount").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(amt.value(0), 100);
    }

    #[test]
    fn test_fk_remap_deterministic() {
        let batch = make_batch();
        let pool = make_fk_pool();
        let mut rng_a = Rng::new(42);
        let mut rng_b = Rng::new(42);

        let a = fk_remap_batch(&batch, &pool, "customer_id", &mut rng_a).unwrap();
        let b = fk_remap_batch(&batch, &pool, "customer_id", &mut rng_b).unwrap();

        let ca = a.column_by_name("customer_id").unwrap().as_string::<i32>();
        let cb = b.column_by_name("customer_id").unwrap().as_string::<i32>();
        for i in 0..4 {
            assert_eq!(ca.value(i), cb.value(i), "mismatch at {i}");
        }
    }

    #[test]
    fn test_fk_remap_empty_pool() {
        let batch = make_batch();
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, true),
        ]));
        let empty = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ]).unwrap();
        let mut rng = Rng::new(0);
        let result = fk_remap_batch(&batch, &empty, "customer_id", &mut rng);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_fk_pool() {
        let batch = make_batch();
        let pool = build_fk_pool(&batch, "customer_id").unwrap();
        assert_eq!(pool.num_rows(), 4);
        assert_eq!(pool.num_columns(), 1);
        let col = pool.column(0).as_string::<i32>();
        assert_eq!(col.value(0), "X001");
        assert_eq!(col.value(3), "X004");
    }
}
