use std::fs::File;

use arrow::array::Array;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

/// Write a RecordBatch to a Parquet file using the high-level ArrowWriter.
pub fn sink_parquet(rb: &RecordBatch, path: &str) -> Result<(), String> {
    let file = File::create(path).map_err(|e| format!("cannot create {path}: {e}"))?;
    let schema = rb.schema();

    let level = parquet::basic::ZstdLevel::try_new(3)
        .map_err(|e| format!("zstd level: {e}"))?;
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(level))
        .set_data_page_size_limit(1_048_576)
        .build();

    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| format!("ArrowWriter error: {e}"))?;

    writer
        .write(rb)
        .map_err(|e| format!("write parquet error: {e}"))?;

    writer
        .close()
        .map_err(|e| format!("close parquet error: {e}"))?;

    Ok(())
}

/// Write a RecordBatch to an Arrow IPC (Feather) file.
pub fn sink_ipc(rb: &RecordBatch, path: &str) -> Result<(), String> {
    let file = File::create(path).map_err(|e| format!("cannot create {path}: {e}"))?;
    let schema = rb.schema();
    let mut writer =
        FileWriter::try_new(file, &schema).map_err(|e| format!("FileWriter error: {e}"))?;
    writer.write(rb).map_err(|e| format!("write IPC error: {e}"))?;
    writer.finish().map_err(|e| format!("finish IPC error: {e}"))?;
    Ok(())
}

/// Write a RecordBatch to a CSV file.
pub fn sink_csv(rb: &RecordBatch, path: &str, include_header: bool) -> Result<(), String> {
    use std::io::Write;

    let file = File::create(path).map_err(|e| format!("cannot create {path}: {e}"))?;
    let mut w = std::io::BufWriter::new(file);
    let schema = rb.schema();
    let ncols = rb.num_columns();
    let nrows = rb.num_rows();

    if include_header {
        let header: Vec<&str> = (0..ncols)
            .map(|i| schema.field(i).name().as_str())
            .collect();
        writeln!(w, "{}", header.join(","))
            .map_err(|e| format!("write header error: {e}"))?;
    }

    for row in 0..nrows {
        let mut vals = Vec::with_capacity(ncols);
        for col in 0..ncols {
            let arr = rb.column(col);
            if arr.is_null(row) {
                vals.push(String::new());
            } else if arr.data_type() == &arrow::datatypes::DataType::Utf8 {
                let s = arrow::array::as_string_array(arr);
                let v = s.value(row);
                if v.contains(',') || v.contains('"') || v.contains('\n') {
                    vals.push(format!("\"{}\"", v.replace('"', "\"\"")));
                } else {
                    vals.push(v.to_string());
                }
            } else if let Some(int_arr) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
                vals.push(int_arr.value(row).to_string());
            } else if let Some(float_arr) =
                arr.as_any().downcast_ref::<arrow::array::Float64Array>()
            {
                vals.push(float_arr.value(row).to_string());
            } else if let Some(bool_arr) =
                arr.as_any().downcast_ref::<arrow::array::BooleanArray>()
            {
                vals.push(if bool_arr.value(row) {
                    "true".into()
                } else {
                    "false".into()
                });
            } else {
                vals.push(String::new());
            }
        }
        writeln!(w, "{}", vals.join(","))
            .map_err(|e| format!("write csv row error: {e}"))?;
    }
    Ok(())
}

/// Dispatch to the correct sink format.
pub fn sink_rb(rb: &RecordBatch, path: &str, format: &str) -> Result<(), String> {
    match format {
        "parquet" => sink_parquet(rb, path),
        "ipc" => sink_ipc(rb, path),
        "csv" => sink_csv(rb, path, true),
        _ => Err(format!("unknown output format: {format}")),
    }
}

/// Inject DupeHell provenance metadata into a Parquet file by rewriting it.
pub fn inject_parquet_metadata(path: &str) -> Result<(), String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
    use parquet::file::properties::WriterProperties;
    use std::collections::HashMap;

    let file = File::open(path).map_err(|e| format!("cannot open {path}: {e}"))?;
    let reader = ParquetRecordBatchReader::try_new(file, 65536)
        .map_err(|e| format!("cannot read parquet: {e}"))?;
    let arrow_schema = reader.schema().clone();

    let tmp = format!("{path}.tmp");
    let out_file =
        File::create(&tmp).map_err(|e| format!("cannot create tmp {tmp}: {e}"))?;

    let mut kv: HashMap<String, String> = HashMap::new();
    kv.insert("dupehell.generator".into(), "DupeHell Rust core".into());
    kv.insert("dupehell.provenance".into(), "dupehell-synthetic-data".into());
    kv.insert("dupehell.license".into(), "MIT".into());
    kv.insert(
        "dupehell.purpose".into(),
        "Educational Use Only -- Record Linkage Benchmarking".into(),
    );
    kv.insert("dupehell.url".into(), "https://github.com/anomalyco/dupehell".into());

    let meta_kv: Vec<parquet::file::metadata::KeyValue> = kv
        .into_iter()
        .map(|(k, v)| parquet::file::metadata::KeyValue::new(k, v))
        .collect();

    let props = WriterProperties::builder()
        .set_key_value_metadata(Some(meta_kv))
        .build();

    let mut writer = parquet::arrow::ArrowWriter::try_new(out_file, arrow_schema, Some(props))
        .map_err(|e| format!("ArrowWriter error: {e}"))?;

    for batch_result in reader {
        let rb = batch_result.map_err(|e| format!("read batch error: {e}"))?;
        writer
            .write(&rb)
            .map_err(|e| format!("write batch error: {e}"))?;
    }

    writer
        .close()
        .map_err(|e| format!("close writer error: {e}"))?;

    std::fs::rename(&tmp, path)
        .map_err(|e| format!("rename {tmp} -> {path}: {e}"))?;

    Ok(())
}
