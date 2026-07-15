// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use pyo3::exceptions::PyValueError;
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use pyo3::prelude::*;

mod buf_gen;
mod canary;
mod column_gen;
pub mod context;
pub mod difficulty;
mod entity_gen;
mod fast_template;
mod fk_remap;
mod graph_gen;
pub mod gt;
mod hn_common;
mod noise;
pub mod pipeline;
mod pool_lookup;
pub mod rng;
pub mod schema;

pub use context::Context;
pub use difficulty::DifficultyReport;
pub use pipeline::{PipelineConfig, PipelineOutput, PipelineStats, run_pipeline};
pub use schema::{
    DomainSchema, EntitySchema, HnSchema, build_pipeline_config, chrono_now, load_schema,
};

#[pyclass]
#[derive(Clone)]
pub struct GenerateResult {
    #[pyo3(get)]
    pub dataset: String,
    #[pyo3(get)]
    pub ground_truth: String,
    #[pyo3(get)]
    pub total_records: usize,
    #[pyo3(get)]
    pub exact_dups: usize,
    #[pyo3(get)]
    pub hard_negs: usize,
    #[pyo3(get)]
    pub uniques: usize,
    #[pyo3(get)]
    pub masters: usize,
    #[pyo3(get)]
    pub nodes: Option<String>,
    #[pyo3(get)]
    pub edges: Option<String>,
}

#[pymethods]
impl GenerateResult {
    fn __repr__(&self) -> String {
        format!(
            "GenerateResult(dataset={:?}, ground_truth={:?}, total_records={}, exact_dups={}, hard_negs={}, uniques={}, masters={})",
            self.dataset,
            self.ground_truth,
            self.total_records,
            self.exact_dups,
            self.hard_negs,
            self.uniques,
            self.masters,
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

#[pyfunction]
fn estimate_difficulty(
    domain: &str,
    size: usize,
    seed: u64,
    difficulty: &str,
    schemas_dir: &str,
    hard_neg_ratio: f64,
) -> PyResult<String> {
    let schema =
        load_schema(domain, std::path::Path::new(schemas_dir)).map_err(PyValueError::new_err)?;
    let report = crate::difficulty::estimate_difficulty(
        domain,
        size,
        seed,
        difficulty,
        hard_neg_ratio,
        &schema,
    )
    .map_err(PyValueError::new_err)?;
    serde_json::to_string(&report).map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn generate(
    domain: &str,
    size: usize,
    seed: u64,
    difficulty: &str,
    output_dir: &str,
    locale: &str,
    pools_dir: &str,
    schemas_dir: &str,
    output_format: &str,
    hard_neg_ratio: f64,
    singleton_master_fraction: f64,
    generate_graph: bool,
    graph_format: &str,
) -> PyResult<GenerateResult> {
    let schema =
        load_schema(domain, std::path::Path::new(schemas_dir)).map_err(PyValueError::new_err)?;

    if output_format != "ipc" && output_format != "parquet" {
        return Err(PyValueError::new_err(format!(
            "invalid output format '{output_format}'; expected 'ipc' or 'parquet'"
        )));
    }

    let mut ctx = Context::new(domain, locale, pools_dir).map_err(PyValueError::new_err)?;

    let run_id = schema::deterministic_run_id(domain, size, seed, difficulty);
    let config = build_pipeline_config(
        domain,
        size,
        seed,
        difficulty,
        hard_neg_ratio,
        singleton_master_fraction,
        &schema,
        &run_id,
        output_format,
        generate_graph,
        graph_format,
    )
    .map_err(PyValueError::new_err)?;

    ctx.enable_watermark(&config.domain, config.size, config.seed);

    let output = run_pipeline(&ctx, &config, output_dir).map_err(PyValueError::new_err)?;

    Ok(GenerateResult {
        dataset: output.output_files.into_iter().next().unwrap_or_default(),
        ground_truth: output.gt_file,
        total_records: output.stats.total_records,
        exact_dups: output.stats.exact_dups,
        hard_negs: output.stats.hard_negs,
        uniques: output.stats.uniques,
        masters: output.stats.masters,
        nodes: output.nodes,
        edges: output.edges,
    })
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(generate, m)?)?;
    m.add_function(wrap_pyfunction!(estimate_difficulty, m)?)?;
    m.add_class::<GenerateResult>()?;
    Ok(())
}
