// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::path::PathBuf;

use clap::Parser;

use dupehell_core::context::Context;
use dupehell_core::difficulty::estimate_difficulty;
use dupehell_core::pipeline::run_pipeline_with_progress;
use dupehell_core::schema::{build_pipeline_config, load_schema};

#[derive(Parser)]
#[command(
    name = "dupehell",
    version = env!("CARGO_PKG_VERSION"),
    about = "Synthetic record linkage dataset generator",
    long_about = "Generates realistic synthetic datasets with controlled duplicate rates, \
                  hard negatives, and noise profiles for benchmarking record linkage systems. \
                  Supports 40 domains (kyc, healthcare, ecommerce, gaming, ...), \
                  four difficulty levels, and outputs Arrow IPC or Parquet format."
)]
struct Cli {
    #[arg(
        long,
        default_value = "kyc",
        help = "Domain name (e.g. kyc, healthcare, gaming, ecommerce)"
    )]
    domain: String,

    #[arg(
        long,
        default_value_t = 1_000_000,
        help = "Number of base records to generate (minimum 10)"
    )]
    size: usize,

    #[arg(
        long,
        default_value_t = 42,
        help = "Random seed for deterministic reproducibility"
    )]
    seed: u64,

    #[arg(long, default_value = "medium", value_parser = clap::builder::PossibleValuesParser::new(["light", "medium", "hard", "hell"]), help = "Difficulty level: light, medium, hard, or hell")]
    difficulty: String,

    #[arg(
        long,
        help = "Estimate difficulty and F1 score without generating data"
    )]
    estimate: bool,

    #[arg(
        long,
        help = "Output file format: parquet (default, ZSTD compressed) or ipc (Arrow IPC)"
    )]
    output_format: Option<String>,

    #[arg(long, action = clap::ArgAction::Count, help = "Shortcut for --output-format parquet (the default; kept for backward compatibility)")]
    parquet: u8,

    #[arg(
        long,
        default_value = ".",
        help = "Output directory (created automatically if missing)"
    )]
    output_dir: PathBuf,

    #[arg(
        long,
        default_value_t = 0.3,
        help = "Hard-negative scaling knob, not a literal fraction: actual count ~= size * hard_neg_ratio * 0.05 (default 0.3 -> ~1.5% of size). Use --estimate to see the exact count first."
    )]
    hard_neg_ratio: f64,

    #[arg(
        long,
        default_value_t = 0.10,
        help = "Fraction of masters with only one record (0.0 to 1.0)"
    )]
    singleton_master_fraction: f64,

    #[arg(long, default_value = "en", value_parser = clap::builder::PossibleValuesParser::new(["en", "fr", "de", "es", "it", "pt"]), help = "Locale for pool data (en, fr, de, es, it, pt)")]
    locale: String,

    #[arg(
        long,
        default_value = "assets/pools",
        help = "Path to asset pools directory"
    )]
    pools_dir: PathBuf,

    #[arg(
        long,
        default_value = "schemas",
        help = "Path to schema JSON directory"
    )]
    schemas_dir: PathBuf,

    #[arg(
        long,
        help = "Generate property-graph output (nodes + edges) in addition to tabular data"
    )]
    graph: bool,

    #[arg(
        long,
        default_value = "parquet",
        value_parser = clap::builder::PossibleValuesParser::new(["ipc", "parquet"]),
        help = "Graph output format: parquet (default, ZSTD compressed) or ipc (requires --graph)"
    )]
    graph_format: String,
}

/// Rough peak-RSS-per-record, measured on this machine (Windows, release
/// build): 859 MB / 10.15M records (~89 B/rec) on `ecommerce`/medium, up to
/// 1285 MB / 10.15M records (~127 B/rec) on `ecommerce`/hell with `--graph`
/// (the worst case tried: max noise-type variety + graph node/edge buffers).
/// Rounded up for headroom across domains with more columns and to hedge
/// against super-linear growth at larger scale, which this single-machine
/// measurement can't rule out.
const BYTES_PER_RECORD_ESTIMATE: usize = 150;

/// Best-effort warning if the requested `--size` looks likely to exceed
/// available system RAM, using `BYTES_PER_RECORD_ESTIMATE`. Advisory only —
/// if system memory can't be read (sandboxed/unusual environment), this
/// silently does nothing rather than block a run on a guess.
fn warn_if_memory_tight(size: usize) {
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let available = sys.available_memory();
    if available == 0 {
        return;
    }
    let estimated = size as u64 * BYTES_PER_RECORD_ESTIMATE as u64;
    if estimated > available {
        eprintln!(
            "Warning: --size {size} is estimated to need ~{:.1} GB of RAM \
             (rough estimate, ~{BYTES_PER_RECORD_ESTIMATE} B/record), but only \
             ~{:.1} GB is available. The run may be killed by the OS or swap \
             heavily. Consider a smaller --size or splitting into multiple runs.",
            estimated as f64 / 1e9,
            available as f64 / 1e9,
        );
    }
}

fn main() {
    env_logger::init();
    let cli = Cli::parse();

    let schema = match load_schema(&cli.domain, &cli.schemas_dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    if cli.estimate {
        match estimate_difficulty(
            &cli.domain,
            cli.size,
            cli.seed,
            &cli.difficulty,
            cli.hard_neg_ratio,
            &schema,
        ) {
            Ok(report) => {
                println!("{}", serde_json::to_string_pretty(&report).unwrap());
            }
            Err(e) => {
                eprintln!("Estimation failed: {e}");
                std::process::exit(1);
            }
        }
        return;
    }

    if cli.size < 10 {
        eprintln!("Error: size must be >= 10, got {}", cli.size);
        std::process::exit(1);
    }
    const MAX_SIZE: usize = 500_000_000;
    if cli.size > MAX_SIZE {
        eprintln!(
            "Error: size must be <= {MAX_SIZE} (500M), got {}. \
             Larger runs risk exhausting memory in a single process; \
             split into multiple runs instead.",
            cli.size
        );
        std::process::exit(1);
    }
    warn_if_memory_tight(cli.size);

    let mut ctx = match Context::new(&cli.domain, &cli.locale, &cli.pools_dir.to_string_lossy()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error loading pools: {e}");
            std::process::exit(1);
        }
    };

    let effective_format = match &cli.output_format {
        Some(fmt) => {
            if cli.parquet > 0 && fmt != "parquet" {
                eprintln!(
                    "Warning: --parquet is ignored because --output-format {fmt} was also given"
                );
            }
            fmt.clone()
        }
        None => "parquet".to_string(),
    };

    if effective_format != "ipc" && effective_format != "parquet" {
        eprintln!("Error: output format must be 'ipc' or 'parquet', got '{effective_format}'");
        std::process::exit(1);
    }

    let run_id = dupehell_core::schema::deterministic_run_id(
        &cli.domain,
        cli.size,
        cli.seed,
        &cli.difficulty,
        cli.hard_neg_ratio,
    );
    let config = match build_pipeline_config(
        &cli.domain,
        cli.size,
        cli.seed,
        &cli.difficulty,
        cli.hard_neg_ratio,
        cli.singleton_master_fraction,
        &schema,
        &run_id,
        &effective_format,
        cli.graph,
        &cli.graph_format,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error building config: {e}");
            std::process::exit(1);
        }
    };

    ctx.enable_watermark(&config.domain, config.size, config.seed);

    eprintln!(
        "DupeHell v{} — {} domain, {} records [{}]",
        env!("CARGO_PKG_VERSION"),
        cli.domain.to_uppercase(),
        cli.size,
        cli.difficulty
    );
    eprintln!(
        "Entities: {} types, {} HN types",
        config.entity_plans.len(),
        config.hard_neg_types.len()
    );

    let t0 = std::time::Instant::now();
    // Progress line only for runs big enough that generation actually takes
    // a noticeable amount of wall time — printing/flushing on every batch of
    // a 10K-record run would just add noise, not information. Throttled to
    // ~1 update/second (not one per 500K-row batch) so it's readable instead
    // of scrolling past on runs with many small entities.
    const PROGRESS_MIN_SIZE: usize = 1_000_000;
    let target_size = cli.size;
    let mut last_print = std::time::Instant::now();
    let mut progress_cb = move |done: usize, total: usize| {
        if target_size < PROGRESS_MIN_SIZE {
            return;
        }
        let now = std::time::Instant::now();
        if now.duration_since(last_print).as_secs_f64() < 1.0 && done < total {
            return;
        }
        last_print = now;
        let pct = (done as f64 / total.max(1) as f64 * 100.0).min(100.0);
        eprint!("\r  Generating... {done}/{total} ({pct:.0}%)   ");
        let _ = std::io::Write::flush(&mut std::io::stderr());
    };
    let output = match run_pipeline_with_progress(
        &ctx,
        &config,
        &cli.output_dir.to_string_lossy(),
        Some(&mut progress_cb),
    ) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Pipeline failed: {e}");
            std::process::exit(1);
        }
    };
    if cli.size >= PROGRESS_MIN_SIZE {
        eprintln!();
    }
    let elapsed = t0.elapsed().as_secs_f64();

    let n = output.stats.total_records;
    eprintln!(
        "\nDone in {:.3}s — {} records ({:.0} rec/s)",
        elapsed,
        n,
        n as f64 / elapsed
    );
    eprintln!(
        "  exact_dups={} fuzzy_dups={} hard_negs={} uniques={} masters={}",
        output.stats.exact_dups,
        output.stats.fuzzy_dups,
        output.stats.hard_negs,
        output.stats.uniques,
        output.stats.masters
    );
    eprintln!("  Dataset: {}", output.output_files[0]);
    eprintln!("  GT:      {}", output.gt_file);
    if let Some(nodes) = &output.nodes {
        eprintln!("  Nodes:  {nodes}");
    }
    if let Some(edges) = &output.edges {
        eprintln!("  Edges:  {edges}");
    }

    let id_cols: Vec<&str> = config
        .entity_plans
        .iter()
        .filter_map(|p| p.identifier_col.as_deref())
        .collect();
    if !id_cols.is_empty() {
        eprintln!(
            "  Note: {} are structural join keys (stable across all duplicates by design) — exclude them from ER match attributes, use record_id instead.",
            id_cols.join(", ")
        );
    }
}
