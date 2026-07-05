// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::path::PathBuf;

use clap::Parser;

use dupehell::context::Context;
use dupehell::difficulty::estimate_difficulty;
use dupehell::pipeline::run_pipeline;
use dupehell::schema::{build_pipeline_config, load_schema};

#[derive(Parser)]
#[command(
    name = "dupehell",
    version = "0.4.0",
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

    #[arg(long, help = "Output file format: ipc (Arrow IPC) or parquet")]
    output_format: Option<String>,

    #[arg(long, action = clap::ArgAction::Count, help = "Shortcut for --output-format parquet")]
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
        help = "Hard-negative ratio relative to size (0.0 to 1.0+)"
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
}

fn main() {
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

    let schema = match load_schema(&cli.domain, &cli.schemas_dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

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

    let mut ctx = match Context::new(&cli.domain, &cli.locale, &cli.pools_dir.to_string_lossy()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error loading pools: {e}");
            std::process::exit(1);
        }
    };

    let effective_format = match &cli.output_format {
        Some(fmt) => fmt.clone(),
        None if cli.parquet > 0 => "parquet".to_string(),
        None => "ipc".to_string(),
    };

    if effective_format != "ipc" && effective_format != "parquet" {
        eprintln!("Error: output format must be 'ipc' or 'parquet', got '{effective_format}'");
        std::process::exit(1);
    }

    let run_id = format!("{}_{}", cli.domain, dupehell::schema::chrono_now());
    let config = match build_pipeline_config(
        &cli.domain,
        cli.size,
        cli.seed,
        &cli.difficulty,
        cli.hard_neg_ratio,
        &schema,
        &run_id,
        &effective_format,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error building config: {e}");
            std::process::exit(1);
        }
    };

    ctx.enable_watermark(&config.domain, config.size, config.seed);

    eprintln!(
        "DupeHell v0.4 — {} domain, {} records [{}]",
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
    let output = match run_pipeline(&ctx, &config, &cli.output_dir.to_string_lossy()) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Pipeline failed: {e}");
            std::process::exit(1);
        }
    };
    let elapsed = t0.elapsed().as_secs_f64();

    let n = output.stats.total_records;
    eprintln!(
        "\nDone in {:.3}s — {} records ({:.0} rec/s)",
        elapsed,
        n,
        n as f64 / elapsed
    );
    eprintln!(
        "  exact_dups={} hard_negs={} uniques={} masters={}",
        output.stats.exact_dups, output.stats.hard_negs, output.stats.uniques, output.stats.masters
    );
    eprintln!("  Dataset: {}", output.output_files[0]);
    eprintln!("  GT:      {}", output.gt_file);
}
