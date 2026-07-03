use std::path::PathBuf;

use clap::Parser;

use dupehell2::context::Context;
use dupehell2::pipeline::run_pipeline;
use dupehell2::schema::{build_pipeline_config, load_schema};

#[derive(Parser)]
#[command(name = "dupehell2", version = "0.4.0")]
struct Cli {
    #[arg(long, default_value = "kyc")]
    domain: String,

    #[arg(long, default_value_t = 1_000_000)]
    size: usize,

    #[arg(long, default_value_t = 42)]
    seed: u64,

    #[arg(long, default_value = "medium")]
    difficulty: String,

    #[arg(long, default_value = "ipc")]
    output_format: String,

    #[arg(long)]
    parquet: bool,

    #[arg(long, default_value = ".")]
    output_dir: PathBuf,

    #[arg(long, default_value_t = 0.3)]
    hard_neg_ratio: f64,

    #[arg(long, default_value_t = 0.10)]
    singleton_master_fraction: f64,

    #[arg(long, default_value = "../dupehell/assets/pools")]
    pools_dir: PathBuf,

    #[arg(long, default_value = "schemas")]
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

    let mut ctx = match Context::new(&cli.domain, &cli.pools_dir.to_string_lossy()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error loading pools: {e}");
            std::process::exit(1);
        }
    };

    let run_id = format!("{}_{}", cli.domain, dupehell2::schema::chrono_now());
    let config = match build_pipeline_config(
        &cli.domain,
        cli.size,
        cli.seed,
        &cli.difficulty,
        cli.hard_neg_ratio,
        &schema,
        &run_id,
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
