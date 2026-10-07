// DupeHell -- MIT License
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// No liability for misuse.

use std::path::PathBuf;

use clap::Parser;

use dupehell_core::context::Context;
use dupehell_core::difficulty::estimate_difficulty;
use dupehell_core::pipeline::run_pipeline_with_progress;
use dupehell_core::schema::{
    build_pipeline_config, default_singleton_master_fraction, load_schema,
};

#[derive(Parser)]
#[command(
    name = "dupehell",
    version = env!("CARGO_PKG_VERSION"),
    about = "Synthetic record linkage dataset generator",
    long_about = "Generates realistic synthetic datasets with controlled duplicate rates, \
                  hard negatives, and noise profiles for benchmarking record linkage systems. \
                  Supports 40 domains (kyc, healthcare, ecommerce, gaming, ...), \
                  three difficulty levels, and outputs Arrow IPC or Parquet format."
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

    #[arg(long, default_value = "medium", value_parser = clap::builder::PossibleValuesParser::new(["light", "medium", "hell"]), help = "Difficulty level: light, medium, or hell")]
    difficulty: String,

    #[arg(
        long,
        help = "Estimate difficulty and F1 score without generating data"
    )]
    estimate: bool,

    #[arg(
        long,
        help = "Pin the process to performance (P) cores only, on hybrid P-core/E-core CPUs (Windows only; no-op elsewhere or on non-hybrid CPUs)"
    )]
    pcore_only: bool,

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
        help = "Hard-negative scaling knob, not a literal fraction: actual count ~= n_duplicates * hard_neg_ratio * 0.125, where n_duplicates depends on --difficulty (light ~0.28*size, medium ~0.40*size, hell ~0.57*size); at medium + default 0.3 this is ~1.5% of size. Use --estimate to see the exact count first."
    )]
    hard_neg_ratio: f64,

    #[arg(
        long,
        help = "Fraction of masters with only one record (0.0 to 1.0). Defaults \
                to the chosen --difficulty tier's own value (0.50/0.30/0.10 \
                for light/medium/hell, see schema::default_singleton_master_fraction) \
                — pass this explicitly only to override that tier default."
    )]
    singleton_master_fraction: Option<f64>,

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

    #[arg(
        long,
        help = "Generate only this entity (e.g. --domain aviation --only-entity passenger). \
                --size then applies entirely to this entity instead of being split across the \
                domain's entities by weight. Other entities it references via FK are generated \
                only far enough to seed a plausible-looking identifier pool (capped, never \
                written to output); entities it doesn't reference are skipped entirely. Output \
                schema is narrowed to just this entity's own columns."
    )]
    only_entity: Option<String>,

    #[arg(
        long,
        help = "DEPRECATED — normally unnecessary: a single run no longer needs RAM in \
                proportion to --size. Generates internally as ceil(size / chunk-size) \
                sequential, independently-seeded chunks, then assembles them into the same \
                single dataset/GT/graph files (record_id/master_id stay globally contiguous), \
                needing twice the disk space while it runs. Kept for compatibility. No effect \
                if omitted or >= --size."
    )]
    chunk_size: Option<usize>,

    #[arg(
        long,
        help = "Skip ground-truth classification and don't write the \
                _ground_truth file (about a third of the dataset's size) — \
                for stress-test runs that only need the dataset itself. \
                Incompatible with --graph, which needs the ground-truth \
                cluster map to emit duplicate-cluster edges."
    )]
    skip_ground_truth: bool,
}

/// RAM per duplicate-cluster row while `--graph` builds its cluster map
/// (`gt::ClusterCsr`: 16-byte pairs, then an 8-byte record buffer).
const GRAPH_BYTES_PER_CLUSTER_ROW: u64 = 24;

/// Without `--graph`, generation streams in fixed-size batches: peak RAM
/// barely moves with `--size` (measured 0.49 GB at 2M rows vs 0.57 GB at
/// 20M, aviation/hell). `--graph` is the exception — its cluster map holds
/// one entry per duplicate-cluster row until the end of the run — so it's
/// the only case worth warning about. Advisory only; silent if system
/// memory can't be read.
fn warn_if_graph_memory_tight(cluster_rows: u64) {
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let available = sys.available_memory();
    let needed = cluster_rows * GRAPH_BYTES_PER_CLUSTER_ROW;
    if available > 0 && needed > available {
        eprintln!(
            "Warning: --graph keeps one entry per duplicate-cluster row in RAM (~{:.1} GB \
             for this run, ~{:.1} GB free) — the run may swap. Consider a smaller --size.",
            gb(needed),
            gb(available),
        );
    }
}

/// Refuses a run whose estimated output doesn't fit on the output volume
/// (and warns when it would leave it nearly full). The estimate is
/// calibrated on real runs (`pipeline::estimate_output_bytes`); the run is
/// additionally stopped mid-way if free space ever drops too low
/// (`disk::DiskGuard`), in case it's off. Skipped if free space is unknown.
fn check_disk_space(output_dir: &std::path::Path, needed: u64) {
    log::debug!("[disk] estimated output: {needed} bytes");
    let Some(free) = dupehell_core::disk::available_space(output_dir) else {
        return;
    };
    if needed > free {
        eprintln!(
            "Error: this run needs ~{:.1} GB of disk space, but only {:.1} GB is free on \
             the disk holding {}. Free some space, choose another --output-dir, use a \
             smaller --size, or switch to --output-format parquet (6-12x smaller).",
            gb(needed),
            gb(free),
            output_dir.display()
        );
        std::process::exit(1);
    }
    if needed > free / 10 * 8 {
        eprintln!(
            "Warning: this run needs ~{:.1} GB of disk space, {:.0}% of the {:.1} GB free \
             on the disk holding {}.",
            gb(needed),
            needed as f64 / free as f64 * 100.0,
            gb(free),
            output_dir.display()
        );
    }
}

fn gb(bytes: u64) -> f64 {
    dupehell_core::disk::gb(bytes)
}

fn main() {
    env_logger::init();
    let cli = Cli::parse();

    if cli.pcore_only {
        if dupehell_core::cpu_affinity::pin_to_p_cores() {
            eprintln!("pcore-only: process pinned to P-cores");
        } else {
            eprintln!("pcore-only: no hybrid P-core/E-core topology detected, ignoring");
        }
    }

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
    // A typo here must not silently fall back to the wall clock — the whole
    // point of setting it is a byte-reproducible output.
    if let Err(e) = dupehell_core::pipeline::source_date_epoch() {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
    if cli.skip_ground_truth && cli.graph {
        eprintln!(
            "Error: --skip-ground-truth is incompatible with --graph (duplicate-cluster \
             edges need the ground-truth cluster map)."
        );
        std::process::exit(1);
    }
    let chunked = match cli.chunk_size {
        Some(0) => {
            eprintln!("Error: --chunk-size must be >= 1, got 0");
            std::process::exit(1);
        }
        Some(cs) if cs < cli.size => {
            eprintln!(
                "Note: --chunk-size is deprecated and normally unnecessary — a single run \
                 no longer needs RAM in proportion to --size. It still works, at the cost \
                 of an extra assembly pass and twice the disk space while it runs."
            );
            true
        }
        _ => false,
    };

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

    // Defaults to the chosen difficulty tier's own singleton fraction unless
    // explicitly overridden — previously this silently defaulted to 0.10
    // (the "hell" tier value) regardless of --difficulty, so every tier's
    // duplicate volume was effectively pinned at "hell" levels unless the
    // caller happened to also pass this flag by hand.
    let tier_default_singleton = default_singleton_master_fraction(&cli.difficulty);
    let singleton_master_fraction = match cli.singleton_master_fraction {
        Some(v) => {
            if (v - tier_default_singleton).abs() > f64::EPSILON {
                eprintln!(
                    "Warning: --singleton-master-fraction {v} overrides the '{}' tier's \
                     default ({tier_default_singleton}) — duplicate volume will differ from \
                     what --estimate reports, which always uses the tier default.",
                    cli.difficulty
                );
            }
            v
        }
        None => tier_default_singleton,
    };

    let run_id = dupehell_core::schema::deterministic_run_id(
        &cli.domain,
        cli.size,
        cli.seed,
        &cli.difficulty,
        cli.hard_neg_ratio,
        singleton_master_fraction,
        &cli.locale,
        cli.only_entity.as_deref(),
        cli.chunk_size,
    );
    let mut config = match build_pipeline_config(
        &cli.domain,
        cli.size,
        cli.seed,
        &cli.difficulty,
        cli.hard_neg_ratio,
        singleton_master_fraction,
        &schema,
        &run_id,
        &effective_format,
        cli.graph,
        &cli.graph_format,
        cli.only_entity.as_deref(),
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error building config: {e}");
            std::process::exit(1);
        }
    };
    config.skip_ground_truth = cli.skip_ground_truth;

    if let Err(e) = dupehell_core::pipeline::check_capacity(&config) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
    check_disk_space(
        &cli.output_dir,
        dupehell_core::pipeline::estimate_output_bytes(&config, chunked),
    );
    if cli.graph {
        // Chunked runs build one cluster map per chunk.
        let rows = dupehell_core::pipeline::max_cluster_rows(&config);
        let per_run = match cli.chunk_size {
            Some(cs) if chunked => rows * cs as u64 / cli.size as u64,
            _ => rows,
        };
        warn_if_graph_memory_tight(per_run);
    }

    // `run_id` is deterministic (it hashes every parameter that
    // affects the data), so a matching file only exists here
    // if this exact run was already generated before — warn instead of
    // silently overwriting it. Advisory only: there's no
    // `--force` gate, since deliberately regenerating an identical run is a
    // normal, common thing to want to do.
    let dataset_ext = if effective_format == "parquet" {
        "parquet"
    } else {
        "ipc"
    };
    let dataset_path = cli.output_dir.join(format!("{run_id}.{dataset_ext}"));
    if dataset_path.exists() {
        eprintln!(
            "Warning: {} already exists and will be overwritten by this run.",
            dataset_path.display()
        );
    }

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
    let output_dir_str = cli.output_dir.to_string_lossy();
    let output = match cli.chunk_size {
        Some(cs) if cs > 0 && cs < cli.size => dupehell_core::pipeline::run_chunked(
            &ctx,
            &cli.domain,
            cli.size,
            cs,
            cli.seed,
            &cli.difficulty,
            cli.hard_neg_ratio,
            singleton_master_fraction,
            &schema,
            &run_id,
            &effective_format,
            cli.graph,
            &cli.graph_format,
            cli.only_entity.as_deref(),
            &output_dir_str,
            Some(&mut progress_cb),
            cli.skip_ground_truth,
        ),
        _ => run_pipeline_with_progress(&ctx, &config, &output_dir_str, Some(&mut progress_cb)),
    };
    let output = match output {
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
    if cli.skip_ground_truth {
        eprintln!("  ground truth: skipped (--skip-ground-truth)");
    } else {
        eprintln!(
            "  exact_dups={} fuzzy_dups={} hard_negs={} uniques={} masters={}",
            output.stats.exact_dups,
            output.stats.fuzzy_dups,
            output.stats.hard_negs,
            output.stats.uniques,
            output.stats.masters
        );
    }
    eprintln!("  Dataset: {}", output.output_files[0]);
    if !cli.skip_ground_truth {
        eprintln!("  GT:      {}", output.gt_file);
    }
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
