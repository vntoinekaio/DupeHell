<!-- DupeHell -- MIT License -->

# Benchmarks

**In measure.**

The numbers previously published here predate several perf/correctness
fixes since made to the pipeline (hunt2407 perf pass, `buf_phone`/`buf_ssn`/
`buf_email` cardinality fixes, `ClusterCsr` rewrite) and were no longer an
accurate picture of current behavior. Rather than leave stale or
potentially misleading figures in place, this file is cleared until a full
re-run is done.

Next session: full benchmark pass across all 40 domains, all 3
difficulties, `--graph` on/off, both output formats (IPC/Parquet), at
multiple scales (1M and beyond) — see `project_benchmark_difficulty_format_backlog`.
