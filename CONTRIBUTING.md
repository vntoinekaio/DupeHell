<!-- DupeHell -- MIT License . Educational Use Only -->
<!-- EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses. -->

# Contributing

## Setup

```bash
git clone https://github.com/anomalyco/dupehell
cd dupehell

# Python
pip install -e .

# Rust (optional, for the core engine)
cd dupehell-core
pip install -e .    # maturin develop
cd ..
```

## Tests

```bash
# Python (1052 tests)
python -m pytest tests/ -q --tb=short \
  -k "not test_generator_pipeline" \
  --deselect tests/test_generator.py::TestGenerate::test_domain_kyc_pipeline \
  --deselect tests/test_generator.py::TestGenerate::test_generate_light \
  --deselect tests/test_generator.py::TestGenerate::test_generate_deterministic \
  --deselect tests/test_generator.py::TestGenerate::test_generate_cleanup

# Rust (97 tests)
cd dupehell-core && cargo test && cd ..

# Smoke test
dupehell generate --domain kyc --size 200 --verbose
dupehell audit
dupehell profile --domain kyc --size 10000
```

## Code structure

```
dupehell/
├── application/          # Pipeline core
│   ├── generator.py      # generate() orchestrator
│   ├── entity_generation.py  # Python column generation
│   ├── ipc_sink.py       # IPC Feather → Parquet sink
│   ├── pyarrow_sink.py   # numpy dict → Parquet sink
│   ├── rust_fallback.py  # Transparent Rust/Python bridge
│   ├── metrics.py        # Ground truth metrics
│   └── validation.py     # Domain/pools/FK validation
├── domain/schemas/       # 37 domain definitions
├── domains/              # Per-domain noise/HN dispatchers
├── noise/                # 8 Python noise modules
├── ui/                   # Textual TUI wizard
├── models.py             # Pydantic config models
├── __main__.py           # CLI entry point
└── yaml_io.py            # Config YAML/JSON I/O

dupehell-core/
├── src/
│   ├── lib.rs            # PyO3 entry points
│   ├── context.rs        # PoolStore + Config
│   ├── rng.rs            # PCG64 RNG
│   ├── buf_gen.rs        # Buffer generators
│   ├── fast_template.rs  # Template dispatch
│   ├── pool_lookup.rs    # Pool lookups
│   ├── column_gen.rs     # Column generation
│   ├── entity_gen.rs     # Entity batch generation
│   ├── ipc_sink.rs       # IPC FileWriter
│   ├── noise/            # 8 noise modules
│   └── hn_common.rs      # Hard negatives
└── tests/                # Rust unit tests
```

## Adding a domain

1. Create `dupehell/domain/schemas/<name>.py` — define the `DomainPreset`
2. Register in `domain/schemas/__init__.py`
3. Define FK relations in `domain/schemas/registry.py`
4. Add required pools in `assets/pools/`
5. (Optional) Add noise/HN dispatchers in `dupehell/domains/`
6. Test: `dupehell validate --domain <name> && dupehell audit --domain <name>`

## Adding a noise type

1. Python: add the function in `dupehell/noise/<module>.py`
2. Rust: add in `dupehell-core/src/noise/<module>.rs`
3. Register in the noise dispatch
4. Unit tests + integration test

## Principles

- Data flows through numpy arrays in the Python pipeline
- All sinks use Polars lazy (no `collect()` until finalization)
- Rust is used for hot paths (generation, intensive noise)
- Everything has a Python fallback if Rust is not available
- `maintain_order=False` everywhere in sinks
- Single `with_columns` for multiple column additions
