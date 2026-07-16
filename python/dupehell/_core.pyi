"""Type stubs for the compiled ``dupehell._core`` extension module.

This file documents the public API surface exposed by the Rust extension
built with PyO3/maturin. It is not imported at runtime; it only provides
editor/type-checker information.
"""

from typing import Optional

class GenerateResult:
    """Result of a :func:`generate` call."""

    dataset: str
    ground_truth: str
    total_records: int
    exact_dups: int
    hard_negs: int
    uniques: int
    masters: int
    # Populated only when ``generate_graph=True``; otherwise ``None``.
    nodes: Optional[str]
    edges: Optional[str]
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

def generate(
    domain: str,
    size: int,
    seed: int,
    difficulty: str,
    output_dir: str,
    locale: str,
    pools_dir: str,
    schemas_dir: str,
    output_format: str,
    hard_neg_ratio: float,
    singleton_master_fraction: float,
    generate_graph: bool = ...,
    graph_format: str = ...,
) -> GenerateResult:
    """Generate a synthetic record-linkage dataset (optionally + a property graph).

    When ``generate_graph`` is true, ``nodes`` and ``edges`` are set to the
    paths of the generated ``{run_id}_nodes.{ext}`` / ``{run_id}_edges.{ext}``
    files; otherwise they are ``None``.
    """
    ...

def estimate_difficulty(
    domain: str,
    size: int,
    seed: int,
    difficulty: str,
    schemas_dir: str,
    hard_neg_ratio: float,
) -> str:
    """Estimate the maximum achievable F1 score without generating data."""
    ...
