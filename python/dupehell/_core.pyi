from typing import Optional


class GenerateResult:
    """Result of a dataset generation.

    Returned by :func:`dupehell.generate`.

    Attributes:
        dataset: Path to the generated dataset file (``.ipc`` or ``.parquet``).
        ground_truth: Path to the ground truth file (same format as dataset).
        total_records: Total number of records in the dataset (base + dups + HN).
        exact_dups: Number of exact duplicate records.
        hard_negs: Number of hard-negative records.
        uniques: Number of unique (singleton) records.
        masters: Number of distinct master entities.
    """
    dataset: str
    ground_truth: str
    total_records: int
    exact_dups: int
    hard_negs: int
    uniques: int
    masters: int

    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...


def generate(
    domain: str,
    size: int,
    seed: int = 42,
    difficulty: str = "medium",
    output_dir: str = ".",
    pools_dir: Optional[str] = None,
    schemas_dir: Optional[str] = None,
    output_format: str = "ipc",
    hard_neg_ratio: float = 0.3,
    singleton_master_fraction: float = 0.10,
) -> GenerateResult:
    """Internal Rust implementation — use :func:`dupehell.generate` instead."""
    ...


def estimate_difficulty(
    domain: str,
    size: int = 1_000_000,
    seed: int = 42,
    difficulty: str = "medium",
    schemas_dir: Optional[str] = None,
    hard_neg_ratio: float = 0.3,
) -> str:
    """Internal Rust implementation — use :func:`dupehell.estimate_difficulty` instead."""
    ...
