"""dupehell — Synthetic record linkage dataset generator.

Generates realistic synthetic datasets with controlled duplicate rates,
hard negatives, and noise profiles for benchmarking record linkage systems.

Educational and research use only — no real PII is used or generated. See
https://github.com/vntoinekaio/DupeHell/blob/master/ETHICS.md for the full
policy and prohibited uses.

Quick start::

    >>> from dupehell import generate, estimate_difficulty, DOMAINS
    >>> est = estimate_difficulty("kyc", 10000, difficulty="medium")
    >>> print(f"Estimated F1: {est.f1_max:.3f}")
    >>> result = generate("kyc", 10000, output_format="parquet")
    >>> print(f"Dataset: {result.dataset}")
"""

import json as _json
from pathlib import Path as _Path

import dupehell._core as _core
import dupehell.models as models
import dupehell.schema as schema

from dupehell._core import generate as _generate, estimate_difficulty as _estimate
from dupehell._core import GenerateResult as _GenerateResult
from dupehell.models import DomainSchema, DifficultyReport as _DifficultyReport
from dupehell.schema import load_and_validate

__all__ = [
    "generate", "estimate_difficulty", "GenerateResult",
    "DomainSchema", "DifficultyReport", "load_and_validate",
    "DOMAINS", "list_domains",
]

GenerateResult = _GenerateResult
DifficultyReport = _DifficultyReport

_VALID_DIFFICULTIES = {"light", "medium", "hard", "hell"}
_VALID_LOCALES = {"en", "fr", "de", "es", "it", "pt"}


def _default_schemas_dir() -> str:
    local = _Path("./schemas")
    if local.is_dir():
        return "./schemas"
    pkg = _Path(__file__).resolve().parent.parent.parent / "schemas"
    if pkg.is_dir():
        return str(pkg)
    return "./schemas"


def _default_pools_dir() -> str:
    local = _Path("./assets/pools")
    if local.is_dir():
        return "./assets/pools"
    pkg = _Path(__file__).resolve().parent.parent.parent / "assets" / "pools"
    if pkg.is_dir():
        return str(pkg)
    return "./assets/pools"


def list_domains(schemas_dir: str | None = None) -> list[str]:
    """List all available domain names.

    Args:
        schemas_dir: Path to directory containing schema JSON files.
            If None (default), tries ``./schemas`` then the package-installed path.

    Returns:
        Sorted list of domain names (without ``.json`` extension).
    """
    if schemas_dir is None:
        schemas_dir = _default_schemas_dir()
    p = _Path(schemas_dir)
    if not p.is_dir():
        return []
    return sorted(f.stem for f in p.iterdir() if f.suffix == ".json")


DOMAINS = list_domains()
"""list[str]: Sorted list of all available domain names discovered at import time."""

# Hide internal references from dir()
del _GenerateResult, _DifficultyReport, _core, models, schema


def generate(
    domain: str,
    size: int,
    seed: int = 42,
    difficulty: str = "medium",
    output_dir: str = ".",
    locale: str = "en",
    pools_dir: str | None = None,
    schemas_dir: str | None = None,
    output_format: str = "ipc",
    hard_neg_ratio: float = 0.3,
    singleton_master_fraction: float = 0.10,
) -> GenerateResult:
    """Generate a synthetic record linkage dataset.

    Args:
        domain: Domain name (e.g. ``"kyc"``, ``"healthcare"``, ``"ecommerce"``).
            Use :func:`list_domains` or :data:`DOMAINS` for available options.
        size: Number of base records to generate. Minimum 10.
            Total output includes duplicates and hard negatives (typically size * 1.01-1.05).
        seed: Random seed for deterministic reproducibility. Same seed + domain = identical output.
        difficulty: Noise/difficulty level. One of ``"light"``, ``"medium"``, ``"hard"``, ``"hell"``.
        output_dir: Directory to write output files (created automatically if missing).
        locale: Locale for pool data. One of ``"en"``, ``"fr"``, ``"de"``, ``"es"``, ``"it"``, ``"pt"``.
            Falls back to ``"en"`` if the requested locale is not available in a pool file.
        pools_dir: Path to the asset pools directory (shipped with the package).
            If None (default), tries ``./assets/pools`` then the package-installed path.
        schemas_dir: Path to the schema JSON directory (shipped with the package).
            If None (default), tries ``./schemas`` then the package-installed path.
        output_format: Output file format. ``"ipc"`` (Arrow IPC) or ``"parquet"`` (ZSTD compressed).

    Returns:
        GenerateResult with paths and statistics.

    Raises:
        ValueError: If ``size`` is out of ``[10, 500_000_000]`` or ``output_format``
            is not ``"ipc"`` or ``"parquet"``.
        FileNotFoundError: If the schema file for *domain* is not found.
            Includes a list of available domains.
        ValidationError (pydantic): If the schema JSON is malformed.
        PyValueError: If the generation pipeline fails internally.

    Example::

        >>> from dupehell import generate
        >>> r = generate("kyc", 1000, seed=1, difficulty="medium", output_format="parquet")
        >>> r.total_records
        1021
    """
    if size < 10:
        raise ValueError(f"size must be >= 10, got {size}")
    if size > 500_000_000:
        raise ValueError(
            f"size must be <= 500000000 (500M), got {size}. Larger runs risk "
            "exhausting memory in a single process; split into multiple runs instead."
        )
    if output_format not in ("ipc", "parquet"):
        raise ValueError(f"output_format must be 'ipc' or 'parquet', got {output_format!r}")
    if difficulty not in _VALID_DIFFICULTIES:
        raise ValueError(f"difficulty must be one of {_VALID_DIFFICULTIES}, got {difficulty!r}")
    if locale.lower() not in _VALID_LOCALES:
        raise ValueError(f"locale must be one of {_VALID_LOCALES}, got {locale!r}")
    if hard_neg_ratio < 0.0:
        raise ValueError(f"hard_neg_ratio must be >= 0.0, got {hard_neg_ratio}")
    if schemas_dir is None:
        schemas_dir = _default_schemas_dir()
    if pools_dir is None:
        pools_dir = _default_pools_dir()
    schema_path = f"{schemas_dir}/{domain}.json"
    try:
        load_and_validate(schema_path)
    except FileNotFoundError:
        available = list_domains(schemas_dir)
        if available:
            msg = (
                f"schema not found for domain '{domain}' at {schema_path}. "
                f"Available domains ({len(available)}): {', '.join(available)}"
            )
        else:
            msg = f"schema not found for domain '{domain}' at {schema_path}. No schemas found in {schemas_dir}/."
        raise FileNotFoundError(msg) from None
    import os as _os
    _os.makedirs(output_dir, exist_ok=True)
    return _generate(domain, size, seed, difficulty, output_dir, locale, pools_dir, schemas_dir, output_format, hard_neg_ratio, singleton_master_fraction)


def estimate_difficulty(
    domain: str,
    size: int = 1_000_000,
    seed: int = 42,
    difficulty: str = "medium",
    schemas_dir: str | None = None,
    hard_neg_ratio: float = 0.3,
) -> DifficultyReport:
    """Estimate the maximum achievable F1 score without generating data.

    Uses column-level heuristics (match utility, noise damage, hard-neg risk)
    rather than a full pipeline run.

    Args:
        domain: Domain name. See :func:`list_domains` or :data:`DOMAINS`.
        size: Number of records to simulate. Larger sizes give more stable estimates.
        seed: Random seed. Must match the seed used for actual generation.
        difficulty: Difficulty level. ``"light"``, ``"medium"``, ``"hard"``, or ``"hell"``.
        schemas_dir: Path to schema JSON directory.
            If None (default), tries ``./schemas`` then the package-installed path.

    Returns:
        DifficultyReport with ``f1_max``, ``precision_max``, ``recall_max``,
        and per-entity column reliability breakdown.

    Raises:
        FileNotFoundError: If the schema file is not found.
            Includes a list of available domains.
        ValidationError (pydantic): If the schema JSON is malformed.

    Example::

        >>> from dupehell import estimate_difficulty
        >>> est = estimate_difficulty("kyc", 10000, difficulty="hard")
        >>> f"{est.f1_max:.1%}"
        '83.3%'
    """
    if difficulty not in _VALID_DIFFICULTIES:
        raise ValueError(f"difficulty must be one of {_VALID_DIFFICULTIES}, got {difficulty!r}")
    if hard_neg_ratio < 0.0:
        raise ValueError(f"hard_neg_ratio must be >= 0.0, got {hard_neg_ratio}")
    if schemas_dir is None:
        schemas_dir = _default_schemas_dir()
    raw = _estimate(domain, size, seed, difficulty, schemas_dir, hard_neg_ratio)
    return DifficultyReport.model_validate(_json.loads(raw))
