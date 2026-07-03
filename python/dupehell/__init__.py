from dupehell._core import generate as _generate, GenerateResult
from dupehell.models import DomainSchema
from dupehell.schema import load_and_validate

__all__ = ["generate", "GenerateResult", "DomainSchema", "load_and_validate"]


def generate(
    domain: str,
    size: int,
    seed: int = 42,
    difficulty: str = "medium",
    output_dir: str = ".",
    pools_dir: str = "./assets/pools",
    schemas_dir: str = "./schemas",
) -> GenerateResult:
    # Validate schema before calling Rust
    schema_path = f"{schemas_dir}/{domain}.json"
    load_and_validate(schema_path)
    return _generate(domain, size, seed, difficulty, output_dir, pools_dir, schemas_dir)
