import json
from pathlib import Path

from dupehell.models import DomainSchema


def load_and_validate(path: str | Path) -> DomainSchema:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return DomainSchema.model_validate(raw)
