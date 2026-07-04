"""Schema loading and Pydantic validation for domain JSON files."""

import json
from pathlib import Path

from dupehell.models import DomainSchema


def load_and_validate(path: str | Path) -> DomainSchema:
    """Load a domain schema JSON file and validate it against the Pydantic model.

    Performs the following validation:
    - The JSON file must exist and be valid JSON.
    - ``domain``, ``entities``, and each entity's ``columns`` must be present.
    - Each column must have a ``name``.
    - ``fk_remaps`` and ``hn_types`` are optional.

    Args:
        path: Path to the schema JSON file (e.g. ``"schemas/kyc.json"``).

    Returns:
        A validated DomainSchema instance.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file contains invalid JSON.
        ValidationError (pydantic): If the JSON structure does not match the schema model.

    Example::

        >>> from dupehell import load_and_validate, list_domains
        >>> schema = load_and_validate("schemas/kyc.json")
        >>> schema.domain
        'kyc'
        >>> [e.name for e in schema.entities]
        ['natural_person', 'legal_entity']
    """
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return DomainSchema.model_validate(raw)
