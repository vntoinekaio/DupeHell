"""Pydantic models for dupehell schema validation and difficulty estimation."""

from pydantic import BaseModel, Field
from typing import Any, Optional


class ColCondition(BaseModel):
    """A conditional rule that modifies a column's value based on another column.

    Used within :class:`ColumnDef` to create data dependencies
    (e.g., "if country == 'US' then set postal_code to a US format pool").

    Attributes:
        depends_on: Name of the column to evaluate.
        op: Comparison operator. One of ``"eq"``, ``"ne"``, ``"in"``, ``"not_in"``,
            ``"gt"``, ``"gte"``, ``"lt"``, ``"lte"``.
        value: Value to compare against (type varies by column).
        action: What to do when the condition matches. One of ``"set_null"``,
            ``"set_value"``, ``"set_pool"``.
        action_value: Parameter for the action (the value or pool name to set).
    """
    depends_on: str
    op: str = Field(description="One of: eq, ne, in, not_in, gt, gte, lt, lte")
    value: Any
    action: str = Field(description="One of: set_null, set_value, set_pool")
    action_value: Optional[Any] = None


class ColumnDef(BaseModel):
    """Definition of a single column in an entity schema.

    Attributes:
        name: Column name (e.g. ``"first_name"``, ``"email"``).
        type: Data type. One of ``"string"``, ``"int"``, ``"float"``, ``"bool"``, ``"date"``.
        pool_name: Name of a value pool to draw from (shipped in assets/pools/).
        nullable: Whether the column allows null values.
        null_rate_default: Base probability (0-1) of a null value for this column.
        conditions: Conditional rules that override values based on other columns.
    """
    name: str
    type: str = "string"
    pool_name: Optional[str] = None
    nullable: bool = True
    null_rate_default: float = 0.0
    conditions: list[ColCondition] = []


class FkRemap(BaseModel):
    """A foreign key remap rule: replace a column's values with target entity identifiers.

    Attributes:
        source_col: Column in the source entity that holds the FK reference.
        target_entity: Entity whose identifier column provides the replacement values.
    """
    source_col: str
    target_entity: str


class EntitySchema(BaseModel):
    """Schema definition for a single entity type within a domain.

    Attributes:
        name: Entity name (e.g. ``"customer"``, ``"patient"``).
        columns: List of column definitions. Must have at least one column.
        fk_remaps: Foreign key remap rules for cross-entity reference integrity.
    """
    name: str
    columns: list[ColumnDef] = Field(min_length=1)
    fk_remaps: list[FkRemap] = []


class HnSchema(BaseModel):
    """Schema definition for a hard-negative type.

    Hard negatives are records that appear to match but belong to different entities —
    they test a linker's ability to distinguish lookalikes.

    Attributes:
        name: Name of this HN type (informational).
        entity_type: The entity type these HN records belong to.
        config_json: JSON string with HN generation parameters
            (e.g. ``'{"same_field": "email"}'``).
    """
    name: str
    entity_type: str
    config_json: str


class DomainSchema(BaseModel):
    """Top-level schema model for a domain.

    Validated automatically by :func:`dupehell.load_and_validate` before generation.

    Attributes:
        domain: Domain name (matches the JSON filename without extension).
        entities: Entity type definitions for this domain.
        hn_types: Hard-negative type definitions.
    """
    domain: str
    entities: list[EntitySchema] = Field(min_length=1)
    hn_types: list[HnSchema] = []


class ColReliability(BaseModel):
    """Per-column reliability metrics used in difficulty estimation.

    Attributes:
        name: Column name.
        col_type: Column type (``"match"``, ``"noise"``, etc.).
        noise_damage: Expected false-negative rate from noise applied to this column.
        hn_risk: Expected false-positive rate from hard-negatives on this column.
        reliability: Overall column reliability (1 - total error from noise + HN).
    """
    name: str
    col_type: str
    noise_damage: float
    hn_risk: float
    reliability: float


class EntityDifficulty(BaseModel):
    """Difficulty breakdown for a single entity type.

    Attributes:
        name: Entity type name.
        n_base: Number of base (unique) records planned.
        n_dup: Number of duplicate records planned.
        true_pairs: Estimated number of true-match record pairs.
        hard_neg_pairs: Estimated number of hard-negative pairs.
        guaranteed_fp: Estimated false positives that cannot be avoided.
        guaranteed_fn: Estimated false negatives that cannot be avoided.
        columns: Per-column reliability breakdown.
    """
    name: str
    n_base: int
    n_dup: int
    true_pairs: int
    hard_neg_pairs: int
    guaranteed_fp: int
    guaranteed_fn: int
    columns: list[ColReliability]


class DifficultyReport(BaseModel):
    """Difficulty estimation report for a domain at a given difficulty level.

    Returned by :func:`dupehell.estimate_difficulty`. All metrics are
    theoretical maximums assuming an optimal linker.

    Attributes:
        domain: Domain name.
        difficulty: Difficulty level used for estimation.
        size: Number of records used in the estimate.
        total_true_pairs: Total true-match record pairs across all entities.
        total_hard_neg_pairs: Total hard-negative pairs.
        total_guaranteed_fp: Total unavoidable false positives.
        total_guaranteed_fn: Total unavoidable false negatives.
        precision_max: Maximum achievable precision (TP / (TP + FP)).
        recall_max: Maximum achievable recall (TP / (TP + FN)).
        f1_max: Maximum achievable F1 score (harmonic mean of precision and recall).
        entities: Per-entity difficulty breakdown.
    """
    domain: str
    difficulty: str
    size: int
    total_true_pairs: int
    total_hard_neg_pairs: int
    total_guaranteed_fp: int
    total_guaranteed_fn: int
    precision_max: float
    recall_max: float
    f1_max: float
    entities: list[EntityDifficulty]
