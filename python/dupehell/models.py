from pydantic import BaseModel, Field
from typing import Any, Optional


class ColCondition(BaseModel):
    depends_on: str
    op: str  # eq, ne, in, not_in, gt, gte, lt, lte
    value: Any
    action: str  # set_null, set_value, set_pool
    action_value: Optional[Any] = None


class ColumnDef(BaseModel):
    name: str
    type: str = "string"
    pool_name: Optional[str] = None
    nullable: bool = True
    null_rate_default: float = 0.0
    conditions: list[ColCondition] = []


class FkRemap(BaseModel):
    source_col: str
    target_entity: str


class EntitySchema(BaseModel):
    name: str
    columns: list[ColumnDef] = Field(min_length=1)
    fk_remaps: list[FkRemap] = []


class HnSchema(BaseModel):
    entity_type: str
    config_json: str


class DomainSchema(BaseModel):
    domain: str
    entities: list[EntitySchema] = Field(min_length=1)
    hn_types: list[HnSchema] = []
