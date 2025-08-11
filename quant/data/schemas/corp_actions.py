from __future__ import annotations

from .types import Schema, Field, TimestampTZ, Float64, Int64, String


CORPORATE_ACTIONS_SCHEMA: Schema = Schema(
    fields=[
        Field("symbol_id", Int64(), nullable=False),
        Field("effective_date", TimestampTZ("UTC", "ns"), nullable=False),
        Field("split_ratio", Float64(), nullable=False),
        Field("dividend", Float64(), nullable=False),
        Field("currency", String(), nullable=False),
    ]
)


def to_pyarrow_schema():
    return CORPORATE_ACTIONS_SCHEMA.to_pyarrow()