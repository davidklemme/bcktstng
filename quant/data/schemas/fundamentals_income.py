from __future__ import annotations

from .types import Schema, Field, TimestampTZ, Int64, Float64, String


INCOME_STATEMENT_SCHEMA: Schema = Schema(
    fields=[
        Field("symbol_id", Int64(), nullable=False),
        Field("period_end", TimestampTZ("UTC", "ns"), nullable=False),
        Field("asof", TimestampTZ("UTC", "ns"), nullable=False),
        Field("currency", String(), nullable=True),
        Field("revenue", Float64(), nullable=True),
        Field("ebitda", Float64(), nullable=True),
        Field("net_income", Float64(), nullable=True),
        Field("interest_expense", Float64(), nullable=True),
    ]
)


def to_pyarrow_schema():
    return INCOME_STATEMENT_SCHEMA.to_pyarrow()