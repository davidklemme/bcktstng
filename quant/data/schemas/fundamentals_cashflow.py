from __future__ import annotations

from .types import Schema, Field, TimestampTZ, Int64, Float64, String


CASHFLOW_STATEMENT_SCHEMA: Schema = Schema(
    fields=[
        Field("symbol_id", Int64(), nullable=False),
        Field("period_end", TimestampTZ("UTC", "ns"), nullable=False),
        Field("asof", TimestampTZ("UTC", "ns"), nullable=False),
        Field("currency", String(), nullable=True),
        Field("operating_cf", Float64(), nullable=True),
        Field("investing_cf", Float64(), nullable=True),
        Field("financing_cf", Float64(), nullable=True),
        Field("free_cash_flow", Float64(), nullable=True),
    ]
)


def to_pyarrow_schema():
    return CASHFLOW_STATEMENT_SCHEMA.to_pyarrow()