from __future__ import annotations

from .types import Schema, Field, TimestampTZ, Int64, Float64, String


BALANCE_SHEET_SCHEMA: Schema = Schema(
    fields=[
        Field("symbol_id", Int64(), nullable=False),
        Field("period_end", TimestampTZ("UTC", "ns"), nullable=False),
        Field("asof", TimestampTZ("UTC", "ns"), nullable=False),
        Field("currency", String(), nullable=True),
        Field("total_assets", Float64(), nullable=True),
        Field("total_liabilities", Float64(), nullable=True),
        Field("total_equity", Float64(), nullable=True),
        Field("net_debt", Float64(), nullable=True),
    ]
)


def to_pyarrow_schema():
    return BALANCE_SHEET_SCHEMA.to_pyarrow()