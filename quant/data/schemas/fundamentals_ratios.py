from __future__ import annotations

from .types import Schema, Field, TimestampTZ, Int64, Float64, String


FUNDAMENTAL_RATIOS_SCHEMA: Schema = Schema(
    fields=[
        Field("symbol_id", Int64(), nullable=False),
        Field("asof", TimestampTZ("UTC", "ns"), nullable=False),
        Field("currency", String(), nullable=True),
        Field("pe", Float64(), nullable=True),
        Field("ev_ebitda", Float64(), nullable=True),
        Field("fcf_yield", Float64(), nullable=True),
        Field("debt_ebitda", Float64(), nullable=True),
        Field("roic", Float64(), nullable=True),
        Field("interest_coverage", Float64(), nullable=True),
    ]
)


def to_pyarrow_schema():
    return FUNDAMENTAL_RATIOS_SCHEMA.to_pyarrow()