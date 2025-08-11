from __future__ import annotations

from .types import Schema, Field, TimestampTZ, Float64, String


FX_RATES_SCHEMA: Schema = Schema(
    fields=[
        Field("ts", TimestampTZ("UTC", "ns"), nullable=False),
        Field("base_ccy", String(), nullable=False),
        Field("quote_ccy", String(), nullable=False),
        Field("rate", Float64(), nullable=False),
    ]
)


def to_pyarrow_schema():
    return FX_RATES_SCHEMA.to_pyarrow()