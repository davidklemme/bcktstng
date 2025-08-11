from __future__ import annotations

from .types import Schema, Field, TimestampTZ, Int64, Float64


BARS_SCHEMA: Schema = Schema(
    fields=[
        Field("ts", TimestampTZ("UTC", "ns"), nullable=False),
        Field("symbol_id", Int64(), nullable=False),
        Field("open", Float64(), nullable=False),
        Field("high", Float64(), nullable=False),
        Field("low", Float64(), nullable=False),
        Field("close", Float64(), nullable=False),
        Field("volume", Int64(), nullable=False),
    ]
)


def to_pyarrow_schema():
    return BARS_SCHEMA.to_pyarrow()