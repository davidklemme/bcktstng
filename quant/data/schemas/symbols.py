from __future__ import annotations

from .types import Schema, Field, TimestampTZ, Int64, String


SYMBOLS_SCHEMA: Schema = Schema(
    fields=[
        Field("symbol_id", Int64(), nullable=False),
        Field("ticker", String(), nullable=False),
        Field("exchange", String(), nullable=False),
        Field("currency", String(), nullable=False),
        Field("active_from", TimestampTZ("UTC", "ns"), nullable=False),
        Field("active_to", TimestampTZ("UTC", "ns"), nullable=True),
    ]
)


def to_pyarrow_schema():
    return SYMBOLS_SCHEMA.to_pyarrow()