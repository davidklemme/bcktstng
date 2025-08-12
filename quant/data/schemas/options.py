from __future__ import annotations

from .types import Schema, Field, TimestampTZ, Int64, Float64, String

# Minimal options chain schema (Phase 1)
# Fields are intentionally concise to match Context.md minimal model
#   ts: timestamp of quote
#   symbol_id: underlying equity symbol identifier
#   expiry: option expiration timestamp (date at 00:00:00Z)
#   strike: strike price
#   right: "C" for Call, "P" for Put
#   bid/ask: NBBO quotes
#   iv: Black-Scholes implied volatility (annualized, decimal). May be missing in raw data
#   oi: open interest (contracts)
#   vol: volume (contracts)

OPTIONS_SCHEMA = Schema(
    fields=[
        Field("ts", TimestampTZ()),
        Field("symbol_id", Int64()),
        Field("expiry", TimestampTZ()),
        Field("strike", Float64()),
        Field("right", String()),
        Field("bid", Float64(), nullable=True),
        Field("ask", Float64(), nullable=True),
        Field("iv", Float64(), nullable=True),
        Field("oi", Int64(), nullable=True),
        Field("vol", Int64(), nullable=True),
    ]
)


def to_pyarrow_schema():
    return OPTIONS_SCHEMA.to_pyarrow()