import pytest

from quant.data.schemas import (
    Schema,
    BARS_SCHEMA,
    FX_RATES_SCHEMA,
    CORPORATE_ACTIONS_SCHEMA,
    SYMBOLS_SCHEMA,
)


@pytest.mark.parametrize(
    "schema_obj, required_fields",
    [
        (BARS_SCHEMA, {"ts", "symbol_id", "open", "high", "low", "close", "volume"}),
        (FX_RATES_SCHEMA, {"ts", "base_ccy", "quote_ccy", "rate"}),
        (
            CORPORATE_ACTIONS_SCHEMA,
            {"symbol_id", "effective_date", "split_ratio", "dividend", "currency"},
        ),
        (
            SYMBOLS_SCHEMA,
            {"symbol_id", "ticker", "exchange", "currency", "active_from", "active_to"},
        ),
    ],
)
def test_schemas_have_required_fields(schema_obj: Schema, required_fields: set[str]) -> None:
    field_names = set(schema_obj.field_names())
    missing = required_fields - field_names
    assert not missing, f"Missing fields: {missing} in {schema_obj}"


def test_pyarrow_conversion_optional() -> None:
    from quant.data.schemas.bars import to_pyarrow_schema as bars_to_pa

    try:
        pa_schema = bars_to_pa()
        assert {f.name for f in pa_schema} >= {"ts", "symbol_id", "open", "high", "low", "close", "volume"}
    except ImportError:
        pytest.skip("pyarrow not available; skipping optional conversion test.")