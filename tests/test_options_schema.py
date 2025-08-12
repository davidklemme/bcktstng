import pytest

from quant.data.schemas import Schema, OPTIONS_SCHEMA


def test_options_schema_has_required_fields():
    required = {"ts", "symbol_id", "expiry", "strike", "right", "bid", "ask", "iv", "oi", "vol"}
    field_names = set(OPTIONS_SCHEMA.field_names())
    missing = required - field_names
    assert not missing, f"Missing fields: {missing} in OPTIONS_SCHEMA"


def test_options_schema_pyarrow_optional():
    from quant.data.schemas.options import to_pyarrow_schema

    try:
        pa_schema = to_pyarrow_schema()
        assert {f.name for f in pa_schema} >= {"ts", "symbol_id", "expiry", "strike", "right"}
    except ImportError:
        pytest.skip("pyarrow not available; skipping optional conversion test.")