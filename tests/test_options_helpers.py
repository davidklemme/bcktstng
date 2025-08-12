from datetime import datetime, timedelta, timezone

from quant.sdk.options_helpers import build_covered_call, build_vertical, roll_rule_on_time, roll_rule_on_delta


def test_build_covered_call_structure():
    now = datetime.now(timezone.utc)
    strat = build_covered_call(symbol_id=123, shares=100, call_expiry=now + timedelta(days=30), call_strike=105.0, call_qty=1)
    assert strat.underlying.quantity == 100
    assert len(strat.options) == 1
    leg = strat.options[0]
    assert leg.right == "C" and leg.quantity == -1


def test_build_vertical_structure():
    now = datetime.now(timezone.utc)
    strat = build_vertical(symbol_id=123, expiry=now + timedelta(days=30), lower_strike=95.0, upper_strike=105.0, right="P", qty=2)
    assert strat.underlying.quantity == 0
    assert len(strat.options) == 2
    qsum = sum(l.quantity for l in strat.options)
    assert qsum == 0


def test_roll_rules():
    now = datetime(2024, 1, 10, tzinfo=timezone.utc)
    expiry = now + timedelta(days=3)
    assert roll_rule_on_time(now=now, current_expiry=expiry, days_before_expiry=5)
    assert not roll_rule_on_time(now=now, current_expiry=expiry + timedelta(days=10), days_before_expiry=5)

    assert roll_rule_on_delta(current_delta=0.1, target_range=(0.2, 0.4))
    assert not roll_rule_on_delta(current_delta=0.25, target_range=(0.2, 0.4))