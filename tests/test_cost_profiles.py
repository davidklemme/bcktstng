from __future__ import annotations

import os
import tempfile

import pytest

from quant.data.costs import load_calculator_from_yaml, Order


def _write_yaml(text: str) -> str:
    tmpdir = tempfile.mkdtemp()
    path = os.path.join(tmpdir, "profiles.yml")
    with open(path, "w") as f:
        f.write(text)
    return path


def test_us_per_share_with_sec_taf() -> None:
    path = _write_yaml(
        """
US:
  type: per_share_plus_fees
  commission_per_share: 0.01
  sec_fee_bps: 0.2
  taf_per_share: 0.0002
""".strip()
    )
    calc = load_calculator_from_yaml(path)

    # Buy: no SEC/TAF
    cost_buy = calc.cost("US", Order(side="BUY", qty=1000, price=10.0))
    assert cost_buy == pytest.approx(0.01 * 1000)

    # Sell: includes SEC/TAF
    notional = 1000 * 10.0
    expected_sell = 0.01 * 1000 + notional * 0.2 / 10000.0 + 0.0002 * 1000
    cost_sell = calc.cost("US", Order(side="SELL", qty=1000, price=10.0))
    assert cost_sell == pytest.approx(expected_sell)


def test_eu_bps() -> None:
    path = _write_yaml(
        """
EU:
  type: bps
  commission_bps: 1.5
""".strip()
    )
    calc = load_calculator_from_yaml(path)

    cost = calc.cost("EU", Order(side="BUY", qty=200, price=50.0))
    assert cost == pytest.approx(200 * 50.0 * 1.5 / 10000.0)


def test_uk_stamp_toggle() -> None:
    path = _write_yaml(
        """
UK:
  type: bps_with_stamp
  commission_bps: 1.0
  stamp_duty_bps: 50
  stamp_enabled: true
""".strip()
    )
    calc = load_calculator_from_yaml(path)

    # Buy: includes stamp
    notional = 100 * 20.0
    expected = notional * (1.0 / 10000.0) + notional * (50 / 10000.0)
    assert calc.cost("UK", Order(side="BUY", qty=100, price=20.0)) == pytest.approx(expected)

    # Disable stamp: only commission
    path2 = _write_yaml(
        """
UK:
  type: bps_with_stamp
  commission_bps: 1.0
  stamp_duty_bps: 50
  stamp_enabled: false
""".strip()
    )
    calc2 = load_calculator_from_yaml(path2)
    expected2 = notional * (1.0 / 10000.0)
    assert calc2.cost("UK", Order(side="BUY", qty=100, price=20.0)) == pytest.approx(expected2)