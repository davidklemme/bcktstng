"""Phase 3: Feature library tests — known-value verification."""

import math
from statistics import mean, pstdev

import pytest

from quant.sdk.features import rolling_mean, zscore, rolling_vol, atr, vol_target


class TestRollingMean:
    def test_basic(self):
        vals = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = rolling_mean(vals, 3)
        assert result[0] is None
        assert result[1] is None
        assert result[2] == pytest.approx(2.0)  # mean(1,2,3)
        assert result[3] == pytest.approx(3.0)  # mean(2,3,4)
        assert result[4] == pytest.approx(4.0)  # mean(3,4,5)

    def test_window_1(self):
        vals = [10.0, 20.0, 30.0]
        result = rolling_mean(vals, 1)
        assert result == [pytest.approx(10.0), pytest.approx(20.0), pytest.approx(30.0)]

    def test_empty(self):
        result = rolling_mean([], 3)
        assert result == []

    def test_window_zero_raises(self):
        with pytest.raises(ValueError, match="positive"):
            rolling_mean([1.0], 0)


class TestZscore:
    def test_known_values(self):
        vals = [10.0, 10.0, 10.0, 20.0]
        result = zscore(vals, 3)
        # First 2 are None
        assert result[0] is None
        assert result[1] is None
        # idx 2: mean=10, std=0 → fallback epsilon → z ≈ 0
        assert result[2] == pytest.approx(0.0, abs=1e-6)
        # idx 3: values [10, 10, 20], mean=40/3, std=pstdev([10,10,20])
        m = mean([10.0, 10.0, 20.0])
        s = pstdev([10.0, 10.0, 20.0])
        expected_z = (20.0 - m) / s
        assert result[3] == pytest.approx(expected_z, rel=1e-6)


class TestRollingVol:
    def test_constant_values_zero_vol(self):
        vals = [5.0, 5.0, 5.0, 5.0]
        result = rolling_vol(vals, 3)
        assert result[2] == pytest.approx(0.0)

    def test_known_std(self):
        vals = [1.0, 2.0, 3.0]
        result = rolling_vol(vals, 3)
        expected = pstdev([1.0, 2.0, 3.0])
        assert result[2] == pytest.approx(expected)


class TestATR:
    def test_single_bar_tr(self):
        """First bar: TR = high - low."""
        high = [110.0]
        low = [90.0]
        close = [100.0]
        result = atr(high, low, close, 1)
        assert result[0] == pytest.approx(20.0)

    def test_two_bars_with_gap(self):
        """Second bar includes gap from previous close."""
        high = [110.0, 120.0]
        low = [90.0, 105.0]
        close = [100.0, 115.0]
        result = atr(high, low, close, 1)
        # Bar 1: max(120-105, |120-100|, |105-100|) = max(15, 20, 5) = 20
        assert result[1] == pytest.approx(20.0)

    def test_atr_averaging(self):
        """ATR of window 2 is average of two TRs."""
        high = [110.0, 120.0, 115.0]
        low = [90.0, 105.0, 108.0]
        close = [100.0, 115.0, 112.0]
        result = atr(high, low, close, 2)
        # TR0 = 20, TR1 = 20, TR2 = max(7, |115-115|, |108-115|) = max(7, 0, 7) = 7
        assert result[0] is None
        assert result[1] == pytest.approx(20.0)  # avg(20, 20)
        assert result[2] == pytest.approx(13.5)  # avg(20, 7)

    def test_mismatched_lengths_raises(self):
        with pytest.raises(ValueError, match="same length"):
            atr([1.0], [1.0, 2.0], [1.0], 1)


class TestVolTarget:
    def test_basic_leverage(self):
        # Constant returns → vol_target leverage = target / realized
        returns = [0.01, -0.01, 0.01, -0.01, 0.01]
        result = vol_target(returns, target_annual_vol=0.15, window=3)
        # Window of 3: [0.01, -0.01, 0.01]
        s = pstdev([0.01, -0.01, 0.01])
        ann = s * math.sqrt(252)
        expected_lev = 0.15 / ann
        assert result[2] == pytest.approx(expected_lev, rel=1e-6)

    def test_window_1_raises(self):
        with pytest.raises(ValueError, match="must be >1"):
            vol_target([0.01], 0.15, window=1)

    def test_zero_vol_capped(self):
        # All same returns → near-zero vol → high leverage (capped by epsilon)
        returns = [0.01, 0.01, 0.01]
        result = vol_target(returns, target_annual_vol=0.15, window=3)
        # pstdev of constant is 0 → epsilon 1e-12 → very large leverage
        assert result[2] is not None
        assert result[2] > 1000  # should be huge
