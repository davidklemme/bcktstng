from quant.options.early_exercise import early_exercise_probability_call, should_exercise_early_call


def test_early_exercise_probability_increases_with_dividend():
    # Deep ITM call
    p_low = early_exercise_probability_call(spot=120, strike=100, rate=0.01, time_years=0.01, dividend_cash=0.1, option_time_value=1.0)
    p_high = early_exercise_probability_call(spot=120, strike=100, rate=0.01, time_years=0.01, dividend_cash=2.0, option_time_value=1.0)
    assert 0.0 <= p_low < p_high <= 1.0


def test_should_exercise_threshold():
    assert not should_exercise_early_call(probability=0.49)
    assert should_exercise_early_call(probability=0.5)