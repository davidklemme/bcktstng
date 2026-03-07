# Phase 3 Implementation Plan: Validate Properly

## Goal
Write comprehensive tests targeting 80%+ coverage on engine/data layers, add walk-forward validation across market regimes, and add Sharpe comparison infrastructure.

---

## Spec Review & Fixes

The REVIEW.md Phase 3 spec says:
1. "Write comprehensive tests (target 80%+ coverage on engine/data)"
2. "Run walk-forward validation on multiple strategies across multiple market regimes"
3. "Compare backtest Sharpe to paper-trading Sharpe to quantify simulation gap"
4. "Only proceed to Phase 4 if simulated Sharpe > 1.0 after realistic costs"

**Spec issues found:**
- Item 3 requires a paper broker with live market data feed — the paper broker exists but has no live feed. We'll implement an **offline Sharpe comparison** instead: backtest with realistic costs vs backtest without costs, measuring the gap.
- Item 4 is a gate condition, not an implementation task. We'll output the Sharpe and let the user decide.
- Missing from spec: no mention of testing `adjusters.py`, `artifacts.py`, `corp_actions_repository.py`, `visualize.py`, or the SDK `strategy.py` context — all of which have zero or minimal test coverage.

---

## Current Coverage Analysis

### Already well-tested:
- `portfolio.py`: fills, corp actions, FX valuation, validation (test_portfolio.py + test_v2_hardening.py)
- `orders.py`: lifecycle, FOK/IOC/DAY, state machine (test_orders_execution.py + test_v2_hardening.py)
- `execution.py`: spread, ADV, market impact, TOD buckets (test_orders_execution.py + test_v2_hardening.py)
- `fx_repository.py`: ingestion, weekend handling (test_fx_repository.py)
- `symbols_repository.py`: PIT queries (test_symbols_pit.py)
- `cost_profiles`: US/EU/UK costs (test_cost_profiles.py)
- `determinism`: seed reproducibility (test_determinism.py)

### NOT tested (gaps to fill):
| Module | Missing Coverage |
|--------|-----------------|
| `calendars.py` | `next_open()` across holidays, DST transitions, `next_close()` edge cases, multi-day holiday sequences |
| `bars_loader.py` | `load_minute_bars_csv()`, gap detection validation, NaN row skipping |
| `pit_reader.py` | `BarsStore.from_rows()`, `BarsStore.get_between()`, `PITDataReader.get_bars()` no-peek guard, `get_fx()` |
| `features.py` | `atr()` correctness, `vol_target()` leverage calc, `rolling_vol()` known values |
| `risk.py` | Existing position + new order check, gross/net exposure (only symbol-level tested) |
| `events.py` | `EventQueue.pop()` empty, `peek()`, `clear()`, priority ordering across types |
| `adjusters.py` | `apply_actions()` with multiple actions, no-op split_ratio=1.0, `dividend_cashflow_on_exdate()` |
| `backtest.py` | `_compute_metrics()` win_rate/profit_factor, `_quote_from_bar()`, `_build_symbol_maps()`, full integration |
| `strategy.py` | `Context.order()`, `_DataAPI.get()`, `_OrderAPI`, `MultiSymbolStrategy` |
| `artifacts.py` | `ArtifactWriter` write/read roundtrip |
| `validation.py` | `make_purged_kfold_folds()` embargo, `make_walk_forward_folds()` step logic |
| `visualize.py` | `_compute_max_drawdown()` |

---

## Implementation Steps

### Step 1: Test `calendars.py` comprehensively
- DST transition (spring forward/fall back for both XNYS and XETR)
- Multi-day holiday sequence (Thanksgiving + day after)
- `next_open()` from mid-session (should return next day)
- `next_close()` from before open (should return today's close)
- XETR half-day Dec 24 close time
- Weekend boundary (Friday close → Monday open)

### Step 2: Test `bars_loader.py` data validation
- `load_daily_bars_csv()` with NaN rows — verify they're skipped and reported
- `load_daily_bars_csv()` gap detection — verify missing session days detected
- `load_minute_bars_csv()` session filtering — out-of-session rows excluded
- `load_minute_bars_csv()` with `ts` column (absolute timestamps)
- Missing columns raise ValueError

### Step 3: Test `pit_reader.py` PIT guards
- `BarsStore.from_rows()` sorts by timestamp
- `BarsStore.get_between()` filters correctly
- `PITDataReader.get_bars()` no-peek guard raises on future data
- `PITDataReader.get_bars()` with end=None defaults to asof

### Step 4: Test `features.py` known values
- `atr()` with known OHLC → verify against hand-calculated ATR
- `vol_target()` leverage = target_vol / realized_vol
- `rolling_mean()` against numpy reference
- `zscore()` known z-score calculation

### Step 5: Test `events.py` event queue
- Priority ordering: CLOCK < BAR < FX < CORPORATE_ACTION
- Same type, same timestamp → ordered by sequence
- `pop()` from empty raises IndexError
- `peek()` on empty returns None
- `clear()` resets counter

### Step 6: Test `risk.py` with existing positions
- Order that with existing position would exceed symbol cap → rejected
- Order within limits with existing position → allowed
- Zero-quantity position doesn't count toward exposure

### Step 7: Test `adjusters.py`
- `apply_actions()` with 2:1 split
- `apply_actions()` with split_ratio=1.0 (no-op)
- Multiple sequential actions
- `dividend_cashflow_on_exdate()` with multiple dividends

### Step 8: Test `backtest.py` metrics and helpers
- `_compute_metrics()` with known trades → verify win_rate, profit_factor
- `_quote_from_bar()` spread matches DEFAULT_SPREAD_BPS
- `_build_symbol_maps()` correctness

### Step 9: Test `strategy.py` SDK context
- `_OrderAPI.order()` creates valid Order objects
- `_OrderAPI.cancel()` adds to canceled set
- `_RiskAPI.set()` updates caps correctly
- `_DataAPI.get()` requires `at` parameter

### Step 10: Test `artifacts.py` roundtrip
- Write equity/orders/fills/positions → read back and verify

### Step 11: Test `validation.py` fold generation
- `make_walk_forward_folds()` correct train/val split
- `make_purged_kfold_folds()` embargo gap
- Edge cases: too few timesteps, n_splits=2

### Step 12: Walk-forward validation test
- Create integration test that runs walk-forward on a simple strategy with dummy data
- Verify Sharpe is computed per fold
- Verify fold artifacts are written

### Step 13: Sharpe comparison infrastructure
- Add a test/utility that runs backtest with and without costs
- Compare Sharpe ratios to quantify simulation gap
- Output summary for manual review

---

## Files to create/modify
- **Create**: `tests/test_phase3_calendars.py` — comprehensive calendar tests
- **Create**: `tests/test_phase3_data.py` — bars_loader, pit_reader, adjusters tests
- **Create**: `tests/test_phase3_features.py` — feature calculations with known values
- **Create**: `tests/test_phase3_engine.py` — events, risk with positions, execution edge cases
- **Create**: `tests/test_phase3_sdk.py` — strategy context, order API, data API
- **Create**: `tests/test_phase3_orchestrator.py` — backtest metrics, artifacts, validation folds
- **Create**: `tests/test_phase3_integration.py` — walk-forward integration, Sharpe comparison
