# V2 Fix Specification: Backtesting Engine Hardening

**Date:** 2026-03-06
**Scope:** Fix critical bugs, harden architecture, improve software quality
**Approach:** Business-first → Architecture → Software
**Revision:** 2 (post adversarial review)

---

## Adversarial Review Findings (Incorporated)

1. **Order state machine unused** — `run_backtest` never calls `order.add_fill()` or `handle_end_of_cycle()`. Wire into loop.
2. **Non-atomic apply_fill** — validate all inputs at top of `apply_fill` before any mutation.
3. **Transaction costs in wrong currency** — apply in instrument currency, not base currency.
4. **Cancel path is no-op** — `_canceled` set never read. Wire cancel filtering into order loop.
5. **available_liquidity override** — remove `max(volume, qty)` that defeats ADV cap.
6. **S3 FX fallback** — only cross-currency lookups hit FX; catch LookupError, log, skip position from total (don't use fake rate).
7. **S5/S6 break existing tests** — update tests alongside code changes.
8. **B2 incomplete** — add state reset in base `MultiSymbolStrategy.on_start()`.
9. **B3 edge cases** — define Sharpe: risk-free=0, annualize=sqrt(252). Zero trades=0.0. Inf profit factor capped at 999.
10. **B4 risk check timing** — use quote mid as pre-trade price estimate, check before simulate.
11. **cost_per_fill scoping** — refactor to use named variable, avoid shadowing.

---

## Part 1: Business Priority Fixes

### B1. Remove IndexStrategy Data Leakage
**Fix:** Rewrite to use PIT data reader only. No HTTP fetches, no pickle cache, no `requests` import.

### B2. Strategy State Isolation (expanded)
**Fix:** Add state reset in `MultiSymbolStrategy.on_start()` base class. Also reset `available_symbols`. Bollinger/ROC reset `last_states` in their own `on_start()`.

### B3. Performance Metrics Suite
**Fix:** Compute Sharpe (rf=0, annualize=sqrt(252)), max drawdown (%), win rate, profit factor (cap at 999.0), total trades, avg trade P&L. Return 0.0 for undefined metrics (zero trades, etc).

### B4. Risk Manager Integration
**Fix:** Before calling `exec_sim.simulate()`, call `risk.manager.check()` using quote.mid as price estimate and fx_rate=1.0 (simplified). If rejected, call `order.reject()` and log. Skip simulation.

---

## Part 2: Architecture Fixes

### A1. Fix Type Safety — Remove `# type: ignore`
**Fix:** Import Engine properly. Add `fx_engine` property to PITDataReader.

### A2. Extract Duplicate `_utc_dt()` Utility
**Fix:** Canonical implementation: use the version from fx_repository.py. Create quant/common/utils.py. Replace all copies.

### A3. Simplify `_tod_bucket()`
**Fix:** Flat if/elif with clear time ranges. Remove all nested conditionals and dead code.

### A4. Clean Up Dead Code
**Fix:** Remove lines 71-76 in validation.py.

### A5. Wire Order State Machine (NEW — from adversarial review)
**Fix:** In `run_backtest`, call `order.acknowledge()` before simulation, `order.add_fill()` for each fill, and `order.handle_end_of_cycle()` after. Log final order state.

### A6. Fix Transaction Cost Currency (NEW — from adversarial review)
**Fix:** Apply transaction cost in instrument currency, not base_currency.

### A7. Wire Cancel Path (NEW — from adversarial review)
**Fix:** Before processing orders, filter out orders whose id/tag is in `_canceled` set.

### A8. Fix available_liquidity Override (NEW — from adversarial review)
**Fix:** Use `bar_for_symbol.volume` directly, not `max(volume, qty)`.

---

## Part 3: Software Quality Fixes

### S1–S8: Same as before, with these revisions:
- **S3:** On LookupError, log warning and skip the position/cash from total (do not use fake rate).
- **S4:** Validate at top of `apply_fill()` before any mutation. Validate `deposit`/`withdraw` amounts are non-negative, finite.
- **S5:** Fix FOK reset AND update test assertion in test_orders_execution.py.
- **S6:** Filter NaN rows AND update test assertion in test_bars_loader.py.
- **S7:** Track `(effective_date, split_ratio, dividend)` tuples. Update type annotation.

---

## Implementation Order (Revised — tests interleaved)

1. S1 (bare except)
2. S2 (loop guard)
3. A3 (simplify _tod_bucket)
4. A4 (dead code)
5. S4 (validation) — validate at top of apply_fill
6. S5 (FOK fix) + update test
7. S6 (NaN bars) + update test
8. S7 (corp action tracking)
9. A1 (type safety)
10. A2 (extract _utc_dt)
11. A5 (wire order state machine)
12. A6 (fix cost currency)
13. A7 (wire cancel path)
14. A8 (fix available_liquidity)
15. S3 (FX error handling)
16. B1 (IndexStrategy)
17. B2 (state isolation)
18. B3 (metrics)
19. B4 (risk integration)
20. S8 (comprehensive tests)

---

## Out of Scope (v3)

- Position sizing engine
- Stop losses / circuit breakers
- Per-symbol spread model, partial fills
- Decompose `run_backtest()` (deferred but acknowledged as priority for v3)
- Live trading skeleton
- Structured logging / metrics export
- Data streaming / pagination
- Non-atomic apply_fill rollback (validate-first is sufficient for v2)
