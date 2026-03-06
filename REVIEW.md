# Critical Review: Backtesting & Trading Bot

**Date:** 2026-03-06
**Verdict:** Research-grade prototype. Not production-ready. Strong foundation to build on.

---

## 1. Architecture Review

### What works
The 8-layer modular design (Data → Engine → SDK → Strategies → Orchestration → Research → Ops → Adapters) is clean and well-separated. Entry points via CLI (Typer), HTTP API (FastAPI), and Docker are solid. The adapter pattern (`ExecutionProvider` → `PaperBroker` → `IBKRAdapter`) is the right abstraction for bridging backtest-to-live.

### What doesn't
- **God function:** `run_backtest()` is 230 lines doing data loading, event dispatch, fill processing, portfolio updates, cost calculation, and artifact writing. This must be decomposed.
- **Non-atomic fill processing:** Portfolio position is updated separately from cost application (backtest.py:184-210). If an error occurs between these steps, state is inconsistent.
- **All bars in RAM:** The `BarsStore` loads the entire dataset into memory with no streaming/pagination. This won't scale to multi-year multi-symbol universes.
- **Circular-ish imports:** The CLI imports concrete strategy classes directly instead of using a registry pattern, making it fragile to add new strategies.

### Verdict
**B- architecture.** The layering is right, but the implementation within layers is too monolithic. The data flow (Stooq → CSV → SQLite → PIT reader → backtest loop → artifacts) is linear and clear, which is good. But the lack of atomicity in critical paths and the monolithic backtest function are real risks.

---

## 2. Software Engineering Review

### Critical Bugs (must fix before any use)

| # | Bug | Location | Impact |
|---|-----|----------|--------|
| 1 | Bare `except:` swallows all errors silently | `visualize.py:70,319` | Hides crashes |
| 2 | Infinite loop possible in `next_open()` | `calendars.py:79-109` | Hangs process |
| 3 | Missing FX rate crashes backtest mid-run | `portfolio.py:132-146` | Data loss |
| 4 | No validation on order quantities or cash | `orders.py:60-68`, `portfolio.py:66-70` | Negative cash, negative qty allowed |
| 5 | FOK order logic resets filled quantities | `orders.py:79-80` | Loses fill history |
| 6 | NaN bars pass through loader silently | `bars_loader.py:109-113` | Corrupts indicators |

### Code Quality Issues

- **Code duplication:** UTC conversion duplicated 5 times across files. CSV loading duplicated across 4 files with ~80% similarity. Strategy boilerplate duplicated in Bollinger and ROC.
- **Type safety bypassed:** 3 instances of `# type: ignore`. String-based enums (`side: str`) instead of proper `Enum` types. No mypy/pyright in CI.
- **Hardcoded time boundaries:** Holidays hardcoded for 2024 only. Will silently produce wrong results in 2025+.
- **Strategy state leak:** `last_states` dict in Bollinger/ROC persists across multiple backtests. Second run produces wrong signals.
- **Dead code:** Impossible conditions like `hour == 10 and minute < 0`.

### Testing Gaps

| Tested | NOT Tested |
|--------|-----------|
| Portfolio fill logic | Calendar correctness |
| Order lifecycle | Feature/indicator calculations |
| Execution simulation | Data loading & validation |
| Simple strategy smoke tests | Risk management |
| | Walk-forward validation |
| | Hyperparameter search |
| | Any edge cases or negative tests |

### Verdict
**D+ software quality.** The codebase has 23 identified bugs (6 critical, 7 high). Error handling is poor, type safety is weak, and test coverage has major gaps. The architecture provides a good skeleton, but the implementation needs significant hardening.

---

## 3. Business Logic Review (How to use for what)

### What it can do today
- Run backtests on daily bar data with 4 simple strategies (Bollinger Bands, Rate of Change, MA Crossover, Random Baseline)
- Multi-symbol backtests across US/EU/UK venues with venue-specific cost models
- Walk-forward validation and hyperparameter search (grid/random)
- PIT-safe data access preventing lookahead bias
- Artifact output (equity curves, orders, fills, positions, metrics)
- EUR-based multi-currency portfolio with FX conversion

### What it CANNOT do
- **No meaningful position sizing.** Every strategy trades exactly 100 shares regardless of account size, asset price, or volatility. A $50 stock uses $5K; a $500 stock uses $50K. This alone makes all backtest results meaningless for real capital allocation.
- **Risk manager exists but is never called.** The `RiskManager` class has `check()` methods for per-symbol and gross/net limits, but `run_backtest()` never invokes them. Exposure limits are theater.
- **Only 5 metrics calculated:** final equity, starting equity, return, num orders, num fills. No Sharpe, Sortino, max drawdown, win rate, profit factor, Calmar, or information ratio. You literally cannot evaluate whether a strategy is good.
- **No stop losses.** Positions can run to -90% with no circuit breaker.
- **IndexStrategy breaks backtesting fundamentally.** It fetches live data from Stooq API during backtest runs - this is lookahead bias / data leakage that invalidates all results.

### Verdict
**C- business logic.** The framework is well-designed for strategy development and research, but the strategies themselves are naive toys. The absence of position sizing and risk enforcement means backtest results don't translate to actionable trading decisions. The research tools (walk-forward, hyperparameter search) are the strongest business logic components.

---

## 4. Real-Life Trading Quality & Applicability

### Execution Model Realism

| Aspect | Current Implementation | Reality | Gap |
|--------|----------------------|---------|-----|
| Spread | Fixed 5 bps all symbols | 1 bp (AAPL) to 50+ bps (micro-cap) | 100x off |
| Fills per day | 1 fill per order per day | Continuous throughout session | Oversimplified |
| Market impact | `0.1 * sqrt(qty/ADV)` uncalibrated | Varies by regime, time, urgency | Unreliable |
| Borrow costs | Not modeled | 5-100% annualized for shorts | Fatal for short strategies |
| Overnight gaps | Not modeled | 1-5% gap risk per night | Missing risk |
| Partial fills | Not supported | Standard for larger orders | Missing feature |

### If deployed with $100K today
- **Expected return:** -20% to +10% (vs backtest showing +103%)
- **Expected max drawdown:** 30-50% (unknown from backtest - metric not calculated)
- **Likely outcome:** Account significantly depleted within 1-2 adverse market cycles
- **Root cause:** Fixed sizing, no stops, unrealistic fill simulation, missing costs

### What's needed for live trading
The IBKR adapter exists but explicitly blocks live execution:
```python
def _ensure_live_disabled(self) -> None:
    if not self.dry_run:
        raise ExecutionError("IBKR live mode is not available in this skeleton")
```

Missing for live: order persistence, position reconciliation, quote streaming, error recovery, audit trails, compliance reporting.

### Verdict
**F for live trading.** The backtest results are optimistic by a factor of 2-5x due to unrealistic execution modeling. The platform cannot trade live. The gap between simulated and real performance is large enough that strategies appearing profitable in backtest would likely lose money live.

---

## Distilled Assessment: What to Keep, What to Rebuild

### Keep (strong foundation)
1. **PIT data reader** - Lookahead bias prevention is correctly implemented and valuable
2. **8-layer architecture** - The separation of concerns is sound
3. **Adapter pattern** - The `ExecutionProvider` abstraction is the right bridge to live
4. **Venue-specific cost profiles** - YAML-driven US/EU/UK cost modeling is realistic
5. **Research infrastructure** - Walk-forward folds and hyperparameter search are well-designed
6. **Event-driven design** - The `on_event()` / `Context` pattern is clean
7. **Multi-currency portfolio** - EUR-base with PIT FX is correctly modeled
8. **Artifact system** - Run manifests with git SHA + params hash enable reproducibility

### Rebuild (fundamental gaps)

| Priority | What | Why | Effort |
|----------|------|-----|--------|
| P0 | **Position sizing engine** | Fixed 100 shares makes everything meaningless | 1 week |
| P0 | **Risk manager integration** | Exists but never called; needs pre-trade checks | 3 days |
| P0 | **Performance metrics** | Need Sharpe, Sortino, max DD, win rate minimum | 3 days |
| P0 | **Stop loss / circuit breaker** | Positions can run to total loss | 2 days |
| P1 | **Execution simulator** | Per-symbol spreads, partial fills, realistic impact | 2 weeks |
| P1 | **Fix all 6 critical bugs** | See Software section table | 1 week |
| P1 | **Strategy state isolation** | Strategies leak state across runs | 2 days |
| P1 | **Remove/fix IndexStrategy** | Live HTTP fetches break backtesting integrity | 1 day |
| P2 | **Test coverage** | Calendars, features, risk, data validation untested | 2 weeks |
| P2 | **Decompose run_backtest()** | 230-line god function needs splitting | 1 week |
| P2 | **Borrow cost model** | Critical for any short-selling strategy | 3 days |
| P3 | **Live trading skeleton** | Order persistence, reconciliation, recovery | 4-6 weeks |

### Recommended approach for v2

**Phase 1 (2-3 weeks): Fix the engine**
- Integrate risk manager into backtest loop (pre-trade check on every order)
- Add volatility-scaled position sizing (ATR-based or risk-parity)
- Implement hard stops and max drawdown circuit breaker
- Add full performance metrics suite
- Fix all critical bugs
- Isolate strategy state per run

**Phase 2 (3-4 weeks): Make execution realistic**
- Per-symbol spread model (use historical bid-ask if available, or ADV-based estimate)
- Partial fill simulation for large orders
- Borrow cost model for shorts
- Overnight gap risk model
- Calibrate market impact model against real execution data

**Phase 3 (2-3 weeks): Validate properly**
- Write comprehensive tests (target 80%+ coverage on engine/data)
- Run walk-forward validation on multiple strategies across multiple market regimes
- Compare backtest Sharpe to paper-trading Sharpe to quantify simulation gap
- Only proceed to Phase 4 if simulated Sharpe > 1.0 after realistic costs

**Phase 4 (4-6 weeks): Go live**
- Complete IBKR adapter (or Alpaca for simpler start)
- Order persistence (SQLite or Redis)
- Position reconciliation loop
- Error recovery and circuit breakers
- Audit trail and compliance logging
- Paper trade for 3+ months before real capital

**Total estimated effort to production: 3-4 months**

---

## Bottom Line

You have a **well-architected research skeleton** that correctly solves the hardest problem in backtesting (lookahead bias via PIT data). The software engineering execution is rough but fixable. The fatal weakness is that **nothing about position sizing, risk management, or execution realism is production-grade** - and these are exactly the things that determine whether a strategy makes or loses money in the real world.

The good news: the architecture doesn't need to change. The adapter pattern, event-driven strategies, and research infrastructure are solid foundations. What needs to change is filling in the "hard parts" that the current implementation either skips (position sizing, metrics) or fakes (execution simulation, risk limits).

**For v2: keep the skeleton, gut the muscles, rebuild with real tissue.**
