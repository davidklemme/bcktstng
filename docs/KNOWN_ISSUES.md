# Known Issues

Findings from an adversarial review of the backtesting engine. Items marked
**OPEN** are *not* fixed — they still affect results today. Read this before
treating any backtest number as decision-grade.

Everything below was confirmed by execution, not by reading alone.

---

## Fixed in this pass

| # | Defect | Effect before fix |
|---|--------|-------------------|
| 1 | Same-bar look-ahead: orders filled against the very bar the strategy had just observed | Every backtest inflated by up to a full bar of return. Now defaults to `fill_timing="next_open"` |
| 2 | `return` metric was `0.0` whenever `starting_cash=0`, and absurd (e.g. −119,470%) otherwise | Hyperparameter leaderboards ranked every trial at exactly `0.0` — the "winner" was arbitrary |
| 3 | No performance metrics beyond 5 fields | Sharpe, Sortino, max drawdown, CAGR, Calmar, volatility, exposure, turnover now computed in `quant/ops/performance.py` |
| 4 | Held positions dropped out of equity on any bar where their symbol did not print | Phantom drawdowns and fabricated volatility. Marks now carry forward |
| 5 | Transaction costs computed in instrument currency, debited from base currency | Cost overstated/understated by the FX rate; corrupted the per-currency ledger |
| 6 | Orders submitted from `on_end` were silently discarded | End-of-run flattening never happened; final equity carried phantom open positions |
| 7 | Orders with no bar at that timestamp vanished from artifacts entirely | `num_orders` understated; strategy state desynced from portfolio |
| 8 | Order ids restarted at `order_1` every bar; state never left `NEW` | Any fills→orders join was wrong |
| 9 | FOK bypassed the ADV cap | Flipping the TIF granted unlimited size at the touch (10× observed) |
| 10 | Market fills clamped to `[bid, ask]`, making the impact model and time-of-day spread multipliers dead code | Slippage was exactly 2.5 bps for *any* order size |
| 11 | Position cost basis averaged old and new price on a flip through zero | Basis understated (short 100@100 then buy 300@110 gave 105.0, not 110.0) |
| 12 | FX had no inverse-pair derivation, no identity pair, was case-sensitive | `EUR/USD` raised `LookupError` when only `USD/EUR` was loaded |
| 13 | `.venv/` committed — 4,226 of 4,631 tracked files | Repo bloat; the committed venv was a different Python version and unusable |
| 14 | Two tests performed live HTTP to nasdaq.com | Suite failed offline/CI. Now marked `network` and deselected by default |
| 15 | `evaluate_signal` duplicated across two strategy adapters | Silent drift risk; now one shared module |

---

## OPEN — correctness

### Corporate actions are never applied during a backtest — HIGH
`Portfolio.process_actions_for_symbol` has zero callers in `run_backtest`, and
`bars_loader` does no split adjustment. A 4:1 split mid-run marks a 100-share
position at the post-split price against the pre-split quantity: a **−75%
phantom loss**. Dividends are never credited, so total return understates by
the full yield.

### Exchange calendar is essentially absent — HIGH
`quant/data/calendars.py` hardcodes exactly two 2024 dates (MLK, Presidents')
plus a fixed-date July 4 with no observed-holiday shift. Christmas, Good
Friday, Thanksgiving and New Year's Day are all "open". XETR has no holiday
rules at all. `XNAS` — which `comprehensive_symbols.csv` actually uses —
raises `ValueError: Unsupported exchange`. Backtests trade on days the market
was shut. Replace with `exchange_calendars` / `pandas_market_calendars`.

### Non-session dates are silently restamped — HIGH
`load_daily_bars_csv` stamps every row with `next_close(...)`, which rolls
forward past weekends and holidays. Bars dated Sat 2024-01-06, Sun 01-07 and
Mon 01-08 all collapse onto one timestamp. Fills then read the first duplicate
while marks read the last — fill price and mark price come from different
bars. Ingest performs no session-date validation.

### No cash, margin or borrow constraint — HIGH
`Portfolio.withdraw` has no balance check. A run with $1,000 was driven to
$6,000,000 of notional and −$500 equity; a naked short reached −2,500,000
shares from a zero-cash account. No margin interest, no borrow fee, no
dividends-on-short. Short-biased strategies earn the full move for free.

### Risk limits are never enforced — HIGH
`RiskManager.check` is called nowhere in `quant/`. `run_backtest` constructs a
`_RiskAPI`, passes it to the context, and never consults it; `ctx.order()` has
no gate, so no order can ever be rejected. Even the reachable path is a stub:
only `max_symbol` is implemented — `max_gross`, `max_net` and `max_leverage`
are stored and ignored.

### Symbol cache is frozen at the first event — MEDIUM
`_DataAPI._symbol_cache` is built once and never invalidated
(`if self._symbol_cache: return`). Symbols listing mid-backtest raise
`KeyError` forever; delisted symbols keep resolving. `Context.order` and
`get_available_symbols` re-query at `now`, so they can disagree with
`data.get` about the same ticker.

### Partial fills and cancels are silent — MEDIUM
Fills are applied straight to the portfolio; `Order.add_fill` aside, the
strategy receives no callback, so a strategy tracking its own position
desyncs whenever the ADV cap bites. `ctx.cancel()` writes to a set nothing
reads. `handle_end_of_cycle` (IOC/FOK semantics) is never invoked by the
backtest loop.

### DST transitions resolve silently — LOW (latent)
`load_minute_bars_csv` uses `replace(tzinfo=...)`, which never raises: the
ambiguous 01:30 on US fall-back collapses two distinct minutes onto one UTC
timestamp, and the nonexistent 02:30 on spring-forward is aliased onto a valid
minute. Currently masked because both transitions fall outside RTH, but live
for extended-hours or non-US feeds.

---

## OPEN — realism

- **Market impact is still zero in practice.** The clamp that made it dead
  code is fixed, but `run_backtest` never populates `sigma_by_symbol` or
  `adv_by_symbol`, so `impact` evaluates to 0. Wiring trailing ADV and
  volatility estimates is a modelling decision left to the owner.
- **Spread is a hardcoded 5 bps** (`DEFAULT_SPREAD_BPS`) for every symbol at
  every time. A $3 illiquid name costs the same as AAPL.
- **`available_liquidity` is the bar's volume**, with no participation-rate
  model beyond a flat 10% ADV cap.
- **Unknown venues now warn and fill at zero cost** rather than crashing
  mid-run. `cost_profiles.yml` is keyed by a mix of MIC codes (`XNYS`,
  `XNAS`) and regions (`EU`, `UK`) with no MIC→region mapping, so any
  non-US symbol silently trades cost-free.

---

## OPEN — research methodology

### `run_walk_forward` does not walk forward — HIGH
It evaluates one fixed parameter set on each fold's **validation** range.
`FoldSpec.train_ranges` is computed and written to the summary but never used
for fitting — verified by intercepting every `run_backtest` call: 3 folds
produced 3 backtests, all on validation ranges. Consequently
`run_hyperparameter_search` performs **in-sample selection**: it ranks
parameters by their score on the same data used to score them. Both functions
now carry explicit warnings, but the ranking still carries no out-of-sample
guarantee. Treat `leaderboard.csv` as a sensitivity study only.

### Purged k-fold does not purge label overlap — MEDIUM
`make_purged_kfold_folds` embargoes by index only. With the default
`embargo_fraction=0.01`, a 1,000-bar run embargoes 10 bars — less than the
lookback of most strategies here, so training windows still leak into
validation through the indicator warm-up.

### `hit_rate` is still unimplemented
`Context.md` lists it; computing it needs round-trip trade reconstruction,
which needs the realized-P&L series that `run_backtest` currently discards.

---

## OPEN — the Lumibot / IBKR path

`quant/strategies/lumibot/` **has never been executed.** Lumibot could not be
installed in the development environment: `ibapi`, `free-proxy` and `schwab`
fail to build, and `import lumibot` eagerly imports every broker module. The
shared signal logic is unit-tested, but `on_trading_iteration`, the
`PandasData` wiring and the lifecycle-hook names are unverified. In
particular `before_market_closes` was assumed, not confirmed — a fallback
time check is implemented alongside it for that reason. Install
`requirements-lumibot.txt` and run a real backtest before relying on it.

---

## OPEN — strategy premise (business)

The Geopolitical Whisper entry — a large volume spike with a flat price — is
described as detecting *institutional accumulation*. High volume with no net
price movement is by construction **balanced two-sided flow**: it is equally
consistent with distribution (institutions selling into buyers) as with
accumulation. The signal carries **no directional information**, yet the
strategy is long-only.

With a ~30-minute holding period, the round-trip cost floor is roughly 0.10%
(0.05% assumed slippage per side) plus IBKR commissions. A signal with no
directional edge therefore has a **negative expectancy of at least that
much per trade**. Establishing whether the premise holds requires an event
study — conditional forward-return distributions after qualifying spikes,
against a matched control — before any capital is committed. That study has
not been done.

Corroborating datapoint: with the return metric fixed, every parameter
combination in a 9-trial grid search that actually traded produced a
**negative** mean return. That was previously invisible because all trials
reported exactly `0.0`.
