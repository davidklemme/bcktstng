from __future__ import annotations

import json
import logging
import os
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..data.bars_loader import BarRow
from ..data.pit_reader import PITDataReader, BarsStore
from ..data.symbols_repository import SymbolRow
from ..data.costs import load_calculator_from_yaml
from ..engine.execution import ExecutionSimulator, Quote
from ..engine.portfolio import Portfolio
from ..engine.orders import Order
from ..ops.artifacts import ArtifactWriter, compute_params_hash, get_git_sha
from ..ops.performance import compute_performance
from ..ops.metrics import events_total, queue_lag_seconds, fill_slippage_bps, orders_total, fills_total, backtest_step_duration_seconds


DEFAULT_SPREAD_BPS = 5.0  # simple default spread if not otherwise provided

# Fill timing modes.
#   next_open  - orders emitted while observing bar T are filled at bar T+1's
#                open. This is the only mode that is free of same-bar
#                look-ahead and is the default.
#   same_close - legacy behaviour: orders fill against the close of the very
#                bar the strategy just observed. Retained only to reproduce
#                historical runs; it inflates results and warns loudly.
FILL_TIMING_NEXT_OPEN = "next_open"
FILL_TIMING_SAME_CLOSE = "same_close"


@dataclass
class BacktestResult:
    run_id: str
    metrics: Dict[str, Any]
    equity: List[Dict[str, Any]]
    orders: List[Dict[str, Any]]
    fills: List[Dict[str, Any]]
    positions: List[Dict[str, Any]]
    out_dir: Path


def _to_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _unique_times_between(store: BarsStore, start: datetime, end: datetime) -> List[datetime]:
    start = _to_utc(start)
    end = _to_utc(end)
    ts_set: set[datetime] = set()
    for rows in store.by_symbol.values():
        for r in rows:
            if r.ts < start or r.ts > end:
                continue
            ts_set.add(r.ts)
    return sorted(ts_set)


def _bars_by_time(store: BarsStore, start: datetime, end: datetime) -> Dict[datetime, List[BarRow]]:
    mapping: Dict[datetime, List[BarRow]] = {}
    for rows in store.by_symbol.values():
        for r in rows:
            if r.ts < start or r.ts > end:
                continue
            mapping.setdefault(r.ts, []).append(r)
    # Ensure deterministic ordering within same timestamp
    for k in list(mapping.keys()):
        mapping[k].sort(key=lambda x: (x.symbol_id, x.ts))
    return mapping


def _build_symbol_maps(symbols: Iterable[SymbolRow]) -> Tuple[Dict[int, SymbolRow], Dict[int, str]]:
    by_id: Dict[int, SymbolRow] = {}
    venue_by_symbol: Dict[int, str] = {}
    for s in symbols:
        by_id[s.symbol_id] = s
        venue_by_symbol[s.symbol_id] = s.exchange
    return by_id, venue_by_symbol


def _quote_from_bar(bar: BarRow, *, reference_price: Optional[float] = None) -> Quote:
    """Synthetic quote around *reference_price* (defaults to the bar close).

    Fills under ``next_open`` timing pass the bar's open, so that an order
    decided on bar T-1's information transacts at the first price actually
    available on bar T.
    """
    px = bar.close if reference_price is None else reference_price
    spread = (DEFAULT_SPREAD_BPS / 10000.0) * max(px, 1e-9)
    bid = max(1e-9, px - spread / 2.0)
    ask = px + spread / 2.0
    return Quote(bid=bid, ask=ask)


def run_backtest(
    *,
    strategy: Any,
    reader: PITDataReader,
    bars_store: BarsStore,
    start: datetime,
    end: datetime,
    costs_yaml_path: Optional[str] = None,
    base_currency: str = "EUR",
    seed: Optional[int] = None,
    out_dir: Optional[str | Path] = None,
    logger: Optional[logging.Logger] = None,
    starting_cash: float = 0.0,
    fill_timing: str = FILL_TIMING_NEXT_OPEN,
    risk_free_rate: float = 0.0,
) -> BacktestResult:
    start = _to_utc(start)
    end = _to_utc(end)

    rng = random.Random(seed)
    run_id = f"run_{int(datetime.now(tz=timezone.utc).timestamp())}_{rng.randrange(10**6):06d}"

    # Logging
    logger = logger or logging.getLogger("quant.backtest")
    logger.setLevel(logging.INFO)

    if fill_timing not in (FILL_TIMING_NEXT_OPEN, FILL_TIMING_SAME_CLOSE):
        raise ValueError(f"Unknown fill_timing: {fill_timing!r}")
    if fill_timing == FILL_TIMING_SAME_CLOSE:
        logger.warning(
            "fill_timing='same_close' fills orders against the same bar the strategy "
            "observed. This is same-bar look-ahead and inflates results; use "
            "'next_open' for any decision-grade run."
        )

    # Costs and execution simulator
    cost_calc = load_calculator_from_yaml(costs_yaml_path) if costs_yaml_path else None
    exec_sim = ExecutionSimulator(cost_calculator=cost_calc)
    # Used when a venue has no cost profile: the trade still has to happen,
    # but with costs explicitly zeroed and loudly reported.
    costless_sim = ExecutionSimulator(cost_calculator=None)

    # Portfolio
    portfolio = Portfolio(base_currency=base_currency)
    if starting_cash and starting_cash != 0.0:
        portfolio.deposit(float(starting_cash), base_currency)

    # Prepare context
    from ..sdk.strategy import Context as StrategyContext, _DataAPI, _OrderAPI, _RiskAPI

    data_api = _DataAPI(reader)
    order_api = _OrderAPI()
    risk_api = _RiskAPI()

    # Symbol metadata. Resolved as-of `start` and then extended forward, so a
    # symbol that lists mid-run is picked up without inheriting metadata from
    # the future (which an as-of-`end` snapshot would do).
    symbols_by_id, venue_by_symbol = _build_symbol_maps(reader.get_symbols(start))
    for extra in reader.get_symbols(end):
        symbols_by_id.setdefault(extra.symbol_id, extra)
        venue_by_symbol.setdefault(extra.symbol_id, extra.exchange)

    # Build bars index
    times = _unique_times_between(bars_store, start, end)
    bars_at_time = _bars_by_time(bars_store, start, end)

    ctx = StrategyContext(
        now=start,
        data=data_api,
        order_api=order_api,
        portfolio=portfolio,
        risk=risk_api,
        log=logging.getLogger("quant.strategy"),
    )

    # Artifacts in memory
    equity_points: List[Dict[str, Any]] = []
    orders_out: List[Dict[str, Any]] = []
    fills_out: List[Dict[str, Any]] = []
    positions_out: List[Dict[str, Any]] = []

    # Last known close per symbol, so a position whose symbol does not print
    # on a given timestamp keeps its previous mark instead of silently
    # contributing zero to equity.
    last_prices: Dict[int, float] = {}
    gross_traded_notional = 0.0
    periods_in_market = 0
    unknown_venues: set[str] = set()
    order_seq = 0

    def _drain_orders() -> List[Order]:
        nonlocal order_seq
        drained = list(ctx.order_api._orders)
        ctx.order_api._orders.clear()
        for o in drained:
            order_seq += 1
            # _OrderAPI restarts its counter every bar; make ids unique across
            # the whole run so fills can be joined back to orders.
            if o.id.startswith("order_"):
                o.id = f"order_{order_seq:06d}"
            o.acknowledge()
        return drained

    def _execute(order: Order, ts: datetime, bar: BarRow, reference_price: Optional[float]) -> None:
        nonlocal gross_traded_notional
        quote = _quote_from_bar(bar, reference_price=reference_price)
        venue = venue_by_symbol.get(order.symbol_id, "UNKNOWN")
        # Real traded volume caps liquidity. Flooring this at the order size
        # (the previous behaviour) let an oversized order manufacture its own
        # liquidity and always fill 10% of whatever it asked for.
        available_liquidity = max(0, int(bar.volume))
        try:
            fills, cost_total = exec_sim.simulate(order, quote, venue, available_liquidity, ts=ts)
        except ValueError as exc:
            # Typically a venue with no entry in cost_profiles.yml. Aborting a
            # long run mid-loop would leave a half-mutated portfolio and no
            # artifacts, so fill without costs and surface it in the metrics.
            if venue not in unknown_venues:
                unknown_venues.add(venue)
                logger.error(
                    "No cost profile for venue %r (%s); orders on this venue fill with "
                    "ZERO cost. Reported costs are understated until a profile is added.",
                    venue, exc,
                )
            fills, cost_total = costless_sim.simulate(order, quote, venue, available_liquidity, ts=ts)

        sym_meta = symbols_by_id.get(order.symbol_id)
        currency = sym_meta.currency if sym_meta else base_currency

        total_filled = sum(f.quantity for f in fills)
        for f in fills:
            fills_total.inc()
            portfolio.apply_fill(order.symbol_id, currency, order.side.value, f.quantity, f.price)
            order.add_fill(f.quantity)
            gross_traded_notional += abs(f.quantity * f.price)
            mid = (quote.bid + quote.ask) / 2.0
            if mid > 0:
                fill_slippage_bps.observe(abs((f.price - mid) / mid) * 10000.0)
            cost_per_fill = (cost_total / total_filled) * f.quantity if total_filled else 0.0
            fills_out.append({
                "ts": ts,
                "order_id": order.id,
                "symbol_id": order.symbol_id,
                "price": f.price,
                "quantity": f.quantity,
                "venue": venue,
                "cost": cost_per_fill,
            })
        if cost_total:
            # Costs are computed from qty*price in the INSTRUMENT currency, so
            # they must be debited from that currency's cash, not the base.
            portfolio.apply_transaction_cost(currency, cost_total)

        _record_order(order, ts, filled=total_filled, note="")

    def _record_order(order: Order, ts: datetime, *, filled: int, note: str) -> None:
        orders_out.append({
            "ts": ts,
            "order_id": order.id,
            "symbol_id": order.symbol_id,
            "side": order.side.value,
            "quantity": order.quantity,
            "filled_quantity": filled,
            "type": order.type.value,
            "tif": order.tif.value,
            "limit_price": order.limit_price,
            "state": order.state.value,
            "note": note,
        })

    # Strategy lifecycle
    if hasattr(strategy, "on_start"):
        strategy.on_start(ctx)

    # Orders decided on the previous bar, awaiting execution on this bar.
    pending: List[Order] = []

    for ts in times:
        ctx.now = ts
        events_total.labels(source="backtest").inc()
        import time as _time
        _t0 = _time.perf_counter()

        bar_rows = bars_at_time.get(ts, [])
        bars_by_symbol = {b.symbol_id: b for b in bar_rows}

        # (1) Execute orders decided on the PREVIOUS bar, against this bar's
        #     open. Under next_open timing the strategy has not yet seen this
        #     bar, so there is no look-ahead.
        if fill_timing == FILL_TIMING_NEXT_OPEN:
            still_pending: List[Order] = []
            for order in pending:
                bar = bars_by_symbol.get(order.symbol_id)
                if bar is None:
                    # No print for this symbol on this bar; carry the order to
                    # the next one rather than silently dropping it.
                    still_pending.append(order)
                    continue
                _execute(order, ts, bar, reference_price=bar.open)
            pending = still_pending

        # (2) Strategy observes the completed bar and emits orders.
        if hasattr(strategy, "on_event"):
            strategy.on_event(None, ctx)
        new_orders = _drain_orders()
        for _ in new_orders:
            orders_total.inc()

        if fill_timing == FILL_TIMING_SAME_CLOSE:
            for order in new_orders:
                bar = bars_by_symbol.get(order.symbol_id)
                if bar is None:
                    order.cancel()
                    _record_order(order, ts, filled=0, note="no_bar_for_symbol_at_ts")
                    continue
                _execute(order, ts, bar, reference_price=bar.close)
        else:
            pending.extend(new_orders)

        # (3) Mark to this bar's close and record equity.
        for b in bar_rows:
            last_prices[b.symbol_id] = b.close
        equity_eur = portfolio.total_value_eur(ts, dict(last_prices), reader._fx_engine)  # type: ignore[attr-defined]
        equity_points.append({"ts": ts, "equity_eur": equity_eur})

        if any(p.quantity != 0 for p in portfolio.positions.values()):
            periods_in_market += 1

        for pos in portfolio.positions.values():
            positions_out.append({
                "ts": ts,
                "symbol_id": pos.symbol_id,
                "currency": pos.currency,
                "quantity": pos.quantity,
                "average_price": pos.average_price,
            })

        queue_lag_seconds.set(0.0)
        backtest_step_duration_seconds.observe(_time.perf_counter() - _t0)

    # Orders still queued when the data ran out never traded. Record them so
    # `num_orders` reflects intent and nothing disappears silently.
    for order in pending:
        order.cancel()
        _record_order(order, times[-1] if times else start, filled=0, note="expired_no_next_bar")

    if hasattr(strategy, "on_end"):
        strategy.on_end(ctx)
        # on_end runs after the final bar, so there is no subsequent price to
        # fill against. Anything it submits is recorded as unexecuted rather
        # than being silently discarded.
        for order in _drain_orders():
            orders_total.inc()
            order.cancel()
            _record_order(
                order,
                times[-1] if times else start,
                filled=0,
                note="submitted_in_on_end_not_executable",
            )

    # ---- Metrics -------------------------------------------------------
    equity_series = [float(p["equity_eur"]) for p in equity_points]
    ts_series = [p["ts"] for p in equity_points]
    # Fall back to the first observed equity only when the run was genuinely
    # unfunded, and flag it, rather than reporting a fabricated 0.0 return.
    initial_capital = float(starting_cash)
    metrics: Dict[str, Any] = compute_performance(
        equity=equity_series,
        timestamps=ts_series,
        initial_capital=initial_capital,
        risk_free_rate=risk_free_rate,
        gross_traded_notional=gross_traded_notional,
        periods_in_market=periods_in_market,
    )
    metrics.update({
        "num_orders": len(orders_out),
        "num_fills": len(fills_out),
        "num_unfilled_orders": sum(1 for o in orders_out if o["filled_quantity"] == 0),
        "gross_traded_notional": gross_traded_notional,
        "fill_timing": fill_timing,
        # Retained for backward compatibility with existing consumers.
        "final_equity_eur": equity_series[-1] if equity_series else 0.0,
        "start_equity_eur": equity_series[0] if equity_series else 0.0,
        "return": metrics.get("total_return"),
    })
    if unknown_venues:
        metrics["warnings"].append(
            f"missing_cost_profile_for_venues:{sorted(unknown_venues)}; costs understated"
        )
    if starting_cash <= 0:
        metrics["warnings"].append(
            "starting_cash<=0: the run was unfunded, positions were bought on "
            "implicit unlimited credit, and return metrics are undefined"
        )

    # Artifacts and manifest
    out_dir_path = Path(out_dir) if out_dir else Path("./runs") / run_id
    writer = ArtifactWriter(out_dir_path)
    writer.write_equity(equity_points)
    writer.write_orders(orders_out)
    writer.write_fills(fills_out)
    writer.write_positions(positions_out)
    writer.write_metrics(metrics)

    params = {
        "strategy": getattr(strategy, "__class__", type(strategy)).__name__,
        "start": start,
        "end": end,
        "base_currency": base_currency,
        "costs_yaml_path": costs_yaml_path,
        "seed": seed,
        "starting_cash": starting_cash,
        "fill_timing": fill_timing,
    }
    manifest = {
        "run_id": run_id,
        "timestamp": datetime.now(tz=timezone.utc),
        "git_sha": get_git_sha(),
        "params_hash": compute_params_hash(params),
        "params": params,
    }
    writer.write_manifest(manifest)

    return BacktestResult(
        run_id=run_id,
        metrics=metrics,
        equity=equity_points,
        orders=orders_out,
        fills=fills_out,
        positions=positions_out,
        out_dir=out_dir_path,
    )
