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
from ..ops.metrics import events_total, queue_lag_seconds, fill_slippage_bps, orders_total, fills_total, backtest_step_duration_seconds


DEFAULT_SPREAD_BPS = 5.0  # simple default spread if not otherwise provided


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


def _quote_from_bar(bar: BarRow) -> Quote:
    # Simple synthetic quote around close
    spread = (DEFAULT_SPREAD_BPS / 10000.0) * max(bar.close, 1e-9)
    bid = max(1e-9, bar.close - spread / 2.0)
    ask = bar.close + spread / 2.0
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
) -> BacktestResult:
    start = _to_utc(start)
    end = _to_utc(end)

    rng = random.Random(seed)
    run_id = f"run_{int(datetime.now(tz=timezone.utc).timestamp())}_{rng.randrange(10**6):06d}"

    # Logging
    logger = logger or logging.getLogger("quant.backtest")
    logger.setLevel(logging.INFO)

    # Costs and execution simulator
    cost_calc = load_calculator_from_yaml(costs_yaml_path) if costs_yaml_path else None
    exec_sim = ExecutionSimulator(cost_calculator=cost_calc)

    # Portfolio
    portfolio = Portfolio(base_currency=base_currency)

    # Prepare context
    order_api = strategy.order_api if hasattr(strategy, "order_api") else None
    # Initialize context compatible with Strategy.Context
    from ..sdk.strategy import Context as StrategyContext, _DataAPI, _OrderAPI, _RiskAPI

    data_api = _DataAPI(reader)
    order_api = _OrderAPI()
    risk_api = _RiskAPI()

    # Symbol metadata
    symbol_rows = reader.get_symbols(end)
    symbols_by_id, venue_by_symbol = _build_symbol_maps(symbol_rows)

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

    # Strategy lifecycle
    if hasattr(strategy, "on_start"):
        strategy.on_start(ctx)

    for ts in times:
        ctx.now = ts

        # Metrics: one step processed
        events_total.labels(source="backtest").inc()

        # Optionally track wall time of the step
        import time as _time
        _t0 = _time.perf_counter()

        if hasattr(strategy, "on_event"):
            strategy.on_event(None, ctx)

        # Process orders emitted this cycle
        pending: List[Order] = list(ctx.order_api._orders)
        ctx.order_api._orders.clear()

        for order in pending:
            orders_total.inc()
            # Find bar for this symbol at ts
            bar_rows = bars_at_time.get(ts, [])
            bar_for_symbol: Optional[BarRow] = next((b for b in bar_rows if b.symbol_id == order.symbol_id), None)
            if bar_for_symbol is None:
                # No trade for this symbol at this timestamp
                continue
            quote = _quote_from_bar(bar_for_symbol)
            venue = venue_by_symbol.get(order.symbol_id, "UNKNOWN")
            available_liquidity = max(int(bar_for_symbol.volume), order.quantity)
            fills, cost_total = exec_sim.simulate(order, quote, venue, available_liquidity, ts=ts)

            # Apply fills to portfolio
            sym_meta = symbols_by_id.get(order.symbol_id)
            currency = sym_meta.currency if sym_meta else base_currency
            for f in fills:
                fills_total.inc()
                side = order.side.value
                portfolio.apply_fill(order.symbol_id, currency, side, f.quantity, f.price)
                # Slippage vs mid in bps
                mid = (quote.bid + quote.ask) / 2.0
                if mid > 0:
                    bps = abs((f.price - mid) / mid) * 10000.0
                    fill_slippage_bps.observe(bps)
                fills_out.append(
                    {
                        "ts": ts,
                        "order_id": order.id,
                        "symbol_id": order.symbol_id,
                        "price": f.price,
                        "quantity": f.quantity,
                        "venue": venue,
                        "cost": 0.0,  # cost recorded per order; not per fill here
                    }
                )
            if cost_total and cost_total != 0.0:
                portfolio.apply_transaction_cost(base_currency, cost_total)

            orders_out.append(
                {
                    "ts": ts,
                    "order_id": order.id,
                    "symbol_id": order.symbol_id,
                    "side": order.side.value,
                    "quantity": order.quantity,
                    "type": order.type.value,
                    "tif": order.tif.value,
                    "limit_price": order.limit_price,
                    "state": order.state.value,
                }
            )

        # Mark portfolio and record equity
        mark_prices: Dict[int, float] = {}
        for b in bars_at_time.get(ts, []):
            mark_prices[b.symbol_id] = b.close
        equity_eur = portfolio.total_value_eur(ts, mark_prices, reader._fx_engine)  # type: ignore[attr-defined]
        equity_points.append({"ts": ts, "equity_eur": equity_eur})

        # Snapshot positions
        for pos in portfolio.positions.values():
            positions_out.append(
                {
                    "ts": ts,
                    "symbol_id": pos.symbol_id,
                    "currency": pos.currency,
                    "quantity": pos.quantity,
                    "average_price": pos.average_price,
                }
            )

        # End of step metrics
        queue_lag_seconds.set(0.0)
        backtest_step_duration_seconds.observe(_time.perf_counter() - _t0)

    if hasattr(strategy, "on_end"):
        strategy.on_end(ctx)

    # Metrics (simple)
    final_equity = equity_points[-1]["equity_eur"] if equity_points else 0.0
    start_equity = equity_points[0]["equity_eur"] if equity_points else 0.0
    ret = (final_equity / start_equity - 1.0) if start_equity not in (0.0, 0) else 0.0
    metrics = {
        "final_equity_eur": final_equity,
        "start_equity_eur": start_equity,
        "return": ret,
        "num_orders": len(orders_out),
        "num_fills": len(fills_out),
    }

    # Artifacts and manifest
    out_dir_path = Path(out_dir) if out_dir else Path("./runs") / run_id
    writer = ArtifactWriter(out_dir_path)
    writer.write_equity(equity_points)
    writer.write_orders(orders_out)
    writer.write_fills(fills_out)
    writer.write_positions(positions_out)
    writer.write_metrics(metrics)

    manifest = {
        "run_id": run_id,
        "timestamp": datetime.now(tz=timezone.utc),
        "git_sha": get_git_sha(),
        "params_hash": compute_params_hash(
            {
                "strategy": getattr(strategy, "__class__", type(strategy)).__name__,
                "start": start,
                "end": end,
                "base_currency": base_currency,
                "costs_yaml_path": costs_yaml_path,
                "seed": seed,
            }
        ),
        "params": {
            "strategy": getattr(strategy, "__class__", type(strategy)).__name__,
            "start": start,
            "end": end,
            "base_currency": base_currency,
            "costs_yaml_path": costs_yaml_path,
            "seed": seed,
        },
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