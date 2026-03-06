from __future__ import annotations

import json
import logging
import math
import os
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..common.utils import to_utc as _to_utc
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


def _compute_metrics(equity_points: List[Dict[str, Any]], fills_out: List[Dict[str, Any]], orders_out: List[Dict[str, Any]]) -> Dict[str, Any]:
    final_equity = equity_points[-1]["equity_eur"] if equity_points else 0.0
    start_equity = equity_points[0]["equity_eur"] if equity_points else 0.0
    ret = (final_equity / start_equity - 1.0) if start_equity not in (0.0, 0) else 0.0

    # Compute daily returns for Sharpe
    equities = [p["equity_eur"] for p in equity_points]
    daily_returns: List[float] = []
    for i in range(1, len(equities)):
        if equities[i - 1] != 0:
            daily_returns.append(equities[i] / equities[i - 1] - 1.0)

    # Sharpe ratio (rf=0, annualized with sqrt(252))
    sharpe = 0.0
    if len(daily_returns) > 1:
        avg_ret = sum(daily_returns) / len(daily_returns)
        var = sum((r - avg_ret) ** 2 for r in daily_returns) / (len(daily_returns) - 1)
        std = math.sqrt(var) if var > 0 else 0.0
        if std > 0:
            sharpe = (avg_ret / std) * math.sqrt(252)

    # Max drawdown
    max_dd = 0.0
    max_dd_duration = 0
    if equities:
        peak = equities[0]
        dd_start = 0
        for i, eq in enumerate(equities):
            if eq > peak:
                peak = eq
                dd_start = i
            if peak > 0:
                dd = (eq / peak) - 1.0
                if dd < max_dd:
                    max_dd = dd
                    max_dd_duration = i - dd_start

    # Win rate and profit factor from fills
    # Track per-order realized P&L using fill prices vs avg entry
    trade_pnls: List[float] = []
    # Simple approach: group fills by order, compute notional P&L per order
    for order_rec in orders_out:
        order_fills = [f for f in fills_out if f["order_id"] == order_rec["order_id"]]
        if not order_fills:
            continue
        total_notional = sum(f["price"] * f["quantity"] for f in order_fills)
        # P&L is approximate; positive for sells, negative for buys (raw notional)
        sign = -1.0 if order_rec["side"] == "BUY" else 1.0
        trade_pnls.append(sign * total_notional)

    wins = [p for p in trade_pnls if p > 0]
    losses = [p for p in trade_pnls if p < 0]
    win_rate = len(wins) / len(trade_pnls) if trade_pnls else 0.0
    gross_profit = sum(wins) if wins else 0.0
    gross_loss = abs(sum(losses)) if losses else 0.0
    profit_factor = min(gross_profit / gross_loss, 999.0) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
    avg_trade_pnl = sum(trade_pnls) / len(trade_pnls) if trade_pnls else 0.0

    return {
        "final_equity_eur": final_equity,
        "start_equity_eur": start_equity,
        "return": ret,
        "sharpe_ratio": round(sharpe, 4),
        "max_drawdown": round(max_dd, 6),
        "max_drawdown_duration_bars": max_dd_duration,
        "win_rate": round(win_rate, 4),
        "profit_factor": round(profit_factor, 4),
        "avg_trade_pnl": round(avg_trade_pnl, 4),
        "num_orders": len(orders_out),
        "num_fills": len(fills_out),
        "num_trades": len(trade_pnls),
    }


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
    if starting_cash and starting_cash != 0.0:
        portfolio.deposit(float(starting_cash), base_currency)

    # Prepare context
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

        import time as _time
        _t0 = _time.perf_counter()

        if hasattr(strategy, "on_event"):
            strategy.on_event(None, ctx)

        # Process orders emitted this cycle
        pending: List[Order] = list(ctx.order_api._orders)
        ctx.order_api._orders.clear()

        # A7: Filter out canceled orders
        canceled_ids = set(ctx.order_api._canceled)
        ctx.order_api._canceled.clear()
        pending = [o for o in pending if o.id not in canceled_ids]

        for order in pending:
            orders_total.inc()

            # A5: Acknowledge order (transition NEW -> WORKING)
            order.acknowledge()

            # Find bar for this symbol at ts
            bar_rows = bars_at_time.get(ts, [])
            bar_for_symbol: Optional[BarRow] = next((b for b in bar_rows if b.symbol_id == order.symbol_id), None)
            if bar_for_symbol is None:
                order.handle_end_of_cycle(0)
                orders_out.append(_order_record(order, ts))
                continue

            quote = _quote_from_bar(bar_for_symbol)
            venue = venue_by_symbol.get(order.symbol_id, "UNKNOWN")

            # B4: Pre-trade risk check using quote mid as price estimate
            sym_meta = symbols_by_id.get(order.symbol_id)
            currency = sym_meta.currency if sym_meta else base_currency
            allowed, reason = risk_api.manager.check(
                portfolio, order.symbol_id, currency,
                quote.mid, order.quantity, fx_rate_to_eur=1.0,
            )
            if not allowed:
                order.reject()
                logger.info("Order %s rejected by risk: %s", order.id, reason)
                orders_out.append(_order_record(order, ts))
                continue

            # A8: Use actual bar volume, not max(volume, qty)
            available_liquidity = int(bar_for_symbol.volume)
            fills, cost_total = exec_sim.simulate(order, quote, venue, available_liquidity, ts=ts)

            # Apply fills to portfolio
            total_fill_qty = sum(fill.quantity for fill in fills)
            for fill in fills:
                fills_total.inc()
                # A5: Update order state machine
                order.add_fill(fill.quantity)
                side = order.side.value
                portfolio.apply_fill(order.symbol_id, currency, side, fill.quantity, fill.price)
                # Slippage vs mid in bps
                mid = quote.mid
                if mid > 0:
                    bps = abs((fill.price - mid) / mid) * 10000.0
                    fill_slippage_bps.observe(bps)
                # Cost per fill (proportional)
                fill_cost = (cost_total * fill.quantity / total_fill_qty) if total_fill_qty > 0 else 0.0
                fills_out.append(
                    {
                        "ts": ts,
                        "order_id": order.id,
                        "symbol_id": order.symbol_id,
                        "price": fill.price,
                        "quantity": fill.quantity,
                        "venue": venue,
                        "cost": fill_cost,
                    }
                )

            # A6: Apply transaction cost in instrument currency, not base_currency
            if cost_total and cost_total != 0.0:
                portfolio.apply_transaction_cost(currency, cost_total)

            # A5: Handle end of cycle for IOC/FOK semantics
            order.handle_end_of_cycle(available_liquidity)

            orders_out.append(_order_record(order, ts))

        # Mark portfolio and record equity
        mark_prices: Dict[int, float] = {}
        for b in bars_at_time.get(ts, []):
            mark_prices[b.symbol_id] = b.close
        equity_eur = portfolio.total_value_eur(ts, mark_prices, reader.fx_engine)
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

    # B3: Comprehensive metrics
    metrics = _compute_metrics(equity_points, fills_out, orders_out)

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
                "starting_cash": starting_cash,
            }
        ),
        "params": {
            "strategy": getattr(strategy, "__class__", type(strategy)).__name__,
            "start": start,
            "end": end,
            "base_currency": base_currency,
            "costs_yaml_path": costs_yaml_path,
            "seed": seed,
            "starting_cash": starting_cash,
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


def _order_record(order: Order, ts: datetime) -> Dict[str, Any]:
    return {
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
