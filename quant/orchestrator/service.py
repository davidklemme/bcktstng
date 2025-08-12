from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Response
from pydantic import BaseModel, Field

from ..data.bars_loader import load_daily_bars_csv
from ..data.pit_reader import PITDataReader, BarsStore
from ..data.symbols_repository import create_sqlite_engine
from .backtest import run_backtest
from .config import get_settings
from ..examples.ma_cross import MACross

app = FastAPI(title="Quant Orchestrator", version="0.1")

# Metrics endpoint (Prometheus)
try:
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # type: ignore

    @app.get("/metrics")
    def metrics() -> Response:  # type: ignore[override]
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
except Exception:  # pragma: no cover
    # prometheus_client may be optional in some environments
    pass


class StrategySpec(BaseModel):
    name: str = Field(..., examples=["ma_cross"])
    params: Dict[str, Any] = Field(default_factory=dict)


class DataSpec(BaseModel):
    bars_csv: str
    exchange: str = "XNYS"
    symbols_db: Optional[str] = None
    fx_db: Optional[str] = None


class BacktestRequest(BaseModel):
    strategy: StrategySpec
    start: datetime
    end: datetime
    data: DataSpec
    costs_yaml: Optional[str] = None


class BacktestResponse(BaseModel):
    run_id: str
    metrics: Dict[str, Any]
    equity: List[Dict[str, Any]]
    orders: List[Dict[str, Any]]
    fills: List[Dict[str, Any]]


class SignalRequest(BaseModel):
    strategy: StrategySpec
    asof: datetime
    data: DataSpec
    costs_yaml: Optional[str] = None


class SignalResponse(BaseModel):
    run_id: str
    orders: List[Dict[str, Any]]


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/backtest", response_model=BacktestResponse)
def backtest(req: BacktestRequest) -> BacktestResponse:
    settings = get_settings()

    rows, _ = load_daily_bars_csv(req.data.bars_csv, req.data.exchange)
    store = BarsStore.from_rows(rows)

    symbols_engine = create_sqlite_engine(req.data.symbols_db or settings.symbols_db_path)
    fx_engine = create_sqlite_engine(req.data.fx_db or settings.fx_db_path)

    reader = PITDataReader(fx_engine, symbols_engine, store)

    # Strategy factory
    if req.strategy.name.lower() == "ma_cross":
        symbol = req.strategy.params.get("symbol", "AAPL")
        fast = int(req.strategy.params.get("fast", 10))
        slow = int(req.strategy.params.get("slow", 30))
        strat = MACross(symbol=symbol, fast=fast, slow=slow)
    else:
        raise ValueError(f"Unknown strategy: {req.strategy.name}")

    result = run_backtest(
        strategy=strat,
        reader=reader,
        bars_store=store,
        start=req.start,
        end=req.end,
        costs_yaml_path=req.costs_yaml or settings.cost_profiles_path,
        out_dir=Path(settings.runs_dir) / "service",
    )

    return BacktestResponse(
        run_id=result.run_id,
        metrics=result.metrics,
        equity=result.equity,
        orders=result.orders,
        fills=result.fills,
    )


@app.post("/signal", response_model=SignalResponse)
def signal(req: SignalRequest) -> SignalResponse:
    bt_resp = backtest(
        BacktestRequest(
            strategy=req.strategy,
            start=req.asof,
            end=req.asof,
            data=req.data,
            costs_yaml=req.costs_yaml,
        )
    )
    # Return orders only for the single step
    return SignalResponse(run_id=bt_resp.run_id, orders=bt_resp.orders)