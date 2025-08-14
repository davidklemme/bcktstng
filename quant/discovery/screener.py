from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from ..data.pit_reader import BarsStore
from ..data.symbols_repository import SymbolRow
from .sector_stats import RatioPoint, compute_sector_stats


EXCHANGE_TO_REGION: Dict[str, str] = {
	"XNAS": "US",
	"XNYS": "US",
	"XLON": "EU",
	"XPAR": "EU",
	"XETR": "EU",
	"XAMS": "EU",
	"XBRU": "EU",
	"SIX": "EU",
}


@dataclass(frozen=True)
class UniverseFilters:
	exchanges: Optional[Sequence[str]] = None
	regions: Optional[Sequence[str]] = None
	min_market_cap: Optional[float] = None
	min_addv: Optional[float] = None  # average daily dollar volume
	addv_window_days: int = 20


@dataclass(frozen=True)
class Candidate:
	symbol_id: int
	ticker: str
	exchange: str
	score: float
	details: Dict[str, float]


def _utc(ts: datetime) -> datetime:
	if ts.tzinfo is None:
		return ts.replace(tzinfo=timezone.utc)
	return ts.astimezone(timezone.utc)


def compute_addv(store: BarsStore, symbol_id: int, asof: datetime, window_days: int) -> float:
	asof_u = _utc(asof)
	# Use last N bars up to asof
	bars = store.get_between(symbol_id, None, asof_u)
	if not bars:
		return 0.0
	# Select last window_days entries
	recent = bars[-window_days:]
	if not recent:
		return 0.0
	# Dollar volume = close * volume
	dvs = [float(b.close) * float(b.volume) for b in recent]
	return sum(dvs) / len(dvs)


def filter_universe(symbols: Iterable[SymbolRow], filters: UniverseFilters) -> List[SymbolRow]:
	out: List[SymbolRow] = []
	for s in symbols:
		if filters.exchanges and s.exchange not in filters.exchanges:
			continue
		if filters.regions:
			region = EXCHANGE_TO_REGION.get(s.exchange, "US")
			if region not in filters.regions:
				continue
		# Market cap unavailable in current dataset; skip unless None
		out.append(s)
	return out


def rank_candidates(
	*,
	store: BarsStore,
	symbols: List[SymbolRow],
	ratio_points: Dict[int, RatioPoint],
	asof: datetime,
	filters: UniverseFilters,
	top_k: int,
	metric_weights: Optional[Dict[str, float]] = None,
) -> List[Candidate]:
	asof_u = _utc(asof)
	metric_weights = metric_weights or {"fcf_yield": 1.0, "roic": 0.5}
	# Build sector stats
	points = [ratio_points[s.symbol_id] for s in symbols if s.symbol_id in ratio_points]
	sector_stats = compute_sector_stats(points, list(metric_weights.keys()))
	# Compute ADDV if required
	eligible: List[Tuple[SymbolRow, Dict[str, float]]] = []
	for s in symbols:
		if s.symbol_id not in ratio_points:
			continue
		addv = compute_addv(store, s.symbol_id, asof_u, filters.addv_window_days)
		if filters.min_addv is not None and addv < float(filters.min_addv):
			continue
		# Percentiles within sector
		sector = ratio_points[s.symbol_id].sector
		pcts = sector_stats.get(sector).percentiles.get(s.symbol_id, {}) if sector in sector_stats else {}
		# Score as weighted sum of percentiles
		score = 0.0
		for m, w in metric_weights.items():
			pct = float(pcts.get(m, 0.0))
			score += w * pct
		eligible.append((s, {"score": score, "addv": addv, **{f"pct_{m}": float(pcts.get(m, 0.0)) for m in metric_weights}}))
	# Rank and take top_k
	ranked = sorted(eligible, key=lambda x: x[1]["score"], reverse=True)[: top_k]
	return [Candidate(symbol_id=s.symbol_id, ticker=s.ticker, exchange=s.exchange, score=det["score"], details=det) for s, det in ranked]


def write_candidates_csv(out_path: Path | str, candidates: List[Candidate]) -> str:
	path = Path(out_path)
	path.parent.mkdir(parents=True, exist_ok=True)
	with path.open("w", newline="") as f:
		writer = csv.writer(f)
		writer.writerow(["rank", "symbol_id", "ticker", "exchange", "score"])
		for i, c in enumerate(candidates, start=1):
			writer.writerow([i, c.symbol_id, c.ticker, c.exchange, f"{c.score:.6f}"])
	return str(path)