from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class RatioPoint:
	symbol_id: int
	sector: str
	metrics: Dict[str, Optional[float]]


@dataclass(frozen=True)
class SectorStats:
	sector: str
	medians: Dict[str, float]
	percentiles: Dict[int, Dict[str, float]]  # symbol_id -> metric -> pct in [0,1]


def _median(values: List[float]) -> float:
	sorted_vals = sorted(values)
	n = len(sorted_vals)
	if n == 0:
		return float("nan")
	mid = n // 2
	if n % 2 == 1:
		return float(sorted_vals[mid])
	return float((sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0)


def compute_sector_stats(points: List[RatioPoint], metrics: List[str]) -> Dict[str, SectorStats]:
	"""Compute sector medians and per-symbol percentiles within each sector for given metrics.
	Percentile defined as rank/ (N-1) for ascending order for metrics where higher is better by default
	Assumes higher is better for all provided metrics; reverse before calling if needed.
	"""
	# Group by sector
	by_sector: Dict[str, List[RatioPoint]] = {}
	for p in points:
		by_sector.setdefault(p.sector, []).append(p)

	stats: Dict[str, SectorStats] = {}
	for sector, lst in by_sector.items():
		meds: Dict[str, float] = {}
		pcts: Dict[int, Dict[str, float]] = {p.symbol_id: {} for p in lst}
		for m in metrics:
			vals: List[Tuple[int, float]] = []
			for p in lst:
				v = p.metrics.get(m)
				if v is not None:
					vals.append((p.symbol_id, float(v)))
			if not vals:
				continue
			meds[m] = _median([v for _, v in vals])
			# Percentiles: rank order ascending
			vals_sorted = sorted(vals, key=lambda x: x[1])
			n = len(vals_sorted)
			if n == 1:
				pcts[vals_sorted[0][0]][m] = 1.0
			else:
				for rank, (sid, _) in enumerate(vals_sorted):
					pcts[sid][m] = rank / (n - 1)
		stats[sector] = SectorStats(sector=sector, medians=meds, percentiles=pcts)
	return stats