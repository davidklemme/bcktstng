from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
import json
import hashlib

from ..data.pit_reader import PITDataReader, BarsStore
from ..orchestrator.backtest import run_backtest


@dataclass(frozen=True)
class FoldSpec:
    train_ranges: Tuple[Tuple[datetime, datetime], ...]
    val_range: Tuple[datetime, datetime]


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


def make_purged_kfold_folds(
    *,
    store: BarsStore,
    start: datetime,
    end: datetime,
    n_splits: int,
    embargo_fraction: float = 0.01,
) -> List[FoldSpec]:
    if n_splits <= 1:
        raise ValueError("n_splits must be >= 2")
    times = _unique_times_between(store, start, end)
    if len(times) < n_splits:
        raise ValueError("Not enough timesteps for the requested number of splits")

    fold_size = max(1, len(times) // n_splits)
    embargo = max(0, int(len(times) * max(0.0, min(embargo_fraction, 0.5))))

    folds: List[FoldSpec] = []
    for i in range(n_splits):
        val_start_idx = i * fold_size
        # last fold takes the remainder
        if i == n_splits - 1:
            val_end_idx = len(times) - 1
        else:
            val_end_idx = min(len(times) - 1, (i + 1) * fold_size - 1)
        val_start = times[val_start_idx]
        val_end = times[val_end_idx]

        # Purge/embargo around validation range
        left_end_idx = max(0, val_start_idx - 1 - embargo)
        right_start_idx = min(len(times) - 1, val_end_idx + 1 + embargo)

        train_ranges: List[Tuple[datetime, datetime]] = []
        if left_end_idx >= 0 and left_end_idx >= 0 and left_end_idx >= 0 and left_end_idx >= 0:
            if left_end_idx >= 0 and left_end_idx >= 0:
                if left_end_idx >= 0 and val_start_idx - embargo - 1 >= 0:
                    if val_start_idx - embargo - 1 >= 0:
                        if (val_start_idx - embargo - 1) >= 0 and (val_start_idx - embargo - 1) >= 0:
                            pass
        # Left training block
        left_train_end_idx = max(-1, val_start_idx - 1 - embargo)
        if left_train_end_idx >= 0:
            train_ranges.append((times[0], times[left_train_end_idx]))
        # Right training block
        right_train_start_idx = min(len(times), val_end_idx + 1 + embargo)
        if right_train_start_idx <= len(times) - 1:
            train_ranges.append((times[right_train_start_idx], times[-1]))

        folds.append(
            FoldSpec(
                train_ranges=tuple(train_ranges),
                val_range=(val_start, val_end),
            )
        )
    return folds


def make_walk_forward_folds(
    *,
    store: BarsStore,
    start: datetime,
    end: datetime,
    train_window: int,
    test_window: int,
    step: Optional[int] = None,
    embargo_fraction: float = 0.0,
) -> List[FoldSpec]:
    if train_window <= 0 or test_window <= 0:
        raise ValueError("train_window and test_window must be > 0 (in number of timesteps)")
    times = _unique_times_between(store, start, end)
    step = step if step is not None and step > 0 else test_window
    embargo = max(0, int(len(times) * max(0.0, min(embargo_fraction, 0.5))))

    folds: List[FoldSpec] = []
    i = 0
    while True:
        train_start_idx = i
        train_end_idx = i + train_window - 1
        val_start_idx = train_end_idx + 1 + embargo
        val_end_idx = val_start_idx + test_window - 1
        if val_end_idx >= len(times):
            break
        train_ranges: List[Tuple[datetime, datetime]] = [(times[train_start_idx], times[train_end_idx])]
        folds.append(
            FoldSpec(
                train_ranges=tuple(train_ranges),
                val_range=(times[val_start_idx], times[val_end_idx]),
            )
        )
        i += step
    return folds


def _hash_params(params: Dict[str, Any]) -> str:
    payload = json.dumps(params, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:8]


def run_walk_forward(
    *,
    strategy_factory: Callable[[Dict[str, Any]], Any],
    strategy_params: Dict[str, Any],
    reader: PITDataReader,
    store: BarsStore,
    folds: Sequence[FoldSpec],
    costs_yaml_path: Optional[str],
    out_dir: Path,
    base_seed: Optional[int] = None,
) -> List[Dict[str, Any]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    results: List[Dict[str, Any]] = []
    params_id = _hash_params(strategy_params)

    for idx, fold in enumerate(folds, start=1):
        fold_dir = out_dir / f"fold_{idx:02d}_{params_id}"
        fold_dir.mkdir(parents=True, exist_ok=True)
        # Build strategy instance
        strategy = strategy_factory(dict(strategy_params))
        # Run only on validation range (no training artifacts in this baseline)
        result = run_backtest(
            strategy=strategy,
            reader=reader,
            bars_store=store,
            start=fold.val_range[0],
            end=fold.val_range[1],
            costs_yaml_path=costs_yaml_path,
            out_dir=fold_dir,
            seed=(None if base_seed is None else (base_seed + idx)),
        )
        # Summarize fold
        results.append(
            {
                "fold_index": idx,
                "val_start": fold.val_range[0],
                "val_end": fold.val_range[1],
                "train_ranges": [(a, b) for (a, b) in fold.train_ranges],
                "metrics": result.metrics,
                "run_id": result.run_id,
                "out_dir": str(result.out_dir),
            }
        )

    # Write summary
    summary_path = out_dir / "folds_summary.json"
    with summary_path.open("w") as f:
        json.dump(results, f, indent=2, default=lambda x: x.isoformat() if isinstance(x, datetime) else str(x))

    return results