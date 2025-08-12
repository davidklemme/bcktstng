from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from itertools import product
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import csv
import json
import os
import random
import hashlib

from .validation import FoldSpec, run_walk_forward
from ..data.pit_reader import PITDataReader, BarsStore


@dataclass(frozen=True)
class TrialResult:
    trial_id: str
    params: Dict[str, Any]
    fold_metrics: List[Dict[str, Any]]
    summary: Dict[str, Any]


def _param_hash(params: Dict[str, Any]) -> str:
    payload = json.dumps(params, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:8]


def _summarize_fold_results(fold_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Aggregate by simple average of return metric; extend as needed
    rets: List[float] = []
    order_counts: List[int] = []
    for fr in fold_results:
        m = fr.get("metrics", {})
        if isinstance(m.get("return"), (int, float)):
            rets.append(float(m["return"]))
        if isinstance(m.get("num_orders"), (int, float)):
            order_counts.append(int(m["num_orders"]))
    avg_ret = sum(rets) / len(rets) if rets else 0.0
    avg_orders = sum(order_counts) / len(order_counts) if order_counts else 0.0
    return {"mean_return": avg_ret, "mean_num_orders": avg_orders, "num_folds": len(fold_results)}


def _evaluate_trial(
    args: Tuple[
        Dict[str, Any],
        Any,
        PITDataReader,
        BarsStore,
        Sequence[FoldSpec],
        Optional[str],
        Path,
        Optional[int],
    ]
) -> TrialResult:
    params, strategy_factory, reader, store, folds, costs_yaml_path, out_dir, base_seed = args
    trial_id = _param_hash(params)
    trial_dir = out_dir / f"trial_{trial_id}"
    fold_results = run_walk_forward(
        strategy_factory=strategy_factory,
        strategy_params=params,
        reader=reader,
        store=store,
        folds=folds,
        costs_yaml_path=costs_yaml_path,
        out_dir=trial_dir,
        base_seed=base_seed,
    )
    summary = _summarize_fold_results(fold_results)
    return TrialResult(trial_id=trial_id, params=params, fold_metrics=fold_results, summary=summary)


def _iter_grid(param_grid: Dict[str, List[Any]]) -> Iterable[Dict[str, Any]]:
    keys = sorted(param_grid.keys())
    for combo in product(*[param_grid[k] for k in keys]):
        yield {k: v for k, v in zip(keys, combo)}


def _sample_random(random_spec: Dict[str, Dict[str, Any]], n_trials: int, seed: Optional[int]) -> Iterable[Dict[str, Any]]:
    rng = random.Random(seed)
    for _ in range(n_trials):
        params: Dict[str, Any] = {}
        for name, spec in random_spec.items():
            mode = spec.get("mode", "int")
            lo = spec.get("min")
            hi = spec.get("max")
            if lo is None or hi is None:
                raise ValueError(f"Random spec for '{name}' missing min/max")
            if mode == "float":
                params[name] = rng.uniform(float(lo), float(hi))
            else:
                params[name] = rng.randint(int(lo), int(hi))
        yield params


def run_hyperparameter_search(
    *,
    strategy_factory: Any,
    reader: PITDataReader,
    store: BarsStore,
    folds: Sequence[FoldSpec],
    mode: str,
    param_grid: Optional[Dict[str, List[Any]]] = None,
    random_spec: Optional[Dict[str, Dict[str, Any]]] = None,
    n_trials: Optional[int] = None,
    costs_yaml_path: Optional[str] = None,
    out_dir: Path,
    base_seed: Optional[int] = None,
    parallel_workers: Optional[int] = None,
) -> List[TrialResult]:
    out_dir.mkdir(parents=True, exist_ok=True)
    tasks: List[Tuple[Dict[str, Any], Any, PITDataReader, BarsStore, Sequence[FoldSpec], Optional[str], Path, Optional[int]]] = []

    if mode == "grid":
        if not param_grid:
            raise ValueError("param_grid must be provided for grid mode")
        for params in _iter_grid(param_grid):
            tasks.append((params, strategy_factory, reader, store, folds, costs_yaml_path, out_dir, base_seed))
    elif mode == "random":
        if not random_spec or not n_trials:
            raise ValueError("random_spec and n_trials must be provided for random mode")
        for params in _sample_random(random_spec, n_trials, base_seed):
            tasks.append((params, strategy_factory, reader, store, folds, costs_yaml_path, out_dir, base_seed))
    else:
        raise ValueError("mode must be 'grid' or 'random'")

    if not tasks:
        return []

    workers = max(1, min(len(tasks), parallel_workers or (cpu_count() or 1)))

    results: List[TrialResult] = []
    if workers == 1:
        for t in tasks:
            results.append(_evaluate_trial(t))
    else:
        with Pool(processes=workers) as pool:
            for tr in pool.imap_unordered(_evaluate_trial, tasks):
                results.append(tr)

    # Write leaderboard
    leaderboard_path = out_dir / "leaderboard.csv"
    with leaderboard_path.open("w", newline="") as f:
        writer = csv.writer(f)
        # header
        param_names: List[str] = sorted({k for r in results for k in r.params.keys()})
        writer.writerow(["trial_id", *param_names, "mean_return", "mean_num_orders", "num_folds"])
        for r in sorted(results, key=lambda x: x.summary.get("mean_return", 0.0), reverse=True):
            row = [r.trial_id] + [r.params.get(k) for k in param_names] + [
                r.summary.get("mean_return", 0.0),
                r.summary.get("mean_num_orders", 0.0),
                r.summary.get("num_folds", 0),
            ]
            writer.writerow(row)

    # Also write JSON
    json_path = out_dir / "leaderboard.json"
    with json_path.open("w") as f:
        json.dump(
            [
                {
                    "trial_id": r.trial_id,
                    "params": r.params,
                    "summary": r.summary,
                }
                for r in results
            ],
            f,
            indent=2,
        )

    return results