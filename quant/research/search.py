from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from itertools import product
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import csv
import logging
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


def _mean(xs: List[float]) -> Optional[float]:
    return sum(xs) / len(xs) if xs else None


def _collect(fold_results: List[Dict[str, Any]], key: str) -> List[float]:
    out: List[float] = []
    for fr in fold_results:
        v = fr.get("metrics", {}).get(key)
        # Explicitly reject None and bool; a None metric means "undefined",
        # and averaging it as 0.0 silently fabricates a score.
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            out.append(float(v))
    return out


def _summarize_fold_results(fold_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    rets = _collect(fold_results, "total_return")
    sharpes = _collect(fold_results, "sharpe")
    dds = _collect(fold_results, "max_drawdown")
    orders = _collect(fold_results, "num_orders")
    return {
        "mean_return": _mean(rets),
        "mean_sharpe": _mean(sharpes),
        "mean_max_drawdown": _mean(dds),
        "worst_max_drawdown": min(dds) if dds else None,
        "mean_num_orders": _mean(orders),
        "num_folds": len(fold_results),
        "num_folds_scored": len(rets),
    }


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
    (params, strategy_factory, reader, store, folds, costs_yaml_path, out_dir,
     base_seed, starting_cash) = args
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
        starting_cash=starting_cash,
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
    starting_cash: float = 100_000.0,
) -> List[TrialResult]:
    """Score a parameter grid/sample across folds and write a leaderboard.

    .. warning::
       The ranking is **in-sample selection**. :func:`run_walk_forward` does
       not fit anything on the folds' training ranges, so picking the top row
       of ``leaderboard.csv`` is choosing the parameters that happened to do
       best on the very data used to score them. Treat the leaderboard as a
       sensitivity study, not as evidence of out-of-sample edge.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    tasks: List[Tuple[Dict[str, Any], Any, PITDataReader, BarsStore, Sequence[FoldSpec], Optional[str], Path, Optional[int], float]] = []

    if mode == "grid":
        if not param_grid:
            raise ValueError("param_grid must be provided for grid mode")
        for params in _iter_grid(param_grid):
            tasks.append((params, strategy_factory, reader, store, folds, costs_yaml_path, out_dir, base_seed, starting_cash))
    elif mode == "random":
        if not random_spec or not n_trials:
            raise ValueError("random_spec and n_trials must be provided for random mode")
        for params in _sample_random(random_spec, n_trials, base_seed):
            tasks.append((params, strategy_factory, reader, store, folds, costs_yaml_path, out_dir, base_seed, starting_cash))
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

    # Rank unscored trials last rather than treating them as a 0.0 return,
    # which would place a broken trial above every genuine loss-maker.
    def _rank_key(r: TrialResult) -> Tuple[int, float]:
        mr = r.summary.get("mean_return")
        return (0, mr) if isinstance(mr, (int, float)) else (1, 0.0)

    ranked = sorted(results, key=_rank_key, reverse=True)

    distinct_scores = {
        r.summary.get("mean_return")
        for r in results
        if isinstance(r.summary.get("mean_return"), (int, float))
    }
    if len(results) > 1 and len(distinct_scores) <= 1:
        logging.getLogger("quant.research").warning(
            "All %d trials scored identically (%s). The leaderboard ordering is "
            "arbitrary and carries no information - check that starting_cash > 0 "
            "and that the strategy actually trades within each fold's window.",
            len(results),
            distinct_scores or "unscored",
        )

    metric_cols = [
        "mean_return", "mean_sharpe", "mean_max_drawdown",
        "worst_max_drawdown", "mean_num_orders", "num_folds", "num_folds_scored",
    ]
    leaderboard_path = out_dir / "leaderboard.csv"
    with leaderboard_path.open("w", newline="") as f:
        writer = csv.writer(f)
        param_names: List[str] = sorted({k for r in results for k in r.params.keys()})
        writer.writerow(["trial_id", *param_names, *metric_cols])
        for r in ranked:
            writer.writerow(
                [r.trial_id]
                + [r.params.get(k) for k in param_names]
                + [r.summary.get(c) for c in metric_cols]
            )

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