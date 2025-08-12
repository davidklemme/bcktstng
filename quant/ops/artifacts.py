from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def compute_params_hash(params: Dict[str, Any]) -> str:
    payload = json.dumps(params, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def get_git_sha(default: str = "unknown") -> str:
    try:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2], timeout=2)
        return sha.decode("utf-8").strip()
    except Exception:
        return default


@dataclass
class ArtifactPaths:
    base_dir: Path
    equity_csv: Path
    orders_csv: Path
    fills_csv: Path
    positions_csv: Path
    metrics_json: Path
    manifest_json: Path
    logs_jsonl: Path


class ArtifactWriter:
    def __init__(self, out_dir: str | Path) -> None:
        base = _ensure_dir(out_dir)
        self.paths = ArtifactPaths(
            base_dir=base,
            equity_csv=base / "equity.csv",
            orders_csv=base / "orders.csv",
            fills_csv=base / "fills.csv",
            positions_csv=base / "positions.csv",
            metrics_json=base / "metrics.json",
            manifest_json=base / "run_manifest.json",
            logs_jsonl=base / "logs.jsonl",
        )

    def write_equity(self, rows: Iterable[Dict[str, Any]]) -> None:
        with self.paths.equity_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["ts", "equity_eur"])
            writer.writeheader()
            for r in rows:
                writer.writerow({"ts": _iso(r["ts"]), "equity_eur": r["equity_eur"]})

    def write_orders(self, rows: Iterable[Dict[str, Any]]) -> None:
        fields = [
            "ts",
            "order_id",
            "symbol_id",
            "side",
            "quantity",
            "type",
            "tif",
            "limit_price",
            "state",
        ]
        with self.paths.orders_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for r in rows:
                r2 = dict(r)
                r2["ts"] = _iso(r2["ts"]) if r2.get("ts") else None
                writer.writerow(r2)

    def write_fills(self, rows: Iterable[Dict[str, Any]]) -> None:
        fields = ["ts", "order_id", "symbol_id", "price", "quantity", "venue", "cost"]
        with self.paths.fills_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for r in rows:
                r2 = dict(r)
                r2["ts"] = _iso(r2["ts"]) if r2.get("ts") else None
                writer.writerow(r2)

    def write_positions(self, rows: Iterable[Dict[str, Any]]) -> None:
        fields = ["ts", "symbol_id", "currency", "quantity", "average_price"]
        with self.paths.positions_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for r in rows:
                r2 = dict(r)
                r2["ts"] = _iso(r2["ts"]) if r2.get("ts") else None
                writer.writerow(r2)

    def write_metrics(self, metrics: Dict[str, Any]) -> None:
        with self.paths.metrics_json.open("w") as f:
            json.dump(metrics, f, indent=2, default=_json_default)

    def write_manifest(self, manifest: Dict[str, Any]) -> None:
        with self.paths.manifest_json.open("w") as f:
            json.dump(manifest, f, indent=2, default=_json_default)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return _iso(value)
    return str(value)


def _iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    return ts.isoformat().replace("+00:00", "Z")