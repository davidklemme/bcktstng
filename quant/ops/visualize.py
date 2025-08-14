from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

# Use a non-interactive backend suitable for headless environments
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np


def _compute_max_drawdown(equity: pd.Series) -> Tuple[float, pd.Timestamp, pd.Timestamp, pd.Series]:
    """Compute max drawdown on an equity curve.

    Returns a tuple of (max_drawdown_fraction, peak_time, trough_time, drawdown_series)
    where drawdown is in fractional terms (e.g., -0.25 for -25%).
    """
    if equity.empty:
        return 0.0, pd.NaT, pd.NaT, pd.Series(dtype=float)

    # Ensure float
    equity = equity.astype(float)
    # Running peak and drawdown
    running_max = equity.cummax()
    dd = equity / running_max - 1.0
    # Max drawdown is the minimum value of dd (most negative)
    trough_idx = dd.idxmin()
    max_dd = float(dd.loc[trough_idx]) if trough_idx in dd.index else 0.0
    # Peak is the last time equity hit the running max before the trough
    peak_idx = equity.loc[:trough_idx].idxmax() if trough_idx in equity.index else equity.idxmax()
    return max_dd, pd.to_datetime(peak_idx), pd.to_datetime(trough_idx), dd


def visualize_run(run_dir: str | Path, out_path: Optional[str | Path] = None) -> Path:
    """Generate a PNG visualization for a run directory.

    - Plots equity over time
    - Overlays per-timestep order counts as a translucent bar chart (right axis)
    - Highlights the max drawdown window and annotates its magnitude

    Returns the output path to the PNG.
    """
    run_dir_path = Path(run_dir)
    equity_csv = run_dir_path / "equity.csv"
    orders_csv = run_dir_path / "orders.csv"

    if not equity_csv.exists():
        raise FileNotFoundError(f"Missing equity.csv in run directory: {equity_csv}")

    # Load equity
    df = pd.read_csv(equity_csv)
    if "ts" not in df.columns or "equity_eur" not in df.columns:
        raise ValueError("equity.csv must have columns: ts,equity_eur")
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.sort_values("ts").reset_index(drop=True)

    # Load orders (optional)
    orders_df = None
    if orders_csv.exists():
        orders_df = pd.read_csv(orders_csv)
        if "ts" in orders_df.columns:
            orders_df["ts"] = pd.to_datetime(orders_df["ts"], utc=True, errors="coerce")
            orders_df = orders_df.dropna(subset=["ts"]).sort_values("ts").reset_index(drop=True)
        else:
            orders_df = None

    # Compute drawdown
    max_dd, dd_peak_ts, dd_trough_ts, dd_series = _compute_max_drawdown(df.set_index("ts")["equity_eur"])

    # Prepare figure
    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(12, 6), dpi=140)

    # Equity line
    ax.plot(df["ts"], df["equity_eur"], color="C0", linewidth=1.8, label="Equity (EUR)")
    ax.set_xlabel("Time")
    ax.set_ylabel("Equity (EUR)")

    # Orders per timestep (bars on secondary axis)
    if orders_df is not None and not orders_df.empty:
        per_ts = orders_df.groupby("ts").size()
        # Align to equity timestamps
        per_ts = per_ts.reindex(df["ts"].values, fill_value=0)

        ax2 = ax.twinx()
        ts_num = mdates.date2num(df["ts"].dt.to_pydatetime())
        if len(ts_num) >= 2:
            bar_width = np.min(np.diff(ts_num)) * 0.6
        else:
            bar_width = 0.8
        ax2.bar(ts_num, per_ts.values, width=bar_width, color="C1", alpha=0.3, label="Orders per step")
        ax2.set_ylabel("Orders per step")

    # Highlight max drawdown window
    if pd.notna(dd_peak_ts) and pd.notna(dd_trough_ts) and dd_peak_ts != dd_trough_ts:
        ax.axvspan(dd_peak_ts, dd_trough_ts, color="red", alpha=0.10, label="Max drawdown window")
        # Annotate drawdown magnitude near trough
        trough_equity = float(df.set_index("ts").loc[dd_trough_ts, "equity_eur"]) if dd_trough_ts in df.set_index("ts").index else float(df["equity_eur"].iloc[-1])
        ax.annotate(
            f"Max DD: {max_dd:.2%}",
            xy=(dd_trough_ts, trough_equity), xytext=(10, -20), textcoords="offset points",
            arrowprops=dict(arrowstyle="->", color="red", lw=1.0), color="red", fontsize=10, weight="bold"
        )

    # Title with summary numbers
    total_orders = int(orders_df.shape[0]) if orders_df is not None else 0
    title = f"Run Visualization — Max DD {max_dd:.2%} | Total orders {total_orders}"
    ax.set_title(title)

    # Legends handling
    lines, labels = ax.get_legend_handles_labels()
    if "ax2" in locals():
        lines2, labels2 = ax2.get_legend_handles_labels()
        lines += lines2
        labels += labels2
    ax.legend(lines, labels, loc="upper left")

    fig.autofmt_xdate()
    fig.tight_layout()

    # Output path
    out = Path(out_path) if out_path is not None else (run_dir_path / "visualization.png")
    fig.savefig(out)
    plt.close(fig)
    return out