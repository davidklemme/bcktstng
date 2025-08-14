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
    
    # Handle case where all values are zero or negative
    if equity.max() <= 0:
        # If all values are zero or negative, calculate drawdown from the highest point
        running_max = equity.cummax()
        dd = equity - running_max  # Absolute drawdown instead of percentage
        trough_idx = dd.idxmin()
        max_dd = float(dd.loc[trough_idx]) if trough_idx in dd.index else 0.0
        peak_idx = equity.loc[:trough_idx].idxmax() if trough_idx in equity.index else equity.idxmax()
        return max_dd, pd.to_datetime(peak_idx), pd.to_datetime(trough_idx), dd
    
    # Normal case: running peak and drawdown
    running_max = equity.cummax()
    dd = equity / running_max - 1.0
    # Max drawdown is the minimum value of dd (most negative)
    trough_idx = dd.idxmin()
    max_dd = float(dd.loc[trough_idx]) if trough_idx in dd.index else 0.0
    # Peak is the last time equity hit the running max before the trough
    peak_idx = equity.loc[:trough_idx].idxmax() if trough_idx in equity.index else equity.idxmax()
    return max_dd, pd.to_datetime(peak_idx), pd.to_datetime(trough_idx), dd


def visualize_run_ascii(run_dir: str | Path, width: int = None, height: int = None) -> str:
    """Generate ASCII art visualization for a run directory with constant scales.
    
    Args:
        run_dir: Directory containing equity.csv and orders.csv
        width: Width of the ASCII chart in characters (auto-detected if None)
        height: Height of the ASCII chart in characters (auto-detected if None)
    
    Returns:
        ASCII art string representation of the equity curve
    """
    # Auto-detect terminal size if not specified
    if width is None or height is None:
        try:
            import shutil
            term_size = shutil.get_terminal_size()
            if width is None:
                width = min(term_size.columns - 10, 150)  # Leave some margin
            if height is None:
                # Use a reasonable default if terminal reports invalid height
                height = min(max(term_size.lines - 10, 20), 40)  # Between 20-40 lines
        except:
            # Fallback to reasonable defaults
            if width is None:
                width = 120
            if height is None:
                height = 30
    
    # Ensure minimum reasonable sizes
    width = max(width, 40)
    height = max(height, 10)
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

    # Get equity values and create constant scale
    equity_values = df["equity_eur"].values
    min_equity = equity_values.min()
    max_equity = equity_values.max()
    equity_range = max_equity - min_equity
    
    # Avoid division by zero
    if equity_range == 0:
        equity_range = 1.0
    
    # Create ASCII chart
    chart = []
    
    # Header with summary
    total_orders = int(orders_df.shape[0]) if orders_df is not None else 0
    
    # Handle different drawdown display formats in header
    if max_equity <= 0:
        header = f"Run Visualization — Max DD {max_dd:.2f} EUR | Total orders {total_orders}"
    else:
        header = f"Run Visualization — Max DD {max_dd:.2%} | Total orders {total_orders}"
    
    chart.append(header)
    chart.append("=" * len(header))
    chart.append(f"Equity range: {min_equity:.2f} - {max_equity:.2f} EUR")
    chart.append("")
    
    # Create the chart grid
    for row in range(height):
        # Calculate the equity value for this row (inverted for ASCII display)
        row_equity = max_equity - (row * equity_range / (height - 1))
        
        # Create the row
        row_chars = []
        
        # Y-axis label (every 3rd row or first/last for more detail)
        if row == 0 or row == height - 1 or row % 3 == 0:
            label = f"{row_equity:7.0f}"
        else:
            label = " " * 7
        row_chars.append(label + " |")
        
        # Plot points
        for col in range(width):
            # Calculate the time index for this column
            time_idx = int(col * (len(equity_values) - 1) / (width - 1))
            if time_idx >= len(equity_values):
                time_idx = len(equity_values) - 1
            
            current_equity = equity_values[time_idx]
            
            # Determine if this point should be plotted
            if abs(current_equity - row_equity) <= equity_range / (height - 1) / 2:
                # Check if this is in the drawdown period
                if (pd.notna(dd_peak_ts) and pd.notna(dd_trough_ts) and 
                    dd_peak_ts <= df.iloc[time_idx]["ts"] <= dd_trough_ts):
                    row_chars.append("D")  # Drawdown period
                else:
                    row_chars.append("*")  # Normal equity point
            else:
                row_chars.append(" ")
        
        chart.append("".join(row_chars))
    
    # X-axis
    chart.append("-" * 7 + "+" + "-" * width)
    
    # X-axis labels (time) - more frequent labels for larger charts
    time_labels = []
    label_interval = max(1, width // 8)  # More labels for wider charts
    for col in range(0, width, label_interval):
        time_idx = int(col * (len(equity_values) - 1) / (width - 1))
        if time_idx >= len(equity_values):
            time_idx = len(equity_values) - 1
        time_label = df.iloc[time_idx]["ts"].strftime("%m-%d")
        time_labels.append(f"{time_label:>6}")
    
    chart.append(" " * 8 + "".join(time_labels))
    
    # Footer with key
    chart.append("")
    chart.append("Legend:")
    chart.append("* = Equity point")
    chart.append("D = Drawdown period")
    
    # Handle different drawdown display formats
    if max_equity <= 0:
        chart.append(f"Max Drawdown: {max_dd:.2f} EUR (absolute)")
    else:
        chart.append(f"Max Drawdown: {max_dd:.2%}")
    
    chart.append(f"Peak: {dd_peak_ts.strftime('%Y-%m-%d') if pd.notna(dd_peak_ts) else 'N/A'}")
    chart.append(f"Trough: {dd_trough_ts.strftime('%Y-%m-%d') if pd.notna(dd_trough_ts) else 'N/A'}")
    
    return "\n".join(chart)


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


def visualize_runs_comparison(run_dirs: list[str | Path], width: int = None, height: int = None) -> str:
    """Generate ASCII art comparison visualization for multiple run directories.
    
    Args:
        run_dirs: List of directories containing equity.csv and orders.csv
        width: Width of the ASCII chart in characters (auto-detected if None)
        height: Height of the ASCII chart in characters (auto-detected if None)
    
    Returns:
        ASCII art string representation comparing multiple equity curves
    """
    # Auto-detect terminal size if not specified
    if width is None or height is None:
        try:
            import shutil
            term_size = shutil.get_terminal_size()
            if width is None:
                width = min(term_size.columns - 10, 150)  # Leave some margin
            if height is None:
                # Use a reasonable default if terminal reports invalid height
                height = min(max(term_size.lines - 20, 20), 40)  # More space for KPI table
        except:
            # Fallback to reasonable defaults
            if width is None:
                width = 120
            if height is None:
                height = 30
    
    # Ensure minimum reasonable sizes
    width = max(width, 40)
    height = max(height, 10)
    
    # Colors for different runs (more distinct ASCII characters)
    colors = ['█', '▓', '▒', '░', '■', '□', '▪', '▫', '▬', '▭']
    
    # Load all run data
    run_data = []
    all_equity_values = []
    
    for i, run_dir in enumerate(run_dirs):
        run_dir_path = Path(run_dir)
        equity_csv = run_dir_path / "equity.csv"
        orders_csv = run_dir_path / "orders.csv"
        
        if not equity_csv.exists():
            continue
            
        # Load equity
        df = pd.read_csv(equity_csv)
        if "ts" not in df.columns or "equity_eur" not in df.columns:
            continue
            
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df = df.sort_values("ts").reset_index(drop=True)
        
        # Load orders (optional)
        orders_df = None
        if orders_csv.exists():
            orders_df = pd.read_csv(orders_csv)
            if "ts" in orders_df.columns:
                orders_df["ts"] = pd.to_datetime(orders_df["ts"], utc=True, errors="coerce")
                orders_df = orders_df.dropna(subset=["ts"]).sort_values("ts").reset_index(drop=True)
        
        # Compute drawdown
        max_dd, dd_peak_ts, dd_trough_ts, dd_series = _compute_max_drawdown(df.set_index("ts")["equity_eur"])
        
        # Calculate KPIs
        equity_values = df["equity_eur"].values
        if len(equity_values) > 0 and equity_values[0] != 0:
            total_return = (equity_values[-1] - equity_values[0]) / equity_values[0]
        else:
            total_return = 0.0
        total_orders = int(orders_df.shape[0]) if orders_df is not None else 0
        
        run_data.append({
            'name': run_dir_path.name,
            'df': df,
            'equity_values': equity_values,
            'max_dd': max_dd,
            'total_return': total_return,
            'total_orders': total_orders,
            'color': colors[i % len(colors)],
            'dd_peak_ts': dd_peak_ts,
            'dd_trough_ts': dd_trough_ts,
            'start_equity': equity_values[0] if len(equity_values) > 0 else 0,
            'end_equity': equity_values[-1] if len(equity_values) > 0 else 0
        })
        
        all_equity_values.extend(equity_values)
    
    if not run_data:
        return "No valid run data found."
    
    # Find full date range (not common range) to show all data
    full_start = min(run['df']['ts'].min() for run in run_data)
    full_end = max(run['df']['ts'].max() for run in run_data)
    
    # Keep all data but align by time for visualization
    all_equity_values = []
    for run in run_data:
        # Keep all data, don't filter
        run['equity_values'] = run['df']['equity_eur'].values
        
        # Calculate KPIs using the full run data (not filtered)
        equity_values = run['equity_values']
        if len(equity_values) > 0 and equity_values[0] != 0:
            run['total_return'] = (equity_values[-1] - equity_values[0]) / equity_values[0]
        else:
            run['total_return'] = 0.0
        run['start_equity'] = equity_values[0] if len(equity_values) > 0 else 0
        run['end_equity'] = equity_values[-1] if len(equity_values) > 0 else 0
        
        all_equity_values.extend(equity_values)
    
    # Calculate global min/max for consistent scale
    min_equity = min(all_equity_values)
    max_equity = max(all_equity_values)
    equity_range = max_equity - min_equity
    
    # Avoid division by zero
    if equity_range == 0:
        equity_range = 1.0
    
    # Create ASCII chart
    chart = []
    
    # Header
    chart.append("Multi-Run Comparison Visualization")
    chart.append("=" * 50)
    chart.append(f"Full period: {full_start.strftime('%Y-%m-%d')} to {full_end.strftime('%Y-%m-%d')}")
    chart.append(f"Equity range: {min_equity:.2f} - {max_equity:.2f} EUR")
    chart.append("")
    
    # Create the chart grid
    for row in range(height):
        # Calculate the equity value for this row (inverted for ASCII display)
        row_equity = max_equity - (row * equity_range / (height - 1))
        
        # Create the row
        row_chars = []
        
        # Y-axis label (every 3rd row or first/last)
        if row == 0 or row == height - 1 or row % 3 == 0:
            label = f"{row_equity:7.0f}"
        else:
            label = " " * 7
        row_chars.append(label + " |")
        
        # Plot points for all runs
        for col in range(width):
            # Calculate the time position (0.0 to 1.0) for this column
            time_pos = col / (width - 1)
            
            # Convert time position to actual date
            current_date = full_start + (full_end - full_start) * time_pos
            
            # Check each run for this point
            point_plotted = False
            for run in run_data:
                # Find the closest date in this run's data
                run_dates = run['df']['ts'].values
                if len(run_dates) == 0:
                    continue
                
                # Convert current_date to numpy datetime64 for comparison
                current_date_np = np.datetime64(current_date)
                
                # Find the closest date index
                time_diff = np.abs(run_dates - current_date_np)
                closest_idx = np.argmin(time_diff)
                
                # Only plot if this date is within the run's actual date range
                run_start = run_dates[0]
                run_end = run_dates[-1]
                
                if run_start <= current_date_np <= run_end:
                    if closest_idx < len(run['equity_values']):
                        current_equity = run['equity_values'][closest_idx]
                        
                        # Determine if this point should be plotted
                        if abs(current_equity - row_equity) <= equity_range / (height - 1) / 2:
                            row_chars.append(run['color'])
                            point_plotted = True
                            break
            
            if not point_plotted:
                row_chars.append(" ")
        
        chart.append("".join(row_chars))
    
    # X-axis
    chart.append("-" * 7 + "+" + "-" * width)
    
    # X-axis labels (time)
    time_labels = []
    label_interval = max(1, width // 8)
    for col in range(0, width, label_interval):
        # Calculate the date for this position using the full date range
        time_pos = col / (width - 1)
        date = full_start + (full_end - full_start) * time_pos
        time_label = date.strftime("%Y-%m")
        time_labels.append(f"{time_label:>6}")
    
    # Add proper spacing between labels
    x_axis_line = " " * 8
    for i, label in enumerate(time_labels):
        if i > 0:
            # Add spacing between labels
            x_axis_line += " " * (label_interval - 6)
        x_axis_line += label
    
    chart.append(x_axis_line)
    
    # KPI Comparison Table
    chart.append("")
    chart.append("KPI Comparison Table:")
    chart.append("-" * 80)
    
    # Table header
    header = f"{'Run Name':<20} {'Total Return':<12} {'Max DD':<12} {'Orders':<8} {'Color':<6}"
    chart.append(header)
    chart.append("-" * len(header))
    
    # Calculate best/worst performers
    best_return = max(run['total_return'] for run in run_data)
    worst_dd = min(run['max_dd'] for run in run_data)
    
    for run in run_data:
        # Format values
        if run['total_return'] >= 0:
            return_str = f"+{run['total_return']:.2%}"
        else:
            return_str = f"{run['total_return']:.2%}"
            
        if run['max_dd'] >= 0:
            dd_str = f"+{run['max_dd']:.2%}"
        else:
            dd_str = f"{run['max_dd']:.2%}"
        
        # Highlight best/worst
        if run['total_return'] == best_return:
            return_str += " ★"
        if run['max_dd'] == worst_dd:
            dd_str += " ▼"
        
        row = f"{run['name']:<20} {return_str:<12} {dd_str:<12} {run['total_orders']:<8} {run['color']:<6}"
        chart.append(row)
    
    # Legend
    chart.append("")
    chart.append("Legend:")
    for run in run_data:
        chart.append(f"{run['color']} = {run['name']}")
    chart.append("★ = Best Total Return")
    chart.append("▼ = Worst Max Drawdown")
    
    return "\n".join(chart)