from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# Counters
# Total processed events (use PromQL rate() to get events/sec)
events_total = Counter("quant_events_total", "Total number of processed events", ["source"])  # source: backtest|service|worker

# Total errors by component
errors_total = Counter("quant_errors_total", "Total errors by component", ["component"])  # component: backtest|service|other

# Orders and fills
orders_total = Counter("quant_orders_total", "Total orders processed")
fills_total = Counter("quant_fills_total", "Total fills processed")

# Gauges
queue_lag_seconds = Gauge("quant_queue_lag_seconds", "Queue lag in seconds (simulation or service)")

# Histograms
# Slippage in basis points per fill
fill_slippage_bps = Histogram(
    "quant_fill_slippage_bps",
    "Per-fill absolute slippage in basis points relative to mid",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500),
)

# Backtest step duration in seconds
backtest_step_duration_seconds = Histogram(
    "quant_backtest_step_duration_seconds",
    "Wall-clock duration per backtest timestep",
    buckets=(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2)
)