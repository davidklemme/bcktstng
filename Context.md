Vision & scope
Build an event-driven backtesting engine with reproducible runs, realistic costs, and pluggable data/execution.
One Strategy → runs in backtest, paper, or live unchanged.
Strategies are callable via HTTP/gRPC and CLI, schedulable by cron or a workflow engine.
Non-functional requirements
Deterministic replays; seedable RNG.
Throughput: ≥ 5M events/hour on a laptop; scale linearly.
Latency (paper/live EOD): ≤ 5s end-to-end from market close to orders.
Audit: immutable run metadata + dataset versions.
Test coverage: ≥ 80% on engine core.
High-level architecture
data/: adapters → parquet lake (+ Postgres metadata), calendars, corporate actions.
engine/: event loop, clock, execution sim, portfolio, risk.
sdk/: Strategy interface + helpers.
orchestrator/: HTTP/gRPC service, CLI, schedulers (cron/Prefect).
adapters/: brokers (paper, IBKR, Alpaca), market data vendors.
ops/: metrics, logging, config, secrets.
examples/: strategies, notebooks.
Core data model (minimum viable)
Symbol master: symbol_id, ticker, exchange, currency, active_from, active_to.
Bars: ts, symbol_id, open, high, low, close, volume.
Quotes/Trades (optional for v1).
Corporate actions: split_ratio, dividend, effective_date.
Options (v2): ts, symbol_id, expiry, strike, right, bid, ask, iv, oi, vol.
Calendars: per-exchange sessions.
Storage: Parquet partitioned by dt=YYYY-MM-DD/symbol=…; Arrow schema; Postgres for catalogs.
Backtest engine spec
Event loop: processes Bar|Quote|Trade|OptionChainUpdate|CorporateAction|Clock.
Clock: UTC, exchange-aware; guarantees no data > now.
Portfolio: positions (avg price, qty, lot ids), cash, leverage, margin, FX, borrow fees.
Execution sim: order types (MKT, LMT, STP, STP-LMT), partial fills, queue priority.
Slippage/impact:
Fill = mid ± k·spread; k depends on order urgency.
Impact = sign(order) * sigma * sqrt(qty/ADV) * alpha, tunable.
Costs: commissions, fees, borrow, option assignment/exercise rules.
Risk: caps (gross/net, per-symbol, sector), VaR/vol targeting (simple).
Metrics: equity curve, drawdowns, turnover, exposure, hit rate, Sharpe/Sortino, per-order slippage.
Strategy SDK (Python first; language-agnostic later)
class Strategy:
    def on_start(self, ctx): ...
    def on_event(self, evt, ctx): ...
    def on_end(self, ctx): ...

# ctx exposes:
ctx.now, ctx.calendar, ctx.log
ctx.data.get(symbol, fields, lookback, at=None)
ctx.order(symbol, qty, type="limit", limit_price=None, tif="DAY", tag=None)
ctx.cancel(tag_or_id)
ctx.portfolio, ctx.risk.set(max_gross=..., max_symbol=..., max_leverage=...)
ctx.features.rolling_mean(...), .vol_target(...), .zscore(...)
Helpers: MA cross, breakout, pair-trade, volatility targeting, earnings filter.
Options helpers (v2): spread builder, roll rules, early-exercise checker.
Orchestration & “callable algos”
Strategy packaged as container exposing:
POST /signal {strategy, asof} → {orders[], rationale, riskCaps}
POST /backtest {strategy, config, date_range} → {run_id, metrics, artifacts}
GET /health, GET /metrics (Prometheus).
CLI parity: quant run backtest …, quant run signal ….
Schedulers: cron for EOD; Prefect/Temporal optional (calendar-aware retries).
Execution adapters
Paper: in-mem broker with the same API as live.
Live: adapters (IBKR/Alpaca) behind ExecutionProvider interface.
Order router: risk checks → throttling → provider.
Reproducibility & governance
Run manifest: {git_sha, params_hash, data_versions, slippage_model, seed, calendar_version, timestamp}.
Artifacts: equity.csv, orders.csv, fills.csv, positions.csv, metrics.json, logs.jsonl.
Each run gets an immutable run_id.
Observability
Structured logs (JSONL).
Metrics: Prometheus (engine events/sec, queue lag, fill slippage, error rates).
Tracing (OpenTelemetry) around event loop and data fetch.
Security/compliance
Secrets via env/VAULT.
Signed configs for live; dry-run mode.
Strategy sandboxing: no network by default during backtests.
Roadmap (milestones)
M1 – MVP Backtester (2–3 weeks)
Event engine, split-aware data loader, daily bars, basic slippage/fees, 2 demo strategies, metrics, CLI.
M2 – Portfolio, Risk, Walk-forward (2 weeks)
Margin/borrow, impact model v1, walk-forward & purged k-fold, hyper-grid search, run artifacts.

M3 – Paper & Live Interface (2 weeks)
Paper broker, execution provider interface, orders topic, HTTP /signal, containerization, Prometheus.

M4 – Options & Advanced Costs (3–4 weeks)
Options chains, greeks/IV ingestion, assignment/early exercise, borrow/locate modeling, spread builder.

M5 – Orchestration/Prefect + Dashboards (1–2 weeks)
Calendar-aware schedules, retries, Grafana boards, access control for live.

Repo layout
/quant
  /adapters/{data,exec}/...
  /engine/{events,clock,portfolio,execution,risk}/...
  /sdk/{strategy.py, features.py}
  /orchestrator/{service.py, grpc/, cli.py}
  /data/{schemas, loaders, calendars}
  /ops/{metrics.py, logging.py, config.py}
  /examples/{ma_cross, covered_call}
  /tests/...
Acceptance test matrix (extract)
Look-ahead guard: features at t cannot reference > t.
Split/div handling: price/qty adjusted; P&L consistent across split dates.
Slippage realism: average fill within [bid, ask]; market orders never outside worst quote + guardrail.
Walk-forward: no overlap/leakage; purged k-fold verified.
Determinism: same seed → same fills & metrics.
Paper/live parity: given identical quotes, paper fills ≈ live (tolerance band).
