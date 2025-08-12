# Cost Profiles

Costs are configured via YAML and loaded by `load_calculator_from_yaml`.

- US venues: per-share commissions + SEC/TAF fees
- EU venues: basis points
- UK: optional stamp duty

Pass the YAML path to CLI/HTTP to apply costs consistently in backtests and paper.