# Incident Runbook

1. Triage
   - Check `/health` and `/metrics`
   - Review logs in `runs/<...>/logs.jsonl`
2. Mitigate
   - Enable dry-run if live adapter misbehaves
   - Reduce parallelism if resource constrained
3. Recover
   - Re-run failed backtests with the same seed for determinism
   - Attach artifacts and manifest to incident ticket