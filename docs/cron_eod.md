# EOD Cron Runners

Use `ops/cron_eod.py` to schedule end-of-day runs aligned with exchange close.

- Calendar-aware offsets
- Retries on failure

Example crontab:

```
0 22 * * 1-5 /usr/bin/python -m quant.ops.cron_eod
```