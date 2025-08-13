from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from quant.data.calendars import next_close


def _sleep_until(ts: datetime) -> None:
    now = datetime.now(tz=timezone.utc)
    target = ts.astimezone(timezone.utc)
    seconds = max(0.0, (target - now).total_seconds())
    time.sleep(seconds)


def run_eod_loop(
    *,
    exchange: str,
    offset_minutes: int = 5,
    command: list[str],
    max_retries: int = 3,
    retry_delay_seconds: int = 30,
) -> None:
    while True:
        # Determine next close in local exchange time via calendar util
        local_noon = datetime.now().replace(hour=12, minute=0, second=0, microsecond=0)
        nxt = next_close(exchange, local_noon)
        run_at = nxt + timedelta(minutes=offset_minutes)
        _sleep_until(run_at)

        attempt = 0
        while attempt <= max_retries:
            try:
                print(f"[cron] Running command at {datetime.now(timezone.utc).isoformat()}Z: {' '.join(command)}")
                res = subprocess.run(command, check=True)
                print(f"[cron] Success with exit code {res.returncode}")
                break
            except subprocess.CalledProcessError as exc:
                attempt += 1
                print(f"[cron] Attempt {attempt} failed with code {exc.returncode}; retrying in {retry_delay_seconds}s")
                if attempt > max_retries:
                    print("[cron] Max retries exceeded; giving up for this schedule")
                    break
                time.sleep(retry_delay_seconds)

        # sleep a minute to avoid tight loop
        time.sleep(60)


if __name__ == "__main__":
    # Example usage: python -m ops.cron_eod XNYS 5
    ex = sys.argv[1] if len(sys.argv) > 1 else "XNYS"
    off = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    cmd = [
        sys.executable,
        "-m",
        "quant.orchestrator.cli",
        "run-backtest",
        "--strategy-name",
        "ma_cross",
        "--strategy-symbol",
        "AAPL",
        "--start",
        datetime.now(timezone.utc).date().isoformat() + "T00:00:00+00:00",
        "--end",
        datetime.now(timezone.utc).date().isoformat() + "T00:00:00+00:00",
        "--bars-csv",
        "data/bars.csv",
        "--exchange",
        ex,
    ]
    run_eod_loop(exchange=ex, offset_minutes=off, command=cmd)