from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.recurring_worker import load_recurring_worker_settings, run_recurring_worker


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the recurring Prometheus ingest worker loop.")
    parser.add_argument("--config", help="Path to the recurring pull TOML/JSON config.")
    parser.add_argument("--interval-seconds", type=int, help="Delay after successful runs.")
    parser.add_argument("--fail-delay-seconds", type=int, help="Delay after failed runs.")
    parser.add_argument("--summary-path", help="Optional path for the latest recurring summary JSON.")
    parser.add_argument("--run-once", action="store_true", help="Run a single recurring cycle and exit.")
    parser.add_argument("--disabled", action="store_true", help="Resolve settings but do not execute the worker.")
    args = parser.parse_args()

    settings = load_recurring_worker_settings(
        config_path=args.config,
        interval_seconds=args.interval_seconds,
        fail_delay_seconds=args.fail_delay_seconds,
        summary_path=args.summary_path,
        run_once=True if args.run_once else None,
        enabled=False if args.disabled else None,
    )
    result = run_recurring_worker(settings)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
