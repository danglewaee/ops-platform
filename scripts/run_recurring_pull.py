from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.recurring_pull import load_recurring_pull_settings, run_recurring_pull


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the recurring Prometheus pull -> evaluate -> prune workflow."
    )
    parser.add_argument("--config", required=True, help="Path to a Prometheus TOML or JSON config.")
    parser.add_argument("--base-url", help="Optional override for the Prometheus base URL.")
    parser.add_argument("--events", help="Optional change-event file to merge with the Prometheus metrics.")
    parser.add_argument("--event-mapping", help="Optional mapping config for the event file.")
    parser.add_argument("--lookback-minutes", type=int, help="Override the configured relative lookback window.")
    parser.add_argument("--end", help="Optional range end as ISO-8601 or unix epoch seconds.")
    parser.add_argument("--source", help="Override the configured source label.")
    parser.add_argument("--environment", help="Override the configured environment label.")
    parser.add_argument("--stream-prefix", help="Prefix used when generating the stream_id for each run.")
    parser.add_argument("--name-prefix", help="Prefix used when generating the stored stream name.")
    parser.add_argument("--description", help="Override the stored stream description.")
    parser.add_argument("--root-cause", help="Optional ground-truth root cause when known.")
    parser.add_argument("--expected-action", help="Optional expected action when known.")
    parser.add_argument("--category", help="Override the stored category.")
    parser.add_argument("--db-path", help="Optional override for the SQLite database path.")
    parser.add_argument("--summary-path", help="Optional path for the run summary JSON.")
    parser.add_argument("--older-than-days", type=int, help="Override retention: delete matching streams older than N days.")
    parser.add_argument("--keep-latest", type=int, help="Override retention: keep only the newest N matching streams.")
    parser.add_argument("--vacuum", action="store_true", help="Override retention: run VACUUM after pruning.")
    parser.add_argument("--skip-evaluate", action="store_true", help="Skip the shadow-mode evaluation step.")
    args = parser.parse_args()

    settings = load_recurring_pull_settings(
        args.config,
        base_url=args.base_url,
        events_path=args.events,
        event_mapping=args.event_mapping,
        lookback_minutes=args.lookback_minutes,
        end=args.end,
        source=args.source,
        environment=args.environment,
        stream_prefix=args.stream_prefix,
        name_prefix=args.name_prefix,
        description=args.description,
        root_cause=args.root_cause,
        expected_action=args.expected_action,
        category=args.category,
        evaluate=False if args.skip_evaluate else None,
        db_path=args.db_path,
        summary_path=args.summary_path,
        retention_older_than_days=args.older_than_days,
        retention_keep_latest=args.keep_latest,
        retention_vacuum=True if args.vacuum else None,
    )
    summary = run_recurring_pull(settings)
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
