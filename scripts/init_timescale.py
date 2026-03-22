from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.storage import get_storage_stats
from ops_platform.timescale_storage import configure_timescale_features, ensure_timescale_schema


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Initialize a TimescaleDB backend for the Ops Decision Platform."
    )
    parser.add_argument(
        "--db-url",
        required=True,
        help="Timescale/PostgreSQL connection string, for example postgresql://user:pass@host:5432/dbname.",
    )
    parser.add_argument(
        "--metric-retention-days",
        type=int,
        help="Optional retention window for metric_samples hypertable.",
    )
    parser.add_argument(
        "--event-retention-days",
        type=int,
        help="Optional retention window for change_events hypertable.",
    )
    parser.add_argument(
        "--compress-after-days",
        type=int,
        help="Optional compression policy threshold for metric_samples.",
    )
    parser.add_argument(
        "--create-metric-rollup",
        action="store_true",
        help="Create a continuous aggregate over metric_samples.",
    )
    parser.add_argument(
        "--aggregate-bucket",
        default="5 minutes",
        help="time_bucket interval for the metric rollup continuous aggregate.",
    )
    parser.add_argument(
        "--aggregate-name",
        default="metric_samples_5m",
        help="Continuous aggregate view name to create.",
    )
    parser.add_argument(
        "--refresh-start-offset",
        default="30 days",
        help="Continuous aggregate refresh start_offset interval.",
    )
    parser.add_argument(
        "--refresh-end-offset",
        default="5 minutes",
        help="Continuous aggregate refresh end_offset interval.",
    )
    parser.add_argument(
        "--refresh-schedule-interval",
        default="5 minutes",
        help="Continuous aggregate refresh schedule interval.",
    )
    args = parser.parse_args()

    db_url = ensure_timescale_schema(args.db_url)
    features = configure_timescale_features(
        db_url,
        metric_retention_days=args.metric_retention_days,
        event_retention_days=args.event_retention_days,
        compress_after_days=args.compress_after_days,
        create_continuous_aggregate=args.create_metric_rollup,
        aggregate_bucket=args.aggregate_bucket,
        aggregate_name=args.aggregate_name,
        refresh_start_offset=args.refresh_start_offset,
        refresh_end_offset=args.refresh_end_offset,
        refresh_schedule_interval=args.refresh_schedule_interval,
    )
    stats = get_storage_stats(db_path=db_url)

    print(
        json.dumps(
            {
                "db_url": db_url,
                "features": features,
                "stats": stats,
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
