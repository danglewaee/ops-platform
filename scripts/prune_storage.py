from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.storage import get_storage_stats, prune_ingested_streams


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect and prune persisted SQLite telemetry streams.")
    parser.add_argument("--db-path", help="Optional override for the SQLite database path.")
    parser.add_argument("--environment", help="Only include streams from this environment.")
    parser.add_argument("--source", help="Only include streams from this source.")
    parser.add_argument("--created-after", help="Only include streams created at or after this ISO-8601 timestamp.")
    parser.add_argument("--created-before", help="Only include streams created at or before this ISO-8601 timestamp.")
    parser.add_argument("--older-than-days", type=int, help="Delete matching streams older than this many days.")
    parser.add_argument("--keep-latest", type=int, help="Keep only the newest N matching streams.")
    parser.add_argument("--vacuum", action="store_true", help="Run SQLite VACUUM after pruning.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without changing the DB.")
    parser.add_argument("--stats-only", action="store_true", help="Print storage stats without pruning.")
    args = parser.parse_args()

    before = get_storage_stats(
        environment=args.environment,
        source=args.source,
        created_after=args.created_after,
        created_before=args.created_before,
        db_path=args.db_path,
    )

    if args.stats_only or (args.older_than_days is None and args.keep_latest is None):
        print(json.dumps({"before": before}, indent=2, default=str))
        return 0

    prune_summary = prune_ingested_streams(
        older_than_days=args.older_than_days,
        keep_latest=args.keep_latest,
        environment=args.environment,
        source=args.source,
        vacuum=args.vacuum,
        dry_run=args.dry_run,
        db_path=args.db_path,
    )
    after = get_storage_stats(
        environment=args.environment,
        source=args.source,
        created_after=args.created_after,
        created_before=args.created_before,
        db_path=args.db_path,
    )

    print(
        json.dumps(
            {
                "before": before,
                "prune": prune_summary,
                "after": after,
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
