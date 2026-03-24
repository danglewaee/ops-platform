from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.settings import load_app_settings
from ops_platform.storage import check_storage_health, initialize_storage


def main() -> int:
    settings = load_app_settings()
    db_path = initialize_storage(
        settings.db_path,
        metric_retention_days=settings.timescale_metric_retention_days,
        event_retention_days=settings.timescale_event_retention_days,
        compress_after_days=settings.timescale_compress_after_days,
        create_continuous_aggregate=settings.timescale_create_metric_rollup,
        aggregate_bucket=settings.timescale_aggregate_bucket,
        aggregate_name=settings.timescale_aggregate_name,
        refresh_start_offset=settings.timescale_refresh_start_offset,
        refresh_end_offset=settings.timescale_refresh_end_offset,
        refresh_schedule_interval=settings.timescale_refresh_schedule_interval,
    )
    print(
        json.dumps(
            {
                "db_path": str(db_path),
                "health": check_storage_health(settings.db_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
