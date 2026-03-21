from __future__ import annotations

import sqlite3
from contextlib import closing
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from ops_platform.pipeline import generate_and_run_pipeline
from ops_platform.storage import (
    get_storage_stats,
    ingest_stream_bundle,
    list_ingested_streams,
    prune_ingested_streams,
    save_stream_report,
)


def _set_created_at(db_path: Path, stream_id: str, created_at: datetime) -> None:
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            "UPDATE streams SET created_at = ? WHERE stream_id = ?",
            (created_at.isoformat(), stream_id),
        )
        connection.commit()


class StorageMaintenanceTests(unittest.TestCase):
    def test_list_streams_supports_filters_and_stats(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops_platform.sqlite3"
            telemetry, events, metadata, report = generate_and_run_pipeline("traffic_spike", seed=7)

            ingest_stream_bundle("prod-a", telemetry, events, source="prometheus", environment="production", metadata=metadata, db_path=db_path)
            ingest_stream_bundle("prod-b", telemetry, events, source="prometheus", environment="production", metadata=metadata, db_path=db_path)
            ingest_stream_bundle("stage-a", telemetry, events, source="file-import", environment="staging", metadata=metadata, db_path=db_path)
            save_stream_report("prod-a", metadata, report, db_path=db_path)
            save_stream_report("prod-b", metadata, report, db_path=db_path)

            filtered = list_ingested_streams(environment="production", source="prometheus", db_path=db_path)
            self.assertEqual(len(filtered), 2)
            self.assertTrue(all(stream["environment"] == "production" for stream in filtered))
            self.assertTrue(all(stream["source"] == "prometheus" for stream in filtered))

            stats = get_storage_stats(environment="production", source="prometheus", db_path=db_path)
            self.assertEqual(stats["stream_count"], 2)
            self.assertEqual(stats["metric_sample_count"], len(telemetry) * 2)
            self.assertEqual(stats["event_count"], len(events) * 2)
            self.assertEqual(stats["report_count"], 2)

    def test_prune_supports_dry_run_and_keep_latest(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops_platform.sqlite3"
            telemetry, events, metadata, report = generate_and_run_pipeline("traffic_spike", seed=7)

            ingest_stream_bundle("old-a", telemetry, events, source="prometheus", environment="production", metadata=metadata, db_path=db_path)
            ingest_stream_bundle("old-b", telemetry, events, source="prometheus", environment="production", metadata=metadata, db_path=db_path)
            ingest_stream_bundle("new-c", telemetry, events, source="prometheus", environment="production", metadata=metadata, db_path=db_path)
            save_stream_report("old-a", metadata, report, db_path=db_path)
            save_stream_report("old-b", metadata, report, db_path=db_path)
            save_stream_report("new-c", metadata, report, db_path=db_path)

            now = datetime.now()
            _set_created_at(db_path, "old-a", now - timedelta(days=10))
            _set_created_at(db_path, "old-b", now - timedelta(days=5))
            _set_created_at(db_path, "new-c", now)

            dry_run = prune_ingested_streams(older_than_days=7, dry_run=True, db_path=db_path)
            self.assertEqual(dry_run["deleted_stream_count"], 1)
            self.assertEqual(dry_run["deleted_stream_ids"], ["old-a"])
            self.assertEqual(get_storage_stats(db_path=db_path)["stream_count"], 3)

            pruned = prune_ingested_streams(keep_latest=1, vacuum=True, db_path=db_path)
            self.assertEqual(pruned["deleted_stream_count"], 2)
            self.assertEqual(set(pruned["deleted_stream_ids"]), {"old-a", "old-b"})

            remaining = list_ingested_streams(db_path=db_path)
            self.assertEqual(len(remaining), 1)
            self.assertEqual(remaining[0]["stream_id"], "new-c")

    def test_storage_api_routes_expose_stats_and_prune(self) -> None:
        try:
            from ops_platform.api import create_app
        except RuntimeError as exc:  # pragma: no cover - optional dependency guard
            self.skipTest(str(exc))

        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops_platform.sqlite3"
            telemetry, events, metadata, _ = generate_and_run_pipeline("traffic_spike", seed=7)
            ingest_stream_bundle("api-a", telemetry, events, source="prometheus", environment="production", metadata=metadata, db_path=db_path)
            ingest_stream_bundle("api-b", telemetry, events, source="file-import", environment="staging", metadata=metadata, db_path=db_path)

            app = create_app()
            routes = {route.path: route.endpoint for route in app.routes if hasattr(route, "endpoint")}
            streams_response = routes["/streams"](environment="production", db_path=str(db_path))
            stats_response = routes["/storage/stats"](environment="production", db_path=str(db_path))
            prune_response = routes["/storage/prune"](
                SimpleNamespace(
                    older_than_days=None,
                    keep_latest=0,
                    environment="production",
                    source=None,
                    vacuum=False,
                    dry_run=False,
                    db_path=str(db_path),
                )
            )

            self.assertEqual(len(streams_response), 1)
            self.assertEqual(streams_response[0]["stream_id"], "api-a")
            self.assertEqual(stats_response["stream_count"], 1)
            self.assertEqual(prune_response["deleted_stream_count"], 1)
            self.assertEqual(get_storage_stats(db_path=db_path)["stream_count"], 1)


if __name__ == "__main__":
    unittest.main()
