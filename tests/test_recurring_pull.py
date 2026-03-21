from __future__ import annotations

import json
import sqlite3
import unittest
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from ops_platform.pipeline import generate_and_run_pipeline
from ops_platform.recurring_pull import load_recurring_pull_settings, run_recurring_pull
from ops_platform.storage import ingest_stream_bundle, list_ingested_streams


def _set_created_at(db_path: Path, stream_id: str, created_at: datetime) -> None:
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            "UPDATE streams SET created_at = ? WHERE stream_id = ?",
            (created_at.isoformat(), stream_id),
        )
        connection.commit()


class RecurringPullTests(unittest.TestCase):
    def test_run_recurring_pull_uses_configured_retention_and_writes_summary(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "ops_platform.sqlite3"
            summary_path = temp_path / "recurring-summary.json"
            config_path = temp_path / "prometheus.toml"
            config_path.write_text(
                "\n".join(
                    [
                        'base_url = "http://prometheus.local:9090"',
                        'step = "60s"',
                        "",
                        "[queries]",
                        'request_rate = "request_rate_query"',
                        "",
                        "[recurring]",
                        'lookback_minutes = 30',
                        'environment = "production"',
                        'source = "prometheus"',
                        'stream_prefix = "ops-live"',
                        'name_prefix = "ops-live"',
                        f'summary_path = "{summary_path.as_posix()}"',
                        'evaluate = true',
                        "",
                        "[retention]",
                        'older_than_days = 7',
                        'keep_latest = 24',
                        'vacuum = false',
                    ]
                ),
                encoding="utf-8",
            )

            telemetry, events, metadata, _ = generate_and_run_pipeline("traffic_spike", seed=7)
            ingest_stream_bundle(
                "old-prom-stream",
                telemetry,
                events,
                source="prometheus",
                environment="production",
                metadata=metadata,
                db_path=db_path,
            )
            _set_created_at(db_path, "old-prom-stream", datetime.now() - timedelta(days=10))

            settings = load_recurring_pull_settings(
                config_path,
                db_path=str(db_path),
                root_cause=metadata.root_cause,
                expected_action=metadata.expected_action,
                description=metadata.description,
                category=metadata.category,
            )

            with patch(
                "ops_platform.recurring_pull.load_prometheus_bundle",
                return_value=(
                    SimpleNamespace(step="60s"),
                    telemetry,
                    events,
                    datetime(2026, 3, 21, 9, 0, 0),
                    datetime(2026, 3, 21, 9, 30, 0),
                ),
            ):
                summary = run_recurring_pull(settings)

            self.assertEqual(summary["before"]["stream_count"], 1)
            self.assertEqual(summary["ingest"]["evaluation"]["recommended_action"], metadata.expected_action)
            self.assertEqual(summary["prune"]["deleted_stream_ids"], ["old-prom-stream"])
            self.assertEqual(summary["after"]["stream_count"], 1)
            self.assertTrue(summary_path.exists())

            file_payload = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(file_payload["ingest"]["stream_id"], summary["ingest"]["stream_id"])
            self.assertTrue(file_payload["ingest"]["stream_id"].startswith("ops-live-"))

            streams = list_ingested_streams(db_path=db_path)
            self.assertEqual(len(streams), 1)
            self.assertTrue(streams[0]["stream_id"].startswith("ops-live-"))


if __name__ == "__main__":
    unittest.main()
