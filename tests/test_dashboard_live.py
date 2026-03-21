from __future__ import annotations

import json
import sqlite3
import unittest
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from ops_platform.dashboard import build_live_stream_bundles, build_live_summary_payload, write_artifacts
from ops_platform.pipeline import generate_and_run_pipeline
from ops_platform.storage import ingest_stream_bundle, save_stream_report
from scripts.serve_dashboard import build_dashboard_html, build_live_payload, resolve_live_filters


def _set_created_at(db_path: Path, stream_id: str, created_at: datetime) -> None:
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            "UPDATE streams SET created_at = ? WHERE stream_id = ?",
            (created_at.isoformat(), stream_id),
        )
        connection.commit()


def _persist_stream(db_path: Path, stream_id: str, scenario_name: str, created_at: datetime) -> str:
    telemetry, events, metadata, report = generate_and_run_pipeline(scenario_name, seed=7)
    ingest_stream_bundle(
        stream_id,
        telemetry,
        events,
        source="prometheus",
        environment="production",
        metadata=metadata,
        db_path=db_path,
    )
    save_stream_report(stream_id, metadata, report, db_path=db_path)
    _set_created_at(db_path, stream_id, created_at)
    return report.recommendations[0].action


class DashboardLiveTests(unittest.TestCase):
    def test_build_live_summary_payload_returns_latest_persisted_stream(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops_platform.sqlite3"
            _persist_stream(db_path, "stream-earlier", "traffic_spike", datetime.now() - timedelta(hours=2))
            expected_action = _persist_stream(db_path, "stream-latest", "queue_backlog", datetime.now() - timedelta(hours=1))

            payload = build_live_summary_payload(
                limit=1,
                environment="production",
                source="prometheus",
                db_path=db_path,
            )

            self.assertEqual(payload["stats"]["stream_count"], 2)
            self.assertEqual(len(payload["streams"]), 1)
            self.assertEqual(payload["streams"][0]["stream_id"], "stream-latest")
            self.assertEqual(payload["streams"][0]["recommendation"]["action"], expected_action)
            self.assertEqual(payload["streams"][0]["evaluation_mode"], "ground_truth")

    def test_build_dashboard_html_renders_live_stream_cards(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops_platform.sqlite3"
            _persist_stream(db_path, "stream-live", "traffic_spike", datetime.now() - timedelta(minutes=30))

            html = build_dashboard_html(
                limit=4,
                environment="production",
                source="prometheus",
                db_path=str(db_path),
            )

            self.assertIn("Live Streams", html)
            self.assertIn("stream-live", html)
            self.assertIn("Recurring pulls and the latest shadow evaluations.", html)

    def test_build_live_payload_and_filters_for_server_helpers(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops_platform.sqlite3"
            _persist_stream(db_path, "stream-live", "traffic_spike", datetime.now() - timedelta(minutes=15))

            filters = resolve_live_filters(
                {"limit": ["2"], "environment": ["production"], "source": ["prometheus"]},
                default_limit=4,
            )
            payload = build_live_payload(db_path=str(db_path), **filters)

            self.assertEqual(filters["limit"], 2)
            self.assertEqual(filters["environment"], "production")
            self.assertEqual(filters["source"], "prometheus")
            self.assertEqual(payload["stats"]["stream_count"], 1)
            self.assertEqual(payload["streams"][0]["stream_id"], "stream-live")

    def test_write_artifacts_writes_live_summary_json(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "ops_platform.sqlite3"
            _persist_stream(db_path, "stream-artifact", "traffic_spike", datetime.now() - timedelta(minutes=10))

            summary_path, dashboard_path = write_artifacts(temp_path, db_path=db_path)

            self.assertTrue(summary_path.exists())
            self.assertTrue(dashboard_path.exists())
            live_summary_path = temp_path / "live_summary.json"
            self.assertTrue(live_summary_path.exists())

            payload = json.loads(live_summary_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["streams"][0]["stream_id"], "stream-artifact")

    def test_build_live_stream_bundles_returns_visualized_stream(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops_platform.sqlite3"
            expected_action = _persist_stream(db_path, "stream-visual", "traffic_spike", datetime.now())

            bundles = build_live_stream_bundles(
                limit=1,
                environment="production",
                source="prometheus",
                db_path=db_path,
            )

            self.assertEqual(len(bundles), 1)
            self.assertEqual(bundles[0]["summary"]["stream_id"], "stream-visual")
            self.assertEqual(bundles[0]["summary"]["recommendation"]["action"], expected_action)
            self.assertIn("metric", bundles[0]["visual"])


if __name__ == "__main__":
    unittest.main()
