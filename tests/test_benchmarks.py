from __future__ import annotations

import json
import sqlite3
import unittest
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from ops_platform.benchmarks import benchmark_persisted_streams, run_benchmark_suite, write_benchmark_artifacts
from ops_platform.pipeline import generate_and_run_pipeline
from ops_platform.storage import ingest_stream_bundle, save_stream_report


def _set_created_at(db_path: Path, stream_id: str, created_at: datetime) -> None:
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute(
            "UPDATE streams SET created_at = ? WHERE stream_id = ?",
            (created_at.isoformat(), stream_id),
        )
        connection.commit()


class BenchmarkTests(unittest.TestCase):
    def test_run_benchmark_suite_aggregates_deterministic_scenarios(self) -> None:
        payload = run_benchmark_suite(seed=7)

        self.assertEqual(payload["suite_name"], "deterministic-scenarios")
        self.assertEqual(payload["summary"]["case_count"], 5)
        self.assertEqual(payload["summary"]["top2_root_cause_accuracy_pct"], 100.0)
        self.assertEqual(payload["summary"]["action_match_rate_pct"], 100.0)
        self.assertEqual(payload["summary"]["false_action_rate_pct"], 0.0)
        self.assertGreater(payload["summary"]["average_first_actionable_minute"], 0.0)
        self.assertEqual(len(payload["cases"]), 5)

    def test_write_benchmark_artifacts_writes_json_and_markdown(self) -> None:
        payload = run_benchmark_suite(seed=7)

        with TemporaryDirectory() as temp_dir:
            json_path, markdown_path = write_benchmark_artifacts(Path(temp_dir), payload)

            self.assertTrue(json_path.exists())
            self.assertTrue(markdown_path.exists())

            summary_payload = json.loads(json_path.read_text(encoding="utf-8"))
            markdown = markdown_path.read_text(encoding="utf-8")

        self.assertEqual(summary_payload["summary"]["case_count"], 5)
        self.assertIn("# Deterministic Scenarios", markdown)
        self.assertIn("| Scenario | Category | Root Cause | Action |", markdown)

    def test_benchmark_persisted_streams_uses_latest_report_payloads(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "ops_platform.sqlite3"
            telemetry, events, metadata, report = generate_and_run_pipeline("traffic_spike", seed=7)
            ingest_stream_bundle(
                "stream-benchmark",
                telemetry,
                events,
                source="prometheus",
                environment="production",
                metadata=metadata,
                db_path=db_path,
            )
            save_stream_report("stream-benchmark", metadata, report, db_path=db_path)
            _set_created_at(db_path, "stream-benchmark", datetime.now() - timedelta(minutes=5))

            payload = benchmark_persisted_streams(
                db_path=db_path,
                environment="production",
                source="prometheus",
                limit=5,
            )

        self.assertEqual(payload["summary"]["case_count"], 1)
        self.assertEqual(payload["cases"][0]["scenario"], "traffic_spike")
        self.assertEqual(payload["cases"][0]["recommended_action"], "scale_out")
        self.assertEqual(payload["summary"]["action_match_rate_pct"], 100.0)


if __name__ == "__main__":
    unittest.main()
