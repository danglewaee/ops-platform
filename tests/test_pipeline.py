from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from ops_platform.pipeline import generate_and_run_pipeline, run_pipeline, run_pipeline_from_streams, run_scenario_matrix
from ops_platform.schemas import ScenarioMetadata
from ops_platform.scenarios import SCENARIOS
from ops_platform.simulator import generate_scenario
from ops_platform.storage import (
    ingest_stream_bundle,
    list_ingested_streams,
    load_ingested_stream,
    load_run_bundle,
    save_run_bundle,
    save_stream_report,
)


class PipelineScenarioTests(unittest.TestCase):
    def test_each_scenario_hits_expected_root_cause_and_action(self) -> None:
        for name, metadata in SCENARIOS.items():
            with self.subTest(scenario=name):
                report = run_pipeline(name, seed=7)
                root_candidates = report.incidents[0].root_cause_candidates if report.incidents else []
                self.assertIn(metadata.root_cause, root_candidates[:2])
                self.assertTrue(report.evaluation.recommended_action_match)
                self.assertEqual(report.recommendations[0].action, metadata.expected_action)

    def test_matrix_covers_all_registered_scenarios(self) -> None:
        reports = run_scenario_matrix(seed=7)
        self.assertEqual({report.metadata.name for report in reports}, set(SCENARIOS))

    def test_pipeline_from_streams_matches_named_scenario(self) -> None:
        telemetry, events, metadata = generate_scenario("traffic_spike", seed=7)
        from_streams = run_pipeline_from_streams(telemetry, events, metadata)
        direct = run_pipeline("traffic_spike", seed=7)
        self.assertEqual(from_streams.metadata.name, direct.metadata.name)
        self.assertEqual(from_streams.recommendations[0].action, direct.recommendations[0].action)
        self.assertEqual(from_streams.incidents[0].root_cause_candidates[:2], direct.incidents[0].root_cause_candidates[:2])

    def test_transient_noise_holds_steady(self) -> None:
        report = run_pipeline("transient_noise", seed=7)
        self.assertEqual(report.recommendations[0].action, "hold_steady")
        self.assertTrue(report.evaluation.recommended_action_match)

    def test_saved_run_replays_cleanly(self) -> None:
        with TemporaryDirectory() as temp_dir:
            telemetry, events, metadata, report = generate_and_run_pipeline("traffic_spike", seed=7)
            path = save_run_bundle(telemetry, events, metadata, report, seed=7, output_dir=Path(temp_dir))
            bundle = load_run_bundle(path)
            replay = run_pipeline_from_streams(bundle["telemetry"], bundle["events"], bundle["metadata"])
            self.assertEqual(report.recommendations[0].action, replay.recommendations[0].action)
            self.assertEqual(report.incidents[0].root_cause_candidates[:2], replay.incidents[0].root_cause_candidates[:2])
            self.assertEqual(report.service_health[0].service, replay.service_health[0].service)

    def test_evaluation_includes_shadow_metrics(self) -> None:
        report = run_pipeline("traffic_spike", seed=7)
        baseline_policies = {item["policy"] for item in report.evaluation.baseline_comparisons}
        self.assertEqual(report.evaluation.evaluation_mode, "ground_truth")
        self.assertIn("naive_reroute", baseline_policies)
        self.assertTrue(report.service_health)
        self.assertGreater(report.forecasts[0].projected_burn_rate, 0.0)
        self.assertGreater(report.evaluation.latency_protection_pct, 0.0)
        self.assertGreaterEqual(report.evaluation.baseline_win_rate_pct, 0.0)
        self.assertEqual(report.evaluation.action_stability_pct, 100.0)

    def test_shadow_only_mode_without_ground_truth(self) -> None:
        telemetry, events, _ = generate_scenario("traffic_spike", seed=7)
        metadata = ScenarioMetadata(
            name="ingested-traffic-spike",
            description="Persisted stream replay without scenario labels.",
            root_cause="",
            expected_action="",
            impacted_services=["gateway", "worker", "payments"],
            category="live",
        )
        report = run_pipeline_from_streams(telemetry, events, metadata)
        self.assertEqual(report.evaluation.evaluation_mode, "shadow_only")
        self.assertIsNone(report.evaluation.top2_root_cause_hit)
        self.assertIsNone(report.evaluation.recommended_action_match)

    def test_sqlite_ingestion_and_report_round_trip(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops_platform.sqlite3"
            telemetry, events, metadata, report = generate_and_run_pipeline("traffic_spike", seed=7)
            ingest_stream_bundle(
                "stream-traffic-spike",
                telemetry,
                events,
                source="test",
                environment="staging",
                metadata=metadata,
                db_path=db_path,
            )
            stream = load_ingested_stream("stream-traffic-spike", db_path=db_path)
            self.assertEqual(len(stream["telemetry"]), len(telemetry))
            self.assertEqual(len(stream["events"]), len(events))
            self.assertEqual(stream["environment"], "staging")

            save_stream_report("stream-traffic-spike", metadata, report, db_path=db_path)
            streams = list_ingested_streams(db_path=db_path)
            self.assertEqual(len(streams), 1)
            self.assertEqual(streams[0]["stream_id"], "stream-traffic-spike")
            self.assertEqual(streams[0]["latest_recommended_action"], report.recommendations[0].action)


if __name__ == "__main__":
    unittest.main()
