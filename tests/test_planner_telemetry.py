from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from ops_platform.pipeline import run_pipeline
from ops_platform.recurring_pull import load_recurring_pull_settings, run_recurring_pull
from ops_platform.schemas import DecisionConstraints
from ops_platform.telemetry import OTEL_AVAILABLE, configure_tracing


class PlannerTelemetryTests(unittest.TestCase):
    def test_default_pipeline_reports_planner_metadata(self) -> None:
        report = run_pipeline("traffic_spike", seed=7)
        self.assertEqual(report.evaluation.planner_mode, "heuristic")
        if report.evaluation.trace_id is not None:
            self.assertEqual(len(report.evaluation.trace_id), 32)

    def test_pipeline_emits_trace_id_when_tracing_is_configured(self) -> None:
        if not OTEL_AVAILABLE:
            self.skipTest("OpenTelemetry extras are not installed.")

        self.assertTrue(configure_tracing(service_name="ops-platform-test"))
        report = run_pipeline("traffic_spike", seed=7)
        self.assertIsNotNone(report.evaluation.trace_id)
        self.assertEqual(len(report.evaluation.trace_id), 32)

    def test_cost_budget_can_override_default_recommendation(self) -> None:
        report = run_pipeline(
            "traffic_spike",
            seed=7,
            planner_mode="cp_sat",
            decision_constraints=DecisionConstraints(max_total_cost_delta_pct=4.0),
        )
        self.assertEqual(report.recommendations[0].action, "reroute_traffic")
        self.assertIn(report.evaluation.planner_mode, {"heuristic", "cp_sat"})

    def test_constraints_can_disable_increase_consumers(self) -> None:
        report = run_pipeline(
            "queue_backlog",
            seed=7,
            decision_constraints=DecisionConstraints(
                allow_increase_consumers=False,
                allow_reroute_traffic=False,
            ),
        )
        self.assertEqual(report.recommendations[0].action, "scale_out")

    def test_recurring_pull_loads_decision_and_observability_config(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            config_path = temp_path / "prometheus.toml"
            db_path = temp_path / "ops_platform.sqlite3"
            summary_path = temp_path / "summary.json"
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
                        'planner_mode = "cp_sat"',
                        f'summary_path = "{summary_path.as_posix()}"',
                        'evaluate = true',
                        "",
                        "[decision]",
                        'max_total_cost_delta_pct = 4.0',
                        "",
                        "[observability]",
                        'enable_tracing = true',
                        'service_name = "ops-platform-test"',
                        'otlp_endpoint = "http://localhost:4318/v1/traces"',
                    ]
                ),
                encoding="utf-8",
            )

            settings = load_recurring_pull_settings(config_path, db_path=str(db_path))
            self.assertEqual(settings.planner_mode, "cp_sat")
            self.assertIsNotNone(settings.decision_constraints)
            self.assertEqual(settings.decision_constraints.max_total_cost_delta_pct, 4.0)
            self.assertTrue(settings.enable_tracing)
            self.assertEqual(settings.tracing_service_name, "ops-platform-test")

            telemetry, events = _load_scenario_streams()

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

            self.assertEqual(summary["ingest"]["evaluation"]["recommended_action"], "reroute_traffic")
            self.assertIn(summary["ingest"]["evaluation"]["planner_mode"], {"heuristic", "cp_sat"})
            self.assertTrue(summary["observability"]["tracing_enabled"])

            trace_id = summary["ingest"]["evaluation"]["trace_id"]
            if trace_id is not None:
                self.assertEqual(len(trace_id), 32)


def _load_scenario_streams():
    from ops_platform.simulator import generate_scenario

    telemetry, events, _ = generate_scenario("traffic_spike", seed=7)
    return telemetry, events


if __name__ == "__main__":
    unittest.main()
