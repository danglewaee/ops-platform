from __future__ import annotations

import unittest

from ops_platform.pipeline import run_pipeline, run_pipeline_from_streams, run_scenario_matrix
from ops_platform.scenarios import SCENARIOS
from ops_platform.simulator import generate_scenario


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


if __name__ == "__main__":
    unittest.main()
