from __future__ import annotations

import unittest

from ops_platform.benchmarks import run_benchmark_suite
from ops_platform.pipeline import run_pipeline, run_scenario_matrix
from ops_platform.scenarios import BOUTIQUE_SCENARIOS, list_scenarios
from ops_platform.testbed import list_testbed_profiles


class TestbedProfileTests(unittest.TestCase):
    def test_lists_registered_testbed_profiles_and_scenarios(self) -> None:
        self.assertIn("core", list_testbed_profiles())
        self.assertIn("boutique_like", list_testbed_profiles())
        self.assertEqual(set(list_scenarios(profile="boutique_like")), set(BOUTIQUE_SCENARIOS))

    def test_boutique_like_scenarios_hit_expected_root_cause_and_action(self) -> None:
        for name, metadata in BOUTIQUE_SCENARIOS.items():
            with self.subTest(scenario=name):
                report = run_pipeline(name, seed=7, testbed_profile="boutique_like")
                root_candidates = report.incidents[0].root_cause_candidates if report.incidents else []
                self.assertIn(metadata.root_cause, root_candidates[:2])
                self.assertTrue(report.evaluation.recommended_action_match)
                self.assertEqual(report.recommendations[0].action, metadata.expected_action)
                self.assertEqual(report.metadata.testbed_profile, "boutique_like")

    def test_boutique_like_matrix_and_benchmark_suite(self) -> None:
        reports = run_scenario_matrix(seed=7, testbed_profile="boutique_like")
        self.assertEqual({report.metadata.name for report in reports}, set(BOUTIQUE_SCENARIOS))

        payload = run_benchmark_suite(seed=7, testbed_profile="boutique_like")
        self.assertEqual(payload["suite_name"], "boutique_like-scenarios")
        self.assertEqual(payload["summary"]["case_count"], 5)
        self.assertEqual(payload["summary"]["top2_root_cause_accuracy_pct"], 100.0)
        self.assertEqual(payload["summary"]["action_match_rate_pct"], 100.0)
        self.assertEqual(payload["summary"]["false_action_rate_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()
