from __future__ import annotations

import unittest

from ops_platform.detection import detect_anomalies
from ops_platform.incident_engine import correlate_incidents
from ops_platform.simulator import generate_scenario


class IncidentGraphTests(unittest.TestCase):
    def test_bad_deploy_includes_change_evidence_and_dependency_graph(self) -> None:
        telemetry, events, _ = generate_scenario("bad_deploy", seed=7)
        incidents = correlate_incidents(detect_anomalies(telemetry), events)

        incident = incidents[0]
        evidence_types = {item.evidence_type for item in incident.evidence}
        graph_pairs = {(edge.source_service, edge.target_service, edge.relation) for edge in incident.graph_edges}

        self.assertEqual(incident.root_cause_candidates[0], "payments")
        self.assertIn("change_event", evidence_types)
        self.assertIn("anomaly_cluster", evidence_types)
        self.assertIn(("gateway", "payments", "depends_on"), graph_pairs)
        self.assertIn("db", incident.blast_radius_services)
        self.assertIn("error_rate_pct", incident.top_signals)

    def test_memory_leak_builds_blast_radius_through_dependencies(self) -> None:
        telemetry, events, _ = generate_scenario("memory_leak", seed=7)
        incidents = correlate_incidents(detect_anomalies(telemetry), events)

        incident = incidents[0]
        graph_pairs = {(edge.source_service, edge.target_service, edge.relation) for edge in incident.graph_edges}

        self.assertEqual(incident.root_cause_candidates[0], "auth")
        self.assertIn("gateway", incident.blast_radius_services)
        self.assertIn("db", incident.blast_radius_services)
        self.assertIn(("gateway", "auth", "depends_on"), graph_pairs)
        self.assertIn(("auth", "gateway", "impacts"), graph_pairs)

    def test_transient_noise_still_has_compact_graph(self) -> None:
        telemetry, events, _ = generate_scenario("transient_noise", seed=7)
        incidents = correlate_incidents(detect_anomalies(telemetry), events)

        incident = incidents[0]
        self.assertEqual(incident.root_cause_candidates[0], "gateway")
        self.assertLessEqual(len(incident.graph_edges), 6)
        self.assertTrue(incident.evidence)


if __name__ == "__main__":
    unittest.main()
