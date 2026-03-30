from __future__ import annotations

import unittest

from ops_platform.detection import detect_anomalies
from ops_platform.feature_builder import build_service_health
from ops_platform.forecasting import forecast_services
from ops_platform.incident_engine import correlate_incidents
from ops_platform.pipeline import run_pipeline
from ops_platform.simulator import generate_scenario


class ServiceHealthTests(unittest.TestCase):
    def test_traffic_spike_surfaces_gateway_budget_pressure(self) -> None:
        report = run_pipeline("traffic_spike", seed=7)

        gateway_health = next(item for item in report.service_health if item.service == "gateway")
        gateway_forecast = next(item for item in report.forecasts if item.service == "gateway")

        self.assertGreater(gateway_health.projected_burn_rate, 1.25)
        self.assertIn(gateway_health.budget_pressure, {"high", "critical"})
        self.assertGreaterEqual(gateway_forecast.projected_burn_rate, gateway_forecast.current_burn_rate)
        self.assertEqual(report.recommendations[0].action, "scale_out")

    def test_queue_backlog_marks_queue_depth_as_dominant_signal(self) -> None:
        report = run_pipeline("queue_backlog", seed=7)

        worker_health = next(item for item in report.service_health if item.service == "worker")
        worker_forecast = next(item for item in report.forecasts if item.service == "worker")

        self.assertEqual(worker_health.dominant_signal, "queue_depth")
        self.assertEqual(worker_forecast.dominant_slo_signal, "queue_depth")
        self.assertEqual(report.recommendations[0].action, "increase_consumers")

    def test_transient_noise_does_not_trip_critical_budget_pressure(self) -> None:
        report = run_pipeline("transient_noise", seed=7)

        gateway_health = next(item for item in report.service_health if item.service == "gateway")
        self.assertIn(gateway_health.budget_pressure, {"low", "medium"})
        self.assertEqual(report.recommendations[0].action, "hold_steady")

    def test_feature_builder_round_trips_current_health_into_forecasting(self) -> None:
        telemetry, events, _ = generate_scenario("traffic_spike", seed=7)
        anomalies = detect_anomalies(telemetry)
        incidents = correlate_incidents(anomalies, events)
        current_health = build_service_health(telemetry, incidents)
        forecasts = forecast_services(telemetry, incidents, service_health=current_health)

        self.assertEqual(len(current_health), 1)
        self.assertEqual(len(forecasts), 1)
        self.assertGreaterEqual(forecasts[0].projected_burn_rate, current_health[0].current_burn_rate)


if __name__ == "__main__":
    unittest.main()
