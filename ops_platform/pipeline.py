from __future__ import annotations

from .decision_engine import build_baseline_recommendations, evaluate_recommendations, recommend_actions
from .detection import detect_anomalies
from .forecasting import forecast_services
from .incident_engine import correlate_incidents
from .schemas import PipelineReport
from .simulator import generate_scenario


def run_pipeline(scenario_name: str, *, seed: int = 7) -> PipelineReport:
    telemetry, events, metadata = generate_scenario(scenario_name, seed=seed)
    anomalies = detect_anomalies(telemetry)
    incidents = correlate_incidents(anomalies, events)
    forecasts = forecast_services(telemetry, incidents)
    recommendations, decision_latency_ms = recommend_actions(incidents, forecasts)
    baseline_recommendations = build_baseline_recommendations(incidents, forecasts)
    evaluation = evaluate_recommendations(
        metadata,
        anomalies_count=len(anomalies),
        incidents=incidents,
        recommendations=recommendations,
        decision_latency_ms=decision_latency_ms,
        baseline_recommendations=baseline_recommendations,
    )

    return PipelineReport(
        metadata=metadata,
        anomalies=anomalies,
        incidents=incidents,
        forecasts=forecasts,
        recommendations=recommendations,
        evaluation=evaluation,
    )
