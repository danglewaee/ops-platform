from __future__ import annotations

from .decision_engine import (
    build_baseline_recommendations,
    evaluate_recommendations,
    recommend_actions,
)
from .detection import detect_anomalies
from .feature_builder import build_service_health
from .forecasting import forecast_services
from .incident_engine import correlate_incidents
from .schemas import ChangeEvent, DecisionConstraints, MetricSample, PipelineReport, ScenarioMetadata
from .scenarios import list_scenarios
from .simulator import generate_scenario
from .telemetry import annotate_span, current_trace_id, traced_span


def run_pipeline_from_streams(
    telemetry: list[MetricSample],
    events: list[ChangeEvent],
    metadata: ScenarioMetadata,
    *,
    planner_mode: str = "heuristic",
    decision_constraints: DecisionConstraints | None = None,
) -> PipelineReport:
    with traced_span(
        "ops.pipeline.run",
        {
            "ops.scenario.name": metadata.name,
            "ops.planner.requested": planner_mode,
            "ops.telemetry.samples": len(telemetry),
            "ops.change_events": len(events),
        },
    ) as pipeline_span:
        with traced_span("ops.pipeline.detect"):
            anomalies = detect_anomalies(telemetry)
        annotate_span(pipeline_span, ops_anomalies=len(anomalies))

        with traced_span("ops.pipeline.correlate"):
            incidents = correlate_incidents(anomalies, events)
        annotate_span(pipeline_span, ops_incidents=len(incidents))

        with traced_span("ops.pipeline.feature_build"):
            current_service_health = build_service_health(telemetry, incidents)
        annotate_span(
            pipeline_span,
            ops_service_health=len(current_service_health),
            ops_slo_hot_services=sum(1 for item in current_service_health if item.budget_pressure in {"high", "critical"}),
        )

        with traced_span("ops.pipeline.forecast"):
            forecasts = forecast_services(telemetry, incidents, service_health=current_service_health)

        with traced_span("ops.pipeline.feature_finalize"):
            service_health = build_service_health(telemetry, incidents, forecasts)

        with traced_span("ops.pipeline.decide", {"ops.planner.mode": planner_mode}):
            recommendations, decision_latency_ms, actual_planner_mode = recommend_actions(
                incidents,
                forecasts,
                service_health=service_health,
                planner_mode=planner_mode,
                constraints=decision_constraints,
            )

        with traced_span("ops.pipeline.evaluate"):
            baseline_recommendations = build_baseline_recommendations(incidents, forecasts)
            evaluation = evaluate_recommendations(
                metadata,
                anomalies_count=len(anomalies),
                incidents=incidents,
                recommendations=recommendations,
                decision_latency_ms=decision_latency_ms,
                baseline_recommendations=baseline_recommendations,
                planner_mode=actual_planner_mode,
                trace_id=current_trace_id(),
            )
        annotate_span(
            pipeline_span,
            ops_planner_actual=actual_planner_mode,
            ops_recommendations=len(recommendations),
            ops_trace_id=evaluation.trace_id,
        )

    return PipelineReport(
        metadata=metadata,
        anomalies=anomalies,
        incidents=incidents,
        forecasts=forecasts,
        service_health=service_health,
        recommendations=recommendations,
        evaluation=evaluation,
    )


def run_pipeline(
    scenario_name: str,
    *,
    seed: int = 7,
    planner_mode: str = "heuristic",
    decision_constraints: DecisionConstraints | None = None,
    testbed_profile: str | None = None,
) -> PipelineReport:
    telemetry, events, metadata = generate_scenario(
        scenario_name,
        seed=seed,
        testbed_profile=testbed_profile,
    )
    return run_pipeline_from_streams(
        telemetry,
        events,
        metadata,
        planner_mode=planner_mode,
        decision_constraints=decision_constraints,
    )


def generate_and_run_pipeline(
    scenario_name: str,
    *,
    seed: int = 7,
    planner_mode: str = "heuristic",
    decision_constraints: DecisionConstraints | None = None,
    testbed_profile: str | None = None,
) -> tuple[list[MetricSample], list[ChangeEvent], ScenarioMetadata, PipelineReport]:
    telemetry, events, metadata = generate_scenario(
        scenario_name,
        seed=seed,
        testbed_profile=testbed_profile,
    )
    report = run_pipeline_from_streams(
        telemetry,
        events,
        metadata,
        planner_mode=planner_mode,
        decision_constraints=decision_constraints,
    )
    return telemetry, events, metadata, report


def run_scenario_matrix(
    *,
    seed: int = 7,
    planner_mode: str = "heuristic",
    decision_constraints: DecisionConstraints | None = None,
    testbed_profile: str | None = None,
) -> list[PipelineReport]:
    return [
        run_pipeline_from_streams(
            *generate_scenario(name, seed=seed, testbed_profile=testbed_profile),
            planner_mode=planner_mode,
            decision_constraints=decision_constraints,
        )
        for name in list_scenarios(profile=testbed_profile)
    ]
