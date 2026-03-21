from __future__ import annotations

import time

from .schemas import EvaluationSummary, Forecast, Incident, Recommendation, ScenarioMetadata


def recommend_actions(
    incidents: list[Incident],
    forecasts: list[Forecast],
) -> tuple[list[Recommendation], float]:
    started = time.perf_counter()
    recommendations: list[Recommendation] = []
    forecast_by_service = {forecast.service: forecast for forecast in forecasts}

    for incident in incidents:
        target = incident.root_cause_candidates[0] if incident.root_cause_candidates else incident.services[0]
        forecast = forecast_by_service.get(target)
        action, rationale, cost_delta, latency_delta, risk_change, confidence = _choose_action(incident, forecast)

        recommendations.append(
            Recommendation(
                action=action,
                target_service=target,
                confidence=confidence,
                rationale=rationale,
                projected_cost_delta_pct=cost_delta,
                projected_p95_delta_ms=latency_delta,
                expected_risk_change=risk_change,
                trigger_incident_id=incident.incident_id,
            )
        )

    latency_ms = (time.perf_counter() - started) * 1000
    return recommendations, latency_ms


def evaluate_recommendations(
    metadata: ScenarioMetadata,
    anomalies_count: int,
    incidents: list[Incident],
    recommendations: list[Recommendation],
    decision_latency_ms: float,
    baseline_recommendations: dict[str, list[Recommendation]] | None = None,
) -> EvaluationSummary:
    top2_hit = any(metadata.root_cause in incident.root_cause_candidates[:2] for incident in incidents)
    action_match = any(recommendation.action == metadata.expected_action for recommendation in recommendations)
    average_cost_delta = sum(rec.projected_cost_delta_pct for rec in recommendations) / max(len(recommendations), 1)
    average_p95_delta = sum(rec.projected_p95_delta_ms for rec in recommendations) / max(len(recommendations), 1)
    alert_reduction = max(0.0, 1 - (len(incidents) / anomalies_count)) if anomalies_count else 0.0
    comparisons = _build_baseline_comparisons(metadata, baseline_recommendations or {})

    return EvaluationSummary(
        alert_reduction_pct=round(alert_reduction * 100, 2),
        incident_count=len(incidents),
        anomaly_count=anomalies_count,
        top2_root_cause_hit=top2_hit,
        recommended_action_match=action_match,
        average_cost_delta_pct=round(average_cost_delta, 2),
        average_p95_delta_ms=round(average_p95_delta, 2),
        decision_latency_ms=round(decision_latency_ms, 3),
        baseline_comparisons=comparisons,
    )


def build_baseline_recommendations(
    incidents: list[Incident],
    forecasts: list[Forecast],
) -> dict[str, list[Recommendation]]:
    forecast_by_service = {forecast.service: forecast for forecast in forecasts}
    no_action: list[Recommendation] = []
    threshold_autoscaling: list[Recommendation] = []

    for incident in incidents:
        target = incident.root_cause_candidates[0] if incident.root_cause_candidates else incident.services[0]
        forecast = forecast_by_service.get(target)

        no_action.append(_make_no_action(target, incident, forecast))
        threshold_autoscaling.append(_make_threshold_policy(target, incident, forecast))

    return {
        "no_action": no_action,
        "threshold_autoscaling": threshold_autoscaling,
    }


def _choose_action(
    incident: Incident,
    forecast: Forecast | None,
) -> tuple[str, str, float, float, str, float]:
    if incident.trigger_event and "deploy" in incident.trigger_event.lower():
        return (
            "rollback_candidate",
            "A recent change event lines up with the incident window, so rollback is the safest shadow-mode recommendation.",
            -2.0,
            -28.0,
            "reduce instability quickly",
            0.86,
        )

    if incident.severity in {"high", "critical"} and "auth" in incident.root_cause_candidates:
        return (
            "reroute_traffic",
            "Auth degradation is likely contaminating downstream requests, so rerouting buys time while keeping user-facing services available.",
            3.0,
            -14.0,
            "reduce user impact",
            0.76,
        )

    if forecast and forecast.risk_level == "high":
        if forecast.projected_queue_depth >= 18:
            return (
                "increase_consumers",
                "Projected queue growth is steep, so increasing consumers is safer than waiting for backlog to compound.",
                6.0,
                -22.0,
                "reduce queue pressure",
                0.82,
            )
        return (
            "scale_out",
            "Short-horizon demand and latency projections indicate the service will miss targets without additional capacity.",
            9.5,
            -25.0,
            "stabilize latency",
            0.81,
        )

    return (
        "hold_steady",
        "Current signal quality suggests waiting is safer than taking a costly action on a transient anomaly cluster.",
        0.0,
        -4.0,
        "avoid unnecessary action churn",
        0.63,
    )


def _make_no_action(target: str, incident: Incident, forecast: Forecast | None) -> Recommendation:
    if forecast and forecast.risk_level == "high":
        latency_penalty = 32.0
    elif forecast and forecast.risk_level == "medium":
        latency_penalty = 18.0
    else:
        latency_penalty = 10.0

    return Recommendation(
        action="hold_steady",
        target_service=target,
        confidence=0.99,
        rationale="No action baseline leaves the system on its current path and absorbs the projected degradation.",
        projected_cost_delta_pct=0.0,
        projected_p95_delta_ms=latency_penalty,
        expected_risk_change="accept current risk",
        trigger_incident_id=incident.incident_id,
    )


def _make_threshold_policy(target: str, incident: Incident, forecast: Forecast | None) -> Recommendation:
    if forecast and (forecast.projected_queue_depth >= 18 or forecast.projected_p95_latency_ms >= 180):
        return Recommendation(
            action="scale_out",
            target_service=target,
            confidence=0.72,
            rationale="Reactive threshold policy scales once projected latency or backlog crosses a hard limit.",
            projected_cost_delta_pct=12.0,
            projected_p95_delta_ms=-12.0,
            expected_risk_change="reduce acute latency",
            trigger_incident_id=incident.incident_id,
        )

    if forecast and forecast.projected_p95_latency_ms >= 125:
        return Recommendation(
            action="scale_out",
            target_service=target,
            confidence=0.68,
            rationale="Threshold autoscaling adds capacity after latency enters a warning band.",
            projected_cost_delta_pct=8.0,
            projected_p95_delta_ms=-6.0,
            expected_risk_change="partially reduce latency",
            trigger_incident_id=incident.incident_id,
        )

    return Recommendation(
        action="hold_steady",
        target_service=target,
        confidence=0.74,
        rationale="Threshold autoscaling takes no action until latency or queue pressure crosses a fixed threshold.",
        projected_cost_delta_pct=0.0,
        projected_p95_delta_ms=8.0,
        expected_risk_change="defer response",
        trigger_incident_id=incident.incident_id,
    )


def _build_baseline_comparisons(
    metadata: ScenarioMetadata,
    baseline_recommendations: dict[str, list[Recommendation]],
) -> list[dict[str, object]]:
    comparisons: list[dict[str, object]] = []
    for name, recommendations in baseline_recommendations.items():
        if not recommendations:
            continue

        action_match = any(recommendation.action == metadata.expected_action for recommendation in recommendations)
        average_cost_delta = sum(rec.projected_cost_delta_pct for rec in recommendations) / len(recommendations)
        average_p95_delta = sum(rec.projected_p95_delta_ms for rec in recommendations) / len(recommendations)
        primary = recommendations[0]
        comparisons.append(
            {
                "policy": name,
                "action": primary.action,
                "target_service": primary.target_service,
                "recommended_action_match": action_match,
                "average_cost_delta_pct": round(average_cost_delta, 2),
                "average_p95_delta_ms": round(average_p95_delta, 2),
            }
        )

    return comparisons
