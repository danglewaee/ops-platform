from __future__ import annotations

import time

from .planner import ActionCandidate, select_recommendations
from .schemas import (
    DecisionConstraints,
    EvaluationSummary,
    Forecast,
    Incident,
    Recommendation,
    ScenarioMetadata,
    ServiceHealth,
)


def recommend_actions(
    incidents: list[Incident],
    forecasts: list[Forecast],
    *,
    service_health: list[ServiceHealth] | None = None,
    planner_mode: str = "heuristic",
    constraints: DecisionConstraints | None = None,
) -> tuple[list[Recommendation], float, str]:
    started = time.perf_counter()
    forecast_by_service = {forecast.service: forecast for forecast in forecasts}
    service_health_by_service = {item.service: item for item in service_health or []}
    candidates_by_incident: list[list[ActionCandidate]] = []

    for incident in incidents:
        target = incident.root_cause_candidates[0] if incident.root_cause_candidates else incident.services[0]
        forecast = forecast_by_service.get(target)
        health = service_health_by_service.get(target)
        candidates_by_incident.append(_candidate_recommendations(incident, target, forecast, health))

    recommendations, actual_planner_mode = select_recommendations(
        candidates_by_incident,
        planner_mode=planner_mode,
        constraints=constraints,
    )
    latency_ms = (time.perf_counter() - started) * 1000
    return recommendations, latency_ms, actual_planner_mode


def evaluate_recommendations(
    metadata: ScenarioMetadata,
    anomalies_count: int,
    incidents: list[Incident],
    recommendations: list[Recommendation],
    decision_latency_ms: float,
    baseline_recommendations: dict[str, list[Recommendation]] | None = None,
    planner_mode: str = "heuristic",
    trace_id: str | None = None,
) -> EvaluationSummary:
    baseline_recommendations = baseline_recommendations or {}
    ground_truth_available = bool(metadata.root_cause and metadata.expected_action)
    top2_hit = (
        any(metadata.root_cause in incident.root_cause_candidates[:2] for incident in incidents)
        if ground_truth_available
        else None
    )
    action_match = (
        any(recommendation.action == metadata.expected_action for recommendation in recommendations)
        if ground_truth_available
        else None
    )
    average_cost_delta = sum(rec.projected_cost_delta_pct for rec in recommendations) / max(len(recommendations), 1)
    average_p95_delta = sum(rec.projected_p95_delta_ms for rec in recommendations) / max(len(recommendations), 1)
    alert_reduction = max(0.0, 1 - (len(incidents) / anomalies_count)) if anomalies_count else 0.0
    comparisons = _build_baseline_comparisons(metadata, baseline_recommendations, ground_truth_available)
    baseline_scores = [comparison["score"] for comparison in comparisons]
    our_score = _policy_score(recommendations)
    baseline_win_rate = (
        sum(1 for score in baseline_scores if our_score >= score) / len(baseline_scores) * 100
        if baseline_scores
        else 0.0
    )
    no_action = next((item for item in comparisons if item["policy"] == "no_action"), None)
    threshold_policy = next((item for item in comparisons if item["policy"] == "threshold_autoscaling"), None)

    return EvaluationSummary(
        alert_reduction_pct=round(alert_reduction * 100, 2),
        incident_count=len(incidents),
        anomaly_count=anomalies_count,
        top2_root_cause_hit=top2_hit,
        recommended_action_match=action_match,
        average_cost_delta_pct=round(average_cost_delta, 2),
        average_p95_delta_ms=round(average_p95_delta, 2),
        decision_latency_ms=round(decision_latency_ms, 3),
        evaluation_mode="ground_truth" if ground_truth_available else "shadow_only",
        latency_protection_pct=round(_latency_protection_pct(average_p95_delta, no_action), 2),
        avoided_overprovisioning_pct=round(_avoided_overprovisioning_pct(average_cost_delta, threshold_policy), 2),
        baseline_win_rate_pct=round(baseline_win_rate, 2),
        action_stability_pct=round(_action_stability_pct(recommendations), 2),
        planner_mode=planner_mode,
        trace_id=trace_id,
        baseline_comparisons=comparisons,
    )


def build_baseline_recommendations(
    incidents: list[Incident],
    forecasts: list[Forecast],
) -> dict[str, list[Recommendation]]:
    forecast_by_service = {forecast.service: forecast for forecast in forecasts}
    no_action: list[Recommendation] = []
    threshold_autoscaling: list[Recommendation] = []
    naive_reroute: list[Recommendation] = []

    for incident in incidents:
        target = incident.root_cause_candidates[0] if incident.root_cause_candidates else incident.services[0]
        forecast = forecast_by_service.get(target)

        no_action.append(_make_no_action(target, incident, forecast))
        threshold_autoscaling.append(_make_threshold_policy(target, incident, forecast))
        naive_reroute.append(_make_naive_reroute_policy(target, incident, forecast))

    return {
        "no_action": no_action,
        "threshold_autoscaling": threshold_autoscaling,
        "naive_reroute": naive_reroute,
    }


def _choose_action(
    incident: Incident,
    forecast: Forecast | None,
    health: ServiceHealth | None,
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

    if health and health.projected_burn_rate >= 1.75:
        if health.dominant_signal == "queue_depth":
            return (
                "increase_consumers",
                "Projected queue budget burn is critical, so adding consumers is safer than waiting for backlog to spill into user latency.",
                6.0,
                -24.0,
                "drain queue pressure",
                0.84,
            )
        return (
            "scale_out",
            "Projected SLO burn is critical, so adding bounded capacity is the safest shadow-mode action before the budget is exhausted.",
            8.5,
            -24.0,
            "protect latency budget",
            0.83,
        )

    if forecast and forecast.budget_pressure in {"high", "critical"}:
        if forecast.dominant_slo_signal == "queue_depth" and forecast.projected_queue_depth >= 12:
            return (
                "increase_consumers",
                "Queue depth is the dominant SLO pressure signal, so increasing consumers is safer than generic scaling.",
                6.0,
                -20.0,
                "reduce queue pressure",
                0.81,
            )
        return (
            "scale_out",
            "Projected burn rate is climbing toward the SLO limit, so adding bounded capacity is safer than waiting for reactive thresholds.",
            8.0,
            -18.0,
            "stabilize latency budget",
            0.79,
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


def _candidate_recommendations(
    incident: Incident,
    target: str,
    forecast: Forecast | None,
    health: ServiceHealth | None,
) -> list[ActionCandidate]:
    preferred_action, rationale, cost_delta, latency_delta, risk_change, confidence = _choose_action(incident, forecast, health)
    candidates: dict[str, Recommendation] = {
        preferred_action: Recommendation(
            action=preferred_action,
            target_service=target,
            confidence=confidence,
            rationale=rationale,
            projected_cost_delta_pct=cost_delta,
            projected_p95_delta_ms=latency_delta,
            expected_risk_change=risk_change,
            trigger_incident_id=incident.incident_id,
        ),
        "hold_steady": _make_hold_action(target, incident, forecast),
    }

    if incident.trigger_event and "deploy" in incident.trigger_event.lower():
        candidates["rollback_candidate"] = _make_rollback_action(target, incident)

    if forecast and forecast.risk_level in {"medium", "high"}:
        candidates["scale_out"] = _make_scale_out_action(target, incident, forecast)

    if forecast and (forecast.projected_queue_depth >= 10 or forecast.risk_level == "high"):
        candidates["increase_consumers"] = _make_increase_consumers_action(target, incident, forecast)

    if incident.severity in {"high", "critical"} or (forecast and forecast.projected_p95_latency_ms >= 125):
        candidates["reroute_traffic"] = _make_reroute_action(target, incident, forecast)

    planned_candidates: list[ActionCandidate] = []
    for action, recommendation in candidates.items():
        bonus = 100.0 if action == preferred_action else 0.0
        score = _recommendation_score(recommendation) + _candidate_fit_bonus(action, incident, forecast, health) + bonus
        planned_candidates.append(ActionCandidate(recommendation=recommendation, score=round(score, 3)))

    return planned_candidates


def _make_hold_action(target: str, incident: Incident, forecast: Forecast | None) -> Recommendation:
    penalty = 10.0
    if forecast and forecast.risk_level == "high":
        penalty = 18.0
    if forecast and forecast.risk_level == "high" and forecast.projected_p95_latency_ms >= 150:
        penalty = 32.0

    return Recommendation(
        action="hold_steady",
        target_service=target,
        confidence=0.63,
        rationale="Waiting is safer when the signal still looks noisy or the intervention budget is constrained.",
        projected_cost_delta_pct=0.0,
        projected_p95_delta_ms=penalty,
        expected_risk_change="defer action",
        trigger_incident_id=incident.incident_id,
    )


def _make_scale_out_action(target: str, incident: Incident, forecast: Forecast | None) -> Recommendation:
    latency_delta = -12.0
    cost_delta = 8.0
    confidence = 0.72
    if forecast and forecast.risk_level == "high":
        latency_delta = -25.0
        cost_delta = 9.5
        confidence = 0.81

    return Recommendation(
        action="scale_out",
        target_service=target,
        confidence=confidence,
        rationale="Capacity can absorb the projected load before latency degrades further.",
        projected_cost_delta_pct=cost_delta,
        projected_p95_delta_ms=latency_delta,
        expected_risk_change="stabilize latency",
        trigger_incident_id=incident.incident_id,
    )


def _make_increase_consumers_action(target: str, incident: Incident, forecast: Forecast | None) -> Recommendation:
    latency_delta = -12.0
    if forecast and forecast.projected_queue_depth >= 18:
        latency_delta = -22.0

    return Recommendation(
        action="increase_consumers",
        target_service=target,
        confidence=0.79,
        rationale="Adding workers drains backlog faster than waiting for queue depth to spill into latency.",
        projected_cost_delta_pct=6.0,
        projected_p95_delta_ms=latency_delta,
        expected_risk_change="reduce queue pressure",
        trigger_incident_id=incident.incident_id,
    )


def _make_reroute_action(target: str, incident: Incident, forecast: Forecast | None) -> Recommendation:
    latency_delta = -8.0
    confidence = 0.68
    if incident.severity in {"high", "critical"} and "auth" in incident.root_cause_candidates:
        latency_delta = -14.0
        confidence = 0.76

    return Recommendation(
        action="reroute_traffic",
        target_service=target,
        confidence=confidence,
        rationale="Rerouting traffic buys time while keeping user-facing paths available.",
        projected_cost_delta_pct=3.0,
        projected_p95_delta_ms=latency_delta,
        expected_risk_change="reduce user impact",
        trigger_incident_id=incident.incident_id,
    )


def _make_rollback_action(target: str, incident: Incident) -> Recommendation:
    return Recommendation(
        action="rollback_candidate",
        target_service=target,
        confidence=0.86,
        rationale="A recent deploy aligns with the incident window, so rollback is the safest intervention candidate.",
        projected_cost_delta_pct=-2.0,
        projected_p95_delta_ms=-28.0,
        expected_risk_change="reduce instability quickly",
        trigger_incident_id=incident.incident_id,
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


def _make_naive_reroute_policy(target: str, incident: Incident, forecast: Forecast | None) -> Recommendation:
    if incident.severity in {"high", "critical"} or (forecast and forecast.projected_p95_latency_ms >= 135):
        return Recommendation(
            action="reroute_traffic",
            target_service=target,
            confidence=0.58,
            rationale="Naive routing baseline shifts traffic away from the hottest service whenever latency looks elevated.",
            projected_cost_delta_pct=5.5,
            projected_p95_delta_ms=-8.0,
            expected_risk_change="reduce user-facing pressure",
            trigger_incident_id=incident.incident_id,
        )

    return Recommendation(
        action="hold_steady",
        target_service=target,
        confidence=0.61,
        rationale="Naive routing baseline only reroutes under obvious latency stress and otherwise waits.",
        projected_cost_delta_pct=0.0,
        projected_p95_delta_ms=6.0,
        expected_risk_change="limited response",
        trigger_incident_id=incident.incident_id,
    )


def _build_baseline_comparisons(
    metadata: ScenarioMetadata,
    baseline_recommendations: dict[str, list[Recommendation]],
    ground_truth_available: bool,
) -> list[dict[str, object]]:
    comparisons: list[dict[str, object]] = []
    for name, recommendations in baseline_recommendations.items():
        if not recommendations:
            continue

        action_match = (
            any(recommendation.action == metadata.expected_action for recommendation in recommendations)
            if ground_truth_available
            else None
        )
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
                "score": round(_policy_score(recommendations), 2),
            }
        )

    return comparisons


def _policy_score(recommendations: list[Recommendation]) -> float:
    if not recommendations:
        return 0.0

    score = 0.0
    for recommendation in recommendations:
        latency_penalty = max(recommendation.projected_p95_delta_ms, 0.0)
        latency_reward = abs(min(recommendation.projected_p95_delta_ms, 0.0)) * 0.35
        cost_penalty = max(recommendation.projected_cost_delta_pct, 0.0) * 1.25
        score += latency_reward - latency_penalty - cost_penalty
    return round(score / len(recommendations), 3)


def _recommendation_score(recommendation: Recommendation) -> float:
    return _policy_score([recommendation])


def _candidate_fit_bonus(
    action: str,
    incident: Incident,
    forecast: Forecast | None,
    health: ServiceHealth | None,
) -> float:
    if action == "rollback_candidate" and incident.trigger_event and "deploy" in incident.trigger_event.lower():
        return 12.0
    if action == "reroute_traffic" and incident.severity in {"high", "critical"}:
        return 6.0
    if action == "increase_consumers" and forecast and forecast.projected_queue_depth >= 18:
        return 8.0
    if action == "scale_out" and forecast and forecast.risk_level == "high":
        return 7.0
    if (
        action == "increase_consumers"
        and health
        and health.projected_burn_rate >= 1.25
        and health.dominant_signal == "queue_depth"
    ):
        return 9.0
    if (
        action == "scale_out"
        and health
        and health.projected_burn_rate >= 1.25
        and health.dominant_signal != "queue_depth"
    ):
        return 8.0
    if action == "hold_steady" and (not forecast or forecast.risk_level == "low") and (
        not health or health.budget_pressure == "low"
    ):
        return 5.0
    return 0.0


def _latency_protection_pct(
    average_p95_delta: float,
    no_action: dict[str, object] | None,
) -> float:
    if not no_action:
        return 0.0

    baseline_penalty = max(float(no_action["average_p95_delta_ms"]), 0.0)
    ours_penalty = max(average_p95_delta, 0.0)
    if baseline_penalty <= 0:
        return 0.0

    recovered = max(0.0, baseline_penalty - ours_penalty)
    return recovered / baseline_penalty * 100


def _avoided_overprovisioning_pct(
    average_cost_delta: float,
    threshold_policy: dict[str, object] | None,
) -> float:
    if not threshold_policy:
        return 0.0

    threshold_cost = max(float(threshold_policy["average_cost_delta_pct"]), 0.0)
    our_cost = max(average_cost_delta, 0.0)
    if threshold_cost <= 0:
        return 0.0

    saved = max(0.0, threshold_cost - our_cost)
    return saved / threshold_cost * 100


def _action_stability_pct(recommendations: list[Recommendation]) -> float:
    if len(recommendations) <= 1:
        return 100.0

    action_changes = sum(
        1
        for index in range(1, len(recommendations))
        if recommendations[index].action != recommendations[index - 1].action
    )
    return max(0.0, (1 - action_changes / (len(recommendations) - 1)) * 100)
