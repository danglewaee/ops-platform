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
