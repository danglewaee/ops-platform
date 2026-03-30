from __future__ import annotations

from collections import defaultdict
from statistics import mean

from .feature_builder import estimate_service_health, latest_metric_values
from .schemas import Forecast, Incident, MetricSample, ServiceHealth


def forecast_services(
    samples: list[MetricSample],
    incidents: list[Incident],
    *,
    service_health: list[ServiceHealth] | None = None,
    horizon_minutes: int = 10,
) -> list[Forecast]:
    if not incidents:
        return []

    series: dict[tuple[str, str], list[MetricSample]] = defaultdict(list)
    for sample in samples:
        series[(sample.service, sample.metric)].append(sample)
    latest_metrics = latest_metric_values(samples)
    service_health_by_service = {item.service: item for item in service_health or []}

    forecasts: list[Forecast] = []

    for incident in incidents:
        target_service = incident.root_cause_candidates[0] if incident.root_cause_candidates else incident.services[0]
        request_projection = _project(series[(target_service, "request_rate")])
        latency_projection = _project(series[(target_service, "p95_latency_ms")])
        queue_projection = _project(series[(target_service, "queue_depth")])
        current_metrics = latest_metrics.get(target_service, {})
        projected_health = estimate_service_health(
            target_service,
            current_metrics,
            {
                **current_metrics,
                "p95_latency_ms": latency_projection,
                "queue_depth": queue_projection,
            },
        )
        current_burn_rate = (
            service_health_by_service[target_service].current_burn_rate
            if target_service in service_health_by_service
            else projected_health.current_burn_rate
        )
        risk = _risk_level(latency_projection, queue_projection, projected_health.projected_burn_rate)
        rationale = (
            f"Recent trend suggests {target_service} will reach "
            f"{latency_projection:.1f} ms p95 and queue depth {queue_projection:.1f} "
            f"within {horizon_minutes} minutes if no action is taken."
        )
        if projected_health.budget_pressure in {"high", "critical"}:
            rationale += (
                f" Estimated SLO burn rises to {projected_health.projected_burn_rate:.2f}x "
                f"through {projected_health.dominant_signal.replace('_', ' ')}."
            )

        forecasts.append(
            Forecast(
                service=target_service,
                horizon_minutes=horizon_minutes,
                projected_request_rate=round(request_projection, 2),
                projected_p95_latency_ms=round(latency_projection, 2),
                projected_queue_depth=round(queue_projection, 2),
                risk_level=risk,
                rationale=rationale,
                current_burn_rate=round(current_burn_rate, 3),
                projected_burn_rate=round(projected_health.projected_burn_rate, 3),
                budget_pressure=projected_health.budget_pressure,
                dominant_slo_signal=projected_health.dominant_signal,
            )
        )

    return forecasts


def _project(series: list[MetricSample]) -> float:
    if len(series) < 4:
        return series[-1].value if series else 0.0

    recent = sorted(series, key=lambda sample: sample.step)[-4:]
    deltas = [recent[index].value - recent[index - 1].value for index in range(1, len(recent))]
    return recent[-1].value + mean(deltas) * 2


def _risk_level(projected_latency: float, projected_queue: float, projected_burn_rate: float) -> str:
    if projected_latency >= 150 or projected_queue >= 18 or projected_burn_rate >= 1.8:
        return "high"
    if projected_latency >= 115 or projected_queue >= 10 or projected_burn_rate >= 1.05:
        return "medium"
    return "low"
