from __future__ import annotations

from collections import defaultdict

from .schemas import Forecast, Incident, MetricSample, ServiceHealth
from .simulator import BASELINES

SLO_TARGETS = {
    service: {
        "latency_slo_ms": round(values["p95_latency_ms"] * 1.35, 1),
        "error_rate_slo_pct": round(max(values["error_rate_pct"] * 3.5, 0.75), 2),
        "queue_depth_slo": round(max(values["queue_depth"] * 2.5, 6.0), 1),
    }
    for service, values in BASELINES.items()
}

FALLBACK_SLO_TARGETS = {
    "latency_slo_ms": 125.0,
    "error_rate_slo_pct": 1.0,
    "queue_depth_slo": 8.0,
}


def build_service_health(
    samples: list[MetricSample],
    incidents: list[Incident],
    forecasts: list[Forecast] | None = None,
) -> list[ServiceHealth]:
    if not incidents:
        return []

    latest_metrics = latest_metric_values(samples)
    forecast_by_service = {forecast.service: forecast for forecast in forecasts or []}
    target_services: list[str] = []
    seen: set[str] = set()

    for incident in incidents:
        service = incident.root_cause_candidates[0] if incident.root_cause_candidates else incident.services[0]
        if service not in seen:
            seen.add(service)
            target_services.append(service)

    health: list[ServiceHealth] = []
    for service in target_services:
        current_metrics = latest_metrics.get(service, {})
        forecast = forecast_by_service.get(service)
        projected_metrics = dict(current_metrics)
        if forecast is not None:
            projected_metrics["p95_latency_ms"] = forecast.projected_p95_latency_ms
            projected_metrics["queue_depth"] = forecast.projected_queue_depth

        health.append(estimate_service_health(service, current_metrics, projected_metrics))

    return sorted(health, key=lambda item: (item.projected_burn_rate, item.current_burn_rate), reverse=True)


def latest_metric_values(samples: list[MetricSample]) -> dict[str, dict[str, float]]:
    latest: dict[str, dict[str, float]] = defaultdict(dict)
    for sample in sorted(samples, key=lambda item: (item.step, item.timestamp, item.service, item.metric)):
        latest[sample.service][sample.metric] = sample.value
    return dict(latest)


def estimate_service_health(
    service: str,
    current_metrics: dict[str, float],
    projected_metrics: dict[str, float] | None = None,
) -> ServiceHealth:
    targets = _resolve_slo_targets(service, current_metrics)
    projected_metrics = projected_metrics or current_metrics

    current_components = _burn_components(current_metrics, targets)
    projected_components = _burn_components(projected_metrics, targets)
    current_signal, current_burn_rate = _dominant_signal(current_components)
    projected_signal, projected_burn_rate = _dominant_signal(projected_components)
    budget_pressure = _budget_pressure(projected_burn_rate)
    remaining_budget = _estimated_budget_remaining(projected_burn_rate)
    dominant_signal = projected_signal if projected_burn_rate >= current_burn_rate else current_signal

    return ServiceHealth(
        service=service,
        latency_slo_ms=round(targets["latency_slo_ms"], 2),
        error_rate_slo_pct=round(targets["error_rate_slo_pct"], 2),
        queue_depth_slo=round(targets["queue_depth_slo"], 2),
        current_latency_ms=round(current_metrics.get("p95_latency_ms", 0.0), 2),
        current_error_rate_pct=round(current_metrics.get("error_rate_pct", 0.0), 2),
        current_queue_depth=round(current_metrics.get("queue_depth", 0.0), 2),
        current_burn_rate=round(current_burn_rate, 3),
        projected_burn_rate=round(projected_burn_rate, 3),
        estimated_error_budget_remaining_pct=round(remaining_budget, 2),
        budget_pressure=budget_pressure,
        dominant_signal=dominant_signal,
        rationale=_health_rationale(
            service=service,
            current_burn_rate=current_burn_rate,
            projected_burn_rate=projected_burn_rate,
            dominant_signal=dominant_signal,
            remaining_budget=remaining_budget,
        ),
    )


def _resolve_slo_targets(service: str, current_metrics: dict[str, float]) -> dict[str, float]:
    targets = dict(SLO_TARGETS.get(service, FALLBACK_SLO_TARGETS))
    if service in SLO_TARGETS:
        return targets

    latency = max(current_metrics.get("p95_latency_ms", 0.0), FALLBACK_SLO_TARGETS["latency_slo_ms"])
    error_rate = max(current_metrics.get("error_rate_pct", 0.0), FALLBACK_SLO_TARGETS["error_rate_slo_pct"])
    queue_depth = max(current_metrics.get("queue_depth", 0.0), FALLBACK_SLO_TARGETS["queue_depth_slo"])
    return {
        "latency_slo_ms": max(FALLBACK_SLO_TARGETS["latency_slo_ms"], latency * 1.15),
        "error_rate_slo_pct": max(FALLBACK_SLO_TARGETS["error_rate_slo_pct"], error_rate * 1.5),
        "queue_depth_slo": max(FALLBACK_SLO_TARGETS["queue_depth_slo"], queue_depth * 1.4),
    }


def _burn_components(metrics: dict[str, float], targets: dict[str, float]) -> dict[str, float]:
    return {
        "p95_latency_ms": _safe_burn(metrics.get("p95_latency_ms", 0.0), targets["latency_slo_ms"]),
        "error_rate_pct": _safe_burn(metrics.get("error_rate_pct", 0.0), targets["error_rate_slo_pct"]),
        "queue_depth": _safe_burn(metrics.get("queue_depth", 0.0), targets["queue_depth_slo"]),
    }


def _dominant_signal(components: dict[str, float]) -> tuple[str, float]:
    dominant_signal, burn_rate = max(components.items(), key=lambda item: item[1])
    return dominant_signal, burn_rate


def _safe_burn(value: float, target: float) -> float:
    if target <= 0:
        return 0.0
    return max(0.0, value / target)


def _budget_pressure(projected_burn_rate: float) -> str:
    if projected_burn_rate >= 1.8:
        return "critical"
    if projected_burn_rate >= 1.25:
        return "high"
    if projected_burn_rate >= 0.9:
        return "medium"
    return "low"


def _estimated_budget_remaining(projected_burn_rate: float) -> float:
    if projected_burn_rate <= 1.0:
        return 100.0
    return max(0.0, 100.0 - (projected_burn_rate - 1.0) * 55.0)


def _health_rationale(
    *,
    service: str,
    current_burn_rate: float,
    projected_burn_rate: float,
    dominant_signal: str,
    remaining_budget: float,
) -> str:
    signal_name = dominant_signal.replace("_", " ")
    if projected_burn_rate <= 1.0:
        return (
            f"{service} remains within its estimated SLO envelope; "
            f"{signal_name} is the hottest signal but projected burn stays at {projected_burn_rate:.2f}x."
        )
    return (
        f"{service} is burning budget through {signal_name}; "
        f"current burn is {current_burn_rate:.2f}x, projected burn is {projected_burn_rate:.2f}x, "
        f"with an estimated {remaining_budget:.1f}% budget remaining."
    )
