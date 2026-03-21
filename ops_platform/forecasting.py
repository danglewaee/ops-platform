from __future__ import annotations

from collections import defaultdict
from statistics import mean

from .schemas import Forecast, Incident, MetricSample


def forecast_services(samples: list[MetricSample], incidents: list[Incident], horizon_minutes: int = 10) -> list[Forecast]:
    if not incidents:
        return []

    series: dict[tuple[str, str], list[MetricSample]] = defaultdict(list)
    for sample in samples:
        series[(sample.service, sample.metric)].append(sample)

    forecasts: list[Forecast] = []

    for incident in incidents:
        target_service = incident.root_cause_candidates[0] if incident.root_cause_candidates else incident.services[0]
        request_projection = _project(series[(target_service, "request_rate")])
        latency_projection = _project(series[(target_service, "p95_latency_ms")])
        queue_projection = _project(series[(target_service, "queue_depth")])
        risk = _risk_level(latency_projection, queue_projection)

        forecasts.append(
            Forecast(
                service=target_service,
                horizon_minutes=horizon_minutes,
                projected_request_rate=round(request_projection, 2),
                projected_p95_latency_ms=round(latency_projection, 2),
                projected_queue_depth=round(queue_projection, 2),
                risk_level=risk,
                rationale=(
                    f"Recent trend suggests {target_service} will reach "
                    f"{latency_projection:.1f} ms p95 and queue depth {queue_projection:.1f} "
                    f"within {horizon_minutes} minutes if no action is taken."
                ),
            )
        )

    return forecasts


def _project(series: list[MetricSample]) -> float:
    if len(series) < 4:
        return series[-1].value if series else 0.0

    recent = sorted(series, key=lambda sample: sample.step)[-4:]
    deltas = [recent[index].value - recent[index - 1].value for index in range(1, len(recent))]
    return recent[-1].value + mean(deltas) * 2


def _risk_level(projected_latency: float, projected_queue: float) -> str:
    if projected_latency >= 150 or projected_queue >= 18:
        return "high"
    if projected_latency >= 115 or projected_queue >= 10:
        return "medium"
    return "low"

