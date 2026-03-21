from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from random import Random

from .scenarios import SCENARIOS
from .schemas import ChangeEvent, MetricSample, ScenarioMetadata

SERVICES = ("gateway", "auth", "payments", "worker", "router", "db")
METRICS = ("request_rate", "p95_latency_ms", "error_rate_pct", "queue_depth", "cpu_pct")

BASELINES: dict[str, dict[str, float]] = {
    "gateway": {"request_rate": 1800, "p95_latency_ms": 95, "error_rate_pct": 0.4, "queue_depth": 4, "cpu_pct": 48},
    "auth": {"request_rate": 1200, "p95_latency_ms": 88, "error_rate_pct": 0.3, "queue_depth": 2, "cpu_pct": 42},
    "payments": {"request_rate": 900, "p95_latency_ms": 92, "error_rate_pct": 0.5, "queue_depth": 3, "cpu_pct": 44},
    "worker": {"request_rate": 700, "p95_latency_ms": 110, "error_rate_pct": 0.2, "queue_depth": 6, "cpu_pct": 54},
    "router": {"request_rate": 950, "p95_latency_ms": 84, "error_rate_pct": 0.2, "queue_depth": 1, "cpu_pct": 38},
    "db": {"request_rate": 2500, "p95_latency_ms": 55, "error_rate_pct": 0.1, "queue_depth": 1, "cpu_pct": 58},
}


def generate_scenario(
    scenario_name: str,
    *,
    steps: int = 24,
    seed: int = 7,
) -> tuple[list[MetricSample], list[ChangeEvent], ScenarioMetadata]:
    if scenario_name not in SCENARIOS:
        available = ", ".join(sorted(SCENARIOS))
        raise ValueError(f"Unknown scenario '{scenario_name}'. Available: {available}")

    rng = Random(seed)
    metadata = SCENARIOS[scenario_name]
    start = datetime(2026, 3, 20, 9, 0, 0)
    telemetry: list[MetricSample] = []
    events: list[ChangeEvent] = []

    service_state: dict[str, dict[str, float]] = {
        service: values.copy() for service, values in BASELINES.items()
    }

    for step in range(steps):
        current_time = start + timedelta(minutes=step)
        _apply_background_noise(service_state, rng)
        _apply_scenario_effects(scenario_name, step, service_state, events, current_time)

        for service in SERVICES:
            for metric in METRICS:
                telemetry.append(
                    MetricSample(
                        timestamp=current_time,
                        step=step,
                        service=service,
                        metric=metric,
                        value=round(service_state[service][metric], 3),
                        unit=_metric_unit(metric),
                    )
                )

    return telemetry, _dedupe_events(events), metadata


def _apply_background_noise(state: dict[str, dict[str, float]], rng: Random) -> None:
    for metrics in state.values():
        metrics["request_rate"] *= 1 + rng.uniform(-0.015, 0.015)
        metrics["p95_latency_ms"] *= 1 + rng.uniform(-0.02, 0.02)
        metrics["error_rate_pct"] = max(0.05, metrics["error_rate_pct"] + rng.uniform(-0.04, 0.04))
        metrics["queue_depth"] = max(0, metrics["queue_depth"] + rng.uniform(-0.5, 0.8))
        metrics["cpu_pct"] = min(95, max(8, metrics["cpu_pct"] + rng.uniform(-1.2, 1.4)))


def _apply_scenario_effects(
    scenario_name: str,
    step: int,
    state: dict[str, dict[str, float]],
    events: list[ChangeEvent],
    timestamp: datetime,
) -> None:
    if scenario_name == "traffic_spike" and step >= 10:
        state["gateway"]["request_rate"] *= 1.14
        state["gateway"]["p95_latency_ms"] *= 1.09
        state["payments"]["queue_depth"] += 4
        state["worker"]["queue_depth"] += 7
        state["worker"]["cpu_pct"] += 4
        if step == 10:
            events.append(ChangeEvent(timestamp, step, "gateway", "traffic_spike", "Sudden demand surge at edge ingress"))

    if scenario_name == "bad_deploy" and step >= 9:
        state["payments"]["error_rate_pct"] += 1.6
        state["payments"]["p95_latency_ms"] *= 1.12
        state["gateway"]["error_rate_pct"] += 0.3
        state["worker"]["queue_depth"] += 3
        if step == 9:
            events.append(ChangeEvent(timestamp, step, "payments", "deploy", "New payments deployment introduced regressions"))

    if scenario_name == "queue_backlog" and step >= 11:
        state["worker"]["queue_depth"] += 8
        state["worker"]["p95_latency_ms"] *= 1.08
        state["payments"]["p95_latency_ms"] *= 1.05
        state["payments"]["queue_depth"] += 2
        if step == 11:
            events.append(ChangeEvent(timestamp, step, "worker", "consumer_lag", "Consumer lag causes backlog growth"))

    if scenario_name == "memory_leak" and step >= 8:
        state["auth"]["cpu_pct"] += 3
        state["auth"]["p95_latency_ms"] *= 1.06
        state["auth"]["error_rate_pct"] += 0.14
        if step >= 14:
            state["gateway"]["p95_latency_ms"] *= 1.04
        if step == 8:
            events.append(ChangeEvent(timestamp, step, "auth", "degradation", "Auth service begins degrading under memory pressure"))


def latest_metric_view(samples: list[MetricSample]) -> dict[str, dict[str, float]]:
    latest: dict[str, dict[str, float]] = defaultdict(dict)
    for sample in samples:
        latest[sample.service][sample.metric] = sample.value
    return dict(latest)


def _dedupe_events(events: list[ChangeEvent]) -> list[ChangeEvent]:
    seen: set[tuple[int, str, str]] = set()
    unique: list[ChangeEvent] = []
    for event in events:
        key = (event.step, event.service, event.event_type)
        if key not in seen:
            seen.add(key)
            unique.append(event)
    return unique


def _metric_unit(metric: str) -> str:
    return {
        "request_rate": "req/s",
        "p95_latency_ms": "ms",
        "error_rate_pct": "%",
        "queue_depth": "messages",
        "cpu_pct": "%",
    }[metric]

