from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from random import Random

from .scenarios import get_scenario_metadata, resolve_scenario_profile
from .schemas import ChangeEvent, MetricSample, ScenarioMetadata
from .testbed import ALL_BASELINES, resolve_testbed_profile

METRICS = ("request_rate", "p95_latency_ms", "error_rate_pct", "queue_depth", "cpu_pct")
BASELINES = ALL_BASELINES


def generate_scenario(
    scenario_name: str,
    *,
    steps: int = 24,
    seed: int = 7,
    testbed_profile: str | None = None,
) -> tuple[list[MetricSample], list[ChangeEvent], ScenarioMetadata]:
    metadata = get_scenario_metadata(scenario_name)
    profile_name = testbed_profile or resolve_scenario_profile(scenario_name)
    if metadata.testbed_profile != profile_name:
        raise ValueError(
            f"Scenario '{scenario_name}' belongs to profile '{metadata.testbed_profile}', not '{profile_name}'."
        )

    profile = resolve_testbed_profile(profile_name)
    rng = Random(seed)
    start = datetime(2026, 3, 20, 9, 0, 0)
    telemetry: list[MetricSample] = []
    events: list[ChangeEvent] = []

    service_state: dict[str, dict[str, float]] = {
        service: values.copy() for service, values in profile.baselines.items()
    }

    for step in range(steps):
        current_time = start + timedelta(minutes=step)
        _apply_background_noise(service_state, rng)
        _apply_scenario_effects(scenario_name, step, service_state, events, current_time)

        for service in profile.services:
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
        metrics["queue_depth"] = max(0.0, metrics["queue_depth"] + rng.uniform(-0.5, 0.8))
        metrics["cpu_pct"] = min(95.0, max(8.0, metrics["cpu_pct"] + rng.uniform(-1.2, 1.4)))


def _apply_scenario_effects(
    scenario_name: str,
    step: int,
    state: dict[str, dict[str, float]],
    events: list[ChangeEvent],
    timestamp: datetime,
) -> None:
    if scenario_name == "traffic_spike":
        if step >= 10:
            state["gateway"]["request_rate"] *= 1.14
            state["gateway"]["p95_latency_ms"] *= 1.09
            state["payments"]["queue_depth"] += 4
            state["worker"]["queue_depth"] += 7
            state["worker"]["cpu_pct"] += 4
            if step == 10:
                events.append(ChangeEvent(timestamp, step, "gateway", "traffic_spike", "Sudden demand surge at edge ingress"))
        return

    if scenario_name == "bad_deploy":
        if step >= 9:
            state["payments"]["error_rate_pct"] += 1.6
            state["payments"]["p95_latency_ms"] *= 1.12
            state["gateway"]["error_rate_pct"] += 0.3
            state["worker"]["queue_depth"] += 3
            if step == 9:
                events.append(ChangeEvent(timestamp, step, "payments", "deploy", "New payments deployment introduced regressions"))
        return

    if scenario_name == "queue_backlog":
        if step >= 11:
            state["worker"]["queue_depth"] += 8
            state["worker"]["p95_latency_ms"] *= 1.08
            state["payments"]["p95_latency_ms"] *= 1.05
            state["payments"]["queue_depth"] += 2
            if step == 11:
                events.append(ChangeEvent(timestamp, step, "worker", "consumer_lag", "Consumer lag causes backlog growth"))
        return

    if scenario_name == "memory_leak":
        if step >= 8:
            state["auth"]["cpu_pct"] += 3
            state["auth"]["p95_latency_ms"] *= 1.06
            state["auth"]["error_rate_pct"] += 0.14
            if step >= 14:
                state["gateway"]["p95_latency_ms"] *= 1.04
            if step == 8:
                events.append(ChangeEvent(timestamp, step, "auth", "degradation", "Auth service begins degrading under memory pressure"))
        return

    if scenario_name == "transient_noise":
        if step in {9, 10, 11}:
            state["gateway"]["p95_latency_ms"] *= 1.11
            state["gateway"]["error_rate_pct"] += 0.08
            state["gateway"]["queue_depth"] += 1.1
        if step in {12, 13}:
            state["gateway"]["p95_latency_ms"] *= 0.96
            state["gateway"]["queue_depth"] = max(0.0, state["gateway"]["queue_depth"] - 0.7)
        if step == 9:
            events.append(
                ChangeEvent(
                    timestamp,
                    step,
                    "gateway",
                    "transient_burst",
                    "Short-lived ingress jitter creates a local latency burst without sustained demand growth",
                )
            )
        return

    if scenario_name == "boutique_frontend_spike":
        if step >= 10:
            state["frontend"]["request_rate"] *= 1.16
            state["frontend"]["p95_latency_ms"] *= 1.09
            state["checkout"]["queue_depth"] += 3
            state["recommendation"]["cpu_pct"] += 4
            state["checkout"]["p95_latency_ms"] *= 1.05
            if step == 10:
                events.append(ChangeEvent(timestamp, step, "frontend", "traffic_spike", "Storefront demand surge fans out across checkout and recommendation paths"))
        return

    if scenario_name == "boutique_bad_canary":
        if step >= 9:
            state["checkout"]["error_rate_pct"] += 1.5
            state["checkout"]["p95_latency_ms"] *= 1.15
            state["frontend"]["error_rate_pct"] += 0.25
            state["payment"]["queue_depth"] += 2
            state["email"]["queue_depth"] += 2
            if step == 9:
                events.append(ChangeEvent(timestamp, step, "checkout", "deploy", "Checkout canary release introduced order-path regressions"))
        return

    if scenario_name == "boutique_payment_timeout":
        if step >= 10:
            state["payment"]["error_rate_pct"] += 0.95
            state["payment"]["p95_latency_ms"] *= 1.18
            state["checkout"]["p95_latency_ms"] *= 1.09
            state["checkout"]["error_rate_pct"] += 0.18
            state["frontend"]["p95_latency_ms"] *= 1.04
            if step == 10:
                events.append(
                    ChangeEvent(
                        timestamp,
                        step,
                        "payment",
                        "backend_timeout",
                        "Payment backend timeout isolates a failing dependency path and makes rerouting safer than generic scaling",
                    )
                )
        return

    if scenario_name == "boutique_email_backlog":
        if step >= 11:
            state["email"]["queue_depth"] += 9
            state["email"]["p95_latency_ms"] *= 1.1
            state["checkout"]["queue_depth"] += 2.5
            state["checkout"]["p95_latency_ms"] *= 1.04
            if step == 11:
                events.append(ChangeEvent(timestamp, step, "email", "consumer_lag", "Order confirmation backlog grows faster than workers can drain it"))
        return

    if scenario_name == "boutique_cache_jitter":
        if step in {9, 10, 11}:
            state["cart"]["p95_latency_ms"] *= 1.1
            state["cart"]["error_rate_pct"] += 0.06
            state["cart"]["queue_depth"] += 1.0
            state["frontend"]["p95_latency_ms"] *= 1.03
        if step in {12, 13}:
            state["cart"]["p95_latency_ms"] *= 0.95
            state["cart"]["queue_depth"] = max(0.0, state["cart"]["queue_depth"] - 0.8)
        if step == 9:
            events.append(
                ChangeEvent(
                    timestamp,
                    step,
                    "cart",
                    "cache_jitter",
                    "Brief cart-path cache jitter raises local latency without sustained demand pressure",
                )
            )
        return

    raise ValueError(f"Scenario '{scenario_name}' is registered but has no simulator effects.")


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
