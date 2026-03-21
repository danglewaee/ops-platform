from __future__ import annotations

from collections import defaultdict
from statistics import mean

from .schemas import Anomaly, MetricSample

BASELINE_WINDOW = 6

METRIC_THRESHOLDS = {
    "request_rate": 0.22,
    "p95_latency_ms": 0.18,
    "error_rate_pct": 0.55,
    "queue_depth": 0.4,
    "cpu_pct": 0.15,
}

MIN_ABSOLUTE_DELTA = {
    "request_rate": 180.0,
    "p95_latency_ms": 18.0,
    "error_rate_pct": 0.18,
    "queue_depth": 2.5,
    "cpu_pct": 8.0,
}


def detect_anomalies(samples: list[MetricSample]) -> list[Anomaly]:
    grouped: dict[tuple[str, str], list[MetricSample]] = defaultdict(list)
    for sample in samples:
        grouped[(sample.service, sample.metric)].append(sample)

    anomalies: list[Anomaly] = []
    for (service, metric), series in grouped.items():
        series.sort(key=lambda sample: sample.step)
        threshold = METRIC_THRESHOLDS[metric]
        absolute_threshold = MIN_ABSOLUTE_DELTA[metric]

        for index in range(BASELINE_WINDOW, len(series)):
            sample = series[index]
            baseline_window = series[index - BASELINE_WINDOW : index]
            baseline = mean(previous.value for previous in baseline_window)
            deviation_ratio = _safe_ratio(sample.value, baseline)
            absolute_delta = sample.value - baseline
            if deviation_ratio < threshold or absolute_delta < absolute_threshold:
                continue

            anomalies.append(
                Anomaly(
                    timestamp=sample.timestamp,
                    step=sample.step,
                    service=service,
                    metric=metric,
                    observed=sample.value,
                    baseline=round(baseline, 3),
                    deviation_ratio=round(deviation_ratio, 3),
                    severity=_classify_severity(deviation_ratio),
                    confidence=round(min(0.99, 0.48 + deviation_ratio), 3),
                    explanation=(
                        f"{metric} on {service} moved to {sample.value:.2f} "
                        f"against a baseline of {baseline:.2f} (+{deviation_ratio * 100:.1f}%)."
                    ),
                )
            )

    anomalies.sort(key=lambda anomaly: (anomaly.step, anomaly.service, anomaly.metric))
    return anomalies


def _classify_severity(deviation_ratio: float) -> str:
    if deviation_ratio >= 0.75:
        return "critical"
    if deviation_ratio >= 0.4:
        return "high"
    if deviation_ratio >= 0.22:
        return "medium"
    return "low"


def _safe_ratio(observed: float, baseline: float) -> float:
    if baseline <= 0:
        return 0.0
    return max(0.0, (observed - baseline) / baseline)
