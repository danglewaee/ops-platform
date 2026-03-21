from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class MetricSample:
    timestamp: datetime
    step: int
    service: str
    metric: str
    value: float
    unit: str = ""
    dimensions: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ChangeEvent:
    timestamp: datetime
    step: int
    service: str
    event_type: str
    description: str


@dataclass(slots=True)
class Anomaly:
    timestamp: datetime
    step: int
    service: str
    metric: str
    observed: float
    baseline: float
    deviation_ratio: float
    severity: str
    confidence: float
    explanation: str


@dataclass(slots=True)
class Incident:
    incident_id: str
    opened_at: datetime
    services: list[str]
    root_cause_candidates: list[str]
    severity: str
    trigger_event: str | None
    anomaly_count: int
    summary: str


@dataclass(slots=True)
class Forecast:
    service: str
    horizon_minutes: int
    projected_request_rate: float
    projected_p95_latency_ms: float
    projected_queue_depth: float
    risk_level: str
    rationale: str


@dataclass(slots=True)
class Recommendation:
    action: str
    target_service: str
    confidence: float
    rationale: str
    projected_cost_delta_pct: float
    projected_p95_delta_ms: float
    expected_risk_change: str
    trigger_incident_id: str


@dataclass(slots=True)
class ScenarioMetadata:
    name: str
    description: str
    root_cause: str
    expected_action: str
    impacted_services: list[str]
    category: str = "systems"


@dataclass(slots=True)
class EvaluationSummary:
    alert_reduction_pct: float
    incident_count: int
    anomaly_count: int
    top2_root_cause_hit: bool | None
    recommended_action_match: bool | None
    average_cost_delta_pct: float
    average_p95_delta_ms: float
    decision_latency_ms: float
    evaluation_mode: str = "ground_truth"
    latency_protection_pct: float = 0.0
    avoided_overprovisioning_pct: float = 0.0
    baseline_win_rate_pct: float = 0.0
    action_stability_pct: float = 100.0
    baseline_comparisons: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class PipelineReport:
    metadata: ScenarioMetadata
    anomalies: list[Anomaly]
    incidents: list[Incident]
    forecasts: list[Forecast]
    recommendations: list[Recommendation]
    evaluation: EvaluationSummary

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PipelineReport":
        return cls(
            metadata=ScenarioMetadata.from_dict(payload["metadata"]),
            anomalies=[Anomaly.from_dict(item) for item in payload["anomalies"]],
            incidents=[Incident.from_dict(item) for item in payload["incidents"]],
            forecasts=[Forecast.from_dict(item) for item in payload["forecasts"]],
            recommendations=[Recommendation.from_dict(item) for item in payload["recommendations"]],
            evaluation=EvaluationSummary.from_dict(payload["evaluation"]),
        )


def _parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


@classmethod
def _metric_sample_from_dict(cls, payload: dict[str, Any]) -> "MetricSample":
    return cls(
        timestamp=_parse_datetime(payload["timestamp"]),
        step=payload["step"],
        service=payload["service"],
        metric=payload["metric"],
        value=payload["value"],
        unit=payload.get("unit", ""),
        dimensions=payload.get("dimensions", {}),
    )


@classmethod
def _change_event_from_dict(cls, payload: dict[str, Any]) -> "ChangeEvent":
    return cls(
        timestamp=_parse_datetime(payload["timestamp"]),
        step=payload["step"],
        service=payload["service"],
        event_type=payload["event_type"],
        description=payload["description"],
    )


@classmethod
def _anomaly_from_dict(cls, payload: dict[str, Any]) -> "Anomaly":
    return cls(
        timestamp=_parse_datetime(payload["timestamp"]),
        step=payload["step"],
        service=payload["service"],
        metric=payload["metric"],
        observed=payload["observed"],
        baseline=payload["baseline"],
        deviation_ratio=payload["deviation_ratio"],
        severity=payload["severity"],
        confidence=payload["confidence"],
        explanation=payload["explanation"],
    )


@classmethod
def _incident_from_dict(cls, payload: dict[str, Any]) -> "Incident":
    return cls(
        incident_id=payload["incident_id"],
        opened_at=_parse_datetime(payload["opened_at"]),
        services=payload["services"],
        root_cause_candidates=payload["root_cause_candidates"],
        severity=payload["severity"],
        trigger_event=payload.get("trigger_event"),
        anomaly_count=payload["anomaly_count"],
        summary=payload["summary"],
    )


@classmethod
def _forecast_from_dict(cls, payload: dict[str, Any]) -> "Forecast":
    return cls(
        service=payload["service"],
        horizon_minutes=payload["horizon_minutes"],
        projected_request_rate=payload["projected_request_rate"],
        projected_p95_latency_ms=payload["projected_p95_latency_ms"],
        projected_queue_depth=payload["projected_queue_depth"],
        risk_level=payload["risk_level"],
        rationale=payload["rationale"],
    )


@classmethod
def _recommendation_from_dict(cls, payload: dict[str, Any]) -> "Recommendation":
    return cls(
        action=payload["action"],
        target_service=payload["target_service"],
        confidence=payload["confidence"],
        rationale=payload["rationale"],
        projected_cost_delta_pct=payload["projected_cost_delta_pct"],
        projected_p95_delta_ms=payload["projected_p95_delta_ms"],
        expected_risk_change=payload["expected_risk_change"],
        trigger_incident_id=payload["trigger_incident_id"],
    )


@classmethod
def _scenario_metadata_from_dict(cls, payload: dict[str, Any]) -> "ScenarioMetadata":
    return cls(
        name=payload["name"],
        description=payload["description"],
        root_cause=payload["root_cause"],
        expected_action=payload["expected_action"],
        impacted_services=payload["impacted_services"],
        category=payload.get("category", "systems"),
    )


@classmethod
def _evaluation_summary_from_dict(cls, payload: dict[str, Any]) -> "EvaluationSummary":
    return cls(
        alert_reduction_pct=payload["alert_reduction_pct"],
        incident_count=payload["incident_count"],
        anomaly_count=payload["anomaly_count"],
        top2_root_cause_hit=payload["top2_root_cause_hit"],
        recommended_action_match=payload["recommended_action_match"],
        average_cost_delta_pct=payload["average_cost_delta_pct"],
        average_p95_delta_ms=payload["average_p95_delta_ms"],
        decision_latency_ms=payload["decision_latency_ms"],
        evaluation_mode=payload.get("evaluation_mode", "ground_truth"),
        latency_protection_pct=payload.get("latency_protection_pct", 0.0),
        avoided_overprovisioning_pct=payload.get("avoided_overprovisioning_pct", 0.0),
        baseline_win_rate_pct=payload.get("baseline_win_rate_pct", 0.0),
        action_stability_pct=payload.get("action_stability_pct", 100.0),
        baseline_comparisons=payload.get("baseline_comparisons", []),
    )


MetricSample.from_dict = classmethod(_metric_sample_from_dict)
ChangeEvent.from_dict = classmethod(_change_event_from_dict)
Anomaly.from_dict = classmethod(_anomaly_from_dict)
Incident.from_dict = classmethod(_incident_from_dict)
Forecast.from_dict = classmethod(_forecast_from_dict)
Recommendation.from_dict = classmethod(_recommendation_from_dict)
ScenarioMetadata.from_dict = classmethod(_scenario_metadata_from_dict)
EvaluationSummary.from_dict = classmethod(_evaluation_summary_from_dict)
