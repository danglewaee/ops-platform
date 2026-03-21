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


@dataclass(slots=True)
class EvaluationSummary:
    alert_reduction_pct: float
    incident_count: int
    anomaly_count: int
    top2_root_cause_hit: bool
    recommended_action_match: bool
    average_cost_delta_pct: float
    average_p95_delta_ms: float
    decision_latency_ms: float
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
