from __future__ import annotations

from dataclasses import asdict
import os

from .prometheus_ingestion import load_prometheus_bundle
from .pipeline import run_pipeline, run_pipeline_from_streams, run_scenario_matrix
from .schemas import ChangeEvent, DecisionConstraints, MetricSample, ScenarioMetadata
from .scenarios import SCENARIOS, list_scenarios
from .storage import (
    get_storage_stats,
    ingest_stream_bundle,
    list_ingested_streams,
    list_saved_runs,
    load_ingested_stream,
    load_run_bundle,
    prune_ingested_streams,
    save_stream_report,
)
from .telemetry import configure_tracing


def _report_summary(report):
    recommendation = report.recommendations[0] if report.recommendations else None
    return {
        "scenario": report.metadata.name,
        "description": report.metadata.description,
        "category": report.metadata.category,
        "root_cause": report.metadata.root_cause,
        "expected_action": report.metadata.expected_action,
        "incident_count": report.evaluation.incident_count,
        "anomaly_count": report.evaluation.anomaly_count,
        "alert_reduction_pct": report.evaluation.alert_reduction_pct,
        "top2_root_cause_hit": report.evaluation.top2_root_cause_hit,
        "recommended_action_match": report.evaluation.recommended_action_match,
        "decision_latency_ms": report.evaluation.decision_latency_ms,
        "evaluation_mode": report.evaluation.evaluation_mode,
        "planner_mode": report.evaluation.planner_mode,
        "trace_id": report.evaluation.trace_id,
        "latency_protection_pct": report.evaluation.latency_protection_pct,
        "avoided_overprovisioning_pct": report.evaluation.avoided_overprovisioning_pct,
        "baseline_win_rate_pct": report.evaluation.baseline_win_rate_pct,
        "action_stability_pct": report.evaluation.action_stability_pct,
        "recommendation": {
            "action": recommendation.action if recommendation else None,
            "target_service": recommendation.target_service if recommendation else None,
            "confidence": recommendation.confidence if recommendation else None,
        },
        "baselines": report.evaluation.baseline_comparisons,
    }


def create_app():
    try:
        from fastapi import FastAPI
        from pydantic import BaseModel, Field
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "FastAPI is not installed. Run `pip install -e .[api]` inside ops-decision-platform first."
        ) from exc

    class MetricSamplePayload(BaseModel):
        timestamp: str
        step: int
        service: str
        metric: str
        value: float
        unit: str = ""
        dimensions: dict[str, str] = Field(default_factory=dict)

    class ChangeEventPayload(BaseModel):
        timestamp: str
        step: int
        service: str
        event_type: str
        description: str

    class IngestBundlePayload(BaseModel):
        stream_id: str
        source: str = "api"
        environment: str = "production"
        db_path: str | None = None
        metadata: dict[str, object] = Field(default_factory=dict)
        telemetry: list[MetricSamplePayload] = Field(default_factory=list)
        events: list[ChangeEventPayload] = Field(default_factory=list)

    class StreamEvaluationPayload(BaseModel):
        name: str | None = None
        description: str | None = None
        root_cause: str = ""
        expected_action: str = ""
        impacted_services: list[str] = Field(default_factory=list)
        category: str = "live"
        planner_mode: str = "heuristic"
        max_total_cost_delta_pct: float | None = None
        max_cost_delta_pct_per_action: float | None = None
        max_allowed_p95_delta_ms: float | None = None
        allow_hold_steady: bool = True
        allow_reroute_traffic: bool = True
        allow_scale_out: bool = True
        allow_increase_consumers: bool = True
        allow_rollback_candidate: bool = True

    class PrometheusIngestPayload(BaseModel):
        config_path: str
        stream_id: str
        start: str | None = None
        end: str | None = None
        lookback_minutes: int | None = None
        base_url: str | None = None
        events_path: str | None = None
        event_mapping: str | None = None
        source: str = "prometheus"
        environment: str = "production"
        db_path: str | None = None
        name: str | None = None
        description: str = "Prometheus telemetry replay evaluated in shadow mode."
        root_cause: str = ""
        expected_action: str = ""
        category: str = "live"
        evaluate: bool = False
        planner_mode: str = "heuristic"
        max_total_cost_delta_pct: float | None = None
        max_cost_delta_pct_per_action: float | None = None
        max_allowed_p95_delta_ms: float | None = None
        allow_hold_steady: bool = True
        allow_reroute_traffic: bool = True
        allow_scale_out: bool = True
        allow_increase_consumers: bool = True
        allow_rollback_candidate: bool = True

    class StoragePrunePayload(BaseModel):
        older_than_days: int | None = None
        keep_latest: int | None = None
        environment: str | None = None
        source: str | None = None
        vacuum: bool = False
        dry_run: bool = False
        db_path: str | None = None

    if os.getenv("OPS_PLATFORM_OTEL_ENABLE", "").lower() in {"1", "true", "yes", "on"}:
        configure_tracing(
            service_name=os.getenv("OPS_PLATFORM_OTEL_SERVICE_NAME", "ops-decision-platform-api"),
            otlp_endpoint=os.getenv("OPS_PLATFORM_OTEL_EXPORTER_OTLP_ENDPOINT"),
        )

    app = FastAPI(title="Ops Decision Platform", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/scenarios")
    def scenarios() -> dict[str, list[str]]:
        return {"scenarios": list_scenarios()}

    @app.get("/scenarios/catalog")
    def scenario_catalog() -> list[dict[str, object]]:
        return [
            {
                "name": metadata.name,
                "description": metadata.description,
                "root_cause": metadata.root_cause,
                "expected_action": metadata.expected_action,
                "impacted_services": metadata.impacted_services,
                "category": metadata.category,
            }
            for metadata in SCENARIOS.values()
        ]

    @app.get("/simulate/{scenario_name}")
    def simulate(
        scenario_name: str,
        seed: int = 7,
        planner_mode: str = "heuristic",
    ):
        report = run_pipeline(scenario_name, seed=seed, planner_mode=planner_mode)
        return report.to_dict()

    @app.get("/simulate/{scenario_name}/summary")
    def simulate_summary(
        scenario_name: str,
        seed: int = 7,
        planner_mode: str = "heuristic",
    ):
        report = run_pipeline(scenario_name, seed=seed, planner_mode=planner_mode)
        return _report_summary(report)

    @app.get("/matrix")
    def matrix(seed: int = 7, planner_mode: str = "heuristic"):
        reports = run_scenario_matrix(seed=seed, planner_mode=planner_mode)
        return [_report_summary(report) for report in reports]

    @app.get("/runs")
    def runs():
        return list_saved_runs()

    @app.post("/ingest/bundle")
    def ingest_bundle(payload: IngestBundlePayload):
        telemetry = [MetricSample.from_dict(sample.model_dump()) for sample in payload.telemetry]
        events = [ChangeEvent.from_dict(event.model_dump()) for event in payload.events]
        ingest_stream_bundle(
            payload.stream_id,
            telemetry,
            events,
            source=payload.source,
            environment=payload.environment,
            metadata=payload.metadata,
            db_path=payload.db_path,
        )
        stream = load_ingested_stream(payload.stream_id, db_path=payload.db_path)
        return {
            "stream_id": stream["stream_id"],
            "environment": stream["environment"],
            "source": stream["source"],
            "metric_count": len(stream["telemetry"]),
            "event_count": len(stream["events"]),
        }

    @app.post("/ingest/prometheus")
    def ingest_prometheus(payload: PrometheusIngestPayload):
        config, telemetry, events, start, end = load_prometheus_bundle(
            payload.config_path,
            start=payload.start,
            end=payload.end,
            lookback_minutes=payload.lookback_minutes,
            base_url=payload.base_url,
            events_path=payload.events_path,
            event_mapping_path=payload.event_mapping,
        )
        services = sorted({sample.service for sample in telemetry})
        stream_metadata = {
            "name": payload.name or payload.stream_id,
            "description": payload.description,
            "root_cause": payload.root_cause,
            "expected_action": payload.expected_action,
            "impacted_services": services,
            "category": payload.category,
        }
        ingest_stream_bundle(
            payload.stream_id,
            telemetry,
            events,
            source=payload.source,
            environment=payload.environment,
            metadata=stream_metadata,
            db_path=payload.db_path,
        )

        summary: dict[str, object] = {
            "stream_id": payload.stream_id,
            "source": payload.source,
            "environment": payload.environment,
            "metric_count": len(telemetry),
            "event_count": len(events),
            "services": services,
            "metrics": sorted({sample.metric for sample in telemetry}),
            "range": {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "step": config.step,
            },
        }

        if payload.evaluate:
            metadata = ScenarioMetadata(
                name=stream_metadata["name"],
                description=stream_metadata["description"],
                root_cause=stream_metadata["root_cause"],
                expected_action=stream_metadata["expected_action"],
                impacted_services=stream_metadata["impacted_services"],
                category=stream_metadata["category"],
            )
            report = run_pipeline_from_streams(
                telemetry,
                events,
                metadata,
                planner_mode=getattr(payload, "planner_mode", "heuristic"),
                decision_constraints=_decision_constraints_from_payload(payload),
            )
            save_stream_report(payload.stream_id, metadata, report, db_path=payload.db_path)
            summary["evaluation"] = _report_summary(report)

        return summary

    @app.get("/streams")
    def streams(
        environment: str | None = None,
        source: str | None = None,
        created_after: str | None = None,
        created_before: str | None = None,
        limit: int | None = None,
        db_path: str | None = None,
    ):
        return list_ingested_streams(
            environment=environment,
            source=source,
            created_after=created_after,
            created_before=created_before,
            limit=limit,
            db_path=db_path,
        )

    @app.get("/storage/stats")
    def storage_stats(
        environment: str | None = None,
        source: str | None = None,
        created_after: str | None = None,
        created_before: str | None = None,
        db_path: str | None = None,
    ):
        return get_storage_stats(
            environment=environment,
            source=source,
            created_after=created_after,
            created_before=created_before,
            db_path=db_path,
        )

    @app.post("/storage/prune")
    def storage_prune(payload: StoragePrunePayload):
        return prune_ingested_streams(
            older_than_days=payload.older_than_days,
            keep_latest=payload.keep_latest,
            environment=payload.environment,
            source=payload.source,
            vacuum=payload.vacuum,
            dry_run=payload.dry_run,
            db_path=payload.db_path,
        )

    @app.get("/streams/{stream_id}")
    def stream_summary(stream_id: str, db_path: str | None = None):
        stream = load_ingested_stream(stream_id, db_path=db_path)
        services = sorted({sample.service for sample in stream["telemetry"]})
        latest_report = stream["latest_report"]
        return {
            "stream_id": stream["stream_id"],
            "created_at": stream["created_at"],
            "source": stream["source"],
            "environment": stream["environment"],
            "metadata": stream["metadata"],
            "metric_count": len(stream["telemetry"]),
            "event_count": len(stream["events"]),
            "services": services,
            "latest_report": _report_summary(latest_report["report"]) if latest_report else None,
        }

    @app.get("/streams/{stream_id}/timeline")
    def stream_timeline(stream_id: str, db_path: str | None = None):
        stream = load_ingested_stream(stream_id, db_path=db_path)
        return {
            "stream_id": stream["stream_id"],
            "telemetry": [asdict(sample) for sample in stream["telemetry"]],
            "events": [asdict(event) for event in stream["events"]],
        }

    @app.post("/streams/{stream_id}/evaluate")
    def evaluate_stream(stream_id: str, payload: StreamEvaluationPayload | None = None, db_path: str | None = None):
        stream = load_ingested_stream(stream_id, db_path=db_path)
        metadata = _resolve_stream_metadata(stream_id, stream, payload)
        report = run_pipeline_from_streams(
            stream["telemetry"],
            stream["events"],
            metadata,
            planner_mode=getattr(payload, "planner_mode", "heuristic") if payload else "heuristic",
            decision_constraints=_decision_constraints_from_payload(payload),
        )
        save_stream_report(stream_id, metadata, report, db_path=db_path)
        return _report_summary(report)

    @app.get("/runs/replay")
    def replay_run(path: str):
        bundle = load_run_bundle(path)
        replay = run_pipeline_from_streams(bundle["telemetry"], bundle["events"], bundle["metadata"])
        return _report_summary(replay)

    return app


def _decision_constraints_from_payload(payload) -> DecisionConstraints | None:
    if payload is None:
        return None

    if all(
        getattr(payload, field, None) is None
        for field in (
            "max_total_cost_delta_pct",
            "max_cost_delta_pct_per_action",
            "max_allowed_p95_delta_ms",
        )
    ) and all(
        getattr(payload, field, True)
        for field in (
            "allow_hold_steady",
            "allow_reroute_traffic",
            "allow_scale_out",
            "allow_increase_consumers",
            "allow_rollback_candidate",
        )
    ):
        return None

    return DecisionConstraints(
        max_total_cost_delta_pct=getattr(payload, "max_total_cost_delta_pct", None),
        max_cost_delta_pct_per_action=getattr(payload, "max_cost_delta_pct_per_action", None),
        max_allowed_p95_delta_ms=getattr(payload, "max_allowed_p95_delta_ms", None),
        allow_hold_steady=getattr(payload, "allow_hold_steady", True),
        allow_reroute_traffic=getattr(payload, "allow_reroute_traffic", True),
        allow_scale_out=getattr(payload, "allow_scale_out", True),
        allow_increase_consumers=getattr(payload, "allow_increase_consumers", True),
        allow_rollback_candidate=getattr(payload, "allow_rollback_candidate", True),
    )


def _resolve_stream_metadata(
    stream_id: str,
    stream: dict[str, object],
    payload,
) -> ScenarioMetadata:
    stored_metadata = stream.get("metadata", {}) or {}
    services = sorted({sample.service for sample in stream["telemetry"]})

    if payload is None:
        return ScenarioMetadata(
            name=str(stored_metadata.get("name") or stream_id),
            description=str(
                stored_metadata.get("description")
                or "Persisted telemetry replay evaluated in shadow mode."
            ),
            root_cause=str(stored_metadata.get("root_cause") or ""),
            expected_action=str(stored_metadata.get("expected_action") or ""),
            impacted_services=list(stored_metadata.get("impacted_services") or services),
            category=str(stored_metadata.get("category") or "live"),
        )

    return ScenarioMetadata(
        name=payload.name or str(stored_metadata.get("name") or stream_id),
        description=payload.description or "Persisted telemetry replay evaluated in shadow mode.",
        root_cause=payload.root_cause,
        expected_action=payload.expected_action,
        impacted_services=payload.impacted_services or services,
        category=payload.category,
    )
