from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import asdict
import os
from uuid import uuid4

from .prometheus_ingestion import load_prometheus_bundle
from .pipeline import run_pipeline, run_pipeline_from_streams, run_scenario_matrix
from .schemas import ChangeEvent, DecisionConstraints, MetricSample, ScenarioMetadata
from .scenarios import SCENARIOS, list_scenarios
from .security import build_rate_limiter
from .settings import AppSettings, load_app_settings
from .storage import (
    check_storage_health,
    get_storage_stats,
    ingest_stream_bundle,
    initialize_storage,
    list_audit_events,
    list_ingested_streams,
    list_saved_runs,
    load_ingested_stream,
    load_run_bundle,
    prune_ingested_streams,
    save_audit_event,
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
        "service_health": [
            {
                "service": item.service,
                "current_burn_rate": item.current_burn_rate,
                "projected_burn_rate": item.projected_burn_rate,
                "budget_pressure": item.budget_pressure,
                "dominant_signal": item.dominant_signal,
                "estimated_error_budget_remaining_pct": item.estimated_error_budget_remaining_pct,
            }
            for item in report.service_health
        ],
        "incidents": [
            {
                "incident_id": incident.incident_id,
                "severity": incident.severity,
                "summary": incident.summary,
                "root_cause_candidates": incident.root_cause_candidates,
                "top_signals": incident.top_signals,
                "blast_radius_services": incident.blast_radius_services,
                "evidence": [asdict(item) for item in incident.evidence],
                "graph_edges": [asdict(item) for item in incident.graph_edges],
            }
            for incident in report.incidents
        ],
        "baselines": report.evaluation.baseline_comparisons,
    }


def create_app():
    try:
        from fastapi import FastAPI, Request
        from fastapi.responses import JSONResponse
        from pydantic import BaseModel, Field
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "FastAPI is not installed. Run `pip install -e .[api]` inside ops-decision-platform first."
        ) from exc

    settings = _load_runtime_settings()

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

    if settings.enable_tracing or os.getenv("OPS_PLATFORM_OTEL_ENABLE", "").lower() in {"1", "true", "yes", "on"}:
        configure_tracing(
            service_name=settings.otel_service_name,
            otlp_endpoint=settings.otlp_endpoint or os.getenv("OPS_PLATFORM_OTEL_EXPORTER_OTLP_ENDPOINT"),
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        initialize_app_runtime(app, settings)
        yield

    app = FastAPI(title="Ops Decision Platform", version="0.1.0", lifespan=lifespan)
    seed_app_runtime(app, settings)

    @app.middleware("http")
    async def request_controls(request: Request, call_next):
        path = request.url.path
        request_id = request.headers.get("x-request-id") or str(uuid4())
        actor = _resolve_actor(request, settings)
        client_ip = request.client.host if request.client else None
        api_key = request.headers.get(settings.auth_header_name)
        action = _resolve_audit_action(request.method, path)
        resource_type, resource_id = _resolve_resource(path)
        rate_decision = None

        if settings.auth_enabled and not _is_public_path(path):
            if not api_key or api_key not in settings.api_keys:
                response = JSONResponse(status_code=401, content={"detail": "Unauthorized"})
                response.headers["X-Request-Id"] = request_id
                _write_audit_event(
                    settings=settings,
                    actor=actor,
                    action=action,
                    method=request.method,
                    path=path,
                    status_code=401,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    client_ip=client_ip,
                    request_id=request_id,
                    metadata={"outcome": "auth_denied"},
                )
                return response

        if settings.rate_limit_enabled and not _is_public_path(path) and app.state.rate_limiter is not None:
            rate_key = _resolve_rate_limit_key(api_key=api_key, actor=actor, client_ip=client_ip)
            rate_decision = app.state.rate_limiter.allow(rate_key)
            if not rate_decision.allowed:
                response = JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})
                response.headers["X-Request-Id"] = request_id
                response.headers["Retry-After"] = str(
                    rate_decision.retry_after_seconds or settings.rate_limit_window_seconds
                )
                response.headers["X-RateLimit-Limit"] = str(settings.rate_limit_requests)
                response.headers["X-RateLimit-Remaining"] = "0"
                _write_audit_event(
                    settings=settings,
                    actor=actor,
                    action=action,
                    method=request.method,
                    path=path,
                    status_code=429,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    client_ip=client_ip,
                    request_id=request_id,
                    metadata={"outcome": "rate_limited"},
                )
                return response

        response = await call_next(request)
        response.headers.setdefault("X-Request-Id", request_id)
        if rate_decision is not None:
            response.headers["X-RateLimit-Limit"] = str(settings.rate_limit_requests)
            response.headers["X-RateLimit-Remaining"] = str(rate_decision.remaining)

        if _should_audit_request(request.method, path, response.status_code):
            metadata = {}
            if request.url.query:
                metadata["query"] = str(request.url.query)
            if rate_decision is not None:
                metadata["rate_limit_remaining"] = rate_decision.remaining
            _write_audit_event(
                settings=settings,
                actor=actor,
                action=action,
                method=request.method,
                path=path,
                status_code=response.status_code,
                resource_type=resource_type,
                resource_id=resource_id,
                client_ip=client_ip,
                request_id=request_id,
                metadata=metadata,
            )
        return response

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    def ready():
        readiness = dict(app.state.readiness)
        status_code = 200 if readiness.get("ready") else 503
        return JSONResponse(status_code=status_code, content=readiness)

    @app.get("/audit/events")
    def audit_events(
        limit: int = 100,
        actor: str | None = None,
        action: str | None = None,
        created_after: str | None = None,
        created_before: str | None = None,
        db_path: str | None = None,
    ):
        return list_audit_events(
            limit=limit,
            actor=actor,
            action=action,
            created_after=created_after,
            created_before=created_before,
            db_path=_resolve_db_path(db_path, settings),
        )

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
            db_path=_resolve_db_path(payload.db_path, settings),
        )
        stream = load_ingested_stream(payload.stream_id, db_path=_resolve_db_path(payload.db_path, settings))
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
            db_path=_resolve_db_path(payload.db_path, settings),
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
            save_stream_report(
                payload.stream_id,
                metadata,
                report,
                db_path=_resolve_db_path(payload.db_path, settings),
            )
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
            db_path=_resolve_db_path(db_path, settings),
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
            db_path=_resolve_db_path(db_path, settings),
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
            db_path=_resolve_db_path(payload.db_path, settings),
        )

    @app.get("/streams/{stream_id}")
    def stream_summary(stream_id: str, db_path: str | None = None):
        stream = load_ingested_stream(stream_id, db_path=_resolve_db_path(db_path, settings))
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
        stream = load_ingested_stream(stream_id, db_path=_resolve_db_path(db_path, settings))
        return {
            "stream_id": stream["stream_id"],
            "telemetry": [asdict(sample) for sample in stream["telemetry"]],
            "events": [asdict(event) for event in stream["events"]],
        }

    @app.post("/streams/{stream_id}/evaluate")
    def evaluate_stream(stream_id: str, payload: StreamEvaluationPayload | None = None, db_path: str | None = None):
        resolved_db_path = _resolve_db_path(db_path, settings)
        stream = load_ingested_stream(stream_id, db_path=resolved_db_path)
        metadata = _resolve_stream_metadata(stream_id, stream, payload)
        report = run_pipeline_from_streams(
            stream["telemetry"],
            stream["events"],
            metadata,
            planner_mode=getattr(payload, "planner_mode", "heuristic") if payload else "heuristic",
            decision_constraints=_decision_constraints_from_payload(payload),
        )
        save_stream_report(stream_id, metadata, report, db_path=resolved_db_path)
        return _report_summary(report)

    @app.get("/runs/replay")
    def replay_run(path: str):
        bundle = load_run_bundle(path)
        replay = run_pipeline_from_streams(bundle["telemetry"], bundle["events"], bundle["metadata"])
        return _report_summary(replay)

    return app


def _load_runtime_settings() -> AppSettings:
    return load_app_settings()


def seed_app_runtime(app, settings: AppSettings) -> None:
    app.state.runtime_settings = settings
    app.state.readiness = _build_readiness_state(settings)
    app.state.rate_limiter = None


def initialize_app_runtime(app, settings: AppSettings) -> None:
    app.state.readiness = _build_readiness_state(settings)

    try:
        app.state.rate_limiter = (
            build_rate_limiter(
                backend=settings.rate_limit_backend,
                max_requests=settings.rate_limit_requests,
                window_seconds=settings.rate_limit_window_seconds,
                redis_url=settings.redis_url,
                redis_key_prefix=settings.redis_key_prefix,
            )
            if settings.rate_limit_enabled
            else None
        )
    except Exception as exc:
        app.state.readiness = {
            **app.state.readiness,
            "ready": False,
            "error": str(exc),
        }
        return

    if settings.auto_init_storage:
        try:
            initialize_storage(
                settings.db_path,
                metric_retention_days=settings.timescale_metric_retention_days,
                event_retention_days=settings.timescale_event_retention_days,
                compress_after_days=settings.timescale_compress_after_days,
                create_continuous_aggregate=settings.timescale_create_metric_rollup,
                aggregate_bucket=settings.timescale_aggregate_bucket,
                aggregate_name=settings.timescale_aggregate_name,
                refresh_start_offset=settings.timescale_refresh_start_offset,
                refresh_end_offset=settings.timescale_refresh_end_offset,
                refresh_schedule_interval=settings.timescale_refresh_schedule_interval,
            )
        except Exception as exc:
            app.state.readiness = {
                **app.state.readiness,
                "ready": False,
                "error": str(exc),
            }
            return

    app.state.readiness = {
        **app.state.readiness,
        **check_storage_health(settings.db_path),
        "auth_enabled": settings.auth_enabled,
        "rate_limit_enabled": settings.rate_limit_enabled,
        "rate_limit_backend": settings.rate_limit_backend,
        "audit_log_enabled": settings.audit_log_enabled,
        "tracing_enabled": settings.enable_tracing,
        "otel_service_name": settings.otel_service_name,
        "error": None,
    }


def _build_readiness_state(settings: AppSettings) -> dict[str, object]:
    return {
        "ready": False,
        "backend": "timescaledb"
        if settings.db_path.lower().startswith(("postgresql://", "postgres://", "timescaledb://"))
        else "sqlite",
        "db_path": settings.db_path,
        "auth_enabled": settings.auth_enabled,
        "rate_limit_enabled": settings.rate_limit_enabled,
        "rate_limit_backend": settings.rate_limit_backend,
        "audit_log_enabled": settings.audit_log_enabled,
        "tracing_enabled": settings.enable_tracing,
        "otel_service_name": settings.otel_service_name,
        "error": "startup not completed",
    }


def _resolve_db_path(db_path: str | None, settings: AppSettings) -> str:
    return db_path or settings.db_path


def _is_public_path(path: str) -> bool:
    return path in {"/health", "/ready"}


def _resolve_actor(request, settings: AppSettings) -> str:
    actor = request.headers.get(settings.actor_header_name)
    if actor:
        return actor

    api_key = request.headers.get(settings.auth_header_name)
    if api_key:
        return f"api_key:{_mask_secret(api_key)}"

    if request.client and request.client.host:
        return request.client.host
    return "anonymous"


def _resolve_rate_limit_key(*, api_key: str | None, actor: str, client_ip: str | None) -> str:
    if api_key:
        return f"key:{_mask_secret(api_key)}"
    if actor and actor != "anonymous":
        return f"actor:{actor}"
    return f"ip:{client_ip or 'unknown'}"


def _resolve_audit_action(method: str, path: str) -> str:
    action_map = {
        ("GET", "/audit/events"): "list_audit_events",
        ("POST", "/ingest/bundle"): "ingest_bundle",
        ("POST", "/ingest/prometheus"): "ingest_prometheus",
        ("POST", "/storage/prune"): "prune_storage",
        ("GET", "/storage/stats"): "storage_stats",
        ("GET", "/streams"): "list_streams",
        ("GET", "/runs"): "list_runs",
        ("GET", "/runs/replay"): "replay_run",
        ("GET", "/matrix"): "scenario_matrix",
    }
    if (method, path) in action_map:
        return action_map[(method, path)]
    if path.startswith("/streams/") and path.endswith("/evaluate"):
        return "evaluate_stream"
    if path.startswith("/streams/") and path.endswith("/timeline"):
        return "stream_timeline"
    if path.startswith("/streams/"):
        return "stream_summary"
    if path.startswith("/simulate/") and path.endswith("/summary"):
        return "simulate_summary"
    if path.startswith("/simulate/"):
        return "simulate_scenario"
    if path.startswith("/scenarios/"):
        return "scenario_catalog"
    if path == "/scenarios":
        return "list_scenarios"
    return f"{method.lower()}_{path.strip('/').replace('/', '_') or 'root'}"


def _resolve_resource(path: str) -> tuple[str | None, str | None]:
    if path.startswith("/streams/"):
        parts = [part for part in path.split("/") if part]
        if len(parts) >= 2:
            return "stream", parts[1]
        return "stream", None
    if path.startswith("/storage"):
        return "storage", None
    if path.startswith("/ingest"):
        return "ingestion", None
    if path.startswith("/simulate"):
        return "scenario", path.split("/")[-1]
    return None, None


def _should_audit_request(method: str, path: str, status_code: int) -> bool:
    if path in {"/health", "/ready", "/audit/events"}:
        return False
    if status_code >= 400:
        return True
    return method.upper() in {"POST", "PUT", "PATCH", "DELETE"}


def _write_audit_event(
    *,
    settings: AppSettings,
    actor: str,
    action: str,
    method: str,
    path: str,
    status_code: int,
    resource_type: str | None,
    resource_id: str | None,
    client_ip: str | None,
    request_id: str,
    metadata: dict[str, object] | None,
) -> None:
    if not settings.audit_log_enabled:
        return
    try:
        save_audit_event(
            actor=actor,
            action=action,
            method=method,
            path=path,
            status_code=status_code,
            resource_type=resource_type,
            resource_id=resource_id,
            client_ip=client_ip,
            request_id=request_id,
            metadata=metadata,
            db_path=settings.db_path,
        )
    except Exception:
        return


def _mask_secret(value: str) -> str:
    if len(value) <= 4:
        return "*" * len(value)
    return f"{'*' * max(len(value) - 4, 4)}{value[-4:]}"


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
