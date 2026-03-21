from __future__ import annotations

import json
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .pipeline import run_pipeline_from_streams
from .prometheus_ingestion import load_prometheus_bundle, parse_time_value
from .schemas import ScenarioMetadata
from .storage import get_storage_stats, ingest_stream_bundle, prune_ingested_streams, save_stream_report


@dataclass(slots=True)
class RecurringPullSettings:
    config_path: Path
    base_url: str | None = None
    events_path: str | None = None
    event_mapping: str | None = None
    lookback_minutes: int = 30
    end: str | None = None
    source: str = "prometheus"
    environment: str = "production"
    stream_prefix: str = "prometheus"
    name_prefix: str | None = None
    description: str = "Prometheus telemetry replay evaluated in shadow mode."
    root_cause: str = ""
    expected_action: str = ""
    category: str = "live"
    evaluate: bool = True
    db_path: str | None = None
    summary_path: str | None = None
    retention_older_than_days: int | None = None
    retention_keep_latest: int | None = None
    retention_vacuum: bool = False


def load_recurring_pull_settings(
    config_path: str | Path,
    *,
    base_url: str | None = None,
    events_path: str | None = None,
    event_mapping: str | None = None,
    lookback_minutes: int | None = None,
    end: str | None = None,
    source: str | None = None,
    environment: str | None = None,
    stream_prefix: str | None = None,
    name_prefix: str | None = None,
    description: str | None = None,
    root_cause: str | None = None,
    expected_action: str | None = None,
    category: str | None = None,
    evaluate: bool | None = None,
    db_path: str | None = None,
    summary_path: str | None = None,
    retention_older_than_days: int | None = None,
    retention_keep_latest: int | None = None,
    retention_vacuum: bool | None = None,
) -> RecurringPullSettings:
    resolved_path = Path(config_path)
    payload = _load_config_payload(resolved_path)
    recurring_payload = payload.get("recurring", {})
    retention_payload = payload.get("retention", {})

    return RecurringPullSettings(
        config_path=resolved_path,
        base_url=base_url or recurring_payload.get("base_url"),
        events_path=events_path or recurring_payload.get("events_path"),
        event_mapping=event_mapping or recurring_payload.get("event_mapping"),
        lookback_minutes=int(lookback_minutes or recurring_payload.get("lookback_minutes", 30)),
        end=end or recurring_payload.get("end"),
        source=source or recurring_payload.get("source", "prometheus"),
        environment=environment or recurring_payload.get("environment", "production"),
        stream_prefix=stream_prefix or recurring_payload.get("stream_prefix", "prometheus"),
        name_prefix=name_prefix or recurring_payload.get("name_prefix"),
        description=description or recurring_payload.get(
            "description",
            "Prometheus telemetry replay evaluated in shadow mode.",
        ),
        root_cause=root_cause if root_cause is not None else str(recurring_payload.get("root_cause", "")),
        expected_action=(
            expected_action
            if expected_action is not None
            else str(recurring_payload.get("expected_action", ""))
        ),
        category=category or recurring_payload.get("category", "live"),
        evaluate=evaluate if evaluate is not None else bool(recurring_payload.get("evaluate", True)),
        db_path=db_path or recurring_payload.get("db_path"),
        summary_path=summary_path or recurring_payload.get("summary_path"),
        retention_older_than_days=(
            retention_older_than_days
            if retention_older_than_days is not None
            else retention_payload.get("older_than_days")
        ),
        retention_keep_latest=(
            retention_keep_latest
            if retention_keep_latest is not None
            else retention_payload.get("keep_latest")
        ),
        retention_vacuum=(
            retention_vacuum
            if retention_vacuum is not None
            else bool(retention_payload.get("vacuum", False))
        ),
    )


def run_recurring_pull(settings: RecurringPullSettings) -> dict[str, Any]:
    before = get_storage_stats(
        environment=settings.environment,
        source=settings.source,
        db_path=settings.db_path,
    )

    config, telemetry, events, start, end = load_prometheus_bundle(
        settings.config_path,
        start=None,
        end=settings.end,
        lookback_minutes=settings.lookback_minutes,
        base_url=settings.base_url,
        events_path=settings.events_path,
        event_mapping_path=settings.event_mapping,
    )
    services = sorted({sample.service for sample in telemetry})
    stream_id = _build_stream_id(settings.stream_prefix, end)
    stream_metadata = {
        "name": _build_stream_name(settings.name_prefix or settings.stream_prefix, end),
        "description": settings.description,
        "root_cause": settings.root_cause,
        "expected_action": settings.expected_action,
        "impacted_services": services,
        "category": settings.category,
    }

    database_path = ingest_stream_bundle(
        stream_id,
        telemetry,
        events,
        source=settings.source,
        environment=settings.environment,
        metadata=stream_metadata,
        db_path=settings.db_path,
    )

    ingest_summary: dict[str, Any] = {
        "stream_id": stream_id,
        "db_path": str(database_path),
        "source": settings.source,
        "environment": settings.environment,
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

    if settings.evaluate:
        metadata = ScenarioMetadata(
            name=stream_metadata["name"],
            description=stream_metadata["description"],
            root_cause=stream_metadata["root_cause"],
            expected_action=stream_metadata["expected_action"],
            impacted_services=stream_metadata["impacted_services"],
            category=stream_metadata["category"],
        )
        report = run_pipeline_from_streams(telemetry, events, metadata)
        save_stream_report(stream_id, metadata, report, db_path=settings.db_path)
        recommendation = report.recommendations[0] if report.recommendations else None
        ingest_summary["evaluation"] = {
            "evaluation_mode": report.evaluation.evaluation_mode,
            "incident_count": report.evaluation.incident_count,
            "anomaly_count": report.evaluation.anomaly_count,
            "recommended_action": recommendation.action if recommendation else None,
            "target_service": recommendation.target_service if recommendation else None,
            "latency_protection_pct": report.evaluation.latency_protection_pct,
            "avoided_overprovisioning_pct": report.evaluation.avoided_overprovisioning_pct,
            "baseline_win_rate_pct": report.evaluation.baseline_win_rate_pct,
        }

    prune_summary = None
    if settings.retention_older_than_days is not None or settings.retention_keep_latest is not None:
        prune_summary = prune_ingested_streams(
            older_than_days=settings.retention_older_than_days,
            keep_latest=settings.retention_keep_latest,
            environment=settings.environment,
            source=settings.source,
            vacuum=settings.retention_vacuum,
            dry_run=False,
            db_path=settings.db_path,
        )

    after = get_storage_stats(
        environment=settings.environment,
        source=settings.source,
        db_path=settings.db_path,
    )
    summary = {
        "before": before,
        "ingest": ingest_summary,
        "prune": prune_summary,
        "after": after,
    }

    if settings.summary_path:
        summary_path = Path(settings.summary_path)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        summary["summary_path"] = str(summary_path)

    return summary


def _load_config_payload(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    if suffix == ".toml":
        return tomllib.loads(path.read_text(encoding="utf-8"))
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Expected a JSON object in {path}.")
        return payload
    raise ValueError(f"Unsupported recurring config format '{suffix}'. Use .toml or .json.")


def _build_stream_id(prefix: str, end: str | Any) -> str:
    timestamp = parse_time_value(end if isinstance(end, str) else end.isoformat())
    safe_prefix = _slugify(prefix)
    return f"{safe_prefix}-{timestamp.strftime('%Y%m%dT%H%M%SZ')}"


def _build_stream_name(prefix: str, end: str | Any) -> str:
    timestamp = parse_time_value(end if isinstance(end, str) else end.isoformat())
    safe_prefix = _slugify(prefix)
    return f"{safe_prefix}-{timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC"


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    return normalized.strip("-") or "stream"
