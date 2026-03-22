from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
from typing import Any

from .schemas import ChangeEvent, MetricSample, PipelineReport, ScenarioMetadata

TIMESCALE_SCHEMES = ("postgresql://", "postgres://", "timescaledb://")


def is_timescale_target(db_path: str | Path | None) -> bool:
    if db_path is None or isinstance(db_path, Path):
        return False
    normalized = str(db_path).lower()
    return normalized.startswith(TIMESCALE_SCHEMES)


def normalize_timescale_dsn(db_path: str | Path) -> str:
    value = str(db_path)
    if value.lower().startswith("timescaledb://"):
        return "postgresql://" + value[len("timescaledb://") :]
    return value


def ensure_timescale_schema(db_path: str | Path) -> str:
    psycopg, _ = _require_psycopg()
    database_url = normalize_timescale_dsn(db_path)

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS streams (
                    stream_id TEXT PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL,
                    source TEXT NOT NULL,
                    environment TEXT NOT NULL,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS metric_samples (
                    id BIGSERIAL PRIMARY KEY,
                    stream_id TEXT NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
                    timestamp TIMESTAMPTZ NOT NULL,
                    step INTEGER NOT NULL,
                    service TEXT NOT NULL,
                    metric TEXT NOT NULL,
                    value DOUBLE PRECISION NOT NULL,
                    unit TEXT NOT NULL,
                    dimensions_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    UNIQUE(stream_id, timestamp, step, service, metric)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS change_events (
                    id BIGSERIAL PRIMARY KEY,
                    stream_id TEXT NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
                    timestamp TIMESTAMPTZ NOT NULL,
                    step INTEGER NOT NULL,
                    service TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    description TEXT NOT NULL,
                    UNIQUE(stream_id, timestamp, step, service, event_type, description)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_reports (
                    id BIGSERIAL PRIMARY KEY,
                    stream_id TEXT NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
                    saved_at TIMESTAMPTZ NOT NULL,
                    metadata_json JSONB NOT NULL,
                    report_json JSONB NOT NULL
                )
                """
            )
            cursor.execute(
                """
                SELECT create_hypertable(
                    'metric_samples',
                    by_range('timestamp'),
                    if_not_exists => TRUE,
                    migrate_data => TRUE
                )
                """
            )
            cursor.execute(
                """
                SELECT create_hypertable(
                    'change_events',
                    by_range('timestamp'),
                    if_not_exists => TRUE,
                    migrate_data => TRUE
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_metric_samples_stream_step
                ON metric_samples(stream_id, step, timestamp DESC)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_change_events_stream_step
                ON change_events(stream_id, step, timestamp DESC)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pipeline_reports_stream_saved_at
                ON pipeline_reports(stream_id, saved_at DESC)
                """
            )
        connection.commit()

    return database_url


def configure_timescale_features(
    db_path: str | Path,
    *,
    metric_retention_days: int | None = None,
    event_retention_days: int | None = None,
    compress_after_days: int | None = None,
    create_continuous_aggregate: bool = False,
    aggregate_bucket: str = "5 minutes",
    aggregate_name: str = "metric_samples_5m",
    refresh_start_offset: str = "30 days",
    refresh_end_offset: str = "5 minutes",
    refresh_schedule_interval: str = "5 minutes",
) -> dict[str, Any]:
    psycopg, _ = _require_psycopg()
    database_url = ensure_timescale_schema(db_path)
    aggregate_name = _validate_identifier(aggregate_name)
    bucket_literal = _validate_interval_literal(aggregate_bucket)
    refresh_start_literal = _validate_interval_literal(refresh_start_offset)
    refresh_end_literal = _validate_interval_literal(refresh_end_offset)
    refresh_schedule_literal = _validate_interval_literal(refresh_schedule_interval)

    actions: dict[str, Any] = {
        "db_path": database_url,
        "metric_retention_days": metric_retention_days,
        "event_retention_days": event_retention_days,
        "compress_after_days": compress_after_days,
        "continuous_aggregate": aggregate_name if create_continuous_aggregate else None,
    }

    with psycopg.connect(database_url, autocommit=True) as connection:
        with connection.cursor() as cursor:
            if compress_after_days is not None:
                cursor.execute(
                    """
                    ALTER TABLE metric_samples SET (
                        timescaledb.compress,
                        timescaledb.compress_orderby = 'timestamp DESC',
                        timescaledb.compress_segmentby = 'stream_id,service,metric'
                    )
                    """
                )
                cursor.execute(
                    """
                    SELECT add_compression_policy(
                        'metric_samples',
                        compress_after => %s::interval,
                        if_not_exists => TRUE
                    )
                    """,
                    (f"{compress_after_days} days",),
                )

            if metric_retention_days is not None:
                cursor.execute(
                    """
                    SELECT add_retention_policy(
                        'metric_samples',
                        drop_after => %s::interval,
                        if_not_exists => TRUE
                    )
                    """,
                    (f"{metric_retention_days} days",),
                )

            if event_retention_days is not None:
                cursor.execute(
                    """
                    SELECT add_retention_policy(
                        'change_events',
                        drop_after => %s::interval,
                        if_not_exists => TRUE
                    )
                    """,
                    (f"{event_retention_days} days",),
                )

            if create_continuous_aggregate:
                cursor.execute(
                    f"""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS {aggregate_name}
                    WITH (timescaledb.continuous) AS
                    SELECT
                        time_bucket(INTERVAL '{bucket_literal}', timestamp) AS bucket,
                        stream_id,
                        service,
                        metric,
                        AVG(value) AS avg_value,
                        MAX(value) AS max_value,
                        MIN(value) AS min_value,
                        COUNT(*) AS sample_count
                    FROM metric_samples
                    GROUP BY bucket, stream_id, service, metric
                    WITH NO DATA
                    """
                )
                cursor.execute(
                    f"""
                    SELECT add_continuous_aggregate_policy(
                        '{aggregate_name}',
                        start_offset => INTERVAL '{refresh_start_literal}',
                        end_offset => INTERVAL '{refresh_end_literal}',
                        schedule_interval => INTERVAL '{refresh_schedule_literal}',
                        if_not_exists => TRUE
                    )
                    """
                )

    return actions


def ingest_stream_bundle_timescale(
    stream_id: str,
    telemetry: list[MetricSample],
    events: list[ChangeEvent],
    *,
    source: str = "api",
    environment: str = "production",
    metadata: dict[str, Any] | ScenarioMetadata | None = None,
    db_path: str | Path,
) -> str:
    psycopg, _ = _require_psycopg()
    database_url = ensure_timescale_schema(db_path)
    metadata_payload = asdict(metadata) if isinstance(metadata, ScenarioMetadata) else (metadata or {})
    created_at = datetime.now(timezone.utc)

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO streams(stream_id, created_at, source, environment, metadata_json)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT(stream_id) DO UPDATE SET
                    source = excluded.source,
                    environment = excluded.environment,
                    metadata_json = excluded.metadata_json
                """,
                (stream_id, created_at, source, environment, json.dumps(metadata_payload)),
            )
            cursor.executemany(
                """
                INSERT INTO metric_samples(
                    stream_id, timestamp, step, service, metric, value, unit, dimensions_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT DO NOTHING
                """,
                [
                    (
                        stream_id,
                        _ensure_aware_datetime(sample.timestamp),
                        sample.step,
                        sample.service,
                        sample.metric,
                        sample.value,
                        sample.unit,
                        json.dumps(sample.dimensions),
                    )
                    for sample in telemetry
                ],
            )
            cursor.executemany(
                """
                INSERT INTO change_events(
                    stream_id, timestamp, step, service, event_type, description
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                [
                    (
                        stream_id,
                        _ensure_aware_datetime(event.timestamp),
                        event.step,
                        event.service,
                        event.event_type,
                        event.description,
                    )
                    for event in events
                ],
            )
        connection.commit()

    return database_url


def load_ingested_stream_timescale(
    stream_id: str,
    *,
    db_path: str | Path,
) -> dict[str, Any]:
    psycopg, dict_row = _require_psycopg()
    database_url = ensure_timescale_schema(db_path)

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    stream_id,
                    created_at,
                    source,
                    environment,
                    metadata_json::text AS metadata_json
                FROM streams
                WHERE stream_id = %s
                """,
                (stream_id,),
            )
            stream_row = cursor.fetchone()
            if stream_row is None:
                raise FileNotFoundError(f"Unknown stream_id '{stream_id}' in {database_url}.")

            cursor.execute(
                """
                SELECT
                    timestamp,
                    step,
                    service,
                    metric,
                    value,
                    unit,
                    dimensions_json::text AS dimensions_json
                FROM metric_samples
                WHERE stream_id = %s
                ORDER BY step, timestamp, service, metric
                """,
                (stream_id,),
            )
            metric_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    timestamp,
                    step,
                    service,
                    event_type,
                    description
                FROM change_events
                WHERE stream_id = %s
                ORDER BY step, timestamp, service, event_type
                """,
                (stream_id,),
            )
            event_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    saved_at,
                    metadata_json::text AS metadata_json,
                    report_json::text AS report_json
                FROM pipeline_reports
                WHERE stream_id = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (stream_id,),
            )
            latest_report_row = cursor.fetchone()

    telemetry = [
        MetricSample.from_dict(
            {
                "timestamp": row["timestamp"].isoformat(),
                "step": row["step"],
                "service": row["service"],
                "metric": row["metric"],
                "value": row["value"],
                "unit": row["unit"],
                "dimensions": json.loads(row["dimensions_json"]),
            }
        )
        for row in metric_rows
    ]
    events = [
        ChangeEvent.from_dict(
            {
                "timestamp": row["timestamp"].isoformat(),
                "step": row["step"],
                "service": row["service"],
                "event_type": row["event_type"],
                "description": row["description"],
            }
        )
        for row in event_rows
    ]

    latest_report = None
    if latest_report_row is not None:
        latest_report = {
            "saved_at": latest_report_row["saved_at"].isoformat(),
            "metadata": json.loads(latest_report_row["metadata_json"]),
            "report": PipelineReport.from_dict(json.loads(latest_report_row["report_json"])),
        }

    return {
        "stream_id": stream_row["stream_id"],
        "created_at": stream_row["created_at"].isoformat(),
        "source": stream_row["source"],
        "environment": stream_row["environment"],
        "metadata": json.loads(stream_row["metadata_json"]),
        "telemetry": telemetry,
        "events": events,
        "latest_report": latest_report,
    }


def save_stream_report_timescale(
    stream_id: str,
    metadata: ScenarioMetadata,
    report: PipelineReport,
    *,
    db_path: str | Path,
) -> str:
    psycopg, _ = _require_psycopg()
    database_url = ensure_timescale_schema(db_path)

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM streams WHERE stream_id = %s", (stream_id,))
            if cursor.fetchone() is None:
                raise FileNotFoundError(f"Unknown stream_id '{stream_id}' in {database_url}.")

            cursor.execute(
                """
                INSERT INTO pipeline_reports(stream_id, saved_at, metadata_json, report_json)
                VALUES (%s, %s, %s::jsonb, %s::jsonb)
                """,
                (
                    stream_id,
                    datetime.now(timezone.utc),
                    json.dumps(asdict(metadata)),
                    json.dumps(report.to_dict(), default=str),
                ),
            )
        connection.commit()

    return database_url


def list_ingested_streams_timescale(
    *,
    environment: str | None = None,
    source: str | None = None,
    created_after: str | datetime | None = None,
    created_before: str | datetime | None = None,
    limit: int | None = None,
    db_path: str | Path,
) -> list[dict[str, Any]]:
    psycopg, dict_row = _require_psycopg()
    database_url = ensure_timescale_schema(db_path)
    where_clause, params = _build_stream_filters(
        environment=environment,
        source=source,
        created_after=created_after,
        created_before=created_before,
    )
    limit_clause = ""
    if limit is not None:
        if limit <= 0:
            raise ValueError("limit must be positive.")
        limit_clause = " LIMIT %s"
        params.append(limit)

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    s.stream_id,
                    s.created_at,
                    s.source,
                    s.environment,
                    s.metadata_json::text AS metadata_json,
                    (SELECT COUNT(*) FROM metric_samples m WHERE m.stream_id = s.stream_id) AS metric_count,
                    (SELECT COUNT(*) FROM change_events e WHERE e.stream_id = s.stream_id) AS event_count,
                    (SELECT COUNT(*) FROM pipeline_reports r WHERE r.stream_id = s.stream_id) AS report_count,
                    (SELECT MIN(timestamp) FROM metric_samples m WHERE m.stream_id = s.stream_id) AS first_metric_at,
                    (SELECT MAX(timestamp) FROM metric_samples m WHERE m.stream_id = s.stream_id) AS last_metric_at,
                    (
                        SELECT report_json::text
                        FROM pipeline_reports r
                        WHERE r.stream_id = s.stream_id
                        ORDER BY r.id DESC
                        LIMIT 1
                    ) AS latest_report_json
                FROM streams s
                {where_clause}
                ORDER BY s.created_at DESC
                {limit_clause}
                """,
                params,
            )
            rows = cursor.fetchall()

    streams: list[dict[str, Any]] = []
    for row in rows:
        latest_recommended_action = None
        evaluation_mode = None
        if row["latest_report_json"]:
            report_payload = json.loads(row["latest_report_json"])
            recommendations = report_payload.get("recommendations", [])
            evaluation = report_payload.get("evaluation", {})
            latest_recommended_action = recommendations[0]["action"] if recommendations else None
            evaluation_mode = evaluation.get("evaluation_mode")

        streams.append(
            {
                "stream_id": row["stream_id"],
                "created_at": row["created_at"].isoformat(),
                "source": row["source"],
                "environment": row["environment"],
                "metadata": json.loads(row["metadata_json"]),
                "metric_count": row["metric_count"],
                "event_count": row["event_count"],
                "report_count": row["report_count"],
                "first_metric_at": row["first_metric_at"].isoformat() if row["first_metric_at"] else None,
                "last_metric_at": row["last_metric_at"].isoformat() if row["last_metric_at"] else None,
                "latest_recommended_action": latest_recommended_action,
                "latest_evaluation_mode": evaluation_mode,
            }
        )

    return streams


def get_storage_stats_timescale(
    *,
    environment: str | None = None,
    source: str | None = None,
    created_after: str | datetime | None = None,
    created_before: str | datetime | None = None,
    db_path: str | Path,
) -> dict[str, Any]:
    psycopg, dict_row = _require_psycopg()
    database_url = ensure_timescale_schema(db_path)
    where_clause, params = _build_stream_filters(
        environment=environment,
        source=source,
        created_after=created_after,
        created_before=created_before,
    )

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    COUNT(*) AS stream_count,
                    MIN(s.created_at) AS first_stream_at,
                    MAX(s.created_at) AS last_stream_at
                FROM streams s
                {where_clause}
                """,
                params,
            )
            stream_summary = cursor.fetchone()
            cursor.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM metric_samples m
                JOIN streams s ON s.stream_id = m.stream_id
                {where_clause}
                """,
                params,
            )
            metric_count = cursor.fetchone()["count"]
            cursor.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM change_events e
                JOIN streams s ON s.stream_id = e.stream_id
                {where_clause}
                """,
                params,
            )
            event_count = cursor.fetchone()["count"]
            cursor.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM pipeline_reports r
                JOIN streams s ON s.stream_id = r.stream_id
                {where_clause}
                """,
                params,
            )
            report_count = cursor.fetchone()["count"]
            cursor.execute("SELECT pg_database_size(current_database()) AS size_bytes")
            db_size_row = cursor.fetchone()

    return {
        "db_path": database_url,
        "db_file_size_bytes": db_size_row["size_bytes"],
        "stream_count": stream_summary["stream_count"],
        "metric_sample_count": metric_count,
        "event_count": event_count,
        "report_count": report_count,
        "first_stream_at": stream_summary["first_stream_at"].isoformat() if stream_summary["first_stream_at"] else None,
        "last_stream_at": stream_summary["last_stream_at"].isoformat() if stream_summary["last_stream_at"] else None,
        "filters": {
            "environment": environment,
            "source": source,
            "created_after": _datetime_to_string(created_after),
            "created_before": _datetime_to_string(created_before),
        },
    }


def prune_ingested_streams_timescale(
    *,
    older_than_days: int | None = None,
    keep_latest: int | None = None,
    environment: str | None = None,
    source: str | None = None,
    vacuum: bool = False,
    dry_run: bool = False,
    db_path: str | Path,
) -> dict[str, Any]:
    if older_than_days is None and keep_latest is None:
        raise ValueError("Provide older_than_days and/or keep_latest for pruning.")
    if older_than_days is not None and older_than_days < 0:
        raise ValueError("older_than_days must be non-negative.")
    if keep_latest is not None and keep_latest < 0:
        raise ValueError("keep_latest must be non-negative.")

    database_url = ensure_timescale_schema(db_path)
    filtered_streams = list_ingested_streams_timescale(
        environment=environment,
        source=source,
        db_path=database_url,
    )
    keep_ids: set[str] = set()
    if keep_latest is not None:
        keep_ids = {stream["stream_id"] for stream in filtered_streams[:keep_latest]}

    cutoff = None
    if older_than_days is not None:
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=older_than_days)

    candidates: list[dict[str, Any]] = []
    for stream in filtered_streams:
        created_at = _normalize_filter_datetime(stream["created_at"])
        delete_due_to_age = cutoff is not None and created_at < cutoff
        delete_due_to_limit = keep_latest is not None and stream["stream_id"] not in keep_ids
        if delete_due_to_age or delete_due_to_limit:
            candidates.append(stream)

    deleted_stream_ids = [stream["stream_id"] for stream in candidates]
    summary = {
        "db_path": database_url,
        "dry_run": dry_run,
        "vacuum": vacuum,
        "older_than_days": older_than_days,
        "keep_latest": keep_latest,
        "environment": environment,
        "source": source,
        "deleted_stream_count": len(candidates),
        "deleted_metric_sample_count": sum(int(stream["metric_count"]) for stream in candidates),
        "deleted_event_count": sum(int(stream["event_count"]) for stream in candidates),
        "deleted_report_count": sum(int(stream["report_count"]) for stream in candidates),
        "deleted_stream_ids": deleted_stream_ids,
    }

    if dry_run or not candidates:
        if vacuum and not dry_run and not candidates:
            compact_timescale_storage(db_path=database_url)
        return summary

    psycopg, _ = _require_psycopg()
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM streams WHERE stream_id = ANY(%s)", (deleted_stream_ids,))
        connection.commit()

    if vacuum:
        compact_timescale_storage(db_path=database_url)

    return summary


def compact_timescale_storage(
    *,
    db_path: str | Path,
) -> str:
    psycopg, _ = _require_psycopg()
    database_url = ensure_timescale_schema(db_path)
    with psycopg.connect(database_url, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("VACUUM (ANALYZE)")
    return database_url


def _require_psycopg():
    try:  # pragma: no cover - optional dependency import
        import psycopg
        from psycopg.rows import dict_row
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency import
        raise RuntimeError(
            "TimescaleDB support requires psycopg. Install it with `pip install -e .[timeseries]` first."
        ) from exc
    return psycopg, dict_row


def _build_stream_filters(
    *,
    environment: str | None,
    source: str | None,
    created_after: str | datetime | None,
    created_before: str | datetime | None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if environment:
        clauses.append("s.environment = %s")
        params.append(environment)
    if source:
        clauses.append("s.source = %s")
        params.append(source)
    if created_after is not None:
        clauses.append("s.created_at >= %s")
        params.append(_ensure_aware_datetime(_normalize_filter_datetime(created_after)))
    if created_before is not None:
        clauses.append("s.created_at <= %s")
        params.append(_ensure_aware_datetime(_normalize_filter_datetime(created_before)))

    if not clauses:
        return "", params
    return "WHERE " + " AND ".join(clauses), params


def _normalize_filter_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    normalized = str(value).strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _datetime_to_string(value: str | datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return value.isoformat()


def _ensure_aware_datetime(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc)
    return value.replace(tzinfo=timezone.utc)


def _validate_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid Timescale identifier '{value}'.")
    return value


def _validate_interval_literal(value: str) -> str:
    normalized = value.strip()
    if not normalized or not re.fullmatch(r"[0-9A-Za-z_. :+-]+", normalized):
        raise ValueError(f"Invalid interval literal '{value}'.")
    return normalized
