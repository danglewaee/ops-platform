from __future__ import annotations

from contextlib import closing
import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .schemas import ChangeEvent, MetricSample, PipelineReport, ScenarioMetadata
from .timescale_storage import (
    compact_timescale_storage,
    get_storage_stats_timescale,
    ingest_stream_bundle_timescale,
    is_timescale_target,
    list_ingested_streams_timescale,
    load_ingested_stream_timescale,
    prune_ingested_streams_timescale,
    save_stream_report_timescale,
)

RUNS_DIR = Path(__file__).resolve().parents[1] / "runs"
SQLITE_DB_PATH = Path(__file__).resolve().parents[1] / "artifacts" / "ops_platform.sqlite3"


def save_run_bundle(
    telemetry: list[MetricSample],
    events: list[ChangeEvent],
    metadata: ScenarioMetadata,
    report: PipelineReport,
    *,
    seed: int,
    output_dir: Path | None = None,
) -> Path:
    target_dir = output_dir or RUNS_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    path = target_dir / f"{metadata.name}-{timestamp}.json"
    payload = {
        "saved_at": datetime.now().isoformat(),
        "seed": seed,
        "metadata": asdict(metadata),
        "telemetry": [asdict(sample) for sample in telemetry],
        "events": [asdict(event) for event in events],
        "report": report.to_dict(),
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def load_run_bundle(path: str | Path) -> dict[str, Any]:
    run_path = Path(path)
    payload = json.loads(run_path.read_text(encoding="utf-8"))
    telemetry = [MetricSample.from_dict(item) for item in payload["telemetry"]]
    events = [ChangeEvent.from_dict(item) for item in payload["events"]]
    metadata = ScenarioMetadata.from_dict(payload["metadata"])
    report = PipelineReport.from_dict(payload["report"])
    return {
        "path": str(run_path),
        "saved_at": payload.get("saved_at"),
        "seed": payload.get("seed"),
        "metadata": metadata,
        "telemetry": telemetry,
        "events": events,
        "report": report,
    }


def list_saved_runs(output_dir: Path | None = None) -> list[dict[str, Any]]:
    target_dir = output_dir or RUNS_DIR
    if not target_dir.exists():
        return []

    runs: list[dict[str, Any]] = []
    for path in sorted(target_dir.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        metadata = payload.get("metadata", {})
        report = payload.get("report", {})
        evaluation = report.get("evaluation", {})
        runs.append(
            {
                "path": str(path),
                "saved_at": payload.get("saved_at"),
                "seed": payload.get("seed"),
                "scenario": metadata.get("name"),
                "category": metadata.get("category"),
                "root_cause": metadata.get("root_cause"),
                "expected_action": metadata.get("expected_action"),
                "recommended_action": (
                    report.get("recommendations", [{}])[0].get("action")
                    if report.get("recommendations")
                    else None
                ),
                "recommended_action_match": evaluation.get("recommended_action_match"),
            }
        )
    return runs


def ensure_sqlite_schema(db_path: str | Path | None = None) -> Path:
    database_path = Path(db_path) if db_path else SQLITE_DB_PATH
    database_path.parent.mkdir(parents=True, exist_ok=True)

    with closing(sqlite3.connect(database_path)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS streams (
                stream_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                source TEXT NOT NULL,
                environment TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS metric_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id TEXT NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
                timestamp TEXT NOT NULL,
                step INTEGER NOT NULL,
                service TEXT NOT NULL,
                metric TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                dimensions_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(stream_id, timestamp, step, service, metric)
            );

            CREATE TABLE IF NOT EXISTS change_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id TEXT NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
                timestamp TEXT NOT NULL,
                step INTEGER NOT NULL,
                service TEXT NOT NULL,
                event_type TEXT NOT NULL,
                description TEXT NOT NULL,
                UNIQUE(stream_id, timestamp, step, service, event_type, description)
            );

            CREATE TABLE IF NOT EXISTS pipeline_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id TEXT NOT NULL REFERENCES streams(stream_id) ON DELETE CASCADE,
                saved_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                report_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_metric_samples_stream_step
            ON metric_samples(stream_id, step, timestamp);

            CREATE INDEX IF NOT EXISTS idx_change_events_stream_step
            ON change_events(stream_id, step, timestamp);

            CREATE INDEX IF NOT EXISTS idx_pipeline_reports_stream_id
            ON pipeline_reports(stream_id, id DESC);
            """
        )
        connection.commit()

    return database_path


def ingest_stream_bundle(
    stream_id: str,
    telemetry: list[MetricSample],
    events: list[ChangeEvent],
    *,
    source: str = "api",
    environment: str = "production",
    metadata: dict[str, Any] | ScenarioMetadata | None = None,
    db_path: str | Path | None = None,
) -> str | Path:
    if is_timescale_target(db_path):
        return ingest_stream_bundle_timescale(
            stream_id,
            telemetry,
            events,
            source=source,
            environment=environment,
            metadata=metadata,
            db_path=db_path,
        )

    database_path = ensure_sqlite_schema(db_path)
    created_at = datetime.now().isoformat()
    metadata_payload = asdict(metadata) if isinstance(metadata, ScenarioMetadata) else (metadata or {})

    with closing(sqlite3.connect(database_path)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO streams(stream_id, created_at, source, environment, metadata_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(stream_id) DO UPDATE SET
                source = excluded.source,
                environment = excluded.environment,
                metadata_json = excluded.metadata_json
            """,
            (stream_id, created_at, source, environment, json.dumps(metadata_payload)),
        )
        connection.executemany(
            """
            INSERT OR IGNORE INTO metric_samples(
                stream_id, timestamp, step, service, metric, value, unit, dimensions_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    stream_id,
                    sample.timestamp.isoformat(),
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
        connection.executemany(
            """
            INSERT OR IGNORE INTO change_events(
                stream_id, timestamp, step, service, event_type, description
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    stream_id,
                    event.timestamp.isoformat(),
                    event.step,
                    event.service,
                    event.event_type,
                    event.description,
                )
                for event in events
            ],
        )
        connection.commit()

    return database_path


def load_ingested_stream(
    stream_id: str,
    *,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    if is_timescale_target(db_path):
        return load_ingested_stream_timescale(stream_id, db_path=db_path)

    database_path = ensure_sqlite_schema(db_path)

    with closing(sqlite3.connect(database_path)) as connection:
        connection.row_factory = sqlite3.Row
        stream_row = connection.execute(
            "SELECT stream_id, created_at, source, environment, metadata_json FROM streams WHERE stream_id = ?",
            (stream_id,),
        ).fetchone()
        if stream_row is None:
            raise FileNotFoundError(f"Unknown stream_id '{stream_id}' in {database_path}.")

        metric_rows = connection.execute(
            """
            SELECT timestamp, step, service, metric, value, unit, dimensions_json
            FROM metric_samples
            WHERE stream_id = ?
            ORDER BY step, timestamp, service, metric
            """,
            (stream_id,),
        ).fetchall()
        event_rows = connection.execute(
            """
            SELECT timestamp, step, service, event_type, description
            FROM change_events
            WHERE stream_id = ?
            ORDER BY step, timestamp, service, event_type
            """,
            (stream_id,),
        ).fetchall()
        latest_report_row = connection.execute(
            """
            SELECT saved_at, metadata_json, report_json
            FROM pipeline_reports
            WHERE stream_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (stream_id,),
        ).fetchone()

    telemetry = [
        MetricSample.from_dict(
            {
                "timestamp": row["timestamp"],
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
                "timestamp": row["timestamp"],
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
            "saved_at": latest_report_row["saved_at"],
            "metadata": json.loads(latest_report_row["metadata_json"]),
            "report": PipelineReport.from_dict(json.loads(latest_report_row["report_json"])),
        }

    return {
        "stream_id": stream_row["stream_id"],
        "created_at": stream_row["created_at"],
        "source": stream_row["source"],
        "environment": stream_row["environment"],
        "metadata": json.loads(stream_row["metadata_json"]),
        "telemetry": telemetry,
        "events": events,
        "latest_report": latest_report,
    }


def save_stream_report(
    stream_id: str,
    metadata: ScenarioMetadata,
    report: PipelineReport,
    *,
    db_path: str | Path | None = None,
) -> str | Path:
    if is_timescale_target(db_path):
        return save_stream_report_timescale(stream_id, metadata, report, db_path=db_path)

    database_path = ensure_sqlite_schema(db_path)

    with closing(sqlite3.connect(database_path)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        exists = connection.execute(
            "SELECT 1 FROM streams WHERE stream_id = ?",
            (stream_id,),
        ).fetchone()
        if exists is None:
            raise FileNotFoundError(f"Unknown stream_id '{stream_id}' in {database_path}.")

        connection.execute(
            """
            INSERT INTO pipeline_reports(stream_id, saved_at, metadata_json, report_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                stream_id,
                datetime.now().isoformat(),
                json.dumps(asdict(metadata)),
                json.dumps(report.to_dict(), default=str),
            ),
        )
        connection.commit()

    return database_path


def list_ingested_streams(
    *,
    environment: str | None = None,
    source: str | None = None,
    created_after: str | datetime | None = None,
    created_before: str | datetime | None = None,
    limit: int | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    if is_timescale_target(db_path):
        return list_ingested_streams_timescale(
            environment=environment,
            source=source,
            created_after=created_after,
            created_before=created_before,
            limit=limit,
            db_path=db_path,
        )

    database_path = ensure_sqlite_schema(db_path)
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
        limit_clause = " LIMIT ?"
        params.append(limit)

    with closing(sqlite3.connect(database_path)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            f"""
            SELECT
                s.stream_id,
                s.created_at,
                s.source,
                s.environment,
                s.metadata_json,
                (SELECT COUNT(*) FROM metric_samples m WHERE m.stream_id = s.stream_id) AS metric_count,
                (SELECT COUNT(*) FROM change_events e WHERE e.stream_id = s.stream_id) AS event_count,
                (SELECT COUNT(*) FROM pipeline_reports r WHERE r.stream_id = s.stream_id) AS report_count,
                (SELECT MIN(timestamp) FROM metric_samples m WHERE m.stream_id = s.stream_id) AS first_metric_at,
                (SELECT MAX(timestamp) FROM metric_samples m WHERE m.stream_id = s.stream_id) AS last_metric_at,
                (
                    SELECT report_json
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
        ).fetchall()

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
                "created_at": row["created_at"],
                "source": row["source"],
                "environment": row["environment"],
                "metadata": json.loads(row["metadata_json"]),
                "metric_count": row["metric_count"],
                "event_count": row["event_count"],
                "report_count": row["report_count"],
                "first_metric_at": row["first_metric_at"],
                "last_metric_at": row["last_metric_at"],
                "latest_recommended_action": latest_recommended_action,
                "latest_evaluation_mode": evaluation_mode,
            }
        )

    return streams


def get_storage_stats(
    *,
    environment: str | None = None,
    source: str | None = None,
    created_after: str | datetime | None = None,
    created_before: str | datetime | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    if is_timescale_target(db_path):
        return get_storage_stats_timescale(
            environment=environment,
            source=source,
            created_after=created_after,
            created_before=created_before,
            db_path=db_path,
        )

    database_path = ensure_sqlite_schema(db_path)
    where_clause, params = _build_stream_filters(
        environment=environment,
        source=source,
        created_after=created_after,
        created_before=created_before,
    )

    with closing(sqlite3.connect(database_path)) as connection:
        connection.row_factory = sqlite3.Row
        stream_summary = connection.execute(
            f"""
            SELECT
                COUNT(*) AS stream_count,
                MIN(s.created_at) AS first_stream_at,
                MAX(s.created_at) AS last_stream_at
            FROM streams s
            {where_clause}
            """,
            params,
        ).fetchone()
        metric_count = connection.execute(
            f"""
            SELECT COUNT(*) AS count
            FROM metric_samples m
            JOIN streams s ON s.stream_id = m.stream_id
            {where_clause}
            """,
            params,
        ).fetchone()["count"]
        event_count = connection.execute(
            f"""
            SELECT COUNT(*) AS count
            FROM change_events e
            JOIN streams s ON s.stream_id = e.stream_id
            {where_clause}
            """,
            params,
        ).fetchone()["count"]
        report_count = connection.execute(
            f"""
            SELECT COUNT(*) AS count
            FROM pipeline_reports r
            JOIN streams s ON s.stream_id = r.stream_id
            {where_clause}
            """,
            params,
        ).fetchone()["count"]

    db_file_size_bytes = database_path.stat().st_size if database_path.exists() else 0
    return {
        "db_path": str(database_path),
        "db_file_size_bytes": db_file_size_bytes,
        "stream_count": stream_summary["stream_count"],
        "metric_sample_count": metric_count,
        "event_count": event_count,
        "report_count": report_count,
        "first_stream_at": stream_summary["first_stream_at"],
        "last_stream_at": stream_summary["last_stream_at"],
        "filters": {
            "environment": environment,
            "source": source,
            "created_after": _datetime_to_string(created_after),
            "created_before": _datetime_to_string(created_before),
        },
    }


def prune_ingested_streams(
    *,
    older_than_days: int | None = None,
    keep_latest: int | None = None,
    environment: str | None = None,
    source: str | None = None,
    vacuum: bool = False,
    dry_run: bool = False,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    if is_timescale_target(db_path):
        return prune_ingested_streams_timescale(
            older_than_days=older_than_days,
            keep_latest=keep_latest,
            environment=environment,
            source=source,
            vacuum=vacuum,
            dry_run=dry_run,
            db_path=db_path,
        )

    if older_than_days is None and keep_latest is None:
        raise ValueError("Provide older_than_days and/or keep_latest for pruning.")
    if older_than_days is not None and older_than_days < 0:
        raise ValueError("older_than_days must be non-negative.")
    if keep_latest is not None and keep_latest < 0:
        raise ValueError("keep_latest must be non-negative.")

    database_path = ensure_sqlite_schema(db_path)
    filtered_streams = list_ingested_streams(
        environment=environment,
        source=source,
        db_path=database_path,
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
        "db_path": str(database_path),
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
            compact_storage(db_path=database_path)
        return summary

    with closing(sqlite3.connect(database_path)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executemany(
            "DELETE FROM streams WHERE stream_id = ?",
            [(stream_id,) for stream_id in deleted_stream_ids],
        )
        connection.commit()

    if vacuum:
        compact_storage(db_path=database_path)

    return summary


def compact_storage(
    *,
    db_path: str | Path | None = None,
) -> str | Path:
    if is_timescale_target(db_path):
        return compact_timescale_storage(db_path=db_path)

    database_path = ensure_sqlite_schema(db_path)
    with closing(sqlite3.connect(database_path)) as connection:
        connection.execute("VACUUM")
    return database_path


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
        clauses.append("s.environment = ?")
        params.append(environment)
    if source:
        clauses.append("s.source = ?")
        params.append(source)
    if created_after is not None:
        clauses.append("s.created_at >= ?")
        params.append(_normalize_filter_datetime(created_after).isoformat())
    if created_before is not None:
        clauses.append("s.created_at <= ?")
        params.append(_normalize_filter_datetime(created_before).isoformat())

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
