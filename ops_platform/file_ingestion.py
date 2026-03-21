from __future__ import annotations

import csv
import json
import tomllib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .schemas import ChangeEvent, MetricSample

DEFAULT_TELEMETRY_FIELDS = {
    "timestamp": "timestamp",
    "step": "step",
    "service": "service",
    "metric": "metric",
    "value": "value",
    "unit": "unit",
}

DEFAULT_EVENT_FIELDS = {
    "timestamp": "timestamp",
    "step": "step",
    "service": "service",
    "event_type": "event_type",
    "description": "description",
}

DEFAULT_UNITS = {
    "request_rate": "req/s",
    "p95_latency_ms": "ms",
    "error_rate_pct": "%",
    "queue_depth": "messages",
    "cpu_pct": "%",
    "memory_pct": "%",
}


@dataclass(slots=True)
class FileIngestionMapping:
    telemetry_fields: dict[str, str] = field(default_factory=lambda: DEFAULT_TELEMETRY_FIELDS.copy())
    event_fields: dict[str, str] = field(default_factory=lambda: DEFAULT_EVENT_FIELDS.copy())
    telemetry_dimensions: dict[str, str] = field(default_factory=dict)
    metric_aliases: dict[str, str] = field(default_factory=dict)
    service_aliases: dict[str, str] = field(default_factory=dict)
    unit_by_metric: dict[str, str] = field(default_factory=lambda: DEFAULT_UNITS.copy())
    timestamp_format: str | None = None
    step_seconds: int = 60
    default_event_type: str = "change"


def load_mapping_config(path: str | Path | None) -> FileIngestionMapping:
    mapping = FileIngestionMapping()
    if path is None:
        return mapping

    config_path = Path(path)
    payload = _load_config_payload(config_path)
    mapping.telemetry_fields.update(payload.get("telemetry_fields", {}))
    mapping.event_fields.update(payload.get("event_fields", {}))
    mapping.telemetry_dimensions.update(payload.get("telemetry_dimensions", {}))
    mapping.metric_aliases.update(payload.get("metric_aliases", {}))
    mapping.service_aliases.update(payload.get("service_aliases", {}))
    mapping.unit_by_metric.update(payload.get("unit_by_metric", {}))
    mapping.timestamp_format = payload.get("timestamp_format", mapping.timestamp_format)
    mapping.step_seconds = int(payload.get("step_seconds", mapping.step_seconds))
    mapping.default_event_type = str(payload.get("default_event_type", mapping.default_event_type))

    if mapping.step_seconds <= 0:
        raise ValueError("step_seconds must be positive.")
    return mapping


def load_file_bundle(
    telemetry_path: str | Path,
    *,
    events_path: str | Path | None = None,
    mapping_path: str | Path | None = None,
) -> tuple[list[MetricSample], list[ChangeEvent]]:
    mapping = load_mapping_config(mapping_path)
    telemetry_records = _read_records(telemetry_path)
    event_records = _read_records(events_path) if events_path else []
    origin = _infer_origin(telemetry_records, event_records, mapping)

    telemetry = _normalize_metric_records(telemetry_records, mapping, origin)
    events = _normalize_event_records(event_records, mapping, origin)
    return telemetry, events


def load_event_file(
    events_path: str | Path,
    *,
    mapping_path: str | Path | None = None,
    origin: datetime | None = None,
) -> list[ChangeEvent]:
    mapping = load_mapping_config(mapping_path)
    event_records = _read_records(events_path)
    event_origin = _normalize_datetime(origin) if origin else _infer_origin([], event_records, mapping)
    return _normalize_event_records(event_records, mapping, event_origin)


def _load_config_payload(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    if suffix == ".toml":
        return tomllib.loads(path.read_text(encoding="utf-8"))
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Expected a JSON object in {path}.")
        return payload
    raise ValueError(f"Unsupported mapping format '{suffix}'. Use .toml or .json.")


def _read_records(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []

    file_path = Path(path)
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]

    if suffix in {".jsonl", ".ndjson"}:
        records: list[dict[str, Any]] = []
        with file_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                payload = line.strip()
                if not payload:
                    continue
                record = json.loads(payload)
                if not isinstance(record, dict):
                    raise ValueError(f"Expected JSON object at {file_path}:{line_number}.")
                records.append(record)
        return records

    if suffix == ".json":
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            if not all(isinstance(item, dict) for item in payload):
                raise ValueError(f"Expected an array of JSON objects in {file_path}.")
            return payload
        if isinstance(payload, dict):
            if "records" in payload and isinstance(payload["records"], list):
                if not all(isinstance(item, dict) for item in payload["records"]):
                    raise ValueError(f"Expected 'records' to contain JSON objects in {file_path}.")
                return payload["records"]
        raise ValueError(f"Unsupported JSON payload in {file_path}.")

    raise ValueError(f"Unsupported data file '{file_path}'. Use CSV, JSON, or JSONL.")


def _infer_origin(
    telemetry_records: list[dict[str, Any]],
    event_records: list[dict[str, Any]],
    mapping: FileIngestionMapping,
) -> datetime:
    timestamps: list[datetime] = []
    telemetry_field = mapping.telemetry_fields["timestamp"]
    event_field = mapping.event_fields["timestamp"]

    for record in telemetry_records:
        timestamps.append(_parse_timestamp(_required_value(record, telemetry_field), mapping))
    for record in event_records:
        timestamps.append(_parse_timestamp(_required_value(record, event_field), mapping))

    if not timestamps:
        raise ValueError("At least one telemetry or event record is required.")
    return min(timestamps)


def _normalize_metric_records(
    records: list[dict[str, Any]],
    mapping: FileIngestionMapping,
    origin: datetime,
) -> list[MetricSample]:
    samples: list[MetricSample] = []
    seen: set[tuple[str, int, str, str]] = set()

    for record in records:
        timestamp = _parse_timestamp(_required_value(record, mapping.telemetry_fields["timestamp"]), mapping)
        service = _normalize_alias(_required_value(record, mapping.telemetry_fields["service"]), mapping.service_aliases)
        metric = _normalize_alias(_required_value(record, mapping.telemetry_fields["metric"]), mapping.metric_aliases)
        step = _resolve_step(record, mapping.telemetry_fields["step"], timestamp, origin, mapping.step_seconds)
        value = float(_required_value(record, mapping.telemetry_fields["value"]))
        unit = str(record.get(mapping.telemetry_fields["unit"], "") or mapping.unit_by_metric.get(metric, ""))
        dimensions = {
            name: str(record[source_field])
            for name, source_field in mapping.telemetry_dimensions.items()
            if source_field in record and record[source_field] not in {"", None}
        }
        key = (timestamp.isoformat(), step, service, metric)
        if key in seen:
            continue
        seen.add(key)
        samples.append(
            MetricSample(
                timestamp=timestamp,
                step=step,
                service=service,
                metric=metric,
                value=value,
                unit=unit,
                dimensions=dimensions,
            )
        )

    samples.sort(key=lambda sample: (sample.step, sample.timestamp, sample.service, sample.metric))
    return samples


def _normalize_event_records(
    records: list[dict[str, Any]],
    mapping: FileIngestionMapping,
    origin: datetime,
) -> list[ChangeEvent]:
    events: list[ChangeEvent] = []
    seen: set[tuple[str, int, str, str, str]] = set()

    for record in records:
        timestamp = _parse_timestamp(_required_value(record, mapping.event_fields["timestamp"]), mapping)
        service = _normalize_alias(_required_value(record, mapping.event_fields["service"]), mapping.service_aliases)
        step = _resolve_step(record, mapping.event_fields["step"], timestamp, origin, mapping.step_seconds)
        event_type = str(record.get(mapping.event_fields["event_type"], "") or mapping.default_event_type)
        description = str(_required_value(record, mapping.event_fields["description"]))
        key = (timestamp.isoformat(), step, service, event_type, description)
        if key in seen:
            continue
        seen.add(key)
        events.append(
            ChangeEvent(
                timestamp=timestamp,
                step=step,
                service=service,
                event_type=event_type,
                description=description,
            )
        )

    events.sort(key=lambda event: (event.step, event.timestamp, event.service, event.event_type))
    return events


def _resolve_step(
    record: dict[str, Any],
    step_field: str,
    timestamp: datetime,
    origin: datetime,
    step_seconds: int,
) -> int:
    raw_value = record.get(step_field)
    if raw_value not in {"", None}:
        return int(raw_value)
    return int((timestamp - origin).total_seconds() // step_seconds)


def _parse_timestamp(value: Any, mapping: FileIngestionMapping) -> datetime:
    if isinstance(value, datetime):
        parsed_datetime = value if value.tzinfo else _assume_local_timezone(value)
        return _normalize_datetime(parsed_datetime)

    if isinstance(value, (int, float)):
        return _normalize_datetime(datetime.fromtimestamp(float(value), tz=timezone.utc))

    raw = str(value).strip()
    if not raw:
        raise ValueError("Timestamp value is empty.")

    if raw.replace(".", "", 1).isdigit():
        return _normalize_datetime(datetime.fromtimestamp(float(raw), tz=timezone.utc))

    normalized = raw.replace("Z", "+00:00")
    try:
        parsed_datetime = datetime.fromisoformat(normalized)
        if parsed_datetime.tzinfo is None:
            parsed_datetime = _assume_local_timezone(parsed_datetime)
        return _normalize_datetime(parsed_datetime)
    except ValueError:
        if mapping.timestamp_format is None:
            raise
        parsed_datetime = datetime.strptime(raw, mapping.timestamp_format)
        return _normalize_datetime(_assume_local_timezone(parsed_datetime))


def _normalize_alias(value: Any, aliases: dict[str, str]) -> str:
    normalized = str(value).strip()
    return aliases.get(normalized, normalized)


def _required_value(record: dict[str, Any], field_name: str) -> Any:
    if field_name not in record or record[field_name] in {"", None}:
        raise ValueError(f"Required field '{field_name}' is missing from record: {record}")
    return record[field_name]


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _assume_local_timezone(value: datetime) -> datetime:
    local_timezone = datetime.now().astimezone().tzinfo or timezone.utc
    return value.replace(tzinfo=local_timezone)
