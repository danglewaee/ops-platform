from __future__ import annotations

import json
import math
import tomllib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .file_ingestion import DEFAULT_UNITS, load_event_file
from .resilience import RetryPolicy, retry_call
from .schemas import ChangeEvent, MetricSample


@dataclass(slots=True)
class PrometheusIngestionConfig:
    base_url: str
    queries: dict[str, str]
    step: str = "60s"
    timeout_seconds: int = 30
    service_label: str = "service"
    headers: dict[str, str] = field(default_factory=dict)
    service_aliases: dict[str, str] = field(default_factory=dict)
    unit_by_metric: dict[str, str] = field(default_factory=lambda: DEFAULT_UNITS.copy())
    retry_attempts: int = 3
    retry_backoff_seconds: float = 0.5
    retry_max_backoff_seconds: float = 5.0


def load_prometheus_config(path: str | Path, *, base_url: str | None = None) -> PrometheusIngestionConfig:
    config_path = Path(path)
    payload = _load_config_payload(config_path)
    queries = payload.get("queries", {})
    if not isinstance(queries, dict) or not queries:
        raise ValueError(f"Prometheus config {config_path} must define a non-empty [queries] table.")

    resolved_base_url = base_url or payload.get("base_url")
    if not resolved_base_url:
        raise ValueError(f"Prometheus config {config_path} is missing base_url.")

    return PrometheusIngestionConfig(
        base_url=str(resolved_base_url).rstrip("/"),
        queries={str(metric): str(query) for metric, query in queries.items()},
        step=str(payload.get("step", "60s")),
        timeout_seconds=int(payload.get("timeout_seconds", 30)),
        service_label=str(payload.get("service_label", "service")),
        headers={str(key): str(value) for key, value in payload.get("headers", {}).items()},
        service_aliases={str(key): str(value) for key, value in payload.get("service_aliases", {}).items()},
        unit_by_metric={**DEFAULT_UNITS, **{str(key): str(value) for key, value in payload.get("unit_by_metric", {}).items()}},
        retry_attempts=int(payload.get("retry_attempts", 3)),
        retry_backoff_seconds=float(payload.get("retry_backoff_seconds", 0.5)),
        retry_max_backoff_seconds=float(payload.get("retry_max_backoff_seconds", 5.0)),
    )


def fetch_prometheus_metrics(
    config: PrometheusIngestionConfig,
    *,
    start: datetime,
    end: datetime,
) -> list[MetricSample]:
    start_dt = _normalize_datetime(start)
    end_dt = _normalize_datetime(end)
    if end_dt < start_dt:
        raise ValueError("Prometheus end time must be greater than or equal to start time.")

    step_seconds = _parse_step_seconds(config.step)
    samples_by_key: dict[tuple[str, int, str, str], MetricSample] = {}

    for metric_name, query in config.queries.items():
        payload = _run_query_range(config, query=query, start=start_dt, end=end_dt)
        result_type = payload.get("data", {}).get("resultType")
        if result_type != "matrix":
            raise ValueError(f"Expected Prometheus matrix result for metric '{metric_name}', got '{result_type}'.")

        for series in payload.get("data", {}).get("result", []):
            labels = series.get("metric", {})
            service = _resolve_service(labels, config.service_label, config.service_aliases)
            dimensions = {
                str(key): str(value)
                for key, value in labels.items()
                if key not in {config.service_label, "__name__"}
            }
            for timestamp_raw, value_raw in series.get("values", []):
                value = float(value_raw)
                if not math.isfinite(value):
                    continue
                timestamp = _normalize_datetime(datetime.fromtimestamp(float(timestamp_raw), tz=timezone.utc))
                step = int((timestamp - start_dt).total_seconds() // step_seconds)
                key = (timestamp.isoformat(), step, service, metric_name)
                samples_by_key[key] = MetricSample(
                    timestamp=timestamp,
                    step=step,
                    service=service,
                    metric=metric_name,
                    value=value,
                    unit=config.unit_by_metric.get(metric_name, ""),
                    dimensions=dimensions,
                )

    samples = list(samples_by_key.values())
    samples.sort(key=lambda sample: (sample.step, sample.timestamp, sample.service, sample.metric))
    return samples


def load_prometheus_bundle(
    config_path: str | Path,
    *,
    start: str | datetime | None,
    end: str | datetime | None,
    lookback_minutes: int | None = None,
    base_url: str | None = None,
    events_path: str | Path | None = None,
    event_mapping_path: str | Path | None = None,
) -> tuple[PrometheusIngestionConfig, list[MetricSample], list[ChangeEvent], datetime, datetime]:
    config = load_prometheus_config(config_path, base_url=base_url)
    start_dt, end_dt = resolve_prometheus_window(start=start, end=end, lookback_minutes=lookback_minutes)
    telemetry = fetch_prometheus_metrics(config, start=start_dt, end=end_dt)
    if not telemetry:
        raise ValueError("Prometheus query returned no metric samples for the configured range.")

    origin = min(sample.timestamp for sample in telemetry)
    events = load_event_file(events_path, mapping_path=event_mapping_path, origin=origin) if events_path else []
    return config, telemetry, events, start_dt, end_dt


def resolve_prometheus_window(
    *,
    start: str | datetime | None,
    end: str | datetime | None,
    lookback_minutes: int | None,
) -> tuple[datetime, datetime]:
    if lookback_minutes is not None:
        if lookback_minutes <= 0:
            raise ValueError("lookback_minutes must be positive.")
        end_dt = parse_time_value(end) if end is not None else datetime.now(timezone.utc).replace(tzinfo=None)
        start_dt = end_dt - timedelta(minutes=lookback_minutes)
        return start_dt, end_dt

    if start is None or end is None:
        raise ValueError("Provide start and end, or set lookback_minutes.")

    start_dt = parse_time_value(start)
    end_dt = parse_time_value(end)
    if end_dt < start_dt:
        raise ValueError("Prometheus end time must be greater than or equal to start time.")
    return start_dt, end_dt


def parse_time_value(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    raw = value.strip()
    if raw.replace(".", "", 1).isdigit():
        return datetime.fromtimestamp(float(raw), tz=timezone.utc).astimezone(timezone.utc).replace(tzinfo=None)

    normalized = raw.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        local_timezone = datetime.now().astimezone().tzinfo or timezone.utc
        parsed = parsed.replace(tzinfo=local_timezone)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _run_query_range(
    config: PrometheusIngestionConfig,
    *,
    query: str,
    start: datetime,
    end: datetime,
) -> dict[str, Any]:
    params = urlencode(
        {
            "query": query,
            "start": start.isoformat() + "Z",
            "end": end.isoformat() + "Z",
            "step": config.step,
        }
    )
    request = Request(
        f"{config.base_url}/api/v1/query_range?{params}",
        headers=config.headers,
    )
    payload = retry_call(
        lambda: _urlopen_json(request, timeout_seconds=config.timeout_seconds),
        policy=RetryPolicy(
            attempts=config.retry_attempts,
            backoff_seconds=config.retry_backoff_seconds,
            max_backoff_seconds=config.retry_max_backoff_seconds,
        ),
        retry_exceptions=(OSError, TimeoutError),
    )

    if payload.get("status") != "success":
        raise ValueError(f"Prometheus query failed: {payload}")
    return payload


def _urlopen_json(request: Request, *, timeout_seconds: int) -> dict[str, Any]:
    with urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _load_config_payload(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    if suffix == ".toml":
        return tomllib.loads(path.read_text(encoding="utf-8"))
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Expected a JSON object in {path}.")
        return payload
    raise ValueError(f"Unsupported Prometheus config format '{suffix}'. Use .toml or .json.")


def _parse_step_seconds(value: str) -> int:
    raw = value.strip().lower()
    if raw.isdigit():
        return int(raw)
    units = {"s": 1, "m": 60, "h": 3600}
    unit = raw[-1]
    if unit not in units or not raw[:-1].isdigit():
        raise ValueError(f"Unsupported Prometheus step '{value}'. Use formats like 60s, 5m, or 1h.")
    return int(raw[:-1]) * units[unit]


def _resolve_service(labels: dict[str, Any], service_label: str, aliases: dict[str, str]) -> str:
    raw_service = str(labels.get(service_label, "")).strip()
    if not raw_service:
        raise ValueError(f"Prometheus series is missing required service label '{service_label}': {labels}")
    return aliases.get(raw_service, raw_service)


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)
