from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

from .storage import SQLITE_DB_PATH

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


@dataclass(slots=True)
class AppSettings:
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    db_path: str = str(SQLITE_DB_PATH)
    auto_init_storage: bool = True
    auth_enabled: bool = False
    api_keys: tuple[str, ...] = ()
    auth_header_name: str = "x-api-key"
    actor_header_name: str = "x-ops-actor"
    rate_limit_enabled: bool = False
    rate_limit_backend: str = "memory"
    rate_limit_requests: int = 60
    rate_limit_window_seconds: int = 60
    redis_url: str | None = None
    redis_key_prefix: str = "ops-platform:rate-limit"
    audit_log_enabled: bool = True
    db_retry_attempts: int = 3
    db_retry_backoff_seconds: float = 0.5
    db_retry_max_backoff_seconds: float = 5.0
    enable_tracing: bool = False
    otlp_endpoint: str | None = None
    otel_service_name: str = "ops-decision-platform-api"
    timescale_metric_retention_days: int | None = None
    timescale_event_retention_days: int | None = None
    timescale_compress_after_days: int | None = None
    timescale_create_metric_rollup: bool = False
    timescale_aggregate_bucket: str = "5 minutes"
    timescale_aggregate_name: str = "metric_samples_5m"
    timescale_refresh_start_offset: str = "30 days"
    timescale_refresh_end_offset: str = "5 minutes"
    timescale_refresh_schedule_interval: str = "5 minutes"


def load_app_settings(environ: Mapping[str, str] | None = None) -> AppSettings:
    env = environ or os.environ
    api_keys = _read_csv(env, "OPS_PLATFORM_API_KEYS")
    settings = AppSettings(
        api_host=env.get("OPS_PLATFORM_API_HOST", "0.0.0.0"),
        api_port=_read_int(env, "OPS_PLATFORM_API_PORT", 8000) or 8000,
        db_path=env.get("OPS_PLATFORM_DB_PATH", str(SQLITE_DB_PATH)),
        auto_init_storage=_read_bool(env, "OPS_PLATFORM_AUTO_INIT_STORAGE", True),
        auth_enabled=_read_bool(env, "OPS_PLATFORM_AUTH_ENABLED", bool(api_keys)),
        api_keys=api_keys,
        auth_header_name=env.get("OPS_PLATFORM_AUTH_HEADER_NAME", "x-api-key"),
        actor_header_name=env.get("OPS_PLATFORM_ACTOR_HEADER_NAME", "x-ops-actor"),
        rate_limit_enabled=_read_bool(env, "OPS_PLATFORM_RATE_LIMIT_ENABLED", False),
        rate_limit_backend=env.get("OPS_PLATFORM_RATE_LIMIT_BACKEND", "memory"),
        rate_limit_requests=_read_int(env, "OPS_PLATFORM_RATE_LIMIT_REQUESTS", 60) or 60,
        rate_limit_window_seconds=_read_int(env, "OPS_PLATFORM_RATE_LIMIT_WINDOW_SECONDS", 60) or 60,
        redis_url=_read_optional(env, "OPS_PLATFORM_REDIS_URL"),
        redis_key_prefix=env.get("OPS_PLATFORM_REDIS_KEY_PREFIX", "ops-platform:rate-limit"),
        audit_log_enabled=_read_bool(env, "OPS_PLATFORM_AUDIT_LOG_ENABLED", True),
        db_retry_attempts=_read_int(env, "OPS_PLATFORM_DB_RETRY_ATTEMPTS", 3) or 3,
        db_retry_backoff_seconds=_read_float(env, "OPS_PLATFORM_DB_RETRY_BACKOFF_SECONDS", 0.5) or 0.5,
        db_retry_max_backoff_seconds=_read_float(env, "OPS_PLATFORM_DB_RETRY_MAX_BACKOFF_SECONDS", 5.0) or 5.0,
        enable_tracing=_read_bool(env, "OPS_PLATFORM_ENABLE_TRACING", False),
        otlp_endpoint=_read_optional(env, "OPS_PLATFORM_OTLP_ENDPOINT"),
        otel_service_name=env.get("OPS_PLATFORM_OTEL_SERVICE_NAME", "ops-decision-platform-api"),
        timescale_metric_retention_days=_read_int(env, "OPS_PLATFORM_TIMESCALE_METRIC_RETENTION_DAYS"),
        timescale_event_retention_days=_read_int(env, "OPS_PLATFORM_TIMESCALE_EVENT_RETENTION_DAYS"),
        timescale_compress_after_days=_read_int(env, "OPS_PLATFORM_TIMESCALE_COMPRESS_AFTER_DAYS"),
        timescale_create_metric_rollup=_read_bool(
            env,
            "OPS_PLATFORM_TIMESCALE_CREATE_METRIC_ROLLUP",
            False,
        ),
        timescale_aggregate_bucket=env.get("OPS_PLATFORM_TIMESCALE_AGGREGATE_BUCKET", "5 minutes"),
        timescale_aggregate_name=env.get("OPS_PLATFORM_TIMESCALE_AGGREGATE_NAME", "metric_samples_5m"),
        timescale_refresh_start_offset=env.get(
            "OPS_PLATFORM_TIMESCALE_REFRESH_START_OFFSET",
            "30 days",
        ),
        timescale_refresh_end_offset=env.get(
            "OPS_PLATFORM_TIMESCALE_REFRESH_END_OFFSET",
            "5 minutes",
        ),
        timescale_refresh_schedule_interval=env.get(
            "OPS_PLATFORM_TIMESCALE_REFRESH_SCHEDULE_INTERVAL",
            "5 minutes",
        ),
    )
    if settings.auth_enabled and not settings.api_keys:
        raise ValueError("OPS_PLATFORM_AUTH_ENABLED requires OPS_PLATFORM_API_KEYS.")
    if settings.rate_limit_backend not in {"memory", "redis"}:
        raise ValueError("OPS_PLATFORM_RATE_LIMIT_BACKEND must be 'memory' or 'redis'.")
    if settings.rate_limit_enabled and settings.rate_limit_backend == "redis" and not settings.redis_url:
        raise ValueError("OPS_PLATFORM_RATE_LIMIT_BACKEND=redis requires OPS_PLATFORM_REDIS_URL.")
    if settings.rate_limit_requests <= 0:
        raise ValueError("OPS_PLATFORM_RATE_LIMIT_REQUESTS must be positive.")
    if settings.rate_limit_window_seconds <= 0:
        raise ValueError("OPS_PLATFORM_RATE_LIMIT_WINDOW_SECONDS must be positive.")
    if settings.db_retry_attempts <= 0:
        raise ValueError("OPS_PLATFORM_DB_RETRY_ATTEMPTS must be positive.")
    if settings.db_retry_backoff_seconds < 0 or settings.db_retry_max_backoff_seconds < 0:
        raise ValueError("DB retry backoff values must be non-negative.")
    return settings


def _read_bool(environ: Mapping[str, str], key: str, default: bool) -> bool:
    raw = _read_optional(environ, key)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"Environment variable {key} must be a boolean string.")


def _read_int(environ: Mapping[str, str], key: str, default: int | None = None) -> int | None:
    raw = _read_optional(environ, key)
    if raw is None:
        return default
    return int(raw)


def _read_float(environ: Mapping[str, str], key: str, default: float | None = None) -> float | None:
    raw = _read_optional(environ, key)
    if raw is None:
        return default
    return float(raw)


def _read_optional(environ: Mapping[str, str], key: str) -> str | None:
    raw = environ.get(key)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _read_csv(environ: Mapping[str, str], key: str) -> tuple[str, ...]:
    raw = _read_optional(environ, key)
    if raw is None:
        return ()
    return tuple(item.strip() for item in raw.split(",") if item.strip())
