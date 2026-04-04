from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from .recurring_pull import load_recurring_pull_settings, run_recurring_pull
from .settings import _read_bool, _read_int, _read_optional


@dataclass(slots=True)
class RecurringWorkerSettings:
    enabled: bool = True
    config_path: str = "/app/deploy/recurring_pull.toml"
    interval_seconds: int = 300
    fail_delay_seconds: int = 30
    summary_path: str | None = None
    run_once: bool = False


def load_recurring_worker_settings(
    environ: Mapping[str, str] | None = None,
    *,
    config_path: str | None = None,
    interval_seconds: int | None = None,
    fail_delay_seconds: int | None = None,
    summary_path: str | None = None,
    run_once: bool | None = None,
    enabled: bool | None = None,
) -> RecurringWorkerSettings:
    env = environ or os.environ
    settings = RecurringWorkerSettings(
        enabled=enabled if enabled is not None else _read_bool(env, "OPS_PLATFORM_RECURRING_ENABLED", True),
        config_path=config_path or env.get("OPS_PLATFORM_RECURRING_CONFIG", "/app/deploy/recurring_pull.toml"),
        interval_seconds=interval_seconds if interval_seconds is not None else (_read_int(env, "OPS_PLATFORM_RECURRING_INTERVAL_SECONDS", 300) or 300),
        fail_delay_seconds=fail_delay_seconds if fail_delay_seconds is not None else (_read_int(env, "OPS_PLATFORM_RECURRING_FAIL_DELAY_SECONDS", 30) or 30),
        summary_path=summary_path if summary_path is not None else _read_optional(env, "OPS_PLATFORM_RECURRING_SUMMARY_PATH"),
        run_once=run_once if run_once is not None else _read_bool(env, "OPS_PLATFORM_RECURRING_RUN_ONCE", False),
    )
    if settings.interval_seconds <= 0:
        raise ValueError("OPS_PLATFORM_RECURRING_INTERVAL_SECONDS must be positive.")
    if settings.fail_delay_seconds <= 0:
        raise ValueError("OPS_PLATFORM_RECURRING_FAIL_DELAY_SECONDS must be positive.")
    return settings


def run_worker_cycle(settings: RecurringWorkerSettings) -> dict[str, Any]:
    recurring_settings = load_recurring_pull_settings(
        settings.config_path,
        summary_path=settings.summary_path,
    )
    return run_recurring_pull(recurring_settings)


def run_recurring_worker(
    settings: RecurringWorkerSettings,
    *,
    iterations: int | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    if not settings.enabled:
        return {"status": "disabled", "config_path": settings.config_path}

    runs = 0
    last_result: dict[str, Any] = {"status": "idle"}
    while True:
        try:
            summary = run_worker_cycle(settings)
            last_result = {"status": "ok", "summary": summary}
            delay_seconds = settings.interval_seconds
        except Exception as exc:
            last_result = {"status": "error", "error": str(exc), "config_path": settings.config_path}
            delay_seconds = settings.fail_delay_seconds

        runs += 1
        if settings.run_once or (iterations is not None and runs >= iterations):
            return last_result
        sleep_fn(delay_seconds)
