from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .settings import _read_bool, _read_int, _read_optional, load_app_settings


@dataclass(slots=True)
class DeployBundleSettings:
    public_base_url: str
    deploy_site_address: str
    api_port: int
    db_path: str
    auth_enabled: bool
    rate_limit_enabled: bool
    rate_limit_backend: str
    tracing_enabled: bool
    recurring_enabled: bool
    recurring_config: str
    recurring_interval_seconds: int
    recurring_fail_delay_seconds: int
    recurring_summary_path: str | None


def load_deploy_bundle_settings(environ: Mapping[str, str] | None = None) -> DeployBundleSettings:
    env = environ or os.environ
    app_settings = load_app_settings(env)
    public_base_url = env.get("OPS_PLATFORM_PUBLIC_BASE_URL", "http://localhost").strip()
    if not public_base_url:
        raise ValueError("OPS_PLATFORM_PUBLIC_BASE_URL must not be blank.")

    settings = DeployBundleSettings(
        public_base_url=public_base_url,
        deploy_site_address=env.get("OPS_PLATFORM_DEPLOY_SITE_ADDRESS", ":80"),
        api_port=app_settings.api_port,
        db_path=app_settings.db_path,
        auth_enabled=app_settings.auth_enabled,
        rate_limit_enabled=app_settings.rate_limit_enabled,
        rate_limit_backend=app_settings.rate_limit_backend,
        tracing_enabled=app_settings.enable_tracing,
        recurring_enabled=_read_bool(env, "OPS_PLATFORM_RECURRING_ENABLED", True),
        recurring_config=env.get("OPS_PLATFORM_RECURRING_CONFIG", "/app/deploy/recurring_pull.toml"),
        recurring_interval_seconds=_read_int(env, "OPS_PLATFORM_RECURRING_INTERVAL_SECONDS", 300) or 300,
        recurring_fail_delay_seconds=_read_int(env, "OPS_PLATFORM_RECURRING_FAIL_DELAY_SECONDS", 30) or 30,
        recurring_summary_path=_read_optional(env, "OPS_PLATFORM_RECURRING_SUMMARY_PATH"),
    )
    if settings.recurring_interval_seconds <= 0:
        raise ValueError("OPS_PLATFORM_RECURRING_INTERVAL_SECONDS must be positive.")
    if settings.recurring_fail_delay_seconds <= 0:
        raise ValueError("OPS_PLATFORM_RECURRING_FAIL_DELAY_SECONDS must be positive.")
    return settings


def build_deploy_bundle_summary(
    settings: DeployBundleSettings,
    *,
    workspace_root: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(workspace_root) if workspace_root else None
    recurring_config_status = _resolve_config_presence(settings.recurring_config, root)
    return {
        "public_base_url": settings.public_base_url,
        "deploy_site_address": settings.deploy_site_address,
        "api": {
            "port": settings.api_port,
            "db_path": settings.db_path,
            "auth_enabled": settings.auth_enabled,
            "rate_limit_enabled": settings.rate_limit_enabled,
            "rate_limit_backend": settings.rate_limit_backend,
            "tracing_enabled": settings.tracing_enabled,
        },
        "worker": {
            "enabled": settings.recurring_enabled,
            "config_path": settings.recurring_config,
            "config_present_in_workspace": recurring_config_status,
            "interval_seconds": settings.recurring_interval_seconds,
            "fail_delay_seconds": settings.recurring_fail_delay_seconds,
            "summary_path": settings.recurring_summary_path,
        },
        "recommended_compose_file": "docker-compose.deploy.yml",
    }


def load_env_file(path: str | Path) -> dict[str, str]:
    env_path = Path(path)
    result: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def _resolve_config_presence(config_path: str, root: Path | None) -> bool | None:
    if not root:
        return None
    if config_path.startswith("/app/"):
        relative = config_path.replace("/app/", "", 1).replace("/", os.sep)
        return (root / Path(relative)).exists()
    path = Path(config_path)
    if path.is_absolute():
        return path.exists()
    return (root / path).exists()
