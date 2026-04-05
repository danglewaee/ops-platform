from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.request import Request, urlopen

from .deploy_bundle import load_deploy_bundle_settings, load_env_file
from .settings import load_app_settings


@dataclass(slots=True)
class DeployEvidenceSettings:
    base_url: str
    auth_enabled: bool
    auth_header_name: str
    actor_header_name: str
    api_key: str | None
    actor: str
    timeout_seconds: int
    worker_summary_path: str | None


def load_deploy_evidence_settings(
    environ: Mapping[str, str] | None = None,
    *,
    base_url: str | None = None,
    api_key: str | None = None,
    actor: str = "deploy-evidence",
    timeout_seconds: int = 5,
) -> DeployEvidenceSettings:
    env = environ or {}
    deploy_settings = load_deploy_bundle_settings(env)
    app_settings = load_app_settings(env)
    resolved_base_url = (base_url or deploy_settings.public_base_url).strip().rstrip("/")
    if not resolved_base_url:
        raise ValueError("Deploy evidence requires a non-empty base URL.")

    resolved_api_key = api_key or (app_settings.api_keys[0] if app_settings.api_keys else None)
    if app_settings.auth_enabled and not resolved_api_key:
        raise ValueError("Authenticated deploy evidence capture requires an API key.")
    if timeout_seconds <= 0:
        raise ValueError("Deploy evidence timeout must be positive.")

    return DeployEvidenceSettings(
        base_url=resolved_base_url,
        auth_enabled=app_settings.auth_enabled,
        auth_header_name=app_settings.auth_header_name,
        actor_header_name=app_settings.actor_header_name,
        api_key=resolved_api_key,
        actor=actor,
        timeout_seconds=timeout_seconds,
        worker_summary_path=deploy_settings.recurring_summary_path,
    )


def build_request_headers(settings: DeployEvidenceSettings) -> dict[str, str]:
    headers = {"accept": "application/json"}
    if settings.auth_enabled and settings.api_key:
        headers[settings.auth_header_name] = settings.api_key
        headers[settings.actor_header_name] = settings.actor
    return headers


def request_json(url: str, *, headers: dict[str, str] | None = None, timeout_seconds: int = 5) -> Any:
    request = Request(url, headers=headers or {})
    with urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def capture_deploy_evidence(
    settings: DeployEvidenceSettings,
    *,
    output_dir: str | Path,
    workspace_root: str | Path | None = None,
    request_json_fn: Callable[..., Any] = request_json,
    captured_at: str | None = None,
) -> dict[str, Any]:
    root = Path(workspace_root) if workspace_root else None
    evidence_dir = Path(output_dir)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    headers = build_request_headers(settings)
    timestamp = captured_at or datetime.now(timezone.utc).isoformat()
    checks: dict[str, Any] = {}
    payloads: dict[str, Any] = {}
    failures: list[dict[str, str]] = []

    for name, relative_url in (
        ("health", "/health"),
        ("ready", "/ready"),
        ("streams", "/streams?limit=5"),
        ("storage_stats", "/storage/stats"),
        ("audit_events", "/audit/events?limit=5"),
    ):
        url = f"{settings.base_url}{relative_url}"
        try:
            payload = request_json_fn(url, headers=headers, timeout_seconds=settings.timeout_seconds)
            artifact_path = evidence_dir / f"{name}.json"
            _write_json(artifact_path, payload)
            payloads[name] = payload
            checks[name] = {
                "status": "ok",
                "url": url,
                "artifact_path": str(artifact_path),
            }
        except Exception as exc:  # pragma: no cover - exercised through tests with mixed failures
            failures.append({"check": name, "error": str(exc)})
            checks[name] = {
                "status": "error",
                "url": url,
                "error": str(exc),
            }

    worker_summary = load_worker_summary(settings.worker_summary_path, workspace_root=root)
    if worker_summary["status"] == "ok":
        worker_artifact_path = evidence_dir / "recurring_worker_summary.json"
        _write_json(worker_artifact_path, worker_summary["payload"])
        worker_summary["artifact_path"] = str(worker_artifact_path)
    checks["worker_summary"] = {
        key: value for key, value in worker_summary.items() if key != "payload"
    }

    summary = {
        "captured_at": timestamp,
        "base_url": settings.base_url,
        "overall_status": "ok" if not failures else "error",
        "auth_enabled": settings.auth_enabled,
        "actor": settings.actor if settings.auth_enabled else None,
        "ready": _resolve_ready_value(payloads.get("ready")),
        "backend": _resolve_backend_value(payloads.get("ready")),
        "health_status": _resolve_health_value(payloads.get("health")),
        "stream_count": _resolve_list_count(payloads.get("streams")),
        "audit_event_count": _resolve_list_count(payloads.get("audit_events")),
        "storage_stats_available": isinstance(payloads.get("storage_stats"), dict),
        "worker_summary_available": worker_summary["status"] == "ok",
        "checks": checks,
        "failures": failures,
    }

    summary_path = evidence_dir / "deploy_evidence_summary.json"
    markdown_path = evidence_dir / "deploy_evidence_summary.md"
    summary["summary_path"] = str(summary_path)
    summary["summary_markdown_path"] = str(markdown_path)
    _write_json(summary_path, summary)
    markdown_path.write_text(render_evidence_markdown(summary), encoding="utf-8")
    return summary


def load_worker_summary(summary_path: str | None, *, workspace_root: str | Path | None = None) -> dict[str, Any]:
    if not summary_path:
        return {"status": "not_configured"}

    path = resolve_workspace_path(summary_path, workspace_root=workspace_root)
    if path is None:
        return {"status": "not_configured"}
    if not path.exists():
        return {"status": "missing", "resolved_path": str(path)}

    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        "status": "ok",
        "resolved_path": str(path),
        "payload": payload,
    }


def resolve_workspace_path(path: str | None, *, workspace_root: str | Path | None = None) -> Path | None:
    if not path:
        return None

    root = Path(workspace_root) if workspace_root else None
    if path.startswith("/app/"):
        if not root:
            return Path(path)
        relative_path = path.replace("/app/", "", 1)
        return root / Path(relative_path)

    candidate = Path(path)
    if candidate.is_absolute() or not root:
        return candidate
    return root / candidate


def render_evidence_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Deploy Evidence Summary",
        "",
        f"- captured_at: `{summary['captured_at']}`",
        f"- base_url: `{summary['base_url']}`",
        f"- overall_status: `{summary['overall_status']}`",
        f"- ready: `{summary['ready']}`",
        f"- backend: `{summary['backend']}`",
        f"- health_status: `{summary['health_status']}`",
        f"- stream_count: `{summary['stream_count']}`",
        f"- audit_event_count: `{summary['audit_event_count']}`",
        f"- worker_summary_available: `{summary['worker_summary_available']}`",
        "",
        "## Checks",
        "",
    ]
    for name, payload in summary["checks"].items():
        if payload["status"] == "ok":
            artifact = payload.get("artifact_path", "n/a")
            lines.append(f"- `{name}`: ok (`{artifact}`)")
        else:
            detail = payload.get("error") or payload.get("resolved_path") or "unavailable"
            lines.append(f"- `{name}`: {payload['status']} (`{detail}`)")
    if summary["failures"]:
        lines.extend(["", "## Failures", ""])
        for failure in summary["failures"]:
            lines.append(f"- `{failure['check']}`: {failure['error']}")
    return "\n".join(lines) + "\n"


def load_evidence_environment(env_file: str | Path | None = None, *, environ: Mapping[str, str] | None = None) -> dict[str, str]:
    payload = dict(environ or {})
    if env_file:
        env_path = Path(env_file)
        if env_path.exists():
            payload.update(load_env_file(env_path))
    return payload


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _resolve_ready_value(payload: Any) -> bool | None:
    if isinstance(payload, dict):
        return payload.get("ready")
    return None


def _resolve_backend_value(payload: Any) -> str | None:
    if isinstance(payload, dict):
        return payload.get("backend")
    return None


def _resolve_health_value(payload: Any) -> str | None:
    if isinstance(payload, dict):
        return payload.get("status")
    return None


def _resolve_list_count(payload: Any) -> int | None:
    if isinstance(payload, list):
        return len(payload)
    return None
