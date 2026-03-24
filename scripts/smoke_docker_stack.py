from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bring up the docker-compose stack, probe readiness, and tear it back down."
    )
    parser.add_argument("--compose-file", default="docker-compose.yml", help="Path to the Compose file.")
    parser.add_argument("--env-file", default=".env", help="Path to the environment file used by Compose.")
    parser.add_argument("--project-name", default="ops-platform-smoke", help="Compose project name for the smoke run.")
    parser.add_argument("--timeout-seconds", type=int, default=180, help="How long to wait for /ready.")
    parser.add_argument("--skip-build", action="store_true", help="Skip docker compose --build during startup.")
    parser.add_argument("--keep-up", action="store_true", help="Leave the stack running after the smoke check.")
    parser.add_argument("--summary-path", help="Optional path to write the smoke summary JSON.")
    args = parser.parse_args()

    summary = run_smoke_check(
        compose_file=Path(args.compose_file),
        env_file=Path(args.env_file),
        project_name=args.project_name,
        timeout_seconds=args.timeout_seconds,
        build=not args.skip_build,
        keep_up=args.keep_up,
        summary_path=Path(args.summary_path) if args.summary_path else None,
    )
    print(json.dumps(summary, indent=2))
    return 0


def run_smoke_check(
    *,
    compose_file: Path,
    env_file: Path,
    project_name: str,
    timeout_seconds: int = 180,
    build: bool = True,
    keep_up: bool = False,
    summary_path: Path | None = None,
) -> dict[str, Any]:
    env = load_env_file(env_file)
    compose_command = build_compose_base_command(
        compose_file=compose_file,
        env_file=env_file,
        project_name=project_name,
    )
    base_url = env.get("OPS_PLATFORM_SMOKE_BASE_URL") or f"http://127.0.0.1:{env.get('OPS_PLATFORM_API_PORT', '8000')}"
    headers = resolve_api_headers(env)

    up_command = [*compose_command, "up", "-d"]
    if build:
        up_command.append("--build")

    subprocess.run(up_command, cwd=ROOT, check=True)

    summary: dict[str, Any] = {
        "compose_file": str(compose_file),
        "env_file": str(env_file),
        "project_name": project_name,
        "base_url": base_url,
        "build": build,
        "keep_up": keep_up,
    }

    try:
        ready = poll_json(f"{base_url}/ready", timeout_seconds=timeout_seconds)
        if not ready.get("ready"):
            raise RuntimeError(f"Stack reported not ready: {ready}")

        health = request_json(f"{base_url}/health")
        streams = request_json(f"{base_url}/streams", headers=headers)
        audit_events = request_json(f"{base_url}/audit/events?limit=5", headers=headers)

        summary["ready"] = ready
        summary["health"] = health
        summary["stream_count"] = len(streams)
        summary["audit_event_count"] = len(audit_events)
        summary["status"] = "ok"
        if summary_path is not None:
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
            summary["summary_path"] = str(summary_path)
        return summary
    finally:
        if not keep_up:
            subprocess.run([*compose_command, "down", "-v"], cwd=ROOT, check=True)


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    payload: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        payload[key.strip()] = value.strip()
    return payload


def resolve_api_headers(env: dict[str, str]) -> dict[str, str]:
    auth_enabled = env.get("OPS_PLATFORM_AUTH_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
    if not auth_enabled:
        return {}

    api_key = env.get("OPS_PLATFORM_API_KEYS", "").split(",")[0].strip()
    if not api_key:
        raise ValueError("OPS_PLATFORM_AUTH_ENABLED requires OPS_PLATFORM_API_KEYS for smoke checks.")

    auth_header = env.get("OPS_PLATFORM_AUTH_HEADER_NAME", "x-api-key")
    actor_header = env.get("OPS_PLATFORM_ACTOR_HEADER_NAME", "x-ops-actor")
    return {
        auth_header: api_key,
        actor_header: "smoke-check",
    }


def build_compose_base_command(
    *,
    compose_file: Path,
    env_file: Path,
    project_name: str,
) -> list[str]:
    command = ["docker", "compose", "-f", str(compose_file), "-p", project_name]
    if env_file.exists():
        command.extend(["--env-file", str(env_file)])
    return command


def poll_json(url: str, *, headers: dict[str, str] | None = None, timeout_seconds: int = 180) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            return request_json(url, headers=headers)
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            last_error = exc
            time.sleep(1.0)
    raise TimeoutError(f"Timed out waiting for {url}: {last_error}")


def request_json(url: str, *, headers: dict[str, str] | None = None) -> dict[str, Any] | list[dict[str, Any]]:
    request = Request(url, headers=headers or {})
    with urlopen(request, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


if __name__ == "__main__":
    raise SystemExit(main())
