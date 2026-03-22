from __future__ import annotations

import argparse
import json
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.dashboard import (
    build_bundle,
    build_bundles,
    build_live_stream_bundles,
    build_live_summary_payload,
    render_dashboard,
)
from ops_platform.scenarios import list_scenarios


def _parse_optional_int(value: str | None, *, name: str) -> int | None:
    if value in {None, ""}:
        return None
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be positive.")
    return parsed


def resolve_live_filters(params: dict[str, list[str]], *, default_limit: int) -> dict[str, object]:
    return {
        "limit": _parse_optional_int(params.get("limit", [None])[0], name="limit") or default_limit,
        "environment": params.get("environment", [None])[0] or None,
        "source": params.get("source", [None])[0] or None,
    }


def build_dashboard_html(
    *,
    limit: int,
    environment: str | None,
    source: str | None,
    db_path: str | None,
) -> str:
    live_payload = build_live_summary_payload(
        limit=limit,
        environment=environment,
        source=source,
        db_path=db_path,
    )
    live_bundles = [
        {"summary": bundle["summary"], "visual": bundle["visual"]}
        for bundle in build_live_stream_bundles(
            limit=limit,
            environment=environment,
            source=source,
            db_path=db_path,
        )
    ]
    return render_dashboard(build_bundles(), live_bundles=live_bundles, live_stats=live_payload["stats"])


def build_live_payload(
    *,
    limit: int,
    environment: str | None,
    source: str | None,
    db_path: str | None,
) -> dict[str, object]:
    return build_live_summary_payload(
        limit=limit,
        environment=environment,
        source=source,
        db_path=db_path,
    )


class DashboardHandler(BaseHTTPRequestHandler):
    db_path: str | None = None
    default_live_limit = 4

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        try:
            live_filters = resolve_live_filters(params, default_limit=self.default_live_limit)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        if path in {"/", "/dashboard"}:
            self._send_html(build_dashboard_html(db_path=self.db_path, **live_filters))
            return

        if path == "/api/summary":
            bundles = build_bundles()
            payload = [bundle["summary"] for bundle in bundles]
            self._send_json(payload)
            return

        if path == "/api/live-summary":
            payload = build_live_payload(db_path=self.db_path, **live_filters)
            self._send_json(payload)
            return

        if path == "/api/live-streams":
            payload = build_live_payload(db_path=self.db_path, **live_filters)["streams"]
            self._send_json(payload)
            return

        if path == "/api/scenario":
            name = params.get("name", ["traffic_spike"])[0]
            if name not in list_scenarios():
                self._send_json({"error": f"unknown scenario: {name}"}, status=HTTPStatus.NOT_FOUND)
                return
            payload = build_bundle(name)["summary"]
            self._send_json(payload)
            return

        self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _send_html(self, body: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_json(self, data: object, status: HTTPStatus = HTTPStatus.OK) -> None:
        payload = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve the Ops Decision Platform dashboard locally.")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind.")
    parser.add_argument("--port", type=int, default=8008, help="Port to bind.")
    parser.add_argument(
        "--db-path",
        default=None,
        help="SQLite path or Timescale/PostgreSQL DSN for persisted live streams. Defaults to artifacts/ops_platform.sqlite3.",
    )
    parser.add_argument(
        "--live-limit",
        type=int,
        default=4,
        help="How many recent persisted streams to surface in the live dashboard by default.",
    )
    args = parser.parse_args()

    if args.live_limit <= 0:
        raise SystemExit("--live-limit must be positive.")

    DashboardHandler.db_path = args.db_path
    DashboardHandler.default_live_limit = args.live_limit
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"Serving live dashboard at http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
