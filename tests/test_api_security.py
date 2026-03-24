from __future__ import annotations

import asyncio
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from ops_platform.pipeline import generate_and_run_pipeline
from ops_platform.settings import AppSettings
from ops_platform.storage import ingest_stream_bundle, list_audit_events


class ApiSecurityTests(unittest.TestCase):
    def test_auth_blocks_unauthorized_requests_and_audits_denial(self) -> None:
        try:
            from ops_platform.api import create_app, initialize_app_runtime
        except RuntimeError as exc:  # pragma: no cover - optional dependency guard
            self.skipTest(str(exc))

        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops_platform.sqlite3"
            settings = AppSettings(
                db_path=str(db_path),
                auth_enabled=True,
                api_keys=("secret-token",),
                audit_log_enabled=True,
            )

            with patch("ops_platform.api.load_app_settings", return_value=settings):
                app = create_app()
                initialize_app_runtime(app, settings)
                response = _request(app, "GET", "/streams")

            self.assertEqual(response.status_code, 401)
            events = list_audit_events(db_path=db_path)
            self.assertEqual(events[0]["action"], "list_streams")
            self.assertEqual(events[0]["status_code"], 401)
            self.assertEqual(events[0]["metadata"]["outcome"], "auth_denied")

    def test_rate_limit_returns_429_and_records_audit_event(self) -> None:
        try:
            from ops_platform.api import create_app, initialize_app_runtime
        except RuntimeError as exc:  # pragma: no cover - optional dependency guard
            self.skipTest(str(exc))

        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops_platform.sqlite3"
            settings = AppSettings(
                db_path=str(db_path),
                auth_enabled=True,
                api_keys=("secret-token",),
                rate_limit_enabled=True,
                rate_limit_requests=1,
                rate_limit_window_seconds=60,
                audit_log_enabled=True,
            )
            headers = {"x-api-key": "secret-token"}

            with patch("ops_platform.api.load_app_settings", return_value=settings):
                app = create_app()
                initialize_app_runtime(app, settings)
                first = _request(app, "GET", "/streams", headers=headers)
                second = _request(app, "GET", "/streams", headers=headers)

            self.assertEqual(first.status_code, 200)
            self.assertEqual(first.headers["x-ratelimit-limit"], "1")
            self.assertEqual(second.status_code, 429)
            self.assertIn("retry-after", second.headers)

            events = list_audit_events(db_path=db_path)
            self.assertEqual(events[0]["action"], "list_streams")
            self.assertEqual(events[0]["status_code"], 429)
            self.assertEqual(events[0]["metadata"]["outcome"], "rate_limited")

    def test_post_request_is_audited_and_queryable(self) -> None:
        try:
            from ops_platform.api import create_app, initialize_app_runtime
        except RuntimeError as exc:  # pragma: no cover - optional dependency guard
            self.skipTest(str(exc))

        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops_platform.sqlite3"
            settings = AppSettings(
                db_path=str(db_path),
                auth_enabled=True,
                api_keys=("secret-token",),
                audit_log_enabled=True,
            )
            headers = {
                "x-api-key": "secret-token",
                "x-ops-actor": "oncall-bot",
            }
            telemetry, events, metadata, _ = generate_and_run_pipeline("traffic_spike", seed=7)
            ingest_stream_bundle("stream-a", telemetry, events, metadata=metadata, db_path=db_path)

            with patch("ops_platform.api.load_app_settings", return_value=settings):
                app = create_app()
                initialize_app_runtime(app, settings)
                response = _request(app, "POST", "/streams/stream-a/evaluate", headers=headers)
                audit_response = _request(app, "GET", "/audit/events", headers=headers)

            self.assertEqual(response.status_code, 200)
            self.assertEqual(audit_response.status_code, 200)
            payload = audit_response.json
            self.assertEqual(payload[0]["action"], "evaluate_stream")
            self.assertEqual(payload[0]["actor"], "oncall-bot")
            self.assertEqual(payload[0]["status_code"], 200)


class _Response:
    def __init__(self, status_code: int, headers: dict[str, str], body: bytes) -> None:
        self.status_code = status_code
        self.headers = headers
        self.body = body

    @property
    def json(self):
        return json.loads(self.body.decode("utf-8")) if self.body else None


def _request(
    app,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    json_body: dict[str, object] | None = None,
) -> _Response:
    return asyncio.run(_request_async(app, method, path, headers=headers, json_body=json_body))


async def _request_async(
    app,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    json_body: dict[str, object] | None = None,
) -> _Response:
    response_start: dict[str, object] = {}
    response_body_parts: list[bytes] = []
    body = json.dumps(json_body).encode("utf-8") if json_body is not None else b""
    encoded_headers = [(key.lower().encode("utf-8"), value.encode("utf-8")) for key, value in (headers or {}).items()]
    if json_body is not None:
        encoded_headers.append((b"content-type", b"application/json"))
        encoded_headers.append((b"content-length", str(len(body)).encode("utf-8")))
    encoded_headers.append((b"host", b"testserver"))

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": encoded_headers,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }

    request_sent = False

    async def receive():
        nonlocal request_sent
        if request_sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        request_sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        if message["type"] == "http.response.start":
            response_start.update(message)
            return
        if message["type"] == "http.response.body":
            response_body_parts.append(message.get("body", b""))

    await app(scope, receive, send)
    response_headers = {
        key.decode("utf-8"): value.decode("utf-8")
        for key, value in response_start.get("headers", [])
    }
    return _Response(
        status_code=int(response_start["status"]),
        headers=response_headers,
        body=b"".join(response_body_parts),
    )
if __name__ == "__main__":
    unittest.main()
