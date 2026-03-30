from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from ops_platform.settings import AppSettings


class ApiRuntimeTests(unittest.TestCase):
    def test_ready_route_reflects_startup_storage_status(self) -> None:
        try:
            from ops_platform.api import create_app
        except RuntimeError as exc:  # pragma: no cover - optional dependency guard
            self.skipTest(str(exc))

        settings = AppSettings(
            db_path="postgresql://ops:ops@db:5432/ops_platform",
            enable_tracing=True,
            otlp_endpoint="http://otel-collector:4318/v1/traces",
            timescale_metric_retention_days=30,
            timescale_event_retention_days=14,
            timescale_compress_after_days=7,
            timescale_create_metric_rollup=True,
        )

        with (
            patch("ops_platform.api.load_app_settings", return_value=settings),
            patch("ops_platform.api.configure_tracing", return_value=True),
            patch("ops_platform.api.initialize_storage", return_value=settings.db_path) as initialize_mock,
            patch(
                "ops_platform.api.check_storage_health",
                return_value={
                    "ready": True,
                    "backend": "timescaledb",
                    "db_path": settings.db_path,
                },
            ),
        ):
            from ops_platform.api import initialize_app_runtime

            app = create_app()
            initialize_app_runtime(app, settings)

            route = next(route.endpoint for route in app.routes if getattr(route, "path", "") == "/ready")
            response = route()
            payload = json.loads(response.body.decode("utf-8"))

        initialize_mock.assert_called_once()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ready"])
        self.assertEqual(payload["backend"], "timescaledb")
        self.assertTrue(payload["tracing_enabled"])
        self.assertIsNone(payload["error"])


if __name__ == "__main__":
    unittest.main()
