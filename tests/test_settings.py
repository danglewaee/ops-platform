from __future__ import annotations

import unittest

from ops_platform.settings import load_app_settings
from ops_platform.storage import SQLITE_DB_PATH


class SettingsTests(unittest.TestCase):
    def test_load_app_settings_uses_defaults(self) -> None:
        settings = load_app_settings({})
        self.assertEqual(settings.api_host, "0.0.0.0")
        self.assertEqual(settings.api_port, 8000)
        self.assertEqual(settings.db_path, str(SQLITE_DB_PATH))
        self.assertTrue(settings.auto_init_storage)
        self.assertFalse(settings.enable_tracing)

    def test_load_app_settings_reads_overrides(self) -> None:
        settings = load_app_settings(
            {
                "OPS_PLATFORM_API_HOST": "127.0.0.1",
                "OPS_PLATFORM_API_PORT": "9001",
                "OPS_PLATFORM_DB_PATH": "postgresql://ops:ops@db:5432/ops_platform",
                "OPS_PLATFORM_AUTO_INIT_STORAGE": "false",
                "OPS_PLATFORM_AUTH_ENABLED": "true",
                "OPS_PLATFORM_API_KEYS": "alpha,beta",
                "OPS_PLATFORM_RATE_LIMIT_ENABLED": "true",
                "OPS_PLATFORM_RATE_LIMIT_BACKEND": "redis",
                "OPS_PLATFORM_RATE_LIMIT_REQUESTS": "25",
                "OPS_PLATFORM_RATE_LIMIT_WINDOW_SECONDS": "120",
                "OPS_PLATFORM_REDIS_URL": "redis://redis:6379/0",
                "OPS_PLATFORM_ENABLE_TRACING": "true",
                "OPS_PLATFORM_DB_RETRY_ATTEMPTS": "5",
                "OPS_PLATFORM_DB_RETRY_BACKOFF_SECONDS": "1.25",
                "OPS_PLATFORM_DB_RETRY_MAX_BACKOFF_SECONDS": "9.0",
                "OPS_PLATFORM_OTLP_ENDPOINT": "http://otel-collector:4318/v1/traces",
                "OPS_PLATFORM_TIMESCALE_METRIC_RETENTION_DAYS": "30",
                "OPS_PLATFORM_TIMESCALE_CREATE_METRIC_ROLLUP": "yes",
            }
        )
        self.assertEqual(settings.api_host, "127.0.0.1")
        self.assertEqual(settings.api_port, 9001)
        self.assertEqual(settings.db_path, "postgresql://ops:ops@db:5432/ops_platform")
        self.assertFalse(settings.auto_init_storage)
        self.assertTrue(settings.auth_enabled)
        self.assertEqual(settings.api_keys, ("alpha", "beta"))
        self.assertTrue(settings.rate_limit_enabled)
        self.assertEqual(settings.rate_limit_backend, "redis")
        self.assertEqual(settings.rate_limit_requests, 25)
        self.assertEqual(settings.rate_limit_window_seconds, 120)
        self.assertEqual(settings.redis_url, "redis://redis:6379/0")
        self.assertEqual(settings.db_retry_attempts, 5)
        self.assertEqual(settings.db_retry_backoff_seconds, 1.25)
        self.assertEqual(settings.db_retry_max_backoff_seconds, 9.0)
        self.assertTrue(settings.enable_tracing)
        self.assertEqual(settings.otlp_endpoint, "http://otel-collector:4318/v1/traces")
        self.assertEqual(settings.timescale_metric_retention_days, 30)
        self.assertTrue(settings.timescale_create_metric_rollup)


if __name__ == "__main__":
    unittest.main()
