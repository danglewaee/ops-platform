from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from ops_platform.deploy_bundle import (
    build_deploy_bundle_summary,
    load_deploy_bundle_settings,
    load_env_file,
)


class DeployBundleTests(unittest.TestCase):
    def test_load_env_file_ignores_comments(self) -> None:
        with TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env.deploy"
            env_path.write_text(
                "# comment\nOPS_PLATFORM_PUBLIC_BASE_URL=http://localhost\nOPS_PLATFORM_AUTH_ENABLED=true\n",
                encoding="utf-8",
            )
            payload = load_env_file(env_path)

        self.assertEqual(payload["OPS_PLATFORM_PUBLIC_BASE_URL"], "http://localhost")
        self.assertEqual(payload["OPS_PLATFORM_AUTH_ENABLED"], "true")

    def test_load_deploy_bundle_settings_uses_app_settings_validation(self) -> None:
        settings = load_deploy_bundle_settings(
            {
                "OPS_PLATFORM_PUBLIC_BASE_URL": "https://ops.example.com",
                "OPS_PLATFORM_AUTH_ENABLED": "true",
                "OPS_PLATFORM_API_KEYS": "token",
                "OPS_PLATFORM_RATE_LIMIT_ENABLED": "true",
                "OPS_PLATFORM_RATE_LIMIT_BACKEND": "redis",
                "OPS_PLATFORM_REDIS_URL": "redis://redis:6379/0",
            }
        )

        self.assertEqual(settings.public_base_url, "https://ops.example.com")
        self.assertTrue(settings.auth_enabled)
        self.assertEqual(settings.rate_limit_backend, "redis")
        self.assertTrue(settings.recurring_enabled)

    def test_build_deploy_bundle_summary_checks_worker_config_presence(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            deploy_dir = root / "deploy"
            deploy_dir.mkdir()
            (deploy_dir / "recurring_pull.toml").write_text("[recurring]\nbase_url = 'http://prometheus:9090'\n", encoding="utf-8")

            settings = load_deploy_bundle_settings(
                {
                    "OPS_PLATFORM_PUBLIC_BASE_URL": "http://localhost",
                    "OPS_PLATFORM_AUTH_ENABLED": "true",
                    "OPS_PLATFORM_API_KEYS": "token",
                    "OPS_PLATFORM_RECURRING_CONFIG": "/app/deploy/recurring_pull.toml",
                }
            )
            summary = build_deploy_bundle_summary(settings, workspace_root=root)

        self.assertEqual(summary["public_base_url"], "http://localhost")
        self.assertTrue(summary["worker"]["config_present_in_workspace"])


if __name__ == "__main__":
    unittest.main()
