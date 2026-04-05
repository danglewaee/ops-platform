from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from ops_platform.deploy_evidence import (
    capture_deploy_evidence,
    load_deploy_evidence_settings,
    load_evidence_environment,
)


class DeployEvidenceTests(unittest.TestCase):
    def test_load_deploy_evidence_settings_uses_env_auth_and_base_url(self) -> None:
        settings = load_deploy_evidence_settings(
            {
                "OPS_PLATFORM_PUBLIC_BASE_URL": "https://ops.example.com",
                "OPS_PLATFORM_AUTH_ENABLED": "true",
                "OPS_PLATFORM_API_KEYS": "token-one,token-two",
                "OPS_PLATFORM_AUTH_HEADER_NAME": "x-api-key",
                "OPS_PLATFORM_ACTOR_HEADER_NAME": "x-ops-actor",
            },
            actor="post-deploy-check",
            timeout_seconds=8,
        )

        self.assertEqual(settings.base_url, "https://ops.example.com")
        self.assertTrue(settings.auth_enabled)
        self.assertEqual(settings.api_key, "token-one")
        self.assertEqual(settings.actor, "post-deploy-check")
        self.assertEqual(settings.timeout_seconds, 8)

    def test_capture_deploy_evidence_writes_artifacts_and_worker_summary(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            artifacts_dir = root / "artifacts" / "recurring"
            artifacts_dir.mkdir(parents=True)
            (artifacts_dir / "latest-summary.json").write_text(
                json.dumps({"status": "ok", "ingest": {"stream_id": "prod-live-001"}}, indent=2),
                encoding="utf-8",
            )

            settings = load_deploy_evidence_settings(
                {
                    "OPS_PLATFORM_PUBLIC_BASE_URL": "https://ops.example.com",
                    "OPS_PLATFORM_AUTH_ENABLED": "true",
                    "OPS_PLATFORM_API_KEYS": "secret-token",
                    "OPS_PLATFORM_RECURRING_SUMMARY_PATH": "/app/artifacts/recurring/latest-summary.json",
                }
            )

            responses = {
                "https://ops.example.com/health": {"status": "ok"},
                "https://ops.example.com/ready": {"ready": True, "backend": "timescaledb"},
                "https://ops.example.com/streams?limit=5": [{"stream_id": "demo"}],
                "https://ops.example.com/storage/stats": {"stream_count": 1, "metric_sample_count": 5},
                "https://ops.example.com/audit/events?limit=5": [{"action": "list_streams"}],
            }

            def _request_json(url: str, *, headers: dict[str, str], timeout_seconds: int):
                self.assertEqual(headers["x-api-key"], "secret-token")
                self.assertEqual(headers["x-ops-actor"], "deploy-evidence")
                self.assertEqual(timeout_seconds, 5)
                return responses[url]

            summary = capture_deploy_evidence(
                settings,
                output_dir=root / "artifacts" / "deploy-evidence" / "latest",
                workspace_root=root,
                request_json_fn=_request_json,
                captured_at="2026-04-05T12:00:00+00:00",
            )

            self.assertEqual(summary["overall_status"], "ok")
            self.assertTrue(summary["ready"])
            self.assertEqual(summary["backend"], "timescaledb")
            self.assertEqual(summary["stream_count"], 1)
            self.assertEqual(summary["audit_event_count"], 1)
            self.assertTrue(summary["worker_summary_available"])
            self.assertTrue(Path(summary["summary_path"]).exists())
            self.assertTrue(Path(summary["summary_markdown_path"]).exists())
            self.assertTrue((root / "artifacts" / "deploy-evidence" / "latest" / "recurring_worker_summary.json").exists())

    def test_capture_deploy_evidence_records_errors_without_dropping_summary(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            settings = load_deploy_evidence_settings(
                {
                    "OPS_PLATFORM_PUBLIC_BASE_URL": "https://ops.example.com",
                    "OPS_PLATFORM_AUTH_ENABLED": "false",
                }
            )

            def _request_json(url: str, *, headers: dict[str, str], timeout_seconds: int):
                if url.endswith("/streams?limit=5"):
                    raise RuntimeError("streams unavailable")
                if url.endswith("/audit/events?limit=5"):
                    raise RuntimeError("audit unavailable")
                payload_map = {
                    "https://ops.example.com/health": {"status": "ok"},
                    "https://ops.example.com/ready": {"ready": True, "backend": "sqlite"},
                    "https://ops.example.com/storage/stats": {"stream_count": 0},
                }
                return payload_map[url]

            summary = capture_deploy_evidence(
                settings,
                output_dir=root / "artifacts" / "deploy-evidence" / "latest",
                workspace_root=root,
                request_json_fn=_request_json,
                captured_at="2026-04-05T12:05:00+00:00",
            )

            self.assertEqual(summary["overall_status"], "error")
            self.assertEqual(len(summary["failures"]), 2)
            self.assertEqual(summary["checks"]["streams"]["status"], "error")
            self.assertEqual(summary["checks"]["worker_summary"]["status"], "not_configured")
            self.assertTrue(Path(summary["summary_path"]).exists())

    def test_load_evidence_environment_reads_optional_env_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env.deploy"
            env_path.write_text(
                "OPS_PLATFORM_PUBLIC_BASE_URL=https://ops.example.com\nOPS_PLATFORM_AUTH_ENABLED=true\n",
                encoding="utf-8",
            )

            environ = load_evidence_environment(env_path, environ={"OPS_PLATFORM_API_KEYS": "token"})

        self.assertEqual(environ["OPS_PLATFORM_PUBLIC_BASE_URL"], "https://ops.example.com")
        self.assertEqual(environ["OPS_PLATFORM_API_KEYS"], "token")


if __name__ == "__main__":
    unittest.main()
