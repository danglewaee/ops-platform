from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.smoke_docker_stack import (
    build_compose_base_command,
    capture_compose_output,
    load_env_file,
    resolve_api_headers,
    run_smoke_check,
)


class SmokeDockerStackTests(unittest.TestCase):
    def test_load_env_file_ignores_comments_and_blank_lines(self) -> None:
        with TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "# comment",
                        "",
                        "OPS_PLATFORM_API_PORT=8123",
                        "OPS_PLATFORM_AUTH_ENABLED=true",
                    ]
                ),
                encoding="utf-8",
            )

            payload = load_env_file(env_path)

        self.assertEqual(payload["OPS_PLATFORM_API_PORT"], "8123")
        self.assertEqual(payload["OPS_PLATFORM_AUTH_ENABLED"], "true")

    def test_build_compose_base_command_includes_env_file_when_present(self) -> None:
        with TemporaryDirectory() as temp_dir:
            compose_path = Path(temp_dir) / "docker-compose.yml"
            compose_path.write_text("services: {}\n", encoding="utf-8")
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("OPS_PLATFORM_API_PORT=8000\n", encoding="utf-8")

            command = build_compose_base_command(
                compose_file=compose_path,
                env_file=env_path,
                project_name="ops-smoke",
            )

        self.assertEqual(
            command,
            ["docker", "compose", "-f", str(compose_path), "-p", "ops-smoke", "--env-file", str(env_path)],
        )

    def test_resolve_api_headers_requires_token_when_auth_enabled(self) -> None:
        with self.assertRaises(ValueError):
            resolve_api_headers({"OPS_PLATFORM_AUTH_ENABLED": "true"})

        headers = resolve_api_headers(
            {
                "OPS_PLATFORM_AUTH_ENABLED": "true",
                "OPS_PLATFORM_API_KEYS": "secret-token",
                "OPS_PLATFORM_AUTH_HEADER_NAME": "x-api-key",
            }
        )
        self.assertEqual(headers["x-api-key"], "secret-token")
        self.assertEqual(headers["x-ops-actor"], "smoke-check")

    def test_run_smoke_check_starts_probes_and_tears_down(self) -> None:
        with TemporaryDirectory() as temp_dir:
            compose_path = Path(temp_dir) / "docker-compose.yml"
            compose_path.write_text("services: {}\n", encoding="utf-8")
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "OPS_PLATFORM_API_PORT=8123",
                        "OPS_PLATFORM_AUTH_ENABLED=true",
                        "OPS_PLATFORM_API_KEYS=secret-token",
                    ]
                ),
                encoding="utf-8",
            )

            with (
                patch("scripts.smoke_docker_stack.subprocess.run") as subprocess_run,
                patch(
                    "scripts.smoke_docker_stack.poll_json",
                    return_value={"ready": True, "backend": "timescaledb"},
                ) as poll_json_mock,
                patch(
                    "scripts.smoke_docker_stack.request_json",
                    side_effect=[
                        {"status": "ok"},
                        [],
                        [{"action": "list_streams"}],
                    ],
                ) as request_json_mock,
            ):
                summary_path = Path(temp_dir) / "artifacts" / "smoke-summary.json"
                summary = run_smoke_check(
                    compose_file=compose_path,
                    env_file=env_path,
                    project_name="ops-smoke",
                    timeout_seconds=30,
                    build=True,
                    keep_up=False,
                    summary_path=summary_path,
                )

                subprocess_calls = [call.args[0] for call in subprocess_run.call_args_list]
                self.assertEqual(subprocess_calls[0][-3:], ["up", "-d", "--build"])
                self.assertEqual(subprocess_calls[-1][-2:], ["down", "-v"])
                poll_json_mock.assert_called_once_with("http://127.0.0.1:8123/ready", timeout_seconds=30)
                self.assertEqual(request_json_mock.call_args_list[0].args[0], "http://127.0.0.1:8123/health")
                self.assertEqual(summary["status"], "ok")
                self.assertEqual(summary["audit_event_count"], 1)
                self.assertTrue(summary_path.exists())
                self.assertEqual(json.loads(summary_path.read_text(encoding="utf-8"))["status"], "ok")

    def test_capture_compose_output_combines_stdout_and_stderr(self) -> None:
        with patch("scripts.smoke_docker_stack.subprocess.run") as subprocess_run:
            subprocess_run.return_value.stdout = "service-a"
            subprocess_run.return_value.stderr = "warning"
            subprocess_run.return_value.returncode = 1

            payload = capture_compose_output(["docker", "compose", "ps"])

        self.assertIn("service-a", payload)
        self.assertIn("warning", payload)

    def test_run_smoke_check_writes_debug_summary_on_failure(self) -> None:
        with TemporaryDirectory() as temp_dir:
            compose_path = Path(temp_dir) / "docker-compose.yml"
            compose_path.write_text("services: {}\n", encoding="utf-8")
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("OPS_PLATFORM_API_PORT=8123\n", encoding="utf-8")

            completed = subprocess.CompletedProcess(args=["docker"], returncode=0, stdout="", stderr="")
            with (
                patch("scripts.smoke_docker_stack.subprocess.run", return_value=completed) as subprocess_run,
                patch(
                    "scripts.smoke_docker_stack.poll_json",
                    return_value={"ready": True, "backend": "timescaledb"},
                ),
                patch(
                    "scripts.smoke_docker_stack.request_json",
                    side_effect=[
                        {"status": "ok"},
                        RuntimeError("streams failed"),
                    ],
                ),
            ):
                summary_path = Path(temp_dir) / "artifacts" / "smoke-summary.json"
                with self.assertRaises(RuntimeError):
                    run_smoke_check(
                        compose_file=compose_path,
                        env_file=env_path,
                        project_name="ops-smoke",
                        keep_up=False,
                        summary_path=summary_path,
                    )

            self.assertTrue(summary_path.exists())
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "error")
            self.assertIn("streams failed", summary["error"])
            subprocess_calls = [call.args[0] for call in subprocess_run.call_args_list]
            self.assertEqual(subprocess_calls[0][-3:], ["up", "-d", "--build"])
            self.assertEqual(subprocess_calls[-3][-1], "ps")
            self.assertEqual(subprocess_calls[-2][-2:], ["logs", "--no-color"])
            self.assertEqual(subprocess_calls[-1][-2:], ["down", "-v"])

    def test_run_smoke_check_captures_compose_up_failures(self) -> None:
        with TemporaryDirectory() as temp_dir:
            compose_path = Path(temp_dir) / "docker-compose.yml"
            compose_path.write_text("services: {}\n", encoding="utf-8")
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("OPS_PLATFORM_API_PORT=8123\n", encoding="utf-8")

            completed = subprocess.CompletedProcess(args=["docker"], returncode=0, stdout="", stderr="")
            failure = subprocess.CalledProcessError(returncode=1, cmd=["docker", "compose", "up"])

            with patch("scripts.smoke_docker_stack.subprocess.run") as subprocess_run:
                subprocess_run.side_effect = [failure, completed, completed, completed]
                summary_path = Path(temp_dir) / "artifacts" / "smoke-summary.json"

                with self.assertRaises(subprocess.CalledProcessError):
                    run_smoke_check(
                        compose_file=compose_path,
                        env_file=env_path,
                        project_name="ops-smoke",
                        keep_up=False,
                        summary_path=summary_path,
                    )

            self.assertTrue(summary_path.exists())
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "error")
            subprocess_calls = [call.args[0] for call in subprocess_run.call_args_list]
            self.assertEqual(subprocess_calls[0][-3:], ["up", "-d", "--build"])
            self.assertEqual(subprocess_calls[1][-1], "ps")
            self.assertEqual(subprocess_calls[2][-2:], ["logs", "--no-color"])
            self.assertEqual(subprocess_calls[3][-2:], ["down", "-v"])


if __name__ == "__main__":
    unittest.main()
