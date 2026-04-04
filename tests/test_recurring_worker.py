from __future__ import annotations

import unittest
from unittest.mock import patch

from ops_platform.recurring_worker import (
    RecurringWorkerSettings,
    load_recurring_worker_settings,
    run_recurring_worker,
)


class RecurringWorkerTests(unittest.TestCase):
    def test_load_recurring_worker_settings_reads_env(self) -> None:
        settings = load_recurring_worker_settings(
            {
                "OPS_PLATFORM_RECURRING_ENABLED": "true",
                "OPS_PLATFORM_RECURRING_CONFIG": "/app/deploy/recurring_pull.toml",
                "OPS_PLATFORM_RECURRING_INTERVAL_SECONDS": "120",
                "OPS_PLATFORM_RECURRING_FAIL_DELAY_SECONDS": "15",
                "OPS_PLATFORM_RECURRING_SUMMARY_PATH": "/app/artifacts/latest.json",
            }
        )

        self.assertTrue(settings.enabled)
        self.assertEqual(settings.config_path, "/app/deploy/recurring_pull.toml")
        self.assertEqual(settings.interval_seconds, 120)
        self.assertEqual(settings.fail_delay_seconds, 15)

    def test_run_recurring_worker_returns_disabled_without_running_cycle(self) -> None:
        result = run_recurring_worker(RecurringWorkerSettings(enabled=False))
        self.assertEqual(result["status"], "disabled")

    def test_run_recurring_worker_runs_once(self) -> None:
        settings = RecurringWorkerSettings(enabled=True, run_once=True)
        with patch("ops_platform.recurring_worker.run_worker_cycle", return_value={"ingest": {"stream_id": "demo"}}) as mocked:
            result = run_recurring_worker(settings)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["summary"]["ingest"]["stream_id"], "demo")
        mocked.assert_called_once()

    def test_run_recurring_worker_uses_fail_delay_on_errors(self) -> None:
        settings = RecurringWorkerSettings(enabled=True, interval_seconds=99, fail_delay_seconds=7)
        sleeps: list[float] = []
        with patch("ops_platform.recurring_worker.run_worker_cycle", side_effect=RuntimeError("boom")):
            result = run_recurring_worker(settings, iterations=1, sleep_fn=sleeps.append)

        self.assertEqual(result["status"], "error")
        self.assertEqual(sleeps, [])


if __name__ == "__main__":
    unittest.main()
