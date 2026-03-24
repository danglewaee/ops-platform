from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from ops_platform.storage import ingest_stream_bundle, list_ingested_streams
from ops_platform.resilience import RetryPolicy
from ops_platform.timescale_storage import configure_timescale_features, ensure_timescale_schema


class _FakeCursor:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object]] = []

    def execute(self, query: str, params=None):
        self.statements.append((query, params))
        return self

    def executemany(self, query: str, params_seq):
        self.statements.append((query, list(params_seq)))
        return self

    def fetchone(self):
        return {"size_bytes": 1024}

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = _FakeCursor()
        self.commit_count = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_count += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakePsycopg:
    def __init__(self, failures_before_success: int = 0) -> None:
        self.connections: list[tuple[str, bool | None, object]] = []
        self.connection_instances: list[_FakeConnection] = []
        self.failures_before_success = failures_before_success

    def connect(self, dsn: str, autocommit: bool | None = None, row_factory=None):
        if self.failures_before_success > 0:
            self.failures_before_success -= 1
            raise OSError("transient db failure")
        self.connections.append((dsn, autocommit, row_factory))
        connection = _FakeConnection()
        self.connection_instances.append(connection)
        return connection


class TimescaleStorageTests(unittest.TestCase):
    def test_storage_dispatches_postgres_urls_to_timescale_backend(self) -> None:
        with patch("ops_platform.storage.ingest_stream_bundle_timescale", return_value="postgresql://db") as ingest_mock:
            result = ingest_stream_bundle("stream-a", [], [], db_path="timescaledb://user:pass@host/db")
        self.assertEqual(result, "postgresql://db")
        ingest_mock.assert_called_once()

        with patch("ops_platform.storage.list_ingested_streams_timescale", return_value=[{"stream_id": "stream-a"}]) as list_mock:
            streams = list_ingested_streams(db_path="postgresql://user:pass@host/db")
        self.assertEqual(streams, [{"stream_id": "stream-a"}])
        list_mock.assert_called_once()

    def test_ensure_timescale_schema_normalizes_dsn_and_creates_hypertables(self) -> None:
        fake_psycopg = _FakePsycopg()
        with patch("ops_platform.timescale_storage._require_psycopg", return_value=(fake_psycopg, object())):
            dsn = ensure_timescale_schema("timescaledb://user:pass@host:5432/ops")

        self.assertEqual(dsn, "postgresql://user:pass@host:5432/ops")
        self.assertEqual(fake_psycopg.connections[0][0], dsn)
        executed = "\n".join(query for query, _ in fake_psycopg.connection_instances[0].cursor_instance.statements)
        self.assertIn("CREATE EXTENSION IF NOT EXISTS timescaledb", executed)
        self.assertIn("create_hypertable", executed)
        self.assertIn("metric_samples", executed)
        self.assertIn("change_events", executed)
        self.assertIn("audit_events", executed)

    def test_configure_timescale_features_adds_policies_and_rollup(self) -> None:
        fake_psycopg = _FakePsycopg()
        with patch("ops_platform.timescale_storage._require_psycopg", return_value=(fake_psycopg, object())):
            summary = configure_timescale_features(
                "postgresql://user:pass@host:5432/ops",
                metric_retention_days=30,
                event_retention_days=14,
                compress_after_days=7,
                create_continuous_aggregate=True,
                aggregate_name="metric_samples_5m",
            )

        self.assertEqual(summary["continuous_aggregate"], "metric_samples_5m")
        executed = "\n".join(query for query, _ in fake_psycopg.connection_instances[-1].cursor_instance.statements)
        self.assertIn("add_compression_policy", executed)
        self.assertIn("add_retention_policy", executed)
        self.assertIn("CREATE MATERIALIZED VIEW IF NOT EXISTS metric_samples_5m", executed)
        self.assertIn("add_continuous_aggregate_policy", executed)

    def test_ensure_timescale_schema_retries_transient_connection_failures(self) -> None:
        fake_psycopg = _FakePsycopg(failures_before_success=1)
        with (
            patch("ops_platform.timescale_storage._require_psycopg", return_value=(fake_psycopg, object())),
            patch(
                "ops_platform.timescale_storage._load_db_retry_policy",
                return_value=RetryPolicy(attempts=2, backoff_seconds=0.0, max_backoff_seconds=0.0),
            ),
        ):
            dsn = ensure_timescale_schema("postgresql://user:pass@host:5432/ops")

        self.assertEqual(dsn, "postgresql://user:pass@host:5432/ops")
        self.assertEqual(len(fake_psycopg.connections), 1)


if __name__ == "__main__":
    unittest.main()
