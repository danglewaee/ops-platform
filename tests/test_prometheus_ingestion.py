from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from urllib.error import URLError
from unittest.mock import patch

from ops_platform.prometheus_ingestion import (
    PrometheusIngestionConfig,
    fetch_prometheus_metrics,
    load_prometheus_config,
    resolve_prometheus_window,
)
from ops_platform.simulator import generate_scenario
from ops_platform.storage import list_ingested_streams


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class PrometheusIngestionTests(unittest.TestCase):
    def test_resolve_prometheus_window_supports_lookback(self) -> None:
        start, end = resolve_prometheus_window(
            start=None,
            end="2026-03-21T09:30:00Z",
            lookback_minutes=15,
        )
        self.assertEqual(start.isoformat(), "2026-03-21T09:15:00")
        self.assertEqual(end.isoformat(), "2026-03-21T09:30:00")

    def test_fetch_prometheus_metrics_normalizes_service_aliases_and_steps(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "prometheus.toml"
            config_path.write_text(
                "\n".join(
                    [
                        'base_url = "http://prometheus.local:9090"',
                        'step = "60s"',
                        'service_label = "service"',
                        "",
                        "[queries]",
                        'request_rate = "sum(rate(http_requests_total[5m])) by (service)"',
                        'p95_latency_ms = "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le, service)) * 1000"',
                        "",
                        "[service_aliases]",
                        'edge-gateway = "gateway"',
                    ]
                ),
                encoding="utf-8",
            )
            config = load_prometheus_config(config_path)

        start = datetime(2026, 3, 20, 9, 0, 0)
        end = datetime(2026, 3, 20, 9, 1, 0)
        timestamps = [
            datetime(2026, 3, 20, 9, 0, 0, tzinfo=timezone.utc).timestamp(),
            datetime(2026, 3, 20, 9, 1, 0, tzinfo=timezone.utc).timestamp(),
        ]

        def fake_urlopen(request, timeout=30):
            query = parse_qs(urlparse(request.full_url).query)["query"][0]
            if "http_requests_total" in query:
                payload = {
                    "status": "success",
                    "data": {
                        "resultType": "matrix",
                        "result": [
                            {
                                "metric": {"service": "edge-gateway", "cluster": "prod-a"},
                                "values": [[timestamps[0], "1800"], [timestamps[1], "1825"]],
                            }
                        ],
                    },
                }
            else:
                payload = {
                    "status": "success",
                    "data": {
                        "resultType": "matrix",
                        "result": [
                            {
                                "metric": {"service": "edge-gateway", "cluster": "prod-a"},
                                "values": [[timestamps[0], "95"], [timestamps[1], "104"]],
                            }
                        ],
                    },
                }
            return _FakeResponse(payload)

        with patch("ops_platform.prometheus_ingestion.urlopen", side_effect=fake_urlopen):
            samples = fetch_prometheus_metrics(config, start=start, end=end)

        self.assertEqual(len(samples), 4)
        self.assertEqual({sample.metric for sample in samples}, {"request_rate", "p95_latency_ms"})
        self.assertEqual({sample.service for sample in samples}, {"gateway"})
        self.assertEqual(sorted({sample.step for sample in samples}), [0, 1])
        self.assertTrue(all(sample.dimensions["cluster"] == "prod-a" for sample in samples))

    def test_fetch_prometheus_metrics_retries_transient_url_errors(self) -> None:
        config = PrometheusIngestionConfig(
            base_url="http://prometheus.local:9090",
            queries={"request_rate": "sum(rate(http_requests_total[5m])) by (service)"},
            retry_attempts=2,
            retry_backoff_seconds=0.0,
            retry_max_backoff_seconds=0.0,
        )
        start = datetime(2026, 3, 20, 9, 0, 0)
        end = datetime(2026, 3, 20, 9, 0, 0)
        timestamp = datetime(2026, 3, 20, 9, 0, 0, tzinfo=timezone.utc).timestamp()
        responses = [
            URLError("temporary network failure"),
            _FakeResponse(
                {
                    "status": "success",
                    "data": {
                        "resultType": "matrix",
                        "result": [
                            {
                                "metric": {"service": "gateway"},
                                "values": [[timestamp, "1800"]],
                            }
                        ],
                    },
                }
            ),
        ]

        with patch("ops_platform.prometheus_ingestion.urlopen", side_effect=responses):
            samples = fetch_prometheus_metrics(config, start=start, end=end)

        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0].service, "gateway")

    def test_prometheus_ingest_endpoint_persists_and_evaluates_stream(self) -> None:
        try:
            from ops_platform.api import create_app
        except RuntimeError as exc:  # pragma: no cover - optional dependency guard
            self.skipTest(str(exc))

        telemetry, events, metadata = generate_scenario("traffic_spike", seed=7)
        app = create_app()
        route = next(route.endpoint for route in app.routes if getattr(route, "path", "") == "/ingest/prometheus")

        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ops_platform.sqlite3"
            payload = SimpleNamespace(
                config_path="ignored.toml",
                stream_id="api-prometheus-stream",
                start=None,
                end="2026-03-21T09:30:00Z",
                lookback_minutes=30,
                base_url=None,
                events_path=None,
                event_mapping=None,
                source="api-test",
                environment="staging",
                db_path=str(db_path),
                name=metadata.name,
                description=metadata.description,
                root_cause=metadata.root_cause,
                expected_action=metadata.expected_action,
                category=metadata.category,
                evaluate=True,
            )

            with patch(
                "ops_platform.api.load_prometheus_bundle",
                return_value=(SimpleNamespace(step="60s"), telemetry, events, datetime(2026, 3, 21, 9, 0, 0), datetime(2026, 3, 21, 9, 30, 0)),
            ):
                response = route(payload)

            self.assertEqual(response["stream_id"], "api-prometheus-stream")
            self.assertEqual(response["metric_count"], len(telemetry))
            self.assertEqual(response["event_count"], len(events))
            self.assertEqual(response["evaluation"]["recommendation"]["action"], metadata.expected_action)

            streams = list_ingested_streams(db_path=db_path)
            self.assertEqual(len(streams), 1)
            self.assertEqual(streams[0]["stream_id"], "api-prometheus-stream")


if __name__ == "__main__":
    unittest.main()
