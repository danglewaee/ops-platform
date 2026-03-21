from __future__ import annotations

import csv
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from ops_platform.file_ingestion import load_file_bundle
from ops_platform.pipeline import run_pipeline, run_pipeline_from_streams
from ops_platform.simulator import generate_scenario


class FileIngestionTests(unittest.TestCase):
    def test_csv_telemetry_and_jsonl_events_match_direct_pipeline(self) -> None:
        telemetry, events, metadata = generate_scenario("traffic_spike", seed=7)

        metric_aliases = {
            "request_rate": "http_requests_per_second",
            "p95_latency_ms": "latency_p95_ms",
            "error_rate_pct": "errors_pct",
            "queue_depth": "queue_messages",
            "cpu_pct": "cpu_usage_pct",
        }
        service_aliases = {
            "gateway": "edge-gateway",
            "payments": "payments-api",
            "worker": "async-worker",
        }

        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            telemetry_path = temp_path / "telemetry.csv"
            events_path = temp_path / "events.jsonl"
            mapping_path = temp_path / "mapping.toml"

            with telemetry_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "ts",
                        "service_name",
                        "metric_name",
                        "metric_value",
                        "metric_unit",
                        "cluster_name",
                        "namespace",
                    ],
                )
                writer.writeheader()
                for sample in telemetry:
                    writer.writerow(
                        {
                            "ts": sample.timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
                            "service_name": service_aliases.get(sample.service, sample.service),
                            "metric_name": metric_aliases[sample.metric],
                            "metric_value": sample.value,
                            "metric_unit": sample.unit,
                            "cluster_name": "cluster-a",
                            "namespace": "prod",
                        }
                    )

            with events_path.open("w", encoding="utf-8") as handle:
                for event in events:
                    payload = {
                        "ts": event.timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
                        "service_name": service_aliases.get(event.service, event.service),
                        "kind": event.event_type,
                        "message": event.description,
                    }
                    handle.write(json.dumps(payload) + "\n")

            mapping_path.write_text(
                "\n".join(
                    [
                        'step_seconds = 60',
                        'timestamp_format = "%Y-%m-%dT%H:%M:%S"',
                        "",
                        "[telemetry_fields]",
                        'timestamp = "ts"',
                        'service = "service_name"',
                        'metric = "metric_name"',
                        'value = "metric_value"',
                        'unit = "metric_unit"',
                        "",
                        "[event_fields]",
                        'timestamp = "ts"',
                        'service = "service_name"',
                        'event_type = "kind"',
                        'description = "message"',
                        "",
                        "[telemetry_dimensions]",
                        'cluster = "cluster_name"',
                        'namespace = "namespace"',
                        "",
                        "[metric_aliases]",
                        'http_requests_per_second = "request_rate"',
                        'latency_p95_ms = "p95_latency_ms"',
                        'errors_pct = "error_rate_pct"',
                        'queue_messages = "queue_depth"',
                        'cpu_usage_pct = "cpu_pct"',
                        "",
                        "[service_aliases]",
                        'edge-gateway = "gateway"',
                        'payments-api = "payments"',
                        'async-worker = "worker"',
                    ]
                ),
                encoding="utf-8",
            )

            loaded_telemetry, loaded_events = load_file_bundle(
                telemetry_path,
                events_path=events_path,
                mapping_path=mapping_path,
            )

        self.assertEqual(len(loaded_telemetry), len(telemetry))
        self.assertEqual(len(loaded_events), len(events))
        self.assertEqual(loaded_telemetry[0].dimensions["cluster"], "cluster-a")
        self.assertEqual(loaded_telemetry[0].dimensions["namespace"], "prod")

        direct = run_pipeline("traffic_spike", seed=7)
        imported = run_pipeline_from_streams(loaded_telemetry, loaded_events, metadata)
        self.assertEqual(imported.recommendations[0].action, direct.recommendations[0].action)
        self.assertEqual(imported.incidents[0].root_cause_candidates[:2], direct.incidents[0].root_cause_candidates[:2])

    def test_jsonl_telemetry_without_explicit_steps_infers_steps(self) -> None:
        with TemporaryDirectory() as temp_dir:
            telemetry_path = Path(temp_dir) / "telemetry.jsonl"
            records = [
                {
                    "timestamp": "2026-03-20T09:00:00",
                    "service": "gateway",
                    "metric": "request_rate",
                    "value": 1800,
                },
                {
                    "timestamp": "2026-03-20T09:01:00",
                    "service": "gateway",
                    "metric": "request_rate",
                    "value": 1812,
                },
            ]
            telemetry_path.write_text("\n".join(json.dumps(record) for record in records), encoding="utf-8")

            loaded_telemetry, loaded_events = load_file_bundle(telemetry_path)

        self.assertEqual(len(loaded_events), 0)
        self.assertEqual([sample.step for sample in loaded_telemetry], [0, 1])
        self.assertEqual(loaded_telemetry[0].unit, "req/s")


if __name__ == "__main__":
    unittest.main()
