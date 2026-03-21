from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.file_ingestion import load_file_bundle
from ops_platform.pipeline import run_pipeline_from_streams
from ops_platform.schemas import ScenarioMetadata
from ops_platform.storage import ingest_stream_bundle, save_stream_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize exported telemetry files and ingest them into SQLite.")
    parser.add_argument("--stream-id", required=True, help="Stable stream identifier for persisted telemetry.")
    parser.add_argument("--telemetry", required=True, help="Path to a telemetry CSV, JSON, or JSONL export.")
    parser.add_argument("--events", help="Optional path to a change-event CSV, JSON, or JSONL export.")
    parser.add_argument("--mapping", help="Optional TOML or JSON mapping config for source field names and aliases.")
    parser.add_argument("--source", default="file-import", help="Source label stored with the ingested stream.")
    parser.add_argument("--environment", default="production", help="Environment label stored with the ingested stream.")
    parser.add_argument("--db-path", help="Optional override for the SQLite database path.")
    parser.add_argument("--name", help="Scenario or stream name for later evaluation.")
    parser.add_argument(
        "--description",
        default="Imported telemetry stream evaluated in shadow mode.",
        help="Description stored with the ingested stream.",
    )
    parser.add_argument("--root-cause", default="", help="Optional ground-truth root cause when known.")
    parser.add_argument("--expected-action", default="", help="Optional expected action when ground truth is known.")
    parser.add_argument("--category", default="live", help="Category stored with the ingested stream.")
    parser.add_argument("--evaluate", action="store_true", help="Run the shadow-mode pipeline after ingestion.")
    args = parser.parse_args()

    telemetry, events = load_file_bundle(args.telemetry, events_path=args.events, mapping_path=args.mapping)
    services = sorted({sample.service for sample in telemetry})
    stream_metadata = {
        "name": args.name or args.stream_id,
        "description": args.description,
        "root_cause": args.root_cause,
        "expected_action": args.expected_action,
        "impacted_services": services,
        "category": args.category,
    }

    database_path = ingest_stream_bundle(
        args.stream_id,
        telemetry,
        events,
        source=args.source,
        environment=args.environment,
        metadata=stream_metadata,
        db_path=args.db_path,
    )

    summary: dict[str, object] = {
        "stream_id": args.stream_id,
        "db_path": str(database_path),
        "source": args.source,
        "environment": args.environment,
        "metric_count": len(telemetry),
        "event_count": len(events),
        "services": services,
        "metrics": sorted({sample.metric for sample in telemetry}),
    }

    if args.evaluate:
        metadata = ScenarioMetadata(
            name=stream_metadata["name"],
            description=stream_metadata["description"],
            root_cause=stream_metadata["root_cause"],
            expected_action=stream_metadata["expected_action"],
            impacted_services=stream_metadata["impacted_services"],
            category=stream_metadata["category"],
        )
        report = run_pipeline_from_streams(telemetry, events, metadata)
        save_stream_report(args.stream_id, metadata, report, db_path=args.db_path)
        recommendation = report.recommendations[0] if report.recommendations else None
        summary["evaluation"] = {
            "evaluation_mode": report.evaluation.evaluation_mode,
            "incident_count": report.evaluation.incident_count,
            "anomaly_count": report.evaluation.anomaly_count,
            "recommended_action": recommendation.action if recommendation else None,
            "target_service": recommendation.target_service if recommendation else None,
            "latency_protection_pct": report.evaluation.latency_protection_pct,
            "avoided_overprovisioning_pct": report.evaluation.avoided_overprovisioning_pct,
            "baseline_win_rate_pct": report.evaluation.baseline_win_rate_pct,
        }

    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
