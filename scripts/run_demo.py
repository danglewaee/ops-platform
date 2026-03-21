from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.pipeline import run_pipeline
from ops_platform.scenarios import list_scenarios


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Ops Decision Platform demo pipeline.")
    parser.add_argument(
        "--scenario",
        default="traffic_spike",
        choices=list_scenarios(),
        help="Scenario to simulate.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=7,
        help="Deterministic seed for the simulator.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Print the full JSON report instead of a concise summary.",
    )
    args = parser.parse_args()

    report = run_pipeline(args.scenario, seed=args.seed)
    if args.full:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        summary = {
            "scenario": report.metadata.name,
            "root_cause": report.metadata.root_cause,
            "expected_action": report.metadata.expected_action,
            "incident_count": report.evaluation.incident_count,
            "anomaly_count": report.evaluation.anomaly_count,
            "alert_reduction_pct": report.evaluation.alert_reduction_pct,
            "top2_root_cause_hit": report.evaluation.top2_root_cause_hit,
            "recommended_action_match": report.evaluation.recommended_action_match,
            "recommendations": [
                {
                    "action": recommendation.action,
                    "target_service": recommendation.target_service,
                    "confidence": recommendation.confidence,
                }
                for recommendation in report.recommendations
            ],
            "root_cause_candidates": [
                {
                    "incident_id": incident.incident_id,
                    "candidates": incident.root_cause_candidates,
                }
                for incident in report.incidents
            ],
            "baselines": report.evaluation.baseline_comparisons,
        }
        print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
