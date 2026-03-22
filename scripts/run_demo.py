from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.pipeline import generate_and_run_pipeline, run_pipeline, run_scenario_matrix
from ops_platform.schemas import DecisionConstraints
from ops_platform.scenarios import list_scenarios
from ops_platform.storage import save_run_bundle


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Ops Decision Platform demo pipeline.")
    parser.add_argument(
        "--scenario",
        default="traffic_spike",
        choices=list_scenarios(),
        help="Scenario to simulate.",
    )
    parser.add_argument(
        "--matrix",
        action="store_true",
        help="Run every scenario and print a concise matrix instead of a single scenario report.",
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
    parser.add_argument(
        "--save-run",
        action="store_true",
        help="Persist the generated telemetry, events, and report to runs/ as a replayable JSON bundle.",
    )
    parser.add_argument(
        "--planner-mode",
        choices=["heuristic", "cp_sat"],
        default="heuristic",
        help="Planner to use for the recommendation step.",
    )
    parser.add_argument(
        "--max-total-cost-delta-pct",
        type=float,
        help="Optional total positive cost budget across selected actions.",
    )
    args = parser.parse_args()
    decision_constraints = (
        DecisionConstraints(max_total_cost_delta_pct=args.max_total_cost_delta_pct)
        if args.max_total_cost_delta_pct is not None
        else None
    )

    if args.matrix:
        reports = run_scenario_matrix(
            seed=args.seed,
            planner_mode=args.planner_mode,
            decision_constraints=decision_constraints,
        )
        matrix = [
            {
                "scenario": report.metadata.name,
                "category": report.metadata.category,
                "root_cause": report.metadata.root_cause,
                "expected_action": report.metadata.expected_action,
                "recommended_action": report.recommendations[0].action if report.recommendations else None,
                "top2_root_cause_hit": report.evaluation.top2_root_cause_hit,
                "recommended_action_match": report.evaluation.recommended_action_match,
                "alert_reduction_pct": report.evaluation.alert_reduction_pct,
                "decision_latency_ms": report.evaluation.decision_latency_ms,
                "planner_mode": report.evaluation.planner_mode,
            }
            for report in reports
        ]
        print(json.dumps(matrix, indent=2, default=str))
        return 0

    if args.save_run:
        telemetry, events, metadata, report = generate_and_run_pipeline(
            args.scenario,
            seed=args.seed,
            planner_mode=args.planner_mode,
            decision_constraints=decision_constraints,
        )
        saved_path = save_run_bundle(telemetry, events, metadata, report, seed=args.seed)
    else:
        report = run_pipeline(
            args.scenario,
            seed=args.seed,
            planner_mode=args.planner_mode,
            decision_constraints=decision_constraints,
        )
        saved_path = None
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
            "evaluation_mode": report.evaluation.evaluation_mode,
            "planner_mode": report.evaluation.planner_mode,
            "trace_id": report.evaluation.trace_id,
            "latency_protection_pct": report.evaluation.latency_protection_pct,
            "avoided_overprovisioning_pct": report.evaluation.avoided_overprovisioning_pct,
            "baseline_win_rate_pct": report.evaluation.baseline_win_rate_pct,
            "action_stability_pct": report.evaluation.action_stability_pct,
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
        if saved_path:
            summary["saved_run"] = str(saved_path)
        print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
