from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.benchmarks import (
    benchmark_persisted_streams,
    run_benchmark_suite,
    write_benchmark_artifacts,
)
from ops_platform.schemas import DecisionConstraints


def main() -> int:
    parser = argparse.ArgumentParser(description="Run reproducible benchmark suites for Ops Decision Platform.")
    parser.add_argument("--seed", type=int, default=7, help="Deterministic seed for scenario benchmarks.")
    parser.add_argument(
        "--planner-mode",
        choices=["heuristic", "cp_sat"],
        default="heuristic",
        help="Planner to use for deterministic scenario benchmarks.",
    )
    parser.add_argument(
        "--suite",
        choices=["scenarios", "streams", "both"],
        default="scenarios",
        help="Which benchmark suite to run.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "artifacts" / "benchmarks"),
        help="Directory for benchmark JSON and Markdown artifacts.",
    )
    parser.add_argument("--db-path", help="Optional SQLite path or Timescale DSN for persisted stream benchmarks.")
    parser.add_argument("--limit-streams", type=int, help="Optional limit for persisted stream benchmarks.")
    parser.add_argument("--environment", help="Optional persisted stream environment filter.")
    parser.add_argument("--source", help="Optional persisted stream source filter.")
    parser.add_argument("--max-total-cost-delta-pct", type=float, help="Optional planner cost budget.")
    parser.add_argument("--full", action="store_true", help="Print the entire suite payload instead of a short summary.")
    args = parser.parse_args()

    constraints = (
        DecisionConstraints(max_total_cost_delta_pct=args.max_total_cost_delta_pct)
        if args.max_total_cost_delta_pct is not None
        else None
    )
    output_dir = Path(args.output_dir)
    payload: dict[str, object]

    if args.suite == "scenarios":
        payload = run_benchmark_suite(
            seed=args.seed,
            planner_mode=args.planner_mode,
            decision_constraints=constraints,
        )
        json_path, markdown_path = write_benchmark_artifacts(output_dir, payload)
    elif args.suite == "streams":
        payload = benchmark_persisted_streams(
            db_path=args.db_path,
            limit=args.limit_streams,
            environment=args.environment,
            source=args.source,
            planner_mode=args.planner_mode,
            decision_constraints=constraints,
        )
        json_path, markdown_path = write_benchmark_artifacts(
            output_dir,
            payload,
            markdown_name="stream_benchmark_report.md",
            json_name="stream_benchmark_summary.json",
        )
    else:
        scenario_payload = run_benchmark_suite(
            seed=args.seed,
            planner_mode=args.planner_mode,
            decision_constraints=constraints,
        )
        stream_payload = benchmark_persisted_streams(
            db_path=args.db_path,
            limit=args.limit_streams,
            environment=args.environment,
            source=args.source,
            planner_mode=args.planner_mode,
            decision_constraints=constraints,
        )
        scenario_json, scenario_markdown = write_benchmark_artifacts(output_dir, scenario_payload)
        stream_json, stream_markdown = write_benchmark_artifacts(
            output_dir,
            stream_payload,
            markdown_name="stream_benchmark_report.md",
            json_name="stream_benchmark_summary.json",
        )
        payload = {
            "scenario_suite": scenario_payload,
            "stream_suite": stream_payload,
            "artifacts": {
                "scenario_json": str(scenario_json),
                "scenario_markdown": str(scenario_markdown),
                "stream_json": str(stream_json),
                "stream_markdown": str(stream_markdown),
            },
        }
        json_path = scenario_json
        markdown_path = scenario_markdown

    if args.full:
        print(json.dumps(payload, indent=2, default=str))
    else:
        if args.suite == "both":
            summary = {
                "scenario_suite": payload["scenario_suite"]["summary"],
                "stream_suite": payload["stream_suite"]["summary"],
                "artifacts": payload["artifacts"],
            }
        else:
            summary = {
                "suite_name": payload["suite_name"],
                "summary": payload["summary"],
                "json_artifact": str(json_path),
                "markdown_artifact": str(markdown_path),
            }
        print(json.dumps(summary, indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
