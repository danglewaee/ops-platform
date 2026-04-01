from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.release_artifacts import build_release_artifacts
from ops_platform.schemas import DecisionConstraints


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a release-ready artifact bundle for portfolio and demo use.")
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "artifacts" / "release"),
        help="Directory where dashboard, benchmark, and manifest artifacts will be written.",
    )
    parser.add_argument("--db-path", help="Optional SQLite path or Timescale/PostgreSQL DSN for live summary generation.")
    parser.add_argument("--seed", type=int, default=7, help="Deterministic seed for scenario benchmarks.")
    parser.add_argument(
        "--planner-mode",
        choices=["heuristic", "cp_sat"],
        default="heuristic",
        help="Planner mode for deterministic benchmark generation.",
    )
    parser.add_argument("--max-total-cost-delta-pct", type=float, help="Optional decision planner cost budget.")
    parser.add_argument("--full", action="store_true", help="Print the full release manifest.")
    args = parser.parse_args()

    constraints = (
        DecisionConstraints(max_total_cost_delta_pct=args.max_total_cost_delta_pct)
        if args.max_total_cost_delta_pct is not None
        else None
    )
    manifest = build_release_artifacts(
        args.output_dir,
        db_path=args.db_path,
        seed=args.seed,
        planner_mode=args.planner_mode,
        decision_constraints=constraints,
    )

    if args.full:
        print(json.dumps(manifest, indent=2))
    else:
        print(
            json.dumps(
                {
                    "benchmark": manifest["benchmark"]["summary"],
                    "artifacts": manifest["artifacts"],
                    "public_docs": manifest["public_docs"],
                },
                indent=2,
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
