from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.pipeline import run_pipeline_from_streams
from ops_platform.storage import load_run_bundle, list_saved_runs


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay a saved Ops Decision Platform run bundle.")
    parser.add_argument("--path", help="Path to a saved run JSON bundle.")
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Replay the newest saved run from runs/.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Print the full replay report instead of a concise comparison.",
    )
    args = parser.parse_args()

    if args.latest:
        runs = list_saved_runs()
        if not runs:
            raise SystemExit("No saved runs found in runs/.")
        path = runs[-1]["path"]
    elif args.path:
        path = args.path
    else:
        parser.error("Provide --path or --latest.")

    bundle = load_run_bundle(path)
    replay = run_pipeline_from_streams(bundle["telemetry"], bundle["events"], bundle["metadata"])

    if args.full:
        print(json.dumps(replay.to_dict(), indent=2, default=str))
        return 0

    original = bundle["report"]
    summary = {
        "path": bundle["path"],
        "scenario": replay.metadata.name,
        "original_action": original.recommendations[0].action if original.recommendations else None,
        "replayed_action": replay.recommendations[0].action if replay.recommendations else None,
        "original_root_cause": original.incidents[0].root_cause_candidates[:2] if original.incidents else [],
        "replayed_root_cause": replay.incidents[0].root_cause_candidates[:2] if replay.incidents else [],
        "matches_original_action": (
            (original.recommendations[0].action if original.recommendations else None)
            == (replay.recommendations[0].action if replay.recommendations else None)
        ),
        "matches_original_root_cause_top2": (
            (original.incidents[0].root_cause_candidates[:2] if original.incidents else [])
            == (replay.incidents[0].root_cause_candidates[:2] if replay.incidents else [])
        ),
    }
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
