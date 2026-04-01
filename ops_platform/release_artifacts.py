from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .benchmarks import run_benchmark_suite, write_benchmark_artifacts
from .dashboard import write_artifacts
from .schemas import DecisionConstraints

ROOT = Path(__file__).resolve().parents[1]


def build_release_artifacts(
    output_dir: str | Path,
    *,
    db_path: str | Path | None = None,
    seed: int = 7,
    planner_mode: str = "heuristic",
    decision_constraints: DecisionConstraints | None = None,
) -> dict[str, Any]:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    summary_path, dashboard_path = write_artifacts(target_dir, db_path=db_path)
    live_summary_path = target_dir / "live_summary.json"

    benchmark_dir = target_dir / "benchmarks"
    benchmark_payload = run_benchmark_suite(
        seed=seed,
        planner_mode=planner_mode,
        decision_constraints=decision_constraints,
    )
    benchmark_json_path, benchmark_markdown_path = write_benchmark_artifacts(benchmark_dir, benchmark_payload)

    manifest: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "release_name": "ops-decision-platform",
        "configuration": {
            "seed": seed,
            "planner_mode": planner_mode,
            "db_path": str(db_path) if db_path is not None else None,
            "decision_constraints": decision_constraints.to_dict() if decision_constraints else None,
        },
        "benchmark": {
            "suite_name": benchmark_payload["suite_name"],
            "summary": benchmark_payload["summary"],
        },
        "artifacts": {
            "summary_json": str(summary_path),
            "live_summary_json": str(live_summary_path),
            "dashboard_html": str(dashboard_path),
            "benchmark_json": str(benchmark_json_path),
            "benchmark_markdown": str(benchmark_markdown_path),
        },
        "public_docs": {
            "benchmark_case_study": str(ROOT / "docs" / "BENCHMARK_CASE_STUDY.md"),
            "portfolio_copy": str(ROOT / "docs" / "PORTFOLIO_COPY.md"),
        },
    }

    overview_path = target_dir / "release_overview.md"
    overview_path.write_text(render_release_overview(manifest), encoding="utf-8")
    manifest["artifacts"]["release_overview_markdown"] = str(overview_path)

    manifest_path = target_dir / "release_manifest.json"
    manifest["artifacts"]["release_manifest_json"] = str(manifest_path)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def render_release_overview(manifest: dict[str, Any]) -> str:
    summary = manifest["benchmark"]["summary"]
    artifacts = manifest["artifacts"]
    public_docs = manifest["public_docs"]
    lines = [
        "# Release Overview",
        "",
        "This bundle packages the current dashboard and benchmark artifacts for the Ops Decision Platform.",
        "",
        "## Verified snapshot",
        "",
        f"- Deterministic cases: {summary['case_count']}",
        f"- Top-1 RCA accuracy: {summary['top1_root_cause_accuracy_pct']:.1f}%",
        f"- Top-2 RCA accuracy: {summary['top2_root_cause_accuracy_pct']:.1f}%",
        f"- Action match rate: {summary['action_match_rate_pct']:.1f}%",
        f"- False action rate: {summary['false_action_rate_pct']:.1f}%",
        f"- Average first actionable minute: {summary['average_first_actionable_minute']:.1f}",
        f"- Average latency protection: {summary['average_latency_protection_pct']:.1f}%",
        f"- Average baseline win rate: {summary['average_baseline_win_rate_pct']:.1f}%",
        "",
        "## Generated artifacts",
        "",
        f"- Dashboard summary JSON: `{artifacts['summary_json']}`",
        f"- Live summary JSON: `{artifacts['live_summary_json']}`",
        f"- Dashboard HTML: `{artifacts['dashboard_html']}`",
        f"- Benchmark JSON: `{artifacts['benchmark_json']}`",
        f"- Benchmark Markdown: `{artifacts['benchmark_markdown']}`",
        "",
        "## Presentation docs",
        "",
        f"- Benchmark case study: `{public_docs['benchmark_case_study']}`",
        f"- Resume and website copy: `{public_docs['portfolio_copy']}`",
        "",
        "## Rebuild command",
        "",
        "```powershell",
        "python .\\scripts\\build_release_artifacts.py",
        "```",
        "",
    ]
    return "\n".join(lines)
