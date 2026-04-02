from __future__ import annotations

import json
from pathlib import Path
from statistics import mean
from typing import Any

from .pipeline import run_pipeline, run_pipeline_from_streams
from .schemas import DecisionConstraints, PipelineReport, ScenarioMetadata
from .scenarios import list_scenarios
from .storage import list_ingested_streams, load_ingested_stream


def run_benchmark_suite(
    *,
    seed: int = 7,
    planner_mode: str = "heuristic",
    decision_constraints: DecisionConstraints | None = None,
    scenario_names: list[str] | None = None,
    testbed_profile: str = "core",
) -> dict[str, Any]:
    selected = scenario_names or list_scenarios(profile=testbed_profile)
    reports = [
        run_pipeline(
            scenario_name,
            seed=seed,
            planner_mode=planner_mode,
            decision_constraints=decision_constraints,
            testbed_profile=testbed_profile,
        )
        for scenario_name in selected
    ]
    suite_name = "deterministic-scenarios" if testbed_profile == "core" else f"{testbed_profile}-scenarios"
    return _build_suite_payload(
        suite_name=suite_name,
        suite_description=f"Deterministic simulator benchmark across the '{testbed_profile}' scenario pack.",
        reports=reports,
        suite_metadata={
            "seed": seed,
            "planner_mode": planner_mode,
            "testbed_profile": testbed_profile,
            "scenario_names": selected,
        },
    )


def benchmark_persisted_streams(
    *,
    db_path: str | Path | None = None,
    limit: int | None = None,
    environment: str | None = None,
    source: str | None = None,
    planner_mode: str = "heuristic",
    decision_constraints: DecisionConstraints | None = None,
) -> dict[str, Any]:
    streams = list_ingested_streams(
        limit=limit,
        environment=environment,
        source=source,
        db_path=db_path,
    )
    reports: list[PipelineReport] = []

    for stream in streams:
        loaded = load_ingested_stream(stream["stream_id"], db_path=db_path)
        latest_report = loaded["latest_report"]
        if latest_report is not None:
            reports.append(latest_report["report"])
            continue

        metadata = _resolve_stream_metadata(loaded)
        reports.append(
            run_pipeline_from_streams(
                loaded["telemetry"],
                loaded["events"],
                metadata,
                planner_mode=planner_mode,
                decision_constraints=decision_constraints,
            )
        )

    return _build_suite_payload(
        suite_name="persisted-streams",
        suite_description="Replay benchmark across persisted telemetry streams with latest shadow reports.",
        reports=reports,
        suite_metadata={
            "db_path": str(db_path) if db_path is not None else None,
            "environment": environment,
            "source": source,
            "stream_count": len(streams),
        },
    )


def write_benchmark_artifacts(
    output_dir: str | Path,
    suite_payload: dict[str, Any],
    *,
    markdown_name: str = "benchmark_report.md",
    json_name: str = "benchmark_summary.json",
) -> tuple[Path, Path]:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    markdown_path = target_dir / markdown_name
    json_path = target_dir / json_name

    markdown_path.write_text(render_benchmark_markdown(suite_payload), encoding="utf-8")
    json_path.write_text(json.dumps(suite_payload, indent=2, default=str), encoding="utf-8")
    return json_path, markdown_path


def render_benchmark_markdown(suite_payload: dict[str, Any]) -> str:
    summary = suite_payload["summary"]
    rows = [
        "| Scenario | Category | Root Cause | Action | Top-1 | Top-2 | Action Match | First Actionable (min) | False Action |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for case in suite_payload["cases"]:
        rows.append(
            "| {scenario} | {category} | {root} | {action} | {top1} | {top2} | {action_match} | {first_actionable} | {false_action} |".format(
                scenario=case["scenario"],
                category=case["category"],
                root=case["root_cause"],
                action=case["recommended_action"] or "n/a",
                top1=_status(case["top1_root_cause_hit"]),
                top2=_status(case["top2_root_cause_hit"]),
                action_match=_status(case["recommended_action_match"]),
                first_actionable=case["first_actionable_minute"],
                false_action=_status(case["false_action"]),
            )
        )

    lines = [
        f"# {suite_payload['suite_name'].replace('-', ' ').title()}",
        "",
        suite_payload["suite_description"],
        "",
        "## Summary",
        "",
        f"- Cases: {summary['case_count']}",
        f"- Top-1 RCA accuracy: {summary['top1_root_cause_accuracy_pct']:.1f}%",
        f"- Top-2 RCA accuracy: {summary['top2_root_cause_accuracy_pct']:.1f}%",
        f"- Action match rate: {summary['action_match_rate_pct']:.1f}%",
        f"- False action rate: {summary['false_action_rate_pct']:.1f}%",
        f"- Average first actionable minute: {summary['average_first_actionable_minute']:.1f}",
        f"- Average decision latency: {summary['average_decision_latency_ms']:.3f} ms",
        f"- Average latency protection: {summary['average_latency_protection_pct']:.1f}%",
        f"- Average baseline win rate: {summary['average_baseline_win_rate_pct']:.1f}%",
        "",
        "## Cases",
        "",
        *rows,
    ]
    return "\n".join(lines) + "\n"


def _build_suite_payload(
    *,
    suite_name: str,
    suite_description: str,
    reports: list[PipelineReport],
    suite_metadata: dict[str, Any],
) -> dict[str, Any]:
    cases = [_case_payload(report) for report in reports]
    return {
        "suite_name": suite_name,
        "suite_description": suite_description,
        "summary": _suite_summary(cases),
        "cases": cases,
        "metadata": suite_metadata,
    }


def _case_payload(report: PipelineReport) -> dict[str, Any]:
    incident = report.incidents[0] if report.incidents else None
    recommendation = report.recommendations[0] if report.recommendations else None
    first_actionable_minute = _first_actionable_minute(report)
    top1_hit = (
        bool(incident and incident.root_cause_candidates and incident.root_cause_candidates[0] == report.metadata.root_cause)
        if report.evaluation.top2_root_cause_hit is not None
        else None
    )
    action_match = report.evaluation.recommended_action_match

    return {
        "scenario": report.metadata.name,
        "testbed_profile": report.metadata.testbed_profile,
        "description": report.metadata.description,
        "category": report.metadata.category,
        "root_cause": report.metadata.root_cause,
        "expected_action": report.metadata.expected_action,
        "recommended_action": recommendation.action if recommendation else None,
        "planner_mode": report.evaluation.planner_mode,
        "top1_root_cause_hit": top1_hit,
        "top2_root_cause_hit": report.evaluation.top2_root_cause_hit,
        "recommended_action_match": action_match,
        "false_action": (not action_match) if action_match is not None else None,
        "first_actionable_minute": first_actionable_minute,
        "decision_latency_ms": report.evaluation.decision_latency_ms,
        "alert_reduction_pct": report.evaluation.alert_reduction_pct,
        "latency_protection_pct": report.evaluation.latency_protection_pct,
        "baseline_win_rate_pct": report.evaluation.baseline_win_rate_pct,
        "average_cost_delta_pct": report.evaluation.average_cost_delta_pct,
        "average_p95_delta_ms": report.evaluation.average_p95_delta_ms,
        "service_health": [
            {
                "service": item.service,
                "projected_burn_rate": item.projected_burn_rate,
                "budget_pressure": item.budget_pressure,
                "dominant_signal": item.dominant_signal,
            }
            for item in report.service_health
        ],
        "top_signals": incident.top_signals if incident else [],
        "blast_radius_services": incident.blast_radius_services if incident else [],
        "rca_evidence": [item.summary for item in incident.evidence] if incident else [],
        "baseline_comparisons": report.evaluation.baseline_comparisons,
    }


def _suite_summary(cases: list[dict[str, Any]]) -> dict[str, Any]:
    if not cases:
        return {
            "case_count": 0,
            "top1_root_cause_accuracy_pct": 0.0,
            "top2_root_cause_accuracy_pct": 0.0,
            "action_match_rate_pct": 0.0,
            "false_action_rate_pct": 0.0,
            "average_first_actionable_minute": 0.0,
            "average_decision_latency_ms": 0.0,
            "average_alert_reduction_pct": 0.0,
            "average_latency_protection_pct": 0.0,
            "average_baseline_win_rate_pct": 0.0,
            "average_cost_delta_pct": 0.0,
            "average_p95_delta_ms": 0.0,
        }

    return {
        "case_count": len(cases),
        "top1_root_cause_accuracy_pct": round(_truth_rate(cases, "top1_root_cause_hit"), 2),
        "top2_root_cause_accuracy_pct": round(_truth_rate(cases, "top2_root_cause_hit"), 2),
        "action_match_rate_pct": round(_truth_rate(cases, "recommended_action_match"), 2),
        "false_action_rate_pct": round(_truth_rate(cases, "false_action"), 2),
        "average_first_actionable_minute": round(mean(case["first_actionable_minute"] for case in cases), 2),
        "average_decision_latency_ms": round(mean(case["decision_latency_ms"] for case in cases), 3),
        "average_alert_reduction_pct": round(mean(case["alert_reduction_pct"] for case in cases), 2),
        "average_latency_protection_pct": round(mean(case["latency_protection_pct"] for case in cases), 2),
        "average_baseline_win_rate_pct": round(mean(case["baseline_win_rate_pct"] for case in cases), 2),
        "average_cost_delta_pct": round(mean(case["average_cost_delta_pct"] for case in cases), 2),
        "average_p95_delta_ms": round(mean(case["average_p95_delta_ms"] for case in cases), 2),
    }


def _truth_rate(cases: list[dict[str, Any]], field: str) -> float:
    relevant = [case[field] for case in cases if case[field] is not None]
    if not relevant:
        return 0.0
    return sum(1 for value in relevant if value) / len(relevant) * 100


def _first_actionable_minute(report: PipelineReport) -> float:
    if not report.anomalies:
        return 0.0
    return float(min(anomaly.step for anomaly in report.anomalies))


def _resolve_stream_metadata(stream_payload: dict[str, Any]) -> ScenarioMetadata:
    metadata = stream_payload.get("metadata", {}) or {}
    impacted_services = sorted({sample.service for sample in stream_payload["telemetry"]})
    stream_id = str(stream_payload["stream_id"])
    return ScenarioMetadata(
        name=str(metadata.get("name") or stream_id),
        description=str(metadata.get("description") or "Persisted telemetry replay benchmark."),
        root_cause=str(metadata.get("root_cause") or ""),
        expected_action=str(metadata.get("expected_action") or ""),
        impacted_services=list(metadata.get("impacted_services") or impacted_services),
        category=str(metadata.get("category") or "live"),
    )


def _status(value: bool | None) -> str:
    if value is None:
        return "n/a"
    return "yes" if value else "no"
