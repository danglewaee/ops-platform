from __future__ import annotations

from .pipeline import run_pipeline, run_pipeline_from_streams, run_scenario_matrix
from .scenarios import SCENARIOS, list_scenarios
from .storage import list_saved_runs, load_run_bundle


def _report_summary(report):
    recommendation = report.recommendations[0] if report.recommendations else None
    return {
        "scenario": report.metadata.name,
        "description": report.metadata.description,
        "category": report.metadata.category,
        "root_cause": report.metadata.root_cause,
        "expected_action": report.metadata.expected_action,
        "incident_count": report.evaluation.incident_count,
        "anomaly_count": report.evaluation.anomaly_count,
        "alert_reduction_pct": report.evaluation.alert_reduction_pct,
        "top2_root_cause_hit": report.evaluation.top2_root_cause_hit,
        "recommended_action_match": report.evaluation.recommended_action_match,
        "decision_latency_ms": report.evaluation.decision_latency_ms,
        "recommendation": {
            "action": recommendation.action if recommendation else None,
            "target_service": recommendation.target_service if recommendation else None,
            "confidence": recommendation.confidence if recommendation else None,
        },
        "baselines": report.evaluation.baseline_comparisons,
    }


def create_app():
    try:
        from fastapi import FastAPI
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "FastAPI is not installed. Run `pip install -e .[api]` inside ops-decision-platform first."
        ) from exc

    app = FastAPI(title="Ops Decision Platform", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/scenarios")
    def scenarios() -> dict[str, list[str]]:
        return {"scenarios": list_scenarios()}

    @app.get("/scenarios/catalog")
    def scenario_catalog() -> list[dict[str, object]]:
        return [
            {
                "name": metadata.name,
                "description": metadata.description,
                "root_cause": metadata.root_cause,
                "expected_action": metadata.expected_action,
                "impacted_services": metadata.impacted_services,
                "category": metadata.category,
            }
            for metadata in SCENARIOS.values()
        ]

    @app.get("/simulate/{scenario_name}")
    def simulate(scenario_name: str, seed: int = 7):
        report = run_pipeline(scenario_name, seed=seed)
        return report.to_dict()

    @app.get("/simulate/{scenario_name}/summary")
    def simulate_summary(scenario_name: str, seed: int = 7):
        report = run_pipeline(scenario_name, seed=seed)
        return _report_summary(report)

    @app.get("/matrix")
    def matrix(seed: int = 7):
        reports = run_scenario_matrix(seed=seed)
        return [_report_summary(report) for report in reports]

    @app.get("/runs")
    def runs():
        return list_saved_runs()

    @app.get("/runs/replay")
    def replay_run(path: str):
        bundle = load_run_bundle(path)
        replay = run_pipeline_from_streams(bundle["telemetry"], bundle["events"], bundle["metadata"])
        return _report_summary(replay)

    return app
