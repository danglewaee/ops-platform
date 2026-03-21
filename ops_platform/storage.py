from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from .schemas import ChangeEvent, MetricSample, PipelineReport, ScenarioMetadata

RUNS_DIR = Path(__file__).resolve().parents[1] / "runs"


def save_run_bundle(
    telemetry: list[MetricSample],
    events: list[ChangeEvent],
    metadata: ScenarioMetadata,
    report: PipelineReport,
    *,
    seed: int,
    output_dir: Path | None = None,
) -> Path:
    target_dir = output_dir or RUNS_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    path = target_dir / f"{metadata.name}-{timestamp}.json"
    payload = {
        "saved_at": datetime.now().isoformat(),
        "seed": seed,
        "metadata": asdict(metadata),
        "telemetry": [asdict(sample) for sample in telemetry],
        "events": [asdict(event) for event in events],
        "report": report.to_dict(),
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def load_run_bundle(path: str | Path) -> dict[str, Any]:
    run_path = Path(path)
    payload = json.loads(run_path.read_text(encoding="utf-8"))
    telemetry = [MetricSample.from_dict(item) for item in payload["telemetry"]]
    events = [ChangeEvent.from_dict(item) for item in payload["events"]]
    metadata = ScenarioMetadata.from_dict(payload["metadata"])
    report = PipelineReport.from_dict(payload["report"])
    return {
        "path": str(run_path),
        "saved_at": payload.get("saved_at"),
        "seed": payload.get("seed"),
        "metadata": metadata,
        "telemetry": telemetry,
        "events": events,
        "report": report,
    }


def list_saved_runs(output_dir: Path | None = None) -> list[dict[str, Any]]:
    target_dir = output_dir or RUNS_DIR
    if not target_dir.exists():
        return []

    runs: list[dict[str, Any]] = []
    for path in sorted(target_dir.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        metadata = payload.get("metadata", {})
        report = payload.get("report", {})
        evaluation = report.get("evaluation", {})
        runs.append(
            {
                "path": str(path),
                "saved_at": payload.get("saved_at"),
                "seed": payload.get("seed"),
                "scenario": metadata.get("name"),
                "category": metadata.get("category"),
                "root_cause": metadata.get("root_cause"),
                "expected_action": metadata.get("expected_action"),
                "recommended_action": (
                    report.get("recommendations", [{}])[0].get("action")
                    if report.get("recommendations")
                    else None
                ),
                "recommended_action_match": evaluation.get("recommended_action_match"),
            }
        )
    return runs
