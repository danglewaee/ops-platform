from __future__ import annotations

from .pipeline import run_pipeline
from .scenarios import list_scenarios


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

    @app.get("/simulate/{scenario_name}")
    def simulate(scenario_name: str):
        report = run_pipeline(scenario_name)
        return report.to_dict()

    return app

