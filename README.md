# Ops Decision Platform

A standalone prototype for a shadow-mode operational decision layer:

- ingest telemetry
- detect anomalies
- cluster incidents
- forecast short-term load
- recommend safer routing or scaling actions
- evaluate against simple reactive baselines

This project intentionally keeps the scope tight. It is meant to be the first working version of a stronger flagship systems project, not a bloated "AI ops" demo with too many moving parts.

## Why this exists

Modern distributed systems rarely fail because teams lack telemetry. They fail when too much noisy telemetry arrives faster than people can reason about it, just as latency, error rates, and cost start moving in the wrong direction.

This prototype explores one concrete question:

> How can an operator move from noisy signals to safer decisions before the system degrades further?

## MVP scope

The current prototype has six layers:

1. `simulator`
   - Generates realistic-but-controlled failure scenarios.
2. `ingestion`
   - Produces a normalized timeline of metrics and change events.
3. `detection`
   - Finds abnormal behavior from rolling baselines and deviation scores.
4. `incident_engine`
   - Clusters related anomalies into incidents and ranks likely root causes.
5. `forecasting`
   - Estimates short-horizon demand and latency risk.
6. `decision_engine`
   - Recommends actions in shadow mode and evaluates them against scenario ground truth.

The current scenario set covers:

- capacity shock (`traffic_spike`)
- deploy regression (`bad_deploy`)
- throughput degradation (`queue_backlog`)
- service degradation (`memory_leak`)
- transient noise where the right decision is to do nothing (`transient_noise`)

## Folder structure

```text
ops-decision-platform/
  docs/
    ARCHITECTURE.md
  ops_platform/
    __init__.py
    api.py
    decision_engine.py
    detection.py
    forecasting.py
    incident_engine.py
    pipeline.py
    scenarios.py
    schemas.py
    simulator.py
  scripts/
    run_demo.py
  tests/
  pyproject.toml
```

## Quick start

Run a demo scenario from this folder:

```powershell
cd "D:\CODE\Personal Website\ops-decision-platform"
python .\scripts\run_demo.py --scenario traffic_spike
```

The demo runner prints a concise summary by default. Use `--full` when you want the entire anomaly, incident, forecast, and recommendation report:

```powershell
python .\scripts\run_demo.py --scenario traffic_spike --full
```

Other available scenarios:

- `traffic_spike`
- `bad_deploy`
- `queue_backlog`
- `memory_leak`
- `transient_noise`

Run the full scenario matrix:

```powershell
python .\scripts\run_demo.py --matrix
```

Build the lightweight evaluation dashboard:

```powershell
python .\scripts\build_artifacts.py
```

This writes:
- `artifacts\summary.json`
- `artifacts\dashboard.html`

Serve a live local dashboard without extra dependencies:

```powershell
python .\scripts\serve_dashboard.py
```

Then open:
- `http://127.0.0.1:8008/`
- `http://127.0.0.1:8008/api/summary`

Run the deterministic core tests:

```powershell
python -m unittest discover -s tests -v
```

## Optional API

The prototype is runnable today with standard Python only. If you want HTTP endpoints later, install the optional API dependencies:

```powershell
pip install -e .[api]
```

Then start the API:

```powershell
uvicorn ops_platform.api:create_app --factory --reload
```

## What "good" looks like next

This prototype is ready for the next layer of work:

- stronger scenario coverage
- better root-cause ranking features
- latency and cost baselines against reactive policies
- a small dashboard for incidents and recommendations
