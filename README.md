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

It now also includes a production-like persistence layer for replayable telemetry streams:

- SQLite-backed stream storage for normalized metrics and change events
- API endpoints to ingest bundles, inspect stored streams, and evaluate them later in shadow mode
- richer evaluation metrics such as latency protection, avoided overprovisioning, baseline win rate, and action stability

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

The test suite now covers:

- deterministic scenario correctness
- JSON run replay
- SQLite ingestion and replay
- shadow-only evaluation when ground truth is unavailable

## Optional API

The prototype is runnable today with standard Python only. If you want HTTP endpoints later, install the optional API dependencies:

```powershell
pip install -e .[api]
```

Then start the API:

```powershell
uvicorn ops_platform.api:create_app --factory --reload
```

New API surfaces for production-like flows:

- `POST /ingest/bundle`
- `POST /ingest/prometheus`
- `GET /streams`
- `GET /storage/stats`
- `POST /storage/prune`
- `GET /streams/{stream_id}`
- `GET /streams/{stream_id}/timeline`
- `POST /streams/{stream_id}/evaluate`

Durable telemetry storage lives at:

- `artifacts\ops_platform.sqlite3`

`GET /streams` now supports lightweight filtering:

- `environment`
- `source`
- `created_after`
- `created_before`
- `limit`

## Import real data exports

When you have telemetry exports from Prometheus, Grafana, CSV dashboards, or log-derived JSONL, normalize them into the platform with:

```powershell
python .\scripts\ingest_real_data.py `
  --stream-id prod-2026-03-21 `
  --telemetry .\exports\telemetry.csv `
  --events .\exports\changes.jsonl `
  --mapping .\docs\real_data_mapping.example.toml `
  --environment production `
  --source prometheus-export `
  --evaluate
```

Supported file formats:

- telemetry: `csv`, `json`, `jsonl`
- change events: `csv`, `json`, `jsonl`

The adapter will:

- rename source columns into the platform schema
- normalize service and metric aliases
- infer `step` from timestamps when the source export does not include one
- preserve extra labels like cluster or namespace in metric dimensions
- persist the normalized stream into SQLite for replay and evaluation

Use `docs\real_data_mapping.example.toml` as the starting mapping config for real exports.

## Import directly from Prometheus

When you have direct Prometheus access, query a time window and persist it as a stream with:

```powershell
python .\scripts\ingest_prometheus.py `
  --config .\docs\prometheus_queries.example.toml `
  --stream-id prom-prod-2026-03-21 `
  --lookback-minutes 30 `
  --environment production `
  --evaluate
```

The Prometheus adapter will:

- run `query_range` for each configured metric
- normalize series into `MetricSample`
- preserve extra Prometheus labels in metric dimensions
- optionally merge a separate change-event file
- support relative windows with `--lookback-minutes`
- persist and evaluate the resulting stream through the same shadow-mode pipeline

Use `docs\prometheus_queries.example.toml` as the starting query config.

## Run the recurring pull workflow

When you want one command that pulls Prometheus, ingests into SQLite, evaluates shadow mode, and prunes old streams, use:

```powershell
python .\scripts\run_recurring_pull.py `
  --config .\docs\prometheus_queries.example.toml
```

The recurring workflow reads these sections from the same Prometheus config:

- `[recurring]` for lookback window, environment, source, stream prefix, evaluation, and summary path
- `[retention]` for `older_than_days`, `keep_latest`, and `vacuum`

You can still override them at runtime, for example:

```powershell
python .\scripts\run_recurring_pull.py `
  --config .\docs\prometheus_queries.example.toml `
  --lookback-minutes 15 `
  --environment staging `
  --db-path .\artifacts\ops_platform.sqlite3
```

## Inspect and prune storage

Check the current SQLite footprint:

```powershell
python .\scripts\prune_storage.py --stats-only
```

Dry-run a retention rule before deleting anything:

```powershell
python .\scripts\prune_storage.py `
  --environment production `
  --source prometheus `
  --older-than-days 7 `
  --keep-latest 24 `
  --dry-run
```

Apply the retention rule and compact the SQLite file:

```powershell
python .\scripts\prune_storage.py `
  --environment production `
  --source prometheus `
  --older-than-days 7 `
  --keep-latest 24 `
  --vacuum
```

The retention logic operates at the `stream_id` level, which matches the rolling-window ingestion model used by the file and Prometheus adapters.

## What "good" looks like next

This prototype is ready for the next layer of work:

- stronger scenario coverage
- better root-cause ranking features
- latency and cost baselines against reactive policies
- a small dashboard for incidents and recommendations
