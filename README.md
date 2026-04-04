# Ops Decision Platform

A shadow-mode incident decision copilot for distributed systems.

It ingests noisy telemetry, detects anomalies, clusters incidents, ranks likely root causes with evidence, forecasts near-term risk, recommends safer scaling or routing actions, and evaluates those actions against reactive baselines before anything touches production.

At a glance:

- ingest telemetry
- detect anomalies
- cluster incidents
- forecast short-term load
- recommend safer routing or scaling actions
- evaluate against simple reactive baselines
- persist replayable live streams in SQLite or TimescaleDB
- benchmark decisions with deterministic scenario suites and dashboard artifacts

This project intentionally keeps the scope tight. It is meant to be the first working version of a stronger flagship systems project, not a bloated "AI ops" demo with too many moving parts.

The current deterministic benchmark reaches:

- `100%` top-2 RCA accuracy
- `100%` action match across `5/5` built-in scenarios
- `0%` false action rate

## Why this exists

Modern distributed systems rarely fail because teams lack telemetry. They fail when too much noisy telemetry arrives faster than people can reason about it, just as latency, error rates, and cost start moving in the wrong direction.

This prototype explores one concrete question:

> How can an operator move from noisy signals to safer decisions before the system degrades further?

## MVP scope

The current prototype has seven layers:

1. `simulator`
   - Generates realistic-but-controlled failure scenarios.
2. `ingestion`
   - Produces a normalized timeline of metrics and change events.
3. `detection`
   - Finds abnormal behavior from rolling baselines and deviation scores.
4. `incident_engine`
   - Clusters related anomalies into incidents, builds a lightweight incident graph, and ranks likely root causes with evidence.
5. `forecasting`
   - Estimates short-horizon demand, SLO burn, and latency risk.
6. `feature_builder`
   - Builds service-level SLI/SLO snapshots, budget pressure, and dominant burn signals.
7. `decision_engine`
   - Recommends actions in shadow mode with SLO-aware policy signals and evaluates them against scenario ground truth.

It now also includes a production-like persistence layer for replayable telemetry streams:

- SQLite-backed stream storage for normalized metrics and change events
- API endpoints to ingest bundles, inspect stored streams, and evaluate them later in shadow mode
- richer evaluation metrics such as latency protection, avoided overprovisioning, baseline win rate, and action stability
- explainable incident output with top signals, blast radius, dependency edges, and RCA evidence

The current scenario set covers:

- capacity shock (`traffic_spike`)
- deploy regression (`bad_deploy`)
- throughput degradation (`queue_backlog`)
- service degradation (`memory_leak`)
- transient noise where the right decision is to do nothing (`transient_noise`)

V2 also includes a production-style testbed profile:

- `boutique_like` for storefront-style microservices with `frontend`, `checkout`, `payment`, `cart`, `recommendation`, `email`, `shipping`, `currency`, and `redis`
- scenario pack: `boutique_frontend_spike`, `boutique_bad_canary`, `boutique_payment_timeout`, `boutique_email_backlog`, `boutique_cache_jitter`

## Folder structure

```text
ops-decision-platform/
  docs/
    ARCHITECTURE.md
    BENCHMARK_CASE_STUDY.md
    PORTFOLIO_COPY.md
  ops_platform/
    __init__.py
    api.py
    decision_engine.py
    detection.py
    feature_builder.py
    forecasting.py
    incident_engine.py
    planner.py
    pipeline.py
    release_artifacts.py
    scenarios.py
    schemas.py
    settings.py
    simulator.py
    telemetry.py
    testbed.py
    timescale_storage.py
  scripts/
    bootstrap_storage.py
    build_release_artifacts.py
    run_benchmarks.py
    init_timescale.py
    run_api.py
    run_demo.py
    smoke_docker_stack.py
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

List scenarios for a specific testbed profile:

```powershell
python .\scripts\run_demo.py --testbed-profile boutique_like --list-scenarios
```

Run a V2 boutique-like testbed scenario:

```powershell
python .\scripts\run_demo.py --testbed-profile boutique_like --scenario boutique_payment_timeout
```

Build the lightweight evaluation dashboard:

```powershell
python .\scripts\build_artifacts.py
```

This writes:
- `artifacts\summary.json`
- `artifacts\live_summary.json`
- `artifacts\dashboard.html`

Serve a live local dashboard without extra dependencies:

```powershell
python .\scripts\serve_dashboard.py --db-path .\artifacts\ops_platform.sqlite3
```

Then open:
- `http://127.0.0.1:8008/`
- `http://127.0.0.1:8008/api/summary`
- `http://127.0.0.1:8008/api/live-summary`

The local dashboard server supports the same lightweight live filters as the storage layer:

- `limit`
- `environment`
- `source`

Run the deterministic core tests:

```powershell
python -m unittest discover -s tests -v
```

The test suite now covers:

- deterministic scenario correctness
- JSON run replay
- SQLite ingestion and replay
- shadow-only evaluation when ground truth is unavailable
- planner constraints and recurring observability config
- SLO burn-rate and budget-pressure feature signals
- reproducible benchmark aggregation and Markdown/JSON artifact generation

Run the reproducible benchmark suite:

```powershell
python .\scripts\run_benchmarks.py --suite scenarios
```

Run the V2 boutique-like benchmark pack:

```powershell
python .\scripts\run_benchmarks.py --suite scenarios --testbed-profile boutique_like
```

See [docs/BENCHMARK_CASE_STUDY.md](docs/BENCHMARK_CASE_STUDY.md) for the current deterministic benchmark summary, scenario highlights, and guidance on how to present the results honestly as a shadow-mode case study.
See [docs/TESTBED_V2_CASE_STUDY.md](docs/TESTBED_V2_CASE_STUDY.md) for the V2 boutique-like microservice testbed benchmark pack.

This writes benchmark artifacts to:

- `artifacts\benchmarks\benchmark_summary.json`
- `artifacts\benchmarks\benchmark_report.md`

If you want to benchmark persisted streams instead of simulator scenarios:

```powershell
python .\scripts\run_benchmarks.py `
  --suite streams `
  --db-path .\artifacts\ops_platform.sqlite3 `
  --environment production `
  --source prometheus
```

The same CLI can be pointed at a Timescale/PostgreSQL DSN instead of SQLite when you want to benchmark persisted live-ingest streams from the production-like storage path.

Build a release-ready demo bundle for portfolio or interview use:

```powershell
python .\scripts\build_release_artifacts.py
```

This writes:

- `artifacts\release\summary.json`
- `artifacts\release\live_summary.json`
- `artifacts\release\dashboard.html`
- `artifacts\release\benchmarks\benchmark_summary.json`
- `artifacts\release\benchmarks\benchmark_report.md`
- `artifacts\release\benchmarks\boutique_like_benchmark_summary.json`
- `artifacts\release\benchmarks\boutique_like_benchmark_report.md`
- `artifacts\release\release_overview.md`
- `artifacts\release\release_manifest.json`

For public-facing copy, use:

- [docs/BENCHMARK_CASE_STUDY.md](docs/BENCHMARK_CASE_STUDY.md)
- [docs/TESTBED_V2_CASE_STUDY.md](docs/TESTBED_V2_CASE_STUDY.md)
- [docs/PORTFOLIO_COPY.md](docs/PORTFOLIO_COPY.md)

## Optional API

The prototype is runnable today with standard Python only. If you want HTTP endpoints later, install the optional API dependencies:

```powershell
pip install -e .[api]
```

Optional upgrade extras:

```powershell
pip install -e .[observability]
pip install -e .[planner]
pip install -e .[security]
pip install -e .[timeseries]
pip install -e .[full]
```

- `observability` adds OpenTelemetry tracing support.
- `planner` adds the optional OR-Tools CP-SAT decision planner.
- `security` adds the optional Redis-backed rate limiter.
- `timeseries` adds the optional TimescaleDB/PostgreSQL backend via `psycopg`.
- `full` installs API, OpenTelemetry, OR-Tools, Redis, and TimescaleDB support together.

Then start the API:

```powershell
uvicorn ops_platform.api:create_app --factory --reload
```

Or run it through the new env-aware wrapper:

```powershell
python .\scripts\run_api.py
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

Every `db_path` argument in the API and CLI can also accept a Timescale/PostgreSQL DSN such as:

- `postgresql://user:password@host:5432/ops_platform`
- `timescaledb://user:password@host:5432/ops_platform`

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

## Use TimescaleDB Instead Of SQLite

If you want the persistence layer to look more production-like, initialize a TimescaleDB backend with:

```powershell
python .\scripts\init_timescale.py `
  --db-url postgresql://user:password@host:5432/ops_platform `
  --metric-retention-days 30 `
  --event-retention-days 14 `
  --compress-after-days 7 `
  --create-metric-rollup
```

This script will:

- create the core tables in PostgreSQL
- convert `metric_samples` and `change_events` into Timescale hypertables
- optionally add retention and compression policies
- optionally create a continuous aggregate over the metrics stream

Once initialized, you can point the existing ingestion and recurring workflows at the same backend by passing the DSN through `--db-path`.

## Run the recurring pull workflow

When you want one command that pulls Prometheus, ingests into SQLite, evaluates shadow mode, and prunes old streams, use:

```powershell
python .\scripts\run_recurring_pull.py `
  --config .\docs\prometheus_queries.example.toml
```

The recurring workflow reads these sections from the same Prometheus config:

- `[recurring]` for lookback window, environment, source, stream prefix, evaluation, and summary path
- `[decision]` for planner constraints such as cost budgets and allowed action types
- `[observability]` for OpenTelemetry tracing toggles and OTLP endpoint
- `[retention]` for `older_than_days`, `keep_latest`, and `vacuum`

You can still override them at runtime, for example:

```powershell
python .\scripts\run_recurring_pull.py `
  --config .\docs\prometheus_queries.example.toml `
  --lookback-minutes 15 `
  --environment staging `
  --planner-mode cp_sat `
  --max-total-cost-delta-pct 12 `
  --enable-tracing `
  --otlp-endpoint http://localhost:4318/v1/traces `
  --db-path postgresql://user:password@host:5432/ops_platform
```

The recurring summary now includes:

- the planner actually used (`heuristic` or `cp_sat`)
- the trace id when OpenTelemetry is enabled and available
- whether tracing was configured successfully for the run

## Deploy With Docker Compose

The repository now includes two container paths:

- `docker-compose.smoke.yml` for a fast SQLite-backed API smoke stack used by CI
- `docker-compose.yml` for the fuller production-like stack with `timescaledb`, `redis`, `otel-collector`, and one-shot `timescale-init`
- `docker-compose.deploy.yml` for a single-host deployment bundle with `caddy`, `api`, `timescaledb`, `redis`, and an optional recurring worker profile

The full stack includes:

- `api`
- `timescaledb`
- `otel-collector`
- `redis`
- one-shot `timescale-init`

Start by copying the sample environment file:

```powershell
Copy-Item .\.env.example .\.env
```

Then bring the stack up:

```powershell
docker compose up --build
```

For the fast SQLite smoke stack, run:

```powershell
Copy-Item .\.env.smoke.example .\.env.smoke
docker compose --env-file .\.env.smoke -f .\docker-compose.smoke.yml up --build
```

Or run the smoke script against that same env file:

```powershell
python .\scripts\smoke_docker_stack.py --compose-file .\docker-compose.smoke.yml --env-file .\.env.smoke
```

If you only want the compose command:

```powershell
docker compose -f .\docker-compose.smoke.yml up --build
```

The repository also includes:

- [.github/workflows/compose-smoke.yml](D:/CODE/Personal%20Website/ops-decision-platform/.github/workflows/compose-smoke.yml) for the default SQLite-backed CI smoke path
- [.github/workflows/timescale-integration.yml](D:/CODE/Personal%20Website/ops-decision-platform/.github/workflows/timescale-integration.yml) for manual full-stack Timescale integration runs with uploaded compose logs and summaries
- [.github/workflows/deploy-config-validate.yml](D:/CODE/Personal%20Website/ops-decision-platform/.github/workflows/deploy-config-validate.yml) for deploy-bundle config validation across the base stack and `worker` profile
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for the deployable single-host bundle with reverse proxy and optional recurring worker

For deploy packaging, start from:

```powershell
Copy-Item .\.env.deploy.example .\.env.deploy
Copy-Item .\deploy\recurring_pull.example.toml .\deploy\recurring_pull.toml
python .\scripts\check_deploy_bundle.py --env-file .\.env.deploy --full
docker compose --env-file .\.env.deploy -f .\docker-compose.deploy.yml up --build -d
```

If you want the recurring Prometheus worker too:

```powershell
docker compose --env-file .\.env.deploy -f .\docker-compose.deploy.yml --profile worker up --build -d
```

`OPS_PLATFORM_API_PORT` controls the port the app listens on inside the container. `OPS_PLATFORM_HOST_PORT` controls the host-side published port. For smoke runs, keep the internal port at `8000` and move the host port if `8000` is already occupied.

Key runtime settings are read from environment variables:

- `OPS_PLATFORM_DB_PATH`
- `OPS_PLATFORM_AUTO_INIT_STORAGE`
- `OPS_PLATFORM_AUTH_ENABLED`
- `OPS_PLATFORM_API_KEYS`
- `OPS_PLATFORM_RATE_LIMIT_ENABLED`
- `OPS_PLATFORM_RATE_LIMIT_BACKEND`
- `OPS_PLATFORM_RATE_LIMIT_REQUESTS`
- `OPS_PLATFORM_RATE_LIMIT_WINDOW_SECONDS`
- `OPS_PLATFORM_REDIS_URL`
- `OPS_PLATFORM_AUDIT_LOG_ENABLED`
- `OPS_PLATFORM_DB_RETRY_ATTEMPTS`
- `OPS_PLATFORM_DB_RETRY_BACKOFF_SECONDS`
- `OPS_PLATFORM_DB_RETRY_MAX_BACKOFF_SECONDS`
- `OPS_PLATFORM_ENABLE_TRACING`
- `OPS_PLATFORM_OTLP_ENDPOINT`
- `OPS_PLATFORM_TIMESCALE_METRIC_RETENTION_DAYS`
- `OPS_PLATFORM_TIMESCALE_EVENT_RETENTION_DAYS`
- `OPS_PLATFORM_TIMESCALE_COMPRESS_AFTER_DAYS`

Important endpoints in the containerized stack:

- `http://127.0.0.1:8000/health`
- `http://127.0.0.1:8000/ready`
- `http://127.0.0.1:8000/streams`
- `http://127.0.0.1:8000/audit/events`

When auth is enabled, send:

- `X-API-Key: <your token>`
- `X-Ops-Actor: <human or service id>`

The API now emits request-edge control headers:

- `X-Request-Id`
- `X-RateLimit-Limit`
- `X-RateLimit-Remaining`

Rate limiting can now run in two modes:

- `memory` for single-instance local runs
- `redis` for shared counters across multiple API replicas

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
