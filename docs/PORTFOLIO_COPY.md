# Portfolio Copy

## Resume Bullets

### Backend / Systems Resume

- Built a shadow-mode operational decision platform for distributed systems that converts noisy telemetry into incidents, ranked root-cause hypotheses, and safer scaling or routing recommendations.
- Implemented anomaly detection, dependency-aware incident correlation, SLO burn-rate features, short-horizon forecasting, and replayable Prometheus-to-SQLite or Timescale ingestion.
- Benchmarked the system across deterministic multi-service failure scenarios, reaching 100% top-2 RCA accuracy, 100% action match, and 0% false actions against reactive baselines.

### Platform / Infra Resume

- Built a shadow-mode incident decision copilot that ingests Prometheus telemetry, evaluates live or replayed streams, and recommends safer routing or scaling actions before operators touch production.
- Added optional FastAPI, Redis-backed rate limiting, OpenTelemetry tracing, TimescaleDB storage, and OR-Tools planning to move the prototype toward a production-like shadow service.
- Packaged the project with recurring ingest workflows, benchmark reports, dashboard artifacts, audit logs, and deployable Docker Compose paths for demo and evaluation.

## Website Copy

### Short Blurb

Ops Decision Platform is a shadow-mode incident decision copilot for distributed systems. It turns noisy telemetry into incidents, explainable root-cause evidence, and safer scaling or routing recommendations, then evaluates those recommendations against reactive baselines before anything touches production.

### Medium Blurb

This project explores a practical systems question: how do you move from noisy telemetry to safer operational decisions before latency, cost, and error budgets drift too far? The platform ingests Prometheus or file-export telemetry, detects anomalies, correlates them into incidents, builds lightweight incident-graph evidence, forecasts near-term risk, and recommends safer actions in shadow mode. It persists replayable streams in SQLite or TimescaleDB, exposes API and dashboard surfaces, and benchmarks decisions against no-action and reactive baseline policies.

## Interview Opening

I built a shadow-mode decision layer for distributed systems. Instead of overclaiming auto-remediation, it observes live or replayed telemetry, clusters anomalies into incidents, ranks likely root causes with graph evidence, recommends safer scaling or routing actions, and evaluates whether those actions would have beaten reactive baselines on latency and cost. The strongest part of the project is that it is end-to-end, benchmarkable, and honest about the line between decision support and production automation.

## Demo Flow

1. Start with the thesis: signal to incident to recommendation to evaluation.
2. Show one deterministic scenario like `bad_deploy` or `queue_backlog`.
3. Highlight the incident graph, top signals, blast radius, and recommended action.
4. Show the benchmark or dashboard artifact to prove the decision beat simple baselines.
5. Close by explaining why shadow mode is the right production-like step before automation.
