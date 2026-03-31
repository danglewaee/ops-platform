# Benchmark Case Study

## Overview

This case study documents the deterministic simulator benchmark for the Ops Decision Platform after the addition of:

- SLO-aware burn-rate features
- incident graph output
- explainable RCA evidence
- shadow-mode benchmark reporting

The benchmark uses the built-in five-scenario suite and measures whether the system can move from noisy telemetry to a correct, actionable shadow recommendation before conditions degrade further.

## System Under Test

The benchmark covers the full shadow-mode pipeline:

1. telemetry ingest
2. anomaly detection
3. incident correlation
4. incident graph and RCA evidence
5. short-horizon forecasting
6. decision recommendation
7. evaluation against baseline policies

## Benchmark Setup

- Suite: `deterministic-scenarios`
- Seed: `7`
- Planner mode: `heuristic`
- Cases: `bad_deploy`, `memory_leak`, `queue_backlog`, `traffic_spike`, `transient_noise`
- Command:

```powershell
python .\scripts\run_benchmarks.py --suite scenarios
```

Artifacts are written to:

- `artifacts\benchmarks\benchmark_summary.json`
- `artifacts\benchmarks\benchmark_report.md`

To benchmark persisted live-ingest streams instead of simulator scenarios:

```powershell
python .\scripts\run_benchmarks.py `
  --suite streams `
  --db-path .\artifacts\ops_platform.sqlite3 `
  --environment production `
  --source prometheus
```

The same command accepts a Timescale/PostgreSQL DSN through `--db-path` when you want the benchmark to run against the production-like persistence backend.

## Aggregate Results

From the current deterministic benchmark run:

- Cases: `5`
- Top-1 RCA accuracy: `100.0%`
- Top-2 RCA accuracy: `100.0%`
- Action match rate: `100.0%`
- False action rate: `0.0%`
- Average first actionable minute: `9.6`
- Average decision latency: `0.088 ms`
- Average alert reduction: `93.86%`
- Average latency protection: `88.89%`
- Average baseline win rate: `86.67%`
- Average projected cost delta: `3.3%`
- Average projected p95 delta: `-15.8 ms`

## Scenario Highlights

### `bad_deploy`

- Root cause ranked `payments` at top-1
- Recommended action: `rollback_candidate`
- Dominant signals: `error_rate_pct`, `p95_latency_ms`
- Blast radius reached `gateway`, `payments`, `worker`, plus dependent services in the graph
- RCA evidence included the deploy change event and dependency path `gateway -> payments`

Why it matters:

- This is the clearest example of explainable RCA instead of raw anomaly reporting.
- The system chose rollback over reactive scale-out, which beats both `no_action` and threshold autoscaling in the benchmark.

### `queue_backlog`

- Root cause ranked `worker` at top-1
- Recommended action: `increase_consumers`
- Dominant signal: `queue_depth`
- Projected burn rate was critical and specifically queue-driven

Why it matters:

- This scenario shows the value of SLO-aware policy selection.
- A generic scale reaction would have been weaker than the queue-specific action.

### `traffic_spike`

- Root cause ranked `gateway` at top-1
- Recommended action: `scale_out`
- Dominant burn signal: `p95_latency_ms`
- Graph evidence linked downstream pressure through `payments` and `worker`

Why it matters:

- This scenario shows the intended flagship narrative best:
  weak signal -> correlated incident -> safer action -> measurable shadow evaluation

### `transient_noise`

- Recommended action: `hold_steady`
- No false action was taken
- This was the weakest benchmark case for baseline win rate, which is expected

Why it matters:

- The platform is not rewarded only for acting quickly.
- It is also rewarded for avoiding unnecessary action churn on noise.

## Interpretation

The benchmark suggests that the project is strongest when:

- the root cause has strong localized evidence
- there is a meaningful safety distinction between actions
- graph context and change events narrow the RCA search space

It is intentionally weaker in the transient case, where the main success criterion is restraint rather than aggressive optimization.

## Limits

This benchmark is still simulator-based, so it should be presented honestly as:

- deterministic replayable evaluation
- shadow-mode policy comparison
- production-like decision support, not production automation

It is not yet a claim of:

- real production traffic control
- auto-remediation in live infrastructure
- large-scale chaos benchmark on Kubernetes

## Next Upgrade

The next meaningful step after this case study is a richer external testbed, for example:

- Online Boutique or another multi-service workload
- load generation plus 3-5 reproducible faults
- the same benchmark report structure reused on that environment
