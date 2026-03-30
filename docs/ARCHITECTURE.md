# Architecture

## Thesis

This system is a shadow-mode decision layer for distributed systems. It does not auto-remediate production failures. Instead, it tries to answer one operator-facing question:

> Given noisy telemetry and limited time, what is the safest action to consider next?

## Pipeline

```text
Synthetic scenario
      ↓
Normalized telemetry + change events
      ↓
Anomaly detection
      ↓
Incident correlation + root-cause ranking
      ↓
SLI/SLO feature builder + budget pressure
      ↓
Short-horizon forecasting
      ↓
Decision recommendation (shadow mode)
      ↓
Evaluation against expected scenario outcome
```

## Core modules

### `scenarios.py`

Defines:

- available scenarios
- service dependency graph
- scenario ground truth

### `simulator.py`

Produces:

- metric samples
- change events
- scenario metadata

### `detection.py`

Produces anomaly events with:

- service
- metric
- severity
- confidence
- explanation

### `incident_engine.py`

Compresses anomalies into incidents and ranks likely root causes using:

- time proximity
- dependency graph
- recent change events
- anomaly severity

### `forecasting.py`

Looks 5 to 15 minutes ahead using recent trend windows and projects:

- request rate
- p95 latency
- queue depth
- estimated SLO burn and budget pressure

### `feature_builder.py`

Builds service health snapshots for decisioning:

- latency, error-rate, and queue-depth SLO targets
- current and projected burn rate
- estimated budget pressure
- dominant signal for actionable triage

### `decision_engine.py`

Recommends one of:

- `scale_out`
- `scale_in`
- `reroute_traffic`
- `rollback_candidate`
- `increase_consumers`
- `hold_steady`

It now also uses SLO-aware pressure signals so the shadow planner can better separate:

- transient noise worth ignoring
- queue pressure that should prefer consumer scaling
- latency burn that should prefer bounded scale-out

### `pipeline.py`

Runs the full scenario and returns:

- anomalies
- incidents
- forecasts
- recommendations
- evaluation metrics

## Why shadow mode

For a first version, shadow mode is stronger than automatic action because it:

- keeps scope realistic
- supports measurable evaluation
- avoids unsafe automation claims
- still demonstrates systems depth
