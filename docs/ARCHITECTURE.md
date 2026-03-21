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

### `decision_engine.py`

Recommends one of:

- `scale_out`
- `scale_in`
- `reroute_traffic`
- `rollback_candidate`
- `increase_consumers`
- `hold_steady`

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

