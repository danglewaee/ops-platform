# V2 Testbed Case Study

## Overview

This case study documents the V2 `boutique_like` benchmark pack for the Ops Decision Platform.

The goal of this pack is not to claim full production traffic control. Its purpose is to move the project beyond the compact flagship simulator into a more realistic storefront-style microservice topology with checkout, payment, cart, recommendation, email, and cache paths.

## Why this matters

The original benchmark pack proved the end-to-end thesis:

- signal
- incident
- recommendation
- evaluation

The V2 testbed pack raises the bar by testing the same decision layer against failure patterns that look closer to an ecommerce microservice environment:

- frontend traffic surges
- bad canary releases
- dependency timeouts
- async worker backlog
- cache jitter that should not trigger unnecessary action

## Testbed Profile

Profile: `boutique_like`

Services:

- `frontend`
- `checkout`
- `payment`
- `cart`
- `recommendation`
- `email`
- `shipping`
- `currency`
- `redis`

Representative dependency paths:

- `frontend -> checkout -> payment`
- `frontend -> cart -> redis`
- `frontend -> recommendation -> productcatalog`
- `checkout -> email`

## Benchmark Setup

- Suite: `boutique_like-scenarios`
- Seed: `7`
- Planner mode: `heuristic`
- Cases:
  - `boutique_frontend_spike`
  - `boutique_bad_canary`
  - `boutique_payment_timeout`
  - `boutique_email_backlog`
  - `boutique_cache_jitter`

Command:

```powershell
python .\scripts\run_benchmarks.py --suite scenarios --testbed-profile boutique_like
```

Artifacts are written to:

- `artifacts\benchmarks\boutique_like_benchmark_summary.json`
- `artifacts\benchmarks\boutique_like_benchmark_report.md`

## Aggregate Results

From the current deterministic V2 benchmark run:

- Cases: `5`
- Top-1 RCA accuracy: `100.0%`
- Top-2 RCA accuracy: `100.0%`
- Action match rate: `100.0%`
- False action rate: `0.0%`
- Average first actionable minute: `10.2`
- Average decision latency: `0.12 ms`
- Average alert reduction: `78.08%`
- Average latency protection: `88.89%`
- Average baseline win rate: `86.67%`
- Average projected cost delta: `3.3%`
- Average projected p95 delta: `-14.6 ms`

## Scenario Highlights

### `boutique_bad_canary`

- Root cause ranked `checkout` at top-1
- Recommended action: `rollback_candidate`
- RCA evidence included the canary release event plus dependency paths through `frontend` and `payment`

Why it matters:

- This is the cleanest V2 example of change-aware diagnosis.
- It proves the system can distinguish rollout failure from generic capacity stress.

### `boutique_payment_timeout`

- Root cause ranked `payment` at top-1
- Recommended action: `reroute_traffic`
- Incident evidence showed a failing dependency path contaminating checkout and frontend latency

Why it matters:

- This is closer to a real dependency failure than the original compact scenarios.
- The safer move is rerouting, not just scaling the hot service blindly.

### `boutique_email_backlog`

- Root cause ranked `email` at top-1
- Recommended action: `increase_consumers`
- Queue depth was the dominant SLO burn signal

Why it matters:

- This validates that the planner can separate async backlog pressure from generic latency spikes.
- The recommendation remains action-specific rather than defaulting to scale-out.

### `boutique_cache_jitter`

- Root cause ranked `cart` at top-1
- Recommended action: `hold_steady`
- Evidence included the short-lived jitter event and a compact blast radius through the cache path

Why it matters:

- This is the most important restraint case in the V2 pack.
- The platform is rewarded for avoiding unnecessary action churn, not just for acting quickly.

## Interpretation

The V2 benchmark suggests the project is now stronger on:

- rollout-aware incident handling
- dependency failure reasoning
- queue-specific policy choice
- transient fault restraint in a more realistic service graph

It is still deterministic and simulator-backed, but it is closer to the shape of an external microservice workload than the original compact pack.

## Limits

This V2 pack should still be presented honestly as:

- deterministic testbed evaluation
- production-like topology and failure patterns
- shadow-mode decision support

It is not yet a claim of:

- live Kubernetes chaos testing
- production traffic routing
- autonomous remediation in a real cluster

## Next Upgrade

The next meaningful step after this V2 pack is an external deployment-backed testbed, for example:

- Docker Compose or Kubernetes deployment of a small storefront workload
- reproducible load generation
- fault injection against live services
- the same benchmark and case-study structure reused on observed telemetry
