# Deployment Guide

## Goal

This repository now has two Docker Compose entry points:

- `docker-compose.smoke.yml` for CI and quick local checks
- `docker-compose.deploy.yml` for a production-like deployment bundle

The deploy bundle is meant for a single host or VM where you want:

- reverse proxy in front of the API
- TimescaleDB as the persistence backend
- Redis-backed shared rate limiting
- optional recurring Prometheus ingest worker
- persistent volumes for database and runtime artifacts

## What the deploy bundle includes

- `caddy`
- `api`
- `timescaledb`
- `redis`
- `otel-collector`
- one-shot `timescale-init`
- optional `worker` profile for recurring Prometheus pulls

## Files to use

- `.env.deploy.example`
- `docker-compose.deploy.yml`
- `deploy/Caddyfile`
- `deploy/recurring_pull.example.toml`
- `scripts/check_deploy_bundle.py`
- `scripts/capture_deploy_evidence.py`
- `scripts/run_recurring_worker.py`
- `docs/DEPLOY_RUNBOOK.md`
- `.github/workflows/deploy-config-validate.yml`

## Quick start

1. Copy the deploy env file:

```powershell
Copy-Item .\.env.deploy.example .\.env.deploy
```

2. Copy the recurring config if you want the worker profile:

```powershell
Copy-Item .\deploy\recurring_pull.example.toml .\deploy\recurring_pull.toml
```

3. Validate the bundle before you deploy:

```powershell
python .\scripts\check_deploy_bundle.py --env-file .\.env.deploy --full
```

4. Bring up the deploy stack:

```powershell
docker compose --env-file .\.env.deploy -f .\docker-compose.deploy.yml up --build -d
```

5. If you also want the recurring ingest worker:

```powershell
docker compose --env-file .\.env.deploy -f .\docker-compose.deploy.yml --profile worker up --build -d
```

The repository also validates this deploy bundle in CI by checking:

- deploy env parsing
- deploy compose rendering
- deploy compose rendering with the `worker` profile enabled

## Real-host launch and evidence capture

`docs/DEPLOYMENT.md` is the high-level bundle guide. For the concrete VM or host checklist, use [docs/DEPLOY_RUNBOOK.md](D:/CODE/Personal%20Website/ops-decision-platform/docs/DEPLOY_RUNBOOK.md).

After the stack is up on a real host, capture proof with:

```powershell
python .\scripts\capture_deploy_evidence.py --env-file .\.env.deploy --output-dir .\artifacts\deploy-evidence\latest --full
```

That script writes:

- endpoint payloads for `/health`, `/ready`, `/streams`, `/storage/stats`, and `/audit/events`
- `deploy_evidence_summary.json`
- `deploy_evidence_summary.md`
- the recurring worker summary if it exists locally under the mounted `./artifacts` directory

## Public routing

`caddy` fronts the API and proxies requests to `api:8000`.

Use `OPS_PLATFORM_DEPLOY_SITE_ADDRESS` to control how Caddy binds:

- `:80` for plain HTTP on a host or VM
- `ops.example.com` for a domain-backed deploy where Caddy can terminate HTTPS automatically

Set `OPS_PLATFORM_PUBLIC_BASE_URL` to the public URL you expect operators to use. This is what the deploy check script reports back.

## Recurring worker

The API alone is enough for manual ingest and replay. The `worker` profile is what turns the deploy into a continuously evaluating shadow-mode service.

The worker reads:

- `OPS_PLATFORM_RECURRING_ENABLED`
- `OPS_PLATFORM_RECURRING_CONFIG`
- `OPS_PLATFORM_RECURRING_INTERVAL_SECONDS`
- `OPS_PLATFORM_RECURRING_FAIL_DELAY_SECONDS`
- `OPS_PLATFORM_RECURRING_SUMMARY_PATH`

The recurring TOML controls:

- Prometheus base URL
- metric queries
- planner constraints
- retention policy
- tracing for worker runs

## Persistence and volumes

The deploy bundle keeps state in named volumes for:

- TimescaleDB data
- Redis append-only state
- Caddy data and config

It also mounts:

- `artifacts/`
- `runs/`
- `deploy/`

## What this deployment is and is not

This is a deployable shadow-mode service bundle. It is production-like, but it is not a claim of:

- multi-node orchestration
- HA database clustering
- managed secret rotation
- zero-downtime rolling upgrades
- autonomous production remediation

It is the right packaging layer for a strong flagship project and a realistic single-host deployment story.
