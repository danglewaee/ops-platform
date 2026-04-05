# Real Deploy Runbook

## What You Still Have To Do Yourself

This repository is now deploy-ready, but the final "real deploy" step still requires either:

- you to run the commands on a VM or host you control, or
- you to give an agent shell access to that target machine

The repo cannot provision your VM, open cloud firewalls, or register DNS on its own.

## What The Repo Already Covers

- deployable Compose bundle in `docker-compose.deploy.yml`
- deploy config validation in `scripts/check_deploy_bundle.py`
- optional recurring Prometheus worker packaging
- post-deploy HTTP and worker-summary evidence capture in `scripts/capture_deploy_evidence.py`

## Before You Start

Target host requirements:

- Docker Engine with `docker compose`
- the repo checked out on the target host
- ports `80` and `443` open if you want public HTTP/HTTPS
- a real value for `OPS_PLATFORM_PUBLIC_BASE_URL`
- non-default secrets for:
  - `OPS_PLATFORM_API_KEYS`
  - `OPS_PLATFORM_POSTGRES_PASSWORD`

Optional but recommended:

- a real domain name if you want Caddy-managed HTTPS
- a Prometheus endpoint configured in `deploy/recurring_pull.toml` before enabling the worker profile

## Step 1: Prepare Deploy Config

Linux or macOS:

```bash
cp .env.deploy.example .env.deploy
cp deploy/recurring_pull.example.toml deploy/recurring_pull.toml
```

Windows PowerShell:

```powershell
Copy-Item .\.env.deploy.example .\.env.deploy
Copy-Item .\deploy\recurring_pull.example.toml .\deploy\recurring_pull.toml
```

Then edit `.env.deploy` and replace at least:

- `OPS_PLATFORM_PUBLIC_BASE_URL`
- `OPS_PLATFORM_DEPLOY_SITE_ADDRESS`
- `OPS_PLATFORM_API_KEYS`
- `OPS_PLATFORM_POSTGRES_PASSWORD`

If you plan to run the recurring worker, also edit `deploy/recurring_pull.toml` with the real Prometheus base URL and query config.

## Step 2: Validate Before Launch

```bash
python scripts/check_deploy_bundle.py --env-file .env.deploy --full
docker compose --env-file .env.deploy -f docker-compose.deploy.yml config
docker compose --env-file .env.deploy -f docker-compose.deploy.yml --profile worker config
```

Do not continue until those commands succeed.

## Step 3: Start The Stack

Base API stack:

```bash
docker compose --env-file .env.deploy -f docker-compose.deploy.yml up --build -d
```

If you also want recurring Prometheus ingest:

```bash
docker compose --env-file .env.deploy -f docker-compose.deploy.yml --profile worker up --build -d
```

Quick service check:

```bash
docker compose --env-file .env.deploy -f docker-compose.deploy.yml ps
```

## Step 4: Capture Proof After Deploy

Run the post-deploy evidence capture script from the repo root on that same host:

```bash
python scripts/capture_deploy_evidence.py --env-file .env.deploy --output-dir artifacts/deploy-evidence/latest --full
```

That script captures:

- `/health`
- `/ready`
- `/streams?limit=5`
- `/storage/stats`
- `/audit/events?limit=5`
- the local recurring worker summary if it exists

It writes:

- `artifacts/deploy-evidence/latest/deploy_evidence_summary.json`
- `artifacts/deploy-evidence/latest/deploy_evidence_summary.md`
- per-endpoint JSON files like `health.json`, `ready.json`, and `storage_stats.json`

The script exits with code `0` only when all checks succeed.

## Step 5: Save Portfolio Evidence

After the deploy is healthy, keep these artifacts:

- `docker compose ... ps` output
- `artifacts/deploy-evidence/latest/deploy_evidence_summary.md`
- one screenshot of `/ready`
- one screenshot of the live dashboard or API output
- one recurring worker summary file if the worker profile is enabled

This is enough to say the project was packaged and deployed on a real host, not just validated in CI.

## Step 6: Common Fixes

- If `capture_deploy_evidence.py` fails on auth, check `OPS_PLATFORM_API_KEYS`, `OPS_PLATFORM_AUTH_HEADER_NAME`, and `OPS_PLATFORM_ACTOR_HEADER_NAME`.
- If `worker_summary_available` is false, make sure the worker profile is running and `OPS_PLATFORM_RECURRING_SUMMARY_PATH` points into the mounted `./artifacts` directory.
- If Caddy does not come up, verify `OPS_PLATFORM_DEPLOY_SITE_ADDRESS` and host firewall rules.
- If Timescale does not initialize, inspect `docker compose ... logs timescale-init timescaledb api`.
