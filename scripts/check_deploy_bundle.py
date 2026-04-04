from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.deploy_bundle import build_deploy_bundle_summary, load_deploy_bundle_settings, load_env_file


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and summarize the deploy bundle configuration.")
    parser.add_argument("--env-file", default=str(ROOT / ".env.deploy"), help="Path to a deploy env file.")
    parser.add_argument("--full", action="store_true", help="Print the full deploy summary.")
    args = parser.parse_args()

    env_path = Path(args.env_file)
    environ = dict(os.environ)
    if env_path.exists():
        environ.update(load_env_file(env_path))

    settings = load_deploy_bundle_settings(environ)
    summary = build_deploy_bundle_summary(settings, workspace_root=ROOT)
    if args.full:
        print(json.dumps(summary, indent=2))
    else:
        print(
            json.dumps(
                {
                    "public_base_url": summary["public_base_url"],
                    "auth_enabled": summary["api"]["auth_enabled"],
                    "rate_limit_backend": summary["api"]["rate_limit_backend"],
                    "worker_enabled": summary["worker"]["enabled"],
                    "worker_config_present_in_workspace": summary["worker"]["config_present_in_workspace"],
                    "recommended_compose_file": summary["recommended_compose_file"],
                },
                indent=2,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
