from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.deploy_evidence import capture_deploy_evidence, load_deploy_evidence_settings, load_evidence_environment


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture post-deploy evidence from a running Ops Decision Platform instance."
    )
    parser.add_argument(
        "--env-file",
        default=str(ROOT / ".env.deploy"),
        help="Optional deploy env file used to resolve base URL, auth headers, and worker summary location.",
    )
    parser.add_argument("--base-url", help="Override the deployed public base URL.")
    parser.add_argument("--api-key", help="Override the API key used for authenticated evidence capture.")
    parser.add_argument("--actor", default="deploy-evidence", help="Actor header value used for authenticated requests.")
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "artifacts" / "deploy-evidence" / "latest"),
        help="Directory where evidence JSON and markdown files will be written.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=5, help="HTTP timeout for each evidence request.")
    parser.add_argument("--full", action="store_true", help="Print the full capture summary.")
    args = parser.parse_args()

    environ = load_evidence_environment(args.env_file)
    settings = load_deploy_evidence_settings(
        environ,
        base_url=args.base_url,
        api_key=args.api_key,
        actor=args.actor,
        timeout_seconds=args.timeout_seconds,
    )
    summary = capture_deploy_evidence(
        settings,
        output_dir=args.output_dir,
        workspace_root=ROOT,
    )

    if args.full:
        print(json.dumps(summary, indent=2))
    else:
        print(
            json.dumps(
                {
                    "overall_status": summary["overall_status"],
                    "base_url": summary["base_url"],
                    "ready": summary["ready"],
                    "backend": summary["backend"],
                    "health_status": summary["health_status"],
                    "stream_count": summary["stream_count"],
                    "audit_event_count": summary["audit_event_count"],
                    "worker_summary_available": summary["worker_summary_available"],
                    "summary_path": summary["summary_path"],
                },
                indent=2,
            )
        )
    return 0 if summary["overall_status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
