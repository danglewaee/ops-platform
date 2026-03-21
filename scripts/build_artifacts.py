from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.dashboard import write_artifacts

OUTPUT_DIR = ROOT / "artifacts"


def main() -> int:
    summary_path, dashboard_path = write_artifacts(OUTPUT_DIR)
    live_summary_path = OUTPUT_DIR / "live_summary.json"
    print(f"Wrote {summary_path}")
    print(f"Wrote {live_summary_path}")
    print(f"Wrote {dashboard_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
