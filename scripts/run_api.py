from __future__ import annotations

import sys
from pathlib import Path

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops_platform.settings import load_app_settings


def main() -> int:
    settings = load_app_settings()
    uvicorn.run(
        "ops_platform.api:create_app",
        factory=True,
        host=settings.api_host,
        port=settings.api_port,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
