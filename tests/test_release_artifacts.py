from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from ops_platform.release_artifacts import build_release_artifacts


class ReleaseArtifactTests(unittest.TestCase):
    def test_build_release_artifacts_writes_manifest_and_outputs(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            output_dir = temp_path / "release"
            db_path = temp_path / "ops_platform.sqlite3"

            manifest = build_release_artifacts(output_dir, db_path=db_path)

            artifact_paths = manifest["artifacts"]
            for key in (
                "summary_json",
                "live_summary_json",
                "dashboard_html",
                "benchmark_json",
                "benchmark_markdown",
                "release_overview_markdown",
                "release_manifest_json",
            ):
                self.assertTrue(Path(artifact_paths[key]).exists(), key)

            self.assertEqual(manifest["benchmark"]["summary"]["case_count"], 5)
            self.assertEqual(manifest["benchmark"]["summary"]["action_match_rate_pct"], 100.0)

            saved_manifest = json.loads(Path(artifact_paths["release_manifest_json"]).read_text(encoding="utf-8"))
            overview = Path(artifact_paths["release_overview_markdown"]).read_text(encoding="utf-8")

        self.assertEqual(saved_manifest["benchmark"]["summary"]["top2_root_cause_accuracy_pct"], 100.0)
        self.assertIn("# Release Overview", overview)
        self.assertIn("Resume and website copy", overview)


if __name__ == "__main__":
    unittest.main()
