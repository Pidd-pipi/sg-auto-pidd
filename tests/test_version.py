"""Tests for the semantic version source and release metadata."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

from api.version import APP_VERSION, VERSION_FILE, bump_version, parse_version, read_version, version_info  # noqa: E402


class VersionTests(unittest.TestCase):
    def test_version_file_is_valid_semver(self):
        self.assertEqual(read_version(), APP_VERSION)
        self.assertEqual(APP_VERSION, VERSION_FILE.read_text(encoding="utf-8").strip())
        parse_version(APP_VERSION)

    def test_semver_prerelease_and_build_metadata_are_supported(self):
        major, minor, patch, prerelease, build = parse_version("1.2.3-beta.1+build.5")
        self.assertEqual((major, minor, patch), (1, 2, 3))
        self.assertEqual(prerelease, "beta.1")
        self.assertEqual(build, "build.5")

    def test_bump_version_resets_lower_components(self):
        self.assertEqual(bump_version("1.2.3", "major"), "2.0.0")
        self.assertEqual(bump_version("1.2.3", "minor"), "1.3.0")
        self.assertEqual(bump_version("1.2.3", "patch"), "1.2.4")
        self.assertEqual(bump_version("1.2.3-beta.1", "patch"), "1.2.4")
        self.assertEqual(bump_version("1.2.3", "2.0.0-rc.1"), "2.0.0-rc.1")

    def test_invalid_version_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_version("v1.2.3")
        with self.assertRaises(ValueError):
            bump_version("1.2.3", "next")

    def test_version_info_contains_release_metadata(self):
        info = version_info()
        self.assertEqual(info["version"], APP_VERSION)
        self.assertEqual(info["displayVersion"], f"v{APP_VERSION}")
        self.assertIn("gitCommit", info)
        self.assertIn("gitBranch", info)

    def test_static_assets_use_the_current_cache_version(self):
        for html in (APP / "static").glob("*.html"):
            text = html.read_text(encoding="utf-8")
            self.assertIn(f"?v={APP_VERSION}", text, str(html))


if __name__ == "__main__":
    unittest.main()
