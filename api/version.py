"""Semantic application version and Git build metadata."""
from __future__ import annotations

import re
import subprocess
from functools import lru_cache
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[1]
VERSION_FILE = APP_DIR / "VERSION"
_SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)


def parse_version(value: str) -> tuple[int, int, int, str, str]:
    """Parse a strict SemVer string."""
    match = _SEMVER_RE.fullmatch(str(value).strip())
    if match is None:
        raise ValueError(f"版本号不是合法 SemVer：{value!r}")
    major, minor, patch, prerelease, build = match.groups()
    return int(major), int(minor), int(patch), prerelease or "", build or ""


def bump_version(current: str, part: str) -> str:
    """Return the next SemVer version for major/minor/patch or an explicit version."""
    normalized = str(part).strip()
    if _SEMVER_RE.fullmatch(normalized):
        return normalized
    major, minor, patch, _, _ = parse_version(current)
    if normalized == "major":
        return f"{major + 1}.0.0"
    if normalized == "minor":
        return f"{major}.{minor + 1}.0"
    if normalized == "patch":
        return f"{major}.{minor}.{patch + 1}"
    raise ValueError("版本参数必须是 major、minor、patch 或完整 SemVer")


def read_version(path: Path = VERSION_FILE) -> str:
    """Read and validate the canonical VERSION file."""
    value = Path(path).read_text(encoding="utf-8").strip()
    parse_version(value)
    return value


def _git_value(*args: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(APP_DIR), *args],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


@lru_cache(maxsize=1)
def version_info() -> dict[str, str]:
    """Version metadata exposed by the HTTP API and the UI."""
    version = read_version()
    commit = _git_value("rev-parse", "--short=7", "HEAD")
    branch = _git_value("rev-parse", "--abbrev-ref", "HEAD")
    return {
        "service": "sologsb-monitor",
        "version": version,
        "displayVersion": f"v{version}",
        "gitCommit": commit,
        "gitBranch": branch,
    }


APP_VERSION = read_version()

__all__ = ["APP_VERSION", "VERSION_FILE", "bump_version", "parse_version", "read_version", "version_info"]
