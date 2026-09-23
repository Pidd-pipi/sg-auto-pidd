"""Shared fixtures for the scheduler test suite."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

from api.common import DEFAULT_CONFIG, deep_merge  # noqa: E402


def make_config(root: Path, **overrides) -> dict:
    """A config pointing at a throwaway state dir and one task root."""
    state_dir = root / ".state"
    state_dir.mkdir(parents=True, exist_ok=True)
    config = deep_merge(DEFAULT_CONFIG, {
        "roots": [str(root / "tasks")],
        "skillScript": str(root / "fake-sologsb.py"),
        "automation": {
            "capacity": 2,
            "paused": False,
            "autoRefill": {"enabled": False},
            "scheduleMode": "containers",
            "maxContainers": 4,
            "candidatesPerTask": 2,
        },
    })
    config["_configPath"] = str(root / "config.json")
    config["_stateDir"] = str(state_dir)
    config.update(overrides)
    return config


def state_dir_of(config: dict) -> Path:
    """The throwaway state directory a test config points at."""
    return Path(config["_stateDir"])


def write_task(root: Path, name: str, *, status: str = "running", sides: dict | None = None) -> Path:
    """Create a minimal but valid sologsb task directory."""
    task_root = root / "tasks" / name
    (task_root / "monitor").mkdir(parents=True, exist_ok=True)
    state = {
        "taskName": name,
        "status": status,
        "createdAt": "2026-09-20T00:00:00Z",
        "updatedAt": "2026-09-20T01:00:00Z",
        "sides": sides if sides is not None else {
            "A": {"status": "running", "attempt": 1, "runPid": os.getpid()},
            "B": {"status": "idle"},
        },
        "candidateIds": [],
        "candidates": {},
    }
    (task_root / "monitor" / "state.json").write_text(json.dumps(state), encoding="utf-8")
    prompt = task_root / "monitor" / "prompt.txt"
    prompt.write_text("题目提示词", encoding="utf-8")
    state["promptPath"] = str(prompt)
    state["promptSha256"] = "0" * 64
    (task_root / "monitor" / "state.json").write_text(json.dumps(state), encoding="utf-8")
    return task_root


class SchedulerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        (self.root / "tasks").mkdir(parents=True, exist_ok=True)
        self.config = make_config(self.root)
