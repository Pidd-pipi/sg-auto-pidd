"""Disk gate and cleanup of finished task directories."""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from unittest import mock

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

from api.housekeeping import CLEANED_MARKER, Housekeeper, housekeeping_settings  # noqa: E402
from tests.support import SchedulerTestCase, write_task  # noqa: E402
from tests.test_scheduler import build_queue, platform_item, tick_with_ready_runner  # noqa: E402

HOUR = 3600


class _Log:
    def __init__(self):
        self.events = []

    def emit(self, event, **fields):
        self.events.append((event, fields))


class HousekeeperTests(SchedulerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.processes: list[dict] = []
        self.containers: list[str] = []
        self.log = _Log()

    def _keeper(self, queue):
        return Housekeeper(queue, log=self.log, processes=lambda: list(self.processes),
                           remove_containers=lambda prefix: self.containers.append(prefix) or [])

    def _task(self, name, *, status="complete", quiet=10 * HOUR, extra_state=None):
        task_root = write_task(self.root, name, status=status)
        modules = task_root / "source" / "candidates" / "candidate-1" / "frontend" / "node_modules" / "pkg"
        modules.mkdir(parents=True)
        (modules / "index.js").write_text("x" * 4096, encoding="utf-8")
        verify = task_root / "monitor" / "verify" / "a"
        verify.mkdir(parents=True)
        (verify / "build.log").write_text("y" * 1024, encoding="utf-8")
        keep = task_root / "workspace" / "轨迹文件" / "a.jsonl"
        keep.parent.mkdir(parents=True)
        keep.write_text("{}", encoding="utf-8")
        (task_root / "source" / "candidates" / "candidate-1" / "frontend" / "App.jsx").write_text("app", encoding="utf-8")
        state_path = task_root / "monitor" / "state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state.update(extra_state or {})
        state_path.write_text(json.dumps(state), encoding="utf-8")
        stamp = time.time() - quiet
        os.utime(state_path, (stamp, stamp))
        return task_root

    def test_complete_task_loses_only_regenerable_dirs(self):
        task_root = self._task("gb-1-done")
        queue = build_queue(self.config, items=[])
        result = self._keeper(queue).run_once(force=True)

        self.assertEqual(result["tasks"], 1)
        self.assertGreaterEqual(result["freedBytes"], 4096 + 1024)
        self.assertFalse((task_root / "monitor" / "verify").exists())
        self.assertFalse((task_root / "source" / "candidates" / "candidate-1" / "frontend" / "node_modules").exists())
        self.assertTrue((task_root / "source" / "candidates" / "candidate-1" / "frontend" / "App.jsx").is_file())
        self.assertTrue((task_root / "workspace" / "轨迹文件" / "a.jsonl").is_file())
        self.assertTrue((task_root / "monitor" / "state.json").is_file())
        marker = json.loads((task_root / "monitor" / CLEANED_MARKER).read_text(encoding="utf-8"))
        self.assertIn("monitor/verify", marker["removed"])
        self.assertEqual(self.containers, [f"sologsb-{task_root.name}-"])
        self.assertEqual(queue.housekeeping_status["lastTasks"], 1)
        self.assertEqual(self.log.events[-1][0], "housekeeping.cleaned")

    def test_cleaned_task_is_not_walked_again(self):
        self._task("gb-1-done")
        queue = build_queue(self.config, items=[])
        keeper = self._keeper(queue)
        keeper.run_once(force=True)
        self.assertEqual(keeper.run_once(force=True)["tasks"], 0)

    def test_dry_run_changes_nothing(self):
        task_root = self._task("gb-1-done")
        queue = build_queue(self.config, items=[])
        result = self._keeper(queue).run_once(force=True, dry_run=True)
        self.assertEqual(result["tasks"], 1)
        self.assertTrue((task_root / "monitor" / "verify").is_dir())
        self.assertFalse((task_root / "monitor" / CLEANED_MARKER).exists())
        self.assertEqual(self.containers, [])

    def test_unfinished_and_recent_tasks_are_kept(self):
        for name, status, quiet in (("gb-1-gsb", "gsb_ready", 10 * HOUR), ("gb-2-blocked", "blocked", 10 * HOUR),
                                    ("gb-3-fresh", "complete", 2 * HOUR)):
            self._task(name, status=status, quiet=quiet)
        queue = build_queue(self.config, items=[])
        self.assertEqual(self._keeper(queue).run_once(force=True)["tasks"], 0)

    def test_task_with_a_live_process_is_kept(self):
        task_root = self._task("gb-1-done")
        self.processes = [{"pid": 7, "pgid": 7, "command": f"node {task_root}/monitor/verify/a/vite"}]
        queue = build_queue(self.config, items=[])
        self.assertEqual(self._keeper(queue).run_once(force=True)["tasks"], 0)
        self.assertTrue((task_root / "monitor" / "verify").is_dir())

    def test_task_held_by_an_active_item_is_kept(self):
        task_root = self._task("gb-1-done")
        queue = build_queue(self.config, items=[platform_item(status="triggered", taskRoot=str(task_root))])
        self.assertEqual(self._keeper(queue).run_once(force=True)["tasks"], 0)

    def test_directory_the_state_refers_to_is_kept(self):
        task_root = self._task("gb-1-done")
        evidence = task_root / "monitor" / "verify" / "a" / "evidence.json"
        evidence.write_text("{}", encoding="utf-8")
        state_path = task_root / "monitor" / "state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["evidencePath"] = str(evidence)
        stamp = state_path.stat().st_mtime
        state_path.write_text(json.dumps(state), encoding="utf-8")
        os.utime(state_path, (stamp, stamp))
        queue = build_queue(self.config, items=[])
        self._keeper(queue).run_once(force=True)
        self.assertTrue(evidence.is_file())
        self.assertFalse((task_root / "source" / "candidates" / "candidate-1" / "frontend" / "node_modules").exists())

    def test_batch_is_bounded_and_interval_respected(self):
        for index in range(7):
            self._task(f"gb-{index}-done")
        queue = build_queue(self.config, items=[])
        keeper = self._keeper(queue)
        with mock.patch.object(queue, "disk_free_gb", return_value=500.0):
            self.assertEqual(keeper.run_once()["tasks"], 5)
            self.assertEqual(keeper.run_once()["status"], "skipped")
            self.assertEqual(keeper.run_once(force=True)["tasks"], 2)

    def test_low_disk_ignores_the_interval(self):
        for index in range(7):
            self._task(f"gb-{index}-done")
        queue = build_queue(self.config, items=[])
        keeper = self._keeper(queue)
        with mock.patch.object(queue, "disk_free_gb", return_value=40.0):
            keeper.run_once()
            self.assertEqual(keeper.run_once()["tasks"], 2)

    def test_background_round_does_not_block_and_does_not_overlap(self):
        self._task("gb-1-done")
        queue = build_queue(self.config, items=[])
        keeper = self._keeper(queue)
        release = __import__("threading").Event()
        original = keeper.run_once
        keeper.run_once = lambda **kw: (release.wait(5), original(force=True, **kw))[1]
        self.assertTrue(keeper.start_background())
        self.assertFalse(keeper.start_background())
        release.set()
        keeper._thread.join(5)
        self.assertEqual(queue.housekeeping_status["lastTasks"], 1)

    def test_disabled_does_nothing(self):
        self._task("gb-1-done")
        self.config["automation"]["housekeeping"] = {"enabled": False}
        queue = build_queue(self.config, items=[])
        self.assertEqual(self._keeper(queue).run_once(force=True)["status"], "skipped")

    def test_settings_are_clamped(self):
        settings = housekeeping_settings({"automation": {"housekeeping": {"minFreeGB": 1, "afterHours": 0}}})
        self.assertEqual((settings["minFreeGB"], settings["afterHours"]), (5, 1))


class DiskGateTests(SchedulerTestCase):
    def test_low_disk_holds_launches_and_recovers(self):
        write_task(self.root, "existing", status="complete")
        queue = build_queue(self.config, items=[platform_item()])
        events = []
        queue._emit = lambda level, event, **fields: events.append(event)
        with mock.patch.object(queue, "disk_free_gb", return_value=12.0):
            tick_with_ready_runner(queue, self.root)
            tick_with_ready_runner(queue, self.root)
        item = queue._items[0]
        self.assertEqual(item["status"], "pending")
        self.assertIn("磁盘剩余 12 GB", item["containerWait"])
        self.assertEqual(events.count("queue.disk_low"), 1)
        self.assertTrue(queue.disk_status()["low"])

        with mock.patch.object(queue, "disk_free_gb", return_value=80.0):
            tick_with_ready_runner(queue, self.root)
        self.assertIn("queue.disk_recovered", events)
        self.assertNotEqual(queue._items[0]["status"], "pending")

    def test_snapshot_reports_disk(self):
        queue = build_queue(self.config, items=[])
        disk = queue.fast_snapshot()["disk"]
        self.assertGreater(disk["freeGB"], 0)
        self.assertEqual(disk["minFreeGB"], 30)
