"""Fallback policies: candidate timeout, no progress, leaked processes."""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

from api.common import MonitorError, iso_from_timestamp  # noqa: E402
from api.guard import TaskGuard, guard_settings, latest_activity  # noqa: E402
from api.service import SchedulerService  # noqa: E402
from tests.support import SchedulerTestCase, write_task  # noqa: E402
from tests.test_scheduler import build_queue, platform_item  # noqa: E402

HOUR = 3600


class _Log:
    def __init__(self):
        self.events = []

    def emit(self, event, **fields):
        self.events.append((event, fields))

    def names(self):
        return [event for event, _ in self.events]


class GuardTestCase(SchedulerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.now = time.time()
        self.processes: list[dict] = []
        self.killed: list[int] = []
        self.removed: list[str] = []
        self.log = _Log()
        self.stop_path = self.root / "skill-stop-tasks.json"

    def _guard(self, queue, mode="observe", **settings):
        self.config["automation"]["guard"] = {"mode": mode, **settings}
        return TaskGuard(
            queue, log=self.log, stop_path=self.stop_path,
            processes=lambda: list(self.processes),
            kill=lambda process: self.killed.append(process["pid"]) or True,
            remove=lambda prefix: self.removed.append(prefix) or [f"{prefix}candidate-1"],
            clock=lambda: self.now,
        )

    def _task(self, name, *, status="candidates_running", race_age=None, race_done=False, quiet=0.0):
        task_root = write_task(self.root, name, status=status, sides={
            "A": {"status": "running", "runPid": 999999}, "B": {"status": "idle"},
        })
        state_path = task_root / "monitor" / "state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if race_age is not None:
            state["candidateRaceStartedAt"] = iso_from_timestamp(self.now - race_age)
        if race_done:
            state["candidateRaceFinishedAt"] = iso_from_timestamp(self.now - 60)
        state["candidates"] = {"candidate-1": {"status": "running"}}
        state_path.write_text(json.dumps(state), encoding="utf-8")
        self._age(task_root, quiet)
        return task_root

    @staticmethod
    def _age(task_root, seconds):
        stamp = time.time() - seconds
        for current, _dirs, files in os.walk(task_root):
            for name in files:
                os.utime(Path(current) / name, (stamp, stamp))

    def _queue(self, task_root, **overrides):
        item = platform_item(**{"status": "triggered", "taskRoot": str(task_root), "capacityHeld": True,
                                "quota": {"state": "claimed"}, **overrides})
        return build_queue(self.config, items=[item])

    def _state(self, task_root):
        return json.loads((task_root / "monitor" / "state.json").read_text(encoding="utf-8"))


class CandidateTimeoutTests(GuardTestCase):
    def test_observe_flags_once_and_touches_nothing(self):
        task_root = self._task("gb-1-slow", race_age=7 * HOUR)
        queue = self._queue(task_root)
        guard = self._guard(queue)
        self.processes = [{"pid": 4242, "pgid": 4242,
                           "command": f"python3 sologsb.py run --task-root {task_root} --side both"}]

        guard.run_once()
        guard.run_once()

        item = queue._items[0]
        self.assertEqual(item["status"], "triggered")
        self.assertEqual(item["guardFlag"]["policy"], "candidate-timeout")
        self.assertEqual(queue.guard_status["flags"][0]["itemId"], item["id"])
        self.assertEqual(self.log.names().count("guard.would_stop"), 1)
        self.assertEqual((self.killed, self.removed), ([], []))
        self.assertFalse(self.stop_path.exists())
        self.assertEqual(self._state(task_root)["status"], "candidates_running")

    def test_enforce_stops_the_task_and_keeps_its_directory(self):
        task_root = self._task("gb-1-slow", race_age=7 * HOUR)
        queue = self._queue(task_root)
        self.stop_path.write_text(json.dumps({"tasks": ["cy-306-old"]}), encoding="utf-8")
        guard = self._guard(queue, mode="enforce")
        self.processes = [
            {"pid": 4242, "pgid": 4242, "command": f"python3 sologsb.py run --task-root {task_root} --side both"},
            {"pid": 4243, "pgid": 4100, "command": f"node {task_root}/source/candidates/candidate-1/node_modules/.bin/vite"},
            {"pid": 4244, "pgid": 4244, "command": f"/opt/python3 queue_worker.py --task-name {task_root.name} --workdir {task_root.parent}"},
            {"pid": 4245, "pgid": 4245, "command": f"/usr/bin/login -qflp me /bin/zsh -fc cd '{task_root}/source'"},
            {"pid": 4246, "pgid": 4246, "command": f"python3 sologsb.py run --task-root {task_root}-other --side A"},
        ]

        actions = guard.run_once()

        self.assertEqual(actions[0]["kind"], "guard-stopped")
        self.assertEqual(sorted(self.killed), [4242, 4243])
        self.assertEqual(self.removed, [f"sologsb-{task_root.name}-"])
        stop = json.loads(self.stop_path.read_text(encoding="utf-8"))
        self.assertEqual(stop["tasks"], ["cy-306-old", task_root.name])
        self.assertEqual(stop["guard"][task_root.name]["policy"], "candidate-timeout")
        state = self._state(task_root)
        self.assertEqual((state["status"], state["previousStatus"], state["closedPolicy"]),
                         ("failed", "candidates_running", "candidate-timeout"))
        self.assertEqual(state["candidates"]["candidate-1"]["status"], "invalidated")
        self.assertTrue(task_root.is_dir())
        item = queue._items[0]
        self.assertEqual(item["status"], "failed")
        self.assertFalse(item["capacityHeld"])
        self.assertIn("候选阶段超时", item["error"])
        self.assertEqual(item["guardStopped"]["killedProcesses"], 2)
        self.assertEqual(item["quota"]["state"], "claimed")  # no platform attached in this test
        self.assertIn("guard.stopped", self.log.names())

    def test_enforce_refunds_the_quota(self):
        task_root = self._task("gb-1-slow", race_age=7 * HOUR)
        queue = self._queue(task_root)
        guard = self._guard(queue, mode="enforce")

        class _Platform:
            released = []

            def release_task(self, task_id):
                self.released.append(task_id)
                return {"ok": False, "mode": "local"}

        guard.platform = _Platform()
        queue._items[0]["quota"] = {"state": "claimed", "platformTaskId": "t-9"}
        guard.run_once()
        self.assertEqual(queue._items[0]["quota"]["state"], "refunded")
        self.assertEqual(_Platform.released, ["t-9"])

    def test_race_within_the_limit_is_left_alone(self):
        queue = self._queue(self._task("gb-1-ok", race_age=5 * HOUR))
        self._guard(queue, mode="enforce").run_once()
        self.assertEqual(queue._items[0]["status"], "triggered")
        self.assertFalse(queue._items[0].get("guardFlag"))

    def test_finished_race_does_not_count(self):
        queue = self._queue(self._task("gb-1-reviewing", race_age=9 * HOUR, race_done=True))
        self._guard(queue).run_once()
        self.assertFalse(queue._items[0].get("guardFlag"))

    def test_limit_is_configurable(self):
        queue = self._queue(self._task("gb-1-slow", race_age=3 * HOUR))
        self._guard(queue, candidatePhaseHours=2).run_once()
        self.assertEqual(queue._items[0]["guardFlag"]["policy"], "candidate-timeout")

    def test_excluded_project_is_skipped(self):
        self.config["automation"]["excludedProjectCodes"] = ["gb-1"]
        queue = self._queue(self._task("gb-1-slow", race_age=9 * HOUR))
        self._guard(queue, mode="enforce").run_once()
        self.assertEqual(queue._items[0]["status"], "triggered")

    def test_finished_queue_item_is_not_touched(self):
        task_root = self._task("gb-1-slow", race_age=9 * HOUR)
        queue = self._queue(task_root, status="done", capacityHeld=False)
        self._guard(queue, mode="enforce").run_once()
        self.assertEqual(queue._items[0]["status"], "done")
        self.assertEqual(self._state(task_root)["status"], "candidates_running")


class NoProgressTests(GuardTestCase):
    def test_quiet_task_is_flagged(self):
        queue = self._queue(self._task("gb-1-quiet", status="semantic_review_required", quiet=2 * HOUR))
        self._guard(queue).run_once()
        flag = queue._items[0]["guardFlag"]
        self.assertEqual(flag["policy"], "no-progress")
        self.assertRegex(flag["reason"], r"^1(19|20) 分钟没有任何写入")

    def test_candidate_output_counts_as_progress(self):
        task_root = self._task("gb-1-busy", quiet=2 * HOUR)
        stdout = task_root / "monitor" / "runtime" / "candidates" / "candidate-1" / "attempt-01" / "stdout.jsonl"
        stdout.parent.mkdir(parents=True)
        stdout.write_text("{}\n", encoding="utf-8")
        queue = self._queue(task_root)
        self._guard(queue).run_once()
        self.assertFalse(queue._items[0].get("guardFlag"))

    def test_dependency_churn_is_not_progress(self):
        task_root = self._task("gb-1-quiet", quiet=2 * HOUR)
        noise = task_root / "source" / "candidates" / "candidate-1" / "node_modules" / "x.js"
        noise.parent.mkdir(parents=True)
        noise.write_text("x", encoding="utf-8")
        self.assertLess(latest_activity(task_root, []), time.time() - HOUR)

    def test_flag_clears_when_activity_resumes(self):
        task_root = self._task("gb-1-quiet", quiet=2 * HOUR)
        queue = self._queue(task_root)
        guard = self._guard(queue)
        guard.run_once()
        self.assertTrue(queue._items[0]["guardFlag"])
        (task_root / "workspace").mkdir(exist_ok=True)
        (task_root / "workspace" / "gsb.md").write_text("草稿", encoding="utf-8")
        guard.run_once()
        self.assertFalse(queue._items[0]["guardFlag"])
        self.assertEqual(queue.guard_status["flags"], [])

    def test_finished_task_state_is_not_no_progress(self):
        queue = self._queue(self._task("gb-1-done", status="complete", quiet=5 * HOUR))
        self._guard(queue).run_once()
        self.assertFalse(queue._items[0].get("guardFlag"))


class LeakedProcessTests(GuardTestCase):
    def _leaky(self, name="ld-9-finished", status="complete", quiet=3 * HOUR):
        task_root = self._task(name, status=status, quiet=quiet)
        self.processes = [
            {"pid": 5001, "pgid": 5000, "command": f"node {task_root}/monitor/verify/a/frontend/node_modules/.bin/vite --port 5610"},
            {"pid": 5002, "pgid": 5002, "command": f"/usr/bin/login -qflp me /bin/zsh -fc cd '{task_root}/source'"},
        ]
        return task_root

    def test_observe_lists_the_leak(self):
        task_root = self._leaky()
        queue = build_queue(self.config, items=[])
        guard = self._guard(queue)
        guard.run_once()
        guard.run_once()
        leaks = queue.guard_status["leaks"]
        self.assertEqual([leak["name"] for leak in leaks], [task_root.name])
        self.assertEqual([p["pid"] for p in leaks[0]["processes"]], [5001])
        self.assertEqual(self.log.names().count("guard.leak_found"), 1)
        self.assertEqual(self.killed, [])

    def test_enforce_kills_only_non_shell_processes(self):
        self._leaky()
        queue = build_queue(self.config, items=[])
        actions = self._guard(queue, mode="enforce").run_once()
        self.assertEqual(self.killed, [5001])
        self.assertEqual(actions[0]["kind"], "guard-leak-killed")

    def test_recently_finished_task_is_left_alone(self):
        self._leaky(quiet=10 * 60)
        queue = build_queue(self.config, items=[])
        self._guard(queue, mode="enforce").run_once()
        self.assertEqual(self.killed, [])

    def test_unfinished_task_is_not_a_leak(self):
        self._leaky(status="gsb_ready")
        queue = build_queue(self.config, items=[])
        self._guard(queue, mode="enforce").run_once()
        self.assertEqual(self.killed, [])

    def test_task_owned_by_an_active_item_is_not_a_leak(self):
        task_root = self._leaky(status="failed")
        queue = self._queue(task_root)
        self._guard(queue, mode="enforce").run_once()
        self.assertEqual(self.killed, [])


class GuardModeTests(GuardTestCase):
    def test_off_clears_flags_and_checks_nothing(self):
        queue = self._queue(self._task("gb-1-slow", race_age=9 * HOUR))
        guard = self._guard(queue)
        guard.run_once()
        self.assertTrue(queue._items[0]["guardFlag"])
        self.config["automation"]["guard"]["mode"] = "off"
        guard.run_once()
        self.assertFalse(queue._items[0]["guardFlag"])
        self.assertEqual(queue.guard_status["mode"], "off")

    def test_settings_default_to_observe_and_are_clamped(self):
        self.assertEqual(guard_settings({})["mode"], "observe")
        clamped = guard_settings({"automation": {"guard": {"mode": "nuke", "candidatePhaseHours": 0.1,
                                                           "noProgressMinutes": 99999}}})
        self.assertEqual((clamped["mode"], clamped["candidatePhaseHours"], clamped["noProgressMinutes"]),
                         ("observe", 1, 1440))

    def test_snapshot_carries_the_guard_status(self):
        queue = self._queue(self._task("gb-1-slow", race_age=9 * HOUR))
        self._guard(queue).run_once()
        self.assertEqual(queue.fast_snapshot()["guard"]["mode"], "observe")
        self.assertEqual(len(queue.snapshot()["guard"]["flags"]), 1)

    def test_stopped_item_stays_failed_after_a_state_machine_pass(self):
        task_root = self._task("gb-1-slow", race_age=9 * HOUR)
        queue = self._queue(task_root)
        self._guard(queue, mode="enforce").run_once()
        queue.snapshot()
        self.assertEqual(queue._items[0]["status"], "failed")
        self.assertFalse(queue._items[0]["capacityHeld"])


class SetGuardActionTests(SchedulerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.service = SchedulerService(self.config)
        self.addCleanup(self.service.stop)

    def test_mode_and_thresholds_persist(self):
        snapshot = self.service.automation_action("set-guard", {"mode": "enforce", "noProgressMinutes": 90,
                                                                "candidatePhaseHours": 7.5})
        self.assertEqual(snapshot["guard"]["mode"], "enforce")
        self.assertEqual(snapshot["guard"]["noProgressMinutes"], 90)
        saved = json.loads((self.root / "config.json").read_text(encoding="utf-8"))["automation"]["guard"]
        self.assertEqual((saved["mode"], saved["noProgressMinutes"], saved["candidatePhaseHours"]),
                         ("enforce", 90, 7.5))

    def test_bad_values_are_rejected(self):
        for payload in ({"mode": "nuke"}, {"noProgressMinutes": 5}, {"candidatePhaseHours": "x"}, {}):
            with self.assertRaises(MonitorError):
                self.service.automation_action("set-guard", payload)

    def test_reconcile_runs_the_guard(self):
        self.assertIs(self.service.reconcile.guard, self.service.guard)
        self.service.reconcile.run_once()
        self.assertEqual(self.service.queue.guard_status["mode"], "observe")
