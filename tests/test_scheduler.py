"""Tests for the queue state machine, capacity modes, quota and reconcile."""
from __future__ import annotations

import json
import os
import sys
import time
import unittest
from pathlib import Path
from unittest import mock

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

from api.common import MonitorError, atomic_write_json, iso_from_timestamp, utc_now  # noqa: E402
from api.scheduler import ContainerLedger, JobManager, QueueManager, ReconcileLoop  # noqa: E402
from api.tasks import DockerCache  # noqa: E402
from tests.support import SchedulerTestCase, make_config, state_dir_of, write_task  # noqa: E402


def docker_fixture(items):
    docker = DockerCache(ttl=0)
    docker._data = {"items": items, "byName": {}, "error": "", "fetchedOk": True}
    return docker


def build_queue(config, items=None, docker=None):
    """A queue backed by a throwaway state dir, never the live one."""
    state_dir = state_dir_of(config)
    (state_dir / "jobs").mkdir(parents=True, exist_ok=True)
    jobs = JobManager(config, state_dir=state_dir / "jobs")
    queue = QueueManager(
        config,
        jobs,
        docker_cache=docker or fake_docker([]),
        state_path=state_dir / "queue.json",
        # Never touch the live shared slot directory.
        slot_root=state_dir / "container-slots",
    )
    if items is not None:
        queue._items = items
        queue._save()
    # Production reaches tick() only after start() has read the task tree and
    # docker and the grace period has elapsed; reproduce that here.
    queue.state_loaded = True
    queue.started_at = time.time() - 10_000
    return queue


def fake_docker(items):
    class _Docker:
        def __init__(self, data):
            self._data = data

        def get(self, force=False):
            return dict(self._data)

    return _Docker({"items": items, "byName": {}, "error": "", "fetchedOk": True})


def tick_with_ready_runner(queue, root):
    """Run one scheduler tick with launch prerequisites mocked as ready."""
    with mock.patch.object(
        queue.jobs,
        "validate_platform_runner",
        return_value=(Path("/tmp/fake-sologsb.py"), Path("/tmp/queue_worker.py"), [root]),
    ), mock.patch.object(queue.jobs, "start_platform", return_value={"pid": 1234}):
        queue.tick()


def platform_item(**overrides):
    item = {
        "id": "platform-1",
        "source": "platform",
        "taskRoot": "",
        "scopeRoot": "",
        "taskName": "gb-1 示例项目",
        "projectCode": "gb-1",
        "projectName": "示例项目",
        "variantId": "variant-1",
        "taskType": "0-1代码生成",
        "difficulty": "困难",
        "side": "both",
        "triggerPrompt": "预拉 2 份候选",
        "status": "pending",
        "attempts": 0,
        "stalledRetryCount": 0,
        "runKey": "rk1",
        "capacityHeld": False,
        "orphaned": False,
        "slotMarker": "",
        "quota": {"state": "pending", "variantId": "variant-1", "remainingBefore": 5},
    }
    item.update(overrides)
    return item


class QueueBasicsTests(SchedulerTestCase):
    def _queue(self, items=None, docker=None):
        return build_queue(self.config, items=items, docker=docker)

    def test_add_and_remove_local_task(self):
        task_root = write_task(self.root, "gb-9-20260920")
        queue = self._queue()
        item = queue.add(task_root, "both")
        self.assertEqual(item["status"], "pending")
        self.assertEqual(queue.pending_count(), 1)
        queue.remove(item["id"])
        self.assertEqual(queue.pending_count(), 0)

    def test_add_rejects_duplicate_local_task(self):
        task_root = write_task(self.root, "gb-9-20260920")
        queue = self._queue()
        queue.add(task_root, "both")
        from api.common import MonitorError

        with self.assertRaises(MonitorError):
            queue.add(task_root, "both")

    def test_platform_item_records_quota_and_folder(self):
        queue = self._queue()
        item = queue.add_platform(
            {"code": "gb-1", "name": "示例项目", "variantId": "variant-1",
             "quotaBefore": {"remaining": 5}},
            folder_id="folder-1",
            folder_path="/tmp/work",
        )
        self.assertEqual(item["quota"]["state"], "pending")
        self.assertEqual(item["quota"]["remainingBefore"], 5)
        self.assertEqual(item["folderId"], "folder-1")
        self.assertEqual(item["slotMarkers"], [])

    def test_retry_resets_quota_and_run_key(self):
        queue = self._queue([platform_item(status="failed", capacityHeld=False, quota={
            "state": "refunded", "variantId": "variant-1", "platformTaskId": "t1",
            "remainingBefore": 5, "refundReason": "失败",
        })])
        item = queue._items[0]
        queue.retry(item["id"])
        self.assertEqual(item["status"], "pending")
        self.assertEqual(item["quota"]["state"], "pending")
        self.assertNotEqual(item["runKey"], "rk1")
        self.assertEqual(item["quota"]["platformTaskId"], "")


class ContainerGateTests(SchedulerTestCase):
    """The monitor bounds tasks; the skill lock bounds candidate containers."""

    def _queue(self, docker_items, **cfg):
        config = make_config(self.root)
        if cfg:
            config["automation"].update(cfg)
            # The hard limit is the minimum of maxContainers and the key-level
            # cap, so both have to move together for the test to say what it means.
            if "maxContainers" in cfg:
                config["automation"].setdefault("keyConcurrency", {})["maxCandidateContainers"] = cfg["maxContainers"]
        return build_queue(config, docker=fake_docker(docker_items))

    def _running(self, count, prefix="gb-1-20260920-120000-abc"):
        return [
            {"name": f"sologsb-{prefix}-candidate-{index}-{1700000000 + index:03x}", "state": "running"}
            for index in range(1, count + 1)
        ]

    def test_non_candidate_sologsb_containers_do_not_fill_capacity(self):
        queue = self._queue([
            {"name": "sologsb-gb131-test-minio", "state": "running"},
            {"name": "sologsb-gb131-test-mysql", "state": "running"},
            {"name": "sologsb-gb131-test-redis", "state": "running"},
        ], maxContainers=4, candidatesPerTask=2, containerRefillBelow=4)

        with queue._lock:
            capacity_in_use, detail = queue._capacity_usage_locked(queue._startup_timeout())

        self.assertEqual(capacity_in_use, 0)
        self.assertEqual(detail["nonTestContainerCount"], 0)
        self.assertEqual(detail["estimatedNonTestContainers"], 0)
        self.assertEqual(detail["containerGroups"], [])

    def test_running_container_count_does_not_block_task_admission(self):
        queue = self._queue(
            self._running(4),
            capacity=3,
            maxContainers=4,
            candidatesPerTask=2,
            containerRefillBelow=4,
        )
        item = platform_item()
        queue._items = [item]
        queue._save()
        tick_with_ready_runner(queue, self.root)
        self.assertEqual(item["status"], "running")
        self.assertEqual(str(item.get("containerWait") or ""), "")

    def test_task_capacity_still_blocks_additional_workers(self):
        active = platform_item(id="platform-active", status="triggered", capacityHeld=True)
        pending = platform_item(id="platform-pending")
        queue = self._queue([], capacity=1)
        queue._items = [active, pending]
        queue._save()

        tick_with_ready_runner(queue, self.root)

        self.assertEqual(pending["status"], "pending")


class StartupGuardTests(SchedulerTestCase):
    """A fresh process must not size capacity against an unread world."""

    def _fresh_queue(self, grace=100):
        config = make_config(self.root)
        config["automation"]["startupGraceSeconds"] = grace
        queue = build_queue(config)
        # build_queue clears the guard; undo that to simulate a cold start.
        queue.state_loaded = False
        queue.started_at = time.time()
        return queue

    def test_blocks_before_the_state_is_read(self):
        queue = self._fresh_queue()
        queue.started_at = time.time() - 10_000  # grace long gone
        item = platform_item()
        queue._items = [item]
        queue._save()
        queue.tick()
        self.assertIn("尚未读取", str(item.get("containerWait") or ""))

    def test_blocks_during_the_grace_period(self):
        queue = self._fresh_queue(grace=100)
        queue.state_loaded = True
        item = platform_item()
        queue._items = [item]
        queue._save()
        queue.tick()
        self.assertIn("启动保护期", str(item.get("containerWait") or ""))

    def test_allows_once_state_is_read_and_grace_elapsed(self):
        queue = self._fresh_queue(grace=100)
        queue.state_loaded = True
        queue.started_at = time.time() - 200
        item = platform_item()
        queue._items = [item]
        queue._save()
        tick_with_ready_runner(queue, self.root)
        self.assertEqual(str(item.get("containerWait") or ""), "")

    def test_grace_defaults_to_100_seconds(self):
        queue = build_queue(make_config(self.root))
        self.assertEqual(queue.startup_grace_seconds(), 100)

    def test_grace_is_clamped(self):
        config = make_config(self.root)
        config["automation"]["startupGraceSeconds"] = 99999
        queue = build_queue(config)
        self.assertEqual(queue.startup_grace_seconds(), 3600)


class StalledRetryTests(SchedulerTestCase):
    """A stalled retry must not start a second attempt beside a live task."""

    def _item(self, **over):
        base = platform_item(status="running", capacityHeld=True, stalledRetrySeconds=600,
                             stalledRetryLimit=1)
        base.update(over)
        return base

    def _task_dir(self, name, status):
        root_dir = self.root / "tasks" / name
        (root_dir / "monitor").mkdir(parents=True, exist_ok=True)
        (root_dir / "monitor" / "state.json").write_text(
            json.dumps({"taskName": name, "status": status}), encoding="utf-8")
        # Make the state file old enough to look stalled.
        old = time.time() - 1200
        os.utime(root_dir / "monitor" / "state.json", (old, old))
        return root_dir

    def test_holds_when_the_desktop_task_is_still_running(self):
        task_root = self._task_dir("gb-1-20260920-120000-abc", "candidates_running")
        config = make_config(self.root)
        config["automation"]["stalledTaskRetrySeconds"] = 600
        config["automation"]["stalledTaskRetryLimit"] = 1
        queue = build_queue(config)
        item = self._item(id="platform-1", taskRoot=str(task_root), runKey="rk1")
        queue._items = [item]
        queue._save()
        queue._sync_running_locked()
        # Must NOT go back to pending with a fresh run key.
        self.assertEqual(item["status"], "orphaned")
        self.assertTrue(item["capacityHeld"])
        self.assertEqual(item["runKey"], "rk1")
        self.assertIn("桌面任务仍处于", str(item.get("error") or ""))

    def test_requeues_when_the_desktop_task_never_started(self):
        """No state.json means nothing is running, so a retry is safe."""
        config = make_config(self.root)
        config["automation"]["stalledTaskRetrySeconds"] = 600
        config["automation"]["stalledTaskRetryLimit"] = 1
        queue = build_queue(config)
        item = self._item(id="platform-2", taskRoot="", runKey="rk2")
        queue._items = [item]
        queue._save()
        queue._sync_running_locked()
        self.assertEqual(item["status"], "pending")
        self.assertNotEqual(item["runKey"], "rk2")
        self.assertEqual(item["stalledRetryCount"], 1)

    def test_held_item_carries_an_orphan_timestamp(self):
        task_root = self._task_dir("gb-4-20260920-120000-abc", "candidates_running")
        config = make_config(self.root)
        config["automation"]["stalledTaskRetrySeconds"] = 600
        config["automation"]["stalledTaskRetryLimit"] = 1
        queue = build_queue(config)
        item = self._item(id="platform-4", taskRoot=str(task_root), runKey="rk4")
        queue._items = [item]
        queue._save()
        queue._sync_running_locked()
        # The grace-period release keys off this.
        self.assertTrue(item.get("orphanedAt"))

    def test_liveness_counts_candidate_trajectories(self):
        """The outer state.json is untouched for a whole candidate attempt."""
        task_root = self._task_dir("gb-3-20260920-120000-abc", "candidates_running")
        attempt = task_root / "monitor" / "runtime" / "candidates" / "candidate-1" / "attempt-01"
        attempt.mkdir(parents=True, exist_ok=True)
        trace = attempt / "stdout.jsonl"
        trace.write_text("{}\n", encoding="utf-8")  # fresh
        config = make_config(self.root)
        queue = build_queue(config)
        stamp = queue._latest_activity_timestamp(
            item={}, job=None, task_root=task_root, result_file=Path("/nonexistent"))
        self.assertGreater(stamp, time.time() - 30)

    def test_manual_release_is_blocked_while_desktop_task_is_live(self):
        task_root = self._task_dir("gb-5-20260920-120000-abc", "candidates_running")
        queue = build_queue(make_config(self.root))
        item = platform_item(
            id="platform-5",
            status="orphaned",
            capacityHeld=True,
            orphaned=True,
            taskRoot=str(task_root),
        )
        queue._items = [item]
        queue._save()
        with self.assertRaises(MonitorError):
            queue.release(item["id"])
        self.assertEqual(item["status"], "orphaned")
        self.assertTrue(item["capacityHeld"])


class CooldownTests(SchedulerTestCase):
    """The minimum interval between task creations applies to every start."""

    def _queue(self, cooldown):
        config = make_config(self.root)
        config["automation"]["cooldownSeconds"] = cooldown
        return build_queue(config)

    def test_recent_start_blocks_the_next_one(self):
        queue = self._queue(210)
        queue._lastStartedAt = utc_now()
        item = platform_item()
        queue._items = [item]
        queue._save()
        queue.tick()
        self.assertIn("冷却", str(item.get("containerWait") or ""))

    def test_cooldown_applies_without_prior_saturation(self):
        """An idle queue used to fire several tasks back-to-back."""
        queue = self._queue(210)
        queue._lastStartedAt = utc_now()
        self.assertFalse(queue._capacity_saturated)
        item = platform_item()
        queue._items = [item]
        queue._save()
        queue.tick()
        self.assertIn("冷却", str(item.get("containerWait") or ""))

    def test_expired_cooldown_lets_the_next_one_through(self):
        queue = self._queue(210)
        queue._lastStartedAt = iso_from_timestamp(time.time() - 400)
        item = platform_item()
        queue._items = [item]
        queue._save()
        tick_with_ready_runner(queue, self.root)
        self.assertEqual(str(item.get("containerWait") or ""), "")

    def test_zero_cooldown_never_blocks(self):
        queue = self._queue(0)
        queue._lastStartedAt = utc_now()
        item = platform_item()
        queue._items = [item]
        queue._save()
        tick_with_ready_runner(queue, self.root)
        self.assertEqual(str(item.get("containerWait") or ""), "")


class SlotLedgerTests(SchedulerTestCase):
    def setUp(self):
        super().setUp()
        self.ledger = ContainerLedger(root=self.root / "slots")

    def test_reserve_and_snapshot(self):
        marker = self.ledger.reserve(container="sologsb-gb-1-a-1", project_code="gb-1", item_id="platform-1")
        self.assertIsNotNone(marker)
        snapshot = self.ledger.snapshot()
        self.assertEqual(snapshot["occupiedCount"], 1)
        self.assertEqual(snapshot["occupied"][0]["itemId"], "platform-1")

    def test_release_for_item_removes_only_its_markers(self):
        self.ledger.reserve(container="sologsb-gb-1-a-1", project_code="gb-1", item_id="platform-1")
        self.ledger.reserve(container="sologsb-gb-2-a-1", project_code="gb-2", item_id="platform-2")
        removed = self.ledger.release_for_item("platform-1")
        self.assertEqual(len(removed), 1)
        self.assertEqual(self.ledger.snapshot()["occupiedCount"], 1)

    def test_marker_with_dead_pid_is_swept(self):
        marker = self.ledger.reserve(container="sologsb-gb-1-a-1", project_code="gb-1", item_id="platform-1")
        data = json.loads(Path(marker).read_text(encoding="utf-8"))
        data["pid"] = 999999
        atomic_write_json(Path(marker), data)
        removed = self.ledger.sweep_dead()
        self.assertEqual(len(removed), 1)
        self.assertEqual(self.ledger.snapshot()["occupiedCount"], 0)

    def test_existing_container_frees_the_reservation(self):
        marker = self.ledger.reserve(container="sologsb-gb-1-a-1", project_code="gb-1", item_id="platform-1")
        ledger = ContainerLedger(root=self.root / "slots", docker_cache=fake_docker([
            {"name": "sologsb-gb-1-a-1", "state": "running"},
        ]))
        snapshot = ledger.snapshot()
        self.assertEqual(snapshot["occupiedCount"], 0)
        self.assertTrue(Path(marker).is_file())


class ReconcileTests(SchedulerTestCase):
    def _queue(self, items, docker_items=None):
        return build_queue(self.config, items=items, docker=fake_docker(docker_items or []))

    def _result_file(self, *, age_seconds: float) -> Path:
        path = self.root / "result.json"
        path.write_text(json.dumps({"status": "running", "stage": "desktop-submitted"}), encoding="utf-8")
        stamp = time.time() - age_seconds
        os.utime(path, (stamp, stamp))
        return path

    def test_recent_uncorroborated_desktop_stage_holds_the_slot(self):
        item = platform_item(
            status="orphaned",
            capacityHeld=True,
            orphaned=True,
            resultFile=str(self._result_file(age_seconds=10)),
        )
        queue = self._queue([item])

        self.assertIn("desktop-submitted", queue.live_task_reason(item))

    def test_old_uncorroborated_desktop_stage_releases_the_slot(self):
        item = platform_item(
            status="orphaned",
            capacityHeld=True,
            orphaned=True,
            triggeredAt="2020-01-01T00:00:00Z",
            resultFile=str(self._result_file(age_seconds=10_000)),
        )
        queue = self._queue([item])
        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=None)

        actions = loop.run_once()

        self.assertEqual(queue.live_task_reason(item), "")
        self.assertTrue(any(action["kind"] == "orphan-released" for action in actions))
        self.assertEqual(item["status"], "skipped")
        self.assertFalse(item["capacityHeld"])

        queue._sync_running_locked()

        self.assertEqual(item["status"], "skipped")
        self.assertFalse(item["capacityHeld"])

    def test_dead_markers_are_swept(self):
        queue = self._queue([])
        marker = queue.slots.reserve(container="sologsb-gb-1-a-1", project_code="gb-1", item_id="platform-1")
        data = json.loads(Path(marker).read_text(encoding="utf-8"))
        data["pid"] = 999999
        atomic_write_json(Path(marker), data)
        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=None)
        actions = loop.run_once()
        self.assertTrue(any(action["kind"] == "dead-markers" for action in actions))

    def test_attempt_inflation_is_flagged_once(self):
        item = platform_item(status="failed", attempts=900)
        queue = self._queue([item])
        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=None)
        loop.run_once()
        self.assertTrue(item["attemptsAlerted"])
        first = list(loop.last_actions)
        loop.run_once()
        self.assertEqual(len([a for a in loop.last_actions if a["kind"] == "attempts-inflated"]), 0)
        self.assertTrue(first)

    def test_stale_claimed_quota_is_force_refunded(self):
        item = platform_item(status="running", capacityHeld=True, quota={
            "state": "claimed",
            "variantId": "variant-1",
            "platformTaskId": "task-1",
            "deductedAt": "2020-01-01T00:00:00Z",
            "remainingBefore": 5,
        })
        queue = self._queue([item])

        class _Platform:
            def __init__(self):
                self.released = []

            def release_task(self, task_id, **kwargs):
                self.released.append(task_id)
                return {"ok": True, "mode": "platform"}

            def project_quota(self, code, task_type=""):
                return {"remaining": 5}

        platform = _Platform()
        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=platform)
        actions = loop.run_once()
        self.assertTrue(any(action["kind"] == "quota-force-refund" for action in actions))
        self.assertEqual(platform.released, ["task-1"])
        self.assertEqual(item["quota"]["state"], "refunded")

    def test_terminal_container_removal_requires_a_terminal_task_state(self):
        """A running container of a live task must never be removed."""
        task_root = self.root / "tasks" / "gb-live-20260920-120000-abc"
        task_root.mkdir(parents=True, exist_ok=True)
        (task_root / "monitor").mkdir(parents=True, exist_ok=True)
        # The queue says the item is done, but the task itself is still racing.
        (task_root / "monitor" / "state.json").write_text(json.dumps({"status": "candidates_running"}), encoding="utf-8")
        item = platform_item(status="done", taskRoot=str(task_root))
        queue = build_queue(
            self.config,
            items=[item],
            docker=fake_docker([
                {"name": "sologsb-gb-live-20260920-120000-abc-candidate-1-1790000000-abc", "state": "running"},
            ]),
        )
        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=None)
        actions = loop.run_once()
        self.assertEqual([a for a in actions if a["kind"] == "zombie-container"], [])

    def test_triggered_archive_does_not_count_as_terminal(self):
        """``_triggered`` also holds jobs recovered at startup that still run."""
        task_root = self.root / "tasks" / "gb-live-20260920-120000-abc"
        task_root.mkdir(parents=True, exist_ok=True)
        (task_root / "monitor").mkdir(parents=True, exist_ok=True)
        (task_root / "monitor" / "state.json").write_text(json.dumps({"status": "candidates_running"}), encoding="utf-8")
        queue = build_queue(
            self.config,
            items=[],
            docker=fake_docker([
                {"name": "sologsb-gb-live-20260920-120000-abc-candidate-1-1790000000-abc", "state": "running"},
            ]),
        )
        queue._triggered = [{"id": "platform-1", "taskRoot": str(task_root)}]
        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=None)
        actions = loop.run_once()
        self.assertEqual([a for a in actions if a["kind"] == "zombie-container"], [])

    def test_terminal_state_resyncs_from_result_json(self):
        item = platform_item(
            status="triggered",
            capacityHeld=True,
            taskRoot=str(self.root / "tasks" / "gb-1-20260920-120000-abc"),
            resultFile=str(self.root / "result.json"),
            stateStatus="running",
        )
        atomic_write_json(Path(item["resultFile"]), {
            "status": "running",
            "stage": "desktop-task-running",
            "taskRoot": item["taskRoot"],
            "stateStatus": "complete",
        })
        queue = self._queue([item])
        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=None)
        actions = loop.run_once()
        self.assertTrue(any(action["kind"] == "state-resync" for action in actions))
        self.assertEqual(item["stateStatus"], "complete")

    def test_stuck_orphan_is_released_after_grace(self):
        item = platform_item(status="orphaned", capacityHeld=True, orphaned=True,
                             triggeredAt="2020-01-01T00:00:00Z")
        queue = self._queue([item])
        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=None)
        loop.run_once()
        self.assertEqual(item["status"], "skipped")
        self.assertFalse(item["capacityHeld"])

    def test_stuck_orphan_keeps_slot_when_desktop_task_is_live(self):
        task_root = self.root / "tasks" / "gb-6-20260920-120000-abc"
        (task_root / "monitor").mkdir(parents=True, exist_ok=True)
        (task_root / "monitor" / "state.json").write_text(
            json.dumps({"status": "candidates_running"}), encoding="utf-8")
        item = platform_item(
            id="platform-6",
            status="orphaned",
            capacityHeld=True,
            orphaned=True,
            taskRoot=str(task_root),
            triggeredAt="2020-01-01T00:00:00Z",
        )
        queue = self._queue([item])
        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=None)
        loop.run_once()
        self.assertEqual(item["status"], "orphaned")
        self.assertTrue(item["capacityHeld"])
        self.assertIn("桌面任务仍处于", str(item.get("notice") or ""))


class SlotLifecycleTests(SchedulerTestCase):
    """The monitor delegates per-container admission to the skill limiter."""

    def _queue(self):
        return build_queue(self.config)

    def _ready_runner(self, queue):
        return mock.patch.object(
            queue.jobs,
            "validate_platform_runner",
            return_value=(Path("/tmp/fake-sologsb.py"), Path("/tmp/queue_worker.py"), [self.root]),
        )

    def test_task_start_does_not_create_monitor_reservations(self):
        queue = self._queue()
        queue.add_platform({"code": "gb-7", "name": "示例", "variantId": "v1",
                            "quotaBefore": {"remaining": 3}})
        with self._ready_runner(queue), mock.patch.object(queue.jobs, "start_platform", return_value={"pid": 1234}):
            queue.tick()
        self.assertEqual(queue._items[0]["status"], "running")
        self.assertEqual(queue._items[0]["slotMarkers"], [])
        self.assertEqual(queue._items[0]["slotReservedAt"], "")
        self.assertEqual(queue.slots.snapshot()["occupiedCount"], 0)

    def test_failed_start_releases_its_reservations(self):
        queue = self._queue()
        item = queue.add_platform({"code": "gb-7", "name": "示例", "variantId": "v1"})

        with self._ready_runner(queue), mock.patch.object(
            queue.jobs, "start_platform", side_effect=MonitorError("启动失败")
        ):
            queue.tick()
        self.assertEqual(item["status"], "pending")
        self.assertEqual(item["slotMarkers"], [])
        self.assertFalse(item["capacityHeld"])
        self.assertEqual(queue.slots.snapshot()["occupiedCount"], 0)

    def test_reconcile_removes_legacy_monitor_reservations(self):
        queue = build_queue(self.config, items=[platform_item(status="triggered", capacityHeld=True)])
        markers = queue.slots.reserve_batch(
            count=2,
            container_prefix="sologsb-gb-1",
            project_code="gb-1",
            item_id="platform-1",
        )
        queue._items[0]["slotMarkers"] = markers
        queue._items[0]["slotReservedAt"] = "2026-09-20T00:00:00Z"
        queue._save()

        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=None)
        actions = loop.run_once()

        self.assertTrue(any(action["kind"] == "legacy-monitor-reservations" for action in actions))
        self.assertEqual(queue._items[0]["slotMarkers"], [])
        self.assertEqual(queue._items[0]["slotReservedAt"], "")
        self.assertEqual(queue.slots.snapshot()["occupiedCount"], 0)

    def test_reconcile_does_not_remove_executor_owned_markers(self):
        queue = self._queue()
        marker = queue.slots.reserve(
            container="sologsb-gb-1-candidate-1",
            project_code="gb-1",
        )
        self.assertIsNotNone(marker)

        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=None)
        loop.run_once()

        self.assertEqual(queue.slots.snapshot()["occupiedCount"], 1)

    def test_reconcile_clears_missing_marker_paths_from_queue(self):
        queue = build_queue(self.config, items=[platform_item(
            status="pending",
            slotMarkers=[str(self.root / "missing-marker.json")],
            slotReservedAt="2026-09-20T00:00:00Z",
        )])

        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=None)
        actions = loop.run_once()

        self.assertTrue(any(action["kind"] == "legacy-monitor-reservations" for action in actions))
        self.assertEqual(queue._items[0]["slotMarkers"], [])
        self.assertEqual(queue._items[0]["slotReservedAt"], "")


class QuotaLedgerTests(SchedulerTestCase):
    """pending → claimed → settled | refunded, with a local-only fallback."""

    class _Platform:
        def __init__(self, releasable=True):
            self.releasable = releasable
            self.deducted = []
            self.released = []

        def pre_deduct(self, variant_id, task_type, **kwargs):
            self.deducted.append((variant_id, task_type))
            return {"platformTaskId": "task-42", "platformTaskNo": "gb-9-代码生成-1",
                    "platformRoundId": "round-1", "projectUsageCount": 4}

        def release_task(self, task_id, **kwargs):
            self.released.append(task_id)
            if not self.releasable:
                return {"ok": False, "mode": "local", "error": "HTTP 405"}
            return {"ok": True, "mode": "platform", "releasedAt": "now"}

        def project_quota(self, code, task_type=""):
            return {"remaining": 5}

    def _queue_with_item(self, code="gb-9"):
        queue = build_queue(self.config)
        queue.add_platform({"code": code, "name": "示例", "variantId": "v-9",
                            "quotaBefore": {"remaining": 5}})
        return queue

    def test_pre_deduct_records_the_platform_task(self):
        platform = self._Platform()
        queue = self._queue_with_item()
        queue.claim_quota(queue._items[0], platform)
        quota = queue._items[0]["quota"]
        self.assertEqual(quota["state"], "claimed")
        self.assertEqual(quota["platformTaskId"], "task-42")
        self.assertEqual(quota["platformTaskNo"], "gb-9-代码生成-1")
        self.assertEqual(quota["remainingBefore"], 5)
        # The create response reports cumulative usage, not remaining quota —
        # see the note in claim_quota.
        self.assertEqual(quota["usageCountAfter"], 4)
        self.assertNotIn("remainingAfter", quota)
        self.assertEqual(platform.deducted, [("v-9", "0-1代码生成")])

    def test_runner_preflight_blocks_before_quota_claim(self):
        platform = self._Platform()
        queue = self._queue_with_item()

        queue.tick(platform=platform)

        self.assertEqual(platform.deducted, [])
        self.assertEqual(queue._items[0]["quota"]["state"], "pending")
        self.assertIn("找不到 sologsb CLI", queue._items[0]["error"])

    def test_item_level_start_failure_refunds_the_claim(self):
        platform = self._Platform()
        queue = self._queue_with_item()

        with mock.patch.object(
            queue.jobs,
            "validate_platform_runner",
            return_value=(Path("/tmp/fake-sologsb.py"), Path("/tmp/queue_worker.py"), [self.root]),
        ), mock.patch.object(queue.jobs, "start_platform", side_effect=MonitorError("worker 启动失败")):
            queue.tick(platform=platform)

        quota = queue._items[0]["quota"]
        self.assertEqual(platform.deducted, [("v-9", "0-1代码生成")])
        self.assertEqual(platform.released, ["task-42"])
        self.assertEqual(quota["state"], "refunded")
        self.assertEqual(quota["refundMode"], "platform")
        self.assertEqual(queue._items[0]["status"], "failed")

    def test_settle_marks_the_attempt_consumed(self):
        queue = self._queue_with_item()
        queue.claim_quota(queue._items[0], self._Platform())
        queue.settle_quota(queue._items[0], success=True, reason="complete")
        self.assertEqual(queue._items[0]["quota"]["state"], "settled")

    def test_refund_calls_the_platform_release_endpoint(self):
        platform = self._Platform()
        queue = self._queue_with_item()
        queue.claim_quota(queue._items[0], platform)
        queue.refund_quota(queue._items[0], platform, "任务失败")
        quota = queue._items[0]["quota"]
        self.assertEqual(quota["state"], "refunded")
        self.assertEqual(quota["refundMode"], "platform")
        self.assertEqual(quota["remainingAfter"], 5)
        self.assertEqual(platform.released, ["task-42"])

    def test_refund_degrades_to_local_bookkeeping(self):
        platform = self._Platform(releasable=False)
        queue = self._queue_with_item()
        queue.claim_quota(queue._items[0], platform)
        queue.refund_quota(queue._items[0], platform, "无法确认桌面任务已停止")
        quota = queue._items[0]["quota"]
        self.assertEqual(quota["state"], "refunded")
        self.assertEqual(quota["refundMode"], "local")
        # The release was still attempted; only the accounting degrades.
        self.assertEqual(platform.released, ["task-42"])

    def test_retry_resets_the_ledger(self):
        queue = self._queue_with_item()
        queue.claim_quota(queue._items[0], self._Platform())
        queue.refund_quota(queue._items[0], self._Platform(), "失败")
        queue.retry(queue._items[0]["id"])
        quota = queue._items[0]["quota"]
        self.assertEqual(quota["state"], "pending")
        self.assertEqual(quota["platformTaskId"], "")


class FolderScopeTests(SchedulerTestCase):
    """The selected folder is the single scan scope and queue workdir."""

    def _queue(self, **cfg):
        config = make_config(self.root)
        if cfg:
            config["automation"].update(cfg)
        return build_queue(config)

    def test_active_roots_are_not_intersected_with_configured_roots(self):
        """A folder outside config.roots must still become the scope."""
        outside = self.root / "elsewhere"
        outside.mkdir(parents=True, exist_ok=True)
        queue = self._queue()
        queue.config["roots"] = [str(self.root / "tasks")]
        queue.config.setdefault("monitor", {})["activeRoots"] = [str(outside)]
        self.assertEqual(queue.active_roots(), [str(outside.resolve())])

    def test_empty_active_roots_falls_back_to_configured_roots(self):
        queue = self._queue()
        queue.config.setdefault("monitor", {})["activeRoots"] = []
        self.assertEqual(queue.active_roots(), queue._roots_locked())

    def test_default_scope_root_follows_the_folder(self):
        outside = self.root / "elsewhere"
        outside.mkdir(parents=True, exist_ok=True)
        queue = self._queue()
        queue.config.setdefault("monitor", {})["activeRoots"] = [str(outside)]
        with queue._lock:
            self.assertEqual(queue._default_scope_root_locked(), str(outside.resolve()))

    def test_snapshot_reports_the_effective_roots(self):
        outside = self.root / "elsewhere"
        outside.mkdir(parents=True, exist_ok=True)
        queue = self._queue()
        queue.config.setdefault("monitor", {})["activeRoots"] = [str(outside)]
        snapshot = queue.snapshot()
        self.assertEqual(snapshot["activeRoots"], [str(outside.resolve())])


class BlocklistTests(SchedulerTestCase):
    """A disabled project leaves the queue and cannot come back."""

    def _queue(self):
        return build_queue(self.config)

    def test_add_platform_rejects_a_blocked_project(self):
        queue = self._queue()
        queue.blocked_codes = {"gb-9"}
        from api.common import MonitorError

        with self.assertRaises(MonitorError) as ctx:
            queue.add_platform({"code": "gb-9", "name": "示例", "variantId": "v1"})
        self.assertIn("禁用", str(ctx.exception))

    def test_blocklist_is_case_insensitive(self):
        queue = self._queue()
        queue.blocked_codes = {"GB-9"}
        from api.common import MonitorError

        with self.assertRaises(MonitorError):
            queue.add_platform({"code": "gb-9", "name": "示例", "variantId": "v1"})

    def test_unblocked_project_can_be_added(self):
        queue = self._queue()
        queue.blocked_codes = {"gb-other"}
        item = queue.add_platform({"code": "gb-9", "name": "示例", "variantId": "v1"})
        self.assertEqual(item["projectCode"], "gb-9")


class RefillThresholdTests(SchedulerTestCase):
    """containerRefillBelow is only meaningful below the hard limit."""

    def _queue(self, **cfg):
        config = make_config(self.root)
        config["automation"].update(cfg)
        return build_queue(config)

    def test_threshold_above_the_hard_limit_is_clamped(self):
        queue = self._queue(maxContainers=4, containerRefillBelow=5)
        self.assertEqual(queue._max_containers_limit(), 4)
        self.assertEqual(queue.effective_refill_below(), 4)

    def test_threshold_below_the_hard_limit_is_kept(self):
        queue = self._queue(maxContainers=6, containerRefillBelow=3)
        self.assertEqual(queue.effective_refill_below(), 3)

    def test_snapshot_reports_both_configured_and_effective(self):
        queue = self._queue(maxContainers=4, containerRefillBelow=9)
        snapshot = queue.snapshot()
        self.assertEqual(snapshot["containerRefillBelow"], 4)
        self.assertEqual(snapshot["containerRefillBelowConfigured"], 9)


class ScheduleModeTests(SchedulerTestCase):
    def test_default_mode_is_container_first(self):
        from api.common import DEFAULT_CONFIG

        self.assertEqual(DEFAULT_CONFIG["automation"]["scheduleMode"], "containers")

    def test_mode_switch_persists(self):
        from api.common import load_config, save_config

        config = make_config(self.root)
        config["automation"]["scheduleMode"] = "tasks"
        save_config(config, Path(config["_configPath"]))
        reloaded = load_config(Path(config["_configPath"]))
        self.assertEqual(reloaded["automation"]["scheduleMode"], "tasks")

    def test_reserve_and_startup_bounds_are_clamped(self):
        from api.common import MAX_CONTAINER_RESERVE_SECONDS, MIN_STARTUP_TIMEOUT_SECONDS

        config = make_config(self.root)
        jobs = JobManager(config)
        queue = QueueManager(config, jobs, docker_cache=fake_docker([]))
        config["automation"]["containerReserveSeconds"] = 99999
        config["automation"]["startupTimeoutSeconds"] = 1
        self.assertEqual(queue._container_reserve_seconds(), MAX_CONTAINER_RESERVE_SECONDS)
        self.assertEqual(queue._startup_timeout(), MIN_STARTUP_TIMEOUT_SECONDS)


if __name__ == "__main__":
    unittest.main()
