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
    """The old gate compared running >= refillBelow and stalled five early."""

    def _queue(self, docker_items, **cfg):
        config = make_config(self.root)
        if cfg:
            config["automation"].update(cfg)
        return build_queue(config, docker=fake_docker(docker_items))

    def _running(self, count, prefix="gb-1-20260920-120000-abc"):
        return [
            {"name": f"sologsb-{prefix}-candidate-{index}-{1700000000 + index:03x}", "state": "running"}
            for index in range(1, count + 1)
        ]

    def test_one_free_slot_is_enough_to_start(self):
        """The executor queues overflow, so the monitor need not fit the batch.

        Requiring room for all ``candidatesPerTask`` containers could only fire
        at two containers against a limit of four, so the count oscillated
        2↔4 and never sat at four.
        """
        queue = self._queue(self._running(3), maxContainers=4, candidatesPerTask=2)
        item = platform_item()
        queue._items = [item]
        queue._save()
        tick_with_ready_runner(queue, self.root)
        # The worker is not startable in a test, so the item stays pending — but it
        # must not be parked on any container-wait message.
        self.assertEqual(str(item.get("containerWait") or ""), "")

    def test_waits_when_no_slot_is_free(self):
        queue = self._queue(self._running(4), maxContainers=4, candidatesPerTask=2)
        item = platform_item()
        queue._items = [item]
        queue._save()
        queue.tick()
        self.assertIn("容器名额已满", str(item.get("containerWait") or ""))
        self.assertEqual(item.get("containerWaitKind"), "capacity")

    def test_waits_at_the_hard_limit_even_with_a_batch_of_one(self):
        queue = self._queue(self._running(6), maxContainers=6, candidatesPerTask=2)
        item = platform_item()
        queue._items = [item]
        queue._save()
        queue.tick()
        self.assertIn("容器名额已满", str(item.get("containerWait") or ""))
        self.assertEqual(item.get("containerWaitKind"), "capacity")

    def test_container_mode_ignores_the_task_cap(self):
        """Tasks past the candidate race hold no container; they must not
        keep the container slots empty just because ``capacity`` is reached."""
        queue = self._queue(self._running(2), maxContainers=4, candidatesPerTask=2, capacity=1)
        item = platform_item()
        queue._items = [item]
        queue._save()
        with mock.patch.object(queue, "_sync_running_locked"), mock.patch.object(
            queue.jobs,
            "validate_platform_runner",
            return_value=(Path("/tmp/fake-sologsb.py"), Path("/tmp/queue_worker.py"), [self.root]),
        ), mock.patch.object(queue.jobs, "start_platform", return_value={"pid": 1234}):
            queue.tick()
        self.assertEqual(str(item.get("containerWait") or ""), "")

    def test_task_mode_gates_on_live_tasks(self):
        queue = self._queue([], scheduleMode="tasks", capacity=1)
        task_root = write_task(self.root, "gb-2-20260920-120000-abc", status="running")
        live = platform_item(id="platform-live", status="triggered", capacityHeld=True,
                             taskRoot=str(task_root), startedAt=utc_now())
        waiting = platform_item(id="platform-wait")
        queue._items = [live, waiting]
        queue._save()
        queue.tick()
        self.assertIn("并行任务已满", str(waiting.get("containerWait") or ""))

    def test_duplicate_pending_waits_for_the_same_project(self):
        queue = self._queue([], scheduleMode="tasks", capacity=3)
        active = platform_item(
            id="platform-active",
            projectCode="gb-dup",
            status="triggered",
            capacityHeld=True,
        )
        pending = platform_item(id="platform-pending", projectCode="gb-dup")
        queue._items = [active, pending]
        queue._save()

        with mock.patch.object(queue, "_sync_running_locked"), mock.patch.object(
            queue.jobs,
            "validate_platform_runner",
            return_value=(Path("/tmp/fake-sologsb.py"), Path("/tmp/queue_worker.py"), [self.root]),
        ), mock.patch.object(queue.jobs, "start_platform", return_value={"pid": 1234}):
            queue.tick()

        self.assertEqual(pending["status"], "pending")
        self.assertIn("同一项目已有任务在执行", str(pending.get("containerWait") or ""))


class ContainerDemandTests(SchedulerTestCase):
    """Occupancy = running containers + what live tasks still need."""

    def _task(self, name, state):
        root_dir = self.root / "tasks" / name
        (root_dir / "monitor").mkdir(parents=True, exist_ok=True)
        (root_dir / "monitor" / "state.json").write_text(json.dumps(state), encoding="utf-8")
        return root_dir

    def _usage(self, items, docker_items=()):
        config = make_config(self.root)
        queue = build_queue(config, items=items, docker=fake_docker(list(docker_items)))
        with queue._lock:
            _tasks, detail = queue._capacity_usage_locked(queue._startup_timeout())
        return detail

    def test_uninitialised_task_needs_a_full_batch(self):
        item = platform_item(status="running", capacityHeld=True, startedAt=utc_now())
        detail = self._usage([item])
        self.assertEqual(detail["estimatedNonTestContainers"], 2)

    def test_queued_candidates_count_but_running_ones_are_not_doubled(self):
        name = "gb-8-20260920-120000-abc"
        root_dir = self._task(name, {"status": "candidates_running", "candidates": {
            "candidate-1": {"status": "running"},   # has a container
            "candidate-2": {"status": "running"},   # waiting in the skill's limiter
        }})
        item = platform_item(status="triggered", capacityHeld=True, taskRoot=str(root_dir),
                             startedAt=utc_now())
        detail = self._usage([item], [{"name": f"sologsb-{name}-candidate-1-1790000000-abc", "state": "running"}])
        self.assertEqual(detail["nonTestContainerCount"], 1)
        self.assertEqual(detail["estimatedNonTestContainers"], 2)

    def test_finished_race_needs_nothing(self):
        root_dir = self._task("gb-9-20260920-120000-abc", {
            "status": "running", "candidateRaceFinishedAt": "2026-09-20T00:00:00Z",
            "candidates": {"candidate-1": {"status": "staged"}, "candidate-2": {"status": "staged"}},
        })
        item = platform_item(status="triggered", capacityHeld=True, taskRoot=str(root_dir),
                             startedAt=utc_now())
        detail = self._usage([item])
        self.assertEqual(detail["estimatedNonTestContainers"], 0)


class PhantomDemandTests(SchedulerTestCase):
    """A candidate stuck at ``running`` without a container must not hold a slot forever.

    The skill's limiter gives a free slot to a queued candidate within seconds,
    so demand that never turns into a container while the limit has room
    belongs to a dead executor.  It used to block every launch until the
    30-minute orphan grace (and, before that fix, for 13 hours).
    """

    NAME = "gb-7-20260920-120000-abc"

    def setUp(self) -> None:
        super().setUp()
        root_dir = self.root / "tasks" / self.NAME
        (root_dir / "monitor").mkdir(parents=True, exist_ok=True)
        (root_dir / "monitor" / "state.json").write_text(json.dumps({
            "status": "candidates_running",
            "candidates": {"candidate-1": {"status": "running"}, "candidate-2": {"status": "running"}},
        }), encoding="utf-8")
        self.item = platform_item(status="triggered", capacityHeld=True, taskRoot=str(root_dir),
                                  startedAt=utc_now())
        self.events = []

        class _Log:
            def emit(inner, event, **fields):
                self.events.append(event)

        self.log = _Log()

    def _queue(self, running: int):
        docker = fake_docker([
            {"name": f"sologsb-other-{index}-candidate-1-1790000000-abc", "state": "running"}
            for index in range(running)
        ])
        queue = build_queue(make_config(self.root), items=[self.item], docker=docker)
        queue.log = self.log
        return queue

    def _detail(self, queue):
        with queue._lock:
            return queue._capacity_usage_locked(queue._startup_timeout())[1]

    def test_fresh_queued_candidates_count(self):
        queue = self._queue(running=1)
        self.assertEqual(self._detail(queue)["pendingContainerDemand"], 2)

    def test_demand_starved_past_the_threshold_is_dropped_and_logged_once(self):
        queue = self._queue(running=1)
        self._detail(queue)
        queue._starved_since[self.NAME] -= 601
        detail = self._detail(queue)
        self.assertEqual(detail["pendingContainerDemand"], 0)
        self.assertEqual(detail["phantomDemand"], [self.NAME])
        self._detail(queue)
        self.assertEqual(self.events.count("queue.phantom_demand"), 1)

    def test_waiting_behind_a_full_limit_is_not_phantom(self):
        queue = self._queue(running=4)
        self._detail(queue)
        self.assertNotIn(self.NAME, queue._starved_since)
        detail = self._detail(queue)
        self.assertEqual(detail["pendingContainerDemand"], 2)
        self.assertEqual(detail["phantomDemand"], [])

    def test_threshold_is_configurable_and_clamped(self):
        queue = self._queue(running=1)
        queue.config["automation"]["phantomDemandSeconds"] = 5
        self.assertEqual(queue._phantom_demand_seconds(), 120)
        queue.config["automation"]["phantomDemandSeconds"] = 900
        self.assertEqual(queue._phantom_demand_seconds(), 900)


class DuplicateEnqueueTests(SchedulerTestCase):
    def test_orphaned_item_blocks_a_second_entry_for_the_same_project(self):
        """An orphaned item may still have a live desktop task for the project."""
        queue = build_queue(self.config, items=[platform_item(status="orphaned", capacityHeld=True)])
        with self.assertRaises(MonitorError):
            queue.add_platform({"code": "gb-1", "name": "示例项目", "variantId": "variant-1"})

    def test_finished_item_does_not_block_a_new_entry(self):
        queue = build_queue(self.config, items=[platform_item(status="done")])
        item = queue.add_platform({"code": "gb-1", "name": "示例项目", "variantId": "variant-1"})
        self.assertEqual(item["status"], "pending")


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
        self.assertIn("桌面任务仍处于", str(item.get("notice") or ""))

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
        self.assertIn("启动间隔", str(item.get("containerWait") or ""))

    def test_cooldown_applies_without_prior_saturation(self):
        """An idle queue used to fire several tasks back-to-back."""
        queue = self._queue(210)
        queue._lastStartedAt = utc_now()
        self.assertFalse(queue._capacity_saturated)
        item = platform_item()
        queue._items = [item]
        queue._save()
        queue.tick()
        self.assertIn("启动间隔", str(item.get("containerWait") or ""))

    def test_expired_cooldown_lets_the_next_one_through(self):
        queue = self._queue(210)
        queue._lastStartedAt = iso_from_timestamp(time.time() - 400)
        item = platform_item()
        queue._items = [item]
        queue._save()
        tick_with_ready_runner(queue, self.root)
        self.assertEqual(str(item.get("containerWait") or ""), "")

    def test_zero_cooldown_still_keeps_the_minimum_spacing(self):
        """Two desktop launches in the same second lost a prompt."""
        queue = self._queue(0)
        queue._lastStartedAt = utc_now()
        item = platform_item()
        queue._items = [item]
        queue._save()
        queue.tick()
        self.assertIn("启动间隔 20 秒", str(item.get("containerWait") or ""))

    def test_zero_cooldown_passes_after_the_minimum_spacing(self):
        queue = self._queue(0)
        queue._lastStartedAt = iso_from_timestamp(time.time() - 30)
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
    """A claimed task takes slots, and gives them back when its containers appear."""

    def _queue(self):
        return build_queue(self.config)

    def _ready_runner(self, queue):
        return mock.patch.object(
            queue.jobs,
            "validate_platform_runner",
            return_value=(Path("/tmp/fake-sologsb.py"), Path("/tmp/queue_worker.py"), [self.root]),
        )

    def test_claim_writes_no_placeholder_markers(self):
        """The skill's limiter counts every live marker, so a placeholder for a
        task made that task's own candidates queue behind it."""
        queue = self._queue()
        queue.add_platform({"code": "gb-7", "name": "示例", "variantId": "v1",
                            "quotaBefore": {"remaining": 3}})
        with self._ready_runner(queue), mock.patch.object(queue.jobs, "start_platform", return_value={"pid": 1234}):
            queue.tick()
        self.assertEqual(queue._items[0].get("slotMarkers") or [], [])
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

    def test_reconcile_releases_markers_for_inactive_items(self):
        queue = build_queue(self.config, items=[platform_item(status="pending")])
        markers = [
            str(queue.slots.reserve(container=f"sologsb-gb-1-reserve-{index}", project_code="gb-1",
                                    item_id="platform-1"))
            for index in (1, 2)
        ]
        queue._items[0]["slotMarkers"] = markers
        queue._items[0]["slotReservedAt"] = "2026-09-20T00:00:00Z"
        queue._save()

        loop = ReconcileLoop(queue, queue.jobs, log=None, platform=None)
        actions = loop.run_once()

        self.assertTrue(any(action["kind"] == "inactive-reservations" for action in actions))
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

        self.assertTrue(any(action["kind"] == "inactive-reservations" for action in actions))
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


class TerminalIdempotencyTests(SchedulerTestCase):
    """A settled item must not be re-failed, resurrected or re-refunded."""

    class _Platform:
        def __init__(self):
            self.released = []

        def release_task(self, task_id):
            self.released.append(task_id)
            return {"ok": True, "mode": "platform"}

        def project_quota(self, code, task_type):
            return None

    def test_blocked_task_fails_once(self):
        task_root = write_task(self.root, "gb-5-20260920-120000-abc", status="blocked")
        old = time.time() - 60
        os.utime(task_root / "monitor" / "state.json", (old, old))
        platform = self._Platform()
        queue = build_queue(make_config(self.root))
        queue.platform = platform
        item = platform_item(status="triggered", capacityHeld=True, taskRoot=str(task_root),
                             quota={"state": "claimed", "platformTaskId": "t-5"})
        queue._items = [item]
        for _ in range(5):
            queue._sync_running_locked()
        self.assertEqual(item["status"], "failed")
        self.assertEqual(item["attempts"], 1)
        self.assertEqual(platform.released, ["t-5"])

    def test_refund_is_idempotent(self):
        platform = self._Platform()
        queue = build_queue(make_config(self.root))
        item = platform_item(quota={"state": "claimed", "platformTaskId": "t-6"})
        queue.refund_quota(item, platform, "x")
        queue.refund_quota(item, platform, "x")
        self.assertEqual(platform.released, ["t-6"])

    def test_skipped_item_is_not_resurrected_as_orphaned(self):
        result = self.root / "r.json"
        result.write_text(json.dumps({"stage": "desktop-submitted",
                                      "taskRoot": str(self.root / "tasks" / "gone")}), encoding="utf-8")
        queue = build_queue(make_config(self.root))
        item = platform_item(status="skipped", capacityHeld=False, resultFile=str(result),
                             autoReleased=True)
        queue._items = [item]
        queue._sync_running_locked()
        self.assertEqual(item["status"], "skipped")

    def test_retry_gets_a_fresh_quota(self):
        queue = build_queue(make_config(self.root))
        item = platform_item(status="failed", quota={"state": "refunded", "platformTaskId": "t-7",
                                                    "variantId": "variant-1"})
        queue._items = [item]
        queue.retry(item["id"])
        self.assertEqual(item["quota"]["state"], "pending")
        self.assertEqual(item["quota"]["platformTaskId"], "")
        self.assertEqual(item["quota"]["history"][0]["platformTaskId"], "t-7")


class SingleLimitTests(SchedulerTestCase):
    """``maxContainers`` is the one limit, shared with the skill's limiter."""

    def _queue(self, **cfg):
        config = make_config(self.root)
        config["automation"].update(cfg)
        return build_queue(config)

    def test_limit_is_clamped_to_the_skill_hard_cap(self):
        self.assertEqual(self._queue(maxContainers=12)._max_containers_limit(), 6)
        self.assertEqual(self._queue(maxContainers=5)._max_containers_limit(), 5)

    def test_limit_is_published_for_the_skill(self):
        queue = self._queue(maxContainers=5, excludedProjectCodes=["gb-501"])
        queue.sync_skill_limits()
        self.assertFalse(queue.sync_skill_limits())  # unchanged → no rewrite
        data = json.loads(queue.slots.limit_path.read_text(encoding="utf-8"))
        self.assertEqual(data["maxContainers"], 5)
        self.assertEqual(data["excludedProjectCodes"], ["gb-501"])
        self.assertEqual(data["managedBy"], "sologsb-monitor")
        self.assertTrue(queue.skill_limit_status()["inSync"])

    def test_publishing_keeps_unrelated_keys(self):
        queue = self._queue(maxContainers=4)
        limit_path = queue.slots.limit_path
        limit_path.write_text(json.dumps({"waitSeconds": 99}), encoding="utf-8")
        self.assertTrue(queue.sync_skill_limits())
        self.assertEqual(json.loads(limit_path.read_text(encoding="utf-8"))["waitSeconds"], 99)

    def test_legacy_knobs_are_pruned(self):
        from api.scheduler import prune_legacy_automation

        automation = {"containerRefillBelow": 3, "containerReserveSeconds": 420,
                      "keyConcurrency": {"maxCandidateContainers": 5}}
        removed = prune_legacy_automation(automation)
        self.assertEqual(sorted(removed), ["containerRefillBelow", "containerReserveSeconds", "keyConcurrency"])
        self.assertEqual(automation, {"maxContainers": 5})


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

    def test_startup_timeout_is_clamped(self):
        from api.common import MIN_STARTUP_TIMEOUT_SECONDS

        queue = build_queue(make_config(self.root))
        queue.config["automation"]["startupTimeoutSeconds"] = 1
        self.assertEqual(queue._startup_timeout(), MIN_STARTUP_TIMEOUT_SECONDS)


class StaleTaskClosureTests(SchedulerTestCase):
    """An abandoned running/blocked task dir would make the skill refuse the
    same project forever; the scheduler closes it right before relaunching."""

    def _blocked_task(self, name, code="cy-381", *, status="blocked", quiet_seconds=1200, run_pid=999999):
        task_root = self.root / "tasks" / name
        (task_root / "monitor").mkdir(parents=True, exist_ok=True)
        state = {
            "status": status,
            "source": {"projectCode": code},
            "blockedReason": "容器名额账本死锁",
            "candidates": {
                "candidate-1": {"status": status, "runPid": run_pid},
                "candidate-2": {"status": "cancelled"},
            },
        }
        path = task_root / "monitor" / "state.json"
        path.write_text(json.dumps(state), encoding="utf-8")
        old = time.time() - quiet_seconds
        os.utime(path, (old, old))
        return task_root

    def _read(self, task_root):
        return json.loads((task_root / "monitor" / "state.json").read_text(encoding="utf-8"))

    def test_launch_closes_the_abandoned_blocked_task_of_the_same_project(self):
        old = self._blocked_task("cy-381-20260923-001819-a350e2d3")
        other = self._blocked_task("gb-538-20260923-001414-605acfdd", code="gb-538")
        queue = build_queue(self.config, items=[platform_item(projectCode="cy-381")])
        tick_with_ready_runner(queue, self.root / "tasks")

        state = self._read(old)
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["previousStatus"], "blocked")
        self.assertEqual(state["closedBy"], "sologsb-monitor")
        self.assertEqual(state["blockedReason"], "容器名额账本死锁")
        self.assertEqual(state["candidates"]["candidate-1"]["status"], "invalidated")
        self.assertEqual(state["candidates"]["candidate-1"]["previousStatus"], "blocked")
        self.assertEqual(state["candidates"]["candidate-2"]["status"], "cancelled")
        self.assertFalse(QueueManager._state_active_for_skill(state))
        # Another project's task is not this launch's business.
        self.assertEqual(self._read(other)["status"], "blocked")

    def test_recently_written_state_is_left_alone(self):
        old = self._blocked_task("cy-381-20260923-001819-a350e2d3", quiet_seconds=5)
        queue = build_queue(self.config, items=[platform_item(projectCode="cy-381")])
        tick_with_ready_runner(queue, self.root / "tasks")
        self.assertEqual(self._read(old)["status"], "blocked")

    def test_task_with_live_containers_is_left_alone(self):
        old = self._blocked_task("cy-381-20260923-001819-a350e2d3", status="running")
        docker = fake_docker([{"name": f"sologsb-{old.name}-candidate-1-1-a", "state": "running"}])
        queue = build_queue(self.config, items=[platform_item(projectCode="cy-381")], docker=docker)
        tick_with_ready_runner(queue, self.root / "tasks")
        self.assertEqual(self._read(old)["status"], "running")

    def test_task_with_a_live_runner_pid_is_left_alone(self):
        old = self._blocked_task("cy-381-20260923-001819-a350e2d3", status="running", run_pid=os.getpid())
        queue = build_queue(self.config, items=[platform_item(projectCode="cy-381")])
        with mock.patch("api.scheduler.runner_pid_alive", return_value=True):
            tick_with_ready_runner(queue, self.root / "tasks")
        self.assertEqual(self._read(old)["status"], "running")

    def test_task_owned_by_an_active_queue_item_is_left_alone(self):
        old = self._blocked_task("cy-381-20260923-001819-a350e2d3", status="running")
        owner = platform_item(id="platform-owner", projectCode="cy-381", status="triggered",
                              capacityHeld=True, taskRoot=str(old))
        # Same project queued again behind a live one: the gate should stay.
        queue = build_queue(self.config, items=[owner, platform_item(id="platform-2", projectCode="cy-381")])
        with mock.patch.object(queue, "_sync_running_locked"):
            tick_with_ready_runner(queue, self.root / "tasks")
        self.assertEqual(self._read(old)["status"], "running")

    def test_closed_task_is_logged(self):
        self._blocked_task("cy-381-20260923-001819-a350e2d3")
        queue = build_queue(self.config, items=[platform_item(projectCode="cy-381")])
        with mock.patch.object(queue, "_emit") as emit:
            tick_with_ready_runner(queue, self.root / "tasks")
        events = [call.args[1] for call in emit.call_args_list]
        self.assertIn("queue.stale_task_closed", events)


class QueuePhaseTests(SchedulerTestCase):
    """``phase`` is what the queue page groups on."""

    def _task(self, name, status, candidates):
        task_root = self.root / "tasks" / name
        (task_root / "monitor").mkdir(parents=True, exist_ok=True)
        (task_root / "monitor" / "state.json").write_text(json.dumps({
            "status": status,
            "candidates": {f"candidate-{i + 1}": {"status": value} for i, value in enumerate(candidates)},
        }), encoding="utf-8")
        return task_root

    def _phase(self, item, docker_items=()):
        item = dict(item)
        QueueManager._annotate_phase(item, {"items": list(docker_items)}, queue_position=1)
        return item

    def test_pending_variants(self):
        self.assertEqual(self._phase(platform_item())["phase"], "queued")
        self.assertEqual(self._phase(platform_item(
            containerWait="容器名额已满", containerWaitKind="capacity"))["phase"], "waiting_slot")
        self.assertEqual(self._phase(platform_item(
            containerWait="启动间隔", containerWaitKind="spacing"))["phase"], "queued")
        self.assertEqual(self._phase(platform_item(
            nextAttemptAt=iso_from_timestamp(time.time() + 60), error="等待自动重试"))["phase"], "retrying")

    def test_triggered_without_containers_is_waiting_for_a_slot(self):
        task_root = self._task("gb-1-20260923-100000-aaaa", "candidates_running", ["running", "running"])
        item = self._phase(platform_item(status="triggered", taskRoot=str(task_root)))
        self.assertEqual(item["phase"], "waiting_slot")
        self.assertEqual(item["containers"], {"running": 0, "wanted": 2})

    def test_triggered_with_half_its_containers_is_still_waiting(self):
        task_root = self._task("gb-1-20260923-100000-aaaa", "candidates_running", ["running", "running"])
        docker = [{"name": f"sologsb-{task_root.name}-candidate-1-1-a", "state": "running"}]
        item = self._phase(platform_item(status="triggered", taskRoot=str(task_root)), docker)
        self.assertEqual(item["phase"], "waiting_slot")
        self.assertEqual(item["containers"], {"running": 1, "wanted": 2})

    def test_triggered_with_all_containers_is_executing(self):
        task_root = self._task("gb-1-20260923-100000-aaaa", "candidates_running", ["running", "running"])
        docker = [
            {"name": f"sologsb-{task_root.name}-candidate-1-1-a", "state": "running"},
            {"name": f"sologsb-{task_root.name}-candidate-2-1-b", "state": "running"},
        ]
        item = self._phase(platform_item(status="triggered", taskRoot=str(task_root)), docker)
        self.assertEqual(item["phase"], "executing")
        self.assertEqual(item["stateStatus"], "candidates_running")

    def test_post_race_phases_are_executing_even_without_containers(self):
        task_root = self._task("gb-1-20260923-100000-aaaa", "semantic_review_required", ["staged", "staged"])
        item = self._phase(platform_item(status="triggered", taskRoot=str(task_root)))
        self.assertEqual(item["phase"], "executing")

    def test_before_state_json_it_is_starting(self):
        self.assertEqual(self._phase(platform_item(status="running"))["phase"], "starting")
        self.assertEqual(self._phase(platform_item(status="triggered", taskRoot=str(self.root / "nope")))["phase"], "starting")
        task_root = self._task("gb-1-20260923-100000-aaaa", "prepared", [])
        self.assertEqual(self._phase(platform_item(status="triggered", taskRoot=str(task_root)))["phase"], "starting")

    def test_orphaned_needs_attention_and_terminal_keeps_status(self):
        self.assertEqual(self._phase(platform_item(status="orphaned"))["phase"], "attention")
        self.assertEqual(self._phase(platform_item(status="failed"))["phase"], "failed")
        self.assertEqual(self._phase(platform_item(status="skipped"))["phase"], "skipped")

    def test_snapshot_counts_phases(self):
        queue = build_queue(self.config, items=[platform_item(), platform_item(id="platform-2", status="orphaned")])
        counts = queue.snapshot()["counts"]["phases"]
        self.assertEqual(counts, {"queued": 1, "attention": 1})


class NoticeHygieneTests(SchedulerTestCase):
    """Notices describe the present; a stale one reads as a fault."""

    def test_retry_notice_clears_once_the_new_desktop_task_exists(self):
        task_root = self.root / "tasks" / "gb-1-20260923-100000-bbbb"
        (task_root / "monitor").mkdir(parents=True, exist_ok=True)
        (task_root / "monitor" / "state.json").write_text(json.dumps({"status": "prepared"}), encoding="utf-8")
        queue = build_queue(self.config)
        item = platform_item(status="running", capacityHeld=True, runKey="rk9", taskRoot=str(task_root),
                             notice="任务连续 10 分钟无状态更新，已自动重试第 1 次", noticeKind="retry",
                             error="任务连续 10 分钟无状态更新，已自动重试第 1 次")
        queue._items = [item]
        queue._save()
        job = {"key": "platform:platform-1", "source": "platform", "platformItemId": "platform-1",
               "runKey": "rk9", "status": "running", "pid": os.getpid(), "startedAt": utc_now(),
               "resultFile": "", "taskRoot": str(task_root)}
        with mock.patch.object(queue.jobs, "get_platform", return_value=job), \
                mock.patch("api.scheduler.persisted_job_process_alive", return_value=True):
            queue._sync_running_locked()
        self.assertEqual(item["status"], "triggered")
        self.assertNotIn("notice", item)
        self.assertEqual(item["error"], "")

    def test_stalled_notice_clears_when_activity_resumes(self):
        task_root = self.root / "tasks" / "gb-1-20260923-100000-cccc"
        (task_root / "monitor").mkdir(parents=True, exist_ok=True)
        (task_root / "monitor" / "state.json").write_text(json.dumps({"status": "candidates_running"}), encoding="utf-8")
        queue = build_queue(self.config)
        queue.config["automation"]["stalledTaskRetrySeconds"] = 600
        queue.config["automation"]["stalledTaskRetryLimit"] = 1
        item = platform_item(status="triggered", capacityHeld=True, taskRoot=str(task_root),
                             triggeredAt=utc_now(), stalledSince=utc_now(),
                             notice="已静默 601 秒，桌面任务仍处于 candidates_running，保留名额等待其终态",
                             noticeKind="stalled")
        queue._items = [item]
        queue._save()
        queue._sync_running_locked()
        self.assertNotIn("stalledSince", item)
        self.assertNotIn("notice", item)

if __name__ == "__main__":
    unittest.main()
