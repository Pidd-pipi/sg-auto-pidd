"""Auto refill: the on/off switch, its status line and the stable preview head."""
from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

from api.common import AUTO_REFILL_PREVIEW_SIZE, SettingsStore  # noqa: E402
from api.service import SchedulerService  # noqa: E402
from tests.support import SchedulerTestCase  # noqa: E402


class _Platform:
    """Solo Manager stand-in: every task type offers the same fresh projects."""

    def __init__(self, codes):
        self.codes = codes

    def candidates(self, *, task_type, force=False, include_pool=False):
        return {"items": [{"code": code, "name": f"项目 {code}", "variantId": f"v-{code}"} for code in self.codes],
                "excluded": []}

    def set_blocklist(self, codes):
        pass


class AutoRefillSwitchTests(SchedulerTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.service = SchedulerService(self.config)
        self.service.settings = SettingsStore(self.root / ".state" / "settings.json")
        self.addCleanup(self.service.stop)

    def _saved(self):
        return json.loads((self.root / "config.json").read_text(encoding="utf-8"))

    def _events(self):
        path = self.root / ".state" / "scheduler.jsonl"
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

    def _add_pending(self, count, prefix="gb"):
        for index in range(count):
            self.service.queue.add_platform({"code": f"{prefix}-{index}", "name": f"项目 {index}",
                                             "variantId": f"v-{index}"})

    def test_switch_persists_and_logs(self):
        snapshot = self.service.automation_action("set-auto-refill", {"enabled": True})
        self.assertTrue(snapshot["autoRefill"]["enabled"])
        self.assertTrue(self._saved()["automation"]["autoRefill"]["enabled"])

        snapshot = self.service.automation_action("set-auto-refill", {"enabled": False})
        self.assertFalse(snapshot["autoRefill"]["enabled"])
        self.assertFalse(self._saved()["automation"]["autoRefill"]["enabled"])
        self.assertEqual(snapshot["autoRefill"]["lastRun"]["status"], "disabled")
        details = [event["detail"] for event in self._events() if event["event"] == "config.auto_refill"]
        self.assertEqual(details, ["自动补队 → 开启", "自动补队 → 关闭"])

    def test_switch_keeps_the_other_refill_settings(self):
        self.config["automation"]["autoRefill"].update({"targetPending": 7, "taskTypes": ["feature迭代"]})
        self.service.automation_action("set-auto-refill", {"enabled": True})
        saved = self._saved()["automation"]["autoRefill"]
        self.assertEqual(saved["targetPending"], 7)
        self.assertEqual(saved["taskTypes"], ["feature迭代"])

    def test_snapshot_exposes_interval_and_preview_size(self):
        refill = self.service.queue.fast_snapshot()["autoRefill"]
        self.assertEqual(refill["previewSize"], AUTO_REFILL_PREVIEW_SIZE)
        self.assertGreaterEqual(refill["intervalSeconds"], 30)
        self.assertEqual(refill["lastRun"]["status"], "idle")

    def test_turning_on_refills_without_waiting_out_the_interval(self):
        self.service._last_queue_refill_at = time.time()
        self.service.automation_action("set-auto-refill", {"enabled": True})
        self.assertEqual(self.service._last_queue_refill_at, 0.0)

    def test_refill_round_is_reported_and_keeps_the_preview_head(self):
        self._add_pending(AUTO_REFILL_PREVIEW_SIZE + 5)
        head = [item["id"] for item in self.service.queue._items[:AUTO_REFILL_PREVIEW_SIZE]]
        self.config["automation"]["autoRefill"].update({
            "enabled": True, "targetPending": AUTO_REFILL_PREVIEW_SIZE + 8, "taskTypes": ["feature迭代"],
            "taskTypeWeights": {"feature迭代": 100}, "randomize": True, "shuffleExisting": True,
        })
        self.service.platform = _Platform([f"new-{index}" for index in range(10)])

        result = self.service.maybe_refill_queue()

        self.assertEqual(result["added"], 3)
        self.assertEqual([item["id"] for item in self.service.queue._items[:AUTO_REFILL_PREVIEW_SIZE]], head)
        last = self.service.queue.fast_snapshot()["autoRefill"]["lastRun"]
        self.assertEqual(last["added"], 3)
        self.assertIn("随机补队 3 项", last["message"])


class ShufflePendingTests(SchedulerTestCase):
    def test_keep_head_leaves_the_first_items_in_place(self):
        service = SchedulerService(self.config)
        self.addCleanup(service.stop)
        for index in range(15):
            service.queue.add_platform({"code": f"gb-{index}", "name": f"项目 {index}", "variantId": f"v-{index}"})
        before = [item["id"] for item in service.queue._items]
        rng = random.Random(7)
        tails = set()
        for _ in range(20):
            self.assertTrue(service.queue.shuffle_pending(rng, keep_head=10))
            after = [item["id"] for item in service.queue._items]
            self.assertEqual(after[:10], before[:10])
            self.assertEqual(sorted(after[10:]), sorted(before[10:]))
            tails.add(tuple(after[10:]))
        self.assertGreater(len(tails), 1)

    def test_nothing_to_shuffle_behind_a_short_queue(self):
        service = SchedulerService(self.config)
        self.addCleanup(service.stop)
        for index in range(10):
            service.queue.add_platform({"code": f"gb-{index}", "name": f"项目 {index}", "variantId": f"v-{index}"})
        self.assertFalse(service.queue.shuffle_pending(random.Random(1), keep_head=10))
