"""Tests for loop heartbeats and the watchdog (unattended operation)."""
from __future__ import annotations

import subprocess
import sys
import threading
import unittest
from pathlib import Path
from unittest import mock

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP))

from api.common import MonitorError, keychain_read, keychain_write  # noqa: E402
from api.health import FAILING_AFTER, STALL_FLOOR_SECONDS, LoopHealth, Watchdog  # noqa: E402


class FakeClock:
    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now


class Recorder:
    def __init__(self):
        self.events: list[tuple[str, str, str]] = []

    def __call__(self, event: str, level: str = "info", **fields) -> None:
        self.events.append((event, level, str(fields.get("detail") or "")))

    def names(self) -> list[str]:
        return [event for event, _level, _detail in self.events]


class LoopHealthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.health = LoopHealth(clock=self.clock)
        self.health.register("auto-loop", 3)

    def status(self) -> str:
        return self.health.snapshot()[0]["status"]

    def test_stall_threshold_has_a_floor(self):
        self.assertEqual(self.health.snapshot()[0]["stallAfterSeconds"], STALL_FLOOR_SECONDS)

    def test_a_loop_that_keeps_beating_is_healthy(self):
        for _ in range(5):
            self.clock.now += 3
            self.health.begin("auto-loop")
            self.health.end("auto-loop")
        self.assertEqual(self.status(), "ok")
        self.assertTrue(self.health.healthy())

    def test_a_hung_iteration_is_stalled(self):
        """The failure that used to go unnoticed: begin() with no end()."""
        self.health.begin("auto-loop")
        self.clock.now += STALL_FLOOR_SECONDS + 1
        self.assertEqual(self.status(), "stalled")
        self.assertFalse(self.health.healthy())
        self.assertGreater(self.health.snapshot()[0]["busySeconds"], STALL_FLOOR_SECONDS)

    def test_repeated_failures_are_failing_but_not_stalled(self):
        for _ in range(FAILING_AFTER):
            self.health.begin("auto-loop")
            self.health.end("auto-loop", "queue-tick: boom")
        snapshot = self.health.snapshot()[0]
        self.assertEqual(snapshot["status"], "failing")
        self.assertEqual(snapshot["lastError"], "queue-tick: boom")
        self.assertTrue(self.health.healthy())
        self.health.begin("auto-loop")
        self.health.end("auto-loop")
        self.assertEqual(self.status(), "ok")

    def test_check_reports_each_transition_once(self):
        self.clock.now += STALL_FLOOR_SECONDS + 1
        self.assertEqual([item["stalled"] for item in self.health.check()], [True])
        self.assertEqual(self.health.check(), [])
        self.health.end("auto-loop")
        self.assertEqual([item["stalled"] for item in self.health.check()], [False])


class WatchdogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.health = LoopHealth(clock=self.clock)
        self.health.register("auto-loop", 3)
        self.emit = Recorder()
        self.watchdog = Watchdog(self.health, emit=self.emit, interval=1)

    def test_stall_is_logged_once_with_the_stuck_stack_and_recovery_is_logged(self):
        release = threading.Event()
        entered = threading.Event()

        def hung_loop():
            self.health.begin("auto-loop")
            entered.set()
            release.wait(5)  # stands in for a subprocess without a timeout
            self.health.end("auto-loop")

        thread = threading.Thread(target=hung_loop, daemon=True)
        thread.start()
        self.assertTrue(entered.wait(2))
        self.clock.now += STALL_FLOOR_SECONDS + 1
        self.watchdog.run_once()
        self.watchdog.run_once()
        stalls = [item for item in self.emit.events if item[0] == "watchdog.loop_stalled"]
        self.assertEqual(len(stalls), 1)
        self.assertEqual(stalls[0][1], "error")
        self.assertIn("hung_loop", stalls[0][2])
        release.set()
        thread.join(2)
        self.watchdog.run_once()
        self.assertIn("watchdog.loop_recovered", self.emit.names())

    def test_dead_thread_is_restarted(self):
        dead = threading.Thread(target=lambda: None)
        dead.start()
        dead.join()
        box = {"thread": dead}

        def restart():
            fresh = threading.Thread(target=lambda: None)
            fresh.start()
            box["thread"] = fresh
            return fresh

        self.watchdog.supervise("auto-loop", lambda: box["thread"], restart)
        self.watchdog.run_once()
        self.assertIn("watchdog.loop_dead", self.emit.names())
        self.assertIsNot(box["thread"], dead)
        self.assertEqual(self.health.snapshot()[0]["restarts"], 1)


class KeychainTimeoutTests(unittest.TestCase):
    """A locked keychain made ``security`` wait for an unlock prompt forever."""

    def test_read_gives_up_instead_of_hanging(self):
        with mock.patch("api.common.subprocess.run",
                        side_effect=subprocess.TimeoutExpired(["security"], 10)) as run:
            self.assertEqual(keychain_read("solo-manager-password"), "")
        self.assertEqual(run.call_args.kwargs["timeout"], 10)

    def test_write_reports_a_timeout(self):
        with mock.patch("api.common.subprocess.run",
                        side_effect=subprocess.TimeoutExpired(["security"], 10)):
            with self.assertRaises(MonitorError):
                keychain_write("solo-manager-password", "secret")


if __name__ == "__main__":
    unittest.main()
