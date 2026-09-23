"""Liveness of the background loops, for unattended operation.

Every loop already survives its own exceptions, so the failure that used to go
unnoticed was a loop that *hangs* (a subprocess or socket with no timeout, a
lock that is never released) or a thread that dies of something that is not an
``Exception``.  Either way the scheduler silently stops launching and nothing in
the UI changes.  Loops report ``begin``/``end`` here; the watchdog notices a
beat that stops, logs where the thread is stuck, and restarts dead threads.
"""
from __future__ import annotations

import sys
import threading
import time
import traceback
from typing import Any, Callable

# A loop is stalled when it has not finished an iteration for this many of its
# own intervals, and never sooner than the floor — a queue tick may legitimately
# spend a minute on Manager HTTP, Keychain and a worker launch.
STALL_INTERVALS = 10
STALL_FLOOR_SECONDS = 180.0
FAILING_AFTER = 3
STACK_LINES = 14


class LoopHealth:
    """Heartbeats of named loops."""

    def __init__(self, clock: Callable[[], float] = time.time):
        self._clock = clock
        self._lock = threading.Lock()
        self._loops: dict[str, dict[str, Any]] = {}

    def register(self, name: str, interval: float, *, stall_after: float | None = None) -> None:
        now = self._clock()
        with self._lock:
            record = self._loops.setdefault(name, {
                "name": name,
                "registeredAt": now,
                "lastBeatAt": now,
                "lastOkAt": 0.0,
                "runningSince": 0.0,
                "iterations": 0,
                "failures": 0,
                "lastError": "",
                "threadId": 0,
                "stalled": False,
                "restarts": 0,
            })
            record["interval"] = float(interval)
            record["stallAfter"] = float(stall_after or max(STALL_FLOOR_SECONDS, float(interval) * STALL_INTERVALS))

    def now(self) -> float:
        return self._clock()

    def begin(self, name: str) -> None:
        with self._lock:
            record = self._loops.get(name)
            if record is not None:
                record["runningSince"] = self._clock()
                record["threadId"] = threading.get_ident()

    def end(self, name: str, error: str = "") -> None:
        now = self._clock()
        with self._lock:
            record = self._loops.get(name)
            if record is None:
                return
            record["lastBeatAt"] = now
            record["runningSince"] = 0.0
            record["iterations"] += 1
            if error:
                record["failures"] += 1
                record["lastError"] = str(error)[:300]
            else:
                record["failures"] = 0
                record["lastOkAt"] = now

    def note_restart(self, name: str) -> None:
        with self._lock:
            record = self._loops.get(name)
            if record is not None:
                record["restarts"] += 1
                record["lastBeatAt"] = self._clock()
                record["runningSince"] = 0.0

    def check(self) -> list[dict[str, Any]]:
        """Return loops whose stalled flag just changed (for logging once)."""
        now = self._clock()
        changed: list[dict[str, Any]] = []
        with self._lock:
            for record in self._loops.values():
                stalled = now - record["lastBeatAt"] > record["stallAfter"]
                if stalled != record["stalled"]:
                    record["stalled"] = stalled
                    changed.append(dict(record))
        return changed

    def snapshot(self) -> list[dict[str, Any]]:
        now = self._clock()
        out: list[dict[str, Any]] = []
        with self._lock:
            for record in self._loops.values():
                silent = now - record["lastBeatAt"]
                if silent > record["stallAfter"]:
                    status = "stalled"
                elif record["failures"] >= FAILING_AFTER:
                    status = "failing"
                else:
                    status = "ok"
                out.append({
                    "name": record["name"],
                    "status": status,
                    "secondsSinceBeat": round(silent, 1),
                    "stallAfterSeconds": record["stallAfter"],
                    "busySeconds": round(now - record["runningSince"], 1) if record["runningSince"] else 0,
                    "iterations": record["iterations"],
                    "failures": record["failures"],
                    "lastError": record["lastError"],
                    "restarts": record["restarts"],
                })
        return out

    def healthy(self) -> bool:
        return all(item["status"] != "stalled" for item in self.snapshot())

    def stack_of(self, name: str) -> str:
        """Where the loop's thread is right now — the evidence for a hang."""
        with self._lock:
            ident = int((self._loops.get(name) or {}).get("threadId") or 0)
        frame = sys._current_frames().get(ident) if ident else None
        if frame is None:
            return ""
        lines = "".join(traceback.format_stack(frame)).rstrip().splitlines()
        return "\n".join(lines[-STACK_LINES:])


class Watchdog:
    """Periodically checks :class:`LoopHealth` and revives dead loop threads."""

    def __init__(
        self,
        health: LoopHealth,
        *,
        emit: Callable[..., None],
        interval: float = 30.0,
    ):
        self.health = health
        self.emit = emit
        self.interval = max(1.0, float(interval))
        self._threads: dict[str, tuple[Callable[[], threading.Thread | None], Callable[[], threading.Thread]]] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def supervise(
        self,
        name: str,
        current: Callable[[], threading.Thread | None],
        restart: Callable[[], threading.Thread],
    ) -> None:
        """Register a loop thread: ``current`` returns it, ``restart`` starts a new one."""
        self._threads[name] = (current, restart)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="watchdog", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.run_once()
            except Exception:  # pragma: no cover - the watchdog must not die
                pass

    def run_once(self) -> None:
        for name, (current, restart) in self._threads.items():
            thread = current()
            if thread is not None and not thread.is_alive() and not self._stop.is_set():
                self.emit("watchdog.loop_dead", level="error", detail=f"后台循环 {name} 的线程已退出，自动重启")
                try:
                    restart()
                    self.health.note_restart(name)
                except Exception as exc:
                    self.emit("watchdog.restart_failed", level="error", detail=f"{name}: {exc}")
        for record in self.health.check():
            name = record["name"]
            if record["stalled"]:
                stack = self.health.stack_of(name)
                self.emit(
                    "watchdog.loop_stalled",
                    level="error",
                    detail=(
                        f"后台循环 {name} 已 {int(self.health.now() - record['lastBeatAt'])} 秒没有完成一轮"
                        f"（阈值 {int(record['stallAfter'])} 秒）"
                        + (f"\n当前位置：\n{stack}" if stack else "")
                    ),
                )
            else:
                self.emit("watchdog.loop_recovered", detail=f"后台循环 {name} 已恢复")
