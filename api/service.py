"""Service orchestration plus the push hub that replaces client polling.

The old frontend rebuilt 67 task cards with ``innerHTML`` every three seconds and
pulled a ~1.1 MB snapshot each time.  Here a single background thread builds a
snapshot every 1.5 s, diffs it against the previous one, and pushes only the
tasks that actually changed over one SSE connection.  Clients fall back to
polling only while SSE is disconnected.
"""
from __future__ import annotations

import copy
import json
import os
import random
import shutil
import signal
import subprocess
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

from .common import (
    AUTO_REFILL_PREVIEW_SIZE,
    AUTO_STATE_PATH,
    DEFAULT_ROOT,
    DISMISSED_TASKS_PATH,
    QUEUE_ACTIVE_STATUSES,
    QUEUE_STATE_PATH,
    SCHEDULER_LOG_PATH,
    SIDES,
    STATE_DIR,
    TERMINAL_TASK_STATUSES,
    FileCache,
    MonitorError,
    ProcessTable,
    atomic_write_json,
    auto_refill_interval_seconds,
    clamp_int,
    deep_merge,
    read_json,
    render_auto_trigger_prompt,
    utc_now,
)
from .folders import FolderProvider
from .guard import GUARD_MODES, TaskGuard, guard_settings
from .housekeeping import Housekeeper
from .health import LoopHealth, Watchdog
from .logs import SchedulerLog
from .platform import PlatformProvider, SubmissionProvider
from .scheduler import SKILL_ABSOLUTE_MAX_CONTAINERS, JobManager, QueueManager, ReconcileLoop, prune_legacy_automation
from .tasks import DockerCache, TaskScanner, TraceCache, read_trace_events

HEARTBEAT_SECONDS = 10.0
# pauseOnStart=crash-loop: this many unclean exits inside the window pause the queue.
CRASH_LOOP_STARTS = 2
CRASH_LOOP_WINDOW_SECONDS = 15 * 60
DEFAULT_TICK_SECONDS = 1.5
SSE_PING_SECONDS = 5.0
VOLATILE_KEYS = {"updatedAt", "generatedAt", "cooldownRemainingSeconds", "fetchedAt", "lastBuildMs", "checkedAt"}


def _strip_volatile(value: Any) -> Any:
    """Drop timestamps that advance on every build so change detection is real."""
    if isinstance(value, dict):
        return {key: _strip_volatile(item) for key, item in value.items() if key not in VOLATILE_KEYS}
    if isinstance(value, list):
        return [_strip_volatile(item) for item in value]
    return value


class SnapshotHub:
    """Builds snapshots on a timer and fans deltas out to SSE subscribers."""

    def __init__(self, service: "SchedulerService", *, interval: float = DEFAULT_TICK_SECONDS):
        self.service = service
        self.interval = max(0.5, float(interval))
        self._lock = threading.Lock()
        self._subscribers: dict[int, deque] = {}
        self._next_id = 1
        self._last_cards: dict[str, str] = {}
        self._last_meta: str = ""
        self._full: dict[str, Any] | None = None
        self._revision = 0
        self._last_push_at = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_build_ms = 0.0
        self.last_error = ""

    # -- lifecycle -------------------------------------------------------- #
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="snapshot-hub", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        health = getattr(self.service, "health", None)
        while not self._stop.wait(self.interval):
            if health is not None:
                health.begin("snapshot-hub")
            error = ""
            try:
                self.build_once()
            except Exception as exc:  # pragma: no cover - defensive
                error = str(exc)
                self.last_error = error
                if self.service.log is not None:
                    try:
                        self.service.log.emit("snapshot.failed", level="error", detail=error)
                    except Exception:
                        pass
            finally:
                if health is not None:
                    health.end("snapshot-hub", error)

    # -- building --------------------------------------------------------- #
    def build_once(self, *, force: bool = False) -> dict[str, Any] | None:
        started = time.monotonic()
        payload = self.service.build_snapshot()
        self.last_build_ms = round((time.monotonic() - started) * 1000, 1)
        self.last_error = ""
        cards = payload.get("tasks") or []
        serialized = {str(card.get("id") or ""): json.dumps(card, ensure_ascii=False, sort_keys=True) for card in cards}
        # Timestamps advance on every build, so comparing them raw would make the
        # hub believe something changed on every tick and push an empty delta
        # 1.5 s forever.  Compare the payload with the volatile scalars removed.
        meta = json.dumps(
            {key: _strip_volatile(payload.get(key)) for key in ("summary", "queue", "containers")},
            ensure_ascii=False,
            sort_keys=True,
        )
        with self._lock:
            self._full = payload
            self._revision += 1
            changed = [
                json.loads(text)
                for key, text in serialized.items()
                if self._last_cards.get(key) != text
            ]
            removed = [key for key in self._last_cards if key not in serialized]
            self._last_cards = serialized
            meta_changed = force or self._last_meta != meta
            self._last_meta = meta
            self._revision_value = self._revision
        if changed or removed or meta_changed or force:
            self._push({
                "type": "tasks",
                "revision": self._revision_value,
                "changed": changed,
                "removed": removed,
                "summary": payload.get("summary") or {},
                "queue": payload.get("queue") or {},
                "containers": payload.get("containers") or {},
                "generatedAt": payload.get("generatedAt") or utc_now(),
            })
        elif time.monotonic() - self._last_push_at >= HEARTBEAT_SECONDS:
            self._push({
                "type": "heartbeat",
                "revision": self._revision_value,
                "generatedAt": payload.get("generatedAt") or utc_now(),
                "buildMs": self.last_build_ms,
            })
        return payload

    def _push(self, message: dict[str, Any]) -> None:
        self._last_push_at = time.monotonic()
        with self._lock:
            queues = list(self._subscribers.values())
        for queue in queues:
            try:
                queue.append(message)
            except Exception:
                continue

    # -- subscriptions ---------------------------------------------------- #
    def subscribe(self, maxsize: int = 200) -> tuple[int, deque]:
        queue: deque = deque(maxlen=max(1, int(maxsize)))
        with self._lock:
            key = self._next_id
            self._next_id += 1
            self._subscribers[key] = queue
        return key, queue

    def unsubscribe(self, key: int) -> None:
        with self._lock:
            self._subscribers.pop(key, None)

    def full(self) -> dict[str, Any]:
        with self._lock:
            if self._full is not None:
                return copy.deepcopy(self._full)
        return self.service.build_snapshot()

    def revision(self) -> int:
        with self._lock:
            return self._revision_value if hasattr(self, "_revision_value") else 0

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "subscribers": len(self._subscribers),
                "revision": getattr(self, "_revision_value", 0),
                "intervalSeconds": self.interval,
                "lastBuildMs": self.last_build_ms,
                "trackedTasks": len(self._last_cards),
                "lastError": self.last_error,
            }


class SchedulerService:
    """Owns every subsystem and answers the questions the HTTP layer asks."""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        removed = prune_legacy_automation(config.setdefault("automation", {}))
        automation = config["automation"]
        monitor_cfg = config.get("monitor") or {}
        self.file_cache = FileCache()
        self.process_table = ProcessTable(float(monitor_cfg.get("dockerCacheSeconds") or 2.0))
        self.trace_cache = TraceCache(
            int(monitor_cfg.get("traceMaxBytes") or 16 * 1024 * 1024),
            lightweight=not bool(monitor_cfg.get("parseTraceOnSnapshot", False)),
        )
        self.docker_cache = DockerCache(float(monitor_cfg.get("dockerCacheSeconds") or 2.0))
        # ``_stateDir`` redirects every piece of persisted state (tests set it).
        # Without this the suite wrote into the live queue, log and slot ledger.
        custom_state = str(config.get("_stateDir") or "").strip()
        self.state_dir = Path(custom_state) if custom_state else STATE_DIR
        self._dismissed_path = self.state_dir / DISMISSED_TASKS_PATH.name
        self._auto_path = self.state_dir / AUTO_STATE_PATH.name
        self._starts_path = self.state_dir / "starts.json"
        self._pause_reason = self._decide_pause_on_start()
        self._paused_on_start = bool(self._pause_reason)
        if self._paused_on_start:
            automation["paused"] = True
        self.log = SchedulerLog(self.state_dir / SCHEDULER_LOG_PATH.name)
        self.settings = None  # set by server once STATE_DIR is known
        self.folders = FolderProvider()
        self.platform = PlatformProvider(config)
        self.submissions_provider = SubmissionProvider(config)
        self.jobs = JobManager(config, process_table=self.process_table, state_dir=self.state_dir / "jobs")
        self.queue = QueueManager(
            config,
            self.jobs,
            docker_cache=self.docker_cache,
            file_cache=self.file_cache,
            process_table=self.process_table,
            log=self.log,
            state_path=self.state_dir / QUEUE_STATE_PATH.name,
            slot_root=(self.state_dir / "container-slots") if custom_state else None,
            platform=self.platform,
        )
        self.scanner = TaskScanner(
            config,
            trace_cache=self.trace_cache,
            docker_cache=self.docker_cache,
            file_cache=self.file_cache,
            process_table=self.process_table,
        )
        self.hub = SnapshotHub(self, interval=float(monitor_cfg.get("snapshotIntervalSeconds") or DEFAULT_TICK_SECONDS))
        self._lock = threading.RLock()
        self._auto_lock = threading.RLock()
        self._dismissed = self._load_dismissed_tasks()
        self._auto = self._load_auto()
        self._queue_refill_lock = threading.RLock()
        self._last_queue_refill_at = 0.0
        self._refill_rng = random.SystemRandom()
        self._last_queue_refill = dict(self.queue.refill_status)
        self.reconcile = ReconcileLoop(
            self.queue,
            self.jobs,
            log=self.log,
            platform=self.platform,
            interval_seconds=clamp_int(
                (config.get("automation") or {}).get("reconcileSeconds"), 15, 3600, 60
            ),
        )
        self.guard = TaskGuard(self.queue, log=self.log, platform=self.platform)
        self.reconcile.guard = self.guard
        self.housekeeper = Housekeeper(self.queue, log=self.log)
        self.reconcile.housekeeper = self.housekeeper
        self._stop = threading.Event()
        self._loops: list[threading.Thread] = []
        self._auto_thread: threading.Thread | None = None
        self._auto_interval = 3.0
        self.health = LoopHealth()
        self.reconcile.health = self.health
        self.watchdog = Watchdog(
            self.health,
            emit=self.log.emit,
            interval=float(((config.get("monitor") or {}).get("watchdogSeconds")) or 30),
        )
        self.started_at = time.time()
        self._snapshot_builds = 0
        self.log.emit("service.started", detail=f"pid={os.getpid()} 扫描目录={', '.join(self.queue.active_roots())}")
        if removed:
            self.log.emit("config.migrated", detail=f"移除已废弃配置项：{', '.join(removed)}")
        if self._paused_on_start:
            # Persist so config.json and the page agree on what the queue is doing.
            self._persist_config()
            self.log.emit("config.paused_on_start", level="warning" if "异常退出" in self._pause_reason else "info",
                          detail=f"{self._pause_reason}，需在队列页手动「启动队列」")
        elif not bool(automation.get("paused", True)):
            self.log.emit("config.resumed_on_start", detail="沿用上次状态：队列调度中（启动保护期后开始启动任务）")

    # -- process starts ------------------------------------------------- #
    def _decide_pause_on_start(self) -> str:
        """Why this start pauses a running queue, or ``""`` to resume it.

        Each start is recorded in ``starts.json``; :meth:`stop` marks it clean.
        A start whose predecessor never marked itself clean follows a crash, a
        kill or a hang that the keepalive had to end.  ``crash-loop`` pauses
        only when that has happened ``CRASH_LOOP_STARTS`` times inside
        ``CRASH_LOOP_WINDOW_SECONDS``: one crash-restart at night should carry
        on, a service that keeps dying should stop launching work.
        """
        automation = self.config["automation"]
        raw = automation.get("pauseOnStart", "crash-loop")
        mode = {True: "always", False: "never"}.get(raw, str(raw or "crash-loop"))
        now = time.time()
        data = read_json(self._starts_path, {})
        starts = [entry for entry in (data.get("starts") if isinstance(data, dict) else None) or []
                  if isinstance(entry, dict) and now - float(entry.get("at") or 0) <= 24 * 3600]
        crashes = sum(1 for entry in starts
                      if not entry.get("clean") and now - float(entry.get("at") or 0) <= CRASH_LOOP_WINDOW_SECONDS)
        starts.append({"at": now, "pid": os.getpid(), "clean": False})
        try:
            atomic_write_json(self._starts_path, {"starts": starts[-50:]})
        except OSError:
            pass
        if bool(automation.get("paused", True)) or mode == "never":
            return ""
        if mode == "always":
            return "启动时默认暂停队列（automation.pauseOnStart=always）"
        if crashes >= CRASH_LOOP_STARTS:
            minutes = CRASH_LOOP_WINDOW_SECONDS // 60
            return f"{minutes} 分钟内服务异常退出 {crashes} 次，疑似崩溃循环，已暂停队列"
        return ""

    def _mark_clean_stop(self) -> None:
        data = read_json(self._starts_path, {})
        starts = data.get("starts") if isinstance(data, dict) else None
        if not isinstance(starts, list):
            return
        for entry in reversed(starts):
            if isinstance(entry, dict) and entry.get("pid") == os.getpid():
                entry["clean"] = True
                break
        try:
            atomic_write_json(self._starts_path, {"starts": starts})
        except OSError:
            pass

    # -- persisted small state ------------------------------------------- #
    def _load_dismissed_tasks(self) -> dict[str, dict[str, Any]]:
        raw = read_json(self._dismissed_path, {})
        items = raw.get("items") if isinstance(raw, dict) else raw
        if isinstance(items, list):
            return {str(task_id): {"dismissedAt": ""} for task_id in items if str(task_id)}
        if not isinstance(items, dict):
            return {}
        return {
            str(task_id): copy.deepcopy(value) if isinstance(value, dict) else {}
            for task_id, value in items.items()
            if str(task_id)
        }

    def _save_dismissed_tasks_locked(self) -> None:
        atomic_write_json(self._dismissed_path, {"items": copy.deepcopy(self._dismissed), "updatedAt": utc_now()})

    def _load_auto(self) -> dict[str, Any]:
        raw = read_json(self._auto_path, {})
        if not isinstance(raw, dict):
            raw = {}
        return deep_merge(
            {
                "globalEnabled": bool(((self.config.get("monitor") or {}).get("autoResume") or {}).get("enabled")),
                "tasks": {},
                "history": {},
                "lastRunAt": {},
                "log": [],
            },
            raw,
        )

    def _save_auto(self) -> None:
        atomic_write_json(self._auto_path, self._auto)

    def _append_auto_log(self, level: str, message: str) -> None:
        log = self._auto.setdefault("log", [])
        log.append({"at": utc_now(), "level": level, "message": message})
        del log[:-120]

    # -- lifecycle -------------------------------------------------------- #
    def start(self) -> None:
        # server.py injects the settings store after construction, so anything
        # that depends on saved settings has to happen here.
        self._sync_blocklist()
        # Read the world once before any scheduling decision can be made.
        try:
            self.build_snapshot()
        except Exception as exc:
            self.log.emit("service.state_load_failed", level="error", detail=str(exc))
        grace = self.queue.startup_grace_seconds()
        self.log.emit(
            "service.startup_guard",
            detail=f"启动保护期 {grace} 秒，期间不启动新任务（读取状态后开始倒计时）",
        )
        orphans = getattr(self.jobs, "_orphans_reaped", []) or []
        if orphans:
            self.log.emit(
                "jobs.orphans_reaped",
                level="warning",
                detail=f"终止上一实例遗留的队列执行器 {len(orphans)} 个",
                count=len(orphans),
            )
        saved_folder = str((self.settings.get() if self.settings else {}).get("defaultFolderId") or "")
        if saved_folder:
            try:
                self._apply_folder_scope(saved_folder)
                self.log.emit("settings.folder", detail=f"采用已保存的默认文件夹 {saved_folder}")
            except MonitorError as exc:
                self.log.emit("settings.folder_failed", level="warning", detail=str(exc))
        auto_interval = float(((self.config.get("monitor") or {}).get("autoResume") or {}).get("tickSeconds") or 15)
        queue_interval = float((self.config.get("automation") or {}).get("tickSeconds") or 3)
        self._auto_interval = max(1.0, min(auto_interval, queue_interval))
        self.health.register("snapshot-hub", self.hub.interval)
        self.health.register("reconcile", self.reconcile.interval_seconds,
                             stall_after=max(600.0, self.reconcile.interval_seconds * 5.0))
        self.health.register("auto-loop", self._auto_interval)
        self.hub.start()
        self.reconcile.start()
        self._start_auto_loop()
        self.watchdog.supervise("snapshot-hub", lambda: self.hub._thread, self.hub.start)
        self.watchdog.supervise("reconcile", lambda: self.reconcile._thread, self.reconcile.start)
        self.watchdog.supervise("auto-loop", lambda: self._auto_thread, self._start_auto_loop)
        self.watchdog.start()

    def _start_auto_loop(self) -> threading.Thread:
        thread = threading.Thread(target=self._auto_loop, args=(self._auto_interval,), name="auto-loop", daemon=True)
        thread.start()
        self._auto_thread = thread
        self._loops = [item for item in self._loops if item.is_alive()] + [thread]
        return thread

    def stop(self) -> None:
        self._mark_clean_stop()
        self._stop.set()
        self.watchdog.stop()
        self.hub.stop()
        self.reconcile.stop()
        self.log.close()
        self.log.emit("service.stopped", detail="监控台退出")

    def _auto_loop(self, interval: float) -> None:
        while not self._stop.wait(interval):
            self.health.begin("auto-loop")
            errors: list[str] = []
            try:
                self._auto_iteration(errors)
            finally:
                self.health.end("auto-loop", "; ".join(errors))

    def _auto_iteration(self, errors: list[str]) -> None:
        for label, fn in (
            ("stop-tasks", self.enforce_stop_tasks),
            ("queue-refill", self.maybe_refill_queue),
            ("auto-resume", self.maybe_auto_resume),
            ("queue-tick", lambda: self.queue.tick(platform=self.platform)),
        ):
            try:
                result = fn()
            except Exception as exc:
                errors.append(f"{label}: {exc}")
                self.log.emit(f"loop.{label}.failed", level="error", detail=str(exc))
                continue
            if isinstance(result, list) and result:
                for action in result:
                    self.log.emit(
                        f"loop.{label}",
                        detail=_describe_action(action),
                        taskId=str((action.get("item") or action.get("task") or {}).get("id") or ""),
                    )

    # -- snapshot --------------------------------------------------------- #
    def _active_roots(self) -> list[Path]:
        return [Path(value) for value in self.queue.active_roots()]

    def build_snapshot(self) -> dict[str, Any]:
        now = time.time()
        self._snapshot_builds += 1
        if self._snapshot_builds == 1:
            # First full read of the task tree, docker and the queue's own state.
            self.queue.state_loaded = True
            self.log.emit(
                "service.state_loaded",
                detail=(
                    f"已读取 {len(self.queue.active_roots())} 个目录，"
                    f"启动保护期剩余 {max(0, self.queue.startup_grace_seconds() - int(time.time() - self.queue.started_at))} 秒"
                ),
            )
        roots = [Path(value).expanduser().resolve() for value in self.config.get("roots") or [DEFAULT_ROOT]]
        active_roots = self._active_roots()
        with self._lock:
            dismissed = set(self._dismissed)
        cards = self.scanner.cards(active_roots, dismissed=dismissed, now=now)
        queue_snapshot = self.queue.snapshot()
        containers = self.queue.container_usage()
        settings = self.settings.get() if self.settings is not None else {}
        payload = {
            "generatedAt": utc_now(),
            "roots": [str(item) for item in roots],
            "activeRoots": [str(item) for item in active_roots],
            "tasks": cards,
            "summary": self.scanner.summary(cards),
            "queue": queue_snapshot,
            "containers": containers,
            "settings": _public_settings(settings),
            "folders": self.folders.list().get("items") or [],
            "folderError": str(self.folders.list().get("error") or ""),
            "selectedFolderId": str(settings.get("defaultFolderId") or ""),
            "selectedFolderPath": str(settings.get("defaultFolderPath") or ""),
            "disabledProjects": self.disabled_projects(),
            "scheduleMode": queue_snapshot.get("scheduleMode"),
            "paused": bool(queue_snapshot.get("paused", True)),
            "config": {
                "pollSeconds": int((self.config.get("monitor") or {}).get("pollSeconds") or 3),
                "staleSeconds": int(((self.config.get("monitor") or {}).get("autoResume") or {}).get("staleSeconds") or 420),
            },
        }
        return payload

    def snapshot(self) -> dict[str, Any]:
        return self.hub.full()

    def submissions(self, force: bool = False) -> dict[str, Any]:
        return self.submissions_provider.get(force=force)

    # -- task access ------------------------------------------------------ #
    def task_detail(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            dismissed = set(self._dismissed)
        return self.scanner.detail(task_id, self._active_roots(), dismissed=dismissed)

    def dismiss_task(self, task_id: str) -> dict[str, Any]:
        task_id = str(task_id or "").strip()
        if not task_id:
            raise MonitorError("缺少任务 ID")
        task = self.task_detail(task_id)
        if not task:
            raise MonitorError("任务不存在或已放弃监控")
        record = {
            "taskId": task_id,
            "taskRoot": str(task.get("taskRoot") or ""),
            "name": str(task.get("name") or ""),
            "dismissedAt": utc_now(),
        }
        with self._lock:
            self._dismissed[task_id] = record
            self._save_dismissed_tasks_locked()
        self.log.emit("task.dismissed", taskId=task_id, projectCode=str(task.get("projectCode") or ""),
                      detail=f"放弃监控 {task.get('name')}")
        return record

    def restore_task(self, task_id: str) -> bool:
        with self._lock:
            existed = self._dismissed.pop(str(task_id or ""), None) is not None
            if existed:
                self._save_dismissed_tasks_locked()
        return existed

    def task_log(self, task_id: str, side: str, lines: int = 200) -> dict[str, Any]:
        path = self.scanner.stdout_path(task_id, side, self._active_roots())
        if path is None:
            return {"lines": [], "path": "", "taskId": task_id, "side": side}
        return {
            "taskId": task_id,
            "side": side,
            "path": str(path),
            "lines": self.scanner.format_stdout_lines(path, lines),
            "generatedAt": utc_now(),
        }

    def task_events(self, task_id: str, side: str, limit: int = 200) -> dict[str, Any]:
        """Structured trace events for one candidate or side.

        This is what the log tab renders visually — tool calls, assistant text,
        tool results, errors — rather than the flattened text lines from
        :meth:`task_log`.
        """
        path = self.scanner.stdout_path(task_id, side, self._active_roots())
        if path is None:
            return {"taskId": task_id, "side": str(side or ""), "path": "", "events": [], "generatedAt": utc_now()}
        events = read_trace_events(path, limit=limit)
        return {
            "taskId": task_id,
            "side": str(side or ""),
            "path": str(path),
            "events": events,
            "generatedAt": utc_now(),
        }

    def task_history(self, task_id: str, side: str, event_limit: int = 400) -> dict[str, Any]:
        task = self.task_detail(task_id)
        if not task:
            raise MonitorError("任务不存在")
        key = str(side or "").strip()
        if key.upper() in SIDES:
            record = (task.get("sides") or {}).get(key.upper()) or {}
        else:
            record = next(
                (item for item in (task.get("candidates") or []) if str(item.get("candidateId") or "").lower() == key.lower()),
                {},
            )
        return {
            "taskId": task_id,
            "side": key,
            "attempts": record.get("history") or [],
            "events": record.get("events") or [],
            "generatedAt": utc_now(),
        }

    # -- actions ---------------------------------------------------------- #
    def start_action(self, task_id: str, side: str, mode: str = "resume") -> dict[str, Any]:
        """Resume or rerun one side of a task.

        There is deliberately no "run both sides at once" action: launching A and
        B together makes it impossible to tell which side's failure caused a
        shared-container or key-exhaustion problem, and the candidate race
        already runs both sides when a task starts from the queue.
        """
        # Validate the request before touching the task tree so a malformed call
        # gets a precise error rather than "task not found".
        side = str(side).upper()
        if side not in SIDES:
            raise MonitorError("side 只能为 A 或 B")
        if mode not in {"resume", "rerun"}:
            raise MonitorError("mode 只能为 resume 或 rerun")
        task = self.task_detail(task_id)
        if not task:
            raise MonitorError("任务不存在或已移出扫描目录")
        force = mode == "rerun"
        current = (task.get("sides") or {}).get(side) or {}
        if current.get("active") and not force:
            raise MonitorError(f"{side} 正在运行，不能重复启动")
        job = self.jobs.start(Path(task["taskRoot"]), side, force=force, reason=mode)
        self.log.emit("task.started", taskId=task_id, projectCode=str(task.get("projectCode") or ""),
                      detail=f"{side}/{mode} PID={job.get('pid')}")
        return job

    # -- automation actions ----------------------------------------------- #
    def automation_action(self, action: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        action = str(action or "").strip()
        if action == "set-capacity":
            value = int(payload.get("capacity") or 0)
            if value < 1 or value > 20:
                raise MonitorError("并发数必须在 1 到 20 之间")
            self.config.setdefault("automation", {})["capacity"] = value
            self._persist_config()
            self.log.emit("config.capacity", detail=f"并发任务数 → {value}")
            return self.queue.fast_snapshot()
        if action == "set-schedule-mode":
            mode = str(payload.get("mode") or "")
            if mode not in {"tasks", "containers"}:
                raise MonitorError("调度模式只能是 tasks 或 containers")
            self.config.setdefault("automation", {})["scheduleMode"] = mode
            self._persist_config()
            self.log.emit("config.schedule_mode", detail=f"调度模式 → {mode}")
            return self.queue.fast_snapshot()
        if action == "set-limits":
            return self._set_limits(payload)
        if action == "set-cooldown":
            value = int(payload.get("cooldownSeconds") or 0)
            if value < 0 or value > 86400:
                raise MonitorError("任务创建间隔必须在 0 到 86400 秒之间")
            self.config.setdefault("automation", {})["cooldownSeconds"] = value
            self._persist_config()
            self.log.emit("config.cooldown", detail=f"冷却 → {value}s")
            return self.queue.fast_snapshot()
        if action == "set-prompt-template":
            template = str(payload.get("template") or "")
            if not template.strip():
                raise MonitorError("Prompt 模板不能为空")
            if len(template) > 200000:
                raise MonitorError("Prompt 模板过长")
            self.config.setdefault("automation", {})["promptTemplate"] = template
            self._sync_pending_prompts(template)
            self._persist_config()
            self.log.emit("config.prompt_template", detail=f"模板已更新（{len(template)} 字）")
            return self.queue.fast_snapshot()
        if action == "set-paused":
            paused = bool(payload.get("paused"))
            self.config.setdefault("automation", {})["paused"] = paused
            self._persist_config()
            self.log.emit("config.paused", detail=f"队列{'已暂停' if paused else '已启动'}")
            return self.queue.fast_snapshot()
        if action == "set-merge-project-pool":
            enabled = bool(payload.get("enabled"))
            self.config.setdefault("platform", {})["mergeProjectPool"] = enabled
            self._persist_config()
            self.log.emit("config.project_pool", detail=f"包含项目池 → {enabled}")
            return self.queue.fast_snapshot()
        if action == "set-guard":
            return self._set_guard(payload)
        if action == "set-auto-refill":
            return self._set_auto_refill(bool(payload.get("enabled")))
        if action == "set-auto-refill-weights":
            weights = payload.get("weights")
            if not isinstance(weights, dict):
                raise MonitorError("weights 必须是对象")
            return self._set_refill_weights(weights)
        if action == "set-roots":
            # Roots now follow the selected folder; this action only exists for
            # the config-file path and direct API callers.
            roots = payload.get("roots")
            if not isinstance(roots, list):
                raise MonitorError("roots 必须是数组")
            return self._set_roots([str(item) for item in roots])
        if action == "queue-add":
            task = self.task_detail(str(payload.get("taskId") or ""))
            if not task:
                raise MonitorError("任务不存在")
            side = str(payload.get("side") or "both")
            self.queue.add(Path(task["taskRoot"]), side)
            self.log.emit("queue.added", taskId=str(task.get("id") or ""), detail=f"本地任务入队 side={side}")
            return self.queue.fast_snapshot()
        if action == "queue-add-platform":
            project = payload.get("project")
            if not isinstance(project, dict):
                raise MonitorError("缺少平台项目数据")
            settings = self.settings.get() if self.settings is not None else {}
            folder_id = str(payload.get("folderId") or settings.get("defaultFolderId") or "")
            folder = self.folders.get(folder_id) if folder_id else None
            folder_path = ""
            if folder:
                folder_path = self.folders.resolve_workdir(folder_id)
            template = str(self.config.get("automation", {}).get("promptTemplate") or "")
            prompt = render_auto_trigger_prompt(
                template,
                project,
                task_type=str(payload.get("taskType") or "0-1代码生成"),
                difficulty=str(payload.get("difficulty") or "困难"),
                base_url=str(self.config.get("automation", {}).get("anthropicBaseUrl") or ""),
                max_tasks=int(self.config.get("automation", {}).get("capacity") or 2),
                max_containers=self.queue._max_containers_limit(),
                candidates_per_task=self.queue._candidates_per_task(),
                schedule_mode=self.queue._schedule_mode(),
            )
            item = self.queue.add_platform(
                project,
                task_type=str(payload.get("taskType") or "0-1代码生成"),
                difficulty=str(payload.get("difficulty") or "困难"),
                side=str(payload.get("side") or "both"),
                trigger_prompt=prompt,
                folder_id=folder_id,
                folder_path=folder_path,
            )
            self.log.emit("queue.added_platform", taskId=str(item.get("id") or ""),
                          projectCode=str(project.get("code") or ""),
                          detail=f"平台项目入队 folder={folder_id or '未选择'}")
            return self.queue.fast_snapshot()
        if action == "queue-remove":
            self.queue.remove(str(payload.get("itemId") or ""))
            return self.queue.fast_snapshot()
        if action == "queue-disable":
            project = payload.get("project") if isinstance(payload.get("project"), dict) else {}
            code = str(payload.get("projectCode") or project.get("code") or "").strip()
            result = self.disable_project(code, item_id=str(payload.get("itemId") or ""))
            return result["queue"]
        if action == "project-enable":
            return self.enable_project(str(payload.get("projectCode") or ""))["disabledProjects"]
        if action == "project-disabled":
            return {"disabledProjects": self.disabled_projects()}
        if action == "queue-move":
            self.queue.move(str(payload.get("itemId") or ""), int(payload.get("delta") or 0))
            return self.queue.fast_snapshot()
        if action == "queue-retry":
            self.queue.retry(str(payload.get("itemId") or ""))
            self.log.emit("queue.retry", taskId=str(payload.get("itemId") or ""), detail="手动重试")
            return self.queue.fast_snapshot()
        if action == "queue-release":
            item_id = str(payload.get("itemId") or "")
            self.queue.release(item_id)
            self.log.emit("queue.released", taskId=item_id, detail="手动释放并发名额")
            return self.queue.fast_snapshot()
        if action == "queue-clear":
            self.queue.clear_finished()
            return self.queue.fast_snapshot()
        raise MonitorError(f"未知 automation action: {action}")

    def _persist_config(self) -> None:
        from .common import CONFIG_PATH, save_config

        save_config(self.config, Path(str(self.config.get("_configPath") or CONFIG_PATH)))

    def _set_limits(self, payload: dict[str, Any]) -> dict[str, Any]:
        """The only knobs an operator needs; everything else has safe defaults.

        ``maxContainers`` is the single container limit: it is also written to
        the skill's ``container-limit.json`` so the executor enforces the same
        number.  Legacy keys (refill threshold, reserve/startup windows, …) are
        accepted from old clients and ignored.
        """
        automation = self.config.setdefault("automation", {})
        changes: list[str] = []
        if "maxTasks" in payload:
            value = int(payload.get("maxTasks") or 0)
            if value < 1 or value > 20:
                raise MonitorError("最大任务数必须在 1 到 20 之间")
            automation["capacity"] = value
            changes.append(f"maxTasks={value}")
        if "maxContainers" in payload:
            value = int(payload.get("maxContainers") or 0)
            if value < 1 or value > SKILL_ABSOLUTE_MAX_CONTAINERS:
                raise MonitorError(f"最大容器数必须在 1 到 {SKILL_ABSOLUTE_MAX_CONTAINERS} 之间（技能侧硬顶）")
            automation["maxContainers"] = value
            changes.append(f"maxContainers={value}")
        if "candidatesPerTask" in payload:
            value = int(payload.get("candidatesPerTask") or 0)
            if value < 2 or value > 8:
                raise MonitorError("单任务候选数必须在 2 到 8 之间")
            automation["candidatesPerTask"] = value
            changes.append(f"candidatesPerTask={value}")
        if "cooldownSeconds" in payload:
            value = int(payload.get("cooldownSeconds") or 0)
            if value < 0 or value > 86400:
                raise MonitorError("启动间隔必须在 0 到 86400 秒之间")
            automation["cooldownSeconds"] = value
            changes.append(f"cooldownSeconds={value}")
        if not changes:
            raise MonitorError("没有需要更新的上限参数")
        prune_legacy_automation(automation)
        self._persist_config()
        self.queue.sync_skill_limits()
        self.log.emit("config.limits", detail="，".join(changes))
        return self.queue.fast_snapshot()

    def _sync_pending_prompts(self, template: str) -> None:
        changed = False
        for item in self.queue._items:
            if item.get("source") != "platform" or item.get("status") == "running":
                continue
            project = {
                "code": str(item.get("projectCode") or ""),
                "name": str(item.get("projectName") or item.get("taskName") or item.get("projectCode") or ""),
            }
            prompt = render_auto_trigger_prompt(
                template,
                project,
                task_type=str(item.get("taskType") or "0-1代码生成"),
                difficulty=str(item.get("difficulty") or "困难"),
                base_url=str(self.config.get("automation", {}).get("anthropicBaseUrl") or ""),
                max_tasks=int(self.config.get("automation", {}).get("capacity") or 2),
                max_containers=self.queue._max_containers_limit(),
                candidates_per_task=self.queue._candidates_per_task(),
                schedule_mode=self.queue._schedule_mode(),
            )
            if item.get("triggerPrompt") != prompt:
                item["triggerPrompt"] = prompt
                changed = True
        if changed:
            self.queue._save()

    def _set_auto_refill(self, enabled: bool) -> dict[str, Any]:
        with self._queue_refill_lock:
            self.config.setdefault("automation", {}).setdefault("autoRefill", {})["enabled"] = enabled
            self._persist_config()
            # Turning it on refills on the next loop pass instead of waiting
            # out the interval; turning it off says so on the page right away.
            self._last_queue_refill_at = 0.0
            if not enabled:
                self._record_refill({"status": "disabled", "added": 0, "message": "已关闭，不再自动补充",
                                     "at": utc_now()})
        self.log.emit("config.auto_refill", detail=f"自动补队 → {'开启' if enabled else '关闭'}")
        return self.queue.fast_snapshot()

    def _set_guard(self, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = self.config.setdefault("automation", {}).setdefault("guard", {})
        changes: list[str] = []
        if "mode" in payload:
            mode = str(payload.get("mode") or "")
            if mode not in GUARD_MODES:
                raise MonitorError("兜底模式只能是 off、observe 或 enforce")
            cfg["mode"] = mode
            changes.append(f"mode={mode}")
        for key, low, high in (("candidatePhaseHours", 1, 48), ("noProgressMinutes", 15, 1440),
                               ("leakedProcessMinutes", 10, 1440)):
            if key not in payload:
                continue
            try:
                value = float(payload.get(key))
            except (TypeError, ValueError):
                raise MonitorError(f"{key} 必须是数字") from None
            if value < low or value > high:
                raise MonitorError(f"{key} 必须在 {low} 到 {high} 之间")
            cfg[key] = value if key == "candidatePhaseHours" else int(value)
            changes.append(f"{key}={cfg[key]:g}")
        if not changes:
            raise MonitorError("没有需要更新的兜底参数")
        self._persist_config()
        self.log.emit("config.guard", detail="，".join(changes))
        # Reflect the new settings right away; flags follow on the next round.
        self.queue.guard_status = {**self.queue.guard_status, **guard_settings(self.config)}
        return self.queue.fast_snapshot()

    def _record_refill(self, result: dict[str, Any]) -> None:
        self._last_queue_refill = result
        self.queue.refill_status = {key: result.get(key) for key in ("status", "added", "pending", "message", "at")
                                    if key in result}

    def _set_refill_weights(self, weights: dict[str, Any]) -> dict[str, Any]:
        with self.queue._lock:
            automation = self.config.setdefault("automation", {})
            cfg = automation.setdefault("autoRefill", {})
            task_types = [str(value).strip() for value in (cfg.get("taskTypes") or []) if str(value).strip()]
            if not task_types:
                raise MonitorError("自动补队未配置任务类型")
            normalized: dict[str, float] = {}
            for task_type in task_types:
                try:
                    value = float(weights.get(task_type) or 0)
                except (TypeError, ValueError):
                    raise MonitorError(f"{task_type} 的比例必须是数字") from None
                if value < 0 or value > 100:
                    raise MonitorError(f"{task_type} 的比例必须在 0 到 100 之间")
                normalized[task_type] = round(value, 2)
            total = sum(normalized.values())
            if abs(total - 100.0) > 0.01:
                raise MonitorError(f"任务类型比例合计必须为 100%，当前为 {total:g}%")
            cfg["taskTypeWeights"] = normalized
            target = max(1, int(cfg.get("targetPending") or 20))
            min_weight = min(normalized.values()) if normalized else 0.0
            cfg["minPendingPerTaskType"] = max(0, int(round(target * min_weight / 100.0)))
            self._persist_config()
        return self.queue.fast_snapshot()

    def _set_roots(self, roots: list[str]) -> dict[str, Any]:
        resolved: list[str] = []
        for raw in roots:
            path = Path(str(raw)).expanduser().resolve()
            if not path.is_dir():
                raise MonitorError(f"监控目录不存在: {path}")
            if str(path) not in resolved:
                resolved.append(str(path))
        if not resolved:
            raise MonitorError("至少保留一个监控目录")
        monitor_cfg = self.config.setdefault("monitor", {})
        if "activeRoots" not in monitor_cfg:
            monitor_cfg["activeRoots"] = self.queue._roots_locked()
        active = {str(Path(value).expanduser().resolve()) for value in monitor_cfg.get("activeRoots") or []}
        self.config["roots"] = resolved
        monitor_cfg["activeRoots"] = [root for root in resolved if root in active]
        self._persist_config()
        self.log.emit("config.roots", detail=f"监控目录 → {', '.join(resolved)}")
        return self.queue.fast_snapshot()

    def set_root_active(self, root: str, active: bool) -> dict[str, Any]:
        path = str(Path(str(root)).expanduser().resolve())
        monitor_cfg = self.config.setdefault("monitor", {})
        roots = self.queue._roots_locked()
        if path not in roots:
            raise MonitorError(f"监控目录不存在: {path}")
        active_roots = {
            str(Path(value).expanduser().resolve())
            for value in (monitor_cfg.get("activeRoots") if "activeRoots" in monitor_cfg else roots)
        }
        if active:
            active_roots.add(path)
        else:
            active_roots.discard(path)
        monitor_cfg["activeRoots"] = [value for value in roots if value in active_roots]
        self._persist_config()
        self.log.emit("config.root_active", detail=f"{path} → {'监控中' if active else '未监控'}")
        return self.queue.fast_snapshot()

    # -- settings --------------------------------------------------------- #
    def disabled_projects(self) -> list[dict[str, Any]]:
        if self.settings is None:
            return []
        raw = self.settings.get().get("disabledProjects") or []
        return [item for item in raw if isinstance(item, dict)]

    def blocked_codes(self) -> set[str]:
        return {str(item.get("code") or "").strip().casefold() for item in self.disabled_projects()
                if str(item.get("code") or "").strip()}

    def _sync_blocklist(self) -> None:
        codes = self.blocked_codes()
        self.platform.set_blocklist(codes)
        self.queue.blocked_codes = codes

    def disable_project(self, project_code: str, *, item_id: str = "", reason: str = "手动禁用") -> dict[str, Any]:
        """Remove a queue item and stop the project from ever coming back."""
        code = str(project_code or "").strip()
        if not code:
            raise MonitorError("缺少项目编号")
        if self.settings is None:
            raise MonitorError("设置存储未初始化")
        if item_id:
            self.queue.remove(item_id)
        else:
            with self.queue._lock:
                for item in list(self.queue._items):
                    if str(item.get("projectCode") or "").casefold() == code.casefold():
                        self.queue.remove(str(item.get("id") or ""))
                        break
        entries = self.disabled_projects()
        entries = [item for item in entries if str(item.get("code") or "").casefold() != code.casefold()]
        project_name = ""
        with self.queue._lock:
            for item in self.queue._triggered:
                if str(item.get("projectCode") or "").casefold() == code.casefold():
                    project_name = str(item.get("projectName") or item.get("taskName") or "")
                    break
        entries.append({"code": code, "name": project_name or code, "reason": reason, "at": utc_now()})
        self.settings.update({"disabledProjects": entries})
        self._sync_blocklist()
        self.log.emit("project.disabled", projectCode=code, detail=f"已禁用，不再自动补队（{reason}）")
        return {"disabledProjects": entries, "queue": self.queue.fast_snapshot()}

    def enable_project(self, project_code: str) -> dict[str, Any]:
        code = str(project_code or "").strip()
        if not code or self.settings is None:
            raise MonitorError("缺少项目编号")
        entries = [item for item in self.disabled_projects()
                   if str(item.get("code") or "").casefold() != code.casefold()]
        self.settings.update({"disabledProjects": entries})
        self._sync_blocklist()
        self.log.emit("project.enabled", projectCode=code, detail="已恢复，可重新入队")
        return {"disabledProjects": entries}

    def _apply_folder_scope(self, folder_id: str) -> str:
        """Point the effective scan roots and queue workdir at one folder."""
        folder = self.folders.get(folder_id)
        if folder is None:
            raise MonitorError(f"文件夹不存在或不在前 10 个之内: {folder_id}")
        roots: list[str] = []
        for raw in folder.get("roots") or []:
            path = Path(str(raw)).expanduser()
            if path.is_dir():
                resolved = str(path.resolve())
                if resolved not in roots:
                    roots.append(resolved)
        if not roots:
            raise MonitorError(f"文件夹 {folder.get('name')} 没有可用的工作目录")
        monitor_cfg = self.config.setdefault("monitor", {})
        monitor_cfg["activeRoots"] = roots
        # Keep the configured superset intact so clearing the folder restores it.
        known = self.queue._roots_locked()
        for root in roots:
            if root not in known:
                self.config.setdefault("roots", []).append(root)
        self._persist_config()
        return roots[0]

    def _clear_folder_scope(self) -> None:
        monitor_cfg = self.config.setdefault("monitor", {})
        monitor_cfg["activeRoots"] = self.queue._roots_locked()
        self._persist_config()

    def update_settings(self, patch: dict[str, Any]) -> dict[str, Any]:
        if self.settings is None:
            raise MonitorError("设置存储未初始化")
        merged = self.settings.update(patch)
        if "defaultFolderId" in patch:
            folder_id = str(merged.get("defaultFolderId") or "")
            if folder_id:
                workdir = self._apply_folder_scope(folder_id)
            else:
                workdir = ""
                self._clear_folder_scope()
            merged = self.settings.update({"defaultFolderPath": workdir})
            self.log.emit("settings.folder", detail=f"默认文件夹 → {folder_id or '未选择'}（{workdir or '全部目录'}）")
        if "disabledProjects" in patch:
            self._sync_blocklist()
        return _public_settings(merged)

    def save_manager_credentials(self, *, base_url: str, username: str, password: str) -> dict[str, Any]:
        """Persist the Solo Manager connection; the password goes to Keychain only."""
        from .common import keychain_write

        base = str(base_url or "").strip().rstrip("/")
        if not base:
            raise MonitorError("managerBaseUrl 不能为空")
        platform_cfg = self.config.setdefault("platform", {})
        platform_cfg["managerBaseUrl"] = base
        platform_cfg["username"] = str(username or "admin").strip() or "admin"
        password_saved = False
        if str(password or "").strip():
            keychain_write(str(platform_cfg.get("passwordKeychainService") or "solo-manager-password"), password.strip())
            password_saved = True
        self._persist_config()
        if self.settings is not None:
            self.settings.update({"managerPasswordSaved": password_saved})
        self.log.emit("settings.manager", detail=f"Solo Manager → {base}（密码{'已保存' if password_saved else '未改动'}）")
        status = self.platform.connection_status()
        return {
            "ok": True,
            "passwordSaved": password_saved,
            "connection": status,
            "settings": _public_settings(self.settings.get() if self.settings is not None else {}),
        }

    # -- background jobs -------------------------------------------------- #
    def enforce_stop_tasks(self) -> list[dict[str, Any]]:
        markers = self.queue.stop_markers()
        if not markers:
            return []
        actions: list[dict[str, Any]] = []
        try:
            proc = subprocess.run(
                ["ps", "-axo", "pid=,pgid=,command="],
                text=True,
                capture_output=True,
                check=False,
                timeout=10,
            )
        except (OSError, subprocess.SubprocessError):
            return []
        for line in proc.stdout.splitlines():
            parts = line.strip().split(None, 2)
            if len(parts) < 3:
                continue
            try:
                pid = int(parts[0])
                pgid = int(parts[1])
            except ValueError:
                continue
            command = parts[2]
            marker = next((value for value in markers if value in command), "")
            if not marker or pid == os.getpid():
                continue
            try:
                os.killpg(pgid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError, OSError):
                continue
            actions.append({"kind": "process", "marker": marker, "pid": pid, "pgid": pgid})

        roots = [Path(value).expanduser().resolve() for value in self.queue.active_roots()]
        for marker in markers:
            for root in roots:
                for task_root in list(root.glob(f"*{marker}*")):
                    if not task_root.is_dir():
                        continue
                    try:
                        _pb, claims = self.platform._modules()
                        claims.release_project_claim(task_root)
                    except Exception:
                        pass
                    try:
                        shutil.rmtree(task_root)
                    except OSError:
                        continue
                    actions.append({"kind": "task-root", "marker": marker, "path": str(task_root)})
        return actions

    def _task_auto_enabled(self, task_root: Path) -> bool:
        if not self._auto.get("globalEnabled"):
            return False
        record = (self._auto.get("tasks") or {}).get(str(task_root.resolve()), {})
        return bool(record.get("enabled"))

    def _auto_quota_ok(self, task_root: Path, side: str) -> tuple[bool, str]:
        auto_cfg = (self.config.get("monitor") or {}).get("autoResume") or {}
        cooldown = float(auto_cfg.get("cooldownSeconds") or 180)
        window = float(auto_cfg.get("windowSeconds") or 3600)
        limit = int(auto_cfg.get("maxRelaunchesPerSide") or 3)
        key = JobManager.key(task_root, side)
        now = time.time()
        last = float((self._auto.get("lastRunAt") or {}).get(key) or 0)
        if last and now - last < cooldown:
            return False, f"冷却中 {int(cooldown - (now - last))}s"
        history = [
            float(value) for value in ((self._auto.get("history") or {}).get(key) or [])
            if isinstance(value, (int, float)) and now - float(value) <= window
        ]
        if len(history) >= limit:
            return False, f"{int(window / 60)} 分钟内已达到 {limit} 次"
        return True, ""

    def maybe_refill_queue(self) -> dict[str, Any]:
        """Validate pending quota and refill from live eligible projects when low."""
        cfg = (self.config.get("automation") or {}).get("autoRefill") or {}
        if not bool(cfg.get("enabled")):
            return {"status": "disabled", "added": 0, "validated": 0, "failed": 0}
        queue_paused = bool((self.config.get("automation") or {}).get("paused", True))
        if not self.queue.active_roots():
            result = {"status": "no-active-roots", "added": 0, "validated": 0, "failed": 0,
                      "message": "未选择 Codex 任务目录，停止补队", "at": utc_now(), "cached": False}
            self._record_refill(result)
            return copy.deepcopy(result)
        interval = auto_refill_interval_seconds(cfg)
        now = time.time()
        with self._queue_refill_lock:
            if now - self._last_queue_refill_at < interval:
                cached = copy.deepcopy(self._last_queue_refill)
                cached["cached"] = True
                return cached
            self._last_queue_refill_at = now
            target = max(1, int(cfg.get("targetPending") or 20))
            target_per_type = max(0, int(cfg.get("minPendingPerTaskType") or 0))
            batch_size = max(1, int(cfg.get("batchSize") or target))
            randomize = bool(cfg.get("randomize", True))
            shuffle_existing = bool(cfg.get("shuffleExisting", True))
            task_types = [str(value).strip() for value in (cfg.get("taskTypes") or []) if str(value).strip()]
            if not task_types:
                task_types = [str((self.config.get("platform") or {}).get("taskType") or "0-1代码生成")]
            raw_weights = cfg.get("taskTypeWeights") or {}
            task_type_weights: dict[str, float] = {}
            for task_type in task_types:
                try:
                    weight = float((raw_weights or {}).get(task_type) or 0)
                except (TypeError, ValueError, AttributeError):
                    weight = 0
                task_type_weights[task_type] = max(0.0, weight)
            weight_total = sum(task_type_weights.values())
            weighted_selection = weight_total > 0
            difficulty = str(cfg.get("difficulty") or (self.config.get("platform") or {}).get("difficulty") or "困难")
            include_project_pool = bool((self.config.get("platform") or {}).get("mergeProjectPool", False))
            candidates_by_type: dict[str, dict[str, dict[str, Any]]] = {}
            reasons_by_type: dict[str, dict[str, str]] = {}
            errors: list[str] = []
            for task_type in task_types:
                try:
                    payload = self.platform.candidates(
                        task_type=task_type, force=True, include_pool=include_project_pool
                    )
                except Exception as exc:
                    errors.append(f"{task_type}: {exc}")
                    continue
                if payload.get("errors"):
                    errors.extend(f"{task_type}: {item}" for item in payload.get("errors") or [])
                candidates_by_type[task_type] = {
                    str(item.get("code") or "").casefold(): item
                    for item in (payload.get("items") or [])
                    if str(item.get("code") or "").strip()
                }
                reasons_by_type[task_type] = {
                    str(item.get("code") or "").casefold(): str(item.get("reason") or "平台配额复核未通过")
                    for item in (payload.get("excluded") or [])
                    if str(item.get("code") or "").strip()
                }

            queue_items = self.queue.snapshot().get("items") or []
            pending_by_type: dict[str, int] = {task_type: 0 for task_type in task_types}
            validated = 0
            failed = 0
            active_count = 0
            for item in queue_items:
                item_status = str(item.get("status") or "")
                if item_status in QUEUE_ACTIVE_STATUSES or item.get("capacityHeld"):
                    active_count += 1
                    task_type = str(item.get("taskType") or "")
                    if task_type in pending_by_type:
                        pending_by_type[task_type] += 1
                    continue
                if item_status != "pending":
                    continue
                task_type = str(item.get("taskType") or "")
                if task_type in pending_by_type:
                    pending_by_type[task_type] += 1
                if task_type not in candidates_by_type:
                    continue
                code = str(item.get("projectCode") or "").casefold()
                if code in candidates_by_type[task_type]:
                    validated += 1
                    continue
                reason = reasons_by_type.get(task_type, {}).get(code) or ""
                # Only fail on an explicit, deterministic exclusion; a missing item
                # can be pagination or transient filtering and must not discard work.
                # "已占用/运行中" is transient too — it is usually this very item's
                # previous run still finishing — so it waits instead of failing.
                if not reason or "已占用" in reason or "运行中" in reason:
                    continue
                if not any(marker in reason for marker in ("配额", "缺少可用源码", "不可选用")):
                    continue
                if self.queue.fail_item(str(item.get("id") or ""), f"启动前配额复核未通过：{reason}"):
                    failed += 1

            pending = self.queue.pending_count()
            shuffled = (
                self.queue.shuffle_pending(self._refill_rng, keep_head=AUTO_REFILL_PREVIEW_SIZE)
                if randomize and shuffle_existing else False
            )
            effective_target = max(0, target - active_count)
            tracked = self.queue.tracked_project_codes()
            added = 0
            while added < batch_size and pending < effective_target:
                needed_types = [
                    task_type for task_type in task_types
                    if pending < effective_target or pending_by_type.get(task_type, 0) < target_per_type
                ]
                if not needed_types:
                    break
                projected_total = sum(pending_by_type.get(value, 0) for value in task_types) + 1

                def selection_rank(task_type: str) -> tuple[float, float, float, float]:
                    count = pending_by_type.get(task_type, 0)
                    min_priority = 0.0 if count < target_per_type else 1.0
                    tie = self._refill_rng.random() if randomize else float(task_types.index(task_type))
                    if weighted_selection:
                        desired = projected_total * task_type_weights.get(task_type, 0.0) / weight_total
                        return min_priority, -(desired - count), -task_type_weights.get(task_type, 0.0), tie
                    return min_priority, float(count), 0.0, tie

                needed_types.sort(key=selection_rank)
                selected: tuple[str, str, dict[str, Any]] | None = None
                for task_type in needed_types:
                    choices = [
                        (code, project)
                        for code, project in candidates_by_type.get(task_type, {}).items()
                        if code not in tracked
                    ]
                    if randomize:
                        self._refill_rng.shuffle(choices)
                    if choices:
                        code, project = choices[0]
                        selected = (task_type, code, project)
                        break
                if not selected:
                    break
                task_type, code, project = selected
                prompt = render_auto_trigger_prompt(
                    str((self.config.get("automation") or {}).get("promptTemplate") or ""),
                    project,
                    task_type=task_type,
                    difficulty=difficulty,
                    base_url=str(self.config.get("automation", {}).get("anthropicBaseUrl") or ""),
                    max_tasks=int(self.config.get("automation", {}).get("capacity") or 2),
                    max_containers=self.queue._max_containers_limit(),
                    candidates_per_task=self.queue._candidates_per_task(),
                    schedule_mode=self.queue._schedule_mode(),
                )
                try:
                    self.queue.add_platform(
                        project,
                        task_type=task_type,
                        difficulty=difficulty,
                        side="both",
                        trigger_prompt=prompt,
                        folder_id=str((self.settings.get() if self.settings else {}).get("defaultFolderId") or ""),
                        folder_path=str((self.settings.get() if self.settings else {}).get("defaultFolderPath") or ""),
                    )
                except MonitorError:
                    tracked.add(code)
                    continue
                tracked.add(code)
                pending += 1
                pending_by_type[task_type] = pending_by_type.get(task_type, 0) + 1
                added += 1
            result = {
                "status": "ok" if not errors else "partial",
                "added": added,
                "validated": validated,
                "failed": failed,
                "pending": pending,
                "pendingByType": pending_by_type,
                "taskTypeWeights": task_type_weights if weighted_selection else {},
                "targetPending": target,
                "effectiveTargetPending": effective_target,
                "activeCount": active_count,
                "queuePaused": queue_paused,
                "randomized": randomize,
                "shuffledExisting": shuffled,
                "errors": errors[-3:],
                "at": utc_now(),
                "cached": False,
            }
            message = f"配额复核 {validated} 项，失效 {failed} 项，随机补队 {added} 项，当前待办 {pending}/{target}"
            if errors:
                message += f"，部分任务类型失败：{' | '.join(errors[-2:])}"
            result["message"] = message
            self._record_refill(result)
            return copy.deepcopy(result)

    def maybe_auto_resume(self) -> list[dict[str, Any]]:
        auto_cfg = (self.config.get("monitor") or {}).get("autoResume") or {}
        if not self._auto.get("globalEnabled"):
            return []
        with self._auto_lock:
            actions: list[dict[str, Any]] = []
            running_jobs = self.jobs.running()
            max_jobs = int(auto_cfg.get("maxConcurrentJobs") or 2)
            if len(running_jobs) >= max_jobs:
                return []
            snapshot = self.build_snapshot()
            for task in snapshot.get("tasks") or []:
                task_root = Path(task["taskRoot"])
                if not self._task_auto_enabled(task_root):
                    continue
                if task.get("status") in TERMINAL_TASK_STATUSES:
                    continue
                for side in SIDES:
                    side_data = (task.get("sides") or {}).get(side) or {}
                    if not side_data.get("everStarted") or not side_data.get("canResume") or side_data.get("active"):
                        continue
                    if not side_data.get("needsResume"):
                        continue
                    current_job = self.jobs.get(task_root, side)
                    if current_job and current_job.get("status") == "running":
                        continue
                    quota_ok, quota_reason = self._auto_quota_ok(task_root, side)
                    if not quota_ok:
                        continue
                    try:
                        job = self.jobs.start(task_root, side, force=False, reason=f"auto:{side_data.get('health')}")
                    except MonitorError as exc:
                        self._append_auto_log("warning", f"{task['name']} / {side} 自动续跑失败：{exc}")
                        continue
                    key = JobManager.key(task_root, side)
                    history = self._auto.setdefault("history", {}).setdefault(key, [])
                    history.append(time.time())
                    now = time.time()
                    self._auto.setdefault("lastRunAt", {})[key] = now
                    self._auto["history"][key] = [
                        value for value in history
                        if isinstance(value, (int, float)) and now - float(value) <= 86400
                    ]
                    message = f"{task['name']} / {side} 自动续跑，原因={side_data.get('health')}，PID={job.get('pid')}"
                    self._append_auto_log("info", message)
                    actions.append({"task": task["name"], "side": side, "job": job, "message": message})
                    running_jobs = self.jobs.running()
                    if len(running_jobs) >= max_jobs:
                        self._save_auto()
                        return actions
            self._save_auto()
            return actions

    def set_auto(self, enabled: bool, task_root: Path | None = None) -> dict[str, Any]:
        with self._auto_lock:
            if task_root is None:
                self._auto["globalEnabled"] = bool(enabled)
            else:
                key = str(task_root.resolve())
                tasks = self._auto.setdefault("tasks", {})
                tasks[key] = {"enabled": bool(enabled), "updatedAt": utc_now()}
            self._append_auto_log(
                "info",
                f"{'启用' if enabled else '关闭'} {'全局' if task_root is None else task_root.name} 自动续跑",
            )
            self._save_auto()
            return {
                "globalEnabled": bool(self._auto.get("globalEnabled")),
                "taskEnabled": bool(
                    ((self._auto.get("tasks") or {}).get(str(task_root.resolve()), {}) or {}).get("enabled")
                ) if task_root else None,
            }

    def platform_candidates(self, task_type: str = "0-1代码生成", force: bool = False) -> dict[str, Any]:
        return self.platform.candidates(task_type=task_type, force=force)

    def stats(self) -> dict[str, Any]:
        return {
            "uptimeSeconds": round(time.time() - self.started_at, 1),
            "hub": self.hub.stats(),
            "fileCache": self.file_cache.stats(),
            "logSubscribers": self.log.subscriber_count(),
            "lastReconcileAt": self.reconcile.last_run_at,
            "loops": self.health.snapshot(),
            "healthy": self.health.healthy(),
        }


def _describe_action(action: dict[str, Any]) -> str:
    if not isinstance(action, dict):
        return str(action)
    item = action.get("item") or action.get("task") or {}
    job = action.get("job") or {}
    name = item.get("taskName") or item.get("name") or item.get("projectCode") or ""
    pid = job.get("pid") or ""
    return f"{name}{f' PID={pid}' if pid else ''}"


def _public_settings(settings: dict[str, Any]) -> dict[str, Any]:
    """Settings as the UI may see them — never any credential material."""
    clean = copy.deepcopy(settings or {})
    clean.setdefault("disabledProjects", [])
    for key in list(clean):
        if any(marker in key.lower() for marker in ("password", "secret", "token")):
            if key == "managerPasswordSaved":
                continue
            clean.pop(key, None)
    manager = clean.get("manager") if isinstance(clean.get("manager"), dict) else {}
    clean["manager"] = {
        "baseUrl": str(manager.get("baseUrl") or ""),
        "username": str(manager.get("username") or "admin"),
        "passwordSaved": bool(manager.get("passwordSaved")),
    }
    return clean
