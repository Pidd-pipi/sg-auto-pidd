"""Disk space: a launch gate and cleanup of finished task directories.

A task directory is ~1 GB, nearly all of it regenerable: ``node_modules``
under the candidates and the ``monitor/verify`` clones the audit step builds.
Unattended, a day of tasks filled 30 GB; the disk runs out in days and then
every task fails at once.

``free_gb`` feeds the launch gate in :meth:`QueueManager.tick`.
:class:`Housekeeper` runs from the reconcile loop, at most every
``intervalMinutes``, and for each task directory that

* is ``complete`` and its state file has been quiet for ``afterHours``,
* is not held by a queue item or worker,
* has no process running under it,

removes the ``node_modules`` directories and ``monitor/verify``.  It never
removes a directory that contains a path the state file refers to (evidence,
Excel, trajectories, recordings), and records what it did in
``monitor/housekeeping.json`` so the directory is not walked again.
Exited ``sologsb-<task>-*`` containers of those tasks are removed too.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .common import atomic_write_json, read_json, safe_slug, utc_now
from .guard import list_processes, path_forms

DEFAULT_MIN_FREE_GB = 30.0
DEFAULT_CLEANUP_AFTER_HOURS = 6.0
DEFAULT_CLEANUP_INTERVAL_MINUTES = 30
# Sizing and deleting node_modules is slow; a bounded batch keeps one reconcile
# round short enough for the watchdog.  Low disk runs a batch every round.
MAX_TASKS_PER_RUN = 5
URGENT_FREE_FACTOR = 1.5
CLEANED_MARKER = "housekeeping.json"
REGENERABLE_DIRS = {"node_modules"}


def housekeeping_settings(config: dict[str, Any]) -> dict[str, Any]:
    cfg = (config.get("automation") or {}).get("housekeeping") or {}

    def number(key: str, default: float, low: float, high: float) -> float:
        try:
            value = float(cfg.get(key) if cfg.get(key) is not None else default)
        except (TypeError, ValueError):
            value = default
        return min(high, max(low, value))

    return {
        "enabled": bool(cfg.get("enabled", True)),
        "minFreeGB": number("minFreeGB", DEFAULT_MIN_FREE_GB, 5, 500),
        "afterHours": number("afterHours", DEFAULT_CLEANUP_AFTER_HOURS, 1, 720),
        "intervalMinutes": number("intervalMinutes", DEFAULT_CLEANUP_INTERVAL_MINUTES, 5, 1440),
    }


def free_gb(path: str | Path) -> float | None:
    try:
        return shutil.disk_usage(str(path)).free / 1024 ** 3
    except OSError:
        return None


def dir_size(path: Path) -> int:
    total = 0
    for current, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += (Path(current) / name).lstat().st_size
            except OSError:
                continue
    return total


def remove_exited_containers(prefix: str) -> list[str]:
    try:
        proc = subprocess.run(
            ["docker", "ps", "-a", "--filter", "status=exited", "--filter", "status=created",
             "--filter", "status=dead", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return []
    removed: list[str] = []
    for name in proc.stdout.splitlines():
        if not name.startswith(prefix):
            continue
        try:
            subprocess.run(["docker", "rm", name], capture_output=True, timeout=30, check=False)
        except (OSError, subprocess.TimeoutExpired):
            continue
        removed.append(name)
    return removed


def referenced_paths(state: Any, forms: set[str]) -> list[Path]:
    """Every path inside the task dir (any of its ``forms``) the state mentions."""
    found: list[Path] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)
        elif isinstance(value, str) and any(value.startswith(form + os.sep) for form in forms):
            found.append(Path(value))

    walk(state)
    return found


class Housekeeper:
    def __init__(
        self,
        queue: Any,
        *,
        log: Any = None,
        processes: Callable[[], list[dict[str, Any]]] = list_processes,
        remove_containers: Callable[[str], list[str]] = remove_exited_containers,
        clock: Callable[[], float] = time.time,
    ):
        self.queue = queue
        self.log = log
        self._processes = processes
        self._remove_containers = remove_containers
        self._clock = clock
        self._last_run = 0.0
        self._thread: threading.Thread | None = None

    def start_background(self) -> bool:
        """Run one due round on a worker thread; never blocks the caller.

        Sizing and deleting ``node_modules`` can take minutes; the reconcile
        loop that calls this must keep its one-minute cadence and heartbeat.
        """
        if self._thread is not None and self._thread.is_alive():
            return False

        def work() -> None:
            try:
                self.run_once()
            except Exception as exc:
                self._emit("housekeeping.failed", level="error", detail=str(exc))

        self._thread = threading.Thread(target=work, name="housekeeping", daemon=True)
        self._thread.start()
        return True

    def _emit(self, event: str, *, level: str = "info", **fields: Any) -> None:
        if self.log is not None:
            try:
                self.log.emit(event, level=level, **fields)
            except Exception:
                pass

    def _forms(self, task_root: Path) -> set[str]:
        """The task dir as resolved and under each configured (unresolved) root."""
        forms = path_forms(task_root)
        parent = str(task_root.resolve().parent)
        for raw in self.queue.config.get("roots") or []:
            base = Path(str(raw)).expanduser()
            if parent in path_forms(base):
                forms.add(str(base / task_root.name))
        return forms

    def _task_roots(self) -> list[Path]:
        roots: list[Path] = []
        for root in self.queue._roots_locked():
            base = Path(root)
            if base.is_dir():
                roots.extend(sorted(path.parent.parent for path in base.glob("*/monitor/state.json")))
        return roots

    def _cleanable(self, task_root: Path, settings: dict[str, Any], now: float,
                   commands: list[str]) -> dict[str, Any] | None:
        monitor = task_root / "monitor"
        if (monitor / CLEANED_MARKER).is_file():
            return None
        state_path = monitor / "state.json"
        state = read_json(state_path, {})
        if not isinstance(state, dict) or str(state.get("status") or "") != "complete":
            return None
        try:
            if now - state_path.stat().st_mtime < settings["afterHours"] * 3600:
                return None
        except OSError:
            return None
        forms = self._forms(task_root)
        if any(form + os.sep in command or command.endswith(form) for command in commands for form in forms):
            return None
        with self.queue._lock:
            if self.queue._task_root_owned_locked(task_root.resolve()):
                return None
        return state

    def _targets(self, task_root: Path, state: dict[str, Any]) -> list[Path]:
        keep = [path.resolve() for path in referenced_paths(state, self._forms(task_root))]
        candidates: list[Path] = []
        verify = task_root / "monitor" / "verify"
        if verify.is_dir():
            candidates.append(verify)
        source = task_root / "source"
        if source.is_dir():
            for current, dirs, _files in os.walk(source):
                for name in list(dirs):
                    if name in REGENERABLE_DIRS:
                        candidates.append(Path(current) / name)
                        dirs.remove(name)
                    elif name == ".git":
                        dirs.remove(name)
        targets: list[Path] = []
        for path in candidates:
            if path.is_symlink():
                continue
            resolved = path.resolve()
            if any(ref == resolved or resolved in ref.parents for ref in keep):
                continue
            targets.append(path)
        return targets

    def run_once(self, *, force: bool = False, dry_run: bool = False,
                 limit: int | None = MAX_TASKS_PER_RUN) -> dict[str, Any]:
        settings = housekeeping_settings(self.queue.config)
        now = self._clock()
        free = self.queue.disk_free_gb()
        urgent = free is not None and free < settings["minFreeGB"] * URGENT_FREE_FACTOR
        due = force or urgent or now - self._last_run >= settings["intervalMinutes"] * 60
        if not settings["enabled"] or not due:
            return {"status": "skipped"}
        self._last_run = now
        commands = [process["command"] for process in self._processes()]
        cleaned: list[dict[str, Any]] = []
        freed = 0
        for task_root in self._task_roots():
            if limit is not None and len(cleaned) >= limit:
                break
            state = self._cleanable(task_root, settings, now, commands)
            if state is None:
                continue
            targets = self._targets(task_root, state)
            size = sum(dir_size(path) for path in targets)
            record = {"taskRoot": str(task_root), "removed": [str(p.relative_to(task_root)) for p in targets],
                      "bytes": size}
            if not dry_run:
                for path in targets:
                    shutil.rmtree(path, ignore_errors=True)
                record["containers"] = self._remove_containers(f"sologsb-{safe_slug(task_root.name)}-")
                try:
                    atomic_write_json(task_root / "monitor" / CLEANED_MARKER,
                                      {"at": utc_now(), "by": "sologsb-monitor", **record})
                except OSError:
                    pass
            freed += size
            cleaned.append(record)
        result = {"status": "ok", "dryRun": dry_run, "tasks": len(cleaned), "freedBytes": freed,
                  "at": utc_now(), "items": cleaned}
        if cleaned and not dry_run:
            self._emit("housekeeping.cleaned",
                       detail=f"清理 {len(cleaned)} 个已完成任务的 node_modules / verify，释放 {freed / 1024 ** 3:.1f} GB")
        if not dry_run:
            previous = getattr(self.queue, "housekeeping_status", {}) or {}
            self.queue.housekeeping_status = {
                "lastRunAt": result["at"], "lastTasks": len(cleaned), "lastFreedBytes": freed,
                "totalFreedBytes": int(previous.get("totalFreedBytes") or 0) + freed,
            }
        return result
