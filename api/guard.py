"""Fallback policies for tasks that will not finish on their own.

Three policies, checked once per reconcile round:

``candidate-timeout``
    The candidate race of a queue-owned task has been running longer than
    ``candidatePhaseHours`` (default 6 h; the slowest healthy race so far took
    5.5 h).
``no-progress``
    A queue-owned task that is not finished has written nothing — state file,
    candidate stdout, trajectories, workspace, the worker's log — for
    ``noProgressMinutes`` (default 60).
``leaked-process``
    Processes are still running under a task directory that finished
    (``complete`` / ``failed`` / ``blocked`` / ``error`` / ``stopped``) more than
    ``leakedProcessMinutes`` ago and that no queue item or worker owns — e.g.
    ``vite`` servers left by a verify step.

``mode`` decides what happens: ``observe`` (default) only flags and logs;
``enforce`` acts; ``off`` does nothing.

Stopping a task never deletes it.  The task name goes on the *skill's* stop
list so the desktop agent's next ``sologsb.py`` call refuses (exit 78); the
task's host processes get SIGTERM and its containers are removed; the state
file is marked ``failed`` with the policy and reason; the queue item becomes
``failed`` and its quota is refunded.  The monitor's own stop list is not used:
``enforce_stop_tasks`` deletes the directory of every marker it finds.
"""
from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any, Callable

from .common import (
    CANDIDATE_PHASE_DONE_STATUSES,
    QUEUE_ACTIVE_STATUSES,
    TASK_FAILURE_STATUSES,
    atomic_write_json,
    parse_time,
    read_json,
    safe_slug,
    utc_now,
)

GUARD_MODES = ("off", "observe", "enforce")
DEFAULT_GUARD_MODE = "observe"
DEFAULT_CANDIDATE_PHASE_HOURS = 6.0
DEFAULT_NO_PROGRESS_MINUTES = 60
DEFAULT_LEAKED_PROCESS_MINUTES = 60
FINISHED_TASK_STATUSES = {"complete", "stopped", *TASK_FAILURE_STATUSES}
SKILL_STOP_TASKS_PATH = Path(os.environ.get(
    "SOLOSB_SKILL_STOP_TASKS_PATH",
    str(Path.home() / ".codex" / "sologsb-0917" / "stop-tasks.json"),
))
# Never signalled: an operator's shell that happens to sit in a task dir, and
# the queue worker, which exits by itself once the state file is terminal.
SPARED_EXECUTABLES = {"login", "zsh", "bash", "sh", "fish", "tmux", "screen", "-zsh", "-bash"}
SPARED_MARKERS = ("queue_worker.py",)
# Directories the activity scan does not descend into: dependency trees and
# verify clones change for reasons unrelated to the task making progress.
SKIPPED_DIRS = {"node_modules", ".git", "verify", "__pycache__", ".venv", "dist", "build"}
POLICY_LABELS = {
    "candidate-timeout": "候选阶段超时",
    "no-progress": "长时间无进展",
    "leaked-process": "残留进程",
}


def guard_settings(config: dict[str, Any]) -> dict[str, Any]:
    cfg = (config.get("automation") or {}).get("guard") or {}
    mode = str(cfg.get("mode") or DEFAULT_GUARD_MODE)

    def number(key: str, default: float, low: float, high: float) -> float:
        try:
            value = float(cfg.get(key) if cfg.get(key) is not None else default)
        except (TypeError, ValueError):
            value = default
        return min(high, max(low, value))

    return {
        "mode": mode if mode in GUARD_MODES else DEFAULT_GUARD_MODE,
        "candidatePhaseHours": number("candidatePhaseHours", DEFAULT_CANDIDATE_PHASE_HOURS, 1, 48),
        "noProgressMinutes": int(number("noProgressMinutes", DEFAULT_NO_PROGRESS_MINUTES, 15, 1440)),
        "leakedProcessMinutes": int(number("leakedProcessMinutes", DEFAULT_LEAKED_PROCESS_MINUTES, 10, 1440)),
    }


def list_processes() -> list[dict[str, Any]]:
    """``[{pid, pgid, command}]`` from one ``ps`` call; empty on failure."""
    try:
        proc = subprocess.run(["ps", "-axo", "pid=,pgid=,command="], capture_output=True, text=True,
                              timeout=10, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return []
    out: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 3:
            continue
        try:
            out.append({"pid": int(parts[0]), "pgid": int(parts[1]), "command": parts[2]})
        except ValueError:
            continue
    return out


def remove_containers(prefix: str) -> list[str]:
    try:
        proc = subprocess.run(["docker", "ps", "-a", "--format", "{{.Names}}"], capture_output=True,
                              text=True, timeout=20, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return []
    removed: list[str] = []
    for name in proc.stdout.splitlines():
        if not name.startswith(prefix):
            continue
        try:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=30, check=False)
        except (OSError, subprocess.TimeoutExpired):
            continue
        removed.append(name)
    return removed


def path_forms(path: Path) -> set[str]:
    """A path as written and as resolved; command lines may carry either."""
    forms = {str(path)}
    try:
        forms.add(str(path.resolve()))
    except OSError:
        pass
    return forms


def terminate(process: dict[str, Any]) -> bool:
    """SIGTERM the process, and its group when it leads one."""
    pid, pgid = int(process["pid"]), int(process.get("pgid") or 0)
    try:
        if pgid == pid and pgid != os.getpgrp():
            os.killpg(pgid, signal.SIGTERM)
        else:
            os.kill(pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        return False
    return True


def latest_activity(task_root: Path, extra: list[Path], *, newer_than: float = 0.0) -> float | None:
    """Newest mtime among the files a working task writes.

    Stops early once something newer than ``newer_than`` turns up, so a busy
    task costs a handful of ``stat`` calls.
    """
    latest: float | None = None

    def seen(path: Path) -> bool:
        nonlocal latest
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return False
        latest = mtime if latest is None else max(latest, mtime)
        return bool(newer_than) and mtime >= newer_than

    for path in [task_root / "monitor" / "state.json", *extra]:
        if str(path) not in {"", "."} and seen(path):
            return latest
    for top in ("monitor", "workspace", "source"):
        base = task_root / top
        if not base.is_dir():
            continue
        for current, dirs, files in os.walk(base):
            dirs[:] = [name for name in dirs if name not in SKIPPED_DIRS]
            for name in files:
                if seen(Path(current) / name):
                    return latest
    return latest


class TaskGuard:
    """Checks the three policies; acts only in ``enforce`` mode."""

    def __init__(
        self,
        queue: Any,
        *,
        log: Any = None,
        platform: Any = None,
        stop_path: Path | None = None,
        processes: Callable[[], list[dict[str, Any]]] = list_processes,
        kill: Callable[[dict[str, Any]], bool] = terminate,
        remove: Callable[[str], list[str]] = remove_containers,
        clock: Callable[[], float] = time.time,
    ):
        self.queue = queue
        self.log = log
        self.platform = platform
        self.stop_path = Path(stop_path or SKILL_STOP_TASKS_PATH)
        self._processes = processes
        self._kill = kill
        self._remove = remove
        self._clock = clock
        # Flags already logged, so a standing condition is logged once.
        self._logged: set[str] = set()

    @property
    def settings(self) -> dict[str, Any]:
        return guard_settings(self.queue.config)

    def _emit(self, event: str, *, level: str = "info", **fields: Any) -> None:
        if self.log is None:
            return
        try:
            self.log.emit(event, level=level, **fields)
        except Exception:
            pass

    # -- checks ----------------------------------------------------------- #
    def _excluded_codes(self) -> set[str]:
        codes = (self.queue.config.get("automation") or {}).get("excludedProjectCodes") or []
        return {str(code).strip().casefold() for code in codes if str(code).strip()}

    def _item_verdict(self, item: dict[str, Any], settings: dict[str, Any], now: float) -> dict[str, Any] | None:
        raw = str(item.get("taskRoot") or "")
        if not raw:
            return None
        task_root = Path(raw).expanduser()
        state = read_json(task_root / "monitor" / "state.json", {})
        if not isinstance(state, dict) or not state:
            return None
        status = str(state.get("status") or "")
        if status in FINISHED_TASK_STATUSES:
            return None
        race_started = parse_time(state.get("candidateRaceStartedAt"))
        race_done = bool(state.get("candidateRaceFinishedAt") or state.get("candidateMapping")
                         or status in CANDIDATE_PHASE_DONE_STATUSES)
        limit = settings["candidatePhaseHours"] * 3600
        if race_started and not race_done and now - race_started.timestamp() >= limit:
            hours = (now - race_started.timestamp()) / 3600
            return {"policy": "candidate-timeout", "taskRoot": str(task_root),
                    "reason": f"候选阶段已运行 {hours:.1f} 小时，超过上限 {settings['candidatePhaseHours']:g} 小时"}
        quiet_limit = settings["noProgressMinutes"] * 60
        # The worker's log grows while the desktop agent's rollout does.
        extra = [Path(str(item.get("resultFile") or ""))]
        job = self.queue.jobs.get_platform(str(item.get("id") or ""), str(item.get("runKey") or ""))
        if job and job.get("logPath"):
            extra.append(Path(str(job["logPath"])))
        latest = latest_activity(task_root, extra, newer_than=now - quiet_limit)
        if latest is not None and now - latest >= quiet_limit:
            return {"policy": "no-progress", "taskRoot": str(task_root),
                    "reason": f"{int((now - latest) / 60)} 分钟没有任何写入（状态、候选输出、轨迹、日志），"
                              f"超过上限 {settings['noProgressMinutes']} 分钟"}
        return None

    def _leaks(self, settings: dict[str, Any], now: float, processes: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Finished, unowned task dirs that still have processes under them."""
        by_root: dict[str, list[dict[str, Any]]] = {}
        own = {os.getpid(), os.getppid()}
        forms = self._root_forms()
        for process in processes:
            if process["pid"] in own or not self._killable(process):
                continue
            command = process["command"]
            for form in forms:
                marker = form + os.sep
                if marker not in command:
                    continue
                rest = command.split(marker, 1)[1].split(os.sep, 1)[0].split()
                if not rest:
                    continue
                key = str((Path(form) / rest[0]).resolve())
                if process not in by_root.get(key, []):
                    by_root.setdefault(key, []).append(process)
        if not by_root:
            return []
        limit = settings["leakedProcessMinutes"] * 60
        leaks: list[dict[str, Any]] = []
        for raw_root, found in sorted(by_root.items()):
            task_root = Path(raw_root)
            if self.queue._task_root_owned_locked(task_root.resolve()):
                continue
            state_path = task_root / "monitor" / "state.json"
            state = read_json(state_path, {})
            if not isinstance(state, dict) or str(state.get("status") or "") not in FINISHED_TASK_STATUSES:
                continue
            try:
                quiet = now - state_path.stat().st_mtime
            except OSError:
                continue
            if quiet < limit:
                continue
            leaks.append({"policy": "leaked-process", "taskRoot": raw_root, "processes": found,
                          "reason": f"任务已 {state.get('status')} {quiet / 3600:.1f} 小时，仍有 {len(found)} 个进程"})
        return leaks

    def _root_forms(self) -> set[str]:
        """Scan roots as configured and as resolved (``/var`` vs ``/private/var``)."""
        forms: set[str] = set()
        for root in [*(self.queue.config.get("roots") or []), *self.queue._roots_locked()]:
            forms |= path_forms(Path(str(root)).expanduser())
        return forms

    @staticmethod
    def _killable(process: dict[str, Any]) -> bool:
        command = str(process.get("command") or "")
        executable = os.path.basename(command.split(None, 1)[0]) if command.strip() else ""
        if executable in SPARED_EXECUTABLES:
            return False
        return not any(marker in command for marker in SPARED_MARKERS)

    # -- actions ---------------------------------------------------------- #
    def _add_stop_marker(self, name: str, policy: str, reason: str) -> None:
        data = read_json(self.stop_path, {})
        data = data if isinstance(data, dict) else {"tasks": data if isinstance(data, list) else []}
        tasks = [str(value) for value in data.get("tasks") or [] if str(value).strip()]
        if name not in tasks:
            tasks.append(name)
        guard = data.get("guard") if isinstance(data.get("guard"), dict) else {}
        guard[name] = {"policy": policy, "reason": reason, "at": utc_now()}
        data.update({"tasks": tasks, "guard": guard})
        self.stop_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.stop_path, data)

    def _kill_under(self, task_root: Path, processes: list[dict[str, Any]]) -> int:
        forms = path_forms(task_root)
        own = {os.getpid(), os.getppid()}
        killed = 0
        for process in processes:
            command = process["command"]
            if process["pid"] in own or not self._killable(process):
                continue
            # The path itself, a file under it, or it as an argument — never a
            # sibling that merely shares the prefix (``<root>-other``).
            if any(form + os.sep in command or command.endswith(form) or f"{form} " in command
                   for form in forms):
                killed += int(self._kill(process))
        return killed

    def _close_state(self, task_root: Path, policy: str, reason: str) -> None:
        state_path = task_root / "monitor" / "state.json"
        state = read_json(state_path, {})
        if not isinstance(state, dict) or not state:
            return
        now = utc_now()
        for group in ("sides", "candidates"):
            records = state.get(group)
            if not isinstance(records, dict):
                continue
            for record in records.values():
                if isinstance(record, dict) and str(record.get("status") or "") in {"running", "queued", "blocked", "prepared"}:
                    record["previousStatus"] = record.get("status")
                    record["status"] = "invalidated"
                    record["closedAt"] = now
        state.update({
            "previousStatus": state.get("status"),
            "status": "failed",
            "closedBy": "sologsb-monitor-guard",
            "closedPolicy": policy,
            "closedAt": now,
            "closedReason": reason,
            "updatedAt": now,
        })
        atomic_write_json(state_path, state)

    def stop_task(self, item: dict[str, Any], verdict: dict[str, Any], processes: list[dict[str, Any]]) -> dict[str, Any]:
        """Stop one queue-owned task; caller holds the queue lock."""
        task_root = Path(verdict["taskRoot"]).expanduser()
        policy, reason = verdict["policy"], verdict["reason"]
        label = POLICY_LABELS[policy]
        self._add_stop_marker(task_root.name, policy, reason)
        killed = self._kill_under(task_root, processes)
        removed = self._remove(f"sologsb-{safe_slug(task_root.name)}-")
        self._close_state(task_root, policy, reason)
        item_id = str(item.get("id") or "")
        self.queue.slots.release_for_item(item_id)
        if self.platform is not None:
            self.queue.refund_quota(item, self.platform, f"兜底终止：{label}")
        item.update({
            "status": "failed",
            "capacityHeld": False,
            "orphaned": False,
            "slotMarkers": [],
            "finishedAt": utc_now(),
            "error": f"兜底终止（{label}）：{reason}",
            "notice": "",
            "guardFlag": {},
            "guardStopped": {"policy": policy, "reason": reason, "at": utc_now(),
                             "killedProcesses": killed, "removedContainers": removed},
        })
        self.queue._save()
        self._emit("guard.stopped", level="warning", taskId=item_id, projectCode=str(item.get("projectCode") or ""),
                   detail=f"{task_root.name}：{label}，{reason}；终止 {killed} 个进程，删除 {len(removed)} 个容器，"
                          f"已加入技能停止名单")
        return {"kind": "guard-stopped", "itemId": item_id, "policy": policy,
                "killed": killed, "containers": removed}

    # -- round ------------------------------------------------------------ #
    def run_once(self) -> list[dict[str, Any]]:
        settings = self.settings
        mode = settings["mode"]
        status: dict[str, Any] = {**settings, "lastRunAt": utc_now(), "flags": [], "leaks": []}
        if mode == "off":
            with self.queue._lock:
                changed = False
                for item in self.queue._items:
                    if item.get("guardFlag"):
                        item["guardFlag"] = {}
                        changed = True
                if changed:
                    self.queue._save()
            self.queue.guard_status = status
            return []
        now = self._clock()
        actions: list[dict[str, Any]] = []
        processes = self._processes()
        excluded = self._excluded_codes()
        with self.queue._lock:
            changed = False
            for item in self.queue._items:
                active = str(item.get("status") or "") in QUEUE_ACTIVE_STATUSES
                verdict = None
                if active and str(item.get("projectCode") or "").strip().casefold() not in excluded:
                    verdict = self._item_verdict(item, settings, now)
                item_id = str(item.get("id") or "")
                if verdict is None:
                    if item.get("guardFlag"):
                        item["guardFlag"] = {}
                        changed = True
                        self._logged = {key for key in self._logged if not key.startswith(item_id + ":")}
                    continue
                label = POLICY_LABELS[verdict["policy"]]
                if mode == "enforce":
                    actions.append(self.stop_task(item, verdict, processes))
                    continue
                flag = {"policy": verdict["policy"], "label": label, "reason": verdict["reason"],
                        "since": (item.get("guardFlag") or {}).get("since") or utc_now()}
                if item.get("guardFlag") != flag:
                    item["guardFlag"] = flag
                    changed = True
                status["flags"].append({"itemId": item_id, "projectCode": item.get("projectCode"), **flag})
                key = f"{item_id}:{verdict['policy']}"
                if key not in self._logged:
                    self._logged.add(key)
                    self._emit("guard.would_stop", level="warning", taskId=item_id,
                               projectCode=str(item.get("projectCode") or ""),
                               detail=f"观察模式：{label}，{verdict['reason']}；开启执行后会终止该任务")
            if changed:
                self.queue._save()
            leaks = self._leaks(settings, now, processes)
        for leak in leaks:
            task_root = Path(leak["taskRoot"])
            summary = {"taskRoot": str(task_root), "name": task_root.name, "reason": leak["reason"],
                       "processes": [{"pid": p["pid"], "command": p["command"][:160]} for p in leak["processes"]]}
            if mode == "enforce":
                killed = sum(int(self._kill(process)) for process in leak["processes"])
                actions.append({"kind": "guard-leak-killed", "taskRoot": str(task_root), "killed": killed})
                self._emit("guard.leak_killed", detail=f"{task_root.name}：{leak['reason']}，已终止 {killed} 个")
                continue
            status["leaks"].append(summary)
            key = f"leak:{task_root}"
            if key not in self._logged:
                self._logged.add(key)
                self._emit("guard.leak_found", level="warning",
                           detail=f"观察模式：{task_root.name}，{leak['reason']}；开启执行后会终止这些进程")
        self.queue.guard_status = status
        return actions
