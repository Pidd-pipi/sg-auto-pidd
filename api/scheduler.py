"""Queue state machine, capacity ledger, quota lifecycle and the reconcile loop.

Two capacity models live side by side:

``tasks``       the historical semantics — ``capacity`` tasks in flight, each
                holding a startup reservation until it produces a container.
``containers``  keep the number of running candidate containers pinned to a
                target.  A task takes a container slot the moment it is claimed,
                using the same reservation-marker format ``side_runner``'s
                ``_ContainerLimiter`` uses, so the two processes agree on the
                ledger instead of each keeping its own count.

Container occupancy is running containers plus what live tasks still need
(read from each task's ``state.json``).  ``maxContainers`` is the single limit;
it is published to the skill's ``container-limit.json`` so both sides enforce
the same number.
"""
from __future__ import annotations

import copy
import json
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .common import (
    APP_DIR,
    CANDIDATE_PHASE_DONE_STATUSES,
    DEFAULT_MAX_CANDIDATE_CONTAINERS,
    DEFAULT_PLATFORM_START_TIMEOUT_SECONDS,
    DEFAULT_QUEUE_RETRY_BACKOFF_SECONDS,
    DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS,
    DEFAULT_RECONCILE_SECONDS,
    DEFAULT_STARTUP_GRACE_SECONDS,
    DEFAULT_ROOT,
    DEFAULT_SKILL_SCRIPT,
    DEFAULT_STALLED_TASK_RETRY_LIMIT,
    DEFAULT_STALLED_TASK_RETRY_SECONDS,
    DEFAULT_STARTUP_TIMEOUT_SECONDS,
    DEFAULT_QUOTA_SETTLE_TIMEOUT_SECONDS,
    DEFAULT_ORPHAN_GRACE_SECONDS,
    DEFAULT_PHANTOM_DEMAND_SECONDS,
    MAX_PHANTOM_DEMAND_SECONDS,
    MAX_STARTUP_TIMEOUT_SECONDS,
    MIN_PHANTOM_DEMAND_SECONDS,
    MIN_STARTUP_TIMEOUT_SECONDS,
    QUEUE_ACTIVE_STATUSES,
    QUEUE_STATE_PATH,
    QUEUE_TERMINAL_STATUSES,
    SCHEDULE_MODE_CONTAINERS,
    SCHEDULE_MODE_TASKS,
    SCHEDULE_MODES,
    SIDE_DONE_STATUSES,
    SIDES,
    STOP_TASKS_PATH,
    TASK_FAILURE_STATUSES,
    TERMINAL_TASK_STATUSES,
    FileCache,
    MonitorError,
    ProcessTable,
    age_seconds,
    atomic_write_json,
    clamp_int,
    iso_from_timestamp,
    parse_time,
    persisted_job_process_alive,
    pid_alive,
    pid_command,
    public_auto_refill_config,
    queue_failure_retryable,
    queue_prompt_sha256,
    read_json,
    runner_pid_alive,
    safe_slug,
    short_hash,
    utc_now,
)

try:  # The worker lives next to the package.
    from ..queue_log import LogWriter
except Exception:  # pragma: no cover - only hit when running from a stripped copy
    LogWriter = None  # type: ignore[assignment]

from .tasks import discover_task_roots, task_container_names  # noqa: E402  (kept last to avoid a cycle)
from .housekeeping import free_gb, housekeeping_settings  # noqa: E402

CONTAINER_SLOT_ROOT = Path(
    os.environ.get(
        "SOLOSB_CONTAINER_SLOTS",
        str(Path.home() / ".codex" / "sologsb-0917" / "container-slots"),
    )
)
ATTEMPTS_ALERT_THRESHOLD = 500
# side_runner.ABSOLUTE_MAX_CONTAINERS; the skill clamps anything above it.
SKILL_ABSOLUTE_MAX_CONTAINERS = 6
SKILL_LIMIT_MANAGED_BY = "sologsb-monitor"
MIN_LAUNCH_SPACING_SECONDS = 20
# A task dir the skill still counts as "active" (project_claims._task_is_active)
# is only closed by the monitor once its state file has been quiet this long,
# so an executor mid-write is never raced.
STALE_TASK_CLOSE_MIN_AGE_SECONDS = 120
# project_claims._task_is_active before skill 1.3.0: these keep a project locked
# in the skill.  1.3.0+ only counts a live ``running`` runner, but older skill
# copies on other hosts still use this rule, so the stale-task close stays.
SKILL_ACTIVE_TASK_STATUSES = {"running", "blocked"}
SKILL_ACTIVE_SIDE_STATUSES = {"running", "blocked", "attempt_invalid"}


# Knobs that no longer change behaviour: the refill threshold was a second,
# confusing container limit; placeholder reservations are gone; the key-level
# cap is now the single ``maxContainers``.  Removed from config on save.
LEGACY_AUTOMATION_KEYS = (
    "containerRefillBelow",
    "containerReserveSeconds",
    "keyConcurrency",
)


def prune_legacy_automation(automation: dict[str, Any]) -> list[str]:
    """Drop dead knobs, folding the old key-level cap into ``maxContainers``."""
    key_cap = (automation.get("keyConcurrency") or {}).get("maxCandidateContainers") if isinstance(automation.get("keyConcurrency"), dict) else None
    if key_cap and not automation.get("maxContainers"):
        automation["maxContainers"] = key_cap
    removed = [key for key in LEGACY_AUTOMATION_KEYS if key in automation]
    for key in removed:
        automation.pop(key, None)
    return removed


def _remove_empty_task_root(raw: Any) -> bool:
    """Drop a task directory the desktop session never used.

    ``rmdir`` only succeeds on an empty directory, so anything the skill wrote
    is left alone.
    """
    text = str(raw or "").strip()
    if not text:
        return False
    try:
        Path(text).rmdir()
        return True
    except OSError:
        return False


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


class JobManager:
    """Owns the subprocesses that run the skill CLI and the queue worker."""

    def __init__(
        self,
        config: dict[str, Any],
        process_table: ProcessTable | None = None,
        *,
        state_dir: Path | None = None,
    ):
        self.config = config
        self.process_table = process_table or ProcessTable()
        self._lock = threading.RLock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._platform_persisted: dict[str, dict[str, Any]] = {}
        self._recent: list[dict[str, Any]] = []
        jobs_dir = Path(state_dir) if state_dir else APP_DIR / ".state" / "jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        self.jobs_dir = jobs_dir
        self._recover_running_jobs()
        self._orphans_reaped = self.reap_orphan_workers()

    # -- recovery --------------------------------------------------------- #
    def _recover_running_jobs(self) -> None:
        seen_keys: set[str] = set()
        candidates = sorted(
            self.jobs_dir.glob("*.json"),
            key=lambda path: path.stat().st_mtime if path.exists() else 0,
            reverse=True,
        )
        for path in candidates:
            job = read_json(path, {})
            if not isinstance(job, dict):
                continue
            key = str(job.get("key") or "")
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            platform_item_id = str(job.get("platformItemId") or "")
            if platform_item_id and platform_item_id not in self._platform_persisted:
                indexed = copy.deepcopy(job)
                indexed["logPath"] = str(indexed.get("logPath") or path.with_suffix(".log"))
                self._platform_persisted[platform_item_id] = indexed
            if job.get("status") != "running" or not persisted_job_process_alive(job):
                continue
            persisted = copy.deepcopy(job)
            persisted["logPath"] = str(persisted.get("logPath") or path.with_suffix(".log"))
            self._jobs[key] = persisted
            self._recent.append(persisted)

    @staticmethod
    def key(task_root: Path | str, side: str) -> str:
        return f"{Path(task_root).resolve()}::{str(side).upper()}"

    def reap_orphan_workers(self) -> list[dict[str, Any]]:
        """Terminate queue workers left behind by a previous scheduler process.

        Workers are started with ``start_new_session=True`` so they outlive the
        server that launched them.  The instance lock guarantees only one server
        owns ``.state/jobs`` at a time, so any worker still writing a result file
        into that directory belongs to a dead instance and will never be
        reconciled by anyone.  Workers whose job record was recovered above are
        left alone — those belong to us.
        """
        if not bool((self.config.get("automation") or {}).get("reapOrphanWorkers", True)):
            return []
        jobs_prefix = str(self.jobs_dir.resolve()) + os.sep
        owned: set[str] = set()
        with self._lock:
            for job in self._jobs.values():
                result_file = str(job.get("resultFile") or "")
                if result_file:
                    owned.add(str(Path(result_file).resolve()))
        reaped: list[dict[str, Any]] = []
        try:
            proc = subprocess.run(
                ["ps", "-axo", "pid=,command="],
                capture_output=True, text=True, timeout=10, check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return []
        for line in proc.stdout.splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) < 2:
                continue
            try:
                pid = int(parts[0])
            except ValueError:
                continue
            command = parts[1]
            if "queue_worker.py" not in command or pid == os.getpid():
                continue
            match = re.search(r"--result-file\s+(\S+)", command)
            if not match:
                continue
            result_file = str(Path(match.group(1)).expanduser().resolve())
            if not result_file.startswith(jobs_prefix) or result_file in owned:
                continue
            terminated = self._terminate_platform_worker({"pid": pid, "resultFile": result_file})
            if terminated:
                reaped.append({"pid": pid, "resultFile": result_file})
        return reaped

    def get(self, task_root: Path | str, side: str) -> dict[str, Any] | None:
        with self._lock:
            item = self._jobs.get(self.key(task_root, side))
            return copy.deepcopy(item) if item else None

    def running(self) -> list[dict[str, Any]]:
        with self._lock:
            return [copy.deepcopy(item) for item in self._jobs.values() if item.get("status") == "running"]

    @staticmethod
    def _platform_job_task_root(job: dict[str, Any]) -> Path | None:
        result_file = Path(str(job.get("resultFile") or ""))
        result = read_json(result_file, {}) if result_file else {}
        raw_root = (result.get("taskRoot") if isinstance(result, dict) else "") or job.get("taskRoot")
        if not str(raw_root or "").strip():
            return None
        return Path(str(raw_root)).expanduser().resolve()

    @classmethod
    def platform_job_started(cls, job: dict[str, Any]) -> bool:
        result_file = Path(str(job.get("resultFile") or ""))
        result = read_json(result_file, {}) if result_file else {}
        if isinstance(result, dict) and str(result.get("stage") or "") == "desktop-task-running":
            return True
        task_root = cls._platform_job_task_root(job)
        if task_root is None:
            return False
        return (
            (task_root / "monitor" / "state.json").is_file()
            or (task_root / "monitor" / "init-failure.json").is_file()
        )

    @staticmethod
    def _terminate_platform_worker(job: dict[str, Any]) -> bool:
        try:
            pid = int(job.get("pid") or 0)
        except (TypeError, ValueError):
            return False
        if pid <= 0:
            return False
        command = pid_command(pid)
        if "queue_worker.py" not in command.lower():
            return False
        result_file = str(job.get("resultFile") or "")
        if result_file and result_file not in command:
            return False
        try:
            os.killpg(pid, signal.SIGTERM)
            return True
        except ProcessLookupError:
            return True
        except OSError:
            try:
                os.kill(pid, signal.SIGTERM)
                return True
            except OSError:
                return False

    def stop_platform_worker_for_retry(self, item_id: str, run_key: str = "") -> bool:
        """Stop a stalled queue worker and mark its record finished before retry."""
        key = f"platform:{item_id}"
        with self._lock:
            job = self._jobs.get(key)
            if job is None or (run_key and str(job.get("runKey") or "") != str(run_key)):
                return True
            terminated = True
            if job.get("status") == "running":
                terminated = self._terminate_platform_worker(job)
            job["status"] = "failed"
            job["finishedAt"] = utc_now()
            job["error"] = str(job.get("error") or "任务无状态更新，监控执行器已停止并准备自动重试")
            self._persist(job)
            return terminated

    def terminate_platform(self, item_id: str, *, reason: str = "") -> bool:
        key = f"platform:{item_id}"
        with self._lock:
            job = self._jobs.get(key)
            if job is None:
                return False
            terminated = True
            if job.get("status") == "running":
                terminated = self._terminate_platform_worker(job)
            job["status"] = "failed"
            job["finishedAt"] = utc_now()
            job["error"] = reason or job.get("error") or "监控台主动终止"
            job["terminatedByMonitor"] = True
            self._persist(job)
            return terminated

    def reap_stale_platform_jobs(self, startup_timeout_seconds: int | float) -> list[dict[str, Any]]:
        timeout = max(30.0, float(startup_timeout_seconds or DEFAULT_PLATFORM_START_TIMEOUT_SECONDS))
        reaped: list[dict[str, Any]] = []
        with self._lock:
            for job in self._jobs.values():
                if job.get("source") != "platform" or job.get("status") != "running":
                    continue
                if self.platform_job_started(job):
                    continue
                age = age_seconds(job.get("startedAt"))
                if age is None or age < timeout:
                    continue
                terminated = self._terminate_platform_worker(job)
                job.update({
                    "status": "failed",
                    "finishedAt": utc_now(),
                    "exitCode": -15,
                    "error": (
                        f"桌面任务启动超时：{int(timeout)} 秒内未创建任务目录，已释放并发名额"
                        + ("并停止执行器" if terminated else "；执行器已不可用")
                    ),
                })
                started = parse_time(job.get("startedAt"))
                job["durationSeconds"] = max(0.0, time.time() - (started.timestamp() if started else time.time()))
                _remove_empty_task_root(job.get("reservedTaskRoot"))
                self._persist(job)
                reaped.append(copy.deepcopy(job))
        return reaped

    # -- starting --------------------------------------------------------- #
    def start(
        self,
        task_root: Path,
        side: str,
        *,
        force: bool = False,
        reason: str = "manual",
    ) -> dict[str, Any]:
        side = str(side).upper()
        if side not in SIDES:
            raise MonitorError("side 只能为 A 或 B")
        key = self.key(task_root, side)
        with self._lock:
            current = self._jobs.get(key)
            if current and current.get("status") == "running":
                raise MonitorError(f"{side} 已有监控端任务在运行，PID={current.get('pid')}")
            script = Path(str(self.config.get("skillScript") or DEFAULT_SKILL_SCRIPT)).expanduser().resolve()
            if not script.is_file():
                raise MonitorError(f"找不到 sologsb CLI：{script}")
            command = [
                sys.executable,
                str(script),
                "run",
                "--task-root",
                str(task_root.resolve()),
                "--side",
                side,
            ]
            if force:
                command.append("--force")
            stamp = datetime_stamp()
            log_path = self.jobs_dir / f"{stamp}-{safe_slug(task_root.name)}-{side.lower()}.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            log_handle = log_path.open("ab", buffering=0)
            try:
                process = subprocess.Popen(
                    command,
                    cwd=str(task_root),
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    env=env,
                    start_new_session=True,
                )
            except Exception:
                log_handle.close()
                raise
            job = {
                "key": key,
                "taskRoot": str(task_root.resolve()),
                "taskName": task_root.name,
                "side": side,
                "status": "running",
                "reason": reason,
                "force": force,
                "pid": process.pid,
                "startedAt": utc_now(),
                "finishedAt": "",
                "exitCode": None,
                "command": " ".join(_quote(part) for part in command),
                "logPath": str(log_path),
            }
            self._jobs[key] = job
            self._recent.append(job)
            del self._recent[:-40]
            self._persist(job)
            threading.Thread(
                target=self._wait,
                args=(key, process, log_handle),
                name=f"job-{side.lower()}-{process.pid}",
                daemon=True,
            ).start()
            return copy.deepcopy(job)

    def get_platform(self, item_id: str, run_key: str = "") -> dict[str, Any] | None:
        with self._lock:
            key = f"platform:{item_id}"

            def finalize_dead_job(job: dict[str, Any]) -> dict[str, Any]:
                result_file = Path(str(job.get("resultFile") or ""))
                result = read_json(result_file, {}) if result_file else {}
                if isinstance(result, dict) and result.get("status") == "finished":
                    job["status"] = "finished"
                    job["exitCode"] = int(result.get("exitCode") or 0)
                else:
                    job["status"] = "failed"
                    job["exitCode"] = int((result or {}).get("exitCode") or -1)
                job["finishedAt"] = utc_now()
                if job["status"] == "failed":
                    job["error"] = str((result or {}).get("error") or "监控重启后发现执行器进程已退出")
                return job

            item = self._jobs.get(key)
            if item is not None and run_key and str(item.get("runKey") or "") != str(run_key):
                item = None
            if item is not None and item.get("status") == "running" and not persisted_job_process_alive(item):
                item = finalize_dead_job(item)
                self._persist_sidecar(item)
            if item is None:
                persisted = self._platform_persisted.get(item_id)
                if (
                    isinstance(persisted, dict)
                    and persisted.get("key") == key
                    and (not run_key or str(persisted.get("runKey") or "") == str(run_key))
                ):
                    persisted = copy.deepcopy(persisted)
                    if persisted.get("status") == "running" and not persisted_job_process_alive(persisted):
                        persisted = finalize_dead_job(persisted)
                        self._persist_sidecar(persisted)
                    item = persisted
                    self._jobs[key] = persisted
                else:
                    pattern = f"*-platform-{safe_slug(item_id)}.json"
                    candidates = sorted(
                        self.jobs_dir.glob(pattern),
                        key=lambda path: path.stat().st_mtime if path.exists() else 0,
                        reverse=True,
                    )
                    for path in candidates:
                        persisted = read_json(path, {})
                        if not isinstance(persisted, dict) or persisted.get("key") != key:
                            continue
                        if run_key and str(persisted.get("runKey") or "") != str(run_key):
                            continue
                        if persisted.get("status") == "running" and not persisted_job_process_alive(persisted):
                            persisted = finalize_dead_job(persisted)
                            _write_json(path, persisted)
                        item = persisted
                        self._jobs[key] = persisted
                        self._platform_persisted[item_id] = copy.deepcopy(persisted)
                        break
            return copy.deepcopy(item) if item else None

    def start_platform(
        self,
        item: dict[str, Any],
        *,
        reason: str = "queue",
        platform: Any = None,
    ) -> dict[str, Any]:
        item_id = str(item.get("id") or "")
        if not item_id:
            raise MonitorError("平台队列项缺少 id")
        key = f"platform:{item_id}"
        script, worker, active_roots = self.validate_platform_runner()
        with self._lock:
            current = self._jobs.get(key)
            if current and current.get("status") == "running":
                raise MonitorError(f"平台任务已在运行，PID={current.get('pid')}")
            push_helper = str((self.config.get("automation") or {}).get("codexQueuePush") or "").strip()
            bound_scope = Path(str(item.get("scopeRoot") or "")).expanduser().resolve() if str(item.get("scopeRoot") or "").strip() else None
            workdir = bound_scope if bound_scope in active_roots else active_roots[0]
            workdir.mkdir(parents=True, exist_ok=True)
            stamp = datetime_stamp()
            run_key = str(item.get("runKey") or "").strip() or uuid.uuid4().hex[:8]
            task_name = f"{item.get('projectCode') or 'platform'}-{stamp}-{safe_slug(run_key)[:8]}"
            task_root = workdir / task_name
            if task_root.exists() and any(task_root.iterdir()):
                raise MonitorError(f"预留任务目录已存在且不为空: {task_root}")
            task_root.mkdir(parents=True, exist_ok=True)
            result_file = self.jobs_dir / f"{stamp}-platform-{safe_slug(item_id)}.result.json"
            log_path = self.jobs_dir / f"{stamp}-platform-{safe_slug(item_id)}.log"
            trigger_prompt = str(item.get("triggerPrompt") or "")
            trigger_prompt_path: Path | None = self.jobs_dir / f"{stamp}-platform-{safe_slug(item_id)}.prompt.txt"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            if trigger_prompt:
                trigger_prompt_path.write_text(trigger_prompt + "\n", encoding="utf-8")
            else:
                trigger_prompt_path = None
            command = [
                sys.executable,
                str(worker),
                "--skill-script", str(script),
                "--workdir", str(workdir),
                "--task-name", task_name,
                "--project-code", str(item.get("projectCode") or ""),
                "--task-type", str(item.get("taskType") or "0-1代码生成"),
                "--difficulty", str(item.get("difficulty") or "困难"),
                "--side", str(item.get("side") or "both"),
                "--startup-timeout", str(int((self.config.get("automation") or {}).get("startupTimeoutSeconds") or DEFAULT_STARTUP_TIMEOUT_SECONDS)),
                "--terminal-stability-seconds", str(float((self.config.get("automation") or {}).get("terminalStabilitySeconds") or 6)),
                "--wait-timeout", str(int((self.config.get("automation") or {}).get("waitTimeoutSeconds") or DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS)),
                "--result-file", str(result_file),
            ]
            quota = item.get("quota") if isinstance(item.get("quota"), dict) else {}
            if quota.get("platformTaskId"):
                command.extend(["--platform-task-id", str(quota.get("platformTaskId"))])
                if quota.get("platformTaskNo"):
                    command.extend(["--platform-task-no", str(quota.get("platformTaskNo"))])
            if quota.get("variantId") or item.get("variantId"):
                command.extend(["--variant-id", str(quota.get("variantId") or item.get("variantId") or "")])
            folder_id = str(item.get("folderId") or "")
            folder_path = str(item.get("folderPath") or "")
            if folder_id:
                command.extend(["--folder-id", folder_id])
            if folder_path:
                command.extend(["--folder-path", folder_path])
            if push_helper:
                command.extend(["--push-helper", push_helper])
            if trigger_prompt_path:
                command.extend(["--trigger-prompt-file", str(trigger_prompt_path)])
            # Keep the per-task rollout log beside the job record; static/ is
            # served to the browser and must not accumulate task logs.
            command.extend(["--log-file", str(log_path)])
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            log_handle = log_path.open("ab", buffering=0)
            try:
                process = subprocess.Popen(
                    command,
                    cwd=str(workdir),
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    env=env,
                    start_new_session=True,
                )
            except Exception:
                log_handle.close()
                _remove_empty_task_root(task_root)
                raise
            job = {
                "key": key,
                "source": "platform",
                "platformItemId": item_id,
                "taskRoot": str(item.get("taskRoot") or ""),
                "taskName": str(item.get("projectName") or item.get("projectCode") or task_name),
                "projectCode": str(item.get("projectCode") or ""),
                "runKey": str(item.get("runKey") or ""),
                "side": str(item.get("side") or "both"),
                "status": "running",
                "reason": reason,
                "force": False,
                "pid": process.pid,
                "startedAt": utc_now(),
                "finishedAt": "",
                "exitCode": None,
                "command": " ".join(_quote(part) for part in command),
                "logPath": str(log_path),
                "resultFile": str(result_file),
                "triggerPromptPath": str(trigger_prompt_path) if trigger_prompt_path else "",
                "taskDirName": task_name,
                "reservedTaskRoot": str(task_root),
            }
            self._jobs[key] = job
            self._platform_persisted[item_id] = copy.deepcopy(job)
            self._recent.append(job)
            del self._recent[:-40]
            self._persist(job)
            threading.Thread(
                target=self._wait,
                args=(key, process, log_handle),
                name=f"job-platform-{process.pid}",
                daemon=True,
            ).start()
            return copy.deepcopy(job)

    def validate_platform_runner(self) -> tuple[Path, Path, list[Path]]:
        """Validate static launch prerequisites without changing quota or slots."""
        script = Path(str(self.config.get("skillScript") or DEFAULT_SKILL_SCRIPT)).expanduser().resolve()
        worker = APP_DIR / "queue_worker.py"
        if not script.is_file():
            raise MonitorError(f"找不到 sologsb CLI：{script}")
        if not worker.is_file():
            raise MonitorError(f"找不到队列 worker：{worker}")
        monitor_cfg = self.config.get("monitor") or {}
        active_values = (
            monitor_cfg.get("activeRoots")
            if "activeRoots" in monitor_cfg
            else self.config.get("roots") or [DEFAULT_ROOT]
        )
        active_roots = [Path(value).expanduser().resolve() for value in active_values or []]
        if not active_roots:
            raise MonitorError("未选择任何 Codex 任务目录，不能启动平台任务")
        return script, worker, active_roots

    def _persist(self, job: dict[str, Any]) -> None:
        item_id = str(job.get("platformItemId") or "")
        if item_id:
            self._platform_persisted[item_id] = copy.deepcopy(job)
        self._persist_sidecar(job)

    def _persist_sidecar(self, job: dict[str, Any]) -> None:
        log_path = Path(str(job.get("logPath") or ""))
        if not log_path:
            return
        try:
            _write_json(log_path.with_suffix(".json"), job)
        except OSError:
            pass

    def _wait(self, key: str, process: subprocess.Popen, log_handle) -> None:
        try:
            code = process.wait()
        finally:
            try:
                log_handle.close()
            except OSError:
                pass
        with self._lock:
            job = self._jobs.get(key)
            if job:
                job["status"] = "finished" if code == 0 else "failed"
                job["exitCode"] = code
                job["finishedAt"] = utc_now()
                started = parse_time(job.get("startedAt"))
                job["durationSeconds"] = max(0.0, time.time() - (started.timestamp() if started else time.time()))
                self._persist(job)

    def _latest_persisted_job(self, task_root: Path, side: str) -> dict[str, Any] | None:
        pattern = f"*-{safe_slug(task_root.name)}-{str(side).lower()}.json"
        candidates = sorted(
            self.jobs_dir.glob(pattern),
            key=lambda item: item.stat().st_mtime if item.exists() else 0,
            reverse=True,
        )
        for candidate in candidates:
            item = read_json(candidate, {})
            if isinstance(item, dict) and item:
                return item
        return None

    def tail_log(self, task_root: Path, side: str, lines: int = 200) -> dict[str, Any]:
        from .common import tail_lines

        job = self.get(task_root, side) or self._latest_persisted_job(task_root, side)
        path = Path(job["logPath"]) if job and job.get("logPath") else None
        if path is None or not path.is_file():
            return {"job": job, "lines": []}
        from .common import redact_text

        return {
            "job": job,
            "lines": [redact_text(line, 1200) for line in tail_lines(path, max(1, min(lines, 1000)))],
        }


def datetime_stamp() -> str:
    from datetime import datetime as _dt

    return _dt.now().strftime("%Y%m%d-%H%M%S")


def _quote(part: str) -> str:
    import shlex

    return shlex.quote(part)


# --------------------------------------------------------------------------- #
# container slot ledger
# --------------------------------------------------------------------------- #
class ContainerLedger:
    """Cross-process container slot accounting built on ``side_runner``'s markers.

    The reservation files use exactly the shape ``_ContainerLimiter`` writes
    (``container`` / ``projectCode`` / ``pid`` / ``createdAt``) plus a few extra
    keys the monitor needs.  Reading the same directory means the executor and
    the monitor never disagree about how many slots are taken.
    """

    def __init__(self, root: Path | None = None, docker_cache: Any = None):
        self.root = Path(root or CONTAINER_SLOT_ROOT)
        self.reservations = self.root / "reservations"
        self.lock_path = self.root / "limit.lock"
        # The skill's limiter reads its settings from the file beside the slot
        # directory (``side_runner.CONTAINER_LIMIT_PATH``).
        self.limit_path = self.root.parent / "container-limit.json"
        self.docker_cache = docker_cache
        self._lock = threading.RLock()

    def write_limits(self, *, max_containers: int, excluded: list[str]) -> bool:
        """Publish the monitor's limit to the skill; returns whether it changed.

        ``managedBy`` makes this value win over the device config inside
        ``_ContainerLimiter``, so the limit is set in one place: the monitor.
        """
        current = read_json(self.limit_path, {})
        current = current if isinstance(current, dict) else {}
        current.pop("updatedAt", None)
        wanted = {
            **current,
            "maxContainers": int(max_containers),
            "excludedProjectCodes": sorted({str(code) for code in excluded if str(code).strip()}),
            "managedBy": SKILL_LIMIT_MANAGED_BY,
        }
        if current == wanted:
            return False
        wanted["updatedAt"] = utc_now()
        try:
            _write_json(self.limit_path, wanted)
        except OSError:
            return False
        return True

    def ensure(self) -> None:
        self.reservations.mkdir(parents=True, exist_ok=True)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)

    def _running_names(self) -> set[str]:
        if self.docker_cache is None:
            return set()
        docker = self.docker_cache.get()
        return {
            str(item.get("name") or "")
            for item in (docker.get("items") or [])
            if str(item.get("state") or "").lower() == "running"
        }

    def _read_markers(self) -> list[tuple[Path, dict[str, Any]]]:
        if not self.reservations.is_dir():
            return []
        out: list[tuple[Path, dict[str, Any]]] = []
        for path in sorted(self.reservations.glob("*.json")):
            data = read_json(path, {})
            if isinstance(data, dict) and data:
                out.append((path, data))
            else:
                try:
                    path.unlink()
                except OSError:
                    pass
        return out

    def snapshot(self) -> dict[str, Any]:
        """Occupied / available slots plus the raw marker list."""
        running = self._running_names()
        markers = self._read_markers()
        occupied: list[dict[str, Any]] = []
        dead: list[Path] = []
        for path, data in markers:
            name = str(data.get("container") or "").strip()
            if not name or name in running:
                # Either a malformed marker or a container that already exists;
                # in both cases the slot is no longer a reservation.
                if not name:
                    dead.append(path)
                continue
            if not pid_alive(data.get("pid")):
                dead.append(path)
                continue
            occupied.append({
                "path": str(path),
                "container": name,
                "projectCode": str(data.get("projectCode") or ""),
                "taskId": str(data.get("taskId") or ""),
                "itemId": str(data.get("itemId") or ""),
                "pid": data.get("pid"),
                "createdAt": str(data.get("createdAt") or ""),
                "ageSeconds": age_seconds(data.get("createdAt")),
            })
        return {
            "occupied": occupied,
            "occupiedCount": len(occupied),
            "deadMarkers": [str(path) for path in dead],
            "runningContainers": len(running),
        }

    def reserve(self, *, container: str, project_code: str, task_id: str = "", item_id: str = "", run_key: str = "") -> Path | None:
        """Write a reservation marker; returns its path or ``None`` on failure."""
        self.ensure()
        marker = self.reservations / f"{uuid.uuid4().hex}.json"
        try:
            _write_json(marker, {
                "container": str(container or ""),
                "projectCode": str(project_code or ""),
                "pid": os.getpid(),
                "createdAt": utc_now(),
                "taskId": str(task_id or ""),
                "itemId": str(item_id or ""),
                "runKey": str(run_key or ""),
            })
        except OSError:
            return None
        return marker

    def reserve_batch(
        self,
        *,
        count: int,
        container_prefix: str,
        project_code: str,
        item_id: str = "",
        run_key: str = "",
    ) -> list[str]:
        """Reserve ``count`` slots for a task that has not started containers yet.

        One marker per expected candidate container, matching how
        ``side_runner._ContainerLimiter`` counts reservations, so the executor
        sees the monitor's claim on the same files and waits instead of
        over-subscribing the key.  The marker names are placeholders; they are
        released as soon as the task's real containers show up in ``docker ps``.
        """
        # A retry can leave older markers behind when a previous launch failed
        # before a worker existed.  Keep this operation idempotent per queue
        # item so every scheduler pass replaces, rather than accumulates,
        # reservations for that item.
        if item_id:
            self.release_for_item(item_id)
        markers: list[str] = []
        for index in range(max(1, int(count))):
            marker = self.reserve(
                container=f"{container_prefix}-reserve-{index + 1}",
                project_code=project_code,
                item_id=item_id,
                run_key=run_key,
            )
            if marker:
                markers.append(str(marker))
        return markers

    def release(self, marker_path: str | Path | None) -> bool:
        if not marker_path:
            return False
        path = Path(str(marker_path))
        try:
            path.unlink()
            return True
        except OSError:
            return False

    def release_for_item(self, item_id: str) -> list[str]:
        """Drop every marker belonging to a queue item; returns removed paths."""
        marker_id = str(item_id or "")
        removed: list[str] = []
        if not marker_id:
            return removed
        for path, data in self._read_markers():
            if str(data.get("itemId") or "") == marker_id and self.release(path):
                removed.append(str(path))
        return removed

    def sweep_dead(self) -> list[str]:
        """Remove markers whose owner PID is gone (the executor does this too)."""
        running = self._running_names()
        removed: list[str] = []
        for path, data in self._read_markers():
            name = str(data.get("container") or "").strip()
            if not name:
                removed.append(str(path)) if self.release(path) else None
                continue
            if name in running:
                continue
            if not pid_alive(data.get("pid")):
                if self.release(path):
                    removed.append(str(path))
        return removed

    def item_slot(self, item_id: str) -> dict[str, Any] | None:
        for entry in self.snapshot()["occupied"]:
            if entry.get("itemId") == str(item_id or ""):
                return entry
        return None


# --------------------------------------------------------------------------- #
# queue
# --------------------------------------------------------------------------- #
class QueueManager:
    """Persistent queue with the two capacity models and the quota lifecycle."""

    def __init__(
        self,
        config: dict[str, Any],
        jobs: JobManager,
        *,
        docker_cache: Any = None,
        file_cache: FileCache | None = None,
        process_table: ProcessTable | None = None,
        log: Any = None,
        state_path: Path | None = None,
        slot_root: Path | None = None,
        blocked_codes: set[str] | None = None,
        platform: Any = None,
    ):
        self.config = config
        self.jobs = jobs
        # Set by the service; ``None`` keeps refunds as local bookkeeping.
        self.platform = platform
        self.docker_cache = docker_cache
        self.file_cache = file_cache or FileCache()
        self.process_table = process_table or ProcessTable()
        self.log = log
        self.state_path = state_path or QUEUE_STATE_PATH
        self.slots = ContainerLedger(root=slot_root, docker_cache=docker_cache)
        self.blocked_codes = {str(code).casefold() for code in (blocked_codes or set())}
        # Set by the service once it has read the task tree and docker at least
        # once; see SchedulerService.startup_guard.
        self.state_loaded = False
        self.started_at = time.time()
        # Outcome of the service's last auto-refill round, shown on the queue page.
        self.refill_status: dict[str, Any] = {"status": "idle", "message": "尚未运行", "at": ""}
        # Last round of the fallback policies (api/guard.py), set by TaskGuard.
        self.guard_status: dict[str, Any] = {}
        # Cleanup totals since start (api/housekeeping.py), set by Housekeeper.
        self.housekeeping_status: dict[str, Any] = {}
        self._disk_low = False
        self._lock = threading.RLock()
        self._triggered: list[dict[str, Any]] = []
        self._lastStartedAt = ""
        self._capacity_saturated = False
        self._docker_down = False
        # Queued-candidate demand that never turns into a container while the
        # limit has room: first-seen time per task, and which ones were logged.
        self._starved_since: dict[str, float] = {}
        self._phantom_logged: set[str] = set()
        self._items = self._load()
        self._recover_triggered()
        self.sync_skill_limits()

    # -- persistence ------------------------------------------------------ #
    def _load(self) -> list[dict[str, Any]]:
        raw = read_json(self.state_path, {})
        triggered = raw.get("triggered") if isinstance(raw, dict) else []
        self._triggered = [item for item in triggered if isinstance(item, dict)] if isinstance(triggered, list) else []
        self._lastStartedAt = str(raw.get("lastStartedAt") or "") if isinstance(raw, dict) else ""
        items = raw.get("items") if isinstance(raw, dict) else []
        if not isinstance(items, list):
            return []
        return [item for item in items if isinstance(item, dict)]

    def _save(self) -> None:
        atomic_write_json(
            self.state_path,
            {
                "items": self._items,
                "triggered": self._triggered[-200:],
                "lastStartedAt": self._lastStartedAt,
                "updatedAt": utc_now(),
            },
        )

    def _recover_triggered(self) -> None:
        known = {str(item.get("id") or "") for item in self._triggered}
        added = False
        candidates = sorted(
            self.jobs.jobs_dir.glob("*-platform-*.json"),
            key=lambda path: path.stat().st_mtime if path.exists() else 0,
            reverse=True,
        )
        for path in candidates:
            job = read_json(path, {})
            if not isinstance(job, dict):
                continue
            item_id = str(job.get("platformItemId") or "")
            if not item_id or item_id in known:
                continue
            result = read_json(Path(str(job.get("resultFile") or "")), {})
            if not isinstance(result, dict) or result.get("stage") not in {"desktop-submitted", "desktop-task-running"}:
                continue
            task_root = Path(str(result.get("taskRoot") or job.get("taskRoot") or ""))
            task_name = task_root.name
            trigger_path = Path(str(job.get("triggerPromptPath") or ""))
            if not task_name or not trigger_path.is_file():
                continue
            trigger_prompt = trigger_path.read_text(encoding="utf-8").strip()
            prompt = (
                "本次监控队列已分配唯一任务名。\n"
                f"- 唯一任务名：`{task_name}`\n"
                "- 必须使用该任务名创建独立目录，不得复用已存在目录。\n\n"
                f"{trigger_prompt}\n"
            )
            self._triggered.append({
                "id": item_id,
                "source": "platform",
                "taskRoot": str(task_root),
                "taskName": task_name,
                "projectCode": str(job.get("projectCode") or ""),
                "triggerPrompt": trigger_prompt,
                "promptSha256": str(result.get("promptSha256") or queue_prompt_sha256(prompt)),
                "triggeredAt": utc_now(),
                "removedReason": "triggered",
            })
            known.add(item_id)
            added = True
        self._triggered = self._triggered[-200:]
        if added:
            self._save()

    def triggered_items(self) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._triggered)

    # -- config helpers --------------------------------------------------- #
    def _automation_cfg(self) -> dict[str, Any]:
        return self.config.setdefault("automation", {})

    def _roots_locked(self) -> list[str]:
        return [str(Path(value).expanduser().resolve()) for value in self.config.get("roots") or []]

    def _active_roots_locked(self) -> list[str]:
        """The roots actually in effect — the selected folder's roots when one
        is chosen, otherwise every configured root.

        This used to intersect ``activeRoots`` with ``roots``, which silently
        dropped a folder whose path was not already listed in ``config.roots``.
        """
        monitor_cfg = self.config.setdefault("monitor", {})
        if "activeRoots" not in monitor_cfg:
            return self._roots_locked()
        resolved: list[str] = []
        for value in monitor_cfg.get("activeRoots") or []:
            path = str(Path(str(value)).expanduser().resolve())
            if path not in resolved:
                resolved.append(path)
        return resolved or self._roots_locked()

    def _default_scope_root_locked(self) -> str:
        active = self._active_roots_locked()
        roots = self._roots_locked()
        return (active or roots or [str(DEFAULT_ROOT)])[0]

    def _schedule_mode(self) -> str:
        value = str(self._automation_cfg().get("scheduleMode") or SCHEDULE_MODE_CONTAINERS).strip().lower()
        return value if value in SCHEDULE_MODES else SCHEDULE_MODE_CONTAINERS

    def _max_containers_limit(self) -> int:
        """The single container limit, shared with the skill's limiter."""
        return clamp_int(
            self._automation_cfg().get("maxContainers"),
            1,
            SKILL_ABSOLUTE_MAX_CONTAINERS,
            DEFAULT_MAX_CANDIDATE_CONTAINERS,
        )

    def sync_skill_limits(self) -> bool:
        """Push ``maxContainers`` / ``excludedProjectCodes`` to the skill."""
        return self.slots.write_limits(
            max_containers=self._max_containers_limit(),
            excluded=sorted(self._excluded_project_codes()),
        )

    def skill_limit_status(self) -> dict[str, Any]:
        """What the skill will actually enforce, read back from its file."""
        data = read_json(self.slots.limit_path, {})
        data = data if isinstance(data, dict) else {}
        managed = data.get("managedBy") == SKILL_LIMIT_MANAGED_BY
        value = data.get("maxContainers")
        return {
            "path": str(self.slots.limit_path),
            "managed": managed,
            "maxContainers": value,
            "inSync": managed and value == self._max_containers_limit(),
        }

    def _phantom_demand_seconds(self) -> int:
        return clamp_int(
            self._automation_cfg().get("phantomDemandSeconds"),
            MIN_PHANTOM_DEMAND_SECONDS,
            MAX_PHANTOM_DEMAND_SECONDS,
            DEFAULT_PHANTOM_DEMAND_SECONDS,
        )

    def _startup_timeout(self) -> int:
        return clamp_int(
            self._automation_cfg().get("startupTimeoutSeconds"),
            MIN_STARTUP_TIMEOUT_SECONDS,
            MAX_STARTUP_TIMEOUT_SECONDS,
            DEFAULT_STARTUP_TIMEOUT_SECONDS,
        )

    def _candidates_per_task(self) -> int:
        try:
            return max(1, int(self._automation_cfg().get("candidatesPerTask") or 2))
        except (TypeError, ValueError):
            return 2

    def _excluded_project_codes(self) -> set[str]:
        return {
            str(value).strip().casefold()
            for value in (self._automation_cfg().get("excludedProjectCodes") or [])
            if str(value).strip()
        }

    @staticmethod
    def _container_group_name(container_name: Any) -> str:
        text = str(container_name or "").strip()
        prefix = "sologsb-"
        if not text.startswith(prefix):
            return ""
        remainder = text[len(prefix):]
        marker = "-candidate-"
        if marker in remainder:
            return remainder.split(marker, 1)[0]
        return remainder

    @classmethod
    def _platform_job_group(cls, job: dict[str, Any]) -> str:
        task_root = JobManager._platform_job_task_root(job)
        if task_root is not None:
            return task_root.name
        return str(
            job.get("projectCode")
            or job.get("taskName")
            or job.get("platformItemId")
            or job.get("key")
            or ""
        )

    def _group_is_excluded(self, group: str) -> bool:
        value = str(group or "").casefold()
        return any(value == code or value.startswith(code + "-") for code in self._excluded_project_codes())

    def _job_is_excluded(self, job: dict[str, Any]) -> bool:
        code = str(job.get("projectCode") or "").casefold()
        return bool(code and code in self._excluded_project_codes()) or self._group_is_excluded(self._platform_job_group(job))

    # -- prompt / capacity estimation ------------------------------------- #
    def _estimated_containers(self, prompt: Any = "") -> int:
        match = re.search(r"预拉\s*([1-9][0-9]*)\s*份候选", str(prompt or ""))
        if match:
            return int(match.group(1))
        return self._candidates_per_task()

    def _job_estimated_containers(self, job: dict[str, Any]) -> int:
        try:
            direct = int(job.get("estimatedContainers") or 0)
        except (TypeError, ValueError):
            direct = 0
        if direct > 0:
            return direct
        prompt_path = Path(str(job.get("triggerPromptPath") or ""))
        prompt = self.file_cache.text(prompt_path) if prompt_path.is_file() else ""
        return self._estimated_containers(prompt)

    def _item_estimated_containers(self, item: dict[str, Any]) -> int:
        return self._estimated_containers(item.get("triggerPrompt") or "")

    # -- stop list -------------------------------------------------------- #
    def stop_markers(self) -> list[str]:
        data = read_json(STOP_TASKS_PATH, {})
        file_markers = data.get("tasks") if isinstance(data, dict) else data
        configured_markers = (self.config.get("automation") or {}).get("stopTasks") or []
        return [
            str(value).strip()
            for value in [
                *(file_markers if isinstance(file_markers, list) else []),
                *(configured_markers if isinstance(configured_markers, list) else []),
            ]
            if str(value).strip()
        ]

    def _item_is_stopped(self, item: dict[str, Any]) -> bool:
        code = str(item.get("projectCode") or "").strip().casefold()
        if not code:
            return False
        for marker in self.stop_markers():
            value = marker.casefold()
            if value == code or value.startswith(code + "-"):
                return True
        return False

    # -- item predicates -------------------------------------------------- #
    @staticmethod
    def _item_holds_slot(item: dict[str, Any]) -> bool:
        return str(item.get("status") or "") in QUEUE_ACTIVE_STATUSES or bool(item.get("capacityHeld"))

    def live_task_reason(self, item: dict[str, Any]) -> str:
        """Describe work that still makes releasing this slot unsafe."""
        result_file = Path(str(item.get("resultFile") or ""))
        result = read_json(result_file, {}) if result_file else {}
        if not isinstance(result, dict):
            result = {}
        raw_root = str(result.get("taskRoot") or item.get("taskRoot") or "")
        task_root = Path(raw_root).expanduser().resolve() if raw_root else None
        job = self.jobs.get_platform(str(item.get("id") or ""), str(item.get("runKey") or ""))
        worker_alive = bool(job and job.get("status") == "running" and persisted_job_process_alive(job))
        latest = self._latest_activity_timestamp(item={}, job=None, task_root=task_root, result_file=result_file)
        idle = (time.time() - latest) if latest is not None else None
        dead_after = self._orphan_grace_seconds() * 2
        state_status = ""
        if task_root is not None:
            state = read_json(task_root / "monitor" / "state.json", {})
            if isinstance(state, dict):
                state_status = str(state.get("status") or "")
            if state_status and state_status not in (TERMINAL_TASK_STATUSES | TASK_FAILURE_STATUSES):
                # A non-terminal state with no writes to state or trajectories
                # for twice the orphan grace, no worker and no containers is a
                # dead desktop session; holding its slot forever starves the
                # queue.
                if worker_alive or idle is None or idle < dead_after:
                    return f"桌面任务仍处于 {state_status}"
        stage = str(result.get("stage") or "")
        # The stage in result.json is the worker's last word.  Once the worker
        # is gone it proves nothing: a task root without state.json after that
        # never started (gb-538 held a slot for twelve hours on this).
        if not state_status and worker_alive and stage in {"desktop-submitted", "desktop-task-running", "wait-timeout"}:
            return f"任务执行阶段仍为 {stage}，尚未确认停止"
        if task_root is not None and self.docker_cache is not None:
            try:
                names = task_container_names(task_root.name, self.docker_cache.get())
            except Exception:
                names = []
            if names:
                return f"仍有 {len(names)} 个候选容器运行"
        if worker_alive:
            return "监控执行器仍在运行"
        return ""

    def _orphan_grace_seconds(self) -> float:
        try:
            return max(60.0, float(self._automation_cfg().get("orphanGraceSeconds") or DEFAULT_ORPHAN_GRACE_SECONDS))
        except (TypeError, ValueError):
            return float(DEFAULT_ORPHAN_GRACE_SECONDS)

    # -- stale task closure ------------------------------------------------ #
    @staticmethod
    def _state_records(state: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
        """``(side, record)`` for every side and candidate entry in a state file."""
        out: list[tuple[str, dict[str, Any]]] = []
        for group in ("sides", "candidates"):
            records = state.get(group)
            if not isinstance(records, dict):
                continue
            for name, record in records.items():
                if isinstance(record, dict):
                    out.append((str(name), record))
        return out

    @classmethod
    def _state_active_for_skill(cls, state: dict[str, Any]) -> bool:
        """Mirror of the skill's ``project_claims._task_is_active``."""
        if str(state.get("status") or "").strip().casefold() in SKILL_ACTIVE_TASK_STATUSES:
            return True
        return any(
            str(record.get("status") or "").strip().casefold() in SKILL_ACTIVE_SIDE_STATUSES
            for _name, record in cls._state_records(state)
        )

    def _task_root_owned_locked(self, task_root: Path) -> str:
        """Why a task dir must not be closed: a live owner, or ``""``."""
        for item in self._items:
            if str(item.get("status") or "") not in QUEUE_ACTIVE_STATUSES:
                continue
            raw = str(item.get("taskRoot") or "")
            if raw and Path(raw).expanduser().resolve() == task_root:
                return f"队列项 {item.get('id')} 仍为 {item.get('status')}"
        for job in self.jobs.running():
            roots = {JobManager._platform_job_task_root(job)}
            reserved = str(job.get("reservedTaskRoot") or "")
            if reserved:
                roots.add(Path(reserved).expanduser().resolve())
            if task_root in roots:
                return f"执行器 PID={job.get('pid')} 仍在运行"
        return ""

    def _close_stale_project_tasks_locked(self, project_code: str, workdir: Path) -> list[str]:
        """Close abandoned task dirs of ``project_code`` under ``workdir``.

        The skill's ``init`` refuses a project when an earlier task in the same
        workdir is still ``running``/``blocked`` (or has such a candidate): that
        is its concurrency gate, and it has no liveness check.  A task whose
        executor is gone, with no containers and no queue item owning it, will
        never be resumed — the monitor always retries in a fresh task dir — so
        every retry of that project would die at the gate (cy-381 on 09-23).
        Marking the old task ``failed`` (candidates ``invalidated``) releases the
        gate while keeping the original status and reason on the record.
        """
        code = str(project_code or "").strip().casefold()
        if not code or not workdir.is_dir():
            return []
        docker = self.docker_cache.get() if self.docker_cache is not None else {"items": []}
        if docker.get("error"):
            # Without a container list a live candidate could be mistaken for
            # an abandoned one; the launch gate already holds in that case.
            return []
        closed: list[str] = []
        for state_path in sorted(workdir.glob("*/monitor/state.json")):
            task_root = state_path.parent.parent.resolve()
            state = read_json(state_path, {})
            if not isinstance(state, dict) or not state:
                continue
            source = state.get("source") if isinstance(state.get("source"), dict) else {}
            if str(source.get("projectCode") or "").strip().casefold() != code:
                continue
            if not self._state_active_for_skill(state):
                continue
            try:
                quiet_for = time.time() - state_path.stat().st_mtime
            except OSError:
                continue
            if quiet_for < STALE_TASK_CLOSE_MIN_AGE_SECONDS:
                continue
            owner = self._task_root_owned_locked(task_root)
            if owner:
                continue
            if task_container_names(task_root.name, docker):
                continue
            if any(
                runner_pid_alive(record, task_root, side)
                for side, record in self._state_records(state)
            ):
                continue
            previous = str(state.get("status") or "")
            now = utc_now()
            reason = (
                f"执行器已退出、无候选容器、无队列项持有，旧状态 {previous} 会让技能拒绝 "
                f"{project_code} 的新任务接入；监控台在启动新任务前关闭该旧任务"
            )
            for _side, record in self._state_records(state):
                side_status = str(record.get("status") or "").strip().casefold()
                if side_status in SKILL_ACTIVE_SIDE_STATUSES:
                    record["previousStatus"] = record.get("status")
                    record["status"] = "invalidated"
                    record["closedAt"] = now
            state.update({
                "status": "failed",
                "previousStatus": previous,
                "closedBy": SKILL_LIMIT_MANAGED_BY,
                "closedAt": now,
                "closedReason": reason,
                "updatedAt": now,
            })
            try:
                atomic_write_json(state_path, state)
            except OSError as exc:
                self._emit("warning", "queue.stale_task_close_failed", projectCode=project_code,
                           detail=f"{task_root.name}: {exc}")
                continue
            closed.append(str(task_root))
            self._emit("info", "queue.stale_task_closed", projectCode=project_code,
                       detail=f"{task_root.name}: {previous} → failed（静默 {int(quiet_for)} 秒），放行同项目新任务")
        return closed

    @staticmethod
    def _job_task_status(job: dict[str, Any]) -> str:
        task_root = JobManager._platform_job_task_root(job)
        if task_root is None:
            return ""
        state = read_json(task_root / "monitor" / "state.json", {})
        return str(state.get("status") or "") if isinstance(state, dict) else ""

    def _job_task_terminal(self, job: dict[str, Any]) -> bool:
        if job.get("status") == "running" and persisted_job_process_alive(job):
            return False
        return self._job_task_status(job) in TERMINAL_TASK_STATUSES

    @staticmethod
    def _job_candidate_phase_finished(job: dict[str, Any]) -> bool:
        """Whether the task no longer needs candidate containers.

        A queue worker can stay alive long after the candidate race while
        semantic review, publishing, verification or recording continues.  Those
        phases must not keep a container slot and block the next task.
        """
        task_root = JobManager._platform_job_task_root(job)
        if task_root is None:
            return False
        state = read_json(task_root / "monitor" / "state.json", {})
        if not isinstance(state, dict):
            return False
        if state.get("candidateRaceFinishedAt") or state.get("candidateMapping"):
            return True
        return str(state.get("status") or "") in CANDIDATE_PHASE_DONE_STATUSES

    def _terminal_state_stable(
        self,
        item: dict[str, Any],
        task_root: Path | None,
        state_status: str,
    ) -> tuple[bool, bool]:
        if task_root is None:
            return False, False
        try:
            stability = max(0.0, float(self._automation_cfg().get("terminalStabilitySeconds") or 6))
        except (TypeError, ValueError):
            stability = 6.0
        signature = f"{task_root.resolve()}::{state_status}"
        dirty = False
        if item.get("terminalSignature") != signature:
            item["terminalSignature"] = signature
            item["terminalSeenAt"] = utc_now()
            dirty = True
        try:
            stable_on_disk = time.time() - (task_root / "monitor" / "state.json").stat().st_mtime >= stability
        except OSError:
            stable_on_disk = False
        if stability <= 0 or stable_on_disk:
            return True, dirty
        seen = parse_time(item.get("terminalSeenAt"))
        return bool(seen and time.time() - seen.timestamp() >= stability), dirty

    def _job_in_active_roots(self, job: dict[str, Any]) -> bool:
        if job.get("source") != "platform":
            return True
        task_root = JobManager._platform_job_task_root(job)
        if task_root is None:
            return True
        roots = [Path(value) for value in self._active_roots_locked()]
        if not roots:
            return False
        return any(task_root == root or root in task_root.parents for root in roots)

    def _queue_reservation_jobs_locked(self) -> list[dict[str, Any]]:
        active_roots = set(self._active_roots_locked())
        reservations: list[dict[str, Any]] = []
        seen: set[str] = set()
        records = [
            *((item, False) for item in self._items),
            *((item, True) for item in self._triggered),
        ]
        for item, from_triggered in records:
            item_id = str(item.get("id") or "")
            if not item_id or item_id in seen or item.get("source") != "platform":
                continue
            if not self._item_holds_slot(item):
                continue
            scope_root = str(item.get("scopeRoot") or "")
            if scope_root and scope_root not in active_roots:
                continue
            result_file = Path(str(item.get("resultFile") or ""))
            result = read_json(result_file, {}) if result_file else {}
            if not isinstance(result, dict):
                result = {}
            raw_root = str(result.get("taskRoot") or item.get("taskRoot") or "")
            task_root = Path(raw_root).expanduser().resolve() if raw_root else None
            if from_triggered and task_root is None and not scope_root:
                continue
            if task_root is not None and active_roots:
                if not any(task_root == Path(root) or Path(root) in task_root.parents for root in active_roots):
                    continue
            state_status = self._job_task_status({
                "taskRoot": str(task_root) if task_root else "",
                "resultFile": str(result_file) if result_file else "",
            })
            if state_status in TERMINAL_TASK_STATUSES:
                continue
            if str(item.get("status") or "") == "failed" and not item.get("capacityHeld"):
                continue
            seen.add(item_id)
            reservations.append({
                "key": f"platform:{item_id}",
                "source": "platform",
                "platformItemId": item_id,
                "taskRoot": str(task_root) if task_root else "",
                "taskName": str(item.get("taskName") or item.get("projectName") or item_id),
                "projectCode": str(item.get("projectCode") or ""),
                "status": "running",
                "startedAt": str(item.get("startedAt") or item.get("addedAt") or utc_now()),
                "resultFile": str(result_file) if result_file else "",
                "triggerPromptPath": str(item.get("triggerPromptPath") or ""),
                "estimatedContainers": self._item_estimated_containers(item),
                "queueHold": True,
            })
        return reservations

    # -- capacity accounting ---------------------------------------------- #
    def _base_capacity_detail(self, mode: str, error: str = "") -> dict[str, Any]:
        return {
            "mode": mode,
            "dockerReady": False,
            "containerGroups": [],
            "startupReservations": [],
            "nonTestContainerCount": 0,
            "nonTestContainerGroups": 0,
            "estimatedNonTestContainers": 0,
            "excludedProjectCodes": sorted(self._excluded_project_codes()),
            "activeJobKeys": [],
            "error": error,
        }

    def _capacity_usage_locked(self, startup_timeout: int) -> tuple[int, dict[str, Any]]:
        jobs_by_key = {
            str(job.get("key") or f"job:{index}"): job
            for index, job in enumerate(self.jobs.running())
            if self._job_in_active_roots(job)
        }
        for reservation in self._queue_reservation_jobs_locked():
            jobs_by_key.setdefault(str(reservation.get("key") or ""), reservation)
        running_jobs = list(jobs_by_key.values())

        def platform_job_active(job: dict[str, Any]) -> bool:
            if job.get("source") != "platform":
                return True
            if self._job_task_terminal(job):
                return False
            if bool(job.get("queueHold")):
                return True
            if self.jobs.platform_job_started(job):
                return True
            return (age_seconds(job.get("startedAt")) or 0) < startup_timeout

        if self.docker_cache is None:
            active_jobs = [
                job for job in running_jobs
                if not self._job_is_excluded(job) and platform_job_active(job)
            ]
            detail = self._base_capacity_detail("jobs")
            detail["estimatedNonTestContainers"] = sum(self._job_estimated_containers(job) for job in active_jobs)
            detail["activeJobKeys"] = [str(job.get("key") or "") for job in active_jobs]
            return len(active_jobs), detail

        docker = self.docker_cache.get()
        if docker.get("error"):
            active_jobs = [
                job for job in running_jobs
                if not self._job_is_excluded(job) and platform_job_active(job)
            ]
            detail = self._base_capacity_detail("fallback", str(docker.get("error") or ""))
            detail["estimatedNonTestContainers"] = sum(self._job_estimated_containers(job) for job in active_jobs)
            detail["activeJobKeys"] = [str(job.get("key") or "") for job in active_jobs]
            return len(active_jobs), detail

        group_container_counts: dict[str, int] = {}
        for item in docker.get("items") or []:
            if not isinstance(item, dict) or str(item.get("state") or "").lower() != "running":
                continue
            group = self._container_group_name(item.get("name"))
            if group:
                group_container_counts[group] = group_container_counts.get(group, 0) + 1
        container_groups = sorted(group_container_counts)
        group_set = set(container_groups)
        non_test_groups = [group for group in container_groups if not self._group_is_excluded(group)]
        non_test_containers = sum(group_container_counts[group] for group in non_test_groups)

        # Container demand = running containers + what each live task still
        # needs but has not started yet (candidates queued in the skill's
        # limiter, or a task that has not reached the candidate race).  This is
        # computed from the task's own state.json instead of placeholder marker
        # files: placeholders were counted by the skill's limiter too, so a
        # freshly claimed task's candidates queued behind its own placeholders.
        pending_demand = 0
        active_tasks = 0
        startup_reservations: list[str] = []
        active_job_keys: list[str] = []
        # The skill's limiter hands a free slot to a queued candidate within
        # seconds.  A candidate still marked ``running`` without a container
        # while the limit has room belongs to a task whose executor is gone; its
        # demand would otherwise block launches until the orphan grace expires.
        limit = self._max_containers_limit()
        has_room = non_test_containers < limit
        phantom_after = self._phantom_demand_seconds()
        starved_now: set[str] = set()
        phantom: list[str] = []
        for job in running_jobs:
            key = str(job.get("key") or "")
            if self._job_is_excluded(job):
                continue
            if job.get("source") != "platform":
                active_tasks += 1
                active_job_keys.append(key)
                continue
            if self._job_task_terminal(job):
                continue
            task_root = JobManager._platform_job_task_root(job)
            task_name = task_root.name if task_root is not None else ""
            matched_group = task_name if task_name in group_set else ""
            project_code = str(job.get("projectCode") or "").casefold()
            if not matched_group and project_code and not task_name:
                matched_group = next(
                    (group for group in container_groups if group.casefold().startswith(project_code + "-")),
                    "",
                )
            started = self.jobs.platform_job_started(job)
            age = age_seconds(job.get("startedAt"))
            if not (matched_group or started or job.get("queueHold") or age is None or age < startup_timeout):
                continue
            active_tasks += 1
            active_job_keys.append(key)
            demand = self._job_container_demand(job, task_root, startup_timeout=startup_timeout)
            running_here = group_container_counts.get(matched_group, 0) if matched_group else 0
            extra = max(0, demand - running_here)
            label = task_name or key
            initialised = task_root is not None and (task_root / "monitor" / "state.json").is_file()
            if extra and has_room and initialised:
                starved_now.add(label)
                waited = time.time() - self._starved_since.setdefault(label, time.time())
                if waited >= phantom_after:
                    phantom.append(label)
                    if label not in self._phantom_logged:
                        self._phantom_logged.add(label)
                        self._emit(
                            "warning",
                            "queue.phantom_demand",
                            projectCode=str(job.get("projectCode") or ""),
                            detail=(
                                f"{label} 有 {extra} 个候选标记为 running，但容器 {non_test_containers}/{limit} "
                                f"未满的情况下 {int(waited)} 秒仍未拿到容器，判定执行器已退出，不再计入占用"
                            ),
                        )
                    extra = 0
            if extra:
                pending_demand += extra
                startup_reservations.append(label)

        for label in list(self._starved_since):
            if label not in starved_now:
                self._starved_since.pop(label, None)
                self._phantom_logged.discard(label)

        return active_tasks, {
            "mode": "containers",
            "dockerReady": True,
            "containerGroups": container_groups,
            "nonTestContainerGroups": non_test_groups,
            "nonTestContainerCount": non_test_containers,
            "pendingContainerDemand": pending_demand,
            "estimatedNonTestContainers": non_test_containers + pending_demand,
            "excludedProjectCodes": sorted(self._excluded_project_codes()),
            "startupReservations": sorted(startup_reservations),
            "phantomDemand": sorted(phantom),
            "activeJobKeys": active_job_keys,
            "error": "",
        }

    def _job_container_demand(self, job: dict[str, Any], task_root: Path | None, *, startup_timeout: int) -> int:
        """How many candidate containers a live task needs right now."""
        state = read_json(task_root / "monitor" / "state.json", {}) if task_root is not None else {}
        if not isinstance(state, dict) or not state:
            # Not initialised yet: it will start a full candidate batch — but
            # only within the startup window.  The skill writes state.json
            # before any container, so an older task without one never will.
            age = age_seconds(job.get("startedAt"))
            if age is not None and age >= startup_timeout:
                return 0
            return self._job_estimated_containers(job)
        if state.get("candidateRaceFinishedAt") or state.get("candidateMapping"):
            return 0
        status = str(state.get("status") or "")
        candidates = state.get("candidates") if isinstance(state.get("candidates"), dict) else {}
        if candidates:
            # ``running`` covers both a live container and a candidate waiting
            # for a slot in the skill's limiter.
            return sum(
                1 for record in candidates.values()
                if isinstance(record, dict) and str(record.get("status") or "") == "running"
            )
        if status in CANDIDATE_PHASE_DONE_STATUSES:
            return 0
        return self._job_estimated_containers(job)

    def container_usage(self) -> dict[str, Any]:
        """Container accounting for the top bar.

        A running ``sologsb-`` container counts only when its group belongs to a
        task this monitor knows about — a discovered task directory or a queue
        item's task directory.  Unrelated containers that merely share the
        ``sologsb-`` prefix no longer inflate the number, which is what the old
        ``excludedProjectCodes``-only filter got wrong.
        """
        startup_timeout = self._startup_timeout()
        with self._lock:
            _in_use, detail = self._capacity_usage_locked(startup_timeout)
            known_groups: set[str] = set()
            for item in self._items:
                root = str(item.get("taskRoot") or "")
                if root:
                    known_groups.add(Path(root).name)
            for item in self._triggered:
                root = str(item.get("taskRoot") or "")
                if root:
                    known_groups.add(Path(root).name)
        known_groups |= {
            Path(root).name
            for root in discover_task_roots(self._active_roots_locked(), file_cache=self.file_cache)
        }

        docker = self.docker_cache.get() if self.docker_cache else {"items": [], "error": ""}
        running: list[str] = []
        attributed = 0
        excluded = 0
        for item in docker.get("items") or []:
            name = str(item.get("name") or "")
            if not name.startswith("sologsb-") or str(item.get("state") or "").lower() != "running":
                continue
            running.append(name)
            group = self._container_group_name(name)
            if any(group == known or group.startswith(known + "-") for known in known_groups if known):
                attributed += 1
                # Test projects (excludedProjectCodes) run outside the limit in
                # both the skill's limiter and the launch gate; showing them
                # against the limit read as "6 / 4".
                if self._group_is_excluded(group):
                    excluded += 1

        slots = self.slots.snapshot()
        hard_limit = self._max_containers_limit()
        # The same numbers the launch gate uses: containers still needed by
        # live tasks (queued candidates, tasks not yet at the race).
        reserved = int(detail.get("pendingContainerDemand") or 0)
        return {
            "running": attributed,
            "runningAll": len(running),
            "excluded": excluded,
            "counted": attributed - excluded,
            "reserved": reserved,
            "used": attributed - excluded + reserved,
            "phantom": detail.get("phantomDemand") or [],
            "hardLimit": hard_limit,
            "groups": detail.get("containerGroups") or [],
            "slots": slots.get("occupied") or [],
            "dockerReady": bool(detail.get("dockerReady")),
            "dockerError": str(detail.get("error") or docker.get("error") or ""),
            "scheduleMode": self._schedule_mode(),
        }

    # -- disk ------------------------------------------------------------- #
    def disk_free_gb(self) -> float | None:
        """Free space where new tasks are written (lowest among active roots)."""
        values = [value for value in (free_gb(root) for root in self._active_roots_locked() or self._roots_locked())
                  if value is not None]
        return min(values) if values else None

    def disk_status(self) -> dict[str, Any]:
        free = self.disk_free_gb()
        return {
            "freeGB": round(free, 1) if free is not None else None,
            "minFreeGB": housekeeping_settings(self.config)["minFreeGB"],
            "low": self._disk_low,
            **self.housekeeping_status,
        }

    # -- snapshots -------------------------------------------------------- #
    def active_roots(self) -> list[str]:
        with self._lock:
            return self._active_roots_locked()

    def fast_snapshot(self) -> dict[str, Any]:
        with self._lock:
            items = self._public_items_locked()
            roots = self._roots_locked()
            active_roots = self._active_roots_locked()
            last_started = self._lastStartedAt
            capacity = int(self._automation_cfg().get("capacity") or 2)
            cooldown = max(0, int(self._automation_cfg().get("cooldownSeconds") or 0))
            paused = bool(self._automation_cfg().get("paused", True))
            prompt_template = str(self._automation_cfg().get("promptTemplate") or "")
            mode = self._schedule_mode()
            max_tasks = capacity
            max_containers = self._max_containers_limit()
            candidates_per_task = self._candidates_per_task()
            startup_timeout = self._startup_timeout()
            reconcile_seconds = clamp_int(self._automation_cfg().get("reconcileSeconds"), 15, 3600, DEFAULT_RECONCILE_SECONDS)
        counts = {
            "pending": sum(1 for item in items if item.get("status") == "pending"),
            "running": sum(1 for item in items if item.get("status") in QUEUE_ACTIVE_STATUSES),
            "done": sum(1 for item in items if item.get("status") == "done"),
            "failed": sum(1 for item in items if item.get("status") == "failed"),
            "skipped": sum(1 for item in items if item.get("status") == "skipped"),
            "jobsRunning": 0,
            "jobsActive": 0,
            "jobsStale": 0,
            "containerGroups": 0,
            "nonTestContainerCount": 0,
            "nonTestContainerGroups": 0,
            "estimatedNonTestContainers": 0,
            "startupReservations": 0,
            "quotaClaimed": sum(1 for item in items if (item.get("quota") or {}).get("state") == "claimed"),
            "quotaRefunded": sum(1 for item in items if (item.get("quota") or {}).get("state") == "refunded"),
        }
        return {
            "roots": roots,
            "activeRoots": active_roots,
            "capacity": capacity,
            "maxContainers": max_containers,
            "maxTasks": max_tasks,
            "candidatesPerTask": candidates_per_task,
            "scheduleMode": mode,
            "startupTimeoutSeconds": startup_timeout,
            "skillLimit": self.skill_limit_status(),
            "reconcileSeconds": reconcile_seconds,
            "startupGraceSeconds": self.startup_grace_seconds(),
            "mergeProjectPool": bool((self.config.get("platform") or {}).get("mergeProjectPool", False)),
            "anthropicBaseUrl": str(self._automation_cfg().get("anthropicBaseUrl") or "https://llm2.jzxhnh.com"),
            "cooldownSeconds": cooldown,
            "stalledTaskRetrySeconds": int(self._stalled_retry_settings()[0]),
            "stalledTaskRetryLimit": int(self._stalled_retry_settings()[1]),
            "cooldownRemainingSeconds": 0,
            "lastStartedAt": last_started,
            "paused": paused,
            "pauseOnStart": {True: "always", False: "never"}.get(self._automation_cfg().get("pauseOnStart", "crash-loop"), str(self._automation_cfg().get("pauseOnStart") or "crash-loop")),
            "promptTemplate": prompt_template,
            "items": items,
            "counts": counts,
            "capacityInUse": counts["running"],
            "autoRefill": {**public_auto_refill_config(self.config), "lastRun": dict(self.refill_status)},
            "guard": dict(self.guard_status),
            "disk": self.disk_status(),
            "capacityMode": "fast",
            "containerGroups": [],
            "startupReservations": [],
            "updatedAt": utc_now(),
        }

    def _public_items_locked(self) -> list[dict[str, Any]]:
        """Queue items without the multi-kilobyte rendered prompt.

        The prompt is only needed when the operator copies it, so it lives behind
        :meth:`item_prompt` instead of in every snapshot.
        """
        public: list[dict[str, Any]] = []
        for item in self._items:
            entry = copy.deepcopy(item)
            prompt = str(entry.pop("triggerPrompt", "") or "")
            entry["triggerPromptLength"] = len(prompt)
            if prompt:
                entry["triggerPromptSha256"] = queue_prompt_sha256(prompt)
            public.append(entry)
        return public

    def item_prompt(self, item_id: str) -> str:
        with self._lock:
            item = next((value for value in self._items if str(value.get("id") or "") == item_id), None)
            return str((item or {}).get("triggerPrompt") or "")

    # -- presentation ------------------------------------------------------ #
    @staticmethod
    def _annotate_phase(item: dict[str, Any], docker: dict[str, Any], *, queue_position: int) -> None:
        """Attach ``phase`` and ``containers`` for the queue page.

        ``status`` is the state machine's word; ``phase`` is the operator's:
        is the item still waiting (for its turn, for a container slot, for the
        executor to start), or has it been handed over and is running with
        its containers up?  The page groups on ``phase`` so a task that is
        simply executing no longer sits in the queue looking like a fault.

        Phases: ``queued`` ``waiting_slot`` ``retrying`` ``starting``
        ``executing`` ``attention`` ``done`` ``failed`` ``skipped``.
        """
        status = str(item.get("status") or "")
        raw_root = str(item.get("taskRoot") or "")
        task_root = Path(raw_root).expanduser() if raw_root else None
        state = read_json(task_root / "monitor" / "state.json", {}) if task_root else {}
        state = state if isinstance(state, dict) else {}
        state_status = str(state.get("status") or "")
        candidates = state.get("candidates") if isinstance(state.get("candidates"), dict) else {}
        # ``running`` covers a live container and a candidate queued in the
        # skill's limiter; the docker list tells the two apart.
        wanted = sum(
            1 for record in candidates.values()
            if isinstance(record, dict) and str(record.get("status") or "") == "running"
        )
        running_here = len(task_container_names(task_root.name, docker)) if task_root else 0
        race_done = bool(
            state.get("candidateRaceFinishedAt")
            or state.get("candidateMapping")
            or (state_status and state_status in CANDIDATE_PHASE_DONE_STATUSES)
        )
        item["containers"] = {"running": running_here, "wanted": wanted}
        # The monitor page addresses tasks by this id (tasks.py card["id"]).
        item["monitorTaskId"] = short_hash(str(task_root.resolve())) if task_root and state else ""
        if state_status:
            item["stateStatus"] = state_status
        item["queuePosition"] = queue_position if status == "pending" else 0

        if status in QUEUE_TERMINAL_STATUSES:
            phase = status
        elif status == "orphaned":
            phase = "attention"
        elif status == "pending":
            next_attempt = parse_time(item.get("nextAttemptAt"))
            if next_attempt and next_attempt.timestamp() > time.time():
                phase = "retrying"
            elif item.get("containerWait") and item.get("containerWaitKind") == "capacity":
                phase = "waiting_slot"
            else:
                phase = "queued"
        elif status in {"launching", "running"}:
            phase = "starting"
        elif not state:
            # Desktop task created; the skill has not written state.json yet.
            phase = "starting"
        elif race_done or running_here >= max(1, wanted):
            phase = "executing"
        elif wanted and running_here < wanted:
            # Candidates exist but not all have a container: they are queued
            # in the skill's limiter behind other tasks' containers.
            phase = "waiting_slot"
        else:
            phase = "starting"
        item["phase"] = phase

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._sync_running_locked()
            items = self._public_items_locked()
            docker = self.docker_cache.get() if self.docker_cache is not None else {"items": []}
            position = 0
            for item in items:
                if item.get("status") == "pending":
                    position += 1
                self._annotate_phase(item, docker, queue_position=position)
                if item.get("capacityHeld") and item.get("status") in {"orphaned", "failed"}:
                    item["notice"] = str(item.get("notice") or "已保留并发名额，未确认桌面任务已停止")
                    continue
                if item.get("status") != "pending":
                    continue
                if item.get("containerWait"):
                    item["notice"] = str(item.get("containerWait") or "")
                    item["error"] = ""
                elif item.get("nextAttemptAt") and item.get("error"):
                    # "等待自动重试（第 N 次，D 秒）" from apply_failure; the cause
                    # stays in ``lastError`` for the page to show alongside.
                    item["notice"] = str(item.get("error") or "等待自动重试")
                    item["error"] = ""
            roots = self._roots_locked()
            active_roots = self._active_roots_locked()
            startup_timeout = self._startup_timeout()
            running_jobs = self.jobs.running()
            stale_jobs = [
                job for job in running_jobs
                if job.get("source") == "platform"
                and not self.jobs.platform_job_started(job)
                and (age_seconds(job.get("startedAt")) or 0) >= startup_timeout
            ]
            capacity_in_use, capacity_detail = self._capacity_usage_locked(startup_timeout)
        pending = sum(1 for item in items if item.get("status") == "pending")
        active = sum(1 for item in items if item.get("status") in QUEUE_ACTIVE_STATUSES)
        phases: dict[str, int] = {}
        for item in items:
            phase = str(item.get("phase") or "")
            phases[phase] = phases.get(phase, 0) + 1
        cooldown_seconds = max(0, int(self._automation_cfg().get("cooldownSeconds") or 0))
        started_at = parse_time(self._lastStartedAt)
        cooldown_remaining = (
            max(0.0, cooldown_seconds - (time.time() - started_at.timestamp()))
            if started_at and cooldown_seconds
            else 0.0
        )
        return {
            "roots": roots,
            "activeRoots": active_roots,
            "capacity": int(self._automation_cfg().get("capacity") or 2),
            "maxContainers": self._max_containers_limit(),
            "maxTasks": int(self._automation_cfg().get("capacity") or 2),
            "candidatesPerTask": self._candidates_per_task(),
            "scheduleMode": self._schedule_mode(),
            "startupTimeoutSeconds": startup_timeout,
            "skillLimit": self.skill_limit_status(),
            "reconcileSeconds": clamp_int(self._automation_cfg().get("reconcileSeconds"), 15, 3600, DEFAULT_RECONCILE_SECONDS),
            "startupGraceSeconds": self.startup_grace_seconds(),
            "mergeProjectPool": bool((self.config.get("platform") or {}).get("mergeProjectPool", False)),
            "anthropicBaseUrl": str(self._automation_cfg().get("anthropicBaseUrl") or "https://llm2.jzxhnh.com"),
            "cooldownSeconds": cooldown_seconds,
            "stalledTaskRetrySeconds": int(self._stalled_retry_settings()[0]),
            "stalledTaskRetryLimit": int(self._stalled_retry_settings()[1]),
            "cooldownRemainingSeconds": round(cooldown_remaining, 1),
            "lastStartedAt": self._lastStartedAt,
            "paused": bool(self._automation_cfg().get("paused", True)),
            "pauseOnStart": {True: "always", False: "never"}.get(self._automation_cfg().get("pauseOnStart", "crash-loop"), str(self._automation_cfg().get("pauseOnStart") or "crash-loop")),
            "promptTemplate": str(self._automation_cfg().get("promptTemplate") or ""),
            "items": items,
            "counts": {
                "pending": pending,
                "running": active,
                "phases": phases,
                "done": sum(1 for item in items if item.get("status") == "done"),
                "failed": sum(1 for item in items if item.get("status") == "failed"),
                "skipped": sum(1 for item in items if item.get("status") == "skipped"),
                "jobsRunning": len(running_jobs),
                "jobsActive": len(capacity_detail.get("activeJobKeys") or []),
                "jobsStale": len(stale_jobs),
                "containerGroups": len(capacity_detail.get("containerGroups") or []),
                "nonTestContainerCount": int(capacity_detail.get("nonTestContainerCount") or 0),
                "nonTestContainerGroups": len(capacity_detail.get("nonTestContainerGroups") or []),
                "estimatedNonTestContainers": int(capacity_detail.get("estimatedNonTestContainers") or 0),
                "startupReservations": len(capacity_detail.get("startupReservations") or []),
                "quotaClaimed": sum(1 for item in items if (item.get("quota") or {}).get("state") == "claimed"),
                "quotaRefunded": sum(1 for item in items if (item.get("quota") or {}).get("state") == "refunded"),
            },
            "capacityInUse": capacity_in_use,
            "autoRefill": {**public_auto_refill_config(self.config), "lastRun": dict(self.refill_status)},
            "guard": dict(self.guard_status),
            "disk": self.disk_status(),
            "capacityMode": capacity_detail.get("mode"),
            "containerGroups": capacity_detail.get("containerGroups") or [],
            "startupReservations": capacity_detail.get("startupReservations") or [],
            "updatedAt": utc_now(),
        }

    # -- mutation --------------------------------------------------------- #
    def add(self, task_root: Path, side: str = "both") -> dict[str, Any]:
        task_root = task_root.expanduser().resolve()
        if not (task_root / "monitor" / "state.json").is_file():
            raise MonitorError(f"不是有效的 sologsb 任务目录: {task_root}")
        side = str(side).upper()
        if side == "BOTH":
            side = "both"
        if side not in {"A", "B", "both"}:
            raise MonitorError("队列 side 只能为 A、B 或 both")
        with self._lock:
            if any(
                Path(item.get("taskRoot") or "").resolve() == task_root
                and str(item.get("side") or "").lower() == side.lower()
                and item.get("status") in {"pending", "running"}
                for item in self._items
            ):
                raise MonitorError("该任务和侧已经在队列中")
            state = read_json(task_root / "monitor" / "state.json", {})
            item = {
                "id": f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
                "taskRoot": str(task_root),
                "taskName": str(state.get("taskName") or task_root.name),
                "projectCode": "",
                "side": side,
                "status": "pending",
                "addedAt": utc_now(),
                "startedAt": "",
                "finishedAt": "",
                "jobPid": "",
                "error": "",
            }
            self._items.append(item)
            self._save()
            return copy.deepcopy(item)

    def add_platform(
        self,
        project: dict[str, Any],
        *,
        task_type: str = "0-1代码生成",
        difficulty: str = "困难",
        side: str = "both",
        trigger_prompt: str = "",
        folder_id: str = "",
        folder_path: str = "",
        allow_duplicate: bool = False,
    ) -> dict[str, Any]:
        project_code = str(project.get("code") or "").strip()
        if not project_code:
            raise MonitorError("平台项目缺少 code")
        # Compare case-folded on both sides: the blocklist is written by the
        # settings store and the project code by the platform, and they do not
        # agree on casing.
        blocked = {str(code).strip().casefold() for code in self.blocked_codes if str(code).strip()}
        if blocked and project_code.casefold() in blocked:
            raise MonitorError(f"项目 {project_code} 已被手动禁用，不能入队")
        side = str(side).lower()
        if side not in {"a", "b", "both"}:
            raise MonitorError("队列 side 只能为 A、B 或 both")
        side = side.upper() if side in {"a", "b"} else "both"
        with self._lock:
            same_project = [
                item for item in self._items
                if item.get("source") == "platform"
                and str(item.get("projectCode") or "").casefold() == project_code.casefold()
            ]
            if allow_duplicate:
                if any(item.get("status") in QUEUE_ACTIVE_STATUSES or item.get("capacityHeld") for item in same_project):
                    raise MonitorError(f"项目 {project_code} 已有任务正在运行")
            elif any(item.get("status") in ({"pending"} | QUEUE_ACTIVE_STATUSES) for item in same_project):
                raise MonitorError(f"项目 {project_code} 已在队列中或正在运行")
            quota_before = project.get("quotaBefore") if isinstance(project.get("quotaBefore"), dict) else {}
            item = {
                "id": f"platform-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
                "source": "platform",
                "taskRoot": "",
                "scopeRoot": self._default_scope_root_locked(),
                "taskName": str(project.get("name") or project_code),
                "projectId": str(project.get("id") or ""),
                "projectCode": project_code,
                "projectName": str(project.get("name") or ""),
                "businessDomain": str(project.get("businessDomain") or ""),
                "category": str(project.get("category") or ""),
                "variantId": str(project.get("variantId") or ""),
                "variantName": str(project.get("variantName") or ""),
                "taskType": task_type,
                "difficulty": difficulty,
                "side": side,
                "triggerPrompt": trigger_prompt,
                "folderId": str(folder_id or ""),
                "folderPath": str(folder_path or ""),
                "quota": {
                    "state": "pending",
                    "variantId": str(project.get("variantId") or ""),
                    "remainingBefore": quota_before.get("remaining"),
                    "platformTaskId": "",
                    "platformTaskNo": "",
                    "deductedAt": "",
                    "settledAt": "",
                    "refundReason": "",
                    "refundMode": "",
                },
                "status": "pending",
                "attempts": 0,
                "stalledRetryCount": 0,
                "lastStalledAt": "",
                "lastStalledTaskRoot": "",
                "lastStalledRunKey": "",
                "runKey": uuid.uuid4().hex[:10],
                "nextAttemptAt": "",
                "addedAt": utc_now(),
                "startedAt": "",
                "finishedAt": "",
                "jobPid": "",
                "claimedAt": "",
                "triggeredAt": "",
                "capacityHeld": False,
                "orphaned": False,
                "slotMarkers": [],
                "slotReservedAt": "",
                "error": "",
            }
            self._items.append(item)
            self._save()
            return copy.deepcopy(item)

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for item in self._items if item.get("status") == "pending")

    def tracked_project_codes(self) -> set[str]:
        with self._lock:
            values = list(self._items) + list(self._triggered)
            return {
                str(item.get("projectCode") or "").casefold()
                for item in values
                if str(item.get("projectCode") or "").strip()
            }

    def fail_item(self, item_id: str, error: str) -> bool:
        with self._lock:
            item = next((value for value in self._items if str(value.get("id") or "") == item_id), None)
            if item is None or item.get("status") != "pending":
                return False
            item["status"] = "failed"
            item["error"] = str(error)
            item["finishedAt"] = utc_now()
            self._save()
            return True

    def remove(self, item_id: str) -> None:
        with self._lock:
            for index, item in enumerate(self._items):
                if item.get("id") == item_id:
                    if item.get("status") in QUEUE_ACTIVE_STATUSES or item.get("capacityHeld"):
                        raise MonitorError("任务仍占用并发名额，请先等待终态或手动释放名额")
                    self._items.pop(index)
                    self._save()
                    return
        raise MonitorError("队列项不存在")

    def move(self, item_id: str, delta: int) -> None:
        with self._lock:
            index = next((i for i, item in enumerate(self._items) if item.get("id") == item_id), -1)
            if index < 0:
                raise MonitorError("队列项不存在")
            target = max(0, min(len(self._items) - 1, index + int(delta)))
            if target == index:
                return
            item = self._items.pop(index)
            self._items.insert(target, item)
            self._save()

    def retry(self, item_id: str) -> None:
        with self._lock:
            item = next((value for value in self._items if value.get("id") == item_id), None)
            if not item:
                raise MonitorError("队列项不存在")
            if item.get("status") in QUEUE_ACTIVE_STATUSES or item.get("capacityHeld"):
                raise MonitorError("任务仍占用并发名额，不能直接重试")
            update = {
                "status": "pending",
                "attempts": 0,
                "stalledRetryCount": 0,
                "nextAttemptAt": "",
                "startedAt": "",
                "finishedAt": "",
                "jobPid": "",
                "claimedAt": "",
                "capacityHeld": False,
                "orphaned": False,
                "notice": "",
                "error": "",
                "lastError": "",
                "triggeredAt": "",
                "terminalSignature": "",
                "terminalSeenAt": "",
                "lastStalledAt": "",
                "lastStalledTaskRoot": "",
                "lastStalledRunKey": "",
                "manualReleased": False,
                "releasedAt": "",
                "slotMarkers": [],
                "slotReservedAt": "",
            }
            if item.get("source") == "platform":
                self._fresh_quota(item)
                update.update({
                    "taskRoot": "",
                    "resultFile": "",
                    "promptSha256": "",
                    "runKey": uuid.uuid4().hex[:10],
                })
            item.update(update)
            self._save()

    def release(self, item_id: str) -> None:
        """Release a held capacity slot without pretending the desktop task stopped."""
        with self._lock:
            item = next((value for value in self._items if str(value.get("id") or "") == item_id), None)
            if item is None:
                raise MonitorError("队列项不存在")
            if not self._item_holds_slot(item) or item.get("status") not in {"orphaned", "failed", "skipped"}:
                raise MonitorError("只有已失去执行器且保留名额的失败项可以手动释放")
            live_reason = self.live_task_reason(item)
            if live_reason:
                item["notice"] = f"{live_reason}，为防超额度已拒绝释放名额"
                item["error"] = item["notice"]
                self._save()
                raise MonitorError(f"{live_reason}，不能释放并发名额；请先停止任务并等待终态")
            self.slots.release_for_item(item_id)
            item.update({
                "status": "skipped",
                "capacityHeld": False,
                "orphaned": False,
                "manualReleased": True,
                "releasedAt": utc_now(),
                "claimedAt": "",
                "jobPid": "",
                "finishedAt": utc_now(),
                "slotMarkers": [],
                "notice": "已手动释放并发名额；未停止可能仍存在的桌面任务",
            })
            self._save()

    def shuffle_pending(self, rng: random.Random | random.SystemRandom, *, keep_head: int = 0) -> bool:
        """Shuffle pending items in place, leaving the first ``keep_head`` alone."""
        with self._lock:
            indexes = [index for index, item in enumerate(self._items) if item.get("status") == "pending"]
            indexes = indexes[max(0, keep_head):]
            if len(indexes) < 2:
                return False
            values = [self._items[index] for index in indexes]
            rng.shuffle(values)
            for index, item in zip(indexes, values):
                self._items[index] = item
            self._save()
            return True

    def clear_finished(self) -> None:
        with self._lock:
            self._items = [
                item for item in self._items
                if item.get("status") not in QUEUE_TERMINAL_STATUSES or item.get("capacityHeld")
            ]
            self._save()

    # -- quota ------------------------------------------------------------ #
    def _quota_of(self, item: dict[str, Any]) -> dict[str, Any]:
        quota = item.get("quota")
        return quota if isinstance(quota, dict) else {}

    def settle_quota(self, item: dict[str, Any], *, success: bool, reason: str = "") -> dict[str, Any]:
        """Move a claimed quota to settled or refunded."""
        quota = self._quota_of(item)
        state = str(quota.get("state") or "pending")
        if state not in {"claimed", "pending"}:
            return quota
        if success:
            quota["state"] = "settled"
            quota["settledAt"] = utc_now()
            if reason:
                quota["settleReason"] = reason
        else:
            quota["state"] = "refunded"
            quota["settledAt"] = utc_now()
            quota["refundReason"] = reason or "任务失败或中止"
        item["quota"] = quota
        return quota

    def _refund(self, item: dict[str, Any], reason: str) -> dict[str, Any]:
        """Refund a failed item's quota, on the platform when one is attached.

        ``settle_quota(success=False)`` only flips the local state; the failure
        paths used to call it directly, so the platform task was never
        cancelled and the project's quota leaked.
        """
        return self.refund_quota(item, self.platform, reason)

    def _fresh_quota(self, item: dict[str, Any]) -> dict[str, Any]:
        """Quota block for a new attempt, so the next claim pre-deducts again.

        Requeueing with the old block kept ``state=refunded`` — ``claim_quota``
        then skipped the pre-deduction and the worker was handed the
        platform task id that had just been cancelled.
        """
        old = self._quota_of(item)
        history = list(old.get("history") or [])
        if old.get("platformTaskId"):
            history.append({
                "platformTaskId": old.get("platformTaskId"),
                "platformTaskNo": old.get("platformTaskNo") or "",
                "state": old.get("state") or "",
                "reason": old.get("refundReason") or old.get("settleReason") or "",
            })
        quota = {
            "state": "pending",
            "variantId": old.get("variantId") or item.get("variantId") or "",
            "remainingBefore": old.get("remainingBefore"),
            "platformTaskId": "",
            "platformTaskNo": "",
            "deductedAt": "",
            "settledAt": "",
            "refundReason": "",
            "refundMode": "",
        }
        if history:
            quota["history"] = history[-10:]
        item["quota"] = quota
        return quota

    def _emit(self, level: str, event: str, **fields: Any) -> None:
        if self.log is None:
            return
        try:
            self.log.emit(event, level=level, **fields)
        except Exception:
            pass

    # -- stalled retry settings ------------------------------------------- #
    def _stalled_retry_settings(self) -> tuple[float, int]:
        automation = self._automation_cfg()
        try:
            retry_after = float(automation.get("stalledTaskRetrySeconds") or DEFAULT_STALLED_TASK_RETRY_SECONDS)
        except (TypeError, ValueError):
            retry_after = float(DEFAULT_STALLED_TASK_RETRY_SECONDS)
        try:
            retry_limit = int(automation.get("stalledTaskRetryLimit") or DEFAULT_STALLED_TASK_RETRY_LIMIT)
        except (TypeError, ValueError):
            retry_limit = DEFAULT_STALLED_TASK_RETRY_LIMIT
        return max(60.0, retry_after), max(0, retry_limit)

    @staticmethod
    def _task_state(task_root: Path) -> dict[str, Any]:
        state = read_json(task_root / "monitor" / "state.json", {})
        return state if isinstance(state, dict) else {}

    def _side_done(self, task_root: Path, side: str) -> bool:
        state = self._task_state(task_root)
        sides = state.get("sides") or {}
        if side == "both":
            return all(str((sides.get(name) or {}).get("status") or "") in SIDE_DONE_STATUSES for name in ("A", "B"))
        return str((sides.get(side) or {}).get("status") or "") in SIDE_DONE_STATUSES

    def _side_active(self, task_root: Path, side: str) -> bool:
        state = self._task_state(task_root)
        sides = state.get("sides") or {}
        names = ("A", "B") if side == "both" else (side,)
        for name in names:
            record = sides.get(name) or {}
            if str(record.get("status") or "") == "running" and runner_pid_alive(record, task_root, name, self.process_table):
                return True
        return False

    @staticmethod
    def _latest_activity_timestamp(
        *,
        item: dict[str, Any],
        job: dict[str, Any] | None,
        task_root: Path | None,
        result_file: Path,
    ) -> float | None:
        candidates: list[float] = []
        for value in (
            item.get("claimedAt"),
            item.get("startedAt"),
            item.get("triggeredAt"),
            (job or {}).get("startedAt"),
        ):
            parsed = parse_time(value)
            if parsed is not None:
                candidates.append(parsed.timestamp())
        paths: list[Path] = []
        if task_root is not None:
            paths.append(task_root / "monitor" / "state.json")
            # The top-level state.json only changes when the task's *phase*
            # changes.  While candidates race inside Docker they write
            # continuously to their own trajectories and the outer state file
            # stays untouched for the whole attempt — which can easily exceed
            # the stall threshold.  Those files are the real liveness signal.
            paths.extend(task_root.glob("monitor/runtime/**/stdout.jsonl"))
            paths.extend(task_root.glob("monitor/runtime/**/attempt-*/*.json"))
            paths.extend(task_root.glob("workspace/轨迹文件/**/stdout.jsonl"))
        if str(result_file) not in {"", "."}:
            paths.append(result_file)
        for path in paths:
            if not str(path):
                continue
            try:
                candidates.append(path.stat().st_mtime)
            except OSError:
                continue
        return max(candidates) if candidates else None

    # -- state machine ---------------------------------------------------- #
    def _sync_running_locked(self) -> None:
        changed = False
        remove_ids: set[str] = set()
        triggered_ids: set[str] = set()

        def apply_failure(item: dict[str, Any], error: str, *, retryable: bool) -> None:
            attempts = int(item.get("attempts") or 0)
            max_attempts = max(1, int(self._automation_cfg().get("maxAttempts") or 3))
            if retryable and attempts + 1 < max_attempts:
                backoff = max(10, int(self._automation_cfg().get("retryBackoffSeconds") or DEFAULT_QUEUE_RETRY_BACKOFF_SECONDS))
                delay = backoff * (attempts + 1)
                self._fresh_quota(item)
                item.update({
                    "attempts": attempts + 1,
                    "status": "pending",
                    "nextAttemptAt": iso_from_timestamp(time.time() + delay),
                    "lastError": error,
                    "error": f"等待自动重试（第 {attempts + 1} 次，{delay} 秒）",
                    "finishedAt": "",
                    "jobPid": "",
                    "claimedAt": "",
                    "orphaned": False,
                    "capacityHeld": False,
                    # A new run key detaches the retry from the failed job's
                    # result file, which would otherwise be read back next tick.
                    "runKey": uuid.uuid4().hex[:10],
                    "taskRoot": "",
                    "resultFile": "",
                })
                return
            item.update({
                "attempts": attempts + 1,
                "status": "failed",
                "lastError": error,
                "error": error,
                "finishedAt": utc_now(),
                "jobPid": "",
                "claimedAt": "",
                "orphaned": False,
                "capacityHeld": False,
            })

        def requeue_stalled(item: dict[str, Any], *, stalled_seconds: float) -> None:
            retry_count = int(item.get("stalledRetryCount") or 0) + 1
            old_task_root = str(item.get("taskRoot") or "")
            old_run_key = str(item.get("runKey") or "")
            if old_run_key:
                old_job = self.jobs.get_platform(str(item.get("id") or ""), old_run_key)
                self.jobs.stop_platform_worker_for_retry(item.get("id", ""), old_run_key)
                _remove_empty_task_root((old_job or {}).get("reservedTaskRoot"))
            _remove_empty_task_root(old_task_root)
            minutes = max(1, int(stalled_seconds // 60))
            message = f"任务连续 {minutes} 分钟无状态更新，已自动重试第 {retry_count} 次"
            self._fresh_quota(item)
            item.update({
                "status": "pending",
                "capacityHeld": False,
                "orphaned": False,
                "jobPid": "",
                "claimedAt": "",
                "startedAt": "",
                "finishedAt": "",
                "triggeredAt": "",
                "taskRoot": "",
                "resultFile": "",
                "promptSha256": "",
                "stateStatus": "",
                "terminalSignature": "",
                "terminalSeenAt": "",
                "containerWait": "",
                "nextAttemptAt": "",
                "runKey": uuid.uuid4().hex[:10],
                "stalledRetryCount": retry_count,
                "lastStalledAt": utc_now(),
                "lastStalledTaskRoot": old_task_root,
                "lastStalledRunKey": old_run_key,
                "lastError": message,
                "error": message,
                "notice": message,
                # Cleared once the retry's own desktop task exists; until then
                # the operator should see why the item went back to pending.
                "noticeKind": "retry",
            })

        for item in list(self._items):
            item_id = str(item.get("id") or "")
            if item.get("status") == "done":
                if item_id:
                    remove_ids.add(item_id)
                    triggered_ids.add(item_id)
                changed = True
                continue

            if item.get("source") != "platform":
                if item.get("status") != "running":
                    continue
                task_root = Path(str(item.get("taskRoot") or ""))
                side = str(item.get("side") or "both")
                job = self.jobs.get(task_root, side)
                if job and job.get("status") == "running":
                    if item.get("jobPid") != job.get("pid"):
                        item["jobPid"] = job.get("pid")
                        changed = True
                    continue
                if job and job.get("status") in {"finished", "failed"}:
                    item["status"] = "done" if job.get("status") == "finished" else "failed"
                    item["error"] = "" if job.get("status") == "finished" else f"执行器退出码 {job.get('exitCode')}"
                elif self._side_done(task_root, side):
                    item["status"] = "done"
                elif self._side_active(task_root, side):
                    continue
                else:
                    item["status"] = "pending"
                    item["error"] = "监控服务重启或执行器已退出，已重新排队"
                item["finishedAt"] = utc_now() if item.get("status") in {"done", "failed"} else ""
                item["jobPid"] = ""
                changed = True
                continue

            # Terminal items are settled exactly once.  Re-evaluating them every
            # tick used to re-apply the failure (``attempts`` climbed into the
            # tens of thousands) and resurrect skipped items as ``orphaned``,
            # which the reconcile loop then released and refunded again, over
            # and over.
            if str(item.get("status") or "") in QUEUE_TERMINAL_STATUSES:
                if item.get("capacityHeld") and not item.get("manualReleased"):
                    item["capacityHeld"] = False
                    changed = True
                continue

            job = self.jobs.get_platform(item_id, str(item.get("runKey") or ""))
            result_file = Path(str((job or {}).get("resultFile") or item.get("resultFile") or ""))
            result = read_json(result_file, {}) if result_file else {}
            if not isinstance(result, dict):
                result = {}
            result_root = str(result.get("taskRoot") or item.get("taskRoot") or "")
            task_root = Path(result_root).expanduser().resolve() if result_root else None
            if result_root and item.get("taskRoot") != result_root:
                item["taskRoot"] = result_root
                changed = True
            if task_root is not None:
                active_roots = set(self._active_roots_locked())
                for root in active_roots:
                    root_path = Path(root)
                    if task_root == root_path or root_path in task_root.parents:
                        if item.get("scopeRoot") != root:
                            item["scopeRoot"] = root
                            changed = True
                        break
            state = read_json(task_root / "monitor" / "state.json", {}) if task_root else {}
            state_status = str(state.get("status") or "") if isinstance(state, dict) else ""
            stage = str(result.get("stage") or "")
            result_status = str(result.get("status") or "")
            task_started = bool(task_root and (task_root / "monitor" / "state.json").is_file())
            desktop_submitted = stage in {"desktop-submitted", "desktop-task-running"} or task_started

            # The executor reports its own platform task when it selected one;
            # that record wins over the monitor's pre-deduction.
            self._absorb_executor_quota(item, result)

            terminal_state = state_status in TERMINAL_TASK_STATUSES or state_status in TASK_FAILURE_STATUSES
            if terminal_state:
                stable, dirty = self._terminal_state_stable(item, task_root, state_status)
                if dirty:
                    changed = True
                if not stable:
                    continue
                if state_status in TERMINAL_TASK_STATUSES:
                    if job and job.get("status") == "running" and persisted_job_process_alive(job):
                        item.update({
                            "status": "triggered",
                            "stateStatus": state_status,
                            "triggeredAt": item.get("triggeredAt") or utc_now(),
                            "jobPid": job.get("pid"),
                            "claimedAt": "",
                            "orphaned": False,
                            "capacityHeld": True,
                        })
                        changed = True
                        continue
                    self.settle_quota(item, success=True, reason=f"任务终态 {state_status}")
                    self.slots.release_for_item(item_id)
                    item.update({
                        "status": "done",
                        "stateStatus": state_status,
                        "finishedAt": utc_now(),
                        "jobPid": "",
                        "claimedAt": "",
                        "orphaned": False,
                        "capacityHeld": False,
                        "slotMarkers": [],
                    })
                    if item_id:
                        remove_ids.add(item_id)
                        triggered_ids.add(item_id)
                    changed = True
                    continue
                self._refund(item, f"桌面任务状态为 {state_status}")
                self.slots.release_for_item(item_id)
                apply_failure(item, f"桌面任务状态为 {state_status}", retryable=False)
                item["slotMarkers"] = []
                changed = True
                continue

            if item.get("manualReleased") and item.get("status") == "skipped":
                item["capacityHeld"] = False
                item["orphaned"] = False
                continue

            retry_after, retry_limit = self._stalled_retry_settings()
            stalled_retry_count = int(item.get("stalledRetryCount") or 0)
            latest_activity = self._latest_activity_timestamp(
                item=item,
                job=job,
                task_root=task_root,
                result_file=result_file,
            )
            stalled_seconds = (
                max(0.0, time.time() - latest_activity)
                if latest_activity is not None
                else 0.0
            )
            if (
                retry_limit > 0
                and stalled_retry_count < retry_limit
                and str(item.get("status") or "") in QUEUE_ACTIVE_STATUSES
                and latest_activity is not None
                and stalled_seconds >= retry_after
            ):
                # SIGTERM on the worker does not stop the ChatGPT desktop
                # session it opened.  Requeueing while that session is still
                # mid-flight starts a *second* concurrent attempt at the same
                # project — two live candidate sets for one queue item.  So only
                # requeue once the desktop task has actually reached a terminal
                # state; otherwise hold the slot and let the orphan grace period
                # deal with it.
                desktop_still_running = task_started and state_status not in (
                    TERMINAL_TASK_STATUSES | TASK_FAILURE_STATUSES
                )
                if desktop_still_running:
                    # Quiet is not dead: candidates can sit in the skill's slot
                    # queue, or the desktop agent can be thinking.  Only annotate
                    # the item (once) and let the normal paths below keep its
                    # status.  Flipping it to ``orphaned`` here used to reset the
                    # orphan clock every tick — so it never expired — and wrote
                    # a warning line every tick.
                    if not item.get("stalledSince"):
                        item["stalledSince"] = utc_now()
                        item["notice"] = (
                            f"已静默 {int(stalled_seconds)} 秒，桌面任务仍处于 {state_status}，"
                            "保留名额等待其终态"
                        )
                        item["noticeKind"] = "stalled"
                        changed = True
                        self._emit("warning", "queue.stalled_held", taskId=item_id,
                                   projectCode=str(item.get("projectCode") or ""),
                                   detail=f"静默 {int(stalled_seconds)} 秒但桌面任务处于 {state_status}，保留名额不重试")
                else:
                    self._refund(item, f"任务静默 {int(stalled_seconds)} 秒后自动重试")
                    self.slots.release_for_item(item_id)
                    requeue_stalled(item, stalled_seconds=stalled_seconds)
                    item["slotMarkers"] = []
                    self._emit("warning", "queue.stalled_retry", taskId=item_id,
                               projectCode=str(item.get("projectCode") or ""),
                               detail=f"静默 {int(stalled_seconds)} 秒，第 {stalled_retry_count + 1} 次自动重试")
                    changed = True
                    continue
            elif item.get("stalledSince") and stalled_seconds < retry_after:
                # Activity resumed: the "quiet" notice no longer describes it.
                item.pop("stalledSince", None)
                if item.get("noticeKind") == "stalled":
                    item.pop("notice", None)
                    item.pop("noticeKind", None)
                changed = True

            worker_alive = bool(job and job.get("status") == "running" and persisted_job_process_alive(job))
            if (
                retry_limit > 0
                and stalled_retry_count < retry_limit
                and str(item.get("status") or "") in QUEUE_ACTIVE_STATUSES
                and item.get("capacityHeld")
                and not task_started
                and not desktop_submitted
                and not worker_alive
            ):
                # The executor exited before it ever created a desktop task.
                # Releasing and retrying is safe because there is no orphan to
                # race with the next attempt.
                self._refund(item, "执行器在创建桌面任务前退出")
                _remove_empty_task_root((job or {}).get("reservedTaskRoot"))
                self.slots.release_for_item(item_id)
                requeue_stalled(item, stalled_seconds=retry_after)
                item["slotMarkers"] = []
                self._emit("warning", "queue.stalled_retry", taskId=item_id,
                           projectCode=str(item.get("projectCode") or ""),
                           detail=f"执行器未创建桌面任务，第 {stalled_retry_count + 1} 次自动重试")
                changed = True
                continue

            if item.pop("terminalSignature", None) is not None:
                changed = True
            if item.pop("terminalSeenAt", None) is not None:
                changed = True

            if item.get("slotMarkers") and task_root is not None:
                # Once the task's own containers are up the placeholder
                # reservations have done their job; keeping them would double
                # count the slots against the hard limit.
                live = [
                    name for name in task_container_names(task_root.name, self.docker_cache.get() if self.docker_cache else {})
                ]
                if live:
                    self.slots.release_for_item(item_id)
                    item["slotMarkers"] = []

            if job and job.get("status") == "running":
                if item.get("status") in {"pending", "launching"}:
                    item["status"] = "running"
                if desktop_submitted:
                    item["status"] = "triggered"
                    item["triggeredAt"] = item.get("triggeredAt") or utc_now()
                    item["capacityHeld"] = True
                    if item.get("noticeKind") == "retry":
                        # The retry has its own desktop task now; the old
                        # "went back to pending" message would read as a fault.
                        item.pop("notice", None)
                        item.pop("noticeKind", None)
                        item["error"] = ""
                item["jobPid"] = job.get("pid")
                item["claimedAt"] = ""
                item["orphaned"] = False
                item.pop("orphanedAt", None)
                if result.get("promptSha256"):
                    item["promptSha256"] = str(result.get("promptSha256"))
                if item.get("taskRoot") != result_root and result_root:
                    item["taskRoot"] = result_root
                changed = True
                continue

            if item.get("status") == "pending" and not desktop_submitted:
                continue

            if desktop_submitted:
                item.update({
                    "status": "triggered" if job and job.get("status") in {"running", "finished"} else "orphaned",
                    "taskRoot": result_root,
                    "triggeredAt": item.get("triggeredAt") or utc_now(),
                    "jobPid": "",
                    "claimedAt": "",
                    "orphaned": not bool(job and job.get("status") == "finished"),
                    "capacityHeld": True,
                    "orphanedAt": item.get("orphanedAt") or utc_now(),
                    "error": "执行器已退出，保留并发名额并等待桌面任务进入真实终态",
                })
                changed = True
                continue

            if item.get("status") == "orphaned" or item.get("capacityHeld"):
                item.update({
                    "status": "orphaned",
                    "jobPid": "",
                    "claimedAt": "",
                    "orphaned": True,
                    "capacityHeld": True,
                    "orphanedAt": item.get("orphanedAt") or utc_now(),
                    "error": "无法确认桌面任务已停止，保留并发名额并禁止自动重试",
                })
                changed = True
                continue

            error = str(result.get("error") or "")
            if not error and job:
                error = f"执行器退出码 {job.get('exitCode')}" if job.get("status") == "failed" else ""
            if not error and item.get("status") == "launching":
                error = "启动器在创建桌面任务前退出"
            if not error:
                error = "监控服务重启或执行器已退出"

            if item.get("status") == "launching":
                age = age_seconds(item.get("claimedAt") or item.get("startedAt"))
                startup_timeout = self._startup_timeout()
                if age is not None and age < startup_timeout and not job:
                    continue
                self.slots.release_for_item(item_id)
                item.update({
                    "status": "orphaned",
                    "jobPid": "",
                    "claimedAt": "",
                    "orphaned": True,
                    "capacityHeld": True,
                    "orphanedAt": item.get("orphanedAt") or utc_now(),
                    "slotMarkers": [],
                    "error": "启动认领后没有可验证的执行器记录，保留并发名额并禁止自动重试",
                })
                changed = True
                continue

            uncertain = task_started or stage in {
                "desktop-start-timeout",
                "desktop-submitted",
                "desktop-task-running",
                "wait-timeout",
            }
            if uncertain:
                item.update({
                    "status": "orphaned",
                    "taskRoot": result_root,
                    "jobPid": "",
                    "claimedAt": "",
                    "orphaned": True,
                    "capacityHeld": True,
                    "orphanedAt": item.get("orphanedAt") or utc_now(),
                    "error": "无法确认桌面任务已停止，保留并发名额；确认后可在队列中手动释放",
                })
                changed = True
                continue

            retryable = queue_failure_retryable(result, error)
            if result_status in {"failed", "done"} or job or item.get("status") in QUEUE_ACTIVE_STATUSES:
                self._refund(item, error)
                self.slots.release_for_item(item_id)
                apply_failure(item, error, retryable=retryable)
                item["slotMarkers"] = []
                changed = True

        if remove_ids:
            for archived in self._items:
                if str(archived.get("id") or "") not in triggered_ids:
                    continue
                previous = next(
                    (value for value in self._triggered if value.get("id") == archived.get("id")),
                    None,
                )
                if previous is None:
                    self._triggered.append(copy.deepcopy(archived))
                else:
                    previous.update(copy.deepcopy(archived))
            self._items = [item for item in self._items if str(item.get("id") or "") not in remove_ids]
        if changed:
            self._save()

    def _absorb_executor_quota(self, item: dict[str, Any], result: dict[str, Any]) -> None:
        """Take the executor's own quota record when it reports one."""
        if not isinstance(result, dict):
            return
        selection = result.get("platformSelection") if isinstance(result.get("platformSelection"), dict) else {}
        if not selection:
            # The executor writes its own selection record inside the task
            # directory; read it when result.json does not carry the block.
            task_root = str(result.get("taskRoot") or item.get("taskRoot") or "")
            if task_root:
                for candidate in ("platform/selection.json", "monitor/platform-selection.json"):
                    payload = read_json(Path(task_root) / candidate, {})
                    nested = payload.get("selection") if isinstance(payload, dict) else {}
                    if isinstance(nested, dict) and nested.get("taskId"):
                        selection = nested
                        break
        if not selection:
            return
        quota = self._quota_of(item)
        task_id = str(selection.get("taskId") or selection.get("platformTaskId") or "")
        if not task_id:
            return
        if quota.get("platformTaskId") and quota.get("platformTaskId") != task_id:
            # The monitor pre-deducted a different task; keep it for the refund
            # trail but record the executor's task as the authoritative one.
            quota.setdefault("supersededTaskIds", [])
            if quota["platformTaskId"] not in quota["supersededTaskIds"]:
                quota["supersededTaskIds"].append(quota["platformTaskId"])
            quota["state"] = "superseded"
        quota["platformTaskId"] = task_id
        quota["platformTaskNo"] = str(selection.get("taskNo") or quota.get("platformTaskNo") or "")
        quota["executorReported"] = True
        if selection.get("quotaBefore") is not None:
            quota["remainingBefore"] = selection.get("quotaBefore")
        if selection.get("quotaAfter") is not None:
            quota["remainingAfter"] = selection.get("quotaAfter")
        if selection.get("variant"):
            quota["variantName"] = str(selection.get("variant") or "")
        item["quota"] = quota

    # -- tick ------------------------------------------------------------- #
    def startup_grace_seconds(self) -> int:
        return clamp_int(
            self._automation_cfg().get("startupGraceSeconds"), 0, 3600, DEFAULT_STARTUP_GRACE_SECONDS
        )

    def startup_guard(self) -> tuple[bool, str]:
        """Whether it is safe to start work yet.

        A fresh process has not read the task tree, docker, or the queue's own
        persisted state.  Starting tasks during that window would size capacity
        against an empty world and over-commit, so starts wait until the state
        has been read *and* the grace period has elapsed.
        """
        elapsed = time.time() - self.started_at
        grace = self.startup_grace_seconds()
        if not self.state_loaded:
            return False, "启动保护期：尚未读取任务与容器状态"
        if elapsed < grace:
            return False, f"启动保护期：已读取状态，距可启动还有 {int(grace - elapsed)} 秒"
        return True, ""

    def tick(self, platform: Any = None) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        with self._lock:
            startup_timeout = self._startup_timeout()
            self.jobs.reap_stale_platform_jobs(startup_timeout)
            self._sync_running_locked()
            stopped_changed = False
            for item in self._items:
                if item.get("status") != "pending" or item.get("source") != "platform":
                    continue
                if self._item_is_stopped(item):
                    item.update({
                        "status": "skipped",
                        "notice": "项目已在停止名单，不再启动",
                        "error": "",
                        "finishedAt": utc_now(),
                    })
                    stopped_changed = True
            if stopped_changed:
                self._save()
            if bool(self._automation_cfg().get("paused", True)):
                return actions

            def hold(reason: str, kind: str = "gate") -> list[dict[str, Any]]:
                """Park the head of the queue on ``reason``; save only on change.

                ``kind`` tells the queue page what the wait is: ``capacity``
                (no container slot / task seat), ``spacing`` (launch interval),
                or ``gate`` (startup guard, docker, runner, config).
                """
                changed = False
                for pending_item in self._items:
                    if pending_item.get("status") != "pending" or pending_item.get("source") != "platform":
                        continue
                    if pending_item.get("containerWait") != reason or pending_item.get("containerWaitKind") != kind:
                        pending_item["containerWait"] = reason
                        pending_item["containerWaitKind"] = kind
                        pending_item["error"] = ""
                        changed = True
                    break
                if changed:
                    self._save()
                return actions

            allowed, _guard_reason = self.startup_guard()
            if not allowed:
                if not self.state_loaded:
                    return hold("启动保护期：尚未读取任务与容器状态")
                ready_at = iso_from_timestamp(self.started_at + self.startup_grace_seconds())
                return hold(f"启动保护期：已读取状态，约 {ready_at[11:19]} UTC 后可启动")
            if not self._active_roots_locked():
                return hold("未选择 Codex 任务目录，暂停启动")
            mode = self._schedule_mode()
            capacity = int(self._automation_cfg().get("capacity") or 2)
            # Launches are spaced by the configured interval, and never closer
            # than MIN_LAUNCH_SPACING_SECONDS: two desktop deep links in the same
            # second lost one prompt and left an empty task directory behind.
            spacing = max(MIN_LAUNCH_SPACING_SECONDS, int(self._automation_cfg().get("cooldownSeconds") or 0))
            last_started = parse_time(self._lastStartedAt)
            if last_started and time.time() - last_started.timestamp() < spacing:
                ready_at = iso_from_timestamp(last_started.timestamp() + spacing)
                return hold(f"启动间隔 {spacing} 秒，约 {ready_at[11:19]} UTC 后启动下一个", "spacing")
            free = self.disk_free_gb()
            min_free = housekeeping_settings(self.config)["minFreeGB"]
            if free is not None and free < min_free:
                if not self._disk_low:
                    self._disk_low = True
                    self._emit("warning", "queue.disk_low",
                               detail=f"磁盘剩余 {free:.1f} GB，低于 {min_free:g} GB，暂停启动新任务")
                return hold(f"磁盘剩余 {free:.0f} GB，低于 {min_free:g} GB：暂停启动，清理出空间后自动继续", "gate")
            if self._disk_low:
                self._disk_low = False
                self._emit("info", "queue.disk_recovered", detail=f"磁盘剩余 {free:.1f} GB，恢复启动")
            capacity_in_use, capacity_detail = self._capacity_usage_locked(startup_timeout)
            if not bool(capacity_detail.get("dockerReady", False)):
                if not self._docker_down:
                    self._docker_down = True
                    self._emit("warning", "queue.docker_unavailable",
                               detail=str(capacity_detail.get("error") or "docker ps 失败"))
                return hold("Docker 状态未确认，等待恢复后启动")
            if self._docker_down:
                self._docker_down = False
                self._emit("info", "queue.docker_recovered", detail="Docker 状态已恢复")

            hard_limit = self._max_containers_limit()
            batch = self._candidates_per_task()
            # Running containers plus the containers live tasks still need
            # (queued candidates, tasks not yet at the candidate race).
            estimated_current = int(capacity_detail.get("estimatedNonTestContainers") or 0)
            non_test = int(capacity_detail.get("nonTestContainerCount") or 0)
            pending_demand = int(capacity_detail.get("pendingContainerDemand") or 0)

            if mode == SCHEDULE_MODE_TASKS:
                # Task-count mode: the number of live tasks is the only gate;
                # the skill's limiter still caps containers at the hard limit.
                if capacity_in_use >= capacity:
                    self._capacity_saturated = True
                    return hold(f"并行任务已满：{capacity_in_use}/{capacity}，有空位自动启动", "capacity")
            elif hard_limit > 0 and estimated_current + 1 > hard_limit:
                # Container mode: start as soon as one slot is free.  Requiring
                # room for the whole batch made the count oscillate 2↔4 against
                # a limit of 4; the skill's limiter queues the overflow.
                return hold(f"容器名额已满：运行中 {non_test} + 待启动 {pending_demand} / 上限 {hard_limit}，有空位自动启动", "capacity")

            try:
                self.jobs.validate_platform_runner()
            except MonitorError as exc:
                changed = False
                error = str(exc)
                for item in self._items:
                    if item.get("status") != "pending" or item.get("source") != "platform":
                        continue
                    if str(item.get("error") or "") == error:
                        continue
                    item["error"] = error
                    item["containerWait"] = "执行器不可启动，修复后自动继续"
                    changed = True
                if changed:
                    self._save()
                    self._emit("warning", "queue.runner_unavailable", detail=error)
                return actions
            active_project_codes = {
                str(item.get("projectCode") or "").casefold()
                for item in self._items
                if item.get("source") == "platform"
                and str(item.get("projectCode") or "").strip()
                and (str(item.get("status") or "") in QUEUE_ACTIVE_STATUSES or item.get("capacityHeld"))
            }
            starting_project_codes: set[str] = set()
            for item in self._items:
                if mode == SCHEDULE_MODE_TASKS:
                    if capacity_in_use >= capacity:
                        break
                elif hard_limit > 0 and estimated_current + 1 > hard_limit:
                    break
                if item.get("status") != "pending":
                    continue
                next_attempt = parse_time(item.get("nextAttemptAt"))
                if next_attempt and next_attempt.timestamp() > time.time():
                    continue
                if item.get("source") == "platform":
                    if not str(item.get("projectCode") or "").strip():
                        item["status"] = "failed"
                        item["error"] = "平台项目缺少 projectCode"
                        item["finishedAt"] = utc_now()
                        continue
                    project_code = str(item.get("projectCode") or "").casefold()
                    if project_code in active_project_codes or project_code in starting_project_codes:
                        item["containerWait"] = "同一项目已有任务在执行，等待其结束后再启动"
                        item["error"] = item["containerWait"]
                        continue
                    claimed_at = utc_now()
                    item_id = str(item.get("id") or "")
                    if platform is not None:
                        try:
                            self.claim_quota(item, platform)
                        except Exception as exc:
                            self._emit("warning", "quota.claim_failed", taskId=item_id,
                                       projectCode=str(item.get("projectCode") or ""), detail=str(exc))
                    item.update({
                        "status": "launching",
                        "scopeRoot": item.get("scopeRoot") or self._default_scope_root_locked(),
                        "claimedAt": claimed_at,
                        "startedAt": claimed_at,
                        "capacityHeld": True,
                        "orphaned": False,
                        "error": "",
                        "containerWait": "",
                        "slotMarkers": [],
                        "slotReservedAt": "",
                    })
                    self._save()
                    # The skill refuses a project whose earlier task in this
                    # workdir is still running/blocked, with no liveness check;
                    # close abandoned ones so the retry is not dead on arrival.
                    # Same workdir choice as JobManager.start_platform.
                    active_paths = [Path(root) for root in self._active_roots_locked()]
                    bound_scope = Path(str(item.get("scopeRoot") or "")).expanduser().resolve()
                    self._close_stale_project_tasks_locked(
                        str(item.get("projectCode") or ""),
                        bound_scope if bound_scope in active_paths else active_paths[0],
                    )
                    try:
                        job = self.jobs.start_platform(item, reason="queue")
                    except (MonitorError, OSError) as exc:
                        self.slots.release_for_item(item_id)
                        quota = self._quota_of(item)
                        if str(quota.get("state") or "") == "claimed":
                            self.refund_quota(item, platform, f"任务启动失败：{exc}")
                            item.update({
                                "status": "failed",
                                "finishedAt": utc_now(),
                                "nextAttemptAt": "",
                            })
                        else:
                            item["status"] = "pending"
                        item.update({
                            "claimedAt": "",
                            "startedAt": "",
                            "jobPid": "",
                            "capacityHeld": False,
                            "orphaned": False,
                            "slotMarkers": [],
                            "slotReservedAt": "",
                            "error": str(exc),
                            "lastError": str(exc),
                        })
                        self._save()
                        self._emit("error", "queue.start_failed", taskId=str(item.get("id") or ""),
                                   projectCode=str(item.get("projectCode") or ""), detail=str(exc))
                        continue
                    item.update({
                        "status": "running",
                        "startedAt": utc_now(),
                        "jobPid": job.get("pid"),
                        "resultFile": str(job.get("resultFile") or ""),
                        "error": "",
                        "lastError": "",
                    })
                    starting_project_codes.add(project_code)
                    self._lastStartedAt = utc_now()
                    actions.append({"item": copy.deepcopy(item), "job": job})
                    capacity_in_use += 1
                    estimated_current += batch
                    if capacity_in_use >= capacity:
                        self._capacity_saturated = True
                    self._emit("info", "queue.started", taskId=str(item.get("id") or ""),
                               projectCode=str(item.get("projectCode") or ""),
                               detail=f"PID={job.get('pid')} 模式={mode}")
                    # One launch per tick; the spacing gate paces the rest.
                    break
                task_root = Path(str(item.get("taskRoot") or ""))
                side = str(item.get("side") or "both")
                if not (task_root / "monitor" / "state.json").is_file():
                    item["status"] = "failed"
                    item["error"] = "任务目录不存在"
                    item["finishedAt"] = utc_now()
                    continue
                if self._side_done(task_root, side):
                    item["status"] = "skipped"
                    item["error"] = "目标侧已完成，无需执行"
                    item["finishedAt"] = utc_now()
                    continue
                if self._side_active(task_root, side):
                    continue
                try:
                    job = self.jobs.start(task_root, side, force=False, reason="queue")
                except MonitorError as exc:
                    item["error"] = str(exc)
                    continue
                item.update({
                    "status": "running",
                    "startedAt": utc_now(),
                    "jobPid": job.get("pid"),
                    "error": "",
                })
                self._lastStartedAt = utc_now()
                actions.append({"item": copy.deepcopy(item), "job": job})
                capacity_in_use += 1
                break
            self._save()
        return actions

    # -- quota settlement at claim ---------------------------------------- #
    def claim_quota(self, item: dict[str, Any], platform: Any) -> dict[str, Any]:
        """Pre-deduct a platform task for a queue item that is about to start."""
        quota = self._quota_of(item)
        if str(quota.get("state") or "pending") not in {"pending", ""}:
            return quota
        variant_id = str(quota.get("variantId") or item.get("variantId") or "")
        task_type = str(item.get("taskType") or "0-1代码生成")
        if not variant_id or platform is None:
            quota["state"] = "claimed"
            quota["deductedAt"] = utc_now()
            quota["note"] = "未配置 variantId，配额仅在本地记账"
            item["quota"] = quota
            return quota
        try:
            created = platform.pre_deduct(variant_id, task_type)
        except Exception as exc:
            quota["state"] = "claimed"
            quota["deductedAt"] = utc_now()
            quota["note"] = f"预扣除失败，退为本地记账：{exc}"
            item["quota"] = quota
            self._emit("warning", "quota.prededuct_failed", taskId=str(item.get("id") or ""),
                       projectCode=str(item.get("projectCode") or ""), detail=str(exc))
            return quota
        quota.update({
            "state": "claimed",
            "deductedAt": utc_now(),
            "platformTaskId": created.get("platformTaskId", ""),
            "platformTaskNo": created.get("platformTaskNo", ""),
            "platformRoundId": created.get("platformRoundId", ""),
            # The create response reports how many times the project has been
            # used, not how much quota is left.  Storing that in a field called
            # ``remainingAfter`` made the ledger read "3 → 12" and look as if
            # quota had gone up.
            "usageCountAfter": created.get("projectUsageCount"),
        })
        item["quota"] = quota
        self._emit("info", "quota.prededucted", taskId=str(item.get("id") or ""),
                   projectCode=str(item.get("projectCode") or ""),
                   detail=(
                       f"预扣除成功 taskNo={quota.get('platformTaskNo')}，"
                       f"领取前剩余 {quota.get('remainingBefore')}，"
                       f"项目累计使用 {quota.get('usageCountAfter')}"
                   ))
        return quota

    def refund_quota(self, item: dict[str, Any], platform: Any, reason: str) -> dict[str, Any]:
        # Idempotent: a quota that was already refunded or settled must never
        # call the platform again.  The old resurrect-and-release loop issued
        # the same cancel over a hundred times.
        if str(self._quota_of(item).get("state") or "pending") not in {"claimed", "pending"}:
            return self._quota_of(item)
        quota = self.settle_quota(item, success=False, reason=reason)
        task_id = str(quota.get("platformTaskId") or "")
        if task_id and platform is not None:
            outcome = platform.release_task(task_id)
            quota["refundMode"] = outcome.get("mode", "local")
            quota["refundDetail"] = outcome
            if outcome.get("ok"):
                fresh = platform.project_quota(str(item.get("projectCode") or ""), str(item.get("taskType") or "0-1代码生成"))
                if fresh is not None:
                    quota["remainingAfter"] = fresh.get("remaining")
            self._emit("info", "quota.refunded", taskId=str(item.get("id") or ""),
                       projectCode=str(item.get("projectCode") or ""),
                       detail=f"回补模式={quota['refundMode']}，原因={reason}")
        else:
            quota["refundMode"] = "local"
            self._emit("info", "quota.refund_local", taskId=str(item.get("id") or ""),
                       projectCode=str(item.get("projectCode") or ""),
                       detail=f"仅本地记账回补，原因={reason}")
        item["quota"] = quota
        return quota


# --------------------------------------------------------------------------- #
# reconcile
# --------------------------------------------------------------------------- #
class ReconcileLoop:
    """Periodic correction of everything the fast path cannot decide safely."""

    def __init__(
        self,
        queue: QueueManager,
        jobs: JobManager,
        *,
        log: Any = None,
        platform: Any = None,
        interval_seconds: int = DEFAULT_RECONCILE_SECONDS,
    ):
        self.queue = queue
        self.jobs = jobs
        self.log = log
        self.platform = platform
        self.interval_seconds = max(15, int(interval_seconds))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_run_at = ""
        self.last_actions: list[dict[str, Any]] = []
        self.health: Any = None  # LoopHealth, injected by the service
        self.guard: Any = None  # TaskGuard, injected by the service
        self.housekeeper: Any = None  # Housekeeper, injected by the service

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="reconcile", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            health = self.health
            if health is not None:
                health.begin("reconcile")
            error = ""
            try:
                self.run_once()
            except Exception as exc:  # pragma: no cover - defensive
                error = str(exc)
                if self.log is not None:
                    try:
                        self.log.emit("reconcile.failed", level="error", detail=error)
                    except Exception:
                        pass
            finally:
                if health is not None:
                    health.end("reconcile", error)

    def _emit(self, event: str, **fields: Any) -> None:
        if self.log is None:
            return
        try:
            self.log.emit(event, **fields)
        except Exception:
            pass

    def run_once(self) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        actions.extend(self._sweep_dead_markers())
        actions.extend(self._release_inactive_reservations())
        actions.extend(self._release_terminal_containers())
        actions.extend(self._release_stuck_orphans())
        actions.extend(self._guard_attempt_inflation())
        actions.extend(self._force_settle_stale_quota())
        actions.extend(self._resync_from_result())
        actions.extend(self._run_guard())
        actions.extend(self._run_housekeeping())
        self.last_run_at = utc_now()
        self.last_actions = actions
        if actions:
            self._emit("reconcile.done", detail=f"纠错 {len(actions)} 项", actions=len(actions))
        return actions

    # -- individual checks ------------------------------------------------ #
    def _run_housekeeping(self) -> list[dict[str, Any]]:
        # Kicks a background round; results land in queue.housekeeping_status.
        if self.housekeeper is not None:
            self.housekeeper.start_background()
        return []

    def _run_guard(self) -> list[dict[str, Any]]:
        """Fallback policies last, after the state machine has settled the round."""
        if self.guard is None:
            return []
        try:
            return self.guard.run_once()
        except Exception as exc:
            self._emit("guard.failed", level="error", detail=str(exc))
            return []

    def _sweep_dead_markers(self) -> list[dict[str, Any]]:
        removed = self.queue.slots.sweep_dead()
        if not removed:
            return []
        self._emit("reconcile.dead_markers", detail=f"清理失效槽位标记 {len(removed)} 个", count=len(removed))
        return [{"kind": "dead-markers", "paths": removed}]

    def _release_inactive_reservations(self) -> list[dict[str, Any]]:
        """Release live-PID reservations that no active queue item owns.

        Older versions could leave markers behind when ``start_platform``
        failed after reserving slots.  Those markers keep counting until the
        scheduler process exits, so clean them up even while the queue is
        paused or otherwise not launching new work.
        """
        removed: list[str] = []
        changed = False
        with self.queue._lock:
            items = {str(item.get("id") or ""): item for item in self.queue._items}
            active_ids = {
                item_id
                for item_id, item in items.items()
                if item_id and (
                    str(item.get("status") or "") in QUEUE_ACTIVE_STATUSES
                    or bool(item.get("capacityHeld"))
                )
            }
            for path, data in self.queue.slots._read_markers():
                item_id = str(data.get("itemId") or "")
                # Executor-owned markers do not carry a queue item id.  Leave
                # those to the executor/limiter; only clean monitor-owned
                # reservations here.
                if not item_id or item_id in active_ids:
                    continue
                path_text = str(path)
                if not self.queue.slots.release(path):
                    continue
                removed.append(path_text)
                item = items.get(item_id)
                if item is None:
                    continue
                item["slotMarkers"] = [
                    marker for marker in item.get("slotMarkers") or []
                    if str(marker) != path_text
                ]
                if not item["slotMarkers"]:
                    item["slotReservedAt"] = ""
                changed = True
            for item in items.values():
                markers = [str(marker) for marker in item.get("slotMarkers") or []]
                existing = [marker for marker in markers if Path(marker).is_file()]
                if existing == markers:
                    continue
                item["slotMarkers"] = existing
                if not existing:
                    item["slotReservedAt"] = ""
                changed = True
            if changed:
                self.queue._save()
        if not removed and not changed:
            return []
        if removed:
            self._emit(
                "reconcile.inactive_reservations",
                detail=f"清理无活动队列项的槽位标记 {len(removed)} 个",
                count=len(removed),
            )
        return [{"kind": "inactive-reservations", "paths": removed, "queueChanged": changed}]

    def _release_terminal_containers(self) -> list[dict[str, Any]]:
        """Remove containers belonging to a queue item that reached a terminal state.

        The precondition is deliberately narrow: the task must be owned by a
        queue item in a terminal status *and* its own ``state.json`` must report a
        terminal status.  ``_triggered`` is not consulted — it also holds jobs
        recovered at startup that are still running, and treating those as
        terminal would delete live containers.
        """
        out: list[dict[str, Any]] = []
        docker = self.queue.docker_cache.get() if self.queue.docker_cache else {"items": []}
        running = [
            item for item in (docker.get("items") or [])
            if str(item.get("state") or "").lower() == "running"
            and str(item.get("name") or "").startswith("sologsb-")
        ]
        if not running:
            return out
        with self.queue._lock:
            live_roots = {
                Path(str(item.get("taskRoot") or "")).name
                for item in self.queue._items
                if str(item.get("status") or "") not in QUEUE_TERMINAL_STATUSES and str(item.get("taskRoot") or "")
            }
            terminal_names: dict[str, str] = {}
            for item in self.queue._items:
                if str(item.get("status") or "") not in QUEUE_TERMINAL_STATUSES:
                    continue
                root = str(item.get("taskRoot") or "")
                if not root:
                    continue
                state = read_json(Path(root) / "monitor" / "state.json", {})
                status = str(state.get("status") or "") if isinstance(state, dict) else ""
                if status:
                    terminal_names[Path(root).name] = status
        for item in running:
            group = self.queue._container_group_name(item.get("name"))
            if not group or group not in terminal_names:
                continue
            if group in live_roots:
                # Another queue item still owns this task; leave it alone.
                continue
            if terminal_names[group] not in TERMINAL_TASK_STATUSES:
                continue
            name = str(item.get("name") or "")
            try:
                subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=20, check=False)
            except (OSError, subprocess.TimeoutExpired):
                continue
            out.append({"kind": "zombie-container", "container": name})
            self._emit("reconcile.zombie_container", detail=f"清理僵尸容器 {name}")
        return out

    def _release_stuck_orphans(self) -> list[dict[str, Any]]:
        """``orphaned`` + ``capacityHeld`` items that never got released."""
        grace = self.queue._orphan_grace_seconds()
        out: list[dict[str, Any]] = []
        with self.queue._lock:
            for item in list(self.queue._items):
                if str(item.get("status") or "") != "orphaned" or not item.get("capacityHeld"):
                    continue
                if item.get("manualReleased"):
                    continue
                since = parse_time(item.get("orphanedAt") or item.get("triggeredAt") or item.get("finishedAt") or item.get("startedAt"))
                age = (time.time() - since.timestamp()) if since else None
                if age is None or age < grace:
                    continue
                item_id = str(item.get("id") or "")
                live_reason = self.queue.live_task_reason(item)
                if live_reason:
                    notice = f"{live_reason}，保留并发名额直到任务停止"
                    if item.get("notice") != notice:
                        # Log the hold once per reason, not once a minute.
                        item["notice"] = notice
                        item["error"] = ""
                        self.queue._save()
                        self._emit("reconcile.orphan_held", taskId=item_id,
                                   projectCode=str(item.get("projectCode") or ""),
                                   detail=f"orphaned 超过 {int(age)} 秒但 {live_reason}，拒绝释放名额")
                    continue
                self.queue.slots.release_for_item(item_id)
                if self.platform is not None:
                    self.queue.refund_quota(item, self.platform, f"orphaned 超过 {int(age)} 秒无终态")
                item.update({
                    "status": "skipped",
                    "capacityHeld": False,
                    "orphaned": False,
                    "slotMarkers": [],
                    "releasedAt": utc_now(),
                    "autoReleased": True,
                    "notice": f"orphaned {int(age)} 秒后自动释放名额",
                    "error": f"orphaned {int(age)} 秒无终态，已自动释放名额并标记 skipped",
                })
                self.queue._save()
                out.append({"kind": "orphan-released", "itemId": item_id, "ageSeconds": age})
                self._emit("reconcile.orphan_released", taskId=item_id,
                           projectCode=str(item.get("projectCode") or ""),
                           detail=f"orphaned {int(age)} 秒，自动释放名额")
        return out

    def _guard_attempt_inflation(self) -> list[dict[str, Any]]:
        """Stop attempts from growing without bound on non-retryable failures."""
        out: list[dict[str, Any]] = []
        with self.queue._lock:
            for item in self.queue._items:
                attempts = int(item.get("attempts") or 0)
                if attempts < ATTEMPTS_ALERT_THRESHOLD:
                    continue
                if item.get("attemptsAlerted"):
                    continue
                item["attemptsAlerted"] = True
                self.queue._save()
                out.append({
                    "kind": "attempts-inflated",
                    "itemId": str(item.get("id") or ""),
                    "attempts": attempts,
                    "status": str(item.get("status") or ""),
                })
                self._emit("reconcile.attempts_inflated", taskId=str(item.get("id") or ""),
                           projectCode=str(item.get("projectCode") or ""),
                           detail=f"attempts={attempts} 异常膨胀，已停止累加并告警")
        return out

    def _force_settle_stale_quota(self) -> list[dict[str, Any]]:
        timeout = DEFAULT_QUOTA_SETTLE_TIMEOUT_SECONDS
        out: list[dict[str, Any]] = []
        with self.queue._lock:
            for item in list(self.queue._items):
                quota = item.get("quota") if isinstance(item.get("quota"), dict) else {}
                if str(quota.get("state") or "") != "claimed":
                    continue
                deducted = parse_time(quota.get("deductedAt"))
                age = (time.time() - deducted.timestamp()) if deducted else None
                if age is None or age < timeout:
                    continue
                item_id = str(item.get("id") or "")
                if self.platform is not None:
                    self.queue.refund_quota(item, self.platform, f"配额 claimed {int(age)} 秒未结算")
                else:
                    self.queue.settle_quota(item, success=False, reason=f"配额 claimed {int(age)} 秒未结算")
                self.queue._save()
                out.append({"kind": "quota-force-refund", "itemId": item_id, "ageSeconds": age})
                self._emit("reconcile.quota_force_refund", taskId=item_id,
                           projectCode=str(item.get("projectCode") or ""),
                           detail=f"claimed {int(age)} 秒未结算，强制回补")
        return out

    def _resync_from_result(self) -> list[dict[str, Any]]:
        """Queue items whose ``result.json`` disagrees with the recorded state."""
        out: list[dict[str, Any]] = []
        with self.queue._lock:
            for item in list(self.queue._items):
                if item.get("source") != "platform":
                    continue
                result_file = Path(str(item.get("resultFile") or ""))
                if not result_file or not result_file.is_file():
                    continue
                result = read_json(result_file, {})
                if not isinstance(result, dict):
                    continue
                stage = str(result.get("stage") or "")
                status = str(result.get("status") or "")
                recorded = str(item.get("stateStatus") or "")
                live = str(result.get("stateStatus") or "")
                if live and recorded and live != recorded:
                    item["stateStatus"] = live
                    self.queue._save()
                    # Normal phase progress; recorded on the item, not logged.
                    out.append({"kind": "state-resync", "itemId": str(item.get("id") or ""), "from": recorded, "to": live})
                elif live and not recorded:
                    item["stateStatus"] = live
                    self.queue._save()
                    out.append({"kind": "state-resync", "itemId": str(item.get("id") or ""), "to": live})
                if stage and status and str(item.get("status") or "") == "pending" and stage in {"desktop-submitted", "desktop-task-running"}:
                    item["status"] = "triggered"
                    item["capacityHeld"] = True
                    item["triggeredAt"] = item.get("triggeredAt") or utc_now()
                    self.queue._save()
                    out.append({"kind": "pending-resync", "itemId": str(item.get("id") or "")})
                    self._emit("reconcile.pending_resync", taskId=str(item.get("id") or ""),
                               projectCode=str(item.get("projectCode") or ""),
                               detail=f"result.json stage={stage}，重新标记为 triggered")
        return out
