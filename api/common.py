"""Shared primitives for the sologsb scheduler: config, IO, secrets, processes."""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

APP_DIR = Path(__file__).resolve().parent.parent
DEFAULT_ROOT = APP_DIR.parent
DEFAULT_SKILL_SCRIPT = Path.home() / ".codex" / "skills" / "sologsb-0917" / "scripts" / "sologsb.py"
PLATFORM_SCRIPTS = Path.home() / ".codex" / "skills" / "solo-annotation-loop" / "scripts"
CONFIG_PATH = APP_DIR / "config.json"
STATE_DIR = APP_DIR / ".state"
SETTINGS_PATH = STATE_DIR / "settings.json"
QUEUE_STATE_PATH = STATE_DIR / "queue.json"
SCHEDULER_LOG_PATH = STATE_DIR / "scheduler.jsonl"
DISMISSED_TASKS_PATH = STATE_DIR / "dismissed-tasks.json"
AUTO_STATE_PATH = STATE_DIR / "auto.json"
MANAGER_AUTH_STATE_PATH = STATE_DIR / "platform-auth.json"

EVENT_LIMIT = 80
DEFAULT_PAGE_SIZE = 100
SUBS_TTL = 60
DEFAULT_PLATFORM_START_TIMEOUT_SECONDS = 300
DEFAULT_QUEUE_RETRY_BACKOFF_SECONDS = 90
DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS = 12 * 60 * 60
DEFAULT_STALLED_TASK_RETRY_SECONDS = 10 * 60
DEFAULT_STALLED_TASK_RETRY_LIMIT = 1
# The next N pending items the queue page previews; auto refill never
# reshuffles them, so what the operator sees is what launches next.
AUTO_REFILL_PREVIEW_SIZE = 10
MANAGER_TOKEN_SERVICE = "solo-manager-token"
MANAGER_PASSWORD_SERVICE = "solo-manager-password"
STOP_TASKS_PATH = Path(os.environ.get("SOLOSB_STOP_TASKS_PATH", str(STATE_DIR / "stop-tasks.json")))

DEFAULT_MANAGER_USERNAME = "admin"
DEFAULT_MANAGER_TIMEOUT_SECONDS = 5
MANAGER_TOKEN_SCOPES = [
    "project:read",
    "task:write",
    "variant:read",
]

SIDES = ("A", "B")
QUEUE_ACTIVE_STATUSES = {"launching", "running", "triggered", "orphaned"}
QUEUE_TERMINAL_STATUSES = {"done", "failed", "skipped"}
TASK_FAILURE_STATUSES = {"blocked", "failed", "error"}
TERMINAL_TASK_STATUSES = {
    "semantic_review_required",
    "ab_clean",
    "verified",
    "gsb_ready",
    "recorded",
    "complete",
}
CANDIDATE_PHASE_DONE_STATUSES = {
    *TERMINAL_TASK_STATUSES,
    "candidates_ready",
    "repo_ready",
    "a_staged",
    "b_staged",
    "blocked",
    "attempt_invalid",
    "stopped",
    "failed",
    "error",
}
RUNNABLE_TASK_STATUSES = {
    "repo_ready",
    "running",
    "a_staged",
    "b_staged",
    "semantic_review_required",
    "attempt_invalid",
    "blocked",
}
SIDE_DONE_STATUSES = {"staged", "clean"}
SIDE_FAILED_STATUSES = {"attempt_invalid", "blocked", "invalidated"}
CANDIDATE_ID_RE = re.compile(r"candidate-[1-9][0-9]*")

DEFAULT_KEY_MAX_PARALLEL_REQUESTS = 8
DEFAULT_KEY_RESERVED_SLOTS = 4
DEFAULT_MAX_CANDIDATE_CONTAINERS = 4
DEFAULT_CONTAINER_REFILL_BELOW = 3

# Scheduler modes. ``tasks`` keeps the historical "N tasks in flight" semantics;
# ``containers`` keeps the running candidate-container count pinned to a target.
SCHEDULE_MODE_TASKS = "tasks"
SCHEDULE_MODE_CONTAINERS = "containers"
SCHEDULE_MODES = (SCHEDULE_MODE_TASKS, SCHEDULE_MODE_CONTAINERS)

DEFAULT_STARTUP_TIMEOUT_SECONDS = 300
MIN_STARTUP_TIMEOUT_SECONDS = 300
MAX_STARTUP_TIMEOUT_SECONDS = 600
DEFAULT_CONTAINER_RESERVE_SECONDS = 420
MIN_CONTAINER_RESERVE_SECONDS = 300
MAX_CONTAINER_RESERVE_SECONDS = 600
DEFAULT_RECONCILE_SECONDS = 60
# On startup the queue has not yet read the task tree or docker, so a naive tick
# would see "zero containers, zero tasks" and fire a whole batch.  Starts are
# held off for this long while the state is read.
DEFAULT_STARTUP_GRACE_SECONDS = 100
DEFAULT_QUOTA_SETTLE_TIMEOUT_SECONDS = 6 * 3600
DEFAULT_ORPHAN_GRACE_SECONDS = 1800
# A candidate marked ``running`` that gets no container for this long while the
# container limit has room is treated as belonging to a dead executor.
DEFAULT_PHANTOM_DEMAND_SECONDS = 600
MIN_PHANTOM_DEMAND_SECONDS = 120
MAX_PHANTOM_DEMAND_SECONDS = 7200

DEFAULT_AUTO_TRIGGER_PROMPT = (
    "使用 `$sologsb-0917`，在监控队列提供的监控工作目录下执行一道完整的 Pair-wise GSB。"
    "仅本地交付：严禁提交 GSB 表单，严禁调用 SOLO2 写接口。\n\n"
    "环境要求：\n"
    "- Solo Manager 必须使用 admin 对应的有效登录态；当前登录态不是 admin 时立即停止。\n"
    "- Claude Code Key 从钥匙串 `benzhi-claude-code-gaobo-pi-a453493f` 读取，通过 `SOLOSB_CLAUDE_KEY` 注入；"
    "禁止把明文 Key 写入任务目录、状态文件、轨迹或日志。\n"
    "- 单 Key 全局硬上限为 {{max_containers}} 个候选容器，按“{{max_tasks}} 个任务、每个任务 "
    "{{candidates_per_task}} 个候选”共享名额；预计当前数量加本批候选数超过 {{max_containers}} 时等待。\n"
    "- 当前调度模式：{{schedule_mode}}。\n\n"
    "项目接入：\n"
    "- 已选定项目：`{{selected_project}}`。\n"
    "- 非空时必须使用其中的 projectCode 或 projectId 接入，不得静默换题；无法按要求接入时立即停止。\n"
    "- 为空时从 Solo Manager 正式选择 `{{task_type}}`、`{{difficulty}}` 项目，"
    "记录 taskId、taskNo、variant、选择结果和选择前后配额变化；平台未返回的字段写“平台未返回”，不得猜测。\n"
    "- `init` 必须显式传入 `--task-root` 指向监控工作目录下的唯一任务目录，不得依赖监控台当前目录。\n"
    "- 仅下载源码不消耗任务配额；没有正式选择时，不得伪造配额消耗。\n\n"
    "执行门禁：\n"
    "- 所有候选共用同一份 UTF-8 题目提示词，字节完全一致；不得选择“代码理解”。\n"
    "- 竞速阶段必须显式执行 `run --side both --candidates {{candidates_per_task}} "
    "--attempts 6 --base-url {{base_url}}`，预拉 {{candidates_per_task}} 份候选并并行运行；"
    "前两名按完成顺序映射 A/B。\n"
    "- A/B 映射完成后才运行 `github-init`，创建 GitHub `main`、`A`、`B`；两侧语义审核都通过后才允许 "
    "`publish` 原子发布产物。\n"
    "- 遇到 429 `max_parallel_requests` 时先等待 Key 恢复，不得立即重启新候选；"
    "恢复后重开时仍使用新容器、新 Claude home 和新 SessionID，实际尝试次数照记。\n"
    "- `end_turn` 不能单独证明完成，必须结合轨迹、diff 和产物语义判断。\n"
    "- 每个 A/B 产物必须执行真实依赖准备、测试、生产构建或启动验证；"
    "结论必须绑定轨迹、commit、命令输出或退出码。\n"
    "- A/B 必须分别使用 Otty 录制真实视频，统一 1280x720；失败也保留真实过程，"
    "禁止 headless、伪造或只录成功片段。\n"
    "- 基础设施或技能门禁不通过时立即停止并报告准确原因；A/B 产物自身失败可以继续保留失败证据，"
    "但不得跳过该侧录屏。\n\n"
    "最终交付：\n"
    "- 最终回复严格使用 `references/final-delivery-format.md` 的八个二级标题，不得增删标题或添加额外说明。\n"
    "- 本地交付时，“SOLO2 推送结果”固定写“未执行（仅本地交付）”。\n"
    "- Excel、字段说明、轨迹和视频均使用绝对路径；视频使用 Markdown 图片语法内嵌。"
)

DEFAULT_CONFIG: dict[str, Any] = {
    "roots": [str(DEFAULT_ROOT)],
    "skillScript": str(DEFAULT_SKILL_SCRIPT),
    "server": {
        "host": "127.0.0.1",
        "port": 8790,
        "allowRemoteActions": False,
    },
    "platform": {
        "managerBaseUrl": os.environ.get("SOLO_MANAGER_BASE_URL", "").rstrip("/"),
        "candidateTtlSeconds": 30,
        "taskType": "0-1代码生成",
        "difficulty": "困难",
        "mergeProjectPool": False,
    },
    "automation": {
        "tickSeconds": 3,
        "capacity": 2,
        "cooldownSeconds": 0,
        "startupTimeoutSeconds": DEFAULT_STARTUP_TIMEOUT_SECONDS,
        "terminalStabilitySeconds": 6,
        "waitTimeoutSeconds": DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS,
        "maxContainers": DEFAULT_MAX_CANDIDATE_CONTAINERS,
        "candidatesPerTask": 2,
        "anthropicBaseUrl": "https://llm2.jzxhnh.com",
        "scheduleMode": SCHEDULE_MODE_CONTAINERS,
        "reconcileSeconds": DEFAULT_RECONCILE_SECONDS,
        "startupGraceSeconds": DEFAULT_STARTUP_GRACE_SECONDS,
        "excludedProjectCodes": [],
        "maxAttempts": 3,
        "retryBackoffSeconds": DEFAULT_QUEUE_RETRY_BACKOFF_SECONDS,
        "stalledTaskRetrySeconds": DEFAULT_STALLED_TASK_RETRY_SECONDS,
        "stalledTaskRetryLimit": DEFAULT_STALLED_TASK_RETRY_LIMIT,
        "autoRefill": {
            "enabled": False,
            "intervalSeconds": 60,
            "targetPending": 20,
            "batchSize": 20,
            "randomize": True,
            "shuffleExisting": True,
            "taskTypes": ["feature迭代"],
            "taskTypeWeights": {"feature迭代": 80, "0-1代码生成": 20},
            "difficulty": "困难",
        },
        "paused": True,
        # What a process start does with the saved ``paused`` (see
        # SchedulerService._decide_pause_on_start): ``crash-loop`` resumes it
        # unless the service keeps dying, ``always`` pauses, ``never`` resumes.
        "pauseOnStart": "crash-loop",
        "reapOrphanWorkers": True,
        # Fallback policies (api/guard.py); ``observe`` only flags and logs.
        "guard": {
            "mode": "observe",
            "candidatePhaseHours": 6,
            "noProgressMinutes": 60,
            "leakedProcessMinutes": 60,
        },
        # Disk launch gate and cleanup of finished tasks (api/housekeeping.py).
        "housekeeping": {
            "enabled": True,
            "minFreeGB": 30,
            "afterHours": 6,
            "intervalMinutes": 30,
        },
        "promptTemplate": DEFAULT_AUTO_TRIGGER_PROMPT,
    },
    "monitor": {
        "pollSeconds": 3,
        "dockerCacheSeconds": 2,
        "snapshotCacheSeconds": 1,
        "parseTraceOnSnapshot": False,
        "traceMaxBytes": 1 * 1024 * 1024,
        "sessionMaxIdleSeconds": 6 * 3600,
        "autoResume": {
            "enabled": False,
            "tickSeconds": 15,
            "staleSeconds": 420,
            "cooldownSeconds": 180,
            "maxRelaunchesPerSide": 3,
            "windowSeconds": 3600,
            "maxConcurrentJobs": 2,
        },
    },
    "solo2": {
        "enabled": True,
        "apiBaseUrl": (os.environ.get("SOLO2_SERVER", "").rstrip("/") + "/api/v1") if os.environ.get("SOLO2_SERVER", "").strip() else "",
        "monitorUrl": "http://127.0.0.1:8787",
        "preferMonitor": True,
        "pageSize": DEFAULT_PAGE_SIZE,
    },
}


class MonitorError(RuntimeError):
    """Raised for expected monitor failures."""


class ManagerApiError(MonitorError):
    """Solo Manager API failure with an optional HTTP status."""

    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = int(status or 0)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def iso_from_timestamp(value: float | None) -> str:
    if not value:
        return ""
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def age_seconds(value: Any, now: float | None = None) -> float | None:
    dt = parse_time(value)
    if dt is None:
        return None
    current = datetime.now(timezone.utc) if now is None else datetime.fromtimestamp(now, timezone.utc)
    return max(0.0, (current - dt).total_seconds())


def safe_slug(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return cleaned or "task"


def short_hash(value: str, length: int = 12) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:length]


def queue_prompt_sha256(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()


def secret_fingerprint(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def truncate(value: Any, limit: int = 220) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"


_SECRET_PATTERNS = (
    re.compile(r"(?i)(api[_-]?key|apikey|token|password|secret|cookie|csrf)\s*[=:]\s*([^\s,;]+)"),
    re.compile(r"(?i)authorization\s*:\s*bearer\s+[^\s]+"),
    re.compile(r"sk-[A-Za-z0-9_-]{8,}"),
)


def redact_text(value: Any, limit: int = 500) -> str:
    text = truncate(value, limit)
    for pattern in _SECRET_PATTERNS:
        if pattern.pattern.startswith("(?i)authorization"):
            text = pattern.sub("authorization: Bearer ***", text)
        elif pattern.pattern.startswith("sk-"):
            text = pattern.sub("sk-***", text)
        else:
            text = pattern.sub(lambda m: f"{m.group(1)}=***", text)
    return text


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return default


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(path: Path | None = None, roots: Iterable[str] | None = None) -> dict[str, Any]:
    config_path = Path(path or CONFIG_PATH)
    raw = read_json(config_path, {})
    if not isinstance(raw, dict):
        raw = {}
    config = deep_merge(DEFAULT_CONFIG, raw)
    if roots:
        resolved = [str(Path(item).expanduser().resolve()) for item in roots]
        config["roots"] = resolved
        config.setdefault("monitor", {})["activeRoots"] = resolved
    config["_configPath"] = str(config_path)
    return config


def save_config(config: dict[str, Any], path: Path | None = None) -> None:
    target = Path(path or CONFIG_PATH)
    clean = {key: value for key, value in config.items() if not key.startswith("_")}
    atomic_write_json(target, clean)


def auto_refill_interval_seconds(cfg: dict[str, Any]) -> int:
    return max(30, int(cfg.get("intervalSeconds") or 180))


def public_auto_refill_config(config: dict[str, Any]) -> dict[str, Any]:
    cfg = (config.get("automation") or {}).get("autoRefill") or {}
    task_types = [str(value).strip() for value in (cfg.get("taskTypes") or []) if str(value).strip()]
    weights = cfg.get("taskTypeWeights") if isinstance(cfg.get("taskTypeWeights"), dict) else {}
    return {
        "enabled": bool(cfg.get("enabled")),
        "intervalSeconds": auto_refill_interval_seconds(cfg),
        "previewSize": AUTO_REFILL_PREVIEW_SIZE,
        "targetPending": max(1, int(cfg.get("targetPending") or 20)),
        "taskTypes": task_types,
        "taskTypeWeights": {str(key): value for key, value in weights.items()},
        "minPendingPerTaskType": max(0, int(cfg.get("minPendingPerTaskType") or 0)),
    }


def queue_error_retryable(error: Any) -> bool:
    text = " ".join(str(error or "").split())
    if not text:
        return True
    non_retryable = (
        "桌面任务启动超时",
        "等待 ChatGPT 任务完成超时",
        "ChatGPT 任务状态为",
        "任务初始化失败",
        "配额",
        "没有找到",
        "不可选用",
        "缺少源码",
        "占用",
        "权限",
        "辅助功能权限",
        "任务类型",
        "不是有效",
        "无法解析",
    )
    return not any(marker in text for marker in non_retryable)


def queue_failure_retryable(result: dict[str, Any] | None, error: Any) -> bool:
    """Only retry failures that happened before a desktop task was submitted."""
    stage = str((result or {}).get("stage") or "")
    if stage in {
        "desktop-start-timeout",
        "desktop-submitted",
        "desktop-task-running",
        "task-init",
        "task-state",
        "wait-timeout",
    }:
        return False
    return queue_error_retryable(error)


# ``security`` can block on an unlock prompt when the login keychain is locked
# (screen locked overnight).  Without a timeout that froze whichever scheduler
# loop asked for the Manager password, and nothing ever unfroze it.
KEYCHAIN_TIMEOUT_SECONDS = 10


def keychain_read(service: str) -> str:
    try:
        proc = subprocess.run(
            ["security", "find-generic-password", "-a", os.environ.get("USER", ""), "-s", service, "-w"],
            text=True,
            capture_output=True,
            timeout=KEYCHAIN_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return proc.stdout.strip() if proc.returncode == 0 else ""


def keychain_write(service: str, value: str) -> None:
    if not str(value or "").strip():
        raise MonitorError(f"拒绝写入空凭据到 Keychain: {service}")
    try:
        proc = subprocess.run(
            ["security", "add-generic-password", "-a", os.environ.get("USER", ""), "-s", service, "-U", "-w", value],
            text=True,
            capture_output=True,
            timeout=KEYCHAIN_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise MonitorError(f"写入 Keychain 超时（钥匙串可能已锁定）: {service}") from exc
    except OSError as exc:
        raise MonitorError(f"写入 Keychain 失败: {service}: {exc}") from exc
    if proc.returncode != 0:
        raise MonitorError(f"写入 Keychain 失败: {service}: {proc.stderr.strip()}")


def manager_request_json(
    base_url: str,
    path: str,
    *,
    token: str = "",
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = DEFAULT_MANAGER_TIMEOUT_SECONDS,
) -> Any:
    """Call Solo Manager directly so stale hosts fail fast instead of hanging for minutes."""
    base = str(base_url or "").strip().rstrip("/")
    api_base = base if base.endswith("/api/v1") else base + "/api/v1"
    url = api_base + (path if str(path).startswith("/") else "/" + str(path))
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Accept", "application/json")
    if data is not None:
        request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(request, timeout=max(0.5, float(timeout))) as response:
            raw = response.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        detail = redact_text(body, 500)
        raise ManagerApiError(f"{method} {url} 返回 HTTP {exc.code}: {detail}", exc.code) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise ManagerApiError(f"{method} {url} 连接失败: {exc}") from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ManagerApiError(f"{method} {url} 返回了无效 JSON: {exc}") from exc


def discover_manager_base_urls(config: dict[str, Any]) -> list[str]:
    """Prefer the most recently used live Manager host, then configured fallbacks."""
    discovered: list[tuple[float, str]] = []
    seen: set[str] = set()
    for root_value in config.get("roots") or []:
        root = Path(str(root_value)).expanduser()
        if not root.is_dir():
            continue
        for pattern in ("*/monitor/platform-selection.json", "*/.solo-platform.json", "*/platform-work/*/.solo-platform.json"):
            for path in root.glob(pattern):
                try:
                    mtime = path.stat().st_mtime
                except OSError:
                    continue
                data = read_json(path, {})
                value = ""
                if isinstance(data, dict):
                    selection = data.get("selection")
                    nested = selection if isinstance(selection, dict) else data.get("platformSelection")
                    value = str(data.get("baseUrl") or (nested.get("baseUrl") if isinstance(nested, dict) else "") or "")
                value = value.strip().rstrip("/")
                if value and value not in seen:
                    seen.add(value)
                    discovered.append((mtime, value))
    discovered.sort(key=lambda item: item[0], reverse=True)

    candidates: list[str] = [value for _, value in discovered[:12]]
    cfg = config.get("platform") or {}
    for value in [
        cfg.get("managerBaseUrl"),
        *(cfg.get("managerCandidates") or []),
        os.environ.get("SOLO_MANAGER_BASE_URL", ""),
    ]:
        value = str(value or "").strip().rstrip("/")
        if value and value not in candidates:
            candidates.append(value)
    return candidates


def pid_alive(pid: Any) -> bool:
    try:
        value = int(pid)
    except (TypeError, ValueError):
        return False
    if value <= 0:
        return False
    try:
        os.kill(value, 0)
        return True
    except OSError:
        return False


def pid_command(pid: Any) -> str:
    try:
        result = subprocess.run(
            ["ps", "-p", str(int(pid)), "-o", "command="],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        return result.stdout.strip()
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return ""


def persisted_job_process_alive(job: dict[str, Any]) -> bool:
    if not pid_alive(job.get("pid")):
        return False
    command = pid_command(job.get("pid"))
    if not command:
        return True
    lowered = command.lower()
    return "queue_worker.py" in lowered or "sologsb" in lowered


def runner_pid_alive(record: dict[str, Any], task_root: Path, side: str, process_table: Any = None) -> bool:
    pid = record.get("runPid")
    if not pid_alive(pid):
        return False
    if process_table is None:
        command = pid_command(pid)
    else:
        command = process_table.command(pid)
    if not command:
        return True
    side_text = str(side).upper()
    side_ok = f"--side {side_text}" in command or "--side both" in command
    task_ok = str(task_root) in command or "sologsb" in command.lower()
    return bool(side_ok and task_ok)


class ProcessTable:
    """One ``ps`` snapshot shared by every liveness check inside a tick."""

    def __init__(self, ttl: float = 2.0):
        self.ttl = max(0.0, float(ttl))
        self._lock = threading.Lock()
        self._at = 0.0
        self._commands: dict[int, str] = {}

    def _refresh(self) -> None:
        try:
            result = subprocess.run(
                ["ps", "-axo", "pid=,command="],
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            self._commands = {}
            return
        commands: dict[int, str] = {}
        for line in result.stdout.splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) < 2:
                continue
            try:
                commands[int(parts[0])] = parts[1]
            except ValueError:
                continue
        self._commands = commands

    def command(self, pid: Any) -> str:
        try:
            value = int(pid)
        except (TypeError, ValueError):
            return ""
        now = time.monotonic()
        with self._lock:
            if now - self._at >= self.ttl:
                self._refresh()
                self._at = time.monotonic()
            return self._commands.get(value, "")

    def invalidate(self) -> None:
        with self._lock:
            self._at = 0.0


class FileCache:
    """Memoize JSON/text reads by ``(path, mtime, size)``.

    Task directories hold a handful of state files that are read several times
    per snapshot (state.json, attempt.json, result.json).  Keying on the stat
    signature means one tick never decodes the same file twice, while an edit is
    picked up immediately.
    """

    def __init__(self, max_entries: int = 4096):
        self._lock = threading.Lock()
        self._entries: dict[str, tuple[float, int, Any]] = {}
        self._max_entries = max(400, int(max_entries))
        self.hits = 0
        self.misses = 0

    def _signature(self, path: Path) -> tuple[float, int] | None:
        try:
            stat = path.stat()
        except OSError:
            return None
        return stat.st_mtime_ns, stat.st_size

    def json(self, path: Path, default: Any = None) -> Any:
        key = str(path)
        signature = self._signature(path)
        if signature is None:
            return default
        with self._lock:
            cached = self._entries.get(key)
            if cached is not None and (cached[0], cached[1]) == signature:
                self.hits += 1
                return cached[2]
        value = read_json(path, default)
        with self._lock:
            if len(self._entries) >= self._max_entries:
                # Drop the oldest insertion; the working set is small enough that
                # a plain eviction is cheaper than maintaining an LRU list.
                for stale in list(self._entries)[: max(1, self._max_entries // 4)]:
                    self._entries.pop(stale, None)
            self._entries[key] = (signature[0], signature[1], value)
            self.misses += 1
        return value

    def text(self, path: Path, limit: int = 0) -> str:
        signature = self._signature(path)
        if signature is None:
            return ""
        key = f"text:{path}"
        with self._lock:
            cached = self._entries.get(key)
            if cached is not None and (cached[0], cached[1]) == signature:
                self.hits += 1
                return cached[2]
        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            raw = ""
        if limit and len(raw) > limit:
            raw = raw[:limit]
        with self._lock:
            if len(self._entries) >= self._max_entries:
                for stale in list(self._entries)[: max(1, self._max_entries // 4)]:
                    self._entries.pop(stale, None)
            self._entries[key] = (signature[0], signature[1], raw)
            self.misses += 1
        return raw

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"entries": len(self._entries), "hits": self.hits, "misses": self.misses}


def tail_lines(path: Path, lines: int, *, chunk_size: int = 64 * 1024) -> list[str]:
    """Read the last ``lines`` lines without decoding the whole file.

    Trajectory files run to several megabytes; ``read_text`` on every poll was
    the single most expensive operation in the old snapshot path.
    """
    wanted = max(1, int(lines))
    try:
        size = path.stat().st_size
    except OSError:
        return []
    if size == 0:
        return []
    blocks: list[bytes] = []
    remaining = size
    try:
        with path.open("rb") as handle:
            while remaining > 0 and len(blocks) < 512:
                step = min(chunk_size, remaining)
                remaining -= step
                handle.seek(remaining)
                blocks.insert(0, handle.read(step))
                if b"\n" in blocks[0][:-1] or b"\n" in blocks[0]:
                    # Count newlines so far; stop once we have enough lines.
                    if sum(block.count(b"\n") for block in blocks) > wanted:
                        break
    except OSError:
        return []
    text = b"".join(blocks).decode("utf-8", errors="replace")
    result = text.splitlines()
    return result[-wanted:]


def clamp_int(value: Any, low: int, high: int, fallback: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return fallback
    return max(low, min(high, number))


def render_auto_trigger_prompt(
    template: str,
    project: dict[str, Any] | None,
    *,
    task_type: str = "0-1代码生成",
    difficulty: str = "困难",
    base_url: str = "https://llm2.jzxhnh.com",
    max_tasks: int = 2,
    max_containers: int = 4,
    candidates_per_task: int = 2,
    schedule_mode: str = SCHEDULE_MODE_CONTAINERS,
) -> str:
    """Render the platform task prompt with the selected project snapshot."""
    project = project if isinstance(project, dict) else {}
    code = str(project.get("code") or "").strip()
    name = str(project.get("name") or "").strip()
    selected = " · ".join(part for part in (code, name) if part) or "未指定（由执行器从 Solo Manager 选择）"
    mode_label = "容器优先（保持运行中的候选容器数等于设定值）" if schedule_mode == SCHEDULE_MODE_CONTAINERS else "任务数量优先（保持并行任务数等于设定值）"
    replacements = {
        "{{selected_project}}": selected,
        "{{project_code}}": code,
        "{{project_name}}": name,
        "{{task_type}}": task_type,
        "{{difficulty}}": difficulty,
        "{{base_url}}": str(base_url or "https://llm2.jzxhnh.com").rstrip("/"),
        "{{max_tasks}}": str(max(1, int(max_tasks))),
        "{{max_containers}}": str(max(1, int(max_containers))),
        "{{candidates_per_task}}": str(max(1, int(candidates_per_task))),
        "{{schedule_mode}}": mode_label,
    }
    rendered = str(template or DEFAULT_AUTO_TRIGGER_PROMPT)
    for marker, value in replacements.items():
        rendered = rendered.replace(marker, value)
    return rendered


class SettingsStore:
    """``.state/settings.json`` — operator-owned state that is not task data.

    Passwords never land here: only the Keychain service name plus a
    saved/unsaved marker are persisted.
    """

    def __init__(self, path: Path | None = None):
        self.path = Path(path or SETTINGS_PATH)
        self._lock = threading.RLock()
        self._data: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        raw = read_json(self.path, {})
        self._data = raw if isinstance(raw, dict) else {}

    def get(self) -> dict[str, Any]:
        with self._lock:
            return copy.deepcopy(self._data)

    def update(self, patch: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            merged = deep_merge(self._data, patch if isinstance(patch, dict) else {})
            merged["updatedAt"] = utc_now()
            self._data = merged
            atomic_write_json(self.path, merged)
            return copy.deepcopy(merged)
