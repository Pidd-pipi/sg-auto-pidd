#!/usr/bin/env python3
"""Format and follow a ChatGPT desktop thread rollout as a queue log."""
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

APP_DIR = Path(__file__).resolve().parent
PUBLIC_LOG_DIR = APP_DIR / "static" / "logs"
SENSITIVE_PATTERNS = (
    re.compile(r"(?i)(api[_-]?key|apikey|token|password|secret|cookie|csrf)\s*[=:]\s*([^\s,;]+)"),
    re.compile(r"(?i)authorization\s*:\s*bearer\s+[^\s]+"),
    re.compile(r"sk-[A-Za-z0-9_-]{8,}"),
    re.compile(r"benzhi-[A-Za-z0-9_-]{8,}"),
)


def safe_log_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip()).strip("-._")
    return cleaned or "queue-job"


def log_id_from_result_file(result_file: Path) -> str:
    name = result_file.name
    marker = "-platform-"
    if marker in name:
        return safe_log_id(name.split(marker, 1)[1].removesuffix(".result.json"))
    return safe_log_id(result_file.stem)


def public_log_path(log_id: str) -> Path:
    PUBLIC_LOG_DIR.mkdir(parents=True, exist_ok=True)
    return PUBLIC_LOG_DIR / f"{safe_log_id(log_id)}.log"


def redact_log_text(value: Any, limit: int = 4000) -> str:
    text = str(value or "").replace("\x00", "")
    for pattern in SENSITIVE_PATTERNS:
        if pattern.pattern.startswith("(?i)authorization"):
            text = pattern.sub("authorization: Bearer ***", text)
        elif pattern.pattern.startswith("sk-"):
            text = pattern.sub("sk-***", text)
        elif pattern.pattern.startswith("benzhi-"):
            text = pattern.sub("benzhi-***", text)
        else:
            text = pattern.sub(lambda match: f"{match.group(1)}=***", text)
    if len(text) > limit:
        text = text[: max(0, limit - 1)] + "…"
    return text


def _stdout_is(path: Path) -> bool:
    """Whether stdout is already redirected into ``path``."""
    try:
        out = os.fstat(sys.stdout.fileno())
        target = path.stat()
    except (OSError, ValueError, AttributeError):
        return False
    return (out.st_dev, out.st_ino) == (target.st_dev, target.st_ino)


class LogWriter:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")
        # The monitor redirects the worker's stdout into this same file; echoing
        # there as well wrote every line twice.
        self._echo = not _stdout_is(self.path)

    def emit(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        line = f"[{timestamp}] {redact_log_text(message)}\n"
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
        if self._echo:
            print(line, end="", flush=True)


def _event_time(event: dict[str, Any]) -> str:
    raw = str(event.get("timestamp") or "")
    match = re.search(r"T(\d{2}:\d{2}:\d{2})", raw)
    return match.group(1) if match else datetime.now().strftime("%H:%M:%S")


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        for key in ("text", "output_text", "input_text"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                parts.append(value.strip())
                break
    return "\n".join(parts)


def _reasoning_text(payload: dict[str, Any]) -> str:
    for key in ("summary_text", "summary", "raw_content"):
        value = payload.get(key)
        text = _content_text(value) if isinstance(value, list) else str(value or "")
        if text.strip():
            return text.strip()
    return ""


def format_rollout_event(event: dict[str, Any], task_name: str = "") -> list[str]:
    timestamp = _event_time(event)
    event_type = str(event.get("type") or "")
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    prefix = f"[{timestamp}]"

    if event_type == "session_meta":
        cwd = str(payload.get("cwd") or "")
        thread_id = str(payload.get("id") or payload.get("session_id") or "")
        return [f"{prefix} [环境] thread={thread_id} cwd={cwd}"]

    if event_type == "turn_context":
        cwd = str(payload.get("cwd") or "")
        model = str(payload.get("model") or "")
        return [f"{prefix} [环境] model={model} cwd={cwd}"]

    if event_type == "event_msg":
        message_type = str(payload.get("type") or "")
        if message_type == "task_started":
            return [f"{prefix} [任务] ChatGPT 已开始处理"]
        if message_type in {"turn_aborted", "stream_error", "error"}:
            return [f"{prefix} [错误] {redact_log_text(payload, 2000)}"]
        return []

    if event_type != "response_item":
        return []

    item_type = str(payload.get("type") or "")
    if item_type == "message":
        role = str(payload.get("role") or "")
        content = _content_text(payload.get("content"))
        if not content:
            return []
        if role == "developer":
            return []
        if role == "user" and (not task_name or task_name not in content):
            return []
        label = {"assistant": "助手", "user": "用户"}.get(role, role or "消息")
        return [f"{prefix} [{label}] {redact_log_text(content)}"]
    if item_type == "reasoning":
        content = _reasoning_text(payload)
        return [f"{prefix} [思考] {redact_log_text(content, 1200)}"] if content else []
    if item_type == "function_call":
        name = str(payload.get("name") or "tool")
        raw_args = payload.get("arguments")
        command = ""
        if isinstance(raw_args, str):
            try:
                parsed = json.loads(raw_args)
            except json.JSONDecodeError:
                parsed = {}
            if isinstance(parsed, dict):
                command = str(parsed.get("cmd") or parsed.get("command") or "")
        if command:
            return [f"{prefix} [命令] {redact_log_text(command, 2000)}"]
        return [f"{prefix} [工具] {name} {redact_log_text(raw_args, 1200)}"]
    if item_type == "function_call_output":
        output = payload.get("output")
        return [f"{prefix} [输出] {redact_log_text(output, 1600)}"] if output else []
    return []


class RolloutLogFollower:
    def __init__(self, path: Path, writer: LogWriter, task_name: str = ""):
        self.path = path
        self.writer = writer
        self.task_name = task_name
        self.position = 0

    def pump(self) -> int:
        if not self.path.is_file():
            return 0
        try:
            size = self.path.stat().st_size
        except OSError:
            return 0
        if size < self.position:
            self.position = 0
        written = 0
        try:
            with self.path.open("rb") as handle:
                handle.seek(self.position)
                while True:
                    raw = handle.readline()
                    if not raw:
                        break
                    self.position = handle.tell()
                    try:
                        event = json.loads(raw.decode("utf-8", errors="replace"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    if not isinstance(event, dict):
                        continue
                    for line in format_rollout_event(event, self.task_name):
                        self.writer.emit(line)
                        written += 1
        except OSError:
            return written
        return written


def find_rollout_by_thread(thread_id: str) -> Path | None:
    marker = str(thread_id or "").strip()
    if not marker:
        return None
    root = Path.home() / ".codex" / "sessions"
    return next(iter(root.rglob(f"rollout-*-{marker}.jsonl")), None)


def discover_rollout(task_name: str, timeout: float = 30.0) -> Path | None:
    """Locate the ChatGPT rollout file that belongs to ``task_name``.

    Scans the day's session directory directly; the old implementation went
    through ``codex_sessions.active_sessions``, which walked the whole session
    tree just to find one file.
    """
    marker = str(task_name or "").strip()
    if not marker:
        return None
    deadline = time.monotonic() + max(0.0, timeout)
    while True:
        now = datetime.now()
        base = Path.home() / ".codex" / "sessions" / f"{now:%Y}" / f"{now:%m}" / f"{now:%d}"
        if base.is_dir():
            for path in sorted(base.glob("rollout-*.jsonl"), key=lambda item: item.stat().st_mtime, reverse=True):
                try:
                    with path.open("r", encoding="utf-8", errors="replace") as handle:
                        head = handle.read(512 * 1024)
                except OSError:
                    continue
                if marker in head:
                    return path
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.5)
