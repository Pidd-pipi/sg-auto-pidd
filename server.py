#!/usr/bin/env python3
"""HTTP and SSE front end for the sologsb scheduler.

This module is deliberately thin: routing, auth of remote clients, static file
serving with real caching, and one SSE endpoint.  All behaviour lives in
``api/``.
"""
from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import mimetypes
import os
import signal
import socket
import sys
import time
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from api.common import CONFIG_PATH, MonitorError, load_config, save_config, utc_now
from api.service import SchedulerService
from api.common import SettingsStore
from api.version import APP_VERSION, version_info

STATIC_DIR = Path(__file__).resolve().parent / "static"
INDEX_PATH = STATIC_DIR / "index.html"
IMMUTABLE_SUFFIXES = {".woff2", ".woff", ".ttf", ".otf"}
GZIP_MIN_BYTES = 512
MAX_LOG_LIMIT = 2000
# Every page polls and holds an SSE stream, so logging each 200/304 grew the
# unattended log by megabytes a day while hiding the lines that matter.
ACCESS_LOG = os.environ.get("SOLOGSB_ACCESS_LOG", "").lower() in {"1", "true", "yes"}


def lan_ip() -> str:
    for interface in ("en0", "en1", "en2"):
        try:
            import subprocess

            result = subprocess.run(
                ["ipconfig", "getifaddr", interface],
                capture_output=True,
                text=True,
                timeout=2,
                check=False,
            )
            value = result.stdout.strip()
            if value and not value.startswith("127."):
                return value
        except Exception:
            continue
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        value = sock.getsockname()[0]
        sock.close()
        return value
    except OSError:
        return ""


class MonitorInstanceLock:
    """Keep one scheduler owner per monitor state directory."""

    def __init__(self, path: Path):
        self.path = path
        self._handle = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.seek(0)
            owner = handle.read().strip() or "未知进程"
            handle.close()
            raise RuntimeError(f"已有监控实例在运行（{owner}）") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()} startedAt={time.strftime('%Y-%m-%dT%H:%M:%S%z')}\n")
        handle.flush()
        os.fsync(handle.fileno())
        self._handle = handle

    def release(self) -> None:
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


class MonitorHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128

    def __init__(self, address, handler, service: SchedulerService):
        super().__init__(address, handler)
        self.service = service
        self.started_at = time.time()


class Handler(BaseHTTPRequestHandler):
    server_version = f"sologsb-monitor/{APP_VERSION}"
    protocol_version = "HTTP/1.1"

    @property
    def service(self) -> SchedulerService:
        return self.server.service  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    def log_request(self, code: Any = "-", size: Any = "-") -> None:
        try:
            status = int(getattr(code, "value", code))
        except (TypeError, ValueError):
            status = 0
        if ACCESS_LOG or status >= 400:
            super().log_request(code, size)

    # -- response helpers ------------------------------------------------- #
    def _accepts_gzip(self) -> bool:
        return "gzip" in str(self.headers.get("Accept-Encoding") or "").lower()

    def _send(
        self,
        body: bytes,
        content_type: str,
        status: int = 200,
        *,
        cache: str = "no-store",
        etag: str = "",
        head_only: bool = False,
    ) -> None:
        headers: list[tuple[str, str]] = [("Content-Type", content_type)]
        payload = body
        if etag:
            headers.append(("ETag", etag))
            if str(self.headers.get("If-None-Match") or "") == etag:
                self.send_response(HTTPStatus.NOT_MODIFIED)
                for key, value in headers:
                    self.send_header(key, value)
                self.send_header("Cache-Control", cache)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
        if self._accepts_gzip() and len(payload) >= GZIP_MIN_BYTES:
            payload = gzip.compress(payload, 6)
            headers.append(("Content-Encoding", "gzip"))
        headers.append(("Content-Length", str(len(payload))))
        headers.append(("Cache-Control", cache))
        headers.append(("X-Content-Type-Options", "nosniff"))
        self.send_response(status)
        for key, value in headers:
            self.send_header(key, value)
        self.end_headers()
        if head_only or not payload:
            return
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, value: Any, status: int = 200) -> None:
        body = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self._send(body, "application/json; charset=utf-8", status)

    def _text(self, value: str, status: int = 200, content_type: str = "text/plain; charset=utf-8") -> None:
        self._send(value.encode("utf-8"), content_type, status)

    def _read_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        raw = self.rfile.read(max(0, min(length, 1024 * 1024)))
        if not raw:
            return {}
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            raise MonitorError("请求体必须是 JSON 对象")
        return value

    def _remote_actions_allowed(self) -> bool:
        cfg = self.service.config
        if (cfg.get("server") or {}).get("allowRemoteActions", False):
            return True
        remote = str(self.client_address[0] if self.client_address else "")
        return remote in {"127.0.0.1", "::1", "localhost"}

    # -- static ----------------------------------------------------------- #
    def _static_path(self, relative: str) -> Path | None:
        candidate = (STATIC_DIR / relative).resolve()
        try:
            candidate.relative_to(STATIC_DIR.resolve())
        except ValueError:
            return None
        return candidate if candidate.is_file() else None

    def _serve_static(self, relative: str, head_only: bool = False) -> None:
        candidate = self._static_path(relative)
        if candidate is None:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        content_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
        if candidate.suffix == ".html":
            content_type = "text/html; charset=utf-8"
        try:
            stat = candidate.stat()
            body = candidate.read_bytes()
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if candidate.suffix in IMMUTABLE_SUFFIXES:
            cache = "public, max-age=31536000, immutable"
        elif candidate.suffix in {".html", ".css", ".js"}:
            cache = "no-cache"
        else:
            cache = "public, max-age=300"
        etag = f'"{hashlib.sha1(str(stat.st_mtime_ns).encode()).hexdigest()[:16]}-{stat.st_size}"'
        self._send(body, content_type, cache=cache, etag=etag, head_only=head_only)

    # -- SSE -------------------------------------------------------------- #
    def _serve_stream(self) -> None:
        self.close_connection = True
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        try:
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return

        hub = self.service.hub
        log = self.service.log
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        try:
            after_seq = int((query.get("afterSeq") or ["0"])[0])
        except ValueError:
            after_seq = 0
        include_logs = str((query.get("logs") or ["1"])[0]).lower() not in {"0", "false", "no"}

        hub_key, hub_queue = hub.subscribe()
        log_key, log_queue = log.subscribe() if include_logs else (0, None)
        pending = log.after(after_seq, limit=500) if include_logs else []
        last_ping = 0.0
        try:
            initial = hub.full()
            self._sse_send("snapshot", initial)
            if pending:
                self._sse_send("log", {"entries": pending, "lastSeq": log.last_seq()})
            while True:
                drained = False
                while True:
                    try:
                        message = hub_queue.popleft()
                    except IndexError:
                        break
                    self._sse_send(message.get("type") or "tasks", message)
                    drained = True
                if log_queue is not None:
                    while True:
                        try:
                            entry = log_queue.popleft()
                        except IndexError:
                            break
                        self._sse_send("log", {"entries": [entry], "lastSeq": log.last_seq()})
                        drained = True
                now = time.monotonic()
                if not drained:
                    # Only send a keepalive every few seconds; the connection is
                    # otherwise idle and there is nothing to say.
                    if now - last_ping >= 5.0:
                        self._sse_send("ping", {"at": utc_now()})
                        last_ping = now
                    time.sleep(0.25)
                else:
                    last_ping = now
                    time.sleep(0.02)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            hub.unsubscribe(hub_key)
            if include_logs:
                log.unsubscribe(log_key)

    def _sse_send(self, event: str, data: Any) -> None:
        payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        frame = f"event: {event}\ndata: {payload}\n\n".encode("utf-8")
        self.wfile.write(frame)
        self.wfile.flush()

    # -- routing ---------------------------------------------------------- #
    def do_HEAD(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if path in {"/", "/index.html"}:
            self._serve_static("index.html", head_only=True)
            return
        if path in {"/tasks", "/tasks/"}:
            self._serve_static("queue.html", head_only=True)
            return
        if path in {"/settings", "/settings/"}:
            self._serve_static("settings.html", head_only=True)
            return
        if path in {"/logs", "/logs/"}:
            self._serve_static("logs.html", head_only=True)
            return
        if path.startswith("/static/"):
            self._serve_static(path.removeprefix("/static/"), head_only=True)
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        try:
            if path in {"/", "/index.html"}:
                self._serve_static("index.html")
                return
            if path in {"/tasks", "/tasks/"}:
                self._serve_static("queue.html")
                return
            if path in {"/settings", "/settings/"}:
                self._serve_static("settings.html")
                return
            if path in {"/logs", "/logs/"}:
                self._serve_static("logs.html")
                return
            if path.startswith("/static/"):
                self._serve_static(path.removeprefix("/static/"))
                return
            if path == "/api/version":
                self._json(version_info())
                return
            if path == "/api/health":
                version = version_info()
                stats = self.service.stats()
                self._json({
                    # False when a background loop has stopped beating; the
                    # keepalive supervisor restarts the process on it.
                    "ok": bool(stats.get("healthy", True)),
                    "service": "sologsb-monitor",
                    "version": version["version"],
                    "gitCommit": version["gitCommit"],
                    "time": utc_now(),
                    "uptimeSeconds": round(time.time() - self.server.started_at, 1),  # type: ignore[attr-defined]
                    "stats": stats,
                })
                return
            if path == "/api/stream":
                self._serve_stream()
                return
            if path == "/api/snapshot":
                self._json(self.service.snapshot())
                return
            if path == "/api/submissions":
                force = str((query.get("refresh") or ["0"])[0]).lower() in {"1", "true", "yes"}
                self._json(self.service.submissions(force=force))
                return
            if path == "/api/folders":
                payload = self.service.folders.list()
                settings = self.service.settings.get() if self.service.settings else {}
                self._json({
                    "items": payload.get("items") or [],
                    "error": str(payload.get("error") or ""),
                    "fetchedAt": payload.get("fetchedAt") or "",
                    "selectedId": str(settings.get("defaultFolderId") or ""),
                    "selectedPath": str(settings.get("defaultFolderPath") or ""),
                })
                return
            if path == "/api/tasks":
                snapshot = self.service.snapshot()
                self._json({"tasks": snapshot.get("tasks") or [], "summary": snapshot.get("summary") or {}})
                return
            if path.startswith("/api/tasks/"):
                self._task_routes(path.removeprefix("/api/tasks/"), query)
                return
            if path == "/api/queue":
                self._json(self.service.queue.snapshot())
                return
            if path.startswith("/api/queue/") and path.endswith("/prompt"):
                item_id = urllib.parse.unquote(path[len("/api/queue/"):-len("/prompt")])
                self._json({"itemId": item_id, "prompt": self.service.queue.item_prompt(item_id)})
                return
            if path == "/api/settings":
                settings = self.service.settings.get() if self.service.settings else {}
                self._json({
                    "settings": _public(settings),
                    "platform": {
                        "managerBaseUrl": str((self.service.config.get("platform") or {}).get("managerBaseUrl") or ""),
                        "username": str((self.service.config.get("platform") or {}).get("username") or "admin"),
                        "passwordKeychainService": str(
                            (self.service.config.get("platform") or {}).get("passwordKeychainService") or "solo-manager-password"
                        ),
                    },
                    "connection": self.service.platform.connection_status(),
                })
                return
            if path == "/api/logs":
                try:
                    after_seq = int((query.get("afterSeq") or ["0"])[0])
                except ValueError:
                    after_seq = 0
                try:
                    limit = int((query.get("limit") or ["200"])[0])
                except ValueError:
                    limit = 200
                limit = max(1, min(limit, MAX_LOG_LIMIT))
                level = str((query.get("level") or [""])[0])
                entries = self.service.log.after(after_seq, limit=limit)
                if level:
                    entries = [item for item in entries if str(item.get("level") or "") == level]
                self._json({
                    "entries": entries,
                    "lastSeq": self.service.log.last_seq(),
                    "recent": self.service.log.recent(limit=limit, level=level),
                })
                return
            if path == "/api/platform/projects":
                task_type = str((query.get("taskType") or ["0-1代码生成"])[0])
                force = str((query.get("refresh") or ["0"])[0]).lower() in {"1", "true", "yes"}
                self._json(self.service.platform_candidates(task_type=task_type, force=force))
                return
            if path == "/api/config":
                config = {key: value for key, value in self.service.config.items() if not key.startswith("_")}
                self._json(config)
                return
            self.send_error(HTTPStatus.NOT_FOUND)
        except MonitorError as exc:
            self._json({"error": str(exc)}, 400)
        except Exception as exc:  # pragma: no cover - defensive
            self._json({"error": str(exc)}, 500)

    def _task_routes(self, remainder: str, query: dict[str, list[str]]) -> None:
        parts = [urllib.parse.unquote(part) for part in remainder.split("/") if part]
        if not parts:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        task_id = parts[0]
        if len(parts) == 1:
            task = self.service.task_detail(task_id)
            if task is None:
                self._json({"error": "任务不存在"}, 404)
                return
            self._json(task)
            return
        kind = parts[1]
        if kind == "log":
            side = str((query.get("side") or ["A"])[0])
            try:
                lines = int((query.get("lines") or ["200"])[0])
            except ValueError:
                lines = 200
            self._json(self.service.task_log(task_id, side, lines))
            return
        if kind == "events":
            side = str((query.get("side") or ["A"])[0])
            try:
                limit = int((query.get("limit") or ["200"])[0])
            except ValueError:
                limit = 200
            self._json(self.service.task_events(task_id, side, limit))
            return
        if kind == "history":
            side = str((query.get("side") or ["A"])[0])
            try:
                limit = int((query.get("limit") or ["400"])[0])
            except ValueError:
                limit = 400
            self._json(self.service.task_history(task_id, side, event_limit=limit))
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if path not in {"/api/action", "/api/automation", "/api/settings", "/api/settings/manager"}:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if not self._remote_actions_allowed():
            self._json({"ok": False, "error": "远程客户端不允许执行操作"}, 403)
            return
        try:
            payload = self._read_json()
        except (ValueError, MonitorError) as exc:
            self._json({"ok": False, "error": str(exc)}, 400)
            return
        try:
            if path == "/api/automation":
                result = self.service.automation_action(str(payload.get("action") or ""), payload)
                self._json({"ok": True, "automation": result})
                return
            if path == "/api/action":
                action = str(payload.get("action") or "")
                if action == "dismiss":
                    task = self.service.dismiss_task(str(payload.get("taskId") or ""))
                    self._json({"ok": True, "task": task})
                    return
                if action == "restore":
                    self.service.restore_task(str(payload.get("taskId") or ""))
                    self._json({"ok": True})
                    return
                job = self.service.start_action(
                    str(payload.get("taskId") or ""),
                    str(payload.get("side") or ""),
                    str(payload.get("mode") or "resume"),
                )
                self._json({"ok": True, "job": job})
                return
            if path == "/api/settings":
                patch = payload.get("settings") if isinstance(payload.get("settings"), dict) else payload
                self._json({"ok": True, "settings": self.service.update_settings(patch)})
                return
            if path == "/api/settings/manager":
                self._json(self.service.save_manager_credentials(
                    base_url=str(payload.get("managerBaseUrl") or ""),
                    username=str(payload.get("username") or ""),
                    password=str(payload.get("password") or ""),
                ))
                return
        except MonitorError as exc:
            self._json({"ok": False, "error": str(exc)}, 400)
        except Exception as exc:  # pragma: no cover - defensive
            self._json({"ok": False, "error": str(exc)}, 500)


def _public(settings: dict[str, Any]) -> dict[str, Any]:
    from api.service import _public_settings

    return _public_settings(settings)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="sologsb-0917 A/B 任务调度监控台")
    parser.add_argument("--root", action="append", type=Path, help="扫描根目录，可重复")
    parser.add_argument("--host", help="监听地址，默认读取 config.json")
    parser.add_argument("--port", type=int, help="监听端口，默认读取 config.json")
    parser.add_argument("--skill-script", type=Path, help="sologsb.py 路径")
    parser.add_argument("--no-submissions", action="store_true", help="禁用 SOLO2 提交信息")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = load_config(roots=[str(path) for path in args.root] if args.root else None)
    if args.host:
        config.setdefault("server", {})["host"] = args.host
    if args.port:
        config.setdefault("server", {})["port"] = int(args.port)
    if args.skill_script:
        config["skillScript"] = str(args.skill_script.expanduser().resolve())
    if args.no_submissions:
        config.setdefault("solo2", {})["enabled"] = False
    if not CONFIG_PATH.exists():
        save_config(config)

    instance_lock = MonitorInstanceLock(Path(__file__).resolve().parent / ".state" / "monitor.lock")
    try:
        instance_lock.acquire()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    service = SchedulerService(config)
    service.settings = SettingsStore()
    host = str((config.get("server") or {}).get("host") or "127.0.0.1")
    port = int((config.get("server") or {}).get("port") or 8790)
    try:
        server = MonitorHTTPServer((host, port), Handler, service)
    except OSError as exc:
        instance_lock.release()
        print(f"无法监听 {host}:{port}: {exc}", file=sys.stderr)
        return 2

    def _graceful_exit(_signum, _frame):
        # SIGTERM from the keepalive supervisor (or launchd) takes the same
        # path as Ctrl-C, so the service logs its stop and releases the lock.
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _graceful_exit)
    service.start()
    print("sologsb-monitor 已启动", flush=True)
    print(f"本机访问: http://127.0.0.1:{port}")
    if host == "0.0.0.0":
        ip = lan_ip()
        if ip:
            print(f"局域网访问: http://{ip}:{port}")
        print("提示：默认远程客户端只能查看，不能执行操作。")
    print(f"生效目录: {', '.join(service.queue.active_roots()) or '(未设置)'}")
    if service.queue.active_roots() != [str(item) for item in config.get("roots") or []]:
        print(f"已配置目录: {', '.join(str(item) for item in config.get('roots') or [])}")
    print(f"技能 CLI: {config.get('skillScript')}")
    print(f"调度模式: {(config.get('automation') or {}).get('scheduleMode')}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")
    finally:
        service.stop()
        server.server_close()
        instance_lock.release()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
