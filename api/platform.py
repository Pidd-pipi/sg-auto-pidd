"""Solo Manager adapter: authentication, project candidates, quota ledger.

The quota ledger is the part that used to be missing.  Previously the monitor
only snapshotted ``quotaBefore`` at enqueue time; the actual deduction happens
inside the ChatGPT desktop session via ``platform_bridge`` and the monitor never
saw it, so a failed task could not give the attempt back.  Here the monitor owns
the whole lifecycle: pre-deduct at claim, settle on success, refund on failure.

The release endpoint is ``POST /api/v1/tasks/{taskId}/cancel`` — verified live:
it returns HTTP 200 and restores ``projectUsageCount``.  If a deployment does
not expose it the ledger degrades to local-only bookkeeping and says so.
"""
from __future__ import annotations

import copy
import json
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Iterable

from .common import (
    DEFAULT_MANAGER_TIMEOUT_SECONDS,
    DEFAULT_MANAGER_USERNAME,
    DEFAULT_PAGE_SIZE,
    MANAGER_AUTH_STATE_PATH,
    MANAGER_PASSWORD_SERVICE,
    MANAGER_TOKEN_SCOPES,
    MANAGER_TOKEN_SERVICE,
    PLATFORM_SCRIPTS,
    SUBS_TTL,
    DEFAULT_SKILL_SCRIPT,
    ManagerApiError,
    MonitorError,
    discover_manager_base_urls,
    keychain_read,
    keychain_write,
    manager_request_json,
    read_json,
    redact_text,
    secret_fingerprint,
    utc_now,
)

QUOTA_PENDING = "pending"
QUOTA_CLAIMED = "claimed"
QUOTA_SETTLED = "settled"
QUOTA_REFUNDED = "refunded"
QUOTA_SUPERSEDED = "superseded"

RELEASE_PATH = "/tasks/{task_id}/cancel"


def _keychain(service: str) -> str:
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", service, "-w"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except (OSError, subprocess.TimeoutExpired):
        return ""


class PlatformProvider:
    """Solo Manager access: auth chain, candidate listing, quota movements."""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self._lock = threading.RLock()
        self._auth_lock = threading.RLock()
        self._cache: dict[tuple[str, bool], dict[str, Any]] = {}
        self._quota_lock = threading.RLock()
        # Operator blocklist, kept in sync from the service via set_blocklist.
        self._blocked: set[str] = set()

    def set_blocklist(self, codes: Iterable[str]) -> None:
        with self._lock:
            self._blocked = {str(code).strip().casefold() for code in codes if str(code).strip()}

    def blocked_codes(self) -> set[str]:
        with self._lock:
            return set(self._blocked)

    def is_blocked(self, project_code: str) -> bool:
        return str(project_code or "").strip().casefold() in self.blocked_codes()

    # -- module loading --------------------------------------------------- #
    def _modules(self):
        for scripts in (PLATFORM_SCRIPTS, Path(DEFAULT_SKILL_SCRIPT).parent):
            if str(scripts) not in sys.path:
                sys.path.insert(0, str(scripts))
        try:
            import platform_bridge  # type: ignore
            import project_claims  # type: ignore
        except Exception as exc:
            raise MonitorError(f"无法加载 Solo Manager 适配器: {exc}") from exc
        return platform_bridge, project_claims

    def occupied_project_codes(self) -> set[str]:
        """Codes the skill would refuse at ``init``: live claims or running tasks.

        Offline check (no Manager login) so manual enqueue can use it cheaply.
        """
        _pb, claims = self._modules()
        codes: set[str] = set()
        sweep = getattr(claims, "sweep_finished_claims", None)
        for base_url in discover_manager_base_urls(self.config):
            if sweep is not None:
                try:
                    sweep(base_url)
                except Exception:
                    pass
            codes |= set(claims.claimed_project_codes(base_url))
        roots = self.config.get("roots") or []
        workdir = Path(str(roots[0])) if roots else None
        running_codes, _source = claims.running_container_project_codes(workdir)
        return codes | set(running_codes)

    # -- authentication --------------------------------------------------- #
    def _auth_cfg(self) -> dict[str, Any]:
        return self.config.get("platform") or {}

    def _manager_username(self) -> str:
        import os

        cfg = self._auth_cfg()
        return str(
            os.environ.get("SOLO_MANAGER_USERNAME")
            or cfg.get("username")
            or DEFAULT_MANAGER_USERNAME
        ).strip()

    def _manager_password(self) -> str:
        import os

        cfg = self._auth_cfg()
        service = str(cfg.get("passwordKeychainService") or MANAGER_PASSWORD_SERVICE)
        return str(os.environ.get("SOLO_MANAGER_PASSWORD") or keychain_read(service)).strip()

    def _manager_timeout(self) -> float:
        cfg = self._auth_cfg()
        try:
            value = float(cfg.get("managerRequestTimeoutSeconds") or DEFAULT_MANAGER_TIMEOUT_SECONDS)
        except (TypeError, ValueError):
            value = DEFAULT_MANAGER_TIMEOUT_SECONDS
        return max(0.5, value)

    def _auth_state(self) -> dict[str, Any]:
        state = read_json(MANAGER_AUTH_STATE_PATH, {})
        return state if isinstance(state, dict) else {}

    def _remember_issued_token(self, base_url: str, issued: dict[str, Any]) -> str:
        token = str((issued or {}).get("token") or "").strip()
        if not token:
            raise MonitorError("Solo Manager 换发令牌成功，但响应里没有 token")
        keychain_write(MANAGER_TOKEN_SERVICE, token)
        from .common import atomic_write_json

        atomic_write_json(
            MANAGER_AUTH_STATE_PATH,
            {
                "service": MANAGER_TOKEN_SERVICE,
                "baseUrl": str(base_url or "").strip().rstrip("/"),
                "tokenSha256": secret_fingerprint(token),
                "tokenId": str(issued.get("id") or ""),
                "name": str(issued.get("name") or ""),
                "scopes": issued.get("scopes") or MANAGER_TOKEN_SCOPES,
                "expiresAt": issued.get("expiresAt"),
                "updatedAt": utc_now(),
            },
        )
        return token

    def _issue_durable_token(self, base_url: str, current_token: str) -> str:
        cfg = self._auth_cfg()
        name = str(cfg.get("tokenName") or "sologsb-monitor-auto").strip()
        issued = manager_request_json(
            base_url,
            "/tokens",
            token=current_token,
            method="POST",
            payload={"name": name, "scopes": MANAGER_TOKEN_SCOPES},
            timeout=self._manager_timeout(),
        )
        if not isinstance(issued, dict):
            raise MonitorError("Solo Manager 换发令牌返回格式异常")
        return self._remember_issued_token(base_url, issued)

    def _login_and_issue_token(self, base_url: str) -> str:
        password = self._manager_password()
        if not password:
            raise MonitorError(
                "Solo Manager 令牌已失效，且没有自动续期密码。请先执行 "
                'security add-generic-password -a "$USER" -s solo-manager-password -U -w'
            )
        login = manager_request_json(
            base_url,
            "/auth/login",
            method="POST",
            payload={"username": self._manager_username(), "password": password},
            timeout=self._manager_timeout(),
        )
        access_token = str((login or {}).get("accessToken") or (login or {}).get("token") or "").strip()
        if not access_token:
            raise MonitorError("Solo Manager 自动登录成功，但响应里没有 accessToken")
        return self._issue_durable_token(base_url, access_token)

    def _recover_authentication(self, base_url: str, *, current_token: str = "") -> str:
        import os

        with self._auth_lock:
            token = str(current_token or "").strip()
            if not token:
                token = str(os.environ.get("SOLO_MANAGER_TOKEN") or keychain_read(MANAGER_TOKEN_SERVICE)).strip()
            if token:
                try:
                    manager_request_json(base_url, "/auth/me", token=token, timeout=self._manager_timeout())
                    return self._issue_durable_token(base_url, token)
                except ManagerApiError as exc:
                    if exc.status not in {401, 403}:
                        raise
            return self._login_and_issue_token(base_url)

    def _resolve_manager_connection(self, pb: Any) -> tuple[str, str, str]:
        candidates = discover_manager_base_urls(self.config)
        if not candidates:
            raise MonitorError("未配置 Solo Manager 地址，请设置 SOLO_MANAGER_BASE_URL 或 config.json 的 platform.managerBaseUrl")
        try:
            token = pb.load_manager_token()
        except Exception:
            import os

            token = str(os.environ.get("SOLO_MANAGER_TOKEN") or keychain_read(MANAGER_TOKEN_SERVICE)).strip()
        errors: list[str] = []
        for base_url in candidates:
            try:
                if not token:
                    token = self._login_and_issue_token(base_url)
                else:
                    try:
                        manager_request_json(base_url, "/auth/me", token=token, timeout=self._manager_timeout())
                    except ManagerApiError as exc:
                        if exc.status in {401, 403}:
                            token = self._recover_authentication(base_url, current_token=token)
                            manager_request_json(base_url, "/auth/me", token=token, timeout=self._manager_timeout())
                        else:
                            raise
                warning = ""
                state = self._auth_state()
                state_matches = (
                    str(state.get("tokenSha256") or "") == secret_fingerprint(token)
                    and str(state.get("baseUrl") or "").rstrip("/") == base_url
                )
                if not state_matches:
                    try:
                        token = self._issue_durable_token(base_url, token)
                    except ManagerApiError as exc:
                        warning = f"未换发不过期令牌: {exc}"
                return base_url, token, warning
            except Exception as exc:
                errors.append(f"{base_url}: {exc}")
        detail = " | ".join(errors[-5:])
        raise MonitorError(f"无法连接或续期 Solo Manager 登录：{detail or '没有可用地址'}")

    # -- project candidates ----------------------------------------------- #
    @staticmethod
    def _project_code(project: dict[str, Any]) -> str:
        return str(project.get("code") or "").strip()

    @staticmethod
    def _usable_variant(project: dict[str, Any]) -> dict[str, Any] | None:
        readiness = str(project.get("readinessStatus") or "").strip().upper()
        if project.get("disabled") or (readiness and readiness != "RUNNABLE"):
            return None
        variants = [
            item for item in (project.get("variants") or [])
            if isinstance(item, dict) and item.get("sourceAvailable") and item.get("sourceAsset")
        ]
        variants.sort(key=lambda item: str(item.get("directoryName") or item.get("id") or ""))
        return variants[0] if variants else None

    @staticmethod
    def _quota(project: dict[str, Any], task_type: str) -> dict[str, Any] | None:
        for quota in project.get("quotas") or []:
            if isinstance(quota, dict) and str(quota.get("taskType") or "") == task_type:
                return quota
        return None

    def _list_projects(self, pb: Any, base_url: str, token: str, path: str) -> list[dict[str, Any]]:
        """Paginate through a project listing.

        ``platform_bridge.list_projects`` already walks ``page`` up to
        ``totalPages``; the old monitor hard-coded ``size=200`` and silently
        dropped everything past the first page.
        """
        meta = {"baseUrl": base_url, "apiBaseUrl": base_url if base_url.endswith("/api/v1") else base_url + "/api/v1"}
        try:
            return pb.list_projects(meta, token, path)
        except Exception:
            # Fall back to a single-page request if the adapter is unavailable.
            payload = manager_request_json(base_url, f"{path}?page=1&size=100", token=token, timeout=self._manager_timeout())
            items = payload.get("items") if isinstance(payload, dict) else []
            return [item for item in (items or []) if isinstance(item, dict)]

    def candidates(
        self,
        task_type: str = "0-1代码生成",
        force: bool = False,
        include_pool: bool | None = None,
    ) -> dict[str, Any]:
        if include_pool is None:
            include_pool = bool((self.config.get("platform") or {}).get("mergeProjectPool", False))
        include_pool = bool(include_pool)
        cache_key = (task_type, include_pool)
        now = time.time()
        with self._lock:
            cached = self._cache.get(cache_key)
            ttl = float((self.config.get("platform") or {}).get("candidateTtlSeconds") or 30)
            if cached and not force and now - float(cached.get("_at") or 0) < ttl:
                return copy.deepcopy(cached)
        pb, claims = self._modules()
        base_url, token, auth_warning = self._resolve_manager_connection(pb)
        workdir = None
        roots = self.config.get("roots") or []
        if roots:
            workdir = Path(str(roots[0]))
        sweep = getattr(claims, "sweep_finished_claims", None)
        if sweep is not None:
            try:
                sweep(base_url)
            except Exception:
                pass
        try:
            running_codes, running_source = claims.running_container_project_codes(workdir)
            claim_codes = claims.claimed_project_codes(base_url)
        except Exception as exc:
            raise MonitorError(f"读取项目占用状态失败: {exc}") from exc
        active_codes = set(running_codes) | set(claim_codes)
        items: list[dict[str, Any]] = []
        seen: set[str] = set()
        excluded: list[dict[str, str]] = []
        errors: list[str] = []
        stages = [("我的项目", "/projects/mine")]
        if include_pool:
            stages.append(("项目池", "/projects"))
        for stage, path in stages:
            try:
                try:
                    projects = self._list_projects(pb, base_url, token, path)
                except ManagerApiError as exc:
                    if exc.status not in {401, 403}:
                        raise
                    token = self._recover_authentication(base_url, current_token=token)
                    projects = self._list_projects(pb, base_url, token, path)
            except Exception as exc:
                errors.append(f"{stage}: {exc}")
                continue
            for project in projects:
                if not isinstance(project, dict):
                    continue
                code = self._project_code(project)
                identity = code.casefold() or str(project.get("id") or "")
                if not identity or identity in seen:
                    continue
                seen.add(identity)
                if code and code.casefold() in {str(value).casefold() for value in active_codes}:
                    excluded.append({"code": code, "reason": "已占用或运行中"})
                    continue
                variant = self._usable_variant(project)
                if variant is None:
                    excluded.append({"code": code or identity, "reason": "缺少可用源码快照"})
                    continue
                quota = self._quota(project, task_type)
                if quota is None:
                    excluded.append({"code": code or identity, "reason": f"没有 {task_type} 配额"})
                    continue
                if int(quota.get("remaining") or 0) <= 0:
                    excluded.append({"code": code or identity, "reason": "配额已耗尽"})
                    continue
                items.append({
                    "id": str(project.get("id") or ""),
                    "code": code,
                    "name": str(project.get("name") or ""),
                    "businessDomain": str(project.get("businessDomain") or ""),
                    "category": str(project.get("category") or ""),
                    "readinessStatus": str(project.get("readinessStatus") or ""),
                    "variantId": str(variant.get("id") or ""),
                    "variantName": str(variant.get("directoryName") or ""),
                    "languages": str(variant.get("languages") or ""),
                    "summary": str(variant.get("summary") or ""),
                    "sourceAssetId": str((variant.get("sourceAsset") or {}).get("id") or ""),
                    "sourceAssetSha256": str((variant.get("sourceAsset") or {}).get("sha256") or ""),
                    "quotaBefore": quota,
                    "stage": stage,
                })
        blocked = self.blocked_codes()
        if blocked:
            for item in items:
                if str(item.get("code") or "").casefold() in blocked:
                    excluded.append({"code": str(item.get("code") or ""), "reason": "已被手动禁用"})
            items = [item for item in items if str(item.get("code") or "").casefold() not in blocked]
        items.sort(key=lambda item: (str(item.get("code") or ""), str(item.get("name") or "")))
        result = {
            "items": items,
            "total": len(items),
            "excluded": excluded[:200],
            "errors": errors,
            "baseUrl": base_url,
            "authWarning": auth_warning,
            "taskType": task_type,
            "runningContainerSource": running_source,
            "fetchedAt": utc_now(),
            "_at": now,
        }
        with self._lock:
            self._cache[cache_key] = result
        return copy.deepcopy(result)

    # -- quota movements -------------------------------------------------- #
    def pre_deduct(self, variant_id: str, root_task_type: str, *, base_url: str = "", token: str = "") -> dict[str, Any]:
        """Create the platform task up front so the attempt is reserved.

        ``platform_bridge.bootstrap`` reuses ``meta["taskId"]`` when it is
        already set, so a task created here is the one the executor will run.
        """
        variant = str(variant_id or "").strip()
        if not variant:
            raise MonitorError("预扣除配额失败：项目缺少 variantId")
        if not token:
            pb, _claims = self._modules()
            base_url, token, _warning = self._resolve_manager_connection(pb)
        created = manager_request_json(
            base_url,
            "/tasks",
            token=token,
            method="POST",
            payload={"variantId": variant, "rootTaskType": str(root_task_type or "0-1代码生成")},
            timeout=self._manager_timeout(),
        )
        if not isinstance(created, dict) or not created.get("id"):
            raise MonitorError("预扣除配额失败：平台未返回 taskId")
        rounds = created.get("rounds") if isinstance(created.get("rounds"), list) else []
        return {
            "platformTaskId": str(created.get("id") or ""),
            "platformTaskNo": str(created.get("taskNo") or ""),
            "platformRoundId": str((rounds[0] or {}).get("id") or "") if rounds and isinstance(rounds[0], dict) else "",
            "projectUsageCount": created.get("projectUsageCount"),
            "createdAt": utc_now(),
        }

    def release_task(self, task_id: str, *, base_url: str = "", token: str = "") -> dict[str, Any]:
        """Give an unused task back to the platform.

        Returns ``{"ok": bool, "mode": "platform"|"local", ...}``.  ``mode`` is
        ``local`` when the endpoint is missing or rejects the call — the ledger
        still records the refund intent so the UI can show it.
        """
        marker = str(task_id or "").strip()
        if not marker:
            return {"ok": False, "mode": "local", "error": "缺少 platformTaskId"}
        if not token:
            try:
                pb, _claims = self._modules()
                base_url, token, _warning = self._resolve_manager_connection(pb)
            except Exception as exc:
                return {"ok": False, "mode": "local", "error": str(exc)}
        path = RELEASE_PATH.format(task_id=marker)
        try:
            manager_request_json(base_url, path, token=token, method="POST", payload={}, timeout=self._manager_timeout())
        except ManagerApiError as exc:
            return {"ok": False, "mode": "local", "error": str(exc), "status": exc.status}
        except MonitorError as exc:
            return {"ok": False, "mode": "local", "error": str(exc)}
        return {"ok": True, "mode": "platform", "releasedAt": utc_now(), "platformTaskId": marker}

    def project_quota(self, project_code: str, task_type: str = "0-1代码生成") -> dict[str, Any] | None:
        """Fresh quota for one project, used to show the effect of a refund."""
        code = str(project_code or "").strip()
        if not code:
            return None
        try:
            pb, _claims = self._modules()
            base_url, token, _warning = self._resolve_manager_connection(pb)
            projects = self._list_projects(pb, base_url, token, "/projects/mine")
        except Exception:
            return None
        for project in projects:
            if not isinstance(project, dict) or self._project_code(project) != code:
                continue
            return self._quota(project, task_type)
        return None

    def connection_status(self) -> dict[str, Any]:
        """Cheap health probe for the settings page."""
        try:
            pb, _claims = self._modules()
            base_url, token, warning = self._resolve_manager_connection(pb)
            return {"ok": True, "baseUrl": base_url, "warning": warning, "checkedAt": utc_now()}
        except Exception as exc:
            return {"ok": False, "error": redact_text(str(exc), 400), "checkedAt": utc_now()}


class SubmissionProvider:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self._lock = threading.Lock()
        self._at = 0.0
        self._data: dict[str, Any] = {"items": [], "error": "", "source": ""}

    def get(self, force: bool = False) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            if not force and self._at and now - self._at < SUBS_TTL:
                return copy.deepcopy(self._data)
        data = self._fetch()
        with self._lock:
            self._at = time.time()
            self._data = data
            return copy.deepcopy(data)

    def _fetch(self) -> dict[str, Any]:
        cfg = self.config.get("solo2") or {}
        if not cfg.get("enabled", True):
            return {"items": [], "error": "", "source": "disabled", "fetchedAt": utc_now()}
        if cfg.get("preferMonitor", True):
            from_monitor = self._fetch_monitor(cfg)
            if not from_monitor.get("error"):
                return from_monitor
            fallback = self._fetch_direct(cfg)
            if not fallback.get("error"):
                fallback["fallbackReason"] = from_monitor.get("error", "")
                return fallback
            return {
                "items": [],
                "error": fallback.get("error") or from_monitor.get("error"),
                "source": "none",
                "fetchedAt": utc_now(),
            }
        return self._fetch_direct(cfg)

    def _fetch_monitor(self, cfg: dict[str, Any]) -> dict[str, Any]:
        base = str(cfg.get("monitorUrl") or "").rstrip("/")
        if not base:
            return {"items": [], "error": "未配置 solo2-monitor 地址", "source": "monitor"}
        size = int(cfg.get("pageSize") or DEFAULT_PAGE_SIZE)
        url = f"{base}/api/submissions?page_size={size}"
        try:
            with urllib.request.urlopen(url, timeout=8) as response:
                data = json.loads(response.read().decode("utf-8"))
        except (OSError, ValueError, urllib.error.URLError) as exc:
            return {"items": [], "error": f"solo2-monitor 未响应：{exc}", "source": "monitor"}
        items = data.get("items") if isinstance(data, dict) else []
        if not isinstance(items, list):
            items = []
        clean = dict(data) if isinstance(data, dict) else {}
        clean["items"] = [self._public_submission(item) for item in items if isinstance(item, dict)]
        clean["source"] = "solo2-monitor"
        clean["fetchedAt"] = clean.get("fetchedAt") or utc_now()
        clean.setdefault("error", "")
        return clean

    @staticmethod
    def _public_submission(item: dict[str, Any]) -> dict[str, Any]:
        allowed = (
            "id", "at", "submittedAt", "who", "type", "round", "status", "statusLabel",
            "stage", "stageLabel", "repo", "sessionId", "avg", "prompt", "qc",
            "qcConclusion", "qcRunning", "qcFinishedAt", "lastTimelineAction",
            "lastTimelineAt", "currentVersion", "lark",
        )
        result = {key: item.get(key) for key in allowed if item.get(key) is not None}
        if result.get("prompt"):
            result["prompt"] = redact_text(result["prompt"], 180)
        if result.get("qc"):
            result["qc"] = redact_text(result["qc"], 240)
        return result

    def _fetch_direct(self, cfg: dict[str, Any]) -> dict[str, Any]:
        cookie = _keychain("solo2-jzxhnh-cookie")
        csrf = _keychain("solo2-jzxhnh-csrf")
        if not cookie:
            return {
                "items": [],
                "error": "读不到 SOLO2 凭据（Keychain service: solo2-jzxhnh-cookie）",
                "source": "keychain",
            }
        base = str(cfg.get("apiBaseUrl") or "").rstrip("/")
        size = int(cfg.get("pageSize") or DEFAULT_PAGE_SIZE)
        headers = {"Cookie": cookie, "x-csrf-token": csrf, "Accept": "application/json"}

        def get(path: str) -> Any:
            request = urllib.request.Request(base + path, headers=headers)
            with urllib.request.urlopen(request, timeout=15) as response:
                return json.loads(response.read().decode("utf-8"))

        try:
            raw = get(f"/submissions?page=1&page_size={size}")
        except (OSError, ValueError, urllib.error.URLError) as exc:
            return {"items": [], "error": f"拉取提交列表失败：{exc}", "source": "keychain"}
        try:
            stats = get("/submissions/stats")
        except Exception:
            stats = None
        items: list[dict[str, Any]] = []
        for item in raw.get("items") or []:
            if not isinstance(item, dict):
                continue
            scores = item.get("scores") or {}
            values = [value for value in scores.values() if isinstance(value, (int, float))]
            normalized = {
                "id": item.get("id"),
                "at": item.get("submitted_at"),
                "submittedAt": item.get("submitted_at"),
                "who": item.get("submitter_name"),
                "type": item.get("question_type"),
                "round": item.get("round_no"),
                "status": item.get("status"),
                "statusLabel": item.get("status_label") or item.get("stage_label"),
                "stage": item.get("stage"),
                "stageLabel": item.get("stage_label"),
                "repo": item.get("repo_id"),
                "sessionId": str(item.get("session_id") or "")[:8],
                "avg": round(sum(values) / len(values), 2) if values else None,
                "prompt": item.get("prompt_excerpt") or "",
                "qc": item.get("qc_summary"),
                "qcConclusion": item.get("qc_conclusion"),
                "qcRunning": bool(item.get("qc_running")),
                "qcFinishedAt": item.get("qc_finished_at"),
                "currentVersion": item.get("current_version"),
                "lark": item.get("lark_sync_status"),
            }
            items.append(self._public_submission(normalized))
        return {
            "items": items,
            "total": (raw.get("meta") or {}).get("total"),
            "stats": stats,
            "source": "keychain",
            "fetchedAt": utc_now(),
            "error": "",
        }
