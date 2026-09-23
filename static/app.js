/* Shared client helpers: fetch with timeout, SSE subscription, formatting, DOM.
   Every page is a plain module that calls into this file. */
(function () {
  "use strict";

  const assetVersion = (() => {
    try {
      const source = document.currentScript && document.currentScript.src;
      return new URL(source || "", document.baseURI).searchParams.get("v") || "";
    } catch (error) {
      return "";
    }
  })();
  const $ = (selector, root) => (root || document).querySelector(selector);
  const $$ = (selector, root) => Array.from((root || document).querySelectorAll(selector));

  function esc(value) {
    return String(value ?? "").replace(/[&<>"']/g, (char) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    }[char]));
  }

  function debounce(fn, ms) {
    let timer = null;
    return function wrapped(...args) {
      clearTimeout(timer);
      timer = setTimeout(() => fn.apply(this, args), ms);
    };
  }

  async function fetchJson(url, options = {}, timeoutMs = 15000) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(url, { ...options, signal: controller.signal, cache: "no-store" });
      const text = await response.text();
      let data = {};
      try {
        data = text ? JSON.parse(text) : {};
      } catch (error) {
        throw new Error(`响应不是合法 JSON（HTTP ${response.status}）`);
      }
      if (!response.ok || data.error) {
        throw new Error(data.error || `HTTP ${response.status}`);
      }
      return data;
    } catch (error) {
      if (error.name === "AbortError") throw new Error("请求超时");
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  async function postJson(url, body, timeoutMs = 20000) {
    return fetchJson(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }, timeoutMs);
  }

  /* ------------------------------------------------------------- formatting */
  function fmtDuration(seconds) {
    if (seconds === null || seconds === undefined || Number.isNaN(Number(seconds))) return "—";
    let value = Math.max(0, Math.floor(Number(seconds)));
    const days = Math.floor(value / 86400);
    value %= 86400;
    const hours = Math.floor(value / 3600);
    value %= 3600;
    const minutes = Math.floor(value / 60);
    const secs = value % 60;
    if (days) return `${days}d ${hours}h`;
    if (hours) return `${hours}h ${String(minutes).padStart(2, "0")}m`;
    if (minutes) return `${minutes}m ${String(secs).padStart(2, "0")}s`;
    return `${secs}s`;
  }

  function fmtAge(seconds) {
    if (seconds === null || seconds === undefined) return "无活动";
    const value = Number(seconds);
    if (value < 8) return "刚刚";
    if (value < 60) return `${Math.floor(value)}s 前`;
    if (value < 3600) return `${Math.floor(value / 60)}m 前`;
    return `${Math.floor(value / 3600)}h 前`;
  }

  function fmtClock(value) {
    if (!value) return "—";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value).slice(0, 19).replace("T", " ");
    const pad = (n) => String(n).padStart(2, "0");
    return `${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
  }

  function fmtTime(value) {
    if (!value) return "—";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    const pad = (n) => String(n).padStart(2, "0");
    return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
  }

  /* ------------------------------------------------------------------ toast */
  let toastTimer = null;
  function toast(message, error = false) {
    let node = $("#toast");
    if (!node) {
      node = document.createElement("div");
      node.id = "toast";
      node.className = "toast";
      node.setAttribute("role", "status");
      node.setAttribute("aria-live", "polite");
      document.body.appendChild(node);
    }
    node.textContent = message;
    node.className = `toast show${error ? " error" : ""}`;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { node.className = "toast"; }, 3600);
  }

  /* ------------------------------------------------------------------ theme */
  function initTheme() {
    const saved = localStorage.getItem("sologsb-theme-v3");
    const system = window.matchMedia && window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
    document.documentElement.dataset.theme = saved || system;
    const button = $("#themeBtn");
    if (button) {
      button.addEventListener("click", () => {
        const root = document.documentElement;
        const next = root.dataset.theme === "dark" ? "light" : "dark";
        root.dataset.theme = next;
        localStorage.setItem("sologsb-theme-v3", next);
      });
    }
  }

  /* ------------------------------------------------------------------- rail */
  function renderRail(state) {
    const queue = state.queue || {};
    const mode = queue.scheduleMode || "containers";
    const badge = $("#modeBadge");
    if (badge) {
      badge.dataset.mode = mode;
      badge.innerHTML = `<span class="dot"></span>${mode === "containers" ? "容器优先" : "任务数量优先"}`;
      badge.title = mode === "containers"
        ? "保持运行中的候选容器数等于设定值；点击切换"
        : "保持并行任务数等于设定值；点击切换";
    }
    const folder = $("#folderSelect");
    if (folder) {
      const selected = state.selectedFolderId || "";
      if (folder.dataset.loaded !== "1") {
        folder.innerHTML = `<option value="">未选择</option>` + (state.folders || []).map((item) => (
          `<option value="${esc(item.id)}">${esc(item.name)}</option>`
        )).join("");
        folder.dataset.loaded = "1";
      }
      folder.value = selected;
    }
    const path = $("#folderPath");
    if (path) {
      const picked = (state.folders || []).find((item) => item.id === state.selectedFolderId);
      path.textContent = picked ? (picked.roots || []).join(" · ") || "无 root" : "";
    }
    const counts = queue.counts || {};
    setText($("#navTasks"), String((state.summary || {}).tasks || 0));
    setText($("#navQueue"), String(counts.pending || 0));
    const sync = $("#sync");
    const syncText = $("#syncText");
    if (sync) {
      sync.classList.toggle("ok", !state.error && Boolean(state.connected));
      sync.classList.toggle("error", Boolean(state.error));
      setText(syncText, state.error ? "连接断开" : (state.connected ? "实时推送" : "连接中"));
      sync.title = state.error || "";
    }
  }

  /* ------------------------------------------------------------ metric band */
  function renderMetrics(state) {
    const band = $("#band");
    if (!band) return;
    const summary = state.summary || {};
    const queue = state.queue || {};
    const counts = queue.counts || {};
    const containers = state.containers || {};
    // Cells 1–3 describe the scheduler; 4–5 describe the task tree.  Mixing the
    // two "done" numbers made the band read as contradictory, so they are
    // labelled for which world they come from.
    //
    // "运行中任务" is the task tree's count, not the queue's: a task started by
    // an earlier scheduler instance, or by the skill directly, is in no queue at
    // all and the queue number silently read zero while tasks were running.
    // The card shows real containers against the limit.  Candidates queued in
    // the skill's limiter are listed separately: adding them to the headline
    // read as "5 / 4" — over the limit — when the limit was exactly met.
    // Excluded (test) projects run outside the limit; only the counted ones go
    // against it, otherwise the card read "6 / 4" with the limit exactly met.
    const running = Number(containers.counted ?? containers.running ?? 0);
    const excluded = Number(containers.excluded ?? 0);
    const limit = Number(containers.hardLimit ?? 0);
    const queued = Number(containers.reserved ?? 0);
    const phantom = (containers.phantom || []).length;
    const cells = [
      {
        k: "运行中任务", v: summary.active || 0, tone: "run",
        s: `并发上限 ${queue.maxTasks || queue.capacity || 0} · 队列占用 ${counts.running || 0}`,
      },
      {
        k: "运行容器", v: running, unit: limit ? `/ ${limit}` : "", tone: "run",
        s: [
          queued ? `排队候选 ${queued}` : (limit && running >= limit ? "已满" : `空位 ${Math.max(0, limit - running)}`),
          excluded ? `免计 ${excluded}` : "",
          phantom ? `忽略失联 ${phantom}` : "",
        ].filter(Boolean).join(" · "),
        meter: limit ? Math.min(100, Math.round((running / limit) * 100)) : null,
      },
      {
        k: "待执行", v: counts.pending || 0, tone: "",
        s: `已预扣配额 ${counts.quotaClaimed || 0} · 已回补 ${counts.quotaRefunded || 0}`,
      },
      {
        k: "需处理", v: summary.attention || 0, tone: "warn",
        s: `任务失败 ${summary.failed || 0}`,
        alert: summary.attention ? "warn" : "",
      },
      {
        k: "已完成", v: summary.finished || 0, tone: "ok",
        s: `共 ${summary.tasks || 0} 个任务 · 队列完成 ${counts.done || 0}`,
      },
    ];
    setHtml(band, cells.map((cell) => `
      <div class="kpi" data-tone="${esc(cell.tone)}" data-alert="${esc(cell.alert || "")}">
        <div class="kpi-k">${esc(cell.k)}</div>
        <div class="kpi-v"><b>${esc(cell.v)}</b>${cell.unit ? `<span>${esc(cell.unit)}</span>` : ""}</div>
        ${cell.meter === null || cell.meter === undefined ? "" : `<div class="meter"><i style="width:${cell.meter}%"></i></div>`}
        <div class="kpi-s" title="${esc(cell.s)}">${esc(cell.s)}</div>
      </div>`).join(""));
  }

  /* -------------------------------------------------------------------- SSE */
  function connectStream({ onSnapshot, onTasks, onLog, onStatus }) {
    let controller = null;
    let retryTimer = null;
    let closed = false;
    let backoff = 1000;
    let lastSeq = 0;
    let dropped = false;

    async function open() {
      if (closed) return;
      if (controller) controller.abort();
      controller = new AbortController();
      const params = new URLSearchParams({ logs: "1", afterSeq: String(lastSeq) });
      if (onStatus) onStatus({ connected: false, error: "" });
      try {
        const response = await fetch(`/api/stream?${params}`, { signal: controller.signal, cache: "no-store" });
        if (!response.ok || !response.body) throw new Error(`HTTP ${response.status}`);
        backoff = 1000;
        if (onStatus) onStatus({ connected: true, error: "" });
        // Back after an outage — usually a restart, possibly onto new code.
        if (dropped) reloadIfServerChanged();
        dropped = false;
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (!closed) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const frames = buffer.split("\n\n");
          buffer = frames.pop() || "";
          for (const frame of frames) {
            const lines = frame.split("\n");
            let event = "message";
            const dataLines = [];
            for (const line of lines) {
              if (line.startsWith("event: ")) event = line.slice(7).trim();
              else if (line.startsWith("data: ")) dataLines.push(line.slice(6));
            }
            if (!dataLines.length) continue;
            let payload = null;
            try {
              payload = JSON.parse(dataLines.join("\n"));
            } catch (error) {
              continue;
            }
            if (event === "snapshot" && onSnapshot) onSnapshot(payload);
            else if (event === "tasks" && onTasks) onTasks(payload);
            else if (event === "log" && onLog) {
              const entries = payload.entries || [];
              if (entries.length) lastSeq = Math.max(lastSeq, Number(entries[entries.length - 1].seq) || lastSeq);
              onLog(payload);
            } else if (event === "heartbeat" && onTasks) onTasks(payload);
          }
        }
        throw new Error("连接关闭");
      } catch (error) {
        if (closed) return;
        dropped = true;
        if (onStatus) onStatus({ connected: false, error: String(error.message || error) });
        retryTimer = setTimeout(open, backoff);
        backoff = Math.min(backoff * 2, 15000);
      }
    }

    open();
    return {
      close() {
        closed = true;
        clearTimeout(retryTimer);
        if (controller) controller.abort();
      },
    };
  }

  /* ------------------------------------------------------- keyed list diff */
  /* Reuses existing DOM nodes by key and patches only what changed, instead of
     rebuilding the list with innerHTML on every tick. */
  function renderKeyed(container, items, renderItem, keyOf) {
    if (!container) return;
    const seen = new Set();
    const fragment = document.createDocumentFragment();
    for (const item of items) {
      const key = keyOf(item);
      seen.add(key);
      let node = container.querySelector(`[data-key="${CSS.escape(key)}"]`);
      if (node) {
        container.removeChild(node);
      } else {
        node = document.createElement("div");
        node.dataset.key = key;
      }
      const changed = renderItem(node, item);
      if (changed) node.dataset.revision = String(Number(node.dataset.revision || 0) + 1);
      fragment.appendChild(node);
    }
    for (const node of Array.from(container.children)) {
      if (!seen.has(node.dataset.key)) container.removeChild(node);
    }
    container.appendChild(fragment);
  }

  function setText(node, value) {
    if (!node) return false;
    const text = String(value ?? "");
    if (node.textContent !== text) {
      node.textContent = text;
      return true;
    }
    return false;
  }

  function setHtml(node, html) {
    if (!node) return false;
    if (node.innerHTML !== html) {
      node.innerHTML = html;
      return true;
    }
    return false;
  }

  function setAttr(node, name, value) {
    if (!node) return false;
    const next = String(value ?? "");
    if (node.getAttribute(name) !== next) {
      node.setAttribute(name, next);
      return true;
    }
    return false;
  }

  /* --------------------------------------------------------------- version */
  /* A dashboard left open overnight keeps the JS/CSS it loaded.  When the
     server comes back reporting a different version than this page's assets,
     reload once (guarded per version so a server still running the old code
     cannot cause a reload loop). */
  function reloadIfServerChanged() {
    if (!/^\d+\.\d+\.\d+/.test(assetVersion)) return;
    fetchJson("/api/version", {}, 5000).then((info) => {
      const version = String(info.version || "");
      if (!version || version === assetVersion) return;
      const key = "sologsb-reloaded-for";
      let last = "";
      try { last = sessionStorage.getItem(key) || ""; } catch (error) { last = ""; }
      if (last === version) return;
      try { sessionStorage.setItem(key, version); } catch (error) { return; }
      location.reload();
    }).catch(() => {});
  }

  function initVersion() {
    const foot = $(".rail-foot");
    let node = $("#appVersion");
    if (!node && !foot) return;
    if (!node) {
      node = document.createElement("div");
      node.id = "appVersion";
      node.className = "app-version";
      foot.appendChild(node);
    }
    node.textContent = "v—";
    node.title = "正在读取版本信息";
    fetchJson("/api/version", {}, 5000).then((info) => {
      const version = String(info.version || "未知");
      const commit = String(info.gitCommit || "").slice(0, 7);
      const branch = String(info.gitBranch || "");
      const stale = /^\d+\.\d+\.\d+/.test(assetVersion) && assetVersion !== version;
      node.textContent = `v${version}${commit ? ` · ${commit}` : ""}${stale ? " · 待重启" : ""}`;
      node.title = [
        `sologsb 调度台 ${version}`,
        branch ? `分支 ${branch}` : "",
        commit ? `提交 ${commit}` : "",
        stale ? `页面资源为 v${assetVersion}，服务进程仍是 v${version}：重启服务后生效` : "",
      ].filter(Boolean).join("\n");
      node.classList.toggle("stale", stale);
      node.dataset.version = version;
    }).catch((error) => {
      if (/^\d+\.\d+\.\d+(?:[-+].*)?$/.test(assetVersion)) {
        node.textContent = `v${assetVersion}`;
        node.title = `sologsb 调度台 ${assetVersion}\n版本接口暂不可用，显示静态资源版本`;
        node.dataset.version = assetVersion;
        return;
      }
      node.textContent = "版本不可用";
      node.title = String(error.message || error);
    });
  }

  /* ------------------------------------------------------------ visibility */
  function onVisibilityChange(handler) {
    document.addEventListener("visibilitychange", () => handler(!document.hidden));
  }

  // Labels for the skill's task / side statuses (state.json), shared by the
  // monitor page and the queue page so the two never disagree.
  const TASK_STATE_LABELS = {
    prepared: "已接入", prompt_ready: "提示词就绪", repo_ready: "仓库就绪", running: "执行中",
    candidates_running: "候选竞速中", candidates_ready: "候选竞速结束", a_staged: "A 已校验",
    b_staged: "B 已校验", semantic_review_required: "待语义审核", ab_clean: "A/B 已发布",
    verified: "已验证", gsb_ready: "GSB 就绪", recorded: "已录屏", complete: "已完成",
    attempt_invalid: "本轮无效", blocked: "已阻断", failed: "已失败", error: "出错", staged: "已校验",
    clean: "已发布", invalidated: "已作废", cancelled: "未进前二", idle: "未启动",
  };

  window.SoloApp = {
    $, $$, esc, debounce, fetchJson, postJson, TASK_STATE_LABELS,
    fmtDuration, fmtAge, fmtClock, fmtTime,
    toast, initTheme, renderRail, renderMetrics, connectStream,
    renderKeyed, setText, setHtml, setAttr, onVisibilityChange, initVersion,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initVersion, { once: true });
  } else {
    initVersion();
  }
})();
