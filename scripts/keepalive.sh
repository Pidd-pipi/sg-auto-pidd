#!/bin/zsh
# Unattended runner for the sologsb monitor.
#
#   scripts/keepalive.sh [server.py 参数…]      例如 scripts/keepalive.sh --port 8791
#
# - Restarts server.py when it exits unexpectedly (backoff 5s → 5min).
# - Restarts it when /api/health keeps reporting ok=false (a background loop
#   stalled) or stops answering at all (the process is wedged).
# - Keeps the Mac from idle-sleeping while the monitor runs (caffeinate -i);
#   a sleeping Mac stops every container and the scheduler with it.
# - Rotates .state/monitor.log (server stdout/stderr) past 20 MB.
#
# Ctrl-C / SIGTERM stops the server gracefully and exits.
#
# Tunables (environment):
#   KEEPALIVE_CHECK_SECONDS      health check interval            (default 60)
#   KEEPALIVE_UNHEALTHY_CHECKS   consecutive bad checks → restart (default 5)
#   KEEPALIVE_NO_CAFFEINATE=1    do not hold the sleep assertion
set -u

APP_DIR="${0:A:h:h}"
cd "$APP_DIR" || exit 1
STATE_DIR="$APP_DIR/.state"
mkdir -p "$STATE_DIR"
SERVER_LOG="$STATE_DIR/monitor.log"
KEEPALIVE_LOG="$STATE_DIR/keepalive.log"
CHECK_SECONDS="${KEEPALIVE_CHECK_SECONDS:-60}"
UNHEALTHY_LIMIT="${KEEPALIVE_UNHEALTHY_CHECKS:-5}"
LOG_ROTATE_BYTES=$((20 * 1024 * 1024))
PYTHON="${PYTHON:-python3}"

# Port: an explicit --port wins, otherwise config.json, otherwise 8790.
PORT=""
args=("$@")
for (( i = 1; i <= $#args; i++ )); do
  case "${args[i]}" in
    --port) PORT="${args[i+1]:-}" ;;
    --port=*) PORT="${args[i]#--port=}" ;;
  esac
done
if [[ -z "$PORT" ]]; then
  PORT="$("$PYTHON" -c 'import json,sys
try: print(int((json.load(open("config.json")).get("server") or {}).get("port") or 8790))
except Exception: print(8790)' 2>/dev/null)"
fi
HEALTH_URL="http://127.0.0.1:${PORT}/api/health"

log() { print -r -- "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$KEEPALIVE_LOG"; }

rotate() {
  local file="$1"
  [[ -f "$file" ]] || return 0
  local size
  size=$(stat -f %z "$file" 2>/dev/null || echo 0)
  if (( size > LOG_ROTATE_BYTES )); then
    mv -f "$file" "$file.1"
    log "日志轮转：$file（${size} 字节）→ $file.1"
  fi
}

# Exit 0 when /api/health answers ok=true; 1 when it answers ok=false; 2 when it
# does not answer.
health() {
  local body
  body=$(curl -s -m 10 "$HEALTH_URL" 2>/dev/null) || return 2
  print -r -- "$body" | "$PYTHON" -c 'import json,sys
try: sys.exit(0 if json.load(sys.stdin).get("ok") else 1)
except Exception: sys.exit(2)'
}

SERVER_PID=""
CAFF_PID=""
STOPPING=0

stop_server() {
  [[ -n "$SERVER_PID" ]] || return 0
  if kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null
    for _ in {1..20}; do
      kill -0 "$SERVER_PID" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$SERVER_PID" 2>/dev/null; then
      log "服务 20 秒内未退出，强制结束 pid=$SERVER_PID"
      kill -KILL "$SERVER_PID" 2>/dev/null
    fi
  fi
  wait "$SERVER_PID" 2>/dev/null
}

on_signal() {
  STOPPING=1
  log "收到停止信号，正在停止服务"
  stop_server
  [[ -n "$CAFF_PID" ]] && kill "$CAFF_PID" 2>/dev/null
  exit 0
}
trap on_signal INT TERM

log "keepalive 启动：端口 $PORT，健康检查每 ${CHECK_SECONDS}s，连续 ${UNHEALTHY_LIMIT} 次异常重启"
backoff=5
while (( ! STOPPING )); do
  rotate "$SERVER_LOG"
  rotate "$KEEPALIVE_LOG"
  started_at=$(date +%s)
  PYTHONUNBUFFERED=1 "$PYTHON" server.py "$@" >>"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  if [[ -z "${KEEPALIVE_NO_CAFFEINATE:-}" ]] && command -v caffeinate >/dev/null; then
    caffeinate -i -w "$SERVER_PID" &
    CAFF_PID=$!
  fi
  log "服务已启动 pid=$SERVER_PID"

  bad=0
  reason=""
  forced=0
  while kill -0 "$SERVER_PID" 2>/dev/null; do
    # Short sleeps so a signal is handled promptly.
    for _ in $(seq 1 "$CHECK_SECONDS"); do
      kill -0 "$SERVER_PID" 2>/dev/null || break 2
      sleep 1
    done
    health
    case $? in
      0) bad=0 ;;
      1) bad=$((bad + 1)); reason="后台循环卡住（/api/health ok=false）" ;;
      *) bad=$((bad + 1)); reason="健康检查无响应" ;;
    esac
    if (( bad > 0 )); then
      log "健康检查异常 ${bad}/${UNHEALTHY_LIMIT}：$reason"
    fi
    if (( bad >= UNHEALTHY_LIMIT )); then
      log "连续 ${bad} 次异常，重启服务：$reason"
      stop_server
      forced=1
      break
    fi
  done

  wait "$SERVER_PID" 2>/dev/null
  code=$?
  SERVER_PID=""
  (( STOPPING )) && break
  if (( forced )); then
    sleep 5
    continue
  fi
  uptime=$(( $(date +%s) - started_at ))
  if (( code == 2 )); then
    # Another instance holds the lock or the port is taken: wait for it.
    log "服务无法启动（退出码 2：已有实例在运行或端口被占用），30 秒后重试"
    sleep 30
    continue
  fi
  (( uptime > 600 )) && backoff=5
  log "服务退出（退出码 $code，运行 ${uptime}s），${backoff}s 后重启"
  sleep "$backoff"
  backoff=$(( backoff * 2 > 300 ? 300 : backoff * 2 ))
done
