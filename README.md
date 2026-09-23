# sologsb 调度监控台

> 公开版不附带真实运行截图，避免泄露任务名、轨迹和命令输出。

`sologsb-0917` Pair-wise GSB 的调度监控台。单进程、单端口、推送驱动：后台线程每 1.5 秒
构建一次增量快照，通过一条 SSE 连接把「变化了的任务」推给所有页面；只有 SSE 断线时前端
才回退到轮询。

四个页面：

| 路径 | 用途 |
|---|---|
| `/` | 任务监看：卡片网格 + 六状态筛选 + 弹窗（概览 / 运行日志 / 提示词） |
| `/tasks` | 队列管理：Solo Manager 项目入队、调度模式与上限、配额与槽位状态 |
| `/settings` | 设置：Solo Manager 凭据、默认文件夹、监控目录、运行状态 |
| `/logs` | 全局调度日志（JSONL 落盘 + SSE 实时滚动） |

## 启动

```bash
./run.sh
```

默认地址：<http://127.0.0.1:8790>

也可以指定多个扫描根目录或端口：

```bash
python3 server.py --root /path/to/task-root --port 8790
```

单实例锁仍然生效：第二个实例会以退出码 2 退出并提示 owner pid。

### 无人值守运行

```bash
scripts/keepalive.sh                # 参数原样传给 server.py，例如 --port 8791
```

`keepalive.sh` 是一个很薄的守护脚本：

- 服务异常退出后自动重启（退避 5 秒 → 5 分钟，稳定运行 10 分钟后重置）；退出码 2（已有实例 /
  端口占用）时每 30 秒重试，不会与手动启动的实例抢锁。
- 每 60 秒查询 `/api/health`：连续 5 次 `ok=false`（后台循环卡住）或无响应（进程僵死）时，
  先 SIGTERM，20 秒后仍未退出再 SIGKILL，然后重启。
- 用 `caffeinate -i` 阻止 Mac 空闲睡眠——睡眠会让容器和调度一起停下。
- `.state/monitor.log`、`.state/keepalive.log` 超过 20 MB 自动轮转。
- 环境变量：`KEEPALIVE_CHECK_SECONDS`、`KEEPALIVE_UNHEALTHY_CHECKS`、`KEEPALIVE_NO_CAFFEINATE=1`。

进程内部另有一层看门狗：三个后台循环（调度 `auto-loop`、纠错 `reconcile`、快照 `snapshot-hub`）
每轮上报心跳，超过阈值（10 个周期，至少 180 秒；纠错循环至少 600 秒）没有完成一轮就记
`watchdog.loop_stalled`，日志里附带该线程当前卡住的调用栈；线程意外退出会被自动拉起
（`watchdog.loop_dead`）。设置页「运行状态」逐个显示循环状态，`/api/health` 的 `ok` 随之变化。

## 版本体系

项目使用根目录 `VERSION` 作为唯一版本源，遵循语义化版本 `MAJOR.MINOR.PATCH`。
运行时通过 `GET /api/version` 返回版本号、Git 分支和短提交号；四个页面的左下角
会统一显示 `v版本号 · 短提交号`，鼠标悬停可查看完整信息。

升级版本：

```bash
python3 scripts/bump_version.py patch   # 0.1.0 → 0.1.1
python3 scripts/bump_version.py minor   # 0.1.1 → 0.2.0
python3 scripts/bump_version.py major   # 0.2.0 → 1.0.0
python3 scripts/bump_version.py 1.2.0   # 指定完整版本
```

脚本会同步更新 `VERSION` 和四个静态页面中的资源缓存键。发布前同时在
`CHANGELOG.md` 记录变更。

## 架构

```
server.py            HTTP + SSE（路由 / 鉴权 / 静态文件缓存 / 一条 SSE 端点）
api/
  common.py          配置、JSON/文件缓存、密钥、进程表、设置存储
  tasks.py           任务发现、瘦身卡片、按需详情、轨迹解析、docker 缓存
  scheduler.py       队列状态机、两种容量模型、容器槽位账本、配额生命周期、纠错循环
  platform.py        Solo Manager 适配（认证 / 项目列表 / 配额预扣与回补）
  folders.py         ChatGPT 应用文件夹读取（~/.codex/state_5.sqlite）
  logs.py            全局调度日志（JSONL + 轮转 + 订阅推送）
static/              app.css / app.js + 四个页面，原生 JS，无构建链
.state/              queue.json、settings.json、scheduler.jsonl、jobs/
```

### 推送替代轮询

`SnapshotHub` 每 1.5 秒构建一次快照，与上一份做差后只推送变化的卡片。对比时会先剔除
`updatedAt` / `generatedAt` 等每次构建都会变的标量，否则hub 会认为自己每秒都在变化、
持续推送空增量。SSE 断线时客户端按 `afterSeq` 续传日志，并重新拉取一次全量快照。

### 快照瘦身

列表接口只返回卡片需要的字段。`promptText` / `sides` / `candidates` / `workflow` /
`taskRoot` / `artifacts` / 容器名列表全部移到按需的 `GET /api/tasks/:id`。队列项的
`triggerPrompt`（每条约 1.5 KB）也移到 `GET /api/queue/:itemId/prompt`。

### 文件读取策略

- `state.json` / `attempt.json` / `result.json` 按 `(path, mtime_ns, size)` memoize，
  同一个 tick 内不重复解码。
- `stdout.jsonl` 与任务日志从文件尾部倒读 N 行，不再整文件 `read_text()`。
- 进程探活共用一个 `ps -axo pid=,command=` 快照，一个 tick 内最多跑一次，而不是每侧一次。
- `docker ps` 保留 2 秒 TTL，但失败结果不再进缓存——以前一次瞬时失败会让队列停摆 2 秒。
- `TraceCache` 有 LRU 上限（默认 400 条），不再永久常驻。

## 两种调度模式

`automation.scheduleMode` 决定容量模型，顶栏徽标可一键切换。

| 模式 | 保持恒定 | 允许浮动 | 适用 |
|---|---|---|---|
| `containers`（容器优先，默认） | 运行中的候选容器数 | 并行任务数 | Key 并发是瓶颈，想让容器跑满 |
| `tasks`（任务数量优先） | 并行任务数 | 运行中的候选容器数 | 想控制同时进行的题目数 |

**容器数只在监控台设置一处**（队列管理页「最大容器数」，1–6）。监控台启动时和每次保存都会把
它连同 `excludedProjectCodes` 写入技能读取的 `~/.codex/sologsb-0917/container-limit.json`，并带
`managedBy: sologsb-monitor` 标记；技能的 `_ContainerLimiter` 看到这个标记时优先使用它，
不再被设备配置 `claude.maxContainers` 覆盖。队列页会显示技能侧是否已同步。

占用 = 运行中的候选容器 + 活任务仍需要的容器（从各任务 `state.json` 读取：未初始化的任务按
一整批计，`candidates` 里状态为 `running` 但还没有容器的候选按 1 计，候选竞速结束后计 0）。

- 容器优先：`占用 + 1 > 上限` 时等待，否则启动；溢出的候选由技能限流器排队，容器保持跑满。
  这个模式不看最大任务数。
- 任务数量优先：活任务数达到最大任务数时等待；容器仍受同一个上限约束。

已初始化的任务如果有候选标记为 `running`，却在容器未满的情况下超过 `phantomDemandSeconds`
（默认 600 秒）仍拿不到容器，判定其执行器已退出（技能限流器几秒内就会把空位交给排队候选），
不再计入占用并记一次 `queue.phantom_demand`，避免失联任务把名额一直占到 30 分钟的孤儿宽限期。

两次启动之间至少间隔 `cooldownSeconds`（不低于 20 秒），每个 tick 最多启动一个任务——同一秒
连发两个桌面深链会丢掉其中一个提示词并留下空目录。

监控台不再写占位标记：技能限流器会把它们一并计数，刚领取的任务的候选会排在自己的占位后面
（cy-381 就因此死锁成 blocked）。

### 同项目旧任务的放行

技能 `init` 会扫描工作目录下所有 `*/monitor/state.json`，只要同一项目有任务处于 `running` /
`blocked`（或任一候选处于 `running` / `blocked` / `attempt_invalid`），就拒绝接入新任务——这是
技能的并发门禁，但它不判断进程是否还活着。执行器已经退出的 `blocked` 任务因此会让该项目的每次
重试都死在门禁上。

调度器在启动某项目前会检查同一工作目录里该项目的旧任务：**执行器不在运行、没有候选容器、没有
活的 runPid、不被任何活跃队列项持有、`state.json` 已静默至少 120 秒**时，把它标记为 `failed`
（相关候选标记 `invalidated`），并保留 `previousStatus` / `blockedReason` / `closedReason`，记一条
`queue.stale_task_closed`。其他项目的任务、仍有活信号的任务一律不动。

## 队列页的分组

队列项的 `status` 是状态机的词（pending / launching / running / triggered / orphaned …），页面按
服务端从 `state.json` 与 `docker ps` 算出的 `phase` 分组：

| phase | 含义 | 出现在 |
|---|---|---|
| `queued` | 排队等启动（含启动间隔、保护期等门禁） | 执行队列 |
| `waiting_slot` | 等容器名额：还没启动的项，或已启动但候选在技能限流器里排队（`containers.running < wanted`） | 执行队列 |
| `retrying` | 失败后等待自动重试 | 执行队列 |
| `starting` | 执行器启动中 / 桌面任务刚创建，候选尚未启动 | 执行队列 |
| `attention` | `orphaned`：执行器退了但桌面任务未到终态，需人工确认 | 执行队列顶部 |
| `executing` | 容器已创建并交接给执行器，或候选阶段已结束进入审核/发布 | 「执行中」面板 |
| `done` / `failed` / `skipped` | 终态 | 执行队列底部折叠组 |

每项带 `containers: {running, wanted}`（已创建容器 / 本轮 `running` 候选数）和 `monitorTaskId`
（监看页的任务 id），「执行中」的项可直接跳到监看页。任务到达终态后离开「执行中」。

## 配额生命周期

```
pending  → claimed   领取时 POST /api/v1/tasks 预扣除，记 deductedAt + remainingBefore
claimed  → settled   终态且成功，正式扣除，记 remainingAfter
claimed  → refunded  失败/中止，POST /api/v1/tasks/{id}/cancel 回补，记 refundReason
```

- 预扣除得到的 `platformTaskId` 会通过 `--platform-task-id` 传给 worker，再由
  `platform_bridge bootstrap` 复用该任务，不会二次扣除。
- 回补端点是 `POST /api/v1/tasks/{taskId}/cancel`（已实测：返回 200 并恢复
  `projectUsageCount`）。部署没有该端点时退化为本地记账，`refundMode` 记为 `local`，
  界面照常显示回补意图。
- 执行器自己选项目时写的 `platform/selection.json` 也会被吸收，以执行器的 `taskId`
  为权威记录。

## 定时纠错

独立循环，默认 60 秒一轮。只有真正纠错时才写 `reconcile.done`；「保留名额」这类判断同一原因只记一次。
终态队列项（done / failed / skipped）只结算一次，不会被重复判失败、复活或重复回补。每轮核查：

| 检查项 | 动作 |
|---|---|
| 队列项终态但容器仍在跑 | 删除僵尸容器 |
| 槽位标记的 PID 已死 | 清理标记 |
| `attempts` 异常膨胀（≥500） | 停止累加并告警 |
| `orphaned` + `capacityHeld` 卡死 | 超过宽限期且无活信号（执行器、容器、近期写入）时释放名额、回补一次并标记 `skipped` |
| 队列项与 `result.json` 状态不一致 | 以 `result.json` 为准重新同步（不写日志） |
| 配额 `claimed` 超过 6 小时未结算 | 强制回补并记录 |

删除容器的前提 deliberately 很窄：任务必须由某个终态队列项拥有，**并且**其
`state.json` 自身也报告终态。`_triggered`（启动时恢复的归档）不参与判定——它同时包含
仍在运行的任务，误判会删掉活容器。

## 兜底策略

`api/guard.py`，随纠错循环每轮检查一次（默认 60 秒），`automation.guard.mode` 三档：

- `observe`（默认）：只在队列项上挂 `guardFlag`、记 `guard.would_stop` / `guard.leak_found`（同一情况只记一次），
  队列页「兜底策略」面板列出命中项，不动任何进程。
- `enforce`：命中即处理（见下）。
- `off`：不检查，并清掉已有标记。

| 策略 | 条件 | 默认阈值 |
|---|---|---|
| 候选阶段超时 `candidate-timeout` | 队列持有的任务 `candidateRaceStartedAt` 起算，竞速仍未结束 | `candidatePhaseHours` = 6（历史最慢正常竞速 5.5 h） |
| 长时间无进展 `no-progress` | 队列持有、未结束的任务：`state.json`、`monitor/`、`workspace/`、`source/`（跳过 `node_modules` / `.git` / `verify` 等）、worker 日志都没有写入 | `noProgressMinutes` = 60 |
| 残留进程 `leaked-process` | 任务目录已 `complete` / `failed` / `blocked` / `error` / `stopped` 超过阈值、没有队列项或 worker 持有，其路径下仍有进程 | `leakedProcessMinutes` = 60 |

`enforce` 下终止一个任务：把任务名写入**技能**的停止名单 `~/.codex/sologsb-0917/stop-tasks.json`（桌面 agent 之后再调
`sologsb.py` 会以 78 退出），对任务路径下的进程发 SIGTERM，删除 `sologsb-<任务名>-*` 容器，`state.json` 标记 `failed`
（`closedPolicy` / `closedReason`，候选 `invalidated`），队列项 `failed` 并回补配额，记 `guard.stopped`。**任务目录与轨迹
保留**。不使用监控台自己的 `stopTasks`——`enforce_stop_tasks` 会删除命中标记的目录。残留进程只 SIGTERM 这些进程。

从不处理：登录 shell / zsh / bash（终端里 `cd` 进任务目录的会话）、`queue_worker.py`（任务终态后自行退出）、
监控台自身、`excludedProjectCodes` 中的项目。

## 配置

配置文件是 [`config.json`](./config.json)。

- `roots` / `monitor.activeRoots`：任务扫描根目录 / 其中实际参与扫描与启动的子集。
- `server.host` / `server.port` / `server.allowRemoteActions`。
- `automation.scheduleMode`：`containers` 或 `tasks`。
- `automation.maxContainers`：唯一的容器上限（1–6），同步给技能。
- `automation.candidatesPerTask`：单任务候选数（2–8）。
- `automation.capacity`（= `maxTasks`）：仅任务数量优先模式生效。
- `automation.cooldownSeconds`：启动间隔（最低 20 秒）。
- `automation.pauseOnStart`（默认 `true`）：每次服务进程启动都先把队列置为暂停并写回 `config.json`，忽略上次保存的 `paused`；崩溃自动重启、部署、重启机器后都不会在没人看的情况下拉起新任务，需在队列页手动「启动队列」。记一条 `config.paused_on_start`。设为 `false` 沿用上次状态。
- `automation.phantomDemandSeconds`：失联候选多久后不再计入占用（120–7200，默认 600）。
- `monitor.watchdogSeconds`：进程内看门狗检查间隔（默认 30）。
- 其余时间参数（`startupTimeoutSeconds` / `reconcileSeconds` / `startupGraceSeconds` /
  `stalledTaskRetrySeconds` …）保留默认即可，不在界面暴露。
- 已废弃并在启动时自动移除：`containerRefillBelow`、`containerReserveSeconds`、
  `keyConcurrency`（其 `maxCandidateContainers` 在未设置 `maxContainers` 时并入）。
- `automation.promptTemplate`：占位符
  `{{selected_project}} {{project_code}} {{project_name}} {{task_type}} {{difficulty}}
  {{base_url}} {{max_tasks}} {{max_containers}} {{candidates_per_task}} {{schedule_mode}}`。
- `automation.autoRefill`：动态随机待办池。`enabled` 由队列页「自动补队」开关控制
  （`set-auto-refill`）；开启后待启动少于 `targetPending` 时每 `intervalSeconds` 秒从 Solo Manager
  随机补充，队列暂停时只补充不启动。补队打乱待启动顺序时不动前 10 项（`AUTO_REFILL_PREVIEW_SIZE`），
  队列页「接下来启动」预览的就是实际启动顺序，每项可直接禁用。上次补队结果在 `autoRefill.lastRun`。
- `platform.*`：Solo Manager 地址、账号、Keychain service、令牌名。
  密码只写 Keychain，不落盘；设置页只显示「已保存 / 未保存」。
- `monitor.parseTraceOnSnapshot` / `monitor.traceMaxBytes` / `monitor.dockerCacheSeconds`。

`.state/settings.json` 保存操作员状态（默认文件夹、UI 偏好、Manager 连接标记），
不含任何凭据原文。

## API

```text
GET  /api/health                       ok=false 表示有后台循环卡住；stats.loops 为各循环心跳
GET  /api/snapshot                      全量快照（SSE 首帧与断线重连用）
GET  /api/stream?logs=1&afterSeq=0      SSE：snapshot / tasks / log / heartbeat / ping
GET  /api/tasks                        瘦身卡片列表
GET  /api/tasks/:id                    完整详情
GET  /api/tasks/:id/log?side=A&lines=200
GET  /api/tasks/:id/history?side=A&limit=400
GET  /api/folders                      ChatGPT 应用前 10 个文件夹
GET  /api/queue                        队列快照
GET  /api/queue/:itemId/prompt
GET  /api/settings
GET  /api/logs?afterSeq=0&limit=200&level=
GET  /api/submissions?refresh=0
GET  /api/platform/projects?taskType=0-1代码生成&refresh=0
POST /api/action                       {"taskId","side":"A|B","mode":"resume|rerun"}
                                        或 {"action":"dismiss|restore"}
POST /api/automation                   {"action":"set-schedule-mode|set-limits|set-capacity|set-cooldown|
                                        set-prompt-template|set-paused|set-merge-project-pool|
                                        set-auto-refill|set-auto-refill-weights|set-guard|set-roots|set-root-active|
                                        queue-add|queue-add-platform|queue-remove|queue-move|
                                        queue-retry|queue-release|queue-clear"}
POST /api/settings                     {"settings":{...}}
POST /api/settings/manager             {"managerBaseUrl","username","password"}
```

POST 操作默认只接受本机请求。需要局域网操作时先自行确认网络环境，再设置
`server.allowRemoteActions=true`。

## 续跑语义

手动续跑只针对单侧，没有「A+B 一起续跑」的入口。A 和 B 同时重启时分不清是哪一侧的
失败引发了共享容器或 Key 耗尽，而候选竞速本身已经在任务入队时把两侧都跑起来了。
任务详情弹窗的概览页里，每一侧各自有「续跑」和「重跑」两个按钮。

页面上的「续跑」只调用技能 CLI，不直接操作容器或任务状态：

```text
续跑 A/B   → sologsb.py run --task-root ROOT --side A|B
重跑 A/B   → sologsb.py run --task-root ROOT --side A|B --force
```

静态资源带 `ETag` 与 `If-None-Match`，HTML 用 `no-cache`，JS/CSS 用 5 分钟缓存，
字体用一年 `immutable`；所有超过 512 字节的响应在客户端声明 gzip 时压缩。

## 环境变量

- `SOLO_MANAGER_BASE_URL` / `SOLO_MANAGER_USERNAME` / `SOLO_MANAGER_PASSWORD`
- `SOLO2_SERVER`：SOLO2 地址；留空时禁用直连提交信息查询。
- `SOLOSB_CONTAINER_SLOTS`：容器槽位标记目录，默认
  `~/.codex/sologsb-0917/container-slots`。
- `CODEX_STATE_DB`：ChatGPT 文件夹数据库路径，默认 `~/.codex/state_5.sqlite`。
- `SOLOGSB_ACCESS_LOG=1`：记录全部 HTTP 访问日志（默认只记 4xx/5xx，避免无人值守时日志暴涨）。

## 测试

```bash
python3 -m pytest tests/ -q
python3 -m py_compile server.py api/*.py queue_worker.py queue_log.py
```

测试全部使用临时 state 目录和临时槽位目录，不会触碰 `.state/` 或共享的容器名额。
