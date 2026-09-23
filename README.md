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

监控台只限制**同时在飞的任务数**。任务被领取后直接启动对应执行器，不再按
`candidatesPerTask` 批量预占容器槽；每个候选真正执行 `docker run` 前，由技能侧的
`side_runner._ContainerLimiter` 通过跨进程文件锁依次取得容器名额。这样多个任务可以并行
推进提示词和初始化，而候选容器仍严格受全局上限约束，不会因一个任务的整批预占位阻塞队列。

旧版本监控台写入的带 `itemId` 预占位会在纠错循环中自动清理；技能执行器自己的
per-container 预占位不带 `itemId`，不会被监控台误删。

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

独立循环，默认 60 秒一轮（`automation.reconcileSeconds`，15–3600）。每轮核查：

| 检查项 | 动作 |
|---|---|
| 容器预占位超时（占槽但无容器） | 终止 worker、释放槽位、重新入队 |
| 队列项终态但容器仍在跑 | 删除僵尸容器 |
| 槽位标记的 PID 已死 | 清理标记 |
| `attempts` 异常膨胀（≥500） | 停止累加并告警 |
| `orphaned` + `capacityHeld` 卡死 | 超过宽限期自动释放名额并标记 `skipped` |
| 队列项与 `result.json` 状态不一致 | 以 `result.json` 为准重新同步 |
| 配额 `claimed` 超过 6 小时未结算 | 强制回补并记录 |

删除容器的前提 deliberately 很窄：任务必须由某个终态队列项拥有，**并且**其
`state.json` 自身也报告终态。`_triggered`（启动时恢复的归档）不参与判定——它同时包含
仍在运行的任务，误判会删掉活容器。

## 配置

配置文件是 [`config.json`](./config.json)。

- `roots` / `monitor.activeRoots`：任务扫描根目录 / 其中实际参与扫描与启动的子集。
- `server.host` / `server.port` / `server.allowRemoteActions`。
- `automation.scheduleMode`：`containers` 或 `tasks`。
- `automation.capacity`（= `maxTasks`）：并行任务数上限。
- `automation.maxContainers`：监控台展示用候选容器上限；实际容器准入由技能设备配置
  `claude.maxContainers` 和 `side_runner._ContainerLimiter` 执行。
- `automation.candidatesPerTask`：单任务备选容器数。
- `automation.containerRefillBelow`：旧版监控台补位阈值，当前不再参与容器准入。
- `automation.startupTimeoutSeconds`（300–600）/ `automation.containerReserveSeconds`（300–600）。
- `automation.reconcileSeconds`（15–3600）。
- `automation.promptTemplate`：占位符
  `{{selected_project}} {{project_code}} {{project_name}} {{task_type}} {{difficulty}}
  {{base_url}} {{max_tasks}} {{max_containers}} {{candidates_per_task}} {{schedule_mode}}`。
- `automation.autoRefill`：动态随机待办池。
- `platform.*`：Solo Manager 地址、账号、Keychain service、令牌名。
  密码只写 Keychain，不落盘；设置页只显示「已保存 / 未保存」。
- `monitor.parseTraceOnSnapshot` / `monitor.traceMaxBytes` / `monitor.dockerCacheSeconds`。

`.state/settings.json` 保存操作员状态（默认文件夹、UI 偏好、Manager 连接标记），
不含任何凭据原文。

## API

```text
GET  /api/health
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
                                        set-auto-refill-weights|set-roots|set-root-active|
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

## 测试

```bash
python3 -m pytest tests/ -q
python3 -m py_compile server.py api/*.py queue_worker.py queue_log.py
```

测试全部使用临时 state 目录和临时槽位目录，不会触碰 `.state/` 或共享的容器名额。
