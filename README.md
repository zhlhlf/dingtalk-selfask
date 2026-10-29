# 钉钉「aa 线程」自问自答（dingtalk-selfask） <!-- project: path:/root/.openclaw/workspace -->

2026-10-22 落地；2026-10-23 重做成**线程模型**：`aa ` 建一个线程会话，`aa<N> ` 往线程 N 里 push，`aa<N>` 只看现状。
用途：在钉钉里自己发命令，助手在该会话里回话。全程走已登录的 dws 个人身份，不需要机器人 AppKey、不需要企业内部应用审批。

## 命令

| 你发的 | 行为 | 助手回什么 |
| --- | --- | --- |
| `aa` | 本会话线程一览（在跑 / 排队 / 空闲） | 纯列表，一条线程一行 |
| `aa0` | 同 `aa`，但每条线程多打一行**会话 key** | 列表 + `key: dthread-…`，方便复制去 `/fork` |
| `aa <正文>` | 新建线程会话，把正文交给它干活 | `线程会话N 创建成功，正在处理…` |
| `aa<N> <正文>` | push 进线程 N；**跑着就 steer 进当前 run**，可连续多次 | `线程会话N push消息成功`（空闲时改为「刚才空闲…按新一轮开始跑了」） |
| `aa<N>` | 不 push，只报线程 N 现状 | `线程会话N 正在跑（已 Xm，累计 push M 条）` / `现在空闲…` |
| `aa<N> /xxx` | 交给该会话的 OpenClaw 斜杠命令层；`/new`、`/reset`、`/clear` 重置线程 | 命令回显，或重置确认 |
| `aa<N> /his` | 报线程 N 的会话 key + **我自己发过的消息**，一行一个编号（默认全列）；列表中间标「⟲ 回退到这儿」切点 | key + 编号列表 + 两块独立 text 围栏（rewind / fork 各一块，互不混排） |
| `aa<N> /rewind <编号>` | **就地**回退：保留该条及之前，撤掉它之后的（key 不变，编号来自 `/his`）；被撤掉的第一条原文回给你改 | `已回退到第 N 条之后…` + 原文 |
| `aa<N> /stop` | 本地打断这一轮（`/abort` 同）；先 abort（上下文保留），被拒才 reset（清空） | `已停下，上下文保留` / `…上下文已清空` |
| `aa<N> /fork <会话key>` | 新开线程 N，把指定会话的**整段**上下文复制进来；**源会话只读不写** | `线程会话N 已新开，已复制 … 的上下文；原会话没动。` |
| `aa<N> /fork <会话key> <编号>` | 保留该会话第 N 条及之前，把后面的切给线程 N（编号来自 `/his`）；线程 N 改指新生成的 key | `已从 … 的第 N 条之后分叉，线程现在指向 …` |

回执统一格式：**首行固定是「线程会话N」**（`t_reply` 一处加，多线程并行时才分得清谁在说话）。

严格匹配、大小写敏感：`aa` 后必须是半角空格或数字。裸 `/xxx`、`aa /xxx`、`aa `、`aa正文`、`AA `、`aa1x` 都不触发。

`/fork` 与 `/rewind` 的分工：`/fork <key> <n>` 另开一份（新 key，原会话不动，适合试别的走法）；`/rewind <n>` 就地剪回去（key 不变，改完直接重发）。两者都只动上下文，**撤不掉工具副作用**。

**编号语义（2026-10-29 定）**：编号 n 表示「**保留第 n 条及之前**」，切点在 n 与 n+1 之间；也就是说选中的那条不会被撤掉。编号是最后一条时没有可切的位置：`/rewind` 会提示改用 n-1，`/fork` 会提示用不带编号的整段复制。

`/fork` 的约束：
1. 目标线程 N **强制重建**：已有上下文会被替换（旧 transcript 进归档），回执会明确说「原有上下文已替换」。刚 `/new` 过的线程只留下一条 system `Reset`，按空线程处理。
2. 线程 N 正在跑时不许 fork。
3. 源会话按 `parentSessionKey` 只读复制，复制到最后一条完整的助手消息（`forkFrom: "last-completed"`）。
4. 带编号的 fork 走网关 `sessions.fork`（实际下刀在 n+1 那条用户消息之前）：新 key 由网关生成，所以线程 N 会换 key（原 key 的完整历史留着不动）；编号在 `/his` 时就落盘（`his_index.json`），之后会话再多几条也不会指错。

## 触发条件

**本人发送** + 剥掉 `@周后霖` / `<@oid>` 前缀后才判定（这些命令都吃 `aa` 前缀）。

| 正文 | 结果 |
| --- | --- |
| `aa` | ✅ 本会话线程一览 |
| `aa0` | ✅ 线程一览 + 每条会话 key |
| `aa 帮我看看日志` | ✅ 新建线程，任务「帮我看看日志」 |
| `<@self> aa 群里任务` | ✅ 同上（先剥 @） |
| `aa1 再补一句` | ✅ push 进线程 1 |
| `aa1` / `aa1 ` | ✅ 只查线程 1 现状 |
| `aa1 /status` | ✅ 裸发给 1 号线程所在的 OpenClaw 会话 |
| `aa2 /fork dthread-96b3b8fe999a` | ✅ 新建 2 号线程，复制该会话的上下文（源会话不动） |
| `aa1 /new` | ✅ 中断后重置 1 号线程，清空其上下文 |
| `/status` / `aa /status` / `aa ` / `aa正文` / `AA 大写` / `aa1x` | ❌ 不算命令，日志记 `ignore not-aa` / `empty-after-aa` / `slash-ignored` |
| `🤖 回答…` | ❌ 自己的回复，防自激 |

### 订阅范围（实测，决定要不要 @ 自己）

| 场景 | 不 @ 自己 | 带 @ 自己 |
| --- | --- | --- |
| 群聊 | ✅ 能收到（`user_im_message_receive_group_all`） | ✅ 能收到（两个订阅各来一次，按 message_id 去重） |
| 单聊（与自己的会话） | ❌ **收不到**，只有 `at-me` 会投递 | ✅ 能收到 |

实测依据：`--kind all-direct`（`user_im_message_receive_o2o_all`）订阅后向自己单聊发消息，消息 `sendStatus=SUCCESS` 但 consumer 收到 0 条；`--kind sender --open-dingtalk-id <自己>`（`user_im_message_receive_user`）同样 0 条。这两种 kind 都只覆盖「别人发给我」的消息，不覆盖「我自己发出」的单聊消息。

**结论：群里直接打 `aa 任务` 即可；单聊仍需 @ 自己一次（`@周后霖 aa 任务`）。**

## 目录与文件（按职责分层）

- 薄启动入口：`selfask.py`。启动方式仍是 `python3 selfask.py`，`watchdog.sh` 无需改动；同时继续转发既有的 `import selfask` 测试接口（`from selfask_app import *`）。
- 共享配置：`selfask_config.py`。常量、环境变量、开关、代理过滤、二进制路径；不依赖任何模块。
- 小工具：`selfask_util.py`。日志、子进程、时间格式化、连接状态记录（`CONN`）。
- 状态持久化：`selfask_state.py`。`ThreadStore` 负责 `threads.json` 的首次合盘、原子保存、稳定 key 与线程号分配；`seen.json` 与消息去重 `Store` 也在这里。
- Gateway 会话层：`selfask_gateway.py`。RPC 调用、会话读写（`session_state` / `session_send`）、abort→reset 兑底、斜杠回显收取（`await_reply`）。
- 钉钉 I/O：`selfask_dingtalk.py`。发消息（含群 @ 占位符与幂等键）、长连接子进程、群表、旧监听清理。
- 线程编排：`selfask_threads.py`。`aa` 线程生命周期、push/steer、slash 裸发与回显、reset、监控、本地状态文案；依赖上面全部。
- 命令解析：`selfask_commands.py`。剥 @ 前缀与严格 `aa` / `aa0` / `aa<N>` / slash 解析；纯函数，无状态。
- 装配层：`selfask_app.py`。消息路由（`handle_incoming`）、监听循环、周期补拉、单实例锁、`main()`。
- 离线回归：`test_selfask_regression.py`。覆盖路由语义、薄入口兼容面、纯 load 后 save 不丢 running 线程、`latest_thread` 逃线程。
- 守护脚本：`watchdog.sh`
- 运行日志：`selfask.log`；启动日志：`boot.log`
- 状态：`threads.json`（线程表）、`seen.json`（已处理 message_id，最多 500 条）、`groups.json`（我所在群的 conversationId 缓存）

依赖方向：`selfask.py → selfask_app.py → selfask_threads.py → {selfask_gateway.py, selfask_dingtalk.py, selfask_state.py} → {selfask_util.py, selfask_config.py}`；`selfask_commands.py` 独立。基础模块不反向导入上层，无循环依赖。

## 依赖

- `dws`（钉钉 CLI，v1.0.64 实测）：收消息（`event consume`）、发消息（`chat message send`）、查历史（`chat +chat-messages`）全靠它
- 已登录的 dws profile（`dws profile list` 有 active，且能 `dws contact user search`）
- OpenClaw gateway（`openclaw gateway call chat.send / chat.history / sessions.reset`）
- Python 3.9+，只用标准库，无第三方依赖

配置都在仓库根的 `.env`（从 `.env.example` 复制），详见下一节。

## 身份常量

身份**不写死在代码里**，按优先级取：

1. 真实环境变量 `SELFASK_SELF_OID` / `SELFASK_SELF_NAME`（systemd、命令行传的最优先）
2. 仓库根目录的 `.env`（`KEY=VALUE`，已 gitignore）
3. 仓库根目录的 `self_oid.txt` / `self_name.txt`（老写法，仍兼容）

三者都没有就发不出消息，boot 日志会报缺身份。推荐只维护 `.env` 一处。

需要准备的值（用 `dws` 自查，不要抄别人的）：

- 本人 openDingTalkId：`dws profile list` 取 userId，再 `dws contact user search --query "<姓名>"` 对照
- 本人 userId / corpId / dws profile：同上，`dws profile list` 直接给
- 本人姓名：用于剥掉群聊里的 `@名字` 前缀

## 架构

```
钉钉（本人账号）
  │  @我 / 群消息
  ▼
dws event consume ──► selfask_app：解析 → 路由（本地判定，不进模型）
  │                        ├─ aa          线程一览（读缓存，零网关调用）
  │                        ├─ aaN         单线程详情（chat.history 只读）
  │                        ├─ aaN <正文>  push / steer 进线程 N
  │                        ├─ aa <正文>   新建线程
  │                        └─ aaN /xxx    斜杠命令裸发进网关
  ▼
threads.json（线程账本：state / run / last_msg / preview / 计数）
  │
  ▼
OpenClaw gateway  chat.send ──► 线程会话 agent:main:dthread-<hash>
  │                                   │ 跑任务、调工具、改文件
  │                                   ▼
  └─ monitor 每 20s 轮询 chat.history（hasActiveRun + 本轮新正文）
        ├─ 正常收尾 → 取终稿 → send_reply
        ├─ 硬超时   → sessions.abort 后回话
        └─ 进度     → 每 15min 一条

send_reply → dws chat message send（群：--conversation-id + @占位符；单聊同样走 cid）
```

分层与依赖方向单向：`selfask.py → selfask_app.py → selfask_threads.py →
{gateway, dingtalk, state} → {util, config}`，`selfask_commands.py` 独立，无循环依赖。

| 模块 | 职责 |
| --- | --- |
| `selfask.py` | 瘦入口，只调 `main()` |
| `selfask_app.py` | 事件循环、命令路由、消息去重、补拉、网关健康自检 |
| `selfask_commands.py` | 命令解析（`aa` / `aa0` / `aaN` / `aaN 正文` / 斜杠；`/fork` 在 threads 层） |
| `selfask_threads.py` | 线程生命周期：起跑、push/steer、监控、超时、重置、aa 面板文案 |
| `selfask_gateway.py` | 封装 `chat.send` / `chat.history` / `sessions.abort|reset`，以及本轮正文与工具名提取 |
| `selfask_dingtalk.py` | 收发：`event consume` 解析、`chat message send` 分片发送、@ 占位符 |
| `selfask_state.py` | `threads.json` 持久化，多进程安全合并 |
| `selfask_util.py` | 日志、时长格式化等小工具 |

一句话流程：**消息进来先本地判命令；要干活才把正文投给线程会话；线程会话在网关里跑，
守护只负责盯状态、把终稿发回钉钉。**

## 处理链路

1. `dws event consume user_im_message_receive_at user_im_message_receive_group_all --flatten -f ndjson` 常驻监听。两个 event_key 分别覆盖「被 @ 的消息」与「群聊里自己发的所有消息」。
2. 只处理 `sender_open_dingtalk_id == 本人` 的消息；别人发的（含别人 @ 我）一律忽略。
3. 剥 `@` 前缀 → 挡 `🤖` 自发回复 → `parse_cmd()` 判新建、push、状态和 slash 路由 → `message_id` 去重。
4. **不再由主 agent 派子任务**。`aa ` 直接对一个线程会话投消息：`openclaw gateway call chat.send --params '{"sessionKey":"...","message":"...","idempotencyKey":"..."}'`，1 秒返回 `{"runId","status":"started"}`。线程会话 key = `dthread-<sha1(conv#n)[:12]>`，**由会话与线程号派生，重启后不变**，所以 push 才接得上。
5. `aa<N> 正文` 走同一个 `chat.send`，追加 `"queueMode":"steer"`：agent 正在跑时这条会在下一次模型调用前注入当前 run，未开始的工具调用被跳过并给合成结果（transcript 里能看到 `Skipped due to queued user message.`）。会话空闲时 steer 退化成新开一轮——所以回执会照实说「刚才空闲…按新一轮开始跑了」，不假装 push 成功。
6. 监控：`chat.history` 读 `sessionInfo.hasActiveRun`（权威的「有没有在跑」）与最后一段 assistant 正文；`busy_seen` 或本轮有新正文且 `hasActiveRun=false` 即收尾，终稿回推。`chat.send` 起的 run **不进 `openclaw tasks`**，所以不能用 `tasks show` 判终态。
7. 回复：群聊 `--conversation-id <cid> --at-open-dingtalk-ids <oid> --content "<@oid> 🤖 …"`；单聊 `--open-dingtalk-id`。幂等键由 message_id 派生。
8. 补拉：推流外每 30 秒用 `chat message list-all` 回看最近 120 秒（启动首轮 1800 秒）；仍按 message_id 去重，因此只补漏、不重复派发。
9. 网关自检：同一轮 30 秒周期里顺手 `gateway call health`。失败时清掉 libcurl 的 `Ignoring extra certs` 噪声、连续两次后把子进程 CA 路径刷到最新的 `secret-egress-proxy/gateway-*/trust-bundle.pem` 再试一次；连续三次失败按实际时长记 `gateway: ALERT unreachable`。**不自动重启网关**——更新/重启窗口里插手只会更糟，监控只负责把真实原因写清楚。

### 单条回复长度（实测 2026-10-27）

- 切块在 `selfask_dingtalk.chunk_text()`：按行拼接，**字符数与 UTF-8 字节数双限**——`SELFASK_CHUNK`（默认 18000 字符）、`SELFASK_CHUNK_BYTES`（默认 30000 字节）；单行超限按字符硬切（中文 3 字节、emoji 4 字节都要算）。分多条时每条带 `🤖 (i/n) `。
- 钉钉服务端按**字节**卡：本人身份 `chat message send` 实测 11000 汉字（约 33010 字节）`SUCCESS`；12000 汉字（约 36010 字节）`FAILED`，错误是「转发内容大小超出上限」；18007 汉字（54021 字节）更早失败（139001 `Parameter not found: 1`）。所以中文单条实际约 1 万字封顶，18000 字符只对纯英文/数字成立，30000 字节才是真闸门。
- 只按字符数切会翻车：18000 汉字 = 54KB，SendStatus 必 FAILED，重试 3 次后整条丢。

### 裸 `aa` 的预览排版（2026-10-27 定稿）

纯列表，一条线程一行，不带标题和尾注：

```
• 🟢 aa1 运行中 1m · 这个任务，目前有什么问题
• ⚪ aa2 已完成 20m 前 · 把 pre-delivery 那套跑一遍
• 🔴 aa3 失败 · 先别动 repo-init，只跑 lint
```

- 状态 emoji：🟢 运行中 / ⚪ 已完成 / 🔴 失败（超时、没起来、启动失败都记失败）；重置过的标「已重置」。
- 编号用 `aa<N>`；最近一句取本轮正文首行、去 Markdown、截 46 字。
- 终态时把 `status`（done/timeout/stale/failed/reset）与 `preview` 写进 `threads.json`，
  概览只读缓存；只有「在跑」的线程才调 `chat.history`，线程再多也是毫秒级。
- **钉钉纯文本不认 ``` 代码块，单换行会被压成空格**（2026-10-27 实测发出去就是一整行）。
  所以行间用单换行、靠列表语义分行；`SELFASK_AA_STYLE` 可兜底切换：
  `list`（默认）/ `para`（空行分段，必分行但更高）/ `compact`（全并一行用 ｜ 分隔）。

### 斜杠路由与中断回退

- 所有 `/xxx` 都直接、裸发给 Gateway，避免无人维护命令白名单；不带 `aa<N>` 的 slash 落到本会话最近在用的线程。
- `/new`、`/reset`、`/clear` 是例外：`chat.send` 不能重置非当前 owner 的会话，脚本改走 `sessions.reset`。若该线程在跑，先尝试中断。
- 中断统一走 `stop_run()`：先 `sessions.abort`（CLI 在 loopback + token 下对 abort 类方法申请 `operator.admin`，能跳过 owner 校验，**上下文保留**）；仍被拒才降级 `sessions.reset`（上下文清空）。`aa<N> /stop`、`/abort` 本地拦下走这条路，**不裸发**给网关斜杠层（裸发恒定 `unauthorized`）。

### 为什么不用 `openclaw agent --session-key` 做 push

实测（2026-10-23）：对同一 session-key 再发一条 `openclaw agent --message`，**不会** steer，而是另起一个 run 排在原 run 后面——61 秒才返回，正文落在原 run 结束之后。要插进当前 run，只能走 Gateway 的 `chat.send` + `queueMode:"steer"`。

## 防自激（关键，第一版踩过）

回复正文本身也带 @我，会被 at-me 事件再次捕获，导致「回答自己的回答」链式触发。三道防线：

1. 正文以 `🤖` 开头；判定时必须先剥 @ 前缀再判断 `strip_mention(content).startswith("🤖")`——第一版只判原文开头，因为 `@周后霖 🤖 ...` 开头是 @，漏判，实测触发了一次自激。
2. `message_id` 去重（`seen.json`）。
3. 发送幂等键。

## 坑

- **websocket 走代理必失败**：本机 HTTP_PROXY/HTTPS_PROXY 指向 127.0.0.1:37441，个人 Stream 长连接报 `dial websocket: network error` 无限重连。启动子进程时必须去掉 `HTTP_PROXY/HTTPS_PROXY/ALL_PROXY/NODE_USE_ENV_PROXY/NO_PROXY`（脚本内 `ENV` 已过滤），普通 HTTP 命令（send/list/groups）走代理反而正常。
- **openclaw 不在非交互 PATH 里**：`which openclaw` 指向 `/root/.openclaw/tmp/agent-cli/openclaw`（wrapper），但普通 PATH 里没有。脚本里硬编码该路径 + `shutil.which` 兜底，否则 `FileNotFoundError: openclaw`。
- **watchdog 匹配要宽松**：用 `pgrep -f "python3 (.*/)?selfask\.py"`，否则相对路径启动的实例和绝对路径启动的实例互不识别，会跑出两个守护进程、重复回答。
- 用 `setsid nohup ... < /dev/null` 脱离会话，避免被 exec 会话结束带走。

## 并发模型

- 监听主循环只做解析 + 投递，**永不阻塞**：每条命令丢进独立线程后立刻返回。
- 同时在跑的线程数上限 `SELFASK_MAX_CONCURRENCY`（默认 4）；到上限先回「排着」，空位释放再回「轮到它了」。
- `threads.json` 记录每个会话的线程表：`n` / `key` / `state` / `started` / `pushes` / `last_ok`。线程号按「最小空号」分配，跑完不回收。首次读盘（包括仅调用 `conv_threads()`）会合并到内存；纯 `load_threads()` 只解析、不改变合盘标记，所以其后的 `save_threads()` 也不会把磁盘上正在跑的线程写丢。
- 进程重启：`resume_threads()` 读回 `threads.json`，`hasActiveRun=true` 的接着盯（回「重启过但还在跑」），停机期间跑完的把结果补发。

| 项 | 值 | 环境变量 |
| --- | --- | --- |
| 线程硬上限 | 7200s（2h），到点尝试 `sessions.abort`；非 owner 时 reset 兜底并回已有进展 | `SELFASK_TASK_HARD_LIMIT` |
| 进度回报 | 每 900s（15min）一条 | `SELFASK_PROGRESS_EVERY` |
| 状态轮询 | 20s | `SELFASK_POLL` |
| 并发上限 | 4 | `SELFASK_MAX_CONCURRENCY` |

## 守护

- `watchdog.sh`：不在跑就拉起来；由 OpenClaw 自动化 `dingtalk-selfask-watchdog`（id `0542848b-92e2-4851-8574-8141646ad400`，`--every 5m --command "sh .../watchdog.sh"`，`--no-deliver`）定时调用。
- 监听进程异常退出时内部退避重连（5s 起，翻倍至 60s 上限；稳定跑过 60s 则重置）。
- 重启时旧 listener 会变孤儿（ppid=1）继续占同一订阅，事件被随机分流：kill 守护进程后必须再按 PID 清掉残留的 `dws event +listen-im` / `dws event consume`。脚本内 `kill_stale_listeners()` 每次重连前也做同样的事（只杀 consumer，不动 `dws event _bus`）。

## 换机重装步骤

1. `dws auth login --device` 登录（无头 Linux 走设备流，登录时用 `env -u HTTPS_PROXY -u HTTP_PROXY` 绕代理），确认 `dws profile list` 有 active profile。
2. 复制 `selfask.py` + `watchdog.sh`，把 `SELF_OID`/`SELF_NAME`/`OPENCLAW_BIN`/`DWS_BIN` 改成本机值。
3. `sh watchdog.sh` 启动，`tail -f selfask.log` 看 `boot:` 与 `listen: starting`。
4. 在测试群直接发一句 `aa 只回 OK`（不用 @）验证：日志出现 `Q`→`R send_ok=True`→`I ignore own reply`，群里收到 `🤖 OK` 即通过。
5. 加自动化做守护。

## 边界

- 依赖本机在线；关机即停（watchdog 5 分钟内拉起）。
- 每条命令一次模型调用，有数秒延迟；push 会在下一次模型调用前生效，不打断正在跑的工具。
- 只在本人发出、正文以 `aa ` 开头时触发；别人 @我 不会触发。
- 群聊不 @ 也能触发；**单聊必须 @ 自己一次**（见上文实测结论）。
- 每个线程一条会话（`agent:main:dthread-*`），跨任务保留上下文；线程数随使用增长，需要时可清理 `threads.json` 与对应会话。
- 想停：kill `selfask.py` 并禁用自动化 `dingtalk-selfask-watchdog`。
