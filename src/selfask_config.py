#!/usr/bin/env python3
"""dingtalk-selfask 共享配置：常量、环境、开关。

不 import 任何兄弟模块，谁都可以依赖它，它不依赖谁。
"""
import os
import re
import shutil

def _load_dotenv():
    """读仓库根的 .env（KEY=VALUE，# 注释），只补没设过的键。

    为什么自己做：部署机没有 python-dotenv，也不想为一行读文件加依赖。
    真实环境变量优先级最高——systemd/命令行传进来的值不会被 .env 覆盖。
    """
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    try:
        with open(os.path.join(root, ".env"), encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        return
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


_load_dotenv()


def _local(name):
    """读仓库根下的本地身份文件（gitignore 掉的），读不到返回空串。"""
    try:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, name), encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return ""


# 本人身份（收件人 / 被 @ 的人）**不写死在代码里**，三种来源按优先级取：
#   1) 环境变量 SELFASK_SELF_OID / SELFASK_SELF_NAME
#   2) 仓库根目录的 self_oid.txt / self_name.txt（.gitignore 忽略）
#   3) 空串——此时发不出消息，boot 日志会直接报缺身份
SELF_OID = os.environ.get("SELFASK_SELF_OID") or _local("self_oid.txt")
SELF_NAME = os.environ.get("SELFASK_SELF_NAME") or _local("self_name.txt")
REPLY_MARKER = "🤖"
# 命令全部严格匹配（大小写敏感、`aa` 后必须是空格或数字）
#   `aa <正文>`    → 新建线程会话
#   `aa<N> <正文>` → push 进线程 N（跑着就 steer，下一轮工具调用前带上）
#   `aa<N>`        → 只看线程 N 的现状，不 push
#   `aa0`          → 线程一览 + 每条会话 key（方便复制去 `/fork`）
#   `aa<N> /fork <会话key>` → 新开线程 N，复制指定会话的上下文（源会话不动）
#   `/xxx`         → OpenClaw 斜杠命令，本地拦截裸发进网关
HOME = "/root"
# BASE 钉在仓库根（本文件在 src/ 里，向上一级才是数据/日志/备份所在处）
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG = os.path.join(BASE, "selfask.log")
STATE = os.path.join(BASE, "seen.json")
GROUPS_CACHE = os.path.join(BASE, "groups.json")
THREADS = os.path.join(BASE, "threads.json")
THREAD_PREFIX = "dthread-"

TASK_HARD_LIMIT = int(os.environ.get("SELFASK_TASK_HARD_LIMIT",
                                     os.environ.get("SELFASK_AGENT_TIMEOUT", "7200")))  # 子任务硬上限，默认 2h
PROGRESS_EVERY = int(os.environ.get("SELFASK_PROGRESS_EVERY", "900"))      # 每 15 分钟报一次进度
POLL_INTERVAL = int(os.environ.get("SELFASK_POLL", "20"))                  # 监控轮询间隔（秒）
MAX_CONCURRENCY = int(os.environ.get("SELFASK_MAX_CONCURRENCY", "6"))  # 同时跑几条 agent
CHUNK = int(os.environ.get("SELFASK_CHUNK", "18000"))               # 单条消息最大字符数
# 钉钉服务端按字节卡：实测 33010 字节（11000 汉字）成功、36010 字节（12000 汉字）
# 报「转发内容大小超出上限」；18007 汉字/54021 字节也失败。取 30000 留余量。
CHUNK_BYTES = int(os.environ.get("SELFASK_CHUNK_BYTES", "30000"))    # 单条消息最大 UTF-8 字节数

# 服务端补拉：推流会「静默丢消息」——连接正常、bus 账本 received++/dropped=0，
# 消息就是没到进程。实测 2026-10-23 十小时内本人发出的 20 条 aa 消息丢了 6 条（30%），
# 且丢的那些进程日志里连「收到」都没有，说明丢在 dws 推流那一层，重连救不了。
# 所以必须有第二条通道：固定周期查历史接口，喂给同一个 handle_incoming，靠 message_id 去重。
BACKFILL_EVERY = int(os.environ.get("SELFASK_BACKFILL_EVERY", "15"))  # 2026-10-28 30→15：推流不推「自己发的无@消息」，补拉是唯一通道
BACKFILL_WINDOW = int(os.environ.get("SELFASK_BACKFILL_WINDOW", "120"))
BACKFILL_DELAY = int(os.environ.get("SELFASK_BACKFILL_DELAY", "15"))
# 启动首轮回看更远：进程停机期间漏掉的（以及停机前不久静默丢掉的）都能一次捞回。
BACKFILL_BOOT_WINDOW = int(os.environ.get("SELFASK_BACKFILL_BOOT_WINDOW", "1800"))
# 钉钉接口的 --start/--end 按 UTC+8 解释，本机时区不一定是 UTC+8，故显式换算。
DD_TZ_OFFSET = 8 * 3600

# 无人值守约定：这条链路没有人在线，问题发出去也没人接。
# 2026-10-27 用户定的口径：要我做选择时**用文字问、停下来等回复**，不要 ask_user。
# 这种做法在 daemon 上天然成立：模型停轮 → monitor 把这段文字当终稿发到钉钉 →
# 线程转 idle（会话历史保留）→ 用户 `aaN <答案>` push 就是同一会话的新一轮。
# 顺带绕开 ask_user 的硬限制（网关要求 verified profile，CLI 起的 run 没有）。
# 首条与 push 共用一行：压缩会吃掉早期消息，每次重申一次才稳。
UNATTENDED_NOTE = ("[无人值守] 需要我做选择时，直接回复文字：列出选项+你的建议，"
                   "然后停下等我回复，不要用 ask_user。\n\n")

SPILL_DIR = os.path.join(BASE, "spill")
# `gateway call --params` 是把 JSON 当命令行参数传的，Linux 对单个参数有硬上限
# MAX_ARG_STRLEN=128KB。超了 subprocess 直接抛 OSError: Argument list too long，
# 监控线程当场崩掉、用户一条回复都收不到（实测踩到）。这里留足余量：
# 超长正文先落盘，只把路径发过去，让 agent 自己读文件。
ARG_SAFE_BYTES = 100 * 1024

# websocket 走代理必失败：本机 HTTP_PROXY/HTTPS_PROXY 指向本地代理，个人 Stream
# 长连接会无限重连；普通 HTTP 命令（send/list/groups）走代理反而正常。
#
# 证书路径同样不能继承：secret-egress-proxy/gateway-XXXX 每次网关重启都换目录，
# 常驻进程带着启动那一刻的旧路径，目录轮换后 libcurl 会打印
# "Warning: Ignoring extra certs from ... load failed"，并且把真正的网关错误盖住
# （2026-10-26 就是这条告警把「网关正在重启」伪装成了证书问题）。dws 直连用系统
# CA、本地 gateway 走 loopback，都不需要这个 bundle，一并剥掉。
#
# OPENCLAW_SHELL / OPENCLAW_SUBAGENT_EXEC 是从 agent exec 环境继承的标记，网关会
# 拒绝带这两个标记的 chat.send（would lose inter-session attribution）。看门狗已经
# unset 过，这里再剥一层，任何启动方式都不会把标记传给子进程。
_DROP_ENV = ("HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "NODE_USE_ENV_PROXY",
             "NO_PROXY", "CURL_CA_BUNDLE", "SSL_CERT_FILE", "NODE_EXTRA_CA_CERTS",
             "REQUESTS_CA_BUNDLE", "GIT_SSL_CAINFO",
             "OPENCLAW_SHELL", "OPENCLAW_SUBAGENT_EXEC")
ENV = {k: v for k, v in os.environ.items() if k.upper() not in _DROP_ENV}
ENV["PATH"] = "/root/.openclaw/tmp/agent-cli:/root/.local/bin:/usr/local/bin:/usr/bin:/bin"

OPENCLAW_BIN = next((p for p in (
    "/root/.openclaw/tmp/agent-cli/openclaw",
    shutil.which("openclaw") or "") if os.access(p, os.X_OK)), "openclaw")
DWS_BIN = next((p for p in ("/root/.local/bin/dws",
                            shutil.which("dws") or "") if os.access(p, os.X_OK)), "dws")

# 「问进度」这类话不进模型，本地直接答；否则它会排在长任务后面干等
PING_RE = re.compile(
    r"^[\s，。！？、,.!?~～]*(咋样了?|怎么样了?|怎样了?|好了没|好了吗|完了吗|完事了吗|"
    r"跑完了吗|进度|进行到哪了?|在吗|还在吗|还在不在|多久|还要多久|"
    r"status|progress|ping)[\s，。！？、,.!?~～]*$",
    re.I)

# reset 家族：走 chat.send 会被网关拒（不是这个会话的 owner），必须改用 sessions.reset
RESET_CMDS = ("/new", "/reset", "/clear")

# stop 家族：走 chat.send 会被网关的斜杠层用「发起 chat.send 的那条连接」做
# owner 校验，而本守护每次都是新的 gateway call 短连接 → 恒返回 unauthorized
# （2026-10-29 用户实测）。本地拦下、直接调 sessions.abort 才停得下来。
STOP_CMDS = ("/stop", "/abort")

# `/his`、`/rewind` 默认列多少条自己的消息；0 = 全部。
# 2026-10-29 用户要求默认 8 条（先全列、后改成 8），编号仍按全量顺序给。
HIS_SHOW = 8
