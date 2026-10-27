#!/usr/bin/env python3
"""dingtalk-selfask 的钉钉 I/O 层：发消息、切块、群表、旧监听清理。

只跟 dws CLI 和钉钉接口打交道；不认识 gateway，也不管理线程。
"""
import hashlib
import glob
import json
import os
import subprocess
import threading
import time

from selfask_config import (DWS_BIN, SELF_OID, REPLY_MARKER, CHUNK,
                            CHUNK_BYTES, GROUPS_CACHE)
from selfask_util import log, run, CONN


def _nbytes(s):
    return len(s.encode("utf-8"))


def _fits(cur, line):
    """字符数与 UTF-8 字节数双限：钉钉按字节拒收，纯字符数会漏判。"""
    joined = (cur + "\n" + line) if cur else line
    return len(joined) <= CHUNK and _nbytes(joined) <= CHUNK_BYTES


def _split_long(line):
    """单行就超限时按字符硬切，同时守字节上限（中文 3 字节、emoji 4 字节）。"""
    out, cur, n = [], [], 0
    for ch in line:
        b = _nbytes(ch)
        if cur and (len(cur) >= CHUNK or n + b > CHUNK_BYTES):
            out.append("".join(cur))
            cur, n = [], 0
        cur.append(ch)
        n += b
    if cur:
        out.append("".join(cur))
    return out


def chunk_text(text):
    """超长回答按行切块，避免单条消息被钉钉截断或拒收。"""
    if len(text) <= CHUNK and _nbytes(text) <= CHUNK_BYTES:
        return [text]
    parts, cur = [], ""
    for line in text.split("\n"):
        if len(line) > CHUNK or _nbytes(line) > CHUNK_BYTES:
            if cur:
                parts.append(cur)
                cur = ""
            pieces = _split_long(line)
            parts.extend(pieces[:-1])
            line = pieces[-1] if pieces else ""
            if not line:
                continue
        if not _fits(cur, line):
            if cur:
                parts.append(cur)
            cur = line
        else:
            cur = (cur + "\n" + line) if cur else line
    if cur:
        parts.append(cur)
    return parts


def send_reply(conv_id, mid, text, groups, tag=""):
    """在原会话回话；群聊必须同时给 at 参数和正文占位符，否则 @ 不亮。"""
    chunks = chunk_text(text)
    ok_all = True
    for i, piece in enumerate(chunks):
        prefix = "" if len(chunks) == 1 else "(%d/%d) " % (i + 1, len(chunks))
        body = "%s %s%s" % (REPLY_MARKER, prefix, piece)
        t = tag if len(chunks) == 1 else "%s-%d" % (tag, i)
        key = "dtask-" + hashlib.sha1((mid + t).encode()).hexdigest()[:24]
        # 一律走 conversation-id + @：单聊也用它（实测可用），这样每条回复都带
        # @本人，和群里一致；`--open-dingtalk-id` 只作兜底（个别 cid 报
        # OPEN_CID_INVALID 时退回纯直发，不带 @）。
        # 第一行只放「@我 + 🤖 图标」，正文从第二行起（用户 2026-10-27 要求：
        # @ 和机器人图标都在第一行，内容另起）。
        # 正文以 ``` 开头时走 --markdown：钉钉只有 markdown 模式会把围栏渲染成
        # 代码块（text 模式围栏变字面量）。其它正文仍走 --content 纯文本。
        head = "<@%s> %s\n\n" % (SELF_OID, REPLY_MARKER)
        if (prefix + piece).lstrip().startswith("```"):
            cmd = [DWS_BIN, "chat", "message", "send", "--conversation-id", conv_id,
                   "--at-open-dingtalk-ids", SELF_OID,
                   "--markdown", head + prefix + piece,
                   "--idempotency-key", key]
        else:
            cmd = [DWS_BIN, "chat", "message", "send", "--conversation-id", conv_id,
                   "--at-open-dingtalk-ids", SELF_OID,
                   "--content", head + prefix + piece,
                   "--idempotency-key", key]
        ok, out, err = False, "", ""
        for attempt in range(3):
            rc, out, err = run(cmd, timeout=120)
            blob = (out or "") + (err or "")
            ok = rc == 0 and '"ok": true' in out.lower().replace('"ok":true', '"ok": true')
            # 幂等键撞车说明这条之前已经送达过（重启后补发同一条、重连重放）。
            # 钉钉用 `Request is repeated with uuid` 拒绝，这不是失败——把它当成功，
            # 否则会白白重试三次、日志里全是假失败。
            if not ok and "repeated with uuid" in blob.lower():
                ok = True
                log("send dedup id=%s tag=%s（之前已送达，按成功算）" % (mid, t))
            if not ok and attempt == 0 and "OPEN_CID_INVALID" in blob:
                # 这个会话不认 cid：退回直发（不带 @），至少别把回复弄丢
                log("send fallback direct id=%s tag=%s" % (mid, t))
                cmd = [DWS_BIN, "chat", "message", "send",
                       "--open-dingtalk-id", SELF_OID,
                       "--content", body, "--idempotency-key", key]
            if ok:
                break
            log("send retry %d/3 id=%s tag=%s %s"
                % (attempt + 1, mid, t, (err or out)[-200:]))
            time.sleep(3)
        ok_all = ok_all and ok
        log("R  id=%s tag=%s part=%d/%d send_ok=%s %s"
            % (mid, t or "-", i + 1, len(chunks), ok,
               "" if ok else (err or out)[-300:]))
    return ok_all


def load_groups():
    """刷新群 id 集合；失败时用旧缓存（判断是否群聊用，只影响是否加 @ 占位符）。"""
    rc, out, err = run([DWS_BIN, "chat", "+my-groups", "--page-all",
                        "--page-limit", "100"], timeout=120)
    ids = set()
    if rc == 0:
        try:
            for g in json.loads(out).get("groups", []):
                ids.add(g.get("conversationId"))
        except Exception as e:
            log("groups parse fail: %r" % e)
    if ids:
        with open(GROUPS_CACHE, "w") as f:
            json.dump(sorted(ids), f)
        return ids
    try:
        with open(GROUPS_CACHE) as f:
            return set(json.load(f))
    except Exception:
        return set()


def kill_stale_listeners():
    """同一 subscribe 上多挂一个 consumer，事件会被随机分走一半，谁都没收全。
    起新的之前，把不是本进程子进程的 consumer 清掉（bus 进程 `dws event _bus` 不能动）。

    判定用 argv 而不是整条命令行的子串：整串匹配会误杀「命令行里恰好包含
    `dws event consume` 字样」的无关进程（实际踩过：一条 grep/编辑命令把自己
    SIGTERM 了）。这里要求 argv[0] 就是 dws 可执行文件，且 argv[1:3] 正好是
    `event consume`。
    """
    try:
        pids = subprocess.run(["pgrep", "-f", r"dws event consume"],
                              capture_output=True, text=True, timeout=20).stdout.split()
    except Exception:
        return
    me = os.getpid()
    for raw in pids:
        if not raw.isdigit():
            continue
        pid = int(raw)
        try:
            with open("/proc/%d/cmdline" % pid, "rb") as f:
                argv = [a.decode("utf-8", "replace")
                        for a in f.read().split(b"\0") if a]
        except Exception:
            continue
        # 必须是 dws 直接起的 consumer：argv[0] 以 dws 结尾，且紧跟 event consume
        if len(argv) < 3:
            continue
        if not os.path.basename(argv[0]).startswith("dws"):
            continue
        if argv[1:3] != ["event", "consume"]:
            continue
        if pid == me:
            continue
        try:
            with open("/proc/%d/stat" % pid) as f:
                ppid = int(f.read().rsplit(")", 1)[1].split()[1])
        except Exception:
            continue
        if ppid == me:
            continue
        try:
            os.kill(pid, 15)
            log("killed stale listener pid=%d ppid=%d cmd=%s"
                % (pid, ppid, " ".join(argv[:3])))
        except Exception as e:
            log("kill stale listener %d fail %r" % (pid, e))


# ---------------------------------------------------------------------------
# 长连接健康检查（2026-10-25 用户要求：监控线程也要顺手看一眼连接）
#
# 只认 bus 自己报的账本（`dws event status`），不靠 stderr 行间隔、
# `state=idle`、`reconnect_count` —— 这三个实测都会误判。
# 连续 3 次（约 3 分钟）异常才动手，修完 5 分钟内不重复修，避免抖动。
# ---------------------------------------------------------------------------
_HEALTH_LOCK = threading.Lock()
_HEALTH_LAST = 0.0
_HEALTH_BAD = 0
_HEALTH_REPAIR_AT = 0.0
HEALTH_EVERY = int(os.environ.get("SELFASK_HEALTH_EVERY", "60"))
HEALTH_BAD_LIMIT = int(os.environ.get("SELFASK_HEALTH_BAD_LIMIT", "3"))
HEALTH_COOLDOWN = int(os.environ.get("SELFASK_HEALTH_COOLDOWN", "300"))


def _pid_alive(pid):
    """真活着才算活着。

    `kill(pid, 0)` 对僵尸进程也返回成功：bus 被 SIGKILL 后没人回收时，
    `dws event status` 仍报 holder 活着、state=running，健康检查于是永远
    只重启 consumer、不动 bus，consumer 又连不上已经消失的 socket，
    形成无限重连。这里按 /proc/<pid>/stat 的进程状态把 Z 判成死。
    """
    try:
        pid = int(pid)
        if pid <= 0:
            return False
        try:
            with open("/proc/%d/stat" % pid) as f:
                fields = f.read().split()
            if len(fields) > 2 and fields[2] == "Z":
                return False
        except OSError:
            return False
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _event_status():
    rc, out, err = run([DWS_BIN, "event", "status", "-f", "json"], timeout=12)
    if rc != 0:
        return None, (err or out or "")[-200:]
    try:
        return json.loads(out), None
    except Exception as e:
        return None, "status json fail %r" % e


def _consumer_pids():
    try:
        r = subprocess.run(["pgrep", "-f", r"dws event consume"],
                           capture_output=True, text=True, timeout=5)
        return [int(x) for x in r.stdout.split() if x.isdigit()]
    except Exception:
        return []


def _repair_listener(reason, kill_bus=False):
    global _HEALTH_BAD, _HEALTH_REPAIR_AT
    now = time.time()
    if now - _HEALTH_REPAIR_AT < HEALTH_COOLDOWN:
        return True, "repair cooldown: %s" % reason
    _HEALTH_REPAIR_AT = now
    _HEALTH_BAD = 0
    log("health: %s -> restart listener" % reason)
    # 先让 consumer 退出；listen_loop 会按退避重新拉起。
    for pid in _consumer_pids():
        try:
            os.kill(pid, 15)
        except Exception:
            pass
    time.sleep(2)
    # bus 还活着但源已 degraded（或状态查不到）时，连 bus 一起重启。
    # 只是 consumer 挂了/订阅掉了，就别动 bus，少一次重连。
    if not kill_bus:
        return True, "restarted consumer: %s" % reason
    d, _ = _event_status()
    holder = 0
    if d:
        holder = (((d.get("bus") or {}).get("entry") or {}).get("holder_pid") or 0)
    if holder and _pid_alive(holder):
        try:
            os.kill(holder, 9)
            log("health: killed bus pid=%d" % holder)
        except Exception:
            pass
        time.sleep(1)
    # holder 已经不在（僵尸/退出）时，bus.lock 与 bus.meta 仍是旧 bus 的：
    # 新 consumer 会以为旧 bus 还活着，永远等不到 socket。除 bus.log 外清掉。
    for p in glob.glob("/tmp/dws-event-0/dws-evt-*.sock"):
        try:
            os.remove(p)
        except OSError:
            pass
    workdir = ""
    if d:
        workdir = (((d.get("bus") or {}).get("entry") or {}).get("workdir") or "")
    if workdir and not (holder and _pid_alive(holder)):
        for name in ("bus.lock", "bus.meta", "personal_subscription_attempts.lock",
                     "personal_subscriptions.lock"):
            p = os.path.join(workdir, name)
            try:
                os.remove(p)
            except OSError:
                pass
        log("health: cleared stale bus state in %s" % workdir)
    return True, "restarted: %s" % reason


def check_live_health():
    """查一次 bus 账本，坏了就修。返回 (ok, reason)。

    节流到每 HEALTH_EVERY 秒最多查一次；多个 monitor 线程同时调用也安全。
    """
    global _HEALTH_LAST, _HEALTH_BAD
    now = time.time()
    with _HEALTH_LOCK:
        if now - _HEALTH_LAST < HEALTH_EVERY:
            return True, "throttled"
        _HEALTH_LAST = now
        d, err = _event_status()
        if not d:
            _HEALTH_BAD += 1
            if _HEALTH_BAD >= HEALTH_BAD_LIMIT:
                return _repair_listener("status unavailable: %s" % err,
                                        kill_bus=True)
            return True, "status fail x%d" % _HEALTH_BAD

        entry = ((d.get("bus") or {}).get("entry") or {})
        live = ((d.get("bus") or {}).get("live") or {})
        holder = entry.get("holder_pid") or 0
        state = entry.get("state") or ""
        subs = d.get("subscriptions") or []
        cons = live.get("consumers") or []
        src = live.get("source_state") or {}

        if not holder or state == "not_running" or not _pid_alive(holder):
            _HEALTH_BAD += 1
            if _HEALTH_BAD >= HEALTH_BAD_LIMIT:
                return _repair_listener("bus down holder=%s state=%s"
                                        % (holder, state), kill_bus=True)
            return True, "bus down x%d" % _HEALTH_BAD
        if not cons:
            _HEALTH_BAD += 1
            if _HEALTH_BAD >= HEALTH_BAD_LIMIT:
                # 连续三次都没有 consumer，说明 consumer 连不上（常见是 socket
                # 没了但 bus 账本还挂着）。只重启 consumer 会无限重复，必须连
                # bus 一起重启。
                return _repair_listener("no consumer in bus ledger",
                                        kill_bus=True)
            return True, "no consumer x%d" % _HEALTH_BAD
        for c in cons:
            if not _pid_alive(c.get("pid") or 0):
                _HEALTH_BAD += 1
                if _HEALTH_BAD >= HEALTH_BAD_LIMIT:
                    return _repair_listener("consumer dead pid=%s"
                                            % c.get("pid"))
                return True, "consumer dead x%d" % _HEALTH_BAD
        bad_subs = [s for s in subs if (s.get("status") or "active") != "active"]
        if bad_subs:
            _HEALTH_BAD += 1
            if _HEALTH_BAD >= HEALTH_BAD_LIMIT:
                return _repair_listener("subscription not active: %s"
                                        % [(s.get("event_key"), s.get("status"))
                                           for s in bad_subs])
            return True, "subscription bad x%d" % _HEALTH_BAD

        # `idle` 是 300s 无事件的推断态，正常；只有 degraded 才值得警惕。
        if (src.get("state") or "") == "degraded":
            _HEALTH_BAD += 1
            if _HEALTH_BAD >= HEALTH_BAD_LIMIT:
                return _repair_listener("source degraded x%d (reconnect=%s)"
                                        % (_HEALTH_BAD,
                                           src.get("reconnect_count")),
                                        kill_bus=True)
            return True, "source degraded x%d" % _HEALTH_BAD

        if _HEALTH_BAD:
            log("health: recovered after %d bad checks" % _HEALTH_BAD)
        _HEALTH_BAD = 0
        return True, "ok"
    time.sleep(1)


def _pump_stderr(p):
    """转抄 consume 的 stderr，顺手把连接状态喂给 CONN。

    只认 `[event] transport {json}` 那行：它是结构化的、状态权威；
    `source state: idle` 这行每 60 秒刷一次、且带 attempt 噪声，只当日志。
    """
    for raw in p.stderr:
        line = raw.rstrip()
        log("stderr: " + line)
        if "[event] transport" not in line:
            continue
        head, _, payload = line.partition("{")
        if not payload:
            continue
        try:
            info = json.loads("{" + payload)
        except Exception:
            continue
        state = info.get("state")
        if state:
            CONN.set_state(state, info.get("reconnect_count"))


def spawn_consumer():
    """起一条 `dws event consume` 长连接子进程。

    stdin 必须是「保持打开」而不是 /dev/null：consume 把 stdin EOF
    当退出信号（报 "stdin closed — shutting down"），给 DEVNULL 会
    一启动就退、无休止重连。这里开一个我们永不写入的管道，等价于
    `tail -f /dev/null` 常开。
    """
    import subprocess as sp
    from selfask_config import ENV
    return sp.Popen(
        [DWS_BIN, "event", "consume",
         "user_im_message_receive_at",
         "user_im_message_receive_group_all",
         "--flatten", "-f", "ndjson"],
        env=ENV, stdin=sp.PIPE,
        stdout=sp.PIPE, stderr=sp.PIPE,
        text=True, bufsize=1, start_new_session=True)
