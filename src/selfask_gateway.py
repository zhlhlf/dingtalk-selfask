#!/usr/bin/env python3
"""dingtalk-selfask 的 Gateway 会话层：RPC 调用、会话读写、abort/reset。

只跟 OpenClaw 网关打交道；不认识钉钉，也不管理线程表。
"""
import glob
import hashlib
import json
import os
import time

from selfask_config import (OPENCLAW_BIN, SPILL_DIR, ARG_SAFE_BYTES, ENV)
from selfask_util import log, run


def gw_call(method, params, timeout=45):
    """调一次 Gateway RPC。返回 (rc, stdout, stderr)。"""
    return run([OPENCLAW_BIN, "gateway", "call", method,
                "--params", json.dumps(params, ensure_ascii=False),
                "--json", "--timeout", str(int(timeout * 1000))],
               timeout=timeout + 30)


def _ok_blob(out):
    return '"ok": true' in (out or "").lower().replace('"ok":true', '"ok": true')


_CERT_WARN = "Warning: Ignoring extra certs"


def _clean_err(text):
    """去掉 libcurl 的证书告警，只留下能指向真实故障的行。

    网关重启/证书目录轮换时，stderr 第一行固定是
    `Warning: Ignoring extra certs from ... load failed: ...`。以前直接截
    err[-200:]，这行会把真正的 `gateway unreachable` 顶掉，看起来像证书问题。
    """
    lines = [ln for ln in (text or "").splitlines()
             if not ln.strip().startswith(_CERT_WARN)]
    return "\n".join(lines).strip()


def gw_health(timeout=15):
    """探一次网关健康（`gateway call health`），给周期自检用。

    返回 (ok, reason)。reason 已清掉证书告警噪声。
    """
    rc, out, err = gw_call("health", {}, timeout=timeout)
    if rc == 0 and _ok_blob(out):
        return True, ""
    detail = _clean_err(err) or _clean_err(out) or ("rc=%s" % rc)
    return False, detail[-300:]


_EGRESS_GLOB = "/root/.openclaw/secret-egress-proxy/gateway-*/trust-bundle.pem"
_CA_KEYS = ("CURL_CA_BUNDLE", "SSL_CERT_FILE", "NODE_EXTRA_CA_CERTS",
            "REQUESTS_CA_BUNDLE", "GIT_SSL_CAINFO")


def refresh_egress_env():
    """把子进程环境里的 CA 路径指向当前最新的一份 trust bundle。

    网关重启会重建 secret-egress-proxy/gateway-XXXX，常驻进程启动时的那份会被
    删掉。这里重新 glob 最新目录；返回是否真的改过（日志用）。
    只改 ENV（子进程环境），不动 os.environ，避免影响进程自身。
    """
    try:
        cands = glob.glob(_EGRESS_GLOB)
        if not cands:
            return False
        newest = max(cands, key=os.path.getmtime)
    except OSError:
        return False
    changed = False
    for key in _CA_KEYS:
        if ENV.get(key) != newest:
            ENV[key] = newest
            changed = True
    if changed:
        log("gateway: refreshed CA bundle -> %s" % newest)
    return changed


def _msg_text(m):
    """取一条消息的正文（content 可能是 block 列表，也可能是字符串）。"""
    cont = m.get("content")
    if isinstance(cont, list):
        return "\n".join(b.get("text") or "" for b in cont
                         if isinstance(b, dict) and b.get("type") == "text").strip()
    return (cont or "").strip() if isinstance(cont, str) else ""


def session_state(skey, since_ms=0):
    """读一个线程会话的现状：是否在跑 + 上次助手正文 + 最后消息时间。

    chat.send 起的 run 不进 `openclaw tasks`，所以状态只能从会话上读：
    `sessionInfo.hasActiveRun` 是权威的「有没有在跑」。

    额外返回本轮以来的全部助手正文（texts）与工具调用名（tools），
    这两个是「中途回答」和「进度汇报」的依据，不然只能在收尾时看到终稿。
    """
    rc, out, err = gw_call("chat.history", {"sessionKey": skey, "limit": 40})
    if rc != 0:
        return None, ((_clean_err(err) or _clean_err(out)) or "rc=%s" % rc)[-200:]
    try:
        d = json.loads(out)
    except Exception as e:
        return None, "history json fail %r" % e
    si = d.get("sessionInfo") or {}
    last, last_ts = "", 0
    texts, tools = [], []
    for m in d.get("messages") or []:
        ts = int(m.get("timestamp") or 0)
        if ts > last_ts:
            last_ts = ts
        if (m.get("role") or "") != "assistant":
            continue
        # 每条助手消息都带起它的 runId。只用时间戳划「本轮」会把同会话里
        # 别的 run（比如斜杠命令）的输出算进来，实测 /status 的回显被上一轮
        # 任务的 monitor 当成终稿又发了一遍。
        run = (m.get("__openclaw") or {}).get("runId") or ""
        cont = m.get("content")
        if isinstance(cont, list):
            txt = "\n".join(b.get("text") or "" for b in cont
                            if isinstance(b, dict) and b.get("type") == "text").strip()
            if ts >= since_ms:
                for b in cont:
                    if isinstance(b, dict) and b.get("type") == "toolCall":
                        name = b.get("name") or b.get("toolName") or "?"
                        tools.append({"name": str(name), "run": run})
        else:
            txt = (cont or "").strip()
        if ts < since_ms:
            continue
        if txt and txt != "NO_REPLY":
            texts.append({"ts": ts, "text": txt, "run": run})
            last = txt
    return {"active": bool(si.get("hasActiveRun")),
            "exists": bool(si.get("key")),
            "last": last, "last_ts": last_ts,
            "texts": texts[-8:], "tools": tools[-40:],
            "updated": int(si.get("updatedAt") or 0)}, None


def _spill(message):
    """超长正文写到磁盘，返回替换用的短消息。写不进去就返回 None。"""
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        name = "task-%d-%s.txt" % (int(time.time()),
                                   hashlib.sha1(message.encode()).hexdigest()[:8])
        path = os.path.join(SPILL_DIR, name)
        with open(path, "w") as f:
            f.write(message)
        return ("正文太长（%d 字节），命令行参数装不下，已原样落盘到：\n%s\n"
                "请直接读这个文件，把它当成本次任务的正文来处理。"
                % (len(message.encode("utf-8")), path))
    except OSError as e:
        log("spill fail %r" % e)
        return None


def session_send(skey, message, steer=False, idem=None):
    """往线程会话投一条消息。

    steer=True 用 queueMode=steer：agent 正在跑时，这条会在下一次模型调用前
    注入当前 run（未开始的工具调用会被跳过并给合成结果），这正是「下一轮工具
    调用时一起带上」。会话空闲时 steer 退化成开新轮。
    """
    if len(message.encode("utf-8")) > ARG_SAFE_BYTES:
        short = _spill(message)
        if not short:
            return None, "正文过长且落盘失败（单条上限 %d 字节）" % ARG_SAFE_BYTES
        log("send spill %d bytes -> file" % len(message.encode("utf-8")))
        message = short
    params = {"sessionKey": skey, "message": message,
              "idempotencyKey": idem or ("dthr-" + hashlib.sha1(
                  (skey + "|" + message + "|" + str(time.time())).encode()
              ).hexdigest()[:24])}
    if steer:
        params["queueMode"] = "steer"
    rc, out, err = gw_call("chat.send", params, timeout=30)
    if rc != 0:
        # 网关的业务错误在 stdout 的 JSON 里；stderr 往往只有 Node 的
        # NODE_EXTRA_CA_CERTS 警告。优先把 stdout 的 error.message 报出来。
        detail = (out or "").strip()
        try:
            d = json.loads(detail)
            msg = ((d.get("error") or {}).get("message") or "").strip()
            if msg:
                detail = msg
        except Exception:
            pass
        return None, (detail or err or "")[-300:]
    try:
        d = json.loads(out)
    except Exception as e:
        return None, "chat.send json fail %r" % e
    if d.get("ok") is False or d.get("error"):
        return None, json.dumps(d.get("error"), ensure_ascii=False)[:300]
    return {"run": d.get("runId") or "", "status": d.get("status") or ""}, None


def session_abort(skey):
    """中断一个正在跑的 run，返回 True/False（真停下来了没有）。

    `sessions.abort` / `chat.abort` 都要求「请求者就是这个 run 的 owner」
    （ownerDeviceId / ownerConnId 比对）。我们用 CLI 起 run、CLI 再发 abort，
    两次是不同的连接，owner 对不上 → 恒定返回 unauthorized。2026-10-23 实测
    两个方法都是这个结果，也就是说**过去的超时中断从未真正停下任务**：
    只打了行日志、把线程标成 idle，run 还在后台继续烧。

    兜底走 `sessions.reset`：它没有 owner 限制（实测 rc=0），而且重置会连带
    停掉该会话的活跃 run（实测 3 秒内 hasActiveRun 由 true 变 false）。
    代价是上下文被清空——只在这条路上用，因为任务已经跑到硬上限了。
    """
    rc, out, err = gw_call("sessions.abort", {"key": skey}, timeout=45)
    blob = (out or "") + (err or "")
    if rc == 0 and _ok_blob(out):
        log("abort key=%s via sessions.abort" % skey[-12:])
        return True
    if "unauthorized" not in blob.lower():
        log("abort key=%s rc=%s %s" % (skey[-12:], rc, blob[-160:]))
        return False
    log("abort key=%s 授权被拒（非 owner），改用 reset 停 run" % skey[-12:])
    rc2, out2, err2 = gw_call("sessions.reset", {"key": skey}, timeout=60)
    ok = rc2 == 0 and _ok_blob(out2)
    log("abort key=%s reset rc=%s ok=%s" % (skey[-12:], rc2, ok))
    return ok


def session_reset(skey):
    """直接走 sessions.reset（reset 家族命令用，返回 (ok, err)。"""
    rc, out, err = gw_call("sessions.reset", {"key": skey}, timeout=60)
    ok = rc == 0 and _ok_blob(out)
    if not ok:
        return False, (err or out or "")[:300]
    return True, ""


def question_pending(skey):
    """该会话挂着的待答问题，没有就返回 None。

    `ask_user` 的问题只渲染在会话自己的通道上。dthread 会话是用 `chat.send`
    起的，没有通道，问题就落进虚空、等 900s 超时 → `no_answer`。唯一能拿到
    它的地方是 `question.list`：记录里带 sessionKey/runId/expiresAtMs。
    已过期但还没被回收的记录要滤掉，否则会对着空气转发。
    """
    rc, out, err = gw_call("question.list", {}, timeout=20)
    if rc != 0:
        return None
    try:
        d = json.loads(out)
    except Exception:
        return None
    now_ms = int(time.time() * 1000)
    for q in (d.get("questions") or []):
        if q.get("sessionKey") != skey:
            continue
        if q.get("status") != "pending":
            continue
        if int(q.get("expiresAtMs") or 0) <= now_ms:
            continue
        return q
    return None


def question_resolve(qid, answers, by="dingtalk-selfask"):
    """替人提交答案。answers 形如 {questionId: ["值"]}，返回 (ok, err)。

    走 `question.resolve`，不是往会话里再发一条消息——ask_user 是阻塞的工具
    调用，只有 resolve 能让它带着答案返回，后续工具调用才会继续。
    """
    params = {"id": qid, "answers": {"answers": answers}, "resolvedBy": by}
    rc, out, err = gw_call("question.resolve", params, timeout=30)
    if rc == 0 and '"status": "answered"' in (out or ""):
        return True, ""
    detail = (err or out or "").strip()
    try:
        d = json.loads(out or "")
        msg = ((d.get("error") or {}).get("message") or "").strip()
        if msg:
            detail = msg
    except Exception:
        pass
    return False, detail[-300:]


def _msg_id(m):
    """一条消息的稳定标识，用来区分「这条是新的」。"""
    ow = m.get("__openclaw") or {}
    return ow.get("id") or ("ts:%s" % m.get("timestamp"))


def history_raw(skey, limit=60):
    rc, out, _ = gw_call("chat.history", {"sessionKey": skey, "limit": limit})
    if rc != 0:
        return []
    try:
        return json.loads(out).get("messages") or []
    except Exception:
        return []


def await_reply(skey, seen_ids, timeout=45):
    """等一个斜杠命令的回复。

    两条判定，缺一不可：

    1. **用消息 id 集合做基线**，不用时间戳。`since_ms` 取的是上一条消息的 ts，
       判定又是 `>=`，那条旧回复会被当成新回复返回——实测 `aa1 /stop` 把上一轮
       `/unknown-cmd-xyz` 的回答原样发了出来。
    2. **只认 runId 为空的助手消息**。斜杠命令不走 agent run，所以它的回复不带
       runId；任务的回复一定带。这样即使有任务正在跑，也不会把任务终稿误当命令
       回显。
    """
    end = time.time() + timeout
    while time.time() < end:
        time.sleep(1.5)
        for m in reversed(history_raw(skey)):
            if (m.get("role") or "") != "assistant":
                continue
            if _msg_id(m) in seen_ids:
                continue
            if (m.get("__openclaw") or {}).get("runId"):
                continue          # 这是某个任务 run 的输出，不是命令回显
            return _msg_text(m)
    return ""
