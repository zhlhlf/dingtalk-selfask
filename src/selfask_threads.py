#!/usr/bin/env python3
"""dingtalk-selfask 的线程编排层：aa 线程生命周期、push/slash/reset、监控、状态文案。

一个 `aa` 线程 = 一个持久的 gateway 会话。本模块负责：
建线程、投消息、盯到终态、push/steer、斜杠命令裸发与回显收取、
reset、重启续盯、本地状态文案（不进模型）。
"""
import os
import re
import threading
import time

from selfask_config import (TASK_HARD_LIMIT, PROGRESS_EVERY, POLL_INTERVAL,
                            MAX_CONCURRENCY, UNATTENDED_NOTE, RESET_CMDS,
                            STOP_CMDS, HIS_SHOW)
from selfask_util import log, human_dur, _hhmm
from selfask_state import (thread_get, thread_put, thread_ensure, thread_alloc,
                           conv_threads, thread_key, threads_snapshot,
                           his_put, his_get, his_prune)
from selfask_gateway import (session_state, session_send, session_abort,
                             session_reset, stop_run, history_raw, await_reply,
                             question_pending, question_resolve,
                             session_fork, history_count, fork_at_message,
                             rewind_at_message, _msg_text)
from selfask_dingtalk import send_reply as _send_reply, check_live_health


def t_reply(n, conv_id, mid, text, groups, tag=""):
    """所有线程回执的唯一出口：首行必须是「线程会话N」。

    多线程并行时，正文里说「已停下」「跑完了」根本分不清是谁，用户
    2026-10-29 要求每条回执都把线程号摆在第一行。已经以「线程会话N」
    开头的文案不再加抬头，免得同一句话出现两遍。
    """
    head = "线程会话%d" % n
    body = text or ""
    # 用「线程会话N 后面不是数字」判定，否则线程 1 会把「线程会话10 …」认成自己的抬头。
    if not re.match(r"线程会话%d(?!\d)" % n, body.lstrip()):
        body = head + "\n\n" + body
    return _send_reply(conv_id, mid, body, groups, tag)

_slots = threading.Semaphore(MAX_CONCURRENCY)   # 同时在跑的线程数上限


def is_slash(text):
    """是不是 OpenClaw 的斜杠命令。"""
    return (text or "").lstrip().startswith("/")


def latest_thread(conv):
    """最近在用的线程号：优先在跑的，其次按最近活动时间。

    裸斜杠命令（不带 `aaN`）要落到某个会话上执行，落「你最近在用的那个」
    比落「随便新建一个」符合直觉。
    """
    row = conv_threads(conv)
    if not row:
        return None
    for n, t in row.items():
        if t.get("state") == "running":
            return n

    def stamp(t):
        return max(t.get("last_push") or 0, t.get("last_ok") or 0,
                   t.get("started") or 0, t.get("created") or 0)
    return max(row, key=lambda n: stamp(row[n]))


def start_thread(conv_id, n, question, mid, groups):
    """`aa ` 建线程后启动：抢并发额度 → 投消息 → 盯到终态。"""
    threading.Thread(target=_run_thread,
                     args=(conv_id, n, question, mid, groups),
                     daemon=True).start()


def _run_thread(conv_id, n, question, mid, groups):
    rec = thread_get(conv_id, n)
    skey = rec.get("key") or thread_key(conv_id, n)
    got = _slots.acquire(blocking=False)
    if not got:
        t_reply(n, conv_id, mid,
                   "线程会话%d 排着：同时在跑的线程已到上限 %d 条，空出来就开跑。"
                   % (n, MAX_CONCURRENCY), groups, "-queued")
        _slots.acquire()
        t_reply(n, conv_id, mid, "线程会话%d 轮到它了，开始跑。" % n, groups, "-turn")

    started = time.time()
    thread_put(conv_id, n, state="running", started=started, last_msg=question[:200],
               pushes=0, deliver=True, last_mid=mid, last_ok=None)
    try:
        before, _ = session_state(skey)
        base_ts = int((before or {}).get("last_ts") or 0)
        thread_put(conv_id, n, base_ts=base_ts)

        info, err = session_send(skey, UNATTENDED_NOTE + question)
        if not info:
            log("thread send fail n=%d %s" % (n, err))
            t_reply(n, conv_id, mid, "线程会话%d 启动失败：%s" % (n, (err or "")[:300]),
                       groups, "-fail")
            thread_put(conv_id, n, state="idle", deliver=False,
                       last_ok=time.time(), status="failed")
            return
        log("thread start conv=%s n=%d run=%s" % (conv_id, n, (info.get("run") or "")[:12]))
        thread_put(conv_id, n, run=info.get("run") or "")
        monitor_thread(conv_id, n, groups, skey, base_ts, started,
                       run_id=info.get("run") or "")
    finally:
        try:
            _slots.release()
        except RuntimeError:
            pass


def monitor_thread(conv_id, n, groups, skey, base_ts, started, mid=None,
                   run_id=""):
    """盯一个线程会话直到这一轮结束，把终稿发回钉钉。

    run_id 用来锁定「这一轮」到底是谁的输出。只按时间戳划范围会把同会话里
    别的 run 算进来——实测发 `aa1 /status` 时，上一个任务的 monitor 把
    /status 的回显当成了自己的终稿，又发了一遍。
    """
    rec = thread_get(conv_id, n)
    mid = mid or rec.get("last_mid") or ("thread-%d" % n)
    run_id = run_id or rec.get("run") or ""
    busy_seen = False
    last_report = time.time()
    while True:
        time.sleep(POLL_INTERVAL)
        # 顺手看一眼长连接；内部节流到 60s 一次，多个 monitor 并发也安全。
        try:
            check_live_health()
        except Exception as e:
            log("health: monitor check fail %r" % e)
        now = time.time()
        elapsed = now - started
        # 线程被 `aaN /new` 重置或手动标成 idle：这一轮已经作废，
        # 不能再拿重置后的空历史去报「跑完了但没正文」。
        if thread_get(conv_id, n).get("state") != "running":
            log("thread monitor stop n=%d (state changed)" % n)
            return "abandoned"
        # ask_user 的问题没有通道可送（dthread 会话不是聊天会话），只能
        # 主动从网关捞出来转发；每个问题只发一次，别每 20s 刷屏。
        q = question_pending(skey)
        if q and thread_get(conv_id, n).get("q_forwarded") != q.get("id"):
            log("question forwarded n=%d id=%s" % (n, q.get("id")))
            t_reply(n, conv_id, mid, question_prompt(n, q), groups, "-question")
            thread_put(conv_id, n, q_forwarded=q.get("id"),
                       q_deadline=q.get("expiresAtMs") or 0)
        if q:
            # 卡在问题上不算「没起来」，也别让心跳插进来刷屏
            last_report = now

        st, err = session_state(skey, since_ms=base_ts)
        # 只认本轮 run 的正文；没有 run_id 就退回全部（兼容历史记录）
        texts = list((st or {}).get("texts") or [])
        mine = [t for t in texts if not run_id or (t.get("run") or "") == run_id]

        if st and st["active"]:
            busy_seen = True

        if elapsed > TASK_HARD_LIMIT:
            session_abort(skey)
            time.sleep(3)
            st2, _ = session_state(skey, since_ms=base_ts)
            t2 = [t for t in ((st2 or {}).get("texts") or [])
                  if not run_id or (t.get("run") or "") == run_id]
            tail = (t2[-1]["text"] if t2 else "") or ""
            msg = ("线程会话%d 跑到 %s 上限，我已中断（不是卡死）。"
                   % (n, human_dur(TASK_HARD_LIMIT)))
            if tail:
                msg += "\n\n中断时已有的进展：\n" + tail[:1200]
            t_reply(n, conv_id, mid, msg, groups, "-timeout")
            thread_put(conv_id, n, state="idle", deliver=False, last_ok=now,
                       status="timeout", preview=_preview_line(tail))
            return "timeout"

        # 结束判定不能只靠 busy_seen：`chat.send` 返回后 run 可能已经跑完
        # （实测 20 秒内收尾的小任务，第一次轮询 hasActiveRun 已是 false），
        # 那样会白等 180s 再报「没起来」。所以只要有本轮的新助手正文，
        # 或者之前进过 running、现在不 active，都算这一轮结束。
        fresh_text = bool(mine)
        if st and not st["active"] and (busy_seen or fresh_text):
            text = mine[-1]["text"] if mine else ""
            spent = human_dur(now - started)
            if text:
                log("thread done n=%d in %s" % (n, spent))
                t_reply(n, conv_id, mid, text, groups, "-done")
            else:
                log("thread done n=%d in %s (no text)" % (n, spent))
                t_reply(n, conv_id, mid,
                           "线程会话%d 跑完了（%s），但没拿到正文输出。"
                           % (n, spent), groups, "-done-empty")
            thread_put(conv_id, n, state="idle", deliver=False, last_ok=now,
                       status="done", preview=_preview_line(text))
            return "done"

        if not q and now - last_report >= PROGRESS_EVERY:
            last_report = now
            bits = ["线程会话%d 还在跑，没停（已 %s）。" % (n, human_dur(elapsed))]
            tail = (mine[-1]["text"] if mine else "") or ""
            if tail:
                bits.append("最近输出：" + tail[:300])
            bits.append("要补话就发 `aa%d <正文>`，下一轮工具调用前带上。" % n)
            t_reply(n, conv_id, mid, "\n".join(bits), groups, "-progress")

        # 一直没进入 busy 状态也没有新正文：多半是没起来，别无限等
        if not busy_seen and not fresh_text and elapsed > 180:
            t_reply(n, conv_id, mid,
                       "线程会话%d 3 分钟了还没进执行状态，可能没起来。"
                       "发 `aa%d` 看现状，或 `aa ` 重建一条。" % (n, n),
                       groups, "-stale")
            thread_put(conv_id, n, state="idle", deliver=False, last_ok=now,
                       status="stale")
            return "stale"


def _deliver_slash_later(conv_id, n, skey, cmd, mid, groups, seen_ids):
    """线程正忙时提交的斜杠命令：等它的回复出来再发给钉钉。

    这条分支以前是直接 return 的，回执说「已提交」，但没有人去收结果：
    实测会话里 `/status` 的回复确实生成了，钉钉上却永远收不到。
    """
    out = await_reply(skey, seen_ids, timeout=240)
    if not out:
        t_reply(n, conv_id, mid,
                   "线程会话%d `%s` 执行了，但没拿到回显。" % (n, cmd),
                   groups, "-slash-empty")
        return
    t_reply(n, conv_id, mid, out, groups, "-slash")


def slash_thread(conv_id, n, skey, raw, mid, groups):
    """`aaN /xxx`：任何斜杠命令都本地拦截，裸发进网关，让它自己的斜杠层处理。

    为什么不维护命令白名单：OpenClaw 自带几十条斜杠命令，还在增加，逐个枚举
    永远追不上。这里只按「是不是 / 开头」分流，命令本身交给网关认。

    为什么必须裸发：投普通任务时正文前面拼了 UNATTENDED_NOTE（首条与 push 同一行），而斜杠命令只有
    位于整条消息开头才被识别，加了前缀就退化成普通对话内容（`aa1 /new` 连发
    三次都没重置，就是这么来的）。

    例外有两族（都有实测依据，不是猜的）：
    - reset 家族：`/new` 走 chat.send 会被网关拒（不是这个会话的 owner），
      必须改用 sessions.reset；
    - stop 家族：`/stop` 交给网关的斜杠层同样会 `unauthorized`（斜杠层拿
      发起 chat.send 的那条连接当 owner，我们是每次新短连接）。本地直接调
      `sessions.abort` 才停得下来，详见 `stop_thread`。
    """
    parts = raw.strip().split(None, 1)
    cmd = parts[0].lower()
    # `/fork` 是本地命令：不交给网关的斜杠层（网关没有这条），而是把指定会话
    # 的上下文复制进线程 N，源会话只读不写。
    if cmd == "/fork":
        fork_thread(conv_id, n, skey, parts[1] if len(parts) > 1 else "",
                    mid, groups)
        return
    if cmd == "/his":
        history_index_thread(conv_id, n, skey, mid, groups)
        return
    # `/rewind` 也必须是本地的：网关的斜杠层同样拿发起 chat.send 的连接当
    # owner，语义也对不上（我们要的是「剪到第 N 条之前」，不是它自己的回退）。
    if cmd == "/rewind":
        rewind_thread(conv_id, n, skey,
                      parts[1] if len(parts) > 1 else "", mid, groups)
        return
    if cmd in STOP_CMDS:
        stop_thread(conv_id, n, skey, mid, groups)
        return
    if cmd in RESET_CMDS:
        reset_thread(conv_id, n, skey, mid, groups)
        return

    st, _ = session_state(skey)
    busy = bool(st and st["active"])
    seen_ids = {_gateway_msg_id(m) for m in history_raw(skey)}

    # 不 steer：斜杠命令是在消息入口被解析的，塞进运行中的 run 里不保证生效
    info, err = session_send(skey, raw)
    if not info:
        t_reply(n, conv_id, mid,
                   "线程会话%d 执行 `%s` 失败：%s" % (n, cmd, (err or "")[:250]),
                   groups, "-slash-fail")
        return
    log("thread slash n=%d cmd=%s run=%s" % (n, cmd, (info.get("run") or "")[:12]))

    if busy:
        t_reply(n, conv_id, mid,
                   "线程会话%d 正跑着，`%s` 已提交，等这一轮结束就执行。"
                   % (n, cmd), groups, "-slash-queued")
        # 不能就此 return：命令的回复确实会产生，得有人去收。
        # 用 id 基线 + runId 为空 两条判定，既不会串台也不会漏。
        threading.Thread(target=_deliver_slash_later,
                         args=(conv_id, n, skey, cmd, mid, groups, seen_ids),
                         daemon=True).start()
        return
    out = await_reply(skey, seen_ids)
    if not out:
        t_reply(n, conv_id, mid,
                   "线程会话%d 已执行 `%s`，没有输出（这条命令本身不返回内容）。"
                   % (n, cmd),
                   groups, "-slash-empty")
        return
    t_reply(n, conv_id, mid, out, groups, "-slash")


def _short_skey(skey):
    """显示/比较用的短 key：`agent:main:dthread-x` → `dthread-x`。"""
    value = (skey or "").strip().strip("`'\"")
    return value.split(":")[-1] if ":" in value else value


def _same_skey(a, b):
    return bool(a) and bool(b) and _short_skey(a) == _short_skey(b)


def _his_text(m):
    """把一条历史消息压成一行可读文本（剥掉无人值守前缀）。"""
    text = _msg_text(m) or ""
    if text.startswith(UNATTENDED_NOTE.strip()):
        text = text[len(UNATTENDED_NOTE.strip()):].lstrip()
    text = " ".join(text.split())
    return text


def _strip_note(text):
    """剥掉无人值守前缀，保留正文换行（fork 回显原文时用）。

    `/his` 自己走 `_his_text` 已经把前缀去掉了，但 `/fork` 回显的是网关
    返回的 `editorText`，是**原始**消息，还带着 `[无人值守] …` 那段。用户
    2026-10-29 实测看到两边不一致，所以这里统一剥一遍。
    """
    body = (text or "").strip()
    note = UNATTENDED_NOTE.strip()
    if body.startswith(note):
        return body[len(note):].lstrip()
    if body.startswith("[无人值守]"):
        # 前缀措辞改过时退化处理：丢掉开头的提示段（到第一个空行）
        parts = body.split("\n\n", 1)
        if len(parts) == 2 and parts[1].strip():
            return parts[1].lstrip()
    return body


def history_index_thread(conv_id, n, skey, mid, groups):
    """`aaN /his`：报出线程 N 的会话 key + 我自己发过的消息（带编号）。

    只列 role=user 的消息，因为 `/fork <key> <编号>` 的切点必须是用户消息
    （网关 `sessions.fork` 也只接受 user entry）。编号在生成时就把 entry id
    钉进 `his_index.json`：之后会话又多了几条消息，按当前历史重新数会指错。
    """
    msgs = history_raw(skey, limit=500)
    if not msgs:
        t_reply(n, conv_id, mid,
                   "线程会话%d 没读到历史（`%s`）。gateway 忙或 key 不对，"
                   "稍后再发一次。" % (n, skey), groups, "-his-empty")
        return
    items = []
    for m in msgs:
        if (m.get("role") or "") != "user":
            continue
        text = _his_text(m)
        if not text:
            continue
        items.append({"id": _gateway_msg_id(m), "text": text[:200],
                      "at": int(m.get("timestamp") or 0)})
    if not items:
        t_reply(n, conv_id, mid,
                   "线程会话%d 还没有你自己发的消息（`%s`）。"
                   % (n, skey), groups, "-his-none")
        return
    total = len(items)
    # HIS_SHOW<=0 表示全列：用户 2026-10-29 明确要求「列表默认展示全部」。
    shown = items if HIS_SHOW <= 0 else items[-HIS_SHOW:]
    numbered = []
    for i, item in enumerate(shown):
        num = total - len(shown) + i + 1
        item["n"] = num
        numbered.append((num, item))
    his_put(skey, [{"n": it["n"], "id": it["id"], "text": it["text"], "at": it["at"]}
                   for _, it in numbered])
    rows = []
    for num, item in numbered:
        stamp = _hhmm(item["at"] / 1000.0) if item["at"] else ""
        # 正文里万一带围栏，先废掉，免得把外层 text 块截断
        body = item["text"][:80].replace("```", "'''")
        rows.append("%d. %s%s" % (num, ("（%s）" % stamp) if stamp else "", body))
    # 切点插在两条之间：**保留**上面那条，撤掉下面那条起（用户 2026-10-29
    # 纠正：选中的编号那条也要保留）。默认钉在最后两条中间，即「撤掉最新
    # 一条、保留它之前的」。只有 1 条时没有可切的位置，不插标记。
    keep = numbered[-2][0] if len(numbered) >= 2 else None
    if keep is not None:
        cut_mark = "   ⟲ 回退到这儿（保留第 %d 条，撤掉第 %d 条起）" % (keep, keep + 1)
        rows.insert(len(rows) - 1, cut_mark)
    # 列表整体用 text 围栏包住（用户 2026-10-29 要求）：等宽、整块可复制，
    # 也不会让钉钉把「1.」当成 markdown 有序列表吃掉缩进。
    lines = ["线程会话%d · `%s`" % (n, skey), "", "```text"]
    lines.extend(rows)
    lines.extend(["```", ""])
    if total > len(shown):
        lines.append("只列最近 %d 条，共 %d 条。" % (len(shown), total))
    # 回退、分叉各自一块 text 围栏：钉钉只有 markdown 模式会渲染围栏，
    # send_reply 见到围栏就切 --markdown，用户点一下就能整行复制。
    # 两块必须分开（用户 2026-10-29 要求），复制时不会连成一行。
    if keep is not None:
        lines.append("回退到这儿（保留第 %d 条，整行复制）：" % keep)
        lines.append("```text")
        lines.append("aa%d /rewind %d" % (n, keep))
        lines.append("```")
        lines.append("")
    lines.append("从这儿分叉（保留第 %d 条，整行复制）："
                 % (keep if keep is not None else numbered[-1][0]))
    lines.append("```text")
    if keep is not None:
        lines.append("aa%d /fork %s %d" % (n, skey, keep))
    else:
        # 只有一条消息：带编号等于整段复制，直接给不带编号的写法
        lines.append("aa%d /fork %s" % (n, skey))
    lines.append("```")
    t_reply(n, conv_id, mid, "\n".join(lines), groups, "-his")


def fork_thread(conv_id, n, skey, arg, mid, groups):
    """`aaN /fork <会话key> [编号]`：把源会话的上下文装进线程 N。

    两种用法：
    - `aaN /fork <key>`：整段上下文（`sessions.create` 显式指定线程 key，
      所以线程 key 不变）；
    - `aaN /fork <key> <编号>`：从该会话第 N 条**用户消息之前**切一份
      （`sessions.fork`，编号来自 `aaM /his`）。这条由网关生成本地新 key，
      所以线程 N 会**改指到新 key**，旧 transcript 留在原 key 上不动。

    约束（都是有意的）：
    - **目标线程强制重建**：网关的 `sessions.create` 对已存在的 key 会替换
      transcript。用户 2026-10-29 要求「直接强制新建再 fork」，所以不再因为
      目标有上下文而拒绝；但回执必须写明「原有上下文已被替换」。
    - 目标线程正在跑时仍然拒绝：换 transcript 会让 monitor 的 runId/历史对不上。
    - **源会话只读**。整段 fork 只把它当 parent；按消息 fork 只读它的前缀，
      源会话既不 reset 也不 push。
    """
    toks = (arg or "").replace("`", "").replace("'", "").replace('"', "").split()
    source = toks[0] if toks else ""
    index = toks[1] if len(toks) > 1 else ""
    if index and not index.isdigit():
        t_reply(n, conv_id, mid,
                   "编号要写数字：`aa%d /fork <会话key> <编号>`。"
                   "先发 `aa%d /his` 拿编号。" % (n, n), groups, "-fork-usage")
        return
    if index:
        fork_at_index_thread(conv_id, n, skey, source, int(index), mid, groups)
        return
    if not source:
        t_reply(n, conv_id, mid,
                   "用法：`aa%d /fork <会话key>` 整段复制；"
                   "`aa%d /fork <会话key> <编号>` 从某条消息之前分叉"
                   "（编号先发 `/his` 拿）。发 `aa0` 能看到各线程的 key。" % (n, n),
                   groups, "-fork-usage")
        return
    if _same_skey(source, skey):
        t_reply(n, conv_id, mid,
                   "线程会话%d 的 key 就是 `%s`，整段复制到自己身上没有意义。"
                   "想回到以前某一条，用 `aa%d /fork %s <编号>`。"
                   % (n, _short_skey(skey), n, _short_skey(skey)),
                   groups, "-fork-same")
        return
    rec = thread_get(conv_id, n)
    if rec.get("state") == "running":
        t_reply(n, conv_id, mid,
                   "线程会话%d 正在跑，现在不能 fork（换 transcript 会把这一轮"
                   "的上下文弄乱）。等它结束，或先 `aa%d /new`。" % (n, n),
                   groups, "-fork-busy")
        return

    count, err = history_count(skey, limit=1)
    if err or count is None:
        t_reply(n, conv_id, mid,
                   "线程会话%d 查不到状态（%s），这次没 fork。"
                   % (n, (err or "")[:150]), groups, "-fork-fail")
        return
    replaced = bool(count)

    scount, serr = history_count(source, limit=1)
    if serr or scount is None:
        t_reply(n, conv_id, mid,
                   "源会话 `%s` 查不到（%s）。检查 key 是否抄对；`aa0` 里的 key "
                   "可以直接复制。" % (source, (serr or "")[:120]),
                   groups, "-fork-srcfail")
        return
    if not scount:
        t_reply(n, conv_id, mid,
                   "源会话 `%s` 没有历史消息（key 不对，或者它本来就是空的）。"
                   "发 `aa0` 核对一下 key。" % source, groups, "-fork-empty")
        return

    info, ferr = session_fork(source, skey)
    if not info:
        t_reply(n, conv_id, mid,
                   "线程会话%d fork 失败：%s" % (n, (ferr or "")[:250]),
                   groups, "-fork-fail")
        return
    thread_put(conv_id, n, state="idle", started=None, base_ts=0, status="fork",
               pushes=0, deliver=False, last_ok=time.time(), run="",
               last_msg="fork: " + _short_skey(source),
               preview="fork: " + _short_skey(source),
               forked_from=source, q_forwarded=None, q_deadline=0)
    log("thread fork n=%d src=%s session=%s"
        % (n, _short_skey(source), (info.get("sessionId") or "")[:12]))
    t_reply(n, conv_id, mid,
               "线程会话%d 已%s，已复制 `%s` 的上下文；源会话没动。\n"
               "接着发 `aa%d <正文>` 就在这份上下文上继续。"
               % (n, "强制新开（原有上下文已替换）" if replaced else "新开",
                  _short_skey(source), n), groups, "-fork")


def rewind_thread(conv_id, n, skey, arg, mid, groups):
    """`aaN /rewind <编号>`：把线程 N 的会话回退到第 N 条用户消息之前。

    与 `/fork` 的分工很关键：
    - `/fork <key> <n>` 是**另开一份**（新 key，原会话不动），用来试别的走法；
    - `/rewind <n>` 是**就地剪回去**（key 不变），被撤掉的那条原文由网关从
      editorText 回给我们，改完直接发就能重来。

    编号来自线程自己的 `aaN /his`（entry id 已在生成时钉死）。回退后 >= 编号
    的 entry 都不在活跃分支上了，所以顺手把那些编号从表里删掉，避免下次
    `/fork <key> <旧编号>` 撞 `off-active-path`。

    注意：rewind 只动上下文，**撤不掉工具副作用**（已经改的文件、跑掉的命令
    都还在），回执里要说清。
    """
    arg = (arg or "").strip().replace("`", "").replace("'", "").replace('"', "")
    if arg and not arg.isdigit():
        t_reply(n, conv_id, mid,
                   "编号要写数字：`aa%d /rewind <编号>`。先发 `aa%d /his` 拿编号。"
                   % (n, n), groups, "-rewind-usage")
        return
    rec = thread_get(conv_id, n)
    st, _ = session_state(skey)
    if rec.get("state") == "running" or (st or {}).get("active"):
        t_reply(n, conv_id, mid,
                   "线程会话%d 正在跑，现在不能回退（网关也会拒：Rewind is "
                   "unavailable while the agent is working）。先 `aa%d /stop`。"
                   % (n, n), groups, "-rewind-busy")
        return
    if not arg:
        t_reply(n, conv_id, mid,
                   "用法：`aa%d /rewind <编号>`，把这条及之后的都从上下文里撤掉。"
                   "先发 `aa%d /his`，列表里那个「⟲ 回退到这儿」就是默认编号。"
                   % (n, n), groups, "-rewind-usage")
        return
    index = int(arg)
    item = his_get(skey, index)
    if not item:
        t_reply(n, conv_id, mid,
                   "没有编号 %d 的记录：先发 `aa%d /his` 生成编号表"
                   "（编号只在生成时有效，之后新消息不会改动它）。"
                   % (index, n), groups, "-rewind-noidx")
        return
    # 语义：**保留**选中的那条，切在它之后（用户 2026-10-29 纠正：选中编号那条
    # 不能一起撤掉）。网关只接受在 user 消息「之前」剪，所以真正下刀的是下一条。
    nxt = his_get(skey, index + 1)
    if not nxt:
        t_reply(n, conv_id, mid,
                   "第 %d 条已经是最后一条，后面没有可撤的消息。"
                   "想连它一起重来，用 `aa%d /rewind %d`（保留前一条、撤掉它）。"
                   % (index, n, index - 1 if index > 1 else 1), groups,
                   "-rewind-last")
        return
    info, err = rewind_at_message(item.get("key") or skey, nxt["id"])
    if not info:
        t_reply(n, conv_id, mid,
                   "线程会话%d 回退失败：%s" % (n, (err or "")[:250]),
                   groups, "-rewind-fail")
        return
    dropped = his_prune(skey, index + 1)
    thread_put(conv_id, n, state="idle", started=None, base_ts=0,
               status="rewind", pushes=0, deliver=False, last_ok=time.time(),
               run="", last_msg="rewind@%d" % index, preview="rewind@%d" % index,
               q_forwarded=None, q_deadline=0)
    log("thread rewind n=%d idx=%d dropped=%d key=%s"
        % (n, index, dropped, _short_skey(skey)))
    cut = _strip_note(info.get("text") or nxt.get("text") or "")
    lines = ["线程会话%d 已回退到第 %d 条之后：保留 #1–#%d，撤掉 #%d 起（`%s`）。"
             % (n, index, index, index + 1, _short_skey(info.get("key") or skey)),
             ""]
    if cut:
        lines.append("撤掉的第一条原文：")
        lines.append(cut[:400])
        lines.append("")
    lines.append("改完直接发 `aa%d <正文>`，不想改就继续。" % n)
    lines.append("提醒：只回退了上下文，**之前跑过的命令、改过的文件还在**；"
                 "编号 >= %d 的已作废，要重新分叉先 `/his`。" % (index + 1))
    t_reply(n, conv_id, mid, "\n".join(lines), groups, "-rewind")


def fork_at_index_thread(conv_id, n, skey, source, index, mid, groups):
    """`aaN /fork <key> <编号>`：从某条用户消息之前分叉，线程 N 改指新会话。

    走网关的 `sessions.fork`：它只接受活跃分支上的 user 消息，子会话 key 由
    网关生成，所以线程 N 必须**换 key**（旧 key 的 transcript 原样保留）。
    编号来自 `aaM /his` 的落盘表，不按当前历史现数。
    """
    rec = thread_get(conv_id, n)
    st, _ = session_state(skey)
    if rec.get("state") == "running" or (st or {}).get("active"):
        t_reply(n, conv_id, mid,
                   "线程会话%d 正在跑，现在不能 fork（换会话会把这一轮弄乱）。"
                   "等它结束，或先 `aa%d /stop`。" % (n, n), groups, "-fork-busy")
        return
    item = his_get(source, index)
    if not item:
        t_reply(n, conv_id, mid,
                   "没有编号 %d 的记录：先在源会话发 `aaM /his` 看编号，"
                   "编号会记在源会话 key 上（`%s`）。" % (index, _short_skey(source)),
                   groups, "-fork-noidx")
        return
    # 语义：**保留**选中的那条，切在它之后（与 /rewind 对齐，用户 2026-10-29
    # 纠正「选择的编号那条也要保留」）。网关只能在 user 消息之前剪，所以真正
    # 下刀的是下一条 user 消息。
    nxt = his_get(source, index + 1)
    if not nxt:
        t_reply(n, conv_id, mid,
                   "第 %d 条已经是最后一条，分叉等于整段复制。"
                   "用 `aa%d /fork %s`（不带编号）即可。"
                   % (index, n, _short_skey(source)), groups, "-fork-last")
        return
    # 编号表里记着完整 key：用户只抄短 key 也能分叉
    source = item.get("key") or source
    info, err = fork_at_message(source, nxt["id"])
    if not info:
        t_reply(n, conv_id, mid,
                   "线程会话%d 分叉失败：%s" % (n, (err or "")[:250]),
                   groups, "-fork-fail")
        return
    old_key = skey
    thread_put(conv_id, n, key=info["key"], state="idle", started=None,
               base_ts=0, status="fork-msg", pushes=0, deliver=False,
               last_ok=time.time(), run="",
               last_msg="fork@%d: %s" % (index, _short_skey(source)),
               preview="fork@%d: %s" % (index, _short_skey(source)),
               forked_from=source, q_forwarded=None, q_deadline=0)
    log("thread fork@msg n=%d src=%s idx=%d -> %s"
        % (n, _short_skey(source), index, _short_skey(info["key"])))
    lines = ["线程会话%d 已从 `%s` 的第 %d 条之后分叉：保留 #1–#%d，"
             "线程现在指向 `%s`。" % (n, _short_skey(source), index, index,
                                      _short_skey(info["key"])), ""]
    cut = _strip_note(info.get("text") or nxt.get("text") or "")
    if cut:
        lines.append("被切出来的第一条原文：")
        lines.append(cut[:400])
        lines.append("")
    lines.append("接着发 `aa%d <正文>` 就从这儿往下写；不想改就直接继续。" % n)
    if not _same_skey(old_key, info["key"]):
        lines.append("")
        lines.append("原会话 `%s` 没动，完整历史还在那里。"
                     % _short_skey(old_key))
    t_reply(n, conv_id, mid, "\n".join(lines), groups, "-fork-msg")


def _gateway_msg_id(m):
    """一条消息的稳定标识（网关历史里的），用来做回显收取的 id 基线。"""
    ow = m.get("__openclaw") or {}
    return ow.get("id") or ("ts:%s" % m.get("timestamp"))


def reset_thread(conv_id, n, skey, mid, groups):
    """`aaN /new`：真正重置线程 N 的会话，而不是把 `/new` 当正文发进去。

    为什么不能在 session_send 里直接发 `/new`：投给 gateway 的正文前面拼了
    UNATTENDED_NOTE，而斜杠命令必须**位于整条消息开头**才被识别，加了前缀就
    退化成普通正文。2026-10-23 实测 `aa1 /new` 连发三次都没重置，还白白
    steer 进原会话把上下文又搅了一遍。所以这里走 gateway 的 sessions.reset。
    """
    st, _ = session_state(skey)
    was_active = bool((st or {}).get("active"))
    if was_active:
        session_abort(skey)
        time.sleep(2)
    ok, err = session_reset(skey)
    if not ok:
        t_reply(n, conv_id, mid,
                   "线程会话%d 重置失败：%s" % (n, err),
                   groups, "-reset-fail")
        return
    # 必须清掉本线程的运行账：否则 monitor 会拿重置后的空历史报「跑完了没正文」
    thread_put(conv_id, n, state="idle", started=None, base_ts=0, status="reset",
               pushes=0, deliver=False, last_ok=time.time())
    log("thread reset n=%d was_active=%s" % (n, was_active))
    msg = ("线程会话%d 已重置，上下文清空。接着发 `aa%d <正文>` 就是全新一轮。"
           % (n, n))
    if was_active:
        msg += "\n（原来那轮还在跑，我先中断再重置的。）"
    t_reply(n, conv_id, mid, msg, groups, "-reset")


_LETTERS = "ABCDEFGH"


def stop_thread(conv_id, n, skey, mid, groups):
    """`aaN /stop`：本地打断线程 N 的这一轮，不交给网关的斜杠层。

    为什么不能裸发：网关的 `/stop` 斜杠命令用「发起 chat.send 的那条连接」
    做 owner 校验（ownerConnId 比对），而本守护每次 `gateway call` 都是新的
    短连接 → 恒定 `unauthorized`（2026-10-29 用户 19:27 实测，用户看到的就是
    「线程会话N 执行 /stop 失败：unauthorized」）。本地调 `sessions.abort`
    走 CLI 的 admin 作用域，owner 校验被跳过，才真停得下来且**不丢上下文**。

    只有当 abort 仍被拒时才会退化成 reset（那才会清空上下文），回执必须把
    这个代价写清楚，不许含糊成「已停止」。
    """
    ok, method, detail = stop_run(skey)
    if not ok:
        t_reply(n, conv_id, mid,
                   "线程会话%d 没停下：%s\n\n"
                   "可以发 `aa%d /new` 强制重置（会清空上下文）。"
                   % (n, detail or "原因不明", n), groups, "-stop-fail")
        return
    if method == "none":
        rec = thread_get(conv_id, n)
        last = _hhmm(rec.get("last_ok")) if rec.get("last_ok") else "刚才"
        t_reply(n, conv_id, mid,
                   "线程会话%d 现在没在跑（上次结束 %s），不用停。"
                   % (n, last), groups, "-stop-idle")
        return
    # 无论走哪条路，这一轮的运行账都要清，否则 monitor 会拿旧 runId 继续盯
    thread_put(conv_id, n, state="idle", deliver=False, last_ok=time.time(),
               status="stopped")
    if method == "reset":
        t_reply(n, conv_id, mid,
                   "线程会话%d 已停下，但网关拒绝了跨连接中断（unauthorized），"
                   "我走的是重置：**上下文已清空**。\n\n"
                   "接着发 `aa%d <正文>` 是全新一轮。" % (n, n),
                   groups, "-stop-reset")
        return
    log("thread stop n=%d via abort" % n)
    t_reply(n, conv_id, mid,
               "线程会话%d 已停下，上下文保留（队列里没开始的补话也一并丢了）。\n\n"
               "接着发 `aa%d <正文>` 就是同一会话的新一轮。" % (n, n),
               groups, "-stop")


def question_prompt(n, q):
    """把待答问题翻成一份适合钉钉读的纯文本（块之间空行，别糊成一段）。"""
    qs = q.get("questions") or []
    left = max(0, int(q.get("expiresAtMs") or 0) / 1000.0 - time.time())
    head = ("线程会话%d 停下来等你拍板，约 %s 内有效；不回就按默认继续。"
            % (n, human_dur(left)))
    if len(qs) == 1:
        tail = "回 `aa%d <选项或你自己的答案>` 继续。" % n
    else:
        tail = ("共 %d 个问题，按顺序用 `|` 隔开作答：`aa%d 答案1 | 答案2…`"
                % (len(qs), n))
    blocks = [head]
    for i, item in enumerate(qs):
        lines = ["%s【%s】%s" % ("%d. " % (i + 1) if len(qs) > 1 else "",
                                 (item.get("header") or "问题").strip(),
                                 (item.get("question") or "").strip())]
        for j, opt in enumerate(item.get("options") or []):
            label = (opt.get("label") or "").strip()
            desc = (opt.get("description") or "").strip()
            mark = "%s." % _LETTERS[j] if j < len(_LETTERS) else "-"
            lines.append("  %s %s%s" % (mark, label, (" — " + desc) if desc else ""))
        if not (item.get("options") or []):
            lines.append("  （无预设选项，直接回文字）")
        blocks.append("\n".join(lines))
    blocks.append(tail)
    return "\n\n".join(blocks)


def _norm_answer(text):
    return (text or "").strip()


def _plain_answers(q, text):
    """正文原样当答案：每个问题都填同一句，不再猜选项。

    为什么不猜了：用户要的是「转发给我、我 push 回一句」，不是做选择题。
    猜错的代价（把他正常的一句新指令当成 A/B 选项吞掉）比猜对的价值大，
    所以这里只做透传；能不能收下由网关的 validateAnswers 判，失败就退回普通 push。
    """
    answers = {}
    for item in (q.get("questions") or []):
        qid = item.get("questionId")
        if qid:
            answers[qid] = [_norm_answer(text)]
    return answers


def _answer_pending(conv_id, q, n, text, mid, groups):
    """把 push 正文交回待答问题，返回 (ok, err)。"""
    answers = _plain_answers(q, text)
    if not answers:
        return False, "no-question-id"
    ok, err = question_resolve(q["id"], answers)
    if not ok:
        log("question resolve fail id=%s %s" % (q.get("id"), (err or "")[:160]))
        return False, err or "unknown"
    log("question resolved id=%s answers=%s" % (q.get("id"), answers))
    t_reply(n, conv_id, mid,
               "线程会话%d 已把这条答复交回问题，继续跑，有结果我再发。" % n,
               groups, "-qanswer")
    return True, ""


def push_thread(conv_id, n, question, mid, groups):
    """`aaN 正文`：往线程 N 投一条。跑着就 steer 进当前 run，空闲就开新一轮。

    N 不存在时按这个号新建再投：用户亲口写了 `aa1`，意图就是「用 1 号」，
    回一句「没有，请改用 aa 」只是把活推回给用户。
    """
    rec, created = thread_ensure(conv_id, n)
    if created:
        log("thread ensure n=%d created by push" % n)
    skey = rec["key"]

    # 任何 `/` 开头的一律本地拦截、裸发进网关（见 slash_thread 注释）。
    # 不枚举命令名：OpenClaw 的斜杠命令有几十条，追不全。
    if is_slash(question):
        slash_thread(conv_id, n, skey, question, mid, groups)
        return

    st, err = session_state(skey)
    if err:
        t_reply(n, conv_id, mid,
                   "线程会话%d 查不到状态（%s），这条没发。" % (n, err[:150]),
                   groups, "-fail")
        return
    busy = bool(st and st["active"])

    # 线程正卡在 ask_user 上：这条正文就是答案，走 question.resolve 交回去，
    # 不能再当普通消息 push——那只会排在新一轮，原问题继续等超时。
    # 只在 run 还活着时当答案：问题记录可能因 run 被取消而残留到过期，
    # 那种残留不该把用户正常的新任务吞掉。
    q = question_pending(skey) if busy else None
    if q:
        ok, err = _answer_pending(conv_id, q, n, question, mid, groups)
        if ok:
            thread_put(conv_id, n, q_forwarded=None, q_deadline=0)
            return
        # 交不上去（过期/已被答/答案不合法）：退回普通 push，别把话吞掉
        t_reply(n, conv_id, mid,
                   "线程会话%d 的问题没交上去（%s），按普通消息继续。"
                   % (n, (err or "")[:160]), groups, "-qfail")

    info, err2 = session_send(skey, UNATTENDED_NOTE + question, steer=busy)
    if not info:
        t_reply(n, conv_id, mid,
                   "线程会话%d push 失败：%s" % (n, (err2 or "")[:300]),
                   groups, "-fail")
        return
    if busy:
        t = thread_put(conv_id, n, pushes=(rec.get("pushes") or 0) + 1,
                       last_push=time.time(), last_msg=question[:200])
        log("thread push n=%d run=%s pushes=%d"
            % (n, (info.get("run") or "")[:12], t.get("pushes") or 0))
        t_reply(n, conv_id, mid,
                   "线程会话%d push消息成功（正跑着，这条会在下一次工具调用前带上）。" % n,
                   groups, "-push")
        return
    # 空闲：不装作 push 成功，按实际情况说，并把它当新一轮盯起来
    base_ts = int((st or {}).get("last_ts") or 0)
    started = time.time()
    thread_put(conv_id, n, state="running", started=started, base_ts=base_ts,
               deliver=True, last_mid=mid, run=info.get("run") or "",
               last_msg=question[:200], pushes=(rec.get("pushes") or 0) + 1)
    t_reply(n, conv_id, mid,
               "线程会话%d 刚才空闲（上次结束 %s），这条没接到运行中的任务上，"
               "我按新一轮开始跑了。" % (n, _hhmm(rec.get("last_ok") or started)),
               groups, "-push-idle")
    threading.Thread(target=monitor_thread,
                     args=(conv_id, n, groups, skey, base_ts, started, mid),
                     kwargs={"run_id": info.get("run") or ""},
                     daemon=True).start()


def resume_threads(groups):
    """重启后接着盯还在跑的线程；停机期间跑完的，把结果补发。"""
    snapshot = threads_snapshot()
    for conv_id, row in snapshot.items():
        for n, t in sorted(row.items()):
            if t.get("state") != "running":
                continue
            skey = t.get("key") or thread_key(conv_id, n)
            started = t.get("started") or time.time()
            base_ts = int(t.get("base_ts") or 0)
            mid = t.get("last_mid") or ("thread-%d" % n)
            st, err = session_state(skey, since_ms=base_ts)
            if st and st["active"]:
                log("resume: thread n=%d still running" % n)
                t_reply(n, conv_id, mid,
                           "守护进程重启过，线程会话%d 还在跑（已 %s），我接着盯。"
                           % (n, human_dur(time.time() - started)),
                           groups, "-reattach")
                threading.Thread(target=monitor_thread,
                                 args=(conv_id, n, groups, skey, base_ts, started, mid),
                                 daemon=True).start()
            else:
                text = (st or {}).get("last") or ""
                log("resume: thread n=%d finished while down" % n)
                if text and t.get("deliver"):
                    t_reply(n, conv_id, mid, text, groups, "-done")
                thread_put(conv_id, n, state="idle", deliver=False,
                           last_ok=time.time(), preview=_preview_line(text))


def _paras(text):
    """把一段正文按「一行一段」重排。

    钉钉把 `--content` 当 Markdown 渲染，单个换行会被压成空格：模型回的多行
    正文（列表、分行结论）发出去就糊成一整段。这里在行间补空行，并吃掉多余
    空行，既保住分行又不会空得太散。

    ``` 围起来的代码块原样保留：往里插空行会把缩进和行号弄乱。
    """
    text = (text or "").strip()
    if not text:
        return ""
    out, pos = [], 0
    for m in _FENCE_RE.finditer(text):
        out.append(_paras_plain(text[pos:m.start()]))
        out.append(m.group(0).strip("\n"))
        pos = m.end()
    out.append(_paras_plain(text[pos:]))
    return "\n\n".join(p for p in out if p)


_FENCE_RE = re.compile(r"```.*?```", re.S)


def _paras_plain(text):
    lines = [ln.rstrip() for ln in (text or "").strip().split("\n")]
    return "\n\n".join(ln for ln in lines if ln)


def status_text(conv_id):
    """本地拼一句本会话的线程现状，不问模型。"""
    threads = conv_threads(conv_id)
    if not threads:
        return "这个会话里还没有线程。发 `aa <任务>` 建一个。"
    now = time.time()
    blocks = ["**线程现状**：共 %d 条。" % len(threads)]
    for n in sorted(threads):
        t = threads[n]
        if t.get("state") == "running":
            blocks.append("**线程会话%d** · 在跑 · 已 %s · 累计 push %d 条"
                          % (n, human_dur(now - (t.get("started") or now)),
                             t.get("pushes") or 0))
        else:
            blocks.append("**线程会话%d** · 空闲 · 上次跑完 %s"
                          % (n, _hhmm(t.get("last_ok") or t.get("created") or now)))
    # 钉钉把 `--content` 当 Markdown 渲染，单个换行会被压成空格；
    # 分块之间必须空一行，否则整份概览糊成一段。
    return "\n\n".join(blocks)


def thread_status_text(conv_id, n):
    """`aaN` 不带正文：本地拼一份线程概览，**不调模型**。

    只读 gateway 的 chat.history（会话状态），不发起任何 run，所以毫秒级返回、
    不打断任务、也不烧 token。跑着就报进度，空闲就报上次结果。
    """
    t, created = thread_ensure(conv_id, n)
    if created:
        return ("线程会话%d 之前没有，我已经按这个号建好了（现在是空的）。"
                "发 `aa%d <正文>` 就开跑。" % (n, n))
    st, err = session_state(t["key"])
    if err:
        return "线程会话%d 查不到状态（%s）。" % (n, err[:120])

    st = st or {}
    # tools 现在是 {"name","run"} 的对象（带 run 才能分清是哪一轮调的）
    tools = [t.get("name") or "?" if isinstance(t, dict) else str(t)
             for t in (st.get("tools") or [])]
    blocks = []
    if st.get("active"):
        elapsed = time.time() - (t.get("started") or time.time())
        blocks.append("**线程会话%d** · 在跑 · 已 %s" % (n, human_dur(elapsed)))
        if t.get("q_forwarded"):
            blocks.append("**在等你回答**：问题已转发，回 `aa%d <选项或文字>` 就继续。" % n)
        if tools:
            counts = {}
            for name in tools:
                counts[name] = counts.get(name, 0) + 1
            top = "、".join("%s×%d" % (k, v) if v > 1 else k
                            for k, v in sorted(counts.items(),
                                               key=lambda kv: -kv[1])[:5])
            blocks.append("工具调用 %d 次：%s" % (len(tools), top))
        else:
            blocks.append("还没开始调工具（在读上下文或思考）。")
    else:
        blocks.append("**线程会话%d** · 空闲" % n)
        blocks.append("上次结束 %s · 累计 push %d 条"
                      % (_hhmm(t.get("last_ok") or t.get("started") or time.time()),
                         t.get("pushes") or 0))
        if tools:
            blocks.append("上一轮工具调用 %d 次。" % len(tools))

    # 最近一段输出：优先取本轮最新的助手正文，没有再退回整会话最后一条。
    tail = ""
    texts = st.get("texts") or []
    if texts:
        tail = (texts[-1] or {}).get("text") or ""
    if not tail:
        tail = st.get("last") or ""
    if tail:
        cut = tail[:400]
        blocks.append("**最近一段输出**\n\n" + _paras(cut)
                      + ("…" if len(tail) > 400 else ""))

    if st.get("active"):
        blocks.append("这条概览没打断任务，它还在跑。要补话发 `aa%d <正文>`。" % n)
    else:
        blocks.append("要接着用发 `aa%d <正文>`（会接上这个线程的历史）。" % n)
    # 同 status_text：块之间空一行，钉钉才按段渲染。
    return "\n\n".join(blocks)


def _last_output_for(rec):
    """取一个线程最近一次助手正文：先本轮 texts，再退整会话 last。失败返回空。"""
    skey = rec.get("key")
    if not skey:
        return ""
    st, err = session_state(skey)
    if err or not st:
        return ""
    texts = st.get("texts") or []
    if texts:
        tail = (texts[-1] or {}).get("text") or ""
        if tail:
            return tail
    return st.get("last") or ""


def _ts_sec(ts):
    """时间戳统一成秒：texts 里的 ts 是毫秒，threads.json 里的是秒。"""
    v = float(ts or 0)
    return v / 1000.0 if v > 1e11 else v


def _preview_line(text, limit=46):
    """把一段助手正文压成一行摘要：首行有内容的文本，去 Markdown 记号，超长截断。"""
    for raw in (text or "").split("\n"):
        ln = raw.strip()
        if not ln or ln.startswith("```") or ln.startswith("|"):
            continue
        ln = re.sub(r"^[#>*\-+\s]+", "", ln)
        ln = ln.replace("**", "").replace("`", "").replace("__", "")
        ln = re.sub(r"\[图片消息\]\([^)]*\)", "", ln)
        ln = re.sub(r"\[(文件|语音|视频)消息\]\([^)]*\)", "", ln)
        ln = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", ln)
        ln = re.sub(r"\s+", " ", ln).strip()
        if ln:
            return ln if len(ln) <= limit else ln[:limit - 1] + "…"
    return ""


def _rel(sec):
    """粗粒度相对时长：45s / 12m / 3h20m。概览里不需要秒级精度。"""
    sec = int(max(0, sec))
    if sec < 60:
        return "%ds" % sec
    if sec < 3600:
        return "%dm" % (sec // 60)
    return "%dh%dm" % (sec // 3600, (sec % 3600) // 60)


def aa_overview_text(conv_id, with_keys=False):
    """裸 `aa`：本会话线程一览，一条线程一行。

    格式（2026-10-27 按用户给的参考样式定稿）：
        - 🟢 aqaq 运行中 1m · an这个任务，目前有什么问题
        - ⚪ apap 已完成 · review diff a/.local/bin/dg_
        - 🔴 anan 失败 · 将 pre-delivery/repo-init 这个 skil…

    - 状态 emoji：🟢 运行中 / ⚪ 已完成 / 🔴 失败（超时、没起来都算失败）。
    - 每行 `- 状态 编号 文案 · 最近一句`；最近一句取本轮正文首行、46 字内。
    - 不加标题和尾注：参考样式就是纯列表，多一行都是噪声。
    - 只对**在跑**的线程查 gateway；空闲线程读 threads.json 的 preview 缓存，
      所以线程再多也是毫秒级、零 gateway 调用。
    - 行间用空行：钉钉纯文本会把单换行压成空格（实测），空行才会真的另起一段。
      SELFASK_AA_STYLE 可切：para（默认）/ list（单换行，压不压行看客户端）/
      compact（全并一行用 ｜ 分隔）。
    """
    threads = conv_threads(conv_id)
    if not threads:
        return "还没有线程。`aa <正文>` 建一条。"
    now = time.time()
    running = [n for n in sorted(threads) if threads[n].get("state") == "running"]
    # 一条纯文本消息就是一整块面板，一条线程一行；不要标题行（用户 2026-10-27 要求去掉）。
    # 不能用 ``` 围栏（钉钉不认），行之间靠空行分行。
    lines = []
    for n in sorted(threads):
        t = threads[n]
        if n in running:
            emoji = "🟢"
            label = "运行中 %s" % _rel(now - (t.get("started") or now))
            prev = _preview_line(t.get("last_msg") or "")
        else:
            last_ok = t.get("last_ok") or 0
            status = t.get("status") or ""
            if status in ("timeout", "stale", "failed"):
                emoji, label = "🔴", "失败"
            elif status == "reset":
                emoji, label = "⚪", "已重置"
            elif status == "fork":
                emoji, label = "⚪", "已 fork"
            else:
                emoji, label = "⚪", "已完成"
            if last_ok:
                label = (label + " " if label else "") + "%s 前" % _rel(now - last_ok)
            elif status not in ("timeout", "stale", "failed", "reset"):
                emoji, label = "⚪", "未开始"
            # 只显示「你发的任务」，不回退到 AI 回复：两者混排会让人以为是自己的话
            prev = _preview_line(t.get("last_msg") or "")
        # 行首不加任何符号：用户要求去掉圆点。注意**不能**用 `- `（钉钉会当
        # Markdown 列表、吃掉项间换行），行之间靠空行分行。
        line = "%s aa%d %s" % (emoji, n, label)
        line += (" · " + prev) if prev else " · （无记录）"
        if with_keys:
            key = t.get("key") or thread_key(conv_id, n)
            line += "\n   key: " + _short_skey(key)
        lines.append(line)
    style = (os.environ.get("SELFASK_AA_STYLE") or "fence").lower()
    if style == "compact":
        return " ｜ ".join(lines)
    if style == "para":
        return "\n\n".join(lines)
    if style == "list":
        return "\n".join(lines)
    # fence（默认）：``` 围栏 + 单换行。钉钉 text 模式不认围栏，但 markdown 模式
    # 会渲染成代码块（2026-10-27 用户确认「探针B」形态正确），send_reply 见到 ``` 开头
    # 就自动切 --markdown 发送。
    return "```plain text\n" + "\n".join(lines) + "\n```"
