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
                            MAX_CONCURRENCY, UNATTENDED_NOTE, RESET_CMDS)
from selfask_util import log, human_dur, _hhmm
from selfask_state import (thread_get, thread_put, thread_ensure, thread_alloc,
                           conv_threads, thread_key, threads_snapshot)
from selfask_gateway import (session_state, session_send, session_abort,
                             session_reset, history_raw, await_reply)
from selfask_dingtalk import send_reply, check_live_health

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
        send_reply(conv_id, mid,
                   "线程会话%d 排着：同时在跑的线程已到上限 %d 条，空出来就开跑。"
                   % (n, MAX_CONCURRENCY), groups, "-queued")
        _slots.acquire()
        send_reply(conv_id, mid, "线程会话%d 轮到它了，开始跑。" % n, groups, "-turn")

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
            send_reply(conv_id, mid, "线程会话%d 启动失败：%s" % (n, (err or "")[:300]),
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
            send_reply(conv_id, mid, msg, groups, "-timeout")
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
                send_reply(conv_id, mid, text, groups, "-done")
            else:
                log("thread done n=%d in %s (no text)" % (n, spent))
                send_reply(conv_id, mid,
                           "线程会话%d 跑完了（%s），但没拿到正文输出。"
                           % (n, spent), groups, "-done-empty")
            thread_put(conv_id, n, state="idle", deliver=False, last_ok=now,
                       status="done", preview=_preview_line(text))
            return "done"

        if now - last_report >= PROGRESS_EVERY:
            last_report = now
            bits = ["线程会话%d 还在跑，没停（已 %s）。" % (n, human_dur(elapsed))]
            tail = (mine[-1]["text"] if mine else "") or ""
            if tail:
                bits.append("最近输出：" + tail[:300])
            bits.append("要补话就发 `aa%d <正文>`，下一轮工具调用前带上。" % n)
            send_reply(conv_id, mid, "\n".join(bits), groups, "-progress")

        # 一直没进入 busy 状态也没有新正文：多半是没起来，别无限等
        if not busy_seen and not fresh_text and elapsed > 180:
            send_reply(conv_id, mid,
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
        send_reply(conv_id, mid,
                   "线程会话%d `%s` 执行了，但没拿到回显。" % (n, cmd),
                   groups, "-slash-empty")
        return
    send_reply(conv_id, mid, out, groups, "-slash")


def slash_thread(conv_id, n, skey, raw, mid, groups):
    """`aaN /xxx`：任何斜杠命令都本地拦截，裸发进网关，让它自己的斜杠层处理。

    为什么不维护命令白名单：OpenClaw 自带几十条斜杠命令，还在增加，逐个枚举
    永远追不上。这里只按「是不是 / 开头」分流，命令本身交给网关认。

    为什么必须裸发：投普通任务时正文前面拼了 UNATTENDED_NOTE，而斜杠命令只有
    位于整条消息开头才被识别，加了前缀就退化成普通对话内容（`aa1 /new` 连发
    三次都没重置，就是这么来的）。

    唯一例外是 reset 家族：`/new` 走 chat.send 会被网关拒（不是这个会话的
    owner），必须改用 sessions.reset。这个例外有实测依据，不是猜的。
    """
    cmd = raw.strip().split()[0].lower()
    if cmd in RESET_CMDS:
        reset_thread(conv_id, n, skey, mid, groups)
        return

    st, _ = session_state(skey)
    busy = bool(st and st["active"])
    seen_ids = {_gateway_msg_id(m) for m in history_raw(skey)}

    # 不 steer：斜杠命令是在消息入口被解析的，塞进运行中的 run 里不保证生效
    info, err = session_send(skey, raw)
    if not info:
        send_reply(conv_id, mid,
                   "线程会话%d 执行 `%s` 失败：%s" % (n, cmd, (err or "")[:250]),
                   groups, "-slash-fail")
        return
    log("thread slash n=%d cmd=%s run=%s" % (n, cmd, (info.get("run") or "")[:12]))

    if busy:
        send_reply(conv_id, mid,
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
        send_reply(conv_id, mid,
                   "线程会话%d 已执行 `%s`，没有输出（这条命令本身不返回内容）。"
                   % (n, cmd),
                   groups, "-slash-empty")
        return
    send_reply(conv_id, mid, out, groups, "-slash")


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
        send_reply(conv_id, mid,
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
    send_reply(conv_id, mid, msg, groups, "-reset")


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
        send_reply(conv_id, mid,
                   "线程会话%d 查不到状态（%s），这条没发。" % (n, err[:150]),
                   groups, "-fail")
        return
    busy = bool(st and st["active"])

    info, err2 = session_send(skey, UNATTENDED_NOTE + question, steer=busy)
    if not info:
        send_reply(conv_id, mid,
                   "线程会话%d push 失败：%s" % (n, (err2 or "")[:300]),
                   groups, "-fail")
        return
    if busy:
        t = thread_put(conv_id, n, pushes=(rec.get("pushes") or 0) + 1,
                       last_push=time.time(), last_msg=question[:200])
        log("thread push n=%d run=%s pushes=%d"
            % (n, (info.get("run") or "")[:12], t.get("pushes") or 0))
        send_reply(conv_id, mid,
                   "线程会话%d push消息成功（正跑着，这条会在下一次工具调用前带上）。" % n,
                   groups, "-push")
        return
    # 空闲：不装作 push 成功，按实际情况说，并把它当新一轮盯起来
    base_ts = int((st or {}).get("last_ts") or 0)
    started = time.time()
    thread_put(conv_id, n, state="running", started=started, base_ts=base_ts,
               deliver=True, last_mid=mid, run=info.get("run") or "",
               last_msg=question[:200], pushes=(rec.get("pushes") or 0) + 1)
    send_reply(conv_id, mid,
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
                send_reply(conv_id, mid,
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
                    send_reply(conv_id, mid, text, groups, "-done")
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


def aa_overview_text(conv_id):
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
