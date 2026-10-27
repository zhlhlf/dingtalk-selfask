#!/usr/bin/env python3
"""钉钉「自己发 aa 任务」自动回复守护进程——装配层。

触发：本人发出、剥掉 @ 前缀后正文以 `aa `（小写 a、a 加一个半角空格）开头的消息。
流程：监听事件 -> 只挑出「发送人是我自己」+ `aa ` 前缀 -> 去掉前缀当问题 ->
派到后台线程执行 -> 立刻回执 -> 跑完把结果推回原会话。

订阅范围（实测结论，见 README）：
- 群聊：`user_im_message_receive_group_all`，自己发的普通消息（不 @ 任何人）也能收到。
- 单聊（含与自己的会话）：只有 `user_im_message_receive_at` 能收到，所以仍需 @ 自己一次。
两个 event_key 同时订阅，同一条消息可能到两次，靠 message_id 去重。

连接：`dws event consume` 长连接负责实时收消息，历史补拉负责捡回推流静默漏掉的
消息；两条通道共用去重和路由。长连接断了（进程退出、网络中断）就退避重连；
收到消息立刻分发给子 agent，不在监视线程里做任何耗时的事。

设计要点（避免「怎么停了」）：
1. 监听线程永不阻塞：收发消息全在后台线程里做，长任务不拖住监听。
2. 立刻回执：收到就回一句，不再让对面静默几分钟。
3. 跑完主动推送：结果、失败原因、超时原因都回推，不留哑尾。
4. 线程会话 key 由 (会话, 线程号) 派生、重启不变：push 才接得上。
5. 问进度不进模型：本会话有任务在跑时，「咋样了」由本地直接答。

模块分层（谁依赖谁，见 README 模块图）：
    config（常量/环境） ← util（日志/子进程） ← state / gateway / dingtalk
    threads（编排）依赖上面全部；本模块装配一切并跑 main。

关键防环：回复正文以 REPLY_MARKER 开头，收到以该标记开头的消息直接跳过，
否则「自己回复自己」会无限自激。`aa ` 前缀天然不匹配 `🤖`，这里仍保留标记判断作双保险。
"""
import fcntl
import json
import os
import threading
import time

from selfask_config import (SELF_OID, SELF_NAME, REPLY_MARKER, BASE,
                            PING_RE, BACKFILL_EVERY, BACKFILL_WINDOW,
                            BACKFILL_DELAY, BACKFILL_BOOT_WINDOW,
                            TASK_HARD_LIMIT, PROGRESS_EVERY, MAX_CONCURRENCY,
                            DWS_BIN)
from selfask_util import log, run, CONN, _dd_time
from selfask_state import (Store, load_seen, save_threads, load_threads,  # noqa: F401
                           thread_get, threads_merge, threads_snapshot)
from selfask_gateway import (gw_call, session_state, session_send,  # noqa: F401
                             session_abort, session_reset, history_raw,
                             await_reply, gw_health, refresh_egress_env)
from selfask_dingtalk import (send_reply, load_groups, chunk_text,  # noqa: F401
                              kill_stale_listeners, _pump_stderr, spawn_consumer,
                              check_live_health)
from selfask_threads import (start_thread, monitor_thread, push_thread,  # noqa: F401
                             slash_thread, reset_thread, resume_threads,
                             status_text, thread_status_text, latest_thread,
                             aa_overview_text, is_slash, thread_put,
                             thread_ensure, thread_alloc, conv_threads,
                             thread_key)
from selfask_commands import (CMD_NEW, CMD_PUSH, CMD_STAT, CMD_SLASH,  # noqa: F401
                              CMD_OVERVIEW, parse_cmd,
                              strip_mention as _strip_mention)


def strip_mention(text):
    """剥掉 @ 前缀（兼容入口：旧的 strip_mention(text) 单参数签名）。"""
    return _strip_mention(text, SELF_NAME)


def parse_task(text):
    """测试入口：剥 @ 前缀后解析，返回 (kind, n, payload)。"""
    return parse_cmd(strip_mention(text))


def handle_incoming(store, groups, mid, conv, content, src):
    """一条收到的本人消息：剥 @ → 挡自发回复 → 认命令 → 去重 → 建线程/push/查状态。

    长连接与补拉共用这一段，保证两条通道判定完全一致；src 只用于日志区分来源。
    补拉通道遇到不相关消息也记进 seen，否则每轮都会重复判定同一条。
    """
    body = strip_mention(content)
    log("E  id=%s src=%s raw=%r body=%r" % (mid, src, content[:120], body[:120]))
    # 双保险：自己的回复以 🤖 开头，先按标记挡掉（顺序仍在剥 @ 之后）
    if body.startswith(REPLY_MARKER):
        if src == "fill":
            store.add(mid)
        log("I  ignore own reply id=%s" % mid)
        return
    kind, n, payload = parse_cmd(body)
    if not kind:
        if src == "fill":
            store.add(mid)
        log("I  ignore %s id=%s src=%s body=%r" % (payload, mid, src, body[:60]))
        return
    if not store.add(mid):
        log("I  duplicate id=%s src=%s" % (mid, src))
        return

    if kind == CMD_OVERVIEW:
        log("Q* conv=%s id=%s src=%s (aa overview)" % (conv, mid, src))
        threading.Thread(
            target=send_reply,
            args=(conv, mid, aa_overview_text(conv), groups, "-aa"),
            daemon=True).start()
        return

    if kind == CMD_NEW:
        if PING_RE.match(payload):
            threading.Thread(
                target=send_reply,
                args=(conv, mid, status_text(conv), groups, "-status"),
                daemon=True).start()
            return
        rec = thread_alloc(conv)
        log("Q+ conv=%s id=%s src=%s n=%d q=%r"
            % (conv, mid, src, rec["n"], payload[:80]))
        send_reply(conv, mid,
                   "线程会话%d 创建成功，正在处理…\n%s"
                   % (rec["n"], payload[:80].replace("\n", " ")),
                   groups, "-new")
        start_thread(conv, rec["n"], payload, mid, groups)
        return

    if kind == CMD_PUSH:
        log("Q> conv=%s id=%s src=%s n=%d q=%r"
            % (conv, mid, src, n, payload[:80]))
        push_thread(conv, n, payload, mid, groups)
        return

    if kind == CMD_SLASH:
        n2 = latest_thread(conv)
        if n2 is None:
            send_reply(conv, mid,
                       "这个会话里还没有线程，`%s` 没地方执行。先发 `aa <任务>` 建一个，"
                       "或用 `aa1 %s` 指定 1 号。" % (payload, payload),
                       groups, "-slash-nothread")
            return
        log("Q/ conv=%s id=%s src=%s n=%d cmd=%r"
            % (conv, mid, src, n2, payload[:80]))
        threading.Thread(target=slash_thread,
                         args=(conv, n2, thread_get(conv, n2)["key"], payload,
                               mid, groups),
                         daemon=True).start()
        return

    log("Q? conv=%s id=%s src=%s n=%d" % (conv, mid, src, n))
    threading.Thread(target=send_reply,
                     args=(conv, mid, thread_status_text(conv, n), groups, "-tstat"),
                     daemon=True).start()


def _backfill_once(store, groups, window):
    """查一次 [now-window, now] 里本人发的消息，新的交给 handle_incoming。

    返回新增条数；查询失败返回 None（调用方据此判断要不要重试）。
    只取 sender 是自己：推流那条路径由 listen_loop 做这个过滤，补拉必须在
    这里自己做，否则别人发的 `aa ` 消息也会被当成任务派出去。
    """
    end = time.time()
    rc, out, err = run([DWS_BIN, "chat", "message", "list-all",
                        "--start", _dd_time(end - window), "--end", _dd_time(end),
                        "--limit", "100", "--page-all", "-f", "json"],
                       timeout=120)
    if rc != 0:
        log("backfill: query fail rc=%s %s" % (rc, (err or out)[-200:]))
        return None
    try:
        data = json.loads(out)
    except Exception as e:
        log("backfill: parse fail %r" % e)
        return None
    fresh = 0
    for c in (data.get("result", {}).get("conversationMessagesList") or []):
        conv = c.get("conversationId") or c.get("openConversationId") or ""
        for m in (c.get("messages") or []):
            if (m.get("senderOpenDingTalkId") or "") != SELF_OID:
                continue
            mid = m.get("messageId") or ""
            if not mid or mid in store.set:
                continue
            content = m.get("text") or m.get("content") or ""
            fresh += 1
            handle_incoming(store, groups, mid, conv, content, "fill")
    return fresh


def backfill_loop(store, groups, stop):
    """固定周期补拉。不看连接状态、不做健康判定——就是照着漏消息的事实一直查。

    间隔 30s < 窗口 120s，相邻四轮重叠，单轮失败也不会断档：
    下一轮还覆盖同一段时间。

    每轮顺手探一次网关：常驻进程里最容易「悄悄坏掉」的不是钉钉连接，而是
    网关不可达（重启、证书目录轮换、代理换端口），坏了以后所有 aa 都只会回
    「查不到状态」。这里先探、必要时修自己的环境，再把真实原因写进日志。
    """
    time.sleep(BACKFILL_DELAY)
    window = max(BACKFILL_WINDOW, BACKFILL_BOOT_WINDOW)
    gw_bad = 0
    gw_seen = False
    while not stop.is_set():
        try:
            ok, why = gw_health()
        except Exception as e:
            ok, why = False, repr(e)
        if ok:
            if not gw_seen:
                log("gateway: probe ok（30s 自检已启用）")
                gw_seen = True
            if gw_bad:
                log("gateway: recovered after %d bad probes" % gw_bad)
            gw_bad = 0
        else:
            gw_bad += 1
            log("gateway: probe fail x%d %s" % (gw_bad, why))
            if gw_bad >= 2:
                # 自愈只碰「我们自己继承的旧环境」，不去重启网关本身：
                # 网关可能正在更新/重启，外人插手只会更糟。
                if refresh_egress_env():
                    time.sleep(2)
                    ok2, why2 = gw_health()
                    log("gateway: retry after env refresh -> %s %s"
                        % ("ok" if ok2 else "fail", why2 if not ok2 else ""))
                    if ok2:
                        gw_bad = 0
            if gw_bad >= 3:
                log("gateway: ALERT unreachable for ~%ds（已连续 %d 次探测失败）"
                    % (gw_bad * BACKFILL_EVERY, gw_bad))
        try:
            fresh = _backfill_once(store, groups, window)
        except Exception as e:
            log("backfill: error %r" % e)
            fresh = None
        # 每轮都留一行：静默成功也要能看出这条通道还活着、间隔对不对。
        if fresh is None:
            log("backfill: 本轮失败 window=%.0fs，下轮重试" % window)
        else:
            log("backfill: ok 补到 %d 条 window=%.0fs" % (fresh, window))
            window = BACKFILL_WINDOW      # 首轮补完就回到常窗口
        left = BACKFILL_EVERY
        while left > 0 and not stop.is_set():
            step = min(5, left)
            time.sleep(step)
            left -= step


def listen_loop(store, groups, stop):
    """常驻监听；进程异常退出时退避重连。

    同时订阅两个 event_key：
    - user_im_message_receive_at        → 被 @ 的消息（单聊里只有这条能到）
    - user_im_message_receive_group_all → 群聊里自己发的所有消息（不 @ 也能到）
    同一条 @ 消息会从两个订阅各来一次，靠 message_id 去重。
    """
    backoff = 5
    while not stop.is_set():
        kill_stale_listeners()
        log("listen: starting dws event consume at-me + all-group")
        try:
            p = spawn_consumer()
        except Exception as e:
            log("listen: spawn fail %r" % e)
            time.sleep(backoff)
            continue
        threading.Thread(target=_pump_stderr, args=(p,), daemon=True).start()
        CONN.set_alive(True)
        started = time.time()
        try:
            for line in p.stdout:
                line = line.strip()
                if not line.startswith("{"):
                    if line:
                        log("out: " + line[:200])
                    continue
                try:
                    ev = json.loads(line)
                except Exception:
                    continue
                if ev.get("type") not in ("user_im_message_receive_at",
                                          "user_im_message_receive_group_all"):
                    continue
                sender = ev.get("sender_open_dingtalk_id") or ""
                content = ev.get("content") or ""
                mid = ev.get("message_id") or ""
                conv = ev.get("conversation_id") or ""
                if sender != SELF_OID:
                    log("I  ignore other sender=%s id=%s" % (sender, mid))
                    continue
                handle_incoming(store, groups, mid, conv, content, "live")
        except Exception as e:
            log("listen: read loop error %r" % e)
        finally:
            CONN.set_alive(False)
            try:
                p.terminate()
            except Exception:
                pass
        if time.time() - started > 60:
            backoff = 5
        log("listen: exited, retry in %ss" % backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


def _acquire_singleton():
    """拿单实例锁，拿不到就退出。

    实测坑：重启时 kill 完的 4 秒空窗里 watchdog 先把进程拉起来了，我们的
    setsid 又起一个，结果两个实例各挂一个 consumer，同一订阅的事件被随机
    分走一半，两边都收不全。只靠 pgrep 判重有竞态（两个进程可以同时通过
    检查），所以用 flock：锁由内核随进程退出自动释放，不会留死锁。
    """
    f = open(os.path.join(BASE, ".selfask.lock"), "w")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, OSError):
        log("boot: 已有实例在运行（锁被占），本进程退出")
        return None
    f.write(str(os.getpid()))
    f.flush()
    return f          # 必须保住引用，文件对象一被回收锁就释放了


def main():
    os.makedirs(BASE, exist_ok=True)
    lock = _acquire_singleton()
    if lock is None:
        return
    store = Store(load_seen())
    groups = load_groups()
    threads_merge()
    log("boot: groups=%d seen=%d threads=%d hard_limit=%ss progress=%ss "
        "concurrency=%d backfill=%ss/%ss"
        % (len(groups), len(store.seen),
           sum(len(r) for r in threads_snapshot().values()), TASK_HARD_LIMIT,
           PROGRESS_EVERY, MAX_CONCURRENCY,
           BACKFILL_EVERY, BACKFILL_WINDOW))
    resume_threads(groups)
    stop = threading.Event()
    # 两条通道并行：推流负责实时，补拉负责捡回它静默丢掉的那些。
    threading.Thread(target=backfill_loop, args=(store, groups, stop),
                     daemon=True).start()
    # 连接健康巡检：空闲时 monitor 线程不存在，单靠它看不住连接。
    threading.Thread(target=health_loop, args=(stop,), daemon=True).start()
    listen_loop(store, groups, stop)


def health_loop(stop):
    """每 60 秒查一次 bus 账本；连续异常就重启 consumer/bus。

    与 monitor 线程里的检查共用同一把锁和节流窗口，不会重复查。
    """
    log("health: loop started (every 60s, repair after 3 bad)")
    while not stop.is_set():
        try:
            check_live_health()
        except Exception as e:
            log("health: loop check fail %r" % e)
        stop.wait(60)


if __name__ == "__main__":
    main()
