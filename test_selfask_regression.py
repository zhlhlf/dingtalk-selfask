#!/usr/bin/env python3
"""Offline regression checks: routing, thin-entrypoint surface, threads.json semantics."""
import json
import os
import tempfile

import selfask
from selfask_config import SELF_NAME
from selfask_state import ThreadStore

# 1) 路由语义：斜杠只留 `aaN /xxx` 一个入口；裸 `/xxx` 与 `aa /xxx` 都忽略；
#    裸 `aa` 是线程一览（2026-10-24 起）。
cases = {
    "/status": (None, None, "not-aa"),
    "aa /status": (None, None, "slash-ignored"),
    "aa": ("overview", None, ""),
    "aa7 /new": ("push", 7, "/new"),
    "aa1": ("status", 1, ""),
    "aa1 follow up": ("push", 1, "follow up"),
    "aa 任务开始": ("new", None, "任务开始"),
    "aa0 x": (None, None, "aa-zero"),
    "aa1x": (None, None, "not-aa"),
}
# 剥 @ 前缀依赖本人姓名；没有配置身份时跳过这条（新克隆的仓库默认没有）
if SELF_NAME:
    cases["@%s aa2 /model" % SELF_NAME] = ("push", 2, "/model")

for raw, expected in cases.items():
    actual = selfask.parse_task(raw)
    assert actual == expected, (raw, actual, expected)

# 2) 兼容 import 面：老测试脚本依赖的名字必须在
for name in ("main", "parse_task", "parse_cmd", "strip_mention", "thread_get",
             "thread_put", "thread_alloc", "thread_ensure", "conv_threads",
             "session_send", "session_state", "session_abort", "handle_incoming",
             "send_reply", "push_thread", "slash_thread", "reset_thread",
             "monitor_thread", "latest_thread", "status_text",
             "thread_status_text", "is_slash", "save_threads", "load_threads",
             "resume_threads", "chunk_text"):
    assert hasattr(selfask, name), name

# 3) 纯 load 不得标记已合并；随后 save 不得丢磁盘上的 running 线程
with tempfile.TemporaryDirectory() as tmp:
    path = os.path.join(tmp, "threads.json")
    fixture = {"cid": {"1": {"n": 1, "key": "dthread-x", "state": "running",
                             "pushes": 2}}}
    with open(path, "w") as f:
        json.dump(fixture, f)

    store = ThreadStore(path)
    assert store.load()["cid"][1]["state"] == "running"
    assert store.loaded is False            # 纯读不置位
    store.save()                            # 首次写盘前必须并盘
    with open(path) as f:
        assert json.load(f)["cid"]["1"]["state"] == "running"

    reader = ThreadStore(path)
    assert reader.conversation("cid")[1]["state"] == "running"
    assert reader.loaded is True

    # alloc 取最小空号；ensure 尊重指定号
    assert reader.alloc("cid")["n"] == 2
    rec, created = reader.ensure("cid", 5)
    assert created and rec["n"] == 5
    assert reader.conversation("cid")[5]["key"].startswith("dthread-")

# 4) latest_thread：在跑的优先；没有在跑的按最近活动
with tempfile.TemporaryDirectory() as tmp:
    import selfask_state
    path = os.path.join(tmp, "threads.json")
    with open(path, "w") as f:
        json.dump({"c2": {
            "1": {"n": 1, "key": "k1", "state": "idle", "created": 100},
            "2": {"n": 2, "key": "k2", "state": "running", "created": 50,
                  "started": 900},
        }}, f)
    old_store = selfask_state._store
    try:
        selfask_state._store = ThreadStore(path)
        assert selfask.latest_thread("c2") == 2
        assert selfask.latest_thread("nope") is None
    finally:
        selfask_state._store = old_store

import selfask_threads as th

# 5) ask_user 转发：转发文案 + 原样透传答案（纯函数，不打网关）
Q = {"id": "q1", "expiresAtMs": int((__import__("time").time() + 600) * 1000),
     "questions": [{"questionId": "deploy", "header": "部署", "question": "发布到哪",
                    "options": [{"label": "Staging (Recommended)", "value": "staging"},
                                {"label": "Production"}]}]}
txt = th.question_prompt(3, Q)
assert "线程会话3" in txt and "A. Staging (Recommended)" in txt
assert "aa3" in txt and "\n\n" in txt
# 答案原样透传：不猜选项，A/B 与整句都照发，收不收由网关判
assert th._plain_answers(Q, "B") == {"deploy": ["B"]}
assert th._plain_answers(Q, "  Production，但先备份 ") == {"deploy": ["Production，但先备份"]}
Q2 = {"id": "q2", "expiresAtMs": 0, "questions": [
    {"questionId": "a", "header": "A", "question": "甲", "options": []},
    {"questionId": "b", "header": "B", "question": "乙", "options": []}]}
assert set(th._plain_answers(Q2, "都行").keys()) == {"a", "b"}
# 提示词只剩一行，且不再提 ask_user（2026-10-27 起 ask_user 在 dthread 直接报错）
from selfask_config import UNATTENDED_NOTE
assert "不要用 ask_user" in UNATTENDED_NOTE and "停下等我回复" in UNATTENDED_NOTE
assert UNATTENDED_NOTE.count("\n") == 2 and len(UNATTENDED_NOTE.encode()) < 300

print("PASS: routing, import surface, threads.json merge-on-first-write, "
      "latest_thread, question-forward + plain answers")
