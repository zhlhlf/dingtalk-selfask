#!/usr/bin/env python3
"""dingtalk-selfask 持久化层：threads.json、seen.json、消息去重。

只负责状态的落盘与合并，不 import 其它兄弟模块（除 config/util），
gateway/threads/dingtalk 都可以放心依赖它。
"""
import hashlib
import json
import os
import threading
import time

from selfask_config import THREADS, THREAD_PREFIX, STATE

HIS = os.path.join(os.path.dirname(THREADS), "his_index.json")
HIS_MAX_KEYS = 20
HIS_MAX_ITEMS = 500
_his_lock = threading.Lock()


def _his_load():
    try:
        with open(HIS) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _his_key(skey):
    """编号表按短 key 索引：`agent:main:dthread-x` 与 `dthread-x` 都能查到。"""
    value = (skey or "").strip().strip("`'\"")
    return value.split(":")[-1] if ":" in value else value


def his_put(skey, items):
    """记下这次 `/his` 的编号表：编号 → 消息 entry id。

    编号必须落盘：`/his` 只回一段文本，之后用户可能隔几分钟才发
    `/fork <key> <编号>`；这期间会话可能又多了几条消息，按「当前历史的第 n 条」
    重新数会指到另一条上去。所以编号在生成时就钉死 entry id。
    """
    with _his_lock:
        data = _his_load()
        data[_his_key(skey)] = {"ts": time.time(), "key": skey,
                                "items": list(items)[-HIS_MAX_ITEMS:]}
        if len(data) > HIS_MAX_KEYS:
            keep = sorted(data.items(), key=lambda kv: kv[1].get("ts") or 0)[-HIS_MAX_KEYS:]
            data = dict(keep)
        tmp = HIS + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(data, f, ensure_ascii=False)
            os.replace(tmp, HIS)
        except Exception:
            pass


def his_get(skey, n):
    """取编号 n 对应的记录（没记录返回 None）。"""
    with _his_lock:
        data = _his_load()
    row = data.get(_his_key(skey)) or {}
    for item in row.get("items") or []:
        if int(item.get("n") or 0) == int(n):
            out = dict(item)
            out.setdefault("key", row.get("key") or skey)
            return out
    return None


def his_prune(skey, cutoff):
    """回退/替换历史后，把 >= cutoff 的编号从表里删掉（那些 entry 已不在活跃分支上）。"""
    with _his_lock:
        data = _his_load()
        key = _his_key(skey)
        row = data.get(key)
        if not row:
            return 0
        before = row.get("items") or []
        row["items"] = [it for it in before if int(it.get("n") or 0) < int(cutoff)]
        dropped = len(before) - len(row["items"])
        tmp = HIS + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(data, f, ensure_ascii=False)
            os.replace(tmp, HIS)
        except Exception:
            pass
        return dropped


class ThreadStore:
    """线程表：首次写盘前先并入磁盘状态，绝不让空内存覆盖磁盘上的记录。

    2026-10-23 实测踩过：外部测试脚本 `import selfask` 后内存 `_threads` 为空，
    直接 dump 会把正在跑的线程表整个覆盖成 `{}`（cidROUTE 两条线程当场消失）。
    所以 `load()` 只做磁盘解析、不动合并标记；`save()` 首次写盘前必并盘。
    """

    def __init__(self, path, prefix="dthread-"):
        self.path = path
        self.prefix = prefix
        self.lock = threading.Lock()
        self._threads = {}
        self._loaded = False

    @property
    def loaded(self):
        return self._loaded

    def load(self):
        """纯磁盘解析；故意不改合并标记（load 不等于已并进内存表）。"""
        try:
            with open(self.path) as f:
                raw = json.load(f)
        except Exception:
            return {}
        out = {}
        for conv, items in (raw or {}).items():
            row = {}
            for key, value in (items or {}).items():
                try:
                    copied = dict(value)
                    copied["n"] = int(key)
                    row[int(key)] = copied
                except Exception:
                    continue
            out[conv] = row
        return out

    def _merge_locked(self):
        if self._loaded:
            return
        for conv, row in self.load().items():
            current = self._threads.setdefault(conv, {})
            for number, record in row.items():
                current.setdefault(number, record)
        self._loaded = True

    def merge(self):
        with self.lock:
            self._merge_locked()

    def snapshot(self):
        with self.lock:
            self._merge_locked()
            return {c: {n: dict(t) for n, t in row.items()}
                    for c, row in self._threads.items()}

    def _save_locked(self):
        self._merge_locked()
        data = {conv: {str(number): record for number, record in sorted(row.items())}
                for conv, row in self._threads.items()}
        temp = self.path + ".tmp"
        with open(temp, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
        os.replace(temp, self.path)

    def save(self):
        with self.lock:
            self._save_locked()

    def key(self, conv, number):
        """线程会话 key 由 (会话, 线程号) 派生：重启后不变，push 才能接上。"""
        return self.prefix + hashlib.sha1(("%s#%d" % (conv, number)).encode()).hexdigest()[:12]

    def get(self, conv, number):
        with self.lock:
            self._merge_locked()
            return dict((self._threads.get(conv) or {}).get(number) or {})

    def put(self, conv, number, **fields):
        with self.lock:
            self._merge_locked()
            row = self._threads.setdefault(conv, {})
            record = row.setdefault(number, {"n": number, "key": self.key(conv, number),
                                             "created": time.time(), "state": "idle"})
            record.update(fields)
            self._save_locked()
            return dict(record)

    def ensure(self, conv, number):
        """按指定编号取线程；不存在就**按这个号**建一个（用户写 aa3 就是 3）。"""
        with self.lock:
            self._merge_locked()
            row = self._threads.setdefault(conv, {})
            if number in row:
                return dict(row[number]), False
            record = {"n": number, "key": self.key(conv, number),
                      "created": time.time(), "state": "idle"}
            row[number] = record
            self._save_locked()
            return dict(record), True

    def alloc(self, conv):
        """新建线程号：取当前最小空号，已有（含已跑完的）号不回收。"""
        with self.lock:
            self._merge_locked()
            row = self._threads.setdefault(conv, {})
            number = 1
            while number in row:
                number += 1
            record = {"n": number, "key": self.key(conv, number),
                      "created": time.time(), "state": "idle"}
            row[number] = record
            self._save_locked()
            return dict(record)

    def conversation(self, conv):
        with self.lock:
            self._merge_locked()
            return {number: dict(record)
                    for number, record in (self._threads.get(conv) or {}).items()}


_store = ThreadStore(THREADS, THREAD_PREFIX)

# ---- 模块级兼容接口（守护进程与测试都从这里拿） ----

def save_threads():
    _store.save()


def load_threads():
    """纯解析 threads.json，不标已合并。"""
    return _store.load()


def threads_merge():
    _store.merge()


def threads_snapshot():
    return _store.snapshot()


def thread_key(conv, n):
    return _store.key(conv, n)


def thread_get(conv, n):
    return _store.get(conv, n)


def thread_put(conv, n, **fields):
    return _store.put(conv, n, **fields)


def thread_ensure(conv, n):
    return _store.ensure(conv, n)


def thread_alloc(conv):
    return _store.alloc(conv)


def conv_threads(conv):
    return _store.conversation(conv)


# ---- seen.json 与消息去重 ----

def load_seen():
    try:
        with open(STATE) as f:
            data = json.load(f)
        return list(data)[-500:]
    except Exception:
        return []


def save_seen(seen):
    tmp = STATE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(seen[-500:], f)
    os.replace(tmp, STATE)


_lock = threading.Lock()


class Store(object):
    """message_id 去重表：长连接与补拉两条通道共用。"""

    def __init__(self, seen):
        self.seen = seen
        self.set = set(seen)

    def add(self, mid):
        with _lock:
            if mid in self.set:
                return False
            self.set.add(mid)
            self.seen.append(mid)
            save_seen(self.seen)
            return True
