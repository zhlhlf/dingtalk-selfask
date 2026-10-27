#!/usr/bin/env python3
"""dingtalk-selfask 通用小件：日志、子进程、时间格式化、连接状态记录。

被所有兄弟模块依赖；自身只依赖 selfask_config，不 import 其它模块。
"""
import subprocess
import threading
import time

from selfask_config import LOG, ENV, DD_TZ_OFFSET


def log(msg):
    line = "%s %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg)
    print(line, flush=True)
    try:
        with open(LOG, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def run(cmd, timeout=60, stdin_data=None):
    started = time.time()
    try:
        p = subprocess.run(cmd, env=ENV, capture_output=True, text=True,
                           timeout=timeout, input=stdin_data)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as e:
        log("run: timeout after %.0fs cmd=%s" % (time.time() - started, cmd[1:3]))
        return 124, (e.stdout or ""), "timeout"


def human_dur(sec):
    sec = int(sec)
    if sec < 60:
        return "%ds" % sec
    if sec < 3600:
        return "%dm%02ds" % (sec // 60, sec % 60)
    return "%dh%02dm" % (sec // 3600, (sec % 3600) // 60)


def _hhmm(ts):
    try:
        return time.strftime("%H:%M", time.localtime(ts))
    except Exception:
        return "?"


def _dd_time(ts):
    """本地 epoch -> 钉钉接口要的 UTC+8 字符串。"""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts + DD_TZ_OFFSET))


class _Conn(object):
    """长连接状态记录，只用于日志与排查，不参与任何判定。

    这里只记住 state / 子进程状态的变化，好在日志里看清什么时候翻过状态。
    """

    def __init__(self):
        self.cond = threading.Condition()
        self.state = "unknown"
        self.since = time.time()
        self.alive = False
        self.reconnects = 0

    def set_state(self, state, reconnects=None):
        with self.cond:
            now = time.time()
            if reconnects is not None:
                self.reconnects = reconnects
            if state != self.state:
                held = now - self.since
                log("conn: state %s -> %s（前一状态持续 %s）"
                    % (self.state, state, human_dur(held)))
                self.state, self.since = state, now

    def set_alive(self, alive):
        with self.cond:
            if self.alive != alive:
                log("conn: consumer %s" % ("已启动" if alive else "已退出"))
            self.alive = alive


CONN = _Conn()
