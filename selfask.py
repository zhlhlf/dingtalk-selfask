#!/usr/bin/env python3
"""dingtalk-selfask 兼容薄入口。

启动方式不变：``python3 selfask.py``（`watchdog.sh` 无需改动）。全部逻辑在
``src/`` 里：

    src/selfask_config    常量 / 环境变量 / 开关
    src/selfask_util      日志、子进程、时间格式化、连接状态
    src/selfask_state     threads.json / seen.json / 去重
    src/selfask_gateway   Gateway RPC、会话读写、abort/reset
    src/selfask_dingtalk  钉钉收发、长连接、群表
    src/selfask_threads   aa 线程编排：建/push/监控/slash/reset/状态文案
    src/selfask_commands  aa 命令解析（纯函数，无状态）
    src/selfask_app       装配：路由、监听、补拉、单实例锁、main

`from selfask import *` 的老用法（测试脚本）继续可用。
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from selfask_app import *  # noqa: F401,F403,E402 - 保持旧 import 面

if __name__ == "__main__":
    main()
